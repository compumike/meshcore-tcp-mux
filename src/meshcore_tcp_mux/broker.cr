require "./config"
require "./session"
require "./protocol"
require "./leases"

module MeshCoreTCPMux
  # All mutable protocol state lives here. Socket fibers exchange events with
  # the runtime; they never inspect or modify these queues and transactions.
  class Broker
    class Transaction
      getter owner : Int64
      getter command : Bytes
      getter descriptor : Protocol::CommandDescriptor
      getter started : Time::Span
      property progress : Time::Span
      property contacts_started = false
      getter pop_sequence : Int64
      getter notification_generation : Int64
      property step : Symbol = :command
      property scoped = false
      property maintenance = false
      property upstream_write_id : Int64? = nil
      property maintenance_write_id : Int64? = nil
      property job_id = 0_i64
      property response_frames = 0

      def initialize(@owner, @command, @descriptor, @started,
                     @pop_sequence = 0_i64, @notification_generation = 0_i64)
        @progress = @started
      end
    end

    getter epoch : Int64
    getter sessions = Hash(Int64, Session).new
    getter active : Transaction? = nil
    getter actions = Array(Action).new
    getter failed = false
    getter orphan : Bytes? = nil
    @order = Deque(Int64).new([0_i64])
    @next_write = 0_i64
    @upstream_writes = Hash(Int64, Time::Span).new
    @pop_sequence = 0_i64
    @notification_generation = 0_i64
    @drain_requested = false
    @last_poll : Time::Span
    @now = Time::Span.zero
    @dm_ring = DmRing.new
    @remote = RemoteLease.new
    @signing = SigningLease.new
    @counters = Hash(Symbol, UInt64).new(0_u64)
    @last_unknown_log : Time::Span? = nil
    @last_orphan_log : Time::Span? = nil

    def initialize(@epoch : Int64, @self_key : Bytes, @config = Config.new, now = Time::Span.zero,
                   @orphan : Bytes? = nil)
      @last_poll = now
      @signing = SigningLease.new(@config.signing_timeout)
    end

    def take_actions : Array(Action)
      result = @actions
      @actions = Array(Action).new
      result
    end

    def admit(id : Int64, now : Time::Span)
      @now = now
      if @failed || @active.try(&.maintenance)
        @actions << CloseSession.new(id, "upstream unavailable")
        return
      end
      @sessions[id] = Session.new(id)
      @counters[:connections] += 1
      @order << id
      if item = @orphan
        @orphan = nil
        fan_out(item)
      end
      request_drain
      schedule(now)
    end

    def client_frame(id : Int64, payload : Bytes, now : Time::Span)
      @now = now
      return if @failed
      return unless session = @sessions[id]?
      @counters[:commands] += 1
      if session.commands.size + (session.sync ? 1 : 0) + (@active.try(&.owner) == id ? 1 : 0) >= @config.command_limit
        remove(id, "command queue overflow")
      else
        session.commands << Command.new(payload.dup, now)
      end
      schedule(now)
    end

    def client_closed(id : Int64, now : Time::Span, reason = "client disconnected", category = :normal)
      @now = now
      remove(id, reason, category)
      # An active command remains owned until its real terminator, even though
      # the owner has left. No subsequent command can inherit those replies.
      schedule(now)
    end

    def written(id : Int64, epoch : Int64, write_id : Int64, now : Time::Span)
      return unless epoch == @epoch && !@failed
      if id == 0
        @upstream_writes.delete(write_id)
      elsif session = @sessions[id]?
        if budget = session.writes.delete(write_id)
          session.output_bytes -= budget.bytes
        end
        if (transaction = @active) && transaction.maintenance && transaction.step == :maintenance_result &&
           transaction.owner == id && transaction.maintenance_write_id == write_id
          finish_transaction(transaction)
        end
      end
      schedule(now)
    end

    def write_failed(id : Int64, epoch : Int64, reason : String, now : Time::Span)
      return unless epoch == @epoch && !@failed
      if id == 0
        fail_epoch("upstream write failed: #{reason}")
      else
        remove(id, "writer failed: #{reason}")
        schedule(now)
      end
    end

    def upstream_frame(payload : Bytes, now : Time::Span)
      @now = now
      return if @failed
      return unless check_active_deadline(now)
      @counters[:upstream_frames] += 1
      Protocol.validate_response_shape!(payload)
      if payload[0] >= 0x80
        push(payload, now)
      else
        transaction_response(payload, now)
      end
      schedule(now)
    rescue ex : Protocol::ProtocolError
      fail_epoch(ex.message || "invalid upstream response")
    end

    def tick(now : Time::Span)
      @now = now
      return if @failed
      return unless check_active_deadline(now)
      if @upstream_writes.any? { |_, deadline| now >= deadline }
        fail_epoch("upstream write deadline")
        return
      end
      @sessions.values.each do |session|
        if session.writes.any? { |_, budget| now >= budget.deadline }
          remove(session.id, "output write deadline")
          next
        end
        if sync = session.sync
          if now >= sync.deadline
            session.sync = nil
            reject(session.id, 4_u8, "virtual sync deadline")
          end
        end
      end
      @remote.expire(now)
      @signing.expire(now) unless @active.try(&.descriptor.flags.includes?(Protocol::CommandFlags::Signing))
      if !@sessions.empty? && now - @last_poll >= @config.poll_interval
        @last_poll = now
        request_drain
      end
      schedule(now)
    end

    def fail_epoch(reason : String)
      return if @failed
      @failed = true
      @actions << Diagnostic.new("epoch=#{@epoch} failed reason=#{reason.inspect}")
      @sessions.keys.each { |id| remove(id, "upstream epoch failed") }
      @active = nil
      @upstream_writes.clear
      @actions << Diagnostic.new("epoch=#{@epoch} summary #{@counters.map { |name, count| "#{name}=#{count}" }.join(' ')}")
      @actions << EndEpoch.new(@epoch, reason)
    end

    private def request_drain
      @drain_requested = true
      @notification_generation += 1
    end

    private def check_active_deadline(now : Time::Span) : Bool
      if transaction = @active
        return true if transaction.step == :maintenance_result
        if now - transaction.progress >= @config.response_timeout ||
           (transaction.descriptor.grammar.contacts? && now - transaction.started >= @config.contacts_timeout)
          fail_epoch("uncertain response timeout opcode=#{transaction.command[0]} owner=#{transaction.owner}")
          return false
        end
      end
      true
    end

    private def remove(id : Int64, reason : String, category = :normal)
      return unless session = @sessions.delete(id)
      @counters[:disconnections] += 1
      @counters[:discarded_inbox_items] += session.inbox.size.to_u64
      @remote.owner_gone(id)
      # Delay abandonment of signing state until an in-flight reply has been
      # classified, otherwise its accepted byte count would have no owner.
      @signing.owner_gone(id) unless @active.try(&.owner) == id
      @order.delete(id)
      @actions << Diagnostic.new("epoch=#{@epoch} session=#{id} close reason=#{reason.inspect} inbox_items=#{session.inbox.size} inbox_bytes=#{session.inbox_bytes} queued_commands=#{session.commands.size}", category)
      @actions << CloseSession.new(id, reason)
      if (transaction = @active) && transaction.maintenance && transaction.step == :maintenance_result && transaction.owner == id
        finish_transaction(transaction)
      end
    end

    private def emit(id : Int64, payload : Bytes) : Int64?
      return nil unless session = @sessions[id]?
      size = payload.size + 3
      if session.writes.size >= @config.output_frames || session.output_bytes + size > @config.output_bytes
        remove(id, "output queue overflow")
        return nil
      end
      @next_write += 1
      session.writes[@next_write] = WriteBudget.new(size, @now + @config.write_timeout)
      session.output_bytes += size
      @actions << SendFrame.new(id, @epoch, @next_write, payload)
      @next_write
    end

    private def reject(id : Int64, reason : UInt8, detail : String)
      @counters[:rejections] += 1
      @actions << Diagnostic.new("epoch=#{@epoch} session=#{id} rejection=#{reason} reason=#{detail.inspect}")
      emit(id, Bytes[1, reason])
    end

    private def schedule(now : Time::Span)
      return if @failed
      @now = now
      return if @active.try(&.maintenance)
      # Local head commands may progress while another client owns upstream.
      # Never let a local response pass the same client's active transaction.
      @sessions.values.each do |session|
        next if session.sync || @active.try(&.owner) == session.id
        while command = session.commands.first?
          validation = Protocol.validate_command(command.payload)
          if reason = validation.reason
            session.commands.shift
            reject(session.id, reason, "invalid command opcode=#{command.payload[0]}")
          elsif now - command.queued_at >= @config.command_age
            session.commands.shift
            reject(session.id, 4_u8, "command queue age")
          elsif command.payload[0] == 10
            session.commands.shift
            if session.inbox.empty?
              session.sync = PendingSync.new(@pop_sequence + 1, now + 5.seconds)
              request_drain
              break
            else
              deliver_item(session)
            end
          elsif command.payload[0] == 54
            session.commands.shift
            session.scope = command.payload.dup
            emit(session.id, Bytes[0])
          elsif command.payload[0] == 23 && !@config.private_key_export
            session.commands.shift
            @counters[:rejections] += 1
            @actions << Diagnostic.new("epoch=#{@epoch} session=#{session.id} command=export_private_key rejection=disabled")
            emit(session.id, Bytes[0x0f])
          elsif {24_u8, 51_u8}.includes?(command.payload[0]) && !@config.maintenance
            session.commands.shift
            reject(session.id, 1_u8, "maintenance disabled")
          else
            break
          end
          break unless @sessions.has_key?(session.id)
        end
      end
      return if @active
      @order.size.times do
        id = @order.shift
        @order << id
        if id == 0
          if @drain_requested && !@sessions.empty?
            @drain_requested = false
            @pop_sequence += 1
            dispatch(0_i64, Bytes[10], now, @pop_sequence, @notification_generation)
            return
          end
        elsif session = @sessions[id]?
          next if session.sync
          if command = session.commands.shift?
            if admit_command?(id, command.payload, now)
              dispatch(id, command.payload, now)
              return
            end
          end
        end
      end
    end

    private def dispatch(owner : Int64, command : Bytes, now : Time::Span, pop_sequence = 0_i64, generation = 0_i64)
      descriptor = Protocol.descriptor(command).not_nil!
      transaction = Transaction.new(owner, command, descriptor, now, pop_sequence, generation)
      transaction.job_id = @next_write + 1
      @active = transaction
      transaction.maintenance = descriptor.flags.includes?(Protocol::CommandFlags::Maintenance)
      if (session = @sessions[owner]?) && descriptor.flags.includes?(Protocol::CommandFlags::ScopeSend) && session.scope != Bytes[0x36, 0]
        transaction.step = :setup
        transaction.scoped = true
        transaction.upstream_write_id = send_upstream(session.scope, now)
      else
        send_command(transaction, now)
      end
    end

    private def send_command(transaction : Transaction, now : Time::Span)
      transaction.step = :command
      command = transaction.command
      forwarded = command[0] == 22 ? Protocol.normalize_device_query(command) : command
      transaction.upstream_write_id = send_upstream(forwarded, now)
    end

    private def send_upstream(payload : Bytes, now : Time::Span) : Int64
      @next_write += 1
      @upstream_writes[@next_write] = now + @config.write_timeout
      @actions << SendFrame.new(0_i64, @epoch, @next_write, payload)
      @counters[:upstream_commands] += 1
      @next_write
    end

    private def admit_command?(owner : Int64, command : Bytes, now : Time::Span) : Bool
      descriptor = Protocol.descriptor(command).not_nil!
      reason : UInt8? = nil
      # Reboot keeps the disruptive-operation lifecycle, but has no maintenance
      # permission, single-client, or idle-radio prerequisite.
      if descriptor.flags.includes?(Protocol::CommandFlags::Maintenance) && command[0] != 19
        if @sessions.size != 1 || @remote.occupied?(now) || @dm_ring.pending_count(now) > 0 || @signing.occupied?(now)
          reason = 4_u8
        end
      elsif command[0] == 2 && command[1] == 0 && !@dm_ring.available?(now)
        reason = 4_u8
      elsif descriptor.flags.includes?(Protocol::CommandFlags::RemoteLease)
        reason = 4_u8 unless @remote.reserve(owner, command, now)
      elsif command[0] == 33
        reason = 4_u8 unless @signing.start(owner, command, now)
      elsif command[0] == 34
        admission = @signing.begin_data(owner, command, now)
        reason = admission.table_full? ? 3_u8 : 4_u8 unless admission.allowed?
      elsif command[0] == 35
        reason = 4_u8 unless @signing.begin_finish(owner, command, now).allowed?
      end
      if reason
        reject(owner, reason, "resource unavailable opcode=#{command[0]}")
        return false
      end
      true
    end

    private def scope_response(transaction : Transaction, payload : Bytes, now : Time::Span)
      unless payload == Bytes[0] || (payload.size == 2 && payload[0] == 1)
        raise Protocol::ProtocolError.new("unexpected internal scope response")
      end
      log_response(transaction, payload, now)
      transaction.progress = now
      if transaction.step == :setup
        if payload[0] == 1
          @remote.rejected if transaction.descriptor.flags.includes?(Protocol::CommandFlags::RemoteLease)
          emit(transaction.owner, payload)
          finish_transaction(transaction)
        else
          send_command(transaction, now)
        end
      else
        raise Protocol::ProtocolError.new("scope restoration rejected after command response") unless payload[0] == 0
        finish_transaction(transaction)
      end
    end

    private def record_acceptance(transaction : Transaction, payload : Bytes, now : Time::Span)
      command = transaction.command
      if command[0] == 2 && command[1] == 0 && payload[0] == 6
        @dm_ring.accepted(payload, now)
      elsif transaction.descriptor.flags.includes?(Protocol::CommandFlags::RemoteLease)
        payload[0] == 6 ? @remote.accepted(payload, now) : @remote.rejected
      elsif command[0] == 33
        payload[0] == 0x13 ? @signing.accepted_start(payload, now) : @signing.rejected_start
      elsif command[0] == 34
        @signing.data_response(payload, now)
      elsif command[0] == 35
        @signing.finish_response(payload, now)
      end
    end

    private def finish_transaction(transaction : Transaction)
      @active = nil
      @signing.owner_gone(transaction.owner) unless @sessions.has_key?(transaction.owner)
      fail_epoch("maintenance operation completed") if transaction.maintenance
    end

    private def transaction_response(payload : Bytes, now : Time::Span)
      transaction = @active || raise Protocol::ProtocolError.new("ordinary response without owner")
      if write_id = transaction.upstream_write_id
        # A response proves the corresponding write completed even if the
        # writer fiber's completion event has not reached the broker yet.
        @upstream_writes.delete(write_id)
        transaction.upstream_write_id = nil
      end
      if transaction.step != :command
        scope_response(transaction, payload, now)
        return
      end
      disposition = Protocol.validate_response!(transaction.descriptor, payload, transaction.command, phase: transaction.contacts_started ? 1 : 0)
      transaction.response_frames += 1
      log_response(transaction, payload, now) if disposition.complete? && (transaction.owner != 0 || payload[0] != 10)
      if transaction.owner == 0
        raise Protocol::ProtocolError.new("internal inbox pop rejected") if payload[0] == 1
        @active = nil
        pop_result(transaction, payload)
        return
      end
      if transaction.descriptor.grammar.contacts? && payload[0] != 1
        if payload[0] == 2
          raise Protocol::ProtocolError.new("duplicate contacts start") if transaction.contacts_started
          transaction.contacts_started = true
        else
          raise Protocol::ProtocolError.new("contacts record before start") unless transaction.contacts_started
        end
      end
      response_write_id : Int64? = nil
      if session = @sessions[transaction.owner]?
        if transaction.command[0] == 22 && payload[0] == 0x0d
          session.target_version = transaction.command[1]
        end
        response_write_id = emit(session.id, payload)
      end
      transaction.progress = now
      if disposition.complete?
        record_acceptance(transaction, payload, now)
        if transaction.scoped
          transaction.step = :restore
          transaction.upstream_write_id = send_upstream(Bytes[0x36, 0], now)
        elsif transaction.maintenance && response_write_id
          # Closing the endpoint in the same action batch can discard this
          # real firmware result. End the epoch only after its writer confirms.
          transaction.step = :maintenance_result
          transaction.maintenance_write_id = response_write_id
        else
          finish_transaction(transaction)
        end
      end
    end

    private def log_response(transaction : Transaction, payload : Bytes, now : Time::Span)
      queued = @sessions[transaction.owner]?.try(&.commands.size) || 0
      @actions << Diagnostic.new("epoch=#{@epoch} session=#{transaction.owner} job=#{transaction.job_id} " \
                                 "command=#{transaction.descriptor.name} command_bytes=#{transaction.command.size} " \
                                 "step=#{transaction.step} response=#{Protocol.response_name(payload[0])} response_bytes=#{payload.size} " \
                                 "response_frames=#{transaction.response_frames} queued_commands=#{queued} elapsed_ms=#{(now - transaction.started).total_milliseconds.round(3)}")
    end

    private def push(payload : Bytes, now : Time::Span)
      case payload[0]
      when 0x83
        request_drain
      when 0x82
        @dm_ring.confirm(payload)
        @sessions.keys.each { |id| emit(id, payload) }
      when 0x85, 0x86, 0x87, 0x89, 0x8b, 0x8c, 0x8d
        if payload[0] == 0x8b && (transaction = @active) && transaction.command[0] == 39 && transaction.command.size == 4 && payload[2, 6] == @self_key[0, 6]
          transaction_response(payload, now)
        elsif owner = @remote.match(payload, now)
          emit(owner, payload)
        else
          @counters[:orphan_remote] += 1
          if !@last_orphan_log || now - @last_orphan_log.not_nil! >= 1.second
            @last_orphan_log = now
            @actions << Diagnostic.new("epoch=#{@epoch} orphan_remote code=#{payload[0]} count=#{@counters[:orphan_remote]}")
          end
        end
      else
        unless {0x80_u8, 0x81_u8, 0x84_u8, 0x88_u8, 0x8a_u8, 0x8e_u8, 0x8f_u8, 0x90_u8}.includes?(payload[0])
          @counters[:unknown_pushes] += 1
          if !@last_unknown_log || now - @last_unknown_log.not_nil! >= 1.second
            @last_unknown_log = now
            @actions << Diagnostic.new("epoch=#{@epoch} unknown_push code=#{payload[0]} count=#{@counters[:unknown_pushes]}")
          end
        end
        @sessions.keys.each { |id| emit(id, payload) }
      end
    end

    private def pop_result(transaction : Transaction, payload : Bytes)
      if payload[0] == 10
        @sessions.values.each do |session|
          if (sync = session.sync) && sync.minimum_pop <= transaction.pop_sequence
            session.sync = nil
            emit(session.id, Bytes[10])
          end
        end
        # A newer notification or sync must survive an older empty observation.
        @drain_requested = @notification_generation > transaction.notification_generation || @sessions.values.any?(&.sync)
      else
        @counters[:inbox_pops] += 1
        if @sessions.empty?
          @orphan = payload
        else
          fan_out(payload)
          @drain_requested = true
        end
      end
    end

    private def fan_out(payload : Bytes)
      @sessions.values.each do |session|
        if session.inbox.size >= @config.inbox_entries || session.inbox_bytes + payload.size > @config.inbox_bytes
          remove(session.id, "inbox overflow")
          next
        end
        was_empty = session.inbox.empty?
        session.inbox << payload
        session.inbox_bytes += payload.size
        if session.sync
          session.sync = nil
          deliver_item(session)
        elsif was_empty
          emit(session.id, Bytes[0x83])
        end
      end
    end

    private def deliver_item(session : Session)
      item = session.inbox.first
      if emit(session.id, Protocol.downgrade_inbox(item, session.target_version))
        session.inbox.shift
        session.inbox_bytes -= item.size
      end
    end
  end
end
