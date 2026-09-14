require "./config"
require "./session"
require "./protocol"
require "./leases"

class MeshCoreTCPMux
  # Namespace for the TCP multiplexer: transport, protocol validation, and per-client state.
  class Broker
    # Owns all mutable protocol state and schedules clients onto one companion connection.
    # Runtime delivers socket events here and executes the returned actions; the broker does no I/O.
    class Transaction
      # Tracks the sole in-flight upstream command, its response grammar, and its owner.
      # Also tracks hidden scope setup/restoration and write completion before releasing ownership.
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
                     @pop_sequence = 0_i64, @notification_generation = 0_i64) : Nil
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
                   @orphan : Bytes? = nil) : Nil
      @last_poll = now
      @signing = SigningLease.new(@config.signing_timeout)
    end

    def take_actions : Array(Action)
      # Transfer pending effects to Runtime and start a fresh batch; no socket operations occur here.
      result = @actions
      @actions = Array(Action).new
      result
    end

    def admit(id : Int64, now : Time::Span) : Nil
      # Create an independent client view and include it in round-robin scheduling. ID 0 is reserved
      # for the broker's physical inbox consumer; downstream clients have positive IDs.
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

    def client_frame(id : Int64, payload : Bytes, now : Time::Span) : Nil
      # Queue a complete downstream command without allowing an unbounded client backlog.
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

    def client_closed(id : Int64, now : Time::Span, reason = "client disconnected", category = :normal) : Nil
      @now = now
      remove(id, reason, category)
      # An active command remains owned until its real terminator, even though
      # the owner has left. No subsequent command can inherit those replies.
      schedule(now)
    end

    def written(id : Int64, epoch : Int64, write_id : Int64, now : Time::Span) : Nil
      # Release output budgets only for completions from this epoch; stale writer events cannot affect new state.
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

    def write_failed(id : Int64, epoch : Int64, reason : String, now : Time::Span) : Nil
      return unless epoch == @epoch && !@failed
      if id == 0
        fail_epoch("upstream write failed: #{reason}")
      else
        remove(id, "writer failed: #{reason}")
        schedule(now)
      end
    end

    def upstream_frame(payload : Bytes, now : Time::Span) : Nil
      @now = now
      return if @failed
      return unless check_active_deadline(now)
      @counters[:upstream_frames] += 1
      Protocol.validate_response_shape!(payload)
      if payload[0] >= 0x80 # 0x80 starts the asynchronous push-code range.
        push(payload, now)
      else
        transaction_response(payload, now)
      end
      schedule(now)
    rescue ex : Protocol::ProtocolError
      fail_epoch(ex.message || "invalid upstream response")
    end

    def tick(now : Time::Span) : Nil
      # Expire client waits and radio reservations, enforce deadlines, and periodically check the physical inbox.
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
            reject(session.id, 4_u8, "virtual sync deadline") # BAD_STATE.
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

    def fail_epoch(reason : String) : Nil
      # Discard uncertain upstream ownership and disconnect all clients. Never replay a possibly executed command.
      return if @failed
      @failed = true
      @actions << Diagnostic.new("epoch=#{@epoch} failed reason=#{reason.inspect}")
      @sessions.keys.each { |id| remove(id, "upstream epoch failed") }
      @active = nil
      @upstream_writes.clear
      @actions << Diagnostic.new("epoch=#{@epoch} summary #{@counters.map { |name, count| "#{name}=#{count}" }.join(' ')}")
      @actions << EndEpoch.new(@epoch, reason)
    end

    private def request_drain : Nil
      # Record new evidence that the physical inbox needs checking. Generations prevent an older empty
      # reply from consuming a newer MSG_WAITING notification or client sync request.
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

    private def remove(id : Int64, reason : String, category = :normal) : Nil
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
      # Budget the three-byte TCP envelope as well as the payload, then request a downstream write.
      # Return its ID for completion tracking, or nil when the client is gone or too slow.
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

    private def reject(id : Int64, reason : UInt8, detail : String) : Nil
      # Return native ERR (1) plus its reason byte; this rejection never goes upstream.
      @counters[:rejections] += 1
      @actions << Diagnostic.new("epoch=#{@epoch} session=#{id} rejection=#{reason} reason=#{detail.inspect}")
      emit(id, Bytes[1, reason])
    end

    private def schedule(now : Time::Span) : Nil
      # Resolve locally virtualized commands first, respecting each client's FIFO, then rotate among
      # clients and the internal inbox consumer. Only one ordinary upstream transaction may be active.
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
            reject(session.id, 4_u8, "command queue age") # BAD_STATE.
          elsif command.payload[0] == 10                  # 10 = SYNC_NEXT_MESSAGE.
            session.commands.shift
            if session.inbox.empty?
              session.sync = PendingSync.new(@pop_sequence + 1, now + 5.seconds)
              request_drain
              break
            else
              deliver_item(session)
            end
          elsif command.payload[0] == 54 # 54 = SET_FLOOD_SCOPE_KEY.
            session.commands.shift
            session.scope = command.payload.dup
            emit(session.id, Bytes[0])                                  # OK: virtual scope update accepted.
          elsif command.payload[0] == 23 && !@config.private_key_export # 23 = EXPORT_PRIVATE_KEY.
            session.commands.shift
            @counters[:rejections] += 1
            @actions << Diagnostic.new("epoch=#{@epoch} session=#{session.id} command=export_private_key rejection=disabled")
            emit(session.id, Bytes[0x0f])                                            # DISABLED: key export is not permitted.
          elsif {24_u8, 51_u8}.includes?(command.payload[0]) && !@config.maintenance # IMPORT_PRIVATE_KEY, FACTORY_RESET.
            session.commands.shift
            reject(session.id, 1_u8, "maintenance disabled") # UNSUPPORTED_CMD.
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
            dispatch(0_i64, Bytes[10], now, @pop_sequence, @notification_generation) # SYNC_NEXT_MESSAGE.
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

    private def dispatch(owner : Int64, command : Bytes, now : Time::Span, pop_sequence = 0_i64, generation = 0_i64) : Nil
      # Claim upstream ownership before sending anything. A nondefault client scope wraps the command
      # in hidden SET_FLOOD_SCOPE_KEY (0x36) setup and default-scope restoration (mode 0, no key).
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

    private def send_command(transaction : Transaction, now : Time::Span) : Nil
      # DEVICE_QUERY (22) is forwarded at our native target; the original client target stays in the transaction.
      transaction.step = :command
      command = transaction.command
      forwarded = command[0] == 22 ? Protocol.normalize_device_query(command) : command # 22 = DEVICE_QUERY.
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
      # Apply permission and shared-radio-resource checks just before dispatch, not when commands are queued.
      # Native rejection reasons are BAD_STATE (4) for conflicts and TABLE_FULL (3) for signing capacity.
      descriptor = Protocol.descriptor(command).not_nil!
      reason : UInt8? = nil
      # Reboot keeps the disruptive-operation lifecycle, but has no maintenance
      # permission, single-client, or idle-radio prerequisite.
      if descriptor.flags.includes?(Protocol::CommandFlags::Maintenance) && command[0] != 19 # 19 = REBOOT.
        if @sessions.size != 1 || @remote.occupied?(now) || @dm_ring.pending_count(now) > 0 || @signing.occupied?(now)
          reason = 4_u8 # BAD_STATE: shared resource is unavailable.
        end
      elsif command[0] == 2 && command[1] == 0 && !@dm_ring.available?(now) # SEND_TXT_MSG (2), plain text (type 0).
        reason = 4_u8                                                       # BAD_STATE: shared resource is unavailable.
      elsif descriptor.flags.includes?(Protocol::CommandFlags::RemoteLease)
        reason = 4_u8 unless @remote.reserve(owner, command, now) # BAD_STATE: shared resource is unavailable.
      elsif command[0] == 33                                      # 33 = SIGN_START.
        reason = 4_u8 unless @signing.start(owner, command, now)  # BAD_STATE: shared resource is unavailable.
      elsif command[0] == 34                                      # 34 = SIGN_DATA.
        admission = @signing.begin_data(owner, command, now)
        reason = admission.table_full? ? 3_u8 : 4_u8 unless admission.allowed?
      elsif command[0] == 35                                                     # 35 = SIGN_FINISH.
        reason = 4_u8 unless @signing.begin_finish(owner, command, now).allowed? # BAD_STATE: shared resource is unavailable.
      end
      if reason
        reject(owner, reason, "resource unavailable opcode=#{command[0]}")
        return false
      end
      true
    end

    private def scope_response(transaction : Transaction, payload : Bytes, now : Time::Span) : Nil
      # Consume internal scope replies instead of exposing them to the client. OK (0) advances the wrapper;
      # ERR (1) during setup rejects the command, while restoration failure makes shared state uncertain.
      unless payload == Bytes[0] || (payload.size == 2 && payload[0] == 1) # 1 = ERR.
        raise Protocol::ProtocolError.new("unexpected internal scope response")
      end
      log_response(transaction, payload, now)
      transaction.progress = now
      if transaction.step == :setup
        if payload[0] == 1 # 1 = ERR.
          @remote.rejected if transaction.descriptor.flags.includes?(Protocol::CommandFlags::RemoteLease)
          emit(transaction.owner, payload)
          finish_transaction(transaction)
        else
          send_command(transaction, now)
        end
      else
        raise Protocol::ProtocolError.new("scope restoration rejected after command response") unless payload[0] == 0 # 0 = OK.
        finish_transaction(transaction)
      end
    end

    private def record_acceptance(transaction : Transaction, payload : Bytes, now : Time::Span) : Nil
      # A SENT (6) reply accepts a radio operation but does not prove delivery. Retain its radio reservation;
      # signing commands instead update their incremental signing phase from the actual firmware reply.
      command = transaction.command
      if command[0] == 2 && command[1] == 0 && payload[0] == 6 # SEND_TXT_MSG (2), plain text (type 0), accepted with SENT (6).
        @dm_ring.accepted(payload, now)
      elsif transaction.descriptor.flags.includes?(Protocol::CommandFlags::RemoteLease)
        payload[0] == 6 ? @remote.accepted(payload, now) : @remote.rejected                  # 6 = SENT.
      elsif command[0] == 33                                                                 # 33 = SIGN_START.
        payload[0] == 0x13 ? @signing.accepted_start(payload, now) : @signing.rejected_start # 0x13 = SIGN_START.
      elsif command[0] == 34                                                                 # 34 = SIGN_DATA.
        @signing.data_response(payload, now)
      elsif command[0] == 35 # 35 = SIGN_FINISH.
        @signing.finish_response(payload, now)
      end
    end

    private def finish_transaction(transaction : Transaction) : Nil
      # Release ordinary response ownership. Disruptive operations end the whole connection epoch.
      @active = nil
      @signing.owner_gone(transaction.owner) unless @sessions.has_key?(transaction.owner)
      fail_epoch("maintenance operation completed") if transaction.maintenance
    end

    private def transaction_response(payload : Bytes, now : Time::Span) : Nil
      # Keep each ordinary reply with its active owner, including when that client has disconnected.
      # Contacts retain ownership until END; scoped commands retain it until hidden restoration succeeds.
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
      log_response(transaction, payload, now) if disposition.complete? && (transaction.owner != 0 || payload[0] != 10) # 10 = NO_MORE_MESSAGES.
      if transaction.owner == 0
        raise Protocol::ProtocolError.new("internal inbox pop rejected") if payload[0] == 1 # 1 = ERR.
        @active = nil
        pop_result(transaction, payload)
        return
      end
      if transaction.descriptor.grammar.contacts? && payload[0] != 1 # 1 = ERR.
        if payload[0] == 2                                           # 2 = CONTACTS_START.
          raise Protocol::ProtocolError.new("duplicate contacts start") if transaction.contacts_started
          transaction.contacts_started = true
        else
          raise Protocol::ProtocolError.new("contacts record before start") unless transaction.contacts_started
        end
      end
      response_write_id : Int64? = nil
      if session = @sessions[transaction.owner]?
        if transaction.command[0] == 22 && payload[0] == 0x0d # 22 = DEVICE_QUERY; 0x0d = DEVICE_INFO.
          session.target_version = transaction.command[1]
        end
        response_write_id = emit(session.id, payload)
      end
      transaction.progress = now
      if disposition.complete?
        record_acceptance(transaction, payload, now)
        if transaction.scoped
          transaction.step = :restore
          transaction.upstream_write_id = send_upstream(Bytes[0x36, 0], now) # SET_FLOOD_SCOPE_KEY: restore default scope.
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

    private def log_response(transaction : Transaction, payload : Bytes, now : Time::Span) : Nil
      # Log routing and timing metadata only, excluding message bodies, keys, and identity fields.
      queued = @sessions[transaction.owner]?.try(&.commands.size) || 0
      @actions << Diagnostic.new("epoch=#{@epoch} session=#{transaction.owner} job=#{transaction.job_id} " \
                                 "command=#{transaction.descriptor.name} command_bytes=#{transaction.command.size} " \
                                 "step=#{transaction.step} response=#{Protocol.response_name(payload[0])} response_bytes=#{payload.size} " \
                                 "response_frames=#{transaction.response_frames} queued_commands=#{queued} elapsed_ms=#{(now - transaction.started).total_milliseconds.round(3)}")
    end

    private def push(payload : Bytes, now : Time::Span) : Nil
      # MSG_WAITING requests a physical drain; SEND_CONFIRMED settles the DM ring and is broadcast.
      # Remote result pushes go only to their lease owner. Self telemetry is exceptional: its push-shaped
      # reply completes the active four-byte SEND_TELEMETRY_REQ (39), matched by the six-byte self-key prefix.
      case payload[0]
      when 0x83 # MSG_WAITING.
        request_drain
      when 0x82 # SEND_CONFIRMED.
        @dm_ring.confirm(payload)
        @sessions.keys.each { |id| emit(id, payload) }
      when 0x85, 0x86, 0x87, 0x89, 0x8b, 0x8c, 0x8d
        # LOGIN_SUCCESS, LOGIN_FAILURE, STATUS_RESPONSE, TRACE_DATA, TELEMETRY_RESPONSE, BINARY_RESPONSE,
        # PATH_DISCOVERY_RESPONSE.
        if payload[0] == 0x8b && (transaction = @active) && transaction.command[0] == 39 && transaction.command.size == 4 && payload[2, 6] == @self_key[0, 6] # 0x8b = TELEMETRY_RESPONSE; 39 = SEND_TELEMETRY_REQ.
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
        # Broadcast ADVERT (0x80), PATH_UPDATED (0x81), RAW_DATA (0x84), LOG_RX_DATA (0x88),
        # NEW_ADVERT (0x8a), CONTROL_DATA (0x8e), CONTACT_DELETED (0x8f), and CONTACTS_FULL (0x90).
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

    private def pop_result(transaction : Transaction, payload : Bytes) : Nil
      # Turn one physical inbox result into independent client copies. NO_MORE_MESSAGES (10) only
      # satisfies waits old enough to be covered by this pop; newer waits must survive.
      if payload[0] == 10 # 10 = NO_MORE_MESSAGES.
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

    private def fan_out(payload : Bytes) : Nil
      # Share immutable inbox payloads across clients but maintain separate queue positions and budgets.
      # An empty-to-nonempty transition sends MSG_WAITING (0x83), unless a waiting sync can receive immediately.
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

    private def deliver_item(session : Session) : Nil
      # Remove a client's oldest inbox item only after successfully enqueueing its version-adjusted reply.
      item = session.inbox.first
      if emit(session.id, Protocol.downgrade_inbox(item, session.target_version))
        session.inbox.shift
        session.inbox_bytes -= item.size
      end
    end
  end
end
