require "socket"
require "log"
require "./broker"
require "./config"
require "./dedicated_client_slot"
require "./frame_codec"
require "./startup"
require "./transport"
require "./wire_log"

class MeshCoreTCPMux
  # Namespace for the TCP multiplexer: transport, protocol validation, and per-client state.
  class Runtime
    # Owns the listener, upstream epochs, socket fibers, and Broker side effects.
    # The Broker is invoked only by this fiber.
    alias ConnectResult = TCPSocket | Exception
    LOGGER = Log

    enum ListenerKind
      # Multi-client accepts anonymous concurrent sessions; dedicated-client
      # accepts replace the prior attachment for one stable port identity.
      MultiClient
      DedicatedClient
    end

    record ListenerBinding, server : TCPServer, kind : ListenerKind, dedicated_slot_id : Int32?
    record AcceptedSocket, socket : TCPSocket, kind : ListenerKind, dedicated_slot_id : Int32?
    record ClientRoute, kind : ListenerKind, dedicated_slot_id : Int32?

    @stopping = Channel(Nil).new
    @finished = Channel(Nil).new(1)
    @accepted = Channel(AcceptedSocket).new
    @accept_done = Channel(Nil).new(1)
    @accept_error : Exception? = nil
    @running = false
    @has_run = false
    @upstream_connection_usable = false
    @stop_requested = false
    @listeners = Array(ListenerBinding).new
    @upstream : Transport::Endpoint? = nil
    @clients = Hash(Int64, Transport::Endpoint).new
    @client_routes = Hash(Int64, ClientRoute).new
    @next_session = 0_i64
    @next_epoch = 0_i64
    @orphan : Bytes? = nil
    @orphan_key : Bytes? = nil
    @dedicated_slots = Hash(Int32, DedicatedClientSlot).new
    @dedicated_slots_key : Bytes? = nil
    @radio_state = CompanionRadioState.new
    @radio_state_key : Bytes? = nil
    @last_malformed_log : Time::Span? = nil
    @suppressed_malformed = 0_u64

    def initialize(@host : String, @port : Int32, @config : Config) : Nil
      @config.listen_dedicated_client_ports.each do |listen_port|
        @dedicated_slots[listen_port] = DedicatedClientSlot.new(listen_port, listen_port)
      end
    end

    def run : Nil
      # Own the listener and every upstream epoch until shutdown. Unexpected
      # acceptor failure is process-fatal; propagate it only after cleanup so
      # BinaryEntrypoint exits nonzero and a supervisor can restart the daemon.
      raise "runtime is single-use" if @has_run
      @has_run = true
      @running = true
      begin
        @listeners = bind_listeners
        @listeners.each do |listener|
          LOGGER.info do
            "event=listener.started kind=#{listener.kind.to_s.underscore} " \
            "dedicated_slot_id=#{listener.dedicated_slot_id || "none"} " \
            "address=#{socket_address(listener.server.local_address)} upstream=#{@host}:#{@port}"
          end
          start_acceptor(listener)
        end
        backoff = 500.milliseconds

        until @stop_requested
          socket = connect_upstream
          unless socket
            break if @stop_requested
            delay = jitter(backoff)
            LOGGER.info { "event=upstream.reconnect_scheduled remote=#{@host}:#{@port} delay_ms=#{delay.total_milliseconds.round}" }
            wait_with_refusal(delay)
            backoff = {backoff * 2, 30.seconds}.min
            next
          end
          @next_epoch += 1
          upstream_events = Channel(Transport::Event).new
          endpoint = Transport::Endpoint.new(
            socket, 0_i64,
            FrameCodec::COMPANION_TO_CLIENT_MARKER,
            FrameCodec::CLIENT_TO_COMPANION_MARKER,
            upstream_events,
            @config.frame_timeout,
            @config.write_timeout,
            16
          )
          @upstream = endpoint

          begin
            startup = synchronize(endpoint, upstream_events, @next_epoch)
            break if @stop_requested
            self_key = startup.self_key.not_nil!
            orphan = orphan_for(self_key)
            prepare_dedicated_slots(self_key)
            radio_state = radio_state_for(self_key)
            broker = Broker.new(@next_epoch, self_key, @config, Clock.now, orphan, @dedicated_slots, radio_state)
            ready_at = Clock.now
            @upstream_connection_usable = true
            LOGGER.info { "event=upstream.ready epoch=#{@next_epoch} remote=#{socket_address(socket.remote_address)} #{startup.identification}" }
            run_epoch(broker, endpoint, upstream_events)
            drain_response_debt(broker, endpoint, upstream_events) if broker.response_debt && !@stop_requested
            @orphan = broker.orphan.try(&.dup)
            @orphan_key = @orphan ? self_key : nil
            backoff = 500.milliseconds if Clock.now - ready_at >= 30.seconds
          rescue ex
            LOGGER.error(exception: ex) { "event=upstream.epoch_failed epoch=#{@next_epoch}" }
          ensure
            endpoint.stop
            @upstream = nil
            close_all_clients
          end

          break if @stop_requested
          delay = jitter(backoff)
          LOGGER.info { "event=upstream.reconnect_scheduled remote=#{@host}:#{@port} delay_ms=#{delay.total_milliseconds.round}" }
          wait_with_refusal(delay)
          backoff = {backoff * 2, 30.seconds}.min
        end
        if error = @accept_error
          raise error
        end
      ensure
        @stop_requested = true
        @stopping.close unless @stopping.closed?
        @listeners.each { |listener| listener.server.close rescue nil }
        @upstream.try &.stop
        close_all_clients
        @listeners.size.times { @accept_done.receive }
        @listeners.clear
        unread = @dedicated_slots.values.sum(&.offline_queue.size)
        LOGGER.info { "event=dedicated_queues.volatile_discard process_stopping=true entries=#{unread}" } unless unread.zero?
        @running = false
        LOGGER.info { "event=runtime.stopped" }
        @finished.send(nil)
      end
    end

    def stop : Nil
      # Request cancellation and join the runtime from an external fiber.
      # The acceptor uses request_stop without joining, avoiding a circular wait.
      request_stop unless @stop_requested
      @finished.receive if @running
    end

    protected def create_listener(port : Int32) : TCPServer
      # Separate listener creation from its ownership loop so socket faults can
      # be injected in specs without exhausting machine-wide descriptors.
      TCPServer.new(@config.listen_host, port)
    end

    private def bind_listeners : Array(ListenerBinding)
      # Bind the complete configured set before starting any acceptor. Partial
      # identity availability is unsafe because clients could reach the wrong mode.
      listeners = Array(ListenerBinding).new
      begin
        listeners << ListenerBinding.new(
          create_listener(@config.listen_multi_client_port),
          ListenerKind::MultiClient,
          nil
        )
        @config.listen_dedicated_client_ports.each do |listen_port|
          listeners << ListenerBinding.new(
            create_listener(listen_port),
            ListenerKind::DedicatedClient,
            listen_port
          )
        end
      rescue ex
        listeners.each { |listener| listener.server.close rescue nil }
        raise ex
      end
      listeners
    end

    private def request_stop : Nil
      # Wake every lifecycle wait and interrupt local socket I/O. Never send a
      # radio command or wait for another fiber here: the acceptor also calls us.
      return if @stop_requested
      LOGGER.info { "event=runtime.stop_requested clients=#{@clients.size} epoch=#{@next_epoch}" }
      @stop_requested = true
      @stopping.close
      @listeners.each { |listener| listener.server.close rescue nil }
      @upstream.try &.socket.close
      @clients.each_value { |endpoint| endpoint.socket.close rescue nil }
    end

    private def start_acceptor(listener : ListenerBinding) : Nil
      # Transfer each accepted socket through a cancellable handoff. A failed
      # accept must stop the process, not leave a live daemon with a dead listener.
      spawn do
        begin
          loop do
            socket = listener.server.accept
            accepted_socket = AcceptedSocket.new(socket, listener.kind, listener.dedicated_slot_id)
            accepted = select
            when @accepted.send(accepted_socket)
              true
            when @stopping.receive?
              false
            end
            unless accepted
              socket.close
              break
            end
          end
        rescue ex
          unless @stop_requested
            @accept_error = ex
            LOGGER.error(exception: ex) { "event=listener.failed" }
            request_stop
          end
        ensure
          @accept_done.send(nil)
        end
      end
    end

    private def connect_upstream : TCPSocket?
      # Refuse downstream sockets during the bounded connect attempt. Join the
      # connector even on cancellation so a late socket cannot leak or start an epoch.
      LOGGER.info { "event=upstream.connecting remote=#{@host}:#{@port}" }
      result = Channel(ConnectResult).new(1)
      done = Channel(Nil).new(1)
      spawn do
        begin
          socket = TCPSocket.new(
            @host,
            @port,
            dns_timeout: @config.connect_timeout,
            connect_timeout: @config.connect_timeout
          )
          result.send(socket)
        rescue ex
          result.send(ex)
        ensure
          done.send(nil)
        end
      end

      loop do
        select
        when connected = result.receive
          done.receive
          if @stop_requested
            connected.close if connected.is_a?(TCPSocket)
            return nil
          end
          if connected.is_a?(Exception)
            LOGGER.warn(exception: connected) { "event=upstream.connect_failed remote=#{@host}:#{@port}" }
            return nil
          end
          LOGGER.info do
            "event=upstream.connected local=#{socket_address(connected.local_address)} remote=#{socket_address(connected.remote_address)}"
          end
          return connected
        when accepted = @accepted.receive
          socket = accepted.socket
          LOGGER.info { "event=client.refused remote=#{socket_address(socket.remote_address)} reason=upstream_connecting" }
          socket.close
        when @stopping.receive?
          connected = result.receive
          done.receive
          connected.close if connected.is_a?(TCPSocket)
          return nil
        end
      end
    end

    private def synchronize(endpoint : Transport::Endpoint, events : Channel(Transport::Event), epoch : Int64) : Startup
      # Keep startup responses private and refuse clients until the complete
      # fence and scope reset succeed. Only validated frames establish readiness.
      endpoint.start # reader is running before any probe is enqueued
      startup = Startup.new(Clock.now, @config.startup_timeout)
      Startup.probes.each_with_index do |payload, index|
        write = Transport::Write.new(epoch, -(index + 1).to_i64, payload)
        LOGGER.info { WireLog.upstream(epoch, :tx, payload) }
        raise Startup::Error.new("startup writer queue full") unless endpoint.enqueue(write)
      end

      until startup.ready?
        select
        when event = events.receive
          case event
          when Transport::Frame
            LOGGER.info { WireLog.upstream(epoch, :rx, event.payload) }
            if command = startup.receive(event.payload, Clock.now)
              write = Transport::Write.new(epoch, -7_i64, command)
              LOGGER.info { WireLog.upstream(epoch, :tx, command) }
              raise Startup::Error.new("startup writer queue full") unless endpoint.enqueue(write)
            end
          when Transport::Closed
            raise Startup::Error.new("upstream closed: #{event.reason}")
          when Transport::WriteFailed
            raise Startup::Error.new("upstream write failed: #{event.reason}")
          when Transport::Written
            # Receipt is useful only for failure detection during startup.
          end
        when accepted = @accepted.receive
          socket = accepted.socket
          LOGGER.info { "event=client.refused remote=#{socket_address(socket.remote_address)} reason=upstream_starting epoch=#{epoch}" }
          socket.close
        when timeout(100.milliseconds)
          startup.check_deadline(Clock.now)
        when @stopping.receive?
          raise Startup::Error.new("runtime stopping")
        end
        startup.check_deadline(Clock.now) unless startup.ready?
      end
      startup
    end

    private def run_epoch(broker : Broker, upstream : Transport::Endpoint, upstream_events : Channel(Transport::Event)) : Nil
      # This fiber alone calls Broker. Apply actions after each event, then tick
      # deadlines even during continuous traffic; uncertain execution is never replayed.
      downstream_events = Channel(Transport::Event).new
      ended = apply_actions(broker, upstream)
      until ended || @stop_requested
        select
        when accepted = @accepted.receive
          admit(accepted, broker, downstream_events)
        when event = upstream_events.receive
          handle_upstream(event, broker)
        when event = downstream_events.receive
          handle_downstream(event, broker)
        when timeout(100.milliseconds)
          # Tick below, as for every other event.
        when @stopping.receive?
          broker.fail_epoch("process shutdown")
          apply_actions(broker, upstream)
          return
        end
        ended = apply_actions(broker, upstream)
        break if ended
        broker.tick(Clock.now)
        ended = apply_actions(broker, upstream)
      end
    rescue ex
      unless @stop_requested
        broker.fail_epoch(ex.message || ex.class.name)
        apply_actions(broker, upstream)
      end
    end

    private def drain_response_debt(
      broker : Broker,
      upstream : Transport::Endpoint,
      upstream_events : Channel(Transport::Event),
    ) : Nil
      # A timed-out command may still be running inside an asynchronous
      # companion. Keep its original TCP generation open and refuse downstream
      # work until its response grammar terminates. Reconnecting first can let
      # an old handler write an untagged reply into the replacement connection.
      transaction = broker.response_debt.not_nil!
      unless @upstream_connection_usable
        upstream.stop
        quarantine_unresolved_response_debt(
          broker.epoch,
          "the upstream connection ended with an ordinary response still outstanding"
        )
        return
      end
      LOGGER.warn do
        "event=upstream.response_debt_draining epoch=#{broker.epoch} " \
        "command=#{transaction.descriptor.name} opcode=#{transaction.command[0]} " \
        "owner=#{transaction.owner} step=#{transaction.step.to_s.underscore} " \
        "deadline_ms=#{@config.response_timeout.total_milliseconds.round}"
      end

      deadline = Clock.now + @config.response_timeout
      while broker.response_debt && !@stop_requested && Clock.now < deadline
        select
        when event = upstream_events.receive
          case event
          when Transport::Frame
            LOGGER.info { WireLog.upstream(broker.epoch, :rx, event.payload) }
            completed = broker.drain_uncertain_frame(event.payload, Clock.now)
            apply_actions(broker, upstream)
            if completed
              LOGGER.info { "event=upstream.response_debt_drained epoch=#{broker.epoch}" }
            end
          when Transport::Closed
            upstream.stop
            quarantine_unresolved_response_debt(
              broker.epoch,
              "upstream closed while an old command could still complete: #{event.reason}"
            )
            return
          when Transport::WriteFailed
            upstream.stop
            quarantine_unresolved_response_debt(
              broker.epoch,
              "upstream writer failed while an old command could still complete: #{event.reason}"
            )
            return
          when Transport::Written
            # The command write was already proven complete before its response
            # deadline began. Later writer notifications carry no ownership.
          end
        when accepted = @accepted.receive
          socket = accepted.socket
          LOGGER.info do
            "event=client.refused remote=#{socket_address(socket.remote_address)} " \
            "reason=upstream_response_debt epoch=#{broker.epoch}"
          end
          socket.close
        when @stopping.receive?
          return
        when timeout(100.milliseconds)
          # Recheck the monotonic drain deadline even when the poisoned
          # connection remains silent and open.
        end
      end
      if broker.response_debt && !@stop_requested
        upstream.stop
        quarantine_unresolved_response_debt(
          broker.epoch,
          "no terminal response arrived during the bounded poisoned drain"
        )
      end
    rescue ex : Protocol::ProtocolError
      upstream.stop
      quarantine_unresolved_response_debt(
        broker.epoch,
        "response debt received an invalid or mismatched ordinary frame: #{ex.message}"
      )
    end

    private def quarantine_unresolved_response_debt(epoch : Int64, reason : String) : Nil
      # A companion whose command tasks outlive TCP replacement exposes no
      # protocol-level completion signal after the old socket is lost. Keep the
      # replacement connection absent for one full response horizon so a late
      # handler writes into the poisoned old generation, then recover through
      # the normal startup fence. This is a bounded operational quarantine, not
      # proof that an arbitrarily delayed third-party handler has terminated.
      deadline = Clock.now + @config.response_timeout
      LOGGER.error do
        "event=upstream.response_debt_unresolved epoch=#{epoch} " \
        "recovery=bounded_quarantine quarantine_ms=#{@config.response_timeout.total_milliseconds.round} " \
        "reason=#{reason.inspect}"
      end
      until @stop_requested || Clock.now >= deadline
        select
        when accepted = @accepted.receive
          socket = accepted.socket
          LOGGER.info do
            "event=client.refused remote=#{socket_address(socket.remote_address)} " \
            "reason=unresolved_upstream_response epoch=#{epoch}"
          end
          socket.close
        when @stopping.receive?
          return
        when timeout(100.milliseconds)
          # Recheck the monotonic quarantine deadline.
        end
      end
      unless @stop_requested
        LOGGER.warn do
          "event=upstream.response_debt_quarantine_complete epoch=#{epoch} " \
          "recovery=reconnect"
        end
      end
    end

    private def admit(accepted : AcceptedSocket, broker : Broker, events : Channel(Transport::Event)) : Nil
      socket = accepted.socket
      @next_session += 1
      id = @next_session
      LOGGER.info do
        "event=client.connected epoch=#{broker.epoch} session=#{id} " \
        "kind=#{accepted.kind.to_s.underscore} dedicated_slot_id=#{accepted.dedicated_slot_id || "none"} " \
        "local=#{socket_address(socket.local_address)} remote=#{socket_address(socket.remote_address)}"
      end
      endpoint = Transport::Endpoint.new(
        socket, id,
        FrameCodec::CLIENT_TO_COMPANION_MARKER,
        FrameCodec::COMPANION_TO_CLIENT_MARKER,
        events,
        @config.frame_timeout,
        @config.write_timeout,
        @config.output_frames
      )
      @clients[id] = endpoint
      @client_routes[id] = ClientRoute.new(accepted.kind, accepted.dedicated_slot_id)
      endpoint.start
      broker.admit(id, Clock.now, accepted.dedicated_slot_id)
    end

    private def handle_upstream(event : Transport::Event, broker : Broker) : Nil
      now = Clock.now
      case event
      when Transport::Frame
        LOGGER.info { WireLog.upstream(broker.epoch, :rx, event.payload) }
        broker.upstream_frame(event.payload, now)
        @upstream_connection_usable = false if broker.failed
      when Transport::Closed
        @upstream_connection_usable = false
        LOGGER.warn { "event=upstream.closed epoch=#{broker.epoch} reason=#{event.reason.inspect}" }
        broker.fail_epoch("upstream closed: #{event.reason}")
      when Transport::Written
        LOGGER.debug { "event=upstream.write_completed epoch=#{event.epoch} write_id=#{event.write_id}" }
        broker.written(0_i64, event.epoch, event.write_id, now)
      when Transport::WriteFailed
        @upstream_connection_usable = false
        LOGGER.error { "event=upstream.write_failed epoch=#{event.epoch} write_id=#{event.write_id} reason=#{event.reason.inspect}" }
        broker.write_failed(0_i64, event.epoch, event.reason, now)
      end
    end

    private def handle_downstream(event : Transport::Event, broker : Broker) : Nil
      now = Clock.now
      case event
      when Transport::Frame
        LOGGER.info { downstream_wire_log(event.endpoint, :rx, event.payload) }
        broker.client_frame(event.endpoint, event.payload, now)
      when Transport::Closed
        remote = @clients[event.endpoint]?.try { |endpoint| socket_address(endpoint.socket.remote_address) } || "unknown"
        # Malformed peers use Broker's rate-limited diagnostic below. Emitting
        # this parallel info record would otherwise defeat the total log bound.
        unless event.category == :malformed
          LOGGER.info do
            "event=client.disconnected epoch=#{broker.epoch} session=#{event.endpoint} " \
            "remote=#{remote} reason=#{event.reason.inspect} category=#{event.category}"
          end
        end
        broker.client_closed(event.endpoint, now, event.reason, event.category)
      when Transport::Written
        LOGGER.debug { "event=client.write_completed epoch=#{event.epoch} session=#{event.endpoint} write_id=#{event.write_id}" }
        broker.written(event.endpoint, event.epoch, event.write_id, now)
      when Transport::WriteFailed
        LOGGER.warn do
          "event=client.write_failed epoch=#{event.epoch} session=#{event.endpoint} " \
          "write_id=#{event.write_id} reason=#{event.reason.inspect}"
        end
        broker.write_failed(event.endpoint, event.epoch, event.reason, now)
      end
    end

    private def apply_actions(broker : Broker, upstream : Transport::Endpoint) : Bool
      # Execute effects only after ownership decisions finish. Queue failures
      # can generate more actions, so drain those before accepting another event.
      ended = false
      loop do
        actions = broker.take_actions
        break if actions.empty?
        actions.each do |action|
          case action
          when SendFrame
            endpoint = action.session == 0 ? upstream : @clients[action.session]?
            if endpoint && endpoint.enqueue(Transport::Write.new(action.epoch, action.write_id, action.payload))
              if action.session == 0
                LOGGER.info { WireLog.upstream(action.epoch, :tx, action.payload) }
              else
                LOGGER.info { downstream_wire_log(action.session, :tx, action.payload) }
              end
            else
              broker.write_failed(action.session, action.epoch, "writer queue unavailable", Clock.now)
            end
          when CloseSession
            if endpoint = @clients.delete(action.session)
              endpoint.stop
            end
            @client_routes.delete(action.session)
          when EndEpoch
            ended = true if action.epoch == broker.epoch
          when Diagnostic
            log_diagnostic(action)
          end
        end
      end
      ended
    end

    private def log_diagnostic(action : Diagnostic) : Nil
      if action.category == :malformed
        now = Clock.now
        if (last = @last_malformed_log) && now - last < 1.second
          @suppressed_malformed += 1
          return
        end
        @last_malformed_log = now
        LOGGER.warn { "#{action.message} suppressed_since_last=#{@suppressed_malformed}" }
        @suppressed_malformed = 0_u64
      else
        case action.category
        when :debug
          LOGGER.debug { action.message }
        when :trace
          LOGGER.trace { action.message }
        when :warn
          LOGGER.warn { action.message }
        when :error
          LOGGER.error { action.message }
        else
          LOGGER.info { action.message }
        end
      end
    end

    private def close_all_clients : Nil
      # Detach the client map before joining endpoints so cancellation cannot
      # leave a stopped socket available to later actions from the old epoch.
      clients = @clients
      @clients = Hash(Int64, Transport::Endpoint).new
      @client_routes.clear
      LOGGER.info { "event=clients.closing count=#{clients.size} epoch=#{@next_epoch}" } unless clients.empty?
      clients.each do |id, endpoint|
        remote = socket_address(endpoint.socket.remote_address) rescue "unknown"
        LOGGER.info { "event=client.closed epoch=#{@next_epoch} session=#{id} remote=#{remote} reason=upstream_epoch_ended" }
        endpoint.stop
      end
    end

    private def prepare_dedicated_slots(self_key : Bytes) : Nil
      # Dedicated history crosses upstream epochs only for the same companion
      # public key. A changed identity must never inherit another node's inbox.
      if previous_key = @dedicated_slots_key
        if previous_key == self_key
          retained = @dedicated_slots.values.sum(&.offline_queue.size)
          LOGGER.info { "event=dedicated_queues.preserved entries=#{retained}" } unless retained.zero?
        else
          discarded = @dedicated_slots.values.sum(&.clear)
          LOGGER.warn do
            "event=dedicated_queues.cleared reason=upstream_identity_changed entries=#{discarded}"
          end
        end
      end
      @dedicated_slots_key = self_key.dup
    end

    private def orphan_for(self_key : Bytes) : Bytes?
      orphan = @orphan
      return nil unless orphan
      if previous_key = @orphan_key
        if previous_key == self_key
          @orphan = nil
          @orphan_key = nil
          return orphan
        end
      end
      LOGGER.warn { "event=inbox.orphan_discarded reason=upstream_identity_changed" }
      @orphan = nil
      @orphan_key = nil
      nil
    end

    private def radio_state_for(self_key : Bytes) : CompanionRadioState
      # A public-key match cannot prove the node did not reboot, but a mismatch
      # does prove that old radio reservations belong to another identity. Keep
      # conservative same-key protection; a real reboot merely waits out it.
      if previous_key = @radio_state_key
        unless previous_key == self_key
          @radio_state = CompanionRadioState.new
          LOGGER.warn { "event=radio_state.cleared reason=upstream_identity_changed" }
        end
      end
      @radio_state_key = self_key.dup
      @radio_state
    end

    private def wait_with_refusal(duration : Time::Span) : Nil
      # Keep the listener responsive during backoff without admitting sessions
      # that have no synchronized upstream; shutdown interrupts this wait.
      deadline = Clock.now + duration
      loop do
        remaining = deadline - Clock.now
        return if remaining <= Time::Span.zero
        select
        when accepted = @accepted.receive
          socket = accepted.socket
          LOGGER.info { "event=client.refused remote=#{socket_address(socket.remote_address)} reason=upstream_backoff" }
          socket.close
        when timeout(remaining)
          return
        when @stopping.receive?
          return
        end
      end
    end

    private def jitter(duration : Time::Span) : Time::Span
      duration * (0.8 + Random.rand * 0.4)
    end

    private def socket_address(address : Socket::Address) : String
      # Socket address rendering is public transport metadata and is vital for
      # distinguishing downstream clients and upstream reconnect attempts.
      address.to_s
    end

    private def downstream_wire_log(session : Int64, direction : Symbol, payload : Bytes) : String
      # Dedicated connections are named by their stable listener port so a
      # replacement socket continues the same visible stream. Multi-client
      # connections retain their transient session number.
      if (route = @client_routes[session]?) && route.kind.dedicated_client?
        WireLog.dedicated_client(route.dedicated_slot_id.not_nil!, direction, payload)
      else
        WireLog.multi_client(session, direction, payload)
      end
    end
  end
end
