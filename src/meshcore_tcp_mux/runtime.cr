require "socket"
require "./broker"
require "./config"
require "./frame_codec"
require "./startup"
require "./transport"

class MeshCoreTCPMux
  class Runtime
    # Owns the listener, upstream epochs, socket fibers, and Broker side effects.
    # The Broker is invoked only by this fiber.
    alias ConnectResult = TCPSocket | Exception

    @stopping = Channel(Nil).new
    @finished = Channel(Nil).new(1)
    @accepted = Channel(TCPSocket).new
    @accept_done = Channel(Nil).new(1)
    @running = false
    @stop_requested = false
    @server : TCPServer? = nil
    @upstream : Transport::Endpoint? = nil
    @clients = Hash(Int64, Transport::Endpoint).new
    @next_session = 0_i64
    @next_epoch = 0_i64
    @orphan : Bytes? = nil
    @orphan_key : Bytes? = nil
    @last_malformed_log : Time::Span? = nil
    @suppressed_malformed = 0_u64

    def initialize(@host : String, @port : Int32, @config : Config)
    end

    def run : Nil
      raise "runtime already running" if @running
      @running = true
      server = TCPServer.new(@config.listen_host, @config.listen_port)
      @server = server
      start_acceptor(server)
      backoff = 500.milliseconds

      until @stop_requested
        socket = connect_upstream
        unless socket
          break if @stop_requested
          wait_with_refusal(jitter(backoff))
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
          self_key = startup.self_info.not_nil![4, 32].dup
          orphan = orphan_for(self_key)
          broker = Broker.new(@next_epoch, self_key, @config, Clock.now, orphan)
          ready_at = Clock.now
          STDERR.puts "runtime epoch=#{@next_epoch} ready #{startup.identification}"
          run_epoch(broker, endpoint, upstream_events)
          @orphan = broker.orphan.try(&.dup)
          @orphan_key = @orphan ? self_key : nil
          backoff = 500.milliseconds if Clock.now - ready_at >= 30.seconds
        rescue ex
          STDERR.puts "runtime epoch=#{@next_epoch} error=#{(ex.message || ex.class.name).inspect}"
        ensure
          endpoint.stop
          @upstream = nil
          close_all_clients
        end

        break if @stop_requested
        wait_with_refusal(jitter(backoff))
        backoff = {backoff * 2, 30.seconds}.min
      end
    ensure
      @stop_requested = true
      @stopping.close unless @stopping.closed?
      @server.try &.close
      @upstream.try &.stop
      close_all_clients
      @accept_done.receive if @server
      @server = nil
      @running = false
      @finished.send(nil)
    end

    def stop : Nil
      # Stops only local I/O. It deliberately emits no radio command.
      return if @stop_requested
      @stop_requested = true
      @stopping.close
      @server.try &.close
      @upstream.try &.socket.close
      @clients.each_value { |endpoint| endpoint.socket.close rescue nil }
      @finished.receive if @running
    end

    private def start_acceptor(server : TCPServer) : Nil
      spawn do
        begin
          loop do
            socket = server.accept
            accepted = select
            when @accepted.send(socket)
              true
            when @stopping.receive?
              false
            end
            unless accepted
              socket.close
              break
            end
          end
        rescue ex : IO::Error
          STDERR.puts "listener error=#{(ex.message || ex.class.name).inspect}" unless @stop_requested
        ensure
          @accept_done.send(nil)
        end
      end
    end

    private def connect_upstream : TCPSocket?
      result = Channel(ConnectResult).new(1)
      done = Channel(Nil).new(1)
      spawn do
        begin
          socket = TCPSocket.new(@host, @port, connect_timeout: 5.seconds)
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
            STDERR.puts "upstream connect error=#{(connected.message || connected.class.name).inspect}"
            return nil
          end
          return connected
        when socket = @accepted.receive
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
      endpoint.start # reader is running before any probe is enqueued
      startup = Startup.new(Clock.now, @config.startup_timeout)
      Startup.probes.each_with_index do |payload, index|
        write = Transport::Write.new(epoch, -(index + 1).to_i64, payload)
        raise Startup::Error.new("startup writer queue full") unless endpoint.enqueue(write)
      end

      until startup.ready?
        select
        when event = events.receive
          case event
          when Transport::Frame
            if command = startup.receive(event.payload, Clock.now)
              raise Startup::Error.new("startup writer queue full") unless endpoint.enqueue(Transport::Write.new(epoch, -7_i64, command))
            end
          when Transport::Closed
            raise Startup::Error.new("upstream closed: #{event.reason}")
          when Transport::WriteFailed
            raise Startup::Error.new("upstream write failed: #{event.reason}")
          when Transport::Written
            # Receipt is useful only for failure detection during startup.
          end
        when socket = @accepted.receive
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
      downstream_events = Channel(Transport::Event).new
      ended = apply_actions(broker, upstream)
      until ended || @stop_requested
        select
        when socket = @accepted.receive
          admit(socket, broker, downstream_events)
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

    private def admit(socket : TCPSocket, broker : Broker, events : Channel(Transport::Event)) : Nil
      @next_session += 1
      id = @next_session
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
      endpoint.start
      broker.admit(id, Clock.now)
    end

    private def handle_upstream(event : Transport::Event, broker : Broker) : Nil
      now = Clock.now
      case event
      when Transport::Frame
        broker.upstream_frame(event.payload, now)
      when Transport::Closed
        broker.fail_epoch("upstream closed: #{event.reason}")
      when Transport::Written
        broker.written(0_i64, event.epoch, event.write_id, now)
      when Transport::WriteFailed
        broker.write_failed(0_i64, event.epoch, event.reason, now)
      end
    end

    private def handle_downstream(event : Transport::Event, broker : Broker) : Nil
      now = Clock.now
      case event
      when Transport::Frame
        broker.client_frame(event.endpoint, event.payload, now)
      when Transport::Closed
        broker.client_closed(event.endpoint, now, event.reason, event.category)
      when Transport::Written
        broker.written(event.endpoint, event.epoch, event.write_id, now)
      when Transport::WriteFailed
        broker.write_failed(event.endpoint, event.epoch, event.reason, now)
      end
    end

    private def apply_actions(broker : Broker, upstream : Transport::Endpoint) : Bool
      ended = false
      loop do
        actions = broker.take_actions
        break if actions.empty?
        actions.each do |action|
          case action
          when SendFrame
            endpoint = action.session == 0 ? upstream : @clients[action.session]?
            unless endpoint && endpoint.enqueue(Transport::Write.new(action.epoch, action.write_id, action.payload))
              broker.write_failed(action.session, action.epoch, "writer queue unavailable", Clock.now)
            end
          when CloseSession
            if endpoint = @clients.delete(action.session)
              endpoint.stop
            end
          when EndEpoch
            ended = true if action.epoch == broker.epoch
          when Diagnostic
            log_diagnostic(action)
          end
        end
      end
      ended
    end

    private def log_diagnostic(action : Diagnostic)
      if action.category == :malformed
        now = Clock.now
        if (last = @last_malformed_log) && now - last < 1.second
          @suppressed_malformed += 1
          return
        end
        @last_malformed_log = now
        STDERR.puts "#{action.message} suppressed_since_last=#{@suppressed_malformed}"
        @suppressed_malformed = 0_u64
      else
        STDERR.puts action.message
      end
    end

    private def close_all_clients : Nil
      clients = @clients
      @clients = Hash(Int64, Transport::Endpoint).new
      clients.each_value(&.stop)
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
      STDERR.puts "discarding orphan inbox item after upstream identity change"
      @orphan = nil
      @orphan_key = nil
      nil
    end

    private def wait_with_refusal(duration : Time::Span) : Nil
      deadline = Clock.now + duration
      loop do
        remaining = deadline - Clock.now
        return if remaining <= Time::Span.zero
        select
        when socket = @accepted.receive
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
  end
end
