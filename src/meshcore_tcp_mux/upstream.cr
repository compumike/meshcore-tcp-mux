require "socket"
require "./startup"
require "./config"
require "./transport"

class MeshCoreTCPMux
  # Namespace for the TCP multiplexer: transport, protocol validation, and per-client state.
  class Upstream
    # Offers a one-shot diagnostic probe using the same transport and startup fence as Runtime.
    def self.probe(host : String, port : Int32, config = Config.new) : String
      # A standalone version probe is also useful when diagnosing deployment access.
      # It uses the same synchronization fence as the daemon, with no listener.
      socket = TCPSocket.new(host, port, connect_timeout: 5.seconds)
      events = Channel(Transport::Event).new(32)
      endpoint = Transport::Endpoint.new(
        socket,
        0_i64,
        FrameCodec::COMPANION_TO_CLIENT_MARKER,
        FrameCodec::CLIENT_TO_COMPANION_MARKER,
        events,
        config.frame_timeout,
        config.write_timeout,
        16
      )
      startup = Startup.new(Clock.now, config.startup_timeout)
      endpoint.start
      Startup.probes.each_with_index do |payload, index|
        write = Transport::Write.new(0_i64, -(index + 1).to_i64, payload)
        raise Startup::Error.new("startup writer queue full") unless endpoint.enqueue(write)
      end
      until startup.ready?
        select
        when event = events.receive
          now = Clock.now
          case event
          when Transport::Frame
            if command = startup.receive(event.payload, now)
              write = Transport::Write.new(0_i64, -7_i64, command)
              raise Startup::Error.new("startup writer queue full") unless endpoint.enqueue(write)
            end
          when Transport::Closed
            raise Startup::Error.new("upstream closed: #{event.reason}")
          when Transport::WriteFailed
            raise Startup::Error.new("upstream write failed: #{event.reason}")
          when Transport::Written
            # Only validated companion frames advance startup.
          end
          startup.check_deadline(now) unless startup.ready?
        when timeout(100.milliseconds)
          startup.check_deadline(Clock.now)
        end
      end
      startup.identification
    ensure
      endpoint.try &.stop
      socket.try &.close
    end
  end
end
