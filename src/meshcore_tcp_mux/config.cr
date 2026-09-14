class MeshCoreTCPMux
  # Namespace for the TCP multiplexer: transport, protocol validation, and per-client state.
  class Config
    # Shared runtime/broker settings: listener address, queue budgets, deadlines, and permissions.
    # Limits count queued and currently written data. There is intentionally no
    # fixed client-count cap: each connection has its own finite budgets.
    property listen_host = "127.0.0.1"
    property listen_port = 5001
    property command_limit = 16
    property command_age = 3.seconds
    property inbox_entries = 256
    property inbox_bytes = 64 * 1024
    property output_frames = 512
    property output_bytes = 128 * 1024
    property frame_timeout = 5.seconds
    property write_timeout = 5.seconds
    property response_timeout = 5.seconds
    property contacts_timeout = 30.seconds
    property startup_timeout = 15.seconds
    property signing_timeout = 30.seconds
    property poll_interval = 5.seconds
    property maintenance = false
    property private_key_export = false

    def validate! : Nil
      raise ArgumentError.new("listen port must be between 1 and 65535") unless (1..65535).includes?(@listen_port)
      unless {@command_limit, @inbox_entries, @inbox_bytes, @output_frames, @output_bytes}.all? { |n| n > 0 }
        raise ArgumentError.new("queue budgets must be positive")
      end
      unless {@command_age, @frame_timeout, @write_timeout, @response_timeout, @contacts_timeout,
              @startup_timeout, @signing_timeout, @poll_interval}.all? { |duration| duration > Time::Span.zero }
        raise ArgumentError.new("deadlines and polling interval must be positive")
      end
    end
  end

  class Clock
    # Supplies monotonic elapsed time for deadlines, unaffected by wall-clock corrections.
    ORIGIN = Time.instant

    def self.now : Time::Span
      Time.instant - ORIGIN
    end
  end
end
