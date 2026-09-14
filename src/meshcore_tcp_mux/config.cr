class MeshCoreTCPMux
  # Namespace for the TCP multiplexer: transport, protocol validation, and per-client state.
  class Config
    # Shared runtime/broker settings: listener address, internal safety policies,
    # operational deadlines, and permissions. Only settings with a credible
    # deployment-specific tradeoff are exposed by the command-line interface;
    # the other mutable properties remain useful for deterministic specs.
    #
    # Queue limits count queued and currently active work. FrameCodec's fixed
    # 176-byte payload maximum means entry/frame counts also impose finite byte
    # bounds, so separate byte budgets would be redundant.
    property listen_host = "127.0.0.1"
    property listen_port = 5001
    property command_limit = 16
    property command_age = 15.seconds
    # A virtual sync waits for a qualifying physical inbox pop behind other
    # clients' transactions. Expiry rejects only this wait, without ending the epoch.
    property virtual_sync_timeout = 15.seconds
    property inbox_entries = 256
    property output_frames = 512
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
      # Reject settings that cannot provide finite, positive resource deadlines.
      raise ArgumentError.new("listen port must be between 1 and 65535") unless (1..65535).includes?(@listen_port)
      unless {@command_limit, @inbox_entries, @output_frames}.all? { |n| n > 0 }
        raise ArgumentError.new("queue budgets must be positive")
      end
      unless {@command_age, @virtual_sync_timeout, @frame_timeout, @write_timeout, @response_timeout, @contacts_timeout,
              @startup_timeout, @signing_timeout, @poll_interval}.all? { |duration| duration > Time::Span.zero }
        raise ArgumentError.new("deadlines and polling interval must be positive")
      end
      if @contacts_timeout < @response_timeout
        raise ArgumentError.new("contacts total timeout must not be shorter than its idle timeout")
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
