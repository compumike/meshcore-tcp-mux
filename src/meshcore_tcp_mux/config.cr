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
    property listen_multi_client_port = 5001
    property listen_dedicated_client_ports = Array(Int32).new
    property offline_queue_size = 256
    property command_limit = 16
    property command_age = 15.seconds
    # A virtual sync waits for a qualifying physical inbox pop behind other
    # clients' transactions. Expiry rejects only this wait, without ending the epoch.
    property virtual_sync_timeout = 30.seconds
    property inbox_entries = 256
    property output_frames = 512
    # Bound both hostname resolution and TCP establishment. A connect timeout
    # alone begins only after DNS has returned in Crystal's socket API.
    property connect_timeout = 5.seconds
    property frame_timeout = 5.seconds
    property write_timeout = 5.seconds
    # openHop companions await radio injection and persistence work before
    # producing otherwise ordinary replies. Their reference client uses a
    # 15-second command horizon, so five seconds is not a portable companion
    # response bound even when the TCP reader remains healthy. Keep additional
    # headroom for radio-link arbitration and persistence contention.
    property response_timeout = 20.seconds
    property contacts_timeout = 30.seconds
    property startup_timeout = 15.seconds
    property signing_timeout = 30.seconds
    # If TCP dies before SENT/ERR, the command may nevertheless have reached
    # the companion. Keep shared radio admission closed long enough for that
    # unobservable operation to finish without being overwritten after reconnect.
    property radio_uncertainty_timeout = 60.seconds
    property poll_interval = 5.seconds
    # Suppress radio retries that the companion exposes as repeated logical
    # inbox messages. Disabled by default because downstream clients normally
    # own this protocol policy themselves.
    property deduplicate_received_messages = false
    # Match a directly connected companion by default. Deployments that expose
    # the mux to clients which should not control identity state can reject each
    # sensitive operation independently through the corresponding CLI flag.
    property private_key_export = true
    property private_key_import = true
    property factory_reset = true

    def maintenance : Bool
      # Preserve the former combined programmatic policy for callers and specs
      # while new code uses the granular import/reset settings.
      @private_key_import && @factory_reset
    end

    def maintenance=(allowed : Bool) : Bool
      # The legacy setter intentionally changes both operations together.
      @private_key_import = allowed
      @factory_reset = allowed
      allowed
    end

    def validate! : Nil
      # Reject settings that cannot provide finite, positive resource deadlines.
      ports = [@listen_multi_client_port] + @listen_dedicated_client_ports
      unless ports.all? { |listen_port| (1..65535).includes?(listen_port) }
        raise ArgumentError.new("listen ports must be between 1 and 65535")
      end
      raise ArgumentError.new("listen ports must be unique") unless ports.uniq.size == ports.size
      unless {@command_limit, @inbox_entries, @offline_queue_size, @output_frames}.all? { |n| n > 0 }
        raise ArgumentError.new("queue budgets must be positive")
      end
      unless {@command_age, @virtual_sync_timeout, @connect_timeout, @frame_timeout, @write_timeout, @response_timeout, @contacts_timeout,
              @startup_timeout, @signing_timeout, @radio_uncertainty_timeout,
              @poll_interval}.all? { |duration| duration > Time::Span.zero }
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
