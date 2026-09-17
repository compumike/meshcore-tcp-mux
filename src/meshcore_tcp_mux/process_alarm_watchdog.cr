require "log"

lib LibC
  fun alarm(seconds : Int32) : Int32
end

class MeshCoreTCPMux
  # Namespace for the TCP multiplexer: transport, protocol validation, and per-client state.
  class ProcessAlarmWatchdog
    # Owns the process-global SIGALRM fallback for a daemon whose runtime fiber
    # has stopped making progress. Runtime touches it only after this entrypoint
    # setup has installed the signal handler, leaving direct runtime specs inert.
    WATCHDOG_SECONDS = 5 * 60
    @@installed = false

    def self.setup! : Nil
      # Install the handler once at executable startup. SIGALRM is process-wide,
      # so neither Runtime cleanup nor individual upstream epochs may cancel it.
      Signal::ALRM.trap do
        Log.error { "ProcessAlarmWatchdog: alarm signal received. Exiting with status 142 (SIGALRM)..." }
        exit(142) # 142 is the standard exit code for SIGALRM.
      end
      @@installed = true
    end

    def self.touch! : Nil
      # Reset the five-minute countdown. An elapsed alarm means the primary
      # runtime loop did not regain control, so a supervisor must replace it.
      # Runtime-only specs never install this process-global signal handler.
      return unless @@installed
      LibC.alarm(WATCHDOG_SECONDS)
    end
  end
end
