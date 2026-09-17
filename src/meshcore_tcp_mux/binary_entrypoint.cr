require "option_parser"
require "log"
require "./upstream"
require "./runtime"
require "./version"

class MeshCoreTCPMux
  # Namespace for the TCP multiplexer: transport, protocol validation, and per-client state.
  class BinaryEntrypoint
    # Parses command-line options and starts either the diagnostic probe or the mux runtime.
    # Keeps process setup and exit handling out of the protocol and broker classes.
    def initialize : Nil
      # Preserve stdout for --probe's machine-readable result while allowing the
      # standard LOG_LEVEL environment variable to select diagnostic verbosity.
      Log.setup_from_env(backend: Log::IOBackend.new(STDERR))
      Log.info { "meshcore-tcp-mux #{VERSION}" }
      host : String? = nil
      port : Int32? = nil
      probe = false
      config = MeshCoreTCPMux::Config.new
      parser = OptionParser.new do |options|
        options.banner = "Usage: meshcore-tcp-mux --upstream-host HOST --upstream-port PORT [options]"
        options.on("--upstream-host HOST", "Physical companion host") { |value| host = value }
        options.on("--upstream-port PORT", "Physical companion port") { |value| port = value.to_i }
        options.on("--probe", "Synchronize and print firmware identification, then exit") { probe = true }
        options.on("--listen-host HOST", "Listener address (default 127.0.0.1)") { |value| config.listen_host = value }
        options.on("--listen-multi-client-port PORT", "Multi-client listener port (default 5001)") do |value|
          config.listen_multi_client_port = value.to_i
        end
        options.on("--listen-dedicated-client-port PORT", "Dedicated-client listener port (repeatable)") do |value|
          config.listen_dedicated_client_ports << value.to_i
        end
        options.on("--offline-queue-size COUNT", "Per-dedicated-client offline queue entries (default 256)") do |value|
          config.offline_queue_size = value.to_i
        end
        options.on("--response-timeout SECONDS", "Upstream response / contacts idle deadline (20)") { |v| config.response_timeout = v.to_f.seconds }
        options.on("--contacts-timeout SECONDS", "Total contacts transaction deadline (30)") { |v| config.contacts_timeout = v.to_f.seconds }
        options.on("--poll-interval SECONDS", "Inbox fallback polling interval (5)") { |v| config.poll_interval = v.to_f.seconds }
        self.class.register_policy_options(options, config)
        options.on("--version", "Show release version") { puts "meshcore-tcp-mux #{MeshCoreTCPMux::VERSION}"; exit }
        options.on("-h", "--help", "Show usage") { puts options; exit }
      end

      begin
        parser.parse
        raise ArgumentError.new("upstream host and port are required") unless host && port
        raise ArgumentError.new("port must be between 1 and 65535") unless (1..65535).includes?(port.not_nil!)
        config.validate!
        if probe
          puts MeshCoreTCPMux::Upstream.probe(host.not_nil!, port.not_nil!, config)
        else
          runtime = MeshCoreTCPMux::Runtime.new(host.not_nil!, port.not_nil!, config)
          Signal::INT.trap { runtime.stop }
          Signal::TERM.trap { runtime.stop }
          runtime.run
        end
      rescue ex
        Log.for("meshcore_tcp_mux").error(exception: ex) { "process failed" }
        exit 1
      end
    end

    def self.register_policy_options(options : OptionParser, config : Config) : Nil
      # Direct companion access permits these operations. Keep that behavior by
      # default while allowing deployments to deny each sensitive command.
      options.on("--reject-private-key-export", "Reject requester-only private-key export") do
        config.private_key_export = false
      end
      options.on("--reject-private-key-import", "Reject private-key import") do
        config.private_key_import = false
      end
      options.on("--reject-factory-reset", "Reject factory reset") do
        config.factory_reset = false
      end

      # These former opt-in switches are accepted so existing service command
      # lines continue to start. They are no-ops because their policies now
      # match the defaults, and therefore cannot override an explicit reject.
      options.on("--maintenance", "Compatibility option; import and reset are allowed by default") { }
      options.on("--allow-private-key-export", "Compatibility option; export is allowed by default") { }
    end
  end
end
