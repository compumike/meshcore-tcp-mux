require "option_parser"
require "./meshcore_tcp_mux/upstream"
require "./meshcore_tcp_mux/runtime"

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
  options.on("--listen-port PORT", "Listener port (default 5001)") { |value| config.listen_port = value.to_i }
  options.on("--command-limit N", "Pending commands per session (16)") { |v| config.command_limit = v.to_i }
  options.on("--inbox-entries N", "Inbox entries per session (256)") { |v| config.inbox_entries = v.to_i }
  options.on("--inbox-bytes N", "Inbox bytes per session (65536)") { |v| config.inbox_bytes = v.to_i }
  options.on("--output-frames N", "Output frames per session, including in-flight (512)") { |v| config.output_frames = v.to_i }
  options.on("--output-bytes N", "Output bytes per session, including in-flight (131072)") { |v| config.output_bytes = v.to_i }
  options.on("--command-age SECONDS", "Maximum queue age before dispatch (3)") { |v| config.command_age = v.to_f.seconds }
  options.on("--frame-timeout SECONDS", "Absolute partial-frame deadline (5)") { |v| config.frame_timeout = v.to_f.seconds }
  options.on("--write-timeout SECONDS", "Absolute socket write deadline (5)") { |v| config.write_timeout = v.to_f.seconds }
  options.on("--response-timeout SECONDS", "Upstream response / contacts idle deadline (5)") { |v| config.response_timeout = v.to_f.seconds }
  options.on("--contacts-timeout SECONDS", "Total contacts transaction deadline (30)") { |v| config.contacts_timeout = v.to_f.seconds }
  options.on("--startup-timeout SECONDS", "Startup synchronization deadline (15)") { |v| config.startup_timeout = v.to_f.seconds }
  options.on("--signing-timeout SECONDS", "Signing inactivity deadline (30)") { |v| config.signing_timeout = v.to_f.seconds }
  options.on("--poll-interval SECONDS", "Inbox fallback polling interval (5)") { |v| config.poll_interval = v.to_f.seconds }
  options.on("--maintenance", "Enable private-key import and factory reset with one idle session") { config.maintenance = true }
  options.on("--allow-private-key-export", "Allow requester-only private key export") { config.private_key_export = true }
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
  STDERR.puts "meshcore-tcp-mux: #{ex.message}"
  exit 1
end
