require "./spec_helper"
require "../src/meshcore_tcp_mux/binary_entrypoint"

private def parse_policy_options(arguments : Array(String)) : MeshCoreTCPMux::Config
  # Exercise policy parsing without opening a companion connection or trapping
  # process signals as the complete binary entrypoint does.
  config = MeshCoreTCPMux::Config.new
  parser = OptionParser.new
  MeshCoreTCPMux::BinaryEntrypoint.register_policy_options(parser, config)
  parser.parse(arguments)
  config
end

describe MeshCoreTCPMux::BinaryEntrypoint do
  # Policy options preserve direct-companion defaults while offering granular
  # rejection and accepting obsolete allow switches for service compatibility.
  it "rejects private-key export, private-key import, and factory reset independently" do
    export = parse_policy_options(["--reject-private-key-export"])
    export.private_key_export.should be_false
    export.private_key_import.should be_true
    export.factory_reset.should be_true

    import = parse_policy_options(["--reject-private-key-import"])
    import.private_key_export.should be_true
    import.private_key_import.should be_false
    import.factory_reset.should be_true

    reset = parse_policy_options(["--reject-factory-reset"])
    reset.private_key_export.should be_true
    reset.private_key_import.should be_true
    reset.factory_reset.should be_false
  end

  it "accepts legacy allow flags without overriding explicit rejection" do
    config = parse_policy_options([
      "--reject-private-key-export",
      "--reject-private-key-import",
      "--reject-factory-reset",
      "--allow-private-key-export",
      "--maintenance",
    ])
    config.private_key_export.should be_false
    config.private_key_import.should be_false
    config.factory_reset.should be_false
  end

  it "enables received-message deduplication only when requested" do
    parse_policy_options([] of String).deduplicate_received_messages.should be_false
    parse_policy_options(["--deduplicate-received-messages"]).deduplicate_received_messages.should be_true
  end
end
