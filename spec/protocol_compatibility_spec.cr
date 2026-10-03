require "./spec_helper"
require "../src/meshcore_tcp_mux/startup"

private alias CompatibilityProtocol = MeshCoreTCPMux::Protocol

private def compatibility_info(level : UInt8, size : Int32 = 82) : Bytes
  # DEVICE_INFO: known 82-byte prefix, firmware at offset 1. Synthetic filler
  # covers capacities, LE PIN, fixed strings, repeat/path fields and any future tail.
  Bytes.new(size, 0x55_u8).tap do |info|
    info[0] = CompatibilityProtocol::RESP_DEVICE_INFO
    info[1] = level
  end
end

describe "companion protocol compatibility" do
  # Firmware capabilities differ from the requested app dialect. Future levels
  # retain their actual diagnostic identity while clients see only implemented features.
  [13_u8, 14_u8, 15_u8, 99_u8, 255_u8].each do |level|
    [82, 90].each do |size|
      it "accepts firmware #{level} with #{size} bytes and virtualizes its known prefix" do
        info = compatibility_info(level, size)
        CompatibilityProtocol.validate_device_info!(info).should eq(level)
        CompatibilityProtocol.validate_response_shape!(info)
        visible = CompatibilityProtocol.downstream_device_info(info)
        visible.size.should eq(82)
        visible[1].should eq(Math.min(level, 14_u8))
        visible[2..].should eq(info[2, 80])
        info[1].should eq(level)

        startup = MeshCoreTCPMux::Startup.new(0.seconds)
        # SELF_INFO: minimum 58-byte prefix, synthetic public key at offsets 4..35.
        self_info = Bytes.new(58, 0_u8)
        self_info[0] = CompatibilityProtocol::RESP_SELF_INFO
        5.times { startup.receive(self_info, 0.seconds) }
        # SET_FLOOD_SCOPE_KEY, mode zero: hidden startup default-scope reset.
        startup.receive(info, 0.seconds).should eq(Bytes[CompatibilityProtocol::CMD_SET_FLOOD_SCOPE_KEY, 0])
        startup.receive(Bytes[CompatibilityProtocol::RESP_OK], 0.seconds)
        startup.ready?.should be_true
        startup.upstream_protocol_level.should eq(level)
        startup.exposed_protocol_level.should eq(Math.min(level, 14_u8))
        startup.device_info.should eq(info)
        startup.downstream_device_info.should eq(visible)
        startup.identification.should contain("upstream_protocol=#{level} exposed_protocol=#{Math.min(level, 14_u8)}")
        startup.identification.should contain("compatibility_mode=#{level > 14}")
      end
    end
  end

  it "rejects old firmware and malformed or oversized known prefixes" do
    [compatibility_info(12_u8), compatibility_info(15_u8, 81), compatibility_info(15_u8, 177)].each do |info|
      expect_raises(CompatibilityProtocol::ProtocolError) { CompatibilityProtocol.validate_device_info!(info) }
    end
    wrong = compatibility_info(15_u8)
    wrong[0] = CompatibilityProtocol::RESP_SELF_INFO # Wrong opcode despite sufficient length.
    expect_raises(CompatibilityProtocol::ProtocolError) { CompatibilityProtocol.validate_device_info!(wrong) }
  end

  it "validates opaque CLI commands and replies at the payload boundaries" do
    p = CompatibilityProtocol
    # RUN_CLI_COMMAND plus text: both unterminated and optional NUL forms are valid.
    ["version", "version\0", "x"].each do |body|
      p.validate_command(Bytes[CompatibilityProtocol::CMD_RUN_CLI_COMMAND] + body.to_slice).valid?.should be_true
    end
    # Opcode only has no command; 176 includes opcode, while 177 exceeds framing limits.
    p.validate_command(Bytes[CompatibilityProtocol::CMD_RUN_CLI_COMMAND]).valid?.should be_false
    [176, 177].each do |size|
      command = Bytes.new(size, 'x'.ord.to_u8)
      command[0] = CompatibilityProtocol::CMD_RUN_CLI_COMMAND
      p.validate_command(command).valid?.should eq(size == 176)
    end
    [1, 8, 176].each do |size|
      reply = Bytes.new(size, 0xff_u8) # Opaque bytes need not decode as UTF-8.
      reply[0] = CompatibilityProtocol::RESP_CLI_REPLY
      p.validate_response_shape!(reply)
    end
    # 0x1e is an unknown neighboring ordinary response, never an opaque push.
    expect_raises(CompatibilityProtocol::ProtocolError) { p.validate_response_shape!(Bytes[0x1e_u8]) }
    p.describe_command(Bytes[CompatibilityProtocol::CMD_RUN_CLI_COMMAND] + "secret".to_slice, include_payload: true).should contain("payload=[redacted]")
    p.describe_response(Bytes[CompatibilityProtocol::RESP_CLI_REPLY] + "secret".to_slice, include_payload: true).should contain("payload=[redacted]")
  end
end
