require "./spec_helper"
require "../src/meshcore_tcp_mux/protocol"

private alias LoggingProtocol = MeshCoreTCPMux::Protocol

describe "protocol-safe logging" do
  # These examples exercise every command/response field whose contents must
  # never enter logs. Each fixture uses a conspicuous repeated byte so both
  # decoded text and hexadecimal leakage are easy to detect.
  it "redacts private keys, passwords, channel keys, PINs, and signing data" do
    fixtures = [
      # IMPORT_PRIVATE_KEY (24): opcode plus 64 private-key bytes.
      Bytes.new(65, 0x53_u8).tap { |p| p[0] = 24_u8 },
      # SEND_LOGIN (26): opcode, 32-byte public destination, then password.
      Bytes.new(45, 0_u8).tap { |p| p[0] = 26_u8; p[33, 12].fill(0x50_u8) },
      # SET_CHANNEL (32): opcode, index, 32-byte name, then 16-byte channel key.
      Bytes.new(50, 0_u8).tap { |p| p[0] = 32_u8; p[34, 16].fill(0x4b_u8) },
      # SIGN_DATA (34): opcode plus opaque signing input.
      Bytes.new(18, 0x47_u8).tap { |p| p[0] = 34_u8 },
      # SET_DEVICE_PIN (37): opcode plus four-byte device PIN.
      Bytes.new(5, 0x49_u8).tap { |p| p[0] = 37_u8 },
      # SET_FLOOD_SCOPE_KEY (54): opcode, mode, then 16-byte scope key.
      Bytes.new(18, 0x46_u8).tap { |p| p[0] = 54_u8; p[1] = 0_u8 },
      # SET_DEFAULT_FLOOD_SCOPE (63): opcode, 31-byte name, then 16-byte key.
      Bytes.new(48, 0_u8).tap { |p| p[0] = 63_u8; p[32, 16].fill(0x44_u8) },
    ]

    fixtures.each do |payload|
      log = LoggingProtocol.describe_command(payload, include_payload: true)
      log.should contain("xx")
    end

    LoggingProtocol.describe_command(fixtures[0], include_payload: true).should_not contain("53" * 64)
    LoggingProtocol.describe_command(fixtures[1], include_payload: true).should_not contain("50" * 12)
    LoggingProtocol.describe_command(fixtures[2], include_payload: true).should_not contain("4b" * 16)
    LoggingProtocol.describe_command(fixtures[3], include_payload: true).should_not contain("47" * 17)
    LoggingProtocol.describe_command(fixtures[4], include_payload: true).should_not contain("49" * 4)
    LoggingProtocol.describe_command(fixtures[5], include_payload: true).should_not contain("46" * 16)
    LoggingProtocol.describe_command(fixtures[6], include_payload: true).should_not contain("44" * 16)
  end

  it "redacts secret response fields while retaining public correlation fields" do
    # PRIVATE_KEY response (0x0e): code plus 64 private-key bytes.
    private_key = Bytes.new(65, 0x53_u8).tap { |p| p[0] = 0x0e_u8 }
    # CHANNEL_INFO response (0x12): code, index, 32-byte name, and 16-byte key.
    channel = Bytes.new(50, 0_u8).tap { |p| p[0] = 0x12_u8; p[34, 16].fill(0x4b_u8) }
    # DEVICE_INFO response (0x0d): the four-byte PIN is at offsets 4..7.
    device = Bytes.new(82, 0_u8).tap do |p|
      p[0] = 0x0d_u8
      p[1] = 13_u8
      p[4, 4].fill(0x49_u8)
    end

    LoggingProtocol.describe_response(private_key, include_payload: true).should_not contain("53" * 64)
    LoggingProtocol.describe_response(channel, include_payload: true).should_not contain("4b" * 16)
    device_log = LoggingProtocol.describe_response(device, include_payload: true)
    device_log.should_not contain("49" * 4)
    device_log.should contain("xxxxxxxx")

    # STATUS_RESPONSE (0x87): reserved byte then six-byte public peer prefix.
    status = Bytes[0x87, 0, 0x91, 0xb4, 0xf2, 0x52, 0xf8, 0xeb, 0]
    LoggingProtocol.describe_response(status).should contain("peer=91b4f252f8eb")
  end

  it "redacts every present PIN byte in truncated device information" do
    (1..7).each do |size|
      # Truncated DEVICE_INFO (0x0d): offsets 4..7 are the little-endian PIN.
      # The conspicuous 0x49 bytes prove pre-validation debug formatting never
      # reveals even a partial secret field.
      device = Bytes.new(size, 0_u8)
      device[0] = 0x0d_u8
      device[4, size - 4].fill(0x49_u8) if size > 4
      log = LoggingProtocol.describe_response(device, include_payload: true)
      log.should_not contain("49") if size > 4
      log.should contain("xx" * (size - 4)) if size > 4
    end
  end

  it "redacts the body of unknown commands and pushes" do
    LoggingProtocol.describe_command(Bytes[0xfe, 0x53, 0x53], include_payload: true)
      .should contain("payload=<redacted-unknown-command>")
    LoggingProtocol.describe_response(Bytes[0xfe, 0x53, 0x53], include_payload: true)
      .should contain("payload=<redacted-unknown-push>")
  end
end
