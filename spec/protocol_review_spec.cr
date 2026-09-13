require "./spec_helper"
require "../src/meshcore_tcp_mux/protocol"

private def review_payload(opcode : Int, size : Int) : Bytes
  Bytes.new(size, 0_u8).tap { |payload| payload[0] = opcode.to_u8 }
end

alias ReviewedProtocol = MeshCoreTCPMux::Protocol

describe "native_v13 protocol validation regressions" do
  it "validates the fixed advert/path keys and raw push metadata prefixes" do
    {0x80_u8 => 33, 0x81_u8 => 33, 0x84_u8 => 4, 0x88_u8 => 3, 0x8e_u8 => 4}.each do |code, size|
      payload = Bytes.new(size, 0_u8)
      payload[0] = code
      MeshCoreTCPMux::Protocol.validate_response_shape!(payload)
      expect_raises(MeshCoreTCPMux::Protocol::ProtocolError) do
        MeshCoreTCPMux::Protocol.validate_response_shape!(payload[0, size - 1])
      end
    end
  end

  it "uses normal encoded-path widths for channel data rather than trace widths" do
    # Normal path 0x81 means one three-byte hash: upper bits encode width - 1.
    command = Bytes[62_u8, 0_u8, 0x81_u8, 0xaa_u8, 0xbb_u8, 0xcc_u8, 1_u8, 0_u8]
    ReviewedProtocol.validate_command(command).valid?.should be_true
  end

  it "rejects unsafe encoded paths stored by contact updates" do
    reserved_width = review_payload(9, 136)
    reserved_width[35] = 0xc1 # Four-byte hashes are reserved by native_v13.
    ReviewedProtocol.validate_command(reserved_width).valid?.should be_false

    oversized = review_payload(9, 136)
    oversized[35] = 0x96 # 22 three-byte hashes require 66 bytes, exceeding MAX_PATH_SIZE.
    ReviewedProtocol.validate_command(oversized).valid?.should be_false
  end

  it "uses normal encoded-path widths in advert and discovery responses" do
    advert_path = review_payload(0x16, 9)
    advert_path[5] = 0x81 # One three-byte hash.
    ReviewedProtocol.validate_response_shape!(advert_path)

    discovery = review_payload(0x8d, 17)
    discovery[8] = 0x81  # Three path bytes follow.
    discovery[12] = 0x42 # Two two-byte hashes (four path bytes) follow.
    ReviewedProtocol.validate_response_shape!(discovery)

    reserved = review_payload(0x16, 6)
    reserved[5] = 0xc0
    expect_raises(ReviewedProtocol::ProtocolError) do
      ReviewedProtocol.validate_response_shape!(reserved)
    end
  end

  it "accepts the full native contact record and rejects a truncated one" do
    ReviewedProtocol.validate_response_shape!(review_payload(0x03, 148))
    ReviewedProtocol.validate_response_shape!(review_payload(0x8a, 148))
    expect_raises(ReviewedProtocol::ProtocolError) do
      ReviewedProtocol.validate_response_shape!(review_payload(0x03, 144))
    end
  end

  it "requires the four-byte sender prefix on signed contact text" do
    legacy = review_payload(0x07, 17)
    legacy[8] = 2 # TXT_TYPE_SIGNED_PLAIN
    ReviewedProtocol.validate_response_shape!(legacy)
    expect_raises(ReviewedProtocol::ProtocolError) do
      ReviewedProtocol.validate_response_shape!(legacy[0, 16])
    end

    v3 = review_payload(0x10, 20)
    v3[11] = 2 # TXT_TYPE_SIGNED_PLAIN
    ReviewedProtocol.validate_response_shape!(v3)
    expect_raises(ReviewedProtocol::ProtocolError) do
      ReviewedProtocol.validate_response_shape!(v3[0, 19])
    end
  end

  it "rejects a trace whose path bytes cannot be divided into complete hashes" do
    trace = review_payload(0x89, 17)
    trace[2] = 3 # Three path bytes...
    trace[3] = 1 # ...with two-byte hashes is structurally incomplete.
    expect_raises(ReviewedProtocol::ProtocolError) do
      ReviewedProtocol.validate_response_shape!(trace)
    end
  end

  it "rejects partial login/status result tails while preserving complete extensions" do
    ReviewedProtocol.validate_response_shape!(review_payload(0x85, 8))  # Legacy login success.
    ReviewedProtocol.validate_response_shape!(review_payload(0x85, 14)) # Native v13 login success.
    ReviewedProtocol.validate_response_shape!(review_payload(0x85, 15)) # Opaque future extension.
    (9..13).each do |size|
      expect_raises(ReviewedProtocol::ProtocolError) do
        ReviewedProtocol.validate_response_shape!(review_payload(0x85, size))
      end
    end

    ReviewedProtocol.validate_response_shape!(review_payload(0x87, 9))
    expect_raises(ReviewedProtocol::ProtocolError) do
      ReviewedProtocol.validate_response_shape!(review_payload(0x87, 8))
    end
  end

  it "accepts the native contact-deleted push with its public key" do
    ReviewedProtocol.validate_response_shape!(review_payload(0x8f, 33))
  end
end
