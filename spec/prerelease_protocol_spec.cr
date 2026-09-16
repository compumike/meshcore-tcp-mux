require "./spec_helper"
require "../src/meshcore_tcp_mux/protocol"

private alias PrereleaseProtocol = MeshCoreTCPMux::Protocol

private def prerelease_trace_result(hops : Int32, hash_width_shift = 0_u8) : Bytes
  # TRACE_DATA (0x89) has a 12-byte prefix, whole path hashes, one SNR byte
  # per hop, and a final SNR byte. Byte 2 counts path bytes and the low two
  # bits of byte 3 encode log2(hash width); the TCP envelope is excluded.
  path_bytes = hops << hash_width_shift
  Bytes.new(12 + path_bytes + hops + 1, 0_u8).tap do |payload|
    payload[0] = PrereleaseProtocol::PUSH_TRACE_DATA
    payload[2] = path_bytes.to_u8
    payload[3] = hash_width_shift
  end
end

describe MeshCoreTCPMux::Protocol, "pre-release protocol boundaries" do
  # These checks come from release review of variable-size upstream frames.
  # The pinned firmware stores at most 64 accumulated trace SNR entries. Trace
  # hash bytes live in the packet payload and can exceed 64 when hashes are wider
  # than one byte. Keep the native 64-hop command boundary while rejecting a
  # response whose decoded hop count exceeds that bound.

  it "accepts the last completable one-byte-hash trace command and result" do
    command = Bytes.new(10 + 63, 0_u8)
    command[0] = PrereleaseProtocol::CMD_SEND_TRACE_PATH
    command[9] = 0_u8 # Flags 0 select one-byte hashes; 63 path bytes follow.
    PrereleaseProtocol.validate_command(command).valid?.should be_true
    PrereleaseProtocol.validate_response_shape!(prerelease_trace_result(63))
  end

  it "documents native acceptance of the receive-side 64-hop limitation" do
    command = Bytes.new(10 + 64, 0_u8)
    command[0] = PrereleaseProtocol::CMD_SEND_TRACE_PATH
    command[9] = 0_u8 # Flags 0 select one-byte hashes; 64 path bytes follow.
    PrereleaseProtocol.validate_command(command).valid?.should be_true
  end

  it "rejects a terminal result beyond the firmware's 64-hop SNR storage" do
    expect_raises(PrereleaseProtocol::ProtocolError) do
      PrereleaseProtocol.validate_response_shape!(prerelease_trace_result(65))
    end
  end

  it "accepts multi-byte trace hash paths whose byte length exceeds 64" do
    # Trace width shifts 1 and 2 mean two- and four-byte hashes. These paths are
    # below 64 hops, fit the 176-byte companion envelope, and deliberately exceed
    # 64 hash bytes to prove MAX_PATH_SIZE applies to SNR entries, not hash bytes.
    two_byte_hashes = prerelease_trace_result(35, 1_u8)  # 70 hash bytes, 35 hops.
    four_byte_hashes = prerelease_trace_result(30, 2_u8) # 120 hash bytes, 30 hops.
    PrereleaseProtocol.validate_response_shape!(two_byte_hashes)
    PrereleaseProtocol.validate_response_shape!(four_byte_hashes)
  end
end
