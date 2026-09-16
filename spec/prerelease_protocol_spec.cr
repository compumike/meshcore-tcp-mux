require "./spec_helper"
require "../src/meshcore_tcp_mux/protocol"

private def prerelease_trace_result(hops : Int32, hash_width_shift = 0_u8) : Bytes
  # TRACE_DATA (0x89) has a 12-byte prefix, whole path hashes, one SNR byte
  # per hop, and a final SNR byte. Byte 2 counts path bytes and the low two
  # bits of byte 3 encode log2(hash width); the TCP envelope is excluded.
  path_bytes = hops << hash_width_shift
  Bytes.new(12 + path_bytes + hops + 1, 0_u8).tap do |payload|
    payload[0] = 0x89_u8
    payload[2] = path_bytes.to_u8
    payload[3] = hash_width_shift
  end
end

describe MeshCoreTCPMux::Protocol, "pre-release protocol boundaries" do
  # These checks come from release review of variable-size upstream frames.
  # The pinned firmware stores at most 64 trace path bytes. Its command handler
  # accepts a one-byte-hash path at that exact boundary even though receive-side
  # trace completion requires the accumulated SNR path to remain shorter. Keep
  # that native compatibility behavior explicit, while rejecting an upstream
  # result that unambiguously exceeds the storage bound.

  it "accepts the last completable one-byte-hash trace command and result" do
    command = Bytes.new(10 + 63, 0_u8)
    command[0] = 36_u8 # SEND_TRACE_PATH.
    command[9] = 0_u8  # Flags 0 select one-byte hashes; 63 path bytes follow.
    MeshCoreTCPMux::Protocol.validate_command(command).valid?.should be_true
    MeshCoreTCPMux::Protocol.validate_response_shape!(prerelease_trace_result(63))
  end

  it "documents native acceptance of the receive-side 64-hop limitation" do
    command = Bytes.new(10 + 64, 0_u8)
    command[0] = 36_u8 # SEND_TRACE_PATH.
    command[9] = 0_u8  # Flags 0 select one-byte hashes; 64 path bytes follow.
    MeshCoreTCPMux::Protocol.validate_command(command).valid?.should be_true
  end

  it "rejects a terminal result beyond the firmware's 64-byte path storage" do
    expect_raises(MeshCoreTCPMux::Protocol::ProtocolError) do
      MeshCoreTCPMux::Protocol.validate_response_shape!(prerelease_trace_result(65))
    end
  end
end
