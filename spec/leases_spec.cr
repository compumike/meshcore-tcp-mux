require "./spec_helper"
require "../src/meshcore_tcp_mux/leases"

private def sent(token : UInt32, timeout_ms = 1_000_u32) : Bytes
  Bytes[
    0x06, 0,
    (token & 0xff).to_u8, ((token >> 8) & 0xff).to_u8, ((token >> 16) & 0xff).to_u8, ((token >> 24) & 0xff).to_u8,
    (timeout_ms & 0xff).to_u8, ((timeout_ms >> 8) & 0xff).to_u8, ((timeout_ms >> 16) & 0xff).to_u8, ((timeout_ms >> 24) & 0xff).to_u8,
  ]
end

private def confirmed(token : UInt32) : Bytes
  Bytes[0x82, (token & 0xff).to_u8, ((token >> 8) & 0xff).to_u8, ((token >> 16) & 0xff).to_u8, ((token >> 24) & 0xff).to_u8, 1, 0, 0, 0]
end

private def command(opcode : UInt8, peer_offset : Int32, peer : Bytes) : Bytes
  bytes = Bytes.new(peer_offset + peer.size, 0)
  bytes[0] = opcode
  bytes[peer_offset, peer.size].copy_from(peer)
  bytes
end

private def peer_push(opcode : UInt8, peer : Bytes) : Bytes
  bytes = Bytes.new(8, 0)
  bytes[0] = opcode
  bytes[2, 6].copy_from(peer)
  bytes
end

describe MeshCoreTCPMux::DmRing do
  it "protects the next physical slot across holes and wraparound" do
    ring = MeshCoreTCPMux::DmRing.new
    8.times { |i| ring.accepted(sent((i + 1).to_u32), 0.seconds) }
    ring.available?(0.seconds).should be_false

    # A hole elsewhere does not make the next physical insertion safe.
    ring.confirm(confirmed(4)).should be_true
    ring.available?(0.seconds).should be_false
    ring.confirm(confirmed(1))
    ring.available?(0.seconds).should be_true
    ring.accepted(sent(9), 0.seconds)
    ring.next_slot.should eq(1)
    ring.available?(0.seconds).should be_false
  end

  it "does not advance for zero tokens and expires using the actual timeout" do
    ring = MeshCoreTCPMux::DmRing.new
    ring.accepted(sent(0), 0.seconds)
    ring.next_slot.should eq(0)
    ring.accepted(sent(1, 8_000), 0.seconds)
    ring.pending_count(10.999.seconds).should eq(1)
    ring.pending_count(11.seconds).should eq(0)
  end

  it "settles every pending slot with the same native token" do
    ring = MeshCoreTCPMux::DmRing.new
    ring.accepted(sent(7), 0.seconds)
    ring.accepted(sent(7), 0.seconds)
    ring.pending_count(0.seconds).should eq(2)
    ring.confirm(confirmed(7)).should be_true
    ring.pending_count(0.seconds).should eq(0)
    ring.confirm(confirmed(99)).should be_false
  end

  it "rejects malformed actual responses before reading fields" do
    ring = MeshCoreTCPMux::DmRing.new
    expect_raises(ArgumentError) { ring.accepted(Bytes[0x06], 0.seconds) }
    expect_raises(ArgumentError) { ring.confirm(Bytes[0x82]) }
  end
end

describe MeshCoreTCPMux::RemoteLease do
  peer = Bytes[1, 2, 3, 4, 5, 6]
  other = Bytes[6, 5, 4, 3, 2, 1]

  it "admits one reservation and never delivers a result while tentative" do
    lease = MeshCoreTCPMux::RemoteLease.new
    lease.reserve(10_i64, command(27, 1, peer), 0.seconds).should be_true
    lease.reserve(11_i64, command(27, 1, other), 0.seconds).should be_false
    lease.match(peer_push(0x87, peer), 0.seconds).should be_nil
    lease.occupied?(0.seconds).should be_true
    lease.rejected
    lease.occupied?(0.seconds).should be_false
  end

  it "uses the actual SENT deadline and requires the right peer and result kind" do
    lease = MeshCoreTCPMux::RemoteLease.new
    lease.reserve(10_i64, command(27, 1, peer), 0.seconds)
    lease.accepted(sent(0x1234, 8_000), 0.seconds)
    lease.match(peer_push(0x87, other), 1.second).should be_nil
    lease.match(peer_push(0x8b, peer), 1.second).should be_nil
    lease.match(peer_push(0x87, peer), 1.second).should eq(10_i64)
    lease.occupied?(1.second).should be_false

    lease.reserve(20_i64, command(27, 1, peer), 2.seconds)
    lease.accepted(sent(1, 8_000), 2.seconds)
    lease.occupied?(12.999.seconds).should be_true
    lease.occupied?(13.seconds).should be_false
  end

  it "matches binary results by the actual SENT tag, not peer or command bytes" do
    lease = MeshCoreTCPMux::RemoteLease.new
    lease.reserve(5_i64, command(50, 1, peer), 0.seconds)
    lease.accepted(sent(0x04030201), 0.seconds)
    lease.match(Bytes[0x8c, 0, 9, 9, 9, 9], 1.second).should be_nil
    lease.match(Bytes[0x8c, 0, 1, 2, 3, 4], 1.second).should eq(5_i64)
  end

  it "matches trace tag and authentication and retains an orphaned operation" do
    cmd = Bytes[36, 1, 2, 3, 4, 5, 6, 7, 8, 0, 9]
    lease = MeshCoreTCPMux::RemoteLease.new
    lease.reserve(7_i64, cmd, 0.seconds)
    lease.accepted(sent(0xaabbccdd), 0.seconds)
    lease.owner_gone(7_i64)
    lease.occupied?(0.seconds).should be_true
    lease.match(Bytes[0x89, 0, 0, 0, 1, 2, 3, 4, 0, 0, 0, 0], 0.seconds).should be_nil
    lease.occupied?(0.seconds).should be_true
    lease.match(Bytes[0x89, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8], 0.seconds).should be_nil
    lease.occupied?(0.seconds).should be_false
  end

  it "does not treat four-byte self telemetry as a remote request" do
    expect_raises(ArgumentError) do
      MeshCoreTCPMux::RemoteLease.new.reserve(1_i64, Bytes[39, 0, 0, 0], 0.seconds)
    end
  end
end

describe MeshCoreTCPMux::SigningLease do
  start = Bytes[0x21]
  start_response = Bytes[0x13, 0, 5, 0, 0, 0]

  it "requires an accepted start and enforces owner and advertised byte count" do
    lease = MeshCoreTCPMux::SigningLease.new
    lease.begin_data(1_i64, Bytes[0x22, 1], 0.seconds).should eq(MeshCoreTCPMux::SigningLease::Admission::BadState)
    lease.start(1_i64, start, 0.seconds).should be_true
    lease.begin_data(1_i64, Bytes[0x22, 1], 0.seconds).should eq(MeshCoreTCPMux::SigningLease::Admission::BadState)
    lease.accepted_start(start_response, 0.seconds)
    lease.begin_data(2_i64, Bytes[0x22, 1], 0.seconds).should eq(MeshCoreTCPMux::SigningLease::Admission::BadState)
    lease.begin_data(1_i64, Bytes[0x22, 1, 2, 3], 0.seconds).should eq(MeshCoreTCPMux::SigningLease::Admission::Allowed)
    lease.data_response(Bytes[0], 1.second)
    lease.accepted_bytes.should eq(3)
    lease.begin_data(1_i64, Bytes[0x22, 4, 5, 6], 1.second).should eq(MeshCoreTCPMux::SigningLease::Admission::TableFull)
  end

  it "allows only the owner to restart and resets its byte count" do
    lease = MeshCoreTCPMux::SigningLease.new
    lease.start(1_i64, start, 0.seconds)
    lease.accepted_start(start_response, 0.seconds)
    lease.begin_data(1_i64, Bytes[0x22, 1, 2], 0.seconds)
    lease.data_response(Bytes[0], 0.seconds)
    lease.start(2_i64, start, 0.seconds).should be_false
    lease.start(1_i64, start, 0.seconds).should be_true
    lease.accepted_bytes.should eq(0)
    lease.accepted_start(Bytes[0x13, 0, 9, 0, 0, 0], 0.seconds)
    lease.limit.should eq(9)
  end

  it "does not count failed data, releases on native BAD_STATE, and retains other errors" do
    lease = MeshCoreTCPMux::SigningLease.new
    lease.start(1_i64, start, 0.seconds)
    lease.accepted_start(start_response, 0.seconds)
    lease.begin_data(1_i64, Bytes[0x22, 1, 2], 0.seconds)
    lease.data_response(Bytes[1, 3], 1.second)
    lease.accepted_bytes.should eq(0)
    lease.occupied?(1.second).should be_true
    lease.begin_data(1_i64, Bytes[0x22, 1], 1.second)
    lease.data_response(Bytes[1, 4], 2.seconds)
    lease.occupied?(2.seconds).should be_false
  end

  it "releases on signature, disconnect, and thirty seconds of inactivity" do
    lease = MeshCoreTCPMux::SigningLease.new
    lease.start(1_i64, start, 0.seconds)
    lease.accepted_start(start_response, 0.seconds)
    lease.begin_finish(1_i64, Bytes[0x23], 1.second).should eq(MeshCoreTCPMux::SigningLease::Admission::Allowed)
    signature = Bytes.new(65, 0)
    signature[0] = 0x14
    lease.finish_response(signature, 2.seconds)
    lease.occupied?(2.seconds).should be_false

    lease.start(1_i64, start, 3.seconds)
    lease.accepted_start(start_response, 3.seconds)
    lease.owner_gone(1_i64)
    lease.occupied?(3.seconds).should be_false

    lease.start(1_i64, start, 4.seconds)
    lease.accepted_start(start_response, 4.seconds)
    lease.occupied?(33.999.seconds).should be_true
    lease.occupied?(34.seconds).should be_false
  end

  it "releases finish ownership on a native BAD_STATE" do
    lease = MeshCoreTCPMux::SigningLease.new
    lease.start(1_i64, start, 0.seconds)
    lease.accepted_start(start_response, 0.seconds)
    lease.begin_finish(1_i64, Bytes[0x23], 0.seconds)
    lease.finish_response(Bytes[1, 4], 1.second)
    lease.occupied?(1.second).should be_false
  end
end
