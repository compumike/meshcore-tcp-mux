require "./spec_helper"
require "../src/meshcore_tcp_mux/leases"

private def sent(token : UInt32, timeout_ms = 1_000_u32) : Bytes
  # SENT (0x06): routing mode, four-byte ACK token, then suggested timeout (u32 LE); acceptance
  # only.
  Bytes[
    0x06, 0,
    (token & 0xff).to_u8, ((token >> 8) & 0xff).to_u8, ((token >> 16) & 0xff).to_u8, ((token >> 24) & 0xff).to_u8,
    (timeout_ms & 0xff).to_u8, ((timeout_ms >> 8) & 0xff).to_u8, ((timeout_ms >> 16) & 0xff).to_u8, ((timeout_ms >> 24) & 0xff).to_u8,
  ]
end

private def confirmed(token : UInt32) : Bytes
  # SEND_CONFIRMED (0x82): four-byte ACK token, then round-trip milliseconds (u32 LE); real
  # delivery confirmation.
  Bytes[0x82, (token & 0xff).to_u8, ((token >> 8) & 0xff).to_u8, ((token >> 16) & 0xff).to_u8, ((token >> 24) & 0xff).to_u8, 1, 0, 0, 0]
end

private def command(opcode : UInt8, peer_offset : Int32, peer : Bytes) : Bytes
  # Lease-only command fixture: opcode plus padding to the peer prefix; not full command-shape
  # validation.
  bytes = Bytes.new(peer_offset + peer.size, 0)
  bytes[0] = opcode
  bytes[peer_offset, peer.size].copy_from(peer)
  bytes
end

private def peer_push(opcode : UInt8, peer : Bytes) : Bytes
  # Lease-only push fixture: opcode, metadata byte, six-byte peer prefix at offset 2.
  bytes = Bytes.new(8, 0)
  bytes[0] = opcode
  bytes[2, 6].copy_from(peer)
  bytes
end

describe MeshCoreTCPMux::DmRing do
  # Lease tests exercise bookkeeping independently of the broker and framing.
  # Some command/push fixtures contain only the fields the lease matcher reads;
  # they are not claims of full protocol validity. Tokens/times are little-endian.
  # A SENT reply accepts work; only a matching asynchronous result settles it.

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
    # LeaseTime adds a 25% margin and 1 second: 8000 ms becomes 11 seconds.
    # Check both sides of the exact expiration boundary without wall-clock sleeps.
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

  it "accepts out-of-order confirmations without moving the protected next slot" do
    ring = MeshCoreTCPMux::DmRing.new
    8.times { |i| ring.accepted(sent((i + 1).to_u32), 0.seconds) }
    ring.next_slot.should eq(0)

    # SEND_CONFIRMED pushes may arrive in radio order rather than send order.
    # Settling tokens 6, 3, and 8 creates holes elsewhere in the eight-slot
    # ring, but slot zero still contains token 1 and remains protected.
    [6_u32, 3_u32, 8_u32].each do |token|
      ring.confirm(confirmed(token)).should be_true
    end
    ring.pending_count(0.seconds).should eq(5)
    ring.available?(0.seconds).should be_false
    ring.next_slot.should eq(0)

    ring.confirm(confirmed(1)).should be_true
    ring.available?(0.seconds).should be_true
    ring.accepted(sent(9), 0.seconds)
    ring.next_slot.should eq(1)
  end

  it "rejects malformed actual responses before reading fields" do
    ring = MeshCoreTCPMux::DmRing.new
    # Truncated SENT (0x06): missing routing mode, ACK token, and timeout.
    expect_raises(ArgumentError) { ring.accepted(Bytes[0x06], 0.seconds) }
    # Truncated SEND_CONFIRMED (0x82), missing ACK token and round-trip time.
    expect_raises(ArgumentError) { ring.confirm(Bytes[0x82]) }
  end
end

describe MeshCoreTCPMux::RemoteLease do
  # Synthetic six-byte peer public-key prefix 01..06; used only for routing matches.
  peer = Bytes[1, 2, 3, 4, 5, 6]
  # A different synthetic peer prefix 06..01; must not match the reserved peer.
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

  it "turns unknown acceptance into an ownerless matching lease" do
    lease = MeshCoreTCPMux::RemoteLease.new
    lease.reserve(10_i64, command(27, 1, peer), 0.seconds)
    lease.acceptance_unknown(60.seconds)
    lease.tentative?.should be_false
    lease.owner.should be_nil
    lease.occupied?(1.second).should be_true
    # STATUS_RESPONSE (0x87): reserved metadata, six-byte peer prefix, and one
    # synthetic status byte. It settles the old operation without a recipient.
    lease.match(peer_push(0x87, peer), 1.second).should be_nil
    lease.occupied?(1.second).should be_false

    lease.reserve(11_i64, command(27, 1, peer), 2.seconds)
    lease.acceptance_unknown(60.seconds)
    lease.occupied?(60.seconds).should be_false
  end

  it "matches binary results by the actual SENT tag, not peer or command bytes" do
    lease = MeshCoreTCPMux::RemoteLease.new
    lease.reserve(5_i64, command(50, 1, peer), 0.seconds)
    lease.accepted(sent(0x04030201), 0.seconds)
    # BINARY_RESPONSE (0x8c), metadata byte 0 and four-byte SENT tag 9 9 9 9.
    # Binary requests do not route replies by public-key prefix. The actual
    # SENT token becomes the reply tag, so a different tag must not settle it.
    lease.match(Bytes[0x8c, 0, 9, 9, 9, 9], 1.second).should be_nil
    # BINARY_RESPONSE (0x8c), metadata byte 0 and four-byte SENT tag 1 2 3 4.
    lease.match(Bytes[0x8c, 0, 1, 2, 3, 4], 1.second).should eq(5_i64)
  end

  it "matches trace tag and authentication and retains an orphaned operation" do
    # SEND_TRACE_PATH (36): four-byte tag, four-byte authentication value, flags 0 (one-byte
    # hashes), then one path hash.
    cmd = Bytes[36, 1, 2, 3, 4, 5, 6, 7, 8, 0, 9]
    lease = MeshCoreTCPMux::RemoteLease.new
    lease.reserve(7_i64, cmd, 0.seconds)
    lease.accepted(sent(0xaabbccdd), 0.seconds)
    lease.owner_gone(7_i64)
    lease.occupied?(0.seconds).should be_true
    # TRACE_DATA (0x89): metadata/path fields, tag at bytes 4..7 and authentication at 8..11;
    # matching needs both.
    # Both replies repeat the command tag 01 02 03 04. Only the second repeats
    # authentication 05 06 07 08 too. The matched orphan returns nil because its
    # client left, but occupied? must still become false to free the radio slot.
    lease.match(Bytes[0x89, 0, 0, 0, 1, 2, 3, 4, 0, 0, 0, 0], 0.seconds).should be_nil
    lease.occupied?(0.seconds).should be_true
    # TRACE_DATA (0x89): metadata/path fields, tag at bytes 4..7 and authentication at 8..11;
    # matching needs both.
    lease.match(Bytes[0x89, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8], 0.seconds).should be_nil
    lease.occupied?(0.seconds).should be_false
  end

  it "does not treat four-byte self telemetry as a remote request" do
    expect_raises(ArgumentError) do
      # Self TELEMETRY request (39): four-byte form with three zero option/reserved bytes, no
      # remote peer.
      MeshCoreTCPMux::RemoteLease.new.reserve(1_i64, Bytes[39, 0, 0, 0], 0.seconds)
    end
  end
end

describe MeshCoreTCPMux::SigningLease do
  # SIGN_START (33/0x21): start the shared signing session.
  start = Bytes[0x21]
  # SIGN_START response (0x13): reserved 0, byte limit 5 (u32 LE).
  start_response = Bytes[0x13, 0, 5, 0, 0, 0]

  it "requires an accepted start and enforces owner and advertised byte count" do
    lease = MeshCoreTCPMux::SigningLease.new
    # SIGN_DATA (34/0x22), followed by 1 literal data byte(s); only accepted data counts against
    # the signing budget.
    lease.begin_data(1_i64, Bytes[0x22, 1], 0.seconds).should eq(MeshCoreTCPMux::SigningLease::Admission::BadState)
    lease.start(1_i64, start, 0.seconds).should be_true
    # SIGN_DATA (34/0x22), followed by 1 literal data byte(s); only accepted data counts against
    # the signing budget.
    lease.begin_data(1_i64, Bytes[0x22, 1], 0.seconds).should eq(MeshCoreTCPMux::SigningLease::Admission::BadState)
    lease.accepted_start(start_response, 0.seconds)
    # SIGN_DATA (34/0x22), followed by 1 literal data byte(s); only accepted data counts against
    # the signing budget.
    lease.begin_data(2_i64, Bytes[0x22, 1], 0.seconds).should eq(MeshCoreTCPMux::SigningLease::Admission::BadState)
    # SIGN_DATA (34/0x22), followed by 3 literal data byte(s); only accepted data counts against
    # the signing budget.
    lease.begin_data(1_i64, Bytes[0x22, 1, 2, 3], 0.seconds).should eq(MeshCoreTCPMux::SigningLease::Admission::Allowed)
    # OK (0x00): signing data appended to the operation.
    lease.data_response(Bytes[0], 1.second)
    lease.accepted_bytes.should eq(3)
    # SIGN_DATA (34/0x22), followed by 3 literal data byte(s); only accepted data counts against
    # the signing budget.
    lease.begin_data(1_i64, Bytes[0x22, 4, 5, 6], 1.second).should eq(MeshCoreTCPMux::SigningLease::Admission::TableFull)
  end

  it "allows only the owner to restart and resets its byte count" do
    lease = MeshCoreTCPMux::SigningLease.new
    lease.start(1_i64, start, 0.seconds)
    lease.accepted_start(start_response, 0.seconds)
    # SIGN_DATA (34/0x22), followed by 2 literal data byte(s); only accepted data counts against
    # the signing budget.
    lease.begin_data(1_i64, Bytes[0x22, 1, 2], 0.seconds)
    # OK (0x00): signing data appended before the explicit restart.
    lease.data_response(Bytes[0], 0.seconds)
    lease.start(2_i64, start, 0.seconds).should be_false
    lease.start(1_i64, start, 0.seconds).should be_true
    lease.accepted_bytes.should eq(0)
    # SIGN_START response (0x13): reserved 0, byte limit 9 (u32 LE).
    lease.accepted_start(Bytes[0x13, 0, 9, 0, 0, 0], 0.seconds)
    lease.limit.should eq(9)
  end

  it "does not count failed data, releases on native BAD_STATE, and retains other errors" do
    lease = MeshCoreTCPMux::SigningLease.new
    lease.start(1_i64, start, 0.seconds)
    lease.accepted_start(start_response, 0.seconds)
    # SIGN_DATA (34/0x22), followed by 2 literal data byte(s); only accepted data counts against
    # the signing budget.
    lease.begin_data(1_i64, Bytes[0x22, 1, 2], 0.seconds)
    # ERR (0x01), TABLE_FULL.
    lease.data_response(Bytes[1, 3], 1.second)
    lease.accepted_bytes.should eq(0)
    lease.occupied?(1.second).should be_true
    # SIGN_DATA (34/0x22), followed by 1 literal data byte(s); only accepted data counts against
    # the signing budget.
    lease.begin_data(1_i64, Bytes[0x22, 1], 1.second)
    # ERR (0x01), BAD_STATE.
    lease.data_response(Bytes[1, 4], 2.seconds)
    lease.occupied?(2.seconds).should be_false
  end

  it "releases on signature, disconnect, and thirty seconds of inactivity" do
    lease = MeshCoreTCPMux::SigningLease.new
    lease.start(1_i64, start, 0.seconds)
    lease.accepted_start(start_response, 0.seconds)
    # SIGN_FINISH (35/0x23): finish the current signing session and await SIGNATURE.
    lease.begin_finish(1_i64, Bytes[0x23], 1.second).should eq(MeshCoreTCPMux::SigningLease::Admission::Allowed)
    # SIGNATURE response: opcode 0x14 plus a 64-byte dummy signature; no cryptography is
    # performed.
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
    # SIGN_FINISH (35/0x23): finish the current signing session and await SIGNATURE.
    lease.begin_finish(1_i64, Bytes[0x23], 0.seconds)
    # ERR (0x01), BAD_STATE.
    lease.finish_response(Bytes[1, 4], 1.second)
    lease.occupied?(1.second).should be_false
  end
end
