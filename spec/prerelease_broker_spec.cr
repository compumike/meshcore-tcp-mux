require "./spec_helper"
require "../src/meshcore_tcp_mux/broker"
require "../src/meshcore_tcp_mux/dedicated_client_slot"

private alias PrereleaseBrokerAction = MeshCoreTCPMux::Action

private def prerelease_sends(actions : Array(PrereleaseBrokerAction), session : Int64) : Array(MeshCoreTCPMux::SendFrame)
  # Select decoded frame writes for one endpoint. Session zero is the physical
  # companion; positive IDs are transient downstream socket identities.
  actions.compact_map do |action|
    action.as?(MeshCoreTCPMux::SendFrame).try { |send| send if send.session == session }
  end
end

private def prerelease_direct_item(seed : UInt8) : Bytes
  # CONTACT_MESSAGE (0x07): a minimum legacy direct-message header. Byte 1 is
  # synthetic occurrence data, and byte 8 is TXT_TYPE_PLAIN (0x00).
  Bytes.new(13, seed).tap do |payload|
    payload[0] = 0x07_u8
    payload[8] = 0_u8
  end
end

private def prerelease_status_request(peer : Bytes) : Bytes
  # SEND_STATUS_REQ (0x1b): opcode plus a 32-byte public key. Remote-result
  # routing uses the first six key bytes, supplied here as a synthetic prefix.
  Bytes.new(33, 0_u8).tap do |payload|
    payload[0] = 27_u8
    payload[1, 6].copy_from(peer)
  end
end

private def prerelease_dm(seed : UInt8) : Bytes
  # SEND_TXT_MSG (0x02): plain text type, attempt one, synthetic timestamp,
  # six-byte synthetic destination prefix, and a one-byte body.
  Bytes[
    2_u8, 0_u8, 1_u8,
    seed, 0_u8, 0_u8, 0_u8,
    1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8,
    seed,
  ]
end

private def prerelease_sent(token : UInt32, timeout_ms : UInt32) : Bytes
  # SENT (0x06): route byte, expected-ACK token and suggested timeout. Both
  # integers use the native little-endian wire order.
  Bytes[
    6_u8, 0_u8,
    (token & 0xff).to_u8, ((token >> 8) & 0xff).to_u8,
    ((token >> 16) & 0xff).to_u8, ((token >> 24) & 0xff).to_u8,
    (timeout_ms & 0xff).to_u8, ((timeout_ms >> 8) & 0xff).to_u8,
    ((timeout_ms >> 16) & 0xff).to_u8, ((timeout_ms >> 24) & 0xff).to_u8,
  ]
end

private def prerelease_confirmed(token : UInt32) : Bytes
  # SEND_CONFIRMED (0x82): expected-ACK token followed by a synthetic zero
  # round-trip time, all little-endian.
  Bytes[
    0x82_u8,
    (token & 0xff).to_u8, ((token >> 8) & 0xff).to_u8,
    ((token >> 16) & 0xff).to_u8, ((token >> 24) & 0xff).to_u8,
    0_u8, 0_u8, 0_u8, 0_u8,
  ]
end

private class PersistentAckRingModel
  # Models only the pinned firmware's persistent circular ACK-table cursor.
  # The model intentionally survives the mux's upstream TCP epoch boundary.
  CAPACITY = 8

  @slots = Array(UInt32).new(CAPACITY, 0_u32)
  @next_slot = 0

  def accept(token : UInt32) : UInt32
    # Firmware overwrites the current physical slot and advances for every
    # accepted nonzero token before it writes SENT to the TCP interface.
    overwritten = @slots[@next_slot]
    @slots[@next_slot] = token
    @next_slot = (@next_slot + 1) % CAPACITY
    overwritten
  end

  def confirm(token : UInt32) : Nil
    # The pinned firmware clears the first physical entry matching an ACK.
    if index = @slots.index(token)
      @slots[index] = 0_u32
    end
  end
end

describe MeshCoreTCPMux::Broker, "pre-release ownership regressions" do
  # These event-order specs target the gaps identified by the earlier release
  # audit: output acceptance at dedicated queue dequeue, callbacks from replaced
  # sockets, and a remote lease whose owner disappears around SENT acceptance.

  it "retains a dedicated item when output acceptance fails and serves it after replacement" do
    config = MeshCoreTCPMux::Config.new
    config.output_frames = 1
    slot = MeshCoreTCPMux::DedicatedClientSlot.new(5047, 5047)
    item = prerelease_direct_item(0x41_u8)
    slot.enqueue_offline(item, 4)
    broker = MeshCoreTCPMux::Broker.new(
      101_i64, Bytes.new(32, 0x51_u8), config, dedicated_slots: {5047 => slot}
    )

    broker.admit(10_i64, Time::Span.zero, 5047)
    admission_actions = broker.take_actions
    old_hint = prerelease_sends(admission_actions, 10_i64).first
    # Keep the admission MSG_WAITING (0x83) unacknowledged. With an output
    # budget of one, the following ordinary inbox reply cannot be accepted.
    old_hint.payload.should eq(Bytes[0x83_u8])
    broker.client_frame(10_i64, Bytes[10_u8], 1.millisecond) # SYNC_NEXT_MESSAGE.
    failed_delivery = broker.take_actions
    failed_delivery.compact_map(&.as?(MeshCoreTCPMux::CloseSession)).map(&.session).should eq([10_i64])
    slot.offline_queue.to_a.should eq([item])
    slot.delivered.should eq(0_u64)

    broker.admit(11_i64, 2.milliseconds, 5047)
    replacement_hint = prerelease_sends(broker.take_actions, 11_i64).first
    broker.written(11_i64, replacement_hint.epoch, replacement_hint.write_id, 2.milliseconds)
    broker.take_actions
    broker.client_frame(11_i64, Bytes[10_u8], 3.milliseconds) # SYNC_NEXT_MESSAGE.
    delivered = prerelease_sends(broker.take_actions, 11_i64)
    delivered.map(&.payload).should eq([item])
    slot.offline_queue.should be_empty
    slot.delivered.should eq(1_u64)
  end

  it "ignores every late callback from a replaced dedicated socket" do
    slot = MeshCoreTCPMux::DedicatedClientSlot.new(5047, 5047)
    broker = MeshCoreTCPMux::Broker.new(
      102_i64, Bytes.new(32, 0x52_u8), dedicated_slots: {5047 => slot}
    )
    broker.admit(20_i64, Time::Span.zero, 5047)
    old_hint = prerelease_sends(broker.take_actions, 20_i64).first

    broker.admit(21_i64, 1.millisecond, 5047)
    replacement_actions = broker.take_actions
    new_hint = prerelease_sends(replacement_actions, 21_i64).first
    slot.attached_session_id.should eq(21_i64)

    # Runtime fibers may report the old socket's close, successful write, or
    # failed write after replacement. Session IDs fence all three callbacks.
    broker.client_closed(20_i64, 2.milliseconds, "late old close")
    broker.written(20_i64, old_hint.epoch, old_hint.write_id, 2.milliseconds)
    broker.write_failed(20_i64, old_hint.epoch, "late old failure", 2.milliseconds)
    broker.take_actions.compact_map(&.as?(MeshCoreTCPMux::CloseSession)).should be_empty
    broker.failed.should be_false
    broker.sessions.has_key?(21_i64).should be_true
    broker.sessions[21_i64].writes.has_key?(new_hint.write_id).should be_true
    slot.attached_session_id.should eq(21_i64)

    broker.written(21_i64, new_hint.epoch, new_hint.write_id, 3.milliseconds)
    broker.sessions[21_i64].writes.should be_empty
  end

  it "keeps an accepted remote lease ownerless after dedicated replacement" do
    slot = MeshCoreTCPMux::DedicatedClientSlot.new(5047, 5047)
    broker = MeshCoreTCPMux::Broker.new(
      103_i64, Bytes.new(32, 0x53_u8), dedicated_slots: {5047 => slot}
    )
    peer = Bytes[1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8]
    status = prerelease_status_request(peer)
    broker.admit(30_i64, Time::Span.zero, 5047)
    old_hint = prerelease_sends(broker.take_actions, 30_i64).first
    broker.written(30_i64, old_hint.epoch, old_hint.write_id, Time::Span.zero)
    broker.take_actions
    broker.client_frame(30_i64, status, Time::Span.zero)
    prerelease_sends(broker.take_actions, 0_i64).map(&.payload).should eq([status])

    # Replacement removes the old transient owner while its command still owns
    # the immediate SENT reply. The lease must survive with no recipient.
    broker.admit(31_i64, 1.millisecond, 5047)
    replacement_hint = prerelease_sends(broker.take_actions, 31_i64).first
    broker.written(31_i64, replacement_hint.epoch, replacement_hint.write_id, 1.millisecond)
    broker.take_actions
    # SENT (0x06): route byte, synthetic token 0x04030201, and 8000 ms timeout
    # as a four-byte little-endian integer. This accepts RF work only.
    broker.upstream_frame(Bytes[6_u8, 1_u8, 1_u8, 2_u8, 3_u8, 4_u8, 0x40_u8, 0x1f_u8, 0_u8, 0_u8], 2.milliseconds)
    prerelease_sends(broker.take_actions, 31_i64).should be_empty

    broker.client_frame(31_i64, status, 3.milliseconds)
    rejected = broker.take_actions
    prerelease_sends(rejected, 0_i64).should be_empty
    prerelease_sends(rejected, 31_i64).map(&.payload).should eq([Bytes[1_u8, 4_u8]]) # ERR(BAD_STATE).

    # STATUS_RESPONSE (0x87): metadata byte, matching peer prefix, and one
    # synthetic status byte. It settles the orphan lease without reaching the
    # replacement, after which that replacement may start its own request.
    old_result = Bytes[0x87_u8, 0_u8] + peer + Bytes[0_u8]
    broker.upstream_frame(old_result, 4.milliseconds)
    prerelease_sends(broker.take_actions, 31_i64).should be_empty
    broker.client_frame(31_i64, status, 5.milliseconds)
    prerelease_sends(broker.take_actions, 0_i64).map(&.payload).should eq([status])
  end

  it "does not overwrite a live physical ACK slot after unknown DM acceptance" do
    # Start with a full, aligned physical/local ring. Token 1 is then confirmed,
    # making the shared next slot reusable while token 2 remains live. The
    # 120-second fixture is a valid u32 native timeout and remains protected by
    # the mux until 151 seconds (1.25x plus one second).
    physical = PersistentAckRingModel.new
    radio = MeshCoreTCPMux::CompanionRadioState.new
    8.times do |offset|
      token = (offset + 1).to_u32
      physical.accept(token).should eq(0_u32)
      radio.dm_ring.accepted(prerelease_sent(token, 120_000_u32), Time::Span.zero)
    end
    physical.confirm(1_u32)
    radio.dm_ring.confirm(prerelease_confirmed(1_u32)).should be_true

    first = MeshCoreTCPMux::Broker.new(
      104_i64, Bytes.new(32, 0x54_u8), radio_state: radio
    )
    first.admit(40_i64, Time::Span.zero)
    first_hint = prerelease_sends(first.take_actions, 40_i64).first
    first.written(40_i64, first_hint.epoch, first_hint.write_id, Time::Span.zero)
    first.take_actions
    first.client_frame(40_i64, prerelease_dm(9_u8), Time::Span.zero)
    # The actual upstream write means firmware may accept token 9 and advance
    # before its SENT reply reaches the mux. Model that physical effect, then
    # fail the epoch while the broker still owns the unanswered command.
    prerelease_sends(first.take_actions, 0_i64).size.should eq(1)
    physical.accept(9_u32).should eq(0_u32)
    first.fail_epoch("synthetic disconnect before SENT")
    first.take_actions

    # Runtime retains CompanionRadioState for a successor epoch with the same
    # validated node identity. Ask the real successor broker to admit another
    # DM exactly when the configured quarantine expires. Advance the independent
    # physical model only if the broker truly emits that command upstream.
    # Pinned firmware can estimate more than 60 seconds: direct timeout is
    # 500 + ((airtime_ms * 6 + 250) * (hop_count + 1)); 114 ms and 63 hops is
    # already 60,276 ms. Capacity safety requires the live token 2 to survive.
    second = MeshCoreTCPMux::Broker.new(
      105_i64, Bytes.new(32, 0x54_u8), now: 60.seconds, radio_state: radio
    )
    second.admit(41_i64, 60.seconds)
    second_hint = prerelease_sends(second.take_actions, 41_i64).first
    second.written(41_i64, second_hint.epoch, second_hint.write_id, 60.seconds)
    second.take_actions
    second.client_frame(41_i64, prerelease_dm(10_u8), 60.seconds)
    successor_upstream = prerelease_sends(second.take_actions, 0_i64)
    overwritten = successor_upstream.empty? ? 0_u32 : physical.accept(10_u32)
    overwritten.should eq(0_u32)
  end
end
