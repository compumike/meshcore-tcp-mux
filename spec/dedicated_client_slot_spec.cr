require "./spec_helper"
require "../src/meshcore_tcp_mux/broker"
require "../src/meshcore_tcp_mux/dedicated_client_slot"

private alias DedicatedAction = MeshCoreTCPMux::Action

private def dedicated_sends(actions : Array(DedicatedAction), session : Int64) : Array(MeshCoreTCPMux::SendFrame)
  actions.compact_map do |action|
    action if action.is_a?(MeshCoreTCPMux::SendFrame) && action.session == session
  end
end

private def direct_inbox_item(seed : UInt8) : Bytes
  # CONTACT_MESSAGE (0x07): the remaining bytes form the minimum legacy header;
  # seed makes queue ordering and replacement preservation visible.
  Bytes.new(13, seed).tap do |payload|
    payload[0] = 0x07_u8
    payload[8] = 0_u8 # TXT_TYPE_PLAIN.
  end
end

describe MeshCoreTCPMux::DedicatedClientSlot do
  # These model specs exercise the firmware-compatible queue independently of
  # sockets. Payloads are decoded native frames, not TCP envelopes.

  it "keeps FIFO order below capacity and tracks broker-accepted deliveries" do
    slot = MeshCoreTCPMux::DedicatedClientSlot.new(5002, 5002)
    first = direct_inbox_item(1_u8)
    second = direct_inbox_item(2_u8)

    slot.enqueue_offline(first, 2).should eq(MeshCoreTCPMux::DedicatedClientSlot::EnqueueResult::Added)
    slot.enqueue_offline(second, 2).should eq(MeshCoreTCPMux::DedicatedClientSlot::EnqueueResult::Added)
    slot.offline_queue.to_a.should eq([first, second])
    slot.enqueued.should eq(2_u64)
    slot.high_water.should eq(2)
  end

  it "recognizes every firmware channel class and evicts the oldest channel entry" do
    [
      0x08_u8, # CHANNEL_MSG_RECV: legacy channel text.
      0x11_u8, # CHANNEL_MSG_RECV_V3: native-v3 channel text.
      0x1b_u8, # CHANNEL_DATA_RECV: binary channel data.
    ].each do |channel_opcode|
      slot = MeshCoreTCPMux::DedicatedClientSlot.new(5002, 5002)
      oldest_direct = direct_inbox_item(1_u8)
      oldest_channel = Bytes[channel_opcode, 0xaa_u8]
      newer_channel = Bytes[0x08_u8, 0xbb_u8] # CHANNEL_MSG_RECV.
      arrival = direct_inbox_item(9_u8)
      slot.enqueue_offline(oldest_direct, 3)
      slot.enqueue_offline(oldest_channel, 3)
      slot.enqueue_offline(newer_channel, 3)

      result = slot.enqueue_offline(arrival, 3)
      result.should eq(MeshCoreTCPMux::DedicatedClientSlot::EnqueueResult::ChannelEvicted)
      slot.offline_queue.to_a.should eq([oldest_direct, newer_channel, arrival])
      slot.channel_evictions.should eq(1_u64)
    end
  end

  it "retains existing direct messages and discards a new arrival when none can be sacrificed" do
    slot = MeshCoreTCPMux::DedicatedClientSlot.new(5002, 5002)
    first = direct_inbox_item(1_u8)
    second = direct_inbox_item(2_u8)
    arrival = Bytes[0x08_u8, 0xcc_u8] # CHANNEL_MSG_RECV.
    slot.enqueue_offline(first, 2)
    slot.enqueue_offline(second, 2)

    result = slot.enqueue_offline(arrival, 2)
    result.should eq(MeshCoreTCPMux::DedicatedClientSlot::EnqueueResult::NewMessageDiscarded)
    slot.offline_queue.to_a.should eq([first, second])
    slot.new_message_discards.should eq(1_u64)
  end
end

describe MeshCoreTCPMux::Broker, "dedicated client queues" do
  # Broker integration proves stable port identity, detached fan-out, and the
  # separation between persistent dedicated queues and transient sessions.

  it "fans an authorized physical result to detached slots without letting hints pop upstream" do
    first = MeshCoreTCPMux::DedicatedClientSlot.new(5002, 5002)
    second = MeshCoreTCPMux::DedicatedClientSlot.new(5003, 5003)
    slots = {5002 => first, 5003 => second}
    broker = MeshCoreTCPMux::Broker.new(1_i64, Bytes.new(32, 0x55_u8), dedicated_slots: slots)
    broker.admit(1_i64, Time::Span.zero)
    admission = dedicated_sends(broker.take_actions, 1_i64).first
    broker.written(1_i64, admission.epoch, admission.write_id, Time::Span.zero)
    broker.take_actions

    broker.upstream_frame(Bytes[0x83_u8], Time::Span.zero) # MSG_WAITING.
    hint_actions = broker.take_actions
    dedicated_sends(hint_actions, 0_i64).should be_empty
    hint = dedicated_sends(hint_actions, 1_i64).first
    broker.written(1_i64, hint.epoch, hint.write_id, Time::Span.zero)
    broker.take_actions
    broker.tick(5.seconds)
    dedicated_sends(broker.take_actions, 0_i64).should be_empty

    broker.client_frame(1_i64, Bytes[10_u8], 5.seconds) # SYNC_NEXT_MESSAGE.
    pop = dedicated_sends(broker.take_actions, 0_i64).first
    pop.payload.should eq(Bytes[10_u8])
    item = direct_inbox_item(7_u8)
    broker.upstream_frame(item, 5.seconds)
    actions = broker.take_actions
    dedicated_sends(actions, 1_i64).map(&.payload).should contain(item)
    first.offline_queue.to_a.should eq([item])
    second.offline_queue.to_a.should eq([item])
  end

  it "replaces a dedicated attachment while preserving its unread queue" do
    slot = MeshCoreTCPMux::DedicatedClientSlot.new(5002, 5002)
    item = direct_inbox_item(4_u8)
    slot.enqueue_offline(item, 128)
    broker = MeshCoreTCPMux::Broker.new(
      2_i64, Bytes.new(32, 0x66_u8), dedicated_slots: {5002 => slot}
    )
    broker.admit(10_i64, Time::Span.zero, 5002)
    broker.take_actions
    broker.admit(11_i64, 1.millisecond, 5002)
    actions = broker.take_actions

    actions.compact_map(&.as?(MeshCoreTCPMux::CloseSession)).map(&.session).should eq([10_i64])
    slot.attached_session_id.should eq(11_i64)
    slot.offline_queue.to_a.should eq([item])
    broker.sessions.has_key?(10_i64).should be_false
    broker.sessions[11_i64].target_version.should eq(0_u8)
  end

  it "continues fan-out when one dedicated queue discards at capacity" do
    config = MeshCoreTCPMux::Config.new
    config.offline_queue_size = 1
    blocked = MeshCoreTCPMux::DedicatedClientSlot.new(5002, 5002)
    replaceable = MeshCoreTCPMux::DedicatedClientSlot.new(5003, 5003)
    retained = direct_inbox_item(1_u8)
    blocked.enqueue_offline(retained, 1)
    replaceable.enqueue_offline(Bytes[0x08_u8, 1_u8], 1) # CHANNEL_MSG_RECV.
    broker = MeshCoreTCPMux::Broker.new(
      3_i64, Bytes.new(32, 0x77_u8), config,
      dedicated_slots: {5002 => blocked, 5003 => replaceable}
    )
    broker.admit(1_i64, Time::Span.zero)
    broker.take_actions
    broker.client_frame(1_i64, Bytes[10_u8], Time::Span.zero) # SYNC_NEXT_MESSAGE.
    broker.take_actions
    arrival = direct_inbox_item(9_u8)
    broker.upstream_frame(arrival, Time::Span.zero)
    actions = broker.take_actions

    blocked.offline_queue.to_a.should eq([retained])
    replaceable.offline_queue.to_a.should eq([arrival])
    dedicated_sends(actions, 1_i64).map(&.payload).should contain(arrival)
    actions.compact_map(&.as?(MeshCoreTCPMux::Diagnostic)).map(&.message).join('\n').should contain("new_message_discarded")
  end
end
