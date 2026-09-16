require "./spec_helper"
require "../src/meshcore_tcp_mux/broker"

private alias MuxAction = MeshCoreTCPMux::Action

private def sends(actions : Array(MuxAction), session : Int64? = nil) : Array(MeshCoreTCPMux::SendFrame)
  actions.compact_map do |action|
    if action.is_a?(MeshCoreTCPMux::SendFrame) && (session.nil? || action.session == session)
      action
    end
  end
end

private def close_actions(actions : Array(MuxAction)) : Array(MeshCoreTCPMux::CloseSession)
  actions.select(MeshCoreTCPMux::CloseSession)
end

private def one(items : Array(T)) : T forall T
  items.size.should eq(1)
  items[0]
end

private def settle_pumps(broker : MeshCoreTCPMux::Broker, now = Time::Span.zero) : Nil
  # Admission schedules internal inbox probes. Answer them empty before each test;
  # otherwise an initialization pop would obscure the user-command ordering.
  loop do
    actions = broker.take_actions
    # Treat downstream admission hints as accepted writes so later upstream or
    # local-queue hints can be emitted independently.
    sends(actions).reject { |send| send.session == 0_i64 }.each do |send|
      broker.written(send.session, send.epoch, send.write_id, now)
    end
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    pop = sends(actions, 0_i64).find { |send| send.payload == Bytes[10_u8] }
    break unless pop
    broker.written(0_i64, pop.epoch, pop.write_id, now)
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    broker.upstream_frame(Bytes[10_u8], now)
  end
end

private def admit_settled(broker : MeshCoreTCPMux::Broker, ids : Array(Int64), now = Time::Span.zero) : Array(MeshCoreTCPMux::Action)
  ids.each { |id| broker.admit(id, now) }
  settle_pumps(broker, now)
  broker.take_actions
end

private def device_info : Bytes
  # DEVICE_INFO: exactly 82 bytes, opcode 0x0d, protocol version 13 at offset 1; other fields
  # are synthetic.
  Bytes.new(82, 0_u8).tap do |payload|
    payload[0] = 0x0d
    payload[1] = 13
  end
end

private def v3_contact(seed = 0_u8) : Bytes
  # Minimal V3 DM (16-byte header); seed distinguishes messages, while offset 11 stays
  # plain-text type 0.
  Bytes.new(16, seed).tap do |payload|
    payload[0] = 0x10
    payload[11] = 0 # TXT_TYPE_PLAIN; seed 2 must not accidentally mean signed.
  end
end

describe MeshCoreTCPMux::Broker do
  # Core broker routing/inbox tests. Session 0 denotes the physical companion;
  # positive session IDs are clients. take_actions drains observable effects, not
  # radio responses. Tests inject replies explicitly and use synthetic identities.
  # Byte arrays are decoded protocol payloads; integer fields are little-endian.

  it "routes identical generic responses to only the active owner and preserves local FIFO" do
    # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
    broker = MeshCoreTCPMux::Broker.new(7_i64, Bytes.new(32))
    admit_settled(broker, [1_i64, 2_i64])

    # GET_DEVICE_TIME (5): local clock query.
    broker.client_frame(1_i64, Bytes[5_u8], Time::Span.zero)
    first = one(sends(broker.take_actions, 0_i64))
    # GET_DEVICE_TIME (5): local clock query.
    first.payload.should eq(Bytes[5_u8])
    # Unsupported/reserved command opcode 0xfe: must be refused locally.
    # This locally rejected command still belongs AFTER client 1's active query
    # in its FIFO; a local error must not overtake that query's response.
    broker.client_frame(1_i64, Bytes[0xfe_u8], Time::Span.zero)
    # GET_DEVICE_TIME (5): local clock query.
    broker.client_frame(2_i64, Bytes[5_u8], Time::Span.zero)

    # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
    # byte-for-byte.
    broker.upstream_frame(Bytes[9_u8, 1_u8, 2_u8, 3_u8, 4_u8], Time::Span.zero)
    actions = broker.take_actions
    sends(actions, 1_i64).map(&.payload).should eq([
      # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
      # byte-for-byte.
      Bytes[9_u8, 1_u8, 2_u8, 3_u8, 4_u8],
      # ERR (0x01), ERR_UNSUPPORTED_CMD.
      Bytes[1_u8, MeshCoreTCPMux::Protocol::ERR_UNSUPPORTED_CMD],
    ])
    sends(actions, 2_i64).should be_empty
    # GET_DEVICE_TIME (5): local clock query.
    one(sends(actions, 0_i64)).payload.should eq(Bytes[5_u8])

    # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
    # byte-for-byte.
    broker.upstream_frame(Bytes[9_u8, 5_u8, 6_u8, 7_u8, 8_u8], Time::Span.zero)
    # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
    # byte-for-byte.
    one(sends(broker.take_actions, 2_i64)).payload.should eq(Bytes[9_u8, 5_u8, 6_u8, 7_u8, 8_u8])
  end

  it "normalizes DEVICE_QUERY upstream but preserves the real reply and requested client version" do
    # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
    broker = MeshCoreTCPMux::Broker.new(1_i64, Bytes.new(32))
    admit_settled(broker, [1_i64])
    # DEVICE_QUERY (22), requested protocol target 2; trailing sentinel must survive
    # normalization.
    original = Bytes[22_u8, 2_u8, 0xaa_u8]
    broker.client_frame(1_i64, original, Time::Span.zero)
    # DEVICE_QUERY (22), requested protocol target 13; trailing sentinel must survive
    # normalization.
    one(sends(broker.take_actions, 0_i64)).payload.should eq(Bytes[22_u8, 13_u8, 0xaa_u8])
    broker.upstream_frame(device_info, Time::Span.zero)
    one(sends(broker.take_actions, 1_i64)).payload.should eq(device_info)
    broker.sessions[1_i64].target_version.should eq(2_u8)
    # DEVICE_QUERY (22), requested protocol target 2; trailing sentinel must survive
    # normalization.
    original.should eq(Bytes[22_u8, 2_u8, 0xaa_u8])
  end

  it "owns a contacts stream through its terminator and drains it after owner disconnect" do
    # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
    broker = MeshCoreTCPMux::Broker.new(2_i64, Bytes.new(32))
    admit_settled(broker, [1_i64, 2_i64])
    # GET_CONTACTS (4); optional trailing u32 LE is the 'since' timestamp.
    broker.client_frame(1_i64, Bytes[4_u8], Time::Span.zero)
    # GET_CONTACTS (4); optional trailing u32 LE is the 'since' timestamp.
    one(sends(broker.take_actions, 0_i64)).payload.should eq(Bytes[4_u8])
    # GET_DEVICE_TIME (5): local clock query.
    broker.client_frame(2_i64, Bytes[5_u8], Time::Span.zero)

    # CONTACTS_START (0x02) with contact count 9 (u32 little-endian).
    broker.upstream_frame(Bytes[2_u8, 9_u8, 0_u8, 0_u8, 0_u8], 1.millisecond)
    actions = broker.take_actions
    one(sends(actions, 1_i64)).payload[0].should eq(2_u8)
    sends(actions, 0_i64).should be_empty
    broker.client_closed(1_i64, 2.milliseconds)
    broker.take_actions

    # CONTACT record: 148 bytes total (opcode 3 plus native contact fields); unused fields are
    # synthetic zeroes.
    contact = Bytes.new(148, 0_u8); contact[0] = 3
    broker.upstream_frame(contact, 3.milliseconds)
    sends(broker.take_actions, 0_i64).should be_empty
    # END_OF_CONTACTS (0x04), followed by the u32 LE last-modified timestamp.
    broker.upstream_frame(Bytes[4_u8, 1_u8, 0_u8, 0_u8, 0_u8], 4.milliseconds)
    actions = broker.take_actions
    sends(actions, 1_i64).should be_empty
    # GET_DEVICE_TIME (5): local clock query.
    one(sends(actions, 0_i64)).payload.should eq(Bytes[5_u8])
  end

  it "fans one physical inbox item to admission-time members with per-session version conversion" do
    # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
    broker = MeshCoreTCPMux::Broker.new(3_i64, Bytes.new(32))
    admit_settled(broker, [1_i64, 2_i64])
    broker.sessions[2_i64].target_version = 3_u8
    # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
    broker.upstream_frame(Bytes[0x83_u8], Time::Span.zero)
    actions = broker.take_actions
    one(sends(actions, 1_i64)).payload.should eq(Bytes[0x83_u8])
    one(sends(actions, 2_i64)).payload.should eq(Bytes[0x83_u8])
    # The hint does not move custody. Client 1's explicit sync authorizes the
    # drain and remains pending until the physical result arrives.
    broker.client_frame(1_i64, Bytes[10_u8], Time::Span.zero)
    pop = one(sends(broker.take_actions, 0_i64))
    item = v3_contact(0x44_u8)
    broker.upstream_frame(item, Time::Span.zero)
    actions = broker.take_actions
    legacy = one(sends(actions, 1_i64)).payload
    # CONTACT_MESSAGE (0x07) legacy opcode; append the original body after stripping V3
    # metadata.
    legacy.should eq(Bytes[7_u8] + item[4..])
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    broker.client_frame(2_i64, Bytes[10_u8], Time::Span.zero)
    one(sends(broker.take_actions, 2_i64)).payload.should eq(item)
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    pop.payload.should eq(Bytes[10_u8])
  end

  it "does not let an old empty observation consume a newer notification generation" do
    # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
    broker = MeshCoreTCPMux::Broker.new(4_i64, Bytes.new(32))
    admit_settled(broker, [1_i64])
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    broker.client_frame(1_i64, Bytes[10_u8], Time::Span.zero)
    first_pop = one(sends(broker.take_actions, 0_i64))
    # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
    broker.upstream_frame(Bytes[0x83_u8], 1.millisecond)
    sends(broker.take_actions, 0_i64).should be_empty
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    # This empty result belongs to the older pop. The intervening MSG_WAITING
    # hint still requires a new pop, even though this client can receive empty.
    broker.upstream_frame(Bytes[10_u8], 2.milliseconds)
    actions = broker.take_actions
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    one(sends(actions, 1_i64)).payload.should eq(Bytes[10_u8])
    next_pop = one(sends(actions, 0_i64))
    next_pop.write_id.should_not eq(first_pop.write_id)
  end

  it "retains at most one in-flight orphan and hands it to the next admitted cohort" do
    # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
    broker = MeshCoreTCPMux::Broker.new(5_i64, Bytes.new(32))
    broker.admit(1_i64, Time::Span.zero)
    broker.take_actions # Discard the admission MSG_WAITING hint.
    # SYNC_NEXT_MESSAGE (10): only an explicit request starts a physical pop.
    broker.client_frame(1_i64, Bytes[10_u8], Time::Span.zero)
    one(sends(broker.take_actions, 0_i64)).payload.should eq(Bytes[10_u8])
    # The destructive physical pop is already in flight when its last consumer
    # leaves. Preserve the returned item for the next cohort, not an unbounded log.
    broker.client_closed(1_i64, 1.millisecond)
    broker.take_actions
    item = v3_contact(7_u8)
    broker.upstream_frame(item, 2.milliseconds)
    broker.orphan.should eq(item)

    broker.admit(2_i64, 3.milliseconds)
    actions = broker.take_actions
    # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
    one(sends(actions, 2_i64)).payload.should eq(Bytes[0x83_u8])
    broker.orphan.should be_nil
    broker.sessions[2_i64].inbox.size.should eq(1)
  end

  it "disconnects only an overflowing inbox while a healthy peer continues" do
    config = MeshCoreTCPMux::Config.new
    config.inbox_entries = 1
    # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
    broker = MeshCoreTCPMux::Broker.new(6_i64, Bytes.new(32), config)
    admit_settled(broker, [1_i64, 2_i64])

    # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
    broker.upstream_frame(Bytes[0x83_u8], Time::Span.zero)
    broker.take_actions
    # Client 2 authorizes the first physical pop while both live multi-client
    # sessions qualify for fan-out at completion.
    broker.client_frame(2_i64, Bytes[10_u8], Time::Span.zero)
    one(sends(broker.take_actions, 0_i64)).payload.should eq(Bytes[10_u8])
    broker.upstream_frame(v3_contact(1_u8), Time::Span.zero)
    first_result_actions = broker.take_actions
    second_pop = one(sends(first_result_actions, 0_i64))
    # Client 2 received the first item through its pending sync; session 1's
    # copy remains full while the authorized cycle issues its next pop.
    second_pop.payload.should eq(Bytes[10_u8])
    broker.upstream_frame(v3_contact(2_u8), 2.milliseconds)
    actions = broker.take_actions
    close_actions(actions).map(&.session).should contain(1_i64)
    broker.sessions.has_key?(1_i64).should be_false
    broker.sessions.has_key?(2_i64).should be_true
  end

  it "gives a queued user command a fair turn between physical inbox pops" do
    # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
    broker = MeshCoreTCPMux::Broker.new(8_i64, Bytes.new(32))
    admit_settled(broker, [1_i64])
    # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
    broker.upstream_frame(Bytes[0x83_u8], Time::Span.zero)
    broker.take_actions
    broker.client_frame(1_i64, Bytes[10_u8], Time::Span.zero)
    one(sends(broker.take_actions, 0_i64)).payload.should eq(Bytes[10_u8])
    # GET_DEVICE_TIME (5): local clock query.
    broker.client_frame(1_i64, Bytes[5_u8], Time::Span.zero)
    sends(broker.take_actions).should be_empty
    broker.upstream_frame(v3_contact, 1.millisecond)
    # GET_DEVICE_TIME (5): local clock query.
    one(sends(broker.take_actions, 0_i64)).payload.should eq(Bytes[5_u8])
  end

  it "ends the epoch on unowned ordinary responses and ignores stale write completions" do
    # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
    broker = MeshCoreTCPMux::Broker.new(9_i64, Bytes.new(32))
    admit_settled(broker, [1_i64])
    # GET_DEVICE_TIME (5): local clock query.
    broker.client_frame(1_i64, Bytes[5_u8], Time::Span.zero)
    old_write = one(sends(broker.take_actions, 0_i64))
    # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
    # byte-for-byte.
    broker.upstream_frame(Bytes[9_u8, 0_u8, 0_u8, 0_u8, 0_u8], Time::Span.zero)
    broker.take_actions
    # GET_DEVICE_TIME (5): local clock query.
    broker.client_frame(1_i64, Bytes[5_u8], 1.millisecond)
    current = one(sends(broker.take_actions, 0_i64))
    broker.written(0_i64, 8_i64, old_write.write_id, 2.milliseconds)
    broker.written(0_i64, 9_i64, old_write.write_id, 2.milliseconds)
    # GET_DEVICE_TIME (5): local clock query.
    broker.active.not_nil!.command.should eq(Bytes[5_u8])
    # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
    # byte-for-byte.
    broker.upstream_frame(Bytes[9_u8, 1_u8, 0_u8, 0_u8, 0_u8], 3.milliseconds)
    one(sends(broker.take_actions, 1_i64)).payload[1].should eq(1_u8)
    current.write_id.should_not eq(old_write.write_id)

    # OK (0x00): unexpected generic success with no active command owner.
    broker.upstream_frame(Bytes[0_u8], 4.milliseconds)
    actions = broker.take_actions
    actions.any?(MeshCoreTCPMux::EndEpoch).should be_true
    broker.failed.should be_true
  end

  it "forwards a DM command byte-for-byte exactly once" do
    # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
    broker = MeshCoreTCPMux::Broker.new(10_i64, Bytes.new(32))
    admit_settled(broker, [1_i64])
    # SEND_TXT_MSG (2): type 0/plain, attempt 3, timestamp bytes 3..6 (u32 LE), recipient prefix
    # bytes 7..12, then message bytes.
    dm = Bytes[2_u8, 0_u8, 3_u8, 0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8,
      1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8, 'h'.ord.to_u8]
    broker.client_frame(1_i64, dm, Time::Span.zero)
    upstream = sends(broker.take_actions, 0_i64)
    upstream.size.should eq(1)
    upstream[0].payload.should eq(dm)
    upstream[0].payload.should_not be(dm)
  end
end
