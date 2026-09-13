require "./spec_helper"
require "../src/meshcore_tcp_mux/broker"

private alias MuxAction = MeshCoreTCPMux::Action

private def sends(actions : Array(MuxAction), session : Int64? = nil)
  actions.compact_map do |action|
    if action.is_a?(MeshCoreTCPMux::SendFrame) && (session.nil? || action.session == session)
      action
    end
  end
end

private def close_actions(actions : Array(MuxAction))
  actions.select(MeshCoreTCPMux::CloseSession)
end

private def one(items : Array(T)) : T forall T
  items.size.should eq(1)
  items[0]
end

private def settle_pumps(broker : MeshCoreTCPMux::Broker, now = Time::Span.zero)
  loop do
    actions = broker.take_actions
    pop = sends(actions, 0_i64).find { |send| send.payload == Bytes[10_u8] }
    break unless pop
    broker.written(0_i64, pop.epoch, pop.write_id, now)
    broker.upstream_frame(Bytes[10_u8], now)
  end
end

private def admit_settled(broker : MeshCoreTCPMux::Broker, ids : Array(Int64), now = Time::Span.zero)
  ids.each { |id| broker.admit(id, now) }
  settle_pumps(broker, now)
  broker.take_actions
end

private def device_info : Bytes
  Bytes.new(82, 0_u8).tap do |payload|
    payload[0] = 0x0d
    payload[1] = 13
  end
end

private def v3_contact(seed = 0_u8) : Bytes
  Bytes.new(16, seed).tap do |payload|
    payload[0] = 0x10
    payload[11] = 0 # TXT_TYPE_PLAIN; seed 2 must not accidentally mean signed.
  end
end

describe MeshCoreTCPMux::Broker do
  it "routes identical generic responses to only the active owner and preserves local FIFO" do
    broker = MeshCoreTCPMux::Broker.new(7_i64, Bytes.new(32))
    admit_settled(broker, [1_i64, 2_i64])

    broker.client_frame(1_i64, Bytes[5_u8], Time::Span.zero)
    first = one(sends(broker.take_actions, 0_i64))
    first.payload.should eq(Bytes[5_u8])
    broker.client_frame(1_i64, Bytes[0xfe_u8], Time::Span.zero)
    broker.client_frame(2_i64, Bytes[5_u8], Time::Span.zero)

    broker.upstream_frame(Bytes[9_u8, 1_u8, 2_u8, 3_u8, 4_u8], Time::Span.zero)
    actions = broker.take_actions
    sends(actions, 1_i64).map(&.payload).should eq([
      Bytes[9_u8, 1_u8, 2_u8, 3_u8, 4_u8],
      Bytes[1_u8, MeshCoreTCPMux::Protocol::ERR_UNSUPPORTED_CMD],
    ])
    sends(actions, 2_i64).should be_empty
    one(sends(actions, 0_i64)).payload.should eq(Bytes[5_u8])

    broker.upstream_frame(Bytes[9_u8, 5_u8, 6_u8, 7_u8, 8_u8], Time::Span.zero)
    one(sends(broker.take_actions, 2_i64)).payload.should eq(Bytes[9_u8, 5_u8, 6_u8, 7_u8, 8_u8])
  end

  it "normalizes DEVICE_QUERY upstream but preserves the real reply and requested client version" do
    broker = MeshCoreTCPMux::Broker.new(1_i64, Bytes.new(32))
    admit_settled(broker, [1_i64])
    original = Bytes[22_u8, 2_u8, 0xaa_u8]
    broker.client_frame(1_i64, original, Time::Span.zero)
    one(sends(broker.take_actions, 0_i64)).payload.should eq(Bytes[22_u8, 13_u8, 0xaa_u8])
    broker.upstream_frame(device_info, Time::Span.zero)
    one(sends(broker.take_actions, 1_i64)).payload.should eq(device_info)
    broker.sessions[1_i64].target_version.should eq(2_u8)
    original.should eq(Bytes[22_u8, 2_u8, 0xaa_u8])
  end

  it "owns a contacts stream through its terminator and drains it after owner disconnect" do
    broker = MeshCoreTCPMux::Broker.new(2_i64, Bytes.new(32))
    admit_settled(broker, [1_i64, 2_i64])
    broker.client_frame(1_i64, Bytes[4_u8], Time::Span.zero)
    one(sends(broker.take_actions, 0_i64)).payload.should eq(Bytes[4_u8])
    broker.client_frame(2_i64, Bytes[5_u8], Time::Span.zero)

    broker.upstream_frame(Bytes[2_u8, 9_u8, 0_u8, 0_u8, 0_u8], 1.millisecond)
    actions = broker.take_actions
    one(sends(actions, 1_i64)).payload[0].should eq(2_u8)
    sends(actions, 0_i64).should be_empty
    broker.client_closed(1_i64, 2.milliseconds)
    broker.take_actions

    contact = Bytes.new(148, 0_u8); contact[0] = 3
    broker.upstream_frame(contact, 3.milliseconds)
    sends(broker.take_actions, 0_i64).should be_empty
    broker.upstream_frame(Bytes[4_u8, 1_u8, 0_u8, 0_u8, 0_u8], 4.milliseconds)
    actions = broker.take_actions
    sends(actions, 1_i64).should be_empty
    one(sends(actions, 0_i64)).payload.should eq(Bytes[5_u8])
  end

  it "fans one physical inbox item to admission-time members with per-session version conversion" do
    broker = MeshCoreTCPMux::Broker.new(3_i64, Bytes.new(32))
    admit_settled(broker, [1_i64, 2_i64])
    broker.sessions[2_i64].target_version = 3_u8
    broker.upstream_frame(Bytes[0x83_u8], Time::Span.zero)
    pop = one(sends(broker.take_actions, 0_i64))
    item = v3_contact(0x44_u8)
    broker.upstream_frame(item, Time::Span.zero)
    actions = broker.take_actions
    one(sends(actions, 1_i64)).payload.should eq(Bytes[0x83_u8])
    one(sends(actions, 2_i64)).payload.should eq(Bytes[0x83_u8])

    broker.client_frame(1_i64, Bytes[10_u8], Time::Span.zero)
    legacy = one(sends(broker.take_actions, 1_i64)).payload
    legacy.should eq(Bytes[7_u8] + item[4..])
    broker.client_frame(2_i64, Bytes[10_u8], Time::Span.zero)
    one(sends(broker.take_actions, 2_i64)).payload.should eq(item)
    pop.payload.should eq(Bytes[10_u8])
  end

  it "does not let an old empty observation consume a newer notification generation" do
    broker = MeshCoreTCPMux::Broker.new(4_i64, Bytes.new(32))
    admit_settled(broker, [1_i64])
    broker.client_frame(1_i64, Bytes[10_u8], Time::Span.zero)
    first_pop = one(sends(broker.take_actions, 0_i64))
    broker.upstream_frame(Bytes[0x83_u8], 1.millisecond)
    broker.take_actions.should be_empty
    broker.upstream_frame(Bytes[10_u8], 2.milliseconds)
    actions = broker.take_actions
    one(sends(actions, 1_i64)).payload.should eq(Bytes[10_u8])
    next_pop = one(sends(actions, 0_i64))
    next_pop.write_id.should_not eq(first_pop.write_id)
  end

  it "retains at most one in-flight orphan and hands it to the next admitted cohort" do
    broker = MeshCoreTCPMux::Broker.new(5_i64, Bytes.new(32))
    broker.admit(1_i64, Time::Span.zero)
    one(sends(broker.take_actions, 0_i64)).payload.should eq(Bytes[10_u8])
    broker.client_closed(1_i64, 1.millisecond)
    broker.take_actions
    item = v3_contact(7_u8)
    broker.upstream_frame(item, 2.milliseconds)
    broker.orphan.should eq(item)

    broker.admit(2_i64, 3.milliseconds)
    actions = broker.take_actions
    one(sends(actions, 2_i64)).payload.should eq(Bytes[0x83_u8])
    broker.orphan.should be_nil
    broker.sessions[2_i64].inbox.size.should eq(1)
  end

  it "disconnects only an overflowing inbox while a healthy peer continues" do
    config = MeshCoreTCPMux::Config.new
    config.inbox_entries = 1
    broker = MeshCoreTCPMux::Broker.new(6_i64, Bytes.new(32), config)
    admit_settled(broker, [1_i64, 2_i64])

    broker.upstream_frame(Bytes[0x83_u8], Time::Span.zero)
    broker.take_actions
    broker.upstream_frame(v3_contact(1_u8), Time::Span.zero)
    broker.take_actions
    # Session 2 drains; session 1 remains full.
    broker.client_frame(2_i64, Bytes[10_u8], Time::Span.zero)
    broker.take_actions
    second_pop = sends(broker.take_actions, 0_i64).first?
    unless second_pop
      broker.upstream_frame(Bytes[10_u8], Time::Span.zero) if broker.active
      broker.take_actions
      broker.upstream_frame(Bytes[0x83_u8], 1.millisecond)
      second_pop = one(sends(broker.take_actions, 0_i64))
    end
    broker.upstream_frame(v3_contact(2_u8), 2.milliseconds)
    actions = broker.take_actions
    close_actions(actions).map(&.session).should contain(1_i64)
    broker.sessions.has_key?(1_i64).should be_false
    broker.sessions.has_key?(2_i64).should be_true
  end

  it "gives a queued user command a fair turn between physical inbox pops" do
    broker = MeshCoreTCPMux::Broker.new(8_i64, Bytes.new(32))
    admit_settled(broker, [1_i64])
    broker.upstream_frame(Bytes[0x83_u8], Time::Span.zero)
    broker.take_actions
    broker.client_frame(1_i64, Bytes[5_u8], Time::Span.zero)
    broker.take_actions.should be_empty
    broker.upstream_frame(v3_contact, 1.millisecond)
    one(sends(broker.take_actions, 0_i64)).payload.should eq(Bytes[5_u8])
  end

  it "ends the epoch on unowned ordinary responses and ignores stale write completions" do
    broker = MeshCoreTCPMux::Broker.new(9_i64, Bytes.new(32))
    admit_settled(broker, [1_i64])
    broker.client_frame(1_i64, Bytes[5_u8], Time::Span.zero)
    old_write = one(sends(broker.take_actions, 0_i64))
    broker.upstream_frame(Bytes[9_u8, 0_u8, 0_u8, 0_u8, 0_u8], Time::Span.zero)
    broker.take_actions
    broker.client_frame(1_i64, Bytes[5_u8], 1.millisecond)
    current = one(sends(broker.take_actions, 0_i64))
    broker.written(0_i64, 8_i64, old_write.write_id, 2.milliseconds)
    broker.written(0_i64, 9_i64, old_write.write_id, 2.milliseconds)
    broker.active.not_nil!.command.should eq(Bytes[5_u8])
    broker.upstream_frame(Bytes[9_u8, 1_u8, 0_u8, 0_u8, 0_u8], 3.milliseconds)
    one(sends(broker.take_actions, 1_i64)).payload[1].should eq(1_u8)
    current.write_id.should_not eq(old_write.write_id)

    broker.upstream_frame(Bytes[0_u8], 4.milliseconds)
    actions = broker.take_actions
    actions.any?(MeshCoreTCPMux::EndEpoch).should be_true
    broker.failed.should be_true
  end

  it "forwards a DM command byte-for-byte exactly once" do
    broker = MeshCoreTCPMux::Broker.new(10_i64, Bytes.new(32))
    admit_settled(broker, [1_i64])
    dm = Bytes[2_u8, 0_u8, 3_u8, 0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8,
      1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8, 'h'.ord.to_u8]
    broker.client_frame(1_i64, dm, Time::Span.zero)
    upstream = sends(broker.take_actions, 0_i64)
    upstream.size.should eq(1)
    upstream[0].payload.should eq(dm)
    upstream[0].payload.should_not be(dm)
  end
end
