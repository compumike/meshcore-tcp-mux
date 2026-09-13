require "./spec_helper"
require "../src/meshcore_tcp_mux/broker"

private alias GapAction = MeshCoreTCPMux::Action

private class GapHarness
  getter broker : MeshCoreTCPMux::Broker
  getter upstream = Deque(MeshCoreTCPMux::SendFrame).new
  getter downstream = Hash(Int64, Array(Bytes)).new { |h, id| h[id] = [] of Bytes }
  property now = Time::Span.zero

  def initialize(config = MeshCoreTCPMux::Config.new, ids = [1_i64, 2_i64])
    @broker = MeshCoreTCPMux::Broker.new(91_i64, Bytes.new(32, 0x77), config)
    ids.each { |id| @broker.admit(id, @now); flush }
    while pop = @upstream.shift?
      pop.payload.should eq(Bytes[10_u8])
      @broker.written(0_i64, pop.epoch, pop.write_id, @now)
      @broker.upstream_frame(Bytes[10_u8], @now)
      flush
    end
    @downstream.clear
  end

  def client(id : Int64, payload : Bytes)
    @broker.client_frame(id, payload, @now)
    flush
  end

  def response(payload : Bytes)
    @broker.upstream_frame(payload, @now)
    flush
  end

  def close(id : Int64)
    @broker.client_closed(id, @now)
    flush
  end

  def flush
    loop do
      actions = @broker.take_actions
      break if actions.empty?
      actions.each do |action|
        next unless action.is_a?(MeshCoreTCPMux::SendFrame)
        if action.session == 0
          @upstream << action
        else
          @downstream[action.session] << action.payload
          @broker.written(action.session, action.epoch, action.write_id, @now)
        end
      end
    end
  end
end

private def gap_sent(token = Bytes[1_u8, 2_u8, 3_u8, 4_u8]) : Bytes
  Bytes[6_u8, 1_u8] + token + Bytes[0x40_u8, 0x1f_u8, 0_u8, 0_u8]
end

private def remote_vector(opcode : UInt8) : {Bytes, Bytes}
  peer = Bytes[1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8]
  command = case opcode
            when 26_u8, 27_u8
              Bytes.new(33, 0_u8).tap { |p| p[0] = opcode; p[1, 6].copy_from(peer) }
            when 36_u8
              Bytes[36_u8, 9_u8, 8_u8, 7_u8, 6_u8, 5_u8, 4_u8, 3_u8, 2_u8, 0_u8, 0xaa_u8]
            when 39_u8
              Bytes.new(36, 0_u8).tap { |p| p[0] = opcode; p[4, 6].copy_from(peer) }
            when 50_u8, 57_u8
              Bytes.new(34, 0_u8).tap { |p| p[0] = opcode; p[1, 6].copy_from(peer) }
            when 52_u8
              Bytes.new(34, 0_u8).tap { |p| p[0] = opcode; p[2, 6].copy_from(peer) }
            else
              raise "unhandled remote opcode"
            end
  result = case opcode
           when 26_u8
             Bytes[0x85_u8, 0_u8] + peer
           when 27_u8
             Bytes[0x87_u8, 0_u8] + peer + Bytes[0_u8]
           when 36_u8
             Bytes[0x89_u8, 0_u8, 0_u8, 0_u8, 9_u8, 8_u8, 7_u8, 6_u8,
               5_u8, 4_u8, 3_u8, 2_u8, 0_u8]
           when 39_u8
             Bytes[0x8b_u8, 0_u8] + peer
           when 50_u8, 57_u8
             Bytes[0x8c_u8, 0_u8, 1_u8, 2_u8, 3_u8, 4_u8]
           when 52_u8
             Bytes[0x8d_u8, 0_u8] + peer + Bytes[0_u8, 0_u8]
           else
             raise "unhandled remote opcode"
           end
  {command, result}
end

describe "remaining design acceptance invariants" do
  it "documents the unavoidable same-peer legacy result ambiguity" do
    peer = Bytes[1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8]
    status = Bytes.new(33, 0_u8).tap { |p| p[0] = 27_u8; p[1, 6].copy_from(peer) }
    result = Bytes[0x87_u8, 0_u8] + peer + Bytes[0_u8]
    lease = MeshCoreTCPMux::RemoteLease.new
    lease.reserve(1_i64, status, 0.seconds).should be_true
    lease.accepted(gap_sent, 0.seconds)
    lease.occupied?(11.seconds).should be_false

    lease.reserve(2_i64, status, 11.seconds).should be_true
    lease.accepted(gap_sent, 11.seconds)
    # The native status result exposes only the peer prefix. An old result for
    # the same peer is therefore indistinguishable and inherits this limitation.
    lease.match(result, 12.seconds).should eq(2_i64)
  end

  it "protects and routes every remote lease command while local queries continue" do
    [26_u8, 27_u8, 36_u8, 39_u8, 50_u8, 52_u8, 57_u8].each do |opcode|
      h = GapHarness.new
      command, result = remote_vector(opcode)
      h.client(1_i64, command)
      h.upstream.shift.payload.should eq(command)
      h.response(gap_sent)

      h.client(2_i64, command)
      h.upstream.should be_empty
      h.downstream[2_i64].should eq([Bytes[1_u8, 4_u8]])

      h.client(2_i64, Bytes[5_u8])
      h.upstream.shift.payload.should eq(Bytes[5_u8])
      h.response(result)
      h.downstream[1_i64].last.should eq(result)
      h.downstream[2_i64].should eq([Bytes[1_u8, 4_u8]])
      h.response(Bytes[9_u8, 1_u8, 2_u8, 3_u8, 4_u8])
      h.downstream[2_i64].last.should eq(Bytes[9_u8, 1_u8, 2_u8, 3_u8, 4_u8])
    end
  end

  it "does not advance DM capacity for ERR or zero-token SENT and retains it after disconnect" do
    h = GapHarness.new
    dm = Bytes[2_u8, 0_u8, 2_u8, 0_u8, 0_u8, 0_u8, 0_u8,
      1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8, 7_u8]

    h.client(1_i64, dm)
    h.upstream.shift.payload.should eq(dm)
    h.response(Bytes[1_u8, 3_u8])
    h.client(1_i64, dm)
    h.upstream.shift.payload.should eq(dm)
    h.response(gap_sent(Bytes[0_u8, 0_u8, 0_u8, 0_u8]))

    8.times do |i|
      h.client(1_i64, dm)
      h.upstream.shift.payload.should eq(dm)
      h.response(gap_sent(Bytes[(i + 1).to_u8, 0_u8, 0_u8, 0_u8]))
    end
    h.close(1_i64)
    h.client(2_i64, dm)
    h.upstream.should be_empty
    h.downstream[2_i64].last.should eq(Bytes[1_u8, 4_u8])
  end

  it "virtualizes unscoped/default reset and forwards persistent default configuration" do
    h = GapHarness.new
    channel = Bytes[3_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8]

    h.client(1_i64, Bytes[54_u8, 1_u8])
    h.downstream[1_i64].last.should eq(Bytes[0_u8])
    h.client(1_i64, channel)
    h.upstream.shift.payload.should eq(Bytes[54_u8, 1_u8])
    h.response(Bytes[0_u8])
    h.upstream.shift.payload.should eq(channel)
    h.response(Bytes[0_u8])
    h.upstream.shift.payload.should eq(Bytes[54_u8, 0_u8])
    h.response(Bytes[0_u8])

    h.client(1_i64, Bytes[54_u8, 0_u8])
    h.downstream[1_i64].last.should eq(Bytes[0_u8])
    persistent = Bytes.new(48, 0_u8)
    persistent[0] = 63_u8
    persistent[1] = 'x'.ord.to_u8
    h.client(1_i64, persistent)
    h.upstream.shift.payload.should eq(persistent)
    h.response(Bytes[0_u8])
    h.client(1_i64, channel)
    h.upstream.shift.payload.should eq(channel)
  end

  it "uses pop-completion membership and preserves byte-identical native inbox entries" do
    h = GapHarness.new
    item = Bytes.new(13, 0x44_u8).tap { |p| p[0] = 7_u8; p[8] = 0_u8 }

    h.response(Bytes[0x83_u8])
    h.upstream.shift.payload.should eq(Bytes[10_u8])
    h.broker.admit(3_i64, h.now)
    h.flush
    h.response(item)
    # Fan-out requests the next physical pop. Session 4 joins after the first
    # fan-out and therefore must not acquire historical item 1.
    h.upstream.shift.payload.should eq(Bytes[10_u8])
    h.broker.admit(4_i64, h.now)
    h.flush
    h.response(item)
    h.upstream.shift.payload.should eq(Bytes[10_u8])
    h.response(Bytes[10_u8])

    {1_i64, 2_i64, 3_i64}.each do |id|
      2.times { h.client(id, Bytes[10_u8]) }
      h.downstream[id].select { |p| p[0] == 7 }.should eq([item, item])
    end
    h.client(4_i64, Bytes[10_u8])
    h.downstream[4_i64].select { |p| p[0] == 7 }.should eq([item])
  end

  it "disconnects only a raw-push output consumer whose bounded writes are full" do
    config = MeshCoreTCPMux::Config.new
    config.output_frames = 1
    broker = MeshCoreTCPMux::Broker.new(92_i64, Bytes.new(32), config)
    broker.admit(1_i64, Time::Span.zero)
    broker.admit(2_i64, Time::Span.zero)
    pop = broker.take_actions.compact_map(&.as?(MeshCoreTCPMux::SendFrame)).find { |a| a.session == 0 }.not_nil!
    broker.written(0_i64, pop.epoch, pop.write_id, Time::Span.zero)
    broker.upstream_frame(Bytes[10_u8], Time::Span.zero)
    broker.take_actions

    first = Bytes.new(33, 1_u8).tap { |p| p[0] = 0x80_u8 }
    broker.upstream_frame(first, Time::Span.zero)
    first_actions = broker.take_actions.compact_map(&.as?(MeshCoreTCPMux::SendFrame))
    healthy_write = first_actions.find { |a| a.session == 2 }.not_nil!
    broker.written(2_i64, healthy_write.epoch, healthy_write.write_id, Time::Span.zero)
    broker.take_actions

    second = Bytes.new(33, 2_u8).tap { |p| p[0] = 0x80_u8 }
    broker.upstream_frame(second, Time::Span.zero)
    actions = broker.take_actions
    actions.compact_map(&.as?(MeshCoreTCPMux::CloseSession)).map(&.session).should eq([1_i64])
    actions.compact_map(&.as?(MeshCoreTCPMux::SendFrame)).select { |a| a.session == 2 }.map(&.payload).should eq([second])
    broker.sessions.has_key?(1_i64).should be_false
    broker.sessions.has_key?(2_i64).should be_true
  end

  it "does not let unrelated raw pushes refresh a contacts progress deadline" do
    config = MeshCoreTCPMux::Config.new
    config.response_timeout = 5.seconds
    config.contacts_timeout = 30.seconds
    h = GapHarness.new(config)
    h.client(1_i64, Bytes[4_u8])
    h.upstream.shift.payload.should eq(Bytes[4_u8])
    h.now = 1.second
    h.response(Bytes[2_u8, 9_u8, 0_u8, 0_u8, 0_u8])
    h.now = 5.999.seconds
    h.response(Bytes[0x88_u8, 1_u8, 0_u8])
    h.broker.failed.should be_false
    h.now = 6.seconds
    h.response(Bytes[0x88_u8, 2_u8, 0_u8])
    h.broker.failed.should be_true
  end

  it "shares only qualifying empty probes and never polls without clients" do
    idle = MeshCoreTCPMux::Broker.new(93_i64, Bytes.new(32))
    idle.tick(20.seconds)
    idle.take_actions.compact_map(&.as?(MeshCoreTCPMux::SendFrame)).should be_empty

    h = GapHarness.new(ids: [1_i64, 2_i64, 3_i64])
    h.client(1_i64, Bytes[4_u8])
    h.upstream.shift.payload.should eq(Bytes[4_u8])
    h.response(Bytes[2_u8, 0_u8, 0_u8, 0_u8, 0_u8])
    h.client(2_i64, Bytes[10_u8])
    h.client(3_i64, Bytes[10_u8])
    h.response(Bytes[4_u8, 0_u8, 0_u8, 0_u8, 0_u8])
    h.upstream.shift.payload.should eq(Bytes[10_u8])

    h.broker.admit(4_i64, h.now)
    h.flush
    h.client(4_i64, Bytes[10_u8])
    h.response(Bytes[10_u8])
    h.downstream[2_i64].last.should eq(Bytes[10_u8])
    h.downstream[3_i64].last.should eq(Bytes[10_u8])
    h.downstream[4_i64].should be_empty
    h.upstream.shift.payload.should eq(Bytes[10_u8])
    h.response(Bytes[10_u8])
    h.downstream[4_i64].last.should eq(Bytes[10_u8])

    h.now = 5.seconds
    h.broker.tick(h.now)
    h.flush
    h.upstream.shift.payload.should eq(Bytes[10_u8])
  end

  it "expires a virtual sync locally while a progressing contacts stream continues" do
    h = GapHarness.new
    h.client(1_i64, Bytes[4_u8])
    h.upstream.shift.payload.should eq(Bytes[4_u8])
    h.client(2_i64, Bytes[10_u8])
    h.now = 4.seconds
    h.response(Bytes[2_u8, 1_u8, 0_u8, 0_u8, 0_u8])
    h.now = 5.seconds
    h.broker.tick(h.now)
    h.flush
    h.downstream[2_i64].last.should eq(Bytes[1_u8, 4_u8])
    h.broker.failed.should be_false
    h.broker.active.should_not be_nil
  end

  it "enforces the contacts total deadline despite valid idle progress" do
    config = MeshCoreTCPMux::Config.new
    config.response_timeout = 5.seconds
    config.contacts_timeout = 30.seconds
    h = GapHarness.new(config)
    h.client(1_i64, Bytes[4_u8])
    h.upstream.shift.payload.should eq(Bytes[4_u8])
    h.now = 1.second
    h.response(Bytes[2_u8, 20_u8, 0_u8, 0_u8, 0_u8])
    contact = Bytes.new(148, 0_u8).tap { |p| p[0] = 3_u8 }
    [5, 9, 13, 17, 21, 25, 29].each do |second|
      h.now = second.seconds
      h.response(contact)
      h.broker.failed.should be_false
    end
    h.now = 30.seconds
    h.broker.tick(h.now)
    h.broker.failed.should be_true
  end

  it "rejects an aged queued command in FIFO position and bounds per-client work" do
    config = MeshCoreTCPMux::Config.new
    config.command_age = 3.seconds
    h = GapHarness.new(config)
    h.client(1_i64, Bytes[5_u8])
    h.upstream.shift.payload.should eq(Bytes[5_u8])
    h.client(2_i64, Bytes[6_u8, 0_u8, 0_u8, 0_u8, 0_u8])
    h.now = 3.seconds
    h.response(Bytes[9_u8, 0_u8, 0_u8, 0_u8, 0_u8])
    h.downstream[2_i64].last.should eq(Bytes[1_u8, 4_u8])
    h.upstream.should be_empty

    limited = MeshCoreTCPMux::Config.new
    limited.command_limit = 1
    one = GapHarness.new(limited, [1_i64])
    one.client(1_i64, Bytes[5_u8])
    one.upstream.shift.payload.should eq(Bytes[5_u8])
    one.client(1_i64, Bytes[5_u8])
    one.broker.sessions.has_key?(1_i64).should be_false
  end

  it "applies the default maintenance and private-key export refusal policy locally" do
    h = GapHarness.new(ids: [1_i64])
    commands = [
      Bytes[19_u8] + "reboot".to_slice,
      Bytes[24_u8] + Bytes.new(64, 0_u8),
      Bytes[51_u8] + "reset".to_slice,
    ]
    commands.each do |command|
      h.client(1_i64, command)
      h.downstream[1_i64].last.should eq(Bytes[1_u8, 1_u8])
      h.upstream.should be_empty
    end
    h.client(1_i64, Bytes[23_u8])
    h.downstream[1_i64].last.should eq(Bytes[0x0f_u8])
    h.upstream.should be_empty
  end

  it "gates enabled maintenance on every live shared radio resource" do
    reset = Bytes[51_u8] + "reset".to_slice

    remote_config = MeshCoreTCPMux::Config.new.tap { |c| c.maintenance = true }
    remote = GapHarness.new(remote_config, [1_i64])
    status, _ = remote_vector(27_u8)
    remote.client(1_i64, status)
    remote.upstream.shift.payload.should eq(status)
    remote.response(gap_sent)
    remote.client(1_i64, reset)
    remote.downstream[1_i64].last.should eq(Bytes[1_u8, 4_u8])
    remote.upstream.should be_empty

    dm = GapHarness.new(remote_config, [1_i64])
    dm_command = Bytes[2_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8,
      1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8, 7_u8]
    dm.client(1_i64, dm_command)
    dm.upstream.shift.payload.should eq(dm_command)
    dm.response(gap_sent)
    dm.client(1_i64, reset)
    dm.downstream[1_i64].last.should eq(Bytes[1_u8, 4_u8])
    dm.upstream.should be_empty

    signing = GapHarness.new(remote_config, [1_i64])
    signing.client(1_i64, Bytes[33_u8])
    signing.upstream.shift.payload.should eq(Bytes[33_u8])
    signing.response(Bytes[0x13_u8, 0_u8, 1_u8, 0_u8, 0_u8, 0_u8])
    signing.client(1_i64, reset)
    signing.downstream[1_i64].last.should eq(Bytes[1_u8, 4_u8])
    signing.upstream.should be_empty
  end

  it "preserves a scoped send exactly once when its owner disconnects mid-compound" do
    h = GapHarness.new
    scope = Bytes[54_u8, 1_u8]
    channel = Bytes[3_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8]
    h.client(1_i64, scope)
    h.client(1_i64, channel)
    h.upstream.shift.payload.should eq(scope)
    h.close(1_i64)
    h.response(Bytes[0_u8])
    h.upstream.shift.payload.should eq(channel)
    h.response(Bytes[0_u8])
    h.upstream.shift.payload.should eq(Bytes[54_u8, 0_u8])
    h.response(Bytes[0_u8])
    h.upstream.should be_empty
    h.downstream[2_i64].should be_empty
    h.broker.failed.should be_false
  end

  it "does not lose MSG_WAITING arriving immediately after an empty result" do
    h = GapHarness.new
    h.client(1_i64, Bytes[10_u8])
    h.upstream.shift.payload.should eq(Bytes[10_u8])
    h.response(Bytes[10_u8])
    h.downstream[1_i64].last.should eq(Bytes[10_u8])
    h.response(Bytes[0x83_u8])
    h.upstream.shift.payload.should eq(Bytes[10_u8])
  end

  it "requires exactly one downstream session for enabled maintenance" do
    config = MeshCoreTCPMux::Config.new
    config.maintenance = true
    h = GapHarness.new(config)
    reset = Bytes[51_u8] + "reset".to_slice
    h.client(1_i64, reset)
    h.downstream[1_i64].last.should eq(Bytes[1_u8, 4_u8])
    h.upstream.should be_empty
  end

  it "keeps queued APP_START and contact lookup behind an interleaved contacts stream" do
    h = GapHarness.new
    h.client(1_i64, Bytes[4_u8, 0_u8, 0_u8, 0_u8, 0_u8])
    h.upstream.shift.payload.should eq(Bytes[4_u8, 0_u8, 0_u8, 0_u8, 0_u8])
    h.response(Bytes[2_u8, 7_u8, 0_u8, 0_u8, 0_u8])

    app_start = Bytes.new(8, 0_u8).tap { |p| p[0] = 1_u8 }
    lookup = Bytes.new(33, 0x22_u8).tap { |p| p[0] = 30_u8 }
    h.client(2_i64, app_start)
    h.client(2_i64, lookup)
    h.response(Bytes[0x88_u8, 1_u8, 2_u8])
    advert = Bytes.new(33, 4_u8).tap { |p| p[0] = 0x80_u8 }
    h.response(advert)
    h.upstream.should be_empty

    contact = Bytes.new(148, 0_u8).tap { |p| p[0] = 3_u8 }
    h.response(contact)
    h.upstream.should be_empty
    h.response(Bytes[4_u8, 1_u8, 0_u8, 0_u8, 0_u8])
    h.upstream.shift.payload.should eq(app_start)
    self_info = Bytes.new(58, 0_u8).tap { |p| p[0] = 5_u8 }
    h.response(self_info)
    h.upstream.shift.payload.should eq(lookup)
    h.response(contact)
    h.downstream[2_i64].should eq([
      Bytes[0x88_u8, 1_u8, 2_u8],
      advert,
      self_info,
      contact,
    ])
  end

  it "expires a remote lease silently and discards its late result" do
    h = GapHarness.new
    status, result = remote_vector(27_u8)
    h.client(1_i64, status)
    h.upstream.shift.payload.should eq(status)
    h.response(gap_sent)
    h.downstream[1_i64].should eq([gap_sent])

    h.now = 11.seconds
    h.broker.tick(h.now)
    h.flush
    h.upstream.shift.payload.should eq(Bytes[10_u8])
    h.response(Bytes[10_u8])
    h.downstream[1_i64].should eq([gap_sent])
    h.response(result)
    h.downstream[1_i64].should eq([gap_sent])
    h.broker.failed.should be_false

    h.client(1_i64, Bytes[5_u8])
    h.upstream.shift.payload.should eq(Bytes[5_u8])
  end

  it "preserves signing chunk order and requires restart after inactivity" do
    h = GapHarness.new
    h.client(1_i64, Bytes[33_u8])
    h.upstream.shift.payload.should eq(Bytes[33_u8])
    h.response(Bytes[0x13_u8, 0_u8, 8_u8, 0_u8, 0_u8, 0_u8])

    first = Bytes[34_u8, 1_u8, 2_u8]
    second = Bytes[34_u8, 3_u8, 4_u8, 5_u8]
    h.client(1_i64, first)
    h.upstream.shift.payload.should eq(first)
    h.response(Bytes[0_u8])
    h.now = 1.second
    h.client(1_i64, second)
    h.upstream.shift.payload.should eq(second)
    h.response(Bytes[0_u8])

    h.now = 31.seconds
    h.broker.tick(h.now)
    h.flush
    h.upstream.shift.payload.should eq(Bytes[10_u8])
    h.response(Bytes[10_u8])
    h.client(1_i64, Bytes[35_u8])
    h.downstream[1_i64].last.should eq(Bytes[1_u8, 4_u8])
    h.upstream.should be_empty
    h.client(1_i64, Bytes[33_u8])
    h.upstream.shift.payload.should eq(Bytes[33_u8])
  end
end
