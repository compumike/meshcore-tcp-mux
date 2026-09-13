require "./spec_helper"
require "../src/meshcore_tcp_mux/broker"

private alias MatrixAction = MeshCoreTCPMux::Action

private def matrix_sends(actions : Array(MatrixAction), session : Int64? = nil)
  actions.compact_map do |action|
    action.as?(MeshCoreTCPMux::SendFrame).try { |send| send if session.nil? || send.session == session }
  end
end

private def matrix_ready(config = MeshCoreTCPMux::Config.new) : MeshCoreTCPMux::Broker
  broker = MeshCoreTCPMux::Broker.new(201_i64, Bytes.new(32, 0x77), config)
  broker.admit(1_i64, Time::Span.zero)
  broker.admit(2_i64, Time::Span.zero)
  loop do
    pop = matrix_sends(broker.take_actions, 0_i64).first?
    break unless pop
    broker.written(0_i64, pop.epoch, pop.write_id, Time::Span.zero)
    broker.upstream_frame(Bytes[10_u8], Time::Span.zero)
  end
  broker.take_actions
  broker
end

private def matrix_channel : Bytes
  Bytes[3_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8]
end

# Returns a broker stopped in the requested transaction phase and the instant
# from which that phase's response deadline runs.
private def matrix_phase(phase : Symbol) : {MeshCoreTCPMux::Broker, Time::Span}
  config = MeshCoreTCPMux::Config.new
  config.response_timeout = 1.second
  config.contacts_timeout = 10.seconds
  config.write_timeout = 10.seconds
  config.poll_interval = 20.seconds
  broker = matrix_ready(config)
  now = Time::Span.zero

  case phase
  when :single
    broker.client_frame(1_i64, Bytes[5_u8], now)
  when :contacts_progress
    broker.client_frame(1_i64, Bytes[4_u8], now)
    broker.take_actions
    now = 100.milliseconds
    broker.upstream_frame(Bytes[2_u8, 2_u8, 0_u8, 0_u8, 0_u8], now)
  when :internal_inbox
    broker.client_frame(1_i64, Bytes[10_u8], now)
  when :self_telemetry
    broker.client_frame(1_i64, Bytes[39_u8, 0_u8, 0_u8, 0_u8], now)
  when :scope_setup
    broker.client_frame(1_i64, Bytes[54_u8, 1_u8], now)
    broker.take_actions
    broker.client_frame(1_i64, matrix_channel, now)
  when :scope_send
    broker.client_frame(1_i64, Bytes[54_u8, 1_u8], now)
    broker.take_actions
    broker.client_frame(1_i64, matrix_channel, now)
    broker.take_actions
    now = 100.milliseconds
    broker.upstream_frame(Bytes[0_u8], now)
  when :scope_restore
    broker.client_frame(1_i64, Bytes[54_u8, 1_u8], now)
    broker.take_actions
    broker.client_frame(1_i64, matrix_channel, now)
    broker.take_actions
    broker.upstream_frame(Bytes[0_u8], 100.milliseconds)
    broker.take_actions
    now = 200.milliseconds
    broker.upstream_frame(Bytes[0_u8], now)
  when :signing_data
    broker.client_frame(1_i64, Bytes[33_u8], now)
    broker.take_actions
    broker.upstream_frame(Bytes[0x13_u8, 0_u8, 8_u8, 0_u8, 0_u8, 0_u8], 100.milliseconds)
    broker.take_actions
    now = 200.milliseconds
    broker.client_frame(1_i64, Bytes[34_u8, 1_u8], now)
  else
    raise "unknown matrix phase #{phase}"
  end
  broker.take_actions
  broker.active.should_not be_nil

  # B is intentionally queued behind the uncertain transaction. It must be
  # canceled with the epoch rather than replayed after either kind of fault.
  broker.client_frame(2_i64, Bytes[6_u8, 0_u8, 0_u8, 0_u8, 0_u8], now)
  broker.take_actions
  {broker, now}
end

private def assert_matrix_epoch_ended(broker : MeshCoreTCPMux::Broker)
  actions = broker.take_actions
  actions.compact_map(&.as?(MeshCoreTCPMux::CloseSession)).map(&.session).sort.should eq([1_i64, 2_i64])
  actions.any?(MeshCoreTCPMux::EndEpoch).should be_true
  matrix_sends(actions, 0_i64).should be_empty                              # queued B was not dispatched
  matrix_sends(actions).reject { |send| send.session == 0 }.should be_empty # no fabricated result
  broker.failed.should be_true
  broker.sessions.should be_empty
end

describe "literal transaction fault matrix" do
  phases = [:single, :contacts_progress, :internal_inbox, :self_telemetry,
            :scope_setup, :scope_send, :scope_restore, :signing_data]

  phases.each do |phase|
    it "ends every old session on upstream writer failure during #{phase}" do
      broker, now = matrix_phase(phase)
      broker.write_failed(0_i64, broker.epoch, "injected upstream write failure", now)
      assert_matrix_epoch_ended(broker)
    end

    it "ends every old session on a missing reply during #{phase}" do
      broker, phase_started = matrix_phase(phase)
      broker.tick(phase_started + 1.second)
      assert_matrix_epoch_ended(broker)
    end
  end

  it "isolates inbox byte overflow independently of the entry limit" do
    config = MeshCoreTCPMux::Config.new
    config.inbox_entries = 10
    config.inbox_bytes = 20
    broker = matrix_ready(config)
    item = Bytes.new(13, 0_u8).tap { |p| p[0] = 7_u8 }

    broker.upstream_frame(Bytes[0x83_u8], Time::Span.zero)
    broker.take_actions
    broker.upstream_frame(item, Time::Span.zero)
    broker.take_actions
    broker.client_frame(2_i64, Bytes[10_u8], Time::Span.zero)
    broker.take_actions
    broker.upstream_frame(item, 1.millisecond)
    actions = broker.take_actions

    actions.compact_map(&.as?(MeshCoreTCPMux::CloseSession)).map(&.session).should eq([1_i64])
    broker.sessions.has_key?(1_i64).should be_false
    broker.sessions.has_key?(2_i64).should be_true
  end

  it "counts the three-byte envelope in output bytes while isolating a slow peer" do
    config = MeshCoreTCPMux::Config.new
    config.output_frames = 10
    config.output_bytes = 5
    broker = matrix_ready(config)
    push = Bytes[0x91_u8, 1_u8] # opaque unknown push: two bytes plus three envelope bytes

    broker.upstream_frame(push, Time::Span.zero)
    first = broker.take_actions
    healthy = matrix_sends(first, 2_i64).first
    broker.written(2_i64, healthy.epoch, healthy.write_id, Time::Span.zero)
    broker.take_actions
    broker.upstream_frame(push, 1.millisecond)
    actions = broker.take_actions

    actions.compact_map(&.as?(MeshCoreTCPMux::CloseSession)).map(&.session).should eq([1_i64])
    matrix_sends(actions, 2_i64).map(&.payload).should eq([push])
    broker.sessions.has_key?(2_i64).should be_true
  end
end
