# Each phase fixture leaves one transaction awaiting a real companion reply.
# A second client's command is queued behind it. A timeout or failed upstream
# write makes attribution uncertain: close the whole epoch rather than dispatch
# or replay queued work. Byte fixtures are decoded payloads, with LE integers.

require "./spec_helper"
require "../src/meshcore_tcp_mux/broker"

private alias MatrixAction = MeshCoreTCPMux::Action

private def matrix_sends(actions : Array(MatrixAction), session : Int64? = nil)
  actions.compact_map do |action|
    action.as?(MeshCoreTCPMux::SendFrame).try { |send| send if session.nil? || send.session == session }
  end
end

private def matrix_ready(config = MeshCoreTCPMux::Config.new) : MeshCoreTCPMux::Broker
  # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
  broker = MeshCoreTCPMux::Broker.new(201_i64, Bytes.new(32, 0x77), config)
  broker.admit(1_i64, Time::Span.zero)
  broker.admit(2_i64, Time::Span.zero)
  loop do
    pop = matrix_sends(broker.take_actions, 0_i64).first?
    break unless pop
    broker.written(0_i64, pop.epoch, pop.write_id, Time::Span.zero)
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    broker.upstream_frame(Bytes[10_u8], Time::Span.zero)
  end
  broker.take_actions
  broker
end

private def matrix_channel : Bytes
  # SEND_CHANNEL_TXT_MSG (3): text type 0, channel index 0, timestamp bytes 3..6 (u32 LE), then
  # text (empty if absent).
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
    # GET_DEVICE_TIME (5): local clock query.
    broker.client_frame(1_i64, Bytes[5_u8], now)
  when :contacts_progress
    # GET_CONTACTS (4); optional trailing u32 LE is the 'since' timestamp.
    broker.client_frame(1_i64, Bytes[4_u8], now)
    broker.take_actions
    now = 100.milliseconds
    # CONTACTS_START (0x02) with contact count 2 (u32 little-endian).
    broker.upstream_frame(Bytes[2_u8, 2_u8, 0_u8, 0_u8, 0_u8], now)
  when :internal_inbox
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    broker.client_frame(1_i64, Bytes[10_u8], now)
  when :self_telemetry
    # Self TELEMETRY request (39): four-byte form with three zero option/reserved bytes, no
    # remote peer.
    broker.client_frame(1_i64, Bytes[39_u8, 0_u8, 0_u8, 0_u8], now)
  when :scope_setup
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 1: explicitly unscoped sends.
    broker.client_frame(1_i64, Bytes[54_u8, 1_u8], now)
    broker.take_actions
    broker.client_frame(1_i64, matrix_channel, now)
  when :scope_send
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 1: explicitly unscoped sends.
    broker.client_frame(1_i64, Bytes[54_u8, 1_u8], now)
    broker.take_actions
    broker.client_frame(1_i64, matrix_channel, now)
    broker.take_actions
    now = 100.milliseconds
    # OK (0x00): command accepted, not proof of radio delivery.
    broker.upstream_frame(Bytes[0_u8], now)
  when :scope_restore
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 1: explicitly unscoped sends.
    broker.client_frame(1_i64, Bytes[54_u8, 1_u8], now)
    broker.take_actions
    broker.client_frame(1_i64, matrix_channel, now)
    broker.take_actions
    # OK (0x00): command accepted, not proof of radio delivery.
    broker.upstream_frame(Bytes[0_u8], 100.milliseconds)
    broker.take_actions
    now = 200.milliseconds
    # OK (0x00): command accepted, not proof of radio delivery.
    broker.upstream_frame(Bytes[0_u8], now)
  when :signing_data
    # SIGN_START (33/0x21): start the shared signing session.
    broker.client_frame(1_i64, Bytes[33_u8], now)
    broker.take_actions
    # SIGN_START response (0x13): reserved 0, byte limit 8 (u32 LE).
    broker.upstream_frame(Bytes[0x13_u8, 0_u8, 8_u8, 0_u8, 0_u8, 0_u8], 100.milliseconds)
    broker.take_actions
    now = 200.milliseconds
    # SIGN_DATA (34/0x22), followed by 1 literal data byte(s); only accepted data counts against
    # the signing budget.
    broker.client_frame(1_i64, Bytes[34_u8, 1_u8], now)
  else
    raise "unknown matrix phase #{phase}"
  end
  broker.take_actions
  broker.active.should_not be_nil

  # B is intentionally queued behind the uncertain transaction. It must be
  # canceled with the epoch rather than replayed after either kind of fault.
  # SET_DEVICE_TIME (6), timestamp 0 (u32 little-endian); a queued state-changing command.
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
    # Minimal legacy CONTACT_MESSAGE: opcode 7, six-byte sender prefix, path/type bytes, u32
    # timestamp; no text body.
    item = Bytes.new(13, 0_u8).tap { |p| p[0] = 7_u8 }

    # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
    broker.upstream_frame(Bytes[0x83_u8], Time::Span.zero)
    broker.take_actions
    broker.upstream_frame(item, Time::Span.zero)
    broker.take_actions
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
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
    # Unknown push 0x91 with one opaque body byte; total framed size is five bytes.
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
