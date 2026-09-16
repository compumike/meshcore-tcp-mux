require "./spec_helper"
require "../src/meshcore_tcp_mux/broker"

private alias MatrixAction = MeshCoreTCPMux::Action

private def matrix_sends(actions : Array(MatrixAction), session : Int64? = nil) : Array(MeshCoreTCPMux::SendFrame)
  actions.compact_map do |action|
    action.as?(MeshCoreTCPMux::SendFrame).try { |send| send if session.nil? || send.session == session }
  end
end

private def matrix_ready(config = MeshCoreTCPMux::Config.new) : MeshCoreTCPMux::Broker
  # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
  broker = MeshCoreTCPMux::Broker.new(201_i64, Bytes.new(32, 0x77), config)
  broker.admit(1_i64, Time::Span.zero)
  broker.admit(2_i64, Time::Span.zero)
  matrix_sends(broker.take_actions).each do |hint|
    # MSG_WAITING admission hints are downstream-only and complete immediately
    # in this broker model harness.
    broker.written(hint.session, hint.epoch, hint.write_id, Time::Span.zero)
  end
  broker.take_actions
  broker
end

private def matrix_channel : Bytes
  # SEND_CHANNEL_TXT_MSG (3): text type 0, channel index 0, timestamp bytes 3..6 (u32 LE), then
  # text (empty if absent).
  Bytes[3_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8]
end

private def matrix_phase(phase : Symbol) : {MeshCoreTCPMux::Broker, Time::Span}
  # Returns a broker stopped in the requested transaction phase and the instant
  # from which that phase's response deadline runs.
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
    # OK (0x00): hidden scope setup completed.
    broker.upstream_frame(Bytes[0_u8], now)
  when :scope_restore
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 1: explicitly unscoped sends.
    broker.client_frame(1_i64, Bytes[54_u8, 1_u8], now)
    broker.take_actions
    broker.client_frame(1_i64, matrix_channel, now)
    broker.take_actions
    # OK (0x00): hidden scope setup completed.
    broker.upstream_frame(Bytes[0_u8], 100.milliseconds)
    broker.take_actions
    now = 200.milliseconds
    # OK (0x00): channel send accepted; radio delivery is not confirmed.
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
  if write_id = broker.active.not_nil!.upstream_write_id
    # Missing-reply cases begin only after the current upstream command has
    # actually left the socket writer. Before Written, the independent write
    # timeout owns the operation and no response deadline is running.
    broker.written(0_i64, broker.epoch, write_id, now)
    broker.take_actions
  end
  {broker, now}
end

private def assert_matrix_epoch_ended(broker : MeshCoreTCPMux::Broker) : Nil
  actions = broker.take_actions
  actions.compact_map(&.as?(MeshCoreTCPMux::CloseSession)).map(&.session).sort.should eq([1_i64, 2_i64])
  actions.any?(MeshCoreTCPMux::EndEpoch).should be_true
  matrix_sends(actions, 0_i64).should be_empty                              # queued B was not dispatched
  matrix_sends(actions).reject { |send| send.session == 0 }.should be_empty # no fabricated result
  broker.failed.should be_true
  broker.sessions.should be_empty
end

describe "literal transaction fault matrix" do
  # Each phase fixture leaves one transaction awaiting a real companion reply.
  # A second client's command is queued behind it. A timeout or failed upstream
  # write makes attribution uncertain: close the whole epoch rather than dispatch
  # or replay queued work. Byte fixtures are decoded payloads, with LE integers.

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
end
