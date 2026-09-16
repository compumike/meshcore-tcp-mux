require "./spec_helper"
require "../src/meshcore_tcp_mux/broker"

private alias BrokerReviewAction = MeshCoreTCPMux::Action
private alias BrokerReviewProtocol = MeshCoreTCPMux::Protocol

private def review_sends(actions : Array(BrokerReviewAction), session : Int64) : Array(MeshCoreTCPMux::SendFrame)
  actions.compact_map do |action|
    action.as?(MeshCoreTCPMux::SendFrame).try { |send| send if send.session == session }
  end
end

private def review_ready_broker(config = MeshCoreTCPMux::Config.new) : MeshCoreTCPMux::Broker
  # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
  broker = MeshCoreTCPMux::Broker.new(77_i64, Bytes.new(32), config)
  broker.admit(1_i64, Time::Span.zero)
  # Admission emits only a downstream MSG_WAITING hint; acknowledge it so the
  # regression under test starts with no outstanding output budget.
  hint = review_sends(broker.take_actions, 1_i64).first
  broker.written(1_i64, hint.epoch, hint.write_id, Time::Span.zero)
  broker.take_actions
  broker
end

describe "broker event-order regressions" do
  # Event-order regressions: writer completion and reader response notifications
  # can arrive in either order. These tests drive time manually and acknowledge
  # only selected writes so races are reproducible. Bytes are decoded payloads.

  it "does not expire an upstream write after its actual response arrived first" do
    broker = review_ready_broker
    # GET_DEVICE_TIME (5): local clock query.
    broker.client_frame(1_i64, Bytes[5_u8], Time::Span.zero)
    command_write = review_sends(broker.take_actions, 0_i64).first

    # The reader event can reach the broker before the writer completion event.
    # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
    # byte-for-byte.
    broker.upstream_frame(Bytes[9_u8, 1_u8, 2_u8, 3_u8, 4_u8], 1.millisecond)
    review_sends(broker.take_actions, 1_i64).size.should eq(1)
    broker.tick(5.seconds)
    broker.failed.should be_false

    # Its eventually delivered completion remains a harmless stale event.
    broker.written(0_i64, command_write.epoch, command_write.write_id, 6.seconds)
    broker.failed.should be_false
  end

  it "starts the response deadline when the matching upstream write completes" do
    config = MeshCoreTCPMux::Config.new
    config.response_timeout = 5.seconds
    config.write_timeout = 30.seconds
    broker = review_ready_broker(config)
    broker.client_frame(1_i64, Bytes[5_u8], Time::Span.zero) # GET_DEVICE_TIME.
    command_write = review_sends(broker.take_actions, 0_i64).first

    # The socket write remains pending after the old dispatch-based response
    # deadline but within its 30-second write budget. It must not be judged by
    # the response clock before the writer completes.
    broker.tick(6.seconds)
    broker.failed.should be_false
    broker.written(0_i64, command_write.epoch, command_write.write_id, 7.seconds)
    broker.tick(10.seconds)
    broker.failed.should be_false
    broker.take_actions # Discard periodic MSG_WAITING reminders; they are unrelated to command ownership.

    # CURRENT_TIME (0x09): four-byte little-endian synthetic timestamp. This is
    # later than five seconds after dispatch but within five seconds of write
    # completion, so it remains the rightful response.
    current_time = Bytes[9_u8, 1_u8, 2_u8, 3_u8, 4_u8]
    broker.upstream_frame(current_time, 11.seconds)
    review_sends(broker.take_actions, 1_i64).map(&.payload).should eq([current_time])
    broker.failed.should be_false
  end

  it "starts an internal inbox response deadline after its delayed write" do
    config = MeshCoreTCPMux::Config.new
    config.response_timeout = 5.seconds
    config.write_timeout = 30.seconds
    broker = review_ready_broker(config)

    # A downstream SYNC_NEXT_MESSAGE against an empty local inbox authorizes
    # the mux's hidden physical pop. The physical command has owner zero.
    broker.client_frame(1_i64, Bytes[BrokerReviewProtocol::CMD_SYNC_NEXT_MESSAGE], Time::Span.zero)
    internal_write = review_sends(broker.take_actions, 0_i64).first
    internal_write.payload.should eq(Bytes[BrokerReviewProtocol::CMD_SYNC_NEXT_MESSAGE])

    broker.tick(6.seconds)
    broker.failed.should be_false
    broker.written(0_i64, internal_write.epoch, internal_write.write_id, 7.seconds)
    broker.tick(10.seconds)
    broker.failed.should be_false

    # NO_MORE_MESSAGES terminates the hidden inbox pop within five seconds of
    # the actual write even though dispatch occurred eleven seconds earlier.
    broker.upstream_frame(Bytes[BrokerReviewProtocol::RESP_NO_MORE_MESSAGES], 11.seconds)
    broker.failed.should be_false
  end

  it "starts the contacts-wide deadline when its upstream write completes" do
    config = MeshCoreTCPMux::Config.new
    config.response_timeout = 5.seconds
    config.contacts_timeout = 30.seconds
    config.write_timeout = 40.seconds
    broker = review_ready_broker(config)
    broker.client_frame(1_i64, Bytes[4_u8], Time::Span.zero) # GET_CONTACTS.
    command_write = review_sends(broker.take_actions, 0_i64).first

    # A queued write may outlive the old dispatch-based 30-second contacts
    # budget while remaining inside its configured write budget.
    broker.tick(31.seconds)
    broker.failed.should be_false
    broker.written(0_i64, command_write.epoch, command_write.write_id, 32.seconds)
    broker.take_actions

    # CONTACTS_START: opcode followed by a synthetic little-endian u32 count.
    broker.upstream_frame(Bytes[2_u8, 0_u8, 0_u8, 0_u8, 0_u8], 36.seconds)
    broker.take_actions
    # END_OF_CONTACTS: opcode followed by the final synthetic u32 count.
    broker.upstream_frame(Bytes[4_u8, 0_u8, 0_u8, 0_u8, 0_u8], 37.seconds)
    replies = review_sends(broker.take_actions, 1_i64).map(&.payload)
    replies.should eq([Bytes[4_u8, 0_u8, 0_u8, 0_u8, 0_u8]])
    broker.failed.should be_false
  end

  it "starts the contacts-wide deadline when a response wins the writer-event race" do
    config = MeshCoreTCPMux::Config.new
    config.response_timeout = 40.seconds
    config.contacts_timeout = 30.seconds
    config.write_timeout = 40.seconds
    broker = review_ready_broker(config)
    broker.client_frame(1_i64, Bytes[BrokerReviewProtocol::CMD_GET_CONTACTS], Time::Span.zero)
    review_sends(broker.take_actions, 0_i64).size.should eq(1)

    # CONTACTS_START can reach the broker before the writer fiber reports its
    # completion. Receiving it proves the queued command was physically sent,
    # so this event must begin both response and contacts clocks.
    start = Bytes[BrokerReviewProtocol::RESP_CONTACTS_START, 0_u8, 0_u8, 0_u8, 0_u8]
    broker.upstream_frame(start, 32.seconds)
    broker.take_actions
    broker.tick(61.seconds)
    broker.failed.should be_false

    terminator = Bytes[BrokerReviewProtocol::RESP_END_OF_CONTACTS, 0_u8, 0_u8, 0_u8, 0_u8]
    broker.upstream_frame(terminator, 61.seconds)
    replies = review_sends(broker.take_actions, 1_i64).map(&.payload)
    replies.select { |payload| payload[0] == BrokerReviewProtocol::RESP_END_OF_CONTACTS }.should eq([terminator])
    broker.failed.should be_false
  end

  it "reports response timeout state needed to diagnose an uncertain command" do
    config = MeshCoreTCPMux::Config.new
    config.response_timeout = 5.seconds
    broker = review_ready_broker(config)
    broker.client_frame(1_i64, Bytes[5_u8], Time::Span.zero) # GET_DEVICE_TIME.
    command_write = review_sends(broker.take_actions, 0_i64).first
    broker.written(0_i64, command_write.epoch, command_write.write_id, Time::Span.zero)
    broker.take_actions

    broker.tick(5.seconds)
    reason = broker.take_actions.compact_map(&.as?(MeshCoreTCPMux::EndEpoch)).first.reason
    reason.should contain("command=get_device_time opcode=5 owner=1 step=command")
    reason.should contain("progress_elapsed_ms=5000.0 response_timeout_ms=5000.0")
    reason.should contain("response_frames=0 contacts_started=false write_pending=false")
  end

  it "gives every hidden scope substep its own response deadline" do
    config = MeshCoreTCPMux::Config.new
    config.response_timeout = 5.seconds
    config.write_timeout = 30.seconds
    broker = review_ready_broker(config)

    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 1: explicitly unscoped sends.
    broker.client_frame(1_i64, Bytes[54_u8, 1_u8], Time::Span.zero)
    broker.take_actions
    # SEND_CHANNEL_TXT_MSG (3): text type 0, channel index 0, timestamp bytes 3..6 (u32 LE),
    # then text (empty if absent).
    channel_send = Bytes[3_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8]
    broker.client_frame(1_i64, channel_send, Time::Span.zero)
    setup = review_sends(broker.take_actions, 0_i64).first
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 1: explicitly unscoped sends.
    setup.payload.should eq(Bytes[54_u8, 1_u8])
    broker.written(0_i64, setup.epoch, setup.write_id, Time::Span.zero)

    # OK (0x00): hidden scope setup completed.
    broker.upstream_frame(Bytes[0_u8], 4.seconds)
    command = review_sends(broker.take_actions, 0_i64).first
    command.payload.should eq(channel_send)
    broker.written(0_i64, command.epoch, command.write_id, 4.seconds)

    # OK (0x00): channel send accepted; radio delivery is not confirmed.
    broker.upstream_frame(Bytes[0_u8], 8.seconds)
    actions = broker.take_actions
    # OK (0x00): only the channel acceptance is visible downstream.
    review_sends(actions, 1_i64).map(&.payload).should eq([Bytes[0_u8]])
    restore = review_sends(actions, 0_i64).first
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 0 with no key: restore the configured default scope.
    restore.payload.should eq(Bytes[54_u8, 0_u8])
    broker.failed.should be_false
    broker.written(0_i64, restore.epoch, restore.write_id, 8.seconds)

    # OK (0x00): hidden default-scope restoration completed.
    broker.upstream_frame(Bytes[0_u8], 12.seconds)
    broker.failed.should be_false
    broker.active.should be_nil
  end

  it "waits for a live owner to write a maintenance result before ending the epoch" do
    config = MeshCoreTCPMux::Config.new
    config.maintenance = true
    broker = review_ready_broker(config)
    # FACTORY_RESET (51) plus required ASCII "reset" magic string.
    reset = Bytes[51_u8, 'r'.ord.to_u8, 'e'.ord.to_u8, 's'.ord.to_u8, 'e'.ord.to_u8, 't'.ord.to_u8]
    broker.client_frame(1_i64, reset, Time::Span.zero)
    upstream = review_sends(broker.take_actions, 0_i64).first
    broker.written(0_i64, upstream.epoch, upstream.write_id, Time::Span.zero)

    # OK (0x00): factory reset accepted; await downstream write completion.
    broker.upstream_frame(Bytes[0_u8], 1.millisecond)
    actions = broker.take_actions
    result = review_sends(actions, 1_i64).first
    actions.any?(MeshCoreTCPMux::CloseSession).should be_false
    actions.any?(MeshCoreTCPMux::EndEpoch).should be_false

    broker.written(1_i64, result.epoch, result.write_id, 2.milliseconds)
    completion = broker.take_actions
    completion.any?(MeshCoreTCPMux::CloseSession).should be_true
    completion.any?(MeshCoreTCPMux::EndEpoch).should be_true
  end

  it "ends maintenance promptly if its owner closes while the result is pending" do
    config = MeshCoreTCPMux::Config.new
    config.maintenance = true
    broker = review_ready_broker(config)
    # FACTORY_RESET (51) plus required ASCII "reset" magic string.
    reset = Bytes[51_u8, 'r'.ord.to_u8, 'e'.ord.to_u8, 's'.ord.to_u8, 'e'.ord.to_u8, 't'.ord.to_u8]
    broker.client_frame(1_i64, reset, Time::Span.zero)
    upstream = review_sends(broker.take_actions, 0_i64).first
    broker.written(0_i64, upstream.epoch, upstream.write_id, Time::Span.zero)
    # OK (0x00): factory reset accepted; result is still queued downstream.
    broker.upstream_frame(Bytes[0_u8], 1.millisecond)
    broker.take_actions # The result has been handed to the downstream writer.

    broker.client_closed(1_i64, 2.milliseconds)
    actions = broker.take_actions
    actions.any?(MeshCoreTCPMux::CloseSession).should be_true
    actions.any?(MeshCoreTCPMux::EndEpoch).should be_true
    broker.active.should be_nil
  end

  it "applies private-key import and factory-reset permissions independently" do
    import = Bytes[24_u8] + Bytes.new(64, 0_u8) # IMPORT_PRIVATE_KEY plus a synthetic 64-byte key.
    reset = Bytes[51_u8] + "reset".to_slice     # FACTORY_RESET plus required ASCII magic.

    import_disabled = MeshCoreTCPMux::Config.new
    import_disabled.private_key_import = false
    import_disabled.factory_reset = true
    first = review_ready_broker(import_disabled)
    first.client_frame(1_i64, import, Time::Span.zero)
    review_sends(first.take_actions, 1_i64).map(&.payload).should eq([Bytes[1_u8, 1_u8]]) # ERR(UNSUPPORTED_CMD).
    first.client_frame(1_i64, reset, Time::Span.zero)
    review_sends(first.take_actions, 0_i64).map(&.payload).should eq([reset])

    reset_disabled = MeshCoreTCPMux::Config.new
    reset_disabled.private_key_import = true
    reset_disabled.factory_reset = false
    second = review_ready_broker(reset_disabled)
    second.client_frame(1_i64, reset, Time::Span.zero)
    review_sends(second.take_actions, 1_i64).map(&.payload).should eq([Bytes[1_u8, 1_u8]]) # ERR(UNSUPPORTED_CMD).
    second.client_frame(1_i64, import, Time::Span.zero)
    review_sends(second.take_actions, 0_i64).map(&.payload).should eq([import])
  end
end
