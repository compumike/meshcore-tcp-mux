require "./spec_helper"
require "../src/meshcore_tcp_mux/broker"

private alias BrokerReviewAction = MeshCoreTCPMux::Action

private def review_sends(actions : Array(BrokerReviewAction), session : Int64) : Array(MeshCoreTCPMux::SendFrame)
  actions.compact_map do |action|
    action.as?(MeshCoreTCPMux::SendFrame).try { |send| send if send.session == session }
  end
end

private def review_ready_broker(config = MeshCoreTCPMux::Config.new) : MeshCoreTCPMux::Broker
  # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
  broker = MeshCoreTCPMux::Broker.new(77_i64, Bytes.new(32), config)
  broker.admit(1_i64, Time::Span.zero)
  pop = review_sends(broker.take_actions, 0_i64).first
  # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
  pop.payload.should eq(Bytes[10_u8])
  broker.written(0_i64, pop.epoch, pop.write_id, Time::Span.zero)
  # NO_MORE_MESSAGES (0x0a): inbox empty.
  broker.upstream_frame(Bytes[10_u8], Time::Span.zero)
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
end
