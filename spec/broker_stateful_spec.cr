require "./spec_helper"
require "../src/meshcore_tcp_mux/broker"

private class StatefulHarness
  getter broker : MeshCoreTCPMux::Broker
  getter upstream = Deque(Bytes).new
  getter replies = Hash(Int64, Array(Bytes)).new { |h, k| h[k] = [] of Bytes }
  property now = Time::Span.zero

  def initialize(config = MeshCoreTCPMux::Config.new)
    @broker = MeshCoreTCPMux::Broker.new(1_i64, Bytes.new(32, 0x77), config)
    @broker.admit(1_i64, @now)
    @broker.admit(2_i64, @now)
    flush
    until @upstream.empty?
      @upstream.shift.should eq(Bytes[10])
      response(Bytes[10])
    end
  end

  def client(id, payload)
    @broker.client_frame(id.to_i64, payload, @now)
    flush
  end

  def response(payload)
    @broker.upstream_frame(payload, @now)
    flush
  end

  def flush
    loop do
      actions = @broker.take_actions
      break if actions.empty?
      actions.each do |action|
        if action.is_a?(MeshCoreTCPMux::SendFrame)
          if action.session == 0
            @upstream << action.payload
          else
            @replies[action.session] << action.payload
          end
          @broker.written(action.session, action.epoch, action.write_id, @now)
        end
      end
    end
  end
end

private def stateful_sent(token : UInt8 = 1)
  Bytes[6, 1, token, 0xbb, 0xcc, 0xdd, 0x40, 0x1f, 0, 0]
end

private def stateful_dm
  Bytes[2, 0, 2, 0x78, 0x56, 0x34, 0x12, 1, 2, 3, 4, 5, 6, 0x68, 0x69]
end

describe "broker stateful operations" do
  it "logs transaction metadata without logging private response bytes" do
    config = MeshCoreTCPMux::Config.new
    config.private_key_export = true
    h = StatefulHarness.new(config)
    h.client(1, Bytes[23])
    h.upstream.shift.should eq(Bytes[23])
    secret = Bytes.new(65, 'S'.ord.to_u8)
    secret[0] = 0x0e
    h.broker.upstream_frame(secret, 1.millisecond)
    logs = h.broker.take_actions.compact_map(&.as?(MeshCoreTCPMux::Diagnostic)).map(&.message).join('\n')
    logs.should contain("command=export_private_key")
    logs.should contain("response=private_key response_bytes=65")
    logs.should contain("elapsed_ms=1.0")
    logs.should_not contain("S" * 64)
  end

  it "preserves signed, CLI, unknown text, and binary inbox bodies for both client versions" do
    legacy_signed = Bytes.new(21, 0xff)
    legacy_signed[0] = 7
    legacy_signed[8] = 2
    legacy_cli = Bytes.new(18, 0x3c)
    legacy_cli[0] = 7
    legacy_cli[8] = 1
    unknown_text = legacy_cli.dup
    unknown_text[8] = 0x7f
    v3_signed = Bytes[0x10, 0xf0, 0, 0] + legacy_signed[1..]
    legacy_channel = Bytes[8, 2, 0xff, 0, 0x78, 0x56, 0x34, 0x12, 0xff, 0, 0x3e]
    v3_channel = Bytes[0x11, 0xf0, 0, 0] + legacy_channel[1..]
    datagram = Bytes[0x1b, 0xf0, 0, 0, 2, 0xff, 0x34, 0x12, 4, 0, 0xff, 0x3c, 0x3e]

    [legacy_signed, legacy_cli, unknown_text, v3_signed, legacy_channel, v3_channel, datagram].each do |item|
      h = StatefulHarness.new
      h.broker.sessions[2_i64].target_version = 13_u8
      h.response(Bytes[0x83])
      h.upstream.shift.should eq(Bytes[10])
      h.response(item)
      h.upstream.shift.should eq(Bytes[10])
      h.response(Bytes[10])
      h.replies.clear
      h.client(1, Bytes[10])
      h.client(2, Bytes[10])
      expected = case item[0]
                 when 0x10 then Bytes[7] + item[4..]
                 when 0x11 then Bytes[8] + item[4..]
                 else           item
                 end
      h.replies[1_i64].should eq([expected])
      h.replies[2_i64].should eq([item])
      h.upstream.should be_empty
    end
  end

  it "preserves send bytes and native acceptance and does not create a retry or echo" do
    h = StatefulHarness.new
    2.times do |attempt|
      h.now = attempt.seconds
      h.client(1, stateful_dm)
      h.upstream.shift.should eq(stateful_dm)
      h.response(stateful_sent)
    end
    h.replies[1_i64].should eq([stateful_sent, stateful_sent])
    h.replies[2_i64].should be_empty
    h.upstream.should be_empty
    channel = Bytes[3, 0, 4, 0x78, 0x56, 0x34, 0x12, 0x68, 0x69]
    h.client(2, channel)
    h.upstream.shift.should eq(channel)
    h.response(Bytes[0])
    h.replies[2_i64].should eq([Bytes[0]])
    h.upstream.should be_empty
  end

  it "wraps only the scoped owner's sends with hidden setup and restoration" do
    h = StatefulHarness.new
    scope = Bytes.new(18, 0x42)
    scope[0] = 54
    scope[1] = 0
    h.client(1, scope)
    h.replies[1_i64].shift.should eq(Bytes[0])
    h.upstream.should be_empty
    h.client(2, stateful_dm)
    h.upstream.shift.should eq(stateful_dm)
    h.response(stateful_sent)
    h.client(1, stateful_dm)
    h.upstream.shift.should eq(scope)
    h.client(2, Bytes[5])
    h.response(Bytes[0])
    h.upstream.shift.should eq(stateful_dm)
    h.replies[1_i64].should be_empty
    h.response(stateful_sent(2))
    h.replies[1_i64].should eq([stateful_sent(2)])
    h.upstream.shift.should eq(Bytes[54, 0])
    h.response(Bytes[0])
    h.upstream.shift.should eq(Bytes[5])
    h.replies[1_i64].should eq([stateful_sent(2)])
  end

  it "does not send after scope setup rejection or replay after restoration failure" do
    h = StatefulHarness.new
    h.client(1, Bytes[54, 1])
    h.replies.clear
    h.client(1, stateful_dm)
    h.upstream.shift.should eq(Bytes[54, 1])
    h.response(Bytes[1, 1])
    h.upstream.should be_empty
    h.replies[1_i64].should eq([Bytes[1, 1]])
    h.client(1, stateful_dm)
    h.upstream.shift.should eq(Bytes[54, 1])
    h.response(Bytes[0])
    h.upstream.shift.should eq(stateful_dm)
    h.response(stateful_sent)
    h.upstream.shift.should eq(Bytes[54, 0])
    h.response(Bytes[1, 4])
    h.broker.failed.should be_true
    h.replies[1_i64].last.should eq(stateful_sent)
    h.upstream.should be_empty
  end

  it "retains remote ownership after SENT without blocking local queries or accepting tentative pushes" do
    h = StatefulHarness.new
    status = Bytes.new(33, 0x11)
    status[0] = 27
    result = Bytes.new(9, 0x11)
    result[0] = 0x87
    h.client(1, status)
    h.upstream.shift.should eq(status)
    h.response(result)
    h.replies[1_i64].should be_empty
    h.response(stateful_sent)
    h.client(2, status)
    h.upstream.should be_empty
    h.replies[2_i64].shift.should eq(Bytes[1, 4])
    h.client(2, Bytes[5])
    h.upstream.shift.should eq(Bytes[5])
    h.response(result)
    h.replies[1_i64].last.should eq(result)
    h.replies[2_i64].should be_empty
    h.response(Bytes[9, 1, 2, 3, 4])
    h.replies[2_i64].should eq([Bytes[9, 1, 2, 3, 4]])
  end

  it "separates self telemetry from a remote telemetry lease" do
    h = StatefulHarness.new
    remote = Bytes.new(36, 0x11)
    remote[0] = 39
    h.client(1, remote)
    h.upstream.shift.should eq(remote)
    h.response(stateful_sent)
    h.client(2, Bytes[39, 0, 0, 0])
    h.upstream.shift.should eq(Bytes[39, 0, 0, 0])
    result = Bytes.new(8, 0x11)
    result[0] = 0x8b
    h.response(result)
    h.replies[1_i64].last.should eq(result)
    h.replies[2_i64].should be_empty
    self_result = Bytes.new(8, 0x77)
    self_result[0] = 0x8b
    h.response(self_result)
    h.replies[2_i64].should eq([self_result])
    h.broker.active.should be_nil
  end

  it "protects the next DM ring slot even when another slot is settled" do
    h = StatefulHarness.new
    8.times do |i|
      h.client(1, stateful_dm)
      h.upstream.shift.should eq(stateful_dm)
      h.response(stateful_sent((i + 1).to_u8))
    end
    h.response(Bytes[0x82, 4, 0xbb, 0xcc, 0xdd, 1, 0, 0, 0])
    h.client(2, stateful_dm)
    h.upstream.should be_empty
    h.replies[2_i64].last.should eq(Bytes[1, 4])
    h.response(Bytes[0x82, 1, 0xbb, 0xcc, 0xdd, 1, 0, 0, 0])
    h.client(2, stateful_dm)
    h.upstream.shift.should eq(stateful_dm)
  end

  it "protects signing chunks and classifies a departed owner's in-flight reply" do
    h = StatefulHarness.new
    h.client(1, Bytes[33])
    h.upstream.shift.should eq(Bytes[33])
    h.response(Bytes[0x13, 0, 3, 0, 0, 0])
    h.client(2, Bytes[33])
    h.replies[2_i64].last.should eq(Bytes[1, 4])
    h.client(1, Bytes[34, 1, 2])
    h.upstream.shift.should eq(Bytes[34, 1, 2])
    h.response(Bytes[0])
    h.client(1, Bytes[34, 3, 4])
    h.replies[1_i64].last.should eq(Bytes[1, 3])
    h.client(1, Bytes[35])
    h.upstream.shift.should eq(Bytes[35])
    h.broker.client_closed(1_i64, h.now)
    signature = Bytes.new(65, 0)
    signature[0] = 0x14
    h.response(signature)
    h.broker.failed.should be_false
    h.client(2, Bytes[34, 1])
    h.replies[2_i64].last.should eq(Bytes[1, 4])
    h.client(2, Bytes[33])
    h.upstream.shift.should eq(Bytes[33])
  end
end
