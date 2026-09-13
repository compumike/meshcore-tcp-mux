# Cross-feature acceptance tests use a deterministic companion conversation.
# GapHarness.client injects a downstream command; response injects a physical
# reply/push. upstream stores physical SendFrame actions, downstream[id] stores
# client-visible payloads. Downstream writes complete immediately; upstream
# writes are explicitly acknowledged where write timing matters. All fixtures
# use synthetic keys/timestamps, and multi-byte wire integers are little-endian.

require "./spec_helper"
require "../src/meshcore_tcp_mux/broker"

private alias GapAction = MeshCoreTCPMux::Action

private class GapHarness
  getter broker : MeshCoreTCPMux::Broker
  getter upstream = Deque(MeshCoreTCPMux::SendFrame).new
  getter downstream = Hash(Int64, Array(Bytes)).new { |h, id| h[id] = [] of Bytes }
  property now = Time::Span.zero

  def initialize(config = MeshCoreTCPMux::Config.new, ids = [1_i64, 2_i64])
    # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
    @broker = MeshCoreTCPMux::Broker.new(91_i64, Bytes.new(32, 0x77), config)
    ids.each { |id| @broker.admit(id, @now); flush }
    while pop = @upstream.shift?
      # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
      pop.payload.should eq(Bytes[10_u8])
      @broker.written(0_i64, pop.epoch, pop.write_id, @now)
      # NO_MORE_MESSAGES (0x0a): inbox empty.
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

# Synthetic four-byte ACK token 01 02 03 04 (not a device identity).
private def gap_sent(token = Bytes[1_u8, 2_u8, 3_u8, 4_u8]) : Bytes
  # SENT (0x06): routing mode, four-byte ACK token, then suggested timeout (u32 LE); acceptance
  # only.
  # Suggested timeout 8000 ms (0x1f40, u32 little-endian).
  Bytes[6_u8, 1_u8] + token + Bytes[0x40_u8, 0x1f_u8, 0_u8, 0_u8]
end

# Each command/result pair uses only synthetic identifiers. Login, status,
# telemetry and discovery match a six-byte peer prefix; binary/anonymous replies
# match the SENT tag; trace matches the command's tag AND authentication bytes.
private def remote_vector(opcode : UInt8) : {Bytes, Bytes}
  # Synthetic six-byte peer public-key prefix 01..06; used only for routing matches.
  peer = Bytes[1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8]
  command = case opcode
            when 26_u8, 27_u8
              # LOGIN/STATUS request: opcode plus a zero-padded 32-byte key, with synthetic peer
              # prefix at offset 1.
              Bytes.new(33, 0_u8).tap { |p| p[0] = opcode; p[1, 6].copy_from(peer) }
            when 36_u8
              # SEND_TRACE_PATH (36): four-byte tag, four-byte authentication value, flags 0
              # (one-byte hashes), then one path hash.
              Bytes[36_u8, 9_u8, 8_u8, 7_u8, 6_u8, 5_u8, 4_u8, 3_u8, 2_u8, 0_u8, 0xaa_u8]
            when 39_u8
              # Remote TELEMETRY: opcode, three zero option bytes, 32-byte key with peer prefix
              # at offset 4.
              Bytes.new(36, 0_u8).tap { |p| p[0] = opcode; p[4, 6].copy_from(peer) }
            when 50_u8, 57_u8
              # BINARY/ANON request: opcode, 32-byte peer key, and one zero request byte.
              Bytes.new(34, 0_u8).tap { |p| p[0] = opcode; p[1, 6].copy_from(peer) }
            when 52_u8
              # PATH_DISCOVERY request: opcode, required reserved zero, then 32-byte peer key.
              Bytes.new(34, 0_u8).tap { |p| p[0] = opcode; p[2, 6].copy_from(peer) }
            else
              raise "unhandled remote opcode"
            end
  result = case opcode
           when 26_u8
             # LOGIN_SUCCESS (0x85), metadata byte 0, followed by the six-byte peer prefix.
             Bytes[0x85_u8, 0_u8] + peer
           when 27_u8
             # STATUS_RESPONSE (0x87), metadata byte 0, peer prefix, and a status-result byte.
             # Trailing status-result byte (synthetic zero status data).
             Bytes[0x87_u8, 0_u8] + peer + Bytes[0_u8]
           when 36_u8
             # TRACE_DATA (0x89): metadata/path fields, tag at bytes 4..7 and authentication at
             # 8..11; matching needs both.
             Bytes[0x89_u8, 0_u8, 0_u8, 0_u8, 9_u8, 8_u8, 7_u8, 6_u8,
               5_u8, 4_u8, 3_u8, 2_u8, 0_u8]
           when 39_u8
             # TELEMETRY_RESPONSE (0x8b), metadata byte 0, followed by six-byte peer prefix.
             Bytes[0x8b_u8, 0_u8] + peer
           when 50_u8, 57_u8
             # BINARY_RESPONSE (0x8c), metadata byte 0 and four-byte SENT tag 1 2 3 4.
             Bytes[0x8c_u8, 0_u8, 1_u8, 2_u8, 3_u8, 4_u8]
           when 52_u8
             # PATH_DISCOVERY_RESPONSE (0x8d), metadata byte 0, peer prefix, then
             # outbound/inbound paths.
             # Empty outbound and inbound path lengths appended to PATH_DISCOVERY_RESPONSE.
             Bytes[0x8d_u8, 0_u8] + peer + Bytes[0_u8, 0_u8]
           else
             raise "unhandled remote opcode"
           end
  {command, result}
end

describe "remaining design acceptance invariants" do
  it "documents the unavoidable same-peer legacy result ambiguity" do
    # Synthetic six-byte peer public-key prefix 01..06; used only for routing matches.
    peer = Bytes[1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8]
    # SEND_STATUS_REQ (27): opcode plus 32-byte synthetic peer key; matching uses its first six
    # bytes.
    status = Bytes.new(33, 0_u8).tap { |p| p[0] = 27_u8; p[1, 6].copy_from(peer) }
    # STATUS_RESPONSE (0x87), metadata byte 0, peer prefix, and a status-result byte.
    # Trailing status-result byte (synthetic zero status data).
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
    # LOGIN, STATUS, TRACE, remote TELEMETRY, BINARY, PATH_DISCOVERY, ANON.
    # Run the same ownership/interleaving contract against every remote family.
    [26_u8, 27_u8, 36_u8, 39_u8, 50_u8, 52_u8, 57_u8].each do |opcode|
      h = GapHarness.new
      command, result = remote_vector(opcode)
      h.client(1_i64, command)
      h.upstream.shift.payload.should eq(command)
      h.response(gap_sent)

      h.client(2_i64, command)
      h.upstream.should be_empty
      # ERR (0x01), BAD_STATE.
      h.downstream[2_i64].should eq([Bytes[1_u8, 4_u8]])

      # GET_DEVICE_TIME (5): local clock query.
      h.client(2_i64, Bytes[5_u8])
      # GET_DEVICE_TIME (5): local clock query.
      h.upstream.shift.payload.should eq(Bytes[5_u8])
      h.response(result)
      h.downstream[1_i64].last.should eq(result)
      # ERR (0x01), BAD_STATE.
      h.downstream[2_i64].should eq([Bytes[1_u8, 4_u8]])
      # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
      # byte-for-byte.
      h.response(Bytes[9_u8, 1_u8, 2_u8, 3_u8, 4_u8])
      # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
      # byte-for-byte.
      h.downstream[2_i64].last.should eq(Bytes[9_u8, 1_u8, 2_u8, 3_u8, 4_u8])
    end
  end

  it "does not advance DM capacity for ERR or zero-token SENT and retains it after disconnect" do
    h = GapHarness.new
    # SEND_TXT_MSG (2): type 0/plain, attempt 2, timestamp bytes 3..6 (u32 LE), recipient prefix
    # bytes 7..12, then message bytes.
    dm = Bytes[2_u8, 0_u8, 2_u8, 0_u8, 0_u8, 0_u8, 0_u8,
      1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8, 7_u8]

    h.client(1_i64, dm)
    h.upstream.shift.payload.should eq(dm)
    # ERR (0x01), TABLE_FULL.
    h.response(Bytes[1_u8, 3_u8])
    h.client(1_i64, dm)
    h.upstream.shift.payload.should eq(dm)
    # Zero ACK token: accepted send does not consume a confirmation-ring slot.
    h.response(gap_sent(Bytes[0_u8, 0_u8, 0_u8, 0_u8]))

    8.times do |i|
      h.client(1_i64, dm)
      h.upstream.shift.payload.should eq(dm)
      # Distinct nonzero ACK token (i + 1), encoded as four little-endian bytes.
      h.response(gap_sent(Bytes[(i + 1).to_u8, 0_u8, 0_u8, 0_u8]))
    end
    h.close(1_i64)
    h.client(2_i64, dm)
    h.upstream.should be_empty
    # ERR (0x01), BAD_STATE.
    h.downstream[2_i64].last.should eq(Bytes[1_u8, 4_u8])
  end

  it "virtualizes unscoped/default reset and forwards persistent default configuration" do
    h = GapHarness.new
    # SEND_CHANNEL_TXT_MSG (3): text type 0, channel index 0, timestamp bytes 3..6 (u32 LE),
    # then text (empty if absent).
    channel = Bytes[3_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8]

    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 1: explicitly unscoped sends.
    h.client(1_i64, Bytes[54_u8, 1_u8])
    # OK (0x00): command accepted, not proof of radio delivery.
    h.downstream[1_i64].last.should eq(Bytes[0_u8])
    h.client(1_i64, channel)
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 1: explicitly unscoped sends.
    h.upstream.shift.payload.should eq(Bytes[54_u8, 1_u8])
    # OK (0x00): command accepted, not proof of radio delivery.
    h.response(Bytes[0_u8])
    h.upstream.shift.payload.should eq(channel)
    # OK (0x00): command accepted, not proof of radio delivery.
    h.response(Bytes[0_u8])
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 0 with no key: restore the configured default scope.
    h.upstream.shift.payload.should eq(Bytes[54_u8, 0_u8])
    # OK (0x00): command accepted, not proof of radio delivery.
    h.response(Bytes[0_u8])

    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 0 with no key: restore the configured default scope.
    h.client(1_i64, Bytes[54_u8, 0_u8])
    # OK (0x00): command accepted, not proof of radio delivery.
    h.downstream[1_i64].last.should eq(Bytes[0_u8])
    # SET_DEFAULT_FLOOD_SCOPE (63): 31-byte NUL-terminated name ('x') plus 16-byte zero key;
    # persistent shared state.
    persistent = Bytes.new(48, 0_u8)
    persistent[0] = 63_u8
    persistent[1] = 'x'.ord.to_u8
    h.client(1_i64, persistent)
    h.upstream.shift.payload.should eq(persistent)
    # OK (0x00): command accepted, not proof of radio delivery.
    h.response(Bytes[0_u8])
    h.client(1_i64, channel)
    h.upstream.shift.payload.should eq(channel)
  end

  it "uses pop-completion membership and preserves byte-identical native inbox entries" do
    h = GapHarness.new
    # Minimal legacy CONTACT_MESSAGE: opcode 7, six-byte sender prefix, path/type bytes, u32
    # timestamp; no text body.
    item = Bytes.new(13, 0x44_u8).tap { |p| p[0] = 7_u8; p[8] = 0_u8 }

    # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
    h.response(Bytes[0x83_u8])
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.upstream.shift.payload.should eq(Bytes[10_u8])
    h.broker.admit(3_i64, h.now)
    h.flush
    h.response(item)
    # Fan-out requests the next physical pop. Session 4 joins after the first
    # fan-out and therefore must not acquire historical item 1.
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.upstream.shift.payload.should eq(Bytes[10_u8])
    h.broker.admit(4_i64, h.now)
    h.flush
    h.response(item)
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.upstream.shift.payload.should eq(Bytes[10_u8])
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    h.response(Bytes[10_u8])

    {1_i64, 2_i64, 3_i64}.each do |id|
      # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
      2.times { h.client(id, Bytes[10_u8]) }
      h.downstream[id].select { |p| p[0] == 7 }.should eq([item, item])
    end
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.client(4_i64, Bytes[10_u8])
    h.downstream[4_i64].select { |p| p[0] == 7 }.should eq([item])
  end

  it "disconnects only a raw-push output consumer whose bounded writes are full" do
    config = MeshCoreTCPMux::Config.new
    config.output_frames = 1
    # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
    broker = MeshCoreTCPMux::Broker.new(92_i64, Bytes.new(32), config)
    broker.admit(1_i64, Time::Span.zero)
    broker.admit(2_i64, Time::Span.zero)
    pop = broker.take_actions.compact_map(&.as?(MeshCoreTCPMux::SendFrame)).find { |a| a.session == 0 }.not_nil!
    broker.written(0_i64, pop.epoch, pop.write_id, Time::Span.zero)
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    broker.upstream_frame(Bytes[10_u8], Time::Span.zero)
    broker.take_actions

    # ADVERT push (0x80) plus a 32-byte synthetic key; different fill bytes distinguish the
    # broadcasts.
    first = Bytes.new(33, 1_u8).tap { |p| p[0] = 0x80_u8 }
    broker.upstream_frame(first, Time::Span.zero)
    first_actions = broker.take_actions.compact_map(&.as?(MeshCoreTCPMux::SendFrame))
    healthy_write = first_actions.find { |a| a.session == 2 }.not_nil!
    broker.written(2_i64, healthy_write.epoch, healthy_write.write_id, Time::Span.zero)
    broker.take_actions

    # ADVERT push (0x80) plus a 32-byte synthetic key; different fill bytes distinguish the
    # broadcasts.
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
    # GET_CONTACTS (4); optional trailing u32 LE is the 'since' timestamp.
    h.client(1_i64, Bytes[4_u8])
    # GET_CONTACTS (4); optional trailing u32 LE is the 'since' timestamp.
    h.upstream.shift.payload.should eq(Bytes[4_u8])
    h.now = 1.second
    # CONTACTS_START (0x02) with contact count 9 (u32 little-endian).
    h.response(Bytes[2_u8, 9_u8, 0_u8, 0_u8, 0_u8])
    h.now = 5.999.seconds
    # LOG_RX_DATA (0x88): SNR and RSSI metadata; unrelated raw traffic must not advance a
    # command deadline.
    h.response(Bytes[0x88_u8, 1_u8, 0_u8])
    h.broker.failed.should be_false
    h.now = 6.seconds
    # LOG_RX_DATA (0x88): SNR and RSSI metadata; unrelated raw traffic must not advance a
    # command deadline.
    h.response(Bytes[0x88_u8, 2_u8, 0_u8])
    h.broker.failed.should be_true
  end

  it "shares only qualifying empty probes and never polls without clients" do
    # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
    idle = MeshCoreTCPMux::Broker.new(93_i64, Bytes.new(32))
    idle.tick(20.seconds)
    idle.take_actions.compact_map(&.as?(MeshCoreTCPMux::SendFrame)).should be_empty

    h = GapHarness.new(ids: [1_i64, 2_i64, 3_i64])
    # GET_CONTACTS (4); optional trailing u32 LE is the 'since' timestamp.
    h.client(1_i64, Bytes[4_u8])
    # GET_CONTACTS (4); optional trailing u32 LE is the 'since' timestamp.
    h.upstream.shift.payload.should eq(Bytes[4_u8])
    # CONTACTS_START (0x02) with contact count 0 (u32 little-endian).
    h.response(Bytes[2_u8, 0_u8, 0_u8, 0_u8, 0_u8])
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.client(2_i64, Bytes[10_u8])
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.client(3_i64, Bytes[10_u8])
    # END_OF_CONTACTS (0x04), followed by the u32 LE last-modified timestamp.
    h.response(Bytes[4_u8, 0_u8, 0_u8, 0_u8, 0_u8])
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.upstream.shift.payload.should eq(Bytes[10_u8])

    # Client 4 joined after this physical pop began: its empty observation must
    # come from a newer probe, not an old result it was never waiting on.
    h.broker.admit(4_i64, h.now)
    h.flush
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.client(4_i64, Bytes[10_u8])
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    h.response(Bytes[10_u8])
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    h.downstream[2_i64].last.should eq(Bytes[10_u8])
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    h.downstream[3_i64].last.should eq(Bytes[10_u8])
    h.downstream[4_i64].should be_empty
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.upstream.shift.payload.should eq(Bytes[10_u8])
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    h.response(Bytes[10_u8])
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    h.downstream[4_i64].last.should eq(Bytes[10_u8])

    h.now = 5.seconds
    h.broker.tick(h.now)
    h.flush
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.upstream.shift.payload.should eq(Bytes[10_u8])
  end

  it "expires a virtual sync locally while a progressing contacts stream continues" do
    h = GapHarness.new
    # GET_CONTACTS (4); optional trailing u32 LE is the 'since' timestamp.
    h.client(1_i64, Bytes[4_u8])
    # GET_CONTACTS (4); optional trailing u32 LE is the 'since' timestamp.
    h.upstream.shift.payload.should eq(Bytes[4_u8])
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.client(2_i64, Bytes[10_u8])
    h.now = 4.seconds
    # CONTACTS_START (0x02) with contact count 1 (u32 little-endian).
    h.response(Bytes[2_u8, 1_u8, 0_u8, 0_u8, 0_u8])
    h.now = 5.seconds
    h.broker.tick(h.now)
    h.flush
    # ERR (0x01), BAD_STATE.
    h.downstream[2_i64].last.should eq(Bytes[1_u8, 4_u8])
    h.broker.failed.should be_false
    h.broker.active.should_not be_nil
  end

  it "enforces the contacts total deadline despite valid idle progress" do
    config = MeshCoreTCPMux::Config.new
    config.response_timeout = 5.seconds
    config.contacts_timeout = 30.seconds
    h = GapHarness.new(config)
    # GET_CONTACTS (4); optional trailing u32 LE is the 'since' timestamp.
    h.client(1_i64, Bytes[4_u8])
    # GET_CONTACTS (4); optional trailing u32 LE is the 'since' timestamp.
    h.upstream.shift.payload.should eq(Bytes[4_u8])
    h.now = 1.second
    # CONTACTS_START (0x02) with contact count 20 (u32 little-endian).
    h.response(Bytes[2_u8, 20_u8, 0_u8, 0_u8, 0_u8])
    # CONTACT record: 148 bytes total (opcode 3 plus native contact fields); unused fields are
    # synthetic zeroes.
    contact = Bytes.new(148, 0_u8).tap { |p| p[0] = 3_u8 }
    # Contact records every four seconds keep the five-second idle timer alive,
    # but cannot extend the separate thirty-second total transaction limit.
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
    # GET_DEVICE_TIME (5): local clock query.
    h.client(1_i64, Bytes[5_u8])
    # GET_DEVICE_TIME (5): local clock query.
    h.upstream.shift.payload.should eq(Bytes[5_u8])
    # SET_DEVICE_TIME (6), timestamp 0 (u32 little-endian); a queued state-changing command.
    h.client(2_i64, Bytes[6_u8, 0_u8, 0_u8, 0_u8, 0_u8])
    h.now = 3.seconds
    # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
    # byte-for-byte.
    h.response(Bytes[9_u8, 0_u8, 0_u8, 0_u8, 0_u8])
    # ERR (0x01), BAD_STATE.
    h.downstream[2_i64].last.should eq(Bytes[1_u8, 4_u8])
    h.upstream.should be_empty

    limited = MeshCoreTCPMux::Config.new
    limited.command_limit = 1
    one = GapHarness.new(limited, [1_i64])
    # GET_DEVICE_TIME (5): local clock query.
    one.client(1_i64, Bytes[5_u8])
    # GET_DEVICE_TIME (5): local clock query.
    one.upstream.shift.payload.should eq(Bytes[5_u8])
    # GET_DEVICE_TIME (5): local clock query.
    one.client(1_i64, Bytes[5_u8])
    one.broker.sessions.has_key?(1_i64).should be_false
  end

  it "applies the default maintenance and private-key export refusal policy locally" do
    h = GapHarness.new(ids: [1_i64])
    commands = [
      # IMPORT_PRIVATE_KEY (24), followed by 64 synthetic zero key bytes;
      # maintenance-restricted.
      # 64 zero bytes stand in for private-key material; this fixture must be refused by
      # default.
      Bytes[24_u8] + Bytes.new(64, 0_u8),
      # FACTORY_RESET (51) plus required ASCII "reset" magic string.
      Bytes[51_u8] + "reset".to_slice,
    ]
    commands.each do |command|
      h.client(1_i64, command)
      # ERR (0x01), UNSUPPORTED_CMD.
      h.downstream[1_i64].last.should eq(Bytes[1_u8, 1_u8])
      h.upstream.should be_empty
    end
    # EXPORT_PRIVATE_KEY (23): request secret key material.
    h.client(1_i64, Bytes[23_u8])
    # DISABLED (0x0f): native private-key-export refusal, not a generic ERR.
    h.downstream[1_i64].last.should eq(Bytes[0x0f_u8])
    h.upstream.should be_empty
  end

  it "allows reboot by default with multiple clients and pending radio work" do
    h = GapHarness.new
    status, _ = remote_vector(27_u8)
    h.client(1_i64, status)
    h.upstream.shift.payload.should eq(status)
    h.response(gap_sent)

    # REBOOT (19) followed by the required ASCII magic string "reboot".
    reboot = Bytes[19_u8] + "reboot".to_slice
    h.client(2_i64, reboot)
    h.upstream.shift.payload.should eq(reboot)
    h.downstream[2_i64].should be_empty
  end

  it "gates enabled maintenance on every live shared radio resource" do
    # FACTORY_RESET (51) plus required ASCII "reset" magic string.
    reset = Bytes[51_u8] + "reset".to_slice

    remote_config = MeshCoreTCPMux::Config.new.tap { |c| c.maintenance = true }
    remote = GapHarness.new(remote_config, [1_i64])
    status, _ = remote_vector(27_u8)
    remote.client(1_i64, status)
    remote.upstream.shift.payload.should eq(status)
    remote.response(gap_sent)
    remote.client(1_i64, reset)
    # ERR (0x01), BAD_STATE.
    remote.downstream[1_i64].last.should eq(Bytes[1_u8, 4_u8])
    remote.upstream.should be_empty

    dm = GapHarness.new(remote_config, [1_i64])
    # SEND_TXT_MSG (2): type 0/plain, attempt 0, timestamp bytes 3..6 (u32 LE), recipient prefix
    # bytes 7..12, then message bytes.
    dm_command = Bytes[2_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8,
      1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8, 7_u8]
    dm.client(1_i64, dm_command)
    dm.upstream.shift.payload.should eq(dm_command)
    dm.response(gap_sent)
    dm.client(1_i64, reset)
    # ERR (0x01), BAD_STATE.
    dm.downstream[1_i64].last.should eq(Bytes[1_u8, 4_u8])
    dm.upstream.should be_empty

    signing = GapHarness.new(remote_config, [1_i64])
    # SIGN_START (33/0x21): start the shared signing session.
    signing.client(1_i64, Bytes[33_u8])
    # SIGN_START (33/0x21): start the shared signing session.
    signing.upstream.shift.payload.should eq(Bytes[33_u8])
    # SIGN_START response (0x13): reserved 0, byte limit 1 (u32 LE).
    signing.response(Bytes[0x13_u8, 0_u8, 1_u8, 0_u8, 0_u8, 0_u8])
    signing.client(1_i64, reset)
    # ERR (0x01), BAD_STATE.
    signing.downstream[1_i64].last.should eq(Bytes[1_u8, 4_u8])
    signing.upstream.should be_empty
  end

  it "preserves a scoped send exactly once when its owner disconnects mid-compound" do
    h = GapHarness.new
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 1: explicitly unscoped sends.
    scope = Bytes[54_u8, 1_u8]
    # SEND_CHANNEL_TXT_MSG (3): text type 0, channel index 0, timestamp bytes 3..6 (u32 LE),
    # then text (empty if absent).
    channel = Bytes[3_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8]
    h.client(1_i64, scope)
    h.client(1_i64, channel)
    h.upstream.shift.payload.should eq(scope)
    h.close(1_i64)
    # OK (0x00): command accepted, not proof of radio delivery.
    h.response(Bytes[0_u8])
    h.upstream.shift.payload.should eq(channel)
    # OK (0x00): command accepted, not proof of radio delivery.
    h.response(Bytes[0_u8])
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 0 with no key: restore the configured default scope.
    h.upstream.shift.payload.should eq(Bytes[54_u8, 0_u8])
    # OK (0x00): command accepted, not proof of radio delivery.
    h.response(Bytes[0_u8])
    h.upstream.should be_empty
    h.downstream[2_i64].should be_empty
    h.broker.failed.should be_false
  end

  it "does not lose MSG_WAITING arriving immediately after an empty result" do
    h = GapHarness.new
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.client(1_i64, Bytes[10_u8])
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.upstream.shift.payload.should eq(Bytes[10_u8])
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    h.response(Bytes[10_u8])
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    h.downstream[1_i64].last.should eq(Bytes[10_u8])
    # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
    h.response(Bytes[0x83_u8])
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.upstream.shift.payload.should eq(Bytes[10_u8])
  end

  it "requires exactly one downstream session for enabled maintenance" do
    config = MeshCoreTCPMux::Config.new
    config.maintenance = true
    h = GapHarness.new(config)
    # FACTORY_RESET (51) plus required ASCII "reset" magic string.
    reset = Bytes[51_u8] + "reset".to_slice
    h.client(1_i64, reset)
    # ERR (0x01), BAD_STATE.
    h.downstream[1_i64].last.should eq(Bytes[1_u8, 4_u8])
    h.upstream.should be_empty
  end

  it "keeps queued APP_START and contact lookup behind an interleaved contacts stream" do
    h = GapHarness.new
    # GET_CONTACTS (4); optional trailing u32 LE is the 'since' timestamp.
    h.client(1_i64, Bytes[4_u8, 0_u8, 0_u8, 0_u8, 0_u8])
    # GET_CONTACTS (4); optional trailing u32 LE is the 'since' timestamp.
    h.upstream.shift.payload.should eq(Bytes[4_u8, 0_u8, 0_u8, 0_u8, 0_u8])
    # CONTACTS_START (0x02) with contact count 7 (u32 little-endian).
    h.response(Bytes[2_u8, 7_u8, 0_u8, 0_u8, 0_u8])

    # APP_START: opcode 1 followed by the seven reserved zero bytes (empty application name).
    app_start = Bytes.new(8, 0_u8).tap { |p| p[0] = 1_u8 }
    # GET_CONTACT_BY_KEY (30) plus 32 synthetic 0x22 public-key bytes.
    lookup = Bytes.new(33, 0x22_u8).tap { |p| p[0] = 30_u8 }
    h.client(2_i64, app_start)
    h.client(2_i64, lookup)
    # LOG_RX_DATA (0x88): SNR and RSSI metadata; unrelated raw traffic must not advance a
    # command deadline.
    h.response(Bytes[0x88_u8, 1_u8, 2_u8])
    # ADVERT push (0x80) plus a 32-byte synthetic key; different fill bytes distinguish the
    # broadcasts.
    advert = Bytes.new(33, 4_u8).tap { |p| p[0] = 0x80_u8 }
    h.response(advert)
    h.upstream.should be_empty

    # CONTACT record: 148 bytes total (opcode 3 plus native contact fields); unused fields are
    # synthetic zeroes.
    contact = Bytes.new(148, 0_u8).tap { |p| p[0] = 3_u8 }
    h.response(contact)
    h.upstream.should be_empty
    # END_OF_CONTACTS (0x04), followed by the u32 LE last-modified timestamp.
    h.response(Bytes[4_u8, 1_u8, 0_u8, 0_u8, 0_u8])
    h.upstream.shift.payload.should eq(app_start)
    # SELF_INFO: minimum 58-byte response; opcode 5, public key occupies offsets 4..35.
    self_info = Bytes.new(58, 0_u8).tap { |p| p[0] = 5_u8 }
    h.response(self_info)
    h.upstream.shift.payload.should eq(lookup)
    h.response(contact)
    h.downstream[2_i64].should eq([
      # LOG_RX_DATA (0x88): SNR and RSSI metadata; unrelated raw traffic must not advance a
      # command deadline.
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
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.upstream.shift.payload.should eq(Bytes[10_u8])
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    h.response(Bytes[10_u8])
    h.downstream[1_i64].should eq([gap_sent])
    h.response(result)
    h.downstream[1_i64].should eq([gap_sent])
    h.broker.failed.should be_false

    # GET_DEVICE_TIME (5): local clock query.
    h.client(1_i64, Bytes[5_u8])
    # GET_DEVICE_TIME (5): local clock query.
    h.upstream.shift.payload.should eq(Bytes[5_u8])
  end

  it "preserves signing chunk order and requires restart after inactivity" do
    h = GapHarness.new
    # SIGN_START (33/0x21): start the shared signing session.
    h.client(1_i64, Bytes[33_u8])
    # SIGN_START (33/0x21): start the shared signing session.
    h.upstream.shift.payload.should eq(Bytes[33_u8])
    # SIGN_START response (0x13): reserved 0, byte limit 8 (u32 LE).
    h.response(Bytes[0x13_u8, 0_u8, 8_u8, 0_u8, 0_u8, 0_u8])

    # SIGN_DATA (34/0x22), followed by 2 literal data byte(s); only accepted data counts against
    # the signing budget.
    first = Bytes[34_u8, 1_u8, 2_u8]
    # SIGN_DATA (34/0x22), followed by 3 literal data byte(s); only accepted data counts against
    # the signing budget.
    second = Bytes[34_u8, 3_u8, 4_u8, 5_u8]
    h.client(1_i64, first)
    h.upstream.shift.payload.should eq(first)
    # OK (0x00): command accepted, not proof of radio delivery.
    h.response(Bytes[0_u8])
    h.now = 1.second
    h.client(1_i64, second)
    h.upstream.shift.payload.should eq(second)
    # OK (0x00): command accepted, not proof of radio delivery.
    h.response(Bytes[0_u8])

    h.now = 31.seconds
    h.broker.tick(h.now)
    h.flush
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    h.upstream.shift.payload.should eq(Bytes[10_u8])
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    h.response(Bytes[10_u8])
    # SIGN_FINISH (35/0x23): finish the current signing session and await SIGNATURE.
    h.client(1_i64, Bytes[35_u8])
    # ERR (0x01), BAD_STATE.
    h.downstream[1_i64].last.should eq(Bytes[1_u8, 4_u8])
    h.upstream.should be_empty
    # SIGN_START (33/0x21): start the shared signing session.
    h.client(1_i64, Bytes[33_u8])
    # SIGN_START (33/0x21): start the shared signing session.
    h.upstream.shift.payload.should eq(Bytes[33_u8])
  end
end
