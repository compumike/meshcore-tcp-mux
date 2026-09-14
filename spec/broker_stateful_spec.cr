require "./spec_helper"
require "../src/meshcore_tcp_mux/broker"

private class StatefulHarness
  # These are broker-level conversations, not socket tests. client(id, bytes)
  # injects one decoded client command; response(bytes) injects a companion reply
  # or asynchronous push. upstream records physical commands in dispatch order;
  # replies[id] records only what that downstream client would receive.
  # Session 0 is the physical companion; sessions 1 and 2 are independent clients.
  # flush acknowledges writes immediately, but never invents companion replies.
  # Time advances only when a test changes now, so radio leases are deterministic.
  # Wire fixtures below are decoded payloads, not TCP frames. All multi-byte
  # integers are little-endian; keys, timestamps and message bodies are synthetic.
  # Comments distinguish immediate command acceptance from later radio results.

  getter broker : MeshCoreTCPMux::Broker
  getter upstream = Deque(Bytes).new
  getter replies = Hash(Int64, Array(Bytes)).new { |h, k| h[k] = [] of Bytes }
  getter diagnostics = [] of MeshCoreTCPMux::Diagnostic
  property now = Time::Span.zero

  def initialize(config = MeshCoreTCPMux::Config.new) : Nil
    # Synthetic 32-byte companion public key; identifies the epoch, never a real radio key.
    @broker = MeshCoreTCPMux::Broker.new(1_i64, Bytes.new(32, 0x77), config)
    @broker.admit(1_i64, @now)
    @broker.admit(2_i64, @now)
    flush
    # Admission probes the physical inbox. Drain it to an empty baseline so a
    # test's first observed command belongs to that test, not initialization.
    until @upstream.empty?
      # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
      @upstream.shift.should eq(Bytes[10])
      # NO_MORE_MESSAGES (0x0a): inbox empty.
      response(Bytes[10])
    end
  end

  def client(id, payload) : Nil
    @broker.client_frame(id.to_i64, payload, @now)
    flush
  end

  def response(payload) : Nil
    @broker.upstream_frame(payload, @now)
    flush
  end

  def flush : Nil
    loop do
      actions = @broker.take_actions
      break if actions.empty?
      actions.each do |action|
        if action.is_a?(MeshCoreTCPMux::Diagnostic)
          @diagnostics << action
        elsif action.is_a?(MeshCoreTCPMux::SendFrame)
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

private def stateful_sent(token : UInt8 = 1) : Bytes
  # SENT is acceptance, not delivery: [0x06, routing mode, ACK token (4 bytes),
  # suggested timeout in milliseconds (u32 little-endian)]. 0x1f40 is 8000 ms.
  # Vary only the token's first byte so ring-slot ownership is easy to track.
  # SENT (0x06): routing mode, four-byte ACK token, then suggested timeout (u32 LE); acceptance
  # only.
  Bytes[6, 1, token, 0xbb, 0xcc, 0xdd, 0x40, 0x1f, 0, 0]
end

private def stateful_dm : Bytes
  # SEND_TXT_MSG: opcode 2, plain-text type 0, attempt 2, timestamp 0x12345678
  # (u32 little-endian), six-byte synthetic recipient prefix 01..06, then "hi".
  # The nonzero attempt/timestamp make unwanted rewriting visible in equality tests.
  # SEND_TXT_MSG (2): type 0/plain, attempt 2, timestamp bytes 3..6 (u32 LE), recipient prefix
  # bytes 7..12, then message bytes.
  Bytes[2, 0, 2, 0x78, 0x56, 0x34, 0x12, 1, 2, 3, 4, 5, 6, 0x68, 0x69]
end

describe "broker stateful operations" do
  it "logs transaction metadata without logging private response bytes" do
    config = MeshCoreTCPMux::Config.new
    # Enable export only for this test; the fake key must reach its requester,
    # but neither its bytes nor an encoded copy belongs in diagnostic logs.
    config.private_key_export = true
    h = StatefulHarness.new(config)
    # EXPORT_PRIVATE_KEY (23): request secret key material.
    h.client(1, Bytes[23])
    # EXPORT_PRIVATE_KEY (23): request secret key material.
    h.upstream.shift.should eq(Bytes[23])
    # PRIVATE_KEY response: opcode 0x0e plus 64 conspicuous synthetic 'S' bytes to detect log
    # leakage.
    secret = Bytes.new(65, 'S'.ord.to_u8)
    secret[0] = 0x0e
    h.broker.upstream_frame(secret, 1.millisecond)
    logs = h.broker.take_actions.compact_map(&.as?(MeshCoreTCPMux::Diagnostic)).map(&.message).join('\n')
    logs.should contain("command=export_private_key")
    logs.should contain("response=private_key code=0x0e response_bytes=65")
    logs.should contain("elapsed_ms=1.0")
    logs.should_not contain("S" * 64)
  end

  it "logs the complete remote status lease lifecycle including expiration" do
    h = StatefulHarness.new
    h.diagnostics.clear
    peer = Bytes.new(32) { |i| (0x40 + i).to_u8 }
    # SEND_STATUS_REQ (27): opcode plus the remote repeater's public key.
    h.client(1, Bytes[27_u8] + peer)
    h.upstream.shift.should eq(Bytes[27_u8] + peer)

    received = h.diagnostics.map(&.message).join('\n')
    received.should contain("event=command.received")
    received.should contain("event=command.dispatched")
    received.should contain("event=remote_lease.reserved")
    received.should contain("peer=#{peer.hexstring}")

    # SENT (0x06): type 1, token 0x12345678, and a 1000 ms radio timeout.
    h.response(Bytes[6, 1, 0x78, 0x56, 0x34, 0x12, 0xe8, 0x03, 0, 0])
    accepted = h.diagnostics.map(&.message).join('\n')
    accepted.should contain("event=remote_lease.accepted")
    accepted.should contain("token=305419896")
    accepted.should contain("radio_timeout_ms=1000")

    # LeaseTime retains the request for at least five seconds; no STATUS_RESPONSE
    # arrived, so the next tick exposes that terminal outcome in the logs.
    h.broker.tick(6.seconds)
    expired = h.broker.take_actions.compact_map(&.as?(MeshCoreTCPMux::Diagnostic)).map(&.message).join('\n')
    expired.should contain("event=remote_lease.expired")
    expired.should contain("kind=status")
    expired.should contain("peer=#{peer.hexstring}")
  end

  it "preserves signed, CLI, unknown text, and binary inbox bodies for both client versions" do
    # Use non-text bytes, embedded NULs and framing markers as message bodies.
    # The mux must treat bodies as opaque bytes, not decode/re-encode strings.
    # Legacy DM: 13-byte header + 4-byte signed sender prefix + 4 body bytes; type byte 8 is set
    # to 2.
    legacy_signed = Bytes.new(21, 0xff)
    legacy_signed[0] = 7
    legacy_signed[8] = 2
    # Legacy DM: 13-byte header + 5 body bytes filled with '<' (0x3c); type byte 8 is set to CLI
    # type 1.
    legacy_cli = Bytes.new(18, 0x3c)
    legacy_cli[0] = 7
    legacy_cli[8] = 1
    # Unknown text types are preserved too; only known signed type 2 needs
    # the extra four-byte sender prefix checked by the protocol validator.
    unknown_text = legacy_cli.dup
    unknown_text[8] = 0x7f
    # CONTACT_MESSAGE_V3 (0x10): SNR byte 0xf0 plus two metadata bytes, then the legacy
    # fields/body.
    v3_signed = Bytes[0x10, 0xf0, 0, 0] + legacy_signed[1..]
    # Legacy CHANNEL_MESSAGE: channel 2, path length 0xff, plain type 0, timestamp 0x12345678,
    # body ff 00 3e.
    legacy_channel = Bytes[8, 2, 0xff, 0, 0x78, 0x56, 0x34, 0x12, 0xff, 0, 0x3e]
    # CHANNEL_MESSAGE_V3 (0x11): SNR byte 0xf0 plus two metadata bytes, then the legacy
    # fields/body.
    v3_channel = Bytes[0x11, 0xf0, 0, 0] + legacy_channel[1..]
    # CHANNEL_DATA (0x1b): eight metadata/header bytes after opcode; byte 8 declares the
    # trailing binary-body length.
    datagram = Bytes[0x1b, 0xf0, 0, 0, 2, 0xff, 0x34, 0x12, 4, 0, 0xff, 0x3c, 0x3e]

    [legacy_signed, legacy_cli, unknown_text, v3_signed, legacy_channel, v3_channel, datagram].each do |item|
      h = StatefulHarness.new
      # Client 1 keeps the default legacy target; client 2 understands V3.
      h.broker.sessions[2_i64].target_version = 13_u8
      # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
      h.response(Bytes[0x83])
      # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
      h.upstream.shift.should eq(Bytes[10])
      # One physical pop creates a separate inbox entry for each client.
      h.response(item)
      # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
      h.upstream.shift.should eq(Bytes[10])
      # NO_MORE_MESSAGES (0x0a): inbox empty.
      h.response(Bytes[10])
      # Ignore availability notifications and inspect only explicit pop replies.
      h.replies.clear
      # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
      h.client(1, Bytes[10])
      # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
      h.client(2, Bytes[10])
      # Legacy conversion changes the text opcode and removes ONLY the three
      # V3 metadata bytes; binary channel data has no legacy conversion.
      expected = case item[0]
                 # CONTACT_MESSAGE (0x07) legacy opcode; append the original body after
                 # stripping V3 metadata.
                 when 0x10 then Bytes[7] + item[4..] # CONTACT_MESSAGE_V3 -> CONTACT_MESSAGE.
                 # CHANNEL_MESSAGE (0x08) legacy opcode; append the original body after
                 # stripping V3 metadata.
                 when 0x11 then Bytes[8] + item[4..] # CHANNEL_MESSAGE_V3 -> CHANNEL_MESSAGE.
                 else           item
                 end
      h.replies[1_i64].should eq([expected])
      h.replies[2_i64].should eq([item])
      h.upstream.should be_empty
    end
  end

  it "classifies every supported push before an unrelated immediate response" do
    h = StatefulHarness.new
    h.replies.clear
    # GET_DEVICE_TIME (5): client 1 owns the ordinary CURRENT_TIME reply slot.
    h.client(1, Bytes[5])
    h.upstream.shift.should eq(Bytes[5])

    # Every native_v13 asynchronous response class is injected while that
    # clock query is active. Global observations are broadcast; remote results
    # without an accepted lease are discarded; MSG_WAITING is only an inbox
    # pump hint. None may complete or refresh the clock transaction.
    global_pushes = [
      Bytes.new(33, 0_u8).tap { |p| p[0] = 0x80_u8 }, # ADVERT: opcode plus 32-byte key.
      Bytes.new(33, 0_u8).tap { |p| p[0] = 0x81_u8 }, # PATH_UPDATED: opcode plus 32-byte key.
      # SEND_CONFIRMED: four-byte token and four-byte little-endian round-trip time.
      Bytes[0x82_u8, 1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 0_u8, 0_u8, 0_u8],
      # RAW_DATA: opcode and the minimum three opaque metadata bytes.
      Bytes[0x84_u8, 0_u8, 0_u8, 0_u8],
      # LOG_RX_DATA: opcode, SNR, and RSSI; no packet body is required.
      Bytes[0x88_u8, 0_u8, 0_u8],
      # NEW_ADVERT: the full 148-byte native contact record.
      Bytes.new(148, 0_u8).tap { |p| p[0] = 0x8a_u8 },
      # CONTROL_DATA: opcode and the minimum three opaque metadata bytes.
      Bytes[0x8e_u8, 0_u8, 0_u8, 0_u8],
      Bytes.new(33, 0_u8).tap { |p| p[0] = 0x8f_u8 }, # CONTACT_DELETED plus 32-byte key.
      Bytes[0x90_u8],                                 # CONTACTS_FULL notification.
      Bytes[0x91_u8, 0xaa_u8],                        # Unknown bounded extension push.
    ]
    orphaned_remote_results = [
      # LOGIN_SUCCESS: metadata and a synthetic six-byte peer prefix.
      Bytes[0x85_u8, 0_u8, 1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8],
      # LOGIN_FAILURE: metadata and a synthetic six-byte peer prefix.
      Bytes[0x86_u8, 0_u8, 1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8],
      # STATUS_RESPONSE: common peer header plus one status byte.
      Bytes[0x87_u8, 0_u8, 1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8, 0_u8],
      # TRACE_DATA: zero path bytes, flags zero, four-byte tag, four-byte
      # authentication value, and the required final SNR byte.
      Bytes[0x89_u8, 0_u8, 0_u8, 0_u8, 1_u8, 2_u8, 3_u8, 4_u8,
        5_u8, 6_u8, 7_u8, 8_u8, 0_u8],
      # TELEMETRY_RESPONSE: metadata and a synthetic six-byte peer prefix.
      Bytes[0x8b_u8, 0_u8, 1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8],
      # BINARY_RESPONSE: metadata plus its four-byte correlation tag.
      Bytes[0x8c_u8, 0_u8, 1_u8, 2_u8, 3_u8, 4_u8],
      # PATH_DISCOVERY_RESPONSE: common peer header followed by empty
      # outbound and inbound encoded paths.
      Bytes[0x8d_u8, 0_u8, 1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8, 0_u8, 0_u8],
    ]

    global_pushes.each { |push| h.response(push) }
    orphaned_remote_results.each { |push| h.response(push) }
    h.response(Bytes[0x83_u8]) # MSG_WAITING: hidden physical-inbox drain hint.
    h.broker.active.should_not be_nil
    h.upstream.should be_empty
    h.replies[1_i64].should eq(global_pushes)
    h.replies[2_i64].should eq(global_pushes)

    # CURRENT_TIME (0x09): only this five-byte ordinary response completes the
    # active GET_DEVICE_TIME, and it is visible only to its owner.
    current_time = Bytes[9_u8, 0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8]
    h.response(current_time)
    h.replies[1_i64].last.should eq(current_time)
    h.replies[2_i64].should eq(global_pushes)
    # Once the unrelated clock transaction completes, the earlier hidden
    # MSG_WAITING hint may finally schedule a physical inbox pop.
    h.upstream.shift.should eq(Bytes[10_u8]) # SYNC_NEXT_MESSAGE.
    h.response(Bytes[10_u8])                 # NO_MORE_MESSAGES.
    h.broker.active.should be_nil
  end

  it "broadcasts a late ACK immediately before or after an unrelated query reply" do
    [:before, :after].each do |order|
      h = StatefulHarness.new
      # Client 1 sends a plain DM and receives native SENT acceptance with the
      # synthetic token 01 bb cc dd. Delivery confirmation remains asynchronous.
      h.client(1, stateful_dm)
      h.upstream.shift.should eq(stateful_dm)
      h.response(stateful_sent(1_u8))
      h.replies.clear

      # Client 2 now owns GET_DEVICE_TIME's immediate response slot.
      h.client(2, Bytes[5_u8])
      h.upstream.shift.should eq(Bytes[5_u8])
      # SEND_CONFIRMED: token 01 bb cc dd and one-millisecond round trip.
      acknowledgement = Bytes[0x82_u8, 1_u8, 0xbb_u8, 0xcc_u8, 0xdd_u8,
        1_u8, 0_u8, 0_u8, 0_u8]
      # CURRENT_TIME: timestamp 0x12345678 in little-endian byte order.
      current_time = Bytes[9_u8, 0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8]

      if order == :before
        h.response(acknowledgement)
        h.broker.active.should_not be_nil
        h.response(current_time)
      else
        h.response(current_time)
        h.broker.active.should be_nil
        h.response(acknowledgement)
      end

      # Actual ACK frames broadcast unchanged under the implemented contract;
      # only the ordinary CURRENT_TIME is owned exclusively by client 2.
      h.replies[1_i64].should eq([acknowledgement])
      expected_second = order == :before ? [acknowledgement, current_time] : [current_time, acknowledgement]
      h.replies[2_i64].should eq(expected_second)
      h.broker.active.should be_nil
    end
  end

  it "preserves send bytes and native acceptance and does not create a retry or echo" do
    h = StatefulHarness.new
    first_attempt = stateful_dm
    retry_attempt = stateful_dm.dup
    # SEND_TXT_MSG attempt is byte 2. The client preserves its logical timestamp
    # and body while explicitly incrementing only this retry counter from 2 to 3.
    retry_attempt[2] = 3_u8
    [first_attempt, retry_attempt].each_with_index do |command, index|
      h.now = index.seconds
      h.client(1, command)
      h.upstream.shift.should eq(command)
      h.response(stateful_sent)
    end
    h.replies[1_i64].should eq([stateful_sent, stateful_sent])
    h.replies[2_i64].should be_empty
    h.upstream.should be_empty
    # Channel acceptance is OK rather than SENT. Neither kind of acceptance
    # is an incoming-message echo for the other connected client.
    # SEND_CHANNEL_TXT_MSG (3): text type 0, channel index 4, timestamp bytes 3..6 (u32 LE),
    # then text (empty if absent).
    channel = Bytes[3, 0, 4, 0x78, 0x56, 0x34, 0x12, 0x68, 0x69]
    h.client(2, channel)
    h.upstream.shift.should eq(channel)
    # OK (0x00): command accepted, not proof of radio delivery.
    h.response(Bytes[0])
    # OK (0x00): command accepted, not proof of radio delivery.
    h.replies[2_i64].should eq([Bytes[0]])
    h.upstream.should be_empty
  end

  it "wraps only the scoped owner's sends with hidden setup and restoration" do
    h = StatefulHarness.new
    # A client changes its virtual scope locally. The physical scope changes
    # only around that client's sends: setup -> send -> restore default.
    # Explicit temporary scope: opcode 54, mode 0, and a synthetic 16-byte key filled with 0x42.
    scope = Bytes.new(18, 0x42)
    scope[0] = 54
    scope[1] = 0
    h.client(1, scope)
    # OK (0x00): command accepted, not proof of radio delivery.
    h.replies[1_i64].shift.should eq(Bytes[0])
    h.upstream.should be_empty
    # Client 2 still uses the physical default and needs no hidden setup.
    h.client(2, stateful_dm)
    h.upstream.shift.should eq(stateful_dm)
    h.response(stateful_sent)
    # Client 1 needs setup; its DM must wait for the setup OK.
    h.client(1, stateful_dm)
    h.upstream.shift.should eq(scope)
    # Queue a competing clock query during setup. It cannot interleave with
    # any part of the compound send, including restoration after SENT.
    # GET_DEVICE_TIME (5): local clock query.
    h.client(2, Bytes[5])
    # OK (0x00): command accepted, not proof of radio delivery.
    h.response(Bytes[0])
    h.upstream.shift.should eq(stateful_dm)
    h.replies[1_i64].should be_empty
    # Only the DM acceptance is visible to client 1, never setup/restore OKs.
    h.response(stateful_sent(2))
    h.replies[1_i64].should eq([stateful_sent(2)])
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 0 with no key: restore the configured default scope.
    h.upstream.shift.should eq(Bytes[54, 0])
    # OK (0x00): command accepted, not proof of radio delivery.
    h.response(Bytes[0])
    # GET_DEVICE_TIME (5): local clock query.
    h.upstream.shift.should eq(Bytes[5])
    h.replies[1_i64].should eq([stateful_sent(2)])
  end

  it "does not send after scope setup rejection or replay after restoration failure" do
    h = StatefulHarness.new
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 1: explicitly unscoped sends.
    h.client(1, Bytes[54, 1])
    h.replies.clear
    h.client(1, stateful_dm)
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 1: explicitly unscoped sends.
    h.upstream.shift.should eq(Bytes[54, 1])
    # Failed setup means the user's send never reached the radio.
    # ERR (0x01), UNSUPPORTED_CMD.
    h.response(Bytes[1, 1])
    h.upstream.should be_empty
    # ERR (0x01), UNSUPPORTED_CMD.
    h.replies[1_i64].should eq([Bytes[1, 1]])
    # The client explicitly tries again. This time setup and the send succeed.
    h.client(1, stateful_dm)
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 1: explicitly unscoped sends.
    h.upstream.shift.should eq(Bytes[54, 1])
    # OK (0x00): command accepted, not proof of radio delivery.
    h.response(Bytes[0])
    h.upstream.shift.should eq(stateful_dm)
    h.response(stateful_sent)
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 0 with no key: restore the configured default scope.
    h.upstream.shift.should eq(Bytes[54, 0])
    # Restoration fails AFTER radio acceptance. The physical scope is now
    # uncertain, so end the epoch; never replay the already accepted message.
    # ERR (0x01), BAD_STATE.
    h.response(Bytes[1, 4])
    h.broker.failed.should be_true
    h.replies[1_i64].last.should eq(stateful_sent)
    h.upstream.should be_empty
  end

  it "restores scope after a failed send before dispatching the next client's send" do
    h = StatefulHarness.new
    # Client 1 selects an explicit synthetic 16-byte temporary scope key. The
    # local OK acknowledges only its virtual setting; no upstream state changes
    # until that client actually sends.
    scope = Bytes.new(18, 0x42_u8)
    scope[0] = 54_u8 # SET_FLOOD_SCOPE_KEY.
    scope[1] = 0_u8  # Explicit-key mode; bytes 2..17 are the key.
    h.client(1, scope)
    h.replies.clear

    # SEND_CHANNEL_TXT_MSG: plain text, channel 0, timestamp zero, empty body.
    first_send = Bytes[3_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8]
    second_send = first_send.dup
    second_send[2] = 1_u8 # Synthetic channel 1 distinguishes client 2's send.
    h.client(1, first_send)
    h.upstream.shift.should eq(scope)
    h.client(2, second_send)

    h.response(Bytes[0_u8]) # Hidden scope-setup OK.
    h.upstream.shift.should eq(first_send)
    # ERR(ILLEGAL_ARG): client 1's radio send failed, so this exact error is
    # visible, but the mux must still restore physical idle scope.
    h.response(Bytes[1_u8, 6_u8])
    h.replies[1_i64].should eq([Bytes[1_u8, 6_u8]])
    h.upstream.shift.should eq(Bytes[54_u8, 0_u8]) # Restore configured default.
    h.response(Bytes[0_u8])                        # Hidden restoration OK.

    # Client 2 retained its default virtual scope. Only after restoration may
    # its queued original command be forwarded, exactly once and unchanged.
    h.upstream.shift.should eq(second_send)
    h.response(Bytes[0_u8]) # Native channel-send acceptance.
    h.replies[2_i64].should eq([Bytes[0_u8]])
    h.upstream.should be_empty
    h.broker.failed.should be_false
  end

  it "retains remote ownership after SENT without blocking local queries or accepting tentative pushes" do
    h = StatefulHarness.new
    # SEND_STATUS_REQ (27): opcode plus 32-byte synthetic peer key; matching uses its first six
    # bytes.
    status = Bytes.new(33, 0x11)
    status[0] = 27
    # STATUS_RESPONSE (0x87): metadata, six 0x11 peer-prefix bytes, and one result byte.
    result = Bytes.new(9, 0x11)
    result[0] = 0x87
    h.client(1, status)
    h.upstream.shift.should eq(status)
    # A result arriving before SENT cannot yet be attributed to this request;
    # the reservation is tentative, so this possibly stale push is discarded.
    h.response(result)
    h.replies[1_i64].should be_empty
    h.response(stateful_sent)
    # SENT releases the ordinary transaction, but not the pending radio lease.
    # A second remote operation could overwrite firmware state and is rejected.
    h.client(2, status)
    h.upstream.should be_empty
    # ERR (0x01), BAD_STATE.
    h.replies[2_i64].shift.should eq(Bytes[1, 4])
    # A local clock query is safe meanwhile. A remote push arriving during it
    # goes to client 1 and must not complete client 2's clock transaction.
    # GET_DEVICE_TIME (5): local clock query.
    h.client(2, Bytes[5])
    # GET_DEVICE_TIME (5): local clock query.
    h.upstream.shift.should eq(Bytes[5])
    h.response(result)
    h.replies[1_i64].last.should eq(result)
    h.replies[2_i64].should be_empty
    # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
    # byte-for-byte.
    h.response(Bytes[9, 1, 2, 3, 4])
    # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
    # byte-for-byte.
    h.replies[2_i64].should eq([Bytes[9, 1, 2, 3, 4]])
  end

  it "separates self telemetry from a remote telemetry lease" do
    h = StatefulHarness.new
    # Remote TELEMETRY (39): three option bytes, then a 32-byte 0x11 peer key starting at offset
    # 4.
    remote = Bytes.new(36, 0x11)
    remote[0] = 39
    h.client(1, remote)
    h.upstream.shift.should eq(remote)
    h.response(stateful_sent)
    # Opcode 39 selects self vs remote telemetry by command length. Both
    # results use 0x8b, so the six-byte peer prefix determines the recipient.
    # Self TELEMETRY request (39): four-byte form with three zero option/reserved bytes, no
    # remote peer.
    h.client(2, Bytes[39, 0, 0, 0])
    # Self TELEMETRY request (39): four-byte form with three zero option/reserved bytes, no
    # remote peer.
    h.upstream.shift.should eq(Bytes[39, 0, 0, 0])
    # Remote TELEMETRY_RESPONSE (0x8b): metadata and six 0x11 peer-prefix bytes.
    result = Bytes.new(8, 0x11)
    result[0] = 0x8b
    h.response(result)
    h.replies[1_i64].last.should eq(result)
    h.replies[2_i64].should be_empty
    # Self TELEMETRY_RESPONSE (0x8b): metadata and six 0x77 key-prefix bytes, matching the
    # harness companion.
    self_result = Bytes.new(8, 0x77)
    self_result[0] = 0x8b
    h.response(self_result)
    h.replies[2_i64].should eq([self_result])
    h.broker.active.should be_nil
  end

  it "protects the next DM ring slot even when another slot is settled" do
    h = StatefulHarness.new
    # Firmware inserts ACK tracking entries in a fixed eight-slot ring. Fill
    # it so the next insertion wraps specifically to the slot for token 1.
    8.times do |i|
      h.client(1, stateful_dm)
      h.upstream.shift.should eq(stateful_dm)
      h.response(stateful_sent((i + 1).to_u8))
    end
    # Settling token 4 leaves a hole, but not at the next insertion position.
    # SEND_CONFIRMED (0x82): four-byte ACK token, then round-trip milliseconds (u32 LE); real
    # delivery confirmation.
    h.response(Bytes[0x82, 4, 0xbb, 0xcc, 0xdd, 1, 0, 0, 0])
    retry = stateful_dm.dup
    # The client increments the SEND_TXT_MSG attempt byte while retaining its
    # timestamp and body. A retry still cannot overwrite the next physical slot.
    retry[2] = 3_u8
    h.client(2, retry)
    h.upstream.should be_empty
    # ERR (0x01), BAD_STATE.
    h.replies[2_i64].last.should eq(Bytes[1, 4])
    # Only settling token 1 makes the next physical write safe.
    # SEND_CONFIRMED (0x82): four-byte ACK token, then round-trip milliseconds (u32 LE); real
    # delivery confirmation.
    h.response(Bytes[0x82, 1, 0xbb, 0xcc, 0xdd, 1, 0, 0, 0])
    h.client(2, retry)
    h.upstream.shift.should eq(retry)
  end

  it "protects signing chunks and classifies a departed owner's in-flight reply" do
    h = StatefulHarness.new
    # Signing is a multi-command session: accepted START reserves ownership,
    # DATA consumes the advertised byte budget, and FINISH returns a signature.
    # SIGN_START (33/0x21): start the shared signing session.
    h.client(1, Bytes[33])
    # SIGN_START (33/0x21): start the shared signing session.
    h.upstream.shift.should eq(Bytes[33])
    # SIGN_START response (0x13): reserved 0, byte limit 3 (u32 LE).
    h.response(Bytes[0x13, 0, 3, 0, 0, 0])
    # SIGN_START (33/0x21): start the shared signing session.
    h.client(2, Bytes[33])
    # ERR (0x01), BAD_STATE.
    h.replies[2_i64].last.should eq(Bytes[1, 4])
    # SIGN_DATA (34/0x22), followed by 2 literal data byte(s); only accepted data counts against
    # the signing budget.
    h.client(1, Bytes[34, 1, 2])
    # SIGN_DATA (34/0x22), followed by 2 literal data byte(s); only accepted data counts against
    # the signing budget.
    h.upstream.shift.should eq(Bytes[34, 1, 2])
    # OK (0x00): command accepted, not proof of radio delivery.
    h.response(Bytes[0])
    # Two bytes are already accepted against a three-byte budget. Another
    # two-byte chunk must be refused locally, without corrupting signing state.
    # SIGN_DATA (34/0x22), followed by 2 literal data byte(s); only accepted data counts against
    # the signing budget.
    h.client(1, Bytes[34, 3, 4])
    # ERR (0x01), TABLE_FULL.
    h.replies[1_i64].last.should eq(Bytes[1, 3])
    # SIGN_FINISH (35/0x23): finish the current signing session and await SIGNATURE.
    h.client(1, Bytes[35])
    # SIGN_FINISH (35/0x23): finish the current signing session and await SIGNATURE.
    h.upstream.shift.should eq(Bytes[35])
    # The owner leaves with FINISH in flight. Its eventual signature must be
    # consumed as that transaction's reply, not mistaken for an unowned frame.
    h.broker.client_closed(1_i64, h.now)
    # SIGNATURE response: opcode 0x14 plus a 64-byte dummy signature; no cryptography is
    # performed.
    signature = Bytes.new(65, 0)
    signature[0] = 0x14
    h.response(signature)
    h.broker.failed.should be_false
    # Client 2 cannot continue the departed client's signing stream; it must
    # start a new one. A classified orphan reply must not kill the whole epoch.
    # SIGN_DATA (34/0x22), followed by 1 literal data byte(s); only accepted data counts against
    # the signing budget.
    h.client(2, Bytes[34, 1])
    # ERR (0x01), BAD_STATE.
    h.replies[2_i64].last.should eq(Bytes[1, 4])
    # SIGN_START (33/0x21): start the shared signing session.
    h.client(2, Bytes[33])
    # SIGN_START (33/0x21): start the shared signing session.
    h.upstream.shift.should eq(Bytes[33])
  end
end
