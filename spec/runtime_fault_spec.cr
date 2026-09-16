require "./spec_helper"
require "../src/meshcore_tcp_mux/runtime"
require "./support/runtime_companion"

private def fault_config : MeshCoreTCPMux::Config
  config = MeshCoreTCPMux::Config.new
  config.listen_host = "127.0.0.1"
  listener = TCPServer.new("127.0.0.1", 0)
  config.listen_multi_client_port = listener.local_address.port
  listener.close
  config.response_timeout = 100.milliseconds
  config.contacts_timeout = 300.milliseconds
  config.frame_timeout = 20.milliseconds
  config.write_timeout = 500.milliseconds
  config.startup_timeout = 1.second
  config.poll_interval = 10.seconds
  config
end

private def start_fault_runtime(companion : SpecSupport::RuntimeCompanion) : {MeshCoreTCPMux::Runtime, MeshCoreTCPMux::Config, Channel(Nil)}
  config = fault_config
  runtime = MeshCoreTCPMux::Runtime.new("127.0.0.1", companion.port, config)
  done = Channel(Nil).new(1)
  spawn do
    runtime.run
    done.send(nil)
  end
  {runtime, config, done}
end

private def await_epoch_ready(companion : SpecSupport::RuntimeCompanion, expected : Int32) : Nil
  select
  when epoch = companion.ready.receive
    epoch.should eq(expected)
  when timeout(3.seconds)
    fail "epoch #{expected} did not become ready"
  end
  5.times { Fiber.yield }
end

private def connect_fault_client(config : MeshCoreTCPMux::Config) : TCPSocket
  socket = TCPSocket.new("127.0.0.1", config.listen_multi_client_port)
  socket.read_timeout = 2.seconds
  socket
end

private def send_command(socket : TCPSocket, payload : Bytes) : Nil
  socket.write(MeshCoreTCPMux::FrameCodec.encode(payload, MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER))
end

private def next_command(companion : SpecSupport::RuntimeCompanion, opcode : UInt8, epoch : Int32? = nil) : SpecSupport::RuntimeCompanion::Command
  select
  when command = companion.commands.receive
    command.payload[0].should eq(opcode)
    command.epoch.should eq(epoch) if epoch
    command
  when timeout(2.seconds)
    fail "upstream command #{opcode} was not observed"
  end
end

private def admit_client(companion : SpecSupport::RuntimeCompanion, config : MeshCoreTCPMux::Config, epoch : Int32) : TCPSocket
  socket = connect_fault_client(config)
  # Admission supplies MSG_WAITING, but custody remains upstream until the
  # client explicitly asks to synchronize its empty local inbox.
  read_payload(socket, 0x83_u8).should eq(Bytes[0x83_u8])
  send_command(socket, Bytes[10_u8]) # SYNC_NEXT_MESSAGE.
  next_command(companion, 10_u8, epoch)
  # NO_MORE_MESSAGES (0x0a): inbox empty.
  companion.reply(Bytes[10_u8])
  read_payload(socket, 10_u8).should eq(Bytes[10_u8])
  socket
end

private def expect_closed(socket : TCPSocket) : Nil
  closed = Channel(Bool).new(1)
  spawn do
    begin
      closed.send(socket.read_byte.nil?)
    rescue IO::Error
      closed.send(true)
    end
  end
  select
  when result = closed.receive
    result.should be_true
  when timeout(2.seconds)
    fail "socket remained open"
  end
end

private class FaultFrameReader
  # Retains one downstream TCP decoder and every completed frame for the life of
  # a test socket. Tests consume the stream in order, so a coalesced duplicate or
  # unexpected ordinary response cannot disappear while searching by opcode.
  @decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER)
  @frames = Deque(Bytes).new

  def feed(bytes : Bytes, now : Time::Span) : Nil
    @decoder.feed(bytes, now) { |payload| @frames << payload }
  end

  def next_payload(socket : TCPSocket) : Bytes
    loop do
      if payload = @frames.shift?
        return payload
      end
      # Socket read scratch space; capacity is arbitrary and is not a protocol field.
      buffer = Bytes.new(256)
      count = socket.read(buffer)
      raise IO::EOFError.new("socket closed before response") if count == 0
      feed(buffer[0, count], MeshCoreTCPMux::Clock.now)
    end
  end

  def queued_payloads : Array(Bytes)
    @frames.to_a
  end
end

FAULT_FRAME_READERS = Hash(TCPSocket, FaultFrameReader).new

private def read_payload(socket : TCPSocket, opcode : UInt8) : Bytes
  payload = FAULT_FRAME_READERS.put_if_absent(socket) { FaultFrameReader.new }.next_payload(socket)
  payload[0].should eq(opcode), "unexpected response before opcode #{opcode}: #{payload[0]}"
  payload
end

private def orphan_message : Bytes
  # Minimal legacy CONTACT_MESSAGE: opcode 7, six-byte sender prefix, path/type bytes, u32
  # timestamp; no text body.
  Bytes.new(13, 0_u8).tap { |payload| payload[0] = 7_u8 }
end

private def fault_status_request(peer_seed = 1_u8) : Bytes
  # SEND_STATUS_REQ (27): opcode plus a synthetic 32-byte peer key. Firmware
  # correlates the eventual result by only the first six peer bytes.
  Bytes.new(33, 0_u8).tap do |payload|
    payload[0] = 27_u8
    6.times { |index| payload[1 + index] = peer_seed &+ index.to_u8 }
  end
end

private def fault_status_result(peer_seed = 1_u8) : Bytes
  # STATUS_RESPONSE (0x87): reserved metadata byte, the six-byte peer prefix,
  # and one synthetic status byte. It has no request or TCP-epoch identifier.
  Bytes[0x87_u8, 0_u8] + Bytes.new(6) { |index| peer_seed &+ index.to_u8 } + Bytes[0_u8]
end

private def fault_sent(timeout_ms = 30_000_u32) : Bytes
  # SENT (0x06): routing type 1, synthetic u32 token 0x04030201, and the
  # firmware's suggested radio timeout as a four-byte little-endian integer.
  Bytes[6_u8, 1_u8, 1_u8, 2_u8, 3_u8, 4_u8,
    (timeout_ms & 0xff).to_u8, ((timeout_ms >> 8) & 0xff).to_u8,
    ((timeout_ms >> 16) & 0xff).to_u8, ((timeout_ms >> 24) & 0xff).to_u8]
end

private class ListenerFaultRuntime < MeshCoreTCPMux::Runtime
  # Supplies a real loopback listener whose lifetime the spec controls. Closing
  # it without Runtime#stop injects accept failure without exhausting descriptors.
  def initialize(@listener : TCPServer, port : Int32, config : MeshCoreTCPMux::Config) : Nil
    super("127.0.0.1", port, config)
  end

  protected def create_listener(port : Int32) : TCPServer
    @listener
  end
end

describe MeshCoreTCPMux::Runtime, "fault and epoch boundaries" do
  # End-to-end fault tests use only loopback sockets and a scripted companion.
  # Each upstream connection is an epoch; uncertainty closes its downstream clients
  # and must never replay their old commands. Helpers hide framing, not responses:
  # the test explicitly tells the companion when to reply, drop, or close.

  it "preserves coalesced, duplicate, and split frames in the test reader" do
    first = MeshCoreTCPMux::FrameCodec.encode(
      Bytes[9_u8, 1_u8, 0_u8, 0_u8, 0_u8], # CURRENT_TIME (0x09), timestamp 1 (u32 LE).
      MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER
    )
    second = MeshCoreTCPMux::FrameCodec.encode(
      Bytes[9_u8, 2_u8, 0_u8, 0_u8, 0_u8], # Duplicate CURRENT_TIME opcode, timestamp 2 (u32 LE).
      MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER
    )
    third = MeshCoreTCPMux::FrameCodec.encode(
      Bytes[13_u8, 3_u8], # Synthetic different opcode after the duplicates.
      MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER
    )
    wire = first + second + third

    (0..wire.size).each do |split|
      reader = FaultFrameReader.new
      reader.feed(wire[0, split], Time::Span.zero)
      reader.feed(wire[split, wire.size - split], 1.millisecond)
      reader.queued_payloads.should eq([
        Bytes[9_u8, 1_u8, 0_u8, 0_u8, 0_u8],
        Bytes[9_u8, 2_u8, 0_u8, 0_u8, 0_u8],
        Bytes[13_u8, 3_u8],
      ])
    end
  end

  it "retains detached dedicated history and replaces the old dedicated socket" do
    companion = SpecSupport::RuntimeCompanion.new
    config = fault_config
    dedicated_listener = TCPServer.new("127.0.0.1", 0)
    dedicated_port = dedicated_listener.local_address.port
    dedicated_listener.close
    config.listen_dedicated_client_ports << dedicated_port
    runtime = MeshCoreTCPMux::Runtime.new("127.0.0.1", companion.port, config)
    runtime_done = Channel(Nil).new(1)
    spawn do
      runtime.run
      runtime_done.send(nil)
    end
    await_epoch_ready(companion, 1)

    detached = TCPSocket.new("127.0.0.1", dedicated_port)
    detached.read_timeout = 2.seconds
    read_payload(detached, 0x83_u8).should eq(Bytes[0x83_u8]) # MSG_WAITING admission hint.
    detached.close
    sleep 100.milliseconds

    puller = connect_fault_client(config)
    read_payload(puller, 0x83_u8).should eq(Bytes[0x83_u8]) # MSG_WAITING admission hint.
    send_command(puller, Bytes[10_u8])                      # SYNC_NEXT_MESSAGE.
    next_command(companion, 10_u8, 1)
    item = orphan_message
    companion.reply(item)
    next_command(companion, 10_u8, 1)
    companion.reply(Bytes[10_u8]) # NO_MORE_MESSAGES ends the authorized cycle.
    read_payload(puller, 7_u8).should eq(item)

    attached = TCPSocket.new("127.0.0.1", dedicated_port)
    attached.read_timeout = 2.seconds
    read_payload(attached, 0x83_u8).should eq(Bytes[0x83_u8])
    send_command(attached, Bytes[10_u8]) # Served from the retained slot queue.
    read_payload(attached, 7_u8).should eq(item)

    replacement = TCPSocket.new("127.0.0.1", dedicated_port)
    replacement.read_timeout = 2.seconds
    read_payload(replacement, 0x83_u8).should eq(Bytes[0x83_u8])
    expect_closed(attached)
  ensure
    detached.try &.close
    puller.try &.close
    attached.try &.close
    replacement.try &.close
    runtime.try &.stop
    runtime_done.try &.receive
    companion.try &.stop
  end

  it "releases an already-bound multi-client listener when a dedicated bind fails" do
    config = fault_config
    occupied = TCPServer.new("127.0.0.1", 0)
    config.listen_dedicated_client_ports << occupied.local_address.port
    runtime = MeshCoreTCPMux::Runtime.new("127.0.0.1", occupied.local_address.port, config)

    expect_raises(Socket::BindError) { runtime.run }
    # Rebinding proves startup closed the earlier multi-client listener instead
    # of exposing a partial listener set after the dedicated-port failure.
    rebound = TCPServer.new("127.0.0.1", config.listen_multi_client_port)
  ensure
    rebound.try &.close
    occupied.try &.close
  end

  it "times out A without dispatching queued B or replaying it after reconnect" do
    companion = SpecSupport::RuntimeCompanion.new
    runtime, config, runtime_done = start_fault_runtime(companion)
    await_epoch_ready(companion, 1)
    a = admit_client(companion, config, 1)
    b = admit_client(companion, config, 1)

    # GET_DEVICE_TIME (5): local clock query.
    send_command(a, Bytes[5_u8])
    next_command(companion, 5_u8, 1)
    companion.drop
    # GET_BATT_AND_STORAGE (20): queued query must not be replayed after reconnect.
    send_command(b, Bytes[20_u8])

    expect_closed(a)
    expect_closed(b)
    await_epoch_ready(companion, 2)
    select
    when command = companion.commands.receive
      fail "old command replayed in epoch 2: #{command.payload[0]}"
    when timeout(200.milliseconds)
    end
  ensure
    a.try &.close
    b.try &.close
    runtime.try &.stop
    runtime_done.try &.receive
    companion.try &.stop
  end

  [:timeout, :disconnect].each do |fault|
    it "never replays a transmit command after upstream #{fault} following its write" do
      companion = SpecSupport::RuntimeCompanion.new
      runtime, config, runtime_done = start_fault_runtime(companion)
      await_epoch_ready(companion, 1)
      sender = admit_client(companion, config, 1)

      # SEND_TXT_MSG (2): plain type 0, attempt 3, timestamp 0x12345678
      # (u32 little-endian), synthetic six-byte destination 01..06, and body
      # "x". The fake observes the complete command, then either omits SENT
      # while keeping TCP open or closes immediately. Both make execution unknown.
      dm = Bytes[2_u8, 0_u8, 3_u8, 0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8,
        1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8, 'x'.ord.to_u8]
      send_command(sender, dm)
      written = next_command(companion, 2_u8, 1)
      written.payload.should eq(dm)
      if fault == :disconnect
        companion.disconnect
      else
        companion.drop
      end
      expect_closed(sender)

      await_epoch_ready(companion, 2)
      select
      when command = companion.commands.receive
        fail "uncertain transmit replayed in epoch 2: #{command.payload[0]}"
      when timeout(200.milliseconds)
      end
    ensure
      sender.try &.close
      runtime.try &.stop
      runtime_done.try &.receive
      companion.try &.stop
    end
  end

  it "retains an accepted remote lease across a same-identity upstream reconnect" do
    companion = SpecSupport::RuntimeCompanion.new
    runtime, config, runtime_done = start_fault_runtime(companion)
    await_epoch_ready(companion, 1)
    original = admit_client(companion, config, 1)
    status = fault_status_request

    send_command(original, status)
    next_command(companion, 27_u8, 1).payload.should eq(status) # SEND_STATUS_REQ.
    # SENT accepts the radio request. The pending radio result remains possible
    # in the fake object independently of later TCP transport replacement.
    companion.reply(fault_sent)
    read_payload(original, 6_u8).should eq(fault_sent)
    send_command(original, Bytes[5_u8]) # GET_DEVICE_TIME gives the fake a deterministic close point.
    next_command(companion, 5_u8, 1)
    companion.disconnect
    expect_closed(original)

    await_epoch_ready(companion, 2)
    replacement = admit_client(companion, config, 2)
    send_command(replacement, status)
    # ERR (0x01), BAD_STATE (0x04): the old same-peer lease still owns the
    # companion's untagged result slot and the command never goes upstream.
    read_payload(replacement, 1_u8).should eq(Bytes[1_u8, 4_u8])
    send_command(replacement, fault_status_request(0x21_u8))
    read_payload(replacement, 1_u8).should eq(Bytes[1_u8, 4_u8])

    # The old STATUS_RESPONSE arrives on the replacement TCP connection. It
    # clears the ownerless old lease but must not be delivered to replacement.
    companion.push(fault_status_result)
    send_command(replacement, Bytes[5_u8]) # GET_DEVICE_TIME remains safe during radio uncertainty.
    next_command(companion, 5_u8, 2)
    current_time = Bytes[9_u8, 1_u8, 0_u8, 0_u8, 0_u8] # CURRENT_TIME, timestamp 1 (u32 LE).
    companion.reply(current_time)
    read_payload(replacement, 9_u8).should eq(current_time)

    # Once the old result has settled its lease, a new request can be accepted.
    send_command(replacement, status)
    next_command(companion, 27_u8, 2).payload.should eq(status)
    companion.reply(Bytes[1_u8, 4_u8]) # ERR(BAD_STATE): finish the synthetic request.
    read_payload(replacement, 1_u8).should eq(Bytes[1_u8, 4_u8])
  ensure
    original.try &.close
    replacement.try &.close
    runtime.try &.stop
    runtime_done.try &.receive
    companion.try &.stop
  end

  it "quarantines radio work whose acceptance was unobserved across reconnect" do
    companion = SpecSupport::RuntimeCompanion.new
    runtime, config, runtime_done = start_fault_runtime(companion)
    config.radio_uncertainty_timeout = 1.second
    await_epoch_ready(companion, 1)
    original = admit_client(companion, config, 1)
    status = fault_status_request

    send_command(original, status)
    next_command(companion, 27_u8, 1)
    # The fake may have executed SEND_STATUS_REQ, but closes before returning
    # SENT. Runtime must neither replay it nor call the radio state empty.
    companion.disconnect
    expect_closed(original)

    await_epoch_ready(companion, 2)
    replacement = admit_client(companion, config, 2)
    send_command(replacement, status)
    read_payload(replacement, 1_u8).should eq(Bytes[1_u8, 4_u8]) # ERR(BAD_STATE).

    # SEND_TXT_MSG (2): plain DM type 0, attempt 1, timestamp 1 (u32 LE),
    # synthetic six-byte peer key, and one-byte body. The global quarantine
    # protects firmware acknowledgement state whose acceptance is also unknown.
    dm = Bytes[2_u8, 0_u8, 1_u8, 1_u8, 0_u8, 0_u8, 0_u8,
      1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8, 'x'.ord.to_u8]
    send_command(replacement, dm)
    read_payload(replacement, 1_u8).should eq(Bytes[1_u8, 4_u8]) # ERR(BAD_STATE).

    companion.push(fault_status_result)
    send_command(replacement, Bytes[5_u8]) # GET_DEVICE_TIME remains available.
    next_command(companion, 5_u8, 2)
    current_time = Bytes[9_u8, 2_u8, 0_u8, 0_u8, 0_u8] # CURRENT_TIME, timestamp 2 (u32 LE).
    companion.reply(current_time)
    read_payload(replacement, 9_u8).should eq(current_time)
  ensure
    original.try &.close
    replacement.try &.close
    runtime.try &.stop
    runtime_done.try &.receive
    companion.try &.stop
  end

  it "clears retained radio reservations after a proven identity change" do
    # Different synthetic key markers model replacement by another companion.
    # A same-key reboot cannot be proven by this legacy protocol and therefore
    # deliberately follows the conservative retention behavior tested above.
    companion = SpecSupport::RuntimeCompanion.new([0xa5_u8, 0xb6_u8])
    runtime, config, runtime_done = start_fault_runtime(companion)
    await_epoch_ready(companion, 1)
    original = admit_client(companion, config, 1)
    status = fault_status_request

    send_command(original, status)
    next_command(companion, 27_u8, 1)
    companion.reply(fault_sent)
    read_payload(original, 6_u8).should eq(fault_sent) # SENT: old identity accepted radio work.
    send_command(original, Bytes[5_u8])                # GET_DEVICE_TIME supplies a deterministic close point.
    next_command(companion, 5_u8, 1)
    companion.disconnect
    expect_closed(original)

    await_epoch_ready(companion, 2)
    replacement = admit_client(companion, config, 2)
    send_command(replacement, status)
    # The different validated public key proves the retained lease cannot
    # belong to this node, so SEND_STATUS_REQ reaches the new companion.
    next_command(companion, 27_u8, 2).payload.should eq(status)
    companion.reply(Bytes[1_u8, 4_u8]) # ERR(BAD_STATE): finish the synthetic request.
    read_payload(replacement, 1_u8).should eq(Bytes[1_u8, 4_u8])
  ensure
    original.try &.close
    replacement.try &.close
    runtime.try &.stop
    runtime_done.try &.receive
    companion.try &.stop
  end

  it "rejects ambiguous and negative raw paths locally and keeps both clients in the same epoch" do
    companion = SpecSupport::RuntimeCompanion.new
    runtime, config, runtime_done = start_fault_runtime(companion)
    await_epoch_ready(companion, 1)
    healthy = admit_client(companion, config, 1)
    malformed = admit_client(companion, config, 1)

    fixtures = [
      # SEND_RAW_DATA (25): byte 0x40 makes the command parser skip 64 literal
      # path bytes, but packet routing decodes it as zero two-byte hashes.
      Bytes.new(70, 0_u8).tap { |payload| payload[0] = 25_u8; payload[1] = 0x40_u8 },
      # SEND_RAW_DATA (25): -1 encoded as 0xff in the signed native field,
      # followed by the minimum four synthetic data bytes.
      Bytes[25_u8, 0xff_u8, 0_u8, 0_u8, 0_u8, 0_u8],
    ]
    fixtures.each do |payload|
      send_command(malformed, payload)
      # ERR (1), ILLEGAL_ARG (6): rejection belongs only to the malformed sender.
      read_payload(malformed, 1_u8).should eq(Bytes[1_u8, 6_u8])
    end
    [healthy, malformed].each do |client|
      send_command(client, Bytes[5_u8]) # GET_DEVICE_TIME: verify continued progress.
      # The next physical command is the query in epoch 1, never SEND_RAW_DATA.
      next_command(companion, 5_u8, 1)
      # CURRENT_TIME (9), synthetic timestamp 1 (u32 LE).
      reply = Bytes[9_u8, 1_u8, 0_u8, 0_u8, 0_u8]
      companion.reply(reply)
      read_payload(client, 9_u8).should eq(reply)
    end
  ensure
    healthy.try &.close
    malformed.try &.close
    runtime.try &.stop
    runtime_done.try &.receive
    companion.try &.stop
  end

  it "propagates acceptor failure after closing clients and joining the runtime" do
    companion = SpecSupport::RuntimeCompanion.new
    config = fault_config
    listener = TCPServer.new("127.0.0.1", 0)
    config.listen_multi_client_port = listener.local_address.port
    runtime = ListenerFaultRuntime.new(listener, companion.port, config)
    outcome = Channel(Exception?).new(1)
    spawn do
      begin
        runtime.run
        outcome.send(nil)
      rescue ex
        outcome.send(ex)
      end
    end
    await_epoch_ready(companion, 1)
    client = admit_client(companion, config, 1)
    listener.close # Unexpected accept failure, not a normal stop request.
    select
    when error = outcome.receive
      # BinaryEntrypoint translates this propagated exception to exit status 1.
      error.should be_a(IO::Error)
    when timeout(2.seconds)
      fail "runtime did not exit after acceptor failure"
    end
    expect_closed(client)
  ensure
    client.try &.close
    listener.try &.close
    runtime.try &.stop
    companion.try &.stop
  end

  it "ends epochs on malformed and truncated upstream frames" do
    companion = SpecSupport::RuntimeCompanion.new
    runtime, config, runtime_done = start_fault_runtime(companion)
    await_epoch_ready(companion, 1)
    first = admit_client(companion, config, 1)
    # GET_DEVICE_TIME (5): local clock query.
    send_command(first, Bytes[5_u8])
    next_command(companion, 5_u8, 1)
    # Wrong-direction upstream frame: request marker '<', length 1, response opcode 9.
    companion.raw_and_close(Bytes[0x3c_u8, 1_u8, 0_u8, 9_u8])
    expect_closed(first)

    await_epoch_ready(companion, 2)
    second = admit_client(companion, config, 2)
    # GET_DEVICE_TIME (5): local clock query.
    send_command(second, Bytes[5_u8])
    next_command(companion, 5_u8, 2)
    # Truncated response: '>' and length 5, but only CURRENT_TIME opcode 9 precedes EOF.
    companion.raw_and_close(Bytes[0x3e_u8, 5_u8, 0_u8, 9_u8])
    expect_closed(second)
    await_epoch_ready(companion, 3)
  ensure
    first.try &.close
    second.try &.close
    runtime.try &.stop
    runtime_done.try &.receive
    companion.try &.stop
  end

  it "closes slow and malformed clients without disturbing a healthy client" do
    companion = SpecSupport::RuntimeCompanion.new
    runtime, config, runtime_done = start_fault_runtime(companion)
    await_epoch_ready(companion, 1)
    healthy = admit_client(companion, config, 1)

    slow = admit_client(companion, config, 1)
    # Only the client '<' marker is written; missing length/body must hit the partial-frame
    # deadline.
    slow.write(Bytes[MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER])
    expect_closed(slow)

    malformed = admit_client(companion, config, 1)
    # GET_DEVICE_TIME (5): local clock query.
    malformed.write(MeshCoreTCPMux::FrameCodec.encode(Bytes[5_u8], MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER))
    expect_closed(malformed)

    # GET_DEVICE_TIME (5): local clock query.
    send_command(healthy, Bytes[5_u8])
    next_command(companion, 5_u8, 1)
    # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
    # byte-for-byte.
    expected = Bytes[9_u8, 0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8]
    companion.reply(expected)
    read_payload(healthy, 9_u8).should eq(expected)
  ensure
    healthy.try &.close
    slow.try &.close
    malformed.try &.close
    runtime.try &.stop
    runtime_done.try &.receive
    companion.try &.stop
  end

  it "carries one orphan inbox item across a matching-identity epoch" do
    # The synthetic public key is identical in both epochs: the orphan belongs
    # to this same companion and can safely be offered after reconnect.
    companion = SpecSupport::RuntimeCompanion.new([0xa5_u8, 0xa5_u8])
    runtime, config, runtime_done = start_fault_runtime(companion)
    config.response_timeout = 1.second
    await_epoch_ready(companion, 1)
    departing = admit_client(companion, config, 1)

    # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
    companion.push(Bytes[0x83_u8])
    send_command(departing, Bytes[10_u8]) # SYNC_NEXT_MESSAGE authorizes custody transfer.
    next_command(companion, 10_u8, 1)
    read_payload(departing, 0x83_u8).should eq(Bytes[0x83_u8])
    # A wrong-direction envelope deterministically closes and removes the final
    # session before the already-issued physical result is returned.
    departing.write(MeshCoreTCPMux::FrameCodec.encode(
      Bytes[5_u8], MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER
    )) # GET_DEVICE_TIME payload in an invalid downstream envelope.
    expect_closed(departing)
    item = orphan_message
    companion.raw_and_close(MeshCoreTCPMux::FrameCodec.encode(item, MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER))

    await_epoch_ready(companion, 2)
    arriving = connect_fault_client(config)
    # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
    read_payload(arriving, 0x83_u8).should eq(Bytes[0x83_u8])
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    send_command(arriving, Bytes[10_u8])
    read_payload(arriving, 7_u8).should eq(item)
  ensure
    departing.try &.close
    arriving.try &.close
    runtime.try &.stop
    runtime_done.try &.receive
    companion.try &.stop
  end

  it "discards an orphan inbox item when the upstream identity changes" do
    # Changing the synthetic key's first byte models a different companion.
    # An old companion's orphan must never leak to clients of the new device.
    companion = SpecSupport::RuntimeCompanion.new([0xa5_u8, 0xb6_u8])
    runtime, config, runtime_done = start_fault_runtime(companion)
    config.response_timeout = 1.second
    await_epoch_ready(companion, 1)
    departing = admit_client(companion, config, 1)

    # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
    companion.push(Bytes[0x83_u8])
    send_command(departing, Bytes[10_u8]) # SYNC_NEXT_MESSAGE authorizes custody transfer.
    next_command(companion, 10_u8, 1)
    read_payload(departing, 0x83_u8).should eq(Bytes[0x83_u8])
    departing.write(MeshCoreTCPMux::FrameCodec.encode(
      Bytes[5_u8], MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER
    )) # Wrong-direction GET_DEVICE_TIME envelope closes the final client.
    expect_closed(departing)
    companion.raw_and_close(
      MeshCoreTCPMux::FrameCodec.encode(orphan_message, MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER)
    )

    await_epoch_ready(companion, 2)
    arriving = admit_client(companion, config, 2)
    # SYNC_NEXT_MESSAGE (10): pop the next inbox item.
    send_command(arriving, Bytes[10_u8])
    next_command(companion, 10_u8, 2)
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    companion.reply(Bytes[10_u8])
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    read_payload(arriving, 10_u8).should eq(Bytes[10_u8])
  ensure
    departing.try &.close
    arriving.try &.close
    runtime.try &.stop
    runtime_done.try &.receive
    companion.try &.stop
  end
end
