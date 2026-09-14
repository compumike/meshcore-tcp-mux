require "./spec_helper"
require "../src/meshcore_tcp_mux/runtime"
require "./support/runtime_companion"

private def fault_config : MeshCoreTCPMux::Config
  config = MeshCoreTCPMux::Config.new
  config.listen_host = "127.0.0.1"
  listener = TCPServer.new("127.0.0.1", 0)
  config.listen_port = listener.local_address.port
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
  socket = TCPSocket.new("127.0.0.1", config.listen_port)
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
  next_command(companion, 10_u8, epoch)
  # NO_MORE_MESSAGES (0x0a): inbox empty.
  companion.reply(Bytes[10_u8])
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

private def read_payload(socket : TCPSocket, opcode : UInt8) : Bytes
  decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER)
  # Socket read scratch space; capacity is arbitrary and is not a protocol field.
  buffer = Bytes.new(256)
  loop do
    count = socket.read(buffer)
    raise IO::EOFError.new("socket closed before response") if count == 0
    found : Bytes? = nil
    decoder.feed(buffer[0, count], MeshCoreTCPMux::Clock.now) do |payload|
      found = payload if payload[0] == opcode
    end
    return found.not_nil! if found
  end
end

private def orphan_message : Bytes
  # Minimal legacy CONTACT_MESSAGE: opcode 7, six-byte sender prefix, path/type bytes, u32
  # timestamp; no text body.
  Bytes.new(13, 0_u8).tap { |payload| payload[0] = 7_u8 }
end

private class ListenerFaultRuntime < MeshCoreTCPMux::Runtime
  # Supplies a real loopback listener whose lifetime the spec controls. Closing
  # it without Runtime#stop injects accept failure without exhausting descriptors.
  def initialize(@listener : TCPServer, port : Int32, config : MeshCoreTCPMux::Config) : Nil
    super("127.0.0.1", port, config)
  end

  protected def create_listener : TCPServer
    @listener
  end
end

describe MeshCoreTCPMux::Runtime, "fault and epoch boundaries" do
  # End-to-end fault tests use only loopback sockets and a scripted companion.
  # Each upstream connection is an epoch; uncertainty closes its downstream clients
  # and must never replay their old commands. Helpers hide framing, not responses:
  # the test explicitly tells the companion when to reply, drop, or close.

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

  it "rejects a negative raw path locally and keeps both clients in the same epoch" do
    companion = SpecSupport::RuntimeCompanion.new
    runtime, config, runtime_done = start_fault_runtime(companion)
    await_epoch_ready(companion, 1)
    healthy = admit_client(companion, config, 1)
    malformed = admit_client(companion, config, 1)

    # SEND_RAW_DATA (25): -1 encoded as 0xff in its signed path field, then
    # four synthetic bytes. The envelope is valid; the command shape is not.
    send_command(malformed, Bytes[25_u8, 0xff_u8, 0_u8, 0_u8, 0_u8, 0_u8])
    # ERR (1), ILLEGAL_ARG (6): rejection belongs only to the malformed sender.
    read_payload(malformed, 1_u8).should eq(Bytes[1_u8, 6_u8])
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
    config.listen_port = listener.local_address.port
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
    await_epoch_ready(companion, 1)
    departing = admit_client(companion, config, 1)

    # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
    companion.push(Bytes[0x83_u8])
    next_command(companion, 10_u8, 1)
    departing.close
    # There is intentionally no public runtime hook for client-close handling;
    # allow its already-readable EOF to reach the broker before the pop result.
    sleep 20.milliseconds
    item = orphan_message
    companion.raw_and_close(MeshCoreTCPMux::FrameCodec.encode(item, MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER))

    await_epoch_ready(companion, 2)
    arriving = admit_client(companion, config, 2)
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
    await_epoch_ready(companion, 1)
    departing = admit_client(companion, config, 1)

    # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
    companion.push(Bytes[0x83_u8])
    next_command(companion, 10_u8, 1)
    departing.close
    sleep 20.milliseconds
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
