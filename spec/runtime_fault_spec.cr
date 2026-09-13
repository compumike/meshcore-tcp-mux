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
  Bytes.new(13, 0_u8).tap { |payload| payload[0] = 7_u8 }
end

describe MeshCoreTCPMux::Runtime, "fault and epoch boundaries" do
  it "times out A without dispatching queued B or replaying it after reconnect" do
    companion = SpecSupport::RuntimeCompanion.new
    runtime, config, runtime_done = start_fault_runtime(companion)
    await_epoch_ready(companion, 1)
    a = admit_client(companion, config, 1)
    b = admit_client(companion, config, 1)

    send_command(a, Bytes[5_u8])
    next_command(companion, 5_u8, 1)
    companion.drop
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

  it "ends epochs on malformed and truncated upstream frames" do
    companion = SpecSupport::RuntimeCompanion.new
    runtime, config, runtime_done = start_fault_runtime(companion)
    await_epoch_ready(companion, 1)
    first = admit_client(companion, config, 1)
    send_command(first, Bytes[5_u8])
    next_command(companion, 5_u8, 1)
    companion.raw_and_close(Bytes[0x3c_u8, 1_u8, 0_u8, 9_u8])
    expect_closed(first)

    await_epoch_ready(companion, 2)
    second = admit_client(companion, config, 2)
    send_command(second, Bytes[5_u8])
    next_command(companion, 5_u8, 2)
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
    slow.write(Bytes[MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER])
    expect_closed(slow)

    malformed = admit_client(companion, config, 1)
    malformed.write(MeshCoreTCPMux::FrameCodec.encode(Bytes[5_u8], MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER))
    expect_closed(malformed)

    send_command(healthy, Bytes[5_u8])
    next_command(companion, 5_u8, 1)
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
    companion = SpecSupport::RuntimeCompanion.new([0xa5_u8, 0xa5_u8])
    runtime, config, runtime_done = start_fault_runtime(companion)
    await_epoch_ready(companion, 1)
    departing = admit_client(companion, config, 1)

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
    read_payload(arriving, 0x83_u8).should eq(Bytes[0x83_u8])
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
    companion = SpecSupport::RuntimeCompanion.new([0xa5_u8, 0xb6_u8])
    runtime, config, runtime_done = start_fault_runtime(companion)
    await_epoch_ready(companion, 1)
    departing = admit_client(companion, config, 1)

    companion.push(Bytes[0x83_u8])
    next_command(companion, 10_u8, 1)
    departing.close
    sleep 20.milliseconds
    companion.raw_and_close(
      MeshCoreTCPMux::FrameCodec.encode(orphan_message, MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER)
    )

    await_epoch_ready(companion, 2)
    arriving = admit_client(companion, config, 2)
    send_command(arriving, Bytes[10_u8])
    next_command(companion, 10_u8, 2)
    companion.reply(Bytes[10_u8])
    read_payload(arriving, 10_u8).should eq(Bytes[10_u8])
  ensure
    departing.try &.close
    arriving.try &.close
    runtime.try &.stop
    runtime_done.try &.receive
    companion.try &.stop
  end
end
