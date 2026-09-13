require "./spec_helper"
require "../src/meshcore_tcp_mux/config"
require "../src/meshcore_tcp_mux/runtime"
require "../src/meshcore_tcp_mux/transport"
require "./support/native_startup"

private def tcp_pair : {TCPSocket, TCPSocket, TCPServer}
  server = TCPServer.new("127.0.0.1", 0)
  accepted = Channel(TCPSocket).new(1)
  spawn { accepted.send(server.accept) }
  client = TCPSocket.new("127.0.0.1", server.local_address.port)
  {client, accepted.receive, server}
end

private def unused_tcp_port : Int32
  server = TCPServer.new("127.0.0.1", 0)
  port = server.local_address.port
  server.close
  port
end

class FaultingWriteSocket < TCPSocket
  # Loopback-only transport tests: endpoints frame and own bytes but do not
  # validate command semantics. Several short payloads are ordering sentinels,
  # not valid MeshCore replies. The final runtime test adds a protocol-aware fake.
  # Envelope lengths and protocol integer fields are little-endian.

  getter write_calls = 0

  def write(slice : Bytes) : Nil
    @write_calls += 1
    super(slice[0, Math.min(2, slice.size)])
    raise IO::Error.new("injected failure after partial write")
  end
end

describe MeshCoreTCPMux::Transport::Endpoint do
  client_marker = MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER
  companion_marker = MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER

  it "incrementally reads owned frames and reports malformed input" do
    client, peer, server = tcp_pair
    events = Channel(MeshCoreTCPMux::Transport::Event).new(4)
    endpoint = MeshCoreTCPMux::Transport::Endpoint.new(
      peer, 7, client_marker, companion_marker, events, 5.seconds, 5.seconds, 4
    )
    endpoint.start
    # GET_DEVICE_TIME opcode plus opaque 0xaa sentinel; transport preserves bytes without
    # parsing them.
    frame = MeshCoreTCPMux::FrameCodec.encode(Bytes[5_u8, 0xaa_u8], client_marker)
    client.write(frame[0, 2])
    client.write(frame[2, frame.size - 2])
    event = events.receive.as(MeshCoreTCPMux::Transport::Frame)
    event.endpoint.should eq(7)
    # GET_DEVICE_TIME opcode plus opaque 0xaa sentinel; transport preserves bytes without
    # parsing them.
    event.payload.should eq(Bytes[5_u8, 0xaa_u8])

    event.payload[0] = 0xff
    # GET_DEVICE_TIME (5): local clock query.
    client.write(MeshCoreTCPMux::FrameCodec.encode(Bytes[5_u8], companion_marker))
    events.receive.should be_a(MeshCoreTCPMux::Transport::Closed)
  ensure
    endpoint.try &.stop
    client.try &.close
    server.try &.close
  end

  it "serializes all output through its sole writer" do
    client, peer, server = tcp_pair
    events = Channel(MeshCoreTCPMux::Transport::Event).new(32)
    endpoint = MeshCoreTCPMux::Transport::Endpoint.new(
      peer, 9, client_marker, companion_marker, events, 5.seconds, 5.seconds, 16
    )
    endpoint.start
    12.times do |index|
      # Opaque sequence-number payload; checks ordering across queued/coalesced frames, not
      # protocol semantics.
      endpoint.enqueue(MeshCoreTCPMux::Transport::Write.new(3, index.to_i64, Bytes[index.to_u8])).should be_true
    end

    decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(companion_marker)
    received = [] of Bytes
    # Socket read scratch space; capacity is arbitrary and is not a protocol field.
    buffer = Bytes.new(128)
    until received.size == 12
      count = client.read(buffer)
      decoder.feed(buffer[0, count], MeshCoreTCPMux::Clock.now) { |payload| received << payload }
    end
    # Opaque sequence-number payload; checks ordering across queued/coalesced frames, not
    # protocol semantics.
    received.should eq((0...12).map { |index| Bytes[index.to_u8] })
    12.times { events.receive.should be_a(MeshCoreTCPMux::Transport::Written) }
  ensure
    endpoint.try &.stop
    client.try &.close
    server.try &.close
  end

  it "enforces the absolute partial-frame deadline while the socket stays open" do
    client, peer, server = tcp_pair
    events = Channel(MeshCoreTCPMux::Transport::Event).new(1)
    endpoint = MeshCoreTCPMux::Transport::Endpoint.new(
      peer, 10, client_marker, companion_marker, events, 20.milliseconds, 5.seconds, 1
    )
    endpoint.start
    # Only the client '<' marker is written; missing length/body must hit the partial-frame
    # deadline.
    client.write(Bytes[client_marker])

    select
    when event = events.receive
      event.should be_a(MeshCoreTCPMux::Transport::Closed)
      event.as(MeshCoreTCPMux::Transport::Closed).reason.should contain("deadline")
    when timeout(1.second)
      fail "partial frame did not expire"
    end
  ensure
    endpoint.try &.stop
    client.try &.close
    server.try &.close
  end

  it "cancels a blocked reader handoff and bounds writer backpressure" do
    client, peer, server = tcp_pair
    events = Channel(MeshCoreTCPMux::Transport::Event).new
    endpoint = MeshCoreTCPMux::Transport::Endpoint.new(
      peer, 11, client_marker, companion_marker, events, 5.seconds, 5.seconds, 1
    )
    endpoint.start
    # GET_DEVICE_TIME (5): local clock query.
    client.write(MeshCoreTCPMux::FrameCodec.encode(Bytes[5_u8], client_marker))
    Fiber.yield

    # Opaque one-byte writer payload 1; distinguishes queue order, not a valid protocol
    # response.
    endpoint.enqueue(MeshCoreTCPMux::Transport::Write.new(4, 1, Bytes[1_u8])).should be_true
    Fiber.yield
    # Opaque one-byte writer payload 2; distinguishes queue order, not a valid protocol
    # response.
    endpoint.enqueue(MeshCoreTCPMux::Transport::Write.new(4, 2, Bytes[2_u8])).should be_true
    # Opaque one-byte writer payload 3; distinguishes queue order, not a valid protocol
    # response.
    endpoint.enqueue(MeshCoreTCPMux::Transport::Write.new(4, 3, Bytes[3_u8])).should be_false

    stopped = Channel(Nil).new(1)
    spawn do
      endpoint.stop
      stopped.send(nil)
    end
    select
    when stopped.receive
    when timeout(1.second)
      fail "endpoint shutdown blocked on an undrained event channel"
    end
  ensure
    endpoint.try &.stop
    client.try &.close
    server.try &.close
  end

  it "never retries a frame after a partial socket write fails" do
    server = TCPServer.new("127.0.0.1", 0)
    accepted = Channel(TCPSocket).new(1)
    spawn { accepted.send(server.accept) }
    socket = FaultingWriteSocket.new("127.0.0.1", server.local_address.port)
    peer = accepted.receive
    peer.read_timeout = 1.second
    events = Channel(MeshCoreTCPMux::Transport::Event).new(4)
    endpoint = MeshCoreTCPMux::Transport::Endpoint.new(
      socket, 12, client_marker, companion_marker, events, 5.seconds, 5.seconds, 2
    )
    endpoint.start
    # GET_DEVICE_TIME opcode plus opaque 0xaa sentinel; transport preserves bytes without
    # parsing them.
    endpoint.enqueue(MeshCoreTCPMux::Transport::Write.new(8, 1, Bytes[5_u8, 0xaa_u8])).should be_true
    # Opaque one-byte writer payload 6; distinguishes queue order, not a valid protocol
    # response.
    endpoint.enqueue(MeshCoreTCPMux::Transport::Write.new(8, 2, Bytes[6_u8])).should be_true

    event = events.receive.as(MeshCoreTCPMux::Transport::WriteFailed)
    event.write_id.should eq(1)
    event.reason.should contain("injected failure")
    socket.write_calls.should eq(1)
    # Socket read scratch space; capacity is arbitrary and is not a protocol field.
    received = Bytes.new(8)
    peer.read(received).should eq(2)
    # First two envelope bytes only: companion marker '>' and low payload-length byte 2.
    received[0, 2].should eq(Bytes[companion_marker, 2_u8])
    3.times { Fiber.yield }
    socket.write_calls.should eq(1)
  ensure
    endpoint.try &.stop
    peer.try &.close
    socket.try &.close
    server.try &.close
  end
end

describe MeshCoreTCPMux::Runtime do
  it "synchronizes before admission and relays framed traffic through the broker" do
    companion_server = TCPServer.new("127.0.0.1", 0)
    config = MeshCoreTCPMux::Config.new
    config.listen_host = "127.0.0.1"
    config.listen_port = unused_tcp_port
    config.startup_timeout = 1.second
    config.response_timeout = 1.second
    config.write_timeout = 1.second
    runtime = MeshCoreTCPMux::Runtime.new("127.0.0.1", companion_server.local_address.port, config)
    scope_written = Channel(Nil).new(1)
    companion_done = Channel(Nil).new(1)

    spawn do
      socket = companion_server.accept
      decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER)
      # Socket read scratch space; capacity is arbitrary and is not a protocol field.
      buffer = Bytes.new(1024)
      begin
        loop do
          count = socket.read(buffer)
          break if count == 0
          decoder.feed(buffer[0, count], MeshCoreTCPMux::Clock.now) do |payload|
            response = case payload[0]
                       when 1
                         SpecSupport::NativeStartupTransport.self_info
                       when 0x16
                         SpecSupport::NativeStartupTransport.device_info
                       when 0x36
                         scope_written.send(nil)
                         # OK (0x00): command accepted, not proof of radio delivery.
                         Bytes[0_u8]
                       when 10
                         # NO_MORE_MESSAGES (0x0a): inbox empty.
                         Bytes[10_u8]
                       when 5
                         # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp;
                         # compare the reply byte-for-byte.
                         Bytes[9_u8, 0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8]
                       else
                         # ERR (0x01), UNSUPPORTED_CMD.
                         Bytes[1_u8, 1_u8]
                       end
            socket.write(MeshCoreTCPMux::FrameCodec.encode(response, MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER))
          end
        end
      ensure
        socket.close
        companion_done.send(nil)
      end
    end

    runtime_done = Channel(Nil).new(1)
    spawn do
      runtime.run
      runtime_done.send(nil)
    end
    scope_written.receive
    sleep 20.milliseconds

    client = TCPSocket.new("127.0.0.1", config.listen_port)
    client.read_timeout = 1.second
    # GET_DEVICE_TIME (5): local clock query.
    client.write(MeshCoreTCPMux::FrameCodec.encode(Bytes[5_u8], MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER))
    decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER)
    replies = [] of Bytes
    # Socket read scratch space; capacity is arbitrary and is not a protocol field.
    buffer = Bytes.new(128)
    until replies.any? { |payload| payload[0] == 9 }
      count = client.read(buffer)
      decoder.feed(buffer[0, count], MeshCoreTCPMux::Clock.now) { |payload| replies << payload }
    end
    # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
    # byte-for-byte.
    replies.should contain(Bytes[9_u8, 0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8])

    runtime.stop
    runtime_done.receive
    companion_done.receive
  ensure
    client.try &.close
    runtime.try &.stop
    companion_server.try &.close
  end
end
