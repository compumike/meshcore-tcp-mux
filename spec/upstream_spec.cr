require "./spec_helper"
require "../src/meshcore_tcp_mux/upstream"
require "./support/native_startup"

private class IsolatedProbeCompanion
  # Loopback-only startup/probe tests. The fake recognizes APP_START (1),
  # DEVICE_QUERY (22/0x16), and default-scope reset (54/0x36). Failure modes inject
  # raw envelopes or omit replies to verify bounded failure and socket cleanup.

  getter commands = [] of Bytes
  getter port : Int32
  getter disconnected = Channel(Nil).new(1)

  @server : TCPServer
  @mode : Symbol
  @socket : TCPSocket? = nil
  @stopped = false

  def initialize(@mode = :success) : Nil
    @server = TCPServer.new("127.0.0.1", 0)
    @port = @server.local_address.port
    spawn { serve }
  end

  def close : Nil
    @socket.try &.close
    @server.close
    unless @stopped
      select
      when @disconnected.receive
      when timeout(1.second)
      end
    end
  end

  private def serve : Nil
    socket = @server.accept
    @socket = socket
    case @mode
    when :eof
      socket.close
      return
    when :short
      # Truncated response envelope: declares 5 payload bytes, supplies only SELF_INFO opcode 5.
      socket.write(Bytes[0x3e_u8, 5_u8, 0_u8, 5_u8])
      socket.close
      return
    when :wrong_direction
      # GET_DEVICE_TIME (5): local clock query.
      socket.write(MeshCoreTCPMux::FrameCodec.encode(Bytes[5_u8], MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER))
      socket.close
      return
    end

    decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER)
    # Socket read scratch space; capacity is arbitrary and is not a protocol field.
    buffer = Bytes.new(512)
    loop do
      count = socket.read(buffer)
      break if count == 0
      decoder.feed(buffer[0, count], MeshCoreTCPMux::Clock.now) do |payload|
        @commands << payload
        respond(socket, payload)
      end
    end
  rescue IO::Error
    # Closing the probe endpoint is the expected successful shutdown path.
  ensure
    @socket = nil
    socket.try &.close
    @stopped = true
    @disconnected.send(nil)
  end

  private def respond(socket : TCPSocket, payload : Bytes) : Nil
    response = case payload[0]
               when 1 # APP_START.
                 SpecSupport::NativeStartupTransport.self_info
               when 0x16 # DEVICE_QUERY.
                 return if @mode == :drop_device_query
                 SpecSupport::NativeStartupTransport.device_info
               when 0x36 # SET_FLOOD_SCOPE_KEY.
                 # ERR (0x01), BAD_STATE.
                 # OK (0x00): startup default-scope restoration completed.
                 @mode == :scope_failure ? Bytes[1_u8, 4_u8] : Bytes[0_u8]
               else
                 # ERR (0x01), UNSUPPORTED_CMD.
                 Bytes[1_u8, 1_u8]
               end
    socket.write(MeshCoreTCPMux::FrameCodec.encode(response, MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER))
  end
end

private def probe_config : MeshCoreTCPMux::Config
  MeshCoreTCPMux::Config.new.tap do |config|
    config.startup_timeout = 80.milliseconds
    config.frame_timeout = 40.milliseconds
    config.write_timeout = 100.milliseconds
  end
end

describe MeshCoreTCPMux::Upstream do
  it "uses the shared endpoint for the complete startup fence and shuts it down cleanly" do
    fake = IsolatedProbeCompanion.new
    result = MeshCoreTCPMux::Upstream.probe("127.0.0.1", fake.port, probe_config)
    result.should contain("profile=native_v13 protocol=13")
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 0 with no key: restore the configured default scope.
    fake.commands.should eq(MeshCoreTCPMux::Startup.probes + [Bytes[0x36_u8, 0_u8]])
    select
    when fake.disconnected.receive
    when timeout(1.second)
      fail "probe returned without closing and joining its endpoint"
    end
  ensure
    fake.try &.close
  end

  it "raises a bounded startup error on immediate EOF" do
    fake = IsolatedProbeCompanion.new(:eof)
    expect_raises(MeshCoreTCPMux::Startup::Error, /upstream closed.*peer disconnected/) do
      MeshCoreTCPMux::Upstream.probe("127.0.0.1", fake.port, probe_config)
    end
  ensure
    fake.try &.close
  end

  it "reports a truncated upstream frame rather than accepting a short reply" do
    fake = IsolatedProbeCompanion.new(:short)
    expect_raises(MeshCoreTCPMux::Startup::Error, /upstream closed.*incomplete frame/) do
      MeshCoreTCPMux::Upstream.probe("127.0.0.1", fake.port, probe_config)
    end
  ensure
    fake.try &.close
  end

  it "rejects a response encoded with the client-to-companion direction" do
    fake = IsolatedProbeCompanion.new(:wrong_direction)
    expect_raises(MeshCoreTCPMux::Startup::Error, /upstream closed.*marker/) do
      MeshCoreTCPMux::Upstream.probe("127.0.0.1", fake.port, probe_config)
    end
  ensure
    fake.try &.close
  end

  it "times out finitely when a required probe response is dropped" do
    fake = IsolatedProbeCompanion.new(:drop_device_query)
    # Use the project's compatibility clock so this timing bound runs on both
    # pre-1.19 Crystal and newer Time::Instant-based compilers.
    started = MeshCoreTCPMux::Clock.now
    expect_raises(MeshCoreTCPMux::Startup::Error, /timeout/) do
      MeshCoreTCPMux::Upstream.probe("127.0.0.1", fake.port, probe_config)
    end
    (MeshCoreTCPMux::Clock.now - started).should be < 1.second
  ensure
    fake.try &.close
  end

  it "rejects a failed startup scope reset" do
    fake = IsolatedProbeCompanion.new(:scope_failure)
    expect_raises(MeshCoreTCPMux::Startup::Error, /scope reset rejected/) do
      MeshCoreTCPMux::Upstream.probe("127.0.0.1", fake.port, probe_config)
    end
  ensure
    fake.try &.close
  end
end
