require "./spec_helper"
require "../src/meshcore_tcp_mux/startup"
require "./support/native_startup"

private def append_frame(buffer : Array(UInt8), payload : Bytes, marker : UInt8) : Nil
  buffer.concat(MeshCoreTCPMux::FrameCodec.encode(payload, marker))
end

private def write_probes(fake : SpecSupport::NativeStartupTransport, now = Time::Span.zero) : Nil
  wire = [] of UInt8
  MeshCoreTCPMux::Startup.probes.each do |payload|
    append_frame(wire, payload, MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER)
  end
  # Contiguous wire copy of all probes; split below to test fragmented/coalesced TCP input.
  bytes = Bytes.new(wire.size) { |i| wire[i] }
  # Exercise request framing across an arbitrary boundary as well as coalescing.
  split = bytes.size // 3
  fake.client_write(bytes[0, split], now)
  fake.client_write(bytes[split, bytes.size - split], now)
end

private def drive_startup(
  fake : SpecSupport::NativeStartupTransport,
  startup : MeshCoreTCPMux::Startup,
  now = Time::Span.zero,
  limit = 100,
) : Array(Bytes)
  decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER)
  received = [] of Bytes
  limit.times do |step|
    if wire = fake.tick
      # Exercise response framing at changing split points.
      split = step % (wire.size + 1)
      decoder.feed(wire[0, split], now) { |payload| consume_startup(payload, fake, startup, received, now) }
      decoder.feed(wire[split, wire.size - split], now) { |payload| consume_startup(payload, fake, startup, received, now) }
    end
    break if startup.ready?
  end
  received
end

private def consume_startup(
  payload : Bytes,
  fake : SpecSupport::NativeStartupTransport,
  startup : MeshCoreTCPMux::Startup,
  received : Array(Bytes),
  now : Time::Span,
) : Nil
  received << payload
  if command = startup.receive(payload, now)
    fake.client_write(
      MeshCoreTCPMux::FrameCodec.encode(command, MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER),
      now
    )
  end
end

describe SpecSupport::NativeStartupTransport do
  # Exercise the startup fence through a deterministic firmware-loop model and
  # real frame encoding/decoding, without sockets. Synthetic marker bytes separate
  # old retained replies from newly generated ones. Pre-ready pushes are ignored
  # by opcode, so marker-only pushes here intentionally omit their normal bodies.

  it "sends its retained queue before reading newly buffered commands" do
    stale = SpecSupport::NativeStartupTransport.device_info(0x55_u8)
    fake = SpecSupport::NativeStartupTransport.new(retained: [stale])
    # The pinned transport clears a replacement client's partial receive header
    # without clearing its response queue.
    # Partial request header: '<', low length byte 16; reconnect must discard it.
    fake.client_write(Bytes[0x3c_u8, 0x10_u8], 0.seconds)
    fake.reconnect
    fake.client_write(
      MeshCoreTCPMux::FrameCodec.encode(MeshCoreTCPMux::Startup.app_start, MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER),
      0.seconds
    )

    fake.tick.should_not be_nil
    fake.commands_received.should be_empty
    fake.tick.should be_nil
    fake.commands_received.should eq([MeshCoreTCPMux::Startup.app_start])
    fake.actions[0, 2].should eq([:send, :read_command])
  end

  it "never accepts a stale boundary for every retained prefix of length zero through four" do
    self_info = SpecSupport::NativeStartupTransport.self_info(0x55_u8)
    device_info = SpecSupport::NativeStartupTransport.device_info(0x55_u8)
    marker_classes = [
      self_info,
      device_info,
      # OK (0x00): stale generic success retained from the previous client.
      Bytes[0_u8],
      # CONTACTS_START (0x02) with contact count 2 (u32 little-endian).
      Bytes[2_u8, 2_u8, 0_u8, 0_u8, 0_u8],
      SpecSupport::NativeStartupTransport.contact(0x55_u8),
      # END_OF_CONTACTS (0x04), followed by the u32 LE last-modified timestamp.
      Bytes[4_u8, 0_u8, 0_u8, 0_u8, 0_u8],
    ]

    (0..4).each do |length|
      (marker_classes.size ** length).times do |combination|
        index = combination
        retained = Array(Bytes).new(length) do
          payload = marker_classes[index % marker_classes.size]
          index //= marker_classes.size
          payload
        end
        fake = SpecSupport::NativeStartupTransport.new(retained: retained)
        startup = MeshCoreTCPMux::Startup.new(0.seconds)
        write_probes(fake)
        drive_startup(fake, startup)

        startup.ready?.should be_true
        startup.device_info.not_nil![2].should eq(0xa5_u8)
        # SET_FLOOD_SCOPE_KEY (54/0x36), mode 0 with no key: restore the configured default
        # scope.
        fake.commands_received.should eq(MeshCoreTCPMux::Startup.probes + [Bytes[0x36_u8, 0_u8]])
      end
    end
  end

  it "stops an old contacts iterator at the first APP_START and tolerates pushes" do
    first_contact = SpecSupport::NativeStartupTransport.contact(0x11_u8)
    never_emitted = SpecSupport::NativeStartupTransport.contact(0x22_u8)
    # Opcode-only push markers; startup ignores high-code traffic before readiness without
    # parsing bodies.
    # ADVERT, MSG_WAITING, LOG_RX_DATA, PATH_DISCOVERY, one-byte push
    # 0x90, unknown extension 0xfe, and SEND_CONFIRMED. Here only high-code
    # classification matters; these are intentionally not full response bodies.
    interleaved_pushes = [0x80, 0x83, 0x88, 0x8d, 0x90, 0xfe, 0x82].map { |code| Bytes[code.to_u8] }
    fake = SpecSupport::NativeStartupTransport.new(
      contacts: [first_contact, never_emitted],
      pushes_after_commands: interleaved_pushes
    )
    startup = MeshCoreTCPMux::Startup.new(0.seconds)

    # With no command buffered, the surviving old iterator advances once.
    fake.tick.should be_nil
    fake.actions.last.should eq(:iterate_contacts)
    write_probes(fake)
    received = drive_startup(fake, startup)

    startup.ready?.should be_true
    fake.iterator_active?.should be_false
    received.should contain(first_contact)
    received.should_not contain(never_emitted)
    # Six different known and unknown high-code pushes interleave with the probe
    # replies. The seventh is queued after scope OK, when readiness is reached.
    interleaved_pushes.first(6).each { |push| received.should contain(push) }
  end

  it "fails boundedly when any required probe or scope response is dropped" do
    (0..6).each do |dropped|
      fake = SpecSupport::NativeStartupTransport.new(drop_responses: [dropped])
      startup = MeshCoreTCPMux::Startup.new(0.seconds)
      write_probes(fake)
      drive_startup(fake, startup)

      startup.ready?.should be_false
      fake.responses_dropped.should eq([dropped])
      expect_raises(MeshCoreTCPMux::Startup::Error, /timeout/) do
        startup.check_deadline(15.seconds)
      end
    end
  end

  it "does not let a continuous push flood extend the startup deadline" do
    fake = SpecSupport::NativeStartupTransport.new
    startup = MeshCoreTCPMux::Startup.new(0.seconds)
    decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER)

    (0..15).each do |second|
      # MSG_WAITING (0x83): inbox availability hint; fetch the actual body separately.
      fake.push(Bytes[0x83_u8]).should be_true
      wire = fake.tick.not_nil!
      if second == 15
        expect_raises(MeshCoreTCPMux::Startup::Error, /timeout/) do
          decoder.feed(wire, second.seconds) { |payload| startup.receive(payload, second.seconds) }
        end
      else
        decoder.feed(wire, second.seconds) { |payload| startup.receive(payload, second.seconds) }
      end
    end
    startup.ready?.should be_false
  end
end
