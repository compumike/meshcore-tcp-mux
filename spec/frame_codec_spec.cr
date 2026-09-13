require "./spec_helper"
require "../src/meshcore_tcp_mux/frame_codec"

private def encoded(payload : Array(Int32), marker = MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER) : Bytes
  # Convert the supplied opaque test payload to bytes before adding the TCP envelope.
  MeshCoreTCPMux::FrameCodec.encode(Bytes.new(payload.size) { |i| payload[i].to_u8 }, marker)
end

describe MeshCoreTCPMux::FrameCodec do
  # Framing-only tests: payloads are opaque here, even when their first byte
  # resembles a command. The envelope is marker ('<' request / '>' response),
  # two-byte little-endian payload length, then payload. Marker-looking bytes
  # inside a body are data, never delimiters. Tests control time without sleeping.

  marker = MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER
  reverse_marker = MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER
  zero = Time::Span.zero

  it "encodes both directional envelopes and rejects invalid payload sizes" do
    # GET_DEVICE_TIME (5): local clock query.
    # Request envelope: '<' (0x3c), payload length 1 (u16 LE), GET_DEVICE_TIME (5).
    MeshCoreTCPMux::FrameCodec.encode(Bytes[0x05_u8], marker).should eq(Bytes[0x3c_u8, 0x01_u8, 0x00_u8, 0x05_u8])
    # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
    # byte-for-byte.
    MeshCoreTCPMux::FrameCodec.encode(Bytes[0x09_u8, 0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8], reverse_marker).should eq(
      # Response envelope: '>' (0x3e), length 5 (u16 LE), CURRENT_TIME = 0x12345678.
      Bytes[0x3e_u8, 0x05_u8, 0x00_u8, 0x09_u8, 0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8]
    )
    expect_raises(ArgumentError) { MeshCoreTCPMux::FrameCodec.encode(Bytes.empty, marker) }
    # 177-byte payload is one byte over the native 176-byte maximum.
    expect_raises(ArgumentError) { MeshCoreTCPMux::FrameCodec.encode(Bytes.new(177), marker) }
    expect_raises(ArgumentError) { MeshCoreTCPMux::FrameCodec::Decoder.new('?'.ord.to_u8) }
  end

  it "decodes a request split at every byte boundary" do
    # Opaque payload includes both framing markers and 0xff; none may split the payload.
    frame = encoded([0x05, 0x3c, 0x3e, 0xff])
    (0..frame.size).each do |split|
      decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(marker)
      found = [] of Bytes
      decoder.feed(frame[0, split], zero) { |payload| found << payload }
      decoder.feed(frame[split, frame.size - split], 1.millisecond) { |payload| found << payload }
      # Opaque payload includes both framing markers and 0xff; none may split the payload.
      found.should eq([Bytes[0x05_u8, 0x3c_u8, 0x3e_u8, 0xff_u8]])
      decoder.finish
    end
  end

  it "decodes a response split at every byte boundary" do
    # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
    # byte-for-byte.
    frame = encoded([0x09, 0x78, 0x56, 0x34, 0x12], reverse_marker)
    (0..frame.size).each do |split|
      decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(reverse_marker)
      found = [] of Bytes
      decoder.feed(frame[0, split], zero) { |payload| found << payload }
      decoder.feed(frame[split, frame.size - split], 1.millisecond) { |payload| found << payload }
      # CURRENT_TIME (0x09), followed by a four-byte little-endian timestamp; compare the reply
      # byte-for-byte.
      found.should eq([Bytes[0x09_u8, 0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8]])
    end
  end

  it "accepts one-byte input and hundreds of coalesced frames" do
    # Opaque sequence-number payload; checks ordering across queued/coalesced frames, not
    # protocol semantics.
    frames = (0...300).map { |i| encoded([i % 256]) }
    # Concatenated wire frames; no separators beyond each frame's own length header.
    stream = Bytes.new(frames.sum(&.size))
    offset = 0
    frames.each do |frame|
      stream[offset, frame.size].copy_from(frame)
      offset += frame.size
    end

    decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(marker)
    found = [] of Bytes
    # Feed exactly one wire byte at a time to exercise incremental decoding.
    stream.each { |byte| decoder.feed(Bytes[byte], zero) { |payload| found << payload } }
    found.size.should eq(300)
    # Opaque sequence-number payload; checks ordering across queued/coalesced frames, not
    # protocol semantics.
    found.each_with_index { |payload, i| payload.should eq(Bytes[(i % 256).to_u8]) }

    coalesced = [] of Bytes
    MeshCoreTCPMux::FrameCodec::Decoder.new(marker).feed(stream, zero) { |payload| coalesced << payload }
    coalesced.should eq(found)
  end

  it "returns a complete frame while retaining a following partial header" do
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    complete = encoded([0x0a])
    # One complete frame plus two bytes of the next header, deliberately leaving it incomplete.
    bytes = Bytes.new(complete.size + 2)
    bytes[0, complete.size].copy_from(complete)
    # Partial request header: '<', low length byte 1; high length byte is missing.
    bytes[complete.size, 2].copy_from(Bytes[0x3c_u8, 0x01_u8])
    decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(marker)

    found = [] of Bytes
    decoder.feed(bytes, zero) { |payload| found << payload }
    # NO_MORE_MESSAGES (0x0a): inbox empty.
    found.should eq([Bytes[0x0a_u8]])
    decoder.partial?.should be_true
    expect_raises(MeshCoreTCPMux::FrameCodec::TruncatedFrameError) { decoder.finish }
  end

  it "owns completed payload bytes independently of reused input scratch" do
    # GET_DEVICE_TIME opcode plus opaque 0xaa sentinel; transport preserves bytes without
    # parsing them.
    scratch = encoded([0x05, 0xaa])
    payload = nil
    MeshCoreTCPMux::FrameCodec::Decoder.new(marker).feed(scratch, zero) { |found| payload = found }
    scratch.fill(0_u8)
    # GET_DEVICE_TIME opcode plus opaque 0xaa sentinel; transport preserves bytes without
    # parsing them.
    payload.not_nil!.should eq(Bytes[0x05_u8, 0xaa_u8])
  end

  it "rejects malformed headers without scanning for a later marker" do
    expect_raises(MeshCoreTCPMux::FrameCodec::MalformedFrameError) do
      # Wrong-direction marker '>'; the later '<' must not trigger resynchronization.
      MeshCoreTCPMux::FrameCodec::Decoder.new(marker).feed(Bytes[0x3e_u8, 1_u8, 0_u8, 5_u8, 0x3c_u8], zero) { }
    end
    expect_raises(MeshCoreTCPMux::FrameCodec::MalformedFrameError) do
      # Request header with forbidden zero payload length (u16 LE).
      MeshCoreTCPMux::FrameCodec::Decoder.new(marker).feed(Bytes[0x3c_u8, 0_u8, 0_u8], zero) { }
    end
    expect_raises(MeshCoreTCPMux::FrameCodec::MalformedFrameError) do
      # Request header with length 177, one byte above the 176-byte limit.
      MeshCoreTCPMux::FrameCodec::Decoder.new(marker).feed(Bytes[0x3c_u8, 177_u8, 0_u8], zero) { }
    end
  end

  it "validates EOF at every offset" do
    # Opaque payload with 0xaa/0xbb sentinels for truncation and ownership checks.
    frame = encoded([0x05, 0xaa, 0xbb])
    (0...frame.size).each do |offset|
      decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(marker)
      decoder.feed(frame[0, offset], zero) { }
      if offset == 0
        decoder.finish
      else
        expect_raises(MeshCoreTCPMux::FrameCodec::TruncatedFrameError) { decoder.finish }
      end
    end
    decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(marker)
    decoder.feed(frame, zero) { }
    decoder.finish
  end

  it "uses an absolute deadline that trickled bytes cannot extend" do
    decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(marker, 5.seconds)
    # Only the '<' request marker: deliberately incomplete header.
    decoder.feed(Bytes[0x3c_u8], zero) { }
    # Low length byte 3 of the trickled header; not a command opcode.
    decoder.feed(Bytes[0x03_u8], 4.seconds) { }
    # High length byte 0 of the trickled header: payload length is 3.
    decoder.feed(Bytes[0x00_u8], 4.9.seconds) { }
    expect_raises(MeshCoreTCPMux::FrameCodec::FrameDeadlineExceededError) do
      # First payload byte arrives at the absolute deadline and must be rejected.
      decoder.feed(Bytes[0x05_u8], 5.seconds) { }
    end
  end

  it "does not deadline an idle decoder and starts a new deadline after each frame" do
    decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(marker, 5.seconds)
    decoder.check_deadline(1.hour)
    found = [] of Bytes
    # GET_DEVICE_TIME (5): local clock query.
    decoder.feed(encoded([0x05]), 1.hour) { |payload| found << payload }
    # GET_DEVICE_TIME (5): local clock query.
    found.should eq([Bytes[0x05_u8]])
    decoder.check_deadline(2.hours)
    # Only the '<' request marker: deliberately incomplete header.
    decoder.feed(Bytes[0x3c_u8], 2.hours) { }
    decoder.check_deadline(2.hours + 4.9.seconds)
    expect_raises(MeshCoreTCPMux::FrameCodec::FrameDeadlineExceededError) do
      decoder.check_deadline(2.hours + 5.seconds)
    end
  end
end
