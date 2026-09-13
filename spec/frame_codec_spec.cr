require "./spec_helper"
require "../src/meshcore_tcp_mux/frame_codec"

private def encoded(payload : Array(Int32), marker = MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER) : Bytes
  MeshCoreTCPMux::FrameCodec.encode(Bytes.new(payload.size) { |i| payload[i].to_u8 }, marker)
end

describe MeshCoreTCPMux::FrameCodec do
  marker = MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER
  reverse_marker = MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER
  zero = Time::Span.zero

  it "encodes both directional envelopes and rejects invalid payload sizes" do
    MeshCoreTCPMux::FrameCodec.encode(Bytes[0x05_u8], marker).should eq(Bytes[0x3c_u8, 0x01_u8, 0x00_u8, 0x05_u8])
    MeshCoreTCPMux::FrameCodec.encode(Bytes[0x09_u8, 0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8], reverse_marker).should eq(
      Bytes[0x3e_u8, 0x05_u8, 0x00_u8, 0x09_u8, 0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8]
    )
    expect_raises(ArgumentError) { MeshCoreTCPMux::FrameCodec.encode(Bytes.empty, marker) }
    expect_raises(ArgumentError) { MeshCoreTCPMux::FrameCodec.encode(Bytes.new(177), marker) }
    expect_raises(ArgumentError) { MeshCoreTCPMux::FrameCodec::Decoder.new('?'.ord.to_u8) }
  end

  it "decodes a request split at every byte boundary" do
    frame = encoded([0x05, 0x3c, 0x3e, 0xff])
    (0..frame.size).each do |split|
      decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(marker)
      found = [] of Bytes
      decoder.feed(frame[0, split], zero) { |payload| found << payload }
      decoder.feed(frame[split, frame.size - split], 1.millisecond) { |payload| found << payload }
      found.should eq([Bytes[0x05_u8, 0x3c_u8, 0x3e_u8, 0xff_u8]])
      decoder.finish
    end
  end

  it "decodes a response split at every byte boundary" do
    frame = encoded([0x09, 0x78, 0x56, 0x34, 0x12], reverse_marker)
    (0..frame.size).each do |split|
      decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(reverse_marker)
      found = [] of Bytes
      decoder.feed(frame[0, split], zero) { |payload| found << payload }
      decoder.feed(frame[split, frame.size - split], 1.millisecond) { |payload| found << payload }
      found.should eq([Bytes[0x09_u8, 0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8]])
    end
  end

  it "accepts one-byte input and hundreds of coalesced frames" do
    frames = (0...300).map { |i| encoded([i % 256]) }
    stream = Bytes.new(frames.sum(&.size))
    offset = 0
    frames.each do |frame|
      stream[offset, frame.size].copy_from(frame)
      offset += frame.size
    end

    decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(marker)
    found = [] of Bytes
    stream.each { |byte| decoder.feed(Bytes[byte], zero) { |payload| found << payload } }
    found.size.should eq(300)
    found.each_with_index { |payload, i| payload.should eq(Bytes[(i % 256).to_u8]) }

    coalesced = [] of Bytes
    MeshCoreTCPMux::FrameCodec::Decoder.new(marker).feed(stream, zero) { |payload| coalesced << payload }
    coalesced.should eq(found)
  end

  it "returns a complete frame while retaining a following partial header" do
    complete = encoded([0x0a])
    bytes = Bytes.new(complete.size + 2)
    bytes[0, complete.size].copy_from(complete)
    bytes[complete.size, 2].copy_from(Bytes[0x3c_u8, 0x01_u8])
    decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(marker)

    found = [] of Bytes
    decoder.feed(bytes, zero) { |payload| found << payload }
    found.should eq([Bytes[0x0a_u8]])
    decoder.partial?.should be_true
    expect_raises(MeshCoreTCPMux::FrameCodec::TruncatedFrameError) { decoder.finish }
  end

  it "owns completed payload bytes independently of reused input scratch" do
    scratch = encoded([0x05, 0xaa])
    payload = nil
    MeshCoreTCPMux::FrameCodec::Decoder.new(marker).feed(scratch, zero) { |found| payload = found }
    scratch.fill(0_u8)
    payload.not_nil!.should eq(Bytes[0x05_u8, 0xaa_u8])
  end

  it "rejects malformed headers without scanning for a later marker" do
    expect_raises(MeshCoreTCPMux::FrameCodec::MalformedFrameError) do
      MeshCoreTCPMux::FrameCodec::Decoder.new(marker).feed(Bytes[0x3e_u8, 1_u8, 0_u8, 5_u8, 0x3c_u8], zero) { }
    end
    expect_raises(MeshCoreTCPMux::FrameCodec::MalformedFrameError) do
      MeshCoreTCPMux::FrameCodec::Decoder.new(marker).feed(Bytes[0x3c_u8, 0_u8, 0_u8], zero) { }
    end
    expect_raises(MeshCoreTCPMux::FrameCodec::MalformedFrameError) do
      MeshCoreTCPMux::FrameCodec::Decoder.new(marker).feed(Bytes[0x3c_u8, 177_u8, 0_u8], zero) { }
    end
  end

  it "validates EOF at every offset" do
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
    decoder.feed(Bytes[0x3c_u8], zero) { }
    decoder.feed(Bytes[0x03_u8], 4.seconds) { }
    decoder.feed(Bytes[0x00_u8], 4.9.seconds) { }
    expect_raises(MeshCoreTCPMux::FrameCodec::FrameDeadlineExceededError) do
      decoder.feed(Bytes[0x05_u8], 5.seconds) { }
    end
  end

  it "does not deadline an idle decoder and starts a new deadline after each frame" do
    decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(marker, 5.seconds)
    decoder.check_deadline(1.hour)
    found = [] of Bytes
    decoder.feed(encoded([0x05]), 1.hour) { |payload| found << payload }
    found.should eq([Bytes[0x05_u8]])
    decoder.check_deadline(2.hours)
    decoder.feed(Bytes[0x3c_u8], 2.hours) { }
    decoder.check_deadline(2.hours + 4.9.seconds)
    expect_raises(MeshCoreTCPMux::FrameCodec::FrameDeadlineExceededError) do
      decoder.check_deadline(2.hours + 5.seconds)
    end
  end
end
