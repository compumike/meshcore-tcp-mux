require "./spec_helper"
require "../src/meshcore_tcp_mux/protocol"

private def bytes(opcode : Int, size : Int) : Bytes
  Bytes.new(size, 0_u8).tap { |b| b[0] = opcode.to_u8 }
end

alias P = MeshCoreTCPMux::Protocol

describe MeshCoreTCPMux::Protocol do
  it "describes every native_v13 command and rejects reserved/unknown opcodes" do
    expected = ((1_u8..43_u8).to_a + (50_u8..52_u8).to_a + (54_u8..65_u8).to_a)
    P::DESCRIPTORS.keys.sort.should eq(expected)
    (44_u8..49_u8).each { |code| P.validate_command(Bytes[code]).reason.should eq(P::ERR_UNSUPPORTED_CMD) }
    P.validate_command(Bytes[53_u8]).reason.should eq(P::ERR_UNSUPPORTED_CMD)
    P.validate_command(Bytes[255_u8]).reason.should eq(P::ERR_UNSUPPORTED_CMD)
    P.validate_command(Bytes.empty).reason.should eq(P::ERR_UNSUPPORTED_CMD)
  end

  it "accepts a structurally valid example of every known command" do
    contact = bytes(9, 136)
    scope = bytes(63, 48)
    scope[1] = 'x'.ord.to_u8
    scope[2] = 0
    custom = Bytes[41_u8, 'a'.ord.to_u8, ':'.ord.to_u8, 'b'.ord.to_u8]
    reboot = Bytes[19_u8] + "reboot".to_slice
    reset = Bytes[51_u8] + "reset".to_slice
    channel_data = Bytes[62_u8, 0_u8, 0xff_u8, 1_u8, 0_u8]
    examples = {
      1 => bytes(1, 8), 2 => bytes(2, 14), 3 => bytes(3, 7), 4 => bytes(4, 1),
      5 => bytes(5, 1), 6 => bytes(6, 5), 7 => bytes(7, 1), 8 => bytes(8, 2),
      9 => contact, 10 => bytes(10, 1), 11 => bytes(11, 11), 12 => bytes(12, 2),
      13 => bytes(13, 33), 14 => bytes(14, 9), 15 => bytes(15, 33), 16 => bytes(16, 33),
      17 => bytes(17, 1), 18 => bytes(18, 99), 19 => reboot, 20 => bytes(20, 1),
      21 => bytes(21, 9), 22 => bytes(22, 2), 23 => bytes(23, 1), 24 => bytes(24, 65),
      25 => bytes(25, 6), 26 => bytes(26, 33), 27 => bytes(27, 33), 28 => bytes(28, 33),
      29 => bytes(29, 33), 30 => bytes(30, 33), 31 => bytes(31, 2), 32 => bytes(32, 50),
      33 => bytes(33, 1), 34 => bytes(34, 2), 35 => bytes(35, 1), 36 => bytes(36, 11),
      37 => bytes(37, 5), 38 => bytes(38, 2), 39 => bytes(39, 4), 40 => bytes(40, 1),
      41 => custom, 42 => bytes(42, 34), 43 => bytes(43, 1), 50 => bytes(50, 34),
      51 => reset, 52 => bytes(52, 34), 54 => Bytes[54_u8, 0_u8],
      55 => Bytes[55_u8, 0x80_u8], 56 => Bytes[56_u8, 0_u8], 57 => bytes(57, 34),
      58 => bytes(58, 2), 59 => bytes(59, 1), 60 => bytes(60, 1),
      61 => Bytes[61_u8, 0_u8, 2_u8], 62 => channel_data, 63 => scope,
      64 => bytes(64, 1), 65 => bytes(65, 4),
    }
    examples.each do |opcode, payload|
      result = P.validate_command(payload)
      result.valid?.should be_true, "opcode #{opcode}: #{result.reason}"
      result.descriptor.not_nil!.opcode.should eq(opcode)
    end
  end

  it "rejects structurally truncated and unsafe dynamic commands locally" do
    malformed = [
      bytes(1, 7), bytes(2, 13), bytes(3, 6), bytes(4, 4), bytes(6, 4),
      bytes(8, 1), bytes(9, 135), bytes(9, 137), bytes(11, 10), bytes(12, 1),
      bytes(13, 32), bytes(14, 12), bytes(15, 32), bytes(16, 32), bytes(17, 32),
      bytes(18, 98), Bytes[19_u8] + "reb00t".to_slice, bytes(21, 8), bytes(22, 1),
      bytes(24, 64), Bytes[25_u8, 4_u8, 1_u8, 2_u8, 3_u8, 4_u8], bytes(26, 32),
      bytes(27, 32), bytes(28, 32), bytes(29, 32), bytes(30, 32), bytes(31, 1),
      bytes(32, 49), bytes(32, 66), bytes(34, 1), bytes(36, 10), bytes(37, 4),
      bytes(38, 1), bytes(39, 35), Bytes[41_u8, 'a'.ord.to_u8, 'b'.ord.to_u8, 'c'.ord.to_u8],
      bytes(42, 33), bytes(50, 33), Bytes[51_u8] + "resat".to_slice,
      Bytes[52_u8, 1_u8] + Bytes.new(32), Bytes[54_u8, 2_u8], Bytes[55_u8, 0_u8],
      Bytes[56_u8, 3_u8], bytes(57, 33), bytes(58, 1), Bytes[61_u8, 1_u8, 0_u8],
      Bytes[62_u8, 0_u8, 2_u8, 0_u8, 1_u8], bytes(63, 47), bytes(65, 3),
    ]
    malformed.each do |payload|
      result = P.validate_command(payload)
      result.valid?.should be_false, "opcode #{payload[0]} was accepted"
      result.reason.should eq(P::ERR_ILLEGAL_ARG)
    end
  end

  it "validates dynamic path, scope, contact, and telemetry forms" do
    # Two one-byte path hashes followed by a two-byte data type.
    P.validate_command(Bytes[62_u8, 0_u8, 2_u8, 0xaa_u8, 0xbb_u8, 1_u8, 0_u8]).valid?.should be_true
    P.validate_command(bytes(9, 144)).valid?.should be_true
    P.validate_command(bytes(9, 148)).valid?.should be_true
    self_telemetry = P.validate_command(bytes(39, 4))
    self_telemetry.descriptor.not_nil!.grammar.should eq(P::Grammar::SelfTelemetry)
    remote_telemetry = P.validate_command(bytes(39, 36))
    remote_telemetry.valid?.should be_true
    remote_telemetry.descriptor.not_nil!.flags.includes?(P::CommandFlags::RemoteLease).should be_true
    P.validate_command(Bytes[54_u8, 0_u8] + Bytes.new(16)).valid?.should be_true
    P.validate_command(Bytes[54_u8, 1_u8]).valid?.should be_true
  end

  it "validates fixed, streamed, echoed, and extensible upstream responses" do
    clock = P::DESCRIPTORS[5_u8]
    P.validate_response!(clock, bytes(9, 5)).should eq(P::ResponseDisposition::Complete)
    expect_raises(P::ProtocolError) { P.validate_response!(clock, bytes(9, 4)) }
    expect_raises(P::ProtocolError) { P.validate_response!(clock, Bytes[0_u8]) }
    P.validate_response!(clock, Bytes[1_u8, 99_u8]).should eq(P::ResponseDisposition::Complete)

    contacts = P::DESCRIPTORS[4_u8]
    P.validate_response!(contacts, bytes(2, 5)).should eq(P::ResponseDisposition::Progress)
    P.validate_response!(contacts, bytes(3, 148), phase: 1).should eq(P::ResponseDisposition::Progress)
    P.validate_response!(contacts, bytes(4, 5), phase: 1).should eq(P::ResponseDisposition::Complete)
    expect_raises(P::ProtocolError) { P.validate_response!(contacts, bytes(3, 148)) }
    expect_raises(P::ProtocolError) { P.validate_response!(contacts, bytes(2, 5), phase: 1) }

    channel_command = Bytes[31_u8, 3_u8]
    channel_response = bytes(0x12, 50); channel_response[1] = 3
    P.validate_response!(P::DESCRIPTORS[31_u8], channel_response, channel_command)
    channel_response[1] = 2
    expect_raises(P::ProtocolError) { P.validate_response!(P::DESCRIPTORS[31_u8], channel_response, channel_command) }

    stats_command = Bytes[56_u8, 1_u8]
    stats_response = bytes(0x18, 14); stats_response[1] = 1
    P.validate_response!(P::DESCRIPTORS[56_u8], stats_response, stats_command)
    expect_raises(P::ProtocolError) { P.validate_response_shape!(Bytes[0x1a_u8, 0_u8]) }
    expect_raises(P::ProtocolError) { P.validate_response_shape!(Bytes[0x2f_u8]) }
    P.validate_response_shape!(Bytes[0xfe_u8, 1_u8])

    advert_path = bytes(0x16, 8); advert_path[5] = 2
    P.validate_response_shape!(advert_path)
    expect_raises(P::ProtocolError) { P.validate_response_shape!(advert_path[0, 7]) }

    discovery = bytes(0x8d, 12)
    discovery[8] = 1; discovery[10] = 1
    P.validate_response_shape!(discovery)
    expect_raises(P::ProtocolError) { P.validate_response_shape!(discovery[0, 11]) }
  end

  it "covers every declared success and terminal error response grammar" do
    samples = {
      0x00_u8 => bytes(0x00, 1), 0x05_u8 => bytes(0x05, 58),
      0x03_u8 => bytes(0x03, 148),
      0x06_u8 => bytes(0x06, 10), 0x07_u8 => bytes(0x07, 13),
      0x08_u8 => bytes(0x08, 8), 0x09_u8 => bytes(0x09, 5),
      0x0a_u8 => bytes(0x0a, 1), 0x0b_u8 => bytes(0x0b, 2),
      0x0c_u8 => bytes(0x0c, 11), 0x0d_u8 => bytes(0x0d, 82),
      0x0e_u8 => bytes(0x0e, 65), 0x0f_u8 => bytes(0x0f, 1),
      0x10_u8 => bytes(0x10, 16), 0x11_u8 => bytes(0x11, 11),
      0x12_u8 => bytes(0x12, 50), 0x13_u8 => bytes(0x13, 6),
      0x14_u8 => bytes(0x14, 65), 0x15_u8 => bytes(0x15, 1),
      0x16_u8 => bytes(0x16, 6), 0x17_u8 => bytes(0x17, 9),
      0x18_u8 => bytes(0x18, 11), 0x19_u8 => bytes(0x19, 3),
      0x1a_u8 => bytes(0x1a, 1), 0x1b_u8 => bytes(0x1b, 9),
      0x1c_u8 => bytes(0x1c, 1), 0x8b_u8 => bytes(0x8b, 8),
    }
    samples[0x18_u8][1] = 0
    samples[0x1b_u8][8] = 0

    P::DESCRIPTORS.each_value do |descriptor|
      next if descriptor.grammar.contacts? || descriptor.success_codes.empty?
      descriptor.success_codes.each do |code|
        P.validate_response!(descriptor, samples[code]).should eq(P::ResponseDisposition::Complete)
      end
      P.validate_response!(descriptor, Bytes[1_u8, 77_u8]).should eq(P::ResponseDisposition::Complete)
    end
  end

  it "validates inbox message bodies and downgrades V3 text without changing the body" do
    contact = Bytes[0x10_u8, 0xf0_u8, 1_u8, 2_u8] + Bytes.new(12) { |i| (i + 10).to_u8 }
    downgraded = P.downgrade_inbox(contact, 0_u8)
    downgraded.should eq(Bytes[0x07_u8] + contact[4..])
    P.downgrade_inbox(contact, 3_u8).should eq(contact)

    channel = Bytes[0x11_u8, 0xf0_u8, 1_u8, 2_u8] + Bytes.new(7, 9_u8)
    P.downgrade_inbox(channel, 2_u8).should eq(Bytes[0x08_u8] + channel[4..])

    datagram = Bytes[0x1b_u8, 0_u8, 0xff_u8, 1_u8, 0_u8, 0_u8, 0_u8, 0_u8, 2_u8, 7_u8, 8_u8]
    P.downgrade_inbox(datagram, 0_u8).should eq(datagram)
    expect_raises(P::ProtocolError) { P.validate_response_shape!(datagram[0, 10]) }
  end

  it "builds handshakes, normalizes query targets, and validates identity" do
    start = P.app_start_payload("test")
    start.should eq(Bytes[1_u8] + Bytes.new(7, 0_u8) + "test".to_slice)
    P.validate_command(start).valid?.should be_true
    expect_raises(ArgumentError) { P.app_start_payload("x", Bytes.new(6)) }

    P.device_query_payload.should eq(Bytes[22_u8, 13_u8])
    P.normalize_device_query(Bytes[22_u8, 2_u8, 99_u8]).should eq(Bytes[22_u8, 13_u8, 99_u8])

    self_info = bytes(5, 58)
    32.times { |i| self_info[4 + i] = i.to_u8 }
    P.validate_self_info!(self_info).should eq(Bytes.new(32) { |i| i.to_u8 })
    expect_raises(P::ProtocolError) { P.validate_self_info!(bytes(5, 57)) }

    device = bytes(0x0d, 82); device[1] = 13
    P.validate_device_info!(device).should eq(13_u8)
    device[1] = 12
    expect_raises(P::ProtocolError) { P.validate_device_info!(device) }
  end

  it "validates trace response variable fields exactly" do
    trace = bytes(0x89, 16)
    trace[2] = 2 # two hash bytes
    trace[3] = 1 # one SNR byte for two hops
    P.validate_response_shape!(trace)
    expect_raises(P::ProtocolError) { P.validate_response_shape!(trace[0, 15]) }
  end
end
