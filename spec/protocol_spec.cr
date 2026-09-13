# Wire-shape tests deliberately retain literal bytes so expected packets are
# independent of production encoders. bytes(opcode, size) zero-fills all fields
# except the first byte; size counts payload bytes, excluding the TCP envelope.
# Commands and responses reuse numeric codes, so direction matters. Multi-byte
# fields are little-endian; identities and bodies are synthetic test data.

require "./spec_helper"
require "../src/meshcore_tcp_mux/protocol"

private def bytes(opcode : Int, size : Int) : Bytes
  # Allocate a zero-filled shape fixture; set its opcode below. Size includes the opcode, not
  # the TCP envelope.
  Bytes.new(size, 0_u8).tap { |b| b[0] = opcode.to_u8 }
end

alias P = MeshCoreTCPMux::Protocol

describe MeshCoreTCPMux::Protocol do
  it "describes every native_v13 command and rejects reserved/unknown opcodes" do
    expected = ((1_u8..43_u8).to_a + (50_u8..52_u8).to_a + (54_u8..65_u8).to_a)
    P::DESCRIPTORS.keys.sort.should eq(expected)
    # Reserved opcode from the tested range; no command is defined for it.
    (44_u8..49_u8).each { |code| P.validate_command(Bytes[code]).reason.should eq(P::ERR_UNSUPPORTED_CMD) }
    # Unsupported/reserved command opcode 53: must be refused locally.
    P.validate_command(Bytes[53_u8]).reason.should eq(P::ERR_UNSUPPORTED_CMD)
    # Unsupported/reserved command opcode 255: must be refused locally.
    P.validate_command(Bytes[255_u8]).reason.should eq(P::ERR_UNSUPPORTED_CMD)
    P.validate_command(Bytes.empty).reason.should eq(P::ERR_UNSUPPORTED_CMD)
  end

  it "accepts a structurally valid example of every known command" do
    # ADD_UPDATE_CONTACT command (9), 136-byte zero-filled shape fixture.
    contact = bytes(9, 136)
    # SET_DEFAULT_FLOOD_SCOPE command (63), 48-byte zero-filled shape fixture.
    scope = bytes(63, 48)
    scope[1] = 'x'.ord.to_u8
    scope[2] = 0
    # SET_CUSTOM_VAR (41) with ASCII "a:b" (required name:value separator).
    custom = Bytes[41_u8, 'a'.ord.to_u8, ':'.ord.to_u8, 'b'.ord.to_u8]
    # REBOOT (19) followed by the required ASCII magic string "reboot".
    reboot = Bytes[19_u8] + "reboot".to_slice
    # FACTORY_RESET (51) plus required ASCII "reset" magic string.
    reset = Bytes[51_u8] + "reset".to_slice
    # SEND_CHANNEL_DATA (62), channel 0, path sentinel 0xff (no explicit path), data type 1 (u16
    # LE).
    channel_data = Bytes[62_u8, 0_u8, 0xff_u8, 1_u8, 0_u8]
    # One independent fixture per supported opcode prevents an omitted descriptor
    # from hiding behind a happy-path test of just a few common CLI commands.
    examples = {
      # APP_START command (1), 8-byte zero-filled shape fixture.
      1 => bytes(1, 8),
      # SEND_TXT_MSG command (2), 14-byte zero-filled shape fixture.
      2 => bytes(2, 14),
      # SEND_CHANNEL_TXT_MSG command (3), 7-byte zero-filled shape fixture.
      3 => bytes(3, 7),
      # GET_CONTACTS command (4), 1-byte zero-filled shape fixture.
      4 => bytes(4, 1),
      # GET_DEVICE_TIME command (5), 1-byte zero-filled shape fixture.
      5 => bytes(5, 1),
      # SET_DEVICE_TIME command (6), 5-byte zero-filled shape fixture.
      6 => bytes(6, 5),
      # SEND_SELF_ADVERT command (7), 1-byte zero-filled shape fixture.
      7 => bytes(7, 1),
      # SET_ADVERT_NAME command (8), 2-byte zero-filled shape fixture.
      8 => bytes(8, 2),
      9 => contact,
      # SYNC_NEXT_MESSAGE command (10), 1-byte zero-filled shape fixture.
      10 => bytes(10, 1),
      # SET_RADIO_PARAMS command (11), 11-byte zero-filled shape fixture.
      11 => bytes(11, 11),
      # SET_RADIO_TX_POWER command (12), 2-byte zero-filled shape fixture.
      12 => bytes(12, 2),
      # RESET_PATH command (13), 33-byte zero-filled shape fixture.
      13 => bytes(13, 33),
      # SET_ADVERT_LATLON command (14), 9-byte zero-filled shape fixture.
      14 => bytes(14, 9),
      # REMOVE_CONTACT command (15), 33-byte zero-filled shape fixture.
      15 => bytes(15, 33),
      # SHARE_CONTACT command (16), 33-byte zero-filled shape fixture.
      16 => bytes(16, 33),
      # EXPORT_CONTACT command (17), 1-byte zero-filled shape fixture.
      17 => bytes(17, 1),
      # IMPORT_CONTACT command (18), 99-byte zero-filled shape fixture.
      18 => bytes(18, 99),
      19 => reboot,
      # GET_BATT_AND_STORAGE command (20), 1-byte zero-filled shape fixture.
      20 => bytes(20, 1),
      # SET_TUNING_PARAMS command (21), 9-byte zero-filled shape fixture.
      21 => bytes(21, 9),
      # DEVICE_QUERY command (22), 2-byte zero-filled shape fixture.
      22 => bytes(22, 2),
      # EXPORT_PRIVATE_KEY command (23), 1-byte zero-filled shape fixture.
      23 => bytes(23, 1),
      # IMPORT_PRIVATE_KEY command (24), 65-byte zero-filled shape fixture.
      24 => bytes(24, 65),
      # SEND_RAW_DATA command (25), 6-byte zero-filled shape fixture.
      25 => bytes(25, 6),
      # SEND_LOGIN command (26), 33-byte zero-filled shape fixture.
      26 => bytes(26, 33),
      # SEND_STATUS_REQ command (27), 33-byte zero-filled shape fixture.
      27 => bytes(27, 33),
      # HAS_CONNECTION command (28), 33-byte zero-filled shape fixture.
      28 => bytes(28, 33),
      # LOGOUT command (29), 33-byte zero-filled shape fixture.
      29 => bytes(29, 33),
      # GET_CONTACT_BY_KEY command (30), 33-byte zero-filled shape fixture.
      30 => bytes(30, 33),
      # GET_CHANNEL command (31), 2-byte zero-filled shape fixture.
      31 => bytes(31, 2),
      # SET_CHANNEL command (32), 50-byte zero-filled shape fixture.
      32 => bytes(32, 50),
      # SIGN_START command (33), 1-byte zero-filled shape fixture.
      33 => bytes(33, 1),
      # SIGN_DATA command (34), 2-byte zero-filled shape fixture.
      34 => bytes(34, 2),
      # SIGN_FINISH command (35), 1-byte zero-filled shape fixture.
      35 => bytes(35, 1),
      # SEND_TRACE_PATH command (36), 11-byte zero-filled shape fixture.
      36 => bytes(36, 11),
      # SET_DEVICE_PIN command (37), 5-byte zero-filled shape fixture.
      37 => bytes(37, 5),
      # SET_OTHER_PARAMS command (38), 2-byte zero-filled shape fixture.
      38 => bytes(38, 2),
      # SEND_TELEMETRY_REQ command (39), 4-byte zero-filled shape fixture.
      39 => bytes(39, 4),
      # GET_CUSTOM_VARS command (40), 1-byte zero-filled shape fixture.
      40 => bytes(40, 1),
      41 => custom,
      # GET_ADVERT_PATH command (42), 34-byte zero-filled shape fixture.
      42 => bytes(42, 34),
      # GET_TUNING_PARAMS command (43), 1-byte zero-filled shape fixture.
      43 => bytes(43, 1),
      # SEND_BINARY_REQ command (50), 34-byte zero-filled shape fixture.
      50 => bytes(50, 34),
      51 => reset,
      # SEND_PATH_DISCOVERY_REQ command (52), 34-byte zero-filled shape fixture.
      52 => bytes(52, 34),
      # SET_FLOOD_SCOPE_KEY (54/0x36), mode 0 with no key: restore the configured default scope.
      54 => Bytes[54_u8, 0_u8],
      # SEND_CONTROL_DATA (55), required high flag bit 0x80 set.
      55 => Bytes[55_u8, 0x80_u8],
      # GET_STATS (56), subtype 0 selects core statistics.
      56 => Bytes[56_u8, 0_u8],
      # SEND_ANON_REQ command (57), 34-byte zero-filled shape fixture.
      57 => bytes(57, 34),
      # SET_AUTOADD_CONFIG command (58), 2-byte zero-filled shape fixture.
      58 => bytes(58, 2),
      # GET_AUTOADD_CONFIG command (59), 1-byte zero-filled shape fixture.
      59 => bytes(59, 1),
      # GET_ALLOWED_REPEAT_FREQ command (60), 1-byte zero-filled shape fixture.
      60 => bytes(60, 1),
      # SET_PATH_HASH_MODE (61), reserved 0, valid mode 2.
      61 => Bytes[61_u8, 0_u8, 2_u8],
      62 => channel_data,
      63 => scope,
      # GET_DEFAULT_FLOOD_SCOPE command (64), 1-byte zero-filled shape fixture.
      64 => bytes(64, 1),
      # SEND_RAW_PACKET command (65), 4-byte zero-filled shape fixture.
      65 => bytes(65, 4),
    }
    examples.each do |opcode, payload|
      result = P.validate_command(payload)
      result.valid?.should be_true, "opcode #{opcode}: #{result.reason}"
      result.descriptor.not_nil!.opcode.should eq(opcode)
    end
  end

  it "rejects structurally truncated and unsafe dynamic commands locally" do
    # Length probes are one byte short of a required field, or fall in a gap
    # between supported native layouts. Non-length failures are noted individually.
    # Every rejection must be local ILLEGAL_ARG, not an attempted upstream write.
    malformed = [
      # APP_START command (1), 7-byte zero-filled shape fixture. Too short; this form needs at
      # least 8 bytes.
      bytes(1, 7),
      # SEND_TXT_MSG command (2), 13-byte zero-filled shape fixture. Too short; this form needs
      # at least 14 bytes.
      bytes(2, 13),
      # SEND_CHANNEL_TXT_MSG command (3), 6-byte zero-filled shape fixture. Too short; this form
      # needs at least 7 bytes.
      bytes(3, 6),
      # GET_CONTACTS command (4), 4-byte zero-filled shape fixture. Too short; this form needs
      # at least 5 bytes.
      bytes(4, 4),
      # SET_DEVICE_TIME command (6), 4-byte zero-filled shape fixture. Too short; this form
      # needs at least 5 bytes.
      bytes(6, 4),
      # SET_ADVERT_NAME command (8), 1-byte zero-filled shape fixture. Too short; this form
      # needs at least 2 bytes.
      bytes(8, 1),
      # ADD_UPDATE_CONTACT command (9), 135-byte zero-filled shape fixture. Too short; this form
      # needs at least 136 bytes.
      bytes(9, 135),
      # ADD_UPDATE_CONTACT command (9), 137-byte zero-filled shape fixture. Invalid gap: contact
      # layouts are 136, 144, or at least 148 bytes.
      bytes(9, 137),
      # SET_RADIO_PARAMS command (11), 10-byte zero-filled shape fixture. Too short; this form
      # needs at least 11 bytes.
      bytes(11, 10),
      # SET_RADIO_TX_POWER command (12), 1-byte zero-filled shape fixture. Too short; this form
      # needs at least 2 bytes.
      bytes(12, 1),
      # RESET_PATH command (13), 32-byte zero-filled shape fixture. Too short; this form needs
      # at least 33 bytes.
      bytes(13, 32),
      # SET_ADVERT_LATLON command (14), 12-byte zero-filled shape fixture. Invalid gap: lat/lon
      # forms are 9 or at least 13 bytes.
      bytes(14, 12),
      # REMOVE_CONTACT command (15), 32-byte zero-filled shape fixture. Too short; this form
      # needs at least 33 bytes.
      bytes(15, 32),
      # SHARE_CONTACT command (16), 32-byte zero-filled shape fixture. Too short; this form
      # needs at least 33 bytes.
      bytes(16, 32),
      # EXPORT_CONTACT command (17), 32-byte zero-filled shape fixture. Too short; this form
      # needs at least 33 bytes.
      bytes(17, 32),
      # IMPORT_CONTACT command (18), 98-byte zero-filled shape fixture. Too short; this form
      # needs at least 99 bytes.
      bytes(18, 98),
      # REBOOT (19) with intentionally misspelled magic text; must be rejected.
      Bytes[19_u8] + "reb00t".to_slice,
      # SET_TUNING_PARAMS command (21), 8-byte zero-filled shape fixture. Too short; this form
      # needs at least 9 bytes.
      bytes(21, 8),
      # DEVICE_QUERY command (22), 1-byte zero-filled shape fixture. Too short; this form needs
      # at least 2 bytes.
      bytes(22, 1),
      # IMPORT_PRIVATE_KEY command (24), 64-byte zero-filled shape fixture. Too short; this form
      # needs at least 65 bytes.
      bytes(24, 64),
      # SEND_RAW_DATA (25) claims four path bytes but omits the required four-byte data tail.
      Bytes[25_u8, 4_u8, 1_u8, 2_u8, 3_u8, 4_u8],
      # SEND_LOGIN command (26), 32-byte zero-filled shape fixture. Too short; this form needs
      # at least 33 bytes.
      bytes(26, 32),
      # SEND_STATUS_REQ command (27), 32-byte zero-filled shape fixture. Too short; this form
      # needs at least 33 bytes.
      bytes(27, 32),
      # HAS_CONNECTION command (28), 32-byte zero-filled shape fixture. Too short; this form
      # needs at least 33 bytes.
      bytes(28, 32),
      # LOGOUT command (29), 32-byte zero-filled shape fixture. Too short; this form needs at
      # least 33 bytes.
      bytes(29, 32),
      # GET_CONTACT_BY_KEY command (30), 32-byte zero-filled shape fixture. Too short; this form
      # needs at least 33 bytes.
      bytes(30, 32),
      # GET_CHANNEL command (31), 1-byte zero-filled shape fixture. Too short; this form needs
      # at least 2 bytes.
      bytes(31, 1),
      # SET_CHANNEL command (32), 49-byte zero-filled shape fixture. Too short; this form needs
      # at least 50 bytes.
      bytes(32, 49),
      # SET_CHANNEL command (32), 66-byte zero-filled shape fixture. Too long: SET_CHANNEL must
      # be under 66 bytes.
      bytes(32, 66),
      # SIGN_DATA command (34), 1-byte zero-filled shape fixture. Too short; this form needs at
      # least 2 bytes.
      bytes(34, 1),
      # SEND_TRACE_PATH command (36), 10-byte zero-filled shape fixture. Too short; this form
      # needs at least 11 bytes.
      bytes(36, 10),
      # SET_DEVICE_PIN command (37), 4-byte zero-filled shape fixture. Too short; this form
      # needs at least 5 bytes.
      bytes(37, 4),
      # SET_OTHER_PARAMS command (38), 1-byte zero-filled shape fixture. Too short; this form
      # needs at least 2 bytes.
      bytes(38, 1),
      # SEND_TELEMETRY_REQ command (39), 35-byte zero-filled shape fixture. Too short for remote
      # (36 bytes), too long for self (4).
      bytes(39, 35),
      # SET_CUSTOM_VAR (41) with "abc": deliberately missing the required colon.
      Bytes[41_u8, 'a'.ord.to_u8, 'b'.ord.to_u8, 'c'.ord.to_u8],
      # GET_ADVERT_PATH command (42), 33-byte zero-filled shape fixture. Too short; this form
      # needs at least 34 bytes.
      bytes(42, 33),
      # SEND_BINARY_REQ command (50), 33-byte zero-filled shape fixture. Too short; this form
      # needs at least 34 bytes.
      bytes(50, 33),
      # FACTORY_RESET (51) with misspelled magic text; must be rejected.
      Bytes[51_u8] + "resat".to_slice,
      # PATH_DISCOVERY request (52) with illegal reserved byte 1, followed by a dummy public
      # key.
      # Synthetic 32-byte peer key; the preceding nonzero reserved field makes this command
      # invalid.
      Bytes[52_u8, 1_u8] + Bytes.new(32),
      # SET_FLOOD_SCOPE_KEY (54), invalid mode 2: reject locally.
      Bytes[54_u8, 2_u8],
      # SEND_CONTROL_DATA (55), missing the required high flag bit: invalid.
      Bytes[55_u8, 0_u8],
      # GET_STATS (56), subtype 3 is invalid (only 0..2 exist).
      Bytes[56_u8, 3_u8],
      # SEND_ANON_REQ command (57), 33-byte zero-filled shape fixture. Too short; this form
      # needs at least 34 bytes.
      bytes(57, 33),
      # SET_AUTOADD_CONFIG command (58), 1-byte zero-filled shape fixture. Too short; this form
      # needs at least 2 bytes.
      bytes(58, 1),
      # SET_PATH_HASH_MODE (61), invalid reserved byte 1.
      Bytes[61_u8, 1_u8, 0_u8],
      # SEND_CHANNEL_DATA (62) claims two path bytes but leaves no two-byte data type:
      # truncated.
      Bytes[62_u8, 0_u8, 2_u8, 0_u8, 1_u8],
      # SET_DEFAULT_FLOOD_SCOPE command (63), 47-byte zero-filled shape fixture. Named scope
      # requires exactly 48 bytes (opcode, 31-byte name, 16-byte key).
      bytes(63, 47),
      # SEND_RAW_PACKET command (65), 3-byte zero-filled shape fixture. Too short; this form
      # needs at least 4 bytes.
      bytes(65, 3),
    ]
    malformed.each do |payload|
      result = P.validate_command(payload)
      result.valid?.should be_false, "opcode #{payload[0]} was accepted"
      result.reason.should eq(P::ERR_ILLEGAL_ARG)
    end
  end

  it "validates dynamic path, scope, contact, and telemetry forms" do
    # Two one-byte path hashes followed by a two-byte data type.
    # SEND_CHANNEL_DATA (62), channel 0, two one-byte hashes aa bb, data type 1 (u16 LE).
    P.validate_command(Bytes[62_u8, 0_u8, 2_u8, 0xaa_u8, 0xbb_u8, 1_u8, 0_u8]).valid?.should be_true
    # ADD_UPDATE_CONTACT command (9), 144-byte zero-filled shape fixture.
    P.validate_command(bytes(9, 144)).valid?.should be_true
    # ADD_UPDATE_CONTACT command (9), 148-byte zero-filled shape fixture.
    P.validate_command(bytes(9, 148)).valid?.should be_true
    # SEND_TELEMETRY_REQ command (39), 4-byte zero-filled shape fixture.
    self_telemetry = P.validate_command(bytes(39, 4))
    self_telemetry.descriptor.not_nil!.grammar.should eq(P::Grammar::SelfTelemetry)
    # SEND_TELEMETRY_REQ command (39), 36-byte zero-filled shape fixture.
    remote_telemetry = P.validate_command(bytes(39, 36))
    remote_telemetry.valid?.should be_true
    remote_telemetry.descriptor.not_nil!.flags.includes?(P::CommandFlags::RemoteLease).should be_true
    # SET_FLOOD_SCOPE_KEY (54), mode 0 plus a synthetic 16-byte explicit scope key.
    # Synthetic 16-byte scope key, appended after opcode 54 and mode 0.
    P.validate_command(Bytes[54_u8, 0_u8] + Bytes.new(16)).valid?.should be_true
    # SET_FLOOD_SCOPE_KEY (54/0x36), mode 1: explicitly unscoped sends.
    P.validate_command(Bytes[54_u8, 1_u8]).valid?.should be_true
  end

  it "validates fixed, streamed, echoed, and extensible upstream responses" do
    clock = P::DESCRIPTORS[5_u8]
    # CURRENT_TIME response (9), 5-byte zero-filled shape fixture.
    P.validate_response!(clock, bytes(9, 5)).should eq(P::ResponseDisposition::Complete)
    # CURRENT_TIME response (9), 4-byte zero-filled shape fixture.
    expect_raises(P::ProtocolError) { P.validate_response!(clock, bytes(9, 4)) }
    # OK (0x00): command accepted, not proof of radio delivery.
    expect_raises(P::ProtocolError) { P.validate_response!(clock, Bytes[0_u8]) }
    # ERR (0x01), opaque native reason 99.
    P.validate_response!(clock, Bytes[1_u8, 99_u8]).should eq(P::ResponseDisposition::Complete)

    contacts = P::DESCRIPTORS[4_u8]
    # CONTACTS_START response (2), 5-byte zero-filled shape fixture.
    P.validate_response!(contacts, bytes(2, 5)).should eq(P::ResponseDisposition::Progress)
    # CONTACT response (3), 148-byte zero-filled shape fixture.
    P.validate_response!(contacts, bytes(3, 148), phase: 1).should eq(P::ResponseDisposition::Progress)
    # END_OF_CONTACTS response (4), 5-byte zero-filled shape fixture.
    P.validate_response!(contacts, bytes(4, 5), phase: 1).should eq(P::ResponseDisposition::Complete)
    # CONTACT response (3), 148-byte zero-filled shape fixture.
    expect_raises(P::ProtocolError) { P.validate_response!(contacts, bytes(3, 148)) }
    # CONTACTS_START response (2), 5-byte zero-filled shape fixture.
    expect_raises(P::ProtocolError) { P.validate_response!(contacts, bytes(2, 5), phase: 1) }

    # GET_CHANNEL (31), channel index 3; the response must echo this index.
    channel_command = Bytes[31_u8, 3_u8]
    # CHANNEL_INFO response (0x12), 50-byte zero-filled shape fixture.
    channel_response = bytes(0x12, 50); channel_response[1] = 3
    P.validate_response!(P::DESCRIPTORS[31_u8], channel_response, channel_command)
    channel_response[1] = 2
    expect_raises(P::ProtocolError) { P.validate_response!(P::DESCRIPTORS[31_u8], channel_response, channel_command) }

    # GET_STATS (56), subtype 1 selects radio statistics.
    stats_command = Bytes[56_u8, 1_u8]
    # STATS response (0x18), 14-byte zero-filled shape fixture.
    stats_response = bytes(0x18, 14); stats_response[1] = 1
    P.validate_response!(P::DESCRIPTORS[56_u8], stats_response, stats_command)
    # ALLOWED_REPEAT_FREQ (0x1a) with a partial eight-byte range record: malformed.
    expect_raises(P::ProtocolError) { P.validate_response_shape!(Bytes[0x1a_u8, 0_u8]) }
    # Unknown ordinary response 0x2f: fatal, unlike extensible high-code pushes.
    expect_raises(P::ProtocolError) { P.validate_response_shape!(Bytes[0x2f_u8]) }
    # Unknown high-code push 0xfe with opaque body 1: preserve for forward compatibility.
    P.validate_response_shape!(Bytes[0xfe_u8, 1_u8])

    # ADVERT_PATH response (0x16), 8-byte zero-filled shape fixture.
    advert_path = bytes(0x16, 8); advert_path[5] = 2
    P.validate_response_shape!(advert_path)
    expect_raises(P::ProtocolError) { P.validate_response_shape!(advert_path[0, 7]) }

    # PATH_DISCOVERY_RESPONSE response (0x8d), 12-byte zero-filled shape fixture.
    discovery = bytes(0x8d, 12)
    discovery[8] = 1; discovery[10] = 1
    P.validate_response_shape!(discovery)
    expect_raises(P::ProtocolError) { P.validate_response_shape!(discovery[0, 11]) }
  end

  it "covers every declared success and terminal error response grammar" do
    # Minimal valid success bodies, reused across descriptors that share a reply
    # code. Contacts are tested separately because they have a multi-frame grammar.
    samples = {
      # OK response (0x00), 1-byte zero-filled shape fixture.
      0x00_u8 => bytes(0x00, 1),
      # SELF_INFO response (0x05), 58-byte zero-filled shape fixture.
      0x05_u8 => bytes(0x05, 58),
      # CONTACT response (0x03), 148-byte zero-filled shape fixture.
      0x03_u8 => bytes(0x03, 148),
      # SENT response (0x06), 10-byte zero-filled shape fixture.
      0x06_u8 => bytes(0x06, 10),
      # CONTACT_MESSAGE response (0x07), 13-byte zero-filled shape fixture.
      0x07_u8 => bytes(0x07, 13),
      # CHANNEL_MESSAGE response (0x08), 8-byte zero-filled shape fixture.
      0x08_u8 => bytes(0x08, 8),
      # CURRENT_TIME response (0x09), 5-byte zero-filled shape fixture.
      0x09_u8 => bytes(0x09, 5),
      # NO_MORE_MESSAGES response (0x0a), 1-byte zero-filled shape fixture.
      0x0a_u8 => bytes(0x0a, 1),
      # EXPORT_CONTACT response (0x0b), 2-byte zero-filled shape fixture.
      0x0b_u8 => bytes(0x0b, 2),
      # BATTERY_AND_STORAGE response (0x0c), 11-byte zero-filled shape fixture.
      0x0c_u8 => bytes(0x0c, 11),
      # DEVICE_INFO response (0x0d), 82-byte zero-filled shape fixture.
      0x0d_u8 => bytes(0x0d, 82),
      # PRIVATE_KEY response (0x0e), 65-byte zero-filled shape fixture.
      0x0e_u8 => bytes(0x0e, 65),
      # DISABLED response (0x0f), 1-byte zero-filled shape fixture.
      0x0f_u8 => bytes(0x0f, 1),
      # CONTACT_MESSAGE_V3 response (0x10), 16-byte zero-filled shape fixture.
      0x10_u8 => bytes(0x10, 16),
      # CHANNEL_MESSAGE_V3 response (0x11), 11-byte zero-filled shape fixture.
      0x11_u8 => bytes(0x11, 11),
      # CHANNEL_INFO response (0x12), 50-byte zero-filled shape fixture.
      0x12_u8 => bytes(0x12, 50),
      # SIGN_START response (0x13), 6-byte zero-filled shape fixture.
      0x13_u8 => bytes(0x13, 6),
      # SIGNATURE response (0x14), 65-byte zero-filled shape fixture.
      0x14_u8 => bytes(0x14, 65),
      # CUSTOM_VARS response (0x15), 1-byte zero-filled shape fixture.
      0x15_u8 => bytes(0x15, 1),
      # ADVERT_PATH response (0x16), 6-byte zero-filled shape fixture.
      0x16_u8 => bytes(0x16, 6),
      # TUNING_PARAMS response (0x17), 9-byte zero-filled shape fixture.
      0x17_u8 => bytes(0x17, 9),
      # STATS response (0x18), 11-byte zero-filled shape fixture.
      0x18_u8 => bytes(0x18, 11),
      # AUTOADD_CONFIG response (0x19), 3-byte zero-filled shape fixture.
      0x19_u8 => bytes(0x19, 3),
      # ALLOWED_REPEAT_FREQ response (0x1a), 1-byte zero-filled shape fixture.
      0x1a_u8 => bytes(0x1a, 1),
      # CHANNEL_DATA response (0x1b), 9-byte zero-filled shape fixture.
      0x1b_u8 => bytes(0x1b, 9),
      # DEFAULT_FLOOD_SCOPE response (0x1c), 1-byte zero-filled shape fixture.
      0x1c_u8 => bytes(0x1c, 1),
      # TELEMETRY_RESPONSE response (0x8b), 8-byte zero-filled shape fixture.
      0x8b_u8 => bytes(0x8b, 8),
    }
    samples[0x18_u8][1] = 0
    samples[0x1b_u8][8] = 0

    P::DESCRIPTORS.each_value do |descriptor|
      next if descriptor.grammar.contacts? || descriptor.success_codes.empty?
      descriptor.success_codes.each do |code|
        P.validate_response!(descriptor, samples[code]).should eq(P::ResponseDisposition::Complete)
      end
      # ERR (0x01), opaque native reason 77.
      P.validate_response!(descriptor, Bytes[1_u8, 77_u8]).should eq(P::ResponseDisposition::Complete)
    end
  end

  it "validates inbox message bodies and downgrades V3 text without changing the body" do
    # CONTACT_MESSAGE_V3 (0x10): SNR byte 0xf0 plus two metadata bytes, then the legacy
    # fields/body.
    # Twelve distinct synthetic legacy DM header bytes make accidental body/metadata rewriting
    # detectable.
    contact = Bytes[0x10_u8, 0xf0_u8, 1_u8, 2_u8] + Bytes.new(12) { |i| (i + 10).to_u8 }
    downgraded = P.downgrade_inbox(contact, 0_u8)
    # CONTACT_MESSAGE (0x07) legacy opcode; append the original body after stripping V3
    # metadata.
    downgraded.should eq(Bytes[0x07_u8] + contact[4..])
    P.downgrade_inbox(contact, 3_u8).should eq(contact)

    # CHANNEL_MESSAGE_V3 (0x11): SNR byte 0xf0 plus two metadata bytes, then the legacy
    # fields/body.
    # Seven-byte legacy channel header filled with sentinel 9; conversion must preserve it
    # exactly.
    channel = Bytes[0x11_u8, 0xf0_u8, 1_u8, 2_u8] + Bytes.new(7, 9_u8)
    # CHANNEL_MESSAGE (0x08) legacy opcode; append the original body after stripping V3
    # metadata.
    P.downgrade_inbox(channel, 2_u8).should eq(Bytes[0x08_u8] + channel[4..])

    # CHANNEL_DATA (0x1b): eight metadata/header bytes after opcode; byte 8 declares the
    # trailing binary-body length.
    datagram = Bytes[0x1b_u8, 0_u8, 0xff_u8, 1_u8, 0_u8, 0_u8, 0_u8, 0_u8, 2_u8, 7_u8, 8_u8]
    P.downgrade_inbox(datagram, 0_u8).should eq(datagram)
    expect_raises(P::ProtocolError) { P.validate_response_shape!(datagram[0, 10]) }
  end

  it "builds handshakes, normalizes query targets, and validates identity" do
    start = P.app_start_payload("test")
    # APP_START opcode; append seven reserved bytes and the application name.
    # Seven reserved APP_START bytes; application-name text follows them.
    start.should eq(Bytes[1_u8] + Bytes.new(7, 0_u8) + "test".to_slice)
    P.validate_command(start).valid?.should be_true
    # Deliberately short APP_START reserved field: six bytes rather than the required seven.
    expect_raises(ArgumentError) { P.app_start_payload("x", Bytes.new(6)) }

    # DEVICE_QUERY (22), requested protocol target 13.
    P.device_query_payload.should eq(Bytes[22_u8, 13_u8])
    # DEVICE_QUERY (22), requested protocol target 2; trailing sentinel must survive
    # normalization.
    # DEVICE_QUERY (22), requested protocol target 13; trailing sentinel must survive
    # normalization.
    P.normalize_device_query(Bytes[22_u8, 2_u8, 99_u8]).should eq(Bytes[22_u8, 13_u8, 99_u8])

    # SELF_INFO response (5), 58-byte zero-filled shape fixture.
    self_info = bytes(5, 58)
    32.times { |i| self_info[4 + i] = i.to_u8 }
    # Expected synthetic public key 00..1f, extracted from SELF_INFO offsets 4..35.
    P.validate_self_info!(self_info).should eq(Bytes.new(32) { |i| i.to_u8 })
    # SELF_INFO response (5), 57-byte zero-filled shape fixture.
    expect_raises(P::ProtocolError) { P.validate_self_info!(bytes(5, 57)) }

    # DEVICE_INFO response (0x0d), 82-byte zero-filled shape fixture.
    device = bytes(0x0d, 82); device[1] = 13
    P.validate_device_info!(device).should eq(13_u8)
    device[1] = 12
    expect_raises(P::ProtocolError) { P.validate_device_info!(device) }
  end

  it "validates trace response variable fields exactly" do
    # TRACE_DATA response (0x89), 16-byte zero-filled shape fixture.
    trace = bytes(0x89, 16)
    trace[2] = 2 # two hash bytes
    trace[3] = 1 # Two-byte hashes: the two path bytes represent one hop.
    P.validate_response_shape!(trace)
    expect_raises(P::ProtocolError) { P.validate_response_shape!(trace[0, 15]) }
  end
end
