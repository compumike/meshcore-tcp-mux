require "./spec_helper"
require "../src/meshcore_tcp_mux/protocol"

private alias LoggingProtocol = MeshCoreTCPMux::Protocol

private def logging_trace(width_shift : UInt8, hops : Int32) : Bytes
  # TRACE_DATA has a 12-byte prefix, path hashes, one SNR per hop, and a final
  # SNR. Trace widths are powers of two, unlike ordinary 1/2/3-byte paths.
  path_bytes = hops << width_shift
  Bytes.new(12 + path_bytes + hops + 1, 0_u8).tap do |payload|
    payload[0] = LoggingProtocol::PUSH_TRACE_DATA
    payload[2] = path_bytes.to_u8
    payload[3] = width_shift
    payload[4] = 0x44_u8 # Synthetic little-endian tag 0x44.
    payload[8] = 0x55_u8 # Synthetic little-endian authentication 0x55.
    path_bytes.times { |index| payload[12 + index] = (index + 1).to_u8 }
    (hops + 1).times { |index| payload[12 + path_bytes + index] = (0x80 + index).to_u8 }
  end
end

describe "protocol wire logging" do
  # Debug descriptions include complete payload hex and decoded native fields.
  # Parsing must remain safe because descriptions precede validation in runtime.

  it "includes complete raw command and response payloads" do
    command = Bytes.new(65, 0x53_u8).tap { |p| p[0] = LoggingProtocol::CMD_IMPORT_PRIVATE_KEY }
    response = Bytes.new(65, 0x54_u8).tap { |p| p[0] = LoggingProtocol::RESP_PRIVATE_KEY }
    LoggingProtocol.describe_command(command, include_payload: true)
      .should contain("payload=#{LoggingProtocol.hex(command)}")
    LoggingProtocol.describe_response(response, include_payload: true)
      .should contain("payload=#{LoggingProtocol.hex(response)}")
    LoggingProtocol.describe_command(Bytes[0xfe_u8, 0xaa_u8], include_payload: true)
      .should contain("payload=feaa")
  end

  it "decodes outgoing direct and channel text fields" do
    direct = Bytes[LoggingProtocol::CMD_SEND_TXT_MSG, 0_u8, 3_u8,
      0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8,
      1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8] + "hello".to_slice
    log = LoggingProtocol.describe_command(direct)
    log.should contain("attempt=3")
    log.should contain("timestamp=305419896")
    log.should contain("destination=010203040506")
    log.should contain(%(text="hello"))

    channel = Bytes[LoggingProtocol::CMD_SEND_CHANNEL_TXT_MSG, 0_u8, 7_u8,
      1_u8, 0_u8, 0_u8, 0_u8] + "room".to_slice
    log = LoggingProtocol.describe_command(channel)
    log.should contain("channel=7")
    log.should contain("timestamp=1")
    log.should contain(%(text="room"))
  end

  it "decodes contact names, coordinates, and ordinary 1/2/3-byte paths" do
    contact = Bytes.new(148, 0_u8)
    contact[0] = LoggingProtocol::CMD_ADD_UPDATE_CONTACT
    contact[1, 32].fill(0x11_u8) # Synthetic public contact key.
    contact[33] = 2_u8           # Synthetic contact type.
    contact[34] = 3_u8           # Synthetic contact flags.
    contact[35] = 0x41_u8        # One two-byte path hash.
    contact[36, 2].copy_from(Bytes[0xaa_u8, 0xbb_u8])
    contact[100, 4].copy_from("peer".to_slice)
    contact[136, 4].copy_from(Bytes[1_u8, 0_u8, 0_u8, 0_u8])
    contact[140, 4].copy_from(Bytes[0xff_u8, 0xff_u8, 0xff_u8, 0xff_u8])
    contact[144, 4].copy_from(Bytes[2_u8, 0_u8, 0_u8, 0_u8])
    log = LoggingProtocol.describe_command(contact)
    log.should contain("latitude_microdegrees=1 longitude_microdegrees=-1")
    log.should contain("last_modified=2")
    log.should contain("path_hashes=1 hash_width=2 path=aabb")

    # CONTACT and NEW_ADVERT carry the same native contact record layout. The
    # fixed name must stop at its first NUL rather than logging all padding.
    contact[0] = LoggingProtocol::PUSH_NEW_ADVERT
    response_log = LoggingProtocol.describe_response(contact)
    response_log.should contain(%(name="peer"))
    response_log.should_not contain("\\u0000")
    response_log.should contain("latitude_microdegrees=1 longitude_microdegrees=-1 last_modified=2")

    {0x01_u8 => 1, 0x41_u8 => 2, 0x81_u8 => 3}.each do |encoded, width|
      response = Bytes[LoggingProtocol::RESP_ADVERT_PATH, 0_u8, 0_u8, 0_u8, 0_u8, encoded] +
                 Bytes.new(width) { |index| (0xa0 + index).to_u8 }
      LoggingProtocol.validate_response_shape!(response)
      path_log = LoggingProtocol.describe_response(response)
      path_log.should contain("path_hashes=1 hash_width=#{width}")
      path_log.should contain("path=#{LoggingProtocol.hex(response[6..])}")
    end
  end

  it "decodes complete self, device, and channel information records" do
    self_info = Bytes.new(62, 0_u8)
    self_info[0] = LoggingProtocol::RESP_SELF_INFO
    self_info[1] = 2_u8
    self_info[2] = 20_u8
    self_info[3] = 30_u8
    self_info[4, 32].fill(0x11_u8) # Synthetic node public key.
    self_info[36, 4].copy_from(Bytes[1_u8, 0_u8, 0_u8, 0_u8])
    self_info[40, 4].copy_from(Bytes[0xff_u8, 0xff_u8, 0xff_u8, 0xff_u8])
    self_info[48, 4].copy_from(Bytes[0x40_u8, 0x42_u8, 0x0f_u8, 0_u8])
    self_info[52, 4].copy_from(Bytes[0x20_u8, 0xa1_u8, 0x07_u8, 0_u8])
    self_info[56] = 9_u8
    self_info[57] = 5_u8
    self_info[58, 4].copy_from("node".to_slice)
    self_log = LoggingProtocol.describe_response(self_info)
    self_log.should contain("public_key=#{"11" * 32}")
    self_log.should contain("latitude_microdegrees=1 longitude_microdegrees=-1")
    self_log.should contain(%(name="node"))

    device = Bytes.new(82, 0_u8)
    device[0] = LoggingProtocol::RESP_DEVICE_INFO
    device[1] = 13_u8
    device[4, 4].copy_from(Bytes[0x78_u8, 0x56_u8, 0x34_u8, 0x12_u8])
    device[8, 5].copy_from("build".to_slice)
    device[20, 5].copy_from("model".to_slice)
    device[60, 8].copy_from("firmware".to_slice)
    device_log = LoggingProtocol.describe_response(device)
    device_log.should contain("pin=305419896")
    device_log.should contain(%(build="build" model="model" firmware="firmware"))
    device_log.should_not contain("\\u0000")

    channel = Bytes.new(50, 0_u8)
    channel[0] = LoggingProtocol::RESP_CHANNEL_INFO
    channel[1] = 7_u8
    channel[2, 4].copy_from("room".to_slice)
    channel_log = LoggingProtocol.describe_response(channel)
    channel_log.should contain(%(channel=7 name="room"))
    channel_log.should_not contain("\\u0000")
  end

  it "decodes legacy and V3 inbox text using their distinct offsets" do
    legacy_dm = Bytes[LoggingProtocol::RESP_CONTACT_MESSAGE,
      1_u8, 2_u8, 3_u8, 4_u8, 5_u8, 6_u8, 0xff_u8, 0_u8,
      2_u8, 0_u8, 0_u8, 0_u8] + "dm".to_slice
    log = LoggingProtocol.describe_response(legacy_dm)
    log.should contain("sender=010203040506")
    log.should contain("timestamp=2")
    log.should contain(%(text="dm"))

    v3_channel = Bytes[LoggingProtocol::RESP_CHANNEL_MESSAGE_V3,
      0xfc_u8, 0_u8, 0_u8, 9_u8, 0xff_u8, 0_u8,
      3_u8, 0_u8, 0_u8, 0_u8] + "channel".to_slice
    log = LoggingProtocol.describe_response(v3_channel)
    log.should contain("snr_quarters=-4")
    log.should contain("channel=9")
    log.should contain("timestamp=3")
    log.should contain(%(text="channel"))
  end

  it "validates and decodes every firmware trace hash width" do
    # Trace shifts 0, 1, 2, and 3 represent 1-, 2-, 4-, and 8-byte hashes.
    {0_u8 => 1, 1_u8 => 2, 2_u8 => 4, 3_u8 => 8}.each do |shift, width|
      response = logging_trace(shift, 2)
      LoggingProtocol.validate_response_shape!(response)
      log = LoggingProtocol.describe_response(response)
      log.should contain("tag=68 auth=85 hash_width=#{width}")
      log.should contain("path_bytes=#{2 * width}")
      log.should contain("hashes=#{LoggingProtocol.hex(response[12, 2 * width])}")
      log.should contain("snr_quarters=808182")
    end
  end

  it "decodes raw packet metadata and safely describes truncated prefixes" do
    raw = Bytes[LoggingProtocol::PUSH_RAW_DATA, 0xfc_u8, 0xa0_u8, 0x41_u8, 0xde_u8, 0xad_u8]
    LoggingProtocol.describe_response(raw).should contain(
      "snr_quarters=-4 rssi=-96 path_encoding=0x41 data=dead"
    )

    samples = [
      Bytes[LoggingProtocol::CMD_SEND_TXT_MSG, 0_u8, 1_u8, 2_u8],
      Bytes[LoggingProtocol::CMD_ADD_UPDATE_CONTACT, 1_u8, 2_u8],
      Bytes[LoggingProtocol::PUSH_TRACE_DATA, 0_u8, 4_u8],
      Bytes[LoggingProtocol::RESP_CONTACT_MESSAGE_V3, 0xfc_u8],
    ]
    samples.each do |sample|
      (0..sample.size).each do |size|
        LoggingProtocol.describe_command(sample[0, size], include_payload: true)
        LoggingProtocol.describe_response(sample[0, size], include_payload: true)
      end
    end

    invalid_utf8 = Bytes[LoggingProtocol::CMD_SET_ADVERT_NAME, 0xff_u8, 0xc0_u8, 0x80_u8]
    LoggingProtocol.describe_command(invalid_utf8).should contain("name=")
    LoggingProtocol.describe_command(invalid_utf8, include_payload: true).should contain("payload=08ffc080")
  end
end
