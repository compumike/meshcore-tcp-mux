class MeshCoreTCPMux
  # Namespace for the TCP multiplexer: transport, protocol validation, and per-client state.
  class Protocol
    # Describes the supported native companion wire protocol. The broker uses these
    # rules to reject malformed commands and recognize complete, correctly owned replies.
    # Native v13 caps decoded payloads at 176 bytes and ordinary encoded paths at 64 bytes.
    MAX_PAYLOAD           =     176
    NATIVE_PROTOCOL_LEVEL =   13_u8
    MAX_PATH_SIZE         =      64
    NO_PATH_ENCODING      = 0xff_u8
    PATH_COUNT_MASK       = 0x3f_u8
    PATH_WIDTH_SHIFT_MASK = 0x03_u8

    ERR_UNSUPPORTED_CMD = 1_u8
    ERR_TABLE_FULL      = 3_u8
    ERR_BAD_STATE       = 4_u8
    ERR_ILLEGAL_ARG     = 6_u8

    STATS_TYPE_CORE   = 0_u8
    STATS_TYPE_RADIO  = 1_u8
    STATS_TYPE_PACKET = 2_u8

    CMD_APP_START               =  1_u8
    CMD_SEND_TXT_MSG            =  2_u8
    CMD_SEND_CHANNEL_TXT_MSG    =  3_u8
    CMD_GET_CONTACTS            =  4_u8
    CMD_GET_DEVICE_TIME         =  5_u8
    CMD_SET_DEVICE_TIME         =  6_u8
    CMD_SEND_SELF_ADVERT        =  7_u8
    CMD_SET_ADVERT_NAME         =  8_u8
    CMD_ADD_UPDATE_CONTACT      =  9_u8
    CMD_SYNC_NEXT_MESSAGE       = 10_u8
    CMD_SET_RADIO_PARAMS        = 11_u8
    CMD_SET_RADIO_TX_POWER      = 12_u8
    CMD_RESET_PATH              = 13_u8
    CMD_SET_ADVERT_LATLON       = 14_u8
    CMD_REMOVE_CONTACT          = 15_u8
    CMD_SHARE_CONTACT           = 16_u8
    CMD_EXPORT_CONTACT          = 17_u8
    CMD_IMPORT_CONTACT          = 18_u8
    CMD_REBOOT                  = 19_u8
    CMD_GET_BATT_AND_STORAGE    = 20_u8
    CMD_SET_TUNING_PARAMS       = 21_u8
    CMD_DEVICE_QUERY            = 22_u8
    CMD_EXPORT_PRIVATE_KEY      = 23_u8
    CMD_IMPORT_PRIVATE_KEY      = 24_u8
    CMD_SEND_RAW_DATA           = 25_u8
    CMD_SEND_LOGIN              = 26_u8
    CMD_SEND_STATUS_REQ         = 27_u8
    CMD_HAS_CONNECTION          = 28_u8
    CMD_LOGOUT                  = 29_u8
    CMD_GET_CONTACT_BY_KEY      = 30_u8
    CMD_GET_CHANNEL             = 31_u8
    CMD_SET_CHANNEL             = 32_u8
    CMD_SIGN_START              = 33_u8
    CMD_SIGN_DATA               = 34_u8
    CMD_SIGN_FINISH             = 35_u8
    CMD_SEND_TRACE_PATH         = 36_u8
    CMD_SET_DEVICE_PIN          = 37_u8
    CMD_SET_OTHER_PARAMS        = 38_u8
    CMD_SEND_TELEMETRY_REQ      = 39_u8
    CMD_GET_CUSTOM_VARS         = 40_u8
    CMD_SET_CUSTOM_VAR          = 41_u8
    CMD_GET_ADVERT_PATH         = 42_u8
    CMD_GET_TUNING_PARAMS       = 43_u8
    CMD_SEND_BINARY_REQ         = 50_u8
    CMD_FACTORY_RESET           = 51_u8
    CMD_SEND_PATH_DISCOVERY_REQ = 52_u8
    CMD_SET_FLOOD_SCOPE_KEY     = 54_u8
    CMD_SEND_CONTROL_DATA       = 55_u8
    CMD_GET_STATS               = 56_u8
    CMD_SEND_ANON_REQ           = 57_u8
    CMD_SET_AUTOADD_CONFIG      = 58_u8
    CMD_GET_AUTOADD_CONFIG      = 59_u8
    CMD_GET_ALLOWED_REPEAT_FREQ = 60_u8
    CMD_SET_PATH_HASH_MODE      = 61_u8
    CMD_SEND_CHANNEL_DATA       = 62_u8
    CMD_SET_DEFAULT_FLOOD_SCOPE = 63_u8
    CMD_GET_DEFAULT_FLOOD_SCOPE = 64_u8
    CMD_SEND_RAW_PACKET         = 65_u8

    RESP_OK                  = 0x00_u8
    RESP_ERR                 = 0x01_u8
    RESP_CONTACTS_START      = 0x02_u8
    RESP_CONTACT             = 0x03_u8
    RESP_END_OF_CONTACTS     = 0x04_u8
    RESP_SELF_INFO           = 0x05_u8
    RESP_SENT                = 0x06_u8
    RESP_CONTACT_MESSAGE     = 0x07_u8
    RESP_CHANNEL_MESSAGE     = 0x08_u8
    RESP_CURRENT_TIME        = 0x09_u8
    RESP_NO_MORE_MESSAGES    = 0x0a_u8
    RESP_EXPORT_CONTACT      = 0x0b_u8
    RESP_BATTERY_AND_STORAGE = 0x0c_u8
    RESP_DEVICE_INFO         = 0x0d_u8
    RESP_PRIVATE_KEY         = 0x0e_u8
    RESP_DISABLED            = 0x0f_u8
    RESP_CONTACT_MESSAGE_V3  = 0x10_u8
    RESP_CHANNEL_MESSAGE_V3  = 0x11_u8
    RESP_CHANNEL_INFO        = 0x12_u8
    RESP_SIGN_START          = 0x13_u8
    RESP_SIGNATURE           = 0x14_u8
    RESP_CUSTOM_VARS         = 0x15_u8
    RESP_ADVERT_PATH         = 0x16_u8
    RESP_TUNING_PARAMS       = 0x17_u8
    RESP_STATS               = 0x18_u8
    RESP_AUTOADD_CONFIG      = 0x19_u8
    RESP_ALLOWED_REPEAT_FREQ = 0x1a_u8
    RESP_CHANNEL_DATA        = 0x1b_u8
    RESP_DEFAULT_FLOOD_SCOPE = 0x1c_u8

    PUSH_ADVERT                  = 0x80_u8
    PUSH_PATH_UPDATED            = 0x81_u8
    PUSH_SEND_CONFIRMED          = 0x82_u8
    PUSH_MSG_WAITING             = 0x83_u8
    PUSH_RAW_DATA                = 0x84_u8
    PUSH_LOGIN_SUCCESS           = 0x85_u8
    PUSH_LOGIN_FAILURE           = 0x86_u8
    PUSH_STATUS_RESPONSE         = 0x87_u8
    PUSH_LOG_RX_DATA             = 0x88_u8
    PUSH_TRACE_DATA              = 0x89_u8
    PUSH_NEW_ADVERT              = 0x8a_u8
    PUSH_TELEMETRY_RESPONSE      = 0x8b_u8
    PUSH_BINARY_RESPONSE         = 0x8c_u8
    PUSH_PATH_DISCOVERY_RESPONSE = 0x8d_u8
    PUSH_CONTROL_DATA            = 0x8e_u8
    PUSH_CONTACT_DELETED         = 0x8f_u8
    PUSH_CONTACTS_FULL           = 0x90_u8

    RESPONSE_NAMES = {
      RESP_OK => "ok", RESP_ERR => "err", RESP_CONTACTS_START => "contacts_start",
      RESP_CONTACT => "contact", RESP_END_OF_CONTACTS => "end_of_contacts",
      RESP_SELF_INFO => "self_info", RESP_SENT => "sent",
      RESP_CONTACT_MESSAGE => "contact_message", RESP_CHANNEL_MESSAGE => "channel_message",
      RESP_CURRENT_TIME => "current_time", RESP_NO_MORE_MESSAGES => "no_more_messages",
      RESP_EXPORT_CONTACT => "export_contact", RESP_BATTERY_AND_STORAGE => "battery_and_storage",
      RESP_DEVICE_INFO => "device_info", RESP_PRIVATE_KEY => "private_key",
      RESP_DISABLED => "disabled", RESP_CONTACT_MESSAGE_V3 => "contact_message_v3",
      RESP_CHANNEL_MESSAGE_V3 => "channel_message_v3", RESP_CHANNEL_INFO => "channel_info",
      RESP_SIGN_START => "sign_start", RESP_SIGNATURE => "signature",
      RESP_CUSTOM_VARS => "custom_vars", RESP_ADVERT_PATH => "advert_path",
      RESP_TUNING_PARAMS => "tuning_params", RESP_STATS => "stats",
      RESP_AUTOADD_CONFIG => "autoadd_config", RESP_ALLOWED_REPEAT_FREQ => "allowed_repeat_freq",
      RESP_CHANNEL_DATA => "channel_data", RESP_DEFAULT_FLOOD_SCOPE => "default_flood_scope",
      PUSH_ADVERT => "advert", PUSH_PATH_UPDATED => "path_updated",
      PUSH_SEND_CONFIRMED => "send_confirmed", PUSH_MSG_WAITING => "msg_waiting",
      PUSH_RAW_DATA => "raw_data", PUSH_LOGIN_SUCCESS => "login_success",
      PUSH_LOGIN_FAILURE => "login_failure", PUSH_STATUS_RESPONSE => "status_response",
      PUSH_LOG_RX_DATA => "log_rx_data", PUSH_TRACE_DATA => "trace_data",
      PUSH_NEW_ADVERT => "new_advert", PUSH_TELEMETRY_RESPONSE => "telemetry_response",
      PUSH_BINARY_RESPONSE => "binary_response", PUSH_PATH_DISCOVERY_RESPONSE => "path_discovery_response",
      PUSH_CONTROL_DATA => "control_data", PUSH_CONTACT_DELETED => "contact_deleted",
      PUSH_CONTACTS_FULL => "contacts_full",
    }

    def self.response_name(code : UInt8) : String
      # Return a safe diagnostic label, never payload contents. Ordinary replies
      # occupy the low codes; asynchronous pushes use the named 0x80..0x90 range.
      RESPONSE_NAMES[code]? || "push_#{code}"
    end

    def self.known_response?(code : UInt8) : Bool
      # Ordinary response codes are a closed native-v13 range. Pushes occupy a
      # sparse named range; other high codes remain forward-compatible opaque
      # broadcasts whose diagnostics must take the rate-limited path.
      RESPONSE_NAMES.has_key?(code)
    end

    def self.describe_command(payload : Bytes, include_payload = false) : String
      # Include stable semantic fields in the summary and, when requested, the
      # complete decoded payload as hexadecimal for wire-level diagnostics.
      descriptor = descriptor(payload)
      name = descriptor.try(&.name) || :unknown
      summary = "command=#{name} opcode=#{hex_byte(payload[0]?)} command_bytes=#{payload.size}"
      if peer = command_peer(payload)
        summary += " peer=#{hex(peer)}"
      end
      summary += command_details(payload)
      summary += " payload=#{hex(payload)}" if include_payload
      summary
    end

    def self.describe_response(payload : Bytes, include_payload = false) : String
      # Summarize ordinary responses and asynchronous pushes, decoding stable
      # correlation fields while optionally retaining the full wire payload.
      return "response=empty response_bytes=0" if payload.empty?
      summary = "response=#{response_name(payload[0])} code=#{hex_byte(payload[0])} response_bytes=#{payload.size}"
      if payload[0] == RESP_SENT && payload.size >= 10 # SENT: type, u32 token, u32 radio timeout.
        summary += " token=#{read_u32(payload, 2)} radio_timeout_ms=#{read_u32(payload, 6)}"
      elsif {PUSH_LOGIN_SUCCESS, PUSH_LOGIN_FAILURE, PUSH_STATUS_RESPONSE,
             PUSH_TELEMETRY_RESPONSE, PUSH_PATH_DISCOVERY_RESPONSE}.includes?(payload[0]) && payload.size >= 8
        # LOGIN_SUCCESS/FAILURE, STATUS, TELEMETRY, and PATH_DISCOVERY identify
        # the public peer by its six-byte key prefix at response offsets 2..7.
        summary += " peer=#{hex(payload[2, 6])}"
      elsif payload[0] == PUSH_SEND_CONFIRMED && payload.size >= 9 # SEND_CONFIRMED: token and round-trip milliseconds.
        summary += " token=#{read_u32(payload, 1)} round_trip_ms=#{read_u32(payload, 5)}"
      end
      summary += response_details(payload)
      summary += " payload=#{hex(payload)}" if include_payload
      summary
    end

    def self.hex(bytes : Bytes) : String
      # Render wire bytes in a stable form suitable for correlating mux logs
      # with companion logs and packet captures.
      String.build(bytes.size * 2) do |io|
        bytes.each { |byte| io << byte.to_s(16).rjust(2, '0') }
      end
    end

    private def self.hex_byte(byte : UInt8?) : String
      byte ? "0x#{byte.to_s(16).rjust(2, '0')}" : "none"
    end

    private def self.read_u32(payload : Bytes, offset : Int32) : UInt32
      payload[offset].to_u32 |
        (payload[offset + 1].to_u32 << 8) |
        (payload[offset + 2].to_u32 << 16) |
        (payload[offset + 3].to_u32 << 24)
    end

    private def self.read_i32(payload : Bytes, offset : Int32) : Int32
      read_u32(payload, offset).unsafe_as(Int32)
    end

    private def self.text(bytes : Bytes) : String
      String.new(bytes).inspect
    end

    private def self.fixed_text(bytes : Bytes) : String
      # Native fixed text fields are NUL-padded C strings. Exclude padding while
      # retaining escaped output for arbitrary non-UTF-8 bytes before the NUL.
      length = bytes.index(0_u8) || bytes.size
      text(bytes[0, length])
    end

    private def self.command_details(payload : Bytes) : String
      # Decode only fields whose complete offsets are present. Descriptions also
      # run for rejected frames, so every branch must remain safe when truncated.
      return "" if payload.empty?
      case payload[0]
      when CMD_SEND_TXT_MSG
        return "" if payload.size < 13
        " type=#{payload[1]} attempt=#{payload[2]} timestamp=#{read_u32(payload, 3)} destination=#{hex(payload[7, 6])}" \
        " text=#{text(payload[13..])}"
      when CMD_SEND_CHANNEL_TXT_MSG
        return "" if payload.size < 7
        " type=#{payload[1]} channel=#{payload[2]} timestamp=#{read_u32(payload, 3)} text=#{text(payload[7..])}"
      when CMD_SET_ADVERT_NAME
        payload.size > 1 ? " name=#{text(payload[1..])}" : ""
      when CMD_ADD_UPDATE_CONTACT
        return "" if payload.size < 132
        path = describe_normal_path(payload[35], payload[36, 64])
        details = " contact_key=#{hex(payload[1, 32])} contact_type=#{payload[33]} flags=#{payload[34]} #{path}" \
                  " name=#{fixed_text(payload[100, 32])}"
        details += " latitude_microdegrees=#{read_i32(payload, 136)} longitude_microdegrees=#{read_i32(payload, 140)}" if payload.size >= 144
        details += " last_modified=#{read_u32(payload, 144)}" if payload.size >= 148
        details
      when CMD_SET_ADVERT_LATLON
        return "" if payload.size < 9
        details = " latitude_microdegrees=#{read_i32(payload, 1)} longitude_microdegrees=#{read_i32(payload, 5)}"
        details += " altitude=#{read_i32(payload, 9)}" if payload.size >= 13
        details
      when CMD_SEND_RAW_DATA
        return "" if payload.size < 2
        path_bytes = payload[1].to_i
        available = Math.min(path_bytes, Math.max(payload.size - 2, 0))
        details = " path_bytes=#{path_bytes} path=#{hex(payload[2, available])}"
        data_offset = 2 + path_bytes
        details += " data=#{hex(payload[data_offset..])}" if data_offset <= payload.size
        details
      when CMD_SEND_TRACE_PATH
        return "" if payload.size < 10
        width = 1 << (payload[9] & PATH_WIDTH_SHIFT_MASK)
        hashes = payload[10..]
        " tag=#{read_u32(payload, 1)} auth=#{read_u32(payload, 5)} hash_width=#{width}" \
        " path_bytes=#{hashes.size} hashes=#{hex(hashes)}"
      when CMD_SET_CHANNEL
        return "" if payload.size < 34
        " channel=#{payload[1]} name=#{fixed_text(payload[2, 32])}"
      when CMD_SEND_CHANNEL_DATA
        return "" if payload.size < 3
        path = describe_normal_path(payload[2], payload.size > 3 ? payload[3..] : Bytes.empty)
        " channel=#{payload[1]} #{path}"
      when CMD_SEND_RAW_PACKET
        payload.size > 1 ? " packet=#{hex(payload[1..])}" : ""
      else
        ""
      end
    end

    private def self.response_details(payload : Bytes) : String
      # Response offsets exclude the three-byte TCP envelope. Text bodies are
      # rendered with escapes and binary bodies remain hexadecimal.
      return "" if payload.empty?
      case payload[0]
      when RESP_CONTACT, PUSH_NEW_ADVERT
        return "" if payload.size < 132
        details = " contact_key=#{hex(payload[1, 32])} contact_type=#{payload[33]} flags=#{payload[34]}" \
                  " #{describe_normal_path(payload[35], payload[36, 64])} name=#{fixed_text(payload[100, 32])}"
        details += " latitude_microdegrees=#{read_i32(payload, 136)} longitude_microdegrees=#{read_i32(payload, 140)}" if payload.size >= 144
        details += " last_modified=#{read_u32(payload, 144)}" if payload.size >= 148
        details
      when RESP_SELF_INFO
        return "" if payload.size < 58
        details = " node_type=#{payload[1]} tx_power=#{payload[2]} max_tx_power=#{payload[3]}" \
                  " public_key=#{hex(payload[4, 32])}" \
                  " latitude_microdegrees=#{read_i32(payload, 36)} longitude_microdegrees=#{read_i32(payload, 40)}" \
                  " frequency_hz=#{read_u32(payload, 48)} bandwidth_hz=#{read_u32(payload, 52)}" \
                  " spreading_factor=#{payload[56]} coding_rate=#{payload[57]}"
        details += " name=#{text(payload[58..])}" if payload.size > 58
        details
      when RESP_DEVICE_INFO
        return "" if payload.size < 82
        " protocol=#{payload[1]} max_contacts_half=#{payload[2]} max_channels=#{payload[3]}" \
        " pin=#{read_u32(payload, 4)} build=#{fixed_text(payload[8, 12])}" \
        " model=#{fixed_text(payload[20, 40])} firmware=#{fixed_text(payload[60, 20])}" \
        " repeater=#{payload[80]} path_hash_mode=#{payload[81]}"
      when RESP_CHANNEL_INFO
        return "" if payload.size < 50
        " channel=#{payload[1]} name=#{fixed_text(payload[2, 32])}"
      when RESP_CONTACT_MESSAGE
        describe_contact_message(payload, 1, 7, 8, 9, 13)
      when RESP_CONTACT_MESSAGE_V3
        details = payload.size >= 4 ? " snr_quarters=#{payload[1].unsafe_as(Int8)}" : ""
        details + describe_contact_message(payload, 4, 10, 11, 12, 16)
      when RESP_CHANNEL_MESSAGE
        describe_channel_message(payload, 1, 2, 3, 4, 8)
      when RESP_CHANNEL_MESSAGE_V3
        details = payload.size >= 4 ? " snr_quarters=#{payload[1].unsafe_as(Int8)}" : ""
        details + describe_channel_message(payload, 4, 5, 6, 7, 11)
      when RESP_ADVERT_PATH
        return "" if payload.size < 6
        " #{describe_normal_path(payload[5], payload[6..])}"
      when PUSH_TRACE_DATA
        describe_trace_result(payload)
      when PUSH_RAW_DATA, PUSH_CONTROL_DATA
        return "" if payload.size < 4
        " snr_quarters=#{payload[1].unsafe_as(Int8)} rssi=#{payload[2].unsafe_as(Int8)} path_encoding=#{hex_byte(payload[3])}" \
        " data=#{hex(payload[4..])}"
      when PUSH_LOG_RX_DATA
        return "" if payload.size < 3
        " snr_quarters=#{payload[1].unsafe_as(Int8)} rssi=#{payload[2].unsafe_as(Int8)} packet=#{hex(payload[3..])}"
      when PUSH_PATH_DISCOVERY_RESPONSE
        describe_discovery_paths(payload)
      else
        ""
      end
    end

    private def self.describe_contact_message(payload : Bytes, peer_offset : Int32, path_offset : Int32,
                                              type_offset : Int32, timestamp_offset : Int32,
                                              body_offset : Int32) : String
      return "" if payload.size < body_offset
      " sender=#{hex(payload[peer_offset, 6])} path_encoding=#{hex_byte(payload[path_offset])}" \
      " type=#{payload[type_offset]} timestamp=#{read_u32(payload, timestamp_offset)}" \
      " text=#{text(payload[body_offset..])}"
    end

    private def self.describe_channel_message(payload : Bytes, channel_offset : Int32, path_offset : Int32,
                                              type_offset : Int32, timestamp_offset : Int32,
                                              body_offset : Int32) : String
      return "" if payload.size < body_offset
      " channel=#{payload[channel_offset]} path_encoding=#{hex_byte(payload[path_offset])}" \
      " type=#{payload[type_offset]} timestamp=#{read_u32(payload, timestamp_offset)}" \
      " text=#{text(payload[body_offset..])}"
    end

    private def self.describe_normal_path(encoded : UInt8, available : Bytes) : String
      return "path_encoding=0xff path=none" if encoded == NO_PATH_ENCODING
      count = (encoded & PATH_COUNT_MASK).to_i
      width = (encoded >> 6).to_i + 1
      wanted = count * width
      present = Math.min(wanted, available.size)
      "path_encoding=#{hex_byte(encoded)} path_hashes=#{count} hash_width=#{width}" \
      " path=#{hex(available[0, present])}"
    end

    private def self.describe_trace_result(payload : Bytes) : String
      return "" if payload.size < 12
      path_bytes = payload[2].to_i
      width = 1 << (payload[3] & PATH_WIDTH_SHIFT_MASK)
      present = Math.min(path_bytes, Math.max(payload.size - 12, 0))
      hop_count = path_bytes // width
      snr_offset = 12 + path_bytes
      snr_count = Math.min(hop_count + 1, Math.max(payload.size - snr_offset, 0))
      " tag=#{read_u32(payload, 4)} auth=#{read_u32(payload, 8)} hash_width=#{width}" \
      " path_bytes=#{path_bytes} hashes=#{hex(payload[12, present])}" \
      " snr_quarters=#{hex(payload[snr_offset, snr_count])}"
    end

    private def self.describe_discovery_paths(payload : Bytes) : String
      return "" if payload.size < 10
      cursor = 8
      out_encoded = payload[cursor]
      out_size = normal_encoded_path_bytes(out_encoded) || 0
      out_available = Math.min(out_size, Math.max(payload.size - cursor - 1, 0))
      details = " outbound_#{describe_normal_path(out_encoded, payload[cursor + 1, out_available])}"
      cursor += 1 + out_size
      return details if cursor >= payload.size
      in_encoded = payload[cursor]
      in_size = normal_encoded_path_bytes(in_encoded) || 0
      in_available = Math.min(in_size, Math.max(payload.size - cursor - 1, 0))
      details + " inbound_#{describe_normal_path(in_encoded, payload[cursor + 1, in_available])}"
    end

    private def self.command_peer(payload : Bytes) : Bytes?
      # Public destination keys exclude the TCP envelope: ordinary peer commands
      # put their key at byte 1, telemetry after three option bytes at byte 4,
      # and path discovery after its reserved byte at byte 2. Truncated commands
      # can be logged before validation; require at least the six-byte prefix.
      return nil if payload.empty?
      offset = case payload[0]
               when CMD_RESET_PATH, CMD_REMOVE_CONTACT, CMD_SHARE_CONTACT,
                    CMD_SEND_LOGIN, CMD_SEND_STATUS_REQ, CMD_HAS_CONNECTION,
                    CMD_LOGOUT, CMD_GET_CONTACT_BY_KEY, CMD_SEND_BINARY_REQ,
                    CMD_SEND_ANON_REQ
                 # RESET_PATH, REMOVE_CONTACT, SHARE_CONTACT, SEND_LOGIN,
                 # SEND_STATUS_REQ, HAS_CONNECTION, LOGOUT, GET_CONTACT_BY_KEY,
                 # SEND_BINARY_REQ, SEND_ANON_REQ.
                 1
               when CMD_SEND_TELEMETRY_REQ # The four-byte self form has no peer.
                 payload.size >= 36 ? 4 : nil
               when CMD_SEND_PATH_DISCOVERY_REQ
                 2
               end
      return nil unless offset && payload.size >= offset + 6
      payload[offset, Math.min(32, payload.size - offset)]
    end

    enum Grammar
      # The response stream shape used to decide when upstream ownership can be released.
      # Contacts is the multi-frame grammar. Inbox, SelfTelemetry, and Disconnecting
      # additionally document special broker paths; their labels alone do not route replies.
      Single
      Contacts
      Inbox
      SelfTelemetry
      Disconnecting
    end

    @[Flags]
    enum CommandFlags : UInt16
      # Active broker policy bits: scope wrapping, shared resources, and reply correlation.
      # Maintenance marks disruptive lifecycle handling; reboot is exempt from permission gating.
      None          =   0
      ScopeSend     =   1
      RemoteLease   =   4
      Signing       =   8
      Maintenance   =  64
      VerifyIndex   = 256
      VerifySubtype = 512
    end

    # Wire identity, completion grammar, allowed successes, and broker policy for one command.
    record CommandDescriptor,
      opcode : UInt8,
      name : Symbol,
      grammar : Grammar,
      success_codes : Array(UInt8),
      flags : CommandFlags = CommandFlags::None

    record ValidationResult, descriptor : CommandDescriptor?, reason : UInt8? do
      # Carries either a validated descriptor or the native error reason to send downstream.
      def valid? : Bool
        !@descriptor.nil? && @reason.nil?
      end
    end

    enum ResponseDisposition
      # Progress retains the active owner; Complete permits scheduling another command.
      Progress
      Complete
    end

    class ProtocolError < Exception
      # An upstream payload violates the protocol; the broker must abandon the connection epoch.
    end

    private def self.d(opcode, name, grammar, success, flags = CommandFlags::None) : CommandDescriptor
      # Build a descriptor with byte-sized wire codes; the table below names commands and their allowed replies.
      CommandDescriptor.new(opcode.to_u8, name, grammar, success.map(&.to_u8), flags)
    end

    DESCRIPTORS = {
      CMD_APP_START               => d(CMD_APP_START, :app_start, Grammar::Single, [RESP_SELF_INFO]),
      CMD_SEND_TXT_MSG            => d(CMD_SEND_TXT_MSG, :send_txt_msg, Grammar::Single, [RESP_SENT], CommandFlags::ScopeSend),
      CMD_SEND_CHANNEL_TXT_MSG    => d(CMD_SEND_CHANNEL_TXT_MSG, :send_channel_txt_msg, Grammar::Single, [RESP_OK], CommandFlags::ScopeSend),
      CMD_GET_CONTACTS            => d(CMD_GET_CONTACTS, :get_contacts, Grammar::Contacts, [RESP_CONTACTS_START, RESP_CONTACT, RESP_END_OF_CONTACTS]),
      CMD_GET_DEVICE_TIME         => d(CMD_GET_DEVICE_TIME, :get_device_time, Grammar::Single, [RESP_CURRENT_TIME]),
      CMD_SET_DEVICE_TIME         => d(CMD_SET_DEVICE_TIME, :set_device_time, Grammar::Single, [RESP_OK]),
      CMD_SEND_SELF_ADVERT        => d(CMD_SEND_SELF_ADVERT, :send_self_advert, Grammar::Single, [RESP_OK]),
      CMD_SET_ADVERT_NAME         => d(CMD_SET_ADVERT_NAME, :set_advert_name, Grammar::Single, [RESP_OK]),
      CMD_ADD_UPDATE_CONTACT      => d(CMD_ADD_UPDATE_CONTACT, :add_update_contact, Grammar::Single, [RESP_OK]),
      CMD_SYNC_NEXT_MESSAGE       => d(CMD_SYNC_NEXT_MESSAGE, :sync_next_message, Grammar::Inbox, [RESP_CONTACT_MESSAGE, RESP_CHANNEL_MESSAGE, RESP_NO_MORE_MESSAGES, RESP_CONTACT_MESSAGE_V3, RESP_CHANNEL_MESSAGE_V3, RESP_CHANNEL_DATA]),
      CMD_SET_RADIO_PARAMS        => d(CMD_SET_RADIO_PARAMS, :set_radio_params, Grammar::Single, [RESP_OK]),
      CMD_SET_RADIO_TX_POWER      => d(CMD_SET_RADIO_TX_POWER, :set_radio_tx_power, Grammar::Single, [RESP_OK]),
      CMD_RESET_PATH              => d(CMD_RESET_PATH, :reset_path, Grammar::Single, [RESP_OK]),
      CMD_SET_ADVERT_LATLON       => d(CMD_SET_ADVERT_LATLON, :set_advert_latlon, Grammar::Single, [RESP_OK]),
      CMD_REMOVE_CONTACT          => d(CMD_REMOVE_CONTACT, :remove_contact, Grammar::Single, [RESP_OK]),
      CMD_SHARE_CONTACT           => d(CMD_SHARE_CONTACT, :share_contact, Grammar::Single, [RESP_OK]),
      CMD_EXPORT_CONTACT          => d(CMD_EXPORT_CONTACT, :export_contact, Grammar::Single, [RESP_EXPORT_CONTACT]),
      CMD_IMPORT_CONTACT          => d(CMD_IMPORT_CONTACT, :import_contact, Grammar::Single, [RESP_OK]),
      CMD_REBOOT                  => d(CMD_REBOOT, :reboot, Grammar::Disconnecting, Array(UInt8).new, CommandFlags::Maintenance),
      CMD_GET_BATT_AND_STORAGE    => d(CMD_GET_BATT_AND_STORAGE, :get_batt_and_storage, Grammar::Single, [RESP_BATTERY_AND_STORAGE]),
      CMD_SET_TUNING_PARAMS       => d(CMD_SET_TUNING_PARAMS, :set_tuning_params, Grammar::Single, [RESP_OK]),
      CMD_DEVICE_QUERY            => d(CMD_DEVICE_QUERY, :device_query, Grammar::Single, [RESP_DEVICE_INFO]),
      CMD_EXPORT_PRIVATE_KEY      => d(CMD_EXPORT_PRIVATE_KEY, :export_private_key, Grammar::Single, [RESP_PRIVATE_KEY, RESP_DISABLED]),
      CMD_IMPORT_PRIVATE_KEY      => d(CMD_IMPORT_PRIVATE_KEY, :import_private_key, Grammar::Single, [RESP_OK, RESP_DISABLED], CommandFlags::Maintenance),
      CMD_SEND_RAW_DATA           => d(CMD_SEND_RAW_DATA, :send_raw_data, Grammar::Single, [RESP_OK]),
      CMD_SEND_LOGIN              => d(CMD_SEND_LOGIN, :send_login, Grammar::Single, [RESP_SENT], CommandFlags::RemoteLease | CommandFlags::ScopeSend),
      CMD_SEND_STATUS_REQ         => d(CMD_SEND_STATUS_REQ, :send_status_req, Grammar::Single, [RESP_SENT], CommandFlags::RemoteLease | CommandFlags::ScopeSend),
      CMD_HAS_CONNECTION          => d(CMD_HAS_CONNECTION, :has_connection, Grammar::Single, [RESP_OK]),
      CMD_LOGOUT                  => d(CMD_LOGOUT, :logout, Grammar::Single, [RESP_OK]),
      CMD_GET_CONTACT_BY_KEY      => d(CMD_GET_CONTACT_BY_KEY, :get_contact_by_key, Grammar::Single, [RESP_CONTACT]),
      CMD_GET_CHANNEL             => d(CMD_GET_CHANNEL, :get_channel, Grammar::Single, [RESP_CHANNEL_INFO], CommandFlags::VerifyIndex),
      CMD_SET_CHANNEL             => d(CMD_SET_CHANNEL, :set_channel, Grammar::Single, [RESP_OK]),
      CMD_SIGN_START              => d(CMD_SIGN_START, :sign_start, Grammar::Single, [RESP_SIGN_START], CommandFlags::Signing),
      CMD_SIGN_DATA               => d(CMD_SIGN_DATA, :sign_data, Grammar::Single, [RESP_OK], CommandFlags::Signing),
      CMD_SIGN_FINISH             => d(CMD_SIGN_FINISH, :sign_finish, Grammar::Single, [RESP_SIGNATURE], CommandFlags::Signing),
      CMD_SEND_TRACE_PATH         => d(CMD_SEND_TRACE_PATH, :send_trace_path, Grammar::Single, [RESP_SENT], CommandFlags::RemoteLease),
      CMD_SET_DEVICE_PIN          => d(CMD_SET_DEVICE_PIN, :set_device_pin, Grammar::Single, [RESP_OK]),
      CMD_SET_OTHER_PARAMS        => d(CMD_SET_OTHER_PARAMS, :set_other_params, Grammar::Single, [RESP_OK]),
      CMD_SEND_TELEMETRY_REQ      => d(CMD_SEND_TELEMETRY_REQ, :send_telemetry_req, Grammar::SelfTelemetry, [PUSH_TELEMETRY_RESPONSE]),
      CMD_GET_CUSTOM_VARS         => d(CMD_GET_CUSTOM_VARS, :get_custom_vars, Grammar::Single, [RESP_CUSTOM_VARS]),
      CMD_SET_CUSTOM_VAR          => d(CMD_SET_CUSTOM_VAR, :set_custom_var, Grammar::Single, [RESP_OK]),
      CMD_GET_ADVERT_PATH         => d(CMD_GET_ADVERT_PATH, :get_advert_path, Grammar::Single, [RESP_ADVERT_PATH]),
      CMD_GET_TUNING_PARAMS       => d(CMD_GET_TUNING_PARAMS, :get_tuning_params, Grammar::Single, [RESP_TUNING_PARAMS]),
      CMD_SEND_BINARY_REQ         => d(CMD_SEND_BINARY_REQ, :send_binary_req, Grammar::Single, [RESP_SENT], CommandFlags::RemoteLease | CommandFlags::ScopeSend),
      CMD_FACTORY_RESET           => d(CMD_FACTORY_RESET, :factory_reset, Grammar::Disconnecting, [RESP_OK], CommandFlags::Maintenance),
      CMD_SEND_PATH_DISCOVERY_REQ => d(CMD_SEND_PATH_DISCOVERY_REQ, :send_path_discovery_req, Grammar::Single, [RESP_SENT], CommandFlags::RemoteLease | CommandFlags::ScopeSend),
      CMD_SET_FLOOD_SCOPE_KEY     => d(CMD_SET_FLOOD_SCOPE_KEY, :set_flood_scope_key, Grammar::Single, [RESP_OK]),
      CMD_SEND_CONTROL_DATA       => d(CMD_SEND_CONTROL_DATA, :send_control_data, Grammar::Single, [RESP_OK]),
      CMD_GET_STATS               => d(CMD_GET_STATS, :get_stats, Grammar::Single, [RESP_STATS], CommandFlags::VerifySubtype),
      CMD_SEND_ANON_REQ           => d(CMD_SEND_ANON_REQ, :send_anon_req, Grammar::Single, [RESP_SENT], CommandFlags::RemoteLease | CommandFlags::ScopeSend),
      CMD_SET_AUTOADD_CONFIG      => d(CMD_SET_AUTOADD_CONFIG, :set_autoadd_config, Grammar::Single, [RESP_OK]),
      CMD_GET_AUTOADD_CONFIG      => d(CMD_GET_AUTOADD_CONFIG, :get_autoadd_config, Grammar::Single, [RESP_AUTOADD_CONFIG]),
      CMD_GET_ALLOWED_REPEAT_FREQ => d(CMD_GET_ALLOWED_REPEAT_FREQ, :get_allowed_repeat_freq, Grammar::Single, [RESP_ALLOWED_REPEAT_FREQ]),
      CMD_SET_PATH_HASH_MODE      => d(CMD_SET_PATH_HASH_MODE, :set_path_hash_mode, Grammar::Single, [RESP_OK]),
      CMD_SEND_CHANNEL_DATA       => d(CMD_SEND_CHANNEL_DATA, :send_channel_data, Grammar::Single, [RESP_OK], CommandFlags::ScopeSend),
      CMD_SET_DEFAULT_FLOOD_SCOPE => d(CMD_SET_DEFAULT_FLOOD_SCOPE, :set_default_flood_scope, Grammar::Single, [RESP_OK]),
      CMD_GET_DEFAULT_FLOOD_SCOPE => d(CMD_GET_DEFAULT_FLOOD_SCOPE, :get_default_flood_scope, Grammar::Single, [RESP_DEFAULT_FLOOD_SCOPE]),
      CMD_SEND_RAW_PACKET         => d(CMD_SEND_RAW_PACKET, :send_raw_packet, Grammar::Single, [RESP_OK]),
    }

    def self.descriptor(payload : Bytes) : CommandDescriptor?
      # Look up routing policy. Only the remote telemetry form completes first with SENT (0x06).
      return nil if payload.empty?
      desc = DESCRIPTORS[payload[0]]?
      return nil unless desc
      # Telemetry opcode 39 is two distinct native commands selected by length.
      return desc unless payload[0] == CMD_SEND_TELEMETRY_REQ && payload.size != 4
      CommandDescriptor.new(CMD_SEND_TELEMETRY_REQ, :send_telemetry_req, Grammar::Single, [RESP_SENT], CommandFlags::RemoteLease | CommandFlags::ScopeSend)
    end

    def self.validate_command(payload : Bytes) : ValidationResult
      # Error reason 1 is UNSUPPORTED_CMD; reason 6 is ILLEGAL_ARG.
      # Separate unknown opcodes (UNSUPPORTED_CMD) from invalid wire layouts (ILLEGAL_ARG).
      # Broker applies client permissions and shared-resource admission after this structural check.
      desc = descriptor(payload)
      return ValidationResult.new(nil, ERR_UNSUPPORTED_CMD) unless desc
      return ValidationResult.new(desc, ERR_ILLEGAL_ARG) unless command_shape_valid?(payload)
      ValidationResult.new(desc, nil)
    end

    private def self.command_shape_valid?(p : Bytes) : Bool
      # Validate payloads, excluding the TCP envelope. Minimum lengths intentionally allow native trailing extensions.
      # Each branch names its command and the fields that establish its minimum legal size.
      return false if p.empty? || p.size > MAX_PAYLOAD
      n = p.size
      case p[0]
      when CMD_APP_START # Opcode + seven reserved bytes, then optional app name.
        n >= 8
      when CMD_SEND_TXT_MSG # Type, attempt, timestamp, six-byte peer prefix, and at least one body byte.
        n >= 14
      when CMD_SEND_CHANNEL_TXT_MSG # Type, channel, and four-byte timestamp before optional text.
        n >= 7
      when CMD_GET_CONTACTS # Optional four-byte modified-since timestamp.
        n == 1 || n >= 5
      when CMD_GET_DEVICE_TIME, CMD_SYNC_NEXT_MESSAGE, CMD_GET_BATT_AND_STORAGE,
           CMD_EXPORT_PRIVATE_KEY, CMD_SIGN_START, CMD_SIGN_FINISH,
           CMD_GET_CUSTOM_VARS, CMD_GET_TUNING_PARAMS, CMD_GET_AUTOADD_CONFIG,
           CMD_GET_ALLOWED_REPEAT_FREQ, CMD_GET_DEFAULT_FLOOD_SCOPE
        # GET_DEVICE_TIME, SYNC_NEXT_MESSAGE, GET_BATT_AND_STORAGE, EXPORT_PRIVATE_KEY, SIGN_START,
        # SIGN_FINISH, GET_CUSTOM_VARS, GET_TUNING_PARAMS, GET_AUTOADD_CONFIG, GET_ALLOWED_REPEAT_FREQ,
        # GET_DEFAULT_FLOOD_SCOPE: opcode-only commands.
        n >= 1
      when CMD_SET_DEVICE_TIME # Four-byte device time.
        n >= 5
      when CMD_SEND_SELF_ADVERT # Advert parameters are optional.
        n >= 1
      when CMD_SET_ADVERT_NAME # At least one name byte.
        n >= 2
      when CMD_ADD_UPDATE_CONTACT # Accept native contact record variants; byte 35 encodes its outbound path.
        return false unless n == 136 || n == 144 || n >= 148
        # 0xff means no learned outbound path; otherwise decode its count and hash width.
        p[35] == NO_PATH_ENCODING || !normal_encoded_path_bytes(p[35]).nil?
      when CMD_SET_RADIO_PARAMS # Ten bytes of radio parameters.
        n >= 11
      when CMD_SET_RADIO_TX_POWER # One transmit-power byte.
        n >= 2
      when CMD_RESET_PATH, CMD_REMOVE_CONTACT, CMD_SHARE_CONTACT,
           CMD_SEND_STATUS_REQ, CMD_HAS_CONNECTION, CMD_LOGOUT,
           CMD_GET_CONTACT_BY_KEY
        # RESET_PATH, REMOVE_CONTACT, SHARE_CONTACT, SEND_STATUS_REQ, HAS_CONNECTION, LOGOUT,
        # GET_CONTACT_BY_KEY: 32-byte public key.
        n >= 33
      when CMD_SET_ADVERT_LATLON # Two four-byte coordinates, optionally followed by altitude.
        n == 9 || n >= 13
      when CMD_EXPORT_CONTACT # No key means self; otherwise require a 32-byte key.
        n == 1 || n >= 33
      when CMD_IMPORT_CONTACT # At least 98 bytes of exported advertisement.
        n >= 99
      when CMD_REBOOT # Exact reboot confirmation string.
        n == 7 && String.new(p[1, 6]) == "reboot"
      when CMD_SET_TUNING_PARAMS # Eight tuning-parameter bytes.
        n >= 9
      when CMD_DEVICE_QUERY # One client protocol-target byte.
        n >= 2
      when CMD_IMPORT_PRIVATE_KEY # 64-byte private key.
        n >= 65
      when CMD_SEND_RAW_DATA # Signed path length, path bytes, and at least four data bytes.
        return false if n < 6
        # Native command parsing treats this byte as a literal path-byte count,
        # but sendDirect later interprets its upper two bits as the ordinary
        # encoded hash width. Only 0..63 has one unambiguous interpretation.
        return false if p[1] >= 0x40
        path_len = p[1].to_i
        2 + path_len + 4 <= n
      when CMD_SEND_LOGIN # 32-byte peer key, followed by optional credentials.
        n >= 33
      when CMD_GET_CHANNEL # One channel index.
        n >= 2
      when CMD_SET_CHANNEL # Channel index, 32-byte name, 16-byte key, and up to 15 ignored trailing bytes.
        n >= 50 && n < 66
      when CMD_SIGN_DATA # At least one signing-data byte.
        n >= 2
      when CMD_SEND_TRACE_PATH # Tag, auth, flags, then whole path hashes.
        return false unless n > 10 && n - 10 < 171
        # Ten-byte command prefix; low two flag bits select log2(hash width).
        width_shift = p[9] & PATH_WIDTH_SHIFT_MASK
        path_bytes = n - 10
        path_bytes % (1 << width_shift) == 0 && (path_bytes >> width_shift) <= MAX_PATH_SIZE
      when CMD_SET_DEVICE_PIN # Four-byte PIN.
        n >= 5
      when CMD_SET_OTHER_PARAMS # At least one parameter byte.
        n >= 2
      when CMD_SEND_TELEMETRY_REQ # Three option bytes; remote form adds a 32-byte peer key.
        n == 4 || n >= 36
      when CMD_SET_CUSTOM_VAR # At least three assignment bytes, including a ':' separator.
        n >= 4 && p[1..].includes?(':'.ord.to_u8)
      when CMD_GET_ADVERT_PATH # 32-byte key plus selector.
        n >= 34
      when CMD_SEND_BINARY_REQ, CMD_SEND_ANON_REQ # 32-byte peer key and at least one request byte.
        n >= 34
      when CMD_FACTORY_RESET # Exact reset confirmation string.
        n == 6 && String.new(p[1, 5]) == "reset"
      when CMD_SEND_PATH_DISCOVERY_REQ # Zero reserved byte and 32-byte peer key.
        n >= 34 && p[1] == 0
      when CMD_SET_FLOOD_SCOPE_KEY # Mode 0 uses default or explicit 16-byte key; mode 1 is unscoped.
        (n == 2 && (p[1] == 0 || p[1] == 1)) || (n == 18 && p[1] == 0)
      when CMD_SEND_CONTROL_DATA # Control data must have its high flag bit set.
        n >= 2 && (p[1] & 0x80) != 0
      when CMD_GET_STATS # One stats subtype: 0 core, 1 radio, or 2 packets.
        n >= 2 && p[1] <= 2
      when CMD_SET_AUTOADD_CONFIG # One configuration byte.
        n >= 2
      when CMD_SET_PATH_HASH_MODE # Zero reserved byte, then hash-width mode 0–2.
        n >= 3 && p[1] == 0 && p[2] < 3
      when CMD_SEND_CHANNEL_DATA # Channel, encoded path, and data-type fields.
        channel_data_command_valid?(p)
      when CMD_SET_DEFAULT_FLOOD_SCOPE # Empty clear command or named scope and key.
        default_scope_command_valid?(p)
      when CMD_SEND_RAW_PACKET # At least three raw-packet header bytes.
        n >= 4
      else false
      end
    end

    private def self.channel_data_command_valid?(p : Bytes) : Bool
      # SEND_CHANNEL_DATA has opcode, channel, encoded path, path bytes, then a two-byte data type.
      # The 0xff path sentinel means no explicit path; other values encode hop count and hash width.
      return false if p.size < 5
      encoded = p[2]
      path_bytes = if encoded == NO_PATH_ENCODING
                     0
                   else
                     normal_encoded_path_bytes(encoded) || return false
                   end
      # opcode, channel, encoded path, path bytes, two-byte data type
      3 + path_bytes + 2 <= p.size
    end

    private def self.default_scope_command_valid?(p : Bytes) : Bool
      # SET_DEFAULT_FLOOD_SCOPE clears with opcode alone, or carries a 31-byte name and 16-byte key.
      # A supplied name must be nonempty and NUL-terminated within its fixed field.
      return true if p.size == 1
      return false unless p.size == 48
      name = p[1, 31]
      nul = name.index(0_u8)
      !nul.nil? && nul > 0 && nul < 31
    end

    def self.validate_response!(descriptor : CommandDescriptor, payload : Bytes, command : Bytes? = nil, phase : Int32 = 0) : ResponseDisposition
      # Validate ownership as well as shape: a reply must belong to this command and its stream phase.
      # ERR (0x01) terminates any command; contacts require START (0x02), records (0x03), then END (0x04).
      # CHANNEL_INFO (0x12) echoes its index; STATS (0x18) echoes its subtype. Only END completes contacts.
      raise ProtocolError.new("empty upstream payload") if payload.empty?
      raise ProtocolError.new("oversized upstream payload") if payload.size > MAX_PAYLOAD
      code = payload[0]
      if code == RESP_ERR
        raise ProtocolError.new("malformed ERR response") unless payload.size == 2
        return ResponseDisposition::Complete
      end
      unless descriptor.success_codes.includes?(code)
        raise ProtocolError.new("unexpected response 0x#{code.to_s(16)} for #{descriptor.name}")
      end
      validate_response_shape!(payload)
      if descriptor.grammar.contacts?
        expected = phase == 0 ? code == RESP_CONTACTS_START : phase == 1 && (code == RESP_CONTACT || code == RESP_END_OF_CONTACTS)
        raise ProtocolError.new("out-of-order contacts response") unless expected
      end
      if command
        if code == RESP_CHANNEL_INFO && descriptor.flags.includes?(CommandFlags::VerifyIndex)
          raise ProtocolError.new("channel response index mismatch") unless payload[1] == command[1]
        elsif code == RESP_STATS && descriptor.flags.includes?(CommandFlags::VerifySubtype)
          raise ProtocolError.new("stats response subtype mismatch") unless payload[1] == command[1]
        end
      end
      descriptor.grammar.contacts? && code != RESP_END_OF_CONTACTS ? ResponseDisposition::Progress : ResponseDisposition::Complete
    end

    def self.validate_response_shape!(p : Bytes) : Nil
      # Validate ordinary replies and asynchronous pushes without assuming an owner.
      # All sizes include the opcode but exclude the TCP frame header; variable bodies are checked separately.
      raise ProtocolError.new("empty upstream payload") if p.empty?
      raise ProtocolError.new("oversized upstream payload") if p.size > MAX_PAYLOAD
      n = p.size
      ok = case p[0]
           when RESP_OK, RESP_NO_MORE_MESSAGES, RESP_DISABLED # Opcode only.
             n == 1
           when RESP_ERR # Opcode and error reason.
             n == 2
           when RESP_CONTACTS_START, RESP_END_OF_CONTACTS, RESP_CURRENT_TIME # Opcode and four-byte count/time value.
             n == 5
           when RESP_CONTACT, PUSH_NEW_ADVERT # Complete native contact record.
             n == 148
           when RESP_SELF_INFO # Fixed self-info prefix; extensions allowed.
             n >= 58
           when RESP_SENT # Type, four-byte token, four-byte timeout.
             n == 10
           when RESP_CONTACT_MESSAGE # Legacy DM header; type at byte 8.
             contact_text_response_valid?(p, 13, 8)
           when RESP_CHANNEL_MESSAGE # Legacy channel header plus optional text.
             n >= 8
           when RESP_EXPORT_CONTACT # Exported contact payload must not be empty.
             n >= 2
           when RESP_BATTERY_AND_STORAGE # Battery/storage fields total ten bytes after opcode.
             n == 11
           when RESP_DEVICE_INFO # Fixed native device-information record.
             n == 82
           when RESP_PRIVATE_KEY, RESP_SIGNATURE # 64-byte key/signature after opcode.
             n == 65
           when RESP_CONTACT_MESSAGE_V3 # V3 DM adds SNR and two reserved bytes; type at byte 11.
             contact_text_response_valid?(p, 16, 11)
           when RESP_CHANNEL_MESSAGE_V3 # V3 channel header plus optional text.
             n >= 11
           when RESP_CHANNEL_INFO # Index, 32-byte name, and 16-byte key.
             n == 50
           when RESP_SIGN_START # Reserved byte and four-byte signing limit.
             n == 6
           when RESP_CUSTOM_VARS # Custom variable list may be empty.
             n >= 1
           when RESP_ADVERT_PATH # Encoded path length must agree with body.
             advert_path_response_valid?(p)
           when RESP_TUNING_PARAMS # Eight tuning bytes.
             n == 9
           when RESP_STATS # Subtype selects a fixed stats layout.
             stats_response_valid?(p)
           when RESP_AUTOADD_CONFIG # Two configuration bytes.
             n == 3
           when RESP_ALLOWED_REPEAT_FREQ # Zero or more pairs of four-byte frequency bounds.
             n >= 1 && (n - 1) % 8 == 0
           when RESP_CHANNEL_DATA # Byte 8 is body length, following an eight-byte metadata prefix.
             n >= 9 && n == 9 + p[8]
           when RESP_DEFAULT_FLOOD_SCOPE # Empty scope or 31-byte name and 16-byte key.
             n == 1 || n == 48
           when PUSH_ADVERT, PUSH_PATH_UPDATED # Opcode and 32-byte public key.
             n == 33
           when PUSH_SEND_CONFIRMED # Four-byte acknowledgement token and four-byte round-trip time.
             n == 9
           when PUSH_RAW_DATA, PUSH_CONTROL_DATA # Raw/control metadata prefix.
             n >= 4
           when PUSH_LOG_RX_DATA # SNR and RSSI, then opaque packet bytes.
             n >= 3
           when PUSH_MSG_WAITING, PUSH_CONTACTS_FULL # Notification has no body.
             n == 1
           when PUSH_CONTACT_DELETED # 32-byte deleted contact key.
             n == 33
           when PUSH_LOGIN_SUCCESS # Legacy eight-byte prefix or complete extended form (at least 14).
             n == 8 || n >= 14
           when PUSH_LOGIN_FAILURE, PUSH_TELEMETRY_RESPONSE # Reserved/metadata byte and six-byte peer prefix.
             n >= 8
           when PUSH_STATUS_RESPONSE # Peer prefix and status data.
             n >= 9
           when PUSH_PATH_DISCOVERY_RESPONSE # Outbound and inbound encoded paths must both fit exactly.
             path_discovery_response_valid?(p)
           when PUSH_TRACE_DATA # Path hashes and per-hop SNR counts must agree.
             trace_response_valid?(p)
           when PUSH_BINARY_RESPONSE # Reserved byte and four-byte response tag.
             n >= 6
           else p[0] >= PUSH_ADVERT # Unknown pushes are opaque; unknown ordinary responses are fatal.
           end
      raise ProtocolError.new("malformed response 0x#{p[0].to_s(16)} (#{n} bytes)") unless ok
    end

    private def self.stats_response_valid?(p : Bytes) : Bool
      # STATS byte 1 selects core, radio, or packet counters; reject unknown subtypes and truncated layouts.
      return false if p.size < 2
      case p[1]
      when STATS_TYPE_CORE # Core statistics.
        p.size == 11
      when STATS_TYPE_RADIO # Radio statistics.
        p.size == 14
      when STATS_TYPE_PACKET # Packet statistics.
        p.size == 30
      else false
      end
    end

    private def self.trace_response_valid?(p : Bytes) : Bool
      # TRACE_DATA has a 12-byte prefix containing tag/auth, followed by path hashes, one SNR per hop,
      # and a final SNR byte. Byte 2 counts path bytes; the low two bits of byte 3 encode log2(hash width).
      return false if p.size < 13
      path_bytes = p[2].to_i
      shift = p[3] & PATH_WIDTH_SHIFT_MASK
      hash_width = 1 << shift
      return false unless path_bytes % hash_width == 0
      hop_count = path_bytes // hash_width
      hop_count <= MAX_PATH_SIZE && p.size == 12 + path_bytes + hop_count + 1
    end

    private def self.normal_encoded_path_bytes(encoded : UInt8) : Int32?
      # Packet::getPathHashSize() in the pinned native firmware defines ordinary
      # path width as upper-bits + 1 (1, 2, 3; 4 is reserved). Trace flags are a
      # distinct format whose low bits select powers-of-two widths.
      count = (encoded & PATH_COUNT_MASK).to_i # Low six bits count hops; upper two bits encode width minus one.
      width = (encoded >> 6).to_i + 1
      return nil if width == 4
      bytes = count * width
      bytes <= MAX_PATH_SIZE ? bytes : nil
    end

    private def self.advert_path_response_valid?(p : Bytes) : Bool
      # ADVERT_PATH contains a five-byte prefix followed by encoded path length at byte 5 and its hashes.
      return false if p.size < 6
      path_size = normal_encoded_path_bytes(p[5]) || return false
      p.size == 6 + path_size
    end

    private def self.path_discovery_response_valid?(p : Bytes) : Bool
      # PATH_DISCOVERY_RESPONSE begins with opcode, metadata, and six-byte peer prefix.
      # Walk the outbound and inbound length-prefixed paths; neither truncation nor trailing bytes is legal.
      return false if p.size < 10
      cursor = 8
      out_size = normal_encoded_path_bytes(p[cursor]) || return false
      cursor += 1 + out_size
      return false if cursor >= p.size
      in_size = normal_encoded_path_bytes(p[cursor]) || return false
      cursor += 1 + in_size
      cursor == p.size
    end

    private def self.contact_text_response_valid?(p : Bytes, base_size : Int32, type_offset : Int32) : Bool
      # Both DM formats carry a text type at the supplied offset. Signed text (type 2) adds a
      # four-byte sender prefix beyond the format's normal header.
      return false if p.size < base_size
      p[type_offset] != 2 || p.size >= base_size + 4
    end

    def self.downgrade_inbox(payload : Bytes, target_version : UInt8) : Bytes
      # Clients targeting pre-V3 messages lack the three-byte SNR/reserved prefix.
      # Convert CONTACT_MESSAGE_V3 (0x10) to CONTACT_MESSAGE (0x07), and CHANNEL_MESSAGE_V3 (0x11)
      # to CHANNEL_MESSAGE (0x08), removing only that metadata and preserving the body.
      validate_response_shape!(payload)
      return payload.dup if target_version >= 3
      case payload[0]
      when RESP_CONTACT_MESSAGE_V3
        Bytes.new(payload.size - 3) do |i|
          i == 0 ? RESP_CONTACT_MESSAGE : payload[i + 3]
        end
      when RESP_CHANNEL_MESSAGE_V3
        Bytes.new(payload.size - 3) do |i|
          i == 0 ? RESP_CHANNEL_MESSAGE : payload[i + 3]
        end
      else
        payload.dup
      end
    end

    def self.plain_dm?(payload : Bytes) : Bool
      # SEND_TXT_MSG (2) type 0 uses the companion's acknowledgement ring;
      # other text types do not reserve a plain-message acknowledgement slot.
      payload[0]? == CMD_SEND_TXT_MSG && payload[1]? == 0
    end

    def self.app_start_payload(app_name : String, reserved : Bytes = Bytes.new(7, 0_u8)) : Bytes
      # Build APP_START (1): opcode, seven reserved bytes, then the application name.
      raise ArgumentError.new("APP_START reserved field must be seven bytes") unless reserved.size == 7
      name = app_name.to_slice
      raise ArgumentError.new("APP_START payload exceeds native profile") if 8 + name.size > MAX_PAYLOAD
      Bytes.new(8 + name.size) do |i|
        if i == 0
          CMD_APP_START
        elsif i < 8
          reserved[i - 1]
        else
          name[i - 8]
        end
      end
    end

    def self.device_query_payload(target : UInt8 = NATIVE_PROTOCOL_LEVEL) : Bytes
      # Build DEVICE_QUERY (22) with the requested companion protocol target.
      Bytes[CMD_DEVICE_QUERY, target]
    end

    def self.normalize_device_query(payload : Bytes, target : UInt8 = NATIVE_PROTOCOL_LEVEL) : Bytes
      # Keep the upstream on our native protocol target, regardless of the downstream client's version.
      # Broker remembers the original target and downgrades that client's inbox separately.
      result = validate_command(payload)
      raise ArgumentError.new("malformed DEVICE_QUERY") unless result.valid? && payload[0] == CMD_DEVICE_QUERY
      copy = payload.dup
      copy[1] = target
      copy
    end

    def self.validate_self_info!(payload : Bytes) : Bytes
      # SELF_INFO (0x05) carries the companion's 32-byte public key at offset 4.
      # Return a copy for identity checks across reconnects; never expose the full reply in logs.
      raise ProtocolError.new("malformed SELF_INFO") unless payload.size >= 58 && payload[0] == RESP_SELF_INFO
      payload[4, 32].dup
    end

    def self.validate_device_info!(payload : Bytes, expected_protocol : UInt8 = NATIVE_PROTOCOL_LEVEL) : UInt8
      # DEVICE_INFO (0x0d) is 82 bytes; byte 1 is the firmware protocol level.
      # Reject incompatible firmware before admitting clients.
      raise ProtocolError.new("malformed DEVICE_INFO") unless payload.size == 82 && payload[0] == RESP_DEVICE_INFO
      actual = payload[1]
      raise ProtocolError.new("unsupported firmware protocol level #{actual}; expected #{expected_protocol}") unless actual == expected_protocol
      actual
    end
  end
end
