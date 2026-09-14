class MeshCoreTCPMux
  # Namespace for the TCP multiplexer: transport, protocol validation, and per-client state.
  class Protocol
    # Describes the supported native companion wire protocol. The broker uses these
    # rules to reject malformed commands and recognize complete, correctly owned replies.
    # Native v13 caps decoded payloads at 176 bytes and ordinary encoded paths at 64 bytes.
    MAX_PAYLOAD           =   176
    NATIVE_PROTOCOL_LEVEL = 13_u8
    MAX_PATH_SIZE         =    64

    ERR_UNSUPPORTED_CMD = 1_u8
    ERR_ILLEGAL_ARG     = 6_u8

    # Ordinary response names are in wire-code order; payloads never enter logs.
    RESPONSE_NAMES = %w(ok err contacts_start contact end_of_contacts self_info
      sent contact_message channel_message current_time no_more_messages
      export_contact battery_and_storage device_info private_key disabled
      contact_message_v3 channel_message_v3 channel_info sign_start signature
      custom_vars advert_path tuning_params stats autoadd_config allowed_repeat_freq
      channel_data default_flood_scope)

    def self.response_name(code : UInt8) : String
      # Return a safe diagnostic label, never payload contents. 0x8b is TELEMETRY_RESPONSE.
      RESPONSE_NAMES[code]? || (code == 0x8b ? "telemetry_response" : "push_#{code}")
    end

    enum Grammar
      # The response stream shape used to decide when upstream ownership can be released.
      Single
      Contacts
      Inbox
      SelfTelemetry
      Disconnecting
    end

    @[Flags]
    enum CommandFlags : UInt16
      # Broker policy bits: scope wrapping, shared resources, local virtualization, and reply correlation.
      # Maintenance marks disruptive lifecycle handling; reboot is exempt from permission gating.
      None          =   0
      ScopeSend     =   1
      PlainDM       =   2
      RemoteLease   =   4
      Signing       =   8
      Virtual       =  16
      SharedState   =  32
      Maintenance   =  64
      PrivateKey    = 128
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
      # Replies: SELF_INFO
      1_u8 => d(1, :app_start, Grammar::Single, [0x05]),
      # Replies: SENT
      2_u8 => d(2, :send_txt_msg, Grammar::Single, [0x06], CommandFlags::ScopeSend | CommandFlags::PlainDM),
      # Replies: OK
      3_u8 => d(3, :send_channel_txt_msg, Grammar::Single, [0x00], CommandFlags::ScopeSend),
      # Replies: CONTACTS_START, CONTACT, END_OF_CONTACTS
      4_u8 => d(4, :get_contacts, Grammar::Contacts, [0x02, 0x03, 0x04]),
      # Replies: CURRENT_TIME
      5_u8 => d(5, :get_device_time, Grammar::Single, [0x09]),
      # Replies: OK
      6_u8 => d(6, :set_device_time, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: OK
      7_u8 => d(7, :send_self_advert, Grammar::Single, [0x00]),
      # Replies: OK
      8_u8 => d(8, :set_advert_name, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: OK
      9_u8 => d(9, :add_update_contact, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: CONTACT_MESSAGE, CHANNEL_MESSAGE, NO_MORE_MESSAGES, CONTACT_MESSAGE_V3, CHANNEL_MESSAGE_V3, CHANNEL_DATA
      10_u8 => d(10, :sync_next_message, Grammar::Inbox, [0x07, 0x08, 0x0a, 0x10, 0x11, 0x1b], CommandFlags::Virtual),
      # Replies: OK
      11_u8 => d(11, :set_radio_params, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: OK
      12_u8 => d(12, :set_radio_tx_power, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: OK
      13_u8 => d(13, :reset_path, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: OK
      14_u8 => d(14, :set_advert_latlon, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: OK
      15_u8 => d(15, :remove_contact, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: OK
      16_u8 => d(16, :share_contact, Grammar::Single, [0x00]),
      # Replies: EXPORT_CONTACT
      17_u8 => d(17, :export_contact, Grammar::Single, [0x0b]),
      # Replies: OK
      18_u8 => d(18, :import_contact, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: No ordinary reply; the companion disconnects.
      19_u8 => d(19, :reboot, Grammar::Disconnecting, Array(UInt8).new, CommandFlags::Maintenance),
      # Replies: BATTERY_AND_STORAGE
      20_u8 => d(20, :get_batt_and_storage, Grammar::Single, [0x0c]),
      # Replies: OK
      21_u8 => d(21, :set_tuning_params, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: DEVICE_INFO
      22_u8 => d(22, :device_query, Grammar::Single, [0x0d]),
      # Replies: PRIVATE_KEY, DISABLED
      23_u8 => d(23, :export_private_key, Grammar::Single, [0x0e, 0x0f], CommandFlags::PrivateKey),
      # Replies: OK, DISABLED
      24_u8 => d(24, :import_private_key, Grammar::Single, [0x00, 0x0f], CommandFlags::Maintenance | CommandFlags::PrivateKey),
      # Replies: OK
      25_u8 => d(25, :send_raw_data, Grammar::Single, [0x00]),
      # Replies: SENT
      26_u8 => d(26, :send_login, Grammar::Single, [0x06], CommandFlags::RemoteLease | CommandFlags::ScopeSend),
      # Replies: SENT
      27_u8 => d(27, :send_status_req, Grammar::Single, [0x06], CommandFlags::RemoteLease | CommandFlags::ScopeSend),
      # Replies: OK
      28_u8 => d(28, :has_connection, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: OK
      29_u8 => d(29, :logout, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: CONTACT
      30_u8 => d(30, :get_contact_by_key, Grammar::Single, [0x03]),
      # Replies: CHANNEL_INFO
      31_u8 => d(31, :get_channel, Grammar::Single, [0x12], CommandFlags::VerifyIndex),
      # Replies: OK
      32_u8 => d(32, :set_channel, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: SIGN_START
      33_u8 => d(33, :sign_start, Grammar::Single, [0x13], CommandFlags::Signing),
      # Replies: OK
      34_u8 => d(34, :sign_data, Grammar::Single, [0x00], CommandFlags::Signing),
      # Replies: SIGNATURE
      35_u8 => d(35, :sign_finish, Grammar::Single, [0x14], CommandFlags::Signing),
      # Replies: SENT
      36_u8 => d(36, :send_trace_path, Grammar::Single, [0x06], CommandFlags::RemoteLease),
      # Replies: OK
      37_u8 => d(37, :set_device_pin, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: OK
      38_u8 => d(38, :set_other_params, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: TELEMETRY_RESPONSE
      39_u8 => d(39, :send_telemetry_req, Grammar::SelfTelemetry, [0x8b]),
      # Replies: CUSTOM_VARS
      40_u8 => d(40, :get_custom_vars, Grammar::Single, [0x15]),
      # Replies: OK
      41_u8 => d(41, :set_custom_var, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: ADVERT_PATH
      42_u8 => d(42, :get_advert_path, Grammar::Single, [0x16]),
      # Replies: TUNING_PARAMS
      43_u8 => d(43, :get_tuning_params, Grammar::Single, [0x17]),
      # Replies: SENT
      50_u8 => d(50, :send_binary_req, Grammar::Single, [0x06], CommandFlags::RemoteLease | CommandFlags::ScopeSend),
      # Replies: OK
      51_u8 => d(51, :factory_reset, Grammar::Disconnecting, [0x00], CommandFlags::Maintenance),
      # Replies: SENT
      52_u8 => d(52, :send_path_discovery_req, Grammar::Single, [0x06], CommandFlags::RemoteLease | CommandFlags::ScopeSend),
      # Replies: OK
      54_u8 => d(54, :set_flood_scope_key, Grammar::Single, [0x00], CommandFlags::Virtual),
      # Replies: OK
      55_u8 => d(55, :send_control_data, Grammar::Single, [0x00]),
      # Replies: STATS
      56_u8 => d(56, :get_stats, Grammar::Single, [0x18], CommandFlags::VerifySubtype),
      # Replies: SENT
      57_u8 => d(57, :send_anon_req, Grammar::Single, [0x06], CommandFlags::RemoteLease | CommandFlags::ScopeSend),
      # Replies: OK
      58_u8 => d(58, :set_autoadd_config, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: AUTOADD_CONFIG
      59_u8 => d(59, :get_autoadd_config, Grammar::Single, [0x19]),
      # Replies: ALLOWED_REPEAT_FREQ
      60_u8 => d(60, :get_allowed_repeat_freq, Grammar::Single, [0x1a]),
      # Replies: OK
      61_u8 => d(61, :set_path_hash_mode, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: OK
      62_u8 => d(62, :send_channel_data, Grammar::Single, [0x00], CommandFlags::ScopeSend),
      # Replies: OK
      63_u8 => d(63, :set_default_flood_scope, Grammar::Single, [0x00], CommandFlags::SharedState),
      # Replies: DEFAULT_FLOOD_SCOPE
      64_u8 => d(64, :get_default_flood_scope, Grammar::Single, [0x1c]),
      # Replies: OK
      65_u8 => d(65, :send_raw_packet, Grammar::Single, [0x00]),
    }

    def self.descriptor(payload : Bytes) : CommandDescriptor?
      # Look up routing policy. Only the remote telemetry form completes first with SENT (0x06).
      return nil if payload.empty?
      desc = DESCRIPTORS[payload[0]]?
      return nil unless desc
      # Telemetry opcode 39 is two distinct native commands selected by length.
      return desc unless payload[0] == 39 && payload.size != 4
      CommandDescriptor.new(39_u8, :send_telemetry_req, Grammar::Single, [0x06_u8], CommandFlags::RemoteLease | CommandFlags::ScopeSend)
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
      when 1 # APP_START: opcode + seven reserved bytes, then optional app name.
        n >= 8
      when 2 # SEND_TXT_MSG: type, attempt, timestamp, six-byte peer prefix, and at least one body byte.
        n >= 14
      when 3 # SEND_CHANNEL_TXT_MSG: type, channel, and four-byte timestamp before optional text.
        n >= 7
      when 4 # GET_CONTACTS: optional four-byte modified-since timestamp.
        n == 1 || n >= 5
      when 5, 10, 20, 23, 33, 35, 40, 43, 59, 60, 64
        # GET_DEVICE_TIME, SYNC_NEXT_MESSAGE, GET_BATT_AND_STORAGE, EXPORT_PRIVATE_KEY, SIGN_START,
        # SIGN_FINISH, GET_CUSTOM_VARS, GET_TUNING_PARAMS, GET_AUTOADD_CONFIG, GET_ALLOWED_REPEAT_FREQ,
        # GET_DEFAULT_FLOOD_SCOPE: opcode-only commands.
        n >= 1
      when 6 # SET_DEVICE_TIME: four-byte device time.
        n >= 5
      when 7 # SEND_SELF_ADVERT: advert parameters are optional.
        n >= 1
      when 8 # SET_ADVERT_NAME: at least one name byte.
        n >= 2
      when 9 # ADD_UPDATE_CONTACT: accept native contact record variants; byte 35 encodes its outbound path.
        return false unless n == 136 || n == 144 || n >= 148
        # 0xff means no learned outbound path; otherwise decode its count and hash width.
        p[35] == 0xff || !normal_encoded_path_bytes(p[35]).nil?
      when 11 # SET_RADIO_PARAMS: ten bytes of radio parameters.
        n >= 11
      when 12 # SET_RADIO_TX_POWER: one transmit-power byte.
        n >= 2
      when 13, 15, 16, 27, 28, 29, 30
        # RESET_PATH, REMOVE_CONTACT, SHARE_CONTACT, SEND_STATUS_REQ, HAS_CONNECTION, LOGOUT,
        # GET_CONTACT_BY_KEY: 32-byte public key.
        n >= 33
      when 14 # SET_ADVERT_LATLON: two four-byte coordinates, optionally followed by altitude.
        n == 9 || n >= 13
      when 17 # EXPORT_CONTACT: no key means self; otherwise require a 32-byte key.
        n == 1 || n >= 33
      when 18 # IMPORT_CONTACT: at least 98 bytes of exported advertisement.
        n >= 99
      when 19 # REBOOT: exact reboot confirmation string.
        n == 7 && String.new(p[1, 6]) == "reboot"
      when 21 # SET_TUNING_PARAMS: eight tuning-parameter bytes.
        n >= 9
      when 22 # DEVICE_QUERY: one client protocol-target byte.
        n >= 2
      when 24 # IMPORT_PRIVATE_KEY: 64-byte private key.
        n >= 65
      when 25 # SEND_RAW_DATA: signed path length, path bytes, and at least four data bytes.
        return false if n < 6
        path_len = p[1].to_i8.to_i
        path_len >= 0 && 2 + path_len + 4 <= n
      when 26 # SEND_LOGIN: 32-byte peer key, followed by optional credentials.
        n >= 33
      when 31 # GET_CHANNEL: one channel index.
        n >= 2
      when 32 # SET_CHANNEL: channel index, 32-byte name, and 16–31 secret bytes.
        n >= 50 && n < 66
      when 34 # SIGN_DATA: at least one signing-data byte.
        n >= 2
      when 36 # SEND_TRACE_PATH: tag, auth, flags, then whole path hashes.
        return false unless n > 10 && n - 10 < 171
        # Ten-byte command prefix; low two flag bits select log2(hash width).
        width_shift = p[9] & 0x03
        path_bytes = n - 10
        path_bytes % (1 << width_shift) == 0 && (path_bytes >> width_shift) <= MAX_PATH_SIZE
      when 37 # SET_DEVICE_PIN: four-byte PIN.
        n >= 5
      when 38 # SET_OTHER_PARAMS: at least one parameter byte.
        n >= 2
      when 39 # SEND_TELEMETRY_REQ: three option bytes; remote form adds a 32-byte peer key.
        n == 4 || n >= 36
      when 41 # SET_CUSTOM_VAR: at least three assignment bytes, including a ':' separator.
        n >= 4 && p[1..].includes?(':'.ord.to_u8)
      when 42 # GET_ADVERT_PATH: 32-byte key plus selector.
        n >= 34
      when 50, 57 # SEND_BINARY_REQ, SEND_ANON_REQ: 32-byte peer key and at least one request byte.
        n >= 34
      when 51 # FACTORY_RESET: exact reset confirmation string.
        n == 6 && String.new(p[1, 5]) == "reset"
      when 52 # SEND_PATH_DISCOVERY_REQ: zero reserved byte and 32-byte peer key.
        n >= 34 && p[1] == 0
      when 54 # SET_FLOOD_SCOPE_KEY: mode 0 uses default or explicit 16-byte key; mode 1 is unscoped.
        (n == 2 && (p[1] == 0 || p[1] == 1)) || (n == 18 && p[1] == 0)
      when 55 # SEND_CONTROL_DATA: control data must have its high flag bit set.
        n >= 2 && (p[1] & 0x80) != 0
      when 56 # GET_STATS: one stats subtype: 0 core, 1 radio, or 2 packets.
        n >= 2 && p[1] <= 2
      when 58 # SET_AUTOADD_CONFIG: one configuration byte.
        n >= 2
      when 61 # SET_PATH_HASH_MODE: zero reserved byte, then hash-width mode 0–2.
        n >= 3 && p[1] == 0 && p[2] < 3
      when 62 # SEND_CHANNEL_DATA: channel, encoded path, and data-type fields.
        channel_data_command_valid?(p)
      when 63 # SET_DEFAULT_FLOOD_SCOPE: empty clear command or named scope and key.
        default_scope_command_valid?(p)
      when 65 # SEND_RAW_PACKET: at least three raw-packet header bytes.
        n >= 4
      else false
      end
    end

    private def self.channel_data_command_valid?(p : Bytes) : Bool
      # SEND_CHANNEL_DATA has opcode, channel, encoded path, path bytes, then a two-byte data type.
      # The 0xff path sentinel means no explicit path; other values encode hop count and hash width.
      return false if p.size < 5
      encoded = p[2]
      path_bytes = if encoded == 0xff
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
      if code == 0x01
        raise ProtocolError.new("malformed ERR response") unless payload.size == 2
        return ResponseDisposition::Complete
      end
      unless descriptor.success_codes.includes?(code)
        raise ProtocolError.new("unexpected response 0x#{code.to_s(16)} for #{descriptor.name}")
      end
      validate_response_shape!(payload)
      if descriptor.grammar.contacts?
        expected = phase == 0 ? code == 0x02 : phase == 1 && (code == 0x03 || code == 0x04)
        raise ProtocolError.new("out-of-order contacts response") unless expected
      end
      if command
        if code == 0x12 && descriptor.flags.includes?(CommandFlags::VerifyIndex)
          raise ProtocolError.new("channel response index mismatch") unless payload[1] == command[1]
        elsif code == 0x18 && descriptor.flags.includes?(CommandFlags::VerifySubtype)
          raise ProtocolError.new("stats response subtype mismatch") unless payload[1] == command[1]
        end
      end
      descriptor.grammar.contacts? && code != 0x04 ? ResponseDisposition::Progress : ResponseDisposition::Complete
    end

    def self.validate_response_shape!(p : Bytes) : Nil
      # Validate ordinary replies and asynchronous pushes without assuming an owner.
      # All sizes include the opcode but exclude the TCP frame header; variable bodies are checked separately.
      raise ProtocolError.new("empty upstream payload") if p.empty?
      raise ProtocolError.new("oversized upstream payload") if p.size > MAX_PAYLOAD
      n = p.size
      ok = case p[0]
           when 0x00, 0x0a, 0x0f # OK, NO_MORE_MESSAGES, DISABLED: opcode only.
             n == 1
           when 0x01 # ERR: opcode and error reason.
             n == 2
           when 0x02, 0x04, 0x09 # CONTACTS_START, END_OF_CONTACTS, CURRENT_TIME: opcode and four-byte count/time value.
             n == 5
           when 0x03, 0x8a # CONTACT, NEW_ADVERT: complete native contact record.
             n == 148
           when 0x05 # SELF_INFO: fixed self-info prefix; extensions allowed.
             n >= 58
           when 0x06 # SENT: opcode, type, four-byte token, four-byte timeout.
             n == 10
           when 0x07 # CONTACT_MESSAGE: legacy DM header; type at byte 8.
             contact_text_response_valid?(p, 13, 8)
           when 0x08 # CHANNEL_MESSAGE: legacy channel header plus optional text.
             n >= 8
           when 0x0b # EXPORT_CONTACT: exported contact payload must not be empty.
             n >= 2
           when 0x0c # BATTERY_AND_STORAGE: battery/storage fields total ten bytes after opcode.
             n == 11
           when 0x0d # DEVICE_INFO: fixed native device-information record.
             n == 82
           when 0x0e, 0x14 # PRIVATE_KEY, SIGNATURE: 64-byte key/signature after opcode.
             n == 65
           when 0x10 # CONTACT_MESSAGE_V3: V3 DM adds SNR and two reserved bytes; type at byte 11.
             contact_text_response_valid?(p, 16, 11)
           when 0x11 # CHANNEL_MESSAGE_V3: V3 channel header plus optional text.
             n >= 11
           when 0x12 # CHANNEL_INFO: index, 32-byte name, and 16-byte key.
             n == 50
           when 0x13 # SIGN_START: reserved byte and four-byte signing limit.
             n == 6
           when 0x15 # CUSTOM_VARS: custom variable list may be empty.
             n >= 1
           when 0x16 # ADVERT_PATH: encoded path length must agree with body.
             advert_path_response_valid?(p)
           when 0x17 # TUNING_PARAMS: eight tuning bytes.
             n == 9
           when 0x18 # STATS: subtype selects a fixed stats layout.
             stats_response_valid?(p)
           when 0x19 # AUTOADD_CONFIG: two configuration bytes.
             n == 3
           when 0x1a # ALLOWED_REPEAT_FREQ: zero or more pairs of four-byte frequency bounds.
             n >= 1 && (n - 1) % 8 == 0
           when 0x1b # CHANNEL_DATA: byte 8 is body length, following an eight-byte metadata prefix.
             n >= 9 && n == 9 + p[8]
           when 0x1c # DEFAULT_FLOOD_SCOPE: empty scope or 31-byte name and 16-byte key.
             n == 1 || n == 48
           when 0x80, 0x81 # ADVERT, PATH_UPDATED: opcode and 32-byte public key.
             n == 33
           when 0x82 # SEND_CONFIRMED: four-byte acknowledgement token and four-byte round-trip time.
             n == 9
           when 0x84, 0x8e # RAW_DATA, CONTROL_DATA: raw/control metadata prefix.
             n >= 4
           when 0x88 # LOG_RX_DATA: SNR and RSSI, then opaque packet bytes.
             n >= 3
           when 0x83, 0x90 # MSG_WAITING, CONTACTS_FULL: notification has no body.
             n == 1
           when 0x8f # CONTACT_DELETED: 32-byte deleted contact key.
             n == 33
           when 0x85 # LOGIN_SUCCESS: legacy eight-byte prefix or complete extended form (at least 14).
             n == 8 || n >= 14
           when 0x86, 0x8b # LOGIN_FAILURE, TELEMETRY_RESPONSE: reserved/metadata byte and six-byte peer prefix.
             n >= 8
           when 0x87 # STATUS_RESPONSE: peer prefix and status data.
             n >= 9
           when 0x8d # PATH_DISCOVERY_RESPONSE: outbound and inbound encoded paths must both fit exactly.
             path_discovery_response_valid?(p)
           when 0x89 # TRACE_DATA: path hashes and per-hop SNR counts must agree.
             trace_response_valid?(p)
           when 0x8c # BINARY_RESPONSE: reserved byte and four-byte response tag.
             n >= 6
           else p[0] >= 0x80 # unknown pushes are opaque; unknown ordinary responses are fatal
           end
      raise ProtocolError.new("malformed response 0x#{p[0].to_s(16)} (#{n} bytes)") unless ok
    end

    private def self.stats_response_valid?(p : Bytes) : Bool
      # STATS byte 1 selects core, radio, or packet counters; reject unknown subtypes and truncated layouts.
      return false if p.size < 2
      case p[1]
      when 0 # core statistics.
        p.size == 11
      when 1 # radio statistics.
        p.size == 14
      when 2 # packet statistics.
        p.size == 30
      else false
      end
    end

    private def self.trace_response_valid?(p : Bytes) : Bool
      # TRACE_DATA has a 12-byte prefix containing tag/auth, followed by path hashes, one SNR per hop,
      # and a final SNR byte. Byte 2 counts path bytes; the low two bits of byte 3 encode log2(hash width).
      return false if p.size < 13
      path_bytes = p[2].to_i
      shift = p[3] & 0x03
      path_bytes % (1 << shift) == 0 && p.size == 12 + path_bytes + (path_bytes >> shift) + 1
    end

    private def self.normal_encoded_path_bytes(encoded : UInt8) : Int32?
      # Packet::getPathHashSize() in the pinned native firmware defines ordinary
      # path width as upper-bits + 1 (1, 2, 3; 4 is reserved). Trace flags are a
      # distinct format whose low bits select powers-of-two widths.
      count = (encoded & 0x3f).to_i # Low six bits count hops; upper two bits encode width minus one.
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
      when 0x10 # CONTACT_MESSAGE_V3 -> legacy CONTACT_MESSAGE (0x07).
        Bytes.new(payload.size - 3) do |i|
          i == 0 ? 0x07_u8 : payload[i + 3]
        end
      when 0x11 # CHANNEL_MESSAGE_V3 -> legacy CHANNEL_MESSAGE (0x08).
        Bytes.new(payload.size - 3) do |i|
          i == 0 ? 0x08_u8 : payload[i + 3]
        end
      else
        payload.dup
      end
    end

    def self.app_start_payload(app_name : String, reserved : Bytes = Bytes.new(7, 0_u8)) : Bytes
      # Build APP_START (1): opcode, seven reserved bytes, then the application name.
      raise ArgumentError.new("APP_START reserved field must be seven bytes") unless reserved.size == 7
      name = app_name.to_slice
      raise ArgumentError.new("APP_START payload exceeds native profile") if 8 + name.size > MAX_PAYLOAD
      Bytes.new(8 + name.size) do |i|
        if i == 0
          1_u8
        elsif i < 8
          reserved[i - 1]
        else
          name[i - 8]
        end
      end
    end

    def self.device_query_payload(target : UInt8 = NATIVE_PROTOCOL_LEVEL) : Bytes
      # Build DEVICE_QUERY (22) with the requested companion protocol target.
      Bytes[22_u8, target]
    end

    def self.normalize_device_query(payload : Bytes, target : UInt8 = NATIVE_PROTOCOL_LEVEL) : Bytes
      # Keep the upstream on our native protocol target, regardless of the downstream client's version.
      # Broker remembers the original target and downgrades that client's inbox separately.
      result = validate_command(payload)
      raise ArgumentError.new("malformed DEVICE_QUERY") unless result.valid? && payload[0] == 22
      copy = payload.dup
      copy[1] = target
      copy
    end

    def self.validate_self_info!(payload : Bytes) : Bytes
      # SELF_INFO (0x05) carries the companion's 32-byte public key at offset 4.
      # Return a copy for identity checks across reconnects; never expose the full reply in logs.
      raise ProtocolError.new("malformed SELF_INFO") unless payload.size >= 58 && payload[0] == 0x05
      payload[4, 32].dup
    end

    def self.validate_device_info!(payload : Bytes, expected_protocol : UInt8 = NATIVE_PROTOCOL_LEVEL) : UInt8
      # DEVICE_INFO (0x0d) is 82 bytes; byte 1 is the firmware protocol level.
      # Reject incompatible firmware before admitting clients.
      raise ProtocolError.new("malformed DEVICE_INFO") unless payload.size == 82 && payload[0] == 0x0d
      actual = payload[1]
      raise ProtocolError.new("unsupported firmware protocol level #{actual}; expected #{expected_protocol}") unless actual == expected_protocol
      actual
    end
  end
end
