class MeshCoreTCPMux
  class Protocol
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
      RESPONSE_NAMES[code]? || (code == 0x8b ? "telemetry_response" : "push_#{code}")
    end

    enum Grammar
      Single
      Contacts
      Inbox
      SelfTelemetry
      Disconnecting
    end

    @[Flags]
    enum CommandFlags : UInt16
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

    record CommandDescriptor,
      opcode : UInt8,
      name : Symbol,
      grammar : Grammar,
      success_codes : Array(UInt8),
      flags : CommandFlags = CommandFlags::None

    record ValidationResult, descriptor : CommandDescriptor?, reason : UInt8? do
      def valid? : Bool
        !@descriptor.nil? && @reason.nil?
      end
    end

    enum ResponseDisposition
      Progress
      Complete
    end

    class ProtocolError < Exception
    end

    private def self.d(opcode, name, grammar, success, flags = CommandFlags::None)
      CommandDescriptor.new(opcode.to_u8, name, grammar, success.map(&.to_u8), flags)
    end

    DESCRIPTORS = {
       1_u8 => d(1, :app_start, Grammar::Single, [0x05]),
       2_u8 => d(2, :send_txt_msg, Grammar::Single, [0x06], CommandFlags::ScopeSend | CommandFlags::PlainDM),
       3_u8 => d(3, :send_channel_txt_msg, Grammar::Single, [0x00], CommandFlags::ScopeSend),
       4_u8 => d(4, :get_contacts, Grammar::Contacts, [0x02, 0x03, 0x04]),
       5_u8 => d(5, :get_device_time, Grammar::Single, [0x09]),
       6_u8 => d(6, :set_device_time, Grammar::Single, [0x00], CommandFlags::SharedState),
       7_u8 => d(7, :send_self_advert, Grammar::Single, [0x00]),
       8_u8 => d(8, :set_advert_name, Grammar::Single, [0x00], CommandFlags::SharedState),
       9_u8 => d(9, :add_update_contact, Grammar::Single, [0x00], CommandFlags::SharedState),
      10_u8 => d(10, :sync_next_message, Grammar::Inbox, [0x07, 0x08, 0x0a, 0x10, 0x11, 0x1b], CommandFlags::Virtual),
      11_u8 => d(11, :set_radio_params, Grammar::Single, [0x00], CommandFlags::SharedState),
      12_u8 => d(12, :set_radio_tx_power, Grammar::Single, [0x00], CommandFlags::SharedState),
      13_u8 => d(13, :reset_path, Grammar::Single, [0x00], CommandFlags::SharedState),
      14_u8 => d(14, :set_advert_latlon, Grammar::Single, [0x00], CommandFlags::SharedState),
      15_u8 => d(15, :remove_contact, Grammar::Single, [0x00], CommandFlags::SharedState),
      16_u8 => d(16, :share_contact, Grammar::Single, [0x00]),
      17_u8 => d(17, :export_contact, Grammar::Single, [0x0b]),
      18_u8 => d(18, :import_contact, Grammar::Single, [0x00], CommandFlags::SharedState),
      19_u8 => d(19, :reboot, Grammar::Disconnecting, Array(UInt8).new, CommandFlags::Maintenance),
      20_u8 => d(20, :get_batt_and_storage, Grammar::Single, [0x0c]),
      21_u8 => d(21, :set_tuning_params, Grammar::Single, [0x00], CommandFlags::SharedState),
      22_u8 => d(22, :device_query, Grammar::Single, [0x0d]),
      23_u8 => d(23, :export_private_key, Grammar::Single, [0x0e, 0x0f], CommandFlags::PrivateKey),
      24_u8 => d(24, :import_private_key, Grammar::Single, [0x00, 0x0f], CommandFlags::Maintenance | CommandFlags::PrivateKey),
      25_u8 => d(25, :send_raw_data, Grammar::Single, [0x00]),
      26_u8 => d(26, :send_login, Grammar::Single, [0x06], CommandFlags::RemoteLease | CommandFlags::ScopeSend),
      27_u8 => d(27, :send_status_req, Grammar::Single, [0x06], CommandFlags::RemoteLease | CommandFlags::ScopeSend),
      28_u8 => d(28, :has_connection, Grammar::Single, [0x00], CommandFlags::SharedState),
      29_u8 => d(29, :logout, Grammar::Single, [0x00], CommandFlags::SharedState),
      30_u8 => d(30, :get_contact_by_key, Grammar::Single, [0x03]),
      31_u8 => d(31, :get_channel, Grammar::Single, [0x12], CommandFlags::VerifyIndex),
      32_u8 => d(32, :set_channel, Grammar::Single, [0x00], CommandFlags::SharedState),
      33_u8 => d(33, :sign_start, Grammar::Single, [0x13], CommandFlags::Signing),
      34_u8 => d(34, :sign_data, Grammar::Single, [0x00], CommandFlags::Signing),
      35_u8 => d(35, :sign_finish, Grammar::Single, [0x14], CommandFlags::Signing),
      36_u8 => d(36, :send_trace_path, Grammar::Single, [0x06], CommandFlags::RemoteLease),
      37_u8 => d(37, :set_device_pin, Grammar::Single, [0x00], CommandFlags::SharedState),
      38_u8 => d(38, :set_other_params, Grammar::Single, [0x00], CommandFlags::SharedState),
      39_u8 => d(39, :send_telemetry_req, Grammar::SelfTelemetry, [0x8b]),
      40_u8 => d(40, :get_custom_vars, Grammar::Single, [0x15]),
      41_u8 => d(41, :set_custom_var, Grammar::Single, [0x00], CommandFlags::SharedState),
      42_u8 => d(42, :get_advert_path, Grammar::Single, [0x16]),
      43_u8 => d(43, :get_tuning_params, Grammar::Single, [0x17]),
      50_u8 => d(50, :send_binary_req, Grammar::Single, [0x06], CommandFlags::RemoteLease | CommandFlags::ScopeSend),
      51_u8 => d(51, :factory_reset, Grammar::Disconnecting, [0x00], CommandFlags::Maintenance),
      52_u8 => d(52, :send_path_discovery_req, Grammar::Single, [0x06], CommandFlags::RemoteLease | CommandFlags::ScopeSend),
      54_u8 => d(54, :set_flood_scope_key, Grammar::Single, [0x00], CommandFlags::Virtual),
      55_u8 => d(55, :send_control_data, Grammar::Single, [0x00]),
      56_u8 => d(56, :get_stats, Grammar::Single, [0x18], CommandFlags::VerifySubtype),
      57_u8 => d(57, :send_anon_req, Grammar::Single, [0x06], CommandFlags::RemoteLease | CommandFlags::ScopeSend),
      58_u8 => d(58, :set_autoadd_config, Grammar::Single, [0x00], CommandFlags::SharedState),
      59_u8 => d(59, :get_autoadd_config, Grammar::Single, [0x19]),
      60_u8 => d(60, :get_allowed_repeat_freq, Grammar::Single, [0x1a]),
      61_u8 => d(61, :set_path_hash_mode, Grammar::Single, [0x00], CommandFlags::SharedState),
      62_u8 => d(62, :send_channel_data, Grammar::Single, [0x00], CommandFlags::ScopeSend),
      63_u8 => d(63, :set_default_flood_scope, Grammar::Single, [0x00], CommandFlags::SharedState),
      64_u8 => d(64, :get_default_flood_scope, Grammar::Single, [0x1c]),
      65_u8 => d(65, :send_raw_packet, Grammar::Single, [0x00]),
    }

    def self.descriptor(payload : Bytes) : CommandDescriptor?
      return nil if payload.empty?
      desc = DESCRIPTORS[payload[0]]?
      return nil unless desc
      # Telemetry opcode 39 is two distinct native commands selected by length.
      return desc unless payload[0] == 39 && payload.size != 4
      CommandDescriptor.new(39_u8, :send_telemetry_req, Grammar::Single, [0x06_u8], CommandFlags::RemoteLease | CommandFlags::ScopeSend)
    end

    def self.validate_command(payload : Bytes) : ValidationResult
      desc = descriptor(payload)
      return ValidationResult.new(nil, ERR_UNSUPPORTED_CMD) unless desc
      return ValidationResult.new(desc, ERR_ILLEGAL_ARG) unless command_shape_valid?(payload)
      ValidationResult.new(desc, nil)
    end

    private def self.command_shape_valid?(p : Bytes) : Bool
      return false if p.empty? || p.size > MAX_PAYLOAD
      n = p.size
      case p[0]
      when 1                                         then n >= 8
      when 2                                         then n >= 14
      when 3                                         then n >= 7
      when 4                                         then n == 1 || n >= 5
      when 5, 10, 20, 23, 33, 35, 40, 43, 59, 60, 64 then n >= 1
      when 6                                         then n >= 5
      when 7                                         then n >= 1
      when 8                                         then n >= 2
      when 9
        return false unless n == 136 || n == 144 || n >= 148
        p[35] == 0xff || !normal_encoded_path_bytes(p[35]).nil?
      when 11                         then n >= 11
      when 12                         then n >= 2
      when 13, 15, 16, 27, 28, 29, 30 then n >= 33
      when 14                         then n == 9 || n >= 13
      when 17                         then n == 1 || n >= 33
      when 18                         then n >= 99
      when 19                         then n == 7 && String.new(p[1, 6]) == "reboot"
      when 21                         then n >= 9
      when 22                         then n >= 2
      when 24                         then n >= 65
      when 25
        return false if n < 6
        path_len = p[1].to_i8.to_i
        path_len >= 0 && 2 + path_len + 4 <= n
      when 26 then n >= 33
      when 31 then n >= 2
      when 32 then n >= 50 && n < 66
      when 34 then n >= 2
      when 36
        return false unless n > 10 && n - 10 < 171
        width_shift = p[9] & 0x03
        path_bytes = n - 10
        path_bytes % (1 << width_shift) == 0 && (path_bytes >> width_shift) <= MAX_PATH_SIZE
      when 37     then n >= 5
      when 38     then n >= 2
      when 39     then n == 4 || n >= 36
      when 41     then n >= 4 && p[1..].includes?(':'.ord.to_u8)
      when 42     then n >= 34
      when 50, 57 then n >= 34
      when 51     then n == 6 && String.new(p[1, 5]) == "reset"
      when 52     then n >= 34 && p[1] == 0
      when 54     then (n == 2 && (p[1] == 0 || p[1] == 1)) || (n == 18 && p[1] == 0)
      when 55     then n >= 2 && (p[1] & 0x80) != 0
      when 56     then n >= 2 && p[1] <= 2
      when 58     then n >= 2
      when 61     then n >= 3 && p[1] == 0 && p[2] < 3
      when 62     then channel_data_command_valid?(p)
      when 63     then default_scope_command_valid?(p)
      when 65     then n >= 4
      else             false
      end
    end

    private def self.channel_data_command_valid?(p : Bytes) : Bool
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
      return true if p.size == 1
      return false unless p.size == 48
      name = p[1, 31]
      nul = name.index(0_u8)
      !nul.nil? && nul > 0 && nul < 31
    end

    def self.validate_response!(descriptor : CommandDescriptor, payload : Bytes, command : Bytes? = nil, phase : Int32 = 0) : ResponseDisposition
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
      raise ProtocolError.new("empty upstream payload") if p.empty?
      raise ProtocolError.new("oversized upstream payload") if p.size > MAX_PAYLOAD
      n = p.size
      ok = case p[0]
           when 0x00, 0x0a, 0x0f then n == 1
           when 0x01             then n == 2
           when 0x02, 0x04, 0x09 then n == 5
           when 0x03, 0x8a       then n == 148
           when 0x05             then n >= 58
           when 0x06             then n == 10
           when 0x07             then contact_text_response_valid?(p, 13, 8)
           when 0x08             then n >= 8
           when 0x0b             then n >= 2
           when 0x0c             then n == 11
           when 0x0d             then n == 82
           when 0x0e, 0x14       then n == 65
           when 0x10             then contact_text_response_valid?(p, 16, 11)
           when 0x11             then n >= 11
           when 0x12             then n == 50
           when 0x13             then n == 6
           when 0x15             then n >= 1
           when 0x16             then advert_path_response_valid?(p)
           when 0x17             then n == 9
           when 0x18             then stats_response_valid?(p)
           when 0x19             then n == 3
           when 0x1a             then n >= 1 && (n - 1) % 8 == 0
           when 0x1b             then n >= 9 && n == 9 + p[8]
           when 0x1c             then n == 1 || n == 48
           when 0x80, 0x81       then n == 33 # opcode plus native public key
           when 0x82             then n == 9
           when 0x84, 0x8e       then n >= 4 # native raw/control metadata prefix
           when 0x88             then n >= 3 # opcode, SNR, RSSI; opaque packet follows
           when 0x83, 0x90       then n == 1
           when 0x8f             then n == 33
           when 0x85             then n == 8 || n >= 14
           when 0x86, 0x8b       then n >= 8
           when 0x87             then n >= 9
           when 0x8d             then path_discovery_response_valid?(p)
           when 0x89             then trace_response_valid?(p)
           when 0x8c             then n >= 6
           else                       p[0] >= 0x80 # unknown pushes are opaque; unknown ordinary responses are fatal
           end
      raise ProtocolError.new("malformed response 0x#{p[0].to_s(16)} (#{n} bytes)") unless ok
    end

    private def self.stats_response_valid?(p : Bytes) : Bool
      return false if p.size < 2
      case p[1]
      when 0 then p.size == 11
      when 1 then p.size == 14
      when 2 then p.size == 30
      else        false
      end
    end

    private def self.trace_response_valid?(p : Bytes) : Bool
      return false if p.size < 13
      path_bytes = p[2].to_i
      shift = p[3] & 0x03
      path_bytes % (1 << shift) == 0 && p.size == 12 + path_bytes + (path_bytes >> shift) + 1
    end

    private def self.normal_encoded_path_bytes(encoded : UInt8) : Int32?
      # Packet::getPathHashSize() in the pinned native firmware defines ordinary
      # path width as upper-bits + 1 (1, 2, 3; 4 is reserved). Trace flags are a
      # distinct format whose low bits select powers-of-two widths.
      count = (encoded & 0x3f).to_i
      width = (encoded >> 6).to_i + 1
      return nil if width == 4
      bytes = count * width
      bytes <= MAX_PATH_SIZE ? bytes : nil
    end

    private def self.advert_path_response_valid?(p : Bytes) : Bool
      return false if p.size < 6
      path_size = normal_encoded_path_bytes(p[5]) || return false
      p.size == 6 + path_size
    end

    private def self.path_discovery_response_valid?(p : Bytes) : Bool
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
      return false if p.size < base_size
      p[type_offset] != 2 || p.size >= base_size + 4
    end

    def self.downgrade_inbox(payload : Bytes, target_version : UInt8) : Bytes
      validate_response_shape!(payload)
      return payload.dup if target_version >= 3
      case payload[0]
      when 0x10
        Bytes.new(payload.size - 3) do |i|
          i == 0 ? 0x07_u8 : payload[i + 3]
        end
      when 0x11
        Bytes.new(payload.size - 3) do |i|
          i == 0 ? 0x08_u8 : payload[i + 3]
        end
      else
        payload.dup
      end
    end

    def self.app_start_payload(app_name : String, reserved : Bytes = Bytes.new(7, 0_u8)) : Bytes
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
      Bytes[22_u8, target]
    end

    def self.normalize_device_query(payload : Bytes, target : UInt8 = NATIVE_PROTOCOL_LEVEL) : Bytes
      result = validate_command(payload)
      raise ArgumentError.new("malformed DEVICE_QUERY") unless result.valid? && payload[0] == 22
      copy = payload.dup
      copy[1] = target
      copy
    end

    def self.validate_self_info!(payload : Bytes) : Bytes
      raise ProtocolError.new("malformed SELF_INFO") unless payload.size >= 58 && payload[0] == 0x05
      payload[4, 32].dup
    end

    def self.validate_device_info!(payload : Bytes, expected_protocol : UInt8 = NATIVE_PROTOCOL_LEVEL) : UInt8
      raise ProtocolError.new("malformed DEVICE_INFO") unless payload.size == 82 && payload[0] == 0x0d
      actual = payload[1]
      raise ProtocolError.new("unsupported firmware protocol level #{actual}; expected #{expected_protocol}") unless actual == expected_protocol
      actual
    end
  end
end
