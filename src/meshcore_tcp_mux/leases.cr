module MeshCoreTCPMux
  private module LeaseParsing
    private def read_u32(payload : Bytes, offset : Int32) : UInt32
      raise ArgumentError.new("payload too short") if payload.size < offset + 4
      payload[offset].to_u32 |
        (payload[offset + 1].to_u32 << 8) |
        (payload[offset + 2].to_u32 << 16) |
        (payload[offset + 3].to_u32 << 24)
    end

    private def require_payload(payload : Bytes, opcode : UInt8, size : Int32) : Nil
      raise ArgumentError.new("expected #{size}-byte payload #{opcode}") unless payload.size == size && payload[0] == opcode
    end

    private def require_command(command : Bytes, opcode : UInt8, minimum_size : Int32) : Nil
      raise ArgumentError.new("invalid command #{opcode}") if command.size < minimum_size || command[0] != opcode
    end
  end

  # The firmware's acknowledgement table is a ring, not a pool.  Keeping the
  # insertion position is therefore as important as keeping the entries.
  class DmRing
    include LeaseParsing
    CAPACITY = 8

    private class Entry
      getter token : UInt32
      getter deadline : Time::Span
      property settled : Bool

      def initialize(@token, @deadline, @settled = false)
      end
    end

    @slots = Array(Entry?).new(CAPACITY, nil)
    getter next_slot = 0

    def available?(now : Time::Span) : Bool
      entry = @slots[@next_slot]
      entry.nil? || entry.settled || now >= entry.deadline
    end

    # Records an actual firmware SENT response. A zero token consumes no
    # physical ring position. The caller must have checked available? before
    # dispatching the corresponding plain DM.
    def accepted(sent : Bytes, now : Time::Span) : Nil
      require_payload(sent, 0x06, 10)
      token = read_u32(sent, 2)
      return if token == 0
      raise InvalidStateError.new("next DM acknowledgement slot is occupied") unless available?(now)

      timeout_ms = read_u32(sent, 6)
      @slots[@next_slot] = Entry.new(token, LeaseTime.deadline(now, timeout_ms))
      @next_slot = (@next_slot + 1) % CAPACITY
    end

    # Settles every equal token. Four-byte acknowledgement hashes are not
    # unique, and retaining only the first match would manufacture a stronger
    # identity guarantee than the firmware provides.
    def confirm(push : Bytes) : Bool
      require_payload(push, 0x82, 9)
      token = read_u32(push, 1)
      matched = false
      @slots.each do |entry|
        if entry && !entry.settled && entry.token == token
          entry.settled = true
          matched = true
        end
      end
      matched
    end

    def pending_count(now : Time::Span) : Int32
      @slots.count { |entry| entry && !entry.settled && now < entry.deadline }
    end
  end

  class RemoteLease
    include LeaseParsing
    enum Kind
      Login
      Status
      Trace
      Telemetry
      Binary
      PathDiscovery
      Anonymous
    end

    getter owner : Int64?
    getter kind : Kind?
    getter command : Bytes?
    getter deadline : Time::Span?

    @peer : Bytes?
    @tag : UInt32?
    @trace_auth : UInt32?
    @tentative = false

    def occupied?(now : Time::Span) : Bool
      expire(now)
      !@kind.nil?
    end

    def tentative? : Bool
      @tentative
    end

    # Returns false for a resource conflict. Commands are required to be a
    # protected remote command and long enough for the fields inspected here;
    # full command grammar validation belongs to the protocol descriptor.
    def reserve(owner : Int64, command : Bytes, now : Time::Span) : Bool
      return false if occupied?(now)
      raise ArgumentError.new("empty remote command") if command.empty?

      kind, peer, tag, auth = classify(command)
      @owner = owner
      @kind = kind
      @command = command.dup
      @peer = peer
      @tag = tag
      @trace_auth = auth
      @deadline = nil
      @tentative = true
      true
    end

    def accepted(sent : Bytes, now : Time::Span) : Nil
      raise InvalidStateError.new("no tentative remote reservation") unless @kind && @tentative
      require_payload(sent, 0x06, 10)
      @tag = read_u32(sent, 2) unless @kind == Kind::Trace
      @deadline = LeaseTime.deadline(now, read_u32(sent, 6))
      @tentative = false
    end

    # An immediate firmware error means no asynchronous operation was accepted.
    def rejected : Nil
      clear
    end

    # Returns the live owner for a matching result. A match always releases the
    # lease, including when the owner has disconnected and the return is nil.
    def match(push : Bytes, now : Time::Span) : Int64?
      expire(now)
      return nil unless kind = @kind
      return nil if @tentative
      return nil unless matches?(kind, push)

      result = @owner
      clear
      result
    end

    # Radio state survives a downstream connection. Preserve the reservation,
    # but make a later matching result undeliverable.
    def owner_gone(owner : Int64) : Nil
      @owner = nil if @owner == owner
    end

    def expire(now : Time::Span) : Bool
      deadline = @deadline
      return false unless deadline && now >= deadline
      clear
      true
    end

    private def classify(command : Bytes) : {Kind, Bytes?, UInt32?, UInt32?}
      case command[0]
      when 26_u8
        {Kind::Login, peer_at(command, 1), nil, nil}
      when 27_u8
        {Kind::Status, peer_at(command, 1), nil, nil}
      when 36_u8
        raise ArgumentError.new("short trace command") if command.size < 11
        {Kind::Trace, nil, read_u32(command, 1), read_u32(command, 5)}
      when 39_u8
        raise ArgumentError.new("self telemetry does not use a remote lease") if command.size == 4
        {Kind::Telemetry, peer_at(command, 4), nil, nil}
      when 50_u8
        {Kind::Binary, peer_at(command, 1), nil, nil}
      when 52_u8
        {Kind::PathDiscovery, peer_at(command, 2), nil, nil}
      when 57_u8
        {Kind::Anonymous, peer_at(command, 1), nil, nil}
      else
        raise ArgumentError.new("command does not use the remote lease")
      end
    end

    private def peer_at(command : Bytes, offset : Int32) : Bytes
      raise ArgumentError.new("short remote command") if command.size < offset + 6
      command[offset, 6].dup
    end

    private def matches?(kind : Kind, push : Bytes) : Bool
      case kind
      when Kind::Login
        return false unless push[0]? == 0x85 || push[0]? == 0x86
        matching_peer?(push)
      when Kind::Status
        return false unless push[0]? == 0x87
        matching_peer?(push)
      when Kind::Telemetry
        return false unless push[0]? == 0x8b
        matching_peer?(push)
      when Kind::Binary, Kind::Anonymous
        return false unless push[0]? == 0x8c
        raise ArgumentError.new("short binary response") if push.size < 6
        read_u32(push, 2) == @tag
      when Kind::PathDiscovery
        return false unless push[0]? == 0x8d
        matching_peer?(push)
      when Kind::Trace
        return false unless push[0]? == 0x89
        raise ArgumentError.new("short trace response") if push.size < 12
        read_u32(push, 4) == @tag && read_u32(push, 8) == @trace_auth
      else
        false
      end
    end

    private def matching_peer?(push : Bytes) : Bool
      raise ArgumentError.new("short peer response") if push.size < 8
      push[2, 6] == @peer
    end

    private def clear : Nil
      @owner = nil
      @kind = nil
      @command = nil
      @peer = nil
      @tag = nil
      @trace_auth = nil
      @deadline = nil
      @tentative = false
    end
  end

  class SigningLease
    include LeaseParsing
    INACTIVITY_TIMEOUT = 30.seconds

    enum Admission
      Allowed
      BadState
      TableFull
    end

    getter owner : Int64?
    getter limit : UInt64?
    getter accepted_bytes = 0_u64

    @tentative = false
    @last_activity : Time::Span?
    @pending_data_bytes : UInt64?
    @finish_pending = false

    def initialize(@inactivity_timeout : Time::Span = INACTIVITY_TIMEOUT)
    end

    def occupied?(now : Time::Span) : Bool
      expire(now)
      !@owner.nil?
    end

    # A start from the current owner is an explicit restart. It immediately
    # makes data/finish ineligible until the real SIGN_START response arrives.
    def start(owner : Int64, command : Bytes, now : Time::Span) : Bool
      require_command(command, 0x21, 1)
      expire(now)
      return false if @owner && @owner != owner

      @owner = owner
      @limit = nil
      @accepted_bytes = 0
      @pending_data_bytes = nil
      @finish_pending = false
      @tentative = true
      @last_activity = now
      true
    end

    def accepted_start(response : Bytes, now : Time::Span) : Nil
      raise InvalidStateError.new("no tentative signing start") unless @owner && @tentative
      require_payload(response, 0x13, 6)
      @limit = read_u32(response, 2).to_u64
      @tentative = false
      @last_activity = now
    end

    def rejected_start : Nil
      clear
    end

    def begin_data(owner : Int64, command : Bytes, now : Time::Span) : Admission
      raise ArgumentError.new("SIGN_DATA must contain data") if command.size < 2 || command[0] != 0x22
      return Admission::BadState unless usable_by?(owner, now)
      bytes = (command.size - 1).to_u64
      maximum = @limit.not_nil!
      return Admission::TableFull if bytes > maximum - @accepted_bytes

      @pending_data_bytes = bytes
      @last_activity = now
      Admission::Allowed
    end

    def data_response(response : Bytes, now : Time::Span) : Nil
      bytes = @pending_data_bytes || raise InvalidStateError.new("no pending signing data")
      validate_ok_or_err(response)
      @pending_data_bytes = nil
      if response[0] == 0x00
        @accepted_bytes += bytes
        @last_activity = now
      elsif response[1] == 0x04
        clear
      else
        @last_activity = now
      end
    end

    def begin_finish(owner : Int64, command : Bytes, now : Time::Span) : Admission
      require_command(command, 0x23, 1)
      return Admission::BadState unless usable_by?(owner, now)
      @finish_pending = true
      @last_activity = now
      Admission::Allowed
    end

    def finish_response(response : Bytes, now : Time::Span) : Nil
      raise InvalidStateError.new("no pending signing finish") unless @finish_pending
      if response[0]? == 0x14
        require_payload(response, 0x14, 65)
        clear
      else
        validate_ok_or_err(response, allow_ok: false)
        @finish_pending = false
        if response[1] == 0x04
          clear
        else
          @last_activity = now
        end
      end
    end

    def owner_gone(owner : Int64) : Nil
      clear if @owner == owner
    end

    def expire(now : Time::Span) : Bool
      last = @last_activity
      return false unless @owner && last && now >= last + @inactivity_timeout
      clear
      true
    end

    private def usable_by?(owner : Int64, now : Time::Span) : Bool
      expire(now)
      @owner == owner && !@tentative && !@limit.nil? && @pending_data_bytes.nil? && !@finish_pending
    end

    private def validate_ok_or_err(response : Bytes, allow_ok = true) : Nil
      raise ArgumentError.new("empty signing response") if response.empty?
      return if allow_ok && response.size == 1 && response[0] == 0x00
      return if response.size == 2 && response[0] == 0x01
      raise ArgumentError.new("unexpected signing response")
    end

    private def clear : Nil
      @owner = nil
      @limit = nil
      @accepted_bytes = 0
      @tentative = false
      @last_activity = nil
      @pending_data_bytes = nil
      @finish_pending = false
    end
  end

  class InvalidStateError < Exception
  end

  private module LeaseTime
    extend self

    def deadline(now : Time::Span, suggested_timeout_ms : UInt32) : Time::Span
      retained_ms = suggested_timeout_ms.to_i64 * 5 // 4 + 1_000
      now + Math.max(5_000_i64, retained_ms).milliseconds
    end
  end
end
