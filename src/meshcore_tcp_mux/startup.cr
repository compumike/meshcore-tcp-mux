class MeshCoreTCPMux
  class Startup
    class Error < Exception
      # Native TCP retains at most four outgoing frames across client replacement.
      # Five consecutive SELF_INFO replies followed by DEVICE_INFO therefore prove
      # that we have crossed the newly submitted handshake, not just stale output.
    end

    getter self_info : Bytes?
    getter device_info : Bytes?
    getter deadline : Time::Span
    @self_run = 0
    @awaiting_scope = false
    @ready = false

    def initialize(now : Time::Span, timeout = 15.seconds)
      @deadline = now + timeout
    end

    def self.app_start : Bytes
      name = "meshcore-mux".to_slice
      payload = Bytes.new(8 + name.size, 0)
      payload[0] = 1
      payload[8, name.size].copy_from(name)
      payload
    end

    def self.probes : Array(Bytes)
      Array.new(5) { app_start } << Bytes[0x16, 13]
    end

    def ready? : Bool
      @ready
    end

    def check_deadline(now : Time::Span)
      raise Error.new("startup synchronization timeout") if !@ready && now >= @deadline
    end

    def receive(payload : Bytes, now : Time::Span) : Bytes?
      # Returns the next internal command, if any. No startup output is public.
      check_deadline(now)
      raise Error.new("empty startup response") if payload.empty?
      return nil if payload[0] >= 0x80
      raise Error.new("ordinary response after startup boundary") if @ready
      if @awaiting_scope
        raise Error.new("startup scope reset rejected") unless payload == Bytes[0]
        @ready = true
      elsif payload[0] == 5
        raise Error.new("short SELF_INFO") if payload.size < 58
        @self_run += 1
        @self_info = payload.dup
      elsif payload[0] == 0x0d && @self_run >= 5
        raise Error.new("invalid native_v13 DEVICE_INFO") unless payload.size == 82 && payload[1] == 13
        @device_info = payload.dup
        @awaiting_scope = true
        return Bytes[0x36, 0]
      else
        @self_run = 0
        @self_info = nil
      end
      nil
    end

    def identification : String
      info = @device_info || raise Error.new("device has not been identified")
      "profile=native_v13 protocol=#{info[1]} model=#{field(info, 20, 40)} firmware=#{field(info, 60, 20)} build=#{field(info, 8, 12)}"
    end

    private def field(info : Bytes, offset : Int32, length : Int32) : String
      # Inspect only public identification fields; DEVICE_INFO also has a PIN.
      String.new(info[offset, length]).split('\0', 2)[0].inspect
    end
  end
end
