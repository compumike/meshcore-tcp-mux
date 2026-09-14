require "./protocol"

class MeshCoreTCPMux
  # Namespace for the TCP multiplexer: transport, protocol validation, and per-client state.
  class Startup
    # Synchronizes a newly connected companion before Runtime admits downstream clients.
    # It fences off stale replies, verifies the protocol profile, and resets temporary flood scope.
    # Native TCP retains at most four outgoing frames across client replacement.
    # Five consecutive SELF_INFO replies followed by DEVICE_INFO therefore prove
    # that we have crossed the newly submitted handshake, not just stale output.
    class Error < Exception
      # Startup could not establish a safe, synchronized companion connection.
    end

    getter self_key : Bytes?
    getter device_info : Bytes?
    getter deadline : Time::Span
    @self_run = 0
    @awaiting_scope = false
    @ready = false

    def initialize(now : Time::Span, timeout = 15.seconds) : Nil
      @deadline = now + timeout
    end

    def self.app_start : Bytes
      # Use the canonical APP_START layout, including its seven reserved bytes.
      # The trailing application name identifies this connection in firmware debug logs.
      Protocol.app_start_payload("meshcore-tcp-mux")
    end

    def self.probes : Array(Bytes)
      # Five APP_START commands fence the four-frame stale queue; DEVICE_QUERY (0x16) requests native v13.
      Array.new(5) { app_start } << Protocol.device_query_payload
    end

    def ready? : Bool
      @ready
    end

    def check_deadline(now : Time::Span) : Nil
      raise Error.new("startup synchronization timeout") if !@ready && now >= @deadline
    end

    def receive(payload : Bytes, now : Time::Span) : Bytes?
      # Returns the next internal command, if any. No startup output is public.
      check_deadline(now)
      raise Error.new("empty startup response") if payload.empty?
      return nil if payload[0] >= 0x80 # 0x80 starts the asynchronous push-code range.
      raise Error.new("ordinary response after startup boundary") if @ready
      if @awaiting_scope
        raise Error.new("startup scope reset rejected") unless payload == Bytes[0] # OK: temporary scope has been reset.
        @ready = true
      elsif payload[0] == 5 # 5 = SELF_INFO.
        @self_key = Protocol.validate_self_info!(payload)
        @self_run += 1
      elsif payload[0] == 0x0d && @self_run >= 5 # 0x0d = DEVICE_INFO.
        Protocol.validate_device_info!(payload)
        @device_info = payload.dup
        @awaiting_scope = true
        return Bytes[0x36, 0] # SET_FLOOD_SCOPE_KEY: mode 0 without a key restores the default scope.
      else
        @self_run = 0
        @self_key = nil
      end
      nil
    rescue ex : Protocol::ProtocolError
      # Preserve the startup error boundary for both the probe and daemon.
      raise Error.new(ex.message || "invalid startup response")
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
