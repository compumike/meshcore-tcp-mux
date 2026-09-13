require "../../src/meshcore_tcp_mux/frame_codec"

class SpecSupport
  class NativeStartupTransport
    # Synthetic firmware fixtures shared by startup and runtime specs. Payloads
    # exclude the TCP envelope. Distinguishing bytes identify fake epochs/contacts;
    # none of the keys, names, or response fields comes from a physical companion.

    # Deterministic model of the pinned SerialWifiInterface / MyMesh startup path.
    # One `tick` is one firmware loop: send one queued frame, otherwise read one
    # command, otherwise advance an old contacts iterator once.
    MAX_RETAINED_FRAMES = 4

    getter actions = [] of Symbol
    getter commands_received = [] of Bytes
    getter payloads_sent = [] of Bytes
    getter responses_dropped = [] of Int32

    @send_queue : Array(Bytes)
    @commands = [] of Bytes
    @contacts : Array(Bytes)
    @iterator_active : Bool
    @request_decoder : MeshCoreTCPMux::FrameCodec::Decoder
    @drop_responses : Array(Int32)
    @pushes_after_commands : Array(Bytes)
    @handled_commands = 0

    def initialize(
      retained : Array(Bytes) = [] of Bytes,
      contacts : Array(Bytes) = [] of Bytes,
      @drop_responses : Array(Int32) = [] of Int32,
      @pushes_after_commands : Array(Bytes) = [] of Bytes,
    )
      raise ArgumentError.new("native retained queue holds at most four frames") if retained.size > MAX_RETAINED_FRAMES
      @send_queue = retained.map(&.dup)
      @contacts = contacts.map(&.dup)
      @iterator_active = !contacts.empty?
      @request_decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER)
    end

    def reconnect : Nil
      # A replacement TCP client gets a fresh receive header/decoder. The native
      # send queue and the higher-level contacts iterator deliberately survive.
      @request_decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER)
    end

    def client_write(bytes : Bytes, now : Time::Span) : Nil
      @request_decoder.feed(bytes, now) do |payload|
        @commands << payload
      end
    end

    def tick : Bytes?
      # Returns at most one framed companion-to-client response.
      if payload = @send_queue.shift?
        @actions << :send
        @payloads_sent << payload
        return MeshCoreTCPMux::FrameCodec.encode(payload, MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER)
      end

      if command = @commands.shift?
        @actions << :read_command
        @commands_received << command
        handle(command)
        return nil
      end

      if @iterator_active
        @actions << :iterate_contacts
        if contact = @contacts.shift?
          enqueue(contact)
        else
          # END_OF_CONTACTS (0x04), followed by the u32 LE last-modified timestamp.
          enqueue(Bytes[0x04_u8, 0_u8, 0_u8, 0_u8, 0_u8])
          @iterator_active = false
        end
      else
        @actions << :idle
      end
      nil
    end

    def push(payload : Bytes) : Bool
      # Models an asynchronous firmware push entering the same bounded send queue.
      enqueue(payload)
    end

    def iterator_active? : Bool
      @iterator_active
    end

    def pending? : Bool
      !@send_queue.empty? || !@commands.empty? || @iterator_active
    end

    private def handle(command : Bytes) : Nil
      response = case command[0]?
                 when 0x01
                   @iterator_active = false
                   self.class.self_info
                 when 0x16
                   self.class.device_info
                 when 0x36
                   # SET_FLOOD_SCOPE_KEY (54/0x36), mode 0 with no key: restore the configured
                   # default scope.
                   # OK (0x00): command accepted, not proof of radio delivery.
                   # ERR (0x01), ILLEGAL_ARG.
                   command == Bytes[0x36_u8, 0_u8] ? Bytes[0_u8] : Bytes[1_u8, 6_u8]
                 else
                   # ERR (0x01), UNSUPPORTED_CMD.
                   Bytes[1_u8, 1_u8]
                 end

      response_number = @handled_commands
      @handled_commands += 1
      if @drop_responses.includes?(response_number)
        @responses_dropped << response_number
      else
        enqueue(response)
      end
      if push = @pushes_after_commands[response_number]?
        enqueue(push)
      end
    end

    private def enqueue(payload : Bytes) : Bool
      return false if @send_queue.size >= MAX_RETAINED_FRAMES
      @send_queue << payload.dup
      true
    end

    def self.self_info(identity_byte : UInt8 = 0xa5_u8) : Bytes
      # SELF_INFO: minimum 58-byte response; opcode 5, public key occupies offsets 4..35.
      Bytes.new(58, 0_u8).tap do |bytes|
        bytes[0] = 0x05
        bytes[4] = identity_byte
      end
    end

    def self.device_info(distinguishing_byte : UInt8 = 0xa5_u8) : Bytes
      # DEVICE_INFO: exactly 82 bytes, opcode 0x0d, protocol version 13 at offset 1; other
      # fields are synthetic.
      Bytes.new(82, 0_u8).tap do |bytes|
        bytes[0] = 0x0d
        bytes[1] = 13
        bytes[2] = distinguishing_byte
      end
    end

    def self.contact(identity_byte : UInt8) : Bytes
      # CONTACT record: 148 bytes total (opcode 3 plus native contact fields); unused fields are
      # synthetic zeroes.
      Bytes.new(148, 0_u8).tap do |bytes|
        bytes[0] = 0x03
        bytes[1] = identity_byte
      end
    end
  end
end
