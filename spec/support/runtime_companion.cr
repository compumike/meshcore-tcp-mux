require "../../src/meshcore_tcp_mux/config"
require "../../src/meshcore_tcp_mux/frame_codec"
require "./native_startup"

class SpecSupport
  # Namespace for fake companion transports shared by the integration specs.
  class RuntimeCompanion
    # Scripted loopback companion: startup is automatic, then each command waits
    # for a test directive. Drop omits a reply but keeps the socket open; RawAndClose
    # writes exactly the supplied wire bytes before disconnecting. Disconnect
    # closes immediately without a reply. Identity bytes
    # select synthetic public keys across reconnects, not real radio identities.

    record Command, epoch : Int32, payload : Bytes
    record Reply, payload : Bytes
    record RawAndClose, bytes : Bytes

    struct Drop
    end

    class Disconnect
      # Ends TCP after receiving a command but before any response, separating
      # an immediate EOF from the missing-reply timeout modeled by Drop.
    end

    alias Directive = Reply | RawAndClose | Drop | Disconnect

    getter port : Int32
    getter ready = Channel(Int32).new(8)
    getter commands = Channel(Command).new(32)
    getter directives = Channel(Directive).new(32)

    @server : TCPServer
    @identities : Array(UInt8)
    @done = Channel(Nil).new(1)
    @stopped = false
    @socket : TCPSocket? = nil

    def initialize(@identities : Array(UInt8) = [0xa5_u8]) : Nil
      # Default fake identity marker; later epochs reuse the last supplied value.
      @server = TCPServer.new("127.0.0.1", 0)
      @port = @server.local_address.port
      spawn run
    end

    def reply(payload : Bytes) : Nil
      @directives.send(Reply.new(payload))
    end

    def drop : Nil
      # Omit this command's reply while leaving TCP connected for timeout tests.
      @directives.send(Drop.new)
    end

    def disconnect : Nil
      # Close the current connection when its received command is classified.
      @directives.send(Disconnect.new)
    end

    def raw_and_close(bytes : Bytes) : Nil
      @directives.send(RawAndClose.new(bytes))
    end

    def push(payload : Bytes) : Nil
      socket = @socket || raise "companion is not connected"
      write(socket, payload)
    end

    def stop : Nil
      return if @stopped
      @stopped = true
      @socket.try &.close
      @server.close
      @done.receive
    end

    private def run : Nil
      epoch = 0
      begin
        until @stopped
          socket = @server.accept
          @socket = socket
          epoch += 1
          serve(socket, epoch)
          socket.close rescue nil
          @socket = nil
        end
      rescue ex : IO::Error
        raise ex unless @stopped
      ensure
        @done.send(nil)
      end
    end

    private def serve(socket : TCPSocket, epoch : Int32) : Nil
      identity = @identities[epoch - 1]? || @identities.last
      decoder = MeshCoreTCPMux::FrameCodec::Decoder.new(MeshCoreTCPMux::FrameCodec::CLIENT_TO_COMPANION_MARKER)
      # Socket read scratch space; capacity is arbitrary and is not a protocol field.
      buffer = Bytes.new(1024)
      startup_complete = false

      loop do
        count = socket.read(buffer)
        break if count == 0
        close_after_frame = false
        decoder.feed(buffer[0, count], MeshCoreTCPMux::Clock.now) do |payload|
          unless startup_complete
            case payload[0]
            when 1 # APP_START.
              write(socket, NativeStartupTransport.self_info(identity))
            when 0x16 # DEVICE_QUERY.
              write(socket, NativeStartupTransport.device_info)
            when 0x36 # SET_FLOOD_SCOPE_KEY.
              # OK (0x00): startup default scope is restored.
              write(socket, Bytes[0_u8])
              startup_complete = true
              @ready.send(epoch)
            else
              raise "unexpected startup command #{payload[0]}"
            end
            next
          end

          @commands.send(Command.new(epoch, payload))
          case directive = @directives.receive
          when Reply
            write(socket, directive.payload)
          when RawAndClose
            socket.write(directive.bytes)
            close_after_frame = true
          when Disconnect
            close_after_frame = true
          when Drop
          end
        end
        break if close_after_frame
      end
    end

    private def write(socket : TCPSocket, payload : Bytes) : Nil
      socket.write(MeshCoreTCPMux::FrameCodec.encode(payload, MeshCoreTCPMux::FrameCodec::COMPANION_TO_CLIENT_MARKER))
    end
  end
end
