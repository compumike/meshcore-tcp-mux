require "socket"
require "./config"
require "./frame_codec"

class MeshCoreTCPMux
  class Transport
    record Frame, endpoint : Int64, payload : Bytes
    record Closed, endpoint : Int64, reason : String, category : Symbol = :normal
    record Written, endpoint : Int64, epoch : Int64, write_id : Int64
    record WriteFailed, endpoint : Int64, epoch : Int64, write_id : Int64, reason : String
    alias Event = Frame | Closed | Written | WriteFailed

    record Write, epoch : Int64, write_id : Int64, payload : Bytes

    class Endpoint
      # Owns a socket. No other fiber reads or writes it after start.
      getter id : Int64
      getter socket : TCPSocket

      @writes : Channel(Write)
      @cancel = Channel(Nil).new
      @done = Channel(Nil).new(2)
      @started = false
      @stopped = false

      def initialize(
        @socket : TCPSocket,
        @id : Int64,
        @input_marker : UInt8,
        @output_marker : UInt8,
        @events : Channel(Event),
        @frame_timeout : Time::Span,
        write_timeout : Time::Span,
        output_capacity : Int32,
      )
        raise ArgumentError.new("output capacity must be positive") unless output_capacity > 0
        @writes = Channel(Write).new(output_capacity)
        @socket.tcp_nodelay = true
        @socket.read_timeout = 200.milliseconds
        @socket.write_timeout = write_timeout
      end

      def start : Nil
        # Starts the reader fiber before the writer fiber. The scheduler observes
        # that order when this caller next yields.
        return if @started
        @started = true
        start_reader
        start_writer
      end

      def enqueue(write : Write) : Bool
        # Never blocks the broker. A full writer queue is a write failure owned by
        # the runtime, not permission to accumulate unbounded bytes.
        return false if @stopped
        select
        when @writes.send(write)
          true
        else
          false
        end
      rescue Channel::ClosedError
        false
      end

      def stop : Nil
        return if @stopped
        @stopped = true
        @cancel.close
        @writes.close
        @socket.close rescue nil
        if @started
          2.times { @done.receive }
        end
      end

      private def start_reader : Nil
        spawn do
          decoder = FrameCodec::Decoder.new(@input_marker, @frame_timeout)
          buffer = Bytes.new(1024)
          begin
            loop do
              begin
                count = @socket.read(buffer)
                if count == 0
                  decoder.finish
                  raise IO::EOFError.new("peer disconnected")
                end
                decoder.feed(buffer[0, count], Clock.now) do |payload|
                  break unless publish(Frame.new(@id, payload))
                end
              rescue IO::TimeoutError
                decoder.check_deadline(Clock.now)
              end
              break if @stopped
            end
          rescue ex
            category = ex.is_a?(FrameCodec::Error) ? :malformed : :normal
            publish(Closed.new(@id, ex.message || ex.class.name, category)) unless @stopped
          ensure
            @done.send(nil)
          end
        end
      end

      private def start_writer : Nil
        spawn do
          begin
            loop do
              write = select
              when value = @writes.receive?
                break unless value
                value
              when @cancel.receive?
                break
              end
              begin
                @socket.write(FrameCodec.encode(write.payload, @output_marker))
                break unless publish(Written.new(@id, write.epoch, write.write_id))
              rescue ex
                publish(WriteFailed.new(@id, write.epoch, write.write_id, ex.message || ex.class.name)) unless @stopped
                break
              end
            end
          ensure
            @done.send(nil)
          end
        end
      end

      private def publish(event : Event) : Bool
        select
        when @events.send(event)
          true
        when @cancel.receive?
          false
        end
      rescue Channel::ClosedError
        false
      end
    end
  end
end
