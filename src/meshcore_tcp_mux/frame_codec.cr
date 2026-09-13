class MeshCoreTCPMux
  class FrameCodec
    MAX_PAYLOAD_SIZE           = 176
    CLIENT_TO_COMPANION_MARKER = '<'.ord.to_u8
    COMPANION_TO_CLIENT_MARKER = '>'.ord.to_u8

    class Error < Exception
    end

    class MalformedFrameError < Error
    end

    class FrameDeadlineExceededError < Error
    end

    class TruncatedFrameError < Error
    end

    class Decoder
      # Incrementally removes the TCP envelope and returns owned payloads.
      #
      # `now` must come from a monotonic clock. The decoder deliberately does not
      # read a clock itself, which keeps deadline handling deterministic and lets a
      # connection's timer fiber use `check_deadline` while no bytes arrive.
      getter marker : UInt8
      getter assembly_timeout : Time::Span

      @header = Bytes.new(3)
      @header_size = 0
      @payload : Bytes? = nil
      @payload_size = 0
      @started_at : Time::Span? = nil

      def initialize(@marker : UInt8, @assembly_timeout : Time::Span = 5.seconds)
        FrameCodec.validate_marker!(@marker)
        raise ArgumentError.new("assembly timeout must be positive") unless @assembly_timeout > Time::Span.zero
      end

      def feed(bytes : Bytes, now : Time::Span, & : Bytes ->) : Nil
        # Accepts any portion of the stream and yields every payload completed by
        # it. Yielded slices never alias `bytes` or the decoder's scratch space.
        check_deadline(now)
        offset = 0

        while offset < bytes.size
          start_frame(now) if @header_size == 0 && @payload.nil?

          if @header_size < 3
            @header[@header_size] = bytes[offset]
            @header_size += 1
            offset += 1

            validate_marker! if @header_size == 1
            allocate_payload! if @header_size == 3
          else
            payload = @payload.not_nil!
            available = bytes.size - offset
            needed = payload.size - @payload_size
            count = Math.min(available, needed)
            payload[@payload_size, count].copy_from(bytes[offset, count])
            @payload_size += count
            offset += count
          end

          if payload = @payload
            if @payload_size == payload.size
              yield payload
              reset
            end
          end
        end
      end

      def check_deadline(now : Time::Span) : Nil
        # Raises once the absolute assembly deadline has elapsed. An idle decoder
        # has no deadline.
        if started_at = @started_at
          if now - started_at >= @assembly_timeout
            raise FrameDeadlineExceededError.new("frame assembly deadline exceeded")
          end
        end
      end

      def finish : Nil
        # Validates EOF. A clean frame boundary is valid; any retained byte means
        # the peer disconnected in the middle of a frame.
        return unless partial?
        raise TruncatedFrameError.new("EOF in incomplete frame")
      end

      def partial? : Bool
        @header_size != 0 || !@payload.nil?
      end

      private def start_frame(now : Time::Span) : Nil
        @started_at = now
      end

      private def validate_marker! : Nil
        actual = @header[0]
        expected = @marker
        return if actual == expected
        raise MalformedFrameError.new("wrong frame marker 0x#{actual.to_s(16)}; expected 0x#{expected.to_s(16)}")
      end

      private def allocate_payload! : Nil
        size = @header[1].to_i | (@header[2].to_i << 8)
        unless 1 <= size <= MAX_PAYLOAD_SIZE
          raise MalformedFrameError.new("payload length #{size} is outside 1..#{MAX_PAYLOAD_SIZE}")
        end
        @payload = Bytes.new(size)
        @payload_size = 0
      end

      private def reset : Nil
        @header_size = 0
        @payload = nil
        @payload_size = 0
        @started_at = nil
      end
    end

    def self.encode(payload : Bytes, marker : UInt8) : Bytes
      validate_marker!(marker)
      size = payload.size
      unless 1 <= size <= MAX_PAYLOAD_SIZE
        raise ArgumentError.new("payload length #{size} is outside 1..#{MAX_PAYLOAD_SIZE}")
      end

      frame = Bytes.new(size + 3)
      frame[0] = marker
      frame[1] = (size & 0xff).to_u8
      frame[2] = ((size >> 8) & 0xff).to_u8
      frame[3, size].copy_from(payload)
      frame
    end

    def self.validate_marker!(marker : UInt8) : Nil
      return if marker == CLIENT_TO_COMPANION_MARKER || marker == COMPANION_TO_CLIENT_MARKER
      raise ArgumentError.new("frame marker must be '<' or '>'")
    end
  end
end
