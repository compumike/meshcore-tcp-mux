require "deque"
require "set"
require "./protocol"

class MeshCoreTCPMux
  # Remembers recently delivered logical text messages from one physical companion.
  # The companion strips the radio retry attempt before exposing an inbox frame,
  # so retries have the same sender/channel, text type, timestamp, and message
  # bytes even when their radio path or receive SNR differs.
  class ReceivedMessageDeduplicator
    # Bound volatile history independently of downstream queue sizes. Retry
    # copies normally arrive together, while 1,024 identities keep memory
    # finite even if a hostile sender supplies a long stream of unique text.
    CAPACITY = 1_024

    @seen = Set(String).new
    @order = Deque(String).new

    def duplicate?(payload : Bytes) : Bool
      # Return false for non-text inbox results. The first occurrence enters
      # the bounded history; later occurrences remain duplicates until evicted.
      identity = Protocol.received_text_message_identity(payload) || return false
      return true if @seen.includes?(identity)

      if @order.size >= CAPACITY
        @seen.delete(@order.shift)
      end
      @order << identity
      @seen << identity
      false
    end
  end
end
