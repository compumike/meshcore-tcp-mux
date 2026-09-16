require "deque"

class MeshCoreTCPMux
  # Namespace for the TCP multiplexer: transport, protocol validation, and per-client state.
  class DedicatedClientSlot
    # Represents one port-identified client across socket replacements. Its
    # firmware-style offline queue survives matching-companion broker epochs but
    # remains deliberately volatile across mux process restart.
    enum EnqueueResult
      Added
      ChannelEvicted
      NewMessageDiscarded
    end

    getter dedicated_slot_id : Int32
    getter listen_port : Int32
    getter offline_queue = Deque(Bytes).new
    property attached_session_id : Int64? = nil
    getter enqueued = 0_u64
    getter delivered = 0_u64
    getter channel_evictions = 0_u64
    getter new_message_discards = 0_u64
    getter high_water = 0

    def initialize(@dedicated_slot_id : Int32, @listen_port : Int32) : Nil
    end

    def enqueue_offline(payload : Bytes, limit : Int32) : EnqueueResult
      # Mirror companion overflow priority: at capacity, sacrifice the oldest
      # channel-class frame. If only direct/contact frames remain, keep them and
      # discard the new arrival without blocking any other recipient.
      result = EnqueueResult::Added
      if @offline_queue.size >= limit
        channel_index = @offline_queue.index { |item| channel_message?(item) }
        unless channel_index
          @new_message_discards += 1
          return EnqueueResult::NewMessageDiscarded
        end
        @offline_queue.delete_at(channel_index)
        @channel_evictions += 1
        result = EnqueueResult::ChannelEvicted
      end

      @offline_queue << payload
      @enqueued += 1
      @high_water = {@high_water, @offline_queue.size}.max
      result
    end

    def record_delivery : Nil
      # Count broker output acceptance, which is the documented v1 delivery
      # boundary; socket or application receipt cannot be proven by this protocol.
      @delivered += 1
    end

    def clear : Int32
      # Return the discarded count for identity-change and shutdown diagnostics.
      count = @offline_queue.size
      @offline_queue.clear
      @attached_session_id = nil
      count
    end

    private def channel_message?(payload : Bytes) : Bool
      # CHANNEL_MSG_RECV (0x08), CHANNEL_MSG_RECV_V3 (0x11), and
      # CHANNEL_DATA_RECV (0x1b) are the firmware's channel-class queue entries.
      {0x08_u8, 0x11_u8, 0x1b_u8}.includes?(payload[0]?)
    end
  end
end
