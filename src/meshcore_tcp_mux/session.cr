require "deque"

class MeshCoreTCPMux
  # Namespace for the TCP multiplexer: transport, protocol validation, and per-client state.
  record Command, payload : Bytes, queued_at : Time::Span
  record PendingSync, minimum_pop : Int64, deadline : Time::Span
  record WriteBudget, deadline : Time::Span

  class Session
    # Holds one downstream client's command FIFO, independent inbox, requested protocol version,
    # temporary flood scope, and outstanding write budgets. Broker owns and mutates this state.
    getter id : Int64
    getter dedicated_slot_id : Int32?
    getter commands = Deque(Command).new
    getter inbox = Deque(Bytes).new
    getter writes = Hash(Int64, WriteBudget).new
    property target_version = 0_u8
    property scope : Bytes = Bytes[0x36, 0] # SET_FLOOD_SCOPE_KEY: mode 0 without a key selects default scope.
    property sync : PendingSync? = nil
    property availability_hint_write_id : Int64? = nil

    def initialize(@id : Int64, @dedicated_slot_id : Int32? = nil) : Nil
    end

    def dedicated? : Bool
      # Dedicated sessions consume their stable slot queue; multi-client
      # sessions retain the connection-scoped inbox above.
      !@dedicated_slot_id.nil?
    end
  end

  # Side effects are explicit values. Tests inspect them; the runtime executes
  # them after each broker event, never while changing protocol ownership.
  record SendFrame, session : Int64, epoch : Int64, write_id : Int64, payload : Bytes
  record CloseSession, session : Int64, reason : String
  record EndEpoch, epoch : Int64, reason : String
  record Diagnostic, message : String, category : Symbol = :normal
  alias Action = SendFrame | CloseSession | EndEpoch | Diagnostic
end
