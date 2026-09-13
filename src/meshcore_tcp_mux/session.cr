require "deque"

class MeshCoreTCPMux
  record Command, payload : Bytes, queued_at : Time::Span
  record PendingSync, minimum_pop : Int64, deadline : Time::Span
  record WriteBudget, bytes : Int32, deadline : Time::Span

  class Session
    getter id : Int64
    getter commands = Deque(Command).new
    getter inbox = Deque(Bytes).new
    getter writes = Hash(Int64, WriteBudget).new
    property inbox_bytes = 0
    property output_bytes = 0
    property target_version = 0_u8
    property scope : Bytes = Bytes[0x36, 0]
    property sync : PendingSync? = nil

    def initialize(@id : Int64)
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
