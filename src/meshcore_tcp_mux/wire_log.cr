require "./protocol"

class MeshCoreTCPMux
  # Namespace for the TCP multiplexer: transport, protocol validation, and per-client state.
  class WireLog
    # Formats one decoded wire frame with its physical direction and complete
    # payload. Runtime supplies the endpoint identity; Protocol supplies names
    # and fields that can be decoded without decrypting application data.
    def self.upstream(epoch : Int64, direction : Symbol, payload : Bytes) : String
      # Identify the shared physical companion by its reconnect epoch.
      description = direction == :tx ? Protocol.describe_command(payload) : Protocol.describe_response(payload)
      "UPSTREAM(#{epoch}): #{direction} #{wire_name(direction == :tx, payload)} payload=#{Protocol.hex(payload)} #{description}"
    end

    def self.multi_client(session : Int64, direction : Symbol, payload : Bytes) : String
      # Multi-client identity lasts only for this downstream socket session.
      description = direction == :rx ? Protocol.describe_command(payload) : Protocol.describe_response(payload)
      "MULTI_CLIENT(#{session}): #{direction} #{wire_name(direction == :rx, payload)} " \
      "payload=#{Protocol.hex(payload)} #{description}"
    end

    def self.dedicated_client(port : Int32, direction : Symbol, payload : Bytes) : String
      # Dedicated identity is the stable configured port across replacements.
      description = direction == :rx ? Protocol.describe_command(payload) : Protocol.describe_response(payload)
      "DEDICATED_CLIENT(#{port}): #{direction} #{wire_name(direction == :rx, payload)} " \
      "payload=#{Protocol.hex(payload)} #{description}"
    end

    private def self.wire_name(command : Bool, payload : Bytes) : String
      # Select the command or response namespace for this side of the socket.
      return "EMPTY" if payload.empty?
      if command
        (Protocol.descriptor(payload).try(&.name) || :unknown).to_s.upcase
      else
        Protocol.response_name(payload[0]).upcase
      end
    end
  end
end
