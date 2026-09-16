require "./spec_helper"
require "../src/meshcore_tcp_mux/wire_log"

describe MeshCoreTCPMux::WireLog do
  # Wire diagnostics must name the transport endpoint and preserve the exact
  # payload. These examples also fence the direction reversal at downstream
  # sockets: a client command is received by the mux, while a response is sent.

  it "formats upstream commands and responses with epoch and raw payload" do
    MeshCoreTCPMux::WireLog.upstream(63_i64, :tx, Bytes[4_u8]).should eq(
      "UPSTREAM(63): tx GET_CONTACTS payload=04 command=get_contacts opcode=0x04 command_bytes=1"
    )
    # END_OF_CONTACTS carries a four-byte little-endian last-modified value.
    terminator = Bytes[4_u8, 0_u8, 0_u8, 0_u8, 0_u8]
    MeshCoreTCPMux::WireLog.upstream(63_i64, :rx, terminator).should eq(
      "UPSTREAM(63): rx END_OF_CONTACTS payload=0400000000 response=end_of_contacts code=0x04 response_bytes=5"
    )
  end

  it "distinguishes multi-client sessions from stable dedicated ports" do
    # END_OF_CONTACTS with a zero u32 last-modified timestamp.
    response = Bytes[4_u8, 0_u8, 0_u8, 0_u8, 0_u8]
    MeshCoreTCPMux::WireLog.multi_client(17_i64, :tx, response).should contain(
      "MULTI_CLIENT(17): tx END_OF_CONTACTS payload=0400000000"
    )
    MeshCoreTCPMux::WireLog.dedicated_client(5047, :tx, response).should contain(
      "DEDICATED_CLIENT(5047): tx END_OF_CONTACTS payload=0400000000"
    )
  end
end
