# Architecture

## Shared-device contract

The multiplexer must be the **only command producer**, including TCP, BLE, and
USB. Native TCP replaces an existing client, and the firmware shares untagged
responses across interfaces. A direct competing connection breaks ownership.

All clients share identity, contacts, channels, configuration, and radio
capacity. Clients must serialize ambiguous command waits on each connection;
the wire protocol cannot distinguish two concurrent generic `OK` waiters in
one client. There is no contact cache, automatic clock setting, radio retry,
synthetic send acknowledgement, or outgoing-message echo.

The broker fetches each physical inbox item once and copies it to all sessions
present when that pop completes. Duplicate native items remain duplicates.
A new connection has a new inbox; this is not durable history. When no clients
are connected, backlog remains on the device. At most one pop already in flight
can become a retained orphan item. Firmware queue overflow and a lost pop reply
cannot be recovered by the proxy.

Each session has bounded command, inbox, and output queues. A slow or malformed
client is disconnected independently. There is deliberately no configured
client-count cap. An uncertain upstream timeout, malformed response, or write
failure closes every session in that epoch. Old commands are **never replayed**.
Clients reconnect, initialize again, and decide what an uncertain operation
means for their application.

## Stateful operations and limitations

Remote operations retain one lease beyond their immediate `SENT` response.
Signing retains one owner across chunks. Plain DMs protect the firmware's
eight-slot acknowledgement ring. A conflict returns native `ERR(BAD_STATE)`;
it does not fabricate a later radio failure or retry. Real DM confirmations
are broadcast unchanged and must be matched by their native four-byte token.
Channel `OK` means acceptance, not radio delivery.

Each session has a virtual application protocol target and temporary flood
scope. The upstream target stays at 13. Legacy clients receive the documented
V3-to-legacy text downgrade, preserving the remaining bytes. A scoped send is
wrapped in acknowledged physical setup and restoration without changing the
original send. Autonomous firmware traffic can still observe a temporary
scope while it is set; full isolation needs firmware support.

Some remote replies expose only a peer prefix. An old same-peer radio reply
may be indistinguishable from a newer one even to firmware. Leases prevent
concurrent overwrite, but do not invent causal identifiers or guarantee
exactly-once radio delivery.

Reboot is always allowed through normal command scheduling, even with multiple
clients or pending radio/signing leases; it disconnects all clients when the
companion restarts. Factory reset and private-key import are disabled by default.
Explicit `--maintenance` allows those two operations only with one client and no
pending radio/signing leases, and ends the epoch after the operation. Private-key
export has a separate `--allow-private-key-export` flag. Logs use Crystal's
standard `Log` facility and `LOG_LEVEL`; info records connection, command,
response, push, routing, and lease lifecycles, while debug adds protocol-aware
sanitized payloads. Private keys, passwords, channel and scope keys, device PINs,
signing input, and custom-variable values never enter either form.

## Implementation

`src/meshcore_tcp_mux/broker.cr` owns scheduling and protocol state. Readers and
writers exchange typed events with `Runtime`; each socket has one writer.
`Protocol` holds the command descriptors and field validators. `Startup`
implements the native transport's five-self-info synchronization fence before
client admission.

The `native_v13` profile is based on MeshCore firmware commit
`0679dbeffc504d562d2f09eb072fdc223f8ffc2a`, with 176-byte payloads. Its assumptions
are not a compatibility claim for serial bridges, forks, or other virtual
nodes. The design's Python reference is `meshcore_py` commit
`1bfd8385d5031a2d8b99aa8d69a31f6853bc35f0`.
