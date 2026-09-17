# Limitations

MeshCore's companion protocol was designed around one client connected to one companion. `meshcore-tcp-mux` coordinates multiple clients, but it cannot remove every protocol or shared-state constraint.

---

## Outgoing messages are not mirrored

If one client sends a DM or channel message, other clients cannot see its contents as an outgoing message because the companion protocol provides no outgoing-message event. Incoming messages are copied to clients. DM delivery confirmations are broadcast, but they do not contain the original message.

Fixing this completely requires a firmware/protocol event containing an outgoing message's ID, content, and delivery state.

## Transactions can briefly delay other clients

Some operations require a two-way transaction with the companion and can delay other clients for a few seconds. These include listing contacts, querying or changing device settings, and waiting for a send acknowledgement. Delayed clients continue when the transaction completes.

Only one login or other remote request can be pending at a time. Concurrent attempts are rejected with `BAD_STATE`, and repeater login/logout state is shared. While a remote repeater login is pending, another client cannot start status, remote telemetry, binary, path-discovery, anonymous, or trace requests; ordinary local queries, incoming-message handling, and outgoing messages can continue.

## Companion identity and radio configuration are shared

Every client uses the physical companion's mesh identity, contacts, channels, and radio settings. A configuration change made by one application affects all of them, and another client may not notice the change until it reconnects.

Use one application as the configuration authority when clients have competing state-management policies. Multiple clients can still send and receive messages; they simply are not independent mesh nodes. (That's the whole point of `meshcore-tcp-mux`!)

## Inbox retention is bounded and volatile

Multi-client sessions are connection-scoped. A newly connected client does not receive messages that were distributed before it joined.

A configured dedicated port provides a stable identity and a bounded reconnect queue, but that queue exists only in RAM. It does not survive a mux restart and does not guarantee that the application processed an item after it was written to the socket. See [CONFIGURATION.md](CONFIGURATION.md) for queue behavior.

## `meshcore-tcp-mux` must be the only direct client of the companion node

All participating applications must connect through `meshcore-tcp-mux`. Connecting to the same companion directly through another TCP connection, BLE, or USB can bypass the mux's coordination and produce incorrect client state.

## The downstream network is trusted

Mux listener ports have no authentication or encryption. Anyone who can reach a port can use the shared companion and access the protocol data available to that client. Anyone who reaches a dedicated-client port can also replace its current socket and consume its retained queue. This is already true of the upstream MeshCore TCP companion protocol, so it's not an additional security concern; just the same one.

Be sure to bind listeners only to appropriate interfaces and protect them with a firewall or VPN. Do not expose them directly to the internet.
