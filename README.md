# meshcore-tcp-mux

A protocol-aware, N-to-1 multiplexing proxy for *correctly* connecting **multiple TCP clients (such as meshcore-cli, meshcore-HA, bots, etc.)** to **one physical TCP-accessible MeshCore companion**.

-----

## How it works

`meshcore-tcp-mux` is a daemon which maintains one upstream TCP connection and accepts multiple downstream TCP connections using the existing companion wire protocol. This lets you share one physical MeshCore companion (or, for example, an OpenHop Repeater software companion) with multiple TCP clients, and allows them all to send and receive DMs and channel messages, list contacts, etc.

The core is **a serialized command broker which connects upstream, plus a separate receive queue for each downstream connection**:

- All clients can transmit and receive messages, and they'll all appear to the mesh as if they're coming from a single companion node.
- All clients share the physical node's identity, contacts, channels, radio settings, etc. They do **not** acquire independent mesh-visible companion identities.
- Certain operations require brief two-way transactions at the protocol level. Due to the protocol (intended for a single client only), these operations may delay other clients for a few seconds, but the other clients will automatically make progress again as soon as the transaction completes.

See limitations below. The biggest limitation is that one client can't see the contents of the DM or channel messages sent by another client.

`meshcore-tcp-mux` has **no database**. Dedicated-client history is retained only
in volatile RAM and is lost when the mux process stops.

### ⚠️ WARNING: this project is mostly "vibe coded," but with lots of test coverage, and it has been tested extensively against both simulated and real-world companion nodes. ⚠️

-----

## Why is this needed?

The default MeshCore companion firmware (and similarly the OpenHop repeater software companion) only handles one TCP connection at a time.

My goal was to run the [MeshCore-HA](https://github.com/meshcore-dev/meshcore-ha) Home Assistant integration on a companion presented by [openHop Repeater](https://github.com/openhop-dev/openhop_repeater), while still having access to use the same companion for other use, such as from mobile/desktop apps or [meshcore-cli](https://github.com/meshcore-dev/meshcore-cli). This would allow writing multiple bots independently.

I looked into several other projects but found nothing that actually worked and was bug-free:
- [MeshMonitor Virtual Node](https://meshmonitor.org/configuration/virtual-node.html)
- [coresplitter](https://github.com/ogarcia/coresplitter)
- [meshcore_multitcp](https://github.com/do6uk/meshcore_multitcp)
- [meshcore_proxy](https://github.com/rgregg/meshcore-proxy/issues/10)

TLDR: this one works.

-----

## Run it with Docker Compose

The easiest way to run `meshcore-tcp-mux` is by spinning up a tiny Docker Compose container, published for amd64 and arm64:

1. Create a directory.

2. Paste this into `compose.yaml`:

```
services:
  meshcore-tcp-mux:
    image: compumike/meshcore-tcp-mux:latest
    restart: unless-stopped
    command:
      - "--upstream-host"
      - "192.168.123.456"
      - "--upstream-port"
      - "5000"
      - "--listen-host"
      - "0.0.0.0"
      - "--listen-multi-client-port"
      - "5001"
      - "--listen-dedicated-client-port"
      - "5002"
    ports:
      - "127.0.0.1:5001:5001/tcp"
      - "127.0.0.1:5002:5002/tcp"
    read_only: true
    cap_drop: ["ALL"]
    security_opt: ["no-new-privileges:true"]
```

Replace "192.168.123.456" with the IP of your upstream companion. (Be sure that `meshcore-cli -t 192.168.123.456 -p 5000` is already working before you try `meshcore-tcp-mux`.)

Keep the "--listen-host" as "0.0.0.0" *within* the container.

The "ports" line controls what gets bound and is accessible from *outside* the container. If you want it to be accessible to other computers / phones / etc, change "127.0.0.1" to "0.0.0.0" and apply firewalls / VPNs / etc at your own risk.

3. Spin it up:

```
docker compose up -d

# To watch the logs:
docker compose logs --follow

# Later, to turn it off:
docker compose down

# To upgrade:
docker compose pull && docker compose down && docker compose up -d
```

4. Connect to it:

```
meshcore-cli -t 127.0.0.1 -p 5001
```

You should now be able to connect multiple clients to `127.0.0.1:5001` and have them all basically work simultaneously.

Port 5001 is the **multi-client** listener: any number of simultaneous,
connection-scoped clients may use it. The Compose example also enables one
**dedicated client** on port 5002. That port is one stable logical client: a new
connection replaces the old connection, while unread incoming messages remain
queued in RAM for the replacement. The binary itself does not enable a
dedicated port unless `--listen-dedicated-client-port PORT` is supplied; repeat
that option to create more dedicated clients. Each internal port is its client
identity, even when Docker maps it to a different host-side port.

Existing command lines must replace the removed `--listen-port` option with
`--listen-multi-client-port`. There is no implicit dedicated-client listener in
the binary defaults; port 5002 is enabled explicitly by the Compose example.

Dedicated queues default to 256 entries and can be changed with
`--offline-queue-size N`. When full, they mirror companion firmware priority:
the oldest channel message is sacrificed for a new arrival; if the queue has no
channel message, the new arrival is discarded. A disconnected or abandoned
dedicated client never stalls delivery to other clients. Large queues can make
initial catch-up slow because native clients pull one item per sync request.

Messages stay on the companion until at least one connected client explicitly
requests inbox synchronization. Once a client starts that drain, each result is
copied to all configured dedicated queues, including disconnected ones, and to
qualifying live multi-client sessions. Retention is not an application receipt
guarantee: an item is consumed from a dedicated queue when the mux accepts it
for socket output, so a connection failure immediately afterward can still lose
that item.

The command policy matches a direct companion connection by default: private-key
export, private-key import, and factory reset are available. Export responses go
only to the requesting connection. Import and factory reset still use the mux's
exclusive disruptive-command lifecycle, which requires one idle downstream
session and ends the upstream epoch after the real companion result.

Deployments can reject these operations independently:

```text
--reject-private-key-export
--reject-private-key-import
--reject-factory-reset
```

The former `--allow-private-key-export` and `--maintenance` options remain
accepted as compatibility no-ops because those permissions are now the default.

At the default `INFO` level, wire diagnostics identify the endpoint and
direction, for example `UPSTREAM(12): rx END_OF_CONTACTS`,
`MULTI_CLIENT(7): tx END_OF_CONTACTS`, or
`DEDICATED_CLIENT(5002): tx END_OF_CONTACTS`. Each line contains the complete
payload in hexadecimal plus fields that can be decoded without decrypting it.

-----

## Limitations

In general, `meshcore-tcp-mux` will work for multiple connected clients.

There are some limitations due to the nature of the MeshCore protocol, which was designed for a single client connecting to a single companion node:

-  If one client sends a DM or channel message, the other clients will NOT see its contents as an outgoing message, because the companion protocol provides no outgoing-message event. Incoming messages are copied to all currently connected clients. DM delivery confirmations are broadcast, but they do not contain the original message.
  - This is the most visible and obvious limitation of the protocol. It can't be fixed without a firmware/protocol addition for an explicit outgoing-message event containing an ID, content, and delivery state.
- Some operations may briefly block other clients for a few seconds because they require a two-way transaction between client and companion. These include listing contacts, querying or changing device settings, and waiting for a send acknowledgement. (The other clients are simply delayed/stalled for a few seconds, and will start working again once the transaction has completed.)
  - This is generally fine, as it's only a brief delay, and only on certain operations.
- Multi-client sessions are connection-scoped, so a newly connected client on
  port 5001 does not receive messages distributed before it joined. A configured
  dedicated port supplies stable identity and volatile reconnect history, but
  does not survive mux restart or guarantee application receipt.
- All clients share the companion's identity and configuration. A change made by one client affects every client, but the others may not notice it until they reconnect.
- While a remote repeater login is pending, another client cannot start status, remote telemetry, binary, path-discovery, anonymous, or trace requests. (Ordinary local queries, incoming message handling, and outbound DMs and channel message sends can continue.)
- Only one login or other remote request can be pending at a time. Concurrent attempts are rejected with BAD_STATE. Repeater login/logout state is shared by all clients.
- This `meshcore-tcp-mux` must be the companion node's only direct client. Connecting to the node through BLE, USB, or another TCP connection will absolutely cause problems.
- The exposed TCP port has no authentication or encryption and should not be exposed directly to the Internet.
  Anyone who can reach a dedicated-client port can replace its current socket
  and consume that client's retained queue.

If you're writing automations or bots, these are generally not significantly concerning limitations.
