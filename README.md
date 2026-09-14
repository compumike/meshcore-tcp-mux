# meshcore-tcp-mux

A small, simple, protocol-aware, N-to-1 TCP multiplexer for **connecting multiple TCP clients (such as meshcore-cli, meshcore-HA, bots, etc.)** to **one physical MeshCore companion**. It maintains one upstream TCP connection and accepts multiple downstream TCP connections using the existing companion wire protocol.

This lets you share one physical MeshCore companion (or, for example, an OpenHop Repeater software companion) with multiple TCP clients, and allows them all to send and receive DMs and channel messages, list contacts, etc.

The core is **a serialized command broker plus a virtual receive inbox for each downstream connection**. All clients share the physical node's identity, contacts, channels, radio settings, etc. They do **not** acquire independent mesh-visible companion identities. Changing node settings on one will change it on all (and may not be reflected properly on all clients until their connections are restarted).

It has **no database**: this is a feature. It's just TCP in, TCP out.

## ⚠️ WARNING: mostly vibe coded, beware! ⚠️

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
      - "--listen-port"
      - "5001"
    ports:
      - "127.0.0.1:5001:5001/tcp"
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
    docker compose logs --follow

    # Later, to turn it off:
    docker compose down
```

4. Connect to it:

```
    meshcore-cli -t 127.0.0.1 -p 5001
```

You should now be able to connect multiple clients to `127.0.0.1:5001` and have them all basically work simultaneously.

-----

## Limitations

In general, `meshcore-tcp-mux` will work for multiple connected clients.

There are some limitations due to the nature of the MeshCore protocol, which was designed for a single client connecting to a single companion node:

-  If one client sends a DM or channel message, the other clients will NOT see its contents as an outgoing message, because the companion protocol provides no outgoing-message event. Incoming messages are copied to all currently connected clients. DM delivery confirmations are broadcast, but they do not contain the original message.
  - This is the most visible and obvious limitation of the protocol. It can't be fixed without a firmware/protocol addition for an explicit outgoing-message event containing an ID, content, and delivery state.
- Some operations may briefly block other clients for a few seconds because they require a two-way transaction between client and companion. These include listing contacts, querying or changing device settings, and waiting for a send acknowledgement. (The other clients are simply delayed/stalled for a few seconds, and will start working again once the transaction has completed.)
  - This is generally fine, as it's only a brief delay, and only on certain operations.
- Messages are consumed from the companion and not retained for future clients, so a new client that connects won't see earlier messages that any other previously-connected clients have received. (When no clients are connected, messages remain queued on the companion.)
  - The existing protocol does not provide a way to identify specific TCP clients, so there is no safe way to store and replay messages that a particular client has not yet seen.
  - Blindly re-delivering old messages risks re-triggering bots and automations in a way which is not consistent with the underlying protocol.
- All clients share the companion's identity and configuration. A change made by one client affects every client, but the others may not notice it until they reconnect.
- While a remote repeater login is pending, another client cannot start status, remote telemetry, binary, path-discovery, anonymous, or trace requests. (Ordinary local queries, incoming message handling, and outbound DMs and channel message sends can continue.)
- Only one login or other remote request can be pending at a time. Concurrent attempts are rejected with BAD_STATE. Repeater login/logout state is shared by all clients.
- This `meshcore-tcp-mux` must be the companion node's only direct client. Connecting to the node through BLE, USB, or another TCP connection will absolutely cause problems.
- The exposed TCP port has no authentication or encryption and should not be exposed directly to the Internet.

If you're writing automations or bots, these are generally not significantly concerning limitations.