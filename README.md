# MeshCore TCP Mux: share one companion node with multiple apps

Run [MeshCore-HA](https://github.com/meshcore-dev/meshcore-ha) alongside any other MeshCore frontends at the same time, sharing a single TCP-connected companion. `meshcore-tcp-mux` is a small, protocol-aware multiplexing proxy service that shares the companion node without replacing your chat app, automation, or bot.


```text
 meshcore-cli --\
 meshcore-cli ---+--> :5001 multi-client ----\    ######################
 misc one-off --/                            |    #                    #
                                             +--> #  meshcore-tcp-mux  # --> your_companion:5000
 MeshCore-HA -------> :5002 dedicated -------|    #                    #
 desktop app -------> :5003 dedicated -------|    ######################
 phone app ---------> :5004 dedicated -------|
 custom bot --------> :5005 dedicated ------/

                  many mux listener ports                           one upstream TCP connection
```

Unlike other multiplexers, `meshcore-tcp-mux` speaks the full MeshCore companion protocol, allowing multiple clients to safely and correctly share a single companion node.

All clients share the upstream companion's identity, contacts, channels, and radio settings. All clients can send and receive messages. (However, they won't see *outgoing* messages sent by the other clients. More details under "Protocol limitations" below.)

---

## Lightweight

`meshcore-tcp-mux` is lightweight: it's distributed as a tiny (~5 MiB) static binary in a multi-arch container image for `linux/amd64` and `linux/arm64` at [compumike/meshcore-tcp-mux](https://hub.docker.com/r/compumike/meshcore-tcp-mux), and uses only a few MiB of RAM while running, making it a great fit to run on a Raspberry Pi or similar.

---

## Multi-client vs. dedicated-client ports

`meshcore-tcp-mux` will simultaneously accept connections from:

- 1 multi-client port (default port 5001) with any number of simultaneous connections
- N dedicated-client ports (for example 5002, 5003, 5004, 5005) with one simultaneous connection each

The single `--listen-multi-client-port` accepts multiple simultaneous, connection-scoped TCP clients. These are great for transient clients, such as a one-off [meshcore-cli](https://github.com/meshcore-dev/meshcore-cli) session to send a few commands.

Each `--listen-dedicated-client-port` (multiple allowed) represents a single stable logical client. This has the advantage that each dedicated client retains its own incoming message inbox while that specific client is disconnected, just like a regular companion node does! When that specific client reconnects to its designated dedicated port, it will receive the (bounded) backlog of received messages that it hasn't seen yet. Dedicated client ports are great for cases like desktop or phone apps that may disconnect for a while and then come back, or for MeshCore-HA or any other apps that may disconnect and reconnect (such as when Home Assistant restarts).

Don't share a single dedicated client port between multiple apps: each dedicated client port only allows one TCP connection, and maintains its own inbox, so sharing a dedicated client port removes the benefits of the dedicated client mode.

---

## How it works

`meshcore-tcp-mux` maintains one upstream TCP connection to the companion node, and accepts native MeshCore companion-protocol TCP connections from downstream clients.

As clients send commands, the mux accepts them round-robin and maintains protocol-specific and command-specific transactions on the upstream connection as needed, so it correctly handles commands that need a companion response. It then returns those responses to the specific client that requested them, and gives each client its own inbox of *all* incoming messages.

All participating clients must connect through the `meshcore-tcp-mux`. Do not also connect directly to the companion over TCP, BLE, or USB while the mux is in use.

---

## Quick-start: Docker Compose setup

First verify that the companion itself is reachable, for example with `meshcore-cli -t 192.168.1.50 -p 5000`. Then create `compose.yaml`:

```yaml
services:
  meshcore-tcp-mux:
    image: compumike/meshcore-tcp-mux:latest
    restart: unless-stopped
    command:
      - "--upstream-host"
      - "192.168.1.50" # Replace this with the companion hostname or IP
      - "--upstream-port"
      - "5000"
      - "--listen-host"
      - "0.0.0.0" # Must bind to 0.0.0.0 *inside* the container, even if you bind only to localhost in the "ports" section.
      - "--listen-multi-client-port"
      - "5001"
      - "--listen-dedicated-client-port"
      - "5002"
      - "--listen-dedicated-client-port"
      - "5003"
      - "--listen-dedicated-client-port"
      - "5004"
      - "--listen-dedicated-client-port"
      - "5005"
      - "--deduplicate-received-messages"
    ports:
      - "127.0.0.1:5001-5005:5001-5005/tcp" # Change "127.0.0.1" to "0.0.0.0" if you want to connect to meshcore-tcp-mux from other devices.
    environment:
      LOG_LEVEL: "INFO" # Change to "DEBUG" for more verbose logging
    read_only: true
    cap_drop: ["ALL"]
    security_opt: ["no-new-privileges:true"]
```

Replace `192.168.1.50` with the companion's address. Replace `127.0.0.1` with `0.0.0.0` if you want to connect from other devices, such as a phone app. Then start the service:

```sh
docker compose up -d
docker compose logs --follow
```

Now run `meshcore-cli -t 127.0.0.1 -p 5001` to verify that `meshcore-tcp-mux` is working.

Reconfigure MeshCore-HA to point to the dedicated port `5002`. Point your desktop app at port `5003`. Point your phone app at `5004`. **You will now be able to send and receive message simultaneously from all of them!**

Other useful commands:

```sh
# To stop the service:
docker compose down

# To upgrade to the latest version of meshcore-tcp-mux:
docker compose pull && docker compose down && docker compose up -d
```

---

## Received message deduplication

MeshCore clients are supposed to deduplicate received channel messages and DMs based on identical content and sender timestamp (but contained in radio packets with different attempt numbers due to sender retries). While phone apps tend to get this right, many clients (including `meshcore-cli` and `MeshCore-HA`) don't implement this deduplication at all or have implementation issues, so they sometimes receive duplicates of the same message.

You may optionally pass the `--deduplicate-received-messages` command line option to have `meshcore-tcp-mux` discard these duplicates before they reach clients.

Enabling `--deduplicate-received-messages` is recommended because it makes it much easier to write Home Assistant automations or bots.

---

## Protocol limitations

When one client sends a DM or channel message, **other clients cannot see its contents as an outgoing message**. This is a MeshCore companion protocol limitation: it has no outgoing-message event. All incoming messages are copied to all clients, and DM delivery confirmations are broadcast, but those confirmations do not include the original message.

See [LIMITATIONS.md](LIMITATIONS.md) for other minor protocol, persistence, shared-state, and security limitations.

---

## See also

- [ARCHITECTURE.md](ARCHITECTURE.md)
- [CONFIGURATION.md](CONFIGURATION.md)
- [DEVELOPMENT.md](DEVELOPMENT.md)

---

## Roadmap

- No other features are planned at this time.
- This project has the goal of being deliberately small, stable, and low maintenance.
- Serial and BLE support is not planned at this time.

---

## Issues and pull requests

- This is a hobby side project.
- Issues and PRs are unlikely to be accepted.
- If you find a reproducible bug, you may file a brief issue.

---

## Development note

This project is almost entirely "vibe coded", but has been tested extensively against physical companion nodes, [OpenHop Repeater](https://github.com/openhop-dev/openhop_repeater) companion nodes, and an internal test suite.