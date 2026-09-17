# MeshCore TCP Mux — one companion, multiple apps

Run [MeshCore-HA](https://github.com/meshcore-dev/meshcore-ha) and compatible MeshCore frontends at the same time, using a single TCP-connected companion. `meshcore-tcp-mux` is a small, protocol-aware service that shares the companion connection without replacing your chat app, automation, or bot.

All clients share the physical companion's identity, contacts, channels, and radio settings.

```text
 meshcore-cli --\
 desktop app ----+--> :5001 multi-client ----\
 phone app -----/                            |
                                             +--> [meshcore-tcp-mux] --> :5000 [companion]
 MeshCore-HA ------> :5002 dedicated --------|
 bot --------------> :5003 dedicated -------/

                    mux listener ports          one upstream TCP connection
```

The multi-client port accepts multiple simultaneous, connection-scoped clients. Each dedicated port represents one stable logical client and retains a bounded incoming-message queue in RAM while that client reconnects. Configure separate dedicated ports as needed, subject to normal host resource limits.

## How it works

The mux maintains one upstream TCP connection to the companion and accepts native MeshCore companion-protocol connections from downstream applications. It serializes commands that need a response, returns those responses to the client that requested them, and gives each client its own view of incoming messages.

All participating clients must connect through the mux. Do not also connect directly to the companion over TCP, BLE, or USB while the mux is in use.

## Docker Compose setup

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
    ports:
      - "127.0.0.1:5001-5004:5001-5004/tcp" # Change "127.0.0.1" to "0.0.0.0" if you want to connect to meshcore-tcp-mux from other devices.
    environment:
      LOG_LEVEL: "INFO" # Change to "DEBUG" for more verbose logging
    read_only: true
    cap_drop: ["ALL"]
    security_opt: ["no-new-privileges:true"]
```

Replace `192.168.1.50` with the companion's address. Then start the service:

```sh
docker compose up -d
docker compose logs --follow
```

Point temporary or interactive frontends at port `5001`. Point MeshCore-HA at the dedicated port `5002` so its logical inbox survives a TCP reconnect while the mux remains running. The example publishes both ports only on the Docker host; to accept trusted-LAN clients, change the host-side `127.0.0.1` bindings to an appropriate LAN address and protect access with your firewall or VPN.

See [CONFIGURATION.md](CONFIGURATION.md) for additional dedicated ports, queue behavior, security policy, deduplication, logging, upgrades, and other options.

## Limitations

When one client sends a DM or channel message, other clients cannot see its contents as an outgoing message. The companion protocol has no outgoing-message event containing that content. Incoming messages are copied to clients, and DM delivery confirmations are broadcast, but those confirmations do not include the original message.

See [LIMITATIONS.md](LIMITATIONS.md) for the remaining protocol, persistence, shared-state, and security limitations.
