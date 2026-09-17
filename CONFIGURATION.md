# Configuration

The Docker Compose example in [README.md](README.md) is the quickest setup. This page describes the settings that are useful when adapting it to a real deployment.

## Endpoints and network access

There are two different endpoints:

- The **upstream endpoint** is the physical companion, such as `192.168.1.50:5000`. Only the mux connects to it.
- The **downstream endpoints** are the mux listener ports. MeshCore-HA and all other participating clients connect to these ports instead of the companion.

Inside a container, keep `--listen-host 0.0.0.0`; Docker's `ports` entries control which host interfaces can reach the listeners. A binding such as `127.0.0.1:5001:5001/tcp` is available only on the Docker host. Replace the first address with an appropriate LAN address or `0.0.0.0` only when remote clients need access, and restrict access with a firewall or VPN.

The listener has no authentication or encryption. Do not expose it directly to the Internet.

## Multi-client and dedicated ports

`--listen-multi-client-port PORT` configures the shared listener and defaults to `5001`. It accepts multiple simultaneous connections, subject to normal host resource limits. Each connection is a temporary client identity and does not receive messages distributed before it connected.

`--listen-dedicated-client-port PORT` adds one stable logical client. Repeat the option to create more dedicated clients:

```yaml
command:
  - "--upstream-host"
  - "192.168.1.50"
  - "--upstream-port"
  - "5000"
  - "--listen-host"
  - "0.0.0.0"
  - "--listen-multi-client-port"
  - "5001"
  - "--listen-dedicated-client-port"
  - "5002"
  - "--listen-dedicated-client-port"
  - "5003"
ports:
  - 127.0.0.1:5001-5003:5001-5003/tcp
```

For dedicated ports: only one socket can use a given dedicated port at a time; a new connection replaces the old one (matching the MeshCore firmware TCP behavior). Each internal listener port number is the dedicated client's identity, even if Docker maps it to a different host-side port. The binary does not create a dedicated port unless one is explicitly configured with `--listen-dedicated-client-port`.

## Dedicated inbox queues

Dedicated-client queues contain incoming messages only, are held in RAM, and are lost when the `meshcore-tcp-mux` process stops. `--offline-queue-size N` sets the capacity of every dedicated queue and defaults to 256 entries.

When a queue is full, the mux follows companion-firmware priority: it removes the oldest channel message to make room for a new arrival; if there is no channel message to remove, it discards the new arrival. A disconnected or abandoned dedicated client does not stall other clients. Large queue sizes may make initial catch-up slow because native clients pull one item per synchronization request.

Messages remain on the companion until at least one connected client requests inbox synchronization. Once draining begins, each result is copied to all configured dedicated queues, including disconnected ones, and to qualifying live multi-client sessions. This is not an application-receipt guarantee: an item leaves a dedicated queue when the mux accepts it for socket output, so a connection failure immediately afterward can still lose it.

## Optional received-message deduplication

`--deduplicate-received-messages` discards retry copies of received text DMs and channel messages before they enter downstream queues. Its bounded in-memory history is shared across downstream clients and survives an upstream reconnect to the same companion identity. Discarded copies are logged at `DEBUG` level.

This is disabled by default because the MeshCore firmware doesn't include the feature, but we recommend enabling it for most use cases.

## Sensitive-command policy

By default, the mux matches a direct companion connection: private-key export, private-key import, and factory reset are available. Export responses are sent only to the requesting connection. Import and factory reset work only when there is a single downstream client.

Deployments can reject the operations independently:

```text
--reject-private-key-export
--reject-private-key-import
--reject-factory-reset
```

## Timing and logging

Advanced timing options are normally best left at their defaults:

```text
--response-timeout SECONDS   Upstream response/contacts idle deadline (20)
--contacts-timeout SECONDS   Total contacts transaction deadline (30)
--poll-interval SECONDS      Inbox fallback polling interval (5)
```

Set the standard `LOG_LEVEL` environment variable to adjust diagnostics. At `LOG_LEVEL=debug`, wire log entries identify the endpoint and direction and include the complete payload in hexadecimal.

## Container operations

```sh
# Start
docker compose up -d

# Follow logs
docker compose logs --follow

# Stop
docker compose down

# Upgrade the image and recreate the service
docker compose pull && docker compose down && docker compose up -d
```
