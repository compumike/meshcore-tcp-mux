# meshcore-tcp-mux

A small, simple, protocol-aware, N-to-1 TCP multiplexer for **connecting multiple TCP clients (such as meshcore-cli, meshcore-HA, bots, etc.)** to **one physical MeshCore companion**. It maintains one upstream TCP connection and accepts multiple downstream TCP connections using the existing companion wire protocol.

This lets you share one physical MeshCore companion (or, for example, an OpenHop Repeater software companion) with multiple TCP clients, and allows them all to send and receive DMs and channel messages, list contacts, etc.

The core is **a serialized command broker plus a virtual receive inbox for each downstream connection**. All clients share the physical node's identity, contacts, channels, radio settings, etc. They do **not** acquire independent mesh-visible companion identities. Changing node settings on one will change it on all (and may not be reflected properly on all clients until their connections are restarted).

It has **no database**: this is a feature. It's just TCP in, TCP out.

## ⚠️ WARNING: mostly vibe coded, beware! ⚠️

## Why is this needed?

- The default MeshCore companion firmware (and similarly the OpenHop repeater software companion) only handles one TCP connection at a time.
- [MeshMonitor Virtual Node](https://meshmonitor.org/configuration/virtual-node.html) has bugs.
- [coresplitter](https://github.com/ogarcia/coresplitter) has bugs.
- [meshcore_multitcp](https://github.com/do6uk/meshcore_multitcp) has bugs.

## Docker Compose

The included `compose.yaml` uses the published
`compumike/meshcore-tcp-mux:latest` image with non-root/read-only security
settings. Set `MESHCORE_UPSTREAM_HOST` in your shell or a local `.env` file,
then run:

```sh
docker compose up -d
docker compose logs -f
docker compose down
```

Optional variables are `MESHCORE_UPSTREAM_PORT` (5000), `MESHCORE_BIND_HOST`
(127.0.0.1), and `MESHCORE_LISTEN_PORT` (5001, host-side). Keep local environment
files uncommitted. Compose does not build or publish an image automatically.
Run only one mux per physical companion, including during updates: stop a
native/systemd instance before starting the container. Do not scale replicas
or probe the physical upstream from a healthcheck, as competing connections
can displace the mux. Restarting loses the in-memory client inboxes.
