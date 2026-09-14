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

## Build and run

Crystal **1.21.0** is pinned in `.tool-versions`. The daemon uses only Crystal's
standard library. With asdf and direnv configured:

```sh
direnv exec . make
direnv exec . make spec
direnv exec . out/meshcore-tcp-mux \
  --upstream-host "$MESHCORE_UPSTREAM_HOST" --upstream-port 5000
```

Set `MESHCORE_UPSTREAM_HOST` to your companion's hostname or IP address before
running the daemon.

The full local CI equivalent also runs real Python clients against an isolated
fake companion (install `meshcore==2.3.9.1` and `meshcore-cli==1.6.3` in that
Python environment):

```sh
direnv exec . make ci PYTHON=/path/to/python
```

The GitHub Actions workflow pins the compiler and client packages. The example
systemd unit in `examples/` assumes the binary has been installed at
`/usr/local/bin/meshcore-tcp-mux`. Create `/etc/meshcore-tcp-mux.env` with
`MESHCORE_UPSTREAM_HOST=your-companion-hostname` before starting the service.

The listener defaults to `127.0.0.1:5001`. Both upstream arguments are required.
Use `--help` for queue budgets, deadlines, and listener options. Binding beyond
loopback requires an explicit `--listen-host`. Do not expose companion TCP to
the Internet; use a trusted network, firewall, VPN, or authenticated tunnel.

```sh
meshcore-cli -t 127.0.0.1 -p 5001 ver
meshcore-cli -t 127.0.0.1 -p 5001 list
```

`--probe` performs startup synchronization and prints public firmware
identification, then exits without starting a listener. Stop a running daemon
before probing the physical upstream directly.

## Docker

The image name is `compumike/meshcore-tcp-mux:latest`. A published multi-platform
tag contains both `linux/amd64` and `linux/arm64`; Docker selects the native
variant automatically. ARM64 requires a 64-bit host OS (not 32-bit ARM).

Set `MESHCORE_UPSTREAM_HOST` to your companion's hostname or IP address, then,
after publishing or locally building the image:

```sh
docker run -d \
  --name meshcore-tcp-mux \
  --restart unless-stopped \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  -p 127.0.0.1:5001:5001 \
  compumike/meshcore-tcp-mux:latest \
  --upstream-host "$MESHCORE_UPSTREAM_HOST" \
  --upstream-port 5000 \
  --listen-host 0.0.0.0 \
  --listen-port 5001
```

The **container** must listen on `0.0.0.0` for port forwarding to work. The
**host** publication above remains local-only. For LAN clients, replace the
host-side `127.0.0.1` with a trusted LAN interface address and configure your
firewall. The protocol is unauthenticated; never publish it to the Internet.
Normal bridge networking suffices; no privileged mode or host networking is
required. The companion address must be reachable from inside the container;
`127.0.0.1` there means the container itself, not the Docker host.

The image runs as non-root, needs no persistent volume, and logs to stderr:

```sh
docker logs -f meshcore-tcp-mux
docker stop meshcore-tcp-mux
docker run --rm compumike/meshcore-tcp-mux:latest --help
```

CLI arguments are passed directly to the executable, including the existing
SIGTERM shutdown handling. Environment variables are not read by the executable
itself; the shell above (or Compose below) supplies their values as arguments.

### Docker Compose

The included `compose.yaml` uses the same image and security settings. Set
`MESHCORE_UPSTREAM_HOST` in your shell or a local `.env` file, then run:

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

### Building and publishing the image

Build and load the current host architecture locally:

```sh
direnv exec . make docker-build
```

On Linux Docker Engine, also verify the final image's non-root/read-only runtime,
bridge networking, two-client inbox fanout, and graceful SIGTERM shutdown:

```sh
direnv exec . make docker-smoke PYTHON=/path/to/python
```

Use a Python environment containing `meshcore==2.3.9.1`. This check creates and
removes its own temporary container and bridge network. It requires access to
the local Linux Docker daemon and its bridge gateway, so it is not intended for
Docker Desktop or a remote Docker context.

The multi-stage Dockerfile uses Crystal 1.21.0 on Alpine, with pinned
multi-platform base-image digests. It compiles a release-mode static binary for
a generic CPU, runs the Crystal specs and real Python-client fake-companion
smoke test, then copies only the executable into a small Alpine runtime. Tests
contact no physical radio. Compiler, Python, source, and test dependencies stay
out of the runtime image. An allowlisted `.dockerignore` excludes environment
files, git history, local caches, host-built binaries, and live-test reports.

Publishing is a separate, explicit operation. Authenticate to Docker Hub with
permission to push `compumike/meshcore-tcp-mux`, and select a Buildx builder
supporting both target architectures, through native nodes or QEMU emulation:

```sh
docker login
direnv exec . make docker-push DOCKER_BUILDER=YOUR_MULTIARCH_BUILDER
docker buildx imagetools inspect compumike/meshcore-tcp-mux:latest
```

`docker-push` builds and tests **both** platforms before pushing the shared
`latest` tag. `docker-build` never pushes. `DOCKER_IMAGE`, `DOCKER_PLATFORMS`, and
`DOCKER_BUILDER` are overridable Make variables. No version tags are generated.
For builder setup, see [Docker's multi-platform guide](https://docs.docker.com/build/building/multi-platform/).
Native ARM64 builders are preferable for frequent releases because emulated
Crystal compilation can be slow. When updating Crystal, update `.tool-versions`
and the Dockerfile's compiler tag/index digest together.

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
pending radio/signing leases, and ends the epoch after the operation. Private-key export has a
separate `--allow-private-key-export` flag. Default logs contain metadata,
queue counts, and public firmware identification, not payloads or secrets.

## Implementation and verification

`src/meshcore_tcp_mux/broker.cr` owns scheduling and protocol state. Readers and
writers exchange typed events with `Runtime`; each socket has one writer.
`Protocol` holds the command descriptors and field validators. `Startup`
implements the native transport's five-self-info synchronization fence before
client admission. Tests use injected monotonic time, a retained-output native
transport model, and real loopback sockets.

The `native_v13` profile is based on MeshCore firmware commit
`0679dbeffc504d562d2f09eb072fdc223f8ffc2a`, with 176-byte payloads. Its assumptions
are not a compatibility claim for serial bridges, forks, or other virtual
nodes. The design's Python reference is `meshcore_py` commit
`1bfd8385d5031a2d8b99aa8d69a31f6853bc35f0`.

The scripts in `scripts/` provide repeatable real-client checks. Run them with
the Python environment containing `meshcore` (for example the `meshcore-cli`
pipx virtual environment). `check_clients.py` is read-only;
`check_fake_clients.py` starts an isolated fake companion and proxy.
`watch_inbox.py` compares incoming DM/channel payloads from two live sessions
and prints only hashes. See `test-results.md` for recorded results and deferred
integration checks.

`check_sends.py` requires explicit `--contact NAME` and `--channel '#CHANNEL'`
destinations and resolves each uniquely. It is read-only unless explicitly given
`--execute`, and then sends
one of each with no retries. `check_live_reconnect.py` takes exclusive ownership
of the physical upstream through a temporary local relay, drops that TCP
connection once, and verifies session closure and fresh initialization. Stop
every other upstream producer before running that check.
