# Development

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
Use `--help` for operational deadlines and listener options. Binding beyond
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
itself; the shell above (or Compose in `README.md`) supplies their values as
arguments.

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

## Verification

Tests use injected monotonic time, a retained-output native transport model, and
real loopback sockets.

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
