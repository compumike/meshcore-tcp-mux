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

Logging uses Crystal's standard `Log` configuration. `LOG_LEVEL` defaults to
`INFO`. Every wire frame includes its complete hexadecimal payload together
with fields the mux can decode without decrypting it. Logs go to stderr so
`--probe` keeps stdout machine-readable.

```sh
LOG_LEVEL=DEBUG direnv exec . out/meshcore-tcp-mux \
  --upstream-host "$MESHCORE_UPSTREAM_HOST" --upstream-port 5000
```

The full local CI equivalent also runs real Python clients against an isolated
fake companion (install `meshcore==2.3.9.1` and `meshcore-cli==1.6.3` in that
Python environment):

```sh
direnv exec . make ci PYTHON=/path/to/python
```

To create a project-local Python environment with asdf, install the asdf Python
plugin once if it is not already available, install and select Python 3.12.11,
then create a virtual environment and install the pinned test clients:

```sh
asdf plugin add python
asdf install python 3.12.11
asdf set python 3.12.11

direnv exec . python -m venv .venv
direnv exec . .venv/bin/python -m pip install --upgrade pip
direnv exec . .venv/bin/python -m pip install \
  'meshcore==2.3.9.1' \
  'meshcore-cli==1.6.3'
```

There is no need to activate the virtual environment. Pass its interpreter to
Make explicitly so the smoke tests use the packages installed above:

```sh
direnv exec . make ci PYTHON="$PWD/.venv/bin/python"
```

The local `make ci` target checks formatting, builds, runs specs, and exercises
the fake companion with the pinned client packages. The example systemd unit in
`examples/` assumes the binary has been installed at
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

## Sensitive-command policy

The default policy preserves the operations available through a direct
companion connection. Private-key export is returned only to its requester;
private-key import and factory reset are admitted only through the exclusive
disruptive-command lifecycle. To deny any of these commands at the mux, add its
independent reject option:

```sh
out/meshcore-tcp-mux \
  --upstream-host "$MESHCORE_UPSTREAM_HOST" --upstream-port 5000 \
  --reject-private-key-export \
  --reject-private-key-import \
  --reject-factory-reset
```

Existing invocations containing `--allow-private-key-export` or `--maintenance`
remain valid. Both switches are compatibility no-ops because they request the
new default policy. A reject option remains effective regardless of where a
legacy allow option appears on the command line.

## Docker

The image name is `compumike/meshcore-tcp-mux:latest`. A published multi-platform
tag contains both `linux/amd64` and `linux/arm64`; Docker selects the native
variant automatically. ARM64 requires a 64-bit host OS (not 32-bit ARM).

Set `MESHCORE_UPSTREAM_HOST` to your companion's hostname or IP address, then,
after publishing or locally building the image:

```sh
docker run -d \
  --name meshcore-tcp-mux \
  -e LOG_LEVEL=DEBUG \
  --restart unless-stopped \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  -p 127.0.0.1:5001:5001 \
  -p 127.0.0.1:5002:5002 \
  compumike/meshcore-tcp-mux:latest \
  --upstream-host "$MESHCORE_UPSTREAM_HOST" \
  --upstream-port 5000 \
  --listen-host 0.0.0.0 \
  --listen-multi-client-port 5001 \
  --listen-dedicated-client-port 5002
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
SIGTERM shutdown handling. Apart from Crystal's standard `LOG_LEVEL`, runtime
settings are supplied as arguments; the shell above (or Compose in `README.md`)
expands deployment environment variables into those arguments.

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

This builds an image with the `dev` tag.

Publishing is a separate, explicit operation. Authenticate to Docker Hub with
permission to push `compumike/meshcore-tcp-mux`, and select a Buildx builder
supporting both target architectures, through native nodes or QEMU emulation:

```sh
docker login
direnv exec . make docker-push DOCKER_BUILDER=YOUR_MULTIARCH_BUILDER
docker buildx imagetools inspect compumike/meshcore-tcp-mux:dev
```

`docker-push` builds and tests **both** platforms before pushing the shared
`dev` tag. `docker-build` never pushes. `DOCKER_IMAGE`, `DOCKER_PLATFORMS`, and
`DOCKER_BUILDER` are overridable Make variables. Version tags are selected
explicitly as described below.
For builder setup, see [Docker's multi-platform guide](https://docs.docker.com/build/building/multi-platform/).
Native ARM64 builders are preferable for frequent releases because emulated
Crystal compilation can be slow. When updating Crystal, update `.tool-versions`
and the Dockerfile's compiler tag/index digest together.

### Versioned releases

The examples here release version `1.0.0`. `meshcore-tcp-mux --version` prints
the version without connecting to a companion or requiring upstream arguments.

When preparing a new release, update `shard.yml` and
`src/meshcore_tcp_mux/version.cr` together, run the local checks, and commit the
reviewed release changes before creating the source tag.

Tag and push the reviewed source commit to your configured Git remote:

```sh
git tag -a v1.0.0 -m 'meshcore-tcp-mux 1.0.0'
git push --atomic origin HEAD v1.0.0
```

Build, test, and publish the matching multi-platform image to the `dev` tag:

```
docker_build_smoke_push_dev
```

After verifying the image, point `latest` and the versioned tag at the same
multi-platform image without rebuilding it:

```sh
docker buildx imagetools create \
  --tag compumike/meshcore-tcp-mux:latest \
  --tag compumike/meshcore-tcp-mux:v1.0.0 \
  compumike/meshcore-tcp-mux:dev
```

One-line build/smoke/push to `dev` tag:

```sh
direnv exec . make docker-build && direnv exec . make docker-smoke PYTHON="$PWD/.venv/bin/python" && direnv exec . make docker-push DOCKER_BUILDER=meshcore-multiarch

# Also aliased as
bin/docker_build_smoke_push_dev
```

Do not move an already published version tag to different source or image
contents. Use a new version for corrections; deployments can pin the version
tag or the inspected digest for reproducibility. These commands publish source
and images only when explicitly run; normal builds do not publish anything.
The runtime image includes the project's MIT license at
`/usr/share/licenses/meshcore-tcp-mux/LICENSE`.

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

The fake-client smoke runs concurrent real `meshcore_py` contacts requests with
both an immediate response stream and a delayed stream. This intentionally
keeps the fast local-TCP case in coverage instead of relying on a timing delay
to let the client install response listeners.

`check_live_dedicated.py` is a bounded real-radio check for one multi-client
listener, one or more dedicated-client listeners, and a separate hardware test
companion. It fences both destinations by exact name and full live public key.
Without `--execute` it performs only identity, contact, and concurrent-query
checks. With `--execute` it sends attempt-zero DMs without retries, verifies ACK
fan-out, disconnects and reconnects both dedicated clients, checks FIFO
backfill, replaces one dedicated connection, and optionally sends one message
to an explicitly selected private channel. It prints marker hashes rather than
message bodies and stops after a configurable radio-send budget. For channel
traffic, the harness accounts for the companion firmware's on-air
`<unverified sender name>: <application body>` wrapper while still requiring
the synthetic application body to match exactly.

Use deployment-specific values rather than adding real addresses or contact
names to the repository:

```sh
direnv exec . .venv/bin/python scripts/check_live_dedicated.py \
  --mux-host "$MUX_HOST" \
  --multi-port "$MESHCORE_MULTI_CLIENT_PORT" \
  --dedicated-port "$MESHCORE_DEDICATED_CLIENT_PORT_1" \
  --dedicated-port "$MESHCORE_DEDICATED_CLIENT_PORT_2" \
  --peer-host "$MESHCORE_TEST_PEER_HOST" \
  --peer-port "$MESHCORE_TEST_PEER_PORT" \
  --mux-contact "$MESHCORE_MUX_CONTACT" \
  --peer-contact "$MESHCORE_TEST_PEER_CONTACT" \
  --channel "$MESHCORE_PRIVATE_TEST_CHANNEL"
```

Review the read-only preflight first, then repeat the same command with
`--execute`. Inbox synchronization is destructive, and an unexpected queued
message is consumed before the harness can identify it as unrelated. Reserve
both companions for the run and use `--max-unmatched 0` when a completely clean
test inbox is required. Channel overflow remains fake-companion-only because
exercising it on RF would add unnecessary shared-channel traffic.
