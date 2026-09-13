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
