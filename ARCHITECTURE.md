# Architecture

`meshcore-tcp-mux` lets several existing MeshCore companion clients share one
physical companion over its native TCP protocol. It is a protocol-aware command
broker, not a second companion implementation:

- Client commands and firmware responses remain native binary payloads.
- One upstream connection is shared by many independent downstream sessions.
- Commands are serialized because ordinary responses such as `OK` and `ERR`
  contain no request or client identifier.
- Incoming messages are fetched once after a downstream sync request and copied
  into connection-scoped multi-client inboxes plus every configured persistent
  dedicated-client queue.
- Shared physical state—identity, contacts, channels, configuration, and radio
  capacity—is deliberately not virtualized.

The implemented compatibility profile is `native_v13`: protocol level 13 with
payloads of at most 176 bytes. Its firmware reference is MeshCore commit
`0679dbeffc504d562d2f09eb072fdc223f8ffc2a`; its client-behavior reference is
`meshcore_py` commit `1bfd8385d5031a2d8b99aa8d69a31f6853bc35f0`.

## Implementation map

- [`BinaryEntrypoint`](src/meshcore_tcp_mux/binary_entrypoint.cr) parses runtime,
  timeout, maintenance, and diagnostic-probe options. [`main.cr`](src/main.cr)
  remains only the executable wrapper.
- [`Runtime`](src/meshcore_tcp_mux/runtime.cr) owns the multi-client listener,
  every configured dedicated-client listener, socket
  lifecycles, upstream epochs, reconnect policy, and execution of broker
  actions. It is the only caller of `Broker`, which keeps protocol decisions in
  one fiber without mutexes.
- [`Transport::Endpoint`](src/meshcore_tcp_mux/transport.cr) gives each socket
  one reader and one writer fiber. Bounded writer queues keep blocking I/O and
  slow clients out of the broker.
- [`FrameCodec`](src/meshcore_tcp_mux/frame_codec.cr) incrementally decodes the
  `< length payload` client envelope and `> length payload` companion envelope.
  It rejects wrong markers, empty or oversized frames, truncated frames, and
  incomplete frames that exceed their assembly deadline.
- [`Broker`](src/meshcore_tcp_mux/broker.cr) is the sole owner of command
  scheduling, response ownership, inbox pumping, push routing, leases, output
  budgets, and failure decisions. It produces typed `Action` values and performs
  no I/O, making the state machine directly testable.
- [`Session`](src/meshcore_tcp_mux/session.cr) holds one connection's command FIFO,
  virtual inbox, output budget, application-protocol target, pending sync, and
  desired flood scope. Reconnecting always creates a new session.
- [`DedicatedClientSlot`](src/meshcore_tcp_mux/dedicated_client_slot.cr) holds
  one port-identified client's native offline queue across socket replacements
  and matching-companion upstream epochs. It is volatile across process restart.
- [`Protocol`](src/meshcore_tcp_mux/protocol.cr) contains the supported command
  descriptors, payload validators, response grammars, logging descriptions,
  V3-to-legacy inbox conversion, and startup payload builders. Centralizing this
  table prevents routing from being inferred from whichever client sent most
  recently.
- [`DmRing`, `RemoteLease`, and `SigningLease`](src/meshcore_tcp_mux/leases.cr)
  model firmware state that outlives one immediate command response. These
  reservations prevent clients from overwriting the companion's finite or
  single-owner transaction state.
- [`Startup`](src/meshcore_tcp_mux/startup.cr) establishes a synchronization
  fence, validates `native_v13`, captures the node identity, and restores the
  default flood scope before `Runtime` admits clients.
- [`Config`](src/meshcore_tcp_mux/config.cr) gathers queue bounds, deadlines,
  polling intervals, and permissions. [`Clock`](src/meshcore_tcp_mux/config.cr)
  supplies monotonic time so wall-clock changes cannot alter protocol deadlines.

## Command and response flow

- A client endpoint decodes a complete command and sends it to `Runtime`.
- `Runtime` passes the event to `Broker`, which validates and queues it on the
  corresponding `Session`.
- `Broker` schedules eligible session heads round-robin. At most one local
  upstream transaction is active, including internal inbox commands and hidden
  flood-scope setup or restoration.
- The active `Broker::Transaction` records its owner and response grammar before
  its write is exposed to the upstream writer. This handles a response arriving
  before the write-completion event.
- Ordinary responses are validated against that grammar and sent only to the
  owner. Contacts remain owned through `END_OF_CONTACTS`; a stream cannot be
  interleaved with another command.
- Asynchronous pushes follow an explicit policy: shared observations are
  broadcast, remote results go only to their lease owner, DM confirmations are
  broadcast and release matching ring capacity, and `MSG_WAITING` prompts
  downstream clients without moving inbox custody by itself.
- Epoch, session, job, and write IDs make late asynchronous completions harmless
  after a connection has been replaced.

Clients must still serialize ambiguous concurrent waits within their own TCP
connection. The native protocol cannot tell two same-session waiters which
generic `OK` belongs to which application coroutine.

## Virtual inbox

The companion's `SYNC_NEXT_MESSAGE` command removes an item from one physical
queue. Forwarding every client's sync command would divide messages between
clients, so `Broker` is the sole upstream inbox consumer.

- Admission, `MSG_WAITING`, and fallback polling emit coalesced downstream
  availability hints but never pop the physical inbox. Only a downstream sync
  against an empty local queue authorizes a drain-to-empty cycle.
- A returned message is stored as native immutable bytes and fanned out to every
  configured dedicated slot, attached or detached, plus every qualifying live
  multi-client session present when the pop completes.
- A downstream sync consumes one item from only that session's queue. The broker
  returns `NO_MORE_MESSAGES` only after a qualifying upstream empty check, so a
  stale empty observation cannot overtake an in-flight message.
- An empty-to-nonempty transition emits one coalesced downstream `MSG_WAITING`
  hint. Notifications are neither counts nor delivery acknowledgements.
- A multi-client inbox overflow disconnects only that session. A dedicated queue
  instead mirrors firmware priority: at capacity it evicts the oldest channel
  entry, or discards the new entry if no channel entry exists. Neither outcome
  can stall other recipients.
- With no sessions, the broker stops draining the companion. If the last client
  leaves during a pop, at most one unfanned item is retained and reused only
  when the next upstream epoch has the same node public key.

Multi-client sessions provide live fan-out, not history. Dedicated clients use
their configured port as stable identity and can retrieve unconsumed queue items
after reconnect. This is still not durable or exactly-once delivery: queues are
RAM-only, bounded, and an item is consumed when broker output accepts it rather
than when the application confirms receipt.

## Stateful operations

- Remote commands reserve `RemoteLease` beyond their immediate `SENT` response,
  because the later radio result otherwise has no client identity. Same-peer
  legacy replies can still be causally ambiguous; the mux does not invent tags.
- Accepted remote leases and direct-message acknowledgement-ring positions
  survive replacement of the upstream TCP socket when startup identifies the
  same companion. Their old downstream owners are removed, so late results are
  consumed or broadcast according to their native type but never attributed to
  a replacement session. If TCP fails before `SENT` or `ERR`, new radio work is
  quarantined for a finite conservative interval because execution is unknown;
  ordinary queries remain available. A changed public key clears this state.
  Protocol v13 cannot distinguish a same-key reboot from a reconnect. The mux
  therefore preserves reconnect safety and waits out retained deadlines, but a
  reboot can reset the firmware's DM-ring cursor without an observable marker;
  perfect post-reboot ring alignment is not claimable without firmware support.
- Signing reserves `SigningLease` across start, data chunks, and finish so
  another client cannot corrupt the shared signing operation.
- Plain direct messages reserve the firmware's eight acknowledgement slots in
  `DmRing`. Actual `SEND_CONFIRMED` frames are forwarded unchanged and matched
  using their native four-byte token.
- Application protocol targets and temporary flood scopes are per-session.
  `Protocol` downgrades supported V3 inbox messages for legacy sessions, while
  `Broker` wraps scoped sends in acknowledged setup and restoration commands.
- Channel `OK` and direct-message `SENT` mean firmware acceptance, not radio
  delivery. The mux never retries a radio send, changes its timestamp, creates
  an outgoing-message echo, or fabricates a confirmation.

Resource conflicts return native `ERR(BAD_STATE)` in the client's FIFO order.
They do not block unrelated local queries or inbox work.

## Failure and operational boundaries

- Malformed or slow downstream clients are closed independently.
- A malformed upstream frame, unexpected ordinary response, upstream write
  failure, or uncertain response timeout ends the entire epoch. All sessions
  disconnect and no possibly executed command is replayed. Radio reservations
  belong to the companion execution lifetime rather than the TCP epoch and are
  retained as described above.
- `Runtime` reconnects with bounded exponential backoff and repeats the startup
  fence before accepting new clients.
- Reboot uses normal scheduling and ends when the companion disconnects.
  Factory reset and private-key import require `--maintenance`, one client, and
  no outstanding radio or signing lease. Private-key export separately requires
  `--allow-private-key-export`.
- Logging uses Crystal `Log` and `LOG_LEVEL`. Protocol-aware payload logging is
  sanitized; private keys, passwords, channel and scope keys, PINs, signing
  input, and custom-variable values are never logged.

The mux must be the companion's **only command producer across TCP, BLE, and
USB**. Another producer can inject untagged responses and make ownership
unknowable. The implementation guarantees companion-interface isolation; it
does not guarantee exactly-once radio delivery, durable receipt, independent
physical configuration, or coordination between applications that all choose
to respond to the same message.

## Verification

The specs exercise framing boundaries, scheduling and ownership, inbox fan-out,
stateful leases, startup fencing, malformed traffic, timeouts, writer failures,
maintenance policy, and protocol-aware logging. Support harnesses under
[`spec/support/`](spec/support/) simulate the native companion and runtime TCP
interactions; the design rationale and complete protocol inventory remain in
[`design_docs/meshcore-tcp-multiplexer-design.md`](design_docs/meshcore-tcp-multiplexer-design.md).
