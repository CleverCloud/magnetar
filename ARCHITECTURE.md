# Magnetar — Architecture

> **Audience.** This document is for engineers evaluating, contributing to,
> or porting code into magnetar. It explains *how* the workspace is wired
> and *why*. See [README.md](README.md) for the user-facing surface and the
> Java parity matrix.

---

## Table of contents

1. [Layering](#layering)
2. [Sans-io design](#sans-io-design)
3. [The no-channels rationale](#the-no-channels-rationale)
4. [Concurrency primitives we *do* use](#concurrency-primitives-we-do-use)
5. [The driver loop](#the-driver-loop)
6. [Protocol state machine (`magnetar-proto`)](#protocol-state-machine-magnetar-proto)
7. [Wire framing](#wire-framing)
8. [Producer paths — batching vs chunking](#producer-paths--batching-vs-chunking)
9. [Consumer paths — ack grouping, unacked tracker, nack tracker, DLQ](#consumer-paths--ack-grouping-unacked-tracker-nack-tracker-dlq)
10. [Multi-topics fan-in](#multi-topics-fan-in)
11. [Pattern consumer + topic watcher (PIP-145)](#pattern-consumer--topic-watcher-pip-145)
12. [Runtime engines](#runtime-engines)
13. [TLS sites](#tls-sites)
14. [Schemas](#schemas)
15. [PIP coverage map](#pip-coverage-map)
16. [Tests](#tests)
17. [Build & validation](#build--validation)
18. [Further reading](#further-reading)

---

## Layering

Magnetar is organised in four layers. Lower layers know nothing about higher
ones — `magnetar-proto` is pure-Rust state machines with **zero I/O
dependencies**, and the high-level façade is a thin re-export plus
ergonomics layer.

```text
+--------------------------------------------------------------------------+
|                                user code                                   |
+--------------------------------------------------------------------------+
                                    |
                                    v
+--------------------------------------------------------------------------+
| magnetar (façade)                | magnetar-cli      | magnetar-admin     |
| ----------------------------     | --------------    | -----------------  |
| PulsarClient, builders,          | clap-driven       | reqwest + rustls   |
| typed schemas wiring,            | produce / consume | REST admin client. |
| partitioned / multi-topics /     | / inspect /       |                    |
| pattern / table-view types,      | admin lookups.    |                    |
| interceptor SPIs,                |                   |                    |
| message routers + hashers.       |                   |                    |
+--------------------------------------------------------------------------+
                                    |
                                    v
+--------------------------------------------------------------------------+
| magnetar-runtime-tokio    |       magnetar-runtime-moonpool              |
| --------------------      |       --------------------------             |
| Public default.           |       Deterministic-simulation engine.       |
| tokio + tokio-rustls.     |       moonpool-core `Providers` (Network,    |
| One driver task per       |       Time, Task, Random, Storage).          |
| Connection.               |       Custom rustls-over-bytepipe adapter.   |
|                           |       Same driver loop + supervisor as the   |
|                           |       tokio engine.                          |
+--------------------------------------------------------------------------+
                                    |
                                    v
+--------------------------------------------------------------------------+
| magnetar-proto (sans-io core — NO I/O deps, NO channels, NO async)       |
| ------------------------------------------------------------             |
| `Connection` state machine — `quinn-proto` shape:                        |
|   handle_bytes(now, &[u8])  -> ...                                       |
|   poll_transmit(&mut Vec<u8>) -> usize                                   |
|   poll_event() -> Option<ConnectionEvent>                                |
|   poll_timeout() -> Option<Instant>                                      |
|   handle_timeout(now)                                                    |
|                                                                          |
| Handle-based façade (no raw `BaseCommand`):                              |
|   create_producer(req), subscribe(req)                                   |
|   send(handle, msg), ack(handle, ack), seek(handle, target), close_*(h)  |
|   watch_topic_list(namespace, pattern), partitioned_metadata_request(t)  |
|                                                                          |
| Internal state: pending_ops (Slab<Waker>), per-producer + per-consumer   |
| state, trackers (ack grouping, nack, unacked), schemas, batch container, |
| chunk reassembly, topic-list watcher registry, transaction client.       |
+--------------------------------------------------------------------------+
                                    |
                                    v
                              wire (TCP/TLS)
```

### Crate-level dependency directions

```text
magnetar-cli ──> magnetar-admin
            └──> magnetar (faç.) ──> magnetar-runtime-tokio ───┐
                                ├──> magnetar-runtime-moonpool ┤
                                ├──> magnetar-auth-{oauth2,sasl,athenz}
                                └──> magnetar-messagecrypto ───┤
                                                               v
                                                       magnetar-proto
```

`magnetar-proto` is the only mandatory dependency for every other crate.
`magnetar-auth-*` and `magnetar-messagecrypto` provide trait
implementations for traits owned by `magnetar-proto` and the runtime
engines. The auth + messagecrypto crates are gated by feature flags on
`magnetar` (see [README.md §Installation](README.md#installation)).

---

## Sans-io design

### What "sans-io" means here

`magnetar-proto::Connection` is a synchronous state machine. It has **no
sockets, no `tokio`, no `async`, no threads**, and **never reads its own
clock**. The whole crate's [`Cargo.toml`] forbids I/O-bound dependencies
— the rule is enforced by a `cargo xtask check-no-io-deps` step that
walks `cargo tree -p magnetar-proto -e features` and trips on `tokio`,
`mio`, `socket2`, … ([GUIDELINES.md §I/O isolation](GUIDELINES.md#io-isolation)).

### Clock injection

The state machine takes the monotonic clock as a parameter at every
user-driven entry, and reads the wall clock through an injected provider.
Engines snapshot the host clocks at the call site (or, in moonpool
simulation, the virtual clock); the protocol layer never calls
`Instant::now()` or `SystemTime::now()` itself.

| Entry | Clock parameter | Engine plumbing |
| --- | --- | --- |
| `handle_bytes(now, &[u8])` | `now: Instant` | `Instant::now()` at the read site. |
| `handle_timeout(now)` | `now: Instant` | Reused from the `select!` deadline. |
| `send(handle, msg, publish_time_ms, now)` | `now: Instant` | Producer façade snapshots `Instant::now()` before locking the connection. |
| `flush_producer(handle, publish_time_ms, now)` | `now: Instant` | Same as `send`. |
| `negative_ack(handle, ids, now)` | `now: Instant` | Consumer façade snapshots before locking. |
| `negative_ack_with_delay(handle, msg, delay, now)` | `now: Instant` | Same. |
| `ack_grouped_individual(handle, msg, now)` | `now: Instant` | Same. |
| `ack_grouped_cumulative(handle, msg, now)` | `now: Instant` | Same. |
| `Connection::with_wall_clock_provider(Arc<dyn Fn() -> SystemTime>)` | constructor | Wall-clock injection. Default `\|\| SystemTime::now()`; moonpool sim plugs in a virtual wall clock. |

Internal call paths inside the state machine propagate these parameters
through their helpers (e.g. `ProducerState::queue_send` /
`emit_single` / `emit_chunked` / `flush_batch` / `add_to_batch`,
`ConsumerState::deliver` / `classify_and_queue`); no helper on the hot
path reaches for the host's clock.

The public surface mirrors [`quinn-proto`]:

| Method | Direction | What it does |
| --- | --- | --- |
| `handle_bytes(now, &[u8])` | wire → state | Decode any complete frames in the supplied bytes. Update state, push events, dispatch wakers. |
| `poll_transmit(&mut Vec<u8>) -> usize` | state → wire | Drain queued outbound bytes into the caller's buffer. |
| `poll_event() -> Option<ConnectionEvent>` | state → engine | Yield semantic events (`AuthChallenge`, `TopicListChanged`, `ChecksumMismatch`, …) the engine needs to react to. |
| `poll_timeout() -> Option<Instant>` | state → engine | Next deadline (keepalive, tracker tick, send timeout). |
| `handle_timeout(now)` | engine → state | Drive timers that elapsed. |

### Known non-determinism leaks (documented)

Two non-time sources of host-environment dependency remain in
`magnetar-proto`; both are accepted with rationale:

1. **`uuid::Uuid::new_v4()` in `ProducerState::emit_chunked`** — PIP-37
   chunked messages need a UUID per logical message so the broker can
   reassemble out-of-order chunk frames. Determinising this requires
   injecting an `Arc<dyn Fn() -> Uuid>` through the chunked-emit path;
   deferred until moonpool-sim chaos tests start exercising chunked
   publishes.
2. **`std::env::var()` in `crates/magnetar-proto/src/auth/token.rs`** —
   read once at `TokenAuth` construction so the auth provider can
   resolve `$ENV_VAR -> token text`. This is a one-shot bootstrap read,
   not on the state-machine hot path.

A `cargo xtask check-no-internal-clock` step (planned) will treepunch
the call graph for any new `Instant::now()` / `SystemTime::now()` /
`uuid::new_v4` / `env::var` site introduced outside the documented
leaks above.

### Why we did it

1. **Multi-engine.** The same state machine is driven by `tokio` in
   production and by `moonpool` for deterministic-simulation testing. A
   future `smol` / `async-std` / `glommio` engine is a swap-out, not a
   rewrite. The boundary is the same five methods above.
2. **Testable in isolation.** Every protocol bug can be reproduced with a
   fixture: feed bytes in, observe transmit out. No sockets, no tasks, no
   timing. The 220+ unit tests do exactly this.
3. **No hidden runtime.** The protocol layer does not spawn tasks or hold
   network handles. Everything it owns can be inspected by a debugger
   without async-context glue.
4. **Compiles fast.** Stripping `tokio` from `magnetar-proto`'s dep graph
   saves measurable build time and lets the crate ship as a pure
   `no_std`-adjacent library (we still need `std` for `Instant` and
   `HashMap`, but no async runtime).

### Reference Java code

The state machine maps onto `ClientCnx.java` plus its sibling state objects
(`ProducerImpl.java`, `ConsumerImpl.java`, `HandlerState.java`,
`AckGroupingTracker.java`, `UnAckedMessageTracker.java`,
`NegativeAcksTracker.java`). The handshake states mirror
`HandlerState.State`. See [`crates/magnetar-proto/src/conn.rs:18-26`] for
the cross-reference at the top of `conn.rs`.

[`Cargo.toml`]: crates/magnetar-proto/Cargo.toml
[`quinn-proto`]: https://docs.rs/quinn-proto
[`crates/magnetar-proto/src/conn.rs:18-26`]: crates/magnetar-proto/src/conn.rs

---

## The no-channels rationale

`tokio::sync::mpsc`, `broadcast`, `watch`, `oneshot`, `std::sync::mpsc`,
`crossbeam-channel`, `flume`, `async-channel`, `kanal`, `postage`,
`tachyonix`, `thingbuf` — **forbidden everywhere in the workspace**. The
ban is enforced three ways:

1. `cargo deny check bans` rejects the crates outright in CI.
2. `clippy.toml`'s `disallowed-types` covers `tokio::sync::mpsc::*` and
   friends so even an accidental local import trips a lint.
3. `cargo xtask check-no-channels` greps the entire source tree for
   `::mpsc`, `::broadcast`, `::watch`, `::oneshot` paths as a final
   belt-and-braces.

### Why we banned them

- **Hidden backpressure.** A bounded mpsc that fills up under load surfaces
  as latency in a place the producer cannot see. An unbounded mpsc leaks
  memory. Either failure mode is invisible at the channel's *type
  signature*.
- **Close semantics.** Every channel library has its own answer to "drop
  the receiver while the sender still holds messages". The bug surface
  multiplies with the number of channels in the architecture.
- **Debug "where did this message go?" mode.** Anyone who has chased a
  message through three mpscs across two tasks knows how expensive this
  is. The sans-io split makes the alternative natural and cheap to debug.

### How we replace channels

The single mechanism is a `Waker` slab keyed by `op_id` *inside the state
machine*:

```text
                    user-facing future                       driver loop
                    -----------------                        -----------
                          |                                        |
                          v                                        v
                  ConnectionShared.inner                          owns same
                  parking_lot::Mutex<Connection>                  Arc<ConnectionShared>
                          |                                        |
                          v                                        v
                  on poll(cx):                              on socket read:
                    lock(inner)                               lock(inner)
                    look up the (op_id) outcome               handle_bytes(now, &bytes)
                    if Some(out) -> Poll::Ready(out)          (state machine pushes
                    else                                       OpOutcome into the slab
                      register cx.waker() in slab               and wakes the matching
                    drop(inner)                                 Waker)
                    return Poll::Pending                      drop(inner)
                                                            then drain events
```

The state machine owns:

- A slab of `(PendingOpKey -> Waker)` where `PendingOpKey` is one of
  `Request(RequestId)` for lookups / seeks / acks-with-response, or
  `Send(ProducerHandle, SequenceId)` for publishes.
- A slab of `(PendingOpKey -> OpOutcome)` where the matching response is
  parked until the future polls it.

When `handle_bytes` decodes a `CommandSendReceipt`, it stores the
`OpOutcome::SendReceipt` in the outcome slab keyed by
`(producer_handle, sequence_id)`, then calls `Waker::wake()` on whatever
the producer future registered. The future polls again, locks the
connection, finds the outcome, and resolves.

This is the cancer-free equivalent of a `oneshot<Result<MessageId,
SendError>>`. The "channel" is the slab entry; the "send" is the state
machine populating it; the "receive" is the future polling it. No
backpressure surface, no orphaned senders, no `Drop` glue.

The driver-to-driver communication path is *also* not a channel — it is a
single-cell `tokio::sync::Notify` (the driver wakes on
`shared.driver_waker.notified()`). `Notify` is permitted because it has no
queue and no payload — it is an async condvar, not a channel. If even
`Notify` feels too channel-flavoured, a `parking_lot::Condvar +
Mutex<bool>` is the documented fallback.

### Reference

The pattern is the same one [`quinn`] *would* be using if it didn't ship
its own bespoke `tokio::sync::mpsc` wrapper for legacy reasons —
`quinn-proto` itself is sans-io and channel-free; the channels are only in
the engine glue.

[`quinn`]: https://github.com/quinn-rs/quinn

---

## Concurrency primitives we *do* use

| Primitive | Where | Why |
| --- | --- | --- |
| `parking_lot::Mutex<Connection>` | `ConnectionShared.inner` | The full sans-io state. Critical sections are short and never `.await`. |
| `parking_lot::Mutex<VecDeque<TopicListChange>>` | `ConnectionShared.topic_list_changes` | PIP-145 topic-list-watcher delta buffer surfaced to user futures. |
| `parking_lot::RwLock` | tracker internals | Pure read paths under load. |
| `tokio::sync::Notify` | `ConnectionShared.driver_waker`, `topic_list_notify` | Single-cell async wake-up. Not a channel. |
| `std::sync::atomic::*` | stats + state flags | Lock-free counters. |
| `core::task::Waker` slab | `magnetar-proto::Connection.pending_ops` | Future completion. |
| `tokio::select!` | driver loop | Control-flow multiplexing. Not a channel. |
| `Arc<T>` | `ConnectionShared`, `MessageEncryptor`, `MessageDecryptor`, `AuthProvider`, `MessageRouter`, interceptors | Cheap clone-and-share. |
| `arc_swap::ArcSwap` | rare config-rotation slots | Lock-free swap. |
| `slab::Slab` | per-future Waker keyspace | O(1) insertion + removal. |

Anything not on this list either has a justification in
[GUIDELINES.md](GUIDELINES.md) or is a candidate for removal.

---

## The driver loop

One driver task per connection. Owns the I/O resources (TCP or TLS
stream), the per-connection read buffer, and the `select!` loop that
shuttles bytes between the state machine and the network.

### State diagram

```text
                                ┌─────────────────────────────┐
                                │   Acquire ConnectionShared  │
                                └──────────────┬──────────────┘
                                               │
                                ┌──────────────▼──────────────┐
                                │   loop {                    │
                                └──────────────┬──────────────┘
                                               │
        ┌──────────────────────────────────────▼──────────────────────────────────┐
        │  (1) Lock state. Drain outbound bytes (poll_transmit) into write_buf.   │
        │      Read next deadline (poll_timeout). Read closing-flag. Drop lock.   │
        └──────────────────────────────────────┬──────────────────────────────────┘
                                               │
                                ┌──────────────▼──────────────┐
                                │  (2) write_all(write_buf)   │
                                │      then flush()           │
                                └──────────────┬──────────────┘
                                               │
                                ┌──────────────▼──────────────┐
                                │  (3) if closing: shutdown   │
                                │      return Ok(())          │
                                └──────────────┬──────────────┘
                                               │
                                ┌──────────────▼──────────────┐
                                │  (4) tokio::select! { biased │
                                └──────────────┬──────────────┘
                                               │
              ┌────────────────────────────────┼────────────────────────────────┐
              │                                │                                │
              ▼                                ▼                                ▼
   ┌──────────────────────┐     ┌─────────────────────────┐     ┌─────────────────────────┐
   │ shared.driver_waker  │     │ socket.read_buf(&buf)   │     │ sleep_until(deadline)   │
   │   .notified()        │     │   on Ok(0) -> PeerClosed │     │   on tick -> handle_   │
   │   (user enqueued     │     │   on Ok(n) -> lock +     │     │   timeout(now)           │
   │   a send/ack/etc.)   │     │   handle_bytes(now, &b)  │     │                         │
   │   loop continues     │     │   then drain events      │     │                         │
   └──────────────────────┘     └─────────────────────────┘     └─────────────────────────┘
                                               │
                                ┌──────────────▼──────────────┐
                                │     back to (1)             │
                                └─────────────────────────────┘
```

### Lock discipline

Every interaction with `Connection` happens inside a `parking_lot::Mutex`
critical section. Critical sections are short — they **never `.await`**.
The write_all / flush calls happen *outside* the lock so user futures can
keep enqueuing while the driver holds the network handle.

### Event dispatch

`handle_bytes` is the inbound entry. As frames are decoded, the state
machine populates the outcome slab and calls `Waker::wake()` on whatever
user future is waiting. After the lock drops, the driver pulls semantic
events via `poll_event()` and reacts to the variants that the runtime
layer must handle:

- `ConnectionEvent::AuthChallenge { method, challenge }` — driver
  consults the configured `AuthProvider`, asks it for a fresh blob via
  `respond_to_challenge`, and submits it via `submit_auth_response`
  (PIP-30 / PIP-292). The same hook carries SASL Kerberos / GSSAPI
  continuation tokens: `magnetar_auth_sasl::SaslKerberos` forwards
  each challenge into its wrapped `GssapiClient` so the GSSAPI
  initiate loop runs naturally over the existing trait surface (no
  new `SaslMechanism` trait was needed; see
  [ADR-0029](specs/adr/0029-sasl-kerberos-gssapi-scope.md)).
- `ConnectionEvent::TopicListChanged { added, removed }` — driver pushes
  the delta into `ConnectionShared.topic_list_changes` and wakes
  `topic_list_notify` (PIP-145).
- `ConnectionEvent::ReplicatedSubscriptionMarkerObserved { handle, marker }`
  — driver pushes the observation into
  `ConnectionShared.replicated_subscription_markers` and wakes
  `replicated_subscription_marker_notify` (PIP-33 / ADR-0034). The
  marker is filtered off the user-visible message stream upstream in
  the `magnetar-proto` receive path so it never reaches
  `Consumer::receive`.

The `MessageReceivedFromShadow` variant (PIP-180 / ADR-0033) is
emitted in place of `Message` for shadow-topic consumers; user-facing
futures pick it up directly via the same Waker slab as `Message`, so
the driver does not need to special-case it.

Every other event has already been turned into a future-completion via
the Waker slab inside the state machine; the driver does not need to
touch it.

### Supervised reconnect

When `driver_loop_inner` returns (the socket errored or the peer closed),
the outer `supervised_driver_loop` decides whether to retry. The supervisor:

1. **Records the disconnect** via `Connection::mark_disconnected(now, wall_now)`
   so `Producer::last_disconnected_timestamp` and the consumer stats are
   correct.
2. **Resets the state machine** with `Connection::reset()` — this snaps the
   handshake back to `Uninitialized`, bumps the `session_epoch`, drains the
   pending-op slabs, and **accumulates** in-flight publish snapshots into
   `Connection::in_flight_publish_snapshots` (append, never clear). The
   snapshot is `rebuild_producers`'s single consumer, so multiple reset
   cycles within a single rebuild are safe. User-facing send futures stay
   `Pending`; the snapshot carries enough state to replay them. Producers /
   consumers see `is_connected() = false` but stay live.
3. **Gates on `is_user_closed()`** — only the explicit `Closing` / `Closed`
   states stop the supervisor. The transport-drop state (`Failed`, set by
   `mark_disconnected`) does NOT count as user-closed, so a TCP drop falls
   into the backoff / redial path instead of returning. This is the
   difference between "broker went away" and "user called `.close()`".
4. **Backs off** with a small exponential schedule capped by
   `ReconnectConfig::max_backoff` (jittered by the engine clock — under
   moonpool-sim this is deterministic per seed).
5. **Reconnects** through the same `Transport::connect` path used at
   client init (re-resolving the broker URL via the configured
   `ServiceUrlProvider` on every attempt — this is where PIP-121 plugs in).
6. **Rebuilds producers and consumers** via
   `Connection::rebuild_producers(now)` and
   `Connection::rebuild_consumers(now)`. Each helper re-emits
   `CommandProducer` / `CommandSubscribe` for every still-open handle,
   stamps the new `session_epoch`, and replays the in-flight `OpSend`
   cached wire frames once the broker acks the producer. Consumers
   replay `initial_flow` followed by an explicit
   `CommandRedeliverUnacknowledgedMessages` after `SubscribeAcked`
   (the broker silently drops `CommandFlow` for an unknown
   `consumer_id`, so the Java `ConsumerImpl#reconnectLater` ordering
   is mandatory). User-facing futures stay registered; they get woken
   when the broker re-issues the producer/consumer IDs.

The supervisor never retries past `ReconnectConfig::max_attempts`; on
exhaustion it propagates the last `EngineError::Io` upward and the
`Client` is closed.

#### Transient-error retry (per-handle)

Not every failed re-attach needs a full reset cycle. Pulsar's broker
classifies a subset of `CommandError` codes as transient retry signals:
`MetadataError` (1), `ServiceNotReady` (6), `TopicNotFound` (11) — the
same set Java's `ProducerImpl.handleProducerCreationError` retries on.
A common case is `NamespaceBundleNotServed`, emitted as
`ServiceNotReady` with the text `"Please redo the lookup"`.

For these codes the supervisor:

1. **Retains state.** `Connection::handle_command_error` emits
   `ProducerOpenFailedTransient` / `SubscribeFailedTransient` events;
   the producer / consumer state is NOT removed. Permanent codes
   (e.g. `AuthorizationError`) still drop state and surface
   `ProducerOpenFailed` / `SubscribeFailed` to the user.
2. **Looks up first, then retries.** The driver's
   `handle_pending_events` runs `lookup_then(topic)` before
   `Connection::retry_producer_open(handle)` /
   `retry_consumer_subscribe(handle)`. `lookup_then` issues a
   `CommandLookupTopic`, waits for the `CommandLookupTopicResponse` via
   a `poll_fn` future bound to the existing `PendingOpKey::Request`
   slot, and only then signals the per-handle retry. This is what
   re-acquires bundle ownership on the broker side.
3. **Re-emits a single command.** `retry_producer_open` bumps the
   handle's `epoch`, emits a fresh `CommandProducer`, and calls
   `ProducerState::replay_pending_outbound` so any `OpSend`s
   enqueued during the transient window get their cached wire frames
   re-pushed onto outbound after the targeted re-attach.
   `retry_consumer_subscribe` resumes from `last_acked_message_id`
   when one exists.

The transient path is independent of the full Stage 2 reset cycle:
it keeps the existing connection alive and only rebuilds the
specific handle that errored.

#### Anti-thrash policy (opt-in, ADR-0028)

Some broker conditions cause a different pathology: the broker
**accepts** `CommandProducer` / `CommandSubscribe`, then drops the
TCP connection within a few milliseconds. magnetar's retry path
treats each drop as a transient error and re-attaches, which feeds
the cascade. The observed trigger is post-restart bundle-ownership
churn on `apachepulsar/pulsar:4.0.4` (Pulsar PR #14467 + #13428 +
#12846 — `ServerCnx#handleProducer` ↔ `AbstractTopic#addProducer`
race, amplified by the standalone-mode ZK session timeout).

[`magnetar-proto::AntiThrashState`](crates/magnetar-proto/src/anti_thrash.rs)
is a per-`Connection` bounded ring that records each re-attach
outcome (`ReAttachOk { handle }`) and the TCP-drop deltas that
follow within `drop_grace`. When `N` re-attaches inside a sliding
window of `M` are all followed by `TcpDropAfterReAttach`, the state
emits `ConnectionEvent::AntiThrashCooldown { until }`; the
supervisor honours it by sleeping until `until` before the next
`Transport::connect`. The detector resets on any re-attach that
survives `drop_grace`.

Default: **OFF** — `SupervisorConfig::anti_thrash_threshold: None`.
Recommended opt-in values from
[ADR-0028](specs/adr/0028-supervised-reconnect-anti-thrash-policy.md):
`(N = 5, M = 2 s, K = 50 ms, cooldown = 30 s)`. The
`magnetar-runtime-moonpool` chaos pack ships a
`DropsTcpAfterCreate { delay_ms }` `BrokerWorkload` variant so the
behaviour is exercised under deterministic seeds (see
[`tests/sim_chaos.rs`](crates/magnetar-runtime-moonpool/tests/sim_chaos.rs)).

### Source

[`crates/magnetar-runtime-tokio/src/driver.rs`](crates/magnetar-runtime-tokio/src/driver.rs)
— `driver_loop_inner` + `supervised_driver_loop` total ~425 lines.

---

## Protocol state machine (`magnetar-proto`)

`magnetar-proto::Connection` is the central state machine. Top-level
types live at [`crates/magnetar-proto/src/conn.rs`].

### Handshake state

```text
Uninitialized
    │  (caller queues CommandConnect via Connection::new()+poll_transmit)
    ▼
ConnectSent
    │  (CommandConnected arrives via handle_bytes)
    ▼
Connected   ⇄  AuthChallenging      (PIP-30/292 in-band auth refresh)
    │                  │
    │                  ▼
    │     submit_auth_response → CommandAuthResponse on the wire
    │                  │
    │                  └─ broker accepts → back to Connected
    │                  └─ broker rejects → Failed
    │
    │  (Client::close)
    ▼
Closing
    │  (driver flushes; peer EOF or shutdown())
    ▼
Closed                      Failed   (handshake error / I/O error)
```

`Connection::state()` reports the live state. Source: [`HandshakeState`
enum at conn.rs:52`].

### Pending-op machinery

```rust
pub enum PendingOpKey {
    /// A pending request keyed by request id (lookup, seek, ack-response, etc.).
    Request(RequestId),
    /// A pending publish keyed by `(producer_id, sequence_id)`.
    Send(ProducerHandle, SequenceId),
}

pub enum OpOutcome {
    SendReceipt { sequence_id, message_id },
    SendError   { sequence_id, code, message },
    Success     { request_id },
    Error       { request_id, code, message },
    Lookup      { request_id, outcome: LookupOutcome },
    // ...
}
```

The slab maps `PendingOpKey -> Waker` + `PendingOpKey -> OpOutcome`. A
future registers its waker via `Connection::register_waker(key, waker)`
and consumes the outcome via `Connection::take_outcome(key)`.

### Producer / consumer states

`ProducerState` lives at
[`crates/magnetar-proto/src/producer.rs`](crates/magnetar-proto/src/producer.rs).
`ConsumerState` lives at
[`crates/magnetar-proto/src/consumer.rs`](crates/magnetar-proto/src/consumer.rs).
Both are owned by the parent `Connection` and addressed by stable
`ProducerHandle` / `ConsumerHandle` ids.

A `ProducerState` carries:

- `producer_id`, `producer_name`, `topic`, `schema`, `compression`,
  `access_mode`.
- A `BatchMessageContainer` (only when batching is enabled).
- A chunked-send slot (only when chunking is enabled — chunks-never-batched).
- The send queue (pending `SendDecision`s).
- Per-producer stats counters.

A `ConsumerState` carries:

- `consumer_id`, `consumer_name`, `subscription`, `subscription_type`,
  `read_compacted`, `priority_level`, `key_shared`, `dead_letter_policy`.
- The receive queue (inbound `IncomingMessage`s pending a `receive()` call).
- The optional `AckGroupingTracker`, `NegativeAcksTracker`, and
  `UnackedMessageTracker`.
- The PIP-54 batch-ack table (per-batch position bitset).
- The PIP-4 `crypto_failure_action`.

### Trackers (`magnetar-proto/src/trackers`)

Three single-purpose tick-driven state machines:

| Tracker | Purpose | Lines | API |
| --- | --- | --- | --- |
| `AckGroupingTracker` | Coalesce acks inside a window so we send one `CommandAck` per batch of N acks. Wired via `ConsumerBuilder::ack_group_time`. | 353 | `add(...)`, `add_cumulative(...)`, `poll(now)`. |
| `NegativeAcksTracker` | Defer redelivery commands by `delay`. Optionally drives a `MultiplierRedeliveryBackoff` over the broker-reported `redelivery_count` (PIP-37). | 212 | `add(...)`, `add_with_delay(...)`, `poll(now)`. |
| `UnackedMessageTracker` | Client-side ack-timeout. Forces a `RedeliverUnacknowledged` if no positive ack arrives within `timeout`. Optionally backs off per-message via the same PIP-37 backoff. | 453 | `track(msg_id)`, `ack(msg_id)`, `poll(now)`. |

All three drive off `Connection::poll_timeout` / `handle_timeout` and emit
their outputs as `Vec<TrackerAction>`. The connection turns each action
into an outbound `BaseCommand`.

### Topic-list watcher

`magnetar-proto::topic_watcher::TopicWatcherRegistry` (85 lines) carries
the PIP-145 broker-driven topic-discovery state. The connection handles
`CommandWatchTopicListResponse` / `CommandTopicListUpdated` opcodes and
emits `ConnectionEvent::TopicListChanged` on the event queue. The driver
forwards those to `ConnectionShared.topic_list_changes`, where
`PatternConsumer::update` reconciles them against its child consumers.

### Replicated-subscription markers (`magnetar-proto/src/markers.rs`)

PIP-33 wire payload typing. Defines the `ReplicatedSubscriptionMarkerKind`
enum (`SnapshotRequest=10`, `SnapshotResponse=11`, `Snapshot=12`,
`Update=13`) and the matching `ReplicatedSubscriptionMarkerDetails` sum
type, plus `decode_replicated_subscription_marker(marker_type, payload)`.
Both enums are `#[non_exhaustive]` so future broker-side kinds stay
additive. The decoder returns `Ok(None)` for txn markers (kinds 20..=22)
and any unknown kind — forward-compat for future broker emits.

The connection's receive-path filter at the `pb::base_command::Type::Message`
arm in `conn.rs` consults this decoder before delivering to the
consumer: replicated-subscription markers are diverted into
`ConnectionEvent::ReplicatedSubscriptionMarkerObserved` and never reach
`ConsumerState::deliver`. The consumer's `record_marker_consumed`
helper bumps `consumed_since_flow` so permit accounting stays symmetric
with the broker's view (otherwise the broker's perceived permit budget
would drift by one per marker). See [ADR-0034](specs/adr/0034-pip-33-replicated-subscriptions-scope.md)
and [`docs/replicated-subscriptions.md`](docs/replicated-subscriptions.md).

### Transactions (`magnetar-proto/src/txn.rs`)

Owns the transaction-coordinator client. Pulsar transactions use four
opcodes (`NEW_TXN`, `ADD_PARTITION_TO_TXN`, `ADD_SUBSCRIPTION_TO_TXN`,
`END_TXN_*`) routed to the TC. `TxnClient` carries the `TxnId` registry
and surfaces a Rust `Transaction` handle. The producer attaches `txn_id`
to its publish via `OutgoingMessage::txn`, and the consumer attaches it
to acks via `ack_with_txn` / `ack_cumulative_with_txn` /
`ack_batch_with_txn`.

[`crates/magnetar-proto/src/conn.rs`]: crates/magnetar-proto/src/conn.rs
[`HandshakeState` enum at conn.rs:52`]: crates/magnetar-proto/src/conn.rs

---

## Wire framing

Pulsar's wire format is three nested shapes plus an optional PIP-90
envelope. Magnetar implements the codec in
[`crates/magnetar-proto/src/frame.rs`](crates/magnetar-proto/src/frame.rs)
(620 lines). All multi-byte integers are big-endian. Outer `total_size`
excludes the four bytes used to encode itself.

### Command-only frame

```text
[total_size u32][cmd_size u32][BaseCommand bytes]
```

`total_size == 4 + cmd_size`. Used for opcodes that have no message
payload (`CONNECT`, `CONNECTED`, `LOOKUP`, `SEEK`, `ACK`, …).

### Payload-bearing frame (SEND / MESSAGE)

```text
[total_size u32][cmd_size u32][BaseCommand]
  [0x0e01 u16][crc32c u32]
  [metadata_size u32][MessageMetadata][payload bytes]
```

`crc32c` (Castagnoli) is computed over
`[metadata_size u32 BE][metadata bytes][payload bytes]`. Mismatch →
emit `ConnectionEvent::ChecksumMismatch` and **drop the frame** (per
[GUIDELINES.md §Protocol-correctness invariants point 1](GUIDELINES.md#protocol-correctness-invariants)).

### Broker-entry-metadata envelope (PIP-90)

When the namespace policy enables broker-entry metadata, dispatched
messages carry a `BrokerEntryMetadata` prelude inserted by the broker:

```text
[total_size u32][cmd_size u32][BaseCommand]
  [0x0e02 u16][bem_size u32][BrokerEntryMetadata]
  [0x0e01 u16][crc32c u32][metadata_size u32][MessageMetadata][payload]
```

A producer must **never** emit `0x0e02`. Consumers peel it before parsing
the standard frame and surface it via
`IncomingMessage::broker_entry_metadata` (`broker_publish_time_ms`,
`broker_index`). Source: [`crates/magnetar-proto/src/frame.rs:30-48`].

### Constants

| Constant | Value | Meaning |
| --- | --- | --- |
| `MAGIC_CRC32C` | `0x0e01` | Marks the start of the CRC + metadata prelude. |
| `MAGIC_BROKER_ENTRY_METADATA` | `0x0e02` | Marks the optional PIP-90 envelope. |
| `MAX_FRAME_SIZE` | `5 MiB` | Pulsar default cap. Higher layers may enforce smaller. |

[`crates/magnetar-proto/src/frame.rs:30-48`]: crates/magnetar-proto/src/frame.rs

---

## Producer paths — batching vs chunking

Pulsar enforces a critical invariant per `ProducerImpl.java:630-654`:

> **Chunked messages can never be batched.** If a message is eligible for
> the batch container, `totalChunks` is forced to `1`.

Magnetar mirrors this in `ProducerState::queue_send`:

```text
                              user calls Producer::send(msg)
                                       │
                                       ▼
                          ┌────────────────────────────┐
                          │  ProducerState::queue_send │
                          └─────────────┬──────────────┘
                                        │
                       canAddToBatch(msg) ?
                                        │
                ┌───────────── yes ─────┴────── no ─────────────┐
                │                                                │
                ▼                                                ▼
   ┌─────────────────────────┐                  ┌──────────────────────────┐
   │ Batched path             │                  │ Chunked path              │
   │ -------------            │                  │ -------------            │
   │ - add to BatchMessage    │                  │ - non-batch compress     │
   │   Container.             │                  │ - schema + metadata      │
   │ - flush condition:       │                  │ - split into chunks of   │
   │     max_messages reached │                  │   max_message_size       │
   │     OR max_bytes reached │                  │ - per-chunk metadata     │
   │     OR publish_delay     │                  │   (chunk_id, total_chunks,│
   │     timer fired.         │                  │   uuid) — PIP-37          │
   │ - on flush:              │                  │ - encrypt each chunk     │
   │     serialise singles    │                  │   (if PIP-4 enabled)     │
   │     compress the whole   │                  │ - one CommandSend frame  │
   │     batch                │                  │   per chunk              │
   │     encrypt              │                  │                          │
   │     set batch metadata   │                  │                          │
   │     send                 │                  │                          │
   └─────────────────────────┘                  └──────────────────────────┘
                │                                                │
                └───────────────────┬────────────────────────────┘
                                    ▼
                         single CommandSend frame
                         (or N chunk frames)
                                    │
                                    ▼
                      enter inflight slab keyed by
                      (producer_id, sequence_id)
                                    │
                                    ▼
                              broker SEND_RECEIPT
                                    │
                                    ▼
                  resolve via OpOutcome::SendReceipt → wake SendFut
```

### Batch flush state machine

```text
                       Empty
                         │
                  add(msg)
                         │
                         ▼
                    Buffering ──── publish_delay timer fires ─────────┐
                         │                                            │
                  add(msg) ─── max_messages reached ─── flush         │
                         │                                            │
                  add(msg) ─── max_bytes reached  ─── flush           │
                         │                                            │
                  flush() ─────────────────────────── flush ──────────┤
                         │                                            │
                         ▼                                            ▼
                     Flushing  ──── awaiting SEND_RECEIPT ────────  done
                                        │
                                        ▼
                                     Empty
```

`batching_max_publish_delay` (Java `batchingMaxPublishDelay`) drives the
left-hand timer. The state machine ticks it via `poll_timeout` /
`handle_timeout` so latency is bounded even if the batch never fills.

Source: [`crates/magnetar-proto/src/producer.rs`](crates/magnetar-proto/src/producer.rs).

### Sequence-id discipline

- Sequence ids are assigned inside the chunk loop (Java
  `ProducerImpl.java:696-704`, `:745-753` — both first-send and resend
  paths).
- Resend reuses the original sequence id.
- `last_sequence_id` and `last_sequence_id_published` are tracked
  separately so the runtime can drive resend-safe dedup.
- Sequence id and request id are **monotonically non-decreasing** per
  connection per producer ([GUIDELINES.md §Protocol-correctness
  invariants point 4](GUIDELINES.md#protocol-correctness-invariants)).

---

## Consumer paths — ack grouping, unacked tracker, nack tracker, DLQ

### Inbound message dispatch

```text
                            broker MESSAGE
                                  │
                                  ▼
                         decode_one (frame.rs)
                                  │
                                  ▼
                    crc32c verify (or drop + ChecksumMismatch)
                                  │
                                  ▼
                       peel PIP-90 broker_entry_metadata (if 0x0e02 present)
                                  │
                                  ▼
                         decompress (CompressionKind)
                                  │
                                  ▼
                    decrypt (if PIP-4 keys present + decryptor configured)
                                  │
                                  ▼
                         schema decode (for TypedConsumer)
                                  │
                                  ▼
                  ConsumerState::push_incoming(IncomingMessage)
                                  │
                                  ▼
                  if a receive() future is parked → wake its Waker
                  else                            → queue in receive_queue
```

### Ack grouping flush window

```text
                            user calls Consumer::ack_grouped(msg_id)
                                            │
                                            ▼
                           AckGroupingTracker::add(msg_id)
                                            │
                            ack_group_time timer not yet armed ?
                                            │
                              ┌─── yes ─────┴───── no ──────────────┐
                              │                                     │
                              ▼                                     ▼
                  arm deadline = now + window         deadline already set
                              │                                     │
                              └──────────────┬──────────────────────┘
                                             │
                                             ▼
                                   Connection::poll_timeout
                                   returns the next deadline
                                             │
                                             ▼
                                   driver sleep_until fires
                                             │
                                             ▼
                                   Connection::handle_timeout
                                             │
                                             ▼
                              AckGroupingTracker::poll(now)
                                             │
                                             ▼
                              emit one coalesced CommandAck
                              with all pending ids
                                             │
                                             ▼
                                     unarm deadline
```

The PIP-54 ack_set bitset is stamped on per-batch ids so partial-batch
acks (one position out of N) round-trip correctly.

### Unacked tracker (ack-timeout)

```text
                receive(msg)
                    │
                    ▼
        UnackedMessageTracker::track(msg.id, now + ack_timeout)
                    │
                    │
       (caller does or doesn't ack inside ack_timeout)
                    │
       ┌──── ack arrives ─────┐         ┌──── timer fires ─────┐
       │                      │         │                       │
       ▼                      ▼         ▼                       ▼
   tracker.ack(msg.id)        OK     tracker.poll(now)         emit
   (purge entry)                     returns {redeliver_ids}   CommandRedeliverUnacked
                                                                │
                                                                ▼
                                                  arm next deadline using
                                                  optional PIP-37 backoff
                                                  (multiplier * base_delay,
                                                  capped at max_delay).
```

### Negative-ack tracker

```text
                negative_ack(msg_id) or negative_ack_with_delay(msg_id, d)
                    │
                    ▼
        NegativeAcksTracker::add(msg_id, now + delay)
                    │
                    │
                    ▼
              poll_timeout returns
              the next nack deadline
                    │
                    ▼
              handle_timeout(now)
                    │
                    ▼
              tracker.poll(now)
                    │
                    ▼
              emit CommandRedeliverUnackedMessages
              for ready ids
                    │
                    ▼
              (re-arm if PIP-37 backoff configured)
```

### DLQ + retry-letter

```text
                      receive(msg) — redelivery_count = N
                                  │
                                  ▼
                  if N >= max_redeliver_count
                                  │
                              yes ───── no → normal ack flow
                                  │
                                  ▼
                  push msg into dead_letter_queue
                  on the consumer state
                                  │
                                  ▼
                  user calls Consumer::drain_dead_letter
                                  │
                                  ▼
                  republish to dead_letter_topic
                  (defaults to `<topic>-<subscription>-DLQ`)
                                  │
                                  ▼
                  ack the original msg
```

`Consumer::reconsume_later` is the retry-letter variant: republish to
the retry topic with delay + properties, then ack the original.

---

## Multi-topics fan-in

`MultiTopicsConsumer<C>` and `PatternConsumer<C>` are engine-generic
façade types layered on top of N child consumers — one per subscribed
topic. `C: ConsumerApi` defaults to the tokio runtime's `Consumer`,
and pass-2 (ADR-0037) lifted the impl bodies to dispatch through the
trait so both engines drive the same coordinator unchanged. The
receive race is *not* a channel — it is a
`futures_util::future::select_all` over the child consumers'
`receive()` futures.

```text
            ┌──── child Consumer 1 ────┐
            │  Consumer::receive() ────┼──┐
            └─────────────────────────┘   │
            ┌──── child Consumer 2 ────┐  │
            │  Consumer::receive() ────┼──┼──> select_all picks the first ready
            └─────────────────────────┘   │     and returns (msg, topic).
            ┌──── child Consumer N ────┐  │
            │  Consumer::receive() ────┼──┘     remaining futures stay parked
            └─────────────────────────┘         on their Connection's Waker slab.
```

Per-topic ack / nack / seek dispatch via the topic name attached to the
incoming message.

### Dynamic membership

- `MultiTopicsConsumer::add_topic` / `remove_topic` subscribe and
  unsubscribe at runtime.
- `PatternConsumer` reconciles topic list deltas on demand via
  `update(&client)`. The driver pushes `TopicListChanged` deltas into
  `ConnectionShared.topic_list_changes`; `update()` drains the buffer,
  diffs against `topics()`, and spawns / closes child consumers.
  `start_auto_reconcile(client, interval)` does the same on a
  `tokio::time::interval` schedule.

---

## Pattern consumer + topic watcher (PIP-145)

```text
                       PatternConsumerBuilder::subscribe(&client)
                                            │
                                            ▼
                  Client::watch_topic_list(namespace, pattern)
                                            │
                                            ▼
                  initial snapshot of matching topics
                                            │
                                            ▼
                  open one child Consumer per matched topic
                  (under the same subscription name)
                                            │
                                            ▼
                  return PatternConsumer { children: Mutex<Vec<...>> }
                                            │
                                            ▼
                  meanwhile, on the driver:
                  every CommandTopicListUpdated →
                    ConnectionShared.topic_list_changes.push_back(delta)
                    topic_list_notify.notify_waiters()
                                            │
                                            ▼
                  caller does PatternConsumer::update(&client)
                                            │
                                            ▼
                  drain topic_list_changes
                  diff against current children
                  open new children for `added`
                  close child consumers for `removed`
                  emit a ReconcileReport
```

`PatternConsumer::start_auto_reconcile(client, interval)` spawns a
`tokio::time::interval` loop that calls `update(&client)` on every
tick; the returned `JoinHandle` is used for clean shutdown. Same
pattern as the partitioned producer / consumer auto-update tickers.

---

## Runtime engines

The façade exposes `PulsarClient<E: Engine = TokioEngine>`. `Engine` is
a marker trait selecting per-engine storage; engine-specific methods
live in concrete `impl PulsarClient<TokioEngine>` /
`impl PulsarClient<MoonpoolEngine<P>>` blocks rather than on the trait
([ADR-0019](specs/adr/0019-engine-scope-and-moonpool-parity.md);
source: [`crates/magnetar/src/engine.rs`](crates/magnetar/src/engine.rs)).

### Per-surface extension traits (ADR-0026 §D1)

Dependent façade surfaces lift through per-family extension traits,
each implemented on the runtime's concrete `Client` / `Producer` /
`Consumer` type. `impl<E: Engine> PulsarClient<E> where E::ClientState:
<Trait>` dispatches the user-visible method through the trait. Today's
trait set:

| Trait | Implemented on | Surfaces driven by it |
| --- | --- | --- |
| `TransactionApi` | runtime `Client` | `PulsarClient::new_transaction` + commit/abort. |
| `SubscribeApi` (with `type Consumer: ConsumerApi`) | runtime `Client` | `ConsumerBuilder<'a, E>::subscribe` + every consumer-spawning builder (`MultiTopicsConsumerBuilder<'a, E>`, `PartitionedConsumerBuilder<'a, E>`, `PatternConsumerBuilder<'a, E>`, `ReaderBuilder<'a, E>`, `TypedConsumerBuilder<'a, S, E>`). |
| `CreateProducerApi` (with `type Producer: ProducerApi`) | runtime `Client` | `ProducerBuilder<'a, E>::create` + `TypedProducerBuilder<'a, S, E>`. |
| `ConsumerApi` (with `type Producer: ProducerApi<Error = Self::Error>`) | runtime `Consumer` | All inherent methods of `MultiTopicsConsumer<C>` / `PatternConsumer<C>` / `Reader<C>` / `TableView<C>` (and the DLQ + retry helpers route through the associated `Producer`). |
| `ProducerApi` | runtime `Producer` | `PartitionedProducer<P>` inherent methods. |
| `BrokerMetadataApi` | runtime `Client` | `PulsarClient::partitions_for_topic` / `topic_list_snapshot`; `PartitionedConsumerBuilder` (partition discovery) + `PatternConsumer::update` (PIP-145 delta polling). |

Pass-2 (ADR-0037, commit `4a29ba9`) extended `ConsumerApi` with the
17 trait methods needed to lift `MultiTopicsConsumer<C>` /
`PatternConsumer<C>` impl bodies (13 multi-topic helpers + `pause` /
`resume` / `seek_to_message` / `seek_to_timestamp` + the
`unsubscribe(force: bool)` overload), and introduced
`BrokerMetadataApi` to lift the partition-count + topic-list-watcher
lookups so the three builders are engine-generic end-to-end.

### `magnetar-runtime-tokio` — production (default)

| File | Role |
| --- | --- |
| [`client.rs`](crates/magnetar-runtime-tokio/src/client.rs) | `Client::connect` + `connect_auth` + `connect_with` + transaction-coordinator helpers, partitioned-metadata lookup, topic-list watcher entry point. |
| [`consumer.rs`](crates/magnetar-runtime-tokio/src/consumer.rs) | `Consumer` façade — `receive`, `receive_with_timeout`, `receive_batch_with_bytes_cap`, ack variants (individual / cumulative / batch / with-properties / with-txn / partial-batch), nack, seek, pause/resume, DLQ drain, stats. |
| [`producer.rs`](crates/magnetar-runtime-tokio/src/producer.rs) | `Producer` façade — `send`, `flush`, `close`, stats, sequence-id getters, `MemoryReserveFut` for `ProducerBlock` policy. |
| [`driver.rs`](crates/magnetar-runtime-tokio/src/driver.rs) | Driver loop + supervised reconnect + auth-challenge dispatch + PIP-145 + PIP-188 forwarding. |
| [`auto_cluster_failover.rs`](crates/magnetar-runtime-tokio/src/auto_cluster_failover.rs) | PIP-121 `AutoClusterFailover` with a `HealthProbe` trait + background prober. |
| [`compress.rs`](crates/magnetar-runtime-tokio/src/compress.rs) | Encode + decode for `None` / `Lz4` / `Zlib` / `Zstd` / `Snappy`. |
| [`transport.rs`](crates/magnetar-runtime-tokio/src/transport.rs) | TCP connect + optional `tokio-rustls` wrap, `connect_with_resolver` for `DnsResolver` plumbing. |
| [`tls_insecure.rs`](crates/magnetar-runtime-tokio/src/tls_insecure.rs) | `tls_allow_insecure_connection(true)` blanket override. |
| [`tls_no_hostname.rs`](crates/magnetar-runtime-tokio/src/tls_no_hostname.rs) | `tls_hostname_verification_enable(false)` chain-on / hostname-off. |
| [`dns.rs`](crates/magnetar-runtime-tokio/src/dns.rs) | `DnsResolver` trait + `TokioDnsResolver`. |
| [`lib.rs`](crates/magnetar-runtime-tokio/src/lib.rs) | `ConnectionShared` (state, atomic counters, `memory_used` + `memory_wakers` slab) + `TopicListChange`. |

### `magnetar-runtime-moonpool` — deterministic simulation

| File | Role |
| --- | --- |
| [`lib.rs`](crates/magnetar-runtime-moonpool/src/lib.rs) | `ConnectionShared`, `MoonpoolEngine<P>` generic over `moonpool_core::Providers`, `connect_plain` / `connect_plain_with_resolver` / `connect_plain_supervised` / `connect_tls`. |
| [`driver.rs`](crates/magnetar-runtime-moonpool/src/driver.rs) | Driver loop + supervised reconnect over the moonpool byte pipe. Mirrors `magnetar-runtime-tokio::driver`. |
| [`client.rs`](crates/magnetar-runtime-moonpool/src/client.rs) | `Client<P>` façade — `connect_plain`, `connect_plain_supervised`, partitioned-metadata lookup, txn coordinator helpers. |
| [`producer.rs`](crates/magnetar-runtime-moonpool/src/producer.rs) | `Producer<P>` façade — `send`, `flush`, `close`, stats. Surface mirrors `magnetar-runtime-tokio::producer` (1:1 method set; `FailImmediately` only on the memory-limit knob). |
| [`consumer.rs`](crates/magnetar-runtime-moonpool/src/consumer.rs) | `Consumer<P>` façade — `receive`, ack variants, nack, seek, pause/resume, DLQ drain. |
| [`tls.rs`](crates/magnetar-runtime-moonpool/src/tls.rs) | `RustlsByteAdapter` — drives sans-io `rustls::ClientConnection` over a `NetworkProvider`-supplied byte pipe. Sans-io composition end to end. |
| [`transport.rs`](crates/magnetar-runtime-moonpool/src/transport.rs) | Plaintext byte pipe over the configured `NetworkProvider::TcpStream`. |
| [`dns.rs`](crates/magnetar-runtime-moonpool/src/dns.rs) | `DnsResolver` trait + `StaticDnsResolver` + `arc_dns_resolver` helper. |

Key properties:

- The engine is generic over `moonpool_core::Providers`, which bundles
  `NetworkProvider`, `TimeProvider`, `TaskProvider`, `RandomProvider`,
  `StorageProvider`. Plug `TokioProviders` for production-style runs
  against a real broker; plug a `moonpool-sim` bundle for reproducible
  chaos under a seed.
- The driver consumes the same `magnetar-proto::Connection` state machine
  as the tokio engine — the differences are which byte pipe carries the
  I/O and which clock source the engine snapshots into
  `Connection::send(now, …)` / `flush_producer(now, …)` and into the
  `with_wall_clock_provider` slot.
- TLS handshakes survive chaos with the same determinism as
  `magnetar-proto` itself — the adapter never blocks on a network call
  inside `process_new_packets`; reads and writes go through the byte
  pipe under simulation control.

The full moonpool engine surface (supervised reconnect, chaos pack,
differential equivalence harness) is covered in
[`docs/moonpool-engine.md`](docs/moonpool-engine.md).

---

## PIP-121 cluster failover architecture

The supervised reconnect path (Stage 2) re-resolves the broker URL on every
attempt via a pluggable `ServiceUrlProvider`. Three implementations ship:

```
+--------------------------------+      +--------------------------------+
| StaticServiceUrlProvider       |      | ControlledClusterFailover      |
| (magnetar-proto::service_url)  |      | (magnetar-proto::cluster_*)    |
|                                |      |                                |
| pulsar://a:6650                |      | active = Arc<Mutex<String>>    |
| (never changes)                |      | set_url(...) -> swap           |
+--------------------------------+      +--------------------------------+
                |                                   |
                |                                   |
                v                                   v
+----------------------------------------------------------------------+
| ServiceUrlProvider trait (sync, Send + Sync + Debug)                  |
|   fn get_service_url(&self) -> String                                 |
+----------------------------------------------------------------------+
                            ^
                            |
+----------------------------------------------------------------------+
| AutoClusterFailover (magnetar-runtime-tokio::auto_cluster_failover)   |
|                                                                      |
|   urls:   Arc<Vec<String>>  (priority order; index 0 = primary)      |
|   probe:  Arc<dyn HealthProbe>  (async fn(url) -> bool)              |
|   active: Arc<Mutex<usize>>                                          |
|                                                                      |
| start(interval) -> tokio::spawn(prober) -> JoinHandle                 |
|                                                                      |
|   on every tick: for each url -> probe -> first-healthy-wins         |
|     if active_index changes -> tracing::info!(...) + atomic swap     |
+----------------------------------------------------------------------+

                            |
                            | (consulted on every reconnect attempt)
                            v
+----------------------------------------------------------------------+
| supervised_driver_loop (magnetar-runtime-tokio::driver)               |
|                                                                      |
| loop {                                                                |
|     let url = reconnect_ctx.service_url_provider                      |
|         .as_ref()                                                     |
|         .map(|p| ParsedUrl::parse(&p.get_service_url())?)             |
|         .unwrap_or(&reconnect_ctx.url);                               |
|     Transport::connect_with_resolver(                                 |
|         url, tls_config, dns_resolver.as_deref()                      |
|     ).await?;                                                         |
|     // ... handshake + rebuild_producers + rebuild_consumers          |
| }                                                                     |
+----------------------------------------------------------------------+
```

Java-parity API:

```rust
use magnetar::PulsarClient;
use magnetar_runtime_tokio::AutoClusterFailover;
use std::sync::Arc;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let failover = AutoClusterFailover::new(
    vec![
        "pulsar://primary:6650".into(),
        "pulsar://standby:6650".into(),
    ],
    Arc::new(MyHealthProbe),
);
let _handle = failover.start(std::time::Duration::from_secs(5));

let client = PulsarClient::builder()
    .service_url_provider(Arc::new(failover))
    .build()
    .await?;
# Ok(()) }
```

ADR-0011 (clock injection) is unaffected — the prober uses tokio's wall
clock for its `interval` driver, but the active URL itself is just a
`String`.

---

## memory_limit runtime accounting

Java's `ClientBuilder#memoryLimit(long, MemoryLimitPolicy)` is enforced via
an `AtomicU64` CAS reservation in `Producer::send`:

```
ClientBuilder::memory_limit(bytes, FailImmediately)
   |
   v  (config.memory_limit_bytes = bytes)
ConnectionConfig (magnetar-proto, just a u64; 0 = unlimited)
   |
   v
ConnectionShared (magnetar-runtime-tokio)
  + memory_limit_bytes: u64    (copied from config at construction)
  + memory_used: AtomicU64     (in-flight reserved bytes)

Producer::send(msg):
  let n = msg.payload.len() as u64;
  shared.try_reserve_memory(n)?
      // CAS loop: load(Acquire) -> check current+n <= limit -> compare_exchange(AcqRel)
      // Err(MemoryLimitExceeded { current, limit, requested }) on overflow.
  let result = conn.send(handle, msg, ...);
  match result {
    Ok(seq) => SendFut { reserved_bytes: n, ... },         // released on Poll::Ready
    Err(_)  => { shared.release_memory(n); SendFut { reserved_bytes: 0 } }
  }

SendFut::poll -> Ready -> release_memory(self.reserved_bytes)
SendFut::drop -> release if not already released (caller cancelled)
```

`MemoryLimitPolicy::ProducerBlock` is the other half: on overflow,
`Producer::send` parks on a `Waker` slab inside `ConnectionShared`;
`release_memory` drains the slab so parked producers re-poll the CAS.
See [`docs/memory-limit.md`](docs/memory-limit.md) and
[ADR-0020](specs/adr/0020-memory-limit-producer-block.md).

---

## PIP-188 reconnect-on-migrate flow

The broker can ask the client to move a producer / consumer to a different
broker via `CommandTopicMigrated`. magnetar handles it as:

```
broker -> CommandTopicMigrated { producer | consumer, new_url, new_url_tls }
                |
                v
magnetar-proto::Connection::handle_bytes
                |
                v
ConnectionEvent::TopicMigrated -> events queue
                |
                v
magnetar-runtime-tokio::driver::handle_pending_events
                |
                v
tracing::info!("PIP-188 topic migration; supervised reconnect will fire")
                |
                v
Err(ClientError::Other) -> caught by supervised_driver_loop
                |
                v
Connection::reset() -> backoff -> Transport::connect(...) -> handshake
                |
                v
rebuild_producers() / rebuild_consumers() -> re-emit every still-open
                                              handle's CommandProducer /
                                              CommandSubscribe (new epoch).
                                              Broker-side lookup happens
                                              naturally and yields the new
                                              owner.
```

User futures stay live across the migration. In-flight publishes severed
by the reset surface `OpOutcome::SessionLost` and the user retries (the
planned Stage 3 follow-up is transparent at-least-once replay).

---

## TLS sites

The workspace has **three** TLS sites. **None** use `native-tls`.

1. **`magnetar-runtime-tokio`** — `tokio_rustls::TlsConnector::connect(server_name, tcp)`
   is the standard path. Roots come from
   `rustls-native-certs` by default; users can override with
   `ClientBuilder::tls_trust_certs_pem` / `tls_trust_certs_file_path`,
   in which case `Client::tls_config_from_pem` builds a custom
   `rustls::ClientConfig` from the supplied PEM chain.
2. **`magnetar-runtime-moonpool`** — `tls::RustlsByteAdapter` drives a
   `rustls::ClientConnection` (itself sans-io) over the moonpool
   byte pipe. Each iteration of the driver loop pumps
   `socket.read` → `session.read_tls` → `session.process_new_packets()`
   → drain `session.reader()` into `plaintext_in`. Symmetric on the
   write path.
3. **`magnetar-admin`** — `reqwest` configured with `rustls-tls` for the
   REST admin client.

Source: GUIDELINES.md §"TLS" — rule is hard. `cargo deny check` rejects
`openssl-sys` / `native-tls` / `native-tls-sys` outright.

---

## Schemas

The `Schema` trait lives at
[`crates/magnetar-proto/src/schema/mod.rs`](crates/magnetar-proto/src/schema/mod.rs):

```rust
pub trait Schema: Send + Sync + std::fmt::Debug {
    type Owned: Send + 'static;
    fn schema_type(&self) -> pb::schema::Type;
    fn schema_data(&self) -> Bytes;
    fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError>;
    fn decode(&self, bytes: &[u8]) -> Result<Self::Owned, SchemaError>;
}
```

### Implementations

| Schema | Owned type | Wire bytes |
| --- | --- | --- |
| `BytesSchema` | `Bytes` | passthrough |
| `StringSchema` | `String` | UTF-8 |
| `JsonSchema<T: Serialize + DeserializeOwned>` | `T` | JSON via `serde_json`; broker stores canonicalised form |
| `AvroSchema` | `apache_avro::Value` | Avro single-object encoding; canonical parsing form for version dedup |
| `ProtobufSchema` | `prost::Message` | Protobuf wire encoding; descriptor-based version dedup |
| `ProtobufNativeSchema` | `prost::Message` | Protobuf wire encoding; byte-identical Java `FileDescriptorSet` for version dedup |
| `KeyValueSchema` | `KeyValuePair<K, V>` | Concatenated `(key_len, key, value_len, value)` with `KeyValueEncodingType::{Inline, Separated}` |
| `AutoConsumeSchema` | `GenericRecord` | Trait surface only — broker-driven lookup pending |
| `AutoProduceBytesSchema` | `Bytes` | Trait surface only |
| `Int8Schema` / `Int16Schema` / `Int32Schema` / `Int64Schema` | `iN` | Big-endian fixed-width |
| `FloatSchema` / `DoubleSchema` | `fN` | IEEE 754 big-endian |
| `BoolSchema` | `bool` | Single byte (`0x00` / `0x01`) |
| `DateSchema` / `TimeSchema` / `TimestampSchema` / `LocalDateSchema` / `LocalTimeSchema` | `i64` | 8-byte big-endian |
| `InstantSchema` | `(i64 seconds, i32 nanos)` | 12-byte big-endian |
| `LocalDateTimeSchema` | `(i64 seconds, i32 nanos)` | 12-byte big-endian |

### Canonicalisation (Codex Q4)

Per the cross-check on
`SchemaRegistryServiceImpl.java:405-438`:

- **AVRO / JSON / PROTOBUF** schemas are re-parsed broker-side via the
  Avro `Schema.Parser` before the version lookup. Magnetar emits the
  Avro canonical parsing form (`AvroSchema`) so two logically-identical
  schemas hash to the same version regardless of whitespace, field
  order, or property ordering.
- **PROTOBUF_NATIVE** and **KeyValue** are stored as opaque blobs and
  compared by **raw-byte equality**. The Java client emits a
  `FileDescriptorSet` for `PROTOBUF_NATIVE` and a stable JSON shape
  (`{"key": ..., "value": ..., "keyValueEncodingType": ...}`) for
  `KeyValue`. Magnetar emits byte-identical output for both, otherwise
  the broker would create a fresh schema version on every (re)connect
  and defeat the registry's deduplication.

Source: [`crates/magnetar-proto/src/schema/mod.rs:19-34`](crates/magnetar-proto/src/schema/mod.rs).

### Typed producer / consumer

`magnetar::TypedProducer<S: Schema>` and `magnetar::TypedConsumer<S>`
serialise / deserialise per call. Construction:

```rust,no_run
# use std::sync::Arc;
# use magnetar::PulsarClient;
# use magnetar_proto::schema::AvroSchema;
# async fn run(client: PulsarClient) -> Result<(), Box<dyn std::error::Error>> {
let schema = Arc::new(AvroSchema::new_from_str(r#"
    {"type":"record","name":"User","fields":[
        {"name":"id","type":"long"},
        {"name":"name","type":"string"}
    ]}
"#)?);

let p = client.typed_producer("persistent://public/default/users", schema.clone()).create().await?;
let c = client.typed_consumer("persistent://public/default/users", schema)
    .subscription("readers")
    .subscribe()
    .await?;
# Ok(()) }
```

The schema is advertised on `CommandProducer.schema` /
`CommandSubscribe.schema`; the broker performs version negotiation.

---

## PIP coverage map

| PIP | Title | Status | Lives in |
| --- | --- | --- | --- |
| PIP-4 | End-to-end encryption (AES-GCM) | ✅ | `crates/magnetar-messagecrypto/src/lib.rs:98-220`; bridge: `crates/magnetar/src/crypto_bridge.rs` |
| PIP-22 | DLQ topic | ✅ | `ConsumerBuilder::dead_letter_policy` + `Consumer::drain_dead_letter` |
| PIP-30 | In-band `AUTH_CHALLENGE` refresh | ✅ | `crates/magnetar-proto/src/auth.rs`; dispatch: `crates/magnetar-runtime-tokio/src/driver.rs:42-66` |
| PIP-31 | Transactions | ✅ | `crates/magnetar-proto/src/txn.rs`; client surface: `Client::new_txn`, `add_partition_to_txn`, `add_subscription_to_txn`, `end_txn` |
| PIP-37 | Chunking + `AckTimeoutRedeliveryBackoff` | ✅ | Chunked producer path: `crates/magnetar-proto/src/producer.rs`; backoff: `crates/magnetar-proto/src/trackers/nack.rs` |
| PIP-54 | Partial-batch ACK (ack_set bitset) | ✅ | `crates/magnetar-proto/src/consumer.rs:109-130`; ack stamping: `crates/magnetar-proto/src/conn.rs:1775` |
| PIP-58 | Retry-letter topic | ✅ | `Consumer::reconsume_later` + `reconsume_later_with_properties` |
| PIP-68 | Exclusive producer access mode | ✅ | `ProducerBuilder::access_mode` |
| PIP-90 | Broker-entry metadata envelope | ✅ | `crates/magnetar-proto/src/frame.rs:30-48`; consumer getters: `IncomingMessage::broker_publish_time_ms` / `broker_index` |
| PIP-124 | Multi-DLQ topics for KeyShared | ✅ | DLQ policy infra (shared with PIP-22) |
| PIP-121 | Cluster failover (Auto + Controlled) | ✅ | `crates/magnetar-proto/src/service_url.rs`, `crates/magnetar-proto/src/cluster_failover.rs`, `crates/magnetar-runtime-tokio/src/auto_cluster_failover.rs` (see [ADR-0016](specs/adr/0016-pip-121-cluster-failover.md)) |
| PIP-145 | Topic list watcher (regex pattern) | ✅ | `crates/magnetar-proto/src/topic_watcher.rs`; consumer façade: `crates/magnetar/src/pattern_consumer.rs` |
| PIP-188 | `TOPIC_MIGRATED` → reconnect-on-migrate | ✅ | Driver event arm in `crates/magnetar-runtime-tokio/src/driver.rs` returns `ClientError` to trigger supervised reset + reconnect; see [ADR-0018](specs/adr/0018-pip-188-reconnect-on-migrate.md) |
| PIP-292 | Better in-band auth refresh ergonomics | ✅ | `crates/magnetar-runtime-tokio/src/driver.rs:42-66` |
| PIP-313 | Force unsubscribe | ✅ | `CommandUnsubscribe.force` field plumbed |
| PIP-34 / 119 / 282 / 379 | Key_Shared family | ✅ | `magnetar_proto::KeySharedConfig` + builder routing |
| PIP-391 | Batch-index ACK polish | ✅ | Pairs with PIP-54 |
| PIP-409 | DLQ + retry-letter polish | ✅ | DLQ + reconsume_later wiring |
| PIP-460 | Scalable topics | ❌ | v0.2.0 wave open (upstream `Draft`; targets Pulsar 5.0 LTS) — see [ADR-0031](specs/adr/0031-pip-460-scalable-subscription-scope.md) + [`specs/proposals/pip-460-scalable-topics.md`](specs/proposals/pip-460-scalable-topics.md) |
| PIP-466 | V5 client API surface | ❌ | v0.2.0 wave open (upstream design-phase; thin skin over v4 wire) — see [ADR-0032](specs/adr/0032-pip-466-v5-client-surface-scope.md) + [`specs/proposals/pip-466-v5-client-surface.md`](specs/proposals/pip-466-v5-client-surface.md) |
| PIP-180 | Shadow topic | ✅ | v0.2.0 — admin REST (`create_shadow_topic` / `delete_shadow_topic` / `get_shadow_topics` / `get_shadow_source`), producer-side `send_with_source_message_id` propagating `CommandSend.message_id`, consumer-side `MessageReceivedFromShadow` event, structural `MessageId` equality across source ⇄ shadow. See [`docs/shadow-topic.md`](docs/shadow-topic.md) + [ADR-0033](specs/adr/0033-pip-180-shadow-topic-scope.md). |
| PIP-415 | `getMessageIdByIndex` | ✅ | `crates/magnetar-admin/src/lib.rs::AdminClient::topic_get_message_id_by_index` — REST-only ([PIP-415 spec](https://github.com/apache/pulsar/blob/master/pip/pip-415.md) leaves "Binary protocol" empty; canonical impl [`apache/pulsar#24222`](https://github.com/apache/pulsar/pull/24222) is admin/broker/CLI only) |
| PIP-33 | Replicated subscriptions | ✅ | v0.2.0 — `ConsumerBuilder::replicate_subscription_state(bool)` on the façade flips `CommandSubscribe` field 14; receive-path filter in `magnetar-proto::conn` drops `REPLICATED_SUBSCRIPTION_*` markers and surfaces them via `PulsarClient::next_replicated_subscription_marker` / `poll_replicated_subscription_marker`. Client never originates markers — broker-side machinery only. See [`docs/replicated-subscriptions.md`](docs/replicated-subscriptions.md) + [ADR-0034](specs/adr/0034-pip-33-replicated-subscriptions-scope.md). |

---

## Tests

See [`docs/testing.md`](docs/testing.md) for the full reference (unit,
integration, deterministic chaos, differential equivalence, e2e, mutation,
fuzz). High-level summary:

- **Unit + integration**: `cargo test --workspace --all-features`. Every
  sans-io behavior is exercised by feeding bytes, asserting events /
  transmit / state. Trackers ship 13 ported behavioral cases from Java's
  `UnAckedMessageTrackerTest` + `AckGroupingTrackerTest`; the producer
  ships 6 ported cases from `BatchMessageContainerImplTest`.
- **Deterministic chaos** ([`crates/magnetar-runtime-moonpool/tests/`](crates/magnetar-runtime-moonpool/tests/)):
  the moonpool engine drives the supervised reconnect path, PIP-121,
  PIP-188, virtual-clock timers, and OAuth2 refresh edges under
  reproducible seeds.
- **Differential equivalence** ([`crates/magnetar-differential/tests/`](crates/magnetar-differential/tests/)):
  tokio + moonpool engines run the same `Trace` against a scripted
  in-process broker; user-visible `EventStream`s must agree.
- **End-to-end** ([`crates/magnetar/tests/e2e_*.rs`](crates/magnetar/tests/)):
  gated on `--features e2e` + `#[ignore = "e2e: requires Docker"]`.
  Spins `apachepulsar/pulsar:4.0.4` via `testcontainers`. Covers
  schemas, DLQ, batching+chunking, interceptors, transactions,
  subscription types, partitioned, compacted+TableView, encryption,
  OAuth2, DNS resolver, force unsubscribe, memory limit, pattern
  auto-reconcile, supervised reconnect, rolling stats, per-partition
  seek, PIP-121 cluster failover.

Run them: `cargo test --workspace --features e2e -- --include-ignored`
(requires Docker).

### Mutation testing (scoped)

```sh
cargo mutants --package magnetar-proto --timeout 60 --shard 1/4
```

Targets: frame decode, request correlation, resend / dedup, flow
permits, chunk metadata, timeout transitions.

### Fuzz (`magnetar-proto/fuzz`)

```sh
cargo +nightly fuzz run encode_roundtrip
```

Round-trip-encodes `BaseCommand` shapes and asserts re-decode equality.

---

## Build & validation

Stable Rust **1.85** (workspace MSRV in `rust-toolchain.toml`).

### Per-commit chain

```sh
cargo build --workspace --all-features
cargo clippy --workspace --all-features -- -D warnings
cargo +nightly fmt --check
cargo test --workspace
cargo deny check
RUSTDOCFLAGS="-D warnings --cfg tokio_unstable" \
  cargo doc --no-deps --all-features --workspace --locked
```

### When touching `magnetar-proto`

```sh
cargo xtask check-no-channels   # greps src/** for banned channel paths
cargo xtask check-no-io-deps    # asserts magnetar-proto has no I/O deps
cargo xtask codegen --check     # asserts proto codegen has no drift
```

### Workspace lints

`forbid(unsafe_code)` workspace-wide. `unreachable_pub = "warn"`,
`missing_debug_implementations = "warn"`. Pedantic clippy on the whole
workspace with `cast_possible_truncation`, `cast_sign_loss`,
`cast_possible_wrap`, `module_name_repetitions`, `must_use_candidate`,
`missing_errors_doc`, `missing_panics_doc`, and `unnecessary_literal_bound`
allowed (justification in workspace `Cargo.toml`).

### Forbidden crates (`cargo deny bans deny`)

Channel-shaped: any crate that ships an `mpsc` / `broadcast` / `watch` /
`oneshot` flavour — `crossbeam-channel`, `flume`, `async-channel`,
`kanal`, `postage`, `tachyonix`, `thingbuf`, plus the corresponding
`tokio::sync::*` paths via `clippy.toml`'s `disallowed-types`.

TLS-related: `openssl-sys`, `openssl`, `native-tls`, `native-tls-sys`.

### Dependency allow-list

The final allow-list is tracked internally and enforced through
`cargo deny`. Any addition needs explicit project-owner approval.

---

## Further reading

- [README.md](README.md) — user-facing entry point.
- [GUIDELINES.md](GUIDELINES.md) — coding conventions + invariants.
- [CONTRIBUTING.md](CONTRIBUTING.md) — patch flow + sign-off.
- The Apache Pulsar Java client at
  [`apache/pulsar/pulsar-client`](https://github.com/apache/pulsar/tree/master/pulsar-client)
  — primary parity reference.
- `quinn-proto` at
  [`quinn-rs/quinn/quinn-proto`](https://github.com/quinn-rs/quinn/tree/main/quinn-proto)
  — sans-io reference shape that `magnetar-proto::Connection` mirrors.
