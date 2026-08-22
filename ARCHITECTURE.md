# Magnetar — Architecture

> **Audience.** This document is for engineers evaluating, contributing to, or porting code into magnetar.
> It explains _how_ the workspace is wired and _why_. See [README.md](README.md) for the user-facing surface and the Java parity matrix.

---

## Table of contents

1. [Overview](#overview)
2. [Layering](#layering)
3. [Sans-io design](#sans-io-design)
4. [The no-channels rationale](#the-no-channels-rationale)
5. [Concurrency primitives we _do_ use](#concurrency-primitives-we-do-use)
6. [The driver loop](#the-driver-loop)
7. [Protocol state machine (`magnetar-proto`)](#protocol-state-machine-magnetar-proto)
8. [Wire framing](#wire-framing)
9. [Producer paths — batching vs chunking](#producer-paths--batching-vs-chunking)
10. [Consumer paths — ack grouping, unacked tracker, nack tracker, DLQ](#consumer-paths--ack-grouping-unacked-tracker-nack-tracker-dlq)
11. [Multi-topics fan-in](#multi-topics-fan-in)
12. [Pattern consumer + topic watcher (PIP-145)](#pattern-consumer--topic-watcher-pip-145)
13. [Runtime engines](#runtime-engines)
14. [TLS sites](#tls-sites)
15. [Schemas](#schemas)
16. [PIP coverage map](#pip-coverage-map)
17. [Tests](#tests)
18. [Build & validation](#build--validation)
19. [Further reading](#further-reading)

---

## Overview

A bird's-eye view of the workspace before the deep dive.
For binding decisions read the [ADR series](specs/adr/); for the user-facing surface read [README.md](README.md).

### Crate topology

```text
crates/
  magnetar/                       Public façade (crates.io package `magnetar-driver`, library / import name `magnetar`) — PulsarClient<E>, builders, typed schemas, partitioned / multi-topics / pattern / reader / table-view / interceptors
  magnetar-proto/                 Sans-io state machine + codec + trackers + topic watcher (zero I/O deps)
  magnetar-runtime-tokio/         Production engine (TCP, tokio-rustls, supervised reconnect)
  magnetar-runtime-moonpool/      Deterministic-simulation engine over moonpool_core::Providers (rustls byte-pipe)
  magnetar-admin/                 reqwest-backed REST admin client (rustls-tls)
  magnetarctl/                   `magnetarctl` crate + binary
  magnetar-fakes/                 In-process broker stub for tests
  magnetar-messagecrypto/         PIP-4 AES-GCM (aws-lc-rs)
  magnetar-auth-oauth2/           ClientCredentialsFlow + token caching
  magnetar-auth-sasl/             SASL PLAIN + Kerberos/GSSAPI (libgssapi behind `kerberos` feature)
  magnetar-auth-athenz/           Athenz role-token auth + optional ZTS round-trip
  magnetar-differential/          tokio ↔ moonpool differential equivalence harness (test-only)
xtask/                            Workspace automation (check-no-channels, check-no-io-deps, check-no-internal-clock, codegen)
```

The dependency direction is strictly downward:

```text
magnetarctl ──> magnetar-admin
           └──> magnetar ──> magnetar-runtime-tokio    ──┐
                          ├──> magnetar-runtime-moonpool ──┤
                          ├──> magnetar-auth-{oauth2,sasl,athenz}
                          └──> magnetar-messagecrypto    ──┤
                                                           v
                                                    magnetar-proto
```

`magnetar-proto` is the only mandatory dependency for every other crate.
Engine, auth, and crypto crates implement traits owned by `magnetar-proto` and the façade.
Feature flags on `magnetar` gate which engine and which auth providers compile in ([README.md#installation](README.md#installation)).

### Sans-io invariants

The crate split is enforced, not aspirational. Each rule below is wired into a CI gate and has a corresponding ADR.

| Invariant                                                            | ADR                                                                                                              | Enforcement                                                                                         |
| -------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `magnetar-proto` has zero I/O deps                                   | [ADR-0004](specs/adr/0004-sans-io-protocol-core.md)                                                              | `cargo run -p xtask -- check-no-io-deps`                                                            |
| No channel crates anywhere                                           | [ADR-0003](specs/adr/0003-no-channels-rule.md)                                                                   | `cargo run -p xtask -- check-no-channels` + `clippy.toml::disallowed-types` + `cargo deny bans`     |
| `magnetar-proto` does not read the host clock                        | [ADR-0011](specs/adr/0011-clock-injection-sans-io.md)                                                            | `cargo run -p xtask -- check-no-internal-clock`                                                     |
| Generated proto code stays in lockstep with the vendored `.proto`    | [ADR-0004](specs/adr/0004-sans-io-protocol-core.md)                                                              | `cargo run -p xtask -- codegen --check`                                                             |
| `rustls` only (openssl admitted only via `rustls-openssl`)           | [ADR-0005](specs/adr/0005-rustls-only-tls.md) amended by [ADR-0035](specs/adr/0035-pluggable-crypto-provider.md) | `deny.toml` bans `native-tls`; `openssl` / `openssl-sys` scoped via `wrappers = ["rustls-openssl"]` |
| Pluggable rustls crypto provider (aws-lc-rs / ring / openssl / fips) | [ADR-0035](specs/adr/0035-pluggable-crypto-provider.md)                                                          | `cargo run -p xtask -- check-crypto-matrix` + cfg-cascade `compile_error!`                          |
| CRC32C verify-or-drop on frames                                      | (in-protocol; see §[Wire framing](#wire-framing))                                                                | `magnetar-proto::Connection::handle_bytes` drops mismatching frames; covered by codec tests         |
| Sequence-id monotonicity                                             | (see §[Producer paths — batching vs chunking](#producer-paths--batching-vs-chunking))                            | Producer batch + chunk paths assert monotonic `SequenceId` on every send                            |

The clock-injection check has **no** allowlist as of [ADR-0086](specs/adr/0086-inject-now-into-proto-latency-recording.md): `xtask/src/main.rs::CLOCK_LEAK_ALLOWLIST` is empty and should stay that way.
Two documented **non-time** leak sites remain in [`crates/magnetar-proto`](crates/magnetar-proto) — the PIP-37 chunk-set `uuid::Uuid::new_v4()` in [`producer.rs`](crates/magnetar-proto/src/producer.rs) and the one-shot `std::env::var()` bootstrap in [`auth/token.rs`](crates/magnetar-proto/src/auth/token.rs) — but the gate has never scanned for either pattern, so they are tracked only by [§Known non-determinism leaks (documented)](#known-non-determinism-leaks-documented).
They previously had allowlist entries on that rationale, which bought no enforcement and hid one of the two `.elapsed()` leaks ADR-0086 closed.

### Engine boundary

`PulsarClient<E: Engine = TokioEngine>` is generic over an `Engine` marker trait that selects per-engine storage ([`crates/magnetar/src/engine/mod.rs`](crates/magnetar/src/engine/mod.rs)).
Two engines ship:

- `TokioEngine` — production default.
  Pulls in `tokio` + `tokio-rustls`.
  One driver task per connection.
  Lives in [`magnetar-runtime-tokio`](crates/magnetar-runtime-tokio).
- `MoonpoolEngine<P>` — deterministic-simulation engine, generic over a `moonpool_core::Providers` bundle.
  Lives in [`magnetar-runtime-moonpool`](crates/magnetar-runtime-moonpool).
  Deep dive in [`docs/moonpool-engine.md`](docs/moonpool-engine.md).

Engine-specific methods (`producer`, `consumer`, partitioned, …) live in concrete `impl PulsarClient<TokioEngine>` / `impl PulsarClient<MoonpoolEngine<P>>` blocks rather than on the trait.
The connect signatures differ enough (URL vs. `host:port` + providers bundle) that a single `Engine::connect(...)` would either lose typing or reintroduce per-engine duplication ([ADR-0019](specs/adr/0019-engine-scope-and-moonpool-parity.md) §"Option B rejected").

Most user-facing builders and dependent surfaces live on the engine-generic `impl<E: Engine> PulsarClient<E>` block and dispatch through per-surface extension traits (`SubscribeApi`, `CreateProducerApi`, `BrokerMetadataApi`, `TransactionApi`, `ProducerApi`, `ConsumerApi`).
The `ConsumerApi` / `BrokerMetadataApi` lift to engine-generic shipped in [ADR-0037](specs/adr/0037-multi-topics-pattern-consumer-pass-2-lift.md).
Tokio-only helper methods still yield a clean compile error when called against `PulsarClient<MoonpoolEngine<P>>` rather than a silent fallback.

### Receive-path classifiers

The `ConnectionEvent` stream is a single ordered queue, but the receive dispatch in `magnetar-proto::Connection` runs a thin classifier before emitting so callers see the most specific variant that matches the inbound frame.
Two features use this pattern:

- **Shadow-topic dispatch (PIP-180 / ADR-0033)** — when a consumer is shadow-attached via [`ConsumerState::set_shadow_metadata`](crates/magnetar-proto/src/consumer.rs) AND the inbound `MessageMetadata.replicated_from` is populated, the classifier emits `ConnectionEvent::MessageReceivedFromShadow` in place of `ConnectionEvent::Message`.
  Regular (non-shadow) topics keep emitting `Message` — the wire path stays byte-identical.
- **Replicated-subscription markers (PIP-33 / ADR-0034)** — markers carried in the payload of a `CommandMessage` with magic type `MarkerType::REPLICATED_SUBSCRIPTION_*` are intercepted by the consumer's receive path and re-emitted as `ConnectionEvent::ReplicatedSubscriptionMarkerObserved` rather than surfaced to user code as a regular `Message`.

Both classifiers stay sans-io: they read only the per-consumer state cache (populated externally by the runtime engine at subscribe time) and the inbound metadata.
No I/O, no clock reads.
Full surfaces are documented under the PIP feature notes in [`docs/pip-features.md`](docs/pip-features.md).

### Auto-update tickers

Several Java client features rely on periodic background work (`PatternConsumer` topic rediscovery, `TableView` partition tracking, `PartitionedProducer`/`PartitionedConsumer`/`MultiTopicsConsumer` partition-count updates).
The pattern is uniform:

- The ticker spawns a `tokio::time::interval` task that signals a `Notify` on every tick.
- The runtime façade (`magnetar-runtime-tokio`) takes the `Instant::now()` snapshot at the call site and forwards it into `magnetar-proto::Connection` entries.
- `magnetar-proto` itself never reads the host clock — the `check-no-internal-clock` xtask enforces this.

The schedule API lives on the relevant builder (`PartitionedProducerBuilder::auto_update_partitions_interval`, `MultiTopicsConsumerBuilder::auto_update_partitions_interval`, `TableViewBuilder::auto_update_partitions_interval`, `PatternConsumer::start_auto_reconcile`).

### Push delivery (consumer `MessageListener`)

`ConsumerBuilder::message_listener(...)` + `subscribe_with_listener()` (and the `TypedConsumerBuilder` twin) flip a consumer from pull to push, mirroring Java `ConsumerBuilder#messageListener` ([ADR-0064](specs/adr/0064-consumer-message-listener-push-delivery.md)).
The mechanism is the same `tokio::spawn`ed `loop { receive(); callback }` background task `TableView::listen` uses (`crates/magnetar/src/table_view.rs` `spawn_drain`), generalised in `crates/magnetar/src/consumer_listener.rs` over `C: ConsumerApi + Clone` so the tokio and moonpool consumers share one poller.
It stays out of `magnetar-proto` (which cannot spawn tasks or invoke callbacks): the poller lives entirely in the façade and drives the engine's existing `receive()`, which already parks on the per-consumer `Notify` / `Waker` slab inside the sans-io state machine — no channel (ADR-0003), no new lock (ADR-0038), no host-clock read (ADR-0011).
Delivery is sequential and in order, and the callback acks explicitly (the poller never auto-acks).
An explicit or terminal remote close ends the task when `receive()` returns an error.
Dropping the returned `MessageListenerHandle` instead aborts the poller and drops its owned consumer clone; only the final clone stages the best-effort close.

The same push surface extends to the wrapper consumers — `MultiTopicsConsumer`, `PartitionedConsumer`, `PatternConsumer` — via a second poller (`spawn_wrapper_message_listener`) generic over the `WrapperReceiver` trait, since those are not `ConsumerApi` (their `receive()` yields a topic-tagged message).
Its callback is `Fn(&str, &IncomingMessage)`: the originating topic is the extra argument so the callback can route an explicit ack to the right child.
Pattern / partition children discovered **after** subscribe inherit the listener — each poller iteration races the in-flight `receive()` against a membership-change `Notify` the wrapper signals on every child add, so a child that joins while the poller is parked is swept on the next iteration (matching Java's parent-owns-the-listener model, where every child is created with `messageListener` `null`).

---

## Layering

Magnetar is organised in four layers.
Lower layers know nothing about higher ones — `magnetar-proto` is pure-Rust state machines with **zero I/O dependencies**, and the high-level façade is a thin re-export plus ergonomics layer.

```text
+--------------------------------------------------------------------------+
|                                user code                                   |
+--------------------------------------------------------------------------+
                                    |
                                    v
+--------------------------------------------------------------------------+
| magnetar (façade)                | magnetarctl       | magnetar-admin     |
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
magnetarctl ──> magnetar-admin
           └──> magnetar (faç.) ──> magnetar-runtime-tokio ───┐
                                ├──> magnetar-runtime-moonpool ┤
                                ├──> magnetar-auth-{oauth2,sasl,athenz}
                                └──> magnetar-messagecrypto ───┤
                                                               v
                                                       magnetar-proto
```

`magnetar-proto` is the only mandatory dependency for every other crate.
`magnetar-auth-*` and `magnetar-messagecrypto` provide trait implementations for traits owned by `magnetar-proto` and the runtime engines.
The auth + messagecrypto crates are gated by feature flags on `magnetar` (see [README.md §Installation](README.md#installation)).

---

## Sans-io design

### What "sans-io" means here

`magnetar-proto::Connection` is a synchronous state machine.
It has **no sockets, no `tokio`, no `async`, no threads**, and **never reads its own clock**. The whole crate's [`Cargo.toml`] forbids I/O-bound dependencies — the rule is enforced by a `cargo run -p xtask -- check-no-io-deps` step that walks `cargo tree -p magnetar-proto -e features` and trips on `tokio`, `mio`, `socket2`, … ([GUIDELINES.md §I/O isolation](GUIDELINES.md#io-isolation)).

### Clock injection

The state machine takes the monotonic clock as a parameter at every user-driven entry, and reads the wall clock through an injected provider.
Engines snapshot the host clocks at the call site (or, in moonpool simulation, the virtual clock); the protocol layer never calls `Instant::now()` or `SystemTime::now()` itself.

| Entry                                                               | Clock parameter | Engine plumbing                                                                                                                                     |
| ------------------------------------------------------------------- | --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `handle_bytes(now, &[u8])`                                          | `now: Instant`  | `Instant::now()` at the read site.                                                                                                                  |
| `handle_timeout(now)`                                               | `now: Instant`  | Reused from the `select!` deadline.                                                                                                                 |
| `send(handle, msg, publish_time_ms, now)`                           | `now: Instant`  | Producer façade snapshots `Instant::now()` before locking the connection.                                                                           |
| `flush_producer(handle, publish_time_ms, now)`                      | `now: Instant`  | Same as `send`.                                                                                                                                     |
| `negative_ack(handle, ids, now)`                                    | `now: Instant`  | Consumer façade snapshots before locking.                                                                                                           |
| `negative_ack_with_delay(handle, msg, delay, now)`                  | `now: Instant`  | Same.                                                                                                                                               |
| `ack_grouped_individual(handle, msg, now)`                          | `now: Instant`  | Same.                                                                                                                                               |
| `ack_grouped_cumulative(handle, msg, now)`                          | `now: Instant`  | Same.                                                                                                                                               |
| `pop_message(handle, now)`                                          | `now: Instant`  | Consumer façade snapshots before locking; the sample is `now - arrived_at` ([ADR-0086](specs/adr/0086-inject-now-into-proto-latency-recording.md)). |
| `Connection::with_wall_clock_provider(Arc<dyn Fn() -> SystemTime>)` | constructor     | Wall-clock injection. Default `\|\| SystemTime::now()`; moonpool sim plugs in a virtual wall clock.                                                 |

Internal call paths inside the state machine propagate these parameters through their helpers (e.g. `ProducerState::queue_send` / `emit_single` / `emit_chunked` / `flush_batch` / `add_to_batch`, `ConsumerState::deliver` / `classify_and_queue`); no helper on the hot path reaches for the host's clock.
The two latency-recording helpers are part of that propagation, not exceptions to it: `ConsumerState::pop_message(now)` records `now - msg.arrived_at` and `ProducerState::apply_receipt(receipt, now)` records `now - op.enqueued_at`, the latter receiving its instant from `handle_frame(now, …)` on the `CommandSendReceipt` path.
Both use `saturating_duration_since`, never the `Sub` impl, which panics on underflow (invariant #6).

The public surface mirrors [`quinn-proto`]:

| Method                                    | Direction      | What it does                                                                                                     |
| ----------------------------------------- | -------------- | ---------------------------------------------------------------------------------------------------------------- |
| `handle_bytes(now, &[u8])`                | wire → state   | Decode any complete frames in the supplied bytes. Update state, push events, dispatch wakers.                    |
| `poll_transmit(&mut Vec<u8>) -> usize`    | state → wire   | Drain queued outbound bytes into the caller's buffer.                                                            |
| `poll_event() -> Option<ConnectionEvent>` | state → engine | Yield semantic events (`AuthChallenge`, `TopicListChanged`, `ChecksumMismatch`, …) the engine needs to react to. |
| `poll_timeout() -> Option<Instant>`       | state → engine | Next deadline (keepalive, tracker tick, send timeout).                                                           |
| `handle_timeout(now)`                     | engine → state | Drive timers that elapsed.                                                                                       |

Diagnostics are two-channel per [ADR-0054](specs/adr/0054-logging-policy.md): semantic events the engine must react to ride `poll_event()`, while proto also emits `tracing` logs at points of detection (checksum mismatch, lookup redirects, handshake transitions) under the single-owner rule — each fault logs exactly once, at the layer holding the richest context.

### Known non-determinism leaks (documented)

Two non-time sources of host-environment dependency remain in `magnetar-proto`; both are accepted with rationale.
A third, façade-level leak is listed after them for completeness — it is not a `magnetar-proto` leak and is not covered by the gate below.
No **time** leaks remain: the last two (`Instant::elapsed()` in the latency-recording sites) were closed by [ADR-0086](specs/adr/0086-inject-now-into-proto-latency-recording.md), which also emptied the `xtask` clock allowlist.
The `uuid` and `env::var` entries below were the allowlist's stated rationale even though the gate never scanned for either pattern; this section is now their sole inventory.

1. **`uuid::Uuid::new_v4()` in `ProducerState::emit_chunked`** — PIP-37 chunked messages need a UUID per logical message so the broker can reassemble out-of-order chunk frames.
   Determinising this requires injecting an `Arc<dyn Fn() -> Uuid>` through the chunked-emit path; deferred until moonpool-sim chaos tests start exercising chunked publishes.
2. **`std::env::var()` in `crates/magnetar-proto/src/auth/token.rs`** — read once at `TokenAuth` construction so the auth provider can resolve `$ENV_VAR -> token text`.
   This is a one-shot bootstrap read, not on the state-machine hot path.

3. **`opentelemetry::Context::current()` + global propagator in `crate::otel::inject_context`** — the façade reads the caller's ambient OTel span context and the process-global propagator to inject `traceparent`/`tracestate` properties at every tokio send boundary (producer send, plus the `TypedConsumer` retry-letter `reconsume_later` and DLQ `republish_dead_letters` re-injection paths, ADR-0053 §D2).
   This is a façade-level (not `magnetar-proto`) leak, gated on `feature = "opentelemetry"` + `feature = "tokio"`.
   The moonpool engine never calls `inject_context` on any path, keeping sim determinism intact (ADR-0053).

A `cargo run -p xtask -- check-no-internal-clock` step rejects any `Instant::now()`, `SystemTime::now()`, or `.elapsed()` occurrence in `crates/magnetar-proto/src/**` outside `#[cfg(test)]`, with **no** file allowlist ([ADR-0011](specs/adr/0011-clock-injection-sans-io.md), [ADR-0086](specs/adr/0086-inject-now-into-proto-latency-recording.md)).
`uuid::new_v4` and `env::var` are **not** mechanically enforced — the gate has never scanned for them, and the two leaks above are tracked by this inventory alone.
The scanner skips `#[cfg(test)]` spans and lexically inert regions (comments, string / raw-string / char literals) via the same helpers `check-log-fields` uses, and is unit-tested in `xtask/src/main.rs`.

### Why we did it

1. **Multi-engine.** The same state machine is driven by `tokio` in production and by `moonpool` for deterministic-simulation testing.
   A future `smol` / `async-std` / `glommio` engine is a swap-out, not a rewrite.
   The boundary is the same five methods above.
2. **Testable in isolation.** Every protocol bug can be reproduced with a fixture: feed bytes in, observe transmit out.
   No sockets, no tasks, no timing.
   The 220+ unit tests do exactly this.
3. **No hidden runtime.** The protocol layer does not spawn tasks or hold network handles.
   Everything it owns can be inspected by a debugger without async-context glue.
4. **Compiles fast.** Stripping `tokio` from `magnetar-proto`'s dep graph saves measurable build time and lets the crate ship as a pure `no_std`-adjacent library (we still need `std` for `Instant` and `HashMap`, but no async runtime).

### Reference Java code

The state machine maps onto `ClientCnx.java` plus its sibling state objects (`ProducerImpl.java`, `ConsumerImpl.java`, `HandlerState.java`, `AckGroupingTracker.java`, `UnAckedMessageTracker.java`, `NegativeAcksTracker.java`).
The handshake states mirror `HandlerState.State`.
See [`crates/magnetar-proto/src/conn.rs:18-26`] for the cross-reference at the top of `conn.rs`.

[`Cargo.toml`]: crates/magnetar-proto/Cargo.toml
[`quinn-proto`]: https://docs.rs/quinn-proto
[`crates/magnetar-proto/src/conn.rs:18-26`]: crates/magnetar-proto/src/conn.rs

---

## The no-channels rationale

`tokio::sync::mpsc`, `broadcast`, `watch`, `oneshot`, `std::sync::mpsc`, `crossbeam-channel`, `flume`, `async-channel`, `kanal`, `postage`, `tachyonix`, `thingbuf` — **forbidden everywhere in the workspace**. The ban is enforced three ways:

1. `cargo deny check bans` rejects the crates outright in CI.
2. `clippy.toml`'s `disallowed-types` covers `tokio::sync::mpsc::*` and friends so even an accidental local import trips a lint.
3. `cargo run -p xtask -- check-no-channels` greps the entire source tree for `::mpsc`, `::broadcast`, `::watch`, `::oneshot` paths as a final belt-and-braces.

### Why we banned them

- **Hidden backpressure.** A bounded mpsc that fills up under load surfaces as latency in a place the producer cannot see.
  An unbounded mpsc leaks memory.
  Either failure mode is invisible at the channel's _type signature_.
- **Close semantics.** Every channel library has its own answer to "drop the receiver while the sender still holds messages".
  The bug surface multiplies with the number of channels in the architecture.
- **Debug "where did this message go?" mode.** Anyone who has chased a message through three mpscs across two tasks knows how expensive this is.
  The sans-io split makes the alternative natural and cheap to debug.

### How we replace channels

The single mechanism is a `Waker` slab keyed by `op_id` _inside the state machine_:

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

- A slab of `(PendingOpKey -> Waker)` where `PendingOpKey` is one of `Request(RequestId)` for lookups / seeks / acks-with-response, or `Send(ProducerHandle, SequenceId)` for publishes.
- A slab of `(PendingOpKey -> OpOutcome)` where the matching response is parked until the future polls it.

When `handle_bytes` decodes a `CommandSendReceipt`, it stores the `OpOutcome::SendReceipt` in the outcome slab keyed by `(producer_handle, sequence_id)`, then calls `Waker::wake()` on whatever the producer future registered.
The future polls again, locks the connection, finds the outcome, and resolves.

This is the cancer-free equivalent of a `oneshot<Result<MessageId, SendError>>`.
The "channel" is the slab entry; the "send" is the state machine populating it; the "receive" is the future polling it.
No backpressure surface, no orphaned senders, no `Drop` glue.

The diagram above shows the _outcome_ side — the path the broker-response → future-wake takes through the global `inner` lock.
The _send_ side has been lifted off that global lock per [ADR-0038](specs/adr/0038-split-connection-mutex.md): `Producer::send` calls `magnetar_proto::ProducerSlot::queue_send` (per-slot mutex only), and the driver merges per-slot staged frames into the connection-wide outbound buffer through `Connection::poll_transmit` on its next tick.
The lock-ordering rule **global → per-slot, never the reverse** is documented in the "Concurrency primitives" table above.

The driver-to-driver communication path is _also_ not a channel — it is a single-cell `tokio::sync::Notify` (the driver wakes on `shared.driver_waker.notified()`).
`Notify` is permitted because it has no queue and no payload — it is an async condvar, not a channel.
If even `Notify` feels too channel-flavoured, a `parking_lot::Condvar + Mutex<bool>` is the documented fallback.

**Enroll-before-drain wakeup discipline.** Every `Notify` the driver pulses with `notify_waiters()` — `driver_waker`, `event_waker`, `topic_list_notify`, `replicated_subscription_marker_notify`, `scalable_notify` — stores **no permit**: it only wakes waiters enrolled at the instant it fires.
So every accessor that parks on one of these (`Client::await_reconnect_or_terminal`, `next_topic_list_change`, `next_replicated_subscription_marker`, `next_scalable_event`) MUST arm its `Notified` future — create it and call `enable()` — **before** it drains the buffer and re-checks `is_closed()`, then `await` the pre-armed future.
The reverse (drain → check → `notified().await`) leaves a window in which the driver can push an item and `notify_waiters()` between the empty-check and the (too-late) enrollment, losing the wakeup and hanging the accessor forever.
This is enforced 1:1 across both engines; the marker accessor's missing enrollment was the latent §5.1 lost-wakeup race (the same shape already fixed for the subscribe-readiness waiter).

The same rule binds hand-written `Future` impls, which enroll by **owning** a `Notified` across polls rather than by `enable()`-ing a local one: `EventWaitFut` (`ProducerReady` / `SubscribeAcked`) and `ConnectedFut` (the `wait_connected` handshake wait) both store an `OwnedNotified` on `event_waker` and poll it **before** inspecting connection state.
Enrolling from a spawned helper task instead — `tokio::spawn(async move { waker.notified().await; … })` — does NOT satisfy this: the helper enrolls whenever the runtime happens to schedule it, so any pulse that lands first is lost.
That was the tokio engine's handshake hang: `ConnectedFut` parked via a spawned helper, and because a freshly dialled connection is silent once `CONNECTED` lands, one missed pulse stranded the wait for the whole `operation_timeout` and surfaced at the caller as `producer target resolution exceeded operation_timeout`.
The moonpool engine has no such window — `handshake_plain` completes the handshake inline, before the driver task is spawned.

### Reference

The pattern is the same one [`quinn`] _would_ be using if it didn't ship its own bespoke `tokio::sync::mpsc` wrapper for legacy reasons — `quinn-proto` itself is sans-io and channel-free; the channels are only in the engine glue.

[`quinn`]: https://github.com/quinn-rs/quinn

---

## Concurrency primitives we _do_ use

| Primitive                                                    | Where                                                                                                                                                                           | Why                                                                                                                                                                              |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `parking_lot::Mutex<Connection>`                             | `ConnectionShared.inner`                                                                                                                                                        | Connection-wide state — frame buffers, handshake, `pending_requests`, the events / outcomes / wakers slabs, the handle registry. Critical sections are short and never `.await`. |
| `parking_lot::Mutex<ProducerState>` / `Mutex<ConsumerState>` | `magnetar_proto::{ProducerSlot,ConsumerSlot}.state`                                                                                                                             | Per-handle hot state — the `Producer::send` hot path takes ONLY this lock, never the connection-wide one ([ADR-0038](specs/adr/0038-split-connection-mutex.md)).                 |
| `parking_lot::Mutex<VecDeque<TopicListChange>>`              | `ConnectionShared.topic_list_changes`                                                                                                                                           | PIP-145 topic-list-watcher delta buffer surfaced to user futures.                                                                                                                |
| `parking_lot::RwLock`                                        | tracker internals                                                                                                                                                               | Pure read paths under load.                                                                                                                                                      |
| `tokio::sync::Notify`                                        | `ConnectionShared.driver_waker`, `topic_list_notify`                                                                                                                            | Single-cell async wake-up. Not a channel.                                                                                                                                        |
| `std::sync::atomic::*`                                       | stats + state flags                                                                                                                                                             | Lock-free counters.                                                                                                                                                              |
| `core::task::Waker` slab                                     | `magnetar-proto::Connection.pending_ops`                                                                                                                                        | Future completion.                                                                                                                                                               |
| `tokio::select!` / `moonpool_core::select!`                  | tokio / Moonpool driver loops respectively                                                                                                                                      | Control-flow multiplexing. Moonpool's fair branch order is seed-driven; `biased;` preserves explicit protocol priority. Not a channel.                                           |
| `Arc<T>`                                                     | `ConnectionShared`, `Arc<ProducerSlot>` / `Arc<ConsumerSlot>` on `Producer` / `Consumer`, `MessageEncryptor`, `MessageDecryptor`, `AuthProvider`, `MessageRouter`, interceptors | Cheap clone-and-share.                                                                                                                                                           |
| `arc_swap::ArcSwap`                                          | rare config-rotation slots                                                                                                                                                      | Lock-free swap.                                                                                                                                                                  |
| `slab::Slab`                                                 | per-future Waker keyspace                                                                                                                                                       | O(1) insertion + removal.                                                                                                                                                        |

Anything not on this list either has a justification in [GUIDELINES.md](GUIDELINES.md) or is a candidate for removal.

### Lock-ordering invariant (ADR-0038)

The two layers — global Connection mutex and per-slot mutex — are acquired in **strict global → per-slot order**:

1. **Global → per-slot is safe** and is the only path the codebase takes.
   `Connection`-level methods that need to touch per-handle state look up the slot under `&mut self`, then take `slot.state.lock()` briefly.
2. **Per-slot → global is FORBIDDEN.** A holder of `slot.state.lock()` that needs Connection-level state MUST release the slot lock first.
   Wrong-order acquisition deadlocks under contention.

The producer-send hot path (`Producer::send` → `ProducerSlot::queue_send`) takes only the per-slot mutex; the driver merges per-slot staged frames into the connection-wide outbound buffer under the global lock via `poll_transmit` (`drain_producer_outbound`).
The reconnect rebuild path (`Connection::rebuild_producers` / `rebuild_consumers`) takes the global lock and each per-slot lock in canonical order.

#### Schema — state layout (where each field lives)

```text
                       Arc<ConnectionShared>            (cheap clone)
                       ─────────────────────
                                │
                                │  inner: parking_lot::Mutex<Connection>   ←──── global mutex
                                ▼
       ┌──────────────────────────────────────────────────────────────────────┐
       │  Connection                                                          │
       │  ─────────                                                           │
       │   • state (HandshakeState)                                           │
       │   • inbound / outbound: BytesMut         (connection-wide buffers)   │
       │   • events: VecDeque<ConnectionEvent>                                │
       │   • outcomes / wakers slabs               (PendingOpKey → ...)       │
       │   • pending_requests: HashMap<RequestId, Kind>                       │
       │   • producers: HashMap<ProducerHandle, Arc<ProducerSlot>> ─────┐     │
       │   • consumers: HashMap<ConsumerHandle, Arc<ConsumerSlot>> ─┐   │     │
       └────────────────────────────────────────────────────────────┼───┼─────┘
                                                                    │   │
                          Arc clone (also held on Consumer/Producer ┘   │
                           runtime handle, cloned at create-time)       │
                                                                        │
                                                                        ▼
        ┌─────────────────────────────────────┐    ┌─────────────────────────────────────┐
        │  ProducerSlot                       │    │  ConsumerSlot                       │
        │  ────────────                       │    │  ────────────                       │
        │   identity: ProducerIdentity        │    │   identity: ConsumerIdentity        │
        │     (frozen — lock-free read)       │    │     (frozen — lock-free read)       │
        │     ─ handle, topic, access_mode    │    │     ─ handle, topic, subscription   │
        │                                     │    │                                     │
        │   state: parking_lot::Mutex<…>  ←── │    │   state: parking_lot::Mutex<…>  ←── │ per-slot mutex
        │     ─ pending: VecDeque<OpSend>     │    │     ─ queue: VecDeque<…>            │
        │     ─ batch: BatchContainer         │    │     ─ receive_wakers: Slab<Waker>   │
        │     ─ outbound: VecDeque<Frame>     │    │    ─ granted_permits, permit_balance│
        │     ─ name, epoch, stats, ...       │    │     ─ ack_tracker, paused, ...      │
        └─────────────────────────────────────┘    └─────────────────────────────────────┘
```

#### Schema — producer-send hot path (lock-free w.r.t. the global mutex)

```text
   Producer::send                                         driver task
   ──────────────                                         ───────────

       │ slot.queue_send(msg, publish_time_ms, now)
       │   takes  ──►  slot.state.lock()
       │   work:  ──►   • assigns SequenceId
       │                • appends to state.pending
       │                • stages frame in state.outbound
       │   drops  ──►  slot.state.lock()
       │
       │ shared.driver_waker.notify_one()  ──notify──►   ◯ wakes
       │                                                  │
       ▼                                                  ▼
   returns SendFut                                  takes  ──►  inner.lock()
   (futures wait on the                             work :  ──►   conn.poll_transmit()
    per-op Waker slab inside                                       └─ drain_producer_outbound():
    Connection — drained                                              for each slot:
    when broker replies)                                                slot.state.lock()        ← briefly,
                                                                        drain state.outbound       global
                                                                          into conn.outbound       lock held
                                                                        drop slot.state.lock()
                                                                       returns Bytes
                                                  drops ──►  inner.lock()
                                                  socket.write_all(&out)
```

The "no global lock on send" is the headline win: two producers on the same connection contend only on their own per-slot mutexes when each calls `Producer::send` from a different task.

#### Schema — lock-ordering rule (legal vs forbidden)

```text
LEGAL (global → per-slot)                FORBIDDEN (per-slot → global)
─────────────────────────                ─────────────────────────────
                                                          ✗
let mut conn = inner.lock();             let mut state = slot.state.lock();
let slot = conn.producer(h).cloned();    let mut conn = inner.lock();   ← DEADLOCK
drop(conn);                              //   thread A: holds slot.state, waits for inner
let mut state = slot.state.lock();       //   thread B: holds inner,     waits for slot.state
// ... per-slot work ...                 //
drop(state);                             // Wrong-order acquisition under
                                         // contention -> cyclic wait.
```

The contract is enforced by code review + the `lock_ordering_global_then_per_slot_does_not_deadlock` smoke test in [`crates/magnetar-proto/tests/slot_hot_path.rs`](crates/magnetar-proto/tests/slot_hot_path.rs).

### Producer close semantics (ADR-0057)

Two close paths exist, with different reliability contracts.

- **Explicit `Producer::close().await`** — the reliable path.
  Enqueues `CommandCloseProducer` via `Connection::close_producer`, wakes the driver, and awaits the broker ack through a `RequestFut` that drains the recorded `OpOutcome` with `take_outcome`.
- **Last-clone drop (RAII)** — the safety net.
  Every `Producer` clone shares one `Arc<ProducerCloseGuard>`; when the last clone drops, the guard fires `Connection::close_producer_forget` — encode + wake, never await.
  The proto layer registers the request as `ProducerCloseForgotten` and consumes the broker ack in-place: recording an `OpOutcome` would leak one permanent entry per dropped producer, since no future will ever drain it.
  A broker rejection surfaces as a structured `warn!` (ADR-0054) instead of being silently swallowed.

The guard is the one `Drop` impl that touches both lock tiers: it probes `slot.state.lock().closed`, **releases** that guard, then takes the global connection mutex — sequential acquisition, never nested, so the ADR-0038 global→per-slot order is respected.
The `closed`-flag dedup covers only a preceding completed client-initiated close; broker-initiated detach keeps `closed = false` on purpose (re-attach on PIP-188 migration / failover), so a drop after broker detach emits one redundant — and broker-tolerated — `CloseProducer`.

### Consumer close semantics (ADR-0077)

Consumer close has the same two reliability tiers as producer close, with one guard shared by every clone of a logical consumer.

- **Explicit `Consumer::close().await`** — the reliable path.
  Enqueues `CommandCloseConsumer` via `Connection::close_consumer`, wakes the driver, awaits the broker acknowledgement, and returns broker or terminal errors to the caller.
- **Last-clone drop (RAII)** — the safety net.
  Every `Consumer` clone shares one `Arc<ConsumerCloseGuard>`; the final clone stages `Connection::close_consumer_forget` synchronously and wakes the existing driver without spawning or blocking.
  Dropping an intermediate clone only decrements the guard `Arc`, so surviving handles remain usable.

The forgotten request is registered as `ConsumerCloseForgotten`.
Broker success consumes it in place, broker error emits a bounded structured warning, and connection reset or terminal cleanup discards it without creating an `OpOutcome` that no future could drain.
If the connection is already in its terminal no-driver state, the guard stages nothing because no task remains to flush the bytes.

The guard first probes `slot.state.lock().closed`, releases that guard, and only then takes the global connection mutex, preserving ADR-0038's lock ordering.
A completed explicit close marks the slot closed and suppresses a later drop close.
The final-clone path is best-effort rather than confirmation-bearing: callers that must know the broker acknowledged resource release use explicit `close().await`.
This is Rust RAII and pulsar-rs migration parity, beyond Java abandonment semantics; it does not claim that Java garbage collection closes consumers.

### Consumer flow-permit accounting (issue #349, ADR-0082)

Each `ConsumerState` (`crates/magnetar-proto/src/consumer.rs`) carries TWO permit counters, split because they answer different questions.

- **`granted_permits: u32`** — a purely ADDITIVE record of every permit granted to the broker since the last zeroing (subscribe, reconnect reset, terminal subscribe failure, same-broker `CommandCloseConsumer`).
  Bumped at the three grant sites (`initial_flow`, `maybe_flow`, `adjust_receiver_queue`'s growth branch); never decremented by dispatch.
  Answers "how much have we told the broker it may use" — the #307 failover-reflow gate (the `ActiveConsumerChange` arm in `conn.rs`), `adjust_receiver_queue`'s want-have delta, and `Connection::initial_flow`'s once-per-attach guard (ADR-0102) all need exactly that, so all three read this field.
- **`permit_balance: u32`** — the REAL broker-side balance: `granted_permits` minus one unit per broker dispatch unit that has actually arrived.
  Incremented at the same three grant sites, by the identical delta.
  Decremented by exactly one (`saturating_sub`) per dispatch unit: once per delivered logical message in `classify_and_queue` (a plain message, each batch member, or the chunk-completing logical message — unconditionally across the queued and dead-lettered branches, since the broker already spent the permit either way), once per incomplete chunk buffered in `deliver`, and once per PIP-33 marker in `record_marker_consumed`.
  Force-zeroed everywhere `granted_permits` is zeroed, so the two never drift apart at a churn boundary.

`flow_stats` feeds `permit_balance` — not `granted_permits` — into `FlowStats::available_permits`, the signal [`Auto::adjust`](../crates/magnetar-proto/src/receiver_queue.rs) uses to detect starvation.
Before this split, the single additive field never registered a genuine dispatch-driven starvation (issue #349): `Auto` never scaled up under real load.

`adjust_receiver_queue` also gates on `granted_permits == 0` before computing anything: a zero grant mirror only occurs right after a reset / terminal-failure / same-broker `CloseConsumer` zeroing — a churn window, not load starvation — so the tick is skipped entirely rather than let the policy misread it and grow (or emit a `CommandFlow` the broker would drop against a torn-down consumer id).

`Connection::consumer_available_permits()` — and the façade's `Consumer::available_permits()` chain on both engines — reads `permit_balance`, matching Java's `ConsumerBase#getAvailablePermits`.
ADR-0082 originally left it on the additive `granted_permits`; issue #414 is what that cost.

A counter that never moves under dispatch cannot distinguish a healthy consumer from one whose broker-side dispatcher has wedged, so the public accessor now reports the balance that does move and `granted_permits` remains available for the three internal callers that genuinely want the cumulative grant.
See [ADR-0082](specs/adr/0082-consumer-permit-balance-split.md) as amended by [ADR-0101](specs/adr/0101-consumer-stall-detection-and-in-place-recovery.md) and [ADR-0102](specs/adr/0102-grant-the-initial-consumer-flow-once-per-attach.md).

`Connection::initial_flow` grants at most ONCE per attach ([ADR-0102](specs/adr/0102-grant-the-initial-consumer-flow-once-per-attach.md)): it emits a `CommandFlow` only when `ConsumerState::initial_grant_due` is set — a `CommandSubscribe` has gone out since the last grant, so the broker's freshly (re-)created dispatcher slot starts at zero permits — or when `granted_permits == 0`, the churn boundary the #307 re-arm exists for.
Two sites can each decide a consumer needs its initial grant, and on a fresh `Exclusive` / `Failover` subscribe a real broker makes both fire: it sends `CommandActiveConsumerChange { is_active: true }` in the same write as the subscribe `Success`, so the #307 re-arm runs inside `handle_bytes` while the engine's own post-ack `initial_flow` is still parked on the resolving subscribe future.
Both used to grant, and the broker held `2 × receiver_queue_size` for a consumer whose mirrors recorded one (issue #427, measured 32 against a configured 16).
Whichever caller now arrives first issues the grant; the other is a no-op, so the order is unobservable on the wire.
`initial_grant_due` is not the same question as `granted_permits == 0`: a post-seek resubscribe re-attaches without zeroing the additive mirror, so only the flag can tell that attach apart from a consumer that is already fed.

### Per-consumer stall watchdog (issue #414, ADR-0101)

The connection keepalive of [ADR-0058](specs/adr/0058-keepalive-watchdog-progress-based.md) refreshes ONE connection-wide `last_activity` baseline off every decoded inbound frame, so a broker whose dispatcher has wedged for a single subscription — still answering `PING` with `PONG`, still serving every other subscription — never ages it.
`ConsumerState` therefore carries its own progress-based watchdog, the same shape scoped to one consumer:

- **`dispatch_units_received: u64`** — a monotonic progress mark bumped by `record_dispatch_unit`, the single helper that also decrements `permit_balance`. One call, so a future dispatch site cannot update one and forget the other. It is a counter, not a timestamp, so no dispatch site needed a `now` parameter ([ADR-0011](specs/adr/0011-clock-injection-sans-io.md)).
- **`stall_watch: Option<StallWatch>`** — the open silence window: the progress mark latched when it opened, the injected instant it opened at, and a `reported` latch that makes one stall episode emit exactly one event. `poll_stall(window, now)` advances and closes it; `next_stall_deadline` surfaces the deadline through `Connection::poll_timeout` so the driver wakes for the sweep deterministically rather than opportunistically. `Connection::initial_flow` opens it at grant time (`arm_stall_watch(now)`, beside the existing `arm_adjust_clock(now)`): that is the only grant site with an injected clock, and it is where a wedge begins, so detection takes the configured window rather than the window plus however long the next unrelated deadline takes to produce a first sweep.

A consumer is a stall candidate only while it holds un-spent permits over an EMPTY queue in a dispatch-eligible state — the same eligibility set the #307 failover re-arm gate uses, since every state outside it (paused, mid-seek, end-of-topic, terminal, mid-re-attach, or simply a non-empty queue) explains the silence without a broker fault.
The window is dropped when candidacy ends and at every grant site, so a fresh grant gets a fresh window.

The whole mechanism is gated on `ConnectionConfig::consumer_stall_timeout`, which defaults to `None`: an armed deadline perturbs the moonpool engine's simulated wake schedule even when it never fires, and there is no Java counterpart to inherit a parity default from.
Its only effect is one `warn!` plus one `ConnectionEvent::ConsumerStalled`; recovery is the caller's explicit `Connection::resubscribe_consumer_in_place` (issue #307's same-broker re-attach, made callable), escalating to an operator-side `topics unload`.
See [`docs/consumer-stall-recovery.md`](docs/consumer-stall-recovery.md).

---

## The driver loop

One driver task per connection.
Owns the I/O resources (TCP or TLS stream), the per-connection read buffer, and the `select!` loop that shuttles bytes between the state machine and the network.

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
        │  (1) Lock state if no write tail is pending. Drain outbound bytes into  │
        │      a driver-owned pending-write queue. Read deadline / closing flag / │
        │      operation_timeout (ADR-0083's write-deadline source). Drop lock.   │
        └──────────────────────────────────────┬──────────────────────────────────┘
                                               │
                                ┌──────────────▼──────────────┐
                                │  (2) if closing, queue empty │
                                │      AND no TLS ciphertext   │
                                │      residue: shutdown now   │
                                └──────────────┬──────────────┘
                                               │
                                ┌──────────────▼──────────────┐
                                │  (3) runtime select! biased  │
                                └──────────────┬──────────────┘
                                               │
      ┌─────────────────┬───────────────────────┼───────────────────────┬─────────────────┐
      │                 │                       │                       │
      ▼                 ▼                       ▼                       ▼
┌───────────────┐┌──────────────────┐┌───────────────────────────┐┌─────────────────────┐
│ read_half     ││ shared.driver_   ││ write_one_budget(…),       ││ runtime timer        │
│  .read_buf(…) ││  waker           ││   IF write_has_work         ││  deadline on tick    │
│  (polled       ││  .notified()     ││   (ADR-0083): write up to  ││  -> handle_timeout   │
│  FIRST —       ││  (user enqueued  ││   256 KiB, bounded by      ││  (now)                │
│  receipt       ││  a send/ack/     ││   operation_timeout; Err   ││                       │
│  fairness)     ││  etc.); loop     ││   -> mark_disconnected +   ││                       │
│  on Ok(0) ->   ││  continues       ││   propagate (redial);      ││                       │
│  PeerClosed    ││                  ││   Ok + queue drained +     ││                       │
│  on Ok(n) ->   ││                  ││   closing -> shutdown      ││                       │
│  lock +        ││                  ││                             ││                       │
│  handle_bytes  ││                  ││                             ││                       │
│  (now, &buf)   ││                  ││                             ││                       │
│  then drain    ││                  ││                             ││                       │
│  events        ││                  ││                             ││                       │
└───────────────┘└──────────────────┘└───────────────────────────┘└─────────────────────┘
                                               │
                                ┌──────────────▼──────────────┐
                                │     back to (1)             │
                                └─────────────────────────────┘
```

`read_half` / `write_half` are produced ONCE, at loop entry, by splitting the connected socket (`tokio::io::split` on tokio; `Transport::into_split` on moonpool — see "TLS byte-pipe" below for why moonpool's split needs a shared adapter) and held as separate local bindings for the life of the loop — not re-split per iteration.

### Read fairness and the write deadline (ADR-0070, ADR-0074, ADR-0083)

The tokio driver uses `tokio::select!`; the Moonpool driver uses `moonpool_core::select!`, whose fair branch offset is derived from the simulation seed.
Both keep `biased;` because ADR-0070 requires a fixed **inbound-read-first** priority before the `driver_waker` arm.
An unbiased Moonpool selection would remain reproducible, but it would rotate that protocol priority instead of guaranteeing receipt fairness.
Every `Producer::send` pulses `driver_waker.notify_one()`, so under sustained publish load a waker permit is almost always pending on loop entry; polling the waker arm first would let the outbound path starve inbound `CommandSendReceipt` reads, inflating `send→ack` latency under load while the broker acks in milliseconds (issue #303).
The read arm is cancel-safe — bytes land in the persistent `read_buf` and are consumed via `split()` only after the arm wins — so the reorder drops no bytes.

Through 2026-07, the write ran unconditionally at the top of every loop iteration (ADR-0074 bounded it to 256 KiB per turn after issue #319, with a fixed continuation arm keeping it flushing whenever bytes remained), and ADR-0070's fairness argument leaned on that: "the outbound path is not starved by giving reads priority, because `poll_transmit` + `write_all` run at the TOP of every loop iteration regardless of which arm wins."
Issue #370 showed that premise fails against a peer that accepts the connection and then simply stops draining its receive window: the write parks inside that unconditional top-of-loop step, which blocks the ENTIRE loop — not just the write path — starving the read arm, the `driver_waker` arm, and (critically) the timer arm that drives `Connection::handle_timeout` (the keepalive watchdog, the `send_timeout` sweep, and the `ack_response_timeout` backstop).
`mark_disconnected()` was never reached, so `is_connected()` kept reporting `true` on a functionally dead connection.

**ADR-0083** (amends ADR-0070 and ADR-0074) makes the write its own `select!` arm — third in order, after read and the waker, before the timer — gated by `write_has_work` so an idle connection never polls it.
Two independent bounds keep it from starving anything: `DRIVER_WRITE_BUDGET_BYTES` (256 KiB, unchanged from ADR-0074) caps how much one arm win writes before yielding back to read, and `Connection::operation_timeout()` (30 s default — **not** `keepalive_interval`, which only detects read-side silence and would never trip against a peer that keeps ACKing pings while refusing to drain writes) caps how long a single win may block on a peer that never drains.
The deadline is anchored to a fixed `Instant` computed once, outside the `select!`, when a logical write first has work, and held fixed while it continues across iterations — re-arming it fresh inside the per-iteration arm expression would silently reset to a full budget every time an unrelated arm won a round, so a stalled write racing ordinary background traffic on the same connection would never accumulate real elapsed time toward its own deadline.
Expiry maps to `io::ErrorKind::TimedOut` and routes through the exact same `mark_disconnected()` + `Err` branch every other write failure already takes, so the supervisor redials unchanged.

Making the write cancellable (droppable mid-poll, routine once it races other arms) required a prerequisite rewrite: both engines' `write_budgeted` now issue single-poll writes and commit `front_offset` synchronously right after each `Ready(n)`, before any further `.await`, so a cancelled write never re-sends bytes the kernel already accepted nor silently drops bytes that were popped out of the queue ahead of the actual I/O (moonpool's old eager `pop_budgeted` detach did exactly that).
Moonpool's TLS arm additionally gained a resumable `pending_ciphertext` queue between the adapter and the wire — encryption capture (`push_plaintext` → `step` → drain into the queue) is fully synchronous, so it can never itself be cancelled mid-way, and the read half appends any protocol-mandated ciphertext its own decrypt step produces (e.g. a TLS 1.3 `KeyUpdate` ack) into the SAME queue rather than stranding it on an otherwise write-idle connection.

### Lock discipline

Every interaction with `Connection` happens inside a `parking_lot::Mutex` critical section.
Critical sections are short — they **never `.await`**. The write / flush calls happen _outside_ the lock so user futures can keep enqueuing while the driver holds the network handle.

### Event dispatch

`handle_bytes` is the inbound entry.
As frames are decoded, the state machine populates the outcome slab and calls `Waker::wake()` on whatever user future is waiting.
After the lock drops, the driver pulls semantic events via `poll_event()` and reacts to the variants that the runtime layer must handle:

- `ConnectionEvent::AuthChallenge { method, challenge }` — driver consults the configured `AuthProvider`, asks it for a fresh blob via `respond_to_challenge`, and submits it via `submit_auth_response` (PIP-30 / PIP-292).
  The same hook carries SASL Kerberos / GSSAPI continuation tokens: `magnetar_auth_sasl::SaslKerberos` forwards each challenge into its wrapped `GssapiClient` so the GSSAPI initiate loop runs naturally over the existing trait surface (no new `SaslMechanism` trait was needed; see [ADR-0029](specs/adr/0029-sasl-kerberos-gssapi-scope.md)).
- `ConnectionEvent::TopicListChanged { added, removed }` — driver pushes the delta into `ConnectionShared.topic_list_changes` and wakes `topic_list_notify` (PIP-145).
- `ConnectionEvent::ReplicatedSubscriptionMarkerObserved { handle, marker }` — driver pushes the observation into `ConnectionShared.replicated_subscription_markers` and wakes `replicated_subscription_marker_notify` (PIP-33 / ADR-0034).
  The marker is filtered off the user-visible message stream upstream in the `magnetar-proto` receive path so it never reaches `Consumer::receive`.
- `ConnectionEvent::ChecksumMismatch { … }` and the **diagnostic** `ConnectionEvent::LookupResponse` carrying `LookupOutcome::Redirected` — events proto already logs at the point of detection ([ADR-0054](specs/adr/0054-logging-policy.md) single-owner rule); both engines admit them to the drain predicate and consume them **silently**, which stops them accumulating unbounded in the proto event queue.
  Note the redirect rides two separate channels: this `poll_event()` event is purely diagnostic, while the **actionable** `Redirected` outcome lands in the `outcomes` slot (keyed `PendingOpKey::Request`) and wakes the lookup future so the engine can dial the redirect target (see "lookup redirect dialing" below).

The `MessageReceivedFromShadow` variant (PIP-180 / ADR-0033) is emitted in place of `Message` for shadow-topic consumers; user-facing futures pick it up directly via the same Waker slab as `Message`, so the driver does not need to special-case it.

Every other event has already been turned into a future-completion via the Waker slab inside the state machine; the driver does not need to touch it.

### Supervised reconnect

When `driver_loop_inner` returns (the socket errored or the peer closed), the outer `supervised_driver_loop` decides whether to retry.
The supervisor:

1. **Records the disconnect** via `Connection::mark_disconnected(now, wall_now)` so `Producer::last_disconnected_timestamp` and the consumer stats are correct.
2. **Resets the state machine** with `Connection::reset()` — this snaps the handshake back to `Uninitialized`, bumps the `session_epoch`, drains the pending-op slabs, and **accumulates** in-flight publish snapshots into `Connection::in_flight_publish_snapshots` (append, never clear).
   The snapshot is `rebuild_producers`'s single consumer, so multiple reset cycles within a single rebuild are safe.
   User-facing send futures stay `Pending`; the snapshot carries enough state to replay them.
   Producers / consumers see `is_connected() = false` but stay live.
3. **Gates on `is_user_closed()`** — only the explicit `Closing` / `Closed` states stop the supervisor.
   The transport-drop state (`Failed`, set by `mark_disconnected`) does NOT count as user-closed, so a TCP drop falls into the backoff / redial path instead of returning.
   This is the difference between "broker went away" and "user called `.close()`".
4. **Backs off** with a small exponential schedule capped by `ReconnectConfig::max_backoff` (jittered by the engine clock — under moonpool-sim this is deterministic per seed).
5. **Reconnects** through the same `Transport::connect` path used at client init (re-resolving the broker URL via the configured `ServiceUrlProvider` on every attempt — this is where PIP-121 plugs in).
6. **Rebuilds producers and consumers** via `Connection::rebuild_producers(now)` and `Connection::rebuild_consumers(now)`.
   Each helper re-emits `CommandProducer` / `CommandSubscribe` for every still-open handle, stamps the new `session_epoch`, and replays the in-flight `OpSend` cached wire frames once the broker acks the producer.
   Consumers replay `initial_flow` followed by an explicit `CommandRedeliverUnacknowledgedMessages` after `SubscribeAcked` (the broker silently drops `CommandFlow` for an unknown `consumer_id`, so the Java `ConsumerImpl#reconnectLater` ordering is mandatory).
   User-facing futures stay registered; they get woken when the broker re-issues the producer/consumer IDs.

The supervisor never retries past `ReconnectConfig::max_attempts`; on exhaustion it propagates the last `EngineError::Io` upward and the `Client` is closed.
The give-up budget counts the FULL dial+handshake cycle, not just TCP-dial failures ([ADR-0061](specs/adr/0061-supervisor-give-up-counts-handshake-failures.md)): the `give_up_attempts` counter is hoisted OUTSIDE the outer supervisor loop and gated by the sans-io `SupervisorConfig::should_give_up(attempts)` helper, so a connection that dials successfully (TCP accept) but then fails the Pulsar handshake — the docker-proxy / LB storm where the backend is down — counts against `max_attempts` instead of resetting it.
The counter resets to 0 only when `should_reset_backoff(socket_alive)` is true (a socket that survived `drop_grace`), the SAME stability gate the `Backoff` schedule resets on — so give-up-reset and backoff-reset share one definition of a stable reconnect.
The default `max_attempts = None` keeps retrying forever (Java parity).
On a successful dial the supervisor logs `"supervisor: TCP connected; handshaking"` at `info!`; the TRUE reconnect-success `info!` (`"supervisor: reconnected to broker; handshake complete, …"`) fires only after the handshake completes, so a TCP accept behind a down backend is never mislabelled as a reconnect.

#### Broker-operation retry: provisional setup and established reattachment

ADR-0080 defines one configurable `OperationRetryConfig` for lookup, partition metadata, producer-open, and subscribe, independent from transport `SupervisorConfig`.
All four operations retry `MetadataError`, `PersistenceError`, `ServiceNotReady`, and `TooManyRequests`.
Producer-open additionally retries both producer-quota variants and `ProducerBusy`; subscribe additionally retries `ConsumerBusy`.
`ProducerBusy` outside producer-open, `ConsumerBusy` outside subscribe, `TopicNotFound`, authentication and authorization failures, schema failures, fencing, termination, and unknown codes remain terminal. A common retryable case is `NamespaceBundleNotServed`, emitted as `ServiceNotReady` with the text `"Please redo the lookup"`.

Retry ownership depends on whether the handle has ever attached:

1. **Provisional setup is client-owned and routing-aware.** Before the first `ProducerSuccess` or subscribe acknowledgement, `Connection::handle_command_error` removes the provisional handle and emits `ProducerOpenFailed` or `SubscribeFailed`.
   The runtime client backs off, re-runs lookup and target resolution, and creates a fresh provisional handle on the resolved connection, so a redirect or ownership move can route the retry to a different broker.
2. **Established reattachment is driver-owned and connection-local.** After a handle has attached at least once, a retryable reattachment failure retains its state and emits `ProducerOpenFailedTransient` or `SubscribeFailedTransient`.
   The driver sleeps, runs `lookup_then(topic)`, and invokes `retry_producer_open_if_current(handle, failed_request_id)` or `retry_consumer_subscribe_if_current(handle, failed_request_id)` for that established handle; the failed request id makes a delayed retry a no-op when a newer reconnect rebuild or retry generation has already superseded it.
   Producer and consumer retry lookup legs run only on a connected session and are awakened when their active request generation is replaced, so a blackholed lookup cannot outlive the generation that authorized it.
   Producer sends remain staged until `ProducerSuccess`; consumer flow remains gated until the subscribe acknowledgement.
   A driver-owned consumer reattachment acknowledgement updates durable attachment state and releases gated flow without emitting an unowned `SubscribeAcked` event.
   Producer and consumer attachment state record the active wire `RequestId`; only that generation may accept a success or transient failure, terminalize the handle after lookup failure, or emit another retry.
   A terminal broker error on the current established generation drains producer sends or marks the consumer terminal and wakes every parked operation before removing replay state.
   Cancellation removes the request correlation, any landed outcome, and any queued success, failure, or broker-close attachment event; a later broker reply for that canceled request is ignored.
   A user-owned subscribe or seek additionally keeps one stable logical waiter token while retry and reconnect replace its active wire `RequestId`; only the current active request can complete that token, so an older same-handle acknowledgement cannot satisfy the waiter or release flow.
   Completion remains durable across reset but is consumed only on a connected rebuilt session, and dropping the waiter transfers the active or next rebuilt subscribe to flow ownership.
3. **Retry budgets stay distinct.** Provisional attachment retries consume the caller operation's attachment counter and shared deadline.
   Established lifecycle reattachment uses an independent per-handle counter under the same configured policy and does not consume the completed setup operation's count or deadline.

Tokio performs provisional and established retry sleeps on `tokio::time`.
Moonpool routes both through the injected `TimeProvider`, preserving deterministic virtual-time behavior.
One `OperationDeadline` context is allocated at the public setup entry and reborrowed across partition metadata, PIP-145 topic-list snapshots, lookup, redirect routing, retry sleeps, attachment, and every child of a composite builder.
Every retryable broker response updates the same latest-error slot, including responses from an intermediate stage or an earlier composite child.
If a later deadline fires, the runtime returns that newest broker code and message instead of replacing it with a generic timeout.

#### Anti-thrash policy (opt-in, ADR-0028)

Some broker conditions cause a different pathology: the broker **accepts** `CommandProducer` / `CommandSubscribe`, then drops the TCP connection within a few milliseconds. magnetar's retry path treats each drop as a transient error and re-attaches, which feeds the cascade.
The observed trigger is post-restart bundle-ownership churn on `apachepulsar/pulsar:4.0.4` (Pulsar PR #14467 + #13428 + #12846 — `ServerCnx#handleProducer` ↔ `AbstractTopic#addProducer` race, amplified by the standalone-mode ZK session timeout).

[`magnetar-proto::AntiThrashState`](crates/magnetar-proto/src/anti_thrash.rs) is a per-`Connection` bounded ring that records each re-attach outcome (`ReAttachOk { handle }`) and the TCP-drop deltas that follow within `drop_grace`.
When `N` re-attaches inside a sliding window of `M` are all followed by `TcpDropAfterReAttach`, the state emits `ConnectionEvent::AntiThrashCooldown { until }`; the supervisor honours it by sleeping until `until` before the next `Transport::connect`.
The detector resets on any re-attach that survives `drop_grace`.

Default: **OFF** — `SupervisorConfig::anti_thrash_threshold: None`.
Recommended opt-in values from [ADR-0028](specs/adr/0028-supervised-reconnect-anti-thrash-policy.md): `(N = 5, M = 2 s, K = 50 ms, cooldown = 30 s)`.
The `magnetar-runtime-moonpool` chaos pack ships a `DropsTcpAfterCreate { delay_ms }` `BrokerWorkload` variant so the behaviour is exercised under deterministic seeds (see [`tests/sim_chaos.rs`](crates/magnetar-runtime-moonpool/tests/sim_chaos.rs)).

### Source

[`crates/magnetar-runtime-tokio/src/driver.rs`](crates/magnetar-runtime-tokio/src/driver.rs) — `driver_loop_inner` + `supervised_driver_loop` total ~425 lines.

---

## Protocol state machine (`magnetar-proto`)

`magnetar-proto::Connection` is the central state machine.
Top-level types live at [`crates/magnetar-proto/src/conn.rs`].

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

`Connection::state()` reports the live state.
Source: [`HandshakeState` enum at `crates/magnetar-proto/src/conn_types.rs:25`](crates/magnetar-proto/src/conn_types.rs).

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

The slab maps `PendingOpKey -> Waker` + `PendingOpKey -> OpOutcome`.
A future registers its waker via `Connection::register_waker(key, waker)` and consumes the outcome via `Connection::take_outcome(key)`.

**Ack deadline (issue #346).** `PendingRequestKind::Ack` (the `Request(RequestId)` variant's payload when the pending op is a `CommandAck`) carries an `enqueued_at: Instant` alongside the `ConsumerHandle`, stamped by `Connection::ack`'s injected `now` parameter (ADR-0011).
Two independent mechanisms resolve a pending ack that would otherwise park its `RequestFut` forever:

1. **Same-broker `CloseConsumer` orphan sweep** — the close-handler's same-broker arm (`assigned_broker_service_url = None`, the #307 root cause) collects every `PendingRequestKind::Ack` entry for the torn-down handle and fails it immediately (`OpOutcome::Error{code: -1, message: "ack orphaned by broker consumer close"}`) before `resubscribe_consumer_after_broker_close` re-attaches a fresh consumer id — the broker will never answer a `CommandAck` addressed to a consumer id it has already forgotten.
2. **`ack_response_timeout` backstop** — a connection-wide `ConnectionConfig::ack_response_timeout: Option<Duration>` (default `Some(30s)`, mirroring the `send_timeout` Java-parity default; `None` disables it) bounds every pending ack regardless of cause.
   `poll_timeout` folds `enqueued_at + ack_response_timeout` over every `PendingRequestKind::Ack` entry into the driver's next wake-up; `handle_timeout` reaps any that crossed the deadline with the same `Error{code: -1, ..}` shape (`message: "ack timeout"`).
   Skipped entirely when the knob is `None`, so a disabled backstop contributes no deadline and no spurious driver wakeups (load-bearing for moonpool determinism).

Both mechanisms mirror the pre-existing per-producer `send_timeout` sweep in shape (two-phase collect-then-mutate over the pending map, then wake + record the outcome), and both also bump `ConsumerState::total_acks_failed` and push a `ConnectionEvent::AckResponse { request_id: Some(rid), result: Err(..) }` so the failure is observable through the same seams a real broker-rejected ack would use.

### Producer / consumer states

`ProducerState` lives at [`crates/magnetar-proto/src/producer.rs`](crates/magnetar-proto/src/producer.rs).
`ConsumerState` lives at [`crates/magnetar-proto/src/consumer.rs`](crates/magnetar-proto/src/consumer.rs).
Both are owned by the parent `Connection` and addressed by stable `ProducerHandle` / `ConsumerHandle` ids.

A `ProducerState` carries:

- `producer_id`, `producer_name`, `topic`, `schema`, `compression`, `access_mode`.
- A `BatchMessageContainer` (only when batching is enabled).
- A chunked-send slot (only when chunking is enabled — chunks-never-batched).
- The send queue (pending `SendDecision`s).
- Per-producer stats counters.

A `ConsumerState` carries:

- `consumer_id`, `consumer_name`, `subscription`, `subscription_type`, `read_compacted`, `priority_level`, `key_shared`, `dead_letter_policy`.
- The receive queue (inbound `IncomingMessage`s pending a `receive()` call).
- The optional `AckGroupingTracker`, `NegativeAcksTracker`, and `UnackedMessageTracker`.
- The PIP-54 batch-ack table (per-batch position bitset).
- The PIP-4 `crypto_failure_action`.

### Consumer flow-permit accounting

Pulsar charges one broker flow permit for every dispatched PIP-37 chunk, while Magnetar exposes the reassembled payload as one logical message.
For an accepted `N`-chunk message, each of the `N - 1` chunks that leaves reassembly incomplete immediately increments `ConsumerState::consumed_since_flow`; the chunk that completes reassembly is repaid when user code pops the resulting logical message.
This conserves all `N` broker permits without making incomplete chunks visible to consumers and remains correct when accepted chunks arrive out of numeric order.

`ConsumerState::maybe_flow` emits the accumulated debt after it reaches half the configured receiver queue.
`Connection` checks it both after inbound delivery and after a logical-message pop, staging any permit count while the per-consumer slot is locked and encoding `CommandFlow` only after releasing that slot guard.
The staging preserves the global-connection-before-per-slot lock order and prevents a chunk stream from exhausting broker permits before enough logical messages exist to cross the refill threshold.
See [ADR-0076](specs/adr/0076-conserve-flow-permits-across-chunk-reassembly.md) for the accounting decision, rejected alternatives, and five-layer verification contract.

### Trackers (`magnetar-proto/src/trackers`)

Three single-purpose tick-driven state machines:

| Tracker                 | Purpose                                                                                                                                                                | Lines | API                                             |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----- | ----------------------------------------------- |
| `AckGroupingTracker`    | Coalesce acks inside a window so we send one `CommandAck` per batch of N acks. Wired via `ConsumerBuilder::ack_group_time`.                                            | 353   | `add(...)`, `add_cumulative(...)`, `poll(now)`. |
| `NegativeAcksTracker`   | Defer redelivery commands by `delay`. Optionally drives a `MultiplierRedeliveryBackoff` over the broker-reported `redelivery_count` (PIP-37).                          | 212   | `add(...)`, `add_with_delay(...)`, `poll(now)`. |
| `UnackedMessageTracker` | Client-side ack-timeout. Forces a `RedeliverUnacknowledged` if no positive ack arrives within `timeout`. Optionally backs off per-message via the same PIP-37 backoff. | 453   | `track(msg_id)`, `ack(msg_id)`, `poll(now)`.    |

All three drive off `Connection::poll_timeout` / `handle_timeout` and emit their outputs as `Vec<TrackerAction>`.
The connection turns each action into an outbound `BaseCommand`.

### Topic-list watcher

`magnetar-proto::topic_watcher::TopicWatcherRegistry` (85 lines) carries the PIP-145 broker-driven topic-discovery state.
The connection handles `CommandWatchTopicListResponse` / `CommandTopicListUpdated` opcodes and emits `ConnectionEvent::TopicListChanged` on the event queue.
The driver forwards those to `ConnectionShared.topic_list_changes`, where `PatternConsumer::update` reconciles them against its child consumers.

### Replicated-subscription markers (`magnetar-proto/src/markers.rs`)

PIP-33 wire payload typing.
Defines the `ReplicatedSubscriptionMarkerKind` enum (`SnapshotRequest=10`, `SnapshotResponse=11`, `Snapshot=12`, `Update=13`) and the matching `ReplicatedSubscriptionMarkerDetails` sum type, plus `decode_replicated_subscription_marker(marker_type, payload)`.
Both enums are `#[non_exhaustive]` so future broker-side kinds stay additive.
The decoder returns `Ok(None)` for txn markers (kinds 20..=22) and any unknown kind — forward-compat for future broker emits.

The connection's receive-path filter at the `pb::base_command::Type::Message` arm in `conn.rs` consults this decoder before delivering to the consumer: replicated-subscription markers are diverted into `ConnectionEvent::ReplicatedSubscriptionMarkerObserved` and never reach `ConsumerState::deliver`.
The consumer's `record_marker_consumed` helper bumps `consumed_since_flow` so permit accounting stays symmetric with the broker's view (otherwise the broker's perceived permit budget would drift by one per marker).
See [ADR-0034](specs/adr/0034-pip-33-replicated-subscriptions-scope.md) and [`docs/pip-features.md#replicated-subscriptions-pip-33`](docs/pip-features.md#replicated-subscriptions-pip-33).

### Transactions (`magnetar-proto/src/txn.rs`)

Owns the transaction-coordinator client.
Pulsar transactions use four opcodes (`NEW_TXN`, `ADD_PARTITION_TO_TXN`, `ADD_SUBSCRIPTION_TO_TXN`, `END_TXN_*`) routed to the TC.
`TxnClient` carries the `TxnId` registry and surfaces a Rust `Transaction` handle.
The producer attaches `txn_id` to its publish via `OutgoingMessage::txn`, and the consumer attaches it to acks via `ack_with_txn` / `ack_cumulative_with_txn` / `ack_batch_with_txn`.

[`crates/magnetar-proto/src/conn.rs`]: crates/magnetar-proto/src/conn.rs

---

## Wire framing

Pulsar's wire format is three nested shapes plus an optional PIP-90 envelope.
Magnetar implements the codec in [`crates/magnetar-proto/src/frame.rs`](crates/magnetar-proto/src/frame.rs) (620 lines).
All multi-byte integers are big-endian.
Outer `total_size` excludes the four bytes used to encode itself.

### Command-only frame

```text
[total_size u32][cmd_size u32][BaseCommand bytes]
```

`total_size == 4 + cmd_size`.
Used for opcodes that have no message payload (`CONNECT`, `CONNECTED`, `LOOKUP`, `SEEK`, `ACK`, …).

### Payload-bearing frame (SEND / MESSAGE)

```text
[total_size u32][cmd_size u32][BaseCommand]
  [0x0e01 u16][crc32c u32]
  [metadata_size u32][MessageMetadata][payload bytes]
```

`crc32c` (Castagnoli) is computed over `[metadata_size u32 BE][metadata bytes][payload bytes]`.
Mismatch → emit `ConnectionEvent::ChecksumMismatch` and **drop the frame** (per [GUIDELINES.md §Protocol-correctness invariants point 1](GUIDELINES.md#protocol-correctness-invariants)).

### Broker-entry-metadata envelope (PIP-90)

When the namespace policy enables broker-entry metadata, dispatched messages carry a `BrokerEntryMetadata` prelude inserted by the broker:

```text
[total_size u32][cmd_size u32][BaseCommand]
  [0x0e02 u16][bem_size u32][BrokerEntryMetadata]
  [0x0e01 u16][crc32c u32][metadata_size u32][MessageMetadata][payload]
```

A producer must **never** emit `0x0e02`.
Consumers peel it before parsing the standard frame and surface it via `IncomingMessage::broker_entry_metadata` (`broker_publish_time_ms`, `broker_index`).
Source: [`crates/magnetar-proto/src/frame.rs:30-48`].

### Constants

| Constant                      | Value    | Meaning                                                |
| ----------------------------- | -------- | ------------------------------------------------------ |
| `MAGIC_CRC32C`                | `0x0e01` | Marks the start of the CRC + metadata prelude.         |
| `MAGIC_BROKER_ENTRY_METADATA` | `0x0e02` | Marks the optional PIP-90 envelope.                    |
| `MAX_FRAME_SIZE`              | `5 MiB`  | Pulsar default cap. Higher layers may enforce smaller. |

[`crates/magnetar-proto/src/frame.rs:30-48`]: crates/magnetar-proto/src/frame.rs

---

## Producer paths — batching vs chunking

Pulsar enforces a critical invariant per `ProducerImpl.java:630-654` (Apache Pulsar Java reference, external):

> **Chunked messages can never be batched.** If a message is eligible for the batch container, `totalChunks` is forced to `1`.

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

`batching_max_publish_delay` (Java `batchingMaxPublishDelay`) drives the left-hand timer.
The state machine ticks it via `poll_timeout` / `handle_timeout` so latency is bounded even if the batch never fills.

Source: [`crates/magnetar-proto/src/producer.rs`](crates/magnetar-proto/src/producer.rs).

### Sequence-id discipline

- Sequence ids are assigned inside the chunk loop (Java `ProducerImpl.java:696-704`, `:745-753` — both first-send and resend paths; Apache Pulsar Java reference, external).
- Resend reuses the original sequence id.
- `last_sequence_id` and `last_sequence_id_published` are tracked separately so the runtime can drive resend-safe dedup.
- Sequence id and request id are **monotonically non-decreasing** per connection per producer ([GUIDELINES.md §Protocol-correctness invariants point 4](GUIDELINES.md#protocol-correctness-invariants)).

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
                                   driver runtime timer fires
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

The PIP-54 ack_set bitset is stamped on per-batch ids so partial-batch acks (one position out of N) round-trip correctly.

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
        UnackedMessageTracker::remove(msg_id)   ← unconditional, mirrors the
                    │                              positive-ack path; drops the id
                    │                              from the ack-timeout tracker so
                    │                              the sweep below cannot redeliver
                    │                              the same id a second time
                    ▼
        NegativeAcksTracker::add(msg_id, now + delay)
                    │                              (skipped when no nack tracker is
                    │                               configured — the removal above
                    ▼                               still ran, then an immediate
              poll_timeout returns                  CommandRedeliverUnackedMessages
              the next nack deadline                is emitted)
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

`negative_ack` removes the nacked id from the `UnackedMessageTracker` **before** it touches the nack tracker, and does so unconditionally — on both the nack-tracker-present and nack-tracker-absent paths.
Without that removal a message that is both nacked and ack-timeout tracked is redelivered twice: once when the nack delay elapses, once when the ack-timeout window elapses.
The removal is symmetric with the positive-ack path, which already drops acked ids from both the unacked tracker and the nack tracker, and mirrors the Java client's `ConsumerImpl#negativeAcknowledge`.

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

`Consumer::reconsume_later` is the retry-letter variant: republish to the retry topic with delay + properties, then ack the original.

---

## Multi-topics fan-in

`MultiTopicsConsumer<C>` and `PatternConsumer<C>` are engine-generic façade types layered on top of N child consumers — one per subscribed topic.
`C: ConsumerApi` defaults to the tokio runtime's `Consumer`, and pass-2 (ADR-0037) lifted the impl bodies to dispatch through the trait so both engines drive the same coordinator unchanged.
The receive race is _not_ a channel — it is a `futures_util::future::select_all` over the child consumers' `receive()` futures.

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

Per-topic ack / nack / seek dispatch via the topic name attached to the incoming message.

### Dynamic membership

- `MultiTopicsConsumer::add_topic` / `remove_topic` subscribe and unsubscribe at runtime.
- `PatternConsumer` reconciles topic list deltas on demand via `update(&client)`.
  The driver pushes `TopicListChanged` deltas into `ConnectionShared.topic_list_changes`; `update()` drains the buffer, diffs against `topics()`, and spawns / closes child consumers.
  `start_auto_reconcile(client, interval)` does the same on a `tokio::time::interval` schedule.

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

`PatternConsumer::start_auto_reconcile(client, interval)` spawns a `tokio::time::interval` loop that calls `update(&client)` on every tick; the returned `JoinHandle` is used for clean shutdown.
Same pattern as the partitioned producer / consumer auto-update tickers.

---

## Runtime engines

The façade exposes `PulsarClient<E: Engine = TokioEngine>`.
`Engine` is a marker trait selecting per-engine storage; engine-specific methods live in concrete `impl PulsarClient<TokioEngine>` / `impl PulsarClient<MoonpoolEngine<P>>` blocks rather than on the trait ([ADR-0019](specs/adr/0019-engine-scope-and-moonpool-parity.md); source: [`crates/magnetar/src/engine/mod.rs`](crates/magnetar/src/engine/mod.rs)).

### Per-surface extension traits (ADR-0026 §D1)

Dependent façade surfaces lift through per-family extension traits, each implemented on the runtime's concrete `Client` / `Producer` / `Consumer` type.
`impl<E: Engine> PulsarClient<E> where E::ClientState:
<Trait>` dispatches the user-visible method through the trait. Today's
trait set:

| Trait                                                                  | Implemented on     | Surfaces driven by it                                                                                                                                                                                                                        |
| ---------------------------------------------------------------------- | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TransactionApi`                                                       | runtime `Client`   | `PulsarClient::new_transaction` + commit/abort.                                                                                                                                                                                              |
| `SubscribeApi` (with `type Consumer: ConsumerApi`)                     | runtime `Client`   | `ConsumerBuilder<'a, E>::subscribe` + every consumer-spawning builder (`MultiTopicsConsumerBuilder<'a, E>`, `PartitionedConsumerBuilder<'a, E>`, `PatternConsumerBuilder<'a, E>`, `ReaderBuilder<'a, E>`, `TypedConsumerBuilder<'a, S, E>`). |
| `CreateProducerApi` (with `type Producer: ProducerApi`)                | runtime `Client`   | `ProducerBuilder<'a, E>::create` + `TypedProducerBuilder<'a, S, E>`.                                                                                                                                                                         |
| `ConsumerApi` (with `type Producer: ProducerApi<Error = Self::Error>`) | runtime `Consumer` | All inherent methods of `MultiTopicsConsumer<C>` / `PatternConsumer<C>` / `Reader<C>` / `TableView<C>` (and the DLQ + retry helpers route through the associated `Producer`).                                                                |
| `ProducerApi`                                                          | runtime `Producer` | `PartitionedProducer<P>` inherent methods.                                                                                                                                                                                                   |
| `BrokerMetadataApi`                                                    | runtime `Client`   | `PulsarClient::partitions_for_topic` / `topic_list_snapshot`; `PartitionedConsumerBuilder` (partition discovery) + `PatternConsumer::update` (PIP-145 delta polling).                                                                        |

Pass-2 (ADR-0037, commit `4a29ba9`) extended `ConsumerApi` with the 17 trait methods needed to lift `MultiTopicsConsumer<C>` / `PatternConsumer<C>` impl bodies (13 multi-topic helpers + `pause` / `resume` / `seek_to_message` / `seek_to_timestamp` + the `unsubscribe(force: bool)` overload), and introduced `BrokerMetadataApi` to lift the partition-count + topic-list-watcher lookups so the three builders are engine-generic end-to-end.

### `magnetar-runtime-tokio` — production (default)

| File                                                                                     | Role                                                                                                                                                                                                                            |
| ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`client.rs`](crates/magnetar-runtime-tokio/src/client.rs)                               | `Client::connect` + `connect_auth` + `connect_with` + transaction-coordinator helpers, partitioned-metadata lookup, topic-list watcher entry point.                                                                             |
| [`consumer.rs`](crates/magnetar-runtime-tokio/src/consumer.rs)                           | `Consumer` façade — `receive`, `receive_with_timeout`, `receive_batch_with_bytes_cap`, ack variants (individual / cumulative / batch / with-properties / with-txn / partial-batch), nack, seek, pause/resume, DLQ drain, stats. |
| [`producer.rs`](crates/magnetar-runtime-tokio/src/producer.rs)                           | `Producer` façade — `send`, `flush`, `close`, stats, sequence-id getters, `MemoryReserveFut` for `ProducerBlock` policy.                                                                                                        |
| [`driver.rs`](crates/magnetar-runtime-tokio/src/driver.rs)                               | Driver loop + supervised reconnect + auth-challenge dispatch + PIP-145 + PIP-188 forwarding.                                                                                                                                    |
| [`auto_cluster_failover.rs`](crates/magnetar-runtime-tokio/src/auto_cluster_failover.rs) | PIP-121 `AutoClusterFailover` with a `HealthProbe` trait + background prober.                                                                                                                                                   |
| [`compress.rs`](crates/magnetar-runtime-tokio/src/compress.rs)                           | Encode + decode for `None` / `Lz4` / `Zlib` / `Zstd` / `Snappy`.                                                                                                                                                                |
| [`transport.rs`](crates/magnetar-runtime-tokio/src/transport.rs)                         | TCP connect + optional `tokio-rustls` wrap, `connect_with_resolver` for `DnsResolver` plumbing.                                                                                                                                 |
| [`tls_insecure.rs`](crates/magnetar-runtime-tokio/src/tls_insecure.rs)                   | `tls_allow_insecure_connection(true)` blanket override.                                                                                                                                                                         |
| [`tls_no_hostname.rs`](crates/magnetar-runtime-tokio/src/tls_no_hostname.rs)             | `tls_hostname_verification_enable(false)` chain-on / hostname-off.                                                                                                                                                              |
| [`dns.rs`](crates/magnetar-runtime-tokio/src/dns.rs)                                     | `DnsResolver` trait + `TokioDnsResolver`.                                                                                                                                                                                       |
| [`lib.rs`](crates/magnetar-runtime-tokio/src/lib.rs)                                     | `ConnectionShared` (state, atomic counters, `memory_used` + `memory_wakers` slab) + `TopicListChange`.                                                                                                                          |

### `magnetar-runtime-moonpool` — deterministic simulation

| File                                                                | Role                                                                                                                                                                           |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`lib.rs`](crates/magnetar-runtime-moonpool/src/lib.rs)             | `ConnectionShared`, `MoonpoolEngine<P>` generic over `moonpool_core::Providers`, `connect_plain` / `connect_plain_with_resolver` / `connect_plain_supervised` / `connect_tls`. |
| [`driver.rs`](crates/magnetar-runtime-moonpool/src/driver.rs)       | Driver loop + supervised reconnect over the moonpool byte pipe. Mirrors `magnetar-runtime-tokio::driver`.                                                                      |
| [`client.rs`](crates/magnetar-runtime-moonpool/src/client.rs)       | `Client<P>` façade — `connect_plain`, `connect_plain_supervised`, partitioned-metadata lookup, txn coordinator helpers.                                                        |
| [`producer.rs`](crates/magnetar-runtime-moonpool/src/producer.rs)   | `Producer<P>` façade — `send`, `flush`, `close`, stats. Surface mirrors `magnetar-runtime-tokio::producer` (1:1 method set; `FailImmediately` only on the memory-limit knob).  |
| [`consumer.rs`](crates/magnetar-runtime-moonpool/src/consumer.rs)   | `Consumer<P>` façade — `receive`, ack variants, nack, seek, pause/resume, DLQ drain.                                                                                           |
| [`tls.rs`](crates/magnetar-runtime-moonpool/src/tls.rs)             | `RustlsByteAdapter` — drives sans-io `rustls::ClientConnection` over a `NetworkProvider`-supplied byte pipe. Sans-io composition end to end.                                   |
| [`transport.rs`](crates/magnetar-runtime-moonpool/src/transport.rs) | Plaintext byte pipe over the configured `NetworkProvider::TcpStream`.                                                                                                          |
| [`dns.rs`](crates/magnetar-runtime-moonpool/src/dns.rs)             | `DnsResolver` trait + `StaticDnsResolver` + `arc_dns_resolver` helper.                                                                                                         |

Key properties:

- The engine is generic over `moonpool_core::Providers`, which bundles `NetworkProvider`, `TimeProvider`, `TaskProvider`, `RandomProvider`, `StorageProvider`.
  Plug `TokioProviders` for production-style runs against a real broker; plug `moonpool_sim::SimProviders` for Moonpool 0.8's native seeded executor, virtual clock, simulated network/storage, and reproducible chaos.
- Provider-generic tasks, timers, and concurrent waits use `TaskProvider`, `TimeProvider`, and `moonpool_core::select!`; the simulation path does not require an ambient Tokio runtime ([ADR-0078](specs/adr/0078-adopt-moonpool-0-8-native-deterministic-runtime.md)).
- The driver consumes the same `magnetar-proto::Connection` state machine as the tokio engine — the differences are which byte pipe carries the I/O and which clock source the engine snapshots into `Connection::send(now, …)` / `flush_producer(now, …)` and into the `with_wall_clock_provider` slot.
- TLS handshakes survive chaos with the same determinism as `magnetar-proto` itself — the adapter never blocks on a network call inside `process_new_packets`; reads and writes go through the byte pipe under simulation control.

The full moonpool engine surface (supervised reconnect, chaos pack, differential equivalence harness) is covered in [`docs/moonpool-engine.md`](docs/moonpool-engine.md).

---

## PIP-121 cluster failover architecture

The supervised reconnect path (Stage 2) re-resolves the broker URL on every attempt via a pluggable `ServiceUrlProvider`.
Three implementations ship:

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

ADR-0011 (clock injection) is unaffected — the prober uses tokio's wall clock for its `interval` driver, but the active URL itself is just a `String`.

---

## memory_limit runtime accounting

Java's `ClientBuilder#memoryLimit(long, MemoryLimitPolicy)` is enforced via an `AtomicU64` CAS reservation in `Producer::send`:

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

`MemoryLimitPolicy::ProducerBlock` is the other half: on overflow, `Producer::send` parks on a `Waker` slab inside `ConnectionShared`; `release_memory` drains the slab so parked producers re-poll the CAS.
See [`docs/memory-limit.md`](docs/memory-limit.md) and [ADR-0020](specs/adr/0020-memory-limit-producer-block.md).

---

## PIP-188 reconnect-on-migrate flow

The broker can ask the client to move a producer / consumer to a different broker via `CommandTopicMigrated`. magnetar handles it as:

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

User futures stay live across the migration.
`Connection::reset()` fails every pending **request** (lookup, partitioned-metadata, seek, ack, transaction round-trip) with `OpOutcome::SessionLost`, but treats in-flight **publishes** specially: it snapshots single and chunked operations and `rebuild_producers()` re-issues them on the new session **without** a `SessionLost` outcome, so the user's `SendFut` stays pending and resolves transparently when the replayed receipt lands (Stage 3 transparent at-least-once replay; mirrors Java `ProducerImpl#resendMessages`).
A producer batch has one ranged wire frame but one `OpSend` per logical member and therefore no safe per-message replay frame; reset installs a deterministic `SendError` for every such operation before waking it, including after flush and before receipt ([ADR-0096](specs/adr/0096-reconnect-batch-and-durable-cursor-safety.md)).
Consumer rebuild likewise distinguishes client hints from durable state: a never-attached or non-durable consumer keeps only its explicit initial position, while every established durable `CommandSubscribe` reattach omits `start_message_id` so the broker's persisted cursor is authoritative.
No reattach uses a locally submitted ack because it is neither broker-confirmed nor necessarily contiguous ([ADR-0099](specs/adr/0099-nondurable-reattach-cursor-safety.md)).
Because reset clears the session-local PIP-54 tracker, a later individual batch ack with valid coordinates reconstructs an all-unacked `BatchAckEntry` and clears only the requested index; missing state never degrades to a full-entry ack.
A lookup behind `subscribe` / `open_producer` severed by the reset is likewise re-issued transparently — the engine's `lookup_topic` parks on `ConnectionShared::await_reconnect_or_terminal` and re-runs the `CommandLookupTopic` against the fresh session, bounded by `MAX_LOOKUP_SESSION_REISSUES`, surfacing `PeerClosed` only if the supervisor gives up (`no_driver` latched).
See [ADR-0060](specs/adr/0060-lookup-retry-on-session-lost.md) (lookup) and [ADR-0059](specs/adr/0059-terminal-fast-fail-new-ops.md) (the `no_driver` terminal latch).

**Lookup redirect dialing** ([ADR-0039](specs/adr/0039-pulsar-proxy-multi-broker-connection-model.md), 2026-06-14 amendment).
A `CommandLookupTopic` resolves to one of three terminal outcomes on its request-id: `Connect` (route the data ops here), `Failed`, or `Redirected` (this broker is not the bundle owner).
The sans-io core never chases a redirect itself — that would mean dialing a socket, forbidden in `magnetar-proto` ([ADR-0004](specs/adr/0004-sans-io-protocol-core.md)).
Instead it surfaces a **driveable** `LookupOutcome::Redirected { broker_service_url, broker_service_url_tls, authoritative, hops_remaining }`, and the engine's `lookup_topic` loop dials the redirect-target broker (reusing `resolve_direct_broker` / the per-broker `ProxyConnectionPool`, no new connection machinery; the dial awaits **outside** any proto/connection lock per [ADR-0038](specs/adr/0038-split-connection-mutex.md) and uses no channel per [ADR-0003](specs/adr/0003-no-channels-rule.md)) and re-issues the lookup THERE via `Connection::lookup_redirect`.
This mirrors Java `BinaryProtoLookupService#findBroker` recursing on `getConnection(redirectAddress)`.
The chase used to run inside `magnetar-proto` (re-encoding `CommandLookupTopic` on the bootstrap socket), which re-asked the same non-owner broker and looped to the cap on a multi-broker cluster.
The `MAX_LOOKUP_REDIRECTS` cap is enforced end-to-end: the proto translate-layer floor (`Failed` at zero), a proto-side clamp in `lookup_redirect` (a buggy engine cannot inflate the carried `hops_remaining`), and an engine guard that refuses to dial a `Redirected` with no budget left and surfaces the same synthetic cap `Failed`.
When the resolved owner answers `Connect` with no advertised URL, the data ops ride the connection the lookup landed on (the dialed target), not the bootstrap.

**Connections per broker** ([ADR-0073](specs/adr/0073-connections-per-broker.md), issue #314).
By default magnetar opens **one** connection per broker, so every producer and consumer for that broker shares one TCP connection — `Client::resolve_target` returns one `Arc<ConnectionShared>` per `(logical, physical)`.
`ClientBuilder::connections_per_broker(n)` (Java `ClientBuilder#connectionsPerBroker`) lifts that to `n`: the per-broker pool key gains a `connection_index ∈ [0, n)` (`(logical, physical)` → `(logical, physical, index)`), and `resolve_target` round-robins the index across producers AND consumers via an `AtomicUsize` cursor — one chokepoint, so both data-plane surfaces fan out.
The bootstrap connection serves index `0`; indices `1..n` are lazy pool siblings (`get_or_open_bootstrap_sibling`) that dial the bootstrap's physical address and replicate its CONNECT.
The round-robin is a plain atomic counter, **not** Java's random key, so the spread is deterministic and the moonpool engine mirrors it bit-for-bit ([ADR-0011](specs/adr/0011-clock-injection-sans-io.md), differential `EventStream` parity).
The knob is runtime-only — it never reaches the sans-io core ([ADR-0004](specs/adr/0004-sans-io-protocol-core.md)); lookups and redirect dials always pin index `0` (they never consume a fan-out slot), and a `from_socket` client with no pool clamps to one connection.
This complements [ADR-0070](specs/adr/0070-driver-read-arm-fairness.md) (which fixed per-connection `send→ack` latency) by letting a logical producer fleet use more than one connection's worth of send pipeline, instead of applications hand-rolling a pool of `PulsarClient`s.

---

## TLS sites

The workspace has **three** TLS sites.
**None** use `native-tls`.

1. **`magnetar-runtime-tokio`** — `tokio_rustls::TlsConnector::connect(server_name, tcp)` is the standard path.
   Roots come from `rustls-native-certs` by default; users can override with `ClientBuilder::tls_trust_certs_pem` / `tls_trust_certs_file_path`, in which case `Client::tls_config_from_pem` builds a custom `rustls::ClientConfig` from the supplied PEM chain.
2. **`magnetar-runtime-moonpool`** — `tls::RustlsByteAdapter` drives a `rustls::ClientConnection` (itself sans-io) over the moonpool byte pipe.
   A read-arm win pumps `socket.read` → `session.read_tls` → `session.process_new_packets()` → drain `session.reader()` into `plaintext_in`; symmetric on the write path.
   Since [ADR-0083](specs/adr/0083-bounded-cancellable-driver-write.md), the read and write `select!` arms hold independent halves of the split transport (`Transport::into_split`), so the adapter — inherently bidirectional (one `step()` call drains both directions) — lives behind `Arc<parking_lot::Mutex<TlsShared>>`, shared by both halves; the mutex is never held across an `.await` (`step()` is fully synchronous).
   `TlsShared` also carries a resumable `pending_ciphertext` queue: the read half only ever appends to it (e.g. a protocol-mandated TLS 1.3 `KeyUpdate` ack produced while decrypting inbound bytes), never writing to the socket itself, and the write half drains it.
3. **`magnetar-admin`** — `reqwest` configured with `rustls-tls` for the REST admin client.

Source: GUIDELINES.md §"TLS" — rule is hard.
`cargo deny check` rejects `openssl-sys` / `native-tls` / `native-tls-sys` outright.

### Pluggable crypto provider (issue #9, ADR-0035)

The rustls crypto primitives that back the handshake are selected at compile time on the `magnetar` façade via four mutually-pluggable features:

| Feature            | Backend                                         |
| ------------------ | ----------------------------------------------- |
| `crypto-aws-lc-rs` | `aws-lc-rs` (default; brings X25519MLKEM768)    |
| `crypto-ring`      | `ring`                                          |
| `crypto-openssl`   | `rustls-openssl` (wraps system OpenSSL)         |
| `crypto-fips`      | `aws-lc-fips-sys` (FIPS-validated; needs cmake) |

Both runtime crates carry a sibling `tls_crypto` module that exposes `install_default_provider()` (idempotent) and `active_provider()`.
The four production callsites (`tls_insecure.rs`, `tls_no_hostname.rs`, `transport.rs`, `client.rs`) go through `active_provider()` rather than the historical `CryptoProvider::get_default()` + `ring` fallback.
Under `--all-features` the cfg cascade resolves to aws-lc-rs.

`openssl` / `openssl-sys` are admitted only as transitive deps of `rustls-openssl` via `deny.toml`'s `wrappers = ["rustls-openssl"]` carve-out; the rest of [ADR-0005](specs/adr/0005-rustls-only-tls.md) (no `native-tls`, rustls everywhere) stays in force.
See [ADR-0035](specs/adr/0035-pluggable-crypto-provider.md).

---

## Schemas

The `Schema` trait lives at [`crates/magnetar-proto/src/schema/mod.rs`](crates/magnetar-proto/src/schema/mod.rs):

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

| Schema                                                                                  | Owned type                 | Wire bytes                                                                                       |
| --------------------------------------------------------------------------------------- | -------------------------- | ------------------------------------------------------------------------------------------------ |
| `BytesSchema`                                                                           | `Bytes`                    | passthrough                                                                                      |
| `StringSchema`                                                                          | `String`                   | UTF-8                                                                                            |
| `JsonSchema<T: Serialize + DeserializeOwned>`                                           | `T`                        | JSON via `serde_json`; broker stores canonicalised form                                          |
| `AvroSchema`                                                                            | `apache_avro::Value`       | Avro single-object encoding; canonical parsing form for version dedup                            |
| `ProtobufSchema`                                                                        | `prost::Message`           | Protobuf wire encoding; descriptor-based version dedup                                           |
| `ProtobufNativeSchema`                                                                  | `prost::Message`           | Protobuf wire encoding; byte-identical Java `FileDescriptorSet` for version dedup                |
| `KeyValueSchema`                                                                        | `KeyValuePair<K, V>`       | Concatenated `(key_len, key, value_len, value)` with `KeyValueEncodingType::{Inline, Separated}` |
| `AutoConsumeSchema`                                                                     | `GenericRecord`            | Trait surface only — broker-driven lookup pending                                                |
| `AutoProduceBytesSchema`                                                                | `Bytes`                    | Trait surface only                                                                               |
| `Int8Schema` / `Int16Schema` / `Int32Schema` / `Int64Schema`                            | `iN`                       | Big-endian fixed-width                                                                           |
| `FloatSchema` / `DoubleSchema`                                                          | `fN`                       | IEEE 754 big-endian                                                                              |
| `BoolSchema`                                                                            | `bool`                     | Single byte (`0x00` / `0x01`)                                                                    |
| `DateSchema` / `TimeSchema` / `TimestampSchema` / `LocalDateSchema` / `LocalTimeSchema` | `i64`                      | 8-byte big-endian                                                                                |
| `InstantSchema`                                                                         | `(i64 seconds, i32 nanos)` | 12-byte big-endian                                                                               |
| `LocalDateTimeSchema`                                                                   | `(i64 seconds, i32 nanos)` | 12-byte big-endian                                                                               |

### Canonicalisation (Codex Q4)

Per the cross-check on `SchemaRegistryServiceImpl.java:405-438` (Apache Pulsar Java reference, external):

- **AVRO / JSON / PROTOBUF** schemas are re-parsed broker-side via the Avro `Schema.Parser` before the version lookup.
  Magnetar emits the Avro canonical parsing form (`AvroSchema`) so two logically-identical schemas hash to the same version regardless of whitespace, field order, or property ordering.
- **PROTOBUF_NATIVE** and **KeyValue** are stored as opaque blobs and compared by **raw-byte equality**. The Java client emits a `FileDescriptorSet` for `PROTOBUF_NATIVE` and a stable JSON shape (`{"key": ..., "value": ..., "keyValueEncodingType": ...}`) for `KeyValue`.
  Magnetar emits byte-identical output for both, otherwise the broker would create a fresh schema version on every (re)connect and defeat the registry's deduplication.

Source: [`crates/magnetar-proto/src/schema/mod.rs:19-34`](crates/magnetar-proto/src/schema/mod.rs).

### Typed producer / consumer

`magnetar::TypedProducer<S: Schema>` and `magnetar::TypedConsumer<S>` serialise / deserialise per call.
Construction:

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

The schema is advertised on `CommandProducer.schema` / `CommandSubscribe.schema`; the broker performs version negotiation.

---

## PIP coverage map

| PIP                      | Title                                    | Status | Lives in                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ------------------------ | ---------------------------------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PIP-4                    | End-to-end encryption (AES-GCM)          | ✅     | `crates/magnetar-messagecrypto/src/lib.rs:98-220`; bridge: `crates/magnetar/src/crypto_bridge.rs`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| PIP-22                   | DLQ topic                                | ✅     | `ConsumerBuilder::dead_letter_policy` + `Consumer::drain_dead_letter`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| PIP-30                   | In-band `AUTH_CHALLENGE` refresh         | ✅     | `crates/magnetar-proto/src/auth.rs`; dispatch: `crates/magnetar-runtime-tokio/src/driver.rs:42-66`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| PIP-31                   | Transactions                             | ✅     | `crates/magnetar-proto/src/txn.rs`; client surface: `Client::new_txn`, `add_partition_to_txn`, `add_subscription_to_txn`, `end_txn`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| PIP-37                   | Chunking + `AckTimeoutRedeliveryBackoff` | ✅     | Chunked producer path: `crates/magnetar-proto/src/producer.rs`; backoff: `crates/magnetar-proto/src/trackers/nack.rs`; consumer-side reassembly bounded in `crates/magnetar-proto/src/consumer.rs` (`max_pending_chunked_message` cap 10 with oldest-eviction, `expire_time_of_incomplete_chunked_message` 60s sweep wired through `poll_timeout` + `handle_timeout`, `auto_ack_oldest_chunked_message_on_queue_full` false — Java-matching), guarding against unbounded `chunk_reassembly` growth; accepted incomplete chunks use Java-compatible per-chunk flow replenishment so each broker permit is repaid before logical-message reassembly completes |
| PIP-54                   | Partial-batch ACK (ack_set bitset)       | ✅     | `crates/magnetar-proto/src/consumer.rs:109-130`; ack stamping: `crates/magnetar-proto/src/conn.rs:1775`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| PIP-58                   | Retry-letter topic                       | ✅     | `Consumer::reconsume_later` + `reconsume_later_with_properties`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| PIP-68                   | Exclusive producer access mode           | ✅     | `ProducerBuilder::access_mode`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| PIP-90                   | Broker-entry metadata envelope           | ✅     | `crates/magnetar-proto/src/frame.rs:30-48`; consumer getters: `IncomingMessage::broker_publish_time_ms` / `broker_index`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| PIP-124                  | Multi-DLQ topics for KeyShared           | ✅     | DLQ policy infra (shared with PIP-22)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| PIP-121                  | Cluster failover (Auto + Controlled)     | ✅     | `crates/magnetar-proto/src/service_url.rs`, `crates/magnetar-proto/src/cluster_failover.rs`, `crates/magnetar-runtime-tokio/src/auto_cluster_failover.rs` (see [ADR-0016](specs/adr/0016-pip-121-cluster-failover.md))                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| PIP-145                  | Topic list watcher (regex pattern)       | ✅     | `crates/magnetar-proto/src/topic_watcher.rs`; consumer façade: `crates/magnetar/src/pattern_consumer.rs`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| PIP-188                  | `TOPIC_MIGRATED` → reconnect-on-migrate  | ✅     | Driver event arm in `crates/magnetar-runtime-tokio/src/driver.rs` returns `ClientError` to trigger supervised reset + reconnect; see [ADR-0018](specs/adr/0018-pip-188-reconnect-on-migrate.md)                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| PIP-292                  | Better in-band auth refresh ergonomics   | ✅     | `crates/magnetar-runtime-tokio/src/driver.rs:42-66`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| PIP-313                  | Force unsubscribe                        | ✅     | `CommandUnsubscribe.force` field plumbed                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| PIP-34 / 119 / 282 / 379 | Key_Shared family                        | ✅     | `magnetar_proto::KeySharedConfig` + builder routing                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| PIP-391                  | Batch-index ACK polish                   | ✅     | Pairs with PIP-54                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| PIP-409                  | DLQ + retry-letter polish                | ✅     | DLQ + reconsume_later wiring                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| PIP-460                  | Scalable topics                          | 🟡     | Experimental, behind `feature = "scalable-topics"` (default off). Speaks the surface vendored from Apache Pulsar 5.0.0-M1 — a milestone, not GA — and the e2e suite runs against a real 5.0.0-M1 broker on every push per ADR-0046. See [ADR-0093](specs/adr/0093-pip-460-upstream-wire-surface.md) (supersedes ADR-0031) + [ADR-0095](specs/adr/0095-ignore-a-re-sent-scalable-layout-epoch.md) + [`docs/pip-features.md#scalable-topics-pip-460--experimental`](docs/pip-features.md#scalable-topics-pip-460--experimental)                                                                                                                               |
| PIP-466                  | V5 client API surface                    | ✅     | Experimental, engine-generic wrapper behind `feature = "experimental-v5-client"`; no wire change. See [ADR-0032](specs/adr/0032-pip-466-v5-client-surface-scope.md) + [`docs/pip-features.md#v5-client-surface-pip-466`](docs/pip-features.md#v5-client-surface-pip-466)                                                                                                                                                                                                                                                                                                                                                                                    |
| PIP-180                  | Shadow topic                             | ✅     | Admin REST (`create_shadow_topic` / `delete_shadow_topic` / `get_shadow_topics` / `get_shadow_source`), producer-side `send_with_source_message_id` propagating `CommandSend.message_id`, consumer-side `MessageReceivedFromShadow` event, structural `MessageId` equality across source ⇄ shadow. See [`docs/pip-features.md#shadow-topics-pip-180`](docs/pip-features.md#shadow-topics-pip-180) + [ADR-0033](specs/adr/0033-pip-180-shadow-topic-scope.md).                                                                                                                                                                                               |
| PIP-415                  | `getMessageIdByIndex`                    | ✅     | `crates/magnetar-admin/src/lib.rs::AdminClient::topic_get_message_id_by_index` — REST-only ([PIP-415 spec](https://github.com/apache/pulsar/blob/master/pip/pip-415.md) leaves "Binary protocol" empty; canonical impl [`apache/pulsar#24222`](https://github.com/apache/pulsar/pull/24222) is admin/broker/CLI only)                                                                                                                                                                                                                                                                                                                                       |
| PIP-33                   | Replicated subscriptions                 | ✅     | `ConsumerBuilder::replicate_subscription_state(bool)` on the façade flips `CommandSubscribe` field 14; receive-path filter in `magnetar-proto::conn` drops `REPLICATED_SUBSCRIPTION_*` markers and surfaces them via `PulsarClient::next_replicated_subscription_marker` / `poll_replicated_subscription_marker`. Client never originates markers — broker-side machinery only. See [`docs/pip-features.md#replicated-subscriptions-pip-33`](docs/pip-features.md#replicated-subscriptions-pip-33) + [ADR-0034](specs/adr/0034-pip-33-replicated-subscriptions-scope.md).                                                                                   |

---

## Tests

See [`docs/testing.md`](docs/testing.md) for the full reference (unit, integration, deterministic chaos, differential equivalence, e2e, mutation, fuzz).
High-level summary:

- **Unit + integration**: `cargo test --workspace --all-features`.
  Every sans-io behavior is exercised by feeding bytes, asserting events / transmit / state.
  Trackers ship 13 ported behavioral cases from Java's `UnAckedMessageTrackerTest` + `AckGroupingTrackerTest`; the producer ships 6 ported cases from `BatchMessageContainerImplTest`.
- **Deterministic chaos** ([`crates/magnetar-runtime-moonpool/tests/`](crates/magnetar-runtime-moonpool/tests/)): `SimProviders` runs the Moonpool engine on its native seeded executor and drives supervised reconnect, PIP-121, PIP-188, virtual-clock timers, and structured-trace invariants under reproducible seeds.
- **Differential equivalence** ([`crates/magnetar-differential/tests/`](crates/magnetar-differential/tests/)): tokio + moonpool engines run the same `Trace` against a scripted in-process broker; user-visible `EventStream`s must agree.
- **End-to-end** ([`crates/magnetar/tests/e2e_*.rs`](crates/magnetar/tests/)): regular tests with no feature gate and no `#[ignore]` ([ADR-0046](specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md)); Docker is the runtime prerequisite.
  Spins `apachepulsar/pulsar:4.0.4` via `testcontainers`.
  Covers schemas, DLQ, batching+chunking, interceptors, transactions, subscription types, partitioned, compacted+TableView, encryption, OAuth2, DNS resolver, force unsubscribe, memory limit, pattern auto-reconcile, supervised reconnect, rolling stats, per-partition seek, PIP-121 cluster failover.

Run them with the regular workspace test command: `cargo test --workspace --all-features --locked` (requires Docker).

### Mutation testing (scoped)

```sh
cargo mutants --package magnetar-proto --timeout 60 --shard 1/4
```

Targets: frame decode, request correlation, resend / dedup, flow permits, chunk metadata, timeout transitions.

### Fuzz (`magnetar-proto/fuzz`)

```sh
cargo +nightly fuzz run encode_roundtrip
```

Round-trip-encodes `BaseCommand` shapes and asserts re-decode equality.

---

## Build & validation

Stable Rust **1.91** (workspace `rust-version` in `Cargo.toml`; see [ADR-0079](specs/adr/0079-raise-msrv-to-rust-1-91.md)).

### Per-commit chain

```sh
cargo build --workspace --all-features
cargo clippy --workspace --all-features -- -D warnings
cargo +nightly fmt --check
cargo test --workspace
cargo deny check
RUSTDOCFLAGS="-D warnings" \
  cargo doc --no-deps --all-features --workspace --locked
```

### When touching `magnetar-proto`

```sh
cargo run -p xtask -- check-no-channels   # greps src/** for banned channel paths
cargo run -p xtask -- check-no-io-deps    # asserts magnetar-proto has no I/O deps
cargo run -p xtask -- codegen --check     # asserts proto codegen has no drift
```

### Workspace lints

`forbid(unsafe_code)` workspace-wide.
`unreachable_pub = "warn"`, `missing_debug_implementations = "warn"`.
Pedantic clippy on the whole workspace with `cast_possible_truncation`, `cast_sign_loss`, `cast_possible_wrap`, `module_name_repetitions`, `must_use_candidate`, `missing_errors_doc`, `missing_panics_doc`, and `unnecessary_literal_bound` allowed (justification in workspace `Cargo.toml`).

### Forbidden crates (`cargo deny bans deny`)

Channel-shaped: any crate that ships an `mpsc` / `broadcast` / `watch` / `oneshot` flavour — `crossbeam-channel`, `flume`, `async-channel`, `kanal`, `postage`, `tachyonix`, `thingbuf`, plus the corresponding `tokio::sync::*` paths via `clippy.toml`'s `disallowed-types`.

TLS-related: `openssl-sys`, `openssl`, `native-tls`, `native-tls-sys`.

### Dependency allow-list

The final allow-list is tracked internally and enforced through `cargo deny`.
Any addition needs explicit project-owner approval.

---

## Further reading

- [README.md](README.md) — user-facing entry point.
- [GUIDELINES.md](GUIDELINES.md) — coding conventions + invariants.
- [CONTRIBUTING.md](CONTRIBUTING.md) — patch flow + sign-off.
- The Apache Pulsar Java client at [`apache/pulsar/pulsar-client`](https://github.com/apache/pulsar/tree/master/pulsar-client) — primary parity reference.
- `quinn-proto` at [`quinn-rs/quinn/quinn-proto`](https://github.com/quinn-rs/quinn/tree/main/quinn-proto) — sans-io reference shape that `magnetar-proto::Connection` mirrors.
