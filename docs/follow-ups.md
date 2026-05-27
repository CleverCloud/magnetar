# Open Follow-Ups

Consolidated tracker for known open work. Each entry lists the gap, the
reason it stays open, and where the unblock lives.

For the public-facing parity status, see
[`parity-status.md`](parity-status.md) and the
[parity matrix in the README](../README.md#java-client-parity-matrix).

This file is the **single source of truth** for what is intentionally
deferred or blocked. Items with a `/goal …` block at the bottom of
their entry are ready to be picked up by an agent team — copy the
prompt verbatim into a fresh session.

History — what already landed — lives in `git log` and in the per-ADR
implementation notes. Anything not listed below is either done, or
explicitly out of scope for v0.2.0 ([ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
§D-series, [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md),
[ADR-0032](../specs/adr/0032-pip-466-v5-client-surface-scope.md)).

---

## Audit 2026-05-27 — open items

The 2026-05-27 multi-agent code audit (eight parallel agents, seven
Claude + one codex run) shipped its correctness, performance, and
sans-io fixes in `fix/audit-p0-findings` (commits `1644cb7`,
`bf66a5b`, `710241d`, `2727f49`, `a31dcaa`, `1ded2f3`, `7f2faee`,
`7ca836e`). The Sub-mutex split for the global Connection mutex
landed separately in [ADR-0038](../specs/adr/0038-split-connection-mutex.md).

What remains open from that audit — bucketed by category — is below.
Findings are `path:line`-verifiable; tags: **[codex]** = codex-only
catch, **[Δ]** = auditor disagreement with documented resolution.

### Open — zero-copy

- **`crates/magnetar-proto/src/frame.rs::encode_payload`** — single
  `BytesMut` accumulator copies every payload into the wire buffer.
  Return a frame descriptor `{head: BytesMut, payload: Bytes}` and
  vectored-write for plaintext — the producer batch path then chains
  `Bytes` segments instead of memcpy-concat. TLS path keeps the
  contiguous coalesce. **Design landed**:
  [ADR-0039](../specs/adr/0039-vectored-io-transmit-enum.md) (Proposed) —
  three-wave landing plan (proto+tokio first, moonpool
  `Providers::Network::write_vectored` second, read-path ownership
  pass-through third). Implementation still TODO.

### Open — syscall reduction

- **No `writev` / `IoSlice`** —
  `crates/magnetar-runtime-tokio/src/driver.rs::driver_loop_inner` +
  `crates/magnetar-proto/src/conn.rs::poll_transmit` — outbound
  coalesces into a single `BytesMut` before write. **Design landed**:
  [ADR-0039](../specs/adr/0039-vectored-io-transmit-enum.md) (Proposed)
  — wave 1 (proto `Transmit` enum + tokio `write_vectored`), wave 2
  (moonpool `Providers::Network::write_vectored` + chaos pack
  segment-granular drops), wave 3 (read-path `BytesMut` ownership
  pass-through). **Waves 1.0 + 1.1 + 1.2 landed**: `Transmit<'a>`
  enum (`Contiguous` / `Vectored`) lives in
  `crates/magnetar-proto/src/transmit.rs`;
  `Connection::poll_transmit_vectored` returns `Transmit<'_>` and
  is wired to emit **either** arm based on which buffer carries
  pending bytes — `Contiguous` for handshake / ack / lookup frames
  in `outbound: BytesMut`, `Vectored` for producer-batch frames in
  `outbound_segments: Vec<Bytes>` populated by the new
  `Connection::drain_producer_outbound_vectored`. The latter calls
  `frame::encode_payload_head` (the wave-1.2 zero-copy encoder)
  which returns the head bytes without copying the payload, then
  pushes a `[head, payload]` pair into the segment list — the
  payload `Bytes` is re-used unchanged from the producer state.
  When both buffers carry data, the contiguous arm wins so wire
  order is preserved; segments stay queued and emerge on the next
  call. **Wave 2** (moonpool `Providers::Network::write_vectored`
  + chaos pack segment-granular drops) and **wave 3** (read-path
  `BytesMut` ownership pass-through) still TODO; runtime adoption
  (tokio `poll_write_vectored` on the Vectored arm) lands with
  wave 2.
- **Read path double-copy** —
  `crates/magnetar-runtime-tokio/src/driver.rs::driver_loop_inner`
  reads `read_buf` → `split().freeze()`. The proto-side re-copy was
  removed by the `handle_bytes` `split_to` refactor (commit
  `bf66a5b`). Once the segment-aware transmit type lands (ADR-0039
  wave 3), the runtime can pass owned `BytesMut` ownership directly.

### Open — security hardening

- **Athenz private key as `String`** —
  `crates/magnetar-auth-athenz/src/lib.rs` — `AthenzConfig::Debug` now
  redacts `private_key_pem`. Wrapping the **parsed** RSA key in
  `zeroize::Zeroizing<…>` is still pending; ADR-0030 defers this to
  the actual ZTS round-trip landing (the parsed key only exists once
  that work happens — see the Athenz ZTS round-trip entry below).

### Open — cleanup and structural clarity

- **`ProducerExt` trait, single impl** —
  `crates/magnetar/src/client.rs::ProducerExt`. The original audit
  suggested inlining as a direct method on
  `magnetar_runtime_tokio::Producer`, but that requires moving
  `MessageBuilder` + `OutgoingMessage` (currently in
  `magnetar/src/client.rs`) **down** into `magnetar-runtime-tokio` —
  which inverts the workspace dep graph (`magnetar-runtime-tokio` is
  below `magnetar`). The trait sits where it sits to satisfy Rust's
  orphan rule for the façade-defined `MessageBuilder` against the
  runtime-defined `Producer`. Resolving cleanly needs a bigger split
  decision (move `MessageBuilder` to a shared crate, or accept the
  trait as the layering artefact). Documented for future
  consideration, not actionable as a pure inline today.
- **`ProducerBuilder<'a, E>` / `ConsumerBuilder<'a, E>` /
  `ReaderBuilder<'a, E>` are 95% tokio-bound** — phantom `E`
  parameter on builder methods that ignore it. Move the generic only
  to the final `.create()` / `.subscribe()` dispatch.
- **Large modules: `client.rs` (now 1581 lines, was 2544),
  `engine.rs` (2148 lines), `conn.rs` (now 5241 lines, was 5724)** —
  split candidates. The audit's original suggestion that `conn.rs`
  shed `txn.rs` / `dlq.rs` / `anti_thrash.rs` satellites was
  **stale**: `txn.rs` and `anti_thrash.rs` already exist as sibling
  files in `crates/magnetar-proto/src/`; `dlq.rs` lives inside
  `consumer.rs::DeadLetterPolicy` / `ConsumerState.dead_letter_pending`
  (no separate file today).
  - `conn.rs` type definitions (`*Request` / `*Config` / `*Outcome` /
    `KeySharedConfig` / `AckRequest` / `SeekTarget` /
    `HandshakeState` / `PendingOpKey` / `MemoryLimitPolicy`) → moved
    to `crates/magnetar-proto/src/conn_types.rs` (504 lines),
    re-exported from `conn::*` via `pub use crate::conn_types::*`.
    **Landed**.
  - `client.rs::ClientBuilder` → moved to
    `crates/magnetar/src/client_builder.rs` (387 lines), re-exported
    from `magnetar::*`. **Landed**.
  - `client.rs::{ProducerBuilder, ConsumerBuilder, ReaderBuilder}`
    (generic + tokio-specific impl blocks) → moved to
    `crates/magnetar/src/builders.rs` (632 lines), re-exported from
    `magnetar::*`. **Landed**. `client.rs` shrank by 963 lines
    (38%) across the two extractions.
  - `engine.rs` → `engine/{traits.rs, tokio.rs, moonpool.rs}` once the
    per-engine impls grow further. Today the trait + two impls fit
    comfortably; defer until the next per-engine surface lift makes
    the file uncomfortably wide. **Deferred.**
- **Test-helper duplication** — `handshake_response_bytes()` and the
  related fixture-byte builders show up in multiple test files. The
  **cross-runtime** duplication (tokio vs. moonpool) is intentional
  per [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md):
  the strict tokio ↔ moonpool 1:1 test-count parity check requires
  each runtime to carry its own copy. The **within-runtime tokio**
  duplication has been collapsed: a shared
  `crates/magnetar-runtime-tokio/tests/common/mod.rs` mirrors the
  moonpool layout, and `anti_thrash.rs` / `reconnect_with_inflight.rs`
  / `two_producers_parallel.rs` all import the helper from there
  instead of re-defining it. The `src/` `#[cfg(test)]` copies in
  `producer.rs` / `consumer.rs` cannot share via that module (test
  mods inside `src/` can't import from `tests/`); those stay
  co-located with the unit tests they support — they're the
  remaining duplication and are out of scope unless rearranged into
  a `pub(crate)` test-helper module under `src/`.

---

## Per-surface builder + impl-body lifts

**Status.** Every ADR-0026 §D1 dependent surface (Transaction, Reader,
TableView, PartitionedProducer, MultiTopicsConsumer,
PartitionedConsumer, PatternConsumer, `TypedProducer`,
`TypedConsumer`) carries an engine-generic struct type parameter on
both its concrete type AND its builder. Builders dispatch their
core entry method (`create()` / `subscribe()`) through the
appropriate `*Api` extension trait so the type-level lift is
complete.

**Remaining gap — entry-point methods on `PulsarClient<E>`.** The
following entry-point methods still live in
`impl PulsarClient<TokioEngine>` rather than the engine-generic
block:

- `PulsarClient::partitioned_producer(...)`
- `PulsarClient::table_view(...)`
- `PulsarClient::typed_table_view(...)`

A previous pass of this entry assumed the inner builders were already
engine-generic. That is **not** the current state: as of
`crates/magnetar/src/partitioned_producer.rs:716` and
`crates/magnetar/src/table_view.rs:333,763`, the three builder types
(`PartitionedProducerBuilder<'a>`, `TableViewBuilder<'a>`,
`TypedTableViewBuilder<'a, S>`) carry no engine parameter and reference
tokio-only types directly (e.g.
`std::sync::Arc<dyn magnetar_runtime_tokio::MessageEncryptor>` on the
partitioned producer builder, `magnetar_runtime_tokio::MessageDecryptor`
on the table-view builder).

Concrete sub-steps before the entry-point lift can happen:

1. Make `PartitionedProducerBuilder<'a, E: Engine>` carry the
   `MessageEncryptor` / `MessageRouter` types via per-engine API
   extension traits.
2. Same for `TableViewBuilder<'a, E: Engine>` /
   `TypedTableViewBuilder<'a, E: Engine, S>`
   (`MessageDecryptor`, broker-metadata lookup).
3. Move all the inner `.consumer(...)` / `.producer(...)` plumbing
   through the engine-generic `SubscribeApi` / `CreateProducerApi`
   traits.
4. Then lift the entry-point methods to
   `impl<E: Engine> PulsarClient<E>`.

Test parity per
[ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md):
the trait additions are pure delegates so they don't introduce new
behavior to mirror; the post-lift runtime test count stays at parity
(tokio=moonpool, currently 155/155).

---

## Differential equivalence harness

### Moonpool runner `LocalSet` pump

**Status.** The consumer-receive orphan-task wake path is closed at the
sans-io layer:
[`magnetar_proto::consumer::ConsumerState`](../crates/magnetar-proto/src/consumer.rs)
exposes a per-consumer `Slab<Waker>` populated by
`register_consumer_receive_waker` / drained by `wake_receivers` on every
delivery, close, and end-of-topic. Both the tokio and moonpool runtime
`Consumer::receive()` futures register their `cx.waker()` into that slab
on first poll and evict it on `Drop`. The tokio differential runner's
`Kicker` is gone — `golden_traces` runs sub-millisecond on the tokio
engine.

What remains is structural to the differential moonpool runner: its
driver task is `spawn_local`'d into a
[`tokio::task::LocalSet`](https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html)
because [`moonpool_core::TokioProviders`]'s `TaskProvider` uses
`tokio::task::Builder::new().spawn_local(...)`. While the test outer
task is parked on `consumer.receive()`, the spawn_local'd driver task
only runs when the LocalSet's `run_until` is polled — and the proto
slab waker that we now fire on delivery is dispatched from the driver
task, which itself isn't being polled. The result is a ~30 s stall per
`Recv` until the proto keepalive deadline elapses and pumps the chain.
[`crates/magnetar-differential/src/runner_moonpool.rs`](../crates/magnetar-differential/src/runner_moonpool.rs)
keeps a 25 ms `Kicker` to pulse `driver_waker.notify_one()` and bridge
the LocalSet pump gap.

**Unblock.** Closed by the future moonpool-sim integration; the
simulator's deterministic scheduler drives both sides without
`spawn_local`. An alternative is restructuring the runner to spawn the
driver via plain `tokio::spawn`, giving up moonpool-sim compatibility
for the differential harness specifically.

### Expand the golden-trace catalog

**Status.** The harness ships seven golden traces (round-trip, batch,
nack-redelivery, seek-to-start, many-publishes, lookup-before-open,
seek-per-partition). Missing: transactional ack paths and the
`cryptoFailureAction` matrix.

**Unblock.** Each new trace extends the scripted broker as needed (the
broker speaks a deliberately minimal subset of the wire protocol; new
opcodes get added per trace). Transactional ack needs `CommandEndTxn`
+ per-txn ack ledger in the broker (~180 LOC). `cryptoFailureAction`
is the largest (~240 LOC) and needs the crypto bridge ported to
moonpool first.

---

## Testing + coverage

### Residual moonpool transport TLS + driver supervised-loop coverage

**Status.**
[ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md)
landed with both `cargo xtask check-sim-coverage` and
`cargo xtask check-runtime-test-parity` enabled and hard-failing.
Runtime test parity sits at **`tokio=155 moonpool=155`** as of this
refresh (pass-1 coverage closure plus subsequent landings, including
the ADR-0038 split-connection-mutex parallel-send tests).
Per-file coverage on the five target files at the last measurement
reads:

| File | Coverage | Gap remaining |
| --- | --- | --- |
| `src/consumer.rs`  | 75.4% | 154 lines |
| `src/driver.rs`    | 54.7% | 141 lines |
| `src/lib.rs`       | 92.4% |  16 lines |
| `src/producer.rs`  | 85.4% |  55 lines |
| `src/transport.rs` | 30.3% | 124 lines |

The largest remaining hunks live in `src/transport.rs` (TLS pump
incl. `connect_tls` / `tls_handshake` / TLS-side `read_buf` /
`write_all` / `flush`) and `src/driver.rs` (supervised reconnect
loop + anti-thrash cooldown). They need either a TLS-enabled
in-process broker fixture (rustls server cert + `RustlsByteAdapter`
peer driver) or a `moonpool_core::SimProviders` substrate, both of
which are substantial scaffolding work.

```text
/goal close the residual moonpool transport TLS + driver supervised-loop coverage hunks. Stand up an in-process rustls-enabled broker fixture (self-signed cert + `RustlsByteAdapter` peer driver) under `crates/magnetar-runtime-moonpool/tests/`, then add targeted tests that exercise `Transport::connect_tls`, `tls_handshake`, the TLS variants of `read_buf` / `write_all` / `flush`, and `Transport::shutdown`. Pair each new moonpool test with a same-named tokio counterpart (the tokio path is already covered via `tls_handshake_chaos.rs`; the mirror may be a Debug / fmt smoke if the surface is engine-private). Optionally close the remaining `driver.rs` `supervised_driver_loop` lines via a synthetic peer that drops the socket between handshakes. Validation chain per CLAUDE.md.
```

---

## Auth

### Athenz ZTS round-trip

**Status.** Scaffold landed behind
`feature = "auth-athenz-zts"` (default off). `zts::ZtsClient` wraps the
reqwest-backed `POST /zts/v1/oauth2/token` exchange with
`tokio::sync::Mutex`-guarded, expiry-aware caching;
`AthenzProvider::with_zts_client(...)` + `refresh_via_zts(...)` (async)
primes the cache; `initial()` (sync) returns the cached role token.
JWT signing is intentionally factored into a pluggable
`zts::JwtSigner` trait — the magnetar workspace ships no concrete
signer because the choice (jsonwebtoken vs. aws-lc-rs vs. ring vs.
HSM-backed) is downstream-policy-dependent (FIPS posture,
key-management story, hardware-backed key support).

**Remaining work** before flipping the parity matrix row from 🟡 to ✅:

1. A concrete `JwtSigner` impl (or documented external pattern using
   `with_role_token` + a sidecar mint).
2. A Dockerised Athenz ZTS fixture (`athenz/athenz-zts-server`) under
   the `e2e` feature.
3. The corresponding e2e tests gated `feature = "e2e,auth-athenz-zts"`.

ADR-0030 deferral stays in place for the parsed-key `zeroize` wrap —
that work belongs with the concrete `JwtSigner` impl since the parsed
RSA key only materialises there.

---

## Protocol — open v0.2.0 PIP wave

The v0.2.0 planning pass produced four per-PIP proposals under
[`specs/proposals/`](../specs/proposals/) authorised by ADRs 0031–0034.
Status snapshot:

| PIP | Upstream | v0.2.0 status |
| --- | --- | --- |
| PIP-33 — Replicated subscriptions | 🟢 LIVE (Pulsar 2.4, 2019) | ✅ landed — see [ADR-0034](../specs/adr/0034-pip-33-replicated-subscriptions-scope.md) + [`docs/replicated-subscriptions.md`](replicated-subscriptions.md) |
| PIP-180 — Shadow topic | 🟢 LIVE (Pulsar 2.11, 2023) | ✅ landed — see [ADR-0033](../specs/adr/0033-pip-180-shadow-topic-scope.md) + [`docs/shadow-topic.md`](shadow-topic.md) |
| PIP-466 — V5 client surface | 🟠 DESIGN-PHASE (Java V5 still iterating; magnetar v0.2.0 surface is a v4-wire skin) | 🟡 experimental scaffold landed (`feature = "experimental-v5-client"`, default off). Remaining work: per-builder type-level surface (today only the wrapper types + mapping module ship); 5 mapping tests × 2 engines + 3 e2e tests; `docs/v5-client.md`; ADR-0032 promotion to Accepted. |
| PIP-460 — Scalable topics | 🔴 NOT LIVE (PIP `Draft`; targets Pulsar 5.0 LTS, Oct 2026; phased 4.3.0 / 4.4.0) | ⏸ blocked — needs `apachepulsar/pulsar:5.0.0-rc-*` |

### PIP-180 post-landing follow-ups

- **Moonpool `BrokerWorkload::ShadowReceive`** — the differential
  `ScriptedBroker` already echoes the client-asserted source id on
  `CommandSendReceipt`, so the moonpool sim_chaos suite doesn't
  need a separate `ShadowTopic` workload variant. If a richer
  scenario lands later (e.g. shadow-aware receive injection with
  `replicated_from` set on the inbound `CommandMessage`), add a
  `BrokerWorkload::ShadowReceive { source_topic }` variant.
- **E2E replicator-side wire path** —
  `crates/magnetar/tests/e2e_shadow_topic.rs` exercises the admin
  REST cycle + a regular produce-on-source / consume-on-shadow
  round-trip. The replicator-style `send_with_source_message_id`
  path against a real broker is covered by the differential
  equivalence test against the scripted broker that echoes the
  source id back; against Pulsar 4.x, the broker's real
  authorisation flow may reject a client-asserted source id that
  doesn't match a registered replicator producer. Adding the e2e
  assertion would need a Pulsar 4.x cluster with a registered
  replicator role — defer until that fixture is available.

### PIP-466 — V5 client surface (🟠 DESIGN-PHASE, surface usable today)

**Status.** Proposal accepted in [`specs/proposals/pip-466-v5-client-surface.md`](../specs/proposals/pip-466-v5-client-surface.md);
scope locked by [ADR-0032](../specs/adr/0032-pip-466-v5-client-surface-scope.md).
No proto change — V5 is a v4-wire skin. Upstream Java V5 is still
iterating, hence the experimental tag — but magnetar's surface works
against current Pulsar 4.x brokers since it ultimately sends the v4
commands.

**Landed (v0.2.0, feature = "experimental-v5-client", default off).**

- `magnetar::v5` module + submodules `client.rs`, `producer.rs`,
  `stream_consumer.rs`, `queue_consumer.rs`, `mapping.rs`.
- `PulsarClientV5` wraps `PulsarClient` (default `TokioEngine`),
  exposes `v4()` / `into_v4()` escape hatches that borrow the SAME
  engine state.
- `v5::Producer` / `v5::StreamConsumer` / `v5::QueueConsumer` thin
  wrappers; receive returns `crate::IncomingMessage`, send returns
  `Option<MessageId>`.
- `v5::mapping`: V5 ↔ v4 field translation table + `Duration` /
  `Option<Duration>` / `Option<usize>` converters with the spec
  defaults (`send_timeout=30s`, `max_pending=Some(1000)`,
  `ack_timeout=None`, `negative_ack_redelivery_delay=60s`,
  `receiver_queue_size=1000`, `initial_position=Latest`).
- Per-surface builders: `v5::producer::ProducerBuilder` (`send_timeout`,
  `max_pending_messages`), `v5::stream_consumer::StreamConsumerBuilder`
  (Exclusive default; `failover()`), `v5::queue_consumer::QueueConsumerBuilder`
  (Shared default; `key_shared()`). Each carries `v4()` escape hatch.
- `PulsarClientV5::producer` / `stream_consumer` / `queue_consumer`
  entry methods return the V5 builders.
- Six `v5::mapping` unit tests pin defaults + translation correctness.
- parity-status.md row + README parity matrix row at 🟡 experimental.

**Remaining work to flip the row to ✅ default-on (out of scope for the
session that landed the above):**

1. The five `crates/magnetar/tests/v5_*.rs` mapping/wire tests asserting
   the bytes `magnetar-fakes` observes match the v4 expectation:
   `v5_producer_mapping.rs`, `v5_stream_consumer_mapping.rs`,
   `v5_queue_consumer_mapping.rs`, `v5_client_v4_escape_hatch.rs`,
   `v5_builder_defaults.rs` (table-driven from `mapping.rs`).
2. Same five files mirrored 1:1 under `SimulationBuilder` for the
   moonpool engine.
3. Three e2e tests (`crates/magnetar/tests/e2e_pulsar_v5.rs` +
   `e2e_sub_types_v5.rs`) gated `feature = "e2e,experimental-v5-client"`
   parameterising existing e2e patterns against Pulsar 4.0.4.
4. `docs/v5-client.md` (NEW) including the mapping table.
5. ADR-0032 flip from Proposed → Accepted once (1)–(4) land + the
   matrix sweep (`check-crypto-matrix` × V5 axis) is green.
6. Engine-genericity: today's `PulsarClientV5` wraps `PulsarClient`
   (default tokio). A future pass parametrises by `<E: Engine>` once
   the v4 builders' tokio-bound types (`MessageEncryptor`,
   `MessageDecryptor`, `MessageRouter`) are abstracted via per-engine
   extension traits (cross-cuts with the
   "Per-surface builder + impl-body lifts" section above).

### PIP-460 — Scalable topics (🔴 NOT LIVE, scaffold-now / e2e-later)

**Status.** Proposal accepted in [`specs/proposals/pip-460-scalable-topics.md`](../specs/proposals/pip-460-scalable-topics.md);
scope locked by [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md).
Upstream PIP is **`Draft`**, targets Pulsar 5.0 LTS (Oct 2026) with
phased rollout via 4.3.0 / 4.4.0. Estimate ~2080 LOC. Wire-protocol
delta is significant — 3 new commands + a new optional
`MessageId.segment_id` — and the proto bump is gated on upstream
cutting an RC.

**Ships in v0.2.0.** StreamConsumer-only, drops-on-DAG-change (no
transparent failover), behind `feature = "scalable-topics"` (default
off). `QueueConsumer`, `CheckpointConsumer`, controller-election, and
in-place repartition are explicit v0.3.0+. **E2E is best-effort and
does not block release**; the 4-layer in-process tests are the binding
acceptance gate.

```text
/goal implement PIP-460 scalable-topics surface per specs/proposals/pip-460-scalable-topics.md and ADR-0031. Upstream is `Draft` and no broker ships PIP-460 today, so this is scaffold-now / e2e-later. Waves: (0) PREREQ — separate commit per ADR-0026 §D4: `cargo run -p xtask -- vendor-proto --rev <pulsar-5.0-rc-sha>` ONCE upstream cuts a 5.0 RC; until that lands, hand-encode the new commands behind a `cfg(feature = "scalable-topics")` gate in `magnetar-proto/src/pb/scalable_topics.rs` (NEW) using prost-build manual definitions; (1) `magnetar-proto/src/types.rs` extend `MessageId { segment_id: Option<SegmentId> }`, new types `SegmentId(u64)`, `KeyRange { start: u32, end: u32 }`, `SegmentState { Active, Splitting, Merging, Sealed }` (`#[non_exhaustive]`), `SegmentDescriptor`; equality rules: `None`-segment ignored for v4 invariant, `Some(_)` vs `None` returns false (cross-mode); (2) `magnetar-proto/src/dag_watch.rs` (NEW) — `DagWatchSession` with monotonic update_seq tracking, `handle_update(SegmentDagUpdate) -> Result<DagDelta, DagError>`, `DagError::{NonMonotonic, UnknownSegment, ...}`; (3) `magnetar-proto/src/conn.rs` — new entries `send_scalable_topic_lookup`, `open_dag_watch`, `close_dag_watch`; `magnetar-proto/src/event.rs` — new variants `ScalableTopicLookupResolved`, `SegmentDagUpdated`, `DagChangedDuringConsume { reason: DagChangeReason }`; `magnetar-proto/src/lib.rs` — new `SUPPORTED_PROTOCOL_VERSION_SCALABLE_TOPICS` constant; (4) `magnetar::scalable` module (NEW) behind `feature = "scalable-topics"` (default off) exposing `ScalableTopicsApi` extension trait + `StreamConsumer<T, E> where E::ClientState: ScalableTopicsApi`; on `DagChangedDuringConsume` close all per-segment v4 consumers and surface `ConsumerEvent::DagChanged`; (5) `magnetar-runtime-tokio` — `topic://` URL parser branch; impl `ScalableTopicsApi for TokioRuntimeState`; driver translates DagWatch events into consumer wake-ups; (6) `magnetar-runtime-moonpool` — impl `ScalableTopicsApi for Client<P>`; `magnetar-runtime-moonpool/tests/scalable_topic_broker.rs` (NEW) — scripted controller-broker (replies to lookup, opens DagWatch, pushes 2 updates: 1 split + 1 merge, then closes); `BrokerWorkload::ScalableTopic` variant in sim_chaos.rs; (7) `magnetar-cli topic-info <topic://...>` subcommand (~80 LOC, prints segment DAG). Test layers per ADR-0024 — all binding: (a) proto unit (9 tests incl. encoder roundtrip + v4-shape byte-identical guard + monotonic update_seq + split/merge), (b) tokio integration in `crates/magnetar-runtime-tokio/tests/scalable_topic.rs` (4 tests incl. `scalable_topics_feature_off_does_not_export` compile_error proof), (c) moonpool 1:1 mirror with 100% diff coverage via `check-sim-coverage`, (d) differential equivalence + golden trace `crates/magnetar-differential/tests/golden/scalable_topic_drop_on_split.json`. E2E gated behind `#[ignore = "e2e: requires Pulsar 5.0 with PIP-460"]` + `feature = "e2e,scalable-topics"` — `crates/magnetar/tests/e2e_scalable_topic.rs` (NEW) does NOT block v0.2.0 release-cut. Docs: `docs/scalable-topics.md` (NEW with experimental banner + drop-on-change semantics), parity-status.md row → 🟡 experimental, README parity matrix row, flip ADR-0031 to Accepted. Land in this exact order to keep `check-runtime-test-parity` green: (a) before (b); moonpool `ScalableTopicBroker` fake before any tokio test; differential after both engines have green tests. Out of scope (v0.3.0+ markers): QueueConsumer, CheckpointConsumer, controller-election awareness, transparent segment failover, in-place repartition, segment-aware sticky-key dispatch.
```

---

## Notes on this file

Items move from this file to git history when their commit lands. The
expected churn pattern:

1. New gap surfaces → entry added with **Status** + **Unblock** + a
   `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → the entry is removed (the ADR / docs file carries the
   post-implementation reference).

Pending **decisions** (`D1` … `Dn`) live in this file until Florentin
calls them. Once decided, the decision becomes an ADR (or a
`/goal …` block) and the `D<n>` entry is removed.
