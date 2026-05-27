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

### Open — sans-io / determinism

- **`crates/magnetar/src/client.rs` (`Reader` subscription naming) +
  `crates/magnetar/src/table_view.rs`** —
  `Uuid::new_v4()` for default subscription names in the façade.
  Inject a random/id provider via Engine, or require explicit
  subscription names for `MoonpoolEngine`.
- **`crates/magnetar-auth-oauth2/src/lib.rs::SystemClock`** —
  `SystemClock::now()` is the production default for OAuth2's
  `Clock` trait; the same crate provides a `VirtualClock` in tests.
  Wire a `Clock` provider through the engine so the production path
  is actually injectable.
### Open — zero-copy

- **Batched-consumer per-message metadata clone** —
  `crates/magnetar-proto/src/consumer.rs::classify_and_queue` (batched
  delivery loop) — for each message in a batch (loop iterating
  `num_in_batch` times), `pb::MessageMetadata` and `BrokerEntryMetadata`
  are cloned into a fresh `IncomingMessage`. A 100-message batch =
  100 metadata clones of identical data. Wrap in
  `Arc<MessageMetadata>` so all messages in the batch share by Arc.
- **Chunked-message metadata clone** —
  `crates/magnetar-proto/src/consumer.rs::ChunkBuffer` (first-chunk
  arrival + final assembly) — metadata cloned on first-chunk arrival,
  then again on final assembly. Arc-wrap in `ChunkBuffer`, or move out
  (not clone) on assembly.
- **`crates/magnetar-proto/src/frame.rs::encode_payload`** — single
  `BytesMut` accumulator copies every payload into the wire buffer.
  Return a frame descriptor `{head: BytesMut, payload: Bytes}` and
  vectored-write for plaintext — the producer batch path then chains
  `Bytes` segments instead of memcpy-concat. TLS path keeps the
  contiguous coalesce.

### Open — performance / contention

- **`ConnectionShared::drain_memory_wakers` allocates a `Vec<Waker>`** —
  `crates/magnetar-runtime-tokio/src/lib.rs` — pre-allocate the
  scratch Vec in `ConnectionShared` and reuse, or drain directly
  without intermediate collect.
- **`pending_index: HashMap<SequenceId, usize>` uses SipHash** —
  `crates/magnetar-proto/src/producer.rs::ProducerState.pending_index`
  — key is a `u64` newtype. Switch to
  `nohash_hasher::NoHashHasher<u64>` or `ahash::AHashMap`.
- **`batch_ack_tracker: HashMap<(u64, u64), …>`** —
  `crates/magnetar-proto/src/consumer.rs::ConsumerState.batch_ack_tracker`
  — same SipHash overkill.
- **`ProducerState::refresh_pending_index` clears + rebuilds on every
  ack** — `crates/magnetar-proto/src/producer.rs` — O(in-flight) work
  per receipt. Use a `VecDeque` with monotonic head and slot
  generation.
- **`ack.rs` uses `HashSet` then drains-and-sorts** —
  `crates/magnetar-proto/src/trackers/ack.rs` —
  `BTreeSet<MessageId>` removes the post-drain sort allocation, or
  `SmallVec` with threshold-based sort for small batches.
- **`multi_topics.rs`, `pattern_consumer.rs` receive loops** — every
  `receive()` call clones the full consumer list and rebuilds a
  `Vec<Future>`. Keep an `Arc<[NamedConsumer]>` snapshot updated only
  on topology change.

### Open — syscall reduction

- **Explicit `flush()` after every `write_all`** —
  `crates/magnetar-runtime-tokio/src/driver.rs::driver_loop_inner` —
  for plaintext TCP, `flush()` is essentially a no-op; for TLS it can
  force extra record work. Skip flush on plaintext; flush only at
  batch boundaries.
- **No `writev` / `IoSlice`** —
  `crates/magnetar-runtime-tokio/src/driver.rs::driver_loop_inner` +
  `crates/magnetar-proto/src/conn.rs::poll_transmit` — outbound
  coalesces into a single `BytesMut` before write. With a `Transmit`
  enum of contiguous-or-vectored segments and `poll_write_vectored`,
  the plaintext path avoids the batch copy entirely (TLS coalesces
  at the rustls boundary). Moonpool parity: `Providers::Network`
  accepts segment list, records equivalent byte stream.
- **Read path double-copy** —
  `crates/magnetar-runtime-tokio/src/driver.rs::driver_loop_inner`
  reads `read_buf` → `split().freeze()`. The proto-side re-copy was
  removed by the `handle_bytes` `split_to` refactor (commit
  `bf66a5b`). Once the segment-aware transmit type lands, the runtime
  can pass owned `BytesMut` ownership directly.

### Open — security hardening

- **Athenz private key as `String`** —
  `crates/magnetar-auth-athenz/src/lib.rs` — wrap parsed key in
  `zeroize::Zeroizing<…>` (ADR-0030 lists this as deferred to
  v0.2.0).

### Open — cleanup and structural clarity

- **`ProducerExt` trait, single impl** —
  `crates/magnetar/src/client.rs::ProducerExt`. Inline as a direct
  method on `magnetar_runtime_tokio::Producer`.
- **`ProducerBuilder<'a, E>` / `ConsumerBuilder<'a, E>` /
  `ReaderBuilder<'a, E>` are 95% tokio-bound** — phantom `E`
  parameter on builder methods that ignore it. Move the generic only
  to the final `.create()` / `.subscribe()` dispatch.
- **Large modules: `client.rs`, `engine.rs`, `conn.rs`** — split
  candidates. `conn.rs` could shed `txn.rs`, `dlq.rs`,
  `anti_thrash.rs` satellites (~500 lines each). `client.rs` could
  move builders to `builders.rs`. `engine.rs` could become
  `engine/{traits,tokio,moonpool}.rs`.
- **Test-helper duplication** — `handshake_response_bytes()` defined
  in multiple test files across both runtimes. Consolidate (or
  document intentional per ADR-0024).

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

Lifting these to the engine-generic `impl<E: Engine> PulsarClient<E>`
block needs the matching `BrokerMetadataApi` / partition-count
lookups already present on both engines and a small amount of
plumbing to surface tokio-only specialised methods
(`refresh_partitions`, `last_sequence_id_published`) via a
specialisation block. The inner builders are already engine-generic
so the lift is mostly mechanical.

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

**Status.** `AthenzProvider::with_role_token` ships (callers that
already hold a valid ZTS role token can hand it directly to the
provider). `AthenzProvider::new(...).initial` returns
`AuthError::Unsupported`.

**Unblock.** Deferred to v0.2.0 per
[ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
§D3 and [ADR-0030](../specs/adr/0030-athenz-zts-round-trip-scope.md).
The work item is implementing a minimal `reqwest`-backed ZTS client
that exchanges the tenant private key for a role token, caches it
with an expiry-aware refresh, and surfaces failures through
`AuthError`. Scope is ~400–600 LOC plus a Dockerised ZTS fixture
(`athenz/athenz-zts-server`) for the e2e suite.

```text
/goal land Athenz ZTS round-trip in magnetar-auth-athenz. Implement a reqwest-backed ZTS client that signs a token request with the tenant private key, caches the response with expiry-aware refresh, and uses it as the `auth_data` payload from `AthenzProvider::initial`. Add a Dockerised ZTS fixture behind the `e2e` feature, and flip the README parity matrix row from 🟡 to ✅. Test layers per ADR-0024.
```

---

## Protocol — open v0.2.0 PIP wave

The v0.2.0 planning pass produced four per-PIP proposals under
[`specs/proposals/`](../specs/proposals/) authorised by ADRs 0031–0034.
Status snapshot:

| PIP | Upstream | v0.2.0 status |
| --- | --- | --- |
| PIP-33 — Replicated subscriptions | 🟢 LIVE (Pulsar 2.4, 2019) | ✅ landed — see [ADR-0034](../specs/adr/0034-pip-33-replicated-subscriptions-scope.md) + [`docs/replicated-subscriptions.md`](replicated-subscriptions.md) |
| PIP-180 — Shadow topic | 🟢 LIVE (Pulsar 2.11, 2023) | ✅ landed — see [ADR-0033](../specs/adr/0033-pip-180-shadow-topic-scope.md) + [`docs/shadow-topic.md`](shadow-topic.md) |
| PIP-466 — V5 client surface | 🟠 DESIGN-PHASE (Java V5 still iterating; magnetar v0.2.0 surface is a v4-wire skin) | ⌛ unblocked — mirrors existing v4 e2e; `/goal` below |
| PIP-460 — Scalable topics | 🔴 NOT LIVE (PIP `Draft`; targets Pulsar 5.0 LTS, Oct 2026; phased 4.3.0 / 4.4.0) | ⏸ blocked — needs `apachepulsar/pulsar:5.0.0-rc-*` |

### PIP-180 post-landing follow-ups

- **Subscribe-time admin REST hint integration (façade-level)** —
  the runtime engines expose `Consumer::set_shadow_source(...)` but
  do NOT call the admin REST `get_shadow_source(topic)` automatically
  at `subscribe()` time. Today the caller threads the source-topic
  hint in by hand (or via the magnetar façade above the runtime,
  which has `magnetar-admin` available behind the `admin` feature).
  A clean addition would be a `Client::subscribe_shadow_aware(...)`
  on the magnetar façade that performs the lookup when the `admin`
  feature is active.
- **Post-subscribe shadow-metadata cache race** — the per-`Consumer`
  shadow metadata is resolved once at subscribe time and cached
  for the consumer's lifetime. If a shadow is created on a topic
  AFTER a consumer subscribed to it, the consumer will not pick up
  the new shadow attachment until it re-subscribes. Documented in
  [`shadow-topic.md`](shadow-topic.md) §Caveats. Low priority —
  operators inspect via `magnetar shadow list <source>`.
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
No proto change — V5 is a v4-wire skin. Estimate ~1080 LOC. Upstream
Java V5 is still iterating, hence the experimental tag — but magnetar's
surface works against current Pulsar 4.x brokers since it ultimately
sends the v4 commands.

**Ships in v0.2.0.** `magnetar::v5` module behind
`feature = "experimental-v5-client"` (default off) exposing
`PulsarClientV5<E>`, `v5::Producer<T, E>`, `v5::StreamConsumer<T, E>`,
`v5::QueueConsumer<T, E>`. Each is a thin wrapper holding the
corresponding v4 type. V5 `Reader`, `TableView`, `Transaction`,
`CheckpointConsumer` are explicit v0.3.0+.

```text
/goal implement PIP-466 V5 client surface per specs/proposals/pip-466-v5-client-surface.md and ADR-0032. No wire change. No sans-io change. No new `Event` variant. The V5 surface is a thin skin over v4 — internally delegates every call. Waves: (1) `magnetar/Cargo.toml` add `experimental-v5-client = []` feature (default OFF); `magnetar/src/lib.rs` add `#[cfg(feature = "experimental-v5-client")] pub mod v5;`; (2) `magnetar/src/v5/mod.rs` (NEW) + submodules `client.rs`, `producer.rs`, `stream_consumer.rs`, `queue_consumer.rs`; (3) `magnetar/src/v5/mapping.rs` (NEW) — single source-of-truth table of V5→v4 field translations: send_timeout: Duration → ms u64 (default 30s); max_pending_messages: Option<usize> → usize with None=0 (default Some(1000)); ack_timeout: Option<Duration> → ms u64 with None=0 (default None); negative_ack_redelivery_delay: Duration → ms u64 (default 60s); receiver_queue_size: usize direct (default 1000); subscription_initial_position direct; (4) `PulsarClientV5<E: Engine>` wraps `Arc<E::ClientState>`; exposes `v4() -> PulsarClient<E>` escape hatch with the SAME state (no double init); (5) `v5::Producer<T, E>` holds `crate::Producer<T, E>`; signatures use Duration + Option<MessageId> return on send; (6) `v5::StreamConsumer<T, E>` → v4 Consumer with SubscriptionType::Exclusive / Failover; `v5::QueueConsumer<T, E>` → v4 with Shared / KeyShared; (7) every public V5 type carries `#[doc = "**Experimental** — PIP-466 V5 client surface (v0.2.0). Behaviour and signatures may change before V5 is promoted to default."]`. Test layers per ADR-0024 — claim and JUSTIFY two exemptions in the commit body via `test-exemption-proto: PIP-466 V5 surface (no wire/sans-io change)` and `test-exemption-differential: PIP-466 V5 surface (no new sans-io surface)`. Required layers: (b) `crates/magnetar/tests/v5_*.rs` — 5 files (`v5_producer_mapping.rs`, `v5_stream_consumer_mapping.rs`, `v5_queue_consumer_mapping.rs`, `v5_client_v4_escape_hatch.rs`, `v5_builder_defaults.rs` table-driven from mapping.rs), each asserting the wire bytes magnetar-fakes observes match the v4 expectation; (c) `crates/magnetar/tests/v5_*_moonpool.rs` — same five files mirrored 1:1 under SimulationBuilder. NO new moonpool BrokerWorkload variant (the v4 fakes already cover it). NO new differential test (v4 differential already covers the wire). E2E: 3 mirror tests under `crates/magnetar/tests/e2e_pulsar_v5.rs` + `e2e_sub_types_v5.rs` parameterising existing e2e patterns against Pulsar 4.0.4 — gated `feature = "e2e,experimental-v5-client"`. Docs: `docs/v5-client.md` (NEW including the mapping table), parity-status.md row → 🟡 experimental, README parity matrix row, flip ADR-0032 to Accepted. Full validation chain incl. `check-crypto-matrix` (V5 × crypto axis).
```

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
