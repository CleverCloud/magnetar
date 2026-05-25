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

---

## What landed (since the multi-source design synthesis)

ADR-0026 locked four design decisions (D1–D4) on 2026-05-23. The
binding rationale, sources, and decision text live in that ADR;
this section only tracks shipping status:

- **D4** — `xtask vendor-proto --rev <sha>` (commit `ac1420c`).
- **D3** — SASL `PLAIN` ✅ + Athenz pre-fetched role token ✅ ship;
  SASL Kerberos/GSSAPI 🟡 + Athenz ZTS round-trip 🟡 deferred to
  v0.2.0 (commit `96d6f74`).
- **D2** — `crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`
  first cut: BrokerWorkload + ClientWorkload under
  `SimulationBuilder`, 16-seed sweep (commit `c23f6fd`). Follow-on:
  extend broker workload with SEND / SUBSCRIBE / SEEK / ACK; add
  invariants (at-least-once, monotonic message-id, no-dup-on-acked,
  supervisor-recovers-within-N-ticks).
- **D1 Transaction surface** — `impl<E: Engine + TransactionApi>
  PulsarClient<E>` works on both engines (commits `1258b89` +
  D1 phase 2-4 commit + `ab9041b` parity flip).


## Moonpool engine — implementation backlog

### Façade surface bound to `PulsarClient<MoonpoolEngine<P>>`

**Status.** Partitioned producer / partitioned consumer /
MultiTopicsConsumer / PatternConsumer / Reader / TableView /
transactions / typed schemas do not compile against the moonpool
engine. Each surface needs:

1. The corresponding moonpool `Client` method ported from
   `magnetar-runtime-tokio` (`new_txn`, `add_partition_to_txn`,
   `end_txn`, partitioned-metadata lookup, etc.). ~150–300 LOC per
   surface.
2. The façade method made generic over `Engine`, dropping its
   `impl PulsarClient<TokioEngine>` block.
3. All four test layers per ADR-0024 + e2e where applicable.

ADR-0026 §D1 locked the trait shape: **concrete generic types
`magnetar::<Surface><T, E: Engine>`**, not `Engine::Producer<T>` /
`Engine::Consumer<T>` GATs. The Engine trait stays at ADR-0025
phase 1 surface (task + timer primitives); per-surface lifts add
their own engine-agnostic indirection (e.g. an
`EngineTransactionApi` extension trait implemented per engine on
its own `Client` type).

See [ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
§D1 + [ADR-0019](../specs/adr/0019-engine-scope-and-moonpool-parity.md)
§Consequences.

#### Landed — Transaction + Reader + TableView surfaces

**Transaction (PIP-31).** `new_transaction` /
`register_partition_to_transaction` /
`register_subscription_to_transaction` / `commit_transaction` /
`abort_transaction` lifted to `impl<E: Engine + TransactionApi>
PulsarClient<E>`. Both `PulsarClient<TokioEngine>` and
`PulsarClient<MoonpoolEngine<P>>` carry the surface.

**Reader.** `Reader<C: ConsumerApi>` with default
`C = magnetar_runtime_tokio::Consumer` (existing callers
unchanged). Generic methods route through the trait;
tokio-engine-specific methods (`read_next_with_timeout`,
`read_next_fut`, `close`, `seek_to_earliest`) stay on the tokio
specialisation.

**TableView.** `TableView<C: ConsumerApi + Clone>` with the same
default-type-arg pattern. The drain task uses `tokio::spawn`
regardless of engine (per ADR-0025: both engines schedule on
tokio; determinism comes from substituting providers, not from
replacing the executor). `TableView::stats()`,
`TableView::is_connected()`, `TableView::last_message_id()`
dispatch through `ConsumerApi`.

**Producer/Consumer extension traits.** `ProducerApi` + `ConsumerApi`
defined in `magnetar::engine`, implemented by both runtimes on
their `Producer<P>` / `Consumer<P>` types. Trait surface grew
through the lift train; current methods:

- `ProducerApi`: `send`, `flush`, `is_closed`, `is_connected`,
  `topic`, `name`, `last_sequence_id`, `get_schema`.
- `ConsumerApi`: `receive`, `ack`, `ack_cumulative`, `negative_ack`,
  `last_message_id`, `has_message_after`, `get_schema`, `topic`,
  `subscription`, `name`, `is_closed`, `is_connected`, `stats`.

`magnetar_runtime_moonpool::Consumer` derives `Clone` (required by
TableView). Compile-time bound checks live in
`magnetar/src/lib.rs` tests. ADR-0024 test parity: tokio=95
moonpool=95 preserved.

#### Why an extension trait, not a method on `Engine`

Pulled forward from ADR-0026's rationale: the methods that operate on
the client state are not "engine primitives" (those are spawn / timer /
clock — ADR-0025 phase 1). They are **client surfaces**. Putting them
on the engine trait would mean every engine grew a method per Pulsar
PIP forever. An extension trait per surface family scales: each PIP
adds at most one trait, each engine implements only the surfaces it
supports, and the façade still gets `impl<E: Engine>` because the
trait bound is `E::ClientState: TransactionApi + ProducerApi + ...`.

#### Next sub-PR — ConsumerBuilder / ProducerBuilder genericity (unblocks 4 phantom-lifted surfaces)

All seven dependent façade surfaces now carry their engine-generic
type parameter (Transaction, Reader, TableView, PartitionedProducer
have full impl-body lifts; TypedProducer/TypedConsumer,
MultiTopicsConsumer/PartitionedConsumer, PatternConsumer are
phantom-lifted with impl-body still tokio-bound). The four
phantom-lifted surfaces share one blocker:
**`ConsumerBuilder` / `ProducerBuilder` are tokio-bound today**.

The blocker shape — `MultiTopicsConsumer::add_topic` (and the
PIP-145 reconciliation loop in `PatternConsumer::update`)
subscribes new children via:

```rust,ignore
let builder = self.inner.template.apply(client.consumer(topic.clone()));
let consumer = builder.subscribe().await?;
```

`client.consumer(topic)` is `PulsarClient<TokioEngine>::consumer()`
which returns `ConsumerBuilder<'_>` — internally bound to the tokio
`SubscribeRequest`, `MessageDecryptor`, and ultimately
`magnetar_runtime_tokio::Client::subscribe()`. The Builder lift
makes the entire chain engine-generic.

**The lift template (mirrors the surface lifts already landed):**

1. **Lift `ConsumerBuilder` to `ConsumerBuilder<'a, E: Engine>`**
   parameterised over the engine, with default `E = TokioEngine`.
   Existing callers (`client.consumer(topic)`) continue compiling
   via the default-type-argument fallback.
2. **Add a `SubscribeApi` extension trait on `E::ClientState`** with
   one method:
   `fn subscribe(&self, req: SubscribeRequest, decryptor: Option<...>)
   -> impl Future<Output = Result<C, ClientError>>`. Implement on
   both runtime `Client` types — both already have the equivalent
   method.
3. **`ConsumerBuilder::subscribe()`** dispatches through the trait.
   Returns `impl Future<Output = Result<<E::ClientState as
   SubscribeApi>::Consumer, ...>>`.
4. **Same template for `ProducerBuilder`** with `CreateProducerApi`
   trait + `Client::open_producer` delegate.
5. **`Reader::create()`** (already lifted) becomes generic over the
   Builder's `E`.
6. **The four phantom-lifted surfaces' impl-body lifts**
   (TypedSchemas, MultiTopicsConsumer, PartitionedConsumer,
   PatternConsumer) become mechanical: each method that used to
   call `client.consumer(topic).subscribe()` now dispatches
   through `<E::ClientState as SubscribeApi>::subscribe`.
7. **Test parity per ADR-0024** — each new trait method needs a
   1:1 mirror test on both runtime sides.
8. **Parity-status rows flip** to ✅/✅ once each surface's
   impl-body is fully lifted.

**Sans-io invariant**: same as the surface lifts — trait surface
uses `Pin<Box<dyn Future + Send + '_>>` with no I/O types;
`magnetar-proto` carries no new deps.

Surface-specific notes (post-Builder genericity):

- **TypedSchemas** (`TypedProducer<S, P>` / `TypedConsumer<S, C>`).
  Phantom-lifted in commit `6a83ea2`. Helper methods needed on
  trait surface: `compression`, `last_sequence_id_published`,
  `pending_count`, `batch_len`, `batch_bytes` (Producer side);
  `ack_grouped`, `ack_grouped_cumulative`, `available_in_queue`,
  `available_permits`, `drain_dead_letter`,
  `has_reached_end_of_topic`, `has_received_any_message`,
  `is_inactive`, `is_paused`, `receive_batch`,
  `receive_with_timeout`, the `ack_with_txn` family (Consumer
  side). All need moonpool ports before adding to trait.
- **MultiTopicsConsumer** (`MultiTopicsConsumer<C>`). Cascading
  phantom-lift in commit `b51680a`. Needs Builder genericity for
  `add_topic` and the `auto_update` reconciliation. The
  `pause` / `resume` family is already on moonpool.
- **PartitionedConsumer**. Type alias for `MultiTopicsConsumer`;
  lifts transitively once `MultiTopicsConsumer` lifts.
- **PatternConsumer** (`PatternConsumer<C>`). Cascading
  phantom-lift in commit `31f9cbe`. Same blocker as
  MultiTopicsConsumer.

```text
/goal lift `ConsumerBuilder` + `ProducerBuilder` to be engine-generic. Step 1: add `SubscribeApi` extension trait (one method: `subscribe(req, decryptor) -> Result<Consumer, Error>`) and `CreateProducerApi` extension trait (one method: `open_producer(req) -> Result<Producer, Error>`) in `magnetar::engine`. Delegate impls on `magnetar_runtime_tokio::Client` (existing inherent methods) and `magnetar_runtime_moonpool::Client` (existing inherent methods). Step 2: lift `ConsumerBuilder<'a>` to `ConsumerBuilder<'a, E: Engine = TokioEngine>` and `ProducerBuilder<'a>` similarly; route `.subscribe()` / `.create()` through the new trait. Step 3: lift `Reader<C>::create` to be generic over `E`. Step 4: lift the impl-bodies of `TypedProducer<S, P>`, `TypedConsumer<S, C>`, `MultiTopicsConsumer<C>`, `PatternConsumer<C>` to dispatch via the new traits; for methods that need helpers not on `ConsumerApi`, split into tokio-specialisation impl blocks (same pattern PartitionedProducer used). Step 5: test parity per ADR-0024 — mirror tests on both runtime sides. Step 6: parity-status + README row flips for the four phantom-lifted surfaces. Validation: `cargo +nightly fmt && cargo build --workspace --all-features && cargo clippy --workspace --all-features --all-targets -- -D warnings && cargo run -p xtask -- check-runtime-test-parity && cargo run -p xtask -- check-no-channels && cargo run -p xtask -- check-no-io-deps && RUSTDOCFLAGS="-D warnings --cfg tokio_unstable" cargo doc --workspace --all-features --no-deps`.
```

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

**Unblock.** Closed by [D2 — vendor moonpool-sim](#d2--vendor-moonpool-sim-into-the-workspace);
the simulator's deterministic scheduler drives both sides without
`spawn_local`. An alternative is restructuring the runner to spawn the
driver via plain `tokio::spawn`, giving up moonpool-sim compatibility
for the differential harness specifically.

### Expand the golden-trace catalog

**Status.** The harness ships six golden traces (round-trip, batch,
nack-redelivery, seek-to-start, many-publishes, lookup-before-open).
Missing: seek-per-partition, transactional ack paths, the
`cryptoFailureAction` matrix.

**Unblock.** Each new trace extends the scripted broker as needed (the
broker speaks a deliberately minimal subset of the wire protocol; new
opcodes get added per trace). Seek-per-partition is the smallest
(~120 LOC; broker tracks partition id, dispatches `Seek` by partition).
Transactional ack needs `CommandEndTxn` + per-txn ack ledger in the
broker (~180 LOC); blocked on [D1](#d1--engine-trait-extension-adr-0025)
because the txn façade only compiles on tokio today.
`cryptoFailureAction` is the largest (~240 LOC) and needs the crypto
bridge ported to moonpool first.

```text
/goal land golden-trace seek-per-partition in magnetar-differential. Single commit. Extends ScriptedBroker.SessionState with per-partition message_id routing, adds Op::SeekPartition variant + Event::SeekedPartition, and a 3-step trace asserting tokio and moonpool agree on per-partition seek replay. All four test layers per ADR-0024.
```

---

## Testing + coverage

### Cross-runtime test + coverage closure (ADR-0024)

**Status.** [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md)
landed 2026-05-22 with both `cargo xtask check-sim-coverage` and
`cargo xtask check-runtime-test-parity` enabled and hard-failing. The
2026-05-22 baseline was `tokio=65 moonpool=61` (gap of 4); subsequent
landings (memory-limit slab, AutoClusterFailover moonpool port, TLS
chaos fixtures, race-stress coverage, lookup-before-open) brought it
to `tokio=91 moonpool=91`. Pre-existing moonpool patch-coverage of
older surface lines is unmeasured today.

**Unblock.** Dedicated session driven by the local prompt at
`tasks/coverage-closure-prompt.md` (gitignored). Phases:
(1) bring tokio↔moonpool counts to 1:1 — **done**;
(2) close pre-existing moonpool coverage gaps file by file using the
`cargo llvm-cov --html` report; (3) full validation chain green
including 32-seed sweep. ADR-0021 still applies — failing tests are
fixed, not `#[ignore]`-d.

```text
/goal close the pre-existing moonpool coverage gap. Generate cargo llvm-cov --html, identify the largest uncovered hunks in crates/magnetar-runtime-moonpool/src/{driver,producer,consumer,lib,transport}.rs, add targeted tests to crates/magnetar-runtime-moonpool/tests/ until check-sim-coverage reports no uncovered lines against origin/main. Keep test parity 1:1; mirror each new moonpool test on the tokio side so the gate stays green.
```

---

## Auth

### SASL Kerberos / GSSAPI

**Status.** `magnetar_auth_sasl::SaslKerberos::initial` returns
`AuthError::Unsupported`. SASL `PLAIN` (RFC 4616) is fully wired and
ships in v0.1.0; the Kerberos/GSSAPI mechanism is the deferred portion.

**Unblock.** Deferred to v0.2.0 per
[ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
§D3. The work item is binding `libgssapi-sys` (or the safer wrapper crate
`libgssapi`) into `SaslKerberos::initial` and threading the
`AUTH_CHALLENGE` continuation through the existing
`AuthProvider::respond_to_challenge` surface. Scope is ~600–900 LOC plus
a Dockerised KDC fixture for the e2e suite.

```text
/goal land SASL Kerberos / GSSAPI in magnetar-auth-sasl. Bind libgssapi to SaslKerberos::initial, thread AUTH_CHALLENGE continuations through AuthProvider::respond_to_challenge, add a Dockerised KDC fixture (testcontainers + bitnami/kerberos) behind the `e2e` feature, and flip the README parity matrix row from 🟡 to ✅. All four test layers per ADR-0024 — the GSSAPI calls themselves go behind a `kerberos` feature flag so the sans-io test layers can mock the wire bytes deterministically.
```

### Athenz ZTS round-trip

**Status.** `AthenzProvider::with_role_token` ships in v0.1.0 (callers
that already hold a valid ZTS role token can hand it directly to the
provider). `AthenzProvider::new(...).initial` returns
`AuthError::Unsupported`.

**Unblock.** Deferred to v0.2.0 per
[ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
§D3. The work item is implementing a minimal `reqwest`-backed ZTS
client that exchanges the tenant private key for a role token,
caches it with an expiry-aware refresh, and surfaces failures through
`AuthError`. Scope is ~400–600 LOC plus a Dockerised ZTS fixture
(`athenz/athenz-zts-server`) for the e2e suite.

```text
/goal land Athenz ZTS round-trip in magnetar-auth-athenz. Implement a reqwest-backed ZTS client that signs a token request with the tenant private key, caches the response with expiry-aware refresh, and uses it as the `auth_data` payload from `AthenzProvider::initial`. Add a Dockerised ZTS fixture behind the `e2e` feature, and flip the README parity matrix row from 🟡 to ✅. Test layers per ADR-0024.
```

---

## Protocol

### PIP-460 scalable topics, PIP-466 V5 surface, PIP-180 shadow topic, PIP-33 replicated subscriptions

**Status.** Out of scope for v0.1.0 per
[ADR-0010](../specs/adr/0010-v0-1-full-java-parity.md). PIP-466 is
"inspired by, not adopted verbatim" — magnetar's public API takes
PIP-466's clean-room style (immutable builders, no
`with*` setter chains) without binding to its exact wire surface.

**Rationale per PIP.**

- **PIP-460 — Scalable subscription model.** Upstream itself carries
  the **experimental** tag (Apache Pulsar 4.0.x). The wire surface
  (`CommandTopicMigrated` variants, subscription-state metadata,
  bundle-split coordination) is still iterating. Binding magnetar to
  the current draft would tie the client to a moving target. **Defer
  to v0.2.0** once upstream stabilises the surface.

- **PIP-466 — V5 client surface.** This is an **API-shape decision**,
  not a wire-protocol change. Magnetar's user-facing API
  (`PulsarClient::builder()`, `ProducerBuilder`,
  `ConsumerBuilder`, immutable configs, `Option<…>` over
  default-bearing setters) already follows PIP-466's spirit. Verbatim
  adoption would mean re-naming method receivers to match Java's V5
  conventions (e.g. `Producer.newMessageAsync()` → `producer.send_async()`
  vs current `producer.send()`). **No wire change required**; decision
  is whether to chase the rename for surface parity.

- **PIP-180 — Shadow topic.** Adds a read-only follower topic that
  mirrors a primary. The wire surface is small (one new
  subscription mode + a metadata flag on `CommandSubscribe`), but
  the consumer-side semantics (no-acks, no-seek, redelivery rules)
  are subtle. **Defer to v0.2.0** as a low-priority feature — primary
  use case is cross-region read fan-out, which Clever Cloud's roadmap
  has not asked for.

- **PIP-33 — Replicated subscriptions.** Subscription state is
  replicated across geo-replicated topics so a consumer can resume on
  a different cluster after failover. Wire-protocol additions:
  per-snapshot markers in the data stream + `CommandReplicatedSubscription
  Snapshot{Request,Response}`. Substantial broker-side coupling.
  **Defer to v0.2.0**; magnetar's geo-replication story (via
  `ServiceUrlProvider` + `AutoClusterFailover`) covers most of the
  use case at the cluster level without subscription-state replication.

**Unblock.** Scoped for v0.2.0. None of these PIPs blocks v0.1.0
parity per [ADR-0010](../specs/adr/0010-v0-1-full-java-parity.md);
all four can land independently as v0.2.0 follow-ups.

```text
/goal scope PIP-460 / PIP-466 / PIP-180 / PIP-33 for v0.2.0. Per PIP, produce: (1) the wire-protocol delta against the current vendored PulsarApi.proto, (2) the magnetar-proto state-machine additions, (3) the runtime-tokio + runtime-moonpool surface ports, (4) the four-layer test plan per ADR-0024, (5) the e2e plan against apachepulsar/pulsar:4.x. Produce one planning doc per PIP under specs/proposals/. No code yet — these are planning passes.
```

---

## E2E-discovered runtime bugs

After the test-fixture bootstrap fixes (#55) and the wire-protocol
bugs surfaced by the e2e sweep, the state is:

- **#56** producer rejected after broker reconnect — **PARTIAL** (commit
  on main): broker-initiated `CommandCloseProducer` no longer flips
  `closed=true`, so `rebuild_producers` can re-attach. Remaining
  issue: post-restart send doesn't get a receipt — tracked as **#66**.
- **#57** chunked send hangs — **FIXED**. Per-chunk payload size now
  reserves 1 KiB for the wire-frame overhead (matches Java's
  `chunkMaxMessageSize`).
- **#58** KeyValue inline schema hangs — **FIXED**. Schema_data is now
  emitted in the Java-compatible binary layout
  (`[u32 len][bytes][u32 len][bytes]`) and the seven KeyValue
  properties are populated on `CommandProducer.schema.properties`.
- **#63** PIP-4 encryption (roundtrip / failure actions / chunking)
  — **FIXED**. Combination of #57 (chunked frame size) + the
  `InitialPosition::Earliest` test-fixture fix on the crypto e2e
  tests.
- **#65** seek_per_partition resubscribe — **PARTIAL** (commit on main).
  Three coupled fixes landed: duringSeek drop in `Consumer::deliver`,
  transient `CommandCloseConsumer` (matches #56), explicit
  `CommandRedeliverUnacknowledgedMessages` after resubscribe. Broker
  still doesn't dispatch post-seek backlog when the same `consumer_id`
  re-subscribes — likely needs fresh `consumer_id` allocation, tracked
  as **#67**.
- **#68** e2e_transactions (PIP-31, all three tests) — **FIXED**. Four
  Java-parity gaps closed in `fix/txn-ttl-millis`:
  1. `txn_ttl_seconds` is actually milliseconds on the wire (Pulsar
     `TransactionMetadataStoreService.newTransaction(tcId, timeoutInMills,
     ...)` passes `command.getTxnTtlSeconds()` directly into
     `timeoutInMills`). magnetar used to divide by 1000, so 30 s arrived
     at the broker as 30 ms and the TC auto-aborted before the next
     RPC. Fix: stop the conversion; the docstring on
     `magnetar_proto::txn::TxnClient::new_txn` now warns about the
     mis-named field.
  2. The TC partition store is loaded on demand. The first
     `CommandNewTxn` against a fresh broker hit
     `TransactionMetadataStoreService.stores.get(tcId) == null` and
     returned `TransactionCoordinatorNotFound`. Fix: new
     `Connection::tc_client_connect(tc_id)` mirrors Java's
     `TransactionMetaStoreHandler.connectionOpened` →
     `Commands.newTcClientConnectRequest`. `Client::new_txn` runs a
     one-shot bootstrap (`lookup_topic` then `tc_client_connect`)
     guarded by `ConnectionShared::txn_bootstrapped`; subsequent calls
     skip it. Lookup alone is not enough — bundle ownership transfer is
     async; the `TC_CLIENT_CONNECT_REQUEST` round-trip is what waits for
     `handleMetadataStoreLoad(tcId)`.
  3. Batched sends dropped the txn id. The flush path hard-coded
     `CommandSend.txnid_*: None`, so any `send().await` of a txn
     message that hit `add_to_batch` bypassed `TransactionBuffer`.
     `BatchContainer` now carries `txn_id`; `queue_send` flushes when
     a non-matching `txn_id` arrives (mirrors Java
     `ProducerImpl.canAddToBatch`).
  4. Java's `TypedMessageBuilderImpl#beforeSend` also stamps the txn
     bits on `MessageMetadata` — the broker's `TopicTransactionBuffer`
     routes off the metadata, not `CommandSend`. Without those bits,
     entries went straight to the dispatcher and aborts couldn't
     suppress delivery (the failing
     `e2e_txn_abort_drops_messages`). Fix: set
     `metadata.txnid_least_bits` / `txnid_most_bits` in all three send
     paths (`emit_single`, `flush_batch`, `emit_chunked`).

  Result: `e2e_transactions` is 3/3 PASS. Implemented entirely on the
  tokio runtime; moonpool simulator does not exercise PIP-31 today.
- **#69** e2e_batch_chunk (all three tests) — **FIXED**. Three batch-
  related defects in `fix/batch-fullness-flush`:
  1. `BatchContainer` never emitted on fullness. `add_to_batch`
     buffered messages but the producer only flushed when
     `batching_max_publish_delay` (60 s in the e2e test) elapsed.
     Java's `ProducerImpl#doBatchSendAndAdd` triggers a flush the
     moment the container fills — added `flush_batch_if_full` invoked
     from `queue_send`.
  2. Every batched send was returned the same `seq_id` (the prior
     `last_sequence_id_pushed`), so the single `OpSend` pushed at flush
     time could only wake one of the N user-side `SendFut`s.
     `add_to_batch` now mints a per-message sequence id and pushes a
     per-message `OpSend(num_messages=1, replay_frames=[])`;
     `flush_batch` reuses `batch.lowest_sequence_id` /
     `highest_sequence_id` for the wire frame instead of bumping the
     counter again.
  3. Pulsar's broker echoes `highest_sequence_id = -1L` (encoded as
     `u64::MAX` over `optional uint64`) on receipts for non-batched
     sends. A naive `for seq in lowest..=receipt.highest_sequence_id`
     fan-out iterated up to `u64::MAX` and panicked with
     `capacity overflow` on the second single send. The receipt handler
     now treats `highest == u64::MAX || highest < lowest` as the
     "no batch" sentinel and resolves a single entry; only
     `highest >= lowest && highest != u64::MAX` triggers the real
     fan-out.

  The `e2e_producer_batching_flushes_on_max_msgs` test was also
  re-shaped to enqueue all 5 sends before awaiting any — mirrors Java
  `BatchMessageTest`'s "fire all `sendAsync`, then join" pattern; the
  sequential `await` would never fill the batch.

### Remaining follow-ups

### #64 — PartitionedProducer RoundRobin (CLOSED — misdiagnosis)

The earlier 0/20/20/0 distribution was caused by the e2e test
using `client.producer(topic)` instead of
`client.partitioned_producer(topic)`. With the corrected test
producer (`fix/seek-per-partition`) the broker shows backlog
10/10/10/10 across all 4 partitions — `RoundRobin` works correctly.

### #66 — Post-restart send doesn't get receipt

**Symptom.** `e2e_supervised_reconnect_across_broker_restart` no
longer fast-fails (the #56 close-flag fix removed the
`InvariantViolation`), but the post-restart `producer.send().await`
hangs waiting for a `CommandSendReceipt` that never arrives. The
test exhausts its 30 × 2 s retry budget on the send.

**Investigation done.**
- TCP reconnect works: testcontainers `stop_with_timeout` + `start`
  is observed by the supervised driver; `reset()` + `rebuild_producers`
  is called after the new handshake (see `driver.rs:362, 502`).
- Broker re-creates the producer (visible in broker logs).
- The first `CommandSend` after reconnect carries the same
  `producer_name` and an incremented `epoch`, but no receipt comes
  back.

**Open hypotheses.**
- `in_flight_publish_snapshots` (conn.rs:846) re-injects replay
  frames with the original `sequence_id`. If the first new send
  after reconnect happens BEFORE the replay drains, the broker may
  see a `sequence_id` gap or duplicate.
- Magnetar's `epoch` bump on `rebuild_producers` may not match what
  the broker's `ProducerImpl.epoch` validation expects.
- The reset path may be flushing the user's pre-reset `OpSend`
  wakers in a way that the future re-registers but never gets fired
  by the receipt (the receipt's `apply_receipt` finds nothing in
  `pending` if the snapshot was already drained).

**Fix steps.**
1. Set up a TCP proxy between magnetar and the broker that logs
   every frame in both directions.
2. Compare the post-reconnect CommandSend wire bytes vs what Java's
   `ProducerImpl#resendMessages` emits.
3. Pay particular attention to `epoch`, `sequence_id`,
   `highest_sequence_id`, and the `metadata.producer_name`.

**Repro.**

```sh
cargo test -p magnetar --features e2e --test e2e_reconnect -- --include-ignored --test-threads=1 --nocapture
```

### #67 — Fresh consumer_id refactor for post-seek resubscribe

**Symptom.** After landing the three-part fix for #65 (duringSeek
drop + transient close + redeliver), the broker confirms `backlog 5`
on `partition-0` after the seek (cursor reset to mid_ms point), but
no message dispatches to the re-subscribed consumer with the same
`consumer_id`.

**Investigation done.** `magnetar_proto::conn::Consumer::deliver` is
never called after the resubscribe — broker isn't dispatching even
though `Created subscription` lands in the broker log post-resub.

**Open hypothesis.** The Pulsar broker tracks "pending acks" per
`consumer_id` even after the disconnect; on resubscribe with the
same `consumer_id`, the broker thinks the previously-dispatched
messages are still owed and refuses to push new ones from the
just-reset cursor.

**Fix steps.**
1. Refactor `resubscribe_consumer_after_seek` to allocate a fresh
   `consumer_id` (via `next_consumer_id`).
2. Re-key `Connection::consumers`, `consumer_subscribe_requests`,
   and any pending receive-wakers to the new handle.
3. Add a per-handle `current_remote_id` indirection so the
   user-facing `Consumer` keeps holding its original
   `ConsumerHandle` while incoming messages are routed to the new
   id.
4. Validate `e2e_seek_per_partition_callback` against
   `apachepulsar/pulsar:4.0.4`.

**Repro.**

```sh
cargo test -p magnetar --features e2e --test e2e_seek_per_partition -- --include-ignored --test-threads=1
```

---

## Notes on this file

Items move from this file to git history when their commit lands. The
expected churn pattern:

1. New gap surfaces → entry added with **Status** + **Unblock** + a
   `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → the entry is removed (or its **Status** is updated to
   "landed by `<commit-sha>`" if a follow-on tracker is needed).

Pending **decisions** (`D1` … `Dn`) live in this file until Florentin
calls them. Once decided, the decision becomes an ADR (or a
`/goal …` block) and the `D<n>` entry is removed.
