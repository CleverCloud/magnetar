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

## Closed design decisions — see ADR-0026

D1 (Engine trait extension), D2 (moonpool-sim wiring), D3 (SASL/Athenz
scope), D4 (vendored-proto bump cadence) were all locked by
[ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
on 2026-05-23 after multi-source synthesis (FoundationDB simulator
docs, Apache Pulsar Java client architecture, Codex independent
review). Summary:

- **D1.** Façade surface lifts use **concrete generic types
  `magnetar::<Surface><T, E: Engine>`**, not `Engine::Producer<T>` /
  `Engine::Consumer<T>` GATs. The trait stays at ADR-0025 phase 1
  (task + timer primitives). Matches Apache Pulsar Java client's
  shape: shared infrastructure + concrete generic surfaces.
- **D2.** Implement a **pure-sim chaos suite** at
  `crates/magnetar-runtime-moonpool/tests/sim_chaos.rs` using
  `moonpool_sim::SimulationBuilder` + an in-simulator broker stub.
  Differential harness untouched. ADR-0024 exemption rationale in
  the commit message.
- **D3.** SASL/Kerberos + Athenz/ZTS deferred to v0.2.0.
  README parity matrix amended to surface partial coverage honestly
  (PLAIN + pre-fetched token are ✅; full GSSAPI / ZTS are 🟡).
- **D4.** Implement `xtask vendor-proto --rev <sha>` immediately;
  proto bumps are milestone-driven, not rolling-master.

The implementation work is queued in the "implementation backlog"
sections below; each entry now carries a `/goal …` block ready to
copy-paste into a fresh session.


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

#### Landed — Transaction surface

`magnetar::Transaction` + `new_transaction` /
`register_partition_to_transaction` /
`register_subscription_to_transaction` / `commit_transaction` /
`abort_transaction` lifted to `impl<E: Engine> PulsarClient<E>
where E::ClientState: TransactionApi` in the D1 phase 2-4 commit.
Both `PulsarClient<TokioEngine>` and
`PulsarClient<MoonpoolEngine<P>>` carry the surface; parity-status
flipped to ✅/✅; ADR-0024 test parity preserved (tokio=95
moonpool=95, with 4 tokio + 4 moonpool + 1 magnetar-side compile-
bound check added).

#### Why an extension trait, not a method on `Engine`

Pulled forward from ADR-0026's rationale: the methods that operate on
the client state are not "engine primitives" (those are spawn / timer /
clock — ADR-0025 phase 1). They are **client surfaces**. Putting them
on the engine trait would mean every engine grew a method per Pulsar
PIP forever. An extension trait per surface family scales: each PIP
adds at most one trait, each engine implements only the surfaces it
supports, and the façade still gets `impl<E: Engine>` because the
trait bound is `E::ClientState: TransactionApi + ProducerApi + ...`.

#### Next sub-PR — Producer + Consumer lift (prerequisite for the remaining seven surfaces)

The remaining seven surfaces (Reader, TypedSchemas,
MultiTopicsConsumer, PartitionedProducer, PartitionedConsumer,
PatternConsumer, TableView) all hold concrete
`magnetar_runtime_tokio::{Producer, Consumer}` instances. They
cannot be lifted to `impl<E: Engine>` until Producer and Consumer
themselves become engine-generic.

The lift shape (applies to both Producer and Consumer):

1. Define `magnetar::engine::ProducerApi` + `ConsumerApi` extension
   traits carrying the wire-level round-trips the façade methods
   consume (`send`, `receive`, `ack`, `ack_cumulative`, `flow`,
   `seek`, `close`, `last_message_id`, `has_message_after`, …).
   ~12–18 methods each.
2. Implement on `magnetar_runtime_tokio::{Producer, Consumer}`
   (delegate-only — methods exist) and on
   `magnetar_runtime_moonpool::{Producer, Consumer}` (production
   port; ~600–900 LOC per surface mirroring the tokio runtime's
   send-loop and receive-slab mechanics over moonpool's
   `ConnectionShared`).
3. Add a façade-level `Producer<T = (), E: Engine>` /
   `Consumer<T = (), E: Engine>` that holds
   `<E::ClientState as ProducerApi>::Producer` /
   `<E::ClientState as ConsumerApi>::Consumer` via the extension
   trait's associated types and dispatches through them.
4. Test layers per ADR-0024: 1:1 mirror tests on both runtime
   crates plus a differential `golden_producer_consumer` trace
   extending the existing differential broker.

After Producer/Consumer land, the seven dependent surfaces lift
mechanically — each becomes a thin façade over its
`{Producer, Consumer}` plus a per-surface extension trait. Sub-PR
order (each follows the same one-extension-trait-per-family template):

1. **Producer + Consumer** (prerequisite for every other surface).
2. **Reader** (holds a `Consumer`).
3. **TypedSchemas** (`TypedProducer<T>` / `TypedConsumer<T>` — wrap
   `Producer` / `Consumer` with a schema codec).
4. **MultiTopicsConsumer** (holds a `Vec<Consumer>`).
5. **PartitionedProducer** (holds a `Vec<Producer>` + a health loop
   on `Engine::spawn` from ADR-0025 phase 1).
6. **PartitionedConsumer** (holds a `Vec<Consumer>`).
7. **PatternConsumer** (holds a `MultiTopicsConsumer` + a
   reconciliation loop on `Engine::spawn`).
8. **TableView** (holds a `Consumer` + a drain task on
   `Engine::spawn`).

```text
/goal lift Producer + Consumer to impl<E: Engine + ProducerApi + ConsumerApi> PulsarClient<E>. Phase 1: define `magnetar::engine::{ProducerApi, ConsumerApi}` extension traits with associated `Producer` / `Consumer` types and ~12-18 methods each. Phase 2: implement on magnetar-runtime-tokio (delegate-only) and on magnetar-runtime-moonpool (full port: send-loop, receive-slab, ack pipeline, flow accounting, seek, close — ~600-900 LOC per surface). Phase 3: define façade `Producer<E: Engine>` / `Consumer<E: Engine>` types holding the associated-type instances; route the existing façade methods through them. Phase 4: ADR-0024 test layers — 4-8 mirror tests on each runtime side plus a `golden_producer_consumer` differential trace. Phase 5: docs/parity-status.md row flips + README parity matrix updates. After this lands, the seven follow-on surface lifts (Reader, TypedSchemas, MultiTopics, PartitionedProducer, PartitionedConsumer, PatternConsumer, TableView) become mechanical thin-façade wrappers over the lifted Producer/Consumer.
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
