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

**Blocked on** [D1 — Engine trait extension](#d1--engine-trait-extension-adr-0025).
Pick the trait shape first, then sequence the surface ports.

See [ADR-0019](../specs/adr/0019-engine-scope-and-moonpool-parity.md)
§Consequences.

```text
/goal once ADR-0025 lands, lift the 8 façade surfaces to PulsarClient<MoonpoolEngine<P>> in this order: Transaction → Reader → TypedSchemas → MultiTopicsConsumer → PartitionedProducer → PartitionedConsumer → PatternConsumer → TableView. One PR per surface. Each PR ships all four test layers per ADR-0024, plus a docs/parity-status.md row flip.
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

### SASL (Kerberos) and Athenz

See [D3 — SASL/Athenz scope for v0.1.0](#d3--saslathenz-scope-for-v010).
The current state in `magnetar-auth-sasl` and `magnetar-auth-athenz`
is pre-alpha stubs; full implementations are deferred to v0.2.0 pending
the decision in D3.

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
