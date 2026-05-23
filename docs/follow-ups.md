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

## Pending design decisions (require Florentin's input before work proceeds)

These items block follow-on implementation work. None of them can be
unilaterally resolved by an agent — they need a deliberate call.

### D1 — Engine trait extension (ADR-0025)

**Status (2026-05-23).** **Phase 1 landed** as
[ADR-0025](../specs/adr/0025-engine-trait-task-and-timer-primitives.md) —
task + timer primitives (`TaskHandle`, `Interval`, `spawn`,
`abort_task`, `new_interval`, `interval_tick`) implemented on both
engines. Phase 2 (Producer/Consumer associated types) is the remaining
open call below.

**Decision needed.** What does `magnetar::Engine` grow to support the
façade lift to `PulsarClient<MoonpoolEngine<P>>`?

Three viable shapes:

1. **Minimal.** Add `Engine::TaskHandle`, `Engine::Interval`,
   `Engine::spawn`, `Engine::abort_task`, `Engine::new_interval`,
   `Engine::interval_tick`. Six new methods. Lets the façade move
   `tokio::spawn` / `tokio::time::interval` calls behind the trait so
   `PartitionedProducer::health_loop`, `TableView::drain_task`,
   `MultiTopicsConsumer::auto_update` compile against either engine.
   Easiest to land, but the façade still needs `runtime_client()` to
   return engine-specific types — meaning `Reader`, `Transaction`,
   `TypedSchemas`, partitioned/multi-topics surfaces still hold
   `magnetar_runtime_tokio::Consumer` directly.

2. **Medium.** Minimal + `Engine::Producer`, `Engine::Consumer`
   associated types. Both façade surfaces now hold `E::Producer<T>`
   / `E::Consumer<T>` rather than tokio-specific types. The trade-off
   is that the engine trait grows generic associated types (GAT-heavy
   for typed schemas), and every façade method that touches producers
   /consumers gets a `where E::Producer: …` bound. Loses some inference
   ergonomics but each surface lift becomes a near-mechanical port.

3. **Maximal.** Medium + per-surface associated types
   (`E::PartitionedProducer`, `E::MultiTopicsConsumer`,
   `E::PatternConsumer`, `E::Reader`, `E::TableView`,
   `E::Transaction`, `E::TypedProducer`, `E::TypedConsumer`). Façade
   becomes a thin re-export shell. Adds eight more associated types
   to the engine trait; every engine implementor (tokio, moonpool,
   any future engine) must produce the full surface set. Lowest
   façade complexity, highest engine-impl ceiling.

**Why this matters.** The 8-surface façade lift (Transaction →
Reader → TypedSchemas → MultiTopicsConsumer → PartitionedProducer →
PartitionedConsumer → PatternConsumer → TableView) is ~6.4k LOC plus
matching test counts on each side per ADR-0024. The Engine trait
extension shape determines whether every surface lift is a 100-LOC
mechanical port (option 3) or a 500-LOC feature port (option 1). Pick
once, ship the surface lifts sequentially after.

**Rationale guide.**
- Option 1 minimises trait surface but pushes complexity into each
  façade method. Good if we expect a third engine (`magnetar-runtime-
  glommio`?) and want to keep its impl small.
- Option 2 is the JDK-style "interface segregation" middle ground;
  each surface stays runtime-agnostic without forcing every engine to
  reimplement table-view drain semantics.
- Option 3 mirrors the Java client's `ClientImpl` shape exactly. Best
  if magnetar's façade goal is "byte-identical user experience to the
  Java client". Worst if we want to keep engine implementations
  lightweight.

**Recommendation.** Option 2 — associated `Producer<T>` / `Consumer<T>`
types plus spawn/interval. It collapses every surface lift to a
mechanical port while keeping the engine trait small enough that a
follow-on engine doesn't need to fork the surface stack.

```text
/goal land ADR-0025 (Engine trait extension, option 2: spawn+interval+Producer+Consumer associated types) and the corresponding TokioEngine + MoonpoolEngine impls. Ship with 4 test layers per ADR-0024 (proto unit, tokio integration, moonpool integration, differential equivalence) plus the new ADR file and specs/README.md index row. No façade surface lift in this PR — that lands as a follow-up train.
```

---

### D2 — Wire `moonpool-sim` into a virtualized chaos harness

**Status (2026-05-23).** **Phase 1 landed** in commit `b15c91d` —
`moonpool-sim = "=0.6"` registered in `Cargo.toml`'s
`[workspace.dependencies]`. The dep is available to any crate that
opts in. **Phase 2 (consume the dep) is the remaining call below.**

**Decision needed.** How do we put `moonpool-sim` to work?
`SimProviders` is not a drop-in replacement for `TokioProviders`:

- `moonpool_sim::SimProviders::new(WeakSimWorld, seed, IpAddr)`
  requires a reference to a `SimWorld` constructed by
  `moonpool_sim::SimulationBuilder::run(|ctx| async { … })`.
- The simulator owns a **virtual network** — `connect()` targets an
  in-memory address, not a real TCP socket. Real `TcpListener::bind`
  endpoints (the differential broker today) are not reachable from
  inside the simulation.
- The simulator owns **virtual time** — `tokio::time::sleep`
  inside the workload yields virtual ticks, not wall-clock waits.

So the originally-imagined "swap `TokioProviders` for `SimProviders`
in `runner_moonpool.rs`" is technically misleading: the differential
broker's `bind`-on-`127.0.0.1:0` model cannot be driven by the
simulator without a fully-virtualized broker.

Three realistic paths:

1. **Pure-sim chaos suite** (new test target). Write a
   `magnetar-runtime-moonpool/tests/sim_chaos.rs` that uses
   `SimulationBuilder` to spawn an in-simulator broker stub +
   `MoonpoolEngine<SimProviders>` clients. Exercises deterministic
   chaos (packet loss, partitions, clock drift) under reproducible
   seeds. Does **not** share the differential harness's broker.

2. **Restructure the differential moonpool runner** to use plain
   `tokio::spawn` instead of `spawn_local` (option (b) from the
   "Moonpool runner LocalSet pump" entry below). Drops the
   `Kicker` workaround without invoking `moonpool-sim`. Loses the
   "differential harness runs under sim" goal entirely.

3. **Virtualize the differential broker** (largest scope). Replace
   `ScriptedBroker::bind` with a `SimWorld`-aware in-memory listener
   so the differential harness runs entirely inside the simulator.
   Both engine runners then use `SimProviders`. Closes the LocalSet
   pump AND unlocks reproducible cross-engine chaos.

**Recommendation.** Option **1** first — ships a new test target
that proves `moonpool-sim` integration end-to-end without
restructuring the differential harness. Option 2 as a
follow-up if the `Kicker` workaround becomes maintenance pain.
Option 3 is deferred to v0.2.0; it's the most thorough closure but
the largest engineering investment.

```text
/goal land magnetar-runtime-moonpool/tests/sim_chaos.rs: a moonpool_sim::SimulationBuilder workload that constructs MoonpoolEngine<SimProviders>, spawns an in-simulator broker stub (single-topic, send+recv+close), and asserts deterministic byte-identical EventStreams across 32 seeds. ADR-0024 four-layer test parity does not apply (this is a moonpool-only fixture by design — document the exemption in the commit message). Pre-existing differential harness untouched.
```

---

### D3 — SASL/Athenz scope for v0.1.0

**Decision needed.** Ship the auth crates as **pre-alpha stubs** in
v0.1.0 (current state), or invest in `libgssapi` (SASL/Kerberos) +
ZTS-token / JWT signing (Athenz) to make them functional?

**Why this matters.** `magnetar-auth-sasl` only implements
`SaslPlain` (RFC 4616); `SaslKerberos` returns
`AuthError::Unsupported`. `magnetar-auth-athenz` ships
`AthenzProvider::with_role_token` (pre-fetched token) but
`AthenzProvider::new` (ZTS round-trip) returns `Unsupported`. Full
GSSAPI integration is ~600 LOC + cross-platform abstraction; full
ZTS is ~400 LOC + RSA signing dep.

**Rationale guide.**
- v0.1.0 parity bar per ADR-0010 is "full Java parity on tokio". The
  Java client treats SASL/Athenz as plug-in auth providers; shipping
  the partial surfaces (PLAIN + pre-fetched token) is honest for
  non-Kerberos / non-Athenz environments.
- Full GSSAPI requires `libgssapi` ≈ 0.12.x (MIT, moderate maintenance,
  non-trivial C FFI). Cross-platform support (Linux libkrb5, macOS
  Heimdal, Windows SSPI) would need its own abstraction layer.
- Full ZTS needs `ring` (already implicit via rustls) for RSA-SHA256
  signing plus optional `jsonwebtoken` (or hand-coded JWT plumbing).

**Recommendation.** Pre-alpha stubs for v0.1.0, full impl in v0.2.0
behind a dedicated PR (the scoping report at the bottom of this file
covers SCRAM-SHA-256 as a v0.2.0 stepping stone before Kerberos —
medium complexity, no GSSAPI dep).

```text
/goal land SASL/Kerberos and Athenz/ZTS in v0.2.0. Phase 1: SCRAM-SHA-256 (~200 LOC, no GSSAPI). Phase 2: full GSSAPI via libgssapi (propose the dep first, await approval). Phase 3: Athenz ZTS round-trip + token refresh via wiremock-backed tests. All four test layers per ADR-0024; e2e against an unauthenticated broker with a mock auth server.
```

---

### D4 — Vendored-proto bump cadence

**Decision needed.** When do we refresh
`crates/magnetar-proto/proto/PulsarApi.proto` from upstream?

**Why this matters.** The current pin is `apache/pulsar@7735851`
(2026-05-04). Several PIPs (PIP-415 was REST-only, but PIP-460 / 466 /
180 / 33 — see "Out of scope for v0.1.0" below — will require proto
bumps if they're brought in scope). `cargo run -p xtask -- vendor-proto`
is not yet implemented (per `xtask/src/main.rs:108`); the refresh has
to be manual today.

**Rationale guide.** Bumping the vendored proto can introduce wire-
breaking changes; we keep it pinned to a known-good commit. The
`codegen --check` xtask gate catches drift if the proto changes
without regenerating `magnetar-proto/src/pb/`.

```text
/goal implement xtask vendor-proto: clone apache/pulsar at --rev <SHA>, copy PulsarApi.proto + PulsarMarkers.proto into crates/magnetar-proto/proto/, run codegen, commit. Then schedule the next proto bump for the v0.2.0 cycle once PIP-460/466 stabilise upstream.
```

---

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
