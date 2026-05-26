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
  `SimulationBuilder`, 16-seed sweep (commit `c23f6fd`). Follow-on
  with stateful broker + invariant assertions (at-least-once,
  monotonic message-id, no-dup-on-acked, supervisor-recovers-within-N)
  landed in `aaa0661`.
- **D1 surface train** — concrete generic types
  `magnetar::<Surface><T, E: Engine>` (no GATs) on all seven
  dependent surfaces. Transaction (`ab9041b`), Reader, TableView,
  PartitionedProducer have full impl-body lifts; TypedSchemas,
  MultiTopicsConsumer, PartitionedConsumer, and PatternConsumer
  carry their cascading type parameter (`Inner<C>`, `NamedConsumer<C>`,
  `<C>` / `<P>`) but their inherent impl methods stay tokio-bound
  pending the per-surface builder lifts (see
  [next section](#per-surface-builder--impl-body-lifts)).
- **D1 base builders** — `ConsumerBuilder<'a, E: Engine = TokioEngine>`,
  `ProducerBuilder<'a, E: Engine = TokioEngine>`, and
  `ReaderBuilder<E: Engine = TokioEngine>` lifted via
  `SubscribeApi` / `CreateProducerApi` extension traits implemented
  on both runtime `Client` types (commits `cc61d4d`, `0b6f363`,
  `08c89ca`).
- **E2E sweep stabilisation** — thirteen broker-driven runtime bugs
  surfaced by the e2e suite (#55 through #73) all landed. Highlights:
  ack-then-flow ordering on post-seek resubscribe (`f4872d7`),
  accumulated in-flight publish snapshots across reset cycles
  (`0e47e14`), lookup-then-retry on transient open errors
  (`c1bc2c6` + `6da2e80`), `is_user_closed` gate so transport drops
  trigger reconnect (`86398a8`), batch flush + per-message seq + receipt
  sentinel (`1508a64`), txn TTL milliseconds + TC bootstrap + txn-id on
  metadata (`19a8df5`), Java-compatible KeyValue inline schema
  (`623a5b3`), chunk-payload metadata reserve (`14cc7f8`),
  CloseProducer treated as transient (`aa9b3fc`). E2E sweep:
  **19 files PASS / 51 tests** at the time these fixes landed.
- **ADR-0028 anti-thrash policy — Accepted + implemented** (commit
  `a083ed2`, closes follow-up #74). Opt-in `SupervisorConfig`
  knobs (`anti_thrash_threshold`, `drop_grace`,
  `max_backoff_after_thrash`), default OFF. `magnetar-proto::
  AntiThrashState` ring + per-Connection observer + `Connection
  Event::AntiThrashCooldown { until }`. Four-layer tests per
  ADR-0024 plus a `DropsTcpAfterCreate { delay_ms }` chaos
  workload. Architecture documentation in `ARCHITECTURE.md`
  §"Supervised reconnect" / "Anti-thrash policy".
- **MultiTopics pass-1: moonpool consumer helpers** — 13 net-new
  methods on `magnetar_runtime_moonpool::Consumer` (commits
  `5f1368f`, `53669f9`, `0f95a3c`, `008abbf`). Pass-2 (the
  surface lift itself) remains open per [next section](#per-surface-builder--impl-body-lifts).
- **Differential harness — seek-per-partition golden trace**
  (commit `3d6c7e6`). Scripted broker partition routing + new
  `Op::SendPartition` / `RecvPartition` / `AckPartition` /
  `SeekPartition` variants. Catalog now ships seven golden traces.
- **Crypto provider pluggability — ADR-0035 Accepted** (commits
  `19f8b9f`, `3f392af`, `9a6ffde`). `rustls` crypto provider
  switched to a feature-gated set (`crypto-aws-lc-rs` default /
  `-ring` / `-openssl` / `-fips`); workspace-scope
  `--all-features` continues to work via the `tls_crypto.rs` cfg
  cascade (priority order resolves multi-provider activation),
  and `cargo xtask check-crypto-matrix` covers the per-provider
  build matrix exhaustively. Per-package invocations
  (`cargo test -p <crate>`) need an explicit crypto feature
  because dependency features don't transitively activate under
  `-p`.
- **Moonpool seed sweep policy — ADR-0036 Accepted** (commit
  `305f31d`). Daily 16-random-seed CI job replaces the per-PR
  fixed 32-seed matrix; each `(commit, seed)` pair is
  bit-for-bit reproducible so the per-PR cost was wasted.
- **`magnetar_proto::SUPPORTED_PROTOCOL_VERSION` constant**
  (commit `51101c5`). Deduplicates the literal `21` from three
  call sites (proto `ConnectionConfig::default`, proto test
  fixture, CLI banner).

---

## Per-surface builder + impl-body lifts

**Status.** Pass-1 (moonpool runtime ports) landed via four commits
`5f1368f`, `53669f9`, `0f95a3c`, `008abbf`. Pass-2 (the actual
surface lift) still pending.

- ✅ **`TypedProducer<S, P>` / `TypedConsumer<S, C>` — LANDED**
  (commit `95b8790`). Builders carry `E: Engine = TokioEngine`;
  helper-method ports added to `ProducerApi`
  (`compression`, `last_sequence_id_published`, `pending_count`,
  `batch_len`, `batch_bytes`) and `ConsumerApi` (`ack_grouped`,
  `ack_grouped_cumulative`, `ack_with_txn`, `ack_cumulative_with_txn`).
- ✅ **MultiTopics pass-1: moonpool helpers — LANDED**. Thirteen
  net-new methods on `magnetar_runtime_moonpool::Consumer`
  (`available_in_queue`, `available_permits`,
  `has_received_any_message`, `has_reached_end_of_topic`,
  `is_paused`, `is_inactive`, `drain_dead_letter`,
  `receive_with_timeout`, `receive_batch`,
  `receive_batch_with_bytes_cap`, `unsubscribe(force: bool)`,
  `reconsume_later`, `reconsume_later_with_properties`,
  `republish_dead_letters`). Test parity tokio=118 moonpool=118.
- 🟡 `MultiTopicsConsumer<C>` — phantom-lift in commit `b51680a`;
  `MultiTopicsConsumerBuilder` is still `<'a>`. Awaiting pass-2.
- 🟡 `PartitionedConsumer` — type alias for `MultiTopicsConsumer<C>`;
  lifts transitively when MultiTopicsConsumer lifts.
- 🟡 `PatternConsumer<C>` — phantom-lift in commit `31f9cbe`; same
  blocker as MultiTopicsConsumer plus topic-watcher subscription
  needs `SubscribeApi`-mediated child consumer creation.

**Pass-2 — the surface lift itself.** All 13 helpers exist on both
runtimes today (tokio's matching surface was already there). What
remains:

1. **Add the 13 helpers to the `ConsumerApi` trait** in
   `crates/magnetar/src/engine.rs`. Each addition is a thin delegate
   on both `impl ConsumerApi for magnetar_runtime_tokio::Consumer`
   and `impl<P: Providers> ConsumerApi for
   magnetar_runtime_moonpool::Consumer<P>`. No new tests for the
   trait pass-through (the existing pass-1 unit tests on each
   runtime already cover the behavior).
2. **Lift `MultiTopicsConsumerBuilder<'a>`** to
   `MultiTopicsConsumerBuilder<'a, E: Engine = TokioEngine>`. Route
   `.subscribe()` / `.subscribe_all()` through the engine-generic
   base `ConsumerBuilder`.
3. **Lift `PatternConsumerBuilder<'a>`** to
   `PatternConsumerBuilder<'a, E: Engine = TokioEngine>`. The
   PIP-145 auto-reconcile child-subscribe routes through
   `<E::ClientState as SubscribeApi>::subscribe`.
4. **Lift `MultiTopicsConsumer<C>` + `PatternConsumer<C>` impl
   bodies** so every method dispatches through the trait. Split
   tokio-only methods into a separate `impl<...>
   MultiTopicsConsumer<C, TokioEngine>` block if needed (mirror
   the PartitionedProducer split pattern).
5. **Flip parity-status.md rows** for "Partitioned consumer",
   "MultiTopicsConsumer", "PatternConsumer (PIP-145)" from
   "🟡 (phantom-lift; impl tokio-bound)" to "✅". Update the
   README parity matrix accordingly.

Test parity per
[ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md):
the 13 trait additions are pure delegates so they don't need new
mirror tests if the underlying impl is already covered on both
sides; the test count should stay at 118/118 unless the lift
introduces new behavior (e.g. cross-engine partition routing).

```text
/goal lift MultiTopicsConsumer<C> + PartitionedConsumer + PatternConsumer<C> impl-bodies on both engines (pass-2; pass-1 helpers already on both runtimes per commits 5f1368f, 53669f9, 0f95a3c, 008abbf). Steps: (1) add the 13 pass-1 helpers to ConsumerApi as thin delegates (`available_in_queue`, `available_permits`, `has_received_any_message`, `has_reached_end_of_topic`, `is_paused`, `is_inactive`, `drain_dead_letter`, `receive_with_timeout`, `receive_batch`, `receive_batch_with_bytes_cap`, `unsubscribe`, `reconsume_later`, `reconsume_later_with_properties`, `republish_dead_letters`); (2) lift `MultiTopicsConsumerBuilder<'a>` → `MultiTopicsConsumerBuilder<'a, E: Engine = TokioEngine>` and `PatternConsumerBuilder<'a>` similarly; route .subscribe()/.subscribe_all() through the engine-generic base ConsumerBuilder; (3) lift `MultiTopicsConsumer<C>` + `PatternConsumer<C>` impl-bodies dispatching through the trait; split tokio-only methods if any; (4) PatternConsumer's PIP-145 auto-reconcile child-subscribe routes through `<E::ClientState as SubscribeApi>::subscribe`; (5) flip parity-status.md rows for "Partitioned consumer", "MultiTopicsConsumer", "PatternConsumer (PIP-145)" to ✅; flip the README parity matrix; (6) full validation chain incl. `cargo +nightly fmt`, `cargo build --workspace --all-features`, `cargo clippy --workspace --all-features --all-targets -- -D warnings`, `cargo test --workspace --features crypto-aws-lc-rs --locked`, `check-runtime-test-parity`, `check-no-channels`, `check-no-io-deps`, `check-no-internal-clock`, `RUSTDOCFLAGS="-D warnings --cfg tokio_unstable" cargo doc --workspace --all-features --no-deps --locked`.
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

**Unblock.** Closed by the future moonpool-sim integration (see
the D2 line under [What landed](#what-landed-since-the-multi-source-design-synthesis));
the simulator's deterministic scheduler drives both sides without
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

### Cross-runtime test + coverage closure (ADR-0024)

**Status.**
[ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md)
landed 2026-05-22 with both `cargo xtask check-sim-coverage` and
`cargo xtask check-runtime-test-parity` enabled and hard-failing. The
2026-05-22 baseline was `tokio=65 moonpool=61` (gap of 4); subsequent
landings (memory-limit slab, AutoClusterFailover moonpool port, TLS
chaos fixtures, race-stress coverage, lookup-before-open, the D1
surface train, the post-seek ack-then-flow fix in `f4872d7`, the
MultiTopics pass-1 moonpool helpers, and the ADR-0028 anti-thrash
implementation) brought it to **`tokio=121 moonpool=121`**.
Pre-existing moonpool patch-coverage of older surface lines is
unmeasured today.

**Unblock.** Dedicated session driven by the local prompt at
`tasks/coverage-closure-prompt.md` (gitignored). Phases:
(1) bring tokio↔moonpool counts to 1:1 — **done**;
(2) close pre-existing moonpool coverage gaps file by file using the
`cargo llvm-cov --html` report; (3) full validation chain green
including the local `1..32` seed sweep (ADR-0024 §3 / ADR-0036 — CI
runs the equivalent as a daily 16-random-seed sweep in
`.github/workflows/moonpool-seed-sweep.yml`). ADR-0021 still applies —
failing tests are fixed, not `#[ignore]`-d.

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
  per-snapshot markers in the data stream +
  `CommandReplicatedSubscriptionSnapshot{Request,Response}`.
  Substantial broker-side coupling. **Defer to v0.2.0**; magnetar's
  geo-replication story (via `ServiceUrlProvider` +
  `AutoClusterFailover`) covers most of the use case at the cluster
  level without subscription-state replication.

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
