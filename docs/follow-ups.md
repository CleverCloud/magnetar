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
- **D3** — SASL `PLAIN` ✅ + Athenz pre-fetched role token ✅ ship
  (commit `96d6f74`). SASL Kerberos/GSSAPI also ✅ via `libgssapi`
  under the `auth-sasl-kerberos` façade feature (commit `db260ea`,
  ahead of the v0.2.0 milestone per
  [ADR-0029](../specs/adr/0029-sasl-kerberos-gssapi-scope.md));
  the multi-round `AUTH_CHALLENGE` continuation reuses the existing
  `AuthProvider::respond_to_challenge` surface (no new
  `SaslMechanism` trait). Athenz ZTS round-trip 🟡 remains deferred
  per ADR-0026 §D3 / [ADR-0030](../specs/adr/0030-athenz-zts-round-trip-scope.md).
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
- **Pre-existing moonpool coverage gap — closure pass 1** (commit
  `82185cb`). 15 mirrored tests on each runtime
  (`tests/coverage_close.rs`) drill the largest uncovered hunks in
  `magnetar-runtime-moonpool/src/{driver,producer,consumer,lib,
  transport}.rs`. Per-file coverage on the five target files now
  reads consumer 75.4%, driver 54.7%, lib 92.4%, producer 85.4%,
  transport 30.3% (172 net-new lines covered, 662 → 490 uncovered);
  test parity tokio=136 moonpool=136. Coverage closure follow-up
  stays open for the next pass on the remaining hunks (transport
  TLS + driver supervised loop) — see the relevant section below.

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
sides; the test count should stay at the current 136/136 (baseline
at MultiTopics pass-1 landing time was 118/118; ADR-0028 took it to
121/121; the coverage-closure pass 1 in commit `82185cb` brought it
to 136/136) unless the lift introduces new behavior (e.g.
cross-engine partition routing).

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
implementation) brought it to `tokio=121 moonpool=121`. Pass 1 of
the pre-existing-gap closure landed in commit `82185cb` with 15
mirrored tests on each runtime (`tests/coverage_close.rs`), taking
the parity count to **`tokio=136 moonpool=136`**. Per-file coverage
on the five target files is now:

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

**Unblock.** Dedicated session driven by the local prompt at
`tasks/coverage-closure-prompt.md` (gitignored). Phases:
(1) bring tokio↔moonpool counts to 1:1 — **done**;
(2a) close the largest pre-existing moonpool coverage gaps — **done
in commit `82185cb` (pass 1)**; (2b) close the residual transport
TLS + driver supervised-loop hunks — open;
(3) full validation chain green including the local `1..32` seed
sweep (ADR-0024 §3 / ADR-0036 — CI runs the equivalent as a daily
16-random-seed sweep in
`.github/workflows/moonpool-seed-sweep.yml`). ADR-0021 still applies
— failing tests are fixed, not `#[ignore]`-d.

```text
/goal close the residual moonpool transport TLS + driver supervised-loop coverage hunks. Stand up an in-process rustls-enabled broker fixture (self-signed cert + `RustlsByteAdapter` peer driver) under `crates/magnetar-runtime-moonpool/tests/`, then add targeted tests that exercise `Transport::connect_tls`, `tls_handshake`, the TLS variants of `read_buf` / `write_all` / `flush`, and `Transport::shutdown`. Pair each new moonpool test with a same-named tokio counterpart (the tokio path is already covered via `tls_handshake_chaos.rs`; the mirror may be a Debug / fmt smoke if the surface is engine-private). Optionally close the remaining `driver.rs` `supervised_driver_loop` lines via a synthetic peer that drops the socket between handshakes. Validation chain per CLAUDE.md.
```

---

## Auth

### SASL Kerberos / GSSAPI ✅ landed

`magnetar_auth_sasl::SaslKerberos` binds `libgssapi` under the
`auth-sasl-kerberos` façade feature; the multi-round `AUTH_CHALLENGE`
continuation threads through `AuthProvider::respond_to_challenge`.
All four sans-io test layers per ADR-0024 drive a
`ScriptedGssapiClient` so they stay free of a libkrb5 build dep; the
end-to-end layer (`crates/magnetar/tests/e2e_sasl_kerberos.rs`) spins
up a Dockerised KDC. Binding decision recorded in
[ADR-0029](../specs/adr/0029-sasl-kerberos-gssapi-scope.md).

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

## Protocol — v0.2.0 PIP wave

The planning pass for PIP-460 / PIP-466 / PIP-180 / PIP-33 landed as
four per-PIP proposals under [`specs/proposals/`](../specs/proposals/),
each citing its authorising ADR (0031–0034) and breaking down
wire-protocol delta, sans-io additions, runtime ports, the four-layer
test plan per [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md),
and the e2e plan. The earlier "scope these four" goal is **closed**.

What follows is one entry per PIP — upstream-readiness flag, what
v0.2.0 ships, and a fresh `/goal …` block ready to implement.

### Upstream-readiness summary

| PIP | Upstream | v0.2.0 status |
| --- | --- | --- |
| PIP-33 — Replicated subscriptions | 🟢 LIVE (Pulsar 2.4, 2019) | ✅ landed — see [`docs/replicated-subscriptions.md`](replicated-subscriptions.md) |
| PIP-180 — Shadow topic | 🟢 LIVE (Pulsar 2.11, 2023) | ✅ landed — see [`docs/shadow-topic.md`](shadow-topic.md) |
| PIP-466 — V5 client surface | 🟠 DESIGN-PHASE (Java V5 still iterating; magnetar v0.2.0 surface is a v4-wire skin) | ⌛ unblocked — mirrors existing v4 e2e; `/goal` below |
| PIP-460 — Scalable topics | 🔴 NOT LIVE (PIP `Draft`; targets Pulsar 5.0 LTS, Oct 2026; phased 4.3.0 / 4.4.0) | ⏸ blocked — needs `apachepulsar/pulsar:5.0.0-rc-*` |

### PIP-33 — Replicated subscriptions ✅ landed

Landed in v0.2.0 ([ADR-0034](../specs/adr/0034-pip-33-replicated-subscriptions-scope.md),
[`docs/replicated-subscriptions.md`](replicated-subscriptions.md)).
`ConsumerBuilder::replicate_subscription_state(bool)` on the façade
flips `CommandSubscribe` field 14; the receive-path filter in
`magnetar-proto::conn` drops `REPLICATED_SUBSCRIPTION_*` markers and
surfaces them via `PulsarClient::next_replicated_subscription_marker` /
`poll_replicated_subscription_marker`. Two-cluster e2e runs weekly via
[`.github/workflows/e2e-replicated-subs.yml`](../.github/workflows/e2e-replicated-subs.yml).

### PIP-180 — Shadow topic ✅ landed

Landed in v0.2.0 ([ADR-0033](../specs/adr/0033-pip-180-shadow-topic-scope.md),
[`docs/shadow-topic.md`](shadow-topic.md)). Three new
`magnetar-admin` methods (`create_shadow_topic` / `delete_shadow_topic` /
`get_shadow_topics` + `get_shadow_source`), producer-side
`send_with_source_message_id`, consumer-side
`ConnectionEvent::MessageReceivedFromShadow`, and the structural
`MessageId` equality contract.

#### Post-landing follow-ups

- **Subscribe-time admin REST hint integration (façade-level)** —
  the runtime engines expose `Consumer::set_shadow_source(...)` but
  do NOT call the admin REST `get_shadow_source(topic)` automatically
  at `subscribe()` time. Today the caller threads the source-topic
  hint in by hand (or via the magnetar façade above the runtime,
  which has `magnetar-admin` available behind the `admin` feature).
  A clean addition would be a `Client::subscribe_shadow_aware(...)`
  on the magnetar façade that performs the lookup when the `admin`
  feature is active. Track here as a quality-of-life follow-up.
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
  need a separate `ShadowTopic` workload variant to exercise the
  wire path. If a richer scenario lands later (e.g. shadow-aware
  receive injection with `replicated_from` set on the inbound
  `CommandMessage`), add a `BrokerWorkload::ShadowReceive {
  source_topic }` variant.
- **E2E replicator-side wire path** —
  `crates/magnetar/tests/e2e_shadow_topic.rs` exercises the admin
  REST cycle + a regular produce-on-source / consume-on-shadow
  round-trip. The replicator-style `send_with_source_message_id`
  path against a real broker is covered by the differential
  equivalence test against the scripted broker that echoes the
  source id back; against Pulsar 4.x, the broker's real authorisation
  flow may reject a client-asserted source id that doesn't match a
  registered replicator producer. Adding the e2e assertion would
  need a Pulsar 4.x cluster with a registered replicator role —
  defer until that fixture is available.

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
3. PR merges → the entry is removed (or its **Status** is updated to
   "landed by `<commit-sha>`" if a follow-on tracker is needed).

Pending **decisions** (`D1` … `Dn`) live in this file until Florentin
calls them. Once decided, the decision becomes an ADR (or a
`/goal …` block) and the `D<n>` entry is removed.
