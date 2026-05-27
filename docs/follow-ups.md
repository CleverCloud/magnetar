# Open Follow-Ups

Consolidated tracker for known open work. Each entry lists the gap,
the reason it stays open, and (where actionable) a `/goal …` block
ready to be copy-pasted verbatim into a fresh session for an agent
team to pick up.

For the public-facing parity status, see
[`parity-status.md`](parity-status.md) and the
[parity matrix in the README](../README.md#java-client-parity-matrix).

This file is the **single source of truth** for what is intentionally
deferred or blocked. Anything not listed below is either landed
(check `git log` for the implementation reference), or explicitly out
of scope for v0.2.0 ([ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
§D-series, [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md),
[ADR-0032](../specs/adr/0032-pip-466-v5-client-surface-scope.md)).

**API stability stance.** The crate is not yet published. Breaking
API changes are acceptable when they improve correctness, ergonomics,
or layering; ship with `BREAKING CHANGE:` in the commit body so the
eventual changelog flags them.

---

## Index

Status tags: ⚡ ready to dispatch · 🔗 blocked on external dep ·
⏳ blocked on upstream PIP release · 🧠 needs design decision ·
🟡 deferred (not load-bearing).

| # | Item | Status |
| - | --- | --- |
| 1 | [Moonpool vectored I/O](#1-moonpool-vectored-io) | 🔗 [PierreZ/moonpool#111](https://github.com/PierreZ/moonpool/issues/111) |
| 2 | [Builder phantom-`E` cleanup](#2-builder-phantom-e-cleanup) | ⚡ |
| 3 | [Per-surface builder + impl-body lifts (partitioned-producer / table-view)](#3-per-surface-builder--impl-body-lifts) | ⚡ |
| 4 | [V5 engine-genericity (PIP-466 promotion)](#4-v5-engine-genericity-pip-466-promotion) | ⚡ (cross-cuts #3) |
| 5 | [Athenz concrete `JwtSigner`](#5-athenz-concrete-jwtsigner) | 🧠 |
| 6 | [Athenz ZTS e2e fixture](#6-athenz-zts-e2e-fixture) | 🔗 (needs #5) |
| 7 | [PIP-180 replicator-side e2e](#7-pip-180-replicator-side-e2e) | 🔗 (Pulsar 4.x replicator fixture) |
| 8 | [PIP-460 scalable topics](#8-pip-460-scalable-topics) | ⏳ (Pulsar 5.0 RC) |
| 9 | [Moonpool transport TLS + supervised-loop coverage](#9-moonpool-transport-tls--supervised-loop-coverage) | ⚡ |
| 10 | [Golden trace catalog — transactional ack + cryptoFailureAction](#10-golden-trace-catalog-extension) | ⚡ (partial) |
| 11 | [Moonpool runner `LocalSet` pump](#11-moonpool-runner-localset-pump) | 🟡 (deferred — closed by future moonpool-sim integration) |
| 12 | [`engine.rs` split](#12-enginers-split) | 🟡 (deferred until file is uncomfortably wide) |
| 13 | [`ProducerExt` trait inline](#13-producerext-trait-inline) | 🧠 (needs layering decision) |

---

## 1. Moonpool vectored I/O

**Gap.** `magnetar-runtime-moonpool/src/driver.rs` coalesces
`TransmitOwned::Vectored` segment lists into a single `BytesMut`
before calling `transport.write_all`, because
`moonpool_sim::network::sim::SimTcpStream` implements only
`poll_write` (no `poll_write_vectored`). The local coalesce
preserves byte correctness but the chaos pack only ever sees one
`poll_write` call — we lose per-`IoSlice` partial-write modelling,
fragmentation, reordering. The tokio engine already dispatches the
Vectored arm via real `writev(2)` through
`AsyncWriteExt::write_vectored`.

**Why it stays open.** Needs upstream change in
[`moonpool-sim`](https://github.com/PierreZ/moonpool): override
`AsyncWrite::poll_write_vectored` + `is_write_vectored` on
`SimTcpStream`, with `writev(2)`-style short-accept semantics. Once
the trait surface lands, magnetar drops the local coalesce.

**Filed.** [PierreZ/moonpool#111](https://github.com/PierreZ/moonpool/issues/111)
(cc'd, awaiting PierreZ — direction to land it or to send a PR from
our side).

**`/goal` (post-upstream).**

```text
/goal flip magnetar-runtime-moonpool to true vectored dispatch once PierreZ/moonpool#111 lands. Replace the local coalesce in crates/magnetar-runtime-moonpool/src/driver.rs::driver_loop_inner's `Vectored` arm with a `write_all_vectored` helper mirroring crates/magnetar-runtime-tokio/src/driver.rs::write_all_vectored — loop `AsyncWriteExt::write_vectored` with per-IoSlice offset advancement, handle partial accepts, WriteZero on n==0 with non-empty slices. Test layers per ADR-0024: extend crates/magnetar-runtime-moonpool/tests/poll_transmit_vectored_parity.rs::poll_transmit_vectored_emits_vectored_for_queued_producer_send to assert the underlying transport observed N separate segment events (not one coalesced write). Validation chain per CLAUDE.md. ADR-0039 wave 2 chaos-fidelity gap closes when this lands.
```

---

## 2. Builder phantom-`E` cleanup

**Gap.** `crates/magnetar/src/builders.rs` —
`ProducerBuilder<'a, E: Engine>`, `ConsumerBuilder<'a, E: Engine>`,
`ReaderBuilder<'a, E: Engine>` carry the engine generic on every
method even though the inherent impl bodies are 95% tokio-bound
(direct `magnetar_runtime_tokio::MessageEncryptor` /
`MessageDecryptor` references). The generic only matters at the
final `.create()` / `.subscribe()` dispatch — through the
`CreateProducerApi` / `SubscribeApi` / `ReaderApi` extension
traits — but the noise is on every chainable method.

**Why it stays open.** Move the generic only to dispatch is a
breaking API change (every caller that named the builder type loses
the parameter). Crate is not yet published — breaking is acceptable
— but needed a green light. Florentin has given it.

**`/goal`.**

```text
/goal land the builder phantom-E cleanup per docs/follow-ups.md §2. In crates/magnetar/src/builders.rs: introduce two builder types per surface — a non-generic chainable form (`ProducerBuilder<'a>`, `ConsumerBuilder<'a>`, `ReaderBuilder<'a>`) that holds the v4 `CreateProducerRequest` / `SubscribeRequest` and any tokio-specific fields (encryptor / decryptor / router), and a generic dispatch form (`ProducerBuilderE<'a, E: Engine>` or similar) that the final `.create()` / `.subscribe()` consumes. Replace every public method that returned `Self` with the non-generic form. Keep the `E`-generic only on `pub async fn create()` / `pub async fn subscribe()` and route through the existing `CreateProducerApi` / `SubscribeApi` / `ReaderApi` extension traits. Update `crates/magnetar/src/client.rs::PulsarClient::producer/consumer/reader` to return the non-generic builder. Run the full validation chain per CLAUDE.md (cargo +nightly fmt, build, clippy -D warnings, test, xtask check-runtime-test-parity, check-no-channels, check-no-io-deps, check-no-internal-clock). Test layers per ADR-0024: extend the existing tokio + moonpool builder integration tests (or add per-builder type-shape tests if missing) to confirm the new shape compiles against both engines. Update docs/v5-client.md mapping table examples if the surface change touches them. BREAKING CHANGE: ProducerBuilder/ConsumerBuilder/ReaderBuilder no longer carry an Engine type parameter on chained methods; the parameter moves to .create()/.subscribe() dispatch.
```

---

## 3. Per-surface builder + impl-body lifts

**Gap.** `PulsarClient::partitioned_producer(...)`,
`PulsarClient::table_view(...)`,
`PulsarClient::typed_table_view(...)` still live in
`impl PulsarClient<TokioEngine>` rather than the engine-generic
block. The inner builder types
(`PartitionedProducerBuilder<'a>`, `TableViewBuilder<'a>`,
`TypedTableViewBuilder<'a, S>`) reference tokio-only types directly
(`Arc<dyn magnetar_runtime_tokio::MessageEncryptor>` on the
partitioned-producer builder,
`magnetar_runtime_tokio::MessageDecryptor` on the table-view
builder). See `crates/magnetar/src/partitioned_producer.rs:716`,
`crates/magnetar/src/table_view.rs:333,763`.

**Why it stays open.** Lifting these surfaces requires abstracting
`MessageEncryptor` / `MessageDecryptor` / `MessageRouter` /
broker-metadata lookup behind per-engine extension traits — same
pattern §2 uses for the core builders. Cross-cuts §2; should land
in the same wave if possible.

**`/goal`.**

```text
/goal lift PulsarClient::{partitioned_producer, table_view, typed_table_view} to the engine-generic impl block per docs/follow-ups.md §3 (ADR-0026 §D1 completion). Sub-steps in order: (1) Add per-engine extension traits in crates/magnetar/src/engine.rs (or a new traits module) for the tokio-only types currently leaking — `MessageEncryptorApi`, `MessageDecryptorApi`, `MessageRouterApi`, `PartitionedTopicMetadataApi`. Each carries the associated types the v4 builders need. (2) Make PartitionedProducerBuilder<'a, E: Engine> carry the Encryptor / Router types via those traits. (3) Same for TableViewBuilder<'a, E: Engine> / TypedTableViewBuilder<'a, E: Engine, S> (Decryptor + broker-metadata). (4) Route every internal `.consumer(...)` / `.producer(...)` plumbing through the engine-generic `SubscribeApi` / `CreateProducerApi` traits. (5) Lift the three entry-point methods to `impl<E: Engine> PulsarClient<E>`. Test layers per ADR-0024 — the lift is a pure delegate so existing tokio integration tests should pass unchanged; runtime parity stays at the pre-lift count (tokio=moonpool). Validation chain per CLAUDE.md. Coordinate landing with §2 (builder phantom-E) if both are in flight — same builder.rs touch points. BREAKING CHANGE: PartitionedProducerBuilder/TableViewBuilder/TypedTableViewBuilder gain an `E: Engine` type parameter and the v4 tokio-typed fields move behind per-engine API traits.
```

---

## 4. V5 engine-genericity (PIP-466 promotion)

**Gap.** `PulsarClientV5` wraps `PulsarClient<TokioEngine>` directly
per ADR-0032; the V5 surface cannot drive `MoonpoolEngine<P>`. This
blocks two PIP-466 remaining-work items:

- The V5 moonpool-side test mirror (the 5 V5 mapping/wire test files
  live at the magnetar tier against a sans-io `Connection` today —
  engine-agnostic by construction — but moonpool exercise of the V5
  surface needs the engine-generic lift).
- ADR-0032 promotion from Proposed → Accepted (gated on the moonpool
  mirror landing per the ADR's own acceptance criteria).

**Why it stays open.** Same `MessageEncryptor` / `MessageDecryptor`
/ `MessageRouter` engine-generic lift that §3 needs. The V5 surface
inherits engine-genericity once the v4 builders gain it.

**`/goal`.**

```text
/goal parametrise PulsarClientV5 (and the v5::producer / v5::stream_consumer / v5::queue_consumer wrappers) by `<E: Engine>` once §3 (per-surface builder + impl-body lifts) has landed the engine-generic v4 builders. Wrap the engine-generic v4 client in PulsarClientV5<E> (replace today's PulsarClientV5 { inner: PulsarClient }). Keep the `into_v4()` / `v4()` escape hatch contract — pin it with a new test in crates/magnetar/tests/v5_client_v4_escape_hatch.rs that asserts mem::size_of::<PulsarClientV5<E>>() == mem::size_of::<PulsarClient<E>>() for both E=TokioEngine and E=MoonpoolEngine<TokioProviders>. Add the moonpool 1:1 mirror of the 5 V5 mapping/wire test files under crates/magnetar-runtime-moonpool/tests/v5_*_moonpool.rs — each exercises the V5 surface against the moonpool engine + SimulationBuilder and asserts the same wire byte shape the magnetar-tier tests do. Flip ADR-0032 status from Proposed → Accepted in specs/adr/0032-pip-466-v5-client-surface-scope.md once the moonpool mirror is green and `cargo run -p xtask -- check-crypto-matrix` (× V5 axis) passes. Update specs/README.md ADR index. Update README.md parity matrix row for PIP-466 from 🟡 experimental to ✅ default-on (or ✅ experimental if the experimental-v5-client feature stays default-off). Update docs/v5-client.md "Roadmap" section to mark items #1, #4, #5 as landed. Validation chain per CLAUDE.md (including the V5-feature build matrix). BREAKING CHANGE: PulsarClientV5 gains an `E: Engine` type parameter.
```

---

## 5. Athenz concrete `JwtSigner`

**Gap.** `crates/magnetar-auth-athenz/src/zts.rs` ships the
`ZtsClient` + `JwtSigner` trait, but no concrete signer
implementation. Without one, the parity matrix row for Athenz stays
at 🟡 — callers either supply their own signer (documented external
pattern using `with_role_token` + sidecar mint) or the feature is
unusable end-to-end.

**Why it stays open.** Crypto-crate choice is downstream-policy-
dependent: jsonwebtoken (ergonomic but pulls a fresh dep tree),
aws-lc-rs (matches ADR-0035's default crypto provider, FIPS-friendly),
ring (familiar shape, no FIPS path), HSM-backed (out-of-process via
PKCS#11 — heaviest). Florentin's call.

ADR-0030 also defers the parsed-key `zeroize::Zeroizing<…>` wrap
here because the parsed RSA key only materialises once a concrete
signer exists.

**Needs decision.** Which crypto crate(s) to support, default vs
optional, FIPS posture.

**`/goal` (post-decision).**

```text
/goal land the concrete Athenz JwtSigner per docs/follow-ups.md §5 using <CRYPTO-CRATE — to be filled in from §5 decision>. Implementation in crates/magnetar-auth-athenz/src/zts.rs::<NewSignerType> (or sibling module): parse the PEM RSA key once at construction, wrap the parsed key in zeroize::Zeroizing<…> (closes ADR-0030 deferral), implement `impl JwtSigner for <NewSignerType>` with the JWS RS256 / ES256 signing the Athenz ZTS spec requires (RFC 7519 + Athenz N-tokens-as-JWT). Sign-on construction or on-demand per signer construction parameter. Test layers per ADR-0024: (a) magnetar-auth-athenz unit tests covering the signer round-trip (sign + decode via the same crate, assert iss/sub/aud/exp); (b)/(c) the existing static-signer integration tests stay; (d) optional differential — the JWT bytes are deterministic given a fixed key + fixed timestamp, so a frozen wall_clock makes the assertion stable across engines. If the chosen crate has FIPS implications, wire it behind a new `crypto-<provider>` feature on magnetar-auth-athenz mirroring the ADR-0035 pattern. Update docs/parity-status.md Athenz row from 🟡 to ✅, README parity matrix row, flip ADR-0030 deferral note in specs/adr/0030-athenz-private-key-zeroize-deferral.md. Validation chain per CLAUDE.md.
```

---

## 6. Athenz ZTS e2e fixture

**Gap.** No end-to-end test exercises the Athenz ZTS round-trip
against a real ZTS server. Tests today are unit-level against the
`zts::ZtsClient` + a static `JwtSigner` mock.

**Why it stays open.** Blocked on §5 (a real signer) and on the
Dockerised ZTS fixture image (`athenz/athenz-zts-server`).

**`/goal` (post-§5).**

```text
/goal stand up the Athenz ZTS e2e fixture per docs/follow-ups.md §6. Add the `athenz/athenz-zts-server` Docker image as a testcontainers-rs spawn under crates/magnetar/tests/e2e_athenz_zts.rs (NEW), gated `feature = "e2e,auth-athenz-zts"` and `#[ignore = "e2e: requires Docker"]`. Tests: (1) ZtsClient::refresh_via_zts → cached role token returned by initial(); (2) cached token's expiry-aware refresh fires when expiry approaches; (3) the cached token is used in a subsequent AuthProvider::respond_to_challenge round-trip (mock challenge). Pre-seed the fixture with a tenant principal + role binding via the ZTS admin API on container startup. Use the §5-landed concrete JwtSigner. Validation chain per CLAUDE.md. Update docs/parity-status.md.
```

---

## 7. PIP-180 replicator-side e2e

**Gap.** `crates/magnetar/tests/e2e_shadow_topic.rs` exercises the
admin REST cycle + a regular produce-on-source / consume-on-shadow
round-trip. The replicator-style `send_with_source_message_id`
path is covered by the differential equivalence test against the
scripted broker that echoes the source id back; against real
Pulsar 4.x, the broker's authorisation flow may reject a
client-asserted source id that doesn't match a registered
replicator producer.

**Why it stays open.** Needs a Pulsar 4.x cluster with a registered
replicator role — not something the testcontainers single-broker
setup gives us out of the box.

**`/goal` (when fixture is available).**

```text
/goal add the PIP-180 replicator-side e2e assertion per docs/follow-ups.md §7. Extend crates/magnetar/tests/e2e_shadow_topic.rs (or a new e2e_shadow_topic_replicator.rs) gated `feature = "e2e"` with an `#[ignore = "e2e: requires Pulsar 4.x with registered replicator role"]` test that: (1) bootstraps a Pulsar standalone with a custom auth config registering a "replicator" role on the source namespace; (2) opens a producer authenticated as that role; (3) calls send_with_source_message_id with a synthetic source MessageId; (4) consumes on the shadow topic and asserts the source id round-trips intact. Document the broker setup in docs/shadow-topic.md under a new "Replicator-role e2e setup" section. Validation chain per CLAUDE.md.
```

---

## 8. PIP-460 scalable topics

**Gap.** PIP-460 surface entirely. Wire-protocol delta is
significant (3 new commands + optional `MessageId.segment_id`); the
proto bump is gated on upstream cutting an RC.

**Why it stays open.** Upstream PIP is **`Draft`**, targets Pulsar
5.0 LTS (Oct 2026) with phased rollout via 4.3.0 / 4.4.0. No broker
ships PIP-460 today, so this is scaffold-now / e2e-later. Estimate
~2080 LOC.

**Scope locked.** [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md)
+ [`specs/proposals/pip-460-scalable-topics.md`](../specs/proposals/pip-460-scalable-topics.md):
StreamConsumer-only, drops-on-DAG-change (no transparent failover),
behind `feature = "scalable-topics"` (default off). QueueConsumer,
CheckpointConsumer, controller-election, in-place repartition are
explicit v0.3.0+. **E2E is best-effort and does not block release**;
the 4-layer in-process tests are the binding acceptance gate.

**`/goal` (scaffold-now path; e2e once a 5.0 RC ships).**

```text
/goal implement PIP-460 scalable-topics surface per specs/proposals/pip-460-scalable-topics.md and ADR-0031. Upstream is `Draft` and no broker ships PIP-460 today, so this is scaffold-now / e2e-later. Waves: (0) PREREQ — separate commit per ADR-0026 §D4: `cargo run -p xtask -- vendor-proto --rev <pulsar-5.0-rc-sha>` ONCE upstream cuts a 5.0 RC; until that lands, hand-encode the new commands behind a `cfg(feature = "scalable-topics")` gate in magnetar-proto/src/pb/scalable_topics.rs (NEW) using prost-build manual definitions; (1) magnetar-proto/src/types.rs extend `MessageId { segment_id: Option<SegmentId> }`, new types `SegmentId(u64)`, `KeyRange { start: u32, end: u32 }`, `SegmentState { Active, Splitting, Merging, Sealed }` (`#[non_exhaustive]`), `SegmentDescriptor`; equality rules: `None`-segment ignored for v4 invariant, `Some(_)` vs `None` returns false (cross-mode); (2) magnetar-proto/src/dag_watch.rs (NEW) — `DagWatchSession` with monotonic update_seq tracking, `handle_update(SegmentDagUpdate) -> Result<DagDelta, DagError>`, `DagError::{NonMonotonic, UnknownSegment, ...}`; (3) magnetar-proto/src/conn.rs — new entries `send_scalable_topic_lookup`, `open_dag_watch`, `close_dag_watch`; magnetar-proto/src/event.rs — new variants `ScalableTopicLookupResolved`, `SegmentDagUpdated`, `DagChangedDuringConsume { reason: DagChangeReason }`; magnetar-proto/src/lib.rs — new `SUPPORTED_PROTOCOL_VERSION_SCALABLE_TOPICS` constant; (4) magnetar::scalable module (NEW) behind `feature = "scalable-topics"` (default off) exposing `ScalableTopicsApi` extension trait + `StreamConsumer<T, E> where E::ClientState: ScalableTopicsApi`; on `DagChangedDuringConsume` close all per-segment v4 consumers and surface `ConsumerEvent::DagChanged`; (5) magnetar-runtime-tokio — `topic://` URL parser branch; impl `ScalableTopicsApi for TokioRuntimeState`; driver translates DagWatch events into consumer wake-ups; (6) magnetar-runtime-moonpool — impl `ScalableTopicsApi for Client<P>`; crates/magnetar-runtime-moonpool/tests/scalable_topic_broker.rs (NEW) — scripted controller-broker (replies to lookup, opens DagWatch, pushes 2 updates: 1 split + 1 merge, then closes); `BrokerWorkload::ScalableTopic` variant in sim_chaos.rs; (7) magnetar-cli `topic-info <topic://...>` subcommand (~80 LOC, prints segment DAG). Test layers per ADR-0024 — all binding: (a) proto unit (9 tests incl. encoder roundtrip + v4-shape byte-identical guard + monotonic update_seq + split/merge), (b) tokio integration in crates/magnetar-runtime-tokio/tests/scalable_topic.rs (4 tests incl. `scalable_topics_feature_off_does_not_export` compile_error proof), (c) moonpool 1:1 mirror with 100% diff coverage via `check-sim-coverage`, (d) differential equivalence + golden trace crates/magnetar-differential/tests/golden/scalable_topic_drop_on_split.json. E2E gated behind `#[ignore = "e2e: requires Pulsar 5.0 with PIP-460"]` + `feature = "e2e,scalable-topics"` — crates/magnetar/tests/e2e_scalable_topic.rs (NEW) does NOT block v0.2.0 release-cut. Docs: docs/scalable-topics.md (NEW with experimental banner + drop-on-change semantics), parity-status.md row → 🟡 experimental, README parity matrix row, flip ADR-0031 to Accepted. Land in this exact order to keep `check-runtime-test-parity` green: (a) before (b); moonpool ScalableTopicBroker fake before any tokio test; differential after both engines have green tests. Out of scope (v0.3.0+ markers): QueueConsumer, CheckpointConsumer, controller-election awareness, transparent segment failover, in-place repartition, segment-aware sticky-key dispatch.
```

---

## 9. Moonpool transport TLS + supervised-loop coverage

**Gap.** Per-file coverage on the moonpool runtime:

| File | Coverage | Gap |
| --- | --- | --- |
| `src/consumer.rs`  | 75.4% | 154 lines |
| `src/driver.rs`    | 54.7% | 141 lines |
| `src/lib.rs`       | 92.4% |  16 lines |
| `src/producer.rs`  | 85.4% |  55 lines |
| `src/transport.rs` | 30.3% | 124 lines |

The largest hunks live in `src/transport.rs` (TLS pump incl.
`connect_tls` / `tls_handshake` / TLS-side `read_buf` /
`write_all` / `flush`) and `src/driver.rs` (supervised reconnect
loop + anti-thrash cooldown).

**Why it stays open.** Needs either a TLS-enabled in-process broker
fixture (rustls server cert + `RustlsByteAdapter` peer driver) or a
`moonpool_core::SimProviders` substrate. Both are scaffolding work
but not architecturally blocked.

**`/goal`.**

```text
/goal close the residual moonpool transport TLS + driver supervised-loop coverage hunks per docs/follow-ups.md §9. Stand up an in-process rustls-enabled broker fixture (self-signed cert + `RustlsByteAdapter` peer driver) under crates/magnetar-runtime-moonpool/tests/, then add targeted tests that exercise `Transport::connect_tls`, `tls_handshake`, the TLS variants of `read_buf` / `write_all` / `flush`, and `Transport::shutdown`. Pair each new moonpool test with a same-named tokio counterpart (the tokio path is already covered via tls_handshake_chaos.rs; the mirror may be a Debug / fmt smoke if the surface is engine-private). Optionally close the remaining `driver.rs` `supervised_driver_loop` lines via a synthetic peer that drops the socket between handshakes. Validation chain per CLAUDE.md.
```

---

## 10. Golden trace catalog extension

**Gap.** The differential harness ships seven golden traces
(round-trip, batch, nack-redelivery, seek-to-start, many-publishes,
lookup-before-open, seek-per-partition). Missing:

- **Transactional ack paths** — ~180 LOC scripted-broker addition
  (`CommandEndTxn` + per-txn ack ledger).
- **`cryptoFailureAction` matrix** — ~240 LOC; **blocked** on
  porting the PIP-4 crypto bridge to moonpool.

**`/goal` (transactional ack — actionable now).**

```text
/goal add the transactional-ack golden trace per docs/follow-ups.md §10. Extend crates/magnetar-differential/src/scripted_broker.rs to handle `CommandEndTxn` (with per-txn ack ledger keyed by `TxnId`) and `CommandAck` carrying a `txn_id`. New trace at crates/magnetar-differential/tests/golden/txn_ack.json exercises: NewTxn → produce + ack-within-txn × N → CommandEndTxn(commit) → assert ledger drained. Mirror via crates/magnetar-differential/tests/golden_traces.rs::run_txn_ack_trace (tokio + moonpool both). Validation chain per CLAUDE.md.
```

**`/goal` (cryptoFailureAction — blocked on crypto bridge port).**

```text
/goal add the cryptoFailureAction matrix golden trace per docs/follow-ups.md §10 — DEPENDS on porting the PIP-4 message crypto bridge (currently in magnetar-messagecrypto + magnetar-runtime-tokio) to the moonpool runtime first. Once the moonpool MessageEncryptor/Decryptor are in place, extend the scripted broker to deliver a payload with intentionally-corrupt ciphertext and assert each `CryptoFailureAction` arm (Fail / Discard / Consume) at the consumer surface. Golden trace at crates/magnetar-differential/tests/golden/crypto_failure_action.json. Validation chain per CLAUDE.md.
```

---

## 11. Moonpool runner `LocalSet` pump

**Gap.** The differential moonpool runner's driver task is
`spawn_local`'d into a [`tokio::task::LocalSet`](https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html)
because [`moonpool_core::TokioProviders`]'s `TaskProvider` uses
`tokio::task::Builder::new().spawn_local(...)`. While the test outer
task is parked on `consumer.receive()`, the spawn_local'd driver
only runs when the LocalSet's `run_until` is polled — and the proto
slab waker we now fire on delivery is dispatched from the driver
task, which itself isn't being polled. Result: ~30 s stall per
`Recv` until the proto keepalive deadline elapses and pumps the
chain.
[`crates/magnetar-differential/src/runner_moonpool.rs`](../crates/magnetar-differential/src/runner_moonpool.rs)
keeps a 25 ms `Kicker` pulsing `driver_waker.notify_one()` to bridge
the LocalSet pump gap.

**Why it stays open.** Closed by the future `moonpool-sim`
integration; the simulator's deterministic scheduler drives both
sides without `spawn_local`. Alternative: restructure the runner to
spawn the driver via plain `tokio::spawn` (gives up moonpool-sim
compatibility for the differential harness specifically).

**Deferred** — the 25 ms Kicker workaround is correct, just
inelegant. Revisit when adopting moonpool-sim or when the Kicker
becomes a measurable test-flakiness source.

---

## 12. `engine.rs` split

**Gap.** `crates/magnetar/src/engine.rs` is 2148 lines. Split into
`engine/{traits.rs, tokio.rs, moonpool.rs}` once the per-engine
impls grow further.

**Deferred** — today the trait + two impls fit comfortably. Trigger
when the next per-engine surface lift (e.g. §3, §4) makes the file
uncomfortably wide.

---

## 13. `ProducerExt` trait inline

**Gap.** `crates/magnetar/src/client.rs::ProducerExt` is a single-
impl extension trait that exists only to satisfy Rust's orphan rule
for the façade-defined `MessageBuilder` against the runtime-defined
`Producer`. The original audit suggested inlining it as a direct
method on `magnetar_runtime_tokio::Producer` — but that requires
moving `MessageBuilder` + `OutgoingMessage` (currently in
`magnetar/src/client.rs`) **down** into `magnetar-runtime-tokio`,
which inverts the workspace dep graph.

**Needs decision.** Two options:

1. Move `MessageBuilder` / `OutgoingMessage` to a new shared crate
   that both `magnetar` and `magnetar-runtime-tokio` depend on
   (cleanest but adds a crate).
2. Accept the `ProducerExt` trait as the layering artefact
   (zero-cost; the trait + single impl is the canonical Rust
   workaround for this case).

Florentin's call. Most projects pick option 2 unless the trait
proliferates.

---

## Notes on this file

Items move from this file to `git log` when their commit lands. The
expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays
   open** + (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the
   post-implementation reference).

Pending **decisions** (§5 crypto crate, §13 layering) live here
until Florentin calls them. Once decided, the decision becomes an
ADR (or the `/goal …` block is filled in) and the entry transitions
to ⚡ ready-to-dispatch.
