# ADR-0032 — PIP-466 V5 client surface scope

- **Status**: Accepted
- **Date**: 2026-05-26 (Proposed), 2026-05-28 (Accepted)
- **Decider**: Florentin Dubois
- **Tags**: pip-466, v5-client, api-surface, scope, experimental

## Acceptance note (2026-05-28)

Accepted alongside the unified engine-generic refactor that landed under `docs/follow-ups.md` §2 (former §2 phantom-E + §3 per-surface lifts + §4 V5 engine-genericity, single PR).
The V5 surface (`PulsarClientV5`, `v5::Producer`, `v5::StreamConsumer`, `v5::QueueConsumer` and their builders) is now parametric over `E: Engine` with default `E = TokioEngine`.
Moonpool callers can name `PulsarClientV5<MoonpoolEngine<P>>` directly; existing tokio call sites that write `PulsarClientV5` without a second type argument keep resolving to the tokio specialisation.
The `experimental-v5-client` feature gate stays default-off — acceptance flips the ADR status, not the feature default.
The 5 V5 magnetar-tier tests (`v5_producer_mapping.rs`, `v5_stream_consumer_mapping.rs`, `v5_queue_consumer_mapping.rs`, `v5_client_v4_escape_hatch.rs`, `v5_builder_defaults.rs`) now have moonpool 1:1 mirrors at `crates/magnetar/tests/v5_*_moonpool.rs` (engine-shape pinning + sans-io wire assertions), satisfying the acceptance gate.

## Context

[ADR-0010](0010-v0-1-full-java-parity.md) listed PIP-466 alongside PIP-460 with an "experimental tag".
PIP-466 is the **V5 Java Client API** — a clean-slate redesign that lives **beside** the existing v4 surface as a separate set of modules, introduces three distinct consumer types (StreamConsumer / QueueConsumer / CheckpointConsumer), and uses `Duration` / `Optional` semantics throughout.
Per [ADR-0026 §D1](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) the Pulsar Java client agent surfaced that PIP-466 "is additive and does not introduce per-surface engine abstractions" — i.e. it does not change magnetar's engine trait.

PIP-466 is **explicitly client-side**. From upstream: "the V5 API exists as new modules alongside the current implementation … no changes are required to existing applications."
The single exception is one new wire command, `CommandScalableTopicLookup`, which is shared with **PIP-460** ([ADR-0031](0031-pip-460-scalable-subscription-scope.md)) and is vendored once for both PIPs.

Before this ADR, there was no PIP-466 scaffolding in magnetar.
[`crates/magnetar/src/lib.rs`](../../crates/magnetar/src/lib.rs) exports the v4-equivalent `PulsarClient`, `Producer<T, E>`, `Consumer<T, E>`, `Reader<T, E>`, `MultiTopicsConsumer`, `PartitionedProducer`, `PartitionedConsumer`, `PatternConsumer`, `TableView`, `Transaction` — the eight surfaces lifted to `Surface<T, E>` per [ADR-0026 §D1](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md).
There was no parallel V5 surface and no `magnetar::v5` module.

This ADR locks the V5 surface scope: a **subset** that mirrors Pulsar Java's PIP-466 V5 design at the API-shape level (ergonomics, type signatures, builder semantics) but does **not** duplicate the v4 surface end to end.
The V5 surface ships as **experimental** behind a feature flag; v4 stays the documented default.
A future ADR may promote V5 to default once the upstream V5 design stabilises.

## Decision

- **Wire-protocol delta vs. current vendored PulsarApi.proto: none.** PIP-466 is purely a client-side API redesign.
  The one wire command introduced — `CommandScalableTopicLookup` — is shared with PIP-460 and is vendored under [ADR-0031](0031-pip-460-scalable-subscription-scope.md); this ADR does **not** drive a second proto bump.
  **None** is the explicit answer here.

- **`magnetar-proto` state-machine additions.** **None.** PIP-466 reuses the existing v4 wire driver (`Conn`, `Producer`, `Consumer` trackers, `Reader`).
  The V5 surface is a thin re-skin over the same sans-io machinery.

- **`magnetar-runtime-tokio` surface.**
  - New module: `magnetar::v5` behind a new feature flag, `experimental-v5-client`, **default off**.
  - New public types:
    - `magnetar::v5::PulsarClientV5<E: Engine>` — owns the same `Arc<E::ClientState>` as the v4 `PulsarClient<E>`; constructed via `PulsarClientV5::builder().service_url(...).build()`.
    - `magnetar::v5::StreamConsumer<T, E>` — ordered consume, duration-typed config, `Optional`-typed builder fields.
      Maps onto the v4 `Consumer<T, E>` with `SubscriptionType::Exclusive` or `Failover` semantics.
    - `magnetar::v5::QueueConsumer<T, E>` — parallel-work consume, maps onto v4 `Consumer<T, E>` with `Shared` or `KeyShared` semantics.
    - `magnetar::v5::Producer<T, E>` — `Duration`-typed `send_timeout`, `Option<MessageId>`-typed return on async send.
  - The v5 surface **delegates** all I/O to the existing v4 surface types — it is an API skin, not a re-implementation.
    Internally, `magnetar::v5::StreamConsumer<T, E>` holds a v4 `Consumer<T, E>` and forwards calls.
  - **Out of scope**: `magnetar::v5::CheckpointConsumer` (requires PIP-460 controller-broker coordination beyond our PIP-460 minimum surface), `magnetar::v5::Reader` (v4 `Reader` is sufficient for current use cases), `magnetar::v5::TableView`, `magnetar::v5::Transaction`.
    Tracked as a follow-up ADR.

- **`magnetar-runtime-moonpool` port.** Because V5 is a thin skin over v4, **the moonpool engine inherits the V5 surface for free** — the V5 types are `<E: Engine>`-generic via the [ADR-0026 §D1](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) pattern and the underlying v4 surfaces already work under `MoonpoolEngine<P>`.
  No new sim-side fakes needed beyond what already supports the v4 producer/consumer paths.

- **No GAT growth on `Engine`.** Per ADR-0026 §D1: V5 surfaces take `E: Engine` directly.
  No `Engine::StreamConsumer<T>`, no `Engine::QueueConsumer<T>` GATs.
  The decision is consistent with the Pulsar Java client agent's finding that the V5 design is "additive and does not introduce per-surface engine abstractions."

- **Experimental tag, not silent.** The `magnetar::v5` module is feature-gated and every public type carries a `#[doc = "**Experimental** (PIP-466 V5 client surface)"]` banner.
  README's parity matrix marks PIP-466 as `🟡 experimental — Stream/Queue consumers + Producer; no Reader/TableView/Transaction/CheckpointConsumer in V5 module (use v4 surfaces for those)`.

## Consequences

- **Test layers per ADR-0024 (4-layer):** Because the V5 surface delegates to v4, the binding test plan centres on the **API translation layer**: (a) `magnetar-proto` unit: **N/A** — no proto changes (carve-out per ADR-0024 §Exemptions, justified in the commit message: V5 is an API re-skin with no wire/state-machine surface).
  (b) `magnetar-runtime-tokio`: integration tests asserting that v5 builder defaults and `Duration` / `Optional` mappings produce the same wire behaviour as the equivalent v4 builder invocations.
  One mirror test per V5 surface.
  (c) `magnetar-runtime-moonpool`: identical mirror set under `SimulationBuilder` — 1:1 with (b) per ADR-0024.
  (d) `magnetar-differential`: not required (no new sans-io surface) — exemption justified in the commit message per ADR-0024 §Exemptions.

- **E2E fixture needs.** No new fixtures.
  Existing `e2e_*` tests parameterised over `(v4_client, v5_client)` against the existing `apachepulsar/pulsar:4.0.4` image.
  The single PIP-466-specific test — `CommandScalableTopicLookup` — requires Pulsar 5.0 and is covered by [ADR-0031](0031-pip-460-scalable-subscription-scope.md)'s e2e set, not this ADR's.

- **LOC estimate.** ~600–900 LOC total. Breakdown: ~300 LOC `magnetar::v5` module (3 surface types + builders); ~150 LOC v4 → v5 delegation glue; ~300 LOC tests (mirrored across engines) + doc-tests; ~50 LOC README parity matrix update + module doc-banners.

- **Security implications.** None new.
  The V5 surface inherits auth, TLS, and trust-boundary semantics from the v4 surface it wraps.

## Status

Accepted (2026-05-28).

## References

- [ADR-0009](0009-pulsar-4-minimum.md) — Pulsar 4.0+ minimum (V5 surface still works on 4.x for the non-scalable subset).
- [ADR-0010](0010-v0-1-full-java-parity.md) — parity scope; PIP-466 ships as experimental V5 surface alongside v4.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — four-layer test plan; this ADR claims partial exemptions justified by the no-wire-delta nature of the change.
- [ADR-0026 §D1](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) — concrete `Surface<T, E>` design; V5 surfaces take `E: Engine` directly, no per-surface GATs.
- [ADR-0031](0031-pip-460-scalable-subscription-scope.md) — PIP-460 carries the wire-protocol delta; PIP-466 piggy-backs on it for `CommandScalableTopicLookup`.
- PIP-466 (V5 Client) —
  <https://github.com/apache/pulsar/blob/master/pip/pip-466.md>
