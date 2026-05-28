# ADR-0031 — PIP-460 scalable-topic / subscription scope for v0.2.0

- **Status**: Accepted
- **Date**: 2026-05-26 (accepted 2026-05-28)
- **Decider**: Florentin Dubois
- **Tags**: pip-460, scalable-topics, segments, v0.2.0, scope, experimental

## Context

[ADR-0010](0010-v0-1-full-java-parity.md) listed PIP-460 / PIP-466
(scalable topics) **with an "experimental tag"** in v0.1.0 scope.
On closer reading of the upstream PIP-460 design — which spans
multiple sub-PIPs, introduces a new `topic://` URL scheme, three new
consumer types (StreamConsumer, QueueConsumer, CheckpointConsumer),
an elected controller broker, segment-DAG management, and persistent
bidirectional streams for DAG watch sessions — shipping any
meaningful PIP-460 surface in v0.1.0 is not realistic. The upstream
itself targets **Pulsar 5.0 LTS** for the phased rollout.

magnetar's [ADR-0009](0009-pulsar-4-minimum.md) baselines the
broker at 4.0+. PIP-460 wire features will not be present in the
broker we target for v0.1.0. The honest decision is to **defer
the PIP-460 client implementation to v0.2.0** as an experimental
surface and **lift PIP-460 out of ADR-0010's v0.1.0 scope** in a
follow-up amendment. ADR-0031 (this file) locks the v0.2.0 scope;
the ADR-0010 amendment is tracked as a separate `docs(adr): amend
ADR-0010 §scalable-topics` commit and follows once Florentin signs
off here.

Today there is no scalable-topic scaffolding in magnetar. The
`magnetar::client::ClientBuilder` only knows persistent and
non-persistent topic URLs (`persistent://` / `non-persistent://`);
the `magnetar-proto` consumer state machine assumes a
fixed-partition `CommandLookupTopic` flow
([`crates/magnetar-proto/src/lookup.rs`](../../crates/magnetar-proto/src/lookup.rs))
and a single-segment `MessageId`
([`crates/magnetar-proto/src/types.rs`](../../crates/magnetar-proto/src/types.rs)).
None of the PIP-460 wire commands
(`CommandScalableTopicLookup`, segment-DAG-watch session frames)
are present in the vendored proto.

This ADR locks the **bounded experimental surface** magnetar will
ship in v0.2.0: enough to consume from a scalable topic against a
Pulsar 5.0+ broker on the **happy path**, plus the wire and proto
groundwork. The full DAG-aware split/merge surface, checkpoint
consumer, and controller-broker coordination are out of scope and
will be revisited in a v0.3.0+ ADR.

## Decision

- **Wire-protocol delta vs. current vendored PulsarApi.proto:
  significant.** PIP-460 introduces (a) `CommandScalableTopicLookup`
  + response for segment-aware lookup, (b) extended `MessageId`
  with a `segment_id` field, (c) `CommandSegmentDagWatch` +
  `CommandSegmentDagUpdate` bidirectional-stream frames for the
  controller broker's DAG-change notifications. **In v0.2.0 we
  vendor PIP-460's proto additions** by running
  `cargo run -p xtask -- vendor-proto --rev <pulsar-5.0-rc-sha>`
  once upstream tags a Pulsar 5.0 candidate including PIP-460.
  Vendoring is a separate commit per [ADR-0026 §D4](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md).

- **`magnetar-proto` state-machine additions.**
  - New entry point on `Conn`: handle `CommandScalableTopicLookup`
    response → emit
    `Event::ScalableTopicLookupResolved { segments: Vec<SegmentDescriptor>, controller_broker_url, lookup_token }`.
  - New `SegmentDescriptor` type: `{ segment_id: SegmentId,
    partition_key_range: KeyRange, broker_url: ServiceUrl,
    state: SegmentState }`.
  - Extended `MessageId` carrying `Option<SegmentId>` (none =
    legacy partitioned/non-partitioned topic; some = scalable).
    The encoder/decoder respects the optional-presence semantics
    so legacy producers don't break.
  - New `DagWatchSession` handle type — sans-io: emits
    `Action::SendDagWatchSubscribe` once, then consumes
    `CommandSegmentDagUpdate` frames into
    `Event::SegmentDagUpdated { added, removed, split_events,
    merge_events }`.
  - **Out of scope for v0.2.0**: controller-election awareness
    on the client side, transparent segment-failover during
    consume, in-place key-range repartitioning under load. The
    client opens a `DagWatchSession`, reads the current DAG,
    consumes against the published segments, and drops the
    subscription if the DAG changes underneath it (surfaces an
    `Event::DagChangedDuringConsume` and lets the caller
    re-resolve). This is the "experimental tag" position.

- **`magnetar-runtime-tokio` surface.**
  - New parser path on `ClientBuilder::topic("topic://<...>")`
    routing to scalable-topic resolution. The legacy
    `persistent://` / `non-persistent://` paths are untouched.
  - New public type:
    `magnetar::scalable::StreamConsumer<T, E: Engine>` that wraps
    the existing `Consumer<T, E>` machinery but binds it to a
    `DagWatchSession`. The MVP is StreamConsumer only;
    `QueueConsumer` and `CheckpointConsumer` are not in v0.2.0
    scope (call out via an
    `// TODO: PIP-460 QueueConsumer (v0.3.0+)` marker in the
    crate root).
  - New feature flag: `scalable-topics` on the `magnetar` crate,
    **default off**. Compiling without the flag preserves the
    v0.1.0 surface bit-for-bit. The CLI parses `topic://` URLs
    behind the same feature flag.
  - New `ScalableTopicsBuilder` extension trait via the
    [ADR-0026 §D1](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
    extension-trait pattern, gated on
    `where E::ClientState: ScalableTopicsApi`.

- **`magnetar-runtime-moonpool` port.** The
  `BrokerWorkload` in
  [`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`](../../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs)
  grows a `ScalableTopicBroker` variant that scripts a
  segment-DAG-update sequence (initial DAG; one split event; one
  merge event). The sans-io component is the existing
  `DagWatchSession` state machine; the moonpool-side fake is the
  scripted broker. No GAT changes — `ScalableTopicsApi` is a new
  extension trait on `Engine::ClientState` per the D1 contract.

- **Experimental tag, not silent.** The scalable-topics surface
  ships behind `feature = "scalable-topics"` and every public
  type carries a `#[doc = "**Experimental** (PIP-460 v0.2.0)"]`
  banner. README's parity matrix marks PIP-460 as
  `🟡 experimental — StreamConsumer only, no Queue/Checkpoint`.

## Consequences

- **Test layers per ADR-0024 (4-layer):**
  (a) `magnetar-proto` unit: encode/decode for the new commands;
  `Conn` state-machine transitions for `ScalableTopicLookup` and
  `DagWatchSession`; `MessageId` round-trip with and without
  `segment_id`; legacy `MessageId` decode for backward compat.
  (b) `magnetar-runtime-tokio` integration: `topic://` URL
  parsing + StreamConsumer happy path against `magnetar-fakes`
  scripted broker.
  (c) `magnetar-runtime-moonpool` integration: same scripted
  scenario under `SimulationBuilder` with one split + one merge
  event.
  (d) `magnetar-differential`: equivalence of the `EventStream`
  on the StreamConsumer surface across engines.
  The 1:1 test count rule of ADR-0024 stays binding; the new
  surface adds matched tests on both sides.

- **E2E fixture needs.** **Blocked on upstream Pulsar 5.0 RC.**
  e2e requires an `apachepulsar/pulsar:5.0.0-rc-*` image
  configured with PIP-460 enabled (`scalableTopicsEnabled=true`).
  Until Pulsar 5.0 ships a tagged image, e2e is gated behind
  `#[ignore = "e2e: requires Pulsar 5.0 with PIP-460"]`. The
  v0.2.0 release-cut decision states e2e is **best-effort** on
  this surface — the 4-layer test set above is the binding
  acceptance gate.
  <!-- TODO: verify Pulsar 5.0 image tag once upstream cuts an RC. -->

- **LOC estimate.** ~1500–2200 LOC total. Breakdown:
  ~400 LOC `magnetar-proto` (new commands, MessageId extension,
  DagWatchSession state machine); ~500 LOC `StreamConsumer<T, E>`
  + builder + lookup glue; ~250 LOC `ScalableTopicsApi`
  extension-trait impls per engine; ~250 LOC moonpool
  ScalableTopicBroker fake; ~400 LOC tests (4-layer).

- **Security implications.** Limited. Segment metadata is
  authority-of-the-broker (the controller broker signs DAG
  updates via the existing broker auth boundary). The
  `DagWatchSession` accepts updates only from the same TLS-validated
  controller-broker URL it opened the session against — same
  trust boundary as today's lookup flow.

## Status

Accepted (2026-05-28). The scaffold landed per the scope locked above:
proto wire commands (hand-encoded behind `feature = "scalable-topics"` until
the Pulsar 5.0 RC vendor bump), the `DagWatchSession` state machine, the
`MessageId.segment_id` extension, both-engine `ScalableTopicsApi` impls, the
`magnetar::scalable::StreamConsumer` surface (drop-on-DAG-change), the CLI
`topic-info` subcommand, and the four-layer test set (proto unit + tokio +
moonpool 1:1 + differential equivalence with a golden trace). E2E is
`#[ignore]`'d behind `feature = "e2e,scalable-topics"` and **does not block
the v0.2.0 release-cut** — it can only run once upstream cuts a Pulsar 5.0 RC
shipping PIP-460. See [`docs/scalable-topics.md`](../../docs/scalable-topics.md)
and the implementation in `git log`.

## References

- [ADR-0009](0009-pulsar-4-minimum.md) — Pulsar 4.0+ minimum;
  PIP-460 requires 5.0+, hence experimental-only in v0.2.0.
- [ADR-0010](0010-v0-1-full-java-parity.md) — v0.1.0 parity
  scope; this ADR is the basis for an amendment lifting PIP-460
  out of v0.1.0 scope.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) —
  four-layer test plan binding.
- [ADR-0026 §D1](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
  — extension-trait pattern (`ScalableTopicsApi`).
- [ADR-0026 §D4](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
  — milestone-based proto vendor bump.
- PIP-460 (Scalable Topics) —
  <https://github.com/apache/pulsar/blob/master/pip/pip-460.md>
- See companion [ADR-0032](0032-pip-466-v5-client-surface-scope.md)
  for the PIP-466 V5 client surface that consumes PIP-460.
