# PIP-460 — Scalable topics / DAG-watch consumer (experimental, v0.2.0)

- **Status**: Accepted (scaffold landed 2026-05-28; ADR-0031 Accepted)
- **ADR**: [ADR-0031](../adr/0031-pip-460-scalable-subscription-scope.md)
- **Target**: v0.2.0
- **Date**: 2026-05-26
- **Owner**: Florentin Dubois
- **Upstream**: [pip/pip-460.md](https://github.com/apache/pulsar/blob/master/pip/pip-460.md)
- **Upstream readiness**: 🔴 **NOT LIVE.** Upstream PIP is `Draft`,
  targeting Pulsar 5.0 LTS (Oct 2026) with phased rollout via 4.3.0 /
  4.4.0. No released Pulsar broker ships PIP-460 wire surface today.
  4-layer tests can land against in-process fakes; e2e is gated on
  upstream cutting a 5.0 RC.
- **Broker baseline shift**: requires `apachepulsar/pulsar:5.0.0+`; the
  client compiled without `feature = "scalable-topics"` stays
  Pulsar-4.0+ compatible per [ADR-0009](../adr/0009-pulsar-4-minimum.md).

## TL;DR

PIP-460 introduces a new `topic://<...>` URL scheme, segment-DAG
metadata, three new wire commands (`CommandScalableTopicLookup`,
`CommandSegmentDagWatch`, `CommandSegmentDagUpdate`), and a
controller-broker session model. magnetar v0.2.0 ships **only the
StreamConsumer happy path** behind a default-off feature flag, drops
on DAG-change-mid-consume (no transparent failover), and gates e2e on
upstream cutting a Pulsar 5.0 RC. The other two PIP-460 consumer types
(`QueueConsumer`, `CheckpointConsumer`), controller election, and
in-place repartition are explicit v0.3.0+ work.

## 1. Wire-protocol delta vs. vendored `PulsarApi.proto`

**Significant** — three new commands and one extended `MessageIdData`
field. None of these are in the current vendored proto.

The delta below is **the proposed shape** based on the upstream PIP
draft as of 2026-05-26. The authoritative bump lands when upstream
tags a Pulsar 5.0 RC including PIP-460 — at that point we run
`cargo run -p xtask -- vendor-proto --rev <pulsar-5.0-rc-sha>` as a
**dedicated commit per** [ADR-0026 §D4](../adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md).
The vendor commit must not co-mingle hand-written code; the proposal
work below predicates on it.

<!-- TODO: confirm final field numbers + names against the Pulsar 5.0
     RC proto once it cuts. Field numbers below are best-effort guesses
     from the PIP-460 draft and will be reconciled by the vendor bump. -->

### 1.1 New `MessageIdData` field — segment id

Extends [`PulsarApi.proto:59-69`](../../crates/magnetar-proto/proto/PulsarApi.proto)
with an optional `segment_id`. Omitting the field preserves wire
compatibility with v4 partitioned/non-partitioned topics — legacy
producers and consumers continue to round-trip bit-for-bit:

```proto
message MessageIdData {
    required uint64 ledgerId = 1;
    required uint64 entryId  = 2;
    optional int32 partition = 3 [default = -1];
    optional int32 batch_index = 4 [default = -1];
    repeated int64 ack_set = 5;
    optional int32 batch_size = 6;
    optional MessageIdData first_chunk_message_id = 7;
    optional uint64 segment_id = 8;   // NEW — PIP-460
}
```

### 1.2 `CommandScalableTopicLookup` + response

Sibling to [`CommandLookupTopic`](../../crates/magnetar-proto/proto/PulsarApi.proto)
(line 448) and its response (line 468). The response returns the full
**current segment DAG** for the topic plus the controller-broker URL
the client must open a `CommandSegmentDagWatch` session against:

```proto
message CommandScalableTopicLookup {
    required string topic       = 1;
    required uint64 request_id  = 2;
    optional bool authoritative = 3 [default = false];
    // Reuse the lookup-style auth carry-through.
    optional string original_principal   = 4;
    optional string original_auth_data   = 5;
    optional string original_auth_method = 6;
}

message CommandScalableTopicLookupResponse {
    enum LookupType { Redirect = 0; Connect = 1; Failed = 2; }
    required uint64 request_id            = 1;
    required LookupType response          = 2;
    optional string controller_broker_url = 3;  // for DagWatch session
    optional string controller_broker_url_tls = 4;
    repeated SegmentDescriptor segments   = 5;  // current DAG snapshot
    optional uint64 lookup_token          = 6;  // monotonic, used in DagWatch
    optional ServerError error            = 7;
    optional string message               = 8;
}

message SegmentDescriptor {
    required uint64 segment_id            = 1;
    required string broker_url            = 2;
    optional string broker_url_tls        = 3;
    required uint32 key_range_start       = 4;
    required uint32 key_range_end         = 5;
    enum SegmentState { Active = 0; Splitting = 1; Merging = 2; Sealed = 3; }
    required SegmentState state           = 6;
}
```

### 1.3 `CommandSegmentDagWatch` + `CommandSegmentDagUpdate`

Bidirectional-stream-like pair carried on the existing TCP connection
to the **controller broker** (not the segment leaders). Subscribe is
single-frame; updates are pushed by the broker until the client sends
`CommandCloseSegmentDagWatch`:

```proto
message CommandSegmentDagWatch {
    required string topic            = 1;
    required uint64 request_id       = 2;
    required uint64 watch_session_id = 3;  // client-allocated
    required uint64 lookup_token     = 4;  // from CommandScalableTopicLookupResponse
}

message CommandSegmentDagWatchResponse {
    required uint64 watch_session_id = 1;
    required uint64 request_id       = 2;
    optional ServerError error       = 3;
    optional string message          = 4;
}

message CommandSegmentDagUpdate {
    required uint64 watch_session_id = 1;
    required uint64 update_seq       = 2;       // monotonic per session
    repeated SegmentDescriptor added   = 3;
    repeated uint64 removed            = 4;     // segment_ids
    repeated SplitEvent split_events   = 5;
    repeated MergeEvent merge_events   = 6;
}

message SplitEvent {
    required uint64 parent_segment_id = 1;
    repeated uint64 child_segment_ids = 2;
    required uint64 split_at_entry    = 3;
}
message MergeEvent {
    repeated uint64 parent_segment_ids = 1;
    required uint64 child_segment_id   = 2;
    required uint64 merge_at_entry     = 3;
}

message CommandCloseSegmentDagWatch {
    required uint64 watch_session_id = 1;
    required uint64 request_id       = 2;
}
```

### 1.4 `BaseCommand` discriminator additions

`PulsarApi.proto` `enum Type` (around `BaseCommand` near line 1188) gains
six new variants and `optional` fields:

```
SCALABLE_TOPIC_LOOKUP          = 80;
SCALABLE_TOPIC_LOOKUP_RESPONSE = 81;
SEGMENT_DAG_WATCH              = 82;
SEGMENT_DAG_WATCH_RESPONSE     = 83;
SEGMENT_DAG_UPDATE             = 84;
CLOSE_SEGMENT_DAG_WATCH        = 85;
```

### 1.5 New `ProtocolVersion` minimum

Adds `v22` (or whatever upstream assigns) to
[`PulsarApi.proto:280`](../../crates/magnetar-proto/proto/PulsarApi.proto)
`enum ProtocolVersion`. The client only advertises `v22+` when the
`scalable-topics` feature is on; otherwise it caps at the v0.1.0
ceiling. The `SUPPORTED_PROTOCOL_VERSION` typed constant in
`magnetar-proto` ([`crates/magnetar-proto/src/lib.rs`](../../crates/magnetar-proto/src/lib.rs))
gets a parallel `SUPPORTED_PROTOCOL_VERSION_SCALABLE_TOPICS` constant.

## 2. `magnetar-proto` state-machine additions

All additions are in `magnetar-proto`, sans-io,
[ADR-0011](../adr/0011-clock-injection-sans-io.md) clock-injection
clean, no I/O deps per [ADR-0004](../adr/0004-sans-io-protocol-core.md).
File mapping:

| Concern | File |
| --- | --- |
| Lookup-response handler | [`crates/magnetar-proto/src/lookup.rs`](../../crates/magnetar-proto/src/lookup.rs) |
| Segment / `MessageId` types | [`crates/magnetar-proto/src/types.rs`](../../crates/magnetar-proto/src/types.rs) |
| `Conn` driver entries | [`crates/magnetar-proto/src/conn.rs`](../../crates/magnetar-proto/src/conn.rs) |
| New `DagWatchSession` state machine | `crates/magnetar-proto/src/dag_watch.rs` (NEW) |
| `Event` enum extensions | [`crates/magnetar-proto/src/event.rs`](../../crates/magnetar-proto/src/event.rs) |

### 2.1 New / extended types (`types.rs`)

```rust
pub struct SegmentId(pub u64);
pub struct KeyRange { pub start: u32, pub end: u32 }
#[non_exhaustive]
pub enum SegmentState { Active, Splitting, Merging, Sealed }

pub struct SegmentDescriptor {
    pub segment_id: SegmentId,
    pub key_range: KeyRange,
    pub broker_url: ServiceUrl,
    pub state: SegmentState,
}

// Extended on existing MessageId — additive Option to preserve v4 layout.
pub struct MessageId {
    /* existing fields … */
    pub segment_id: Option<SegmentId>,
}
```

`MessageId::eq` and `MessageId::cmp` ignore `segment_id` when both
sides are `None` (v4 invariant preserved) and treat `Some(a) == Some(b)
&& a == b` for scalable topics. Cross-mode comparison
(`Some(_)` vs. `None`) is `false` — caller is expected to know which
mode the message came from.

### 2.2 New `Conn` entries (`conn.rs`)

```rust
impl Conn {
    /// Initiate scalable-topic lookup. Returns request_id for caller correlation.
    pub fn send_scalable_topic_lookup(
        &mut self,
        topic: &TopicName,
        authoritative: bool,
        now: Instant,
    ) -> RequestId;

    /// Open a DAG-watch session against the controller broker.
    /// Caller MUST have an open connection to controller_broker_url.
    pub fn open_dag_watch(
        &mut self,
        topic: &TopicName,
        lookup_token: u64,
        now: Instant,
    ) -> WatchSessionId;

    pub fn close_dag_watch(&mut self, sid: WatchSessionId, now: Instant);
}
```

### 2.3 New `DagWatchSession` (`dag_watch.rs` — NEW)

```rust
pub struct DagWatchSession { /* monotonic update_seq, last_dag, … */ }

impl DagWatchSession {
    pub fn new(initial_dag: Vec<SegmentDescriptor>, lookup_token: u64) -> Self;

    /// Apply a SEGMENT_DAG_UPDATE frame; returns Event variants to surface.
    pub fn handle_update(&mut self, upd: SegmentDagUpdate)
        -> Result<DagDelta, DagError>;

    pub fn snapshot(&self) -> &[SegmentDescriptor];
}

pub struct DagDelta {
    pub added: Vec<SegmentDescriptor>,
    pub removed: Vec<SegmentId>,
    pub split_events: Vec<SplitEvent>,
    pub merge_events: Vec<MergeEvent>,
}

#[derive(Debug, thiserror::Error)]
pub enum DagError {
    #[error("non-monotonic update_seq: got {got} expected > {prev}")]
    NonMonotonic { got: u64, prev: u64 },
    #[error("update for unknown segment_id {0:?}")]
    UnknownSegment(SegmentId),
    /* … */
}
```

### 2.4 New `Event` variants (`event.rs`)

```rust
pub enum Event {
    /* … existing variants … */

    /// PIP-460: lookup resolved into a segment DAG + controller broker.
    ScalableTopicLookupResolved {
        request_id: RequestId,
        controller_broker_url: ServiceUrl,
        segments: Vec<SegmentDescriptor>,
        lookup_token: u64,
    },

    /// PIP-460: a DAG-watch session received an update.
    SegmentDagUpdated {
        session_id: WatchSessionId,
        delta: DagDelta,
    },

    /// PIP-460: the segment DAG changed while a StreamConsumer was
    /// actively consuming. Caller must re-resolve and re-subscribe.
    /// This is the "experimental, drop-on-change" guarantee.
    DagChangedDuringConsume {
        session_id: WatchSessionId,
        reason: DagChangeReason,
    },
}

pub enum DagChangeReason { Split, Merge, SegmentRemoved, Unknown }
```

### 2.5 Out of scope for v0.2.0 (`magnetar-proto`)

- Controller-election awareness: when the controller broker
  disconnects we surface `Event::DagWatchClosed { reason }` and let the
  caller decide. No automatic re-lookup.
- Transparent failover across split/merge events. Consumer is dropped
  and a `DagChangedDuringConsume` event is emitted.
- In-place repartitioning under live consumers.
- Segment-aware sticky-key dispatch (Key_Shared semantics across the
  full DAG). v0.2.0 KeyRange is observation-only.

## 3. Runtime surface ports

### 3.1 `magnetar-runtime-tokio`

| File | Change |
| --- | --- |
| [`crates/magnetar-runtime-tokio/src/lib.rs`](../../crates/magnetar-runtime-tokio/src/lib.rs) | Re-export `magnetar::scalable::{StreamConsumer, ScalableTopicsApi}` behind `feature = "scalable-topics"`. |
| [`crates/magnetar-runtime-tokio/src/client.rs`](../../crates/magnetar-runtime-tokio/src/client.rs) | Add `topic://` URL parser branch in builder; route to scalable lookup. |
| [`crates/magnetar-runtime-tokio/src/driver.rs`](../../crates/magnetar-runtime-tokio/src/driver.rs) | Translate `Event::SegmentDagUpdated` / `DagChangedDuringConsume` into async-task wake-ups for the consumer. |
| [`crates/magnetar-runtime-tokio/src/consumer.rs`](../../crates/magnetar-runtime-tokio/src/consumer.rs) | Add `pub use crate::scalable::StreamConsumer;` only — no v4 surface change. |
| `crates/magnetar-runtime-tokio/src/scalable.rs` (NEW) | `StreamConsumer<T>` implementation: holds `DagWatchSession`, opens one v4 `Consumer<T>` per active segment, surfaces `Event::Message` from the union. On `DagChangedDuringConsume`: closes all segment consumers, emits `ConsumerEvent::DagChanged` to the caller. |

`magnetar` façade ([`crates/magnetar/src/lib.rs`](../../crates/magnetar/src/lib.rs))
gains a `pub mod scalable` module gated by `feature = "scalable-topics"`,
exporting `StreamConsumer<T, E>` and the `ScalableTopicsApi` extension
trait. Per [ADR-0026 §D1](../adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md):

```rust
// crates/magnetar/src/scalable.rs (NEW)
pub trait ScalableTopicsApi { /* engine-side hooks */ }

impl ScalableTopicsApi for TokioRuntimeState { /* … */ }

pub struct StreamConsumer<T, E: Engine>
where
    E::ClientState: ScalableTopicsApi,
{ /* … */ }

impl<T, E> StreamConsumer<T, E>
where
    E::ClientState: ScalableTopicsApi,
{
    pub fn builder() -> StreamConsumerBuilder<T, E> { /* … */ }
}
```

Builder method:

```rust
impl<T, E: Engine> ClientBuilder<E>
where
    E::ClientState: ScalableTopicsApi,
{
    pub fn scalable_stream_consumer<U>(self) -> StreamConsumerBuilder<U, E>;
}
```

Feature flag: `scalable-topics` on the `magnetar` crate, **default off**.
Compiling without the flag leaves the v0.1.0 surface bit-for-bit
unchanged. The `magnetar-cli` binary picks the feature up via
`--features magnetar/scalable-topics` only.

`#[doc = "**Experimental** (PIP-460 v0.2.0). StreamConsumer drops on DAG change."]`
banner on every public type in the module.

### 3.2 `magnetar-runtime-moonpool`

| File | Change |
| --- | --- |
| [`crates/magnetar-runtime-moonpool/src/lib.rs`](../../crates/magnetar-runtime-moonpool/src/lib.rs) | `impl ScalableTopicsApi for Client<P>` — same hook shape as tokio. |
| [`crates/magnetar-runtime-moonpool/src/client.rs`](../../crates/magnetar-runtime-moonpool/src/client.rs) | `topic://` URL parser parity. |
| [`crates/magnetar-runtime-moonpool/src/driver.rs`](../../crates/magnetar-runtime-moonpool/src/driver.rs) | Same event-translation as tokio. |
| [`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`](../../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs) | Add `BrokerWorkload::ScalableTopic { initial_dag, scripted_events }` variant. |
| `crates/magnetar-runtime-moonpool/tests/scalable_topic_broker.rs` (NEW) | Scripted controller-broker fake: replies to `ScalableTopicLookup`, opens `DagWatch`, pushes 2 updates (one split, one merge), then closes. |

The moonpool engine inherits `StreamConsumer` from the `magnetar`
façade — generic over `E: Engine`. No GAT growth; new behaviour rides
the `ScalableTopicsApi` extension trait per ADR-0026 §D1.

### 3.3 `magnetar-cli`

`magnetar://topic-info <topic://...>` subcommand prints the current
segment DAG. Gated on the same feature flag. Implementation: open a
`ClientBuilder`, call `lookup_scalable_topic(...)`, format
`Vec<SegmentDescriptor>` as a table. ~80 LOC.

## 4. Four-layer test plan ([ADR-0024](../adr/0024-cross-runtime-test-and-coverage-policy.md))

Every behavioural row below ships **in the same commit**. The 1:1
tokio ↔ moonpool count is enforced by `cargo xtask check-runtime-test-parity`.

### (a) `magnetar-proto` unit

| Test | File | Asserts |
| --- | --- | --- |
| `command_scalable_topic_lookup_roundtrip` | `crates/magnetar-proto/src/lookup.rs` (or `tests/lookup.rs`) | Encode/decode the new request + response, including the segment list. |
| `command_segment_dag_watch_roundtrip` | `crates/magnetar-proto/src/dag_watch.rs` (`#[cfg(test)] mod tests`) | Frame-level encode/decode of `CommandSegmentDagWatch` + response. |
| `command_segment_dag_update_roundtrip` | same | Add/remove/split/merge variants encode and decode cleanly. |
| `message_id_with_segment_roundtrip` | `crates/magnetar-proto/src/types.rs` | `MessageId { segment_id: Some(…), … }` encodes and decodes; v4-shape (`None`) byte-identical to current `MessageId` encoding. |
| `dag_watch_session_monotonic_update_seq` | `crates/magnetar-proto/src/dag_watch.rs` | Non-monotonic `update_seq` → `DagError::NonMonotonic`. |
| `dag_watch_session_apply_split` | same | Initial DAG + split event → DAG has parent removed, two children present. |
| `dag_watch_session_apply_merge` | same | Inverse: two parents removed, one child present. |
| `conn_emits_scalable_topic_lookup_resolved` | `crates/magnetar-proto/src/conn.rs` | Feed `CommandScalableTopicLookupResponse` bytes; assert `Event::ScalableTopicLookupResolved`. |
| `conn_emits_dag_changed_during_consume` | same | Scripted: open `DagWatch`, open consumer, feed a `SegmentDagUpdate` with non-empty `split_events` → `Event::DagChangedDuringConsume`. |

### (b) `magnetar-runtime-tokio` integration

| Test | File |
| --- | --- |
| `scalable_topic_url_parsing` | `crates/magnetar-runtime-tokio/tests/scalable_topic.rs` (NEW) |
| `stream_consumer_happy_path_against_fake_broker` | same |
| `stream_consumer_drops_on_dag_change` | same |
| `scalable_topics_feature_off_does_not_export` | `crates/magnetar-runtime-tokio/tests/scalable_topic_feature_gate.rs` (NEW, `compile_error!` proof) |

`magnetar-fakes` ([`crates/magnetar-fakes`](../../crates/magnetar-fakes/))
grows a `ScriptedScalableBroker` builder that replies to
`CommandScalableTopicLookup` with a fixed DAG and pushes scripted
`SegmentDagUpdate`s on the watch session.

### (c) `magnetar-runtime-moonpool` integration

**Same four test names, 1:1**, under
`crates/magnetar-runtime-moonpool/tests/scalable_topic.rs` (NEW), each
running inside a `SimulationBuilder` with the `ScalableTopicBroker`
variant of `BrokerWorkload`. The two functional tests
(`happy_path` + `drops_on_dag_change`) script the **same** DAG +
update sequence as their tokio counterparts.

`cargo xtask check-sim-coverage` requires **100% diff coverage** on
the new `magnetar-proto::dag_watch` module + the new conn entries.

### (d) `magnetar-differential`

| Test | File |
| --- | --- |
| `dag_change_event_stream_parity` | `crates/magnetar-differential/tests/scalable_topic_equivalence.rs` (NEW) |
| `scalable_topic_lookup_event_stream_parity` | same |

Both record a `Vec<Event>` from the tokio and moonpool engines against
an identical scripted broker transcript; assert pairwise equality
ignoring `Instant`-typed timestamp fields (per the existing
differential harness convention).

Golden trace under
`crates/magnetar-differential/tests/golden/scalable_topic_drop_on_split.json`
(human-reviewable, regenerated by `MAGNETAR_REGENERATE_GOLDEN=1`).

### Exemptions: none

PIP-460 introduces wire changes, sans-io state machines, and a new
runtime surface. All four layers are binding. The 1:1 runtime-test-count
rule applies.

## 5. E2E plan

**Blocked on upstream Pulsar 5.0 RC.**

| Item | Plan |
| --- | --- |
| Image | `apachepulsar/pulsar:5.0.0-rc-*` with `scalableTopicsEnabled=true` (broker config TBD by upstream). |
| Test file | `crates/magnetar/tests/e2e_scalable_topic.rs` (NEW). |
| Gating | `#[ignore = "e2e: requires Pulsar 5.0 with PIP-460"]` + cargo feature `e2e,scalable-topics`. |
| Coverage | (1) lookup-then-consume happy path; (2) `topic-info` CLI round-trip; (3) drop-on-DAG-change observed against a broker-driven split. |

The 4-layer set above is the **binding acceptance gate**; e2e is
best-effort and explicitly **does not block the v0.2.0 release-cut**
on this surface. Once Pulsar 5.0 GA ships, e2e becomes blocking — at
which point we cut a follow-up proposal flipping the gate.

`docker-compose.scalable-topics.yml` under
`crates/magnetar/tests/fixtures/` (NEW) — single-broker, but pre-config
includes the scalable-topic namespace; reuses the existing test cluster
helpers (`crates/magnetar/tests/helpers/`).

## 6. LOC + risk

| Component | LOC est. |
| --- | --- |
| `magnetar-proto` (commands + DagWatchSession + MessageId ext) | ~400 |
| `magnetar::scalable` module (`StreamConsumer`, builder, lookup glue) | ~500 |
| `ScalableTopicsApi` extension-trait impls per engine | ~250 |
| Moonpool `ScalableTopicBroker` fake + sim_chaos variant | ~250 |
| Tests (4-layer) | ~400 |
| `magnetar-cli` `topic-info` subcommand | ~80 |
| e2e + docker-compose helper | ~200 |
| **Total** | **~2080** |

### Risks

1. **Proto field numbers churn.** Upstream may renumber commands
   between PIP draft and Pulsar 5.0 RC. Mitigation: vendor on the RC
   tag, not on master; the proposal's §1 field numbers are explicitly
   provisional.
2. **Controller-broker disconnect semantics under-specified upstream.**
   Mitigation: v0.2.0 surfaces `DagWatchClosed { reason }` and lets the
   caller decide. No silent retry. Documented in
   `docs/scalable-topics.md` (NEW) alongside the experimental banner.
3. **Test-count parity strain.** The 1:1 rule means moonpool needs a
   working scripted controller-broker before any tokio test lands.
   Mitigation: land the `ScalableTopicBroker` fake **before** the
   tokio surface, in a prerequisite PR.
4. **Feature-flag combinatorics.** `scalable-topics` ×
   `crypto-{aws-lc-rs,ring,openssl,fips}` per
   [ADR-0035](../adr/0035-pluggable-crypto-provider.md). `cargo xtask
   check-crypto-matrix` already covers the crypto axis; the new
   feature multiplies the build matrix by 2 (on/off).

### Rollback

PIP-460 is feature-flagged off by default. If a critical bug surfaces
post-release, the rollback is `--no-default-features` or simply not
enabling `scalable-topics`. No revert PR needed; v0.1.0-compatible
callers are unaffected.

## 7. Dependencies + sequencing

1. **Prereq (separate PR)**: vendor-proto bump to Pulsar 5.0 RC
   (single commit, [ADR-0026 §D4](../adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)).
2. **Prereq (separate PR)**: `magnetar-fakes` +
   `magnetar-runtime-moonpool::tests::scalable_topic_broker` scripted
   broker. No magnetar-proto surface change yet.
3. **Wave 1**: `magnetar-proto` additions (types, `DagWatchSession`,
   `Conn` entries, `Event` variants) + (a) test layer.
4. **Wave 2**: `magnetar::scalable` surface + (b) test layer (tokio).
5. **Wave 3**: moonpool surface + (c) test layer. Test-count parity
   enforced.
6. **Wave 4**: differential + (d) test layer.
7. **Wave 5**: `magnetar-cli` `topic-info` subcommand + docs +
   parity-matrix flip in `README.md` to `🟡 experimental`.
8. **Wave 6** (post-RC): e2e tests, fixture, docker-compose helper.

## 8. Documentation deliverables (same wave)

- `docs/scalable-topics.md` (NEW) — surface overview, experimental
  banner, drop-on-change semantics, examples.
- `docs/parity-status.md` — add PIP-460 row, `🟡 experimental`.
- `README.md` — Java-client parity matrix row update.
- `specs/README.md` ADR index — flip ADR-0031 status to `Accepted`
  the moment Florentin signs the proposal off.
- `docs/follow-ups.md` — record v0.3.0+ items: QueueConsumer,
  CheckpointConsumer, controller election, transparent failover.

## 9. References

- [ADR-0031](../adr/0031-pip-460-scalable-subscription-scope.md) — scope.
- [ADR-0024](../adr/0024-cross-runtime-test-and-coverage-policy.md) — test plan binding.
- [ADR-0026 §D1](../adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) — `Surface<T, E>` + extension traits.
- [ADR-0026 §D4](../adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) — vendor-proto in a dedicated commit.
- [ADR-0011](../adr/0011-clock-injection-sans-io.md) — clock injection.
- Upstream PIP — [pip/pip-460.md](https://github.com/apache/pulsar/blob/master/pip/pip-460.md).
- Companion proposal — [PIP-466](pip-466-v5-client-surface.md) (V5 surface consumes scalable lookup).
