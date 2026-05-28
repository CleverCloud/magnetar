# Scalable topics (PIP-460) — experimental

> **⚠️ EXPERIMENTAL — scaffold only.** This surface ships behind the
> default-off `scalable-topics` feature. Upstream
> [PIP-460](https://github.com/apache/pulsar/blob/master/pip/pip-460.md) is
> **`Draft`** and **no released Apache Pulsar broker speaks the scalable-topic
> wire protocol today** (it targets Pulsar 5.0 LTS, ~Oct 2026, with a phased
> rollout). magnetar v0.2.0 ships the **client-side scaffold** — the wire
> commands, the segment-DAG state machine, the `StreamConsumer` surface, and
> the four-layer in-process test coverage — so the surface is ready the day a
> broker ships it. End-to-end against a live broker is **deferred until
> upstream cuts a Pulsar 5.0 RC**. See
> [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md).

## What PIP-460 is

PIP-460 introduces a third topic shape alongside non-partitioned and
partitioned topics: a **scalable topic**, addressed by a new `topic://...`
URL scheme. A scalable topic is backed by a **segment DAG** — a set of
hash-key-ranged segments that the broker can **split** (one segment fans out
into children) and **merge** (children fold back) at runtime, each served by
its own segment-leader broker and coordinated by an elected **controller
broker**. Clients open a **DAG-watch session** against the controller broker
to observe the live segment layout.

## What magnetar v0.2.0 ships

A bounded, experimental **StreamConsumer** surface with **drop-on-DAG-change**
semantics:

- **`topic://...` URL scheme** recognition (`is_scalable_topic_url`), routed to
  the scalable lookup path. The `persistent://` / `non-persistent://` v4 paths
  are untouched.
- **Three new wire commands** (hand-encoded behind the feature, see below):
  `CommandScalableTopicLookup` + response, `CommandSegmentDagWatch` +
  response, `CommandSegmentDagUpdate`, plus `CommandCloseSegmentDagWatch`.
- **`SegmentDescriptor` / `SegmentId` / `KeyRange` / `SegmentState`** types and
  an additive, default-`None` `MessageId::segment_id` field (the v4 wire layout
  stays byte-identical when `None` — legacy producers / consumers round-trip
  bit-for-bit).
- **`DagWatchSession`** — a sans-io state machine that tracks the current DAG,
  enforces a **monotonic `update_seq`**, and applies add / remove / split /
  merge deltas.
- **`scalable::StreamConsumer<T, E>`** on the façade, generic over the engine
  via the `ScalableTopicsApi` extension trait (`where E::ClientState:
  ScalableTopicsApi`), available on **both** the tokio and moonpool engines.
- A **`magnetar topic-info topic://...`** CLI subcommand that prints the
  current segment DAG.

## Drop-on-DAG-change semantics

v0.2.0 is **observation + drop-on-change**, not transparent failover. When the
controller broker pushes a segment **split**, **merge**, or **removal** while a
`StreamConsumer` is active:

1. The proto `DagWatchSession` applies the delta and emits
   `SegmentDagUpdated { delta }`.
2. Because the delta is *consume-affecting*, the connection also emits
   `DagChangedDuringConsume { reason }`.
3. The runtime drains those into the per-client scalable-event buffer; the
   façade `StreamConsumer::next_event` surfaces
   `ConsumerEvent::DagChanged { reason }` and flips `is_dropped()`.
4. The caller **re-resolves** (`scalable_stream_consumer(...)` again) and
   re-subscribes to continue.

A pure-**add** update (a fresh segment with no split / merge / removal) is
*benign* — it refreshes the DAG snapshot and surfaces
`ConsumerEvent::DagUpdated` without dropping.

If the controller-broker connection closes, the surface emits
`ConsumerEvent::Closed { reason }` and lets the caller decide — there is **no
automatic re-lookup** (controller-election awareness is v0.3.0+).

## Out of scope (v0.3.0+)

`QueueConsumer`, `CheckpointConsumer`, controller-election awareness,
transparent segment failover during consume, in-place key-range repartition,
and segment-aware sticky-key dispatch (Key_Shared across the full DAG) are all
explicit follow-ups. v0.2.0's `KeyRange` is **observation-only**.

## The hand-encoded wire commands

Because no broker ships PIP-460 and the upstream field numbers are still
provisional, magnetar does **not** vendor the commands into the generated
`crates/magnetar-proto/src/pb/pulsar.proto.rs`. Instead they live in a
hand-maintained, feature-gated module
(`crates/magnetar-proto/src/pb/scalable_topics.rs`) as `#[derive(prost::Message)]`
structs that ride the standard Pulsar command frame via a hand-built
`ScalableBaseCommand` envelope (sharing the `type` field-1 tag, so a v4 peer
skips the additive 80-85 fields). The **authoritative** proto bump lands when
upstream tags a Pulsar 5.0 RC — at that point a dedicated
`cargo run -p xtask -- vendor-proto --rev <sha>` commit
([ADR-0026 §D4](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md))
replaces the hand-encoded module and reconciles the field numbers.

## Feature flag

`scalable-topics` on the `magnetar` crate, **default off**. Compiling without
it leaves the v0.1.0 surface bit-for-bit unchanged (proved by the
`scalable_topics_feature_off_does_not_export` test on both runtime engines).
The CLI picks it up via `--features magnetar-cli/scalable-topics`.

## Example (against a future Pulsar 5.0 broker)

```rust,no_run
# #[cfg(all(feature = "tokio", feature = "scalable-topics"))]
# async fn run() -> Result<(), Box<dyn std::error::Error>> {
use magnetar::PulsarClient;
use magnetar::scalable::ConsumerEvent;

let client = PulsarClient::builder()
    .service_url("pulsar://localhost:6650")
    .build()
    .await?;

// Resolve + open a DAG-watch-backed StreamConsumer.
let mut consumer = client
    .scalable_stream_consumer::<Vec<u8>>("topic://public/default/scaled")
    .await?;

println!("initial DAG: {} segment(s)", consumer.dag().len());

while let Some(event) = consumer.next_event().await {
    match event {
        ConsumerEvent::DagUpdated { .. } => {
            println!("DAG now has {} segment(s)", consumer.dag().len());
        }
        ConsumerEvent::DagChanged { reason, .. } => {
            // Drop-on-change: re-resolve + re-subscribe.
            eprintln!("DAG changed ({reason:?}); re-resolving");
            break;
        }
        ConsumerEvent::Closed { reason, .. } => {
            eprintln!("watch closed: {reason:?}");
            break;
        }
    }
}
# Ok(()) }
```

## References

- [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md) — scope.
- [Proposal](../specs/proposals/pip-460-scalable-topics.md) — full wire delta + test plan.
- [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md) — the four-layer test plan.
- Upstream [PIP-460](https://github.com/apache/pulsar/blob/master/pip/pip-460.md).
