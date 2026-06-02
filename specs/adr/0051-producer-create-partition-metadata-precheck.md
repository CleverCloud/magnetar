# ADR-0051 — Pre-check partition metadata in `ProducerBuilder::create()`

- **Status**: Accepted
- **Date**: 2026-06-02
- **Decider**: Florentin Dubois
- **Tags**: api, producer, partitioned, java-parity, error-quality

## Context

`PulsarClient::producer(topic).create()` opens a single, non-partitioned producer.
If the topic happens to carry partition metadata in the broker (`topic_create_partitioned` was run, or the namespace policy auto-creates partitioned topics), the bare `open_producer` round-trip surfaces as broker `ServerError::NotAllowedError(22)` with the message `"Found partitioned metadata for non-partitioned topic"` (per `pulsar-broker/src/main/java/.../PersistentTopicsBase.java`).

Rémi Collignon-Ducret reported the symptom on Slack (`otelgw::controller::logs`, 2026-06-02): the gateway maps a per-organisation topic name into `client.producer(...).create()`, has no way to know in advance whether the broker has partitioned the topic, and the failure surface is the raw broker string — operators cannot tell what the recovery is.

The Java client side-steps this by always calling `CommandPartitionedTopicMetadata` first inside `PulsarClientImpl#createProducerAsync` and routing to either a single `ProducerImpl` or a `PartitionedProducerImpl` transparently.
Magnetar deliberately splits the entry points (`client.producer(t)` vs `client.partitioned_producer(t)`) to keep the per-builder configuration surfaces explicit, but the split makes the broker error the only signal that the topic shape disagrees with the chosen API.

Three options were on the table:

1. **Pre-check + actionable error.** `client.producer(t).create()` resolves `CommandPartitionedTopicMetadata` first.
   `N == 0` → unchanged single-producer path.
   `N > 0` → return `PulsarError::Other` whose message points the caller at `client.partitioned_producer(t)`.
   Adds one round-trip per `create()` on warm topic names (`<base>-partition-<i>` short-circuits to `N = 0` via the existing F11 fast-path — see [`crates/magnetar/tests/e2e_partition_fast_path.rs`](../../crates/magnetar/tests/e2e_partition_fast_path.rs)).
   No type cascade.

2. **Full Java-parity auto-dispatch.** `client.producer(t).create()` returns a `PartitionedProducer<R>` for both N=0 and N>0 cases, delegating internally to the partitioned path.
   The change cascades through `TypedProducer<S, P: ProducerApi>` (the `inner: P` slot would have to be `PartitionedProducer<R>`, which needs a `ProducerApi for PartitionedProducer` impl), through `v5::Producer::from_v4` (which assumes the v4 inner is `R`, not a wrapper), and through `magnetar-cli` / the moonpool engine seam.
   Substantial refactor — 600–1200 LOC across 6–8 files, plus 4-layer test coverage per ADR-0024.

3. **Reactive translation only.** Catch the broker `NotAllowedError(22)` with the specific body inside `open_producer` and translate to the same actionable error.
   No extra round-trip on the success path, but the first call still pays the broker rejection.

Option 1 won.
It removes Rémi's confusion in one round-trip (the same cost the Java client already pays), keeps the engine-generic builder cascade intact, and leaves the door open for option 2 as a separate effort once the `ProducerApi` / `PartitionedProducer` interplay is designed end-to-end.

## Decision

Add a `partitioned_topic_metadata` pre-check inside `magnetar::builders::ProducerBuilder::create()`:

- Resolve `CommandPartitionedTopicMetadata` for `self.req.topic` via `crate::BrokerMetadataApi::partitioned_topic_metadata`.
- If `partitions > 0`, return `PulsarError::Other` whose message includes the topic, the partition count, and the literal recovery hint `client.partitioned_producer("…").create()`.
- If `partitions == 0`, proceed with the existing `CreateProducerApi::open_producer` path.

The `where` clause on `ProducerBuilder::create()` gains a `crate::BrokerMetadataApi` bound on the engine state — every downstream wrapper that calls `builder.create().await` (i.e. `TypedProducerBuilder::create()` and `v5::ProducerBuilder::create()`) inherits the same bound.

The bound matches the one `PartitionedProducerBuilder::create()` already requires, so engines that ship one already ship the other.

The shared `open_partitioned_with_metadata` helper extracted from `PartitionedProducerBuilder::create()` is the seam we'll re-use when option 2 lands.

## Consequences

**Easier**

- Operators see `"topic 'X' is partitioned (broker reports N partitions); call client.partitioned_producer(\"X\").create() instead"` instead of `NotAllowedError(22)`.
- The Java-parity round-trip cost is documented and bounded — one `CommandPartitionedTopicMetadata` per producer open.
- `e2e_partition_fast_path` already protects the per-partition suffix short-circuit, so spawning per-partition producers does NOT cascade into N+1 metadata lookups.

**Harder**

- Every producer open path now goes through partition metadata.
  On a topic where the broker's metadata cache is cold the extra round-trip is observable.
  Acceptable because the Java client pays the same cost.
- Engines that want to dispatch through `ProducerBuilder::create()` must implement `BrokerMetadataApi` — already true for the in-tree engines, future engines must mirror that.

**Cost**

- One `CommandPartitionedTopicMetadata` round-trip per `producer().create()` call on non-partitioned topics.
  Fast-path short-circuit applies when the caller's topic already encodes `-partition-<N>`.

**Incompatible with**

- The previous "open the bare topic and trust the broker to accept it" path — that path was always wrong on a partitioned topic and is now refused with a clean error instead of a confusing one.

**Queued follow-up: option 2 (auto-dispatch) — future ADR.**
The auto-dispatch refactor is queued as a separate effort.
Its skeleton — `open_partitioned_with_metadata` in [`crates/magnetar/src/partitioned_producer.rs`](../../crates/magnetar/src/partitioned_producer.rs) — already exists, shared between `PartitionedProducerBuilder::create()` and any future auto-dispatch implementation.
The blocker is the `TypedProducer<S, P>` and `v5::Producer::from_v4` cascade: every `inner: P` slot has to gain a unified producer interface (either a `ProducerApi for PartitionedProducer` impl that round-robins through the children, or a wrapper enum that branches at every call site).
That design needs ADR-0024 cross-runtime test coverage on the new method dispatching, which is out of scope here.

## References

- [`crates/magnetar/src/builders.rs`](../../crates/magnetar/src/builders.rs) — `ProducerBuilder::create()` carrying the new pre-check.
- [`crates/magnetar/src/partitioned_producer.rs`](../../crates/magnetar/src/partitioned_producer.rs) — extracted `open_partitioned_with_metadata` helper, ready for option 2.
- [`crates/magnetar/tests/e2e_partitioned_deep.rs`](../../crates/magnetar/tests/e2e_partitioned_deep.rs) — `producer_create_on_partitioned_topic_returns_actionable_error` regression test against a real broker.
- [`crates/magnetar/tests/e2e_partition_fast_path.rs`](../../crates/magnetar/tests/e2e_partition_fast_path.rs) — F11 short-circuit covers `<base>-partition-<i>` to `N=0` without a round-trip.
- ADR-0024 — Cross-runtime test + coverage policy; the pre-check pre-empts a wire-level broker error, not a state-machine invariant, so the four-layer rule does not apply.
- ADR-0021 — No silent `#[ignore]`; the new e2e test ships without an ignore.
- ADR-0046 — E2e tests as casual (no feature flag, no ignore); same.
