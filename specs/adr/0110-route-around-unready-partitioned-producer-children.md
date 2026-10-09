# ADR-0110 — Route keyless publishes around unready partitioned producer children

- **Status**: Accepted
- **Date**: 2026-09-30
- **Decider**: Florentin Dubois
- **Tags**: producer, routing, readiness, issue-463

## Context

[Issue #463](https://github.com/CleverCloud/magnetar/issues/463) records a partial producer outage that stalled a bounded pipeline. `PartitionedProducer::pick_partition` advanced its round-robin cursor without examining the selected child's send-drain gate. When a child was detached while its connection remained live, the router kept feeding it its full share. Those sends waited until `send_timeout`; a caller with a bounded in-flight window eventually filled every slot with these waits and stopped feeding healthy partitions. ADR-0106 re-attaches a broker-closed child but cannot eliminate the window while that attachment, its retry leg, or an owner broker is unavailable.

`Producer::is_connected()` describes the shared transport, not the child's broker attachment. The authoritative `ProducerState::broker_ready` gate already prevents an unsafe `CommandSend` from reaching a broker before `CommandProducerSuccess`, but it sat behind the per-slot mutex and had no façade accessor. Pulsar's Java router does not skip disconnected children; preserving that parity here would retain the observed throughput collapse. The requirement to keep healthy partitions progressing takes precedence for keyless round-robin sends. Keyed sends must keep their partition for ordering and affinity.

## Decision

`ProducerSlot` mirrors effective routing readiness in an `AtomicBool`. Its initial value derives from `state.broker_ready && !state.closed`; a newly opened producer starts unready because its broker attachment has not been acknowledged. Every change to the broker attachment gate updates the mirror under the same slot guard: `CommandProducerSuccess` opens it, while broker close, retryable open rejection and terminal open failure close it. Connection handshake transitions clear all mirrors when the transport is not connected and restore only acknowledged, open attachments on reconnect; explicit producer close and reset clear the mirror as well. The in-tree Tokio and Moonpool `ProducerApi::is_ready()` implementations read this atomic hint without taking either the connection or slot mutex. `TypedProducer::is_ready()` forwards the chosen implementation's observation. External `ProducerApi` implementations retain a compatibility default that returns `is_connected()` until they override it with their own attachment signal. The protocol's locked `broker_ready` check remains authoritative at send drain, so a close racing a routing decision may stage one send but cannot write it before re-attachment.

For `RoundRobin` and the keyless arm of `KeyHashOrRoundRobin`, the router takes one cursor ticket, scans at most one full rotation, and chooses the first ready child. In the uncontended case it advances past skipped children so available children retain a rotation. If every child is unready, it uses the original ticket's partition: a send still receives the configured `send_timeout` or terminal error instead of inventing a new outcome. A non-empty key, `SinglePartition` and a custom router retain their exact partition choices even when that child is unready. The public `not_ready_partitions()` method reads children sequentially and lists indices observed unready during that scan, giving applications a direct alert surface without changing the aggregate stats schema.

## Consequences

- Keyless traffic shifts onto ready partitions during a partial outage; per-partition distribution changes while the outage lasts. A recovered child rejoins on the next eligible cursor scan. Keyed messages keep their affinity and may still wait for their chosen child.
- Each readiness read observes one child at a point in time; the scan is not an atomic snapshot across children. Concurrent broker changes can race a routing choice, but the send-drain gate still protects the connection. No timer, channel, I/O, or runtime-specific retry loop is added.
- When no child is ready, a caller still observes its existing send timeout or terminal error. `not_ready_partitions()` can distinguish this condition from a partially degraded producer; `is_connected()` keeps its transport meaning.
- A child terminalized after an exhausted re-attach budget remains unready. This change routes around it; it does not re-home the handle to another broker or change ADR-0106's retry policy.

## Verification

The façade regression holds one child unready across a bounded in-flight window, observes the old router stall before the change, and requires repeated sends to reach the healthy child after it. It also covers keyed affinity, no-ready fallback, return to availability, and the observable index snapshot. Protocol, Tokio, Moonpool and differential tests check the close/reattach gate transitions; the real-broker partition-unload test covers publication during an actual broker detach. These layers follow [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md).
