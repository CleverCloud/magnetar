# ADR-0034 — PIP-33 replicated subscriptions scope

- **Status**: Accepted (2026-05-26), **partially superseded by [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md)** (the `e2e-multi-cluster` Cargo feature is removed; the two-cluster docker-compose fixture + the `e2e_replicated_subscriptions.rs` test are unchanged and now run on every per-PR CI run via the regular `test` job)
- **Date**: 2026-05-26
- **Decider**: Florentin Dubois
- **Tags**: pip-33, replicated-subscriptions, geo-replication, scope

## Context

[ADR-0010](0010-v0-1-full-java-parity.md) listed PIP-33 (replicated subscriptions) in core-parity scope.
PIP-33 enables subscription state synchronisation across geo-replicated Pulsar clusters at sub-second granularity, so a consumer that fails over from cluster A to cluster B resumes near its previous cursor position (with up-to- one-second of duplicate messages by design).
The mechanism is purely **broker-driven**: the broker inserts periodic snapshot markers (`ReplicatedSubscriptionsSnapshotRequest` / `Response` / `Snapshot` / `Update`) inline with the topic data and propagates cursor positions across clusters via geo-replication.

The client side of PIP-33 is small: a single producer/consumer option, `replicateSubscriptionState(boolean)`, that flips a flag in the `CommandSubscribe` payload.
The broker does the heavy lifting.
Despite the small surface, magnetar's parity finishing wave never landed the client-side hook — `ConsumerBuilder` exposes no equivalent of `Consumer.Builder#replicateSubscriptionState` and the `CommandSubscribe` encoder does not set the flag.
The vendored proto **does** have the flag and the markers ([`crates/magnetar-proto/proto/PulsarApi.proto:397-398`](../../crates/magnetar-proto/proto/PulsarApi.proto), [`crates/magnetar-proto/proto/PulsarMarkers.proto:28-33,40-82`](../../crates/magnetar-proto/proto/PulsarMarkers.proto)); they are simply unused on the client driver side.

This ADR locks the PIP-33 surface: a one-line builder option on consumers, marker-aware decode on the receive path (so the markers are not surfaced as application messages), and an explicit **non-goal**: magnetar does not implement broker-side replication.
The amendment lifting PIP-33 out of the original ADR-0010 core-parity scope follows in a separate `docs(adr): amend ADR-0010` commit.

## Decision

- **Wire-protocol delta vs. current vendored PulsarApi.proto: none.** All wire pieces are present:
  - `CommandSubscribe.replicateSubscriptionState` ([`crates/magnetar-proto/proto/PulsarApi.proto:397-401`](../../crates/magnetar-proto/proto/PulsarApi.proto)).
  - `MarkerType::REPLICATED_SUBSCRIPTION_*` enum + `ReplicatedSubscriptionsSnapshotRequest` / `ReplicatedSubscriptionsSnapshotResponse` / `ReplicatedSubscriptionsSnapshot` / `ReplicatedSubscriptionsUpdate` messages ([`crates/magnetar-proto/proto/PulsarMarkers.proto:28-33,40-82`](../../crates/magnetar-proto/proto/PulsarMarkers.proto)).
    No proto bump required.

- **`magnetar-proto` state-machine additions.**
  - `SubscribeOptions { replicate_subscription_state: bool, …existing fields… }` — extend the existing subscribe-options carrier with one bool field.
    Default `false` to preserve prior behaviour.
  - `CommandSubscribe` encoder emits the new field when set.
  - Consumer receive path: parse the optional `MessageMetadata.marker_type` (already vendored) and, for `REPLICATED_SUBSCRIPTION_*` marker types, **drop the marker from the user-visible `EventStream`** and emit a low-cardinality `Event::ReplicatedSubscriptionsMarkerObserved { marker_type, snapshot_id }` instead — useful for observability metrics, not for application code.
    The markers are server-driven; the client never originates them.
    **The client never emits snapshot markers.** This is the explicit non-goal.
  - `MessageId` for the marker carries the same fields as a regular message but is filtered before the `Event::MessageReceived` path.

- **`magnetar-runtime-tokio` surface.**
  - `ConsumerBuilder::replicate_subscription_state(self, enabled: bool) -> Self` — new builder method, mirroring Java's `Consumer.Builder#replicateSubscriptionState`.
    Wires through to `SubscribeOptions.replicate_subscription_state`.
  - No new feature flag.
    PIP-33 has been in Pulsar since 2.4 and is available on the baseline Pulsar 4.0+ broker ([ADR-0009](0009-pulsar-4-minimum.md)).
  - No new method on `PulsarClient<E>`; the surface is builder-only.

- **`magnetar-runtime-moonpool` port.** Sans-io extension only.
  The `SubscribeOptions` change and the marker-filter on the receive path are both in `magnetar-proto`; moonpool inherits them through the existing `Consumer` driver.
  The `BrokerWorkload` in [`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`](../../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs) grows a scripted "insert a `REPLICATED_SUBSCRIPTION_SNAPSHOT` marker every N messages" mode so the marker-filter logic is exercised under the simulator.

- **Explicit non-goals.** Two:
  1. Magnetar does **not** originate `REPLICATED_SUBSCRIPTION_*` markers.
     Snapshot generation is broker-side.
  2. Magnetar does **not** implement the broker-side geo-replication state machine.
     Setting `replicate_subscription_state(true)` only flips the flag; correctness depends on the broker cluster running with geo-replication enabled and the namespace configured with `replication_clusters` + `replicated_subscription_status_enabled`.
     These are documented in [`docs/pip-features.md#replicated-subscriptions-pip-33`](../../docs/pip-features.md#replicated-subscriptions-pip-33) so callers aren't surprised when the flag has no observable effect on a single-cluster broker.

## Consequences

- **Test layers per ADR-0024 (4-layer):** (a) `magnetar-proto` unit: `CommandSubscribe` encode with `replicate_subscription_state=true`/`false`; marker round-trip decode for each `MarkerType::REPLICATED_SUBSCRIPTION_*`; marker-filter test (markers do not surface on `EventStream` but `Event::ReplicatedSubscriptionsMarkerObserved` does).
  (b) `magnetar-runtime-tokio` integration: builder option flows through to wire bytes; `magnetar-fakes` broker stub scripts a marker injection and asserts the consumer skips the marker.
  (c) `magnetar-runtime-moonpool` integration: identical scripted-marker test under `SimulationBuilder`.
  (d) `magnetar-differential`: equivalence test asserting that given an identical broker transcript including `REPLICATED_SUBSCRIPTION_*` markers, both engines surface the same `EventStream` (markers filtered, observation events emitted in the same order).

- **E2E fixture needs.** A multi-cluster Pulsar fixture is the only honest e2e for PIP-33 (otherwise the flag is a no-op).
  Stand up **two** `apachepulsar/pulsar:4.0.4` clusters configured as geo-replication peers (`bin/pulsar-admin clusters create cluster-a/b`, namespace configured with both as `replication_clusters` and `replicated-subscription-status-enabled=true`).
  The fixture produces in cluster A, consumes from cluster A with `replicate_subscription_state(true)`, kills the cluster-A consumer, switches the client to cluster B, and asserts the resumed cursor position is within the documented one-second tolerance.
  Gated by `#[ignore = "e2e: requires two-cluster Docker fixture"]`.
  This is the **most expensive** e2e fixture in the suite — call out in `docs/testing.md`.

- **LOC estimate.** ~400–600 LOC total. Breakdown: ~150 LOC `magnetar-proto` (SubscribeOptions field, marker filter, observation event); ~50 LOC `ConsumerBuilder` method; ~200 LOC tests (4-layer); ~150 LOC e2e fixture + two-cluster docker-compose helper.

- **Security implications.** None new.
  PIP-33 inherits topic and subscription ACLs from the source cluster; cursor position is not a confidential value.
  The cross-cluster trust boundary is broker-side and out of scope for the client.

## Status

Accepted — landed in feat/pip-33-replicated-subscriptions (see [`docs/pip-features.md#replicated-subscriptions-pip-33`](../../docs/pip-features.md#replicated-subscriptions-pip-33)).

## Implementation footprint

The detailed implementation map originally lived under `specs/proposals/pip-33-replicated-subscriptions.md`; it was folded back into this ADR once the work landed.
Authoritative landing artefacts:

| Concern                                                                                     | File                                                                                                                                           |
| ------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Marker decoder + types (`ReplicatedSubscriptionMarker{Kind,Details}`, `ClusterMessageId`)   | [`crates/magnetar-proto/src/markers.rs`](../../crates/magnetar-proto/src/markers.rs)                                                           |
| `SubscribeOptions.replicate_subscription_state` + `CommandSubscribe` encoder                | [`crates/magnetar-proto/src/consumer.rs`](../../crates/magnetar-proto/src/consumer.rs)                                                         |
| Receive-path marker filter + `Event::ReplicatedSubscriptionMarkerObserved`                  | [`crates/magnetar-proto/src/consumer.rs`](../../crates/magnetar-proto/src/consumer.rs), [`event.rs`](../../crates/magnetar-proto/src/event.rs) |
| `PulsarClient::next_replicated_subscription_marker` / `poll_replicated_subscription_marker` | [`crates/magnetar/src/client.rs`](../../crates/magnetar/src/client.rs)                                                                         |
| `ConsumerBuilder::replicate_subscription_state(bool)`                                       | [`crates/magnetar/src/client.rs`](../../crates/magnetar/src/client.rs) (`ConsumerBuilder::replicate_subscription_state`)                       |
| Moonpool scripted broker `InjectsReplicatedMarkers` workload                                | [`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`](../../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs)                             |
| Differential golden trace                                                                   | `crates/magnetar-differential/tests/golden/replicated_subscription_filter.json`                                                                |
| User docs                                                                                   | [`docs/pip-features.md#replicated-subscriptions-pip-33`](../../docs/pip-features.md#replicated-subscriptions-pip-33)                           |
| Two-cluster e2e                                                                             | `crates/magnetar/tests/e2e_replicated_subscriptions.rs` + `.github/workflows/e2e-replicated-subs.yml` (weekly)                                 |

Total landed footprint ≈ 1K LOC including tests.
The two-cluster e2e fixture is opt-in via a separate `e2e-multi-cluster` sub-feature; the weekly workflow files an automatic GitHub issue on failure.

## References

- [ADR-0009](0009-pulsar-4-minimum.md) — Pulsar 4.0+ minimum; PIP-33 has been available since 2.4 so 4.x is safe.
- [ADR-0010](0010-v0-1-full-java-parity.md) — parity scope; this ADR is the basis for lifting PIP-33 out of the original core-parity scope.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — four-layer test plan binding.
- PIP-33 (Replicated Subscriptions) —
  <https://github.com/apache/pulsar/wiki/PIP-33%3A-Replicated-subscriptions>
- Apache Pulsar Java — `org.apache.pulsar.client.impl.ConsumerBuilderImpl#replicateSubscriptionState`.
- `crates/magnetar-proto/proto/PulsarApi.proto:397-398` — `CommandSubscribe.replicateSubscriptionState`.
- `crates/magnetar-proto/proto/PulsarMarkers.proto:28-33,40-82` — marker wire format.
