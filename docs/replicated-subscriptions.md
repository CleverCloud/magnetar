# Replicated subscriptions (PIP-33)

**Scope**: [ADR-0034](../specs/adr/0034-pip-33-replicated-subscriptions-scope.md) · **Upstream**: [PIP-33 (Apache Pulsar wiki)](https://github.com/apache/pulsar/wiki/PIP-33%3A-Replicated-subscriptions)

PIP-33 keeps a subscription's cursor position in sync across geo-replicated Pulsar clusters at sub-second granularity.
A consumer that fails over from cluster A to cluster B resumes near its previous position (up to ~1s of duplicate messages, by design).

The mechanism is entirely **broker-driven**: the broker periodically injects `REPLICATED_SUBSCRIPTION_*` markers into the topic's data stream and propagates cursor positions across clusters via geo-replication.
The client's job is small: (1) flip one builder flag so the broker enables the machinery, and (2) filter the markers off the user-visible message stream so they never surface as application payload.

## Quick reference

```rust
use magnetar::PulsarClient;

let client = PulsarClient::builder()
    .service_url("pulsar://cluster-a:6650")
    .build()
    .await?;

let consumer = client
    .consumer("persistent://public/default/orders")
    .subscription("orders-shared")
    .replicate_subscription_state(true)    // PIP-33 on
    .subscribe()
    .await?;

// Application code is unchanged — markers never appear here.
while let Ok(msg) = consumer.receive().await {
    process(msg).await?;
    consumer.ack(msg.message_id).await?;
}
```

CLI:

```sh
magnetar consumer subscribe \
    persistent://public/default/orders \
    --subscription orders-shared \
    --replicate-subscription-state
```

## Broker-side prerequisites

The flag is a **no-op against a single-cluster broker**. PIP-33 only delivers cross-cluster cursor sync when:

1. **At least two clusters** know about each other:

   ```sh
   bin/pulsar-admin clusters create cluster-a \
       --url http://broker-a:8080 --broker-url pulsar://broker-a:6650
   bin/pulsar-admin clusters create cluster-b \
       --url http://broker-b:8080 --broker-url pulsar://broker-b:6650
   ```

2. The tenant allows both clusters:

   ```sh
   bin/pulsar-admin tenants update public --allowed-clusters cluster-a,cluster-b
   ```

3. The namespace replicates between both:

   ```sh
   bin/pulsar-admin namespaces set-clusters public/default \
       --clusters cluster-a,cluster-b
   ```

4. The namespace has **replicated subscription status** enabled — without this, the broker silently ignores the `replicateSubscriptionState` flag on `CommandSubscribe`:

   ```sh
   bin/pulsar-admin namespaces set-replicated-subscription-status \
       public/default --enable
   ```

5. (Optional) Pin the snapshot interval. The default is 1000ms; lower values tighten the cursor-resume window at the cost of more marker traffic:

   ```ini
   # broker.conf
   replicatedSubscriptionsSnapshotFrequencyMillis = 1000
   ```

A working reference fixture lives at [`crates/magnetar/tests/fixtures/docker-compose.replicated-subs.yml`](../crates/magnetar/tests/fixtures/docker-compose.replicated-subs.yml) with a one-shot setup script at [`configure_replicated_subs.sh`](../crates/magnetar/tests/fixtures/configure_replicated_subs.sh).

## What the client does

1. **Encoder**. With `replicate_subscription_state(true)`, the encoder sets `CommandSubscribe.replicate_subscription_state = true` ([wire field 14](../crates/magnetar-proto/proto/PulsarApi.proto)).
   The default (`None`) leaves the wire bytes unchanged.

2. **Receive-path filter**. When a frame arrives with `MessageMetadata.marker_type ∈ {10, 11, 12, 13}` — `REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST`, `…RESPONSE`, `…SNAPSHOT`, or `…UPDATE` — `magnetar_proto::Connection` decodes it via [`magnetar_proto::markers::decode_replicated_subscription_marker`](../crates/magnetar-proto/src/markers.rs), drops it from the user-visible event stream, and emits a `ConnectionEvent::ReplicatedSubscriptionMarkerObserved` event instead.
   The consumer's flow-control counter is bumped so the broker can still send the next batch of permits-worth of messages.

3. **Observation buffer (optional)**. The driver pushes every observation into a per-client buffer.
   Advanced callers read it via `PulsarClient::poll_replicated_subscription_marker` or `PulsarClient::next_replicated_subscription_marker` — useful for metrics, regression tests, and operational dashboards.
   Most applications can ignore this surface entirely.

## What the client deliberately does **not** do

- **Origination.** Magnetar never emits `REPLICATED_SUBSCRIPTION_*` markers; snapshot generation is broker-side.
  The proto's `Producer` surface has no hook to construct one and won't grow one.
- **Broker-side replication state.** Magnetar does not implement cross-cluster cursor synchronisation.
  `replicate_subscription_state(true)` only flips the wire flag; correctness depends entirely on the broker's geo-replication setup.

These are the two explicit non-goals locked in [ADR-0034](../specs/adr/0034-pip-33-replicated-subscriptions-scope.md).

## Failure modes / caveats

| Symptom                                                | Cause                                                                                                                          | Fix                                                                                         |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- |
| Flag has no observable effect                          | Single-cluster broker, or namespace lacks `replicated_subscription_status=true`                                                | Run all four `pulsar-admin` steps above.                                                    |
| Consumer sees garbage payload that looks like protobuf | Bug — please file. The receive-path filter is supposed to catch every kind 10–13 marker.                                       | Capture the wire bytes via `tcpdump` or a recording broker and open an issue with the dump. |
| Cursor-resume tolerance > 1s on failover               | Broker's `replicatedSubscriptionsSnapshotFrequencyMillis` is higher than expected, or geo-replication lag is significant       | Pin the snapshot frequency lower; check geo-replication health on both clusters.            |
| Markers observed but cursor doesn't sync               | Namespace not flagged with `replicated-subscription-status=true` (separate from `replicateSubscriptionState` on the subscribe) | `bin/pulsar-admin namespaces set-replicated-subscription-status … --enable`.                |

## Testing

- **Unit / proto**: 11 tests in `crates/magnetar-proto/src/markers.rs` + `…/src/conn.rs` cover the decoder, the filter, and the `CommandSubscribe` wire field.
  Run via `cargo test -p magnetar-proto`.
- **Runtime parity (ADR-0024)**: 5 tokio + 5 moonpool integration tests under `crates/magnetar-runtime-{tokio,moonpool}/tests/replicated_subscriptions.rs` with identical names — verified by `cargo run -p xtask -- check-runtime-test-parity`.
- **Differential**: 2 equivalence tests at `crates/magnetar-differential/tests/replicated_subscriptions_equivalence.rs` assert tokio ↔ moonpool produce the same `EventStream` + byte-identical `CommandSubscribe`.
- **End-to-end**: 2 tests at `crates/magnetar/tests/e2e_replicated_subscriptions.rs` against the two-cluster Docker fixture.
  Runs as a regular `cargo test` per ADR-0046; CI runs them **weekly only** in [`.github/workflows/e2e-replicated-subs.yml`](../.github/workflows/e2e-replicated-subs.yml) per the ADR-0036 cost-shifting precedent.

## References

- [ADR-0034](../specs/adr/0034-pip-33-replicated-subscriptions-scope.md) — scope and non-goals.
- [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md) — cross-runtime test + coverage policy.
- [ADR-0036](../specs/adr/0036-moonpool-seed-sweep-daily-random.md) — weekly-workflow precedent for cost-shifting heavy fixtures.
- [`crates/magnetar-proto/src/markers.rs`](../crates/magnetar-proto/src/markers.rs) — decoder + types.
- Apache Pulsar Java — `org.apache.pulsar.client.impl.ConsumerBuilderImpl#replicateSubscriptionState`.
