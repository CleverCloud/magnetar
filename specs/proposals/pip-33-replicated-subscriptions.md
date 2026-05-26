# PIP-33 — Replicated subscriptions (v0.2.0)

- **Status**: Draft
- **ADR**: [ADR-0034](../adr/0034-pip-33-replicated-subscriptions-scope.md)
- **Target**: v0.2.0
- **Date**: 2026-05-26
- **Owner**: Florentin Dubois
- **Upstream**: [PIP-33: Replicated subscriptions](https://github.com/apache/pulsar/wiki/PIP-33%3A-Replicated-subscriptions)
- **Upstream readiness**: 🟢 **LIVE.** Merged upstream in Pulsar 2.4
  (2019) — the oldest of the v0.2.0 PIP wave. Wire bits + markers are
  already vendored (`CommandSubscribe.replicate_subscription_state`,
  `MarkerType::REPLICATED_SUBSCRIPTION_*`). e2e is unblocked but
  **requires a two-cluster fixture** because the flag is a no-op
  against a single-cluster broker (see §5).
- **Broker baseline**: Pulsar 4.0+ — PIP-33 has shipped since 2.4, so
  the v0.1.0 baseline ([ADR-0009](../adr/0009-pulsar-4-minimum.md)) is
  comfortably above the floor.

## TL;DR

PIP-33 enables subscription state synchronisation across
geo-replicated Pulsar clusters at sub-second granularity, so a
consumer failed over from cluster A to cluster B resumes near its
previous cursor position (with up-to-one-second of duplicate messages
by design). The mechanism is **broker-driven** — the broker inserts
periodic `REPLICATED_SUBSCRIPTION_*` snapshot markers inline with the
topic data. The client's job is small: (1) a one-line builder option
that flips `CommandSubscribe.replicateSubscriptionState`; (2) filter
the markers off the user-visible event stream so they don't leak as
application messages. Magnetar does **not** originate snapshots and
does **not** implement broker-side replication. No feature flag.

## 1. Wire-protocol delta vs. vendored `PulsarApi.proto`

**None.** All wire pieces are present in the current vendored proto:

| Field / message | Location |
| --- | --- |
| `CommandSubscribe.replicate_subscription_state` (`optional bool`, field 14) | [`PulsarApi.proto:397-400`](../../crates/magnetar-proto/proto/PulsarApi.proto) |
| `MessageMetadata.marker_type` (`optional int32`, field 20) | [`PulsarApi.proto:149`](../../crates/magnetar-proto/proto/PulsarApi.proto) |
| `MarkerType::REPLICATED_SUBSCRIPTION_*` (4 variants: `SNAPSHOT_REQUEST=10`, `SNAPSHOT_RESPONSE=11`, `SNAPSHOT=12`, `UPDATE=13`) | [`PulsarMarkers.proto:25-37`](../../crates/magnetar-proto/proto/PulsarMarkers.proto) |
| `ReplicatedSubscriptionsSnapshotRequest` / `…Response` / `…Snapshot` / `…Update` | [`PulsarMarkers.proto:42-82`](../../crates/magnetar-proto/proto/PulsarMarkers.proto) |
| `ClusterMessageId` + `MarkersMessageIdData` | [`PulsarMarkers.proto:75-88`](../../crates/magnetar-proto/proto/PulsarMarkers.proto) |

No proto bump. No new field. The work below is **encoder-side use of an
existing field** + **decoder-side filtering on the receive path**.

## 2. `magnetar-proto` state-machine additions

| Concern | File |
| --- | --- |
| Subscribe options carrier | [`crates/magnetar-proto/src/consumer.rs`](../../crates/magnetar-proto/src/consumer.rs) (existing `SubscribeOptions` struct) |
| Receive-path marker filter | same |
| Event variants | [`crates/magnetar-proto/src/event.rs`](../../crates/magnetar-proto/src/event.rs) |
| Marker decoder | `crates/magnetar-proto/src/markers.rs` (NEW — one decoder, one enum) |

### 2.1 Extend `SubscribeOptions`

```rust
pub struct SubscribeOptions {
    /* … existing fields … */

    /// PIP-33: mark this subscription as replicated. The broker will
    /// then periodically synchronise cursor state to peer clusters
    /// configured in the namespace's replication_clusters set.
    ///
    /// **Default: `false`** — preserves v0.1.0 behaviour.
    /// Setting `true` against a non-geo-replicated namespace is a
    /// no-op (the broker silently ignores it) — see
    /// `docs/replicated-subscriptions.md` for caveats.
    pub replicate_subscription_state: bool,
}
```

### 2.2 `CommandSubscribe` encoder

The encoder at the existing `subscribe` entry point sets
`replicate_subscription_state` on the `CommandSubscribe` proto message
when the opts say so. When the field stays `None`, the wire bytes are
byte-identical to v0.1.0 — guards backward compat.

### 2.3 New `markers.rs` module

```rust
// crates/magnetar-proto/src/markers.rs (NEW)

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplicatedSubscriptionMarkerKind {
    SnapshotRequest = 10,
    SnapshotResponse = 11,
    Snapshot = 12,
    Update = 13,
}

#[derive(Clone, Debug)]
pub struct ReplicatedSubscriptionMarker {
    pub kind: ReplicatedSubscriptionMarkerKind,
    pub snapshot_id: String,
    /// For SNAPSHOT / SNAPSHOT_RESPONSE: the local message id (or
    /// per-cluster id list). For SNAPSHOT_REQUEST: source cluster name.
    /// For UPDATE: subscription name + cluster cursors.
    pub details: ReplicatedSubscriptionMarkerDetails,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ReplicatedSubscriptionMarkerDetails {
    SnapshotRequest { source_cluster: Option<String> },
    SnapshotResponse { cluster: Option<ClusterMessageId> },
    Snapshot {
        local_message_id: Option<MarkersMessageIdData>,
        clusters: Vec<ClusterMessageId>,
    },
    Update {
        subscription_name: String,
        clusters: Vec<ClusterMessageId>,
    },
}

pub struct ClusterMessageId {
    pub cluster: String,
    pub message_id: MarkersMessageIdData,
}

pub struct MarkersMessageIdData {
    pub ledger_id: u64,
    pub entry_id: u64,
}

/// Decode a `marker_type` + payload into a typed marker. Returns
/// `None` for `UNKNOWN_MARKER` (0) and for `TXN_*` markers (20+) —
/// txn markers are filtered by the existing txn path.
pub fn decode_replicated_subscription_marker(
    marker_type: i32,
    payload: &[u8],
) -> Result<Option<ReplicatedSubscriptionMarker>, MarkerDecodeError>;
```

### 2.4 New `Event` variant + receive-path filter

```rust
pub enum Event {
    /* … existing variants … */

    /// PIP-33: the broker injected a replicated-subscription marker.
    /// Surfaced for observability only — application code should not
    /// generally care. **Never** carries application payload.
    ReplicatedSubscriptionMarkerObserved {
        consumer_id: ConsumerId,
        marker: ReplicatedSubscriptionMarker,
    },
}
```

Receive-path logic in `Consumer` ([`crates/magnetar-proto/src/consumer.rs`](../../crates/magnetar-proto/src/consumer.rs)):

```text
if metadata.marker_type is None:
    emit Event::MessageReceived (unchanged behaviour)
elif marker_type in REPLICATED_SUBSCRIPTION_*:
    decode marker; emit Event::ReplicatedSubscriptionMarkerObserved
    do NOT emit MessageReceived
elif marker_type in TXN_*:
    existing txn path (unchanged)
else:
    log + ignore (forward-compat for future marker types)
```

The acknowledgement bookkeeping: markers are **not** auto-acked by the
client — the broker manages marker positions independently. The
consumer's local cursor advances past the marker entry as if it had
been delivered and acked, since the broker has already absorbed the
marker into the topic's logical ordering.

### 2.5 Explicit non-goals (`magnetar-proto`)

1. **Client never originates** `REPLICATED_SUBSCRIPTION_*` markers.
   No `Producer::send_marker` for these kinds. The snapshot generation
   is broker-side per PIP-33 design.
2. **No broker-side state machine.** Magnetar does not implement
   cross-cluster cursor synchronisation. Setting
   `replicate_subscription_state(true)` only flips the flag;
   correctness depends on the **broker cluster** running with
   geo-replication enabled and the namespace configured with
   `replication_clusters` + `replicatedSubscriptionStatus=true`.

## 3. Runtime surface ports

### 3.1 `magnetar-runtime-tokio`

| File | Change |
| --- | --- |
| [`crates/magnetar-runtime-tokio/src/consumer.rs`](../../crates/magnetar-runtime-tokio/src/consumer.rs) | New `ConsumerBuilder::replicate_subscription_state(self, enabled: bool) -> Self` mirroring Java's `Consumer.Builder#replicateSubscriptionState`. |

```rust
impl<T, E: Engine> ConsumerBuilder<T, E> {
    /// PIP-33. See `docs/replicated-subscriptions.md` for caveats.
    pub fn replicate_subscription_state(mut self, enabled: bool) -> Self {
        self.opts.replicate_subscription_state = enabled;
        self
    }
}
```

The marker-filter is in `magnetar-proto`; the runtime surfaces the
filtered event stream as-is. No `Consumer::on_marker_observed`
callback in v0.2.0 — the observation event is in `Event` for advanced
callers to inspect via the lower-level event API.

#### Observability hook (optional, same wave)

A new `consumer.subscription.replicated_markers_observed_total{kind=…}`
counter on the existing `magnetar_metrics` registry tracks the marker
filter as a side observation. Off the critical path; off by default
behind the `metrics` feature. ~20 LOC.

#### Feature flag

**None.** PIP-33 ships unconditionally.

### 3.2 `magnetar-runtime-moonpool`

Pure sans-io extension. The `SubscribeOptions` change and the
marker-filter both live in `magnetar-proto`; moonpool inherits them
through the existing `Consumer` driver. The work on the moonpool side
is **testing infrastructure**:

| File | Change |
| --- | --- |
| [`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`](../../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs) | `BrokerWorkload` gains a `InjectsReplicatedMarkers { every_n_messages: usize, kinds: Vec<MarkerKind> }` mode. Scripts an `Update` marker every N regular messages plus an initial `Snapshot`. |
| [`crates/magnetar-runtime-moonpool/src/consumer.rs`](../../crates/magnetar-runtime-moonpool/src/consumer.rs) | 1:1 mirror of the tokio `ConsumerBuilder::replicate_subscription_state` method. |

### 3.3 `magnetar-cli`

`magnetar consumer subscribe ... --replicate-subscription-state` flag
on the existing `magnetar consumer subscribe` command. ~15 LOC.

## 4. Four-layer test plan ([ADR-0024](../adr/0024-cross-runtime-test-and-coverage-policy.md))

All four layers required.

### (a) `magnetar-proto` unit

| Test | File |
| --- | --- |
| `command_subscribe_with_replicate_state_true` | `crates/magnetar-proto/src/consumer.rs` (`#[cfg(test)] mod tests`) — wire byte capture: field 14 present and set. |
| `command_subscribe_with_replicate_state_false_byte_identical_to_v01` | same — guards backward compat. |
| `marker_decode_snapshot_request_roundtrip` | `crates/magnetar-proto/src/markers.rs` |
| `marker_decode_snapshot_response_roundtrip` | same |
| `marker_decode_snapshot_roundtrip` | same |
| `marker_decode_update_roundtrip` | same |
| `marker_decode_txn_marker_returns_none` | same — txn markers are not RS markers. |
| `consumer_filters_replicated_marker_from_event_stream` | `crates/magnetar-proto/src/consumer.rs` — markers do not appear as `MessageReceived`. |
| `consumer_emits_marker_observation_event` | same — `ReplicatedSubscriptionMarkerObserved` fires. |
| `consumer_passes_through_non_marker_messages` | same — regression guard. |
| `consumer_passes_through_txn_markers` | same — txn path unchanged. |

### (b) `magnetar-runtime-tokio` integration

`crates/magnetar/tests/replicated_subscriptions.rs` (NEW):

| Test | Asserts |
| --- | --- |
| `builder_replicate_subscription_state_true_emits_field` | `magnetar-fakes` byte capture: `CommandSubscribe.replicate_subscription_state = true`. |
| `builder_replicate_subscription_state_default_false` | Default behaviour: field absent / `false`. |
| `consumer_skips_replicated_marker_against_scripted_broker` | Broker scripts: 5 messages → 1 `Update` marker → 5 messages → 1 `Snapshot` marker. Consumer receives **10** messages (markers filtered). |
| `consumer_emits_marker_observation_in_order` | The two markers' `ReplicatedSubscriptionMarkerObserved` events appear in the expected positions in the lower-level event stream. |
| `metrics_replicated_marker_counter_increments` | Optional `metrics` feature: counter advances by 2 in the test above. |

### (c) `magnetar-runtime-moonpool` integration

**Same five test names, 1:1**, under
`crates/magnetar/tests/replicated_subscriptions_moonpool.rs` (NEW).
Each runs inside a `SimulationBuilder` with
`BrokerWorkload::InjectsReplicatedMarkers { every_n_messages: 5, kinds:
[Snapshot, Update] }`. Coverage: 100% on the diff
(`cargo xtask check-sim-coverage`).

### (d) `magnetar-differential`

`crates/magnetar-differential/tests/replicated_subscriptions_equivalence.rs` (NEW):

| Test | Asserts |
| --- | --- |
| `marker_filter_event_stream_parity` | Given an identical broker transcript with `REPLICATED_SUBSCRIPTION_*` markers, both engines surface the **same** `EventStream` (markers filtered identically, observation events in the same order). |
| `subscribe_options_wire_parity` | `CommandSubscribe` bytes are byte-identical across engines for the same builder options. |

Golden trace under
`crates/magnetar-differential/tests/golden/replicated_subscription_filter.json`
(human-reviewable, regenerated via `MAGNETAR_REGENERATE_GOLDEN=1`).

### Exemptions: none

All four layers are binding.

## 5. E2E plan — two-cluster fixture (most expensive in v0.2.0)

PIP-33 is **only meaningfully testable against a multi-cluster Pulsar
fixture**. A single-cluster broker silently ignores
`replicate_subscription_state(true)`, so a single-broker e2e would
pass vacuously.

### 5.1 Fixture

| File | Purpose |
| --- | --- |
| `crates/magnetar/tests/fixtures/docker-compose.replicated-subs.yml` (NEW) | Two `apachepulsar/pulsar:4.0.4` containers, configured as peers (cluster-a, cluster-b), plus a shared ZooKeeper / Configuration Store. |
| `crates/magnetar/tests/fixtures/configure_replicated_subs.sh` (NEW) | One-shot setup script: `bin/pulsar-admin clusters create cluster-a`, `clusters create cluster-b`, `tenants create public --allowed-clusters cluster-a,cluster-b`, `namespaces create public/default --clusters cluster-a,cluster-b`, `namespaces set-replicated-subscription-status public/default --enable`. |

The two-cluster setup is fully scripted; no manual broker poking. The
docker-compose helper is reusable for future multi-cluster e2e tests.

### 5.2 Test

`crates/magnetar/tests/e2e_replicated_subscriptions.rs` (NEW):

| Test | Steps |
| --- | --- |
| `consumer_resumes_within_one_second_after_cluster_failover` | (1) Produce 100 messages on cluster-a. (2) Subscribe with `replicate_subscription_state(true)`. (3) Consume 50 messages, ack them. (4) Sleep 2s (snapshot interval). (5) Kill cluster-a connection. (6) Reconnect to cluster-b. (7) Consume from same subscription. (8) Assert: cursor resumes within **≤ 1 second** of duplicate messages (so at most ~1 sec of redelivery, never < 50). |
| `marker_observation_event_fires_against_real_broker` | Subscribe with `replicate_subscription_state(true)` on a topic with active geo-replication; assert at least one `ReplicatedSubscriptionMarkerObserved` event over ~5 seconds. |

Gating: `#[ignore = "e2e: requires two-cluster Docker fixture"]` +
`feature = "e2e"`. The fixture's start-up is heavier than the
single-broker e2e (~30s vs. ~5s), so the test is **opt-in even in the
`e2e` feature** via a separate `e2e-multi-cluster` sub-feature.
`docs/testing.md` documents the cost.

### 5.3 CI alignment

The two-cluster fixture is **excluded from per-PR CI** (cost +
flake-risk against external network). It runs in a dedicated
**weekly** workflow `.github/workflows/e2e-replicated-subs.yml` (NEW),
modelled on `.github/workflows/moonpool-seed-sweep.yml`
([ADR-0036](../adr/0036-moonpool-seed-sweep-daily-random.md)). Failures
file an automatic GitHub issue; the v0.2.0 release-cut requires the
last weekly run to be green.

## 6. LOC + risk

| Component | LOC est. |
| --- | --- |
| `magnetar-proto` (SubscribeOptions field, marker decoder module, receive filter, observation event) | ~250 |
| `magnetar-runtime-tokio` (`ConsumerBuilder` method + metrics) | ~50 |
| `magnetar-runtime-moonpool` (1:1 builder + scripted-marker `BrokerWorkload` variant) | ~150 |
| Tests (a)+(b)+(c)+(d) | ~350 |
| `magnetar-cli` flag | ~15 |
| E2E two-cluster fixture + tests + weekly workflow | ~250 |
| **Total** | **~1065** |

### Risks

1. **`replicate_subscription_state(true)` is a no-op against
   single-cluster brokers.** Users will report "the flag does
   nothing." Mitigation: `docs/replicated-subscriptions.md` (NEW)
   explains the broker-side prerequisites clearly. The `magnetar-cli`
   flag's help text reproduces a short version.
2. **Two-cluster fixture cost.** Slow per-run + risk of CI flake.
   Mitigation: weekly workflow, not per-PR. Documented in
   `docs/testing.md`.
3. **Marker payload format changes.** Pulsar may extend
   `REPLICATED_SUBSCRIPTION_*` markers (e.g. new kinds, new fields).
   Mitigation: `#[non_exhaustive]` on `ReplicatedSubscriptionMarkerKind`
   and `…Details`; decoder returns `Ok(None)` for unknown kinds.
   Tracked under `docs/follow-ups.md`.
4. **Cursor-resume tolerance.** The e2e test asserts `≤ 1 second`
   tolerance, matching PIP-33's documented contract. If the broker's
   snapshot interval changes (default 1s), the test may flake.
   Mitigation: pin the snapshot interval explicitly in the broker
   config script (`replicatedSubscriptionsSnapshotFrequencyMillis=1000`).

### Rollback

The builder method is opt-in (default `false`). Existing v0.1.0
consumers see no change. The new `Event` variant is on a
`#[non_exhaustive]` enum, so the addition is non-breaking. Rollback
path: revert the four commits (proto, runtime, cli, e2e). No data risk.

## 7. Dependencies + sequencing

PIP-33 has **no upstream prereq** — wire bits are already vendored.
It can land in parallel with PIP-466, PIP-460, PIP-180.

1. **Wave 1**: `magnetar-proto::markers` module + decoder + tests (a).
2. **Wave 2**: `magnetar-proto::consumer` filter + `Event` variant +
   tests (a).
3. **Wave 3**: `ConsumerBuilder::replicate_subscription_state` on
   tokio + tests (b).
4. **Wave 4**: Moonpool 1:1 mirror + scripted-marker workload +
   tests (c).
5. **Wave 5**: `magnetar-differential` tests (d).
6. **Wave 6**: `magnetar-cli` flag.
7. **Wave 7**: E2E two-cluster fixture + weekly workflow + docs.

## 8. Documentation deliverables (same wave)

- `docs/replicated-subscriptions.md` (NEW) — concept overview, builder
  usage, **broker-side prerequisites** (geo-replication, namespace
  config), known caveats, observation event.
- `docs/parity-status.md` — PIP-33 row, `✅ landed`.
- `docs/testing.md` — document the weekly two-cluster fixture.
- `docs/follow-ups.md` — record any open items (e.g. additional marker
  kinds, transparent cluster failover orchestration).
- `README.md` — parity-matrix row update.
- `specs/README.md` — flip ADR-0034 to `Accepted` on sign-off.

## 9. References

- [ADR-0034](../adr/0034-pip-33-replicated-subscriptions-scope.md) — scope.
- [ADR-0024](../adr/0024-cross-runtime-test-and-coverage-policy.md) — test plan binding.
- [ADR-0036](../adr/0036-moonpool-seed-sweep-daily-random.md) — weekly-workflow precedent for the e2e cost-shifting model.
- [ADR-0009](../adr/0009-pulsar-4-minimum.md) — Pulsar 4.0+ baseline (PIP-33 floor is 2.4).
- [`crates/magnetar-proto/proto/PulsarApi.proto:397-400`](../../crates/magnetar-proto/proto/PulsarApi.proto) — `CommandSubscribe.replicate_subscription_state`.
- [`crates/magnetar-proto/proto/PulsarMarkers.proto:25-88`](../../crates/magnetar-proto/proto/PulsarMarkers.proto) — marker wire format.
- Upstream — [PIP-33: Replicated subscriptions](https://github.com/apache/pulsar/wiki/PIP-33%3A-Replicated-subscriptions).
- Apache Pulsar Java — `org.apache.pulsar.client.impl.ConsumerBuilderImpl#replicateSubscriptionState`.
