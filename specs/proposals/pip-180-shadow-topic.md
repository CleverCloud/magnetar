# PIP-180 — Shadow topic (v0.2.0)

- **Status**: Implemented in v0.2.0 (see [docs/shadow-topic.md](../../docs/shadow-topic.md))
- **ADR**: [ADR-0033](../adr/0033-pip-180-shadow-topic-scope.md)
- **Target**: v0.2.0
- **Date**: 2026-05-26
- **Owner**: Florentin Dubois
- **Upstream**: [pip/pip-180.md](https://github.com/apache/pulsar/blob/master/pip/pip-180.md)
- **Upstream readiness**: 🟢 **LIVE.** Merged upstream in Pulsar 2.11
  (early 2023). Wire bits are already vendored in our `PulsarApi.proto`
  (see §1). e2e is unblocked against the v0.1.0 baseline broker
  (`apachepulsar/pulsar:4.0.4`).
- **Broker baseline**: Pulsar 4.0+ — PIP-180 is available against the
  v0.1.0 baseline broker per [ADR-0009](../adr/0009-pulsar-4-minimum.md).

## TL;DR

PIP-180 introduces a read-only topic ownership model that shares
ledgers with a source topic — a lightweight fan-out alternative to
geo-replication targeting up to ~100K subscribers on the shadow side.
Wire support is already vendored. v0.2.0 ships (1) the producer-side
`CommandSend.message_id` propagation so replicator-style producers can
preserve source-topic message IDs, (2) the three admin REST endpoints
(`create` / `delete` / `get` shadow topics), and (3) consumer-side
`MessageId` equality so a shadow-side ID compares equal to its
source-side counterpart. No feature flag — PIP-180 is straight v0.2.0.

## 1. Wire-protocol delta vs. vendored `PulsarApi.proto`

**None.**

All wire pieces are already vendored:

| Field | Location | Status |
| --- | --- | --- |
| `CommandSend.message_id` (`optional MessageIdData`, field 9) | [`PulsarApi.proto:547-548`](../../crates/magnetar-proto/proto/PulsarApi.proto) | **Vendored, unused by encoder.** The comment line 547 says "currently is used in replicator for shadow topic." Today's producer encoder never emits it. |
| `CommandSendReceipt.message_id` | [`PulsarApi.proto:551-555`](../../crates/magnetar-proto/proto/PulsarApi.proto) | Already decoded by `magnetar-proto`. |
| `MessageMetadata` (the receive-side carrier for source-topic context) | [`PulsarApi.proto:107-`](../../crates/magnetar-proto/proto/PulsarApi.proto) | Already vendored. |
| `MessageMetadata.replicated_from` (carries source-cluster name; on shadow topics, also carries source-topic hints in `properties`) | [`PulsarApi.proto:113-115`](../../crates/magnetar-proto/proto/PulsarApi.proto) | Already decoded. |

The work below is **encoder/decoder behaviour** — adding code paths
that exercise already-vendored fields. No proto bump.

## 2. `magnetar-proto` state-machine additions

| Concern | File |
| --- | --- |
| Producer send entry | [`crates/magnetar-proto/src/producer.rs`](../../crates/magnetar-proto/src/producer.rs) |
| `MessageId` equality docs / impl | [`crates/magnetar-proto/src/types.rs`](../../crates/magnetar-proto/src/types.rs) |
| Consumer event variants | [`crates/magnetar-proto/src/event.rs`](../../crates/magnetar-proto/src/event.rs) |
| Receive-path source-topic detection | [`crates/magnetar-proto/src/consumer.rs`](../../crates/magnetar-proto/src/consumer.rs) |

### 2.1 New producer entry — `send_with_source_message_id`

```rust
impl Producer {
    /// PIP-180 replicator-style send. Emits a `CommandSend` carrying
    /// `message_id = Some(source_msg_id)`. Used by producers writing
    /// to a shadow topic that need to preserve the source-topic ID
    /// chain.
    pub fn send_with_source_message_id(
        &mut self,
        source_msg_id: MessageId,
        payload: Bytes,
        metadata: MessageMetadata,
        now: Instant,
    ) -> SendHandle;
}
```

`SendHandle` is the existing send-tracking handle; no changes there.

The existing `send` keeps `CommandSend.message_id = None` — v0.1.0
callers see no change.

### 2.2 `MessageId` equality contract

The existing `MessageId` already derives `PartialEq`/`Eq` over its
structural fields. PIP-180 promotes the existing structural equality
to a **documented contract**:

```rust
/// `MessageId` equality under PIP-180.
///
/// Two `MessageId`s compare equal iff they share `(ledger_id,
/// entry_id, batch_index, partition_index)`. On a shadow topic, the
/// broker presents messages with the **source** `MessageId` (same
/// ledger/entry pointers as the original write). The shadow-side
/// reader observes IDs that compare equal to the source-side reader's
/// IDs — i.e. "same message" is structurally evident.
///
/// Note: under PIP-460 ([scalable topics](../proposals/pip-460-scalable-topics.md)),
/// `MessageId` gains an optional `segment_id`. The cross-mode
/// comparison rule (`Some(_)` vs. `None` → `false`) is defined in
/// the PIP-460 proposal.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct MessageId { /* … */ }
```

No code change in the equality impl itself — just docs + a unit test
pinning the contract.

### 2.3 New `Event` variant — `MessageReceivedFromShadow`

```rust
pub enum Event {
    /* … existing variants … */

    /// PIP-180: a message presented by the broker on a shadow topic.
    /// The shadow_message_id and source_message_id compare equal
    /// under the structural equality contract above.
    MessageReceivedFromShadow {
        consumer_id: ConsumerId,
        source_topic: TopicName,
        source_message_id: MessageId,
        shadow_message_id: MessageId,
        payload: Bytes,
        metadata: MessageMetadata,
    },
}
```

Emitted when the receive path detects a shadow-presented message —
`MessageMetadata.replicated_from` is `Some(_)` and the subscribing
topic is **known to be a shadow** (resolved via the admin REST
`getShadowTopics(source)` hint cached on `ClientState`).

The existing `Event::MessageReceived` continues to fire for non-shadow
topics. Callers that want the shadow context match on the new variant;
callers that don't care can collapse both via a helper.

### 2.4 Source-topic resolution on the receive path

```rust
struct ShadowTopicMetadata {
    source_topic: TopicName,
    /* cached from admin REST */
}

impl Consumer {
    fn classify_received(&self, msg: &MessageMetadata, msg_id: &MessageId)
        -> ReceiveClass;
}

enum ReceiveClass {
    Regular,
    Shadow(ShadowTopicMetadata),
}
```

Shadow-ness is **cached on `Consumer`** at subscribe time via the
admin REST hint provided by the runtime engine — `magnetar-proto` does
no admin REST itself ([ADR-0004](../adr/0004-sans-io-protocol-core.md)).
The engine injects shadow metadata via a new
`Consumer::set_shadow_metadata(...)` sans-io entry.

## 3. Runtime surface ports

### 3.1 `magnetar-runtime-tokio`

| File | Change |
| --- | --- |
| [`crates/magnetar-runtime-tokio/src/producer.rs`](../../crates/magnetar-runtime-tokio/src/producer.rs) | Wire `Producer::send_with_source_message_id` async method through to the sans-io entry. |
| [`crates/magnetar-runtime-tokio/src/consumer.rs`](../../crates/magnetar-runtime-tokio/src/consumer.rs) | Surface `Event::MessageReceivedFromShadow` to the consumer's `recv()` future as a `ReceivedMessage::FromShadow { … }` variant. |
| [`crates/magnetar-runtime-tokio/src/client.rs`](../../crates/magnetar-runtime-tokio/src/client.rs) | On `subscribe()`, query the admin REST `getShadowTopics(...)` and pre-populate shadow metadata on the new `Consumer`. |

#### `magnetar-admin` (lives outside the runtime crates)

| File | Change |
| --- | --- |
| [`crates/magnetar-admin/src/lib.rs`](../../crates/magnetar-admin/src/lib.rs) | New `Topics` trait surface (mirror of Java `org.apache.pulsar.client.admin.Topics`): three new async methods. |

```rust
impl AdminClient {
    /// PUT /admin/v2/persistent/{tenant}/{namespace}/{topic}/shadowTopics
    pub async fn create_shadow_topic(
        &self,
        source: &TopicName,
        shadow: &TopicName,
        properties: ShadowTopicProperties,
    ) -> Result<(), AdminError>;

    /// DELETE /admin/v2/persistent/{tenant}/{namespace}/{topic}/shadowTopics/{shadow}
    pub async fn delete_shadow_topic(
        &self,
        shadow: &TopicName,
    ) -> Result<(), AdminError>;

    /// GET /admin/v2/persistent/{tenant}/{namespace}/{topic}/shadowTopics
    pub async fn get_shadow_topics(
        &self,
        source: &TopicName,
    ) -> Result<Vec<TopicName>, AdminError>;
}

#[derive(Clone, Debug)]
pub struct ShadowTopicProperties {
    pub properties: BTreeMap<String, String>,
}
```

Endpoint paths cross-checked against Java's reference
([`org.apache.pulsar.client.admin.Topics`](https://github.com/apache/pulsar/blob/master/pulsar-client-admin-api/src/main/java/org/apache/pulsar/client/admin/Topics.java#L4670)
— `createShadowTopic` / `deleteShadowTopic` / `getShadowTopics`).
The HTTP error mapping (`404` → `TopicNotFound`, `409` → `Conflict`,
`401`/`403` → `Unauthorized` / `Forbidden`) reuses the existing
`AdminError` taxonomy.

#### Client builder

**No new builder.** Consumers subscribe to a shadow topic by **name**
— the `topic` argument is just the shadow's fully-qualified name. The
client driver detects shadow-ness at subscribe time via the admin REST
hint described above. No `ConsumerBuilder::shadow_of(...)` because
that would imply the client orchestrates the shadow creation, which is
broker-side.

#### Feature flag

**None.** PIP-180 is v4-line, no opt-in.

### 3.2 `magnetar-runtime-moonpool`

| File | Change |
| --- | --- |
| [`crates/magnetar-runtime-moonpool/src/producer.rs`](../../crates/magnetar-runtime-moonpool/src/producer.rs) | 1:1 mirror of the tokio surface — `send_with_source_message_id` rides the existing sans-io entry. |
| [`crates/magnetar-runtime-moonpool/src/consumer.rs`](../../crates/magnetar-runtime-moonpool/src/consumer.rs) | 1:1 mirror — `Event::MessageReceivedFromShadow` → `ReceivedMessage::FromShadow`. |
| [`crates/magnetar-runtime-moonpool/src/client.rs`](../../crates/magnetar-runtime-moonpool/src/client.rs) | Sub-script the admin REST `getShadowTopics(...)` reply from the in-process fake (no real HTTP under simulator). |
| [`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`](../../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs) | `BrokerWorkload` gains a `ShadowTopic { source, shadow }` mode: replies to `CommandSend` with a `CommandSendReceipt` whose `message_id` field is the **client-provided** `source_msg_id` (round-trip preservation). |
| `crates/magnetar-runtime-moonpool/tests/scripted_admin.rs` (extend) | Add scripted handlers for the three new admin endpoints — no new file, extend the existing scripted admin fake. |

The moonpool engine has no real reqwest; the admin REST methods are
**tokio-only** on the production path. For tests, the moonpool client
state exposes a `ShadowMetadataInjector` trait that the simulator
fulfils directly — same pattern as the existing admin scripting.

### 3.3 `magnetar-cli`

`magnetar shadow create <source> <shadow>` /
`magnetar shadow delete <shadow>` /
`magnetar shadow list <source>` subcommands, ~120 LOC under
`crates/magnetar-cli/src/cmd/shadow.rs` (NEW). No feature flag.

## 4. Four-layer test plan ([ADR-0024](../adr/0024-cross-runtime-test-and-coverage-policy.md))

All four layers required — PIP-180 changes sans-io behaviour
(`send_with_source_message_id`, marker-free shadow detection) and the
runtime surfaces.

### (a) `magnetar-proto` unit

| Test | File |
| --- | --- |
| `command_send_with_message_id_roundtrip` | `crates/magnetar-proto/src/producer.rs` (`#[cfg(test)] mod tests`) |
| `command_send_without_message_id_byte_identical_to_v01` | same — guards backward compat |
| `command_send_receipt_with_message_id_roundtrip` | same |
| `message_id_equality_shadow_vs_source` | `crates/magnetar-proto/src/types.rs` |
| `consumer_classifies_shadow_via_metadata` | `crates/magnetar-proto/src/consumer.rs` |
| `consumer_emits_message_received_from_shadow` | same |
| `consumer_emits_message_received_for_non_shadow` | same — proves no regression |

### (b) `magnetar-runtime-tokio` integration

`crates/magnetar/tests/shadow_topic.rs` (NEW):

| Test | Asserts |
| --- | --- |
| `producer_send_with_source_id_emits_field` | Wire byte capture via `magnetar-fakes` shows `CommandSend.message_id` populated. |
| `producer_send_normal_does_not_emit_field` | Negative — `CommandSend.message_id` is `None`. |
| `consumer_observes_shadow_from_variant` | Receive a fake-replayed message; assert `ReceivedMessage::FromShadow` returned. |
| `consumer_message_id_equals_source_message_id` | Structural equality on (`ledger_id`, `entry_id`, …). |
| `admin_create_shadow_topic_puts_correct_url` | `wiremock` asserts the request URL + JSON body. |
| `admin_delete_shadow_topic_uses_delete_verb` | same |
| `admin_get_shadow_topics_parses_response_array` | same |
| `subscribe_pre_populates_shadow_metadata` | After `subscribe(shadow_topic)`, the `Consumer` holds the resolved source-topic name (via admin REST hint). |

The `wiremock` dependency is already in scope for `magnetar-admin`'s
existing tests.

### (c) `magnetar-runtime-moonpool` integration

**Same eight test names, 1:1**, under
`crates/magnetar/tests/shadow_topic_moonpool.rs` (NEW). Each runs
inside a `SimulationBuilder` with the `BrokerWorkload::ShadowTopic`
variant and the scripted admin fake. Coverage: 100% on the diff
(`cargo xtask check-sim-coverage`).

### (d) `magnetar-differential`

`crates/magnetar-differential/tests/shadow_topic_equivalence.rs` (NEW):

| Test | Asserts |
| --- | --- |
| `send_with_source_id_event_stream_parity` | Identical broker transcript → identical `Vec<Event>` across engines. |
| `consumer_shadow_event_stream_parity` | Same, on the receive side. |

Golden trace under
`crates/magnetar-differential/tests/golden/shadow_send_with_source.json`
(human-reviewable, regenerated via `MAGNETAR_REGENERATE_GOLDEN=1`).

### Exemptions: none

All four layers are binding for PIP-180.

## 5. E2E plan

**Pulsar 4.0.4 already ships PIP-180** (PIP merged in 2.11). No
multi-cluster requirement — single-broker is sufficient.

| Item | Plan |
| --- | --- |
| Image | `apachepulsar/pulsar:4.0.4` (existing in CI). |
| Test file | `crates/magnetar/tests/e2e_shadow_topic.rs` (NEW). |
| Gating | `#[ignore = "e2e: requires Docker"]` + `feature = "e2e"`. |
| Coverage | (1) `admin_create_shadow_topic` against live broker; (2) produce on source → consume on shadow → assert `MessageId` equality; (3) produce on source with `send_with_source_message_id` → consume on shadow → assert source-id preserved; (4) `admin_get_shadow_topics(source)` returns the created shadow; (5) `admin_delete_shadow_topic(shadow)` removes it. |

No additional docker-compose helper — uses the existing single-broker
fixture under `crates/magnetar/tests/helpers/`. The broker needs no
special config; PIP-180 is on by default in Pulsar 4.x.

### Pre-flight check

Before the test starts, it does a `pulsar-admin brokers version`
lookup and skips with a clear error if the broker is `< 2.11.0` — the
PIP-180 floor. Magnetar's broker baseline of 4.0+ makes this defensive
only, but it documents the lower bound for users running custom
clusters.

## 6. LOC + risk

| Component | LOC est. |
| --- | --- |
| `magnetar-proto` (producer entry + receive classification + `Event` variant) | ~200 |
| `magnetar-admin` (3 methods + `ShadowTopicProperties` + error mapping) | ~250 |
| `magnetar-runtime-tokio` (producer / consumer surface) | ~150 |
| `magnetar-runtime-moonpool` (1:1 mirror) | ~150 |
| Moonpool scripted-broker + admin extension | ~150 |
| Tests (a)+(b)+(c)+(d) | ~400 |
| `magnetar-cli shadow` subcommand | ~120 |
| `e2e_shadow_topic.rs` | ~150 |
| **Total** | **~1570** |

### Risks

1. **Source-topic resolution races.** The admin REST `getShadowTopics`
   lookup is cached on `Consumer` at subscribe time. If the user
   creates a new shadow after subscribe, the consumer won't pick it
   up. Mitigation: documented behaviour in `docs/shadow-topic.md`
   (NEW). The CLI's `magnetar shadow list <source>` makes the cache
   inspectable.
2. **Source `MessageId` is client-asserted.** The broker validates
   that the producer is authorised to write to the shadow topic but
   does **not** cryptographically prove the source-message-id matches
   a real source message. This is upstream's behaviour, not a magnetar
   gap. Documented in `docs/parity-status.md` under "PIP-180 caveats".
3. **`MessageReceivedFromShadow` is a new sum-variant.** Callers
   exhaustively matching `Event` will need to add the arm. Mitigation:
   `Event` is `#[non_exhaustive]` already; the variant addition is
   non-breaking by convention.
4. **Admin REST endpoint drift.** PIP-180's REST paths are stable
   since 2.11; an upstream change is unlikely but possible.
   Mitigation: the e2e test pins the contract; CI catches drift.

### Rollback

The producer-side `send_with_source_message_id` entry is opt-in (a
distinct method); regular `send` is unchanged. The admin methods are
additive. The new `Event` variant is `#[non_exhaustive]`-tolerant.
Rollback path: revert the three commits (proto, admin, surface). No
data risk.

## 7. Dependencies + sequencing

PIP-180 has **no upstream prereq** beyond what already ships in
Pulsar 4.0.4. It can land in parallel with PIP-466, PIP-460, PIP-33.

1. **Wave 1**: `magnetar-proto` producer entry + receive classification
   + tests (a).
2. **Wave 2**: `magnetar-admin` three methods + tests (b for admin).
3. **Wave 3**: `magnetar-runtime-tokio` surface + tests (b for
   producer/consumer).
4. **Wave 4**: `magnetar-runtime-moonpool` mirror + tests (c).
5. **Wave 5**: `magnetar-differential` tests (d).
6. **Wave 6**: `magnetar-cli shadow` subcommand.
7. **Wave 7**: e2e + docs.

## 8. Documentation deliverables (same wave)

- `docs/shadow-topic.md` (NEW) — shadow-topic concept, admin REST
  surface, producer-with-source-id contract, consumer-side equality,
  caveats.
- `docs/parity-status.md` — PIP-180 row, `✅ landed`.
- `docs/follow-ups.md` — record any open items (e.g. transparent
  reconcile of shadow-metadata cache if a shadow is created post-subscribe).
- `README.md` — parity-matrix row update.
- `specs/README.md` — flip ADR-0033 to `Accepted` on sign-off.

## 9. References

- [ADR-0033](../adr/0033-pip-180-shadow-topic-scope.md) — scope.
- [ADR-0024](../adr/0024-cross-runtime-test-and-coverage-policy.md) — test plan binding.
- [ADR-0009](../adr/0009-pulsar-4-minimum.md) — Pulsar 4.0+ baseline.
- [ADR-0004](../adr/0004-sans-io-protocol-core.md) — `magnetar-proto` zero-I/O constraint.
- [`crates/magnetar-proto/proto/PulsarApi.proto:547`](../../crates/magnetar-proto/proto/PulsarApi.proto) — optional `message_id` on `CommandSend`.
- Upstream PIP — [pip/pip-180.md](https://github.com/apache/pulsar/blob/master/pip/pip-180.md).
- Apache Pulsar Java — `org.apache.pulsar.client.admin.Topics#createShadowTopic`.
- Companion proposal — [PIP-460](pip-460-scalable-topics.md) introduces the optional `MessageId.segment_id`; cross-mode equality rules are pinned in §2.2 above and in PIP-460 §2.1.
