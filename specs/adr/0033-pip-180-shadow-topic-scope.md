# ADR-0033 — PIP-180 shadow topic scope for v0.2.0

- **Status**: Proposed
- **Date**: 2026-05-26
- **Decider**: Florentin Dubois
- **Tags**: pip-180, shadow-topic, admin, v0.2.0, scope

## Context

[ADR-0010](0010-v0-1-full-java-parity.md) listed PIP-180 (shadow
topic) in v0.1.0 scope. PIP-180 introduces a read-only topic
ownership model that shares underlying storage ledgers from a
source topic — a lightweight alternative to geo-replication for
broadcast-style fan-out (up to ~100K subscriptions on the shadow).
On detailed review of the implementation requirements, **PIP-180 is
larger than ADR-0010's listing suggested**: it touches the
producer-side wire (an optional `message_id` field on `CommandSend`
to carry the source-topic message ID through the shadow), the admin
REST surface (three new endpoints + three new client methods), and
introduces shadow-aware semantics on the consumer side
(message-ID equality across source ⇄ shadow). Magnetar's v0.1.0
finishing wave has no room for the consumer-side and admin-REST
work without slipping the rest of the parity matrix.

Today there is no PIP-180 scaffolding. The
`CommandSend` encoder
([`crates/magnetar-proto/src/producer.rs`](../../crates/magnetar-proto/src/producer.rs))
emits the v4 send command with no optional `message_id`. The
`magnetar-admin` crate
([`crates/magnetar-admin/src/`](../../crates/magnetar-admin))
has no `create_shadow_topic` / `delete_shadow_topic` /
`get_shadow_topics` methods. Java's reference is
`org.apache.pulsar.client.admin.Topics` (interface methods
`createShadowTopic` / `deleteShadowTopic` / `getShadowTopics` plus
async variants), and broker REST endpoints under
`/{tenant}/{namespace}/{topic}/shadowTopics`.

The vendored proto already carries the wire bit. From the
`PulsarApi.proto` review (line 547):
> `// Message id of this message, currently is used in replicator
> for shadow topic.`
indicating the optional `MessageIdData` is present on `CommandSend`
since the proto bump that brought it in.

This ADR locks the v0.2.0 PIP-180 surface: producer-side
`CommandSend` shadow `message_id` propagation, the three admin
methods, consumer-side equality semantics on `MessageId`, plus the
parity-matrix amendment lifting PIP-180 out of v0.1.0 scope.

## Decision

- **Wire-protocol delta vs. current vendored PulsarApi.proto: none.**
  PIP-180's optional `message_id` on `CommandSend` is already
  present in the vendored proto
  ([`PulsarApi.proto:547`](../../crates/magnetar-proto/proto/PulsarApi.proto)
  comment). No proto bump required. The producer encoder needs to
  start **emitting** the field on the shadow-replication path;
  today it never does.

- **`magnetar-proto` state-machine additions.**
  - `Producer::send_with_source_message_id(source_msg_id: MessageId,
    payload: Bytes, now: Instant) -> SendHandle` — new entry that
    sets the optional `CommandSend.message_id` to `Some(source_msg_id)`.
    Used by shadow-topic replicator-style producers.
  - `Event::MessageReceivedFromShadow { source_topic: String,
    source_message_id: MessageId, shadow_message_id: MessageId,
    payload: Bytes }` — new consumer event when the broker
    presents a message originating from a source topic. The
    `source_topic` is resolved from the broker's
    `originalProducerName` + topic-metadata hints.
  - `MessageId` equality + `Ord`: extended so a shadow-side
    `MessageId` and its source-side counterpart compare equal
    when they share `(ledger_id, entry_id, batch_index,
    partition_index)`. This is the user-visible "same message"
    contract on PIP-180. Implemented as a documented `PartialEq`
    override; today's derived impl already produces this for
    structurally identical fields.

- **`magnetar-runtime-tokio` surface.**
  - New `magnetar_admin::Topics::create_shadow_topic(source:
    &TopicName, shadow: &TopicName, properties: ShadowTopicProperties)`
    method on the admin REST client. Backed by HTTP PUT
    `${admin_url}/admin/v2/persistent/{tenant}/{namespace}/{topic}/shadowTopics`
    with a JSON body listing the source topic.
  - `magnetar_admin::Topics::delete_shadow_topic(shadow: &TopicName)`
    — DELETE on the same path.
  - `magnetar_admin::Topics::get_shadow_topics(source: &TopicName)
    -> Vec<TopicName>` — GET on
    `${admin_url}/admin/v2/persistent/{tenant}/{namespace}/{topic}/shadowTopics`.
  - **No new client builder.** Consumers subscribe to a shadow
    topic by name — the client driver detects shadow-ness from the
    admin REST `getShadowTopics(source)` hint on the topic
    metadata, or simply consumes shadow data transparently.
  - New feature flag: **none**. PIP-180 is a v4-line PIP and
    works against the v0.1.0 baseline broker
    ([ADR-0009](0009-pulsar-4-minimum.md)) — no opt-in flag.

- **`magnetar-runtime-moonpool` port.** The producer-side
  `send_with_source_message_id` is a sans-io entry; moonpool
  inherits it through the existing `Producer` driver. The
  admin REST methods are tokio-only (reqwest); the moonpool port
  re-uses the existing fake admin REST surface, scripting the
  three new endpoint responses. No new sim-side broker fake
  required: `BrokerWorkload`
  ([`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`](../../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs))
  already replies to `CommandSend`; it gains a scripted reply
  that includes the optional `message_id` field on the
  `CommandSendReceipt`.

- **No new auth dependency.** PIP-180 reuses topic-level
  authorisation via the broker's namespace ACL. No new auth
  provider, no token-shape change.

## Consequences

- **Test layers per ADR-0024 (4-layer):**
  (a) `magnetar-proto` unit: encode `CommandSend` with and
  without `message_id`; decode `CommandSendReceipt`-with-source
  round-trip; `MessageId` equality on shadow ⇄ source pairs.
  (b) `magnetar-runtime-tokio` integration:
  `send_with_source_message_id` produces a wire frame carrying
  the field; admin REST tests using `wiremock` for the three
  new endpoints.
  (c) `magnetar-runtime-moonpool` integration: identical
  send-with-source test under `SimulationBuilder`; scripted
  admin REST fake.
  (d) `magnetar-differential`: equivalence of producer
  `EventStream` for the source-message-id-bearing send path.

- **E2E fixture needs.** `apachepulsar/pulsar:4.0.4` already
  ships PIP-180. The e2e fixture
  (`crates/magnetar/tests/e2e_shadow_topic.rs`) creates a source
  topic, creates a shadow via the new admin method, produces to
  the source, consumes from both, asserts identical `MessageId`s.
  Gated by the standard `e2e` feature + `#[ignore = "e2e:
  requires Docker"]`. No additional containers beyond the
  existing Pulsar image.

- **LOC estimate.** ~500–800 LOC total. Breakdown:
  ~150 LOC `magnetar-proto` producer-side send-with-source path
  + `MessageId` equality docs; ~250 LOC `magnetar-admin` shadow
  topic methods + types; ~300 LOC tests (4-layer + e2e).

- **Security implications.** Limited. PIP-180 doesn't introduce
  a new principal or trust boundary — shadow topics inherit the
  source topic's ACL. The admin methods inherit the existing
  admin-REST auth flow. One small consideration: the source
  `MessageId` on `CommandSend` is **client-asserted**; the
  broker validates that the producer is authorised to write to
  the shadow topic but does not (and cannot) cryptographically
  prove the source-message-id matches a real source message.
  This is upstream's behaviour, not a magnetar-specific gap —
  documented in `docs/parity-status.md`.

## Status

Proposed (awaiting Florentin sign-off, 2026-05-26)

## References

- [ADR-0009](0009-pulsar-4-minimum.md) — Pulsar 4.0+ minimum;
  PIP-180 is available on 4.x.
- [ADR-0010](0010-v0-1-full-java-parity.md) — v0.1.0 parity
  scope; this ADR is the basis for lifting PIP-180 out of v0.1.0
  to v0.2.0.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) —
  four-layer test plan binding.
- PIP-180 (Shadow Topic) —
  <https://github.com/apache/pulsar/blob/master/pip/pip-180.md>
- Apache Pulsar Java —
  `org.apache.pulsar.client.admin.Topics#createShadowTopic`,
  `org.apache.pulsar.client.api.PulsarClient`.
- `crates/magnetar-proto/proto/PulsarApi.proto:547` — comment
  documenting the optional `message_id` on `CommandSend`.
