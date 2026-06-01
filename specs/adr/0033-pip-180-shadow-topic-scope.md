# ADR-0033 — PIP-180 shadow topic scope

- **Status**: Accepted (2026-05-26)
- **Date**: 2026-05-26
- **Decider**: Florentin Dubois
- **Tags**: pip-180, shadow-topic, admin, scope

## Context

[ADR-0010](0010-v0-1-full-java-parity.md) listed PIP-180 (shadow topic) in core-parity scope.
PIP-180 introduces a read-only topic ownership model that shares underlying storage ledgers from a source topic — a lightweight alternative to geo-replication for broadcast-style fan-out (up to ~100K subscriptions on the shadow).
On detailed review of the implementation requirements, **PIP-180 is larger than ADR-0010's listing suggested**: it touches the producer-side wire (an optional `message_id` field on `CommandSend` to carry the source-topic message ID through the shadow), the admin REST surface (three new endpoints + three new client methods), and introduces shadow-aware semantics on the consumer side (message-ID equality across source ⇄ shadow).
The parity finishing wave had no room for the consumer-side and admin-REST work without slipping the rest of the parity matrix, so PIP-180 ships as a focused follow-up locked by this ADR.

Today there is no PIP-180 scaffolding.
The `CommandSend` encoder ([`crates/magnetar-proto/src/producer.rs`](../../crates/magnetar-proto/src/producer.rs)) emits the v4 send command with no optional `message_id`.
The `magnetar-admin` crate ([`crates/magnetar-admin/src/`](../../crates/magnetar-admin)) has no `create_shadow_topic` / `delete_shadow_topic` / `get_shadow_topics` methods.
Java's reference is `org.apache.pulsar.client.admin.Topics` (interface methods `createShadowTopic` / `deleteShadowTopic` / `getShadowTopics` plus async variants), and broker REST endpoints under `/{tenant}/{namespace}/{topic}/shadowTopics`.

The vendored proto already carries the wire bit.
From the `PulsarApi.proto` review (line 547):

> `// Message id of this message, currently is used in replicator for shadow topic.`
> indicating the optional `MessageIdData` is present on `CommandSend` since the proto bump that brought it in.

This ADR locks the PIP-180 surface: producer-side `CommandSend` shadow `message_id` propagation, the three admin methods, consumer-side equality semantics on `MessageId`, plus the parity-matrix amendment lifting PIP-180 out of the original ADR-0010 core-parity scope.

## Decision

- **Wire-protocol delta vs. current vendored PulsarApi.proto: none.** PIP-180's optional `message_id` on `CommandSend` is already present in the vendored proto ([`PulsarApi.proto:547`](../../crates/magnetar-proto/proto/PulsarApi.proto) comment).
  No proto bump required.
  The producer encoder needs to start **emitting** the field on the shadow-replication path; today it never does.

- **`magnetar-proto` state-machine additions.**
  - `Producer::send_with_source_message_id(source_msg_id: MessageId, payload: Bytes, now: Instant) -> SendHandle` — new entry that sets the optional `CommandSend.message_id` to `Some(source_msg_id)`.
    Used by shadow-topic replicator-style producers.
  - `Event::MessageReceivedFromShadow { source_topic: String, source_message_id: MessageId, shadow_message_id: MessageId, payload: Bytes }` — new consumer event when the broker presents a message originating from a source topic.
    The `source_topic` is resolved from the broker's `originalProducerName` + topic-metadata hints.
  - `MessageId` equality + `Ord`: extended so a shadow-side `MessageId` and its source-side counterpart compare equal when they share `(ledger_id, entry_id, batch_index, partition_index)`.
    This is the user-visible "same message" contract on PIP-180.
    Implemented as a documented `PartialEq` override; today's derived impl already produces this for structurally identical fields.

- **`magnetar-runtime-tokio` surface.**
  - New `magnetar_admin::Topics::create_shadow_topic(source: &TopicName, shadow: &TopicName, properties: ShadowTopicProperties)` method on the admin REST client.
    Backed by HTTP PUT `${admin_url}/admin/v2/persistent/{tenant}/{namespace}/{topic}/shadowTopics` with a JSON body listing the source topic.
  - `magnetar_admin::Topics::delete_shadow_topic(shadow: &TopicName)` — DELETE on the same path.
  - `magnetar_admin::Topics::get_shadow_topics(source: &TopicName) -> Vec<TopicName>` — GET on `${admin_url}/admin/v2/persistent/{tenant}/{namespace}/{topic}/shadowTopics`.
  - **No new client builder.** Consumers subscribe to a shadow topic by name — the client driver detects shadow-ness from the admin REST `getShadowTopics(source)` hint on the topic metadata, or simply consumes shadow data transparently.
  - New feature flag: **none**. PIP-180 is a v4-line PIP and works against the baseline Pulsar 4.0+ broker ([ADR-0009](0009-pulsar-4-minimum.md)) — no opt-in flag.

- **`magnetar-runtime-moonpool` port.** The producer-side `send_with_source_message_id` is a sans-io entry; moonpool inherits it through the existing `Producer` driver.
  The admin REST methods are tokio-only (reqwest); the moonpool port re-uses the existing fake admin REST surface, scripting the three new endpoint responses.
  No new sim-side broker fake required: `BrokerWorkload` ([`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`](../../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs)) already replies to `CommandSend`; it gains a scripted reply that includes the optional `message_id` field on the `CommandSendReceipt`.

- **No new auth dependency.** PIP-180 reuses topic-level authorisation via the broker's namespace ACL.
  No new auth provider, no token-shape change.

## Consequences

- **Test layers per ADR-0024 (4-layer):** (a) `magnetar-proto` unit: encode `CommandSend` with and without `message_id`; decode `CommandSendReceipt`-with-source round-trip; `MessageId` equality on shadow ⇄ source pairs.
  (b) `magnetar-runtime-tokio` integration: `send_with_source_message_id` produces a wire frame carrying the field; admin REST tests using `wiremock` for the three new endpoints.
  (c) `magnetar-runtime-moonpool` integration: identical send-with-source test under `SimulationBuilder`; scripted admin REST fake.
  (d) `magnetar-differential`: equivalence of producer `EventStream` for the source-message-id-bearing send path.

- **E2E fixture needs.** `apachepulsar/pulsar:4.0.4` already ships PIP-180.
  The e2e fixture (`crates/magnetar/tests/e2e_shadow_topic.rs`) creates a source topic, creates a shadow via the new admin method, produces to the source, consumes from both, asserts identical `MessageId`s.
  Gated by the standard `e2e` feature + `#[ignore = "e2e: requires Docker"]`.
  No additional containers beyond the existing Pulsar image.

- **LOC estimate.** ~500–800 LOC total. Breakdown: ~150 LOC `magnetar-proto` producer-side send-with-source path
  - `MessageId` equality docs; ~250 LOC `magnetar-admin` shadow
    topic methods + types; ~300 LOC tests (4-layer + e2e).

- **Security implications.** Limited.
  PIP-180 doesn't introduce a new principal or trust boundary — shadow topics inherit the source topic's ACL.
  The admin methods inherit the existing admin-REST auth flow.
  One small consideration: the source `MessageId` on `CommandSend` is **client-asserted**; the broker validates that the producer is authorised to write to the shadow topic but does not (and cannot) cryptographically prove the source-message-id matches a real source message.
  This is upstream's behaviour, not a magnetar-specific gap — documented in `README.md`'s parity-matrix row.

## Status

Accepted (2026-05-26).
Implemented in `feat/pip-180-shadow-topic` — producer-side `send_with_source_message_id` propagating `CommandSend.message_id`, consumer-side `ConnectionEvent::MessageReceivedFromShadow` classification driven by a sans-io `ShadowTopicMetadata` cache, three admin REST methods (`create_shadow_topic` / `delete_shadow_topic` / `get_shadow_topics`, plus the inverse `get_shadow_source`), `magnetar shadow {create,delete, list,source}` CLI subcommands, four-layer test set (proto unit, tokio integration, moonpool 1:1 mirror, differential equivalence) + e2e against `apachepulsar/pulsar:4.0.4`.
No proto bump, no feature flag — regular sends remain byte-identical to the pre-PIP-180 behaviour.
User-facing docs at [`docs/pip-features.md#shadow-topics-pip-180`](../../docs/pip-features.md#shadow-topics-pip-180).

## Implementation footprint

The detailed implementation map originally lived under `specs/proposals/pip-180-shadow-topic.md`; it was folded back into this ADR once the work landed.
Authoritative landing artefacts:

| Concern                                                                | File                                                                                                        |
| ---------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Producer send entry (`send_with_source_message_id`)                    | [`crates/magnetar-proto/src/producer.rs`](../../crates/magnetar-proto/src/producer.rs)                      |
| Receive-path source-topic classification + `ShadowTopicMetadata` cache | [`crates/magnetar-proto/src/consumer.rs`](../../crates/magnetar-proto/src/consumer.rs)                      |
| `Event::MessageReceivedFromShadow` variant                             | [`crates/magnetar-proto/src/event.rs`](../../crates/magnetar-proto/src/event.rs)                            |
| Admin REST surface (create / delete / get / get-source)                | [`crates/magnetar-admin/src/`](../../crates/magnetar-admin)                                                 |
| Tokio runtime wiring (producer + consumer + subscribe hint)            | [`crates/magnetar-runtime-tokio/src/`](../../crates/magnetar-runtime-tokio/src)                             |
| Moonpool runtime mirror + scripted broker `ShadowTopic` workload       | [`crates/magnetar-runtime-moonpool/src/`](../../crates/magnetar-runtime-moonpool/src), `tests/sim_chaos.rs` |
| Differential golden trace                                              | `crates/magnetar-differential/tests/golden/shadow_send_with_source.json`                                    |
| CLI `magnetar shadow {create,delete,list,source}`                      | [`crates/magnetar-cli/src/main.rs`](../../crates/magnetar-cli/src/main.rs) (`ShadowCmd` + `run_shadow`)     |
| User docs                                                              | [`docs/pip-features.md#shadow-topics-pip-180`](../../docs/pip-features.md#shadow-topics-pip-180)            |
| E2E                                                                    | `crates/magnetar/tests/e2e_shadow_topic.rs`                                                                 |

Total landed footprint ≈ 1.5K LOC including tests.
`MessageId` equality keeps the structural contract `(ledger_id, entry_id, batch_index, partition_index)`; the consumer-side classifier resolves shadow-ness from the admin REST `getShadowTopics(source)` hint cached on the `Consumer` at subscribe time.

## References

- [ADR-0009](0009-pulsar-4-minimum.md) — Pulsar 4.0+ minimum; PIP-180 is available on 4.x.
- [ADR-0010](0010-v0-1-full-java-parity.md) — parity scope; this ADR is the basis for lifting PIP-180 out of the original core-parity scope into a focused follow-up.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — four-layer test plan binding.
- PIP-180 (Shadow Topic) —
  <https://github.com/apache/pulsar/blob/master/pip/pip-180.md>
- Apache Pulsar Java — `org.apache.pulsar.client.admin.Topics#createShadowTopic`, `org.apache.pulsar.client.api.PulsarClient`.
- `crates/magnetar-proto/proto/PulsarApi.proto:547` — comment documenting the optional `message_id` on `CommandSend`.
