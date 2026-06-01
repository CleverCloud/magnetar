# Shadow topics (PIP-180)

Status: ✅ supported — scope locked in
[ADR-0033](../specs/adr/0033-pip-180-shadow-topic-scope.md).

PIP-180 introduces a read-only topic ownership model that shares the
underlying BookKeeper ledgers of a **source topic** — a lightweight
fan-out alternative to geo-replication, targeting up to ~100K shadow-side
subscribers without re-paying the storage cost.

This page documents the magnetar surface for shadow topics: the admin
REST methods, the producer-side replicator entry, the consumer-side
classification contract, and the caveats inherited from upstream.

## When to use shadow topics

A shadow topic is the right tool when you need:

- **High-fanout read paths** sharing one source-of-truth ledger
  (broadcast / dashboards / cache-fill).
- **Lightweight geo-style read replicas** within a single cluster
  (PIP-180 is intra-cluster — for cross-cluster, use geo-replication).
- **Stable consumer groups** that should observe the **same**
  `MessageId` as the source-side reader (e.g. cross-side
  deduplication, cross-side correlation logs).

It is **not** a substitute for:

- Cross-cluster replication (use geo-replication).
- Independent storage / retention on the shadow side (the shadow shares
  the source's ledger).

## Surface

### `magnetar-admin::AdminClient` — three new methods

```rust
use magnetar_admin::{AdminClient, ShadowTopicProperties};

let admin = AdminClient::builder()
    .service_url("http://broker:8080".parse().unwrap())
    .build()
    .unwrap();

// Create the shadow on top of a source. `properties` is a free-form
// `BTreeMap<String, String>` (mirrors Java's
// `org.apache.pulsar.client.admin.Topics#createShadowTopic` third arg).
admin
    .create_shadow_topic(
        "persistent://public/default/source",
        "persistent://public/default/shadow",
        ShadowTopicProperties::default(),
    )
    .await?;

// List the shadows of a source.
let shadows = admin
    .get_shadow_topics("persistent://public/default/source")
    .await?;

// Resolve the source of a shadow. Returns `None` for a non-shadow topic.
let source = admin
    .get_shadow_source("persistent://public/default/shadow")
    .await?;

// Delete the shadow.
admin
    .delete_shadow_topic("persistent://public/default/shadow", /* force */ true)
    .await?;
```

Endpoint paths mirror the upstream broker (
`pulsar-broker/.../v2/PersistentTopics.java`):

| Method                    | Verb   | Path                                                                            |
| ------------------------- | ------ | ------------------------------------------------------------------------------- |
| `create_shadow_topic`     | PUT    | `/admin/v2/persistent/{tenant}/{namespace}/{source}/shadowTopics`               |
| `delete_shadow_topic`     | DELETE | `/admin/v2/persistent/{tenant}/{namespace}/{shadow}?force={force}`              |
| `get_shadow_topics`       | GET    | `/admin/v2/persistent/{tenant}/{namespace}/{source}/shadowTopics`               |
| `get_shadow_source`       | GET    | `/admin/v2/persistent/{tenant}/{namespace}/{shadow}/shadowSource`               |

Errors reuse the existing `AdminError` taxonomy — `404` → `Status { code:
404, .. }` (topic not found), `409` → `Status { code: 409, .. }` (shadow
already exists), `401`/`403` → `Status { code: 401|403, .. }` (auth).

### Producer — replicator-style send

`magnetar_runtime_tokio::Producer::send_with_source_message_id` (mirror
on the moonpool side) propagates a source-topic `MessageId` on the wire
via `CommandSend.message_id`. The broker echoes the asserted id back on
the resulting `CommandSendReceipt`, so the returned `SendFut` resolves
to a `MessageId` structurally equal to the asserted one.

```rust
use magnetar_proto::MessageId;

let source_id = MessageId {
    ledger_id: 99,
    entry_id: 42,
    partition: 0,
    batch_index: -1,
    batch_size: 0,
};
let receipt_id = producer
    .send_with_source_message_id(
        source_id,
        b"replicated payload".as_slice(),
        Default::default(),
    )
    .await?;
assert_eq!(receipt_id, source_id); // round-trip preservation
```

This entry **bypasses batching** by design — mirrors Java's
`org.apache.pulsar.broker.service.persistent.Replicator`, which writes
each entry one at a time. Chunking still applies for payloads larger
than `max_message_size`; in that case the same `source_msg_id` is
stamped on every chunk (one logical message, multiple frames).

The regular `Producer::send(...)` continues to emit `CommandSend.message_id =
None` — byte-identical on the wire (no proto bump). The new field on
`OutgoingMessage` defaults to `None` so callers that don't use the
replicator entry see no change.

### Consumer — shadow-side classification

A shadow-attached `Consumer` emits a distinct event variant on every
shadow-presented message so callers can observe the source-topic context
without an out-of-band lookup:

```rust
use magnetar_proto::ConnectionEvent;

// At subscribe time, pre-populate the shadow metadata. The runtime's
// admin REST helper does this automatically via `get_shadow_source`;
// direct callers (tests, integration scenarios) can set it themselves.
consumer.set_shadow_source("persistent://public/default/source-t");

// On every inbound message carrying `MessageMetadata.replicated_from`,
// the connection emits `MessageReceivedFromShadow` instead of `Message`:
//
//   ConnectionEvent::MessageReceivedFromShadow {
//       handle,
//       source_topic,           // resolved at subscribe time
//       source_message_id,      // == shadow_message_id (structural equality)
//       shadow_message_id,
//       message,                // the full IncomingMessage
//   }
//
// Callers that don't care about the shadow context can collapse this
// variant onto `Message` by inspecting `message`. The variant is
// non-breaking by convention (`ConnectionEvent` is treated as
// `#[non_exhaustive]` per ADR-0033's "new sum-variant is additive"
// risk note).
```

### `MessageId` equality contract

PIP-180 promotes the existing structural-equality on `MessageId` to a
**documented contract**:

> Two `MessageId`s compare equal iff they share `(ledger_id, entry_id,
> partition, batch_index, batch_size)`. On a shadow topic, the broker
> presents messages with the **source** `MessageId` (same ledger/entry
> pointers as the original write). The shadow-side reader observes ids
> that compare equal to the source-side reader's ids — "same message"
> is structurally evident.

This means cross-side deduplication needs no out-of-band correlation
key: a `HashSet<MessageId>` populated by a source-side reader will
collide with shadow-side ids on the same physical entry.

See the pinned unit test in
[`crates/magnetar-proto/src/types.rs`](../crates/magnetar-proto/src/types.rs)
(`message_id_equality_shadow_vs_source`) for the contract guard.

## CLI

```sh
# Create a shadow topic
magnetar shadow create persistent://public/default/source \
                       persistent://public/default/shadow

# List the shadows of a source
magnetar shadow list persistent://public/default/source

# Resolve a shadow's source
magnetar shadow source persistent://public/default/shadow

# Delete a shadow
magnetar shadow delete persistent://public/default/shadow --force
```

## Caveats

### Client-asserted source `MessageId`

`Producer::send_with_source_message_id` lets the client assert any
`MessageId` it wants on the outbound `CommandSend.message_id`. The
broker validates that the producer is authorised to write to the shadow
topic, but does **not** cryptographically prove that the
source-message-id matches a real entry on the source topic. This is
upstream's behaviour (see PIP-180 §"Security"), mirrored verbatim by
magnetar.

In practice, the source-id chain is asserted by trusted replicator
processes (the broker-internal `Replicator` on the source side, or a
trusted aggregation pipeline). Callers should treat
`source_message_id` as an **assertion** by the writer, not a
broker-validated invariant.

### Subscribe-time shadow metadata cache

The runtime engine resolves a consumer's shadow attachment at subscribe
time via `AdminClient::get_shadow_source(topic)`. The result is cached
on the per-consumer state for the lifetime of the consumer. If a new
shadow is created on a topic after a consumer has subscribed to it (an
unusual but legal flow), the consumer will not pick up the new shadow
attachment until it is re-subscribed.

The CLI's `magnetar shadow list <source>` makes the broker-side cache
inspectable so operators can detect the race.

### Receive-path event variant

The new `ConnectionEvent::MessageReceivedFromShadow` variant is
**additive**. Callers that exhaustively match on `ConnectionEvent` need
to add the arm. By convention, magnetar treats `ConnectionEvent` as
`#[non_exhaustive]`, so the addition is non-breaking — but a stale
match arm will fail to compile.

## Replicator-role e2e setup

The producer-side replicator entry
(`Producer::send_with_source_message_id`) is exercised against a real
Apache Pulsar 4.0.4 broker by
[`crates/magnetar/tests/e2e_shadow_topic_replicator.rs`](../crates/magnetar/tests/e2e_shadow_topic_replicator.rs)
(the executable reference). The fixture is **self-hosting** — one
standalone container via `testcontainers-rs`, no external broker
dependency, no second cluster (PIP-180 is intra-cluster; cross-cluster
geo-replication is PIP-33, covered separately by
[`docs/replicated-subscriptions.md`](replicated-subscriptions.md)).

### Fixture (`start_pulsar_with_token_auth_and_replicator_role`)

1. Boot a single `apachepulsar/pulsar:4.0.4` standalone container with
   token auth turned on via `PULSAR_PREFIX_*` env overrides
   (`authenticationEnabled=true`,
   `authenticationProviders=…AuthenticationProviderToken`,
   `authorizationEnabled=true`, `superUserRoles=admin`). The HS256
   signing secret is seeded inline as
   `tokenSecretKey=data:;base64,<b64>` — no bind-mount.
2. Mint three HS256 JWTs in-process (`sub` claim = role), hand-encoded
   with `aws-lc-rs::hmac`: `admin` (super-user), `replicator`,
   `magnetar-test-user`.
3. Pre-seed the namespace by exec'ing `pulsar-admin namespaces
   grant-permission public/default --role replicator --actions
   produce,consume` inside the container (waits for exit). The
   non-replicator role is deliberately left **un-granted**.

### Broker contract pinned

The replicator-style send is gated by the broker on **two orthogonal
axes**:

| Gate | Enforced on | Failure surfaced |
| --- | --- | --- |
| **Authorisation** | producer attach | role lacks `produce` on the namespace → wire-level rejection on `producer.create()` |
| **Topic type** | `CommandSend.message_id` | target is not a registered shadow → `SendRejected { code: 22, message: "Only shadow topic supports sending messages with messageId" }` (`code 22` = `ServerError::NotAllowedError`) |

The two tests pin both:

- `e2e_v4_replicator_role_can_assert_source_message_id` — one authorised
  `replicator` producer. On a **regular** topic the source-id assertion
  is refused with `code 22`; on a **shadow** topic (created via
  in-container `pulsar-admin topics create-shadow-topic`) the same call
  is **accepted** on the wire.
- `e2e_v4_non_replicator_role_send_with_source_id_is_rejected` — a
  producer whose role has no `produce` grant is rejected at attach time,
  before the topic-type gate is even reached.

### Caveat — no receipt echo on a live shadow

The receipt-echo contract documented under
[Producer — replicator-style send](#producer--replicator-style-send)
(`receipt_id == source_id`) is a property of the **scripted** broker in
the differential harness, which deterministically reflects the asserted
id back. A **live** Pulsar 4.0.4 shadow topic does NOT: its managed
ledger is *source-backed* (`ShadowManagedLedgerImpl`), so a
client-fabricated source id pointing at no real source entry is silently
absorbed — no `CommandSendReceipt`, no consumer delivery. The e2e test
therefore pins "the send is **accepted** on a shadow but **rejected** on
a regular topic", not a receipt round-trip. Running the suite:

```sh
cargo test --features e2e -p magnetar \
    --test e2e_shadow_topic_replicator -- --include-ignored --nocapture
```

> **Admin-client wire-shape note.** The e2e creates the shadow topic via
> the broker's own `pulsar-admin` CLI, not
> `magnetar_admin::AdminClient::create_shadow_topic`, because Pulsar
> 4.0.4's `PUT .../{source}/shadowTopics` endpoint deserialises the body
> as a bare `List<String>` while the admin client sends a
> `{"shadowTopics":[…]}` object (HTTP 400). That mismatch is tracked in
> [`docs/follow-ups.md`](follow-ups.md).

## See also

- [PIP-180 (upstream)](https://github.com/apache/pulsar/blob/master/pip/pip-180.md)
- [ADR-0033 — PIP-180 shadow topic scope](../specs/adr/0033-pip-180-shadow-topic-scope.md)
- [ADR-0024 — Cross-runtime test + coverage policy](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md)
- [ADR-0004 — Sans-io protocol core](../specs/adr/0004-sans-io-protocol-core.md)
- Apache Pulsar Java —
  [`org.apache.pulsar.client.admin.Topics`](https://github.com/apache/pulsar/blob/master/pulsar-client-admin-api/src/main/java/org/apache/pulsar/client/admin/Topics.java)
