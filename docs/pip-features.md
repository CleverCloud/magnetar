# PIP feature notes

User-facing how-to for the Pulsar Improvement Proposals and auth providers magnetar supports.
For the binding scope decisions, follow the per-section ADR links; for the parity-matrix view, see [`../README.md#java-client-parity-matrix`](../README.md#java-client-parity-matrix) and the [engine-by-engine surface coverage](../README.md#engine-by-engine-surface-coverage).

## Table of contents

1. [V5 client surface (PIP-466)](#v5-client-surface-pip-466)
2. [Shadow topics (PIP-180)](#shadow-topics-pip-180)
3. [Replicated subscriptions (PIP-33)](#replicated-subscriptions-pip-33)
4. [Scalable topics (PIP-460) — experimental](#scalable-topics-pip-460--experimental)
5. [Athenz auth provider](#athenz-auth-provider)

---

## V5 client surface (PIP-466)

**Status**: experimental (gated `feature = "experimental-v5-client"`, default off).
The surface targets Pulsar 4.x brokers.
Upstream Java V5 is still iterating; magnetar's V5 surface is a thin wrapper around the v4 wire commands, with V5-shaped types (`Duration`, `Option<usize>`, `V5SubscriptionInitialPosition`) on the caller-facing builders.

Locked by [ADR-0032](../specs/adr/0032-pip-466-v5-client-surface-scope.md) (Accepted).
See [`../README.md#java-client-parity-matrix`](../README.md#java-client-parity-matrix) for the parity-matrix row.

### When to use V5

- You want the Java V5 ergonomics today (`Duration`-typed timeouts, per-surface `StreamConsumer` vs `QueueConsumer` builders, named initial-position enum) without waiting for the upstream V5 release.
- You're building greenfield code on Pulsar 4.x — the V5 builders translate to the existing v4 wire commands, so there's no broker version gate.
- You're prototyping the V5 migration for an existing v4 codebase and want to mix-and-match V5 + v4 surfaces against the same connection (the [V5 v4 escape hatch](#v5-v4-escape-hatch) returns the inner v4 client, no double-init).

### When NOT to use V5

- You need a feature V5 hasn't lifted yet — `Reader`, `TableView`, transactions.
  Stay on the v4 surface; the V5 wrapper exposes `v4()` if you need a mixed setup.
- You're shipping to production with strict surface-stability requirements.
  V5 stays default-off behind `experimental-v5-client` even though ADR-0032 is Accepted and the parity-matrix row is ✅.

### Enable the V5 feature

```toml
[dependencies]
magnetar = { features = ["experimental-v5-client"] }
```

V5 is mutually composable with every other magnetar feature (`tokio`, `moonpool`, `auth-oauth2`, `encryption`, `crypto-aws-lc-rs`, …) — it's purely an additive surface.

### V5 quick start

```rust
use std::time::Duration;
use magnetar::v5::{PulsarClientV5, mapping::V5SubscriptionInitialPosition};
use magnetar::PulsarClient;

// 1. Build a v4 client as usual.
let v4 = PulsarClient::builder()
    .service_url("pulsar://broker:6650")
    .build()
    .await?;

// 2. Wrap it in the V5 surface.
let client = PulsarClientV5::from_v4(v4);

// 3. Producer with V5 ergonomics.
let producer = client
    .producer("persistent://public/default/topic")
    .send_timeout(Duration::from_secs(30))
    .max_pending_messages(Some(1000))
    .create()
    .await?;

// 4. Stream consumer (Exclusive default; .failover() for Failover).
let stream = client
    .stream_consumer("persistent://public/default/topic")
    .subscription("my-sub")
    .negative_ack_redelivery_delay(Duration::from_secs(60))
    .ack_timeout(Some(Duration::from_secs(30)))
    .subscribe()
    .await?;

// 5. Queue consumer (Shared default; .key_shared() for KeyShared).
let queue = client
    .queue_consumer("persistent://public/default/queue")
    .subscription("my-queue-sub")
    .key_shared()
    .subscribe()
    .await?;
```

### V5 → v4 mapping table

The V5 builders accept `Duration` / `Option<usize>` / `Option<Duration>` / `V5SubscriptionInitialPosition`, then translate to the v4 wire fields via the centralised [`v5::mapping`](../crates/magnetar/src/v5/mapping.rs) module.
The defaults match Java V5 (`org.apache.pulsar.client.api.v5`).

| V5 builder field                | V5 type                         | V5 default   | v4 wire field                            | v4 type | Translation function                                                             |
| ------------------------------- | ------------------------------- | ------------ | ---------------------------------------- | ------- | -------------------------------------------------------------------------------- |
| `send_timeout`                  | `Duration`                      | `30 s`       | `send_timeout` (millis)                  | `u64`   | [`send_timeout_to_ms`](../crates/magnetar/src/v5/mapping.rs)                     |
| `max_pending_messages`          | `Option<usize>`                 | `Some(1000)` | `max_pending_messages` (`0` = unlimited) | `usize` | [`max_pending_messages_to_v4`](../crates/magnetar/src/v5/mapping.rs)             |
| `ack_timeout`                   | `Option<Duration>`              | `None`       | `ack_timeout_ms` (`0` = disabled)        | `u64`   | [`ack_timeout_to_ms`](../crates/magnetar/src/v5/mapping.rs)                      |
| `negative_ack_redelivery_delay` | `Duration`                      | `60 s`       | `negative_ack_redelivery_delay_ms`       | `u64`   | [`negative_ack_redelivery_delay_to_ms`](../crates/magnetar/src/v5/mapping.rs)    |
| `receiver_queue_size`           | `usize`                         | `1000`       | `receiver_queue_size`                    | `usize` | _(direct)_                                                                       |
| `subscription_initial_position` | `V5SubscriptionInitialPosition` | `Latest`     | `pb::command_subscribe::InitialPosition` | enum    | [`V5SubscriptionInitialPosition::into_pb`](../crates/magnetar/src/v5/mapping.rs) |

#### V5 edge cases worth knowing

- **`ack_timeout = None` vs `Some(Duration::ZERO)`** — both translate to wire `0` (the v4 "disabled" sentinel).
  The V5 type distinguishes them, but the v4 wire collapses both.
  Pinned by `v5_builder_defaults::v5_translation_edge_cases`.
- **`max_pending_messages = None` vs `Some(0)`** — both translate to wire `0` (the v4 "unlimited" sentinel).
  Same pin.
- **`send_timeout` saturation** — pathological `Duration` values beyond `u64::MAX` millis clamp at `u64::MAX` rather than panic.
  The most-permissive interpretation.
  Pinned by the same test.

### V5 subscription types

| V5 builder                                  | v4 `SubType` | Notes                                                                                  |
| ------------------------------------------- | ------------ | -------------------------------------------------------------------------------------- |
| `client.stream_consumer(topic)` (default)   | `Exclusive`  | Single active consumer per partition; ordered delivery.                                |
| `client.stream_consumer(topic).failover()`  | `Failover`   | One active consumer per partition with automatic failover to backups.                  |
| `client.queue_consumer(topic)` (default)    | `Shared`     | Work-distribution across multiple active consumers per partition; no per-key ordering. |
| `client.queue_consumer(topic).key_shared()` | `KeyShared`  | Per-key ordering across a set of active consumers. Attaches default `KeySharedMeta`.   |

### V5 v4 escape hatch

`PulsarClientV5` holds no parallel state — it wraps the underlying v4 `PulsarClient` directly.
`v4()` borrows the inner client; `into_v4()` consumes the wrapper.

```rust
let v5 = PulsarClientV5::from_v4(v4_client);

// Mix surfaces on the same engine state:
let v4_reader = v5.v4().reader(topic).start_message_id(start).create().await?;
let v5_producer = v5.producer(topic).create().await?;

// Or migrate back wholesale:
let back_to_v4: PulsarClient = v5.into_v4();
```

ADR-0032 pins this contract via the `v5_client_v4_escape_hatch::v5_wrapper_is_zero_sized_over_v4_client` test — `mem::size_of::<PulsarClientV5>` must equal `mem::size_of::<PulsarClient>`.
A future refactor that added parallel state would fail that assertion.

### V5 test layers

The V5 mapping translations are covered by:

| Layer         | File                                                                                                            |
| ------------- | --------------------------------------------------------------------------------------------------------------- |
| Unit          | [`crates/magnetar/src/v5/mapping.rs::tests`](../crates/magnetar/src/v5/mapping.rs)                              |
| Producer wire | [`crates/magnetar/tests/v5_producer_mapping.rs`](../crates/magnetar/tests/v5_producer_mapping.rs)               |
| Stream wire   | [`crates/magnetar/tests/v5_stream_consumer_mapping.rs`](../crates/magnetar/tests/v5_stream_consumer_mapping.rs) |
| Queue wire    | [`crates/magnetar/tests/v5_queue_consumer_mapping.rs`](../crates/magnetar/tests/v5_queue_consumer_mapping.rs)   |
| Escape hatch  | [`crates/magnetar/tests/v5_client_v4_escape_hatch.rs`](../crates/magnetar/tests/v5_client_v4_escape_hatch.rs)   |
| Defaults      | [`crates/magnetar/tests/v5_builder_defaults.rs`](../crates/magnetar/tests/v5_builder_defaults.rs)               |

The wire-byte tests use [`magnetar_fakes::FrameRecorder`](../crates/magnetar-fakes/src/lib.rs) to drain a sans-io `Connection` and decode the resulting frames; they assert that V5 builder calls translate to the expected v4 `CommandProducer` / `CommandSubscribe` field values on the wire.

### V5 status note

The parity-matrix row sits at ✅ since ADR-0032 was Accepted alongside the engine-generic refactor.
The `experimental-v5-client` feature stays default-off; acceptance reflects the matrix state and unlocks moonpool-engine V5 usage, not the default-on flag.
What that acceptance covers:

- **Engine-generic surface.** `PulsarClientV5<E: Engine = TokioEngine>` is parametric, with the same v4 escape hatch and per-surface builders on either engine.
  `MessageEncryptor` / `MessageDecryptor` types live behind the per-engine [`MessageEncryptorApi`] / [`MessageDecryptorApi`] extension traits (tokio plugs in `Arc<dyn magnetar_runtime_tokio::MessageEncryptor>`; moonpool plugs in a no-op stub).
  `MessageRouter` is a façade-level trait (pure routing math), engine-agnostic by construction.
- **Test coverage.** The five mapping/wire test files have moonpool 1:1 mirrors at `crates/magnetar/tests/v5_*_moonpool.rs` (engine-shape pinning + sans-io wire assertions against `MoonpoolEngine<TokioProviders>`); the V5 surface has full deterministic-simulation coverage symmetric with the v4 surface.
  Three e2e tests (`crates/magnetar/tests/e2e_pulsar_v5.rs` + `e2e_sub_types_v5.rs`) gated `feature = "e2e,experimental-v5-client"` cover Pulsar 4.x compatibility.
  `check-crypto-matrix` × V5 axis is green.
- **Per-surface builder lifts.** `PartitionedProducerBuilder<E>`, `TableViewBuilder<E>`, `TypedTableViewBuilder<S, E>` are engine-generic.
  The tokio-specialised `.create_with_encryption` / `.create_with_decryption` impls retain the PIP-4 carve-out.

[`MessageEncryptorApi`]: ../crates/magnetar/src/engine/mod.rs
[`MessageDecryptorApi`]: ../crates/magnetar/src/engine/mod.rs

### V5 references

- [PIP-466 proposal](../specs/proposals/pip-466-v5-client-surface.md)
- [ADR-0032 — V5 client surface scope](../specs/adr/0032-pip-466-v5-client-surface-scope.md)
- [Apache Pulsar V5 client (Java)](https://github.com/apache/pulsar-client-reactive) — upstream design source

---

## Shadow topics (PIP-180)

Status: ✅ supported — scope locked in [ADR-0033](../specs/adr/0033-pip-180-shadow-topic-scope.md).

PIP-180 introduces a read-only topic ownership model that shares the underlying BookKeeper ledgers of a **source topic** — a lightweight fan-out alternative to geo-replication, targeting up to ~100K shadow-side subscribers without re-paying the storage cost.

This section documents the magnetar surface for shadow topics: the admin REST methods, the producer-side replicator entry, the consumer-side classification contract, and the caveats inherited from upstream.

### When to use shadow topics

A shadow topic is the right tool when you need:

- **High-fanout read paths** sharing one source-of-truth ledger (broadcast / dashboards / cache-fill).
- **Lightweight geo-style read replicas** within a single cluster (PIP-180 is intra-cluster — for cross-cluster, use geo-replication).
- **Stable consumer groups** that should observe the **same** `MessageId` as the source-side reader (e.g. cross-side deduplication, cross-side correlation logs).

It is **not** a substitute for:

- Cross-cluster replication (use geo-replication).
- Independent storage / retention on the shadow side (the shadow shares the source's ledger).

### Shadow topics surface

#### `magnetar-admin::AdminClient` — three new methods

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

Endpoint paths mirror the upstream broker (`pulsar-broker/.../v2/PersistentTopics.java`):

| Method                | Verb   | Path                                                               |
| --------------------- | ------ | ------------------------------------------------------------------ |
| `create_shadow_topic` | PUT    | `/admin/v2/persistent/{tenant}/{namespace}/{source}/shadowTopics`  |
| `delete_shadow_topic` | DELETE | `/admin/v2/persistent/{tenant}/{namespace}/{shadow}?force={force}` |
| `get_shadow_topics`   | GET    | `/admin/v2/persistent/{tenant}/{namespace}/{source}/shadowTopics`  |
| `get_shadow_source`   | GET    | `/admin/v2/persistent/{tenant}/{namespace}/{shadow}/shadowSource`  |

Errors reuse the existing `AdminError` taxonomy — `404` → `Status { code: 404, .. }` (topic not found), `409` → `Status { code: 409, .. }` (shadow already exists), `401`/`403` → `Status { code: 401|403, .. }` (auth).

#### Producer — replicator-style send

`magnetar_runtime_tokio::Producer::send_with_source_message_id` (mirror on the moonpool side) propagates a source-topic `MessageId` on the wire via `CommandSend.message_id`.
The broker echoes the asserted id back on the resulting `CommandSendReceipt`, so the returned `SendFut` resolves to a `MessageId` structurally equal to the asserted one.

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

This entry **bypasses batching** by design — mirrors Java's `org.apache.pulsar.broker.service.persistent.Replicator`, which writes each entry one at a time.
Chunking still applies for payloads larger than `max_message_size`; in that case the same `source_msg_id` is stamped on every chunk (one logical message, multiple frames).

The regular `Producer::send(...)` continues to emit `CommandSend.message_id = None` — byte-identical on the wire (no proto bump).
The new field on `OutgoingMessage` defaults to `None` so callers that don't use the replicator entry see no change.

#### Consumer — shadow-side classification

A shadow-attached `Consumer` emits a distinct event variant on every shadow-presented message so callers can observe the source-topic context without an out-of-band lookup:

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

#### Shadow `MessageId` equality contract

PIP-180 promotes the existing structural-equality on `MessageId` to a **documented contract**:

> Two `MessageId`s compare equal iff they share `(ledger_id, entry_id, partition, batch_index, batch_size)`.
> On a shadow topic, the broker presents messages with the **source** `MessageId` (same ledger/entry pointers as the original write).
> The shadow-side reader observes ids that compare equal to the source-side reader's ids — "same message" is structurally evident.

This means cross-side deduplication needs no out-of-band correlation key: a `HashSet<MessageId>` populated by a source-side reader will collide with shadow-side ids on the same physical entry.

See the pinned unit test in [`crates/magnetar-proto/src/types.rs`](../crates/magnetar-proto/src/types.rs) (`message_id_equality_shadow_vs_source`) for the contract guard.

### Shadow topics CLI

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

### Shadow topic caveats

#### Client-asserted source `MessageId`

`Producer::send_with_source_message_id` lets the client assert any `MessageId` it wants on the outbound `CommandSend.message_id`.
The broker validates that the producer is authorised to write to the shadow topic, but does **not** cryptographically prove that the source-message-id matches a real entry on the source topic.
This is upstream's behaviour (see PIP-180 §"Security"), mirrored verbatim by magnetar.

In practice, the source-id chain is asserted by trusted replicator processes (the broker-internal `Replicator` on the source side, or a trusted aggregation pipeline).
Callers should treat `source_message_id` as an **assertion** by the writer, not a broker-validated invariant.

#### Subscribe-time shadow metadata cache

The runtime engine resolves a consumer's shadow attachment at subscribe time via `AdminClient::get_shadow_source(topic)`.
The result is cached on the per-consumer state for the lifetime of the consumer.
If a new shadow is created on a topic after a consumer has subscribed to it (an unusual but legal flow), the consumer will not pick up the new shadow attachment until it is re-subscribed.

The CLI's `magnetar shadow list <source>` makes the broker-side cache inspectable so operators can detect the race.

#### Receive-path event variant

The new `ConnectionEvent::MessageReceivedFromShadow` variant is **additive**. Callers that exhaustively match on `ConnectionEvent` need to add the arm.
By convention, magnetar treats `ConnectionEvent` as `#[non_exhaustive]`, so the addition is non-breaking — but a stale match arm will fail to compile.

### Shadow replicator-role e2e setup

The producer-side replicator entry (`Producer::send_with_source_message_id`) is exercised against a real Apache Pulsar 4.0.4 broker by [`crates/magnetar/tests/e2e_shadow_topic_replicator.rs`](../crates/magnetar/tests/e2e_shadow_topic_replicator.rs) (the executable reference).
The fixture is **self-hosting** — one standalone container via `testcontainers-rs`, no external broker dependency, no second cluster (PIP-180 is intra-cluster; cross-cluster geo-replication is PIP-33, covered by [Replicated subscriptions (PIP-33)](#replicated-subscriptions-pip-33)).

#### Shadow fixture (`start_pulsar_with_token_auth_and_replicator_role`)

1. Boot a single `apachepulsar/pulsar:4.0.4` standalone container with token auth turned on via `PULSAR_PREFIX_*` env overrides (`authenticationEnabled=true`, `authenticationProviders=…AuthenticationProviderToken`, `authorizationEnabled=true`, `superUserRoles=admin`).
   The HS256 signing secret is seeded inline as `tokenSecretKey=data:;base64,<b64>` — no bind-mount.
2. Mint three HS256 JWTs in-process (`sub` claim = role), hand-encoded with `aws-lc-rs::hmac`: `admin` (super-user), `replicator`, `magnetar-test-user`.
3. Pre-seed the namespace by exec'ing `pulsar-admin namespaces grant-permission public/default --role replicator --actions produce,consume` inside the container (waits for exit).
   The non-replicator role is deliberately left **un-granted**.

#### Shadow broker contract pinned

The replicator-style send is gated by the broker on **two orthogonal axes**:

| Gate              | Enforced on              | Failure surfaced                                                                                                                                                                    |
| ----------------- | ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Authorisation** | producer attach          | role lacks `produce` on the namespace → wire-level rejection on `producer.create()`                                                                                                 |
| **Topic type**    | `CommandSend.message_id` | target is not a registered shadow → `SendRejected { code: 22, message: "Only shadow topic supports sending messages with messageId" }` (`code 22` = `ServerError::NotAllowedError`) |

The two tests pin both:

- `e2e_v4_replicator_role_can_assert_source_message_id` — one authorised `replicator` producer.
  On a **regular** topic the source-id assertion is refused with `code 22`; on a **shadow** topic (created via in-container `pulsar-admin topics create-shadow-topic`) the same call is **accepted** on the wire.
- `e2e_v4_non_replicator_role_send_with_source_id_is_rejected` — a producer whose role has no `produce` grant is rejected at attach time, before the topic-type gate is even reached.

#### Shadow caveat — no receipt echo on a live shadow

The receipt-echo contract documented under [Producer — replicator-style send](#producer--replicator-style-send) (`receipt_id == source_id`) is a property of the **scripted** broker in the differential harness, which deterministically reflects the asserted id back.
A **live** Pulsar 4.0.4 shadow topic does NOT: its managed ledger is _source-backed_ (`ShadowManagedLedgerImpl`), so a client-fabricated source id pointing at no real source entry is silently absorbed — no `CommandSendReceipt`, no consumer delivery.
The e2e test therefore pins "the send is **accepted** on a shadow but **rejected** on a regular topic", not a receipt round-trip.
Running the suite:

```sh
cargo test -p magnetar \
    --test e2e_shadow_topic_replicator -- --include-ignored --nocapture
```

> **Admin-client wire-shape note.** The e2e creates the shadow topic via the broker's own `pulsar-admin` CLI, not `magnetar_admin::AdminClient::create_shadow_topic`, because Pulsar 4.0.4's `PUT .../{source}/shadowTopics` endpoint deserialises the body as a bare `List<String>` while the admin client sends a `{"shadowTopics":[…]}` object (HTTP 400).
> That mismatch is tracked in [`follow-ups.md`](follow-ups.md).

### Shadow topics references

- [PIP-180 (upstream)](https://github.com/apache/pulsar/blob/master/pip/pip-180.md)
- [ADR-0033 — PIP-180 shadow topic scope](../specs/adr/0033-pip-180-shadow-topic-scope.md)
- [ADR-0024 — Cross-runtime test + coverage policy](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md)
- [ADR-0004 — Sans-io protocol core](../specs/adr/0004-sans-io-protocol-core.md)
- Apache Pulsar Java — [`org.apache.pulsar.client.admin.Topics`](https://github.com/apache/pulsar/blob/master/pulsar-client-admin-api/src/main/java/org/apache/pulsar/client/admin/Topics.java)

---

## Replicated subscriptions (PIP-33)

**Scope**: [ADR-0034](../specs/adr/0034-pip-33-replicated-subscriptions-scope.md) · **Upstream**: [PIP-33 (Apache Pulsar wiki)](https://github.com/apache/pulsar/wiki/PIP-33%3A-Replicated-subscriptions)

PIP-33 keeps a subscription's cursor position in sync across geo-replicated Pulsar clusters at sub-second granularity.
A consumer that fails over from cluster A to cluster B resumes near its previous position (up to ~1s of duplicate messages, by design).

The mechanism is entirely **broker-driven**: the broker periodically injects `REPLICATED_SUBSCRIPTION_*` markers into the topic's data stream and propagates cursor positions across clusters via geo-replication.
The client's job is small: (1) flip one builder flag so the broker enables the machinery, and (2) filter the markers off the user-visible message stream so they never surface as application payload.

### Replicated subscriptions quick reference

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

### Replicated subscriptions broker prerequisites

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

### What the client does (PIP-33)

1. **Encoder**. With `replicate_subscription_state(true)`, the encoder sets `CommandSubscribe.replicate_subscription_state = true` ([wire field 14](../crates/magnetar-proto/proto/PulsarApi.proto)).
   The default (`None`) leaves the wire bytes unchanged.

2. **Receive-path filter**. When a frame arrives with `MessageMetadata.marker_type ∈ {10, 11, 12, 13}` — `REPLICATED_SUBSCRIPTION_SNAPSHOT_REQUEST`, `…RESPONSE`, `…SNAPSHOT`, or `…UPDATE` — `magnetar_proto::Connection` decodes it via [`magnetar_proto::markers::decode_replicated_subscription_marker`](../crates/magnetar-proto/src/markers.rs), drops it from the user-visible event stream, and emits a `ConnectionEvent::ReplicatedSubscriptionMarkerObserved` event instead.
   The consumer's flow-control counter is bumped so the broker can still send the next batch of permits-worth of messages.

3. **Observation buffer (optional)**. The driver pushes every observation into a per-client buffer.
   Advanced callers read it via `PulsarClient::poll_replicated_subscription_marker` or `PulsarClient::next_replicated_subscription_marker` — useful for metrics, regression tests, and operational dashboards.
   Most applications can ignore this surface entirely.

### What the client deliberately does **not** do (PIP-33)

- **Origination.** Magnetar never emits `REPLICATED_SUBSCRIPTION_*` markers; snapshot generation is broker-side.
  The proto's `Producer` surface has no hook to construct one and won't grow one.
- **Broker-side replication state.** Magnetar does not implement cross-cluster cursor synchronisation.
  `replicate_subscription_state(true)` only flips the wire flag; correctness depends entirely on the broker's geo-replication setup.

These are the two explicit non-goals locked in [ADR-0034](../specs/adr/0034-pip-33-replicated-subscriptions-scope.md).

### Replicated subscriptions failure modes / caveats

| Symptom                                                | Cause                                                                                                                          | Fix                                                                                         |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- |
| Flag has no observable effect                          | Single-cluster broker, or namespace lacks `replicated_subscription_status=true`                                                | Run all four `pulsar-admin` steps above.                                                    |
| Consumer sees garbage payload that looks like protobuf | Bug — please file. The receive-path filter is supposed to catch every kind 10–13 marker.                                       | Capture the wire bytes via `tcpdump` or a recording broker and open an issue with the dump. |
| Cursor-resume tolerance > 1s on failover               | Broker's `replicatedSubscriptionsSnapshotFrequencyMillis` is higher than expected, or geo-replication lag is significant       | Pin the snapshot frequency lower; check geo-replication health on both clusters.            |
| Markers observed but cursor doesn't sync               | Namespace not flagged with `replicated-subscription-status=true` (separate from `replicateSubscriptionState` on the subscribe) | `bin/pulsar-admin namespaces set-replicated-subscription-status … --enable`.                |

### Replicated subscriptions testing

- **Unit / proto**: 11 tests in `crates/magnetar-proto/src/markers.rs` + `…/src/conn.rs` cover the decoder, the filter, and the `CommandSubscribe` wire field.
  Run via `cargo test -p magnetar-proto`.
- **Runtime parity (ADR-0024)**: 5 tokio + 5 moonpool integration tests under `crates/magnetar-runtime-{tokio,moonpool}/tests/replicated_subscriptions.rs` with identical names — verified by `cargo run -p xtask -- check-runtime-test-parity`.
- **Differential**: 2 equivalence tests at `crates/magnetar-differential/tests/replicated_subscriptions_equivalence.rs` assert tokio ↔ moonpool produce the same `EventStream` + byte-identical `CommandSubscribe`.
- **End-to-end**: 2 tests at `crates/magnetar/tests/e2e_replicated_subscriptions.rs` against the two-cluster Docker fixture.
  Runs as a regular `cargo test` per ADR-0046; CI runs them **weekly only** in [`.github/workflows/e2e-replicated-subs.yml`](../.github/workflows/e2e-replicated-subs.yml) per the ADR-0036 cost-shifting precedent.

### Replicated subscriptions references

- [ADR-0034](../specs/adr/0034-pip-33-replicated-subscriptions-scope.md) — scope and non-goals.
- [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md) — cross-runtime test + coverage policy.
- [ADR-0036](../specs/adr/0036-moonpool-seed-sweep-daily-random.md) — weekly-workflow precedent for cost-shifting heavy fixtures.
- [`crates/magnetar-proto/src/markers.rs`](../crates/magnetar-proto/src/markers.rs) — decoder + types.
- Apache Pulsar Java — `org.apache.pulsar.client.impl.ConsumerBuilderImpl#replicateSubscriptionState`.

---

## Scalable topics (PIP-460) — experimental

> **⚠️ EXPERIMENTAL — scaffold only.** This surface lives behind the default-off `scalable-topics` feature.
> Upstream [PIP-460](https://github.com/apache/pulsar/blob/master/pip/pip-460.md) is **`Draft`** and **no released Apache Pulsar broker speaks the scalable-topic wire protocol today** (it targets Pulsar 5.0 LTS, ~Oct 2026, with a phased rollout).
> Magnetar provides the **client-side scaffold** — the wire commands, the segment-DAG state machine, the `StreamConsumer` surface, and the four-layer in-process test coverage — so the surface is ready the day a broker ships it.
> End-to-end against a live broker is **blocked until upstream cuts a Pulsar 5.0 RC**. See [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md).

### What PIP-460 is

PIP-460 introduces a third topic shape alongside non-partitioned and partitioned topics: a **scalable topic**, addressed by a new `topic://...` URL scheme.
A scalable topic is backed by a **segment DAG** — a set of hash-key-ranged segments that the broker can **split** (one segment fans out into children) and **merge** (children fold back) at runtime, each served by its own segment-leader broker and coordinated by an elected **controller broker**. Clients open a **DAG-watch session** against the controller broker to observe the live segment layout.

### What the scaffold provides

A bounded, experimental **StreamConsumer** surface with **drop-on-DAG-change** semantics:

- **`topic://...` URL scheme** recognition (`is_scalable_topic_url`), routed to the scalable lookup path.
  The `persistent://` / `non-persistent://` paths are untouched.
- **Three new wire commands** (hand-encoded behind the feature, see below): `CommandScalableTopicLookup` + response, `CommandSegmentDagWatch` + response, `CommandSegmentDagUpdate`, plus `CommandCloseSegmentDagWatch`.
- **`SegmentDescriptor` / `SegmentId` / `KeyRange` / `SegmentState`** types and an additive, default-`None` `MessageId::segment_id` field (the wire layout stays byte-identical when `None` — pre-existing producers / consumers round-trip bit-for-bit).
- **`DagWatchSession`** — a sans-io state machine that tracks the current DAG, enforces a **monotonic `update_seq`**, and applies add / remove / split / merge deltas.
- **`scalable::StreamConsumer<T, E>`** on the façade, generic over the engine via the `ScalableTopicsApi` extension trait (`where E::ClientState: ScalableTopicsApi`), available on **both** the tokio and moonpool engines.
- A **`magnetar topic-info topic://...`** CLI subcommand that prints the current segment DAG.

### Scalable topics drop-on-DAG-change semantics

The current behaviour is **observation + drop-on-change**, not transparent failover.
When the controller broker pushes a segment **split**, **merge**, or **removal** while a `StreamConsumer` is active:

1. The proto `DagWatchSession` applies the delta and emits `SegmentDagUpdated { delta }`.
2. Because the delta is _consume-affecting_, the connection also emits `DagChangedDuringConsume { reason }`.
3. The runtime drains those into the per-client scalable-event buffer; the façade `StreamConsumer::next_event` surfaces `ConsumerEvent::DagChanged { reason }` and flips `is_dropped()`.
4. The caller **re-resolves** (`scalable_stream_consumer(...)` again) and re-subscribes to continue.

A pure-**add** update (a fresh segment with no split / merge / removal) is _benign_ — it refreshes the DAG snapshot and surfaces `ConsumerEvent::DagUpdated` without dropping.

If the controller-broker connection closes, the surface emits `ConsumerEvent::Closed { reason }` and lets the caller decide — there is **no automatic re-lookup** (controller-election awareness is out of scope for the current scaffold).

### Scalable topics out of scope

`QueueConsumer`, `CheckpointConsumer`, controller-election awareness, transparent segment failover during consume, in-place key-range repartition, and segment-aware sticky-key dispatch (Key_Shared across the full DAG) are all explicit follow-ups for when the broker side stabilises.
The current `KeyRange` is **observation-only**.

### Scalable topics hand-encoded wire commands

Because no released broker speaks PIP-460 and the upstream field numbers are still provisional, magnetar does **not** vendor the commands into the generated `crates/magnetar-proto/src/pb/pulsar.proto.rs`.
Instead they live in a hand-maintained, feature-gated module (`crates/magnetar-proto/src/pb/scalable_topics.rs`) as `#[derive(prost::Message)]` structs that ride the standard Pulsar command frame via a hand-built `ScalableBaseCommand` envelope (sharing the `type` field-1 tag, so a pre-PIP-460 peer skips the additive 80-85 fields).
The **authoritative** proto bump lands when upstream tags a Pulsar 5.0 RC — at that point a dedicated `cargo run -p xtask -- vendor-proto --rev <sha>` commit ([ADR-0026 §D4](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)) replaces the hand-encoded module and reconciles the field numbers.

### Scalable topics feature flag

`scalable-topics` on the `magnetar` crate, **default off**. Compiling without it leaves the non-scalable surface bit-for-bit unchanged on the wire (proved by the `scalable_topics_feature_off_does_not_export` test on both runtime engines).
The CLI picks it up via `--features magnetar-cli/scalable-topics`.

### Scalable topics example (against a future Pulsar 5.0 broker)

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

### Scalable topics references

- [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md) — scope.
- [Proposal](../specs/proposals/pip-460-scalable-topics.md) — full wire delta + test plan.
- [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md) — the four-layer test plan.
- Upstream [PIP-460](https://github.com/apache/pulsar/blob/master/pip/pip-460.md).

---

## Athenz auth provider

The [`magnetar-auth-athenz`](../crates/magnetar-auth-athenz/) crate ships the client side of Apache Pulsar's Athenz authentication method: the tenant signs an N-token / OAuth2 `client_assertion` JWT with its RSA private key, exchanges it at the Athenz ZTS endpoint for a role token, and presents the role-token bytes as the Pulsar CONNECT `auth_data` payload.

This section covers the **client-side configuration matrix** — which backend signs the JWT, how to wire it from the [`magnetar`](../crates/magnetar/) façade, and what the deterministic-signature guarantee buys callers that run the moonpool simulation engine.
The as-built design — the testability seams (`ensure_role_token`, injected `wall_clock`, the pluggable `ZtsClient` trait) and the aws-lc-rs / ring RS256 JWT exchange — is locked in [ADR-0041](../specs/adr/0041-athenz-provider-testability-seams.md), which supersedes the originally-proposed [ADR-0030](../specs/adr/0030-athenz-zts-round-trip-scope.md); the cross-workspace crypto-provider selection is locked in [ADR-0035](../specs/adr/0035-pluggable-crypto-provider.md).

### Athenz surface at a glance

```text
AthenzProvider::with_role_token(config, role_token)   ← out-of-band sidecar (pinned token)
AthenzProvider::with_default_signer(config)            ← in-tree backend + HttpZtsClient
AthenzProvider::builder()                              ← custom signer / ZtsClient / wall_clock
    .config(config).signer(signer).zts_client(client).build()
```

The refresh + cache state machine lives on the provider: `ensure_role_token(now: Instant)` performs a ZTS exchange when the cache is missing or within `refresh_margin` of expiry; `needs_refresh(now)` queries that decision; `AuthProvider::initial()` returns the cached role-token bytes (or `AuthError::Unsupported` before the first fetch).

- `with_role_token` skips the JWT signer entirely — useful when a sidecar (`zts-agent`, custom mint service) already holds the role token; the pinned token never expires and `ensure_role_token` is a no-op.
- `with_default_signer` wires the cfg-active in-tree signer to a production [`zts::HttpZtsClient`].
- `builder()` is the general path: supply a custom [`zts::JwtSigner`] (HSM, `jsonwebtoken`, …), a custom [`zts::ZtsClient`] (the deterministic-simulation tests inject a scripted fake here), and an injected `wall_clock` for reproducible JWT `iat` / `exp`.

### Athenz crypto-provider matching

The two concrete signer backends are gated on the same feature flags that select the rustls crypto provider (ADR-0035).
The mapping is deliberately 1:1 so a single feature flip switches every consumer (rustls + Athenz signer + PIP-4 message encryption) at once and the workspace stays internally consistent.

| Workspace feature            | rustls provider                          | Athenz signer            | PIP-4 message crypto |
| ---------------------------- | ---------------------------------------- | ------------------------ | -------------------- |
| `crypto-aws-lc-rs` (default) | aws-lc-rs (with post-quantum hybrid KEX) | [`AwsLcRsSigner`]        | aws-lc-rs (always)   |
| `crypto-ring`                | ring                                     | [`RingSigner`]           | aws-lc-rs (always)   |
| `crypto-openssl`             | rustls-openssl                           | _none_ (use [`builder`]) | aws-lc-rs (always)   |
| `crypto-fips`                | aws-lc-rs FIPS                           | _none_ (use [`builder`]) | aws-lc-rs (always)   |

`crypto-openssl` and `crypto-fips` do not currently ship an Athenz signer because:

- `crypto-openssl` carves OpenSSL into the graph **only** as a transitive dep of `rustls-openssl` (ADR-0035 §4 `deny.toml` `wrappers = [...]` carve-out).
  Adding an `openssl`-backed signer would re-open the ban; callers wanting it should use [`builder`] with their own `openssl::sign` implementation.
- `crypto-fips` already pulls aws-lc-rs (FIPS module).
  FIPS callers who also want the in-tree signer should enable `crypto-aws-lc-rs` + `crypto-fips` simultaneously; the cfg cascade picks the FIPS-validated aws-lc-rs provider for rustls and the same library backs the signer (FIPS-validated RSA sign path).

When both `crypto-aws-lc-rs` and `crypto-ring` are enabled (e.g. `--all-features`) the cfg cascade in [`crates/magnetar-auth-athenz/src/jwt_signer/mod.rs`](../crates/magnetar-auth-athenz/src/jwt_signer/mod.rs) picks aws-lc-rs first, matching the ADR-0035 priority `aws-lc-rs > fips > openssl > ring`.
The ring path stays compiled and publicly callable via [`RingSigner`] in case a downstream consumer wants to instantiate it explicitly.

[`AwsLcRsSigner`]: ../crates/magnetar-auth-athenz/src/jwt_signer/aws_lc_rs.rs
[`RingSigner`]: ../crates/magnetar-auth-athenz/src/jwt_signer/ring.rs
[`builder`]: ../crates/magnetar-auth-athenz/src/lib.rs

### Athenz usage

#### From the façade with the default backend

```rust
use magnetar_auth_athenz::{AthenzConfig, AthenzProvider};

let config = AthenzConfig {
    tenant_domain:    "mydomain".to_owned(),
    tenant_service:   "myservice".to_owned(),
    provider_domain:  "pulsar.tenant".to_owned(),
    key_id:           "key0".to_owned(),
    private_key_pem:  std::fs::read_to_string("tenant.pkcs8.pem")?,
    zts_url:          "https://zts.example.com:4443/zts/v1/".to_owned(),
    principal_header: None,
    role_header:      None,
};
let provider = AthenzProvider::with_default_signer(config)?;
// pump the cache before the connection's first use; `now` is the
// engine-snapshotted monotonic instant (sans-io clock injection).
provider.ensure_role_token(std::time::Instant::now()).await?;
```

Requires `magnetar-auth-athenz` to be built with both `crypto-aws-lc-rs` (or `crypto-ring`) **and** `zts`.
The façade's `auth-athenz-zts` feature propagates `zts`; the workspace's `crypto-*` features propagate the matching backend.

#### Athenz with a caller-supplied signer

```rust
use std::sync::Arc;
use magnetar_auth_athenz::{AthenzConfig, AthenzProvider, zts::{HttpZtsClient, JwtSigner, ZtsGrant}};

#[derive(Debug)]
struct HsmSigner { /* ... */ }
impl JwtSigner for HsmSigner { /* ... */ }

let signer: Arc<dyn JwtSigner> = Arc::new(HsmSigner { /* ... */ });
let client = Arc::new(HttpZtsClient::new(&config.zts_url, ZtsGrant::default())?);
let provider = AthenzProvider::builder()
    .config(config)
    .signer(signer)
    .zts_client(client)
    .build()?;
```

The `ZtsClient` trait is the HTTPS seam: production wires [`zts::HttpZtsClient`], while the moonpool / differential test layers inject a scripted fake so the refresh + cache mechanics are exercised without an HTTP endpoint (ADR-0030 §moonpool, ADR-0041).

### Athenz ADR-0030 close-out: zeroization

Both backends wrap the parsed PKCS#8 DER bytes in [`zeroize::Zeroizing`] so the secret material is wiped from memory when the signer drops.
The aws-lc-rs / ring `RsaKeyPair` types themselves are opaque wrappers around C-allocated `EVP_PKEY` / BIGNUM structures and cannot be made `Zeroize`-friendly from Rust.
The implementation therefore stores the **DER bytes** under `Zeroizing<Vec<u8>>` and reconstructs the keypair on each sign.
The trade-off:

- **Cost.** One PKCS#8 ASN.1 parse + RSA structure rebuild per sign call.
  Negligible alongside the 2048-bit modular exponentiation that the signature itself drives.
- **Benefit.** A hard guarantee that the parsed private key does not linger in memory after the signer drops, closing the deferral recorded in [ADR-0030 §Security implications (a)](../specs/adr/0030-athenz-zts-round-trip-scope.md).

The `AthenzConfig::private_key_pem` field itself is **not** zeroized — the PEM string is owned by the caller's configuration scope and is expected to be redacted via the `Debug` impl (`<redacted>` sentinel) rather than wiped on drop.
Callers handling rotating secrets should zero their own PEM after constructing the signer.

### Athenz deterministic signatures

RSASSA-PKCS1-v1_5 with SHA-256 is deterministic per RFC 8017 §8.2 — the same key + payload produces byte-identical signature bytes across calls and across libraries.
This buys two properties:

1. **moonpool reproducibility.** With `wall_clock` frozen at the call site (sans-io clock injection per [ADR-0011](../specs/adr/0011-clock-injection-sans-io.md)) the entire JWT emission is bit-for-bit deterministic.
   The same `(seed, commit)` pair always produces the same network bytes — load-bearing for the [moonpool-engine](moonpool-engine.md) chaos pack.
2. **Cross-backend equivalence.** aws-lc-rs and ring must produce identical signature bytes for the same key + payload.
   Pinned by the [`magnetar_auth_athenz::jwt_signer::ring::tests::cross_backend_signature_byte_identity`](../crates/magnetar-auth-athenz/src/jwt_signer/ring.rs) test (gated on both features enabled).
   If this assertion ever fails, that is a bug in one of the libraries (we have produced a reproducer).

### Athenz end-to-end testing against a real ZTS

End-to-end coverage lives in [`crates/magnetar/tests/e2e_athenz_zts.rs`](../crates/magnetar/tests/e2e_athenz_zts.rs) behind `feature = "e2e,auth-athenz-zts"` and is `#[ignore]`'d by default (parity with every other `e2e_*.rs` test).
Run with:

```sh
cargo test --features auth-athenz-zts \
  -p magnetar --test e2e_athenz_zts -- --nocapture --include-ignored
```

#### Athenz hybrid fixture shape

The Athenz ZTS server is operationally non-trivial to spin up in testcontainers — the upstream image expects a co-deployed ZMS (manager), per-tenant public-key seeding via the ZMS admin REST, and a chained TLS server certificate (Athenz's [`make deploy-dev`](https://github.com/AthenZ/athenz/blob/master/docker/README.md) orchestrates four containers + a cert-bootstrap pre-flight that together take ~15 minutes to build).
The test file therefore takes a hybrid shape:

| Test                                                       | Fixture                                     | What it proves                                                                                                                                                                                       |
| ---------------------------------------------------------- | ------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `e2e_athenz_zts_refresh_then_cached_initial`               | wiremock-stub                               | `ensure_role_token` populates the cache; `AuthProvider::initial()` returns the cached bytes; bearer header is a compact-JWS three-segment payload from the §3 signer.                                |
| `e2e_athenz_zts_expiry_aware_refresh_fires_on_near_expiry` | wiremock-stub                               | Driving the injected `now: Instant` past the cached deadline (`t0 + ttl − refresh_margin`) triggers a fresh exchange and rotates the cached bytes — no wall-clock wait.                              |
| `e2e_athenz_zts_cached_token_used_on_auth_challenge`       | wiremock-stub                               | `AuthChallengeState::handle_challenge` routes through `respond_to_challenge`, which echoes the cached role-token bytes verbatim; no extra ZTS round-trip.                                            |
| `e2e_athenz_zts_image_pulls_and_serves_status`             | Docker (`athenz/athenz-zts-server:1.12.41`) | The upstream image is pullable and `testcontainers-rs` port mapping works; if the host lacks a co-deployed ZMS the test surfaces the documented "expected without ZMS bootstrap" warning and passes. |

The wiremock tests run against a real `reqwest` client + real HTTP server (deterministic responses, no Docker dep — wiremock binds an ephemeral local port).
They cover every behavioural assertion the follow-up `/goal` enumerates.
The Docker probe wires the upstream image into the e2e surface so a downstream consumer with a fully-bootstrapped ZMS+ZTS topology can layer their own pre-seed step on top.

#### Athenz full ZMS+ZTS topology

Full ZMS+ZTS+cert-bootstrap testing requires running the Athenz `make deploy-dev` topology as a shared CI fixture (four containers, MySQL persistence, a CA hierarchy, ZMS-side `zms-cli add-public-key` seeding for the tenant).
Adding it would replace the `#[ignore]`'d Docker probe with a full multi-container compose fixture similar to [`crates/magnetar/tests/fixtures/docker-compose.replicated-subs.yml`](../crates/magnetar/tests/fixtures/docker-compose.replicated-subs.yml).
That work is out of scope for the current Athenz surface.

### Athenz cross-runtime test coverage (ADR-0024)

The testability seams (injected `wall_clock`, the `ZtsClient` trait, and `ensure_role_token(now)`) let the Athenz provider carry the full four-layer coverage ADR-0024 mandates — the same bar SASL meets:

| Layer            | File                                                                                                                                   | What it pins                                                                                                                                                              |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| (a) unit         | [`src/lib.rs`](../crates/magnetar-auth-athenz/src/lib.rs) / [`src/zts.rs`](../crates/magnetar-auth-athenz/src/zts.rs)                  | `build_claims` populates `iss`/`sub`/`kid` + the `iat`/`exp` window; `needs_refresh` cache transitions; `HttpZtsClient` URL validation.                                   |
| (b) tokio        | [`magnetar-runtime-tokio/tests/athenz_zts_round_trip.rs`](../crates/magnetar-runtime-tokio/tests/athenz_zts_round_trip.rs)             | The real `HttpZtsClient` against a `wiremock` ZTS stub: mint+cache, cache-hit absorption, expiry-driven rotation.                                                         |
| (c) moonpool     | [`magnetar-runtime-moonpool/tests/athenz_refresh_edge.rs`](../crates/magnetar-runtime-moonpool/tests/athenz_refresh_edge.rs)           | A scripted `ZtsClient` fake + injected `now: Instant`: refresh fires exactly at the virtual deadline; `with_role_token` bypass; ZTS failure leaves the cache un-poisoned. |
| (d) differential | [`magnetar-differential/tests/athenz_auth_data_equivalence.rs`](../crates/magnetar-differential/tests/athenz_auth_data_equivalence.rs) | Two independently-built providers on the same `(now, action)` schedule mint byte-identical JWTs and cache byte-identical CONNECT `auth_data`.                             |

The moonpool / differential layers never speak HTTPS — they inject the scripted `ZtsClient` fake, exactly as [ADR-0030 §moonpool](../specs/adr/0030-athenz-zts-round-trip-scope.md) and [ADR-0041](../specs/adr/0041-athenz-provider-testability-seams.md) prescribe — while the aws-lc-rs signer still mints a real, deterministic RS256 JWT.

### Athenz — what is _not_ here

- **ES256 (EC) keys.** The /goal mentioned ES256 as a fallback for EC keys, but Pulsar's Athenz integration and the Athenz Java client itself only emit RS256.
  The shape is ready (the JWS header builder already takes the alg as a parameter) but no consumer requests ES256 today.
- **SVC-token flow.** Out of scope per [ADR-0030](../specs/adr/0030-athenz-zts-round-trip-scope.md).
  Requires ZMS-side provisioning and an `instance_id` claim that the current `ZtsClaims` struct does not model.
