# Magnetar

> A blazing-fast, async, sans-io Apache Pulsar client for Rust.

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-orange.svg)](rust-toolchain.toml)
[![Status](https://img.shields.io/badge/status-pre--alpha-red.svg)](#status)
[![Pulsar](https://img.shields.io/badge/Pulsar-4.0%2B-2bc56b.svg)](#supported-broker-versions)

> **Status: pre-alpha.** The wire protocol layer is feature-rich, the tokio
> engine is usable end-to-end with supervised reconnect + transparent
> producer/consumer rebuild, and the moonpool engine ships the full
> client/producer/consumer/reader façade family for deterministic-simulation
> testing. API is unstable. Do not depend on this in production.

---

## What is magnetar?

Magnetar is a from-scratch Apache Pulsar client driver written in Rust. It
mirrors the surface area of the Apache Pulsar Java client and adds two
properties that the Java client cannot reach:

1. **Sans-io core.** The protocol state machine ([`magnetar-proto`]) is a pure,
   `quinn-proto`-style state machine — `handle_bytes` in, `poll_transmit` out,
   `poll_event` for semantic events, `poll_timeout` for timers. Zero I/O
   dependencies. No `tokio`. No `async`. No sockets. It is feed-only.
2. **Multiple swappable engines.** The same sans-io state machine is driven by
   a production tokio engine ([`magnetar-runtime-tokio`]) and by a deterministic
   simulation engine ([`magnetar-runtime-moonpool`]) for chaos testing of
   reconnects, partitions, and TLS handshake reorderings under reproducible
   seeds.

The architecture explicitly bans channels (`mpsc`, `broadcast`, `watch`,
`oneshot`, `crossbeam-channel`, `flume`, `async-channel`, …). The wake-up
mechanism is `Arc<parking_lot::Mutex<State>>` plus `tokio::sync::Notify` plus
`core::task::Waker` slabs inside the state machine. See
[ARCHITECTURE.md](ARCHITECTURE.md) for the full rationale.

Magnetar is independent of the existing `pulsar-rs` crate — it shares neither
code nor dependencies. The goal is feature-complete parity with the Java
client at v0.1.0.

[`magnetar-proto`]: crates/magnetar-proto
[`magnetar-runtime-tokio`]: crates/magnetar-runtime-tokio
[`magnetar-runtime-moonpool`]: crates/magnetar-runtime-moonpool

---

## Features at a glance

- **Protocol coverage**: producer, consumer, reader, partitioned producer,
  partitioned consumer, multi-topics consumer, pattern (regex) consumer,
  table view, transactions.
- **PIPs implemented or partially wired**: PIP-4 (end-to-end encryption),
  PIP-30 / PIP-292 (in-band `AUTH_CHALLENGE` refresh), PIP-31 (transactions),
  PIP-37 (chunking + redelivery backoff), PIP-54 (partial-batch ACK), PIP-90
  (broker-entry metadata), PIP-145 (regex topic discovery), PIP-313 (force
  unsubscribe). See [Supported PIPs](#supported-pips).
- **Transports**: TCP, TLS 1.3 (`rustls`-only — no `native-tls`,
  no `openssl`), binary proxy (`proxy_to_broker_url`).
- **Schemas**: bytes, string, JSON, Avro, Protobuf, Protobuf-native,
  KeyValue, Auto-consume, Auto-produce-bytes, plus the full primitive
  family — Int8, Int16, Int32, Int64, Float, Double, Bool, Date, Time,
  Timestamp, LocalDate, LocalTime, Instant, LocalDateTime.
- **Compression**: LZ4, ZSTD, Snappy, ZLIB.
- **Auth providers**: token, mTLS (the two stock providers in
  `magnetar-proto::auth`), plus the OAuth2 ClientCredentialsFlow, SASL, and
  Athenz scaffolds in dedicated crates.
- **Trackers**: ack grouping, unacked-message tracker (ack timeout +
  redelivery), negative-ack tracker with `MultiplierRedeliveryBackoff`
  (PIP-37), batch-index ACK set (PIP-54).
- **Interceptors**: `ProducerInterceptor` + `ConsumerInterceptor` SPIs.
- **Admin REST client**: a `reqwest`-backed admin client lives in
  `magnetar-admin`.
- **CLI**: `magnetar` binary in `magnetar-cli` covers admin lookups and stats
  today; data-plane subcommands ship in M9.

---

## Installation

Magnetar is not yet on crates.io. Use the Git path until the first release:

```toml
[dependencies]
magnetar = { git = "https://github.com/FlorentinDUBOIS/magnetar", branch = "main" }
```

The default feature set enables the tokio engine. The feature flags catalog:

| Flag | Default | Effect |
| --- | --- | --- |
| `tokio` | yes | Pulls in `magnetar-runtime-tokio` plus `tokio`/`futures-util`. The public `PulsarClient` lives behind this flag. |
| `moonpool` | no | Pulls in `magnetar-runtime-moonpool` for deterministic-simulation testing. |
| `admin` | no | Re-exports `magnetar-admin` under `magnetar::admin`. |
| `auth-oauth2` | no | Pulls in `magnetar-auth-oauth2` (OAuth2 ClientCredentialsFlow provider). |
| `auth-sasl` | no | Pulls in `magnetar-auth-sasl`. |
| `auth-athenz` | no | Pulls in `magnetar-auth-athenz`. |
| `encryption` | no | Pulls in `magnetar-messagecrypto` plus the PIP-4 bridge type. |
| `e2e` | no | Implies `tokio` + `admin`; flips on the `testcontainers`-backed end-to-end suite (requires Docker). |

The workspace ships eleven crates:

| Crate | Role |
| --- | --- |
| `magnetar` | Public façade — re-exports + builder + typed schemas wiring. |
| `magnetar-proto` | Sans-io protocol crate. The heart of the project. |
| `magnetar-runtime-tokio` | Production tokio engine with `tokio-rustls` TLS. |
| `magnetar-runtime-moonpool` | Deterministic-simulation engine (rustls-over-bytepipe TLS, no native TLS). |
| `magnetar-admin` | REST admin client (`reqwest` + `rustls-tls`). |
| `magnetar-cli` | `magnetar` binary — admin lookups today, produce / consume / inspect coming. |
| `magnetar-fakes` | In-process broker fake (dev-dep). Mirrors Java's `MockBrokerService`. |
| `magnetar-auth-oauth2` | OAuth2 ClientCredentialsFlow auth provider. |
| `magnetar-auth-sasl` | SASL auth provider. |
| `magnetar-auth-athenz` | Athenz auth provider. |
| `magnetar-messagecrypto` | PIP-4 end-to-end encryption (AES-GCM via `aws-lc-rs`). |

`xtask` is a workspace member but is **not published** — it hosts build
helpers (`protoc` codegen, e2e driver, dependency audits).

---

## Quickstart

The high-level `PulsarClient` builder is the public entry point. It wires the
tokio engine to the sans-io state machine and gives you producer / consumer /
reader / table-view / partitioned / multi-topics / pattern builders.

### Producer + consumer round trip

```rust,no_run
use magnetar::{OutgoingMessage, PulsarClient};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = PulsarClient::builder()
    .service_url("pulsar://localhost:6650")
    .build()
    .await?;

let producer = client
    .producer("persistent://public/default/orders")
    .name("orders-writer")
    .compression(magnetar_proto::types::CompressionKind::Zstd)
    .batching(/* max_messages */ 256, /* max_bytes */ 128 * 1024)
    .create()
    .await?;

producer
    .send(OutgoingMessage::with_payload(b"hello, pulsar".as_slice()).into())
    .await?;

let consumer = client
    .consumer("persistent://public/default/orders")
    .subscription("worker")
    .subscription_type(magnetar_proto::pb::command_subscribe::SubType::Shared)
    .subscribe()
    .await?;

let msg = consumer.receive().await?;
println!("payload: {:?}", msg.payload);
consumer.ack(msg.message_id).await?;
# Ok(()) }
```

### Typed schemas (`TypedProducer` + `TypedConsumer`)

```rust,no_run
use std::sync::Arc;
use magnetar::{PulsarClient, TypedProducerBuilder, TypedConsumerBuilder};
use magnetar_proto::schema::StringSchema;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = PulsarClient::builder()
    .service_url("pulsar://localhost:6650")
    .build()
    .await?;

let schema = Arc::new(StringSchema::new());

let producer = client
    .typed_producer("persistent://public/default/notes", schema.clone())
    .create()
    .await?;

producer.new_message().value("a note".to_string()).send().await?;

let consumer = client
    .typed_consumer("persistent://public/default/notes", schema)
    .subscription("transcriber")
    .subscribe()
    .await?;

let msg = consumer.receive().await?;
println!("decoded value: {}", msg.value);
consumer.ack(msg.id).await?;
# Ok(()) }
```

### Reader (non-durable, exclusive)

```rust,no_run
use magnetar::PulsarClient;
use magnetar_proto::MessageId;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = PulsarClient::builder()
    .service_url("pulsar://localhost:6650")
    .build()
    .await?;

let reader = client
    .reader("persistent://public/default/events")
    .start_message_id(MessageId::EARLIEST)
    .create()
    .await?;

while let Ok(msg) = reader.receive().await {
    println!("entry {:?}", msg.message_id);
}
# Ok(()) }
```

### Partitioned producer + consumer

```rust,no_run
use magnetar::{PulsarClient, MessageRoutingMode};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = PulsarClient::builder()
    .service_url("pulsar://localhost:6650")
    .build()
    .await?;

let p = client
    .partitioned_producer("persistent://public/default/events")
    .routing_mode(MessageRoutingMode::RoundRobin)
    .batching(/* max_messages */ 128, /* max_bytes */ 64 * 1024)
    .create()
    .await?;

p.new_message().key("user-42").value(b"event".as_slice()).send().await?;

let c = client
    .partitioned_consumer("persistent://public/default/events")
    .subscription("workers")
    .subscribe()
    .await?;

let msg = c.receive().await?;
c.ack(msg.topic(), msg.message_id).await?;
# Ok(()) }
```

### Pattern (regex) consumer — PIP-145

```rust,no_run
use magnetar::PulsarClient;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = PulsarClient::builder()
    .service_url("pulsar://localhost:6650")
    .build()
    .await?;

let pc = client
    .pattern_consumer()
    .namespace("public/default")
    .pattern("orders-.*")
    .subscription("workers")
    .subscribe(&client)
    .await?;

println!("matched topics: {:?}", pc.topics());

let msg = pc.receive().await?;
pc.ack(msg.topic(), msg.message_id).await?;
# Ok(()) }
```

### Table view (latest-value-per-key snapshot from a compacted topic)

```rust,no_run
use magnetar::PulsarClient;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = PulsarClient::builder()
    .service_url("pulsar://localhost:6650")
    .build()
    .await?;

let view = client
    .table_view("persistent://public/default/config")
    .subscription("cfg-watcher")
    .create()
    .await?;

view.for_each(|key, value| println!("{key} = {value:?}"));
let last = view.get("api.threshold");
# Ok(()) }
```

### Transactions — PIP-31

```rust,no_run
use std::time::Duration;
use magnetar::{PulsarClient, OutgoingMessage};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = PulsarClient::builder()
    .service_url("pulsar://localhost:6650")
    .build()
    .await?;

// open the transaction-coordinator-backed transaction
let runtime_client = /* obtain magnetar_runtime_tokio::Client */
#   unreachable!();
let txn = runtime_client.new_txn(Duration::from_secs(60)).await?;

let producer = client
    .producer("persistent://public/default/orders")
    .create()
    .await?;

producer
    .send(OutgoingMessage::with_payload(b"line-item".as_slice()).txn(txn.id()).into())
    .await?;

txn.commit().await?;
# Ok(()) }
```

### Interceptors (`ProducerInterceptor` + `ConsumerInterceptor`)

```rust,no_run
use std::sync::Arc;
use magnetar::{
    ConsumerInterceptor, IncomingMessage, OutgoingMessage, ProducerInterceptor, PulsarClient,
    send_with_interceptors,
};

#[derive(Debug)]
struct StampSender;
impl ProducerInterceptor for StampSender {
    fn before_send(&self, msg: &mut OutgoingMessage) {
        msg.properties.push(("client".to_owned(), "magnetar".to_owned()));
    }
}

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = PulsarClient::builder()
    .service_url("pulsar://localhost:6650")
    .build()
    .await?;

let producer = client
    .producer("persistent://public/default/orders")
    .create()
    .await?;

let chain: Vec<Arc<dyn ProducerInterceptor>> = vec![Arc::new(StampSender)];
let id = send_with_interceptors(
    &producer,
    OutgoingMessage::with_payload(b"hi".as_slice()),
    &chain,
)
.await?;
println!("acked at {id:?}");
# Ok(()) }
```

---

## Java client parity matrix

A check (`✅`) is a working public-API surface backed by code in the
workspace. A flag (`🟡`) means partial — a working subset; check
[ARCHITECTURE.md](ARCHITECTURE.md) for the open gaps. A cross (`❌`) is a
known-missing feature.

### Producer

| Feature | Java | Magnetar | Notes |
| --- | --- | --- | --- |
| `send(...)` / `sendAsync(...)` | ✅ | ✅ | `Producer::send` returns a `SendFut`. |
| Producer name | ✅ | ✅ | `ProducerBuilder::name`. |
| Compression (LZ4, ZSTD, Snappy, ZLIB, NONE) | ✅ | ✅ | `ProducerBuilder::compression`. |
| Batching (`BatchMessageContainerImpl`) | ✅ | ✅ | `ProducerBuilder::batching` (max-msgs + max-bytes). |
| `batchingMaxPublishDelay` flush timer | ✅ | ✅ | `ProducerBuilder::batching_max_publish_delay`. |
| Chunking (PIP-37) | ✅ | ✅ | `ProducerBuilder::chunking`. Chunks-never-batched enforced. |
| `initialSequenceId` | ✅ | ✅ | `ProducerBuilder::initial_sequence_id`. |
| `sendTimeout` | ✅ | ✅ | `ProducerBuilder::send_timeout`. |
| `accessMode` (Shared/Exclusive/WaitForExclusive/Fencing) | ✅ | ✅ | `ProducerBuilder::access_mode`. PIP-68. |
| `accessMode` getter | ✅ | ✅ | `Producer::access_mode`. |
| `getProducerName` | ✅ | ✅ | `Producer::name`. |
| `getTopic` | ✅ | ✅ | `Producer::topic`. |
| `isConnected` / `isClosed` | ✅ | ✅ | `Producer::is_connected` / `is_closed`. |
| `getLastSequenceId` | ✅ | ✅ | `Producer::last_sequence_id`. |
| `getLastSequenceIdPublished` | ✅ | ✅ | `Producer::last_sequence_id_published`. |
| `getLastDisconnectedTimestamp` | ✅ | ✅ | `Producer::last_disconnected_timestamp`. |
| `flush()` | ✅ | ✅ | `Producer::flush`. |
| `close()` | ✅ | ✅ | `Producer::close`. |
| `getStats` | ✅ | ✅ | `Producer::stats` — counters + `send_latency_{p50,p99,max}_ms` via `hdrhistogram`. Rolling per-second windows still pending. |
| `getCompressionType` getter | ✅ | ✅ | `Producer::compression`. |
| Per-message `key` / `orderingKey` | ✅ | ✅ | `OutgoingMessage::key` / `ordering_key`. |
| Per-message `eventTime` | ✅ | ✅ | `OutgoingMessage::event_time_ms`. |
| `deliverAt` / `deliverAfter` | ✅ | ✅ | `OutgoingMessage::deliver_at_ms` / `deliver_after_ms`. |
| `replicationClusters` + `disableReplication` | ✅ | ✅ | `OutgoingMessage::replication_clusters` / `disable_replication`. |
| `newMessage(Transaction)` (PIP-31) | ✅ | ✅ | `OutgoingMessage::txn(txn_id)`. |
| `Properties` (per-message key/value) | ✅ | ✅ | `OutgoingMessage::property`. |
| `TypedMessageBuilder` | ✅ | ✅ | `MessageBuilder` via `ProducerExt::new_message`. |
| `ProducerInterceptor` SPI | ✅ | ✅ | `magnetar::ProducerInterceptor` + `send_with_interceptors`. |
| `pendingQueueSize` getter | ✅ | ✅ | `Producer::pending_count` (`batch_len` + `batch_bytes` are bonus). |

### Consumer

| Feature | Java | Magnetar | Notes |
| --- | --- | --- | --- |
| `subscribe(...)` (`Exclusive` / `Shared` / `Failover` / `Key_Shared`) | ✅ | ✅ | `ConsumerBuilder::subscription_type`. |
| `receive` / `receiveAsync` / `receive(timeout)` | ✅ | ✅ | `Consumer::receive` + `receive_with_timeout`. |
| `batchReceive` / `batchReceiveAsync` | ✅ | ✅ | `Consumer::receive_batch_with_bytes_cap` (cap on count + bytes). |
| `acknowledge` (individual) | ✅ | ✅ | `Consumer::ack`. |
| `acknowledgeCumulative` | ✅ | ✅ | `Consumer::ack_cumulative`. |
| `acknowledge(messages)` (batch ack) | ✅ | ✅ | `Consumer::ack_batch`. |
| `acknowledge(MessageId, Map<String,String>)` | ✅ | ✅ | `Consumer::ack_with_properties`. |
| `acknowledge(MessageId, Transaction)` (PIP-31) | ✅ | ✅ | `Consumer::ack_with_txn`. |
| `acknowledgeAsync(messages, Transaction)` | ✅ | ✅ | `Consumer::ack_batch_with_txn`. |
| `acknowledgeCumulative(MessageId, Map)` | ✅ | ✅ | `Consumer::ack_cumulative_with_properties`. |
| `acknowledgeCumulative(MessageId, Transaction)` | ✅ | ✅ | `Consumer::ack_cumulative_with_txn`. |
| Batch-index ACK (PIP-54) | ✅ | ✅ | `ack_set` bitset stamped on individual acks. |
| `acknowledgmentGroupTime` (grouping window) | ✅ | ✅ | `ConsumerBuilder::ack_group_time` + `ack_grouped` / `ack_grouped_cumulative`. |
| `negativeAcknowledge` | ✅ | ✅ | `Consumer::negative_ack`. |
| `negativeAcknowledge(messages)` | ✅ | ✅ | `Consumer::negative_ack_batch`. |
| `negativeAcknowledge(MessageId, delay)` | ✅ | ✅ | `Consumer::negative_ack_with_delay`. |
| `MultiplierRedeliveryBackoff` (PIP-37) | ✅ | ✅ | `magnetar_proto::trackers::MultiplierRedeliveryBackoff`. |
| `reconsumeLater` (retry-letter topic) | ✅ | ✅ | `Consumer::reconsume_later` + `_with_properties`. |
| `ackTimeout` (unacked tracker) | ✅ | ✅ | `ConsumerBuilder::ack_timeout`. |
| `ackTimeoutRedeliveryBackoff` (PIP-37) | ✅ | ✅ | `ConsumerBuilder::ack_timeout_backoff`. |
| `negativeAckRedeliveryDelay` | ✅ | ✅ | `ConsumerBuilder::negative_ack_redelivery_delay`. |
| `seek(MessageId)` | ✅ | ✅ | `Consumer::seek`. |
| `seek(timestamp)` | ✅ | ✅ | `Consumer::seek_timestamp`. |
| `seekAsync(Function<String, Object>)` (per-partition) | ✅ | ✅ | `PartitionedConsumer::seek_per_partition` / `MultiTopicsConsumer::seek_per_partition` — callback returns `SeekTarget::MessageId` or `SeekTarget::PublishTimeMs` per topic. |
| `seekToEarliest` / `seekToLatest` | ✅ | ✅ | `Consumer::seek_to_earliest` / `seek_to_latest`. |
| `pause()` / `resume()` / `isPaused()` | ✅ | ✅ | `Consumer::pause` / `resume` / `is_paused`. |
| `hasReachedEndOfTopic` | ✅ | ✅ | `Consumer::has_reached_end_of_topic`. |
| `redeliverUnacknowledgedMessages` | ✅ | ✅ | `Consumer::redeliver_unacked`. |
| `getLastMessageId` | ✅ | ✅ | `Consumer::last_message_id`. |
| `getStats` (counters) | ✅ | ✅ | `Consumer::stats`. Includes `total_chunked_msgs_received`. |
| Stats: rolling windows (msgs/sec, bytes/sec) | ✅ | 🟡 | Cumulative only today; rolling-window tick pending. |
| Stats: latency hdrhistogram (p50/p99/max) | ✅ | ✅ | `Consumer::stats` exposes `receive_latency_{p50,p99,max}_ms`; `Producer::stats` exposes `send_latency_{p50,p99,max}_ms`. |
| `subscriptionProperties` | ✅ | ✅ | `ConsumerBuilder::subscription_property`. |
| `replicateSubscriptionState` | ✅ | ✅ | `ConsumerBuilder::replicate_subscription_state`. |
| `priorityLevel` | ✅ | ✅ | `ConsumerBuilder::priority_level`. |
| `keySharedPolicy` (sticky / auto-split / hash) | ✅ | ✅ | `ConsumerBuilder::key_shared_policy`. PIP-34/119/282/379. |
| `startMessageId` | ✅ | ✅ | `ConsumerBuilder::start_message_id`. |
| `startMessageRollbackDuration` | ✅ | ✅ | `ConsumerBuilder::start_message_rollback_duration`. |
| `readCompacted` | ✅ | ✅ | `ConsumerBuilder::read_compacted`. |
| `forceTopicCreation` | ✅ | ✅ | `ConsumerBuilder::force_topic_creation`. |
| Dead-letter policy | ✅ | ✅ | `ConsumerBuilder::dead_letter_policy` + `Consumer::drain_dead_letter`. PIP-22/58/124/409. |
| `cryptoFailureAction` (PIP-4) | ✅ | ✅ | `Fail` returns the decryption error; `Discard` silently drops the message; `Consume` delivers the ciphertext to the user. All three wired in `magnetar-runtime-tokio::consumer::deliver_post_process`. |
| Encryption (PIP-4) | ✅ | ✅ | `ConsumerBuilder::encryption` accepts a `MessageDecryptor`. |
| `ConsumerInterceptor` SPI | ✅ | ✅ | `magnetar::ConsumerInterceptor` + `receive_with_interceptors`. |
| `unsubscribe()` | ✅ | ✅ | Consumer / multi-topics expose unsubscribe. |
| `forceUnsubscribe` (PIP-313) | ✅ | ✅ | Wired through `CommandUnsubscribe.force`. |
| `availablePermits` getter | ✅ | ✅ | `Consumer::available_permits`. |
| `availableInQueue` getter | ✅ | ✅ | `Consumer::available_in_queue`. |
| `hasReceivedAnyMessage` getter | ✅ | ✅ | `Consumer::has_received_any_message`. |

### Partitioned producer

| Feature | Java | Magnetar | Notes |
| --- | --- | --- | --- |
| Auto partition discovery | ✅ | ✅ | `PulsarClient::partitions_for_topic` + builder. |
| `MessageRoutingMode` (RoundRobin / SinglePartition / Custom) | ✅ | ✅ | `MessageRoutingMode`. |
| Custom `MessageRouter` trait | ✅ | ✅ | `MessageRouter` trait + `message_router(...)`. |
| Murmur3 + JavaStringHash hashers | ✅ | ✅ | `Murmur3HashHasher` / `JavaStringHashHasher`. |
| `TypedMessageBuilder`-equivalent on partitioned producer | ✅ | ✅ | `PartitionedMessageBuilder`. |
| Per-partition stats / `lastSequenceId` | ✅ | ✅ | Aggregated across child producers. |
| Auto-update partition count (background ticker) | ✅ | 🟡 | Single-shot today; periodic reconcile is a Java parity gap. |

### Partitioned consumer

| Feature | Java | Magnetar | Notes |
| --- | --- | --- | --- |
| Auto partition discovery + one consumer per partition | ✅ | ✅ | `PulsarClient::partitioned_consumer`. |
| Full `ConsumerBuilder` knob forwarding | ✅ | ✅ | 12 knobs forwarded from builder. |
| Receive / ack / nack / seek / unsubscribe across partitions | ✅ | ✅ | All forwarded. |
| Auto-update partition count | ✅ | ❌ | Same gap as partitioned producer. |

### Multi-topics consumer

| Feature | Java | Magnetar | Notes |
| --- | --- | --- | --- |
| Subscribe to N explicit topics under one subscription | ✅ | ✅ | `MultiTopicsConsumerBuilder::topics`. |
| Receive / ack / nack / seek across all topics | ✅ | ✅ | Per-topic forwarding. |
| `negativeAckWithDelay` / `ackCumulative` | ✅ | ✅ | Forwarded. |
| Dynamic `add_topic` / `remove_topic` | ✅ | ✅ | `MultiTopicsConsumer::add_topic` / `remove_topic` — subscribe / unsubscribe at runtime. |

### Pattern consumer (PIP-145)

| Feature | Java | Magnetar | Notes |
| --- | --- | --- | --- |
| Regex topic subscription | ✅ | ✅ | `PatternConsumerBuilder::pattern`. |
| `TopicListChanged` delta stream | ✅ | ✅ | `Client::next_topic_list_change`. |
| Manual `update()` reconcile | ✅ | ✅ | `PatternConsumer::update(&client)` returns a `ReconcileReport`. |
| Auto-update background ticker | ✅ | ✅ | `PatternConsumer::start_auto_reconcile(client, interval)` spawns a `tokio::time::interval`-driven loop that calls `update(&client)` on every tick; returns a `JoinHandle` for clean shutdown. |

### Reader

| Feature | Java | Magnetar | Notes |
| --- | --- | --- | --- |
| Non-durable exclusive subscription | ✅ | ✅ | `ReaderBuilder` builds on `ConsumerBuilder`. |
| `startMessageId` (Earliest / Latest / explicit) | ✅ | ✅ | `ReaderBuilder::start_message_id`. |
| `startMessageIdInclusive` rollback duration | ✅ | ✅ | `ReaderBuilder::start_message_rollback_duration`. |
| `readCompacted` | ✅ | ✅ | `ReaderBuilder::read_compacted`. |
| `cryptoKeyReader` (PIP-4 decryptor) | ✅ | ✅ | `ReaderBuilder::encryption`. |
| `hasMessageAvailable` / `seek` | ✅ | ✅ | Via the underlying consumer surface. |
| Stats / closure getters (`isClosed`, etc.) | ✅ | ✅ | `Reader::is_closed`, `available_in_queue`, `available_permits`. |

### Table view

| Feature | Java | Magnetar | Notes |
| --- | --- | --- | --- |
| Compacted-topic snapshot keyed by message key | ✅ | ✅ | `TableView::get` / `for_each` / `snapshot` / `keys` / `values`. |
| Listener registration | ✅ | ✅ | `TableView::listen` (`TableViewListener`). |
| Schema-aware `TypedTableView` | ✅ | ✅ | `TypedTableView<S>` decodes per-read. |
| `startMessageId` / `subscriptionProperty` / `property` knobs | ✅ | ✅ | `TableViewBuilder` knob set. |
| Auto-update-partitions ticker | ✅ | ❌ | Same gap as PartitionedProducer. |
| `cryptoKeyReader` wired through | ✅ | ✅ | `TableViewBuilder::encryption` + `TypedTableViewBuilder::encryption` stamp the decryptor onto the underlying `ConsumerBuilder`. |

### Transactions (PIP-31)

| Feature | Java | Magnetar | Notes |
| --- | --- | --- | --- |
| Transaction coordinator client | ✅ | ✅ | `magnetar-proto::txn::TxnClient`. |
| Begin / commit / abort | ✅ | ✅ | `Client::new_txn` + `Transaction::commit` / `abort`. |
| `ADD_PARTITION_TO_TXN` / `ADD_SUBSCRIPTION_TO_TXN` | ✅ | ✅ | `Client::add_partition_to_txn` / `add_subscription_to_txn`. |
| Producer publish under txn | ✅ | ✅ | `OutgoingMessage::txn`. |
| Consumer ack under txn (individual + cumulative + batch) | ✅ | ✅ | `Consumer::ack_with_txn` and friends. |
| `END_TXN_ON_PARTITION` / `_ON_SUBSCRIPTION` cleanup | ✅ | ✅ | Driven by `end_txn`. |

### Auth + TLS

| Feature | Java | Magnetar | Notes |
| --- | --- | --- | --- |
| Token auth | ✅ | ✅ | `magnetar_proto::auth::TokenAuth`. |
| mTLS | ✅ | ✅ | `magnetar_proto::auth::TlsAuth` + `tls_trust_certs_pem` / `tls_trust_certs_file_path`. |
| OAuth2 ClientCredentialsFlow | ✅ | 🟡 | Crate scaffolded (`magnetar-auth-oauth2`); flow integration is pre-alpha. |
| SASL (Kerberos) | ✅ | 🟡 | Crate scaffolded (`magnetar-auth-sasl`); pre-alpha. |
| Athenz | ✅ | 🟡 | Crate scaffolded (`magnetar-auth-athenz`); pre-alpha. |
| In-band `AUTH_CHALLENGE` refresh (PIP-30 / PIP-292) | ✅ | ✅ | Driver consults the configured `AuthProvider` and submits `CommandAuthResponse`. |
| `pulsar+ssl://` URLs | ✅ | ✅ | Built-in. |
| Binary proxy (`proxy_to_broker_url`) | ✅ | ✅ | `ClientBuilder::proxy_to_broker_url`. |

### Encryption (PIP-4)

| Feature | Java | Magnetar | Notes |
| --- | --- | --- | --- |
| `MessageEncryptor` trait on producer | ✅ | ✅ | `ProducerBuilder::encryption`. |
| `MessageDecryptor` trait on consumer | ✅ | ✅ | `ConsumerBuilder::encryption`. |
| AES-GCM via `aws-lc-rs` | n/a (Java uses BouncyCastle) | ✅ | `magnetar-messagecrypto::MessageCrypto`. |
| `cryptoFailureAction` | ✅ | ✅ | `Fail` / `Discard` / `Consume` all wired end-to-end in `magnetar-runtime-tokio::consumer::deliver_post_process`. |

### Schemas

| Feature | Java | Magnetar | Notes |
| --- | --- | --- | --- |
| `BytesSchema` | ✅ | ✅ | |
| `StringSchema` | ✅ | ✅ | |
| `JsonSchema` | ✅ | ✅ | Canonicalised via the Avro parser per Codex Q4. |
| `AvroSchema` | ✅ | ✅ | `apache-avro` 0.21 — canonical-parsing form. |
| `ProtobufSchema` (descriptor) | ✅ | ✅ | |
| `ProtobufNativeSchema` | ✅ | ✅ | Byte-identical Java `FileDescriptorSet` output. |
| `KeyValueSchema` | ✅ | ✅ | Byte-identical canonical JSON wrapper. |
| `AutoConsumeSchema` (broker lookup) | ✅ | ✅ | `TypedConsumer::receive` auto-fetches the broker schema on first call via `Connection::get_schema`; the result is cached on the schema's `Arc<Mutex<Option<pb::Schema>>>`. |
| `AutoProduceBytesSchema` | ✅ | 🟡 | Trait surface only. |
| Int8 / Int16 / Int32 / Int64 / Float / Double / Bool | ✅ | ✅ | |
| Date / Time / Timestamp / LocalDate / LocalTime / Instant / LocalDateTime | ✅ | ✅ | |
| Schema-version negotiation | ✅ | ✅ | Sent on `CommandProducer` / `CommandSubscribe`. |

### Client builder

| Feature | Java | Magnetar | Notes |
| --- | --- | --- | --- |
| `serviceUrl` | ✅ | ✅ | `ClientBuilder::service_url`. |
| `clientVersion` | ✅ | ✅ | `ClientBuilder::client_version`. |
| `keepAliveInterval` | ✅ | ✅ | `ClientBuilder::keepalive`. |
| `operationTimeout` | ✅ | ✅ | `ClientBuilder::operation_timeout`. |
| `maxMessageSize` | ✅ | ✅ | `ClientBuilder::max_message_size`. |
| `tlsTrustCertsFilePath` | ✅ | ✅ | `ClientBuilder::tls_trust_certs_file_path`. |
| `tlsAllowInsecureConnection` | ✅ | ✅ | `ClientBuilder::tls_allow_insecure_connection(true)` — accepts any server cert via a custom rustls verifier. **Insecure**, do not use in production. |
| `enableTlsHostnameVerification` | ✅ | 🟡 | `ClientBuilder::tls_hostname_verification_enable(bool)`; the "chain on + hostname off" combination is the planned follow-up (today honoured only via the `tls_allow_insecure_connection` blanket override). |
| `serviceUrlProvider` (URL rotation) | ✅ | ✅ | `ClientBuilder::service_url_provider(Arc<dyn ServiceUrlProvider>)` — the supervised reconnect path calls `provider.get_service_url()` on every reconnect attempt, so cluster-failover policies can swap broker URLs between attempts. |
| `proxyServiceUrl` (binary proxy) | ✅ | ✅ | `ClientBuilder::proxy_to_broker_url`. |
| `Authentication` plugin | ✅ | ✅ | `ClientBuilder::auth(Arc<dyn AuthProvider>)`. |
| `memoryLimit` | ✅ | 🟡 | `ClientBuilder::memory_limit(bytes, MemoryLimitPolicy)` + `PulsarClient::memory_limit` getter; runtime enforcement (accounting + blocking on `ProducerBlock`) pending. |
| `dnsResolver` customisation | ✅ | 🟡 | `ClientBuilder::dns_resolver(Arc<dyn DnsResolver>)` trait + `TokioDnsResolver` default impl shipped; routing through `Transport::connect` is the planned follow-up (same pattern ServiceUrlProvider followed). |
| `isClosed` / `shutdown` / `getLastDisconnectedTimestamp` | ✅ | ✅ | All exposed on `PulsarClient`. |
| Cluster failover (PIP-121) | ✅ | 🟡 | `ServiceUrlProvider` trait + `StaticServiceUrlProvider` + `ClientBuilder::service_url_provider`; `AutoClusterFailover` and `ControlledClusterFailover` policies pending. |

### Open structural gaps

- **Stats rolling windows.** Cumulative-only counters today; the broker dashboard expects msgs/sec, bytes/sec rolling windows. `hdrhistogram` p50/p99/max has shipped (`Consumer::stats` + `Producer::stats`).
- **PIP-121 cluster failover.** `ServiceUrlProvider` + `ControlledClusterFailover` policy in flight; today the driver reconnects to the same `service_url`.
- **PIP-460 scalable topics** + **PIP-466 V5 surface** + **PIP-180 shadow topic** + **PIP-415 `getMessageIdByIndex`** + **PIP-33 replicated subscriptions** are scoped for the M9 milestone.

---

## Supported PIPs

| PIP | Title | Status | Lives in |
| --- | --- | --- | --- |
| PIP-4 | End-to-end encryption (AES-GCM) | ✅ | `magnetar-messagecrypto`, `crypto_bridge` in `magnetar` |
| PIP-22 | DLQ topic | ✅ | `ConsumerBuilder::dead_letter_policy` |
| PIP-30 | In-band `AUTH_CHALLENGE` refresh | ✅ | `magnetar-proto::auth`, driver |
| PIP-31 | Transactions | ✅ | `magnetar-proto::txn`, `Client::new_txn` |
| PIP-37 | Chunking + ack-timeout redelivery backoff | ✅ | `magnetar-proto::producer`, `trackers::nack` |
| PIP-54 | Partial-batch ACK (ack_set bitset) | ✅ | `magnetar-proto::consumer` |
| PIP-58 | Retry-letter topic | ✅ | `Consumer::reconsume_later` |
| PIP-68 | Exclusive producer access mode | ✅ | `ProducerBuilder::access_mode` |
| PIP-90 | Broker-entry metadata envelope | ✅ | `magnetar-proto::frame` (magic `0x0e02`), `IncomingMessage::broker_*` |
| PIP-124 | Multi-DLQ topics for KeyShared | ✅ | DLQ policy infra |
| PIP-145 | Topic list watcher (regex pattern) | ✅ | `magnetar-proto::topic_watcher`, `PatternConsumer` |
| PIP-292 | Better in-band auth refresh ergonomics | ✅ | Driver event handler |
| PIP-313 | Force unsubscribe | ✅ | `CommandUnsubscribe.force` plumbed |
| PIP-34 / 119 / 282 / 379 | Key_Shared family | ✅ | `KeySharedConfig` + builder |
| PIP-409 | DLQ + retry-letter polish | ✅ | DLQ + reconsume_later wiring |
| PIP-391 | Batch-index ACK polish | ✅ | Pairs with PIP-54 |
| PIP-188 | `TOPIC_MIGRATED` | 🟡 | Wire opcode present; engine-level reconnect-on-migrate pending |
| PIP-460 | Scalable topics | ❌ | Scoped for M9 (experimental) |
| PIP-466 | V5 client API surface | ❌ | Inspired by, not adopted verbatim — magnetar ships its own idiomatic surface |
| PIP-180 | Shadow topic | ❌ | M9 |
| PIP-415 | `getMessageIdByIndex` | ❌ | M9 |
| PIP-33 | Replicated subscriptions | ❌ | M9 |
| PIP-121 | Cluster failover (Auto + Controlled) | 🟡 | `ServiceUrlProvider` trait + `StaticServiceUrlProvider` shipped; Auto/Controlled policies pending |

---

## Runtime engines

Magnetar publishes two engines that drive the same sans-io state machine.
Pick at compile time via feature flags.

### `magnetar-runtime-tokio` — production (default)

- ~2,950 lines of code across `client.rs` (654), `consumer.rs` (937),
  `producer.rs` (382), `driver.rs` (239), `compress.rs` (210),
  `transport.rs` (125), `url_parse.rs` (113), `crypto.rs` (48),
  `error.rs` (67), `lib.rs` (182).
- TLS via [`tokio-rustls`](https://crates.io/crates/tokio-rustls) (ring
  backend); no `native-tls`, no `openssl`.
- One driver task per connection — see
  [ARCHITECTURE.md §"The driver loop"](ARCHITECTURE.md#the-driver-loop).
- The user-facing futures (`Consumer::receive`, `Producer::send`, …) lock
  the shared state machine, register their `Waker` in a slab, and wait. The
  driver picks them up as the matching `OpOutcome` lands.
- This is what `magnetar::PulsarClient` wires by default.

### `magnetar-runtime-moonpool` — deterministic simulation

- ~2,740 lines of code across `client.rs` (414), `consumer.rs` (670),
  `driver.rs` (295), `lib.rs` (343), `producer.rs` (701), `tls.rs` (215),
  `transport.rs` (104). The M1 → M4 milestones (engine, client, producer,
  consumer) have all landed; the surface mirrors the tokio engine 1:1 so
  the same `magnetar::PulsarClient`-style usage compiles under the
  `moonpool` feature.
- TLS uses a local `rustls::ClientConnection` adapter (`tls.rs`) that drives
  `read_tls` / `process_new_packets` / `write_tls` over the moonpool byte
  pipe. The handshake is therefore deterministic under `moonpool-sim` chaos.
- Generic over `moonpool_core::Providers` (`NetworkProvider`, `TimeProvider`,
  `TaskProvider`, `RandomProvider`, `StorageProvider`). Plug `TokioProviders`
  for production-style runs against a real broker, or a sim bundle for
  reproducible chaos under `moonpool-sim` seeds.

---

## Supported broker versions

- **Pulsar 4.0+** (LTS). The CONNECT frame advertises `ProtocolVersion::V21`
  and the connection falls back to whichever lower version the broker
  reports on `CONNECTED`.
- The end-to-end suite runs against `apachepulsar/pulsar:4.0.4`.

---

## Roadmap

Magnetar tracks v0.1.0 against true Java parity. Open work is segmented
into milestones M0 through M9; M9 wraps with the admin client + CLI +
PIP-121 / PIP-33 / PIP-460 / PIP-180 / PIP-415 / replicated subscriptions
+ shadow topic. Milestone + gap tracking is internal-only.

Top open items at the time of this writing:

1. **PIP-121 cluster failover policies** — `AutoClusterFailover` (latency-
   based) + `ControlledClusterFailover` (external-signal) on top of the
   shipped `ServiceUrlProvider` runtime URL rotation.
2. **PIP-415 `getMessageIdByIndex`** — blocked on vendored proto bump
   (opcode missing from the current `PulsarApi.proto` snapshot).
3. **PIP-460 scalable topics / PIP-466 V5 surface / PIP-180 shadow
   topic / PIP-33 replicated subscriptions** — scoped for M9.

`hdrhistogram` latency stats (p50/p99/max), the
`MultiTopicsConsumer::add_topic` / `remove_topic` mutators, the
`PatternConsumer::start_auto_reconcile` ticker, and the
`ServiceUrlProvider` runtime URL rotation on every reconnect attempt
have already landed.

The supervised reconnect (Stage 2) and transparent in-flight producer +
consumer rebuild (Stage 3, `Connection::rebuild_producers` /
`rebuild_consumers`) have landed and run on every disconnect.

---

## Validation

The whole workspace builds against stable Rust 1.85.

```sh
# Build / lint / format
cargo build --workspace --all-features
cargo clippy --workspace --all-features -- -D warnings
cargo +nightly fmt --check

# Unit + integration tests (no broker needed)
cargo test --workspace

# Dependency audits
cargo deny check

# Docs
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

End-to-end tests against a real broker (Docker required, runs `pulsar:4.0.4`):

```sh
cargo test --workspace --features e2e
```

Additional `xtask` checks specific to the sans-io invariants:

```sh
cargo xtask check-no-channels   # greps src/** for banned channel crates
cargo xtask check-no-io-deps    # magnetar-proto must not depend on any I/O crate
cargo xtask codegen --check     # asserts proto codegen has no drift
```

---

## License

Apache-2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE). The project vendors
a verbatim copy of the Apache Pulsar wire protocol definition
(`PulsarApi.proto`, `PulsarMarkers.proto`), released by the Apache Software
Foundation under Apache-2.0.

See [GUIDELINES.md](GUIDELINES.md) and [CONTRIBUTING.md](CONTRIBUTING.md) for
project conventions before sending a patch.
