# Magnetar

> A blazing-fast, async, sans-io Apache Pulsar client for Rust.

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE) [![MSRV](https://img.shields.io/badge/MSRV-1.88-orange.svg)](rust-toolchain.toml) [![Status](https://img.shields.io/badge/status-pre--alpha-red.svg)](#status) [![Pulsar](https://img.shields.io/badge/Pulsar-4.0%2B-2bc56b.svg)](#supported-broker-versions)

> **Status: pre-alpha.** The wire protocol layer is feature-rich, the tokio engine is usable end-to-end with supervised reconnect + transparent producer/consumer rebuild, and the moonpool engine carries client/producer/consumer for deterministic-simulation testing.
> API is unstable.
> Do not depend on this in production.

---

## What is magnetar?

Magnetar is a from-scratch Apache Pulsar client driver written in Rust.
It mirrors the surface area of the Apache Pulsar Java client and adds two properties that the Java client cannot reach:

1. **Sans-io core.** The protocol state machine ([`magnetar-proto`]) is a pure, `quinn-proto`-style state machine — `handle_bytes` in, `poll_transmit` out, `poll_event` for semantic events, `poll_timeout` for timers.
   Zero I/O dependencies.
   No `tokio`.
   No `async`.
   No sockets.
   It is feed-only.
2. **Multiple swappable engines.** The same sans-io state machine is driven by a production tokio engine ([`magnetar-runtime-tokio`]) and by a deterministic simulation engine ([`magnetar-runtime-moonpool`]) for chaos testing of reconnects, partitions, and TLS handshake reorderings under reproducible seeds.

The architecture explicitly bans channels (`mpsc`, `broadcast`, `watch`, `oneshot`, `crossbeam-channel`, `flume`, `async-channel`, …).
The wake-up mechanism is `Arc<parking_lot::Mutex<State>>` plus `tokio::sync::Notify` plus `core::task::Waker` slabs inside the state machine.
See [ARCHITECTURE.md](ARCHITECTURE.md) for the full rationale.

Magnetar is independent of the existing `pulsar-rs` crate — it shares neither code nor dependencies.
The goal is feature-complete parity with the Apache Pulsar Java client.

[`magnetar-proto`]: crates/magnetar-proto
[`magnetar-runtime-tokio`]: crates/magnetar-runtime-tokio
[`magnetar-runtime-moonpool`]: crates/magnetar-runtime-moonpool

---

## Features at a glance

- **Protocol coverage**: producer, consumer, reader, partitioned producer, partitioned consumer, multi-topics consumer, pattern (regex) consumer, table view, transactions.
- **PIPs implemented or partially wired**: PIP-4 (end-to-end encryption), PIP-30 / PIP-292 (in-band `AUTH_CHALLENGE` refresh), PIP-31 (transactions), PIP-37 (chunking + redelivery backoff), PIP-54 (partial-batch ACK), PIP-87 (AutoConsumeSchema broker lookup), PIP-90 (broker-entry metadata), PIP-121 (cluster failover — `ServiceUrlProvider` + `ControlledClusterFailover`
  - `AutoClusterFailover`), PIP-145 (regex topic discovery), PIP-188
    (`TOPIC_MIGRATED` with supervised reconnect), PIP-313 (force unsubscribe).
    See [Supported PIPs](#supported-pips).
- **Resilience**: supervised reconnect with `Connection::reset` (Stage 2) + transparent producer / consumer rebuild via `rebuild_producers` / `rebuild_consumers` (Stage 3) + `memory_limit` runtime enforcement (Java `MemoryLimitPolicy::FailImmediately`) + global publish-bytes accounting via `AtomicU64` CAS in `Producer::send` with release on `Drop`.
- **Observability**: cumulative counters + `hdrhistogram` p50/p99/max latency
  - rolling-window msgs/sec + bytes/sec rates (`record_rate_window`).
- **Transports**: TCP, TLS 1.3 (`rustls`-only — no `native-tls`, no `openssl`), binary proxy (`proxy_to_broker_url`), pluggable DNS (`DnsResolver` trait + `TokioDnsResolver` default routed through `Transport::connect`).
- **TLS knobs**: `tls_trust_certs_file_path`, `tls_allow_insecure_connection` (blanket override), `tls_hostname_verification_enable(false)` paired with a PEM trust store (chain-on / hostname-off via custom rustls verifier).
- **Schemas**: bytes, string, JSON, Avro, Protobuf, Protobuf-native, KeyValue, Auto-consume, Auto-produce-bytes, plus the full primitive family — Int8, Int16, Int32, Int64, Float, Double, Bool, Date, Time, Timestamp, LocalDate, LocalTime, Instant, LocalDateTime.
- **Compression**: LZ4, ZSTD, Snappy, ZLIB.
- **Auth providers**: token, mTLS (the two stock providers in `magnetar-proto::auth`), OAuth2 `ClientCredentialsFlow` (working — fetches
  - caches + auto-refreshes JWTs against a standard OIDC token endpoint),
    SASL `PLAIN` (RFC 4616, working), SASL Kerberos / GSSAPI via `libgssapi` under the `auth-sasl-kerberos` feature (working — multi-round `AUTH_CHALLENGE` initiate loop), Athenz with a pre-fetched role token (`AthenzProvider::with_role_token`, working), and the opt-in Athenz ZTS round-trip (`auth-athenz-zts`, working via `AthenzProvider::with_default_signer` or a custom `zts::JwtSigner` / `zts::ZtsClient`).
- **Trackers**: ack grouping, unacked-message tracker (ack timeout + redelivery), negative-ack tracker with `MultiplierRedeliveryBackoff` (PIP-37), batch-index ACK set (PIP-54).
- **Interceptors**: `ProducerInterceptor` + `ConsumerInterceptor` SPIs.
- **Admin REST client**: a `reqwest`-backed admin client lives in `magnetar-admin`.
- **CLI**: `magnetar` binary in `magnetar-cli` covers admin lookups and stats today; data-plane subcommands (produce / consume / inspect) are in progress.

---

## Installation

Magnetar is not yet on crates.io.
Use the Git path until the first release:

```toml
[dependencies]
magnetar = { git = "https://github.com/CleverCloud/magnetar", branch = "main" }
```

The default feature set enables the tokio engine.
The feature flags catalog:

| Flag                 | Default | Effect                                                                                                                                                                                                                                                                                                                                   |
| -------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tokio`              | yes     | Pulls in `magnetar-runtime-tokio` plus `tokio`/`futures-util`. The public `PulsarClient` lives behind this flag.                                                                                                                                                                                                                         |
| `moonpool`           | no      | Pulls in `magnetar-runtime-moonpool` for deterministic-simulation testing.                                                                                                                                                                                                                                                               |
| `admin`              | no      | Re-exports `magnetar-admin` under `magnetar::admin`.                                                                                                                                                                                                                                                                                     |
| `auth-oauth2`        | no      | Pulls in `magnetar-auth-oauth2` (OAuth2 ClientCredentialsFlow provider).                                                                                                                                                                                                                                                                 |
| `auth-sasl`          | no      | Pulls in `magnetar-auth-sasl` (SASL PLAIN + the sans-io Kerberos surface).                                                                                                                                                                                                                                                               |
| `auth-sasl-kerberos` | no      | Implies `auth-sasl` and turns on `magnetar-auth-sasl/kerberos`, which binds `libgssapi`. Build host needs the MIT KRB5 / Heimdal headers (`krb5-devel` / `libkrb5-dev`) **and** `libclang` (`clang-libs` / `libclang-dev`) — `libgssapi-sys` runs `bindgen` at build time. See [ADR-0029](specs/adr/0029-sasl-kerberos-gssapi-scope.md). |
| `auth-athenz`        | no      | Pulls in `magnetar-auth-athenz`.                                                                                                                                                                                                                                                                                                         |
| `auth-athenz-zts`    | no      | Implies `auth-athenz` and turns on the reqwest-backed ZTS exchange plus in-tree JWT signer support.                                                                                                                                                                                                                                      |
| `encryption`         | no      | Pulls in `magnetar-messagecrypto` plus the PIP-4 bridge type.                                                                                                                                                                                                                                                                            |

<!-- `e2e` and `e2e-multi-cluster` features removed per ADR-0046: the
end-to-end suite runs as a regular `cargo test` with no feature gate and no `#[ignore]`.
Docker on the host is the only prerequisite. -->

| `experimental-v5-client` | no | Enables the PIP-466 V5 wrapper surface (`magnetar::v5`) over the v4 wire commands. |
| `scalable-topics` | no | Enables the experimental PIP-460 scalable-topic scaffold (`topic://`, DAG watch, StreamConsumer, `topic-info`). |
| `crypto-aws-lc-rs` | yes | rustls crypto provider: `aws-lc-rs`; brings post-quantum hybrid KEX (X25519MLKEM768). See [TLS crypto provider](#tls-crypto-provider). |
| `crypto-ring` | no | rustls crypto provider: `ring`. |
| `crypto-openssl` | no | rustls crypto provider: `rustls-openssl` (wraps system OpenSSL via `deny.toml` carve-out). |
| `crypto-fips` | no | rustls crypto provider: aws-lc-rs FIPS-validated module (requires `cmake` + C toolchain). |

The workspace ships eleven crates:

| Crate                       | Role                                                                         |
| --------------------------- | ---------------------------------------------------------------------------- |
| `magnetar`                  | Public façade — re-exports + builder + typed schemas wiring.                 |
| `magnetar-proto`            | Sans-io protocol crate. The heart of the project.                            |
| `magnetar-runtime-tokio`    | Production tokio engine with `tokio-rustls` TLS.                             |
| `magnetar-runtime-moonpool` | Deterministic-simulation engine (rustls-over-bytepipe TLS, no native TLS).   |
| `magnetar-admin`            | REST admin client (`reqwest` + `rustls-tls`).                                |
| `magnetar-cli`              | `magnetar` binary — admin lookups today, produce / consume / inspect coming. |
| `magnetar-fakes`            | In-process broker fake (dev-dep). Mirrors Java's `MockBrokerService`.        |
| `magnetar-auth-oauth2`      | OAuth2 ClientCredentialsFlow auth provider.                                  |
| `magnetar-auth-sasl`        | SASL auth provider.                                                          |
| `magnetar-auth-athenz`      | Athenz auth provider.                                                        |
| `magnetar-messagecrypto`    | PIP-4 end-to-end encryption (AES-GCM via `aws-lc-rs`).                       |

`xtask` is a workspace member but is **not published** — it hosts build helpers (`protoc` codegen, e2e driver, dependency audits).

---

## TLS crypto provider

The rustls crypto backend is selected at compile time via four mutually-pluggable Cargo features on the `magnetar` façade.
The wire protocol — TLS 1.3 (default) / TLS 1.2 — is identical across every provider; what differs is the audited / FIPS-validated / post-quantum posture of the underlying primitives.

| Feature            | Backend         | Post-quantum KEX     | FIPS validated           | Pure Rust | Default |
| ------------------ | --------------- | -------------------- | ------------------------ | --------- | ------- |
| `crypto-aws-lc-rs` | aws-lc-rs       | yes (X25519MLKEM768) | no                       | no (C)    | ✓       |
| `crypto-ring`      | ring            | no                   | no                       | no (C)    |         |
| `crypto-openssl`   | rustls-openssl  | yes                  | depends on OpenSSL build | no        |         |
| `crypto-fips`      | aws-lc-fips-sys | (FIPS-approved only) | yes                      | no (C)    |         |

```bash
# Pick a single provider (mutually exclusive at build time).
cargo build -p magnetar --no-default-features --features tokio,crypto-aws-lc-rs
cargo build -p magnetar --no-default-features --features tokio,crypto-ring
cargo build -p magnetar --no-default-features --features tokio,crypto-openssl   # needs system OpenSSL
cargo build -p magnetar --no-default-features --features tokio,crypto-fips      # needs cmake + C toolchain
```

Under `cargo build --workspace --all-features` the compile-time cfg cascade resolves to aws-lc-rs (highest priority).
Single-provider builds go through `cargo run -p xtask -- check-crypto-matrix`.
A single `compile_error!` fires if no `crypto-*` feature is enabled.

The `crypto-aws-lc-rs` default picks up rustls 0.23's built-in `prefer-post-quantum` feature, so the wire client negotiates the X25519MLKEM768 hybrid key exchange with brokers that support it.

`openssl` / `openssl-sys` are admitted only as transitive deps of `rustls-openssl`; the rest of [ADR-0005](specs/adr/0005-rustls-only-tls.md) (no `native-tls`, rustls everywhere) stays in force.
See [ADR-0035](specs/adr/0035-pluggable-crypto-provider.md) for the binding decision.

---

## Build metadata (`magnetar --version`)

The `magnetar` binary exposes a sozu / systemd-style identification banner:

```
$ magnetar --version
magnetar 0.1.0-dev.0 (a1b2c3d4e5f6-dirty)
built 2026-05-26T14:32:11Z · profile=release · rustc=rustc 1.88.0 (…) · target=x86_64-unknown-linux-gnu
features: +default
pulsar wire protocol: v21
os: linux · report bugs at https://github.com/CleverCloud/magnetar
```

- `-V` prints a single-line, never-colorized form: `magnetar 0.1.0-dev.0 (sha-dirty)`.
- `--version` prints the multi-line form above, colorized on a TTY.
  `NO_COLOR=1` or piping suppresses ANSI (https://no-color.org).
- `SOURCE_DATE_EPOCH=<unix-seconds>` pins the build timestamp for reproducible builds.

Full reference: [`docs/cli.md`](docs/cli.md).

---

## Quickstart

The high-level `PulsarClient` builder is the public entry point.
It wires the tokio engine to the sans-io state machine and gives you producer / consumer / reader / table-view / partitioned / multi-topics / pattern builders.

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

A check (`✅`) is a working public-API surface backed by code in the workspace.
A flag (`🟡`) means partial — a working subset; check [ARCHITECTURE.md](ARCHITECTURE.md) for the open gaps.
A cross (`❌`) is a known-missing feature.

### Producer

| Feature                                                  | Java | Magnetar | Notes                                                                                                                                                                            |
| -------------------------------------------------------- | ---- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `send(...)` / `sendAsync(...)`                           | ✅   | ✅       | `Producer::send` returns a `SendFut`.                                                                                                                                            |
| Producer name                                            | ✅   | ✅       | `ProducerBuilder::name`.                                                                                                                                                         |
| Compression (LZ4, ZSTD, Snappy, ZLIB, NONE)              | ✅   | ✅       | `ProducerBuilder::compression`.                                                                                                                                                  |
| Batching (`BatchMessageContainerImpl`)                   | ✅   | ✅       | `ProducerBuilder::batching` (max-msgs + max-bytes).                                                                                                                              |
| `batchingMaxPublishDelay` flush timer                    | ✅   | ✅       | `ProducerBuilder::batching_max_publish_delay`.                                                                                                                                   |
| Chunking (PIP-37)                                        | ✅   | ✅       | `ProducerBuilder::chunking`. Chunks-never-batched enforced.                                                                                                                      |
| `initialSequenceId`                                      | ✅   | ✅       | `ProducerBuilder::initial_sequence_id`.                                                                                                                                          |
| `sendTimeout`                                            | ✅   | ✅       | `ProducerBuilder::send_timeout`.                                                                                                                                                 |
| `accessMode` (Shared/Exclusive/WaitForExclusive/Fencing) | ✅   | ✅       | `ProducerBuilder::access_mode`. PIP-68.                                                                                                                                          |
| `accessMode` getter                                      | ✅   | ✅       | `Producer::access_mode`.                                                                                                                                                         |
| `getProducerName`                                        | ✅   | ✅       | `Producer::name`.                                                                                                                                                                |
| `getTopic`                                               | ✅   | ✅       | `Producer::topic`.                                                                                                                                                               |
| `isConnected` / `isClosed`                               | ✅   | ✅       | `Producer::is_connected` / `is_closed`.                                                                                                                                          |
| `getLastSequenceId`                                      | ✅   | ✅       | `Producer::last_sequence_id`.                                                                                                                                                    |
| `getLastSequenceIdPublished`                             | ✅   | ✅       | `Producer::last_sequence_id_published`.                                                                                                                                          |
| `getLastDisconnectedTimestamp`                           | ✅   | ✅       | `Producer::last_disconnected_timestamp`.                                                                                                                                         |
| `flush()`                                                | ✅   | ✅       | `Producer::flush`.                                                                                                                                                               |
| `close()`                                                | ✅   | ✅       | `Producer::close`.                                                                                                                                                               |
| `getStats`                                               | ✅   | ✅       | `Producer::stats` — counters + `send_latency_{p50,p99,max}_ms` via `hdrhistogram` + rolling per-second `msgs_per_sec` / `bytes_per_sec` windows (`producer_record_rate_window`). |
| `getCompressionType` getter                              | ✅   | ✅       | `Producer::compression`.                                                                                                                                                         |
| Per-message `key` / `orderingKey`                        | ✅   | ✅       | `OutgoingMessage::key` / `ordering_key`.                                                                                                                                         |
| Per-message `eventTime`                                  | ✅   | ✅       | `OutgoingMessage::event_time_ms`.                                                                                                                                                |
| `deliverAt` / `deliverAfter`                             | ✅   | ✅       | `OutgoingMessage::deliver_at_ms` / `deliver_after_ms`.                                                                                                                           |
| `replicationClusters` + `disableReplication`             | ✅   | ✅       | `OutgoingMessage::replication_clusters` / `disable_replication`.                                                                                                                 |
| `newMessage(Transaction)` (PIP-31)                       | ✅   | ✅       | `OutgoingMessage::txn(txn_id)`.                                                                                                                                                  |
| `Properties` (per-message key/value)                     | ✅   | ✅       | `OutgoingMessage::property`.                                                                                                                                                     |
| `TypedMessageBuilder`                                    | ✅   | ✅       | `MessageBuilder` via `ProducerExt::new_message`.                                                                                                                                 |
| `ProducerInterceptor` SPI                                | ✅   | ✅       | `magnetar::ProducerInterceptor` + `send_with_interceptors`.                                                                                                                      |
| `pendingQueueSize` getter                                | ✅   | ✅       | `Producer::pending_count` (`batch_len` + `batch_bytes` are bonus).                                                                                                               |

### Consumer

| Feature                                                               | Java | Magnetar | Notes                                                                                                                                                                                                                                                                                                    |
| --------------------------------------------------------------------- | ---- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `subscribe(...)` (`Exclusive` / `Shared` / `Failover` / `Key_Shared`) | ✅   | ✅       | `ConsumerBuilder::subscription_type`.                                                                                                                                                                                                                                                                    |
| `receive` / `receiveAsync` / `receive(timeout)`                       | ✅   | ✅       | `Consumer::receive` + `receive_with_timeout`.                                                                                                                                                                                                                                                            |
| `batchReceive` / `batchReceiveAsync`                                  | ✅   | ✅       | `Consumer::receive_batch_with_bytes_cap` (cap on count + bytes).                                                                                                                                                                                                                                         |
| `acknowledge` (individual)                                            | ✅   | ✅       | `Consumer::ack`.                                                                                                                                                                                                                                                                                         |
| `acknowledgeCumulative`                                               | ✅   | ✅       | `Consumer::ack_cumulative`.                                                                                                                                                                                                                                                                              |
| `acknowledge(messages)` (batch ack)                                   | ✅   | ✅       | `Consumer::ack_batch`.                                                                                                                                                                                                                                                                                   |
| `acknowledge(MessageId, Map<String,String>)`                          | ✅   | ✅       | `Consumer::ack_with_properties`.                                                                                                                                                                                                                                                                         |
| `acknowledge(MessageId, Transaction)` (PIP-31)                        | ✅   | ✅       | `Consumer::ack_with_txn`.                                                                                                                                                                                                                                                                                |
| `acknowledgeAsync(messages, Transaction)`                             | ✅   | ✅       | `Consumer::ack_batch_with_txn`.                                                                                                                                                                                                                                                                          |
| `acknowledgeCumulative(MessageId, Map)`                               | ✅   | ✅       | `Consumer::ack_cumulative_with_properties`.                                                                                                                                                                                                                                                              |
| `acknowledgeCumulative(MessageId, Transaction)`                       | ✅   | ✅       | `Consumer::ack_cumulative_with_txn`.                                                                                                                                                                                                                                                                     |
| Batch-index ACK (PIP-54)                                              | ✅   | ✅       | `ack_set` bitset stamped on individual acks.                                                                                                                                                                                                                                                             |
| `acknowledgmentGroupTime` (grouping window)                           | ✅   | ✅       | `ConsumerBuilder::ack_group_time` + `ack_grouped` / `ack_grouped_cumulative`.                                                                                                                                                                                                                            |
| `negativeAcknowledge`                                                 | ✅   | ✅       | `Consumer::negative_ack`.                                                                                                                                                                                                                                                                                |
| `negativeAcknowledge(messages)`                                       | ✅   | ✅       | `Consumer::negative_ack_batch`.                                                                                                                                                                                                                                                                          |
| `negativeAcknowledge(MessageId, delay)`                               | ✅   | ✅       | `Consumer::negative_ack_with_delay`.                                                                                                                                                                                                                                                                     |
| `MultiplierRedeliveryBackoff` (PIP-37)                                | ✅   | ✅       | `magnetar_proto::trackers::MultiplierRedeliveryBackoff`.                                                                                                                                                                                                                                                 |
| `reconsumeLater` (retry-letter topic)                                 | ✅   | ✅       | `Consumer::reconsume_later` + `_with_properties`.                                                                                                                                                                                                                                                        |
| `ackTimeout` (unacked tracker)                                        | ✅   | ✅       | `ConsumerBuilder::ack_timeout`.                                                                                                                                                                                                                                                                          |
| `ackTimeoutRedeliveryBackoff` (PIP-37)                                | ✅   | ✅       | `ConsumerBuilder::ack_timeout_backoff`.                                                                                                                                                                                                                                                                  |
| `negativeAckRedeliveryDelay`                                          | ✅   | ✅       | `ConsumerBuilder::negative_ack_redelivery_delay`.                                                                                                                                                                                                                                                        |
| `seek(MessageId)`                                                     | ✅   | ✅       | `Consumer::seek`.                                                                                                                                                                                                                                                                                        |
| `seek(timestamp)`                                                     | ✅   | ✅       | `Consumer::seek_timestamp`.                                                                                                                                                                                                                                                                              |
| `seekAsync(Function<String, Object>)` (per-partition)                 | ✅   | ✅       | `PartitionedConsumer::seek_per_partition` / `MultiTopicsConsumer::seek_per_partition` — callback returns `SeekTarget::MessageId` or `SeekTarget::PublishTimeMs` per topic.                                                                                                                               |
| `seekToEarliest` / `seekToLatest`                                     | ✅   | ✅       | `Consumer::seek_to_earliest` / `seek_to_latest`.                                                                                                                                                                                                                                                         |
| `pause()` / `resume()` / `isPaused()`                                 | ✅   | ✅       | `Consumer::pause` / `resume` / `is_paused`.                                                                                                                                                                                                                                                              |
| `hasReachedEndOfTopic`                                                | ✅   | ✅       | `Consumer::has_reached_end_of_topic`.                                                                                                                                                                                                                                                                    |
| `redeliverUnacknowledgedMessages`                                     | ✅   | ✅       | `Consumer::redeliver_unacked`.                                                                                                                                                                                                                                                                           |
| `getLastMessageId`                                                    | ✅   | ✅       | `Consumer::last_message_id`.                                                                                                                                                                                                                                                                             |
| `getStats` (counters)                                                 | ✅   | ✅       | `Consumer::stats`. Includes `total_chunked_msgs_received`.                                                                                                                                                                                                                                               |
| Stats: rolling windows (msgs/sec, bytes/sec)                          | ✅   | ✅       | `ConsumerStats::msgs_per_sec` / `bytes_per_sec` + `ProducerStats` same. Runtime calls `Connection::consumer_record_rate_window(handle, now)` / `producer_record_rate_window` on a `tokio::time::interval` ticker; first call records baseline, subsequent calls compute per-second rates from the delta. |
| Stats: latency hdrhistogram (p50/p99/max)                             | ✅   | ✅       | `Consumer::stats` exposes `receive_latency_{p50,p99,max}_ms`; `Producer::stats` exposes `send_latency_{p50,p99,max}_ms`.                                                                                                                                                                                 |
| `subscriptionProperties`                                              | ✅   | ✅       | `ConsumerBuilder::subscription_property`.                                                                                                                                                                                                                                                                |
| `replicateSubscriptionState`                                          | ✅   | ✅       | `ConsumerBuilder::replicate_subscription_state`.                                                                                                                                                                                                                                                         |
| `priorityLevel`                                                       | ✅   | ✅       | `ConsumerBuilder::priority_level`.                                                                                                                                                                                                                                                                       |
| `keySharedPolicy` (sticky / auto-split / hash)                        | ✅   | ✅       | `ConsumerBuilder::key_shared_policy`. PIP-34/119/282/379.                                                                                                                                                                                                                                                |
| `startMessageId`                                                      | ✅   | ✅       | `ConsumerBuilder::start_message_id`.                                                                                                                                                                                                                                                                     |
| `startMessageRollbackDuration`                                        | ✅   | ✅       | `ConsumerBuilder::start_message_rollback_duration`.                                                                                                                                                                                                                                                      |
| `readCompacted`                                                       | ✅   | ✅       | `ConsumerBuilder::read_compacted`.                                                                                                                                                                                                                                                                       |
| `forceTopicCreation`                                                  | ✅   | ✅       | `ConsumerBuilder::force_topic_creation`.                                                                                                                                                                                                                                                                 |
| Dead-letter policy                                                    | ✅   | ✅       | `ConsumerBuilder::dead_letter_policy` + `Consumer::drain_dead_letter`. PIP-22/58/124/409.                                                                                                                                                                                                                |
| `cryptoFailureAction` (PIP-4)                                         | ✅   | ✅       | `Fail` / `Discard` / `Consume` all wired end-to-end in `magnetar-runtime-tokio::consumer::deliver_post_process`.                                                                                                                                                                                         |
| Encryption (PIP-4)                                                    | ✅   | ✅       | `ConsumerBuilder::encryption` accepts a `MessageDecryptor`.                                                                                                                                                                                                                                              |
| `ConsumerInterceptor` SPI                                             | ✅   | ✅       | `magnetar::ConsumerInterceptor` + `receive_with_interceptors`.                                                                                                                                                                                                                                           |
| `unsubscribe()`                                                       | ✅   | ✅       | Consumer / multi-topics expose unsubscribe.                                                                                                                                                                                                                                                              |
| `forceUnsubscribe` (PIP-313)                                          | ✅   | ✅       | Wired through `CommandUnsubscribe.force`.                                                                                                                                                                                                                                                                |
| `availablePermits` getter                                             | ✅   | ✅       | `Consumer::available_permits`.                                                                                                                                                                                                                                                                           |
| `availableInQueue` getter                                             | ✅   | ✅       | `Consumer::available_in_queue`.                                                                                                                                                                                                                                                                          |
| `hasReceivedAnyMessage` getter                                        | ✅   | ✅       | `Consumer::has_received_any_message`.                                                                                                                                                                                                                                                                    |

### Partitioned producer

| Feature                                                      | Java | Magnetar | Notes                                                                                                                                                                                               |
| ------------------------------------------------------------ | ---- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Auto partition discovery                                     | ✅   | ✅       | `PulsarClient::partitions_for_topic` + builder.                                                                                                                                                     |
| `MessageRoutingMode` (RoundRobin / SinglePartition / Custom) | ✅   | ✅       | `MessageRoutingMode`.                                                                                                                                                                               |
| Custom `MessageRouter` trait                                 | ✅   | ✅       | `MessageRouter` trait + `message_router(...)`.                                                                                                                                                      |
| Murmur3 + JavaStringHash hashers                             | ✅   | ✅       | `Murmur3HashHasher` / `JavaStringHashHasher`.                                                                                                                                                       |
| `TypedMessageBuilder`-equivalent on partitioned producer     | ✅   | ✅       | `PartitionedMessageBuilder`.                                                                                                                                                                        |
| Per-partition stats / `lastSequenceId`                       | ✅   | ✅       | Aggregated across child producers.                                                                                                                                                                  |
| Auto-update partition count (background ticker)              | ✅   | ✅       | `PartitionedProducerBuilder::auto_update_partitions_interval` spawns a `tokio::time::interval` that signals `partitions_changed_notify`; user drives `refresh_partitions(&client)` from the signal. |

### Partitioned consumer

| Feature                                                     | Java | Magnetar | Notes                                                                                                                                    |
| ----------------------------------------------------------- | ---- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Auto partition discovery + one consumer per partition       | ✅   | ✅       | `PulsarClient::partitioned_consumer`.                                                                                                    |
| Full `ConsumerBuilder` knob forwarding                      | ✅   | ✅       | 12 knobs forwarded from builder.                                                                                                         |
| Receive / ack / nack / seek / unsubscribe across partitions | ✅   | ✅       | All forwarded.                                                                                                                           |
| Auto-update partition count                                 | ✅   | ✅       | `PartitionedConsumerBuilder::auto_update_partitions_interval` mirrors the producer pattern; signal drives `refresh_partitions(&client)`. |

### Multi-topics consumer

| Feature                                               | Java | Magnetar | Notes                                                                                                                                                                                                                  |
| ----------------------------------------------------- | ---- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Subscribe to N explicit topics under one subscription | ✅   | ✅       | `MultiTopicsConsumerBuilder::topics`.                                                                                                                                                                                  |
| Receive / ack / nack / seek across all topics         | ✅   | ✅       | Per-topic forwarding.                                                                                                                                                                                                  |
| `negativeAckWithDelay` / `ackCumulative`              | ✅   | ✅       | Forwarded.                                                                                                                                                                                                             |
| Dynamic `add_topic` / `remove_topic`                  | ✅   | ✅       | `MultiTopicsConsumer::add_topic` / `remove_topic` — subscribe / unsubscribe at runtime.                                                                                                                                |
| Auto-update partition count (background ticker)       | ✅   | ✅       | `MultiTopicsConsumerBuilder::auto_update_partitions_interval` spawns a `tokio::time::interval` that signals `partitions_changed_notify`; user drives `refresh_partitions(&client)` + `add_topic(...)` from the signal. |

### Pattern consumer (PIP-145)

| Feature                         | Java | Magnetar | Notes                                                                                                                                                                                  |
| ------------------------------- | ---- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Regex topic subscription        | ✅   | ✅       | `PatternConsumerBuilder::pattern`.                                                                                                                                                     |
| `TopicListChanged` delta stream | ✅   | ✅       | `Client::next_topic_list_change`.                                                                                                                                                      |
| Manual `update()` reconcile     | ✅   | ✅       | `PatternConsumer::update(&client)` returns a `ReconcileReport`.                                                                                                                        |
| Auto-update background ticker   | ✅   | ✅       | `PatternConsumer::start_auto_reconcile(client, interval)` spawns a `tokio::time::interval` loop that calls `update(&client)` on every tick; returns a `JoinHandle` for clean shutdown. |

### Reader

| Feature                                         | Java | Magnetar | Notes                                                           |
| ----------------------------------------------- | ---- | -------- | --------------------------------------------------------------- |
| Non-durable exclusive subscription              | ✅   | ✅       | `ReaderBuilder` builds on `ConsumerBuilder`.                    |
| `startMessageId` (Earliest / Latest / explicit) | ✅   | ✅       | `ReaderBuilder::start_message_id`.                              |
| `startMessageIdInclusive` rollback duration     | ✅   | ✅       | `ReaderBuilder::start_message_rollback_duration`.               |
| `readCompacted`                                 | ✅   | ✅       | `ReaderBuilder::read_compacted`.                                |
| `cryptoKeyReader` (PIP-4 decryptor)             | ✅   | ✅       | `ReaderBuilder::encryption`.                                    |
| `hasMessageAvailable` / `seek`                  | ✅   | ✅       | Via the underlying consumer surface.                            |
| Stats / closure getters (`isClosed`, etc.)      | ✅   | ✅       | `Reader::is_closed`, `available_in_queue`, `available_permits`. |

### Table view

| Feature                                                      | Java | Magnetar | Notes                                                                                                                                                                                                     |
| ------------------------------------------------------------ | ---- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Compacted-topic snapshot keyed by message key                | ✅   | ✅       | `TableView::get` / `for_each` / `snapshot` / `keys` / `values`.                                                                                                                                           |
| Listener registration                                        | ✅   | ✅       | `TableView::listen` (`TableViewListener`).                                                                                                                                                                |
| Schema-aware `TypedTableView`                                | ✅   | ✅       | `TypedTableView<S>` decodes per-read.                                                                                                                                                                     |
| `startMessageId` / `subscriptionProperty` / `property` knobs | ✅   | ✅       | `TableViewBuilder` knob set.                                                                                                                                                                              |
| Auto-update-partitions ticker                                | ✅   | ✅       | `TableViewBuilder::auto_update_partitions_interval(Duration)` spawns a background timer that signals `TableView::partitions_changed_notify`; callers drive `refresh_partitions(&client)` from the signal. |
| `cryptoKeyReader` wired through                              | ✅   | ✅       | `TableViewBuilder::encryption` + `TypedTableViewBuilder::encryption` stamp the decryptor onto the underlying `ConsumerBuilder`.                                                                           |

### Transactions (PIP-31)

| Feature                                                  | Java | Magnetar | Notes                                                       |
| -------------------------------------------------------- | ---- | -------- | ----------------------------------------------------------- |
| Transaction coordinator client                           | ✅   | ✅       | `magnetar-proto::txn::TxnClient`.                           |
| Begin / commit / abort                                   | ✅   | ✅       | `Client::new_txn` + `Transaction::commit` / `abort`.        |
| `ADD_PARTITION_TO_TXN` / `ADD_SUBSCRIPTION_TO_TXN`       | ✅   | ✅       | `Client::add_partition_to_txn` / `add_subscription_to_txn`. |
| Producer publish under txn                               | ✅   | ✅       | `OutgoingMessage::txn`.                                     |
| Consumer ack under txn (individual + cumulative + batch) | ✅   | ✅       | `Consumer::ack_with_txn` and friends.                       |
| `END_TXN_ON_PARTITION` / `_ON_SUBSCRIPTION` cleanup      | ✅   | ✅       | Driven by `end_txn`.                                        |

### Auth + TLS

| Feature                                             | Java | Magnetar                   | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| --------------------------------------------------- | ---- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Token auth                                          | ✅   | ✅                         | `magnetar_proto::auth::TokenAuth`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| mTLS                                                | ✅   | ✅                         | `magnetar_proto::auth::TlsAuth` + `tls_trust_certs_pem` / `tls_trust_certs_file_path`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| OAuth2 ClientCredentialsFlow                        | ✅   | ✅                         | `magnetar_auth_oauth2::ClientCredentialsFlow` — POSTs `grant_type=client_credentials` to the IDP, caches the JWT, refreshes within 30 s of expiry. Reports `auth_method_name = "token"`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| SASL `PLAIN` (RFC 4616)                             | ✅   | ✅                         | `magnetar_auth_sasl::SaslPlain` — `\0<username>\0<password>` payload.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| SASL Kerberos / GSSAPI                              | ✅   | ✅                         | `magnetar_auth_sasl::SaslKerberos` runs the GSSAPI initiate loop via `libgssapi` (façade feature `auth-sasl-kerberos`). The multi-round `AUTH_CHALLENGE` / `AUTH_RESPONSE` exchange threads through `AuthProvider::respond_to_challenge`; the four sans-io test layers per [ADR-0024](specs/adr/0024-cross-runtime-test-and-coverage-policy.md) drive a `magnetar_auth_sasl::ScriptedGssapiClient` so they stay free of a libkrb5 build dep. End-to-end coverage uses a Dockerised KDC fixture. See [ADR-0029](specs/adr/0029-sasl-kerberos-gssapi-scope.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| Athenz (pre-fetched role token)                     | ✅   | ✅                         | `AthenzProvider::with_role_token` — bypass the ZTS round-trip when the caller already holds a valid role token.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| Athenz (ZTS round-trip)                             | ✅   | ✅                         | `feature = "auth-athenz-zts"` (default off). The pluggable `zts::ZtsClient` trait (`zts::HttpZtsClient` does the reqwest-backed POST) exchanges a signed JWT for a role token; `AthenzProvider` owns the expiry-aware cache and `ensure_role_token(now)` / `needs_refresh(now)` (sans-io clock injection). Build via `AthenzProvider::with_default_signer(config)` (cfg-active in-tree signer) or `AthenzProvider::builder()` (custom signer / client / `wall_clock`). The concrete `zts::JwtSigner` ships in two flavours — `jwt_signer::AwsLcRsSigner` and `jwt_signer::RingSigner` — gated on the crypto-provider matrix per [ADR-0035](specs/adr/0035-pluggable-crypto-provider.md); parsed PKCS#8 DER wrapped in `zeroize::Zeroizing<…>`, byte-identical deterministic RS256 (RFC 8017 §8.2). Full four-layer cross-runtime coverage (tokio/moonpool/differential + e2e) per [ADR-0024](specs/adr/0024-cross-runtime-test-and-coverage-policy.md). See [`docs/pip-features.md#athenz-auth-provider`](docs/pip-features.md#athenz-auth-provider) and [ADR-0041](specs/adr/0041-athenz-provider-testability-seams.md). |
| In-band `AUTH_CHALLENGE` refresh (PIP-30 / PIP-292) | ✅   | ✅                         | Driver consults the configured `AuthProvider` and submits `CommandAuthResponse`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `pulsar+ssl://` URLs                                | ✅   | ✅                         | Built-in.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| Binary proxy (`proxy_to_broker_url`)                | ✅   | ✅ (tokio) / 🟡 (moonpool) | `ClientBuilder::proxy_to_broker_url`. Tokio ships the full `ProxyConnectionPool` ([ADR-0039](specs/adr/0039-pulsar-proxy-multi-broker-connection-model.md)); moonpool lookup returns `ProxyUnsupportedOnUnsupervisedClient` until the moonpool pool variant lands ([`docs/follow-ups.md §3`](docs/follow-ups.md#3-moonpool-proxyconnectionpool-parity)).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |

### Encryption (PIP-4)

| Feature                              | Java                         | Magnetar | Notes                                                                                                            |
| ------------------------------------ | ---------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------- |
| `MessageEncryptor` trait on producer | ✅                           | ✅       | `ProducerBuilder::encryption`.                                                                                   |
| `MessageDecryptor` trait on consumer | ✅                           | ✅       | `ConsumerBuilder::encryption`.                                                                                   |
| AES-GCM via `aws-lc-rs`              | n/a (Java uses BouncyCastle) | ✅       | `magnetar-messagecrypto::MessageCrypto`.                                                                         |
| `cryptoFailureAction`                | ✅                           | ✅       | `Fail` / `Discard` / `Consume` all wired end-to-end in `magnetar-runtime-tokio::consumer::deliver_post_process`. |

### Schemas

| Feature                                                                   | Java | Magnetar | Notes                                                                                                                                                                      |
| ------------------------------------------------------------------------- | ---- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BytesSchema`                                                             | ✅   | ✅       |                                                                                                                                                                            |
| `StringSchema`                                                            | ✅   | ✅       |                                                                                                                                                                            |
| `JsonSchema`                                                              | ✅   | ✅       | Canonicalised via the Avro parser per Codex Q4.                                                                                                                            |
| `AvroSchema`                                                              | ✅   | ✅       | `apache-avro` 0.21 — canonical-parsing form.                                                                                                                               |
| `ProtobufSchema` (descriptor)                                             | ✅   | ✅       |                                                                                                                                                                            |
| `ProtobufNativeSchema`                                                    | ✅   | ✅       | Byte-identical Java `FileDescriptorSet` output.                                                                                                                            |
| `KeyValueSchema`                                                          | ✅   | ✅       | Byte-identical canonical JSON wrapper.                                                                                                                                     |
| `AutoConsumeSchema` (broker lookup)                                       | ✅   | ✅       | `TypedConsumer::receive` auto-fetches the broker schema on first call via `Connection::get_schema`; the result is cached on the schema's `Arc<Mutex<Option<pb::Schema>>>`. |
| `AutoProduceBytesSchema`                                                  | ✅   | ✅       | `TypedProducer::send` warms the broker schema on first send via `Producer::get_schema`; `encode()` stays pass-through per Java parity.                                     |
| Int8 / Int16 / Int32 / Int64 / Float / Double / Bool                      | ✅   | ✅       |                                                                                                                                                                            |
| Date / Time / Timestamp / LocalDate / LocalTime / Instant / LocalDateTime | ✅   | ✅       |                                                                                                                                                                            |
| Schema-version negotiation                                                | ✅   | ✅       | Sent on `CommandProducer` / `CommandSubscribe`.                                                                                                                            |

### Client builder

| Feature                                                  | Java | Magnetar                   | Notes                                                                                                                                                                                                                                                                                      |
| -------------------------------------------------------- | ---- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `serviceUrl`                                             | ✅   | ✅                         | `ClientBuilder::service_url`.                                                                                                                                                                                                                                                              |
| `clientVersion`                                          | ✅   | ✅                         | `ClientBuilder::client_version`.                                                                                                                                                                                                                                                           |
| `keepAliveInterval`                                      | ✅   | ✅                         | `ClientBuilder::keepalive`.                                                                                                                                                                                                                                                                |
| `operationTimeout`                                       | ✅   | ✅                         | `ClientBuilder::operation_timeout`.                                                                                                                                                                                                                                                        |
| `maxMessageSize`                                         | ✅   | ✅                         | `ClientBuilder::max_message_size`.                                                                                                                                                                                                                                                         |
| `tlsTrustCertsFilePath`                                  | ✅   | ✅                         | `ClientBuilder::tls_trust_certs_file_path`.                                                                                                                                                                                                                                                |
| `tlsAllowInsecureConnection`                             | ✅   | ✅                         | `ClientBuilder::tls_allow_insecure_connection(true)` — accepts any server cert via a custom rustls verifier. **Insecure**, do not use in production.                                                                                                                                       |
| `enableTlsHostnameVerification`                          | ✅   | ✅                         | `ClientBuilder::tls_hostname_verification_enable(bool)` — `true` uses the standard WebPKI verifier; `false` paired with `tls_trust_certs_pem` routes through `magnetar_runtime_tokio::tls_config_no_hostname` which delegates chain check to WebPKI and intercepts only `NotValidForName`. |
| `serviceUrlProvider` (URL rotation)                      | ✅   | ✅                         | `ClientBuilder::service_url_provider(Arc<dyn ServiceUrlProvider>)` — the supervised reconnect path calls `provider.get_service_url()` on every reconnect attempt so cluster-failover policies can swap URLs between attempts.                                                              |
| `proxyServiceUrl` (binary proxy)                         | ✅   | ✅ (tokio) / 🟡 (moonpool) | `ClientBuilder::proxy_to_broker_url`. Tokio-only `ProxyConnectionPool` ([ADR-0039](specs/adr/0039-pulsar-proxy-multi-broker-connection-model.md)); moonpool variant tracked in [`docs/follow-ups.md §3`](docs/follow-ups.md#3-moonpool-proxyconnectionpool-parity).                        |
| `Authentication` plugin                                  | ✅   | ✅                         | `ClientBuilder::auth(Arc<dyn AuthProvider>)`.                                                                                                                                                                                                                                              |
| `memoryLimit`                                            | ✅   | ✅                         | `ClientBuilder::memory_limit(bytes, MemoryLimitPolicy)`. Both `FailImmediately` (atomic CAS, [ADR-0017](specs/adr/0017-memory-limit-atomic-reservation.md)) and `ProducerBlock` (Waker slab, [ADR-0020](specs/adr/0020-memory-limit-producer-block.md)) ship.                              |
| `dnsResolver` customisation                              | ✅   | ✅                         | `ClientBuilder::dns_resolver(Arc<dyn DnsResolver>)` — `Transport::connect_with_resolver` resolves via the provider on every (re)connect; `TokioDnsResolver` is the default.                                                                                                                |
| `isClosed` / `shutdown` / `getLastDisconnectedTimestamp` | ✅   | ✅                         | All exposed on `PulsarClient`.                                                                                                                                                                                                                                                             |
| Cluster failover (PIP-121)                               | ✅   | ✅                         | `ServiceUrlProvider` + `StaticServiceUrlProvider` + `ControlledClusterFailover` (proto) + `AutoClusterFailover` (runtime, with user-supplied `HealthProbe` callback + background tokio task). All three plug into `ClientBuilder::service_url_provider`.                                   |

### Engine-by-engine surface coverage

Per [ADR-0019](specs/adr/0019-engine-scope-and-moonpool-parity.md) the Java parity matrix above is satisfied **by the tokio engine**.
The moonpool engine reaches feature parity with tokio on its own follow-up train; the table below cuts the matrix engine-by-engine.

| Surface                                                                                                                                                            | tokio                                                                                                    | moonpool                                                                                                                                                                                                                  |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Engine driver loop + transport scaffold                                                                                                                            | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| Vectored producer-batch writes ([ADR-0040](specs/adr/0040-vectored-io-transmit-enum.md))                                                                           | ✅ (`writev` on plaintext)                                                                               | ✅ (real `futures` `write_vectored`: segment-granular under `SimProviders`, single-write fallback under `TokioProviders`' `Compat`; TLS contiguous — [ADR-0043](specs/adr/0043-temporary-floating-moonpool-git-dep.md))   |
| Client (lookup + partitioned-metadata + topic-watch)                                                                                                               | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| Producer façade (send / flush / close)                                                                                                                             | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| Consumer façade (subscribe / receive / ack)                                                                                                                        | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| PIP-4 message encryption + decryption (AES-GCM) with `CryptoFailureAction` ([ADR-0044](specs/adr/0044-moonpool-message-crypto-bridge.md))                          | ✅                                                                                                       | ✅ (encrypt-on-send / decrypt-on-receive bridge; Fail / Discard / Consume arms; equivalence in `magnetar-differential`)                                                                                                   |
| Supervised reconnect (Stage 2 + Stage 3 rebuild)                                                                                                                   | ✅                                                                                                       | ✅ (multi-cycle redial coverage via `supervised_redial.rs`)                                                                                                                                                               |
| DNS resolver injection ([ADR-0015](specs/adr/0015-dns-resolver-injection.md))                                                                                      | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| Driver-level TLS (rustls byte-pipe — [ADR-0006](specs/adr/0006-moonpool-tls-byte-pipe.md))                                                                         | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| `memory_limit` atomic-CAS reservation ([ADR-0017](specs/adr/0017-memory-limit-atomic-reservation.md))                                                              | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| `MemoryLimitPolicy::ProducerBlock` ([ADR-0020](specs/adr/0020-memory-limit-producer-block.md), [ADR-0022](specs/adr/0022-memory-limit-producer-block-moonpool.md)) | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| `ServiceUrlProvider` + `ControlledClusterFailover` ([ADR-0016](specs/adr/0016-pip-121-cluster-failover.md))                                                        | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| `AutoClusterFailover` (PIP-121 with `HealthProbe`)                                                                                                                 | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| PIP-188 `TOPIC_MIGRATED` → reconnect ([ADR-0018](specs/adr/0018-pip-188-reconnect-on-migrate.md))                                                                  | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| Generic `PulsarClient<E: Engine>` ([ADR-0019](specs/adr/0019-engine-scope-and-moonpool-parity.md))                                                                 | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| Partitioned producer                                                                                                                                               | ✅                                                                                                       | ✅ (engine-generic; tokio-only `refresh_partitions` + batch counters on specialisation)                                                                                                                                   |
| Partitioned consumer                                                                                                                                               | ✅                                                                                                       | ✅ (engine-generic via `MultiTopicsConsumer<C>` alias + `PartitionedConsumerBuilder<'a, E>`)                                                                                                                              |
| MultiTopicsConsumer                                                                                                                                                | ✅                                                                                                       | ✅ (engine-generic `MultiTopicsConsumer<C>` + `MultiTopicsConsumerBuilder<'a, E>`)                                                                                                                                        |
| PatternConsumer (PIP-145)                                                                                                                                          | ✅                                                                                                       | ✅ (engine-generic `PatternConsumer<C>` + `PatternConsumerBuilder<'a, E>`; PIP-145 child-subscribe via `<E::ClientState as SubscribeApi>::subscribe`)                                                                     |
| Reader                                                                                                                                                             | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| TableView                                                                                                                                                          | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| Transactions (PIP-31)                                                                                                                                              | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| Typed schemas                                                                                                                                                      | ✅                                                                                                       | ✅                                                                                                                                                                                                                        |
| Binary proxy (`proxy_to_broker_url`, [ADR-0039](specs/adr/0039-pulsar-proxy-multi-broker-connection-model.md))                                                     | ✅ (`ProxyConnectionPool` pins per-broker connections, avoids the ~90 ms reconnect storm from issue #15) | 🟡 (moonpool lookup path returns `ProxyUnsupportedOnUnsupervisedClient`; the moonpool flavour of `ProxyConnectionPool` is tracked in [`docs/follow-ups.md §3`](docs/follow-ups.md#3-moonpool-proxyconnectionpool-parity)) |
| Deterministic chaos pack                                                                                                                                           | n/a                                                                                                      | ✅                                                                                                                                                                                                                        |
| tokio ↔ moonpool differential equivalence harness                                                                                                                  | n/a                                                                                                      | ✅                                                                                                                                                                                                                        |

`MoonpoolEngine<P>` is generic over the [`moonpool_core::Providers`](https://crates.io/crates/moonpool-core) bundle.
`TokioProviders` runs it against a real broker; `moonpool-sim`'s `SimProviders` runs it under deterministic seeds ([`docs/moonpool-engine.md`](docs/moonpool-engine.md)).

All dependent façade surfaces are lifted per ADR-0026 §D1 and now work on both engines: Transaction (PIP-31) via `TransactionApi`; Reader via `Reader<C: ConsumerApi>`; TableView via `TableView<C: ConsumerApi + Clone>`; PartitionedProducer via `impl<P: ProducerApi> PartitionedProducer<P>` (with tokio-only specialisation for `refresh_partitions`, batch counters, `last_sequence_id_published`); MultiTopicsConsumer / PartitionedConsumer via `MultiTopicsConsumer<C: ConsumerApi>` + builders; PatternConsumer (PIP-145) via `PatternConsumer<C: ConsumerApi>` (auto-reconcile subscribes child consumers through `SubscribeApi` and polls `TopicListChanged` deltas through `BrokerMetadataApi`); TypedProducer / TypedConsumer engine-generic at struct + builder level.
Callers that reach for a tokio-only method on the moonpool engine still get a trait-bound compile error, not a silent fallback — see ADR-0019 §Consequences.

### Open structural gaps

- **Moonpool engine parity.** Java parity is fully satisfied by the tokio engine ([ADR-0019](specs/adr/0019-engine-scope-and-moonpool-parity.md)); the façade's dependent surfaces are engine-generic through ADR-0026 §D1 extension traits.
  Producers, consumers, readers, partitioned producer/consumer, multi-topics, pattern consumers, TableView, transactions, and typed schema builders all dispatch through `impl<E: Engine> PulsarClient<E>` where the selected runtime implements the matching `*Api` trait.
  See the [Engine-by-engine surface coverage](#engine-by-engine-surface-coverage) table above for the remaining moonpool gaps.
- **PIP-460 scalable topics** ship as an experimental scaffold behind `feature = "scalable-topics"` (default off); the in-process four-layer test suite is the binding gate. e2e against a real broker waits for an upstream Pulsar release that ships PIP-460 (currently `Draft`, targeting Pulsar 5.0 LTS).
- **PIP-466 V5 surface** ships as an experimental, engine-generic wrapper behind `feature = "experimental-v5-client"` (default off).
  No wire change — it wraps the v4 surface.
- **SASL** ships both mechanisms end-to-end: `PLAIN` (RFC 4616) under the default `auth-sasl` feature, and Kerberos/GSSAPI via `libgssapi` under the `auth-sasl-kerberos` feature.
  The multi-round `AUTH_CHALLENGE` exchange threads through `AuthProvider::respond_to_challenge`.
  The four sans-io test layers drive a deterministic `ScriptedGssapiClient`; the e2e layer runs against a Dockerised KDC.
  See [ADR-0029](specs/adr/0029-sasl-kerberos-gssapi-scope.md).
- **Athenz** ships both the pre-fetched role-token path and the opt-in ZTS round-trip (`auth-athenz-zts`).
  Production-style ZMS+ZTS+certificate bootstrap remains out of scope for the local fixture.

---

## Supported PIPs

| PIP                      | Title                                                                                      | Status      | Lives in                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ------------------------ | ------------------------------------------------------------------------------------------ | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| PIP-4                    | End-to-end encryption (AES-GCM)                                                            | ✅          | `magnetar-messagecrypto`, `crypto_bridge` in `magnetar`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| PIP-22                   | DLQ topic                                                                                  | ✅          | `ConsumerBuilder::dead_letter_policy`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| PIP-30                   | In-band `AUTH_CHALLENGE` refresh                                                           | ✅          | `magnetar-proto::auth`, driver                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| PIP-31                   | Transactions                                                                               | ✅          | `magnetar-proto::txn`, `Client::new_txn`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| PIP-37                   | Chunking + ack-timeout redelivery backoff                                                  | ✅          | `magnetar-proto::producer`, `trackers::nack`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| PIP-54                   | Partial-batch ACK (ack_set bitset)                                                         | ✅          | `magnetar-proto::consumer`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| PIP-58                   | Retry-letter topic                                                                         | ✅          | `Consumer::reconsume_later`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| PIP-68                   | Exclusive producer access mode                                                             | ✅          | `ProducerBuilder::access_mode`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| PIP-90                   | Broker-entry metadata envelope                                                             | ✅          | `magnetar-proto::frame` (magic `0x0e02`), `IncomingMessage::broker_*`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| PIP-124                  | Multi-DLQ topics for KeyShared                                                             | ✅          | DLQ policy infra                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| PIP-145                  | Topic list watcher (regex pattern)                                                         | ✅          | `magnetar-proto::topic_watcher`, `PatternConsumer`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| PIP-292                  | Better in-band auth refresh ergonomics                                                     | ✅          | Driver event handler                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| PIP-313                  | Force unsubscribe                                                                          | ✅          | `CommandUnsubscribe.force` plumbed                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| PIP-34 / 119 / 282 / 379 | Key_Shared family                                                                          | ✅          | `KeySharedConfig` + builder                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| PIP-409                  | DLQ + retry-letter polish                                                                  | ✅          | DLQ + reconsume_later wiring                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| PIP-391                  | Batch-index ACK polish                                                                     | ✅          | Pairs with PIP-54                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| PIP-188                  | `TOPIC_MIGRATED`                                                                           | ✅          | Wire opcode decoded; tokio driver's event loop catches `ConnectionEvent::TopicMigrated`, logs the new-broker hint, and returns an error from `driver_loop_inner` so the supervisor triggers `Connection::reset` + reconnect. `rebuild_producers` / `rebuild_consumers` re-attach every still-open handle on the new socket.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| _local_                  | Anti-thrash policy ([ADR-0028](specs/adr/0028-supervised-reconnect-anti-thrash-policy.md)) | ✅ (opt-in) | Per-handle ack-then-drop detector + connection-level cooldown. Mitigates broker-driven post-restart cascades (Pulsar PR #14467 / #13428 / #12846 — `ServerCnx#handleProducer` ↔ `AbstractTopic#addProducer` race). `SupervisorConfig::anti_thrash_threshold` default `None`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| PIP-460                  | Scalable topics                                                                            | 🟡          | Experimental scaffold behind `feature = "scalable-topics"` (default off), ADR-0031. **No released Pulsar broker ships PIP-460** (upstream `Draft`, targets 5.0 LTS) — so this is scaffold-now / e2e-later. Ships the `topic://` URL scheme, hand-encoded wire commands (lookup + DAG-watch + DAG-update, behind the feature gate until the Pulsar 5.0 RC vendor bump per ADR-0026 §D4), the `DagWatchSession` sans-io state machine, the additive default-`None` `MessageId::segment_id` field (v4 wire byte-identical), both-engine `ScalableTopicsApi` impls, `magnetar::scalable::StreamConsumer` (StreamConsumer-only, **drops on DAG change**), and the `magnetar topic-info` CLI. Four-layer in-process tests are the binding gate; e2e is `#[ignore]`'d. QueueConsumer / CheckpointConsumer / controller-election / repartition stay out of the current scaffold and will land once an upstream release pins the wire commands. See [`docs/pip-features.md#scalable-topics-pip-460--experimental`](docs/pip-features.md#scalable-topics-pip-460--experimental) + [ADR-0031](specs/adr/0031-pip-460-scalable-subscription-scope.md). |
| PIP-466                  | V5 client API surface                                                                      | ✅          | Behind `feature = "experimental-v5-client"` (default off). Engine-generic per ADR-0032. `magnetar::v5` exposes `PulsarClientV5<E: Engine = TokioEngine>` (with `v4()` escape hatch), `v5::Producer<E>`, `v5::StreamConsumer<E>` (Exclusive / Failover), `v5::QueueConsumer<E>` (Shared / `KeyShared`), and the `v5::mapping` field-translation table. Moonpool callers name `PulsarClientV5<MoonpoolEngine<P>>` directly. Wraps the v4 surface — no wire change. See [ADR-0032](specs/adr/0032-pip-466-v5-client-surface-scope.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| PIP-180                  | Shadow topic                                                                               | ✅          | Admin REST (`create_shadow_topic` / `delete_shadow_topic` / `get_shadow_topics` / `get_shadow_source`), producer-side `send_with_source_message_id` propagating `CommandSend.message_id`, consumer-side `MessageReceivedFromShadow` event, structural `MessageId` equality across source ⇄ shadow. See [`docs/pip-features.md#shadow-topics-pip-180`](docs/pip-features.md#shadow-topics-pip-180) + [ADR-0033](specs/adr/0033-pip-180-shadow-topic-scope.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| PIP-415                  | `getMessageIdByIndex`                                                                      | ✅          | `magnetar-admin::AdminClient::topic_get_message_id_by_index` — REST-only per [PIP-415](https://github.com/apache/pulsar/blob/master/pip/pip-415.md) (binary-protocol section intentionally empty; canonical implementation [`apache/pulsar#24222`](https://github.com/apache/pulsar/pull/24222) is admin / broker / CLI only)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| PIP-33                   | Replicated subscriptions                                                                   | ✅          | `ConsumerBuilder::replicate_subscription_state(bool)` flips `CommandSubscribe` field 14; receive-path filter in `magnetar-proto::conn` drops `REPLICATED_SUBSCRIPTION_*` markers and surfaces them via `PulsarClient::next_replicated_subscription_marker` / `poll_replicated_subscription_marker`. Client never originates markers — broker-side machinery only. See [`docs/pip-features.md#replicated-subscriptions-pip-33`](docs/pip-features.md#replicated-subscriptions-pip-33) + [ADR-0034](specs/adr/0034-pip-33-replicated-subscriptions-scope.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| PIP-121                  | Cluster failover (Auto + Controlled)                                                       | ✅          | `ServiceUrlProvider` + `StaticServiceUrlProvider` + `ControlledClusterFailover` (proto) + `AutoClusterFailover` (runtime with `HealthProbe`). Active URL re-resolved on every supervised-reconnect attempt.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |

---

## Runtime engines

Magnetar publishes two engines that drive the same sans-io state machine.
Pick at compile time via feature flags.

### `magnetar-runtime-tokio` — production (default)

- TLS via [`tokio-rustls`](https://crates.io/crates/tokio-rustls) (ring backend); no `native-tls`, no `openssl`.
- One driver task per connection — see [ARCHITECTURE.md §"The driver loop"](ARCHITECTURE.md#the-driver-loop).
- The user-facing futures (`Consumer::receive`, `Producer::send`, …) lock the shared state machine, register their `Waker` in a slab, and wait.
  The driver picks them up as the matching `OpOutcome` lands.
- This is what `magnetar::PulsarClient` wires by default (`PulsarClient<TokioEngine>`).

### `magnetar-runtime-moonpool` — deterministic simulation

- Drives the same sans-io state machine as the tokio engine over `moonpool_core::Providers` (a bundle of `NetworkProvider`, `TimeProvider`, `TaskProvider`, `RandomProvider`, `StorageProvider`).
  Plug `TokioProviders` for production-style runs against a real broker, or a `moonpool-sim` provider bundle for reproducible chaos under a seed.
- TLS uses a local `rustls::ClientConnection` adapter ([`tls.rs`](crates/magnetar-runtime-moonpool/src/tls.rs)) that drives `read_tls` / `process_new_packets` / `write_tls` over the moonpool byte pipe — the handshake stays deterministic under chaos.
- See [`docs/moonpool-engine.md`](docs/moonpool-engine.md) for the engine's surface, supervised reconnect, chaos test pack, and the tokio ↔ moonpool differential equivalence harness.

---

## Supported broker versions

- **Pulsar 4.0+** (LTS).
  The CONNECT frame advertises `ProtocolVersion::V21` and the connection falls back to whichever lower version the broker reports on `CONNECTED`.
- The end-to-end suite runs against `apachepulsar/pulsar:4.0.4`.

---

## Status

Magnetar targets a feature-complete Apache Pulsar driver with full Java-client parity, driven by two interchangeable engines (production tokio + deterministic-simulation moonpool) over the same sans-io protocol state machine ([ADR-0010](specs/adr/0010-v0-1-full-java-parity.md), [ADR-0019](specs/adr/0019-engine-scope-and-moonpool-parity.md)).
Java parity is satisfied on the tokio engine; the moonpool engine is on the same trajectory and the differential harness enforces tokio ↔ moonpool `EventStream` equivalence.

The bulk of the parity matrix above ships on `main`, including:

- **PIP-180** shadow topic ([`docs/pip-features.md#shadow-topics-pip-180`](docs/pip-features.md#shadow-topics-pip-180), [ADR-0033](specs/adr/0033-pip-180-shadow-topic-scope.md)).
- **PIP-33** replicated subscriptions ([`docs/pip-features.md#replicated-subscriptions-pip-33`](docs/pip-features.md#replicated-subscriptions-pip-33), [ADR-0034](specs/adr/0034-pip-33-replicated-subscriptions-scope.md)).
- **SASL Kerberos / GSSAPI** ([ADR-0029](specs/adr/0029-sasl-kerberos-gssapi-scope.md)).
- **Pluggable rustls crypto provider** (aws-lc-rs / ring / openssl / fips — [ADR-0035](specs/adr/0035-pluggable-crypto-provider.md)).
- **Daily 128-random-seed moonpool sweep** ([ADR-0036](specs/adr/0036-moonpool-seed-sweep-daily-random.md)).
- **Anti-thrash supervised reconnect policy** (opt-in, [ADR-0028](specs/adr/0028-supervised-reconnect-anti-thrash-policy.md)).

Known open work is narrow and tracked in [`docs/follow-ups.md`](docs/follow-ups.md): PIP-460 e2e waits for a Pulsar 5.0 RC that pins the scalable-topic wire commands, the moonpool git dependency waits for a release containing upstream PR #113, and a few simulation / test-harness gaps remain.

API is unstable until the first tagged release — do not depend on this in production yet.

---

## Validation

The whole workspace builds against stable Rust 1.88.

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
RUSTDOCFLAGS="-D warnings --cfg tokio_unstable --cfg tracing_unstable" \
  cargo doc --workspace --all-features --no-deps
```

End-to-end tests against a real broker run as part of `cargo test --workspace --all-features` (ADR-0046 — no `--features e2e`, no `#[ignore]`).
Docker is the only prerequisite; the suite spins `pulsar:4.0.4` via `testcontainers-rs`.

Additional `xtask` checks specific to the sans-io invariants:

```sh
cargo run -p xtask -- check-no-channels   # greps src/** for banned channel crates
cargo run -p xtask -- check-no-io-deps    # magnetar-proto must not depend on any I/O crate
cargo run -p xtask -- codegen --check     # asserts proto codegen has no drift
```

---

## License

Apache-2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
The project vendors a verbatim copy of the Apache Pulsar wire protocol definition (`PulsarApi.proto`, `PulsarMarkers.proto`), released by the Apache Software Foundation under Apache-2.0.

See [GUIDELINES.md](GUIDELINES.md) and [CONTRIBUTING.md](CONTRIBUTING.md) for project conventions before sending a patch.
