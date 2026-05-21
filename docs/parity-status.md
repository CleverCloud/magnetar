# Magnetar — Java Parity Status Snapshot

**Generated**: 2026-05-21
**HEAD**: `d8fae87 feat(consumer): PIP-37 AckTimeoutRedeliveryBackoff (Java parity)`
**Total LOC across crates**: ~17,100

## Crate sizes (LOC)

| Crate | Lines | Role |
|---|---|---|
| magnetar-proto | ~7,500 | Sans-io state machine + protobuf wire types |
| magnetar-runtime-tokio | ~3,300 | Production engine (driver loop, producer/consumer façades) |
| magnetar-runtime-moonpool | ~450 | Engine stub — handshake only, no driver loop, no producer/consumer |
| magnetar | ~5,800 | High-level façade — PulsarClient, builders, typed schemas, partitioned, multi-topics, pattern |
| magnetar-admin | (separate) | REST admin client |
| magnetar-auth-* | (separate) | OAuth2, SASL, Athenz providers |
| magnetar-messagecrypto | (separate) | PIP-4 AES-GCM bridge |

## Recently landed (last 50 commits, reverse chronological)

PIP-37 AckTimeoutRedeliveryBackoff · PatternConsumer full ConsumerBuilder knob set ·
PIP-145 PatternConsumer (regex topic discovery) · PIP-145 TopicListChanged event streaming
on the runtime client · ConsumerInterceptor SPI · ProducerInterceptor SPI ·
TypedTableView · PartitionedProducer MessageRouter trait + new_message ·
TypedTableView + builder · client max_message_size + proxy_to_broker_url ·
MessageBuilder helpers (size, is_empty, is_replicated, etc.) ·
AckGroupingTracker wire-up · ack_grouped + ack_grouped_cumulative forwarding ·
Reader full lifecycle (is_closed, available_in_queue, etc.) ·
batching_max_publish_delay timer · reconsume_later + with_properties (retry-letter
topic Java parity) · PIP-54 partial-batch ACK with ack_set bitset ·
TopicListChanged delivery via Mutex<VecDeque> + Notify (no channels) ·
TypedTableViewBuilder + listen typed callbacks · TypedProducer/TypedConsumer
full surface · MultiTopicsConsumer add of forwarders · PartitionedConsumer
12 builder knobs · TableView property/subscription_property/start_message_id ·
Date/Time/Timestamp/LocalDate/LocalTime/Instant/LocalDateTime primitive schemas ·
ConsumerStats: total_chunked_msgs_received counter ·
hdrhistogram still pending · auto-reconnect supervisor still pending.

## Open gaps (priority-ordered)

### 1. Auto-reconnect supervisor (LARGEST)

Driver currently exits on I/O failure. `Backoff` struct exists in
`magnetar-proto`. No supervisor loop wraps `driver_loop` to:
- Detect connection drop
- Apply exponential backoff
- Re-handshake, re-subscribe every producer + consumer with prior state
- Replay pending acks / un-flushed batches

**Touch points**:
- `crates/magnetar-runtime-tokio/src/driver.rs` (driver_loop entry point)
- `crates/magnetar-proto/src/conn.rs` (connection state machine)
- `crates/magnetar-runtime-tokio/src/client.rs` (Client::close semantics)

### 2. Moonpool engine completion

`crates/magnetar-runtime-moonpool` is 450 LOC and only does the handshake.
Mirror the tokio engine:
- driver_loop (sans-io event consumer ↔ socket I/O)
- Producer façade (send, batching, flush, close)
- Consumer façade (subscribe, receive, ack, nack, seek, close)
- Client façade (connect, lookup, producer(), consumer(), watch_topic_list)
- magnetar facade integration behind `moonpool` feature

### 3. hdrhist latency stats

Java's `ConsumerStatsRecorder` carries p50/p99/max latency histograms. Magnetar
exposes only counters. Adding `hdrhistogram` to workspace allow-list, then
wiring per-send / per-receive latency into Producer/Consumer stats.

### 4. ConsumerStats / ProducerStats rolling windows

Java tracks msgs/sec, bytes/sec on a sliding window. Magnetar exposes only
cumulative counters. Needs a tick-driven rolling computation.

### 5. PatternConsumer auto-update background task

`PatternConsumer::update` is caller-driven today. Java drives reconcile from a
background ticker. Blocked on PulsarClient ownership (not Clone). Resolution
path: wrap inner Client in Arc, then spawn a tokio task that polls
`next_topic_list_change` or fires on a periodic tick.

### 6. MultiTopicsConsumer dynamic add_topic / remove_topic

Blocked by `Arc<Inner>` immutability on the existing MultiTopicsConsumer.
Either refactor to `Mutex<Vec<NamedConsumer>>` (like PatternConsumer), or
provide a new `DynamicMultiTopicsConsumer` sibling.

### 7. Consumer#seek(Function<String, Object>)

Java allows per-partition function-based seek. Useful for replay scenarios.

### 8. PIP-37 nack backoff propagated to ConsumerBuilder.negative_ack_backoff

Tracker has `MultiplierRedeliveryBackoff` + `add_with_delay`, but the builder
doesn't have a `negative_ack_backoff(...)` surface that automatically routes
`negative_ack()` through the backoff using each message's redelivery_count.

### 9. Custom MessageHasher (Murmur3, JavaStringHash) on PartitionedProducer

Java offers pluggable hashers. We hardcode the default.

### 10. CryptoFailureAction on Consumer

Java's encryption builder lets users choose between fail-fast, consume-with-stub,
and silent-drop on decryption errors. Magnetar lacks this knob.

### 11. TableView crypto reader + auto-update-partitions interval

Java's TableView wires through encryption + periodic partition discovery.
Magnetar covers neither.

### 12. Producer access_mode getter

Setter exists, getter doesn't.

## Test surface — current

- 220 unit tests pass (`cargo test --all-features --workspace`)
- 5 e2e tests gated behind `--features e2e`:
  - `e2e_produce_consume_roundtrip`
  - `e2e_partitioned_topic_roundtrip`
  - `e2e_key_shared_dispatch`
  - `e2e_pattern_consumer_snapshot`
  - (one more — to be counted by researcher)
- Image: `apachepulsar/pulsar:4.0.4` (Pulsar 4.0 LTS)

## Java tests inventory (raw)

- `/home/florentin/Sources/github.com/apache/pulsar/pulsar-client/src/test`: 143 java files
- `/home/florentin/Sources/github.com/apache/pulsar/pulsar-broker/src/test` (Consumer/Producer/Client):
  134 java files

Of these, the *behavioral* tests worth porting are a subset — JVM-specific
tests, broker-internal tests, and tests that exercise Java APIs not present in
Magnetar should be skipped.

## Constraints

- **No channels**: never use `tokio::sync::mpsc / broadcast / oneshot / watch`,
  `crossbeam-channel`, `flume`, `async-channel`. Use
  `Arc<parking_lot::Mutex<...>>` + `tokio::sync::Notify` + per-future Waker slabs.
- **Commits**: GPG-signed via `git commit -s -S` (enforced by hook).
- **Branches**: `feat/<scope>`, `fix/<scope>`, etc.
- **Worktree-first**: `wt switch --create feat/<scope> -y`, work, `wt merge -y`.
- **No Claude attribution** on commits / PRs / MRs.
- **Conventional commits**: `<type>(<scope>): <subject>`.
- **Validation chain (every commit)**:
  ```
  cargo build --all-features
  cargo test --all-features --workspace
  cargo clippy --all-features --workspace -- -D warnings
  cargo +nightly fmt --all
  RUSTDOCFLAGS="-D warnings --cfg tokio_unstable" cargo doc --no-deps --all-features --workspace --locked
  cargo deny check
  ```
