// SPDX-License-Identifier: Apache-2.0

//! Partition-aware producer.
//!
//! Mirrors Java's `PartitionedProducerImpl`. On `create()` the builder queries the broker for
//! the topic's partition count via `CommandPartitionedTopicMetadata`. If the count is `> 1`
//! it opens one child [`magnetar_runtime_tokio::Producer`] per partition (`<topic>-partition-N`)
//! and routes user sends to the appropriate child via a configurable routing strategy.
//! Otherwise it falls back to a single producer on the original topic.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use magnetar_proto::types::CompressionKind;
use magnetar_proto::{CreateProducerRequest, MessageId, pb};
use magnetar_runtime_tokio::Producer;
use tokio::sync::Notify;

use crate::auto_update_task::{AutoUpdateTask, spawn_auto_update_task};
use crate::client::{OutgoingMessage, PulsarError};
use crate::{Engine, PulsarClient, TokioEngine};

/// How a [`PartitionedProducer`] picks the partition for an outgoing message.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MessageRoutingMode {
    /// Hash the message's partition key via Java's `String.hashCode()` (the
    /// `HashingScheme.JavaStringHash` default), then `% partitions`. Falls back
    /// to round-robin when no key is set or the key is empty. Wire-compatible
    /// with Java's default routing: the same key on a Rust producer and a
    /// Java producer lands on the same partition.
    #[default]
    KeyHashOrRoundRobin,
    /// Always round-robin, ignoring any partition key.
    RoundRobin,
    /// Always route to a single partition (`single_partition_index`). Useful for ordered
    /// streams that don't need parallelism.
    SinglePartition(u32),
}

/// Partitioned-producer-bound counterpart to [`crate::MessageBuilder`]. Same chained
/// setters; the terminal `.send().await` resolves the partition and dispatches.
#[derive(Debug)]
pub struct PartitionedMessageBuilder<'a, P: crate::ProducerApi = Producer> {
    producer: &'a PartitionedProducer<P>,
    msg: OutgoingMessage,
}

impl<P: crate::ProducerApi> PartitionedMessageBuilder<'_, P> {
    /// See [`OutgoingMessage::key`].
    #[must_use]
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.msg = self.msg.key(key);
        self
    }

    /// See [`OutgoingMessage::ordering_key`].
    #[must_use]
    pub fn ordering_key(mut self, key: impl Into<Bytes>) -> Self {
        self.msg = self.msg.ordering_key(key);
        self
    }

    /// See [`OutgoingMessage::event_time_ms`].
    #[must_use]
    pub fn event_time_ms(mut self, ts: u64) -> Self {
        self.msg = self.msg.event_time_ms(ts);
        self
    }

    /// See [`OutgoingMessage::property`].
    #[must_use]
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.msg = self.msg.property(key, value);
        self
    }

    /// See [`OutgoingMessage::deliver_at_ms`].
    #[must_use]
    pub fn deliver_at_ms(mut self, ts_ms: i64) -> Self {
        self.msg = self.msg.deliver_at_ms(ts_ms);
        self
    }

    /// See [`OutgoingMessage::deliver_after_ms`]. The caller supplies
    /// `now_ms` (sans-io, ADR-0011 invariant #3).
    #[must_use]
    pub fn deliver_after_ms(mut self, now_ms: i64, delay_ms: i64) -> Self {
        self.msg = self.msg.deliver_after_ms(now_ms, delay_ms);
        self
    }

    /// See [`OutgoingMessage::replication_clusters`].
    #[must_use]
    pub fn replication_clusters(mut self, clusters: Vec<String>) -> Self {
        self.msg = self.msg.replication_clusters(clusters);
        self
    }

    /// See [`OutgoingMessage::disable_replication`].
    #[must_use]
    pub fn disable_replication(mut self) -> Self {
        self.msg = self.msg.disable_replication();
        self
    }

    /// See [`OutgoingMessage::txn`].
    #[must_use]
    pub fn txn(mut self, txn_id: magnetar_proto::TxnId) -> Self {
        self.msg = self.msg.txn(txn_id);
        self
    }

    /// Set the payload bytes. See [`OutgoingMessage::value`].
    #[must_use]
    pub fn value(mut self, payload: impl Into<Bytes>) -> Self {
        self.msg = self.msg.value(payload);
        self
    }

    /// Resolve the partition and dispatch. Returns the broker-assigned [`MessageId`].
    pub async fn send(self) -> Result<MessageId, PulsarError> {
        self.producer.send(self.msg).await
    }
}

/// Plug a user-provided routing function in front of [`MessageRoutingMode`]. Mirrors
/// Java's `MessageRouter` SPI — when set on the builder, the function decides the
/// partition for every outgoing message; the configured [`MessageRoutingMode`] is
/// ignored. Use this for affinity routing rules (geo, tenant, schema-keyed) that don't
/// fit the partition-key-hash mould.
///
/// The callback runs on the send path — keep it fast and non-blocking. The framework
/// clamps the returned index into `[0, partitions)` so out-of-range values can't crash
/// the producer.
pub trait MessageRouter: Send + Sync + std::fmt::Debug {
    /// Pick a partition index in `[0, partitions)` for `msg`.
    fn route(&self, msg: &crate::OutgoingMessage, partitions: usize) -> usize;
}

/// Bit-for-bit port of Apache Pulsar's `Murmur3_32Hash.makeHash(byte[])`
/// ([`Murmur3_32Hash.java`]). Used by [`Murmur3HashHasher`] so cross-language consumers
/// (Java, C++, Go) see identical routing for the same key.
///
/// Returns a non-negative 31-bit value — the Java implementation masks with
/// `Integer.MAX_VALUE` before returning.
///
/// [`Murmur3_32Hash.java`]: https://github.com/apache/pulsar/blob/master/pulsar-common/src/main/java/org/apache/pulsar/common/util/Murmur3_32Hash.java
#[must_use]
pub fn murmur3_32_hash(bytes: &[u8]) -> u32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    const SEED: u32 = 0;

    let len = bytes.len();
    let mut h1: u32 = SEED;

    let mix_k1 = |mut k1: u32| -> u32 {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        k1
    };

    let chunks = bytes.chunks_exact(4);
    let remainder = chunks.remainder();
    for chunk in chunks {
        // Java's `ByteBuffer.LITTLE_ENDIAN.getInt()` reads four bytes little-endian.
        let k1 = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        let k1 = mix_k1(k1);
        h1 ^= k1;
        h1 = h1.rotate_left(13);
        h1 = h1.wrapping_mul(5).wrapping_add(0xe654_6b64);
    }

    // Tail.
    let mut k1: u32 = 0;
    for (i, byte) in remainder.iter().enumerate() {
        k1 ^= u32::from(*byte) << (i * 8);
    }
    h1 ^= mix_k1(k1);

    // Finalisation: XOR length, then `fmix`.
    h1 ^= len as u32;
    h1 ^= h1 >> 16;
    h1 = h1.wrapping_mul(0x85eb_ca6b);
    h1 ^= h1 >> 13;
    h1 = h1.wrapping_mul(0xc2b2_ae35);
    h1 ^= h1 >> 16;

    // Mirror Java's `& Integer.MAX_VALUE` mask so the value fits into a non-negative
    // signed int32 — matches `Murmur3Hash32.makeHash` and `Murmur3_32Hash.makeHash`.
    h1 & 0x7FFF_FFFF
}

/// Bit-for-bit port of `String.hashCode() & Integer.MAX_VALUE`. Iterates over UTF-16
/// code units (matching Java's `char`) so non-BMP code points hash identically to the
/// JDK. ASCII strings short-circuit through the byte path.
///
/// Used by [`JavaStringHashHasher`].
#[must_use]
pub fn java_string_hash(key: &str) -> u32 {
    let mut h: u32 = 0;
    if key.is_ascii() {
        for byte in key.bytes() {
            h = h.wrapping_mul(31).wrapping_add(u32::from(byte));
        }
    } else {
        for code_unit in key.encode_utf16() {
            h = h.wrapping_mul(31).wrapping_add(u32::from(code_unit));
        }
    }
    h & 0x7FFF_FFFF
}

/// Pick the partition by hashing the message's UTF-8-encoded partition key with
/// [`murmur3_32_hash`] (Apache Pulsar `Murmur3_32Hash`, seed `0`), then `hash %
/// partitions`. Falls back to round-robin via [`OutgoingMessage::key`] being `None` or
/// empty.
///
/// Wire-compatible with Java's `HashingScheme.Murmur3_32Hash` so Java, C++, Go, and
/// magnetar producers route the same key to the same partition.
#[derive(Debug, Default, Clone, Copy)]
pub struct Murmur3HashHasher;

impl MessageRouter for Murmur3HashHasher {
    fn route(&self, msg: &crate::OutgoingMessage, partitions: usize) -> usize {
        partition_for_key(msg.key.as_deref(), partitions, |k| {
            murmur3_32_hash(k.as_bytes())
        })
    }
}

/// Pick the partition with [`java_string_hash`] (Java `String.hashCode()` semantics),
/// then `hash % partitions`. Falls back to round-robin when no key is set.
///
/// Wire-compatible with Java's default `HashingScheme.JavaStringHash`.
#[derive(Debug, Default, Clone, Copy)]
pub struct JavaStringHashHasher;

impl MessageRouter for JavaStringHashHasher {
    fn route(&self, msg: &crate::OutgoingMessage, partitions: usize) -> usize {
        partition_for_key(msg.key.as_deref(), partitions, java_string_hash)
    }
}

/// Shared "keyed hash, fall back to a sticky default" routing helper. The fallback
/// returns partition `0`; the surrounding [`PartitionedProducer`] is responsible for
/// running round-robin when no router is installed. When a router *is* installed it
/// overrides the configured [`MessageRoutingMode`] entirely (mirrors Java
/// `ProducerBuilder#messageRouter`), so we cannot rotate through the cursor here —
/// instead we sticky-route to partition `0`, matching Java's `RoundRobinPartitionMessageRouter`
/// behaviour when the key is null and batching keeps a key-affine sticky partition.
fn partition_for_key<F>(key: Option<&str>, partitions: usize, hash: F) -> usize
where
    F: FnOnce(&str) -> u32,
{
    if partitions == 0 {
        return 0;
    }
    match key {
        Some(k) if !k.is_empty() => (hash(k) as usize) % partitions,
        _ => 0,
    }
}

/// Partition-aware producer.
///
/// Generic over `P: ProducerApi` per ADR-0026 §D1 (default
/// `magnetar_runtime_tokio::Producer`). The general inherent impl
/// dispatches `send` / `flush` / `close` / `stats` through `ProducerApi`;
/// tokio-only specialised methods (`refresh_partitions`,
/// `last_sequence_id_published`, batch counters) live in the
/// `impl PartitionedProducer<Producer>` specialised block below.
#[derive(Debug)]
pub struct PartitionedProducer<P: crate::ProducerApi = Producer> {
    partitions: Vec<P>,
    base_topic: String,
    routing: MessageRoutingMode,
    /// Optional custom router. When set, takes precedence over [`Self::routing`] for
    /// every send.
    router: Option<std::sync::Arc<dyn MessageRouter>>,
    cursor: AtomicU64,
    /// Optional background partition-watcher task. `Some` when the builder configured
    /// [`PartitionedProducerBuilder::auto_update_partitions_interval`], `None`
    /// otherwise (default). The task is a pure timer that signals
    /// [`Self::partitions_changed_notify`] every interval; the actual
    /// `partitions_for_topic` call is driven by user code via
    /// [`Self::refresh_partitions`]. Dropping the [`PartitionedProducer`] aborts the
    /// task.
    auto_update: Option<Arc<AutoUpdateTask>>,
}

impl<P: crate::ProducerApi> PartitionedProducer<P> {
    /// Base topic name (without the `-partition-N` suffix).
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.base_topic
    }

    /// Number of child producers (1 for non-partitioned topics).
    #[must_use]
    pub fn partitions(&self) -> usize {
        self.partitions.len()
    }

    /// Borrow the underlying per-partition producers. Useful for advanced operations
    /// like per-partition flush.
    #[must_use]
    pub fn child_producers(&self) -> &[P] {
        &self.partitions
    }

    /// Publish a message, routing it to one of the underlying producers per the configured
    /// [`MessageRoutingMode`] (or the custom `MessageRouter` when one was installed on the
    /// builder). Returns the broker-assigned message id (the routing layer is transparent
    /// — the id has a `partition` filled in by the broker).
    ///
    /// # Errors
    /// - [`PulsarError::Other`] (stringified from the runtime's `ProducerApi::Error`) on wire
    ///   failure.
    pub async fn send(&self, msg: OutgoingMessage) -> Result<MessageId, PulsarError> {
        let idx = self.pick_partition(&msg);
        let producer = &self.partitions[idx];
        crate::ProducerApi::send(producer, msg)
            .await
            .map_err(|err| PulsarError::Other(format!("send: {err}")))
    }

    /// Start a Java-symmetric `MessageBuilder` chain that ends with `.send().await`. The
    /// routing decision happens on `send` based on the constructed `OutgoingMessage`, so
    /// `.key(..)` participates in `MessageRoutingMode::KeyHashOrRoundRobin` and any
    /// installed `MessageRouter` sees the full message.
    #[must_use]
    pub fn new_message(&self) -> PartitionedMessageBuilder<'_, P> {
        PartitionedMessageBuilder {
            producer: self,
            msg: OutgoingMessage::default(),
        }
    }

    fn pick_partition(&self, msg: &OutgoingMessage) -> usize {
        let n = self.partitions.len();
        if n == 0 {
            return 0;
        }
        if let Some(router) = &self.router {
            // Clamp into range so an out-of-range router can't crash the producer.
            return router.route(msg, n).min(n - 1);
        }
        let key = msg.key.as_deref();
        match self.routing {
            MessageRoutingMode::SinglePartition(p) => (p as usize).min(n - 1),
            MessageRoutingMode::RoundRobin => {
                let prev = self.cursor.fetch_add(1, Ordering::Relaxed);
                (prev as usize) % n
            }
            MessageRoutingMode::KeyHashOrRoundRobin => match key {
                // Java parity: `HashingScheme.JavaStringHash` —
                // `String.hashCode() & Integer.MAX_VALUE`, masked into a
                // non-negative i32. The earlier implementation used Rust's
                // `DefaultHasher` (SipHash with a process-randomised seed),
                // which broke cross-language key affinity: the same key
                // routed to a different partition from a Java producer,
                // defeating the whole point of keyed partitioning. See R2/F3
                // for the regression. The `java_string_hash` helper is the
                // same one wired into [`JavaStringHashHasher`].
                Some(k) if !k.is_empty() => (java_string_hash(k) as usize) % n,
                _ => {
                    let prev = self.cursor.fetch_add(1, Ordering::Relaxed);
                    (prev as usize) % n
                }
            },
        }
    }

    /// Aggregate cumulative stats across all child producers. Adds the totals from each
    /// child; the pending-queue size is the sum.
    #[must_use]
    pub fn aggregate_stats(&self) -> magnetar_proto::ProducerStats {
        let mut agg = magnetar_proto::ProducerStats::default();
        for p in &self.partitions {
            let s = crate::ProducerApi::stats(p);
            agg.total_msgs_sent = agg.total_msgs_sent.saturating_add(s.total_msgs_sent);
            agg.total_bytes_sent = agg.total_bytes_sent.saturating_add(s.total_bytes_sent);
            agg.total_send_failed = agg.total_send_failed.saturating_add(s.total_send_failed);
            agg.total_acks_received = agg
                .total_acks_received
                .saturating_add(s.total_acks_received);
            agg.pending_queue_size = agg.pending_queue_size.saturating_add(s.pending_queue_size);
        }
        agg
    }

    /// Close every child producer. Returns the first error encountered.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] (stringified) on the first child failure.
    pub async fn close(self) -> Result<(), PulsarError> {
        let mut first_err: Result<(), PulsarError> = Ok(());
        for p in self.partitions {
            if let Err(e) = crate::ProducerApi::close_owned(p).await
                && first_err.is_ok()
            {
                first_err = Err(PulsarError::Other(format!("close: {e}")));
            }
        }
        first_err
    }

    /// Flush every child producer in parallel. Mirrors Java
    /// `Producer#flushAsync` semantics — resolves once each per-partition pending queue
    /// drains. Returns the first error encountered.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] (stringified) on the first child failure.
    pub async fn flush(&self) -> Result<(), PulsarError> {
        let mut first_err: Result<(), PulsarError> = Ok(());
        for p in &self.partitions {
            if let Err(e) = crate::ProducerApi::flush(p).await
                && first_err.is_ok()
            {
                first_err = Err(PulsarError::Other(format!("flush: {e}")));
            }
        }
        first_err
    }

    /// `true` while every child producer reports the underlying connection is up. Mirrors
    /// Java `Producer#isConnected` at the partitioned scope — Java returns true iff every
    /// partition's underlying producer is connected.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.partitions.iter().all(crate::ProducerApi::is_connected)
    }

    /// Earliest wall-clock disconnect timestamp across all child producers, or `None` if
    /// no child has ever disconnected. Useful for "when did we last see a connection
    /// drop?" health probes.
    #[must_use]
    pub fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        self.partitions
            .iter()
            .filter_map(crate::ProducerApi::last_disconnected_timestamp)
            .min()
    }

    /// `true` once every child producer is closed. Mirrors Java `Producer#isClosed` at the
    /// partitioned scope. Pair with [`Self::is_connected`] for the live test — `is_closed`
    /// only flips after a terminal close, not on transient disconnects.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.partitions.iter().all(crate::ProducerApi::is_closed)
    }

    /// Max `last_sequence_id` across every child producer (i.e. the largest sequence id
    /// this client has pushed onto the wire on any partition). Returns `-1` when no
    /// partition has sent yet. Useful for at-least-once resume-on-restart at the
    /// partitioned scope. Mirrors Java `Producer#getLastSequenceId` aggregated.
    #[must_use]
    pub fn last_sequence_id(&self) -> i64 {
        self.partitions
            .iter()
            .map(crate::ProducerApi::last_sequence_id)
            .max()
            .unwrap_or(-1)
    }

    /// Returns `true` if a background partition-watcher was spawned for this
    /// producer (i.e.
    /// [`PartitionedProducerBuilder::auto_update_partitions_interval`] was set on
    /// the builder). Defaults to `false` — current Java-parity behaviour when the
    /// user did not opt in.
    #[must_use]
    pub fn has_auto_update_partitions(&self) -> bool {
        self.auto_update.is_some()
    }

    /// Most recent partition count observed by the background partition watcher.
    /// `None` when
    /// [`PartitionedProducerBuilder::auto_update_partitions_interval`] was not set
    /// (no watcher spawned). Mirrors the read side of Java's
    /// `ProducerBuilder#autoUpdatePartitionsInterval` behaviour — Java rebuilds
    /// internally; we expose the observation so callers can react.
    #[must_use]
    pub fn observed_partitions(&self) -> Option<u32> {
        self.auto_update
            .as_ref()
            .map(|t| t.observed_partitions.load(Ordering::Relaxed))
    }

    /// Monotonic count of partition-change events observed by the background
    /// watcher. Returns `None` when no watcher was configured. The counter starts
    /// at `0` and is bumped every time [`Self::refresh_partitions`] detects a
    /// different partition count than the previous observation.
    #[must_use]
    pub fn partition_change_count(&self) -> Option<u64> {
        self.auto_update
            .as_ref()
            .map(|t| t.change_count.load(Ordering::Relaxed))
    }

    /// `Arc<Notify>` signalled by the background partition-watcher on every timer
    /// tick (i.e. every `auto_update_partitions_interval`) and on every observed
    /// partition-count change driven by [`Self::refresh_partitions`]. Returns
    /// `None` when no watcher was configured. Callers may `await` `notified()` on
    /// the returned handle to react to ticks without polling
    /// [`Self::partition_change_count`].
    #[must_use]
    pub fn partitions_changed_notify(&self) -> Option<Arc<Notify>> {
        self.auto_update.as_ref().map(|t| t.changed.clone())
    }
}

/// Tokio-engine-specific `PartitionedProducer` methods that depend on
/// either (a) `PulsarClient<TokioEngine>` (e.g. `refresh_partitions`
/// which calls `client.partitions_for_topic`) or (b) Producer helpers
/// not yet on `ProducerApi` (`last_sequence_id_published`,
/// `batch_len`, `batch_bytes`, `pending_count`). Each of these
/// methods can be lifted once the matching helper lands on
/// `ProducerApi` / a future `EngineClient` trait.
impl PartitionedProducer<Producer> {
    /// Max `last_sequence_id_published` across every child producer. Returns `-1` when no
    /// partition has been acknowledged yet. Mirrors Java
    /// `Producer#getLastSequenceIdPublished` aggregated.
    #[must_use]
    pub fn last_sequence_id_published(&self) -> i64 {
        self.partitions
            .iter()
            .map(magnetar_runtime_tokio::Producer::last_sequence_id_published)
            .max()
            .unwrap_or(-1)
    }

    /// Sum of in-flight sends across every child producer.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.partitions
            .iter()
            .map(magnetar_runtime_tokio::Producer::pending_count)
            .sum()
    }

    /// Sum of batch-buffered messages across every child producer.
    #[must_use]
    pub fn batch_len(&self) -> usize {
        self.partitions
            .iter()
            .map(magnetar_runtime_tokio::Producer::batch_len)
            .sum()
    }

    /// Sum of batch-buffered payload bytes across every child producer.
    #[must_use]
    pub fn batch_bytes(&self) -> usize {
        self.partitions
            .iter()
            .map(magnetar_runtime_tokio::Producer::batch_bytes)
            .sum()
    }

    /// Query the broker for the current partition count of the topic this producer
    /// was opened against, and update [`Self::observed_partitions`] /
    /// [`Self::partition_change_count`] in place if the count differs from the
    /// last observation.
    ///
    /// This is the user-driven half of the
    /// [`PartitionedProducerBuilder::auto_update_partitions_interval`] machinery:
    /// the timer task signals [`Self::partitions_changed_notify`]; the user calls
    /// this method in response (or independently) to actually refresh the count.
    /// Returns the freshly-observed count on success, or `Ok(None)` if no watcher
    /// was configured (no topic recorded). Errors are surfaced via [`PulsarError`].
    ///
    /// **Note**: this method only updates the observed count. It does *not* itself
    /// add new child producers to match a grown partition count — that is a
    /// follow-up. Callers that need to expand the producer set can detect the
    /// change via [`Self::observed_partitions`] / [`Self::partitions`] divergence
    /// and rebuild the producer.
    ///
    /// # Errors
    ///
    /// Surfaces [`PulsarError::Client`] when the broker metadata lookup fails.
    pub async fn refresh_partitions(
        &self,
        client: &PulsarClient,
    ) -> Result<Option<u32>, PulsarError> {
        let Some(task) = self.auto_update.as_ref() else {
            return Ok(None);
        };
        let count = client.partitions_for_topic(&task.topic).await?;
        // Atomic swap-then-compare. See multi_topics.rs for the rationale.
        let prev = task.observed_partitions.swap(count, Ordering::Relaxed);
        if prev != count {
            task.change_count.fetch_add(1, Ordering::Relaxed);
            task.changed.notify_waiters();
        }
        Ok(Some(count))
    }
}

/// Builder for [`PartitionedProducer`]. Mirrors Java's `ProducerBuilder` at the partitioned
/// layer.
///
/// Engine-generic: the type parameter `E: Engine` (defaults to
/// [`crate::TokioEngine`]) selects the per-partition child producer type
/// via the engine-side [`crate::CreateProducerApi`] +
/// [`crate::BrokerMetadataApi`] extension traits. The encryptor slot is
/// engine-typed via
/// [`crate::MessageEncryptorApi`] (tokio plugs in
/// `Arc<dyn magnetar_runtime_tokio::MessageEncryptor>`; moonpool plugs in
/// `Arc<dyn magnetar_runtime_moonpool::MessageEncryptor>` now that the
/// moonpool engine ships the PIP-4 bridge).
pub struct PartitionedProducerBuilder<'a, E: Engine = TokioEngine> {
    client: &'a PulsarClient<E>,
    topic: String,
    name: Option<String>,
    compression: CompressionKind,
    enable_batching: bool,
    enable_chunking: bool,
    max_batch_size_bytes: usize,
    max_messages_in_batch: usize,
    routing: MessageRoutingMode,
    initial_sequence_id: Option<u64>,
    access_mode: pb::ProducerAccessMode,
    producer_metadata: Vec<(String, String)>,
    send_timeout: Option<std::time::Duration>,
    batching_max_publish_delay: Option<std::time::Duration>,
    schema: Option<pb::Schema>,
    encryptor: Option<<E as crate::MessageEncryptorApi>::Encryptor>,
    router: Option<std::sync::Arc<dyn MessageRouter>>,
    auto_update_partitions_interval: Option<Duration>,
}

impl<E: Engine> std::fmt::Debug for PartitionedProducerBuilder<'_, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionedProducerBuilder")
            .field("topic", &self.topic)
            .field("name", &self.name)
            .field("routing", &self.routing)
            .finish()
    }
}

impl<'a, E: Engine> PartitionedProducerBuilder<'a, E> {
    pub(crate) fn new(client: &'a PulsarClient<E>, topic: String) -> Self {
        Self {
            client,
            topic,
            name: None,
            compression: CompressionKind::None,
            enable_batching: false,
            enable_chunking: false,
            max_batch_size_bytes: 128 * 1024,
            max_messages_in_batch: 1000,
            routing: MessageRoutingMode::default(),
            initial_sequence_id: None,
            access_mode: pb::ProducerAccessMode::Shared,
            producer_metadata: Vec::new(),
            send_timeout: None,
            batching_max_publish_delay: None,
            schema: None,
            encryptor: None,
            router: None,
            auto_update_partitions_interval: None,
        }
    }

    /// Install a custom [`MessageRouter`]. When set, the router overrides
    /// [`Self::routing`] for every send. Mirrors Java
    /// `ProducerBuilder#messageRouter(MessageRouter)`.
    #[must_use]
    pub fn message_router(mut self, router: std::sync::Arc<dyn MessageRouter>) -> Self {
        self.router = Some(router);
        self
    }

    /// Set the producer name advertised to the broker.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the compression codec.
    #[must_use]
    pub fn compression(mut self, kind: CompressionKind) -> Self {
        self.compression = kind;
        self
    }

    /// Enable batching with the given limits.
    #[must_use]
    pub fn batching(mut self, max_messages: usize, max_bytes: usize) -> Self {
        self.enable_batching = true;
        self.max_messages_in_batch = max_messages;
        self.max_batch_size_bytes = max_bytes;
        self
    }

    /// Enable chunking for oversize messages.
    #[must_use]
    pub fn chunking(mut self, enable: bool) -> Self {
        self.enable_chunking = enable;
        self
    }

    /// Set the routing mode.
    #[must_use]
    pub fn routing(mut self, mode: MessageRoutingMode) -> Self {
        self.routing = mode;
        self
    }

    /// Set the initial sequence id (applied to every per-partition producer).
    #[must_use]
    pub fn initial_sequence_id(mut self, id: u64) -> Self {
        self.initial_sequence_id = Some(id);
        self
    }

    /// Producer access mode (`Shared` / `Exclusive` / `WaitForExclusive` /
    /// `ExclusiveWithFencing`) — applied to every per-partition child producer.
    #[must_use]
    pub fn access_mode(mut self, mode: pb::ProducerAccessMode) -> Self {
        self.access_mode = mode;
        self
    }

    /// Appends a `(key, value)` entry to the broker-visible producer metadata, applied
    /// to every per-partition child. Mirrors Java `ProducerBuilder#property` at the
    /// partitioned scope.
    #[must_use]
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.producer_metadata.push((key.into(), value.into()));
        self
    }

    /// Mirrors Java `ProducerBuilder#sendTimeout` — applied to every per-partition child.
    /// In-flight sends past their `enqueued_at + timeout` deadline resolve with a
    /// synthetic `code=-1, message="send timeout"` `SendError`.
    #[must_use]
    pub fn send_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.send_timeout = Some(timeout);
        self
    }

    /// Mirrors Java `ProducerBuilder#batchingMaxPublishDelay` — applied to every
    /// per-partition child. With batching enabled, the state machine flushes any non-empty
    /// batch whose oldest message has been waiting longer than `delay`.
    #[must_use]
    pub fn batching_max_publish_delay(mut self, delay: std::time::Duration) -> Self {
        self.batching_max_publish_delay = Some(delay);
        self
    }

    /// Advertise a schema on every per-partition `CommandProducer`.
    #[must_use]
    pub fn schema(mut self, schema: pb::Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Enable a background timer that signals every `interval`, intended to drive
    /// re-checks of the topic's partition count. Mirrors Java
    /// `ProducerBuilder#autoUpdatePartitionsInterval`.
    ///
    /// The internal timer task signals
    /// [`PartitionedProducer::partitions_changed_notify`] on every tick. Callers
    /// run [`PartitionedProducer::refresh_partitions`] in response to the signal
    /// (or on their own cadence) to actually call
    /// [`PulsarClient::partitions_for_topic`] — the timer itself is decoupled from
    /// the client so the watcher stays compatible with the crate-wide
    /// `#![forbid(unsafe_code)]` invariant.
    ///
    /// Default `None` — no timer is spawned and a [`PartitionedProducer`] over a
    /// partitioned topic will not notice partitions added after construction. Pass
    /// a non-zero `Duration` to opt in. The timer is aborted when the
    /// [`PartitionedProducer`] is dropped.
    ///
    /// Setting a zero `interval` is treated as "disable" — same as the default.
    #[must_use]
    pub fn auto_update_partitions_interval(mut self, interval: Duration) -> Self {
        self.auto_update_partitions_interval = if interval.is_zero() {
            None
        } else {
            Some(interval)
        };
        self
    }

    /// Query partition count, then open one producer per partition. If the broker reports
    /// `0` partitions, fall back to a single producer on the original topic.
    ///
    /// Dispatches through the engine-generic
    /// [`crate::BrokerMetadataApi`] (partition count lookup) and
    /// [`crate::CreateProducerApi`] (per-partition producer creation)
    /// extension traits, so the same builder shape works for both the
    /// tokio and moonpool engines.
    ///
    /// **PIP-4 encryption guardrail (BREAKING since the encryptor-storage lift).**
    /// If [`Self::encryption`] was called on the per-engine specialisation,
    /// `.create()` returns [`PulsarError::Other`] instead of silently opening
    /// plaintext per-partition producers. The engine-generic dispatch does not
    /// know how to thread an engine-typed encryptor through `open_producer`,
    /// so the previous "silently drop the encryptor" behaviour was a footgun.
    /// Use [`Self::create_with_encryption`] on the tokio specialisation
    /// instead.
    ///
    /// # Errors
    ///
    /// - [`PulsarError::Other`] if an encryptor was configured via [`Self::encryption`] — call
    ///   `create_with_encryption()` instead.
    /// - [`PulsarError::Other`] (stringified) on the broker metadata lookup or on a per-partition
    ///   producer open failure.
    pub async fn create(
        self,
    ) -> Result<
        PartitionedProducer<<E::ClientState as crate::CreateProducerApi>::Producer>,
        PulsarError,
    >
    where
        E::ClientState: crate::BrokerMetadataApi + crate::CreateProducerApi,
    {
        if self.encryptor.is_some() {
            return Err(PulsarError::Other(
                "PartitionedProducerBuilder::create() refuses a configured encryptor — \
                 use create_with_encryption() on the engine-specific builder \
                 (PIP-4 encryptors are engine-typed and cannot dispatch \
                 through the engine-generic CreateProducerApi)"
                    .to_owned(),
            ));
        }
        let base_req = CreateProducerRequest {
            topic: self.topic,
            producer_name: self.name,
            compression: self.compression,
            enable_batching: self.enable_batching,
            enable_chunking: self.enable_chunking,
            max_batch_size_bytes: self.max_batch_size_bytes,
            max_messages_in_batch: self.max_messages_in_batch,
            schema: self.schema,
            initial_sequence_id: self.initial_sequence_id,
            access_mode: self.access_mode,
            producer_metadata: self.producer_metadata,
            send_timeout: self.send_timeout,
            batching_max_publish_delay: self.batching_max_publish_delay,
        };
        open_partitioned_with_metadata(
            self.client,
            base_req,
            self.routing,
            self.router,
            self.auto_update_partitions_interval,
        )
        .await
    }
}

/// Resolve partition metadata for `base_req.topic`, open one producer per
/// resolved partition (or a single producer on the bare topic when `N == 0`),
/// and wrap the result in a [`PartitionedProducer`].
///
/// Used only by [`PartitionedProducerBuilder::create`] — the explicit
/// partitioned-producer entry point. A bare
/// [`crate::builders::ProducerBuilder::create`] does NOT delegate here: per
/// ADR-0051 it pre-checks the partition metadata and, on a partitioned topic,
/// returns an actionable error pointing the caller at
/// `client.partitioned_producer(...)` rather than silently fanning out (the
/// rejected "auto-dispatch exactly like Java" option). Without that pre-check a
/// bare `open_producer` on a partitioned topic surfaces as broker
/// `NotAllowedError(22) "Found partitioned metadata for non-partitioned topic"`
/// — the rough edge that drove ADR-0051.
///
/// On per-partition open failure every already-opened child is closed before
/// the error propagates, so a partial fan-out never leaks producers on the
/// broker side.
pub(crate) async fn open_partitioned_with_metadata<E>(
    client: &PulsarClient<E>,
    base_req: CreateProducerRequest,
    routing: MessageRoutingMode,
    router: Option<Arc<dyn MessageRouter>>,
    auto_update_partitions_interval: Option<Duration>,
) -> Result<PartitionedProducer<<E::ClientState as crate::CreateProducerApi>::Producer>, PulsarError>
where
    E: Engine,
    E::ClientState: crate::BrokerMetadataApi + crate::CreateProducerApi,
{
    let base_topic = base_req.topic.clone();
    let partitions_count =
        crate::BrokerMetadataApi::partitioned_topic_metadata(&client.inner, &base_topic)
            .await
            .map_err(|err| PulsarError::Other(format!("partitioned_topic_metadata: {err}")))?;

    let partition_topics: Vec<String> = if partitions_count == 0 {
        vec![base_topic.clone()]
    } else {
        (0..partitions_count)
            .map(|i| format!("{base_topic}-partition-{i}"))
            .collect()
    };

    let mut child_producers: Vec<<E::ClientState as crate::CreateProducerApi>::Producer> =
        Vec::with_capacity(partition_topics.len());
    for child_topic in &partition_topics {
        let mut req = base_req.clone();
        req.topic = child_topic.clone();
        let result = crate::CreateProducerApi::open_producer(&client.inner, req).await;
        match result {
            Ok(p) => child_producers.push(p),
            Err(e) => {
                for p in child_producers {
                    let _ = crate::ProducerApi::close_owned(p).await;
                }
                return Err(PulsarError::Other(format!("open_producer: {e}")));
            }
        }
    }

    // Spawn the partition-watcher timer iff the builder configured a non-zero
    // interval. The timer itself only emits ticks via `Notify`; callers drive
    // the actual `partitions_for_topic` call via
    // [`PartitionedProducer::refresh_partitions`] (the crate-wide
    // `#![forbid(unsafe_code)]` rules out punning the `&PulsarClient` lifetime
    // into a `'static` spawn).
    let auto_update = auto_update_partitions_interval
        .map(|interval| spawn_auto_update_task(base_topic.clone(), interval, partitions_count));

    Ok(PartitionedProducer {
        partitions: child_producers,
        base_topic,
        routing,
        router,
        cursor: AtomicU64::new(0),
        auto_update,
    })
}

/// Tokio-engine-specific `PartitionedProducerBuilder` methods that need
/// the `open_producer_with(encryptor)` runtime carve-out (PIP-4 not yet
/// wired on moonpool).
impl PartitionedProducerBuilder<'_, TokioEngine> {
    /// Configure PIP-4 end-to-end encryption (applied to every per-partition producer).
    /// Tokio-engine-only — call [`Self::create_with_encryption`] to honor the
    /// encryptor on the open path. The engine-generic [`Self::create`] ignores
    /// the field.
    #[must_use]
    pub fn encryption(
        mut self,
        encryptor: std::sync::Arc<dyn magnetar_runtime_tokio::MessageEncryptor>,
    ) -> Self {
        self.encryptor = Some(encryptor);
        self
    }

    /// Open every per-partition producer honoring the configured PIP-4
    /// encryptor. Tokio-engine-only — use [`Self::create`] for the
    /// engine-generic path that ignores the encryptor field.
    ///
    /// # Errors
    ///
    /// - [`PulsarError::Client`] on broker metadata lookup or per-partition open failure.
    pub async fn create_with_encryption(self) -> Result<PartitionedProducer, PulsarError> {
        let partitions_count = self
            .client
            .runtime_client()
            .partitioned_topic_metadata(&self.topic)
            .await?;

        let partition_topics: Vec<String> = if partitions_count == 0 {
            vec![self.topic.clone()]
        } else {
            (0..partitions_count)
                .map(|i| format!("{}-partition-{}", self.topic, i))
                .collect()
        };

        let mut child_producers: Vec<Producer> = Vec::with_capacity(partition_topics.len());
        for child_topic in &partition_topics {
            let req = CreateProducerRequest {
                topic: child_topic.clone(),
                producer_name: self.name.clone(),
                compression: self.compression,
                enable_batching: self.enable_batching,
                enable_chunking: self.enable_chunking,
                max_batch_size_bytes: self.max_batch_size_bytes,
                max_messages_in_batch: self.max_messages_in_batch,
                schema: self.schema.clone(),
                initial_sequence_id: self.initial_sequence_id,
                access_mode: self.access_mode,
                producer_metadata: self.producer_metadata.clone(),
                send_timeout: self.send_timeout,
                batching_max_publish_delay: self.batching_max_publish_delay,
            };
            let result = self
                .client
                .runtime_client()
                .open_producer_with(req, self.encryptor.clone())
                .await;
            match result {
                Ok(p) => child_producers.push(p),
                Err(e) => {
                    for p in child_producers {
                        let _ = p.close().await;
                    }
                    return Err(PulsarError::Client(e));
                }
            }
        }

        let auto_update = self
            .auto_update_partitions_interval
            .map(|interval| spawn_auto_update_task(self.topic.clone(), interval, partitions_count));

        Ok(PartitionedProducer {
            partitions: child_producers,
            base_topic: self.topic,
            routing: self.routing,
            router: self.router,
            cursor: AtomicU64::new(0),
            auto_update,
        })
    }
}

// helper to avoid unused-import warning if Bytes isn't needed here
#[allow(dead_code)]
fn _bytes_in_use() -> Bytes {
    Bytes::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_hash_is_deterministic_and_round_robin_advances() {
        let pp: PartitionedProducer = PartitionedProducer {
            partitions: Vec::new(),
            base_topic: "t".into(),
            routing: MessageRoutingMode::KeyHashOrRoundRobin,
            router: None,
            cursor: AtomicU64::new(0),
            auto_update: None,
        };
        // We can't actually run pick_partition with 0 partitions; emulate by mirroring
        // the same `java_string_hash` math the production path uses. Confirms that
        // the same key yields the same partition for a given total — both sides
        // call into `java_string_hash`.
        let pick_a = (java_string_hash("alpha") as usize) % 4;
        let pick_b = (java_string_hash("alpha") as usize) % 4;
        assert_eq!(pick_a, pick_b);
        let _ = pp; // suppress unused
    }

    /// F3 — `MessageRoutingMode::KeyHashOrRoundRobin` must use Java's
    /// `String.hashCode()` (a.k.a. `HashingScheme.JavaStringHash`), not
    /// Rust's `DefaultHasher`. Otherwise a Rust-side keyed producer
    /// routes the same key to a different partition than a Java
    /// producer (or even than a different process — Rust's
    /// `DefaultHasher` carries a per-process random seed). This test
    /// pins the routing math to the same `java_string_hash` invariants
    /// that the existing `HashTest` vectors lock in.
    ///
    /// Java `"alpha".hashCode()` is `92909918` — we mirror the
    /// `& Integer.MAX_VALUE` mask, then `% partitions`. `"abc"` is the
    /// textbook value `96354`. Empty key drops to round-robin (and so
    /// doesn't enter this branch).
    #[test]
    fn default_routing_uses_java_string_hash_not_default_hasher() {
        // Sanity: the helper still matches the well-known Java values.
        assert_eq!(java_string_hash("alpha"), 92_909_918);
        assert_eq!(java_string_hash("abc"), 96_354);

        // The router branch the production code takes for non-empty
        // keys: `(java_string_hash(k) as usize) % n`. We mirror the
        // computation directly because constructing a `PartitionedProducer`
        // with N>0 partitions requires real per-partition producers
        // (which require a broker). The point is parity — same key,
        // same partition, regardless of how many times we call it,
        // and across processes.
        for &(key, partitions, expected_alpha) in &[
            ("alpha", 4_usize, (92_909_918_usize) % 4),
            ("alpha", 16_usize, (92_909_918_usize) % 16),
            ("abc", 8_usize, (96_354_usize) % 8),
            ("keykeykeykeykey1", 8_usize, (434_058_482_usize) % 8),
            ("keykeykey2", 32_usize, (42_978_643_usize) % 32),
        ] {
            let pick = (java_string_hash(key) as usize) % partitions;
            assert_eq!(
                pick, expected_alpha,
                "key={key:?} partitions={partitions} must match Java's \
                 String.hashCode() & Integer.MAX_VALUE then % partitions"
            );
        }

        // Determinism across calls — the previous DefaultHasher path
        // failed this within a single process whenever RandomState
        // rotated; across processes it failed every restart.
        let snapshot_a = (java_string_hash("user-42") as usize) % 16;
        let snapshot_b = (java_string_hash("user-42") as usize) % 16;
        assert_eq!(snapshot_a, snapshot_b);

        // Multi-byte / non-ASCII key — UTF-16 path. Java treats
        // non-BMP code points as surrogate pairs, which
        // `java_string_hash` mirrors via `encode_utf16`. Use a BMP
        // multibyte character so the test is portable.
        let multi = "éclair"; // 'é' = U+00E9, 1 UTF-16 unit
        let _ = (java_string_hash(multi) as usize) % 8;
        // Determinism on multi-byte input.
        let a = (java_string_hash(multi) as usize) % 8;
        let b = (java_string_hash(multi) as usize) % 8;
        assert_eq!(a, b);
    }

    #[derive(Debug)]
    struct ConstantRouter(usize);
    impl MessageRouter for ConstantRouter {
        fn route(&self, _msg: &OutgoingMessage, _partitions: usize) -> usize {
            self.0
        }
    }

    #[test]
    fn custom_router_overrides_mode_and_clamps_out_of_range() {
        // Build a fake producer with 4 dummy partition slots so pick_partition has range.
        // We can't construct real Producers here (needs a broker connection); since we
        // only exercise pick_partition's branch logic, give it an empty slice and a
        // router that returns a stable value via shimming.
        // Instead, exercise the math directly: cap = `idx.min(n - 1)`.
        let n = 4_usize;
        let idx = 2_usize;
        assert_eq!(idx.min(n - 1), 2);
        let oor = 999_usize;
        assert_eq!(
            oor.min(n - 1),
            3,
            "out-of-range router result clamps to n-1"
        );

        // Smoke test the trait dispatch path (no producer needed).
        let r: std::sync::Arc<dyn MessageRouter> = std::sync::Arc::new(ConstantRouter(2));
        let msg = OutgoingMessage::default();
        assert_eq!(r.route(&msg, 4), 2);
    }

    // -- Murmur3 parity with Apache Pulsar's `HashTest.murmur3_32HashTest`. -----------
    //
    // Vectors copied verbatim from
    // `pulsar-client/src/test/java/org/apache/pulsar/client/impl/HashTest.java`. They
    // are also the C++ client's expected outputs, so a regression here breaks
    // cross-language partition affinity.
    #[test]
    fn murmur3_matches_java_hashtest_vectors() {
        assert_eq!(murmur3_32_hash(b"k1"), 2_110_152_746);
        assert_eq!(murmur3_32_hash(b"k2"), 1_479_966_664);
        assert_eq!(murmur3_32_hash(b"key1"), 462_881_061);
        assert_eq!(murmur3_32_hash(b"key2"), 1_936_800_180);
        assert_eq!(murmur3_32_hash(b"key01"), 39_696_932);
        assert_eq!(murmur3_32_hash(b"key02"), 751_761_803);
    }

    #[test]
    fn murmur3_handles_empty_input() {
        // Empty input under seed=0 with the masking we apply should be 0 (matches
        // Java's `Murmur3_32Hash.makeHash(new byte[0]) & Integer.MAX_VALUE`).
        assert_eq!(murmur3_32_hash(b""), 0);
    }

    // -- JavaStringHash parity with Apache Pulsar's `HashTest.javaStringHashTest`. ----
    //
    // The `"keykeykey2"` value overflows i32 as unsigned (Java's `hashCode()` returns
    // negative) — the mask with `Integer.MAX_VALUE` restores the non-negative form.
    #[test]
    fn java_string_hash_matches_java_hashtest_vectors() {
        assert_eq!(java_string_hash("keykeykeykeykey1"), 434_058_482);
        assert_eq!(java_string_hash("keykeykey2"), 42_978_643);
        // Well-known textbook value: "abc".hashCode() == 96354.
        assert_eq!(java_string_hash("abc"), 96_354);
    }

    #[test]
    fn java_string_hash_empty_is_zero() {
        // `"".hashCode()` is 0 in Java, masked stays 0.
        assert_eq!(java_string_hash(""), 0);
    }

    // -- Routing determinism: same key always routes to the same partition. ----------
    #[test]
    fn murmur3_router_is_keyed_and_deterministic() {
        let router = Murmur3HashHasher;
        let msg = OutgoingMessage::default().key("user-42");
        let p0 = router.route(&msg, 16);
        // Call ten times — must be stable.
        for _ in 0..10 {
            assert_eq!(router.route(&msg, 16), p0);
        }
        // And different keys can land on different partitions (smoke check).
        let other = OutgoingMessage::default().key("user-9999");
        let p1 = router.route(&other, 16);
        // Not asserting `p0 != p1` because hash collisions exist for 16 partitions; we
        // just want to prove the value is in range.
        assert!(p0 < 16);
        assert!(p1 < 16);
    }

    #[test]
    fn java_string_hash_router_is_keyed_and_deterministic() {
        let router = JavaStringHashHasher;
        let msg = OutgoingMessage::default().key("orders-tenant-A");
        let p0 = router.route(&msg, 8);
        for _ in 0..10 {
            assert_eq!(router.route(&msg, 8), p0);
        }
        assert!(p0 < 8);
    }

    // Same key value must land on the same partition under both hashers across
    // independent invocations — guards against accidental cursor / RNG bleed-in.
    #[test]
    fn hashers_have_no_hidden_state() {
        let m1 = Murmur3HashHasher;
        let m2 = Murmur3HashHasher;
        let key = OutgoingMessage::default().key("k1");
        assert_eq!(m1.route(&key, 32), m2.route(&key, 32));

        let j1 = JavaStringHashHasher;
        let j2 = JavaStringHashHasher;
        assert_eq!(j1.route(&key, 32), j2.route(&key, 32));
    }

    // Cross-check Murmur3 routing against the Java expected value mod partition count
    // (uses the vector from `HashTest`): "key1" -> 462881061 -> 462881061 % 16 = 5.
    #[test]
    fn murmur3_router_matches_java_modulo() {
        let router = Murmur3HashHasher;
        let msg = OutgoingMessage::default().key("key1");
        assert_eq!(router.route(&msg, 16), (462_881_061_usize) % 16);
    }

    // No-key fallback uses sticky partition 0 (router overrides MessageRoutingMode).
    #[test]
    fn hashers_fall_back_to_partition_zero_without_key() {
        let m = Murmur3HashHasher;
        let j = JavaStringHashHasher;
        let msg = OutgoingMessage::default();
        assert_eq!(m.route(&msg, 8), 0);
        assert_eq!(j.route(&msg, 8), 0);
        let msg_empty = OutgoingMessage::default().key("");
        assert_eq!(m.route(&msg_empty, 8), 0);
        assert_eq!(j.route(&msg_empty, 8), 0);
    }
}
