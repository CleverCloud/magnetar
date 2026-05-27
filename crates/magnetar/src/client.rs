// SPDX-License-Identifier: Apache-2.0

//! Ergonomic top-level client built on the tokio engine.
//!
//! Wraps [`magnetar_runtime_tokio::Client`] with a builder API plus simple
//! `producer(topic).create()` / `consumer(topic).subscription(s).subscribe()`
//! constructors so the common path doesn't expose raw protocol types like
//! [`magnetar_proto::conn::CreateProducerRequest`] unless the user wants
//! them.

use bytes::Bytes;
use magnetar_proto::pb;
use magnetar_runtime_tokio::{Client, ClientError};

/// Result alias used inside this module.
type Result<T, E = PulsarError> = std::result::Result<T, E>;

/// Top-level errors surfaced by the façade.
#[derive(Debug, thiserror::Error)]
pub enum PulsarError {
    /// Underlying tokio engine error.
    #[error("client error: {0}")]
    Client(#[from] ClientError),
    /// Configuration error before any I/O happened.
    #[error("configuration error: {0}")]
    Config(String),
    /// Schema encode / decode error from a [`crate::TypedProducer`] / [`crate::TypedConsumer`].
    #[error("schema error: {0}")]
    Schema(#[from] magnetar_proto::schema::SchemaError),
    /// Engine-agnostic error surfaced by a generic façade method that
    /// dispatches through an extension trait (e.g.
    /// [`crate::TransactionApi`]). Carries the runtime's error message
    /// stringified — the per-engine error type is recovered through
    /// the runtime crate when full-fidelity diagnostics are needed.
    /// Phase 4 of the D1 lift train (ADR-0026 §D1).
    #[error("engine error: {0}")]
    Other(String),
}

/// Convenience alias for outgoing application messages.
///
/// Wraps a `Bytes` payload plus optional [`pb::MessageMetadata`] overrides.
/// The producer state machine assigns the sequence id and stamps publish
/// time on send.
#[derive(Debug, Clone, Default)]
pub struct OutgoingMessage {
    /// Application payload bytes.
    pub payload: Bytes,
    /// Optional message key (sets `partition_key`).
    pub key: Option<String>,
    /// Optional ordering key.
    pub ordering_key: Option<Bytes>,
    /// Optional event time (millis since epoch).
    pub event_time_ms: Option<u64>,
    /// Optional per-message properties.
    pub properties: Vec<(String, String)>,
    /// Optional absolute deliver-at time (millis since epoch). Mirrors Java's
    /// `TypedMessageBuilder#deliverAt`; the broker holds the message until the deadline.
    pub deliver_at_ms: Option<i64>,
    /// Optional explicit replication cluster list. Mirrors Java's
    /// `TypedMessageBuilder#replicationClusters`. An empty vector means "use the namespace
    /// default"; pass `vec!["__local__".to_owned()]` to opt out of replication entirely
    /// (Java's `disableReplication()` writes the same sentinel).
    pub replication_clusters: Vec<String>,
    /// Optional transaction id (PIP-31). When set, the broker treats this publish as part
    /// of the open transaction. Mirrors Java `Producer#newMessage(Transaction)`.
    pub txn_id: Option<magnetar_proto::TxnId>,
}

impl OutgoingMessage {
    /// Construct an `OutgoingMessage` from raw payload bytes.
    pub fn with_payload(payload: impl Into<Bytes>) -> Self {
        Self {
            payload: payload.into(),
            ..Self::default()
        }
    }

    /// Set the routing key.
    #[must_use]
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set the ordering key.
    #[must_use]
    pub fn ordering_key(mut self, key: impl Into<Bytes>) -> Self {
        self.ordering_key = Some(key.into());
        self
    }

    /// Set the event time (milliseconds since epoch).
    #[must_use]
    pub fn event_time_ms(mut self, ts: u64) -> Self {
        self.event_time_ms = Some(ts);
        self
    }

    /// Append a property.
    #[must_use]
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.push((key.into(), value.into()));
        self
    }

    /// Mirrors `TypedMessageBuilder#deliverAt`. The broker holds the message until the
    /// supplied UNIX-epoch millisecond deadline before dispatching it.
    #[must_use]
    pub fn deliver_at_ms(mut self, ts_ms: i64) -> Self {
        self.deliver_at_ms = Some(ts_ms);
        self
    }

    /// Mirrors `TypedMessageBuilder#deliverAfter`. Adds `delay_ms` to the
    /// current wall-clock time and stamps the resulting absolute deadline
    /// on the message.
    ///
    /// # Determinism warning
    ///
    /// This convenience reads the host's `SystemTime::now`. Code that runs
    /// under `MoonpoolEngine` and depends on byte-identical wire output
    /// across simulator seeds should use [`Self::deliver_after_ms_from`]
    /// (caller-supplied `now_ms`) or [`Self::deliver_at_ms`] (absolute
    /// timestamp) instead — the broker compares `deliver_at_ms` to its
    /// own clock, so what matters for replay parity is that the *client*
    /// stamp is deterministic. Tracked in `docs/follow-ups.md` under
    /// the 2026-05-27 audit section.
    #[must_use]
    pub fn deliver_after_ms(mut self, delay_ms: i64) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        self.deliver_at_ms = Some(now.saturating_add(delay_ms));
        self
    }

    /// Engine-agnostic variant of [`Self::deliver_after_ms`]. Stamps the
    /// message with `now_ms + delay_ms` as the absolute UNIX-epoch
    /// millisecond deadline. The caller supplies `now_ms` — under
    /// `MoonpoolEngine` this should come from the engine's virtual wall
    /// clock so the resulting wire bytes are deterministic across seeds.
    ///
    /// Tracked in `docs/follow-ups.md` under the 2026-05-27 audit
    /// section ("Determinism warning" entry).
    #[must_use]
    pub fn deliver_after_ms_from(mut self, now_ms: i64, delay_ms: i64) -> Self {
        self.deliver_at_ms = Some(now_ms.saturating_add(delay_ms));
        self
    }

    /// Mirrors `TypedMessageBuilder#replicationClusters`. Overrides the namespace-default
    /// replication list with the given clusters for this message only.
    #[must_use]
    pub fn replication_clusters(mut self, clusters: Vec<String>) -> Self {
        self.replication_clusters = clusters;
        self
    }

    /// Mirrors `TypedMessageBuilder#disableReplication`. Sentinel for "do not replicate this
    /// message to any other cluster" — the broker recognises the `__local__` cluster id.
    #[must_use]
    pub fn disable_replication(mut self) -> Self {
        self.replication_clusters = vec!["__local__".to_owned()];
        self
    }

    /// Mirrors Java `Producer#newMessage(Transaction)`. Stamps the supplied transaction id
    /// on the publish so the broker treats it as part of the open transaction (PIP-31).
    #[must_use]
    pub fn txn(mut self, txn_id: magnetar_proto::TxnId) -> Self {
        self.txn_id = Some(txn_id);
        self
    }

    /// Set the payload bytes. Mirrors Java `TypedMessageBuilder#value(byte[])` for the raw
    /// bytes case — schema-encoded values land here after the schema-aware layer serialises
    /// them. Lets the builder be constructed `OutgoingMessage::default().key(..).value(..)`
    /// without forcing the caller through [`Self::with_payload`].
    #[must_use]
    pub fn value(mut self, payload: impl Into<Bytes>) -> Self {
        self.payload = payload.into();
        self
    }

    /// Send this message through `producer` and return the in-flight
    /// [`magnetar_runtime_tokio::SendFut`]. Mirrors the terminal `send()` step of Java's
    /// `TypedMessageBuilder`: `producer.newMessage().key(..).value(..).send()`. Equivalent
    /// to `producer.send(msg.into())`, just chainable.
    pub fn send(
        self,
        producer: &magnetar_runtime_tokio::Producer,
    ) -> magnetar_runtime_tokio::SendFut {
        producer.send(self.into())
    }
}

impl From<OutgoingMessage> for magnetar_proto::producer::OutgoingMessage {
    fn from(msg: OutgoingMessage) -> Self {
        let mut metadata = pb::MessageMetadata::default();
        if let Some(k) = msg.key {
            metadata.partition_key = Some(k);
            metadata.partition_key_b64_encoded = Some(false);
        }
        if let Some(ok) = msg.ordering_key {
            metadata.ordering_key = Some(ok);
        }
        if let Some(ts) = msg.event_time_ms {
            metadata.event_time = Some(ts);
        }
        if let Some(ts) = msg.deliver_at_ms {
            metadata.deliver_at_time = Some(ts);
        }
        if !msg.replication_clusters.is_empty() {
            metadata.replicate_to = msg.replication_clusters;
        }
        for (k, v) in msg.properties {
            metadata.properties.push(pb::KeyValue { key: k, value: v });
        }
        let uncompressed_size = u32::try_from(msg.payload.len()).unwrap_or(u32::MAX);
        Self {
            payload: msg.payload,
            metadata,
            uncompressed_size,
            num_messages: 1,
            txn_id: msg.txn_id,
            source_message_id: None,
        }
    }
}

/// Per-topic seek target supplied by the closure passed to
/// [`crate::MultiTopicsConsumer::seek_per_partition`] (and the equivalent on
/// [`crate::PartitionedConsumer`]). Mirrors Java's
/// `Consumer#seek(Function<String, Object>)`, where the function returns either a
/// `MessageId` or a `Long` publish-time millis-since-epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekTarget {
    /// Seek the child consumer to a specific message id. Mirrors Java
    /// `Consumer#seek(MessageId)`.
    MessageId(magnetar_proto::MessageId),
    /// Seek the child consumer to a publish-time deadline (millis since UNIX epoch).
    /// Mirrors Java `Consumer#seek(long)`.
    PublishTimeMs(u64),
}

/// Java `ProducerInterceptor` SPI. Plug pipeline hooks in front of `Producer::send` to
/// inspect, mutate, or react to outgoing messages. Mirrors the Java
/// `org.apache.pulsar.client.api.interceptor.ProducerInterceptor` interface — `eligible`
/// gates whether the interceptor runs for a given message, `before_send` runs first
/// (mutating the [`OutgoingMessage`]), and `on_send_acknowledgement` fires after the
/// broker acks the publish (or the send errors out).
///
/// Each callback runs on the send path — keep them fast and non-blocking. Use
/// [`send_with_interceptors`] to chain a list of interceptors against an
/// [`OutgoingMessage`].
pub trait ProducerInterceptor: Send + Sync + std::fmt::Debug {
    /// Decide whether this interceptor applies to the given message. Default: always.
    fn eligible(&self, _msg: &OutgoingMessage) -> bool {
        true
    }

    /// Mutate the message before it is encoded and sent. Mirrors Java
    /// `ProducerInterceptor#beforeSend`.
    fn before_send(&self, msg: &mut OutgoingMessage);

    /// Fired after the broker acks the publish (or the send errors out). Mirrors Java
    /// `ProducerInterceptor#onSendAcknowledgement`. The default no-ops so most
    /// implementations only have to provide [`Self::before_send`].
    fn on_send_acknowledgement(
        &self,
        _msg: &OutgoingMessage,
        _outcome: Result<magnetar_proto::MessageId, &PulsarError>,
    ) {
    }
}

/// Send `msg` through `producer`, running every eligible [`ProducerInterceptor`] in
/// `interceptors` in order. Mirrors Java's interceptor-chain semantics: `eligible` is
/// evaluated against the *original* message, `before_send` runs in order on a single
/// message the chain progressively mutates, and `on_send_acknowledgement` fires on every
/// eligible interceptor regardless of whether the broker accepted the publish.
///
/// Use [`magnetar_runtime_tokio::Producer::send`] directly when no interceptors are
/// configured — this helper exists so callers can opt into the chain without weaving the
/// dispatch logic into the producer struct.
///
/// # Errors
///
/// Propagates the producer's error wrapped in [`PulsarError::Client`] after notifying
/// the chain.
pub async fn send_with_interceptors(
    producer: &magnetar_runtime_tokio::Producer,
    mut msg: OutgoingMessage,
    interceptors: &[std::sync::Arc<dyn ProducerInterceptor>],
) -> Result<magnetar_proto::MessageId, PulsarError> {
    let eligible: Vec<std::sync::Arc<dyn ProducerInterceptor>> = interceptors
        .iter()
        .filter(|i| i.eligible(&msg))
        .cloned()
        .collect();
    for i in &eligible {
        i.before_send(&mut msg);
    }
    let snapshot = msg.clone();
    let mapped: Result<magnetar_proto::MessageId, PulsarError> =
        producer.send(msg.into()).await.map_err(PulsarError::Client);
    for i in &eligible {
        let outcome: Result<magnetar_proto::MessageId, &PulsarError> = match &mapped {
            Ok(id) => Ok(*id),
            Err(err) => Err(err),
        };
        i.on_send_acknowledgement(&snapshot, outcome);
    }
    mapped
}

/// Java `ConsumerInterceptor` SPI. Plug receive-side hooks behind `Consumer::receive`
/// to inspect / mutate incoming messages and observe ack outcomes. Mirrors
/// `org.apache.pulsar.client.api.interceptor.ConsumerInterceptor`:
/// - `before_consume` runs on every received message and may mutate it.
/// - `on_acknowledge` fires on every individual / batch ack.
/// - `on_acknowledge_cumulative` fires on every cumulative ack.
/// - `on_negative_acks_send` fires when the runtime forwards a redeliver-unacknowledged command
///   (negative ack with delay or immediate).
///
/// Each callback runs on the receive / ack path — keep them fast and non-blocking. Use
/// [`receive_with_interceptors`] to chain a list against a [`magnetar_runtime_tokio::Consumer`].
pub trait ConsumerInterceptor: Send + Sync + std::fmt::Debug {
    /// Inspect and optionally mutate the incoming message before it is handed back to
    /// the user. Mirrors Java `ConsumerInterceptor#beforeConsume`.
    fn before_consume(&self, msg: &mut IncomingMessage);

    /// Fired after an individual or batch ack completes (success or error). Mirrors Java
    /// `ConsumerInterceptor#onAcknowledge`.
    fn on_acknowledge(
        &self,
        _message_id: magnetar_proto::MessageId,
        _outcome: Result<(), &PulsarError>,
    ) {
    }

    /// Fired after a cumulative ack completes. Mirrors Java
    /// `ConsumerInterceptor#onAcknowledgeCumulative`.
    fn on_acknowledge_cumulative(
        &self,
        _message_id: magnetar_proto::MessageId,
        _outcome: Result<(), &PulsarError>,
    ) {
    }

    /// Fired when the runtime forwards a `CommandRedeliverUnacknowledgedMessages` for one
    /// or more message ids. Mirrors Java `ConsumerInterceptor#onNegativeAcksSend`.
    fn on_negative_acks_send(&self, _message_ids: &[magnetar_proto::MessageId]) {}
}

/// Receive the next message via `consumer`, running every [`ConsumerInterceptor`] in
/// `interceptors` against the payload before it is returned. Mirrors Java's interceptor
/// chain on the receive path — every interceptor's `before_consume` runs in order on a
/// single progressively-mutated message.
///
/// # Errors
///
/// Propagates the underlying receive error wrapped in [`PulsarError::Client`].
pub async fn receive_with_interceptors(
    consumer: &magnetar_runtime_tokio::Consumer,
    interceptors: &[std::sync::Arc<dyn ConsumerInterceptor>],
) -> Result<IncomingMessage, PulsarError> {
    let raw = consumer.receive().await.map_err(PulsarError::Client)?;
    let mut msg: IncomingMessage = raw.into();
    for i in interceptors {
        i.before_consume(&mut msg);
    }
    Ok(msg)
}

/// Ack via `consumer` and notify every interceptor of the outcome. Mirrors Java's
/// post-ack callback chain. Returns whatever the runtime ack returned, mapped into a
/// [`PulsarError`].
pub async fn ack_with_interceptors(
    consumer: &magnetar_runtime_tokio::Consumer,
    message_id: magnetar_proto::MessageId,
    interceptors: &[std::sync::Arc<dyn ConsumerInterceptor>],
) -> Result<(), PulsarError> {
    let result: Result<(), PulsarError> =
        consumer.ack(message_id).await.map_err(PulsarError::Client);
    for i in interceptors {
        let outcome: Result<(), &PulsarError> = match &result {
            Ok(()) => Ok(()),
            Err(err) => Err(err),
        };
        i.on_acknowledge(message_id, outcome);
    }
    result
}

/// Cumulative ack variant of [`ack_with_interceptors`]. Notifies via
/// `on_acknowledge_cumulative` instead of `on_acknowledge`.
pub async fn ack_cumulative_with_interceptors(
    consumer: &magnetar_runtime_tokio::Consumer,
    message_id: magnetar_proto::MessageId,
    interceptors: &[std::sync::Arc<dyn ConsumerInterceptor>],
) -> Result<(), PulsarError> {
    let result: Result<(), PulsarError> = consumer
        .ack_cumulative(message_id)
        .await
        .map_err(PulsarError::Client);
    for i in interceptors {
        let outcome: Result<(), &PulsarError> = match &result {
            Ok(()) => Ok(()),
            Err(err) => Err(err),
        };
        i.on_acknowledge_cumulative(message_id, outcome);
    }
    result
}

/// Extension trait that gives [`magnetar_runtime_tokio::Producer`] the Java-symmetric
/// `producer.new_message().key(..).value(..).send().await` entry point.
///
/// Bring it into scope with `use magnetar::ProducerExt;`.
pub trait ProducerExt {
    /// Start a new [`OutgoingMessage`] bound to this producer. Chain the same setters as
    /// `OutgoingMessage` ([`OutgoingMessage::key`], [`OutgoingMessage::value`],
    /// [`OutgoingMessage::event_time_ms`], etc.) and finish with `.send().await`.
    fn new_message(&self) -> MessageBuilder<'_>;
}

impl ProducerExt for magnetar_runtime_tokio::Producer {
    fn new_message(&self) -> MessageBuilder<'_> {
        MessageBuilder {
            producer: self,
            msg: OutgoingMessage::default(),
        }
    }
}

/// Producer-bound counterpart to [`OutgoingMessage`]. Mirrors Java's
/// `TypedMessageBuilder` — the producer is captured at construction so the terminal `send()`
/// has no extra argument.
#[derive(Debug)]
pub struct MessageBuilder<'a> {
    producer: &'a magnetar_runtime_tokio::Producer,
    msg: OutgoingMessage,
}

impl MessageBuilder<'_> {
    /// Set the routing key. See [`OutgoingMessage::key`].
    #[must_use]
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.msg = self.msg.key(key);
        self
    }

    /// Set the ordering key. See [`OutgoingMessage::ordering_key`].
    #[must_use]
    pub fn ordering_key(mut self, key: impl Into<Bytes>) -> Self {
        self.msg = self.msg.ordering_key(key);
        self
    }

    /// Set the event time (millis since epoch). See [`OutgoingMessage::event_time_ms`].
    #[must_use]
    pub fn event_time_ms(mut self, ts: u64) -> Self {
        self.msg = self.msg.event_time_ms(ts);
        self
    }

    /// Append a property. See [`OutgoingMessage::property`].
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

    /// See [`OutgoingMessage::deliver_after_ms`].
    #[must_use]
    pub fn deliver_after_ms(mut self, delay_ms: i64) -> Self {
        self.msg = self.msg.deliver_after_ms(delay_ms);
        self
    }

    /// See [`OutgoingMessage::deliver_after_ms_from`]. Engine-agnostic
    /// alternative to [`Self::deliver_after_ms`] for moonpool-deterministic
    /// callers — see the determinism warning on
    /// [`OutgoingMessage::deliver_after_ms`].
    #[must_use]
    pub fn deliver_after_ms_from(mut self, now_ms: i64, delay_ms: i64) -> Self {
        self.msg = self.msg.deliver_after_ms_from(now_ms, delay_ms);
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

    /// Submit the message to the producer captured at `new_message()` time. Mirrors Java's
    /// terminal `TypedMessageBuilder#send`.
    pub fn send(self) -> magnetar_runtime_tokio::SendFut {
        self.producer.send(self.msg.into())
    }
}

/// Convenience alias for an incoming message handed back to the caller.
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    /// Message id assigned by the broker.
    pub id: magnetar_proto::types::MessageId,
    /// Pulsar `MessageMetadata` for the message. Refcounted (Arc) so the
    /// batched-delivery path inside the consumer state machine can share
    /// one parsed metadata across every sub-message of a batch instead of
    /// deep-cloning per message. Field access works transparently
    /// (`Arc` derefs).
    pub metadata: std::sync::Arc<pb::MessageMetadata>,
    /// Application payload bytes (post-decompression / post-decryption).
    pub payload: Bytes,
    /// Broker-supplied redelivery count.
    pub redelivery_count: u32,
    /// PIP-90 `BrokerEntryMetadata`. `None` when the broker did not stamp one (older
    /// brokers / disabled namespace policy). Carries the broker's wall-clock timestamp
    /// and per-topic index — useful for routing, dedup, and exactly-once-ish flows.
    pub broker_entry_metadata: Option<std::sync::Arc<pb::BrokerEntryMetadata>>,
}

impl IncomingMessage {
    /// Mirrors Java `Message#getKey`. Returns `None` for keyless messages.
    #[must_use]
    pub fn key(&self) -> Option<&str> {
        self.metadata.partition_key.as_deref()
    }

    /// Mirrors Java `Message#hasKey`.
    #[must_use]
    pub fn has_key(&self) -> bool {
        self.metadata.partition_key.is_some()
    }

    /// Mirrors Java `Message#getOrderingKey`. Returns `None` if unset.
    #[must_use]
    pub fn ordering_key(&self) -> Option<&Bytes> {
        self.metadata.ordering_key.as_ref()
    }

    /// Mirrors Java `Message#getPublishTime` — millis since the UNIX epoch as stamped by
    /// the producer's state machine at queue time.
    #[must_use]
    pub fn publish_time_ms(&self) -> u64 {
        self.metadata.publish_time
    }

    /// Mirrors Java `Message#getEventTime`. Returns `0` if the producer didn't stamp one
    /// (Java returns `0` in the same situation).
    #[must_use]
    pub fn event_time_ms(&self) -> u64 {
        self.metadata.event_time.unwrap_or(0)
    }

    /// Mirrors Java `Message#getSequenceId`. The sequence id assigned by the producer's
    /// state machine (visible alongside the broker-assigned message id).
    #[must_use]
    pub fn sequence_id(&self) -> u64 {
        self.metadata.sequence_id
    }

    /// Mirrors Java `Message#getProducerName`.
    #[must_use]
    pub fn producer_name(&self) -> &str {
        &self.metadata.producer_name
    }

    /// Mirrors Java `Message#getProperty(String)`. Returns the value for the first matching
    /// property entry, or `None` if absent.
    #[must_use]
    pub fn property(&self, key: &str) -> Option<&str> {
        self.metadata
            .properties
            .iter()
            .find(|kv| kv.key == key)
            .map(|kv| kv.value.as_str())
    }

    /// Mirrors Java `Message#getProperties` — every (key, value) pair on the message.
    pub fn properties(&self) -> impl Iterator<Item = (&str, &str)> {
        self.metadata
            .properties
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_str()))
    }

    /// Mirrors Java `Message#getRedeliveryCount`. The broker-side count of how many times
    /// this message has been redelivered.
    #[must_use]
    pub fn redelivery_count(&self) -> u32 {
        self.redelivery_count
    }

    /// Mirrors Java `Message#getReplicatedFrom`. `None` if the message wasn't replicated.
    #[must_use]
    pub fn replicated_from(&self) -> Option<&str> {
        self.metadata.replicated_from.as_deref()
    }

    /// Mirrors Java `Message#isReplicated`. `true` if this message was geo-replicated from
    /// another cluster — equivalent to `replicated_from().is_some()`.
    #[must_use]
    pub fn is_replicated(&self) -> bool {
        self.metadata.replicated_from.is_some()
    }

    /// `true` if the message arrived as part of a batched entry. The position within the
    /// batch is on `id.batch_index`. Useful for partial-batch ack logic and telemetry.
    #[must_use]
    pub fn is_batched(&self) -> bool {
        self.id.batch_index >= 0
    }

    /// `true` if the message arrived on a partitioned topic. The partition index is on
    /// `id.partition`.
    #[must_use]
    pub fn is_partitioned(&self) -> bool {
        self.id.partition >= 0
    }

    /// Payload size in bytes (post-decompression / post-decryption). Mirrors Java
    /// `Message#size`. Equivalent to `self.payload.len()`.
    #[must_use]
    pub fn size(&self) -> usize {
        self.payload.len()
    }

    /// `true` if the payload is empty — the Pulsar convention for a tombstone in a
    /// compacted topic. Mirrors Java `Message#isEmpty`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }

    /// Mirrors Java `Message#hasReplicateTo`. `true` when the producer stamped an explicit
    /// replication cluster list (via `OutgoingMessage::replication_clusters` /
    /// `disable_replication`).
    #[must_use]
    pub fn has_replicate_to(&self) -> bool {
        !self.metadata.replicate_to.is_empty()
    }

    /// Mirrors Java `Message#getReplicateTo`. Returns the cluster ids the message was
    /// pinned to, or an empty slice when the producer used the namespace default.
    #[must_use]
    pub fn replicate_to(&self) -> &[String] {
        &self.metadata.replicate_to
    }

    /// Mirrors Java `Message#hasEventTime`. `true` if the producer stamped a non-zero
    /// event-time (Java distinguishes "unset" from "stamped 0" via this predicate).
    #[must_use]
    pub fn has_event_time(&self) -> bool {
        self.metadata.event_time.is_some_and(|t| t != 0)
    }

    /// Mirrors Java `Message#hasOrderingKey`.
    #[must_use]
    pub fn has_ordering_key(&self) -> bool {
        self.metadata.ordering_key.is_some()
    }

    /// Mirrors Java `Message#hasProperty(String)`.
    #[must_use]
    pub fn has_property(&self, key: &str) -> bool {
        self.metadata.properties.iter().any(|kv| kv.key == key)
    }

    /// Mirrors Java `Message#hasProperties` — `true` if the message carries at least one
    /// (key, value) property entry.
    #[must_use]
    pub fn has_properties(&self) -> bool {
        !self.metadata.properties.is_empty()
    }

    /// Mirrors Java `Message#getSchemaVersion`. `None` for messages produced by schemaless
    /// producers (or via auto-produce-bytes).
    #[must_use]
    pub fn schema_version(&self) -> Option<&[u8]> {
        self.metadata.schema_version.as_deref()
    }

    /// PIP-90 broker timestamp — wall-clock millis since epoch the broker assigned when it
    /// persisted the entry. Returns `None` when the namespace policy disables broker-entry
    /// metadata or the broker is older than PIP-90.
    #[must_use]
    pub fn broker_publish_time_ms(&self) -> Option<u64> {
        self.broker_entry_metadata
            .as_ref()
            .and_then(|m| m.broker_timestamp)
    }

    /// PIP-90 per-topic broker index — monotonic offset the broker assigned when it
    /// persisted the entry. `None` under the same conditions as
    /// [`Self::broker_publish_time_ms`].
    #[must_use]
    pub fn broker_index(&self) -> Option<u64> {
        self.broker_entry_metadata.as_ref().and_then(|m| m.index)
    }

    /// `true` if the message metadata carries PIP-4 encryption context (one or more
    /// wrapped symmetric keys + the encryption algorithm name). Useful for callers
    /// running with `CryptoFailureAction::Consume` who want to know whether they need
    /// to attempt out-of-band decryption.
    #[must_use]
    pub fn has_encryption(&self) -> bool {
        !self.metadata.encryption_keys.is_empty()
    }

    /// PIP-4 encryption algorithm name (e.g. `"AES/GCM/NoPadding"`). `None` if the
    /// producer did not encrypt this message.
    #[must_use]
    pub fn encryption_algorithm(&self) -> Option<&str> {
        self.metadata.encryption_algo.as_deref()
    }

    /// PIP-4 wrapped symmetric-key entries. Empty slice when the producer did not
    /// encrypt this message. Each entry carries the key name + the ciphertext-wrapped
    /// data key the broker echoed back from the producer's `CryptoKeyReader`.
    #[must_use]
    pub fn encryption_keys(&self) -> &[magnetar_proto::pb::EncryptionKeys] {
        &self.metadata.encryption_keys
    }

    /// PIP-4 encryption parameter bytes (typically the AES GCM IV/nonce). `None` if the
    /// producer did not encrypt this message.
    #[must_use]
    pub fn encryption_param(&self) -> Option<&[u8]> {
        self.metadata.encryption_param.as_deref()
    }
}

impl From<magnetar_proto::event::IncomingMessage> for IncomingMessage {
    fn from(msg: magnetar_proto::event::IncomingMessage) -> Self {
        Self {
            id: msg.message_id,
            metadata: msg.metadata,
            payload: msg.payload,
            redelivery_count: msg.redelivery_count,
            broker_entry_metadata: msg.broker_entry_metadata,
        }
    }
}

/// High-level Pulsar client, generic over the runtime [`Engine`](crate::Engine).
///
/// Defaults to [`crate::TokioEngine`] (the production engine) so existing
/// callers write `PulsarClient::builder()` without naming a type parameter.
/// Callers exercising the moonpool deterministic-simulation engine
/// parametrise with `PulsarClient::<MoonpoolEngine<P>>` (see
/// [ADR-0019](../../specs/adr/0019-engine-scope-and-moonpool-parity.md)
/// gate (e), "Option A").
///
/// Every façade surface (`producer`, `consumer`, `reader`, `typed_producer`,
/// `typed_consumer`, partitioned / multi-topics / pattern / table-view
/// constructors, transactions, interceptor SPI, …) is implemented only on
/// `PulsarClient<TokioEngine>` for v0.1.0. Moonpool-side callers that reach
/// for one of these get a clean trait-bound failure — matching ADR-0019
/// §Decision "no silent fallbacks".
#[derive(Debug)]
pub struct PulsarClient<E: crate::Engine = crate::TokioEngine> {
    pub(crate) inner: E::ClientState,
    pub(crate) memory_limit: Option<MemoryLimit>,
}

impl PulsarClient<crate::TokioEngine> {
    /// Borrow the underlying runtime client. Re-exported for sibling modules
    /// ([`crate::PartitionedProducer`]) that need to call lower-level methods like
    /// `partitioned_topic_metadata` without going through a builder.
    pub(crate) fn runtime_client(&self) -> &Client {
        &self.inner
    }

    /// Start building a client. Returns a tokio-engine [`ClientBuilder`] —
    /// the default `E = TokioEngine` on [`PulsarClient<E>`]. Users targeting
    /// the moonpool engine open the engine directly via
    /// [`magnetar_runtime_moonpool::MoonpoolEngine`] (see
    /// [`PulsarClient::<MoonpoolEngine<P>>::from_moonpool`](crate::PulsarClient)
    /// for the equivalent constructor).
    #[must_use]
    pub fn builder() -> crate::client_builder::ClientBuilder {
        crate::client_builder::ClientBuilder::default()
    }

    /// The global publish memory budget configured at build time, if any.
    /// Mirrors Java `PulsarClient#getMemoryLimit`. `None` means no limit was
    /// configured (the Java default).
    ///
    /// **Note**: today this is configuration-only — the runtime does not yet
    /// enforce the limit. See [`ClientBuilder::memory_limit`] for the planned
    /// follow-up.
    #[must_use]
    pub fn memory_limit(&self) -> Option<MemoryLimit> {
        self.memory_limit
    }

    // producer / consumer / reader are engine-generic — see the dedicated
    // `impl<E: Engine> PulsarClient<E>` block below.

    /// Open a [`crate::TableViewBuilder`] for the given topic. A [`crate::TableView`] is a
    /// key/value snapshot built from a compacted topic — useful for config snapshots and
    /// similar "latest value wins per key" patterns. Mirrors
    /// `PulsarClient#newTableViewBuilder`.
    #[must_use]
    pub fn table_view(&self, topic: impl Into<String>) -> crate::TableViewBuilder<'_> {
        crate::TableViewBuilder::new(self, topic.into())
    }

    /// Schema-aware [`crate::TypedTableView`] builder. Mirrors Java
    /// `pulsar.tableViewBuilder(Schema)` — the view decodes payloads on read so getters
    /// return `S::Owned` directly.
    #[must_use]
    pub fn typed_table_view<S: magnetar_proto::schema::Schema>(
        &self,
        topic: impl Into<String>,
        schema: std::sync::Arc<S>,
    ) -> crate::TypedTableViewBuilder<'_, S> {
        crate::TypedTableViewBuilder::new(self, topic.into(), schema)
    }

    /// Open a [`crate::PartitionedProducerBuilder`] for the given topic. The builder queries
    /// the broker for the partition count and opens one child producer per partition.
    /// Mirrors Java's `PulsarClient#newProducer()` against a partitioned topic.
    #[must_use]
    pub fn partitioned_producer(
        &self,
        topic: impl Into<String>,
    ) -> crate::PartitionedProducerBuilder<'_> {
        crate::PartitionedProducerBuilder::new(self, topic.into())
    }

    /// PIP-180 (ADR-0033): subscribe with automatic shadow-source resolution.
    ///
    /// Performs the `magnetar-admin` `get_shadow_source(topic)` REST lookup,
    /// subscribes to `topic` with `subscription_name` (exclusive, durable),
    /// and — when the broker reports `topic` is a shadow — primes the
    /// consumer's shadow metadata via
    /// [`magnetar_runtime_tokio::Consumer::set_shadow_source`] so the receive
    /// path emits
    /// [`magnetar_proto::ConnectionEvent::MessageReceivedFromShadow`]
    /// without an out-of-band lookup per message.
    ///
    /// For regular (non-shadow) topics the call collapses to a plain
    /// `.consumer(topic).subscription(subscription_name).subscribe()`.
    ///
    /// # Errors
    ///
    /// - [`PulsarError::Other`] wrapping the admin REST error if the `get_shadow_source` lookup
    ///   fails.
    /// - Any error from the underlying `.subscribe()` round-trip.
    #[cfg(feature = "admin")]
    pub async fn subscribe_shadow_aware(
        &self,
        admin: &magnetar_admin::AdminClient,
        topic: impl Into<String>,
        subscription_name: impl Into<String>,
    ) -> Result<magnetar_runtime_tokio::Consumer, PulsarError> {
        let topic = topic.into();
        let subscription_name = subscription_name.into();
        let source = admin
            .get_shadow_source(&topic)
            .await
            .map_err(|e| PulsarError::Other(format!("get_shadow_source({topic}): {e}")))?;
        let consumer = self
            .consumer(topic)
            .subscription(subscription_name)
            .subscribe()
            .await?;
        if let Some(source_topic) = source {
            consumer.set_shadow_source(source_topic);
        }
        Ok(consumer)
    }

    /// PIP-33 (ADR-0034): non-blocking peek for the next replicated-subscription
    /// marker observation buffered by the driver. `None` when the buffer is empty.
    /// Mirrors [`magnetar_runtime_tokio::Client::poll_replicated_subscription_marker`].
    #[must_use]
    pub fn poll_replicated_subscription_marker(
        &self,
    ) -> Option<magnetar_runtime_tokio::ObservedReplicatedSubscriptionMarker> {
        self.inner.poll_replicated_subscription_marker()
    }

    /// PIP-33 (ADR-0034): await the next replicated-subscription marker
    /// observation. Resolves to `None` when the connection has closed and no
    /// further markers will arrive. Mirrors
    /// [`magnetar_runtime_tokio::Client::next_replicated_subscription_marker`].
    pub async fn next_replicated_subscription_marker(
        &self,
    ) -> Option<magnetar_runtime_tokio::ObservedReplicatedSubscriptionMarker> {
        self.inner.next_replicated_subscription_marker().await
    }

    /// Close the underlying connection.
    pub async fn close(self) {
        self.inner.close().await;
    }

    /// Alias for [`Self::close`]. Mirrors Java `PulsarClient#shutdown`, which is just the
    /// blocking form of `close` — same semantics from Rust because every async future is
    /// already non-blocking from the caller's perspective.
    pub async fn shutdown(self) {
        self.close().await;
    }

    /// Returns `true` while the underlying broker connection is up. Mirrors Java's
    /// `org.apache.pulsar.client.api.Producer#isConnected` and
    /// `Consumer#isConnected` at the client scope.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.inner.is_connected()
    }

    /// `true` once [`Self::close`] has been called or the broker connection has entered a
    /// terminal state. Mirrors Java `PulsarClient#isClosed`.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Wall-clock time the underlying broker connection was most recently torn down (peer
    /// EOF, I/O error, or an explicit `close()`). `None` while it has never been torn down.
    ///
    /// Mirrors `org.apache.pulsar.client.api.Producer#getLastDisconnectedTimestamp` /
    /// `Consumer#getLastDisconnectedTimestamp`. Convert with
    /// [`std::time::SystemTime::duration_since`] for Java-style millis-since-epoch.
    #[must_use]
    pub fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        self.inner.last_disconnected_timestamp()
    }
}

impl<E: crate::Engine> PulsarClient<E> {
    /// Open a [`ProducerBuilder`] for the given topic. Engine-generic — the
    /// underlying transport is selected at construction time.
    #[must_use]
    pub fn producer(&self, topic: impl Into<String>) -> crate::builders::ProducerBuilder<'_, E> {
        crate::builders::ProducerBuilder::new(self, topic.into())
    }

    /// Open a [`ConsumerBuilder`] for the given topic. Engine-generic — the
    /// underlying transport is selected at construction time.
    #[must_use]
    pub fn consumer(&self, topic: impl Into<String>) -> crate::builders::ConsumerBuilder<'_, E> {
        crate::builders::ConsumerBuilder::new(self, topic.into())
    }

    /// Open a [`ReaderBuilder`] for the given topic. A reader is a non-durable, exclusive
    /// consumer with an auto-generated subscription — useful for log inspection and replay.
    /// Engine-generic — the underlying transport is selected at construction time.
    #[must_use]
    pub fn reader(&self, topic: impl Into<String>) -> crate::builders::ReaderBuilder<'_, E> {
        crate::builders::ReaderBuilder::new(self, topic.into())
    }

    /// Open a schema-aware [`crate::TypedProducerBuilder`] for the given topic. Mirrors Java's
    /// `PulsarClient#newProducer(Schema<T>)`. Engine-generic per ADR-0026 §D1.
    #[must_use]
    pub fn typed_producer<S: magnetar_proto::schema::Schema>(
        &self,
        topic: impl Into<String>,
        schema: std::sync::Arc<S>,
    ) -> crate::TypedProducerBuilder<'_, S, E> {
        crate::TypedProducerBuilder::new(self, topic.into(), schema)
    }

    /// Open a schema-aware [`crate::TypedConsumerBuilder`] for the given topic. Mirrors Java's
    /// `PulsarClient#newConsumer(Schema<T>)`. Engine-generic per ADR-0026 §D1.
    #[must_use]
    pub fn typed_consumer<S: magnetar_proto::schema::Schema>(
        &self,
        topic: impl Into<String>,
        schema: std::sync::Arc<S>,
    ) -> crate::TypedConsumerBuilder<'_, S, E> {
        crate::TypedConsumerBuilder::new(self, topic.into(), schema)
    }

    /// Open a [`crate::MultiTopicsConsumerBuilder`] that subscribes to many topics at once.
    /// Mirrors Java's `PulsarClient#newConsumer().topics(...)`. Engine-generic per
    /// ADR-0026 §D1 — `.subscribe()` routes through the engine-generic
    /// [`crate::ConsumerBuilder`].
    #[must_use]
    pub fn multi_topics_consumer(&self) -> crate::MultiTopicsConsumerBuilder<'_, E> {
        crate::MultiTopicsConsumerBuilder::new(self)
    }

    /// Open a [`crate::PatternConsumerBuilder`] that subscribes to every topic in a namespace
    /// matching a broker-side regex pattern (PIP-145). Reconciles against `TopicListChanged`
    /// deltas on demand via [`crate::PatternConsumer::update`]. Mirrors Java's
    /// `PulsarClient#newConsumer().topicsPattern(...)`. Engine-generic per ADR-0026 §D1.
    #[must_use]
    pub fn pattern_consumer(&self) -> crate::PatternConsumerBuilder<'_, E> {
        crate::PatternConsumerBuilder::new(self)
    }

    /// Open a [`crate::PartitionedConsumerBuilder`] for the given topic. The builder
    /// auto-discovers the partition count and subscribes to every partition under a single
    /// subscription name. Mirrors Java's `PulsarClient#newConsumer()` against a partitioned
    /// topic. Engine-generic per ADR-0026 §D1.
    #[must_use]
    pub fn partitioned_consumer(
        &self,
        topic: impl Into<String>,
    ) -> crate::PartitionedConsumerBuilder<'_, E> {
        crate::PartitionedConsumerBuilder::new(self, topic.into())
    }
}

/// Broker-metadata methods that dispatch through the
/// [`crate::BrokerMetadataApi`] extension trait. Engine-generic per
/// ADR-0026 §D1 — both runtimes implement `BrokerMetadataApi` on their
/// `Client` type.
impl<E: crate::Engine> PulsarClient<E>
where
    E::ClientState: crate::BrokerMetadataApi,
{
    /// Query the broker for the partition count of `topic`. Returns `0` for non-partitioned
    /// topics. Mirrors Java `PulsarClient#getPartitionsForTopic`.
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Other`] if the broker refuses the metadata lookup.
    pub async fn partitions_for_topic(&self, topic: &str) -> Result<u32> {
        crate::BrokerMetadataApi::partitioned_topic_metadata(&self.inner, topic)
            .await
            .map_err(|err| PulsarError::Other(format!("partitions_for_topic: {err}")))
    }

    /// Subscribe to a topic-list watcher and return the initial topic snapshot for the
    /// given namespace + regex pattern (PIP-145). Useful for "discover all topics matching
    /// this pattern right now" workflows. Live updates are emitted by the connection as
    /// `TopicListChanged` events and surfaced through
    /// [`crate::BrokerMetadataApi::poll_topic_list_change`].
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Other`] if the broker refuses the watch.
    pub async fn topic_list_snapshot(&self, namespace: &str, pattern: &str) -> Result<Vec<String>> {
        crate::BrokerMetadataApi::watch_topic_list(&self.inner, namespace, pattern)
            .await
            .map_err(|err| PulsarError::Other(format!("topic_list_snapshot: {err}")))
    }
}

/// Java parity: `org.apache.pulsar.client.api.MemoryLimitPolicy`.
///
/// Selects how the client behaves when the configured global publish memory budget is
/// exhausted (see [`ClientBuilder::memory_limit`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryLimitPolicy {
    /// Fail new sends immediately with an `out of memory` error. Mirrors Java
    /// `MemoryLimitPolicy.FAIL_IMMEDIATELY` (the Java default).
    FailImmediately,
    /// Block the producer's `send`/`sendAsync` until enough room frees up. Mirrors
    /// Java `MemoryLimitPolicy.PRODUCER_BLOCK`.
    ProducerBlock,
}

/// Java parity: configured global publish memory budget. Stored verbatim on
/// [`ClientBuilder`] and exposed to consumers via [`PulsarClient::memory_limit`].
///
/// **Note**: today this is configuration storage only — the actual enforcement
/// (accounting against in-flight publish bytes per the policy) is a follow-up
/// to land before the `0.1` release. The surface is shipped now so callers can
/// migrate from Java without changing their builder chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryLimit {
    /// Upper bound in bytes. `0` disables the limit (matches Java default).
    pub bytes: usize,
    /// Policy applied when the budget is exhausted.
    pub policy: MemoryLimitPolicy,
}

/// Reader handle — a non-durable consumer that reads from a topic without persisting an
/// acknowledgement cursor. Use a reader for: log replay, message inspection, batch ETL, or
/// anywhere you want at-most-once delivery semantics that the broker doesn't track.
///
/// Generic over `C: ConsumerApi` per ADR-0026 §D1. The default
/// (`C = magnetar_runtime_tokio::Consumer`) keeps existing callers — including
/// `magnetar::Reader` (no type argument) — pointing at the tokio specialisation.
/// Moonpool callers name `Reader<magnetar_runtime_moonpool::Consumer<P>>` directly.
#[derive(Debug)]
pub struct Reader<C: crate::ConsumerApi = magnetar_runtime_tokio::Consumer> {
    pub(crate) consumer: C,
    /// Last message id returned via [`Self::read_next`]. Used by
    /// [`Self::has_message_available`] to ask the broker "is there anything past
    /// what I last handed you?" without the caller having to track the cursor.
    pub(crate) last_received: parking_lot::Mutex<Option<magnetar_proto::MessageId>>,
}

impl<C: crate::ConsumerApi> Reader<C> {
    /// Block until the next message arrives. Identical to Java `Reader#readNext`.
    /// Internally also stamps the returned id into the per-reader cursor so a subsequent
    /// [`Self::has_message_available`] call asks the broker the right question.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] (with the runtime's error stringified) on broker rejection or wire
    ///   failure.
    pub async fn read_next(&self) -> Result<IncomingMessage, PulsarError> {
        let msg = crate::ConsumerApi::receive(&self.consumer)
            .await
            .map_err(|err| PulsarError::Other(format!("read_next: {err}")))?;
        *self.last_received.lock() = Some(msg.message_id);
        Ok(IncomingMessage::from(msg))
    }

    /// Manually record a received message id into the per-reader cursor. Useful when
    /// callers go through engine-specific receive paths directly and still want
    /// [`Self::has_message_available`] to behave correctly.
    pub fn record_received(&self, message_id: magnetar_proto::MessageId) {
        *self.last_received.lock() = Some(message_id);
    }

    /// `true` if the broker has at least one message strictly past the most-recently
    /// returned message id. Mirrors Java `Reader#hasMessageAvailable` (no argument —
    /// the reader tracks its own cursor). Returns `true` for fresh readers (no
    /// `read_next` yet) if the broker reports any non-empty topic.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] on broker rejection or wire failure.
    pub async fn has_message_available(&self) -> Result<bool, PulsarError> {
        let cursor = *self.last_received.lock();
        if let Some(c) = cursor {
            return crate::ConsumerApi::has_message_after(&self.consumer, c)
                .await
                .map_err(|err| PulsarError::Other(format!("has_message_available: {err}")));
        }
        let last = crate::ConsumerApi::last_message_id(&self.consumer)
            .await
            .map_err(|err| PulsarError::Other(format!("has_message_available: {err}")))?;
        Ok(last != magnetar_proto::MessageId::EARLIEST)
    }

    /// Borrow the underlying consumer for advanced operations not covered by
    /// [`crate::ConsumerApi`] (close, seek, flow, etc.).
    #[must_use]
    pub fn consumer(&self) -> &C {
        &self.consumer
    }

    /// Topic this reader is bound to. Mirrors Java `Reader#getTopic`.
    #[must_use]
    pub fn topic(&self) -> String {
        crate::ConsumerApi::topic(&self.consumer)
    }

    /// Auto-generated subscription name behind this reader. Mirrors Java
    /// `Reader#getSubscriptionName`.
    #[must_use]
    pub fn subscription(&self) -> String {
        crate::ConsumerApi::subscription(&self.consumer)
    }

    /// Ask the broker for the topic's last-published message id. Mirrors Java
    /// `Reader#getLastMessageId`.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] on broker rejection or wire failure.
    pub async fn last_message_id(&self) -> Result<magnetar_proto::MessageId, PulsarError> {
        crate::ConsumerApi::last_message_id(&self.consumer)
            .await
            .map_err(|err| PulsarError::Other(format!("last_message_id: {err}")))
    }

    /// `true` if the broker has at least one message strictly past the supplied cursor.
    /// Mirrors Java `Reader#hasMessageAvailable` (the Reader form takes no cursor; pass
    /// the last id you received).
    ///
    /// # Errors
    /// - [`PulsarError::Other`] on broker rejection or wire failure.
    pub async fn has_message_after(
        &self,
        cursor: magnetar_proto::MessageId,
    ) -> Result<bool, PulsarError> {
        crate::ConsumerApi::has_message_after(&self.consumer, cursor)
            .await
            .map_err(|err| PulsarError::Other(format!("has_message_after: {err}")))
    }
}

/// Tokio-engine-specific Reader methods that touch types not on the
/// engine-agnostic [`crate::ConsumerApi`] surface — the tokio `ReceiveFut`,
/// `tokio::time::timeout`, `Consumer::close(self)`, and `seek_to_earliest`.
impl Reader<magnetar_runtime_tokio::Consumer> {
    /// Same as [`Self::read_next`] but bounded by `timeout`. Returns `Ok(None)` when the
    /// deadline elapses with no message. Mirrors Java
    /// `Reader#readNext(int timeout, TimeUnit unit)`.
    pub async fn read_next_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Option<magnetar_proto::IncomingMessage>, PulsarError> {
        match tokio::time::timeout(timeout, self.consumer.receive()).await {
            Ok(Ok(msg)) => {
                *self.last_received.lock() = Some(msg.message_id);
                Ok(Some(msg))
            }
            Ok(Err(err)) => Err(PulsarError::Client(err)),
            Err(_) => Ok(None),
        }
    }

    /// Returns the raw [`magnetar_runtime_tokio::ReceiveFut`] without per-reader cursor
    /// tracking. Use this when integrating with a custom select loop where you want
    /// cancel-safe receive futures; pair with [`Self::record_received`] if you still want
    /// `has_message_available` to work.
    pub fn read_next_fut(&self) -> magnetar_runtime_tokio::ReceiveFut {
        self.consumer.receive()
    }

    /// Close the reader.
    pub async fn close(self) -> Result<(), PulsarError> {
        self.consumer.close().await.map_err(PulsarError::Client)
    }

    /// Seek the reader to the earliest available message. Mirrors Java
    /// `Reader#seek(MessageId.earliest)`.
    pub async fn seek_to_earliest(&self) -> Result<(), PulsarError> {
        self.consumer
            .seek_to_earliest()
            .await
            .map_err(PulsarError::Client)
    }

    /// Seek the reader to the latest (head) position. Mirrors Java
    /// `Reader#seek(MessageId.latest)`.
    pub async fn seek_to_latest(&self) -> Result<(), PulsarError> {
        self.consumer
            .seek_to_latest()
            .await
            .map_err(PulsarError::Client)
    }

    /// Seek the reader to a specific message id. Mirrors Java
    /// `Reader#seek(MessageId)`.
    pub async fn seek_to_message(
        &self,
        message_id: magnetar_proto::MessageId,
    ) -> Result<(), PulsarError> {
        self.consumer
            .seek_to_message(message_id)
            .await
            .map_err(PulsarError::Client)
    }

    /// Seek the reader to a publish-time deadline (millis since UNIX epoch). Mirrors Java
    /// `Reader#seek(long)`.
    pub async fn seek_to_timestamp(&self, publish_time_ms: u64) -> Result<(), PulsarError> {
        self.consumer
            .seek_to_timestamp(publish_time_ms)
            .await
            .map_err(PulsarError::Client)
    }

    /// Mirrors `org.apache.pulsar.client.api.Reader#isConnected`.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.consumer.is_connected()
    }

    /// Mirrors `org.apache.pulsar.client.api.Reader#getLastDisconnectedTimestamp`.
    #[must_use]
    pub fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        self.consumer.last_disconnected_timestamp()
    }

    /// Mirrors `org.apache.pulsar.client.api.Reader#getStats`.
    #[must_use]
    pub fn stats(&self) -> magnetar_proto::ConsumerStats {
        self.consumer.stats()
    }

    /// `true` once the broker has signalled (via `CommandReachedEndOfTopic`) that no more
    /// messages will be dispatched on this topic. Mirrors Java
    /// `Reader#hasReachedEndOfTopic`.
    #[must_use]
    pub fn has_reached_end_of_topic(&self) -> bool {
        self.consumer.has_reached_end_of_topic()
    }

    /// Pause delivery for this reader. The broker stops dispatching new messages once
    /// already-issued permits drain; buffered messages remain available via
    /// [`Self::read_next`]. Mirrors `Reader#pause`.
    pub fn pause(&self) {
        self.consumer.pause();
    }

    /// Resume delivery after [`Self::pause`]. Mirrors `Reader#resume`.
    pub fn resume(&self) {
        self.consumer.resume();
    }

    /// `true` when the reader has been disconnected longer than the configured
    /// "inactive" threshold. Mirrors Java `Reader#isInactive` (returns the underlying
    /// consumer's inactivity state since readers wrap an `Exclusive` subscription).
    #[must_use]
    pub fn is_inactive(&self) -> bool {
        self.consumer.is_inactive()
    }

    /// `true` once the reader's underlying subscription has been closed locally or by
    /// the broker. Mirrors Java `Reader#isClosed`.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.consumer.is_closed()
    }

    /// Number of messages currently buffered in the reader's receiver queue, waiting for
    /// a `read_next` call to pull them out. Mirrors Java
    /// `Reader#getNumOfPendingMessages` semantics.
    #[must_use]
    pub fn available_in_queue(&self) -> usize {
        self.consumer.available_in_queue()
    }

    /// Number of dispatch permits this reader still has with the broker.
    #[must_use]
    pub fn available_permits(&self) -> u32 {
        self.consumer.available_permits()
    }

    /// `true` if the reader has received at least one message since opening. Mirrors
    /// Java `Reader#hasReceivedAnyMessage`.
    #[must_use]
    pub fn has_received_any_message(&self) -> bool {
        self.consumer.has_received_any_message()
    }
}

#[cfg(test)]
mod outgoing_message_tests {
    use super::*;

    #[test]
    fn value_sets_payload() {
        let msg = OutgoingMessage::default()
            .key("k")
            .event_time_ms(42)
            .property("p", "v")
            .value("hello");
        assert_eq!(msg.payload.as_ref(), b"hello");
        assert_eq!(msg.key.as_deref(), Some("k"));
        assert_eq!(msg.event_time_ms, Some(42));
        assert_eq!(msg.properties.len(), 1);
    }

    #[test]
    fn into_carries_payload_and_metadata() {
        let msg = OutgoingMessage::default()
            .key("k")
            .event_time_ms(7)
            .property("p", "v")
            .value(b"abc".to_vec());
        let converted: magnetar_proto::producer::OutgoingMessage = msg.into();
        assert_eq!(converted.payload.as_ref(), b"abc");
        assert_eq!(converted.metadata.partition_key.as_deref(), Some("k"));
        assert_eq!(converted.metadata.event_time, Some(7));
        assert_eq!(converted.metadata.properties.len(), 1);
        assert_eq!(converted.uncompressed_size, 3);
    }

    fn message_with(metadata: pb::MessageMetadata) -> IncomingMessage {
        IncomingMessage {
            id: magnetar_proto::types::MessageId::EARLIEST,
            metadata: std::sync::Arc::new(metadata),
            payload: Bytes::new(),
            redelivery_count: 0,
            broker_entry_metadata: None,
        }
    }

    #[test]
    fn incoming_has_event_time_distinguishes_zero_and_unset() {
        let unset = message_with(pb::MessageMetadata::default());
        assert!(!unset.has_event_time());

        let zero = message_with(pb::MessageMetadata {
            event_time: Some(0),
            ..pb::MessageMetadata::default()
        });
        assert!(!zero.has_event_time());

        let stamped = message_with(pb::MessageMetadata {
            event_time: Some(42),
            ..pb::MessageMetadata::default()
        });
        assert!(stamped.has_event_time());
        assert_eq!(stamped.event_time_ms(), 42);
    }

    #[test]
    fn incoming_property_helpers() {
        let msg = message_with(pb::MessageMetadata {
            properties: vec![pb::KeyValue {
                key: "k".to_owned(),
                value: "v".to_owned(),
            }],
            ..pb::MessageMetadata::default()
        });
        assert!(msg.has_properties());
        assert!(msg.has_property("k"));
        assert!(!msg.has_property("missing"));
        assert_eq!(msg.property("k"), Some("v"));
    }

    #[test]
    fn incoming_replicate_to_helpers() {
        let empty = message_with(pb::MessageMetadata::default());
        assert!(!empty.has_replicate_to());
        assert!(empty.replicate_to().is_empty());

        let stamped = message_with(pb::MessageMetadata {
            replicate_to: vec!["a".to_owned(), "b".to_owned()],
            ..pb::MessageMetadata::default()
        });
        assert!(stamped.has_replicate_to());
        assert_eq!(stamped.replicate_to(), &["a", "b"]);
    }

    #[test]
    fn broker_entry_metadata_getters() {
        let mut msg = message_with(pb::MessageMetadata::default());
        assert_eq!(msg.broker_publish_time_ms(), None);
        assert_eq!(msg.broker_index(), None);

        msg.broker_entry_metadata = Some(std::sync::Arc::new(pb::BrokerEntryMetadata {
            broker_timestamp: Some(1_700_000_000_000),
            index: Some(42),
        }));
        assert_eq!(msg.broker_publish_time_ms(), Some(1_700_000_000_000));
        assert_eq!(msg.broker_index(), Some(42));
    }

    #[test]
    fn is_replicated_tracks_metadata() {
        let unset = message_with(pb::MessageMetadata::default());
        assert!(!unset.is_replicated());

        let stamped = message_with(pb::MessageMetadata {
            replicated_from: Some("us-east".to_owned()),
            ..pb::MessageMetadata::default()
        });
        assert!(stamped.is_replicated());
        assert_eq!(stamped.replicated_from(), Some("us-east"));
    }

    #[test]
    fn is_batched_and_is_partitioned_track_id_fields() {
        let single = message_with(pb::MessageMetadata::default());
        assert!(!single.is_batched());
        assert!(!single.is_partitioned());

        let mut batched = message_with(pb::MessageMetadata::default());
        batched.id = magnetar_proto::types::MessageId {
            ledger_id: 1,
            entry_id: 2,
            partition: -1,
            batch_index: 3,
            batch_size: 10,
        };
        assert!(batched.is_batched());
        assert!(!batched.is_partitioned());

        let mut partitioned = message_with(pb::MessageMetadata::default());
        partitioned.id = magnetar_proto::types::MessageId {
            ledger_id: 1,
            entry_id: 2,
            partition: 4,
            batch_index: -1,
            batch_size: 0,
        };
        assert!(!partitioned.is_batched());
        assert!(partitioned.is_partitioned());
    }

    #[derive(Debug, Default)]
    struct AppendPropertyInterceptor {
        key: String,
        value: String,
        applied: std::sync::atomic::AtomicUsize,
    }

    impl ProducerInterceptor for AppendPropertyInterceptor {
        fn eligible(&self, msg: &OutgoingMessage) -> bool {
            !msg.payload.is_empty()
        }

        fn before_send(&self, msg: &mut OutgoingMessage) {
            msg.properties.push((self.key.clone(), self.value.clone()));
            self.applied
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    fn interceptor_eligibility_skips_unmatched_messages() {
        let i = AppendPropertyInterceptor {
            key: "trace-id".to_owned(),
            value: "abc".to_owned(),
            applied: std::sync::atomic::AtomicUsize::new(0),
        };
        let empty = OutgoingMessage::default();
        assert!(!i.eligible(&empty));

        let with_payload = OutgoingMessage::with_payload("hi");
        assert!(i.eligible(&with_payload));
    }

    #[test]
    fn interceptor_before_send_mutates_message() {
        let i = AppendPropertyInterceptor {
            key: "trace-id".to_owned(),
            value: "abc".to_owned(),
            applied: std::sync::atomic::AtomicUsize::new(0),
        };
        let mut msg = OutgoingMessage::with_payload("hi");
        assert!(msg.properties.is_empty());
        i.before_send(&mut msg);
        assert_eq!(msg.properties.len(), 1);
        assert_eq!(msg.properties[0].0, "trace-id");
        assert_eq!(msg.properties[0].1, "abc");
        assert_eq!(i.applied.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[derive(Debug, Default)]
    struct StampSeenInterceptor {
        seen: std::sync::atomic::AtomicUsize,
    }

    impl ConsumerInterceptor for StampSeenInterceptor {
        fn before_consume(&self, _msg: &mut IncomingMessage) {
            self.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    fn consumer_interceptor_before_consume_runs_on_messages() {
        let i = StampSeenInterceptor::default();
        let mut msg = message_with(pb::MessageMetadata::default());
        i.before_consume(&mut msg);
        assert_eq!(i.seen.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
