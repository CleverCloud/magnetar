// SPDX-License-Identifier: Apache-2.0

//! Strongly-typed producer and consumer wrappers.
//!
//! Mirrors Java's `Producer<T>` / `Consumer<T>` shape, where `T` is the value type produced or
//! consumed and a [`magnetar_proto::schema::Schema`] handles the serialisation.
//!
//! [`TypedProducer`] wraps a runtime [`Producer`](magnetar_runtime_tokio::Producer) and a
//! schema; calling `send(value)` encodes the value, stamps `MessageMetadata.partition_key` when
//! a key is supplied, and forwards to the inner producer. [`TypedConsumer`] does the inverse on
//! the receive path, returning [`TypedMessage<S>`] (payload + decoded value + message id).
//!
//! Both wrappers stamp the schema's wire bytes on the underlying open frames via the
//! `magnetar_proto` schema field on `CreateProducerRequest` / `SubscribeRequest`, so the broker
//! records the schema and surfaces it to the dashboard.
//!
//! # Engine-generic surfaces (ADR-0026 §D1)
//!
//! `TypedProducer<S, P>` and `TypedConsumer<S, C>` are generic over
//! their inner runtime type (`P: ProducerApi`, `C: ConsumerApi`). The
//! defaults (`P = magnetar_runtime_tokio::Producer`,
//! `C = magnetar_runtime_tokio::Consumer`) keep existing callers
//! pointing at the tokio specialisation without naming the producer /
//! consumer type. Moonpool callers name
//! `TypedProducer<S, magnetar_runtime_moonpool::Producer<P>>` and
//! `TypedConsumer<S, magnetar_runtime_moonpool::Consumer<P>>`.
//!
//! Methods that depend on the runtime's `magnetar_proto::IncomingMessage`
//! shape (the `receive` family, `receive_batch`, `reconsume_later`,
//! `republish_dead_letters`) stay on the tokio specialisation
//! because the engine-generic [`crate::ConsumerApi`] trait returns
//! [`crate::IncomingMessage`] which loses the protocol-level
//! `single_metadata` and `arrived_at` fields. Same split pattern as
//! `PartitionedProducer<P>` (commit `aaa0661`).
//!
//! [`TypedProducerBuilder<'a, S, E>`] and [`TypedConsumerBuilder<'a,
//! S, E>`] are generic over the engine and route their `.create()` /
//! `.subscribe()` through [`crate::CreateProducerApi`] /
//! [`crate::SubscribeApi`] on the inner runtime client.

use std::sync::Arc;

use bytes::Bytes;
use magnetar_proto::schema::{Schema, SchemaError};
use magnetar_proto::{IncomingMessage, MessageId, pb};
use magnetar_runtime_tokio::{Consumer, Producer};

use crate::PulsarClient;
use crate::client::PulsarError;

/// A schema-aware producer. Wraps a producer and applies the configured schema to every
/// outbound value.
///
/// Generic over `P: ProducerApi` per ADR-0026 §D1. The default
/// (`P = magnetar_runtime_tokio::Producer`) keeps existing callers —
/// `magnetar::TypedProducer<S>` without a producer type argument —
/// pointing at the tokio specialisation. Moonpool callers name
/// `TypedProducer<S, magnetar_runtime_moonpool::Producer<P>>`.
///
/// Every inherent method dispatches through the [`crate::ProducerApi`]
/// trait, so the surface compiles against both engines.
pub struct TypedProducer<S: Schema, P: crate::ProducerApi = Producer> {
    inner: P,
    schema: Arc<S>,
}

impl<S: Schema, P: crate::ProducerApi + std::fmt::Debug> std::fmt::Debug for TypedProducer<S, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedProducer")
            .field("inner", &self.inner)
            .field("schema_type", &self.schema.schema_type())
            .finish()
    }
}

impl<S: Schema, P: crate::ProducerApi> TypedProducer<S, P> {
    /// The inner runtime producer. Useful for accessing connection-state observers and stats.
    #[must_use]
    pub fn inner(&self) -> &P {
        &self.inner
    }

    /// Encode `value` with the schema and publish it. `key` (optional) becomes the message's
    /// `partition_key`, which the broker uses for compaction and `key_shared` routing.
    ///
    /// PIP-87 [`AutoProduceBytesSchema`](magnetar_proto::schema::AutoProduceBytesSchema)
    /// producers transparently warm their schema cache on first send via
    /// [`Schema::needs_broker_schema`](magnetar_proto::schema::Schema::needs_broker_schema) +
    /// [`Schema::store_resolved_schema`](magnetar_proto::schema::Schema::store_resolved_schema).
    /// Encoding is pass-through whether or not the cache is populated (Java parity); the lookup
    /// is purely diagnostic / cache-warming and subsequent sends skip the round-trip.
    pub async fn send(
        &self,
        value: &S::Owned,
        key: Option<String>,
    ) -> Result<MessageId, PulsarError> {
        self.warm_broker_schema().await?;
        let bytes = self.schema.encode(value).map_err(schema_to_pulsar)?;
        // Build a façade [`crate::OutgoingMessage`] so the engine-generic
        // [`crate::ProducerApi::send`] (which consumes that shape) can
        // dispatch through the runtime's per-engine
        // `From<crate::OutgoingMessage> for magnetar_proto::OutgoingMessage`
        // bridge.
        let mut msg = crate::OutgoingMessage::with_payload(bytes);
        if let Some(k) = key {
            msg = msg.key(k);
        }
        let id = crate::ProducerApi::send(&self.inner, msg)
            .await
            .map_err(|err| PulsarError::Other(format!("send: {err}")))?;
        Ok(id)
    }

    /// Warm the schema cache by issuing a `CommandGetSchema` for the producer's topic if the
    /// schema reports `needs_broker_schema()`. Pure no-op for inline schemas (Avro / JSON /
    /// primitives). Used on every send path so PIP-87 `AutoProduceBytesSchema` producers cache
    /// the broker-resolved schema after the first successful round-trip.
    async fn warm_broker_schema(&self) -> Result<(), PulsarError> {
        if self.schema.needs_broker_schema() {
            let resolved = crate::ProducerApi::get_schema(&self.inner, None)
                .await
                .map_err(|err| PulsarError::Other(format!("get_schema: {err}")))?;
            self.schema.store_resolved_schema(resolved);
        }
        Ok(())
    }

    /// Close the underlying producer.
    pub async fn close(self) -> Result<(), PulsarError> {
        crate::ProducerApi::close_owned(self.inner)
            .await
            .map_err(|err| PulsarError::Other(format!("close: {err}")))
    }

    /// Topic this producer is bound to. Mirrors Java `Producer#getTopic`.
    #[must_use]
    pub fn topic(&self) -> String {
        crate::ProducerApi::topic(&self.inner)
    }

    /// Producer name (broker-assigned if not user-supplied). Mirrors Java
    /// `Producer#getProducerName`.
    #[must_use]
    pub fn name(&self) -> String {
        crate::ProducerApi::name(&self.inner)
    }

    /// Compression codec this producer was configured with. See `Producer::compression`.
    #[must_use]
    pub fn compression(&self) -> magnetar_proto::types::CompressionKind {
        crate::ProducerApi::compression(&self.inner)
    }

    /// `true` while the broker connection is up. Mirrors Java `Producer#isConnected`.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        crate::ProducerApi::is_connected(&self.inner)
    }

    /// `true` once [`Self::close`] has been called.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        crate::ProducerApi::is_closed(&self.inner)
    }

    /// Cumulative producer counters snapshot. Mirrors Java `Producer#getStats`.
    #[must_use]
    pub fn stats(&self) -> magnetar_proto::ProducerStats {
        crate::ProducerApi::stats(&self.inner)
    }

    /// Wall-clock instant of the most-recent connection drop. Mirrors Java
    /// `Producer#getLastDisconnectedTimestamp`.
    #[must_use]
    pub fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        crate::ProducerApi::last_disconnected_timestamp(&self.inner)
    }

    /// Last sequence id pushed onto the wire. Mirrors Java `Producer#getLastSequenceId`.
    #[must_use]
    pub fn last_sequence_id(&self) -> i64 {
        crate::ProducerApi::last_sequence_id(&self.inner)
    }

    /// Last sequence id the broker has acknowledged. Mirrors Java
    /// `Producer#getLastSequenceIdPublished`.
    #[must_use]
    pub fn last_sequence_id_published(&self) -> i64 {
        crate::ProducerApi::last_sequence_id_published(&self.inner)
    }

    /// Number of in-flight sends. See `Producer::pending_count`.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        crate::ProducerApi::pending_count(&self.inner)
    }

    /// Number of messages buffered in the batch container. See `Producer::batch_len`.
    #[must_use]
    pub fn batch_len(&self) -> usize {
        crate::ProducerApi::batch_len(&self.inner)
    }

    /// Payload bytes buffered in the batch container. See `Producer::batch_bytes`.
    #[must_use]
    pub fn batch_bytes(&self) -> usize {
        crate::ProducerApi::batch_bytes(&self.inner)
    }

    /// Flush pending batches and await every in-flight send. Mirrors Java
    /// `Producer#flushAsync`.
    pub async fn flush(&self) -> Result<(), PulsarError> {
        crate::ProducerApi::flush(&self.inner)
            .await
            .map_err(|err| PulsarError::Other(format!("flush: {err}")))
    }
}

impl<S: Schema> TypedProducer<S, Producer> {
    /// Start a Java-symmetric `TypedMessageBuilder`. Mirrors `producer.newMessage()` —
    /// chain `.key`, `.event_time_ms`, `.property`, etc., end with `.send(&value).await`.
    ///
    /// Tokio-only — [`TypedMessageBuilder`] composes the per-message
    /// [`crate::OutgoingMessage`] which today flows through the
    /// tokio-specialised `Producer::send` for the dispatch surface
    /// `.into()` already covers. Moonpool callers can build their own
    /// `OutgoingMessage` and call [`Self::send`] directly.
    pub fn new_message(&self) -> TypedMessageBuilder<'_, S> {
        TypedMessageBuilder {
            producer: self,
            msg: crate::OutgoingMessage::default(),
        }
    }
}

/// Schema-aware counterpart to [`crate::MessageBuilder`]. Captures a `&TypedProducer`
/// and lets callers chain Java-style: `producer.new_message().key(..).value(&typed).send()`.
/// The schema runs on `.send(&value)` so we don't pay the encode cost on values that get
/// dropped mid-build (a logic error caught by the borrow checker, but cheap to be
/// defensive about).
#[derive(Debug)]
pub struct TypedMessageBuilder<'a, S: Schema> {
    producer: &'a TypedProducer<S, Producer>,
    msg: crate::OutgoingMessage,
}

impl<S: Schema> TypedMessageBuilder<'_, S> {
    /// Set the routing key. See [`crate::OutgoingMessage::key`].
    #[must_use]
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.msg = self.msg.key(key);
        self
    }

    /// Set the ordering key. See [`crate::OutgoingMessage::ordering_key`].
    #[must_use]
    pub fn ordering_key(mut self, key: impl Into<Bytes>) -> Self {
        self.msg = self.msg.ordering_key(key);
        self
    }

    /// Set the event time (millis since epoch). See [`crate::OutgoingMessage::event_time_ms`].
    #[must_use]
    pub fn event_time_ms(mut self, ts: u64) -> Self {
        self.msg = self.msg.event_time_ms(ts);
        self
    }

    /// Append a property. See [`crate::OutgoingMessage::property`].
    #[must_use]
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.msg = self.msg.property(key, value);
        self
    }

    /// See [`crate::OutgoingMessage::deliver_at_ms`].
    #[must_use]
    pub fn deliver_at_ms(mut self, ts_ms: i64) -> Self {
        self.msg = self.msg.deliver_at_ms(ts_ms);
        self
    }

    /// See [`crate::OutgoingMessage::deliver_after_ms`]. The caller
    /// supplies `now_ms` (sans-io, ADR-0011 invariant #3).
    #[must_use]
    pub fn deliver_after_ms(mut self, now_ms: i64, delay_ms: i64) -> Self {
        self.msg = self.msg.deliver_after_ms(now_ms, delay_ms);
        self
    }

    /// See [`crate::OutgoingMessage::replication_clusters`].
    #[must_use]
    pub fn replication_clusters(mut self, clusters: Vec<String>) -> Self {
        self.msg = self.msg.replication_clusters(clusters);
        self
    }

    /// See [`crate::OutgoingMessage::disable_replication`].
    #[must_use]
    pub fn disable_replication(mut self) -> Self {
        self.msg = self.msg.disable_replication();
        self
    }

    /// See [`crate::OutgoingMessage::txn`].
    #[must_use]
    pub fn txn(mut self, txn_id: magnetar_proto::TxnId) -> Self {
        self.msg = self.msg.txn(txn_id);
        self
    }

    /// Encode `value` with the producer's schema and submit. Mirrors Java's
    /// terminal `TypedMessageBuilder#send`. PIP-87 `AutoProduceBytesSchema` producers warm
    /// their broker-schema cache on first invocation via the same path as
    /// [`TypedProducer::send`].
    pub async fn send(self, value: &S::Owned) -> Result<MessageId, PulsarError> {
        self.producer.warm_broker_schema().await?;
        let bytes = self
            .producer
            .schema
            .encode(value)
            .map_err(schema_to_pulsar)?;
        let mut with_payload = self.msg.value(bytes);
        crate::inject_otel_context(&mut with_payload.properties);
        let id = self
            .producer
            .inner
            .send(with_payload.into())
            .await
            .map_err(PulsarError::Client)?;
        Ok(id)
    }
}

/// Builder for a [`TypedProducer`]. The schema is required; the topic comes from the parent
/// [`PulsarClient::typed_producer`] entry point.
///
/// Generic over `E: Engine` (ADR-0026 §D1). The default
/// (`E = crate::TokioEngine`) keeps existing callers source-compatible.
/// Moonpool callers parametrise with
/// `TypedProducerBuilder<'_, S, MoonpoolEngine<P>>` and get a
/// `TypedProducer<S, magnetar_runtime_moonpool::Producer<P>>` from
/// [`Self::create`].
pub struct TypedProducerBuilder<'a, S: Schema, E: crate::Engine = crate::TokioEngine> {
    client: &'a PulsarClient<E>,
    topic: String,
    schema: Arc<S>,
    name: Option<String>,
    compression: magnetar_proto::types::CompressionKind,
    batching: Option<(usize, usize)>,
    chunking: bool,
    properties: Vec<(String, String)>,
    initial_sequence_id: Option<u64>,
    access_mode: pb::ProducerAccessMode,
    send_timeout: Option<std::time::Duration>,
    batching_max_publish_delay: Option<std::time::Duration>,
    /// Tokio-engine encryption hook (PIP-4). Only consulted on the
    /// tokio specialisation of [`Self::create`] — the engine-generic
    /// path routes through [`crate::CreateProducerApi`] which does not
    /// surface the encryptor.
    encryptor: Option<Arc<dyn magnetar_runtime_tokio::MessageEncryptor>>,
}

impl<S: Schema, E: crate::Engine> std::fmt::Debug for TypedProducerBuilder<'_, S, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedProducerBuilder")
            .field("topic", &self.topic)
            .field("schema_type", &self.schema.schema_type())
            .field("name", &self.name)
            .finish()
    }
}

impl<'a, S: Schema, E: crate::Engine> TypedProducerBuilder<'a, S, E> {
    pub(crate) fn new(client: &'a PulsarClient<E>, topic: String, schema: Arc<S>) -> Self {
        Self {
            client,
            topic,
            schema,
            name: None,
            compression: magnetar_proto::types::CompressionKind::None,
            batching: None,
            chunking: false,
            properties: Vec::new(),
            initial_sequence_id: None,
            access_mode: pb::ProducerAccessMode::Shared,
            send_timeout: None,
            batching_max_publish_delay: None,
            encryptor: None,
        }
    }

    /// Override the producer name advertised to the broker.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Mirrors `ProducerBuilder::compression`.
    #[must_use]
    pub fn compression(mut self, kind: magnetar_proto::types::CompressionKind) -> Self {
        self.compression = kind;
        self
    }

    /// Mirrors `ProducerBuilder::batching`.
    #[must_use]
    pub fn batching(mut self, max_messages: usize, max_bytes: usize) -> Self {
        self.batching = Some((max_messages, max_bytes));
        self
    }

    /// Mirrors `ProducerBuilder::chunking`.
    #[must_use]
    pub fn chunking(mut self, enable: bool) -> Self {
        self.chunking = enable;
        self
    }

    /// Mirrors `ProducerBuilder::property`.
    #[must_use]
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.push((key.into(), value.into()));
        self
    }

    /// Mirrors `ProducerBuilder::initial_sequence_id`.
    #[must_use]
    pub fn initial_sequence_id(mut self, id: u64) -> Self {
        self.initial_sequence_id = Some(id);
        self
    }

    /// Mirrors `ProducerBuilder::access_mode`.
    #[must_use]
    pub fn access_mode(mut self, mode: pb::ProducerAccessMode) -> Self {
        self.access_mode = mode;
        self
    }

    /// Mirrors `ProducerBuilder::send_timeout`.
    #[must_use]
    pub fn send_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.send_timeout = Some(timeout);
        self
    }

    /// Mirrors `ProducerBuilder::batching_max_publish_delay`.
    #[must_use]
    pub fn batching_max_publish_delay(mut self, delay: std::time::Duration) -> Self {
        self.batching_max_publish_delay = Some(delay);
        self
    }
}

impl<S: Schema, E: crate::Engine> TypedProducerBuilder<'_, S, E>
where
    E::ClientState: crate::CreateProducerApi + crate::BrokerMetadataApi,
{
    /// Build and open the producer via the engine-generic
    /// [`crate::CreateProducerApi`] trait. The configured schema is
    /// advertised on `CommandProducer.schema`.
    ///
    /// **PIP-4 encryption guardrail (BREAKING since the encryptor-storage lift).**
    /// If [`Self::encryption`] was called on the tokio specialisation,
    /// `.create()` returns [`PulsarError::Other`] instead of silently opening
    /// a plaintext producer. Use [`Self::create_with_encryption`] to honor
    /// the PIP-4 encryptor.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] if an encryptor was configured via [`Self::encryption`] — call
    ///   `create_with_encryption()` instead.
    /// - [`PulsarError::Other`] (stringified) on broker rejection or wire failure.
    pub async fn create(
        self,
    ) -> Result<TypedProducer<S, <E::ClientState as crate::CreateProducerApi>::Producer>, PulsarError>
    {
        if self.encryptor.is_some() {
            return Err(PulsarError::Other(
                "TypedProducerBuilder::create() refuses a configured encryptor — \
                 use create_with_encryption() to honor the PIP-4 encryptor"
                    .to_owned(),
            ));
        }
        let schema_pb = pb::Schema {
            name: self.topic.clone(),
            schema_data: self.schema.schema_data(),
            r#type: self.schema.schema_type() as i32,
            properties: self
                .schema
                .properties()
                .into_iter()
                .map(|(key, value)| pb::KeyValue { key, value })
                .collect(),
        };
        let mut builder = self
            .client
            .producer(self.topic)
            .schema(schema_pb)
            .compression(self.compression)
            .chunking(self.chunking)
            .access_mode(self.access_mode);
        if let Some(n) = self.name {
            builder = builder.name(n);
        }
        if let Some((max_msgs, max_bytes)) = self.batching {
            builder = builder.batching(max_msgs, max_bytes);
        }
        for (k, v) in self.properties {
            builder = builder.property(k, v);
        }
        if let Some(id) = self.initial_sequence_id {
            builder = builder.initial_sequence_id(id);
        }
        if let Some(t) = self.send_timeout {
            builder = builder.send_timeout(t);
        }
        if let Some(d) = self.batching_max_publish_delay {
            builder = builder.batching_max_publish_delay(d);
        }
        let inner = builder.create().await?;
        Ok(TypedProducer {
            inner,
            schema: self.schema,
        })
    }
}

/// Tokio-engine-specific `TypedProducerBuilder` methods that depend on
/// the tokio `MessageEncryptor` extension (PIP-4 not yet wired on
/// moonpool).
impl<S: Schema> TypedProducerBuilder<'_, S, crate::TokioEngine> {
    /// Mirrors `ProducerBuilder::encryption`. Only consulted by
    /// [`Self::create_with_encryption`].
    #[must_use]
    pub fn encryption(
        mut self,
        encryptor: Arc<dyn magnetar_runtime_tokio::MessageEncryptor>,
    ) -> Self {
        self.encryptor = Some(encryptor);
        self
    }

    /// Build and open the producer honoring the configured
    /// `MessageEncryptor` (PIP-4). The configured schema is advertised on
    /// `CommandProducer.schema`.
    pub async fn create_with_encryption(self) -> Result<TypedProducer<S>, PulsarError> {
        let schema_pb = pb::Schema {
            name: self.topic.clone(),
            schema_data: self.schema.schema_data(),
            r#type: self.schema.schema_type() as i32,
            properties: self
                .schema
                .properties()
                .into_iter()
                .map(|(key, value)| pb::KeyValue { key, value })
                .collect(),
        };
        let mut builder = self
            .client
            .producer(self.topic)
            .schema(schema_pb)
            .compression(self.compression)
            .chunking(self.chunking)
            .access_mode(self.access_mode);
        if let Some(n) = self.name {
            builder = builder.name(n);
        }
        if let Some((max_msgs, max_bytes)) = self.batching {
            builder = builder.batching(max_msgs, max_bytes);
        }
        for (k, v) in self.properties {
            builder = builder.property(k, v);
        }
        if let Some(id) = self.initial_sequence_id {
            builder = builder.initial_sequence_id(id);
        }
        if let Some(t) = self.send_timeout {
            builder = builder.send_timeout(t);
        }
        if let Some(d) = self.batching_max_publish_delay {
            builder = builder.batching_max_publish_delay(d);
        }
        if let Some(e) = self.encryptor {
            builder = builder.encryption(e);
        }
        let inner = builder.create_with_encryption().await?;
        Ok(TypedProducer {
            inner,
            schema: self.schema,
        })
    }
}

/// A decoded message yielded by [`TypedConsumer::receive`].
pub struct TypedMessage<S: Schema> {
    /// Broker-assigned message id (use it to ack).
    pub message_id: MessageId,
    /// The decoded value.
    pub value: S::Owned,
    /// Raw payload bytes (post-decryption, post-decompression). Useful when a caller wants to
    /// re-emit the message verbatim.
    pub payload: Bytes,
    /// The underlying incoming message (metadata, single-message metadata, etc.).
    pub raw: IncomingMessage,
}

impl<S: Schema> std::fmt::Debug for TypedMessage<S>
where
    S::Owned: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedMessage")
            .field("message_id", &self.message_id)
            .field("value", &self.value)
            .field("payload_len", &self.payload.len())
            .field("raw", &self.raw)
            .finish()
    }
}

/// A schema-aware consumer. Wraps a consumer and decodes every received payload with the
/// configured schema before returning to the caller.
///
/// Generic over `C: ConsumerApi` per ADR-0026 §D1. The default
/// (`C = magnetar_runtime_tokio::Consumer`) keeps existing callers —
/// `magnetar::TypedConsumer<S>` without a consumer type argument —
/// pointing at the tokio specialisation. Moonpool callers name
/// `TypedConsumer<S, magnetar_runtime_moonpool::Consumer<P>>`.
///
/// Engine-generic methods dispatch through [`crate::ConsumerApi`].
/// Methods that require the runtime's
/// `magnetar_proto::IncomingMessage` shape (the `receive` family,
/// `receive_batch`, `reconsume_later`, `republish_dead_letters`) or
/// helpers not on `ConsumerApi` today (`pause`, `resume`, `flow`,
/// `is_paused`, `has_reached_end_of_topic`, `available_in_queue`,
/// `available_permits`, `has_received_any_message`, `is_inactive`,
/// `ack_batch`, `ack_with_properties`,
/// `ack_cumulative_with_properties`, `ack_batch_with_txn`,
/// `seek_to_message`, `seek_to_timestamp`,
/// `receive_batch_with_bytes_cap`, `drain_dead_letter`, unsubscribe
/// with `force=true`) stay on the tokio specialisation
/// `impl TypedConsumer<S, magnetar_runtime_tokio::Consumer>` until the
/// trait grows them.
pub struct TypedConsumer<S: Schema, C: crate::ConsumerApi = Consumer> {
    inner: C,
    schema: Arc<S>,
}

impl<S: Schema, C: crate::ConsumerApi + std::fmt::Debug> std::fmt::Debug for TypedConsumer<S, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedConsumer")
            .field("inner", &self.inner)
            .field("schema_type", &self.schema.schema_type())
            .finish()
    }
}

impl<S: Schema, C: crate::ConsumerApi> TypedConsumer<S, C> {
    /// The inner runtime consumer.
    #[must_use]
    pub fn inner(&self) -> &C {
        &self.inner
    }

    /// Acknowledge a single message.
    pub async fn ack(&self, message_id: MessageId) -> Result<(), PulsarError> {
        crate::ConsumerApi::ack(&self.inner, message_id)
            .await
            .map_err(|err| PulsarError::Other(format!("ack: {err}")))
    }

    /// Close the underlying consumer.
    pub async fn close(self) -> Result<(), PulsarError> {
        crate::ConsumerApi::close_owned(self.inner)
            .await
            .map_err(|err| PulsarError::Other(format!("close: {err}")))
    }

    /// Topic this consumer is bound to. Mirrors Java `Consumer#getTopic`.
    #[must_use]
    pub fn topic(&self) -> String {
        crate::ConsumerApi::topic(&self.inner)
    }

    /// Subscription name. Mirrors Java `Consumer#getSubscription`.
    #[must_use]
    pub fn subscription(&self) -> String {
        crate::ConsumerApi::subscription(&self.inner)
    }

    /// Consumer name. Mirrors Java `Consumer#getConsumerName`.
    #[must_use]
    pub fn name(&self) -> String {
        crate::ConsumerApi::name(&self.inner)
    }

    /// `true` while the broker connection is up. Mirrors Java `Consumer#isConnected`.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        crate::ConsumerApi::is_connected(&self.inner)
    }

    /// `true` once [`Self::close`] / `unsubscribe` has completed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        crate::ConsumerApi::is_closed(&self.inner)
    }

    /// Cumulative consumer counters snapshot. Mirrors Java `Consumer#getStats`.
    #[must_use]
    pub fn stats(&self) -> magnetar_proto::ConsumerStats {
        crate::ConsumerApi::stats(&self.inner)
    }

    /// Wall-clock instant of the most-recent connection drop. Mirrors Java
    /// `Consumer#getLastDisconnectedTimestamp`.
    #[must_use]
    pub fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        crate::ConsumerApi::last_disconnected_timestamp(&self.inner)
    }

    /// Negative-ack a message. Mirrors Java `Consumer#negativeAcknowledge`.
    pub fn negative_ack(&self, message_id: MessageId) {
        crate::ConsumerApi::negative_ack(&self.inner, message_id);
    }

    /// Tell the broker to redeliver every unacked message. Mirrors Java
    /// `Consumer#redeliverUnacknowledgedMessages`.
    pub fn redeliver_unacked(&self) {
        crate::ConsumerApi::redeliver_unacked(&self.inner);
    }

    /// Cumulative ack. Mirrors Java `Consumer#acknowledgeCumulativeAsync(MessageId)`.
    pub async fn ack_cumulative(&self, message_id: MessageId) -> Result<(), PulsarError> {
        crate::ConsumerApi::ack_cumulative(&self.inner, message_id)
            .await
            .map_err(|err| PulsarError::Other(format!("ack_cumulative: {err}")))
    }

    /// Fire-and-forget ack into the consumer's ack-grouping tracker (opt-in via
    /// `TypedConsumerBuilder::ack_group_time`).
    pub fn ack_grouped(&self, message_id: MessageId) {
        crate::ConsumerApi::ack_grouped(&self.inner, message_id);
    }

    /// Fire-and-forget cumulative ack into the consumer's ack-grouping tracker.
    pub fn ack_grouped_cumulative(&self, message_id: MessageId) {
        crate::ConsumerApi::ack_grouped_cumulative(&self.inner, message_id);
    }

    /// Ack a single message inside a transaction. Mirrors Java
    /// `Consumer#acknowledgeAsync(MessageId, Transaction)`.
    pub async fn ack_with_txn(
        &self,
        message_id: MessageId,
        txn_id: magnetar_proto::TxnId,
    ) -> Result<(), PulsarError> {
        crate::ConsumerApi::ack_with_txn(&self.inner, message_id, txn_id)
            .await
            .map_err(|err| PulsarError::Other(format!("ack_with_txn: {err}")))
    }

    /// Cumulative ack inside a transaction. Mirrors Java
    /// `Consumer#acknowledgeCumulativeAsync(MessageId, Transaction)`.
    pub async fn ack_cumulative_with_txn(
        &self,
        message_id: MessageId,
        txn_id: magnetar_proto::TxnId,
    ) -> Result<(), PulsarError> {
        crate::ConsumerApi::ack_cumulative_with_txn(&self.inner, message_id, txn_id)
            .await
            .map_err(|err| PulsarError::Other(format!("ack_cumulative_with_txn: {err}")))
    }

    /// Seek to the earliest message. Mirrors Java `Consumer#seek(MessageId.earliest)`.
    pub async fn seek_to_earliest(&self) -> Result<(), PulsarError> {
        crate::ConsumerApi::seek_to_earliest(&self.inner)
            .await
            .map_err(|err| PulsarError::Other(format!("seek_to_earliest: {err}")))
    }

    /// Seek to the latest (head) position. Mirrors Java `Consumer#seek(MessageId.latest)`.
    pub async fn seek_to_latest(&self) -> Result<(), PulsarError> {
        crate::ConsumerApi::seek_to_latest(&self.inner)
            .await
            .map_err(|err| PulsarError::Other(format!("seek_to_latest: {err}")))
    }

    /// Ask the broker for the topic's last-published message id. Mirrors Java
    /// `Consumer#getLastMessageId`.
    pub async fn last_message_id(&self) -> Result<MessageId, PulsarError> {
        crate::ConsumerApi::last_message_id(&self.inner)
            .await
            .map_err(|err| PulsarError::Other(format!("last_message_id: {err}")))
    }

    /// `true` if the broker has at least one message strictly past `cursor`. Mirrors Java
    /// `Consumer#hasMessageAvailable` (the variant taking a cursor).
    pub async fn has_message_after(&self, cursor: MessageId) -> Result<bool, PulsarError> {
        crate::ConsumerApi::has_message_after(&self.inner, cursor)
            .await
            .map_err(|err| PulsarError::Other(format!("has_message_after: {err}")))
    }
}

/// Tokio-engine-specific `TypedConsumer` methods.
///
/// These methods depend on either (a) the runtime's
/// `magnetar_proto::IncomingMessage` shape (the `receive` family
/// returns the proto-level message so the consumer keeps access to
/// `single_metadata` + `arrived_at` — fields the engine-generic
/// [`crate::ConsumerApi::receive`] trait method drops by widening to
/// [`crate::IncomingMessage`]), or (b) `Consumer` helpers not yet on
/// [`crate::ConsumerApi`] (the long tail of `pause` / `flow` / the
/// extended ack family / DLQ / retry / batched receive). Each of these
/// methods can be lifted into the engine-generic impl block as the
/// matching helper lands on the trait — same incremental split as
/// `PartitionedProducer<P>`.
impl<S: Schema> TypedConsumer<S, Consumer> {
    /// Receive the next message. The payload is schema-decoded; if decoding fails the error
    /// is surfaced as [`PulsarError::Schema`] and the message remains unacked so the broker
    /// re-delivers it (subject to the consumer's redelivery policy).
    ///
    /// PIP-87 [`AutoConsumeSchema`](magnetar_proto::schema::AutoConsumeSchema) consumers
    /// transparently fetch the broker-registered schema on first call via
    /// [`Schema::needs_broker_schema`](magnetar_proto::schema::Schema::needs_broker_schema) +
    /// [`Schema::store_resolved_schema`](magnetar_proto::schema::Schema::store_resolved_schema).
    /// Subsequent receives reuse the cache. A broker-side schema-lookup failure surfaces as
    /// [`PulsarError::Client`] with [`magnetar_runtime_tokio::ClientError::Broker`].
    pub async fn receive(&self) -> Result<TypedMessage<S>, PulsarError> {
        if self.schema.needs_broker_schema() {
            let resolved = self
                .inner
                .get_schema(None)
                .await
                .map_err(PulsarError::Client)?;
            self.schema.store_resolved_schema(resolved);
        }
        let raw = self.inner.receive().await?;
        let value = self.schema.decode(&raw.payload).map_err(schema_to_pulsar)?;
        Ok(TypedMessage {
            message_id: raw.message_id,
            value,
            payload: raw.payload.clone(),
            raw,
        })
    }

    /// Pause delivery. Mirrors Java `Consumer#pause`.
    pub fn pause(&self) {
        self.inner.pause();
    }

    /// Resume delivery. Mirrors Java `Consumer#resume`.
    pub fn resume(&self) {
        self.inner.resume();
    }

    /// `true` after [`Self::pause`] until [`Self::resume`]. Mirrors Java
    /// `Consumer#isPaused` semantics.
    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.inner.is_paused()
    }

    /// `true` once the broker has signalled end-of-topic. Mirrors Java
    /// `Consumer#hasReachedEndOfTopic`.
    #[must_use]
    pub fn has_reached_end_of_topic(&self) -> bool {
        self.inner.has_reached_end_of_topic()
    }

    /// Buffered message count. Mirrors Java `Consumer#getNumMessagesInQueue`.
    #[must_use]
    pub fn available_in_queue(&self) -> usize {
        self.inner.available_in_queue()
    }

    /// Outstanding broker permits. Mirrors Java `ConsumerBase#getAvailablePermits`.
    #[must_use]
    pub fn available_permits(&self) -> u32 {
        self.inner.available_permits()
    }

    /// `true` if this consumer has received at least one message since opening. Mirrors
    /// Java `Consumer#hasReceivedAnyMessage`.
    #[must_use]
    pub fn has_received_any_message(&self) -> bool {
        self.inner.has_received_any_message()
    }

    /// `true` when the consumer has been disconnected longer than the configured inactive
    /// threshold. Mirrors Java `Consumer#isInactive` semantics.
    #[must_use]
    pub fn is_inactive(&self) -> bool {
        self.inner.is_inactive()
    }

    /// Batched individual ack. Mirrors Java `Consumer#acknowledgeAsync(List<MessageId>)`.
    pub async fn ack_batch(&self, message_ids: Vec<MessageId>) -> Result<(), PulsarError> {
        self.inner
            .ack_batch(message_ids)
            .await
            .map_err(PulsarError::Client)
    }

    /// Unsubscribe this consumer's subscription from the broker. Mirrors Java
    /// `Consumer#unsubscribe`. `force=true` (PIP-313) drops the subscription even when
    /// other consumers are still attached to the same subscription name.
    pub async fn unsubscribe(&self, force: bool) -> Result<(), PulsarError> {
        self.inner
            .unsubscribe(force)
            .await
            .map_err(PulsarError::Client)
    }

    /// Seek to a specific message id. Mirrors Java `Consumer#seek(MessageId)`.
    pub async fn seek_to_message(&self, message_id: MessageId) -> Result<(), PulsarError> {
        self.inner
            .seek_to_message(message_id)
            .await
            .map_err(PulsarError::Client)
    }

    /// Seek to a publish-time deadline (millis since epoch). Mirrors Java
    /// `Consumer#seek(long)`.
    pub async fn seek_to_timestamp(&self, publish_time_ms: u64) -> Result<(), PulsarError> {
        self.inner
            .seek_to_timestamp(publish_time_ms)
            .await
            .map_err(PulsarError::Client)
    }

    /// Issue an explicit FLOW (permit refill). Mirrors `ConsumerBase#increaseAvailablePermits`.
    pub fn flow(&self, permits: u32) {
        self.inner.flow(permits);
    }

    /// Same as [`Self::receive`] but bounded by `timeout`. Returns `Ok(None)` when the
    /// deadline elapses with no message. Mirrors Java
    /// `Consumer#receive(int timeout, TimeUnit unit)`.
    pub async fn receive_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Option<TypedMessage<S>>, PulsarError> {
        match self.inner.receive_with_timeout(timeout).await? {
            Some(raw) => {
                let value = self.schema.decode(&raw.payload).map_err(schema_to_pulsar)?;
                Ok(Some(TypedMessage {
                    message_id: raw.message_id,
                    value,
                    payload: raw.payload.clone(),
                    raw,
                }))
            }
            None => Ok(None),
        }
    }

    /// Batched receive. Mirrors Java `Consumer#batchReceive`. Decodes every payload with
    /// the schema; the first decode error short-circuits the call.
    pub async fn receive_batch(
        &self,
        max_messages: usize,
        max_wait: std::time::Duration,
    ) -> Result<Vec<TypedMessage<S>>, PulsarError> {
        let raw_batch = self.inner.receive_batch(max_messages, max_wait).await?;
        let mut out = Vec::with_capacity(raw_batch.len());
        for raw in raw_batch {
            let value = self.schema.decode(&raw.payload).map_err(schema_to_pulsar)?;
            out.push(TypedMessage {
                message_id: raw.message_id,
                value,
                payload: raw.payload.clone(),
                raw,
            });
        }
        Ok(out)
    }

    /// Batched receive with a bytes cap. See [`Self::receive_batch`] and the runtime's
    /// `Consumer::receive_batch_with_bytes_cap` for `BatchReceivePolicy` parity.
    pub async fn receive_batch_with_bytes_cap(
        &self,
        max_messages: usize,
        max_bytes: usize,
        max_wait: std::time::Duration,
    ) -> Result<Vec<TypedMessage<S>>, PulsarError> {
        let raw_batch = self
            .inner
            .receive_batch_with_bytes_cap(max_messages, max_bytes, max_wait)
            .await?;
        let mut out = Vec::with_capacity(raw_batch.len());
        for raw in raw_batch {
            let value = self.schema.decode(&raw.payload).map_err(schema_to_pulsar)?;
            out.push(TypedMessage {
                message_id: raw.message_id,
                value,
                payload: raw.payload.clone(),
                raw,
            });
        }
        Ok(out)
    }

    /// Ack with caller-supplied properties. Mirrors Java
    /// `Consumer#acknowledgeAsync(MessageId, Map<String, Long>)`.
    pub async fn ack_with_properties(
        &self,
        message_id: MessageId,
        properties: Vec<(String, i64)>,
    ) -> Result<(), PulsarError> {
        self.inner
            .ack_with_properties(message_id, properties)
            .await
            .map_err(PulsarError::Client)
    }

    /// Batched ack inside a transaction. Mirrors Java
    /// `Consumer#acknowledgeAsync(List<MessageId>, Transaction)`.
    pub async fn ack_batch_with_txn(
        &self,
        message_ids: Vec<MessageId>,
        txn_id: magnetar_proto::TxnId,
    ) -> Result<(), PulsarError> {
        self.inner
            .ack_batch_with_txn(message_ids, txn_id)
            .await
            .map_err(PulsarError::Client)
    }

    /// Cumulative ack with caller-supplied properties. Mirrors Java
    /// `Consumer#acknowledgeCumulativeAsync(MessageId, Map<String, Long>)`.
    pub async fn ack_cumulative_with_properties(
        &self,
        message_id: MessageId,
        properties: Vec<(String, i64)>,
    ) -> Result<(), PulsarError> {
        self.inner
            .ack_cumulative_with_properties(message_id, properties)
            .await
            .map_err(PulsarError::Client)
    }

    /// Drain every DLQ-flagged message (raw, un-decoded so schema mismatches don't lose
    /// the payload). See the runtime's `Consumer::drain_dead_letter`.
    #[must_use]
    pub fn drain_dead_letter(&self) -> Vec<IncomingMessage> {
        self.inner.drain_dead_letter()
    }

    /// Drain the DLQ pending list and republish every entry via `dlq_producer`. See the
    /// runtime's `Consumer::republish_dead_letters`. Returns the number republished.
    ///
    /// With the `opentelemetry` feature on, the republish span context is re-injected onto
    /// every dead-letter copy (overwriting any inbound `traceparent` / `tracestate`) so the
    /// republish is traced under the caller's current span; the original trace stays
    /// reachable via the `REAL_TOPIC` / `ORIGINAL_MESSAGE_ID` correlation stamps
    /// (ADR-0053 §D2).
    pub async fn republish_dead_letters(
        &self,
        dlq_producer: &magnetar_runtime_tokio::Producer,
    ) -> Result<usize, PulsarError> {
        let mut extra_properties = Vec::new();
        crate::inject_otel_context(&mut extra_properties);
        self.inner
            .republish_dead_letters_with_properties(dlq_producer, extra_properties)
            .await
            .map_err(PulsarError::Client)
    }

    /// Republish `msg` via `retry_producer` with a delay, then ack the original. Mirrors
    /// Java `Consumer#reconsumeLater(Message, long, TimeUnit)`. Takes the raw
    /// `IncomingMessage` (use [`TypedMessage::raw`]) so the original payload is
    /// preserved verbatim through the retry topic.
    ///
    /// With the `opentelemetry` feature on, the retrying consumer's current span context is
    /// re-injected onto the retry-letter copy, replacing the inbound `traceparent` /
    /// `tracestate` (ADR-0053 §D2).
    pub async fn reconsume_later(
        &self,
        retry_producer: &magnetar_runtime_tokio::Producer,
        msg: magnetar_proto::IncomingMessage,
        delay: std::time::Duration,
    ) -> Result<(), PulsarError> {
        // Route through the properties-aware variant so the OTel re-injection (ADR-0053 §D2)
        // happens on this path too.
        self.reconsume_later_with_properties(retry_producer, msg, Vec::new(), delay)
            .await
    }

    /// Same as [`Self::reconsume_later`] but stamps custom properties on the republished
    /// message. Mirrors Java's properties-aware reconsumeLater overload.
    ///
    /// With the `opentelemetry` feature on, the current span context is re-injected into
    /// `custom_properties` before the runtime merges them (override on key collision), so
    /// the retry-letter copy carries the retrying consumer's trace rather than the inbound
    /// one (ADR-0053 §D2). An explicit `traceparent` / `tracestate` in `custom_properties`
    /// is overwritten by the injected value.
    pub async fn reconsume_later_with_properties(
        &self,
        retry_producer: &magnetar_runtime_tokio::Producer,
        msg: magnetar_proto::IncomingMessage,
        mut custom_properties: Vec<(String, String)>,
        delay: std::time::Duration,
    ) -> Result<(), PulsarError> {
        crate::inject_otel_context(&mut custom_properties);
        self.inner
            .reconsume_later_with_properties(retry_producer, msg, custom_properties, delay)
            .await
            .map_err(PulsarError::Client)
    }
}

/// Builder for a [`TypedConsumer`].
///
/// Generic over `E: Engine` (ADR-0026 §D1). The default
/// (`E = crate::TokioEngine`) keeps existing callers source-compatible.
/// Moonpool callers parametrise with
/// `TypedConsumerBuilder<'_, S, MoonpoolEngine<P>>` and get a
/// `TypedConsumer<S, magnetar_runtime_moonpool::Consumer<P>>` from
/// [`Self::subscribe`].
pub struct TypedConsumerBuilder<'a, S: Schema, E: crate::Engine = crate::TokioEngine> {
    client: &'a PulsarClient<E>,
    topic: String,
    schema: Arc<S>,
    subscription: Option<String>,
    sub_type: pb::command_subscribe::SubType,
    durable: bool,
    initial_position: pb::command_subscribe::InitialPosition,
    receiver_queue_size: usize,
    consumer_name: Option<String>,
    priority_level: Option<i32>,
    properties: Vec<(String, String)>,
    subscription_properties: Vec<(String, String)>,
    read_compacted: bool,
    negative_ack_redelivery_delay: Option<std::time::Duration>,
    ack_timeout: Option<std::time::Duration>,
    ack_group_time: Option<std::time::Duration>,
    dlq_policy: Option<(u32, Option<String>)>,
    key_shared: Option<magnetar_proto::KeySharedConfig>,
    start_message_id: Option<magnetar_proto::MessageId>,
    replicate_subscription_state: Option<bool>,
    force_topic_creation: Option<bool>,
    start_message_rollback_duration_sec: Option<u64>,
}

impl<S: Schema, E: crate::Engine> std::fmt::Debug for TypedConsumerBuilder<'_, S, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedConsumerBuilder")
            .field("topic", &self.topic)
            .field("schema_type", &self.schema.schema_type())
            .field("subscription", &self.subscription)
            .field("sub_type", &self.sub_type)
            .field("durable", &self.durable)
            .finish()
    }
}

impl<'a, S: Schema, E: crate::Engine> TypedConsumerBuilder<'a, S, E> {
    pub(crate) fn new(client: &'a PulsarClient<E>, topic: String, schema: Arc<S>) -> Self {
        Self {
            client,
            topic,
            schema,
            subscription: None,
            sub_type: pb::command_subscribe::SubType::Exclusive,
            durable: true,
            initial_position: pb::command_subscribe::InitialPosition::Latest,
            receiver_queue_size: 1000,
            consumer_name: None,
            priority_level: None,
            properties: Vec::new(),
            subscription_properties: Vec::new(),
            read_compacted: false,
            negative_ack_redelivery_delay: None,
            ack_timeout: None,
            ack_group_time: None,
            dlq_policy: None,
            key_shared: None,
            start_message_id: None,
            replicate_subscription_state: None,
            force_topic_creation: None,
            start_message_rollback_duration_sec: None,
        }
    }

    /// Set the consumer name advertised to the broker. Mirrors Java
    /// `ConsumerBuilder#consumerName`.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.consumer_name = Some(name.into());
        self
    }

    /// Mirrors `ConsumerBuilder::priority_level`.
    #[must_use]
    pub fn priority_level(mut self, level: i32) -> Self {
        self.priority_level = Some(level);
        self
    }

    /// Mirrors `ConsumerBuilder::property`.
    #[must_use]
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.push((key.into(), value.into()));
        self
    }

    /// Mirrors `ConsumerBuilder::subscription_property`.
    #[must_use]
    pub fn subscription_property(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.subscription_properties
            .push((key.into(), value.into()));
        self
    }

    /// Mirrors `ConsumerBuilder::read_compacted`.
    #[must_use]
    pub fn read_compacted(mut self, on: bool) -> Self {
        self.read_compacted = on;
        self
    }

    /// Mirrors `ConsumerBuilder::negative_ack_redelivery_delay`.
    #[must_use]
    pub fn negative_ack_redelivery_delay(mut self, delay: std::time::Duration) -> Self {
        self.negative_ack_redelivery_delay = Some(delay);
        self
    }

    /// Mirrors `ConsumerBuilder::ack_timeout`.
    #[must_use]
    pub fn ack_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.ack_timeout = Some(timeout);
        self
    }

    /// Mirrors `ConsumerBuilder::ack_group_time`. Coalesces fire-and-forget acks emitted
    /// via [`TypedConsumer::ack_grouped`] / [`TypedConsumer::ack_grouped_cumulative`].
    #[must_use]
    pub fn ack_group_time(mut self, window: std::time::Duration) -> Self {
        self.ack_group_time = Some(window);
        self
    }

    /// Mirrors `ConsumerBuilder::dead_letter_policy`.
    #[must_use]
    pub fn dead_letter_policy(
        mut self,
        max_redeliver_count: u32,
        dead_letter_topic: Option<String>,
    ) -> Self {
        self.dlq_policy = Some((max_redeliver_count, dead_letter_topic));
        self
    }

    /// Mirrors `ConsumerBuilder::key_shared_policy`. Only meaningful with `Key_Shared`
    /// subscription type.
    #[must_use]
    pub fn key_shared_policy(mut self, cfg: magnetar_proto::KeySharedConfig) -> Self {
        self.key_shared = Some(cfg);
        self
    }

    /// Mirrors `ConsumerBuilder::start_message_id`. Only honoured for fresh subscriptions.
    #[must_use]
    pub fn start_message_id(mut self, id: magnetar_proto::MessageId) -> Self {
        self.start_message_id = Some(id);
        self
    }

    /// Mirrors `ConsumerBuilder::replicate_subscription_state`.
    #[must_use]
    pub fn replicate_subscription_state(mut self, on: bool) -> Self {
        self.replicate_subscription_state = Some(on);
        self
    }

    /// Mirrors `ConsumerBuilder::force_topic_creation`.
    #[must_use]
    pub fn force_topic_creation(mut self, on: bool) -> Self {
        self.force_topic_creation = Some(on);
        self
    }

    /// Mirrors `ConsumerBuilder::start_message_rollback_duration`. Rolls the subscription
    /// cursor back by `seconds` at subscribe time.
    #[must_use]
    pub fn start_message_rollback_duration(mut self, seconds: u64) -> Self {
        self.start_message_rollback_duration_sec = Some(seconds);
        self
    }

    /// Required: set the subscription name.
    #[must_use]
    pub fn subscription(mut self, name: impl Into<String>) -> Self {
        self.subscription = Some(name.into());
        self
    }

    /// Set the subscription type.
    #[must_use]
    pub fn subscription_type(mut self, sub_type: pb::command_subscribe::SubType) -> Self {
        self.sub_type = sub_type;
        self
    }

    /// Toggle durability.
    #[must_use]
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// Set the initial position the broker dispatches from when the subscription is new.
    #[must_use]
    pub fn initial_position(mut self, position: pb::command_subscribe::InitialPosition) -> Self {
        self.initial_position = position;
        self
    }

    /// Set the receiver queue size.
    #[must_use]
    pub fn receiver_queue_size(mut self, size: usize) -> Self {
        self.receiver_queue_size = size;
        self
    }
}

impl<S: Schema, E: crate::Engine> TypedConsumerBuilder<'_, S, E>
where
    E::ClientState: crate::SubscribeApi,
{
    /// Build and subscribe via the engine-generic [`crate::SubscribeApi`]
    /// trait. The configured schema is advertised on
    /// `CommandSubscribe.schema`.
    pub async fn subscribe(
        self,
    ) -> Result<TypedConsumer<S, <E::ClientState as crate::SubscribeApi>::Consumer>, PulsarError>
    {
        let subscription = self
            .subscription
            .ok_or_else(|| PulsarError::Config("subscription name is required".to_owned()))?;
        let schema_pb = pb::Schema {
            name: self.topic.clone(),
            schema_data: self.schema.schema_data(),
            r#type: self.schema.schema_type() as i32,
            properties: self
                .schema
                .properties()
                .into_iter()
                .map(|(key, value)| pb::KeyValue { key, value })
                .collect(),
        };
        let mut builder = self
            .client
            .consumer(self.topic)
            .subscription(subscription)
            .subscription_type(self.sub_type)
            .durable(self.durable)
            .initial_position(self.initial_position)
            .receiver_queue_size(self.receiver_queue_size)
            .read_compacted(self.read_compacted)
            .schema(schema_pb);
        if let Some(name) = self.consumer_name {
            builder = builder.name(name);
        }
        if let Some(level) = self.priority_level {
            builder = builder.priority_level(level);
        }
        for (k, v) in self.properties {
            builder = builder.property(k, v);
        }
        for (k, v) in self.subscription_properties {
            builder = builder.subscription_property(k, v);
        }
        if let Some(d) = self.negative_ack_redelivery_delay {
            builder = builder.negative_ack_redelivery_delay(d);
        }
        if let Some(t) = self.ack_timeout {
            builder = builder.ack_timeout(t);
        }
        if let Some(w) = self.ack_group_time {
            builder = builder.ack_group_time(w);
        }
        if let Some((max, topic_opt)) = self.dlq_policy {
            builder = builder.dead_letter_policy(max, topic_opt);
        }
        if let Some(cfg) = self.key_shared {
            builder = builder.key_shared_policy(cfg);
        }
        if let Some(id) = self.start_message_id {
            builder = builder.start_message_id(id);
        }
        if let Some(on) = self.replicate_subscription_state {
            builder = builder.replicate_subscription_state(on);
        }
        if let Some(on) = self.force_topic_creation {
            builder = builder.force_topic_creation(on);
        }
        if let Some(sec) = self.start_message_rollback_duration_sec {
            builder = builder.start_message_rollback_duration(sec);
        }
        let inner = builder.subscribe().await?;
        Ok(TypedConsumer {
            inner,
            schema: self.schema,
        })
    }
}

fn schema_to_pulsar(err: SchemaError) -> PulsarError {
    PulsarError::Schema(err)
}
