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

use std::sync::Arc;

use bytes::Bytes;
use magnetar_proto::producer::OutgoingMessage as ProtoOutgoingMessage;
use magnetar_proto::schema::{Schema, SchemaError};
use magnetar_proto::{IncomingMessage, MessageId, pb};
use magnetar_runtime_tokio::{Consumer, Producer};

use crate::PulsarClient;
use crate::client::PulsarError;

/// A schema-aware producer. Wraps a [`Producer`] and applies the configured schema to every
/// outbound value.
pub struct TypedProducer<S: Schema> {
    inner: Producer,
    schema: Arc<S>,
}

impl<S: Schema> std::fmt::Debug for TypedProducer<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedProducer")
            .field("inner", &self.inner)
            .field("schema_type", &self.schema.schema_type())
            .finish()
    }
}

impl<S: Schema> TypedProducer<S> {
    /// The inner runtime producer. Useful for accessing connection-state observers and stats.
    #[must_use]
    pub fn inner(&self) -> &Producer {
        &self.inner
    }

    /// Encode `value` with the schema and publish it. `key` (optional) becomes the message's
    /// `partition_key`, which the broker uses for compaction and `key_shared` routing.
    pub async fn send(
        &self,
        value: &S::Owned,
        key: Option<String>,
    ) -> Result<MessageId, PulsarError> {
        let bytes = self.schema.encode(value).map_err(schema_to_pulsar)?;
        let mut metadata = pb::MessageMetadata::default();
        if let Some(k) = key {
            metadata.partition_key = Some(k);
            metadata.partition_key_b64_encoded = Some(false);
        }
        let payload_len = bytes.len();
        let msg = ProtoOutgoingMessage {
            payload: bytes,
            metadata,
            uncompressed_size: u32::try_from(payload_len).unwrap_or(u32::MAX),
            num_messages: 1,
            txn_id: None,
        };
        let id = self.inner.send(msg).await?;
        Ok(id)
    }

    /// Close the underlying producer.
    pub async fn close(self) -> Result<(), PulsarError> {
        self.inner.close().await.map_err(PulsarError::Client)
    }
}

/// Builder for a [`TypedProducer`]. The schema is required; the topic comes from the parent
/// [`PulsarClient::typed_producer`] entry point.
pub struct TypedProducerBuilder<'a, S: Schema> {
    client: &'a PulsarClient,
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
    encryptor: Option<Arc<dyn magnetar_runtime_tokio::MessageEncryptor>>,
}

impl<S: Schema> std::fmt::Debug for TypedProducerBuilder<'_, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedProducerBuilder")
            .field("topic", &self.topic)
            .field("schema_type", &self.schema.schema_type())
            .field("name", &self.name)
            .finish()
    }
}

impl<'a, S: Schema> TypedProducerBuilder<'a, S> {
    pub(crate) fn new(client: &'a PulsarClient, topic: String, schema: Arc<S>) -> Self {
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

    /// Mirrors `ProducerBuilder::encryption`.
    #[must_use]
    pub fn encryption(
        mut self,
        encryptor: Arc<dyn magnetar_runtime_tokio::MessageEncryptor>,
    ) -> Self {
        self.encryptor = Some(encryptor);
        self
    }

    /// Build and open the producer. The configured schema is advertised on
    /// `CommandProducer.schema`.
    pub async fn create(self) -> Result<TypedProducer<S>, PulsarError> {
        let schema_pb = pb::Schema {
            name: self.topic.clone(),
            schema_data: self.schema.schema_data().to_vec(),
            r#type: self.schema.schema_type() as i32,
            properties: Vec::new(),
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
        if let Some(e) = self.encryptor {
            builder = builder.encryption(e);
        }
        let inner = builder.create().await?;
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

/// A schema-aware consumer. Wraps a [`Consumer`] and decodes every received payload with the
/// configured schema before returning to the caller.
pub struct TypedConsumer<S: Schema> {
    inner: Consumer,
    schema: Arc<S>,
}

impl<S: Schema> std::fmt::Debug for TypedConsumer<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedConsumer")
            .field("inner", &self.inner)
            .field("schema_type", &self.schema.schema_type())
            .finish()
    }
}

impl<S: Schema> TypedConsumer<S> {
    /// The inner runtime consumer.
    #[must_use]
    pub fn inner(&self) -> &Consumer {
        &self.inner
    }

    /// Receive the next message. The payload is schema-decoded; if decoding fails the error
    /// is surfaced as [`PulsarError::Schema`] and the message remains unacked so the broker
    /// re-delivers it (subject to the consumer's redelivery policy).
    pub async fn receive(&self) -> Result<TypedMessage<S>, PulsarError> {
        let raw = self.inner.receive().await?;
        let value = self.schema.decode(&raw.payload).map_err(schema_to_pulsar)?;
        Ok(TypedMessage {
            message_id: raw.message_id,
            value,
            payload: raw.payload.clone(),
            raw,
        })
    }

    /// Acknowledge a single message.
    pub async fn ack(&self, message_id: MessageId) -> Result<(), PulsarError> {
        self.inner
            .ack(message_id)
            .await
            .map_err(PulsarError::Client)
    }

    /// Close the underlying consumer.
    pub async fn close(self) -> Result<(), PulsarError> {
        self.inner.close().await.map_err(PulsarError::Client)
    }
}

/// Builder for a [`TypedConsumer`].
pub struct TypedConsumerBuilder<'a, S: Schema> {
    client: &'a PulsarClient,
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
    dlq_policy: Option<(u32, Option<String>)>,
}

impl<S: Schema> std::fmt::Debug for TypedConsumerBuilder<'_, S> {
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

impl<'a, S: Schema> TypedConsumerBuilder<'a, S> {
    pub(crate) fn new(client: &'a PulsarClient, topic: String, schema: Arc<S>) -> Self {
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
            dlq_policy: None,
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

    /// Build and subscribe. The configured schema is advertised on `CommandSubscribe.schema`.
    pub async fn subscribe(self) -> Result<TypedConsumer<S>, PulsarError> {
        let subscription = self
            .subscription
            .ok_or_else(|| PulsarError::Config("subscription name is required".to_owned()))?;
        let schema_pb = pb::Schema {
            name: self.topic.clone(),
            schema_data: self.schema.schema_data().to_vec(),
            r#type: self.schema.schema_type() as i32,
            properties: Vec::new(),
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
        if let Some((max, topic_opt)) = self.dlq_policy {
            builder = builder.dead_letter_policy(max, topic_opt);
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
