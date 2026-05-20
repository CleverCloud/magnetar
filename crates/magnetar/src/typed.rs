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
        }
    }

    /// Override the producer name advertised to the broker.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
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
        let mut builder = self.client.producer(self.topic).schema(schema_pb);
        if let Some(n) = self.name {
            builder = builder.name(n);
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
        }
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
        let inner = self
            .client
            .consumer(self.topic)
            .subscription(subscription)
            .subscription_type(self.sub_type)
            .durable(self.durable)
            .initial_position(self.initial_position)
            .receiver_queue_size(self.receiver_queue_size)
            .schema(schema_pb)
            .subscribe()
            .await?;
        Ok(TypedConsumer {
            inner,
            schema: self.schema,
        })
    }
}

fn schema_to_pulsar(err: SchemaError) -> PulsarError {
    PulsarError::Schema(err)
}
