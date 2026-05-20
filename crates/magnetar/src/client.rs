// SPDX-License-Identifier: Apache-2.0

//! Ergonomic top-level client built on the tokio engine.
//!
//! Wraps [`magnetar_runtime_tokio::Client`] with a builder API plus simple
//! `producer(topic).create()` / `consumer(topic).subscription(s).subscribe()`
//! constructors so the common path doesn't expose raw protocol types like
//! [`magnetar_proto::conn::CreateProducerRequest`] unless the user wants
//! them.

use std::time::Duration;

use bytes::Bytes;
use magnetar_proto::conn::{CreateProducerRequest, SubscribeRequest};
use magnetar_proto::pb;
use magnetar_proto::types::CompressionKind;
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
        for (k, v) in msg.properties {
            metadata.properties.push(pb::KeyValue { key: k, value: v });
        }
        let uncompressed_size = u32::try_from(msg.payload.len()).unwrap_or(u32::MAX);
        Self {
            payload: msg.payload,
            metadata,
            uncompressed_size,
            num_messages: 1,
        }
    }
}

/// Convenience alias for an incoming message handed back to the caller.
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    /// Message id assigned by the broker.
    pub id: magnetar_proto::types::MessageId,
    /// Pulsar `MessageMetadata` for the message.
    pub metadata: pb::MessageMetadata,
    /// Application payload bytes (post-decompression / post-decryption).
    pub payload: Bytes,
}

impl From<magnetar_proto::event::IncomingMessage> for IncomingMessage {
    fn from(msg: magnetar_proto::event::IncomingMessage) -> Self {
        Self {
            id: msg.message_id,
            metadata: msg.metadata,
            payload: msg.payload,
        }
    }
}

/// High-level Pulsar client. Backed by the tokio engine.
#[derive(Debug)]
pub struct PulsarClient {
    inner: Client,
}

impl PulsarClient {
    /// Start building a client.
    #[must_use]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Open a `ProducerBuilder` for the given topic.
    #[must_use]
    pub fn producer(&self, topic: impl Into<String>) -> ProducerBuilder<'_> {
        ProducerBuilder::new(self, topic.into())
    }

    /// Open a `ConsumerBuilder` for the given topic.
    #[must_use]
    pub fn consumer(&self, topic: impl Into<String>) -> ConsumerBuilder<'_> {
        ConsumerBuilder::new(self, topic.into())
    }

    /// Close the underlying connection.
    pub async fn close(self) {
        self.inner.close().await;
    }
}

/// Builder for [`PulsarClient`].
#[derive(Debug, Default, Clone)]
pub struct ClientBuilder {
    service_url: Option<String>,
    client_version: Option<String>,
    keepalive: Option<Duration>,
    operation_timeout: Option<Duration>,
    auth_method_name: Option<String>,
    auth_data: Option<Vec<u8>>,
}

impl ClientBuilder {
    /// Set the Pulsar service URL (`pulsar://` or `pulsar+ssl://`).
    #[must_use]
    pub fn service_url(mut self, url: impl Into<String>) -> Self {
        self.service_url = Some(url.into());
        self
    }

    /// Override the advertised client version.
    #[must_use]
    pub fn client_version(mut self, version: impl Into<String>) -> Self {
        self.client_version = Some(version.into());
        self
    }

    /// Set the keep-alive (ping) interval.
    #[must_use]
    pub fn keepalive(mut self, dur: Duration) -> Self {
        self.keepalive = Some(dur);
        self
    }

    /// Set the operation timeout (lookup + send).
    #[must_use]
    pub fn operation_timeout(mut self, dur: Duration) -> Self {
        self.operation_timeout = Some(dur);
        self
    }

    /// Use the supplied auth provider to populate the initial CONNECT auth data.
    #[must_use]
    pub fn auth(mut self, provider: &dyn magnetar_proto::AuthProvider) -> Self {
        self.auth_method_name = Some(provider.method().to_owned());
        self.auth_data = provider.initial().ok().map(|bytes| bytes.to_vec());
        self
    }

    /// Build and connect the client.
    ///
    /// # Errors
    /// Returns [`PulsarError::Config`] if the service URL is missing, or
    /// [`PulsarError::Client`] if the underlying tokio engine fails to
    /// connect.
    pub async fn build(self) -> Result<PulsarClient> {
        let service_url = self
            .service_url
            .ok_or_else(|| PulsarError::Config("service_url is required".to_owned()))?;
        let mut config = magnetar_proto::conn::ConnectionConfig::default();
        if let Some(v) = self.client_version {
            config.client_version = v;
        }
        if let Some(d) = self.keepalive {
            config.keepalive_interval = d;
        }
        if let Some(d) = self.operation_timeout {
            config.operation_timeout = d;
        }
        if let Some(name) = self.auth_method_name {
            config.auth_method_name = name;
        }
        if let Some(data) = self.auth_data {
            config.auth_data = Some(data);
        }
        let inner = Client::connect(&service_url, config).await?;
        Ok(PulsarClient { inner })
    }
}

/// Builder for a producer.
#[derive(Debug)]
pub struct ProducerBuilder<'a> {
    client: &'a PulsarClient,
    req: CreateProducerRequest,
}

impl<'a> ProducerBuilder<'a> {
    fn new(client: &'a PulsarClient, topic: String) -> Self {
        let req = CreateProducerRequest {
            topic,
            ..CreateProducerRequest::default()
        };
        Self { client, req }
    }

    /// Set the optional producer name.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.req.producer_name = Some(name.into());
        self
    }

    /// Enable batching with the given limits.
    #[must_use]
    pub fn batching(mut self, max_messages: usize, max_bytes: usize) -> Self {
        self.req.enable_batching = true;
        self.req.max_messages_in_batch = max_messages;
        self.req.max_batch_size_bytes = max_bytes;
        self
    }

    /// Enable chunking for oversize messages.
    #[must_use]
    pub fn chunking(mut self, enable: bool) -> Self {
        self.req.enable_chunking = enable;
        self
    }

    /// Set the compression codec.
    #[must_use]
    pub fn compression(mut self, kind: CompressionKind) -> Self {
        self.req.compression = kind;
        self
    }

    /// Open the producer.
    pub async fn create(self) -> Result<magnetar_runtime_tokio::Producer> {
        Ok(self.client.inner.open_producer(self.req).await?)
    }
}

/// Builder for a consumer.
#[derive(Debug)]
pub struct ConsumerBuilder<'a> {
    client: &'a PulsarClient,
    req: SubscribeRequest,
}

impl<'a> ConsumerBuilder<'a> {
    fn new(client: &'a PulsarClient, topic: String) -> Self {
        let req = SubscribeRequest {
            topic,
            ..SubscribeRequest::default()
        };
        Self { client, req }
    }

    /// Required: set the subscription name.
    #[must_use]
    pub fn subscription(mut self, name: impl Into<String>) -> Self {
        self.req.subscription = name.into();
        self
    }

    /// Set the subscription type.
    #[must_use]
    pub fn subscription_type(mut self, sub_type: pb::command_subscribe::SubType) -> Self {
        self.req.sub_type = sub_type;
        self
    }

    /// Set the consumer name.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.req.consumer_name = Some(name.into());
        self
    }

    /// Set the receiver queue size.
    #[must_use]
    pub fn receiver_queue_size(mut self, size: usize) -> Self {
        self.req.receiver_queue_size = size;
        self
    }

    /// Subscribe.
    pub async fn subscribe(self) -> Result<magnetar_runtime_tokio::Consumer> {
        Ok(self.client.inner.subscribe(self.req).await?)
    }
}
