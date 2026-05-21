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
    /// Schema encode / decode error from a [`crate::TypedProducer`] / [`crate::TypedConsumer`].
    #[error("schema error: {0}")]
    Schema(#[from] magnetar_proto::schema::SchemaError),
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

    /// Mirrors `TypedMessageBuilder#deliverAfter`. Adds `delay_ms` to the current wall-clock
    /// time and stamps the resulting absolute deadline on the message.
    #[must_use]
    pub fn deliver_after_ms(mut self, delay_ms: i64) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        self.deliver_at_ms = Some(now.saturating_add(delay_ms));
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
    /// Broker-supplied redelivery count.
    pub redelivery_count: u32,
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
}

impl From<magnetar_proto::event::IncomingMessage> for IncomingMessage {
    fn from(msg: magnetar_proto::event::IncomingMessage) -> Self {
        Self {
            id: msg.message_id,
            metadata: msg.metadata,
            payload: msg.payload,
            redelivery_count: msg.redelivery_count,
        }
    }
}

/// High-level Pulsar client. Backed by the tokio engine.
#[derive(Debug)]
pub struct PulsarClient {
    inner: Client,
}

impl PulsarClient {
    /// Borrow the underlying runtime client. Re-exported for sibling modules
    /// ([`crate::PartitionedProducer`]) that need to call lower-level methods like
    /// `partitioned_topic_metadata` without going through a builder.
    pub(crate) fn runtime_client(&self) -> &Client {
        &self.inner
    }

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

    /// Open a [`ReaderBuilder`] for the given topic. A reader is a non-durable, exclusive
    /// consumer with an auto-generated subscription — useful for log inspection and replay.
    #[must_use]
    pub fn reader(&self, topic: impl Into<String>) -> ReaderBuilder<'_> {
        ReaderBuilder::new(self, topic.into())
    }

    /// Open a [`crate::TableViewBuilder`] for the given topic. A [`crate::TableView`] is a
    /// key/value snapshot built from a compacted topic — useful for config snapshots and
    /// similar "latest value wins per key" patterns. Mirrors
    /// `PulsarClient#newTableViewBuilder`.
    #[must_use]
    pub fn table_view(&self, topic: impl Into<String>) -> crate::TableViewBuilder<'_> {
        crate::TableViewBuilder::new(self, topic.into())
    }

    /// Open a [`crate::MultiTopicsConsumerBuilder`] that subscribes to many topics at once.
    /// Mirrors Java's `PulsarClient#newConsumer().topics(...)`.
    #[must_use]
    pub fn multi_topics_consumer(&self) -> crate::MultiTopicsConsumerBuilder<'_> {
        crate::MultiTopicsConsumerBuilder::new(self)
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

    /// Open a [`crate::PartitionedConsumerBuilder`] for the given topic. The builder
    /// auto-discovers the partition count and subscribes to every partition under a single
    /// subscription name. Mirrors Java's `PulsarClient#newConsumer()` against a partitioned
    /// topic.
    #[must_use]
    pub fn partitioned_consumer(
        &self,
        topic: impl Into<String>,
    ) -> crate::PartitionedConsumerBuilder<'_> {
        crate::PartitionedConsumerBuilder::new(self, topic.into())
    }

    /// Query the broker for the partition count of `topic`. Returns `0` for non-partitioned
    /// topics. Mirrors Java `PulsarClient#getPartitionsForTopic`.
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Client`] if the broker refuses the metadata lookup.
    pub async fn partitions_for_topic(&self, topic: &str) -> Result<u32> {
        self.inner
            .partitioned_topic_metadata(topic)
            .await
            .map_err(PulsarError::Client)
    }

    /// Subscribe to a topic-list watcher and return the initial topic snapshot for the
    /// given namespace + regex pattern (PIP-145). Useful for "discover all topics matching
    /// this pattern right now" workflows. Live updates are emitted by the connection as
    /// `TopicListChanged` events but are not yet streamed by this helper.
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Client`] if the broker refuses the watch.
    pub async fn topic_list_snapshot(&self, namespace: &str, pattern: &str) -> Result<Vec<String>> {
        self.inner
            .watch_topic_list(namespace, pattern)
            .await
            .map_err(PulsarError::Client)
    }

    /// Open a schema-aware [`crate::TypedProducerBuilder`] for the given topic. Mirrors Java's
    /// `PulsarClient#newProducer(Schema<T>)`.
    #[must_use]
    pub fn typed_producer<S: magnetar_proto::schema::Schema>(
        &self,
        topic: impl Into<String>,
        schema: std::sync::Arc<S>,
    ) -> crate::TypedProducerBuilder<'_, S> {
        crate::TypedProducerBuilder::new(self, topic.into(), schema)
    }

    /// Open a schema-aware [`crate::TypedConsumerBuilder`] for the given topic. Mirrors Java's
    /// `PulsarClient#newConsumer(Schema<T>)`.
    #[must_use]
    pub fn typed_consumer<S: magnetar_proto::schema::Schema>(
        &self,
        topic: impl Into<String>,
        schema: std::sync::Arc<S>,
    ) -> crate::TypedConsumerBuilder<'_, S> {
        crate::TypedConsumerBuilder::new(self, topic.into(), schema)
    }

    /// Close the underlying connection.
    pub async fn close(self) {
        self.inner.close().await;
    }

    /// Returns `true` while the underlying broker connection is up. Mirrors Java's
    /// `org.apache.pulsar.client.api.Producer#isConnected` and
    /// `Consumer#isConnected` at the client scope.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.inner.is_connected()
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

/// Builder for [`PulsarClient`].
#[derive(Debug, Default, Clone)]
pub struct ClientBuilder {
    service_url: Option<String>,
    client_version: Option<String>,
    keepalive: Option<Duration>,
    operation_timeout: Option<Duration>,
    auth_method_name: Option<String>,
    auth_data: Option<Vec<u8>>,
    auth_provider: Option<std::sync::Arc<dyn magnetar_proto::AuthProvider>>,
    tls_trust_certs_pem: Option<Vec<u8>>,
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

    /// Use the supplied auth provider to populate the initial CONNECT auth data,
    /// and keep the provider for in-band `CommandAuthChallenge` refresh
    /// (PIP-30 / PIP-292).
    #[must_use]
    pub fn auth(mut self, provider: std::sync::Arc<dyn magnetar_proto::AuthProvider>) -> Self {
        self.auth_method_name = Some(provider.method().to_owned());
        self.auth_data = provider.initial().ok().map(|bytes| bytes.to_vec());
        self.auth_provider = Some(provider);
        self
    }

    /// Mirrors Java `ClientBuilder#tlsTrustCertsFilePath`. Supplies a PEM-encoded chain
    /// (typically a self-signed CA used by the broker). When set, the connection's TLS
    /// handshake validates the broker against this chain INSTEAD OF the system trust
    /// store. Only honoured for `pulsar+ssl://` URLs.
    #[must_use]
    pub fn tls_trust_certs_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.tls_trust_certs_pem = Some(pem.into());
        self
    }

    /// Convenience: read a PEM file from `path` and apply it via [`Self::tls_trust_certs_pem`].
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Config`] if the file cannot be read.
    pub fn tls_trust_certs_file_path(mut self, path: impl AsRef<std::path::Path>) -> Result<Self> {
        let bytes = std::fs::read(path.as_ref())
            .map_err(|e| PulsarError::Config(format!("read tls trust certs file: {e}")))?;
        self.tls_trust_certs_pem = Some(bytes);
        Ok(self)
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
        let inner = if let Some(pem) = self.tls_trust_certs_pem {
            let parsed = magnetar_runtime_tokio::ParsedUrl::parse(&service_url)?;
            let tls_config = match parsed.scheme {
                magnetar_runtime_tokio::Scheme::Tls => Some(Client::tls_config_from_pem(&pem)?),
                magnetar_runtime_tokio::Scheme::Plain => None,
            };
            Client::connect_with(parsed, tls_config, config, self.auth_provider).await?
        } else {
            Client::connect_auth(&service_url, config, self.auth_provider).await?
        };
        Ok(PulsarClient { inner })
    }
}

/// Builder for a producer.
#[derive(Debug)]
pub struct ProducerBuilder<'a> {
    client: &'a PulsarClient,
    req: CreateProducerRequest,
    encryptor: Option<std::sync::Arc<dyn magnetar_runtime_tokio::MessageEncryptor>>,
}

impl<'a> ProducerBuilder<'a> {
    fn new(client: &'a PulsarClient, topic: String) -> Self {
        let req = CreateProducerRequest {
            topic,
            ..CreateProducerRequest::default()
        };
        Self {
            client,
            req,
            encryptor: None,
        }
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

    /// Advertise the given Pulsar schema on `CommandProducer.schema`. The broker stores it
    /// and surfaces it on the dashboard; magnetar does not enforce serialisation on its own
    /// — pair with a [`crate::TypedProducer`] for that.
    #[must_use]
    pub fn schema(mut self, schema: pb::Schema) -> Self {
        self.req.schema = Some(schema);
        self
    }

    /// Mirrors Java `ProducerBuilder#initialSequenceId`. The producer's first publish gets
    /// the supplied sequence id; the next one gets `id + 1`, and so on. Useful for resuming
    /// at-least-once delivery from a checkpoint (where the caller knows the last sequence
    /// id the broker acknowledged for this producer name).
    #[must_use]
    pub fn initial_sequence_id(mut self, id: u64) -> Self {
        self.req.initial_sequence_id = Some(id);
        self
    }

    /// Mirrors Java `ProducerBuilder#accessMode`. Defaults to `Shared`; switch to
    /// `Exclusive` for single-writer-per-topic patterns, `WaitForExclusive` to queue
    /// behind the current writer, or `ExclusiveWithFencing` to evict it.
    #[must_use]
    pub fn access_mode(mut self, mode: pb::ProducerAccessMode) -> Self {
        self.req.access_mode = mode;
        self
    }

    /// Mirrors Java `ProducerBuilder#property`. Appends a `(key, value)` entry to the
    /// producer metadata advertised on `CommandProducer.metadata`. Visible on the broker
    /// dashboard alongside the producer.
    #[must_use]
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.req.producer_metadata.push((key.into(), value.into()));
        self
    }

    /// Mirrors Java `ProducerBuilder#sendTimeout`. When set, in-flight sends past
    /// `enqueued_at + timeout` resolve with a synthetic `SendError` carrying
    /// `code=-1, message="send timeout"` on the next state-machine tick.
    #[must_use]
    pub fn send_timeout(mut self, timeout: Duration) -> Self {
        self.req.send_timeout = Some(timeout);
        self
    }

    /// Configure PIP-4 end-to-end encryption. The encryptor is consulted on every
    /// `send()` to wrap the (post-compression) payload. Pass an
    /// [`Arc`](std::sync::Arc) of e.g. `magnetar::MessageCryptoBridge` from the
    /// `encryption` feature.
    #[must_use]
    pub fn encryption(
        mut self,
        encryptor: std::sync::Arc<dyn magnetar_runtime_tokio::MessageEncryptor>,
    ) -> Self {
        self.encryptor = Some(encryptor);
        self
    }

    /// Open the producer.
    pub async fn create(self) -> Result<magnetar_runtime_tokio::Producer> {
        Ok(self
            .client
            .inner
            .open_producer_with(self.req, self.encryptor)
            .await?)
    }
}

/// Builder for a consumer.
#[derive(Debug)]
pub struct ConsumerBuilder<'a> {
    client: &'a PulsarClient,
    req: SubscribeRequest,
    decryptor: Option<std::sync::Arc<dyn magnetar_runtime_tokio::MessageDecryptor>>,
}

impl<'a> ConsumerBuilder<'a> {
    fn new(client: &'a PulsarClient, topic: String) -> Self {
        let req = SubscribeRequest {
            topic,
            ..SubscribeRequest::default()
        };
        Self {
            client,
            req,
            decryptor: None,
        }
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

    /// Choose between a durable subscription (cursor persisted broker-side, the default)
    /// and a non-durable one (used by [`Reader`] / streaming use cases).
    #[must_use]
    pub fn durable(mut self, durable: bool) -> Self {
        self.req.durable = durable;
        self
    }

    /// Set the initial position the broker dispatches from when the subscription is new.
    #[must_use]
    pub fn initial_position(mut self, position: pb::command_subscribe::InitialPosition) -> Self {
        self.req.initial_position = position;
        self
    }

    /// Configure PIP-4 end-to-end decryption. The decryptor is consulted on every received
    /// message whose `MessageMetadata.encryption_keys` is non-empty.
    #[must_use]
    pub fn encryption(
        mut self,
        decryptor: std::sync::Arc<dyn magnetar_runtime_tokio::MessageDecryptor>,
    ) -> Self {
        self.decryptor = Some(decryptor);
        self
    }

    /// Read from the compacted (key-deduplicated) view of the topic. Required by
    /// [`crate::TableView`] and by any "latest-value-per-key" workflow against compacted topics.
    #[must_use]
    pub fn read_compacted(mut self, on: bool) -> Self {
        self.req.read_compacted = on;
        self
    }

    /// Advertise the given Pulsar schema on `CommandSubscribe.schema`. The broker uses it
    /// for schema-version negotiation; magnetar does not enforce deserialisation on its own
    /// — pair with a [`crate::TypedConsumer`] for that.
    #[must_use]
    pub fn schema(mut self, schema: pb::Schema) -> Self {
        self.req.schema = Some(schema);
        self
    }

    /// Mirrors Java `ConsumerBuilder#priorityLevel`. The broker uses the value for Shared
    /// / Failover dispatch ordering — higher-priority consumers receive messages first.
    #[must_use]
    pub fn priority_level(mut self, level: i32) -> Self {
        self.req.priority_level = Some(level);
        self
    }

    /// Append a (key, value) entry to the subscription properties advertised on
    /// `CommandSubscribe.subscription_properties`. Mirrors Java
    /// `ConsumerBuilder#subscriptionProperties` (one entry at a time).
    #[must_use]
    pub fn subscription_property(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.req
            .subscription_properties
            .push((key.into(), value.into()));
        self
    }

    /// Mirrors Java `ConsumerBuilder#keySharedPolicy`. Only meaningful when
    /// [`Self::subscription_type`] is `Key_Shared`. The broker rejects the subscribe if
    /// the config is invalid (e.g. overlapping sticky ranges across consumers in the same
    /// subscription).
    #[must_use]
    pub fn key_shared_policy(mut self, cfg: magnetar_proto::KeySharedConfig) -> Self {
        self.req.key_shared = Some(cfg);
        self
    }

    /// Mirrors Java `ConsumerBuilder#startMessageId`. Overrides the initial position with a
    /// specific message id. Only honoured for fresh subscriptions — has no effect if the
    /// subscription already has a persisted cursor.
    #[must_use]
    pub fn start_message_id(mut self, id: magnetar_proto::MessageId) -> Self {
        self.req.start_message_id = Some(id);
        self
    }

    /// Mirrors Java `ConsumerBuilder#replicateSubscriptionState`. When `true`, the broker
    /// replicates this subscription's cursor across geo-replicated clusters.
    #[must_use]
    pub fn replicate_subscription_state(mut self, on: bool) -> Self {
        self.req.replicate_subscription_state = Some(on);
        self
    }

    /// Mirrors Java `ConsumerBuilder#enableTopicCreation`. When `false`, the broker fails
    /// the subscribe if the topic doesn't already exist. Defaults to the broker default
    /// (which is `true`).
    #[must_use]
    pub fn force_topic_creation(mut self, on: bool) -> Self {
        self.req.force_topic_creation = Some(on);
        self
    }

    /// Mirrors Java's `startMessageRollbackDuration` knob — rolls the subscription cursor
    /// back by `seconds` at subscribe time so the consumer re-reads recent history. Useful
    /// for "catch up on the last hour" patterns.
    #[must_use]
    pub fn start_message_rollback_duration(mut self, seconds: u64) -> Self {
        self.req.start_message_rollback_duration_sec = Some(seconds);
        self
    }

    /// Mirrors Java `ConsumerBuilder#property`. Appends a `(key, value)` entry to the
    /// consumer metadata advertised on `CommandSubscribe.metadata`. Visible on the broker
    /// dashboard alongside the consumer.
    #[must_use]
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.req.consumer_metadata.push((key.into(), value.into()));
        self
    }

    /// Mirrors Java `ConsumerBuilder#negativeAckRedeliveryDelay`. When set, the consumer
    /// keeps nacked ids locally and defers the redelivery command until the delay has
    /// elapsed. The state machine drives the timer on its existing keepalive tick.
    #[must_use]
    pub fn negative_ack_redelivery_delay(mut self, delay: Duration) -> Self {
        self.req.negative_ack_redelivery_delay = Some(delay);
        self
    }

    /// Mirrors Java `ConsumerBuilder#ackTimeout`. The consumer client-tracks every
    /// delivered message and forces a redelivery if no positive ack arrives within
    /// `timeout`. The state machine drives the tracker on its existing tick.
    #[must_use]
    pub fn ack_timeout(mut self, timeout: Duration) -> Self {
        self.req.ack_timeout = Some(timeout);
        self
    }

    /// Mirrors Java `ConsumerBuilder#deadLetterPolicy`. After `max_redeliver_count`
    /// redeliveries, the consumer flags the message as dead-letter — drain via
    /// [`magnetar_runtime_tokio::Consumer::drain_dead_letter`] and republish to
    /// `dead_letter_topic` (or to the Java-default `<topic>-<subscription>-DLQ` when
    /// `dead_letter_topic` is `None`).
    ///
    /// `0` disables DLQ routing (the default).
    #[must_use]
    pub fn dead_letter_policy(
        mut self,
        max_redeliver_count: u32,
        dead_letter_topic: Option<String>,
    ) -> Self {
        self.req.max_redeliver_count = max_redeliver_count;
        self.req.dead_letter_topic = dead_letter_topic;
        self
    }

    /// Subscribe.
    pub async fn subscribe(self) -> Result<magnetar_runtime_tokio::Consumer> {
        Ok(self
            .client
            .inner
            .subscribe_with(self.req, self.decryptor)
            .await?)
    }
}

/// Builder for a [`Reader`].
///
/// Mirrors `org.apache.pulsar.client.api.ReaderBuilder`. Internally a `Reader` is just a
/// non-durable `Exclusive` consumer with an auto-generated subscription name — there's no
/// dedicated wire command, so the protocol layer doesn't need any extra plumbing.
#[derive(Debug)]
pub struct ReaderBuilder<'a> {
    inner: ConsumerBuilder<'a>,
}

impl<'a> ReaderBuilder<'a> {
    fn new(client: &'a PulsarClient, topic: String) -> Self {
        let subscription = format!("reader-{}", uuid::Uuid::new_v4().simple());
        let inner = ConsumerBuilder::new(client, topic)
            .subscription(subscription)
            .subscription_type(pb::command_subscribe::SubType::Exclusive)
            .durable(false);
        Self { inner }
    }

    /// Override the auto-generated subscription name. Rarely needed — Reader subscriptions
    /// are not visible on the broker dashboard anyway.
    #[must_use]
    pub fn subscription_name(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.subscription(name);
        self
    }

    /// Set the receiver queue size.
    #[must_use]
    pub fn receiver_queue_size(mut self, size: usize) -> Self {
        self.inner = self.inner.receiver_queue_size(size);
        self
    }

    /// Set the consumer name advertised to the broker.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.name(name);
        self
    }

    /// Choose where the reader starts when its non-durable subscription is fresh.
    /// Defaults to [`pb::command_subscribe::InitialPosition::Latest`].
    #[must_use]
    pub fn start_position(mut self, position: pb::command_subscribe::InitialPosition) -> Self {
        self.inner = self.inner.initial_position(position);
        self
    }

    /// Read from the compacted (key-deduplicated) view of the topic. Mirrors Java
    /// `ReaderBuilder#readCompacted`. Required for compacted-topic readers.
    #[must_use]
    pub fn read_compacted(mut self, on: bool) -> Self {
        self.inner = self.inner.read_compacted(on);
        self
    }

    /// Override the initial message id the reader starts from. Mirrors Java
    /// `ReaderBuilder#startMessageId`. Pass [`magnetar_proto::MessageId::EARLIEST`] /
    /// [`magnetar_proto::MessageId::LATEST`] for the sentinel positions.
    #[must_use]
    pub fn start_message_id(mut self, id: magnetar_proto::MessageId) -> Self {
        self.inner = self.inner.start_message_id(id);
        self
    }

    /// Mirrors Java `ReaderBuilder#cryptoKeyReader` — supplies a PIP-4 decryptor for the
    /// reader's underlying subscription.
    #[must_use]
    pub fn encryption(
        mut self,
        decryptor: std::sync::Arc<dyn magnetar_runtime_tokio::MessageDecryptor>,
    ) -> Self {
        self.inner = self.inner.encryption(decryptor);
        self
    }

    /// Mirrors `ConsumerBuilder::property`. The reader's underlying consumer carries the
    /// (key, value) pair on its `CommandSubscribe.metadata`.
    #[must_use]
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.property(key, value);
        self
    }

    /// Roll the reader cursor back by `seconds` at create time. Mirrors Java
    /// `ReaderBuilder#startMessageIdInclusive` rollback knob.
    #[must_use]
    pub fn start_message_rollback_duration(mut self, seconds: u64) -> Self {
        self.inner = self.inner.start_message_rollback_duration(seconds);
        self
    }

    /// Create the reader.
    pub async fn create(self) -> Result<Reader> {
        let consumer = self.inner.subscribe().await?;
        Ok(Reader {
            consumer,
            last_received: parking_lot::Mutex::new(None),
        })
    }
}

/// Reader handle — a non-durable consumer that reads from a topic without persisting an
/// acknowledgement cursor. Use a reader for: log replay, message inspection, batch ETL, or
/// anywhere you want at-most-once delivery semantics that the broker doesn't track.
#[derive(Debug)]
pub struct Reader {
    consumer: magnetar_runtime_tokio::Consumer,
    /// Last message id returned via [`Self::read_next_tracked`] / [`Self::read_next`].
    /// Used by [`Self::has_message_available`] to ask the broker "is there anything past
    /// what I last handed you?" without the caller having to track the cursor.
    last_received: parking_lot::Mutex<Option<magnetar_proto::MessageId>>,
}

impl Reader {
    /// Block until the next message arrives. Identical to Java `Reader#readNext`.
    /// Internally also stamps the returned id into the per-reader cursor so a subsequent
    /// [`Self::has_message_available`] call asks the broker the right question.
    pub async fn read_next(&self) -> Result<magnetar_proto::IncomingMessage, PulsarError> {
        let msg = self.consumer.receive().await.map_err(PulsarError::Client)?;
        *self.last_received.lock() = Some(msg.message_id);
        Ok(msg)
    }

    /// Returns the raw [`magnetar_runtime_tokio::ReceiveFut`] without per-reader cursor
    /// tracking. Use this when integrating with a custom select loop where you want
    /// cancel-safe receive futures; pair with [`Self::record_received`] if you still want
    /// `has_message_available` to work.
    pub fn read_next_fut(&self) -> magnetar_runtime_tokio::ReceiveFut {
        self.consumer.receive()
    }

    /// Manually record a received message id into the per-reader cursor. Useful when
    /// callers go through [`Self::read_next_fut`] / `.consumer().receive()` directly and
    /// still want [`Self::has_message_available`] to behave correctly.
    pub fn record_received(&self, message_id: magnetar_proto::MessageId) {
        *self.last_received.lock() = Some(message_id);
    }

    /// `true` if the broker has at least one message strictly past the most-recently
    /// returned message id. Mirrors Java `Reader#hasMessageAvailable` (no argument —
    /// the reader tracks its own cursor). Returns `true` for fresh readers (no
    /// `read_next` yet) if the broker reports any non-empty topic.
    pub async fn has_message_available(&self) -> Result<bool, PulsarError> {
        let cursor = *self.last_received.lock();
        if let Some(c) = cursor {
            return self
                .consumer
                .has_message_after(c)
                .await
                .map_err(PulsarError::Client);
        }
        let last = self
            .consumer
            .last_message_id()
            .await
            .map_err(PulsarError::Client)?;
        Ok(last != magnetar_proto::MessageId::EARLIEST)
    }

    /// Borrow the underlying consumer (for advanced operations like `flow()`).
    #[must_use]
    pub fn consumer(&self) -> &magnetar_runtime_tokio::Consumer {
        &self.consumer
    }

    /// Topic this reader is bound to. Mirrors Java `Reader#getTopic`.
    #[must_use]
    pub fn topic(&self) -> String {
        self.consumer.topic()
    }

    /// Auto-generated subscription name behind this reader. Mirrors Java
    /// `Reader#getSubscriptionName`.
    #[must_use]
    pub fn subscription(&self) -> String {
        self.consumer.subscription()
    }

    /// Ask the broker for the topic's last-published message id. Mirrors Java
    /// `Reader#getLastMessageId`.
    pub async fn last_message_id(&self) -> Result<magnetar_proto::MessageId, PulsarError> {
        self.consumer
            .last_message_id()
            .await
            .map_err(PulsarError::Client)
    }

    /// `true` if the broker has at least one message strictly past the supplied cursor.
    /// Mirrors Java `Reader#hasMessageAvailable` (the Reader form takes no cursor; pass
    /// the last id you received).
    pub async fn has_message_after(
        &self,
        cursor: magnetar_proto::MessageId,
    ) -> Result<bool, PulsarError> {
        self.consumer
            .has_message_after(cursor)
            .await
            .map_err(PulsarError::Client)
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
}
