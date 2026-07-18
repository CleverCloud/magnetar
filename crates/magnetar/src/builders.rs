// SPDX-License-Identifier: Apache-2.0

//! Per-surface builders for [`crate::PulsarClient`] — extracted from
//! `client.rs` so the central façade module stays focused on the
//! `PulsarClient` surface, message types, interceptor traits, and the
//! `Reader` impl. The builders here are
//! [`ProducerBuilder`] / [`ConsumerBuilder`] / [`ReaderBuilder`],
//! each carrying a phantom `E: Engine` parameter (default
//! [`crate::TokioEngine`]) so `PulsarClient<E>::producer(...)` /
//! `consumer(...)` / `reader(...)` dispatch through the
//! engine-generic factory traits.
//!
//! **Engine-genericity.** The encryptor
//! / decryptor storage is engine-typed via the per-engine
//! [`crate::MessageEncryptorApi`] / [`crate::MessageDecryptorApi`]
//! extension traits: tokio plugs in
//! `Arc<dyn magnetar_runtime_tokio::MessageEncryptor>` and moonpool
//! plugs in `Arc<dyn magnetar_runtime_moonpool::MessageEncryptor>`
//! (both engines now ship the PIP-4 bridge). The chainable
//! surface stays engine-agnostic — the `E: Engine` parameter only
//! surfaces in the terminal `.create()` / `.subscribe()` dispatch
//! through [`crate::CreateProducerApi`] / [`crate::SubscribeApi`], and
//! in the per-engine `.create_with_encryption()` /
//! `.subscribe_with_decryption()` specialisations.
//!
//! All three builders are re-exported from `magnetar::*` via the
//! façade `lib.rs` so existing call sites keep working unchanged.

use std::time::Duration;

use magnetar_proto::conn::{CreateProducerRequest, SubscribeRequest};
use magnetar_proto::pb;
use magnetar_proto::types::CompressionKind;

use crate::client::{PulsarClient, PulsarError, Reader};

/// Result alias used inside this module, mirroring the one in
/// `client.rs`.
type Result<T, E = PulsarError> = std::result::Result<T, E>;

/// Builder for a producer.
///
/// Phantom-generic over `E: Engine` per ADR-0026 §D1 — type
/// parameter present (defaulting to [`crate::TokioEngine`]). Same lift
/// pattern as [`ConsumerBuilder`]; the inherent impl methods stay
/// tokio-bound until the [`crate::CreateProducerApi`] dispatch
/// path lands (foundation traits added in commit `cc61d4d`).
pub struct ProducerBuilder<'a, E: crate::Engine = crate::TokioEngine> {
    client: &'a PulsarClient<E>,
    req: CreateProducerRequest,
    /// Engine-typed encryptor slot. Tokio resolves
    /// `<TokioEngine as MessageEncryptorApi>::Encryptor` to
    /// `Arc<dyn magnetar_runtime_tokio::MessageEncryptor>`; moonpool
    /// resolves it to `Arc<dyn magnetar_runtime_moonpool::MessageEncryptor>`.
    /// The generic `.create()` path **rejects** a configured encryptor — only
    /// the per-engine `.create_with_encryption()` specialisations actually
    /// open a PIP-4-encrypting producer.
    ///
    /// `MessageEncryptorApi` is a supertrait of [`crate::Engine`], so the
    /// resolution is automatic — no extra bound needed at the use site.
    encryptor: Option<<E as crate::MessageEncryptorApi>::Encryptor>,
}

impl<E: crate::Engine> std::fmt::Debug for ProducerBuilder<'_, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerBuilder")
            .field("topic", &self.req.topic)
            .field("producer_name", &self.req.producer_name)
            .finish_non_exhaustive()
    }
}

impl<'a, E: crate::Engine> ProducerBuilder<'a, E> {
    pub(crate) fn new(client: &'a PulsarClient<E>, topic: String) -> Self {
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

    /// Mirrors Java `ProducerBuilder#sendTimeout`. In-flight sends past
    /// `enqueued_at + timeout` resolve with a synthetic `SendError` carrying
    /// `code=-1, message="send timeout"` on the next state-machine tick.
    ///
    /// The default is **30 s** (Java parity — `sendTimeoutMs = 30000`, ADR-0072),
    /// so a send whose receipt is lost or corrupted in flight fails deterministically
    /// rather than hanging forever. Call [`Self::disable_send_timeout`] for the
    /// unbounded (never-times-out) behavior.
    #[must_use]
    pub fn send_timeout(mut self, timeout: Duration) -> Self {
        self.req.send_timeout = Some(timeout);
        self
    }

    /// Disable the send timeout: in-flight sends never resolve with a synthetic
    /// timeout `SendError` — they wait indefinitely for the broker's receipt
    /// (or a session-loss / terminal error). Mirrors Java
    /// `ProducerBuilder#sendTimeout(0, …)`. Overrides the 30 s default (ADR-0072).
    #[must_use]
    pub fn disable_send_timeout(mut self) -> Self {
        self.req.send_timeout = None;
        self
    }

    /// Mirrors Java `ProducerBuilder#batchingMaxPublishDelay`. With batching enabled,
    /// the state machine flushes any non-empty batch whose oldest message has been waiting
    /// longer than this duration. Caps end-to-end latency for batched sends that would
    /// otherwise sit until the batch fills up.
    #[must_use]
    pub fn batching_max_publish_delay(mut self, delay: Duration) -> Self {
        self.req.batching_max_publish_delay = Some(delay);
        self
    }

    /// Open the producer via the engine-generic
    /// [`crate::CreateProducerApi`] trait. Returns the engine's
    /// concrete `Producer` type.
    ///
    /// **PIP-4 encryption guardrail (BREAKING since the encryptor-storage lift).**
    /// If [`Self::encryption`] was called on the per-engine specialisation,
    /// `.create()` returns [`PulsarError::Other`] instead of silently opening
    /// a plaintext producer. The engine-generic dispatch does not know how to
    /// thread an engine-typed encryptor through `open_producer`, so the
    /// previous "silently drop the encryptor" behaviour was a footgun.
    /// Use [`Self::create_with_encryption`] on the tokio /
    /// moonpool specialisation instead.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] if an encryptor was configured via [`Self::encryption`] — call
    ///   `create_with_encryption()` instead.
    /// - [`PulsarError::Other`] (stringified) on broker rejection or wire failure.
    pub async fn create(
        self,
    ) -> Result<<E::ClientState as crate::CreateProducerApi>::Producer, PulsarError>
    where
        E::ClientState: crate::BrokerMetadataApi + crate::CreateProducerApi,
    {
        if self.encryptor.is_some() {
            return Err(PulsarError::Other(
                "ProducerBuilder::create() refuses a configured encryptor — \
                 use create_with_encryption() on the engine-specific builder \
                 (PIP-4 encryptors are engine-typed and cannot dispatch \
                 through the engine-generic CreateProducerApi)"
                    .to_owned(),
            ));
        }
        // Pre-check the partition metadata: if the topic happens to be
        // partitioned, opening a bare-topic producer surfaces as the broker's
        // `NotAllowedError(22) "Found partitioned metadata for non-partitioned
        // topic"`. Catch that here and surface an actionable error pointing at
        // `client.partitioned_producer(...)`, which fans out into one child
        // producer per partition (the Java client's
        // `PulsarClientImpl#createProducerAsync` does the same thing
        // transparently — magnetar makes the routing explicit per ADR-0051).
        //
        // The per-partition fast path in `partitioned_topic_metadata`
        // short-circuits `<base>-partition-<N>` suffixes to `N == 0` without
        // a broker round-trip; for other topic names this is one
        // `CommandPartitionedTopicMetadata` round-trip, the same cost the
        // Java client pays.
        let topic = self.req.topic.clone();
        let mut deadline =
            crate::CreateProducerApi::new_producer_operation_deadline(&self.client.inner);
        let partitions = crate::BrokerMetadataApi::partitioned_topic_metadata_with_deadline(
            &self.client.inner,
            &topic,
            &mut deadline,
        )
        .await
        .map_err(|err| PulsarError::Other(format!("partitioned_topic_metadata: {err}")))?;
        if partitions > 0 {
            return Err(PulsarError::Other(format!(
                "topic `{topic}` is partitioned (broker reports {partitions} partitions); \
                 call `client.partitioned_producer(\"{topic}\").create()` instead — \
                 a bare `client.producer(t).create()` would round-trip to a broker \
                 NotAllowedError(22) \"Found partitioned metadata for non-partitioned topic\"."
            )));
        }
        crate::CreateProducerApi::open_producer_with_deadline(
            &self.client.inner,
            self.req,
            &mut deadline,
        )
        .await
        .map_err(|err| PulsarError::Other(format!("open_producer: {err}")))
    }
}

/// Tokio-engine-specific `ProducerBuilder` methods that depend on the
/// tokio `MessageEncryptor` extension. The moonpool equivalent lives in
/// the `#[cfg(feature = "moonpool")]` block below (ADR-0044).
impl ProducerBuilder<'_, crate::TokioEngine> {
    /// Set the PIP-4 encryptor. The encryptor is consulted on every
    /// `send()` to wrap the (post-compression) payload.
    #[must_use]
    pub fn encryption(
        mut self,
        encryptor: std::sync::Arc<dyn magnetar_runtime_tokio::MessageEncryptor>,
    ) -> Self {
        // `<TokioEngine as MessageEncryptorApi>::Encryptor` resolves
        // exactly to `Arc<dyn MessageEncryptor>` so we store the arg
        // directly into the engine-typed slot.
        self.encryptor = Some(encryptor);
        self
    }

    /// Open the producer honoring the configured encryptor (PIP-4).
    /// Tokio-engine-only — use [`Self::create`] for the engine-generic
    /// path that ignores the encryptor.
    ///
    /// # Errors
    /// - [`PulsarError::Client`] on broker rejection or wire failure.
    pub async fn create_with_encryption(self) -> Result<magnetar_runtime_tokio::Producer> {
        Ok(self
            .client
            .inner
            .open_producer_with(self.req, self.encryptor)
            .await?)
    }
}

/// Moonpool-engine-specific `ProducerBuilder` methods that depend on the
/// moonpool `MessageEncryptor` extension (PIP-4). 1:1 mirror of the tokio
/// specialisation above — the moonpool runtime now ships the same encryption
/// hook surface, so the façade exposes the same `.encryption()` +
/// `.create_with_encryption()` chain for the moonpool engine.
#[cfg(feature = "moonpool")]
impl<P: moonpool_core::Providers + Send + Sync + 'static>
    ProducerBuilder<'_, crate::MoonpoolEngine<P>>
{
    /// Set the PIP-4 encryptor. The encryptor is consulted on every
    /// `send()` to wrap the (post-compression) payload.
    #[must_use]
    pub fn encryption(
        mut self,
        encryptor: std::sync::Arc<dyn magnetar_runtime_moonpool::MessageEncryptor>,
    ) -> Self {
        // `<MoonpoolEngine<P> as MessageEncryptorApi>::Encryptor` resolves
        // exactly to `Arc<dyn MessageEncryptor>` so we store the arg
        // directly into the engine-typed slot.
        self.encryptor = Some(encryptor);
        self
    }

    /// Open the producer honoring the configured encryptor (PIP-4).
    /// Moonpool-engine-only — use [`Self::create`] for the engine-generic
    /// path that ignores the encryptor.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] (stringified) on broker rejection or wire failure.
    pub async fn create_with_encryption(self) -> Result<magnetar_runtime_moonpool::Producer<P>> {
        self.client
            .inner
            .open_producer_with(self.req, self.encryptor)
            .await
            .map_err(|err| PulsarError::Other(format!("open_producer: {err}")))
    }
}

/// Builder for a consumer.
///
/// Engine-generic over `E: Engine` per ADR-0026 §D1 (default
/// [`crate::TokioEngine`]). The base `subscribe()` dispatches through
/// the [`crate::SubscribeApi`] extension trait implemented by both
/// runtimes' `Client`; the per-engine PIP-4 decryption knobs live on the
/// engine-specialised `impl ConsumerBuilder<TokioEngine>` /
/// `#[cfg(feature = "moonpool")]` blocks (ADR-0044).
pub struct ConsumerBuilder<'a, E: crate::Engine = crate::TokioEngine> {
    client: &'a PulsarClient<E>,
    req: SubscribeRequest,
    /// Engine-typed decryptor slot. See
    /// [`ProducerBuilder`] for the analogous tokio /
    /// moonpool split; same per-engine
    /// [`crate::MessageDecryptorApi`] resolution (supertrait of
    /// [`crate::Engine`], so no extra bound needed at the use site).
    ///
    /// The generic `.subscribe()` path **rejects** a configured decryptor —
    /// only the per-engine `.subscribe_with_decryption()` specialisations
    /// actually open a PIP-4-decrypting consumer.
    decryptor: Option<<E as crate::MessageDecryptorApi>::Decryptor>,
    /// Optional push-delivery callback (Java `ConsumerBuilder#messageListener`).
    /// Engine-agnostic — the façade [`crate::MessageListener`] takes the façade
    /// [`crate::IncomingMessage`], which both engines produce. Set it via
    /// [`Self::message_listener`] and subscribe via
    /// [`Self::subscribe_with_listener`]; the plain [`Self::subscribe`] ignores
    /// it and returns a pull-mode consumer.
    listener: Option<crate::MessageListener>,
}

impl<E: crate::Engine> std::fmt::Debug for ConsumerBuilder<'_, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerBuilder")
            .field("topic", &self.req.topic)
            .field("subscription", &self.req.subscription)
            .finish_non_exhaustive()
    }
}

impl<'a, E: crate::Engine> ConsumerBuilder<'a, E> {
    pub(crate) fn new(client: &'a PulsarClient<E>, topic: String) -> Self {
        let req = SubscribeRequest {
            topic,
            ..SubscribeRequest::default()
        };
        Self {
            client,
            req,
            decryptor: None,
            listener: None,
        }
    }

    /// Read-only snapshot of the [`SubscribeRequest`] this builder has assembled
    /// so far. Test-support seam (`#[doc(hidden)]`, not part of the stable API):
    /// lets the builder-surface guard test assert that each of the five consumer
    /// builders seeds the bounded-chunk-reassembly knobs into the request that
    /// seeds `ConsumerState`, without opening a real broker connection.
    #[doc(hidden)]
    #[must_use]
    pub fn request_snapshot(&self) -> &SubscribeRequest {
        &self.req
    }

    /// Test-support seam (`#[doc(hidden)]`, not part of the stable API):
    /// `true` once a push-delivery listener has been set via
    /// [`Self::message_listener`]. Lets the builder-surface guard test pin the
    /// listener → field wiring without opening a real broker connection.
    #[doc(hidden)]
    #[must_use]
    pub fn has_listener_for_test(&self) -> bool {
        self.listener.is_some()
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

    /// Set the receiver queue size (the fixed permit count handed to the broker).
    ///
    /// This is sugar for [`Self::receiver_queue_policy`]`(`[`magnetar_proto::Fixed`]`(size))`:
    /// it pins the queue to a constant size, the historical (and default)
    /// behaviour. To let the queue self-tune under load, use
    /// [`Self::receiver_queue_policy`] with [`magnetar_proto::Auto`] instead.
    #[must_use]
    pub fn receiver_queue_size(mut self, size: usize) -> Self {
        self.req.receiver_queue_size = size;
        // Clearing any previously-set policy keeps `receiver_queue_size` and
        // `receiver_queue_policy` from disagreeing: the last setter wins.
        self.req.receiver_queue_policy = None;
        self.req.receiver_queue_adjust_interval = None;
        self
    }

    /// Set a pluggable receiver-queue-size policy (issue #301, PIP-74
    /// `autoScaledReceiverQueueSizeEnabled` parity).
    ///
    /// Pass [`magnetar_proto::Auto`] (wrapped in `Arc`) to let the queue grow
    /// under starvation and shrink under memory pressure, or any custom
    /// [`magnetar_proto::ReceiverQueuePolicy`]. The policy's `adjust` is called
    /// from the connection's deterministic timeout tick, so it MUST be pure (no
    /// clock, no RNG, no I/O) — see the trait docs.
    ///
    /// Enabling a policy turns on auto-adjust with a default 5-second tick;
    /// override the cadence with [`Self::receiver_queue_adjust_interval`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use magnetar_proto::Auto;
    /// # fn demo<'a>(b: magnetar::ConsumerBuilder<'a>) -> magnetar::ConsumerBuilder<'a> {
    /// b.receiver_queue_policy(Arc::new(Auto::new(1_000, 128 * 1024 * 1024)))
    /// # }
    /// ```
    #[must_use]
    pub fn receiver_queue_policy(
        mut self,
        policy: std::sync::Arc<dyn magnetar_proto::ReceiverQueuePolicy>,
    ) -> Self {
        self.req.receiver_queue_policy = Some(policy);
        // Default auto-adjust cadence; tunable via `receiver_queue_adjust_interval`.
        if self.req.receiver_queue_adjust_interval.is_none() {
            self.req.receiver_queue_adjust_interval = Some(Duration::from_secs(5));
        }
        self
    }

    /// Override the auto-adjust tick cadence used when a
    /// [`Self::receiver_queue_policy`] is set. No effect with the default
    /// [`magnetar_proto::Fixed`] policy. Mirrors how often Java re-evaluates its
    /// auto-scaled receiver queue.
    #[must_use]
    pub fn receiver_queue_adjust_interval(mut self, interval: Duration) -> Self {
        self.req.receiver_queue_adjust_interval = Some(interval);
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

    /// Mirrors Java `ConsumerBuilder#ackTimeoutRedeliveryBackoff`. PIP-37 backoff applied to
    /// the per-message ack-timeout deadline using the broker-reported `redelivery_count`.
    /// Has no effect unless [`Self::ack_timeout`] is also set.
    #[must_use]
    pub fn ack_timeout_backoff(
        mut self,
        backoff: magnetar_proto::trackers::MultiplierRedeliveryBackoff,
    ) -> Self {
        self.req.ack_timeout_backoff = Some(backoff);
        self
    }

    /// Mirrors Java `ConsumerBuilder#acknowledgmentGroupTime`. When set, calls to
    /// [`magnetar_runtime_tokio::Consumer::ack_grouped`] (and
    /// `ack_grouped_cumulative`) stage acks in an in-memory tracker and the state
    /// machine flushes them as one coalesced `CommandAck` every `window`. Trades
    /// broker-confirmation guarantees for lower ack bandwidth on high-throughput
    /// consumers. Has no effect on the synchronous [`Self::ack_timeout`] or the
    /// awaited `Consumer::ack` paths.
    #[must_use]
    pub fn ack_group_time(mut self, window: Duration) -> Self {
        self.req.ack_group_time = Some(window);
        self
    }

    /// Mirrors Java `ConsumerBuilder#cryptoFailureAction`. Controls what the consumer does
    /// when payload decryption fails (PIP-4): `Fail` (default) propagates the error,
    /// `Discard` silently drops the message, `Consume` returns the encrypted ciphertext
    /// as-is. All three arms are honored by the
    /// [`magnetar_runtime_tokio::Consumer`] receive path.
    #[must_use]
    pub fn crypto_failure_action(
        mut self,
        action: magnetar_proto::conn::CryptoFailureAction,
    ) -> Self {
        self.req.crypto_failure_action = action;
        self
    }

    /// Mirrors Java `ConsumerBuilder#maxPendingChunkedMessage` (default `10`).
    /// Bounds the number of distinct incomplete chunked messages the consumer
    /// buffers; on breach the oldest incomplete message is evicted. `0` disables
    /// the cap. Guards against a hostile/buggy broker streaming distinct-UUID
    /// first chunks that never complete (unbounded `chunk_reassembly` growth →
    /// OOM).
    #[must_use]
    pub fn max_pending_chunked_message(mut self, max: usize) -> Self {
        self.req.max_pending_chunked_message = max;
        self
    }

    /// Mirrors Java `ConsumerBuilder#autoAckOldestChunkedMessageOnQueueFull`
    /// (default `false`). When `true`, an evicted/expired partial chunked
    /// message's first-chunk id is acked before drop (the broker treats it as
    /// consumed); when `false`, it is dropped without acking so the broker
    /// redelivers the whole message.
    #[must_use]
    pub fn auto_ack_oldest_chunked_message_on_queue_full(mut self, auto_ack: bool) -> Self {
        self.req.auto_ack_oldest_chunked_message_on_queue_full = auto_ack;
        self
    }

    /// Mirrors Java `ConsumerBuilder#expireTimeOfIncompleteChunkedMessage`
    /// (default `60s`). An incomplete chunked message older than this is swept
    /// on the connection's existing timeout tick and dropped (or acked, per
    /// [`Self::auto_ack_oldest_chunked_message_on_queue_full`]).
    #[must_use]
    pub fn expire_time_of_incomplete_chunked_message(mut self, expire: Duration) -> Self {
        self.req.expire_time_of_incomplete_chunked_message = Some(expire);
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

    /// Subscribe via the engine-generic [`crate::SubscribeApi`] trait.
    /// Returns the engine's concrete `Consumer` type.
    ///
    /// **PIP-4 decryption guardrail (BREAKING since the decryptor-storage lift).**
    /// If [`Self::encryption`] was called on the per-engine specialisation,
    /// `.subscribe()` returns [`PulsarError::Other`] instead of silently opening
    /// a plaintext consumer. The engine-generic dispatch cannot thread an
    /// engine-typed decryptor through `subscribe`, so the previous "silently
    /// drop the decryptor" behaviour was a footgun. Use
    /// [`Self::subscribe_with_decryption`] on the tokio / moonpool
    /// specialisation instead.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] if a decryptor was configured via [`Self::encryption`] — call
    ///   `subscribe_with_decryption()` instead.
    /// - [`PulsarError::Other`] (stringified) on broker rejection or wire failure.
    pub async fn subscribe(
        self,
    ) -> Result<<E::ClientState as crate::SubscribeApi>::Consumer, PulsarError>
    where
        E::ClientState: crate::SubscribeApi,
    {
        let mut deadline =
            crate::SubscribeApi::new_subscribe_operation_deadline(&self.client.inner);
        self.subscribe_with_deadline(&mut deadline).await
    }

    pub(crate) async fn subscribe_with_deadline(
        self,
        deadline: &mut crate::OperationDeadline,
    ) -> Result<<E::ClientState as crate::SubscribeApi>::Consumer, PulsarError>
    where
        E::ClientState: crate::SubscribeApi,
    {
        if self.decryptor.is_some() {
            return Err(PulsarError::Other(
                "ConsumerBuilder::subscribe() refuses a configured decryptor — \
                 use subscribe_with_decryption() on the engine-specific builder \
                 (PIP-4 decryptors are engine-typed and cannot dispatch \
                 through the engine-generic SubscribeApi)"
                    .to_owned(),
            ));
        }
        crate::SubscribeApi::subscribe_with_deadline(&self.client.inner, self.req, deadline)
            .await
            .map_err(|err| PulsarError::Other(format!("subscribe: {err}")))
    }

    /// Register a push-delivery callback (Java
    /// `ConsumerBuilder#messageListener`). Once set, subscribe via
    /// [`Self::subscribe_with_listener`] to start a background poller that
    /// drives `receive()` and hands every message to `listener`, sequentially
    /// and in order.
    ///
    /// The plain [`Self::subscribe`] does **not** consult the listener — it
    /// returns a pull-mode consumer. Pull and push are mutually exclusive
    /// (Java forbids `receive()` on a listener-backed consumer): use
    /// `subscribe_with_listener` for push and never call `receive()`, or use
    /// `subscribe` for pull and call `receive()` yourself.
    ///
    /// The callback **must ack explicitly** — the poller never auto-acks (Java
    /// parity). Hold a clone of your consumer in the closure to ack.
    #[must_use]
    pub fn message_listener(mut self, listener: crate::MessageListener) -> Self {
        self.listener = Some(listener);
        self
    }

    /// Subscribe and start a push-delivery poller over the resulting consumer,
    /// returning the owning [`crate::MessageListenerHandle`]. Mirrors Java's
    /// `ConsumerBuilder#messageListener(...)` + `subscribe()` flow.
    ///
    /// The poller delivers messages sequentially and in order, does **not**
    /// auto-ack (the callback acks explicitly), and stops cleanly when the
    /// consumer is closed or the returned handle is dropped. Because the
    /// consumer is moved into the poller, there is no handle left to call
    /// `receive()` on — the listener owns delivery (matching Java's
    /// "no `receive()` with a `messageListener`" rule).
    ///
    /// # Errors
    /// - [`PulsarError::Config`] if no listener was set via [`Self::message_listener`].
    /// - [`PulsarError::Other`] (stringified) on broker rejection or wire failure.
    pub async fn subscribe_with_listener(self) -> Result<crate::MessageListenerHandle, PulsarError>
    where
        E::ClientState: crate::SubscribeApi,
        <E::ClientState as crate::SubscribeApi>::Consumer: Clone,
    {
        let Some(listener) = self.listener.clone() else {
            return Err(PulsarError::Config(
                "subscribe_with_listener() requires a listener — \
                 call message_listener(...) first (or use subscribe() for pull mode)"
                    .to_owned(),
            ));
        };
        let consumer = self.subscribe().await?;
        Ok(crate::consumer_listener::spawn_message_listener(
            consumer, listener,
        ))
    }
}

/// Tokio-engine-specific `ConsumerBuilder` methods that depend on the
/// tokio `MessageDecryptor` extension. The moonpool equivalent lives in
/// the `#[cfg(feature = "moonpool")]` block below (ADR-0044).
impl ConsumerBuilder<'_, crate::TokioEngine> {
    /// Configure PIP-4 end-to-end decryption. The decryptor is consulted on every received
    /// message whose `MessageMetadata.encryption_keys` is non-empty.
    #[must_use]
    pub fn encryption(
        mut self,
        decryptor: std::sync::Arc<dyn magnetar_runtime_tokio::MessageDecryptor>,
    ) -> Self {
        // `<TokioEngine as MessageDecryptorApi>::Decryptor` resolves
        // exactly to `Arc<dyn MessageDecryptor>` so we store the arg
        // directly into the engine-typed slot.
        self.decryptor = Some(decryptor);
        self
    }

    /// Subscribe with the configured decryptor (PIP-4). Tokio-engine-only.
    /// Use [`Self::subscribe`] for the engine-generic path that ignores
    /// the decryptor.
    ///
    /// # Errors
    /// - [`PulsarError::Client`] on broker rejection or wire failure.
    pub async fn subscribe_with_decryption(self) -> Result<magnetar_runtime_tokio::Consumer> {
        Ok(self
            .client
            .inner
            .subscribe_with(self.req, self.decryptor)
            .await?)
    }
}

/// Moonpool-engine-specific `ConsumerBuilder` methods that depend on the
/// moonpool `MessageDecryptor` extension (PIP-4). 1:1 mirror of the tokio
/// specialisation above.
#[cfg(feature = "moonpool")]
impl<P: moonpool_core::Providers + Send + Sync + 'static>
    ConsumerBuilder<'_, crate::MoonpoolEngine<P>>
{
    /// Configure PIP-4 end-to-end decryption. The decryptor is consulted on every received
    /// message whose `MessageMetadata.encryption_keys` is non-empty.
    #[must_use]
    pub fn encryption(
        mut self,
        decryptor: std::sync::Arc<dyn magnetar_runtime_moonpool::MessageDecryptor>,
    ) -> Self {
        // `<MoonpoolEngine<P> as MessageDecryptorApi>::Decryptor` resolves
        // exactly to `Arc<dyn MessageDecryptor>` so we store the arg
        // directly into the engine-typed slot.
        self.decryptor = Some(decryptor);
        self
    }

    /// Subscribe with the configured decryptor (PIP-4). Moonpool-engine-only.
    /// Use [`Self::subscribe`] for the engine-generic path that ignores
    /// the decryptor.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] (stringified) on broker rejection or wire failure.
    pub async fn subscribe_with_decryption(self) -> Result<magnetar_runtime_moonpool::Consumer<P>> {
        self.client
            .inner
            .subscribe_with(self.req, self.decryptor)
            .await
            .map_err(|err| PulsarError::Other(format!("subscribe: {err}")))
    }
}

/// Builder for a [`Reader`].
///
/// Mirrors `org.apache.pulsar.client.api.ReaderBuilder`. Internally a `Reader` is just a
/// non-durable `Exclusive` consumer with an auto-generated subscription name — there's no
/// dedicated wire command, so the protocol layer doesn't need any extra plumbing.
///
/// Phantom-generic over `E: Engine` (defaults to [`crate::TokioEngine`]).
/// Wraps a [`ConsumerBuilder<E>`]; the impl methods stay tokio-bound
/// until the `SubscribeApi` dispatch path lands in the Builder lift
/// sub-PR.
pub struct ReaderBuilder<'a, E: crate::Engine = crate::TokioEngine> {
    inner: ConsumerBuilder<'a, E>,
}

impl<E: crate::Engine> std::fmt::Debug for ReaderBuilder<'_, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReaderBuilder")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<'a, E: crate::Engine> ReaderBuilder<'a, E> {
    pub(crate) fn new(client: &'a PulsarClient<E>, topic: String) -> Self {
        let subscription = format!("reader-{}", E::random_subscription_suffix());
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

    /// Create the reader via the engine-generic
    /// [`crate::SubscribeApi`] dispatch path. Returns
    /// `Reader<<E::ClientState as SubscribeApi>::Consumer>` —
    /// resolves to `Reader<magnetar_runtime_tokio::Consumer>` (the
    /// default `Reader<>` alias) under the default
    /// `E = TokioEngine`.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] on broker rejection or wire failure.
    pub async fn create(
        self,
    ) -> Result<Reader<<E::ClientState as crate::SubscribeApi>::Consumer>, PulsarError>
    where
        E::ClientState: crate::SubscribeApi,
    {
        let consumer = self.inner.subscribe().await?;
        Ok(Reader {
            consumer,
            last_received: parking_lot::Mutex::new(None),
        })
    }
}

/// Tokio-engine-specific `ReaderBuilder` methods that depend on the
/// tokio `MessageDecryptor` extension (PIP-4).
impl ReaderBuilder<'_, crate::TokioEngine> {
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
}
