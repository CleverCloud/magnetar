// SPDX-License-Identifier: Apache-2.0

//! Events emitted by the [`Connection`](crate::Connection) state machine.
//!
//! The state machine never performs I/O; it produces frames into an outbound queue and surfaces
//! semantic events to the driver via [`Connection::poll_event`](crate::Connection::poll_event).
//! Driver code (`magnetar-runtime-tokio`, `magnetar-runtime-moonpool`) translates these events
//! into user-facing futures by waking the registered `Waker`s.
//!
//! Why this enum and not `tokio::sync::*`: per [GUIDELINES.md#no-channels-rule], the sans-io core
//! must not depend on a runtime. Driver-to-future dispatch happens via `Waker` slabs keyed by
//! ids embedded in these events.

use bytes::Bytes;

use crate::markers::ReplicatedSubscriptionMarker;
use crate::pb;
use crate::txn::{TxnError, TxnId, TxnState};
use crate::types::{ConsumerHandle, MessageId, ProducerHandle, RequestId, SequenceId};

/// Outcome of a `CommandGetSchema` round-trip (PIP-87 broker-side schema lookup).
///
/// `Ok((schema, version))` on success — the broker-resolved [`pb::Schema`] and the optional
/// schema version assigned by the registry. `Err((code, message))` carries the wire-protocol
/// `ServerError` code and broker-supplied message on failure (e.g. `TopicNotFound`).
pub type GetSchemaResult = Result<(pb::Schema, Option<Bytes>), (i32, String)>;

/// Result variants of one round-trip against the Transaction Coordinator.
///
/// Mirrors PIP-31's response set. The `Result` shape carries either the
/// expected payload (e.g. a fresh [`TxnId`]) or the broker's [`TxnError`].
#[derive(Debug, Clone)]
pub enum TxnRoundTrip {
    /// `CommandNewTxnResponse` — broker minted a new transaction.
    NewTxn(Result<TxnId, TxnError>),
    /// `CommandAddPartitionToTxnResponse` — partition registered with the txn.
    AddPartition(Result<(), TxnError>),
    /// `CommandAddSubscriptionToTxnResponse` — subscription registered with the txn.
    AddSubscription(Result<(), TxnError>),
    /// `CommandEndTxnResponse` — txn committed or aborted; carries the final state.
    EndTxn(Result<TxnState, TxnError>),
}

/// A semantic event surfaced by the state machine.
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    /// Handshake completed. The driver should now allow producer/consumer opens.
    Connected {
        /// Protocol version negotiated with the broker.
        protocol_version: i32,
        /// Maximum message size declared by the broker (0 if unset).
        max_message_size: u32,
        /// Capabilities negotiated with the broker.
        feature_flags: pb::FeatureFlags,
    },

    /// The broker sent a `CommandAuthChallenge` mid-connection.
    ///
    /// The auth layer (above `magnetar-proto`) is expected to compute the response and feed
    /// it back via [`Connection::submit_auth_response`](crate::Connection::submit_auth_response).
    AuthChallenge {
        /// Auth method requested by the broker, if it differs from the original.
        method: Option<String>,
        /// Server-supplied challenge data (opaque to the protocol layer).
        challenge: Option<Bytes>,
    },

    /// A producer that was queued via `create_producer` is now ready to send.
    ProducerReady {
        /// The producer handle this event refers to.
        handle: ProducerHandle,
        /// Producer name assigned by the broker (server-side if user did not specify one).
        producer_name: String,
        /// Last sequence id seen by the broker for this producer (`-1` if none).
        last_sequence_id: i64,
        /// Schema version assigned by the broker (empty if none).
        schema_version: Bytes,
    },

    /// The broker rejected a `CommandProducer` open with `CommandError`.
    ///
    /// Emitted from the `CommandError` handler when the failing request id correlates with a
    /// pending producer-open. The corresponding producer state has already been dropped from
    /// the connection — the user-facing handle is dead. Pair with
    /// [`ConnectionEvent::ProducerReady`] as the success/failure split for an `open_producer`
    /// round-trip.
    ProducerOpenFailed {
        /// The producer that failed to open.
        handle: ProducerHandle,
        /// Pulsar wire-protocol `ServerError` code (`pb::ServerError`).
        code: i32,
        /// Human-readable error from the broker.
        message: String,
    },

    /// The broker rejected a `CommandProducer` with a TRANSIENT error code (e.g.
    /// `ServiceNotReady`, `MetadataError`, `TopicNotFound`) — typically a
    /// post-restart broker whose namespace bundle hasn't been re-acquired yet.
    /// Unlike [`Self::ProducerOpenFailed`], the producer state is NOT dropped: the
    /// runtime is expected to back off and call
    /// [`crate::Connection::retry_producer_open`] to retry the attach. Mirrors
    /// Java `ProducerImpl.handleProducerCreationError` retrying on the same
    /// codes.
    ProducerOpenFailedTransient {
        /// The producer that failed to open.
        handle: ProducerHandle,
        /// Pulsar wire-protocol `ServerError` code (`pb::ServerError`).
        code: i32,
        /// Human-readable error from the broker.
        message: String,
    },

    /// A subscribe request was acknowledged by the broker.
    SubscribeAcked {
        /// The consumer handle this event refers to.
        handle: ConsumerHandle,
    },

    /// The broker rejected a `CommandSubscribe` with `CommandError`.
    ///
    /// Emitted from the `CommandError` handler when the failing request id correlates with a
    /// pending subscribe. The corresponding consumer state has already been dropped from the
    /// connection. Pair with [`ConnectionEvent::SubscribeAcked`] as the success/failure split
    /// for a `subscribe` round-trip.
    SubscribeFailed {
        /// The consumer that failed to subscribe.
        handle: ConsumerHandle,
        /// Pulsar wire-protocol `ServerError` code (`pb::ServerError`).
        code: i32,
        /// Human-readable error from the broker.
        message: String,
    },

    /// Consumer-side companion to [`Self::ProducerOpenFailedTransient`]. The
    /// broker rejected `CommandSubscribe` with a transient code; the consumer
    /// state is retained and the runtime should retry via
    /// [`crate::Connection::retry_consumer_subscribe`] after a backoff.
    SubscribeFailedTransient {
        /// The consumer that failed to subscribe.
        handle: ConsumerHandle,
        /// Pulsar wire-protocol `ServerError` code (`pb::ServerError`).
        code: i32,
        /// Human-readable error from the broker.
        message: String,
    },

    /// An incoming message was delivered by the broker.
    Message {
        /// The consumer that received it.
        handle: ConsumerHandle,
        /// The decoded message.
        message: IncomingMessage,
    },

    /// PIP-180 / ADR-0033: an incoming message was delivered by the broker on a
    /// shadow topic, originating from a source topic. Emitted in place of
    /// [`Self::Message`] when the consumer was subscribed to a shadow topic
    /// (resolved at subscribe time via the admin REST `getShadowTopics(source)`
    /// hint, see [`crate::consumer::ConsumerState::set_shadow_metadata`]) AND
    /// the inbound entry's [`pb::MessageMetadata::replicated_from`] is set.
    ///
    /// `source_message_id` and `message.message_id` compare equal under the
    /// PIP-180 structural-equality contract documented on
    /// [`crate::types::MessageId`] — the broker presents shadow-side entries
    /// with the source-topic `(ledger_id, entry_id, batch_index, partition)`,
    /// so cross-side deduplication needs no out-of-band correlation key.
    ///
    /// Callers that don't care about the shadow context can collapse this
    /// variant onto [`Self::Message`] by inspecting `message`. The variant
    /// is non-breaking by convention (`ConnectionEvent` is treated as
    /// `#[non_exhaustive]` per ADR-0033's "new sum-variant is additive" risk
    /// note).
    MessageReceivedFromShadow {
        /// The consumer that received it.
        handle: ConsumerHandle,
        /// Source-topic name (resolved via admin REST `getShadowTopics(source)`
        /// at subscribe time, cached on
        /// [`crate::consumer::ConsumerState::shadow_metadata`]).
        source_topic: String,
        /// Source-topic `MessageId`. Equal to `message.message_id` under
        /// [`crate::types::MessageId`]'s structural-equality contract.
        source_message_id: MessageId,
        /// Shadow-side `MessageId` — same fields as `source_message_id`, but
        /// surfaced separately so callers don't have to derive it from
        /// `message`.
        shadow_message_id: MessageId,
        /// The decoded message — same payload + metadata the consumer would
        /// have surfaced via [`Self::Message`] on a non-shadow topic.
        message: IncomingMessage,
    },

    /// PIP-33: the broker emitted a `REPLICATED_SUBSCRIPTION_*` marker on this
    /// consumer's topic. Surfaced for observability only — the marker is filtered
    /// off the user-visible message stream (never appears as [`Self::Message`])
    /// because it carries broker-side snapshot/update payload, not application
    /// data. Magnetar never originates these markers; the broker generates them
    /// when the namespace has `replicated_subscription_status=true` and a peer
    /// cluster's snapshot/update cycle fires. See
    /// [`crate::markers`] for the payload typing and
    /// [ADR-0034](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0034-pip-33-replicated-subscriptions-scope.md)
    /// for scope.
    ReplicatedSubscriptionMarkerObserved {
        /// The consumer that received the marker.
        handle: ConsumerHandle,
        /// Decoded marker payload (kind + details).
        marker: ReplicatedSubscriptionMarker,
    },

    /// A `CommandSendReceipt` correlated with one of our pending publishes.
    SendReceipt {
        /// The producer that owns the publish.
        handle: ProducerHandle,
        /// Publisher-side sequence id of the receipt.
        sequence_id: SequenceId,
        /// Broker-assigned message id.
        message_id: MessageId,
    },

    /// A `CommandSendError` correlated with one of our pending publishes.
    SendError {
        /// The producer that owns the publish.
        handle: ProducerHandle,
        /// Publisher-side sequence id of the failed publish.
        sequence_id: SequenceId,
        /// Pulsar wire-protocol `ServerError` code.
        code: i32,
        /// Human-readable error from the broker.
        message: String,
    },

    /// Response to a `CommandAck` request.
    AckResponse {
        /// Request id of the originating `CommandAck` (when set; the broker may omit it).
        request_id: Option<RequestId>,
        /// `Ok(())` on success, `Err(error)` with the broker message on failure.
        result: Result<(), String>,
    },

    /// Response to a `CommandLookupTopic` request.
    LookupResponse {
        /// Request id of the originating `CommandLookupTopic`.
        request_id: RequestId,
        /// Resolved broker URL on success, `None` on failure or redirect.
        result: LookupOutcome,
    },

    /// Response to a `CommandPartitionedTopicMetadata` request.
    PartitionedMetadataResponse {
        /// Request id of the originating `CommandPartitionedTopicMetadata`.
        request_id: RequestId,
        /// Number of partitions (0 = non-partitioned topic).
        partitions: u32,
        /// Pulsar wire-protocol `ServerError` if the request failed.
        error: Option<(i32, String)>,
    },

    /// Topic list watcher initial snapshot.
    TopicListSnapshot {
        /// Request id of the originating `CommandWatchTopicList`.
        request_id: RequestId,
        /// Initial list of topics matching the pattern.
        topics: Vec<String>,
    },

    /// Topic list watcher delta (PIP-145).
    TopicListChanged {
        /// Topics that newly match the pattern.
        added: Vec<String>,
        /// Topics that no longer match the pattern.
        removed: Vec<String>,
    },

    /// The broker signalled end-of-topic on a non-durable subscription.
    ReachedEndOfTopic {
        /// The consumer that reached end-of-topic.
        handle: ConsumerHandle,
    },

    /// A consumer's active/passive state changed (failover).
    ActiveConsumerChanged {
        /// The consumer whose active state changed.
        handle: ConsumerHandle,
        /// `true` if the consumer became active, `false` if it became passive.
        active: bool,
    },

    /// Broker requested the producer or consumer to migrate to a different broker URL.
    TopicMigrated {
        /// Producer handle if the resource type was `Producer`.
        producer: Option<ProducerHandle>,
        /// Consumer handle if the resource type was `Consumer`.
        consumer: Option<ConsumerHandle>,
        /// New plaintext broker service URL.
        broker_service_url: Option<String>,
        /// New TLS broker service URL.
        broker_service_url_tls: Option<String>,
    },

    /// The broker asked us to close a producer (e.g. fenced).
    ProducerClosedByBroker {
        /// The producer that was closed.
        handle: ProducerHandle,
        /// Optional re-target URL hinted by the broker.
        assigned_broker_service_url: Option<String>,
    },

    /// The broker asked us to close a consumer.
    ConsumerClosedByBroker {
        /// The consumer that was closed.
        handle: ConsumerHandle,
        /// Optional re-target URL hinted by the broker.
        assigned_broker_service_url: Option<String>,
    },

    /// A CRC32C checksum mismatch was detected on an inbound payload frame.
    ///
    /// Per [GUIDELINES.md] §"Protocol-correctness invariants", the frame is dropped (never
    /// delivered) and the event is surfaced for diagnostics.
    ChecksumMismatch {
        /// Computed CRC32C.
        computed: u32,
        /// Expected CRC32C from the wire.
        expected: u32,
    },

    /// The connection is closing (locally initiated or peer-triggered).
    Closed {
        /// Optional close reason for diagnostics.
        reason: Option<String>,
    },

    /// A Transaction Coordinator round-trip completed.
    ///
    /// Carries the outcome for one of: `new_txn`, `add_partition_to_txn`,
    /// `add_subscription_to_txn`, `end_txn`.
    TxnResponse {
        /// Request id correlating the response to the originating request.
        request_id: RequestId,
        /// The transactional outcome.
        outcome: TxnRoundTrip,
    },

    /// Response to a `CommandGetSchema` request (PIP-87 broker-side schema lookup).
    ///
    /// Emitted after the runtime calls
    /// [`Connection::get_schema`](crate::Connection::get_schema) and the broker replies. The
    /// payload is a [`GetSchemaResult`] — `Ok` carries the registry-resolved [`pb::Schema`] and
    /// schema version, `Err` carries the broker's `ServerError` code and message.
    GetSchemaResponse {
        /// Request id correlating the response to the originating `CommandGetSchema`.
        request_id: RequestId,
        /// The schema-registry round-trip outcome.
        result: GetSchemaResult,
    },

    /// The connection-level anti-thrash detector (ADR-0028) has engaged. The
    /// supervisor must sleep until `until` before its next
    /// `Transport::connect`, even if its per-handle backoff would have
    /// retried sooner. Emitted exactly once on each `Normal → Cooldown`
    /// transition by
    /// [`Connection::record_reattach_outcome`](crate::Connection::record_reattach_outcome).
    AntiThrashCooldown {
        /// Absolute `Instant` the cooldown expires. Compare to the engine's
        /// `Instant::now()` to compute the remaining sleep.
        until: std::time::Instant,
    },

    /// The connection-level anti-thrash cooldown (ADR-0028) has lifted —
    /// either because the supervisor slept past `until` and explicitly
    /// cleared it via
    /// [`Connection::anti_thrash_state_mut`](crate::Connection::anti_thrash_state_mut),
    /// or because the broker stabilised and the driver called
    /// [`Connection::record_first_op_success`](crate::Connection::record_first_op_success).
    AntiThrashCleared,
}

/// Outcome of a `CommandLookupTopic` round-trip.
#[derive(Debug, Clone)]
pub enum LookupOutcome {
    /// Topic resolved to the given broker URL.
    Connect {
        /// Plaintext broker service URL.
        broker_service_url: Option<String>,
        /// TLS broker service URL.
        broker_service_url_tls: Option<String>,
        /// Whether to honour `proxy_through_service_url`.
        proxy_through_service_url: bool,
    },
    /// Broker redirected the lookup (the state machine has already re-emitted the lookup with
    /// `authoritative=true`; this variant is surfaced for observability only).
    Redirected {
        /// New broker URL to retry the lookup against.
        broker_service_url: Option<String>,
        /// New TLS broker URL.
        broker_service_url_tls: Option<String>,
    },
    /// Lookup failed.
    Failed {
        /// Pulsar wire-protocol `ServerError`.
        code: i32,
        /// Broker-supplied error string.
        message: String,
    },
}

/// A message delivered to a consumer (after batch explosion and chunk reassembly).
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    /// Broker-assigned message id (with `batch_index` filled for batched messages).
    pub message_id: MessageId,
    /// Decoded metadata for the *batch* (the producer's metadata, not the
    /// single's). Wrapped in [`std::sync::Arc`] so the batched-delivery loop in
    /// `ConsumerState::deliver` can hand every sub-message a refcount of
    /// the same parsed metadata instead of `clone()`-ing it N times per
    /// batch (a 100-message batch was 100 metadata deep-clones; with the
    /// `Arc` it is 100 refcount bumps).
    pub metadata: std::sync::Arc<pb::MessageMetadata>,
    /// Optional single-message metadata if the message was part of a batch.
    pub single_metadata: Option<pb::SingleMessageMetadata>,
    /// The payload bytes (post-decompression by the consumer driver; the state machine itself
    /// surfaces raw bytes — decompression happens above us because the codec lives in the
    /// runtime crate to avoid pulling compression algorithm crates into `magnetar-proto`).
    pub payload: Bytes,
    /// Broker-supplied redelivery count.
    pub redelivery_count: u32,
    /// Optional broker-entry metadata (PIP-90). Refcounted for the same
    /// batched-delivery-loop reason as `metadata`.
    pub broker_entry_metadata: Option<std::sync::Arc<pb::BrokerEntryMetadata>>,
    /// Wall-clock instant at which the consumer state machine first saw this message (i.e. the
    /// moment `ConsumerState::deliver` queued it). The consumer uses
    /// `pop_message`-time `Instant::now() - arrived_at` to feed its
    /// `receive_latency_hist`, mirroring Java `ConsumerStatsRecorder` p50/p99/max.
    pub arrived_at: std::time::Instant,
}
