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

use crate::pb;
use crate::txn::{TxnError, TxnId, TxnState};
use crate::types::{ConsumerHandle, MessageId, ProducerHandle, RequestId, SequenceId};

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
        challenge: Option<Vec<u8>>,
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
        schema_version: Vec<u8>,
    },

    /// A subscribe request was acknowledged by the broker.
    SubscribeAcked {
        /// The consumer handle this event refers to.
        handle: ConsumerHandle,
    },

    /// An incoming message was delivered by the broker.
    Message {
        /// The consumer that received it.
        handle: ConsumerHandle,
        /// The decoded message.
        message: IncomingMessage,
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
    /// Decoded metadata for the *batch* (the producer's metadata, not the single's).
    pub metadata: pb::MessageMetadata,
    /// Optional single-message metadata if the message was part of a batch.
    pub single_metadata: Option<pb::SingleMessageMetadata>,
    /// The payload bytes (post-decompression by the consumer driver; the state machine itself
    /// surfaces raw bytes — decompression happens above us because the codec lives in the
    /// runtime crate to avoid pulling compression algorithm crates into `magnetar-proto`).
    pub payload: Bytes,
    /// Broker-supplied redelivery count.
    pub redelivery_count: u32,
    /// Optional broker-entry metadata (PIP-90).
    pub broker_entry_metadata: Option<pb::BrokerEntryMetadata>,
}
