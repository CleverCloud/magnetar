// SPDX-License-Identifier: Apache-2.0

//! The central `Connection` sans-io state machine.
//!
//! Public surface mirrors `quinn-proto::Connection`:
//!
//! - [`Connection::handle_bytes`] takes inbound bytes and updates internal state.
//! - [`Connection::poll_transmit`] drains queued outbound bytes.
//! - [`Connection::poll_event`] yields semantic [`ConnectionEvent`]s.
//! - [`Connection::poll_timeout`] / [`Connection::handle_timeout`] drive keepalives + trackers.
//!
//! On top of that, a handle-based façade lets callers (the runtime crate) open producers /
//! consumers, send, ack, seek, look up, etc. — all without I/O.
//!
//! Waker registration uses a small slab keyed by `op_id` per
//! [GUIDELINES.md] §"No-channels rule" — no `tokio::sync::*`, no `crossbeam`, no `flume`.
//!
//! # References
//!
//! - `ClientCnx.java:117` (channel constants and request id seed)
//! - `ClientCnx.java:132-158` (constructor wiring)
//! - `ClientCnx.java:432` (handleConnected)
//! - `ClientCnx.java:464` (handleAuthChallenge)
//! - `ClientCnx.java:515` (request dispatch)
//! - `HandlerState.java` (handshake states)

use core::time::Duration;
use std::collections::{HashMap, VecDeque};
use std::task::Waker;
use std::time::{Instant, SystemTime};

use bytes::{Buf, Bytes, BytesMut};

use crate::consumer::ConsumerState;
use crate::error::ProtocolError;
use crate::event::{ConnectionEvent, IncomingMessage, LookupOutcome, TxnRoundTrip};
use crate::frame::{Frame, decode_one, encode_command, encode_payload};
use crate::lookup::{LookupRegistry, LookupRequest, PartitionedMetadataRequest};
use crate::pb;
use crate::producer::{ProducerState, SendDecision};
use crate::topic_watcher::{TopicWatcher, TopicWatcherRegistry};
use crate::txn::{TxnAction, TxnClient, TxnError, TxnId, TxnState};
use crate::types::{
    CompressionKind, ConsumerHandle, MessageId, ProducerHandle, RequestId, SequenceId,
};

/// Handshake state — modelled after `HandlerState`.
///
/// The state diagram is:
/// `Uninitialized → ConnectSent → Connected ⇄ AuthChallenging → Closing → Closed | Failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeState {
    /// Constructed but no bytes sent yet.
    Uninitialized,
    /// Local sent `CommandConnect`, waiting for `CommandConnected`.
    ConnectSent,
    /// Handshake done; producers/consumers can be created.
    Connected,
    /// Mid-connection re-auth (PIP-30/292). Returns to `Connected` after `CommandAuthResponse`.
    AuthChallenging,
    /// Local-initiated close; waiting for the driver to flush.
    Closing,
    /// Closed cleanly.
    Closed,
    /// Failed (handshake error or peer-initiated abort).
    Failed,
}

/// Identifier for a pending operation. Used by [`Connection::register_waker`] /
/// [`Connection::take_outcome`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PendingOpKey {
    /// A pending request keyed by request id (lookup, seek, ack-response, etc.).
    Request(RequestId),
    /// A pending publish keyed by `(producer_id, sequence_id)`.
    Send(ProducerHandle, SequenceId),
}

/// Connection configuration.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Client version string advertised in `CommandConnect`.
    pub client_version: String,
    /// Authentication method name (e.g. `"none"`, `"token"`).
    pub auth_method_name: String,
    /// Initial auth data (when an auth provider already has a token).
    pub auth_data: Option<Vec<u8>>,
    /// Protocol version to advertise; `21` covers Pulsar 4.x.
    pub protocol_version: i32,
    /// Capabilities to advertise on connect.
    pub feature_flags: pb::FeatureFlags,
    /// Keepalive (ping) interval. Default `30 s`.
    pub keepalive_interval: Duration,
    /// Operation timeout (e.g. lookup + send). Default `30 s`.
    pub operation_timeout: Duration,
    /// Default compression for producers (overridable per producer).
    pub default_compression: CompressionKind,
    /// Default max-message-size if the broker omits it. Pulsar default = 5 MiB.
    pub default_max_message_size: usize,
    /// Optional proxy-to-broker URL for the binary proxy path.
    pub proxy_to_broker_url: Option<String>,
    /// Optional auto-reconnect supervisor. When `Some`, runtime engines wrap the
    /// driver loop in a backoff-driven reconnect cycle that survives transport
    /// failures. `None` (the default) keeps the pre-supervisor behavior — driver
    /// exits on the first I/O error. Mirrors Java's `PulsarClientImpl` reconnect
    /// loop.
    pub supervisor: Option<crate::supervisor::SupervisorConfig>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            client_version: format!("magnetar/{}", env!("CARGO_PKG_VERSION")),
            auth_method_name: "none".to_owned(),
            auth_data: None,
            protocol_version: 21,
            feature_flags: pb::FeatureFlags::default(),
            keepalive_interval: Duration::from_secs(30),
            operation_timeout: Duration::from_secs(30),
            default_compression: CompressionKind::None,
            default_max_message_size: 5 * 1024 * 1024,
            proxy_to_broker_url: None,
            supervisor: None,
        }
    }
}

/// Result of consuming a pending op via [`Connection::take_outcome`].
#[derive(Debug, Clone)]
pub enum OpOutcome {
    /// A `CommandSendReceipt` correlated with the publish.
    SendReceipt {
        /// Sequence id of the publish.
        sequence_id: SequenceId,
        /// Broker-assigned message id.
        message_id: MessageId,
    },
    /// A `CommandSendError` correlated with the publish.
    SendError {
        /// Sequence id of the publish.
        sequence_id: SequenceId,
        /// `ServerError` code.
        code: i32,
        /// Broker error message.
        message: String,
    },
    /// Generic broker success (request id matched but no payload).
    Success {
        /// Request id of the originating request.
        request_id: RequestId,
    },
    /// Generic broker error.
    Error {
        /// Request id of the originating request.
        request_id: RequestId,
        /// `ServerError` code.
        code: i32,
        /// Broker error message.
        message: String,
    },
    /// Lookup outcome.
    LookupResponse {
        /// Request id of the originating lookup.
        request_id: RequestId,
        /// The outcome of the lookup.
        outcome: LookupOutcome,
    },
    /// Partitioned-topic metadata.
    PartitionedMetadata {
        /// Request id of the originating request.
        request_id: RequestId,
        /// Number of partitions.
        partitions: u32,
        /// Optional error if the request failed.
        error: Option<(i32, String)>,
    },
    /// `CommandNewTxnResponse` correlated with a `new_txn` call.
    NewTxn {
        /// Request id of the originating request.
        request_id: RequestId,
        /// Resulting transaction id on success, or the [`TxnError`] on failure.
        result: Result<TxnId, TxnError>,
    },
    /// `CommandAddPartitionToTxnResponse` correlated with an `add_partition_to_txn` call.
    AddPartitionToTxn {
        /// Request id of the originating request.
        request_id: RequestId,
        /// `Ok(())` on success.
        result: Result<(), TxnError>,
    },
    /// `CommandAddSubscriptionToTxnResponse` correlated with an `add_subscription_to_txn` call.
    AddSubscriptionToTxn {
        /// Request id of the originating request.
        request_id: RequestId,
        /// `Ok(())` on success.
        result: Result<(), TxnError>,
    },
    /// `CommandEndTxnResponse` correlated with an `end_txn` call.
    EndTxn {
        /// Request id of the originating request.
        request_id: RequestId,
        /// Final transaction state on success.
        result: Result<TxnState, TxnError>,
    },
    /// `CommandGetLastMessageIdResponse` correlated with a `get_last_message_id` call.
    LastMessageId {
        /// Request id of the originating request.
        request_id: RequestId,
        /// Broker's view of the last published message id on the topic.
        last_message_id: MessageId,
        /// Optional consumer mark-delete position (where the broker thinks this consumer's
        /// cursor is).
        consumer_mark_delete_position: Option<MessageId>,
    },
    /// `CommandWatchTopicListSuccess` correlated with a `watch_topic_list` call —
    /// the initial snapshot for a topic-list watcher (PIP-145).
    TopicListSnapshot {
        /// Request id of the originating request.
        request_id: RequestId,
        /// Topics currently matching the watcher's namespace + pattern.
        topics: Vec<String>,
    },
    /// `CommandGetSchemaResponse` correlated with a [`Connection::get_schema`] call.
    ///
    /// Carries the schema-registry round-trip outcome: `Ok((schema, version))` on success,
    /// `Err((code, message))` on failure.
    GetSchemaResponse {
        /// Request id of the originating `CommandGetSchema`.
        request_id: RequestId,
        /// The schema-registry round-trip outcome.
        result: crate::event::GetSchemaResult,
    },
    /// Synthetic outcome surfaced to every waiter when the underlying broker
    /// connection drops and the supervisor begins a reconnect. Callers detect
    /// the lost session via the embedded `PendingOpKey` and decide whether to
    /// retry the operation against the freshly-handshaked connection. Mirrors
    /// the "session-lost" failure mode of Java
    /// `ClientCnx#handleConnectionClosed`.
    SessionLost {
        /// The original op key (request id or `(producer, sequence_id)`)
        /// whose future is being woken up with this outcome.
        key: PendingOpKey,
    },
}

/// Parameters for opening a producer.
#[derive(Debug, Clone)]
pub struct CreateProducerRequest {
    /// Topic name.
    pub topic: String,
    /// Optional producer name (broker assigns one if `None`).
    pub producer_name: Option<String>,
    /// Compression codec.
    pub compression: CompressionKind,
    /// Whether the producer wishes to enable batching.
    pub enable_batching: bool,
    /// Whether the producer wishes to enable chunking.
    pub enable_chunking: bool,
    /// Max batch size in bytes.
    pub max_batch_size_bytes: usize,
    /// Max messages per batch.
    pub max_messages_in_batch: usize,
    /// Optional schema to advertise.
    pub schema: Option<pb::Schema>,
    /// Mirrors Java `ProducerBuilder#initialSequenceId`. When `Some(n)`, the producer starts
    /// allocating sequence ids from `n` instead of `0`. Useful for at-least-once
    /// resume-on-restart from a known checkpoint.
    pub initial_sequence_id: Option<u64>,
    /// Producer access mode. Mirrors `CommandProducer.producer_access_mode`. Defaults to
    /// `Shared`; switch to `Exclusive` / `WaitForExclusive` / `ExclusiveWithFencing` for
    /// single-writer-per-topic patterns.
    pub access_mode: pb::ProducerAccessMode,
    /// Mirrors `CommandProducer.metadata` — broker-side KV metadata advertised at producer
    /// open. Surfaces on the broker dashboard alongside the producer.
    pub producer_metadata: Vec<(String, String)>,
    /// Mirrors Java `ProducerBuilder#sendTimeout`. When set, any in-flight send whose
    /// `enqueued_at + timeout` has elapsed surfaces a synthetic
    /// `SendError(code=11008, "send timeout")` on the next `Connection::handle_timeout`
    /// tick. `None` disables the sweep (the default).
    pub send_timeout: Option<Duration>,
    /// Mirrors Java `ProducerBuilder#batchingMaxPublishDelay`. When set and batching is
    /// enabled, the state machine flushes any non-empty batch whose oldest message has
    /// been waiting longer than this duration. Caps end-to-end latency for batched sends
    /// that would otherwise sit until the batch fills. `None` (the default) means the
    /// batch only flushes on size / count limits.
    pub batching_max_publish_delay: Option<Duration>,
}

impl Default for CreateProducerRequest {
    fn default() -> Self {
        Self {
            topic: String::new(),
            producer_name: None,
            compression: CompressionKind::None,
            enable_batching: false,
            enable_chunking: false,
            max_batch_size_bytes: 128 * 1024,
            max_messages_in_batch: 1000,
            schema: None,
            initial_sequence_id: None,
            access_mode: pb::ProducerAccessMode::Shared,
            producer_metadata: Vec::new(),
            send_timeout: None,
            batching_max_publish_delay: None,
        }
    }
}

/// Parameters for opening a consumer.
#[derive(Debug, Clone)]
pub struct SubscribeRequest {
    /// Topic name.
    pub topic: String,
    /// Subscription name.
    pub subscription: String,
    /// Subscription type (`Exclusive`, `Shared`, `Failover`, `Key_Shared`).
    pub sub_type: pb::command_subscribe::SubType,
    /// Receiver queue size.
    pub receiver_queue_size: usize,
    /// Initial position to read from.
    pub initial_position: pb::command_subscribe::InitialPosition,
    /// Consumer name (optional — broker assigns one).
    pub consumer_name: Option<String>,
    /// Optional schema.
    pub schema: Option<pb::Schema>,
    /// Whether the subscription is durable.
    pub durable: bool,
    /// Read from the compacted (key-deduplicated) view of the topic. Required by TableView and
    /// by any "latest-value-per-key" workflow against compacted topics. Mirrors
    /// `CommandSubscribe.read_compacted`.
    pub read_compacted: bool,
    /// Mirrors `CommandSubscribe.priority_level`. The broker uses it for Shared / Failover
    /// dispatch ordering. `None` means default (broker treats as 0).
    pub priority_level: Option<i32>,
    /// Mirrors `CommandSubscribe.subscription_properties` — per-subscription key/value
    /// metadata visible to the broker dashboard.
    pub subscription_properties: Vec<(String, String)>,
    /// Optional [`KeySharedConfig`] for `Key_Shared` subscriptions. Ignored for other
    /// subscription types.
    pub key_shared: Option<KeySharedConfig>,
    /// Optional starting message id for a fresh subscription. Mirrors Java
    /// `ReaderBuilder#startMessageId` / `ConsumerBuilder#startMessageId` and the
    /// `CommandSubscribe.start_message_id` wire field. Has no effect on a subscription
    /// that already has a persisted cursor.
    pub start_message_id: Option<MessageId>,
    /// Mirrors `CommandSubscribe.replicate_subscription_state`. When `Some(true)`, the broker
    /// replicates this subscription's cursor across geo-replicated clusters. Defaults to
    /// `None` (broker decision).
    pub replicate_subscription_state: Option<bool>,
    /// Mirrors `CommandSubscribe.force_topic_creation`. When `Some(false)` the broker fails
    /// the subscribe if the topic doesn't already exist. Defaults to `None` (broker default,
    /// which is `true`).
    pub force_topic_creation: Option<bool>,
    /// Mirrors `CommandSubscribe.start_message_rollback_duration_sec`. Rolls the subscription
    /// cursor back by N seconds at subscribe time, so the consumer re-reads recent history.
    pub start_message_rollback_duration_sec: Option<u64>,
    /// Mirrors Java `DeadLetterPolicy#maxRedeliverCount`. When a message has been redelivered
    /// more than this many times, the consumer routes it into the dead-letter queue instead
    /// of the user-facing queue. `0` disables DLQ routing.
    pub max_redeliver_count: u32,
    /// Mirrors Java `DeadLetterPolicy#deadLetterTopic`. Where the consumer republishes
    /// messages that exceeded `max_redeliver_count`. Convention if `None`:
    /// `<topic>-<subscription>-DLQ` (matches the Java client default).
    pub dead_letter_topic: Option<String>,
    /// Mirrors `CommandSubscribe.metadata` — broker-side KV metadata advertised at
    /// subscribe time. Surfaces on the broker dashboard alongside the consumer.
    pub consumer_metadata: Vec<(String, String)>,
    /// Mirrors Java `ConsumerBuilder#negativeAckRedeliveryDelay`. When `Some(d)`, nacked
    /// messages stay locally tracked for `d` before the redelivery command goes out. `None`
    /// means the redelivery is sent immediately (the default).
    pub negative_ack_redelivery_delay: Option<Duration>,
    /// Mirrors Java `ConsumerBuilder#ackTimeout`. When `Some(d)`, every delivered message
    /// is tracked client-side; if no positive ack arrives within `d`, the consumer forces a
    /// redelivery. `None` disables the tracker (the default).
    pub ack_timeout: Option<Duration>,
    /// Mirrors Java `ConsumerBuilder#ackTimeoutRedeliveryBackoff`. PIP-37: when set together
    /// with [`Self::ack_timeout`], the ack-timeout deadline for each delivered message is
    /// computed via
    /// [`crate::trackers::nack::MultiplierRedeliveryBackoff::delay_for`] using the
    /// broker-reported `redelivery_count` on the incoming message. `None` keeps the flat
    /// `ack_timeout` window.
    pub ack_timeout_backoff: Option<crate::trackers::nack::MultiplierRedeliveryBackoff>,
    /// Mirrors Java `ConsumerBuilder#acknowledgmentGroupTime`. When `Some(d)`, calls to
    /// the runtime `Consumer::ack_grouped` family stage acks in an in-memory tracker and
    /// flush them as a single coalesced `CommandAck` every `d`. Trades broker-confirmation
    /// guarantees for lower ack-bandwidth on high-throughput consumers. `None` keeps every
    /// ack synchronous (the default).
    pub ack_group_time: Option<Duration>,
    /// Mirrors Java `ConsumerBuilder#cryptoFailureAction`. Controls what the consumer does
    /// when payload decryption fails (PIP-4). `Fail` (default) propagates the error to the
    /// caller; `Discard` silently drops the message; `Consume` delivers the encrypted
    /// ciphertext as-is.
    pub crypto_failure_action: CryptoFailureAction,
}

/// PIP-4 decryption failure handling. Mirrors Java
/// `org.apache.pulsar.client.api.ConsumerCryptoFailureAction`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CryptoFailureAction {
    /// Surface the decryption error to the caller (default — fail-fast). Matches the
    /// pre-PIP-4 behavior.
    #[default]
    Fail,
    /// Silently drop the message and continue receiving. The caller never sees the
    /// undecryptable payload — useful when some keys are rotated out and lingering
    /// messages encrypted with retired keys should be ignored.
    Discard,
    /// Deliver the encrypted ciphertext + the `EncryptionKeys` metadata as-is to the
    /// caller, who can then attempt out-of-band decryption.
    Consume,
}

/// Mirrors Java's `KeySharedPolicy`. Configures how a `Key_Shared` subscription distributes
/// messages with the same partition key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeySharedConfig {
    /// Routing mode — broker-managed `AutoSplit` or client-pinned `Sticky`.
    pub mode: pb::KeySharedMode,
    /// For `Sticky` mode: the hash ranges this consumer claims. Ignored for `AutoSplit`.
    pub sticky_hash_ranges: Vec<(i32, i32)>,
    /// Tolerate out-of-order delivery within the same key group. Mirrors
    /// `KeySharedMeta.allow_out_of_order_delivery`.
    pub allow_out_of_order_delivery: bool,
}

impl Default for KeySharedConfig {
    fn default() -> Self {
        Self {
            mode: pb::KeySharedMode::AutoSplit,
            sticky_hash_ranges: Vec::new(),
            allow_out_of_order_delivery: false,
        }
    }
}

impl Default for SubscribeRequest {
    fn default() -> Self {
        Self {
            topic: String::new(),
            subscription: String::new(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            receiver_queue_size: 1000,
            initial_position: pb::command_subscribe::InitialPosition::Latest,
            consumer_name: None,
            schema: None,
            durable: true,
            read_compacted: false,
            priority_level: None,
            subscription_properties: Vec::new(),
            key_shared: None,
            start_message_id: None,
            replicate_subscription_state: None,
            force_topic_creation: None,
            start_message_rollback_duration_sec: None,
            max_redeliver_count: 0,
            dead_letter_topic: None,
            consumer_metadata: Vec::new(),
            negative_ack_redelivery_delay: None,
            ack_timeout: None,
            ack_timeout_backoff: None,
            ack_group_time: None,
            crypto_failure_action: CryptoFailureAction::Fail,
        }
    }
}

/// Ack request — covers both individual and cumulative semantics.
#[derive(Debug, Clone)]
pub struct AckRequest {
    /// The message ids to ack.
    pub message_ids: Vec<MessageId>,
    /// Whether this is an `Individual` or `Cumulative` ack.
    pub ack_type: pb::command_ack::AckType,
    /// Optional ack-time properties. Mirrors Java
    /// `Consumer#acknowledgeAsync(MessageId, Map<String, Long>)`. The broker stores them
    /// alongside the cursor for diagnostic / replay tooling.
    pub properties: Vec<(String, i64)>,
    /// Optional transaction id (PIP-31). When set, the ack participates in the open
    /// transaction — it only takes effect when the transaction commits. Mirrors Java
    /// `Consumer#acknowledgeAsync(MessageId, Transaction)`.
    pub txn_id: Option<crate::txn::TxnId>,
}

/// Seek target — either to a message id or to a publish-time.
#[derive(Debug, Clone)]
pub enum SeekTarget {
    /// Seek to a specific message id.
    MessageId(MessageId),
    /// Seek to a specific publish timestamp (ms since UNIX epoch).
    PublishTime(u64),
}

/// The central sans-io state machine.
pub struct Connection {
    config: ConnectionConfig,
    state: HandshakeState,
    broker_max_message_size: Option<usize>,
    broker_protocol_version: i32,
    feature_flags: pb::FeatureFlags,
    /// Outbound bytes buffer drained by [`Self::poll_transmit`].
    outbound: BytesMut,
    /// Inbound bytes buffer; framed into commands by [`Self::handle_bytes`].
    inbound: BytesMut,
    /// Event queue.
    events: VecDeque<ConnectionEvent>,
    /// Outcomes ready to be consumed by user futures.
    outcomes: HashMap<PendingOpKey, OpOutcome>,
    /// Waker slab keyed by op id.
    wakers: HashMap<PendingOpKey, Waker>,
    /// Pending requests keyed by request id, with the kind of operation that produced them.
    pending_requests: HashMap<RequestId, PendingRequestKind>,
    /// Open producers.
    producers: HashMap<ProducerHandle, ProducerState>,
    /// Open consumers.
    consumers: HashMap<ConsumerHandle, ConsumerState>,
    /// Lookup registry.
    lookup: LookupRegistry,
    /// Topic watcher registry.
    topic_watchers: TopicWatcherRegistry,
    /// Transaction-coordinator client (PIP-31). One per connection — the connection only opens
    /// transactions against the TC that lives behind it.
    txn_client: TxnClient,
    /// Next request id.
    next_request_id: u64,
    /// Next producer id.
    next_producer_id: u64,
    /// Next consumer id.
    next_consumer_id: u64,
    /// Next watcher id.
    next_watcher_id: u64,
    /// Time of last outbound or inbound traffic (for keepalive).
    last_activity: Option<Instant>,
    /// Wall-clock time of the most recent transition to [`HandshakeState::Connected`].
    /// Mirrors Java's `Producer/Consumer#getLastDisconnectedTimestamp` companion: useful
    /// for application-level health probes and reconnect diagnostics.
    last_connected_at: Option<SystemTime>,
    /// Wall-clock time of the most recent transition out of [`HandshakeState::Connected`]
    /// (to `Closing`, `Closed`, or `Failed`). Mirrors
    /// `org.apache.pulsar.client.api.Producer#getLastDisconnectedTimestamp` (millis since
    /// the UNIX epoch in Java; an [`Option<SystemTime>`] here so the caller picks its own
    /// epoch conversion).
    last_disconnected_at: Option<SystemTime>,
    /// Monotonic counter incremented each time [`Self::reset`] is called. Lets
    /// callers detect that an in-flight operation was severed by a supervisor
    /// reconnect: capture the epoch before issuing an op, then re-check after
    /// the outcome arrives. Mirrors Java's `ClientCnx#getEpoch` semantics for
    /// session-bound operations.
    session_epoch: u64,
    /// Wall-clock provider — the sans-io state machine never calls
    /// [`SystemTime::now`] directly. Engines inject a real wall clock via
    /// [`Self::with_wall_clock_provider`]; the default is
    /// `|| SystemTime::now()` so existing callers keep working. moonpool /
    /// deterministic-simulation engines plug in a virtual clock here.
    wall_clock: std::sync::Arc<dyn Fn() -> SystemTime + Send + Sync>,
}

impl core::fmt::Debug for Connection {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Connection")
            .field("state", &self.state)
            .field("producers", &self.producers.len())
            .field("consumers", &self.consumers.len())
            .field("pending_requests", &self.pending_requests.len())
            .field("events_queue", &self.events.len())
            .field("outbound_bytes", &self.outbound.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy)]
enum PendingRequestKind {
    Lookup,
    PartitionedMetadata,
    ProducerOpen { handle: ProducerHandle },
    ConsumerSubscribe { handle: ConsumerHandle },
    ConsumerSeek { handle: ConsumerHandle },
    ConsumerUnsubscribe { handle: ConsumerHandle },
    ConsumerGetLastMessageId { handle: ConsumerHandle },
    Ack { handle: ConsumerHandle },
    ProducerClose { handle: ProducerHandle },
    ConsumerClose { handle: ConsumerHandle },
    TopicWatcher { watcher_id: u64 },
    NewTxn,
    AddPartitionToTxn,
    AddSubscriptionToTxn,
    EndTxn,
    GetSchema,
}

impl Connection {
    /// Construct a fresh, unconnected sans-io `Connection`.
    pub fn new(config: ConnectionConfig) -> Self {
        Self {
            config,
            state: HandshakeState::Uninitialized,
            broker_max_message_size: None,
            broker_protocol_version: 0,
            feature_flags: pb::FeatureFlags::default(),
            outbound: BytesMut::with_capacity(4 * 1024),
            inbound: BytesMut::with_capacity(4 * 1024),
            events: VecDeque::new(),
            outcomes: HashMap::new(),
            wakers: HashMap::new(),
            pending_requests: HashMap::new(),
            producers: HashMap::new(),
            consumers: HashMap::new(),
            lookup: LookupRegistry::default(),
            topic_watchers: TopicWatcherRegistry::default(),
            txn_client: TxnClient::new(0),
            next_request_id: 0,
            next_producer_id: 0,
            next_consumer_id: 0,
            next_watcher_id: 0,
            last_activity: None,
            last_connected_at: None,
            last_disconnected_at: None,
            session_epoch: 0,
            wall_clock: std::sync::Arc::new(SystemTime::now),
        }
    }

    /// Inject an explicit wall-clock provider. Engines that drive the state
    /// machine under a virtual clock (e.g. moonpool simulation) call this
    /// once after [`Self::new`] to make every internal `SystemTime` value
    /// flow from the injected source instead of the host's wall clock.
    /// Mirrors the same pattern as `now: Instant` parameters on
    /// [`Self::handle_bytes`] / [`Self::handle_timeout`]: the state machine
    /// never reaches for a clock on its own.
    #[must_use]
    pub fn with_wall_clock_provider(
        mut self,
        provider: std::sync::Arc<dyn Fn() -> SystemTime + Send + Sync>,
    ) -> Self {
        self.wall_clock = provider;
        self
    }

    /// Replace the wall-clock provider on an existing Connection. See
    /// [`Self::with_wall_clock_provider`] for the rationale.
    pub fn set_wall_clock_provider(
        &mut self,
        provider: std::sync::Arc<dyn Fn() -> SystemTime + Send + Sync>,
    ) {
        self.wall_clock = provider;
    }

    /// Returns the current handshake state.
    pub fn state(&self) -> HandshakeState {
        self.state
    }

    /// Returns whether the connection is ready to accept producer / consumer opens.
    pub fn is_connected(&self) -> bool {
        matches!(self.state, HandshakeState::Connected)
    }

    /// `true` once the connection has entered any terminal state (`Closing`, `Closed`, or
    /// `Failed`). Mirrors Java `PulsarClient#isClosed`. Returns `false` for an active or
    /// still-handshaking connection — pair with [`Self::is_connected`] for the live test.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        matches!(
            self.state,
            HandshakeState::Closing | HandshakeState::Closed | HandshakeState::Failed
        )
    }

    /// Wall-clock time the connection last reached [`HandshakeState::Connected`], if ever.
    /// Returns `None` before the first successful handshake.
    pub fn last_connected_timestamp(&self) -> Option<SystemTime> {
        self.last_connected_at
    }

    /// Wall-clock time the connection most recently left [`HandshakeState::Connected`] (to
    /// `Closing`, `Closed`, or `Failed`), if ever. Mirrors Java's
    /// `Producer/Consumer#getLastDisconnectedTimestamp`.
    pub fn last_disconnected_timestamp(&self) -> Option<SystemTime> {
        self.last_disconnected_at
    }

    /// Mark the connection as failed (e.g. peer EOF, I/O error) and record the disconnect
    /// timestamp. Called by the runtime driver when the underlying socket dies before a
    /// graceful close has been initiated.
    pub fn mark_disconnected(&mut self) {
        if !matches!(
            self.state,
            HandshakeState::Closed | HandshakeState::Failed | HandshakeState::Closing
        ) {
            self.last_disconnected_at = Some((self.wall_clock)());
        }
        self.state = HandshakeState::Failed;
    }

    /// Monotonic session epoch — incremented each time the supervisor invokes
    /// [`Self::reset`]. Callers that need to detect whether an in-flight operation
    /// survived a reconnect snapshot this value before issuing the op and compare
    /// after the response arrives. Mirrors Java `ClientCnx#getEpoch`.
    #[must_use]
    pub fn session_epoch(&self) -> u64 {
        self.session_epoch
    }

    /// Borrow the auto-reconnect supervisor configuration, if one was set. The
    /// runtime driver reads this between disconnects to decide whether to
    /// re-handshake. Returning `None` keeps the pre-supervisor behavior (driver
    /// exits on first I/O failure).
    #[must_use]
    pub fn supervisor_config(&self) -> Option<&crate::supervisor::SupervisorConfig> {
        self.config.supervisor.as_ref()
    }

    /// Reset the state machine for a fresh handshake on a new transport. Used by the
    /// runtime supervisor between [`mark_disconnected`](Self::mark_disconnected) and the
    /// new TCP / TLS handshake.
    ///
    /// Semantics, in order:
    ///
    /// 1. Bump [`Self::session_epoch`].
    /// 2. Emit [`OpOutcome::SessionLost`] for every pending request (lookup, seek, ack, transaction
    ///    round-trip, …) and every in-flight producer publish. The corresponding user futures are
    ///    woken with that outcome.
    /// 3. Reset every producer's in-flight `OpSend` queue and batch container, and clear every
    ///    consumer's queue + pending seek + ack tracker. Producers and consumers themselves are
    ///    *not* removed — Stage 3 of the supervisor work will replay their `CommandProducer` /
    ///    `CommandSubscribe` against the new transport. Until then, callers must reopen them by
    ///    hand.
    /// 4. Clear connection-level outbound + inbound byte buffers; flush queued events.
    /// 5. Snap the state machine back to [`HandshakeState::Uninitialized`] so
    ///    [`Self::begin_handshake`] can fire again on the new socket.
    pub fn reset(&mut self) {
        self.session_epoch = self.session_epoch.wrapping_add(1);

        // (2) Fail every pending request and wake its waiter.
        let pending_request_keys: Vec<PendingOpKey> = self
            .pending_requests
            .keys()
            .copied()
            .map(PendingOpKey::Request)
            .collect();
        for key in pending_request_keys {
            self.outcomes.insert(key, OpOutcome::SessionLost { key });
            if let Some(w) = self.wakers.remove(&key) {
                w.wake();
            }
        }
        self.pending_requests.clear();

        // Fail every in-flight publish across every producer.
        let producer_handles: Vec<ProducerHandle> = self.producers.keys().copied().collect();
        for handle in producer_handles {
            if let Some(producer) = self.producers.get_mut(&handle) {
                let drained = producer.drain_pending_sends();
                for (seq, waker_opt) in drained {
                    let key = PendingOpKey::Send(handle, seq);
                    self.outcomes.insert(key, OpOutcome::SessionLost { key });
                    // Prefer the producer-stored waker (registered via
                    // ProducerState::register_waker); fall back to the connection-level
                    // slab when no producer-stored waker was set.
                    if let Some(w) = waker_opt {
                        w.wake();
                    } else if let Some(w) = self.wakers.remove(&key) {
                        w.wake();
                    }
                }
            }
        }

        // Drop any remaining (orphaned) wakers — every legitimate one was either
        // dispatched above or belongs to an op the runtime will re-register after the
        // reconnect.
        let leftover_keys: Vec<PendingOpKey> = self.wakers.keys().copied().collect();
        for key in leftover_keys {
            if let Some(w) = self.wakers.remove(&key) {
                self.outcomes.insert(key, OpOutcome::SessionLost { key });
                w.wake();
            }
        }

        // (3) Reset consumer-side per-session state. We keep the ConsumerState struct
        // itself (Stage 3 will replay CommandSubscribe), but clear anything that was
        // pinned to the now-dead session: in-flight seek, in-memory queue, ack-tracker
        // state, broker permits. The runtime layer is responsible for re-subscribing
        // and re-issuing the initial flow.
        for consumer in self.consumers.values_mut() {
            consumer.queue.clear();
            consumer.pending_seek = None;
            consumer.available_permits = 0;
            consumer.consumed_since_flow = 0;
            consumer.dead_letter_pending.clear();
            consumer.batch_ack_tracker.clear();
            if let Some(receive_waker) = consumer.receive_waker.take() {
                // Wake any in-flight receive so it observes the queue is empty and
                // re-registers on the freshly-handshaked connection.
                receive_waker.wake();
            }
        }

        // (4) Drop queued events + raw bytes. Anything not yet observed by the runtime
        // belongs to the dead session.
        self.events.clear();
        self.outbound.clear();
        self.inbound.clear();

        // Lookup / topic-watcher registries hold no Wakers themselves — their futures
        // poll via the per-request waker slab we already drained above. Clearing the
        // registries avoids replaying stale `Connect`/`Redirect` traffic on the new
        // socket.
        self.lookup = LookupRegistry::default();
        self.topic_watchers = TopicWatcherRegistry::default();

        // (5) Back to Uninitialized so begin_handshake on the freshly-handshaked socket
        // succeeds.
        self.state = HandshakeState::Uninitialized;
        self.broker_max_message_size = None;
        self.broker_protocol_version = 0;
        self.feature_flags = pb::FeatureFlags::default();
        self.last_activity = None;
    }

    /// Returns the feature flags negotiated with the broker (empty until `Connected`).
    pub fn feature_flags(&self) -> &pb::FeatureFlags {
        &self.feature_flags
    }

    /// Begin the handshake. Enqueues a `CommandConnect` for the driver to send.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::Handshake`] if the connection is not in
    /// [`HandshakeState::Uninitialized`].
    pub fn begin_handshake(&mut self) -> Result<(), ProtocolError> {
        if self.state != HandshakeState::Uninitialized {
            return Err(ProtocolError::Handshake("handshake already started"));
        }
        let connect = pb::CommandConnect {
            client_version: self.config.client_version.clone(),
            auth_method: None,
            auth_method_name: Some(self.config.auth_method_name.clone()),
            auth_data: self.config.auth_data.clone(),
            protocol_version: Some(self.config.protocol_version),
            proxy_to_broker_url: self.config.proxy_to_broker_url.clone(),
            original_principal: None,
            original_auth_data: None,
            original_auth_method: None,
            feature_flags: Some(self.config.feature_flags),
            proxy_version: None,
        };
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Connect as i32,
            connect: Some(connect),
            ..Default::default()
        };
        self.encode_command(&cmd)?;
        self.state = HandshakeState::ConnectSent;
        Ok(())
    }

    /// Feed inbound bytes to the state machine.
    pub fn handle_bytes(&mut self, now: Instant, bytes: &[u8]) -> Result<(), ProtocolError> {
        self.last_activity = Some(now);
        self.inbound.extend_from_slice(bytes);
        loop {
            // We must work with `Bytes` because `decode_one` advances it; we clone the buffer
            // contents into a `Bytes` so the cursor advances inside the cloned cursor; on
            // success we then truncate `self.inbound` by the same amount.
            let mut snapshot = Bytes::copy_from_slice(&self.inbound);
            let before = snapshot.len();
            match decode_one(&mut snapshot) {
                Ok(frame) => {
                    let consumed = before - snapshot.len();
                    self.inbound.advance(consumed);
                    self.handle_frame(now, frame)?;
                }
                Err(crate::frame::FrameError::Incomplete { .. }) => return Ok(()),
                Err(crate::frame::FrameError::ChecksumMismatch { computed, expected }) => {
                    let consumed = before - snapshot.len();
                    self.inbound.advance(consumed);
                    self.events
                        .push_back(ConnectionEvent::ChecksumMismatch { computed, expected });
                    // continue decoding next frames after dropping the corrupt one
                }
                Err(other) => return Err(other.into()),
            }
        }
    }

    fn handle_frame(&mut self, now: Instant, frame: Frame) -> Result<(), ProtocolError> {
        let Frame { command, payload } = frame;
        let cmd_type = pb::base_command::Type::try_from(command.r#type)
            .map_err(|_| ProtocolError::UnsupportedCommand(command.r#type))?;

        match cmd_type {
            pb::base_command::Type::Connected => {
                let connected = command
                    .connected
                    .ok_or(ProtocolError::Handshake("missing CommandConnected"))?;
                self.state = HandshakeState::Connected;
                self.last_connected_at = Some((self.wall_clock)());
                self.broker_max_message_size = connected.max_message_size.map(|v| v as usize);
                self.broker_protocol_version = connected.protocol_version.unwrap_or(0);
                self.feature_flags = connected.feature_flags.unwrap_or_default();
                self.events.push_back(ConnectionEvent::Connected {
                    protocol_version: self.broker_protocol_version,
                    max_message_size: connected.max_message_size.unwrap_or(0) as u32,
                    feature_flags: self.feature_flags,
                });
            }
            pb::base_command::Type::Ping => {
                // Pong back immediately.
                let pong = pb::BaseCommand {
                    r#type: pb::base_command::Type::Pong as i32,
                    pong: Some(pb::CommandPong {}),
                    ..Default::default()
                };
                self.encode_command(&pong)?;
            }
            pb::base_command::Type::Pong => {
                // Nothing to do — last_activity already updated above.
            }
            pb::base_command::Type::AuthChallenge => {
                let challenge = command
                    .auth_challenge
                    .ok_or(ProtocolError::Handshake("missing CommandAuthChallenge"))?;
                self.state = HandshakeState::AuthChallenging;
                self.events.push_back(ConnectionEvent::AuthChallenge {
                    method: challenge
                        .challenge
                        .as_ref()
                        .and_then(|d| d.auth_method_name.clone()),
                    challenge: challenge.challenge.and_then(|d| d.auth_data),
                });
            }
            pb::base_command::Type::SendReceipt => {
                let receipt = command
                    .send_receipt
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandSendReceipt body",
                    ))?;
                if let Some(producer) = self.producers.get_mut(&ProducerHandle(receipt.producer_id))
                {
                    if let Some((seq, mid, waker)) = producer.apply_receipt(&receipt) {
                        producer.total_acks_received =
                            producer.total_acks_received.saturating_add(1);
                        let handle = producer.handle;
                        let key = PendingOpKey::Send(handle, seq);
                        self.outcomes.insert(
                            key,
                            OpOutcome::SendReceipt {
                                sequence_id: seq,
                                message_id: mid,
                            },
                        );
                        if let Some(w) = waker {
                            w.wake();
                        } else if let Some(w) = self.wakers.remove(&key) {
                            w.wake();
                        }
                        self.events.push_back(ConnectionEvent::SendReceipt {
                            handle,
                            sequence_id: seq,
                            message_id: mid,
                        });
                    }
                }
            }
            pb::base_command::Type::SendError => {
                let err = command.send_error.ok_or(ProtocolError::InvariantViolation(
                    "missing CommandSendError",
                ))?;
                if let Some(producer) = self.producers.get_mut(&ProducerHandle(err.producer_id)) {
                    if let Some((seq, waker, code, message)) = producer.apply_send_error(&err) {
                        producer.total_send_failed = producer.total_send_failed.saturating_add(1);
                        let handle = producer.handle;
                        let key = PendingOpKey::Send(handle, seq);
                        self.outcomes.insert(
                            key,
                            OpOutcome::SendError {
                                sequence_id: seq,
                                code,
                                message: message.clone(),
                            },
                        );
                        if let Some(w) = waker {
                            w.wake();
                        } else if let Some(w) = self.wakers.remove(&key) {
                            w.wake();
                        }
                        self.events.push_back(ConnectionEvent::SendError {
                            handle,
                            sequence_id: seq,
                            code,
                            message,
                        });
                    }
                }
            }
            pb::base_command::Type::Message => {
                let msg = command
                    .message
                    .ok_or(ProtocolError::InvariantViolation("missing CommandMessage"))?;
                let payload = payload.ok_or(ProtocolError::InvariantViolation(
                    "Message frame missing payload",
                ))?;
                let handle = ConsumerHandle(msg.consumer_id);
                if let Some(consumer) = self.consumers.get_mut(&handle) {
                    let outcome = consumer.deliver(
                        &msg,
                        payload.metadata.clone(),
                        payload.broker_entry_metadata.clone(),
                        payload.body.clone(),
                        now,
                    );
                    if let Ok(crate::consumer::DeliverOutcome::Delivered { .. }) = outcome {
                        // Emit one event per delivered message — easier for the driver to
                        // surface to its waker pool than batching here.
                        while let Some(im) = consumer.pop_front_clone() {
                            self.events.push_back(ConnectionEvent::Message {
                                handle,
                                message: im,
                            });
                        }
                    }
                }
            }
            pb::base_command::Type::ProducerSuccess => {
                let ok = command
                    .producer_success
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandProducerSuccess",
                    ))?;
                let request_id = RequestId(ok.request_id);
                if let Some(PendingRequestKind::ProducerOpen { handle }) =
                    self.pending_requests.remove(&request_id)
                {
                    if let Some(producer) = self.producers.get_mut(&handle) {
                        producer.name = Some(ok.producer_name.clone());
                        producer.last_sequence_id_published = ok.last_sequence_id.unwrap_or(-1);
                    }
                    self.outcomes.insert(
                        PendingOpKey::Request(request_id),
                        OpOutcome::Success { request_id },
                    );
                    self.wake_for_request(request_id);
                    self.events.push_back(ConnectionEvent::ProducerReady {
                        handle,
                        producer_name: ok.producer_name,
                        last_sequence_id: ok.last_sequence_id.unwrap_or(-1),
                        schema_version: ok.schema_version.unwrap_or_default(),
                    });
                }
            }
            pb::base_command::Type::Success => {
                let ok = command
                    .success
                    .ok_or(ProtocolError::InvariantViolation("missing CommandSuccess"))?;
                let request_id = RequestId(ok.request_id);
                let kind = self.pending_requests.remove(&request_id);
                self.outcomes.insert(
                    PendingOpKey::Request(request_id),
                    OpOutcome::Success { request_id },
                );
                self.wake_for_request(request_id);
                if let Some(PendingRequestKind::ConsumerSubscribe { handle }) = kind {
                    self.events
                        .push_back(ConnectionEvent::SubscribeAcked { handle });
                }
                if let Some(PendingRequestKind::ConsumerSeek { handle }) = kind {
                    if let Some(c) = self.consumers.get_mut(&handle) {
                        let _ = c.seek_acked();
                    }
                }
            }
            pb::base_command::Type::Error => {
                let err = command
                    .error
                    .ok_or(ProtocolError::InvariantViolation("missing CommandError"))?;
                let request_id = RequestId(err.request_id);
                self.pending_requests.remove(&request_id);
                self.outcomes.insert(
                    PendingOpKey::Request(request_id),
                    OpOutcome::Error {
                        request_id,
                        code: err.error,
                        message: err.message.clone(),
                    },
                );
                self.wake_for_request(request_id);
            }
            pb::base_command::Type::AckResponse => {
                let ack = command
                    .ack_response
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandAckResponse",
                    ))?;
                let result = if let Some(message) = ack.message.clone() {
                    Err(message)
                } else {
                    Ok(())
                };
                let request_id = ack.request_id.map(RequestId);
                if let Some(rid) = request_id {
                    let kind = self.pending_requests.remove(&rid);
                    if result.is_err() {
                        if let Some(PendingRequestKind::Ack { handle }) = kind {
                            if let Some(consumer) = self.consumers.get_mut(&handle) {
                                consumer.total_acks_failed =
                                    consumer.total_acks_failed.saturating_add(1);
                            }
                        }
                    }
                    self.outcomes.insert(
                        PendingOpKey::Request(rid),
                        match &result {
                            Ok(()) => OpOutcome::Success { request_id: rid },
                            Err(msg) => OpOutcome::Error {
                                request_id: rid,
                                code: ack.error.unwrap_or(0),
                                message: msg.clone(),
                            },
                        },
                    );
                    self.wake_for_request(rid);
                }
                self.events
                    .push_back(ConnectionEvent::AckResponse { request_id, result });
            }
            pb::base_command::Type::LookupResponse => {
                let resp =
                    command
                        .lookup_topic_response
                        .ok_or(ProtocolError::InvariantViolation(
                            "missing CommandLookupTopicResponse",
                        ))?;
                let rid = RequestId(resp.request_id);
                if let Some(req) = self.lookup.take_lookup(rid) {
                    let (outcome, retry) = crate::lookup::translate_lookup_response(&resp, &req);
                    if let Some(retry) = retry {
                        let new_id = self.alloc_request_id();
                        let _ = self.send_lookup_internal(new_id, retry);
                    }
                    self.pending_requests.remove(&rid);
                    self.outcomes.insert(
                        PendingOpKey::Request(rid),
                        OpOutcome::LookupResponse {
                            request_id: rid,
                            outcome: outcome.clone(),
                        },
                    );
                    self.wake_for_request(rid);
                    self.events.push_back(ConnectionEvent::LookupResponse {
                        request_id: rid,
                        result: outcome,
                    });
                }
            }
            pb::base_command::Type::PartitionedMetadataResponse => {
                let resp = command.partition_metadata_response.ok_or(
                    ProtocolError::InvariantViolation(
                        "missing CommandPartitionedTopicMetadataResponse",
                    ),
                )?;
                let rid = RequestId(resp.request_id);
                if self.lookup.take_partition(rid).is_some() {
                    self.pending_requests.remove(&rid);
                    let error = resp
                        .error
                        .map(|code| (code, resp.message.clone().unwrap_or_default()));
                    let partitions = resp.partitions.unwrap_or(0);
                    self.outcomes.insert(
                        PendingOpKey::Request(rid),
                        OpOutcome::PartitionedMetadata {
                            request_id: rid,
                            partitions,
                            error: error.clone(),
                        },
                    );
                    self.wake_for_request(rid);
                    self.events
                        .push_back(ConnectionEvent::PartitionedMetadataResponse {
                            request_id: rid,
                            partitions,
                            error,
                        });
                }
            }
            pb::base_command::Type::GetLastMessageIdResponse => {
                let resp = command.get_last_message_id_response.ok_or(
                    ProtocolError::InvariantViolation("missing CommandGetLastMessageIdResponse"),
                )?;
                let rid = RequestId(resp.request_id);
                self.pending_requests.remove(&rid);
                let last_message_id = MessageId::from_pb(&resp.last_message_id);
                let consumer_mark_delete_position = resp
                    .consumer_mark_delete_position
                    .as_ref()
                    .map(MessageId::from_pb);
                self.outcomes.insert(
                    PendingOpKey::Request(rid),
                    OpOutcome::LastMessageId {
                        request_id: rid,
                        last_message_id,
                        consumer_mark_delete_position,
                    },
                );
                self.wake_for_request(rid);
            }
            pb::base_command::Type::CloseProducer => {
                let close = command
                    .close_producer
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandCloseProducer",
                    ))?;
                let handle = ProducerHandle(close.producer_id);
                if let Some(p) = self.producers.get_mut(&handle) {
                    p.close();
                }
                self.events
                    .push_back(ConnectionEvent::ProducerClosedByBroker {
                        handle,
                        assigned_broker_service_url: close.assigned_broker_service_url,
                    });
            }
            pb::base_command::Type::CloseConsumer => {
                let close = command
                    .close_consumer
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandCloseConsumer",
                    ))?;
                let handle = ConsumerHandle(close.consumer_id);
                if let Some(c) = self.consumers.get_mut(&handle) {
                    c.close();
                }
                self.events
                    .push_back(ConnectionEvent::ConsumerClosedByBroker {
                        handle,
                        assigned_broker_service_url: close.assigned_broker_service_url,
                    });
            }
            pb::base_command::Type::ReachedEndOfTopic => {
                let rc = command
                    .reached_end_of_topic
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandReachedEndOfTopic",
                    ))?;
                let handle = ConsumerHandle(rc.consumer_id);
                if let Some(consumer) = self.consumers.get_mut(&handle) {
                    consumer.reached_end_of_topic = true;
                    if let Some(w) = consumer.receive_waker.take() {
                        w.wake();
                    }
                }
                self.events
                    .push_back(ConnectionEvent::ReachedEndOfTopic { handle });
            }
            pb::base_command::Type::ActiveConsumerChange => {
                let acc =
                    command
                        .active_consumer_change
                        .ok_or(ProtocolError::InvariantViolation(
                            "missing CommandActiveConsumerChange",
                        ))?;
                let handle = ConsumerHandle(acc.consumer_id);
                self.events
                    .push_back(ConnectionEvent::ActiveConsumerChanged {
                        handle,
                        active: acc.is_active.unwrap_or(false),
                    });
            }
            pb::base_command::Type::TopicMigrated => {
                let migrated = command
                    .topic_migrated
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandTopicMigrated",
                    ))?;
                use pb::command_topic_migrated::ResourceType;
                let producer = if migrated.resource_type == ResourceType::Producer as i32 {
                    Some(ProducerHandle(migrated.resource_id))
                } else {
                    None
                };
                let consumer = if migrated.resource_type == ResourceType::Consumer as i32 {
                    Some(ConsumerHandle(migrated.resource_id))
                } else {
                    None
                };
                self.events.push_back(ConnectionEvent::TopicMigrated {
                    producer,
                    consumer,
                    broker_service_url: migrated.broker_service_url,
                    broker_service_url_tls: migrated.broker_service_url_tls,
                });
            }
            pb::base_command::Type::WatchTopicListSuccess => {
                let ok =
                    command
                        .watch_topic_list_success
                        .ok_or(ProtocolError::InvariantViolation(
                            "missing CommandWatchTopicListSuccess",
                        ))?;
                let rid = RequestId(ok.request_id);
                if let Some(watcher) = self.topic_watchers.lookup_by_request(rid) {
                    watcher.topics_hash = Some(ok.topics_hash.clone());
                    watcher.initialised = true;
                }
                self.pending_requests.remove(&rid);
                let topics = ok.topic.clone();
                self.outcomes.insert(
                    PendingOpKey::Request(rid),
                    OpOutcome::TopicListSnapshot {
                        request_id: rid,
                        topics: topics.clone(),
                    },
                );
                self.wake_for_request(rid);
                self.events.push_back(ConnectionEvent::TopicListSnapshot {
                    request_id: rid,
                    topics,
                });
            }
            pb::base_command::Type::WatchTopicUpdate => {
                let upd = command
                    .watch_topic_update
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandWatchTopicUpdate",
                    ))?;
                if let Some(watcher) = self.topic_watchers.lookup_by_watcher_id(upd.watcher_id) {
                    watcher.topics_hash = Some(upd.topics_hash.clone());
                }
                self.events.push_back(ConnectionEvent::TopicListChanged {
                    added: upd.new_topics,
                    removed: upd.deleted_topics,
                });
            }
            pb::base_command::Type::NewTxnResponse => {
                let resp = command
                    .new_txn_response
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandNewTxnResponse",
                    ))?;
                let request_id = RequestId(resp.request_id);
                self.pending_requests.remove(&request_id);
                let result = match self.txn_client.handle_new_txn_response(resp) {
                    Ok(Some(id)) => Ok(id),
                    Ok(None) => {
                        // Unknown request id — drop the outcome silently. The driver will not
                        // surface a future for a request we never enqueued.
                        return Ok(());
                    }
                    Err(err) => Err(err),
                };
                self.outcomes.insert(
                    PendingOpKey::Request(request_id),
                    OpOutcome::NewTxn {
                        request_id,
                        result: result.clone(),
                    },
                );
                self.wake_for_request(request_id);
                self.events.push_back(ConnectionEvent::TxnResponse {
                    request_id,
                    outcome: TxnRoundTrip::NewTxn(result),
                });
            }
            pb::base_command::Type::AddPartitionToTxnResponse => {
                let resp = command.add_partition_to_txn_response.ok_or(
                    ProtocolError::InvariantViolation("missing CommandAddPartitionToTxnResponse"),
                )?;
                let request_id = RequestId(resp.request_id);
                self.pending_requests.remove(&request_id);
                let result = self.txn_client.handle_add_partition_response(resp);
                self.outcomes.insert(
                    PendingOpKey::Request(request_id),
                    OpOutcome::AddPartitionToTxn {
                        request_id,
                        result: result.clone(),
                    },
                );
                self.wake_for_request(request_id);
                self.events.push_back(ConnectionEvent::TxnResponse {
                    request_id,
                    outcome: TxnRoundTrip::AddPartition(result),
                });
            }
            pb::base_command::Type::AddSubscriptionToTxnResponse => {
                let resp = command.add_subscription_to_txn_response.ok_or(
                    ProtocolError::InvariantViolation(
                        "missing CommandAddSubscriptionToTxnResponse",
                    ),
                )?;
                let request_id = RequestId(resp.request_id);
                self.pending_requests.remove(&request_id);
                let result = self.txn_client.handle_add_subscription_response(resp);
                self.outcomes.insert(
                    PendingOpKey::Request(request_id),
                    OpOutcome::AddSubscriptionToTxn {
                        request_id,
                        result: result.clone(),
                    },
                );
                self.wake_for_request(request_id);
                self.events.push_back(ConnectionEvent::TxnResponse {
                    request_id,
                    outcome: TxnRoundTrip::AddSubscription(result),
                });
            }
            pb::base_command::Type::EndTxnResponse => {
                let resp = command
                    .end_txn_response
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandEndTxnResponse",
                    ))?;
                let request_id = RequestId(resp.request_id);
                self.pending_requests.remove(&request_id);
                let result = self.txn_client.handle_end_txn_response(resp);
                self.outcomes.insert(
                    PendingOpKey::Request(request_id),
                    OpOutcome::EndTxn {
                        request_id,
                        result: result.clone(),
                    },
                );
                self.wake_for_request(request_id);
                self.events.push_back(ConnectionEvent::TxnResponse {
                    request_id,
                    outcome: TxnRoundTrip::EndTxn(result),
                });
            }
            pb::base_command::Type::GetSchemaResponse => {
                let resp = command
                    .get_schema_response
                    .ok_or(ProtocolError::InvariantViolation(
                        "missing CommandGetSchemaResponse",
                    ))?;
                let request_id = RequestId(resp.request_id);
                if matches!(
                    self.pending_requests.get(&request_id),
                    Some(PendingRequestKind::GetSchema)
                ) {
                    self.pending_requests.remove(&request_id);
                    let result = match (resp.schema, resp.error_code) {
                        (Some(schema), None) => Ok((schema, resp.schema_version)),
                        (_, Some(code)) => Err((code, resp.error_message.unwrap_or_default())),
                        (None, None) => Err((
                            0,
                            "broker returned empty CommandGetSchemaResponse".to_owned(),
                        )),
                    };
                    self.outcomes.insert(
                        PendingOpKey::Request(request_id),
                        OpOutcome::GetSchemaResponse {
                            request_id,
                            result: result.clone(),
                        },
                    );
                    self.wake_for_request(request_id);
                    self.events
                        .push_back(ConnectionEvent::GetSchemaResponse { request_id, result });
                }
            }
            _ => {
                // Unhandled command — we tolerate them silently for forward compatibility, but
                // we DO push an event for the driver to log.
                tracing::trace!(target: "magnetar_proto", cmd_type = ?cmd_type, "unhandled command type");
            }
        }
        // Drain producer outbound frames opportunistically — we accumulate them into the
        // central byte buffer so the driver can flush them in one syscall.
        self.drain_producer_outbound();
        let _ = now;
        Ok(())
    }

    fn wake_for_request(&mut self, request_id: RequestId) {
        if let Some(w) = self.wakers.remove(&PendingOpKey::Request(request_id)) {
            w.wake();
        }
    }

    /// Drain queued outbound bytes into `buf`. Returns the number of bytes copied.
    pub fn poll_transmit(&mut self, buf: &mut Vec<u8>) -> usize {
        self.drain_producer_outbound();
        if self.outbound.is_empty() {
            return 0;
        }
        let n = self.outbound.len();
        buf.extend_from_slice(&self.outbound);
        self.outbound.clear();
        n
    }

    /// Pull the next [`ConnectionEvent`], if any.
    pub fn poll_event(&mut self) -> Option<ConnectionEvent> {
        self.events.pop_front()
    }

    /// Time of the next scheduled wake-up — the earliest of the keepalive deadline and any
    /// per-consumer tracker deadline (negative-ack delay + unacked-message timeout).
    pub fn poll_timeout(&self) -> Option<Instant> {
        let mut next = self
            .last_activity
            .map(|t| t + self.config.keepalive_interval);
        let mut consider = |deadline: Instant| {
            next = Some(match next {
                Some(current) => current.min(deadline),
                None => deadline,
            });
        };
        for consumer in self.consumers.values() {
            if let Some(t) = consumer.nack_tracker.as_ref() {
                if let Some(d) = t.next_deadline() {
                    consider(d);
                }
            }
            if let Some(t) = consumer.unacked_tracker.as_ref() {
                if let Some(d) = t.next_deadline() {
                    consider(d);
                }
            }
            if let Some(t) = consumer.ack_tracker.as_ref() {
                if let Some(d) = t.next_deadline() {
                    consider(d);
                }
            }
        }
        for producer in self.producers.values() {
            if let Some(d) = producer.next_send_deadline() {
                consider(d);
            }
            if let Some(d) = producer.next_batch_deadline() {
                consider(d);
            }
        }
        next
    }

    /// Tick the state machine — fires keepalive pings + any per-consumer tracker actions
    /// whose deadlines have elapsed.
    pub fn handle_timeout(&mut self, now: Instant) {
        // Keepalive.
        let due = match self.last_activity {
            Some(last) if now >= last + self.config.keepalive_interval => true,
            None => false,
            _ => false,
        };
        if due && self.is_connected() {
            let ping = pb::BaseCommand {
                r#type: pb::base_command::Type::Ping as i32,
                ping: Some(pb::CommandPing {}),
                ..Default::default()
            };
            let _ = self.encode_command(&ping);
            self.last_activity = Some(now);
        }

        // Tracker-driven redeliveries — both negative-ack delay and unacked-message timeout
        // produce the same CommandRedeliverUnacknowledgedMessages payload, so we collect
        // then emit through the shared helper.
        let mut redeliveries: Vec<(ConsumerHandle, Vec<MessageId>)> = Vec::new();
        let mut ack_actions: Vec<crate::trackers::AckAction> = Vec::new();
        for (handle, consumer) in &mut self.consumers {
            if let Some(tracker) = consumer.nack_tracker.as_mut() {
                for action in tracker.poll(now) {
                    let crate::trackers::NackAction::RedeliverUnacked { message_ids, .. } = action;
                    redeliveries.push((*handle, message_ids));
                }
            }
            if let Some(tracker) = consumer.unacked_tracker.as_mut() {
                for action in tracker.poll(now) {
                    let crate::trackers::UnackedAction::RedeliverExpired { message_ids, .. } =
                        action;
                    redeliveries.push((*handle, message_ids));
                }
            }
            if let Some(tracker) = consumer.ack_tracker.as_mut() {
                ack_actions.extend(tracker.poll(now));
            }
        }
        for (handle, ids) in redeliveries {
            self.emit_redeliver_unacked(handle, ids);
        }
        // Flush the ack-grouping tracker. The actions go through the shared dispatcher
        // which allocates a `RequestId` per coalesced `CommandAck`; the response is
        // routed back through the existing pending-requests slot, but no user future is
        // tied to it (ack_grouped_* is fire-and-forget).
        if !ack_actions.is_empty() {
            self.dispatch_ack_actions(ack_actions);
        }

        // Per-producer batch flush sweep — Java `ProducerBuilder#batchingMaxPublishDelay`.
        // Any non-empty batch whose first message has been waiting longer than the
        // configured delay flushes now, capping end-to-end batch latency.
        let publish_time_ms = (self.wall_clock)()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0u64, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        let due_batch_handles: Vec<ProducerHandle> = self
            .producers
            .iter()
            .filter(|(_, p)| p.batch_deadline_elapsed(now))
            .map(|(h, _)| *h)
            .collect();
        for handle in due_batch_handles {
            if let Some(producer) = self.producers.get_mut(&handle) {
                let _ = producer.flush_batch(publish_time_ms, now);
            }
        }
        // Drain any frames the batch flush queued so callers don't need an extra
        // poll_transmit round-trip just to wake them up.
        self.drain_producer_outbound();

        // Per-producer send-timeout sweep. Surface each timed-out send as an
        // `OpOutcome::SendError` so the caller's send future resolves with the configured
        // timeout error.
        let mut send_timeouts: Vec<(ProducerHandle, SequenceId, Option<Waker>)> = Vec::new();
        for (handle, producer) in &mut self.producers {
            for (seq, waker) in producer.drain_timed_out_sends(now) {
                producer.total_send_failed = producer.total_send_failed.saturating_add(1);
                send_timeouts.push((*handle, seq, waker));
            }
        }
        for (handle, seq, waker) in send_timeouts {
            let key = PendingOpKey::Send(handle, seq);
            // Pulsar's ServerError enum has no TimeoutError; use the same `-1` sentinel
            // Java surfaces as TimeoutException with a descriptive message so callers can
            // pattern-match on the error string.
            self.outcomes.insert(
                key,
                OpOutcome::SendError {
                    sequence_id: seq,
                    code: -1,
                    message: "send timeout".to_owned(),
                },
            );
            if let Some(w) = waker {
                w.wake();
            } else if let Some(w) = self.wakers.remove(&key) {
                w.wake();
            }
            self.events.push_back(ConnectionEvent::SendError {
                handle,
                sequence_id: seq,
                code: -1,
                message: "send timeout".to_owned(),
            });
        }
    }

    /// Register a waker for a pending op. The waker will be woken when an outcome lands.
    pub fn register_waker(&mut self, key: PendingOpKey, waker: Waker) {
        if let Some(_outcome) = self.outcomes.get(&key) {
            // Wake immediately if outcome is already present.
            waker.wake();
            return;
        }
        match key {
            PendingOpKey::Send(handle, seq) => {
                if let Some(p) = self.producers.get_mut(&handle) {
                    p.register_waker(seq, waker);
                    return;
                }
            }
            PendingOpKey::Request(_) => {}
        }
        self.wakers.insert(key, waker);
    }

    /// Consume the outcome of a pending op, if one is ready.
    pub fn take_outcome(&mut self, key: PendingOpKey) -> Option<OpOutcome> {
        self.outcomes.remove(&key)
    }

    /// Open a producer. The state machine emits a `CommandProducer` and assigns a
    /// [`ProducerHandle`]. The corresponding [`ConnectionEvent::ProducerReady`] arrives on the
    /// next `poll_event` cycle after the broker responds.
    pub fn create_producer(&mut self, req: CreateProducerRequest) -> ProducerHandle {
        let handle = ProducerHandle(self.next_producer_id);
        self.next_producer_id = self.next_producer_id.wrapping_add(1);
        let request_id = self.alloc_request_id();
        let max_size = self
            .broker_max_message_size
            .unwrap_or(self.config.default_max_message_size);
        let mut state = ProducerState::new(handle, req.topic.clone(), req.compression, max_size);
        state.batching_enabled = req.enable_batching;
        state.chunking_enabled = req.enable_chunking;
        state.max_batch_size_bytes = req.max_batch_size_bytes;
        state.max_messages_in_batch = req.max_messages_in_batch;
        state.name = req.producer_name.clone();
        if let Some(initial) = req.initial_sequence_id {
            state.set_initial_sequence_id(initial);
        }
        state.send_timeout = req.send_timeout;
        state.batching_max_publish_delay = req.batching_max_publish_delay;
        state.access_mode = req.access_mode;
        self.producers.insert(handle, state);

        let producer_metadata: Vec<pb::KeyValue> = req
            .producer_metadata
            .iter()
            .map(|(k, v)| pb::KeyValue {
                key: k.clone(),
                value: v.clone(),
            })
            .collect();
        let cmd = pb::CommandProducer {
            topic: req.topic,
            producer_id: handle.0,
            request_id: request_id.0,
            producer_name: req.producer_name.clone(),
            encrypted: None,
            metadata: producer_metadata,
            schema: req.schema,
            epoch: None,
            user_provided_producer_name: Some(req.producer_name.is_some()),
            producer_access_mode: Some(req.access_mode as i32),
            topic_epoch: None,
            txn_enabled: None,
            initial_subscription_name: None,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Producer as i32,
            producer: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::ProducerOpen { handle });
        handle
    }

    /// Open a consumer. Returns the handle and emits `CommandSubscribe`. The driver receives
    /// [`ConnectionEvent::SubscribeAcked`] on success and should then call
    /// [`Self::initial_flow`] to feed the broker an initial flow.
    pub fn subscribe(&mut self, req: SubscribeRequest) -> ConsumerHandle {
        let handle = ConsumerHandle(self.next_consumer_id);
        self.next_consumer_id = self.next_consumer_id.wrapping_add(1);
        let request_id = self.alloc_request_id();
        let mut state = ConsumerState::new(
            handle,
            req.topic.clone(),
            req.subscription.clone(),
            req.receiver_queue_size,
        );
        state.max_redeliver_count = req.max_redeliver_count;
        state.consumer_name = req.consumer_name.clone();
        if let Some(delay) = req.negative_ack_redelivery_delay {
            state.nack_tracker = Some(crate::trackers::NegativeAcksTracker::new(handle, delay));
        }
        if let Some(timeout) = req.ack_timeout {
            let mut tracker = crate::trackers::UnackedMessageTracker::new(handle, timeout);
            if let Some(backoff) = req.ack_timeout_backoff {
                tracker = tracker.with_backoff(backoff);
            }
            state.unacked_tracker = Some(tracker);
        }
        if let Some(group_time) = req.ack_group_time {
            state.ack_tracker = Some(crate::trackers::AckGroupingTracker::new(handle, group_time));
        }
        state.crypto_failure_action = req.crypto_failure_action;
        self.consumers.insert(handle, state);

        let subscription_properties: Vec<pb::KeyValue> = req
            .subscription_properties
            .into_iter()
            .map(|(key, value)| pb::KeyValue { key, value })
            .collect();
        let key_shared_meta = req.key_shared.as_ref().map(|cfg| pb::KeySharedMeta {
            key_shared_mode: cfg.mode as i32,
            hash_ranges: cfg
                .sticky_hash_ranges
                .iter()
                .map(|(start, end)| pb::IntRange {
                    start: *start,
                    end: *end,
                })
                .collect(),
            allow_out_of_order_delivery: Some(cfg.allow_out_of_order_delivery),
        });
        let start_message_id = req.start_message_id.map(MessageId::to_pb);
        let consumer_metadata: Vec<pb::KeyValue> = req
            .consumer_metadata
            .into_iter()
            .map(|(key, value)| pb::KeyValue { key, value })
            .collect();
        let cmd = pb::CommandSubscribe {
            topic: req.topic,
            subscription: req.subscription,
            sub_type: req.sub_type as i32,
            consumer_id: handle.0,
            request_id: request_id.0,
            consumer_name: req.consumer_name,
            priority_level: req.priority_level,
            durable: Some(req.durable),
            start_message_id,
            metadata: consumer_metadata,
            read_compacted: if req.read_compacted { Some(true) } else { None },
            schema: req.schema,
            initial_position: Some(req.initial_position as i32),
            replicate_subscription_state: req.replicate_subscription_state,
            force_topic_creation: req.force_topic_creation,
            start_message_rollback_duration_sec: req.start_message_rollback_duration_sec,
            key_shared_meta,
            subscription_properties,
            consumer_epoch: None,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Subscribe as i32,
            subscribe: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::ConsumerSubscribe { handle });
        handle
    }

    /// Emit the initial flow command for a consumer once it's been acked.
    pub fn initial_flow(&mut self, handle: ConsumerHandle) -> Option<RequestId> {
        let flow_cmd = self.consumers.get_mut(&handle)?.initial_flow();
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Flow as i32,
            flow: Some(flow_cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        None
    }

    /// Send a message via the given producer.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvariantViolation`] if the handle is unknown, and propagates
    /// the producer's own [`crate::error::ProducerError`] (wrapped) if the send is rejected.
    pub fn send(
        &mut self,
        handle: ProducerHandle,
        msg: crate::producer::OutgoingMessage,
        publish_time_ms: u64,
        now: Instant,
    ) -> Result<SequenceId, ProtocolError> {
        let producer = self
            .producers
            .get_mut(&handle)
            .ok_or(ProtocolError::InvariantViolation("unknown producer handle"))?;
        let decision = producer
            .queue_send(msg, publish_time_ms, now)
            .map_err(|_| ProtocolError::InvariantViolation("producer rejected send"))?;
        let seq_id = SequenceId(producer.last_sequence_id_pushed.max(0) as u64);
        match decision {
            SendDecision::Emit { .. } | SendDecision::Batched => {}
        }
        self.drain_producer_outbound();
        Ok(seq_id)
    }

    /// Force a batch flush for a producer.
    pub fn flush_producer(
        &mut self,
        handle: ProducerHandle,
        publish_time_ms: u64,
        now: Instant,
    ) -> usize {
        let n = self
            .producers
            .get_mut(&handle)
            .map(|p| p.flush_batch(publish_time_ms, now))
            .unwrap_or(0);
        self.drain_producer_outbound();
        n
    }

    /// Number of in-flight sends on a producer (i.e. sends with no `CommandSendReceipt` yet).
    /// Used by the runtime engines' `Producer::flush` to know when it's safe to return.
    #[must_use]
    pub fn producer_pending_count(&self, handle: ProducerHandle) -> usize {
        self.producers.get(&handle).map_or(0, |p| p.pending.len())
    }

    /// Number of messages currently buffered in the producer's batch container (waiting
    /// for the next flush cycle). Returns `0` for unknown handles or when batching is
    /// disabled / the batch is empty.
    #[must_use]
    pub fn producer_batch_len(&self, handle: ProducerHandle) -> usize {
        self.producers.get(&handle).map_or(0, |p| p.batch.len())
    }

    /// Sum of payload bytes currently buffered in the producer's batch container.
    #[must_use]
    pub fn producer_batch_bytes(&self, handle: ProducerHandle) -> usize {
        self.producers
            .get(&handle)
            .map_or(0, |p| p.batch.current_size_bytes)
    }

    /// Access mode the producer was opened with. Returns
    /// `ProducerAccessMode::Shared` (the broker default) for unknown handles. Mirrors Java
    /// `Producer#getProducerAccessMode`.
    #[must_use]
    pub fn producer_access_mode(&self, handle: ProducerHandle) -> pb::ProducerAccessMode {
        self.producers
            .get(&handle)
            .map_or(pb::ProducerAccessMode::Shared, |p| p.access_mode)
    }

    /// Last sequence id this client has pushed onto the wire. `-1` if the producer has
    /// never sent. Mirrors Java's `Producer#getLastSequenceId` (which counts pushes,
    /// not broker acknowledgements).
    #[must_use]
    pub fn producer_last_sequence_id_pushed(&self, handle: ProducerHandle) -> i64 {
        self.producers
            .get(&handle)
            .map_or(-1, |p| p.last_sequence_id_pushed)
    }

    /// Last sequence id the broker has acknowledged via `CommandSendReceipt`. `-1` if the
    /// producer has no acknowledged sends yet. Useful for at-least-once resume-on-restart.
    #[must_use]
    pub fn producer_last_sequence_id_published(&self, handle: ProducerHandle) -> i64 {
        self.producers
            .get(&handle)
            .map_or(-1, |p| p.last_sequence_id_published)
    }

    /// Cumulative producer counters snapshot. Returns `None` if the producer handle is unknown.
    #[must_use]
    pub fn producer_stats(&self, handle: ProducerHandle) -> Option<crate::producer::ProducerStats> {
        self.producers.get(&handle).map(ProducerState::stats)
    }

    /// Cumulative consumer counters snapshot. Returns `None` if the consumer handle is unknown.
    #[must_use]
    pub fn consumer_stats(&self, handle: ConsumerHandle) -> Option<crate::consumer::ConsumerStats> {
        self.consumers.get(&handle).map(ConsumerState::stats)
    }

    /// `true` if the producer with this handle has been closed (locally via
    /// [`Self::close_producer`] or remotely via a broker `CloseProducer`). Returns `true`
    /// for unknown handles so callers can treat "handle dropped" as "closed". Mirrors Java
    /// `Producer#isConnected` inversion — Pulsar Java has no direct `isClosed` on
    /// Producer, but ProducerImpl exposes `getState() == CLOSED` for this exact purpose.
    #[must_use]
    pub fn producer_is_closed(&self, handle: ProducerHandle) -> bool {
        self.producers.get(&handle).is_none_or(|p| p.closed)
    }

    /// `true` if the consumer with this handle has been closed (locally via
    /// [`Self::close_consumer`] / [`Self::unsubscribe`] or remotely via a broker
    /// `CloseConsumer`). Returns `true` for unknown handles. Mirrors Java
    /// `Consumer#isClosed` semantics via ConsumerImpl's `getState() == CLOSED`.
    #[must_use]
    pub fn consumer_is_closed(&self, handle: ConsumerHandle) -> bool {
        self.consumers.get(&handle).is_none_or(|c| c.closed)
    }

    /// Number of messages currently buffered in the consumer's receiver queue, waiting for
    /// a `receive()` call to pull them out. Returns `0` for unknown handles. Mirrors Java
    /// `ConsumerImpl#numMessagesInQueue` / `getTotalIncomingMessages` (the in-memory side).
    #[must_use]
    pub fn consumer_queue_len(&self, handle: ConsumerHandle) -> usize {
        self.consumers.get(&handle).map_or(0, |c| c.queue.len())
    }

    /// Number of dispatch permits the consumer still has with the broker — i.e. messages
    /// it has authorised the broker to push without an explicit `CommandFlow`. Returns `0`
    /// for unknown handles. Mirrors Java `ConsumerBase#getAvailablePermits`.
    #[must_use]
    pub fn consumer_available_permits(&self, handle: ConsumerHandle) -> u32 {
        self.consumers
            .get(&handle)
            .map_or(0, |c| c.available_permits)
    }

    /// PIP-4 decryption failure handling configured for this consumer. Returns
    /// [`CryptoFailureAction::Fail`] (the safe default) for unknown handles so callers can
    /// treat a missing consumer as fail-fast. Mirrors Java `Consumer#getCryptoFailureAction`.
    #[must_use]
    pub fn consumer_crypto_failure_action(&self, handle: ConsumerHandle) -> CryptoFailureAction {
        self.consumers.get(&handle).map_or(
            CryptoFailureAction::Fail,
            ConsumerState::crypto_failure_action,
        )
    }

    fn drain_producer_outbound(&mut self) {
        // Pull every queued frame from every producer and emit it into the connection's
        // outbound byte buffer.
        let handles: Vec<ProducerHandle> = self.producers.keys().copied().collect();
        for handle in handles {
            while let Some(frame) = self
                .producers
                .get_mut(&handle)
                .and_then(ProducerState::next_outbound_frame)
            {
                let _ = encode_payload(
                    &mut self.outbound,
                    &frame.command,
                    &frame.metadata,
                    &frame.payload,
                );
            }
        }
    }

    /// Acknowledge messages.
    pub fn ack(&mut self, handle: ConsumerHandle, ack: AckRequest) -> RequestId {
        let request_id = self.alloc_request_id();
        let n_ids = ack.message_ids.len() as u64;
        // Stop tracking the acked ids in both the unacked-message tracker and the nack tracker
        // (caller may have nacked then acked the same id).
        if let Some(consumer) = self.consumers.get_mut(&handle) {
            for id in &ack.message_ids {
                if let Some(t) = consumer.unacked_tracker.as_mut() {
                    t.remove(id);
                }
                if let Some(t) = consumer.nack_tracker.as_mut() {
                    t.remove(id);
                }
            }
        }
        // PIP-54: for any message id with `batch_index >= 0`, look up the per-batch ack
        // tracker, clear the bit at `batch_index`, and emit either a "full" MessageIdData
        // (no ack_set; the batch is now fully acked, so the broker can advance the cursor
        // past it) or a partial-ack MessageIdData carrying the bitset of still-unacked
        // positions so the broker holds the cursor.
        let pb_ids: Vec<pb::MessageIdData> =
            if matches!(ack.ack_type, pb::command_ack::AckType::Individual) {
                if let Some(consumer) = self.consumers.get_mut(&handle) {
                    ack.message_ids
                        .iter()
                        .map(|id| {
                            let mut pb_id = id.to_pb();
                            if id.batch_index >= 0 {
                                let key = (id.ledger_id, id.entry_id);
                                let fully = if let Some(entry) =
                                    consumer.batch_ack_tracker.get_mut(&key)
                                {
                                    let fully = entry.ack_position(id.batch_index);
                                    if !fully {
                                        pb_id.ack_set = entry.ack_set_i64();
                                    }
                                    fully
                                } else {
                                    // No tracker entry — either the batch's first delivery happened
                                    // before PIP-54 wiring or the tracker was already cleared by a
                                    // prior full-batch ack. Fall through as a regular ack.
                                    true
                                };
                                if fully {
                                    consumer.batch_ack_tracker.remove(&key);
                                }
                            }
                            pb_id
                        })
                        .collect()
                } else {
                    ack.message_ids.iter().map(|m| m.to_pb()).collect()
                }
            } else {
                // Cumulative ack — every position up to the supplied id is implicitly acked,
                // so any per-batch tracker entries the cumulative position covers are stale.
                // Drop them so future individual acks on the same batch don't synthesise a
                // partial bitset for state the broker has already moved past.
                if let Some(consumer) = self.consumers.get_mut(&handle) {
                    let covered: Vec<(u64, u64)> = ack
                        .message_ids
                        .iter()
                        .map(|id| (id.ledger_id, id.entry_id))
                        .collect();
                    for key in covered {
                        consumer.batch_ack_tracker.remove(&key);
                    }
                }
                ack.message_ids.iter().map(|m| m.to_pb()).collect()
            };
        let properties: Vec<pb::KeyLongValue> = ack
            .properties
            .iter()
            .map(|(k, v)| pb::KeyLongValue {
                key: k.clone(),
                value: *v as u64,
            })
            .collect();
        let cmd = pb::CommandAck {
            consumer_id: handle.0,
            ack_type: ack.ack_type as i32,
            message_id: pb_ids,
            validation_error: None,
            properties,
            txnid_least_bits: ack.txn_id.map(|t| t.least_sig_bits),
            txnid_most_bits: ack.txn_id.map(|t| t.most_sig_bits),
            request_id: Some(request_id.0),
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Ack as i32,
            ack: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::Ack { handle });
        if let Some(consumer) = self.consumers.get_mut(&handle) {
            consumer.total_acks_sent = consumer.total_acks_sent.saturating_add(n_ids);
        }
        request_id
    }

    /// Stage an individual ack into this consumer's ack-grouping tracker. The state
    /// machine flushes the tracker once `ack_group_time` has elapsed since the first
    /// staged ack, emitting one coalesced `CommandAck` for the whole batch. Fire-and-
    /// forget: there is no per-call `RequestId` because the broker response will not be
    /// tied to any one ack call. Falls back to an immediate `CommandAck` (synchronous,
    /// allocated `RequestId` is discarded) when no tracker is configured so the message
    /// is never silently dropped. Mirrors Java's `acknowledgmentGroupTime` path.
    pub fn ack_grouped_individual(
        &mut self,
        handle: ConsumerHandle,
        message_id: MessageId,
        now: Instant,
    ) {
        let actions = self
            .consumers
            .get_mut(&handle)
            .and_then(|c| c.ack_tracker.as_mut())
            .map(|t| t.add_individual(message_id, now));
        if let Some(actions) = actions {
            self.dispatch_ack_actions(actions);
        } else {
            let _ = self.ack(
                handle,
                AckRequest {
                    message_ids: vec![message_id],
                    ack_type: pb::command_ack::AckType::Individual,
                    properties: Vec::new(),
                    txn_id: None,
                },
            );
        }
    }

    /// Stage a cumulative ack into this consumer's ack-grouping tracker. See
    /// [`Self::ack_grouped_individual`] for the semantics.
    pub fn ack_grouped_cumulative(
        &mut self,
        handle: ConsumerHandle,
        message_id: MessageId,
        now: Instant,
    ) {
        let actions = self
            .consumers
            .get_mut(&handle)
            .and_then(|c| c.ack_tracker.as_mut())
            .map(|t| t.add_cumulative(message_id, now));
        if let Some(actions) = actions {
            self.dispatch_ack_actions(actions);
        } else {
            let _ = self.ack(
                handle,
                AckRequest {
                    message_ids: vec![message_id],
                    ack_type: pb::command_ack::AckType::Cumulative,
                    properties: Vec::new(),
                    txn_id: None,
                },
            );
        }
    }

    fn dispatch_ack_actions(&mut self, actions: Vec<crate::trackers::AckAction>) {
        for action in actions {
            match action {
                crate::trackers::AckAction::SendIndividualAck {
                    handle,
                    message_ids,
                } => {
                    let _ = self.ack(
                        handle,
                        AckRequest {
                            message_ids,
                            ack_type: pb::command_ack::AckType::Individual,
                            properties: Vec::new(),
                            txn_id: None,
                        },
                    );
                }
                crate::trackers::AckAction::SendCumulativeAck { handle, message_id } => {
                    let _ = self.ack(
                        handle,
                        AckRequest {
                            message_ids: vec![message_id],
                            ack_type: pb::command_ack::AckType::Cumulative,
                            properties: Vec::new(),
                            txn_id: None,
                        },
                    );
                }
            }
        }
    }

    /// Issue an explicit FLOW for a consumer.
    pub fn flow(&mut self, handle: ConsumerHandle, permits: u32) {
        let cmd = pb::CommandFlow {
            consumer_id: handle.0,
            message_permits: permits,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Flow as i32,
            flow: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
    }

    /// Mark a consumer as paused / resumed. Mirrors Java `Consumer#pause` / `#resume`. While
    /// paused the consumer skips automatic flow refills, so the broker stops dispatching new
    /// messages once already-issued permits drain. Buffered messages remain available via
    /// [`Self::pop_message`].
    pub fn set_paused(&mut self, handle: ConsumerHandle, paused: bool) {
        if let Some(c) = self.consumers.get_mut(&handle) {
            c.paused = paused;
        }
    }

    /// Drain every message the consumer has classified as dead-letter (redelivery count
    /// strictly greater than `max_redeliver_count` at subscribe time). Returns an empty
    /// vec when the consumer is unknown or has no DLQ-flagged messages. Mirrors Java
    /// `ConsumerImpl#getDeadLetterMessages` behavior — the caller is responsible for
    /// republishing them to the configured DLQ topic.
    pub fn drain_dead_letter(&mut self, handle: ConsumerHandle) -> Vec<IncomingMessage> {
        self.consumers
            .get_mut(&handle)
            .map(|c| std::mem::take(&mut c.dead_letter_pending))
            .unwrap_or_default()
    }

    /// Returns the per-consumer pause flag, or `None` if the consumer handle is unknown.
    #[must_use]
    pub fn is_paused(&self, handle: ConsumerHandle) -> Option<bool> {
        self.consumers.get(&handle).map(|c| c.paused)
    }

    /// Returns `true` once the broker has sent `CommandReachedEndOfTopic` for this
    /// consumer. Mirrors Java `Consumer#hasReachedEndOfTopic`.
    #[must_use]
    pub fn consumer_reached_end_of_topic(&self, handle: ConsumerHandle) -> bool {
        self.consumers
            .get(&handle)
            .map(|c| c.reached_end_of_topic)
            .unwrap_or(false)
    }

    /// Topic name this consumer is bound to. Returns `None` if the consumer handle is
    /// unknown.
    #[must_use]
    pub fn consumer_topic(&self, handle: ConsumerHandle) -> Option<&str> {
        self.consumers.get(&handle).map(|c| c.topic.as_str())
    }

    /// Subscription name of this consumer. Returns `None` if the consumer handle is unknown.
    #[must_use]
    pub fn consumer_subscription(&self, handle: ConsumerHandle) -> Option<&str> {
        self.consumers.get(&handle).map(|c| c.subscription.as_str())
    }

    /// Caller-supplied consumer name advertised at subscribe time. Returns `None` if the
    /// consumer handle is unknown or no name was supplied.
    #[must_use]
    pub fn consumer_name(&self, handle: ConsumerHandle) -> Option<&str> {
        self.consumers
            .get(&handle)
            .and_then(|c| c.consumer_name.as_deref())
    }

    /// Topic name this producer is bound to. Returns `None` if the producer handle is
    /// unknown.
    #[must_use]
    pub fn producer_topic(&self, handle: ProducerHandle) -> Option<&str> {
        self.producers.get(&handle).map(|p| p.topic.as_str())
    }

    /// Broker-assigned producer name (set after the CommandProducer / CommandProducerSuccess
    /// round-trip). Returns `None` if the producer handle is unknown or the name has not
    /// arrived yet.
    #[must_use]
    pub fn producer_name(&self, handle: ProducerHandle) -> Option<&str> {
        self.producers.get(&handle).and_then(|p| p.name.as_deref())
    }

    /// Negatively acknowledge messages — request the broker to redeliver them.
    /// Mirrors `ConsumerImpl#negativeAcknowledge`.
    ///
    /// Empty `message_ids` means "redeliver every unacked message on this consumer"
    /// (Java's `consumer.redeliverUnacknowledgedMessages()`) and is always sent immediately.
    /// Otherwise, if the consumer has a negative-ack tracker configured (via
    /// [`SubscribeRequest::negative_ack_redelivery_delay`]), the supplied ids are deferred
    /// until [`Self::handle_timeout`] notices the delay has elapsed. With no tracker the
    /// redelivery is sent immediately.
    pub fn negative_ack(
        &mut self,
        handle: ConsumerHandle,
        message_ids: Vec<MessageId>,
        now: Instant,
    ) {
        if !message_ids.is_empty() {
            if let Some(consumer) = self.consumers.get_mut(&handle) {
                if let Some(tracker) = consumer.nack_tracker.as_mut() {
                    for id in &message_ids {
                        tracker.add(*id, now);
                    }
                    return;
                }
            }
        }
        self.emit_redeliver_unacked(handle, message_ids);
    }

    /// Negative-ack a single message with an explicit per-message delay, bypassing the
    /// consumer's default `negative_ack_redelivery_delay`. Falls back to an immediate
    /// redelivery when the subscription was opened without a nack tracker (so the message
    /// is never silently lost). Mirrors PIP-37's per-message backoff path — the caller
    /// computes `delay` from the message's redelivery count via
    /// [`crate::trackers::nack::MultiplierRedeliveryBackoff::delay_for`].
    pub fn negative_ack_with_delay(
        &mut self,
        handle: ConsumerHandle,
        message_id: MessageId,
        delay: core::time::Duration,
        now: Instant,
    ) {
        if let Some(consumer) = self.consumers.get_mut(&handle) {
            if let Some(tracker) = consumer.nack_tracker.as_mut() {
                tracker.add_with_delay(message_id, delay, now);
                return;
            }
        }
        self.emit_redeliver_unacked(handle, vec![message_id]);
    }

    fn emit_redeliver_unacked(&mut self, handle: ConsumerHandle, message_ids: Vec<MessageId>) {
        let pb_ids = message_ids.into_iter().map(MessageId::to_pb).collect();
        let cmd = pb::CommandRedeliverUnacknowledgedMessages {
            consumer_id: handle.0,
            message_ids: pb_ids,
            consumer_epoch: None,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::RedeliverUnacknowledgedMessages as i32,
            redeliver_unacknowledged_messages: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
    }

    /// Request the broker's last-published message id for the topic this consumer is
    /// subscribed to. Java equivalent: `consumer.getLastMessageId()`. Useful for
    /// "more messages?" checks against the consumer's most-recently-received id (or for
    /// Reader's `hasMessageAvailable()` semantics).
    pub fn get_last_message_id(&mut self, handle: ConsumerHandle) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = pb::CommandGetLastMessageId {
            consumer_id: handle.0,
            request_id: request_id.0,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::GetLastMessageId as i32,
            get_last_message_id: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests.insert(
            request_id,
            PendingRequestKind::ConsumerGetLastMessageId { handle },
        );
        request_id
    }

    /// Issue a seek.
    pub fn seek(&mut self, handle: ConsumerHandle, target: SeekTarget) -> RequestId {
        let request_id = self.alloc_request_id();
        let (message_id, publish_time) = match target {
            SeekTarget::MessageId(mid) => (Some(mid.to_pb()), None),
            SeekTarget::PublishTime(t) => (None, Some(t)),
        };
        let cmd = pb::CommandSeek {
            consumer_id: handle.0,
            request_id: request_id.0,
            message_id,
            message_publish_time: publish_time,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Seek as i32,
            seek: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        if let Some(c) = self.consumers.get_mut(&handle) {
            c.begin_seek(request_id);
        }
        self.pending_requests
            .insert(request_id, PendingRequestKind::ConsumerSeek { handle });
        request_id
    }

    /// Issue a topic lookup. The state machine handles redirects internally; the user receives
    /// either a `Connect` or `Failed` outcome.
    pub fn lookup(&mut self, topic: &str, authoritative: bool) -> RequestId {
        let request_id = self.alloc_request_id();
        let req = LookupRequest {
            topic: topic.to_owned(),
            authoritative,
        };
        let _ = self.send_lookup_internal(request_id, req);
        request_id
    }

    fn send_lookup_internal(
        &mut self,
        request_id: RequestId,
        req: LookupRequest,
    ) -> Result<(), ProtocolError> {
        let cmd = pb::CommandLookupTopic {
            topic: req.topic.clone(),
            request_id: request_id.0,
            authoritative: Some(req.authoritative),
            original_principal: None,
            original_auth_data: None,
            original_auth_method: None,
            advertised_listener_name: None,
            properties: Vec::new(),
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Lookup as i32,
            lookup_topic: Some(cmd),
            ..Default::default()
        };
        self.encode_command(&base)?;
        self.lookup.insert_lookup(request_id, req);
        self.pending_requests
            .insert(request_id, PendingRequestKind::Lookup);
        Ok(())
    }

    /// Issue a `CommandGetSchema` to look up the schema declared for `topic` in the broker's
    /// schema registry.
    ///
    /// Mirrors Java `PulsarClientImpl#getSchema` and the `LookupService#getSchema` round-trip.
    /// The state machine surfaces the response via [`OpOutcome::GetSchemaResponse`] and
    /// [`ConnectionEvent::GetSchemaResponse`].
    ///
    /// `version` is the requested schema version when known (e.g. when re-decoding a historical
    /// payload). Pass `None` to ask the broker for the topic's current schema.
    ///
    /// Used by [`crate::schema::AutoConsumeSchema`] and
    /// [`crate::schema::AutoProduceBytesSchema`] to populate their per-instance schema cache
    /// (PIP-87 broker-side schema lookup).
    pub fn get_schema(&mut self, topic: &str, version: Option<Vec<u8>>) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = pb::CommandGetSchema {
            request_id: request_id.0,
            topic: topic.to_owned(),
            schema_version: version,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::GetSchema as i32,
            get_schema: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::GetSchema);
        request_id
    }

    /// Request partitioned-topic metadata.
    pub fn get_partitioned_topic_metadata(&mut self, topic: &str) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = pb::CommandPartitionedTopicMetadata {
            topic: topic.to_owned(),
            request_id: request_id.0,
            original_principal: None,
            original_auth_data: None,
            original_auth_method: None,
            metadata_auto_creation_enabled: Some(true),
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::PartitionedMetadata as i32,
            partition_metadata: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.lookup.insert_partition(
            request_id,
            PartitionedMetadataRequest {
                topic: topic.to_owned(),
            },
        );
        self.pending_requests
            .insert(request_id, PendingRequestKind::PartitionedMetadata);
        request_id
    }

    /// Start a topic-list watcher (PIP-145).
    pub fn watch_topic_list(&mut self, namespace: &str, pattern: &str) -> RequestId {
        let request_id = self.alloc_request_id();
        let watcher_id = self.next_watcher_id;
        self.next_watcher_id = self.next_watcher_id.wrapping_add(1);
        let cmd = pb::CommandWatchTopicList {
            request_id: request_id.0,
            watcher_id,
            namespace: namespace.to_owned(),
            topics_pattern: pattern.to_owned(),
            topics_hash: None,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::WatchTopicList as i32,
            watch_topic_list: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.topic_watchers.insert(
            watcher_id,
            request_id,
            TopicWatcher {
                pattern: pattern.to_owned(),
                namespace: namespace.to_owned(),
                topics_hash: None,
                initialised: false,
            },
        );
        self.pending_requests
            .insert(request_id, PendingRequestKind::TopicWatcher { watcher_id });
        request_id
    }

    /// Close a producer.
    pub fn close_producer(&mut self, handle: ProducerHandle) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = pb::CommandCloseProducer {
            producer_id: handle.0,
            request_id: request_id.0,
            assigned_broker_service_url: None,
            assigned_broker_service_url_tls: None,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::CloseProducer as i32,
            close_producer: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        if let Some(p) = self.producers.get_mut(&handle) {
            p.close();
        }
        self.pending_requests
            .insert(request_id, PendingRequestKind::ProducerClose { handle });
        request_id
    }

    /// Close a consumer.
    pub fn close_consumer(&mut self, handle: ConsumerHandle) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = pb::CommandCloseConsumer {
            consumer_id: handle.0,
            request_id: request_id.0,
            assigned_broker_service_url: None,
            assigned_broker_service_url_tls: None,
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::CloseConsumer as i32,
            close_consumer: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        if let Some(c) = self.consumers.get_mut(&handle) {
            c.close();
        }
        self.pending_requests
            .insert(request_id, PendingRequestKind::ConsumerClose { handle });
        request_id
    }

    /// Unsubscribe — remove this consumer's subscription from the broker.
    ///
    /// Mirrors `org.apache.pulsar.client.api.Consumer#unsubscribe`. Unlike
    /// [`close_consumer`](Self::close_consumer) which keeps the subscription
    /// cursor alive on the broker, `unsubscribe` deletes the subscription
    /// entirely — useful for tear-down + cleanup. The runtime should call
    /// `close_consumer` afterwards.
    ///
    /// `force=true` (PIP-313) drops the subscription even if other consumers
    /// are still attached.
    pub fn unsubscribe(&mut self, handle: ConsumerHandle, force: bool) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = pb::CommandUnsubscribe {
            consumer_id: handle.0,
            request_id: request_id.0,
            force: Some(force),
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::Unsubscribe as i32,
            unsubscribe: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests.insert(
            request_id,
            PendingRequestKind::ConsumerUnsubscribe { handle },
        );
        request_id
    }

    /// Mutable accessor for the embedded [`TxnClient`].
    ///
    /// Drivers needing to register a waker against a pending TC request (`new_txn`,
    /// `add_partition_to_txn`, …) reach in via this accessor — the [`Connection`] otherwise
    /// owns and drives the client.
    pub fn txn_client_mut(&mut self) -> &mut TxnClient {
        &mut self.txn_client
    }

    /// Read-only accessor for the embedded [`TxnClient`].
    pub fn txn_client(&self) -> &TxnClient {
        &self.txn_client
    }

    /// Open a new transaction at the broker-side transaction coordinator. Returns the request
    /// id; the matching [`OpOutcome::NewTxn`] is consumed via [`Self::take_outcome`].
    pub fn new_txn(&mut self, timeout: Duration) -> RequestId {
        let request_id = self.alloc_request_id();
        let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        let cmd = self.txn_client.new_txn(request_id.0, timeout_ms);
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::NewTxn as i32,
            new_txn: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::NewTxn);
        request_id
    }

    /// Register `topic` as a partition that the transaction will write to. Returns the request
    /// id; the matching [`OpOutcome::AddPartitionToTxn`] is consumed via [`Self::take_outcome`].
    pub fn add_partition_to_txn(&mut self, txn: TxnId, topic: String) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = self.txn_client.add_partition(request_id.0, txn, topic);
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::AddPartitionToTxn as i32,
            add_partition_to_txn: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::AddPartitionToTxn);
        request_id
    }

    /// Register `(subscription, topic)` as a subscription the transaction will acknowledge on.
    /// Returns the request id; the matching [`OpOutcome::AddSubscriptionToTxn`] is consumed via
    /// [`Self::take_outcome`].
    pub fn add_subscription_to_txn(
        &mut self,
        txn: TxnId,
        subscription: String,
        topic: String,
    ) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = self
            .txn_client
            .add_subscription(request_id.0, txn, subscription, topic);
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::AddSubscriptionToTxn as i32,
            add_subscription_to_txn: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::AddSubscriptionToTxn);
        request_id
    }

    /// Commit or abort the transaction. Returns the request id; the matching
    /// [`OpOutcome::EndTxn`] is consumed via [`Self::take_outcome`] once the broker replies.
    pub fn end_txn(&mut self, txn: TxnId, action: TxnAction) -> RequestId {
        let request_id = self.alloc_request_id();
        let cmd = self.txn_client.end_txn(request_id.0, txn, action);
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::EndTxn as i32,
            end_txn: Some(cmd),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        self.pending_requests
            .insert(request_id, PendingRequestKind::EndTxn);
        request_id
    }

    /// Close the whole connection.
    pub fn close(&mut self) {
        if matches!(
            self.state,
            HandshakeState::Connected | HandshakeState::AuthChallenging
        ) {
            self.last_disconnected_at = Some((self.wall_clock)());
        }
        self.state = HandshakeState::Closing;
        self.events
            .push_back(ConnectionEvent::Closed { reason: None });
    }

    /// Submit a `CommandAuthResponse` in answer to a server `CommandAuthChallenge`.
    pub fn submit_auth_response(&mut self, auth_data: Vec<u8>, auth_method: Option<String>) {
        let resp = pb::CommandAuthResponse {
            client_version: Some(self.config.client_version.clone()),
            response: Some(pb::AuthData {
                auth_method_name: auth_method,
                auth_data: Some(auth_data),
            }),
            protocol_version: Some(self.config.protocol_version),
        };
        let base = pb::BaseCommand {
            r#type: pb::base_command::Type::AuthResponse as i32,
            auth_response: Some(resp),
            ..Default::default()
        };
        let _ = self.encode_command(&base);
        if self.state == HandshakeState::AuthChallenging {
            self.state = HandshakeState::Connected;
        }
    }

    /// Access a producer state (read-only) — useful in tests + driver instrumentation.
    pub fn producer(&self, handle: ProducerHandle) -> Option<&ProducerState> {
        self.producers.get(&handle)
    }

    /// Mutable access to a producer.
    pub fn producer_mut(&mut self, handle: ProducerHandle) -> Option<&mut ProducerState> {
        self.producers.get_mut(&handle)
    }

    /// Access a consumer state (read-only).
    pub fn consumer(&self, handle: ConsumerHandle) -> Option<&ConsumerState> {
        self.consumers.get(&handle)
    }

    /// Mutable access to a consumer.
    pub fn consumer_mut(&mut self, handle: ConsumerHandle) -> Option<&mut ConsumerState> {
        self.consumers.get_mut(&handle)
    }

    /// Number of bytes pending transmit.
    pub fn outbound_len(&self) -> usize {
        self.outbound.len()
    }

    /// Payload size (post-decompression / post-decryption — payload as it sits in the
    /// queue, which is the bytes the runtime layer will hand to user code) of the next
    /// message that [`Self::pop_message`] would return. Returns `None` for unknown
    /// handles or empty queues. Lets the runtime peek before committing to a pop —
    /// useful for size-capped batch receive (Java `BatchReceivePolicy.maxNumBytes`).
    #[must_use]
    pub fn peek_message_payload_size(&self, handle: ConsumerHandle) -> Option<usize> {
        self.consumers
            .get(&handle)
            .and_then(|c| c.queue.front())
            .map(|m| m.payload.len())
    }

    /// Drain a single message from the given consumer's queue.
    pub fn pop_message(&mut self, handle: ConsumerHandle) -> Option<IncomingMessage> {
        let consumer = self.consumers.get_mut(&handle)?;
        let msg = consumer.pop_message();
        // After popping, opportunistically check whether we owe the broker a FLOW.
        if let Some(flow_cmd) = consumer.maybe_flow() {
            let base = pb::BaseCommand {
                r#type: pb::base_command::Type::Flow as i32,
                flow: Some(flow_cmd),
                ..Default::default()
            };
            let _ = self.encode_command(&base);
        }
        msg
    }

    fn alloc_request_id(&mut self) -> RequestId {
        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        RequestId(id)
    }

    fn encode_command(&mut self, cmd: &pb::BaseCommand) -> Result<(), ProtocolError> {
        encode_command(&mut self.outbound, cmd)?;
        Ok(())
    }
}

// We use a small helper on ConsumerState to clone-pop the front message without leaving the
// crate's public API to expose all of ConsumerState's internals. The runtime crate goes through
// `Connection::pop_message`; this path is for the internal "burst-emit on dispatch" code path
// in `handle_frame`.
impl ConsumerState {
    pub(crate) fn pop_front_clone(&mut self) -> Option<IncomingMessage> {
        let msg = self.queue.pop_front()?;
        self.consumed_since_flow = self.consumed_since_flow.saturating_add(1);
        Some(msg)
    }
}

#[cfg(test)]
mod conn_state_tests {
    use super::*;
    use crate::frame::encode_command;

    fn handshake_response_bytes() -> bytes::BytesMut {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-test".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode CommandConnected");
        buf
    }

    #[test]
    fn timestamps_track_connect_and_disconnect() {
        let mut conn = Connection::new(ConnectionConfig::default());
        assert!(conn.last_connected_timestamp().is_none());
        assert!(conn.last_disconnected_timestamp().is_none());
        assert!(!conn.is_connected());

        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame).expect("handle");
        assert!(conn.is_connected());
        let connected_at = conn
            .last_connected_timestamp()
            .expect("connected timestamp set");
        assert!(conn.last_disconnected_timestamp().is_none());

        conn.mark_disconnected();
        assert!(!conn.is_connected());
        let disconnected_at = conn
            .last_disconnected_timestamp()
            .expect("disconnected timestamp set");
        assert!(disconnected_at >= connected_at);

        // Marking disconnected again should not bump the timestamp now that we're already in
        // a terminal state (idempotency for repeated mark_disconnected calls on Failed).
        let pinned = disconnected_at;
        conn.mark_disconnected();
        assert_eq!(conn.last_disconnected_timestamp(), Some(pinned));
    }

    #[test]
    fn local_close_records_disconnect() {
        let mut conn = Connection::new(ConnectionConfig::default());
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame).expect("handle");
        assert!(conn.is_connected());

        conn.close();
        assert!(conn.last_disconnected_timestamp().is_some());
    }

    #[test]
    fn is_closed_tracks_terminal_states() {
        let mut conn = Connection::new(ConnectionConfig::default());
        assert!(!conn.is_closed(), "uninitialized is not closed");
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame).expect("handle");
        assert!(!conn.is_closed(), "connected is not closed");
        conn.close();
        assert!(conn.is_closed(), "after close, is_closed is true");

        // Mark_disconnected (Failed) is also a terminal state.
        let mut conn2 = Connection::new(ConnectionConfig::default());
        conn2.begin_handshake().expect("handshake");
        let frame2 = handshake_response_bytes();
        conn2.handle_bytes(Instant::now(), &frame2).expect("handle");
        conn2.mark_disconnected();
        assert!(conn2.is_closed(), "Failed state counts as closed");
    }

    #[test]
    fn consumer_crypto_failure_action_defaults_to_fail_for_unknown_handle() {
        let conn = Connection::new(ConnectionConfig::default());
        // No consumer has been created; an arbitrary handle must map to the safe default.
        let action = conn.consumer_crypto_failure_action(ConsumerHandle(42));
        assert_eq!(action, CryptoFailureAction::Fail);
    }

    #[test]
    fn consumer_crypto_failure_action_round_trips_from_subscribe_request() {
        // Spin up a handshake-complete connection so `subscribe` runs cleanly. We never
        // observe the broker response — we only need the locally-stored consumer state.
        let mut conn = Connection::new(ConnectionConfig::default());
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame).expect("handle");

        for action in [
            CryptoFailureAction::Fail,
            CryptoFailureAction::Discard,
            CryptoFailureAction::Consume,
        ] {
            let req = SubscribeRequest {
                topic: "persistent://public/default/t".to_owned(),
                subscription: "s".to_owned(),
                crypto_failure_action: action,
                ..Default::default()
            };
            let handle = conn.subscribe(req);
            assert_eq!(
                conn.consumer_crypto_failure_action(handle),
                action,
                "crypto_failure_action {action:?} should round-trip through subscribe",
            );
        }
    }

    /// PIP-188: feeding a `CommandTopicMigrated` BaseCommand surfaces a
    /// [`ConnectionEvent::TopicMigrated`] carrying the resource handle and the new broker URLs
    /// so the engine layer can re-bind the affected producer/consumer to the new broker.
    #[test]
    fn topic_migrated_command_surfaces_event() {
        let mut conn = Connection::new(ConnectionConfig::default());
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame)
            .expect("handle handshake response");

        // Drain the `Connected` event so subsequent `poll_event` returns the migration event.
        match conn.poll_event() {
            Some(ConnectionEvent::Connected { .. }) => {}
            other => panic!("expected Connected event, got {other:?}"),
        }

        // Feed a CommandTopicMigrated for a producer being moved to a new broker.
        let migrated = pb::BaseCommand {
            r#type: pb::base_command::Type::TopicMigrated as i32,
            topic_migrated: Some(pb::CommandTopicMigrated {
                resource_id: 7,
                resource_type: pb::command_topic_migrated::ResourceType::Producer as i32,
                broker_service_url: Some("pulsar://new-broker:6650".to_owned()),
                broker_service_url_tls: Some("pulsar+ssl://new-broker:6651".to_owned()),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &migrated).expect("encode CommandTopicMigrated");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("handle CommandTopicMigrated");

        match conn.poll_event() {
            Some(ConnectionEvent::TopicMigrated {
                producer,
                consumer,
                broker_service_url,
                broker_service_url_tls,
            }) => {
                assert_eq!(producer, Some(ProducerHandle(7)));
                assert_eq!(consumer, None);
                assert_eq!(
                    broker_service_url.as_deref(),
                    Some("pulsar://new-broker:6650")
                );
                assert_eq!(
                    broker_service_url_tls.as_deref(),
                    Some("pulsar+ssl://new-broker:6651")
                );
            }
            other => panic!("expected TopicMigrated event, got {other:?}"),
        }

        // A consumer migration must surface in the `consumer` slot of the same variant.
        let migrated_cons = pb::BaseCommand {
            r#type: pb::base_command::Type::TopicMigrated as i32,
            topic_migrated: Some(pb::CommandTopicMigrated {
                resource_id: 42,
                resource_type: pb::command_topic_migrated::ResourceType::Consumer as i32,
                broker_service_url: None,
                broker_service_url_tls: None,
            }),
            ..Default::default()
        };
        let mut buf2 = bytes::BytesMut::new();
        encode_command(&mut buf2, &migrated_cons)
            .expect("encode consumer-side CommandTopicMigrated");
        conn.handle_bytes(Instant::now(), &buf2)
            .expect("handle consumer-side CommandTopicMigrated");

        match conn.poll_event() {
            Some(ConnectionEvent::TopicMigrated {
                producer,
                consumer,
                broker_service_url,
                broker_service_url_tls,
            }) => {
                assert_eq!(producer, None);
                assert_eq!(consumer, Some(ConsumerHandle(42)));
                assert!(broker_service_url.is_none());
                assert!(broker_service_url_tls.is_none());
            }
            other => panic!("expected consumer-side TopicMigrated event, got {other:?}"),
        }
    }

    #[test]
    fn reset_bumps_epoch_and_fails_pending_ops_with_session_lost() {
        let mut conn = Connection::new(ConnectionConfig::default());
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame).expect("handle");
        assert!(conn.is_connected());
        let epoch_before = conn.session_epoch();
        assert_eq!(epoch_before, 0);

        // Issue a request-bound op (partitioned-metadata lookup) — pending until broker reply.
        let request_id = conn.get_partitioned_topic_metadata("persistent://public/default/t");
        let key = PendingOpKey::Request(request_id);
        assert!(
            conn.take_outcome(key).is_none(),
            "no outcome before broker reply"
        );

        // Also queue an in-flight publish so we exercise the producer-side drain branch.
        let producer = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/p".to_owned(),
            ..Default::default()
        });
        let seq = conn
            .send(
                producer,
                crate::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"hi"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 2,
                    num_messages: 1,
                    txn_id: None,
                },
                0,
            )
            .expect("send queues");
        let send_key = PendingOpKey::Send(producer, seq);
        // The send should have been queued as pending.
        assert!(
            conn.take_outcome(send_key).is_none(),
            "publish stays pending until broker replies"
        );
        // Sanity: the producer reports the publish as pending.
        assert_eq!(
            conn.producer_pending_count(producer),
            1,
            "send must produce a pending OpSend"
        );

        // Now reset — every pending op must surface SessionLost and the epoch must bump.
        conn.reset();
        assert_eq!(conn.session_epoch(), epoch_before + 1);
        assert!(
            matches!(
                conn.take_outcome(key),
                Some(OpOutcome::SessionLost { key: k }) if k == key
            ),
            "request-bound op fails with SessionLost after reset"
        );
        assert!(
            matches!(
                conn.take_outcome(send_key),
                Some(OpOutcome::SessionLost { key: k }) if k == send_key
            ),
            "in-flight publish fails with SessionLost after reset"
        );
        assert_eq!(
            conn.state(),
            HandshakeState::Uninitialized,
            "reset snaps state back to Uninitialized so begin_handshake can fire on a new socket"
        );
    }

    #[test]
    fn op_outcome_session_lost_round_trips_through_outcome_slab() {
        // The slab itself is HashMap<PendingOpKey, OpOutcome>; this test exercises the
        // SessionLost variant end-to-end so the runtime-side dispatcher can pattern-match.
        let mut conn = Connection::new(ConnectionConfig::default());
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame).expect("handle");

        let request_id = conn.get_partitioned_topic_metadata("persistent://public/default/t");
        let key = PendingOpKey::Request(request_id);

        // No outcome before reset.
        assert!(conn.take_outcome(key).is_none());

        conn.reset();

        match conn.take_outcome(key) {
            Some(OpOutcome::SessionLost { key: k }) => assert_eq!(k, key),
            other => panic!("expected SessionLost, got {other:?}"),
        }
        // Second take is empty — outcomes are one-shot.
        assert!(conn.take_outcome(key).is_none());
    }
}
