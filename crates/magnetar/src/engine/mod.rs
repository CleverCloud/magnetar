// SPDX-License-Identifier: Apache-2.0

//! `Engine` trait — the abstraction the public [`crate::PulsarClient`] is
//! generic over.
//!
//! `Engine` is a marker trait with a single associated type
//! ([`Engine::ClientState`]) that selects the per-engine storage backing
//! [`crate::PulsarClient<E>`]. Today the two implementations are
//! [`TokioEngine`] (production, default) and [`MoonpoolEngine<P>`]
//! (deterministic simulation; `P` is the
//! [`moonpool_core::Providers`](moonpool_core::Providers) bundle).
//!
//! Engine-specific methods (`producer`, `consumer`, partitioned, …) live in
//! dedicated `impl PulsarClient<ConcreteEngine>` blocks rather than on the
//! trait — production engines have wildly different connect signatures
//! (tokio takes a URL, moonpool takes `host:port` + a `Providers` bundle)
//! and trying to surface those through a single trait would either lose
//! typing or reintroduce the per-engine façade duplication
//! [ADR-0019](../../specs/adr/0019-engine-scope-and-moonpool-parity.md)
//! rejected as Option B.
//!
//! Instead, moonpool callers that reach for a tokio-only method get a
//! clean trait-bound error rather than a silent fallback — exactly the
//! ADR-0019 §Decision contract for v0.1.0.
//!
//! See ADR-0019 gate (e) — "Option A: generic `PulsarClient<E: Engine>`
//! with default `E = TokioEngine`" — for the rationale.
//!
//! # Module layout
//!
//! - `mod.rs` (this file) — the [`Engine`] trait, the per-surface extension traits
//!   (`TransactionApi`, `ProducerApi`, `ConsumerApi`, `BrokerMetadataApi`, `SubscribeApi`,
//!   `CreateProducerApi`), the shared type aliases (`SubscribeFut`, `ReceiveOptFut`,
//!   `ReceiveBatchFut`, `WatchTopicListFut`, `OpenProducerFut`), and the [`TopicListChange`] data
//!   struct.
//! - [`tokio`] — the [`TokioEngine`] marker + every `impl … for magnetar_runtime_tokio::*` block.
//! - [`moonpool`] — the [`MoonpoolEngine`] marker + every `impl<P> … for
//!   magnetar_runtime_moonpool::*` block.

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

#[cfg(feature = "moonpool")]
pub(crate) mod moonpool;
#[cfg(feature = "tokio")]
pub(crate) mod tokio;

#[cfg(feature = "moonpool")]
pub use moonpool::MoonpoolEngine;
#[cfg(feature = "tokio")]
pub use tokio::TokioEngine;

/// Marker trait labelling a runtime engine. Implementations select the
/// concrete storage type ([`Self::ClientState`]) that backs the engine's
/// branch of [`crate::PulsarClient<E>`].
///
/// `'static + Send + Sync` mirrors what we already require of producers and
/// consumers; downstream users that hand `PulsarClient<E>` to a tokio
/// `spawn` (or moonpool `spawn`) need at least that.
///
/// # Task and timer primitives (ADR-0025 phase 1)
///
/// The associated [`Self::TaskHandle`] and [`Self::Interval`] types plus the
/// [`Self::spawn`] / [`Self::abort_task`] / [`Self::new_interval`] /
/// [`Self::interval_tick`] methods give the façade an engine-agnostic way to
/// spawn background tasks and drive periodic timers. They are the
/// prerequisite for moving `PartitionedProducer::health_loop`,
/// `TableView::drain_task`, `MultiTopicsConsumer::auto_update`, and the
/// other surface lifts off `impl PulsarClient<TokioEngine>`. See
/// [ADR-0025](../../specs/adr/0025-engine-trait-task-and-timer-primitives.md).
pub trait Engine:
    'static + Send + Sync + Debug + MessageEncryptorApi + MessageDecryptorApi
{
    /// Per-engine state stored inside [`crate::PulsarClient<E>`]. The tokio
    /// engine plugs in [`magnetar_runtime_tokio::Client`]; the moonpool
    /// engine plugs in `(Arc<moonpool::ConnectionShared>,
    /// moonpool::DriverHandle)`. Both bundles are `'static + Send + Sync`
    /// so the façade can be moved across spawn boundaries unchanged.
    type ClientState: 'static + Send + Sync;

    /// Opaque, cancel-safe handle to a background task spawned via
    /// [`Self::spawn`]. Dropping the handle aborts the task on the tokio
    /// engine; explicit [`Self::abort_task`] is the happens-before-Drop
    /// path the façade uses on shutdown.
    type TaskHandle: 'static + Send;

    /// Opaque periodic timer created via [`Self::new_interval`]. The
    /// façade drives ticks via [`Self::interval_tick`].
    type Interval: 'static + Send;

    /// Human-readable engine name, surfaced in logs / panics / errors.
    /// Default returns the Rust type name — engines override to e.g.
    /// `"tokio"` / `"moonpool"`.
    fn name() -> &'static str
    where
        Self: Sized,
    {
        std::any::type_name::<Self>()
    }

    /// Spawn an async future on the engine's executor. Returns a cancel-
    /// safe [`Self::TaskHandle`]. Tokio wraps [`::tokio::spawn`]; moonpool
    /// delegates through its `Providers::TaskProvider` (`moonpool_core`).
    fn spawn<F>(fut: F) -> Self::TaskHandle
    where
        F: Future<Output = ()> + Send + 'static;

    /// Abort a spawned task. Idempotent: calling on an already-completed
    /// or already-aborted handle is a no-op.
    fn abort_task(handle: &mut Self::TaskHandle);

    /// Create a periodic timer with `period` between ticks. The first
    /// tick fires immediately (matches `tokio::time::interval`).
    fn new_interval(period: Duration) -> Self::Interval;

    /// Await the next tick. The returned future is `Send` and boxed so
    /// the caller can `.await` from a generic context without exposing
    /// the engine-specific timer shape.
    fn interval_tick<'a>(
        interval: &'a mut Self::Interval,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

    /// Engine-injected id provider for the façade's auto-generated
    /// subscription names (`Reader`, `TableView`). Tokio plugs in
    /// `Uuid::new_v4().simple()` (RFC 4122 random); moonpool plugs in
    /// a process-global atomic counter so deterministic-simulation runs
    /// produce stable, reproducible names. Callers that need fully
    /// deterministic names across processes should always pass an
    /// explicit subscription / reader name through the builder.
    fn random_subscription_suffix() -> String
    where
        Self: Sized;

    /// Engine-provided `OAuth2` [`magnetar_auth_oauth2::Clock`]. Used by
    /// callers that build a `ClientCredentialsFlow` from generic-engine
    /// code so the `OAuth2` cache deadlines flow through the same clock
    /// the engine uses everywhere else, instead of always landing on
    /// `Arc::new(SystemClock)` at the `OAuth2` builder boundary.
    ///
    /// Default is `Arc::new(magnetar_auth_oauth2::SystemClock)` —
    /// matches the `OAuth2` builder's own default. Engines wired into a
    /// virtual-time substrate (e.g. moonpool with `SimProviders`)
    /// override this to return a clock that reads the simulated time.
    #[cfg(feature = "auth-oauth2")]
    fn oauth2_clock() -> std::sync::Arc<dyn magnetar_auth_oauth2::Clock>
    where
        Self: Sized,
    {
        std::sync::Arc::new(magnetar_auth_oauth2::SystemClock)
    }
}

// ---------------------------------------------------------------------------
// Per-surface extension traits — ADR-0026 §D1.
//
// The Engine trait stays at ADR-0025 phase 1 (task + timer primitives).
// Each Pulsar surface family (transactions, reader, typed schemas, …)
// instead defines its own extension trait implemented by each runtime
// on its `Client` type. The façade then writes
//   `impl<E: Engine> PulsarClient<E> where E::ClientState: TransactionApi`
// and dispatches via `<E::ClientState as TransactionApi>::method(...)`.
//
// Why an extension trait, not a method on `Engine`:
//   - Engine primitives are bounded (spawn / timer / clock).
//   - Surface families grow with each PIP — putting them on `Engine` would mean every engine grows
//     with the Pulsar wire surface.
//   - Each engine implements only the families it supports. Moonpool can land Transaction before
//     TableView without the trait fattening.
//
// Sans-io: every trait method here returns a `Future` that resolves into
// a broker round-trip; the I/O lives in the runtime crates that
// implement these traits. `magnetar-proto` carries no `TransactionApi`
// dep — the protocol-level handshakes (`CommandNewTxn` →
// `CommandNewTxnResponse`, etc.) already live on `Connection` and are
// called via `shared.inner.lock(); conn.new_txn(...)` from inside the
// runtime impl. The trait surface stays free of tokio / mio / socket
// types. See [ADR-0004](../../specs/adr/0004-sans-io-protocol-core.md).
// ---------------------------------------------------------------------------

/// Pulsar transactions (PIP-31) — implemented by each runtime on its
/// `Client` type. Phase 1 of the D1 lift train.
///
/// The façade's [`crate::PulsarClient::new_transaction`] +
/// `commit_transaction` / `abort_transaction` + the two `register_*`
/// methods dispatch through this trait once
/// [`crate::PulsarClient<E>`]'s impl block carries the
/// `where E::ClientState: TransactionApi` bound. Subsequent surface
/// lifts (`Reader`, `TypedSchemas`, `TableView`, …) follow the same
/// template — one extension trait per family.
///
/// **Sans-io.** Methods are `async fn` returning `impl Future + Send +
/// '_`; no tokio / mio / socket types appear in the trait surface. The
/// runtime impl is responsible for driving the
/// [`magnetar_proto::Connection`] state machine and waking its driver.
///
/// See [ADR-0026](../../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
/// §D1 for the rationale (concrete-generic surfaces over GATs).
pub trait TransactionApi {
    /// Error surfaced by the runtime when a TC round-trip fails.
    /// Each runtime maps this onto its own client-error variant.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Open a new transaction at the broker-side transaction coordinator
    /// (`CommandNewTxn` → `CommandNewTxnResponse`). Returns the TC-assigned
    /// [`magnetar_proto::TxnId`] on success.
    fn new_txn(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::TxnId, Self::Error>> + Send + '_>>;

    /// Register a partition that the given transaction will write to
    /// (`CommandAddPartitionToTxn` → `CommandAddPartitionToTxnResponse`).
    fn add_partition_to_txn(
        &self,
        txn: magnetar_proto::TxnId,
        topic: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;

    /// Register a subscription that the given transaction will
    /// acknowledge on
    /// (`CommandAddSubscriptionToTxn` → `CommandAddSubscriptionToTxnResponse`).
    fn add_subscription_to_txn(
        &self,
        txn: magnetar_proto::TxnId,
        topic: String,
        subscription: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;

    /// Commit or abort an open transaction
    /// (`CommandEndTxn` → `CommandEndTxnResponse`). Returns the final
    /// transaction state reported by the TC.
    fn end_txn(
        &self,
        txn: magnetar_proto::TxnId,
        action: magnetar_proto::TxnAction,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::TxnState, Self::Error>> + Send + '_>>;
}

// `OutgoingMessage` + `IncomingMessage` currently live in
// `client.rs` (tokio-gated). Until those move to a feature-
// independent module, the `ProducerApi` / `ConsumerApi` traits also
// gate on the same set of features. Phase 4 of the façade lift will
// move the message types out of `client.rs` to drop this gate.

/// Pulsar producer wire surface — implemented by each runtime on its
/// `Producer` type. Foundational for the seven dependent façade lifts
/// (`Reader`, `TypedSchemas`, `MultiTopicsConsumer`, `PartitionedProducer`,
/// `PartitionedConsumer`, `PatternConsumer`, `TableView`) per ADR-0026 §D1.
///
/// **Sans-io.** Async methods return `Pin<Box<dyn Future + Send + '_>>`;
/// no tokio / mio / socket types appear in the surface. Each impl drives
/// the [`magnetar_proto::Connection`] state machine and wakes its
/// runtime-specific driver.
///
/// The method set here is **wire-level**: `send` (the only wire-bound
/// publish path), `flush` (drain pending), `is_closed`, `topic`, `name`,
/// `last_sequence_id`. Higher-level helpers (`send_bytes`, `stats`,
/// `batch_len`, `pending_count`, `get_schema`, `access_mode`) stay
/// engine-specific until a façade caller needs them — extending this
/// trait is a non-breaking change so the additive growth pattern is
/// safe.
#[cfg(feature = "tokio")]
pub trait ProducerApi: 'static + Send + Sync {
    /// Per-runtime client error type used by the wire calls.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Send a message. Resolves with the broker-assigned
    /// [`magnetar_proto::MessageId`].
    fn send(
        &self,
        msg: crate::OutgoingMessage,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::MessageId, Self::Error>> + Send + '_>>;

    /// Wait for every previously-queued send to be acknowledged or
    /// fail. Mirrors Java `Producer#flush()`.
    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;

    /// `true` once the producer has entered a terminal state.
    fn is_closed(&self) -> bool;

    /// `true` while the broker connection is up. Mirrors Java
    /// `Producer#isConnected`.
    fn is_connected(&self) -> bool;

    /// Topic this producer publishes to.
    fn topic(&self) -> String;

    /// Producer name advertised to the broker (broker-assigned if
    /// the user didn't set one).
    fn name(&self) -> String;

    /// Latest sequence id the producer assigned. Mirrors Java
    /// `Producer#getLastSequenceId`.
    fn last_sequence_id(&self) -> i64;

    /// Look up the broker-registered schema for the producer's topic
    /// (PIP-87). Used by
    /// `magnetar_proto::schema::AutoProduceBytesSchema` to warm its
    /// cache on first send. `version = None` asks for the current
    /// schema; pass `Some(schema_version_bytes)` to re-resolve.
    fn get_schema(
        &self,
        version: Option<bytes::Bytes>,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::pb::Schema, Self::Error>> + Send + '_>>;

    /// Cumulative producer-side counters. Mirrors Java
    /// `Producer#getStats`. Returns a zeroed snapshot if the
    /// producer handle is no longer registered.
    fn stats(&self) -> magnetar_proto::producer::ProducerStats;

    /// Consume the producer and tear down the broker-side resource
    /// (`CommandCloseProducer`). Mirrors Java `Producer#close`.
    /// Both runtime types implement close by consuming `self`; the
    /// trait exposes the same shape so generic façade surfaces (e.g.
    /// `PartitionedProducer<P>::close`) can fan out closes over a
    /// `Vec<P>`.
    fn close_owned(self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>>
    where
        Self: Sized;

    /// Wall-clock timestamp of the last broker disconnection observed
    /// by this producer's connection, or `None` if no disconnect has
    /// happened yet. Mirrors Java
    /// `Producer#getLastDisconnectedTimestamp`.
    fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime>;

    /// Compression codec this producer was opened with. Mirrors Java
    /// `ProducerImpl#conf.getCompressionType()`. Returns
    /// `CompressionKind::None` when the producer was opened without
    /// explicit compression.
    fn compression(&self) -> magnetar_proto::types::CompressionKind;

    /// Last sequence id the broker has acknowledged via
    /// `CommandSendReceipt`. Returns `-1` if no sends have been acked
    /// yet. Mirrors Java
    /// `Producer#getLastSequenceIdPublished`. Useful for
    /// resume-from-checkpoint flows.
    fn last_sequence_id_published(&self) -> i64;

    /// Number of in-flight sends (queued and not yet acked by the
    /// broker). Mirrors the un-batched view of Java
    /// `ProducerStats#getPendingQueueSize`. Equivalent to
    /// `self.stats().pending_queue_size as usize` but spares the full
    /// stats snapshot.
    fn pending_count(&self) -> usize;

    /// Number of messages currently buffered in the batch container,
    /// waiting for the next flush cycle. Returns `0` when batching is
    /// disabled or the batch is empty.
    fn batch_len(&self) -> usize;

    /// Sum of payload bytes currently buffered in the batch container.
    fn batch_bytes(&self) -> usize;
}

/// Pulsar consumer wire surface — implemented by each runtime on its
/// `Consumer` type. Foundational alongside [`ProducerApi`] per
/// ADR-0026 §D1.
///
/// Same sans-io contract as [`ProducerApi`]. The method set covers
/// the wire-level subscription lifecycle: `receive`, the ack family
/// (`ack`, `ack_cumulative`, `ack_with_txn`, `ack_cumulative_with_txn`),
/// `negative_ack`, plus topic / subscription / `is_closed` accessors.
///
/// Pass-2 of the `MultiTopicsConsumer` / `PatternConsumer` lift extends
/// this trait with the queue/permits getters (`available_in_queue`,
/// `available_permits`, `has_received_any_message`, `has_reached_end_of_topic`,
/// `is_paused`, `is_inactive`), the DLQ helpers (`drain_dead_letter`,
/// `republish_dead_letters`, `reconsume_later`,
/// `reconsume_later_with_properties`), the receive-batch family
/// (`receive_with_timeout`, `receive_batch`, `receive_batch_with_bytes_cap`),
/// flow control (`pause`, `resume`), and the remaining seek primitives
/// (`seek_to_message`, `seek_to_timestamp`). The `unsubscribe` method now
/// carries the PIP-313 `force: bool` flag so the trait matches the runtime
/// signatures verbatim.
///
/// The associated [`Self::Producer`] type ties each engine's `Consumer` to
/// its matching `Producer`, letting [`Self::republish_dead_letters`] and the
/// [`Self::reconsume_later`] family accept a runtime-typed producer reference
/// at the trait level without re-introducing a tokio-only carve-out.
#[cfg(feature = "tokio")]
pub trait ConsumerApi: 'static + Send + Sync {
    /// Per-runtime client error type used by the wire calls.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Matched runtime producer used by the DLQ + retry helpers.
    /// Each runtime ties this to its own `Producer` (tokio →
    /// [`magnetar_runtime_tokio::Producer`]; moonpool →
    /// `magnetar_runtime_moonpool::Producer<P>`) so
    /// [`Self::republish_dead_letters`] /
    /// [`Self::reconsume_later`] /
    /// [`Self::reconsume_later_with_properties`] dispatch through the
    /// trait without a tokio-only carve-out.
    type Producer: ProducerApi<Error = Self::Error>;

    /// Receive the next message. Resolves once the broker has
    /// delivered an entry. Returns the
    /// [`magnetar_proto::IncomingMessage`] surfaced by the state
    /// machine; callers that prefer the façade-side
    /// [`crate::IncomingMessage`] (with computed accessors) can call
    /// `.into()` on the result.
    fn receive(
        &self,
    ) -> Pin<
        Box<dyn Future<Output = Result<magnetar_proto::IncomingMessage, Self::Error>> + Send + '_>,
    >;

    /// Acknowledge `message_id` individually. Mirrors Java
    /// `Consumer#acknowledge(MessageId)`.
    fn ack(
        &self,
        message_id: magnetar_proto::MessageId,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;

    /// Acknowledge all messages up to and including `message_id`.
    /// Mirrors Java `Consumer#acknowledgeCumulative(MessageId)`.
    fn ack_cumulative(
        &self,
        message_id: magnetar_proto::MessageId,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;

    /// Negatively acknowledge `message_id`. Triggers a redelivery
    /// after the configured `nackRedeliveryBackoff`. Mirrors Java
    /// `Consumer#negativeAcknowledge`.
    fn negative_ack(&self, message_id: magnetar_proto::MessageId);

    /// Ask the broker for the topic's last-published message id.
    /// Mirrors Java `Consumer#getLastMessageId`.
    fn last_message_id(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::MessageId, Self::Error>> + Send + '_>>;

    /// `true` if the broker has at least one message strictly past
    /// `cursor`. Mirrors Java `Consumer#hasMessageAvailable` (with a
    /// caller-supplied cursor variant).
    fn has_message_after(
        &self,
        cursor: magnetar_proto::MessageId,
    ) -> Pin<Box<dyn Future<Output = Result<bool, Self::Error>> + Send + '_>>;

    /// Look up the broker-registered schema for the consumer's topic
    /// (PIP-87). Used by
    /// `magnetar_proto::schema::AutoConsumeSchema` to warm its cache
    /// on first receive. `version = None` asks for the current schema;
    /// pass `Some(schema_version_bytes)` to re-resolve.
    fn get_schema(
        &self,
        version: Option<bytes::Bytes>,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::pb::Schema, Self::Error>> + Send + '_>>;

    /// Topic this consumer is subscribed to.
    fn topic(&self) -> String;

    /// Subscription name this consumer holds.
    fn subscription(&self) -> String;

    /// Broker-assigned consumer name. Empty string when not yet known.
    /// Mirrors Java `Consumer#getConsumerName`.
    fn name(&self) -> String;

    /// `true` once the consumer has entered a terminal state.
    fn is_closed(&self) -> bool;

    /// `true` while the broker connection is up. Mirrors Java
    /// `Consumer#isConnected`.
    fn is_connected(&self) -> bool;

    /// Cumulative consumer-side counters. Mirrors Java
    /// `Consumer#getStats`. Returns a zeroed snapshot if the consumer
    /// handle is no longer registered.
    fn stats(&self) -> magnetar_proto::consumer::ConsumerStats;

    /// Wall-clock timestamp of the last broker disconnection observed
    /// by this consumer's connection, or `None` if no disconnect has
    /// happened yet. Mirrors Java
    /// `Consumer#getLastDisconnectedTimestamp`.
    fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime>;

    /// Ask the broker to redeliver every unacknowledged message on
    /// this consumer. Mirrors Java
    /// `Consumer#redeliverUnacknowledgedMessages`.
    fn redeliver_unacked(&self);

    /// Negatively acknowledge a single message with an explicit
    /// per-message redelivery delay. PIP-37 backoff variant.
    fn negative_ack_with_delay(
        &self,
        message_id: magnetar_proto::MessageId,
        delay: std::time::Duration,
    );

    /// Tear down this consumer's subscription on the broker. Mirrors
    /// Java `Consumer#unsubscribe`. `force=true` selects the PIP-313
    /// destructive variant that detaches every other attached consumer
    /// on the same subscription; `force=false` (Java default) keeps the
    /// cursor in place when other consumers are still attached.
    fn unsubscribe(
        &self,
        force: bool,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;

    /// Seek to the earliest available message. Mirrors Java
    /// `Consumer#seek(MessageId.earliest)`.
    fn seek_to_earliest(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;

    /// Seek to the latest available message. Mirrors Java
    /// `Consumer#seek(MessageId.latest)`.
    fn seek_to_latest(&self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;

    /// Seek to an explicit message id. Mirrors Java
    /// `Consumer#seek(MessageId)`.
    fn seek_to_message(
        &self,
        message_id: magnetar_proto::MessageId,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;

    /// Seek to a publish-time deadline (broker-side wall clock, ms
    /// since epoch). Mirrors Java `Consumer#seek(long)`.
    fn seek_to_timestamp(
        &self,
        publish_time_ms: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;

    /// Stop automatic flow refills. Mirrors Java `Consumer#pause` —
    /// already-issued permits keep draining, no new FLOW frames are
    /// emitted until [`Self::resume`].
    fn pause(&self);

    /// Re-enable automatic flow refills. Mirrors Java `Consumer#resume`.
    fn resume(&self);

    /// Number of messages currently buffered in the per-consumer
    /// receiver queue, waiting for a `receive()` call. Mirrors Java
    /// `Consumer#getNumMessagesInQueue`.
    fn available_in_queue(&self) -> usize;

    /// Outstanding dispatch permits the consumer has granted the broker
    /// (messages it has authorised the broker to push without an
    /// explicit `CommandFlow`). Mirrors Java
    /// `ConsumerBase#getAvailablePermits`.
    fn available_permits(&self) -> u32;

    /// `true` once the consumer has received at least one message since
    /// opening. Mirrors Java `Consumer#hasReceivedAnyMessage`.
    fn has_received_any_message(&self) -> bool;

    /// `true` once the broker has indicated end-of-topic for this
    /// consumer (no more messages will be dispatched). Mirrors Java
    /// `Consumer#hasReachedEndOfTopic`.
    fn has_reached_end_of_topic(&self) -> bool;

    /// `true` while [`Self::pause`] has flipped the consumer's flow
    /// refills off. Mirrors Java `Consumer#isPaused`.
    fn is_paused(&self) -> bool;

    /// Mirrors Java `Consumer#isInactive`. Returns `true` once the
    /// consumer has reached end-of-topic (no more messages will be
    /// dispatched). Note: a closed consumer is not represented as
    /// "inactive" here.
    fn is_inactive(&self) -> bool;

    /// Drain every message the state machine has flagged as dead-letter
    /// (redelivery count greater than the configured
    /// `max_redeliver_count`). The caller is responsible for
    /// republishing them to the DLQ topic (or using
    /// [`Self::republish_dead_letters`] for the transparent path).
    fn drain_dead_letter(&self) -> Vec<magnetar_proto::IncomingMessage>;

    /// Receive the next message bounded by `timeout`. Resolves with
    /// `Ok(None)` when the deadline elapses with no message. Mirrors
    /// Java `Consumer#receive(int, TimeUnit)`.
    fn receive_with_timeout(&self, timeout: Duration) -> ReceiveOptFut<'_, Self>;

    /// Receive up to `max_messages` messages in one call. Waits up to
    /// `max_wait` for the first message, then drains additional
    /// already-buffered messages without further waiting. Mirrors Java
    /// `Consumer#batchReceive`.
    fn receive_batch(&self, max_messages: usize, max_wait: Duration) -> ReceiveBatchFut<'_, Self>;

    /// Same as [`Self::receive_batch`] but stops once the accumulated
    /// payload size would exceed `max_bytes`. Mirrors Java
    /// `BatchReceivePolicy` with all three caps (count, bytes, wait).
    fn receive_batch_with_bytes_cap(
        &self,
        max_messages: usize,
        max_bytes: usize,
        max_wait: Duration,
    ) -> ReceiveBatchFut<'_, Self>;

    /// Drain the per-consumer dead-letter queue and republish every
    /// entry via `dlq_producer`, preserving the message's metadata.
    /// Acks each original after a successful republish. Returns the
    /// number of messages republished.
    fn republish_dead_letters<'a>(
        &'a self,
        dlq_producer: &'a Self::Producer,
    ) -> Pin<Box<dyn Future<Output = Result<usize, Self::Error>> + Send + 'a>>;

    /// Republish a single message via `retry_producer` with a
    /// `delay`-bounded deadline, then ack the original. Mirrors Java
    /// `Consumer#reconsumeLater(Message, long, TimeUnit)`.
    fn reconsume_later<'a>(
        &'a self,
        retry_producer: &'a Self::Producer,
        msg: magnetar_proto::IncomingMessage,
        delay: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>>;

    /// Same as [`Self::reconsume_later`] but lets the caller stamp
    /// additional custom properties on the republished message. Mirrors
    /// Java's properties-aware reconsumeLater overload.
    fn reconsume_later_with_properties<'a>(
        &'a self,
        retry_producer: &'a Self::Producer,
        msg: magnetar_proto::IncomingMessage,
        custom_properties: Vec<(String, String)>,
        delay: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>>;

    /// Consume the consumer and tear down the broker-side resource
    /// (`CommandCloseConsumer`). Mirrors Java `Consumer#close`. Both
    /// runtime types implement close by consuming `self`.
    fn close_owned(self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>>
    where
        Self: Sized;

    /// Fire-and-forget individual ack into the consumer's
    /// ack-grouping tracker (opt-in via
    /// `ConsumerBuilder::ack_group_time`). The state machine flushes
    /// the tracker after `ack_group_time` elapses, emitting one
    /// coalesced `CommandAck`. With no tracker configured, the proto
    /// layer falls back to a synchronous immediate `CommandAck` so the
    /// message is never silently dropped. Mirrors Java's
    /// `acknowledgmentGroupTime` path.
    fn ack_grouped(&self, message_id: magnetar_proto::MessageId);

    /// Fire-and-forget cumulative ack into the consumer's ack-grouping
    /// tracker. See [`Self::ack_grouped`] for the semantics.
    fn ack_grouped_cumulative(&self, message_id: magnetar_proto::MessageId);

    /// Acknowledge `message_id` as part of a Pulsar transaction
    /// (PIP-31). The ack only takes effect once the transaction
    /// commits. Mirrors Java
    /// `Consumer#acknowledgeAsync(MessageId, Transaction)`.
    fn ack_with_txn(
        &self,
        message_id: magnetar_proto::MessageId,
        txn_id: magnetar_proto::TxnId,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;

    /// Cumulative ack as part of a Pulsar transaction (PIP-31).
    /// Mirrors Java
    /// `Consumer#acknowledgeCumulativeAsync(MessageId, Transaction)`.
    fn ack_cumulative_with_txn(
        &self,
        message_id: magnetar_proto::MessageId,
        txn_id: magnetar_proto::TxnId,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;
}

/// PIP-145 `TopicListChanged` delta surfaced through
/// [`BrokerMetadataApi::poll_topic_list_change`]. Façade-side analogue
/// of the per-runtime `TopicListChange` structs — each runtime impl
/// converts its own delta into this engine-agnostic shape so generic
/// surfaces (`PatternConsumer<C>::update`) can reconcile without
/// touching runtime-specific types.
#[cfg(feature = "tokio")]
#[derive(Debug, Clone)]
pub struct TopicListChange {
    /// Topics that newly match the pattern.
    pub added: Vec<String>,
    /// Topics that no longer match the pattern.
    pub removed: Vec<String>,
}

/// Engine-side broker metadata lookups used by
/// [`crate::PartitionedConsumerBuilder`] and
/// [`crate::PatternConsumerBuilder`] (alongside other partition-aware
/// surfaces). Each runtime implements this on its concrete `Client`
/// type.
///
/// Same sans-io contract as [`SubscribeApi`] — async methods return
/// `Pin<Box<dyn Future + Send + '_>>`; the impl drives the
/// `magnetar_proto::Connection` state machine.
#[cfg(feature = "tokio")]
pub trait BrokerMetadataApi: 'static + Send + Sync {
    /// Per-runtime client error type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Query the broker for the partition count of `topic`. Returns
    /// `0` for non-partitioned topics. Mirrors Java
    /// `PulsarClient#getPartitionsForTopic`.
    fn partitioned_topic_metadata<'a>(
        &'a self,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<u32, Self::Error>> + Send + 'a>>;

    /// Subscribe to a topic-list watcher and return the initial topic
    /// snapshot for the given namespace + regex pattern (PIP-145).
    fn watch_topic_list<'a>(
        &'a self,
        namespace: &'a str,
        pattern: &'a str,
    ) -> WatchTopicListFut<'a, Self>;

    /// Drain the next pending `TopicListChanged` delta from the
    /// connection's PIP-145 buffer, if any. Returns `None` when no
    /// deltas are pending. Used by `PatternConsumer::update` to
    /// reconcile its child set.
    fn poll_topic_list_change(&self) -> Option<TopicListChange>;
}

/// Engine-side subscribe surface used by `ConsumerBuilder<E>` and the
/// other consumer-spawning façade surfaces (`MultiTopicsConsumer`,
/// `PatternConsumer`, `Reader`). Each runtime implements this on its
/// concrete `Client` type with the runtime-specific `Consumer` type
/// surfaced via the associated `Consumer` type.
///
/// Per ADR-0026 §D1: this is the next sub-PR after the per-surface
/// lifts. Lifting `ConsumerBuilder<E>` to dispatch through this
/// trait unblocks the impl-body lifts on the four phantom-lifted
/// surfaces (`TypedSchemas`, `MultiTopicsConsumer` /
/// `PartitionedConsumer`, `PatternConsumer`).
#[cfg(feature = "tokio")]
pub trait SubscribeApi: 'static + Send + Sync {
    /// Concrete consumer type each runtime returns. Required to
    /// implement [`ConsumerApi`] so generic surfaces can dispatch
    /// further methods through that trait.
    type Consumer: ConsumerApi;
    /// Runtime client error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Issue a `CommandSubscribe` and resolve with the broker-side
    /// `CommandSuccess` correlated with the request id (subscribe
    /// ack). After this resolves the state machine has a fresh
    /// per-consumer queue and the initial FLOW frame has been queued
    /// for the driver.
    fn subscribe(&self, req: magnetar_proto::SubscribeRequest) -> SubscribeFut<'_, Self>;
}

/// Helper alias: `SubscribeApi::subscribe` future return type.
#[cfg(feature = "tokio")]
pub type SubscribeFut<'a, S> = Pin<
    Box<
        dyn Future<Output = Result<<S as SubscribeApi>::Consumer, <S as SubscribeApi>::Error>>
            + Send
            + 'a,
    >,
>;

/// Helper alias: `ConsumerApi::receive_with_timeout` future return type.
#[cfg(feature = "tokio")]
pub type ReceiveOptFut<'a, C> = Pin<
    Box<
        dyn Future<
                Output = Result<Option<magnetar_proto::IncomingMessage>, <C as ConsumerApi>::Error>,
            > + Send
            + 'a,
    >,
>;

/// Helper alias: `ConsumerApi::receive_batch` / `receive_batch_with_bytes_cap`
/// future return type.
#[cfg(feature = "tokio")]
pub type ReceiveBatchFut<'a, C> = Pin<
    Box<
        dyn Future<Output = Result<Vec<magnetar_proto::IncomingMessage>, <C as ConsumerApi>::Error>>
            + Send
            + 'a,
    >,
>;

/// Helper alias: `BrokerMetadataApi::watch_topic_list` future return type.
#[cfg(feature = "tokio")]
pub type WatchTopicListFut<'a, B> =
    Pin<Box<dyn Future<Output = Result<Vec<String>, <B as BrokerMetadataApi>::Error>> + Send + 'a>>;

/// Engine-side producer-creation surface used by `ProducerBuilder<E>`
/// and `PartitionedProducer<E>`. Same shape as [`SubscribeApi`] for
/// the producer side.
#[cfg(feature = "tokio")]
pub trait CreateProducerApi: 'static + Send + Sync {
    /// Concrete producer type each runtime returns.
    type Producer: ProducerApi;
    /// Runtime client error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Issue a `CommandProducer` and resolve with
    /// `CommandProducerSuccess` correlated with the request id.
    fn open_producer(
        &self,
        req: magnetar_proto::CreateProducerRequest,
    ) -> OpenProducerFut<'_, Self>;
}

/// Helper alias: `CreateProducerApi::open_producer` future return type.
#[cfg(feature = "tokio")]
pub type OpenProducerFut<'a, P> = Pin<
    Box<
        dyn Future<
                Output = Result<
                    <P as CreateProducerApi>::Producer,
                    <P as CreateProducerApi>::Error,
                >,
            > + Send
            + 'a,
    >,
>;

// ---------------------------------------------------------------------------
// PIP-4 per-engine encryption extension traits (docs/follow-ups.md §2 WAVE 1).
//
// Tokio defines `magnetar_runtime_tokio::MessageEncryptor` and
// `magnetar_runtime_tokio::MessageDecryptor` for its own producer / consumer
// surfaces. The façade builders historically stored
// `Option<Arc<dyn magnetar_runtime_tokio::MessageEncryptor>>` directly,
// hard-locking them to the tokio engine. The two extension traits below
// lift that storage off tokio: each engine declares its own concrete
// encryptor / decryptor type, the façade stores
// `Option<<E as MessageEncryptorApi>::Encryptor>` instead, and engines
// that don't support encryption can supply a zero-sized stub
// (e.g. moonpool's `NoEncryption`).
//
// The traits live on the engine marker (not on `Client`) because the
// encryptor identity is engine-global config rather than per-connection
// state. The associated `Encryptor` / `Decryptor` types are `Clone +
// Send + Sync + 'static` so the builders can pass them to the runtime's
// `open_producer_with` / `subscribe_with` without further bounds churn.
//
// Sans-io: the traits define types only. Real encryption happens in the
// runtime crates that supply the concrete types (`magnetar-runtime-tokio`
// today; moonpool ships a no-op stub).
// ---------------------------------------------------------------------------

/// Engine-side message-encryptor selection. Each engine declares its own
/// concrete encryptor type; the façade's `ProducerBuilder` stores
/// `Option<E::Encryptor>` (engine-typed) instead of an
/// `Arc<dyn magnetar_runtime_tokio::MessageEncryptor>` (tokio-locked).
///
/// Implemented on the engine marker ([`TokioEngine`] / [`MoonpoolEngine<P>`]).
/// Tokio plugs in `Arc<dyn magnetar_runtime_tokio::MessageEncryptor>`;
/// moonpool plugs in [`NoEncryption`] (a zero-sized stub) until real
/// moonpool-side encryption lands. The choice of `Encryptor: Clone` lets
/// the façade fan out the encryptor across child producers in
/// `PartitionedProducer`.
pub trait MessageEncryptorApi {
    /// Concrete per-engine encryptor type. `Clone + Send + Sync + 'static`
    /// so it survives spawn boundaries and fan-out into child producers.
    type Encryptor: Clone + Send + Sync + 'static;
}

/// Engine-side message-decryptor selection. Mirror of
/// [`MessageEncryptorApi`] for the consume path. Implemented on the
/// engine marker.
pub trait MessageDecryptorApi {
    /// Concrete per-engine decryptor type. `Clone + Send + Sync + 'static`.
    type Decryptor: Clone + Send + Sync + 'static;
}

// ---------------------------------------------------------------------------
// PIP-460 scalable topics (ADR-0031, experimental). The `ScalableTopicsApi`
// extension trait follows the same ADR-0026 §D1 pattern as `TransactionApi`:
// defined here, implemented by each runtime on its `Client` type (which is the
// engine's `ClientState`), dispatched through
//   `impl<E: Engine> PulsarClient<E> where E::ClientState: ScalableTopicsApi`.
// Gated on `feature = "scalable-topics"` so the default surface is unchanged.
// ---------------------------------------------------------------------------

/// **Experimental** (PIP-460, ADR-0031). Engine-side scalable-topic hooks —
/// implemented by each runtime on its `Client` type. The façade's
/// [`crate::scalable::StreamConsumer`] dispatches through this trait once
/// [`crate::PulsarClient<E>`] carries the
/// `where E::ClientState: ScalableTopicsApi` bound.
///
/// **Sans-io.** Async methods return `Pin<Box<dyn Future + Send + '_>>`; no
/// tokio / mio / socket types appear in the surface. Each impl drives the
/// [`magnetar_proto::Connection`] scalable entries (`send_scalable_topic_lookup`,
/// `open_dag_watch`, `close_dag_watch`) and reads the driver-drained events.
#[cfg(all(feature = "tokio", feature = "scalable-topics"))]
pub trait ScalableTopicsApi: 'static + Send + Sync {
    /// Per-runtime client error type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Resolve a `topic://...` scalable topic: lookup → segment DAG snapshot +
    /// controller broker + lookup token.
    fn scalable_topic_lookup<'a>(
        &'a self,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ScalableLookup, Self::Error>> + Send + 'a>>;

    /// Open a DAG-watch session, seeded with the lookup snapshot + token.
    /// Returns the client-allocated watch session id.
    fn open_dag_watch(
        &self,
        topic: &str,
        lookup_token: u64,
        segments: Vec<magnetar_proto::SegmentDescriptor>,
    ) -> u64;

    /// Close a DAG-watch session.
    fn close_dag_watch(&self, watch_session_id: u64);

    /// Await the next scalable-topic event (DAG update / drop-on-change /
    /// close). Resolves `None` once the connection closes.
    fn next_scalable_event(
        &self,
    ) -> Pin<Box<dyn Future<Output = Option<ScalableEvent>> + Send + '_>>;
}

/// **Experimental** (PIP-460, ADR-0031). Engine-agnostic resolved
/// scalable-topic lookup surfaced through [`ScalableTopicsApi`]. Façade-side
/// analogue of each runtime's `ScalableLookup`.
#[cfg(all(feature = "tokio", feature = "scalable-topics"))]
#[derive(Debug, Clone)]
pub struct ScalableLookup {
    /// Controller broker to open the DAG-watch session against.
    pub controller_broker_url: String,
    /// Current DAG snapshot for the topic.
    pub segments: Vec<magnetar_proto::SegmentDescriptor>,
    /// Monotonic lookup token, echoed into the DAG-watch subscribe.
    pub lookup_token: u64,
}

/// **Experimental** (PIP-460, ADR-0031). Engine-agnostic scalable-topic event
/// surfaced through [`ScalableTopicsApi::next_scalable_event`]. Façade-side
/// analogue of each runtime's `ScalableEvent`.
#[cfg(all(feature = "tokio", feature = "scalable-topics"))]
#[derive(Debug, Clone)]
pub enum ScalableEvent {
    /// A scalable-topic lookup resolved into the current DAG.
    LookupResolved {
        /// Controller broker to open the DAG-watch session against.
        controller_broker_url: String,
        /// Current DAG snapshot.
        segments: Vec<magnetar_proto::SegmentDescriptor>,
        /// Monotonic lookup token.
        lookup_token: u64,
    },
    /// A DAG-watch session received and applied an update.
    DagUpdated {
        /// Watch session id.
        watch_session_id: u64,
        /// The applied delta.
        delta: magnetar_proto::DagDelta,
    },
    /// The segment DAG changed under a live consumer (drop-on-change).
    DagChangedDuringConsume {
        /// Watch session id whose DAG changed.
        watch_session_id: u64,
        /// Why the DAG changed.
        reason: magnetar_proto::DagChangeReason,
    },
    /// The DAG-watch session closed.
    DagWatchClosed {
        /// Watch session id that closed.
        watch_session_id: u64,
        /// Optional close reason.
        reason: Option<String>,
    },
}

/// Zero-sized stub for engines that don't yet wire real encryption.
/// Used by [`MoonpoolEngine`] as both `MessageEncryptorApi::Encryptor`
/// and `MessageDecryptorApi::Decryptor`. Constructing one is meaningless
/// — engines that hand a `NoEncryption` to the façade signal "encryption
/// not supported on this engine"; the builders' generic `.create()` /
/// `.subscribe()` paths ignore the field. Tokio-specialised builder
/// methods (`.create_with_encryption` / `.subscribe_with_decryption`)
/// remain available on the tokio engine for real encryption.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoEncryption;

// Per-engine storage for [`crate::PulsarClient<MoonpoolEngine<P>>`] is
// [`magnetar_runtime_moonpool::Client<P>`] directly — see
// `Engine::ClientState` above. This mirrors the tokio engine
// (`type ClientState = magnetar_runtime_tokio::Client`) so the existing
// `SubscribeApi` / `CreateProducerApi` / `ConsumerApi` / `ProducerApi`
// impls on the runtime `Client<P>` automatically satisfy the trait
// bounds the façade builders dispatch through, without a parallel
// state struct.

#[cfg(test)]
mod tests {
    // NOTE: We import the trait + marker types explicitly rather than
    // via `use super::*;`. The parent module exposes sibling `tokio` /
    // `moonpool` submodules whose names would shadow the external
    // `tokio` crate inside this test scope and break the
    // `#[::tokio::test]` macro expansions below.
    use super::Engine;
    #[cfg(feature = "moonpool")]
    use super::MoonpoolEngine;
    #[cfg(feature = "tokio")]
    use super::TokioEngine;

    #[cfg(feature = "tokio")]
    #[test]
    fn tokio_engine_implements_engine() {
        fn takes_engine<E: Engine>() -> &'static str {
            E::name()
        }
        assert_eq!(takes_engine::<TokioEngine>(), "tokio");
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn tokio_engine_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TokioEngine>();
    }

    #[cfg(feature = "moonpool")]
    #[test]
    fn moonpool_engine_implements_engine() {
        use moonpool_core::TokioProviders;
        fn takes_engine<E: Engine>() -> &'static str {
            E::name()
        }
        assert_eq!(takes_engine::<MoonpoolEngine<TokioProviders>>(), "moonpool");
    }

    #[cfg(feature = "moonpool")]
    #[test]
    fn moonpool_engine_is_send_sync() {
        use moonpool_core::TokioProviders;
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MoonpoolEngine<TokioProviders>>();
    }

    #[cfg(all(feature = "tokio", feature = "auth-oauth2"))]
    #[test]
    fn tokio_engine_oauth2_clock_is_monotonic() {
        let clock = <TokioEngine as Engine>::oauth2_clock();
        let a = clock.now();
        let b = clock.now();
        assert!(b >= a, "OAuth2 clock must be monotonic");
    }

    #[cfg(all(feature = "moonpool", feature = "auth-oauth2"))]
    #[test]
    fn moonpool_engine_oauth2_clock_is_monotonic() {
        use moonpool_core::TokioProviders;
        let clock = <MoonpoolEngine<TokioProviders> as Engine>::oauth2_clock();
        let a = clock.now();
        let b = clock.now();
        assert!(b >= a, "OAuth2 clock must be monotonic");
    }

    // -------------------------------------------------------------
    // ADR-0025 phase 1: task + timer primitive smoke tests. One pair
    // per engine — keeps the per-engine test count balanced even
    // though the new primitives don't yet have façade callers.

    // Note: the tests below reference the external `tokio` crate via the
    // absolute `::tokio::` path because this module has a sibling
    // `tokio` submodule (carrying the `TokioEngine` impl) — the
    // unqualified `tokio` identifier would otherwise resolve to that
    // submodule rather than to the crate.

    #[cfg(feature = "tokio")]
    #[::tokio::test(flavor = "current_thread", start_paused = true)]
    async fn tokio_engine_spawn_and_abort_round_trip() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        let handle = <TokioEngine as Engine>::spawn(async move {
            c.fetch_add(1, Ordering::SeqCst);
        });
        // Drive the spawned task once.
        ::tokio::task::yield_now().await;
        // Awaiting the JoinHandle works on a non-aborted task.
        handle.await.expect("spawned task ran to completion");
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Spawn a second task that we abort before it can increment.
        let c2 = counter.clone();
        let mut handle2 = <TokioEngine as Engine>::spawn(async move {
            // Sleep forever — abort wins.
            ::tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            c2.fetch_add(1, Ordering::SeqCst);
        });
        <TokioEngine as Engine>::abort_task(&mut handle2);
        // Second abort is a no-op.
        <TokioEngine as Engine>::abort_task(&mut handle2);
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "aborted task must not run its body",
        );
    }

    #[cfg(feature = "tokio")]
    #[::tokio::test(flavor = "current_thread", start_paused = true)]
    async fn tokio_engine_interval_first_tick_is_immediate() {
        use std::time::Duration;

        let mut interval = <TokioEngine as Engine>::new_interval(Duration::from_secs(10));
        let start = ::tokio::time::Instant::now();
        <TokioEngine as Engine>::interval_tick(&mut interval).await;
        // First tick fires immediately per the tokio interval contract.
        assert_eq!(
            ::tokio::time::Instant::now().duration_since(start),
            Duration::ZERO,
            "first interval tick must fire immediately on tokio",
        );
        // Second tick waits for the period.
        <TokioEngine as Engine>::interval_tick(&mut interval).await;
        assert!(
            ::tokio::time::Instant::now().duration_since(start) >= Duration::from_secs(10),
            "second tick must wait one period",
        );
    }

    #[cfg(feature = "moonpool")]
    #[::tokio::test(flavor = "current_thread", start_paused = true)]
    async fn moonpool_engine_spawn_and_abort_round_trip() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use moonpool_core::TokioProviders;

        type E = MoonpoolEngine<TokioProviders>;

        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        let handle = <E as Engine>::spawn(async move {
            c.fetch_add(1, Ordering::SeqCst);
        });
        ::tokio::task::yield_now().await;
        handle.await.expect("spawned task ran to completion");
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        let c2 = counter.clone();
        let mut handle2 = <E as Engine>::spawn(async move {
            ::tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            c2.fetch_add(1, Ordering::SeqCst);
        });
        <E as Engine>::abort_task(&mut handle2);
        <E as Engine>::abort_task(&mut handle2);
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "aborted task must not run its body",
        );
    }

    #[cfg(feature = "moonpool")]
    #[::tokio::test(flavor = "current_thread", start_paused = true)]
    async fn moonpool_engine_interval_first_tick_is_immediate() {
        use std::time::Duration;

        use moonpool_core::TokioProviders;

        type E = MoonpoolEngine<TokioProviders>;

        let mut interval = <E as Engine>::new_interval(Duration::from_secs(10));
        let start = ::tokio::time::Instant::now();
        <E as Engine>::interval_tick(&mut interval).await;
        assert_eq!(
            ::tokio::time::Instant::now().duration_since(start),
            Duration::ZERO,
            "first interval tick must fire immediately on moonpool",
        );
        <E as Engine>::interval_tick(&mut interval).await;
        assert!(
            ::tokio::time::Instant::now().duration_since(start) >= Duration::from_secs(10),
            "second tick must wait one period",
        );
    }
}
