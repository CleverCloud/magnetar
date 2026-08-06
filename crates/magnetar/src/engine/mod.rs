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
//! ADR-0019 §Decision contract.
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

    /// Clone of this producer's live send-latency histogram (issue #347).
    /// `None` if the producer handle is no longer registered or the
    /// histogram was never initialised. Used by
    /// [`crate::PartitionedProducer::aggregate_stats`] to merge several
    /// producers' distributions via
    /// [`magnetar_proto::producer::ProducerStats::fold`] — `stats()` alone
    /// only carries the three pre-computed percentiles, not the
    /// distribution a sound merge needs.
    fn send_latency_histogram(&self) -> Option<hdrhistogram::Histogram<u64>>;

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

    /// Clone of this consumer's live receive-latency histogram (issue
    /// #347). `None` if the consumer handle is no longer registered or the
    /// histogram was never initialised. Used by
    /// [`crate::MultiTopicsConsumer::aggregate_stats`] (and, via the
    /// `PartitionedConsumer` alias, `PartitionedConsumer::aggregate_stats`)
    /// to merge several consumers' distributions via
    /// [`magnetar_proto::consumer::ConsumerStats::fold`] — `stats()` alone
    /// only carries the three pre-computed percentiles, not the
    /// distribution a sound merge needs.
    fn receive_latency_histogram(&self) -> Option<hdrhistogram::Histogram<u64>>;

    /// Last broker-reported Failover active/standby state (issue #348).
    /// `None` until the first `CommandActiveConsumerChange` lands for this
    /// consumer (e.g. a `Shared` / `Exclusive` subscription never receives
    /// it). Mirrors the implicit state Java's `ConsumerEventListener`
    /// callbacks track.
    fn is_active(&self) -> Option<bool>;

    /// Resolve the next not-yet-observed Failover active/standby transition.
    /// Backs [`crate::spawn_consumer_event_listener`] — the poller awaits
    /// this in a loop and turns each `Ok(bool)` into a
    /// [`crate::ConsumerEvent::BecameActive`] /
    /// [`crate::ConsumerEvent::BecameInactive`] callback. Resolves the same
    /// error [`Self::receive`] does once the consumer reaches a terminal
    /// state with no unobserved transition buffered.
    fn next_active_change(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<bool, Self::Error>> + Send + '_>>;

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

    /// Consume the consumer and reliably tear down the broker-side resource
    /// (`CommandCloseConsumer`). Mirrors Java `Consumer#close`: both runtime
    /// implementations await the broker acknowledgement and return close
    /// errors through the future.
    ///
    /// Dropping the final runtime consumer clone is a distinct best-effort
    /// safety net that cannot report acknowledgement or failure. Generic
    /// code that requires confirmed release must call and await this method.
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

/// One setup-operation context shared across every caller-visible stage.
///
/// The pinned timer enforces the total deadline, while `last_broker_error`
/// preserves the newest broker diagnostic across metadata, lookup, routing,
/// and attachment so a later timeout does not replace it with a generic error.
#[cfg(feature = "tokio")]
#[doc(hidden)]
pub struct OperationDeadline {
    timer: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    last_broker_error: Option<(i32, String)>,
}

type OperationDeadlineParts<'a> = (
    Pin<&'a mut (dyn Future<Output = ()> + Send)>,
    &'a mut Option<(i32, String)>,
);

#[cfg(feature = "tokio")]
impl core::fmt::Debug for OperationDeadline {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("OperationDeadline").finish_non_exhaustive()
    }
}

#[cfg(feature = "tokio")]
impl OperationDeadline {
    /// Build a façade deadline from an engine-provided timer.
    #[doc(hidden)]
    pub fn new(timer: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) -> Self {
        Self {
            timer,
            last_broker_error: None,
        }
    }

    fn never() -> Self {
        Self::new(Box::pin(std::future::pending()))
    }

    /// Reborrow the pinned engine timer.
    #[doc(hidden)]
    pub fn timer(&mut self) -> Pin<&mut (dyn Future<Output = ()> + Send)> {
        self.timer.as_mut()
    }

    /// Reborrow both parts for a built-in runtime operation.
    pub(crate) fn parts(&mut self) -> OperationDeadlineParts<'_> {
        (self.timer.as_mut(), &mut self.last_broker_error)
    }
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

    /// Create a provider-correct setup timer.
    ///
    /// The default preserves compatibility for downstream custom engines:
    /// their established operation method remains authoritative until they
    /// opt into the deadline-aware companion below.
    #[doc(hidden)]
    fn new_metadata_operation_deadline(&self) -> OperationDeadline {
        OperationDeadline::never()
    }

    /// Deadline-aware metadata lookup used by built-in composite builders.
    #[doc(hidden)]
    fn partitioned_topic_metadata_with_deadline<'a>(
        &'a self,
        topic: &'a str,
        _deadline: &'a mut OperationDeadline,
    ) -> Pin<Box<dyn Future<Output = Result<u32, Self::Error>> + Send + 'a>> {
        self.partitioned_topic_metadata(topic)
    }

    /// Subscribe to a topic-list watcher and return the initial topic
    /// snapshot for the given namespace + regex pattern (PIP-145).
    fn watch_topic_list<'a>(
        &'a self,
        namespace: &'a str,
        pattern: &'a str,
    ) -> WatchTopicListFut<'a, Self>;

    /// Deadline-aware topic-list snapshot used by built-in composite builders.
    #[doc(hidden)]
    fn watch_topic_list_with_deadline<'a>(
        &'a self,
        namespace: &'a str,
        pattern: &'a str,
        _deadline: &'a mut OperationDeadline,
    ) -> WatchTopicListFut<'a, Self> {
        self.watch_topic_list(namespace, pattern)
    }

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

    /// Create a provider-correct setup timer.
    #[doc(hidden)]
    fn new_subscribe_operation_deadline(&self) -> OperationDeadline {
        OperationDeadline::never()
    }

    /// Deadline-aware subscribe used by built-in composite builders.
    #[doc(hidden)]
    fn subscribe_with_deadline<'a>(
        &'a self,
        req: magnetar_proto::SubscribeRequest,
        _deadline: &'a mut OperationDeadline,
    ) -> SubscribeFut<'a, Self> {
        self.subscribe(req)
    }
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

    /// Create a provider-correct setup timer.
    #[doc(hidden)]
    fn new_producer_operation_deadline(&self) -> OperationDeadline {
        OperationDeadline::never()
    }

    /// Deadline-aware producer-open used by built-in composite builders.
    #[doc(hidden)]
    fn open_producer_with_deadline<'a>(
        &'a self,
        req: magnetar_proto::CreateProducerRequest,
        _deadline: &'a mut OperationDeadline,
    ) -> OpenProducerFut<'a, Self> {
        self.open_producer(req)
    }
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
// PIP-4 per-engine encryption extension traits.
//
// Tokio defines `magnetar_runtime_tokio::MessageEncryptor` and
// `magnetar_runtime_tokio::MessageDecryptor` for its own producer / consumer
// surfaces. The façade builders historically stored
// `Option<Arc<dyn magnetar_runtime_tokio::MessageEncryptor>>` directly,
// hard-locking them to the tokio engine. The two extension traits below
// lift that storage off tokio: each engine declares its own concrete
// encryptor / decryptor type, the façade stores
// `Option<<E as MessageEncryptorApi>::Encryptor>` instead. Both runtime
// engines now ship the PIP-4 bridge, so each resolves the associated type
// to its own `Arc<dyn …MessageEncryptor>` / `…MessageDecryptor`. The
// zero-sized [`NoEncryption`] stub remains for any future engine that
// genuinely cannot drive on-the-wire encryption.
//
// The traits live on the engine marker (not on `Client`) because the
// encryptor identity is engine-global config rather than per-connection
// state. The associated `Encryptor` / `Decryptor` types are `Clone +
// Send + Sync + 'static` so the builders can pass them to the runtime's
// `open_producer_with` / `subscribe_with` without further bounds churn.
//
// Sans-io: the traits define types only. Real encryption happens in the
// runtime crates that supply the concrete types (`magnetar-runtime-tokio`
// and `magnetar-runtime-moonpool`).
// ---------------------------------------------------------------------------

/// Engine-side message-encryptor selection. Each engine declares its own
/// concrete encryptor type; the façade's `ProducerBuilder` stores
/// `Option<E::Encryptor>` (engine-typed) instead of an
/// `Arc<dyn magnetar_runtime_tokio::MessageEncryptor>` (tokio-locked).
///
/// Implemented on the engine marker ([`TokioEngine`] / [`MoonpoolEngine<P>`]).
/// Tokio plugs in `Arc<dyn magnetar_runtime_tokio::MessageEncryptor>`;
/// moonpool plugs in `Arc<dyn magnetar_runtime_moonpool::MessageEncryptor>`.
/// The choice of `Encryptor: Clone` lets the façade fan out the encryptor
/// across child producers in `PartitionedProducer`.
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
// PIP-460 scalable topics (experimental). Two deliberately separate adapters
// live here:
//
// - `ScalableTopicsApi` keeps the low-level lookup/watch surface used by the CLI and
//   protocol-facing callers.
// - `SegmentSubscriberApi` creates one owned, assignment-driven aggregate for the high-level
//   `scalable::StreamConsumer`. Its returned backend implements `StreamConsumerBackend` and owns
//   typed controller/child routes. It never drains the low-level client-global event queue.
//
// Both follow the ADR-0026 §D1 extension-trait pattern: the public runtime
// clients remain non-Clone while an operation can return the narrow owned
// capability needed after its opening borrow ends.
// ---------------------------------------------------------------------------

/// Runtime-neutral opening configuration for an assignment-driven scalable
/// stream consumer.
///
/// This type is public only so runtime adapters and downstream engine
/// implementations can satisfy [`SegmentSubscriberApi`]. Application callers
/// configure it through [`crate::scalable::StreamConsumerBuilder`]. In
/// particular, it deliberately contains no wire consumer id and no child
/// subscription-type selector.
#[cfg(all(feature = "tokio", feature = "scalable-topics"))]
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct StreamConsumerOptions {
    /// Canonical scalable parent topic requested by the application.
    pub topic: String,
    /// Subscription shared by every assigned segment child.
    pub subscription: String,
    /// Stable aggregate consumer name. Child names append `-seg-<id>`.
    pub consumer_name: String,
    /// Broker schema metadata copied to every ordinary child subscribe.
    pub schema: magnetar_proto::pb::Schema,
    /// One aggregate receive budget, never multiplied by child count.
    pub receiver_budget: magnetar_proto::ReceiverBudget,
    /// Parent-before-child ordering policy.
    pub ordering_mode: magnetar_proto::OrderingMode,
}

/// One raw aggregate delivery returned by a runtime backend.
///
/// The ordinary message owns its payload and metadata; the token is the sole
/// process-local acknowledgement authority for that aggregate delivery.
#[cfg(all(feature = "tokio", feature = "scalable-topics"))]
#[doc(hidden)]
#[derive(Debug)]
pub struct RawStreamMessage {
    /// Ordinary child delivery.
    pub message: magnetar_proto::IncomingMessage,
    /// Source-, incarnation-, generation-, and delivery-epoch-bound authority.
    pub token: magnetar_proto::DeliveryToken,
}

/// Owned aggregate backend returned by [`SegmentSubscriberApi`].
///
/// Implementations are runtime resources, not public client clones. All
/// methods target the backend's typed controller/segment routes; none may
/// consume the client-global scalable event queue. Receive methods accept
/// concurrent callers, and `receive_batch` must reserve its returned batch
/// atomically after the first-message wait.
#[cfg(all(feature = "tokio", feature = "scalable-topics"))]
#[doc(hidden)]
pub trait StreamConsumerBackend: 'static + Send + Sync {
    /// Await and reserve one delivery.
    fn receive(
        &self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<RawStreamMessage, crate::scalable::StreamConsumerError>>
                + Send
                + '_,
        >,
    >;

    /// Await the first delivery, then atomically reserve the complete bounded
    /// batch before another receive can interleave.
    fn receive_batch(
        &self,
        policy: crate::scalable::BatchReceivePolicy,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<RawStreamMessage>, crate::scalable::StreamConsumerError>>
                + Send
                + '_,
        >,
    >;

    /// Return deliveries reserved by a cancelled facade future to their
    /// original aggregate queue positions without changing their authority.
    fn restore_messages(&self, messages: Vec<RawStreamMessage>);

    /// Resolve broker schema metadata for a child when the retained schema
    /// instance requests PIP-87 discovery.
    fn get_schema<'a>(
        &'a self,
        source: &'a magnetar_proto::SegmentSource,
        version: Option<bytes::Bytes>,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        magnetar_proto::pb::Schema,
                        crate::scalable::StreamConsumerError,
                    >,
                > + Send
                + 'a,
        >,
    >;

    /// Individually acknowledge one live delivery.
    fn acknowledge<'a>(
        &'a self,
        token: &'a magnetar_proto::DeliveryToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>;

    /// Cumulatively acknowledge the delivered position vector carried by one
    /// live token.
    fn acknowledge_cumulative<'a>(
        &'a self,
        token: &'a magnetar_proto::DeliveryToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>;

    /// Acknowledge a restored serializable position vector after validating it
    /// against current assignment and child generations.
    fn acknowledge_positions<'a>(
        &'a self,
        positions: &'a magnetar_proto::PositionVector,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>;

    /// Validate all tokens, then issue a batch acknowledgement.
    fn acknowledge_batch<'a>(
        &'a self,
        tokens: Vec<&'a magnetar_proto::DeliveryToken>,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>;

    /// Negatively acknowledge one live delivery using the configured/default
    /// redelivery delay.
    fn negative_acknowledge(
        &self,
        token: &magnetar_proto::DeliveryToken,
    ) -> Result<(), crate::scalable::StreamConsumerError>;

    /// Admit and issue one individual acknowledgement in a Pulsar transaction.
    fn acknowledge_in_transaction<'a>(
        &'a self,
        token: &'a magnetar_proto::DeliveryToken,
        txn_id: magnetar_proto::TxnId,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>;

    /// Admit every component of a live cumulative position in a transaction.
    fn acknowledge_cumulative_in_transaction<'a>(
        &'a self,
        token: &'a magnetar_proto::DeliveryToken,
        txn_id: magnetar_proto::TxnId,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>;

    /// Admit every component of a restored position vector in a transaction.
    fn acknowledge_positions_in_transaction<'a>(
        &'a self,
        positions: &'a magnetar_proto::PositionVector,
        txn_id: magnetar_proto::TxnId,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>;

    /// Current highest position delivered to the application per segment.
    fn delivered_position(&self) -> magnetar_proto::PositionVector;

    /// Current aggregate lifecycle/resource snapshot.
    fn status(&self) -> crate::scalable::StreamConsumerStatus;

    /// Await the next event on this aggregate's owned typed route.
    fn next_event(
        &self,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Option<crate::scalable::StreamConsumerEvent>,
                        crate::scalable::StreamConsumerError,
                    >,
                > + Send
                + '_,
        >,
    >;

    /// Apply the M1-limited all-current-leaves vector seek.
    fn seek_positions<'a>(
        &'a self,
        positions: &'a magnetar_proto::PositionVector,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>;

    /// Propagate a confirmed or unknown transaction outcome to this aggregate.
    fn transaction_outcome(
        &self,
        txn_id: magnetar_proto::TxnId,
        outcome: crate::scalable::TransactionOutcome,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + '_>>;

    /// Globally fence the aggregate and await typed-route/task/child cleanup.
    fn close(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + '_>>;

    /// Synchronous final-user-guard cleanup. Must not block or spawn.
    fn close_best_effort(&self);
}

/// Engine-side factory for an owned assignment-driven segment subscriber.
///
/// The opening future may borrow the public runtime client, but the returned
/// aggregate backend is owned and `'static`. This is the capability boundary
/// that removes the former `E::ClientState: Clone` requirement.
#[cfg(all(feature = "tokio", feature = "scalable-topics"))]
#[doc(hidden)]
pub trait SegmentSubscriberApi: 'static + Send + Sync {
    /// Runtime-owned aggregate backend.
    type StreamConsumer: StreamConsumerBackend;

    /// Resolve controller authority, install owned typed routes, register the
    /// scalable member, and open its initial assigned segment children.
    fn subscribe_stream_consumer(
        &self,
        options: StreamConsumerOptions,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Self::StreamConsumer, crate::scalable::StreamConsumerError>>
                + Send
                + '_,
        >,
    >;
}

/// **Experimental** (PIP-460, ADR-0093). Engine-side scalable-topic hooks —
/// implemented by each runtime on its `Client` type. This is the low-level raw
/// lookup/watch API; the assignment-driven [`crate::scalable::StreamConsumer`]
/// dispatches through [`SegmentSubscriberApi`] instead.
///
/// **Sans-io.** Async methods return `Pin<Box<dyn Future + Send + '_>>`; no
/// tokio / mio / socket types appear in the surface. Each impl drives the
/// [`magnetar_proto::Connection`] scalable entries
/// (`open_scalable_topic_session`, `close_scalable_topic_session`) and reads the
/// driver-drained events.
#[cfg(all(feature = "tokio", feature = "scalable-topics"))]
pub trait ScalableTopicsApi: 'static + Send + Sync {
    /// Per-runtime client error type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Open a low-level scalable-topic session and await its first layout. The
    /// session stays open, pushing later layouts through
    /// [`Self::next_scalable_event`], until it is closed. Owned high-level
    /// consumers must not use this queue.
    fn scalable_topic_lookup<'a>(
        &'a self,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ScalableLookup, Self::Error>> + Send + 'a>>;

    /// Whether the connected broker advertised the PIP-460 capability.
    /// `false` against a Pulsar 4.x peer.
    fn broker_supports_scalable_topics(&self) -> bool;

    /// Register as a scalable consumer with the controller leader and await the
    /// initial assignment — the `segment://` topics this consumer owns.
    fn scalable_topic_subscribe<'a>(
        &'a self,
        topic: &'a str,
        subscription: &'a str,
        consumer_name: &'a str,
        consumer_id: u64,
        consumer_type: magnetar_proto::ScalableConsumerType,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<magnetar_proto::ConsumerAssignment, Self::Error>>
                + Send
                + 'a,
        >,
    >;

    /// Open a namespace-level watch over the scalable topics matching
    /// `property_filters` (empty = every scalable topic in the namespace).
    fn watch_scalable_topics(
        &self,
        namespace: &str,
        property_filters: Vec<(String, String)>,
    ) -> Result<u64, Self::Error>;

    /// Close a namespace-level scalable-topics watch.
    fn close_scalable_topics_watch(&self, watch_id: u64);

    /// The current matching topic set for a namespace watch.
    fn scalable_topics_snapshot(&self, watch_id: u64) -> Option<Vec<String>>;

    /// Whether the broker advertised metadata-driven transaction-coordinator
    /// discovery. Independent of `supports_scalable_topics` upstream.
    fn broker_supports_tc_metadata_discovery(&self) -> bool;

    /// Open a transaction-coordinator discovery watch.
    fn watch_tc_assignments(&self) -> Result<u64, Self::Error>;

    /// Close a transaction-coordinator discovery watch.
    fn close_tc_assignments_watch(&self, watch_id: u64);

    /// Close a scalable-topic session.
    fn close_scalable_topic_session(&self, session_id: u64);

    /// Await the next unclaimed low-level scalable-topic event. Resolves `None`
    /// once the connection closes. Events claimed by an owned high-level route
    /// never appear here.
    fn next_scalable_event(
        &self,
    ) -> Pin<Box<dyn Future<Output = Option<ScalableEvent>> + Send + '_>>;
}

/// **Experimental** (PIP-460, ADR-0093). Engine-agnostic resolved
/// scalable-topic lookup surfaced through [`ScalableTopicsApi`]. Façade-side
/// analogue of each runtime's `ScalableLookup`.
#[cfg(all(feature = "tokio", feature = "scalable-topics"))]
#[derive(Debug, Clone)]
pub struct ScalableLookup {
    /// Client-allocated session id; the session stays open until closed.
    pub session_id: u64,
    /// Canonical `topic://...` identity the broker resolved the request to.
    pub resolved_topic_name: Option<String>,
    /// Controller broker serving this topic's layout, when advertised.
    pub controller_broker_url: Option<String>,
    /// TLS controller broker serving this topic's layout, when advertised.
    pub controller_broker_url_tls: Option<String>,
    /// Complete validated initial DAG snapshot.
    pub snapshot: magnetar_proto::DagSnapshot,
    /// Initial DAG snapshot for the topic.
    pub segments: Vec<magnetar_proto::SegmentDescriptor>,
    /// Layout epoch the snapshot was stamped with.
    pub epoch: u64,
}

/// **Experimental** (PIP-460, ADR-0093). Engine-agnostic scalable-topic event
/// surfaced through [`ScalableTopicsApi::next_scalable_event`]. Façade-side
/// analogue of each runtime's `ScalableEvent`.
#[cfg(all(feature = "tokio", feature = "scalable-topics"))]
#[derive(Debug, Clone)]
pub enum ScalableEvent {
    /// A scalable-topic session resolved: its first layout landed.
    LookupResolved {
        /// Client-allocated session id.
        session_id: u64,
        /// Canonical `topic://...` identity the broker resolved to.
        resolved_topic_name: Option<String>,
        /// Controller broker serving this topic's layout, when advertised.
        controller_broker_url: Option<String>,
        /// TLS controller broker serving this topic's layout, when advertised.
        controller_broker_url_tls: Option<String>,
        /// Complete validated initial DAG snapshot.
        snapshot: magnetar_proto::DagSnapshot,
        /// Initial DAG snapshot.
        segments: Vec<magnetar_proto::SegmentDescriptor>,
        /// Layout epoch the snapshot was stamped with.
        epoch: u64,
    },
    /// An open session applied a subsequent layout.
    DagUpdated {
        /// Session id.
        session_id: u64,
        /// The applied delta.
        delta: magnetar_proto::DagDelta,
        /// Complete validated replacement DAG snapshot.
        snapshot: magnetar_proto::DagSnapshot,
    },
    /// Legacy low-level consume-affecting DAG notification. Owned aggregate
    /// consumers reconcile the replacement snapshot on their typed route and
    /// do not consume this global event.
    DagChangedDuringConsume {
        /// Session id whose DAG changed.
        session_id: u64,
        /// Why the DAG changed.
        reason: magnetar_proto::DagChangeReason,
    },
    /// The scalable-topic session closed.
    DagWatchClosed {
        /// Session id that closed.
        session_id: u64,
        /// Optional close reason.
        reason: Option<String>,
    },
    /// A scalable consumer's registration resolved with its initial share.
    ConsumerAssigned {
        /// Consumer id that registered.
        consumer_id: u64,
        /// Local controller-connection incarnation carrying the baseline.
        incarnation: magnetar_proto::ControllerIncarnation,
        /// The `segment://` topics this consumer owns.
        assignment: magnetar_proto::ConsumerAssignment,
    },
    /// The controller leader rebalanced a registered consumer's share.
    AssignmentChanged {
        /// Consumer id whose share changed.
        consumer_id: u64,
        /// Local controller-connection incarnation carrying the update.
        incarnation: magnetar_proto::ControllerIncarnation,
        /// Complete authoritative assignment after applying the update.
        assignment: magnetar_proto::ConsumerAssignment,
        /// What to attach to and detach from.
        delta: magnetar_proto::AssignmentDelta,
    },
    /// A scalable consumer's registration was rejected.
    ConsumerRejected {
        /// Consumer id whose registration failed.
        consumer_id: u64,
        /// Local controller-connection incarnation carrying the rejection.
        incarnation: magnetar_proto::ControllerIncarnation,
        /// Why the broker rejected it.
        reason: String,
    },
    /// A namespace-level scalable-topics watch delivered a snapshot or a diff.
    TopicsChanged {
        /// Watch id the update belongs to.
        watch_id: u64,
        /// The snapshot or diff the broker sent.
        change: magnetar_proto::TopicsChange,
    },
    /// A namespace-level scalable-topics watch ended.
    TopicsWatchClosed {
        /// Watch id that closed.
        watch_id: u64,
        /// Optional close reason.
        reason: Option<String>,
    },
    /// The metadata-driven transaction-coordinator assignment set changed.
    TcAssignmentsChanged {
        /// Watch id the update belongs to.
        watch_id: u64,
        /// Number of transaction-coordinator partitions.
        parallelism: u32,
        /// Which broker serves each coordinator.
        assignments: Vec<magnetar_proto::TcAssignment>,
    },
    /// A transaction-coordinator discovery watch ended.
    TcAssignmentsWatchClosed {
        /// Watch id that closed.
        watch_id: u64,
        /// Optional close reason.
        reason: Option<String>,
    },
}

/// Zero-sized stub for any future engine that genuinely cannot wire real
/// encryption. Both shipped engines ([`TokioEngine`] and
/// [`MoonpoolEngine`]) now resolve their `MessageEncryptorApi::Encryptor`
/// / `MessageDecryptorApi::Decryptor` to their own runtime's
/// `Arc<dyn …MessageEncryptor>` / `…MessageDecryptor`, so `NoEncryption`
/// is no longer used by either. It is retained as the documented opt-out
/// type an engine can hand to the façade to signal "encryption not
/// supported on this engine" — the builders' generic `.create()` /
/// `.subscribe()` paths ignore the encryptor field regardless.
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
            ::tokio::time::sleep(std::time::Duration::from_hours(1)).await;
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
            ::tokio::time::sleep(std::time::Duration::from_hours(1)).await;
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
