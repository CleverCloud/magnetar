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

use std::fmt::Debug;
use std::future::Future;
#[cfg(feature = "moonpool")]
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

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
pub trait Engine: 'static + Send + Sync + Debug {
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
    /// safe [`Self::TaskHandle`]. Tokio wraps [`tokio::spawn`]; moonpool
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

#[cfg(feature = "tokio")]
impl TransactionApi for magnetar_runtime_tokio::Client {
    type Error = magnetar_runtime_tokio::ClientError;

    fn new_txn(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::TxnId, Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Client::new_txn(self, timeout))
    }

    fn add_partition_to_txn(
        &self,
        txn: magnetar_proto::TxnId,
        topic: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Client::add_partition_to_txn(
            self, txn, topic,
        ))
    }

    fn add_subscription_to_txn(
        &self,
        txn: magnetar_proto::TxnId,
        topic: String,
        subscription: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Client::add_subscription_to_txn(
            self,
            txn,
            topic,
            subscription,
        ))
    }

    fn end_txn(
        &self,
        txn: magnetar_proto::TxnId,
        action: magnetar_proto::TxnAction,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::TxnState, Self::Error>> + Send + '_>>
    {
        Box::pin(magnetar_runtime_tokio::Client::end_txn(self, txn, action))
    }
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
        version: Option<Vec<u8>>,
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
/// Helpers that touch tokio-specific futures (`ReceiveFut`,
/// `receive_with_timeout`), the broader ack family
/// (`ack_with_txn`, `ack_batch`, `ack_cumulative_with_txn`,
/// `ack_with_properties`), `flow`, `redeliver_unacked`,
/// `last_message_id`, `has_message_after`, `seek_*`, and `close` stay
/// runtime-specific in Phase 1; the trait is additive so subsequent
/// façade lifts grow it as they need the surface.
#[cfg(feature = "tokio")]
pub trait ConsumerApi: 'static + Send + Sync {
    /// Per-runtime client error type used by the wire calls.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Receive the next message. Resolves once the broker has
    /// delivered an entry.
    fn receive(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<crate::IncomingMessage, Self::Error>> + Send + '_>>;

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
        version: Option<Vec<u8>>,
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
    /// Java `Consumer#unsubscribe`. PIP-313's `force=true` variant lives
    /// directly on the concrete runtime types as
    /// `Consumer::unsubscribe(force)`; pass-2 of the
    /// `MultiTopicsConsumer` surface lift will add the boolean to the
    /// trait method itself.
    fn unsubscribe(&self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;

    /// Seek to the earliest available message. Mirrors Java
    /// `Consumer#seek(MessageId.earliest)`.
    fn seek_to_earliest(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;

    /// Seek to the latest available message. Mirrors Java
    /// `Consumer#seek(MessageId.latest)`.
    fn seek_to_latest(&self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;

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

#[cfg(feature = "tokio")]
impl SubscribeApi for magnetar_runtime_tokio::Client {
    type Consumer = magnetar_runtime_tokio::Consumer;
    type Error = magnetar_runtime_tokio::ClientError;

    fn subscribe(
        &self,
        req: magnetar_proto::SubscribeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Consumer, Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Client::subscribe(self, req))
    }
}

#[cfg(feature = "tokio")]
impl CreateProducerApi for magnetar_runtime_tokio::Client {
    type Producer = magnetar_runtime_tokio::Producer;
    type Error = magnetar_runtime_tokio::ClientError;

    fn open_producer(
        &self,
        req: magnetar_proto::CreateProducerRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Producer, Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Client::open_producer(self, req))
    }
}

#[cfg(all(feature = "tokio", feature = "moonpool"))]
impl<P: moonpool_core::Providers + Send + Sync + 'static> SubscribeApi
    for magnetar_runtime_moonpool::Client<P>
{
    type Consumer = magnetar_runtime_moonpool::Consumer<P>;
    type Error = magnetar_runtime_moonpool::ClientError;

    fn subscribe(
        &self,
        req: magnetar_proto::SubscribeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Consumer, Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_moonpool::Client::subscribe(self, req))
    }
}

#[cfg(all(feature = "tokio", feature = "moonpool"))]
impl<P: moonpool_core::Providers + Send + Sync + 'static> CreateProducerApi
    for magnetar_runtime_moonpool::Client<P>
{
    type Producer = magnetar_runtime_moonpool::Producer<P>;
    type Error = magnetar_runtime_moonpool::ClientError;

    fn open_producer(
        &self,
        req: magnetar_proto::CreateProducerRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Producer, Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_moonpool::Client::open_producer(self, req))
    }
}

#[cfg(feature = "tokio")]
impl ProducerApi for magnetar_runtime_tokio::Producer {
    type Error = magnetar_runtime_tokio::ClientError;

    fn send(
        &self,
        msg: crate::OutgoingMessage,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::MessageId, Self::Error>> + Send + '_>>
    {
        Box::pin(magnetar_runtime_tokio::Producer::send(self, msg.into()))
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Producer::flush(self))
    }

    fn is_closed(&self) -> bool {
        magnetar_runtime_tokio::Producer::is_closed(self)
    }

    fn is_connected(&self) -> bool {
        magnetar_runtime_tokio::Producer::is_connected(self)
    }

    fn topic(&self) -> String {
        magnetar_runtime_tokio::Producer::topic(self)
    }

    fn name(&self) -> String {
        magnetar_runtime_tokio::Producer::name(self)
    }

    fn last_sequence_id(&self) -> i64 {
        magnetar_runtime_tokio::Producer::last_sequence_id(self)
    }

    fn get_schema(
        &self,
        version: Option<Vec<u8>>,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::pb::Schema, Self::Error>> + Send + '_>>
    {
        Box::pin(magnetar_runtime_tokio::Producer::get_schema(self, version))
    }

    fn stats(&self) -> magnetar_proto::producer::ProducerStats {
        magnetar_runtime_tokio::Producer::stats(self)
    }

    fn close_owned(self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>> {
        Box::pin(magnetar_runtime_tokio::Producer::close(self))
    }

    fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        magnetar_runtime_tokio::Producer::last_disconnected_timestamp(self)
    }

    fn compression(&self) -> magnetar_proto::types::CompressionKind {
        magnetar_runtime_tokio::Producer::compression(self)
    }

    fn last_sequence_id_published(&self) -> i64 {
        magnetar_runtime_tokio::Producer::last_sequence_id_published(self)
    }

    fn pending_count(&self) -> usize {
        magnetar_runtime_tokio::Producer::pending_count(self)
    }

    fn batch_len(&self) -> usize {
        magnetar_runtime_tokio::Producer::batch_len(self)
    }

    fn batch_bytes(&self) -> usize {
        magnetar_runtime_tokio::Producer::batch_bytes(self)
    }
}

#[cfg(feature = "tokio")]
impl ConsumerApi for magnetar_runtime_tokio::Consumer {
    type Error = magnetar_runtime_tokio::ClientError;

    fn receive(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<crate::IncomingMessage, Self::Error>> + Send + '_>>
    {
        Box::pin(async move {
            magnetar_runtime_tokio::Consumer::receive(self)
                .await
                .map(crate::IncomingMessage::from)
        })
    }

    fn ack(
        &self,
        message_id: magnetar_proto::MessageId,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Consumer::ack(self, message_id))
    }

    fn ack_cumulative(
        &self,
        message_id: magnetar_proto::MessageId,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Consumer::ack_cumulative(
            self, message_id,
        ))
    }

    fn negative_ack(&self, message_id: magnetar_proto::MessageId) {
        magnetar_runtime_tokio::Consumer::negative_ack(self, message_id);
    }

    fn last_message_id(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::MessageId, Self::Error>> + Send + '_>>
    {
        Box::pin(magnetar_runtime_tokio::Consumer::last_message_id(self))
    }

    fn has_message_after(
        &self,
        cursor: magnetar_proto::MessageId,
    ) -> Pin<Box<dyn Future<Output = Result<bool, Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Consumer::has_message_after(
            self, cursor,
        ))
    }

    fn get_schema(
        &self,
        version: Option<Vec<u8>>,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::pb::Schema, Self::Error>> + Send + '_>>
    {
        Box::pin(magnetar_runtime_tokio::Consumer::get_schema(self, version))
    }

    fn topic(&self) -> String {
        magnetar_runtime_tokio::Consumer::topic(self)
    }

    fn subscription(&self) -> String {
        magnetar_runtime_tokio::Consumer::subscription(self)
    }

    fn name(&self) -> String {
        magnetar_runtime_tokio::Consumer::name(self)
    }

    fn is_closed(&self) -> bool {
        magnetar_runtime_tokio::Consumer::is_closed(self)
    }

    fn is_connected(&self) -> bool {
        magnetar_runtime_tokio::Consumer::is_connected(self)
    }

    fn stats(&self) -> magnetar_proto::consumer::ConsumerStats {
        magnetar_runtime_tokio::Consumer::stats(self)
    }

    fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        magnetar_runtime_tokio::Consumer::last_disconnected_timestamp(self)
    }

    fn redeliver_unacked(&self) {
        magnetar_runtime_tokio::Consumer::redeliver_unacked(self);
    }

    fn negative_ack_with_delay(
        &self,
        message_id: magnetar_proto::MessageId,
        delay: std::time::Duration,
    ) {
        magnetar_runtime_tokio::Consumer::negative_ack_with_delay(self, message_id, delay);
    }

    fn unsubscribe(&self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Consumer::unsubscribe(self, false))
    }

    fn seek_to_earliest(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Consumer::seek_to_earliest(self))
    }

    fn seek_to_latest(&self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Consumer::seek_to_latest(self))
    }

    fn close_owned(self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>> {
        Box::pin(magnetar_runtime_tokio::Consumer::close(self))
    }

    fn ack_grouped(&self, message_id: magnetar_proto::MessageId) {
        magnetar_runtime_tokio::Consumer::ack_grouped(self, message_id);
    }

    fn ack_grouped_cumulative(&self, message_id: magnetar_proto::MessageId) {
        magnetar_runtime_tokio::Consumer::ack_grouped_cumulative(self, message_id);
    }

    fn ack_with_txn(
        &self,
        message_id: magnetar_proto::MessageId,
        txn_id: magnetar_proto::TxnId,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Consumer::ack_with_txn(
            self, message_id, txn_id,
        ))
    }

    fn ack_cumulative_with_txn(
        &self,
        message_id: magnetar_proto::MessageId,
        txn_id: magnetar_proto::TxnId,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Consumer::ack_cumulative_with_txn(
            self, message_id, txn_id,
        ))
    }
}

#[cfg(all(feature = "tokio", feature = "moonpool"))]
impl<P: moonpool_core::Providers + Send + Sync + 'static> ProducerApi
    for magnetar_runtime_moonpool::Producer<P>
{
    type Error = magnetar_runtime_moonpool::ClientError;

    fn send(
        &self,
        msg: crate::OutgoingMessage,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::MessageId, Self::Error>> + Send + '_>>
    {
        // The moonpool runtime's `Producer::send` returns its own
        // `SendFut`; we drive it through `.await` and return a boxed
        // future to keep the trait signature engine-agnostic. The
        // moonpool `OutgoingMessage` is a re-export of the same proto
        // type the façade carries.
        let mp_msg: magnetar_proto::producer::OutgoingMessage = msg.into();
        Box::pin(async move { magnetar_runtime_moonpool::Producer::send(self, mp_msg).await })
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_moonpool::Producer::flush(self))
    }

    fn is_closed(&self) -> bool {
        magnetar_runtime_moonpool::Producer::is_closed(self)
    }

    fn is_connected(&self) -> bool {
        magnetar_runtime_moonpool::Producer::is_connected(self)
    }

    fn topic(&self) -> String {
        magnetar_runtime_moonpool::Producer::topic(self)
    }

    fn name(&self) -> String {
        magnetar_runtime_moonpool::Producer::name(self)
    }

    fn last_sequence_id(&self) -> i64 {
        magnetar_runtime_moonpool::Producer::last_sequence_id(self)
    }

    fn get_schema(
        &self,
        version: Option<Vec<u8>>,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::pb::Schema, Self::Error>> + Send + '_>>
    {
        Box::pin(magnetar_runtime_moonpool::Producer::get_schema(
            self, version,
        ))
    }

    fn stats(&self) -> magnetar_proto::producer::ProducerStats {
        magnetar_runtime_moonpool::Producer::stats(self)
    }

    fn close_owned(self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>> {
        Box::pin(magnetar_runtime_moonpool::Producer::close(self))
    }

    fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        magnetar_runtime_moonpool::Producer::last_disconnected_timestamp(self)
    }

    fn compression(&self) -> magnetar_proto::types::CompressionKind {
        magnetar_runtime_moonpool::Producer::compression(self)
    }

    fn last_sequence_id_published(&self) -> i64 {
        magnetar_runtime_moonpool::Producer::last_sequence_id_published(self)
    }

    fn pending_count(&self) -> usize {
        magnetar_runtime_moonpool::Producer::pending_count(self)
    }

    fn batch_len(&self) -> usize {
        magnetar_runtime_moonpool::Producer::batch_len(self)
    }

    fn batch_bytes(&self) -> usize {
        magnetar_runtime_moonpool::Producer::batch_bytes(self)
    }
}

#[cfg(all(feature = "tokio", feature = "moonpool"))]
impl<P: moonpool_core::Providers + Send + Sync + 'static> ConsumerApi
    for magnetar_runtime_moonpool::Consumer<P>
{
    type Error = magnetar_runtime_moonpool::ClientError;

    fn receive(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<crate::IncomingMessage, Self::Error>> + Send + '_>>
    {
        Box::pin(async move {
            magnetar_runtime_moonpool::Consumer::receive(self)
                .await
                .map(crate::IncomingMessage::from)
        })
    }

    fn ack(
        &self,
        message_id: magnetar_proto::MessageId,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_moonpool::Consumer::ack(self, message_id))
    }

    fn ack_cumulative(
        &self,
        message_id: magnetar_proto::MessageId,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_moonpool::Consumer::ack_cumulative(
            self, message_id,
        ))
    }

    fn negative_ack(&self, message_id: magnetar_proto::MessageId) {
        magnetar_runtime_moonpool::Consumer::negative_ack(self, message_id);
    }

    fn last_message_id(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::MessageId, Self::Error>> + Send + '_>>
    {
        Box::pin(magnetar_runtime_moonpool::Consumer::last_message_id(self))
    }

    fn has_message_after(
        &self,
        cursor: magnetar_proto::MessageId,
    ) -> Pin<Box<dyn Future<Output = Result<bool, Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_moonpool::Consumer::has_message_after(
            self, cursor,
        ))
    }

    fn get_schema(
        &self,
        version: Option<Vec<u8>>,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::pb::Schema, Self::Error>> + Send + '_>>
    {
        Box::pin(magnetar_runtime_moonpool::Consumer::get_schema(
            self, version,
        ))
    }

    fn topic(&self) -> String {
        magnetar_runtime_moonpool::Consumer::topic(self)
    }

    fn subscription(&self) -> String {
        magnetar_runtime_moonpool::Consumer::subscription(self)
    }

    fn name(&self) -> String {
        magnetar_runtime_moonpool::Consumer::name(self)
    }

    fn is_closed(&self) -> bool {
        magnetar_runtime_moonpool::Consumer::is_closed(self)
    }

    fn is_connected(&self) -> bool {
        magnetar_runtime_moonpool::Consumer::is_connected(self)
    }

    fn stats(&self) -> magnetar_proto::consumer::ConsumerStats {
        magnetar_runtime_moonpool::Consumer::stats(self)
    }

    fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        magnetar_runtime_moonpool::Consumer::last_disconnected_timestamp(self)
    }

    fn redeliver_unacked(&self) {
        magnetar_runtime_moonpool::Consumer::redeliver_unacked(self);
    }

    fn negative_ack_with_delay(
        &self,
        message_id: magnetar_proto::MessageId,
        delay: std::time::Duration,
    ) {
        magnetar_runtime_moonpool::Consumer::negative_ack_with_delay(self, message_id, delay);
    }

    fn unsubscribe(&self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        // The `ConsumerApi` trait keeps a zero-arg `unsubscribe()` for now;
        // pass-2 of the MultiTopicsConsumer surface lift adds the `force`
        // variant onto the trait directly. Default to `force=false`
        // (PIP-313: respect other attached consumers) — same as the tokio
        // engine's matching trait impl.
        Box::pin(magnetar_runtime_moonpool::Consumer::unsubscribe(
            self, false,
        ))
    }

    fn seek_to_earliest(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_moonpool::Consumer::seek_to_earliest(self))
    }

    fn seek_to_latest(&self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_moonpool::Consumer::seek_to_latest(self))
    }

    fn close_owned(self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>> {
        Box::pin(magnetar_runtime_moonpool::Consumer::close(self))
    }

    fn ack_grouped(&self, message_id: magnetar_proto::MessageId) {
        magnetar_runtime_moonpool::Consumer::ack_grouped(self, message_id);
    }

    fn ack_grouped_cumulative(&self, message_id: magnetar_proto::MessageId) {
        magnetar_runtime_moonpool::Consumer::ack_grouped_cumulative(self, message_id);
    }

    fn ack_with_txn(
        &self,
        message_id: magnetar_proto::MessageId,
        txn_id: magnetar_proto::TxnId,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_moonpool::Consumer::ack_with_txn(
            self, message_id, txn_id,
        ))
    }

    fn ack_cumulative_with_txn(
        &self,
        message_id: magnetar_proto::MessageId,
        txn_id: magnetar_proto::TxnId,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(
            magnetar_runtime_moonpool::Consumer::ack_cumulative_with_txn(self, message_id, txn_id),
        )
    }
}

#[cfg(feature = "moonpool")]
impl<P: moonpool_core::Providers + Send + Sync + 'static> TransactionApi
    for magnetar_runtime_moonpool::Client<P>
{
    type Error = magnetar_runtime_moonpool::ClientError;

    fn new_txn(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::TxnId, Self::Error>> + Send + '_>> {
        let shared = self.shared().clone();
        Box::pin(async move {
            let request_id = {
                let mut conn = shared.inner.lock();
                conn.new_txn(timeout)
            };
            shared.driver_waker.notify_one();
            let outcome = moonpool_request_fut(shared.clone(), request_id).await;
            match outcome {
                magnetar_proto::OpOutcome::NewTxn { result, .. } => result.map_err(|err| {
                    magnetar_runtime_moonpool::ClientError::Other(format!("new_txn: {err}"))
                }),
                magnetar_proto::OpOutcome::Error { code, message, .. } => {
                    Err(magnetar_runtime_moonpool::ClientError::Broker { code, message })
                }
                other => Err(magnetar_runtime_moonpool::ClientError::Other(format!(
                    "unexpected new_txn outcome: {other:?}"
                ))),
            }
        })
    }

    fn add_partition_to_txn(
        &self,
        txn: magnetar_proto::TxnId,
        topic: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        let shared = self.shared().clone();
        Box::pin(async move {
            let request_id = {
                let mut conn = shared.inner.lock();
                conn.add_partition_to_txn(txn, topic)
            };
            shared.driver_waker.notify_one();
            let outcome = moonpool_request_fut(shared.clone(), request_id).await;
            match outcome {
                magnetar_proto::OpOutcome::AddPartitionToTxn { result, .. } => {
                    result.map_err(|err| {
                        magnetar_runtime_moonpool::ClientError::Other(format!(
                            "add_partition_to_txn: {err}"
                        ))
                    })
                }
                magnetar_proto::OpOutcome::Error { code, message, .. } => {
                    Err(magnetar_runtime_moonpool::ClientError::Broker { code, message })
                }
                other => Err(magnetar_runtime_moonpool::ClientError::Other(format!(
                    "unexpected add_partition_to_txn outcome: {other:?}"
                ))),
            }
        })
    }

    fn add_subscription_to_txn(
        &self,
        txn: magnetar_proto::TxnId,
        topic: String,
        subscription: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        let shared = self.shared().clone();
        Box::pin(async move {
            let request_id = {
                let mut conn = shared.inner.lock();
                // Proto layer wants (subscription, topic); façade exposes (topic, subscription).
                conn.add_subscription_to_txn(txn, subscription, topic)
            };
            shared.driver_waker.notify_one();
            let outcome = moonpool_request_fut(shared.clone(), request_id).await;
            match outcome {
                magnetar_proto::OpOutcome::AddSubscriptionToTxn { result, .. } => {
                    result.map_err(|err| {
                        magnetar_runtime_moonpool::ClientError::Other(format!(
                            "add_subscription_to_txn: {err}"
                        ))
                    })
                }
                magnetar_proto::OpOutcome::Error { code, message, .. } => {
                    Err(magnetar_runtime_moonpool::ClientError::Broker { code, message })
                }
                other => Err(magnetar_runtime_moonpool::ClientError::Other(format!(
                    "unexpected add_subscription_to_txn outcome: {other:?}"
                ))),
            }
        })
    }

    fn end_txn(
        &self,
        txn: magnetar_proto::TxnId,
        action: magnetar_proto::TxnAction,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::TxnState, Self::Error>> + Send + '_>>
    {
        let shared = self.shared().clone();
        Box::pin(async move {
            let request_id = {
                let mut conn = shared.inner.lock();
                conn.end_txn(txn, action)
            };
            shared.driver_waker.notify_one();
            let outcome = moonpool_request_fut(shared.clone(), request_id).await;
            match outcome {
                magnetar_proto::OpOutcome::EndTxn { result, .. } => result.map_err(|err| {
                    magnetar_runtime_moonpool::ClientError::Other(format!("end_txn: {err}"))
                }),
                magnetar_proto::OpOutcome::Error { code, message, .. } => {
                    Err(magnetar_runtime_moonpool::ClientError::Broker { code, message })
                }
                other => Err(magnetar_runtime_moonpool::ClientError::Other(format!(
                    "unexpected end_txn outcome: {other:?}"
                ))),
            }
        })
    }
}

/// Park on a request-id-correlated outcome from the moonpool engine's
/// shared connection state. Mirrors `magnetar_runtime_moonpool`'s
/// internal `RequestFut`; reproduced here because that type is
/// `pub(crate)` to the moonpool runtime.
#[cfg(feature = "moonpool")]
fn moonpool_request_fut(
    shared: std::sync::Arc<magnetar_runtime_moonpool::ConnectionShared>,
    request_id: magnetar_proto::RequestId,
) -> Pin<Box<dyn Future<Output = magnetar_proto::OpOutcome> + Send>> {
    use std::task::{Context, Poll};

    struct Fut {
        shared: std::sync::Arc<magnetar_runtime_moonpool::ConnectionShared>,
        request_id: magnetar_proto::RequestId,
    }
    impl Future for Fut {
        type Output = magnetar_proto::OpOutcome;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let key = magnetar_proto::PendingOpKey::Request(self.request_id);
            let mut conn = self.shared.inner.lock();
            if let Some(outcome) = conn.take_outcome(key) {
                return Poll::Ready(outcome);
            }
            conn.register_waker(key, cx.waker().clone());
            Poll::Pending
        }
    }
    Box::pin(Fut { shared, request_id })
}

/// Zero-sized marker for the tokio production engine. Default `E` on
/// [`crate::PulsarClient<E>`].
///
/// Available behind the `tokio` feature (default-on).
#[cfg(feature = "tokio")]
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioEngine;

#[cfg(feature = "tokio")]
impl Engine for TokioEngine {
    type ClientState = magnetar_runtime_tokio::Client;
    type TaskHandle = tokio::task::JoinHandle<()>;
    type Interval = tokio::time::Interval;

    fn name() -> &'static str {
        "tokio"
    }

    fn spawn<F>(fut: F) -> Self::TaskHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(fut)
    }

    fn abort_task(handle: &mut Self::TaskHandle) {
        handle.abort();
    }

    fn new_interval(period: Duration) -> Self::Interval {
        // tokio's `interval` fires immediately on the first tick; the
        // ADR contract preserves that behaviour.
        tokio::time::interval(period)
    }

    fn interval_tick<'a>(
        interval: &'a mut Self::Interval,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            interval.tick().await;
        })
    }
}

/// Zero-sized marker for the moonpool deterministic-simulation engine,
/// parametrised by the [`moonpool_core::Providers`] bundle the underlying
/// driver runs on.
///
/// Available behind the `moonpool` feature. `P` is the providers bundle —
/// `TokioProviders` for production-ish runs and a `moonpool-sim`
/// `SimProviders` for chaos-tested reproducible test suites.
#[cfg(feature = "moonpool")]
pub struct MoonpoolEngine<P: moonpool_core::Providers> {
    _marker: PhantomData<fn() -> P>,
}

#[cfg(feature = "moonpool")]
impl<P: moonpool_core::Providers> Default for MoonpoolEngine<P> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

// Hand-rolled `Clone` so the bound `P: Providers` doesn't propagate through
// `derive(Clone)` — the marker holds no value, so cloning is just
// reconstructing the phantom.
#[cfg(feature = "moonpool")]
impl<P: moonpool_core::Providers> Clone for MoonpoolEngine<P> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

#[cfg(feature = "moonpool")]
impl<P: moonpool_core::Providers> Debug for MoonpoolEngine<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MoonpoolEngine").finish_non_exhaustive()
    }
}

#[cfg(feature = "moonpool")]
impl<P: moonpool_core::Providers> Engine for MoonpoolEngine<P> {
    type ClientState = magnetar_runtime_moonpool::Client<P>;
    // Under both TokioProviders and moonpool-sim's SimProviders the
    // moonpool engine ultimately schedules onto tokio (determinism comes
    // from substituting the providers, not from replacing tokio). The
    // task handle and interval types are therefore the same tokio shapes
    // as the TokioEngine — see ADR-0025 §Decision.
    type TaskHandle = tokio::task::JoinHandle<()>;
    type Interval = tokio::time::Interval;

    fn name() -> &'static str {
        "moonpool"
    }

    fn spawn<F>(fut: F) -> Self::TaskHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(fut)
    }

    fn abort_task(handle: &mut Self::TaskHandle) {
        handle.abort();
    }

    fn new_interval(period: Duration) -> Self::Interval {
        tokio::time::interval(period)
    }

    fn interval_tick<'a>(
        interval: &'a mut Self::Interval,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            interval.tick().await;
        })
    }
}

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
    use super::*;

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

    // -------------------------------------------------------------
    // ADR-0025 phase 1: task + timer primitive smoke tests. One pair
    // per engine — keeps the per-engine test count balanced even
    // though the new primitives don't yet have façade callers.

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn tokio_engine_spawn_and_abort_round_trip() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        let handle = <TokioEngine as Engine>::spawn(async move {
            c.fetch_add(1, Ordering::SeqCst);
        });
        // Drive the spawned task once.
        tokio::task::yield_now().await;
        // Awaiting the JoinHandle works on a non-aborted task.
        handle.await.expect("spawned task ran to completion");
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Spawn a second task that we abort before it can increment.
        let c2 = counter.clone();
        let mut handle2 = <TokioEngine as Engine>::spawn(async move {
            // Sleep forever — abort wins.
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
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
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn tokio_engine_interval_first_tick_is_immediate() {
        use std::time::Duration;

        let mut interval = <TokioEngine as Engine>::new_interval(Duration::from_secs(10));
        let start = tokio::time::Instant::now();
        <TokioEngine as Engine>::interval_tick(&mut interval).await;
        // First tick fires immediately per the tokio interval contract.
        assert_eq!(
            tokio::time::Instant::now().duration_since(start),
            Duration::ZERO,
            "first interval tick must fire immediately on tokio",
        );
        // Second tick waits for the period.
        <TokioEngine as Engine>::interval_tick(&mut interval).await;
        assert!(
            tokio::time::Instant::now().duration_since(start) >= Duration::from_secs(10),
            "second tick must wait one period",
        );
    }

    #[cfg(feature = "moonpool")]
    #[tokio::test(flavor = "current_thread", start_paused = true)]
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
        tokio::task::yield_now().await;
        handle.await.expect("spawned task ran to completion");
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        let c2 = counter.clone();
        let mut handle2 = <E as Engine>::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
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
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn moonpool_engine_interval_first_tick_is_immediate() {
        use std::time::Duration;

        use moonpool_core::TokioProviders;

        type E = MoonpoolEngine<TokioProviders>;

        let mut interval = <E as Engine>::new_interval(Duration::from_secs(10));
        let start = tokio::time::Instant::now();
        <E as Engine>::interval_tick(&mut interval).await;
        assert_eq!(
            tokio::time::Instant::now().duration_since(start),
            Duration::ZERO,
            "first interval tick must fire immediately on moonpool",
        );
        <E as Engine>::interval_tick(&mut interval).await;
        assert!(
            tokio::time::Instant::now().duration_since(start) >= Duration::from_secs(10),
            "second tick must wait one period",
        );
    }
}
