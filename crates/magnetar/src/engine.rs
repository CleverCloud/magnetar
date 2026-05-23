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

    /// Topic this producer publishes to.
    fn topic(&self) -> String;

    /// Producer name advertised to the broker (broker-assigned if
    /// the user didn't set one).
    fn name(&self) -> String;

    /// Latest sequence id the producer assigned. Mirrors Java
    /// `Producer#getLastSequenceId`.
    fn last_sequence_id(&self) -> i64;
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

    /// Topic this consumer is subscribed to.
    fn topic(&self) -> String;

    /// Subscription name this consumer holds.
    fn subscription(&self) -> String;

    /// `true` once the consumer has entered a terminal state.
    fn is_closed(&self) -> bool;
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

    fn topic(&self) -> String {
        magnetar_runtime_tokio::Producer::topic(self)
    }

    fn name(&self) -> String {
        magnetar_runtime_tokio::Producer::name(self)
    }

    fn last_sequence_id(&self) -> i64 {
        magnetar_runtime_tokio::Producer::last_sequence_id(self)
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

    fn topic(&self) -> String {
        magnetar_runtime_tokio::Consumer::topic(self)
    }

    fn subscription(&self) -> String {
        magnetar_runtime_tokio::Consumer::subscription(self)
    }

    fn is_closed(&self) -> bool {
        magnetar_runtime_tokio::Consumer::is_closed(self)
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

    fn topic(&self) -> String {
        magnetar_runtime_moonpool::Producer::topic(self)
    }

    fn name(&self) -> String {
        magnetar_runtime_moonpool::Producer::name(self)
    }

    fn last_sequence_id(&self) -> i64 {
        magnetar_runtime_moonpool::Producer::last_sequence_id(self)
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

    fn topic(&self) -> String {
        magnetar_runtime_moonpool::Consumer::topic(self)
    }

    fn subscription(&self) -> String {
        magnetar_runtime_moonpool::Consumer::subscription(self)
    }

    fn is_closed(&self) -> bool {
        magnetar_runtime_moonpool::Consumer::is_closed(self)
    }
}

#[cfg(feature = "moonpool")]
impl TransactionApi for MoonpoolClientState {
    type Error = magnetar_runtime_moonpool::ClientError;

    fn new_txn(
        &self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::TxnId, Self::Error>> + Send + '_>> {
        let shared = self.shared.clone();
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
        let shared = self.shared.clone();
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
        let shared = self.shared.clone();
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
        let shared = self.shared.clone();
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
    type ClientState = MoonpoolClientState;
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

/// Per-engine storage for [`crate::PulsarClient<MoonpoolEngine<P>>`] — the
/// shared connection state plus the driver join handle, in line with the
/// pair the engine's `connect_*` calls return.
///
/// Lives at the façade boundary (not inside `magnetar-runtime-moonpool`) so
/// the moonpool crate's public surface stays oriented around the engine's
/// own `(Arc<ConnectionShared>, DriverHandle)` return shape rather than a
/// façade-coupled bundle.
#[cfg(feature = "moonpool")]
#[derive(Debug)]
pub struct MoonpoolClientState {
    /// Shared connection state — the sans-io [`magnetar_proto::Connection`]
    /// behind a non-async mutex plus the driver wakeup.
    pub shared: std::sync::Arc<magnetar_runtime_moonpool::ConnectionShared>,
    /// Driver-task handle returned by
    /// [`magnetar_runtime_moonpool::MoonpoolEngine::connect_plain`]. The
    /// façade keeps it alive for the lifetime of the
    /// [`crate::PulsarClient`].
    pub driver: parking_lot::Mutex<Option<magnetar_runtime_moonpool::DriverHandle>>,
}

// `PhantomData<fn() -> P>` keeps the engine `Send + Sync` regardless of
// `P`'s thread-safety story. The marker is a witness type, not a value
// holder — engine state actually lives on `PulsarClient<E>`.

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
