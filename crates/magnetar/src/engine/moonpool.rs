// SPDX-License-Identifier: Apache-2.0

//! [`MoonpoolEngine`] — deterministic-simulation runtime engine.
//!
//! This module carries the [`MoonpoolEngine`] marker struct together with
//! every trait impl that pins the façade's per-surface extension traits
//! (`TransactionApi`, plus `ProducerApi` / `ConsumerApi` /
//! `BrokerMetadataApi` / `SubscribeApi` / `CreateProducerApi` when the
//! `tokio` feature is also on) to the
//! [`magnetar_runtime_moonpool`] client / producer / consumer types.
//!
//! Companion module to [`super::tokio`]; the shared trait definitions
//! live in [`super`] (the engine module root).

use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

#[cfg(feature = "tokio")]
use super::{
    BrokerMetadataApi, ConsumerApi, CreateProducerApi, OperationDeadline, ProducerApi,
    ReceiveBatchFut, ReceiveOptFut, SubscribeApi, TopicListChange, WatchTopicListFut,
};
use super::{Engine, MessageDecryptorApi, MessageEncryptorApi, TransactionApi};

/// Zero-sized marker for the moonpool deterministic-simulation engine,
/// parametrised by the [`moonpool_core::Providers`] bundle the underlying
/// driver runs on.
///
/// Available behind the `moonpool` feature. `P` is the providers bundle —
/// `TokioProviders` for production-ish runs and a `moonpool-sim`
/// `SimProviders` for chaos-tested reproducible test suites.
pub struct MoonpoolEngine<P: moonpool_core::Providers> {
    _marker: PhantomData<fn() -> P>,
}

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
impl<P: moonpool_core::Providers> Clone for MoonpoolEngine<P> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl<P: moonpool_core::Providers> Debug for MoonpoolEngine<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MoonpoolEngine").finish_non_exhaustive()
    }
}

impl<P: moonpool_core::Providers> Engine for MoonpoolEngine<P> {
    type ClientState = magnetar_runtime_moonpool::Client<P>;
    // These are the same tokio shapes as the TokioEngine (ADR-0025
    // §Decision), and that is a real constraint rather than a free
    // abstraction: `spawn` is `tokio::spawn` and `new_interval` is
    // `tokio::time::interval`, so both read the HOST executor and the HOST
    // clock.
    //
    // That is fine under `TokioProviders`, but it is NOT usable on a
    // `SimProviders` path. Since ADR-0078, `SimTaskProvider` schedules on
    // Moonpool's own single-threaded seeded executor with no ambient tokio
    // runtime, and that ADR lists code depending on `tokio::time` or ambient
    // tokio task state as incompatible with the Moonpool engine. A ticker
    // built on these primitives would therefore be paced by wall-clock time
    // (or panic outright for want of a runtime) instead of replaying from a
    // seed. ADR-0037's "engine-invariant, not a tokio carve-out" reading of
    // these associated types predates ADR-0078 and no longer holds.
    //
    // Deterministic periodic work must go through the providers instead —
    // `providers.task().spawn_task(...)` + `providers.time().sleep(...)`, as
    // in `magnetar-runtime-moonpool`'s `AutoClusterFailover::start`. These
    // four `Engine` methods are static and take no providers argument, so
    // they cannot be retrofitted in place; they currently have no production
    // call sites.
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

    fn random_subscription_suffix() -> String {
        // Deterministic counter — every moonpool run produces the same
        // suffix sequence so `Reader` / `TableView` auto-names are
        // reproducible. Tests that need stronger isolation across
        // sub-tests should still pass an explicit subscription name.
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("sim-{n:016x}")
    }
}

// PIP-4 encryption hookup for the moonpool engine. The moonpool runtime now
// ships the same `MessageEncryptor` / `MessageDecryptor` trait surface as the
// tokio engine (`magnetar_runtime_moonpool::{MessageEncryptor, MessageDecryptor}`),
// so both associated types resolve to the runtime's `Arc<dyn …>` trait objects
// rather than the `NoEncryption` stub. The engine-generic `.create()` /
// `.subscribe()` paths still ignore the field; the moonpool-specialised
// `.create_with_encryption` / `.subscribe_with_decryption` builder methods
// consult it (mirroring the tokio specialisation).
impl<P: moonpool_core::Providers> MessageEncryptorApi for MoonpoolEngine<P> {
    type Encryptor = std::sync::Arc<dyn magnetar_runtime_moonpool::MessageEncryptor>;
}

impl<P: moonpool_core::Providers> MessageDecryptorApi for MoonpoolEngine<P> {
    type Decryptor = std::sync::Arc<dyn magnetar_runtime_moonpool::MessageDecryptor>;
}

// PIP-460 scalable topics (ADR-0093, experimental). 1:1 with the tokio
// engine's `ScalableTopicsApi` impl — maps the façade's engine-agnostic
// `ScalableLookup` / `ScalableEvent` onto the moonpool runtime's types.
#[cfg(feature = "scalable-topics")]
impl<P: moonpool_core::Providers + Send + Sync + 'static> super::ScalableTopicsApi
    for magnetar_runtime_moonpool::Client<P>
{
    type Error = magnetar_runtime_moonpool::ClientError;

    fn scalable_topic_lookup<'a>(
        &'a self,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<super::ScalableLookup, Self::Error>> + Send + 'a>> {
        Box::pin(async move {
            let l = magnetar_runtime_moonpool::Client::scalable_topic_lookup(self, topic).await?;
            Ok(super::ScalableLookup {
                session_id: l.session_id,
                resolved_topic_name: l.resolved_topic_name,
                controller_broker_url: l.controller_broker_url,
                controller_broker_url_tls: l.controller_broker_url_tls,
                snapshot: l.snapshot,
                segments: l.segments,
                epoch: l.epoch,
            })
        })
    }

    fn broker_supports_scalable_topics(&self) -> bool {
        magnetar_runtime_moonpool::Client::broker_supports_scalable_topics(self)
    }

    fn close_scalable_topic_session(&self, session_id: u64) {
        magnetar_runtime_moonpool::Client::close_scalable_topic_session(self, session_id);
    }

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
    > {
        Box::pin(async move {
            magnetar_runtime_moonpool::Client::scalable_topic_subscribe(
                self,
                topic,
                subscription,
                consumer_name,
                consumer_id,
                consumer_type,
            )
            .await
        })
    }

    fn watch_scalable_topics(
        &self,
        namespace: &str,
        property_filters: Vec<(String, String)>,
    ) -> Result<u64, Self::Error> {
        magnetar_runtime_moonpool::Client::watch_scalable_topics(self, namespace, property_filters)
    }

    fn close_scalable_topics_watch(&self, watch_id: u64) {
        magnetar_runtime_moonpool::Client::close_scalable_topics_watch(self, watch_id);
    }

    fn scalable_topics_snapshot(&self, watch_id: u64) -> Option<Vec<String>> {
        magnetar_runtime_moonpool::Client::scalable_topics_snapshot(self, watch_id)
    }

    fn broker_supports_tc_metadata_discovery(&self) -> bool {
        magnetar_runtime_moonpool::Client::broker_supports_tc_metadata_discovery(self)
    }

    fn watch_tc_assignments(&self) -> Result<u64, Self::Error> {
        magnetar_runtime_moonpool::Client::watch_tc_assignments(self)
    }

    fn close_tc_assignments_watch(&self, watch_id: u64) {
        magnetar_runtime_moonpool::Client::close_tc_assignments_watch(self, watch_id);
    }

    fn next_scalable_event(
        &self,
    ) -> Pin<Box<dyn Future<Output = Option<super::ScalableEvent>> + Send + '_>> {
        Box::pin(async move {
            magnetar_runtime_moonpool::Client::next_scalable_event(self)
                .await
                .map(map_scalable_event)
        })
    }
}

#[cfg(feature = "scalable-topics")]
impl<P: moonpool_core::Providers + Send + Sync + 'static> super::SegmentSubscriberApi
    for magnetar_runtime_moonpool::Client<P>
{
    type StreamConsumer = magnetar_runtime_moonpool::StreamConsumer<P>;

    fn subscribe_stream_consumer(
        &self,
        options: super::StreamConsumerOptions,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Self::StreamConsumer, crate::scalable::StreamConsumerError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let subscriber = magnetar_runtime_moonpool::Client::segment_subscriber(self)
                .map_err(|error| crate::scalable::StreamConsumerError::engine("moonpool", error))?;
            subscriber
                .subscribe_stream_consumer(magnetar_runtime_moonpool::StreamConsumerOptions {
                    topic: options.topic,
                    subscription: options.subscription,
                    consumer_name: options.consumer_name,
                    schema: options.schema,
                    receiver_budget: options.receiver_budget,
                    ordering_mode: options.ordering_mode,
                })
                .await
                .map_err(map_stream_consumer_error)
        })
    }
}

#[cfg(feature = "scalable-topics")]
impl<P: moonpool_core::Providers + Send + Sync + 'static> super::StreamConsumerBackend
    for magnetar_runtime_moonpool::StreamConsumer<P>
{
    fn receive(
        &self,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<super::RawStreamMessage, crate::scalable::StreamConsumerError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let message = magnetar_runtime_moonpool::StreamConsumer::receive(self)
                .await
                .map_err(map_stream_consumer_error)?;
            Ok(super::RawStreamMessage {
                message: message.message,
                token: message.token,
            })
        })
    }

    fn receive_batch(
        &self,
        policy: crate::scalable::BatchReceivePolicy,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Vec<super::RawStreamMessage>,
                        crate::scalable::StreamConsumerError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            magnetar_runtime_moonpool::StreamConsumer::receive_batch(
                self,
                policy.max_messages(),
                policy.max_bytes(),
                policy.max_wait(),
            )
            .await
            .map(|messages| {
                messages
                    .into_iter()
                    .map(|message| super::RawStreamMessage {
                        message: message.message,
                        token: message.token,
                    })
                    .collect()
            })
            .map_err(map_stream_consumer_error)
        })
    }

    fn restore_messages(&self, messages: Vec<super::RawStreamMessage>) {
        let result = magnetar_runtime_moonpool::StreamConsumer::restore_deliveries(
            self,
            messages
                .into_iter()
                .map(|message| magnetar_runtime_moonpool::StreamConsumerMessage {
                    message: message.message,
                    token: message.token,
                })
                .collect(),
        );
        if let Err(error) = result {
            magnetar_runtime_moonpool::StreamConsumer::delivery_restoration_failed(self, &error);
        }
    }

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
    > {
        Box::pin(async move {
            magnetar_runtime_moonpool::StreamConsumer::get_schema(self, source, version)
                .await
                .map_err(map_stream_consumer_error)
        })
    }

    fn acknowledge<'a>(
        &'a self,
        token: &'a magnetar_proto::DeliveryToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>
    {
        Box::pin(async move {
            magnetar_runtime_moonpool::StreamConsumer::acknowledge(self, token)
                .await
                .map_err(map_stream_consumer_error)
        })
    }

    fn acknowledge_cumulative<'a>(
        &'a self,
        token: &'a magnetar_proto::DeliveryToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>
    {
        Box::pin(async move {
            magnetar_runtime_moonpool::StreamConsumer::acknowledge_cumulative(self, token)
                .await
                .map_err(map_stream_consumer_error)
        })
    }

    fn acknowledge_positions<'a>(
        &'a self,
        positions: &'a magnetar_proto::PositionVector,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>
    {
        Box::pin(async move {
            magnetar_runtime_moonpool::StreamConsumer::acknowledge_positions(self, positions)
                .await
                .map_err(map_stream_consumer_error)
        })
    }

    fn acknowledge_batch<'a>(
        &'a self,
        tokens: Vec<&'a magnetar_proto::DeliveryToken>,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>
    {
        Box::pin(async move {
            magnetar_runtime_moonpool::StreamConsumer::acknowledge_batch(self, &tokens)
                .await
                .map_err(map_stream_consumer_error)
        })
    }

    fn negative_acknowledge(
        &self,
        token: &magnetar_proto::DeliveryToken,
    ) -> Result<(), crate::scalable::StreamConsumerError> {
        magnetar_runtime_moonpool::StreamConsumer::negative_acknowledge(self, token)
            .map_err(map_stream_consumer_error)
    }

    fn acknowledge_in_transaction<'a>(
        &'a self,
        token: &'a magnetar_proto::DeliveryToken,
        txn_id: magnetar_proto::TxnId,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>
    {
        Box::pin(async move {
            magnetar_runtime_moonpool::StreamConsumer::acknowledge_in_transaction(
                self, token, txn_id,
            )
            .await
            .map_err(map_stream_consumer_error)
        })
    }

    fn acknowledge_cumulative_in_transaction<'a>(
        &'a self,
        token: &'a magnetar_proto::DeliveryToken,
        txn_id: magnetar_proto::TxnId,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>
    {
        Box::pin(async move {
            magnetar_runtime_moonpool::StreamConsumer::acknowledge_cumulative_in_transaction(
                self, token, txn_id,
            )
            .await
            .map_err(map_stream_consumer_error)
        })
    }

    fn acknowledge_positions_in_transaction<'a>(
        &'a self,
        positions: &'a magnetar_proto::PositionVector,
        txn_id: magnetar_proto::TxnId,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>
    {
        Box::pin(async move {
            magnetar_runtime_moonpool::StreamConsumer::acknowledge_positions_in_transaction(
                self, positions, txn_id,
            )
            .await
            .map_err(map_stream_consumer_error)
        })
    }

    fn delivered_position(&self) -> magnetar_proto::PositionVector {
        magnetar_runtime_moonpool::StreamConsumer::delivered_position(self)
    }

    fn status(&self) -> crate::scalable::StreamConsumerStatus {
        map_stream_consumer_status(&magnetar_runtime_moonpool::StreamConsumer::status(self))
    }

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
    > {
        Box::pin(async move {
            magnetar_runtime_moonpool::StreamConsumer::next_event(self)
                .await
                .map(|event| event.map(map_stream_consumer_event))
                .map_err(map_stream_consumer_error)
        })
    }

    fn seek_positions<'a>(
        &'a self,
        positions: &'a magnetar_proto::PositionVector,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>
    {
        Box::pin(async move {
            magnetar_runtime_moonpool::StreamConsumer::seek_positions(self, positions)
                .await
                .map_err(map_stream_consumer_error)
        })
    }

    fn transaction_outcome(
        &self,
        txn_id: magnetar_proto::TxnId,
        outcome: crate::scalable::TransactionOutcome,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + '_>>
    {
        Box::pin(async move {
            magnetar_runtime_moonpool::StreamConsumer::transaction_outcome(
                self,
                txn_id,
                map_transaction_outcome(outcome),
            )
            .await
            .map_err(map_stream_consumer_error)
        })
    }

    fn close(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + '_>>
    {
        Box::pin(async move {
            magnetar_runtime_moonpool::StreamConsumer::close(self)
                .await
                .map_err(map_stream_consumer_error)
        })
    }

    fn close_best_effort(&self) {
        magnetar_runtime_moonpool::StreamConsumer::close_best_effort(self);
    }
}

#[cfg(feature = "scalable-topics")]
fn map_stream_consumer_error(
    error: magnetar_runtime_moonpool::StreamConsumerError,
) -> crate::scalable::StreamConsumerError {
    match error {
        magnetar_runtime_moonpool::StreamConsumerError::Model(error) => error.into(),
        magnetar_runtime_moonpool::StreamConsumerError::Client(error) => {
            crate::scalable::StreamConsumerError::engine("moonpool", error)
        }
        magnetar_runtime_moonpool::StreamConsumerError::PartialAcknowledgement {
            confirmed,
            failed,
        } => crate::scalable::StreamConsumerError::PartialAcknowledgement {
            confirmed,
            failed: failed
                .into_iter()
                .map(|failure| {
                    crate::scalable::StreamAckFailure::engine(
                        failure.position,
                        "moonpool",
                        failure.message,
                    )
                })
                .collect(),
        },
        magnetar_runtime_moonpool::StreamConsumerError::Closed => {
            crate::scalable::StreamConsumerError::engine("moonpool", "stream consumer is closed")
        }
        magnetar_runtime_moonpool::StreamConsumerError::Failed(message) => {
            crate::scalable::StreamConsumerError::engine("moonpool", message)
        }
    }
}

#[cfg(feature = "scalable-topics")]
fn map_stream_consumer_status(
    status: &magnetar_proto::StreamConsumerStatusSnapshot,
) -> crate::scalable::StreamConsumerStatus {
    let phase = status.phase();
    crate::scalable::StreamConsumerStatus::new(
        phase,
        (phase != magnetar_proto::AggregatePhase::ResyncRequired).then_some(status.layout_epoch()),
        status.assigned_segments(),
        status.attached_segments(),
        status.draining_segments(),
        status.pending_ownership().to_vec(),
        status.ordering_unprovable().to_vec(),
        status.receiver_budget_limit(),
        status.receiver_budget_used(),
    )
}

#[cfg(feature = "scalable-topics")]
fn map_transaction_outcome(
    outcome: crate::scalable::TransactionOutcome,
) -> magnetar_proto::TransactionAcknowledgementOutcome {
    match outcome {
        crate::scalable::TransactionOutcome::Committed => {
            magnetar_proto::TransactionAcknowledgementOutcome::Committed
        }
        crate::scalable::TransactionOutcome::Aborted => {
            magnetar_proto::TransactionAcknowledgementOutcome::Aborted
        }
        crate::scalable::TransactionOutcome::Unknown => {
            magnetar_proto::TransactionAcknowledgementOutcome::Unknown
        }
    }
}

#[cfg(feature = "scalable-topics")]
fn map_stream_consumer_event(
    event: magnetar_runtime_moonpool::StreamConsumerEvent,
) -> crate::scalable::StreamConsumerEvent {
    match event {
        magnetar_runtime_moonpool::StreamConsumerEvent::AssignmentApplied {
            layout_epoch,
            sources,
        } => crate::scalable::StreamConsumerEvent::AssignmentApplied {
            layout_epoch,
            sources,
        },
        magnetar_runtime_moonpool::StreamConsumerEvent::SegmentPhaseChanged { source, phase } => {
            crate::scalable::StreamConsumerEvent::SegmentPhaseChanged { source, phase }
        }
        magnetar_runtime_moonpool::StreamConsumerEvent::OrderingUnprovable {
            segment_id,
            ancestors,
        } => crate::scalable::StreamConsumerEvent::OrderingUnprovable {
            segment_id,
            ancestors,
        },
        magnetar_runtime_moonpool::StreamConsumerEvent::ResyncRequired { reason } => {
            crate::scalable::StreamConsumerEvent::ResyncRequired { reason }
        }
        magnetar_runtime_moonpool::StreamConsumerEvent::TransactionOutcome { txn_id, outcome } => {
            let outcome = match outcome {
                magnetar_proto::TransactionAcknowledgementOutcome::Committed => {
                    crate::scalable::TransactionOutcome::Committed
                }
                magnetar_proto::TransactionAcknowledgementOutcome::Aborted => {
                    crate::scalable::TransactionOutcome::Aborted
                }
                magnetar_proto::TransactionAcknowledgementOutcome::Unknown => {
                    crate::scalable::TransactionOutcome::Unknown
                }
            };
            crate::scalable::StreamConsumerEvent::TransactionOutcome { txn_id, outcome }
        }
        magnetar_runtime_moonpool::StreamConsumerEvent::Closed => {
            crate::scalable::StreamConsumerEvent::Closed
        }
    }
}

/// Map a moonpool-runtime `ScalableEvent` onto the façade's engine-agnostic one.
#[cfg(feature = "scalable-topics")]
fn map_scalable_event(ev: magnetar_runtime_moonpool::ScalableEvent) -> super::ScalableEvent {
    match ev {
        magnetar_runtime_moonpool::ScalableEvent::LookupResolved {
            session_id,
            resolved_topic_name,
            controller_broker_url,
            controller_broker_url_tls,
            snapshot,
            segments,
            epoch,
        } => super::ScalableEvent::LookupResolved {
            session_id,
            resolved_topic_name,
            controller_broker_url,
            controller_broker_url_tls,
            snapshot,
            segments,
            epoch,
        },
        magnetar_runtime_moonpool::ScalableEvent::DagUpdated {
            session_id,
            delta,
            snapshot,
        } => super::ScalableEvent::DagUpdated {
            session_id,
            delta,
            snapshot,
        },
        magnetar_runtime_moonpool::ScalableEvent::DagChangedDuringConsume {
            session_id,
            reason,
        } => super::ScalableEvent::DagChangedDuringConsume { session_id, reason },
        magnetar_runtime_moonpool::ScalableEvent::DagWatchClosed { session_id, reason } => {
            super::ScalableEvent::DagWatchClosed { session_id, reason }
        }
        magnetar_runtime_moonpool::ScalableEvent::ConsumerAssigned {
            consumer_id,
            incarnation,
            assignment,
        } => super::ScalableEvent::ConsumerAssigned {
            consumer_id,
            incarnation,
            assignment,
        },
        magnetar_runtime_moonpool::ScalableEvent::AssignmentChanged {
            consumer_id,
            incarnation,
            assignment,
            delta,
        } => super::ScalableEvent::AssignmentChanged {
            consumer_id,
            incarnation,
            assignment,
            delta,
        },
        magnetar_runtime_moonpool::ScalableEvent::ConsumerRejected {
            consumer_id,
            incarnation,
            reason,
        } => super::ScalableEvent::ConsumerRejected {
            consumer_id,
            incarnation,
            reason,
        },
        magnetar_runtime_moonpool::ScalableEvent::TopicsChanged { watch_id, change } => {
            super::ScalableEvent::TopicsChanged { watch_id, change }
        }
        magnetar_runtime_moonpool::ScalableEvent::TopicsWatchClosed { watch_id, reason } => {
            super::ScalableEvent::TopicsWatchClosed { watch_id, reason }
        }
        magnetar_runtime_moonpool::ScalableEvent::TcAssignmentsChanged {
            watch_id,
            parallelism,
            assignments,
        } => super::ScalableEvent::TcAssignmentsChanged {
            watch_id,
            parallelism,
            assignments,
        },
        magnetar_runtime_moonpool::ScalableEvent::TcAssignmentsWatchClosed { watch_id, reason } => {
            super::ScalableEvent::TcAssignmentsWatchClosed { watch_id, reason }
        }
    }
}

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
                conn.end_txn(txn, action).map_err(|err| {
                    magnetar_runtime_moonpool::ClientError::Other(format!("end_txn: {err}"))
                })?
            };
            let _waiter = MoonpoolEndTxnWaiterGuard::new(shared.clone(), txn, action);
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

struct MoonpoolEndTxnWaiterGuard {
    shared: std::sync::Arc<magnetar_runtime_moonpool::ConnectionShared>,
    txn: magnetar_proto::TxnId,
    action: magnetar_proto::TxnAction,
}

impl MoonpoolEndTxnWaiterGuard {
    fn new(
        shared: std::sync::Arc<magnetar_runtime_moonpool::ConnectionShared>,
        txn: magnetar_proto::TxnId,
        action: magnetar_proto::TxnAction,
    ) -> Self {
        Self {
            shared,
            txn,
            action,
        }
    }
}

impl Drop for MoonpoolEndTxnWaiterGuard {
    fn drop(&mut self) {
        self.shared
            .inner
            .lock()
            .release_end_txn_waiter(self.txn, self.action);
    }
}

/// Park on a request-id-correlated outcome from the moonpool engine's
/// shared connection state. Mirrors `magnetar_runtime_moonpool`'s
/// internal `RequestFut`; reproduced here because that type is
/// `pub(crate)` to the moonpool runtime.
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

#[cfg(feature = "tokio")]
impl<P: moonpool_core::Providers + Send + Sync + 'static> BrokerMetadataApi
    for magnetar_runtime_moonpool::Client<P>
{
    type Error = magnetar_runtime_moonpool::ClientError;

    fn partitioned_topic_metadata<'a>(
        &'a self,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<u32, Self::Error>> + Send + 'a>> {
        Box::pin(magnetar_runtime_moonpool::Client::partitioned_topic_metadata(self, topic))
    }

    fn new_metadata_operation_deadline(&self) -> OperationDeadline {
        OperationDeadline::new(magnetar_runtime_moonpool::Client::operation_timer(self))
    }

    fn partitioned_topic_metadata_with_deadline<'a>(
        &'a self,
        topic: &'a str,
        deadline: &'a mut OperationDeadline,
    ) -> Pin<Box<dyn Future<Output = Result<u32, Self::Error>> + Send + 'a>> {
        let (timer, last_broker_error) = deadline.parts();
        Box::pin(
            magnetar_runtime_moonpool::Client::partitioned_topic_metadata_with_operation_deadline(
                self,
                topic,
                timer,
                last_broker_error,
            ),
        )
    }

    fn watch_topic_list<'a>(
        &'a self,
        namespace: &'a str,
        pattern: &'a str,
    ) -> WatchTopicListFut<'a, Self> {
        Box::pin(magnetar_runtime_moonpool::Client::watch_topic_list(
            self, namespace, pattern,
        ))
    }

    fn watch_topic_list_with_deadline<'a>(
        &'a self,
        namespace: &'a str,
        pattern: &'a str,
        deadline: &'a mut OperationDeadline,
    ) -> WatchTopicListFut<'a, Self> {
        let (timer, last_broker_error) = deadline.parts();
        Box::pin(
            magnetar_runtime_moonpool::Client::watch_topic_list_with_operation_deadline(
                self,
                namespace,
                pattern,
                timer,
                last_broker_error,
            ),
        )
    }

    fn poll_topic_list_change(&self) -> Option<TopicListChange> {
        magnetar_runtime_moonpool::Client::poll_topic_list_change(self).map(|c| TopicListChange {
            added: c.added,
            removed: c.removed,
        })
    }
}

#[cfg(feature = "tokio")]
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

    fn new_subscribe_operation_deadline(&self) -> OperationDeadline {
        OperationDeadline::new(magnetar_runtime_moonpool::Client::operation_timer(self))
    }

    fn subscribe_with_deadline<'a>(
        &'a self,
        req: magnetar_proto::SubscribeRequest,
        deadline: &'a mut OperationDeadline,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Consumer, Self::Error>> + Send + 'a>> {
        let (timer, last_broker_error) = deadline.parts();
        Box::pin(
            magnetar_runtime_moonpool::Client::subscribe_with_operation_deadline(
                self,
                req,
                None,
                timer,
                last_broker_error,
            ),
        )
    }
}

#[cfg(feature = "tokio")]
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

    fn new_producer_operation_deadline(&self) -> OperationDeadline {
        OperationDeadline::new(magnetar_runtime_moonpool::Client::operation_timer(self))
    }

    fn open_producer_with_deadline<'a>(
        &'a self,
        req: magnetar_proto::CreateProducerRequest,
        deadline: &'a mut OperationDeadline,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Producer, Self::Error>> + Send + 'a>> {
        let (timer, last_broker_error) = deadline.parts();
        Box::pin(
            magnetar_runtime_moonpool::Client::open_producer_with_operation_deadline(
                self,
                req,
                None,
                timer,
                last_broker_error,
            ),
        )
    }
}

#[cfg(feature = "tokio")]
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
        version: Option<bytes::Bytes>,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::pb::Schema, Self::Error>> + Send + '_>>
    {
        Box::pin(magnetar_runtime_moonpool::Producer::get_schema(
            self, version,
        ))
    }

    fn stats(&self) -> magnetar_proto::producer::ProducerStats {
        magnetar_runtime_moonpool::Producer::stats(self)
    }

    fn send_latency_histogram(&self) -> Option<hdrhistogram::Histogram<u64>> {
        magnetar_runtime_moonpool::Producer::send_latency_histogram(self)
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

#[cfg(feature = "tokio")]
impl<P: moonpool_core::Providers + Send + Sync + 'static> ConsumerApi
    for magnetar_runtime_moonpool::Consumer<P>
{
    type Error = magnetar_runtime_moonpool::ClientError;
    type Producer = magnetar_runtime_moonpool::Producer<P>;

    fn receive(
        &self,
    ) -> Pin<
        Box<dyn Future<Output = Result<magnetar_proto::IncomingMessage, Self::Error>> + Send + '_>,
    > {
        Box::pin(magnetar_runtime_moonpool::Consumer::receive(self))
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
        version: Option<bytes::Bytes>,
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

    fn receive_latency_histogram(&self) -> Option<hdrhistogram::Histogram<u64>> {
        magnetar_runtime_moonpool::Consumer::receive_latency_histogram(self)
    }

    fn is_active(&self) -> Option<bool> {
        magnetar_runtime_moonpool::Consumer::is_active(self)
    }

    fn next_active_change(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<bool, Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_moonpool::Consumer::next_active_change(
            self,
        ))
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

    fn unsubscribe(
        &self,
        force: bool,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_moonpool::Consumer::unsubscribe(
            self, force,
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

    fn seek_to_message(
        &self,
        message_id: magnetar_proto::MessageId,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_moonpool::Consumer::seek_to_message(
            self, message_id,
        ))
    }

    fn seek_to_timestamp(
        &self,
        publish_time_ms: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_moonpool::Consumer::seek_to_timestamp(
            self,
            publish_time_ms,
        ))
    }

    fn pause(&self) {
        magnetar_runtime_moonpool::Consumer::pause(self);
    }

    fn resume(&self) {
        magnetar_runtime_moonpool::Consumer::resume(self);
    }

    fn available_in_queue(&self) -> usize {
        magnetar_runtime_moonpool::Consumer::available_in_queue(self)
    }

    fn available_permits(&self) -> u32 {
        magnetar_runtime_moonpool::Consumer::available_permits(self)
    }

    fn has_received_any_message(&self) -> bool {
        magnetar_runtime_moonpool::Consumer::has_received_any_message(self)
    }

    fn has_reached_end_of_topic(&self) -> bool {
        magnetar_runtime_moonpool::Consumer::has_reached_end_of_topic(self)
    }

    fn is_paused(&self) -> bool {
        magnetar_runtime_moonpool::Consumer::is_paused(self)
    }

    fn is_inactive(&self) -> bool {
        magnetar_runtime_moonpool::Consumer::is_inactive(self)
    }

    fn drain_dead_letter(&self) -> Vec<magnetar_proto::IncomingMessage> {
        magnetar_runtime_moonpool::Consumer::drain_dead_letter(self)
    }

    fn receive_with_timeout(&self, timeout: Duration) -> ReceiveOptFut<'_, Self> {
        Box::pin(magnetar_runtime_moonpool::Consumer::receive_with_timeout(
            self, timeout,
        ))
    }

    fn receive_batch(&self, max_messages: usize, max_wait: Duration) -> ReceiveBatchFut<'_, Self> {
        Box::pin(magnetar_runtime_moonpool::Consumer::receive_batch(
            self,
            max_messages,
            max_wait,
        ))
    }

    fn receive_batch_with_bytes_cap(
        &self,
        max_messages: usize,
        max_bytes: usize,
        max_wait: Duration,
    ) -> ReceiveBatchFut<'_, Self> {
        Box::pin(
            magnetar_runtime_moonpool::Consumer::receive_batch_with_bytes_cap(
                self,
                max_messages,
                max_bytes,
                max_wait,
            ),
        )
    }

    fn republish_dead_letters<'a>(
        &'a self,
        dlq_producer: &'a Self::Producer,
    ) -> Pin<Box<dyn Future<Output = Result<usize, Self::Error>> + Send + 'a>> {
        Box::pin(magnetar_runtime_moonpool::Consumer::republish_dead_letters(
            self,
            dlq_producer,
        ))
    }

    fn reconsume_later<'a>(
        &'a self,
        retry_producer: &'a Self::Producer,
        msg: magnetar_proto::IncomingMessage,
        delay: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        Box::pin(magnetar_runtime_moonpool::Consumer::reconsume_later(
            self,
            retry_producer,
            msg,
            delay,
        ))
    }

    fn reconsume_later_with_properties<'a>(
        &'a self,
        retry_producer: &'a Self::Producer,
        msg: magnetar_proto::IncomingMessage,
        custom_properties: Vec<(String, String)>,
        delay: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        Box::pin(
            magnetar_runtime_moonpool::Consumer::reconsume_later_with_properties(
                self,
                retry_producer,
                msg,
                custom_properties,
                delay,
            ),
        )
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
