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
    BrokerMetadataApi, ConsumerApi, CreateProducerApi, ProducerApi, ReceiveBatchFut, ReceiveOptFut,
    SubscribeApi, TopicListChange, WatchTopicListFut,
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

// PIP-460 scalable topics (ADR-0031, experimental). 1:1 with the tokio
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
                controller_broker_url: l.controller_broker_url,
                segments: l.segments,
                lookup_token: l.lookup_token,
            })
        })
    }

    fn open_dag_watch(
        &self,
        topic: &str,
        lookup_token: u64,
        segments: Vec<magnetar_proto::SegmentDescriptor>,
    ) -> u64 {
        magnetar_runtime_moonpool::Client::open_scalable_dag_watch(
            self,
            topic,
            lookup_token,
            segments,
        )
    }

    fn close_dag_watch(&self, watch_session_id: u64) {
        magnetar_runtime_moonpool::Client::close_scalable_dag_watch(self, watch_session_id);
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

/// Map a moonpool-runtime `ScalableEvent` onto the façade's engine-agnostic one.
#[cfg(feature = "scalable-topics")]
fn map_scalable_event(ev: magnetar_runtime_moonpool::ScalableEvent) -> super::ScalableEvent {
    match ev {
        magnetar_runtime_moonpool::ScalableEvent::LookupResolved {
            controller_broker_url,
            segments,
            lookup_token,
            ..
        } => super::ScalableEvent::LookupResolved {
            controller_broker_url,
            segments,
            lookup_token,
        },
        magnetar_runtime_moonpool::ScalableEvent::DagUpdated {
            watch_session_id,
            delta,
        } => super::ScalableEvent::DagUpdated {
            watch_session_id,
            delta,
        },
        magnetar_runtime_moonpool::ScalableEvent::DagChangedDuringConsume {
            watch_session_id,
            reason,
        } => super::ScalableEvent::DagChangedDuringConsume {
            watch_session_id,
            reason,
        },
        magnetar_runtime_moonpool::ScalableEvent::DagWatchClosed {
            watch_session_id,
            reason,
        } => super::ScalableEvent::DagWatchClosed {
            watch_session_id,
            reason,
        },
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

    fn watch_topic_list<'a>(
        &'a self,
        namespace: &'a str,
        pattern: &'a str,
    ) -> WatchTopicListFut<'a, Self> {
        Box::pin(magnetar_runtime_moonpool::Client::watch_topic_list(
            self, namespace, pattern,
        ))
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
