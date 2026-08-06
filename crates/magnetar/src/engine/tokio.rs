// SPDX-License-Identifier: Apache-2.0

//! [`TokioEngine`] — production runtime engine for the magnetar façade.
//!
//! This module carries the [`TokioEngine`] marker struct together with
//! every trait impl that pins the façade's per-surface extension traits
//! (`TransactionApi`, `ProducerApi`, `ConsumerApi`, `BrokerMetadataApi`,
//! `SubscribeApi`, `CreateProducerApi`) to the
//! [`magnetar_runtime_tokio`] client / producer / consumer types.
//!
//! Companion module to [`super::moonpool`]; the shared trait definitions
//! live in [`super`] (the engine module root).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use super::{
    BrokerMetadataApi, ConsumerApi, CreateProducerApi, Engine, MessageDecryptorApi,
    MessageEncryptorApi, OperationDeadline, ProducerApi, ReceiveBatchFut, ReceiveOptFut,
    SubscribeApi, TopicListChange, TransactionApi, WatchTopicListFut,
};

/// Zero-sized marker for the tokio production engine. Default `E` on
/// [`crate::PulsarClient<E>`].
///
/// Available behind the `tokio` feature (default-on).
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioEngine;

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

    fn random_subscription_suffix() -> String {
        uuid::Uuid::new_v4().simple().to_string()
    }
}

// PIP-4 encryption hookup for the tokio engine. The associated types
// plug the existing `magnetar_runtime_tokio::MessageEncryptor` /
// `MessageDecryptor` trait objects (already `Send + Sync + Debug`) into
// the engine-generic builder storage via the API extension traits.
impl MessageEncryptorApi for TokioEngine {
    type Encryptor = Arc<dyn magnetar_runtime_tokio::MessageEncryptor>;
}

impl MessageDecryptorApi for TokioEngine {
    type Decryptor = Arc<dyn magnetar_runtime_tokio::MessageDecryptor>;
}

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

impl BrokerMetadataApi for magnetar_runtime_tokio::Client {
    type Error = magnetar_runtime_tokio::ClientError;

    fn partitioned_topic_metadata<'a>(
        &'a self,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<u32, Self::Error>> + Send + 'a>> {
        Box::pin(magnetar_runtime_tokio::Client::partitioned_topic_metadata(
            self, topic,
        ))
    }

    fn new_metadata_operation_deadline(&self) -> OperationDeadline {
        OperationDeadline::new(magnetar_runtime_tokio::Client::operation_timer(self))
    }

    fn partitioned_topic_metadata_with_deadline<'a>(
        &'a self,
        topic: &'a str,
        deadline: &'a mut OperationDeadline,
    ) -> Pin<Box<dyn Future<Output = Result<u32, Self::Error>> + Send + 'a>> {
        let (timer, last_broker_error) = deadline.parts();
        Box::pin(
            magnetar_runtime_tokio::Client::partitioned_topic_metadata_with_operation_deadline(
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
        Box::pin(magnetar_runtime_tokio::Client::watch_topic_list(
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
            magnetar_runtime_tokio::Client::watch_topic_list_with_operation_deadline(
                self,
                namespace,
                pattern,
                timer,
                last_broker_error,
            ),
        )
    }

    fn poll_topic_list_change(&self) -> Option<TopicListChange> {
        magnetar_runtime_tokio::Client::poll_topic_list_change(self).map(|c| TopicListChange {
            added: c.added,
            removed: c.removed,
        })
    }
}

impl SubscribeApi for magnetar_runtime_tokio::Client {
    type Consumer = magnetar_runtime_tokio::Consumer;
    type Error = magnetar_runtime_tokio::ClientError;

    fn subscribe(
        &self,
        req: magnetar_proto::SubscribeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Consumer, Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Client::subscribe(self, req))
    }

    fn new_subscribe_operation_deadline(&self) -> OperationDeadline {
        OperationDeadline::new(magnetar_runtime_tokio::Client::operation_timer(self))
    }

    fn subscribe_with_deadline<'a>(
        &'a self,
        req: magnetar_proto::SubscribeRequest,
        deadline: &'a mut OperationDeadline,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Consumer, Self::Error>> + Send + 'a>> {
        let (timer, last_broker_error) = deadline.parts();
        Box::pin(
            magnetar_runtime_tokio::Client::subscribe_with_operation_deadline(
                self,
                req,
                None,
                timer,
                last_broker_error,
            ),
        )
    }
}

impl CreateProducerApi for magnetar_runtime_tokio::Client {
    type Producer = magnetar_runtime_tokio::Producer;
    type Error = magnetar_runtime_tokio::ClientError;

    fn open_producer(
        &self,
        req: magnetar_proto::CreateProducerRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Producer, Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Client::open_producer(self, req))
    }

    fn new_producer_operation_deadline(&self) -> OperationDeadline {
        OperationDeadline::new(magnetar_runtime_tokio::Client::operation_timer(self))
    }

    fn open_producer_with_deadline<'a>(
        &'a self,
        req: magnetar_proto::CreateProducerRequest,
        deadline: &'a mut OperationDeadline,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Producer, Self::Error>> + Send + 'a>> {
        let (timer, last_broker_error) = deadline.parts();
        Box::pin(
            magnetar_runtime_tokio::Client::open_producer_with_operation_deadline(
                self,
                req,
                None,
                timer,
                last_broker_error,
            ),
        )
    }
}

impl ProducerApi for magnetar_runtime_tokio::Producer {
    type Error = magnetar_runtime_tokio::ClientError;

    fn send(
        &self,
        mut msg: crate::OutgoingMessage,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::MessageId, Self::Error>> + Send + '_>>
    {
        crate::inject_otel_context(&mut msg.properties);
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
        version: Option<bytes::Bytes>,
    ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::pb::Schema, Self::Error>> + Send + '_>>
    {
        Box::pin(magnetar_runtime_tokio::Producer::get_schema(self, version))
    }

    fn stats(&self) -> magnetar_proto::producer::ProducerStats {
        magnetar_runtime_tokio::Producer::stats(self)
    }

    fn send_latency_histogram(&self) -> Option<hdrhistogram::Histogram<u64>> {
        magnetar_runtime_tokio::Producer::send_latency_histogram(self)
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

// PIP-460 scalable topics (ADR-0093, experimental). Maps the façade's
// engine-agnostic `ScalableLookup` / `ScalableEvent` onto the tokio runtime's
// identically-shaped types.
#[cfg(feature = "scalable-topics")]
impl super::ScalableTopicsApi for magnetar_runtime_tokio::Client {
    type Error = magnetar_runtime_tokio::ClientError;

    fn scalable_topic_lookup<'a>(
        &'a self,
        topic: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<super::ScalableLookup, Self::Error>> + Send + 'a>> {
        Box::pin(async move {
            let l = magnetar_runtime_tokio::Client::scalable_topic_lookup(self, topic).await?;
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
        magnetar_runtime_tokio::Client::broker_supports_scalable_topics(self)
    }

    fn close_scalable_topic_session(&self, session_id: u64) {
        magnetar_runtime_tokio::Client::close_scalable_topic_session(self, session_id);
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
            magnetar_runtime_tokio::Client::scalable_topic_subscribe(
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
        magnetar_runtime_tokio::Client::watch_scalable_topics(self, namespace, property_filters)
    }

    fn close_scalable_topics_watch(&self, watch_id: u64) {
        magnetar_runtime_tokio::Client::close_scalable_topics_watch(self, watch_id);
    }

    fn scalable_topics_snapshot(&self, watch_id: u64) -> Option<Vec<String>> {
        magnetar_runtime_tokio::Client::scalable_topics_snapshot(self, watch_id)
    }

    fn broker_supports_tc_metadata_discovery(&self) -> bool {
        magnetar_runtime_tokio::Client::broker_supports_tc_metadata_discovery(self)
    }

    fn watch_tc_assignments(&self) -> Result<u64, Self::Error> {
        magnetar_runtime_tokio::Client::watch_tc_assignments(self)
    }

    fn close_tc_assignments_watch(&self, watch_id: u64) {
        magnetar_runtime_tokio::Client::close_tc_assignments_watch(self, watch_id);
    }

    fn next_scalable_event(
        &self,
    ) -> Pin<Box<dyn Future<Output = Option<super::ScalableEvent>> + Send + '_>> {
        Box::pin(async move {
            magnetar_runtime_tokio::Client::next_scalable_event(self)
                .await
                .map(map_scalable_event)
        })
    }
}

#[cfg(feature = "scalable-topics")]
impl super::SegmentSubscriberApi for magnetar_runtime_tokio::Client {
    type StreamConsumer = magnetar_runtime_tokio::StreamConsumer;

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
            let subscriber = magnetar_runtime_tokio::Client::segment_subscriber(self)
                .map_err(|error| crate::scalable::StreamConsumerError::engine("tokio", error))?;
            subscriber
                .subscribe_stream_consumer(magnetar_runtime_tokio::StreamConsumerOptions {
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
impl super::StreamConsumerBackend for magnetar_runtime_tokio::StreamConsumer {
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
            let message = magnetar_runtime_tokio::StreamConsumer::receive(self)
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
            magnetar_runtime_tokio::StreamConsumer::receive_batch(
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
        let result = magnetar_runtime_tokio::StreamConsumer::restore_deliveries(
            self,
            messages
                .into_iter()
                .map(|message| magnetar_runtime_tokio::StreamConsumerMessage {
                    message: message.message,
                    token: message.token,
                })
                .collect(),
        );
        if let Err(error) = result {
            magnetar_runtime_tokio::StreamConsumer::delivery_restoration_failed(self, &error);
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
            magnetar_runtime_tokio::StreamConsumer::get_schema(self, source, version)
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
            magnetar_runtime_tokio::StreamConsumer::acknowledge(self, token)
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
            magnetar_runtime_tokio::StreamConsumer::acknowledge_cumulative(self, token)
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
            magnetar_runtime_tokio::StreamConsumer::acknowledge_positions(self, positions)
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
            magnetar_runtime_tokio::StreamConsumer::acknowledge_batch(self, &tokens)
                .await
                .map_err(map_stream_consumer_error)
        })
    }

    fn negative_acknowledge(
        &self,
        token: &magnetar_proto::DeliveryToken,
    ) -> Result<(), crate::scalable::StreamConsumerError> {
        magnetar_runtime_tokio::StreamConsumer::negative_acknowledge(self, token)
            .map_err(map_stream_consumer_error)
    }

    fn acknowledge_in_transaction<'a>(
        &'a self,
        token: &'a magnetar_proto::DeliveryToken,
        txn_id: magnetar_proto::TxnId,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + 'a>>
    {
        Box::pin(async move {
            magnetar_runtime_tokio::StreamConsumer::acknowledge_in_transaction(self, token, txn_id)
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
            magnetar_runtime_tokio::StreamConsumer::acknowledge_cumulative_in_transaction(
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
            magnetar_runtime_tokio::StreamConsumer::acknowledge_positions_in_transaction(
                self, positions, txn_id,
            )
            .await
            .map_err(map_stream_consumer_error)
        })
    }

    fn delivered_position(&self) -> magnetar_proto::PositionVector {
        magnetar_runtime_tokio::StreamConsumer::delivered_position(self)
    }

    fn status(&self) -> crate::scalable::StreamConsumerStatus {
        map_stream_consumer_status(&magnetar_runtime_tokio::StreamConsumer::status(self))
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
            magnetar_runtime_tokio::StreamConsumer::next_event(self)
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
            magnetar_runtime_tokio::StreamConsumer::seek_positions(self, positions)
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
            magnetar_runtime_tokio::StreamConsumer::transaction_outcome(
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
            magnetar_runtime_tokio::StreamConsumer::close(self)
                .await
                .map_err(map_stream_consumer_error)
        })
    }

    fn close_best_effort(&self) {
        magnetar_runtime_tokio::StreamConsumer::close_best_effort(self);
    }
}

#[cfg(feature = "scalable-topics")]
fn map_stream_consumer_error(
    error: magnetar_runtime_tokio::StreamConsumerError,
) -> crate::scalable::StreamConsumerError {
    match error {
        magnetar_runtime_tokio::StreamConsumerError::Model(error) => error.into(),
        magnetar_runtime_tokio::StreamConsumerError::Client(error) => {
            crate::scalable::StreamConsumerError::engine("tokio", error)
        }
        magnetar_runtime_tokio::StreamConsumerError::PartialAcknowledgement {
            confirmed,
            failed,
        } => crate::scalable::StreamConsumerError::PartialAcknowledgement {
            confirmed,
            failed: failed
                .into_iter()
                .map(|failure| {
                    crate::scalable::StreamAckFailure::engine(
                        failure.position,
                        "tokio",
                        failure.message,
                    )
                })
                .collect(),
        },
        magnetar_runtime_tokio::StreamConsumerError::Closed => {
            crate::scalable::StreamConsumerError::engine("tokio", "stream consumer is closed")
        }
        magnetar_runtime_tokio::StreamConsumerError::Failed(message) => {
            crate::scalable::StreamConsumerError::engine("tokio", message)
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
    event: magnetar_runtime_tokio::StreamConsumerEvent,
) -> crate::scalable::StreamConsumerEvent {
    match event {
        magnetar_runtime_tokio::StreamConsumerEvent::AssignmentApplied {
            layout_epoch,
            sources,
        } => crate::scalable::StreamConsumerEvent::AssignmentApplied {
            layout_epoch,
            sources,
        },
        magnetar_runtime_tokio::StreamConsumerEvent::SegmentPhaseChanged { source, phase } => {
            crate::scalable::StreamConsumerEvent::SegmentPhaseChanged { source, phase }
        }
        magnetar_runtime_tokio::StreamConsumerEvent::OrderingUnprovable {
            segment_id,
            ancestors,
        } => crate::scalable::StreamConsumerEvent::OrderingUnprovable {
            segment_id,
            ancestors,
        },
        magnetar_runtime_tokio::StreamConsumerEvent::ResyncRequired { reason } => {
            crate::scalable::StreamConsumerEvent::ResyncRequired { reason }
        }
        magnetar_runtime_tokio::StreamConsumerEvent::TransactionOutcome { txn_id, outcome } => {
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
        magnetar_runtime_tokio::StreamConsumerEvent::Closed => {
            crate::scalable::StreamConsumerEvent::Closed
        }
    }
}

/// Map a tokio-runtime `ScalableEvent` onto the façade's engine-agnostic one.
#[cfg(feature = "scalable-topics")]
fn map_scalable_event(ev: magnetar_runtime_tokio::ScalableEvent) -> super::ScalableEvent {
    match ev {
        magnetar_runtime_tokio::ScalableEvent::LookupResolved {
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
        magnetar_runtime_tokio::ScalableEvent::DagUpdated {
            session_id,
            delta,
            snapshot,
        } => super::ScalableEvent::DagUpdated {
            session_id,
            delta,
            snapshot,
        },
        magnetar_runtime_tokio::ScalableEvent::DagChangedDuringConsume { session_id, reason } => {
            super::ScalableEvent::DagChangedDuringConsume { session_id, reason }
        }
        magnetar_runtime_tokio::ScalableEvent::DagWatchClosed { session_id, reason } => {
            super::ScalableEvent::DagWatchClosed { session_id, reason }
        }
        magnetar_runtime_tokio::ScalableEvent::ConsumerAssigned {
            consumer_id,
            incarnation,
            assignment,
        } => super::ScalableEvent::ConsumerAssigned {
            consumer_id,
            incarnation,
            assignment,
        },
        magnetar_runtime_tokio::ScalableEvent::AssignmentChanged {
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
        magnetar_runtime_tokio::ScalableEvent::ConsumerRejected {
            consumer_id,
            incarnation,
            reason,
        } => super::ScalableEvent::ConsumerRejected {
            consumer_id,
            incarnation,
            reason,
        },
        magnetar_runtime_tokio::ScalableEvent::TopicsChanged { watch_id, change } => {
            super::ScalableEvent::TopicsChanged { watch_id, change }
        }
        magnetar_runtime_tokio::ScalableEvent::TopicsWatchClosed { watch_id, reason } => {
            super::ScalableEvent::TopicsWatchClosed { watch_id, reason }
        }
        magnetar_runtime_tokio::ScalableEvent::TcAssignmentsChanged {
            watch_id,
            parallelism,
            assignments,
        } => super::ScalableEvent::TcAssignmentsChanged {
            watch_id,
            parallelism,
            assignments,
        },
        magnetar_runtime_tokio::ScalableEvent::TcAssignmentsWatchClosed { watch_id, reason } => {
            super::ScalableEvent::TcAssignmentsWatchClosed { watch_id, reason }
        }
    }
}

impl ConsumerApi for magnetar_runtime_tokio::Consumer {
    type Error = magnetar_runtime_tokio::ClientError;
    type Producer = magnetar_runtime_tokio::Producer;

    fn receive(
        &self,
    ) -> Pin<
        Box<dyn Future<Output = Result<magnetar_proto::IncomingMessage, Self::Error>> + Send + '_>,
    > {
        Box::pin(magnetar_runtime_tokio::Consumer::receive(self))
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
        version: Option<bytes::Bytes>,
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

    fn receive_latency_histogram(&self) -> Option<hdrhistogram::Histogram<u64>> {
        magnetar_runtime_tokio::Consumer::receive_latency_histogram(self)
    }

    fn is_active(&self) -> Option<bool> {
        magnetar_runtime_tokio::Consumer::is_active(self)
    }

    fn next_active_change(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<bool, Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Consumer::next_active_change(self))
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

    fn unsubscribe(
        &self,
        force: bool,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Consumer::unsubscribe(self, force))
    }

    fn seek_to_earliest(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Consumer::seek_to_earliest(self))
    }

    fn seek_to_latest(&self) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Consumer::seek_to_latest(self))
    }

    fn seek_to_message(
        &self,
        message_id: magnetar_proto::MessageId,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Consumer::seek_to_message(
            self, message_id,
        ))
    }

    fn seek_to_timestamp(
        &self,
        publish_time_ms: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(magnetar_runtime_tokio::Consumer::seek_to_timestamp(
            self,
            publish_time_ms,
        ))
    }

    fn pause(&self) {
        magnetar_runtime_tokio::Consumer::pause(self);
    }

    fn resume(&self) {
        magnetar_runtime_tokio::Consumer::resume(self);
    }

    fn available_in_queue(&self) -> usize {
        magnetar_runtime_tokio::Consumer::available_in_queue(self)
    }

    fn available_permits(&self) -> u32 {
        magnetar_runtime_tokio::Consumer::available_permits(self)
    }

    fn has_received_any_message(&self) -> bool {
        magnetar_runtime_tokio::Consumer::has_received_any_message(self)
    }

    fn has_reached_end_of_topic(&self) -> bool {
        magnetar_runtime_tokio::Consumer::has_reached_end_of_topic(self)
    }

    fn is_paused(&self) -> bool {
        magnetar_runtime_tokio::Consumer::is_paused(self)
    }

    fn is_inactive(&self) -> bool {
        magnetar_runtime_tokio::Consumer::is_inactive(self)
    }

    fn drain_dead_letter(&self) -> Vec<magnetar_proto::IncomingMessage> {
        magnetar_runtime_tokio::Consumer::drain_dead_letter(self)
    }

    fn receive_with_timeout(&self, timeout: Duration) -> ReceiveOptFut<'_, Self> {
        Box::pin(magnetar_runtime_tokio::Consumer::receive_with_timeout(
            self, timeout,
        ))
    }

    fn receive_batch(&self, max_messages: usize, max_wait: Duration) -> ReceiveBatchFut<'_, Self> {
        Box::pin(magnetar_runtime_tokio::Consumer::receive_batch(
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
            magnetar_runtime_tokio::Consumer::receive_batch_with_bytes_cap(
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
        Box::pin(magnetar_runtime_tokio::Consumer::republish_dead_letters(
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
        Box::pin(magnetar_runtime_tokio::Consumer::reconsume_later(
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
            magnetar_runtime_tokio::Consumer::reconsume_later_with_properties(
                self,
                retry_producer,
                msg,
                custom_properties,
                delay,
            ),
        )
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
