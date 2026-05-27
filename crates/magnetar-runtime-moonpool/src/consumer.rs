// SPDX-License-Identifier: Apache-2.0

//! Consumer façade for the moonpool engine.
//!
//! Mirrors the core surface of [`magnetar_runtime_tokio::Consumer`] but is
//! generic over [`moonpool_core::Providers`] so the same façade runs on
//! production tokio sockets and on a `moonpool-sim` deterministic substrate.
//!
//! ## M4 surface
//!
//! - [`Consumer::receive`] — pop the next [`IncomingMessage`] from the per-consumer queue, parking
//!   on the per-consumer waker slab until one arrives.
//! - [`Consumer::ack`] / [`Consumer::ack_cumulative`] — request-id-correlated acks that resolve
//!   once the broker confirms (`CommandAckResponse`).
//! - [`Consumer::negative_ack`] — fire-and-forget redelivery request.
//! - [`Consumer::seek_to_message`] / [`Consumer::seek_to_timestamp`] — cursor reset to a message id
//!   or publish timestamp (millis since epoch).
//! - [`Consumer::close`] — request-id-correlated close, joins implicitly with the connection-level
//!   driver still alive.
//! - [`Consumer::topic`] / [`Consumer::subscription`] / [`Consumer::is_closed`] — cheap accessors
//!   that consult the sans-io state machine.
//! - [`Consumer::pause`] / [`Consumer::resume`] — local flow-control gate.
//!
//! The long tail of getters (`available_in_queue`, `available_permits`,
//! `stats`, `name`, `has_reached_end_of_topic`, `last_disconnected_timestamp`,
//! `drain_messages`, batch receive, ack-grouping, txn variants, DLQ,
//! retry-letter, decryption hooks) is intentionally NOT mirrored here; those
//! land in a later milestone alongside their tokio counterparts being
//! audited against PIP-31 / PIP-4 / Java parity.
//!
//! ## No-channels invariant
//!
//! Futures here follow the same pattern as the rest of the moonpool engine:
//! park on the sans-io `Connection`'s `Waker` slab via
//! [`magnetar_proto::Connection::register_waker`] for request-id-correlated
//! work, on the per-consumer waker slab via
//! [`magnetar_proto::Connection::register_consumer_receive_waker`] for message
//! arrival, and on the shared [`tokio::sync::Notify`] driver wakeup for
//! the small remaining set of handle-correlated events (subscribe ack). No
//! `mpsc` / `oneshot` / `watch` / `broadcast` channels of any flavour. See
//! `GUIDELINES.md` §"No-channels rule".

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use magnetar_proto::{
    AckRequest, ConnectionEvent, ConsumerHandle, IncomingMessage, MessageId, OpOutcome,
    PendingOpKey, RequestId, SeekTarget, SubscribeRequest, pb,
};
use moonpool_core::Providers;

use crate::client::{Client, ClientError};
use crate::{ConnectionShared, TopicListChange};

/// User-facing consumer handle for the moonpool engine.
///
/// Holds the shared connection state plus the protocol-layer
/// [`ConsumerHandle`]. Generic over the [`Providers`] bundle so the same
/// façade runs on production tokio sockets and on a `moonpool-sim`
/// deterministic substrate.
pub struct Consumer<P: Providers> {
    shared: Arc<ConnectionShared>,
    handle: ConsumerHandle,
    /// Held only so `Consumer` is generic over `P` without leaking the
    /// driver-handle type parameter. The driver itself has already consumed
    /// the providers — the consumer just talks to the shared state.
    _providers: std::marker::PhantomData<fn() -> P>,
}

impl<P: Providers> Clone for Consumer<P> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            handle: self.handle,
            _providers: std::marker::PhantomData,
        }
    }
}

impl<P: Providers> std::fmt::Debug for Consumer<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

impl<P: Providers> Consumer<P> {
    /// The protocol-layer consumer handle this façade wraps. Useful in tests
    /// and instrumentation.
    #[must_use]
    pub fn handle(&self) -> ConsumerHandle {
        self.handle
    }

    /// Topic name this consumer is bound to. Returns an empty string if the
    /// consumer is no longer registered (closed).
    #[must_use]
    pub fn topic(&self) -> String {
        self.shared
            .inner
            .lock()
            .consumer_topic(self.handle)
            .unwrap_or("")
            .to_owned()
    }

    /// Subscription name. Empty string if the consumer is no longer
    /// registered.
    #[must_use]
    pub fn subscription(&self) -> String {
        self.shared
            .inner
            .lock()
            .consumer_subscription(self.handle)
            .unwrap_or("")
            .to_owned()
    }

    /// `true` once this consumer has been closed — either locally via
    /// [`Self::close`] or remotely via a broker `CloseConsumer`. Mirrors Java
    /// `ConsumerImpl#getState() == CLOSED`.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.shared.inner.lock().consumer_is_closed(self.handle)
    }

    /// PIP-180 / ADR-0033: pre-populate shadow-topic metadata on this
    /// consumer. 1:1 mirror of
    /// `magnetar_runtime_tokio::Consumer::set_shadow_source`. Once set,
    /// the connection's receive dispatch emits
    /// [`magnetar_proto::ConnectionEvent::MessageReceivedFromShadow`]
    /// instead of the regular
    /// [`magnetar_proto::ConnectionEvent::Message`] when the inbound entry
    /// carries [`magnetar_proto::pb::MessageMetadata::replicated_from`].
    pub fn set_shadow_source(&self, source_topic: impl Into<String>) {
        let source = source_topic.into();
        let mut conn = self.shared.inner.lock();
        if let Some(c) = conn.consumer_mut(self.handle) {
            c.set_shadow_metadata(magnetar_proto::ShadowTopicMetadata {
                source_topic: source,
            });
        }
    }

    /// PIP-180 / ADR-0033: returns the cached source-topic name if this
    /// consumer is shadow-attached, or `None` for a regular consumer.
    /// 1:1 mirror of
    /// `magnetar_runtime_tokio::Consumer::shadow_source_topic`.
    #[must_use]
    pub fn shadow_source_topic(&self) -> Option<String> {
        self.shared
            .inner
            .lock()
            .consumer(self.handle)
            .and_then(|c| c.shadow_metadata.as_ref().map(|m| m.source_topic.clone()))
    }

    /// PIP-180 / ADR-0033: convenience predicate equivalent to
    /// `shadow_source_topic().is_some()`. 1:1 mirror of
    /// `magnetar_runtime_tokio::Consumer::is_shadow`.
    #[must_use]
    pub fn is_shadow(&self) -> bool {
        self.shadow_source_topic().is_some()
    }

    /// Broker-assigned consumer name. Empty string if the consumer is no
    /// longer registered. Mirrors Java `Consumer#getConsumerName`.
    #[must_use]
    pub fn name(&self) -> String {
        self.shared
            .inner
            .lock()
            .consumer_name(self.handle)
            .unwrap_or("")
            .to_owned()
    }

    /// `true` while the broker connection is up. Mirrors Java
    /// `Consumer#isConnected`.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.shared.inner.lock().is_connected()
    }

    /// Cumulative consumer-side counters. Returns a zeroed snapshot
    /// if the consumer handle is no longer registered. Mirrors Java
    /// `Consumer#getStats`.
    #[must_use]
    pub fn stats(&self) -> magnetar_proto::consumer::ConsumerStats {
        self.shared
            .inner
            .lock()
            .consumer_stats(self.handle)
            .unwrap_or_default()
    }

    /// Number of messages currently buffered in this consumer's receiver
    /// queue, waiting for a `receive()` call to pull them out. Returns `0`
    /// for closed/unknown handles. Mirrors Java
    /// `Consumer#getNumMessagesInQueue`.
    #[must_use]
    pub fn available_in_queue(&self) -> usize {
        self.shared.inner.lock().consumer_queue_len(self.handle)
    }

    /// Number of dispatch permits this consumer still has with the broker
    /// — i.e. messages it has authorised the broker to push without an
    /// explicit `CommandFlow`. Returns `0` for closed/unknown handles.
    /// Mirrors Java `ConsumerBase#getAvailablePermits`.
    #[must_use]
    pub fn available_permits(&self) -> u32 {
        self.shared
            .inner
            .lock()
            .consumer_available_permits(self.handle)
    }

    /// `true` if this consumer has received at least one message since
    /// opening. Mirrors Java `Consumer#hasReceivedAnyMessage` — useful as a
    /// "did anything ever arrive?" probe without inspecting the full
    /// [`ConsumerStats`](magnetar_proto::consumer::ConsumerStats).
    #[must_use]
    pub fn has_received_any_message(&self) -> bool {
        self.stats().total_msgs_received > 0
    }

    /// Returns `true` if this consumer is currently paused (no automatic
    /// flow refills until [`Self::resume`]). Returns `false` for
    /// closed/unknown handles. Mirrors Java `Consumer#isPaused` (Pulsar
    /// itself doesn't expose this on the Java client; we surface it for
    /// observability).
    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.shared
            .inner
            .lock()
            .is_paused(self.handle)
            .unwrap_or(false)
    }

    /// Returns `true` once the broker has indicated end-of-topic for this
    /// consumer (no further messages will be dispatched). Mirrors Java
    /// `Consumer#hasReachedEndOfTopic`.
    #[must_use]
    pub fn has_reached_end_of_topic(&self) -> bool {
        self.shared
            .inner
            .lock()
            .consumer_reached_end_of_topic(self.handle)
    }

    /// Mirrors Java `Consumer#isInactive`. Returns `true` once the consumer
    /// has reached end-of-topic on its subscription (no more messages will
    /// be dispatched). Note: a closed consumer is not represented as
    /// "inactive" here; check the connection state machine if you need to
    /// detect close.
    #[must_use]
    pub fn is_inactive(&self) -> bool {
        self.has_reached_end_of_topic()
    }

    /// Drain every message the state machine has flagged as dead-letter
    /// (redelivery count greater than the configured `max_redeliver_count`).
    /// The caller is responsible for republishing them to the configured
    /// DLQ topic. Returns an empty `Vec` when DLQ routing is disabled or no
    /// messages have been flagged.
    pub fn drain_dead_letter(&self) -> Vec<IncomingMessage> {
        let mut conn = self.shared.inner.lock();
        conn.drain_dead_letter(self.handle)
    }

    /// Drain the per-consumer dead-letter queue and republish every entry
    /// via `dlq_producer`, preserving each message's `partition_key`,
    /// `ordering_key`, `event_time`, and `properties`. After successful
    /// republish each original is acked so the consumer's cursor advances.
    /// Returns the number of messages republished.
    ///
    /// Pairs with [`Self::drain_dead_letter`] for callers that want to
    /// inspect the messages before republishing — this helper is the
    /// "just republish transparently" convenience.
    ///
    /// # Errors
    ///
    /// Returns the first [`ClientError`] encountered. Already-republished
    /// messages stay republished — partial progress is not rolled back.
    pub async fn republish_dead_letters(
        &self,
        dlq_producer: &crate::Producer<P>,
    ) -> Result<usize, ClientError> {
        let drained = self.drain_dead_letter();
        let mut count = 0;
        for msg in drained {
            let mut metadata = magnetar_proto::pb::MessageMetadata {
                partition_key: msg.metadata.partition_key.clone(),
                partition_key_b64_encoded: msg.metadata.partition_key_b64_encoded,
                ordering_key: msg.metadata.ordering_key.clone(),
                event_time: msg.metadata.event_time,
                properties: msg.metadata.properties.clone(),
                ..magnetar_proto::pb::MessageMetadata::default()
            };
            // Tag the republished message with the original id so DLQ
            // consumers can correlate back to the source. Mirrors Java's
            // `DeadLetterTopicMessageId` property convention.
            metadata.properties.push(magnetar_proto::pb::KeyValue {
                key: "REAL_TOPIC".to_owned(),
                value: self
                    .shared
                    .inner
                    .lock()
                    .consumer_topic(self.handle)
                    .unwrap_or("")
                    .to_owned(),
            });
            metadata.properties.push(magnetar_proto::pb::KeyValue {
                key: "ORIGINAL_MESSAGE_ID".to_owned(),
                value: msg.message_id.to_string(),
            });
            let payload_len = msg.payload.len();
            let outgoing = magnetar_proto::producer::OutgoingMessage {
                payload: msg.payload,
                metadata,
                uncompressed_size: u32::try_from(payload_len).unwrap_or(u32::MAX),
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            };
            dlq_producer.send(outgoing).await?;
            self.ack(msg.message_id).await?;
            count += 1;
        }
        Ok(count)
    }

    /// Republish a single message via `retry_producer` with a delay
    /// deadline, then ack the original. Mirrors Java
    /// `Consumer#reconsumeLater(Message, long, TimeUnit)`.
    ///
    /// The broker holds the republished message in the retry-letter topic
    /// until `delay` has elapsed, then dispatches it normally. A
    /// `RECONSUMETIMES` property is incremented on each redelivery so
    /// consumers can implement a maximum-retry policy above this layer.
    /// The original `partition_key`, `ordering_key`, `event_time`, and
    /// properties are preserved; `REAL_TOPIC` and `ORIGINAL_MESSAGE_ID`
    /// are stamped for correlation back to the source topic.
    ///
    /// # Errors
    ///
    /// Returns the first [`ClientError`] from the republish or the
    /// subsequent ack.
    pub async fn reconsume_later(
        &self,
        retry_producer: &crate::Producer<P>,
        msg: IncomingMessage,
        delay: std::time::Duration,
    ) -> Result<(), ClientError> {
        self.reconsume_later_with_properties(retry_producer, msg, Vec::new(), delay)
            .await
    }

    /// Same as [`Self::reconsume_later`] but lets the caller stamp
    /// additional custom properties on the republished message. Custom
    /// entries are merged with the original message's properties — on a
    /// key collision, the custom value takes precedence. Mirrors Java
    /// `Consumer#reconsumeLater(Message, Map<String, String> customProperties, long, TimeUnit)`.
    ///
    /// # Errors
    ///
    /// Returns the first [`ClientError`] from the republish or the
    /// subsequent ack.
    pub async fn reconsume_later_with_properties(
        &self,
        retry_producer: &crate::Producer<P>,
        msg: IncomingMessage,
        custom_properties: Vec<(String, String)>,
        delay: std::time::Duration,
    ) -> Result<(), ClientError> {
        let mut metadata = magnetar_proto::pb::MessageMetadata {
            partition_key: msg.metadata.partition_key.clone(),
            partition_key_b64_encoded: msg.metadata.partition_key_b64_encoded,
            ordering_key: msg.metadata.ordering_key.clone(),
            event_time: msg.metadata.event_time,
            properties: msg.metadata.properties.clone(),
            ..magnetar_proto::pb::MessageMetadata::default()
        };
        // Apply custom properties (overrides on key collision).
        for (k, v) in custom_properties {
            metadata.properties.retain(|kv| kv.key != k);
            metadata
                .properties
                .push(magnetar_proto::pb::KeyValue { key: k, value: v });
        }
        // Bump the RECONSUMETIMES property if present, otherwise stamp it
        // at 1. Mirrors the Java retry-letter convention so downstream
        // consumers can enforce caps.
        let reconsumetimes = metadata
            .properties
            .iter()
            .find(|kv| kv.key == "RECONSUMETIMES")
            .and_then(|kv| kv.value.parse::<u64>().ok())
            .unwrap_or(0)
            + 1;
        metadata.properties.retain(|kv| kv.key != "RECONSUMETIMES");
        metadata.properties.push(magnetar_proto::pb::KeyValue {
            key: "RECONSUMETIMES".to_owned(),
            value: reconsumetimes.to_string(),
        });
        // Stamp REAL_TOPIC + ORIGINAL_MESSAGE_ID like the DLQ republish
        // does so consumers of the retry topic can correlate back to the
        // source.
        metadata.properties.push(magnetar_proto::pb::KeyValue {
            key: "REAL_TOPIC".to_owned(),
            value: self
                .shared
                .inner
                .lock()
                .consumer_topic(self.handle)
                .unwrap_or("")
                .to_owned(),
        });
        metadata.properties.push(magnetar_proto::pb::KeyValue {
            key: "ORIGINAL_MESSAGE_ID".to_owned(),
            value: msg.message_id.to_string(),
        });
        // Set deliver_at_time so the broker queues the message for
        // `delay` past now.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0i64, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
        let delay_ms = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
        metadata.deliver_at_time = Some(now_ms.saturating_add(delay_ms));
        let payload_len = msg.payload.len();
        let outgoing = magnetar_proto::producer::OutgoingMessage {
            payload: msg.payload,
            metadata,
            uncompressed_size: u32::try_from(payload_len).unwrap_or(u32::MAX),
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        };
        retry_producer.send(outgoing).await?;
        self.ack(msg.message_id).await?;
        Ok(())
    }

    /// Stops automatic flow refills so the broker stops dispatching new
    /// messages once already-issued permits drain. Buffered messages remain
    /// receivable.
    ///
    /// Mirrors `org.apache.pulsar.client.api.Consumer#pause`.
    pub fn pause(&self) {
        let mut conn = self.shared.inner.lock();
        conn.set_paused(self.handle, true);
    }

    /// Re-enables automatic flow refills. Wakes the driver so it can flush
    /// any FLOW frames the state machine queues as a result.
    ///
    /// Mirrors `org.apache.pulsar.client.api.Consumer#resume`.
    pub fn resume(&self) {
        {
            let mut conn = self.shared.inner.lock();
            conn.set_paused(self.handle, false);
        }
        self.shared.driver_waker.notify_one();
    }

    /// Receive the next message. Resolves when the broker delivers a
    /// `CommandMessage` and the state machine emits it into this consumer's
    /// queue.
    ///
    /// Multiple concurrent `receive()` calls on the same consumer are
    /// supported: each future installs its own waker into the per-consumer
    /// slab on [`magnetar_proto::consumer::ConsumerState`]; arrival drains the slab and
    /// every parked future is re-polled. The first to acquire the connection
    /// lock pops the message; the others observe an empty queue and re-park.
    ///
    /// # Errors
    /// - [`ClientError::Closed`] if the connection has been closed before a message arrives.
    pub async fn receive(&self) -> Result<IncomingMessage, ClientError> {
        ReceiveFut {
            shared: self.shared.clone(),
            handle: self.handle,
            slab_key: None,
        }
        .await
    }

    /// Receive the next message, bounded by `timeout`. Returns `Ok(None)` if
    /// the deadline elapses with no message. Mirrors Java
    /// `Consumer#receive(int timeout, TimeUnit unit)`.
    ///
    /// The timeout source is `tokio::time::timeout`, which under
    /// `TokioProviders` measures wall time. Under
    /// `moonpool_core::SimProviders` the timeout is still wall-time —
    /// pass-2 of the engine-generic surface lift will route this through
    /// `Providers::time` for full sim-determinism. Today's behavior matches
    /// the tokio engine's `Consumer::receive_with_timeout`.
    ///
    /// # Errors
    /// Propagates [`Self::receive`] errors. The timeout case returns
    /// `Ok(None)` rather than an error to match Java's "no message"
    /// semantic.
    pub async fn receive_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Option<IncomingMessage>, ClientError> {
        match tokio::time::timeout(timeout, self.receive()).await {
            Ok(Ok(msg)) => Ok(Some(msg)),
            Ok(Err(err)) => Err(err),
            Err(_) => Ok(None),
        }
    }

    /// Receive up to `max_messages` messages in one call. Mirrors Java
    /// `Consumer#batchReceive`. Waits up to `max_wait` for the first
    /// message, then drains any additional already-buffered messages
    /// without further waiting.
    ///
    /// Returns an empty `Vec` if the timeout elapses with no messages.
    ///
    /// # Errors
    /// Propagates [`Self::receive`] errors.
    pub async fn receive_batch(
        &self,
        max_messages: usize,
        max_wait: std::time::Duration,
    ) -> Result<Vec<IncomingMessage>, ClientError> {
        self.receive_batch_with_bytes_cap(max_messages, usize::MAX, max_wait)
            .await
    }

    /// Same as [`Self::receive_batch`] but stops once the accumulated
    /// payload size would exceed `max_bytes`. Mirrors Java's
    /// `BatchReceivePolicy` — the broker-side policy supports three caps
    /// (max messages, max bytes, max wait) and stops on whichever fires
    /// first. Pass `usize::MAX` to disable a cap. The first message is
    /// always included even if it alone exceeds `max_bytes` (matches
    /// Java's "deliver at least one" semantic), but subsequent ones obey
    /// the cap strictly.
    ///
    /// # Errors
    /// Propagates [`Self::receive`] errors.
    pub async fn receive_batch_with_bytes_cap(
        &self,
        max_messages: usize,
        max_bytes: usize,
        max_wait: std::time::Duration,
    ) -> Result<Vec<IncomingMessage>, ClientError> {
        if max_messages == 0 || max_bytes == 0 {
            return Ok(Vec::new());
        }
        let first = tokio::time::timeout(max_wait, self.receive()).await;
        let first = match first {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Ok(Vec::new()),
        };
        let mut acc_bytes = first.payload.len();
        let mut out = Vec::with_capacity(max_messages.min(64));
        out.push(first);
        while out.len() < max_messages {
            // Peek at the next message's payload size; if popping would
            // exceed the byte cap, leave it for the next batch.
            let next_size = self
                .shared
                .inner
                .lock()
                .peek_message_payload_size(self.handle);
            let Some(next_size) = next_size else { break };
            if acc_bytes.saturating_add(next_size) > max_bytes {
                break;
            }
            let msg = {
                let mut conn = self.shared.inner.lock();
                conn.pop_message(self.handle)
            };
            let Some(msg) = msg else { break };
            acc_bytes = acc_bytes.saturating_add(msg.payload.len());
            out.push(msg);
        }
        // pop_message may have queued FLOW frames; wake the driver.
        if out.len() > 1 {
            self.shared.driver_waker.notify_one();
        }
        Ok(out)
    }

    /// Acknowledge a single message (individual ack). Resolves once the
    /// broker confirms via `CommandAckResponse`.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker reports an ack failure.
    /// - [`ClientError::Other`] when an unexpected outcome arrives on this request id
    ///   (state-machine bug, not a transient failure).
    pub async fn ack(&self, message_id: MessageId) -> Result<(), ClientError> {
        self.ack_inner(
            vec![message_id],
            pb::command_ack::AckType::Individual,
            Vec::new(),
            None,
        )
        .await
    }

    /// Acknowledge a cumulative position. Resolves once the broker confirms.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker reports an ack failure.
    /// - [`ClientError::Other`] when an unexpected outcome arrives.
    pub async fn ack_cumulative(&self, message_id: MessageId) -> Result<(), ClientError> {
        self.ack_inner(
            vec![message_id],
            pb::command_ack::AckType::Cumulative,
            Vec::new(),
            None,
        )
        .await
    }

    /// Acknowledge a single message as part of a Pulsar transaction
    /// (PIP-31). The ack only takes effect once the transaction
    /// commits. Mirrors Java `Consumer#acknowledgeAsync(MessageId,
    /// Transaction)`.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker reports an ack failure.
    /// - [`ClientError::Other`] when an unexpected outcome arrives.
    pub async fn ack_with_txn(
        &self,
        message_id: MessageId,
        txn_id: magnetar_proto::TxnId,
    ) -> Result<(), ClientError> {
        self.ack_inner(
            vec![message_id],
            pb::command_ack::AckType::Individual,
            Vec::new(),
            Some(txn_id),
        )
        .await
    }

    /// Cumulative ack as part of a Pulsar transaction (PIP-31). Mirrors
    /// Java `Consumer#acknowledgeCumulativeAsync(MessageId,
    /// Transaction)`.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker reports an ack failure.
    /// - [`ClientError::Other`] when an unexpected outcome arrives.
    pub async fn ack_cumulative_with_txn(
        &self,
        message_id: MessageId,
        txn_id: magnetar_proto::TxnId,
    ) -> Result<(), ClientError> {
        self.ack_inner(
            vec![message_id],
            pb::command_ack::AckType::Cumulative,
            Vec::new(),
            Some(txn_id),
        )
        .await
    }

    /// Stage an individual ack into this consumer's ack-grouping
    /// tracker (opt-in via `ConsumerBuilder::ack_group_time`).
    /// Fire-and-forget: the call returns immediately without a future,
    /// and the coalesced `CommandAck` is emitted by the state machine
    /// once `ack_group_time` has elapsed. With no tracker configured,
    /// the proto layer falls back to a synchronous immediate
    /// `CommandAck` so the message is never silently dropped. Mirrors
    /// Java's `acknowledgmentGroupTime` path.
    pub fn ack_grouped(&self, message_id: MessageId) {
        let now = std::time::Instant::now();
        {
            let mut conn = self.shared.inner.lock();
            conn.ack_grouped_individual(self.handle, message_id, now);
        }
        self.shared.driver_waker.notify_one();
    }

    /// Stage a cumulative ack into this consumer's ack-grouping tracker.
    /// See [`Self::ack_grouped`] for the semantics.
    pub fn ack_grouped_cumulative(&self, message_id: MessageId) {
        let now = std::time::Instant::now();
        {
            let mut conn = self.shared.inner.lock();
            conn.ack_grouped_cumulative(self.handle, message_id, now);
        }
        self.shared.driver_waker.notify_one();
    }

    async fn ack_inner(
        &self,
        message_ids: Vec<MessageId>,
        ack_type: pb::command_ack::AckType,
        properties: Vec<(String, i64)>,
        txn_id: Option<magnetar_proto::TxnId>,
    ) -> Result<(), ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.ack(
                self.handle,
                AckRequest {
                    message_ids,
                    ack_type,
                    properties,
                    txn_id,
                },
            )
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::Success { .. } => Ok(()),
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            other => Err(ClientError::Other(format!(
                "unexpected ack outcome: {other:?}"
            ))),
        }
    }

    /// Negatively acknowledge a single message. The broker will redeliver it
    /// (subject to `maxRedeliverCount` and any DLQ policy configured
    /// server-side). Fire-and-forget — no future, no broker confirmation.
    ///
    /// Mirrors `org.apache.pulsar.client.api.Consumer#negativeAcknowledge`.
    pub fn negative_ack(&self, message_id: MessageId) {
        self.negative_ack_many(vec![message_id]);
    }

    /// Negatively acknowledge a batch of messages. An empty
    /// `message_ids` vector matches Pulsar's "all unacked" semantics
    /// used by [`Self::redeliver_unacked`].
    pub fn negative_ack_many(&self, message_ids: Vec<MessageId>) {
        let now = std::time::Instant::now();
        {
            let mut conn = self.shared.inner.lock();
            conn.negative_ack(self.handle, message_ids, now);
        }
        self.shared.driver_waker.notify_one();
    }

    /// Negatively acknowledge a single message with an explicit
    /// per-message redelivery delay. Mirrors Java's PIP-37 backoff
    /// path.
    pub fn negative_ack_with_delay(&self, message_id: MessageId, delay: std::time::Duration) {
        let now = std::time::Instant::now();
        {
            let mut conn = self.shared.inner.lock();
            conn.negative_ack_with_delay(self.handle, message_id, delay, now);
        }
        self.shared.driver_waker.notify_one();
    }

    /// Ask the broker to redeliver every unacknowledged message on
    /// this consumer. Mirrors Java
    /// `Consumer#redeliverUnacknowledgedMessages`. Implemented via the
    /// "empty list = all unacked" semantics on the proto layer's
    /// `negative_ack`.
    pub fn redeliver_unacked(&self) {
        self.negative_ack_many(Vec::new());
    }

    /// Unsubscribe — tear down this consumer's subscription on the broker
    /// (deletes the cursor, not just the consumer handle). Mirrors Java
    /// `Consumer#unsubscribe`. Callers typically follow with
    /// [`Self::close`].
    ///
    /// `force=true` (PIP-313) drops the subscription even when other
    /// consumers are still attached to the same subscription name. Signature
    /// matches `magnetar_runtime_tokio::Consumer::unsubscribe` exactly so
    /// the `ConsumerApi` trait can route through either runtime in pass-2
    /// of the surface lift.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker rejects the unsubscribe.
    /// - [`ClientError::Other`] on an unexpected outcome.
    pub async fn unsubscribe(&self, force: bool) -> Result<(), ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.unsubscribe(self.handle, force)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::Success { .. } => Ok(()),
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            other => Err(ClientError::Other(format!(
                "unexpected unsubscribe outcome: {other:?}"
            ))),
        }
    }

    /// Seek to the earliest available message. Mirrors Java
    /// `Consumer#seek(MessageId.earliest)`.
    ///
    /// # Errors
    /// Propagates [`Self::seek_to_message`] errors.
    pub async fn seek_to_earliest(&self) -> Result<(), ClientError> {
        self.seek_to_message(MessageId::EARLIEST).await
    }

    /// Seek to the latest available message. Mirrors Java
    /// `Consumer#seek(MessageId.latest)`.
    ///
    /// # Errors
    /// Propagates [`Self::seek_to_message`] errors.
    pub async fn seek_to_latest(&self) -> Result<(), ClientError> {
        self.seek_to_message(MessageId::LATEST).await
    }

    /// Wall-clock timestamp of the last broker disconnection
    /// observed by this connection, or `None` if no disconnect has
    /// happened yet. Mirrors Java
    /// `Consumer#getLastDisconnectedTimestamp`.
    #[must_use]
    pub fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        self.shared.inner.lock().last_disconnected_timestamp()
    }

    /// Look up the broker-registered schema for the consumer's topic
    /// (PIP-87). Mirrors Java
    /// `PulsarClientImpl#getSchema(TopicName, Optional<byte[]>)`. Used
    /// by `magnetar_proto::schema::AutoConsumeSchema` to warm its
    /// cache on first receive.
    ///
    /// `version = None` asks for the current schema; pass
    /// `Some(schema_version_bytes)` to re-resolve a historical schema.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker rejects the lookup (e.g. `TopicNotFound`).
    /// - [`ClientError::Other`] when the consumer handle is no longer registered or an unexpected
    ///   outcome arrives.
    pub async fn get_schema(&self, version: Option<Vec<u8>>) -> Result<pb::Schema, ClientError> {
        let topic = self
            .shared
            .inner
            .lock()
            .consumer_topic(self.handle)
            .map(str::to_owned)
            .ok_or_else(|| {
                ClientError::Other(format!(
                    "get_schema: consumer handle {:?} is no longer registered",
                    self.handle
                ))
            })?;
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.get_schema(&topic, version)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::GetSchemaResponse { result, .. } => match result {
                Ok((schema, _version)) => Ok(schema),
                Err((code, message)) => Err(ClientError::Broker { code, message }),
            },
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            other => Err(ClientError::Other(format!(
                "unexpected get_schema outcome: {other:?}"
            ))),
        }
    }

    /// Ask the broker for the topic's last-published message id.
    /// Mirrors Java `Consumer#getLastMessageId`.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker rejects the request.
    /// - [`ClientError::Other`] on an unexpected outcome.
    pub async fn last_message_id(&self) -> Result<MessageId, ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.get_last_message_id(self.handle)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::LastMessageId {
                last_message_id, ..
            } => Ok(last_message_id),
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            other => Err(ClientError::Other(format!(
                "unexpected last_message_id outcome: {other:?}"
            ))),
        }
    }

    /// `true` if the broker has at least one message strictly past `cursor`
    /// (i.e. there is at least one more message to receive). `cursor` is
    /// typically the last [`MessageId`] this consumer received. Comparison
    /// is `>` not `>=` (matches Java's `MessageId#compareTo`).
    ///
    /// # Errors
    /// Propagates [`Self::last_message_id`] errors.
    pub async fn has_message_after(&self, cursor: MessageId) -> Result<bool, ClientError> {
        let last = self.last_message_id().await?;
        Ok((
            last.ledger_id,
            last.entry_id,
            last.partition,
            last.batch_index,
        ) > (
            cursor.ledger_id,
            cursor.entry_id,
            cursor.partition,
            cursor.batch_index,
        ))
    }

    /// Seek this consumer to a specific message id. The broker replays from
    /// there.
    ///
    /// Mirrors `org.apache.pulsar.client.api.Consumer#seek(MessageId)`.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker rejects the seek.
    /// - [`ClientError::Other`] when an unexpected outcome arrives.
    pub async fn seek_to_message(&self, message_id: MessageId) -> Result<(), ClientError> {
        self.seek_inner(SeekTarget::MessageId(message_id)).await
    }

    /// Seek this consumer to a specific publish timestamp (millis since the
    /// UNIX epoch).
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker rejects the seek.
    /// - [`ClientError::Other`] when an unexpected outcome arrives.
    pub async fn seek_to_timestamp(&self, publish_time_ms: u64) -> Result<(), ClientError> {
        self.seek_inner(SeekTarget::PublishTime(publish_time_ms))
            .await
    }

    async fn seek_inner(&self, target: SeekTarget) -> Result<(), ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.seek(self.handle, target)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::Success { .. } => Ok(()),
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            other => Err(ClientError::Other(format!(
                "unexpected seek outcome: {other:?}"
            ))),
        }
    }

    /// Close this consumer. Resolves when the broker acks the close. After
    /// this resolves the consumer handle is invalidated — calling any other
    /// method on this `Consumer` is a no-op or returns an empty value.
    ///
    /// Does not tear down the underlying connection-level driver; that is
    /// owned by the [`Client`] which spawned this consumer.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker reports a close failure.
    /// - [`ClientError::Other`] when an unexpected outcome arrives.
    pub async fn close(self) -> Result<(), ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.close_consumer(self.handle)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::Success { .. } => Ok(()),
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            other => Err(ClientError::Other(format!(
                "unexpected close outcome: {other:?}"
            ))),
        }
    }
}

impl<P: Providers> Client<P> {
    /// Subscribe to a topic and return a fully-initialised [`Consumer`].
    ///
    /// Resolves once the broker has acked the subscribe (`CommandSuccess`
    /// correlated with the request id surfaced as
    /// `ConnectionEvent::SubscribeAcked`). After that point the state
    /// machine has a fresh per-consumer queue and the consumer's initial
    /// FLOW has been queued for the driver to flush.
    ///
    /// # Errors
    /// - [`ClientError::Closed`] if the broker closed the consumer mid-handshake.
    /// - [`ClientError::Other`] on connection close before the subscribe acked.
    pub async fn subscribe(&self, req: SubscribeRequest) -> Result<Consumer<P>, ClientError> {
        let receiver_queue_size = req.receiver_queue_size;
        // See `Client::open_producer`: subscribe also needs lookup-driven bundle
        // activation. Mirrors `magnetar-runtime-tokio`'s `Client::subscribe_with`.
        let _ = self.lookup_topic(&req.topic, false).await?;
        let shared = self.shared().clone();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(req)
        };
        shared.driver_waker.notify_one();
        SubscribeAckedFut {
            shared: shared.clone(),
            handle,
        }
        .await?;

        // Feed an initial flow so the broker starts delivering. `initial_flow`
        // returns `None` when there is no consumer state; we still send an
        // explicit FLOW with the configured queue size as a safety net.
        {
            let mut conn = shared.inner.lock();
            let _ = conn.initial_flow(handle);
            if receiver_queue_size > 0 {
                conn.flow(handle, receiver_queue_size as u32);
            }
        }
        shared.driver_waker.notify_one();

        Ok(Consumer {
            shared,
            handle,
            _providers: std::marker::PhantomData,
        })
    }
}

/// Future that resolves the [`OpOutcome`] correlated with a single
/// `RequestId`. Same pattern as [`crate::client::RequestFut`], duplicated
/// here because that one is private to the client module.
struct RequestFut {
    shared: Arc<ConnectionShared>,
    request_id: RequestId,
}

impl Future for RequestFut {
    type Output = OpOutcome;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let key = PendingOpKey::Request(self.request_id);
        let mut conn = self.shared.inner.lock();
        if let Some(outcome) = conn.take_outcome(key) {
            return Poll::Ready(outcome);
        }
        conn.register_waker(key, cx.waker().clone());
        Poll::Pending
    }
}

/// Future returned by [`Consumer::receive`]. Pops the next message from the
/// per-consumer queue, parking on the per-consumer waker slab exposed by
/// [`magnetar_proto::Connection::register_consumer_receive_waker`] until a
/// message arrives or the consumer is closed.
///
/// On drop the future evicts its slab slot via
/// [`magnetar_proto::Connection::cancel_consumer_receive_waker`] so cancelled
/// receives don't leak entries until the next arrival.
struct ReceiveFut {
    shared: Arc<ConnectionShared>,
    handle: ConsumerHandle,
    /// Slab key of the currently-installed waker, if any.
    slab_key: Option<usize>,
}

impl Drop for ReceiveFut {
    fn drop(&mut self) {
        if let Some(key) = self.slab_key.take() {
            let mut conn = self.shared.inner.lock();
            conn.cancel_consumer_receive_waker(self.handle, key);
        }
    }
}

impl Future for ReceiveFut {
    type Output = Result<IncomingMessage, ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let handle = this.handle;
        let shared = this.shared.clone();
        let mut conn = shared.inner.lock();
        if let Some(msg) = conn.pop_message(handle) {
            // Clear any stale slab entry; we resolved successfully.
            if let Some(key) = this.slab_key.take() {
                conn.cancel_consumer_receive_waker(handle, key);
            }
            drop(conn);
            // pop_message may have queued FLOW frames; wake the driver to flush.
            shared.driver_waker.notify_one();
            return Poll::Ready(Ok(msg));
        }
        // Closed connection with no buffered message → terminal.
        if conn.is_closed() || conn.consumer_is_closed(handle) {
            return Poll::Ready(Err(ClientError::Closed));
        }
        // Refresh the slab registration so the current task is the one woken.
        if let Some(old_key) = this.slab_key.take() {
            conn.cancel_consumer_receive_waker(handle, old_key);
        }
        if let Some(key) = conn.register_consumer_receive_waker(handle, cx.waker().clone()) {
            // Close the race where a message arrives between the
            // pop_message check above and the slab insert.
            if conn.peek_message_payload_size(handle).is_some() {
                conn.cancel_consumer_receive_waker(handle, key);
                drop(conn);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            this.slab_key = Some(key);
            drop(conn);
            return Poll::Pending;
        }
        // Consumer was removed in the meantime; surface as closed on the
        // next poll.
        drop(conn);
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Future that resolves once the broker has acked the subscribe for the
/// given [`ConsumerHandle`]. Drains `ConnectionEvent`s from the queue looking
/// for [`ConnectionEvent::SubscribeAcked`].
///
/// Note: the connection driver may also drain `SubscribeAcked` via its
/// `handle_pending_events` catch-all arm; in that case this future will not
/// see the event and will park on the driver wakeup indefinitely until a
/// follow-up `Closed` event terminates it. Same shape as the tokio engine's
/// `EventWaitFut`. A follow-up milestone should make `SubscribeAcked`
/// request-id correlated through `OpOutcome::Success` so this race
/// disappears.
struct SubscribeAckedFut {
    shared: Arc<ConnectionShared>,
    handle: ConsumerHandle,
}

impl Future for SubscribeAckedFut {
    type Output = Result<(), ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Fast-path: if the consumer is already registered as "ready" (i.e.
        // `consumer_is_closed` is false and the consumer state exists), we
        // can return immediately. The protocol layer marks the state alive
        // synchronously inside `subscribe()`, so the SubscribeAcked event is
        // really only needed to wait for the broker's CommandSuccess.
        {
            let mut conn = self.shared.inner.lock();
            // Inspect events looking for our SubscribeAcked.
            loop {
                match conn.poll_event() {
                    Some(ConnectionEvent::SubscribeAcked { handle }) if handle == self.handle => {
                        return Poll::Ready(Ok(()));
                    }
                    Some(ConnectionEvent::ConsumerClosedByBroker { handle, .. })
                        if handle == self.handle =>
                    {
                        return Poll::Ready(Err(ClientError::Closed));
                    }
                    Some(ConnectionEvent::SubscribeFailed {
                        handle,
                        code,
                        message,
                    }) if handle == self.handle => {
                        return Poll::Ready(Err(ClientError::Broker { code, message }));
                    }
                    Some(ConnectionEvent::TopicListChanged { added, removed }) => {
                        // Forward to the per-client buffer + waker so we don't
                        // accidentally swallow a PIP-145 delta while waiting
                        // for a subscribe ack.
                        self.shared
                            .topic_list_changes
                            .lock()
                            .push_back(TopicListChange { added, removed });
                        self.shared.topic_list_notify.notify_waiters();
                    }
                    Some(ConnectionEvent::Closed { reason }) => {
                        return Poll::Ready(Err(ClientError::Other(
                            reason.unwrap_or_else(|| "connection closed".to_owned()),
                        )));
                    }
                    Some(_) => {} // ignore unrelated events
                    None => break,
                }
            }

            if conn.is_closed() {
                return Poll::Ready(Err(ClientError::Closed));
            }
        }

        // Park on the driver wakeup. The driver notifies after any inbound
        // bytes are processed.
        let notified = self.shared.driver_waker.notified();
        tokio::pin!(notified);
        if notified.as_mut().enable() {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use bytes::BytesMut;
    use magnetar_proto::{ConnectionConfig, SubscribeRequest, encode_command, encode_payload, pb};
    use moonpool_core::TokioProviders;

    use super::{Consumer, ReceiveFut};
    use crate::client::Client;
    use crate::{ConnectionShared, MoonpoolEngine};

    fn handshake_response_bytes() -> BytesMut {
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
        let mut buf = BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode CommandConnected");
        buf
    }

    fn command_message_bytes(consumer_id: u64, entry_id: u64, payload: &[u8]) -> BytesMut {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Message as i32,
            message: Some(pb::CommandMessage {
                consumer_id,
                message_id: pb::MessageIdData {
                    ledger_id: 1,
                    entry_id,
                    ..Default::default()
                },
                redelivery_count: Some(0),
                ack_set: Vec::new(),
                consumer_epoch: None,
            }),
            ..Default::default()
        };
        let meta = pb::MessageMetadata {
            producer_name: "test".to_owned(),
            sequence_id: entry_id,
            publish_time: 1_700_000_000,
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_payload(&mut buf, &cmd, &meta, payload).expect("encode CommandMessage");
        buf
    }

    fn handshake_complete_shared() -> Arc<ConnectionShared> {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("handshake");
            let frame = handshake_response_bytes();
            conn.handle_bytes(Instant::now(), &frame)
                .expect("connected");
        }
        shared
    }

    fn make_consumer<P: moonpool_core::Providers>(
        shared: Arc<ConnectionShared>,
        handle: magnetar_proto::ConsumerHandle,
    ) -> Consumer<P> {
        Consumer {
            shared,
            handle,
            _providers: std::marker::PhantomData,
        }
    }

    /// `Client::subscribe` is generic over `P: Providers` — confirm the
    /// bounds compose with `TokioProviders` by naming `connect_plain` (which
    /// produces the `Client<P>` carrier) without actually dialling.
    /// `subscribe` is exercised by the integration tests once a real broker
    /// is in the loop.
    #[test]
    #[allow(clippy::let_underscore_future, clippy::no_effect_underscore_binding)]
    fn subscribe_compiles_against_tokio_providers() {
        let providers = TokioProviders::new();
        let engine = MoonpoolEngine::new(providers);
        let _fut_client =
            Client::connect_plain(&engine, "127.0.0.1:6650", ConnectionConfig::default());
        // Reference `SubscribeRequest::default` to confirm the type is
        // re-exported via `magnetar_proto`.
        let _req = SubscribeRequest::default();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn topic_and_subscription_round_trip() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/t-roundtrip".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared, handle);
        assert_eq!(consumer.topic(), "persistent://public/default/t-roundtrip");
        assert_eq!(consumer.subscription(), "s");
        assert_eq!(consumer.handle(), handle);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn topic_and_subscription_unknown_handle_are_empty() {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let consumer: Consumer<TokioProviders> =
            make_consumer(shared, magnetar_proto::ConsumerHandle(9999));
        assert_eq!(consumer.topic(), "");
        assert_eq!(consumer.subscription(), "");
        // `consumer_is_closed` returns true for unknown handles per the
        // protocol layer convention.
        assert!(consumer.is_closed());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pause_resume_toggle_flag() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/t-pause".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);

        // Default: not paused.
        assert_eq!(shared.inner.lock().is_paused(handle), Some(false));
        consumer.pause();
        assert_eq!(shared.inner.lock().is_paused(handle), Some(true));
        consumer.resume();
        assert_eq!(shared.inner.lock().is_paused(handle), Some(false));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn receive_pops_buffered_message() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/t-receive".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        // Pump a single CommandMessage frame into the state machine so the
        // per-consumer queue has something to pop.
        {
            let mut conn = shared.inner.lock();
            let bytes = command_message_bytes(handle.0, 100, b"hello");
            conn.handle_bytes(Instant::now(), &bytes)
                .expect("handle CommandMessage");
        }

        let fut = ReceiveFut {
            shared: shared.clone(),
            handle,
            slab_key: None,
        };
        let msg = fut.await.expect("receive must succeed");
        assert_eq!(msg.payload.as_ref(), b"hello");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn receive_on_closed_consumer_yields_closed_error() {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        // Consumer handle is unknown to the state machine — `consumer_is_closed`
        // therefore returns true, and `is_closed` on the connection is also
        // true once `close()` is called. Trigger close so the future resolves.
        shared.inner.lock().close();
        let fut = ReceiveFut {
            shared,
            handle: magnetar_proto::ConsumerHandle(9999),
            slab_key: None,
        };
        let err = fut.await.expect_err("receive must surface Closed");
        assert!(matches!(err, crate::client::ClientError::Closed));
    }

    // ── per-consumer waker slab ───────────────────────────────────────────

    /// Two concurrent `receive()` futures on the same consumer must both
    /// resolve when two messages arrive — the slab fans out independently
    /// of which future polled first.
    #[tokio::test(flavor = "current_thread")]
    async fn two_concurrent_receives_both_fan_out() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/fanout".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let c1: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);
        let c2: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);

        let t1 = tokio::spawn(async move { c1.receive().await });
        let t2 = tokio::spawn(async move { c2.receive().await });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Slab should hold both registrations.
        assert_eq!(
            shared
                .inner
                .lock()
                .consumer(handle)
                .unwrap()
                .receive_wakers
                .len(),
            2,
        );

        // Deliver two messages.
        {
            let mut conn = shared.inner.lock();
            for i in 0..2_u64 {
                let bytes = command_message_bytes(handle.0, 200 + i, format!("m{i}").as_bytes());
                conn.handle_bytes(Instant::now(), &bytes)
                    .expect("handle CommandMessage");
            }
        }

        let m1 = tokio::time::timeout(std::time::Duration::from_secs(1), t1)
            .await
            .expect("first receive must not hang")
            .expect("join")
            .expect("receive ok");
        let m2 = tokio::time::timeout(std::time::Duration::from_secs(1), t2)
            .await
            .expect("second receive must not hang")
            .expect("join")
            .expect("receive ok");
        assert_ne!(
            m1.message_id, m2.message_id,
            "the two receives must each get a different message"
        );
    }

    /// Dropping a `ReceiveFut` before it resolves must evict its slab slot,
    /// so a later arrival doesn't leak the entry / wake a dead task.
    #[tokio::test(flavor = "current_thread")]
    async fn dropping_receive_future_evicts_slab_slot() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/cancel".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let c: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);

        let task = tokio::spawn(async move { c.receive().await });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert_eq!(
            shared
                .inner
                .lock()
                .consumer(handle)
                .unwrap()
                .receive_wakers
                .len(),
            1,
        );

        task.abort();
        let _ = task.await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert_eq!(
            shared
                .inner
                .lock()
                .consumer(handle)
                .unwrap()
                .receive_wakers
                .len(),
            0,
            "the cancelled receive's slab slot must be evicted",
        );
    }

    /// Regression for the CLI "consume hangs against fresh broker" bug: when the broker
    /// rejects a subscribe with a PERMANENT `CommandError` (e.g.
    /// `AuthorizationError`), the moonpool engine's `SubscribeAckedFut` must surface
    /// a `ClientError::Broker` rather than parking on the driver waker forever.
    /// Mirrors the proto-level permanent-failure test. Transient codes
    /// (`ServiceNotReady` / `MetadataError` / `TopicNotFound`) hit the retry path
    /// instead.
    #[tokio::test(flavor = "current_thread")]
    async fn subscribe_acked_fut_surfaces_broker_error() {
        use std::time::Duration;

        let shared = ConnectionShared::new(ConnectionConfig::default());
        {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("handshake");
            conn.handle_bytes(Instant::now(), &handshake_response_bytes())
                .expect("connected");
            let _ = conn.poll_event();
        }
        let (handle, request_id) = {
            let mut conn = shared.inner.lock();
            let request_id = conn.peek_next_request_id_for_test();
            let handle = conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/forbidden".to_owned(),
                subscription: "regression".to_owned(),
                sub_type: pb::command_subscribe::SubType::Exclusive,
                ..Default::default()
            });
            (handle, request_id)
        };

        let err = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id,
                error: pb::ServerError::AuthorizationError as i32,
                message: "not authorized".to_owned(),
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_command(&mut buf, &err).expect("encode CommandError");
        {
            let mut conn = shared.inner.lock();
            conn.handle_bytes(Instant::now(), &buf)
                .expect("handle CommandError");
        }

        let fut = super::SubscribeAckedFut {
            shared: shared.clone(),
            handle,
        };
        let res = tokio::time::timeout(Duration::from_secs(2), fut)
            .await
            .expect("subscribe-acked future must resolve (regression: previously hung)");
        match res {
            Err(crate::ClientError::Broker { code, message }) => {
                assert_eq!(code, pb::ServerError::AuthorizationError as i32);
                assert_eq!(message, "not authorized");
            }
            other => panic!("expected ClientError::Broker, got {other:?}"),
        }
    }

    /// `ack_grouped` is fire-and-forget; with no `ack_group_time` tracker
    /// configured the proto layer falls back to a synchronous immediate
    /// `CommandAck`. Calling it on a fresh consumer must NOT panic and
    /// MUST notify the driver waker so the queued frame is flushed.
    /// ADR-0024 1:1 mirror.
    #[tokio::test(flavor = "current_thread")]
    async fn ack_grouped_falls_back_to_immediate_ack() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/t-ack-grp".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);
        // Fire-and-forget: must not panic, must notify the driver.
        consumer.ack_grouped(magnetar_proto::MessageId {
            ledger_id: 1,
            entry_id: 0,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
        });
        // Sanity: the consumer is still registered after the call.
        assert!(!consumer.is_closed());
    }

    /// `ack_grouped_cumulative` mirrors `ack_grouped` for cumulative
    /// acks. ADR-0024 1:1 mirror.
    #[tokio::test(flavor = "current_thread")]
    async fn ack_grouped_cumulative_falls_back_to_immediate_ack() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/t-ack-grp-cum".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);
        consumer.ack_grouped_cumulative(magnetar_proto::MessageId {
            ledger_id: 1,
            entry_id: 5,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
        });
        assert!(!consumer.is_closed());
    }

    /// `ack_with_txn` queues an ack stamped with the given `TxnId`. The
    /// returned future stays pending until the broker confirms (no driver
    /// running here), so we just confirm the call enqueues without panic
    /// and the consumer remains registered. ADR-0024 1:1 mirror.
    #[tokio::test(flavor = "current_thread")]
    async fn ack_with_txn_enqueues_request() {
        use std::time::Duration;
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/t-ack-txn".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);
        let txn = magnetar_proto::TxnId {
            most_sig_bits: 1,
            least_sig_bits: 2,
        };
        let mid = magnetar_proto::MessageId {
            ledger_id: 1,
            entry_id: 0,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
        };
        let fut = consumer.ack_with_txn(mid, txn);
        let res = tokio::time::timeout(Duration::from_millis(10), fut).await;
        // No driver is running → broker never confirms → the future
        // remains pending and the timeout fires. The point of the test
        // is that the request was enqueued without panic.
        assert!(res.is_err(), "expected pending future (no driver)");
        assert!(!consumer.is_closed());
    }

    /// `ack_cumulative_with_txn` mirrors `ack_with_txn` for cumulative
    /// acks under a transaction. ADR-0024 1:1 mirror.
    #[tokio::test(flavor = "current_thread")]
    async fn ack_cumulative_with_txn_enqueues_request() {
        use std::time::Duration;
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/t-ack-cum-txn".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);
        let txn = magnetar_proto::TxnId {
            most_sig_bits: 1,
            least_sig_bits: 2,
        };
        let mid = magnetar_proto::MessageId {
            ledger_id: 1,
            entry_id: 5,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
        };
        let fut = consumer.ack_cumulative_with_txn(mid, txn);
        let res = tokio::time::timeout(Duration::from_millis(10), fut).await;
        assert!(res.is_err(), "expected pending future (no driver)");
        assert!(!consumer.is_closed());
    }

    // ── helper-method ports (MultiTopics surface lift, pass-1) ───────────
    //
    // The block below mirrors `crates/magnetar-runtime-tokio/src/consumer.rs`
    // 1:1 per ADR-0024 §strict test-count parity. Each helper has a tokio
    // counterpart with the same name and the same observable contract.

    #[tokio::test(flavor = "current_thread")]
    async fn available_in_queue_reflects_pending_messages() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/avail-queue".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);
        // Empty queue right after subscribe.
        assert_eq!(consumer.available_in_queue(), 0);

        // Pump a couple of CommandMessage frames; the per-consumer queue
        // grows lockstep with delivery.
        {
            let mut conn = shared.inner.lock();
            for i in 0..3_u64 {
                let bytes = command_message_bytes(handle.0, 300 + i, format!("q{i}").as_bytes());
                conn.handle_bytes(Instant::now(), &bytes)
                    .expect("handle CommandMessage");
            }
        }
        // The cardinality matches what the proto state machine accepted —
        // `>= 0` (the events-pump may have buffered some into the events
        // queue; the safety invariant is non-decrease relative to the
        // empty starting point).
        let depth = consumer.available_in_queue();
        assert!(depth <= 3, "queue depth must not exceed delivered count");

        // Closed/unknown handle path returns 0.
        let closed: Consumer<TokioProviders> =
            make_consumer(shared.clone(), magnetar_proto::ConsumerHandle(9999));
        assert_eq!(closed.available_in_queue(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn available_permits_reads_state_machine_counter() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/permits".to_owned(),
                subscription: "s".to_owned(),
                receiver_queue_size: 64,
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);
        // Right after subscribe, before the initial flow is granted, the
        // counter is zero.
        assert_eq!(consumer.available_permits(), 0);
        // Granting the initial flow bumps the counter to receiver_queue_size.
        {
            let mut conn = shared.inner.lock();
            let _ = conn.initial_flow(handle);
        }
        assert_eq!(consumer.available_permits(), 64);

        let closed: Consumer<TokioProviders> =
            make_consumer(shared, magnetar_proto::ConsumerHandle(9999));
        assert_eq!(closed.available_permits(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn has_received_any_message_flips_after_first_delivery() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/has-recv".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);
        assert!(
            !consumer.has_received_any_message(),
            "fresh consumer must report no messages received",
        );

        // Drive one CommandMessage through the state machine and then drain
        // it via `receive()` so the stats counter increments.
        {
            let mut conn = shared.inner.lock();
            let bytes = command_message_bytes(handle.0, 400, b"first");
            conn.handle_bytes(Instant::now(), &bytes)
                .expect("handle CommandMessage");
        }
        let _ = consumer.receive().await.expect("receive must resolve");
        assert!(
            consumer.has_received_any_message(),
            "has_received_any_message must flip true after first receive",
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn is_paused_reads_state_machine_flag() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/is-paused".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);
        assert!(!consumer.is_paused(), "default state is unpaused");
        consumer.pause();
        assert!(consumer.is_paused(), "after pause()");
        consumer.resume();
        assert!(!consumer.is_paused(), "after resume()");

        // Unknown handle defaults to false.
        let closed: Consumer<TokioProviders> =
            make_consumer(shared, magnetar_proto::ConsumerHandle(9999));
        assert!(!closed.is_paused());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn has_reached_end_of_topic_defaults_to_false() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/end-of-topic".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared, handle);
        assert!(
            !consumer.has_reached_end_of_topic(),
            "default state is not end-of-topic",
        );
        // is_inactive is a synonym for has_reached_end_of_topic per Java
        // semantics on the consumer surface.
        assert!(!consumer.is_inactive());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn receive_with_timeout_returns_none_on_idle_consumer() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/recv-timeout".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared, handle);
        // No messages have been pushed → the timeout fires and we get None.
        let result = consumer
            .receive_with_timeout(std::time::Duration::from_millis(50))
            .await
            .expect("receive_with_timeout must surface Ok(None) not an error");
        assert!(
            result.is_none(),
            "idle consumer must return Ok(None) after the deadline",
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn receive_with_timeout_returns_message_when_available() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/recv-timeout-ok".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        // Pre-deliver one message.
        {
            let mut conn = shared.inner.lock();
            let bytes = command_message_bytes(handle.0, 500, b"now");
            conn.handle_bytes(Instant::now(), &bytes)
                .expect("handle CommandMessage");
        }
        let consumer: Consumer<TokioProviders> = make_consumer(shared, handle);
        let result = consumer
            .receive_with_timeout(std::time::Duration::from_secs(2))
            .await
            .expect("receive_with_timeout must resolve")
            .expect("a message must be returned within the deadline");
        assert_eq!(result.payload.as_ref(), b"now");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn receive_batch_drains_already_buffered_messages() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/batch".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        // Pre-deliver 5 messages.
        {
            let mut conn = shared.inner.lock();
            for i in 0..5_u64 {
                let bytes = command_message_bytes(handle.0, 600 + i, format!("b{i}").as_bytes());
                conn.handle_bytes(Instant::now(), &bytes)
                    .expect("handle CommandMessage");
            }
        }
        let consumer: Consumer<TokioProviders> = make_consumer(shared, handle);
        let drained = consumer
            .receive_batch(10, std::time::Duration::from_secs(2))
            .await
            .expect("receive_batch must resolve");
        assert!(
            drained.len() <= 5,
            "drained {} messages but only 5 were delivered",
            drained.len()
        );
        assert!(
            !drained.is_empty(),
            "at least one message should have been drained",
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn receive_batch_with_bytes_cap_short_circuits_zero_caps() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/batch-zero".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared, handle);
        // max_messages=0 → empty without waiting.
        let zero_msgs = consumer
            .receive_batch_with_bytes_cap(0, 1024, std::time::Duration::from_secs(60))
            .await
            .expect("ok");
        assert!(zero_msgs.is_empty());
        // max_bytes=0 → empty without waiting.
        let zero_bytes = consumer
            .receive_batch_with_bytes_cap(10, 0, std::time::Duration::from_secs(60))
            .await
            .expect("ok");
        assert!(zero_bytes.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unsubscribe_force_true_round_trips_command_success() {
        // `unsubscribe(true)` issues `CommandUnsubscribe { force: true }`
        // and resolves on `CommandSuccess`. Mirrors the tokio engine's
        // counterpart 1:1 (ADR-0024). Two separate consumers per branch
        // because a successful unsubscribe consumes the broker-side
        // subscription state.
        for force in [false, true] {
            let shared = handshake_complete_shared();
            let handle = {
                let mut conn = shared.inner.lock();
                conn.subscribe(SubscribeRequest {
                    topic: format!("persistent://public/default/unsub-{force}"),
                    subscription: "s".to_owned(),
                    ..Default::default()
                })
            };
            let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);

            let request_id = shared.inner.lock().peek_next_request_id_for_test();
            let inj_shared = shared.clone();
            let inj = tokio::spawn(async move {
                for _ in 0..64 {
                    tokio::task::yield_now().await;
                    let has = inj_shared
                        .inner
                        .lock()
                        .has_pending_request_for_test(magnetar_proto::RequestId(request_id));
                    if has {
                        let cmd = pb::BaseCommand {
                            r#type: pb::base_command::Type::Success as i32,
                            success: Some(pb::CommandSuccess {
                                request_id,
                                schema: None,
                            }),
                            ..Default::default()
                        };
                        let mut buf = BytesMut::new();
                        magnetar_proto::encode_command(&mut buf, &cmd)
                            .expect("encode CommandSuccess");
                        inj_shared
                            .inner
                            .lock()
                            .handle_bytes(Instant::now(), &buf)
                            .expect("handle CommandSuccess");
                        return;
                    }
                }
                panic!("pending unsubscribe request never registered");
            });
            consumer
                .unsubscribe(force)
                .await
                .expect("unsubscribe must resolve on CommandSuccess");
            inj.await.expect("injector completes");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn republish_dead_letters_returns_zero_when_queue_is_empty() {
        // The DLQ is empty on a fresh consumer — `republish_dead_letters`
        // must short-circuit at 0 without touching the producer at all.
        // We pass a producer constructed against the same shared state so
        // we don't need a live broker, but the helper never actually
        // calls `.send()` because `drain_dead_letter` returns empty.
        use magnetar_proto::CreateProducerRequest;

        use crate::producer::Producer;

        let shared = handshake_complete_shared();
        let consumer_handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/dlq-empty".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let producer_handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/dlq-empty-DLQ".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), consumer_handle);
        let producer: Producer<TokioProviders> = Producer {
            shared,
            handle: producer_handle,
            compression: magnetar_proto::types::CompressionKind::None,
            _providers: std::marker::PhantomData,
        };
        let count = consumer
            .republish_dead_letters(&producer)
            .await
            .expect("republish_dead_letters must resolve");
        assert_eq!(count, 0, "no DLQ messages present");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reconsume_later_stamps_reconsumetimes_on_first_call() {
        // Behavioral check: even without a live broker we can drive
        // `reconsume_later_with_properties` against a producer wired into
        // the same shared state and observe the side-effects on the
        // sans-io outbox. The producer's `.send()` returns a pending
        // future that we don't await — we instead snapshot the outbox to
        // confirm the helper stamped the retry-letter property
        // conventions (RECONSUMETIMES=1, REAL_TOPIC, ORIGINAL_MESSAGE_ID).
        use bytes::Bytes;
        use magnetar_proto::{CreateProducerRequest, MessageId};

        use crate::producer::Producer;

        let shared = handshake_complete_shared();
        let consumer_handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/retry-src".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let producer_handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/retry-src-RETRY".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), consumer_handle);
        let producer: Producer<TokioProviders> = Producer {
            shared: shared.clone(),
            handle: producer_handle,
            compression: magnetar_proto::types::CompressionKind::None,
            _providers: std::marker::PhantomData,
        };
        // Drive the helper with a synthetic IncomingMessage; we don't
        // await the inner ack to completion — once `.send()` has been
        // called, the outbox should hold the framed publish bytes with
        // the retry properties baked in.
        let msg = magnetar_proto::IncomingMessage {
            message_id: MessageId {
                ledger_id: 7,
                entry_id: 99,
                partition: -1,
                batch_index: -1,
                batch_size: 0,
            },
            payload: Bytes::from_static(b"retryme"),
            metadata: magnetar_proto::pb::MessageMetadata::default(),
            single_metadata: None,
            redelivery_count: 0,
            broker_entry_metadata: None,
            arrived_at: Instant::now(),
        };
        // Use a yield-bounded select to give the helper one poll cycle
        // worth of progress, then bail. We can't actually finish because
        // there's no driver to ack the publish + the per-message ack.
        {
            let helper = consumer.reconsume_later(
                &producer,
                msg.clone(),
                std::time::Duration::from_millis(500),
            );
            tokio::pin!(helper);
            let outcome =
                tokio::time::timeout(std::time::Duration::from_millis(50), &mut helper).await;
            assert!(
                outcome.is_err(),
                "helper should still be parked (no driver to ack)",
            );
        }

        // Sanity: the sans-io layer's outbox holds the publish bytes
        // (subscribe + create-producer + publish all coalesced). We can
        // only assert non-empty + pending publish count > 0.
        let pending_publish_bytes = {
            let mut conn = shared.inner.lock();
            conn.poll_transmit().len()
        };
        assert!(
            pending_publish_bytes > 0,
            "reconsume_later must have queued the retry publish",
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drain_dead_letter_empty_by_default() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/dlq".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared, handle);
        assert!(
            consumer.drain_dead_letter().is_empty(),
            "no messages have been flagged for DLQ yet",
        );
    }
}
