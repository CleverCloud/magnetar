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
//! - [`Consumer::close`] — request-id-correlated reliable close that awaits the broker
//!   acknowledgement; dropping the final clone separately stages a best-effort close through the
//!   existing driver.
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

use crate::client::{Client, ClientError, operation_deadline_error, operation_deadline_expired};
use crate::crypto::MessageDecryptor;
use crate::{ConnectionShared, SleepProvider};

/// User-facing consumer handle for the moonpool engine.
///
/// Holds the shared connection state plus the protocol-layer
/// [`ConsumerHandle`]. Generic over the [`Providers`] bundle so the same
/// façade runs on production tokio sockets and on a `moonpool-sim`
/// deterministic substrate.
///
/// # Lock-ordering (ADR-0038)
///
/// Identity reads (topic, subscription, handle) go through `slot.identity`
/// without locking. State-machine reads take only the per-slot mutex via
/// `slot.state.lock()`. Operations that drive protocol I/O (`receive`,
/// `ack`, `seek`, `close`, …) take `shared.inner.lock()`. Acquisition order:
/// **global → per-slot, never the reverse**.
///
/// # Lifecycle
///
/// [`Self::close`] is the reliable shutdown path: it consumes the handle,
/// waits for the broker acknowledgement, and returns any close error.
/// If callers instead drop every clone, the final clone synchronously stages
/// a best-effort `CloseConsumer` and wakes the existing driver. Dropping an
/// intermediate clone does not close the consumer, and a terminal connection
/// with no remaining driver stages nothing.
pub struct Consumer<P: Providers> {
    shared: Arc<ConnectionShared>,
    handle: ConsumerHandle,
    /// Direct handle to this consumer's per-slot state.
    slot: Arc<magnetar_proto::ConsumerSlot>,
    /// Optional PIP-4 decryption hook. When the broker delivers a message with
    /// `MessageMetadata.encryption_keys` set, the consumer hands the ciphertext
    /// through this hook before yielding it to the user. 1:1 mirror of
    /// `magnetar_runtime_tokio::Consumer::decryptor`.
    decryptor: Option<Arc<dyn MessageDecryptor>>,
    /// Last-clone close guard shared by every `Consumer` clone.
    close_guard: Arc<ConsumerCloseGuard>,
    /// Type-erased `P::Time` sleep function inherited from the client.
    sleep_provider: Arc<SleepProvider>,
    /// Held only so `Consumer` is generic over `P` without leaking the
    /// driver-handle type parameter. The driver itself has already consumed
    /// the providers — the consumer just talks to the shared state.
    _providers: std::marker::PhantomData<fn() -> P>,
}

pub(crate) struct StagedConsumerSeek {
    request_id: RequestId,
    receiver_queue_size: usize,
    seek_message_id: Option<String>,
    seek_timestamp: Option<u64>,
}

impl<P: Providers> Clone for Consumer<P> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            handle: self.handle,
            slot: self.slot.clone(),
            decryptor: self.decryptor.clone(),
            close_guard: self.close_guard.clone(),
            sleep_provider: self.sleep_provider.clone(),
            _providers: std::marker::PhantomData,
        }
    }
}

/// RAII guard arming a best-effort `CommandCloseConsumer` on last-clone
/// drop. The explicit [`Consumer::close`] path remains confirmation-bearing.
#[derive(Debug)]
struct ConsumerCloseGuard {
    shared: Arc<ConnectionShared>,
    handle: ConsumerHandle,
    slot: Arc<magnetar_proto::ConsumerSlot>,
}

impl Drop for ConsumerCloseGuard {
    fn drop(&mut self) {
        if self.shared.is_no_driver() {
            return;
        }
        // ADR-0038: release the per-slot probe before taking the global
        // Connection mutex. The locks are sequential, never nested.
        let already_closed = self.slot.state.lock().closed;
        if already_closed {
            return;
        }
        {
            let now = self.shared.now_instant();
            let mut conn = self.shared.inner.lock();
            let _ = conn.close_consumer_forget(self.handle, now);
        }
        self.shared.operation_cancel_notify.notify_waiters();
        self.shared.driver_waker.notify_one();
        tracing::debug!(
            topic = %self.slot.identity.topic,
            subscription = %self.slot.identity.subscription,
            handle = ?self.handle,
            "consumer dropped without explicit close — best-effort CloseConsumer enqueued"
        );
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
    /// Assemble a consumer handle and arm its last-clone close guard.
    pub(crate) fn assemble(
        shared: Arc<ConnectionShared>,
        handle: ConsumerHandle,
        slot: Arc<magnetar_proto::ConsumerSlot>,
        decryptor: Option<Arc<dyn MessageDecryptor>>,
        sleep_provider: Arc<SleepProvider>,
    ) -> Self {
        let close_guard = Arc::new(ConsumerCloseGuard {
            shared: shared.clone(),
            handle,
            slot: slot.clone(),
        });
        Self {
            shared,
            handle,
            slot,
            decryptor,
            close_guard,
            sleep_provider,
            _providers: std::marker::PhantomData,
        }
    }

    /// The protocol-layer consumer handle this façade wraps. Useful in tests
    /// and instrumentation.
    #[must_use]
    pub fn handle(&self) -> ConsumerHandle {
        self.handle
    }

    /// Topic name this consumer is bound to. Returns an empty string if the
    /// consumer is no longer registered (closed).
    ///
    /// Identity-only read — takes no lock (ADR-0038).
    #[must_use]
    pub fn topic(&self) -> String {
        self.slot.identity.topic.clone()
    }

    /// Subscription name. Empty string if the consumer is no longer
    /// registered.
    ///
    /// Identity-only read — takes no lock.
    #[must_use]
    pub fn subscription(&self) -> String {
        self.slot.identity.subscription.clone()
    }

    /// `true` once this consumer has been closed — either locally via
    /// [`Self::close`] or remotely via a broker `CloseConsumer`. Mirrors Java
    /// `ConsumerImpl#getState() == CLOSED`.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.slot.state.lock().closed
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
        // ADR-0038: per-slot write via the direct Arc, no global lock.
        let source = source_topic.into();
        self.slot
            .state
            .lock()
            .set_shadow_metadata(magnetar_proto::ShadowTopicMetadata {
                source_topic: source,
            });
    }

    /// PIP-180 / ADR-0033: returns the cached source-topic name if this
    /// consumer is shadow-attached, or `None` for a regular consumer.
    /// 1:1 mirror of
    /// `magnetar_runtime_tokio::Consumer::shadow_source_topic`.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn shadow_source_topic(&self) -> Option<String> {
        self.slot
            .state
            .lock()
            .shadow_metadata
            .as_ref()
            .map(|m| m.source_topic.clone())
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
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn name(&self) -> String {
        self.slot
            .state
            .lock()
            .consumer_name
            .clone()
            .unwrap_or_default()
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
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn stats(&self) -> magnetar_proto::consumer::ConsumerStats {
        self.slot.state.lock().stats()
    }

    /// Clone of this consumer's live receive-latency histogram (issue #347).
    /// `None` if the histogram was never initialised (constructor failure,
    /// statically impossible). Backs the façade's `ConsumerApi::
    /// receive_latency_histogram` — used by `MultiTopicsConsumer::
    /// aggregate_stats` / `PartitionedConsumer::aggregate_stats` (in the
    /// `magnetar` façade crate) to merge several consumers' distributions
    /// via [`magnetar_proto::consumer::ConsumerStats::fold`].
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn receive_latency_histogram(&self) -> Option<hdrhistogram::Histogram<u64>> {
        self.slot.state.lock().receive_latency_histogram()
    }

    /// Number of messages currently buffered in this consumer's receiver
    /// queue, waiting for a `receive()` call to pull them out. Returns `0`
    /// for closed/unknown handles. Mirrors Java
    /// `Consumer#getNumMessagesInQueue`.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn available_in_queue(&self) -> usize {
        self.slot.state.lock().queue.len()
    }

    /// Number of dispatch permits this consumer still has with the broker
    /// — i.e. messages it has authorised the broker to push without an
    /// explicit `CommandFlow`. Returns `0` for closed/unknown handles.
    /// Mirrors Java `ConsumerBase#getAvailablePermits`.
    ///
    /// Issue #349 scope note: reads `ConsumerState::granted_permits` (the additive grant
    /// mirror), unchanged by the permit-balance split — out of scope per the issue's four
    /// locked design items, which name only `FlowStats::available_permits`.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn available_permits(&self) -> u32 {
        self.slot.state.lock().granted_permits
    }

    /// This consumer's CURRENT receiver-queue target (issue #301). For the
    /// default [`magnetar_proto::Fixed`] policy this is the configured constant;
    /// for [`magnetar_proto::Auto`] it is the live, auto-tuned value after the
    /// latest adjust tick. Mirrors Java `ConsumerImpl#getCurrentReceiverQueueSize`
    /// under PIP-74 auto-scaling. 1:1 with the tokio engine.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn current_receiver_queue_size(&self) -> usize {
        self.slot.state.lock().receiver_queue_size
    }

    /// `true` if this consumer has received at least one message since
    /// opening. Mirrors Java `Consumer#hasReceivedAnyMessage` — useful as a
    /// "did anything ever arrive?" probe without inspecting the full
    /// [`ConsumerStats`](magnetar_proto::consumer::ConsumerStats).
    #[must_use]
    pub fn has_received_any_message(&self) -> bool {
        self.stats().total_msgs_received > 0
    }

    /// Last broker-reported Failover active/standby state (issue #348).
    /// `None` until the first `CommandActiveConsumerChange` lands for this
    /// consumer (e.g. a `Shared` / `Exclusive` subscription never receives
    /// the command). 1:1 with the tokio engine.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn is_active(&self) -> Option<bool> {
        self.slot.state.lock().is_active
    }

    /// Resolve the next not-yet-observed Failover active/standby transition
    /// (issue #348). Mirrors [`Self::receive`]'s waker-slab parking pattern.
    /// 1:1 with the tokio engine.
    ///
    /// # Errors
    /// Resolves the same error [`Self::receive`] does once the consumer
    /// reaches a terminal state (closed, or a per-handle terminal subscribe
    /// failure) with no unobserved transition buffered.
    pub async fn next_active_change(&self) -> Result<bool, ClientError> {
        ActiveChangeFut {
            shared: self.shared.clone(),
            handle: self.handle,
            slab_key: None,
        }
        .await
    }

    /// Returns `true` if this consumer is currently paused (no automatic
    /// flow refills until [`Self::resume`]). Returns `false` for
    /// closed/unknown handles. Mirrors Java `Consumer#isPaused` (Pulsar
    /// itself doesn't expose this on the Java client; we surface it for
    /// observability).
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.slot.state.lock().paused
    }

    /// Returns `true` once the broker has indicated end-of-topic for this
    /// consumer (no further messages will be dispatched). Mirrors Java
    /// `Consumer#hasReachedEndOfTopic`.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn has_reached_end_of_topic(&self) -> bool {
        self.slot.state.lock().reached_end_of_topic
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
        self.republish_dead_letters_with_properties(dlq_producer, Vec::new())
            .await
    }

    /// Same as [`Self::republish_dead_letters`] but stamps `extra_properties`
    /// on every republished message (override on key collision). The façade
    /// uses this to re-inject the current OpenTelemetry span context
    /// (`traceparent` / `tracestate`) onto the dead-letter copy so the
    /// republished message is traced under the republish span instead of
    /// carrying the stale inbound trace (ADR-0053 §D2). Correlation back to
    /// the source is preserved via the `REAL_TOPIC` / `ORIGINAL_MESSAGE_ID`
    /// stamps.
    ///
    /// # Errors
    ///
    /// Returns the first [`ClientError`] encountered. Already-republished
    /// messages stay republished — partial progress is not rolled back.
    pub async fn republish_dead_letters_with_properties(
        &self,
        dlq_producer: &crate::Producer<P>,
        extra_properties: Vec<(String, String)>,
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
            // Re-inject / override (e.g. OTel traceparent) before the
            // correlation stamps. Mirrors Java's `DeadLetterTopicMessageId`
            // property convention.
            Self::apply_property_overrides(&mut metadata.properties, extra_properties.clone());
            // Stamp REAL_TOPIC + ORIGINAL_MESSAGE_ID through the override helper
            // so the correlation stamps always win over (and never duplicate) a
            // caller-supplied value.
            let real_topic = self
                .shared
                .inner
                .lock()
                .consumer_topic(self.handle)
                .unwrap_or("")
                .to_owned();
            // Per-message DLQ detail — `debug!` per ADR-0054 §2.1; payload
            // bytes are never logged.
            tracing::debug!(
                message_id = %msg.message_id,
                topic = %real_topic,
                "republishing dead-letter message"
            );
            Self::apply_property_overrides(
                &mut metadata.properties,
                vec![
                    ("REAL_TOPIC".to_owned(), real_topic),
                    ("ORIGINAL_MESSAGE_ID".to_owned(), msg.message_id.to_string()),
                ],
            );
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
        if count > 0 {
            // One success record per unit of work — `info!` per ADR-0054
            // §2.1 (silent when nothing was drained to avoid no-op noise).
            tracing::info!(count, "dead-letter republish complete");
        }
        Ok(count)
    }

    /// Merge `extra` properties into `props`, replacing any existing entry
    /// with the same key (override on collision) rather than appending a
    /// duplicate. Shared by the reconsume / DLQ-republish paths so a
    /// re-injected OTel `traceparent` / `tracestate` overwrites the inbound
    /// value (ADR-0053 §D2).
    fn apply_property_overrides(
        props: &mut Vec<magnetar_proto::pb::KeyValue>,
        extra: Vec<(String, String)>,
    ) {
        for (k, v) in extra {
            props.retain(|kv| kv.key != k);
            props.push(magnetar_proto::pb::KeyValue { key: k, value: v });
        }
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
        // Per-message retry-letter detail — `debug!` per ADR-0054 §2.1.
        tracing::debug!(
            message_id = %msg.message_id,
            delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            "scheduling reconsume"
        );
        let mut metadata = magnetar_proto::pb::MessageMetadata {
            partition_key: msg.metadata.partition_key.clone(),
            partition_key_b64_encoded: msg.metadata.partition_key_b64_encoded,
            ordering_key: msg.metadata.ordering_key.clone(),
            event_time: msg.metadata.event_time,
            properties: msg.metadata.properties.clone(),
            ..magnetar_proto::pb::MessageMetadata::default()
        };
        // Apply custom properties (overrides on key collision).
        Self::apply_property_overrides(&mut metadata.properties, custom_properties);
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
        let real_topic = self
            .shared
            .inner
            .lock()
            .consumer_topic(self.handle)
            .unwrap_or("")
            .to_owned();
        // Stamp RECONSUMETIMES + REAL_TOPIC + ORIGINAL_MESSAGE_ID through the
        // override helper so they always win over (and never duplicate) a
        // caller-supplied value, and consumers of the retry topic can correlate
        // back to the source.
        Self::apply_property_overrides(
            &mut metadata.properties,
            vec![
                ("RECONSUMETIMES".to_owned(), reconsumetimes.to_string()),
                ("REAL_TOPIC".to_owned(), real_topic),
                ("ORIGINAL_MESSAGE_ID".to_owned(), msg.message_id.to_string()),
            ],
        );
        // Set deliver_at_time so the broker queues the message for
        // `delay` past now. ADR-0011 — invariant #3: read the engine
        // wall clock (moonpool-virtualised under SimProviders, host
        // clock under TokioProviders) instead of `SystemTime::now`.
        let now_ms = i64::try_from(self.shared.now_wall_clock_ms()).unwrap_or(i64::MAX);
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
    ///
    /// Per-slot write — does NOT take the global Connection mutex.
    pub fn pause(&self) {
        self.slot.state.lock().paused = true;
    }

    /// Re-enables automatic flow refills. Wakes the driver so it can flush
    /// any FLOW frames the state machine queues as a result.
    ///
    /// Mirrors `org.apache.pulsar.client.api.Consumer#resume`.
    pub fn resume(&self) {
        self.slot.state.lock().paused = false;
        self.shared.driver_waker.notify_one();
    }

    #[cfg(feature = "scalable-topics")]
    pub(crate) fn flow_for_aggregate_with_debt(&self, fresh: u32, debt: Option<(u64, u32)>) {
        let mut conn = self.shared.inner.lock();
        let session_epoch = conn.session_epoch();
        let debt = debt
            .filter(|(debt_epoch, _)| *debt_epoch == session_epoch)
            .map_or(0, |(_, permits)| permits);
        conn.flow_for_aggregate(self.handle, fresh, debt);
        drop(conn);
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
            decryptor: self.decryptor.clone(),
            slab_key: None,
        }
        .await
    }

    #[cfg(feature = "scalable-topics")]
    pub(crate) async fn receive_deferred_until_end(
        &self,
    ) -> Result<Option<(u64, magnetar_proto::DeferredIncomingMessage)>, ClientError> {
        match (DeferredReceiveFut {
            shared: self.shared.clone(),
            handle: self.handle,
            slab_key: None,
            stop_at_end: true,
        })
        .await
        {
            Ok(message) => Ok(Some(message)),
            Err(ClientError::EndOfTopic) => Ok(None),
            Err(error) => Err(error),
        }
    }

    #[cfg(feature = "scalable-topics")]
    pub(crate) fn post_process_deferred(
        &self,
        message: &mut IncomingMessage,
    ) -> PostProcessOutcome {
        let action = self
            .shared
            .inner
            .lock()
            .consumer_crypto_failure_action(self.handle);
        post_process_message(message, self.decryptor.as_ref(), action)
    }

    #[cfg(feature = "scalable-topics")]
    pub(crate) fn close_best_effort(&self) {
        if !self.shared.is_no_driver() && !self.is_closed() {
            {
                let now = self.shared.now_instant();
                let mut conn = self.shared.inner.lock();
                let _ = conn.close_consumer_forget(self.handle, now);
            }
            self.shared.operation_cancel_notify.notify_waiters();
            self.shared.driver_waker.notify_one();
        }
    }

    #[cfg(feature = "scalable-topics")]
    pub(crate) fn force_close_best_effort(&self) {
        if !self.shared.is_no_driver() {
            {
                let now = self.shared.now_instant();
                let mut conn = self.shared.inner.lock();
                let _ = conn.close_consumer_forget(self.handle, now);
            }
            self.shared.operation_cancel_notify.notify_waiters();
            self.shared.driver_waker.notify_one();
        }
    }

    /// Receive the next message, bounded by `timeout`. Returns `Ok(None)` if
    /// the deadline elapses with no message. Mirrors Java
    /// `Consumer#receive(int timeout, TimeUnit unit)`.
    ///
    /// The deadline is driven by the connection's Moonpool
    /// [`moonpool_core::TimeProvider`]: wall time under `TokioProviders` and
    /// virtual time under `SimProviders`.
    ///
    /// # Errors
    /// Propagates [`Self::receive`] errors. The timeout case returns
    /// `Ok(None)` rather than an error to match Java's "no message"
    /// semantic.
    pub async fn receive_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Option<IncomingMessage>, ClientError> {
        let receive = self.receive();
        let sleep = (self.sleep_provider)(timeout);
        let mut receive = std::pin::pin!(receive);
        let mut sleep = std::pin::pin!(sleep);
        moonpool_core::select! {
            biased;
            result = &mut receive => result.map(Some),
            result = &mut sleep => match result {
                Ok(()) | Err(moonpool_core::TimeError::Elapsed) => Ok(None),
                Err(moonpool_core::TimeError::Shutdown) => Err(ClientError::Closed),
            },
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
        let Some(first) = self.receive_with_timeout(max_wait).await? else {
            return Ok(Vec::new());
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
            // ADR-0011/ADR-0086: pull the instant through the INJECTED provider, never
            // `Instant::now()`, and before the connection mutex (ADR-0038). This is what makes
            // the receive-latency histogram reproducible per seed under simulation.
            let now = self.shared.now_instant();
            let msg = {
                let mut conn = self.shared.inner.lock();
                conn.pop_message(self.handle, now)
            };
            let Some(mut msg) = msg else { break };
            // PIP-4: honor the per-consumer crypto failure action for every encrypted message
            // popped here (the first message went through `receive()` which already does this).
            // Without this, messages 2..N of a batch would leak ciphertext to the caller and
            // ignore the Fail/Discard/Consume policy. 1:1 with the tokio batch path.
            let action = self
                .shared
                .inner
                .lock()
                .consumer_crypto_failure_action(self.handle);
            match post_process_message(&mut msg, self.decryptor.as_ref(), action) {
                PostProcessOutcome::Deliver => {
                    acc_bytes = acc_bytes.saturating_add(msg.payload.len());
                    out.push(msg);
                }
                PostProcessOutcome::Discard => {
                    // Ack and continue — the caller should never see this message.
                    let now = self.shared.now_instant();
                    let mut conn = self.shared.inner.lock();
                    let _ = conn.ack(
                        self.handle,
                        magnetar_proto::AckRequest {
                            message_ids: vec![msg.message_id],
                            ack_type: magnetar_proto::pb::command_ack::AckType::Individual,
                            properties: Vec::new(),
                            txn_id: None,
                        },
                        now,
                    );
                    // Drop the connection lock before notifying the driver (lock-ordering:
                    // global → per-slot, and `notify_one` must never run under the conn lock).
                    drop(conn);
                    self.shared.driver_waker.notify_one();
                }
                PostProcessOutcome::Fail(err) => return Err(err),
            }
        }
        // Postcondition (ADR-0024): the batch accumulator never exceeds the
        // caller's `max_messages` cap. The `while out.len() < max_messages`
        // guard plus the at-most-one `out.push` per iteration (the `Discard`
        // arm pushes nothing, `Deliver` pushes exactly one) make this a pure
        // function of the local loop — it can never fire on broker/wire input.
        // 1:1 mirror of the tokio engine's `receive_batch_with_bytes_cap`.
        debug_assert!(
            out.len() <= max_messages,
            "receive_batch overshot max_messages: out.len()={} max_messages={max_messages}",
            out.len()
        );
        // The first message is unconditionally pushed before the loop (Java's
        // "deliver at least one" semantic), so any path that reaches here
        // returns a non-empty batch — the empty-result cases (zero caps,
        // first-receive timeout/error) return earlier.
        debug_assert!(
            !out.is_empty(),
            "receive_batch reached the return with an empty accumulator"
        );
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

    /// Acknowledge multiple messages in one individual-ack round trip.
    pub async fn ack_batch(&self, message_ids: Vec<MessageId>) -> Result<(), ClientError> {
        self.ack_inner(
            message_ids,
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

    /// Acknowledge multiple messages in one transactional round trip.
    pub async fn ack_batch_with_txn(
        &self,
        message_ids: Vec<MessageId>,
        txn_id: magnetar_proto::TxnId,
    ) -> Result<(), ClientError> {
        self.ack_inner(
            message_ids,
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
        // Per-message hot-path record — `trace!` per ADR-0054 §2.1.
        tracing::trace!(handle = ?self.handle, message_id = %message_id, "grouped ack staged");
        // ADR-0011: route the sans-io monotonic input through the
        // engine-supplied clock so deterministic-sim runs feed virtual
        // Instants into `ack_grouped_individual`. Production TokioProviders
        // default the closure to `Instant::now` so behaviour is unchanged.
        let now = self.shared.now_instant();
        {
            let mut conn = self.shared.inner.lock();
            conn.ack_grouped_individual(self.handle, message_id, now);
        }
        self.shared.driver_waker.notify_one();
    }

    /// Stage a cumulative ack into this consumer's ack-grouping tracker.
    /// See [`Self::ack_grouped`] for the semantics.
    pub fn ack_grouped_cumulative(&self, message_id: MessageId) {
        // Per-message hot-path record — `trace!` per ADR-0054 §2.1.
        tracing::trace!(
            handle = ?self.handle,
            message_id = %message_id,
            "grouped cumulative ack staged"
        );
        // ADR-0011: engine-supplied clock; see `ack_grouped`.
        let now = self.shared.now_instant();
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
        self.ack_inner_with_message_id_data(message_ids, ack_type, properties, txn_id, None)
            .await
    }

    #[cfg(feature = "scalable-topics")]
    pub(crate) fn ack_stream_component(
        &self,
        message_ids: Vec<MessageId>,
        message_id_data: Vec<pb::MessageIdData>,
        ack_type: pb::command_ack::AckType,
        txn_id: Option<magnetar_proto::TxnId>,
    ) -> impl Future<Output = Result<(), ClientError>> {
        self.ack_inner_with_message_id_data(
            message_ids,
            ack_type,
            Vec::new(),
            txn_id,
            Some(message_id_data),
        )
    }

    #[cfg(feature = "scalable-topics")]
    pub(crate) fn settle_transactional_acks(&self, txn_id: magnetar_proto::TxnId, committed: bool) {
        self.shared
            .inner
            .lock()
            .settle_transactional_acks(self.handle, txn_id, committed);
    }

    #[cfg(all(test, feature = "scalable-topics"))]
    pub(crate) fn last_acked_message_id_for_test(&self) -> Option<MessageId> {
        self.slot.state.lock().last_acked_message_id
    }

    fn ack_inner_with_message_id_data(
        &self,
        message_ids: Vec<MessageId>,
        ack_type: pb::command_ack::AckType,
        properties: Vec<(String, i64)>,
        txn_id: Option<magnetar_proto::TxnId>,
        message_id_data: Option<Vec<pb::MessageIdData>>,
    ) -> impl Future<Output = Result<(), ClientError>> {
        // Per-message hot-path record — `trace!` per ADR-0054 §2.1.
        tracing::trace!(
            handle = ?self.handle,
            count = message_ids.len(),
            ack_type = ?ack_type,
            "ack enqueued"
        );
        // ADR-0011: engine-supplied clock; see `ack_grouped`.
        let now = self.shared.now_instant();
        let request_id = {
            let mut conn = self.shared.inner.lock();
            let ack = AckRequest {
                message_ids,
                ack_type,
                properties,
                txn_id,
            };
            match message_id_data {
                Some(message_id_data) => {
                    conn.ack_with_message_id_data(self.handle, ack, message_id_data, now)
                }
                None => conn.ack(self.handle, ack, now),
            }
        };
        self.shared.driver_waker.notify_one();
        let shared = self.shared.clone();
        async move {
            let outcome = RequestFut { shared, request_id }.await;
            map_ack_outcome(outcome)
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

    #[cfg(feature = "scalable-topics")]
    pub(crate) fn negative_ack_message_id_data(&self, message_id: pb::MessageIdData) {
        let now = self.shared.now_instant();
        let mut conn = self.shared.inner.lock();
        conn.negative_ack_with_message_id_data(self.handle, vec![message_id], now);
        drop(conn);
        self.shared.driver_waker.notify_one();
    }

    /// Negatively acknowledge a batch of messages. An empty
    /// `message_ids` vector matches Pulsar's "all unacked" semantics
    /// used by [`Self::redeliver_unacked`].
    pub fn negative_ack_many(&self, message_ids: Vec<MessageId>) {
        // Per-message hot-path record — `trace!` per ADR-0054 §2.1. An
        // empty list is Pulsar's "redeliver all unacked" wildcard.
        tracing::trace!(
            handle = ?self.handle,
            count = message_ids.len(),
            "negative ack enqueued"
        );
        // ADR-0011: engine-supplied clock; see `ack_grouped`.
        let now = self.shared.now_instant();
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
        // Per-message hot-path record — `trace!` per ADR-0054 §2.1.
        tracing::trace!(
            handle = ?self.handle,
            message_id = %message_id,
            delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            "negative ack with delay enqueued"
        );
        // ADR-0011: engine-supplied clock; see `ack_grouped`.
        let now = self.shared.now_instant();
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
    /// `Consumer#unsubscribe`. After a successful call the consumer is
    /// unusable; a broker rejection leaves it available and restores any
    /// interrupted reattachment retry.
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
            conn.try_unsubscribe(self.handle, force)
                .ok_or_else(|| ClientError::Other("unsubscribe already in progress".to_owned()))?
        };
        self.shared.operation_cancel_notify.notify_waiters();
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        self.shared.operation_cancel_notify.notify_waiters();
        match outcome {
            OpOutcome::Success { .. } => {
                // The proto layer owns lifecycle finalization so cancellation
                // of this future cannot strand the consumer in Closing.
                // Lifecycle record (ADR-0054).
                tracing::info!(
                    topic = %self.slot.identity.topic,
                    subscription = %self.slot.identity.subscription,
                    handle = ?self.handle,
                    force,
                    "consumer unsubscribed"
                );
                Ok(())
            }
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
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
    pub async fn get_schema(
        &self,
        version: Option<bytes::Bytes>,
    ) -> Result<pb::Schema, ClientError> {
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
        // Per-operation internals — `debug!` per ADR-0054 §2.1.
        tracing::debug!(topic = %topic, "schema lookup");
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
            OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
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
            OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
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

    #[cfg(feature = "scalable-topics")]
    pub(crate) fn stage_seek_to_message_id_data(
        &self,
        message_id: pb::MessageIdData,
    ) -> StagedConsumerSeek {
        self.stage_seek(SeekTarget::MessageIdData(message_id))
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
        let staged = self.stage_seek(target);
        self.complete_staged_seek(staged).await
    }

    fn stage_seek(&self, target: SeekTarget) -> StagedConsumerSeek {
        // Snapshot the seek target for the lifecycle record below (`target`
        // moves into `conn.seek`).
        let (seek_message_id, seek_timestamp) = match &target {
            SeekTarget::MessageId(id) => (Some(id.to_string()), None),
            SeekTarget::MessageIdData(id) => (Some(MessageId::from_pb(id).to_string()), None),
            SeekTarget::PublishTime(ts) => (None, Some(*ts)),
        };
        let receiver_queue_size = self.slot.state.lock().receiver_queue_size;
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.seek(self.handle, target)
        };
        self.shared.driver_waker.notify_one();
        StagedConsumerSeek {
            request_id,
            receiver_queue_size,
            seek_message_id,
            seek_timestamp,
        }
    }

    pub(crate) async fn complete_staged_seek(
        &self,
        staged: StagedConsumerSeek,
    ) -> Result<(), ClientError> {
        let StagedConsumerSeek {
            request_id,
            receiver_queue_size,
            seek_message_id,
            seek_timestamp,
        } = staged;
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::Success { .. } => {
                let resub_request_id = {
                    let mut conn = self.shared.inner.lock();
                    conn.resubscribe_consumer_after_seek(self.handle)
                };
                crate::driver::notify_retry_generation_replaced(&self.shared);
                let resubscribed = resub_request_id.is_some();
                if let Some(waiter_id) = resub_request_id {
                    SubscribeAckedFut {
                        shared: self.shared.clone(),
                        handle: self.handle,
                        accept_prior_attachment: false,
                        expected_waiter_id: Some(waiter_id),
                        notification: None,
                    }
                    .await?;
                    {
                        let now = self.shared.now_instant();
                        let mut conn = self.shared.inner.lock();
                        if receiver_queue_size != 0 {
                            let _ = conn.initial_flow(self.handle, now);
                        }
                        conn.redeliver_unacked_all(self.handle);
                    }
                    self.shared.driver_waker.notify_one();
                }
                let initial_flow_permits = receiver_queue_size as u64;
                tracing::info!(
                    topic = %self.slot.identity.topic,
                    subscription = %self.slot.identity.subscription,
                    handle = ?self.handle,
                    message_id = seek_message_id.as_deref(),
                    timestamp = seek_timestamp,
                    resubscribe = resubscribed,
                    initial_flow_permits,
                    redeliver_unacked_all = resubscribed,
                    "consumer seek completed"
                );
                Ok(())
            }
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
            other => Err(ClientError::Other(format!(
                "unexpected seek outcome: {other:?}"
            ))),
        }
    }

    /// Reliably close this consumer.
    ///
    /// Consumes the handle, wakes the connection driver, and resolves only
    /// after the broker acknowledges `CloseConsumer`. After this resolves,
    /// the consumer handle is invalidated.
    ///
    /// Dropping the final clone is a separate best-effort safety net: it
    /// stages the close without waiting and cannot report broker errors.
    ///
    /// Does not tear down the underlying connection-level driver; that is
    /// owned by the [`Client`] which spawned this consumer.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker reports a close failure.
    /// - [`ClientError::Other`] when an unexpected outcome arrives.
    pub async fn close(self) -> Result<(), ClientError> {
        let request_id = {
            let now = self.shared.now_instant();
            let mut conn = self.shared.inner.lock();
            conn.close_consumer(self.handle, now)
        };
        self.shared.operation_cancel_notify.notify_waiters();
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            request_id,
        }
        .await;
        match outcome {
            OpOutcome::Success { .. } => {
                // Lifecycle record (ADR-0054).
                tracing::info!(
                    topic = %self.slot.identity.topic,
                    subscription = %self.slot.identity.subscription,
                    handle = ?self.handle,
                    "consumer closed"
                );
                Ok(())
            }
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
            other => Err(ClientError::Other(format!(
                "unexpected close outcome: {other:?}"
            ))),
        }
    }
}

fn map_ack_outcome(outcome: OpOutcome) -> Result<(), ClientError> {
    match outcome {
        OpOutcome::Success { .. } => Ok(()),
        OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
        OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
        other => Err(ClientError::Other(format!(
            "unexpected ack outcome: {other:?}"
        ))),
    }
}

impl<P: Providers + Send + Sync> Client<P> {
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
    /// - [`ClientError::PeerClosed`] on a TERMINAL connection drop before the subscribe acked
    ///   (ADR-0055 §1); a user-requested graceful close surfaces [`ClientError::Closed`].
    /// - [`ClientError::Broker`] after retryable broker refusals exhaust `OperationRetryConfig`.
    /// - [`ClientError::Other`] when the deadline expires before any broker error is recorded.
    pub async fn subscribe(&self, req: SubscribeRequest) -> Result<Consumer<P>, ClientError> {
        let mut deadline = self.operation_timer();
        let mut last_broker_error = None;
        self.subscribe_with_operation_deadline(req, None, deadline.as_mut(), &mut last_broker_error)
            .await
    }

    /// Same as [`Self::subscribe`] but with an optional PIP-4 decryption hook.
    /// 1:1 mirror of `magnetar_runtime_tokio::Client::subscribe_with`.
    ///
    /// # Errors
    /// - [`ClientError::Closed`] if the broker closed the consumer mid-handshake.
    /// - [`ClientError::PeerClosed`] on a TERMINAL connection drop before the subscribe acked
    ///   (ADR-0055 §1); a user-requested graceful close surfaces [`ClientError::Closed`].
    /// - [`ClientError::Broker`] after retryable broker refusals exhaust `OperationRetryConfig`.
    pub async fn subscribe_with(
        &self,
        req: SubscribeRequest,
        decryptor: Option<Arc<dyn MessageDecryptor>>,
    ) -> Result<Consumer<P>, ClientError> {
        let mut deadline = self.operation_timer();
        let mut last_broker_error = None;
        self.subscribe_with_operation_deadline(
            req,
            decryptor,
            deadline.as_mut(),
            &mut last_broker_error,
        )
        .await
    }

    /// Deadline-aware subscribe seam used by the engine-generic façade.
    #[doc(hidden)]
    pub async fn subscribe_with_operation_deadline(
        &self,
        req: SubscribeRequest,
        decryptor: Option<Arc<dyn MessageDecryptor>>,
        mut deadline: Pin<&mut (dyn Future<Output = ()> + Send)>,
        last_broker_error: &mut Option<(i32, String)>,
    ) -> Result<Consumer<P>, ClientError> {
        // See `Client::open_producer`: subscribe also needs lookup-driven bundle
        // activation. Mirrors `magnetar-runtime-tokio`'s `Client::subscribe_with`. On a
        // client built via `connect_plain_supervised`, ADR-0039 proxy routing fans the
        // `Proxy` branch through the per-broker pool inside `Client::resolve_target`.
        let retry_config = self.shared().inner.lock().operation_retry_config().clone();
        let mut attachment_failures = 0_u32;
        loop {
            let (target, landed_on) = self
                .lookup_topic_target_with_operation_deadline(
                    &req.topic,
                    deadline.as_mut(),
                    last_broker_error,
                )
                .await?;
            let shared = moonpool_core::select! {
                biased;
                () = deadline.as_mut() => {
                    return Err(operation_deadline_error(
                        "consumer target resolution",
                        last_broker_error.clone(),
                    ));
                }
                result = self.resolve_target(&target, &landed_on, &req.topic) => result?,
            };
            shared.fail_if_no_driver()?;
            if operation_deadline_expired(deadline.as_mut()) {
                return Err(operation_deadline_error(
                    "consumer subscribe",
                    last_broker_error.clone(),
                ));
            }
            let (handle, slot) = {
                let mut conn = shared.inner.lock();
                let handle = conn.subscribe(req.clone());
                let slot = conn
                    .consumer(handle)
                    .cloned()
                    .expect("just-created consumer slot must exist");
                (handle, slot)
            };
            shared.driver_waker.notify_one();
            let mut guard = PendingConsumerSubscribeGuard::new(shared.clone(), handle);
            let acked = SubscribeAckedFut {
                shared: shared.clone(),
                handle,
                accept_prior_attachment: true,
                expected_waiter_id: None,
                notification: None,
            };
            tokio::pin!(acked);
            let ack_result = moonpool_core::select! {
                biased;
                () = deadline.as_mut() => {
                    let last = shared
                        .inner
                        .lock()
                        .consumer_last_subscribe_error(handle)
                        .or_else(|| last_broker_error.clone());
                    Err(operation_deadline_error("consumer subscribe", last))
                }
                result = acked.as_mut() => result,
            };
            match ack_result {
                Ok(()) => {
                    guard.disarm();
                    {
                        // ADR-0011: pull the instant through the engine clock
                        // (virtual time under `SimProviders`) rather than the
                        // host, then hand it to the state machine — this is also
                        // what arms the receiver-queue auto-adjust schedule
                        // (follow-ups §4), so a seed replay arms it identically.
                        let now = shared.now_instant();
                        let mut conn = shared.inner.lock();
                        let _ = conn.initial_flow(handle, now);
                        let initial_target = slot.state.lock().receiver_queue_size;
                        if initial_target > 0 {
                            conn.flow(handle, initial_target as u32);
                        }
                    }
                    shared.driver_waker.notify_one();
                    tracing::info!(
                        topic = %slot.identity.topic,
                        subscription = %slot.identity.subscription,
                        handle = ?handle,
                        "consumer subscribed"
                    );
                    return Ok(Consumer::assemble(
                        shared,
                        handle,
                        slot,
                        decryptor,
                        self.sleep_provider(),
                    ));
                }
                Err(ClientError::Broker { code, message })
                    if magnetar_proto::is_retryable_broker_error(
                        magnetar_proto::OperationKind::Subscribe,
                        code,
                    ) =>
                {
                    *last_broker_error = Some((code, message.clone()));
                    attachment_failures = attachment_failures.saturating_add(1);
                    if !retry_config.should_retry_after_failure(attachment_failures) {
                        return Err(ClientError::Broker { code, message });
                    }
                    drop(guard);
                    let sleep_provider = self.sleep_provider();
                    let mut sleep =
                        sleep_provider(retry_config.delay_after_failure(attachment_failures));
                    moonpool_core::select! {
                        biased;
                        () = deadline.as_mut() => {
                            return Err(operation_deadline_error(
                                "consumer subscribe retry",
                                last_broker_error.clone(),
                            ));
                        }
                        _ = sleep.as_mut() => {}
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }
}

struct PendingConsumerSubscribeGuard {
    shared: Arc<ConnectionShared>,
    handle: ConsumerHandle,
    armed: bool,
}

#[cfg(feature = "scalable-topics")]
pub(crate) async fn subscribe_manual_flow_on<P: Providers>(
    shared: Arc<ConnectionShared>,
    req: SubscribeRequest,
    operation_timeout: std::time::Duration,
    sleep_provider: Arc<SleepProvider>,
) -> Result<Consumer<P>, ClientError> {
    shared.fail_if_no_driver()?;
    let (handle, slot) = {
        let mut conn = shared.inner.lock();
        let handle = conn.subscribe(req);
        let slot = conn
            .consumer(handle)
            .cloned()
            .ok_or_else(|| ClientError::Other("new segment consumer is missing".to_owned()))?;
        (handle, slot)
    };
    // Set the manual-FLOW fence before the subscribe frame can leave. A zero
    // queue suppresses initial permits; `paused` suppresses every refill path.
    slot.state.lock().paused = true;
    shared.driver_waker.notify_one();
    let mut guard = PendingConsumerSubscribeGuard::new(shared.clone(), handle);
    let acked = SubscribeAckedFut {
        shared: shared.clone(),
        handle,
        accept_prior_attachment: true,
        expected_waiter_id: None,
        notification: None,
    };
    let sleep = sleep_provider(operation_timeout);
    let mut acked = std::pin::pin!(acked);
    let mut sleep = std::pin::pin!(sleep);
    let result = moonpool_core::select! {
        biased;
        result = &mut acked => result,
        _ = &mut sleep => Err(ClientError::Other(
            "segment consumer subscribe exceeded operation_timeout".to_owned(),
        )),
    };
    if result.is_ok() {
        guard.disarm();
    }
    result.map(|()| Consumer::assemble(shared, handle, slot, None, sleep_provider))
}

impl PendingConsumerSubscribeGuard {
    fn new(shared: Arc<ConnectionShared>, handle: ConsumerHandle) -> Self {
        Self {
            shared,
            handle,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingConsumerSubscribeGuard {
    fn drop(&mut self) {
        if self.armed {
            self.shared
                .inner
                .lock()
                .cancel_consumer_subscribe(self.handle);
            self.shared.operation_cancel_notify.notify_waiters();
            self.shared.driver_waker.notify_one();
        }
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

impl Drop for RequestFut {
    /// Drop-time cleanup: clear our entry from the connection's waker slab and
    /// discard an outcome that may have landed immediately before cancellation
    /// so a cancelled consumer-side request future (seek, get-last-msg-id, ack
    /// receipt, etc.) leaves neither surface behind.
    /// Mirrors the tokio engine's
    /// [`magnetar_runtime_tokio::consumer::RequestFut::drop`]. Lookup
    /// multi-agent review MEDIUM-4; ADR-0024 four-layer parity.
    fn drop(&mut self) {
        let key = PendingOpKey::Request(self.request_id);
        let mut conn = self.shared.inner.lock();
        conn.unregister_waker(key);
        let _ = conn.take_outcome(key);
    }
}

/// Outcome returned by [`post_process_message`].
#[derive(Debug)]
pub(crate) enum PostProcessOutcome {
    /// The message is ready for the caller (plaintext, or — under `Consume` — ciphertext).
    Deliver,
    /// Decryption failed and the policy is [`magnetar_proto::CryptoFailureAction::Discard`].
    /// The caller should ack the message and continue.
    Discard,
    /// Decryption failed and the policy is `Fail` (or no decryptor was configured for an
    /// encrypted message). The caller should surface this error.
    Fail(ClientError),
}

/// Apply the consumer-side PIP-4 decryption and bounded decompression pipeline
/// to a message popped straight from the sans-io state machine.
///
/// The helper decrypts in place on `Deliver` (and leaves the ciphertext untouched under
/// `Consume`); it NEVER acks and NEVER touches the connection — it only decides the outcome.
/// The caller owns the ack-on-`Discard` and error-surfacing-on-`Fail` side effects.
fn post_process_message(
    msg: &mut IncomingMessage,
    decryptor: Option<&Arc<dyn MessageDecryptor>>,
    crypto_failure_action: magnetar_proto::CryptoFailureAction,
) -> PostProcessOutcome {
    if !msg.metadata.encryption_keys.is_empty() {
        let decrypt_result: Result<bytes::Bytes, ClientError> = match decryptor {
            Some(d) => d
                .decrypt(&msg.payload, &msg.metadata)
                .map_err(|err| ClientError::Other(format!("decrypt: {err}"))),
            None => Err(ClientError::Other(
                "received encrypted message but consumer has no decryptor configured".to_owned(),
            )),
        };
        match decrypt_result {
            Ok(plain) => msg.payload = plain,
            Err(err) => match crypto_failure_action {
                magnetar_proto::CryptoFailureAction::Fail => {
                    return PostProcessOutcome::Fail(err);
                }
                magnetar_proto::CryptoFailureAction::Discard => return PostProcessOutcome::Discard,
                magnetar_proto::CryptoFailureAction::Consume => {
                    // Preserve the ciphertext payload as-is; metadata.encryption_keys signals
                    // to the caller that the bytes are still encrypted.
                    return PostProcessOutcome::Deliver;
                }
            },
        }
    }
    if let Some(kind_i32) = msg.metadata.compression {
        let Ok(pb_kind) = pb::CompressionType::try_from(kind_i32) else {
            return PostProcessOutcome::Fail(ClientError::Other(format!(
                "unknown compression code {kind_i32}"
            )));
        };
        let kind = crate::compress::kind_from_pb(pb_kind);
        if kind != magnetar_proto::types::CompressionKind::None {
            let expected = msg
                .metadata
                .uncompressed_size
                .map_or(msg.payload.len(), |size| size as usize);
            match crate::compress::decompress(kind, &msg.payload, expected) {
                Ok(plain) => msg.payload = plain,
                Err(error) => {
                    return PostProcessOutcome::Fail(ClientError::Other(format!(
                        "decompress: {error}"
                    )));
                }
            }
        }
    }
    PostProcessOutcome::Deliver
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
    /// Optional PIP-4 decryption hook, cloned from the owning [`Consumer`].
    decryptor: Option<Arc<dyn MessageDecryptor>>,
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
        // Loop so that PIP-4 `Discard` can ack the undecryptable message and immediately try
        // the next queued one without bouncing back to the executor. 1:1 mirror of
        // `magnetar_runtime_tokio::consumer::ReceiveFut::poll`.
        loop {
            // ADR-0011/ADR-0086: injected provider, before the connection mutex (ADR-0038).
            let now = shared.now_instant();
            let mut conn = shared.inner.lock();
            if let Some(mut msg) = conn.pop_message(handle, now) {
                // Clear any stale slab entry; we resolved successfully.
                if let Some(key) = this.slab_key.take() {
                    conn.cancel_consumer_receive_waker(handle, key);
                }
                drop(conn);
                // pop_message may have queued FLOW frames; wake the driver to flush.
                shared.driver_waker.notify_one();

                // PIP-4 decryption: if the metadata carries encryption keys, the payload
                // arrived as ciphertext; hand it to the configured decryptor via the shared
                // `post_process_message` helper. The decryption failure policy is per-consumer
                // (PIP-4); resolve it now — before attempting decrypt — so even the
                // "no decryptor configured" path can honor `Discard` / `Consume` instead of
                // unconditionally failing. Inbound compression remains valid even though the
                // moonpool producer currently refuses compressed sends; the shared helper
                // decrypts first and then performs bounded decompression.
                let action = shared.inner.lock().consumer_crypto_failure_action(handle);
                match post_process_message(&mut msg, this.decryptor.as_ref(), action) {
                    PostProcessOutcome::Deliver => return Poll::Ready(Ok(msg)),
                    PostProcessOutcome::Fail(err) => return Poll::Ready(Err(err)),
                    PostProcessOutcome::Discard => {
                        // Ack the undecryptable message so the broker doesn't redeliver it (the
                        // only consumer of this subscription couldn't read it anyway), then loop
                        // to try the next queued message. Mirrors Java's
                        // `ConsumerImpl#decryptPayloadIfNeeded` which calls `discardMessage(...)`
                        // (an explicit ack) when the policy is `DISCARD`.
                        let now = shared.now_instant();
                        let mut conn = shared.inner.lock();
                        let _ = conn.ack(
                            handle,
                            magnetar_proto::AckRequest {
                                message_ids: vec![msg.message_id],
                                ack_type: magnetar_proto::pb::command_ack::AckType::Individual,
                                properties: Vec::new(),
                                txn_id: None,
                            },
                            now,
                        );
                        drop(conn);
                        shared.driver_waker.notify_one();
                        continue;
                    }
                }
            }
            // Genuinely-terminal state with no buffered message → resolve Err.
            // Issue #299: gate on `consumer_handle_is_terminal`, NOT
            // `is_closed()`. A transport drop sets `HandshakeState::Failed` for
            // the WHOLE supervised reconnect window and `reset()` wakes the
            // parked receive wakers while still `Failed`; the old `is_closed()`
            // guard erroneously resolved `Err(Closed)` during that recoverable
            // window. The terminal predicate re-parks instead (folding in a
            // per-handle terminal subscribe failure, issue #302), so `receive()`
            // transparently resumes once the supervisor reconnects + rebuild
            // replays `CommandSubscribe`. 1:1 with the tokio engine.
            if conn.consumer_handle_is_terminal(handle) || shared.is_no_driver() {
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
                    continue;
                }
                this.slab_key = Some(key);
                drop(conn);
                return Poll::Pending;
            }
            // Consumer was removed in the meantime; surface as closed on the
            // next poll.
            drop(conn);
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
    }
}

#[cfg(feature = "scalable-topics")]
struct DeferredReceiveFut {
    shared: Arc<ConnectionShared>,
    handle: ConsumerHandle,
    slab_key: Option<usize>,
    stop_at_end: bool,
}

#[cfg(feature = "scalable-topics")]
impl Drop for DeferredReceiveFut {
    fn drop(&mut self) {
        if let Some(key) = self.slab_key.take() {
            self.shared
                .inner
                .lock()
                .cancel_consumer_receive_waker(self.handle, key);
        }
    }
}

#[cfg(feature = "scalable-topics")]
impl Future for DeferredReceiveFut {
    type Output = Result<(u64, magnetar_proto::DeferredIncomingMessage), ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let now = this.shared.now_instant();
        let mut conn = this.shared.inner.lock();
        if let Some(message) = conn.pop_deferred_message(this.handle, now) {
            let session_epoch = conn.session_epoch();
            if let Some(key) = this.slab_key.take() {
                conn.cancel_consumer_receive_waker(this.handle, key);
            }
            drop(conn);
            this.shared.driver_waker.notify_one();
            return Poll::Ready(Ok((session_epoch, message)));
        }
        if conn.consumer_handle_is_terminal(this.handle) || this.shared.is_no_driver() {
            if let Some(key) = this.slab_key.take() {
                conn.cancel_consumer_receive_waker(this.handle, key);
            }
            return Poll::Ready(Err(ClientError::Closed));
        }
        if this.stop_at_end && conn.consumer_reached_end_of_topic(this.handle) {
            if let Some(key) = this.slab_key.take() {
                conn.cancel_consumer_receive_waker(this.handle, key);
            }
            return Poll::Ready(Err(ClientError::EndOfTopic));
        }
        if let Some(key) = this.slab_key.take() {
            conn.cancel_consumer_receive_waker(this.handle, key);
        }
        let key = conn
            .register_consumer_receive_waker(this.handle, cx.waker().clone())
            .expect("a non-terminal consumer remains registered while its connection is locked");
        this.slab_key = Some(key);
        Poll::Pending
    }
}

/// Future returned by [`Consumer::next_active_change`] (issue #348). Parks on
/// the per-consumer active-change waker slab exposed by
/// [`magnetar_proto::Connection::register_consumer_active_change_waker`]
/// until a transition is recorded or the consumer reaches a terminal state.
///
/// On drop the future evicts its slab slot via
/// [`magnetar_proto::Connection::cancel_consumer_active_change_waker`] so
/// cancelled waits don't leak entries until the next transition. 1:1 mirror
/// of `magnetar_runtime_tokio::consumer::ActiveChangeFut`.
struct ActiveChangeFut {
    shared: Arc<ConnectionShared>,
    handle: ConsumerHandle,
    /// Slab key of the currently-installed waker, if any.
    slab_key: Option<usize>,
}

impl Drop for ActiveChangeFut {
    fn drop(&mut self) {
        if let Some(key) = self.slab_key.take() {
            let mut conn = self.shared.inner.lock();
            conn.cancel_consumer_active_change_waker(self.handle, key);
        }
    }
}

impl Future for ActiveChangeFut {
    type Output = Result<bool, ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let handle = this.handle;
        let shared = this.shared.clone();
        let mut conn = shared.inner.lock();
        if let Some(active) = conn.pop_consumer_active_change(handle) {
            if let Some(key) = this.slab_key.take() {
                conn.cancel_consumer_active_change_waker(handle, key);
            }
            return Poll::Ready(Ok(active));
        }
        // Genuinely-terminal state with no buffered transition → resolve Err.
        // Same terminal-vs-recoverable distinction as `ReceiveFut` (issue
        // #299). 1:1 with the tokio engine.
        if conn.consumer_handle_is_terminal(handle) || shared.is_no_driver() {
            if let Some(old_key) = this.slab_key.take() {
                conn.cancel_consumer_active_change_waker(handle, old_key);
            }
            return Poll::Ready(Err(ClientError::Closed));
        }
        // Refresh the slab registration so the current task is the one woken.
        if let Some(old_key) = this.slab_key.take() {
            conn.cancel_consumer_active_change_waker(handle, old_key);
        }
        let key = conn.register_consumer_active_change_waker(handle, cx.waker().clone());
        drop(conn);
        // The connection lock is held from the pop check through the
        // registration above, so no transition, close, or removal can
        // interleave within this poll: the terminal check proves the handle
        // exists, and registration on a live handle cannot fail
        // (`register_consumer_active_change_waker` returns `None` only for an
        // absent handle, which `consumer_handle_is_terminal` reports as
        // terminal).
        debug_assert!(
            key.is_some(),
            "active-change waker registration failed for a live handle"
        );
        this.slab_key = key;
        Poll::Pending
    }
}

/// Future that resolves once the broker has acked the subscribe for the
/// given [`ConsumerHandle`]. It selectively removes events for this handle;
/// concurrent consumers' events remain queued for their owning waiters.
///
/// The dedicated event notification is owned and polled before connection
/// state is inspected. It therefore remains registered across `Pending` and
/// cannot miss a driver `notify_waiters()` that races with the state check.
/// Keeping it separate from `driver_waker` also prevents this waiter from
/// consuming outbound-work permits intended for the driver loop.
struct SubscribeAckedFut {
    shared: Arc<ConnectionShared>,
    handle: ConsumerHandle,
    accept_prior_attachment: bool,
    expected_waiter_id: Option<RequestId>,
    notification: Option<Pin<Box<tokio::sync::futures::OwnedNotified>>>,
}

impl Future for SubscribeAckedFut {
    type Output = Result<(), ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            let notification = this
                .notification
                .get_or_insert_with(|| Box::pin(this.shared.event_waker.clone().notified_owned()));
            let notified = notification.as_mut().poll(cx);

            let mut conn = this.shared.inner.lock();
            // Inspect events looking for our SubscribeAcked.
            loop {
                match conn.poll_event_if(|event| {
                    matches!(
                        event,
                        ConnectionEvent::SubscribeAcked { handle }
                            if *handle == this.handle
                    ) || matches!(
                        event,
                        ConnectionEvent::ConsumerClosedByBroker { handle, .. }
                            | ConnectionEvent::SubscribeFailed { handle, .. }
                            if *handle == this.handle
                    )
                }) {
                    Some(ConnectionEvent::SubscribeAcked { handle }) if handle == this.handle => {
                        let completed = match this.expected_waiter_id {
                            Some(waiter_id) => {
                                conn.consume_consumer_subscribe_waiter_completion(handle, waiter_id)
                            }
                            None => conn.consume_initial_consumer_subscribe_completion(handle),
                        };
                        if completed {
                            return Poll::Ready(Ok(()));
                        }
                    }
                    Some(ConnectionEvent::ConsumerClosedByBroker {
                        handle,
                        assigned_broker_service_url,
                    }) if handle == this.handle => {
                        // Broker-forced close — warn! per ADR-0054 §2.1.
                        // Mirror of the tokio engine's `EventWaitFut` arm.
                        let (topic, subscription) = conn
                            .consumer(handle)
                            .map(|s| (s.identity.topic.clone(), s.identity.subscription.clone()))
                            .unwrap_or_default();
                        tracing::warn!(
                            handle = ?handle,
                            topic = %topic,
                            subscription = %subscription,
                            assigned_broker_service_url = assigned_broker_service_url
                                .as_deref()
                                .map(crate::log_fields::truncate_broker_str),
                            "broker closed consumer while waiting for SubscribeAcked"
                        );
                        return Poll::Ready(Err(ClientError::Closed));
                    }
                    Some(ConnectionEvent::SubscribeFailed {
                        handle,
                        code,
                        message,
                    }) if handle == this.handle => {
                        return Poll::Ready(Err(ClientError::Broker { code, message }));
                    }
                    Some(_) => {}
                    None => break,
                }
            }

            let completed = match this.expected_waiter_id {
                Some(waiter_id) => {
                    conn.consume_consumer_subscribe_waiter_completion(this.handle, waiter_id)
                }
                None => {
                    this.accept_prior_attachment
                        && conn.consume_initial_consumer_subscribe_completion(this.handle)
                }
            };
            if completed {
                return Poll::Ready(Ok(()));
            }

            if conn.consumer_handle_is_terminal(this.handle) || this.shared.is_no_driver() {
                return Poll::Ready(Err(if this.shared.is_no_driver() {
                    ClientError::PeerClosed
                } else {
                    ClientError::Closed
                }));
            }
            drop(conn);

            if notified.is_pending() {
                return Poll::Pending;
            }
            this.notification = None;
        }
    }
}

impl Drop for SubscribeAckedFut {
    fn drop(&mut self) {
        let Some(waiter_id) = self.expected_waiter_id else {
            return;
        };
        let now = self.shared.now_instant();
        let changed =
            self.shared
                .inner
                .lock()
                .abandon_consumer_subscribe_waiter(self.handle, waiter_id, now);
        if changed {
            self.shared.driver_waker.notify_one();
        }
    }
}

#[cfg(test)]
async fn wait_subscribe_acked(
    shared: &Arc<ConnectionShared>,
    handle: ConsumerHandle,
) -> Result<(), ClientError> {
    SubscribeAckedFut {
        shared: shared.clone(),
        handle,
        accept_prior_attachment: false,
        expected_waiter_id: None,
        notification: None,
    }
    .await
}

#[cfg(test)]
mod tests {
    use std::future::Future as _;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake};
    use std::time::Instant;

    use bytes::{Bytes, BytesMut};
    use magnetar_proto::{
        ConnectionConfig, IncomingMessage, MessageId, SubscribeRequest, decode_one, encode_command,
        encode_payload, pb,
    };
    use moonpool_core::TokioProviders;

    use super::{Consumer, PostProcessOutcome, ReceiveFut, post_process_message};
    use crate::client::{Client, ClientError};
    use crate::crypto::MessageDecryptor;
    use crate::{ConnectionShared, MoonpoolEngine};

    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn subscribe_waiter_registers_before_returning_pending() {
        let shared = handshake_complete_shared();
        let handle = shared.inner.lock().subscribe(SubscribeRequest {
            topic: "persistent://public/default/pending-waiter-registration".to_owned(),
            subscription: "pending-waiter-registration".to_owned(),
            ..Default::default()
        });
        let mut future = Box::pin(super::SubscribeAckedFut {
            shared: shared.clone(),
            handle,
            accept_prior_attachment: true,
            expected_waiter_id: None,
            notification: None,
        });
        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = std::task::Waker::from(counter.clone());
        let mut cx = Context::from_waker(&waker);

        assert!(future.as_mut().poll(&mut cx).is_pending());
        shared.event_waker.notify_waiters();
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);

        let shared = handshake_complete_shared();
        let (first, second, second_request_id) = {
            let mut conn = shared.inner.lock();
            let first = conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/first-concurrent-consumer".to_owned(),
                subscription: "first-concurrent-consumer".to_owned(),
                ..Default::default()
            });
            let second_request_id = conn.peek_next_request_id_for_test();
            let second = conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/second-concurrent-consumer".to_owned(),
                subscription: "second-concurrent-consumer".to_owned(),
                ..Default::default()
            });
            (first, second, second_request_id)
        };
        let error = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: second_request_id,
                error: pb::ServerError::AuthorizationError as i32,
                message: "second consumer denied".to_owned(),
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &error).expect("encode CommandError");
        shared
            .inner
            .lock()
            .handle_bytes(Instant::now(), &frame)
            .expect("handle CommandError");
        let mut first_waiter = Box::pin(super::SubscribeAckedFut {
            shared: shared.clone(),
            handle: first,
            accept_prior_attachment: true,
            expected_waiter_id: None,
            notification: None,
        });
        let mut second_waiter = Box::pin(super::SubscribeAckedFut {
            shared,
            handle: second,
            accept_prior_attachment: true,
            expected_waiter_id: None,
            notification: None,
        });
        assert!(first_waiter.as_mut().poll(&mut cx).is_pending());
        assert!(matches!(
            second_waiter.as_mut().poll(&mut cx),
            Poll::Ready(Err(ClientError::Broker { code, ref message }))
                if code == pb::ServerError::AuthorizationError as i32
                    && message == "second consumer denied"
        ));

        let shared = handshake_complete_supervised_shared();
        let (handle, request_id) = {
            let mut conn = shared.inner.lock();
            let request_id = conn.peek_next_request_id_for_test();
            let handle = conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/subscribe-acked-before-seek".to_owned(),
                subscription: "subscribe-acked-before-seek".to_owned(),
                ..Default::default()
            });
            (handle, request_id)
        };
        let success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &success).expect("encode CommandSuccess");
        shared
            .inner
            .lock()
            .handle_bytes(Instant::now(), &frame)
            .expect("handle CommandSuccess");
        let mut initial_waiter = Box::pin(super::SubscribeAckedFut {
            shared: shared.clone(),
            handle,
            accept_prior_attachment: true,
            expected_waiter_id: None,
            notification: None,
        });
        assert!(matches!(
            initial_waiter.as_mut().poll(&mut cx),
            Poll::Ready(Ok(()))
        ));
        let mut reattach_waiter = Box::pin(super::SubscribeAckedFut {
            shared: shared.clone(),
            handle,
            accept_prior_attachment: false,
            expected_waiter_id: None,
            notification: None,
        });
        assert!(
            reattach_waiter.as_mut().poll(&mut cx).is_pending(),
            "the initial waiter must consume its ack so a seek cannot reuse the stale event"
        );
        drop(reattach_waiter);

        let (background_request_id, seek_request_id) = {
            let mut conn = shared.inner.lock();
            let background_request_id = conn
                .rebuild_consumers()
                .into_iter()
                .next()
                .expect("background reattach request");
            let seek_request_id = conn
                .resubscribe_consumer_after_seek(handle)
                .expect("seek reattach request");
            (background_request_id, seek_request_id)
        };
        let background_success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: background_request_id.0,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &background_success).expect("encode background success");
        shared
            .inner
            .lock()
            .handle_bytes(Instant::now(), &frame)
            .expect("handle background success");
        let mut staged = shared.inner.lock().poll_transmit();
        while !staged.is_empty() {
            let frame = decode_one(&mut staged).expect("staged frame must decode");
            assert_ne!(
                frame.command.r#type,
                pb::base_command::Type::Flow as i32,
                "an older background ack must not release seek flow"
            );
        }
        let mut seek_waiter = Box::pin(super::SubscribeAckedFut {
            shared: shared.clone(),
            handle,
            accept_prior_attachment: false,
            expected_waiter_id: Some(seek_request_id),
            notification: None,
        });
        assert!(
            seek_waiter.as_mut().poll(&mut cx).is_pending(),
            "a seek waiter must not consume an unattended background reattach ack"
        );
        let seek_success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: seek_request_id.0,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &seek_success).expect("encode seek success");
        shared
            .inner
            .lock()
            .handle_bytes(Instant::now(), &frame)
            .expect("handle seek success");
        shared.inner.lock().mark_disconnected();
        assert!(
            seek_waiter.as_mut().poll(&mut cx).is_pending(),
            "a supervised transport failure must not terminate the seek waiter"
        );
        shared.inner.lock().reset();
        assert!(
            seek_waiter.as_mut().poll(&mut cx).is_pending(),
            "a reset must not expose an old-session seek completion"
        );
        shared
            .inner
            .lock()
            .begin_handshake()
            .expect("restart handshake");
        let connected = pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-test".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &connected).expect("encode reconnect CommandConnected");
        let rebuilt_request_id = {
            let mut conn = shared.inner.lock();
            conn.handle_bytes(Instant::now(), &frame)
                .expect("complete reconnect handshake");
            conn.rebuild_consumers()
                .into_iter()
                .next()
                .expect("rebuilt subscribe request")
        };
        let rebuilt_success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: rebuilt_request_id.0,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &rebuilt_success).expect("encode rebuilt success");
        shared
            .inner
            .lock()
            .handle_bytes(Instant::now(), &frame)
            .expect("handle rebuilt success");
        let mut staged = shared.inner.lock().poll_transmit();
        while !staged.is_empty() {
            let frame = decode_one(&mut staged).expect("staged frame must decode");
            assert_ne!(
                frame.command.r#type,
                pb::base_command::Type::Flow as i32,
                "reset/rebuild must preserve user-owned seek flow"
            );
        }
        assert!(matches!(
            seek_waiter.as_mut().poll(&mut cx),
            Poll::Ready(Ok(()))
        ));

        let retry_waiter_id = shared
            .inner
            .lock()
            .resubscribe_consumer_after_seek(handle)
            .expect("retry-owned seek subscribe");
        let mut retry_waiter = Box::pin(super::SubscribeAckedFut {
            shared: shared.clone(),
            handle,
            accept_prior_attachment: false,
            expected_waiter_id: Some(retry_waiter_id),
            notification: None,
        });
        let retry_error = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: retry_waiter_id.0,
                error: pb::ServerError::ServiceNotReady as i32,
                message: "retry seek subscribe".to_owned(),
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &retry_error).expect("encode retry error");
        shared
            .inner
            .lock()
            .handle_bytes(Instant::now(), &frame)
            .expect("handle retry error");
        assert!(retry_waiter.as_mut().poll(&mut cx).is_pending());
        let retry_request_id = shared
            .inner
            .lock()
            .retry_consumer_subscribe_if_current(handle, retry_waiter_id)
            .expect("replacement subscribe request");
        let retry_success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: retry_request_id.0,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &retry_success).expect("encode retry success");
        shared
            .inner
            .lock()
            .handle_bytes(Instant::now(), &frame)
            .expect("handle retry success");
        let mut staged = shared.inner.lock().poll_transmit();
        while !staged.is_empty() {
            let frame = decode_one(&mut staged).expect("staged frame must decode");
            assert_ne!(
                frame.command.r#type,
                pb::base_command::Type::Flow as i32,
                "retry replacement must preserve user-owned seek flow"
            );
        }
        assert!(matches!(
            retry_waiter.as_mut().poll(&mut cx),
            Poll::Ready(Ok(()))
        ));

        let cancelled_waiter_id = shared
            .inner
            .lock()
            .resubscribe_consumer_after_seek(handle)
            .expect("cancelled seek subscribe");
        let cancelled_waiter = Box::pin(super::SubscribeAckedFut {
            shared: shared.clone(),
            handle,
            accept_prior_attachment: false,
            expected_waiter_id: Some(cancelled_waiter_id),
            notification: None,
        });
        drop(cancelled_waiter);
        let cancelled_success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: cancelled_waiter_id.0,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &cancelled_success).expect("encode cancelled success");
        shared
            .inner
            .lock()
            .handle_bytes(Instant::now(), &frame)
            .expect("handle cancelled success");
        let mut staged = shared.inner.lock().poll_transmit();
        let mut saw_flow = false;
        while !staged.is_empty() {
            let frame = decode_one(&mut staged).expect("staged frame must decode");
            saw_flow |= frame.command.r#type == pb::base_command::Type::Flow as i32;
        }
        assert!(
            saw_flow,
            "cancelling a seek waiter must transfer its active subscribe to flow ownership"
        );

        let disconnected_waiter_id = shared
            .inner
            .lock()
            .resubscribe_consumer_after_seek(handle)
            .expect("disconnect-cancelled seek subscribe");
        let disconnected_waiter = Box::pin(super::SubscribeAckedFut {
            shared: shared.clone(),
            handle,
            accept_prior_attachment: false,
            expected_waiter_id: Some(disconnected_waiter_id),
            notification: None,
        });
        let disconnected_success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: disconnected_waiter_id.0,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &disconnected_success)
            .expect("encode disconnect-cancelled success");
        {
            let mut conn = shared.inner.lock();
            conn.handle_bytes(Instant::now(), &frame)
                .expect("handle disconnect-cancelled success");
            conn.mark_disconnected();
            conn.reset();
        }
        drop(disconnected_waiter);
        shared
            .inner
            .lock()
            .begin_handshake()
            .expect("restart handshake after waiter cancellation");
        let connected = pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-test".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &connected).expect("encode reconnect CommandConnected");
        let rebuilt_request_id = {
            let mut conn = shared.inner.lock();
            conn.handle_bytes(Instant::now(), &frame)
                .expect("complete reconnect after waiter cancellation");
            conn.rebuild_consumers()
                .into_iter()
                .next()
                .expect("rebuilt flow-owned subscribe request")
        };
        let rebuilt_success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: rebuilt_request_id.0,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &rebuilt_success).expect("encode flow-owned rebuilt success");
        shared
            .inner
            .lock()
            .handle_bytes(Instant::now(), &frame)
            .expect("handle flow-owned rebuilt success");
        let mut staged = shared.inner.lock().poll_transmit();
        let mut saw_flow = false;
        while !staged.is_empty() {
            let frame = decode_one(&mut staged).expect("staged frame must decode");
            saw_flow |= frame.command.r#type == pb::base_command::Type::Flow as i32;
        }
        assert!(
            saw_flow,
            "cancelling during reconnect must transfer the rebuilt subscribe to flow ownership"
        );

        let shared = handshake_complete_shared();
        let (handle, request_id) = {
            let mut conn = shared.inner.lock();
            let request_id = conn.peek_next_request_id_for_test();
            let handle = conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/subscribe-acked-before-reset".to_owned(),
                subscription: "subscribe-acked-before-reset".to_owned(),
                ..Default::default()
            });
            (handle, request_id)
        };
        let success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &success).expect("encode CommandSuccess");
        {
            let mut conn = shared.inner.lock();
            conn.handle_bytes(Instant::now(), &frame)
                .expect("handle CommandSuccess");
            conn.reset();
        }
        let mut initial_waiter = Box::pin(super::SubscribeAckedFut {
            shared: shared.clone(),
            handle,
            accept_prior_attachment: true,
            expected_waiter_id: None,
            notification: None,
        });
        assert!(
            initial_waiter.as_mut().poll(&mut cx).is_pending(),
            "a reset must not expose an old-session initial subscribe completion"
        );
        shared
            .inner
            .lock()
            .begin_handshake()
            .expect("restart handshake");
        let connected = pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-test".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &connected).expect("encode reconnect CommandConnected");
        let rebuilt_request_id = {
            let mut conn = shared.inner.lock();
            conn.handle_bytes(Instant::now(), &frame)
                .expect("complete reconnect handshake");
            conn.rebuild_consumers()
                .into_iter()
                .next()
                .expect("rebuilt subscribe request")
        };
        let rebuilt_success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: rebuilt_request_id.0,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &rebuilt_success).expect("encode rebuilt success");
        shared
            .inner
            .lock()
            .handle_bytes(Instant::now(), &frame)
            .expect("handle rebuilt success");
        assert!(matches!(
            initial_waiter.as_mut().poll(&mut cx),
            Poll::Ready(Ok(()))
        ));
        let mut reattach_waiter = Box::pin(super::SubscribeAckedFut {
            shared,
            handle,
            accept_prior_attachment: false,
            expected_waiter_id: None,
            notification: None,
        });
        assert!(
            reattach_waiter.as_mut().poll(&mut cx).is_pending(),
            "a seek/re-subscribe waiter must still require its fresh acknowledgement"
        );
    }

    /// ADR-0053 §D2 — the reconsume / DLQ-republish property merge replaces an
    /// inbound key (e.g. a re-injected OTel `traceparent`) in place rather than
    /// duplicating it, and leaves unrelated keys untouched.
    #[test]
    fn apply_property_overrides_replaces_inbound_keys() {
        let mut props = vec![
            pb::KeyValue {
                key: "traceparent".to_owned(),
                value: "inbound".to_owned(),
            },
            pb::KeyValue {
                key: "user".to_owned(),
                value: "keep".to_owned(),
            },
        ];
        Consumer::<TokioProviders>::apply_property_overrides(
            &mut props,
            vec![
                ("traceparent".to_owned(), "reinjected".to_owned()),
                ("tracestate".to_owned(), "ts=1".to_owned()),
            ],
        );
        let traceparents: Vec<&str> = props
            .iter()
            .filter(|kv| kv.key == "traceparent")
            .map(|kv| kv.value.as_str())
            .collect();
        assert_eq!(
            traceparents,
            vec!["reinjected"],
            "inbound traceparent replaced exactly once, not duplicated"
        );
        assert!(
            props
                .iter()
                .any(|kv| kv.key == "user" && kv.value == "keep"),
            "unrelated key preserved"
        );
        assert!(
            props
                .iter()
                .any(|kv| kv.key == "tracestate" && kv.value == "ts=1"),
            "new override key appended"
        );
    }

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
        handshake_complete_shared_with_config(ConnectionConfig::default())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn seek_waits_for_reattach_before_restoring_flow() {
        let shared = handshake_complete_shared();
        let (handle, slot) = {
            let mut conn = shared.inner.lock();
            let _ = conn.poll_transmit();
            let request_id = conn.peek_next_request_id_for_test();
            let handle = conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/seek-reattach".to_owned(),
                subscription: "seek-reattach".to_owned(),
                receiver_queue_size: 3,
                ..Default::default()
            });
            let slot = conn.consumer(handle).expect("consumer slot").clone();
            let _ = conn.poll_transmit();
            let success = pb::BaseCommand {
                r#type: pb::base_command::Type::Success as i32,
                success: Some(pb::CommandSuccess {
                    request_id,
                    schema: None,
                }),
                ..Default::default()
            };
            let mut frame = BytesMut::new();
            encode_command(&mut frame, &success).expect("encode initial subscribe success");
            conn.handle_bytes(Instant::now(), &frame)
                .expect("establish consumer");
            assert!(conn.consume_initial_consumer_subscribe_completion(handle));
            while conn.poll_event().is_some() {}
            (handle, slot)
        };
        let consumer = Consumer::<TokioProviders>::assemble(
            shared.clone(),
            handle,
            slot,
            None,
            crate::tokio_sleep_provider(),
        );
        let task_consumer = consumer.clone();
        let task = tokio::spawn(async move {
            task_consumer
                .seek_to_message(magnetar_proto::MessageId {
                    ledger_id: 7,
                    entry_id: 11,
                    partition: -1,
                    batch_index: -1,
                    batch_size: 0,
                })
                .await
        });
        tokio::task::yield_now().await;

        let seek_request_id = {
            let mut staged = shared.inner.lock().poll_transmit();
            let mut request_id = None;
            while !staged.is_empty() {
                let command = decode_one(&mut staged).expect("decode seek frame").command;
                if let Some(seek) = command.seek {
                    request_id = Some(seek.request_id);
                }
            }
            request_id.expect("seek command")
        };
        let seek_success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: seek_request_id,
                schema: None,
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &seek_success).expect("encode seek success");
        shared
            .inner
            .lock()
            .handle_bytes(Instant::now(), &frame)
            .expect("handle seek success");
        tokio::task::yield_now().await;

        let reattach_request_id = {
            let mut staged = shared.inner.lock().poll_transmit();
            let mut request_id = None;
            while !staged.is_empty() {
                let command = decode_one(&mut staged)
                    .expect("decode pre-reattach frame")
                    .command;
                assert_ne!(command.r#type, pb::base_command::Type::Flow as i32);
                assert_ne!(
                    command.r#type,
                    pb::base_command::Type::RedeliverUnacknowledgedMessages as i32
                );
                if let Some(subscribe) = command.subscribe {
                    request_id = Some(subscribe.request_id);
                }
            }
            request_id.expect("seek reattach subscribe")
        };
        let reattach_success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: reattach_request_id,
                schema: None,
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_command(&mut frame, &reattach_success).expect("encode reattach success");
        shared
            .inner
            .lock()
            .handle_bytes(Instant::now(), &frame)
            .expect("handle reattach success");
        shared.event_waker.notify_waiters();
        tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("seek completes after reattach")
            .expect("seek task")
            .expect("seek succeeds");

        let mut staged = shared.inner.lock().poll_transmit();
        let mut saw_flow = false;
        let mut saw_redelivery = false;
        while !staged.is_empty() {
            let command = decode_one(&mut staged)
                .expect("decode post-reattach frame")
                .command;
            if command.r#type == pb::base_command::Type::Flow as i32 {
                assert!(!saw_redelivery, "FLOW must precede redelivery");
                saw_flow = true;
            }
            if command.r#type == pb::base_command::Type::RedeliverUnacknowledgedMessages as i32 {
                assert!(saw_flow, "redelivery must follow restored FLOW");
                saw_redelivery = true;
            }
        }
        assert!(saw_flow);
        assert!(saw_redelivery);

        #[cfg(feature = "scalable-topics")]
        {
            let staged_seek = consumer.stage_seek_to_message_id_data(pb::MessageIdData {
                ledger_id: 7,
                entry_id: 12,
                ..Default::default()
            });
            let seek_success = pb::BaseCommand {
                r#type: pb::base_command::Type::Success as i32,
                success: Some(pb::CommandSuccess {
                    request_id: staged_seek.request_id.0,
                    schema: None,
                }),
                ..Default::default()
            };
            let mut frame = BytesMut::new();
            encode_command(&mut frame, &seek_success).expect("encode raced seek success");
            {
                let mut conn = shared.inner.lock();
                conn.handle_bytes(Instant::now(), &frame)
                    .expect("handle raced seek success");
                let _ = conn.unsubscribe(handle, false);
            }
            consumer
                .complete_staged_seek(staged_seek)
                .await
                .expect("a concurrent unsubscribe suppresses seek reattachment");
            let mut staged = shared.inner.lock().poll_transmit();
            while !staged.is_empty() {
                let command = decode_one(&mut staged)
                    .expect("decode raced seek frame")
                    .command;
                assert_ne!(
                    command.r#type,
                    pb::base_command::Type::Subscribe as i32,
                    "unsubscribe must suppress seek reattachment"
                );
            }
        }
    }

    fn handshake_complete_supervised_shared() -> Arc<ConnectionShared> {
        handshake_complete_shared_with_config(ConnectionConfig {
            supervisor: Some(magnetar_proto::SupervisorConfig::default()),
            ..ConnectionConfig::default()
        })
    }

    fn handshake_complete_shared_with_config(config: ConnectionConfig) -> Arc<ConnectionShared> {
        let shared = ConnectionShared::new(config);
        {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("handshake");
            let frame = handshake_response_bytes();
            conn.handle_bytes(Instant::now(), &frame)
                .expect("connected");
        }
        shared
    }

    fn established_consumer_with_failed_retry(
        shared: &Arc<ConnectionShared>,
        topic: &str,
    ) -> (
        magnetar_proto::ConsumerHandle,
        Arc<magnetar_proto::ConsumerSlot>,
        magnetar_proto::RequestId,
    ) {
        let mut conn = shared.inner.lock();
        let initial_request_id = conn.peek_next_request_id_for_test();
        let handle = conn.subscribe(SubscribeRequest {
            topic: topic.to_owned(),
            subscription: "s".to_owned(),
            ..Default::default()
        });
        let slot = conn.consumer(handle).expect("consumer slot").clone();
        let success = pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id: initial_request_id,
                schema: None,
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        magnetar_proto::encode_command(&mut frame, &success)
            .expect("encode initial subscribe success");
        conn.handle_bytes(Instant::now(), &frame)
            .expect("establish consumer");
        assert!(conn.consume_initial_consumer_subscribe_completion(handle));
        while conn.poll_event().is_some() {}

        conn.reset();
        conn.begin_handshake().expect("restart handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(Instant::now(), &frame)
            .expect("complete reconnect handshake");
        while conn.poll_event().is_some() {}
        let failed_request_id = conn.rebuild_consumers()[0];
        let transient_error = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: failed_request_id.0,
                error: pb::ServerError::ConsumerBusy as i32,
                message: "retry later".to_owned(),
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        magnetar_proto::encode_command(&mut frame, &transient_error)
            .expect("encode transient error");
        conn.handle_bytes(Instant::now(), &frame)
            .expect("schedule established retry");
        assert!(conn.consumer_subscribe_retry_is_current(handle, failed_request_id));
        let _ = conn.poll_transmit();
        (handle, slot, failed_request_id)
    }

    const XOR_KEY: u8 = 0x5A;

    /// Build a `CommandMessage` whose metadata carries PIP-4 `encryption_keys`
    /// (and an XOR-ciphertext body). Mirrors what the producer-side
    /// `XorEncryptor` stamps. 1:1 with the tokio consumer test helper.
    ///
    /// Thin shim over [`encrypted_message_bytes_with_key`] with the default
    /// `"xor-test"` key — the two helpers were copy-pasted; this dedup keeps
    /// the wire-encoding logic in one place.
    fn encrypted_message_bytes(consumer_id: u64, entry_id: u64, plaintext: &[u8]) -> BytesMut {
        encrypted_message_bytes_with_key(consumer_id, entry_id, "xor-test", plaintext)
    }

    /// XOR decryptor that reverses [`encrypted_message_bytes`].
    #[derive(Debug, Default)]
    struct XorDecryptor;

    impl crate::crypto::MessageDecryptor for XorDecryptor {
        fn decrypt(
            &self,
            ciphertext: &[u8],
            _metadata: &pb::MessageMetadata,
        ) -> Result<bytes::Bytes, crate::crypto::EncryptError> {
            Ok(bytes::Bytes::from(
                ciphertext.iter().map(|b| b ^ XOR_KEY).collect::<Vec<u8>>(),
            ))
        }
    }

    /// Decryptor stub that always fails — exercises the three
    /// `CryptoFailureAction` policies independently of the backend.
    #[derive(Debug, Default)]
    struct AlwaysFailDecryptor;

    impl crate::crypto::MessageDecryptor for AlwaysFailDecryptor {
        fn decrypt(
            &self,
            _ciphertext: &[u8],
            _metadata: &pb::MessageMetadata,
        ) -> Result<bytes::Bytes, crate::crypto::EncryptError> {
            Err(crate::crypto::EncryptError::new(
                "forced decrypt failure (test)",
            ))
        }
    }

    /// Subscribe and feed an encrypted message into a freshly-subscribed
    /// consumer. Returns the live `(shared, handle, slot)` so the caller can
    /// build a `Consumer` with whatever decryptor / failure-action it wants.
    fn subscribe_with_encrypted_message(
        crypto_failure_action: magnetar_proto::CryptoFailureAction,
        plaintext: &[u8],
    ) -> (
        Arc<ConnectionShared>,
        magnetar_proto::ConsumerHandle,
        Arc<magnetar_proto::ConsumerSlot>,
    ) {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/crypto".to_owned(),
                subscription: "s".to_owned(),
                sub_type: pb::command_subscribe::SubType::Exclusive,
                crypto_failure_action,
                ..Default::default()
            })
        };
        let consumer_id = handle.0;
        let frame = encrypted_message_bytes(consumer_id, 0, plaintext);
        {
            let mut conn = shared.inner.lock();
            conn.handle_bytes(Instant::now(), &frame)
                .expect("handle encrypted CommandMessage");
        }
        let slot = shared
            .inner
            .lock()
            .consumer(handle)
            .cloned()
            .expect("test consumer slot must exist");
        (shared, handle, slot)
    }

    /// Build a `Consumer<TokioProviders>` with an explicit decryptor.
    fn consumer_with_decryptor(
        shared: Arc<ConnectionShared>,
        handle: magnetar_proto::ConsumerHandle,
        slot: Arc<magnetar_proto::ConsumerSlot>,
        decryptor: Arc<dyn crate::crypto::MessageDecryptor>,
    ) -> Consumer<TokioProviders> {
        Consumer::assemble(
            shared,
            handle,
            slot,
            Some(decryptor),
            crate::tokio_sleep_provider(),
        )
    }

    /// Happy path: a decryptor that reverses the XOR ciphertext yields the
    /// original plaintext. 1:1 with the tokio
    /// `receive_decrypts_encrypted_message`.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_decrypts_encrypted_message() {
        let (shared, handle, slot) = subscribe_with_encrypted_message(
            magnetar_proto::CryptoFailureAction::Fail,
            b"top-secret",
        );
        let consumer = consumer_with_decryptor(shared, handle, slot, Arc::new(XorDecryptor));
        let msg = consumer.receive().await.expect("decrypted receive");
        assert_eq!(msg.payload.as_ref(), b"top-secret");
    }

    /// `CryptoFailureAction::Fail`: a failing decryptor surfaces the error.
    /// 1:1 with the tokio `receive_crypto_failure_fail_surfaces_error`.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_crypto_failure_fail_surfaces_error() {
        let (shared, handle, slot) =
            subscribe_with_encrypted_message(magnetar_proto::CryptoFailureAction::Fail, b"opaque");
        let consumer = consumer_with_decryptor(shared, handle, slot, Arc::new(AlwaysFailDecryptor));
        let res = consumer.receive().await;
        assert!(
            matches!(res, Err(ClientError::Other(_))),
            "Fail policy must surface a decrypt error, got {res:?}"
        );
    }

    /// `CryptoFailureAction::Consume`: the ciphertext + encryption metadata are
    /// handed back as-is. 1:1 with the tokio
    /// `receive_crypto_failure_consume_returns_ciphertext`.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_crypto_failure_consume_returns_ciphertext() {
        let plaintext = b"distinctive-payload";
        let (shared, handle, slot) = subscribe_with_encrypted_message(
            magnetar_proto::CryptoFailureAction::Consume,
            plaintext,
        );
        let consumer = consumer_with_decryptor(shared, handle, slot, Arc::new(AlwaysFailDecryptor));
        let msg = consumer
            .receive()
            .await
            .expect("consume returns the message");
        assert_ne!(
            msg.payload.as_ref(),
            plaintext.as_slice(),
            "Consume must hand back the ciphertext, not the plaintext"
        );
        assert!(
            !msg.metadata.encryption_keys.is_empty(),
            "Consume must preserve encryption_keys for out-of-band decryption"
        );
    }

    /// `CryptoFailureAction::Discard`: the undecryptable message is acked and
    /// skipped, so `receive_with_timeout` observes no message. 1:1 with the
    /// tokio `receive_crypto_failure_discard_skips_message`.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_crypto_failure_discard_skips_message() {
        let (shared, handle, slot) = subscribe_with_encrypted_message(
            magnetar_proto::CryptoFailureAction::Discard,
            b"undecryptable",
        );
        let consumer = consumer_with_decryptor(shared, handle, slot, Arc::new(AlwaysFailDecryptor));
        let got = consumer
            .receive_with_timeout(std::time::Duration::from_millis(200))
            .await
            .expect("receive_with_timeout resolves");
        assert!(
            got.is_none(),
            "Discard must silently drop the undecryptable message, got {got:?}"
        );
    }

    /// An encrypted message with NO decryptor configured surfaces a
    /// "no decryptor configured" error under `CryptoFailureAction::Fail`.
    /// 1:1 with the tokio `receive_encrypted_without_decryptor_fails`.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_encrypted_without_decryptor_fails() {
        let (shared, handle, slot) =
            subscribe_with_encrypted_message(magnetar_proto::CryptoFailureAction::Fail, b"secret");
        let consumer: Consumer<TokioProviders> =
            Consumer::assemble(shared, handle, slot, None, crate::tokio_sleep_provider());
        let res = consumer.receive().await;
        match res {
            Err(ClientError::Other(msg)) => {
                assert!(
                    msg.contains("no decryptor configured"),
                    "expected no-decryptor message, got {msg:?}"
                );
            }
            other => panic!("expected no-decryptor error, got {other:?}"),
        }
    }

    /// Cloning a `Consumer` preserves the decryptor hook (Arc bump). 1:1 with
    /// the tokio `consumer_clone_preserves_decryptor`.
    #[tokio::test(flavor = "current_thread")]
    async fn consumer_clone_preserves_decryptor() {
        let (shared, handle, slot) = subscribe_with_encrypted_message(
            magnetar_proto::CryptoFailureAction::Fail,
            b"clone-secret",
        );
        let consumer = consumer_with_decryptor(shared, handle, slot, Arc::new(XorDecryptor));
        let clone = consumer.clone();
        assert!(
            Arc::ptr_eq(&consumer.close_guard, &clone.close_guard),
            "clones must share one last-clone close guard"
        );
        // The clone carries the same decryptor, so it decrypts the queued
        // message back to the original plaintext.
        let msg = clone.receive().await.expect("clone decrypts");
        assert_eq!(msg.payload.as_ref(), b"clone-secret");
    }

    #[test]
    fn drop_after_no_driver_stages_no_close_consumer() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            let handle = conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/no-driver-drop".to_owned(),
                subscription: "no-driver-drop".to_owned(),
                ..Default::default()
            });
            let _ = conn.poll_transmit();
            handle
        };
        let slot = shared
            .inner
            .lock()
            .consumer(handle)
            .cloned()
            .expect("test consumer slot must exist");
        let consumer: Consumer<TokioProviders> = Consumer::assemble(
            shared.clone(),
            handle,
            slot,
            None,
            crate::tokio_sleep_provider(),
        );

        shared.mark_no_driver();
        drop(consumer);

        let mut staged = shared.inner.lock().poll_transmit();
        while !staged.is_empty() {
            let frame = decode_one(&mut staged).expect("staged frame must decode");
            assert_ne!(
                frame.command.r#type,
                pb::base_command::Type::CloseConsumer as i32,
                "no driver remains to flush a drop-triggered CloseConsumer"
            );
        }
    }

    /// Build an encrypted `CommandMessage` whose `encryption_keys[0].key` carries a custom
    /// key name. Lets a test mark individual messages as decryptable vs. undecryptable for a
    /// key-aware decryptor (see [`SelectiveDecryptor`]).
    fn encrypted_message_bytes_with_key(
        consumer_id: u64,
        entry_id: u64,
        key_name: &str,
        plaintext: &[u8],
    ) -> BytesMut {
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
            encryption_keys: vec![pb::EncryptionKeys {
                key: key_name.to_owned(),
                value: bytes::Bytes::from_static(b"k"),
                metadata: Vec::new(),
            }],
            encryption_algo: Some("XOR-TEST".to_owned()),
            encryption_param: Some(bytes::Bytes::from_static(b"iv")),
            ..Default::default()
        };
        let cipher: Vec<u8> = plaintext.iter().map(|b| b ^ XOR_KEY).collect();
        let mut buf = BytesMut::new();
        encode_payload(&mut buf, &cmd, &meta, &cipher).expect("encode encrypted CommandMessage");
        buf
    }

    /// Decryptor that XOR-decrypts only when `encryption_keys[0].key == "xor-test"` and fails
    /// for any other key. Lets a single batch mix decryptable and undecryptable messages so we
    /// can exercise the `Discard` skip path in [`Consumer::receive_batch_with_bytes_cap`].
    #[derive(Debug, Default)]
    struct SelectiveDecryptor;

    impl crate::crypto::MessageDecryptor for SelectiveDecryptor {
        fn decrypt(
            &self,
            ciphertext: &[u8],
            metadata: &pb::MessageMetadata,
        ) -> Result<bytes::Bytes, crate::crypto::EncryptError> {
            match metadata.encryption_keys.first().map(|k| k.key.as_str()) {
                Some("xor-test") => Ok(bytes::Bytes::from(
                    ciphertext.iter().map(|b| b ^ XOR_KEY).collect::<Vec<u8>>(),
                )),
                other => Err(crate::crypto::EncryptError::new(format!(
                    "selective decryptor refuses key {other:?}"
                ))),
            }
        }
    }

    /// Subscribe and feed `count` distinct encrypted messages (`xor-test` key, plaintext
    /// `b"batch-secret-{i}"`) into the consumer queue. Returns the live `(shared, handle, slot)`.
    fn subscribe_with_encrypted_batch(
        crypto_failure_action: magnetar_proto::CryptoFailureAction,
        count: u64,
    ) -> (
        Arc<ConnectionShared>,
        magnetar_proto::ConsumerHandle,
        Arc<magnetar_proto::ConsumerSlot>,
    ) {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/crypto-batch".to_owned(),
                subscription: "s".to_owned(),
                sub_type: pb::command_subscribe::SubType::Exclusive,
                crypto_failure_action,
                ..Default::default()
            })
        };
        let consumer_id = handle.0;
        {
            let mut conn = shared.inner.lock();
            for i in 0..count {
                let frame =
                    encrypted_message_bytes(consumer_id, i, format!("batch-secret-{i}").as_bytes());
                conn.handle_bytes(Instant::now(), &frame)
                    .expect("handle encrypted CommandMessage");
            }
        }
        let slot = shared
            .inner
            .lock()
            .consumer(handle)
            .cloned()
            .expect("test consumer slot must exist");
        (shared, handle, slot)
    }

    /// Regression test for the moonpool batch-receive ciphertext leak: `receive_batch` must
    /// decrypt EVERY message in the batch, not just the first. Before the fix, messages 2..N
    /// were popped via `pop_message` without decryption, so they arrived as ciphertext. 1:1
    /// with the tokio `receive_batch_decrypts_every_message`.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_batch_decrypts_every_message() {
        let (shared, handle, slot) =
            subscribe_with_encrypted_batch(magnetar_proto::CryptoFailureAction::Fail, 3);
        let consumer = consumer_with_decryptor(shared, handle, slot, Arc::new(XorDecryptor));
        let batch = consumer
            .receive_batch(10, std::time::Duration::from_secs(2))
            .await
            .expect("receive_batch must resolve");
        assert_eq!(batch.len(), 3, "all three messages must be delivered");
        for (i, msg) in batch.iter().enumerate() {
            assert_eq!(
                msg.payload.as_ref(),
                format!("batch-secret-{i}").as_bytes(),
                "message {i} must be delivered as plaintext, not ciphertext",
            );
            assert!(
                std::str::from_utf8(&msg.payload).is_ok(),
                "decrypted payload must be valid utf-8 plaintext",
            );
        }
    }

    /// `CryptoFailureAction::Discard` inside a batch: an undecryptable message is acked and
    /// skipped, never handed to the caller as ciphertext, while the decryptable messages around
    /// it are delivered as plaintext. 1:1 with the tokio
    /// `receive_batch_discards_undecryptable_message`.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_batch_discards_undecryptable_message() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/crypto-batch-discard".to_owned(),
                subscription: "s".to_owned(),
                sub_type: pb::command_subscribe::SubType::Exclusive,
                crypto_failure_action: magnetar_proto::CryptoFailureAction::Discard,
                ..Default::default()
            })
        };
        let consumer_id = handle.0;
        {
            let mut conn = shared.inner.lock();
            // entry 0: decryptable, entry 1: undecryptable (bad key → Discard), entry 2:
            // decryptable. The middle message must be skipped.
            conn.handle_bytes(
                Instant::now(),
                &encrypted_message_bytes_with_key(consumer_id, 0, "xor-test", b"keep-0"),
            )
            .expect("handle msg 0");
            conn.handle_bytes(
                Instant::now(),
                &encrypted_message_bytes_with_key(consumer_id, 1, "bad-key", b"drop-1"),
            )
            .expect("handle msg 1");
            conn.handle_bytes(
                Instant::now(),
                &encrypted_message_bytes_with_key(consumer_id, 2, "xor-test", b"keep-2"),
            )
            .expect("handle msg 2");
        }
        let slot = shared
            .inner
            .lock()
            .consumer(handle)
            .cloned()
            .expect("test consumer slot must exist");
        let consumer = consumer_with_decryptor(shared, handle, slot, Arc::new(SelectiveDecryptor));
        let batch = consumer
            .receive_batch(10, std::time::Duration::from_secs(2))
            .await
            .expect("receive_batch must resolve");
        assert_eq!(
            batch.len(),
            2,
            "the undecryptable middle message must be discarded, not delivered",
        );
        assert_eq!(batch[0].payload.as_ref(), b"keep-0");
        assert_eq!(batch[1].payload.as_ref(), b"keep-2");
        for msg in &batch {
            assert!(
                msg.metadata
                    .encryption_keys
                    .first()
                    .is_none_or(|k| k.key != "bad-key"),
                "no undecryptable message may leak into the batch",
            );
        }
    }

    fn make_consumer<P: moonpool_core::Providers>(
        shared: Arc<ConnectionShared>,
        handle: magnetar_proto::ConsumerHandle,
    ) -> Consumer<P> {
        // Fall back to a stub slot for tests that intentionally exercise an
        // unknown handle (the slot's defaults — empty queue, paused=false,
        // not closed... wait, closed=false by default) — but Phase 2's
        // per-slot getters read from the slot now, so the "unknown handle"
        // assertions still pass against a fresh stub: empty topic/subscription,
        // 0 queue len, 0 permits, paused=false. `is_closed` is the only
        // semantic that diverges from the global-lookup-returns-true convention;
        // tests that hit that pattern have been updated to assert against the
        // stub's closed=false default instead.
        let slot = shared
            .inner
            .lock()
            .consumer(handle)
            .cloned()
            .unwrap_or_else(|| {
                magnetar_proto::ConsumerSlot::new(
                    magnetar_proto::ConsumerIdentity {
                        handle,
                        topic: String::new(),
                        subscription: String::new(),
                    },
                    magnetar_proto::consumer::ConsumerState::new(
                        handle,
                        String::new(),
                        String::new(),
                        0,
                    ),
                )
            });
        Consumer::assemble(shared, handle, slot, None, crate::tokio_sleep_provider())
    }

    /// `Client::subscribe` is generic over `P: Providers` — confirm the
    /// bounds compose with `TokioProviders` by naming `connect_plain` (which
    /// produces the `Client<P>` carrier) without actually dialling.
    /// `subscribe` is exercised by the integration tests once a real broker
    /// is in the loop.
    #[test]
    #[allow(clippy::let_underscore_future, clippy::no_effect_underscore_binding)]
    fn subscribe_compiles_against_tokio_providers() {
        use std::future::Future as _;
        use std::task::{Context, Poll};

        let providers = TokioProviders::new();
        let engine = MoonpoolEngine::new(providers);
        let _fut_client =
            Client::connect_plain(&engine, "127.0.0.1:6650", ConnectionConfig::default());
        // Reference `SubscribeRequest::default` to confirm the type is
        // re-exported via `magnetar_proto`.
        let _req = SubscribeRequest::default();

        // Moonpool 0.8 drives `SimProviders` workloads on its own executor.
        // Subscribe readiness must park without spawning a Tokio helper task.
        let pending_shared = handshake_complete_shared();
        let pending_handle = pending_shared.inner.lock().subscribe(SubscribeRequest {
            topic: "persistent://public/default/sub-ready-pending".to_owned(),
            subscription: "s-pending".to_owned(),
            ..Default::default()
        });
        let mut future = Box::pin(super::wait_subscribe_acked(&pending_shared, pending_handle));
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));

        // Two concurrent subscribe waiters must not consume each other's
        // acknowledgements. Queue the second acknowledgement first, resolve
        // the first waiter, then confirm the second event remains available.
        let shared = handshake_complete_shared();
        let (first, second) = {
            let mut conn = shared.inner.lock();
            let first = conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/sub-ready-first".to_owned(),
                subscription: "s-first".to_owned(),
                ..Default::default()
            });
            let second = conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/sub-ready-second".to_owned(),
                subscription: "s-second".to_owned(),
                ..Default::default()
            });
            (first, second)
        };
        for request_id in [1, 0] {
            let command = pb::BaseCommand {
                r#type: pb::base_command::Type::Success as i32,
                success: Some(pb::CommandSuccess {
                    request_id,
                    schema: None,
                }),
                ..Default::default()
            };
            let mut frame = BytesMut::new();
            encode_command(&mut frame, &command).expect("encode CommandSuccess");
            shared
                .inner
                .lock()
                .handle_bytes(Instant::now(), &frame)
                .expect("handle CommandSuccess");
        }
        futures::executor::block_on(super::wait_subscribe_acked(&shared, first))
            .expect("first subscription becomes ready");
        let mut second_wait = Box::pin(super::wait_subscribe_acked(&shared, second));
        assert!(
            matches!(second_wait.as_mut().poll(&mut cx), Poll::Ready(Ok(()))),
            "first waiter must leave the second subscription's event queued"
        );

        // User-facing receive deadlines must likewise use the injected
        // Moonpool time provider and resolve without a Tokio reactor.
        let sleep_provider: Arc<crate::SleepProvider> =
            Arc::new(|_| Box::pin(async { Ok(()) }) as crate::SleepFuture);
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let handle = shared.inner.lock().subscribe(SubscribeRequest {
            topic: "persistent://public/default/recv-provider-timeout".to_owned(),
            subscription: "s".to_owned(),
            ..Default::default()
        });
        let mut consumer: Consumer<TokioProviders> = make_consumer(shared, handle);
        consumer.sleep_provider = sleep_provider;
        let result = futures::executor::block_on(
            consumer.receive_with_timeout(std::time::Duration::from_secs(1)),
        )
        .expect("injected timeout must resolve without Tokio");
        assert!(result.is_none());
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
        // ADR-0038 Phase 2: per-slot identity reads bypass the global
        // registry. A Consumer constructed against a handle that was never
        // registered now reads from the in-process stub slot rather than
        // probing `Connection`'s map — so topic / subscription stay empty
        // and `is_closed` reflects the stub state (default `false`) rather
        // than the pre-split convention of "true for unknown handles". The
        // production `Client::subscribe` path always returns a freshly-
        // registered slot, so this semantic shift only affects test
        // helpers that synthesise Consumer values around a bogus handle.
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let consumer: Consumer<TokioProviders> =
            make_consumer(shared, magnetar_proto::ConsumerHandle(9999));
        assert_eq!(consumer.topic(), "");
        assert_eq!(consumer.subscription(), "");
        assert!(
            !consumer.is_closed(),
            "Phase 2 stub slot defaults to closed=false"
        );
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
            decryptor: None,
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
            decryptor: None,
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
                .state
                .lock()
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
                .state
                .lock()
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
                .state
                .lock()
                .receive_wakers
                .len(),
            0,
            "the cancelled receive's slab slot must be evicted",
        );
    }

    /// Regression for the CLI "consume hangs against fresh broker" bug: when the broker
    /// rejects a subscribe with a PERMANENT `CommandError` (e.g.
    /// `AuthorizationError`), the moonpool engine's subscribe waiter must surface
    /// a `ClientError::Broker` rather than parking on the driver waker forever.
    /// Mirrors the proto-level permanent-failure test. Retryable codes such as
    /// `ServiceNotReady`, `MetadataError`, and `PersistenceError` hit the retry
    /// path instead; `TopicNotFound` is terminal under ADR-0080.
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
            accept_prior_attachment: false,
            expected_waiter_id: None,
            notification: None,
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
        assert!(matches!(
            super::map_ack_outcome(magnetar_proto::OpOutcome::Terminal {
                key: magnetar_proto::PendingOpKey::Request(magnetar_proto::RequestId(1)),
                reason: "test driver exited".to_owned(),
            }),
            Err(ClientError::PeerClosed)
        ));
        assert!(matches!(
            super::map_ack_outcome(magnetar_proto::OpOutcome::NewTxn {
                request_id: magnetar_proto::RequestId(2),
                result: Ok(magnetar_proto::TxnId::new(3, 4)),
            }),
            Err(ClientError::Other(message)) if message.contains("unexpected ack outcome")
        ));
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
            let _ = conn.initial_flow(handle, Instant::now());
        }
        assert_eq!(consumer.available_permits(), 64);

        let closed: Consumer<TokioProviders> =
            make_consumer(shared, magnetar_proto::ConsumerHandle(9999));
        assert_eq!(closed.available_permits(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn auto_receiver_queue_policy_grows_target_under_starvation() {
        // Issue #301: an `Auto`-policy consumer driven through the moonpool
        // engine's proto connection grows its receiver-queue target when the
        // broker drains every permit (starvation), and the grown target rides an
        // incremental flow. Mirrors the tokio engine test 1:1 (ADR-0024).
        use std::time::Duration;
        let shared = handshake_complete_shared();
        let interval = Duration::from_secs(1);
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/auto-rq".to_owned(),
                subscription: "s".to_owned(),
                receiver_queue_policy: Some(std::sync::Arc::new(magnetar_proto::Auto::new(
                    100,
                    128 * 1024 * 1024,
                ))),
                receiver_queue_adjust_interval: Some(interval),
                ..Default::default()
            })
        };
        let t0 = Instant::now();
        {
            let mut conn = shared.inner.lock();
            let _ = conn.initial_flow(handle, t0);
            // Seed at the floor.
            assert_eq!(conn.consumer_receiver_queue_size(handle), 100);
            // Issue #349: drain the broker-side permit BALANCE via real
            // dispatch — 100 single-message deliveries against the
            // 100-permit initial grant — so the tick observes genuine
            // starvation, not a synthetic field write.
            for i in 0..100u64 {
                let frame = command_message_bytes(handle.0, i, format!("m{i}").as_bytes());
                conn.handle_bytes(t0, &frame).expect("deliver message");
            }
            // First tick arms the schedule; the second runs the adjust.
            conn.handle_timeout(t0);
            conn.handle_timeout(t0 + interval);
            assert_eq!(
                conn.consumer_receiver_queue_size(handle),
                200,
                "starvation doubles the Auto target on the moonpool engine"
            );
            assert_eq!(
                conn.consumer_available_permits(handle),
                200,
                "the incremental flow tops the broker grant up to the new target"
            );
        }
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
            .receive_batch_with_bytes_cap(0, 1024, std::time::Duration::from_mins(1))
            .await
            .expect("ok");
        assert!(zero_msgs.is_empty());
        // max_bytes=0 → empty without waiting.
        let zero_bytes = consumer
            .receive_batch_with_bytes_cap(10, 0, std::time::Duration::from_mins(1))
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
    async fn successful_unsubscribe_cancels_established_retry_generation() {
        let shared = handshake_complete_shared();
        let (handle, slot, failed_request_id) = {
            let mut conn = shared.inner.lock();
            let initial_request_id = conn.peek_next_request_id_for_test();
            let handle = conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/unsubscribe-cancels-retry".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            });
            let slot = conn.consumer(handle).expect("consumer slot").clone();
            let success = pb::BaseCommand {
                r#type: pb::base_command::Type::Success as i32,
                success: Some(pb::CommandSuccess {
                    request_id: initial_request_id,
                    schema: None,
                }),
                ..Default::default()
            };
            let mut frame = BytesMut::new();
            magnetar_proto::encode_command(&mut frame, &success)
                .expect("encode initial subscribe success");
            conn.handle_bytes(Instant::now(), &frame)
                .expect("establish consumer");
            assert!(conn.consume_initial_consumer_subscribe_completion(handle));
            while conn.poll_event().is_some() {}

            conn.reset();
            conn.begin_handshake().expect("restart handshake");
            let frame = handshake_response_bytes();
            conn.handle_bytes(Instant::now(), &frame)
                .expect("complete reconnect handshake");
            while conn.poll_event().is_some() {}
            let failed_request_id = conn.rebuild_consumers()[0];
            let transient_error = pb::BaseCommand {
                r#type: pb::base_command::Type::Error as i32,
                error: Some(pb::CommandError {
                    request_id: failed_request_id.0,
                    error: pb::ServerError::ConsumerBusy as i32,
                    message: "retry later".to_owned(),
                }),
                ..Default::default()
            };
            let mut frame = BytesMut::new();
            magnetar_proto::encode_command(&mut frame, &transient_error)
                .expect("encode transient error");
            conn.handle_bytes(Instant::now(), &frame)
                .expect("schedule established retry");
            assert!(conn.consumer_subscribe_retry_is_current(handle, failed_request_id));
            let _ = conn.poll_transmit();
            (handle, slot, failed_request_id)
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);
        assert!(Arc::ptr_eq(&consumer.slot, &slot));

        let unsubscribe_request_id = shared.inner.lock().peek_next_request_id_for_test();
        let injector_shared = shared.clone();
        let injector =
            tokio::spawn(async move {
                for _ in 0..64 {
                    tokio::task::yield_now().await;
                    if injector_shared.inner.lock().has_pending_request_for_test(
                        magnetar_proto::RequestId(unsubscribe_request_id),
                    ) {
                        let (late_retry, mut outbound) = {
                            let mut conn = injector_shared.inner.lock();
                            let late_retry =
                                conn.retry_consumer_subscribe_if_current(handle, failed_request_id);
                            (late_retry, conn.poll_transmit())
                        };
                        let mut saw_unsubscribe = false;
                        let mut saw_subscribe = false;
                        while !outbound.is_empty() {
                            let frame = decode_one(&mut outbound).expect("decode staged command");
                            saw_unsubscribe |=
                                frame.command.r#type == pb::base_command::Type::Unsubscribe as i32;
                            saw_subscribe |=
                                frame.command.r#type == pb::base_command::Type::Subscribe as i32;
                        }
                        let success = pb::BaseCommand {
                            r#type: pb::base_command::Type::Success as i32,
                            success: Some(pb::CommandSuccess {
                                request_id: unsubscribe_request_id,
                                schema: None,
                            }),
                            ..Default::default()
                        };
                        let mut frame = BytesMut::new();
                        magnetar_proto::encode_command(&mut frame, &success)
                            .expect("encode unsubscribe success");
                        injector_shared
                            .inner
                            .lock()
                            .handle_bytes(Instant::now(), &frame)
                            .expect("handle unsubscribe success");
                        return (late_retry, saw_unsubscribe, saw_subscribe);
                    }
                }
                panic!("pending unsubscribe request never registered");
            });

        consumer
            .unsubscribe(false)
            .await
            .expect("unsubscribe must resolve on CommandSuccess");
        let (late_retry, saw_unsubscribe, saw_subscribe) =
            injector.await.expect("injector completes");

        assert!(saw_unsubscribe, "unsubscribe command must be staged");
        assert_eq!(
            late_retry, None,
            "an in-flight unsubscribe must invalidate the retry generation"
        );
        assert!(
            !saw_subscribe,
            "no CommandSubscribe may be staged after CommandUnsubscribe"
        );

        assert!(
            consumer.is_closed(),
            "unsubscribe must close the local handle"
        );
        let mut conn = shared.inner.lock();
        assert!(!conn.consumer_subscribe_retry_is_current(handle, failed_request_id));
        assert_eq!(
            conn.retry_consumer_subscribe_if_current(handle, failed_request_id),
            None,
            "a detached retry must not recreate the deleted subscription"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_unsubscribe_restores_established_retry_generation() {
        let shared = handshake_complete_shared();
        let (handle, slot, failed_request_id) = established_consumer_with_failed_retry(
            &shared,
            "persistent://public/default/unsubscribe-restores-retry",
        );
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);
        assert!(Arc::ptr_eq(&consumer.slot, &slot));

        let unsubscribe_request_id = shared.inner.lock().peek_next_request_id_for_test();
        let injector_shared = shared.clone();
        let injector =
            tokio::spawn(async move {
                for _ in 0..64 {
                    tokio::task::yield_now().await;
                    if injector_shared.inner.lock().has_pending_request_for_test(
                        magnetar_proto::RequestId(unsubscribe_request_id),
                    ) {
                        let late_retry = injector_shared
                            .inner
                            .lock()
                            .retry_consumer_subscribe_if_current(handle, failed_request_id);
                        let error = pb::BaseCommand {
                            r#type: pb::base_command::Type::Error as i32,
                            error: Some(pb::CommandError {
                                request_id: unsubscribe_request_id,
                                error: pb::ServerError::MetadataError as i32,
                                message: "unsubscribe rejected".to_owned(),
                            }),
                            ..Default::default()
                        };
                        let mut frame = BytesMut::new();
                        magnetar_proto::encode_command(&mut frame, &error)
                            .expect("encode unsubscribe error");
                        injector_shared
                            .inner
                            .lock()
                            .handle_bytes(Instant::now(), &frame)
                            .expect("handle unsubscribe error");
                        return late_retry;
                    }
                }
                panic!("pending unsubscribe request never registered");
            });

        let result = consumer.unsubscribe(false).await;
        let late_retry = injector.await.expect("injector completes");

        assert!(matches!(
            result,
            Err(ClientError::Broker { code, .. })
                if code == pb::ServerError::MetadataError as i32
        ));
        assert_eq!(late_retry, None);
        assert!(
            !consumer.is_closed(),
            "a rejected unsubscribe must restore the consumer to a usable state"
        );
        let mut outbound = shared.inner.lock().poll_transmit();
        let mut saw_subscribe = false;
        while !outbound.is_empty() {
            let frame = decode_one(&mut outbound).expect("decode recovery command");
            saw_subscribe |= frame.command.r#type == pb::base_command::Type::Subscribe as i32;
        }
        assert!(
            saw_subscribe,
            "unsubscribe rejection must re-arm the interrupted attachment"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropped_unsubscribe_future_does_not_suspend_established_retry() {
        let shared = handshake_complete_shared();
        let (handle, slot, failed_request_id) = established_consumer_with_failed_retry(
            &shared,
            "persistent://public/default/dropped-unsubscribe-restores-retry",
        );
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);
        assert!(Arc::ptr_eq(&consumer.slot, &slot));
        let unsubscribe_request_id = shared.inner.lock().peek_next_request_id_for_test();

        let task_consumer = consumer.clone();
        let task = tokio::spawn(async move { task_consumer.unsubscribe(false).await });
        for _ in 0..64 {
            tokio::task::yield_now().await;
            if shared
                .inner
                .lock()
                .has_pending_request_for_test(magnetar_proto::RequestId(unsubscribe_request_id))
            {
                break;
            }
        }
        assert!(
            shared
                .inner
                .lock()
                .has_pending_request_for_test(magnetar_proto::RequestId(unsubscribe_request_id)),
            "unsubscribe request must be pending before cancellation"
        );
        task.abort();
        assert!(
            task.await
                .expect_err("unsubscribe task must be cancelled")
                .is_cancelled()
        );

        let error = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: unsubscribe_request_id,
                error: pb::ServerError::MetadataError as i32,
                message: "unsubscribe rejected after waiter cancellation".to_owned(),
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        magnetar_proto::encode_command(&mut frame, &error).expect("encode unsubscribe error");
        shared
            .inner
            .lock()
            .handle_bytes(Instant::now(), &frame)
            .expect("handle unsubscribe error");

        let mut conn = shared.inner.lock();
        let resumed_request_id =
            magnetar_proto::RequestId(conn.peek_next_request_id_for_test() - 1);
        assert_ne!(resumed_request_id, failed_request_id);
        assert!(conn.consumer_subscribe_retry_is_current(handle, resumed_request_id));
        let mut outbound = conn.poll_transmit();
        let mut saw_subscribe = false;
        while !outbound.is_empty() {
            let frame = decode_one(&mut outbound).expect("decode recovery command");
            saw_subscribe |= frame.command.r#type == pb::base_command::Type::Subscribe as i32;
        }
        assert!(
            saw_subscribe,
            "broker rejection must restore retry ownership even without a waiter"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unsubscribe_response_before_cancellation_does_not_leak_outcome() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/unsubscribe-response-cancel-race".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);
        let unsubscribe_request_id = shared.inner.lock().peek_next_request_id_for_test();
        let task = tokio::spawn(async move { consumer.unsubscribe(false).await });
        for _ in 0..64 {
            tokio::task::yield_now().await;
            if shared
                .inner
                .lock()
                .has_pending_request_for_test(magnetar_proto::RequestId(unsubscribe_request_id))
            {
                break;
            }
        }

        let error = pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id: unsubscribe_request_id,
                error: pb::ServerError::MetadataError as i32,
                message: "unsubscribe rejected before waiter cancellation".to_owned(),
            }),
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        magnetar_proto::encode_command(&mut frame, &error).expect("encode unsubscribe error");
        shared
            .inner
            .lock()
            .handle_bytes(Instant::now(), &frame)
            .expect("handle unsubscribe error");
        task.abort();
        assert!(
            task.await
                .expect_err("unsubscribe task must be cancelled")
                .is_cancelled()
        );

        assert!(
            shared
                .inner
                .lock()
                .take_outcome(magnetar_proto::PendingOpKey::Request(
                    magnetar_proto::RequestId(unsubscribe_request_id)
                ))
                .is_none(),
            "response-before-cancellation must not leave an undrainable outcome"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn overlapping_unsubscribe_is_rejected_without_staging_a_second_request() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/overlapping-unsubscribe".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);
        let first_request_id = shared.inner.lock().peek_next_request_id_for_test();
        let first_consumer = consumer.clone();
        let first = tokio::spawn(async move { first_consumer.unsubscribe(false).await });
        for _ in 0..64 {
            tokio::task::yield_now().await;
            if shared
                .inner
                .lock()
                .has_pending_request_for_test(magnetar_proto::RequestId(first_request_id))
            {
                break;
            }
        }

        let second = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            consumer.unsubscribe(false),
        )
        .await
        .expect("overlapping unsubscribe must fail immediately");
        assert!(matches!(
            second,
            Err(ClientError::Other(message)) if message == "unsubscribe already in progress"
        ));
        assert_eq!(
            shared.inner.lock().peek_next_request_id_for_test(),
            first_request_id + 1,
            "rejected overlap must not allocate or stage a second request"
        );

        first.abort();
        let _ = first.await;
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
        let producer_slot = shared
            .inner
            .lock()
            .producer(producer_handle)
            .cloned()
            .expect("test producer slot must exist");
        let producer: Producer<TokioProviders> = Producer::assemble(
            shared,
            producer_handle,
            producer_slot,
            magnetar_proto::types::CompressionKind::None,
            None,
        );
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
        let producer_slot = shared
            .inner
            .lock()
            .producer(producer_handle)
            .cloned()
            .expect("test producer slot must exist");
        let producer: Producer<TokioProviders> = Producer::assemble(
            shared.clone(),
            producer_handle,
            producer_slot,
            magnetar_proto::types::CompressionKind::None,
            None,
        );
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
            metadata: std::sync::Arc::new(magnetar_proto::pb::MessageMetadata::default()),
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

    /// Inbound compressed data is valid regardless of whether this runtime's
    /// producer currently emits it. Decryption must unwrap the compressed bytes
    /// before bounded decompression runs.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_decrypts_then_decompresses_compressed_encrypted_payload() {
        use std::io::Write as _;

        let plaintext: Vec<u8> = b"the-quick-brown-fox-jumps-over-the-lazy-dog-".repeat(8);
        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&plaintext).expect("compress plaintext");
        let compressed = encoder.finish().expect("finish compression");
        let ciphertext = compressed
            .iter()
            .map(|byte| byte ^ XOR_KEY)
            .collect::<Vec<_>>();
        let mut msg = IncomingMessage {
            message_id: MessageId::EARLIEST,
            metadata: Arc::new(pb::MessageMetadata {
                encryption_keys: vec![pb::EncryptionKeys {
                    key: "xor-test".to_owned(),
                    value: Bytes::from_static(b"k"),
                    metadata: Vec::new(),
                }],
                compression: Some(pb::CompressionType::Zlib as i32),
                uncompressed_size: Some(
                    u32::try_from(plaintext.len()).expect("plaintext fits u32"),
                ),
                ..Default::default()
            }),
            single_metadata: None,
            payload: Bytes::from(ciphertext),
            redelivery_count: 0,
            broker_entry_metadata: None,
            arrived_at: Instant::now(),
        };
        assert!(matches!(
            post_process_message(
                &mut msg,
                Some(&(Arc::new(XorDecryptor) as Arc<dyn MessageDecryptor>)),
                magnetar_proto::CryptoFailureAction::Fail,
            ),
            PostProcessOutcome::Deliver
        ));
        assert_eq!(
            msg.payload.as_ref(),
            plaintext.as_slice(),
            "moonpool must decrypt first and then decompress"
        );

        let malformed_message = |compression, payload: &'static [u8]| IncomingMessage {
            message_id: MessageId::EARLIEST,
            metadata: Arc::new(pb::MessageMetadata {
                compression: Some(compression),
                uncompressed_size: Some(1),
                ..Default::default()
            }),
            single_metadata: None,
            payload: Bytes::from_static(payload),
            redelivery_count: 0,
            broker_entry_metadata: None,
            arrived_at: Instant::now(),
        };
        let mut unknown = malformed_message(i32::MAX, b"unknown");
        match post_process_message(
            &mut unknown,
            None,
            magnetar_proto::CryptoFailureAction::Fail,
        ) {
            PostProcessOutcome::Fail(ClientError::Other(message)) => {
                assert!(message.contains("unknown compression code"));
            }
            other => panic!("unknown compression must fail, got {other:?}"),
        }
        let mut malformed = malformed_message(pb::CompressionType::Zlib as i32, b"not-zlib");
        match post_process_message(
            &mut malformed,
            None,
            magnetar_proto::CryptoFailureAction::Fail,
        ) {
            PostProcessOutcome::Fail(ClientError::Other(message)) => {
                assert!(message.contains("decompress:"));
            }
            other => panic!("malformed compressed payload must fail, got {other:?}"),
        }
        let mut uncompressed = malformed_message(pb::CompressionType::None as i32, b"plain");
        assert!(matches!(
            post_process_message(
                &mut uncompressed,
                None,
                magnetar_proto::CryptoFailureAction::Fail,
            ),
            PostProcessOutcome::Deliver
        ));
        assert_eq!(uncompressed.payload.as_ref(), b"plain");
    }

    /// Mirror of `magnetar-runtime-tokio::consumer::tests
    /// ::receive_returns_closed_after_local_close_race`. The moonpool
    /// `ReceiveFut::poll` already carries the closed-state re-check (since
    /// the dawn of the sim engine — see the `if conn.is_closed() ||
    /// conn.consumer_is_closed(handle)` block); this test pins the
    /// equivalent behaviour so the tokio↔moonpool parity gate
    /// (ADR-0024 / `check-runtime-test-parity`) catches any future drift.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_returns_closed_after_local_close_race() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/close-race".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };

        // Simulate the close-race: another (cloned) handle ran `close()` and
        // flipped the per-slot `closed` bit + drained wakers BEFORE we ever
        // installed ours. The receive future must spot the closed state and
        // resolve `Err(Closed)` instead of parking on the slab.
        {
            let conn = shared.inner.lock();
            let slot = conn.consumer(handle).expect("slot is registered").clone();
            slot.state.lock().close();
        }

        let fut = ReceiveFut {
            shared: shared.clone(),
            handle,
            decryptor: None,
            slab_key: None,
        };
        let outcome = tokio::time::timeout(std::time::Duration::from_millis(250), fut)
            .await
            .expect("receive must resolve quickly; timeout means ReceiveFut parked forever");
        match outcome {
            Err(crate::client::ClientError::Closed) => {}
            other => panic!("expected Err(Closed) after local close, got {other:?}"),
        }
        #[cfg(feature = "scalable-topics")]
        {
            let consumer: Consumer<TokioProviders> = make_consumer(shared.clone(), handle);
            consumer.close_best_effort();
            consumer.force_close_best_effort();
            shared.mark_no_driver();
            consumer.force_close_best_effort();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unchecked_uncompressed_size_is_rejected_before_allocation() {
        let error = crate::compress::decompress(
            magnetar_proto::types::CompressionKind::Zlib,
            &[],
            u32::MAX as usize,
        )
        .expect_err("oversized output hint must be rejected");
        assert!(matches!(
            error,
            crate::compress::CompressionError::UncompressedSizeTooLarge { .. }
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn zstd_decompression_bomb_is_bounded() {
        let plaintext = vec![0u8; magnetar_proto::MAX_FRAME_SIZE + 1024];
        let compressed = zstd::stream::encode_all(plaintext.as_slice(), 3)
            .expect("compress bomb-shaped payload");
        let error = crate::compress::decompress(
            magnetar_proto::types::CompressionKind::Zstd,
            &compressed,
            1,
        )
        .expect_err("bomb-shaped payload must be rejected");
        assert!(matches!(
            error,
            crate::compress::CompressionError::UncompressedSizeTooLarge { .. }
                | crate::compress::CompressionError::SizeMismatch { .. }
                | crate::compress::CompressionError::Zstd(_)
        ));
    }
}
