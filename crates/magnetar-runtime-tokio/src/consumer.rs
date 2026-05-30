// SPDX-License-Identifier: Apache-2.0

//! Consumer handle exposed to user code.
//!
//! Wraps an [`Arc<ConnectionShared>`](crate::ConnectionShared) and a
//! [`magnetar_proto::ConsumerHandle`]. Receiving a message means pulling the next
//! [`magnetar_proto::IncomingMessage`] from the sans-io state machine's per-consumer queue. The
//! state machine refills permits opportunistically via FLOW commands.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use magnetar_proto::{
    AckRequest, ConsumerHandle, IncomingMessage, MessageId, OpOutcome, PendingOpKey, SeekTarget, pb,
};

use crate::ConnectionShared;
use crate::client::wait_subscribe_acked;
use crate::error::ClientError;

/// User-facing consumer handle.
///
/// # Lock-ordering (ADR-0038)
///
/// Identity reads (topic, subscription, handle) go through `slot.identity`
/// with no lock. State-machine reads (`available_in_queue`, `is_closed`,
/// `stats`, etc.) take only the per-slot mutex via `slot.state.lock()`.
/// Operations that drive protocol I/O (`receive`, `ack`, `seek`, `close`)
/// still take `shared.inner.lock()`. Acquisition order: **global → per-slot,
/// never the reverse**.
#[derive(Debug, Clone)]
pub struct Consumer {
    pub(crate) shared: Arc<ConnectionShared>,
    pub(crate) handle: ConsumerHandle,
    /// Direct handle to this consumer's per-slot state, cloned from the
    /// Connection's registry at subscribe time.
    pub(crate) slot: Arc<magnetar_proto::ConsumerSlot>,
    /// Optional PIP-4 decryption hook. When the broker delivers a message with
    /// `MessageMetadata.encryption_keys` set, the consumer hands the ciphertext through
    /// this hook before yielding it to the user.
    pub(crate) decryptor: Option<Arc<dyn crate::crypto::MessageDecryptor>>,
}

impl Consumer {
    /// The protocol-layer consumer handle this façade wraps.
    pub fn handle(&self) -> ConsumerHandle {
        self.handle
    }

    /// `true` if this consumer has been closed (locally via [`Self::close`] /
    /// [`Self::unsubscribe`] or remotely via a broker `CloseConsumer`). Mirrors Java
    /// `ConsumerImpl#getState() == CLOSED`. Use [`Self::is_connected`] for the live test
    /// — `is_closed` only flips after a terminal close, not on transient disconnects.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.slot.state.lock().closed
    }

    /// PIP-180 / ADR-0033: pre-populate shadow-topic metadata so the receive
    /// dispatch surfaces source-topic context on each incoming message.
    ///
    /// Once set, every inbound entry carrying
    /// [`pb::MessageMetadata::replicated_from`] is emitted by the sans-io
    /// state machine as
    /// [`magnetar_proto::ConnectionEvent::MessageReceivedFromShadow`]
    /// rather than the regular
    /// [`magnetar_proto::ConnectionEvent::Message`].
    ///
    /// Typically called by `Client::subscribe_shadow_aware` (or
    /// `magnetar::Client::subscribe` under the `admin` feature) after a
    /// `magnetar-admin` `get_shadow_source(topic)` lookup resolves a
    /// shadow-attached topic. Direct callers may set it themselves when
    /// they already know the source topic (e.g. tests, integration
    /// scenarios where the admin REST is mocked).
    pub fn set_shadow_source(&self, source_topic: impl Into<String>) {
        // ADR-0038: per-slot write via the direct Arc<ConsumerSlot>, no global lock.
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
    ///
    /// `Some(topic)` ⇒ shadow-attached (broker-side topic is a shadow of
    /// `topic`); `None` ⇒ regular consumer (no shadow translation on the
    /// receive path).
    #[must_use]
    pub fn shadow_source_topic(&self) -> Option<String> {
        // ADR-0038: per-slot read, no global lock.
        self.slot
            .state
            .lock()
            .shadow_metadata
            .as_ref()
            .map(|m| m.source_topic.clone())
    }

    /// PIP-180 / ADR-0033: convenience predicate equivalent to
    /// `shadow_source_topic().is_some()`.
    #[must_use]
    pub fn is_shadow(&self) -> bool {
        self.shadow_source_topic().is_some()
    }

    /// Number of messages currently buffered in this consumer's receiver queue, waiting
    /// for a `receive()` call to pull them out. Mirrors Java
    /// `Consumer#getNumMessagesInQueue`.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn available_in_queue(&self) -> usize {
        self.slot.state.lock().queue.len()
    }

    /// Drain up to `max` already-buffered messages from this consumer's receive queue
    /// without awaiting the broker. Returns an empty `Vec` when the queue is empty or
    /// `max == 0`. Does NOT acknowledge — the caller is responsible for acking each
    /// returned [`IncomingMessage`] (or batching them via [`Self::ack_batch`]).
    ///
    /// Mirrors the Java pattern of polling `Consumer#receiveAsync` in a non-blocking loop:
    /// useful for "process whatever's already here, then move on" workloads where blocking
    /// for new arrivals is undesirable.
    ///
    /// Encrypted or compressed payloads are returned as-is — this is a raw drain of the
    /// state-machine queue. Callers that need the decompression / decryption pipeline
    /// should use [`Self::receive`] or [`Self::receive_batch`] instead.
    #[must_use]
    pub fn drain_messages(&self, max: usize) -> Vec<IncomingMessage> {
        if max == 0 {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(max.min(64));
        let mut conn = self.shared.inner.lock();
        while out.len() < max {
            match conn.pop_message(self.handle) {
                Some(msg) => out.push(msg),
                None => break,
            }
        }
        drop(conn);
        // `pop_message` may have queued FLOW frames; wake the driver to flush them.
        if !out.is_empty() {
            self.shared.driver_waker.notify_one();
        }
        out
    }

    /// Number of dispatch permits this consumer still has with the broker — i.e. messages
    /// it has authorised the broker to push without an explicit `CommandFlow`. Mirrors
    /// Java `ConsumerBase#getAvailablePermits`.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn available_permits(&self) -> u32 {
        self.slot.state.lock().available_permits
    }

    /// `true` if this consumer has received at least one message since opening. Mirrors
    /// Java `Consumer#hasReceivedAnyMessage` — useful as a "did anything ever arrive?"
    /// probe without inspecting the full `ConsumerStats`.
    #[must_use]
    pub fn has_received_any_message(&self) -> bool {
        self.stats().total_msgs_received > 0
    }

    /// Receive the next message, bounded by `timeout`. Returns `Ok(None)` if the deadline
    /// elapses with no message. Mirrors Java `Consumer#receive(int timeout, TimeUnit unit)`.
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

    /// Receive the next message. Resolves when the broker delivers a `CommandMessage` and the
    /// state machine emits it into this consumer's queue.
    ///
    /// Multiple concurrent `receive()` calls on the same consumer are
    /// supported: each future installs its own waker into the per-consumer
    /// slab on [`magnetar_proto::consumer::ConsumerState`]; arrival drains the slab and
    /// every parked future is re-polled. The first to acquire the connection
    /// lock pops the message; the others observe an empty queue and re-park.
    pub fn receive(&self) -> ReceiveFut {
        ReceiveFut {
            shared: self.shared.clone(),
            handle: self.handle,
            decryptor: self.decryptor.clone(),
            slab_key: None,
        }
    }

    /// Acknowledge a single message (individual ack).
    ///
    /// Returns a future that resolves when the broker confirms (`CommandAckResponse`).
    pub fn ack(&self, message_id: MessageId) -> impl Future<Output = Result<(), ClientError>> {
        self.ack_many(vec![message_id], pb::command_ack::AckType::Individual)
    }

    /// Acknowledge multiple messages in a single round-trip. Mirrors Java
    /// `Consumer#acknowledgeAsync(List<MessageId>)`. Returns a future that resolves when the
    /// broker confirms (`CommandAckResponse`).
    pub fn ack_batch(
        &self,
        message_ids: Vec<MessageId>,
    ) -> impl Future<Output = Result<(), ClientError>> {
        self.ack_many(message_ids, pb::command_ack::AckType::Individual)
    }

    /// Acknowledge a cumulative position.
    pub fn ack_cumulative(
        &self,
        message_id: MessageId,
    ) -> impl Future<Output = Result<(), ClientError>> {
        self.ack_many(vec![message_id], pb::command_ack::AckType::Cumulative)
    }

    /// Acknowledge a single message with caller-supplied properties. Mirrors Java
    /// `Consumer#acknowledgeAsync(MessageId, Map<String, Long>)`. The broker stores the
    /// properties alongside the cursor (no semantic effect at the dispatch layer; useful
    /// for diagnostics and replay tooling).
    pub fn ack_with_properties(
        &self,
        message_id: MessageId,
        properties: Vec<(String, i64)>,
    ) -> impl Future<Output = Result<(), ClientError>> {
        self.ack_many_with(
            vec![message_id],
            pb::command_ack::AckType::Individual,
            properties,
            None,
        )
    }

    /// Acknowledge a single message as part of a Pulsar transaction (PIP-31). The ack only
    /// takes effect once the transaction commits. Mirrors Java
    /// `Consumer#acknowledgeAsync(MessageId, Transaction)`.
    pub fn ack_with_txn(
        &self,
        message_id: MessageId,
        txn_id: magnetar_proto::TxnId,
    ) -> impl Future<Output = Result<(), ClientError>> {
        self.ack_many_with(
            vec![message_id],
            pb::command_ack::AckType::Individual,
            Vec::new(),
            Some(txn_id),
        )
    }

    /// Acknowledge a batch of messages as part of a Pulsar transaction (PIP-31). Mirrors
    /// Java `Consumer#acknowledgeAsync(List<MessageId>, Transaction)`.
    pub fn ack_batch_with_txn(
        &self,
        message_ids: Vec<MessageId>,
        txn_id: magnetar_proto::TxnId,
    ) -> impl Future<Output = Result<(), ClientError>> {
        self.ack_many_with(
            message_ids,
            pb::command_ack::AckType::Individual,
            Vec::new(),
            Some(txn_id),
        )
    }

    /// Stage an individual ack into the consumer's ack-grouping tracker (opt-in via
    /// `ConsumerBuilder::ack_group_time`). Fire-and-forget: the call returns immediately
    /// without a future, and the coalesced `CommandAck` is emitted by the state machine
    /// once `ack_group_time` has elapsed. Mirrors Java's `acknowledgmentGroupTime` path
    /// — trades broker-confirmation guarantees for lower ack bandwidth on high-throughput
    /// consumers. With no tracker configured, falls back to a synchronous immediate
    /// `CommandAck` so the message is never silently dropped.
    pub fn ack_grouped(&self, message_id: MessageId) {
        let now = std::time::Instant::now();
        let mut conn = self.shared.inner.lock();
        conn.ack_grouped_individual(self.handle, message_id, now);
        drop(conn);
        self.shared.driver_waker.notify_one();
    }

    /// Stage a cumulative ack into the consumer's ack-grouping tracker. See
    /// [`Self::ack_grouped`] for the semantics.
    pub fn ack_grouped_cumulative(&self, message_id: MessageId) {
        let now = std::time::Instant::now();
        let mut conn = self.shared.inner.lock();
        conn.ack_grouped_cumulative(self.handle, message_id, now);
        drop(conn);
        self.shared.driver_waker.notify_one();
    }

    /// Cumulative ack with caller-supplied properties. Mirrors Java
    /// `Consumer#acknowledgeCumulativeAsync(MessageId, Map<String, Long>)`. The broker
    /// stores the properties alongside the cursor (no semantic effect at the dispatch
    /// layer; useful for diagnostics and replay tooling).
    pub fn ack_cumulative_with_properties(
        &self,
        message_id: MessageId,
        properties: Vec<(String, i64)>,
    ) -> impl Future<Output = Result<(), ClientError>> {
        self.ack_many_with(
            vec![message_id],
            pb::command_ack::AckType::Cumulative,
            properties,
            None,
        )
    }

    /// Cumulative ack as part of a Pulsar transaction (PIP-31). Mirrors Java
    /// `Consumer#acknowledgeCumulativeAsync(MessageId, Transaction)`.
    pub fn ack_cumulative_with_txn(
        &self,
        message_id: MessageId,
        txn_id: magnetar_proto::TxnId,
    ) -> impl Future<Output = Result<(), ClientError>> {
        self.ack_many_with(
            vec![message_id],
            pb::command_ack::AckType::Cumulative,
            Vec::new(),
            Some(txn_id),
        )
    }

    fn ack_many(
        &self,
        message_ids: Vec<MessageId>,
        ack_type: pb::command_ack::AckType,
    ) -> impl Future<Output = Result<(), ClientError>> {
        self.ack_many_with(message_ids, ack_type, Vec::new(), None)
    }

    fn ack_many_with(
        &self,
        message_ids: Vec<MessageId>,
        ack_type: pb::command_ack::AckType,
        properties: Vec<(String, i64)>,
        txn_id: Option<magnetar_proto::TxnId>,
    ) -> impl Future<Output = Result<(), ClientError>> {
        let shared = self.shared.clone();
        let request_id = {
            let mut conn = shared.inner.lock();
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
        shared.driver_waker.notify_one();
        async move {
            let outcome = RequestFut {
                shared,
                key: PendingOpKey::Request(request_id),
            }
            .await;
            match outcome {
                OpOutcome::Success { .. } => Ok(()),
                OpOutcome::Error { code, message, .. } => {
                    Err(ClientError::Broker { code, message })
                }
                other => Err(ClientError::Other(format!(
                    "unexpected ack outcome: {other:?}"
                ))),
            }
        }
    }

    /// Issue an explicit FLOW (permit refill) for this consumer.
    pub fn flow(&self, permits: u32) {
        let mut conn = self.shared.inner.lock();
        conn.flow(self.handle, permits);
        drop(conn);
        self.shared.driver_waker.notify_one();
    }

    /// Negatively acknowledge a single message. The broker will redeliver it (subject to
    /// `maxRedeliverCount` and any DLQ policy configured server-side). Fire-and-forget.
    pub fn negative_ack(&self, message_id: MessageId) {
        self.negative_ack_many(vec![message_id]);
    }

    /// Negatively acknowledge a batch of messages.
    pub fn negative_ack_many(&self, message_ids: Vec<MessageId>) {
        let now = std::time::Instant::now();
        let mut conn = self.shared.inner.lock();
        conn.negative_ack(self.handle, message_ids, now);
        drop(conn);
        self.shared.driver_waker.notify_one();
    }

    /// Negatively acknowledge a single message with an explicit per-message redelivery
    /// delay. Mirrors Java's PIP-37 backoff path — pair with
    /// `magnetar_proto::trackers::nack::MultiplierRedeliveryBackoff::delay_for` to compute
    /// the delay from the broker-reported redelivery count.
    pub fn negative_ack_with_delay(&self, message_id: MessageId, delay: std::time::Duration) {
        let now = std::time::Instant::now();
        let mut conn = self.shared.inner.lock();
        conn.negative_ack_with_delay(self.handle, message_id, delay, now);
        drop(conn);
        self.shared.driver_waker.notify_one();
    }

    /// Ask the broker to redeliver *every* unacked message on this consumer. Useful when a
    /// consumer detects it has lost local state and wants the broker to replay.
    pub fn redeliver_unacked(&self) {
        self.negative_ack_many(Vec::new());
    }

    /// Ask the broker for the topic's last-published message id. Mirrors
    /// `org.apache.pulsar.client.api.Consumer#getLastMessageId`. Useful for "more messages
    /// available?" checks against the consumer's most-recently-received id.
    pub async fn last_message_id(&self) -> Result<MessageId, ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.get_last_message_id(self.handle)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            key: PendingOpKey::Request(request_id),
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

    /// Convenience: returns `true` if the broker has a message strictly past `cursor`
    /// (i.e. there is at least one more message to receive). `cursor` is typically the
    /// last [`MessageId`] this consumer received.
    ///
    /// Returns `false` if the broker reports an equal or earlier last-id (the comparison
    /// is `>` not `>=` — exact equality means "you've consumed up to here").
    pub async fn has_message_after(&self, cursor: MessageId) -> Result<bool, ClientError> {
        let last = self.last_message_id().await?;
        Ok(message_id_greater(&last, &cursor))
    }

    /// Look up the broker-registered schema for the consumer's topic (PIP-87).
    ///
    /// Issues a `CommandGetSchema` for the topic this consumer is bound to and awaits the
    /// `CommandGetSchemaResponse`. Returns the registry-resolved [`pb::Schema`] on success or
    /// [`ClientError::Broker`] when the broker rejects the lookup (e.g. `TopicNotFound`).
    /// Mirrors Java `PulsarClientImpl#getSchema(TopicName, Optional<byte[]>)`.
    ///
    /// `version = None` asks the broker for the topic's current schema; pass
    /// `Some(schema_version_bytes)` to re-resolve a historical schema (used by replay paths).
    ///
    /// The result is **not** cached here — callers that need a per-instance cache (e.g.
    /// [`magnetar_proto::schema::AutoConsumeSchema`]) push the resolved schema into their own
    /// `Arc<Mutex<…>>` after this future resolves.
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
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.get_schema(&topic, version)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            key: PendingOpKey::Request(request_id),
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

    /// Seek this consumer to a specific message id. The broker replays from there.
    ///
    /// Mirrors `org.apache.pulsar.client.api.Consumer#seek(MessageId)`.
    pub async fn seek_to_message(&self, message_id: MessageId) -> Result<(), ClientError> {
        self.seek_inner(SeekTarget::MessageId(message_id)).await
    }

    /// Seek this consumer to a specific publish timestamp (millis since the UNIX epoch).
    pub async fn seek_to_timestamp(&self, publish_time_ms: u64) -> Result<(), ClientError> {
        self.seek_inner(SeekTarget::PublishTime(publish_time_ms))
            .await
    }

    /// Seek to the earliest available message in the topic. Mirrors Java
    /// `Consumer#seek(MessageId.earliest)`. After this resolves, the next `receive()`
    /// returns the oldest message the broker still has.
    pub async fn seek_to_earliest(&self) -> Result<(), ClientError> {
        self.seek_to_message(MessageId::EARLIEST).await
    }

    /// Seek to the latest position (i.e. the broker's current head — skip any pending
    /// backlog). Mirrors Java `Consumer#seek(MessageId.latest)`.
    pub async fn seek_to_latest(&self) -> Result<(), ClientError> {
        self.seek_to_message(MessageId::LATEST).await
    }

    async fn seek_inner(&self, target: SeekTarget) -> Result<(), ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.seek(self.handle, target)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            key: PendingOpKey::Request(request_id),
        }
        .await;
        match outcome {
            OpOutcome::Success { .. } => {
                // Pulsar's broker disconnects the consumer as part of `CommandSeek`
                // processing (it has to quiesce the subscription before resetting
                // the cursor) but does NOT send `CommandCloseConsumer` on the wire
                // — the client is expected to re-establish. Without this, the
                // broker's internal consumer-id map no longer has us, and any
                // subsequent `CommandFlow` silently no-ops; `receive()` then hangs
                // forever.
                let resub_request_id = {
                    let mut conn = self.shared.inner.lock();
                    conn.resubscribe_consumer_after_seek(self.handle)
                };
                self.shared.driver_waker.notify_one();
                if resub_request_id.is_some() {
                    // Must wait for the broker to ACK the re-subscribe before
                    // sending Flow + Redeliver. Pulsar's `ServerCnx.handleFlow`
                    // silently drops `CommandFlow` for a consumer that doesn't
                    // exist yet — sending it inline (before SubscribeSuccess
                    // arrives) loses the permits and the broker creates the
                    // consumer with `available_permits = 0` so no dispatch
                    // ever happens. The previous in-proto-layer order
                    // (subscribe + flow + redeliver in one shot) tripped this
                    // race and was the real root cause of #67's "broker
                    // confirms backlog but no message dispatches".
                    wait_subscribe_acked(&self.shared, self.handle).await?;
                    {
                        let mut conn = self.shared.inner.lock();
                        let _ = conn.initial_flow(self.handle);
                        conn.redeliver_unacked_all(self.handle);
                    }
                    self.shared.driver_waker.notify_one();
                }
                Ok(())
            }
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            other => Err(ClientError::Other(format!(
                "unexpected seek outcome: {other:?}"
            ))),
        }
    }

    /// Unsubscribe this consumer's subscription from the broker. Unlike
    /// [`close`](Self::close) which only tears down the consumer instance,
    /// `unsubscribe` deletes the subscription cursor entirely.
    ///
    /// Mirrors `org.apache.pulsar.client.api.Consumer#unsubscribe`. After this
    /// call the consumer is unusable; callers typically follow with `close()`.
    ///
    /// `force=true` (PIP-313) drops the subscription even if other consumers
    /// are still attached to the same subscription name.
    pub async fn unsubscribe(&self, force: bool) -> Result<(), ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.unsubscribe(self.handle, force)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            key: PendingOpKey::Request(request_id),
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

    /// Close this consumer. Resolves when the broker acks the close.
    pub async fn close(self) -> Result<(), ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.close_consumer(self.handle)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            key: PendingOpKey::Request(request_id),
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

    /// Mirrors `org.apache.pulsar.client.api.Consumer#isConnected`. Returns `true` while the
    /// underlying broker connection is up.
    pub fn is_connected(&self) -> bool {
        self.shared.inner.lock().is_connected()
    }

    /// Mirrors `org.apache.pulsar.client.api.Consumer#getLastDisconnectedTimestamp`: wall-clock
    /// time at which the underlying connection most recently went down. `None` while the
    /// connection has never been disconnected.
    pub fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        self.shared.inner.lock().last_disconnected_timestamp()
    }

    /// Snapshot of this consumer's cumulative counters. Mirrors Java
    /// `org.apache.pulsar.client.api.Consumer#getStats`. Returns a zeroed snapshot if the
    /// consumer handle is no longer registered (closed).
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    pub fn stats(&self) -> magnetar_proto::ConsumerStats {
        self.slot.state.lock().stats()
    }

    /// Capture a rolling-window sample for this consumer. Mirrors Java
    /// `ConsumerStatsRecorderImpl#updateNumMsgsReceived` — call periodically (e.g.
    /// once per second) to refresh [`magnetar_proto::ConsumerStats::msgs_per_sec`]
    /// and [`magnetar_proto::ConsumerStats::bytes_per_sec`]. The first call only
    /// seeds the baseline (rates stay at `0.0`); the second and subsequent calls
    /// compute the per-second deltas between consecutive samples.
    ///
    /// Per-slot write — does NOT take the global Connection mutex.
    pub fn record_rate_window(&self, now: std::time::Instant) {
        self.slot.state.lock().record_rate_window(now);
    }

    /// Mirrors `org.apache.pulsar.client.api.Consumer#pause`. Stops automatic flow refills so
    /// the broker stops dispatching new messages once already-issued permits drain. Buffered
    /// messages remain receivable.
    ///
    /// Per-slot write — does NOT take the global Connection mutex.
    pub fn pause(&self) {
        self.slot.state.lock().paused = true;
    }

    /// Mirrors `org.apache.pulsar.client.api.Consumer#resume`. Re-enables automatic flow
    /// refills.
    pub fn resume(&self) {
        self.slot.state.lock().paused = false;
        // Nudge the driver — it may have a flow to emit now that we're un-paused.
        self.shared.driver_waker.notify_one();
    }

    /// Returns `true` if the consumer is currently paused.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    pub fn is_paused(&self) -> bool {
        self.slot.state.lock().paused
    }

    /// Returns `true` once the broker has indicated end-of-topic for this consumer (no
    /// further messages will be dispatched). Mirrors Java
    /// `Consumer#hasReachedEndOfTopic`.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    pub fn has_reached_end_of_topic(&self) -> bool {
        self.slot.state.lock().reached_end_of_topic
    }

    /// Topic name this consumer is bound to. Returns an empty string if the consumer is
    /// no longer registered (closed).
    ///
    /// Identity-only read — takes no lock.
    pub fn topic(&self) -> String {
        self.slot.identity.topic.clone()
    }

    /// Subscription name. Empty string if the consumer is no longer registered.
    ///
    /// Identity-only read — takes no lock.
    pub fn subscription(&self) -> String {
        self.slot.identity.subscription.clone()
    }

    /// Caller-supplied consumer name. Empty string if no name was supplied at subscribe
    /// time, or if the consumer is no longer registered. Mirrors Java
    /// `Consumer#getConsumerName`.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    pub fn name(&self) -> String {
        self.slot
            .state
            .lock()
            .consumer_name
            .clone()
            .unwrap_or_default()
    }

    /// Drain every message the state machine has flagged as dead-letter (redelivery count
    /// greater than the configured `max_redeliver_count`). The caller is responsible for
    /// republishing them to the configured DLQ topic. Returns an empty `Vec` when DLQ
    /// routing is disabled or no messages have been flagged.
    pub fn drain_dead_letter(&self) -> Vec<IncomingMessage> {
        let mut conn = self.shared.inner.lock();
        conn.drain_dead_letter(self.handle)
    }

    /// Drain the per-consumer dead-letter queue and republish every entry via `dlq_producer`,
    /// preserving each message's `partition_key`, `ordering_key`, `event_time`, and
    /// `properties`. After successful republish each original is acked so the consumer's
    /// cursor advances. Returns the number of messages republished.
    ///
    /// Pairs with [`crate::consumer::Consumer::drain_dead_letter`] for callers that want
    /// to inspect the messages before republishing — this helper is the "just republish
    /// transparently" convenience.
    ///
    /// # Errors
    ///
    /// Returns the first [`ClientError`] encountered. Already-republished messages stay
    /// republished — partial progress is not rolled back.
    pub async fn republish_dead_letters(
        &self,
        dlq_producer: &crate::Producer,
    ) -> Result<usize, ClientError> {
        let drained = self.drain_dead_letter();
        let mut count = 0;
        for msg in drained {
            let mut metadata = magnetar_proto::pb::MessageMetadata::default();
            metadata.partition_key = msg.metadata.partition_key.clone();
            metadata.partition_key_b64_encoded = msg.metadata.partition_key_b64_encoded;
            metadata.ordering_key = msg.metadata.ordering_key.clone();
            metadata.event_time = msg.metadata.event_time;
            metadata.properties = msg.metadata.properties.clone();
            // Tag the republished message with the original id so DLQ consumers can
            // correlate back to the source. Mirrors Java's DeadLetterTopicMessageId
            // property convention.
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

    /// Republish a single message via `retry_producer` with a delay deadline, then ack
    /// the original. Mirrors Java `Consumer#reconsumeLater(Message, long, TimeUnit)`.
    ///
    /// The broker holds the republished message in the retry-letter topic until
    /// `delay` has elapsed, then dispatches it normally. A `RECONSUMETIMES` property is
    /// incremented on each redelivery so consumers can implement a maximum-retry policy
    /// above this layer. The original `partition_key`, `ordering_key`, `event_time`, and
    /// properties are preserved; `REAL_TOPIC` and `ORIGINAL_MESSAGE_ID` are stamped for
    /// correlation back to the source topic.
    ///
    /// # Errors
    ///
    /// Returns the first [`ClientError`] from the republish or the subsequent ack.
    pub async fn reconsume_later(
        &self,
        retry_producer: &crate::Producer,
        msg: IncomingMessage,
        delay: std::time::Duration,
    ) -> Result<(), ClientError> {
        self.reconsume_later_with_properties(retry_producer, msg, Vec::new(), delay)
            .await
    }

    /// Same as [`Self::reconsume_later`] but lets the caller stamp additional custom
    /// properties on the republished message. Custom entries are merged with the original
    /// message's properties — on a key collision, the custom value takes precedence.
    /// Mirrors Java
    /// `Consumer#reconsumeLater(Message, Map<String, String> customProperties, long, TimeUnit)`.
    pub async fn reconsume_later_with_properties(
        &self,
        retry_producer: &crate::Producer,
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
        // Bump the RECONSUMETIMES property if present, otherwise stamp it at 1. Mirrors
        // the Java retry-letter convention so downstream consumers can enforce caps.
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
        // Stamp REAL_TOPIC + ORIGINAL_MESSAGE_ID like the DLQ republish does so consumers
        // of the retry topic can correlate back to the source.
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
        // Set deliver_at_time so the broker queues the message for `delay` past now.
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

    /// Mirrors Java `Consumer#isInactive`. Returns `true` once the consumer has reached
    /// end-of-topic on its subscription (no more messages will be dispatched). Note: a
    /// closed consumer is not represented as "inactive" here; check the connection state
    /// machine if you need to detect close.
    pub fn is_inactive(&self) -> bool {
        self.has_reached_end_of_topic()
    }

    /// Receive up to `max_messages` messages in one call. Mirrors Java
    /// `Consumer#batchReceive`. Waits up to `max_wait` for the first message, then drains any
    /// additional already-buffered messages without further waiting.
    ///
    /// Returns an empty `Vec` if the timeout elapses with no messages.
    pub async fn receive_batch(
        &self,
        max_messages: usize,
        max_wait: std::time::Duration,
    ) -> Result<Vec<IncomingMessage>, ClientError> {
        self.receive_batch_with_bytes_cap(max_messages, usize::MAX, max_wait)
            .await
    }

    /// Same as [`Self::receive_batch`] but stops once the accumulated payload size would
    /// exceed `max_bytes`. Mirrors Java's `BatchReceivePolicy` — the broker-side policy
    /// supports three caps (max messages, max bytes, max wait) and stops on whichever
    /// fires first. Pass `usize::MAX` to disable a cap. The first message is always
    /// included even if it alone exceeds `max_bytes` (matches Java's "deliver at least
    /// one" semantic), but subsequent ones obey the cap strictly.
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
            // Peek at the next message's payload size; if popping it would exceed the
            // byte cap, leave it for the next batch.
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
            let Some(mut msg) = msg else { break };
            // PIP-4: honor the per-consumer crypto failure action for any encrypted message
            // popped here (the first message went through `receive()` which already does
            // this).
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
                    let mut conn = self.shared.inner.lock();
                    let _ = conn.ack(
                        self.handle,
                        magnetar_proto::AckRequest {
                            message_ids: vec![msg.message_id],
                            ack_type: magnetar_proto::pb::command_ack::AckType::Individual,
                            properties: Vec::new(),
                            txn_id: None,
                        },
                    );
                    drop(conn);
                    self.shared.driver_waker.notify_one();
                }
                PostProcessOutcome::Fail(err) => return Err(err),
            }
        }
        Ok(out)
    }
}

/// Outcome returned by [`post_process_message`].
#[derive(Debug)]
enum PostProcessOutcome {
    /// The message is ready for the caller (plaintext, or — under `Consume` — ciphertext).
    Deliver,
    /// Decryption failed and the policy is [`magnetar_proto::CryptoFailureAction::Discard`].
    /// The caller should ack the message and continue.
    Discard,
    /// Either decryption failed and the policy is `Fail`, or another step (decompression,
    /// unknown compression code) hit an unrecoverable error. The caller should surface this
    /// error.
    Fail(ClientError),
}

/// Apply the consumer-side PIP-4 decryption + decompression pipeline to a message popped
/// straight from the sans-io state machine. Mirrors the inline logic in [`ReceiveFut::poll`].
///
/// **Order matters: decrypt FIRST, decompress SECOND.** The tokio producer applies the
/// inverse order on send — `compression → encryption` (see
/// `magnetar-runtime-tokio/src/producer.rs:194-230`, which mirrors Java
/// `ProducerImpl.java:986-1003`) — so the wire payload is ciphertext wrapping the
/// compressed bytes. Decompressing first would feed ciphertext into the codec and fail
/// (silently bypassing the PIP-4 [`CryptoFailureAction`] policy). Decrypt first to
/// recover the compressed plaintext, then decompress to get the user payload.
///
/// `crypto_failure_action` governs what happens when the decryption step fails (see
/// [`magnetar_proto::CryptoFailureAction`]).
fn post_process_message(
    msg: &mut IncomingMessage,
    decryptor: Option<&Arc<dyn crate::crypto::MessageDecryptor>>,
    crypto_failure_action: magnetar_proto::CryptoFailureAction,
) -> PostProcessOutcome {
    // Step 1 — PIP-4 decryption (outermost wrapper on the wire).
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
                    // to the caller that the bytes are still encrypted. We deliberately skip
                    // decompression too — the bytes are still ciphertext, no codec could
                    // interpret them.
                    return PostProcessOutcome::Deliver;
                }
            },
        }
    }
    // Step 2 — decompression (compression was applied first on send, so it lives INSIDE
    // the encryption envelope). After step 1 `msg.payload` is compressed plaintext.
    if let Some(kind_i32) = msg.metadata.compression {
        let Ok(pb_kind) = magnetar_proto::pb::CompressionType::try_from(kind_i32) else {
            return PostProcessOutcome::Fail(ClientError::Other(format!(
                "unknown compression code {kind_i32}"
            )));
        };
        let kind = crate::compress::kind_from_pb(pb_kind);
        if kind != magnetar_proto::types::CompressionKind::None {
            let expected = msg
                .metadata
                .uncompressed_size
                .map_or(msg.payload.len(), |s| s as usize);
            match crate::compress::decompress(kind, &msg.payload, expected) {
                Ok(plain) => msg.payload = plain,
                Err(err) => {
                    return PostProcessOutcome::Fail(ClientError::Other(format!(
                        "decompress: {err}"
                    )));
                }
            }
        }
    }
    PostProcessOutcome::Deliver
}

/// Future returned by [`Consumer::receive`].
///
/// Parks on the per-consumer waker slab exposed by
/// [`magnetar_proto::Connection::register_consumer_receive_waker`]. On drop,
/// the future evicts its slot via
/// [`magnetar_proto::Connection::cancel_consumer_receive_waker`] so cancelled
/// receives don't leak entries until the next arrival.
#[derive(Debug)]
pub struct ReceiveFut {
    shared: Arc<ConnectionShared>,
    handle: ConsumerHandle,
    decryptor: Option<Arc<dyn crate::crypto::MessageDecryptor>>,
    /// Slab key of the currently-installed waker, if any. `Some` once we've
    /// registered on the per-consumer slab; cleared on cancel / wake.
    slab_key: Option<usize>,
}

impl Drop for ReceiveFut {
    fn drop(&mut self) {
        if let Some(key) = self.slab_key.take() {
            // Cancel the slab registration so a future arrival doesn't wake a
            // dropped task. Idempotent on the proto side.
            let mut conn = self.shared.inner.lock();
            conn.cancel_consumer_receive_waker(self.handle, key);
        }
    }
}

impl Future for ReceiveFut {
    type Output = Result<IncomingMessage, ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Loop so that PIP-4 `Discard` can ack the undecryptable message and immediately try
        // the next queued one without bouncing back to the executor — otherwise the caller
        // would need to re-poll just to skip a single dropped message.
        let this = self.get_mut();
        let handle = this.handle;
        let shared = this.shared.clone();
        loop {
            let mut conn = shared.inner.lock();
            let Some(mut msg) = conn.pop_message(handle) else {
                // No message ready. Install (or refresh) our per-consumer waker
                // slab slot so the state machine wakes us when a new
                // `CommandMessage` arrives. If the consumer has been closed
                // since we last polled, register_consumer_receive_waker returns
                // `None` and we surface the terminal state immediately.
                if let Some(old_key) = this.slab_key.take() {
                    conn.cancel_consumer_receive_waker(handle, old_key);
                }
                if let Some(key) = conn.register_consumer_receive_waker(handle, cx.waker().clone())
                {
                    // Re-check the queue under the lock so an arrival that
                    // landed between the pop_message above and the slab
                    // insert doesn't wake a (now-evicted) earlier slot.
                    if conn.peek_message_payload_size(handle).is_some() {
                        // Cancel our just-installed slot and loop to pop.
                        conn.cancel_consumer_receive_waker(handle, key);
                        continue;
                    }
                    this.slab_key = Some(key);
                    drop(conn);
                    return Poll::Pending;
                }
                // Consumer removed (closed connection). Fall through to the
                // closed-handling path: a follow-up poll will observe the
                // closed event and resolve.
                drop(conn);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            };
            // We popped a message; clear any stale slab registration.
            if let Some(key) = this.slab_key.take() {
                conn.cancel_consumer_receive_waker(handle, key);
            }
            drop(conn);
            // Decrypt FIRST, then decompress. The producer applies the inverse order on send
            // — `compression → encryption` (see `producer::Producer::send` and Java
            // `ProducerImpl.java:986-1003`) — so the wire payload is ciphertext wrapping the
            // compressed bytes. Decompressing first would feed ciphertext into the codec and
            // fail silently (bypassing the PIP-4 `CryptoFailureAction` policy entirely).
            // Step 1 — PIP-4 decryption (outermost wrapper on the wire). The decryption
            // failure policy is per-consumer (PIP-4); we resolve it now — before attempting
            // decrypt — so even the "no decryptor configured" path can honor
            // `Discard` / `Consume` instead of unconditionally failing.
            if !msg.metadata.encryption_keys.is_empty() {
                let action = shared.inner.lock().consumer_crypto_failure_action(handle);
                let decrypt_result: Result<bytes::Bytes, ClientError> =
                    match this.decryptor.as_ref() {
                        Some(decryptor) => decryptor
                            .decrypt(&msg.payload, &msg.metadata)
                            .map_err(|err| ClientError::Other(format!("decrypt: {err}"))),
                        None => Err(ClientError::Other(
                            "received encrypted message but consumer has no decryptor configured"
                                .to_owned(),
                        )),
                    };
                match decrypt_result {
                    Ok(plaintext) => {
                        msg.payload = plaintext;
                    }
                    Err(err) => match action {
                        magnetar_proto::CryptoFailureAction::Fail => {
                            return Poll::Ready(Err(err));
                        }
                        magnetar_proto::CryptoFailureAction::Discard => {
                            // Ack the undecryptable message so the broker doesn't redeliver it
                            // (the only consumer of this subscription couldn't read it
                            // anyway), then loop to try the next queued message. Mirrors
                            // Java's `ConsumerImpl#decryptPayloadIfNeeded` which calls
                            // `discardMessage(...)` (an explicit ack) when the policy is
                            // `DISCARD`.
                            let mut conn = shared.inner.lock();
                            let _ = conn.ack(
                                handle,
                                magnetar_proto::AckRequest {
                                    message_ids: vec![msg.message_id],
                                    ack_type: magnetar_proto::pb::command_ack::AckType::Individual,
                                    properties: Vec::new(),
                                    txn_id: None,
                                },
                            );
                            drop(conn);
                            shared.driver_waker.notify_one();
                            continue;
                        }
                        magnetar_proto::CryptoFailureAction::Consume => {
                            // Hand the ciphertext + `encryption_keys` metadata back to the
                            // caller untouched, so they can attempt out-of-band decryption.
                            // We deliberately SKIP decompression — the bytes are still
                            // ciphertext, no codec could interpret them.
                            return Poll::Ready(Ok(msg));
                        }
                    },
                }
            }
            // Step 2 — decompression. Compression was applied first on send, so it lives
            // INSIDE the encryption envelope: after step 1 the payload is compressed
            // plaintext. `uncompressed_size` is mandatory when `compression` is set (per
            // `MessageMetadata` semantics); if it is absent we treat the payload as
            // already-plain bytes.
            if let Some(kind_i32) = msg.metadata.compression {
                let pb_kind =
                    magnetar_proto::pb::CompressionType::try_from(kind_i32).map_err(|_| {
                        ClientError::Other(format!(
                            "unknown compression code {kind_i32} on inbound message"
                        ))
                    })?;
                let kind = crate::compress::kind_from_pb(pb_kind);
                if kind != magnetar_proto::types::CompressionKind::None {
                    let expected_size = msg
                        .metadata
                        .uncompressed_size
                        .map_or(msg.payload.len(), |s| s as usize);
                    let plain = crate::compress::decompress(kind, &msg.payload, expected_size)
                        .map_err(|err| ClientError::Other(format!("decompress: {err}")))?;
                    msg.payload = plain;
                }
            }
            return Poll::Ready(Ok(msg));
        }
    }
}

struct RequestFut {
    shared: Arc<ConnectionShared>,
    key: PendingOpKey,
}

impl Future for RequestFut {
    type Output = OpOutcome;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut conn = self.shared.inner.lock();
        if let Some(outcome) = conn.take_outcome(self.key) {
            drop(conn);
            return Poll::Ready(outcome);
        }
        conn.register_waker(self.key, cx.waker().clone());
        Poll::Pending
    }
}

/// Compare two message ids lexicographically by `(ledger_id, entry_id, partition, batch_index)`.
/// Returns `true` iff `lhs` is strictly greater than `rhs` (i.e. is from a later position in the
/// log). Matches Java's `MessageId#compareTo` semantics.
fn message_id_greater(lhs: &MessageId, rhs: &MessageId) -> bool {
    (lhs.ledger_id, lhs.entry_id, lhs.partition, lhs.batch_index)
        > (rhs.ledger_id, rhs.entry_id, rhs.partition, rhs.batch_index)
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use bytes::BytesMut;
    use magnetar_proto::{ConnectionConfig, SubscribeRequest, encode_command, encode_payload, pb};

    use super::Consumer;
    use crate::ConnectionShared;
    use crate::error::ClientError;

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

    const XOR_KEY: u8 = 0x5A;

    /// Build a `CommandMessage` whose metadata carries PIP-4 `encryption_keys`
    /// (and an XOR-ciphertext body). Mirrors what the producer-side
    /// `XorEncryptor` stamps. 1:1 with the moonpool consumer test helper.
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

    /// Feed an encrypted message into a freshly-subscribed consumer.
    /// Returns the live `(shared, handle, slot)` so the caller can build a
    /// `Consumer` with whatever decryptor / failure-action it wants.
    fn subscribe_with_encrypted_message(
        action: pb::command_subscribe::SubType,
        crypto_failure_action: magnetar_proto::CryptoFailureAction,
        plaintext: &[u8],
    ) -> (
        std::sync::Arc<ConnectionShared>,
        magnetar_proto::ConsumerHandle,
        std::sync::Arc<magnetar_proto::ConsumerSlot>,
    ) {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/crypto".to_owned(),
                subscription: "s".to_owned(),
                sub_type: action,
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
        let slot = consumer_slot_for(&shared, handle);
        (shared, handle, slot)
    }

    /// Happy path: a decryptor that reverses the XOR ciphertext yields the
    /// original plaintext. 1:1 with the moonpool
    /// `receive_decrypts_encrypted_message`.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_decrypts_encrypted_message() {
        let (shared, handle, slot) = subscribe_with_encrypted_message(
            pb::command_subscribe::SubType::Exclusive,
            magnetar_proto::CryptoFailureAction::Fail,
            b"top-secret",
        );
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: Some(std::sync::Arc::new(XorDecryptor)),
        };
        let msg = consumer.receive().await.expect("decrypted receive");
        assert_eq!(msg.payload.as_ref(), b"top-secret");
    }

    /// `CryptoFailureAction::Fail`: a failing decryptor surfaces the error.
    /// 1:1 with the moonpool `receive_crypto_failure_fail_surfaces_error`.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_crypto_failure_fail_surfaces_error() {
        let (shared, handle, slot) = subscribe_with_encrypted_message(
            pb::command_subscribe::SubType::Exclusive,
            magnetar_proto::CryptoFailureAction::Fail,
            b"opaque",
        );
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: Some(std::sync::Arc::new(AlwaysFailDecryptor)),
        };
        let res = consumer.receive().await;
        assert!(
            matches!(res, Err(ClientError::Other(_))),
            "Fail policy must surface a decrypt error, got {res:?}"
        );
    }

    /// `CryptoFailureAction::Consume`: the ciphertext + encryption metadata are
    /// handed back as-is. 1:1 with the moonpool
    /// `receive_crypto_failure_consume_returns_ciphertext`.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_crypto_failure_consume_returns_ciphertext() {
        let plaintext = b"distinctive-payload";
        let (shared, handle, slot) = subscribe_with_encrypted_message(
            pb::command_subscribe::SubType::Exclusive,
            magnetar_proto::CryptoFailureAction::Consume,
            plaintext,
        );
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: Some(std::sync::Arc::new(AlwaysFailDecryptor)),
        };
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
    /// moonpool `receive_crypto_failure_discard_skips_message`.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_crypto_failure_discard_skips_message() {
        let (shared, handle, slot) = subscribe_with_encrypted_message(
            pb::command_subscribe::SubType::Exclusive,
            magnetar_proto::CryptoFailureAction::Discard,
            b"undecryptable",
        );
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: Some(std::sync::Arc::new(AlwaysFailDecryptor)),
        };
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
    /// 1:1 with the moonpool `receive_encrypted_without_decryptor_fails`.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_encrypted_without_decryptor_fails() {
        let (shared, handle, slot) = subscribe_with_encrypted_message(
            pb::command_subscribe::SubType::Exclusive,
            magnetar_proto::CryptoFailureAction::Fail,
            b"secret",
        );
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: None,
        };
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
    /// the moonpool `consumer_clone_preserves_decryptor`.
    #[tokio::test(flavor = "current_thread")]
    async fn consumer_clone_preserves_decryptor() {
        let (shared, handle, slot) = subscribe_with_encrypted_message(
            pb::command_subscribe::SubType::Exclusive,
            magnetar_proto::CryptoFailureAction::Fail,
            b"clone-secret",
        );
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: Some(std::sync::Arc::new(XorDecryptor)),
        };
        let clone = consumer.clone();
        let msg = clone.receive().await.expect("clone decrypts");
        assert_eq!(msg.payload.as_ref(), b"clone-secret");
    }

    fn handshake_complete_shared() -> std::sync::Arc<ConnectionShared> {
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

    /// Capture the per-slot Arc for a `handle` known to be in the registry.
    fn consumer_slot_for(
        shared: &std::sync::Arc<ConnectionShared>,
        handle: magnetar_proto::ConsumerHandle,
    ) -> std::sync::Arc<magnetar_proto::ConsumerSlot> {
        shared
            .inner
            .lock()
            .consumer(handle)
            .cloned()
            .expect("test consumer slot must exist")
    }

    /// Placeholder slot for tests that intentionally hold an unknown handle.
    fn stub_consumer_slot_for_test(
        handle: magnetar_proto::ConsumerHandle,
    ) -> std::sync::Arc<magnetar_proto::ConsumerSlot> {
        magnetar_proto::ConsumerSlot::new(
            magnetar_proto::ConsumerIdentity {
                handle,
                topic: String::new(),
                subscription: String::new(),
            },
            magnetar_proto::consumer::ConsumerState::new(handle, String::new(), String::new(), 0),
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drain_messages_with_zero_returns_empty_vec() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/drain-zero".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let slot = consumer_slot_for(&shared, handle);
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: None,
        };
        assert!(
            consumer.drain_messages(0).is_empty(),
            "max=0 must short-circuit to an empty vec"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drain_messages_with_unknown_handle_returns_empty() {
        // Even when the underlying handle is unknown to the sans-io layer (e.g. closed
        // consumer), `drain_messages` returns an empty `Vec` rather than panicking.
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let bogus_handle = magnetar_proto::ConsumerHandle(9999);
        let consumer = Consumer {
            shared,
            handle: bogus_handle,
            slot: stub_consumer_slot_for_test(bogus_handle),
            decryptor: None,
        };
        assert!(consumer.drain_messages(10).is_empty());
    }

    fn get_schema_response_bytes(request_id: u64, schema: Option<pb::Schema>) -> BytesMut {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::GetSchemaResponse as i32,
            get_schema_response: Some(pb::CommandGetSchemaResponse {
                request_id,
                schema,
                schema_version: Some(bytes::Bytes::from_static(b"v1")),
                error_code: None,
                error_message: None,
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode CommandGetSchemaResponse");
        buf
    }

    fn get_schema_error_bytes(request_id: u64, code: i32, message: &str) -> BytesMut {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::GetSchemaResponse as i32,
            get_schema_response: Some(pb::CommandGetSchemaResponse {
                request_id,
                schema: None,
                schema_version: None,
                error_code: Some(code),
                error_message: Some(message.to_owned()),
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode CommandGetSchemaResponse error");
        buf
    }

    #[tokio::test(flavor = "current_thread")]
    async fn consumer_get_schema_round_trip_resolves_with_cached_schema() {
        // End-to-end: Consumer::get_schema issues a CommandGetSchema, the broker replies with a
        // CommandGetSchemaResponse, and the future resolves with the broker-resolved pb::Schema.
        // Mirrors the PIP-87 runtime path used by AutoConsumeSchema's on-first-receive lookup.
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/auto-schema-ok".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer = Consumer {
            shared: shared.clone(),
            handle,
            slot: consumer_slot_for(&shared, handle),
            decryptor: None,
        };

        let request_id = shared.inner.lock().peek_next_request_id_for_test();
        let response_schema = pb::Schema {
            name: "persistent://public/default/auto-schema-ok-schema".to_owned(),
            schema_data: bytes::Bytes::from_static(
                b"{\"type\":\"record\",\"name\":\"X\",\"fields\":[]}",
            ),
            r#type: pb::schema::Type::Avro as i32,
            properties: Vec::new(),
        };

        // Spawn a task that injects the broker response once the request has been registered.
        let injector_shared = shared.clone();
        let injector_schema = response_schema.clone();
        let injector = tokio::spawn(async move {
            // Yield until the get_schema future has registered the pending request — then feed
            // the response back through `handle_bytes`. Bounded retries so we don't spin forever
            // if the wiring breaks.
            for _ in 0..32 {
                tokio::task::yield_now().await;
                let has_pending = injector_shared
                    .inner
                    .lock()
                    .has_pending_request_for_test(magnetar_proto::RequestId(request_id));
                if has_pending {
                    let frame = get_schema_response_bytes(request_id, Some(injector_schema));
                    injector_shared
                        .inner
                        .lock()
                        .handle_bytes(Instant::now(), &frame)
                        .expect("handle CommandGetSchemaResponse");
                    return;
                }
            }
            panic!("pending get_schema request was never registered");
        });

        let resolved = consumer
            .get_schema(None)
            .await
            .expect("get_schema resolves with broker reply");
        injector.await.expect("injector task completes");

        assert_eq!(resolved.name, response_schema.name);
        assert_eq!(resolved.schema_data, response_schema.schema_data);
        assert_eq!(resolved.r#type, response_schema.r#type);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn consumer_get_schema_surfaces_broker_error() {
        // Error path: broker returns CommandGetSchemaResponse with error_code set —
        // Consumer::get_schema surfaces a ClientError::Broker carrying both code and message.
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/auto-schema-missing".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        let consumer = Consumer {
            shared: shared.clone(),
            handle,
            slot: consumer_slot_for(&shared, handle),
            decryptor: None,
        };

        let request_id = shared.inner.lock().peek_next_request_id_for_test();
        let injector_shared = shared.clone();
        let injector = tokio::spawn(async move {
            for _ in 0..32 {
                tokio::task::yield_now().await;
                let has_pending = injector_shared
                    .inner
                    .lock()
                    .has_pending_request_for_test(magnetar_proto::RequestId(request_id));
                if has_pending {
                    let frame = get_schema_error_bytes(request_id, 13, "TopicNotFound");
                    injector_shared
                        .inner
                        .lock()
                        .handle_bytes(Instant::now(), &frame)
                        .expect("handle CommandGetSchemaResponse error");
                    return;
                }
            }
            panic!("pending get_schema request was never registered");
        });

        let err = consumer
            .get_schema(None)
            .await
            .expect_err("get_schema must surface broker error");
        injector.await.expect("injector task completes");
        match err {
            crate::error::ClientError::Broker { code, message } => {
                assert_eq!(
                    code, 13,
                    "code propagates from CommandGetSchemaResponse.error_code"
                );
                assert_eq!(
                    message, "TopicNotFound",
                    "message propagates from CommandGetSchemaResponse.error_message"
                );
            }
            other => panic!("expected ClientError::Broker, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drain_messages_respects_the_cap_when_messages_are_in_flight() {
        // Pump CommandMessage frames through `handle_bytes` to materialise per-consumer
        // delivery state, then assert the drain never exceeds the requested cap. Even if
        // the burst-emit shape of `handle_bytes` routes the message bytes through the
        // events queue (current scaffolding behavior, see conn.rs:825-833), the
        // cardinality invariant `len <= max` must hold — that's the safety guarantee
        // callers rely on.
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/drain-cap".to_owned(),
                subscription: "s".to_owned(),
                ..Default::default()
            })
        };
        {
            let mut conn = shared.inner.lock();
            for i in 0..5_u64 {
                let bytes = command_message_bytes(handle.0, 100 + i, format!("m{i}").as_bytes());
                conn.handle_bytes(Instant::now(), &bytes)
                    .expect("handle CommandMessage");
            }
        }

        let slot = consumer_slot_for(&shared, handle);
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: None,
        };

        for cap in [0_usize, 1, 3, 100] {
            let drained = consumer.drain_messages(cap);
            assert!(
                drained.len() <= cap,
                "drain_messages({cap}) returned {} items, exceeds the cap",
                drained.len()
            );
        }
    }

    // ── per-consumer waker slab ───────────────────────────────────────────

    /// Two concurrent `receive()` futures on the same consumer must both
    /// resolve when two messages arrive — neither's waker may be clobbered
    /// by the other's registration. Pre-slab implementation, this hung
    /// indefinitely because the single `Option<Waker>` field on
    /// `ConsumerState` was overwritten by the second poll.
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
        let consumer = Consumer {
            shared: shared.clone(),
            handle,
            slot: consumer_slot_for(&shared, handle),
            decryptor: None,
        };

        // Spawn two parallel receive tasks before any message arrives so
        // both park on the slab.
        let c1 = consumer.clone();
        let c2 = consumer.clone();
        let t1 = tokio::spawn(async move { c1.receive().await });
        let t2 = tokio::spawn(async move { c2.receive().await });
        // Yield so the spawned tasks actually poll and register on the slab.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Slab should hold both registrations now.
        {
            let conn = shared.inner.lock();
            let slot = conn.consumer(handle).expect("consumer still alive");
            assert_eq!(
                slot.state.lock().receive_wakers.len(),
                2,
                "both in-flight receives must be parked on the slab",
            );
        }

        // Deliver two messages; both receives must resolve.
        {
            let mut conn = shared.inner.lock();
            for i in 0..2 {
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
    /// so a later arrival doesn't wake a dead task and leak the entry.
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
        let consumer = Consumer {
            shared: shared.clone(),
            handle,
            slot: consumer_slot_for(&shared, handle),
            decryptor: None,
        };

        let c1 = consumer.clone();
        let task = tokio::spawn(async move { c1.receive().await });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Slab must have exactly one registration now.
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

        // Cancel the receive; the Drop impl must evict the slab slot.
        task.abort();
        let _ = task.await; // wait for the cancel to actually run Drop
        // Give the runtime a tick to settle.
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

    /// `ack_grouped` is fire-and-forget; with no `ack_group_time` tracker
    /// configured the proto layer falls back to a synchronous immediate
    /// `CommandAck`. Calling it on a fresh consumer must NOT panic and
    /// MUST leave the consumer registered. ADR-0024 1:1 mirror.
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
        let consumer = Consumer {
            shared: shared.clone(),
            handle,
            slot: consumer_slot_for(&shared, handle),
            decryptor: None,
        };
        consumer.ack_grouped(magnetar_proto::MessageId {
            ledger_id: 1,
            entry_id: 0,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        });
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
        let consumer = Consumer {
            shared: shared.clone(),
            handle,
            slot: consumer_slot_for(&shared, handle),
            decryptor: None,
        };
        consumer.ack_grouped_cumulative(magnetar_proto::MessageId {
            ledger_id: 1,
            entry_id: 5,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
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
        let consumer = Consumer {
            shared: shared.clone(),
            handle,
            slot: consumer_slot_for(&shared, handle),
            decryptor: None,
        };
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
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        let fut = consumer.ack_with_txn(mid, txn);
        let res = tokio::time::timeout(Duration::from_millis(10), fut).await;
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
        let consumer = Consumer {
            shared: shared.clone(),
            handle,
            slot: consumer_slot_for(&shared, handle),
            decryptor: None,
        };
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
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        let fut = consumer.ack_cumulative_with_txn(mid, txn);
        let res = tokio::time::timeout(Duration::from_millis(10), fut).await;
        assert!(res.is_err(), "expected pending future (no driver)");
        assert!(!consumer.is_closed());
    }

    // ── helper-method ports (MultiTopics surface lift, pass-1) ───────────
    //
    // The block below mirrors `crates/magnetar-runtime-moonpool/src/consumer.rs`
    // 1:1 per ADR-0024 §strict test-count parity.

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
        let consumer = Consumer {
            shared: shared.clone(),
            handle,
            slot: consumer_slot_for(&shared, handle),
            decryptor: None,
        };
        assert_eq!(consumer.available_in_queue(), 0);

        {
            let mut conn = shared.inner.lock();
            for i in 0..3_u64 {
                let bytes = command_message_bytes(handle.0, 300 + i, format!("q{i}").as_bytes());
                conn.handle_bytes(Instant::now(), &bytes)
                    .expect("handle CommandMessage");
            }
        }
        let depth = consumer.available_in_queue();
        assert!(depth <= 3, "queue depth must not exceed delivered count");

        let bogus = magnetar_proto::ConsumerHandle(9999);
        let closed = Consumer {
            shared,
            handle: bogus,
            slot: stub_consumer_slot_for_test(bogus),
            decryptor: None,
        };
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
        let consumer = Consumer {
            shared: shared.clone(),
            handle,
            slot: consumer_slot_for(&shared, handle),
            decryptor: None,
        };
        // Right after subscribe, before the initial flow is granted, the
        // counter is zero.
        assert_eq!(consumer.available_permits(), 0);
        // Granting the initial flow bumps the counter to receiver_queue_size.
        {
            let mut conn = shared.inner.lock();
            let _ = conn.initial_flow(handle);
        }
        assert_eq!(consumer.available_permits(), 64);

        let bogus = magnetar_proto::ConsumerHandle(9999);
        let closed = Consumer {
            shared,
            handle: bogus,
            slot: stub_consumer_slot_for_test(bogus),
            decryptor: None,
        };
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
        let consumer = Consumer {
            shared: shared.clone(),
            handle,
            slot: consumer_slot_for(&shared, handle),
            decryptor: None,
        };
        assert!(
            !consumer.has_received_any_message(),
            "fresh consumer must report no messages received",
        );

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
        let consumer = Consumer {
            shared: shared.clone(),
            handle,
            slot: consumer_slot_for(&shared, handle),
            decryptor: None,
        };
        assert!(!consumer.is_paused(), "default state is unpaused");
        consumer.pause();
        assert!(consumer.is_paused(), "after pause()");
        consumer.resume();
        assert!(!consumer.is_paused(), "after resume()");

        let bogus = magnetar_proto::ConsumerHandle(9999);
        let closed = Consumer {
            shared,
            handle: bogus,
            slot: stub_consumer_slot_for_test(bogus),
            decryptor: None,
        };
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
        let slot = consumer_slot_for(&shared, handle);
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: None,
        };
        assert!(
            !consumer.has_reached_end_of_topic(),
            "default state is not end-of-topic",
        );
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
        let slot = consumer_slot_for(&shared, handle);
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: None,
        };
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
        {
            let mut conn = shared.inner.lock();
            let bytes = command_message_bytes(handle.0, 500, b"now");
            conn.handle_bytes(Instant::now(), &bytes)
                .expect("handle CommandMessage");
        }
        let slot = consumer_slot_for(&shared, handle);
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: None,
        };
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
        {
            let mut conn = shared.inner.lock();
            for i in 0..5_u64 {
                let bytes = command_message_bytes(handle.0, 600 + i, format!("b{i}").as_bytes());
                conn.handle_bytes(Instant::now(), &bytes)
                    .expect("handle CommandMessage");
            }
        }
        let slot = consumer_slot_for(&shared, handle);
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: None,
        };
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

    /// Build an encrypted `CommandMessage` whose `encryption_keys[0].key` carries a custom
    /// key name. Lets a test mark individual messages as decryptable vs. undecryptable for a
    /// key-aware decryptor (see [`SelectiveDecryptor`]). 1:1 with the moonpool helper.
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
    /// can exercise the `Discard` skip path in [`Consumer::receive_batch_with_bytes_cap`]. 1:1
    /// with the moonpool helper.
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

    /// Regression mirror for the moonpool batch-receive ciphertext leak: `receive_batch` must
    /// decrypt EVERY message in the batch, not just the first. 1:1 with the moonpool
    /// `receive_batch_decrypts_every_message`.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_batch_decrypts_every_message() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/crypto-batch".to_owned(),
                subscription: "s".to_owned(),
                sub_type: pb::command_subscribe::SubType::Exclusive,
                crypto_failure_action: magnetar_proto::CryptoFailureAction::Fail,
                ..Default::default()
            })
        };
        {
            let mut conn = shared.inner.lock();
            for i in 0..3_u64 {
                let frame =
                    encrypted_message_bytes(handle.0, i, format!("batch-secret-{i}").as_bytes());
                conn.handle_bytes(Instant::now(), &frame)
                    .expect("handle encrypted CommandMessage");
            }
        }
        let slot = consumer_slot_for(&shared, handle);
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: Some(std::sync::Arc::new(XorDecryptor)),
        };
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
    /// it are delivered as plaintext. 1:1 with the moonpool
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
        {
            let mut conn = shared.inner.lock();
            // entry 0: decryptable, entry 1: undecryptable (bad key → Discard), entry 2:
            // decryptable. The middle message must be skipped.
            conn.handle_bytes(
                Instant::now(),
                &encrypted_message_bytes_with_key(handle.0, 0, "xor-test", b"keep-0"),
            )
            .expect("handle msg 0");
            conn.handle_bytes(
                Instant::now(),
                &encrypted_message_bytes_with_key(handle.0, 1, "bad-key", b"drop-1"),
            )
            .expect("handle msg 1");
            conn.handle_bytes(
                Instant::now(),
                &encrypted_message_bytes_with_key(handle.0, 2, "xor-test", b"keep-2"),
            )
            .expect("handle msg 2");
        }
        let slot = consumer_slot_for(&shared, handle);
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: Some(std::sync::Arc::new(SelectiveDecryptor)),
        };
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
        let slot = consumer_slot_for(&shared, handle);
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: None,
        };
        let zero_msgs = consumer
            .receive_batch_with_bytes_cap(0, 1024, std::time::Duration::from_secs(60))
            .await
            .expect("ok");
        assert!(zero_msgs.is_empty());
        let zero_bytes = consumer
            .receive_batch_with_bytes_cap(10, 0, std::time::Duration::from_secs(60))
            .await
            .expect("ok");
        assert!(zero_bytes.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unsubscribe_force_true_round_trips_command_success() {
        // `unsubscribe(true)` issues `CommandUnsubscribe { force: true }`
        // and resolves on `CommandSuccess`. Mirrors
        // `magnetar-runtime-moonpool` 1:1 (ADR-0024).
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
            let consumer = Consumer {
                shared: shared.clone(),
                handle,
                slot: consumer_slot_for(&shared, handle),
                decryptor: None,
            };

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
                        encode_command(&mut buf, &cmd).expect("encode CommandSuccess");
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
        // Mirror of the moonpool engine's identically-named test
        // (ADR-0024). DLQ is empty → helper returns 0 without invoking
        // `Producer::send`.
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
        let consumer = Consumer {
            shared: shared.clone(),
            handle: consumer_handle,
            slot: consumer_slot_for(&shared, consumer_handle),
            decryptor: None,
        };
        let producer_slot = shared
            .inner
            .lock()
            .producer(producer_handle)
            .cloned()
            .expect("producer slot must exist");
        let producer = Producer {
            shared,
            handle: producer_handle,
            slot: producer_slot,
            compression: magnetar_proto::types::CompressionKind::None,
            encryptor: None,
        };
        let count = consumer
            .republish_dead_letters(&producer)
            .await
            .expect("republish_dead_letters must resolve");
        assert_eq!(count, 0, "no DLQ messages present");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reconsume_later_stamps_reconsumetimes_on_first_call() {
        // Mirror of the moonpool engine's identically-named test
        // (ADR-0024). Drives `reconsume_later` against a producer wired
        // into the same shared state; the helper queues the retry-letter
        // publish into the outbox (verified non-empty after one poll
        // cycle).
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
        let consumer = Consumer {
            shared: shared.clone(),
            handle: consumer_handle,
            slot: consumer_slot_for(&shared, consumer_handle),
            decryptor: None,
        };
        let producer_slot = shared
            .inner
            .lock()
            .producer(producer_handle)
            .cloned()
            .expect("producer slot must exist");
        let producer = Producer {
            shared: shared.clone(),
            handle: producer_handle,
            slot: producer_slot,
            compression: magnetar_proto::types::CompressionKind::None,
            encryptor: None,
        };
        let msg = magnetar_proto::IncomingMessage {
            message_id: MessageId {
                ledger_id: 7,
                entry_id: 99,
                partition: -1,
                batch_index: -1,
                batch_size: 0,
                #[cfg(feature = "scalable-topics")]
                segment_id: None,
            },
            payload: Bytes::from_static(b"retryme"),
            metadata: std::sync::Arc::new(magnetar_proto::pb::MessageMetadata::default()),
            single_metadata: None,
            redelivery_count: 0,
            broker_entry_metadata: None,
            arrived_at: Instant::now(),
        };
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

        let pending_publish_bytes = {
            let mut conn = shared.inner.lock();
            conn.poll_transmit().len()
        };
        assert!(
            pending_publish_bytes > 0,
            "reconsume_later must have queued the retry publish",
        );
    }

    /// Regression for the decompress-vs-decrypt ordering bug: the producer
    /// applies `compression → encryption` on send, so the wire payload is
    /// ciphertext-of-compressed-data. The consumer MUST decrypt FIRST and
    /// decompress SECOND. The legacy "decompress first" order would feed
    /// ciphertext into the codec and silently bypass `CryptoFailureAction`.
    ///
    /// This test pins the corrected order by building a wire frame the same
    /// way the producer would: zstd-compress the plaintext, then XOR-encrypt
    /// the result; stamp both `metadata.compression` + `encryption_keys`.
    /// A consumer with the matching XOR decryptor must deliver the original
    /// plaintext.
    ///
    /// Has no 1:1 moonpool mirror: the moonpool producer refuses any non-`None`
    /// compression on send, so the moonpool consumer's `post_process_message`
    /// has no decompression branch and nothing to reorder. The parity-count
    /// delta is compensated by `moonpool_helper_handles_encrypted_only_payload`
    /// on the moonpool side.
    #[tokio::test(flavor = "current_thread")]
    async fn receive_decrypts_then_decompresses_compressed_encrypted_payload() {
        use magnetar_proto::types::CompressionKind;

        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.subscribe(SubscribeRequest {
                topic: "persistent://public/default/crypto-compress-order".to_owned(),
                subscription: "s".to_owned(),
                sub_type: pb::command_subscribe::SubType::Exclusive,
                crypto_failure_action: magnetar_proto::CryptoFailureAction::Fail,
                ..Default::default()
            })
        };
        let plaintext: Vec<u8> = b"the-quick-brown-fox-jumps-over-the-lazy-dog-".repeat(8);
        // Mimic producer: compression → encryption.
        let compressed =
            crate::compress::compress(CompressionKind::Zstd, &plaintext).expect("zstd compress");
        let ciphertext: Vec<u8> = compressed.iter().map(|b| b ^ XOR_KEY).collect();

        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Message as i32,
            message: Some(pb::CommandMessage {
                consumer_id: handle.0,
                message_id: pb::MessageIdData {
                    ledger_id: 1,
                    entry_id: 0,
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
            sequence_id: 0,
            publish_time: 1_700_000_000,
            compression: Some(pb::CompressionType::Zstd as i32),
            uncompressed_size: Some(plaintext.len() as u32),
            encryption_keys: vec![pb::EncryptionKeys {
                key: "xor-test".to_owned(),
                value: bytes::Bytes::from_static(b"k"),
                metadata: Vec::new(),
            }],
            encryption_algo: Some("XOR-TEST".to_owned()),
            encryption_param: Some(bytes::Bytes::from_static(b"iv")),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_payload(&mut buf, &cmd, &meta, &ciphertext)
            .expect("encode compressed+encrypted CommandMessage");
        {
            let mut conn = shared.inner.lock();
            conn.handle_bytes(Instant::now(), &buf)
                .expect("handle compressed+encrypted CommandMessage");
        }

        let slot = consumer_slot_for(&shared, handle);
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: Some(std::sync::Arc::new(XorDecryptor)),
        };
        let msg = consumer
            .receive()
            .await
            .expect("decrypt-then-decompress must yield the original plaintext");
        assert_eq!(
            msg.payload.as_ref(),
            plaintext.as_slice(),
            "consumer must decrypt first (ciphertext → compressed plaintext) THEN \
             decompress (compressed plaintext → user plaintext); legacy reverse \
             order would have failed at decompress on raw ciphertext"
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
        let slot = consumer_slot_for(&shared, handle);
        let consumer = Consumer {
            shared,
            handle,
            slot,
            decryptor: None,
        };
        assert!(
            consumer.drain_dead_letter().is_empty(),
            "no messages have been flagged for DLQ yet",
        );
    }
}
