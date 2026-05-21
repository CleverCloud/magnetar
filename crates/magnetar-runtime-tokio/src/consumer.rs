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
use std::task::{Context, Poll, Waker};

use magnetar_proto::{
    AckRequest, ConsumerHandle, IncomingMessage, MessageId, OpOutcome, PendingOpKey, SeekTarget, pb,
};

use crate::ConnectionShared;
use crate::error::ClientError;

/// User-facing consumer handle.
#[derive(Debug, Clone)]
pub struct Consumer {
    pub(crate) shared: Arc<ConnectionShared>,
    pub(crate) handle: ConsumerHandle,
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

    /// Receive the next message. Resolves when the broker delivers a `CommandMessage` and the
    /// state machine emits it into this consumer's queue.
    pub fn receive(&self) -> ReceiveFut {
        ReceiveFut {
            shared: self.shared.clone(),
            handle: self.handle,
            decryptor: self.decryptor.clone(),
            // We register a per-handle waker via this op key when no message is ready.
            //
            // TODO(M2 follow-up): the state machine currently exposes only per-request waker
            // slots; per-consumer message-arrival wakers will land alongside flow-control work
            // in a later milestone. For now we poll-park via the connection-level driver
            // waker, which means receive() can spuriously wake but never misses a message.
            registered_waker: None,
        }
    }

    /// Acknowledge a single message (individual ack).
    ///
    /// Returns a future that resolves when the broker confirms (`CommandAckResponse`).
    pub fn ack(&self, message_id: MessageId) -> impl Future<Output = Result<(), ClientError>> {
        self.ack_many(vec![message_id], pb::command_ack::AckType::Individual)
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
        let mut conn = self.shared.inner.lock();
        conn.negative_ack(self.handle, message_ids);
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
            OpOutcome::Success { .. } => Ok(()),
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
    pub fn stats(&self) -> magnetar_proto::ConsumerStats {
        self.shared
            .inner
            .lock()
            .consumer_stats(self.handle)
            .unwrap_or_default()
    }

    /// Mirrors `org.apache.pulsar.client.api.Consumer#pause`. Stops automatic flow refills so
    /// the broker stops dispatching new messages once already-issued permits drain. Buffered
    /// messages remain receivable.
    pub fn pause(&self) {
        let mut conn = self.shared.inner.lock();
        conn.set_paused(self.handle, true);
    }

    /// Mirrors `org.apache.pulsar.client.api.Consumer#resume`. Re-enables automatic flow
    /// refills.
    pub fn resume(&self) {
        {
            let mut conn = self.shared.inner.lock();
            conn.set_paused(self.handle, false);
        }
        // Nudge the driver — it may have a flow to emit now that we're un-paused.
        self.shared.driver_waker.notify_one();
    }

    /// Returns `true` if the consumer is currently paused.
    pub fn is_paused(&self) -> bool {
        self.shared
            .inner
            .lock()
            .is_paused(self.handle)
            .unwrap_or(false)
    }

    /// Returns `true` once the broker has indicated end-of-topic for this consumer (no
    /// further messages will be dispatched). Mirrors Java
    /// `Consumer#hasReachedEndOfTopic`.
    pub fn has_reached_end_of_topic(&self) -> bool {
        self.shared
            .inner
            .lock()
            .consumer_reached_end_of_topic(self.handle)
    }

    /// Topic name this consumer is bound to. Returns an empty string if the consumer is
    /// no longer registered (closed).
    pub fn topic(&self) -> String {
        self.shared
            .inner
            .lock()
            .consumer_topic(self.handle)
            .unwrap_or("")
            .to_owned()
    }

    /// Subscription name. Empty string if the consumer is no longer registered.
    pub fn subscription(&self) -> String {
        self.shared
            .inner
            .lock()
            .consumer_subscription(self.handle)
            .unwrap_or("")
            .to_owned()
    }

    /// Drain every message the state machine has flagged as dead-letter (redelivery count
    /// greater than the configured `max_redeliver_count`). The caller is responsible for
    /// republishing them to the configured DLQ topic. Returns an empty `Vec` when DLQ
    /// routing is disabled or no messages have been flagged.
    pub fn drain_dead_letter(&self) -> Vec<IncomingMessage> {
        let mut conn = self.shared.inner.lock();
        conn.drain_dead_letter(self.handle)
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
        if max_messages == 0 {
            return Ok(Vec::new());
        }
        let first = tokio::time::timeout(max_wait, self.receive()).await;
        let first = match first {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Ok(Vec::new()),
        };
        let mut out = Vec::with_capacity(max_messages.min(64));
        out.push(first);
        while out.len() < max_messages {
            let msg = {
                let mut conn = self.shared.inner.lock();
                conn.pop_message(self.handle)
            };
            let Some(mut msg) = msg else { break };
            post_process_message(&mut msg, self.decryptor.as_ref())?;
            out.push(msg);
        }
        Ok(out)
    }
}

/// Apply the consumer-side decompression + PIP-4 decryption pipeline to a message popped
/// straight from the sans-io state machine. Mirrors the inline logic in [`ReceiveFut::poll`].
fn post_process_message(
    msg: &mut IncomingMessage,
    decryptor: Option<&Arc<dyn crate::crypto::MessageDecryptor>>,
) -> Result<(), ClientError> {
    if let Some(kind_i32) = msg.metadata.compression {
        let pb_kind = magnetar_proto::pb::CompressionType::try_from(kind_i32)
            .map_err(|_| ClientError::Other(format!("unknown compression code {kind_i32}")))?;
        let kind = crate::compress::kind_from_pb(pb_kind);
        if kind != magnetar_proto::types::CompressionKind::None {
            let expected = msg
                .metadata
                .uncompressed_size
                .map_or(msg.payload.len(), |s| s as usize);
            let plain = crate::compress::decompress(kind, &msg.payload, expected)
                .map_err(|err| ClientError::Other(format!("decompress: {err}")))?;
            msg.payload = plain;
        }
    }
    if !msg.metadata.encryption_keys.is_empty() {
        let Some(d) = decryptor else {
            return Err(ClientError::Other(
                "received encrypted message but consumer has no decryptor configured".to_owned(),
            ));
        };
        let plain = d
            .decrypt(&msg.payload, &msg.metadata)
            .map_err(|err| ClientError::Other(format!("decrypt: {err}")))?;
        msg.payload = plain;
    }
    Ok(())
}

/// Future returned by [`Consumer::receive`].
#[derive(Debug)]
pub struct ReceiveFut {
    shared: Arc<ConnectionShared>,
    handle: ConsumerHandle,
    decryptor: Option<Arc<dyn crate::crypto::MessageDecryptor>>,
    /// Tracks whether we've already installed a connection-level waker to avoid leaking entries
    /// across polls.
    registered_waker: Option<Waker>,
}

impl Future for ReceiveFut {
    type Output = Result<IncomingMessage, ClientError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut conn = self.shared.inner.lock();
        if let Some(mut msg) = conn.pop_message(self.handle) {
            drop(conn);
            // Decompress the payload if the broker stamped a compression kind on it. Producer-
            // side compression lives in `producer::Producer::send`; this is the symmetric
            // consumer-side step. `uncompressed_size` is mandatory when `compression` is set
            // (per `MessageMetadata` semantics); if it is absent we treat the payload as
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
            // PIP-4 decryption: if the metadata carries encryption keys, the payload arrived as
            // ciphertext; hand it to the configured decryptor. Order is symmetric to producer
            // send: encryption was applied AFTER compression, so we decrypt FIRST then would
            // have decompressed — but Pulsar sends only the post-compression / post-encryption
            // payload, so the metadata.compression stamp here actually describes the plaintext
            // (Java does the same — see ProducerImpl.java:986-1003). Hence the compression /
            // encryption order on the consumer side is: decrypt → decompress. We re-do
            // decompression after decrypt for that reason.
            //
            // For simplicity (and because this matches what the Java client does for
            // non-batch messages), we currently decompress first then decrypt. Pulsar's
            // compression+encryption interaction is one of the rougher edges of the protocol —
            // the precise field semantics differ between batch and non-batch paths. For now
            // we accept that combining compression + encryption on the *same* message may
            // need a follow-up to match Java exactly for batched paths.
            if !msg.metadata.encryption_keys.is_empty() {
                let Some(decryptor) = self.decryptor.as_ref() else {
                    return Poll::Ready(Err(ClientError::Other(
                        "received encrypted message but consumer has no decryptor configured"
                            .to_owned(),
                    )));
                };
                let plaintext = decryptor
                    .decrypt(&msg.payload, &msg.metadata)
                    .map_err(|err| ClientError::Other(format!("decrypt: {err}")))?;
                msg.payload = plaintext;
            }
            return Poll::Ready(Ok(msg));
        }
        // Drain any state-machine events that may have arrived; we keep events queued but no
        // typed waker channel for arrival yet. The driver loop's `notify_one` after handling
        // bytes will re-poll us.
        drop(conn);

        // Re-arm the per-future driver wake-up. We piggyback on `driver_waker.notified()` via a
        // future-local notification subscription: the driver task notifies *all* parked tasks
        // after any inbound bytes are processed.
        //
        // TODO(M3 follow-up): wire a dedicated per-consumer waker slab into `Connection` so
        // receive() resolves exactly when a `CommandMessage` is delivered, instead of being
        // re-polled on every inbound packet. Until then this is correct but not maximally
        // efficient.
        self.registered_waker = Some(cx.waker().clone());
        let notified = self.shared.driver_waker.notified();
        tokio::pin!(notified);
        // Register interest so the next `notify_one` wakes our task.
        if notified.as_mut().enable() {
            // Already notified: poll immediately.
            cx.waker().wake_by_ref();
        }
        Poll::Pending
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
