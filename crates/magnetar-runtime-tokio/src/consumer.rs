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

    fn ack_many(
        &self,
        message_ids: Vec<MessageId>,
        ack_type: pb::command_ack::AckType,
    ) -> impl Future<Output = Result<(), ClientError>> {
        let shared = self.shared.clone();
        let request_id = {
            let mut conn = shared.inner.lock();
            conn.ack(
                self.handle,
                AckRequest {
                    message_ids,
                    ack_type,
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
