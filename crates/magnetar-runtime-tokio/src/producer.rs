// SPDX-License-Identifier: Apache-2.0

//! Producer handle exposed to user code.
//!
//! Wraps an [`Arc<ConnectionShared>`](crate::ConnectionShared) and a
//! [`magnetar_proto::ProducerHandle`]. Cheap to clone (Arc bump). User-facing futures lock the
//! shared state machine directly to enqueue sends; the driver task picks the frames up via
//! [`magnetar_proto::Connection::poll_transmit`].

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::types::CompressionKind;
use magnetar_proto::{MessageId, OpOutcome, PendingOpKey, ProducerHandle, SequenceId};

use crate::ConnectionShared;
use crate::crypto::MessageEncryptor;
use crate::error::ClientError;

/// User-facing producer handle.
#[derive(Debug, Clone)]
pub struct Producer {
    pub(crate) shared: Arc<ConnectionShared>,
    pub(crate) handle: ProducerHandle,
    pub(crate) compression: CompressionKind,
    /// Optional encryption hook (PIP-4). When present, the producer encrypts every
    /// outbound payload after compression but before handing it to the sans-io layer.
    pub(crate) encryptor: Option<Arc<dyn MessageEncryptor>>,
}

impl Producer {
    /// The protocol-layer producer handle this façade wraps.
    pub fn handle(&self) -> ProducerHandle {
        self.handle
    }

    /// `true` if this producer has been closed (locally via [`Self::close`] or remotely
    /// via a broker `CloseProducer`). Mirrors Java `ProducerImpl#getState() == CLOSED`.
    /// Use [`Self::is_connected`] for the live test — `is_closed` only flips after a
    /// terminal close, not on transient disconnects.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.shared.inner.lock().producer_is_closed(self.handle)
    }

    /// Last sequence id this client has pushed onto the wire. Returns `-1` if the producer
    /// has never sent. Mirrors `org.apache.pulsar.client.api.Producer#getLastSequenceId`.
    pub fn last_sequence_id(&self) -> i64 {
        self.shared
            .inner
            .lock()
            .producer_last_sequence_id_pushed(self.handle)
    }

    /// Number of in-flight sends (queued and not yet acked by the broker). Mirrors the
    /// un-batched view of Java `ProducerStats#getPendingQueueSize`. Equivalent to
    /// `self.stats().pending_queue_size as usize` but spares the full stats snapshot.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.shared.inner.lock().producer_pending_count(self.handle)
    }

    /// Number of messages currently buffered in the batch container, waiting for the next
    /// flush cycle. Returns `0` when batching is disabled or the batch is empty.
    #[must_use]
    pub fn batch_len(&self) -> usize {
        self.shared.inner.lock().producer_batch_len(self.handle)
    }

    /// Sum of payload bytes currently buffered in the batch container.
    #[must_use]
    pub fn batch_bytes(&self) -> usize {
        self.shared.inner.lock().producer_batch_bytes(self.handle)
    }

    /// Last sequence id the broker has acknowledged via `CommandSendReceipt`. Returns `-1`
    /// if no sends have been acked yet. Useful for resume-from-checkpoint flows.
    pub fn last_sequence_id_published(&self) -> i64 {
        self.shared
            .inner
            .lock()
            .producer_last_sequence_id_published(self.handle)
    }

    /// Convenience: publish raw payload bytes with no extra metadata. Mirrors Java
    /// `Producer#sendAsync(byte[])`. For richer metadata (keys, properties, deliver-at,
    /// etc.) construct an [`OutgoingMessage`] explicitly and call [`Self::send`].
    pub fn send_bytes(&self, payload: impl Into<bytes::Bytes>) -> SendFut {
        let payload = payload.into();
        let uncompressed_size = u32::try_from(payload.len()).unwrap_or(u32::MAX);
        self.send(OutgoingMessage {
            payload,
            metadata: magnetar_proto::pb::MessageMetadata::default(),
            uncompressed_size,
            num_messages: 1,
            txn_id: None,
        })
    }

    /// Enqueue a send. The returned future resolves when the broker acknowledges the publish
    /// (a `CommandSendReceipt`) or rejects it (a `CommandSendError`).
    pub fn send(&self, mut msg: OutgoingMessage) -> SendFut {
        let publish_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);

        // Compress the payload before handing it to the sans-io state machine. The producer
        // state machine stamps `metadata.compression` based on its configured CompressionKind
        // (per ProducerImpl.java:581-608); here we run the actual codec. Compression failure
        // bubbles up as a SendError so the caller can retry or surface to the user.
        if self.compression != CompressionKind::None {
            match crate::compress::compress(self.compression, &msg.payload) {
                Ok(compressed) => {
                    msg.uncompressed_size = u32::try_from(msg.payload.len()).unwrap_or(u32::MAX);
                    msg.payload = compressed;
                }
                Err(err) => {
                    return SendFut {
                        shared: self.shared.clone(),
                        handle: self.handle,
                        state: SendState::Failed {
                            error: Some(ClientError::Other(format!("compress: {err}"))),
                        },
                    };
                }
            }
        }

        // Encrypt the (compressed) payload if a PIP-4 encryptor is wired. Mirrors the Java
        // `ProducerImpl.java:986-1003` ordering — compression first, encryption second so the
        // broker sees ciphertext and the consumer reverses the order on receive.
        if let Some(encryptor) = self.encryptor.as_ref() {
            match encryptor.encrypt(&msg.payload, &mut msg.metadata) {
                Ok(ciphertext) => msg.payload = ciphertext,
                Err(err) => {
                    return SendFut {
                        shared: self.shared.clone(),
                        handle: self.handle,
                        state: SendState::Failed {
                            error: Some(ClientError::Other(format!("encrypt: {err}"))),
                        },
                    };
                }
            }
        }

        let result = {
            let mut conn = self.shared.inner.lock();
            conn.send(self.handle, msg, publish_time_ms)
        };

        // Wake the driver so it can drain the freshly-queued frame.
        self.shared.driver_waker.notify_one();

        SendFut {
            shared: self.shared.clone(),
            handle: self.handle,
            state: match result {
                Ok(seq) => SendState::Pending { sequence_id: seq },
                Err(err) => SendState::Failed {
                    error: Some(ClientError::Protocol(err)),
                },
            },
        }
    }

    /// Flush this producer: force any pending batch to flush and wait for every in-flight
    /// send to be acknowledged by the broker. Idempotent — calling `flush()` on a quiescent
    /// producer returns immediately.
    ///
    /// Mirrors `org.apache.pulsar.client.api.Producer#flushAsync`. Use before `close()` if
    /// you want at-least-once semantics on the trailing sends.
    pub async fn flush(&self) -> Result<(), ClientError> {
        let publish_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        {
            let mut conn = self.shared.inner.lock();
            conn.flush_producer(self.handle, publish_time_ms);
        }
        self.shared.driver_waker.notify_one();

        // Drain by waiting on the driver waker until the producer's pending queue is empty.
        // The driver task notifies all parked tasks after every inbound packet, so each
        // `CommandSendReceipt` wakes us; we re-check the count and re-park if needed.
        loop {
            let pending = self.shared.inner.lock().producer_pending_count(self.handle);
            if pending == 0 {
                return Ok(());
            }
            let notified = self.shared.driver_waker.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            notified.await;
        }
    }

    /// Close this producer. The returned future resolves when the broker acknowledges the close.
    ///
    /// # Errors
    ///
    /// - [`ClientError::Broker`] if the broker returns an error correlating to the close.
    pub async fn close(self) -> Result<(), ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.close_producer(self.handle)
        };
        self.shared.driver_waker.notify_one();
        wait_request(&self.shared, request_id).await
    }

    /// Mirrors `org.apache.pulsar.client.api.Producer#isConnected`. Returns `true` while the
    /// underlying broker connection is up (the producer itself does not maintain a separate
    /// session — it lives on the shared client connection).
    pub fn is_connected(&self) -> bool {
        self.shared.inner.lock().is_connected()
    }

    /// Mirrors `org.apache.pulsar.client.api.Producer#getLastDisconnectedTimestamp`: wall-clock
    /// time at which the underlying connection most recently went down. `None` if the
    /// connection has never been disconnected.
    pub fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        self.shared.inner.lock().last_disconnected_timestamp()
    }

    /// Snapshot of this producer's cumulative counters. Mirrors Java
    /// `org.apache.pulsar.client.api.Producer#getStats`. Returns a zeroed snapshot if the
    /// producer handle is no longer registered (closed).
    pub fn stats(&self) -> magnetar_proto::ProducerStats {
        self.shared
            .inner
            .lock()
            .producer_stats(self.handle)
            .unwrap_or_default()
    }

    /// Topic name this producer is bound to. Returns an empty string if the producer is no
    /// longer registered (closed).
    pub fn topic(&self) -> String {
        self.shared
            .inner
            .lock()
            .producer_topic(self.handle)
            .unwrap_or("")
            .to_owned()
    }

    /// Broker-assigned producer name. Returns an empty string until the broker assigns one
    /// (typically right after the ProducerSuccess round-trip) or if the producer is no
    /// longer registered.
    pub fn name(&self) -> String {
        self.shared
            .inner
            .lock()
            .producer_name(self.handle)
            .unwrap_or("")
            .to_owned()
    }
}

async fn wait_request(
    shared: &Arc<ConnectionShared>,
    request_id: magnetar_proto::RequestId,
) -> Result<(), ClientError> {
    let outcome = RequestFut {
        shared: shared.clone(),
        key: PendingOpKey::Request(request_id),
    }
    .await;
    match outcome {
        OpOutcome::Success { .. } => Ok(()),
        OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
        // Any other shape means the connection layer corrupted the request-id space — surface as
        // a protocol violation rather than silently succeeding.
        other => Err(ClientError::Other(format!(
            "unexpected outcome for request {request_id}: {other:?}"
        ))),
    }
}

/// Future returned by [`Producer::send`].
///
/// Polls until the matching [`OpOutcome::SendReceipt`] / [`OpOutcome::SendError`] lands inside
/// the sans-io state machine. NO oneshot channel.
#[derive(Debug)]
pub struct SendFut {
    shared: Arc<ConnectionShared>,
    handle: ProducerHandle,
    state: SendState,
}

#[derive(Debug)]
enum SendState {
    Pending {
        sequence_id: SequenceId,
    },
    /// `send()` returned an error synchronously (e.g. producer not yet open). We surface it on
    /// the first `poll`.
    Failed {
        error: Option<ClientError>,
    },
}

impl Future for SendFut {
    type Output = Result<MessageId, ClientError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Snapshot fields before borrowing `state` mutably to keep the borrow checker happy.
        let handle = self.handle;
        let shared = self.shared.clone();
        match &mut self.state {
            SendState::Failed { error } => {
                let err = error
                    .take()
                    .unwrap_or_else(|| ClientError::Other("send future polled after error".into()));
                Poll::Ready(Err(err))
            }
            SendState::Pending { sequence_id } => {
                let key = PendingOpKey::Send(handle, *sequence_id);
                let mut conn = shared.inner.lock();
                if let Some(outcome) = conn.take_outcome(key) {
                    drop(conn);
                    return Poll::Ready(translate_send_outcome(outcome));
                }
                conn.register_waker(key, cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

fn translate_send_outcome(outcome: OpOutcome) -> Result<MessageId, ClientError> {
    match outcome {
        OpOutcome::SendReceipt { message_id, .. } => Ok(message_id),
        OpOutcome::SendError { code, message, .. } => {
            Err(ClientError::SendRejected { code, message })
        }
        other => Err(ClientError::Other(format!(
            "unexpected send outcome: {other:?}"
        ))),
    }
}

/// Helper future to wait for a generic request outcome.
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
