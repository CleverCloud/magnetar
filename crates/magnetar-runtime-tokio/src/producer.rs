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

    /// Compression codec this producer was opened with. Mirrors Java
    /// `ProducerImpl#conf.getCompressionType()`. Returns `CompressionKind::None` when
    /// the producer was opened without explicit compression.
    #[must_use]
    pub fn compression(&self) -> CompressionKind {
        self.compression
    }

    /// Access mode the producer was opened with (`Shared`, `Exclusive`,
    /// `WaitForExclusive`, `ExclusiveWithFencing`). Mirrors Java
    /// `Producer#getProducerAccessMode`.
    #[must_use]
    pub fn access_mode(&self) -> magnetar_proto::pb::ProducerAccessMode {
        self.shared.inner.lock().producer_access_mode(self.handle)
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
                        reserved_bytes: 0,
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
                        reserved_bytes: 0,
                    };
                }
            }
        }

        // Reserve memory against the configured global budget BEFORE handing the payload to
        // the sans-io state machine. Mirrors Java `MemoryLimitController.reserveMemory(...)`.
        // Two policies (Java parity):
        //  - `FailImmediately`: try the CAS once; an overflow surfaces synchronously as
        //    `ClientError::MemoryLimitExceeded`.
        //  - `ProducerBlock`: park the send on a Waker slab until enough budget frees up; the
        //    `Reserving` variant of `SendState` re-attempts the CAS on every poll.
        // `try_reserve_memory` is a no-op when `memory_limit_bytes = 0` (the default).
        let reserved_bytes = msg.payload.len() as u64;
        match self.shared.memory_limit_policy {
            magnetar_proto::MemoryLimitPolicy::FailImmediately => {
                if let Err(err) = self.shared.try_reserve_memory(reserved_bytes) {
                    return SendFut {
                        shared: self.shared.clone(),
                        handle: self.handle,
                        state: SendState::Failed { error: Some(err) },
                        reserved_bytes: 0,
                    };
                }
                self.queue_send(msg, publish_time_ms, reserved_bytes)
            }
            magnetar_proto::MemoryLimitPolicy::ProducerBlock => {
                // Fast path: budget has room right now. The slow path inside `Reserving`
                // takes over otherwise; we don't synchronously park here so callers that
                // never `.await` (e.g. `Pin::poll` from a custom executor) still get a
                // future they can drive.
                if self.shared.try_reserve_memory(reserved_bytes).is_ok() {
                    return self.queue_send(msg, publish_time_ms, reserved_bytes);
                }
                SendFut {
                    shared: self.shared.clone(),
                    handle: self.handle,
                    state: SendState::Reserving {
                        msg: Some(Box::new(msg)),
                        publish_time_ms,
                        bytes: reserved_bytes,
                        slab_key: None,
                    },
                    // `Reserving` owns the reservation lifecycle itself: it only
                    // transitions to `Pending` AFTER a successful CAS, at which point
                    // it copies `bytes` into the outer `reserved_bytes`. Until then
                    // there is no reservation outstanding.
                    reserved_bytes: 0,
                }
            }
        }
    }

    /// Hand the (compressed/encrypted) message to the sans-io state machine. Assumes the
    /// `reserved_bytes` reservation has already been taken; releases it on synchronous
    /// failure so the budget reflects only actually-in-flight bytes.
    fn queue_send(
        &self,
        msg: OutgoingMessage,
        publish_time_ms: u64,
        reserved_bytes: u64,
    ) -> SendFut {
        let result = {
            let now = std::time::Instant::now();
            let mut conn = self.shared.inner.lock();
            conn.send(self.handle, msg, publish_time_ms, now)
        };

        // Wake the driver so it can drain the freshly-queued frame.
        self.shared.driver_waker.notify_one();

        match result {
            Ok(seq) => SendFut {
                shared: self.shared.clone(),
                handle: self.handle,
                state: SendState::Pending { sequence_id: seq },
                reserved_bytes,
            },
            Err(err) => {
                // The state machine rejected the send (e.g. producer not yet open); release
                // the reservation so the budget reflects only actually-in-flight bytes.
                self.shared.release_memory(reserved_bytes);
                SendFut {
                    shared: self.shared.clone(),
                    handle: self.handle,
                    state: SendState::Failed {
                        error: Some(ClientError::Protocol(err)),
                    },
                    reserved_bytes: 0,
                }
            }
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
            let now = std::time::Instant::now();
            let mut conn = self.shared.inner.lock();
            conn.flush_producer(self.handle, publish_time_ms, now);
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

    /// Flush this producer, bounded by `timeout`. Wraps [`Self::flush`] in
    /// [`tokio::time::timeout`]. If every in-flight send is acknowledged within the deadline
    /// the call resolves with `Ok(())`; if the deadline elapses with sends still pending the
    /// call resolves with [`ClientError::Timeout`].
    ///
    /// The pending sends are *not* cancelled — they remain in flight and may still be acked
    /// (or rejected) by the broker afterwards. Callers that need cancellation semantics must
    /// drop the producer or call [`Self::close`].
    ///
    /// Mirrors the Java pattern `producer.flushAsync().get(timeout, TimeUnit.MILLIS)`.
    pub async fn flush_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<(), ClientError> {
        if let Ok(res) = tokio::time::timeout(timeout, self.flush()).await {
            res
        } else {
            let pending = self.shared.inner.lock().producer_pending_count(self.handle);
            Err(ClientError::Timeout(format!(
                "producer flush exceeded {timeout:?} with {pending} sends still pending"
            )))
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
///
/// Holds the memory-budget reservation taken in [`Producer::send`] and releases it on
/// completion (success OR error). Mirrors Java `MemoryLimitController.releaseMemory(...)`.
#[derive(Debug)]
pub struct SendFut {
    shared: Arc<ConnectionShared>,
    handle: ProducerHandle,
    state: SendState,
    /// Bytes reserved against `shared.memory_limit_bytes` for this send. Released
    /// exactly once when the future returns `Poll::Ready`. `0` when no reservation
    /// was taken (the budget is unlimited, or the send failed synchronously and the
    /// reservation was already released in `send()`).
    reserved_bytes: u64,
}

impl Drop for SendFut {
    fn drop(&mut self) {
        // The future may be dropped before completion (caller cancelled). Release
        // the reservation so the budget doesn't permanently leak.
        if self.reserved_bytes > 0 {
            self.shared.release_memory(self.reserved_bytes);
            self.reserved_bytes = 0;
        }
        // If dropped while parked on the budget waker slab, evict the slot so
        // a later `release_memory` doesn't try to wake a dead future.
        if let SendState::Reserving {
            slab_key: Some(key),
            ..
        } = &self.state
        {
            self.shared.cancel_memory_waker(*key);
        }
    }
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
    /// `MemoryLimitPolicy::ProducerBlock` saw the budget full on the synchronous fast
    /// path. Each `poll` retries the CAS via `try_reserve_memory_or_register`; on
    /// success the state transitions to `Pending`; on failure the waker is parked in
    /// the runtime's slab and dispatched when capacity frees up. `msg` is boxed so
    /// this variant doesn't dominate the `SendState` discriminant size.
    Reserving {
        msg: Option<Box<OutgoingMessage>>,
        publish_time_ms: u64,
        bytes: u64,
        slab_key: Option<usize>,
    },
}

impl Future for SendFut {
    type Output = Result<MessageId, ClientError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Snapshot fields before borrowing `state` mutably to keep the borrow checker happy.
        let handle = self.handle;
        let shared = self.shared.clone();

        // `Reserving` needs to move out of `self.state`; handle it before the borrow.
        if matches!(self.state, SendState::Reserving { .. }) {
            let prev = std::mem::replace(&mut self.state, SendState::Failed { error: None });
            let SendState::Reserving {
                mut msg,
                publish_time_ms,
                bytes,
                slab_key,
            } = prev
            else {
                unreachable!()
            };
            match shared.try_reserve_memory_or_register(bytes, cx.waker()) {
                Ok(()) => {
                    if let Some(prior) = slab_key {
                        shared.cancel_memory_waker(prior);
                    }
                    let owned = *msg.take().expect("Reserving polled with no message");
                    let result = {
                        let now = std::time::Instant::now();
                        let mut conn = shared.inner.lock();
                        conn.send(handle, owned, publish_time_ms, now)
                    };
                    shared.driver_waker.notify_one();
                    match result {
                        Ok(seq) => {
                            self.state = SendState::Pending { sequence_id: seq };
                            self.reserved_bytes = bytes;
                            // Loop back to attempt to take the outcome now that
                            // we're in `Pending`; falls through to the normal match.
                        }
                        Err(err) => {
                            shared.release_memory(bytes);
                            return Poll::Ready(Err(ClientError::Protocol(err)));
                        }
                    }
                }
                Err(new_key) => {
                    if let Some(prior) = slab_key {
                        shared.cancel_memory_waker(prior);
                    }
                    self.state = SendState::Reserving {
                        msg,
                        publish_time_ms,
                        bytes,
                        slab_key: Some(new_key),
                    };
                    return Poll::Pending;
                }
            }
        }

        let outcome = match &mut self.state {
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
                    Poll::Ready(translate_send_outcome(outcome))
                } else {
                    conn.register_waker(key, cx.waker().clone());
                    Poll::Pending
                }
            }
            SendState::Reserving { .. } => unreachable!("Reserving handled above"),
        };
        if matches!(outcome, Poll::Ready(_)) && self.reserved_bytes > 0 {
            // Release the budget reservation. `Drop` would also catch the cancellation
            // path; this branch covers the normal completion path so the count is
            // current the instant the user observes the result.
            self.shared.release_memory(self.reserved_bytes);
            self.reserved_bytes = 0;
        }
        outcome
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

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use bytes::{Bytes, BytesMut};
    use magnetar_proto::producer::OutgoingMessage;
    use magnetar_proto::types::CompressionKind;
    use magnetar_proto::{ConnectionConfig, CreateProducerRequest, encode_command, pb};

    use super::Producer;
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

    /// Spin up a `ConnectionShared` whose inner state machine has completed the handshake, so
    /// `create_producer` runs cleanly without erroring on protocol-state checks.
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

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn flush_with_timeout_returns_timeout_when_nothing_acks() {
        let shared = handshake_complete_shared();
        // Register a producer and queue a send. No driver task is running, so the broker
        // will never respond with `CommandSendReceipt` — `pending_count` stays at 1 forever.
        let handle = {
            let mut conn = shared.inner.lock();
            let h = conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/flush-timeout".to_owned(),
                ..Default::default()
            });
            let _ = conn.send(
                h,
                OutgoingMessage {
                    payload: Bytes::from_static(b"x"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 1,
                    num_messages: 1,
                    txn_id: None,
                },
                1_700_000_000_000,
                std::time::Instant::now(),
            );
            h
        };
        let producer = Producer {
            shared: shared.clone(),
            handle,
            compression: CompressionKind::None,
            encryptor: None,
        };
        // Pre-condition: at least one in-flight send.
        assert!(
            producer.pending_count() >= 1,
            "expected pending send; got {}",
            producer.pending_count()
        );

        match producer.flush_with_timeout(Duration::from_millis(50)).await {
            Err(ClientError::Timeout(msg)) => {
                assert!(
                    msg.contains("pending"),
                    "timeout message should mention pending sends: {msg}"
                );
            }
            other => panic!("expected ClientError::Timeout, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn flush_with_timeout_returns_ok_on_quiescent_producer() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/flush-ok".to_owned(),
                ..Default::default()
            })
        };
        let producer = Producer {
            shared,
            handle,
            compression: CompressionKind::None,
            encryptor: None,
        };
        assert_eq!(producer.pending_count(), 0);
        producer
            .flush_with_timeout(Duration::from_secs(5))
            .await
            .expect("idempotent flush on quiescent producer must succeed");
    }
}
