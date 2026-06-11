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
use magnetar_proto::{MessageId, OpOutcome, PendingOpKey, ProducerHandle, SequenceId, pb};

use crate::ConnectionShared;
use crate::crypto::MessageEncryptor;
use crate::error::ClientError;

/// User-facing producer handle.
///
/// # Lock-ordering (ADR-0038)
///
/// Identity reads (topic, access mode, handle) go through `slot.identity` and
/// take **no lock at all**. State-machine reads (`pending_count`, `batch_len`,
/// `last_sequence_id_*`, `stats`, `is_closed`) take only the per-slot mutex
/// via `slot.state.lock()` — they do **not** acquire the global Connection
/// mutex. Operations that drive protocol I/O (`send`, `flush`, `close`,
/// `get_schema`) still take `shared.inner.lock()` because they mutate the
/// connection-wide state machine. Acquisition order is always **global →
/// per-slot, never the reverse**.
#[derive(Debug, Clone)]
pub struct Producer {
    pub(crate) shared: Arc<ConnectionShared>,
    pub(crate) handle: ProducerHandle,
    /// Direct handle to this producer's per-slot state, cloned from the
    /// Connection's registry at create time. Identity reads bypass any lock;
    /// hot-state reads/writes take only `slot.state.lock()`.
    pub(crate) slot: Arc<magnetar_proto::ProducerSlot>,
    pub(crate) compression: CompressionKind,
    /// Optional encryption hook (PIP-4). When present, the producer encrypts every
    /// outbound payload after compression but before handing it to the sans-io layer.
    pub(crate) encryptor: Option<Arc<dyn MessageEncryptor>>,
    /// Last-clone close guard. `Producer` is cheap-clone, so the broker-side
    /// best-effort close must fire exactly once — when the **last** clone
    /// drops. See [`ProducerCloseGuard`]. Held for its `Drop` impl only:
    /// this side derives `Clone` (L36), so the field is never read by name
    /// and needs the `dead_code` allow — the moonpool mirror hand-writes
    /// `Clone` and reads `self.close_guard.clone()`, so it carries none.
    #[allow(dead_code)]
    pub(crate) close_guard: Arc<ProducerCloseGuard>,
}

/// RAII guard arming a best-effort `CommandCloseProducer` on last-clone drop
/// (ADR-0057).
///
/// Every [`Producer`] clone shares one guard behind an `Arc`; the `Drop`
/// below therefore runs exactly once, when the last clone goes away.
/// Without it, dropping a producer without an explicit [`Producer::close`]
/// leaks the broker-side registration for as long as the shared TCP
/// connection stays open — recreating a producer with the same
/// user-provided name then fails forever with `NamingException`
/// (broker error code 16).
///
/// The explicit-close path stays the reliable one: [`Producer::close`]
/// awaits the broker ack. This guard fires
/// [`magnetar_proto::Connection::close_producer_forget`] — encode the frame
/// and wake the driver, never await. The proto layer consumes the broker
/// ack in-place (no orphaned `OpOutcome` entry) and surfaces a rejection as
/// a `warn!`.
///
/// Dedup is best-effort, not a hard invariant: the slot's `closed` flag
/// (set synchronously by `Connection::close_producer`) dedups a *preceding
/// completed* client-initiated close as observed here. It does NOT cover
/// broker-initiated detach — `handle_close_producer` deliberately keeps
/// `closed = false` so `rebuild_producers` can re-attach on PIP-188
/// migration / failover — and the check+act below is non-atomic against a
/// concurrent `close()` on another clone. Both residual cases emit one
/// redundant `CloseProducer` frame, which the broker tolerates.
#[derive(Debug)]
pub(crate) struct ProducerCloseGuard {
    shared: Arc<ConnectionShared>,
    handle: ProducerHandle,
    slot: Arc<magnetar_proto::ProducerSlot>,
}

impl Drop for ProducerCloseGuard {
    fn drop(&mut self) {
        // ADR-0038 lock order: the per-slot probe drops its guard before the
        // global Connection mutex is taken (sequential, never nested).
        let already_closed = self.slot.state.lock().closed;
        if already_closed {
            return;
        }
        {
            let mut conn = self.shared.inner.lock();
            let _ = conn.close_producer_forget(self.handle);
        }
        self.shared.driver_waker.notify_one();
        tracing::debug!(
            topic = %self.slot.identity.topic,
            handle = ?self.handle,
            "producer dropped without explicit close — best-effort CloseProducer enqueued"
        );
    }
}

impl Producer {
    /// Assemble a producer handle and arm its last-clone close guard.
    ///
    /// Single construction point — keeps the [`ProducerCloseGuard`] wiring
    /// in one place for every producer the engine hands out.
    pub(crate) fn assemble(
        shared: Arc<ConnectionShared>,
        handle: ProducerHandle,
        slot: Arc<magnetar_proto::ProducerSlot>,
        compression: CompressionKind,
        encryptor: Option<Arc<dyn MessageEncryptor>>,
    ) -> Self {
        let close_guard = Arc::new(ProducerCloseGuard {
            shared: shared.clone(),
            handle,
            slot: slot.clone(),
        });
        Self {
            shared,
            handle,
            slot,
            compression,
            encryptor,
            close_guard,
        }
    }

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
    ///
    /// Identity-only read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn access_mode(&self) -> magnetar_proto::pb::ProducerAccessMode {
        self.slot.identity.access_mode
    }

    /// `true` if this producer has been closed (locally via [`Self::close`] or remotely
    /// via a broker `CloseProducer`). Mirrors Java `ProducerImpl#getState() == CLOSED`.
    /// Use [`Self::is_connected`] for the live test — `is_closed` only flips after a
    /// terminal close, not on transient disconnects.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.slot.state.lock().closed
    }

    /// Last sequence id this client has pushed onto the wire. Returns `-1` if the producer
    /// has never sent. Mirrors `org.apache.pulsar.client.api.Producer#getLastSequenceId`.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    pub fn last_sequence_id(&self) -> i64 {
        self.slot.state.lock().last_sequence_id_pushed
    }

    /// Number of in-flight sends (queued and not yet acked by the broker). Mirrors the
    /// un-batched view of Java `ProducerStats#getPendingQueueSize`. Equivalent to
    /// `self.stats().pending_queue_size as usize` but spares the full stats snapshot.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.slot.state.lock().pending.len()
    }

    /// Number of messages currently buffered in the batch container, waiting for the next
    /// flush cycle. Returns `0` when batching is disabled or the batch is empty.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn batch_len(&self) -> usize {
        self.slot.state.lock().batch.len()
    }

    /// Sum of payload bytes currently buffered in the batch container.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn batch_bytes(&self) -> usize {
        self.slot.state.lock().batch.current_size_bytes
    }

    /// Last sequence id the broker has acknowledged via `CommandSendReceipt`. Returns `-1`
    /// if no sends have been acked yet. Useful for resume-from-checkpoint flows.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    pub fn last_sequence_id_published(&self) -> i64 {
        self.slot.state.lock().last_sequence_id_published
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
            source_message_id: None,
        })
    }

    /// PIP-180 / ADR-0033: replicator-style send that propagates a source-topic
    /// `MessageId` on the wire (`CommandSend.message_id`). Used by producers
    /// writing to a shadow topic to preserve the source-topic id chain.
    ///
    /// The broker echoes the asserted source id back on the resulting
    /// `CommandSendReceipt` (PIP-180 §"Wire protocol"), so the returned
    /// [`SendFut`] resolves to a [`MessageId`] structurally equal to
    /// `source_msg_id`.
    ///
    /// Bypasses batching by design — mirrors Java
    /// `org.apache.pulsar.broker.service.persistent.Replicator` which writes
    /// each replicated entry as an individual `CommandSend`. Chunking still
    /// applies for payloads larger than `max_message_size`; in that case the
    /// same `source_msg_id` is stamped on every chunk (one logical message,
    /// multiple frames).
    ///
    /// Caveat: the source id is **client-asserted** — the broker validates
    /// write authorisation on the shadow topic but does not cryptographically
    /// prove the source-message-id matches a real source entry (upstream
    /// PIP-180 behaviour, mirrored verbatim — see
    /// [`docs/shadow-topic.md`](../../docs/shadow-topic.md)).
    pub fn send_with_source_message_id(
        &self,
        source_msg_id: MessageId,
        payload: impl Into<bytes::Bytes>,
        metadata: pb::MessageMetadata,
    ) -> SendFut {
        let payload = payload.into();
        let uncompressed_size = u32::try_from(payload.len()).unwrap_or(u32::MAX);
        self.send(OutgoingMessage {
            payload,
            metadata,
            uncompressed_size,
            num_messages: 1,
            txn_id: None,
            source_message_id: Some(source_msg_id),
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
                    // Pre-enqueue rejection — expected anomaly surfaced as
                    // `Err` to the caller, so `debug!` per ADR-0054 §2.1.
                    tracing::debug!(
                        compression = ?self.compression,
                        error = %err,
                        "send rejected: compression failed"
                    );
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
                    // Pre-enqueue rejection — `debug!` per ADR-0054 §2.1.
                    // Payload and key material are never logged.
                    tracing::debug!(error = %err, "send rejected: encryption failed");
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
                    // Caller-visible rejection whose rate scales with send
                    // throughput under overload — `debug!` per ADR-0054
                    // §2.1 (never `warn!` on a per-message path).
                    tracing::debug!(
                        payload_len = reserved_bytes,
                        "send rejected: memory limit exceeded"
                    );
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
    ///
    /// ADR-0038 Phase 3 hot path: takes only the per-slot mutex via
    /// [`magnetar_proto::ProducerSlot::queue_send`] — does NOT acquire the
    /// global Connection mutex. The driver merges per-slot staged frames
    /// into the connection-wide outbound buffer on its next tick (it calls
    /// `Connection::drain_producer_outbound` right before `poll_transmit`).
    fn queue_send(
        &self,
        msg: OutgoingMessage,
        publish_time_ms: u64,
        reserved_bytes: u64,
    ) -> SendFut {
        // Precondition (ADR-0038): the per-slot Arc this `Producer` was built
        // with must denote the same producer as `self.handle`. The hot path
        // routes the send through `self.slot` (per-slot lock only) while the
        // eventual `SendFut` correlates the receipt by `self.handle`; a
        // mismatch would silently queue against the wrong slot. Identity read
        // takes no lock, so this cannot self-deadlock.
        debug_assert_eq!(
            self.slot.identity.handle, self.handle,
            "producer slot/handle mismatch: slot is for {:?} but handle is {:?}",
            self.slot.identity.handle, self.handle,
        );

        let now = std::time::Instant::now();
        let result = self.slot.queue_send(msg, publish_time_ms, now);

        // Wake the driver so it can drain the freshly-queued frame.
        self.shared.driver_waker.notify_one();

        match result {
            Ok(seq) => {
                // NOTE: no cross-lock postcondition assert here. The returned
                // seq is computed under the per-slot guard INSIDE
                // `ProducerSlot::queue_send`; re-locking afterwards to compare
                // against `last_sequence_id_pushed` raced the driver's
                // reset/replay machinery (snapshot + ack-gated re-emit can
                // interleave between the two acquisitions during a supervised
                // reconnect) and panicked debug builds on a perfectly legal
                // schedule. The contract is pinned where it is sound — in the
                // proto unit tests, under a single guard.
                // ADR-0054 hot-path record: no lock is held here (the
                // per-slot guard inside `ProducerSlot::queue_send` has been
                // released), two integer fields, and the disabled-level cost
                // is a cached callsite check (ADR-0038 stays intact).
                tracing::trace!(
                    sequence_id = seq.0,
                    payload_len = reserved_bytes,
                    "send queued"
                );
                SendFut {
                    shared: self.shared.clone(),
                    handle: self.handle,
                    state: SendState::Pending { sequence_id: seq },
                    reserved_bytes,
                }
            }
            Err(err) => {
                // The state machine rejected the send (e.g. producer not yet open); release
                // the reservation so the budget reflects only actually-in-flight bytes.
                self.shared.release_memory(reserved_bytes);
                // Expected anomaly surfaced as `Err` to the caller —
                // `debug!` per ADR-0054 §2.1.
                tracing::debug!(error = %err, "send rejected by producer state machine");
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
        //
        // ADR-0038: the pending-count probe reads from the per-slot mutex directly
        // (no global Connection lock), so a parallel send on a sibling producer
        // doesn't serialise against this drain.
        loop {
            let pending = self.slot.state.lock().pending.len();
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
            // ADR-0038: per-slot read, no global lock.
            let pending = self.slot.state.lock().pending.len();
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
        // Snapshot identity for the lifecycle record before the round-trip.
        let topic = self.slot.identity.topic.clone();
        let producer_name = self.slot.state.lock().name.clone().unwrap_or_default();
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.close_producer(self.handle)
        };
        self.shared.driver_waker.notify_one();
        let result = wait_request(&self.shared, request_id).await;
        if result.is_ok() {
            // Lifecycle record (ADR-0054).
            tracing::info!(
                topic = %topic,
                producer_name = %producer_name,
                handle = ?self.handle,
                access_mode = ?self.slot.identity.access_mode,
                "producer closed"
            );
        }
        result
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
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    pub fn stats(&self) -> magnetar_proto::ProducerStats {
        self.slot.state.lock().stats()
    }

    /// Capture a rolling-window sample for this producer. Mirrors Java
    /// `ProducerStatsRecorderImpl#updateNumMsgsSent` — call periodically (e.g. once
    /// per second) to refresh [`magnetar_proto::ProducerStats::msgs_per_sec`] and
    /// [`magnetar_proto::ProducerStats::bytes_per_sec`]. The first call only seeds
    /// the baseline (rates stay at `0.0`); the second and subsequent calls compute
    /// the per-second deltas between consecutive samples.
    ///
    /// Per-slot write — does NOT take the global Connection mutex.
    pub fn record_rate_window(&self, now: std::time::Instant) {
        self.slot.state.lock().record_rate_window(now);
    }

    /// Topic name this producer is bound to. Returns an empty string if the producer is no
    /// longer registered (closed).
    ///
    /// Identity-only read — does NOT take any lock.
    pub fn topic(&self) -> String {
        self.slot.identity.topic.clone()
    }

    /// Broker-assigned producer name. Returns an empty string until the broker assigns one
    /// (typically right after the ProducerSuccess round-trip) or if the producer is no
    /// longer registered.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    pub fn name(&self) -> String {
        self.slot.state.lock().name.clone().unwrap_or_default()
    }

    /// Look up the broker-registered schema for the producer's topic (PIP-87).
    ///
    /// Issues a `CommandGetSchema` for the topic this producer is bound to and awaits the
    /// `CommandGetSchemaResponse`. Returns the registry-resolved [`pb::Schema`] on success or
    /// [`ClientError::Broker`] when the broker rejects the lookup (e.g. `TopicNotFound`).
    /// Mirrors Java `PulsarClientImpl#getSchema(TopicName, Optional<byte[]>)`, used on the
    /// producer side by `AutoProduceBytesSchema` to warm its diagnostic cache on first send.
    ///
    /// `version = None` asks the broker for the topic's current schema; pass
    /// `Some(schema_version_bytes)` to re-resolve a historical schema.
    ///
    /// The result is **not** cached here — callers that need a per-instance cache (e.g.
    /// [`magnetar_proto::schema::AutoProduceBytesSchema`]) push the resolved schema into
    /// their own `Arc<Mutex<…>>` after this future resolves.
    pub async fn get_schema(
        &self,
        version: Option<bytes::Bytes>,
    ) -> Result<pb::Schema, ClientError> {
        // ADR-0038: identity-only read, no global lock.
        let topic = self.slot.identity.topic.clone();
        // Per-operation internals — `debug!` per ADR-0054 §2.1.
        tracing::debug!(topic = %topic, "schema lookup");
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
            OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
            other => Err(ClientError::Other(format!(
                "unexpected get_schema outcome: {other:?}"
            ))),
        }
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
        OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
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
        OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
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

impl Drop for RequestFut {
    /// Drop-time cleanup: clear our entry from the connection's waker slab so
    /// a cancelled producer-side request future (close-producer, etc.) does
    /// not leave a dangling [`std::task::Waker`] behind. For
    /// [`PendingOpKey::Send`] keys [`magnetar_proto::Connection::unregister_waker`]
    /// also clears the per-producer-slot waker. See the matching docstring on
    /// [`crate::client::RequestFut::drop`] for the rationale and
    /// the lookup multi-agent review MEDIUM-4 finding.
    fn drop(&mut self) {
        self.shared.inner.lock().unregister_waker(self.key);
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use bytes::{Bytes, BytesMut};
    use magnetar_proto::producer::OutgoingMessage;
    use magnetar_proto::types::{CompressionKind, ProducerHandle};
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

    /// Capture the per-slot Arc for `handle`, panicking if the slot is gone
    /// (caller must hold a fresh handle from `create_producer`). Tests that
    /// intentionally exercise an unknown handle use [`stub_slot_for_test`].
    fn slot_for(
        shared: &std::sync::Arc<ConnectionShared>,
        handle: ProducerHandle,
    ) -> std::sync::Arc<magnetar_proto::ProducerSlot> {
        shared
            .inner
            .lock()
            .producer(handle)
            .cloned()
            .expect("test producer slot must exist")
    }

    /// For tests that deliberately construct a `Producer` whose handle is not
    /// in the registry (e.g. the bogus-handle stats-fallback case). The slot's
    /// `state.lock()` will never be inspected because the caller never reaches
    /// the per-slot paths; only used as a placeholder to satisfy the struct
    /// initializer.
    fn stub_slot_for_test(handle: ProducerHandle) -> std::sync::Arc<magnetar_proto::ProducerSlot> {
        magnetar_proto::ProducerSlot::new(
            magnetar_proto::ProducerIdentity {
                handle,
                topic: String::new(),
                access_mode: pb::ProducerAccessMode::Shared,
            },
            magnetar_proto::producer::ProducerState::new(
                handle,
                String::new(),
                CompressionKind::None,
                0,
            ),
        )
    }

    /// Deterministic, dependency-free PIP-4 encryptor stub: XORs every payload
    /// byte with a fixed key and stamps the canonical encryption metadata
    /// fields. Records the last plaintext it saw so tests can assert the
    /// encrypt hook ran on the pre-encryption bytes. 1:1 mirror of the moonpool
    /// engine's stub (ADR-0024 cross-runtime parity).
    #[derive(Debug, Default)]
    struct XorEncryptor {
        seen_plaintext: std::sync::Mutex<Option<Vec<u8>>>,
    }

    const XOR_KEY: u8 = 0x5A;

    impl crate::crypto::MessageEncryptor for XorEncryptor {
        fn encrypt(
            &self,
            plaintext: &[u8],
            metadata: &mut pb::MessageMetadata,
        ) -> Result<Bytes, crate::crypto::EncryptError> {
            *self.seen_plaintext.lock().unwrap() = Some(plaintext.to_vec());
            metadata.encryption_keys.push(pb::EncryptionKeys {
                key: "xor-test".to_owned(),
                value: Bytes::from_static(b"k"),
                metadata: Vec::new(),
            });
            metadata.encryption_algo = Some("XOR-TEST".to_owned());
            metadata.encryption_param = Some(Bytes::from_static(b"iv"));
            Ok(Bytes::from(
                plaintext.iter().map(|b| b ^ XOR_KEY).collect::<Vec<u8>>(),
            ))
        }
    }

    /// `send` with a PIP-4 encryptor wired stamps the encryption metadata and
    /// hands the ciphertext (not the plaintext) to the sans-io layer. We observe
    /// the encrypt hook fired against the original plaintext; the resulting send
    /// enqueues a pending op (no driver running drains it). 1:1 with the moonpool
    /// `send_encrypts_payload_and_stamps_metadata`.
    #[tokio::test(flavor = "current_thread")]
    async fn send_encrypts_payload_and_stamps_metadata() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/encrypt".to_owned(),
                ..Default::default()
            })
        };
        let encryptor = std::sync::Arc::new(XorEncryptor::default());
        let producer = Producer::assemble(
            shared.clone(),
            handle,
            slot_for(&shared, handle),
            CompressionKind::None,
            Some(encryptor.clone()),
        );
        let _fut = producer.send(OutgoingMessage {
            payload: Bytes::from_static(b"plain-secret"),
            metadata: pb::MessageMetadata::default(),
            uncompressed_size: 12,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        });
        // The encryptor must have run against the original plaintext.
        assert_eq!(
            encryptor.seen_plaintext.lock().unwrap().as_deref(),
            Some(b"plain-secret".as_slice()),
            "encrypt hook must see the pre-encryption payload",
        );
        // The send enqueued a pending op against the per-slot queue.
        assert!(
            producer.pending_count() >= 1,
            "expected pending encrypted send; got {}",
            producer.pending_count()
        );
    }

    /// Encryptor that always fails. Exercises the producer-side encrypt-error
    /// branch (`send` surfaces `ClientError::Other("encrypt: …")`). 1:1 with the
    /// moonpool `send_encrypt_failure_surfaces_error`.
    #[derive(Debug, Default)]
    struct FailingEncryptor;

    impl crate::crypto::MessageEncryptor for FailingEncryptor {
        fn encrypt(
            &self,
            _plaintext: &[u8],
            _metadata: &mut pb::MessageMetadata,
        ) -> Result<Bytes, crate::crypto::EncryptError> {
            Err(crate::crypto::EncryptError::new(
                "forced encrypt failure (test)",
            ))
        }
    }

    /// A failing encryptor makes `send` resolve to `ClientError::Other` and the
    /// payload never reaches the sans-io layer (no pending op). 1:1 with the
    /// moonpool `send_encrypt_failure_surfaces_error`.
    #[tokio::test(flavor = "current_thread")]
    async fn send_encrypt_failure_surfaces_error() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/encrypt-fail".to_owned(),
                ..Default::default()
            })
        };
        let producer = Producer::assemble(
            shared.clone(),
            handle,
            slot_for(&shared, handle),
            CompressionKind::None,
            Some(std::sync::Arc::new(FailingEncryptor)),
        );
        let res = producer
            .send(OutgoingMessage {
                payload: Bytes::from_static(b"plain"),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 5,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            })
            .await;
        let err = res.expect_err("encrypt failure must surface");
        assert!(
            format!("{err}").contains("encrypt:"),
            "expected encrypt-error message, got {err:?}"
        );
        assert_eq!(
            producer.pending_count(),
            0,
            "a failed encrypt must not enqueue a send"
        );
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
                    source_message_id: None,
                },
                1_700_000_000_000,
                std::time::Instant::now(),
            );
            h
        };
        let producer = Producer::assemble(
            shared.clone(),
            handle,
            slot_for(&shared, handle),
            CompressionKind::None,
            None,
        );
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
        let slot = slot_for(&shared, handle);
        let producer = Producer::assemble(shared, handle, slot, CompressionKind::None, None);
        assert_eq!(producer.pending_count(), 0);
        producer
            .flush_with_timeout(Duration::from_secs(5))
            .await
            .expect("idempotent flush on quiescent producer must succeed");
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
    async fn producer_get_schema_round_trip_resolves_with_cached_schema() {
        // End-to-end: Producer::get_schema issues a CommandGetSchema, the broker replies with a
        // CommandGetSchemaResponse, and the future resolves with the broker-resolved pb::Schema.
        // Mirrors the PIP-87 runtime path used by AutoProduceBytesSchema's on-first-send lookup.
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/auto-produce-ok".to_owned(),
                ..Default::default()
            })
        };
        let producer = Producer::assemble(
            shared.clone(),
            handle,
            slot_for(&shared, handle),
            CompressionKind::None,
            None,
        );

        let request_id = shared.inner.lock().peek_next_request_id_for_test();
        let response_schema = pb::Schema {
            name: "persistent://public/default/auto-produce-ok-schema".to_owned(),
            schema_data: bytes::Bytes::from_static(
                b"{\"type\":\"record\",\"name\":\"X\",\"fields\":[]}",
            ),
            r#type: pb::schema::Type::Avro as i32,
            properties: Vec::new(),
        };

        let injector_shared = shared.clone();
        let injector_schema = response_schema.clone();
        let injector = tokio::spawn(async move {
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

        let resolved = producer
            .get_schema(None)
            .await
            .expect("get_schema resolves with broker reply");
        injector.await.expect("injector task completes");

        assert_eq!(resolved.name, response_schema.name);
        assert_eq!(resolved.schema_data, response_schema.schema_data);
        assert_eq!(resolved.r#type, response_schema.r#type);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn producer_get_schema_surfaces_broker_error() {
        // Error path: broker returns CommandGetSchemaResponse with error_code set —
        // Producer::get_schema surfaces a ClientError::Broker carrying both code and message.
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/auto-produce-missing".to_owned(),
                ..Default::default()
            })
        };
        let producer = Producer::assemble(
            shared.clone(),
            handle,
            slot_for(&shared, handle),
            CompressionKind::None,
            None,
        );

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

        let err = producer
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
    async fn producer_get_schema_caches_into_auto_produce_bytes_schema() {
        // Integration with the proto-side `AutoProduceBytesSchema`: the runtime `get_schema`
        // future resolves, the caller pushes the schema into the schema's cache via the
        // `Schema::store_resolved_schema` hook, and subsequent calls to
        // `needs_broker_schema()` correctly report `false`. This is the exact sequence the
        // `TypedProducer::send` warm-up path runs on first send.
        use magnetar_proto::schema::{AutoProduceBytesSchema, Schema as SchemaTrait};

        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/auto-produce-cache".to_owned(),
                ..Default::default()
            })
        };
        let producer = Producer::assemble(
            shared.clone(),
            handle,
            slot_for(&shared, handle),
            CompressionKind::None,
            None,
        );
        let schema = AutoProduceBytesSchema::new();
        assert!(
            schema.needs_broker_schema(),
            "fresh AutoProduceBytesSchema must require a broker lookup"
        );

        let request_id = shared.inner.lock().peek_next_request_id_for_test();
        let response_schema = pb::Schema {
            name: "persistent://public/default/auto-produce-cache-schema".to_owned(),
            schema_data: bytes::Bytes::from_static(b"avro-schema-bytes"),
            r#type: pb::schema::Type::Avro as i32,
            properties: Vec::new(),
        };

        let injector_shared = shared.clone();
        let injector_schema = response_schema.clone();
        let injector = tokio::spawn(async move {
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

        let resolved = producer
            .get_schema(None)
            .await
            .expect("get_schema resolves with broker reply");
        injector.await.expect("injector task completes");
        schema.store_resolved_schema(resolved);

        assert!(
            !schema.needs_broker_schema(),
            "cache must be populated after store_resolved_schema"
        );
        assert_eq!(
            schema.schema_data().as_ref(),
            response_schema.schema_data.as_ref(),
            "cached schema_data must round-trip from the broker reply"
        );
        // Encode remains pass-through whether or not the cache is populated (Java parity).
        let payload = bytes::Bytes::from_static(b"already-encoded-bytes");
        let encoded = schema.encode(&payload).expect("encode pass-through");
        assert_eq!(
            encoded, payload,
            "AutoProduceBytesSchema::encode is pure pass-through"
        );
    }

    // ------------------------------------------------------------------
    // ProducerBlock race / error-path coverage. These three tests mirror
    // the moonpool engine's equivalent fixtures 1:1 so the tokio runtime
    // carries the same regression guard for the three previously-
    // uncovered ProducerBlock paths (fast-path early-return, send-error
    // releases reservation, re-park cancels prior slot). ADR-0024
    // parity gate requires the test count to stay 1:1 between runtimes.

    /// Build a `tokio::sync::Notify`-free noop waker for direct
    /// `SendFut::poll` driving. Stable on Rust 1.85+ (workspace MSRV).
    fn futures_task_waker() -> std::task::Waker {
        std::task::Waker::noop().clone()
    }

    /// `ProducerBlock`: fast-path success when the budget has room takes
    /// the synchronous `queue_send` return (no `SendFut` slow path, no
    /// slab insert). Mirrors `FailImmediately`'s fast path on the
    /// `ProducerBlock` side.
    #[tokio::test(flavor = "current_thread")]
    async fn producer_block_fast_path_when_budget_available() {
        let cfg = ConnectionConfig {
            memory_limit_bytes: 1024,
            memory_limit_policy: magnetar_proto::MemoryLimitPolicy::ProducerBlock,
            ..ConnectionConfig::default()
        };
        let shared = ConnectionShared::new(cfg);
        {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("handshake");
            let frame = handshake_response_bytes();
            conn.handle_bytes(Instant::now(), &frame)
                .expect("connected");
        }
        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/fast".to_owned(),
                ..Default::default()
            })
        };
        let producer = Producer::assemble(
            shared.clone(),
            handle,
            slot_for(&shared, handle),
            CompressionKind::None,
            None,
        );
        // Budget has 1024 free bytes; the 4-byte payload reserves
        // synchronously and takes the fast-path `queue_send` return.
        let _fut = producer.send(OutgoingMessage {
            payload: Bytes::from_static(b"fast"),
            metadata: pb::MessageMetadata::default(),
            uncompressed_size: 4,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        });
        assert_eq!(
            shared
                .memory_used
                .load(std::sync::atomic::Ordering::Acquire),
            4,
            "ProducerBlock fast path must reserve synchronously",
        );
        assert!(
            shared.memory_wakers.lock().is_empty(),
            "fast path must not register a waker slot",
        );
    }

    /// `ProducerBlock`: when `conn.send` errors after a successful memory
    /// reservation, [`SendFut::poll`] must release the reservation and
    /// surface a [`ClientError::Other`] (the `Err` arm of the inner
    /// `match result {}`). We force the error by sending against an
    /// unregistered [`ProducerHandle`] — the proto layer rejects with
    /// `ProtocolError::InvariantViolation("unknown producer handle")`,
    /// which the runtime wraps as `ClientError::Protocol(_)` along the
    /// `Err` arm of [`super::SendFut::new`].
    #[tokio::test(flavor = "current_thread")]
    async fn producer_block_send_error_releases_reservation() {
        use std::future::Future as _;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        let cfg = ConnectionConfig {
            memory_limit_bytes: 16,
            memory_limit_policy: magnetar_proto::MemoryLimitPolicy::ProducerBlock,
            ..ConnectionConfig::default()
        };
        let shared = ConnectionShared::new(cfg);
        {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("handshake");
            let frame = handshake_response_bytes();
            conn.handle_bytes(Instant::now(), &frame)
                .expect("connected");
        }
        shared.try_reserve_memory(16).expect("seed budget");
        let bogus_handle = ProducerHandle(u64::MAX);
        let producer = Producer::assemble(
            shared.clone(),
            bogus_handle,
            stub_slot_for_test(bogus_handle),
            CompressionKind::None,
            None,
        );
        let mut fut = producer.send(OutgoingMessage {
            payload: Bytes::from_static(b"err"),
            metadata: pb::MessageMetadata::default(),
            uncompressed_size: 3,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        });
        let waker = futures_task_waker();
        let mut cx = Context::from_waker(&waker);
        // First poll: budget full → register on slab → Pending.
        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));

        // Release the seed so the next poll proceeds through the success
        // branch of `try_reserve_memory_or_register` AND lands the
        // synchronous `conn.send` error.
        shared.release_memory(16);
        let outcome = Pin::new(&mut fut).poll(&mut cx);
        match outcome {
            Poll::Ready(Err(ClientError::Protocol(
                magnetar_proto::ProtocolError::InvariantViolation(msg),
            ))) => {
                assert!(
                    msg.contains("unknown producer handle"),
                    "expected `unknown producer handle` invariant, got {msg:?}",
                );
            }
            other => {
                panic!("expected Ready(Err(Protocol(InvariantViolation(...)))), got {other:?}")
            }
        }
        // The reservation must have been released along the error path.
        assert_eq!(
            shared
                .memory_used
                .load(std::sync::atomic::Ordering::Acquire),
            0,
            "Err arm must release the reservation it took",
        );
    }

    /// `ProducerBlock`: re-polling a `Reserving` future while the budget
    /// is still full must evict the prior slab entry before inserting a
    /// new one. Two polls park the same future twice; the slab must
    /// carry exactly one entry after the second poll (the prior slot
    /// must have been cancelled, not leaked).
    #[tokio::test(flavor = "current_thread")]
    async fn producer_block_re_park_cancels_prior_waker_slot() {
        use std::future::Future as _;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        let cfg = ConnectionConfig {
            memory_limit_bytes: 4,
            memory_limit_policy: magnetar_proto::MemoryLimitPolicy::ProducerBlock,
            ..ConnectionConfig::default()
        };
        let shared = ConnectionShared::new(cfg);
        {
            let mut conn = shared.inner.lock();
            conn.begin_handshake().expect("handshake");
            let frame = handshake_response_bytes();
            conn.handle_bytes(Instant::now(), &frame)
                .expect("connected");
        }
        shared.try_reserve_memory(4).expect("seed budget");

        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/repark".to_owned(),
                ..Default::default()
            })
        };
        let producer = Producer::assemble(
            shared.clone(),
            handle,
            slot_for(&shared, handle),
            CompressionKind::None,
            None,
        );
        let mut fut = producer.send(OutgoingMessage {
            payload: Bytes::from_static(b"hi"),
            metadata: pb::MessageMetadata::default(),
            uncompressed_size: 2,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        });
        let waker = futures_task_waker();
        let mut cx = Context::from_waker(&waker);
        // First poll: lands in `Reserving { slab_key: Some(_) }`.
        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));
        assert_eq!(shared.memory_wakers.lock().len(), 1);
        // Second poll: the budget is still full, so the slow path
        // re-registers and evicts the prior slot.
        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));
        assert_eq!(
            shared.memory_wakers.lock().len(),
            1,
            "re-park must cancel the prior waker before inserting a new one",
        );
    }

    /// `last_sequence_id_published` reports `-1` until the broker has
    /// acked at least one send. ADR-0024 1:1 mirror of the moonpool
    /// runtime test.
    #[tokio::test(flavor = "current_thread")]
    async fn last_sequence_id_published_defaults_to_minus_one() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/last-seq-pub".to_owned(),
                ..Default::default()
            })
        };
        let slot = slot_for(&shared, handle);
        let producer = Producer::assemble(shared, handle, slot, CompressionKind::None, None);
        assert_eq!(
            producer.last_sequence_id_published(),
            -1,
            "no broker ack yet → -1 (parity with moonpool engine + Java)"
        );
    }

    /// `batch_len` reports `0` on a producer opened without batching.
    /// ADR-0024 1:1 mirror.
    #[tokio::test(flavor = "current_thread")]
    async fn batch_len_reports_zero_when_batching_disabled() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/batch-len".to_owned(),
                ..Default::default()
            })
        };
        let slot = slot_for(&shared, handle);
        let producer = Producer::assemble(shared, handle, slot, CompressionKind::None, None);
        assert_eq!(
            producer.batch_len(),
            0,
            "batching disabled → batch_len == 0"
        );
    }

    /// `batch_bytes` reports `0` on a producer opened without batching.
    /// ADR-0024 1:1 mirror.
    #[tokio::test(flavor = "current_thread")]
    async fn batch_bytes_reports_zero_when_batching_disabled() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/batch-bytes".to_owned(),
                ..Default::default()
            })
        };
        let slot = slot_for(&shared, handle);
        let producer = Producer::assemble(shared, handle, slot, CompressionKind::None, None);
        assert_eq!(
            producer.batch_bytes(),
            0,
            "batching disabled → batch_bytes == 0"
        );
    }
}
