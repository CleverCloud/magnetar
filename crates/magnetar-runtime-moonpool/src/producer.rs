// SPDX-License-Identifier: Apache-2.0

//! Producer façade for the moonpool engine.
//!
//! Mirrors [`magnetar_runtime_tokio::Producer`] but is generic over
//! [`moonpool_core::Providers`] so the same façade runs on production Tokio
//! sockets and on a `moonpool-sim` deterministic substrate.
//!
//! ## Public surface
//!
//! - [`Client::open_producer`] — `CommandProducer` round-trip.
//! - [`Producer::send`] / [`Producer::flush`] / [`Producer::close`].
//! - Introspection: [`Producer::topic`], [`Producer::name`], [`Producer::is_closed`],
//!   [`Producer::pending_count`], [`Producer::last_sequence_id`], [`Producer::stats`].
//!
//! ## No-channels invariant
//!
//! Futures here follow the same pattern as the tokio engine: park on the
//! sans-io [`Connection`]'s `Waker` slab via
//! [`Connection::register_waker`], plus a single
//! [`tokio::sync::Notify`] (`driver_waker`) used as a wake-up signal across
//! the protocol-level pending queue. No `mpsc` / `oneshot` / `watch` /
//! `broadcast` channels of any flavour. See `GUIDELINES.md`
//! §"No-channels rule".
//!
//! ## Compression
//!
//! The user-facing [`Producer`] stores the [`CompressionKind`] it was opened
//! with so the broker sees the same compression metadata the state machine
//! stamps. Compression is not yet wired on the moonpool engine; calling
//! [`Producer::send`] with anything other than [`CompressionKind::None`]
//! refuses synchronously with [`ClientError::Other`]. The tokio engine's
//! ordering (compression → encryption → state machine) is mirrored so the
//! codec swap will be a drop-in once it lands.
//!
//! [`Connection`]: magnetar_proto::Connection
//! [`Connection::register_waker`]: magnetar_proto::Connection::register_waker

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::types::CompressionKind;
use magnetar_proto::{
    ConnectionEvent, CreateProducerRequest, MessageId, OpOutcome, PendingOpKey, ProducerHandle,
    ProducerStats, SequenceId,
};
use moonpool_core::Providers;

use crate::ConnectionShared;
use crate::client::{Client, ClientError};
use crate::crypto::MessageEncryptor;

/// User-facing producer handle, moonpool engine flavour.
///
/// Holds an [`Arc<ConnectionShared>`] plus a [`magnetar_proto::ProducerHandle`]
/// — cheap to clone (Arc bump). Caller-facing futures park on the sans-io
/// state machine's `Waker` slab, never on channels.
///
/// # Lock-ordering (ADR-0038)
///
/// Identity reads (topic, access mode, handle) go through `slot.identity`
/// without locking. State-machine reads take only the per-slot mutex via
/// `slot.state.lock()`. Operations that mutate the connection-wide state
/// (`send`, `flush`, `close`, …) take `shared.inner.lock()`. Acquisition
/// order: **global → per-slot, never the reverse**.
pub struct Producer<P: Providers> {
    pub(crate) shared: Arc<ConnectionShared>,
    pub(crate) handle: ProducerHandle,
    /// Direct handle to this producer's per-slot state, cloned from the
    /// Connection's registry at create time.
    pub(crate) slot: Arc<magnetar_proto::ProducerSlot>,
    pub(crate) compression: CompressionKind,
    /// Optional PIP-4 encryption hook. When present, the producer encrypts every
    /// outbound payload after compression but before handing it to the sans-io
    /// layer. 1:1 mirror of `magnetar_runtime_tokio::Producer::encryptor`.
    pub(crate) encryptor: Option<Arc<dyn MessageEncryptor>>,
    /// Held only so `Producer` is generic over `P` without leaking the
    /// driver-handle type parameter. The driver itself has already consumed
    /// the providers.
    pub(crate) _providers: std::marker::PhantomData<fn() -> P>,
}

impl<P: Providers> Clone for Producer<P> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            handle: self.handle,
            slot: self.slot.clone(),
            compression: self.compression,
            encryptor: self.encryptor.clone(),
            _providers: std::marker::PhantomData,
        }
    }
}

impl<P: Providers> std::fmt::Debug for Producer<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("handle", &self.handle)
            .field("compression", &self.compression)
            .finish_non_exhaustive()
    }
}

impl<P: Providers> Producer<P> {
    /// The protocol-layer producer handle this façade wraps.
    #[must_use]
    pub fn handle(&self) -> ProducerHandle {
        self.handle
    }

    /// Compression codec this producer was opened with. Mirrors Java
    /// `ProducerImpl#conf.getCompressionType()`. Returns
    /// [`CompressionKind::None`] when the producer was opened without
    /// explicit compression.
    #[must_use]
    pub fn compression(&self) -> CompressionKind {
        self.compression
    }

    /// Topic name this producer is bound to. Returns an empty string if the
    /// producer is no longer registered (closed).
    ///
    /// Identity-only read — takes no lock (ADR-0038).
    #[must_use]
    pub fn topic(&self) -> String {
        self.slot.identity.topic.clone()
    }

    /// Broker-assigned producer name. Returns an empty string until the
    /// broker assigns one (typically right after the `ProducerSuccess`
    /// round-trip) or if the producer is no longer registered.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn name(&self) -> String {
        self.slot.state.lock().name.clone().unwrap_or_default()
    }

    /// `true` if this producer has been closed (locally via
    /// [`Self::close`] or remotely via a broker `CloseProducer`). Mirrors
    /// Java `ProducerImpl#getState() == CLOSED`.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.slot.state.lock().closed
    }

    /// `true` while the broker connection is up. Mirrors Java
    /// `Producer#isConnected`.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.shared.inner.lock().is_connected()
    }

    /// Wall-clock timestamp of the last broker disconnection
    /// observed by this connection, or `None` if no disconnect has
    /// happened yet. Mirrors Java
    /// `Producer#getLastDisconnectedTimestamp`.
    #[must_use]
    pub fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        self.shared.inner.lock().last_disconnected_timestamp()
    }

    /// Number of in-flight sends (queued and not yet acked by the broker).
    /// Mirrors the un-batched view of Java
    /// `ProducerStats#getPendingQueueSize`.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.slot.state.lock().pending.len()
    }

    /// Last sequence id this client has pushed onto the wire. Returns `-1`
    /// if the producer has never sent. Mirrors
    /// `org.apache.pulsar.client.api.Producer#getLastSequenceId`.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn last_sequence_id(&self) -> i64 {
        self.slot.state.lock().last_sequence_id_pushed
    }

    /// Last sequence id the broker has acknowledged via
    /// `CommandSendReceipt`. Returns `-1` if no sends have been acked
    /// yet. Mirrors `org.apache.pulsar.client.api.Producer#getLastSequenceIdPublished`.
    /// Useful for resume-from-checkpoint flows.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn last_sequence_id_published(&self) -> i64 {
        self.slot.state.lock().last_sequence_id_published
    }

    /// Number of messages currently buffered in the batch container,
    /// waiting for the next flush cycle. Returns `0` when batching is
    /// disabled or the batch is empty. Mirrors the tokio runtime's
    /// `Producer::batch_len`.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn batch_len(&self) -> usize {
        self.slot.state.lock().batch.len()
    }

    /// Sum of payload bytes currently buffered in the batch container.
    /// Mirrors the tokio runtime's `Producer::batch_bytes`.
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn batch_bytes(&self) -> usize {
        self.slot.state.lock().batch.current_size_bytes
    }

    /// Snapshot of this producer's cumulative counters. Mirrors Java
    /// `org.apache.pulsar.client.api.Producer#getStats`. Returns a zeroed
    /// snapshot if the producer handle is no longer registered (closed).
    ///
    /// Per-slot read — does NOT take the global Connection mutex.
    #[must_use]
    pub fn stats(&self) -> ProducerStats {
        self.slot.state.lock().stats()
    }

    /// Enqueue a send. The returned future resolves when the broker
    /// acknowledges the publish (a `CommandSendReceipt`) or rejects it (a
    /// `CommandSendError`).
    ///
    /// # Errors
    ///
    /// - [`ClientError::Other`] if compression is requested but no codec is wired into the moonpool
    ///   engine yet.
    /// - [`ClientError::Other`] wrapping a [`magnetar_proto::ProtocolError`] if the state machine
    ///   rejects the send (e.g. closed producer, unknown handle).
    /// - [`ClientError::Broker`] if the broker subsequently rejects the publish.
    pub fn send(&self, mut msg: OutgoingMessage) -> SendFut {
        // ADR-0011 — invariant #3. Read the engine-provided wall clock
        // (moonpool TimeProvider-driven atomic) instead of the host
        // SystemTime so deterministic-simulation runs reproduce
        // bit-for-bit on the `publish_time` field. Mirrors the tokio
        // engine's snapshot semantics — same callsite, different
        // backing clock.
        let publish_time_ms = self.shared.now_wall_clock_ms();

        // The moonpool engine does not yet ship a compression codec stack.
        // The state machine still stamps `metadata.compression` based on the
        // configured `CompressionKind`; until the runtime codec lands, we
        // refuse non-`None` codecs so the broker never sees mis-labelled
        // bytes. Mirrors the tokio engine's ordering — compression goes
        // first, before the sans-io enqueue.
        if self.compression != CompressionKind::None {
            // Pre-enqueue rejection — expected anomaly surfaced as `Err` to
            // the caller, so `debug!` per ADR-0054 §2.1.
            tracing::debug!(
                compression = ?self.compression,
                "send rejected: compression not yet wired on the moonpool engine"
            );
            return SendFut {
                shared: self.shared.clone(),
                handle: self.handle,
                state: SendState::Failed {
                    error: Some(ClientError::Other(format!(
                        "moonpool engine: compression {:?} not yet wired; \
                         use CompressionKind::None for now",
                        self.compression
                    ))),
                },
                reserved_bytes: 0,
            };
        }

        // Encrypt the (post-compression) payload if a PIP-4 encryptor is wired. 1:1 mirror of
        // the tokio engine's ordering (`magnetar_runtime_tokio::Producer::send`,
        // `ProducerImpl.java:986-1003`): compression first (refused above on moonpool until
        // codecs land), encryption second, so the broker sees ciphertext and the consumer
        // reverses the order on receive. Encryption failure surfaces synchronously as a
        // `SendFut` that resolves to `ClientError::Other` on the first poll.
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

        // Reserve memory against the configured global budget BEFORE
        // handing the payload to the sans-io state machine. Mirrors Java's
        // `MemoryLimitController.reserveMemory(...)`. Two policies (Java
        // parity, see ADR-0017 and ADR-0020):
        //  - `FailImmediately`: try the CAS once; an overflow surfaces synchronously as
        //    `EngineError::MemoryLimitExceeded` wrapped in `ClientError::Engine`.
        //  - `ProducerBlock`: park the send on the runtime's Waker slab until enough budget frees
        //    up; the `Reserving` variant of `SendState` re-attempts the CAS on every poll.
        // `try_reserve_memory` is a no-op when `memory_limit_bytes = 0`
        // (the default). The fairness contract under
        // `moonpool_core::SimProviders` is documented in ADR-0022.
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
                        state: SendState::Failed {
                            error: Some(ClientError::Engine(err)),
                        },
                        reserved_bytes: 0,
                    };
                }
                self.queue_send(msg, publish_time_ms, reserved_bytes)
            }
            magnetar_proto::MemoryLimitPolicy::ProducerBlock => {
                // Fast path: budget has room right now. The slow path
                // inside `Reserving` takes over otherwise; we don't
                // synchronously park here so callers that never `.await`
                // (e.g. `Pin::poll` from a custom executor) still get a
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
                    // `Reserving` owns the reservation lifecycle itself:
                    // it only transitions to `Pending` AFTER a successful
                    // CAS, at which point it copies `bytes` into the
                    // outer `reserved_bytes`. Until then there is no
                    // reservation outstanding.
                    reserved_bytes: 0,
                }
            }
        }
    }

    /// PIP-180 / ADR-0033: replicator-style send that propagates a
    /// source-topic `MessageId` on the wire (`CommandSend.message_id`).
    /// 1:1 mirror of `magnetar_runtime_tokio::Producer::send_with_source_message_id`.
    /// Used by producers writing to a shadow topic to preserve the
    /// source-topic id chain.
    ///
    /// Bypasses batching by design — mirrors Java
    /// `org.apache.pulsar.broker.service.persistent.Replicator`. The broker
    /// echoes the asserted source id back on the resulting
    /// `CommandSendReceipt`, so the returned [`SendFut`] resolves to a
    /// [`MessageId`](magnetar_proto::MessageId) structurally equal to
    /// `source_msg_id`.
    pub fn send_with_source_message_id(
        &self,
        source_msg_id: magnetar_proto::MessageId,
        payload: impl Into<bytes::Bytes>,
        metadata: magnetar_proto::pb::MessageMetadata,
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

    /// Hand the (compressed/encrypted) message to the sans-io state
    /// machine. Assumes the `reserved_bytes` reservation has already been
    /// taken; releases it on synchronous failure so the budget reflects
    /// only actually-in-flight bytes. Mirrors the tokio engine's helper of
    /// the same name.
    ///
    /// ADR-0038 Phase 3 hot path: takes only the per-slot mutex via
    /// [`magnetar_proto::ProducerSlot::queue_send`] — does NOT acquire the
    /// global Connection mutex. The moonpool driver merges per-slot staged
    /// frames into the connection-wide outbound buffer on its next tick.
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
        // takes no lock, so this cannot self-deadlock. 1:1 with the tokio
        // engine's `queue_send` precondition.
        debug_assert_eq!(
            self.slot.identity.handle, self.handle,
            "producer slot/handle mismatch: slot is for {:?} but handle is {:?}",
            self.slot.identity.handle, self.handle,
        );

        // ADR-0011 — invariant #3. The proto state machine's monotonic
        // input flows through the engine-supplied clock provider: under
        // production TokioProviders it tracks the host clock; under
        // SimProviders it advances only as the simulator ticks. Mirrors
        // the tokio engine which captures `Instant::now` directly.
        let now = self.shared.now_instant();
        let result = self.slot.queue_send(msg, publish_time_ms, now);

        // Wake the driver so it can drain the freshly-queued frame.
        self.shared.driver_waker.notify_one();

        match result {
            Ok(seq) => {
                // NOTE: no cross-lock postcondition assert here (1:1 with the
                // tokio engine). The returned seq is computed under the
                // per-slot guard INSIDE `ProducerSlot::queue_send`; re-locking
                // afterwards to compare against `last_sequence_id_pushed`
                // raced the driver's reset/replay machinery during supervised
                // reconnects and panicked debug builds on a legal schedule.
                // The contract is pinned in the proto unit tests, under a
                // single guard.
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
                // The state machine rejected the send (e.g. producer not
                // yet open). Release the reservation so the budget
                // reflects only actually-in-flight bytes.
                self.shared.release_memory(reserved_bytes);
                // Expected anomaly surfaced as `Err` to the caller —
                // `debug!` per ADR-0054 §2.1.
                tracing::debug!(error = %err, "send rejected by producer state machine");
                SendFut {
                    shared: self.shared.clone(),
                    handle: self.handle,
                    state: SendState::Failed {
                        error: Some(ClientError::Other(format!("send: {err}"))),
                    },
                    reserved_bytes: 0,
                }
            }
        }
    }

    /// Flush this producer: force any pending batch to flush and wait for
    /// every in-flight send to be acknowledged by the broker. Idempotent —
    /// calling `flush()` on a quiescent producer returns immediately.
    ///
    /// Mirrors `org.apache.pulsar.client.api.Producer#flushAsync`. Use
    /// before `close()` if you want at-least-once semantics on the trailing
    /// sends.
    ///
    /// # Errors
    ///
    /// Currently infallible. The signature returns
    /// `Result<(), ClientError>` for parity with the tokio engine and so
    /// future drop-detection / disconnect-detection can surface errors
    /// without a breaking change.
    pub async fn flush(&self) -> Result<(), ClientError> {
        // ADR-0011 — invariant #3. Both clocks routed through the
        // engine's providers (host clock under TokioProviders, virtual
        // clock under SimProviders).
        let publish_time_ms = self.shared.now_wall_clock_ms();
        {
            let now = self.shared.now_instant();
            let mut conn = self.shared.inner.lock();
            conn.flush_producer(self.handle, publish_time_ms, now);
        }
        self.shared.driver_waker.notify_one();

        // Drain by waiting on the driver waker until the producer's pending
        // queue is empty. Each `CommandSendReceipt` decrements the pending
        // count inside the sans-io layer; the per-send `Waker`s registered
        // by [`SendFut`] wake their owners directly, and any user code
        // calling `flush` repolls the count after every `driver_waker`
        // notification. The notify cell is set by user-facing futures
        // (`send`, `close_producer`); the driver itself sets it on every
        // loop tick.
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

    /// Close this producer. The returned future resolves when the broker
    /// acknowledges the close.
    ///
    /// # Errors
    ///
    /// - [`ClientError::Broker`] if the broker returns an error correlated with the close.
    /// - [`ClientError::Other`] if an unexpected outcome arrives on the close request id.
    pub async fn close(self) -> Result<(), ClientError> {
        // Snapshot identity for the lifecycle record before the round-trip.
        let topic = self.slot.identity.topic.clone();
        let producer_name = self.slot.state.lock().name.clone().unwrap_or_default();
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.close_producer(self.handle)
        };
        self.shared.driver_waker.notify_one();
        let outcome = RequestFut {
            shared: self.shared.clone(),
            key: PendingOpKey::Request(request_id),
        }
        .await;
        match outcome {
            OpOutcome::Success { .. } => {
                // Lifecycle record (ADR-0054).
                tracing::info!(
                    topic = %topic,
                    producer_name = %producer_name,
                    handle = ?self.handle,
                    access_mode = ?self.slot.identity.access_mode,
                    "producer closed"
                );
                Ok(())
            }
            OpOutcome::Error { code, message, .. } => Err(ClientError::Broker { code, message }),
            OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
            other => Err(ClientError::Other(format!(
                "unexpected outcome for close request {request_id}: {other:?}"
            ))),
        }
    }

    /// Look up the broker-registered schema for the producer's topic
    /// (PIP-87). Mirrors Java
    /// `PulsarClientImpl#getSchema(TopicName, Optional<byte[]>)`. Used
    /// by `magnetar_proto::schema::AutoProduceBytesSchema` to warm its
    /// cache on first send.
    ///
    /// `version = None` asks for the current schema; pass
    /// `Some(schema_version_bytes)` to re-resolve a historical schema.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker rejects the lookup.
    /// - [`ClientError::Other`] when the producer handle is no longer registered or an unexpected
    ///   outcome arrives.
    pub async fn get_schema(
        &self,
        version: Option<bytes::Bytes>,
    ) -> Result<magnetar_proto::pb::Schema, ClientError> {
        let topic = self
            .shared
            .inner
            .lock()
            .producer_topic(self.handle)
            .map(str::to_owned)
            .ok_or_else(|| {
                ClientError::Other(format!(
                    "get_schema: producer handle {:?} is no longer registered",
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

impl<P: Providers + Send + Sync> Client<P> {
    /// Open a producer.
    ///
    /// Returns once the broker has sent `CommandProducerSuccess`.
    ///
    /// # Errors
    ///
    /// - [`ClientError::Closed`] if the broker closes the producer before it becomes ready (or
    ///   while we wait for the success ack).
    /// - [`ClientError::Other`] if the connection drops mid-open.
    pub async fn open_producer(
        &self,
        req: CreateProducerRequest,
    ) -> Result<Producer<P>, ClientError> {
        self.open_producer_with(req, None).await
    }

    /// Same as [`Self::open_producer`] but with an optional PIP-4 encryption hook.
    /// 1:1 mirror of `magnetar_runtime_tokio::Client::open_producer_with`.
    ///
    /// # Errors
    ///
    /// - [`ClientError::Closed`] if the broker closes the producer before it becomes ready.
    /// - [`ClientError::Other`] if the connection drops mid-open.
    pub async fn open_producer_with(
        &self,
        req: CreateProducerRequest,
        encryptor: Option<Arc<dyn MessageEncryptor>>,
    ) -> Result<Producer<P>, ClientError> {
        let compression = req.compression;
        // Pulsar requires a `CommandLookupTopic` round-trip before opening a producer or
        // consumer: lookup is what triggers the broker to acquire ownership of the topic's
        // namespace bundle. Skipping it works only when the bundle has already been activated
        // by some prior operation; a fresh broker rejects `CommandProducer` with
        // `ServerError::ServiceNotReady` ("not served by this instance, please redo the
        // lookup"). Mirrors `magnetar-runtime-tokio`'s `Client::open_producer_with` and Java's
        // `PulsarClientImpl#createProducerAsync`.
        //
        // ADR-0039 routing: detect `proxy_through_service_url = true` via
        // [`Client::lookup_topic_target`], then dispatch via [`Client::resolve_target`].
        // On a client built via `connect_plain_supervised`, the `Proxy` branch routes
        // through the per-broker connection pool (`crate::pool`); on `connect_plain` /
        // `from_parts` clients the pool is `None` and the branch surfaces
        // `ProxyUnsupportedOnUnsupervisedClient`.
        let target = self.lookup_topic_target(&req.topic).await?;
        let target_shared = self.resolve_target(&target, &req.topic).await?;
        let (handle, slot) = {
            let mut conn = target_shared.inner.lock();
            let handle = conn.create_producer(req);
            let slot = conn
                .producer(handle)
                .cloned()
                .expect("just-created producer slot must exist");
            (handle, slot)
        };
        target_shared.driver_waker.notify_one();
        wait_producer_ready(&target_shared, handle).await?;
        // Lifecycle record (ADR-0054): the broker-assigned producer name is
        // available once `ProducerReady` has landed. Per-slot read only.
        let producer_name = slot.state.lock().name.clone().unwrap_or_default();
        tracing::info!(
            topic = %slot.identity.topic,
            producer_name = %producer_name,
            handle = ?handle,
            access_mode = ?slot.identity.access_mode,
            "producer created"
        );
        Ok(Producer {
            shared: target_shared,
            handle,
            slot,
            compression,
            encryptor,
            _providers: std::marker::PhantomData,
        })
    }
}

/// Future returned by [`Producer::send`].
///
/// Polls until the matching [`OpOutcome::SendReceipt`] /
/// [`OpOutcome::SendError`] lands inside the sans-io state machine. NO
/// channel.
///
/// Holds the memory-budget reservation taken in [`Producer::send`] and
/// releases it on completion (success OR error) or on `Drop`. Mirrors Java
/// `MemoryLimitController.releaseMemory(...)`. Both policies are
/// supported: `FailImmediately` surfaces an
/// [`EngineError::MemoryLimitExceeded`] on overflow, while
/// `ProducerBlock` parks the future on
/// [`ConnectionShared::memory_wakers`] until budget frees up. See
/// [ADR-0020](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0020-memory-limit-producer-block.md)
/// for the tokio mechanism and
/// [ADR-0022](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0022-memory-limit-producer-block-moonpool.md)
/// for the moonpool-specific fairness contract under
/// [`moonpool_core::Providers`].
///
/// [`EngineError::MemoryLimitExceeded`]: crate::EngineError::MemoryLimitExceeded
/// [`ConnectionShared::memory_wakers`]: crate::ConnectionShared::memory_wakers
#[derive(Debug)]
pub struct SendFut {
    shared: Arc<ConnectionShared>,
    handle: ProducerHandle,
    state: SendState,
    /// Bytes reserved against [`ConnectionShared::memory_limit_bytes`] for
    /// this send. Released exactly once when the future returns
    /// `Poll::Ready` or is dropped (whichever comes first). `0` when no
    /// reservation was taken (the budget is unlimited, or the send failed
    /// synchronously before reserving).
    reserved_bytes: u64,
}

#[derive(Debug)]
enum SendState {
    Pending {
        sequence_id: SequenceId,
    },
    /// `send()` returned an error synchronously (e.g. producer not yet
    /// open, compression not wired). We surface it on the first `poll`.
    Failed {
        error: Option<ClientError>,
    },
    /// `MemoryLimitPolicy::ProducerBlock` saw the budget full on the
    /// synchronous fast path. Each `poll` retries the CAS via
    /// `try_reserve_memory_or_register`; on success the state transitions
    /// to `Pending`; on failure the waker is parked in the runtime's slab
    /// and dispatched when capacity frees up. `msg` is boxed so this
    /// variant doesn't dominate the `SendState` discriminant size.
    Reserving {
        msg: Option<Box<OutgoingMessage>>,
        publish_time_ms: u64,
        bytes: u64,
        slab_key: Option<usize>,
    },
}

impl Drop for SendFut {
    fn drop(&mut self) {
        // The future may be dropped before completion (caller cancelled
        // the send). Release the reservation so the budget doesn't
        // permanently leak. Note: if `poll` already released and zeroed
        // `reserved_bytes` on `Poll::Ready`, this branch is a no-op.
        if self.reserved_bytes > 0 {
            self.shared.release_memory(self.reserved_bytes);
            self.reserved_bytes = 0;
        }
        // If dropped while parked on the budget waker slab, evict the
        // slot so a later `release_memory` doesn't try to wake a dead
        // future.
        if let SendState::Reserving {
            slab_key: Some(key),
            ..
        } = &self.state
        {
            self.shared.cancel_memory_waker(*key);
        }
    }
}

impl Future for SendFut {
    type Output = Result<MessageId, ClientError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Snapshot fields before borrowing `state` mutably to keep the
        // borrow checker happy.
        let handle = self.handle;
        let shared = self.shared.clone();

        // `Reserving` needs to move `msg` out of `self.state`; handle it
        // before the borrow.
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
                        // ADR-0011 — invariant #3. Engine-provided
                        // monotonic clock so `Reserving → Pending`
                        // transitions stamp deterministic Instants
                        // into the proto state machine.
                        let now = shared.now_instant();
                        let mut conn = shared.inner.lock();
                        conn.send(handle, owned, publish_time_ms, now)
                    };
                    shared.driver_waker.notify_one();
                    match result {
                        Ok(seq) => {
                            self.state = SendState::Pending { sequence_id: seq };
                            self.reserved_bytes = bytes;
                            // Fall through to the normal match so we
                            // attempt to take the outcome immediately.
                        }
                        Err(err) => {
                            shared.release_memory(bytes);
                            return Poll::Ready(Err(ClientError::Other(format!("send: {err}"))));
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
            // Release the budget reservation. `Drop` would also catch the
            // cancellation path; this branch covers the normal completion
            // path so the count is current the instant the user observes
            // the result.
            self.shared.release_memory(self.reserved_bytes);
            self.reserved_bytes = 0;
        }
        outcome
    }
}

fn translate_send_outcome(outcome: OpOutcome) -> Result<MessageId, ClientError> {
    match outcome {
        OpOutcome::SendReceipt { message_id, .. } => Ok(message_id),
        OpOutcome::SendError { code, message, .. } => Err(ClientError::Broker { code, message }),
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
    /// not leak a [`std::task::Waker`] in the slab. For
    /// [`PendingOpKey::Send`] keys the per-producer-slot waker is also
    /// cleared by [`magnetar_proto::Connection::unregister_waker`]. Mirrors
    /// the tokio engine's
    /// [`magnetar_runtime_tokio::producer::RequestFut::drop`]. Lookup
    /// multi-agent review MEDIUM-4; ADR-0024 four-layer parity.
    fn drop(&mut self) {
        self.shared.inner.lock().unregister_waker(self.key);
    }
}

/// Future that drives the connection's semantic event queue until the
/// expected [`ConnectionEvent::ProducerReady`] (or a terminal
/// `ProducerClosedByBroker` / `Closed`) lands for the given handle.
///
/// Mirrors the tokio engine's `EventWaitFut::ProducerReady`. Unlike
/// [`RequestFut`] this watches an event stream, not a single outcome
/// slot, because the broker emits `CommandProducerSuccess` separately
/// from any request-correlated outcome — the sans-io layer surfaces it
/// as `ProducerReady`.
///
/// Each `Pending` return spawns a helper that awaits
/// `driver_waker.notified()` and wakes the caller; on the next poll (or
/// on drop) the previous helper is aborted. Without that abort, the
/// stale helper from an earlier poll lingers on
/// `driver_waker.notified()` and competes with the driver loop for
/// `notify_one` permits emitted by user-facing futures.
struct ProducerReadyFut {
    shared: Arc<ConnectionShared>,
    handle: ProducerHandle,
    helper: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for ProducerReadyFut {
    fn drop(&mut self) {
        if let Some(h) = self.helper.take() {
            h.abort();
        }
    }
}

impl Future for ProducerReadyFut {
    type Output = Result<(), ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut conn = this.shared.inner.lock();
        loop {
            match conn.poll_event() {
                Some(ConnectionEvent::ProducerReady { handle, .. }) => {
                    if handle == this.handle {
                        return Poll::Ready(Ok(()));
                    }
                }
                Some(ConnectionEvent::ProducerClosedByBroker {
                    handle,
                    assigned_broker_service_url,
                }) => {
                    if handle == this.handle {
                        // Broker-forced close — degraded-but-recovering
                        // (warn! per ADR-0054 §2.1); the open future
                        // surfaces `Closed` and the caller decides. Mirror
                        // of the tokio engine's `EventWaitFut` arm.
                        let topic = conn
                            .producer(handle)
                            .map(|s| s.identity.topic.clone())
                            .unwrap_or_default();
                        tracing::warn!(
                            handle = ?handle,
                            topic = %topic,
                            assigned_broker_service_url = assigned_broker_service_url
                                .as_deref()
                                .map(crate::log_fields::truncate_broker_str),
                            "broker closed producer while waiting for ProducerReady"
                        );
                        return Poll::Ready(Err(ClientError::Closed));
                    }
                }
                Some(ConnectionEvent::ProducerOpenFailed {
                    handle,
                    code,
                    message,
                }) => {
                    if handle == this.handle {
                        return Poll::Ready(Err(ClientError::Broker { code, message }));
                    }
                }
                Some(ConnectionEvent::Closed { reason }) => {
                    // Broker/connection-level forced close — warn! per
                    // ADR-0054 §2.1. `reason` is broker-controlled text.
                    tracing::warn!(
                        reason = reason
                            .as_deref()
                            .map(crate::log_fields::truncate_broker_str),
                        "connection closed while waiting for producer readiness"
                    );
                    return Poll::Ready(Err(ClientError::Other(
                        reason.unwrap_or_else(|| "connection closed".into()),
                    )));
                }
                Some(_) => {} // ignore unrelated events
                None => break,
            }
        }
        drop(conn);

        // We have no per-event waker slot in the sans-io layer; park on the
        // driver waker. Every inbound batch ends with the driver looping
        // back to `select!`, which gives any pending `notified()` a chance
        // to fire as the next loop tick.
        if let Some(prev) = this.helper.take() {
            prev.abort();
        }
        let waker = cx.waker().clone();
        let shared = this.shared.clone();
        this.helper = Some(tokio::spawn(async move {
            shared.driver_waker.notified().await;
            waker.wake();
        }));
        Poll::Pending
    }
}

async fn wait_producer_ready(
    shared: &Arc<ConnectionShared>,
    handle: ProducerHandle,
) -> Result<(), ClientError> {
    ProducerReadyFut {
        shared: shared.clone(),
        handle,
        helper: None,
    }
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::{Bytes, BytesMut};
    use magnetar_proto::producer::OutgoingMessage;
    use magnetar_proto::types::{CompressionKind, ProducerHandle};
    use magnetar_proto::{ConnectionConfig, CreateProducerRequest, encode_command, pb};
    use moonpool_core::TokioProviders;

    use super::Producer;
    use crate::client::{Client, ClientError};
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

    /// Spin up a `ConnectionShared` whose inner state machine has completed
    /// the handshake, so `create_producer` runs cleanly without erroring
    /// on protocol-state checks.
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

    /// Capture the per-slot Arc for a `handle` known to be in the registry.
    fn slot_for(
        shared: &Arc<ConnectionShared>,
        handle: ProducerHandle,
    ) -> Arc<magnetar_proto::ProducerSlot> {
        shared
            .inner
            .lock()
            .producer(handle)
            .cloned()
            .expect("test producer slot must exist")
    }

    /// Placeholder slot for tests that intentionally use an unknown handle.
    fn stub_slot_for_test(handle: ProducerHandle) -> Arc<magnetar_proto::ProducerSlot> {
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
    /// encrypt hook ran on the pre-encryption bytes. 1:1 mirror of the tokio
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
    /// enqueues a pending op (no driver running drains it). 1:1 with the tokio
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
        let encryptor = Arc::new(XorEncryptor::default());
        let producer: Producer<TokioProviders> = Producer {
            shared: shared.clone(),
            handle,
            slot: slot_for(&shared, handle),
            compression: CompressionKind::None,
            encryptor: Some(encryptor.clone()),
            _providers: std::marker::PhantomData,
        };
        let _fut = producer.send(OutgoingMessage {
            payload: Bytes::from_static(b"plain-secret"),
            metadata: pb::MessageMetadata::default(),
            uncompressed_size: 12,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        });
        assert_eq!(
            encryptor.seen_plaintext.lock().unwrap().as_deref(),
            Some(b"plain-secret".as_slice()),
            "encrypt hook must see the pre-encryption payload",
        );
        assert!(
            producer.pending_count() >= 1,
            "expected pending encrypted send; got {}",
            producer.pending_count()
        );
    }

    /// Encryptor that always fails. Exercises the producer-side encrypt-error
    /// branch (`send` surfaces `ClientError::Other("encrypt: …")`). 1:1 with the
    /// tokio `send_encrypt_failure_surfaces_error`.
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
    /// tokio `send_encrypt_failure_surfaces_error`.
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
        let producer: Producer<TokioProviders> = Producer {
            shared: shared.clone(),
            handle,
            slot: slot_for(&shared, handle),
            compression: CompressionKind::None,
            encryptor: Some(Arc::new(FailingEncryptor)),
            _providers: std::marker::PhantomData,
        };
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

    /// Smoke test: a freshly-constructed producer reports defaults that
    /// match the sans-io layer (no sends pushed, none pending, no name).
    #[tokio::test(flavor = "current_thread")]
    async fn fresh_producer_reports_defaults() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/defaults".to_owned(),
                ..Default::default()
            })
        };
        let producer: Producer<TokioProviders> = Producer {
            shared: shared.clone(),
            handle,
            slot: slot_for(&shared, handle),
            compression: CompressionKind::None,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
        assert_eq!(producer.pending_count(), 0);
        assert_eq!(producer.last_sequence_id(), -1);
        assert!(!producer.is_closed());
        assert_eq!(producer.name(), "");
        assert_eq!(producer.topic(), "persistent://public/default/defaults");
        assert_eq!(producer.compression(), CompressionKind::None);
        let stats = producer.stats();
        assert_eq!(stats.total_msgs_sent, 0);
        assert_eq!(stats.pending_queue_size, 0);
    }

    /// `send` on a freshly-opened (post-handshake) producer enqueues the
    /// frame into the sans-io state machine; `pending_count` flips to 1
    /// because no driver is running to drain the `CommandSendReceipt`.
    #[tokio::test(flavor = "current_thread")]
    async fn send_enqueues_pending_op() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/enqueue".to_owned(),
                ..Default::default()
            })
        };
        let slot = slot_for(&shared, handle);
        let producer: Producer<TokioProviders> = Producer {
            shared,
            handle,
            slot,
            compression: CompressionKind::None,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
        let _fut = producer.send(OutgoingMessage {
            payload: Bytes::from_static(b"hello"),
            metadata: pb::MessageMetadata::default(),
            uncompressed_size: 5,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        });
        assert!(
            producer.pending_count() >= 1,
            "expected pending send; got {}",
            producer.pending_count()
        );
    }

    /// `send` with a non-`None` compression codec yields a `SendFut` that
    /// resolves to `ClientError::Other` on the first poll. Until the
    /// moonpool engine ships a runtime codec, the producer refuses to
    /// hand mis-labelled bytes to the state machine.
    #[tokio::test(flavor = "current_thread")]
    async fn send_with_compression_returns_error() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/zstd".to_owned(),
                ..Default::default()
            })
        };
        let slot = slot_for(&shared, handle);
        let producer: Producer<TokioProviders> = Producer {
            shared,
            handle,
            slot,
            compression: CompressionKind::Zstd,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
        let res = producer
            .send(OutgoingMessage {
                payload: Bytes::from_static(b"hello"),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 5,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            })
            .await;
        let err = res.expect_err("expected error for unwired compression");
        let s = format!("{err}");
        assert!(
            s.contains("not yet wired"),
            "expected compression-not-wired message, got {s:?}"
        );
    }

    /// `flush()` on a quiescent producer returns immediately. Idempotency
    /// guarantee mirrored from the tokio engine.
    #[tokio::test(flavor = "current_thread")]
    async fn flush_quiescent_is_noop() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/flush-ok".to_owned(),
                ..Default::default()
            })
        };
        let slot = slot_for(&shared, handle);
        let producer: Producer<TokioProviders> = Producer {
            shared,
            handle,
            slot,
            compression: CompressionKind::None,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
        assert_eq!(producer.pending_count(), 0);
        tokio::time::timeout(Duration::from_secs(1), producer.flush())
            .await
            .expect("flush should resolve on quiescent producer")
            .expect("flush ok");
    }

    /// Producer façade is `Clone` (cheap Arc bump). Confirm both clones
    /// share the same handle.
    #[test]
    fn producer_clones_share_handle() {
        let shared = handshake_complete_shared();
        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/clone".to_owned(),
                ..Default::default()
            })
        };
        let slot = slot_for(&shared, handle);
        let producer: Producer<TokioProviders> = Producer {
            shared,
            handle,
            slot,
            compression: CompressionKind::None,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
        let clone = producer.clone();
        assert_eq!(producer.handle(), clone.handle());
        assert_eq!(producer.compression(), clone.compression());
    }

    /// `Client::open_producer` against `TokioProviders` resolves at the
    /// type level. We can't construct a `Client` without a real
    /// connection, so the bound is checked through the free function
    /// below.
    #[allow(dead_code)]
    fn _open_producer_bounds<P: moonpool_core::Providers>(
        client: &Client<P>,
        req: CreateProducerRequest,
    ) -> impl std::future::Future<Output = Result<super::Producer<P>, super::ClientError>> + '_
    {
        client.open_producer(req)
    }

    /// Smoke: `Client::connect_plain` is generic over `TokioProviders` and
    /// the engine's surface composes with the producer module.
    #[test]
    #[allow(clippy::let_underscore_future, clippy::no_effect_underscore_binding)]
    fn open_producer_compiles_against_tokio_providers() {
        let providers = TokioProviders::new();
        let engine = MoonpoolEngine::new(providers);
        let _client_fut =
            Client::connect_plain(&engine, "127.0.0.1:6650", ConnectionConfig::default());
    }

    /// `send` reserves payload bytes against the configured memory budget
    /// (FailImmediately policy). Once enqueued, `ConnectionShared::memory_used`
    /// reflects the reservation. Dropping the `SendFut` (the test stand-in
    /// for cancellation) releases the reservation.
    #[tokio::test(flavor = "current_thread")]
    async fn send_reserves_and_releases_memory_budget() {
        let cfg = ConnectionConfig {
            memory_limit_bytes: 1024,
            ..ConnectionConfig::default()
        };
        let shared = ConnectionShared::new(cfg);
        // Seed the handshake by hand so create_producer succeeds.
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
                topic: "persistent://public/default/budget".to_owned(),
                ..Default::default()
            })
        };
        let producer: Producer<TokioProviders> = Producer {
            shared: shared.clone(),
            handle,
            slot: slot_for(&shared, handle),
            compression: CompressionKind::None,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
        let fut = producer.send(OutgoingMessage {
            payload: Bytes::from_static(b"abcdef"),
            metadata: pb::MessageMetadata::default(),
            uncompressed_size: 6,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        });
        assert_eq!(
            shared
                .memory_used
                .load(std::sync::atomic::Ordering::Acquire),
            6,
            "payload bytes must be reserved against the budget"
        );
        drop(fut);
        assert_eq!(
            shared
                .memory_used
                .load(std::sync::atomic::Ordering::Acquire),
            0,
            "dropping the SendFut must release the reservation"
        );
    }

    /// `send` with a payload larger than the memory budget refuses
    /// synchronously (FailImmediately policy). The budget counter stays at
    /// zero — the reservation never lands.
    #[tokio::test(flavor = "current_thread")]
    async fn send_fails_when_memory_budget_would_overflow() {
        let cfg = ConnectionConfig {
            memory_limit_bytes: 4,
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
                topic: "persistent://public/default/overflow".to_owned(),
                ..Default::default()
            })
        };
        let producer: Producer<TokioProviders> = Producer {
            shared: shared.clone(),
            handle,
            slot: slot_for(&shared, handle),
            compression: CompressionKind::None,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
        let res = producer
            .send(OutgoingMessage {
                payload: Bytes::from_static(b"too-big-payload"),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 15,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            })
            .await;
        assert!(matches!(
            res,
            Err(super::ClientError::Engine(
                super::super::EngineError::MemoryLimitExceeded { .. }
            ))
        ));
        assert_eq!(
            shared
                .memory_used
                .load(std::sync::atomic::Ordering::Acquire),
            0,
            "rejected sends must not bump the budget counter"
        );
    }

    /// Regression for the CLI "produce hangs against fresh broker" bug: when the broker
    /// rejects a producer-open with a PERMANENT `CommandError` (e.g.
    /// `AuthorizationError`), the moonpool engine's `wait_producer_ready` must surface
    /// a `ClientError::Broker { code, message }` rather than parking on the driver
    /// waker forever. Mirrors the proto-level
    /// `command_error_on_producer_open_with_permanent_code_emits_producer_open_failed`
    /// test, but covers the engine-side bridge from event to future-result.
    /// `ServiceNotReady` / `MetadataError` / `TopicNotFound` are deliberately NOT used
    /// here — those are transient (the runtime retries via
    /// `retry_producer_open`).
    #[tokio::test(flavor = "current_thread")]
    async fn wait_producer_ready_surfaces_broker_error() {
        let shared = handshake_complete_shared();
        let (handle, request_id) = {
            let mut conn = shared.inner.lock();
            let request_id = conn.peek_next_request_id_for_test();
            let handle = conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/forbidden".to_owned(),
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

        // The fix replaces an unbounded wait with a typed Broker error. Hard-cap the await
        // with a tight timeout so a regression would surface as `Elapsed`, not as a hung
        // test process.
        let res = tokio::time::timeout(
            Duration::from_secs(2),
            super::wait_producer_ready(&shared, handle),
        )
        .await
        .expect("producer-ready future must resolve (regression: previously hung)");
        match res {
            Err(super::ClientError::Broker { code, message }) => {
                assert_eq!(code, pb::ServerError::AuthorizationError as i32);
                assert_eq!(message, "not authorized");
            }
            other => panic!("expected ClientError::Broker, got {other:?}"),
        }
    }

    /// `ProducerBlock`: an overflowing send must NOT error synchronously.
    /// The `SendFut` parks in the `Reserving` state with a waker
    /// registered on `ConnectionShared::memory_wakers`. We poll the
    /// future once via `noop_waker` to land it in `Pending`, then verify
    /// the slab carries our registration.
    #[tokio::test(flavor = "current_thread")]
    async fn producer_block_parks_on_overflow_instead_of_erroring() {
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
        // Pre-fill the budget so the next `send` cannot reserve.
        shared
            .try_reserve_memory(4)
            .expect("seeding the budget at the limit");

        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/block".to_owned(),
                ..Default::default()
            })
        };
        let producer: Producer<TokioProviders> = Producer {
            shared: shared.clone(),
            handle,
            slot: slot_for(&shared, handle),
            compression: CompressionKind::None,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
        let mut fut = producer.send(OutgoingMessage {
            payload: Bytes::from_static(b"overflow"),
            metadata: pb::MessageMetadata::default(),
            uncompressed_size: 8,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        });
        // Poll once: the future must register on the waker slab and
        // return `Poll::Pending`.
        let waker = futures_task_waker();
        let mut cx = Context::from_waker(&waker);
        let poll = Pin::new(&mut fut).poll(&mut cx);
        assert!(
            matches!(poll, Poll::Pending),
            "ProducerBlock must park instead of erroring (got {poll:?})"
        );
        assert_eq!(
            shared.memory_wakers.lock().len(),
            1,
            "Reserving must register exactly one waker"
        );
        // Drop the future: the registered waker must be evicted so the
        // next release does not wake a dead future.
        drop(fut);
        assert!(
            shared.memory_wakers.lock().is_empty(),
            "dropping the SendFut must cancel its registration"
        );
    }

    /// `ProducerBlock`: releasing the held budget drains every parked
    /// waker. The drained slot must be evicted from the slab so a
    /// later `release_memory` does not double-wake.
    #[tokio::test(flavor = "current_thread")]
    async fn producer_block_release_drains_wakers() {
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
        // Saturate the budget so the next `send` parks.
        shared
            .try_reserve_memory(4)
            .expect("seeding the budget at the limit");

        let handle = {
            let mut conn = shared.inner.lock();
            conn.create_producer(CreateProducerRequest {
                topic: "persistent://public/default/release".to_owned(),
                ..Default::default()
            })
        };
        let producer: Producer<TokioProviders> = Producer {
            shared: shared.clone(),
            handle,
            slot: slot_for(&shared, handle),
            compression: CompressionKind::None,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
        let mut fut = producer.send(OutgoingMessage {
            payload: Bytes::from_static(b"AB"),
            metadata: pb::MessageMetadata::default(),
            uncompressed_size: 2,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        });
        let waker = futures_task_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));
        assert_eq!(shared.memory_wakers.lock().len(), 1);

        // Release the seed reservation. The drain must empty the slab.
        shared.release_memory(4);
        assert!(
            shared.memory_wakers.lock().is_empty(),
            "release_memory must drain the slab"
        );

        // The drop guard cleans up `fut`'s reservation if it took one.
        drop(fut);
    }

    /// `ProducerBlock`: a fully-released budget completes the parked
    /// reservation on the next poll. We park the future, drop the prior
    /// holder, then re-poll: the future advances from `Reserving` to
    /// `Pending`, the budget counter reflects the new reservation, and
    /// the slab is empty.
    #[tokio::test(flavor = "current_thread")]
    async fn producer_block_completes_when_budget_frees_up() {
        use std::future::Future as _;
        use std::pin::Pin;
        use std::sync::atomic::Ordering;
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
                topic: "persistent://public/default/free".to_owned(),
                ..Default::default()
            })
        };
        let producer: Producer<TokioProviders> = Producer {
            shared: shared.clone(),
            handle,
            slot: slot_for(&shared, handle),
            compression: CompressionKind::None,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
        let mut fut = producer.send(OutgoingMessage {
            payload: Bytes::from_static(b"ab"),
            metadata: pb::MessageMetadata::default(),
            uncompressed_size: 2,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        });
        let waker = futures_task_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));

        // Free the seed; the drain wakes every parked future.
        shared.release_memory(4);
        assert_eq!(shared.memory_used.load(Ordering::Acquire), 0);

        // Re-poll: the future reserves its 2 bytes, transitions to
        // `Pending`, and stays pending waiting for the broker receipt
        // (no driver is running here).
        let poll = Pin::new(&mut fut).poll(&mut cx);
        assert!(
            matches!(poll, Poll::Pending),
            "still waiting on broker receipt"
        );
        assert_eq!(
            shared.memory_used.load(Ordering::Acquire),
            2,
            "the released budget must have been re-reserved by the parked send"
        );
        assert!(
            shared.memory_wakers.lock().is_empty(),
            "successful reservation must clear the slab slot"
        );

        // Drop releases the reservation back to zero.
        drop(fut);
        assert_eq!(shared.memory_used.load(Ordering::Acquire), 0);
    }

    /// `ProducerBlock`: fast-path success when the budget has room takes
    /// the synchronous `queue_send` return on line 242 (no `SendFut` slow
    /// path, no slab insert). Mirrors the `FailImmediately` fast path but
    /// proves the early return on the `ProducerBlock` side.
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
        let producer: Producer<TokioProviders> = Producer {
            shared: shared.clone(),
            handle,
            slot: slot_for(&shared, handle),
            compression: CompressionKind::None,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
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
    /// `ProtocolError::InvariantViolation("unknown producer handle")`.
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
        // Saturate the budget so the first `send` lands in `Reserving`,
        // then release and re-poll: this drives the future through the
        // `Reserving → Ok(()) → conn.send → Err(_)` path.
        shared.try_reserve_memory(16).expect("seed budget");
        // Fabricate a producer handle that the state machine does NOT know
        // about. `ProducerHandle` is a transparent wrapper around `u64`; we
        // pick an id that the `create_producer` path won't have allocated.
        let bogus_handle = ProducerHandle(u64::MAX);
        let producer: Producer<TokioProviders> = Producer {
            shared: shared.clone(),
            handle: bogus_handle,
            slot: stub_slot_for_test(bogus_handle),
            compression: CompressionKind::None,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
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
            Poll::Ready(Err(ClientError::Other(msg))) => {
                assert!(
                    msg.contains("send:"),
                    "expected `send:` error prefix, got {msg:?}",
                );
            }
            other => panic!("expected Ready(Err(Other(...))), got {other:?}"),
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
    /// new one (line 549). Two polls park the same future twice; we
    /// assert the slab carries exactly one entry after the second poll
    /// (the prior slot must have been cancelled, not leaked).
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
        let producer: Producer<TokioProviders> = Producer {
            shared: shared.clone(),
            handle,
            slot: slot_for(&shared, handle),
            compression: CompressionKind::None,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
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
        // re-registers and evicts the prior slot (line 549).
        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));
        assert_eq!(
            shared.memory_wakers.lock().len(),
            1,
            "re-park must cancel the prior waker before inserting a new one",
        );
    }

    /// Build a no-op `Waker` suitable for synchronously polling futures
    /// in tests. We rely on `tokio`'s public re-export rather than
    /// hand-rolling unsafe raw-waker glue. `tokio::sync::Notify` already
    /// drives the production wake path; this helper is test-only so we
    /// can drive `SendFut::poll` deterministically without spinning up
    /// the executor.
    fn futures_task_waker() -> std::task::Waker {
        // `noop_waker` is stable via `std::task::Waker::noop`
        // (Rust 1.85+). The workspace MSRV is 1.85 per ADR-0007 so we
        // can use it directly.
        std::task::Waker::noop().clone()
    }

    /// `last_sequence_id_published` reports `-1` until the broker has
    /// acked at least one send. Mirrors the tokio runtime's
    /// `Producer::last_sequence_id_published`. ADR-0024 1:1 mirror.
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
        let producer: Producer<TokioProviders> = Producer {
            shared,
            handle,
            slot,
            compression: CompressionKind::None,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
        assert_eq!(
            producer.last_sequence_id_published(),
            -1,
            "no broker ack yet → -1 (parity with tokio engine + Java)"
        );
    }

    /// `batch_len` reports `0` on a producer opened without batching.
    /// Mirrors the tokio runtime's `Producer::batch_len`. ADR-0024 1:1.
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
        let producer: Producer<TokioProviders> = Producer {
            shared,
            handle,
            slot,
            compression: CompressionKind::None,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
        assert_eq!(
            producer.batch_len(),
            0,
            "batching disabled → batch_len == 0"
        );
    }

    /// `batch_bytes` reports `0` on a producer opened without batching.
    /// Mirrors the tokio runtime's `Producer::batch_bytes`. ADR-0024 1:1.
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
        let producer: Producer<TokioProviders> = Producer {
            shared,
            handle,
            slot,
            compression: CompressionKind::None,
            encryptor: None,
            _providers: std::marker::PhantomData,
        };
        assert_eq!(
            producer.batch_bytes(),
            0,
            "batching disabled → batch_bytes == 0"
        );
    }
}
