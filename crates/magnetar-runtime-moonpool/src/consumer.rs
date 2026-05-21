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
//!   on the driver wakeup until one arrives.
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
//! work, and on the shared [`tokio::sync::Notify`] driver wakeup for
//! handle-correlated work (subscribe ack, message arrival). No `mpsc` /
//! `oneshot` / `watch` / `broadcast` channels of any flavour. See
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
    /// TODO(M4 follow-up): the state machine currently exposes only
    /// per-request waker slots; per-consumer message-arrival wakers will land
    /// alongside flow-control work in a later milestone. For now we park on
    /// the connection-level driver wakeup, which means `receive()` can
    /// spuriously wake but never misses a message.
    ///
    /// # Errors
    /// - [`ClientError::Closed`] if the connection has been closed before a message arrives.
    pub async fn receive(&self) -> Result<IncomingMessage, ClientError> {
        ReceiveFut {
            shared: self.shared.clone(),
            handle: self.handle,
        }
        .await
    }

    /// Acknowledge a single message (individual ack). Resolves once the
    /// broker confirms via `CommandAckResponse`.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker reports an ack failure.
    /// - [`ClientError::Other`] when an unexpected outcome arrives on this request id
    ///   (state-machine bug, not a transient failure).
    pub async fn ack(&self, message_id: MessageId) -> Result<(), ClientError> {
        self.ack_inner(vec![message_id], pb::command_ack::AckType::Individual)
            .await
    }

    /// Acknowledge a cumulative position. Resolves once the broker confirms.
    ///
    /// # Errors
    /// - [`ClientError::Broker`] when the broker reports an ack failure.
    /// - [`ClientError::Other`] when an unexpected outcome arrives.
    pub async fn ack_cumulative(&self, message_id: MessageId) -> Result<(), ClientError> {
        self.ack_inner(vec![message_id], pb::command_ack::AckType::Cumulative)
            .await
    }

    async fn ack_inner(
        &self,
        message_ids: Vec<MessageId>,
        ack_type: pb::command_ack::AckType,
    ) -> Result<(), ClientError> {
        let request_id = {
            let mut conn = self.shared.inner.lock();
            conn.ack(
                self.handle,
                AckRequest {
                    message_ids,
                    ack_type,
                    properties: Vec::new(),
                    txn_id: None,
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
        let now = std::time::Instant::now();
        {
            let mut conn = self.shared.inner.lock();
            conn.negative_ack(self.handle, vec![message_id], now);
        }
        self.shared.driver_waker.notify_one();
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
/// per-consumer queue, parking on the driver wakeup until one arrives.
///
/// Mirrors the tokio engine's `ReceiveFut`. See the TODO on
/// [`Consumer::receive`] for the per-consumer waker plan.
struct ReceiveFut {
    shared: Arc<ConnectionShared>,
    handle: ConsumerHandle,
}

impl Future for ReceiveFut {
    type Output = Result<IncomingMessage, ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut conn = self.shared.inner.lock();
        if let Some(msg) = conn.pop_message(self.handle) {
            drop(conn);
            // pop_message may have queued FLOW frames; wake the driver to flush.
            self.shared.driver_waker.notify_one();
            return Poll::Ready(Ok(msg));
        }
        // Closed connection with no buffered message → terminal.
        if conn.is_closed() || conn.consumer_is_closed(self.handle) {
            return Poll::Ready(Err(ClientError::Closed));
        }
        drop(conn);
        // Re-arm via the driver wake-up. The driver task notifies *all* parked
        // tasks after any inbound bytes are processed.
        let notified = self.shared.driver_waker.notified();
        tokio::pin!(notified);
        if notified.as_mut().enable() {
            // Already notified: poll immediately.
            cx.waker().wake_by_ref();
        }
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
        };
        let err = fut.await.expect_err("receive must surface Closed");
        assert!(matches!(err, crate::client::ClientError::Closed));
    }
}
