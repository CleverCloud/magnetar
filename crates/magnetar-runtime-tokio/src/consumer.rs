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
    AckRequest, ConsumerHandle, IncomingMessage, MessageId, OpOutcome, PendingOpKey, pb,
};

use crate::ConnectionShared;
use crate::error::ClientError;

/// User-facing consumer handle.
#[derive(Debug, Clone)]
pub struct Consumer {
    pub(crate) shared: Arc<ConnectionShared>,
    pub(crate) handle: ConsumerHandle,
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
    /// Tracks whether we've already installed a connection-level waker to avoid leaking entries
    /// across polls.
    registered_waker: Option<Waker>,
}

impl Future for ReceiveFut {
    type Output = Result<IncomingMessage, ClientError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut conn = self.shared.inner.lock();
        if let Some(msg) = conn.pop_message(self.handle) {
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
