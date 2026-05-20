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
