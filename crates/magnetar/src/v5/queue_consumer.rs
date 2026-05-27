// SPDX-License-Identifier: Apache-2.0

//! **Experimental** — PIP-466 V5 queue-consumer surface (ADR-0032).
//!
//! Wrapper over [`magnetar_runtime_tokio::Consumer`] constrained to
//! Shared / `KeyShared` subscriptions. The "queue" split mirrors Java
//! V5's `QueueConsumer` — work-distribution patterns across multiple
//! active consumers per partition.

use magnetar_proto::types::MessageId;
use magnetar_runtime_tokio::{ClientError, Consumer as V4Consumer};

use crate::IncomingMessage;

/// **Experimental** — PIP-466 V5 queue consumer (ADR-0032). Behaviour
/// and signatures may change before V5 is promoted to default.
///
/// Pinned to Shared / `KeyShared` subscriptions: multiple active
/// consumers per partition, work-distribution semantics. Use
/// [`super::StreamConsumer`] for Exclusive / Failover ordered
/// delivery.
#[derive(Debug)]
pub struct QueueConsumer {
    inner: V4Consumer,
}

impl QueueConsumer {
    /// Wrap an already-built v4 consumer. Callers are responsible for
    /// ensuring the underlying v4 subscription type is one of
    /// `Shared` / `KeyShared` — V5's separation between Queue and
    /// Stream is enforced at the builder layer (out of scope for this
    /// initial scaffold).
    #[must_use]
    pub fn from_v4(inner: V4Consumer) -> Self {
        Self { inner }
    }

    /// Escape hatch back to the v4 consumer.
    #[must_use]
    pub fn v4(&self) -> &V4Consumer {
        &self.inner
    }

    /// Consume the V5 wrapper and return the inner v4 consumer.
    #[must_use]
    pub fn into_v4(self) -> V4Consumer {
        self.inner
    }

    /// Receive the next message from the queue.
    ///
    /// # Errors
    /// Propagates [`ClientError`] from the underlying v4 consumer
    /// (transport drop, consumer-closed, decrypt failure, …).
    pub async fn receive(&self) -> Result<IncomingMessage, ClientError> {
        let msg = self.inner.receive().await?;
        Ok(msg.into())
    }

    /// Acknowledge a single message.
    ///
    /// # Errors
    /// Propagates [`ClientError`] from the underlying v4 consumer.
    pub async fn ack(&self, id: MessageId) -> Result<(), ClientError> {
        self.inner.ack(id).await
    }
}
