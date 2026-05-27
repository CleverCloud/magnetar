// SPDX-License-Identifier: Apache-2.0

//! **Experimental** — PIP-466 V5 stream-consumer surface (ADR-0032).
//!
//! Wrapper over [`magnetar_runtime_tokio::Consumer`] constrained to
//! Exclusive / Failover subscriptions. The "stream" split mirrors
//! Java V5's `StreamConsumer` — guarantees ordered delivery on a
//! single active consumer per partition.

use magnetar_proto::types::MessageId;
use magnetar_runtime_tokio::{ClientError, Consumer as V4Consumer};

use crate::IncomingMessage;

/// **Experimental** — PIP-466 V5 stream consumer (ADR-0032).
/// Behaviour and signatures may change before V5 is promoted to
/// default.
///
/// Pinned to Exclusive / Failover subscriptions: a single active
/// consumer per partition, ordered delivery. Use [`super::QueueConsumer`]
/// for Shared / `KeyShared` work-distribution patterns.
#[derive(Debug)]
pub struct StreamConsumer {
    inner: V4Consumer,
}

impl StreamConsumer {
    /// Wrap an already-built v4 consumer. Callers are responsible for
    /// ensuring the underlying v4 subscription type is one of
    /// `Exclusive` / `Failover` — V5's separation between Stream and
    /// Queue is enforced at the builder layer (out of scope for this
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

    /// Receive the next message from the stream.
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
