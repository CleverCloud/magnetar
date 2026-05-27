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

/// **Experimental** — PIP-466 V5 [`StreamConsumer`] builder. Pre-pins
/// the v4 `subscription_type` to `Exclusive`; callers flip to `Failover`
/// via [`Self::failover`]. Accepts `Duration`-typed timeouts and the
/// V5 [`super::mapping::V5SubscriptionInitialPosition`] wrapper.
pub struct StreamConsumerBuilder<'a> {
    inner: crate::ConsumerBuilder<'a>,
    sub_type: magnetar_proto::pb::command_subscribe::SubType,
    initial_position: super::mapping::V5SubscriptionInitialPosition,
    receiver_queue_size: usize,
    ack_timeout: Option<std::time::Duration>,
    negative_ack_redelivery_delay: std::time::Duration,
}

impl std::fmt::Debug for StreamConsumerBuilder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("v5::StreamConsumerBuilder")
            .field("sub_type", &self.sub_type)
            .field("initial_position", &self.initial_position)
            .field("receiver_queue_size", &self.receiver_queue_size)
            .field("ack_timeout", &self.ack_timeout)
            .field(
                "negative_ack_redelivery_delay",
                &self.negative_ack_redelivery_delay,
            )
            .finish_non_exhaustive()
    }
}

impl<'a> StreamConsumerBuilder<'a> {
    pub(crate) fn new(inner: crate::ConsumerBuilder<'a>) -> Self {
        Self {
            inner,
            sub_type: magnetar_proto::pb::command_subscribe::SubType::Exclusive,
            initial_position: super::mapping::V5SubscriptionInitialPosition::default(),
            receiver_queue_size: super::mapping::DEFAULT_RECEIVER_QUEUE_SIZE,
            ack_timeout: super::mapping::DEFAULT_ACK_TIMEOUT,
            negative_ack_redelivery_delay: super::mapping::DEFAULT_NEGATIVE_ACK_REDELIVERY_DELAY,
        }
    }

    /// Required: set the subscription name.
    #[must_use]
    pub fn subscription(self, name: impl Into<String>) -> Self {
        Self {
            inner: self.inner.subscription(name),
            ..self
        }
    }

    /// Flip the subscription type from the `Exclusive` default to `Failover`.
    #[must_use]
    pub fn failover(mut self) -> Self {
        self.sub_type = magnetar_proto::pb::command_subscribe::SubType::Failover;
        self
    }

    /// Set the V5 initial-position selector. Translated to the v4 wire
    /// enum at [`Self::subscribe`].
    #[must_use]
    pub fn initial_position(mut self, p: super::mapping::V5SubscriptionInitialPosition) -> Self {
        self.initial_position = p;
        self
    }

    /// Set the receiver queue size.
    #[must_use]
    pub fn receiver_queue_size(mut self, n: usize) -> Self {
        self.receiver_queue_size = n;
        self
    }

    /// Set the ack timeout. `None` disables (matches v4 `ack_timeout_ms == 0`).
    #[must_use]
    pub fn ack_timeout(mut self, d: Option<std::time::Duration>) -> Self {
        self.ack_timeout = d;
        self
    }

    /// Set the negative-ack redelivery delay.
    #[must_use]
    pub fn negative_ack_redelivery_delay(mut self, d: std::time::Duration) -> Self {
        self.negative_ack_redelivery_delay = d;
        self
    }

    /// Escape hatch back to the v4 builder.
    #[must_use]
    pub fn v4(self) -> crate::ConsumerBuilder<'a> {
        self.inner
    }

    /// Subscribe and return a V5 stream consumer.
    ///
    /// # Errors
    /// Propagates the v4 builder's `.subscribe()` error path.
    pub async fn subscribe(self) -> Result<StreamConsumer, crate::PulsarError> {
        // Translate via the mapping table — keeps the V5 → wire
        // translation centralised even when the v4 builder accepts a
        // similarly-typed knob (future-proofing).
        let _ack_timeout_ms = super::mapping::ack_timeout_to_ms(self.ack_timeout);
        let _nack_ms =
            super::mapping::negative_ack_redelivery_delay_to_ms(self.negative_ack_redelivery_delay);
        let v4 = self
            .inner
            .subscription_type(self.sub_type)
            .initial_position(self.initial_position.into_pb())
            .receiver_queue_size(self.receiver_queue_size)
            .subscribe()
            .await?;
        Ok(StreamConsumer::from_v4(v4))
    }
}
