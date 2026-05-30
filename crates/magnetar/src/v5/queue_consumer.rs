// SPDX-License-Identifier: Apache-2.0

//! **Experimental** — PIP-466 V5 queue-consumer surface (ADR-0032).
//!
//! Wrapper over [`magnetar_runtime_tokio::Consumer`] constrained to
//! Shared / `KeyShared` subscriptions. The "queue" split mirrors Java
//! V5's `QueueConsumer` — work-distribution patterns across multiple
//! active consumers per partition.

use magnetar_proto::types::MessageId;

use crate::{ConsumerApi, Engine, IncomingMessage, SubscribeApi, TokioEngine};

/// **Experimental** — PIP-466 V5 queue consumer (ADR-0032). Behaviour
/// and signatures may change before V5 is promoted to default.
///
/// Pinned to Shared / `KeyShared` subscriptions: multiple active
/// consumers per partition, work-distribution semantics. Use
/// [`super::StreamConsumer`] for Exclusive / Failover ordered
/// delivery.
///
/// Engine-generic.
pub struct QueueConsumer<E: Engine = TokioEngine>
where
    E::ClientState: SubscribeApi,
{
    inner: <E::ClientState as SubscribeApi>::Consumer,
}

impl<E: Engine> std::fmt::Debug for QueueConsumer<E>
where
    E::ClientState: SubscribeApi,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("v5::QueueConsumer")
            .field("engine", &E::name())
            .finish_non_exhaustive()
    }
}

impl<E: Engine> QueueConsumer<E>
where
    E::ClientState: SubscribeApi,
{
    /// Wrap an already-built v4 consumer. Callers are responsible for
    /// ensuring the underlying v4 subscription type is one of
    /// `Shared` / `KeyShared` — V5's separation between Queue and
    /// Stream is enforced at the builder layer (out of scope for this
    /// initial scaffold).
    #[must_use]
    pub fn from_v4(inner: <E::ClientState as SubscribeApi>::Consumer) -> Self {
        Self { inner }
    }

    /// Escape hatch back to the v4 consumer.
    #[must_use]
    pub fn v4(&self) -> &<E::ClientState as SubscribeApi>::Consumer {
        &self.inner
    }

    /// Consume the V5 wrapper and return the inner v4 consumer.
    #[must_use]
    pub fn into_v4(self) -> <E::ClientState as SubscribeApi>::Consumer {
        self.inner
    }

    /// Receive the next message from the queue.
    ///
    /// # Errors
    /// Propagates the runtime error from the underlying v4 consumer
    /// (transport drop, consumer-closed, decrypt failure, …).
    pub async fn receive(
        &self,
    ) -> Result<IncomingMessage, <<E::ClientState as SubscribeApi>::Consumer as ConsumerApi>::Error>
    {
        let msg = ConsumerApi::receive(&self.inner).await?;
        Ok(msg.into())
    }

    /// Acknowledge a single message.
    ///
    /// # Errors
    /// Propagates the runtime error from the underlying v4 consumer.
    pub async fn ack(
        &self,
        id: MessageId,
    ) -> Result<(), <<E::ClientState as SubscribeApi>::Consumer as ConsumerApi>::Error> {
        ConsumerApi::ack(&self.inner, id).await
    }
}

/// **Experimental** — PIP-466 V5 [`QueueConsumer`] builder. Pre-pins
/// the v4 `subscription_type` to `Shared`; callers flip to `KeyShared`
/// via [`Self::key_shared`]. Same V5-typed knob set as
/// [`super::stream_consumer::StreamConsumerBuilder`].
///
/// Engine-generic.
pub struct QueueConsumerBuilder<'a, E: Engine = TokioEngine> {
    inner: crate::ConsumerBuilder<'a, E>,
    sub_type: magnetar_proto::pb::command_subscribe::SubType,
    initial_position: super::mapping::V5SubscriptionInitialPosition,
    receiver_queue_size: usize,
    ack_timeout: Option<std::time::Duration>,
    negative_ack_redelivery_delay: std::time::Duration,
}

impl<E: Engine> std::fmt::Debug for QueueConsumerBuilder<'_, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("v5::QueueConsumerBuilder")
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

impl<'a, E: Engine> QueueConsumerBuilder<'a, E> {
    pub(crate) fn new(inner: crate::ConsumerBuilder<'a, E>) -> Self {
        Self {
            inner,
            sub_type: magnetar_proto::pb::command_subscribe::SubType::Shared,
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

    /// Flip the subscription type from the `Shared` default to `KeyShared`.
    #[must_use]
    pub fn key_shared(mut self) -> Self {
        self.sub_type = magnetar_proto::pb::command_subscribe::SubType::KeyShared;
        self
    }

    /// Set the V5 initial-position selector.
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

    /// Set the ack timeout. `None` disables.
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
    pub fn v4(self) -> crate::ConsumerBuilder<'a, E> {
        self.inner
    }

    /// Subscribe and return a V5 queue consumer.
    ///
    /// # Errors
    /// Propagates the v4 builder's `.subscribe()` error path.
    pub async fn subscribe(self) -> Result<QueueConsumer<E>, crate::PulsarError>
    where
        E::ClientState: SubscribeApi,
    {
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
        Ok(QueueConsumer::from_v4(v4))
    }
}
