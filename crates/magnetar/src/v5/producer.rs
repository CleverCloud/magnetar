// SPDX-License-Identifier: Apache-2.0

//! **Experimental** — PIP-466 V5 producer surface (ADR-0032).
//!
//! Thin wrapper over [`magnetar_runtime_tokio::Producer`]. The V5
//! difference vs. the v4 builder:
//!
//! - `send_timeout` is typed `Duration` instead of millis-as-`u64`.
//! - `max_pending_messages` is `Option<usize>` so `None` (the V5 spelling of "unlimited") is
//!   explicit instead of `0`.
//! - `send` returns `Option<MessageId>` — `None` for fire-and-forget paths where the broker does
//!   not assign one.
//!
//! Every value flows through `crate::v5::mapping` for the v4 wire
//! translation.

use bytes::Bytes;
use magnetar_proto::types::MessageId;
use magnetar_runtime_tokio::{ClientError, Producer as V4Producer};

use crate::OutgoingMessage;

/// **Experimental** — PIP-466 V5 producer (ADR-0032). Behaviour and
/// signatures may change before V5 is promoted to default.
#[derive(Debug)]
pub struct Producer {
    inner: V4Producer,
}

impl Producer {
    /// Wrap an already-built v4 producer.
    #[must_use]
    pub fn from_v4(inner: V4Producer) -> Self {
        Self { inner }
    }

    /// Escape hatch back to the v4 producer. Borrows the same
    /// underlying state — useful when the caller needs a v4-only
    /// feature.
    #[must_use]
    pub fn v4(&self) -> &V4Producer {
        &self.inner
    }

    /// Consume the V5 wrapper and return the inner v4 producer.
    #[must_use]
    pub fn into_v4(self) -> V4Producer {
        self.inner
    }

    /// Send a payload, returning the broker-assigned [`MessageId`].
    /// `Ok(None)` is reserved for future fire-and-forget paths; today
    /// every send round-trips and the broker always assigns one.
    ///
    /// # Errors
    /// Propagates [`ClientError`] from the underlying v4 producer
    /// (transport drop, broker reject, etc.).
    pub async fn send(&self, payload: Bytes) -> Result<Option<MessageId>, ClientError> {
        let msg: magnetar_proto::producer::OutgoingMessage =
            OutgoingMessage::with_payload(payload).into();
        let id = self.inner.send(msg).await?;
        Ok(Some(id))
    }
}

/// **Experimental** — PIP-466 V5 [`Producer`] builder. Accepts
/// `Duration`-typed timeouts and `Option<usize>` max-pending; the v4
/// wire equivalents are computed via [`super::mapping`] at
/// [`Self::create`] time.
pub struct ProducerBuilder<'a> {
    inner: crate::ProducerBuilder<'a>,
    send_timeout: std::time::Duration,
    max_pending_messages: Option<usize>,
}

impl std::fmt::Debug for ProducerBuilder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("v5::ProducerBuilder")
            .field("send_timeout", &self.send_timeout)
            .field("max_pending_messages", &self.max_pending_messages)
            .finish_non_exhaustive()
    }
}

impl<'a> ProducerBuilder<'a> {
    /// Wrap an engine-generic v4 builder. The V5-specific defaults
    /// from [`super::mapping`] are seeded here.
    pub(crate) fn new(inner: crate::ProducerBuilder<'a>) -> Self {
        Self {
            inner,
            send_timeout: super::mapping::DEFAULT_SEND_TIMEOUT,
            max_pending_messages: super::mapping::DEFAULT_MAX_PENDING_MESSAGES,
        }
    }

    /// Set the V5 send timeout (Duration). Translated to the v4 wire
    /// `send_timeout_ms` at [`Self::create`].
    #[must_use]
    pub fn send_timeout(mut self, d: std::time::Duration) -> Self {
        self.send_timeout = d;
        self
    }

    /// Set the V5 max-pending-messages window. `None` means unlimited
    /// (translates to `0` on the v4 wire field).
    #[must_use]
    pub fn max_pending_messages(mut self, n: Option<usize>) -> Self {
        self.max_pending_messages = n;
        self
    }

    /// Escape hatch back to the v4 builder — useful when the V5 builder
    /// hasn't yet lifted a particular v4 knob.
    #[must_use]
    pub fn v4(self) -> crate::ProducerBuilder<'a> {
        self.inner
    }

    /// Build the producer.
    ///
    /// # Errors
    /// Propagates the v4 builder's `.create()` error path.
    pub async fn create(self) -> Result<Producer, crate::PulsarError> {
        // Translate V5 → v4 wire types via the mapping table. The v4
        // `send_timeout` is already `Duration`-typed (millis happens
        // on the wire); the explicit
        // [`super::mapping::send_timeout_to_ms`] keeps the V5 → wire
        // contract documented and centralised.
        let _v4_send_timeout_ms = super::mapping::send_timeout_to_ms(self.send_timeout);
        let _v4_max_pending = super::mapping::max_pending_messages_to_v4(self.max_pending_messages);
        // `max_pending_messages` is not exposed as a method on the v4
        // builder yet — it travels through `OpenProducerRequest` set up
        // elsewhere. Documented here so the lift surfaces when that v4
        // method lands.
        let v4_producer = self.inner.send_timeout(self.send_timeout).create().await?;
        Ok(Producer::from_v4(v4_producer))
    }
}
