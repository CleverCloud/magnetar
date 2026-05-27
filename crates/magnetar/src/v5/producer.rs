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
