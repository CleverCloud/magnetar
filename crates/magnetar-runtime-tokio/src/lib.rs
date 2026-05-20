// SPDX-License-Identifier: Apache-2.0

//! Tokio engine for magnetar.
//!
//! Drives the sans-io [`magnetar_proto::Connection`] state machine over a tokio TCP stream,
//! optionally wrapped with `tokio-rustls`. One driver task per connection, no channels.
//!
//! # Quickstart
//!
//! ```no_run
//! use magnetar_proto::{ConnectionConfig, CreateProducerRequest};
//! use magnetar_proto::producer::OutgoingMessage;
//! use magnetar_runtime_tokio::Client;
//!
//! # async fn run() -> Result<(), magnetar_runtime_tokio::ClientError> {
//! let client = Client::connect("pulsar://localhost:6650", ConnectionConfig::default()).await?;
//!
//! let producer = client.open_producer(CreateProducerRequest {
//!     topic: "persistent://public/default/example".to_owned(),
//!     ..Default::default()
//! }).await?;
//!
//! let mut msg = OutgoingMessage {
//!     payload: bytes::Bytes::from_static(b"hello"),
//!     metadata: Default::default(),
//!     uncompressed_size: 5,
//!     num_messages: 1,
//! };
//! msg.metadata.producer_name = "demo".to_owned();
//! let _id = producer.send(msg).await?;
//!
//! client.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! # No channels
//!
//! This crate does not use any flavour of channel (mpsc / broadcast / watch / oneshot). The
//! pattern is documented in [GUIDELINES.md] §"No-channels rule":
//!
//! - User-facing futures lock `Arc<parking_lot::Mutex<magnetar_proto::Connection>>` directly.
//! - Driver wake-ups travel through a single-cell [`tokio::sync::Notify`].
//! - Future completion uses [`Waker`](core::task::Waker) slabs inside the sans-io state machine,
//!   registered via [`magnetar_proto::Connection::register_waker`] and dispatched when the matching
//!   [`magnetar_proto::OpOutcome`] lands.
//!
//! [GUIDELINES.md]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/GUIDELINES.md

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]
#![allow(
    // The driver state machine is naturally branchy; pedantic lints fight the readability of
    // an event-pump loop. We tighten these later once the engine has stabilised.
    clippy::too_many_lines,
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::doc_markdown
)]

mod client;
mod consumer;
mod driver;
mod error;
mod producer;
mod transport;
mod url_parse;

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;

pub use crate::client::Client;
pub use crate::consumer::{Consumer, ReceiveFut};
pub use crate::driver::DriverHandle;
pub use crate::error::ClientError;
pub use crate::producer::{Producer, SendFut};
pub use crate::url_parse::{ParsedUrl, Scheme};

/// Shared connection state — the lock-protected sans-io state machine + a single-cell driver
/// wake-up.
///
/// Cheap to share via `Arc`. The mutex is `parking_lot::Mutex` (not async), held only for the
/// duration of a sans-io call (no `.await` inside the critical section).
#[derive(Debug)]
pub struct ConnectionShared {
    /// The sans-io state machine, guarded by a non-async mutex.
    pub inner: Mutex<magnetar_proto::Connection>,
    /// Single-cell wakeup for the driver loop. Not a channel.
    pub driver_waker: Notify,
}

impl ConnectionShared {
    /// Construct shared state from the given protocol-layer config.
    pub fn new(config: magnetar_proto::ConnectionConfig) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(magnetar_proto::Connection::new(config)),
            driver_waker: Notify::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use magnetar_proto::ConnectionConfig;

    use super::ConnectionShared;

    #[test]
    fn shared_state_can_be_constructed() {
        let s = ConnectionShared::new(ConnectionConfig::default());
        let _g = s.inner.lock();
    }
}
