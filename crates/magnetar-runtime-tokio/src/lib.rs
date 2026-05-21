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
//!     txn_id: None,
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
//! pattern is documented in [GUIDELINES.md] §"No-channels rule" and atomised in
//! [ADR-0003](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0003-no-channels-rule.md):
//!
//! - User-facing futures lock `Arc<parking_lot::Mutex<magnetar_proto::Connection>>` directly.
//! - Driver wake-ups travel through a single-cell [`tokio::sync::Notify`].
//! - Future completion uses [`Waker`](core::task::Waker) slabs inside the sans-io state machine,
//!   registered via [`magnetar_proto::Connection::register_waker`] and dispatched when the matching
//!   [`magnetar_proto::OpOutcome`] lands.
//!
//! See also [ADR-0004](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md)
//! (sans-io split) and [ADR-0011](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0011-clock-injection-sans-io.md)
//! (clock injection on state-machine entries).
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
pub mod compress;
mod consumer;
pub mod crypto;
pub mod dns;
mod driver;
mod error;
mod producer;
pub mod tls_insecure;
mod transport;
mod url_parse;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use parking_lot::Mutex;
use tokio::sync::Notify;

pub use crate::client::Client;
pub use crate::compress::CompressionError;
pub use crate::consumer::{Consumer, ReceiveFut};
pub use crate::crypto::{EncryptError, MessageDecryptor, MessageEncryptor};
pub use crate::dns::{DnsResolveFuture, DnsResolver, TokioDnsResolver, arc_dns_resolver};
pub use crate::driver::DriverHandle;
pub use crate::error::ClientError;
pub use crate::producer::{Producer, SendFut};
pub use crate::tls_insecure::insecure_tls_config;
pub use crate::transport::default_tls_config;
pub use crate::url_parse::{ParsedUrl, Scheme};

/// Shared connection state — the lock-protected sans-io state machine + a single-cell driver
/// wake-up.
///
/// Cheap to share via `Arc`. The mutex is `parking_lot::Mutex` (not async), held only for the
/// duration of a sans-io call (no `.await` inside the critical section).
pub struct ConnectionShared {
    /// The sans-io state machine, guarded by a non-async mutex.
    pub inner: Mutex<magnetar_proto::Connection>,
    /// Single-cell wakeup for the driver loop. Not a channel.
    pub driver_waker: Notify,
    /// Optional auth provider that the driver consults when the broker emits
    /// [`CommandAuthChallenge`](magnetar_proto::pb::CommandAuthChallenge).
    /// `None` means no in-band token refresh — the connection will drop if the
    /// broker challenges. PIP-30 / PIP-292.
    pub auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
    /// PIP-145 topic-list-watcher deltas. The driver pushes
    /// [`magnetar_proto::ConnectionEvent::TopicListChanged`] events here as the broker
    /// emits them; surface them via [`Client::next_topic_list_change`].
    pub topic_list_changes: Mutex<std::collections::VecDeque<TopicListChange>>,
    /// Wakeup for `next_topic_list_change` futures. Notified after every push to
    /// `topic_list_changes`.
    pub topic_list_notify: Notify,
    /// Set by the auto-reconnect supervisor between [`magnetar_proto::Connection::reset`] and
    /// the new socket's handshake. When `true`, the driver loop runs
    /// [`magnetar_proto::Connection::rebuild_producers`] +
    /// [`magnetar_proto::Connection::rebuild_consumers`] the first time it observes the new
    /// session transitioning to [`magnetar_proto::HandshakeState::Connected`], then clears
    /// the flag so the rebuild fires exactly once per reconnect. Stage 3 of the supervisor
    /// work: transparent producer / consumer replay on session loss.
    pub pending_rebuild: AtomicBool,
}

/// PIP-145 topic-list-watcher delta surfaced from the driver to the user-facing
/// [`Client`]. Mirrors `ConnectionEvent::TopicListChanged` with owned vectors so callers
/// don't pay for borrows across the await boundary.
#[derive(Debug, Clone)]
pub struct TopicListChange {
    /// Topics that newly match the pattern.
    pub added: Vec<String>,
    /// Topics that no longer match the pattern.
    pub removed: Vec<String>,
}

impl std::fmt::Debug for ConnectionShared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionShared")
            .field("inner", &"<Connection>")
            .field("has_auth_provider", &self.auth_provider.is_some())
            .finish_non_exhaustive()
    }
}

impl ConnectionShared {
    /// Construct shared state from the given protocol-layer config.
    pub fn new(config: magnetar_proto::ConnectionConfig) -> Arc<Self> {
        Self::with_auth(config, None)
    }

    /// Construct with an auth provider for in-band challenge refresh.
    pub fn with_auth(
        config: magnetar_proto::ConnectionConfig,
        auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(magnetar_proto::Connection::new(config)),
            driver_waker: Notify::new(),
            auth_provider,
            topic_list_changes: Mutex::new(std::collections::VecDeque::new()),
            topic_list_notify: Notify::new(),
            pending_rebuild: AtomicBool::new(false),
        })
    }
}

#[cfg(test)]
mod tests {
    use magnetar_proto::ConnectionConfig;

    use super::{ConnectionShared, TopicListChange};

    #[test]
    fn shared_state_can_be_constructed() {
        let s = ConnectionShared::new(ConnectionConfig::default());
        let _g = s.inner.lock();
        // Topic-list buffer starts empty.
        assert!(s.topic_list_changes.lock().is_empty());
    }

    #[test]
    fn topic_list_changes_buffer_round_trip() {
        let s = ConnectionShared::new(ConnectionConfig::default());
        s.topic_list_changes.lock().push_back(TopicListChange {
            added: vec!["a".to_owned()],
            removed: vec![],
        });
        s.topic_list_changes.lock().push_back(TopicListChange {
            added: vec![],
            removed: vec!["b".to_owned()],
        });
        let first = s.topic_list_changes.lock().pop_front().unwrap();
        assert_eq!(first.added, vec!["a".to_owned()]);
        let second = s.topic_list_changes.lock().pop_front().unwrap();
        assert_eq!(second.removed, vec!["b".to_owned()]);
        assert!(s.topic_list_changes.lock().is_empty());
    }
}
