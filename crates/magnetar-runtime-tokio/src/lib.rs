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

pub mod auto_cluster_failover;
mod client;
pub mod compress;
mod consumer;
pub mod crypto;
pub mod dns;
mod driver;
mod error;
mod producer;
pub mod tls_insecure;
pub mod tls_no_hostname;
mod transport;
mod url_parse;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::Mutex;
use tokio::sync::Notify;

pub use crate::auto_cluster_failover::{AutoClusterFailover, HealthProbe, HealthProbeFuture};
pub use crate::client::Client;
pub use crate::compress::CompressionError;
pub use crate::consumer::{Consumer, ReceiveFut};
pub use crate::crypto::{EncryptError, MessageDecryptor, MessageEncryptor};
pub use crate::dns::{DnsResolveFuture, DnsResolver, TokioDnsResolver, arc_dns_resolver};
pub use crate::driver::DriverHandle;
pub use crate::error::ClientError;
pub use crate::producer::{Producer, SendFut};
pub use crate::tls_insecure::insecure_tls_config;
pub use crate::tls_no_hostname::tls_config_no_hostname;
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
    /// Configured global publish memory budget in bytes. `0` disables the limit
    /// (matches `ConnectionConfig::memory_limit_bytes` default). Mirrors Java's
    /// `ClientBuilder#memoryLimit`. Reservations against this budget happen in
    /// [`crate::Producer::send`] BEFORE the payload reaches the sans-io state
    /// machine; sends that would push `memory_used` past the limit are rejected
    /// synchronously with [`ClientError::MemoryLimitExceeded`].
    pub memory_limit_bytes: u64,
    /// Current in-flight publish bytes reserved by [`crate::Producer::send`] calls
    /// that have not yet seen their [`magnetar_proto::OpOutcome::SendReceipt`] /
    /// `SendError`. Bumped in `send` (CAS against `memory_limit_bytes`); decremented
    /// in [`crate::SendFut::poll`] when the future returns `Poll::Ready`.
    pub memory_used: AtomicU64,
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

    /// Try to reserve `bytes` against the configured memory budget. Returns
    /// `Ok(())` when the reservation succeeds (or no limit is configured —
    /// `memory_limit_bytes = 0`); returns `Err(ClientError::MemoryLimitExceeded
    /// { current, limit, requested })` when the reservation would push
    /// `memory_used` past `memory_limit_bytes`.
    ///
    /// Lock-free: a CAS loop on `memory_used`. Mirrors Java's
    /// `MemoryLimitController` (in `MemoryLimitPolicy.FailImmediately`
    /// mode).
    ///
    /// See [ADR-0003](https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0003-no-channels-rule.md)
    /// — `AtomicU64` is not a channel; it's the right primitive for this counter.
    pub fn try_reserve_memory(&self, bytes: u64) -> Result<(), ClientError> {
        if self.memory_limit_bytes == 0 {
            return Ok(());
        }
        loop {
            let current = self.memory_used.load(Ordering::Acquire);
            let next = current.saturating_add(bytes);
            if next > self.memory_limit_bytes {
                return Err(ClientError::MemoryLimitExceeded {
                    current,
                    limit: self.memory_limit_bytes,
                    requested: bytes,
                });
            }
            // Acquire-Release CAS so that releases on other threads are visible.
            if self
                .memory_used
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
            // Lost the race; retry with the fresh value.
        }
    }

    /// Release a previous reservation. Called by [`crate::SendFut`] when the
    /// send completes (success or error). Saturating sub so a buggy
    /// over-release can't underflow the counter.
    pub fn release_memory(&self, bytes: u64) {
        if bytes == 0 || self.memory_limit_bytes == 0 {
            return;
        }
        // `fetch_sub` wraps on underflow; guard manually with a CAS loop.
        loop {
            let current = self.memory_used.load(Ordering::Acquire);
            let next = current.saturating_sub(bytes);
            if self
                .memory_used
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Construct with an auth provider for in-band challenge refresh.
    pub fn with_auth(
        config: magnetar_proto::ConnectionConfig,
        auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
    ) -> Arc<Self> {
        let memory_limit_bytes = config.memory_limit_bytes;
        Arc::new(Self {
            inner: Mutex::new(magnetar_proto::Connection::new(config)),
            driver_waker: Notify::new(),
            auth_provider,
            topic_list_changes: Mutex::new(std::collections::VecDeque::new()),
            topic_list_notify: Notify::new(),
            pending_rebuild: AtomicBool::new(false),
            memory_limit_bytes,
            memory_used: AtomicU64::new(0),
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

    #[test]
    fn memory_limit_zero_disables_enforcement() {
        let s = ConnectionShared::new(ConnectionConfig::default());
        assert_eq!(s.memory_limit_bytes, 0);
        assert!(s.try_reserve_memory(u64::MAX).is_ok());
        // No-op release.
        s.release_memory(u64::MAX);
    }

    #[test]
    fn memory_limit_reserve_and_release_round_trip() {
        let cfg = ConnectionConfig {
            memory_limit_bytes: 1024,
            ..ConnectionConfig::default()
        };
        let s = ConnectionShared::new(cfg);

        assert!(s.try_reserve_memory(400).is_ok());
        assert!(s.try_reserve_memory(400).is_ok());
        assert_eq!(s.memory_used.load(super::Ordering::Acquire), 800);

        // Overflow: 800 + 300 > 1024.
        match s.try_reserve_memory(300) {
            Err(super::ClientError::MemoryLimitExceeded {
                current,
                limit,
                requested,
            }) => {
                assert_eq!(current, 800);
                assert_eq!(limit, 1024);
                assert_eq!(requested, 300);
            }
            other => panic!("expected MemoryLimitExceeded, got {other:?}"),
        }

        // Releasing makes room.
        s.release_memory(400);
        assert!(s.try_reserve_memory(300).is_ok());
    }

    #[test]
    fn memory_limit_release_is_saturating() {
        let cfg = ConnectionConfig {
            memory_limit_bytes: 1024,
            ..ConnectionConfig::default()
        };
        let s = ConnectionShared::new(cfg);
        // Over-release must not underflow.
        s.release_memory(1_000_000);
        assert_eq!(s.memory_used.load(super::Ordering::Acquire), 0);
    }
}
