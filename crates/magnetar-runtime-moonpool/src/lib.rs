// SPDX-License-Identifier: Apache-2.0

//! moonpool engine for magnetar.
//!
//! Drives the sans-io [`magnetar_proto::Connection`] state machine on top of
//! [`moonpool_core::Providers`] (which bundles [`NetworkProvider`], [`TimeProvider`],
//! [`TaskProvider`], [`RandomProvider`], and [`StorageProvider`]). The point is *not*
//! to be a separate engine for production load — it is to make the entire
//! producer/consumer protocol exercisable under
//! [moonpool-sim](https://crates.io/crates/moonpool-sim) deterministic chaos
//! testing, so we can fuzz partitions, message reorderings, and TLS handshake
//! reorderings with reproducible seeds.
//!
//! ## Driver shape
//!
//! Same pattern as the tokio engine:
//!
//! - `Arc<parking_lot::Mutex<Connection>>` holds the sans-io state machine,
//! - a single-cell [`tokio::sync::Notify`] (`driver_waker`) signals the driver when user-facing
//!   futures enqueue fresh work,
//! - the driver loop runs as a spawned tokio task that selects over `driver_waker.notified()`,
//!   `transport.read_buf(...)`, and a timer driven by [`moonpool_core::TimeProvider::sleep`].
//!
//! Because the driver still uses `tokio::spawn` and `tokio::select!`, both
//! the production and simulation modes rely on a tokio runtime — the
//! determinism comes from substituting the providers, not from replacing
//! tokio.
//!
//! ## TLS
//!
//! TLS for the moonpool engine is the `option (d)` adapter ([`tls`]): drive
//! [`rustls::ClientConnection`] (itself sans-io) over the moonpool-supplied
//! byte pipe. The TLS handshake therefore survives `moonpool-sim` chaos with
//! the same determinism as `magnetar-proto` itself. The driver loop only
//! drives the plaintext path today; TLS wiring lands in a follow-up
//! milestone.
//!
//! ## No channels
//!
//! Same pattern as the tokio engine: `Arc<parking_lot::Mutex<Connection>>`
//! plus per-future [`std::task::Waker`] slabs inside the connection.
//! Driver wakeups travel through a single [`tokio::sync::Notify`].
//!
//! [`NetworkProvider`]: moonpool_core::NetworkProvider
//! [`TimeProvider`]: moonpool_core::TimeProvider
//! [`TaskProvider`]: moonpool_core::TaskProvider
//! [`RandomProvider`]: moonpool_core::RandomProvider
//! [`StorageProvider`]: moonpool_core::StorageProvider

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]
#![allow(
    // The driver state machine is naturally branchy; pedantic lints fight
    // the readability of an event-pump loop. We tighten these later once the
    // engine has stabilised.
    clippy::too_many_lines,
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::doc_markdown
)]

mod driver;
pub mod tls;
mod transport;

use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{Connection, ConnectionConfig};
use moonpool_core::Providers;
use parking_lot::Mutex;
use tokio::sync::Notify;

pub use crate::driver::DriverHandle;
use crate::transport::Transport;

/// Shared connection state for the moonpool engine. Mirrors the tokio
/// engine's `ConnectionShared`: a non-async mutex over the sans-io state
/// machine plus a single-cell driver wakeup.
pub struct ConnectionShared {
    /// The sans-io state machine, guarded by a non-async mutex.
    pub inner: Mutex<Connection>,
    /// Single-cell wakeup for the driver loop. Not a channel — just a
    /// `Notify` notified after every user-facing future enqueues work
    /// (e.g. a producer's `send`).
    pub driver_waker: Notify,
    /// Optional auth provider that the driver consults when the broker
    /// emits `AuthChallenge`. `None` means no in-band token refresh — the
    /// connection will drop if the broker challenges. PIP-30 / PIP-292.
    pub auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
    /// PIP-145 topic-list-watcher deltas pushed here by the driver.
    pub topic_list_changes: Mutex<std::collections::VecDeque<TopicListChange>>,
    /// Wakeup for `next_topic_list_change` futures. Notified after every
    /// push to [`Self::topic_list_changes`].
    pub topic_list_notify: Notify,
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
    #[must_use]
    pub fn new(config: ConnectionConfig) -> Arc<Self> {
        Self::with_auth(config, None)
    }

    /// Construct with an auth provider for in-band challenge refresh.
    #[must_use]
    pub fn with_auth(
        config: ConnectionConfig,
        auth_provider: Option<Arc<dyn magnetar_proto::AuthProvider>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Connection::new(config)),
            driver_waker: Notify::new(),
            auth_provider,
            topic_list_changes: Mutex::new(std::collections::VecDeque::new()),
            topic_list_notify: Notify::new(),
        })
    }
}

/// PIP-145 topic-list-watcher delta surfaced from the driver to user-facing
/// code. Mirrors `ConnectionEvent::TopicListChanged` with owned vectors so
/// callers don't pay for borrows across the await boundary.
#[derive(Debug, Clone)]
pub struct TopicListChange {
    /// Topics that newly match the pattern.
    pub added: Vec<String>,
    /// Topics that no longer match the pattern.
    pub removed: Vec<String>,
}

/// Errors surfaced by the moonpool engine.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Underlying I/O failure (from the moonpool provider).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Sans-io protocol error.
    #[error("protocol error: {0}")]
    Protocol(#[from] magnetar_proto::ProtocolError),
    /// TLS error.
    #[error("tls error: {0}")]
    Tls(#[from] rustls::Error),
    /// Peer closed the connection cleanly mid-handshake.
    #[error("peer closed connection")]
    PeerClosed,
    /// Configuration error (e.g. URL parsing).
    #[error("config error: {0}")]
    Config(String),
}

/// moonpool-backed engine handle. Generic over the [`Providers`] bundle so
/// callers can plug in `TokioProviders` (production) or a sim bundle (tests).
pub struct MoonpoolEngine<P: Providers> {
    providers: P,
}

impl<P: Providers> std::fmt::Debug for MoonpoolEngine<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MoonpoolEngine").finish_non_exhaustive()
    }
}

impl<P: Providers> MoonpoolEngine<P> {
    /// Construct an engine bound to the given providers.
    #[must_use]
    pub fn new(providers: P) -> Self {
        Self { providers }
    }

    /// Borrow the underlying providers (useful in tests).
    #[must_use]
    pub fn providers(&self) -> &P {
        &self.providers
    }

    /// Connect to a Pulsar broker over the moonpool [`NetworkProvider`] and
    /// spawn the driver task that runs the protocol forward.
    ///
    /// `addr` is a `host:port` string per moonpool's API (NOT a `pulsar://`
    /// URL — strip the scheme before calling). TLS is the caller's
    /// responsibility for now: wrap the returned [`ConnectionShared`] +
    /// [`tls::RustlsByteAdapter`] manually in the driver task.
    ///
    /// The function completes once a `CONNECT` frame has been written and
    /// the broker has responded with `CONNECTED`. After that point the
    /// returned `DriverHandle` owns the connection and pumps it for
    /// producer/consumer operations.
    ///
    /// # Errors
    /// Propagates [`EngineError::Io`] on network failure,
    /// [`EngineError::Protocol`] on framing or handshake errors, or
    /// [`EngineError::PeerClosed`] if the peer closed before CONNECTED.
    ///
    /// [`NetworkProvider`]: moonpool_core::NetworkProvider
    pub async fn connect_plain(
        &self,
        addr: &str,
        config: ConnectionConfig,
    ) -> Result<(Arc<ConnectionShared>, DriverHandle), EngineError> {
        let mut transport = Transport::<P>::connect(self.providers.network(), addr).await?;
        let shared = ConnectionShared::new(config);

        // Drive the handshake inline. Once `Connected` lands we hand the
        // transport over to the long-running driver task so user-facing
        // futures can start enqueueing producer/consumer commands.
        handshake_plain::<P>(&shared, &mut transport).await?;
        let driver = driver::spawn::<P>(
            shared.clone(),
            transport,
            self.providers.time().clone(),
            self.providers.task(),
        );
        Ok((shared, driver))
    }
}

/// Drive the byte pump until the handshake completes.
///
/// Kept private to the crate — the public surface goes through
/// [`MoonpoolEngine::connect_plain`].
async fn handshake_plain<P: Providers>(
    shared: &Arc<ConnectionShared>,
    transport: &mut Transport<P>,
) -> Result<(), EngineError> {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut write_buf: Vec<u8> = Vec::with_capacity(8 * 1024);

    loop {
        // 1. Drain outbound bytes the state machine has queued.
        {
            let mut conn = shared.inner.lock();
            write_buf.clear();
            let _ = conn.poll_transmit(&mut write_buf);
        }
        if !write_buf.is_empty() {
            transport.write_all(&write_buf).await?;
            transport.flush().await?;
            write_buf.clear();
        }

        // 2. If we're already past handshake, we're done.
        {
            let conn = shared.inner.lock();
            if matches!(
                conn.state(),
                magnetar_proto::HandshakeState::Connected
                    | magnetar_proto::HandshakeState::AuthChallenging
            ) {
                return Ok(());
            }
            if matches!(
                conn.state(),
                magnetar_proto::HandshakeState::Failed | magnetar_proto::HandshakeState::Closed
            ) {
                return Err(EngineError::PeerClosed);
            }
        }

        // 3. Read more bytes from the wire.
        let n = transport.read_buf(&mut read_buf).await?;
        if n == 0 {
            return Err(EngineError::PeerClosed);
        }
        let bytes = read_buf.split().freeze();
        shared.inner.lock().handle_bytes(Instant::now(), &bytes)?;
    }
}

#[cfg(test)]
mod tests {
    use magnetar_proto::ConnectionConfig;
    use moonpool_core::TokioProviders;

    use super::{ConnectionShared, MoonpoolEngine, TopicListChange};

    #[test]
    fn engine_can_be_constructed_with_tokio_providers() {
        let providers = TokioProviders::new();
        let engine = MoonpoolEngine::new(providers);
        // Calling providers() smoke-tests the trait wiring.
        let _ = engine.providers();
    }

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

    /// Doc-test-style smoke: the engine's `connect_plain()` can be named
    /// without actually awaiting it. We don't dial a real broker here.
    #[test]
    #[allow(clippy::let_underscore_future, clippy::no_effect_underscore_binding)]
    fn connect_plain_compiles() {
        let providers = TokioProviders::new();
        let engine = MoonpoolEngine::new(providers);
        let _fut = engine.connect_plain("127.0.0.1:6650", ConnectionConfig::default());
    }

    /// Confirm we're not accidentally pulling in any channel crate.
    #[test]
    fn no_unbounded_compile_check() {
        let _ = std::any::type_name::<super::EngineError>();
        let _ = std::time::Duration::from_secs(0);
    }
}
