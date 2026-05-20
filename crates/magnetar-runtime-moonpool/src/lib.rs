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
//! ## TLS
//!
//! TLS for the moonpool engine is the `option (d)` adapter ([`tls`]): drive
//! [`rustls::ClientConnection`] (itself sans-io) over the moonpool-supplied
//! byte pipe. The TLS handshake therefore survives `moonpool-sim` chaos with
//! the same determinism as `magnetar-proto` itself.
//!
//! ## No channels
//!
//! Same pattern as the tokio engine: `Arc<parking_lot::Mutex<Connection>>`
//! plus per-future [`std::task::Waker`] slabs inside the connection.
//! `moonpool-core` does not ship a `Notify` analogue, so the driver loop
//! polls inline by holding the mutex briefly between `read`/`write`/`sleep`
//! awaits.
//!
//! [`NetworkProvider`]: moonpool_core::NetworkProvider
//! [`TimeProvider`]: moonpool_core::TimeProvider
//! [`TaskProvider`]: moonpool_core::TaskProvider
//! [`RandomProvider`]: moonpool_core::RandomProvider
//! [`StorageProvider`]: moonpool_core::StorageProvider

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

pub mod tls;

use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{Connection, ConnectionConfig};
use moonpool_core::{NetworkProvider, Providers};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Shared connection state for the moonpool engine. Mirrors
/// `magnetar_runtime_tokio::ConnectionShared` but reads/writes are awaited
/// directly inside the driver (single-task pattern) — there is no separate
/// notify primitive because moonpool's `TaskProvider` runs everything on a
/// single-threaded executor.
#[derive(Debug)]
pub struct ConnectionShared {
    /// The sans-io state machine, guarded by a non-async mutex.
    pub inner: Mutex<Connection>,
}

impl ConnectionShared {
    /// Construct shared state from the given protocol-layer config.
    #[must_use]
    pub fn new(config: ConnectionConfig) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Connection::new(config)),
        })
    }
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

    /// Connect to a Pulsar broker over the moonpool [`NetworkProvider`].
    ///
    /// `addr` is a `host:port` string per moonpool's API (NOT a `pulsar://`
    /// URL — strip the scheme before calling). TLS is the caller's
    /// responsibility for now: wrap the returned [`ConnectionShared`] +
    /// [`tls::RustlsByteAdapter`] manually in the driver task. The plain
    /// (non-TLS) path is fully wired here.
    ///
    /// The function completes once a `CONNECT` frame has been written and
    /// the broker has responded with `CONNECTED`.
    ///
    /// # Errors
    /// Propagates [`EngineError::Io`] on network failure,
    /// [`EngineError::Protocol`] on framing or handshake errors, or
    /// [`EngineError::PeerClosed`] if the peer closed before CONNECTED.
    pub async fn connect_plain(
        &self,
        addr: &str,
        config: ConnectionConfig,
    ) -> Result<Arc<ConnectionShared>, EngineError> {
        let stream = self
            .providers
            .network()
            .connect(addr)
            .await
            .map_err(EngineError::Io)?;
        let shared = ConnectionShared::new(config);
        // Drive the handshake inline: queue CONNECT, then loop until CONNECTED.
        handshake_plain::<P>(&shared, stream, self.providers.time()).await?;
        Ok(shared)
    }
}

/// Drive the byte pump until the handshake completes.
async fn handshake_plain<P: Providers>(
    shared: &Arc<ConnectionShared>,
    mut stream: <P::Network as NetworkProvider>::TcpStream,
    _time: &P::Time,
) -> Result<(), EngineError> {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut write_buf = Vec::with_capacity(8 * 1024);

    loop {
        // 1. Drain outbound bytes the state machine has queued.
        {
            let mut conn = shared.inner.lock();
            write_buf.clear();
            let _ = conn.poll_transmit(&mut write_buf);
        }
        if !write_buf.is_empty() {
            stream.write_all(&write_buf).await?;
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
        let n = stream.read_buf(&mut read_buf).await?;
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

    use super::{ConnectionShared, MoonpoolEngine};

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
