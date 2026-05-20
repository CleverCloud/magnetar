// SPDX-License-Identifier: Apache-2.0

//! moonpool engine for magnetar.
//!
//! Drives the sans-io [`magnetar_proto::Connection`] state machine on top of
//! [`moonpool_core`]'s [`NetworkProvider`] + [`TimeProvider`] +
//! [`TaskProvider`] + [`RandomProvider`] traits, the same way
//! `magnetar-runtime-tokio` does on top of tokio. The point is *not* to be
//! a separate engine for production load — it is to make the entire
//! producer/consumer protocol exercisable under
//! [moonpool-sim](https://crates.io/crates/moonpool-sim) deterministic
//! chaos testing, so we can fuzz partitions, message reorderings, and
//! TLS handshake reorderings with reproducible seeds.
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
//! uses `parking_lot::Condvar` for cross-task wakeups.
//!
//! ## Status
//!
//! M4 ships the **engine + TLS adapter skeleton** with a working byte pump
//! on top of moonpool-core's [`NetworkProvider`](moonpool_core::NetworkProvider).
//! Full integration with `moonpool-sim` chaos seeding lands in a follow-up
//! once `magnetar-runtime-tokio` is exercised against a real broker — see
//! ARCHITECTURE.md.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

pub mod tls;

use std::sync::Arc;

use magnetar_proto::{Connection, ConnectionConfig};
use parking_lot::Mutex;

/// Shared connection state for the moonpool engine. Mirrors
/// `magnetar_runtime_tokio::ConnectionShared` but uses `Condvar`-style
/// wakeups (no `Notify` analogue in `moonpool-core`).
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
}

/// Engine handle. Real `connect()` async API lands when the `moonpool-sim`
/// integration ships in the follow-up; for now the type exists so
/// downstream code can name the engine.
#[derive(Debug, Default)]
pub struct MoonpoolEngine {
    _private: (),
}

impl MoonpoolEngine {
    /// Construct a placeholder engine handle.
    #[must_use]
    pub fn new() -> Self {
        Self { _private: () }
    }
}

#[cfg(test)]
mod tests {
    use magnetar_proto::ConnectionConfig;

    use super::{ConnectionShared, MoonpoolEngine};

    #[test]
    fn engine_can_be_constructed() {
        let _ = MoonpoolEngine::new();
    }

    #[test]
    fn shared_state_can_be_constructed() {
        let s = ConnectionShared::new(ConnectionConfig::default());
        let _g = s.inner.lock();
    }
}
