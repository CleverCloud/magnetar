// SPDX-License-Identifier: Apache-2.0

//! Tokio engine for magnetar.
//!
//! Drives the sans-io [`magnetar_proto::Connection`] state machine over a tokio
//! TCP stream wrapped with `tokio-rustls`. One driver task per connection.
//!
//! ## No channels
//!
//! This crate does not use any flavour of channel (mpsc / broadcast / watch /
//! oneshot). Communication between user-facing futures and the driver task
//! uses `Arc<parking_lot::Mutex<…>>` + [`tokio::sync::Notify`] + in-state
//! `Waker` slabs registered inside `magnetar_proto::Connection`.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

use std::sync::Arc;

use magnetar_proto::ConnectionConfig;
use parking_lot::Mutex;
use tokio::sync::Notify;

/// Placeholder shared state for the tokio engine.
///
/// Real driver wiring (TCP + rustls + select loop + waker dispatch) lands in M3.
/// For now this exists so M2's `Connection` can be exercised under the lock + notify
/// pattern that the engine will adopt.
#[derive(Debug)]
pub struct ConnectionShared {
    /// The sans-io state machine, guarded by a non-async mutex.
    pub inner: Mutex<magnetar_proto::Connection>,
    /// Single-cell wakeup for the driver loop. Not a channel.
    pub driver_waker: Notify,
}

impl ConnectionShared {
    /// Construct shared state from the given protocol-layer config.
    pub fn new(config: ConnectionConfig) -> Arc<Self> {
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
        // Lock-unlock smoke test.
        let _g = s.inner.lock();
    }
}
