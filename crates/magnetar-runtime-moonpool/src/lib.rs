// SPDX-License-Identifier: Apache-2.0

//! moonpool engine for magnetar.
//!
//! Drives the sans-io [`magnetar_proto::Connection`] state machine on top of
//! [`moonpool_core`]'s [`NetworkProvider`] + [`TimeProvider`] +
//! [`TaskProvider`] + [`RandomProvider`] traits. Skips `moonpool-transport`
//! because its CRC32C-length-prefixed wire format conflicts with Pulsar
//! framing.
//!
//! ## TLS
//!
//! TLS is provided by a custom adapter at `tls.rs` that drives
//! [`rustls::ClientConnection`] via its sans-io methods (`read_tls`,
//! `process_new_packets`, `write_tls`) over the moonpool-supplied byte pipe.
//! Handshakes are deterministic under `moonpool-sim` chaos.
//!
//! [`NetworkProvider`]: moonpool_core::NetworkProvider
//! [`TimeProvider`]: moonpool_core::TimeProvider
//! [`TaskProvider`]: moonpool_core::TaskProvider
//! [`RandomProvider`]: moonpool_core::RandomProvider

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

/// Marker type for M0. Real engine in M4.
#[derive(Debug, Default)]
pub struct MoonpoolEngine {
    _private: (),
}

impl MoonpoolEngine {
    /// Construct a placeholder engine.
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::MoonpoolEngine;

    #[test]
    fn engine_can_be_constructed() {
        let _ = MoonpoolEngine::new();
    }
}
