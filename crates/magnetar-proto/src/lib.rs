// SPDX-License-Identifier: Apache-2.0

//! Sans-io Apache Pulsar wire protocol.
//!
//! `magnetar-proto` is the protocol heart of the magnetar workspace. It contains:
//! - Pulsar binary-protocol framing (`0x0e01` payload frames, `0x0e02` broker-entry metadata).
//! - CRC32C (Castagnoli) checksums.
//! - The full `BaseCommand` codec, generated from the vendored `PulsarApi.proto`.
//! - State machines for `Connection`, `Producer`, `Consumer`, lookup, trackers, batching, chunking.
//!
//! It has **zero I/O dependencies** and **zero channels**. The public API uses the `quinn-proto`
//! shape — see [`Connection`].
//!
//! # Architecture
//!
//! See [`ARCHITECTURE.md`](https://github.com/FlorentinDUBOIS/magnetar/blob/main/ARCHITECTURE.md)
//! for the layered diagram and the no-channels rationale.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

/// Placeholder for the connection state machine.
///
/// Real implementation lands in M2 per the magnetar plan. For M0 this is a marker
/// so the workspace compiles cleanly.
#[derive(Debug, Default)]
pub struct Connection {
    _private: (),
}

impl Connection {
    /// Construct a fresh, unconnected sans-io `Connection`.
    ///
    /// This will become the entry point that the runtime engines (`magnetar-runtime-tokio`,
    /// `magnetar-runtime-moonpool`) drive byte-for-byte. For now it is intentionally inert.
    pub fn new() -> Self {
        Self { _private: () }
    }
}

#[cfg(test)]
mod tests {
    use super::Connection;

    #[test]
    fn connection_can_be_constructed() {
        let _ = Connection::new();
    }
}
