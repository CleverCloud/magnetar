// SPDX-License-Identifier: Apache-2.0

//! Sans-io Apache Pulsar wire protocol.
//!
//! `magnetar-proto` is the protocol heart of the magnetar workspace. It contains:
//! - Pulsar binary-protocol framing (`0x0e01` payload frames, `0x0e02` broker-entry metadata).
//! - CRC32C (Castagnoli) checksums.
//! - The full `BaseCommand` codec, generated from the vendored `PulsarApi.proto`.
//! - State machines for `Connection`, `Producer`, `Consumer`, lookup, trackers, batching, chunking
//!   (state-machine work lands incrementally — see the milestone plan).
//!
//! It has **zero I/O dependencies** and **zero channels**. The public API uses the `quinn-proto`
//! shape — see [`Connection`].
//!
//! # Modules
//!
//! - [`frame`]: encoding and decoding of magnetar wire frames (command + optional payload).
//! - [`pb`]: protobuf-generated Pulsar wire types. Regenerate via `cargo run -p xtask -- codegen`.
//!
//! # Architecture
//!
//! See [`ARCHITECTURE.md`](https://github.com/FlorentinDUBOIS/magnetar/blob/main/ARCHITECTURE.md)
//! for the layered diagram and the no-channels rationale.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

pub mod frame;

/// Protobuf-generated Pulsar wire types.
///
/// Regenerate via `cargo run -p xtask -- codegen`. The contents of this module are committed to
/// the repository so consumers do not need `protoc` available at build time.
#[allow(
    clippy::all,
    clippy::pedantic,
    unreachable_pub,
    missing_debug_implementations
)]
pub mod pb {
    include!("pb/pulsar.proto.rs");
}

pub use crate::frame::{
    Frame, FrameError, MAGIC_BROKER_ENTRY_METADATA, MAGIC_CRC32C, MAX_FRAME_SIZE, Payload,
    decode_one, encode_command, encode_payload,
};

/// Placeholder for the connection state machine.
///
/// The real sans-io [`Connection`] lands in M2. For now it exists so other workspace crates
/// (notably the runtime engines) can keep referencing the public type. M2 will replace this with
/// the full state machine (handshake, lookup, producer/consumer dispatchers, waker slabs, etc.).
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
