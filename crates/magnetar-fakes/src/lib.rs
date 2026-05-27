// SPDX-License-Identifier: Apache-2.0

//! In-process Pulsar broker fake — frame-in / frame-out, with per-command
//! hooks for fault injection.
//!
//! Mirrors the Java `MockBrokerService` design (`apache/pulsar`
//! `pulsar-broker/src/test/java/.../MockBrokerService.java`): a sans-io broker
//! that takes client frames in and emits responses out. Use it from
//! `magnetar-proto/tests/` and from runtime integration tests to validate
//! client behavior against scripted broker scenarios.
//!
//! # Current surface (v0)
//!
//! - [`BrokerFake`] — empty placeholder kept for backwards compatibility.
//! - [`FrameRecorder`] — drains a [`magnetar_proto::Connection`]'s outbound byte stream and decodes
//!   each frame into a [`RecordedFrame`] for wire-shape assertions. Used by the V5 mapping tests
//!   (`crates/magnetar/tests/v5_*_mapping.rs`) to confirm that V5 surface calls translate to the
//!   expected v4 wire commands.
//!
//! The recorder is intentionally one-way (drain, decode, assert). A
//! later cut of the fake adds the reverse direction — synthetic broker
//! frames fed back via `handle_bytes`, with per-command response hooks —
//! once the V5 surface grows tests that need `ProducerSuccess` /
//! `SendReceipt` etc. plumbed back into the client.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

use bytes::Bytes;
use magnetar_proto::frame::peek_full_frame_len;
use magnetar_proto::{Connection, Frame, TransmitOwned, decode_one};

/// Placeholder broker fake — preserved for backwards compatibility with
/// callers that depend on the `BrokerFake::new()` shape. New tests
/// should use [`FrameRecorder`] for outbound-byte assertions.
#[derive(Debug, Default)]
pub struct BrokerFake {
    _private: (),
}

impl BrokerFake {
    /// Construct an idle broker fake.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// One frame decoded from a client's outbound byte stream. Combines the
/// [`Frame`] (`BaseCommand` + optional payload) with the wire-level
/// total length of the frame as seen on the wire — the latter is what
/// lets callers reconstruct the on-wire `total_size` field for round-trip
/// assertions.
#[derive(Debug, Clone)]
pub struct RecordedFrame {
    /// The decoded frame.
    pub frame: Frame,
    /// Total length of the on-wire frame in bytes, including the leading
    /// `total_size u32`. Equivalent to what
    /// [`peek_full_frame_len`] returned for this frame.
    pub wire_len: usize,
}

/// Drain a [`Connection`]'s outbound byte stream and decode every
/// complete frame into a [`RecordedFrame`]. Calls
/// [`Connection::poll_transmit_owned`] in a loop, coalescing
/// `TransmitOwned::Vectored` segments locally so the decoder sees a
/// single contiguous byte stream.
///
/// Intended for tests that need to assert what the client put on the
/// wire — e.g. "the V5 `ProducerBuilder` with this config emits a
/// `CommandProducer` whose `producer_name` field is X".
#[derive(Debug, Default)]
pub struct FrameRecorder {
    /// Bytes pulled from the connection but not yet decoded — keeps
    /// partial-frame trailing bytes between [`Self::drain`] calls so
    /// the recorder works even if the test does interleaved drain +
    /// connection-feed work.
    leftover: bytes::BytesMut,
}

/// Recorder error surface.
#[derive(Debug, thiserror::Error)]
pub enum RecorderError {
    /// A frame failed to decode (framing error, bad length, CRC,
    /// malformed protobuf). Wraps the underlying error.
    #[error("frame decode failed: {0}")]
    FrameDecode(#[from] magnetar_proto::FrameError),
}

impl FrameRecorder {
    /// Construct an empty recorder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Pull every outbound byte the connection has queued, decode each
    /// complete frame, and return the list. Trailing partial-frame
    /// bytes are stashed for the next [`Self::drain`] call.
    ///
    /// # Errors
    ///
    /// [`RecorderError::FrameDecode`] on framing / CRC / protobuf
    /// failures.
    pub fn drain(&mut self, conn: &mut Connection) -> Result<Vec<RecordedFrame>, RecorderError> {
        match conn.poll_transmit_owned() {
            TransmitOwned::Contiguous(buf) => {
                self.leftover.extend_from_slice(&buf);
            }
            TransmitOwned::Vectored(segs) => {
                for seg in segs {
                    self.leftover.extend_from_slice(&seg);
                }
            }
        }
        let mut frames = Vec::new();
        loop {
            let frame_len = match peek_full_frame_len(&self.leftover) {
                Ok(None) => return Ok(frames),
                Ok(Some(len)) => len,
                Err(err) => return Err(err.into()),
            };
            let mut frame_bytes: Bytes = self.leftover.split_to(frame_len).freeze();
            let frame = decode_one(&mut frame_bytes)?;
            frames.push(RecordedFrame {
                frame,
                wire_len: frame_len,
            });
        }
    }

    /// `true` if no leftover bytes are buffered. Used by tests that
    /// want to assert the connection produced exactly the frames they
    /// expected, with no stray trailing bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.leftover.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use magnetar_proto::{ConnectionConfig, encode_command, pb};

    use super::*;

    fn fresh_conn() -> Connection {
        Connection::new(
            ConnectionConfig::default(),
            std::sync::Arc::new(std::time::SystemTime::now),
        )
    }

    fn handshake_response_bytes() -> bytes::BytesMut {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-test".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        encode_command(&mut buf, &cmd).expect("encode CommandConnected");
        buf
    }

    #[test]
    fn fake_can_be_constructed() {
        let _ = BrokerFake::new();
    }

    #[test]
    fn recorder_drains_post_begin_handshake_connect_frame() {
        let mut conn = fresh_conn();
        conn.begin_handshake().expect("handshake");
        let mut rec = FrameRecorder::new();
        let frames = rec.drain(&mut conn).expect("drain ok");
        assert_eq!(
            frames.len(),
            1,
            "begin_handshake emits exactly one Connect frame"
        );
        let recorded = &frames[0];
        assert_eq!(
            recorded.frame.command.r#type,
            pb::base_command::Type::Connect as i32,
            "first frame is CommandConnect"
        );
        assert!(recorded.wire_len > 0);
        assert!(
            rec.is_empty(),
            "no leftover trailing bytes after a clean drain"
        );
    }

    #[test]
    fn recorder_returns_empty_for_quiet_connection() {
        let mut conn = fresh_conn();
        // Pre-handshake: the connection hasn't queued any bytes yet.
        let mut rec = FrameRecorder::new();
        let frames = rec.drain(&mut conn).expect("drain ok");
        assert!(frames.is_empty(), "no frames before begin_handshake");
        assert!(rec.is_empty());
    }

    #[test]
    fn recorder_handles_multiple_frames_in_one_drain() {
        // Drive handshake to Connected, then queue two lookups so the
        // outbound carries both in one drain.
        let mut conn = fresh_conn();
        conn.begin_handshake().expect("handshake");
        let resp = handshake_response_bytes();
        conn.handle_bytes(std::time::Instant::now(), &resp)
            .expect("connected");
        let _ = conn.poll_event();
        // First drain takes the Connect frame off the wire.
        let mut rec = FrameRecorder::new();
        let first = rec.drain(&mut conn).expect("drain 1 ok");
        assert_eq!(first.len(), 1, "first drain: only CommandConnect");
        // Queue two lookups; both should appear in the next drain.
        conn.lookup("persistent://public/default/r1", false);
        conn.lookup("persistent://public/default/r2", false);
        let second = rec.drain(&mut conn).expect("drain 2 ok");
        assert_eq!(second.len(), 2, "second drain: both lookups");
        assert_eq!(
            second[0].frame.command.r#type,
            pb::base_command::Type::Lookup as i32
        );
        assert_eq!(
            second[1].frame.command.r#type,
            pb::base_command::Type::Lookup as i32
        );
    }
}
