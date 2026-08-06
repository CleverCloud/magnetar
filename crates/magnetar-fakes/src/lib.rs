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
//! # Current surface
//!
//! - [`BrokerFake`] — empty placeholder kept for backwards compatibility.
//! - [`FrameRecorder`] — drains a [`magnetar_proto::Connection`]'s outbound byte stream and decodes
//!   each frame into a [`RecordedFrame`] for wire-shape assertions. Used by the V5 mapping tests
//!   (`crates/magnetar/tests/v5_*_mapping.rs`) to confirm that V5 surface calls translate to the
//!   expected v4 wire commands.
//! - `m1::M1FakeCluster` — a stateful, multi-endpoint Pulsar 5.0.0-M1 scalable-topic cluster. It
//!   validates controller and segment routing, assignment ownership, flow permits,
//!   acknowledgements, reconnects, and resource cleanup while exchanging only generated vendored
//!   protocol frames.
//!
//! The recorder remains intentionally one-way (drain, decode, assert).
//! The M1 cluster provides the reverse frame direction for scalable consumers;
//! unrelated producer responses such as `ProducerSuccess` / `SendReceipt` stay
//! outside this focused fake.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

use bytes::Bytes;
use magnetar_proto::frame::peek_full_frame_len;
use magnetar_proto::{Connection, Frame, TransmitOwned, decode_one};

#[cfg(feature = "scalable-topics")]
pub mod m1;

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

// ---------------------------------------------------------------------------
// PIP-460 scalable topics (ADR-0093, experimental). Scripted controller-broker
// fake — replies to `CommandScalableTopicLookup` with the initial layout, then
// emits a scripted sequence of `CommandScalableTopicUpdate` frames (one split
// layout + one merge layout) on the same session.
// ---------------------------------------------------------------------------

/// **Experimental** (PIP-460, ADR-0093). Scripted controller-broker fake for
/// the scalable-topic surface. Drives the client end-to-end through the real
/// upstream wire commands — ordinary `BaseCommand` frames, same framing as
/// every other command: feed the client's outbound bytes via
/// [`Self::on_client_bytes`], collect the broker's reply bytes, and pull the
/// scripted layouts via [`Self::split_update`] / [`Self::merge_update`].
/// Retained for transcript compatibility; new multi-endpoint consumer fixtures
/// should use [`m1::M1FakeCluster`].
///
/// Upstream pushes **whole layouts** stamped with a monotonic epoch rather than
/// split / merge deltas, so each scripted update here is a complete
/// `ScalableTopicDag` whose segments carry the `parent_ids` edges the client
/// reads the topology change off.
#[cfg(feature = "scalable-topics")]
#[derive(Debug, Clone)]
pub struct ScriptedScalableBroker {
    controller_broker_url: String,
    initial_dag: Vec<magnetar_proto::pb::SegmentInfoProto>,
    /// Session id observed from the client's lookup (filled on lookup).
    session_id: Option<u64>,
    /// Next layout epoch the broker will stamp.
    next_epoch: u64,
}

#[cfg(feature = "scalable-topics")]
impl ScriptedScalableBroker {
    /// Construct a broker with a two-segment initial layout (`[0,32767]` /
    /// `[32768,65535]`) and a fixed controller URL.
    #[must_use]
    pub fn two_segment() -> Self {
        Self {
            controller_broker_url: "pulsar://controller:6650".to_owned(),
            initial_dag: vec![
                Self::segment(1, 0, 32_767, &[]),
                Self::segment(2, 32_768, 65_535, &[]),
            ],
            session_id: None,
            next_epoch: 1,
        }
    }

    /// Build a `SegmentInfoProto` with the given hash range and parent edges.
    fn segment(
        id: u64,
        start: u32,
        end: u32,
        parents: &[u64],
    ) -> magnetar_proto::pb::SegmentInfoProto {
        magnetar_proto::pb::SegmentInfoProto {
            segment_id: id,
            hash_start: start,
            hash_end: end,
            state: magnetar_proto::pb::SegmentState::Active as i32,
            parent_ids: parents.to_vec(),
            child_ids: Vec::new(),
            created_at_epoch: 0,
            sealed_at_epoch: None,
            created_at_ms: 0,
            sealed_at_ms: None,
            legacy_topic_name: None,
        }
    }

    /// The controller-broker URL this fake advertises in its layouts.
    #[must_use]
    pub fn controller_broker_url(&self) -> &str {
        &self.controller_broker_url
    }

    /// The initial DAG snapshot.
    #[must_use]
    pub fn initial_dag(&self) -> &[magnetar_proto::pb::SegmentInfoProto] {
        &self.initial_dag
    }

    /// The session id the client allocated (after a lookup was seen).
    #[must_use]
    pub fn session_id(&self) -> Option<u64> {
        self.session_id
    }

    /// Encode a `CommandScalableTopicUpdate` carrying `segments` as the whole
    /// layout at the next epoch.
    fn layout_update(
        &mut self,
        segments: Vec<magnetar_proto::pb::SegmentInfoProto>,
    ) -> Option<bytes::BytesMut> {
        let session_id = self.session_id?;
        let epoch = self.next_epoch;
        self.next_epoch += 1;
        let segment_brokers = segments
            .iter()
            .map(|s| magnetar_proto::pb::SegmentBrokerAddress {
                segment_id: s.segment_id,
                broker_url: format!("pulsar://seg{}:6650", s.segment_id),
                broker_url_tls: None,
            })
            .collect();
        let cmd = magnetar_proto::pb::BaseCommand {
            r#type: magnetar_proto::pb::base_command::Type::ScalableTopicUpdate as i32,
            scalable_topic_update: Some(magnetar_proto::pb::CommandScalableTopicUpdate {
                session_id,
                dag: Some(magnetar_proto::pb::ScalableTopicDag {
                    epoch,
                    segments,
                    segment_brokers,
                    controller_broker_url: Some(self.controller_broker_url.clone()),
                    controller_broker_url_tls: None,
                }),
                error: None,
                message: None,
                resolved_topic_name: Some("topic://public/default/scaled".to_owned()),
            }),
            ..Default::default()
        };
        let mut out = bytes::BytesMut::new();
        magnetar_proto::frame::encode_command(&mut out, &cmd).ok()?;
        Some(out)
    }

    /// Feed one frame of the client's outbound bytes. Returns the broker's
    /// reply bytes — the initial layout for a lookup, empty for anything else.
    /// Records the session id on a lookup.
    #[must_use]
    pub fn on_client_bytes(&mut self, frame_bytes: &mut bytes::Bytes) -> bytes::BytesMut {
        let out = bytes::BytesMut::new();
        let Ok(frame) = magnetar_proto::frame::decode_one(frame_bytes) else {
            return out;
        };
        let Some(lookup) = frame.command.scalable_topic_lookup else {
            // Close frames need no reply, and neither does anything else.
            return out;
        };
        self.session_id = Some(lookup.session_id);
        let segments = self.initial_dag.clone();
        self.layout_update(segments).unwrap_or_default()
    }

    /// Produce the scripted **split** layout for the current session: the
    /// initial segment `1` splits into children `3` + `4`, which name it in
    /// their `parent_ids`. Segment `2` is carried through untouched. Returns
    /// the encoded frame, or `None` if no session is open.
    #[must_use]
    pub fn split_update(&mut self) -> Option<bytes::BytesMut> {
        let epoch = self.next_epoch;
        let mut parent = Self::segment(1, 0, 32_767, &[]);
        parent.state = magnetar_proto::pb::SegmentState::Sealed as i32;
        parent.child_ids = vec![3, 4];
        parent.sealed_at_epoch = Some(epoch);
        let mut child_three = Self::segment(3, 0, 16_383, &[1]);
        child_three.created_at_epoch = epoch;
        let mut child_four = Self::segment(4, 16_384, 32_767, &[1]);
        child_four.created_at_epoch = epoch;
        self.layout_update(vec![
            parent,
            Self::segment(2, 32_768, 65_535, &[]),
            child_three,
            child_four,
        ])
    }

    /// Produce the scripted **merge** layout: segments `3` + `4` fold into a
    /// single child `5`, which names both in its `parent_ids`. Returns the
    /// encoded frame, or `None` if no session is open.
    #[must_use]
    pub fn merge_update(&mut self) -> Option<bytes::BytesMut> {
        let epoch = self.next_epoch;
        let split_epoch = epoch.saturating_sub(1);
        let mut root = Self::segment(1, 0, 32_767, &[]);
        root.state = magnetar_proto::pb::SegmentState::Sealed as i32;
        root.child_ids = vec![3, 4];
        root.sealed_at_epoch = Some(split_epoch);
        let mut child_three = Self::segment(3, 0, 16_383, &[1]);
        child_three.created_at_epoch = split_epoch;
        child_three.state = magnetar_proto::pb::SegmentState::Sealed as i32;
        child_three.child_ids = vec![5];
        child_three.sealed_at_epoch = Some(epoch);
        let mut child_four = Self::segment(4, 16_384, 32_767, &[1]);
        child_four.created_at_epoch = split_epoch;
        child_four.state = magnetar_proto::pb::SegmentState::Sealed as i32;
        child_four.child_ids = vec![5];
        child_four.sealed_at_epoch = Some(epoch);
        let mut merged = Self::segment(5, 0, 32_767, &[3, 4]);
        merged.created_at_epoch = epoch;
        self.layout_update(vec![
            root,
            Self::segment(2, 32_768, 65_535, &[]),
            child_three,
            child_four,
            merged,
        ])
    }
}
