// SPDX-License-Identifier: Apache-2.0
//
// Hand-encoded PIP-460 (scalable topics) wire commands. See the module-level
// doc on `pb::scalable_topics` (declared in `lib.rs`) for the rationale —
// briefly: PIP-460 is upstream `Draft`, no broker ships the wire surface, so
// these commands are hand-maintained `#[derive(prost::Message)]` structs
// behind `#[cfg(feature = "scalable-topics")]` rather than vendored into the
// generated `pb/pulsar.proto.rs`. The authoritative bump lands when upstream
// tags a Pulsar 5.0 RC (ADR-0026 §D4). Field numbers are the proposal's
// best-effort guesses; the `encode`/`decode` helpers are the single point of
// change when the vendor bump reconciles them.

use bytes::{Buf, BufMut, BytesMut};
use prost::Message as _;

use crate::frame::{FrameError, MAX_FRAME_SIZE, TOTAL_SIZE_LEN};

/// `BaseCommand.Type` discriminators reserved for PIP-460 (proposal §1.4).
///
/// These extend the generated `pb::base_command::Type` enum. They are kept
/// as a dedicated module rather than patched into the generated enum so the
/// generated file stays vendor-clean.
pub mod base_command_type {
    /// `CommandScalableTopicLookup` (proposal §1.2).
    pub const SCALABLE_TOPIC_LOOKUP: i32 = 80;
    /// `CommandScalableTopicLookupResponse` (proposal §1.2).
    pub const SCALABLE_TOPIC_LOOKUP_RESPONSE: i32 = 81;
    /// `CommandSegmentDagWatch` (proposal §1.3).
    pub const SEGMENT_DAG_WATCH: i32 = 82;
    /// `CommandSegmentDagWatchResponse` (proposal §1.3).
    pub const SEGMENT_DAG_WATCH_RESPONSE: i32 = 83;
    /// `CommandSegmentDagUpdate` (proposal §1.3).
    pub const SEGMENT_DAG_UPDATE: i32 = 84;
    /// `CommandCloseSegmentDagWatch` (proposal §1.3).
    pub const CLOSE_SEGMENT_DAG_WATCH: i32 = 85;
}

/// `ProtocolVersion` level a `scalable-topics` client advertises (proposal
/// §1.5). One past the v4 ceiling (`v21`); only sent when the feature is on.
pub const PROTOCOL_VERSION_SCALABLE_TOPICS: i32 = 22;

/// Segment lifecycle state (proposal §1.2, mirrors the wire `SegmentState`
/// enum). Encoded as the protobuf enum integer in [`SegmentDescriptor`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[repr(i32)]
pub enum SegmentStatePb {
    /// Segment is live and serving reads/writes.
    #[default]
    Active = 0,
    /// Segment is splitting into children.
    Splitting = 1,
    /// Segment is merging into a child.
    Merging = 2,
    /// Segment is sealed (no more writes); reads drain then it is removed.
    Sealed = 3,
}

impl SegmentStatePb {
    /// Decode from the wire integer, falling back to [`Self::Active`] on an
    /// unknown value (forward-compatibility with a future broker enum).
    #[must_use]
    pub fn from_i32(value: i32) -> Self {
        match value {
            1 => Self::Splitting,
            2 => Self::Merging,
            3 => Self::Sealed,
            _ => Self::Active,
        }
    }
}

/// A single segment in the topic DAG (proposal §1.2).
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct SegmentDescriptor {
    /// Segment id, unique within the topic DAG.
    #[prost(uint64, tag = "1")]
    pub segment_id: u64,
    /// Plaintext broker URL serving this segment.
    #[prost(string, tag = "2")]
    pub broker_url: ::prost::alloc::string::String,
    /// TLS broker URL serving this segment, if any.
    #[prost(string, optional, tag = "3")]
    pub broker_url_tls: ::core::option::Option<::prost::alloc::string::String>,
    /// Inclusive start of the segment's hash key range.
    #[prost(uint32, tag = "4")]
    pub key_range_start: u32,
    /// Exclusive end of the segment's hash key range.
    #[prost(uint32, tag = "5")]
    pub key_range_end: u32,
    /// Lifecycle state — see [`SegmentStatePb`].
    #[prost(int32, tag = "6")]
    pub state: i32,
}

/// `CommandScalableTopicLookup` (proposal §1.2).
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CommandScalableTopicLookup {
    /// Topic name (`topic://...`).
    #[prost(string, tag = "1")]
    pub topic: ::prost::alloc::string::String,
    /// Per-connection request id for response correlation.
    #[prost(uint64, tag = "2")]
    pub request_id: u64,
    /// Whether this round-trip is authoritative (redirect bound).
    #[prost(bool, optional, tag = "3")]
    pub authoritative: ::core::option::Option<bool>,
    /// Lookup-style auth carry-through — original principal.
    #[prost(string, optional, tag = "4")]
    pub original_principal: ::core::option::Option<::prost::alloc::string::String>,
    /// Lookup-style auth carry-through — original auth data.
    #[prost(string, optional, tag = "5")]
    pub original_auth_data: ::core::option::Option<::prost::alloc::string::String>,
    /// Lookup-style auth carry-through — original auth method.
    #[prost(string, optional, tag = "6")]
    pub original_auth_method: ::core::option::Option<::prost::alloc::string::String>,
}

/// `CommandScalableTopicLookupResponse` (proposal §1.2).
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CommandScalableTopicLookupResponse {
    /// Request id correlating to the originating lookup.
    #[prost(uint64, tag = "1")]
    pub request_id: u64,
    /// Response type — see [`scalable_lookup_response::LookupType`].
    #[prost(int32, tag = "2")]
    pub response: i32,
    /// Plaintext controller-broker URL to open the DagWatch session against.
    #[prost(string, optional, tag = "3")]
    pub controller_broker_url: ::core::option::Option<::prost::alloc::string::String>,
    /// TLS controller-broker URL.
    #[prost(string, optional, tag = "4")]
    pub controller_broker_url_tls: ::core::option::Option<::prost::alloc::string::String>,
    /// Current DAG snapshot for the topic.
    #[prost(message, repeated, tag = "5")]
    pub segments: ::prost::alloc::vec::Vec<SegmentDescriptor>,
    /// Monotonic lookup token, echoed into the DagWatch subscribe.
    #[prost(uint64, optional, tag = "6")]
    pub lookup_token: ::core::option::Option<u64>,
    /// `ServerError` code on failure.
    #[prost(int32, optional, tag = "7")]
    pub error: ::core::option::Option<i32>,
    /// Broker-supplied error message.
    #[prost(string, optional, tag = "8")]
    pub message: ::core::option::Option<::prost::alloc::string::String>,
}

/// Response-type enum for [`CommandScalableTopicLookupResponse`].
pub mod scalable_lookup_response {
    /// Lookup outcome discriminator (proposal §1.2).
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    #[repr(i32)]
    pub enum LookupType {
        /// Broker redirected the lookup to another controller.
        Redirect = 0,
        /// Lookup resolved; connect to the returned controller + DAG.
        Connect = 1,
        /// Lookup failed.
        Failed = 2,
    }

    impl LookupType {
        /// Decode from the wire integer, defaulting to [`Self::Failed`] on an
        /// unknown / malformed value.
        #[must_use]
        pub fn from_i32(value: i32) -> Self {
            match value {
                0 => Self::Redirect,
                1 => Self::Connect,
                _ => Self::Failed,
            }
        }
    }
}

/// `CommandSegmentDagWatch` (proposal §1.3) — single-frame subscribe.
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CommandSegmentDagWatch {
    /// Topic name.
    #[prost(string, tag = "1")]
    pub topic: ::prost::alloc::string::String,
    /// Per-connection request id.
    #[prost(uint64, tag = "2")]
    pub request_id: u64,
    /// Client-allocated watch session id.
    #[prost(uint64, tag = "3")]
    pub watch_session_id: u64,
    /// Token from the lookup response.
    #[prost(uint64, tag = "4")]
    pub lookup_token: u64,
}

/// `CommandSegmentDagWatchResponse` (proposal §1.3).
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CommandSegmentDagWatchResponse {
    /// Watch session id echoed back.
    #[prost(uint64, tag = "1")]
    pub watch_session_id: u64,
    /// Request id correlating to the subscribe.
    #[prost(uint64, tag = "2")]
    pub request_id: u64,
    /// `ServerError` code on failure.
    #[prost(int32, optional, tag = "3")]
    pub error: ::core::option::Option<i32>,
    /// Broker-supplied error message.
    #[prost(string, optional, tag = "4")]
    pub message: ::core::option::Option<::prost::alloc::string::String>,
}

/// A split event in a [`CommandSegmentDagUpdate`] (proposal §1.3).
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct SplitEvent {
    /// Parent segment id being split.
    #[prost(uint64, tag = "1")]
    pub parent_segment_id: u64,
    /// Child segment ids produced by the split.
    #[prost(uint64, repeated, tag = "2")]
    pub child_segment_ids: ::prost::alloc::vec::Vec<u64>,
    /// Entry id at which the split takes effect.
    #[prost(uint64, tag = "3")]
    pub split_at_entry: u64,
}

/// A merge event in a [`CommandSegmentDagUpdate`] (proposal §1.3).
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct MergeEvent {
    /// Parent segment ids being merged.
    #[prost(uint64, repeated, tag = "1")]
    pub parent_segment_ids: ::prost::alloc::vec::Vec<u64>,
    /// Child segment id produced by the merge.
    #[prost(uint64, tag = "2")]
    pub child_segment_id: u64,
    /// Entry id at which the merge takes effect.
    #[prost(uint64, tag = "3")]
    pub merge_at_entry: u64,
}

/// `CommandSegmentDagUpdate` (proposal §1.3) — broker-pushed DAG delta.
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CommandSegmentDagUpdate {
    /// Watch session id this update belongs to.
    #[prost(uint64, tag = "1")]
    pub watch_session_id: u64,
    /// Monotonic per-session update sequence number.
    #[prost(uint64, tag = "2")]
    pub update_seq: u64,
    /// Segments added to the DAG.
    #[prost(message, repeated, tag = "3")]
    pub added: ::prost::alloc::vec::Vec<SegmentDescriptor>,
    /// Segment ids removed from the DAG.
    #[prost(uint64, repeated, tag = "4")]
    pub removed: ::prost::alloc::vec::Vec<u64>,
    /// Split events.
    #[prost(message, repeated, tag = "5")]
    pub split_events: ::prost::alloc::vec::Vec<SplitEvent>,
    /// Merge events.
    #[prost(message, repeated, tag = "6")]
    pub merge_events: ::prost::alloc::vec::Vec<MergeEvent>,
}

/// `CommandCloseSegmentDagWatch` (proposal §1.3).
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CommandCloseSegmentDagWatch {
    /// Watch session id to close.
    #[prost(uint64, tag = "1")]
    pub watch_session_id: u64,
    /// Per-connection request id.
    #[prost(uint64, tag = "2")]
    pub request_id: u64,
}

/// Hand-built `BaseCommand` envelope carrying a PIP-460 command.
///
/// Field 1 (`type`) shares the wire tag with the generated
/// `pb::BaseCommand`, so a frame produced here decodes as the right command
/// type at any Pulsar-compatible peer. The new command fields (80-85) are
/// additive `optional`s — a v4 decoder skips them.
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct ScalableBaseCommand {
    /// `BaseCommand.Type` discriminator (shared field 1).
    #[prost(int32, tag = "1")]
    pub r#type: i32,
    /// `SCALABLE_TOPIC_LOOKUP` payload.
    #[prost(message, optional, tag = "80")]
    pub scalable_topic_lookup: ::core::option::Option<CommandScalableTopicLookup>,
    /// `SCALABLE_TOPIC_LOOKUP_RESPONSE` payload.
    #[prost(message, optional, tag = "81")]
    pub scalable_topic_lookup_response: ::core::option::Option<CommandScalableTopicLookupResponse>,
    /// `SEGMENT_DAG_WATCH` payload.
    #[prost(message, optional, tag = "82")]
    pub segment_dag_watch: ::core::option::Option<CommandSegmentDagWatch>,
    /// `SEGMENT_DAG_WATCH_RESPONSE` payload.
    #[prost(message, optional, tag = "83")]
    pub segment_dag_watch_response: ::core::option::Option<CommandSegmentDagWatchResponse>,
    /// `SEGMENT_DAG_UPDATE` payload.
    #[prost(message, optional, tag = "84")]
    pub segment_dag_update: ::core::option::Option<CommandSegmentDagUpdate>,
    /// `CLOSE_SEGMENT_DAG_WATCH` payload.
    #[prost(message, optional, tag = "85")]
    pub close_segment_dag_watch: ::core::option::Option<CommandCloseSegmentDagWatch>,
}

impl ScalableBaseCommand {
    /// Build an envelope around a `CommandScalableTopicLookup`.
    #[must_use]
    pub fn lookup(cmd: CommandScalableTopicLookup) -> Self {
        Self {
            r#type: base_command_type::SCALABLE_TOPIC_LOOKUP,
            scalable_topic_lookup: Some(cmd),
            ..Default::default()
        }
    }

    /// Build an envelope around a `CommandScalableTopicLookupResponse`.
    #[must_use]
    pub fn lookup_response(cmd: CommandScalableTopicLookupResponse) -> Self {
        Self {
            r#type: base_command_type::SCALABLE_TOPIC_LOOKUP_RESPONSE,
            scalable_topic_lookup_response: Some(cmd),
            ..Default::default()
        }
    }

    /// Build an envelope around a `CommandSegmentDagWatch`.
    #[must_use]
    pub fn dag_watch(cmd: CommandSegmentDagWatch) -> Self {
        Self {
            r#type: base_command_type::SEGMENT_DAG_WATCH,
            segment_dag_watch: Some(cmd),
            ..Default::default()
        }
    }

    /// Build an envelope around a `CommandSegmentDagWatchResponse`.
    #[must_use]
    pub fn dag_watch_response(cmd: CommandSegmentDagWatchResponse) -> Self {
        Self {
            r#type: base_command_type::SEGMENT_DAG_WATCH_RESPONSE,
            segment_dag_watch_response: Some(cmd),
            ..Default::default()
        }
    }

    /// Build an envelope around a `CommandSegmentDagUpdate`.
    #[must_use]
    pub fn dag_update(cmd: CommandSegmentDagUpdate) -> Self {
        Self {
            r#type: base_command_type::SEGMENT_DAG_UPDATE,
            segment_dag_update: Some(cmd),
            ..Default::default()
        }
    }

    /// Build an envelope around a `CommandCloseSegmentDagWatch`.
    #[must_use]
    pub fn close_dag_watch(cmd: CommandCloseSegmentDagWatch) -> Self {
        Self {
            r#type: base_command_type::CLOSE_SEGMENT_DAG_WATCH,
            close_segment_dag_watch: Some(cmd),
            ..Default::default()
        }
    }
}

/// Encode a [`ScalableBaseCommand`] into the standard command-only frame
/// (`[total_size u32][cmd_size u32][BaseCommand bytes]`). Mirrors
/// [`crate::frame::encode_command`] for the hand-built envelope.
///
/// # Errors
///
/// Returns [`FrameError::Encode`] / [`FrameError::BadLength`] on the same
/// conditions as the generated-command encoder.
pub fn encode(dst: &mut BytesMut, cmd: &ScalableBaseCommand) -> Result<(), FrameError> {
    const CMD_SIZE_LEN: usize = 4;
    let cmd_size = cmd.encoded_len();
    let total_size = CMD_SIZE_LEN
        .checked_add(cmd_size)
        .ok_or(FrameError::BadLength(u32::MAX))?;
    let total_size_u32 = u32::try_from(total_size).map_err(|_| FrameError::BadLength(u32::MAX))?;
    let cmd_size_u32 = u32::try_from(cmd_size).map_err(|_| FrameError::BadLength(u32::MAX))?;

    dst.reserve(TOTAL_SIZE_LEN + total_size);
    dst.put_u32(total_size_u32);
    dst.put_u32(cmd_size_u32);
    cmd.encode(dst)?;
    Ok(())
}

/// Decode exactly one [`ScalableBaseCommand`] frame from the front of `src`,
/// advancing the cursor past the consumed bytes. Returns `Ok(None)` when the
/// buffer holds an incomplete frame (caller should read more bytes).
///
/// This is the broker-fake / test path: the production client never decodes a
/// *frame* here (it receives PIP-460 responses through the connection's
/// dispatch loop), but the in-process broker fake and the differential
/// harness drive the wire end-to-end.
///
/// # Errors
///
/// Returns [`FrameError::BadLength`] when the framed length is implausible,
/// or [`FrameError::Decode`] when the inner protobuf is malformed.
pub fn decode(src: &mut BytesMut) -> Result<Option<ScalableBaseCommand>, FrameError> {
    if src.len() < TOTAL_SIZE_LEN {
        return Ok(None);
    }
    let total_size = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
    if total_size < 4 || total_size > MAX_FRAME_SIZE {
        return Err(FrameError::BadLength(total_size as u32));
    }
    if src.len() < TOTAL_SIZE_LEN + total_size {
        return Ok(None);
    }
    src.advance(TOTAL_SIZE_LEN);
    let cmd_size = src.get_u32() as usize;
    if cmd_size > total_size.saturating_sub(4) {
        return Err(FrameError::BadLength(cmd_size as u32));
    }
    let cmd_bytes = src.split_to(cmd_size);
    let cmd = ScalableBaseCommand::decode(&cmd_bytes[..])?;
    Ok(Some(cmd))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Layer (a) test: a `CommandScalableTopicLookup` request and its
    /// response (carrying the segment list) round-trip through the
    /// hand-built `BaseCommand` envelope.
    #[test]
    fn command_scalable_topic_lookup_roundtrip() {
        let req = CommandScalableTopicLookup {
            topic: "topic://public/default/scaled".to_owned(),
            request_id: 7,
            authoritative: Some(true),
            original_principal: None,
            original_auth_data: None,
            original_auth_method: None,
        };
        let mut buf = BytesMut::new();
        encode(&mut buf, &ScalableBaseCommand::lookup(req.clone())).expect("encode lookup");
        let back = decode(&mut buf)
            .expect("decode ok")
            .expect("one full frame");
        assert_eq!(back.r#type, base_command_type::SCALABLE_TOPIC_LOOKUP);
        assert_eq!(back.scalable_topic_lookup, Some(req));
        assert!(buf.is_empty(), "frame fully consumed");

        let resp = CommandScalableTopicLookupResponse {
            request_id: 7,
            response: scalable_lookup_response::LookupType::Connect as i32,
            controller_broker_url: Some("pulsar://controller:6650".to_owned()),
            controller_broker_url_tls: None,
            segments: vec![
                SegmentDescriptor {
                    segment_id: 1,
                    broker_url: "pulsar://seg1:6650".to_owned(),
                    broker_url_tls: None,
                    key_range_start: 0,
                    key_range_end: 32_768,
                    state: SegmentStatePb::Active as i32,
                },
                SegmentDescriptor {
                    segment_id: 2,
                    broker_url: "pulsar://seg2:6650".to_owned(),
                    broker_url_tls: None,
                    key_range_start: 32_768,
                    key_range_end: 65_536,
                    state: SegmentStatePb::Active as i32,
                },
            ],
            lookup_token: Some(42),
            error: None,
            message: None,
        };
        let mut buf = BytesMut::new();
        encode(&mut buf, &ScalableBaseCommand::lookup_response(resp.clone()))
            .expect("encode response");
        let back = decode(&mut buf)
            .expect("decode ok")
            .expect("one full frame");
        assert_eq!(
            back.r#type,
            base_command_type::SCALABLE_TOPIC_LOOKUP_RESPONSE
        );
        let got = back
            .scalable_topic_lookup_response
            .expect("response payload present");
        assert_eq!(got, resp);
        assert_eq!(got.segments.len(), 2);
        assert_eq!(got.lookup_token, Some(42));
    }

    /// Layer (a) test: `CommandSegmentDagWatch` + its response frame-level
    /// encode/decode.
    #[test]
    fn command_segment_dag_watch_roundtrip() {
        let watch = CommandSegmentDagWatch {
            topic: "topic://public/default/scaled".to_owned(),
            request_id: 11,
            watch_session_id: 99,
            lookup_token: 42,
        };
        let mut buf = BytesMut::new();
        encode(&mut buf, &ScalableBaseCommand::dag_watch(watch.clone())).expect("encode watch");
        let back = decode(&mut buf).expect("ok").expect("frame");
        assert_eq!(back.r#type, base_command_type::SEGMENT_DAG_WATCH);
        assert_eq!(back.segment_dag_watch, Some(watch));

        let resp = CommandSegmentDagWatchResponse {
            watch_session_id: 99,
            request_id: 11,
            error: None,
            message: None,
        };
        let mut buf = BytesMut::new();
        encode(&mut buf, &ScalableBaseCommand::dag_watch_response(resp.clone()))
            .expect("encode watch response");
        let back = decode(&mut buf).expect("ok").expect("frame");
        assert_eq!(back.r#type, base_command_type::SEGMENT_DAG_WATCH_RESPONSE);
        assert_eq!(back.segment_dag_watch_response, Some(resp));
    }

    /// Layer (a) test: a `CommandSegmentDagUpdate` carrying add / remove /
    /// split / merge variants encodes and decodes cleanly, then a
    /// `CommandCloseSegmentDagWatch` round-trips.
    #[test]
    fn command_segment_dag_update_roundtrip() {
        let upd = CommandSegmentDagUpdate {
            watch_session_id: 99,
            update_seq: 1,
            added: vec![SegmentDescriptor {
                segment_id: 3,
                broker_url: "pulsar://seg3:6650".to_owned(),
                broker_url_tls: Some("pulsar+ssl://seg3:6651".to_owned()),
                key_range_start: 0,
                key_range_end: 16_384,
                state: SegmentStatePb::Active as i32,
            }],
            removed: vec![1],
            split_events: vec![SplitEvent {
                parent_segment_id: 1,
                child_segment_ids: vec![3, 4],
                split_at_entry: 1000,
            }],
            merge_events: vec![MergeEvent {
                parent_segment_ids: vec![5, 6],
                child_segment_id: 7,
                merge_at_entry: 2000,
            }],
        };
        let mut buf = BytesMut::new();
        encode(&mut buf, &ScalableBaseCommand::dag_update(upd.clone())).expect("encode update");
        let back = decode(&mut buf).expect("ok").expect("frame");
        assert_eq!(back.r#type, base_command_type::SEGMENT_DAG_UPDATE);
        let got = back.segment_dag_update.expect("update present");
        assert_eq!(got, upd);
        assert_eq!(got.added.len(), 1);
        assert_eq!(got.removed, vec![1]);
        assert_eq!(got.split_events[0].child_segment_ids, vec![3, 4]);
        assert_eq!(got.merge_events[0].parent_segment_ids, vec![5, 6]);

        let close = CommandCloseSegmentDagWatch {
            watch_session_id: 99,
            request_id: 12,
        };
        let mut buf = BytesMut::new();
        encode(&mut buf, &ScalableBaseCommand::close_dag_watch(close.clone()))
            .expect("encode close");
        let back = decode(&mut buf).expect("ok").expect("frame");
        assert_eq!(back.r#type, base_command_type::CLOSE_SEGMENT_DAG_WATCH);
        assert_eq!(back.close_segment_dag_watch, Some(close));
    }

    /// `decode` returns `Ok(None)` on a partial frame so the caller knows to
    /// read more bytes rather than erroring.
    #[test]
    fn decode_partial_frame_returns_none() {
        let req = CommandScalableTopicLookup {
            topic: "topic://x".to_owned(),
            request_id: 1,
            authoritative: None,
            original_principal: None,
            original_auth_data: None,
            original_auth_method: None,
        };
        let mut full = BytesMut::new();
        encode(&mut full, &ScalableBaseCommand::lookup(req)).expect("encode");
        // Feed only the first half — decode must report "need more".
        let mut partial = full.split_to(full.len() / 2);
        assert!(decode(&mut partial).expect("ok").is_none());
    }

    /// `SegmentStatePb::from_i32` saturates unknown enum integers to
    /// `Active` (forward-compatible with a future broker enum).
    #[test]
    fn segment_state_unknown_falls_back_to_active() {
        assert_eq!(SegmentStatePb::from_i32(0), SegmentStatePb::Active);
        assert_eq!(SegmentStatePb::from_i32(1), SegmentStatePb::Splitting);
        assert_eq!(SegmentStatePb::from_i32(2), SegmentStatePb::Merging);
        assert_eq!(SegmentStatePb::from_i32(3), SegmentStatePb::Sealed);
        assert_eq!(SegmentStatePb::from_i32(99), SegmentStatePb::Active);
    }
}
