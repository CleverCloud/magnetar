// SPDX-License-Identifier: Apache-2.0

//! Shared sans-io types.
//!
//! Public identifier and handle types used throughout the state-machine layer. These types are
//! intentionally `Copy + Eq + Hash` so they can be threaded through slabs and hash maps without
//! cloning.
//!
//! # References
//!
//! - `ClientCnx.java:117` (id allocation), `ProducerImpl.java:419` (producer id),
//!   `ConsumerImpl.java:143` (consumer id).
//! - `MessageIdImpl.java` (logical message id structure).

use core::fmt;

use crate::pb;

/// A protocol-level request id, monotonically increasing per connection.
///
/// Mirrors `request_id` in `CommandSubscribe`, `CommandProducer`, `CommandSeek`, etc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestId(pub u64);

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A producer id, allocated by the [`Connection`](crate::Connection) when a producer opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProducerHandle(pub u64);

impl fmt::Display for ProducerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A consumer id, allocated by the [`Connection`](crate::Connection) when a subscription opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConsumerHandle(pub u64);

impl fmt::Display for ConsumerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A monotonic per-producer publish sequence id.
///
/// Mirrors `sequenceId` in `MessageMetadata` / `CommandSend` / `CommandSendReceipt`. Reused on
/// resend (per `ProducerImpl.java:745-753`) so dedup at the broker remains correct.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SequenceId(pub u64);

impl fmt::Display for SequenceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// PIP-460 segment identifier — unique within a scalable topic's segment DAG.
///
/// **Experimental** (PIP-460, ADR-0031). Only meaningful under
/// `feature = "scalable-topics"`; carried on [`MessageId::segment_id`] for
/// messages read from a scalable topic.
#[cfg(feature = "scalable-topics")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SegmentId(pub u64);

#[cfg(feature = "scalable-topics")]
impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// PIP-460 hash key range `[start, end)` a segment is responsible for.
///
/// **Experimental** (PIP-460, ADR-0031). v0.2.0 surfaces the key range for
/// observation only — segment-aware sticky-key dispatch (Key_Shared across
/// the full DAG) is out of scope (v0.3.0+).
#[cfg(feature = "scalable-topics")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyRange {
    /// Inclusive start of the hash range.
    pub start: u32,
    /// Exclusive end of the hash range.
    pub end: u32,
}

/// PIP-460 segment lifecycle state.
///
/// **Experimental** (PIP-460, ADR-0031). `#[non_exhaustive]` so a future
/// broker enum value cannot break a `match` on this type downstream.
#[cfg(feature = "scalable-topics")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum SegmentState {
    /// Segment is live and serving reads/writes.
    #[default]
    Active,
    /// Segment is splitting into children.
    Splitting,
    /// Segment is merging into a child.
    Merging,
    /// Segment is sealed (no more writes); reads drain then it is removed.
    Sealed,
}

#[cfg(feature = "scalable-topics")]
impl SegmentState {
    /// Convert from the wire enum integer, saturating unknown values to
    /// [`Self::Active`] (forward-compatibility with a future broker enum).
    #[must_use]
    pub fn from_pb_i32(value: i32) -> Self {
        match crate::pb::scalable_topics::SegmentStatePb::from_i32(value) {
            crate::pb::scalable_topics::SegmentStatePb::Active => Self::Active,
            crate::pb::scalable_topics::SegmentStatePb::Splitting => Self::Splitting,
            crate::pb::scalable_topics::SegmentStatePb::Merging => Self::Merging,
            crate::pb::scalable_topics::SegmentStatePb::Sealed => Self::Sealed,
        }
    }

    /// Convert to the wire enum integer.
    #[must_use]
    pub fn to_pb_i32(self) -> i32 {
        match self {
            Self::Active => crate::pb::scalable_topics::SegmentStatePb::Active as i32,
            Self::Splitting => crate::pb::scalable_topics::SegmentStatePb::Splitting as i32,
            Self::Merging => crate::pb::scalable_topics::SegmentStatePb::Merging as i32,
            Self::Sealed => crate::pb::scalable_topics::SegmentStatePb::Sealed as i32,
        }
    }
}

/// PIP-460 segment descriptor — one node of a scalable topic's segment DAG.
///
/// **Experimental** (PIP-460, ADR-0031). Returned by the scalable-topic
/// lookup and carried in DAG-watch updates. `broker_url` is the segment
/// leader's plaintext URL.
#[cfg(feature = "scalable-topics")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentDescriptor {
    /// Segment id, unique within the topic DAG.
    pub segment_id: SegmentId,
    /// Hash key range this segment serves.
    pub key_range: KeyRange,
    /// Plaintext broker URL serving this segment.
    pub broker_url: String,
    /// Lifecycle state.
    pub state: SegmentState,
}

#[cfg(feature = "scalable-topics")]
impl SegmentDescriptor {
    /// Decode from the wire [`crate::pb::scalable_topics::SegmentDescriptor`].
    #[must_use]
    pub fn from_pb(pb: &crate::pb::scalable_topics::SegmentDescriptor) -> Self {
        Self {
            segment_id: SegmentId(pb.segment_id),
            key_range: KeyRange {
                start: pb.key_range_start,
                end: pb.key_range_end,
            },
            broker_url: pb.broker_url.clone(),
            state: SegmentState::from_pb_i32(pb.state),
        }
    }

    /// Encode into the wire [`crate::pb::scalable_topics::SegmentDescriptor`].
    #[must_use]
    pub fn to_pb(&self) -> crate::pb::scalable_topics::SegmentDescriptor {
        crate::pb::scalable_topics::SegmentDescriptor {
            segment_id: self.segment_id.0,
            broker_url: self.broker_url.clone(),
            broker_url_tls: None,
            key_range_start: self.key_range.start,
            key_range_end: self.key_range.end,
            state: self.state.to_pb_i32(),
        }
    }
}

/// A logical message identifier (ledger / entry / batch / partition).
///
/// Mirrors the Java `MessageId` interface. `partition` defaults to `-1` for non-partitioned
/// topics; `batch_index` defaults to `-1` for non-batched messages.
///
/// # Structural equality (PIP-180)
///
/// Two `MessageId`s compare equal iff every structural field matches —
/// `(ledger_id, entry_id, partition, batch_index, batch_size)`. On a shadow topic
/// (PIP-180, ADR-0033) the broker presents messages with the **source** `MessageId`
/// (same ledger/entry pointers as the original write), so a shadow-side reader
/// observes ids that compare equal to the source-side reader's ids — "same
/// message" is structurally evident and needs no out-of-band correlation key.
///
/// # PIP-460 scalable-topic segment (experimental)
///
/// Under `feature = "scalable-topics"` the id carries an optional
/// `segment_id` field. The derived `PartialEq` / `Ord` / `Hash`
/// give exactly the cross-mode contract ADR-0031 specifies: two v4 ids both
/// carry `None`, so the segment field is a tie and the v4 invariant is
/// preserved bit-for-bit; a scalable id (`Some(_)`) never compares equal to a
/// v4 id (`None`) — so callers can't accidentally deduplicate across the
/// scalable / partitioned mode boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId {
    /// Bookkeeper ledger id where the entry lives.
    pub ledger_id: u64,
    /// Entry id within the ledger.
    pub entry_id: u64,
    /// Partition index, `-1` if non-partitioned.
    pub partition: i32,
    /// Index within a batched entry, `-1` if not batched.
    pub batch_index: i32,
    /// Size of the batch the message came from, `-1` if not batched.
    pub batch_size: i32,
    /// PIP-460 segment id (experimental). `None` for v4 partitioned /
    /// non-partitioned topics — this preserves the v4 wire layout and
    /// structural-equality contract. `Some(_)` only for messages read from a
    /// scalable topic.
    #[cfg(feature = "scalable-topics")]
    pub segment_id: Option<SegmentId>,
}

impl MessageId {
    /// A sentinel "earliest" position. Mirrors `MessageId.earliest`.
    pub const EARLIEST: Self = Self {
        ledger_id: u64::MAX,
        entry_id: u64::MAX,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
    };

    /// A sentinel "latest" position. Mirrors `MessageId.latest`.
    pub const LATEST: Self = Self {
        ledger_id: i64::MAX as u64,
        entry_id: i64::MAX as u64,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
    };

    /// Construct a message id from the wire protobuf representation.
    pub fn from_pb(pb: &pb::MessageIdData) -> Self {
        Self {
            ledger_id: pb.ledger_id,
            entry_id: pb.entry_id,
            partition: pb.partition.unwrap_or(-1),
            batch_index: pb.batch_index.unwrap_or(-1),
            batch_size: pb.batch_size.unwrap_or(-1),
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        }
    }

    /// Encode this message id back into its protobuf form.
    pub fn to_pb(self) -> pb::MessageIdData {
        pb::MessageIdData {
            ledger_id: self.ledger_id,
            entry_id: self.entry_id,
            partition: Some(self.partition),
            batch_index: Some(self.batch_index),
            ack_set: Vec::new(),
            batch_size: Some(self.batch_size),
            first_chunk_message_id: None,
        }
    }

    /// Serialise this message id to a portable byte string. Mirrors Java
    /// `MessageId#toByteArray` — encodes a `MessageIdData` protobuf message. Callers can
    /// stash the result anywhere (Kafka header, DB column, log line) and reconstruct via
    /// [`Self::from_bytes`] later.
    pub fn to_bytes(self) -> Vec<u8> {
        use prost::Message as _;
        // `encode_to_vec` is the idiomatic prost infallible encode: it sizes the `Vec`
        // via `encoded_len()` (so `BufMut::remaining_mut() == usize::MAX` on a `Vec`
        // never trips the EncodeError-on-short-buffer path). Invariant #6: no panics
        // in magnetar-proto outside `#[cfg(test)]`.
        self.to_pb().encode_to_vec()
    }

    /// Reconstruct a message id from the byte string produced by [`Self::to_bytes`].
    /// Mirrors Java `MessageId#fromByteArray`. Returns `None` if `bytes` is not a valid
    /// protobuf `MessageIdData`.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        use prost::Message as _;
        let pb = pb::MessageIdData::decode(bytes).ok()?;
        Some(Self::from_pb(&pb))
    }

    /// PIP-460: attach a [`SegmentId`] to this message id, marking it as read
    /// from a scalable topic's segment. The runtime scalable layer calls this
    /// when surfacing a message delivered on a per-segment v4 consumer so the
    /// caller can correlate the id back to its DAG node.
    #[cfg(feature = "scalable-topics")]
    #[must_use]
    pub fn with_segment(mut self, segment_id: SegmentId) -> Self {
        self.segment_id = Some(segment_id);
        self
    }

    /// PIP-460: the segment this message was read from, if any. `None` for v4
    /// partitioned / non-partitioned topics.
    #[cfg(feature = "scalable-topics")]
    #[must_use]
    pub fn segment(&self) -> Option<SegmentId> {
        self.segment_id
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}",
            self.ledger_id, self.entry_id, self.partition, self.batch_index
        )
    }
}

/// The transport-layer compression codec selected for a producer.
///
/// Maps 1:1 to `pb::CompressionType`. The state machine carries this enum so callers do not have
/// to deal with the protobuf i32 directly. Re-encoded onto the wire by the producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CompressionKind {
    /// No compression.
    #[default]
    None,
    /// LZ4 block compression.
    Lz4,
    /// Zlib deflate.
    Zlib,
    /// Zstandard.
    Zstd,
    /// Snappy.
    Snappy,
}

impl CompressionKind {
    /// Convert to the wire-format `pb::CompressionType`.
    pub fn to_pb(self) -> pb::CompressionType {
        match self {
            Self::None => pb::CompressionType::None,
            Self::Lz4 => pb::CompressionType::Lz4,
            Self::Zlib => pb::CompressionType::Zlib,
            Self::Zstd => pb::CompressionType::Zstd,
            Self::Snappy => pb::CompressionType::Snappy,
        }
    }

    /// Decode from the wire-format `pb::CompressionType` integer.
    pub fn from_pb_i32(value: i32) -> Self {
        match pb::CompressionType::try_from(value).unwrap_or(pb::CompressionType::None) {
            pb::CompressionType::None => Self::None,
            pb::CompressionType::Lz4 => Self::Lz4,
            pb::CompressionType::Zlib => Self::Zlib,
            pb::CompressionType::Zstd => Self::Zstd,
            pb::CompressionType::Snappy => Self::Snappy,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a non-batched MessageId mirroring Java `MessageIdImpl(ledger, entry,
    /// partition)`. `batch_index = -1` marks "not batched" (Java semantics).
    fn mid(ledger: u64, entry: u64, partition: i32) -> MessageId {
        MessageId {
            ledger_id: ledger,
            entry_id: entry,
            partition,
            batch_index: -1,
            batch_size: 0,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        }
    }

    /// Helper: build a batched MessageId mirroring Java `BatchMessageIdImpl(ledger, entry,
    /// partition, batch_index)`.
    fn bmid(ledger: u64, entry: u64, partition: i32, batch_index: i32) -> MessageId {
        MessageId {
            ledger_id: ledger,
            entry_id: entry,
            partition,
            batch_index,
            batch_size: 0,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        }
    }

    /// Layer (a) test #4: `MessageId { segment_id: Some(..) }` round-trips
    /// through [`MessageId::with_segment`] / [`MessageId::segment`], and the
    /// v4-shape (`segment_id: None`) `to_bytes` is **byte-identical** to a
    /// plain v4 id — the wire `MessageIdData` carries no segment field, so a
    /// legacy producer / consumer round-trips bit-for-bit (ADR-0031 §2.1).
    #[cfg(feature = "scalable-topics")]
    #[test]
    fn message_id_with_segment_roundtrip() {
        let v4 = mid(10, 20, 1);
        assert_eq!(v4.segment(), None, "v4 id has no segment");

        // In-process segment attach / read.
        let scaled = v4.with_segment(SegmentId(7));
        assert_eq!(scaled.segment(), Some(SegmentId(7)));

        // Cross-mode equality contract: scalable (`Some`) != v4 (`None`).
        assert_ne!(scaled, v4, "Some(_) segment never equals None segment");
        // Same segment + same coords == equal.
        let scaled2 = mid(10, 20, 1).with_segment(SegmentId(7));
        assert_eq!(scaled, scaled2);
        // Different segment, same coords != equal.
        let other_seg = mid(10, 20, 1).with_segment(SegmentId(8));
        assert_ne!(scaled, other_seg);

        // v4 invariant preserved: two `None`-segment ids compare exactly as
        // the 5-field v4 contract did.
        let v4b = mid(10, 20, 1);
        assert_eq!(v4, v4b);

        // Byte-identical guard: the wire `MessageIdData` has no segment field,
        // so `to_bytes` for a `None`-segment id matches a freshly-built v4 id
        // with the same coords. (The segment rides the lookup / DAG, not the
        // per-message wire id, until the Pulsar 5.0 RC vendor bump.)
        let none_seg = mid(10, 20, 1);
        assert_eq!(
            none_seg.to_bytes(),
            v4.to_bytes(),
            "None-segment wire encoding is byte-identical to v4"
        );
        // A `Some`-segment id encodes the same wire bytes (segment is dropped
        // on the v0.2.0 wire) — documented scaffold behaviour.
        assert_eq!(
            scaled.to_bytes(),
            v4.to_bytes(),
            "segment is in-process only on the v0.2.0 scaffold wire"
        );
    }

    #[test]
    fn message_id_byte_roundtrip() {
        let id = MessageId {
            ledger_id: 1234,
            entry_id: 5678,
            partition: 2,
            batch_index: 7,
            batch_size: 16,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        let bytes = id.to_bytes();
        let back = MessageId::from_bytes(&bytes).expect("decode");
        assert_eq!(back, id);
    }

    #[test]
    fn message_id_from_bytes_rejects_garbage() {
        let garbage = &[0xFF, 0xFE, 0xFD][..];
        assert!(MessageId::from_bytes(garbage).is_none());
    }

    /// V6: `MessageId::to_bytes` previously used `.expect("encoding MessageIdData into
    /// a fresh Vec cannot fail")` — a panic-shaped invariant-#6 violation. The fix
    /// switches to `prost::Message::encode_to_vec`, which is infallible by contract
    /// (writes to an internally-sized `Vec` via `BufMut::remaining_mut() == usize::MAX`).
    /// Smoke-test every documented edge case: `EARLIEST` / `LATEST` sentinels, batched
    /// ids with negative `batch_index`, ids with `partition == -1`, and round-trip
    /// each through `from_bytes` so we know the encoder didn't silently truncate.
    #[test]
    fn to_bytes_never_panics_on_edge_cases() {
        for id in [
            MessageId::EARLIEST,
            MessageId::LATEST,
            MessageId {
                ledger_id: 0,
                entry_id: 0,
                partition: -1,
                batch_index: -1,
                batch_size: 0,
                #[cfg(feature = "scalable-topics")]
                segment_id: None,
            },
            MessageId {
                ledger_id: u64::MAX,
                entry_id: u64::MAX,
                partition: i32::MAX,
                batch_index: i32::MIN,
                batch_size: i32::MIN,
                #[cfg(feature = "scalable-topics")]
                segment_id: None,
            },
        ] {
            // No panic on encode + round-trip — the previous `.expect(...)` path is gone.
            let bytes = id.to_bytes();
            let back = MessageId::from_bytes(&bytes).expect("round-trip decode");
            assert_eq!(back, id);
        }
    }

    /// Ported from Java `MessageIdCompareToTest#testEqual` (non-batched + batched variants).
    /// Two MessageIds with identical fields must compare equal.
    #[test]
    fn message_id_compare_to_equal() {
        // Non-batched
        let a = mid(123, 345, 567);
        let b = mid(123, 345, 567);
        assert_eq!(a.cmp(&b), core::cmp::Ordering::Equal);

        // Batched
        let c = bmid(234, 345, 456, 567);
        let d = bmid(234, 345, 456, 567);
        assert_eq!(c.cmp(&d), core::cmp::Ordering::Equal);
    }

    /// Ported from Java `MessageIdCompareToTest#testGreaterThan` and `testLessThan`.
    /// Verifies the (ledger, entry, partition, batch_index) lexicographic ordering and its
    /// antisymmetry — for every `a > b`, `b < a` must hold.
    #[test]
    fn message_id_compare_to_greater_and_less_than() {
        // Non-batched: walk one axis at a time.
        let m1 = mid(124, 345, 567);
        let m2 = mid(123, 345, 567);
        let m3 = mid(123, 344, 567);
        let m4 = mid(123, 344, 566);
        assert!(m1 > m2, "ledger axis: m1>m2");
        assert!(m1 > m3, "ledger then entry: m1>m3");
        assert!(m1 > m4, "ledger axis dominates: m1>m4");
        assert!(m2 > m3, "entry axis: m2>m3");
        assert!(m2 > m4, "entry then partition: m2>m4");
        assert!(m3 > m4, "partition axis: m3>m4");
        // Antisymmetry — every `>` above must have a `<` counterpart.
        assert!(m2 < m1);
        assert!(m4 < m3);

        // Batched: same axes plus a batch_index tiebreaker.
        let b1 = bmid(235, 345, 456, 567);
        let b2 = bmid(234, 346, 456, 567);
        let b3 = bmid(234, 345, 456, 568);
        let b4 = bmid(234, 345, 457, 567);
        let b5 = bmid(234, 345, 456, 567);
        assert!(b1 > b2, "ledger dominates entry");
        assert!(b1 > b3, "ledger dominates batch_index");
        assert!(b1 > b4, "ledger dominates partition");
        assert!(b1 > b5);
        assert!(b2 > b3, "entry axis: b2>b3");
        assert!(b2 > b4, "entry dominates partition");
        assert!(b2 > b5, "entry axis: b2>b5");
        assert!(b4 > b3, "partition dominates batch_index");
        assert!(b3 > b5, "batch_index axis: b3>b5");
        assert!(b4 > b5, "partition axis: b4>b5");
        // Antisymmetric checks.
        assert!(b2 < b1);
        assert!(b5 < b3);
    }

    /// Ported from Java `MessageIdCompareToTest#compareToSymmetricTest`. The key invariant: a
    /// "non-batched" message id (`batch_index == -1`) and a "batched" one with the same
    /// `(ledger, entry, partition)` but `batch_index == -1` compare equal — Java treats a
    /// `MessageIdImpl` as equivalent to a `BatchMessageIdImpl(..., -1)`. The single Rust
    /// `MessageId` struct unifies both: this test pins down that the derived `Ord` still puts
    /// `batch_index = -1` before any non-negative `batch_index`.
    #[test]
    fn message_id_compare_to_batched_versus_non_batched_symmetric() {
        let plain = mid(123, 345, 567);
        let b1 = bmid(123, 345, 567, -1); // identical
        let b2 = bmid(123, 345, 567, 1); // batched, same (l, e, p)
        let b3 = bmid(123, 345, 566, 1); // batched, smaller partition
        let b4 = bmid(123, 345, 566, -1); // non-batched, smaller partition

        // batch_index = -1 with identical (l, e, p) is the "same" id.
        assert_eq!(plain.cmp(&b1), core::cmp::Ordering::Equal);
        assert_eq!(b1.cmp(&plain), core::cmp::Ordering::Equal);

        // Any positive batch_index orders strictly after batch_index = -1 for identical (l, e, p).
        assert!(b2 > plain, "b2 (batch_index=1) > plain (batch_index=-1)");
        assert!(plain < b2);

        // Smaller partition dominates batch_index tiebreaker.
        assert!(plain > b3);
        assert!(b3 < plain);
        assert!(plain > b4);
        assert!(b4 < plain);
    }

    /// Ported from Java `MessageIdSerializationTest#testProtobufSerialization2`.
    /// `partition = -1` (non-partitioned topic) must survive the byte round-trip.
    #[test]
    fn message_id_byte_roundtrip_non_partitioned() {
        let id = MessageId {
            ledger_id: 1,
            entry_id: 2,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        let bytes = id.to_bytes();
        let back = MessageId::from_bytes(&bytes).expect("decode non-partitioned id");
        assert_eq!(back, id);
        assert_eq!(back.partition, -1);
        assert_eq!(back.batch_index, -1);
    }

    /// Ported from Java `MessageIdSerializationTest#testBatchSizeNotSet`. The wire format
    /// distinguishes "batch_size absent" from "batch_size = 0"; in Rust we collapse the
    /// "absent" case to `-1` so callers can always reason about the value as an `i32`.
    /// Round-tripping through `to_bytes` / `from_bytes` must preserve `batch_size = -1`.
    #[test]
    fn message_id_byte_roundtrip_batch_size_absent() {
        let id = MessageId {
            ledger_id: 1,
            entry_id: 2,
            partition: 3,
            batch_index: 4,
            batch_size: -1,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        let bytes = id.to_bytes();
        let back = MessageId::from_bytes(&bytes).expect("decode batched id w/o batch_size");
        assert_eq!(back, id);
        assert_eq!(back.batch_size, -1);
    }

    /// Ported (with a documented divergence) from Java
    /// `MessageIdSerializationTest#testProtobufSerializationEmpty`. Java throws
    /// `IOException` on empty bytes because its `required` fields are enforced at decode.
    /// `prost` accepts empty input and fills the `required` fields with their wire-format
    /// defaults (zero). We document the divergence here: an empty buffer decodes to a
    /// "default" `MessageId` with `ledger_id = 0, entry_id = 0, partition = -1,
    /// batch_index = -1, batch_size = -1`. Callers that need Java-style strictness should
    /// reject empty buffers themselves before calling `from_bytes`.
    #[test]
    fn message_id_from_bytes_empty_decodes_to_zero() {
        let decoded = MessageId::from_bytes(&[]).expect("prost accepts empty buffer");
        assert_eq!(
            decoded,
            MessageId {
                ledger_id: 0,
                entry_id: 0,
                partition: -1,
                batch_index: -1,
                batch_size: -1,
                #[cfg(feature = "scalable-topics")]
                segment_id: None,
            },
            "empty buffer decodes to wire-format defaults"
        );
    }

    /// `MessageId` derives `Hash` so it can key hash maps (e.g. `pending_acks`). Two
    /// MessageIds with identical fields must hash identically. Pinned because the field order
    /// — and therefore the `Hash` impl shape — is part of the public surface.
    #[test]
    fn message_id_hash_consistent_with_eq() {
        use std::collections::HashSet;
        let a = MessageId {
            ledger_id: 7,
            entry_id: 8,
            partition: 9,
            batch_index: 10,
            batch_size: 11,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        let b = a;
        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
        assert_eq!(set.len(), 1);
    }

    /// Sanity-check the sentinel ordering: `EARLIEST` is the largest possible position by
    /// virtue of `ledger_id = u64::MAX`, while `LATEST` uses `i64::MAX as u64`. They must
    /// compare unequal and respect the derived `Ord`.
    #[test]
    fn message_id_earliest_and_latest_sentinels_distinct() {
        assert_ne!(MessageId::EARLIEST, MessageId::LATEST);
        // `u64::MAX` > `i64::MAX as u64`, so EARLIEST is "larger" under derived `Ord`.
        // This is an arbitrary but stable encoding; mirror what we promise to callers.
        assert!(MessageId::EARLIEST > MessageId::LATEST);
        // Sentinels round-trip through the byte format like any other id.
        let earliest_bytes = MessageId::EARLIEST.to_bytes();
        assert_eq!(
            MessageId::from_bytes(&earliest_bytes),
            Some(MessageId::EARLIEST)
        );
    }

    /// PIP-180 / ADR-0033: pins the documented structural-equality contract on
    /// `MessageId`. The broker on a shadow topic presents messages with the **source**
    /// `(ledger_id, entry_id, batch_index, partition)`; a structurally identical id
    /// constructed on the source-side reader must compare `==` and hash to the same
    /// bucket. Without this, callers cannot use `MessageId` as a deduplication key
    /// across the source ⇄ shadow split.
    #[test]
    fn message_id_equality_shadow_vs_source() {
        use std::collections::HashSet;
        // Same physical entry observed on both sides — ledger/entry/partition/batch_index
        // all match. PIP-180's "same message" contract.
        let source_side = MessageId {
            ledger_id: 42,
            entry_id: 7,
            partition: 0,
            batch_index: -1,
            batch_size: 0,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        let shadow_side = MessageId {
            ledger_id: 42,
            entry_id: 7,
            partition: 0,
            batch_index: -1,
            batch_size: 0,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        assert_eq!(source_side, shadow_side, "PIP-180 structural equality");
        // Hash consistency — must collide so callers can use the id as a HashSet/HashMap key
        // across the source ⇄ shadow boundary.
        let mut set = HashSet::new();
        set.insert(source_side);
        assert!(set.contains(&shadow_side));
        // A different ledger or entry breaks equality (sanity).
        let other = MessageId {
            ledger_id: 42,
            entry_id: 8,
            partition: 0,
            batch_index: -1,
            batch_size: 0,
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        };
        assert_ne!(source_side, other);
    }

    /// `CompressionKind::from_pb_i32` accepts unknown protobuf integers by falling through to
    /// `None`. Mirrors the Java `Commands#getCompressionType` fall-back so a future broker
    /// (with an enum we have not yet bumped) cannot crash decode.
    #[test]
    fn compression_kind_unknown_variant_falls_back_to_none() {
        let unknown = CompressionKind::from_pb_i32(9999);
        assert_eq!(unknown, CompressionKind::None);
    }

    /// Every `CompressionKind` round-trips through `to_pb` -> `from_pb_i32`.
    #[test]
    fn compression_kind_round_trips_through_pb() {
        for kind in [
            CompressionKind::None,
            CompressionKind::Lz4,
            CompressionKind::Zlib,
            CompressionKind::Zstd,
            CompressionKind::Snappy,
        ] {
            let pb = kind.to_pb();
            assert_eq!(
                CompressionKind::from_pb_i32(pb as i32),
                kind,
                "round-trip for {kind:?}"
            );
        }
    }
}
