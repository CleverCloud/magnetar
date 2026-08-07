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
/// **Experimental** (PIP-460, ADR-0093). Only meaningful under
/// `feature = "scalable-topics"`. M1 ordinary [`MessageId`] values do not carry
/// this identity; scalable deliveries qualify them with a separate segment
/// source.
#[cfg(feature = "scalable-topics")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SegmentId(pub u64);

#[cfg(feature = "scalable-topics")]
impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Lowest value in the M1 16-bit hash key space.
#[cfg(feature = "scalable-topics")]
pub const MIN_HASH: u32 = 0;

/// Highest value in the M1 16-bit hash key space.
#[cfg(feature = "scalable-topics")]
pub const MAX_HASH: u32 = u16::MAX as u32;

/// An invalid M1 inclusive hash range.
#[cfg(feature = "scalable-topics")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum KeyRangeError {
    /// The start lies outside the 16-bit key space.
    #[error("hash range start {start} is outside 0..={MAX_HASH}")]
    StartOutOfBounds {
        /// Invalid inclusive start.
        start: u32,
    },
    /// The end lies outside the 16-bit key space.
    #[error("hash range end {end} is outside 0..={MAX_HASH}")]
    EndOutOfBounds {
        /// Invalid inclusive end.
        end: u32,
    },
    /// An inclusive range cannot end before it starts.
    #[error("hash range end {end} precedes start {start}")]
    Reversed {
        /// Inclusive start.
        start: u32,
        /// Inclusive end.
        end: u32,
    },
}

/// PIP-460 inclusive hash key range `[start, end]` a segment is responsible for.
///
/// **Experimental** (PIP-460, ADR-0093). Surfaces the key range for
/// observation only — segment-aware sticky-key dispatch (Key_Shared across
/// the full DAG) is out of scope (future work).
#[cfg(feature = "scalable-topics")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyRange {
    /// Inclusive start of the hash range.
    start: u32,
    /// Inclusive end of the hash range.
    end: u32,
}

#[cfg(feature = "scalable-topics")]
impl KeyRange {
    /// The complete M1 key space, `0..=65535`.
    pub const FULL: Self = Self {
        start: MIN_HASH,
        end: MAX_HASH,
    };

    /// Validate and construct an inclusive M1 key range.
    ///
    /// # Errors
    ///
    /// Returns [`KeyRangeError`] when either endpoint lies outside the 16-bit
    /// key space or the range is reversed.
    pub fn new(start: u32, end: u32) -> Result<Self, KeyRangeError> {
        if start > MAX_HASH {
            return Err(KeyRangeError::StartOutOfBounds { start });
        }
        if end > MAX_HASH {
            return Err(KeyRangeError::EndOutOfBounds { end });
        }
        if end < start {
            return Err(KeyRangeError::Reversed { start, end });
        }
        Ok(Self { start, end })
    }

    /// Inclusive start.
    #[must_use]
    pub const fn start(self) -> u32 {
        self.start
    }

    /// Inclusive end.
    #[must_use]
    pub const fn end(self) -> u32 {
        self.end
    }

    /// Number of hash values in this inclusive range.
    #[must_use]
    pub const fn len(self) -> u32 {
        self.end - self.start + 1
    }

    /// Whether this range contains no values. A validated range is never empty.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        false
    }

    /// Whether this range contains `hash`, including both endpoints.
    #[must_use]
    pub const fn contains(self, hash: u32) -> bool {
        self.start <= hash && hash <= self.end
    }

    /// Whether this range completely contains `other`.
    #[must_use]
    pub const fn contains_range(self, other: Self) -> bool {
        self.start <= other.start && self.end >= other.end
    }

    /// Whether this range and `other` touch without overlapping.
    #[must_use]
    pub const fn is_adjacent_to(self, other: Self) -> bool {
        (self.end < MAX_HASH && self.end + 1 == other.start)
            || (other.end < MAX_HASH && other.end + 1 == self.start)
    }

    /// Canonical M1 lower-case hexadecimal range descriptor.
    #[must_use]
    pub fn to_hex_string(self) -> String {
        format!("{:04x}-{:04x}", self.start, self.end)
    }
}

#[cfg(feature = "scalable-topics")]
impl TryFrom<(u32, u32)> for KeyRange {
    type Error = KeyRangeError;

    fn try_from((start, end): (u32, u32)) -> Result<Self, Self::Error> {
        Self::new(start, end)
    }
}

/// PIP-460 segment lifecycle state.
///
/// **Experimental** (PIP-460, ADR-0093). Mirrors the upstream wire enum
/// [`pb::SegmentState`], which has exactly two members — a segment is either
/// serving writes or sealed. `#[non_exhaustive]` so a future broker enum value
/// cannot break a `match` on this type downstream.
///
/// A split or merge is **not** a segment state upstream: it is a DAG-topology
/// change, read off the `parent_ids` / `child_ids` edges of a new layout and
/// stamped by a fresh [`ScalableTopicDag`](crate::pb::ScalableTopicDag) epoch.
#[cfg(feature = "scalable-topics")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum SegmentState {
    /// Segment is live and serving reads/writes.
    #[default]
    Active,
    /// Segment is sealed (no more writes); reads drain then it is removed.
    Sealed,
}

#[cfg(feature = "scalable-topics")]
impl SegmentState {
    /// Strictly convert from the M1 wire enum integer.
    ///
    /// # Errors
    ///
    /// Returns the unknown integer instead of silently treating a future state
    /// as active.
    pub fn try_from_pb_i32(value: i32) -> Result<Self, i32> {
        match crate::pb::SegmentState::try_from(value) {
            Ok(crate::pb::SegmentState::Sealed) => Ok(Self::Sealed),
            Ok(crate::pb::SegmentState::Active) => Ok(Self::Active),
            Err(_) => Err(value),
        }
    }

    /// Convert to the wire enum integer.
    #[must_use]
    pub fn to_pb_i32(self) -> i32 {
        match self {
            Self::Sealed => crate::pb::SegmentState::Sealed as i32,
            Self::Active => crate::pb::SegmentState::Active as i32,
        }
    }
}

/// PIP-460 segment descriptor — one node of a scalable topic's segment DAG.
///
/// **Experimental** (PIP-460, ADR-0093). Assembled from the upstream wire pair
/// [`pb::SegmentInfoProto`] (topology + lifecycle) and [`pb::SegmentBrokerAddress`]
/// (placement), which [`pb::ScalableTopicDag`] carries as two parallel lists keyed
/// by `segment_id`. Placement is therefore **optional**: a sealed segment the
/// broker no longer serves has no address entry, and `broker_url` is `None` for it.
///
/// `parent_ids` / `child_ids` are the DAG edges. They are what identifies a split
/// (one parent, several children) or a merge (several parents, one child) — see
/// [`DagDelta`](crate::dag_watch::DagDelta).
#[cfg(feature = "scalable-topics")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentDescriptor {
    /// Segment id, unique within the topic DAG.
    pub segment_id: SegmentId,
    /// Hash key range this segment serves.
    pub key_range: KeyRange,
    /// Plaintext broker URL serving this segment, when the DAG carries a
    /// placement entry for it.
    pub broker_url: Option<String>,
    /// TLS broker URL serving this segment, when advertised.
    pub broker_url_tls: Option<String>,
    /// Lifecycle state.
    pub state: SegmentState,
    /// Ids of the segments this one descends from (empty for an original segment).
    pub parent_ids: Vec<SegmentId>,
    /// Ids of the segments that descend from this one (empty for a leaf).
    pub child_ids: Vec<SegmentId>,
    /// DAG generation at which the segment was created. This is a layout epoch,
    /// not a clock.
    pub created_at_epoch: u64,
    /// DAG generation at which the segment was sealed, when it is sealed.
    pub sealed_at_epoch: Option<u64>,
    /// Legacy-segment marker. When set, the segment is not managed by the
    /// scalable-topic controller and wraps this externally-managed
    /// `persistent://...` topic instead of a `segment://...` one. The broker sets
    /// it on the synthetic single-segment layout it returns for a regular topic
    /// that has not been migrated to a scalable topic.
    pub legacy_topic_name: Option<String>,
}

/// Invalid data while joining an M1 segment descriptor and placement.
#[cfg(feature = "scalable-topics")]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SegmentDescriptorError {
    /// The wire range is not an inclusive 16-bit range.
    #[error("invalid segment hash range: {0}")]
    InvalidRange(#[from] KeyRangeError),
    /// The wire carried a segment lifecycle state not defined by M1.
    #[error("unknown M1 segment state {0}")]
    UnknownState(i32),
    /// A placement was joined to a different descriptor.
    #[error("placement for segment {placement_id} cannot attach to segment {segment_id}")]
    PlacementMismatch {
        /// Descriptor id.
        segment_id: u64,
        /// Placement id.
        placement_id: u64,
    },
    /// M1 uses absence, not an empty string, for a non-legacy segment.
    #[error("legacy topic name must not be empty")]
    EmptyLegacyTopic,
}

#[cfg(feature = "scalable-topics")]
impl SegmentDescriptor {
    /// Assemble from the wire pair. `broker` is the [`pb::SegmentBrokerAddress`]
    /// whose `segment_id` matches `info`, when the DAG carries one.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentDescriptorError`] for invalid M1 ranges, states, legacy
    /// markers, or a mismatched placement id.
    pub fn try_from_pb(
        info: &crate::pb::SegmentInfoProto,
        broker: Option<&crate::pb::SegmentBrokerAddress>,
    ) -> Result<Self, SegmentDescriptorError> {
        if let Some(broker) = broker
            && broker.segment_id != info.segment_id
        {
            return Err(SegmentDescriptorError::PlacementMismatch {
                segment_id: info.segment_id,
                placement_id: broker.segment_id,
            });
        }
        if info.legacy_topic_name.as_deref() == Some("") {
            return Err(SegmentDescriptorError::EmptyLegacyTopic);
        }
        Ok(Self {
            segment_id: SegmentId(info.segment_id),
            key_range: KeyRange::new(info.hash_start, info.hash_end)?,
            broker_url: broker.map(|b| b.broker_url.clone()),
            broker_url_tls: broker.and_then(|b| b.broker_url_tls.clone()),
            state: SegmentState::try_from_pb_i32(info.state)
                .map_err(SegmentDescriptorError::UnknownState)?,
            parent_ids: info.parent_ids.iter().copied().map(SegmentId).collect(),
            child_ids: info.child_ids.iter().copied().map(SegmentId).collect(),
            created_at_epoch: info.created_at_epoch,
            sealed_at_epoch: info.sealed_at_epoch,
            legacy_topic_name: info.legacy_topic_name.clone(),
        })
    }

    /// Split back into the wire pair. The address half is `None` when the
    /// descriptor carries no placement.
    ///
    /// `created_at_ms` / `sealed_at_ms` are broker-authored wall-clock stamps that
    /// this client only ever reads, so the encode side emits `0` / `None` for them
    /// rather than inventing a clock — `magnetar-proto` holds no clock at all
    /// (ADR-0011). Round-tripping a decoded descriptor therefore does not preserve
    /// them; nothing in the client reads them back.
    #[must_use]
    pub fn to_pb(
        &self,
    ) -> (
        crate::pb::SegmentInfoProto,
        Option<crate::pb::SegmentBrokerAddress>,
    ) {
        let info = crate::pb::SegmentInfoProto {
            segment_id: self.segment_id.0,
            hash_start: self.key_range.start(),
            hash_end: self.key_range.end(),
            state: self.state.to_pb_i32(),
            parent_ids: self.parent_ids.iter().map(|s| s.0).collect(),
            child_ids: self.child_ids.iter().map(|s| s.0).collect(),
            created_at_epoch: self.created_at_epoch,
            sealed_at_epoch: self.sealed_at_epoch,
            created_at_ms: 0,
            sealed_at_ms: None,
            legacy_topic_name: self.legacy_topic_name.clone(),
        };
        let address = self
            .broker_url
            .as_ref()
            .map(|url| crate::pb::SegmentBrokerAddress {
                segment_id: self.segment_id.0,
                broker_url: url.clone(),
                broker_url_tls: self.broker_url_tls.clone(),
            });
        (info, address)
    }

    /// `true` when this descriptor is the broker's synthetic wrapper around a
    /// regular, unmigrated topic rather than a controller-managed segment.
    #[must_use]
    pub fn is_legacy(&self) -> bool {
        self.legacy_topic_name
            .as_deref()
            .is_some_and(|name| !name.is_empty())
    }
}

/// PIP-473 transaction-coordinator assignment — which broker serves one
/// coordinator partition.
///
/// **Experimental** (PIP-460 / PIP-473, ADR-0093). Delivered by the
/// metadata-driven coordinator-discovery watch, which replaces resolving the
/// coordinator topic through an ordinary lookup.
#[cfg(feature = "scalable-topics")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcAssignment {
    /// Transaction-coordinator partition id.
    pub tc_id: u64,
    /// Plaintext broker URL serving this coordinator, when advertised.
    pub broker_service_url: Option<String>,
    /// TLS broker URL serving this coordinator, when advertised.
    pub broker_service_url_tls: Option<String>,
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
/// M1 does not extend `MessageIdData` with a scalable segment. Scalable values
/// therefore qualify this ordinary id with an explicit segment source rather
/// than changing its equality or wire representation.
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
}

impl MessageId {
    /// A sentinel "earliest" position. Mirrors `MessageId.earliest`.
    pub const EARLIEST: Self = Self {
        ledger_id: u64::MAX,
        entry_id: u64::MAX,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
    };

    /// A sentinel "latest" position. Mirrors `MessageId.latest`.
    pub const LATEST: Self = Self {
        ledger_id: i64::MAX as u64,
        entry_id: i64::MAX as u64,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
    };

    /// Construct a message id from the wire protobuf representation.
    pub fn from_pb(pb: &pb::MessageIdData) -> Self {
        Self {
            ledger_id: pb.ledger_id,
            entry_id: pb.entry_id,
            partition: pb.partition.unwrap_or(-1),
            batch_index: pb.batch_index.unwrap_or(-1),
            batch_size: pb.batch_size.unwrap_or(-1),
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
    #[cfg(feature = "scalable-topics")]
    use proptest::prelude::*;

    use super::*;

    #[cfg(feature = "scalable-topics")]
    #[test]
    fn key_range_uses_inclusive_m1_endpoints() {
        let full = KeyRange::FULL;
        assert_eq!(full.start(), 0);
        assert_eq!(full.end(), 65_535);
        assert_eq!(full.len(), 65_536);
        assert!(full.contains(0));
        assert!(full.contains(65_535));
        assert_eq!(full.to_hex_string(), "0000-ffff");

        let halves = [
            KeyRange::new(0, 32_767).expect("lower half"),
            KeyRange::new(32_768, 65_535).expect("upper half"),
        ];
        assert!(halves[0].is_adjacent_to(halves[1]));
        assert_eq!(halves[0].len() + halves[1].len(), full.len());
    }

    #[cfg(feature = "scalable-topics")]
    #[test]
    fn key_range_rejects_every_invalid_shape() {
        assert_eq!(
            KeyRange::new(65_536, 65_536),
            Err(KeyRangeError::StartOutOfBounds { start: 65_536 })
        );
        assert_eq!(
            KeyRange::new(0, 65_536),
            Err(KeyRangeError::EndOutOfBounds { end: 65_536 })
        );
        assert_eq!(
            KeyRange::new(2, 1),
            Err(KeyRangeError::Reversed { start: 2, end: 1 })
        );
    }

    #[cfg(feature = "scalable-topics")]
    #[test]
    fn segment_descriptor_rejects_mismatched_placement_identity() {
        let info = pb::SegmentInfoProto {
            segment_id: 1,
            hash_start: 0,
            hash_end: 65_535,
            state: pb::SegmentState::Active as i32,
            parent_ids: Vec::new(),
            child_ids: Vec::new(),
            created_at_epoch: 0,
            sealed_at_epoch: None,
            created_at_ms: 0,
            sealed_at_ms: None,
            legacy_topic_name: None,
        };
        let placement = pb::SegmentBrokerAddress {
            segment_id: 2,
            broker_url: "pulsar://broker:6650".to_owned(),
            broker_url_tls: None,
        };
        assert_eq!(
            SegmentDescriptor::try_from_pb(&info, Some(&placement)),
            Err(SegmentDescriptorError::PlacementMismatch {
                segment_id: 1,
                placement_id: 2,
            })
        );
    }

    #[cfg(feature = "scalable-topics")]
    proptest! {
        #[test]
        fn key_range_contains_exactly_its_inclusive_interval(
            start in 0u32..=65_535,
            end in 0u32..=65_535,
            hash in 0u32..=65_535,
        ) {
            let (start, end) = if start <= end { (start, end) } else { (end, start) };
            let range = KeyRange::new(start, end).expect("ordered bounded range");
            prop_assert_eq!(range.contains(hash), start <= hash && hash <= end);
            prop_assert_eq!(range.len(), end - start + 1);
        }
    }

    /// Helper: build a non-batched MessageId mirroring Java `MessageIdImpl(ledger, entry,
    /// partition)`. `batch_index = -1` marks "not batched" (Java semantics).
    fn mid(ledger: u64, entry: u64, partition: i32) -> MessageId {
        MessageId {
            ledger_id: ledger,
            entry_id: entry,
            partition,
            batch_index: -1,
            batch_size: 0,
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
        }
    }

    #[test]
    fn message_id_byte_roundtrip() {
        let id = MessageId {
            ledger_id: 1234,
            entry_id: 5678,
            partition: 2,
            batch_index: 7,
            batch_size: 16,
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
            },
            MessageId {
                ledger_id: u64::MAX,
                entry_id: u64::MAX,
                partition: i32::MAX,
                batch_index: i32::MIN,
                batch_size: i32::MIN,
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
        };
        let shadow_side = MessageId {
            ledger_id: 42,
            entry_id: 7,
            partition: 0,
            batch_index: -1,
            batch_size: 0,
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
