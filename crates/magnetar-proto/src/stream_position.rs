// SPDX-License-Identifier: Apache-2.0

//! Canonical source-qualified scalable stream positions.
//!
//! `StreamMessageId` and `PositionVector` use the frozen Magnetar `MSTR` v1
//! envelope from the M1 hardened-consumer proposal. Delivery authority is
//! deliberately absent from this module: serialized values are positions, not
//! permission to acknowledge through a live consumer.

use std::collections::BTreeMap;

use prost::Message as _;

use crate::pb;
use crate::scalable_consumer::{SegmentSource, SegmentTopicError};
use crate::types::MessageId;

const MAGIC: &[u8; 4] = b"MSTR";
const VERSION: u8 = 1;
const STREAM_MESSAGE_ID_KIND: u8 = 1;
const POSITION_VECTOR_KIND: u8 = 2;
const HEADER_LEN: usize = 12;

/// Maximum serialized position components in v1.
pub const MAX_POSITION_COMPONENTS: usize = 4_096;
/// Maximum UTF-8 bytes in a canonical segment topic.
pub const MAX_POSITION_TOPIC_SIZE: usize = 4_096;
/// Maximum bytes in one canonical ordinary `MessageIdData` blob.
pub const MAX_ORDINARY_MESSAGE_ID_SIZE: usize = 65_536;
/// Maximum complete `MSTR` envelope size.
pub const MAX_STREAM_POSITION_SIZE: usize = 1024 * 1024;

/// Strict `MSTR` v1 codec failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamPositionError {
    /// The complete value exceeds the fixed safety bound.
    #[error("stream position is {actual} bytes; maximum is {max}")]
    EnvelopeTooLarge {
        /// Actual bytes.
        actual: usize,
        /// Fixed maximum.
        max: usize,
    },
    /// The fixed header was incomplete.
    #[error("truncated MSTR header")]
    TruncatedHeader,
    /// The value is not a Magnetar stream position.
    #[error("invalid MSTR magic")]
    InvalidMagic,
    /// Only the frozen v1 shape is accepted.
    #[error("unsupported MSTR version {0}")]
    UnsupportedVersion(u8),
    /// The caller requested one value kind but the envelope carried another.
    #[error("unexpected MSTR kind {got}; expected {expected}")]
    UnexpectedKind {
        /// Encoded kind.
        got: u8,
        /// Required kind.
        expected: u8,
    },
    /// Both v1 flag bytes are frozen to zero.
    #[error("unsupported MSTR flags 0x{0:04x}")]
    UnsupportedFlags(u16),
    /// Header length and actual payload size disagree, including trailing data.
    #[error("MSTR payload length says {declared} bytes, got {actual}")]
    LengthMismatch {
        /// Header length.
        declared: usize,
        /// Actual bytes following the header.
        actual: usize,
    },
    /// A length-prefixed field ended before its declared length.
    #[error("truncated MSTR payload")]
    TruncatedPayload,
    /// A topic was not UTF-8.
    #[error("segment topic is not valid UTF-8")]
    InvalidUtf8,
    /// A topic exceeded the frozen v1 limit.
    #[error("segment topic is {actual} bytes; maximum is {max}")]
    TopicTooLong {
        /// Actual bytes.
        actual: usize,
        /// Fixed maximum.
        max: usize,
    },
    /// An ordinary id exceeded the frozen v1 limit.
    #[error("ordinary message id is {actual} bytes; maximum is {max}")]
    OrdinaryIdTooLong {
        /// Actual bytes.
        actual: usize,
        /// Fixed maximum.
        max: usize,
    },
    /// The ordinary blob is not a `MessageIdData` protobuf.
    #[error("ordinary message id is not valid MessageIdData")]
    InvalidOrdinaryId,
    /// Decoding and canonical re-encoding changed the ordinary blob.
    #[error("ordinary message id protobuf is not canonical")]
    NonCanonicalOrdinaryId,
    /// A decoded ordinary id contains a field combination Magnetar cannot use.
    #[error("ordinary message id has impossible {field} value {value}")]
    ImpossibleOrdinaryId {
        /// Invalid field.
        field: &'static str,
        /// Invalid signed value.
        value: i64,
    },
    /// First-chunk pointers cannot recursively contain another pointer.
    #[error("ordinary message id has nested first-chunk pointers")]
    NestedChunkId,
    /// A source URI and explicit segment id are not a canonical pair.
    #[error(transparent)]
    SegmentTopic(#[from] SegmentTopicError),
    /// Position vectors are explicitly bounded.
    #[error("position vector has {actual} components; maximum is {max}")]
    TooManyComponents {
        /// Actual count.
        actual: usize,
        /// Fixed maximum.
        max: usize,
    },
    /// Components must be strictly ordered by `(segment id, topic bytes)`.
    #[error("position vector components are not in canonical order")]
    NonCanonicalComponentOrder,
    /// A constructed vector repeated a source.
    #[error("position vector repeats source {topic:?}")]
    DuplicateComponent {
        /// Repeated canonical topic.
        topic: String,
    },
    /// A Rust-side field cannot fit the frozen big-endian `u32` length.
    #[error("stream position field length exceeds u32")]
    LengthOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CanonicalMessageId {
    projected: MessageId,
    encoded: Vec<u8>,
}

impl CanonicalMessageId {
    fn from_message_id(message_id: MessageId) -> Result<Self, StreamPositionError> {
        let decoded = message_id.to_pb();
        validate_ordinary_id(&decoded, false)?;
        Ok(Self {
            projected: message_id,
            encoded: encode_canonical_ordinary_id(&decoded)?,
        })
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, StreamPositionError> {
        if bytes.len() > MAX_ORDINARY_MESSAGE_ID_SIZE {
            return Err(StreamPositionError::OrdinaryIdTooLong {
                actual: bytes.len(),
                max: MAX_ORDINARY_MESSAGE_ID_SIZE,
            });
        }
        let decoded =
            pb::MessageIdData::decode(bytes).map_err(|_| StreamPositionError::InvalidOrdinaryId)?;
        validate_ordinary_id(&decoded, false)?;
        let encoded = encode_canonical_ordinary_id(&decoded)?;
        if encoded != bytes {
            return Err(StreamPositionError::NonCanonicalOrdinaryId);
        }
        Ok(Self {
            projected: MessageId::from_pb(&decoded),
            encoded,
        })
    }
}

fn validate_ordinary_id(
    message_id: &pb::MessageIdData,
    nested: bool,
) -> Result<(), StreamPositionError> {
    for (field, value) in [
        ("partition", message_id.partition.unwrap_or(-1)),
        ("batch_index", message_id.batch_index.unwrap_or(-1)),
        ("batch_size", message_id.batch_size.unwrap_or(-1)),
    ] {
        if value < -1 {
            return Err(StreamPositionError::ImpossibleOrdinaryId {
                field,
                value: i64::from(value),
            });
        }
    }
    if let Some(first_chunk) = message_id.first_chunk_message_id.as_deref() {
        if nested || first_chunk.first_chunk_message_id.is_some() {
            return Err(StreamPositionError::NestedChunkId);
        }
        validate_ordinary_id(first_chunk, true)?;
    }
    Ok(())
}

// Freeze the protobuf subset embedded in MSTR v1 instead of delegating its
// canonical byte shape to a particular prost release. Fields are emitted once,
// in tag order, with proto2's unpacked representation for `ack_set`.
fn encode_canonical_ordinary_id(
    message_id: &pb::MessageIdData,
) -> Result<Vec<u8>, StreamPositionError> {
    let encoded_len = canonical_ordinary_id_len(message_id)?;
    let mut encoded = Vec::with_capacity(encoded_len);
    encode_canonical_ordinary_id_into(&mut encoded, message_id)?;
    Ok(encoded)
}

fn encode_canonical_ordinary_id_into(
    encoded: &mut Vec<u8>,
    message_id: &pb::MessageIdData,
) -> Result<(), StreamPositionError> {
    encode_varint_field(encoded, 1, message_id.ledger_id);
    encode_varint_field(encoded, 2, message_id.entry_id);
    if let Some(partition) = message_id.partition {
        encode_varint_field(encoded, 3, signed_i32_varint(partition));
    }
    if let Some(batch_index) = message_id.batch_index {
        encode_varint_field(encoded, 4, signed_i32_varint(batch_index));
    }
    for ack_word in &message_id.ack_set {
        encode_varint_field(encoded, 5, *ack_word as u64);
    }
    if let Some(batch_size) = message_id.batch_size {
        encode_varint_field(encoded, 6, signed_i32_varint(batch_size));
    }
    if let Some(first_chunk) = message_id.first_chunk_message_id.as_deref() {
        let nested_len = canonical_ordinary_id_len(first_chunk)?;
        encode_varint(encoded, (7 << 3) | 2);
        encode_varint(
            encoded,
            u64::try_from(nested_len).map_err(|_| StreamPositionError::LengthOverflow)?,
        );
        encode_canonical_ordinary_id_into(encoded, first_chunk)?;
    }
    Ok(())
}

fn canonical_ordinary_id_len(message_id: &pb::MessageIdData) -> Result<usize, StreamPositionError> {
    let mut length = varint_field_len(1, message_id.ledger_id)
        .checked_add(varint_field_len(2, message_id.entry_id))
        .ok_or(StreamPositionError::LengthOverflow)?;
    for field_len in [
        message_id
            .partition
            .map(|value| varint_field_len(3, signed_i32_varint(value))),
        message_id
            .batch_index
            .map(|value| varint_field_len(4, signed_i32_varint(value))),
        message_id
            .batch_size
            .map(|value| varint_field_len(6, signed_i32_varint(value))),
    ]
    .into_iter()
    .flatten()
    {
        length = checked_ordinary_id_len_add(length, field_len)?;
    }
    for ack_word in &message_id.ack_set {
        length = checked_ordinary_id_len_add(length, varint_field_len(5, *ack_word as u64))?;
    }
    if let Some(first_chunk) = message_id.first_chunk_message_id.as_deref() {
        let nested_len = canonical_ordinary_id_len(first_chunk)?;
        let field_len = varint_len((7 << 3) | 2)
            .checked_add(varint_len(
                u64::try_from(nested_len).map_err(|_| StreamPositionError::LengthOverflow)?,
            ))
            .and_then(|length| length.checked_add(nested_len))
            .ok_or(StreamPositionError::LengthOverflow)?;
        length = checked_ordinary_id_len_add(length, field_len)?;
    }
    Ok(length)
}

fn checked_ordinary_id_len_add(
    length: usize,
    additional: usize,
) -> Result<usize, StreamPositionError> {
    let actual = length
        .checked_add(additional)
        .ok_or(StreamPositionError::LengthOverflow)?;
    if actual > MAX_ORDINARY_MESSAGE_ID_SIZE {
        return Err(StreamPositionError::OrdinaryIdTooLong {
            actual,
            max: MAX_ORDINARY_MESSAGE_ID_SIZE,
        });
    }
    Ok(actual)
}

fn varint_field_len(field_number: u64, value: u64) -> usize {
    varint_len(field_number << 3) + varint_len(value)
}

fn varint_len(mut value: u64) -> usize {
    let mut length = 1;
    while value >= 0x80 {
        length += 1;
        value >>= 7;
    }
    length
}

fn signed_i32_varint(value: i32) -> u64 {
    i64::from(value) as u64
}

fn encode_varint_field(target: &mut Vec<u8>, field_number: u64, value: u64) {
    encode_varint(target, field_number << 3);
    encode_varint(target, value);
}

fn encode_varint(target: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        target.push((value as u8) | 0x80);
        value >>= 7;
    }
    target.push(value as u8);
}

/// One source-qualified ordinary Pulsar message id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMessageId {
    source: SegmentSource,
    ordinary: CanonicalMessageId,
}

impl StreamMessageId {
    /// Construct a source-qualified value from an ordinary in-memory id.
    ///
    /// # Errors
    ///
    /// Returns [`StreamPositionError`] if a frozen v1 size or ordinary-id
    /// invariant is violated.
    pub fn new(source: SegmentSource, message_id: MessageId) -> Result<Self, StreamPositionError> {
        validate_source_size(&source)?;
        Ok(Self {
            source,
            ordinary: CanonicalMessageId::from_message_id(message_id)?,
        })
    }

    /// Construct from a complete in-memory ordinary protobuf id while
    /// canonicalizing its frozen byte representation.
    ///
    /// Unlike [`Self::new`], this retains `ack_set` and
    /// `first_chunk_message_id`.
    pub fn from_message_id_data(
        source: SegmentSource,
        message_id: &pb::MessageIdData,
    ) -> Result<Self, StreamPositionError> {
        validate_source_size(&source)?;
        validate_ordinary_id(message_id, false)?;
        Ok(Self {
            source,
            ordinary: CanonicalMessageId {
                projected: MessageId::from_pb(message_id),
                encoded: encode_canonical_ordinary_id(message_id)?,
            },
        })
    }

    /// Decode the retained canonical ordinary protobuf id.
    pub fn ordinary_message_id_data(&self) -> Result<pb::MessageIdData, StreamPositionError> {
        pb::MessageIdData::decode(self.ordinary.encoded.as_slice())
            .map_err(|_| StreamPositionError::InvalidOrdinaryId)
    }

    /// Construct while retaining all canonical ordinary protobuf fields,
    /// including ack sets and first-chunk pointers.
    ///
    /// # Errors
    ///
    /// Returns [`StreamPositionError`] for non-canonical or impossible ordinary
    /// protobuf bytes.
    pub fn from_ordinary_bytes(
        source: SegmentSource,
        ordinary_bytes: &[u8],
    ) -> Result<Self, StreamPositionError> {
        validate_source_size(&source)?;
        Ok(Self {
            source,
            ordinary: CanonicalMessageId::from_bytes(ordinary_bytes)?,
        })
    }

    /// Segment source.
    #[must_use]
    pub const fn source(&self) -> &SegmentSource {
        &self.source
    }

    /// Ordinary in-memory id projection.
    #[must_use]
    pub const fn ordinary_message_id(&self) -> MessageId {
        self.ordinary.projected
    }

    /// Canonical ordinary `MessageIdData` bytes retained by this value.
    #[must_use]
    pub fn ordinary_message_id_bytes(&self) -> &[u8] {
        &self.ordinary.encoded
    }

    /// Exact encoded `MSTR` envelope length, validated against the v1 bound.
    ///
    /// # Errors
    ///
    /// Returns [`StreamPositionError::EnvelopeTooLarge`] before allocating a
    /// payload buffer when the value cannot fit v1.
    pub fn encoded_len(&self) -> Result<usize, StreamPositionError> {
        checked_envelope_len(component_encoded_len(&self.source, &self.ordinary)?)
    }

    /// Encode the exact frozen `MSTR` v1 stream-message-id envelope.
    ///
    /// # Errors
    ///
    /// Returns [`StreamPositionError`] if a length cannot fit v1.
    pub fn to_bytes(&self) -> Result<Vec<u8>, StreamPositionError> {
        let payload_len = component_encoded_len(&self.source, &self.ordinary)?;
        checked_envelope_len(payload_len)?;
        let mut payload = Vec::with_capacity(payload_len);
        write_component(&mut payload, &self.source, &self.ordinary)?;
        encode_envelope(STREAM_MESSAGE_ID_KIND, payload)
    }

    /// Decode one exact frozen `MSTR` v1 stream-message-id envelope.
    ///
    /// # Errors
    ///
    /// Rejects every non-canonical shape listed by [`StreamPositionError`].
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, StreamPositionError> {
        let payload = decode_envelope(bytes, STREAM_MESSAGE_ID_KIND)?;
        let mut cursor = Cursor::new(payload);
        let (source, ordinary) = read_component(&mut cursor)?;
        cursor.finish()?;
        Ok(Self { source, ordinary })
    }
}

/// Immutable delivered-position snapshot across segment sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionVector {
    layout_epoch: u64,
    components: BTreeMap<SegmentSource, CanonicalMessageId>,
}

impl PositionVector {
    /// Construct and normalize a vector from source-qualified ordinary ids.
    ///
    /// # Errors
    ///
    /// Returns [`StreamPositionError`] for duplicate sources, size limits, or
    /// invalid ordinary ids.
    pub fn new(
        layout_epoch: u64,
        components: impl IntoIterator<Item = (SegmentSource, MessageId)>,
    ) -> Result<Self, StreamPositionError> {
        let mut canonical = BTreeMap::new();
        for (source, message_id) in components {
            validate_source_size(&source)?;
            if canonical
                .insert(
                    source.clone(),
                    CanonicalMessageId::from_message_id(message_id)?,
                )
                .is_some()
            {
                return Err(StreamPositionError::DuplicateComponent {
                    topic: source.topic().to_owned(),
                });
            }
            if canonical.len() > MAX_POSITION_COMPONENTS {
                return Err(StreamPositionError::TooManyComponents {
                    actual: canonical.len(),
                    max: MAX_POSITION_COMPONENTS,
                });
            }
        }
        Ok(Self {
            layout_epoch,
            components: canonical,
        })
    }

    pub(crate) fn from_canonical(
        layout_epoch: u64,
        components: &BTreeMap<SegmentSource, StreamMessageId>,
    ) -> Result<Self, StreamPositionError> {
        let components = components
            .iter()
            .map(|(source, message_id)| (source.clone(), message_id.ordinary.clone()))
            .collect();
        Ok(Self {
            layout_epoch,
            components,
        })
    }

    /// Originating layout epoch.
    #[must_use]
    pub const fn layout_epoch(&self) -> u64 {
        self.layout_epoch
    }

    /// Number of represented sources.
    #[must_use]
    pub fn len(&self) -> usize {
        self.components.len()
    }

    /// Whether no source has delivered a position yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.components.is_empty()
    }

    /// Ordinary position for one exact source.
    #[must_use]
    pub fn get(&self, source: &SegmentSource) -> Option<MessageId> {
        self.components.get(source).map(|id| id.projected)
    }

    /// Exact canonical ordinary-id bytes retained for one source.
    #[must_use]
    pub fn ordinary_message_id_bytes(&self, source: &SegmentSource) -> Option<&[u8]> {
        self.components.get(source).map(|id| id.encoded.as_slice())
    }

    /// Canonically ordered source and ordinary-id projections.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&SegmentSource, MessageId)> {
        self.components
            .iter()
            .map(|(source, id)| (source, id.projected))
    }

    pub(crate) fn stream_message_ids(&self) -> impl ExactSizeIterator<Item = StreamMessageId> + '_ {
        self.components
            .iter()
            .map(|(source, ordinary)| StreamMessageId {
                source: source.clone(),
                ordinary: ordinary.clone(),
            })
    }

    /// Exact encoded `MSTR` envelope length, validated before payload allocation.
    ///
    /// # Errors
    ///
    /// Returns [`StreamPositionError::EnvelopeTooLarge`] when the represented
    /// components cannot fit the frozen 1 MiB envelope.
    pub fn encoded_len(&self) -> Result<usize, StreamPositionError> {
        checked_envelope_len(self.payload_encoded_len()?)
    }

    /// Encode the exact frozen `MSTR` v1 position-vector envelope.
    ///
    /// # Errors
    ///
    /// Returns [`StreamPositionError`] if a length cannot fit v1.
    pub fn to_bytes(&self) -> Result<Vec<u8>, StreamPositionError> {
        let payload_len = self.payload_encoded_len()?;
        checked_envelope_len(payload_len)?;
        let mut payload = Vec::with_capacity(payload_len);
        payload.extend_from_slice(&self.layout_epoch.to_be_bytes());
        write_u32(&mut payload, self.components.len())?;
        for (source, ordinary) in &self.components {
            write_component(&mut payload, source, ordinary)?;
        }
        encode_envelope(POSITION_VECTOR_KIND, payload)
    }

    fn payload_encoded_len(&self) -> Result<usize, StreamPositionError> {
        self.components
            .iter()
            .try_fold(12usize, |length, (source, ordinary)| {
                length
                    .checked_add(component_encoded_len(source, ordinary)?)
                    .ok_or(StreamPositionError::LengthOverflow)
            })
    }

    /// Decode one exact frozen `MSTR` v1 position-vector envelope.
    ///
    /// # Errors
    ///
    /// Rejects unsorted or duplicate components and every non-canonical field.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, StreamPositionError> {
        let payload = decode_envelope(bytes, POSITION_VECTOR_KIND)?;
        let mut cursor = Cursor::new(payload);
        let layout_epoch = cursor.read_u64()?;
        let count = cursor.read_u32()? as usize;
        if count > MAX_POSITION_COMPONENTS {
            return Err(StreamPositionError::TooManyComponents {
                actual: count,
                max: MAX_POSITION_COMPONENTS,
            });
        }
        let mut components = BTreeMap::new();
        let mut previous: Option<SegmentSource> = None;
        for _ in 0..count {
            let (source, ordinary) = read_component(&mut cursor)?;
            if previous
                .as_ref()
                .is_some_and(|previous| source <= *previous)
            {
                return Err(StreamPositionError::NonCanonicalComponentOrder);
            }
            previous = Some(source.clone());
            components.insert(source, ordinary);
        }
        cursor.finish()?;
        Ok(Self {
            layout_epoch,
            components,
        })
    }
}

fn validate_source_size(source: &SegmentSource) -> Result<(), StreamPositionError> {
    if source.topic().len() > MAX_POSITION_TOPIC_SIZE {
        return Err(StreamPositionError::TopicTooLong {
            actual: source.topic().len(),
            max: MAX_POSITION_TOPIC_SIZE,
        });
    }
    Ok(())
}

fn write_component(
    target: &mut Vec<u8>,
    source: &SegmentSource,
    ordinary: &CanonicalMessageId,
) -> Result<(), StreamPositionError> {
    validate_source_size(source)?;
    target.extend_from_slice(&source.segment_id().0.to_be_bytes());
    write_bytes(target, source.topic().as_bytes())?;
    write_bytes(target, &ordinary.encoded)?;
    Ok(())
}

fn component_encoded_len(
    source: &SegmentSource,
    ordinary: &CanonicalMessageId,
) -> Result<usize, StreamPositionError> {
    validate_source_size(source)?;
    8usize
        .checked_add(4)
        .and_then(|length| length.checked_add(source.topic().len()))
        .and_then(|length| length.checked_add(4))
        .and_then(|length| length.checked_add(ordinary.encoded.len()))
        .ok_or(StreamPositionError::LengthOverflow)
}

fn read_component(
    cursor: &mut Cursor<'_>,
) -> Result<(SegmentSource, CanonicalMessageId), StreamPositionError> {
    let segment_id = crate::SegmentId(cursor.read_u64()?);
    let topic_bytes = cursor.read_sized(MAX_POSITION_TOPIC_SIZE, true)?;
    let topic = core::str::from_utf8(topic_bytes)
        .map_err(|_| StreamPositionError::InvalidUtf8)?
        .to_owned();
    let source = SegmentSource::new(segment_id, topic)?;
    let ordinary_bytes = cursor.read_sized(MAX_ORDINARY_MESSAGE_ID_SIZE, false)?;
    let ordinary = CanonicalMessageId::from_bytes(ordinary_bytes)?;
    Ok((source, ordinary))
}

fn write_u32(target: &mut Vec<u8>, value: usize) -> Result<(), StreamPositionError> {
    let value = u32::try_from(value).map_err(|_| StreamPositionError::LengthOverflow)?;
    target.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn write_bytes(target: &mut Vec<u8>, value: &[u8]) -> Result<(), StreamPositionError> {
    write_u32(target, value.len())?;
    target.extend_from_slice(value);
    Ok(())
}

fn encode_envelope(kind: u8, payload: Vec<u8>) -> Result<Vec<u8>, StreamPositionError> {
    let total = checked_envelope_len(payload.len())?;
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| StreamPositionError::LengthOverflow)?;
    let mut encoded = Vec::with_capacity(total);
    encoded.extend_from_slice(MAGIC);
    encoded.push(VERSION);
    encoded.push(kind);
    encoded.extend_from_slice(&0u16.to_be_bytes());
    encoded.extend_from_slice(&payload_len.to_be_bytes());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn checked_envelope_len(payload_len: usize) -> Result<usize, StreamPositionError> {
    let total = HEADER_LEN
        .checked_add(payload_len)
        .ok_or(StreamPositionError::LengthOverflow)?;
    if total > MAX_STREAM_POSITION_SIZE {
        return Err(StreamPositionError::EnvelopeTooLarge {
            actual: total,
            max: MAX_STREAM_POSITION_SIZE,
        });
    }
    Ok(total)
}

fn decode_envelope(bytes: &[u8], expected_kind: u8) -> Result<&[u8], StreamPositionError> {
    if bytes.len() > MAX_STREAM_POSITION_SIZE {
        return Err(StreamPositionError::EnvelopeTooLarge {
            actual: bytes.len(),
            max: MAX_STREAM_POSITION_SIZE,
        });
    }
    let Some(header) = bytes.get(..HEADER_LEN) else {
        return Err(StreamPositionError::TruncatedHeader);
    };
    if header.get(..4) != Some(MAGIC.as_slice()) {
        return Err(StreamPositionError::InvalidMagic);
    }
    let version = header[4];
    if version != VERSION {
        return Err(StreamPositionError::UnsupportedVersion(version));
    }
    let kind = header[5];
    if kind != expected_kind {
        return Err(StreamPositionError::UnexpectedKind {
            got: kind,
            expected: expected_kind,
        });
    }
    let flags = u16::from_be_bytes([header[6], header[7]]);
    if flags != 0 {
        return Err(StreamPositionError::UnsupportedFlags(flags));
    }
    let declared = u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as usize;
    let actual = bytes.len() - HEADER_LEN;
    if declared != actual {
        return Err(StreamPositionError::LengthMismatch { declared, actual });
    }
    bytes
        .get(HEADER_LEN..)
        .ok_or(StreamPositionError::TruncatedPayload)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u32(&mut self) -> Result<u32, StreamPositionError> {
        let bytes = self.take(4)?;
        let array: [u8; 4] = bytes
            .try_into()
            .map_err(|_| StreamPositionError::TruncatedPayload)?;
        Ok(u32::from_be_bytes(array))
    }

    fn read_u64(&mut self) -> Result<u64, StreamPositionError> {
        let bytes = self.take(8)?;
        let array: [u8; 8] = bytes
            .try_into()
            .map_err(|_| StreamPositionError::TruncatedPayload)?;
        Ok(u64::from_be_bytes(array))
    }

    fn read_sized(&mut self, maximum: usize, topic: bool) -> Result<&'a [u8], StreamPositionError> {
        let length = self.read_u32()? as usize;
        if length > maximum {
            return Err(if topic {
                StreamPositionError::TopicTooLong {
                    actual: length,
                    max: maximum,
                }
            } else {
                StreamPositionError::OrdinaryIdTooLong {
                    actual: length,
                    max: maximum,
                }
            });
        }
        self.take(length)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], StreamPositionError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(StreamPositionError::TruncatedPayload)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(StreamPositionError::TruncatedPayload)?;
        self.offset = end;
        Ok(value)
    }

    fn finish(self) -> Result<(), StreamPositionError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(StreamPositionError::LengthMismatch {
                declared: self.offset,
                actual: self.bytes.len(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::scalable_consumer::canonical_segment_topic;
    use crate::{KeyRange, SegmentId};

    fn source(id: u64, start: u32, end: u32) -> SegmentSource {
        let range = KeyRange::new(start, end).expect("valid test range");
        let topic =
            canonical_segment_topic("topic://t/n/x", range, SegmentId(id)).expect("valid parent");
        SegmentSource::new(SegmentId(id), topic).expect("canonical source")
    }

    fn message_id(entry_id: u64) -> MessageId {
        MessageId {
            ledger_id: 1,
            entry_id,
            partition: 0,
            batch_index: 0,
            batch_size: 1,
        }
    }

    #[test]
    fn stream_message_id_matches_frozen_golden_vector() {
        let value =
            StreamMessageId::new(source(7, 0, 65_535), message_id(2)).expect("valid stream id");
        let expected = vec![
            0x4d, 0x53, 0x54, 0x52, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x35, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x1b, b's', b'e', b'g', b'm',
            b'e', b'n', b't', b':', b'/', b'/', b't', b'/', b'n', b'/', b'x', b'/', b'0', b'0',
            b'0', b'0', b'-', b'f', b'f', b'f', b'f', b'-', b'7', 0x00, 0x00, 0x00, 0x0a, 0x08,
            0x01, 0x10, 0x02, 0x18, 0x00, 0x20, 0x00, 0x30, 0x01,
        ];
        assert_eq!(value.to_bytes().expect("encode"), expected);
        assert_eq!(StreamMessageId::from_bytes(&expected), Ok(value));
    }

    #[test]
    fn position_vector_matches_frozen_golden_vector() {
        let value =
            PositionVector::new(9, [(source(7, 0, 65_535), message_id(2))]).expect("valid vector");
        let expected = vec![
            0x4d, 0x53, 0x54, 0x52, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x41, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x1b, b's', b'e', b'g', b'm', b'e', b'n',
            b't', b':', b'/', b'/', b't', b'/', b'n', b'/', b'x', b'/', b'0', b'0', b'0', b'0',
            b'-', b'f', b'f', b'f', b'f', b'-', b'7', 0x00, 0x00, 0x00, 0x0a, 0x08, 0x01, 0x10,
            0x02, 0x18, 0x00, 0x20, 0x00, 0x30, 0x01,
        ];
        assert_eq!(value.to_bytes().expect("encode"), expected);
        assert_eq!(PositionVector::from_bytes(&expected), Ok(value));
    }

    #[test]
    fn canonical_ordinary_blob_preserves_chunk_and_ack_fields() {
        let ordinary = pb::MessageIdData {
            ledger_id: 10,
            entry_id: 20,
            partition: Some(0),
            batch_index: Some(1),
            ack_set: vec![3, 5],
            batch_size: Some(2),
            first_chunk_message_id: Some(Box::new(pb::MessageIdData {
                ledger_id: 10,
                entry_id: 18,
                partition: Some(0),
                batch_index: Some(-1),
                ack_set: Vec::new(),
                batch_size: None,
                first_chunk_message_id: None,
            })),
        }
        .encode_to_vec();
        let value = StreamMessageId::from_ordinary_bytes(source(1, 0, 65_535), &ordinary)
            .expect("canonical chunk id");
        let decoded =
            StreamMessageId::from_bytes(&value.to_bytes().expect("encode")).expect("decode");
        assert_eq!(decoded.ordinary_message_id_bytes(), ordinary);
    }

    #[test]
    fn in_memory_ordinary_id_cannot_exceed_the_frozen_bound() {
        let ordinary = pb::MessageIdData {
            ledger_id: 10,
            entry_id: 20,
            ack_set: vec![-1; MAX_ORDINARY_MESSAGE_ID_SIZE / 10],
            ..Default::default()
        };
        assert!(matches!(
            StreamMessageId::from_message_id_data(source(1, 0, 65_535), &ordinary),
            Err(StreamPositionError::OrdinaryIdTooLong {
                actual,
                max: MAX_ORDINARY_MESSAGE_ID_SIZE,
            }) if actual > MAX_ORDINARY_MESSAGE_ID_SIZE
        ));
    }

    #[test]
    fn wire_equivalent_noncanonical_ordinary_ids_are_rejected() {
        let source = source(1, 0, 65_535);
        let reordered = [
            0x10, 0x02, // entry id before ledger id
            0x08, 0x01, 0x18, 0x00, 0x20, 0x00, 0x30, 0x01,
        ];
        assert_eq!(
            StreamMessageId::from_ordinary_bytes(source.clone(), &reordered),
            Err(StreamPositionError::NonCanonicalOrdinaryId)
        );

        let duplicate_partition = [
            0x08, 0x01, 0x10, 0x02, 0x18, 0x00, 0x18, 0x00, 0x20, 0x00, 0x30, 0x01,
        ];
        assert_eq!(
            StreamMessageId::from_ordinary_bytes(source.clone(), &duplicate_partition),
            Err(StreamPositionError::NonCanonicalOrdinaryId)
        );

        let packed_ack_set = [0x08, 0x01, 0x10, 0x02, 0x2a, 0x02, 0x03, 0x05];
        assert_eq!(
            StreamMessageId::from_ordinary_bytes(source.clone(), &packed_ack_set),
            Err(StreamPositionError::NonCanonicalOrdinaryId)
        );

        let unknown_field = [0x08, 0x01, 0x10, 0x02, 0x40, 0x00];
        assert_eq!(
            StreamMessageId::from_ordinary_bytes(source, &unknown_field),
            Err(StreamPositionError::NonCanonicalOrdinaryId)
        );
    }

    #[test]
    fn envelope_header_rejections_are_strict() {
        let valid = StreamMessageId::new(source(1, 0, 65_535), message_id(1))
            .expect("value")
            .to_bytes()
            .expect("encode");
        assert_eq!(
            StreamMessageId::from_bytes(&valid[..4]),
            Err(StreamPositionError::TruncatedHeader)
        );

        let mut changed = valid.clone();
        changed[0] = b'X';
        assert_eq!(
            StreamMessageId::from_bytes(&changed),
            Err(StreamPositionError::InvalidMagic)
        );
        changed = valid.clone();
        changed[4] = 2;
        assert_eq!(
            StreamMessageId::from_bytes(&changed),
            Err(StreamPositionError::UnsupportedVersion(2))
        );
        changed = valid.clone();
        changed[5] = POSITION_VECTOR_KIND;
        assert_eq!(
            StreamMessageId::from_bytes(&changed),
            Err(StreamPositionError::UnexpectedKind {
                got: POSITION_VECTOR_KIND,
                expected: STREAM_MESSAGE_ID_KIND,
            })
        );
        changed = valid.clone();
        changed[7] = 1;
        assert_eq!(
            StreamMessageId::from_bytes(&changed),
            Err(StreamPositionError::UnsupportedFlags(1))
        );
        changed = valid.clone();
        changed.push(0);
        assert!(matches!(
            StreamMessageId::from_bytes(&changed),
            Err(StreamPositionError::LengthMismatch { .. })
        ));
    }

    #[test]
    fn vector_rejects_noncanonical_order_and_duplicates() {
        let vector = PositionVector::new(
            1,
            [
                (source(1, 0, 32_767), message_id(1)),
                (source(2, 32_768, 65_535), message_id(2)),
            ],
        )
        .expect("vector");
        let encoded = vector.to_bytes().expect("encode");
        let payload = decode_envelope(&encoded, POSITION_VECTOR_KIND).expect("payload");
        let mut cursor = Cursor::new(payload);
        let epoch = cursor.take(8).expect("epoch").to_vec();
        let count = cursor.take(4).expect("count").to_vec();
        let first_start = cursor.offset;
        let _ = read_component(&mut cursor).expect("first");
        let first = payload[first_start..cursor.offset].to_vec();
        let second = payload[cursor.offset..].to_vec();

        let mut reversed_payload = epoch.clone();
        reversed_payload.extend_from_slice(&count);
        reversed_payload.extend_from_slice(&second);
        reversed_payload.extend_from_slice(&first);
        let reversed = encode_envelope(POSITION_VECTOR_KIND, reversed_payload).expect("encode");
        assert_eq!(
            PositionVector::from_bytes(&reversed),
            Err(StreamPositionError::NonCanonicalComponentOrder)
        );

        let mut duplicate_payload = epoch;
        duplicate_payload.extend_from_slice(&count);
        duplicate_payload.extend_from_slice(&first);
        duplicate_payload.extend_from_slice(&first);
        let duplicate = encode_envelope(POSITION_VECTOR_KIND, duplicate_payload).expect("encode");
        assert_eq!(
            PositionVector::from_bytes(&duplicate),
            Err(StreamPositionError::NonCanonicalComponentOrder)
        );
    }

    #[test]
    fn ordinary_and_topic_rejections_are_strict() {
        let invalid = pb::MessageIdData {
            ledger_id: 1,
            entry_id: 1,
            partition: Some(-2),
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(
            StreamMessageId::from_ordinary_bytes(source(1, 0, 65_535), &invalid),
            Err(StreamPositionError::ImpossibleOrdinaryId {
                field: "partition",
                value: -2,
            })
        );
        assert_eq!(
            StreamMessageId::from_ordinary_bytes(source(1, 0, 65_535), &[]),
            Err(StreamPositionError::NonCanonicalOrdinaryId)
        );
        assert!(
            SegmentSource::new(SegmentId(1), "segment://t/n/x/0000-FFFF-1".to_owned()).is_err()
        );
    }

    #[test]
    fn frozen_size_and_utf8_limits_are_enforced_before_payload_reads() {
        let oversized_envelope = vec![0; MAX_STREAM_POSITION_SIZE + 1];
        assert_eq!(
            StreamMessageId::from_bytes(&oversized_envelope),
            Err(StreamPositionError::EnvelopeTooLarge {
                actual: MAX_STREAM_POSITION_SIZE + 1,
                max: MAX_STREAM_POSITION_SIZE,
            })
        );
        assert_eq!(
            encode_envelope(STREAM_MESSAGE_ID_KIND, vec![0; MAX_STREAM_POSITION_SIZE]),
            Err(StreamPositionError::EnvelopeTooLarge {
                actual: MAX_STREAM_POSITION_SIZE + HEADER_LEN,
                max: MAX_STREAM_POSITION_SIZE,
            })
        );

        let mut invalid_utf8 = SegmentId(1).0.to_be_bytes().to_vec();
        write_u32(&mut invalid_utf8, 1).expect("topic length");
        invalid_utf8.push(0xff);
        write_u32(&mut invalid_utf8, 0).expect("ordinary length");
        let invalid_utf8 = encode_envelope(STREAM_MESSAGE_ID_KIND, invalid_utf8).expect("envelope");
        assert_eq!(
            StreamMessageId::from_bytes(&invalid_utf8),
            Err(StreamPositionError::InvalidUtf8)
        );

        let mut oversized_topic = SegmentId(1).0.to_be_bytes().to_vec();
        write_u32(&mut oversized_topic, MAX_POSITION_TOPIC_SIZE + 1).expect("topic length");
        let oversized_topic =
            encode_envelope(STREAM_MESSAGE_ID_KIND, oversized_topic).expect("envelope");
        assert_eq!(
            StreamMessageId::from_bytes(&oversized_topic),
            Err(StreamPositionError::TopicTooLong {
                actual: MAX_POSITION_TOPIC_SIZE + 1,
                max: MAX_POSITION_TOPIC_SIZE,
            })
        );

        let source = source(1, 0, 65_535);
        let mut oversized_ordinary = SegmentId(1).0.to_be_bytes().to_vec();
        write_bytes(&mut oversized_ordinary, source.topic().as_bytes()).expect("topic");
        write_u32(&mut oversized_ordinary, MAX_ORDINARY_MESSAGE_ID_SIZE + 1)
            .expect("ordinary length");
        let oversized_ordinary =
            encode_envelope(STREAM_MESSAGE_ID_KIND, oversized_ordinary).expect("envelope");
        assert_eq!(
            StreamMessageId::from_bytes(&oversized_ordinary),
            Err(StreamPositionError::OrdinaryIdTooLong {
                actual: MAX_ORDINARY_MESSAGE_ID_SIZE + 1,
                max: MAX_ORDINARY_MESSAGE_ID_SIZE,
            })
        );
        assert_eq!(
            StreamMessageId::from_ordinary_bytes(
                source,
                &vec![0; MAX_ORDINARY_MESSAGE_ID_SIZE + 1],
            ),
            Err(StreamPositionError::OrdinaryIdTooLong {
                actual: MAX_ORDINARY_MESSAGE_ID_SIZE + 1,
                max: MAX_ORDINARY_MESSAGE_ID_SIZE,
            })
        );
    }

    #[test]
    fn vector_component_limit_is_checked_before_component_decode() {
        let mut payload = 1u64.to_be_bytes().to_vec();
        write_u32(&mut payload, MAX_POSITION_COMPONENTS + 1).expect("component count");
        let encoded = encode_envelope(POSITION_VECTOR_KIND, payload).expect("envelope");
        assert_eq!(
            PositionVector::from_bytes(&encoded),
            Err(StreamPositionError::TooManyComponents {
                actual: MAX_POSITION_COMPONENTS + 1,
                max: MAX_POSITION_COMPONENTS,
            })
        );

        let parent = format!("topic://t/n/{}", "x".repeat(MAX_POSITION_TOPIC_SIZE));
        let topic = canonical_segment_topic(&parent, KeyRange::FULL, SegmentId(1))
            .expect("canonical long source");
        let source = SegmentSource::new(SegmentId(1), topic).expect("source");
        assert!(matches!(
            StreamMessageId::new(source, message_id(1)),
            Err(StreamPositionError::TopicTooLong { .. })
        ));
    }

    #[test]
    fn oversized_vector_is_rejected_by_length_preflight_before_encoding() {
        let local_name = "x".repeat(4_000);
        let parent = format!("topic://t/n/{local_name}");
        let components = (0..300u64).map(|id| {
            let topic = canonical_segment_topic(&parent, KeyRange::FULL, SegmentId(id))
                .expect("canonical source");
            let source = SegmentSource::new(SegmentId(id), topic).expect("source");
            (source, message_id(id))
        });
        let vector = PositionVector::new(1, components).expect("bounded component count");
        let expected = StreamPositionError::EnvelopeTooLarge {
            actual: vector
                .payload_encoded_len()
                .expect("representable payload length")
                + HEADER_LEN,
            max: MAX_STREAM_POSITION_SIZE,
        };
        assert_eq!(vector.encoded_len(), Err(expected.clone()));
        assert_eq!(vector.to_bytes(), Err(expected));
    }

    proptest! {
        #[test]
        fn stream_message_id_roundtrips_without_panicking(
            segment_id in any::<u64>(),
            ledger_id in any::<u64>(),
            entry_id in any::<u64>(),
            partition in -1i32..16,
            batch_index in -1i32..16,
            batch_size in -1i32..32,
        ) {
            let source = source(segment_id, 0, 65_535);
            let value = StreamMessageId::new(source, MessageId {
                ledger_id,
                entry_id,
                partition,
                batch_index,
                batch_size,
            }).expect("generated values are valid");
            let encoded = value.to_bytes().expect("bounded encode");
            prop_assert_eq!(StreamMessageId::from_bytes(&encoded), Ok(value));
        }

        #[test]
        fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..2048)) {
            let _ = StreamMessageId::from_bytes(&bytes);
            let _ = PositionVector::from_bytes(&bytes);
        }
    }
}
