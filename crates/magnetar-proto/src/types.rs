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

/// A logical message identifier (ledger / entry / batch / partition).
///
/// Mirrors the Java `MessageId` interface. `partition` defaults to `-1` for non-partitioned
/// topics; `batch_index` defaults to `-1` for non-batched messages.
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
        let pb = self.to_pb();
        let mut buf = Vec::with_capacity(pb.encoded_len());
        pb.encode(&mut buf)
            .expect("encoding MessageIdData into a fresh Vec cannot fail");
        buf
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
    use super::*;

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
}
