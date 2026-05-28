// SPDX-License-Identifier: Apache-2.0

//! Pulsar wire framing — `BaseCommand` envelopes, optional payload region, CRC32C and
//! broker-entry-metadata magic handling.
//!
//! # Wire format
//!
//! All multi-byte integers are big-endian. The outer `total_size` excludes the four bytes used
//! to encode itself.
//!
//! ## Command-only frame
//!
//! ```text
//! [total_size u32][cmd_size u32][BaseCommand bytes]
//! ```
//!
//! Where `total_size == 4 + cmd_size`.
//!
//! ## Payload-bearing frame (SEND / MESSAGE)
//!
//! ```text
//! [total_size u32][cmd_size u32][BaseCommand]
//!   [0x0e01 u16][crc32c u32]
//!   [metadata_size u32][MessageMetadata][payload bytes]
//! ```
//!
//! The CRC32C (Castagnoli) is computed over the concatenation
//! `[metadata_size u32 BE][metadata bytes][payload bytes]`.
//!
//! ## Broker-entry-metadata envelope (PIP-90)
//!
//! Dispatched messages may carry a `BrokerEntryMetadata` prelude inserted by the broker:
//!
//! ```text
//! [total_size u32][cmd_size u32][BaseCommand]
//!   [0x0e02 u16][bem_size u32][BrokerEntryMetadata]
//!   [0x0e01 u16][crc32c u32][metadata_size u32][MessageMetadata][payload]
//! ```
//!
//! A producer never emits `0x0e02`; consumers must peel it before parsing the standard payload
//! prelude.
//!
//! # References
//!
//! - `org.apache.pulsar.common.protocol.Commands.serializeWithSize` (Commands.java:1866-1885).
//! - `org.apache.pulsar.common.protocol.Commands.serializeCommandSendWithSize`
//!   (Commands.java:1887-1934).
//! - `org.apache.pulsar.common.protocol.Commands.addBrokerEntryMetadata` (Commands.java:1936-1970).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message as _;

use crate::pb;

/// The magic constant for CRC32C-protected payload frames.
pub const MAGIC_CRC32C: u16 = 0x0e01;

/// The magic constant for broker-entry-metadata envelopes (PIP-90).
pub const MAGIC_BROKER_ENTRY_METADATA: u16 = 0x0e02;

/// Maximum frame size we accept on decode. Mirrors the Pulsar default of 5 MiB. Higher layers
/// may enforce a smaller cap; this is the absolute ceiling enforced by `decode_one`.
pub const MAX_FRAME_SIZE: usize = 5 * 1024 * 1024;

pub(crate) const TOTAL_SIZE_LEN: usize = 4;
const CMD_SIZE_LEN: usize = 4;
const MAGIC_LEN: usize = 2;
const CHECKSUM_LEN: usize = 4;
const METADATA_SIZE_LEN: usize = 4;
const BEM_SIZE_LEN: usize = 4;

/// Errors that can occur while encoding or decoding a magnetar frame.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    /// The buffer ran out before a complete frame could be read.
    #[error("frame incomplete: need {needed} more bytes")]
    Incomplete {
        /// Minimum extra bytes the caller must supply before retrying.
        needed: usize,
    },

    /// The frame's outer length is implausible (zero or > [`MAX_FRAME_SIZE`]).
    #[error("frame length out of bounds: {0}")]
    BadLength(u32),

    /// Protobuf decode failed on the inner `BaseCommand`, `MessageMetadata`, or
    /// `BrokerEntryMetadata`.
    #[error("protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),

    /// Protobuf encode failed (e.g. buffer too small or encoding too large).
    #[error("protobuf encode error: {0}")]
    Encode(#[from] prost::EncodeError),

    /// CRC32C verification failed on a payload-bearing frame.
    #[error("crc32c mismatch: computed {computed:08x}, expected {expected:08x}")]
    ChecksumMismatch {
        /// CRC32C we computed over `[metadata_size][metadata][payload]`.
        computed: u32,
        /// CRC32C the peer advertised in the payload prelude.
        expected: u32,
    },

    /// The frame's payload-prelude magic was not `0x0e01`.
    #[error("missing or bad payload-prelude magic: 0x{0:04x}")]
    MissingMagic(u16),
}

/// A decoded magnetar frame: a `BaseCommand` plus an optional payload region.
#[derive(Debug, Clone)]
pub struct Frame {
    /// The `BaseCommand` parsed from the frame head.
    pub command: pb::BaseCommand,
    /// The optional payload region. Present for SEND / MESSAGE frames; absent for control
    /// frames such as PING, PONG, CONNECT.
    pub payload: Option<Payload>,
}

/// The optional payload region attached to SEND / MESSAGE frames.
#[derive(Debug, Clone)]
pub struct Payload {
    /// Optional broker-entry-metadata envelope (PIP-90); present on dispatched messages when
    /// the consumer opted into the matching `FeatureFlag`.
    pub broker_entry_metadata: Option<pb::BrokerEntryMetadata>,
    /// The mandatory message metadata.
    pub metadata: pb::MessageMetadata,
    /// The raw payload bytes (possibly compressed, possibly a batch, possibly encrypted).
    /// Higher layers are responsible for further interpretation.
    pub body: Bytes,
}

/// Encode a payload-less command into `dst`.
///
/// Wire format: `[total_size u32][cmd_size u32][BaseCommand]` (big-endian).
///
/// # Errors
///
/// Returns [`FrameError::Encode`] if the protobuf encoder rejects the command.
pub fn encode_command(dst: &mut BytesMut, cmd: &pb::BaseCommand) -> Result<(), FrameError> {
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

/// Encode a SEND-shaped command with metadata and payload.
///
/// Wire format:
///
/// ```text
/// [total_size u32][cmd_size u32][BaseCommand]
///   [0x0e01 u16][crc32c u32]
///   [metadata_size u32][MessageMetadata][payload]
/// ```
///
/// The CRC32C is computed over `[metadata_size u32 BE][metadata bytes][payload bytes]`.
///
/// # Errors
///
/// Returns [`FrameError::Encode`] if any protobuf encode step fails, or [`FrameError::BadLength`]
/// if the resulting frame would exceed `u32::MAX` bytes.
pub fn encode_payload(
    dst: &mut BytesMut,
    cmd: &pb::BaseCommand,
    metadata: &pb::MessageMetadata,
    payload: &[u8],
) -> Result<(), FrameError> {
    let cmd_size = cmd.encoded_len();
    let meta_size = metadata.encoded_len();
    let payload_len = payload.len();

    // header_content_size = cmd_size_field(4) + cmd + magic(2) + crc(4) + meta_size_field(4) + meta
    let header_content_size = CMD_SIZE_LEN
        .checked_add(cmd_size)
        .and_then(|s| s.checked_add(MAGIC_LEN))
        .and_then(|s| s.checked_add(CHECKSUM_LEN))
        .and_then(|s| s.checked_add(METADATA_SIZE_LEN))
        .and_then(|s| s.checked_add(meta_size))
        .ok_or(FrameError::BadLength(u32::MAX))?;
    let total_size = header_content_size
        .checked_add(payload_len)
        .ok_or(FrameError::BadLength(u32::MAX))?;
    let total_size_u32 = u32::try_from(total_size).map_err(|_| FrameError::BadLength(u32::MAX))?;
    let cmd_size_u32 = u32::try_from(cmd_size).map_err(|_| FrameError::BadLength(u32::MAX))?;
    let meta_size_u32 = u32::try_from(meta_size).map_err(|_| FrameError::BadLength(u32::MAX))?;

    dst.reserve(TOTAL_SIZE_LEN + total_size);
    dst.put_u32(total_size_u32);
    dst.put_u32(cmd_size_u32);
    cmd.encode(dst)?;
    dst.put_u16(MAGIC_CRC32C);

    // Reserve the checksum slot; we backfill it after we know the metadata + payload bytes.
    let checksum_offset = dst.len();
    dst.put_u32(0);

    let checksummed_start = dst.len();
    dst.put_u32(meta_size_u32);
    metadata.encode(dst)?;
    // CRC over [meta_size_field][metadata bytes]; resume over payload.
    let pre_payload_crc = crc32c::crc32c(&dst[checksummed_start..]);
    let full_crc = crc32c::crc32c_append(pre_payload_crc, payload);
    dst[checksum_offset..checksum_offset + CHECKSUM_LEN].copy_from_slice(&full_crc.to_be_bytes());

    dst.extend_from_slice(payload);
    Ok(())
}

/// Encode a payload-bearing frame **without** copying the payload into
/// the head buffer — returns the encoded head bytes
/// (`[total_size][cmd_size][BaseCommand][magic][crc32c][meta_size][MessageMetadata]`)
/// and leaves the caller to emit the payload as a separate segment.
///
/// This is the wave-1.2 zero-copy path for ADR-0040: the producer batch
/// drain pushes `[head, payload]` segment pairs into
/// `Connection::outbound_segments`, and the runtime adapter feeds the
/// list into `poll_write_vectored` / `IoSlice` to skip the user-space
/// memcpy that the contiguous-coalesce [`encode_payload`] path incurs
/// at the `dst.extend_from_slice(payload)` line.
///
/// The CRC32C is computed over `[meta_size u32 BE][metadata bytes][payload bytes]`
/// exactly as in [`encode_payload`] — the two functions emit
/// **byte-identical** wire output when the head is concatenated with
/// the payload.
///
/// # Errors
///
/// Returns [`FrameError::Encode`] if any protobuf encode step fails, or
/// [`FrameError::BadLength`] if the resulting frame would exceed
/// `u32::MAX` bytes.
pub fn encode_payload_head(
    cmd: &pb::BaseCommand,
    metadata: &pb::MessageMetadata,
    payload: &[u8],
) -> Result<BytesMut, FrameError> {
    let cmd_size = cmd.encoded_len();
    let meta_size = metadata.encoded_len();
    let payload_len = payload.len();

    // Mirror `encode_payload`'s size accounting exactly so the head's
    // `total_size u32` covers the eventual payload segment.
    let header_content_size = CMD_SIZE_LEN
        .checked_add(cmd_size)
        .and_then(|s| s.checked_add(MAGIC_LEN))
        .and_then(|s| s.checked_add(CHECKSUM_LEN))
        .and_then(|s| s.checked_add(METADATA_SIZE_LEN))
        .and_then(|s| s.checked_add(meta_size))
        .ok_or(FrameError::BadLength(u32::MAX))?;
    let total_size = header_content_size
        .checked_add(payload_len)
        .ok_or(FrameError::BadLength(u32::MAX))?;
    let total_size_u32 = u32::try_from(total_size).map_err(|_| FrameError::BadLength(u32::MAX))?;
    let cmd_size_u32 = u32::try_from(cmd_size).map_err(|_| FrameError::BadLength(u32::MAX))?;
    let meta_size_u32 = u32::try_from(meta_size).map_err(|_| FrameError::BadLength(u32::MAX))?;

    let mut dst = BytesMut::with_capacity(TOTAL_SIZE_LEN + header_content_size);
    dst.put_u32(total_size_u32);
    dst.put_u32(cmd_size_u32);
    cmd.encode(&mut dst)?;
    dst.put_u16(MAGIC_CRC32C);

    let checksum_offset = dst.len();
    dst.put_u32(0);

    let checksummed_start = dst.len();
    dst.put_u32(meta_size_u32);
    metadata.encode(&mut dst)?;
    let pre_payload_crc = crc32c::crc32c(&dst[checksummed_start..]);
    let full_crc = crc32c::crc32c_append(pre_payload_crc, payload);
    dst[checksum_offset..checksum_offset + CHECKSUM_LEN].copy_from_slice(&full_crc.to_be_bytes());

    // Intentionally do NOT extend with `payload` — the caller emits
    // `[head, payload]` as two adjacent segments via vectored I/O.
    Ok(dst)
}

/// Peek at the front of `inbound` to determine whether a complete frame
/// is ready to decode.
///
/// Returns `Ok(None)` if fewer than `TOTAL_SIZE_LEN` header bytes are
/// present, or if the announced frame extends past the current buffer —
/// the caller should park and try again after more bytes arrive.
/// Returns `Ok(Some(len))` if exactly `len` bytes at the front of
/// `inbound` form a complete frame and can be split off via
/// `inbound.split_to(len)` for handing to [`decode_one`].
/// Returns `Err(BadLength)` if the announced `total_size` is zero or
/// exceeds [`MAX_FRAME_SIZE`].
///
/// This is the cheap front-of-stream check that lets
/// [`crate::Connection::handle_bytes`] avoid `Bytes::copy_from_slice`
/// on every decode iteration; see the receive-path zero-copy entry
/// in `docs/follow-ups.md` under the 2026-05-27 audit section.
pub fn peek_full_frame_len(inbound: &BytesMut) -> Result<Option<usize>, FrameError> {
    if inbound.len() < TOTAL_SIZE_LEN {
        return Ok(None);
    }
    let total_size = u32::from_be_bytes([inbound[0], inbound[1], inbound[2], inbound[3]]);
    if total_size == 0 {
        return Err(FrameError::BadLength(total_size));
    }
    let total_size_usize = total_size as usize;
    if total_size_usize > MAX_FRAME_SIZE {
        return Err(FrameError::BadLength(total_size));
    }
    let full_frame_len = TOTAL_SIZE_LEN + total_size_usize;
    if inbound.len() < full_frame_len {
        return Ok(None);
    }
    Ok(Some(full_frame_len))
}

/// Decode exactly one frame from the head of `src`, advancing `src` past the consumed bytes.
///
/// Returns [`FrameError::Incomplete`] if `src` is shorter than a full frame; the caller may
/// retry once more bytes are available. The returned `needed` value is a lower bound — the
/// decoder may need more bytes after the first re-read.
///
/// # Errors
///
/// - [`FrameError::Incomplete`]: not enough bytes for the announced frame.
/// - [`FrameError::BadLength`]: announced length is zero or exceeds [`MAX_FRAME_SIZE`].
/// - [`FrameError::MissingMagic`]: payload prelude has neither `0x0e01` nor `0x0e02`.
/// - [`FrameError::ChecksumMismatch`]: CRC32C verification failed.
/// - [`FrameError::Decode`]: protobuf decode failed for command, metadata, or BEM.
pub fn decode_one(src: &mut Bytes) -> Result<Frame, FrameError> {
    if src.len() < TOTAL_SIZE_LEN {
        return Err(FrameError::Incomplete {
            needed: TOTAL_SIZE_LEN - src.len(),
        });
    }
    // Peek total_size without consuming, so an Incomplete error leaves the buffer intact.
    let total_size = u32::from_be_bytes([src[0], src[1], src[2], src[3]]);
    if total_size == 0 {
        return Err(FrameError::BadLength(total_size));
    }
    let total_size_usize = total_size as usize;
    if total_size_usize > MAX_FRAME_SIZE {
        return Err(FrameError::BadLength(total_size));
    }
    let full_frame_len = TOTAL_SIZE_LEN + total_size_usize;
    if src.len() < full_frame_len {
        return Err(FrameError::Incomplete {
            needed: full_frame_len - src.len(),
        });
    }

    // We have a complete frame in `src`. Carve it out and advance the caller's cursor.
    let mut frame_bytes = src.split_to(full_frame_len);
    frame_bytes.advance(TOTAL_SIZE_LEN); // discard total_size field

    // After this point we operate on `frame_bytes` which contains exactly `total_size` bytes.
    if frame_bytes.remaining() < CMD_SIZE_LEN {
        return Err(FrameError::BadLength(total_size));
    }
    let cmd_size = frame_bytes.get_u32();
    let cmd_size_usize = cmd_size as usize;
    if cmd_size_usize > frame_bytes.remaining() {
        return Err(FrameError::BadLength(total_size));
    }

    let cmd_bytes = frame_bytes.split_to(cmd_size_usize);
    let command = pb::BaseCommand::decode(cmd_bytes)?;

    // If anything remains after the command, the frame carries a payload region.
    if !frame_bytes.has_remaining() {
        return Ok(Frame {
            command,
            payload: None,
        });
    }

    let mut broker_entry_metadata = None;
    if frame_bytes.remaining() < MAGIC_LEN {
        return Err(FrameError::Incomplete {
            needed: MAGIC_LEN - frame_bytes.remaining(),
        });
    }
    let mut magic = u16::from_be_bytes([frame_bytes[0], frame_bytes[1]]);
    if magic == MAGIC_BROKER_ENTRY_METADATA {
        frame_bytes.advance(MAGIC_LEN);
        if frame_bytes.remaining() < BEM_SIZE_LEN {
            return Err(FrameError::Incomplete {
                needed: BEM_SIZE_LEN - frame_bytes.remaining(),
            });
        }
        let bem_size = frame_bytes.get_u32() as usize;
        if bem_size > frame_bytes.remaining() {
            return Err(FrameError::BadLength(total_size));
        }
        let bem_bytes = frame_bytes.split_to(bem_size);
        broker_entry_metadata = Some(pb::BrokerEntryMetadata::decode(bem_bytes)?);
        if frame_bytes.remaining() < MAGIC_LEN {
            return Err(FrameError::Incomplete {
                needed: MAGIC_LEN - frame_bytes.remaining(),
            });
        }
        magic = u16::from_be_bytes([frame_bytes[0], frame_bytes[1]]);
    }
    if magic != MAGIC_CRC32C {
        return Err(FrameError::MissingMagic(magic));
    }
    frame_bytes.advance(MAGIC_LEN);

    if frame_bytes.remaining() < CHECKSUM_LEN + METADATA_SIZE_LEN {
        return Err(FrameError::Incomplete {
            needed: CHECKSUM_LEN + METADATA_SIZE_LEN - frame_bytes.remaining(),
        });
    }
    let expected_crc = frame_bytes.get_u32();

    // The CRC covers the remaining bytes verbatim: [meta_size u32 BE][metadata][payload].
    let checksummed = frame_bytes.clone();
    let computed_crc = crc32c::crc32c(&checksummed);
    if computed_crc != expected_crc {
        return Err(FrameError::ChecksumMismatch {
            computed: computed_crc,
            expected: expected_crc,
        });
    }

    let meta_size = frame_bytes.get_u32() as usize;
    if meta_size > frame_bytes.remaining() {
        return Err(FrameError::BadLength(total_size));
    }
    let metadata_bytes = frame_bytes.split_to(meta_size);
    let metadata = pb::MessageMetadata::decode(metadata_bytes)?;
    let body = frame_bytes; // remainder = payload

    Ok(Frame {
        command,
        payload: Some(Payload {
            broker_entry_metadata,
            metadata,
            body,
        }),
    })
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};

    use super::*;
    use crate::pb;

    fn ping_command() -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::Ping as i32,
            ping: Some(pb::CommandPing {}),
            ..Default::default()
        }
    }

    fn pong_command() -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::Pong as i32,
            pong: Some(pb::CommandPong {}),
            ..Default::default()
        }
    }

    fn connect_command() -> pb::BaseCommand {
        let connect = pb::CommandConnect {
            client_version: "magnetar/0.1".to_owned(),
            auth_method_name: Some("none".to_owned()),
            protocol_version: Some(21),
            ..Default::default()
        };
        pb::BaseCommand {
            r#type: pb::base_command::Type::Connect as i32,
            connect: Some(connect),
            ..Default::default()
        }
    }

    fn send_command(producer_id: u64, sequence_id: u64) -> pb::BaseCommand {
        pb::BaseCommand {
            r#type: pb::base_command::Type::Send as i32,
            send: Some(pb::CommandSend {
                producer_id,
                sequence_id,
                num_messages: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn message_metadata(producer: &str, sequence_id: u64) -> pb::MessageMetadata {
        pb::MessageMetadata {
            producer_name: producer.to_owned(),
            sequence_id,
            publish_time: 1_700_000_000_000,
            ..Default::default()
        }
    }

    fn encode_one(cmd: &pb::BaseCommand) -> Bytes {
        let mut buf = BytesMut::new();
        encode_command(&mut buf, cmd).expect("encode_command");
        buf.freeze()
    }

    fn encode_send(cmd: &pb::BaseCommand, meta: &pb::MessageMetadata, payload: &[u8]) -> Bytes {
        let mut buf = BytesMut::new();
        encode_payload(&mut buf, cmd, meta, payload).expect("encode_payload");
        buf.freeze()
    }

    #[test]
    fn encode_payload_head_matches_encode_payload_concatenated() {
        // ADR-0040 wave 1.2: byte-equivalence between the contiguous
        // `encode_payload` (head + payload memcpy'd into one buffer)
        // and the vectored `encode_payload_head` (head returned, payload
        // emitted as a separate segment). The two paths MUST produce
        // byte-identical wire bytes when concatenated — the kernel sees
        // the same bytes whether they arrive via `write_all(&buf)` or
        // `write_vectored(&[head, payload])`.
        let cmd = send_command(7, 42);
        let meta = message_metadata("p", 42);
        let payload = b"hello vectored world";

        let contiguous = encode_send(&cmd, &meta, payload);
        let head = encode_payload_head(&cmd, &meta, payload).expect("encode_payload_head");
        let mut vectored = BytesMut::with_capacity(head.len() + payload.len());
        vectored.extend_from_slice(&head);
        vectored.extend_from_slice(payload);

        assert_eq!(
            &contiguous[..],
            &vectored[..],
            "encode_payload_head + payload must be byte-identical to encode_payload"
        );
        // Empty payload: the head must still carry the correct CRC32C
        // for an empty payload region.
        let empty_contig = encode_send(&cmd, &meta, &[]);
        let empty_head = encode_payload_head(&cmd, &meta, &[]).expect("encode_payload_head empty");
        assert_eq!(
            &empty_contig[..],
            &empty_head[..],
            "empty-payload vectored head must equal full encode_payload output"
        );
    }

    #[test]
    fn roundtrip_empty_ping() {
        let cmd = ping_command();
        let mut bytes = encode_one(&cmd);
        let frame = decode_one(&mut bytes).expect("decode ping");
        assert!(bytes.is_empty(), "decoder must consume the whole frame");
        assert_eq!(frame.command.r#type, cmd.r#type);
        assert!(frame.command.ping.is_some());
        assert!(frame.payload.is_none());
    }

    #[test]
    fn roundtrip_connect() {
        let cmd = connect_command();
        let mut bytes = encode_one(&cmd);
        let frame = decode_one(&mut bytes).expect("decode connect");
        let decoded_connect = frame.command.connect.expect("connect payload");
        assert_eq!(decoded_connect.client_version, "magnetar/0.1");
        assert_eq!(decoded_connect.auth_method_name.as_deref(), Some("none"));
        assert_eq!(decoded_connect.protocol_version, Some(21));
        assert!(frame.payload.is_none());
    }

    #[test]
    fn roundtrip_ping_and_pong() {
        // Concatenate two frames in the same buffer and ensure the decoder consumes them in
        // order, leaving the cursor at the boundary between them.
        let mut joined = BytesMut::new();
        encode_command(&mut joined, &ping_command()).unwrap();
        encode_command(&mut joined, &pong_command()).unwrap();
        let mut bytes = joined.freeze();

        let first = decode_one(&mut bytes).expect("first frame");
        assert_eq!(first.command.r#type, pb::base_command::Type::Ping as i32);
        let second = decode_one(&mut bytes).expect("second frame");
        assert_eq!(second.command.r#type, pb::base_command::Type::Pong as i32);
        assert!(bytes.is_empty());
    }

    #[test]
    fn roundtrip_send_with_payload() {
        let cmd = send_command(7, 42);
        let meta = message_metadata("producer-A", 42);
        let payload = b"hello".to_vec();
        let mut bytes = encode_send(&cmd, &meta, &payload);
        let frame = decode_one(&mut bytes).expect("decode send");

        assert!(bytes.is_empty(), "decoder must consume the whole frame");
        assert_eq!(frame.command.r#type, pb::base_command::Type::Send as i32);
        let payload_region = frame.payload.expect("payload region");
        assert!(payload_region.broker_entry_metadata.is_none());
        assert_eq!(payload_region.metadata.producer_name, "producer-A");
        assert_eq!(payload_region.metadata.sequence_id, 42);
        assert_eq!(payload_region.body.as_ref(), payload.as_slice());
    }

    #[test]
    fn detects_crc32c_mismatch() {
        let cmd = send_command(1, 1);
        let meta = message_metadata("p", 1);
        let payload = b"corrupt-me".to_vec();
        let bytes = encode_send(&cmd, &meta, &payload);
        // Flip the last byte of the payload (last byte of the frame) to invalidate the CRC.
        let mut mutated = BytesMut::from(bytes.as_ref());
        let last = mutated.len() - 1;
        mutated[last] ^= 0xff;
        let mut mutated_bytes = mutated.freeze();
        match decode_one(&mut mutated_bytes) {
            Err(FrameError::ChecksumMismatch { computed, expected }) => {
                assert_ne!(computed, expected);
            }
            other => panic!("expected ChecksumMismatch, got {other:?}"),
        }
    }

    #[test]
    fn detects_bad_payload_magic() {
        let cmd = send_command(1, 1);
        let meta = message_metadata("p", 1);
        let payload = b"".to_vec();
        let bytes = encode_send(&cmd, &meta, &payload);

        // Locate the magic by searching for it relative to cmd_size. Easier: re-derive cmd_size
        // from the buffer and compute the offset.
        let cmd_size = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
        let magic_offset = TOTAL_SIZE_LEN + CMD_SIZE_LEN + cmd_size;
        let mut mutated = BytesMut::from(bytes.as_ref());
        mutated[magic_offset] = 0xde;
        mutated[magic_offset + 1] = 0xad;
        let mut mutated_bytes = mutated.freeze();
        match decode_one(&mut mutated_bytes) {
            Err(FrameError::MissingMagic(0xdead)) => {}
            other => panic!("expected MissingMagic(0xdead), got {other:?}"),
        }
    }

    #[test]
    fn peels_broker_entry_metadata() {
        // Hand-craft a frame shape: [hdr][BEM prelude][BEM bytes][CRC prelude][meta][payload].
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Message as i32,
            message: Some(pb::CommandMessage {
                consumer_id: 1,
                message_id: pb::MessageIdData {
                    ledger_id: 1,
                    entry_id: 1,
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        };
        let meta = message_metadata("p", 7);
        let payload = b"with-bem".to_vec();
        let bem = pb::BrokerEntryMetadata {
            broker_timestamp: Some(1_700_000_000_500),
            index: Some(99),
        };

        // We assemble the frame by hand to be explicit about the prelude ordering.
        let cmd_size = cmd.encoded_len();
        let bem_size = bem.encoded_len();
        let meta_size = meta.encoded_len();
        let payload_len = payload.len();

        let total_size = CMD_SIZE_LEN
            + cmd_size
            + MAGIC_LEN
            + BEM_SIZE_LEN
            + bem_size
            + MAGIC_LEN
            + CHECKSUM_LEN
            + METADATA_SIZE_LEN
            + meta_size
            + payload_len;

        let mut buf = BytesMut::with_capacity(TOTAL_SIZE_LEN + total_size);
        buf.put_u32(total_size as u32);
        buf.put_u32(cmd_size as u32);
        cmd.encode(&mut buf).unwrap();
        buf.put_u16(MAGIC_BROKER_ENTRY_METADATA);
        buf.put_u32(bem_size as u32);
        bem.encode(&mut buf).unwrap();
        buf.put_u16(MAGIC_CRC32C);

        // Reserve and backfill the CRC over [meta_size][metadata][payload].
        let crc_offset = buf.len();
        buf.put_u32(0);
        let checksummed_start = buf.len();
        buf.put_u32(meta_size as u32);
        meta.encode(&mut buf).unwrap();
        let pre = crc32c::crc32c(&buf[checksummed_start..]);
        let full = crc32c::crc32c_append(pre, &payload);
        buf[crc_offset..crc_offset + CHECKSUM_LEN].copy_from_slice(&full.to_be_bytes());
        buf.extend_from_slice(&payload);

        let mut bytes = buf.freeze();
        let frame = decode_one(&mut bytes).expect("decode bem frame");
        assert!(bytes.is_empty());
        let payload_region = frame.payload.expect("payload region");
        let parsed_bem = payload_region
            .broker_entry_metadata
            .expect("broker entry metadata");
        assert_eq!(parsed_bem.broker_timestamp, Some(1_700_000_000_500));
        assert_eq!(parsed_bem.index, Some(99));
        assert_eq!(payload_region.metadata.producer_name, "p");
        assert_eq!(payload_region.metadata.sequence_id, 7);
        assert_eq!(payload_region.body.as_ref(), payload.as_slice());
    }

    #[test]
    fn incomplete_frame_reports_needed_bytes() {
        let mut bytes = Bytes::from_static(&[0x00]);
        match decode_one(&mut bytes) {
            Err(FrameError::Incomplete { needed }) => {
                assert!(needed >= 3, "needed at least 3 more bytes; got {needed}");
            }
            other => panic!("expected Incomplete, got {other:?}"),
        }
        // The decoder must not consume the single available byte on an incomplete read; the
        // caller should be able to append more bytes and retry.
        assert_eq!(bytes.len(), 1);
    }

    #[test]
    fn rejects_oversized_frame() {
        // Announce a frame larger than MAX_FRAME_SIZE without supplying its body.
        let announced = (MAX_FRAME_SIZE as u32) + 1;
        let mut buf = BytesMut::with_capacity(TOTAL_SIZE_LEN);
        buf.put_u32(announced);
        let mut bytes = buf.freeze();
        match decode_one(&mut bytes) {
            Err(FrameError::BadLength(reported)) => assert_eq!(reported, announced),
            other => panic!("expected BadLength, got {other:?}"),
        }
    }

    #[test]
    fn rejects_zero_length_frame() {
        let mut bytes = Bytes::from_static(&[0u8; 4]);
        match decode_one(&mut bytes) {
            Err(FrameError::BadLength(0)) => {}
            other => panic!("expected BadLength(0), got {other:?}"),
        }
    }

    #[test]
    fn peek_full_frame_len_distinguishes_incomplete_header_body_and_complete() {
        // Empty buffer → need header.
        let empty = BytesMut::new();
        assert!(matches!(peek_full_frame_len(&empty), Ok(None)));

        // 3-byte header (need 4) → still incomplete.
        let short = BytesMut::from(&[0u8, 0, 0][..]);
        assert!(matches!(peek_full_frame_len(&short), Ok(None)));

        // 4-byte header announcing 100 bytes, only 50 follow → incomplete body.
        let mut partial = BytesMut::with_capacity(54);
        partial.put_u32(100);
        partial.extend_from_slice(&[0u8; 50]);
        assert!(matches!(peek_full_frame_len(&partial), Ok(None)));

        // 4-byte header announcing 100 bytes, full 100 bytes follow → complete.
        let mut complete = BytesMut::with_capacity(104);
        complete.put_u32(100);
        complete.extend_from_slice(&[0u8; 100]);
        match peek_full_frame_len(&complete) {
            Ok(Some(len)) => assert_eq!(len, 104),
            other => panic!("expected Ok(Some(104)), got {other:?}"),
        }

        // total_size = 0 → BadLength.
        let zero = BytesMut::from(&[0u8, 0, 0, 0][..]);
        assert!(matches!(
            peek_full_frame_len(&zero),
            Err(FrameError::BadLength(0))
        ));

        // total_size > MAX_FRAME_SIZE → BadLength.
        let mut oversized = BytesMut::with_capacity(4);
        oversized.put_u32(MAX_FRAME_SIZE as u32 + 1);
        match peek_full_frame_len(&oversized) {
            Err(FrameError::BadLength(n)) => assert_eq!(n, MAX_FRAME_SIZE as u32 + 1),
            other => panic!("expected BadLength, got {other:?}"),
        }
    }
}
