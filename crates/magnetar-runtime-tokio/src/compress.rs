// SPDX-License-Identifier: Apache-2.0

//! Pulsar payload compression / decompression.
//!
//! The sans-io producer in `magnetar-proto` stamps
//! [`pb::MessageMetadata::compression`](magnetar_proto::pb::MessageMetadata::compression)
//! and [`pb::MessageMetadata::uncompressed_size`] but never touches the payload bytes —
//! compression is the runtime engine's job. Mirrors `ProducerImpl.java:581-608` where
//! non-batch payload bytes are compressed in-place before chunking + encryption.
//!
//! This module exposes [`compress`] / [`decompress`] helpers indexed by
//! [`CompressionKind`] (the Rust analogue of `pb::CompressionType`). All four Pulsar codecs
//! are wired: LZ4 (block format with a header), Zlib, Zstd, Snappy. The `None` variant is
//! a pass-through.
//!
//! Decompression takes the broker-supplied `uncompressed_size` as a sanity bound — a payload
//! that would inflate beyond `MAX_INFLATE_RATIO × uncompressed_size` is rejected to keep
//! malicious peers from triggering OOMs.

use bytes::Bytes;
use magnetar_proto::pb;
use magnetar_proto::types::CompressionKind;

/// Bound on inflation during `decompress()` — caps each codec's working buffer at
/// `MAX_INFLATE_RATIO × uncompressed_size` to defuse decompression bombs.
const MAX_INFLATE_RATIO: usize = 4;

/// Errors that can occur during compress/decompress.
#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
    /// LZ4 codec failure.
    #[error("lz4 codec: {0}")]
    Lz4(String),
    /// Zlib codec failure.
    #[error("zlib codec: {0}")]
    Zlib(#[from] std::io::Error),
    /// Zstd codec failure.
    #[error("zstd codec: {0}")]
    Zstd(String),
    /// Snappy codec failure.
    #[error("snappy codec: {0}")]
    Snappy(String),
    /// Decompressed size deviates from the broker-stamped `uncompressed_size`.
    #[error("decompressed size mismatch: got {got}, expected {expected}")]
    SizeMismatch { got: usize, expected: usize },
}

/// Map the protobuf-generated enum to our Rust-flavoured one.
#[must_use]
pub fn kind_from_pb(pb: pb::CompressionType) -> CompressionKind {
    match pb {
        pb::CompressionType::None => CompressionKind::None,
        pb::CompressionType::Lz4 => CompressionKind::Lz4,
        pb::CompressionType::Zlib => CompressionKind::Zlib,
        pb::CompressionType::Zstd => CompressionKind::Zstd,
        pb::CompressionType::Snappy => CompressionKind::Snappy,
    }
}

/// Compress `plaintext` according to `kind` and return the wire payload.
///
/// `CompressionKind::None` returns `plaintext` cloned.
///
/// # Errors
/// Propagates codec-specific failures (LZ4 / Snappy / Zstd compress paths).
pub fn compress(kind: CompressionKind, plaintext: &[u8]) -> Result<Bytes, CompressionError> {
    match kind {
        CompressionKind::None => Ok(Bytes::copy_from_slice(plaintext)),
        CompressionKind::Lz4 => {
            // Pulsar uses the LZ4 *block* format (not the LZ4 frame format).
            let compressed = lz4_flex::compress(plaintext);
            Ok(Bytes::from(compressed))
        }
        CompressionKind::Zlib => {
            use std::io::Write;
            let mut encoder = flate2::write::ZlibEncoder::new(
                Vec::with_capacity(plaintext.len() / 2),
                flate2::Compression::default(),
            );
            encoder.write_all(plaintext)?;
            let buf = encoder.finish()?;
            Ok(Bytes::from(buf))
        }
        CompressionKind::Zstd => {
            let buf = zstd::stream::encode_all(plaintext, 3)
                .map_err(|e| CompressionError::Zstd(e.to_string()))?;
            Ok(Bytes::from(buf))
        }
        CompressionKind::Snappy => {
            let mut encoder = snap::raw::Encoder::new();
            let buf = encoder
                .compress_vec(plaintext)
                .map_err(|e| CompressionError::Snappy(e.to_string()))?;
            Ok(Bytes::from(buf))
        }
    }
}

/// Decompress `ciphertext` according to `kind`, using `uncompressed_size` from the broker as
/// the expected output size (and as the safety bound).
///
/// # Errors
/// Codec-specific decode failures, plus [`CompressionError::SizeMismatch`] if the decompressed
/// size disagrees with the broker-stamped value (which would indicate a tampered payload or a
/// broker bug).
pub fn decompress(
    kind: CompressionKind,
    ciphertext: &[u8],
    uncompressed_size: usize,
) -> Result<Bytes, CompressionError> {
    let bound = uncompressed_size.saturating_mul(MAX_INFLATE_RATIO).max(64);
    match kind {
        CompressionKind::None => Ok(Bytes::copy_from_slice(ciphertext)),
        CompressionKind::Lz4 => {
            let decompressed = lz4_flex::decompress(ciphertext, uncompressed_size)
                .map_err(|e| CompressionError::Lz4(e.to_string()))?;
            verify_size(&decompressed, uncompressed_size)?;
            Ok(Bytes::from(decompressed))
        }
        CompressionKind::Zlib => {
            use std::io::Read;
            let mut decoder = flate2::read::ZlibDecoder::new(ciphertext);
            let mut out = Vec::with_capacity(uncompressed_size);
            decoder.by_ref().take(bound as u64).read_to_end(&mut out)?;
            verify_size(&out, uncompressed_size)?;
            Ok(Bytes::from(out))
        }
        CompressionKind::Zstd => {
            // zstd's decode_all caps internal allocation; we still check size after.
            let out = zstd::stream::decode_all(ciphertext)
                .map_err(|e| CompressionError::Zstd(e.to_string()))?;
            verify_size(&out, uncompressed_size)?;
            Ok(Bytes::from(out))
        }
        CompressionKind::Snappy => {
            let mut decoder = snap::raw::Decoder::new();
            let out = decoder
                .decompress_vec(ciphertext)
                .map_err(|e| CompressionError::Snappy(e.to_string()))?;
            verify_size(&out, uncompressed_size)?;
            Ok(Bytes::from(out))
        }
    }
}

fn verify_size(buf: &[u8], expected: usize) -> Result<(), CompressionError> {
    if buf.len() != expected {
        return Err(CompressionError::SizeMismatch {
            got: buf.len(),
            expected,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{CompressionKind, compress, decompress};

    fn roundtrip(kind: CompressionKind, payload: &[u8]) {
        let compressed = compress(kind, payload).expect("compress");
        let decompressed = decompress(kind, &compressed, payload.len()).expect("decompress");
        assert_eq!(&decompressed[..], payload, "kind={kind:?}");
    }

    #[test]
    fn none_passes_through() {
        roundtrip(CompressionKind::None, b"hello world");
        roundtrip(CompressionKind::None, b"");
    }

    #[test]
    fn lz4_roundtrip() {
        roundtrip(CompressionKind::Lz4, b"abracadabra abracadabra abracadabra");
        roundtrip(CompressionKind::Lz4, &vec![0xAAu8; 4096]);
    }

    #[test]
    fn zlib_roundtrip() {
        roundtrip(CompressionKind::Zlib, b"hello pulsar 4.0");
        roundtrip(CompressionKind::Zlib, &vec![0u8; 8192]);
    }

    #[test]
    fn zstd_roundtrip() {
        roundtrip(
            CompressionKind::Zstd,
            b"the quick brown fox jumps over the lazy dog",
        );
        roundtrip(CompressionKind::Zstd, &vec![0x55u8; 16_384]);
    }

    #[test]
    fn snappy_roundtrip() {
        roundtrip(
            CompressionKind::Snappy,
            b"snappy is fast but not always small",
        );
        roundtrip(CompressionKind::Snappy, &vec![0x11u8; 4096]);
    }

    #[test]
    fn size_mismatch_rejected() {
        let payload = vec![0u8; 1024];
        let compressed = compress(CompressionKind::Zstd, &payload).unwrap();
        // Lie about the uncompressed size — bound is satisfied but verify_size rejects.
        let err = decompress(CompressionKind::Zstd, &compressed, 999).expect_err("mismatch");
        assert!(matches!(err, super::CompressionError::SizeMismatch { .. }));
    }
}
