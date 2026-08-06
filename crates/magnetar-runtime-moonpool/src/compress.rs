// SPDX-License-Identifier: Apache-2.0

//! Bounded inbound Pulsar payload decompression.

use bytes::Bytes;
use magnetar_proto::pb;
use magnetar_proto::types::CompressionKind;

const MAX_INFLATE_RATIO: usize = 4;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CompressionError {
    #[error("lz4 codec: {0}")]
    Lz4(String),
    #[error("zlib codec: {0}")]
    Zlib(#[from] std::io::Error),
    #[error("zstd codec: {0}")]
    Zstd(String),
    #[error("snappy codec: {0}")]
    Snappy(String),
    #[error("decompressed size mismatch: got {got}, expected {expected}")]
    SizeMismatch { got: usize, expected: usize },
    #[error("uncompressed_size {got} exceeds frame ceiling {ceiling}")]
    UncompressedSizeTooLarge { got: usize, ceiling: usize },
}

pub(crate) fn kind_from_pb(pb: pb::CompressionType) -> CompressionKind {
    match pb {
        pb::CompressionType::None => CompressionKind::None,
        pb::CompressionType::Lz4 => CompressionKind::Lz4,
        pb::CompressionType::Zlib => CompressionKind::Zlib,
        pb::CompressionType::Zstd => CompressionKind::Zstd,
        pb::CompressionType::Snappy => CompressionKind::Snappy,
    }
}

pub(crate) fn decompress(
    kind: CompressionKind,
    ciphertext: &[u8],
    uncompressed_size: usize,
) -> Result<Bytes, CompressionError> {
    let ceiling = magnetar_proto::MAX_FRAME_SIZE;
    if uncompressed_size > ceiling {
        return Err(CompressionError::UncompressedSizeTooLarge {
            got: uncompressed_size,
            ceiling,
        });
    }
    let bound = uncompressed_size.saturating_mul(MAX_INFLATE_RATIO).max(64);
    match kind {
        CompressionKind::None => Ok(Bytes::copy_from_slice(ciphertext)),
        CompressionKind::Lz4 => {
            let decompressed = lz4_flex::decompress(ciphertext, uncompressed_size)
                .map_err(|error| CompressionError::Lz4(error.to_string()))?;
            reject_oversize(&decompressed, ceiling)?;
            verify_size(&decompressed, uncompressed_size)?;
            Ok(Bytes::from(decompressed))
        }
        CompressionKind::Zlib => {
            use std::io::Read as _;

            let mut decoder = flate2::read::ZlibDecoder::new(ciphertext);
            let mut output = Vec::with_capacity(uncompressed_size);
            decoder
                .by_ref()
                .take(bound.saturating_add(1) as u64)
                .read_to_end(&mut output)?;
            reject_oversize(&output, ceiling)?;
            verify_size(&output, uncompressed_size)?;
            Ok(Bytes::from(output))
        }
        CompressionKind::Zstd => {
            use std::io::Read as _;

            let mut decoder = zstd::stream::Decoder::new(ciphertext)
                .map_err(|error| CompressionError::Zstd(error.to_string()))?;
            let mut output = Vec::with_capacity(uncompressed_size);
            decoder
                .by_ref()
                .take(ceiling.saturating_add(1) as u64)
                .read_to_end(&mut output)
                .map_err(|error| CompressionError::Zstd(error.to_string()))?;
            reject_oversize(&output, ceiling)?;
            verify_size(&output, uncompressed_size)?;
            Ok(Bytes::from(output))
        }
        CompressionKind::Snappy => {
            use snap::raw::{Decoder, decompress_len};

            let announced = decompress_len(ciphertext)
                .map_err(|error| CompressionError::Snappy(error.to_string()))?;
            if announced > ceiling {
                return Err(CompressionError::UncompressedSizeTooLarge {
                    got: announced,
                    ceiling,
                });
            }
            let mut output = vec![0u8; announced];
            let len = Decoder::new()
                .decompress(ciphertext, &mut output)
                .map_err(|error| CompressionError::Snappy(error.to_string()))?;
            output.truncate(len);
            reject_oversize(&output, ceiling)?;
            verify_size(&output, uncompressed_size)?;
            Ok(Bytes::from(output))
        }
    }
}

fn reject_oversize(bytes: &[u8], ceiling: usize) -> Result<(), CompressionError> {
    if bytes.len() > ceiling {
        return Err(CompressionError::UncompressedSizeTooLarge {
            got: bytes.len(),
            ceiling,
        });
    }
    Ok(())
}

fn verify_size(bytes: &[u8], expected: usize) -> Result<(), CompressionError> {
    if bytes.len() != expected {
        return Err(CompressionError::SizeMismatch {
            got: bytes.len(),
            expected,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;

    #[test]
    fn every_codec_round_trips_and_maps_from_wire() {
        let input = b"moonpool scalable compression";
        let lz4 = lz4_flex::compress(input);
        let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        zlib.write_all(input).expect("write zlib fixture");
        let zlib = zlib.finish().expect("finish zlib fixture");
        let zstd = zstd::stream::encode_all(input.as_slice(), 0).expect("encode zstd fixture");
        let snappy = snap::raw::Encoder::new()
            .compress_vec(input)
            .expect("encode snappy fixture");

        for (wire, kind, encoded) in [
            (
                pb::CompressionType::None,
                CompressionKind::None,
                input.as_slice(),
            ),
            (
                pb::CompressionType::Lz4,
                CompressionKind::Lz4,
                lz4.as_slice(),
            ),
            (
                pb::CompressionType::Zlib,
                CompressionKind::Zlib,
                zlib.as_slice(),
            ),
            (
                pb::CompressionType::Zstd,
                CompressionKind::Zstd,
                zstd.as_slice(),
            ),
            (
                pb::CompressionType::Snappy,
                CompressionKind::Snappy,
                snappy.as_slice(),
            ),
        ] {
            assert_eq!(kind_from_pb(wire), kind);
            assert_eq!(
                decompress(kind, encoded, input.len()).expect("decode codec fixture"),
                input.as_slice()
            );
        }
    }

    #[test]
    fn malformed_sizes_and_payloads_are_bounded() {
        assert!(matches!(
            decompress(
                CompressionKind::None,
                b"ignored",
                magnetar_proto::MAX_FRAME_SIZE + 1,
            ),
            Err(CompressionError::UncompressedSizeTooLarge { .. })
        ));
        assert!(matches!(
            decompress(CompressionKind::Lz4, b"invalid", 1),
            Err(CompressionError::Lz4(_))
        ));
        assert!(matches!(
            decompress(CompressionKind::Zlib, b"invalid", 1),
            Err(CompressionError::Zlib(_))
        ));
        assert!(matches!(
            decompress(CompressionKind::Zstd, b"invalid", 1),
            Err(CompressionError::Zstd(_))
        ));
        assert!(matches!(
            decompress(CompressionKind::Snappy, b"invalid", 1),
            Err(CompressionError::Snappy(_))
        ));

        let input = b"size mismatch";
        let encoded = lz4_flex::compress(input);
        assert!(matches!(
            decompress(CompressionKind::Lz4, &encoded, input.len() + 1),
            Err(CompressionError::SizeMismatch { .. })
        ));

        let oversized = vec![0_u8; magnetar_proto::MAX_FRAME_SIZE + 1];
        let encoded = snap::raw::Encoder::new()
            .compress_vec(&oversized)
            .expect("encode oversized snappy fixture");
        assert!(matches!(
            decompress(
                CompressionKind::Snappy,
                &encoded,
                magnetar_proto::MAX_FRAME_SIZE,
            ),
            Err(CompressionError::UncompressedSizeTooLarge { .. })
        ));
    }
}
