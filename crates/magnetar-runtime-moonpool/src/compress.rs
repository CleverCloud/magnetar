// SPDX-License-Identifier: Apache-2.0

//! Bounded inbound Pulsar payload decompression.

use bytes::Bytes;
use magnetar_proto::pb;
use magnetar_proto::types::CompressionKind;

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
    let validation_len = uncompressed_size
        .checked_add(magnetar_proto::DECOMPRESSION_VALIDATION_SLACK)
        .ok_or(CompressionError::UncompressedSizeTooLarge {
            got: usize::MAX,
            ceiling,
        })?;
    match kind {
        CompressionKind::None => Ok(Bytes::copy_from_slice(ciphertext)),
        CompressionKind::Lz4 => {
            let mut decompressed = vec![0_u8; validation_len];
            let len = lz4_flex::block::decompress_into(ciphertext, &mut decompressed)
                .map_err(|error| CompressionError::Lz4(error.to_string()))?;
            decompressed.truncate(len);
            verify_size(&decompressed, uncompressed_size)?;
            Ok(Bytes::from(decompressed))
        }
        CompressionKind::Zlib => {
            let mut decoder = flate2::bufread::ZlibDecoder::new(ciphertext);
            let output = read_bounded(&mut decoder, validation_len)?;
            verify_size(&output, uncompressed_size)?;
            Ok(Bytes::from(output))
        }
        CompressionKind::Zstd => {
            let mut decoder = zstd::bulk::Decompressor::new()
                .map_err(|error| CompressionError::Zstd(error.to_string()))?;
            decoder
                .window_log_max(zstd_window_log(uncompressed_size))
                .map_err(|error| CompressionError::Zstd(error.to_string()))?;
            let mut output = vec![0_u8; validation_len];
            let written = decoder
                .decompress_to_buffer(ciphertext, output.as_mut_slice())
                .map_err(|error| CompressionError::Zstd(error.to_string()))?;
            output.truncate(written);
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
            if announced != uncompressed_size {
                return Err(CompressionError::SizeMismatch {
                    got: announced,
                    expected: uncompressed_size,
                });
            }
            let mut output = vec![0u8; uncompressed_size];
            let len = Decoder::new()
                .decompress(ciphertext, &mut output)
                .map_err(|error| CompressionError::Snappy(error.to_string()))?;
            output.truncate(len);
            verify_size(&output, uncompressed_size)?;
            Ok(Bytes::from(output))
        }
    }
}

fn zstd_window_log(uncompressed_size: usize) -> u32 {
    let window = uncompressed_size
        .max(magnetar_proto::ZSTD_MIN_WINDOW_SIZE)
        .next_power_of_two();
    usize::BITS - window.leading_zeros() - 1
}

fn read_bounded(
    reader: &mut impl std::io::Read,
    validation_len: usize,
) -> Result<Vec<u8>, std::io::Error> {
    let mut output = vec![0_u8; validation_len];
    let mut written = 0;
    while written < validation_len {
        let read = reader.read(&mut output[written..])?;
        if read == 0 {
            break;
        }
        written += read;
    }
    output.truncate(written);
    Ok(output)
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
        assert!(decompress(CompressionKind::Snappy, b"invalid", 1).is_err());

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

    #[test]
    #[allow(clippy::match_same_arms)]
    fn every_codec_stops_at_the_advertised_size_plus_validation_slack() {
        let payload = vec![0_u8; 4096];
        let lz4 = lz4_flex::compress(&payload);
        let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        zlib.write_all(&payload).expect("write zlib fixture");
        let zlib = zlib.finish().expect("finish zlib fixture");
        let zstd = zstd::stream::encode_all(payload.as_slice(), 0).expect("encode zstd fixture");
        let snappy = snap::raw::Encoder::new()
            .compress_vec(&payload)
            .expect("encode snappy fixture");

        for (kind, encoded) in [
            (CompressionKind::Lz4, lz4),
            (CompressionKind::Zlib, zlib),
            (CompressionKind::Zstd, zstd),
            (CompressionKind::Snappy, snappy),
        ] {
            let error = decompress(kind, &encoded, 1).expect_err("overexpansion rejected");
            match (kind, error) {
                (CompressionKind::Lz4, CompressionError::Lz4(_)) => {}
                (
                    CompressionKind::Zlib,
                    CompressionError::SizeMismatch {
                        got: 2,
                        expected: 1,
                    },
                ) => {}
                (CompressionKind::Zstd, CompressionError::Zstd(_)) => {}
                (
                    CompressionKind::Snappy,
                    CompressionError::SizeMismatch {
                        got: 4096,
                        expected: 1,
                    },
                ) => {}
                (_, other) => panic!("kind={kind:?} returned unexpected error {other:?}"),
            }
        }
    }
}
