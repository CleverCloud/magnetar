// SPDX-License-Identifier: Apache-2.0

//! Shared bounded inbound Pulsar payload decompression.

pub(crate) use magnetar_proto::compress::{CompressionError, decompress, kind_from_pb};

#[cfg(test)]
use magnetar_proto::{pb, types::CompressionKind};

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
