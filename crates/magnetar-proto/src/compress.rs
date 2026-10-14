// SPDX-License-Identifier: Apache-2.0

//! Pulsar payload compression / decompression.
//!
//! The codecs live in the sans-io core because the batch layout depends on them (ADR-0112):
//! a Java producer compresses the WHOLE packed batch body (`BatchMessageContainerImpl`), so
//! `ConsumerState::deliver` has to decompress a batched entry before it can split it
//! into members, and `ProducerState::flush_batch` has to compress the concatenated body
//! it builds. Every codec crate below is pure computation over in-memory buffers — no
//! I/O, no async — so `cargo run -p xtask -- check-no-io-deps` stays green.
//! `magnetar_runtime_tokio::compress` re-exports this module so its public path keeps
//! working.
//!
//! This module exposes [`compress`] / [`decompress`] helpers indexed by
//! [`CompressionKind`] (the Rust analogue of `pb::CompressionType`). All four Pulsar codecs
//! are wired: LZ4 (block format with a header), Zlib, Zstd, Snappy. The `None` variant is
//! a pass-through.
//!
//! Decompression takes the broker-supplied `uncompressed_size` as an output
//! bound. The aggregate separately reserves bounded codec context/window work.

use bytes::Bytes;

use crate::pb;
use crate::types::CompressionKind;

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
    /// Broker-supplied `uncompressed_size` exceeds the per-frame ceiling. A peer
    /// can otherwise drive an arbitrarily large `Vec::with_capacity` through
    /// the `expires_in`-style allocation hint and exhaust the process heap
    /// without ever producing a real frame. Capped at
    /// [`crate::MAX_FRAME_SIZE`] (5 MiB), matching the wire ceiling
    /// the frame codec already enforces on the outer length.
    #[error("uncompressed_size {got} exceeds frame ceiling {ceiling}")]
    UncompressedSizeTooLarge {
        /// Broker-advertised uncompressed size.
        got: usize,
        /// Ceiling the call enforced — [`crate::MAX_FRAME_SIZE`] for [`decompress`], the
        /// caller's remaining budget (never above it) for [`decompress_within`].
        ceiling: usize,
    },
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
/// Every codec output path is bounded by the broker-advertised size plus one
/// validation byte. Zlib uses a borrowed input buffer; zstd decodes into a
/// fixed destination with a window capped from the same advertised size.
///
/// # Errors
/// Codec-specific decode failures, plus [`CompressionError::SizeMismatch`] if the decompressed
/// size disagrees with the broker-stamped value (which would indicate a tampered payload or a
/// broker bug), plus [`CompressionError::UncompressedSizeTooLarge`] when the codec's own
/// length header (Snappy / LZ4 size-prepended frames) or its actual decoded output exceeds
/// [`crate::MAX_FRAME_SIZE`].
pub fn decompress(
    kind: CompressionKind,
    ciphertext: &[u8],
    uncompressed_size: usize,
) -> Result<Bytes, CompressionError> {
    // Cap the broker-controlled `uncompressed_size` BEFORE allocating. Without
    // this guard a peer can advertise e.g. `uncompressed_size = u32::MAX` (4
    // GiB) and drive `Vec::with_capacity(uncompressed_size)` in the Zlib path
    // (or the `lz4_flex::decompress` output-buffer pre-allocation) to exhaust
    // the process heap, never producing a real frame. The outer wire codec
    // already rejects frames larger than `MAX_FRAME_SIZE`; cap the inflated
    // payload at the same ceiling.
    if uncompressed_size > crate::MAX_FRAME_SIZE {
        return Err(CompressionError::UncompressedSizeTooLarge {
            got: uncompressed_size,
            ceiling: crate::MAX_FRAME_SIZE,
        });
    }
    let ceiling = crate::MAX_FRAME_SIZE;
    let validation_len = uncompressed_size
        .checked_add(crate::DECOMPRESSION_VALIDATION_SLACK)
        .ok_or(CompressionError::UncompressedSizeTooLarge {
            got: usize::MAX,
            ceiling,
        })?;
    match kind {
        CompressionKind::None => Ok(Bytes::copy_from_slice(ciphertext)),
        CompressionKind::Lz4 => {
            let mut decompressed = vec![0_u8; validation_len];
            let len = lz4_flex::block::decompress_into(ciphertext, &mut decompressed)
                .map_err(|e| CompressionError::Lz4(e.to_string()))?;
            decompressed.truncate(len);
            verify_size(&decompressed, uncompressed_size)?;
            Ok(Bytes::from(decompressed))
        }
        CompressionKind::Zlib => {
            let mut decoder = flate2::bufread::ZlibDecoder::new(ciphertext);
            let out = read_bounded(&mut decoder, validation_len)?;
            verify_size(&out, uncompressed_size)?;
            Ok(Bytes::from(out))
        }
        CompressionKind::Zstd => {
            let mut decoder = zstd::bulk::Decompressor::new()
                .map_err(|e| CompressionError::Zstd(e.to_string()))?;
            decoder
                .window_log_max(zstd_window_log(uncompressed_size))
                .map_err(|e| CompressionError::Zstd(e.to_string()))?;
            let mut out = vec![0_u8; validation_len];
            let written = decoder
                .decompress_to_buffer(ciphertext, out.as_mut_slice())
                .map_err(|e| CompressionError::Zstd(e.to_string()))?;
            out.truncate(written);
            verify_size(&out, uncompressed_size)?;
            Ok(Bytes::from(out))
        }
        CompressionKind::Snappy => {
            // The Snappy block format embeds the uncompressed size in its
            // header. Read the header BEFORE allocating so a malicious frame
            // that claims e.g. 4 GiB cannot drive a multi-GiB
            // `Vec::with_capacity` in the codec's own `decompress_vec`. The
            // codec then decodes into a fixed-size buffer.
            use snap::raw::{Decoder, decompress_len};
            let announced =
                decompress_len(ciphertext).map_err(|e| CompressionError::Snappy(e.to_string()))?;
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
            let mut buf = vec![0u8; uncompressed_size];
            let n = Decoder::new()
                .decompress(ciphertext, &mut buf)
                .map_err(|e| CompressionError::Snappy(e.to_string()))?;
            buf.truncate(n);
            verify_size(&buf, uncompressed_size)?;
            Ok(Bytes::from(buf))
        }
    }
}

fn zstd_window_log(uncompressed_size: usize) -> u32 {
    let window = uncompressed_size
        .max(crate::ZSTD_MIN_WINDOW_SIZE)
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

/// Upper bound on how far one LZ4 block can inflate. Every input byte adds at most 255
/// output bytes — a match-length extension byte encodes 255 more bytes of the match — so a
/// block of `n` bytes never decodes to more than `255 × n`. Used to size the output buffer
/// when the wire does not name the uncompressed size (see [`decompress_within`]).
const LZ4_MAX_INFLATE_RATIO: usize = 255;

/// Decompress `ciphertext` when the wire does NOT carry its exact uncompressed size, refusing
/// to produce more than `limit` bytes (itself capped at [`crate::MAX_FRAME_SIZE`]).
///
/// The one caller is the batch layout magnetar's producer wrote up to and including 1.7.2
/// (ADR-0112): it compressed every batch member on its own and stamped the batch's
/// `uncompressed_size` with the sum of the COMPRESSED member sizes, so no field names any
/// member's real size. [`decompress`] cannot read such a member — its exact-size check is what
/// rejects it — so this variant keeps the decompression-bomb ceiling and drops only the
/// exact-size verification. The ceiling still applies before any codec allocates: Snappy's
/// announced length and LZ4's output buffer are both bounded by `limit`, and the two streaming
/// codecs stop reading one byte past it.
///
/// # Errors
/// Codec-specific decode failures, plus [`CompressionError::UncompressedSizeTooLarge`] when the
/// decoded (or, for Snappy, announced) size exceeds `limit`.
pub fn decompress_within(
    kind: CompressionKind,
    ciphertext: &[u8],
    limit: usize,
) -> Result<Bytes, CompressionError> {
    let limit = limit.min(crate::MAX_FRAME_SIZE);
    let mut out = match kind {
        CompressionKind::None => ciphertext.to_vec(),
        CompressionKind::Lz4 => {
            let capacity = limit.min(ciphertext.len().saturating_mul(LZ4_MAX_INFLATE_RATIO));
            lz4_flex::decompress(ciphertext, capacity)
                .map_err(|e| CompressionError::Lz4(e.to_string()))?
        }
        CompressionKind::Zlib => read_within(flate2::read::ZlibDecoder::new(ciphertext), limit)?,
        CompressionKind::Zstd => zstd::stream::Decoder::new(ciphertext)
            .and_then(|decoder| read_within(decoder, limit))
            .map_err(|e| CompressionError::Zstd(e.to_string()))?,
        CompressionKind::Snappy => {
            let announced = snap::raw::decompress_len(ciphertext)
                .map_err(|e| CompressionError::Snappy(e.to_string()))?;
            ensure_within(announced, limit)?;
            snap::raw::Decoder::new()
                .decompress_vec(ciphertext)
                .map_err(|e| CompressionError::Snappy(e.to_string()))?
        }
    };
    ensure_within(out.len(), limit)?;
    // LZ4's buffer was sized from `limit` (up to 255 x the input) and the streaming codecs
    // grow theirs by doubling. `Bytes::from` keeps the whole allocation alive for as long as
    // the payload is queued, so hand back only the decoded bytes (ADR-0112).
    out.shrink_to_fit();
    Ok(Bytes::from(out))
}

/// Drain `reader` into a buffer, reading at most one byte past `limit` so an over-long
/// stream is detectable by [`ensure_within`] instead of being silently truncated.
fn read_within(reader: impl std::io::Read, limit: usize) -> std::io::Result<Vec<u8>> {
    use std::io::Read as _;
    let mut out = Vec::new();
    reader
        .take(limit.saturating_add(1) as u64)
        .read_to_end(&mut out)?;
    Ok(out)
}

/// Reject a decoded size above `ceiling`.
fn ensure_within(got: usize, ceiling: usize) -> Result<(), CompressionError> {
    if got > ceiling {
        return Err(CompressionError::UncompressedSizeTooLarge { got, ceiling });
    }
    Ok(())
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
    use crate::pb;

    #[test]
    fn every_codec_round_trips_and_maps_from_wire() {
        let input = b"tokio scalable compression";
        for (wire, kind) in [
            (pb::CompressionType::None, CompressionKind::None),
            (pb::CompressionType::Lz4, CompressionKind::Lz4),
            (pb::CompressionType::Zlib, CompressionKind::Zlib),
            (pb::CompressionType::Zstd, CompressionKind::Zstd),
            (pb::CompressionType::Snappy, CompressionKind::Snappy),
        ] {
            assert_eq!(super::kind_from_pb(wire), kind);
            let encoded = compress(kind, input).expect("encode codec fixture");
            assert_eq!(
                decompress(kind, &encoded, input.len()).expect("decode codec fixture"),
                input.as_slice()
            );
        }
    }

    #[test]
    fn malformed_sizes_and_payloads_are_bounded() {
        assert!(matches!(
            decompress(CompressionKind::None, b"ignored", crate::MAX_FRAME_SIZE + 1,),
            Err(super::CompressionError::UncompressedSizeTooLarge { .. })
        ));
        for kind in [
            CompressionKind::Lz4,
            CompressionKind::Zlib,
            CompressionKind::Zstd,
            CompressionKind::Snappy,
        ] {
            assert!(
                decompress(kind, b"invalid", 1).is_err(),
                "kind={kind:?} must reject malformed input"
            );
        }
        let oversized = vec![0_u8; crate::MAX_FRAME_SIZE + 1];
        let encoded = compress(CompressionKind::Snappy, &oversized).expect("encode oversized");
        assert!(matches!(
            decompress(CompressionKind::Snappy, &encoded, crate::MAX_FRAME_SIZE,),
            Err(super::CompressionError::UncompressedSizeTooLarge { .. })
        ));
    }

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
        let compressed = compress(CompressionKind::Zlib, &payload).unwrap();
        // Lie about the uncompressed size — bound is satisfied but verify_size rejects.
        let err = decompress(CompressionKind::Zlib, &compressed, 999).expect_err("mismatch");
        assert!(matches!(err, super::CompressionError::SizeMismatch { .. }));
    }

    #[test]
    #[allow(clippy::match_same_arms)]
    fn every_codec_stops_at_the_advertised_size_plus_validation_slack() {
        let payload = vec![0_u8; 4096];
        for kind in [
            CompressionKind::Lz4,
            CompressionKind::Zlib,
            CompressionKind::Zstd,
            CompressionKind::Snappy,
        ] {
            let compressed = compress(kind, &payload).expect("compress overexpansion fixture");
            let error = decompress(kind, &compressed, 1).expect_err("overexpansion rejected");
            match (kind, error) {
                (CompressionKind::Lz4, super::CompressionError::Lz4(_)) => {}
                (
                    CompressionKind::Zlib,
                    super::CompressionError::SizeMismatch {
                        got: 2,
                        expected: 1,
                    },
                ) => {}
                (CompressionKind::Zstd, super::CompressionError::Zstd(_)) => {}
                (
                    CompressionKind::Snappy,
                    super::CompressionError::SizeMismatch {
                        got: 4096,
                        expected: 1,
                    },
                ) => {}
                (_, other) => panic!("kind={kind:?} returned unexpected error {other:?}"),
            }
        }
    }

    /// A broker (or a tampered MITM) advertising an outlandish
    /// `uncompressed_size` must be rejected BEFORE the codec allocates a
    /// multi-GiB output buffer. The Zlib path used `Vec::with_capacity(
    /// uncompressed_size)` directly, so without the cap a peer could drive
    /// the process to OOM with a tiny payload that claims `u32::MAX` bytes
    /// of plaintext. The ceiling matches `crate::MAX_FRAME_SIZE`,
    /// which the frame codec already enforces on the outer wire length.
    #[test]
    fn unchecked_uncompressed_size_is_rejected_before_allocation() {
        // A vanishingly small Zlib body — its actual plaintext is empty, but
        // we lie about `uncompressed_size` so the allocator-arming pre-cap is
        // the only thing standing between the test and an OOM.
        let tiny = compress(CompressionKind::Zlib, b"").expect("zlib empty compress");
        let huge = u32::MAX as usize;
        let err = decompress(CompressionKind::Zlib, &tiny, huge)
            .expect_err("u32::MAX uncompressed_size must be rejected before allocation");
        match err {
            super::CompressionError::UncompressedSizeTooLarge { got, ceiling } => {
                assert_eq!(got, huge);
                assert_eq!(ceiling, crate::MAX_FRAME_SIZE);
            }
            other => panic!("expected UncompressedSizeTooLarge, got {other:?}"),
        }

        // The cap applies uniformly across codecs — Zstd / LZ4 / Snappy must
        // not be a bypass route. `uncompressed_size = MAX_FRAME_SIZE + 1` is
        // the smallest over-cap value; using it locks the boundary check.
        let over = crate::MAX_FRAME_SIZE + 1;
        for kind in [
            CompressionKind::Zlib,
            CompressionKind::Zstd,
            CompressionKind::Lz4,
            CompressionKind::Snappy,
        ] {
            // The body content does not matter — the cap rejects before any
            // codec call. Passing an empty slice keeps the test cheap.
            let err = decompress(kind, &[], over).expect_err("over-cap rejected");
            assert!(
                matches!(
                    err,
                    super::CompressionError::UncompressedSizeTooLarge { .. }
                ),
                "kind={kind:?} expected UncompressedSizeTooLarge, got {err:?}"
            );
        }

        // Right at the cap, the pre-allocation gate must NOT fire — the
        // codec's own decode path then takes over. A real payload of exactly
        // 64 bytes round-trips successfully through Zstd; this lets us
        // distinguish "guard rejected" from "guard accepted, codec verified".
        let payload = vec![0xABu8; 64];
        let compressed = compress(CompressionKind::Zstd, &payload).expect("zstd compress");
        let out = decompress(CompressionKind::Zstd, &compressed, payload.len())
            .expect("under-cap legit payload round-trips");
        assert_eq!(&out[..], &payload[..]);
    }

    /// Decompression-bomb defence (CWE-409). A tiny Zstd ciphertext whose
    /// `uncompressed_size` header advertises **1 byte** but whose actual
    /// decoded output would be 10 MiB of zeroes must NOT allocate the full
    /// 10 MiB before the size check fires. The R1 cap on `uncompressed_size`
    /// alone was insufficient because `zstd::stream::decode_all` ignored the
    /// header entirely — it just expanded the ciphertext into a fresh `Vec`
    /// whatever the announced size. With the R2 streaming decoder, the
    /// `take(MAX_FRAME_SIZE + 1)` cap stops the decode at the wire ceiling
    /// regardless of the lying header.
    ///
    /// The test asserts an error is surfaced (no panic, no OOM); either
    /// `UncompressedSizeTooLarge` (the bounded-decode path tripped its
    /// post-check) or `SizeMismatch` (the announced 1 byte does not match
    /// the actual decode) is acceptable — both prove the cap engaged
    /// without paying the 10 MiB allocation cost. A "Got OK" outcome is
    /// the regression we are guarding against.
    #[test]
    #[allow(clippy::match_same_arms)]
    fn zstd_decompression_bomb_is_bounded() {
        // 10 MiB of zeroes — highly compressible. Real ciphertext is a few
        // hundred bytes; without the bound the decoder would happily produce
        // the full 10 MiB.
        let bomb_plaintext = vec![0u8; 10 * 1024 * 1024];
        let bomb_ciphertext = compress(CompressionKind::Zstd, &bomb_plaintext).expect("compress");
        // Sanity: the compressed form is much smaller than the wire ceiling.
        assert!(
            bomb_ciphertext.len() < crate::MAX_FRAME_SIZE,
            "bomb ciphertext should be small (real attack shape); got {} bytes",
            bomb_ciphertext.len()
        );

        // Lie about uncompressed_size — claim a single byte. With R1 alone
        // the cap accepted this (1 ≤ MAX_FRAME_SIZE) and `decode_all`
        // unconditionally expanded the 10 MiB without checking the header.
        let lying = 1_usize;
        let err = decompress(CompressionKind::Zstd, &bomb_ciphertext, lying)
            .expect_err("bomb must be rejected, never accepted into a 10 MiB allocation");
        match err {
            super::CompressionError::UncompressedSizeTooLarge { got, ceiling } => {
                assert!(
                    got > ceiling,
                    "over-cap branch fired with got={got} ceiling={ceiling}"
                );
                assert_eq!(ceiling, crate::MAX_FRAME_SIZE);
            }
            super::CompressionError::SizeMismatch {
                got: _,
                expected: _,
            } => {
                // Acceptable: the bounded decode pulled some bytes (well under
                // the ceiling) and the post-check spotted the mismatch with the
                // lying 1-byte advertisement. The point is no 10 MiB alloc.
            }
            super::CompressionError::Zstd(_) => {
                // Fixed-destination zstd rejects before producing an oversized
                // output buffer.
            }
            other => {
                panic!("expected a bounded zstd bomb rejection, got {other:?}")
            }
        }

        // Same shape against Snappy: a highly-compressible block whose header
        // honestly announces 10 MiB must be rejected at the header check,
        // not after a multi-GiB alloc.
        let snappy_bomb =
            compress(CompressionKind::Snappy, &bomb_plaintext).expect("snappy compress");
        let err = decompress(CompressionKind::Snappy, &snappy_bomb, lying)
            .expect_err("snappy 10 MiB bomb must be rejected");
        match err {
            super::CompressionError::UncompressedSizeTooLarge { got, ceiling } => {
                assert!(got > ceiling, "got={got} ceiling={ceiling}");
            }
            super::CompressionError::SizeMismatch { .. } => {
                // Also acceptable — the bounded codec returned its real size,
                // which disagrees with the lying advertisement.
            }
            other => panic!("expected bounded error for snappy bomb, got {other:?}"),
        }
    }
}
