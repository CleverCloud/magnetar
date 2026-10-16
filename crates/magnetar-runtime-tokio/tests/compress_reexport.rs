// SPDX-License-Identifier: Apache-2.0

//! The codecs moved from `magnetar_runtime_tokio::compress` into `magnetar_proto::compress` with
//! ADR-0112 — a batched entry has to be decompressed as ONE body before
//! `ConsumerState::deliver` can split it, so the sans-io core needs them. The tokio crate keeps
//! the old path as a re-export, and this pins that every public item an existing caller could
//! have named still resolves through it and still round-trips.
//!
//! Tokio preserves its public codec path; Moonpool delegates its private inbound path to the same
//! core.

use magnetar_proto::pb;
use magnetar_proto::types::CompressionKind;
use magnetar_runtime_tokio::CompressionError;
use magnetar_runtime_tokio::compress::{compress, decompress, decompress_within, kind_from_pb};

#[test]
fn compress_module_reexport_keeps_its_public_path() {
    let payload = b"re-exported codec path|".repeat(8);
    for (wire, kind) in [
        (pb::CompressionType::Lz4, CompressionKind::Lz4),
        (pb::CompressionType::Zlib, CompressionKind::Zlib),
        (pb::CompressionType::Zstd, CompressionKind::Zstd),
        (pb::CompressionType::Snappy, CompressionKind::Snappy),
    ] {
        assert_eq!(kind_from_pb(wire), kind);
        let compressed = compress(kind, &payload).expect("compress");
        let plain = decompress(kind, &compressed, payload.len()).expect("decompress");
        assert_eq!(plain.as_ref(), payload.as_slice(), "{kind:?}");
        let within = decompress_within(kind, &compressed, payload.len()).expect("within");
        assert_eq!(within.as_ref(), payload.as_slice(), "{kind:?}");
    }
    let err: CompressionError = decompress(CompressionKind::Zstd, b"not zstd", 8)
        .expect_err("garbage is rejected through the re-exported path too");
    assert!(matches!(err, CompressionError::Zstd(_)), "{err:?}");
}

#[test]
fn uncompressed_codec_preserves_bytes_and_validates_the_size_ceiling() {
    assert_eq!(
        kind_from_pb(pb::CompressionType::None),
        CompressionKind::None
    );
    let payload = b"uncompressed segment entry";
    assert_eq!(
        decompress(CompressionKind::None, payload, payload.len())
            .expect("plain")
            .as_ref(),
        payload
    );
    assert!(matches!(
        decompress(
            CompressionKind::None,
            payload,
            magnetar_proto::MAX_FRAME_SIZE + 1
        ),
        Err(CompressionError::UncompressedSizeTooLarge { .. })
    ));
}

#[test]
fn malformed_compressed_payloads_and_size_mismatches_are_rejected() {
    for kind in [
        CompressionKind::Lz4,
        CompressionKind::Zlib,
        CompressionKind::Zstd,
        CompressionKind::Snappy,
    ] {
        assert!(
            decompress(kind, b"invalid", 1).is_err(),
            "{kind:?} accepted malformed bytes"
        );
        let payload = b"segment size mismatch";
        let encoded = compress(kind, payload).expect("encode fixture");
        assert!(
            decompress(kind, &encoded, payload.len() + 1).is_err(),
            "{kind:?} accepted a wrong size"
        );
    }
}

#[test]
fn each_public_codec_rejects_expansion_beyond_the_advertised_and_legacy_limits() {
    let payload = vec![0_u8; 4096];
    for kind in [
        CompressionKind::Lz4,
        CompressionKind::Zlib,
        CompressionKind::Zstd,
        CompressionKind::Snappy,
    ] {
        let encoded = compress(kind, &payload).expect("encode compressed segment");
        assert!(
            decompress(kind, &encoded, 1).is_err(),
            "{kind:?} accepted overexpansion"
        );
        assert!(
            decompress_within(kind, &encoded, 1).is_err(),
            "{kind:?} exceeded the legacy output bound"
        );
    }
}
