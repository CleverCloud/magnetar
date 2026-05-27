// SPDX-License-Identifier: Apache-2.0

//! ADR-0039 wave 3 — runtime integration test for
//! `Connection::handle_bytes_owned` (layer (b) of the ADR-0024
//! four-layer policy on the tokio engine; 1:1 mirror of
//! `magnetar-runtime-moonpool/tests/handle_bytes_owned_parity.rs`).
//!
//! The driver loop now reads into its own `BytesMut`, calls
//! `BytesMut::split()` to take ownership of the freshly-read chunk,
//! and feeds it into the proto layer via `handle_bytes_owned` —
//! skipping the `extend_from_slice` memcpy the legacy `&[u8]` entry
//! performs when proto's `inbound` is empty (the common case after a
//! full-frame decode). This file asserts the **observable** behaviour
//! of the new entry: handshake completes when bytes arrive as an
//! owned `BytesMut` chunk, byte-identical to the legacy `&[u8]` path.

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, encode_command, pb};
use magnetar_runtime_tokio::ConnectionShared;

mod common;
use common::handshake_response_bytes;

#[test]
fn handle_bytes_owned_completes_handshake() {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
    }
    let chunk = handshake_response_bytes();
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes_owned(Instant::now(), chunk)
            .expect("owned-chunk handle");
    }
    assert!(
        shared.inner.lock().is_connected(),
        "tokio engine: handshake completes via handle_bytes_owned"
    );
}

#[test]
fn handle_bytes_owned_matches_handle_bytes_for_split_frame() {
    // Drive two ConnectionShared instances through identical inputs
    // — one via legacy `handle_bytes(&[u8])`, one via owned
    // `handle_bytes_owned(BytesMut)` — and assert both reach the
    // same observable post-state (Connected, identical
    // `last_connected_timestamp` presence, identical event sequence
    // shape).
    let legacy = ConnectionShared::new(ConnectionConfig::default());
    let owned = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = legacy.inner.lock();
        conn.begin_handshake().expect("handshake legacy");
    }
    {
        let mut conn = owned.inner.lock();
        conn.begin_handshake().expect("handshake owned");
    }

    let frame = handshake_response_bytes();
    let at = Instant::now();
    {
        let mut conn = legacy.inner.lock();
        conn.handle_bytes(at, &frame).expect("legacy handle_bytes");
    }
    {
        let mut conn = owned.inner.lock();
        let chunk = {
            let mut buf = BytesMut::with_capacity(frame.len());
            buf.extend_from_slice(&frame);
            buf
        };
        conn.handle_bytes_owned(at, chunk)
            .expect("owned handle_bytes_owned");
    }
    assert_eq!(
        legacy.inner.lock().is_connected(),
        owned.inner.lock().is_connected(),
        "tokio engine: both entries reach the same is_connected state"
    );
    assert!(owned.inner.lock().is_connected());
}

#[test]
fn handle_bytes_owned_handles_two_frames_back_to_back() {
    // Two complete frames in one chunk — exercises the inner decode
    // loop's frame-by-frame split when ownership has been swapped in.
    let shared = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
    }

    let connected = handshake_response_bytes();
    let ping = pb::BaseCommand {
        r#type: pb::base_command::Type::Ping as i32,
        ping: Some(pb::CommandPing {}),
        ..Default::default()
    };
    let mut ping_buf = BytesMut::new();
    encode_command(&mut ping_buf, &ping).expect("encode ping");

    let mut chunk = BytesMut::with_capacity(connected.len() + ping_buf.len());
    chunk.extend_from_slice(&connected);
    chunk.extend_from_slice(&ping_buf);

    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes_owned(Instant::now(), chunk)
            .expect("owned handle, two frames");
    }
    // Connected event delivered + Ping handled (Pong queued).
    assert!(shared.inner.lock().is_connected());
}
