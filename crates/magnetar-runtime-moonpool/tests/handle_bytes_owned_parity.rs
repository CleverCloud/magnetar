// SPDX-License-Identifier: Apache-2.0

//! ADR-0039 wave 3 — runtime integration test for
//! `Connection::handle_bytes_owned` (layer (c) of the ADR-0024
//! four-layer policy on the moonpool engine; 1:1 mirror of
//! `magnetar-runtime-tokio/tests/handle_bytes_owned_parity.rs`).

use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, encode_command, pb};
use magnetar_runtime_moonpool::ConnectionShared;

mod common;
use common::handshake_response_bytes;

#[test]
fn handle_bytes_owned_completes_handshake() {
    let shared = Arc::new(ConnectionShared::new(ConnectionConfig::default()));
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
        "moonpool engine: handshake completes via handle_bytes_owned"
    );
}

#[test]
fn handle_bytes_owned_matches_handle_bytes_for_split_frame() {
    let legacy = Arc::new(ConnectionShared::new(ConnectionConfig::default()));
    let owned = Arc::new(ConnectionShared::new(ConnectionConfig::default()));
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
        "moonpool engine: both entries reach the same is_connected state"
    );
    assert!(owned.inner.lock().is_connected());
}

#[test]
fn handle_bytes_owned_handles_two_frames_back_to_back() {
    let shared = Arc::new(ConnectionShared::new(ConnectionConfig::default()));
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
    assert!(shared.inner.lock().is_connected());
}
