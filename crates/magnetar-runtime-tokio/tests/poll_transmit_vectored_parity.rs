// SPDX-License-Identifier: Apache-2.0

//! ADR-0039 wave 1.1 — runtime integration test for
//! `Connection::poll_transmit_vectored` (layer (b) of the ADR-0024
//! four-layer policy on the tokio engine).
//!
//! Today the entry point always returns `Transmit::Contiguous(slice)`
//! whose bytes are identical to what the legacy `poll_transmit` path
//! produces. The test drives the same `ConnectionConfig::default()`
//! handshake against two `ConnectionShared` instances under the tokio
//! engine's locking discipline, then asserts byte equivalence. Mirrored
//! 1:1 by `magnetar-runtime-moonpool/tests/poll_transmit_vectored_parity.rs`.
//! Wave 1.2 will start emitting `Vectored` for producer batches; this
//! file gains the `Vectored` arm coverage at that point.

use std::sync::Arc;
use std::time::Instant;

use magnetar_proto::{ConnectionConfig, Transmit};
use magnetar_runtime_tokio::ConnectionShared;

mod common;
use common::handshake_response_bytes;

/// Drive a `ConnectionShared` to `Connected` so the outbound buffer
/// carries the same post-handshake payload as a fresh handshake's
/// pending Connect frame.
fn handshake_complete_shared() -> Arc<ConnectionShared> {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("connected");
    }
    shared
}

#[test]
fn poll_transmit_vectored_matches_poll_transmit_post_handshake() {
    let legacy_shared = ConnectionShared::new(ConnectionConfig::default());
    let vectored_shared = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = legacy_shared.inner.lock();
        conn.begin_handshake().expect("handshake legacy");
    }
    {
        let mut conn = vectored_shared.inner.lock();
        conn.begin_handshake().expect("handshake vectored");
    }
    let legacy_bytes = {
        let mut conn = legacy_shared.inner.lock();
        conn.poll_transmit()
    };
    let vectored_owned = {
        let mut conn = vectored_shared.inner.lock();
        match conn.poll_transmit_vectored() {
            Transmit::Contiguous(slice) => slice.to_vec(),
            Transmit::Vectored(_) => panic!("wave 1.1 must not emit Vectored — that is wave 1.2"),
        }
    };
    assert_eq!(
        &vectored_owned[..],
        &legacy_bytes[..],
        "tokio engine: poll_transmit_vectored::Contiguous bytes must match poll_transmit bytes for the pending Connect frame"
    );
    assert!(
        !vectored_owned.is_empty(),
        "tokio engine: handshake Connect frame is non-empty"
    );
}

#[test]
fn poll_transmit_vectored_is_empty_after_drain() {
    let shared = handshake_complete_shared();
    // Drain any post-handshake bytes via the legacy path so the buffer
    // is empty before we test the vectored entry point's empty case.
    {
        let mut conn = shared.inner.lock();
        let _ = conn.poll_transmit();
    }
    let mut conn = shared.inner.lock();
    match conn.poll_transmit_vectored() {
        Transmit::Contiguous(slice) => {
            assert!(
                slice.is_empty(),
                "tokio engine: post-drain poll_transmit_vectored::Contiguous must be empty"
            );
        }
        Transmit::Vectored(_) => panic!("wave 1.1 must not emit Vectored"),
    }
}
