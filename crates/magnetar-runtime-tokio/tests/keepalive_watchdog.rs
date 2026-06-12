// SPDX-License-Identifier: Apache-2.0

// The keepalive-watchdog scenario reads best as one linear `fn` that walks
// the synthetic timer + byte sequence; splitting it would obscure the wedge
// it pins. Accept the length.
#![allow(clippy::too_many_lines, clippy::expect_used)]

//! Chaos scenario (ADR-0058): the **progress-based keepalive watchdog** —
//! tokio mirror of `magnetar-runtime-moonpool/tests/keepalive_watchdog.rs`.
//!
//! A bit-flip on the un-checksummed outer `total_size` length prefix that
//! lands on a plausible-but-unreachable value makes
//! [`magnetar_proto::frame::peek_full_frame_len`] return `Incomplete`
//! forever, so a desynced-but-*chatty* socket dribbles bytes that never
//! frame. Before ADR-0058 the keepalive baseline refreshed per *raw inbound
//! chunk*, so this chatter reset the watchdog indefinitely and the connection
//! wedged; and the watchdog only ever re-pinged, never failing a dead socket.
//!
//! The fix refreshes the baseline per *decoded frame* and adds an
//! outstanding-ping flag so the **second** consecutive missed keepalive
//! interval escalates to `mark_disconnected()` → [`HandshakeState::Failed`],
//! which the driver reads as `should_close` → supervised reconnect.
//!
//! The proto state machine is engine-agnostic, so the tokio mirror drives the
//! same synthetic byte + `Instant` timeline through the tokio
//! [`ConnectionShared`] wrapper and asserts the identical terminal state,
//! keeping `cargo run -p xtask -- check-runtime-test-parity` 1:1 (ADR-0024).
//! On the tokio engine `now: Instant` is host-supplied in production; here we
//! pass explicit synthetic instants so the watchdog timeline is exact and
//! seed-free.

mod common;

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, HandshakeState};
use magnetar_runtime_tokio::ConnectionShared;

use crate::common::handshake_response_bytes;

/// Short keepalive interval so the synthetic timeline stays compact.
const KEEPALIVE: Duration = Duration::from_secs(1);

/// A desynced chunk: a 4-byte big-endian `total_size` prefix announcing a
/// plausible 1024-byte frame, followed by far fewer bytes. `peek_full_frame_len`
/// parks (`Incomplete`) — no frame ever decodes from it.
fn desync_chunk() -> BytesMut {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&1024u32.to_be_bytes());
    buf.extend_from_slice(b"chatty-but-never-a-whole-frame");
    buf
}

/// Drive a freshly-handshaked tokio connection's keepalive watchdog across a
/// chatty desync, returning the terminal handshake state.
fn run_chatty_desync_wedge() -> HandshakeState {
    let t0 = Instant::now();
    let cfg = ConnectionConfig {
        keepalive_interval: KEEPALIVE,
        ..ConnectionConfig::default()
    };
    let shared = ConnectionShared::new(cfg);
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("connected");
        let _ = conn.poll_event();
        assert!(conn.is_connected(), "fixture must reach Connected");
        let _ = conn.poll_transmit(); // drain Connect frame
    }

    // Chatty desync inside the first keepalive interval — must NOT refresh the
    // watchdog baseline (the regression these chunks used to trigger).
    {
        let mut conn = shared.inner.lock();
        for tick in 1..=4u32 {
            let at = t0 + KEEPALIVE / 2 * tick;
            conn.handle_bytes(at, &desync_chunk())
                .expect("a desynced chunk parks, it is not a hard error");
            assert!(
                conn.is_connected(),
                "a chatty desync chunk must not, by itself, change handshake state",
            );
        }
    }

    // First missed interval → ping, arm outstanding, stay Connected.
    {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(t0 + KEEPALIVE + Duration::from_millis(1));
        assert!(
            conn.is_connected(),
            "the first missed interval only pings; it must not fail the connection",
        );
        assert!(
            !conn.poll_transmit().is_empty(),
            "the first missed interval must put a keepalive ping on the wire",
        );
    }

    // More chatty desync between the intervals — the regression trap.
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0 + KEEPALIVE + Duration::from_millis(500), &desync_chunk())
            .expect("desync chunk parks");
        assert!(conn.is_connected(), "still chatty, still connected for now");
    }

    // Second missed interval with no decoded inbound frame → escalate to Failed.
    {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(t0 + KEEPALIVE * 2 + Duration::from_millis(2));
        conn.state()
    }
}

/// A chatty desync must escalate to `Failed` on the second missed keepalive
/// interval. Mirror of the moonpool single-run test.
#[tokio::test(flavor = "current_thread")]
async fn chatty_desync_escalates_keepalive_watchdog_to_failed() {
    let terminal = run_chatty_desync_wedge();
    assert_eq!(
        terminal,
        HandshakeState::Failed,
        "two missed keepalive intervals over a chatty desync must fail the \
         connection (ADR-0058); a chatty socket must not keep the watchdog \
         alive forever (issues #187, #221)",
    );
}

/// The escalation is deterministic — re-running the scenario must reach the
/// same terminal `Failed` state every time. Mirror of the moonpool seed-sweep
/// test (the tokio engine has no seed knob; repeating the run is the
/// determinism guard that keeps the 1:1 test count balanced under ADR-0024).
#[tokio::test(flavor = "current_thread")]
async fn chatty_desync_watchdog_escalation_is_deterministic() {
    for run in 0..16u32 {
        let terminal = run_chatty_desync_wedge();
        assert_eq!(
            terminal,
            HandshakeState::Failed,
            "run {run}: chatty-desync keepalive escalation must reach Failed \
             deterministically (ADR-0058)",
        );
    }
}
