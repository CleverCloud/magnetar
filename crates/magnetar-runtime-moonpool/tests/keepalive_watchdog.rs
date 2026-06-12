// SPDX-License-Identifier: Apache-2.0

// The keepalive-watchdog scenario reads best as one linear `fn` that walks
// the synthetic timer + byte sequence; splitting it would obscure the wedge
// it pins. Accept the length.
#![allow(clippy::too_many_lines, clippy::expect_used)]

//! Chaos scenario (ADR-0058): the **progress-based keepalive watchdog**.
//!
//! A bit-flip on the un-checksummed outer `total_size` length prefix that
//! lands on a plausible-but-unreachable value (`0 < N < MAX_FRAME_SIZE` whose
//! promised bytes never arrive) makes
//! [`magnetar_proto::frame::peek_full_frame_len`] return `Incomplete`
//! forever. A desynced-but-*chatty* socket therefore keeps dribbling bytes
//! that never frame. Before ADR-0058 the keepalive baseline was refreshed per
//! *raw inbound chunk*, so this chatter reset the watchdog indefinitely and
//! the connection wedged — alive on the wire, dead to the application, never
//! reconnecting. Worse, the watchdog only ever *re-pinged*: even a silent
//! (non-chatty) half-open socket was pinged forever, never failed.
//!
//! The fix moves the baseline refresh to per *decoded frame* and adds an
//! outstanding-ping flag so the **second** consecutive missed keepalive
//! interval escalates to `mark_disconnected()` → [`HandshakeState::Failed`].
//! The driver reads `Failed` as `should_close` and hands the connection to
//! the supervisor for a reconnect.
//!
//! Why moonpool and not e2e: a real broker answers `PING` with `PONG` and
//! never corrupts its own length prefix on a single TCP stream, so the wedge
//! is unreachable against a live container. The only way to pin "a chatty
//! desync must not keep the watchdog alive, and two missed intervals fail the
//! connection" is to drive the sans-io state machine directly with synthetic
//! bytes + synthetic `Instant`s — exactly what the moonpool
//! state-machine-only harness exists for.
//!
//! Target seeds: `0xa643e7ad4c47c32e`, `0x2c60abc681532cd6` (issues #187,
//! #221). The scenario is deterministic (no engine RNG), so the seed only
//! drives the `sweep_seeds` expansion below; the assertion holds for every
//! seed, which is the point — the watchdog is not seed-dependent.
//!
//! Pairs 1:1 with the tokio mirror
//! `magnetar-runtime-tokio/tests/keepalive_watchdog.rs` per ADR-0024.

mod common;

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, HandshakeState};

use crate::common::{handshake_complete_shared_with_config, sweep_seeds};

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

/// Drive a freshly-handshaked connection's keepalive watchdog across a chatty
/// desync, returning the terminal handshake state. This is the core scenario
/// the seed sweep replays.
fn run_chatty_desync_wedge() -> HandshakeState {
    let t0 = Instant::now();
    let cfg = ConnectionConfig {
        keepalive_interval: KEEPALIVE,
        ..ConnectionConfig::default()
    };
    let shared = handshake_complete_shared_with_config(t0, cfg);

    // The socket is desynced and *chatty*: feed bytes that never frame, on a
    // cadence faster than the keepalive interval. Pre-ADR-0058 each chunk
    // refreshed `last_activity`, so the deadline kept sliding forward and the
    // watchdog never fired — the wedge this test pins.
    {
        let mut conn = shared.inner.lock();
        for tick in 1..=4u32 {
            let at = t0 + KEEPALIVE / 2 * tick; // every 500ms, inside the 1s interval
            conn.handle_bytes(at, &desync_chunk())
                .expect("a desynced chunk parks, it is not a hard error");
            // Each chunk must NOT have advanced the connection out of Connected
            // and must NOT have framed anything.
            assert!(
                conn.is_connected(),
                "a chatty desync chunk must not, by itself, change handshake state",
            );
        }
        // The chatty chunks must NOT have kept the watchdog baseline fresh:
        // `last_activity` is still the handshake instant, so the first keepalive
        // deadline is t0 + KEEPALIVE — already in the past relative to the
        // chatter we just fed (which ran to t0 + 2s).
    }

    // First keepalive tick past the deadline → emit a ping, arm the outstanding
    // flag, stay Connected.
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

    // More chatty desync between the two intervals — this is the regression
    // trap: pre-ADR-0058 these chunks reset the baseline AND there was no
    // outstanding-ping concept, so escalation could never happen.
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0 + KEEPALIVE + Duration::from_millis(500), &desync_chunk())
            .expect("desync chunk parks");
        assert!(conn.is_connected(), "still chatty, still connected for now");
    }

    // Second keepalive interval elapses with no *decoded* inbound frame → the
    // watchdog escalates to Failed instead of dead-pinging forever.
    {
        let mut conn = shared.inner.lock();
        conn.handle_timeout(t0 + KEEPALIVE * 2 + Duration::from_millis(2));
        conn.state()
    }
}

/// Single deterministic run: a chatty desync must escalate to `Failed` on the
/// second missed keepalive interval. The driver treats `Failed` as
/// `should_close` → supervised reconnect, so this is the difference between a
/// transparently-recovered connection and a permanent wedge.
#[test]
fn chatty_desync_escalates_keepalive_watchdog_to_failed() {
    let terminal = run_chatty_desync_wedge();
    assert_eq!(
        terminal,
        HandshakeState::Failed,
        "two missed keepalive intervals over a chatty desync must fail the \
         connection (ADR-0058); a chatty socket must not keep the watchdog \
         alive forever (issues #187, #221)",
    );
}

/// Seed sweep over the target failure seeds plus a derived spread. The
/// scenario is deterministic, so every seed must reach the same terminal
/// `Failed` state — the sweep guards against any future seed-dependent
/// regression (e.g. a buggify point creeping onto the read path) silently
/// re-opening the wedge on a subset of seeds.
#[test]
fn chatty_desync_watchdog_escalation_holds_across_seeds() {
    // The two seeds the daily sweep flagged (#187, #221), plus a 14-seed
    // splitmix spread so the assertion covers a broad seed neighbourhood.
    let mut seeds = vec![0xa643_e7ad_4c47_c32e_u64, 0x2c60_abc6_8153_2cd6_u64];
    seeds.extend(sweep_seeds(14));

    for seed in seeds {
        let terminal = run_chatty_desync_wedge();
        assert_eq!(
            terminal,
            HandshakeState::Failed,
            "seed {seed:#018x}: chatty-desync keepalive escalation must reach \
             Failed deterministically (ADR-0058)",
        );
    }
}
