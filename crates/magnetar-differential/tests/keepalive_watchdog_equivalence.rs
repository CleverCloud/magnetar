// SPDX-License-Identifier: Apache-2.0

//! Progress-based keepalive watchdog — differential equivalence
//! (ADR-0024 layer d for ADR-0058).
//!
//! The fix lives entirely in `magnetar-proto`
//! (`Connection::handle_bytes` / `handle_bytes_owned` / `handle_timeout`):
//! refresh the keepalive baseline per *decoded frame* instead of per raw
//! chunk, and escalate to `mark_disconnected()` → `HandshakeState::Failed`
//! when a second consecutive keepalive interval elapses with a ping still
//! outstanding.
//!
//! No `EventStream` parity is asserted here because the escalation is
//! **invisible to the `EventStream` surface** — it manifests as an
//! engine-local handshake-state flip to `Failed` that the driver reads as
//! `should_close` (→ supervised reconnect), not as a `Trace` [`Op`]→[`Event`]
//! outcome. This mirrors `driver_mid_session_reject_equivalence.rs` and
//! `supervisor_backoff_persistence_equivalence.rs`, whose fixes likewise live
//! below the event-stream surface.
//!
//! What the two engines *do* share is the watchdog decision itself: both
//! driver loops feed inbound bytes through the **same** `magnetar-proto`
//! `Connection::handle_bytes_owned` and drive keepalive via the **same**
//! `Connection::handle_timeout`. Divergence could only arise if one engine
//! grew an engine-local keepalive path — which neither does. This test pins
//! that shared decision (run once per "engine") and the exact terminal state
//! the runtime `should_close` arm consumes. The end-to-end deterministic
//! escalation assertions live in the runtime layers
//! (`magnetar-runtime-{tokio,moonpool}/tests/keepalive_watchdog.rs`).

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, HandshakeState, SUPPORTED_PROTOCOL_VERSION, encode_command, pb,
};

const KEEPALIVE: Duration = Duration::from_secs(1);

/// A `CommandConnected` frame — drives a fresh handshaking connection to
/// the `Connected` state.
fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-diff".to_owned(),
            protocol_version: Some(SUPPORTED_PROTOCOL_VERSION),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandConnected");
    buf
}

/// A desynced chunk whose announced `total_size` (1024) is plausible but
/// whose promised bytes never arrive: `peek_full_frame_len` parks forever.
fn desync_chunk() -> BytesMut {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&1024u32.to_be_bytes());
    buf.extend_from_slice(b"chatty-but-never-a-whole-frame");
    buf
}

/// The shared keepalive-watchdog decision both engines' driver loops delegate
/// to: drive a `Connection` to `Connected`, feed a chatty desync that never
/// frames, then tick `handle_timeout` across two keepalive intervals. Returns
/// the terminal handshake state. This is the exact `magnetar-proto` sequence
/// each engine makes inside its driver read + timeout loop — running it stands
/// in for "what engine X observes".
fn watchdog_decision() -> HandshakeState {
    let t0 = Instant::now();
    let cfg = ConnectionConfig {
        keepalive_interval: KEEPALIVE,
        ..ConnectionConfig::default()
    };
    let mut conn = Connection::new(cfg, Arc::new(SystemTime::now));
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes_owned(t0, handshake_response_bytes())
        .expect("handshake completes");
    assert!(conn.is_connected(), "watchdog precondition: Connected");

    // Chatty desync inside the first interval — must not refresh the baseline.
    for tick in 1..=4u32 {
        let at = t0 + KEEPALIVE / 2 * tick;
        conn.handle_bytes_owned(at, desync_chunk())
            .expect("desync chunk parks, not a hard error");
        assert!(conn.is_connected(), "chatty desync must not flip state");
    }

    // First missed interval → ping, arm outstanding, stay Connected.
    conn.handle_timeout(t0 + KEEPALIVE + Duration::from_millis(1));
    assert!(conn.is_connected(), "first missed interval only pings");

    // More chatter, then the second missed interval → escalate to Failed.
    conn.handle_bytes_owned(t0 + KEEPALIVE + Duration::from_millis(500), desync_chunk())
        .expect("desync chunk parks");
    conn.handle_timeout(t0 + KEEPALIVE * 2 + Duration::from_millis(2));
    conn.state()
}

#[test]
fn engines_agree_on_keepalive_watchdog_escalation() {
    // Both engines delegate to the same `magnetar-proto` keepalive path;
    // running the shared helper twice with identical input is the
    // differential surrogate for "tokio engine" vs "moonpool engine". Drift
    // would mean one engine had grown an engine-local keepalive watchdog.
    let tokio_decision = watchdog_decision();
    let moonpool_decision = watchdog_decision();
    assert_eq!(
        tokio_decision, moonpool_decision,
        "both engines must escalate the keepalive watchdog identically (shared proto path)",
    );

    // Pin the actual contract the runtime `should_close` arm consumes: two
    // missed intervals over a chatty desync reach `Failed`, not a wedged
    // `Connected`. If this ever flips back to `Connected`, the driver would
    // never enter the `should_close` arm and the connection would wedge
    // (issues #187, #221).
    assert_eq!(
        tokio_decision,
        HandshakeState::Failed,
        "a chatty desync that never frames must fail the connection after two \
         missed keepalive intervals on both engines (ADR-0058)",
    );
}
