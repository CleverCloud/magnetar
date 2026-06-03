// SPDX-License-Identifier: Apache-2.0

//! Driver mid-session reject — differential equivalence.
//!
//! Layer (d) of the ADR-0024 four-layer policy for the driver
//! re-entrant-mutex deadlock fix (ADR-0038). The fix lives in the two
//! engines' driver read loops (`magnetar-runtime-{tokio,moonpool}`):
//! binding `handle_bytes_owned`'s result to a `let` so the `shared.inner`
//! `parking_lot::Mutex` guard drops before the error arm re-locks.
//!
//! No `EventStream` parity is asserted here because the reject is
//! **invisible to the `EventStream` surface** — it manifests as an
//! engine-local terminal error (`ClientError::Protocol` /
//! `EngineError::Protocol`) that ends the driver task, not as a
//! `Trace` [`Op`]→[`Event`] outcome. This mirrors
//! `supervisor_backoff_persistence_equivalence.rs`, whose fix likewise
//! lives in the two driver loops and is invisible to the event stream.
//!
//! What the two engines *do* share is the reject decision itself: both
//! delegate inbound-frame decoding to the **same** `magnetar-proto`
//! `Connection::handle_bytes_owned`. Divergence between engines on a
//! malformed mid-session frame could therefore only arise if one engine
//! grew an engine-local decode path — which neither does. This test pins
//! that shared decision (run once per "engine") and the exact reject the
//! runtime error arm consumes. The end-to-end deterministic *no-deadlock*
//! assertion lives in the runtime layers
//! (`magnetar-runtime-{tokio,moonpool}/tests/driver_mid_session_reject.rs`)
//! and the `magnetar-runtime-moonpool/tests/sim_chaos.rs` swizzle-clog
//! sweep (seeds #65 / #136).

use std::sync::Arc;
use std::time::{Instant, SystemTime};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, FrameError, ProtocolError, SUPPORTED_PROTOCOL_VERSION,
    encode_command, pb,
};

/// A `CommandConnected` frame — drives a fresh handshaking connection to
/// the `Connected` state so the subsequent reject is genuinely
/// *mid-session*.
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

/// The shared decode/reject decision both engines' driver loops delegate
/// to: drive a `Connection` to `Connected`, then feed `malformed` via
/// `handle_bytes_owned` and collapse the outcome to a stable string so the
/// two engines compare equal regardless of `Display` punctuation. This is
/// the exact `magnetar-proto` call each engine makes inside its driver
/// read loop — running it stands in for "what engine X observes".
fn reject_decision(malformed: &[u8]) -> Result<(), String> {
    let mut conn = Connection::new(ConnectionConfig::default(), Arc::new(SystemTime::now));
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes_owned(Instant::now(), handshake_response_bytes())
        .expect("handshake completes");
    assert!(conn.is_connected(), "mid-session precondition");

    let mut chunk = BytesMut::with_capacity(malformed.len());
    chunk.extend_from_slice(malformed);
    match conn.handle_bytes_owned(Instant::now(), chunk) {
        Ok(()) => Ok(()),
        Err(ProtocolError::Frame(FrameError::BadLength(_))) => Err("frame:bad_length".to_owned()),
        Err(other) => Err(format!("other:{other:?}")),
    }
}

#[test]
fn engines_agree_on_malformed_mid_session_frame_reject() {
    // A 4-byte big-endian `total_size = 0` prefix — the cheapest
    // deterministic reject (`peek_full_frame_len` → `BadLength(0)`), and
    // the shape the swizzle-clog seeds reorder into.
    let malformed = [0u8; 4];

    // Both engines delegate to the same `magnetar-proto` decode path;
    // running the shared helper twice with identical input is the
    // differential surrogate for "tokio engine" vs "moonpool engine".
    // Drift would mean one engine had grown an engine-local decoder.
    let tokio_decision = reject_decision(&malformed);
    let moonpool_decision = reject_decision(&malformed);
    assert_eq!(
        tokio_decision, moonpool_decision,
        "both engines must reject a malformed mid-session frame identically (shared proto decode)",
    );

    // Pin the actual contract the runtime error arm consumes: a framing
    // `BadLength` reject, not `Ok` and not some other error. If this ever
    // flips to `Ok`, the driver would never enter the error arm the fix
    // guards, and the runtime layers' deadlock guard would be vacuous.
    assert_eq!(
        tokio_decision,
        Err("frame:bad_length".to_owned()),
        "a malformed mid-session frame must surface as a BadLength framing reject on both engines",
    );
}
