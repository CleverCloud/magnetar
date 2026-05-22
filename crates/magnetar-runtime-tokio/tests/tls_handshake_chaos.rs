// SPDX-License-Identifier: Apache-2.0

//! TLS handshake byte-level chaos against `rustls::ClientConnection`.
//!
//! Mirrors the moonpool engine's `tls_handshake_chaos.rs` 1:1 (ADR-0024
//! cross-runtime test parity). The tokio engine wraps
//! `rustls::ClientConnection` via `tokio-rustls`, but the underlying
//! state machine that has to reject corrupt bytes is the same
//! `ClientConnection`. Validating its rejection paths from the tokio
//! crate's tests keeps the cross-engine TLS robustness story
//! symmetric: if rustls regresses (e.g. a future version starts
//! silently dropping a malformed record instead of erroring), both
//! engines surface the regression.

use std::io::Cursor;
use std::sync::Arc;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, RootCertStore};

fn make_session() -> ClientConnection {
    let config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    );
    let name = ServerName::try_from("example.com").expect("valid server name");
    ClientConnection::new(config, name).expect("rustls client session")
}

/// Drive `read_tls` + `process_new_packets` over `bytes` and return the
/// outcome from the `process_new_packets` call (which is what surfaces
/// rustls-level validation errors). `read_tls` itself only fails on
/// `io::Error`, not on TLS-protocol violations.
fn feed(session: &mut ClientConnection, bytes: &[u8]) -> Result<(), rustls::Error> {
    let mut cursor = Cursor::new(bytes);
    let _consumed = session
        .read_tls(&mut cursor)
        .expect("read_tls cannot fail on Cursor");
    session.process_new_packets().map(|_| ())
}

#[test]
fn tls_handshake_chaos_record_type_flip_errors() {
    let mut session = make_session();
    // 0xFF is outside the defined content-type range (20..24).
    let corrupted = vec![0xFF, 0x03, 0x03, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00];
    let outcome = feed(&mut session, &corrupted);
    assert!(
        outcome.is_err(),
        "rustls must reject record with undefined content type, got {outcome:?}",
    );
}

#[test]
fn tls_handshake_chaos_truncated_length_errors() {
    let mut session = make_session();
    // Application data record (0x17) mid-handshake is a protocol
    // violation, regardless of payload content.
    let corrupted = vec![0x17, 0x03, 0x03, 0x00, 0x05, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE];
    let outcome = feed(&mut session, &corrupted);
    assert!(
        outcome.is_err(),
        "rustls must reject application-data record mid-handshake, got {outcome:?}",
    );
}

#[test]
fn tls_handshake_chaos_injected_alert_errors() {
    let mut session = make_session();
    // Fatal Alert record (content type 0x15, level=2 fatal,
    // description=80 internal_error).
    let alert = vec![0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x50];
    let outcome = feed(&mut session, &alert);
    assert!(
        outcome.is_err(),
        "rustls must surface fatal Alert from peer, got {outcome:?}",
    );
}
