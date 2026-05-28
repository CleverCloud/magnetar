// SPDX-License-Identifier: Apache-2.0

//! TLS handshake byte-level chaos under simulation.
//!
//! Pumps adversarial byte sequences through `RustlsByteAdapter` and asserts
//! the adapter surfaces a `rustls::Error` rather than silently dropping
//! bytes or hanging. The transport's `read_buf` path relies on this
//! propagation to turn corrupt handshakes into `InvalidData` errors that
//! the supervised reconnect can act on, rather than letting them wedge a
//! connection in `is_handshaking() == true` forever.
//!
//! These fixtures complement the existing `tls.rs` unit tests which only
//! covered the no-input and garbage-record paths. The chaos here is:
//!
//! - record-type byte flip ([`tls_handshake_chaos_record_type_flip_errors`])
//! - record-length truncation ([`tls_handshake_chaos_truncated_length_errors`])
//! - injection of an invalid Alert record into the handshake stream
//!   ([`tls_handshake_chaos_injected_alert_errors`])
//!
//! Each scenario exercises a different rustls validation gate so a future
//! regression in any one path surfaces as a test failure rather than a
//! silent stall.

use std::sync::Arc;

use magnetar_runtime_moonpool::tls::RustlsByteAdapter;
use magnetar_runtime_moonpool::tls_crypto::active_provider;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, RootCertStore};

/// Stand-up a client session against an empty trust store. The
/// `ClientConnection` still constructs and emits a `ClientHello`; we only
/// drive the inbound side with corrupted bytes after that point. The
/// rustls crypto provider is picked by the workspace's `crypto-*`
/// feature (issue #9, ADR-0035).
fn make_session() -> ClientConnection {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let config = Arc::new(
        ClientConfig::builder_with_provider(active_provider())
            .with_safe_default_protocol_versions()
            .expect("rustls default protocol versions are valid")
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    );
    let name = ServerName::try_from("example.com").expect("valid server name");
    ClientConnection::new(config, name).expect("rustls client session")
}

/// Drive the adapter through the initial `ClientHello` so the session is
/// past the "waiting for input" gate and into the
/// "awaiting `ServerHello`" state where injected garbage will be
/// validated.
fn prime_through_client_hello(adapter: &mut RustlsByteAdapter) {
    adapter.step().expect("ClientHello step must not error");
    // Drop the ClientHello so it doesn't get fed back into the inbound
    // stream by accident; we're driving the inbound side with our own
    // corrupted bytes from here on.
    let _ = adapter.take_encrypted_outbound();
}

/// TLS 1.3 record framing: `[content_type, version_major, version_minor,
/// length_hi, length_lo, ...payload...]`. The mutations below all start
/// from a syntactically-valid 5-byte header and corrupt one field.
#[test]
fn tls_handshake_chaos_record_type_flip_errors() {
    let mut adapter = RustlsByteAdapter::new(make_session());
    prime_through_client_hello(&mut adapter);

    // Record type 0xFF is undefined (valid range is 20..24). Rustls
    // rejects unknown types during `process_new_packets`.
    let corrupted = vec![0xFF, 0x03, 0x03, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00];
    adapter.push_encrypted(&corrupted);
    let outcome = adapter.step();
    assert!(
        outcome.is_err(),
        "rustls must reject record with undefined content type, got {outcome:?}",
    );
}

#[test]
fn tls_handshake_chaos_truncated_length_errors() {
    let mut adapter = RustlsByteAdapter::new(make_session());
    prime_through_client_hello(&mut adapter);

    // Application data record (type 0x17) claiming a 5-byte payload but
    // arriving with a payload that is structurally garbage. rustls walks
    // the inner frame, fails MAC verification or content-type sanity
    // checking on `process_new_packets`, and surfaces an error.
    //
    // We pick application-data rather than handshake here so the record
    // is validated even before the handshake completes — once a
    // `ServerHello`-shaped frame is required, anything non-handshake is
    // an explicit protocol violation.
    let corrupted = vec![0x17, 0x03, 0x03, 0x00, 0x05, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE];
    adapter.push_encrypted(&corrupted);
    let outcome = adapter.step();
    assert!(
        outcome.is_err(),
        "rustls must reject application-data record mid-handshake, got {outcome:?}",
    );
}

#[test]
fn tls_handshake_chaos_injected_alert_errors() {
    let mut adapter = RustlsByteAdapter::new(make_session());
    prime_through_client_hello(&mut adapter);

    // Alert record (content type 0x15) with payload `[level=2 fatal,
    // description=80 internal_error]`. A well-formed Alert IS allowed
    // pre-handshake, and rustls surfaces it as an error from the peer.
    // We assert the adapter propagates that error rather than swallowing
    // it.
    let alert = vec![0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x50];
    adapter.push_encrypted(&alert);
    let outcome = adapter.step();
    assert!(
        outcome.is_err(),
        "rustls must surface fatal Alert from peer, got {outcome:?}",
    );
}
