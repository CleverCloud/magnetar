// SPDX-License-Identifier: Apache-2.0

//! TLS crypto provider smoke test — moonpool engine (issue #9,
//! ADR-0035).
//!
//! Mirrors `magnetar-runtime-tokio::tests::tls_crypto_provider_smoke`
//! 1:1 (ADR-0024 `check-runtime-test-parity` keeps tokio and moonpool
//! test counts in lockstep). The moonpool variant runs the
//! `ClientHello` through the byte-pipe TLS adapter (`RustlsByteAdapter`,
//! ADR-0006) instead of `tokio-rustls`.

use std::sync::Arc;

use magnetar_runtime_moonpool::tls::RustlsByteAdapter;
use magnetar_runtime_moonpool::tls_crypto::{active_provider, install_default_provider};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, RootCertStore};

#[test]
fn tls_crypto_provider_drives_client_hello_via_bytepipe() {
    install_default_provider();

    let provider = active_provider();
    assert!(
        !provider.cipher_suites.is_empty(),
        "active provider must expose at least one cipher suite"
    );

    let config = Arc::new(
        ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("rustls default protocol versions are valid")
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    );

    let name = ServerName::try_from("example.com").expect("valid server name");
    let session = ClientConnection::new(config, name).expect("rustls client session");

    // Pump the session through the moonpool byte-pipe adapter to
    // emit the `ClientHello`. A successful emit confirms the active
    // provider supplied a usable kx + signature algorithm set.
    let mut adapter = RustlsByteAdapter::new(session);
    adapter.step().expect("ClientHello step must not error");
    let outbound = adapter.take_encrypted_outbound();
    assert!(
        !outbound.is_empty(),
        "active provider must produce a ClientHello via the byte-pipe adapter"
    );
    // Sanity-check the framing: TLS 1.x record type 0x16 = handshake.
    assert_eq!(outbound[0], 0x16, "first record must be a handshake record");
}
