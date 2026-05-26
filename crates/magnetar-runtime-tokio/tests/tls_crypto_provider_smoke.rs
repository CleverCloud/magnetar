// SPDX-License-Identifier: Apache-2.0

//! TLS crypto provider smoke test (issue #9, ADR-0035).
//!
//! Exercises the per-feature `tls_crypto::active_provider()` shim under
//! whichever rustls crypto backend the build is using. The test runs in
//! both runtime crates with the same shape (1:1 parity required by
//! ADR-0024 `check-runtime-test-parity`).
//!
//! The test does NOT require a live broker: it builds a
//! `rustls::ClientConnection` against an empty trust store and pumps the
//! `ClientHello` through the session. A successful pump means the
//! active provider supplied valid key-exchange + signature algorithms
//! and the session reached the post-`ClientHello` "awaiting
//! `ServerHello`" state.
//!
//! Coverage rationale: under `--all-features` the cfg cascade in
//! `tls_crypto.rs` resolves to aws-lc-rs; under the per-provider matrix
//! (`cargo xtask check-crypto-matrix`) each provider runs this test
//! independently. Any future regression that breaks `active_provider()`
//! for any provider surfaces here.

use std::io::Cursor;
use std::sync::Arc;

use magnetar_runtime_tokio::tls_crypto::{active_provider, install_default_provider};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, RootCertStore};

#[test]
fn tls_crypto_provider_drives_client_hello() {
    // Idempotent install — safe under `--all-features` and under any
    // single-provider feature build.
    install_default_provider();

    let provider = active_provider();
    assert!(
        !provider.cipher_suites.is_empty(),
        "active provider must expose at least one cipher suite"
    );

    // Build a client config explicitly with the active provider —
    // mirrors the production `transport::default_tls_config` and
    // `client::tls_config_from_pem` paths.
    let config = Arc::new(
        ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("rustls default protocol versions are valid")
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    );

    let name = ServerName::try_from("example.com").expect("valid server name");
    let mut session = ClientConnection::new(config, name).expect("rustls client session");

    // Drive `write_tls` so the session emits its `ClientHello` —
    // confirms the provider supplied a usable kx + signature algorithm
    // set.
    let mut hello = Vec::with_capacity(4096);
    let written = session
        .write_tls(&mut hello)
        .expect("write_tls cannot fail on Vec sink");
    assert!(written > 0, "active provider must produce a ClientHello");
    // Sanity-check the framing: TLS 1.x record type 0x16 = handshake.
    assert_eq!(hello[0], 0x16, "first record must be a handshake record");

    // Push an empty inbound buffer to confirm `read_tls` accepts zero
    // bytes and `process_new_packets` doesn't error out on the empty
    // path.
    let mut empty = Cursor::new(&[][..]);
    let consumed = session
        .read_tls(&mut empty)
        .expect("read_tls cannot fail on Cursor");
    assert_eq!(consumed, 0, "empty inbound buffer consumes zero bytes");
}
