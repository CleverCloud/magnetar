// SPDX-License-Identifier: Apache-2.0

//! Insecure TLS configuration — accepts any server certificate.
//!
//! This module exists exclusively to mirror Java's
//! `ClientBuilder#tlsAllowInsecureConnection(true)`, which is useful in:
//!
//! - Local development against a self-signed broker certificate.
//! - CI / e2e suites running against an ephemeral container.
//!
//! **Do not use in production.** A `ClientConfig` built from
//! [`insecure_tls_config`] disables both signature-chain verification and
//! hostname checking, so the client cannot tell a legitimate broker from
//! a MITM. The crate-wide ban on `native-tls` / `openssl` still applies —
//! this only adds a rustls verifier that always says "yes". See
//! [ADR-0005](../../specs/adr/0005-rustls-only-tls.md) for the TLS
//! backend rule and `docs/security-review-2026-05-21.md` §M-01 for the
//! footgun-mitigation discussion.
//!
//! The verifier honors whichever subset of TLS 1.2 / 1.3 the configured
//! `rustls::crypto::CryptoProvider` supports. The provider itself is
//! picked by the workspace's `crypto-*` feature (issue #9, ADR-0035) —
//! aws-lc-rs by default (with rustls's built-in post-quantum hybrid key
//! exchange), or ring / rustls-openssl / FIPS aws-lc-rs by explicit
//! opt-in. TLS 1.3 stays the wire default under every provider.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::WebPkiSupportedAlgorithms;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error as RustlsError, SignatureScheme};

/// Build a [`rustls::ClientConfig`] that accepts any server certificate without
/// verifying its trust chain or hostname. Mirrors Java
/// `tlsAllowInsecureConnection(true) + enableTlsHostnameVerification(false)`.
///
/// The returned config has `with_no_client_auth` — no client certs are
/// presented. Pair with a separate auth provider (token / oauth2) for client
/// identity.
///
/// The active rustls crypto provider is picked by the workspace's
/// `crypto-*` feature (issue #9, ADR-0035) and installed idempotently
/// via [`crate::tls_crypto::active_provider`]. No silent `ring`
/// fallback.
#[must_use]
pub fn insecure_tls_config() -> Arc<rustls::ClientConfig> {
    let provider = crate::tls_crypto::active_provider();
    let verifier = Arc::new(NoCertificateVerification {
        supported_algs: provider.signature_verification_algorithms,
    });
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("rustls default protocol versions are valid")
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    Arc::new(config)
}

#[derive(Debug)]
struct NoCertificateVerification {
    supported_algs: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported_algs.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insecure_config_builds() {
        // Install the workspace-selected provider (per the `crypto-*`
        // feature; aws-lc-rs by default).
        crate::tls_crypto::install_default_provider();
        let cfg = insecure_tls_config();
        assert!(Arc::strong_count(&cfg) >= 1);
        // ALPN-protocols default is empty; we just sanity-check that the
        // config was constructed without panicking.
        assert!(cfg.alpn_protocols.is_empty());
    }

    #[test]
    fn verifier_accepts_any_cert() {
        crate::tls_crypto::install_default_provider();
        let provider = crate::tls_crypto::active_provider();
        let verifier = NoCertificateVerification {
            supported_algs: provider.signature_verification_algorithms,
        };
        let now = UnixTime::since_unix_epoch(std::time::Duration::from_secs(0));
        // Empty cert payload — verifier should still succeed.
        let bogus = CertificateDer::from(vec![0u8; 8]);
        let name = ServerName::try_from("example.com").expect("valid name");
        let result = verifier.verify_server_cert(&bogus, &[], &name, &[], now);
        assert!(result.is_ok());
    }
}
