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
//! `rustls::crypto::CryptoProvider` supports; under the workspace's `ring`
//! feature set that means TLS 1.3 is the wire default.

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
/// # Panics
///
/// Panics if no [`rustls::crypto::CryptoProvider`] is installed for the current
/// process. The workspace's `ring` feature installs one automatically; this
/// function exists for callers wiring their own provider.
#[must_use]
pub fn insecure_tls_config() -> Arc<rustls::ClientConfig> {
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
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
        // Install a default provider if none is set (matches what `tokio_rustls`
        // does in its docs example for tests).
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cfg = insecure_tls_config();
        assert!(Arc::strong_count(&cfg) >= 1);
        // ALPN-protocols default is empty; we just sanity-check that the
        // config was constructed without panicking.
        assert!(cfg.alpn_protocols.is_empty());
    }

    #[test]
    fn verifier_accepts_any_cert() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let provider = rustls::crypto::CryptoProvider::get_default()
            .cloned()
            .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
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
