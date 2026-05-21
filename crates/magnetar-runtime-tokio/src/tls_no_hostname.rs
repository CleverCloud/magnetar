// SPDX-License-Identifier: Apache-2.0

//! TLS verifier that checks the certificate chain but **skips hostname
//! verification**.
//!
//! Java parity: `enableTlsHostnameVerification(false)` paired with a
//! configured trust store. The full chain is verified (signature,
//! validity dates, revocation if OCSP-stapled) — only the
//! CN/SAN-vs-hostname match is bypassed. Useful when the broker presents
//! a certificate issued for an internal hostname / IP that the client
//! reaches by a different name (e.g. via NAT or a service-mesh sidecar).
//!
//! **Still insecure for the open Internet.** A holder of any valid
//! CA-issued certificate can impersonate the broker. Pair with mTLS or
//! a private CA when you flip this off.
//!
//! Mirrors the pattern documented in [`crate::tls_insecure`] (which
//! disables BOTH the chain check and the hostname check); this module is
//! the strictly-narrower variant where only the hostname check is off.

use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::WebPkiSupportedAlgorithms;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error as RustlsError, SignatureScheme};

/// Build a [`rustls::ClientConfig`] that verifies the certificate chain
/// against the supplied PEM roots but **skips the hostname match**.
/// Mirrors Java's `tlsTrustCertsFilePath(...) + enableTlsHostnameVerification(false)`
/// combination.
///
/// # Errors
///
/// Returns [`crate::ClientError::Other`] if no valid certificate is
/// parsed from the PEM, or if rustls rejects any cert from the bundle.
pub fn tls_config_no_hostname(
    pem_bytes: &[u8],
) -> Result<Arc<rustls::ClientConfig>, crate::ClientError> {
    use rustls_pki_types::pem::PemObject;

    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(pem_bytes) {
        let cert = cert.map_err(|e| {
            crate::ClientError::Other(format!("failed to parse a trust certificate from PEM: {e}"))
        })?;
        roots.add(cert).map_err(|e| {
            crate::ClientError::Other(format!("rustls rejected a trust certificate: {e}"))
        })?;
    }
    if roots.is_empty() {
        return Err(crate::ClientError::Other(
            "no trust certificates were parsed from the provided PEM".to_owned(),
        ));
    }

    // Wrap the standard WebPKI verifier so the chain check still runs —
    // we only override `verify_server_cert` to drop the hostname-match
    // step.
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
    let inner = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .map_err(|e| {
            crate::ClientError::Other(format!("failed to build WebPkiServerVerifier: {e}"))
        })?;
    let verifier = Arc::new(NoHostnameVerification {
        inner,
        supported_algs: provider.signature_verification_algorithms,
    });

    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("rustls default protocol versions are valid")
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

#[derive(Debug)]
struct NoHostnameVerification {
    /// Underlying WebPKI verifier. We re-use it for chain + signature
    /// validation, then ignore its hostname-mismatch decision below.
    inner: Arc<WebPkiServerVerifier>,
    supported_algs: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for NoHostnameVerification {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        match self
            .inner
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
        {
            Ok(verified) => Ok(verified),
            // The full set of "bad cert" errors per rustls is collapsed under
            // `InvalidCertificate(...)`. We only treat the `NotValidForName`
            // sub-variant as "ignore"; every other failure (expired, bad
            // signature, untrusted root) is propagated as-is.
            Err(RustlsError::InvalidCertificate(
                rustls::CertificateError::NotValidForName
                | rustls::CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            Err(other) => Err(other),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported_algs.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sanity-check the builder rejects an empty PEM blob.
    #[test]
    fn empty_pem_is_rejected() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let err = tls_config_no_hostname(b"").expect_err("empty PEM must be rejected");
        match err {
            crate::ClientError::Other(msg) => {
                assert!(
                    msg.contains("no trust certificates"),
                    "unexpected error: {msg}"
                );
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
