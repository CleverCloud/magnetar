// SPDX-License-Identifier: Apache-2.0

//! mTLS [`AuthProvider`] — surfaces the cert chain + private key bytes.
//!
//! Mirrors `org.apache.pulsar.client.impl.auth.AuthenticationTls`. The Pulsar wire protocol
//! conveys mTLS purely at the TLS handshake layer: the `CommandConnect.auth_data` is left empty,
//! and the broker derives the client identity from the certificate presented during the rustls
//! handshake.
//!
//! This provider therefore returns empty bytes from [`AuthProvider::initial`] and exposes
//! [`TlsAuth::cert_chain_pem`] / [`TlsAuth::private_key_pem`] for the runtime engine to load into
//! a `rustls::ClientConfig`.

use std::fs;
use std::path::Path;

use bytes::Bytes;

use super::{AuthError, AuthProvider};

/// mTLS auth provider carrying PEM-encoded cert and key material.
///
/// `Debug` is implemented manually to redact `private_key_pem` (CWE-532).
/// A derived `Debug` would print the PEM key body whenever an
/// `AuthProvider: Debug` is rendered into a tracing span, panic dump,
/// or support bundle — enabling full client impersonation from a leaked
/// log line. The cert chain length is shown as a coarse identifier;
/// callers needing a stable fingerprint should hash the cert themselves.
#[derive(Clone)]
pub struct TlsAuth {
    cert_chain_pem: Bytes,
    private_key_pem: Bytes,
}

impl std::fmt::Debug for TlsAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsAuth")
            .field("cert_chain_pem_len", &self.cert_chain_pem.len())
            .field("private_key_pem", &"<redacted>")
            .finish()
    }
}

impl TlsAuth {
    /// Construct from already-loaded PEM bytes.
    #[must_use]
    pub fn from_pem_bytes(cert_chain_pem: Bytes, private_key_pem: Bytes) -> Self {
        Self {
            cert_chain_pem,
            private_key_pem,
        }
    }

    /// Read PEM-encoded cert and key from disk.
    pub fn from_pem_files(
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
    ) -> Result<Self, AuthError> {
        let cert = fs::read(cert_path.as_ref()).map_err(|err| {
            AuthError::Io(format!(
                "reading cert file {}: {err}",
                cert_path.as_ref().display()
            ))
        })?;
        let key = fs::read(key_path.as_ref()).map_err(|err| {
            AuthError::Io(format!(
                "reading key file {}: {err}",
                key_path.as_ref().display()
            ))
        })?;
        Ok(Self::from_pem_bytes(Bytes::from(cert), Bytes::from(key)))
    }

    /// PEM-encoded cert chain.
    #[must_use]
    pub fn cert_chain_pem(&self) -> &Bytes {
        &self.cert_chain_pem
    }

    /// PEM-encoded private key.
    #[must_use]
    pub fn private_key_pem(&self) -> &Bytes {
        &self.private_key_pem
    }
}

impl AuthProvider for TlsAuth {
    #[allow(clippy::unnecessary_literal_bound)]
    fn method(&self) -> &str {
        "tls"
    }

    fn initial(&self) -> Result<Bytes, AuthError> {
        // mTLS carries the auth at the TLS handshake layer; the protocol `auth_data` is empty.
        Ok(Bytes::new())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{AuthProvider, TlsAuth};

    #[test]
    fn round_trip_holds_bytes_and_method_is_tls() {
        let cert =
            Bytes::from_static(b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n");
        let key =
            Bytes::from_static(b"-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n");
        let p = TlsAuth::from_pem_bytes(cert.clone(), key.clone());
        assert_eq!(p.method(), "tls");
        assert_eq!(p.cert_chain_pem(), &cert);
        assert_eq!(p.private_key_pem(), &key);
        assert!(p.initial().expect("initial").is_empty());
    }

    #[test]
    fn from_pem_files_missing_paths() {
        let err = TlsAuth::from_pem_files(
            "/this/path/does/not/exist/cert.pem",
            "/this/path/does/not/exist/key.pem",
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cert file"), "msg={msg}");
    }

    /// CWE-532 regression: a derived `Debug` would print the PEM private
    /// key body (enabling full client impersonation from a leaked log
    /// line). The manual impl must redact it.
    #[test]
    fn debug_redacts_private_key() {
        let cert = Bytes::from_static(
            b"-----BEGIN CERTIFICATE-----\npublic-cert-bytes\n-----END CERTIFICATE-----\n",
        );
        let key = Bytes::from_static(
            b"-----BEGIN PRIVATE KEY-----\nSECRET-KEY-MATERIAL-1234\n-----END PRIVATE KEY-----\n",
        );
        let p = TlsAuth::from_pem_bytes(cert, key);
        let rendered = format!("{p:?}");
        assert!(
            !rendered.contains("SECRET-KEY-MATERIAL"),
            "private key leaked through Debug: {rendered}",
        );
        assert!(
            !rendered.contains("BEGIN PRIVATE KEY"),
            "PEM header for key leaked through Debug: {rendered}",
        );
        assert!(
            rendered.contains("<redacted>"),
            "Debug should mark redaction explicitly: {rendered}",
        );
    }
}
