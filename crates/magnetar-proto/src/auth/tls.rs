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
#[derive(Debug, Clone)]
pub struct TlsAuth {
    cert_chain_pem: Bytes,
    private_key_pem: Bytes,
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
}
