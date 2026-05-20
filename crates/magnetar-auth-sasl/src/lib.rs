// SPDX-License-Identifier: Apache-2.0

//! SASL auth providers for magnetar.
//!
//! Two surfaces are exposed:
//!
//! - [`SaslPlain`] — RFC 4616 `PLAIN` mechanism. Useful for username/password broker auth in tests
//!   and for environments where token-based auth is not configured.
//! - [`SaslKerberos`] — GSSAPI/Kerberos surface, gated behind the `kerberos` feature flag. The
//!   default impl returns [`AuthError::Unsupported`]; a future patch wires it to a real GSSAPI
//!   library when an enterprise consumer needs it.
//!
//! The `method()` is always `"sasl"`.
//!
//! Mirrors `org.apache.pulsar.client.impl.auth.AuthenticationSasl`.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

use bytes::Bytes;
use magnetar_proto::{AuthError, AuthProvider};

/// SASL `PLAIN` (RFC 4616) credentials.
#[derive(Debug, Clone)]
pub struct SaslPlain {
    username: String,
    password: String,
}

impl SaslPlain {
    /// Construct a `PLAIN` provider from username + password.
    #[must_use]
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }

    /// Compute the wire bytes for the `PLAIN` mechanism: `\0<username>\0<password>`.
    fn encode(&self) -> Bytes {
        let mut out = Vec::with_capacity(2 + self.username.len() + self.password.len());
        out.push(0);
        out.extend_from_slice(self.username.as_bytes());
        out.push(0);
        out.extend_from_slice(self.password.as_bytes());
        Bytes::from(out)
    }
}

impl AuthProvider for SaslPlain {
    fn method(&self) -> &str {
        "sasl"
    }

    fn initial(&self) -> Result<Bytes, AuthError> {
        Ok(self.encode())
    }
}

/// SASL Kerberos/GSSAPI surface.
///
/// Real Kerberos support is gated behind the `kerberos` feature flag. Without that feature, the
/// provider compiles but returns [`AuthError::Unsupported`] on every call so the trait surface
/// is stable for downstream code.
#[derive(Debug, Clone, Default)]
pub struct SaslKerberos {
    _private: (),
}

impl SaslKerberos {
    /// Construct a Kerberos provider. The actual GSSAPI bindings are wired up when the
    /// `kerberos` feature is enabled (not yet in M6).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl AuthProvider for SaslKerberos {
    fn method(&self) -> &str {
        "sasl"
    }

    fn initial(&self) -> Result<Bytes, AuthError> {
        #[cfg(feature = "kerberos")]
        {
            Err(AuthError::Unsupported(
                "Kerberos/GSSAPI is feature-gated but the implementation is not yet wired up"
                    .to_owned(),
            ))
        }
        #[cfg(not(feature = "kerberos"))]
        {
            Err(AuthError::Unsupported(
                "Kerberos/GSSAPI requires the `kerberos` feature flag".to_owned(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use magnetar_proto::AuthProvider;

    use super::{SaslKerberos, SaslPlain};

    #[test]
    fn plain_roundtrip_matches_rfc_4616() {
        let p = SaslPlain::new("alice", "s3cret");
        assert_eq!(p.method(), "sasl");
        let bytes = p.initial().expect("initial");
        assert_eq!(bytes.as_ref(), b"\0alice\0s3cret".as_slice());
    }

    #[test]
    fn plain_handles_empty_credentials() {
        let p = SaslPlain::new("", "");
        let bytes = p.initial().expect("initial");
        assert_eq!(bytes.as_ref(), &[0u8, 0u8][..]);
    }

    #[test]
    fn kerberos_returns_unsupported() {
        let p = SaslKerberos::new();
        assert_eq!(p.method(), "sasl");
        let err = p.initial().unwrap_err();
        assert!(err.to_string().contains("Kerberos"), "err={err}");
    }
}
