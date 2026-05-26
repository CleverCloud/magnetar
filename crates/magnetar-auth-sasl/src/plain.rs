// SPDX-License-Identifier: Apache-2.0

//! SASL `PLAIN` (RFC 4616) provider.
//!
//! Mirrors `org.apache.pulsar.client.impl.auth.AuthenticationSasl` in its PLAIN configuration.

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

#[cfg(test)]
mod tests {
    use magnetar_proto::AuthProvider;

    use super::SaslPlain;

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
}
