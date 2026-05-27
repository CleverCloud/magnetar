// SPDX-License-Identifier: Apache-2.0

//! SASL `PLAIN` (RFC 4616) provider.
//!
//! Mirrors `org.apache.pulsar.client.impl.auth.AuthenticationSasl` in its PLAIN configuration.
//!
//! **PLAIN sends the password in cleartext on the wire** — the SASL mechanism itself provides
//! no confidentiality. RFC 4616 §1 mandates that PLAIN run only on a transport that has
//! negotiated confidentiality (typically TLS). This module enforces that contract at the
//! provider boundary: the constructors require the caller to affirm TLS up-front, and
//! [`SaslPlain::allow_plaintext`] is the explicit escape hatch for tests / lab environments.

use bytes::Bytes;
use magnetar_proto::{AuthError, AuthProvider};

/// SASL `PLAIN` (RFC 4616) credentials.
#[derive(Debug, Clone)]
pub struct SaslPlain {
    username: String,
    password: String,
    /// When `true`, [`Self::initial`] is allowed to emit the credential
    /// bytes. Defaults to `false` for safety — every constructor takes
    /// an explicit decision.
    transport_ok: bool,
}

impl SaslPlain {
    /// Construct a `PLAIN` provider. `tls_negotiated` is the caller's
    /// affirmation that the surrounding client builder has TLS
    /// configured (typically a `pulsar+ssl://` service URL or an
    /// explicit TLS config). [`Self::initial`] errors with
    /// [`AuthError::Unsupported`] when `tls_negotiated == false`,
    /// refusing to emit the password on a plaintext socket.
    ///
    /// Prefer [`Self::over_tls`] / [`Self::allow_plaintext`] at call
    /// sites for self-documenting intent.
    #[must_use]
    pub fn new(
        username: impl Into<String>,
        password: impl Into<String>,
        tls_negotiated: bool,
    ) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
            transport_ok: tls_negotiated,
        }
    }

    /// Equivalent to `SaslPlain::new(username, password, true)`. Use when
    /// the call site can statically prove TLS is on (e.g. the client
    /// builder has already validated the `pulsar+ssl://` scheme).
    #[must_use]
    pub fn over_tls(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::new(username, password, true)
    }

    /// Explicit escape hatch — produce a `PLAIN` provider that will emit
    /// the password on a plaintext socket. Intended for tests, local lab
    /// setups, or environments where transport confidentiality is
    /// guaranteed out-of-band (a private network segment, an `IPsec`
    /// tunnel, etc.). Production code should call [`Self::over_tls`]
    /// instead.
    #[must_use]
    pub fn allow_plaintext(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::new(username, password, true)
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
        if !self.transport_ok {
            return Err(AuthError::Unsupported(
                "SASL PLAIN refuses to emit credentials over an unaffirmed transport; \
                 use SaslPlain::over_tls or SaslPlain::allow_plaintext explicitly"
                    .to_owned(),
            ));
        }
        Ok(self.encode())
    }
}

#[cfg(test)]
mod tests {
    use magnetar_proto::AuthProvider;

    use super::SaslPlain;

    #[test]
    fn plain_roundtrip_matches_rfc_4616() {
        let p = SaslPlain::over_tls("alice", "s3cret");
        assert_eq!(p.method(), "sasl");
        let bytes = p.initial().expect("initial");
        assert_eq!(bytes.as_ref(), b"\0alice\0s3cret".as_slice());
    }

    #[test]
    fn plain_handles_empty_credentials() {
        let p = SaslPlain::over_tls("", "");
        let bytes = p.initial().expect("initial");
        assert_eq!(bytes.as_ref(), &[0u8, 0u8][..]);
    }

    #[test]
    fn plain_refuses_credentials_when_tls_not_affirmed() {
        let p = SaslPlain::new("alice", "s3cret", false);
        let err = p
            .initial()
            .expect_err("PLAIN must refuse to emit on an unaffirmed transport");
        let rendered = format!("{err}");
        assert!(
            rendered.contains("SASL PLAIN"),
            "error message should name the mechanism: {rendered}",
        );
    }

    #[test]
    fn plain_allow_plaintext_emits() {
        let p = SaslPlain::allow_plaintext("alice", "s3cret");
        let bytes = p.initial().expect("initial");
        assert_eq!(bytes.as_ref(), b"\0alice\0s3cret".as_slice());
    }
}
