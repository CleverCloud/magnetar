// SPDX-License-Identifier: Apache-2.0

//! Athenz auth provider for magnetar.
//!
//! Athenz is a Yahoo-originated AuthN/AuthZ service. The Pulsar Java client (`pulsar-broker-
//! auth-athenz` + `org.apache.pulsar.client.impl.auth.AuthenticationAthenz`) fetches a role
//! token from a ZTS endpoint using the tenant's private key, then advertises that role token
//! via the standard `auth_data` channel.
//!
//! M6 ships the **trait surface and configuration struct**, so downstream code can compile
//! against the Athenz auth method, but [`AthenzProvider::initial`] surfaces
//! [`AuthError::Unsupported`] — the ZTS round-trip is a niche, multi-stakeholder dependency
//! that we defer until a real consumer needs it. Callers that already have a role token
//! out-of-band can use [`AthenzProvider::with_role_token`] which uses the supplied token as
//! the `auth_data` payload directly.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

use bytes::Bytes;
use magnetar_proto::{AuthError, AuthProvider};
use serde::{Deserialize, Serialize};

/// Athenz tenant/service configuration.
///
/// The `Debug` impl is manual: the `private_key_pem` field is redacted
/// behind a `<redacted>` sentinel so accidental `{:?}` logging of the
/// config can't leak the PEM body. Every other field is surfaced as-is
/// (they're all metadata: domain names, key ids, URLs).
#[derive(Clone, Serialize, Deserialize)]
pub struct AthenzConfig {
    /// Tenant domain (e.g. `"mydomain"`).
    pub tenant_domain: String,
    /// Tenant service name (e.g. `"myservice"`).
    pub tenant_service: String,
    /// Provider domain registered with Athenz (e.g. `"pulsar.tenant"`).
    pub provider_domain: String,
    /// Athenz key id used by the ZTS server to verify the signature.
    pub key_id: String,
    /// Tenant private key PEM (RSA). Redacted from [`std::fmt::Debug`].
    pub private_key_pem: String,
    /// Athenz ZTS endpoint URL.
    pub zts_url: String,
    /// Optional Athenz principal header name override.
    #[serde(default)]
    pub principal_header: Option<String>,
    /// Optional role header name override.
    #[serde(default)]
    pub role_header: Option<String>,
}

impl std::fmt::Debug for AthenzConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AthenzConfig")
            .field("tenant_domain", &self.tenant_domain)
            .field("tenant_service", &self.tenant_service)
            .field("provider_domain", &self.provider_domain)
            .field("key_id", &self.key_id)
            .field("private_key_pem", &"<redacted>")
            .field("zts_url", &self.zts_url)
            .field("principal_header", &self.principal_header)
            .field("role_header", &self.role_header)
            .finish()
    }
}

/// Athenz auth provider.
#[derive(Debug, Clone)]
pub struct AthenzProvider {
    config: AthenzConfig,
    /// Optional pre-fetched role token. When set, `initial()` returns these bytes instead of
    /// performing a ZTS round-trip.
    role_token: Option<Bytes>,
}

impl AthenzProvider {
    /// Construct an Athenz provider configured for ZTS-backed token fetch.
    ///
    /// **M6 status:** the ZTS round-trip is not implemented; calling [`AuthProvider::initial`]
    /// on this provider returns [`AuthError::Unsupported`].
    #[must_use]
    pub fn new(config: AthenzConfig) -> Self {
        Self {
            config,
            role_token: None,
        }
    }

    /// Construct an Athenz provider with a pre-fetched role token (e.g. produced by an
    /// out-of-band Athenz client). `initial()` returns the token bytes verbatim.
    #[must_use]
    pub fn with_role_token(config: AthenzConfig, role_token: Bytes) -> Self {
        Self {
            config,
            role_token: Some(role_token),
        }
    }

    /// Borrow the Athenz configuration.
    #[must_use]
    pub fn config(&self) -> &AthenzConfig {
        &self.config
    }
}

impl AuthProvider for AthenzProvider {
    fn method(&self) -> &str {
        "athenz"
    }

    fn initial(&self) -> Result<Bytes, AuthError> {
        match &self.role_token {
            Some(token) => Ok(token.clone()),
            None => Err(AuthError::Unsupported(
                "Athenz ZTS round-trip not yet implemented; provide a pre-fetched role token \
                 via AthenzProvider::with_role_token"
                    .to_owned(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use magnetar_proto::AuthProvider;

    use super::{AthenzConfig, AthenzProvider};

    fn sample_config() -> AthenzConfig {
        AthenzConfig {
            tenant_domain: "mydomain".to_owned(),
            tenant_service: "myservice".to_owned(),
            provider_domain: "pulsar.tenant".to_owned(),
            key_id: "key0".to_owned(),
            private_key_pem: "-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n"
                .to_owned(),
            zts_url: "https://zts.example.invalid:4443/zts/v1".to_owned(),
            principal_header: None,
            role_header: None,
        }
    }

    #[test]
    fn config_serialises_round_trip() {
        let cfg = sample_config();
        let json = serde_json::to_string(&cfg).expect("serialize");
        let back: AthenzConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.tenant_domain, cfg.tenant_domain);
        assert_eq!(back.zts_url, cfg.zts_url);
    }

    #[test]
    fn provider_default_returns_unsupported() {
        let p = AthenzProvider::new(sample_config());
        assert_eq!(p.method(), "athenz");
        let err = p.initial().unwrap_err();
        assert!(err.to_string().contains("ZTS"), "err={err}");
    }

    #[test]
    fn provider_with_token_returns_token_bytes() {
        let p = AthenzProvider::with_role_token(
            sample_config(),
            Bytes::from_static(b"role-token-bytes"),
        );
        let bytes = p.initial().expect("initial");
        assert_eq!(bytes.as_ref(), b"role-token-bytes".as_slice());
    }

    #[test]
    fn config_debug_redacts_private_key_pem() {
        let cfg = sample_config();
        let rendered = format!("{cfg:?}");
        assert!(
            !rendered.contains("BEGIN PRIVATE KEY"),
            "PEM body leaked through Debug: {rendered}",
        );
        assert!(
            rendered.contains("<redacted>"),
            "expected redaction sentinel in {rendered}",
        );
        assert!(
            rendered.contains("mydomain"),
            "non-secret fields should still surface in Debug: {rendered}",
        );
    }
}
