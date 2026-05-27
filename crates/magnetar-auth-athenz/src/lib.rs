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

#[cfg(feature = "zts")]
pub mod zts;

/// Athenz-specific error returned by the optional ZTS client.
/// Surfaced through [`AuthError::Provider`] when the high-level
/// `AuthProvider::initial` path needs to bubble a ZTS failure.
#[cfg(feature = "zts")]
#[derive(Debug, thiserror::Error)]
pub enum AthenzError {
    /// Configuration problem (bad URL, missing field, etc.).
    #[error("athenz config error: {0}")]
    Config(String),
    /// HTTP / network failure talking to the ZTS endpoint.
    #[error("athenz transport: {0}")]
    Transport(String),
    /// Caller-supplied JWT signer returned an error.
    #[error("athenz signer failure: {0}")]
    SignerFailure(String),
    /// ZTS endpoint returned a non-2xx response.
    #[error("athenz ZTS rejected the role-token request: {0}")]
    ZtsRejected(String),
}

#[cfg(feature = "zts")]
impl From<AthenzError> for AuthError {
    fn from(e: AthenzError) -> Self {
        AuthError::Provider(Box::new(e))
    }
}

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
///
/// `AuthProvider::initial` is sync; the ZTS round-trip is async. The
/// provider therefore exposes two construction paths:
///
/// - [`Self::with_role_token`] — the caller hands a pre-fetched role token (e.g. minted by an
///   external `zts-agent` sidecar). `initial` returns it verbatim.
/// - [`Self::with_zts_client`] (behind `feature = "zts"`) — the provider holds a
///   [`zts::ZtsClient`]. The caller pumps the cache by calling [`Self::refresh_via_zts`] (async)
///   before the connection's first `initial()` invocation; the cached token is what `initial()`
///   returns. The runtime engine's `CommandAuthChallenge` path re-invokes `initial` on every
///   challenge, so subsequent refreshes can be driven from outside.
#[derive(Debug, Clone)]
pub struct AthenzProvider {
    config: AthenzConfig,
    /// Optional pre-fetched role token. When set, `initial()` returns these bytes instead of
    /// performing a ZTS round-trip.
    role_token: std::sync::Arc<std::sync::Mutex<Option<Bytes>>>,
    #[cfg(feature = "zts")]
    zts: Option<std::sync::Arc<zts::ZtsClient>>,
}

impl AthenzProvider {
    /// Construct an Athenz provider configured for ZTS-backed token fetch.
    ///
    /// Without [`Self::with_zts_client`], calling [`AuthProvider::initial`]
    /// on this provider returns [`AuthError::Unsupported`] (the legacy
    /// pre-`zts`-feature behaviour).
    #[must_use]
    pub fn new(config: AthenzConfig) -> Self {
        Self {
            config,
            role_token: std::sync::Arc::new(std::sync::Mutex::new(None)),
            #[cfg(feature = "zts")]
            zts: None,
        }
    }

    /// Construct an Athenz provider with a pre-fetched role token (e.g. produced by an
    /// out-of-band Athenz client). `initial()` returns the token bytes verbatim.
    #[must_use]
    pub fn with_role_token(config: AthenzConfig, role_token: Bytes) -> Self {
        Self {
            config,
            role_token: std::sync::Arc::new(std::sync::Mutex::new(Some(role_token))),
            #[cfg(feature = "zts")]
            zts: None,
        }
    }

    /// Construct an Athenz provider that exchanges a caller-signed JWT
    /// for a role token via the supplied [`zts::ZtsClient`]. Call
    /// [`Self::refresh_via_zts`] before the connection's first use to
    /// warm the cache; subsequent challenges re-invoke `initial()` so
    /// callers wanting automatic refresh wrap a `tokio::task::spawn`
    /// loop around `refresh_via_zts` keyed on the cache's TTL.
    #[cfg(feature = "zts")]
    #[must_use]
    pub fn with_zts_client(config: AthenzConfig, zts: std::sync::Arc<zts::ZtsClient>) -> Self {
        Self {
            config,
            role_token: std::sync::Arc::new(std::sync::Mutex::new(None)),
            zts: Some(zts),
        }
    }

    /// Refresh the cached role token via the configured ZTS client.
    /// No-op when the provider was not built with [`Self::with_zts_client`].
    ///
    /// # Errors
    /// Propagates [`AthenzError`] from the ZTS round-trip.
    #[cfg(feature = "zts")]
    pub async fn refresh_via_zts(&self) -> Result<(), AthenzError> {
        let Some(zts) = self.zts.as_ref() else {
            return Ok(());
        };
        let token = zts.fetch_role_token().await?;
        if let Ok(mut guard) = self.role_token.lock() {
            *guard = Some(token);
        }
        Ok(())
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
        let cached = self.role_token.lock().ok().and_then(|g| g.clone());
        match cached {
            Some(token) => Ok(token),
            None => Err(AuthError::Unsupported(
                "Athenz role token not yet fetched; provide one via AthenzProvider::with_role_token \
                 or call AthenzProvider::refresh_via_zts before the connection's first use"
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
        // Error message names the two recovery paths: pre-fetch via
        // `with_role_token` or refresh via the optional ZTS client.
        let rendered = err.to_string();
        assert!(
            rendered.contains("role token") && rendered.contains("with_role_token"),
            "err={rendered}",
        );
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
