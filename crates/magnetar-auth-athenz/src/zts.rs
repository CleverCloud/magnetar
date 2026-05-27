// SPDX-License-Identifier: Apache-2.0

//! Athenz ZTS round-trip — opt-in HTTP client + caching layer.
//!
//! The Athenz `ZTS` REST endpoint takes a caller-signed JWT (an Athenz
//! `n-token` or `OAuth2` `client_credentials` grant) and returns an
//! Athenz role token. This module wraps the HTTP exchange + the
//! expiry-aware caching, leaving the JWT signing as a pluggable
//! [`JwtSigner`] trait — the magnetar workspace doesn't currently
//! ship a concrete signer impl because the choice (jsonwebtoken vs.
//! aws-lc-rs vs. ring) is downstream-policy-dependent (FIPS posture,
//! key-management story, hardware-backed key support, …).
//!
//! Callers that already have a role token out-of-band can keep using
//! [`AthenzProvider::with_role_token`](crate::AthenzProvider::with_role_token)
//! — the lightweight path that skips this module entirely.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::AthenzError;

/// Pluggable JWT signer. The magnetar workspace doesn't ship a
/// concrete implementation today — callers wire one based on their
/// crypto-provider posture (jsonwebtoken with `aws-lc-rs`, `ring`, or
/// an external signing service / HSM).
///
/// The signed JWT is sent to the `ZTS` endpoint as the bearer credential
/// in the `Authorization` header (or as the `client_assertion` form
/// field on the `OAuth2` grant flavour, depending on
/// [`ZtsGrant::ClientCredentials`]).
pub trait JwtSigner: Send + Sync + std::fmt::Debug {
    /// Sign the supplied JOSE-encoded claims and return the compact
    /// serialisation (`header.payload.signature`, all base64url).
    fn sign(&self, claims: &ZtsClaims) -> Result<String, AthenzError>;
}

/// Athenz ZTS role-token request claims. Mirrors the n-token format
/// the Athenz UI / ZTS server expects.
#[derive(Debug, Clone, Serialize)]
pub struct ZtsClaims {
    /// Issuer — tenant `domain.service`.
    pub iss: String,
    /// Subject — the tenant service.
    pub sub: String,
    /// Audience — typically the ZTS URL.
    pub aud: String,
    /// Key id matching the Athenz public key registered for the tenant.
    pub kid: String,
    /// Issued-at, seconds since UNIX epoch.
    pub iat: u64,
    /// Expiry, seconds since UNIX epoch.
    pub exp: u64,
}

/// Which ZTS grant flavour to use. Default is
/// [`Self::ClientCredentials`] (modern `OAuth2` grant); `NToken` is kept
/// for callers stuck on older ZTS deployments.
#[derive(Debug, Default, Clone, Copy)]
pub enum ZtsGrant {
    /// Athenz `n-token` over the legacy `/zts/v1/domain/<provider>/token` path.
    NToken,
    /// `OAuth2` `client_credentials` over `/zts/v1/oauth2/token`.
    #[default]
    ClientCredentials,
}

/// ZTS-issued role token + the deadline at which it should be
/// refreshed. The cache evicts entries past `refresh_at` so the next
/// `initial()` triggers a fresh round-trip.
#[derive(Debug, Clone)]
pub(crate) struct CachedRoleToken {
    pub token: Bytes,
    pub refresh_at: std::time::Instant,
}

/// ZTS endpoint response — narrow subset of what the Athenz ZTS server
/// returns. The full schema carries lots of optional fields that
/// magnetar doesn't need (`granted_role_name`, `signed_policy_data`, …).
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ZtsTokenResponse {
    /// The opaque role token bytes (base64-encoded by the server).
    pub access_token: String,
    /// Token validity in seconds.
    #[serde(default = "default_token_ttl")]
    pub expires_in: u64,
}

fn default_token_ttl() -> u64 {
    // Athenz ZTS default role-token TTL is 1 hour.
    3600
}

/// HTTP client for the Athenz ZTS REST endpoint. Wraps a `reqwest`
/// client with the role-token cache + a refresh leeway so the next
/// `initial()` after a near-expiry hit pre-fetches without waiting
/// for the token to actually expire.
#[derive(Debug)]
pub struct ZtsClient {
    zts_url: url::Url,
    grant: ZtsGrant,
    signer: Arc<dyn JwtSigner>,
    http: reqwest::Client,
    cache: tokio::sync::Mutex<Option<CachedRoleToken>>,
    refresh_leeway: Duration,
}

impl ZtsClient {
    /// Construct a fresh ZTS client.
    ///
    /// # Errors
    /// Returns [`AthenzError::Config`] if `zts_url` is not a valid URL
    /// or if the default `reqwest::Client` cannot be built.
    pub fn new(
        zts_url: impl AsRef<str>,
        grant: ZtsGrant,
        signer: Arc<dyn JwtSigner>,
    ) -> Result<Self, AthenzError> {
        let zts_url = url::Url::parse(zts_url.as_ref())
            .map_err(|e| AthenzError::Config(format!("invalid zts_url: {e}")))?;
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| AthenzError::Transport(format!("build reqwest client: {e}")))?;
        Ok(Self {
            zts_url,
            grant,
            signer,
            http,
            cache: tokio::sync::Mutex::new(None),
            refresh_leeway: Duration::from_secs(60),
        })
    }

    /// Override the refresh leeway (default 60s). When the cached
    /// token's remaining TTL drops below this, the next request
    /// triggers a fresh ZTS round-trip.
    #[must_use]
    pub fn with_refresh_leeway(mut self, leeway: Duration) -> Self {
        self.refresh_leeway = leeway;
        self
    }

    /// Fetch a fresh role token, populating the cache. The caller is
    /// expected to drive this from `AuthProvider::initial` — magnetar
    /// itself doesn't run a background refresh loop for the cache
    /// (the connection driver re-invokes the provider on auth
    /// challenges).
    ///
    /// # Errors
    /// - [`AthenzError::SignerFailure`] if the JWT signer trips.
    /// - [`AthenzError::Transport`] on HTTP failure (connect, TLS, timeout).
    /// - [`AthenzError::ZtsRejected`] on a non-2xx response.
    pub async fn fetch_role_token(&self) -> Result<Bytes, AthenzError> {
        // Cache hit?
        {
            let guard = self.cache.lock().await;
            if let Some(entry) = guard.as_ref() {
                if entry.refresh_at > std::time::Instant::now() {
                    return Ok(entry.token.clone());
                }
            }
        }

        // Cache miss — sign + POST + cache.
        let claims = self.claims_now()?;
        let jwt = self.signer.sign(&claims)?;
        let response = self.post_token(&jwt).await?;
        let token = Bytes::from(response.access_token.into_bytes());
        let ttl = Duration::from_secs(response.expires_in);
        let refresh_at =
            std::time::Instant::now() + ttl.checked_sub(self.refresh_leeway).unwrap_or(ttl);
        *self.cache.lock().await = Some(CachedRoleToken {
            token: token.clone(),
            refresh_at,
        });
        Ok(token)
    }

    fn claims_now(&self) -> Result<ZtsClaims, AthenzError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| AthenzError::Config(format!("system clock pre-epoch: {e}")))?
            .as_secs();
        Ok(ZtsClaims {
            // The caller's signer is expected to override iss/sub/kid
            // with its own configuration; the placeholders here just
            // stamp now / exp so callers that wrap a partial signer
            // don't have to recompute them.
            iss: String::new(),
            sub: String::new(),
            aud: self.zts_url.as_str().to_owned(),
            kid: String::new(),
            iat: now,
            exp: now + 60,
        })
    }

    async fn post_token(&self, jwt: &str) -> Result<ZtsTokenResponse, AthenzError> {
        let path = match self.grant {
            ZtsGrant::NToken => "domain/sys.auth/token",
            ZtsGrant::ClientCredentials => "oauth2/token",
        };
        let url = self
            .zts_url
            .join(path)
            .map_err(|e| AthenzError::Config(format!("zts_url join {path}: {e}")))?;
        let response = self
            .http
            .post(url)
            .bearer_auth(jwt)
            .send()
            .await
            .map_err(|e| AthenzError::Transport(format!("zts post: {e}")))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| AthenzError::Transport(format!("zts body: {e}")))?;
        if !status.is_success() {
            return Err(AthenzError::ZtsRejected(format!(
                "zts returned {status}: {body}"
            )));
        }
        serde_json::from_str(&body).map_err(|e| {
            AthenzError::Config(format!(
                "zts response not JSON-decodable (body={body}): {e}"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct StaticJwtSigner;

    impl JwtSigner for StaticJwtSigner {
        fn sign(&self, _claims: &ZtsClaims) -> Result<String, AthenzError> {
            Ok("header.payload.signature".to_owned())
        }
    }

    #[test]
    fn zts_client_builds_with_static_signer() {
        let signer: Arc<dyn JwtSigner> = Arc::new(StaticJwtSigner);
        let client = ZtsClient::new(
            "https://zts.example.invalid:4443/zts/v1/",
            ZtsGrant::ClientCredentials,
            signer,
        )
        .expect("client builds");
        assert_eq!(client.refresh_leeway, Duration::from_secs(60));
    }

    #[test]
    fn refresh_leeway_override() {
        let signer: Arc<dyn JwtSigner> = Arc::new(StaticJwtSigner);
        let client = ZtsClient::new(
            "https://zts.example.invalid:4443/zts/v1/",
            ZtsGrant::ClientCredentials,
            signer,
        )
        .expect("client builds")
        .with_refresh_leeway(Duration::from_secs(120));
        assert_eq!(client.refresh_leeway, Duration::from_secs(120));
    }

    #[test]
    fn invalid_zts_url_returns_config_error() {
        let signer: Arc<dyn JwtSigner> = Arc::new(StaticJwtSigner);
        let err = ZtsClient::new("not-a-url", ZtsGrant::ClientCredentials, signer).unwrap_err();
        assert!(matches!(err, AthenzError::Config(_)));
    }

    #[test]
    fn zts_grant_default_is_client_credentials() {
        assert!(matches!(ZtsGrant::default(), ZtsGrant::ClientCredentials));
    }
}
