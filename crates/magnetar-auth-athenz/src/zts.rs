// SPDX-License-Identifier: Apache-2.0

//! Athenz ZTS round-trip — opt-in HTTP exchange behind a pluggable trait.
//!
//! The Athenz `ZTS` REST endpoint takes a caller-signed JWT (an Athenz
//! `n-token` or `OAuth2` `client_credentials` grant) and returns an
//! Athenz role token. The exchange is split into two cleanly-separated
//! pieces so the deterministic-simulation engine (which cannot speak
//! HTTPS) can still exercise the refresh / cache mechanics
//! (ADR-0030 §moonpool, ADR-0024 layers c/d):
//!
//! - [`ZtsClient`] — the **trait**. `exchange` takes a signed JWT bearer credential and returns a
//!   [`RoleTokenResponse`]. Tests inject a scripted fake; production wires [`HttpZtsClient`].
//! - [`HttpZtsClient`] — the production `reqwest`-backed impl. Tokio-only at runtime.
//!
//! Claim construction + JWT signing + the expiry-aware cache live in
//! [`crate::AthenzProvider`], which owns the tenant [`crate::AthenzConfig`],
//! the [`JwtSigner`], and the injected `wall_clock` — keeping this module
//! a thin HTTP seam.
//!
//! Callers that already have a role token out-of-band can keep using
//! [`AthenzProvider::with_role_token`](crate::AthenzProvider::with_role_token)
//! — the lightweight path that skips this module entirely.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::AthenzError;

/// Pluggable JWT signer (RS256). Concrete backends ship behind the
/// crypto-provider feature matrix in [`crate::jwt_signer`]
/// (`AwsLcRsSigner` / `RingSigner`, ADR-0030 + ADR-0035); callers without
/// either crypto feature wire their own (jsonwebtoken, an HSM bridge, …).
///
/// The signed JWT is sent to the `ZTS` endpoint as the bearer credential
/// in the `Authorization` header.
pub trait JwtSigner: Send + Sync + std::fmt::Debug {
    /// Sign the supplied JOSE-encoded claims and return the compact
    /// serialisation (`header.payload.signature`, all base64url).
    ///
    /// # Errors
    /// Surfaces [`AthenzError::SignerFailure`] when the underlying RSA
    /// signing operation fails.
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
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ZtsGrant {
    /// Athenz `n-token` over the legacy `/zts/v1/domain/<provider>/token` path.
    NToken,
    /// `OAuth2` `client_credentials` over `/zts/v1/oauth2/token`.
    #[default]
    ClientCredentials,
}

/// ZTS endpoint response — narrow subset of what the Athenz ZTS server
/// returns. The full schema carries lots of optional fields that
/// magnetar doesn't need (`granted_role_name`, `signed_policy_data`, …).
#[derive(Debug, Clone, Deserialize)]
pub struct RoleTokenResponse {
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

/// The ZTS exchange seam. Given a signed JWT bearer credential, return a
/// fresh [`RoleTokenResponse`]. Production uses [`HttpZtsClient`]; the
/// moonpool / differential test layers inject a scripted fake so the
/// provider's refresh + cache state machine is exercised without HTTPS.
#[async_trait]
pub trait ZtsClient: Send + Sync + std::fmt::Debug {
    /// Exchange the signed JWT for an Athenz role token.
    ///
    /// # Errors
    /// - [`AthenzError::Transport`] on HTTP failure (connect, TLS, timeout).
    /// - [`AthenzError::ZtsRejected`] on a non-2xx response.
    /// - [`AthenzError::Config`] when the response body is not decodable.
    async fn exchange(&self, signed_jwt: &str) -> Result<RoleTokenResponse, AthenzError>;
}

/// Production `reqwest`-backed [`ZtsClient`]. Posts the signed JWT to the
/// Athenz ZTS REST endpoint and parses the role-token response. The
/// expiry-aware cache lives in [`crate::AthenzProvider`]; this type is a
/// stateless HTTP shim.
#[derive(Debug)]
pub struct HttpZtsClient {
    zts_url: url::Url,
    grant: ZtsGrant,
    http: reqwest::Client,
}

impl HttpZtsClient {
    /// Construct a fresh HTTP ZTS client.
    ///
    /// # Errors
    /// Returns [`AthenzError::Config`] if `zts_url` is not a valid URL
    /// or if the default `reqwest::Client` cannot be built.
    pub fn new(zts_url: impl AsRef<str>, grant: ZtsGrant) -> Result<Self, AthenzError> {
        let zts_url = url::Url::parse(zts_url.as_ref())
            .map_err(|e| AthenzError::Config(format!("invalid zts_url: {e}")))?;
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| AthenzError::Transport(format!("build reqwest client: {e}")))?;
        Ok(Self {
            zts_url,
            grant,
            http,
        })
    }

    /// The grant flavour this client posts under.
    #[must_use]
    pub fn grant(&self) -> ZtsGrant {
        self.grant
    }
}

#[async_trait]
impl ZtsClient for HttpZtsClient {
    async fn exchange(&self, signed_jwt: &str) -> Result<RoleTokenResponse, AthenzError> {
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
            .bearer_auth(signed_jwt)
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

    #[test]
    fn http_client_builds_with_valid_url() {
        let client = HttpZtsClient::new(
            "https://zts.example.invalid:4443/zts/v1/",
            ZtsGrant::ClientCredentials,
        )
        .expect("client builds");
        assert_eq!(client.grant(), ZtsGrant::ClientCredentials);
    }

    #[test]
    fn invalid_zts_url_returns_config_error() {
        let err = HttpZtsClient::new("not-a-url", ZtsGrant::ClientCredentials).unwrap_err();
        assert!(matches!(err, AthenzError::Config(_)));
    }

    #[test]
    fn zts_grant_default_is_client_credentials() {
        assert!(matches!(ZtsGrant::default(), ZtsGrant::ClientCredentials));
    }
}
