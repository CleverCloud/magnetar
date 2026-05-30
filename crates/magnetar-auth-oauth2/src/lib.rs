// SPDX-License-Identifier: Apache-2.0

//! `OAuth2` `ClientCredentialsFlow` auth provider for magnetar.
//!
//! Mirrors `org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2` and its underlying
//! `ClientCredentialsFlow`. The Pulsar broker accepts the resulting JWT through the regular
//! `token` auth method, so this provider reports `method() = "token"` exactly like the Java client
//! (see `AuthenticationOAuth2.AUTH_METHOD_NAME`).
//!
//! # Surface
//!
//! [`ClientCredentialsFlow`] owns the IDP endpoint, credentials, and the cached token. Two entry
//! points exist:
//!
//! - [`ClientCredentialsFlow::fetch_token`] — unconditional token exchange against the IDP.
//! - [`ClientCredentialsFlow::ensure_fresh`] — fetch only when the cache is empty or within
//!   [`REFRESH_LEEWAY`] of expiry. Engines call this before constructing `CommandConnect`.
//!
//! The synchronous [`AuthProvider`] surface returns whatever is cached. If the cache is empty
//! [`AuthProvider::initial`] surfaces [`AuthError::Invalid`] so the engine knows it must call
//! [`ClientCredentialsFlow::ensure_fresh`] first. This matches the Java client's "first call
//! triggers a blocking exchange" semantics, except we make the async boundary explicit instead of
//! hiding it behind a blocking call inside `getAuthData()`.
//!
//! # Security
//!
//! The [`Debug`] impls on this crate's public types redact `client_secret` and private-key bytes.
//! Never log a [`ClientCredentialsFlow`] or a [`Credentials`] with the standard formatter without
//! reviewing what reaches the log sink.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

use core::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use magnetar_proto::{AuthError, AuthProvider};
use parking_lot::Mutex;
use reqwest::Client;
use serde::Deserialize;
use url::Url;

/// How close to the token's deadline before [`ClientCredentialsFlow::ensure_fresh`] performs a
/// refresh exchange. Matches the magnitude of the Java client's default 2-second mandatory-stop
/// (`AuthenticationOAuth2.buildBackoff`) rounded up to a more conservative network margin.
pub const REFRESH_LEEWAY: Duration = Duration::from_secs(30);

/// Standard `OpenID` Connect token endpoint suffix appended to the issuer URL when the caller does
/// not supply an explicit token endpoint. Matches Keycloak / Auth0 / generic OIDC discovery.
pub const TOKEN_ENDPOINT_SUFFIX: &str = "protocol/openid-connect/token";

/// Errors surfaced by [`ClientCredentialsFlow`].
#[derive(Debug, thiserror::Error)]
pub enum OAuth2Error {
    /// Configuration was rejected before any network call (invalid URL, blank field, etc.).
    #[error("invalid OAuth2 configuration: {0}")]
    Config(String),

    /// The HTTP exchange with the IDP failed (transport, TLS, timeout).
    #[error("OAuth2 token endpoint transport error: {0}")]
    Transport(#[source] reqwest::Error),

    /// The IDP returned a non-2xx response.
    ///
    /// The `Display` implementation REDACTS the body to defuse CWE-532
    /// (sensitive-data exposure in logs): IDP error payloads frequently echo
    /// the original POST form back — `client_secret=...`, refresh tokens,
    /// session JWTs — and operators routinely log error variants verbatim.
    /// Use [`Self::body`] (or a `tracing::trace!` span gated behind an
    /// opt-in flag) to get the raw bytes when debugging.
    #[error("OAuth2 token endpoint returned HTTP {status} [body redacted, {} bytes; use OAuth2Error::body() to inspect]", body.len())]
    Idp {
        /// HTTP status code from the IDP.
        status: u16,
        /// Response body — usually a JSON `{"error": "...", "error_description": "..."}`.
        /// Available via [`Self::body`]; redacted from the `Display` output.
        body: String,
    },

    /// The IDP response was not parseable as a [`TokenResponse`].
    #[error("OAuth2 token endpoint returned malformed JSON: {0}")]
    Decode(#[source] serde_json::Error),
}

impl OAuth2Error {
    /// Inspect the raw IDP response body for an [`OAuth2Error::Idp`] variant.
    /// Returns `None` for every other variant. The body is deliberately NOT
    /// included in `Display` / `to_string` output (CWE-532, F7) — IDP error
    /// payloads frequently echo the original POST form back and operators
    /// routinely log error variants verbatim.
    #[must_use]
    pub fn body(&self) -> Option<&str> {
        match self {
            OAuth2Error::Idp { body, .. } => Some(body),
            _ => None,
        }
    }
}

impl From<OAuth2Error> for AuthError {
    fn from(err: OAuth2Error) -> Self {
        AuthError::Provider(Box::new(err))
    }
}

/// Parsed RFC 6749 / OIDC token endpoint response.
///
/// Mirrors `org.apache.pulsar.client.impl.auth.oauth2.protocol.TokenResult`. Unknown fields are
/// ignored so the same decoder works against Keycloak, Auth0, Okta, Azure AD, and bespoke IDPs.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TokenResponse {
    /// The JWT (or opaque token) the broker will accept as a bearer credential.
    pub access_token: String,
    /// Optional ID token from OIDC-aware IDPs.
    #[serde(default)]
    pub id_token: Option<String>,
    /// Optional refresh token; unused by client-credentials but parsed for completeness.
    #[serde(default)]
    pub refresh_token: Option<String>,
    /// Token lifetime in seconds.
    pub expires_in: u64,
    /// Optional token type (`Bearer` for OIDC). Parsed for completeness.
    #[serde(default)]
    pub token_type: Option<String>,
}

/// Credentials used at the token endpoint.
///
/// Both variants ultimately resolve to a `client_id` + `client_secret` posted to the IDP — the
/// `KeyFile` variant exists to match the Java client's habit of carrying both in a single JSON
/// blob (`org.apache.pulsar.client.impl.auth.oauth2.KeyFile`).
#[derive(Clone)]
pub enum Credentials {
    /// `client_secret_post` — secret posted alongside the client id.
    ClientSecret {
        /// IDP-issued client identifier.
        client_id: String,
        /// IDP-issued client secret.
        client_secret: String,
    },
    /// `client_credentials` driven from a Pulsar-style key file (`client_id` + `client_secret`).
    KeyFile {
        /// IDP-issued client identifier.
        client_id: String,
        /// IDP-issued client secret.
        client_secret: String,
    },
}

impl Credentials {
    fn client_id(&self) -> &str {
        match self {
            Self::ClientSecret { client_id, .. } | Self::KeyFile { client_id, .. } => client_id,
        }
    }

    fn client_secret(&self) -> &str {
        match self {
            Self::ClientSecret { client_secret, .. } | Self::KeyFile { client_secret, .. } => {
                client_secret
            }
        }
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Redact `client_secret` so calling `Debug` on the provider never spills credentials to
        // tracing or stdout. The variant name and `client_id` are safe to surface; the secret is
        // replaced with a fixed sentinel.
        match self {
            Self::ClientSecret { client_id, .. } => f
                .debug_struct("ClientSecret")
                .field("client_id", client_id)
                .field("client_secret", &"<redacted>")
                .finish(),
            Self::KeyFile { client_id, .. } => f
                .debug_struct("KeyFile")
                .field("client_id", client_id)
                .field("client_secret", &"<redacted>")
                .finish(),
        }
    }
}

/// Monotonic clock surface that lets tests inject a virtual `now`. Production uses [`Instant::now`]
/// via the [`SystemClock`] default.
pub trait Clock: Send + Sync + 'static + fmt::Debug {
    /// Current monotonic instant.
    fn now(&self) -> Instant;
}

/// Default [`Clock`] backed by [`Instant::now`].
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Cached access-token entry, keyed on a monotonic deadline.
#[derive(Debug, Clone)]
struct CachedToken {
    access_token: Bytes,
    deadline: Instant,
}

impl CachedToken {
    fn is_expiring(&self, now: Instant, leeway: Duration) -> bool {
        now + leeway >= self.deadline
    }
}

/// `OAuth2` `client_credentials` auth provider.
///
/// Construct with [`ClientCredentialsFlow::builder`], then call
/// [`ClientCredentialsFlow::ensure_fresh`] before every connection attempt. The cached access
/// token is exposed through the [`AuthProvider`] surface so the existing `CommandConnect` /
/// `AUTH_CHALLENGE` machinery in [`magnetar_proto`] consumes it without further plumbing.
pub struct ClientCredentialsFlow {
    issuer_url: Url,
    token_endpoint: Url,
    audience: Option<String>,
    scope: Option<String>,
    credentials: Credentials,
    http: Client,
    cache: Arc<Mutex<Option<CachedToken>>>,
    clock: Arc<dyn Clock>,
    leeway: Duration,
}

impl fmt::Debug for ClientCredentialsFlow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The credentials Debug impl already redacts; we still hide the cached token bytes since
        // they are a live bearer credential. `http` and `clock` are surfaced through opaque
        // placeholders so the manual impl covers every field (clippy::missing_fields_in_debug)
        // without leaking reqwest's middleware chain or virtual-clock internals.
        let cached = self.cache.lock();
        f.debug_struct("ClientCredentialsFlow")
            .field("issuer_url", &self.issuer_url.as_str())
            .field("token_endpoint", &self.token_endpoint.as_str())
            .field("audience", &self.audience)
            .field("scope", &self.scope)
            .field("credentials", &self.credentials)
            .field("http", &"<reqwest::Client>")
            .field("clock", &self.clock)
            .field("leeway", &self.leeway)
            .field("cached", &cached.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Builder for [`ClientCredentialsFlow`].
#[derive(Debug)]
pub struct ClientCredentialsFlowBuilder {
    issuer_url: Option<Url>,
    token_endpoint: Option<Url>,
    audience: Option<String>,
    scope: Option<String>,
    credentials: Option<Credentials>,
    http: Option<Client>,
    clock: Option<Arc<dyn Clock>>,
    leeway: Duration,
}

impl Default for ClientCredentialsFlowBuilder {
    fn default() -> Self {
        Self {
            issuer_url: None,
            token_endpoint: None,
            audience: None,
            scope: None,
            credentials: None,
            http: None,
            clock: None,
            leeway: REFRESH_LEEWAY,
        }
    }
}

impl ClientCredentialsFlowBuilder {
    /// Set the IDP issuer URL. The token endpoint defaults to `<issuer>/<TOKEN_ENDPOINT_SUFFIX>`
    /// unless explicitly overridden via [`Self::token_endpoint`].
    #[must_use]
    pub fn issuer_url(mut self, url: Url) -> Self {
        self.issuer_url = Some(url);
        self
    }

    /// Override the token endpoint. Defaults to the OIDC standard suffix appended to the issuer.
    #[must_use]
    pub fn token_endpoint(mut self, url: Url) -> Self {
        self.token_endpoint = Some(url);
        self
    }

    /// Set the `OAuth2` `audience` claim — required by Auth0, optional elsewhere.
    #[must_use]
    pub fn audience(mut self, audience: impl Into<String>) -> Self {
        self.audience = Some(audience.into());
        self
    }

    /// Set the `OAuth2` `scope` claim.
    #[must_use]
    pub fn scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    /// Set the credentials.
    #[must_use]
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Supply a custom [`reqwest::Client`]. If unset, a default rustls-backed client is built.
    #[must_use]
    pub fn http_client(mut self, http: Client) -> Self {
        self.http = Some(http);
        self
    }

    /// Inject a custom [`Clock`] (used by tests).
    #[must_use]
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Override the refresh leeway (default [`REFRESH_LEEWAY`]).
    #[must_use]
    pub fn refresh_leeway(mut self, leeway: Duration) -> Self {
        self.leeway = leeway;
        self
    }

    /// Finish the builder. Fails with [`OAuth2Error::Config`] when required fields are missing or
    /// when the default `reqwest::Client` cannot be constructed.
    pub fn build(self) -> Result<ClientCredentialsFlow, OAuth2Error> {
        let issuer_url = self
            .issuer_url
            .ok_or_else(|| OAuth2Error::Config("issuer_url is required".to_owned()))?;
        let credentials = self
            .credentials
            .ok_or_else(|| OAuth2Error::Config("credentials are required".to_owned()))?;
        if credentials.client_id().is_empty() {
            return Err(OAuth2Error::Config(
                "credentials.client_id must not be empty".to_owned(),
            ));
        }
        if credentials.client_secret().is_empty() {
            return Err(OAuth2Error::Config(
                "credentials.client_secret must not be empty".to_owned(),
            ));
        }
        let token_endpoint = match self.token_endpoint {
            Some(url) => url,
            None => default_token_endpoint(&issuer_url)?,
        };
        let http = match self.http {
            Some(http) => http,
            None => Client::builder().build().map_err(OAuth2Error::Transport)?,
        };
        let clock: Arc<dyn Clock> = self.clock.unwrap_or_else(|| Arc::new(SystemClock));
        Ok(ClientCredentialsFlow {
            issuer_url,
            token_endpoint,
            audience: self.audience,
            scope: self.scope,
            credentials,
            http,
            cache: Arc::new(Mutex::new(None)),
            clock,
            leeway: self.leeway,
        })
    }
}

fn default_token_endpoint(issuer: &Url) -> Result<Url, OAuth2Error> {
    // `Url::join` resolves the suffix relative to the issuer, preserving the issuer's path
    // (Keycloak realms live under `/realms/<name>`). We force a trailing slash so the suffix is
    // appended rather than replacing the last path segment.
    let mut base = issuer.clone();
    if !base.path().ends_with('/') {
        let path = format!("{}/", base.path());
        base.set_path(&path);
    }
    base.join(TOKEN_ENDPOINT_SUFFIX)
        .map_err(|err| OAuth2Error::Config(format!("invalid issuer_url: {err}")))
}

impl ClientCredentialsFlow {
    /// Construct a [`ClientCredentialsFlowBuilder`].
    #[must_use]
    pub fn builder() -> ClientCredentialsFlowBuilder {
        ClientCredentialsFlowBuilder::default()
    }

    /// Configured token endpoint.
    #[must_use]
    pub fn token_endpoint(&self) -> &Url {
        &self.token_endpoint
    }

    /// Configured issuer URL.
    #[must_use]
    pub fn issuer_url(&self) -> &Url {
        &self.issuer_url
    }

    /// Perform an unconditional `client_credentials` token exchange against the IDP.
    ///
    /// The result is also cached, so subsequent [`AuthProvider::initial`] calls succeed.
    pub async fn fetch_token(&self) -> Result<TokenResponse, OAuth2Error> {
        let mut form: Vec<(&str, &str)> = Vec::with_capacity(5);
        form.push(("grant_type", "client_credentials"));
        form.push(("client_id", self.credentials.client_id()));
        form.push(("client_secret", self.credentials.client_secret()));
        if let Some(audience) = self.audience.as_deref() {
            form.push(("audience", audience));
        }
        if let Some(scope) = self.scope.as_deref() {
            form.push(("scope", scope));
        }
        tracing::debug!(
            target: "magnetar::auth::oauth2",
            token_endpoint = %self.token_endpoint,
            client_id = self.credentials.client_id(),
            "issuing OAuth2 client_credentials exchange",
        );
        let response = self
            .http
            .post(self.token_endpoint.as_str())
            .header(reqwest::header::ACCEPT, "application/json")
            .form(&form)
            .send()
            .await
            .map_err(OAuth2Error::Transport)?;
        let status = response.status();
        let body = response.text().await.map_err(OAuth2Error::Transport)?;
        if !status.is_success() {
            return Err(OAuth2Error::Idp {
                status: status.as_u16(),
                body,
            });
        }
        let parsed: TokenResponse = serde_json::from_str(&body).map_err(OAuth2Error::Decode)?;
        let deadline = deadline_from_expires_in(self.clock.now(), parsed.expires_in);
        let cached = CachedToken {
            access_token: Bytes::from(parsed.access_token.clone().into_bytes()),
            deadline,
        };
        *self.cache.lock() = Some(cached);
        Ok(parsed)
    }

    /// Refresh the cached token if it is missing or within the configured
    /// refresh leeway of expiry (see [`ClientCredentialsFlowBuilder::refresh_leeway`]).
    pub async fn ensure_fresh(&self) -> Result<(), OAuth2Error> {
        if self.needs_refresh() {
            self.fetch_token().await?;
        }
        Ok(())
    }

    /// `true` when [`Self::ensure_fresh`] would issue a network call.
    #[must_use]
    pub fn needs_refresh(&self) -> bool {
        let now = self.clock.now();
        match self.cache.lock().as_ref() {
            None => true,
            Some(cached) => cached.is_expiring(now, self.leeway),
        }
    }

    /// Snapshot of the cached access token, if any.
    #[must_use]
    pub fn cached_access_token(&self) -> Option<Bytes> {
        self.cache.lock().as_ref().map(|c| c.access_token.clone())
    }
}

/// Compute the cached-token deadline from the IDP-advertised `expires_in`
/// seconds without ever panicking on overflow.
///
/// IDPs occasionally advertise wildly large `expires_in` values — Auth0 has
/// been observed returning `u32::MAX` for "never expires" client-credentials
/// grants, and a misconfigured Keycloak realm can stamp seconds-since-epoch
/// rather than a duration. Naive `Instant::now() + Duration::from_secs(...)`
/// then panics inside `Instant::add`. Clamp via `checked_add`; on overflow,
/// fall back to a 1-hour safe default with a warning so the cache still gets
/// a non-`None` deadline and `needs_refresh` re-fetches well before any
/// plausible real expiry.
fn deadline_from_expires_in(now: Instant, expires_in: u64) -> Instant {
    now.checked_add(Duration::from_secs(expires_in))
        .unwrap_or_else(|| {
            tracing::warn!(
                target: "magnetar::auth::oauth2",
                expires_in,
                "IDP-advertised expires_in overflows Instant; falling back to 1h safe default",
            );
            now + Duration::from_secs(3600)
        })
}

impl AuthProvider for ClientCredentialsFlow {
    fn method(&self) -> &str {
        // Pulsar's broker classifies OAuth2-acquired credentials as a regular bearer token; this
        // matches `AuthenticationOAuth2.AUTH_METHOD_NAME`.
        "token"
    }

    fn initial(&self) -> Result<Bytes, AuthError> {
        match self.cache.lock().as_ref() {
            Some(cached) => Ok(cached.access_token.clone()),
            None => Err(AuthError::Invalid(
                "OAuth2 token cache is empty; call ClientCredentialsFlow::ensure_fresh first"
                    .to_owned(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use magnetar_proto::AuthProvider;
    use url::Url;

    use super::{
        ClientCredentialsFlow, Clock, Credentials, REFRESH_LEEWAY, TOKEN_ENDPOINT_SUFFIX,
        TokenResponse, default_token_endpoint,
    };

    /// Test clock that advances only via `advance(...)`.
    #[derive(Debug)]
    struct VirtualClock {
        base: Instant,
        offset_ms: AtomicU64,
    }

    impl VirtualClock {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                base: Instant::now(),
                offset_ms: AtomicU64::new(0),
            })
        }

        fn advance(&self, by: Duration) {
            self.offset_ms
                .fetch_add(by.as_millis() as u64, Ordering::SeqCst);
        }
    }

    impl Clock for VirtualClock {
        fn now(&self) -> Instant {
            self.base + Duration::from_millis(self.offset_ms.load(Ordering::SeqCst))
        }
    }

    fn sample_credentials() -> Credentials {
        Credentials::ClientSecret {
            client_id: "test-client".to_owned(),
            client_secret: "super-secret".to_owned(),
        }
    }

    fn build_flow(clock: Arc<VirtualClock>) -> ClientCredentialsFlow {
        ClientCredentialsFlow::builder()
            .issuer_url(Url::parse("https://idp.example/realms/test").expect("issuer"))
            .audience("urn:pulsar:broker")
            .credentials(sample_credentials())
            .clock(clock as Arc<dyn Clock>)
            .build()
            .expect("build flow")
    }

    /// Verify the token endpoint resolver appends the OIDC standard suffix to the issuer path
    /// without clobbering the realm segment.
    #[test]
    fn default_token_endpoint_appends_oidc_suffix() {
        let issuer = Url::parse("https://idp.example/realms/test").expect("issuer");
        let endpoint = default_token_endpoint(&issuer).expect("endpoint");
        assert_eq!(
            endpoint.as_str(),
            format!("https://idp.example/realms/test/{TOKEN_ENDPOINT_SUFFIX}")
        );
    }

    /// JSON fixture decoder — mirrors what a Keycloak-style IDP returns.
    #[test]
    fn token_response_decoder_handles_standard_fixture() {
        let fixture = r#"{
            "access_token": "eyJhbGciOiJIUzI1NiJ9.payload.signature",
            "expires_in": 3600,
            "refresh_token": "refresh-xyz",
            "token_type": "Bearer",
            "not-modeled-field": 42
        }"#;
        let parsed: TokenResponse = serde_json::from_str(fixture).expect("decode");
        assert_eq!(
            parsed.access_token,
            "eyJhbGciOiJIUzI1NiJ9.payload.signature"
        );
        assert_eq!(parsed.expires_in, 3600);
        assert_eq!(parsed.refresh_token.as_deref(), Some("refresh-xyz"));
        assert_eq!(parsed.token_type.as_deref(), Some("Bearer"));
        assert!(parsed.id_token.is_none());
    }

    /// Builder validation: empty `client_id` is rejected before any network call.
    #[test]
    fn builder_rejects_empty_client_id() {
        let result = ClientCredentialsFlow::builder()
            .issuer_url(Url::parse("https://idp.example/realms/test").expect("issuer"))
            .credentials(Credentials::ClientSecret {
                client_id: String::new(),
                client_secret: "x".to_owned(),
            })
            .build();
        let err = result.expect_err("expected config error");
        assert!(format!("{err}").contains("client_id"));
    }

    /// `needs_refresh()` is `true` on a fresh provider (empty cache) and flips after a synthetic
    /// cache prime; advancing the virtual clock past the leeway boundary flips it back to `true`.
    #[test]
    fn needs_refresh_tracks_cache_state_and_leeway() {
        let clock = VirtualClock::new();
        let flow = build_flow(clock.clone());
        assert!(flow.needs_refresh(), "fresh provider has no token");

        // Prime the cache via the private surface — we don't want a live HTTP call in unit tests.
        let bytes = bytes::Bytes::from_static(b"cached-jwt");
        *flow.cache.lock() = Some(super::CachedToken {
            access_token: bytes.clone(),
            deadline: clock.now() + Duration::from_secs(120),
        });

        assert!(
            !flow.needs_refresh(),
            "cached token well outside leeway must not trigger refresh",
        );
        assert_eq!(
            flow.cached_access_token().as_deref(),
            Some(b"cached-jwt".as_ref())
        );

        // AuthProvider::initial returns the cached bytes.
        let bytes_via_trait = flow.initial().expect("initial");
        assert_eq!(bytes_via_trait.as_ref(), b"cached-jwt");

        // Advance to inside the leeway window (deadline - REFRESH_LEEWAY + 1s) and expect a
        // refresh to be required.
        let advance = Duration::from_secs(120)
            .checked_sub(REFRESH_LEEWAY)
            .expect("test leeway < deadline")
            + Duration::from_secs(1);
        clock.advance(advance);
        assert!(
            flow.needs_refresh(),
            "token inside leeway window must trigger refresh",
        );
    }

    /// Cache miss surfaces an explicit `AuthError` so the engine can call `ensure_fresh()`.
    #[test]
    fn initial_returns_invalid_when_cache_empty() {
        let clock = VirtualClock::new();
        let flow = build_flow(clock);
        let err = flow.initial().expect_err("expected error");
        let msg = format!("{err}");
        assert!(msg.contains("OAuth2 token cache is empty"), "msg={msg}");
    }

    /// The Debug impl on the provider must not leak credentials. The crate-level invariant is that
    /// rendering a `ClientCredentialsFlow` to logs is always safe.
    #[test]
    fn debug_redacts_client_secret_and_cached_token() {
        let clock = VirtualClock::new();
        let flow = build_flow(clock.clone());
        *flow.cache.lock() = Some(super::CachedToken {
            access_token: bytes::Bytes::from_static(b"top-secret-jwt"),
            deadline: clock.now() + Duration::from_secs(60),
        });
        let rendered = format!("{flow:?}");
        assert!(!rendered.contains("super-secret"), "rendered={rendered}");
        assert!(!rendered.contains("top-secret-jwt"), "rendered={rendered}");
        assert!(rendered.contains("<redacted>"), "rendered={rendered}");
        assert!(
            rendered.contains("test-client"),
            "client_id is fine to log: {rendered}"
        );
    }

    /// `Credentials` itself must also redact. Some call sites log credentials independently.
    #[test]
    fn credentials_debug_redacts() {
        let creds = sample_credentials();
        let rendered = format!("{creds:?}");
        assert!(!rendered.contains("super-secret"));
        assert!(rendered.contains("<redacted>"));
    }

    /// `method()` is `"token"` to match Java's `AuthenticationOAuth2.AUTH_METHOD_NAME`.
    #[test]
    fn auth_method_name_is_token() {
        let clock = VirtualClock::new();
        let flow = build_flow(clock);
        assert_eq!(flow.method(), "token");
    }

    /// F7 regression (CWE-532): the IDP response body MUST NOT bleed into
    /// the `Display` output of [`OAuth2Error::Idp`]. IDP error payloads
    /// frequently echo the original POST form back — `client_secret=...`,
    /// refresh tokens, session JWTs — and operators routinely log error
    /// variants verbatim. The raw body stays available via
    /// [`OAuth2Error::body`] for opt-in inspection.
    #[test]
    fn idp_error_display_redacts_response_body() {
        let body = "{\"error\":\"invalid_grant\",\"posted_form\":\"client_secret=hunter2&refresh_token=eyJ.shh\"}";
        let err = super::OAuth2Error::Idp {
            status: 401,
            body: body.to_owned(),
        };
        let rendered = format!("{err}");
        assert!(
            !rendered.contains("hunter2"),
            "client_secret must NOT leak through Display: {rendered}"
        );
        assert!(
            !rendered.contains("refresh_token"),
            "refresh_token must NOT leak through Display: {rendered}"
        );
        assert!(
            !rendered.contains("eyJ.shh"),
            "JWT material must NOT leak through Display: {rendered}"
        );
        assert!(
            rendered.contains("redacted"),
            "redaction marker must be present: {rendered}"
        );
        assert!(
            rendered.contains("401"),
            "status code is safe to surface: {rendered}"
        );
        // The body must still be retrievable via the opt-in getter.
        assert_eq!(err.body(), Some(body));
        // Other variants do not have a body.
        let cfg = super::OAuth2Error::Config("missing field".to_owned());
        assert_eq!(cfg.body(), None);
    }

    /// F4 regression: an IDP advertising `expires_in = u64::MAX` (Auth0's
    /// observed "never expires" sentinel, or a misconfigured realm
    /// stamping seconds-since-epoch instead of a duration) must not panic
    /// inside `Instant::add`. The deadline helper falls back to a 1-hour
    /// safe default so the cache still gets a usable refresh-by deadline.
    #[test]
    fn deadline_from_expires_in_clamps_u64_max() {
        let now = Instant::now();
        // u64::MAX seconds is well past the Instant range on every
        // platform we support; before the fix this would have panicked on
        // overflow inside `Instant::add`.
        let deadline = super::deadline_from_expires_in(now, u64::MAX);
        // Fallback is `now + 3600s`, which is always representable.
        let expected = now + Duration::from_secs(3600);
        assert_eq!(deadline, expected, "u64::MAX must clamp to 1h default");

        // Sanity: a normal in-range value still produces the obvious deadline.
        let normal = super::deadline_from_expires_in(now, 60);
        assert_eq!(normal, now + Duration::from_secs(60));
    }
}
