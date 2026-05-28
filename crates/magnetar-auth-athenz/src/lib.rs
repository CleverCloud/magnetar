// SPDX-License-Identifier: Apache-2.0

//! Athenz auth provider for magnetar.
//!
//! Athenz is a Yahoo-originated AuthN/AuthZ service. The Pulsar Java client (`pulsar-broker-
//! auth-athenz` + `org.apache.pulsar.client.impl.auth.AuthenticationAthenz`) signs a JWT with
//! the tenant's private key, exchanges it for a role token at a ZTS endpoint, then advertises
//! that role token via the standard `auth_data` channel.
//!
//! # Surface
//!
//! Three construction paths, mirroring `magnetar_auth_oauth2::ClientCredentialsFlow`'s
//! cache-then-serve shape:
//!
//! - [`AthenzProvider::with_role_token`] — the caller hands a pre-fetched role token (e.g. minted
//!   by an external `zts-agent` sidecar). [`AuthProvider::initial`] returns it verbatim and no ZTS
//!   round-trip ever fires.
//! - [`AthenzProvider::with_default_signer`] (behind `feature = "zts"` + a `crypto-*` feature) —
//!   wires the cfg-active in-tree [`zts::JwtSigner`] (`AwsLcRsSigner` / `RingSigner`, ADR-0035) to
//!   a production [`zts::HttpZtsClient`].
//! - [`AthenzProvider::builder`] (behind `feature = "zts"`) — the general path: supply a custom
//!   [`zts::JwtSigner`], a custom [`zts::ZtsClient`] (the deterministic-simulation tests inject a
//!   scripted fake here), an injected `wall_clock`, and the refresh tunables.
//!
//! The async [`AthenzProvider::ensure_role_token`] takes `now: Instant` (sans-io clock injection,
//! [ADR-0011]) and refreshes the cached role token when it is missing or within `refresh_margin`
//! of expiry. The synchronous [`AuthProvider::initial`] returns whatever is cached; an empty cache
//! surfaces [`AuthError::Unsupported`] so the engine knows it must call `ensure_role_token` first.
//!
//! [ADR-0011]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0011-clock-injection-sans-io.md

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

use std::sync::Arc;

use bytes::Bytes;
use magnetar_proto::{AuthError, AuthProvider};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

// The concrete `JwtSigner` backends are only meaningful when paired
// with the `zts` feature — they exist to feed the `zts::ZtsClient`,
// and the `zts::{JwtSigner, ZtsClaims}` trait surface they implement
// itself lives behind `feature = "zts"`. Gating the module on the
// `(zts, crypto-*)` intersection keeps the standalone `crypto-*`
// cells in the `cargo xtask check-crypto-matrix` cartesian product
// compiling cleanly (no orphan-trait references).
#[cfg(all(
    feature = "zts",
    any(feature = "crypto-aws-lc-rs", feature = "crypto-ring"),
))]
pub mod jwt_signer;
#[cfg(feature = "zts")]
pub mod zts;

/// Default refresh margin: the cached role token is refreshed once `now`
/// is within this window of its expiry. Matches the Athenz Java client's
/// 5-minute default (ADR-0030).
#[cfg(feature = "zts")]
pub const DEFAULT_REFRESH_MARGIN: std::time::Duration = std::time::Duration::from_secs(300);

/// Default lifetime stamped into the signed JWT's `exp` claim (the
/// assertion the ZTS endpoint verifies, distinct from the role token's
/// own TTL which the server controls).
#[cfg(feature = "zts")]
pub const DEFAULT_JWT_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// Wall-clock provider — the sans-io `SystemTime` injection point
/// ([ADR-0011]). Production passes [`std::time::SystemTime::now`];
/// the deterministic-simulation tests pass a frozen closure so the JWT
/// `iat` / `exp` claims (and therefore the RS256 signature bytes) are
/// reproducible.
///
/// [ADR-0011]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0011-clock-injection-sans-io.md
#[cfg(feature = "zts")]
pub type WallClock = Arc<dyn Fn() -> std::time::SystemTime + Send + Sync>;

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

/// Cached role token + the monotonic deadline at which it should be
/// refreshed. `refresh_at == None` marks a pinned token (the
/// [`AthenzProvider::with_role_token`] path) that never expires.
#[derive(Debug, Clone)]
struct CachedRoleToken {
    token: Bytes,
    /// Monotonic refresh deadline. Only consulted on the ZTS path; a
    /// pinned token (`with_role_token`) carries `None`.
    #[cfg(feature = "zts")]
    refresh_at: Option<std::time::Instant>,
}

/// Athenz auth provider. See the [module docs](crate) for the three
/// construction paths.
#[derive(Clone)]
pub struct AthenzProvider {
    config: AthenzConfig,
    cache: Arc<Mutex<Option<CachedRoleToken>>>,
    #[cfg(feature = "zts")]
    wall_clock: WallClock,
    #[cfg(feature = "zts")]
    signer: Option<Arc<dyn zts::JwtSigner>>,
    #[cfg(feature = "zts")]
    zts: Option<Arc<dyn zts::ZtsClient>>,
    #[cfg(feature = "zts")]
    jwt_ttl: std::time::Duration,
    #[cfg(feature = "zts")]
    refresh_margin: std::time::Duration,
}

impl std::fmt::Debug for AthenzProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The cached role token is a live credential; surface only its
        // presence, never the bytes.
        let cached = self.cache.lock().as_ref().map(|_| "<redacted>");
        let mut dbg = f.debug_struct("AthenzProvider");
        dbg.field("config", &self.config).field("cached", &cached);
        #[cfg(feature = "zts")]
        {
            dbg.field("wall_clock", &"<wall_clock>")
                .field("signer", &self.signer.as_ref().map(|_| "<signer>"))
                .field("zts", &self.zts.as_ref().map(|_| "<zts-client>"))
                .field("jwt_ttl", &self.jwt_ttl)
                .field("refresh_margin", &self.refresh_margin);
        }
        dbg.finish()
    }
}

impl AthenzProvider {
    /// Construct an Athenz provider with no ZTS wiring. Without a
    /// pre-fetched token (see [`Self::with_role_token`]) or a ZTS client
    /// (see [`Self::builder`] / [`Self::with_default_signer`]),
    /// [`AuthProvider::initial`] surfaces [`AuthError::Unsupported`].
    #[must_use]
    pub fn new(config: AthenzConfig) -> Self {
        Self {
            config,
            cache: Arc::new(Mutex::new(None)),
            #[cfg(feature = "zts")]
            wall_clock: system_wall_clock(),
            #[cfg(feature = "zts")]
            signer: None,
            #[cfg(feature = "zts")]
            zts: None,
            #[cfg(feature = "zts")]
            jwt_ttl: DEFAULT_JWT_TTL,
            #[cfg(feature = "zts")]
            refresh_margin: DEFAULT_REFRESH_MARGIN,
        }
    }

    /// Construct an Athenz provider with a pre-fetched role token (e.g.
    /// produced by an out-of-band Athenz client). `initial()` returns
    /// the token bytes verbatim and `ensure_role_token` is a no-op.
    #[must_use]
    pub fn with_role_token(config: AthenzConfig, role_token: Bytes) -> Self {
        let provider = Self::new(config);
        *provider.cache.lock() = Some(CachedRoleToken {
            token: role_token,
            #[cfg(feature = "zts")]
            refresh_at: None,
        });
        provider
    }

    /// Borrow the Athenz configuration.
    #[must_use]
    pub fn config(&self) -> &AthenzConfig {
        &self.config
    }

    /// Snapshot of the cached role token, if any.
    #[must_use]
    pub fn cached_role_token(&self) -> Option<Bytes> {
        self.cache.lock().as_ref().map(|c| c.token.clone())
    }
}

#[cfg(feature = "zts")]
fn system_wall_clock() -> WallClock {
    Arc::new(std::time::SystemTime::now)
}

#[cfg(feature = "zts")]
impl AthenzProvider {
    /// Start a [`AthenzProviderBuilder`].
    #[must_use]
    pub fn builder() -> AthenzProviderBuilder {
        AthenzProviderBuilder::default()
    }

    /// Construct an Athenz provider wired to the cfg-active in-tree
    /// [`zts::JwtSigner`] (aws-lc-rs or ring, per the workspace
    /// crypto-provider feature matrix — ADR-0035) and a production
    /// [`zts::HttpZtsClient`]. Under `--all-features` the cfg cascade
    /// picks aws-lc-rs first.
    ///
    /// The grant follows [`zts::ZtsGrant`]'s default
    /// (`ClientCredentials`); use [`Self::builder`] for the legacy
    /// `NToken` flavour or a caller-supplied signer / client.
    ///
    /// # Errors
    /// Surfaces [`AthenzError::Config`] / [`AthenzError::SignerFailure`]
    /// from signer construction or the [`zts::HttpZtsClient::new`] build.
    #[cfg(any(feature = "crypto-aws-lc-rs", feature = "crypto-ring"))]
    pub fn with_default_signer(config: AthenzConfig) -> Result<Self, AthenzError> {
        let signer = jwt_signer::default_signer_for(&config)?;
        let client = zts::HttpZtsClient::new(&config.zts_url, zts::ZtsGrant::default())?;
        AthenzProviderBuilder::default()
            .config(config)
            .signer(signer)
            .zts_client(Arc::new(client))
            .build()
    }

    /// `true` when [`Self::ensure_role_token`] would issue a ZTS exchange
    /// at `now` — the cache is empty or within `refresh_margin` of expiry.
    /// A pinned token (from [`Self::with_role_token`]) never needs refresh.
    #[must_use]
    pub fn needs_refresh(&self, now: std::time::Instant) -> bool {
        match self.cache.lock().as_ref() {
            None => true,
            Some(entry) => match entry.refresh_at {
                None => false,
                Some(deadline) => now >= deadline,
            },
        }
    }

    /// Ensure a fresh role token is cached, performing a ZTS exchange when
    /// [`Self::needs_refresh`] is `true`. `now` is the injected monotonic
    /// instant ([ADR-0011]); the refreshed entry's deadline is
    /// `now + (server_ttl − refresh_margin)`.
    ///
    /// No-op when the cache is fresh, when the provider holds a pinned
    /// token, or when no ZTS client was wired (returns `Ok(())`).
    ///
    /// [ADR-0011]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0011-clock-injection-sans-io.md
    ///
    /// # Errors
    /// Propagates [`AthenzError`] from claim signing or the ZTS exchange.
    pub async fn ensure_role_token(&self, now: std::time::Instant) -> Result<(), AthenzError> {
        if !self.needs_refresh(now) {
            return Ok(());
        }
        let (Some(signer), Some(zts)) = (self.signer.as_ref(), self.zts.as_ref()) else {
            // Pinned-token / no-ZTS providers have nothing to refresh.
            return Ok(());
        };
        let claims = self.build_claims()?;
        let jwt = signer.sign(&claims)?;
        let response = zts.exchange(&jwt).await?;
        let ttl = std::time::Duration::from_secs(response.expires_in);
        let refresh_at = now + ttl.checked_sub(self.refresh_margin).unwrap_or(ttl);
        *self.cache.lock() = Some(CachedRoleToken {
            token: Bytes::from(response.access_token.into_bytes()),
            refresh_at: Some(refresh_at),
        });
        Ok(())
    }

    /// Build the JWT claims for the current `wall_clock` reading. The
    /// tenant principal (`domain.service`) populates `iss` / `sub`, the
    /// configured key id populates `kid`, and the ZTS URL is the `aud`.
    fn build_claims(&self) -> Result<zts::ZtsClaims, AthenzError> {
        let now = (self.wall_clock)()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| AthenzError::Config(format!("system clock pre-epoch: {e}")))?
            .as_secs();
        let principal = format!(
            "{}.{}",
            self.config.tenant_domain, self.config.tenant_service
        );
        Ok(zts::ZtsClaims {
            iss: principal.clone(),
            sub: principal,
            aud: self.config.zts_url.clone(),
            kid: self.config.key_id.clone(),
            iat: now,
            exp: now + self.jwt_ttl.as_secs(),
        })
    }
}

/// Builder for [`AthenzProvider`] (behind `feature = "zts"`).
#[cfg(feature = "zts")]
#[derive(Default)]
pub struct AthenzProviderBuilder {
    config: Option<AthenzConfig>,
    signer: Option<Arc<dyn zts::JwtSigner>>,
    zts: Option<Arc<dyn zts::ZtsClient>>,
    wall_clock: Option<WallClock>,
    jwt_ttl: Option<std::time::Duration>,
    refresh_margin: Option<std::time::Duration>,
}

#[cfg(feature = "zts")]
impl std::fmt::Debug for AthenzProviderBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AthenzProviderBuilder")
            .field("config", &self.config)
            .field("signer", &self.signer.as_ref().map(|_| "<signer>"))
            .field("zts", &self.zts.as_ref().map(|_| "<zts-client>"))
            .field(
                "wall_clock",
                &self.wall_clock.as_ref().map(|_| "<wall_clock>"),
            )
            .field("jwt_ttl", &self.jwt_ttl)
            .field("refresh_margin", &self.refresh_margin)
            .finish()
    }
}

#[cfg(feature = "zts")]
impl AthenzProviderBuilder {
    /// Tenant configuration (required).
    #[must_use]
    pub fn config(mut self, config: AthenzConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// JWT signer used to mint the ZTS bearer assertion.
    #[must_use]
    pub fn signer(mut self, signer: Arc<dyn zts::JwtSigner>) -> Self {
        self.signer = Some(signer);
        self
    }

    /// ZTS exchange client. Production wires [`zts::HttpZtsClient`]; tests
    /// inject a scripted fake.
    #[must_use]
    pub fn zts_client(mut self, zts: Arc<dyn zts::ZtsClient>) -> Self {
        self.zts = Some(zts);
        self
    }

    /// Inject a custom `wall_clock` (used by tests for deterministic
    /// `iat` / `exp`). Defaults to [`std::time::SystemTime::now`].
    #[must_use]
    pub fn wall_clock(mut self, wall_clock: WallClock) -> Self {
        self.wall_clock = Some(wall_clock);
        self
    }

    /// Override the signed-JWT lifetime (default [`DEFAULT_JWT_TTL`]).
    #[must_use]
    pub fn jwt_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.jwt_ttl = Some(ttl);
        self
    }

    /// Override the refresh margin (default [`DEFAULT_REFRESH_MARGIN`]).
    #[must_use]
    pub fn refresh_margin(mut self, margin: std::time::Duration) -> Self {
        self.refresh_margin = Some(margin);
        self
    }

    /// Finish the builder.
    ///
    /// # Errors
    /// [`AthenzError::Config`] when `config` was not supplied.
    pub fn build(self) -> Result<AthenzProvider, AthenzError> {
        let config = self
            .config
            .ok_or_else(|| AthenzError::Config("AthenzProvider requires a config".to_owned()))?;
        Ok(AthenzProvider {
            config,
            cache: Arc::new(Mutex::new(None)),
            wall_clock: self.wall_clock.unwrap_or_else(system_wall_clock),
            signer: self.signer,
            zts: self.zts,
            jwt_ttl: self.jwt_ttl.unwrap_or(DEFAULT_JWT_TTL),
            refresh_margin: self.refresh_margin.unwrap_or(DEFAULT_REFRESH_MARGIN),
        })
    }
}

impl AuthProvider for AthenzProvider {
    fn method(&self) -> &str {
        "athenz"
    }

    fn initial(&self) -> Result<Bytes, AuthError> {
        match self.cache.lock().as_ref() {
            Some(entry) => Ok(entry.token.clone()),
            None => Err(AuthError::Unsupported(
                "Athenz role token not yet fetched; provide one via AthenzProvider::with_role_token \
                 or call AthenzProvider::ensure_role_token before the connection's first use"
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
            zts_url: "https://zts.example.invalid:4443/zts/v1/".to_owned(),
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
        assert_eq!(
            p.cached_role_token().as_deref(),
            Some(b"role-token-bytes".as_ref())
        );
    }

    #[cfg(feature = "zts")]
    #[test]
    fn with_role_token_never_needs_refresh() {
        let p = AthenzProvider::with_role_token(sample_config(), Bytes::from_static(b"pinned"));
        assert!(!p.needs_refresh(std::time::Instant::now()));
    }

    #[cfg(feature = "zts")]
    #[test]
    fn empty_cache_needs_refresh() {
        let p = AthenzProvider::new(sample_config());
        assert!(p.needs_refresh(std::time::Instant::now()));
    }

    #[cfg(feature = "zts")]
    #[test]
    fn build_claims_populates_principal_and_window() {
        // The injected wall_clock makes iat/exp (and therefore the signed
        // bytes) deterministic; iss/sub carry the tenant principal and kid
        // carries the configured key id.
        let provider = AthenzProvider::builder()
            .config(sample_config())
            .wall_clock(std::sync::Arc::new(|| {
                std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000)
            }))
            .build()
            .expect("build provider");
        let claims = provider.build_claims().expect("build claims");
        assert_eq!(claims.iss, "mydomain.myservice");
        assert_eq!(claims.sub, "mydomain.myservice");
        assert_eq!(claims.kid, "key0");
        assert_eq!(claims.aud, "https://zts.example.invalid:4443/zts/v1/");
        assert_eq!(claims.iat, 1_700_000_000);
        assert_eq!(claims.exp, 1_700_000_000 + super::DEFAULT_JWT_TTL.as_secs());
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
