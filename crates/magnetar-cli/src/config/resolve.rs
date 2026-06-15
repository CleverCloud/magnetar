// SPDX-License-Identifier: Apache-2.0

//! Resolve an active context into the connection settings the CLI applies to
//! the admin + data-plane clients.
//!
//! Context selection: `--context <name>` › the file's `current-context`.
//! From the selected `contexts.<name>` + `auth-info.<name>` this produces a
//! [`ResolvedContext`]: the admin URL, the derived data-plane URL, and the
//! auth method (token / token-file / TLS / `OAuth2`).

#[cfg(test)]
use super::model::Context;
use super::model::{AuthInfo, PulsarConfig};

/// The auth method a context resolves to. Mirrors the `auth-info` field set:
/// a context carries at most one of these (token › token-file › oauth2),
/// always overlaid with the TLS trust/insecure settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResolvedAuth {
    /// No credentials.
    None,
    /// Inline bearer token.
    Token(String),
    /// Path to a file holding a bearer token (read at apply time).
    TokenFile(String),
    /// `OAuth2` `client_credentials` parameters.
    OAuth2(OAuth2Params),
}

/// `OAuth2` `client_credentials` parameters lifted from an `auth-info` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OAuth2Params {
    /// Issuer endpoint (`issuer_endpoint`).
    pub(crate) issuer_endpoint: String,
    /// Client id (`client_id`).
    pub(crate) client_id: String,
    /// Audience (`audience`), empty when unset.
    pub(crate) audience: String,
    /// Scope (`scope`), empty when unset.
    pub(crate) scope: String,
    /// Key file (`key_file`), empty when unset.
    pub(crate) key_file: String,
}

/// TLS trust settings lifted from an `auth-info` entry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ResolvedTls {
    /// Custom CA trust cert path (`tls_trust_certs_file_path`), empty if unset.
    pub(crate) trust_cert_path: String,
    /// Allow-insecure (`tls_allow_insecure_connection`).
    pub(crate) allow_insecure: bool,
}

/// A fully resolved context — what the CLI applies to its clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedContext {
    /// The context name that was selected.
    pub(crate) name: String,
    /// Admin REST URL (`admin-service-url`).
    pub(crate) admin_url: String,
    /// Data-plane URL derived from `admin-service-url` (see [`derive_data_plane_url`]).
    pub(crate) data_plane_url: Option<String>,
    /// Resolved auth method.
    pub(crate) auth: ResolvedAuth,
    /// Resolved TLS settings.
    pub(crate) tls: ResolvedTls,
}

/// Errors from resolving a context.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ResolveError {
    /// The selected context name is not present in the config.
    #[error("context not found: {0}")]
    NotFound(String),
}

/// Select and resolve the active context.
///
/// `explicit` is `--context <name>` (highest precedence); otherwise the file's
/// `current-context` is used. Returns `Ok(None)` when there is no context to
/// select AND none was explicitly requested — the caller then falls back to
/// the built-in localhost defaults.
pub(crate) fn resolve(
    cfg: &PulsarConfig,
    explicit: Option<&str>,
) -> Result<Option<ResolvedContext>, ResolveError> {
    let name = if let Some(n) = explicit {
        n.to_owned()
    } else if cfg.current_context.is_empty() {
        // No context anywhere — caller uses built-in defaults.
        return Ok(None);
    } else {
        cfg.current_context.clone()
    };

    let ctx = cfg
        .contexts
        .get(&name)
        .ok_or_else(|| ResolveError::NotFound(name.clone()))?;
    let auth_info = cfg.auth_info.get(&name);

    let auth = auth_info.map_or(ResolvedAuth::None, resolve_auth);
    let tls = auth_info.map_or_else(ResolvedTls::default, resolve_tls);
    let data_plane_url = derive_data_plane_url(&ctx.admin_service_url);

    Ok(Some(ResolvedContext {
        name,
        admin_url: ctx.admin_service_url.clone(),
        data_plane_url,
        auth,
        tls,
    }))
}

/// Lift the single active auth method from an `auth-info` entry. Precedence:
/// inline token › token file › `OAuth2` (issuer present). `None` when none set.
fn resolve_auth(info: &AuthInfo) -> ResolvedAuth {
    if !info.token.is_empty() {
        return ResolvedAuth::Token(info.token.clone());
    }
    if !info.token_file.is_empty() {
        return ResolvedAuth::TokenFile(info.token_file.clone());
    }
    if !info.issuer_endpoint.is_empty() {
        return ResolvedAuth::OAuth2(OAuth2Params {
            issuer_endpoint: info.issuer_endpoint.clone(),
            client_id: info.client_id.clone(),
            audience: info.audience.clone(),
            scope: info.scope.clone(),
            key_file: info.key_file.clone(),
        });
    }
    ResolvedAuth::None
}

/// Lift the TLS settings from an `auth-info` entry.
fn resolve_tls(info: &AuthInfo) -> ResolvedTls {
    ResolvedTls {
        trust_cert_path: info.tls_trust_certs_file_path.clone(),
        allow_insecure: info.tls_allow_insecure_connection,
    }
}

/// Derive the binary-protocol data-plane URL from an `admin-service-url`.
///
/// pulsarctl stores no `pulsar://` URL, so magnetar derives one heuristically:
/// keep the host, ALWAYS substitute the default binary port (the admin port is
/// never the binary port), and map the scheme:
///
/// | `admin-service-url`        | derived                       |
/// | -------------------------- | ----------------------------- |
/// | `http://host[:port]`       | `pulsar://host:6650`          |
/// | `https://host[:port]`      | `pulsar+ssl://host:6651`      |
///
/// Returns `None` when the admin URL is empty or has no host / an unrecognized
/// scheme — the caller then keeps the built-in `--service-url` default. The
/// heuristic is best-effort (proxied deployments expose the binary protocol
/// elsewhere), so an explicit `--service-url` always wins upstream and the
/// derived value is logged at startup.
pub(crate) fn derive_data_plane_url(admin_url: &str) -> Option<String> {
    if admin_url.is_empty() {
        return None;
    }
    let parsed = url::Url::parse(admin_url).ok()?;
    let host = parsed.host_str()?;
    match parsed.scheme() {
        "http" => Some(format!("pulsar://{host}:6650")),
        // Canonical Pulsar TLS scheme magnetar parses is `pulsar+ssl://`
        // (NOT `pulsar+tls://`).
        "https" => Some(format!("pulsar+ssl://{host}:6651")),
        _ => None,
    }
}

/// Lift a `Context` + optional `AuthInfo` into a `ResolvedContext` for a known
/// name. Exposed for tests.
#[cfg(test)]
pub(crate) fn resolve_named(
    name: &str,
    ctx: &Context,
    auth_info: Option<&AuthInfo>,
) -> ResolvedContext {
    ResolvedContext {
        name: name.to_owned(),
        admin_url: ctx.admin_service_url.clone(),
        data_plane_url: derive_data_plane_url(&ctx.admin_service_url),
        auth: auth_info.map_or(ResolvedAuth::None, resolve_auth),
        tls: auth_info.map_or_else(ResolvedTls::default, resolve_tls),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The data-plane derivation table from the issue / ADR-0068. Host is
    /// kept, the binary port is ALWAYS substituted, the scheme is mapped.
    #[test]
    fn data_plane_derivation_table() {
        // https → pulsar+ssl on 6651 (admin port 443 dropped).
        assert_eq!(
            derive_data_plane_url("https://host:443").as_deref(),
            Some("pulsar+ssl://host:6651")
        );
        // http → pulsar on 6650 (admin port 8080 dropped).
        assert_eq!(
            derive_data_plane_url("http://host:8080").as_deref(),
            Some("pulsar://host:6650")
        );
        // No explicit port: still substitute the binary port.
        assert_eq!(
            derive_data_plane_url("https://broker.example").as_deref(),
            Some("pulsar+ssl://broker.example:6651")
        );
        assert_eq!(
            derive_data_plane_url("http://broker.example").as_deref(),
            Some("pulsar://broker.example:6650")
        );
        // Unrecognized scheme / empty → None (caller keeps its default).
        assert_eq!(derive_data_plane_url(""), None);
        assert_eq!(derive_data_plane_url("pulsar://host:6650"), None);
        assert_eq!(derive_data_plane_url("ftp://host"), None);
    }

    fn cfg_with_two_contexts() -> PulsarConfig {
        let mut cfg = PulsarConfig::default();
        cfg.contexts.insert(
            "c1".to_owned(),
            Context {
                admin_service_url: "https://broker-1:443".to_owned(),
                bookie_service_url: "http://bookie-1:8080".to_owned(),
                ..Default::default()
            },
        );
        cfg.contexts.insert(
            "c2".to_owned(),
            Context {
                admin_service_url: "http://broker-2:8080".to_owned(),
                ..Default::default()
            },
        );
        cfg.auth_info.insert(
            "c1".to_owned(),
            AuthInfo {
                token: "tok-c1".to_owned(),
                ..Default::default()
            },
        );
        cfg.auth_info.insert(
            "c2".to_owned(),
            AuthInfo {
                issuer_endpoint: "https://idp.example/realms/p".to_owned(),
                client_id: "cid".to_owned(),
                audience: "aud".to_owned(),
                key_file: "/run/kf.json".to_owned(),
                tls_allow_insecure_connection: true,
                ..Default::default()
            },
        );
        cfg.current_context = "c1".to_owned();
        cfg
    }

    /// `current-context` selects c1: token auth + https-derived data-plane URL.
    #[test]
    fn resolve_uses_current_context() {
        let cfg = cfg_with_two_contexts();
        let r = resolve(&cfg, None).expect("resolve").expect("some");
        assert_eq!(r.name, "c1");
        assert_eq!(r.admin_url, "https://broker-1:443");
        assert_eq!(
            r.data_plane_url.as_deref(),
            Some("pulsar+ssl://broker-1:6651")
        );
        assert_eq!(r.auth, ResolvedAuth::Token("tok-c1".to_owned()));
        assert!(!r.tls.allow_insecure);
    }

    /// `--context c2` overrides current-context: `OAuth2` auth + insecure TLS +
    /// http-derived data-plane URL.
    #[test]
    fn resolve_explicit_context_overrides_current() {
        let cfg = cfg_with_two_contexts();
        let r = resolve(&cfg, Some("c2")).expect("resolve").expect("some");
        assert_eq!(r.name, "c2");
        assert_eq!(r.data_plane_url.as_deref(), Some("pulsar://broker-2:6650"));
        assert!(r.tls.allow_insecure);
        match r.auth {
            ResolvedAuth::OAuth2(p) => {
                assert_eq!(p.issuer_endpoint, "https://idp.example/realms/p");
                assert_eq!(p.client_id, "cid");
                assert_eq!(p.key_file, "/run/kf.json");
            }
            other => panic!("expected OAuth2, got {other:?}"),
        }
    }

    /// An empty `current-context` and no explicit name → no context (defaults).
    #[test]
    fn resolve_no_context_returns_none() {
        let mut cfg = cfg_with_two_contexts();
        cfg.current_context = String::new();
        assert!(resolve(&cfg, None).expect("resolve").is_none());
    }

    /// A non-existent name errors.
    #[test]
    fn resolve_unknown_context_errors() {
        let cfg = cfg_with_two_contexts();
        let err = resolve(&cfg, Some("nope")).expect_err("should error");
        assert!(matches!(err, ResolveError::NotFound(name) if name == "nope"));
    }

    /// Token-file auth is lifted when no inline token is set.
    #[test]
    fn resolve_named_token_file_auth() {
        let ctx = Context {
            admin_service_url: "http://h:8080".to_owned(),
            ..Default::default()
        };
        let info = AuthInfo {
            token_file: "/run/t".to_owned(),
            ..Default::default()
        };
        let r = resolve_named("x", &ctx, Some(&info));
        assert_eq!(r.auth, ResolvedAuth::TokenFile("/run/t".to_owned()));
    }
}
