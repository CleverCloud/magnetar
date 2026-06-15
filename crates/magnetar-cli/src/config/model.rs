// SPDX-License-Identifier: Apache-2.0

//! Serde model for the pulsarctl `~/.config/pulsar/config` file.
//!
//! The on-disk shape is fixed by `streamnative/pulsarctl`
//! [`pkg/cmdutils/ctx_conf.go`]. The casing is intentionally mixed — kebab-case
//! at the top level, `snake_case` inside `auth-info` with a lone camelCase
//! outlier (`tokenFile`) — and must be reproduced verbatim so a file written
//! by `magnetarctl context set` stays readable by pulsarctl and vice-versa.
//!
//! Round-trip fidelity is preserved two ways:
//! - every documented field is modeled with an explicit `#[serde(rename)]`;
//! - any unknown key (incl. `locationoforigin`, modeled explicitly) is captured into a
//!   `#[serde(flatten)]` `extra` map of [`serde_norway::Value`] so it survives a load → save cycle
//!   untouched.
//!
//! [`pkg/cmdutils/ctx_conf.go`]: https://github.com/streamnative/pulsarctl/blob/master/pkg/cmdutils/ctx_conf.go
//!
//! Empty strings serialize as `""` (pulsarctl writes them for every unset
//! `auth-info` field), so `#[serde(default)]` + `String` reproduces them.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Top-level pulsarctl config document.
///
/// `BTreeMap` keeps the contexts / auth-info maps in a stable (sorted) order
/// across saves, which keeps diffs minimal — pulsarctl itself does not promise
/// key order, so sorting is a safe superset.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct PulsarConfig {
    /// Per-context credentials. The key is the context name. NB: the on-disk
    /// key is the singular `auth-info`, but the value is a map.
    #[serde(
        rename = "auth-info",
        default,
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub(crate) auth_info: BTreeMap<String, AuthInfo>,

    /// Per-context connection endpoints. The key is the context name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) contexts: BTreeMap<String, Context>,

    /// The active context name.
    #[serde(rename = "current-context", default)]
    pub(crate) current_context: String,

    /// Any unknown top-level keys, preserved verbatim across a load → save.
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, serde_norway::Value>,
}

/// A context's connection endpoints (`contexts.<name>`).
///
/// pulsarctl stores ONLY these two URLs — there is no binary-protocol
/// (`pulsar://`) URL in the format. NB: the Go field for the admin URL is
/// `BrokerServiceURL`, but the on-disk key is `admin-service-url`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Context {
    /// Admin REST URL (`http(s)://host:port`).
    #[serde(rename = "admin-service-url", default)]
    pub(crate) admin_service_url: String,

    /// `BookKeeper` HTTP URL. Carried for round-trip fidelity; magnetar does not
    /// use it.
    #[serde(rename = "bookie-service-url", default)]
    pub(crate) bookie_service_url: String,

    /// Any unknown per-context keys, preserved verbatim.
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, serde_norway::Value>,
}

/// A context's credentials (`auth-info.<name>`).
///
/// Field renames reproduce pulsarctl's exact casing. `locationoforigin` is
/// pulsarctl bookkeeping (the file a context was loaded from); it is modeled
/// explicitly so it round-trips, but magnetar never reads it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct AuthInfo {
    /// Origin-file bookkeeping (untagged in pulsarctl). Preserved, unused.
    #[serde(default)]
    pub(crate) locationoforigin: String,

    /// Path to a custom CA trust cert (`snake_case`).
    #[serde(default)]
    pub(crate) tls_trust_certs_file_path: String,

    /// Disable TLS certificate verification.
    #[serde(default)]
    pub(crate) tls_allow_insecure_connection: bool,

    /// Bearer token (lowercase).
    #[serde(default)]
    pub(crate) token: String,

    /// Path to a file containing a bearer token (camelCase outlier).
    #[serde(rename = "tokenFile", default)]
    pub(crate) token_file: String,

    /// `OAuth2` issuer endpoint (`snake_case`).
    #[serde(default)]
    pub(crate) issuer_endpoint: String,

    /// `OAuth2` client id.
    #[serde(default)]
    pub(crate) client_id: String,

    /// `OAuth2` audience.
    #[serde(default)]
    pub(crate) audience: String,

    /// `OAuth2` scope.
    #[serde(default)]
    pub(crate) scope: String,

    /// `OAuth2` key file (Pulsar-style `client_id` + `client_secret` blob).
    #[serde(default)]
    pub(crate) key_file: String,

    /// Any unknown `auth-info` keys, preserved verbatim.
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, serde_norway::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A representative pulsarctl config: two contexts, a populated
    /// `current-context`, `locationoforigin`, and an UNKNOWN key inside
    /// `auth-info` (`future_field`) plus an unknown TOP-LEVEL key
    /// (`telemetry`) that a newer pulsarctl might add.
    const FIXTURE: &str = r#"auth-info:
  c1:
    locationoforigin: /home/u/.config/pulsar/config
    tls_trust_certs_file_path: ""
    tls_allow_insecure_connection: false
    token: tok-c1
    tokenFile: ""
    issuer_endpoint: ""
    client_id: ""
    audience: ""
    scope: ""
    key_file: ""
    future_field: keep-me
  c2:
    locationoforigin: ""
    tls_trust_certs_file_path: /etc/ca.pem
    tls_allow_insecure_connection: true
    token: ""
    tokenFile: /run/secrets/token
    issuer_endpoint: ""
    client_id: ""
    audience: ""
    scope: ""
    key_file: ""
contexts:
  c1:
    admin-service-url: https://broker-1:443
    bookie-service-url: http://bookie-1:8080
  c2:
    admin-service-url: http://broker-2:8080
    bookie-service-url: http://bookie-2:8080
current-context: c1
telemetry:
  enabled: true
"#;

    /// Parse → serialize preserves every documented field, the exact key
    /// casing, AND both unknown keys (`auth-info.c1.future_field`, top-level
    /// `telemetry`). This is the pulsarctl round-trip-fidelity guarantee.
    #[test]
    fn round_trip_preserves_unknown_keys_and_casing() {
        let cfg: PulsarConfig = serde_norway::from_str(FIXTURE).expect("parse fixture");

        // Documented fields.
        assert_eq!(cfg.current_context, "c1");
        assert_eq!(cfg.auth_info["c1"].token, "tok-c1");
        assert_eq!(
            cfg.auth_info["c1"].locationoforigin,
            "/home/u/.config/pulsar/config"
        );
        assert_eq!(cfg.auth_info["c2"].token_file, "/run/secrets/token");
        assert!(cfg.auth_info["c2"].tls_allow_insecure_connection);
        assert_eq!(cfg.auth_info["c2"].tls_trust_certs_file_path, "/etc/ca.pem");
        assert_eq!(cfg.contexts["c1"].admin_service_url, "https://broker-1:443");
        assert_eq!(
            cfg.contexts["c1"].bookie_service_url,
            "http://bookie-1:8080"
        );

        // Unknown keys captured.
        assert!(cfg.auth_info["c1"].extra.contains_key("future_field"));
        assert!(cfg.extra.contains_key("telemetry"));

        // Re-serialize and re-parse: still equal (idempotent round-trip).
        let out = serde_norway::to_string(&cfg).expect("serialize");
        let reparsed: PulsarConfig = serde_norway::from_str(&out).expect("re-parse");
        assert_eq!(cfg, reparsed);

        // The camelCase outlier and kebab-case keys survive verbatim.
        assert!(out.contains("tokenFile:"));
        assert!(out.contains("admin-service-url:"));
        assert!(out.contains("bookie-service-url:"));
        assert!(out.contains("current-context:"));
        assert!(out.contains("auth-info:"));
        // Unknown keys survive verbatim.
        assert!(out.contains("future_field:"));
        assert!(out.contains("telemetry:"));
    }

    /// A `Set`-style write (default `AuthInfo` with one token) serializes the
    /// empty-string fields as `""` so pulsarctl can read them back.
    #[test]
    fn default_auth_info_serializes_empty_strings() {
        let mut cfg = PulsarConfig::default();
        cfg.contexts.insert(
            "dev".to_owned(),
            Context {
                admin_service_url: "http://localhost:8080".to_owned(),
                ..Default::default()
            },
        );
        cfg.auth_info.insert(
            "dev".to_owned(),
            AuthInfo {
                token: "abc".to_owned(),
                ..Default::default()
            },
        );
        cfg.current_context = "dev".to_owned();
        let out = serde_norway::to_string(&cfg).expect("serialize");
        let reparsed: PulsarConfig = serde_norway::from_str(&out).expect("re-parse");
        assert_eq!(cfg, reparsed);
        assert_eq!(reparsed.auth_info["dev"].token, "abc");
        assert_eq!(reparsed.auth_info["dev"].token_file, "");
    }
}
