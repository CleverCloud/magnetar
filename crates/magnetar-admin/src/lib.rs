// SPDX-License-Identifier: Apache-2.0

//! Apache Pulsar admin REST client (`/admin/v2/...`).
//!
//! Thin async wrapper around [`reqwest`] for the broker's JAX-RS admin API.
//! TLS is via `rustls-tls`. There are no channels and no background tasks: every
//! call is a one-shot `await` that resolves to a [`Result`].
//!
//! Endpoint paths mirror the broker. Each method's rustdoc cites the Java
//! endpoint class (file + relevant `@Path` annotation) in `apache/pulsar` so a
//! reader can confirm the URL and HTTP verb against upstream.
//!
//! ## Quick start
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use magnetar_admin::{AdminClient, TenantInfo};
//!
//! let admin = AdminClient::builder()
//!     .service_url("http://localhost:8080".parse()?)
//!     .build()?;
//!
//! let tenants = admin.tenants_list().await?;
//! println!("{tenants:?}");
//!
//! admin
//!     .tenant_create(
//!         "acme",
//!         TenantInfo {
//!             admin_roles: vec!["admin".into()],
//!             allowed_clusters: vec!["standalone".into()],
//!         },
//!     )
//!     .await?;
//! # Ok(()) }
//! ```

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

use std::time::Duration;

use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use reqwest::{Method, RequestBuilder, Response, StatusCode};
use serde::{Deserialize, Serialize};
use url::Url;

/// Default request timeout. Mirrors `PulsarAdminBuilder` Java default of 60s
/// (see `pulsar-client-admin/src/main/java/org/apache/pulsar/client/admin/internal/
/// PulsarAdminBuilderImpl.java`).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// Authentication strategy used by the admin client.
///
/// `Token(...)` adds `Authorization: Bearer <token>` to every request.
/// Mirrors Java's `AuthenticationToken` provider.
#[derive(Debug, Clone, Default)]
pub enum AdminAuth {
    /// No authentication.
    #[default]
    None,
    /// Bearer token. The string is the raw token; the `Bearer ` prefix is added
    /// at request time.
    Token(String),
}

/// Apache Pulsar admin REST client.
#[derive(Debug, Clone)]
pub struct AdminClient {
    base_url: Url,
    http: reqwest::Client,
    auth: AdminAuth,
}

impl AdminClient {
    /// Start building an admin client.
    #[must_use]
    pub fn builder() -> AdminClientBuilder {
        AdminClientBuilder::default()
    }

    /// Return the base URL the client targets (with the trailing `/admin/v2/`
    /// component already appended). Exposed for tests and diagnostics.
    #[must_use]
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    /// Return the configured auth strategy. Exposed for tests and diagnostics.
    #[must_use]
    pub fn auth(&self) -> &AdminAuth {
        &self.auth
    }

    // --- Cluster ---------------------------------------------------------

    /// List clusters.
    ///
    /// `GET /admin/v2/clusters`.
    /// Java: `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/Clusters.java`
    /// (`@Path("/clusters")`) + `admin/impl/ClustersBase.java#getClusters`.
    pub async fn cluster_list(&self) -> Result<Vec<String>, AdminError> {
        let url = self.url(&["clusters"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    // --- Tenants ---------------------------------------------------------

    /// List tenants.
    ///
    /// `GET /admin/v2/tenants`.
    /// Java: `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/Tenants.java`
    /// (`@Path("/tenants")`) + `admin/impl/TenantsBase.java#getTenants`.
    pub async fn tenants_list(&self) -> Result<Vec<String>, AdminError> {
        let url = self.url(&["tenants"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Create a tenant.
    ///
    /// `PUT /admin/v2/tenants/{tenant}` with a JSON [`TenantInfo`] body.
    /// Java: `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/impl/TenantsBase.java#
    /// createTenant`.
    pub async fn tenant_create(&self, name: &str, info: TenantInfo) -> Result<(), AdminError> {
        let url = self.url(&["tenants", name])?;
        let resp = self
            .send(self.http.request(Method::PUT, url).json(&info))
            .await?;
        empty_ok(resp).await
    }

    /// Delete a tenant.
    ///
    /// `DELETE /admin/v2/tenants/{tenant}`.
    /// Java: `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/impl/TenantsBase.java#
    /// deleteTenant`.
    pub async fn tenant_delete(&self, name: &str) -> Result<(), AdminError> {
        let url = self.url(&["tenants", name])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    // --- Namespaces ------------------------------------------------------

    /// List namespaces under a tenant.
    ///
    /// `GET /admin/v2/namespaces/{tenant}`.
    /// Java: `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/Namespaces.java`
    /// (`@Path("/namespaces")` + `@Path("/{tenant}")`).
    pub async fn namespaces_list(&self, tenant: &str) -> Result<Vec<String>, AdminError> {
        let url = self.url(&["namespaces", tenant])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Create a namespace.
    ///
    /// `PUT /admin/v2/namespaces/{tenant}/{namespace}`. The namespace argument
    /// is `tenant/namespace`, matching how Pulsar expresses fully qualified
    /// namespace names on the wire and CLI.
    /// Java: `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/Namespaces.java`
    /// (`@PUT @Path("/{tenant}/{namespace}")`).
    pub async fn namespace_create(&self, ns: &str) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace])?;
        let resp = self.send(self.http.request(Method::PUT, url)).await?;
        empty_ok(resp).await
    }

    /// Delete a namespace.
    ///
    /// `DELETE /admin/v2/namespaces/{tenant}/{namespace}`.
    /// Java: `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/Namespaces.java`
    /// (`@DELETE @Path("/{tenant}/{namespace}")`).
    pub async fn namespace_delete(&self, ns: &str) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    // --- Topics ----------------------------------------------------------

    /// List persistent topics in a namespace.
    ///
    /// `GET /admin/v2/persistent/{tenant}/{namespace}`.
    /// Java: `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/PersistentTopics.java`
    /// (`@Path("/persistent")` + `@GET @Path("/{tenant}/{namespace}")`).
    pub async fn topics_list(&self, namespace: &str) -> Result<Vec<String>, AdminError> {
        let (tenant, namespace) = split_namespace(namespace)?;
        let url = self.url(&["persistent", tenant, namespace])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Create a partitioned topic with `partitions` partitions.
    ///
    /// `PUT /admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions`
    /// with the partition count as a JSON integer body.
    /// Java: `PersistentTopics.java#createPartitionedTopic`
    /// (`@PUT @Path("/{tenant}/{namespace}/{topic}/partitions")`).
    pub async fn topic_create_partitioned(
        &self,
        topic: &str,
        partitions: u32,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "partitions"])?;
        let resp = self
            .send(self.http.request(Method::PUT, url).json(&partitions))
            .await?;
        empty_ok(resp).await
    }

    /// Delete a partitioned topic.
    ///
    /// `DELETE /admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions?force={force}`.
    /// Java: `PersistentTopics.java#deletePartitionedTopic`
    /// (`@DELETE @Path("/{tenant}/{namespace}/{topic}/partitions")`,
    /// `@QueryParam("force")`).
    pub async fn topic_delete(&self, topic: &str, force: bool) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let mut url = self.url(&["persistent", tenant, namespace, name, "partitions"])?;
        url.query_pairs_mut()
            .append_pair("force", if force { "true" } else { "false" });
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get topic stats.
    ///
    /// `GET /admin/v2/persistent/{tenant}/{namespace}/{topic}/stats`.
    /// Java: `PersistentTopics.java#getStats`
    /// (`@GET @Path("/{tenant}/{namespace}/{topic}/stats")`,
    /// response shape `PersistentTopicStats`).
    pub async fn topic_stats(&self, topic: &str) -> Result<TopicStats, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "stats"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    // --- Internal --------------------------------------------------------

    /// Build a request URL by joining `segments` onto `base_url`. Each segment
    /// is percent-encoded for the URL path.
    fn url(&self, segments: &[&str]) -> Result<Url, AdminError> {
        let mut url = self.base_url.clone();
        {
            // `Url::path_segments_mut` only fails for cannot-be-a-base URLs;
            // builder already rejected those.
            let mut path = url
                .path_segments_mut()
                .map_err(|()| AdminError::Builder("base url is cannot-be-a-base".into()))?;
            for segment in segments {
                path.push(segment);
            }
        }
        Ok(url)
    }

    /// Apply auth headers and dispatch.
    async fn send(&self, req: RequestBuilder) -> Result<Response, AdminError> {
        let req = match &self.auth {
            AdminAuth::None => req,
            AdminAuth::Token(tok) => {
                let value = format!("Bearer {tok}");
                let mut headers = HeaderMap::new();
                let header_value = HeaderValue::from_str(&value)
                    .map_err(|err| AdminError::Builder(format!("invalid bearer token: {err}")))?;
                headers.insert(AUTHORIZATION, header_value);
                req.headers(headers)
            }
        };
        Ok(req.send().await?)
    }
}

/// Tenant policy info — admin roles and allowed clusters.
///
/// Mirrors Java's `org.apache.pulsar.common.policies.data.TenantInfoImpl` —
/// the JSON keys (`adminRoles`, `allowedClusters`) match upstream verbatim.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TenantInfo {
    /// Roles permitted to administrate the tenant.
    #[serde(rename = "adminRoles")]
    pub admin_roles: Vec<String>,
    /// Cluster names the tenant may use.
    #[serde(rename = "allowedClusters")]
    pub allowed_clusters: Vec<String>,
}

/// Topic stats. Intentionally permissive: the Java
/// `PersistentTopicStatsImpl` shape is large and shifts between releases;
/// we extract the high-signal counters and pass the rest through as raw JSON.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TopicStats {
    /// Total messages received.
    #[serde(rename = "msgInCounter")]
    pub msg_in_counter: i64,
    /// Total bytes received.
    #[serde(rename = "bytesInCounter")]
    pub bytes_in_counter: i64,
    /// Publishers, raw JSON because the schema is large and version-dependent.
    pub publishers: Vec<serde_json::Value>,
    /// Subscriptions map (raw JSON).
    pub subscriptions: serde_json::Value,
}

/// Builder for [`AdminClient`].
#[derive(Debug, Default)]
pub struct AdminClientBuilder {
    base_url: Option<Url>,
    auth: AdminAuth,
    timeout: Option<Duration>,
}

impl AdminClientBuilder {
    /// Set the service URL — the base for `/admin/v2/...`. Required.
    #[must_use]
    pub fn service_url(mut self, url: Url) -> Self {
        self.base_url = Some(url);
        self
    }

    /// Configure bearer-token auth (`Authorization: Bearer <token>`).
    #[must_use]
    pub fn token(mut self, token: String) -> Self {
        self.auth = AdminAuth::Token(token);
        self
    }

    /// Override the request timeout. Defaults to [`DEFAULT_TIMEOUT`].
    #[must_use]
    pub fn timeout(mut self, dur: Duration) -> Self {
        self.timeout = Some(dur);
        self
    }

    /// Build the client.
    pub fn build(self) -> Result<AdminClient, AdminError> {
        let base_url = self
            .base_url
            .ok_or_else(|| AdminError::Builder("service_url is required".into()))?;
        if base_url.cannot_be_a_base() {
            return Err(AdminError::Builder(format!(
                "service_url cannot be a base url: {base_url}"
            )));
        }
        // Anchor every API call below `/admin/v2/`. We append the suffix here
        // so callers pass plain `http://broker:8080` rather than baking the
        // prefix in.
        let base_url = base_url.join("admin/v2/")?;

        let timeout = self.timeout.unwrap_or(DEFAULT_TIMEOUT);
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(AdminError::Http)?;

        Ok(AdminClient {
            base_url,
            http,
            auth: self.auth,
        })
    }
}

/// Errors returned by the admin client.
#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    /// Transport-layer error from `reqwest`.
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    /// API returned a non-success HTTP status.
    #[error("api error {code}: {body}")]
    Status {
        /// HTTP status code.
        code: u16,
        /// Response body (or a placeholder if reading the body failed).
        body: String,
    },
    /// JSON decode error.
    #[error("json decode: {0}")]
    Json(#[from] serde_json::Error),
    /// URL parse / construction error.
    #[error("invalid url: {0}")]
    Url(#[from] url::ParseError),
    /// Builder configuration error (missing service URL, invalid argument...).
    #[error("invalid builder: {0}")]
    Builder(String),
    /// Caller passed a namespace or topic name that the client could not parse.
    #[error("invalid name: {0}")]
    InvalidName(String),
}

/// Decode a non-error JSON response body.
async fn json_ok<T>(resp: Response) -> Result<T, AdminError>
where
    T: for<'de> Deserialize<'de>,
{
    let resp = ensure_status(resp).await?;
    let bytes = resp.bytes().await?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Discard a successful no-content response body.
async fn empty_ok(resp: Response) -> Result<(), AdminError> {
    let _ = ensure_status(resp).await?;
    Ok(())
}

/// Convert a non-success response into [`AdminError::Status`]. Returns the
/// original response on 2xx so the caller can decode the body.
async fn ensure_status(resp: Response) -> Result<Response, AdminError> {
    let status = resp.status();
    if status.is_success() || status == StatusCode::NO_CONTENT {
        return Ok(resp);
    }
    let code = status.as_u16();
    let body = resp
        .text()
        .await
        .unwrap_or_else(|err| format!("<failed to read body: {err}>"));
    Err(AdminError::Status { code, body })
}

/// Split a `tenant/namespace` string into its two segments.
fn split_namespace(ns: &str) -> Result<(&str, &str), AdminError> {
    let (tenant, namespace) = ns.split_once('/').ok_or_else(|| {
        AdminError::InvalidName(format!("expected tenant/namespace, got {ns:?} (no '/')"))
    })?;
    if tenant.is_empty() || namespace.is_empty() || namespace.contains('/') {
        return Err(AdminError::InvalidName(format!(
            "expected tenant/namespace, got {ns:?}"
        )));
    }
    Ok((tenant, namespace))
}

/// Split a `persistent://tenant/namespace/topic` (or `tenant/namespace/topic`)
/// into its three path segments. The scheme is optional; if present it must
/// be `persistent://`.
fn split_topic(topic: &str) -> Result<(&str, &str, &str), AdminError> {
    let rest = topic.strip_prefix("persistent://").unwrap_or(topic);
    let mut parts = rest.splitn(3, '/');
    let tenant = parts.next().unwrap_or("");
    let namespace = parts.next().unwrap_or("");
    let name = parts.next().unwrap_or("");
    if tenant.is_empty() || namespace.is_empty() || name.is_empty() || name.contains('/') {
        return Err(AdminError::InvalidName(format!(
            "expected [persistent://]tenant/namespace/topic, got {topic:?}"
        )));
    }
    Ok((tenant, namespace, name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_requires_service_url() {
        let err = AdminClient::builder().build().unwrap_err();
        assert!(matches!(err, AdminError::Builder(_)));
    }

    #[test]
    fn builder_appends_admin_v2_prefix() {
        let client = AdminClient::builder()
            .service_url("http://localhost:8080".parse().unwrap())
            .build()
            .unwrap();
        assert_eq!(
            client.base_url().as_str(),
            "http://localhost:8080/admin/v2/"
        );
    }

    #[test]
    fn builder_carries_token() {
        let client = AdminClient::builder()
            .service_url("http://localhost:8080".parse().unwrap())
            .token("abc".into())
            .build()
            .unwrap();
        assert!(matches!(client.auth(), AdminAuth::Token(t) if t == "abc"));
    }

    #[test]
    fn split_namespace_ok() {
        assert_eq!(
            split_namespace("public/default").unwrap(),
            ("public", "default")
        );
    }

    #[test]
    fn split_namespace_rejects_missing_slash() {
        assert!(matches!(
            split_namespace("public"),
            Err(AdminError::InvalidName(_))
        ));
    }

    #[test]
    fn split_namespace_rejects_extra_segment() {
        assert!(matches!(
            split_namespace("public/default/extra"),
            Err(AdminError::InvalidName(_))
        ));
    }

    #[test]
    fn split_topic_with_scheme() {
        let (t, n, name) = split_topic("persistent://acme/svc/orders").unwrap();
        assert_eq!((t, n, name), ("acme", "svc", "orders"));
    }

    #[test]
    fn split_topic_without_scheme() {
        let (t, n, name) = split_topic("acme/svc/orders").unwrap();
        assert_eq!((t, n, name), ("acme", "svc", "orders"));
    }

    #[test]
    fn split_topic_rejects_short_name() {
        assert!(matches!(
            split_topic("acme/svc"),
            Err(AdminError::InvalidName(_))
        ));
    }
}
