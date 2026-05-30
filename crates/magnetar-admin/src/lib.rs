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

use magnetar_proto::MessageId;
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
#[derive(Clone, Default)]
pub enum AdminAuth {
    /// No authentication.
    #[default]
    None,
    /// Bearer token. The string is the raw token; the `Bearer ` prefix is added
    /// at request time.
    Token(String),
}

impl std::fmt::Debug for AdminAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the token body so calling `Debug` on the admin client never
        // spills the bearer credential to tracing or stdout. Mirrors the
        // `Credentials`/`ClientCredentialsFlow` Debug redaction in
        // `magnetar-auth-oauth2`.
        match self {
            Self::None => f.write_str("None"),
            Self::Token(_) => f.debug_tuple("Token").field(&"<redacted>").finish(),
        }
    }
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

    /// Resolve a broker-entry-metadata `index` to a [`MessageId`] (PIP-415).
    ///
    /// `GET /admin/v2/persistent/{tenant}/{namespace}/{topic}/getMessageIdByIndex?index={index}`.
    /// Per [PIP-415](https://github.com/apache/pulsar/blob/master/pip/pip-415.md)
    /// this is **REST-only** — the spec's "Binary protocol" section is
    /// intentionally empty and the canonical implementation PR
    /// [`apache/pulsar#24222`](https://github.com/apache/pulsar/pull/24222)
    /// (merged 2025-06-23) touches only admin / broker / CLI Java code.
    ///
    /// Java:
    /// `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/PersistentTopics.java`
    /// (`@GET @Path("/{tenant}/{namespace}/{topic}/getMessageIdByIndex")`,
    /// `@QueryParam("index") long`); admin-client side is
    /// `pulsar-client-admin/src/main/java/org/apache/pulsar/client/admin/internal/
    /// TopicsImpl.java#getMessageIdByIndexAsync` which deserialises the
    /// response into `MessageIdImpl` (i.e. `{ledgerId, entryId, partitionIndex}`).
    ///
    /// `topic` follows the same rule as every other topic-scoped method:
    /// either `persistent://tenant/ns/topic` or `tenant/ns/topic`. For a
    /// partitioned topic, pass the specific partition (`my-topic-partition-0`).
    ///
    /// The response carries only `(ledgerId, entryId, partitionIndex)`. The
    /// returned [`MessageId`] sets `batch_index = -1` and `batch_size = -1`
    /// because the broker resolves at entry granularity — see PIP-415 §"Why
    /// Precise Index Matching Isn't Implemented on the Broker Side".
    pub async fn topic_get_message_id_by_index(
        &self,
        topic: &str,
        index: i64,
    ) -> Result<MessageId, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let mut url = self.url(&["persistent", tenant, namespace, name, "getMessageIdByIndex"])?;
        url.query_pairs_mut()
            .append_pair("index", &index.to_string());
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        let dto: MessageIdResponse = json_ok(resp).await?;
        dto.try_into_message_id()
    }

    // --- Shadow topics (PIP-180 / ADR-0033) ------------------------------

    /// Create a shadow topic ([PIP-180](https://github.com/apache/pulsar/blob/master/pip/pip-180.md)).
    ///
    /// `PUT /admin/v2/persistent/{tenant}/{namespace}/{topic}/shadowTopics`
    /// where `{topic}` is the **source** topic name. The request body is a
    /// **bare JSON array** `["persistent://tenant/ns/shadow"]` listing the
    /// shadow topics to set on the source — the broker's
    /// `@PUT setShadowTopics(List<String> shadowTopics)` handler
    /// deserialises the body directly into a `List<String>`, NOT an
    /// envelope object. magnetar takes one shadow at a time for an
    /// explicit single-call surface; call multiple times for a fan-out.
    ///
    /// Java:
    /// `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/PersistentTopics.java`
    /// (`@PUT @Path("/{tenant}/{namespace}/{topic}/shadowTopics")` →
    /// `setShadowTopics(List<String>)`).
    ///
    /// **No per-shadow properties on this endpoint.** The Pulsar
    /// `setShadowTopics` REST handler carries only the topic-name list.
    /// To attach metadata to the shadow topic, pre-create it as a normal
    /// topic with properties (a separate topic-create call) *before*
    /// linking it to the source here — that's what the Java
    /// `Topics#createShadowTopic(shadow, source, props)` convenience does
    /// under the hood (create-with-props, then set-shadow). magnetar keeps
    /// the two steps explicit. A previous version of this method sent a
    /// `{ "shadowTopics": [...], "properties": {...} }` envelope that
    /// Pulsar 4.0.4 rejects with HTTP 400 (caught by the PIP-180
    /// replicator e2e fixture in
    /// `crates/magnetar/tests/e2e_shadow_topic_replicator.rs`).
    ///
    /// Errors mirror the existing `AdminError` taxonomy: 404 → `Status { code:
    /// 404, .. }` (the source topic does not exist), 409 → `Status { code:
    /// 409, .. }` (the shadow topic already exists on this source),
    /// 401/403 → `Status { code: 401|403, .. }` (auth).
    pub async fn create_shadow_topic(&self, source: &str, shadow: &str) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(source)?;
        // Validate the shadow name eagerly so a misformatted argument errors
        // out with `InvalidName` rather than as a broker 4xx after we've
        // already crossed the wire.
        let _ = split_topic(shadow)?;
        let url = self.url(&["persistent", tenant, namespace, name, "shadowTopics"])?;
        // Bare `List<String>` — the broker's `setShadowTopics` handler
        // deserialises the body directly into a `List<String>`. Any
        // wrapping object yields HTTP 400.
        let body = vec![shadow.to_owned()];
        let resp = self
            .send(self.http.request(Method::PUT, url).json(&body))
            .await?;
        empty_ok(resp).await
    }

    /// Delete a shadow topic (PIP-180).
    ///
    /// `DELETE /admin/v2/persistent/{tenant}/{namespace}/{topic}` where
    /// `{topic}` is the **shadow** topic name. PIP-180's deletion contract
    /// goes through the regular topic-delete path on the shadow itself —
    /// the broker recognises the topic as a shadow and detaches it from
    /// the source ledger atomically with the metadata delete.
    ///
    /// `force` controls whether active subscribers are kicked off before
    /// the delete (`?force=true`) or whether the broker rejects the
    /// request when subscribers exist (`?force=false`, the default).
    ///
    /// Java: `org.apache.pulsar.client.admin.Topics#deleteShadowTopic` calls
    /// the same `@DELETE @Path("/{tenant}/{namespace}/{topic}")` endpoint.
    pub async fn delete_shadow_topic(&self, shadow: &str, force: bool) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(shadow)?;
        let mut url = self.url(&["persistent", tenant, namespace, name])?;
        url.query_pairs_mut()
            .append_pair("force", if force { "true" } else { "false" });
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// List the shadow topics created on a source topic (PIP-180).
    ///
    /// `GET /admin/v2/persistent/{tenant}/{namespace}/{topic}/shadowTopics`
    /// where `{topic}` is the **source** topic name. The broker returns a
    /// JSON array of fully-qualified shadow topic names.
    ///
    /// Java:
    /// `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/PersistentTopics.java`
    /// (`@GET @Path("/{tenant}/{namespace}/{topic}/shadowTopics")`).
    ///
    /// Used by the runtime engine at consumer subscribe time: when the user
    /// subscribes to a topic the runtime cannot yet classify, a single
    /// `get_shadow_topics` lookup on every other topic in the namespace is
    /// expensive; instead the runtime calls `get_shadow_topics(subscribed)`
    /// on the topic itself — a non-shadow topic returns an empty array, a
    /// shadow topic surfaces nothing but the broker has already populated
    /// the consumer's `shadow_metadata` via the topic's policy.
    /// (See `crates/magnetar-runtime-tokio/src/client.rs::subscribe`.)
    pub async fn get_shadow_topics(&self, source: &str) -> Result<Vec<String>, AdminError> {
        let (tenant, namespace, name) = split_topic(source)?;
        let url = self.url(&["persistent", tenant, namespace, name, "shadowTopics"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Resolve the **source** topic of a shadow topic (PIP-180).
    ///
    /// `GET /admin/v2/persistent/{tenant}/{namespace}/{topic}/shadowSource`.
    /// Returns the source-topic name when the queried topic is a shadow;
    /// returns `None` when it is a regular topic. Used by the runtime at
    /// subscribe time to populate
    /// [`magnetar_proto::ShadowTopicMetadata::source_topic`] on the new
    /// consumer (so the receive path can emit
    /// [`magnetar_proto::ConnectionEvent::MessageReceivedFromShadow`]
    /// without an out-of-band lookup per message).
    ///
    /// Java: `org.apache.pulsar.client.admin.Topics#getShadowSource` —
    /// `@GET @Path("/{tenant}/{namespace}/{topic}/shadowSource")`.
    pub async fn get_shadow_source(&self, shadow: &str) -> Result<Option<String>, AdminError> {
        let (tenant, namespace, name) = split_topic(shadow)?;
        let url = self.url(&["persistent", tenant, namespace, name, "shadowSource"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        // Some broker builds return 204 No Content for non-shadow topics; treat
        // that as `None`. Otherwise the body is a JSON string (Jackson default
        // for a `String` response).
        let resp = ensure_status(resp).await?;
        if resp.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        let bytes = resp.bytes().await?;
        if bytes.is_empty() {
            return Ok(None);
        }
        let s: Option<String> = serde_json::from_slice(&bytes)?;
        Ok(s.filter(|t| !t.is_empty()))
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
            // `base_url` is anchored at `/admin/v2/` (trailing slash), so the
            // segments iterator carries a sentinel empty trailing segment.
            // Drop it before appending API segments — otherwise pushes land
            // after the empty, producing `/admin/v2//persistent/...`. Real
            // brokers tolerate the double slash; strict mocks (wiremock) do
            // not, and Java's `PulsarAdmin` emits the single-slash form.
            path.pop_if_empty();
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

/// Wire shape of the PIP-415 `getMessageIdByIndex` response.
///
/// Mirrors Java's `MessageIdImpl` JSON shape (Jackson default property-name
/// serialisation): `{ledgerId, entryId, partitionIndex}`. See
/// `pulsar-client/src/main/java/org/apache/pulsar/client/impl/MessageIdImpl.java`.
///
/// Kept as a deserialise-only DTO and converted into
/// [`magnetar_proto::MessageId`] at the boundary so callers do not see this
/// wire detail. Not exposed publicly.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MessageIdResponse {
    ledger_id: i64,
    entry_id: i64,
    #[serde(default = "default_partition_index")]
    partition_index: i32,
}

fn default_partition_index() -> i32 {
    -1
}

impl MessageIdResponse {
    /// Convert the REST response into the canonical [`MessageId`]. The broker
    /// resolves at entry granularity, so `batch_index` / `batch_size` are not
    /// part of the JSON — they default to `-1` (the same sentinel
    /// `MessageId::from_pb` uses for `MessageIdData` without batch fields).
    ///
    /// Returns `AdminError::Protocol` if the broker emits a negative
    /// `ledgerId` or `entryId` — both fields are `u64` in the canonical type
    /// (matching the proto wire format) and Java's `MessageIdImpl` cannot
    /// represent negative values either, so a negative wire value is a
    /// broker bug we must surface rather than silently wrap.
    fn try_into_message_id(self) -> Result<MessageId, AdminError> {
        let ledger_id = u64::try_from(self.ledger_id).map_err(|_| {
            AdminError::Protocol(format!("negative ledgerId from broker: {}", self.ledger_id))
        })?;
        let entry_id = u64::try_from(self.entry_id).map_err(|_| {
            AdminError::Protocol(format!("negative entryId from broker: {}", self.entry_id))
        })?;
        Ok(MessageId {
            ledger_id,
            entry_id,
            partition: self.partition_index,
            batch_index: -1,
            batch_size: -1,
            // PIP-460 (ADR-0031): admin REST never resolves a scalable
            // segment id; the field only exists under `scalable-topics`.
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
        })
    }
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
    /// Broker returned a response that violates the documented wire contract
    /// (e.g. negative `ledgerId` from `getMessageIdByIndex`, which Java
    /// `MessageIdImpl` cannot represent either).
    #[error("broker protocol violation: {0}")]
    Protocol(String),
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
/// Reject path segments the `url` crate would silently rewrite. `.` and `..`
/// disappear under RFC 3986 dot-segment normalisation; percent-encoded slash
/// (`%2F` / `%2f`) lets a hostile name escape its segment; NUL / ASCII
/// control bytes have no place in an admin path. Refusing all of these at
/// the input boundary keeps the URL the client builds in lock-step with the
/// path the broker eventually parses.
fn validate_segment(segment: &str) -> Result<(), AdminError> {
    if segment.is_empty() {
        return Err(AdminError::InvalidName("empty path segment".into()));
    }
    if segment == "." || segment == ".." {
        return Err(AdminError::InvalidName(format!(
            "dot segment is not a valid name: {segment:?}",
        )));
    }
    if segment.contains("%2F") || segment.contains("%2f") {
        return Err(AdminError::InvalidName(format!(
            "percent-encoded slash in segment: {segment:?}",
        )));
    }
    if segment.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Err(AdminError::InvalidName(format!(
            "control byte in segment: {segment:?}",
        )));
    }
    Ok(())
}

fn split_namespace(ns: &str) -> Result<(&str, &str), AdminError> {
    let (tenant, namespace) = ns.split_once('/').ok_or_else(|| {
        AdminError::InvalidName(format!("expected tenant/namespace, got {ns:?} (no '/')"))
    })?;
    if tenant.is_empty() || namespace.is_empty() || namespace.contains('/') {
        return Err(AdminError::InvalidName(format!(
            "expected tenant/namespace, got {ns:?}"
        )));
    }
    validate_segment(tenant)?;
    validate_segment(namespace)?;
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
    validate_segment(tenant)?;
    validate_segment(namespace)?;
    validate_segment(name)?;
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
    fn admin_auth_token_debug_redacts_secret() {
        let auth = AdminAuth::Token("super-secret-jwt".to_owned());
        let rendered = format!("{auth:?}");
        assert!(
            !rendered.contains("super-secret-jwt"),
            "raw token leaked through Debug: {rendered}",
        );
        assert!(
            rendered.contains("<redacted>"),
            "expected redaction sentinel in {rendered}"
        );
        assert!(
            rendered.contains("Token"),
            "expected variant name in {rendered}"
        );

        let none_rendered = format!("{:?}", AdminAuth::None);
        assert_eq!(none_rendered, "None");
    }

    #[test]
    fn admin_client_debug_does_not_leak_token() {
        let client = AdminClient::builder()
            .service_url("http://localhost:8080".parse().unwrap())
            .token("leaky-token".into())
            .build()
            .unwrap();
        let rendered = format!("{client:?}");
        assert!(
            !rendered.contains("leaky-token"),
            "raw token leaked through AdminClient Debug: {rendered}",
        );
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

    #[test]
    fn message_id_response_deserialises_java_camelcase() {
        // The exact body shape upstream PIP-415 §"Success Response" advertises.
        let json = r#"{"ledgerId":12345,"entryId":67890,"partitionIndex":0}"#;
        let dto: MessageIdResponse = serde_json::from_str(json).unwrap();
        let msg = dto.try_into_message_id().unwrap();
        assert_eq!(msg.ledger_id, 12345);
        assert_eq!(msg.entry_id, 67890);
        assert_eq!(msg.partition, 0);
        // The broker resolves at entry granularity — batch fields are absent
        // from the JSON and must default to -1 to match the canonical sentinel.
        assert_eq!(msg.batch_index, -1);
        assert_eq!(msg.batch_size, -1);
    }

    #[test]
    fn message_id_response_defaults_partition_for_non_partitioned_topic() {
        // PIP-415 §"Success Response": `partitionIndex: -1` for non-partitioned
        // topics. Some broker versions omit the field entirely on
        // non-partitioned topics; serde default keeps us correct in either case.
        let json = r#"{"ledgerId":1,"entryId":2}"#;
        let dto: MessageIdResponse = serde_json::from_str(json).unwrap();
        assert_eq!(dto.try_into_message_id().unwrap().partition, -1);
    }

    #[test]
    fn url_helper_emits_single_slash_after_admin_v2() {
        // Regression guard: the previous url() helper appended segments after
        // the trailing-slash sentinel of /admin/v2/, producing
        // /admin/v2//persistent/... — real brokers tolerated it but strict
        // mocks (and Java's PulsarAdmin) emit the single-slash form. Pin the
        // current behaviour so we notice any future regression.
        let client = AdminClient::builder()
            .service_url("http://broker.example:8080".parse().unwrap())
            .build()
            .unwrap();
        let url = client.url(&["clusters"]).unwrap();
        assert_eq!(url.as_str(), "http://broker.example:8080/admin/v2/clusters");
        let url2 = client
            .url(&["persistent", "public", "default", "topic", "stats"])
            .unwrap();
        assert_eq!(
            url2.as_str(),
            "http://broker.example:8080/admin/v2/persistent/public/default/topic/stats"
        );
    }

    #[test]
    fn split_topic_rejects_dot_segments() {
        // LISA-001: `..` / `.` in any segment would silently normalise out via
        // url::Url::path_segments_mut, producing a client/server URL parser
        // differential. Refuse them at the input boundary.
        assert!(matches!(
            split_topic("persistent://../foo/bar"),
            Err(AdminError::InvalidName(_))
        ));
        assert!(matches!(
            split_topic("./foo/bar"),
            Err(AdminError::InvalidName(_))
        ));
        assert!(matches!(
            split_topic("tenant/./topic"),
            Err(AdminError::InvalidName(_))
        ));
    }

    #[test]
    fn split_topic_rejects_control_bytes_and_percent_encoded_slash() {
        assert!(matches!(
            split_topic("tenant/ns/topic%2Fevil"),
            Err(AdminError::InvalidName(_))
        ));
        assert!(matches!(
            split_topic("tenant/ns/top\0ic"),
            Err(AdminError::InvalidName(_))
        ));
    }
}
