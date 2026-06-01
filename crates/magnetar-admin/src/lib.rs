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

mod tls_crypto;

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

    /// List failure-domains configured on a cluster.
    ///
    /// `GET /admin/v2/clusters/{cluster}/failureDomains`. The broker returns
    /// a `Map<String, FailureDomain>` keyed by domain name; each value
    /// carries a `brokers: Set<String>` member. The map is exposed as a
    /// raw `serde_json::Value` for forward-compat — broker minor versions
    /// add fields.
    /// Java: `ClustersBase#getFailureDomains`.
    pub async fn cluster_failure_domains_list(
        &self,
        cluster: &str,
    ) -> Result<serde_json::Value, AdminError> {
        let url = self.url(&["clusters", cluster, "failureDomains"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Get one failure-domain by name.
    ///
    /// `GET /admin/v2/clusters/{cluster}/failureDomains/{domain}`.
    /// Java: `ClustersBase#getDomain`.
    pub async fn cluster_failure_domain_get(
        &self,
        cluster: &str,
        domain: &str,
    ) -> Result<serde_json::Value, AdminError> {
        let url = self.url(&["clusters", cluster, "failureDomains", domain])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// List namespace-isolation policies configured on a cluster.
    ///
    /// `GET /admin/v2/clusters/{cluster}/namespaceIsolationPolicies`. The
    /// broker returns a `Map<String, NamespaceIsolationData>` carrying
    /// the namespace regex, primary/secondary broker lists, and the
    /// auto-failover policy. Exposed as raw JSON for forward-compat.
    /// Java: `ClustersBase#getNamespaceIsolationPolicies`.
    pub async fn namespace_isolation_policies_list(
        &self,
        cluster: &str,
    ) -> Result<serde_json::Value, AdminError> {
        let url = self.url(&["clusters", cluster, "namespaceIsolationPolicies"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    // --- Brokers ---------------------------------------------------------

    /// List active brokers in a cluster.
    ///
    /// `GET /admin/v2/brokers/{cluster}`. Returns a list of `host:port`
    /// strings — one entry per broker that's currently registered with
    /// the cluster's metadata store. Java: `BrokersBase#getActiveBrokers`.
    pub async fn brokers_list(&self, cluster: &str) -> Result<Vec<String>, AdminError> {
        let url = self.url(&["brokers", cluster])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Get the current leader broker for the cluster.
    ///
    /// `GET /admin/v2/brokers/leaderBroker`. Returns `{ serviceUrl,
    /// brokerId }`. Exposed as raw JSON for forward-compat — newer
    /// brokers add `clusterName` and similar fields.
    /// Java: `BrokersBase#getLeaderBroker`.
    pub async fn brokers_leader(&self) -> Result<serde_json::Value, AdminError> {
        let url = self.url(&["brokers", "leaderBroker"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// List the names of all dynamic-config keys the broker exposes.
    ///
    /// `GET /admin/v2/brokers/configuration`. Returns the bare list of
    /// `ServiceConfiguration` fields tagged `@FieldContext(dynamic = true)`
    /// — the set of keys that `brokers_set_dynamic_config` accepts. Use
    /// [`Self::brokers_dynamic_config_overrides`] for the current values.
    /// Java: `BrokersBase#getDynamicConfigurationName`.
    pub async fn brokers_dynamic_config_keys(&self) -> Result<Vec<String>, AdminError> {
        let url = self.url(&["brokers", "configuration"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Get the currently-overridden dynamic configuration values.
    ///
    /// `GET /admin/v2/brokers/configuration/values`. Returns a
    /// `Map<String, String>` of every dynamic key the operator has set
    /// (the broker omits keys still on their static / default value).
    /// Exposed as raw JSON because broker minor versions add keys.
    /// Java: `BrokersBase#getAllDynamicConfigurations`.
    pub async fn brokers_dynamic_config_overrides(&self) -> Result<serde_json::Value, AdminError> {
        let url = self.url(&["brokers", "configuration", "values"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Get the broker's runtime (merged static + dynamic) configuration.
    ///
    /// `GET /admin/v2/brokers/configuration/runtime`. Returns the full
    /// `Map<String, String>` of `ServiceConfiguration` values as they
    /// currently apply on the broker process — static defaults
    /// overlaid with any `brokers_set_dynamic_config` overrides. Raw
    /// JSON for forward-compat. Java: `BrokersBase#getRuntimeConfiguration`.
    pub async fn brokers_runtime_config(&self) -> Result<serde_json::Value, AdminError> {
        let url = self.url(&["brokers", "configuration", "runtime"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Get the broker's internal-stack endpoints.
    ///
    /// `GET /admin/v2/brokers/internal-configuration`. Returns the
    /// `InternalConfigurationData` envelope — metadata-store URLs
    /// (`zookeeperServers`, `configurationMetadataStoreUrl`),
    /// `BookKeeper` metadata service URI, ledger root paths. Raw JSON
    /// for forward-compat; the shape rolls between releases as the
    /// metadata layer evolves.
    /// Java: `BrokersBase#getInternalConfigurationData`.
    pub async fn brokers_internal_config(&self) -> Result<serde_json::Value, AdminError> {
        let url = self.url(&["brokers", "internal-configuration"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Probe broker health — produces and consumes one heartbeat message
    /// on an internal topic.
    ///
    /// `GET /admin/v2/brokers/health`. The broker returns the plain-text
    /// string `"ok"` on success; non-200 surfaces as `AdminError::Status`.
    /// Java: `BrokersBase#healthCheck`.
    pub async fn brokers_health_check(&self) -> Result<String, AdminError> {
        let url = self.url(&["brokers", "health"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        let resp = ensure_status(resp).await?;
        Ok(resp.text().await?)
    }

    /// List the namespaces a specific broker currently owns.
    ///
    /// `GET /admin/v2/brokers/{cluster}/{broker}/ownedNamespaces`. The
    /// `broker` argument must be the broker's `host:port` (matching the
    /// strings [`Self::brokers_list`] returns). Returns a
    /// `Map<String, NamespaceOwnershipStatus>` keyed by namespace name —
    /// raw JSON for forward-compat.
    /// Java: `BrokersBase#getOwnedNamespaces`.
    pub async fn brokers_owned_namespaces(
        &self,
        cluster: &str,
        broker: &str,
    ) -> Result<serde_json::Value, AdminError> {
        let url = self.url(&["brokers", cluster, broker, "ownedNamespaces"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Override a dynamic broker configuration value.
    ///
    /// `POST /admin/v2/brokers/configuration/{name}/{value}`. Both the
    /// key and the value travel in the URL path — there is no request
    /// body — matching the broker's `updateDynamicConfiguration(@PathParam
    /// String configName, @PathParam String configValue)` signature.
    /// The key must be one of those returned by
    /// [`Self::brokers_dynamic_config_keys`]; unknown keys yield 412.
    /// Java: `BrokersBase#updateDynamicConfiguration`.
    pub async fn brokers_set_dynamic_config(
        &self,
        name: &str,
        value: &str,
    ) -> Result<(), AdminError> {
        validate_segment(name)?;
        validate_segment(value)?;
        let url = self.url(&["brokers", "configuration", name, value])?;
        let resp = self.send(self.http.request(Method::POST, url)).await?;
        empty_ok(resp).await
    }

    /// Drop a dynamic configuration override, reverting to the static value.
    ///
    /// `DELETE /admin/v2/brokers/configuration/{name}`. After the call
    /// the key disappears from [`Self::brokers_dynamic_config_overrides`]
    /// and [`Self::brokers_runtime_config`] reflects the underlying
    /// static / default value again.
    /// Java: `BrokersBase#deleteDynamicConfiguration`.
    pub async fn brokers_delete_dynamic_config(&self, name: &str) -> Result<(), AdminError> {
        validate_segment(name)?;
        let url = self.url(&["brokers", "configuration", name])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    // --- Bookies ---------------------------------------------------------

    /// List every bookie the broker knows about — both writable and
    /// read-only — as registered in `BookKeeper` metadata.
    ///
    /// `GET /admin/v2/bookies/all`. Returns the broker's
    /// `BookiesClusterInfo` envelope — a `bookies: [{ address: "host:port" }]`
    /// array. Raw JSON for forward-compat.
    /// Java: `BookiesBase#getAllAvailableBookies`.
    pub async fn bookies_list_all(&self) -> Result<serde_json::Value, AdminError> {
        let url = self.url(&["bookies", "all"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Get every bookie's group + rack assignment, as configured for the
    /// rack-aware placement policy.
    ///
    /// `GET /admin/v2/bookies/racks-info`. Returns the nested
    /// `Map<group, Map<bookieAddress, BookieInfo>>` shape Pulsar
    /// persists in metadata. Raw JSON because the wire shape exposes
    /// nested maps that change between releases (the `default` group
    /// is implicit on older brokers).
    /// Java: `BookiesBase#getBookieRackInfo`.
    pub async fn bookies_racks_info(&self) -> Result<serde_json::Value, AdminError> {
        let url = self.url(&["bookies", "racks-info"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set (or update) a bookie's rack assignment.
    ///
    /// `POST /admin/v2/bookies/racks-info/{bookie}` with a JSON
    /// [`BookieInfo`] body. `bookie` is the `host:port` registered in
    /// `BookKeeper` metadata. The placement policy picks up the new
    /// rack on its next reconciliation tick.
    /// Java: `BookiesBase#updateBookieRackInfo`.
    pub async fn bookies_set_rack(&self, bookie: &str, info: BookieInfo) -> Result<(), AdminError> {
        let url = self.url(&["bookies", "racks-info", bookie])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&info))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a bookie's rack assignment.
    ///
    /// `DELETE /admin/v2/bookies/racks-info/{bookie}`. The bookie falls
    /// back to the placement policy's default group / rack until
    /// [`Self::bookies_set_rack`] is called again.
    /// Java: `BookiesBase#deleteBookieRackInfo`.
    pub async fn bookies_delete_rack(&self, bookie: &str) -> Result<(), AdminError> {
        let url = self.url(&["bookies", "racks-info", bookie])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    // --- Schemas ---------------------------------------------------------

    /// Get the latest schema attached to a topic.
    ///
    /// `GET /admin/v2/schemas/{tenant}/{ns}/{topic}/schema`. Returns
    /// `{ version, type, schema, properties, timestamp }`; raw JSON
    /// because the `type` axis (`AVRO` / `JSON` / `PROTOBUF` /
    /// `PROTOBUF_NATIVE` / `KEY_VALUE` / `STRING` / `BYTES` / …) is
    /// open-ended and broker minor versions add keys (deletion
    /// tombstones surface as `type: "DELETE"` on the GET, for
    /// instance). Java: `SchemasResourceBase#getSchema`.
    pub async fn schema_get_latest(&self, topic: &str) -> Result<serde_json::Value, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["schemas", tenant, namespace, name, "schema"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Get a specific schema version attached to a topic.
    ///
    /// `GET /admin/v2/schemas/{tenant}/{ns}/{topic}/schema/{version}`.
    /// `version` is the monotonically-increasing integer the broker
    /// assigns at registration. Same wire shape as
    /// [`Self::schema_get_latest`].
    /// Java: `SchemasResourceBase#getSchema` (with version path param).
    pub async fn schema_get_version(
        &self,
        topic: &str,
        version: i64,
    ) -> Result<serde_json::Value, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let v = version.to_string();
        let url = self.url(&["schemas", tenant, namespace, name, "schema", &v])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// List every schema version registered for a topic.
    ///
    /// `GET /admin/v2/schemas/{tenant}/{ns}/{topic}/schemas`. Returns a
    /// JSON array — one entry per version, each carrying the same
    /// per-version shape as [`Self::schema_get_latest`]. Raw JSON for
    /// forward-compat.
    /// Java: `SchemasResourceBase#getAllSchemas`.
    pub async fn schema_list_versions(
        &self,
        topic: &str,
    ) -> Result<Vec<serde_json::Value>, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["schemas", tenant, namespace, name, "schemas"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Register a new schema version on a topic.
    ///
    /// `POST /admin/v2/schemas/{tenant}/{ns}/{topic}/schema` with a JSON
    /// [`PostSchemaPayload`] body. The broker returns `{ version: N }`;
    /// raw JSON because the upstream response envelope wraps the
    /// version under `data` on some 4.x point releases. Compatibility
    /// is enforced server-side per the namespace's
    /// `schemaCompatibilityStrategy` — incompatible posts fail with
    /// 409. Java: `SchemasResourceBase#postSchema`.
    pub async fn schema_post(
        &self,
        topic: &str,
        payload: PostSchemaPayload,
    ) -> Result<serde_json::Value, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["schemas", tenant, namespace, name, "schema"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&payload))
            .await?;
        json_ok(resp).await
    }

    /// Delete a topic's schema.
    ///
    /// `DELETE /admin/v2/schemas/{tenant}/{ns}/{topic}/schema?force={force}`.
    /// `force = true` skips the broker's "is the schema in use"
    /// guard — equivalent to `pulsar-admin schemas delete --force`.
    /// Java: `SchemasResourceBase#deleteSchema`.
    pub async fn schema_delete(&self, topic: &str, force: bool) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let mut url = self.url(&["schemas", tenant, namespace, name, "schema"])?;
        url.query_pairs_mut()
            .append_pair("force", if force { "true" } else { "false" });
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Check whether a candidate schema would be compatible with the
    /// topic's current schema.
    ///
    /// `POST /admin/v2/schemas/{tenant}/{ns}/{topic}/compatibility` with
    /// a JSON [`PostSchemaPayload`] body — the same shape
    /// [`Self::schema_post`] sends, but the broker only evaluates
    /// compatibility and never persists. Returns `{ isCompatible:
    /// bool, schemaCompatibilityStrategy: "..." }`; raw JSON for
    /// forward-compat.
    /// Java: `SchemasResourceBase#testCompatibility`.
    pub async fn schema_compatibility_check(
        &self,
        topic: &str,
        payload: PostSchemaPayload,
    ) -> Result<serde_json::Value, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["schemas", tenant, namespace, name, "compatibility"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&payload))
            .await?;
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

    /// Get a namespace's retention policy.
    ///
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/retention`.
    /// Returns `RetentionPolicies { retentionTimeInMinutes, retentionSizeInMB }`.
    /// Java: `NamespacesBase#getRetention`.
    pub async fn namespace_get_retention(&self, ns: &str) -> Result<RetentionPolicies, AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "retention"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a namespace's retention policy.
    ///
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/retention` with a JSON
    /// `RetentionPolicies` body. `-1` means infinite (size or time).
    /// Java: `NamespacesBase#setRetention`.
    pub async fn namespace_set_retention(
        &self,
        ns: &str,
        policy: RetentionPolicies,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "retention"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&policy))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a namespace's retention policy (fall back to broker default).
    ///
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/retention`.
    /// Java: `NamespacesBase#removeRetention`.
    pub async fn namespace_remove_retention(&self, ns: &str) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "retention"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get all backlog-quota policies on a namespace.
    ///
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/backlogQuotaMap`. Returns
    /// `Map<BacklogQuotaType, BacklogQuota>` — kept as raw JSON because
    /// broker versions add quota types (`message_age` since 2.10).
    /// Java: `NamespacesBase#getBacklogQuotaMap`.
    pub async fn namespace_get_backlog_quotas(
        &self,
        ns: &str,
    ) -> Result<serde_json::Value, AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "backlogQuotaMap"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a backlog-quota policy on a namespace.
    ///
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/backlogQuota?backlogQuotaType={type}`
    /// with a JSON `BacklogQuota` body. `backlog_quota_type` selects which
    /// dimension to limit (`destination_storage` for byte size, `message_age`
    /// for wall-clock TTL).
    /// Java: `NamespacesBase#setBacklogQuota`.
    pub async fn namespace_set_backlog_quota(
        &self,
        ns: &str,
        backlog_quota_type: BacklogQuotaType,
        quota: BacklogQuota,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let mut url = self.url(&["namespaces", tenant, namespace, "backlogQuota"])?;
        url.query_pairs_mut()
            .append_pair("backlogQuotaType", backlog_quota_type.as_query_value());
        let resp = self
            .send(self.http.request(Method::POST, url).json(&quota))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a backlog-quota policy from a namespace.
    ///
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/backlogQuota?backlogQuotaType={type}`.
    /// Java: `NamespacesBase#removeBacklogQuota`.
    pub async fn namespace_remove_backlog_quota(
        &self,
        ns: &str,
        backlog_quota_type: BacklogQuotaType,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let mut url = self.url(&["namespaces", tenant, namespace, "backlogQuota"])?;
        url.query_pairs_mut()
            .append_pair("backlogQuotaType", backlog_quota_type.as_query_value());
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a namespace's message-TTL (seconds).
    ///
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/messageTTL`. Returns a
    /// bare integer (or `null` if no TTL is set — which decodes as
    /// `Option::None`).
    /// Java: `NamespacesBase#getNamespaceMessageTTL`.
    pub async fn namespace_get_message_ttl(&self, ns: &str) -> Result<Option<i32>, AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "messageTTL"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a namespace's message-TTL (seconds).
    ///
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/messageTTL` with a bare
    /// integer body. `0` disables (broker treats as no TTL).
    /// Java: `NamespacesBase#setNamespaceMessageTTL`.
    pub async fn namespace_set_message_ttl(
        &self,
        ns: &str,
        ttl_seconds: i32,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "messageTTL"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&ttl_seconds))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a namespace's message-TTL (fall back to broker default).
    ///
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/messageTTL`.
    /// Java: `NamespacesBase#removeNamespaceMessageTTL`.
    pub async fn namespace_remove_message_ttl(&self, ns: &str) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "messageTTL"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    // --- Namespace policies — persistence + rates ----------------------

    /// Get a namespace's persistence policy.
    ///
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/persistence`. Returns the
    /// BookKeeper ensemble / write-quorum / ack-quorum triple plus the
    /// managed-ledger mark-delete rate cap. `null` body decodes to
    /// `PersistencePolicies::default()` via `#[serde(default)]`.
    /// Java: `NamespacesBase#getPersistence`.
    pub async fn namespace_get_persistence(
        &self,
        ns: &str,
    ) -> Result<PersistencePolicies, AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "persistence"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a namespace's persistence policy.
    ///
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/persistence` with a JSON
    /// `PersistencePolicies` body.
    /// Java: `NamespacesBase#setPersistence`.
    pub async fn namespace_set_persistence(
        &self,
        ns: &str,
        policy: PersistencePolicies,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "persistence"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&policy))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a namespace's persistence policy (fall back to broker default).
    ///
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/persistence`.
    /// Java: `NamespacesBase#deletePersistence`.
    pub async fn namespace_remove_persistence(&self, ns: &str) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "persistence"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a namespace's consumer dispatch-rate policy.
    ///
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/dispatchRate`. Returns
    /// the per-namespace consumer-dispatch throttle (msg/sec, byte/sec,
    /// window in seconds). `-1` on either dimension means unlimited.
    /// Java: `NamespacesBase#getDispatchRate`.
    pub async fn namespace_get_dispatch_rate(&self, ns: &str) -> Result<DispatchRate, AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "dispatchRate"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a namespace's consumer dispatch-rate policy.
    ///
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/dispatchRate` with a
    /// JSON `DispatchRate` body.
    /// Java: `NamespacesBase#setDispatchRate`.
    pub async fn namespace_set_dispatch_rate(
        &self,
        ns: &str,
        rate: DispatchRate,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "dispatchRate"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&rate))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a namespace's consumer dispatch-rate policy.
    ///
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/dispatchRate`.
    /// Java: `NamespacesBase#deleteDispatchRate`.
    pub async fn namespace_remove_dispatch_rate(&self, ns: &str) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "dispatchRate"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a namespace's per-subscription dispatch-rate policy.
    ///
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/subscriptionDispatchRate`.
    /// Reuses the [`DispatchRate`] body shape — the policy applies per
    /// subscription rather than aggregated across all consumers.
    /// Java: `NamespacesBase#getSubscriptionDispatchRate`.
    pub async fn namespace_get_subscription_dispatch_rate(
        &self,
        ns: &str,
    ) -> Result<DispatchRate, AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "subscriptionDispatchRate"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a namespace's per-subscription dispatch-rate policy.
    ///
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/subscriptionDispatchRate`
    /// with a JSON `DispatchRate` body.
    /// Java: `NamespacesBase#setSubscriptionDispatchRate`.
    pub async fn namespace_set_subscription_dispatch_rate(
        &self,
        ns: &str,
        rate: DispatchRate,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "subscriptionDispatchRate"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&rate))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a namespace's per-subscription dispatch-rate policy.
    ///
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/subscriptionDispatchRate`.
    /// Java: `NamespacesBase#deleteSubscriptionDispatchRate`.
    pub async fn namespace_remove_subscription_dispatch_rate(
        &self,
        ns: &str,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "subscriptionDispatchRate"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a namespace's cross-cluster replicator dispatch-rate policy.
    ///
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/replicatorDispatchRate`.
    /// Reuses the [`DispatchRate`] body shape — the policy throttles
    /// outbound geo-replication traffic from this cluster.
    /// Java: `NamespacesBase#getReplicatorDispatchRate`.
    pub async fn namespace_get_replicator_dispatch_rate(
        &self,
        ns: &str,
    ) -> Result<DispatchRate, AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "replicatorDispatchRate"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a namespace's cross-cluster replicator dispatch-rate policy.
    ///
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/replicatorDispatchRate`
    /// with a JSON `DispatchRate` body.
    /// Java: `NamespacesBase#setReplicatorDispatchRate`.
    pub async fn namespace_set_replicator_dispatch_rate(
        &self,
        ns: &str,
        rate: DispatchRate,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "replicatorDispatchRate"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&rate))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a namespace's cross-cluster replicator dispatch-rate policy.
    ///
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/replicatorDispatchRate`.
    /// Java: `NamespacesBase#removeReplicatorDispatchRate`.
    pub async fn namespace_remove_replicator_dispatch_rate(
        &self,
        ns: &str,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "replicatorDispatchRate"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a namespace's publish-rate policy.
    ///
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/publishRate`. Returns
    /// the producer-side throttle (msg/sec + byte/sec). `-1` on either
    /// dimension means unlimited.
    /// Java: `NamespacesBase#getPublishRate`.
    pub async fn namespace_get_publish_rate(&self, ns: &str) -> Result<PublishRate, AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "publishRate"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a namespace's publish-rate policy.
    ///
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/publishRate` with a JSON
    /// `PublishRate` body.
    /// Java: `NamespacesBase#setPublishRate`.
    pub async fn namespace_set_publish_rate(
        &self,
        ns: &str,
        rate: PublishRate,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "publishRate"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&rate))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a namespace's publish-rate policy.
    ///
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/publishRate`.
    /// Java: `NamespacesBase#removePublishRate`.
    pub async fn namespace_remove_publish_rate(&self, ns: &str) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "publishRate"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    // --- Namespace policies — limits + dedup + delayed delivery -----

    /// Get a namespace's broker-side message deduplication flag.
    ///
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/deduplication`. Returns a
    /// bare JSON boolean, or `null` (decoded as `None`) when the policy
    /// is unset and the broker default applies.
    /// Java: `NamespacesBase#getDeduplication`.
    pub async fn namespace_get_deduplication(
        &self,
        ns: &str,
    ) -> Result<Option<bool>, AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "deduplication"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a namespace's broker-side message deduplication flag.
    ///
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/deduplication` with a
    /// bare JSON boolean body.
    /// Java: `NamespacesBase#modifyDeduplication`.
    pub async fn namespace_set_deduplication(
        &self,
        ns: &str,
        enabled: bool,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "deduplication"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&enabled))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a namespace's deduplication flag (fall back to broker default).
    ///
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/deduplication`.
    /// Java: `NamespacesBase#removeDeduplication`.
    pub async fn namespace_remove_deduplication(&self, ns: &str) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "deduplication"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a namespace's deduplication-snapshot interval (entries).
    ///
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/deduplicationSnapshotInterval`.
    /// Returns a bare integer (the entry count between dedup cursor
    /// snapshots), or `null` (decoded as `None`) when the broker default
    /// applies.
    /// Java: `NamespacesBase#getDeduplicationSnapshotInterval`.
    pub async fn namespace_get_deduplication_snapshot_interval(
        &self,
        ns: &str,
    ) -> Result<Option<i32>, AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&[
            "namespaces",
            tenant,
            namespace,
            "deduplicationSnapshotInterval",
        ])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a namespace's deduplication-snapshot interval (entries).
    ///
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/deduplicationSnapshotInterval`
    /// with a bare JSON integer body.
    /// Java: `NamespacesBase#setDeduplicationSnapshotInterval`.
    pub async fn namespace_set_deduplication_snapshot_interval(
        &self,
        ns: &str,
        interval_entries: i32,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&[
            "namespaces",
            tenant,
            namespace,
            "deduplicationSnapshotInterval",
        ])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&interval_entries))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a namespace's deduplication-snapshot interval override.
    ///
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/deduplicationSnapshotInterval`.
    /// Java: `NamespacesBase#deleteDeduplicationSnapshotInterval`.
    pub async fn namespace_remove_deduplication_snapshot_interval(
        &self,
        ns: &str,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&[
            "namespaces",
            tenant,
            namespace,
            "deduplicationSnapshotInterval",
        ])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a namespace's compaction threshold (bytes).
    ///
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/compactionThreshold`. Returns
    /// a bare integer (bytes of accumulated topic backlog above which the
    /// broker triggers automatic compaction), or `null` (decoded as `None`)
    /// when the broker default applies.
    /// Java: `NamespacesBase#getCompactionThreshold`.
    pub async fn namespace_get_compaction_threshold(
        &self,
        ns: &str,
    ) -> Result<Option<i64>, AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "compactionThreshold"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a namespace's compaction threshold (bytes).
    ///
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/compactionThreshold` with
    /// a bare JSON long body. `0` disables automatic compaction.
    /// Java: `NamespacesBase#setCompactionThreshold`.
    pub async fn namespace_set_compaction_threshold(
        &self,
        ns: &str,
        threshold_bytes: i64,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "compactionThreshold"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&threshold_bytes))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a namespace's compaction threshold override.
    ///
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/compactionThreshold`.
    /// Java: `NamespacesBase#deleteCompactionThreshold`.
    pub async fn namespace_remove_compaction_threshold(
        &self,
        ns: &str,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "compactionThreshold"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a namespace's delayed-delivery policy.
    ///
    /// `GET /admin/v2/namespaces/{tenant}/{ns}/delayedDelivery`. Returns
    /// the active flag + tick time (the broker's index-tick granularity
    /// for delivering delayed messages). `null` decodes as `None`.
    /// Java: `NamespacesBase#getDelayedDeliveryPolicies`.
    pub async fn namespace_get_delayed_delivery(
        &self,
        ns: &str,
    ) -> Result<Option<DelayedDeliveryPolicies>, AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "delayedDelivery"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a namespace's delayed-delivery policy.
    ///
    /// `POST /admin/v2/namespaces/{tenant}/{ns}/delayedDelivery` with a
    /// JSON `DelayedDeliveryPolicies` body.
    /// Java: `NamespacesBase#setDelayedDeliveryPolicies`.
    pub async fn namespace_set_delayed_delivery(
        &self,
        ns: &str,
        policy: DelayedDeliveryPolicies,
    ) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "delayedDelivery"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&policy))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a namespace's delayed-delivery policy override.
    ///
    /// `DELETE /admin/v2/namespaces/{tenant}/{ns}/delayedDelivery`.
    /// Java: `NamespacesBase#removeDelayedDeliveryPolicies`.
    pub async fn namespace_remove_delayed_delivery(&self, ns: &str) -> Result<(), AdminError> {
        let (tenant, namespace) = split_namespace(ns)?;
        let url = self.url(&["namespaces", tenant, namespace, "delayedDelivery"])?;
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
    ///
    /// For a **partitioned** topic, the broker returns 404 on this endpoint
    /// because there is no ledger backing the parent name. Call
    /// [`Self::topic_partitioned_stats`] instead, or look up the count via
    /// [`Self::topic_partitions_count`] first.
    pub async fn topic_stats(&self, topic: &str) -> Result<TopicStats, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "stats"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Get aggregated stats for a partitioned topic.
    ///
    /// `GET /admin/v2/persistent/{tenant}/{namespace}/{topic}/partitioned-stats?
    /// perPartition=false`. Java: `PersistentTopics.java#getPartitionedStats`
    /// (`@GET @Path("/{tenant}/{namespace}/{topic}/partitioned-stats")`,
    /// response shape `PartitionedTopicStats` which extends
    /// `PersistentTopicStats` with `partitions: Map<String, TopicStats>`
    /// and `metadata: PartitionedTopicMetadata`).
    ///
    /// magnetar exposes only the aggregated top-level counters through the
    /// same [`TopicStats`] shape — the broker populates `msgInCounter`,
    /// `bytesInCounter`, `publishers`, `subscriptions` at the response root
    /// summed across partitions. The `partitions` and `metadata` fields are
    /// dropped on deserialisation; for per-partition detail call
    /// [`Self::topic_stats`] on each `<topic>-partition-N` instead. We pass
    /// `perPartition=false` to keep the wire response small.
    pub async fn topic_partitioned_stats(&self, topic: &str) -> Result<TopicStats, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let mut url = self.url(&["persistent", tenant, namespace, name, "partitioned-stats"])?;
        url.query_pairs_mut().append_pair("perPartition", "false");
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Resolve the partition count of a topic.
    ///
    /// `GET /admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions`.
    /// Java: `PersistentTopics.java#getPartitionedMetadata`
    /// (`@GET @Path("/{tenant}/{namespace}/{topic}/partitions")`,
    /// response shape `PartitionedTopicMetadata{ partitions: int }`).
    ///
    /// Returns `0` for non-partitioned topics; lets a caller disambiguate
    /// between [`Self::topic_stats`] and [`Self::topic_partitioned_stats`]
    /// when the topology is not known in advance.
    pub async fn topic_partitions_count(&self, topic: &str) -> Result<u32, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "partitions"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        let meta: PartitionedTopicMetadata = json_ok(resp).await?;
        Ok(meta.partitions)
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

    /// Trigger ledger compaction for a topic.
    ///
    /// `PUT /admin/v2/persistent/{tenant}/{namespace}/{topic}/compaction`.
    /// Returns 204 on success; the broker queues the work asynchronously —
    /// poll [`Self::topic_compaction_status`] to observe progress.
    /// Java: `PersistentTopics#triggerCompaction`.
    pub async fn topic_compact(&self, topic: &str) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "compaction"])?;
        let resp = self.send(self.http.request(Method::PUT, url)).await?;
        empty_ok(resp).await
    }

    /// Get the current compaction status for a topic.
    ///
    /// `GET /admin/v2/persistent/{tenant}/{namespace}/{topic}/compaction`.
    /// Returns Java's `LongRunningProcessStatus`: `status` ∈ {`NOT_RUN`,
    /// `RUNNING`, `SUCCESS`, `ERROR`} plus an optional `lastError` string.
    /// Java: `PersistentTopics#compactionStatus`.
    pub async fn topic_compaction_status(
        &self,
        topic: &str,
    ) -> Result<LongRunningProcessStatus, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "compaction"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Unload a topic from its current broker (forces rebalancing).
    ///
    /// `PUT /admin/v2/persistent/{tenant}/{namespace}/{topic}/unload`.
    /// Operators use this to drain a hot broker or to re-elect ownership
    /// after a configuration change. Java: `PersistentTopics#unloadTopic`.
    pub async fn topic_unload(&self, topic: &str) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "unload"])?;
        let resp = self.send(self.http.request(Method::PUT, url)).await?;
        empty_ok(resp).await
    }

    /// Terminate (seal) a topic — no further produces succeed.
    ///
    /// `POST /admin/v2/persistent/{tenant}/{namespace}/{topic}/terminate`.
    /// Returns the [`MessageId`] of the last message that landed before the
    /// seal. Java: `PersistentTopics#terminate`.
    pub async fn topic_terminate(&self, topic: &str) -> Result<MessageId, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "terminate"])?;
        let resp = self.send(self.http.request(Method::POST, url)).await?;
        let dto: MessageIdResponse = json_ok(resp).await?;
        dto.try_into_message_id()
    }

    /// Grow a partitioned topic's partition count.
    ///
    /// `POST /admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions`
    /// with a bare JSON integer body. Only forward (grow) is supported by
    /// the broker — shrinking returns 409. Java:
    /// `PersistentTopics#updatePartitionedTopic`.
    pub async fn topic_update_partitions(
        &self,
        topic: &str,
        new_partitions: u32,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "partitions"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&new_partitions))
            .await?;
        empty_ok(resp).await
    }

    // --- Topic policies — per-topic overrides ---------------------------

    /// Get a topic's retention policy.
    ///
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/retention`.
    /// Returns the per-topic [`RetentionPolicies`] override; the broker
    /// emits a `RetentionPolicies` JSON when the policy is set and a bare
    /// `null` (decoded as `RetentionPolicies::default()` via `#[serde(default)]`)
    /// when no override is in place — callers fall back to the namespace
    /// policy in that case. Java: `PersistentTopicsBase#getRetention`.
    pub async fn topic_get_retention(&self, topic: &str) -> Result<RetentionPolicies, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "retention"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a topic's retention policy (overrides the namespace default).
    ///
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/retention` with a
    /// JSON `RetentionPolicies` body. `-1` means infinite (size or time).
    /// Java: `PersistentTopicsBase#setRetention`.
    pub async fn topic_set_retention(
        &self,
        topic: &str,
        policy: RetentionPolicies,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "retention"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&policy))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a topic's retention policy (fall back to namespace default).
    ///
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/retention`.
    /// Java: `PersistentTopicsBase#removeRetention`.
    pub async fn topic_remove_retention(&self, topic: &str) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "retention"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get all backlog-quota policies on a topic.
    ///
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/backlogQuotaMap`.
    /// Returns `Map<BacklogQuotaType, BacklogQuota>` — kept as raw JSON
    /// for the same reason as [`Self::namespace_get_backlog_quotas`]:
    /// broker minor versions add quota types.
    /// Java: `PersistentTopicsBase#getBacklogQuotaMap`.
    pub async fn topic_get_backlog_quotas(
        &self,
        topic: &str,
    ) -> Result<serde_json::Value, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "backlogQuotaMap"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a backlog-quota policy on a topic (overrides the namespace
    /// default for the matching `backlogQuotaType`).
    ///
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/backlogQuota
    /// ?backlogQuotaType={type}` with a JSON `BacklogQuota` body.
    /// Java: `PersistentTopicsBase#setBacklogQuota`.
    pub async fn topic_set_backlog_quota(
        &self,
        topic: &str,
        backlog_quota_type: BacklogQuotaType,
        quota: BacklogQuota,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let mut url = self.url(&["persistent", tenant, namespace, name, "backlogQuota"])?;
        url.query_pairs_mut()
            .append_pair("backlogQuotaType", backlog_quota_type.as_query_value());
        let resp = self
            .send(self.http.request(Method::POST, url).json(&quota))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a backlog-quota policy from a topic.
    ///
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/backlogQuota
    /// ?backlogQuotaType={type}`.
    /// Java: `PersistentTopicsBase#removeBacklogQuota`.
    pub async fn topic_remove_backlog_quota(
        &self,
        topic: &str,
        backlog_quota_type: BacklogQuotaType,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let mut url = self.url(&["persistent", tenant, namespace, name, "backlogQuota"])?;
        url.query_pairs_mut()
            .append_pair("backlogQuotaType", backlog_quota_type.as_query_value());
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a topic's message-TTL (seconds, or `null` if unset).
    ///
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/messageTTL`. Returns
    /// a bare integer when the override is set, `null` (decoded as
    /// `Option::None`) when no topic-level override is in place.
    /// Java: `PersistentTopicsBase#getMessageTTL`.
    pub async fn topic_get_message_ttl(&self, topic: &str) -> Result<Option<i32>, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "messageTTL"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a topic's message-TTL (seconds).
    ///
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/messageTTL` with
    /// a bare integer body. `0` disables (broker treats as no TTL).
    /// Java: `PersistentTopicsBase#setMessageTTL`.
    pub async fn topic_set_message_ttl(
        &self,
        topic: &str,
        ttl_seconds: i32,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "messageTTL"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&ttl_seconds))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a topic's message-TTL (fall back to namespace default).
    ///
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/messageTTL`.
    /// Java: `PersistentTopicsBase#removeMessageTTL`.
    pub async fn topic_remove_message_ttl(&self, topic: &str) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "messageTTL"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a topic's persistence policy.
    ///
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/persistence`. The
    /// broker emits a `PersistencePolicies` JSON when the topic override
    /// is set and `null` (decoded as `Option::None`) when no override is
    /// in place — callers fall back to the namespace policy in that case.
    /// Java: `PersistentTopicsBase#getPersistence`.
    pub async fn topic_get_persistence(
        &self,
        topic: &str,
    ) -> Result<Option<PersistencePolicies>, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "persistence"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a topic's persistence policy (overrides the namespace default).
    ///
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/persistence` with a
    /// JSON `PersistencePolicies` body.
    /// Java: `PersistentTopicsBase#setPersistence`.
    pub async fn topic_set_persistence(
        &self,
        topic: &str,
        policy: PersistencePolicies,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "persistence"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&policy))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a topic's persistence policy (fall back to namespace default).
    ///
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/persistence`.
    /// Java: `PersistentTopicsBase#removePersistence`.
    pub async fn topic_remove_persistence(&self, topic: &str) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "persistence"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a topic's consumer dispatch-rate policy (or `null` if no override).
    ///
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/dispatchRate`. The
    /// broker emits the per-topic [`DispatchRate`] override or `null` when
    /// no override is set; callers fall back to the namespace policy in the
    /// `None` case. Java: `PersistentTopicsBase#getDispatchRate`.
    pub async fn topic_get_dispatch_rate(
        &self,
        topic: &str,
    ) -> Result<Option<DispatchRate>, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "dispatchRate"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a topic's consumer dispatch-rate policy (overrides namespace default).
    ///
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/dispatchRate` with a
    /// JSON `DispatchRate` body. Java: `PersistentTopicsBase#setDispatchRate`.
    pub async fn topic_set_dispatch_rate(
        &self,
        topic: &str,
        rate: DispatchRate,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "dispatchRate"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&rate))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a topic's consumer dispatch-rate policy.
    ///
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/dispatchRate`.
    /// Java: `PersistentTopicsBase#removeDispatchRate`.
    pub async fn topic_remove_dispatch_rate(&self, topic: &str) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "dispatchRate"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a topic's per-subscription dispatch-rate policy (or `null`).
    ///
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/subscriptionDispatchRate`.
    /// Reuses the [`DispatchRate`] body shape — the policy applies per
    /// subscription rather than aggregated across all consumers.
    /// Java: `PersistentTopicsBase#getSubscriptionDispatchRate`.
    pub async fn topic_get_subscription_dispatch_rate(
        &self,
        topic: &str,
    ) -> Result<Option<DispatchRate>, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&[
            "persistent",
            tenant,
            namespace,
            name,
            "subscriptionDispatchRate",
        ])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a topic's per-subscription dispatch-rate policy (overrides namespace default).
    ///
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/subscriptionDispatchRate`
    /// with a JSON `DispatchRate` body.
    /// Java: `PersistentTopicsBase#setSubscriptionDispatchRate`.
    pub async fn topic_set_subscription_dispatch_rate(
        &self,
        topic: &str,
        rate: DispatchRate,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&[
            "persistent",
            tenant,
            namespace,
            name,
            "subscriptionDispatchRate",
        ])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&rate))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a topic's per-subscription dispatch-rate policy.
    ///
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/subscriptionDispatchRate`.
    /// Java: `PersistentTopicsBase#removeSubscriptionDispatchRate`.
    pub async fn topic_remove_subscription_dispatch_rate(
        &self,
        topic: &str,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&[
            "persistent",
            tenant,
            namespace,
            name,
            "subscriptionDispatchRate",
        ])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a topic's cross-cluster replicator dispatch-rate policy (or `null`).
    ///
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/replicatorDispatchRate`.
    /// Reuses the [`DispatchRate`] body shape — the policy throttles
    /// outbound geo-replication traffic from this cluster.
    /// Java: `PersistentTopicsBase#getReplicatorDispatchRate`.
    pub async fn topic_get_replicator_dispatch_rate(
        &self,
        topic: &str,
    ) -> Result<Option<DispatchRate>, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&[
            "persistent",
            tenant,
            namespace,
            name,
            "replicatorDispatchRate",
        ])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a topic's cross-cluster replicator dispatch-rate policy.
    ///
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/replicatorDispatchRate`
    /// with a JSON `DispatchRate` body.
    /// Java: `PersistentTopicsBase#setReplicatorDispatchRate`.
    pub async fn topic_set_replicator_dispatch_rate(
        &self,
        topic: &str,
        rate: DispatchRate,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&[
            "persistent",
            tenant,
            namespace,
            name,
            "replicatorDispatchRate",
        ])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&rate))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a topic's cross-cluster replicator dispatch-rate policy.
    ///
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/replicatorDispatchRate`.
    /// Java: `PersistentTopicsBase#removeReplicatorDispatchRate`.
    pub async fn topic_remove_replicator_dispatch_rate(
        &self,
        topic: &str,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&[
            "persistent",
            tenant,
            namespace,
            name,
            "replicatorDispatchRate",
        ])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a topic's publish-rate policy (or `null` if no override).
    ///
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/publishRate`. Returns
    /// the per-topic [`PublishRate`] producer-side throttle (msg/sec +
    /// byte/sec). `-1` on either dimension means unlimited.
    /// Java: `PersistentTopicsBase#getPublishRate`.
    pub async fn topic_get_publish_rate(
        &self,
        topic: &str,
    ) -> Result<Option<PublishRate>, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "publishRate"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a topic's publish-rate policy (overrides namespace default).
    ///
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/publishRate` with a
    /// JSON `PublishRate` body. Java: `PersistentTopicsBase#setPublishRate`.
    pub async fn topic_set_publish_rate(
        &self,
        topic: &str,
        rate: PublishRate,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "publishRate"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&rate))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a topic's publish-rate policy.
    ///
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/publishRate`.
    /// Java: `PersistentTopicsBase#removePublishRate`.
    pub async fn topic_remove_publish_rate(&self, topic: &str) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "publishRate"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a topic's max-producers cap (or `null` if no override).
    ///
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/maxProducers`. Returns
    /// a bare integer when the override is set, `null` (decoded as
    /// `Option::None`) when no topic-level cap is in place.
    /// Java: `PersistentTopicsBase#getMaxProducers`.
    pub async fn topic_get_max_producers(&self, topic: &str) -> Result<Option<i32>, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "maxProducers"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a topic's max-producers cap.
    ///
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/maxProducers` with
    /// a bare integer body. `0` disables (broker treats as unlimited).
    /// Java: `PersistentTopicsBase#setMaxProducers`.
    pub async fn topic_set_max_producers(
        &self,
        topic: &str,
        max_producers: i32,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "maxProducers"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&max_producers))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a topic's max-producers cap (fall back to namespace / broker default).
    ///
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/maxProducers`.
    /// Java: `PersistentTopicsBase#removeMaxProducers`.
    pub async fn topic_remove_max_producers(&self, topic: &str) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "maxProducers"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    /// Get a topic's max-consumers cap (or `null` if no override).
    ///
    /// `GET /admin/v2/persistent/{tenant}/{ns}/{topic}/maxConsumers`. Returns
    /// a bare integer when the override is set, `null` (decoded as
    /// `Option::None`) when no topic-level cap is in place.
    /// Java: `PersistentTopicsBase#getMaxConsumers`.
    pub async fn topic_get_max_consumers(&self, topic: &str) -> Result<Option<i32>, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "maxConsumers"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Set a topic's max-consumers cap.
    ///
    /// `POST /admin/v2/persistent/{tenant}/{ns}/{topic}/maxConsumers` with
    /// a bare integer body. `0` disables (broker treats as unlimited).
    /// Java: `PersistentTopicsBase#setMaxConsumers`.
    pub async fn topic_set_max_consumers(
        &self,
        topic: &str,
        max_consumers: i32,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "maxConsumers"])?;
        let resp = self
            .send(self.http.request(Method::POST, url).json(&max_consumers))
            .await?;
        empty_ok(resp).await
    }

    /// Remove a topic's max-consumers cap (fall back to namespace / broker default).
    ///
    /// `DELETE /admin/v2/persistent/{tenant}/{ns}/{topic}/maxConsumers`.
    /// Java: `PersistentTopicsBase#removeMaxConsumers`.
    pub async fn topic_remove_max_consumers(&self, topic: &str) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "maxConsumers"])?;
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
    }

    // --- Subscriptions ---------------------------------------------------

    /// List subscription names on a topic.
    ///
    /// `GET /admin/v2/persistent/{tenant}/{namespace}/{topic}/subscriptions`.
    /// Java: `PersistentTopics#getSubscriptions`.
    pub async fn subscriptions_list(&self, topic: &str) -> Result<Vec<String>, AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&["persistent", tenant, namespace, name, "subscriptions"])?;
        let resp = self.send(self.http.request(Method::GET, url)).await?;
        json_ok(resp).await
    }

    /// Reset a subscription's cursor to a specific message-id position.
    ///
    /// `POST /admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{sub}/resetcursor`
    /// with body `{ledgerId, entryId, partitionIndex, batchIndex, isExcluded}`.
    /// `is_excluded = true` skips the message at `message_id` itself; `false`
    /// leaves it eligible for redelivery. Java: `PersistentTopics#resetCursorOnPosition`.
    pub async fn subscription_reset_cursor_to_position(
        &self,
        topic: &str,
        subscription: &str,
        message_id: MessageId,
        is_excluded: bool,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&[
            "persistent",
            tenant,
            namespace,
            name,
            "subscription",
            subscription,
            "resetcursor",
        ])?;
        let body = ResetCursorData {
            ledger_id: message_id.ledger_id,
            entry_id: message_id.entry_id,
            partition_index: message_id.partition,
            batch_index: message_id.batch_index,
            is_excluded,
        };
        let resp = self
            .send(self.http.request(Method::POST, url).json(&body))
            .await?;
        empty_ok(resp).await
    }

    /// Reset a subscription's cursor to a wall-clock timestamp (millis since epoch).
    ///
    /// `POST /admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{sub}/resetcursor/
    /// {timestamp}`. Java: `PersistentTopics#resetCursor(topic, sub, timestamp)`.
    pub async fn subscription_reset_cursor_to_timestamp(
        &self,
        topic: &str,
        subscription: &str,
        timestamp_millis: u64,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let timestamp = timestamp_millis.to_string();
        let url = self.url(&[
            "persistent",
            tenant,
            namespace,
            name,
            "subscription",
            subscription,
            "resetcursor",
            &timestamp,
        ])?;
        let resp = self.send(self.http.request(Method::POST, url)).await?;
        empty_ok(resp).await
    }

    /// Advance a subscription's cursor past N undelivered messages.
    ///
    /// `POST /admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{sub}/skip/
    /// {numMessages}`. Java: `PersistentTopics#skipMessages`.
    pub async fn subscription_skip_messages(
        &self,
        topic: &str,
        subscription: &str,
        num_messages: u64,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let n = num_messages.to_string();
        let url = self.url(&[
            "persistent",
            tenant,
            namespace,
            name,
            "subscription",
            subscription,
            "skip",
            &n,
        ])?;
        let resp = self.send(self.http.request(Method::POST, url)).await?;
        empty_ok(resp).await
    }

    /// Drain the entire backlog of a subscription (clear-backlog).
    ///
    /// `POST /admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{sub}/skip_all`.
    /// Java: `PersistentTopics#skipAllMessages`.
    pub async fn subscription_skip_all_messages(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let url = self.url(&[
            "persistent",
            tenant,
            namespace,
            name,
            "subscription",
            subscription,
            "skip_all",
        ])?;
        let resp = self.send(self.http.request(Method::POST, url)).await?;
        empty_ok(resp).await
    }

    /// Expire all messages older than `expire_time_seconds` for a subscription.
    ///
    /// `POST /admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{sub}/expireMessages/
    /// {seconds}`. Java: `PersistentTopics#expireMessagesForSubscription`.
    pub async fn subscription_expire_messages(
        &self,
        topic: &str,
        subscription: &str,
        expire_time_seconds: u64,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let s = expire_time_seconds.to_string();
        let url = self.url(&[
            "persistent",
            tenant,
            namespace,
            name,
            "subscription",
            subscription,
            "expireMessages",
            &s,
        ])?;
        let resp = self.send(self.http.request(Method::POST, url)).await?;
        empty_ok(resp).await
    }

    /// Delete (unsubscribe) a subscription.
    ///
    /// `DELETE /admin/v2/persistent/{tenant}/{namespace}/{topic}/subscription/{sub}?force={force}`.
    /// `force = true` disconnects active consumers before deletion. Java:
    /// `PersistentTopics#deleteSubscription`.
    pub async fn subscription_delete(
        &self,
        topic: &str,
        subscription: &str,
        force: bool,
    ) -> Result<(), AdminError> {
        let (tenant, namespace, name) = split_topic(topic)?;
        let mut url = self.url(&[
            "persistent",
            tenant,
            namespace,
            name,
            "subscription",
            subscription,
        ])?;
        if force {
            url.query_pairs_mut().append_pair("force", "true");
        }
        let resp = self.send(self.http.request(Method::DELETE, url)).await?;
        empty_ok(resp).await
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

/// Partitioned-topic metadata, as returned by
/// `GET /admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions`.
/// Java: `org.apache.pulsar.common.partition.PartitionedTopicMetadata`.
/// Only the partition count is consumed; broker-side extensions are ignored.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct PartitionedTopicMetadata {
    partitions: u32,
}

/// Java `RetentionPolicies` — namespace-level retention policy. `-1` for
/// either dimension means infinite. The broker applies whichever quota
/// becomes binding first (time OR size).
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RetentionPolicies {
    /// Maximum retention time in minutes. `-1` = infinite, `0` = none.
    pub retention_time_in_minutes: i32,
    /// Maximum retention size in megabytes. `-1` = infinite, `0` = none.
    #[serde(rename = "retentionSizeInMB")]
    pub retention_size_in_mb: i64,
}

/// Java `PersistencePolicies` — namespace-level BookKeeper layout +
/// managed-ledger write-shaping knobs. Maps to the broker's
/// `org.apache.pulsar.common.policies.data.PersistencePolicies`. Use
/// `Default` (`0/0/0/0.0`) only for "unset" semantics — the broker
/// rejects ensemble values < 1 on `set`.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PersistencePolicies {
    /// BookKeeper ensemble size — the number of bookies the managed
    /// ledger striping is spread across.
    pub bookkeeper_ensemble: i32,
    /// BookKeeper write quorum — the number of bookies each entry is
    /// written to.
    pub bookkeeper_write_quorum: i32,
    /// BookKeeper ack quorum — the number of acks required before an
    /// add is considered durable.
    pub bookkeeper_ack_quorum: i32,
    /// Managed-ledger mark-delete-rate cap (ops/sec). `0.0` disables
    /// the throttle.
    pub managed_ledger_max_mark_delete_rate: f64,
}

/// Java `DispatchRate` — a sliding-window throttle (msg/sec + byte/sec
/// over a `ratePeriodInSecond` window). Shared shape between the
/// per-namespace consumer dispatch rate, the per-subscription dispatch
/// rate, and the cross-cluster replicator dispatch rate. `-1` on either
/// dimension disables that axis of the throttle.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct DispatchRate {
    /// Throttle in messages/sec. `-1` = unlimited.
    pub dispatch_throttling_rate_in_msg: i32,
    /// Throttle in bytes/sec. `-1` = unlimited.
    pub dispatch_throttling_rate_in_byte: i64,
    /// Window size in seconds the throttle averages over.
    pub rate_period_in_second: i32,
    /// If `true`, dispatch rate is interpreted as an addend on top of
    /// the namespace publish rate rather than an absolute cap.
    pub relative_to_publish_rate: bool,
}

/// Java `PublishRate` — producer-side throttle (msg/sec + byte/sec).
/// `-1` on either dimension disables that axis of the throttle. Unlike
/// `DispatchRate`, there is no rate-period field — the broker uses a
/// fixed 1-second window.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PublishRate {
    /// Throttle in messages/sec. `-1` = unlimited.
    pub publish_throttling_rate_in_msg: i32,
    /// Throttle in bytes/sec. `-1` = unlimited.
    pub publish_throttling_rate_in_byte: i64,
}

/// Java `DelayedDeliveryPolicies` — namespace-level switch + index-tick
/// granularity for the broker's delayed-message delivery tracker.
/// Maps to `org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies`.
/// `tick_time_millis` controls how often the broker's delay-index buckets
/// are re-evaluated; smaller values give tighter delivery accuracy at a
/// higher tracker cost.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct DelayedDeliveryPolicies {
    /// Whether delayed delivery is enabled for the namespace.
    pub active: bool,
    /// Index-tick granularity in milliseconds.
    pub tick_time_millis: i64,
}

/// Java `BacklogQuota` — one entry in the namespace-level backlog quota
/// map. `policy` is a string (`producer_request_hold`,
/// `producer_exception`, `consumer_backlog_eviction`) rather than a
/// closed Rust enum so new broker enum values forward-decode cleanly.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct BacklogQuota {
    /// Maximum allowed backlog in bytes (when type=`destination_storage`).
    /// `-1` = unlimited.
    pub limit_size: i64,
    /// Maximum allowed backlog age in seconds (when type=`message_age`).
    /// `-1` = unlimited.
    pub limit_time: i32,
    /// Action when the quota is exceeded.
    pub policy: String,
}

/// Java `BookieInfo` — a single bookie's group + rack assignment, as
/// stored in the `racks-info` metadata path and shipped on
/// [`AdminClient::bookies_set_rack`]. Field names are camelCase on the
/// wire (matching `org.apache.pulsar.common.policies.data.BookieInfo`).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct BookieInfo {
    /// Rack-aware placement group. Defaults to `"default"` when unset
    /// in older brokers; modern brokers expose every group explicitly.
    pub group: String,
    /// Rack identifier within the group — opaque to the broker, only
    /// the placement policy cares about it.
    pub rack: String,
    /// Resolved hostname for the bookie. The broker uses it for
    /// log lines; it does not have to match DNS.
    pub hostname: String,
}

/// Java `PostSchemaPayload` — the request body for
/// [`AdminClient::schema_post`] and
/// [`AdminClient::schema_compatibility_check`]. The Java DTO has
/// (`type`, `schema`, `properties`); both keys travel as-is on the wire.
/// `schema` is the canonical-form blob for AVRO / JSON / PROTOBUF and
/// the protobuf descriptor for `PROTOBUF_NATIVE`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct PostSchemaPayload {
    /// Schema type (`AVRO` / `JSON` / `PROTOBUF` / `PROTOBUF_NATIVE` /
    /// `KEY_VALUE` / `STRING` / `BYTES` / ...).
    #[serde(rename = "type")]
    pub schema_type: String,
    /// Schema definition, encoded per the type axis.
    pub schema: String,
    /// User-defined per-schema properties.
    pub properties: std::collections::HashMap<String, String>,
}

/// Java `BacklogQuotaType` — selects which dimension a `BacklogQuota`
/// entry limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacklogQuotaType {
    /// Bytes-on-disk dimension. Uses `BacklogQuota::limit_size`.
    DestinationStorage,
    /// Message-age dimension. Uses `BacklogQuota::limit_time`.
    MessageAge,
}

impl BacklogQuotaType {
    /// Render as the lowercase snake_case value the broker REST surface
    /// expects in the `backlogQuotaType` query parameter.
    #[must_use]
    pub fn as_query_value(self) -> &'static str {
        match self {
            Self::DestinationStorage => "destination_storage",
            Self::MessageAge => "message_age",
        }
    }
}

/// Java `LongRunningProcessStatus` — the polling shape for triggered
/// background jobs (compaction, offload). The broker returns one of four
/// `status` values: `NOT_RUN` (never triggered), `RUNNING`, `SUCCESS`,
/// `ERROR`. `last_error` is populated only on `ERROR`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct LongRunningProcessStatus {
    /// Job state — `NOT_RUN`, `RUNNING`, `SUCCESS`, or `ERROR`.
    pub status: String,
    /// Human-readable error message, present on `ERROR`.
    pub last_error: String,
}

/// Request body for `POST .../subscription/{sub}/resetcursor` (Java
/// `ResetCursorData`). The CLI exposes `message_id` and `is_excluded`;
/// Pulsar's `batchIndexes` / `properties` fields are not currently set —
/// they exist for transactional dedup metadata and would require
/// txn-aware callers anyway.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ResetCursorData {
    ledger_id: u64,
    entry_id: u64,
    partition_index: i32,
    batch_index: i32,
    #[serde(rename = "isExcluded")]
    is_excluded: bool,
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

        // reqwest 0.13 panics in `Client::builder().build()` when the active
        // `rustls` flavor is `rustls-no-provider` and no global
        // `CryptoProvider` is installed. That happens whenever more than one
        // `crypto-*` feature is unified (e.g. default `crypto-aws-lc-rs`
        // plus an explicit `crypto-ring`), so install the default here —
        // the shim is idempotent and a no-op once a provider is set, which
        // covers parallel callers and processes that also boot the tokio
        // engine.
        tls_crypto::install_default_provider();

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
