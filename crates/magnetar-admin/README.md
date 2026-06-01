# magnetar-admin

> **Status: pre-alpha (M9).** Surface stable enough to drive `magnetar-cli`, broader Java parity (schemas, functions, sinks, sources, proxy stats) lands later.

Async Apache Pulsar admin REST client (`/admin/v2/...`).
Backed by `reqwest` with the `rustls-tls` feature — no `native-tls`, no `openssl`.

## Surface

```rust,no_run
use magnetar_admin::{AdminClient, TenantInfo};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let admin = AdminClient::builder()
        .service_url("http://localhost:8080".parse()?)
        .token(std::env::var("MAGNETAR_TOKEN").unwrap_or_default())
        .build()?;

    let tenants = admin.tenants_list().await?;
    println!("tenants: {tenants:?}");

    admin
        .tenant_create(
            "acme",
            TenantInfo {
                admin_roles: vec!["admin".into()],
                allowed_clusters: vec!["standalone".into()],
            },
        )
        .await?;
    Ok(())
}
```

The base service URL is whatever the broker exposes (`http://localhost:8080`, `https://broker.example`); the client appends `/admin/v2/` itself.

## Endpoints implemented

| Method                     | Endpoint                                                                                        | Pulsar (Java) reference                                                                         |
| -------------------------- | ----------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `cluster_list`             | `GET    /admin/v2/clusters`                                                                     | `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/Clusters.java`                   |
| `tenants_list`             | `GET    /admin/v2/tenants`                                                                      | `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/Tenants.java`                    |
| `tenant_create`            | `PUT    /admin/v2/tenants/{tenant}`                                                             | `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/impl/TenantsBase.java#createTenant` |
| `tenant_delete`            | `DELETE /admin/v2/tenants/{tenant}`                                                             | `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/impl/TenantsBase.java#deleteTenant` |
| `namespaces_list`          | `GET    /admin/v2/namespaces/{tenant}`                                                          | `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/Namespaces.java`                 |
| `namespace_create`         | `PUT    /admin/v2/namespaces/{tenant}/{namespace}`                                              | `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/Namespaces.java`                 |
| `namespace_delete`         | `DELETE /admin/v2/namespaces/{tenant}/{namespace}`                                              | `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/Namespaces.java`                 |
| `topics_list`              | `GET    /admin/v2/persistent/{tenant}/{namespace}`                                              | `pulsar-broker/src/main/java/org/apache/pulsar/broker/admin/v2/PersistentTopics.java`           |
| `topic_create_partitioned` | `PUT    /admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions`                           | `PersistentTopics.java#createPartitionedTopic`                                                  |
| `topic_delete`             | `DELETE /admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions?force={bool}`              | `PersistentTopics.java#deletePartitionedTopic`                                                  |
| `topic_stats`              | `GET    /admin/v2/persistent/{tenant}/{namespace}/{topic}/stats`                                | `PersistentTopics.java#getStats`                                                                |
| `topic_partitioned_stats`  | `GET    /admin/v2/persistent/{tenant}/{namespace}/{topic}/partitioned-stats?perPartition=false` | `PersistentTopics.java#getPartitionedStats`                                                     |
| `topic_partitions_count`   | `GET    /admin/v2/persistent/{tenant}/{namespace}/{topic}/partitions`                           | `PersistentTopics.java#getPartitionedMetadata`                                                  |

`TopicStats` exposes the high-signal counters (`msgInCounter`, `bytesInCounter`) and passes `publishers` / `subscriptions` through as raw JSON because the Java schema is large and version-dependent.

`topic_partitioned_stats` reuses the same `TopicStats` shape (it carries the broker's aggregated top-level counters); the per-partition breakdown is intentionally dropped.
Call `topic_partitions_count` to size a topic and then either dispatch to `topic_stats` (non-partitioned) or `topic_partitioned_stats` (partitioned).
The `magnetar` CLI's `admin topic-stats` does this auto-dispatch.

## Auth

```rust,ignore
AdminClient::builder()
    .service_url(url)
    .token("eyJhbGciOi...".into())  // → Authorization: Bearer …
    .build()?;
```

OAuth2, SASL, and Athenz live in `magnetar-auth-*` crates; their integration with the admin client lands later.

## TLS

`reqwest` is built with `rustls-tls` (workspace-level).
`native-tls`, `openssl`, and `openssl-sys` are banned by `deny.toml`.

## Tests

```sh
cargo test -p magnetar-admin
```

Tests assert builder semantics, URL prefix handling, and the `TenantInfo` JSON layout.
Wire-level round-trips run in the e2e suite against `apachepulsar/pulsar:4.x`.
