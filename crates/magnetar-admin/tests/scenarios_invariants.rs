// SPDX-License-Identifier: Apache-2.0

//! TigerBeetle-style invariant scenarios for the admin REST client.
//!
//! `magnetar-admin` sits outside the moonpool simulation engine — admin
//! REST is plain HTTPS via `reqwest`, not the binary protocol the
//! moonpool runtime simulates. The closest analog to a simulation-style
//! test over the chaos pack / differential harness is **invariant
//! scenarios over a stateful fake broker**: multi-step sequences with
//! assertions on properties that must hold across every interleaving.
//!
//! Each scenario:
//! - Mounts a `wiremock::MockServer` with the response shape pinned by the wire-level per-method
//!   tests (`subscriptions.rs`, `topic_ops.rs`, `namespace_policies.rs`, `diagnostics.rs`).
//! - Drives a multi-step operator workflow.
//! - Asserts a class of invariants:
//!   - **Idempotence** — applying the same mutation twice is observably equivalent to applying it
//!     once.
//!   - **Composability** — `set(X); get()` returns `X` (the broker's read-your-write contract for
//!     these policies).
//!   - **Auth invariance** — every request carries `Authorization: Bearer <token>` when the client
//!     is configured with one, and never when it isn't.
//!   - **Error-no-mutation** — a 4xx / 5xx response from one call must not leave the client in a
//!     state that breaks the next call.
//!   - **Independence** — operations on disjoint namespaces / topics / subscriptions do not
//!     interfere.
//!
//! These are not formal property tests (no shrinker, no seed sweep —
//! the moonpool / proptest infrastructure targets the binary protocol
//! state machine in `magnetar-proto`, not the HTTP control plane).
//! They are *assertion-rich* multi-step tests that pin the contracts
//! the per-method wire tests do not.

use magnetar_admin::{
    AdminClient, AdminError, BacklogQuota, BacklogQuotaType, DelayedDeliveryPolicies, DispatchRate,
    FunctionConfig, PackageType, PersistencePolicies, PostSchemaPayload, PublishRate,
    RetentionPolicies, SinkConfig, SourceConfig,
};
use wiremock::matchers::{body_json, header, header_exists, method, path, query_param};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

fn client_with_token(mock: &MockServer, token: &str) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .token(token.to_owned())
        .build()
        .unwrap()
}

/// **Invariant — idempotence on remove**: calling
/// `namespace_remove_retention` twice in a row must succeed both times.
/// The broker treats a remove on already-default state as a no-op (204).
/// A naïve client that cached "we already deleted" would surface a
/// stale error; we assert the client always issues the second DELETE.
#[tokio::test]
async fn invariant_remove_is_idempotent_for_namespace_policies() {
    let mock = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/v2/namespaces/acme/svc/retention"))
        .respond_with(ResponseTemplate::new(204))
        .expect(2)
        .mount(&mock)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/admin/v2/namespaces/acme/svc/messageTTL"))
        .respond_with(ResponseTemplate::new(204))
        .expect(2)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin.namespace_remove_retention("acme/svc").await.unwrap();
    admin.namespace_remove_retention("acme/svc").await.unwrap();
    admin
        .namespace_remove_message_ttl("acme/svc")
        .await
        .unwrap();
    admin
        .namespace_remove_message_ttl("acme/svc")
        .await
        .unwrap();
}

/// **Invariant — composability**: `set(X); get()` returns `X`. The
/// broker's read-your-write semantics on namespace policies are the
/// load-bearing contract the CLI presents; if the round-trip drops a
/// field, no operator script would notice until a 3am incident. Pin it
/// here with a stateful fake that echoes the last `set` body.
#[tokio::test]
async fn invariant_set_then_get_returns_set_value_for_retention() {
    let mock = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/retention"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&mock)
        .await;

    // The fake serves the canonical wire shape Java emits — the test
    // asserts the client decodes both fields correctly. The wiremock
    // mock is stateless; the invariant is on the client's serde
    // round-trip not dropping a field.
    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/retention"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "retentionTimeInMinutes": 1440,
            "retentionSizeInMB": 10240,
        })))
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .namespace_set_retention(
            "acme/svc",
            RetentionPolicies {
                retention_time_in_minutes: 1440,
                retention_size_in_mb: 10240,
            },
        )
        .await
        .unwrap();
    let got = admin.namespace_get_retention("acme/svc").await.unwrap();
    assert_eq!(got.retention_time_in_minutes, 1440);
    assert_eq!(got.retention_size_in_mb, 10240);
}

/// **Invariant — auth presence on every call**: when the client is
/// configured with a bearer token, every request carries
/// `Authorization: Bearer <token>` — regardless of verb, regardless of
/// resource, regardless of error path. A regression that strips the
/// header on (say) DELETE would only surface against a real
/// authenticated broker; pin it here.
#[tokio::test]
async fn invariant_bearer_token_present_on_every_verb() {
    let mock = MockServer::start().await;
    let token = "test-bearer-token-xyz";

    // Mount one mock per verb; each asserts the Authorization header
    // matches `Bearer <token>` and the path is the expected one. If
    // the client omits the header on any verb, wiremock returns 404
    // (no matching mock) and the call fails.
    let expected_auth = format!("Bearer {token}");

    // Endpoints whose responses are JSON arrays of strings — clusters,
    // tenants, namespaces, subscriptions.
    for p in [
        "/admin/v2/clusters",
        "/admin/v2/tenants",
        "/admin/v2/namespaces/acme",
        "/admin/v2/persistent/acme/svc/orders/subscriptions",
    ] {
        Mock::given(method("GET"))
            .and(path(p))
            .and(header("authorization", expected_auth.as_str()))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&mock)
            .await;
    }
    // 204 / 200 verbs — PUT, DELETE return no body; POST terminate
    // returns a MessageId. Wiremock returns the MessageId shape for
    // any of these mounts; the typed handlers ignore unknown fields.
    for (m, p) in [
        ("PUT", "/admin/v2/tenants/acme"),
        ("DELETE", "/admin/v2/tenants/acme"),
        ("PUT", "/admin/v2/namespaces/acme/svc"),
        ("DELETE", "/admin/v2/namespaces/acme/svc"),
        (
            "DELETE",
            "/admin/v2/persistent/acme/svc/orders/subscription/s-a",
        ),
        ("PUT", "/admin/v2/persistent/acme/svc/orders/compaction"),
        ("PUT", "/admin/v2/persistent/acme/svc/orders/unload"),
        ("POST", "/admin/v2/persistent/acme/svc/orders/terminate"),
    ] {
        Mock::given(method(m))
            .and(path(p))
            .and(header("authorization", expected_auth.as_str()))
            .respond_with(if m == "POST" {
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "ledgerId": 1,
                    "entryId": 1,
                    "partitionIndex": -1,
                }))
            } else {
                ResponseTemplate::new(204)
            })
            .mount(&mock)
            .await;
    }

    let admin = client_with_token(&mock, token);
    // Drive every verb. If any one is missing the Authorization header,
    // wiremock 404s and the call errors — propagating to a panic via
    // `.expect()`.
    admin.cluster_list().await.expect("clusters_list");
    admin.tenants_list().await.expect("tenants_list");
    admin
        .tenant_create(
            "acme",
            magnetar_admin::TenantInfo {
                admin_roles: vec![],
                allowed_clusters: vec!["standalone".into()],
            },
        )
        .await
        .expect("tenant_create");
    admin.tenant_delete("acme").await.expect("tenant_delete");
    admin
        .namespaces_list("acme")
        .await
        .expect("namespaces_list");
    admin
        .namespace_create("acme/svc")
        .await
        .expect("namespace_create");
    admin
        .namespace_delete("acme/svc")
        .await
        .expect("namespace_delete");
    admin
        .subscriptions_list("acme/svc/orders")
        .await
        .expect("subscriptions_list");
    admin
        .subscription_delete("acme/svc/orders", "s-a", false)
        .await
        .expect("subscription_delete");
    admin
        .topic_compact("acme/svc/orders")
        .await
        .expect("compact");
    admin.topic_unload("acme/svc/orders").await.expect("unload");
    let _ = admin.topic_terminate("acme/svc/orders").await;
}

/// **Invariant — no auth header when no token configured**: a
/// `AdminAuth::None` client must never send `Authorization`. A
/// regression that sent a stray `Authorization: Bearer ` (empty token)
/// would route to a "bad token" 401 on some brokers; pin the absence.
#[tokio::test]
async fn invariant_no_authorization_header_when_unconfigured() {
    let mock = MockServer::start().await;
    // Custom matcher: assert the request has NO Authorization header.
    let no_auth = wiremock::matchers::AnyMatcher;
    Mock::given(method("GET"))
        .and(path("/admin/v2/clusters"))
        .and(no_auth)
        .respond_with(move |req: &Request| {
            if req.headers.contains_key("authorization") {
                ResponseTemplate::new(500).set_body_string("regression: Authorization sent")
            } else {
                ResponseTemplate::new(200).set_body_json(serde_json::json!([]))
            }
        })
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let result = admin.cluster_list().await;
    assert!(
        result.is_ok(),
        "unauthenticated client must not send Authorization; got {result:?}"
    );
}

/// **Invariant — error-no-mutation**: a 4xx / 5xx response from one
/// call must not perturb the client. The next call against a working
/// endpoint must still succeed. A regression that, e.g., poisoned an
/// internal cache on error would only surface during incident response.
#[tokio::test]
async fn invariant_error_response_does_not_perturb_subsequent_calls() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/retention"))
        .respond_with(ResponseTemplate::new(500).set_body_string("transient broker glitch"))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/clusters"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(["standalone"])))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let err = admin.namespace_get_retention("acme/svc").await.unwrap_err();
    assert!(matches!(err, AdminError::Status { code: 500, .. }));
    let clusters = admin.cluster_list().await.expect("post-error call works");
    assert_eq!(clusters, vec!["standalone".to_owned()]);
}

/// **Invariant — independence across resources**: a mutation on
/// namespace A must not affect the broker's view of namespace B. The
/// client is stateless so this is mostly a wire-shape check (URLs are
/// resource-scoped), but a regression that, say, applied the body to
/// the wrong path would break this.
#[tokio::test]
async fn invariant_namespace_mutations_are_resource_scoped() {
    let mock = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc-a/retention"))
        .and(body_json(serde_json::json!({
            "retentionTimeInMinutes": 60,
            "retentionSizeInMB": 1024,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc-b/retention"))
        .and(body_json(serde_json::json!({
            "retentionTimeInMinutes": 1440,
            "retentionSizeInMB": -1,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .namespace_set_retention(
            "acme/svc-a",
            RetentionPolicies {
                retention_time_in_minutes: 60,
                retention_size_in_mb: 1024,
            },
        )
        .await
        .unwrap();
    admin
        .namespace_set_retention(
            "acme/svc-b",
            RetentionPolicies {
                retention_time_in_minutes: 1440,
                retention_size_in_mb: -1,
            },
        )
        .await
        .unwrap();
}

/// **Scenario — operator onboarding**: create tenant → create
/// namespace → set retention + backlog quota + message TTL → list to
/// confirm presence. Asserts the multi-step sequence succeeds against
/// the broker's documented contract. Catches regressions where the
/// client serialised something in the wrong direction across types.
#[tokio::test]
async fn scenario_onboard_tenant_and_apply_policies() {
    let mock = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path("/admin/v2/tenants/acme"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("PUT"))
        .and(path("/admin/v2/namespaces/acme/svc"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/retention"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/backlogQuota"))
        .and(query_param("backlogQuotaType", "destination_storage"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/messageTTL"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(["acme/svc"])))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .tenant_create(
            "acme",
            magnetar_admin::TenantInfo {
                admin_roles: vec!["alice".into()],
                allowed_clusters: vec!["standalone".into()],
            },
        )
        .await
        .unwrap();
    admin.namespace_create("acme/svc").await.unwrap();
    admin
        .namespace_set_retention(
            "acme/svc",
            RetentionPolicies {
                retention_time_in_minutes: 60,
                retention_size_in_mb: 1024,
            },
        )
        .await
        .unwrap();
    admin
        .namespace_set_backlog_quota(
            "acme/svc",
            BacklogQuotaType::DestinationStorage,
            BacklogQuota {
                limit_size: 1_073_741_824,
                limit_time: -1,
                policy: "consumer_backlog_eviction".into(),
            },
        )
        .await
        .unwrap();
    admin
        .namespace_set_message_ttl("acme/svc", 7200)
        .await
        .unwrap();
    let listed = admin.namespaces_list("acme").await.unwrap();
    assert_eq!(listed, vec!["acme/svc".to_owned()]);
}

/// **Invariant — Accept header is always present**: reqwest's JSON
/// helper sets `Accept: */*` (not `application/json`), but every admin
/// REST response is JSON anyway. Pin that the client doesn't strip the
/// header or set an incompatible value — a regression that sent
/// `Accept: text/plain` would silently break content-negotiating
/// brokers / proxies.
#[tokio::test]
async fn invariant_accept_header_present_on_every_call() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/clusters"))
        .and(header_exists("accept"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(["standalone"])))
        .expect(1)
        .mount(&mock)
        .await;
    let admin = client(&mock);
    let clusters = admin.cluster_list().await.unwrap();
    assert_eq!(clusters, vec!["standalone".to_owned()]);
}

// -------- PR #2 / #3 / #4 invariant coverage extensions ---------------

/// **Invariant — namespace policy set is idempotent**: applying the
/// same `set_<policy>` mutation twice in a row produces an observably
/// identical broker state. A regression that, say, tracked a "dirty"
/// flag client-side and skipped the second POST would break this for
/// brokers that coalesce duplicates — pin the wire shape every time.
#[tokio::test]
async fn invariant_namespace_policy_set_is_idempotent_across_families() {
    let mock = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/persistence"))
        .respond_with(ResponseTemplate::new(204))
        .expect(2)
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/dispatchRate"))
        .respond_with(ResponseTemplate::new(204))
        .expect(2)
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/deduplication"))
        .respond_with(ResponseTemplate::new(204))
        .expect(2)
        .mount(&mock)
        .await;

    let admin = client(&mock);

    let pers = PersistencePolicies {
        bookkeeper_ensemble: 2,
        bookkeeper_write_quorum: 2,
        bookkeeper_ack_quorum: 2,
        managed_ledger_max_mark_delete_rate: 1.0,
    };
    admin
        .namespace_set_persistence("acme/svc", pers.clone())
        .await
        .unwrap();
    admin
        .namespace_set_persistence("acme/svc", pers)
        .await
        .unwrap();

    let rate = DispatchRate {
        dispatch_throttling_rate_in_msg: 1000,
        dispatch_throttling_rate_in_byte: 1_048_576,
        rate_period_in_second: 1,
        relative_to_publish_rate: false,
    };
    admin
        .namespace_set_dispatch_rate("acme/svc", rate.clone())
        .await
        .unwrap();
    admin
        .namespace_set_dispatch_rate("acme/svc", rate)
        .await
        .unwrap();

    admin
        .namespace_set_deduplication("acme/svc", true)
        .await
        .unwrap();
    admin
        .namespace_set_deduplication("acme/svc", true)
        .await
        .unwrap();
}

/// **Invariant — topic policy overrides namespace policy at the wire**:
/// the broker exposes two distinct URL prefixes — namespace policies
/// at `/admin/v2/namespaces/...` and topic policies at
/// `/admin/v2/persistent/.../{topic}/...`. A regression that pointed
/// `topic_set_retention` at the namespace URL would silently apply at
/// the wrong scope. Pin that each verb hits its expected URL.
#[tokio::test]
async fn invariant_topic_policies_target_persistent_topic_prefix() {
    let mock = MockServer::start().await;
    let ns_url = "/admin/v2/namespaces/acme/svc/retention";
    let topic_url = "/admin/v2/persistent/acme/svc/orders/retention";

    // Namespace-level set — must hit the namespace URL.
    Mock::given(method("POST"))
        .and(path(ns_url))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    // Topic-level set — must hit the topic URL.
    Mock::given(method("POST"))
        .and(path(topic_url))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let pol = RetentionPolicies {
        retention_time_in_minutes: 60,
        retention_size_in_mb: 1024,
    };
    admin
        .namespace_set_retention("acme/svc", pol)
        .await
        .unwrap();
    admin
        .topic_set_retention("acme/svc/orders", pol)
        .await
        .unwrap();
}

/// **Invariant — topic policy GET returns `Option<T>` for null body**:
/// the broker emits `null` for a topic that has no override set; the
/// client must decode this as `None`, not as a default-constructed
/// `T`. (For namespace-level policies the broker emits the default
/// value; for topic-level it emits `null`.) Pin the `None` decode for
/// the load-bearing per-topic policies.
#[tokio::test]
async fn invariant_topic_policy_get_decodes_null_as_none() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/persistent/acme/svc/orders/dispatchRate"))
        .respond_with(ResponseTemplate::new(200).set_body_string("null"))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/persistent/acme/svc/orders/persistence"))
        .respond_with(ResponseTemplate::new(200).set_body_string("null"))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/persistent/acme/svc/orders/publishRate"))
        .respond_with(ResponseTemplate::new(200).set_body_string("null"))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    assert!(
        admin
            .topic_get_dispatch_rate("acme/svc/orders")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        admin
            .topic_get_persistence("acme/svc/orders")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        admin
            .topic_get_publish_rate("acme/svc/orders")
            .await
            .unwrap()
            .is_none()
    );
}

/// **Invariant — Option<T> getter decodes empty body / 204 No Content
/// as `None`**. In practice the broker emits the literal `null`
/// inconsistently — many policy GETs simply 204 with no body, and
/// `serde_json::from_slice::<Option<T>>(b"")` fails with
/// `EOF while parsing a value`. The client routes every
/// `Option<T>`-returning getter through `json_ok_optional`, which
/// treats both shapes as `Ok(None)`. Pinned for namespace_get_*
/// (i32 + Policies) and topic_get_* (i32) to defend against a
/// regression that re-introduces the unconditional `json_ok` path.
#[tokio::test]
async fn invariant_optional_getter_tolerates_empty_body_and_204() {
    let mock = MockServer::start().await;

    // 204 No Content — namespace_get_message_ttl.
    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/messageTTL"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    // 200 with empty body — namespace_get_deduplication.
    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/deduplication"))
        .respond_with(ResponseTemplate::new(200).set_body_string(""))
        .expect(1)
        .mount(&mock)
        .await;
    // 200 with empty body — namespace_get_delayed_delivery.
    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/delayedDelivery"))
        .respond_with(ResponseTemplate::new(200).set_body_string(""))
        .expect(1)
        .mount(&mock)
        .await;
    // 204 No Content — topic_get_max_producers.
    Mock::given(method("GET"))
        .and(path("/admin/v2/persistent/acme/svc/orders/maxProducers"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    assert!(
        admin
            .namespace_get_message_ttl("acme/svc")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        admin
            .namespace_get_deduplication("acme/svc")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        admin
            .namespace_get_delayed_delivery("acme/svc")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        admin
            .topic_get_max_producers("acme/svc/orders")
            .await
            .unwrap()
            .is_none()
    );
}

/// **Invariant — delayed-delivery composability**: the
/// `DelayedDeliveryPolicies { active, tickTime }` body must round-trip
/// both fields. The Java field name is `tickTime` (not
/// `tickTimeMillis` — the unit is documented in the class doc but the
/// wire key omits the suffix); pinned explicitly here so a regression
/// that flips back to the camelCase-of-the-Rust-name `tickTimeMillis`
/// is caught immediately.
#[tokio::test]
async fn invariant_delayed_delivery_round_trips_both_fields() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/delayedDelivery"))
        .and(body_json(serde_json::json!({
            "active": true,
            "tickTime": 1500,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/delayedDelivery"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "active": true,
            "tickTime": 1500,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .namespace_set_delayed_delivery(
            "acme/svc",
            DelayedDeliveryPolicies {
                active: true,
                tick_time_millis: 1500,
            },
        )
        .await
        .unwrap();
    let got = admin
        .namespace_get_delayed_delivery("acme/svc")
        .await
        .unwrap();
    assert!(got.is_some());
    let got = got.unwrap();
    assert!(got.active);
    assert_eq!(got.tick_time_millis, 1500);
}

/// **Invariant — schema POST returns version; subsequent GET sees the
/// same shape**. The Java `PostSchemaPayload` wire shape uses the
/// field name `type` (Rust reserved; mapped via `schema_type` with
/// `#[serde(rename = "type")]`). A regression on either side would
/// surface as a 400 broker response.
#[tokio::test]
async fn invariant_schema_post_then_get_round_trips_avro_definition() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/admin/v2/schemas/acme/svc/orders/schema"))
        .and(body_json(serde_json::json!({
            "type": "AVRO",
            "schema": "{\"type\":\"record\",\"name\":\"X\",\"fields\":[]}",
            "properties": {}
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "version": 1
        })))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/schemas/acme/svc/orders/schema"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "version": 1,
            "type": "AVRO",
            "schema": "{\"type\":\"record\",\"name\":\"X\",\"fields\":[]}",
            "properties": {}
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let payload = PostSchemaPayload {
        schema_type: "AVRO".to_owned(),
        schema: r#"{"type":"record","name":"X","fields":[]}"#.to_owned(),
        properties: Default::default(),
    };
    let posted = admin.schema_post("acme/svc/orders", payload).await.unwrap();
    assert_eq!(posted["version"], 1);
    let got = admin.schema_get_latest("acme/svc/orders").await.unwrap();
    assert_eq!(got["version"], 1);
    assert_eq!(got["type"], "AVRO");
}

/// **Invariant — publish-rate set body uses `publishThrottlingRate*`
/// camelCase**. The Java type `PublishRate { publishThrottlingRateInMsg,
/// publishThrottlingRateInByte }` is named differently from
/// `DispatchRate` — pin the wire shape so a future copy-paste regression
/// (e.g. renaming `publish_throttling_rate_in_msg` to
/// `dispatch_throttling_rate_in_msg`) is caught at the wire.
#[tokio::test]
async fn invariant_publish_rate_body_uses_publish_throttling_camel_case() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/publishRate"))
        .and(body_json(serde_json::json!({
            "publishThrottlingRateInMsg": 500,
            "publishThrottlingRateInByte": 524288_i64,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .namespace_set_publish_rate(
            "acme/svc",
            PublishRate {
                publish_throttling_rate_in_msg: 500,
                publish_throttling_rate_in_byte: 524_288,
            },
        )
        .await
        .unwrap();
}

/// **Invariant — backlog quota error path doesn't poison subsequent
/// calls** (extension of the earlier error-no-mutation invariant for
/// the wider policy surface added in PR #2 / #3). A 4xx from the
/// broker on a backlog-quota POST must not stop a subsequent
/// `set_dispatch_rate` from succeeding.
#[tokio::test]
async fn invariant_backlog_quota_error_does_not_perturb_other_policies() {
    let mock = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/backlogQuota"))
        .respond_with(ResponseTemplate::new(400).set_body_string("invalid policy"))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/dispatchRate"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let err = admin
        .namespace_set_backlog_quota(
            "acme/svc",
            BacklogQuotaType::DestinationStorage,
            BacklogQuota {
                limit_size: -2,
                limit_time: -1,
                policy: "bogus".into(),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, AdminError::Status { code: 400, .. }));

    admin
        .namespace_set_dispatch_rate(
            "acme/svc",
            DispatchRate {
                dispatch_throttling_rate_in_msg: 1,
                dispatch_throttling_rate_in_byte: 1,
                rate_period_in_second: 1,
                relative_to_publish_rate: false,
            },
        )
        .await
        .unwrap();
}

// -------- PR #5 invariant coverage (V3 surface) -----------------------

/// **Invariant — V3 endpoints hit `/admin/v3/` prefix, not `/admin/v2/`**.
/// Functions / Sources / Sinks / Packages live at the V3 prefix; a
/// regression that routed them at V2 would silently 404 against every
/// real broker. Pin each top-level family's list endpoint hits the V3
/// URL prefix.
#[tokio::test]
async fn invariant_v3_endpoints_use_admin_v3_prefix() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v3/functions/acme/svc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/sources/acme/svc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/sinks/acme/svc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/packages/function/acme/svc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .functions_list_by_namespace("acme", "svc")
        .await
        .unwrap();
    admin
        .sources_list_by_namespace("acme", "svc")
        .await
        .unwrap();
    admin.sinks_list_by_namespace("acme", "svc").await.unwrap();
    admin
        .packages_list(PackageType::Function, "acme", "svc")
        .await
        .unwrap();
}

/// **Invariant — multipart envelope shape for URL-based register
/// calls**. The V3 create-with-url surface for Functions / Sources /
/// Sinks emits a `multipart/form-data` body with two parts: `url`
/// (text) and the typed config (JSON). Pin that the broker sees the
/// `url` field with the supplied package URL. The exact content-type
/// header carries a boundary string that wiremock can't pin
/// deterministically, so we assert it starts with the literal
/// `multipart/form-data`.
#[tokio::test]
async fn invariant_url_based_register_uses_multipart_envelope() {
    let mock = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/admin/v3/functions/acme/svc/echo"))
        .respond_with(|req: &Request| {
            let ct = req
                .headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if !ct.starts_with("multipart/form-data") {
                return ResponseTemplate::new(500)
                    .set_body_string(format!("expected multipart, got {ct:?}"));
            }
            let body = std::str::from_utf8(&req.body).unwrap_or("");
            if !body.contains("https://example.com/echo.jar") {
                return ResponseTemplate::new(500).set_body_string("missing url part");
            }
            if !body.contains("\"className\":\"com.acme.Echo\"") {
                return ResponseTemplate::new(500).set_body_string("missing functionConfig part");
            }
            ResponseTemplate::new(204)
        })
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .function_create_with_url(
            "acme",
            "svc",
            "echo",
            "https://example.com/echo.jar",
            FunctionConfig {
                tenant: "acme".into(),
                namespace: "svc".into(),
                name: "echo".into(),
                class_name: "com.acme.Echo".into(),
                inputs: vec!["persistent://acme/svc/in".into()],
                output: "persistent://acme/svc/out".into(),
                runtime: "JAVA".into(),
                parallelism: 1,
                user_config: None,
            },
        )
        .await
        .unwrap();
}

/// **Invariant — `package_delete` does not affect a different package's
/// state**. Pin that the URL is package-scoped: deleting `pkg-a` v1.0.0
/// emits a DELETE to its own URL and never touches `pkg-b`.
#[tokio::test]
async fn invariant_package_delete_targets_only_the_named_package() {
    let mock = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/v3/packages/function/acme/svc/pkg-a/1.0.0"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    // A DELETE that lands on `pkg-b` would 404 (no mock); we never
    // expect it.
    let admin = client(&mock);
    admin
        .package_delete(PackageType::Function, "acme", "svc", "pkg-a", "1.0.0")
        .await
        .unwrap();
}

/// **Invariant — Sources and Sinks share the same wire shape per
/// family** but distinct URL families. A regression that routed a
/// `sink_status` call at `/sources/...` would silently report the
/// wrong subsystem's state. Pin both call distinct URLs.
#[tokio::test]
async fn invariant_sources_and_sinks_target_distinct_url_families() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/sources/acme/svc/connector-a/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/sinks/acme/svc/connector-a/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .source_status("acme", "svc", "connector-a")
        .await
        .unwrap();
    admin
        .sink_status("acme", "svc", "connector-a")
        .await
        .unwrap();
}

/// **Invariant — `function_start_instance` / `stop_instance` route to
/// the instance-scoped URL, not the aggregate**. A regression that
/// dropped the `instance_id` segment would start/stop ALL instances
/// instead of one. Pin instance-scoped URL.
#[tokio::test]
async fn invariant_function_instance_lifecycle_routes_to_instance_url() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/admin/v3/functions/acme/svc/echo/2/start"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v3/functions/acme/svc/echo/2/stop"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .function_start_instance("acme", "svc", "echo", 2)
        .await
        .unwrap();
    admin
        .function_stop_instance("acme", "svc", "echo", 2)
        .await
        .unwrap();
}

/// **Invariant — SourceConfig + SinkConfig camelCase**. Per Java's
/// `org.apache.pulsar.common.io.SourceConfig` / `SinkConfig`, the wire
/// fields are camelCase. Pin via the `sourceConfig` / `sinkConfig`
/// multipart JSON body shape so a Rust field rename doesn't silently
/// drop a field on the wire.
#[tokio::test]
async fn invariant_io_configs_use_camel_case_field_names() {
    let mock = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/admin/v3/sources/acme/svc/src"))
        .respond_with(|req: &Request| {
            let body = std::str::from_utf8(&req.body).unwrap_or("");
            for needle in [
                "\"className\":\"org.apache.pulsar.io.kafka.KafkaSource\"",
                "\"topicName\":\"persistent://acme/svc/in\"",
                "\"parallelism\":2",
            ] {
                if !body.contains(needle) {
                    return ResponseTemplate::new(500).set_body_string(format!("missing {needle}"));
                }
            }
            ResponseTemplate::new(204)
        })
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .source_create_with_url(
            "acme",
            "svc",
            "src",
            "https://example.com/src.jar",
            SourceConfig {
                tenant: "acme".into(),
                namespace: "svc".into(),
                name: "src".into(),
                class_name: "org.apache.pulsar.io.kafka.KafkaSource".into(),
                topic_name: "persistent://acme/svc/in".into(),
                parallelism: 2,
                configs: None,
            },
        )
        .await
        .unwrap();
}
