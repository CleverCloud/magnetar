// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the namespace policy REST endpoints — retention,
//! backlog quota, message TTL.
//!
//! These pin the exact path, verb, query parameter, and JSON body shape
//! against `pulsar-broker/.../v2/Namespaces.java` and the policy verbs
//! in `NamespacesBase` (`getRetention`, `setRetention`, `removeRetention`,
//! `getBacklogQuotaMap`, `setBacklogQuota`, `removeBacklogQuota`,
//! `getNamespaceMessageTTL`, `setNamespaceMessageTTL`,
//! `removeNamespaceMessageTTL`).

use magnetar_admin::{AdminClient, BacklogQuota, BacklogQuotaType, RetentionPolicies};
use wiremock::matchers::{body_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn retention_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/retention"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "retentionTimeInMinutes": 1440,
            "retentionSizeInMB": 10240,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/retention"))
        .and(body_json(serde_json::json!({
            "retentionTimeInMinutes": 60,
            "retentionSizeInMB": -1,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/admin/v2/namespaces/acme/svc/retention"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let pol = admin
        .namespace_get_retention("acme/svc")
        .await
        .expect("get retention");
    assert_eq!(pol.retention_time_in_minutes, 1440);
    assert_eq!(pol.retention_size_in_mb, 10240);

    admin
        .namespace_set_retention(
            "acme/svc",
            RetentionPolicies {
                retention_time_in_minutes: 60,
                retention_size_in_mb: -1,
            },
        )
        .await
        .expect("set retention");
    admin
        .namespace_remove_retention("acme/svc")
        .await
        .expect("remove retention");
}

#[tokio::test]
async fn retention_get_handles_post_remove_empty_body() {
    // Pulsar 4 `getRetention` calls `asyncResponse.resume(policies.retention_policies)`
    // — after a `remove`, `retention_policies` is null, which Jersey
    // serialises as 204 No Content with an empty body (or, depending
    // on the JAX-RS config, a literal `null` text body). Strict
    // `json_ok` decoding errors with `EOF while parsing a value` /
    // `invalid type: null, expected struct RetentionPolicies`. The
    // tolerant decoder folds either case to `RetentionPolicies::default()`
    // — matching the broker semantic "policy unset = broker default".
    for (body, status) in [
        (None, 204_u16),                      // No Content
        (Some(serde_json::Value::Null), 200), // literal `null`
        (Some(serde_json::json!({})), 200),   // empty object (older brokers)
    ] {
        let mock = MockServer::start().await;
        let resp = if let Some(b) = body {
            ResponseTemplate::new(status).set_body_json(b)
        } else {
            ResponseTemplate::new(status)
        };
        Mock::given(method("GET"))
            .and(path("/admin/v2/namespaces/acme/svc/retention"))
            .respond_with(resp)
            .expect(1)
            .mount(&mock)
            .await;
        let admin = client(&mock);
        let pol = admin
            .namespace_get_retention("acme/svc")
            .await
            .expect("post-remove get must not surface as EOF / type error");
        assert_eq!(pol.retention_time_in_minutes, 0);
        assert_eq!(pol.retention_size_in_mb, 0);
    }
}

#[tokio::test]
async fn backlog_quota_set_with_destination_storage_type() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/backlogQuota"))
        .and(query_param("backlogQuotaType", "destination_storage"))
        .and(body_json(serde_json::json!({
            "limitSize": 1_073_741_824_i64,
            "limitTime": -1,
            "policy": "consumer_backlog_eviction",
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
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
        .expect("set backlog quota");
}

#[tokio::test]
async fn backlog_quota_remove_with_message_age_type() {
    let mock = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/v2/namespaces/acme/svc/backlogQuota"))
        .and(query_param("backlogQuotaType", "message_age"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .namespace_remove_backlog_quota("acme/svc", BacklogQuotaType::MessageAge)
        .await
        .expect("remove backlog quota");
}

#[tokio::test]
async fn backlog_quotas_map_returns_keyed_object() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/backlogQuotaMap"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "destination_storage": {
                "limitSize": 1_073_741_824_i64,
                "limitTime": -1,
                "policy": "consumer_backlog_eviction",
            }
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let v = admin
        .namespace_get_backlog_quotas("acme/svc")
        .await
        .expect("get map");
    assert_eq!(v["destination_storage"]["limitSize"], 1_073_741_824_i64);
}

#[tokio::test]
async fn message_ttl_get_set_remove_with_bare_integer_body() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/messageTTL"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(3600)))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/messageTTL"))
        .and(body_json(serde_json::json!(7200)))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/admin/v2/namespaces/acme/svc/messageTTL"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    assert_eq!(
        admin.namespace_get_message_ttl("acme/svc").await.unwrap(),
        Some(3600)
    );
    admin
        .namespace_set_message_ttl("acme/svc", 7200)
        .await
        .unwrap();
    admin
        .namespace_remove_message_ttl("acme/svc")
        .await
        .unwrap();
}

#[tokio::test]
async fn message_ttl_get_returns_none_for_null_body() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/messageTTL"))
        .respond_with(ResponseTemplate::new(200).set_body_string("null"))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let ttl = admin
        .namespace_get_message_ttl("acme/svc")
        .await
        .expect("get returns 200");
    assert!(ttl.is_none());
}
