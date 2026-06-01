// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the per-topic policy REST endpoints — the
//! topic-level overrides for each namespace policy family covered by
//! `tests/namespace_policies.rs` and `tests/namespace_policies_breadth.rs`.
//!
//! These pin the exact path, verb, query parameter, and JSON body shape
//! against `pulsar-broker/.../v2/PersistentTopics.java` and the policy
//! verbs in `PersistentTopicsBase` (`getRetention`, `setRetention`,
//! `removeRetention`, …).

use magnetar_admin::{AdminClient, RetentionPolicies};
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn topic_retention_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/persistent/acme/svc/orders/retention"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "retentionTimeInMinutes": 1440,
            "retentionSizeInMB": 10240,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/persistent/acme/svc/orders/retention"))
        .and(body_json(serde_json::json!({
            "retentionTimeInMinutes": 60,
            "retentionSizeInMB": -1,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/admin/v2/persistent/acme/svc/orders/retention"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let pol = admin
        .topic_get_retention("acme/svc/orders")
        .await
        .expect("get topic retention");
    assert_eq!(pol.retention_time_in_minutes, 1440);
    assert_eq!(pol.retention_size_in_mb, 10240);

    admin
        .topic_set_retention(
            "acme/svc/orders",
            RetentionPolicies {
                retention_time_in_minutes: 60,
                retention_size_in_mb: -1,
            },
        )
        .await
        .expect("set topic retention");
    admin
        .topic_remove_retention("acme/svc/orders")
        .await
        .expect("remove topic retention");
}
