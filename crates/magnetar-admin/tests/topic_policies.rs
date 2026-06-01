// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the per-topic policy REST endpoints — the
//! topic-level overrides for each namespace policy family covered by
//! `tests/namespace_policies.rs` and `tests/namespace_policies_breadth.rs`.
//!
//! These pin the exact path, verb, query parameter, and JSON body shape
//! against `pulsar-broker/.../v2/PersistentTopics.java` and the policy
//! verbs in `PersistentTopicsBase` (`getRetention`, `setRetention`,
//! `removeRetention`, …).

use magnetar_admin::{
    AdminClient, BacklogQuota, BacklogQuotaType, DispatchRate, PersistencePolicies, PublishRate,
    RetentionPolicies,
};
use wiremock::matchers::{body_json, method, path, query_param};
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

#[tokio::test]
async fn topic_backlog_quota_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/persistent/acme/svc/orders/backlogQuotaMap"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "destination_storage": {
                "limitSize": 1073741824_i64,
                "limitTime": -1,
                "policy": "consumer_backlog_eviction",
            }
        })))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/persistent/acme/svc/orders/backlogQuota"))
        .and(query_param("backlogQuotaType", "destination_storage"))
        .and(body_json(serde_json::json!({
            "limitSize": 2147483648_i64,
            "limitTime": -1,
            "policy": "producer_request_hold",
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/admin/v2/persistent/acme/svc/orders/backlogQuota"))
        .and(query_param("backlogQuotaType", "message_age"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let v = admin
        .topic_get_backlog_quotas("acme/svc/orders")
        .await
        .expect("get topic quotas");
    assert_eq!(v["destination_storage"]["limitSize"], 1_073_741_824_i64);

    admin
        .topic_set_backlog_quota(
            "acme/svc/orders",
            BacklogQuotaType::DestinationStorage,
            BacklogQuota {
                limit_size: 2_147_483_648,
                limit_time: -1,
                policy: "producer_request_hold".into(),
            },
        )
        .await
        .expect("set topic backlog quota");
    admin
        .topic_remove_backlog_quota("acme/svc/orders", BacklogQuotaType::MessageAge)
        .await
        .expect("remove topic backlog quota");
}

#[tokio::test]
async fn topic_message_ttl_get_set_remove_with_bare_integer_body() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/persistent/acme/svc/orders/messageTTL"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(3600)))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v2/persistent/acme/svc/orders/messageTTL"))
        .and(body_json(serde_json::json!(7200)))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/admin/v2/persistent/acme/svc/orders/messageTTL"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    assert_eq!(
        admin
            .topic_get_message_ttl("acme/svc/orders")
            .await
            .unwrap(),
        Some(3600)
    );
    admin
        .topic_set_message_ttl("acme/svc/orders", 7200)
        .await
        .unwrap();
    admin
        .topic_remove_message_ttl("acme/svc/orders")
        .await
        .unwrap();
}

#[tokio::test]
async fn topic_persistence_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/persistent/acme/svc/orders/persistence"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "bookkeeperEnsemble": 3,
            "bookkeeperWriteQuorum": 2,
            "bookkeeperAckQuorum": 2,
            "managedLedgerMaxMarkDeleteRate": 1.0,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/persistent/acme/svc/orders/persistence"))
        .and(body_json(serde_json::json!({
            "bookkeeperEnsemble": 5,
            "bookkeeperWriteQuorum": 3,
            "bookkeeperAckQuorum": 2,
            "managedLedgerMaxMarkDeleteRate": 2.5,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/admin/v2/persistent/acme/svc/orders/persistence"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let pol = admin
        .topic_get_persistence("acme/svc/orders")
        .await
        .expect("get topic persistence")
        .expect("non-null body");
    assert_eq!(pol.bookkeeper_ensemble, 3);
    assert_eq!(pol.bookkeeper_write_quorum, 2);
    assert_eq!(pol.bookkeeper_ack_quorum, 2);
    assert!((pol.managed_ledger_max_mark_delete_rate - 1.0).abs() < f64::EPSILON);

    admin
        .topic_set_persistence(
            "acme/svc/orders",
            PersistencePolicies {
                bookkeeper_ensemble: 5,
                bookkeeper_write_quorum: 3,
                bookkeeper_ack_quorum: 2,
                managed_ledger_max_mark_delete_rate: 2.5,
            },
        )
        .await
        .expect("set topic persistence");
    admin
        .topic_remove_persistence("acme/svc/orders")
        .await
        .expect("remove topic persistence");
}

#[tokio::test]
async fn topic_dispatch_rate_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/persistent/acme/svc/orders/dispatchRate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "dispatchThrottlingRateInMsg": 1000,
            "dispatchThrottlingRateInByte": 1048576_i64,
            "ratePeriodInSecond": 1,
            "relativeToPublishRate": false,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/persistent/acme/svc/orders/dispatchRate"))
        .and(body_json(serde_json::json!({
            "dispatchThrottlingRateInMsg": -1,
            "dispatchThrottlingRateInByte": -1,
            "ratePeriodInSecond": 1,
            "relativeToPublishRate": true,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/admin/v2/persistent/acme/svc/orders/dispatchRate"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let rate = admin
        .topic_get_dispatch_rate("acme/svc/orders")
        .await
        .expect("get topic dispatch rate")
        .expect("non-null body");
    assert_eq!(rate.dispatch_throttling_rate_in_msg, 1000);
    assert_eq!(rate.dispatch_throttling_rate_in_byte, 1_048_576);

    admin
        .topic_set_dispatch_rate(
            "acme/svc/orders",
            DispatchRate {
                dispatch_throttling_rate_in_msg: -1,
                dispatch_throttling_rate_in_byte: -1,
                rate_period_in_second: 1,
                relative_to_publish_rate: true,
            },
        )
        .await
        .expect("set topic dispatch rate");
    admin
        .topic_remove_dispatch_rate("acme/svc/orders")
        .await
        .expect("remove topic dispatch rate");
}

#[tokio::test]
async fn topic_subscription_dispatch_rate_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/acme/svc/orders/subscriptionDispatchRate",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "dispatchThrottlingRateInMsg": 500,
            "dispatchThrottlingRateInByte": 524288_i64,
            "ratePeriodInSecond": 1,
            "relativeToPublishRate": false,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path(
            "/admin/v2/persistent/acme/svc/orders/subscriptionDispatchRate",
        ))
        .and(body_json(serde_json::json!({
            "dispatchThrottlingRateInMsg": 2000,
            "dispatchThrottlingRateInByte": -1,
            "ratePeriodInSecond": 2,
            "relativeToPublishRate": false,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path(
            "/admin/v2/persistent/acme/svc/orders/subscriptionDispatchRate",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let rate = admin
        .topic_get_subscription_dispatch_rate("acme/svc/orders")
        .await
        .expect("get topic subscription dispatch rate")
        .expect("non-null body");
    assert_eq!(rate.dispatch_throttling_rate_in_msg, 500);
    assert_eq!(rate.dispatch_throttling_rate_in_byte, 524_288);

    admin
        .topic_set_subscription_dispatch_rate(
            "acme/svc/orders",
            DispatchRate {
                dispatch_throttling_rate_in_msg: 2000,
                dispatch_throttling_rate_in_byte: -1,
                rate_period_in_second: 2,
                relative_to_publish_rate: false,
            },
        )
        .await
        .expect("set topic subscription dispatch rate");
    admin
        .topic_remove_subscription_dispatch_rate("acme/svc/orders")
        .await
        .expect("remove topic subscription dispatch rate");
}

#[tokio::test]
async fn topic_replicator_dispatch_rate_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/acme/svc/orders/replicatorDispatchRate",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "dispatchThrottlingRateInMsg": 100,
            "dispatchThrottlingRateInByte": 65536_i64,
            "ratePeriodInSecond": 1,
            "relativeToPublishRate": false,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path(
            "/admin/v2/persistent/acme/svc/orders/replicatorDispatchRate",
        ))
        .and(body_json(serde_json::json!({
            "dispatchThrottlingRateInMsg": 300,
            "dispatchThrottlingRateInByte": 131072_i64,
            "ratePeriodInSecond": 1,
            "relativeToPublishRate": false,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path(
            "/admin/v2/persistent/acme/svc/orders/replicatorDispatchRate",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let rate = admin
        .topic_get_replicator_dispatch_rate("acme/svc/orders")
        .await
        .expect("get topic replicator dispatch rate")
        .expect("non-null body");
    assert_eq!(rate.dispatch_throttling_rate_in_msg, 100);
    assert_eq!(rate.dispatch_throttling_rate_in_byte, 65_536);

    admin
        .topic_set_replicator_dispatch_rate(
            "acme/svc/orders",
            DispatchRate {
                dispatch_throttling_rate_in_msg: 300,
                dispatch_throttling_rate_in_byte: 131_072,
                rate_period_in_second: 1,
                relative_to_publish_rate: false,
            },
        )
        .await
        .expect("set topic replicator dispatch rate");
    admin
        .topic_remove_replicator_dispatch_rate("acme/svc/orders")
        .await
        .expect("remove topic replicator dispatch rate");
}

#[tokio::test]
async fn topic_publish_rate_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/persistent/acme/svc/orders/publishRate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "publishThrottlingRateInMsg": 5000,
            "publishThrottlingRateInByte": 2097152_i64,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/persistent/acme/svc/orders/publishRate"))
        .and(body_json(serde_json::json!({
            "publishThrottlingRateInMsg": -1,
            "publishThrottlingRateInByte": -1,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/admin/v2/persistent/acme/svc/orders/publishRate"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let rate = admin
        .topic_get_publish_rate("acme/svc/orders")
        .await
        .expect("get topic publish rate")
        .expect("non-null body");
    assert_eq!(rate.publish_throttling_rate_in_msg, 5000);
    assert_eq!(rate.publish_throttling_rate_in_byte, 2_097_152);

    admin
        .topic_set_publish_rate(
            "acme/svc/orders",
            PublishRate {
                publish_throttling_rate_in_msg: -1,
                publish_throttling_rate_in_byte: -1,
            },
        )
        .await
        .expect("set topic publish rate");
    admin
        .topic_remove_publish_rate("acme/svc/orders")
        .await
        .expect("remove topic publish rate");
}
