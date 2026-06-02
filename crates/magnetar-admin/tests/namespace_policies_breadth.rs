// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the second batch of namespace policy REST
//! endpoints — persistence + the four rate policy families
//! (dispatch, subscription-dispatch, replicator-dispatch, publish).
//!
//! These pin the exact path, verb, and JSON body shape against
//! `pulsar-broker/.../v2/Namespaces.java` and the policy verbs in
//! `NamespacesBase` (`getPersistence`, `setPersistence`,
//! `deletePersistence`, `getDispatchRate`, `setDispatchRate`,
//! `deleteDispatchRate`, `getSubscriptionDispatchRate`,
//! `setSubscriptionDispatchRate`, `deleteSubscriptionDispatchRate`,
//! `getReplicatorDispatchRate`, `setReplicatorDispatchRate`,
//! `removeReplicatorDispatchRate`, `getPublishRate`, `setPublishRate`,
//! `removePublishRate`).

use magnetar_admin::{AdminClient, DispatchRate, PersistencePolicies, PublishRate};
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn persistence_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/persistence"))
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
        .and(path("/admin/v2/namespaces/acme/svc/persistence"))
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
        .and(path("/admin/v2/namespaces/acme/svc/persistence"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let pol = admin
        .namespace_get_persistence("acme/svc")
        .await
        .expect("get persistence");
    assert_eq!(pol.bookkeeper_ensemble, 3);
    assert_eq!(pol.bookkeeper_write_quorum, 2);
    assert_eq!(pol.bookkeeper_ack_quorum, 2);
    assert!((pol.managed_ledger_max_mark_delete_rate - 1.0).abs() < f64::EPSILON);

    admin
        .namespace_set_persistence(
            "acme/svc",
            PersistencePolicies {
                bookkeeper_ensemble: 5,
                bookkeeper_write_quorum: 3,
                bookkeeper_ack_quorum: 2,
                managed_ledger_max_mark_delete_rate: 2.5,
            },
        )
        .await
        .expect("set persistence");
    admin
        .namespace_remove_persistence("acme/svc")
        .await
        .expect("remove persistence");
}

#[tokio::test]
async fn dispatch_rate_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/dispatchRate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "dispatchThrottlingRateInMsg": 1000,
            "dispatchThrottlingRateInByte": 1_048_576_i64,
            "ratePeriodInSecond": 1,
            "relativeToPublishRate": false,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/dispatchRate"))
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
        .and(path("/admin/v2/namespaces/acme/svc/dispatchRate"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let rate = admin
        .namespace_get_dispatch_rate("acme/svc")
        .await
        .expect("get dispatch rate");
    assert_eq!(rate.dispatch_throttling_rate_in_msg, 1000);
    assert_eq!(rate.dispatch_throttling_rate_in_byte, 1_048_576);
    assert_eq!(rate.rate_period_in_second, 1);
    assert!(!rate.relative_to_publish_rate);

    admin
        .namespace_set_dispatch_rate(
            "acme/svc",
            DispatchRate {
                dispatch_throttling_rate_in_msg: -1,
                dispatch_throttling_rate_in_byte: -1,
                rate_period_in_second: 1,
                relative_to_publish_rate: true,
            },
        )
        .await
        .expect("set dispatch rate");
    admin
        .namespace_remove_dispatch_rate("acme/svc")
        .await
        .expect("remove dispatch rate");
}

#[tokio::test]
async fn subscription_dispatch_rate_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/namespaces/acme/svc/subscriptionDispatchRate",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "dispatchThrottlingRateInMsg": 500,
            "dispatchThrottlingRateInByte": 524_288_i64,
            "ratePeriodInSecond": 1,
            "relativeToPublishRate": false,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path(
            "/admin/v2/namespaces/acme/svc/subscriptionDispatchRate",
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
            "/admin/v2/namespaces/acme/svc/subscriptionDispatchRate",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let rate = admin
        .namespace_get_subscription_dispatch_rate("acme/svc")
        .await
        .expect("get subscription dispatch rate");
    assert_eq!(rate.dispatch_throttling_rate_in_msg, 500);
    assert_eq!(rate.dispatch_throttling_rate_in_byte, 524_288);

    admin
        .namespace_set_subscription_dispatch_rate(
            "acme/svc",
            DispatchRate {
                dispatch_throttling_rate_in_msg: 2000,
                dispatch_throttling_rate_in_byte: -1,
                rate_period_in_second: 2,
                relative_to_publish_rate: false,
            },
        )
        .await
        .expect("set subscription dispatch rate");
    admin
        .namespace_remove_subscription_dispatch_rate("acme/svc")
        .await
        .expect("remove subscription dispatch rate");
}

#[tokio::test]
async fn replicator_dispatch_rate_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/replicatorDispatchRate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "dispatchThrottlingRateInMsg": 100,
            "dispatchThrottlingRateInByte": 65_536_i64,
            "ratePeriodInSecond": 1,
            "relativeToPublishRate": false,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/replicatorDispatchRate"))
        .and(body_json(serde_json::json!({
            "dispatchThrottlingRateInMsg": 300,
            "dispatchThrottlingRateInByte": 131_072_i64,
            "ratePeriodInSecond": 1,
            "relativeToPublishRate": false,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/admin/v2/namespaces/acme/svc/replicatorDispatchRate"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let rate = admin
        .namespace_get_replicator_dispatch_rate("acme/svc")
        .await
        .expect("get replicator dispatch rate");
    assert_eq!(rate.dispatch_throttling_rate_in_msg, 100);
    assert_eq!(rate.dispatch_throttling_rate_in_byte, 65_536);

    admin
        .namespace_set_replicator_dispatch_rate(
            "acme/svc",
            DispatchRate {
                dispatch_throttling_rate_in_msg: 300,
                dispatch_throttling_rate_in_byte: 131_072,
                rate_period_in_second: 1,
                relative_to_publish_rate: false,
            },
        )
        .await
        .expect("set replicator dispatch rate");
    admin
        .namespace_remove_replicator_dispatch_rate("acme/svc")
        .await
        .expect("remove replicator dispatch rate");
}

#[tokio::test]
async fn publish_rate_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/publishRate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "publishThrottlingRateInMsg": 5000,
            "publishThrottlingRateInByte": 2_097_152_i64,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/publishRate"))
        .and(body_json(serde_json::json!({
            "publishThrottlingRateInMsg": -1,
            "publishThrottlingRateInByte": -1,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/admin/v2/namespaces/acme/svc/publishRate"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let rate = admin
        .namespace_get_publish_rate("acme/svc")
        .await
        .expect("get publish rate");
    assert_eq!(rate.publish_throttling_rate_in_msg, 5000);
    assert_eq!(rate.publish_throttling_rate_in_byte, 2_097_152);

    admin
        .namespace_set_publish_rate(
            "acme/svc",
            PublishRate {
                publish_throttling_rate_in_msg: -1,
                publish_throttling_rate_in_byte: -1,
            },
        )
        .await
        .expect("set publish rate");
    admin
        .namespace_remove_publish_rate("acme/svc")
        .await
        .expect("remove publish rate");
}
