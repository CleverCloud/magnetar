// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the third batch of namespace policy REST
//! endpoints — deduplication + deduplication-snapshot-interval +
//! compaction threshold + delayed-delivery + the per-topic /
//! per-consumer / per-subscription limit knobs.
//!
//! These pin the exact path, verb, and JSON body shape against
//! `pulsar-broker/.../v2/Namespaces.java` and the policy verbs in
//! `NamespacesBase` (`getDeduplication`, `modifyDeduplication`,
//! `removeDeduplication`, `getDeduplicationSnapshotInterval`,
//! `setDeduplicationSnapshotInterval`,
//! `deleteDeduplicationSnapshotInterval`, `getCompactionThreshold`,
//! `setCompactionThreshold`, `deleteCompactionThreshold`,
//! `getDelayedDeliveryPolicies`, `setDelayedDeliveryPolicies`,
//! `removeDelayedDeliveryPolicies`, `getMaxProducersPerTopic`,
//! `setMaxProducersPerTopic`, `removeMaxProducersPerTopic`,
//! `getMaxConsumersPerTopic`, `setMaxConsumersPerTopic`,
//! `removeMaxConsumersPerTopic`,
//! `getMaxUnackedMessagesPerConsumer`,
//! `setMaxUnackedMessagesPerConsumer`,
//! `removeMaxUnackedMessagesPerConsumer`,
//! `getMaxUnackedMessagesPerSubscription`,
//! `setMaxUnackedMessagesPerSubscription`,
//! `removeMaxUnackedMessagesPerSubscription`).

use magnetar_admin::{AdminClient, DelayedDeliveryPolicies};
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn deduplication_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/deduplication"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(true)))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/deduplication"))
        .and(body_json(serde_json::json!(false)))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/admin/v2/namespaces/acme/svc/deduplication"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    assert_eq!(
        admin
            .namespace_get_deduplication("acme/svc")
            .await
            .expect("get deduplication"),
        Some(true)
    );
    admin
        .namespace_set_deduplication("acme/svc", false)
        .await
        .expect("set deduplication");
    admin
        .namespace_remove_deduplication("acme/svc")
        .await
        .expect("remove deduplication");
}

#[tokio::test]
async fn deduplication_snapshot_interval_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/namespaces/acme/svc/deduplicationSnapshotInterval",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(1000)))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path(
            "/admin/v2/namespaces/acme/svc/deduplicationSnapshotInterval",
        ))
        .and(body_json(serde_json::json!(2500)))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path(
            "/admin/v2/namespaces/acme/svc/deduplicationSnapshotInterval",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    assert_eq!(
        admin
            .namespace_get_deduplication_snapshot_interval("acme/svc")
            .await
            .expect("get dedup snapshot interval"),
        Some(1000)
    );
    admin
        .namespace_set_deduplication_snapshot_interval("acme/svc", 2500)
        .await
        .expect("set dedup snapshot interval");
    admin
        .namespace_remove_deduplication_snapshot_interval("acme/svc")
        .await
        .expect("remove dedup snapshot interval");
}

#[tokio::test]
async fn compaction_threshold_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/compactionThreshold"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!(536_870_912_i64)),
        )
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/compactionThreshold"))
        .and(body_json(serde_json::json!(1_073_741_824_i64)))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/admin/v2/namespaces/acme/svc/compactionThreshold"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    assert_eq!(
        admin
            .namespace_get_compaction_threshold("acme/svc")
            .await
            .expect("get compaction threshold"),
        Some(536_870_912)
    );
    admin
        .namespace_set_compaction_threshold("acme/svc", 1_073_741_824)
        .await
        .expect("set compaction threshold");
    admin
        .namespace_remove_compaction_threshold("acme/svc")
        .await
        .expect("remove compaction threshold");
}

#[tokio::test]
async fn delayed_delivery_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/delayedDelivery"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "active": true,
            "tickTimeMillis": 1000,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/delayedDelivery"))
        .and(body_json(serde_json::json!({
            "active": false,
            "tickTimeMillis": 5000,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/admin/v2/namespaces/acme/svc/delayedDelivery"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let pol = admin
        .namespace_get_delayed_delivery("acme/svc")
        .await
        .expect("get delayed delivery")
        .expect("policy present");
    assert!(pol.active);
    assert_eq!(pol.tick_time_millis, 1000);

    admin
        .namespace_set_delayed_delivery(
            "acme/svc",
            DelayedDeliveryPolicies {
                active: false,
                tick_time_millis: 5000,
            },
        )
        .await
        .expect("set delayed delivery");
    admin
        .namespace_remove_delayed_delivery("acme/svc")
        .await
        .expect("remove delayed delivery");
}

#[tokio::test]
async fn max_producers_per_topic_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/maxProducersPerTopic"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(64)))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/maxProducersPerTopic"))
        .and(body_json(serde_json::json!(128)))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/admin/v2/namespaces/acme/svc/maxProducersPerTopic"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    assert_eq!(
        admin
            .namespace_get_max_producers_per_topic("acme/svc")
            .await
            .expect("get max producers per topic"),
        Some(64)
    );
    admin
        .namespace_set_max_producers_per_topic("acme/svc", 128)
        .await
        .expect("set max producers per topic");
    admin
        .namespace_remove_max_producers_per_topic("acme/svc")
        .await
        .expect("remove max producers per topic");
}

#[tokio::test]
async fn max_consumers_per_topic_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/acme/svc/maxConsumersPerTopic"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(256)))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path("/admin/v2/namespaces/acme/svc/maxConsumersPerTopic"))
        .and(body_json(serde_json::json!(512)))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path("/admin/v2/namespaces/acme/svc/maxConsumersPerTopic"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    assert_eq!(
        admin
            .namespace_get_max_consumers_per_topic("acme/svc")
            .await
            .expect("get max consumers per topic"),
        Some(256)
    );
    admin
        .namespace_set_max_consumers_per_topic("acme/svc", 512)
        .await
        .expect("set max consumers per topic");
    admin
        .namespace_remove_max_consumers_per_topic("acme/svc")
        .await
        .expect("remove max consumers per topic");
}

#[tokio::test]
async fn max_unacked_messages_per_consumer_get_set_remove_cycle() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/namespaces/acme/svc/maxUnackedMessagesPerConsumer",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(50_000)))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("POST"))
        .and(path(
            "/admin/v2/namespaces/acme/svc/maxUnackedMessagesPerConsumer",
        ))
        .and(body_json(serde_json::json!(100_000)))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    Mock::given(method("DELETE"))
        .and(path(
            "/admin/v2/namespaces/acme/svc/maxUnackedMessagesPerConsumer",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    assert_eq!(
        admin
            .namespace_get_max_unacked_messages_per_consumer("acme/svc")
            .await
            .expect("get max unacked per consumer"),
        Some(50_000)
    );
    admin
        .namespace_set_max_unacked_messages_per_consumer("acme/svc", 100_000)
        .await
        .expect("set max unacked per consumer");
    admin
        .namespace_remove_max_unacked_messages_per_consumer("acme/svc")
        .await
        .expect("remove max unacked per consumer");
}
