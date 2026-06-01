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

use magnetar_admin::AdminClient;
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
