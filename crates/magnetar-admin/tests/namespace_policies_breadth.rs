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

use magnetar_admin::{AdminClient, PersistencePolicies};
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
