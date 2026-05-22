// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the PIP-415 `getMessageIdByIndex` REST endpoint.
//!
//! These pin the exact path, verb, query parameter, and JSON wire shape
//! against the upstream Apache Pulsar admin REST surface
//! ([`apache/pulsar#24222`](https://github.com/apache/pulsar/pull/24222),
//! merged 2025-06-23).
//!
//! The endpoint is intentionally **REST-only**: the
//! [PIP-415 spec](https://github.com/apache/pulsar/blob/master/pip/pip-415.md)
//! leaves its "Binary protocol" section empty, and PR #24222 touches only
//! admin / broker / CLI Java code. Implementation lives in
//! `magnetar-admin`; the vendored proto stays untouched.

use magnetar_admin::{AdminClient, AdminError};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Build an [`AdminClient`] pointed at the wiremock server.
fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn get_message_id_by_index_hits_pip_415_endpoint() {
    let mock = MockServer::start().await;

    // Upstream: `@GET @Path("/{tenant}/{namespace}/{topic}/getMessageIdByIndex")`
    // (`pulsar-broker/.../broker/admin/v2/PersistentTopics.java`), with
    // `@QueryParam("index") long`.
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/getMessageIdByIndex",
        ))
        .and(query_param("index", "12345"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ledgerId": 42,
            "entryId": 7,
            "partitionIndex": 0,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let id = admin
        .topic_get_message_id_by_index("public/default/orders", 12345)
        .await
        .expect("PIP-415 endpoint returned a MessageId");

    assert_eq!(id.ledger_id, 42);
    assert_eq!(id.entry_id, 7);
    assert_eq!(id.partition, 0);
    // The broker only resolves at entry granularity; batch fields are absent
    // from the JSON and must collapse to the canonical `-1` sentinel.
    assert_eq!(id.batch_index, -1);
    assert_eq!(id.batch_size, -1);
}

#[tokio::test]
async fn get_message_id_by_index_accepts_persistent_scheme_prefix() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/acme/svc/events/getMessageIdByIndex",
        ))
        .and(query_param("index", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ledgerId": 1,
            "entryId": 2,
            "partitionIndex": -1,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let id = admin
        .topic_get_message_id_by_index("persistent://acme/svc/events", 0)
        .await
        .unwrap();
    assert_eq!(id.partition, -1);
}

#[tokio::test]
async fn get_message_id_by_index_propagates_broker_412() {
    let mock = MockServer::start().await;
    // PIP-415 §"Error Responses": 412 means broker-entry-metadata is disabled.
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/public/default/topic/getMessageIdByIndex",
        ))
        .respond_with(
            ResponseTemplate::new(412).set_body_string("Broker entry metadata is not enabled"),
        )
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let err = admin
        .topic_get_message_id_by_index("public/default/topic", 1)
        .await
        .unwrap_err();
    match err {
        AdminError::Status { code, body } => {
            assert_eq!(code, 412);
            assert!(body.contains("Broker entry metadata"));
        }
        other => panic!("expected AdminError::Status, got {other:?}"),
    }
}

#[tokio::test]
async fn get_message_id_by_index_propagates_broker_404_invalid_index() {
    let mock = MockServer::start().await;
    // PIP-415 §"Error Responses": 404 covers both "topic not found" and
    // "invalid index" (e.g. negative or beyond the topic's max index).
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/public/default/topic/getMessageIdByIndex",
        ))
        .and(query_param("index", "-1"))
        .respond_with(ResponseTemplate::new(404).set_body_string("Invalid index"))
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let err = admin
        .topic_get_message_id_by_index("public/default/topic", -1)
        .await
        .unwrap_err();
    assert!(matches!(err, AdminError::Status { code: 404, .. }));
}

#[tokio::test]
async fn get_message_id_by_index_rejects_malformed_topic() {
    // Topic-name validation runs before any HTTP traffic — the mock should
    // never be touched. Starting one keeps the test surface identical to
    // its peers.
    let mock = MockServer::start().await;
    let admin = client(&mock);
    let err = admin
        .topic_get_message_id_by_index("missing-namespace", 0)
        .await
        .unwrap_err();
    assert!(matches!(err, AdminError::InvalidName(_)));
}
