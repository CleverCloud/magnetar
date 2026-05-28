// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the PIP-180 shadow-topic admin REST endpoints.
//!
//! These pin the exact path, verb, query parameter, and JSON wire shape
//! against the upstream Apache Pulsar admin REST surface
//! ([PIP-180](https://github.com/apache/pulsar/blob/master/pip/pip-180.md),
//! merged in Pulsar 2.11) — `createShadowTopic`, `deleteShadowTopic`,
//! `getShadowTopics`, `getShadowSource` on
//! `pulsar-broker/.../v2/PersistentTopics.java`.

use magnetar_admin::{AdminClient, AdminError};
use wiremock::matchers::{body_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn create_shadow_topic_puts_correct_url_and_bare_array_body() {
    let mock = MockServer::start().await;
    // PIP-180: `@PUT @Path("/{tenant}/{namespace}/{topic}/shadowTopics")` on
    // the source topic. The broker's `setShadowTopics(List<String>)` handler
    // deserialises the body directly into a `List<String>` — the body MUST be
    // a bare JSON array, NOT a `{ "shadowTopics": [...] }` envelope (Pulsar
    // 4.0.4 rejects the envelope with HTTP 400; see docs/follow-ups.md §5).
    Mock::given(method("PUT"))
        .and(path(
            "/admin/v2/persistent/public/default/source-t/shadowTopics",
        ))
        .and(body_json(serde_json::json!([
            "persistent://public/default/shadow-t"
        ])))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .create_shadow_topic(
            "public/default/source-t",
            "persistent://public/default/shadow-t",
        )
        .await
        .expect("PIP-180 createShadowTopic returns 204");
}

#[tokio::test]
async fn create_shadow_topic_propagates_409_conflict() {
    // PIP-180: 409 = shadow already exists. Must surface as
    // AdminError::Status { code: 409, .. }.
    let mock = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path(
            "/admin/v2/persistent/public/default/source-t/shadowTopics",
        ))
        .respond_with(ResponseTemplate::new(409).set_body_string("Shadow topic already exists"))
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let err = admin
        .create_shadow_topic(
            "public/default/source-t",
            "persistent://public/default/shadow-t",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, AdminError::Status { code: 409, ref body } if body.contains("already exists")),
        "expected AdminError::Status {{ code: 409, .. }}, got {err:?}",
    );
}

#[tokio::test]
async fn delete_shadow_topic_uses_delete_verb_and_force_query() {
    let mock = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/v2/persistent/public/default/shadow-t"))
        .and(query_param("force", "true"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    let admin = client(&mock);
    admin
        .delete_shadow_topic("public/default/shadow-t", true)
        .await
        .expect("DELETE returns 204");
}

#[tokio::test]
async fn delete_shadow_topic_propagates_404() {
    // PIP-180: 404 = topic does not exist on the broker — must surface
    // verbatim through AdminError::Status.
    let mock = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/v2/persistent/public/default/missing"))
        .respond_with(ResponseTemplate::new(404).set_body_string("Topic not found"))
        .mount(&mock)
        .await;
    let admin = client(&mock);
    let err = admin
        .delete_shadow_topic("public/default/missing", false)
        .await
        .unwrap_err();
    assert!(matches!(err, AdminError::Status { code: 404, .. }));
}

#[tokio::test]
async fn get_shadow_topics_parses_response_array() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/public/default/source-t/shadowTopics",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            "persistent://public/default/shadow-1",
            "persistent://public/default/shadow-2",
        ])))
        .expect(1)
        .mount(&mock)
        .await;
    let admin = client(&mock);
    let shadows = admin
        .get_shadow_topics("public/default/source-t")
        .await
        .expect("GET shadowTopics returns array");
    assert_eq!(shadows.len(), 2);
    assert_eq!(shadows[0], "persistent://public/default/shadow-1");
    assert_eq!(shadows[1], "persistent://public/default/shadow-2");
}

#[tokio::test]
async fn get_shadow_topics_returns_empty_for_non_shadow_topic() {
    // A regular (non-shadowed) source topic returns an empty array.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/public/default/regular/shadowTopics",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .mount(&mock)
        .await;
    let admin = client(&mock);
    let shadows = admin
        .get_shadow_topics("public/default/regular")
        .await
        .unwrap();
    assert!(shadows.is_empty());
}

#[tokio::test]
async fn get_shadow_source_resolves_shadow_to_source_topic() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/public/default/shadow-t/shadowSource",
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!("persistent://public/default/source-t")),
        )
        .mount(&mock)
        .await;
    let admin = client(&mock);
    let source = admin
        .get_shadow_source("public/default/shadow-t")
        .await
        .expect("shadowSource resolves on a shadow topic");
    assert_eq!(
        source,
        Some("persistent://public/default/source-t".to_owned())
    );
}

#[tokio::test]
async fn get_shadow_source_returns_none_for_non_shadow_topic() {
    // Pulsar 2.11+ returns 204 No Content for a non-shadow topic; older
    // builds return 200 with a `null` body. Both must collapse to None.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/public/default/regular/shadowSource",
        ))
        .respond_with(ResponseTemplate::new(204))
        .mount(&mock)
        .await;
    let admin = client(&mock);
    let source = admin
        .get_shadow_source("public/default/regular")
        .await
        .unwrap();
    assert!(source.is_none());
}

#[tokio::test]
async fn shadow_admin_methods_reject_malformed_topic_name() {
    // Name validation runs before HTTP — the mock should never see traffic.
    let mock = MockServer::start().await;
    let admin = client(&mock);
    let err = admin
        .get_shadow_topics("missing-namespace")
        .await
        .unwrap_err();
    assert!(matches!(err, AdminError::InvalidName(_)));
    let err = admin
        .create_shadow_topic("ok/ns/source", "bad-shadow-name")
        .await
        .unwrap_err();
    assert!(matches!(err, AdminError::InvalidName(_)));
}
