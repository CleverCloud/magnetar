// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the schema-registry REST endpoints — get
//! latest, list versions, post + delete round-trip, compatibility
//! check.
//!
//! These pin the exact path, verb, and JSON body shape against
//! `SchemasResourceBase` in
//! `pulsar-broker/.../v2/SchemasResource.java` (`getSchema`,
//! `getAllSchemas`, `postSchema`, `deleteSchema`, `testCompatibility`).
//! Response payloads stay as `serde_json::Value` because the schema-
//! type axis is open-ended and the response envelope shifts between
//! 4.x point releases.

use std::collections::HashMap;

use magnetar_admin::{AdminClient, PostSchemaPayload};
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn schema_get_latest_returns_envelope() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/schemas/public/default/orders/schema"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "version": 3,
            "type": "AVRO",
            "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}",
            "properties": {},
            "timestamp": 1_700_000_000_000_i64,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let schema = admin
        .schema_get_latest("public/default/orders")
        .await
        .expect("get-latest returns 200");
    assert_eq!(schema["version"], 3);
    assert_eq!(schema["type"], "AVRO");
}

#[tokio::test]
async fn schema_list_versions_accepts_bare_array() {
    // Legacy / proxy surfaces may emit a bare JSON array for this
    // endpoint; pinning that we still accept the flat shape so
    // older deployments don't break on upgrade.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/schemas/public/default/orders/schemas"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            { "version": 1, "type": "AVRO", "schema": "v1", "properties": {} },
            { "version": 2, "type": "AVRO", "schema": "v2", "properties": {} },
        ])))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let versions = admin
        .schema_list_versions("public/default/orders")
        .await
        .expect("list-versions returns 200");
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0]["version"], 1);
    assert_eq!(versions[1]["version"], 2);
}

#[tokio::test]
async fn schema_list_versions_unwraps_get_all_versions_envelope() {
    // Pulsar 4 wraps per-version entries in
    // `GetAllVersionsSchemaResponse { getSchemaResponses: [...] }`
    // (per `SchemasResourceBase#convertToAllVersionsSchemaResponse`).
    // The admin client unwraps that envelope at the boundary so
    // callers see a flat `Vec<Value>`.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/schemas/public/default/orders/schemas"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "getSchemaResponses": [
                { "version": 1, "type": "AVRO", "schema": "v1", "properties": {} },
                { "version": 2, "type": "AVRO", "schema": "v2", "properties": {} },
            ]
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let versions = admin
        .schema_list_versions("public/default/orders")
        .await
        .expect("list-versions returns 200 envelope");
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0]["version"], 1);
    assert_eq!(versions[1]["version"], 2);
}

#[tokio::test]
async fn schema_post_then_delete_round_trip() {
    let mock = MockServer::start().await;
    // `postSchema` accepts the bare `PostSchemaPayload` and returns
    // the assigned version. We pin the body keys (`type`, `schema`,
    // `properties`) so a future serde rename can't drift.
    Mock::given(method("POST"))
        .and(path("/admin/v2/schemas/public/default/orders/schema"))
        .and(body_json(serde_json::json!({
            "type": "STRING",
            "schema": "",
            "properties": { "owner": "team-a" },
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "version": 7,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    // `?force=true` round-trips the broker's `deleteSchema(force)`
    // path-param; the wire encodes it as a query parameter and we
    // assert the path-only here (the matcher is path-only).
    Mock::given(method("DELETE"))
        .and(path("/admin/v2/schemas/public/default/orders/schema"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let mut props = HashMap::new();
    props.insert("owner".to_owned(), "team-a".to_owned());
    let posted = admin
        .schema_post(
            "public/default/orders",
            PostSchemaPayload {
                schema_type: "STRING".into(),
                schema: String::new(),
                properties: props,
            },
        )
        .await
        .expect("post returns 200 + version");
    assert_eq!(posted["version"], 7);

    admin
        .schema_delete("public/default/orders", true)
        .await
        .expect("delete returns 204");
}

#[tokio::test]
async fn schema_compatibility_check_returns_verdict() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(
            "/admin/v2/schemas/public/default/orders/compatibility",
        ))
        .and(body_json(serde_json::json!({
            "type": "AVRO",
            "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}",
            "properties": {},
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "isCompatible": true,
            "schemaCompatibilityStrategy": "FULL",
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let verdict = admin
        .schema_compatibility_check(
            "public/default/orders",
            PostSchemaPayload {
                schema_type: "AVRO".into(),
                schema: "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}".into(),
                properties: HashMap::new(),
            },
        )
        .await
        .expect("compatibility check returns 200");
    assert_eq!(verdict["isCompatible"], true);
    assert_eq!(verdict["schemaCompatibilityStrategy"], "FULL");
}
