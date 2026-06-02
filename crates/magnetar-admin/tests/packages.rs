// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the Pulsar Packages REST endpoints
//! (`/admin/v3/packages/...`) — list / versions / metadata get + set /
//! delete.
//!
//! Pins the path, verb, and metadata JSON body shape against
//! `PackagesBase` in `pulsar-broker/.../v3/Packages.java`. The package
//! type axis is fixed at the source level (`PackageType` is a closed
//! Rust enum) so the URL builder cannot emit a token the broker would
//! reject; the tests exercise each variant once to lock the wire
//! form.

use std::collections::HashMap;

use magnetar_admin::{AdminClient, PackageMetadata, PackageType};
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn packages_list_returns_names_per_type() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/packages/function/acme/svc"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!(["enrich", "rollup"])),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let names = admin
        .packages_list(PackageType::Function, "acme", "svc")
        .await
        .expect("list returns 200");
    assert_eq!(names, vec!["enrich".to_owned(), "rollup".to_owned()]);
}

#[tokio::test]
async fn package_versions_list_emits_source_url_token() {
    let mock = MockServer::start().await;
    // Confirms `PackageType::Source` lowers to `source` in the URL
    // path — a closed-enum-to-string mapping regression that would
    // otherwise stay invisible until a real broker rejected it.
    Mock::given(method("GET"))
        .and(path("/admin/v3/packages/source/acme/svc/kafka-in"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!(["1.0.0", "1.1.0"])),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let versions = admin
        .package_versions_list(PackageType::Source, "acme", "svc", "kafka-in")
        .await
        .expect("versions returns 200");
    assert_eq!(versions, vec!["1.0.0".to_owned(), "1.1.0".to_owned()]);
}

#[tokio::test]
async fn package_metadata_get_returns_envelope() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/admin/v3/packages/sink/acme/svc/jdbc-out/1.0.0/metadata",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "description": "JDBC sink pinned for prod",
            "contact": "team-data@acme.example",
            "modificationTime": 1_700_000_000_000_i64,
            "properties": { "ci": "build-9876" },
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let meta = admin
        .package_metadata_get(PackageType::Sink, "acme", "svc", "jdbc-out", "1.0.0")
        .await
        .expect("metadata get returns 200");
    assert_eq!(meta["description"], "JDBC sink pinned for prod");
    assert_eq!(meta["properties"]["ci"], "build-9876");
}

#[tokio::test]
async fn package_metadata_set_pins_body_shape() {
    let mock = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path(
            "/admin/v3/packages/function/acme/svc/enrich/1.0.0/metadata",
        ))
        .and(body_json(serde_json::json!({
            "description": "enrich function",
            "contact": "ops@acme.example",
            "modificationTime": 0,
            "properties": { "owner": "team-a" },
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let mut props = HashMap::new();
    props.insert("owner".to_owned(), "team-a".to_owned());
    admin
        .package_metadata_set(
            PackageType::Function,
            "acme",
            "svc",
            "enrich",
            "1.0.0",
            PackageMetadata {
                description: "enrich function".to_owned(),
                contact: "ops@acme.example".to_owned(),
                modification_time: 0,
                properties: props,
            },
        )
        .await
        .expect("metadata set returns 204");
}

#[tokio::test]
async fn package_delete_strips_version_only() {
    let mock = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/v3/packages/sink/acme/svc/jdbc-out/1.0.0"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .package_delete(PackageType::Sink, "acme", "svc", "jdbc-out", "1.0.0")
        .await
        .expect("delete returns 204");
}
