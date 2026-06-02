// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the Pulsar IO Sinks REST endpoints
//! (`/admin/v3/sinks/...`) — list / get / status / create-with-url /
//! delete / start / stop / restart.
//!
//! Pins the path, verb, and multipart Content-Type prefix against
//! `SinksBase` in `pulsar-broker/.../v3/Sinks.java`. Mirrors the
//! Sources test layout — the two surfaces are intentionally symmetric.

use magnetar_admin::{AdminClient, SinkConfig};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

fn sample_config() -> SinkConfig {
    SinkConfig {
        tenant: "acme".to_owned(),
        namespace: "svc".to_owned(),
        name: "jdbc-out".to_owned(),
        class_name: "org.apache.pulsar.io.jdbc.PostgresJdbcAutoSchemaSink".to_owned(),
        inputs: vec!["persistent://acme/svc/orders".to_owned()],
        parallelism: 1,
        configs: Some(serde_json::json!({ "jdbcUrl": "jdbc:postgresql://db:5432/app" })),
    }
}

#[tokio::test]
async fn sinks_list_by_namespace_returns_names() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/sinks/acme/svc"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!(["jdbc-out", "s3-out"])),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let names = admin
        .sinks_list_by_namespace("acme", "svc")
        .await
        .expect("list returns 200");
    assert_eq!(names, vec!["jdbc-out".to_owned(), "s3-out".to_owned()]);
}

#[tokio::test]
async fn sink_get_returns_raw_envelope() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/sinks/acme/svc/jdbc-out"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tenant": "acme",
            "namespace": "svc",
            "name": "jdbc-out",
            "className": "org.apache.pulsar.io.jdbc.PostgresJdbcAutoSchemaSink",
            "inputs": ["persistent://acme/svc/orders"],
            "parallelism": 1,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let info = admin
        .sink_get("acme", "svc", "jdbc-out")
        .await
        .expect("get returns 200");
    assert_eq!(info["inputs"][0], "persistent://acme/svc/orders");
    assert_eq!(info["parallelism"], 1);
}

#[tokio::test]
async fn sink_status_returns_running_state() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/sinks/acme/svc/jdbc-out/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "numInstances": 1,
            "numRunning": 1,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let status = admin
        .sink_status("acme", "svc", "jdbc-out")
        .await
        .expect("status returns 200");
    assert_eq!(status["numInstances"], 1);
}

#[tokio::test]
async fn sink_create_with_url_sends_multipart_form() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/admin/v3/sinks/acme/svc/jdbc-out"))
        .and(header_prefix("content-type", "multipart/form-data"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .sink_create_with_url(
            "acme",
            "svc",
            "jdbc-out",
            "https://repo.example/pulsar-io-jdbc.nar",
            sample_config(),
        )
        .await
        .expect("create-with-url returns 204");
}

#[tokio::test]
async fn sink_update_with_url_uses_put() {
    let mock = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/admin/v3/sinks/acme/svc/jdbc-out"))
        .and(header_prefix("content-type", "multipart/form-data"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .sink_update_with_url(
            "acme",
            "svc",
            "jdbc-out",
            "https://repo.example/pulsar-io-jdbc.nar",
            sample_config(),
        )
        .await
        .expect("update-with-url returns 204");
}

#[tokio::test]
async fn sink_delete_returns_204() {
    let mock = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/v3/sinks/acme/svc/jdbc-out"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .sink_delete("acme", "svc", "jdbc-out")
        .await
        .expect("delete returns 204");
}

#[tokio::test]
async fn sink_start_stop_restart_round_trip() {
    let mock = MockServer::start().await;
    for verb in ["start", "stop", "restart"] {
        Mock::given(method("POST"))
            .and(path(format!("/admin/v3/sinks/acme/svc/jdbc-out/{verb}")))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&mock)
            .await;
    }

    let admin = client(&mock);
    admin
        .sink_start("acme", "svc", "jdbc-out")
        .await
        .expect("start returns 204");
    admin
        .sink_stop("acme", "svc", "jdbc-out")
        .await
        .expect("stop returns 204");
    admin
        .sink_restart("acme", "svc", "jdbc-out")
        .await
        .expect("restart returns 204");
}

/// `wiremock`'s built-in `header(name, value)` matcher requires an
/// exact match. The Content-Type emitted by reqwest's multipart form
/// carries a `; boundary=...` suffix we cannot pin, so we wrap a
/// prefix-aware matcher here. See `tests/sources.rs` for the
/// reasoning — kept duplicated rather than hoisted to keep the
/// integration tests self-contained (cargo doesn't ship a "tests
/// common module" pattern that survives `cargo test --test sinks` in
/// isolation without a `pub mod`).
fn header_prefix(
    name: &'static str,
    prefix: &'static str,
) -> impl wiremock::Match + Send + Sync + 'static {
    HeaderPrefixMatcher { name, prefix }
}

struct HeaderPrefixMatcher {
    name: &'static str,
    prefix: &'static str,
}

impl wiremock::Match for HeaderPrefixMatcher {
    fn matches(&self, request: &wiremock::Request) -> bool {
        request
            .headers
            .get(self.name)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with(self.prefix))
    }
}
