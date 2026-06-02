// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the Pulsar IO Sources REST endpoints
//! (`/admin/v3/sources/...`) — list / get / status / create-with-url /
//! delete / start / stop / restart.
//!
//! Pins the path, verb, and (where useful) JSON body shape against
//! `SourcesBase` in `pulsar-broker/.../v3/Sources.java`. The connector
//! envelope (`SourceStatus`, the stored `SourceConfig`) is decoded as
//! `serde_json::Value` because broker minor versions add fields under
//! the connector-specific `configs` map.

use magnetar_admin::{AdminClient, SourceConfig};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

fn sample_config() -> SourceConfig {
    SourceConfig {
        tenant: "acme".to_owned(),
        namespace: "svc".to_owned(),
        name: "kafka-in".to_owned(),
        class_name: "org.apache.pulsar.io.kafka.KafkaSource".to_owned(),
        topic_name: "persistent://acme/svc/ingest".to_owned(),
        parallelism: 2,
        configs: Some(serde_json::json!({ "bootstrapServers": "kafka:9092" })),
    }
}

#[tokio::test]
async fn sources_list_by_namespace_returns_names() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/sources/acme/svc"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!(["kafka-in", "jdbc-in",])),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let names = admin
        .sources_list_by_namespace("acme", "svc")
        .await
        .expect("list returns 200");
    assert_eq!(names, vec!["kafka-in".to_owned(), "jdbc-in".to_owned()]);
}

#[tokio::test]
async fn source_get_returns_raw_envelope() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/sources/acme/svc/kafka-in"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tenant": "acme",
            "namespace": "svc",
            "name": "kafka-in",
            "className": "org.apache.pulsar.io.kafka.KafkaSource",
            "topicName": "persistent://acme/svc/ingest",
            "parallelism": 2,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let info = admin
        .source_get("acme", "svc", "kafka-in")
        .await
        .expect("get returns 200");
    assert_eq!(info["className"], "org.apache.pulsar.io.kafka.KafkaSource");
    assert_eq!(info["parallelism"], 2);
}

#[tokio::test]
async fn source_status_returns_running_state() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/sources/acme/svc/kafka-in/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "numInstances": 2,
            "numRunning": 2,
            "instances": [
                { "instanceId": 0, "status": { "running": true } },
                { "instanceId": 1, "status": { "running": true } },
            ],
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let status = admin
        .source_status("acme", "svc", "kafka-in")
        .await
        .expect("status returns 200");
    assert_eq!(status["numInstances"], 2);
    assert_eq!(status["numRunning"], 2);
}

#[tokio::test]
async fn source_create_with_url_sends_multipart_form() {
    let mock = MockServer::start().await;
    // The broker insists on `multipart/form-data`; we pin the
    // Content-Type prefix (the boundary suffix varies) and the verb +
    // path, then trust the multipart body framing.
    Mock::given(method("POST"))
        .and(path("/admin/v3/sources/acme/svc/kafka-in"))
        .and(header_prefix("content-type", "multipart/form-data"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .source_create_with_url(
            "acme",
            "svc",
            "kafka-in",
            "https://repo.example/pulsar-io-kafka.nar",
            sample_config(),
        )
        .await
        .expect("create-with-url returns 204");
}

#[tokio::test]
async fn source_update_with_url_uses_put() {
    let mock = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/admin/v3/sources/acme/svc/kafka-in"))
        .and(header_prefix("content-type", "multipart/form-data"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .source_update_with_url(
            "acme",
            "svc",
            "kafka-in",
            "https://repo.example/pulsar-io-kafka.nar",
            sample_config(),
        )
        .await
        .expect("update-with-url returns 204");
}

#[tokio::test]
async fn source_delete_returns_204() {
    let mock = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/v3/sources/acme/svc/kafka-in"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .source_delete("acme", "svc", "kafka-in")
        .await
        .expect("delete returns 204");
}

#[tokio::test]
async fn source_start_stop_restart_round_trip() {
    let mock = MockServer::start().await;
    for verb in ["start", "stop", "restart"] {
        Mock::given(method("POST"))
            .and(path(format!("/admin/v3/sources/acme/svc/kafka-in/{verb}")))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&mock)
            .await;
    }

    let admin = client(&mock);
    admin
        .source_start("acme", "svc", "kafka-in")
        .await
        .expect("start returns 204");
    admin
        .source_stop("acme", "svc", "kafka-in")
        .await
        .expect("stop returns 204");
    admin
        .source_restart("acme", "svc", "kafka-in")
        .await
        .expect("restart returns 204");
}

/// `wiremock`'s built-in `header(name, value)` matcher requires an
/// exact match. The Content-Type emitted by reqwest's multipart form
/// always carries a `; boundary=...` suffix we cannot pin, so we wrap
/// a prefix-aware matcher here. Using `header(name, _)` (any value)
/// would weaken the assertion to "header present", which would not
/// catch a regression that emits, say, `application/x-www-form-urlencoded`.
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

