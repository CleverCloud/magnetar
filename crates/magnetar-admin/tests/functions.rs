// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the Pulsar Functions REST surface — list,
//! get, status, stats, URL-based register, lifecycle (start / stop /
//! restart / delete).
//!
//! Each test pins the exact path, verb, and (for write methods) the
//! body shape Java's `FunctionsBase` advertises (`@Path("/{tenant}/
//! {namespace}/{functionName}")` and friends in
//! `pulsar-broker/.../v3/Functions.java`). Response payloads stay as
//! `serde_json::Value` — the upstream `FunctionConfig` and
//! `FunctionStatus` envelopes are large and grow on every minor
//! release.

use magnetar_admin::{AdminClient, FunctionConfig};
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn functions_list_by_namespace_returns_names() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/functions/public/default"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(["fn-a", "fn-b"])))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let names = admin
        .functions_list_by_namespace("public", "default")
        .await
        .expect("list returns 200");
    assert_eq!(names, vec!["fn-a".to_owned(), "fn-b".to_owned()]);
}

#[tokio::test]
async fn function_get_returns_envelope() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/functions/public/default/my-fn"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tenant": "public",
            "namespace": "default",
            "name": "my-fn",
            "className": "com.acme.MyFunction",
            "runtime": "JAVA",
            "parallelism": 2,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let cfg = admin
        .function_get("public", "default", "my-fn")
        .await
        .expect("get returns 200");
    assert_eq!(cfg["className"], "com.acme.MyFunction");
    assert_eq!(cfg["parallelism"], 2);
}

#[tokio::test]
async fn function_status_returns_aggregate() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/functions/public/default/my-fn/status"))
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
        .function_status("public", "default", "my-fn")
        .await
        .expect("status returns 200");
    assert_eq!(status["numInstances"], 2);
    assert_eq!(status["numRunning"], 2);
}

#[tokio::test]
async fn function_stats_returns_envelope() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/functions/public/default/my-fn/stats"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "receivedTotal": 1000,
            "processedSuccessfullyTotal": 990,
            "systemExceptionsTotal": 10,
            "avgProcessLatency": 12.5,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let stats = admin
        .function_stats("public", "default", "my-fn")
        .await
        .expect("stats returns 200");
    assert_eq!(stats["receivedTotal"], 1000);
    assert_eq!(stats["processedSuccessfullyTotal"], 990);
}

#[tokio::test]
async fn function_instance_status_addresses_one_instance() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v3/functions/public/default/my-fn/3/status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "instanceId": 3,
            "status": { "running": true, "numRestarts": 0 },
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let status = admin
        .function_instance_status("public", "default", "my-fn", 3)
        .await
        .expect("instance status returns 200");
    assert_eq!(status["instanceId"], 3);
}

#[tokio::test]
async fn function_create_with_url_sends_multipart_envelope() {
    let mock = MockServer::start().await;
    // `multipart/form-data` carries the `url=` and `functionConfig=`
    // parts inline as text bodies separated by a boundary. wiremock
    // doesn't parse the envelope, but the body always contains the two
    // form-field names + the values verbatim — enough to pin the wire
    // shape against `FunctionsBase#registerFunction(..., @FormDataParam
    // "url", @FormDataParam "functionConfig")`.
    Mock::given(method("POST"))
        .and(path("/admin/v3/functions/public/default/my-fn"))
        .and(body_string_contains("name=\"url\""))
        .and(body_string_contains("https://example.test/my-fn.jar"))
        .and(body_string_contains("name=\"functionConfig\""))
        .and(body_string_contains(
            "\"className\":\"com.acme.MyFunction\"",
        ))
        .and(body_string_contains("\"runtime\":\"JAVA\""))
        .and(body_string_contains("\"parallelism\":2"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let cfg = FunctionConfig {
        tenant: "public".into(),
        namespace: "default".into(),
        name: "my-fn".into(),
        class_name: "com.acme.MyFunction".into(),
        inputs: vec!["persistent://public/default/in".into()],
        output: "persistent://public/default/out".into(),
        runtime: "JAVA".into(),
        parallelism: 2,
        user_config: None,
    };
    admin
        .function_create_with_url(
            "public",
            "default",
            "my-fn",
            "https://example.test/my-fn.jar",
            cfg,
        )
        .await
        .expect("create-with-url returns 204");
}

#[tokio::test]
async fn function_update_with_url_uses_put() {
    let mock = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/admin/v3/functions/public/default/my-fn"))
        .and(body_string_contains("name=\"url\""))
        .and(body_string_contains("name=\"functionConfig\""))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .function_update_with_url(
            "public",
            "default",
            "my-fn",
            "https://example.test/my-fn-v2.jar",
            FunctionConfig {
                tenant: "public".into(),
                namespace: "default".into(),
                name: "my-fn".into(),
                class_name: "com.acme.MyFunction".into(),
                runtime: "JAVA".into(),
                parallelism: 4,
                ..Default::default()
            },
        )
        .await
        .expect("update-with-url returns 204");
}

#[tokio::test]
async fn function_delete_round_trip() {
    let mock = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/v3/functions/public/default/my-fn"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .function_delete("public", "default", "my-fn")
        .await
        .expect("delete returns 204");
}

#[tokio::test]
async fn function_start_stop_restart_round_trip() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/admin/v3/functions/public/default/my-fn/start"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v3/functions/public/default/my-fn/stop"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v3/functions/public/default/my-fn/restart"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .function_start("public", "default", "my-fn")
        .await
        .expect("start returns 204");
    admin
        .function_stop("public", "default", "my-fn")
        .await
        .expect("stop returns 204");
    admin
        .function_restart("public", "default", "my-fn")
        .await
        .expect("restart returns 204");
}

#[tokio::test]
async fn function_start_stop_instance_round_trip() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/admin/v3/functions/public/default/my-fn/2/start"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/v3/functions/public/default/my-fn/2/stop"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .function_start_instance("public", "default", "my-fn", 2)
        .await
        .expect("start-instance returns 204");
    admin
        .function_stop_instance("public", "default", "my-fn", 2)
        .await
        .expect("stop-instance returns 204");
}
