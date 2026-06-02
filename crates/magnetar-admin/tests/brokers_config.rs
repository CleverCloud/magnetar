// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the brokers configuration REST endpoints —
//! dynamic-config key listing, override map, runtime view, and
//! internal-stack endpoints.
//!
//! These pin the exact path and verb against `BrokersBase` in
//! `pulsar-broker/.../v2/Brokers.java`. Response payloads stay as
//! `serde_json::Value` (or `Vec<String>` for the bare key list)
//! because broker minor versions extend `ServiceConfiguration` with
//! new keys and `InternalConfigurationData` with new fields — a typed
//! Rust struct would forward-break.

use magnetar_admin::AdminClient;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn brokers_dynamic_config_keys_returns_string_list() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/brokers/configuration"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            "brokerShutdownTimeoutMs",
            "loadBalancerEnabled",
        ])))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let keys = admin
        .brokers_dynamic_config_keys()
        .await
        .expect("keys returns 200 + JSON array");
    assert_eq!(
        keys,
        vec![
            "brokerShutdownTimeoutMs".to_owned(),
            "loadBalancerEnabled".to_owned(),
        ]
    );
}

#[tokio::test]
async fn brokers_dynamic_config_keys_accepts_object_shape() {
    // Some Pulsar surfaces (Function Worker, proxy splits) emit the
    // dynamic-configuration endpoint as the underlying
    // `Map<String, ConfigField>` rather than the documented
    // `List<String>`. We accept both — object → keys — so the CLI
    // doesn't break on those deployments.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/brokers/configuration"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "brokerShutdownTimeoutMs": {"type": "long", "doc": "shutdown grace"},
            "loadBalancerEnabled":    {"type": "boolean", "doc": "lb master switch"},
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let mut keys = admin
        .brokers_dynamic_config_keys()
        .await
        .expect("keys returns 200 + JSON object");
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "brokerShutdownTimeoutMs".to_owned(),
            "loadBalancerEnabled".to_owned(),
        ]
    );
}

#[tokio::test]
async fn brokers_dynamic_config_overrides_returns_map() {
    let mock = MockServer::start().await;
    // `getAllDynamicConfigurations` returns only the keys an operator
    // has overridden — static / default values stay out of the map.
    Mock::given(method("GET"))
        .and(path("/admin/v2/brokers/configuration/values"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "brokerShutdownTimeoutMs": "5000",
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let overrides = admin
        .brokers_dynamic_config_overrides()
        .await
        .expect("overrides returns 200");
    assert_eq!(overrides["brokerShutdownTimeoutMs"], "5000");
}

#[tokio::test]
async fn brokers_runtime_config_returns_merged_map() {
    let mock = MockServer::start().await;
    // The runtime view carries every `ServiceConfiguration` key with
    // its currently-applied value — static defaults plus any operator
    // override. We assert two representative keys to confirm the path
    // is wired without depending on the full upstream key set.
    Mock::given(method("GET"))
        .and(path("/admin/v2/brokers/configuration/runtime"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "brokerShutdownTimeoutMs": "5000",
            "clusterName": "standalone",
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let runtime = admin
        .brokers_runtime_config()
        .await
        .expect("runtime returns 200");
    assert_eq!(runtime["clusterName"], "standalone");
    assert_eq!(runtime["brokerShutdownTimeoutMs"], "5000");
}

#[tokio::test]
async fn brokers_set_dynamic_config_uses_path_params_only() {
    // `updateDynamicConfiguration` takes both name and value as path
    // segments — there is no request body. The POST returns 204.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(
            "/admin/v2/brokers/configuration/brokerShutdownTimeoutMs/5000",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .brokers_set_dynamic_config("brokerShutdownTimeoutMs", "5000")
        .await
        .expect("set-dynamic-config returns 204");
}

#[tokio::test]
async fn brokers_delete_dynamic_config_drops_override() {
    let mock = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path(
            "/admin/v2/brokers/configuration/brokerShutdownTimeoutMs",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .brokers_delete_dynamic_config("brokerShutdownTimeoutMs")
        .await
        .expect("delete-dynamic-config returns 204");
}

#[tokio::test]
async fn brokers_internal_config_returns_metadata_endpoints() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/brokers/internal-configuration"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "zookeeperServers": "zk-1:2181,zk-2:2181",
            "configurationMetadataStoreUrl": "zk-1:2181,zk-2:2181",
            "ledgersRootPath": "/ledgers",
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let internal = admin
        .brokers_internal_config()
        .await
        .expect("internal-config returns 200");
    assert_eq!(internal["zookeeperServers"], "zk-1:2181,zk-2:2181");
    assert_eq!(internal["ledgersRootPath"], "/ledgers");
}
