// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for broker / cluster diagnostic REST endpoints.
//!
//! These pin the exact path and verb against the upstream Apache Pulsar
//! admin REST surface — `BrokersBase` and `ClustersBase` in
//! `pulsar-broker/.../v2/`. Response payloads stay as `serde_json::Value`
//! because broker minor versions add fields (e.g. `clusterName` on
//! `LeaderBroker` since Pulsar 3.0); a typed Rust struct would
//! forward-break.

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
async fn brokers_list_returns_host_port_strings() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/brokers/standalone"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!(["broker-a:8080", "broker-b:8080",])),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let brokers = admin
        .brokers_list("standalone")
        .await
        .expect("list returns 200 + JSON array");
    assert_eq!(
        brokers,
        vec!["broker-a:8080".to_owned(), "broker-b:8080".to_owned()]
    );
}

#[tokio::test]
async fn brokers_leader_returns_raw_json() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/brokers/leaderBroker"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "serviceUrl": "http://broker-a:8080",
            "brokerId": "broker-a:8080",
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let leader = admin.brokers_leader().await.expect("leader returns 200");
    assert_eq!(leader["serviceUrl"], "http://broker-a:8080");
    assert_eq!(leader["brokerId"], "broker-a:8080");
}

#[tokio::test]
async fn cluster_failure_domains_list_returns_map() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/clusters/standalone/failureDomains"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "rack-a": { "brokers": ["broker-a:8080"] },
            "rack-b": { "brokers": ["broker-b:8080"] },
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let map = admin
        .cluster_failure_domains_list("standalone")
        .await
        .expect("list returns 200");
    assert!(map.get("rack-a").is_some());
    assert!(map.get("rack-b").is_some());
}

#[tokio::test]
async fn cluster_failure_domain_get_returns_one_domain() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/clusters/standalone/failureDomains/rack-a"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "brokers": ["broker-a:8080"],
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let dom = admin
        .cluster_failure_domain_get("standalone", "rack-a")
        .await
        .expect("get returns 200");
    assert_eq!(dom["brokers"][0], "broker-a:8080");
}

#[tokio::test]
async fn namespace_isolation_policies_list_returns_map() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/clusters/standalone/namespaceIsolationPolicies",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "policy-a": {
                "namespaces": ["acme/svc"],
                "primary": ["broker-a:8080"],
                "secondary": ["broker-b:8080"],
                "auto_failover_policy": {
                    "policy_type": "min_available",
                    "parameters": { "min_limit": "1", "usage_threshold": "80" },
                }
            }
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let map = admin
        .namespace_isolation_policies_list("standalone")
        .await
        .expect("list returns 200");
    assert_eq!(map["policy-a"]["primary"][0], "broker-a:8080");
}

#[tokio::test]
async fn namespace_isolation_policies_list_404_returns_empty_map() {
    // Pulsar 4 surfaces "no isolation policies on this cluster" as 404
    // with a `NamespaceIsolationPolicies for cluster X does not exist`
    // body, not an empty `{}`. We pin the Java-client semantic: empty
    // configuration = empty map, never an error.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/clusters/standalone/namespaceIsolationPolicies",
        ))
        .respond_with(ResponseTemplate::new(404).set_body_string(
            r#"{"reason":"NamespaceIsolationPolicies for cluster standalone does not exist"}"#,
        ))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let map = admin
        .namespace_isolation_policies_list("standalone")
        .await
        .expect("404 with the well-known body must not surface as Status error");
    assert!(map.is_object(), "expected empty `{{}}`, got {map}");
    assert_eq!(map.as_object().map(|m| m.len()), Some(0));
}
