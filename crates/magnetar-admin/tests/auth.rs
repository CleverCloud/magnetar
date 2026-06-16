// SPDX-License-Identifier: Apache-2.0

//! Auth-header tests for the admin client — the `Token` bearer regression
//! and the `OAuth2` `client_credentials` fetch → bearer path.
//!
//! Both assert the exact `Authorization: Bearer <…>` header reaches the
//! broker. The `OAuth2` case wires a second wiremock as the IDP token endpoint
//! so the whole fetch → cache → attach chain runs without a real IDP.

use std::sync::Arc;

use magnetar_admin::AdminClient;
use magnetar_auth_oauth2::{ClientCredentialsFlow, Credentials};
use wiremock::matchers::{body_string_contains, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Regression: a `token`-configured client attaches `Authorization: Bearer
/// <token>` to every admin call. This is the pre-existing behavior the
/// `OAuth2` refactor must not break.
#[tokio::test]
async fn token_auth_attaches_bearer_header() {
    let broker = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/clusters"))
        .and(header("authorization", "Bearer secret-token-123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(["cl-1"])))
        .expect(1)
        .mount(&broker)
        .await;

    let admin = AdminClient::builder()
        .service_url(broker.uri().parse().unwrap())
        .token("secret-token-123".to_owned())
        .build()
        .expect("build admin client");

    let clusters = admin
        .cluster_list()
        .await
        .expect("cluster list returns 200");
    assert_eq!(clusters, vec!["cl-1".to_owned()]);
}

/// `OAuth2`: the client performs a `client_credentials` exchange against the
/// IDP, caches the access token, then attaches it as a bearer credential on
/// the admin call. Pins both the IDP `grant_type=client_credentials` request
/// and the broker-side `Authorization: Bearer <access-token>` header.
#[tokio::test]
async fn oauth2_auth_fetches_token_then_attaches_bearer() {
    // The IDP token endpoint — returns a fixed access token.
    let idp = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("grant_type=client_credentials"))
        .and(body_string_contains("client_id=admin-client"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "idp-issued-access-token",
            "expires_in": 3600,
            "token_type": "Bearer"
        })))
        .expect(1)
        .mount(&idp)
        .await;

    // The broker — expects the IDP-issued token as a bearer credential.
    let broker = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/clusters"))
        .and(header("authorization", "Bearer idp-issued-access-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(["cl-oauth"])))
        .expect(1)
        .mount(&broker)
        .await;

    let flow = ClientCredentialsFlow::builder()
        // The issuer is only used to derive the default token endpoint; we
        // override the endpoint to the wiremock IDP directly.
        .issuer_url("https://idp.example/realms/test".parse().unwrap())
        .token_endpoint(format!("{}/token", idp.uri()).parse().unwrap())
        .audience("urn:pulsar:broker")
        .credentials(Credentials::ClientSecret {
            client_id: "admin-client".to_owned(),
            client_secret: "admin-secret".to_owned(),
        })
        .build()
        .expect("build oauth2 flow");

    let admin = AdminClient::builder()
        .service_url(broker.uri().parse().unwrap())
        .oauth2(Arc::new(flow))
        .build()
        .expect("build admin client");

    let clusters = admin
        .cluster_list()
        .await
        .expect("cluster list returns 200");
    assert_eq!(clusters, vec!["cl-oauth".to_owned()]);
}

/// The TLS builder knobs (custom CA trust cert + allow-insecure) wire through
/// `build()` without error against a plain-HTTP mock. A faithful self-signed
/// HTTPS handshake test belongs to the e2e suite (it needs a cert fixture and
/// a real TLS listener); here we assert the builder accepts and applies the
/// options and the resulting client still works.
#[tokio::test]
async fn tls_builder_options_apply_cleanly() {
    let broker = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/clusters"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .expect(1)
        .mount(&broker)
        .await;

    // A self-signed CA PEM (test-only) — exercises `Certificate::from_pem`.
    let ca_pem = TEST_CA_PEM.as_bytes().to_vec();
    let admin = AdminClient::builder()
        .service_url(broker.uri().parse().unwrap())
        .tls_trust_cert_pem(ca_pem)
        .tls_allow_insecure(true)
        .build()
        .expect("build admin client with TLS options");

    let clusters = admin
        .cluster_list()
        .await
        .expect("cluster list returns 200");
    assert!(clusters.is_empty());
}

/// A throwaway self-signed CA certificate (PEM). Generated for this test
/// only; never trusted by any real deployment. Used solely to exercise
/// reqwest's `Certificate::from_pem` parse path.
const TEST_CA_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIBjDCCATGgAwIBAgIUemNWluoHpjirTKja7Gw+wVFlFWIwCgYIKoZIzj0EAwIw
GzEZMBcGA1UEAwwQbWFnbmV0YXItdGVzdC1jYTAeFw0yNjA2MTUyMDQ0MTNaFw0z
NjA2MTIyMDQ0MTNaMBsxGTAXBgNVBAMMEG1hZ25ldGFyLXRlc3QtY2EwWTATBgcq
hkjOPQIBBggqhkjOPQMBBwNCAATwIEPGpbT9u0SPTBR+SAhIUBy9GSrhPmlMH3Xp
4fQ8MUFi5KDxDV1s/JPw4Iv0JJx5WE7X0Wn/eZG9kFnZ6I5To1MwUTAdBgNVHQ4E
FgQUMolXG6aL9uxcHt4DlzEb+HGogtMwHwYDVR0jBBgwFoAUMolXG6aL9uxcHt4D
lzEb+HGogtMwDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNJADBGAiEA1z/z
Yg9awrqi95eIIAnLH3jCzopiA7vtxyR54zbT5LsCIQCwOK9PVJwjwhMxwtDC3h44
yxkLerhB+WdzfB1b3bMgUA==
-----END CERTIFICATE-----
";
