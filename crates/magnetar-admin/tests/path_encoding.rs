// SPDX-License-Identifier: Apache-2.0

//! Percent-encoding sweep across the admin endpoint families.
//!
//! Every admin verb interpolates its name segments through the one shared
//! builder (`AdminClient::url_for`), so RFC 3986 conformance is a property of
//! that builder rather than of any single verb. These tests pin that property
//! at the *public API* boundary for one representative endpoint per family —
//! tenant, namespace, topic, subscription (v2) and function, package (v3) —
//! so a future refactor that reintroduces a per-verb path cannot regress one
//! family silently.
//!
//! `[`, `]`, `^` and `|` are the four printable ASCII bytes that RFC 3986
//! forbids in a path while the `url` crate's WHATWG encode set lets through.
//! Pulsar's Jetty front end answers a raw one with `400 Illegal Path
//! Character` at URI-parse time, before routing — so the failure carries no
//! broker `reason` and reads as an unexplained empty-bodied 400.

use magnetar_admin::AdminClient;
use wiremock::{Mock, MockServer, ResponseTemplate};

/// The name fragment under test, and the bytes it must become on the wire.
const RAW: &str = "n|a[m]e^x";
const ENC: &str = "n%7Ca%5Bm%5De%5Ex";

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

/// Mount a catch-all returning `body`, run `f`, and return the path the client
/// actually put on the wire (percent-encoded, as `Url::path` preserves).
async fn captured_path<F, Fut>(body: serde_json::Value, f: F) -> String
where
    F: FnOnce(AdminClient) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mock = MockServer::start().await;
    Mock::given(wiremock::matchers::any())
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&mock)
        .await;
    let admin = client(&mock);
    f(admin).await;
    let requests = mock.received_requests().await.expect("recording enabled");
    requests[0].url.path().to_owned()
}

#[tokio::test]
async fn tenant_segment_is_percent_encoded() {
    let path = captured_path(serde_json::json!([]), |admin| async move {
        let _ = admin.namespaces_list(RAW).await;
    })
    .await;
    assert_eq!(path, format!("/admin/v2/namespaces/{ENC}"));
}

#[tokio::test]
async fn namespace_segment_is_percent_encoded() {
    let path = captured_path(serde_json::json!({}), |admin| async move {
        let _ = admin.namespace_get_retention(&format!("acme/{RAW}")).await;
    })
    .await;
    assert_eq!(path, format!("/admin/v2/namespaces/acme/{ENC}/retention"));
}

#[tokio::test]
async fn topic_segment_is_percent_encoded() {
    let path = captured_path(serde_json::json!([]), |admin| async move {
        let _ = admin.subscriptions_list(&format!("acme/svc/{RAW}")).await;
    })
    .await;
    assert_eq!(
        path,
        format!("/admin/v2/persistent/acme/svc/{ENC}/subscriptions"),
    );
}

/// The reported bug: `magnetarctl admin subscriptions delete … 'name|app_id'`.
#[tokio::test]
async fn subscription_segment_is_percent_encoded() {
    let path = captured_path(serde_json::json!({}), |admin| async move {
        let _ = admin
            .subscription_delete("acme/svc/orders", RAW, false)
            .await;
    })
    .await;
    assert_eq!(
        path,
        format!("/admin/v2/persistent/acme/svc/orders/subscription/{ENC}"),
    );
}

/// `/admin/v3/` endpoints share the same builder, so they inherit the fix.
#[tokio::test]
async fn v3_function_segment_is_percent_encoded() {
    let path = captured_path(serde_json::json!({}), |admin| async move {
        let _ = admin.function_get("acme", "svc", RAW).await;
    })
    .await;
    assert_eq!(path, format!("/admin/v3/functions/acme/svc/{ENC}"));
}

/// All three name-bearing segments of one path encode independently — a single
/// escaped segment is not enough if the builder stops after the first.
#[tokio::test]
async fn every_segment_of_a_path_is_encoded_independently() {
    let path = captured_path(serde_json::json!([]), |admin| async move {
        let _ = admin.subscriptions_list("t|1/n|2/topic|3").await;
    })
    .await;
    assert_eq!(
        path,
        "/admin/v2/persistent/t%7C1/n%7C2/topic%7C3/subscriptions",
    );
}

/// Constant segments and ordinary names must stay byte-identical, so the fix
/// changes nothing for the paths that already worked.
#[tokio::test]
async fn ordinary_names_are_unchanged() {
    let path = captured_path(serde_json::json!([]), |admin| async move {
        let _ = admin
            .subscriptions_list("public/default/persistent-orders_v2.retry")
            .await;
    })
    .await;
    assert_eq!(
        path,
        "/admin/v2/persistent/public/default/persistent-orders_v2.retry/subscriptions",
    );
}
