// SPDX-License-Identifier: Apache-2.0

//! Tests for the enriched decode / status diagnostics (issue #282).
//!
//! When a 2xx admin response is *not* the JSON the client expected — an
//! empty body, an HTML error page, or a plain-text proxy banner — the
//! client must surface the method, URL, HTTP status, content-type, and a
//! body snippet instead of the bare serde "expected value at line 1
//! column 1" message. These tests pin that contract against real
//! `AdminClient` methods using `json_ok` (`cluster_list`),
//! `json_ok_or_default` (`namespace_get_retention`), and
//! `json_ok_optional` (`namespace_get_message_ttl`).

use magnetar_admin::{AdminClient, AdminError};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

// --- Decode-error enrichment on a json_ok endpoint -----------------------

#[tokio::test]
async fn decode_error_on_empty_body_names_method_url_status() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/clusters"))
        // 200 with no body at all — the classic "wrong endpoint / proxy
        // swallowed the payload" failure mode.
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let err = admin.cluster_list().await.unwrap_err();

    match &err {
        AdminError::Decode {
            method,
            url,
            status,
            ..
        } => {
            assert_eq!(method, "GET");
            assert!(url.contains("/admin/v2/clusters"), "url was {url}");
            assert_eq!(*status, 200);
        }
        other => panic!("expected AdminError::Decode, got {other:?}"),
    }

    // The Display string must carry the diagnostic context and must NOT be
    // the bare serde message.
    let display = err.to_string();
    assert!(display.contains("GET"), "display: {display}");
    assert!(display.contains("/admin/v2/clusters"), "display: {display}");
    assert!(display.contains("HTTP 200"), "display: {display}");
    assert!(
        !display.contains("expected value at line 1 column 1"),
        "display still reads as the bare serde error: {display}"
    );
}

#[tokio::test]
async fn decode_error_on_html_body_names_content_type_and_snippet() {
    let mock = MockServer::start().await;
    let html = "<html><body><h1>502 Bad Gateway</h1></body></html>";
    Mock::given(method("GET"))
        .and(path("/admin/v2/clusters"))
        // `set_body_raw` sets the body *and* the content-type header in one
        // shot; `set_body_string` would override any pre-set content-type
        // with `text/plain`.
        .respond_with(ResponseTemplate::new(200).set_body_raw(html, "text/html"))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let err = admin.cluster_list().await.unwrap_err();

    match &err {
        AdminError::Decode {
            method,
            url,
            status,
            content_type,
            snippet,
            ..
        } => {
            assert_eq!(method, "GET");
            assert!(url.contains("/admin/v2/clusters"), "url was {url}");
            assert_eq!(*status, 200);
            assert!(
                content_type.contains("text/html"),
                "content_type was {content_type}"
            );
            assert!(snippet.contains("502 Bad Gateway"), "snippet was {snippet}");
        }
        other => panic!("expected AdminError::Decode, got {other:?}"),
    }
}

#[tokio::test]
async fn decode_error_on_plain_text_body_names_content_type_and_snippet() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/clusters"))
        .respond_with(ResponseTemplate::new(200).set_body_raw("not json", "text/plain"))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let err = admin.cluster_list().await.unwrap_err();

    match &err {
        AdminError::Decode {
            content_type,
            snippet,
            status,
            ..
        } => {
            assert_eq!(*status, 200);
            assert!(
                content_type.contains("text/plain"),
                "content_type was {content_type}"
            );
            assert!(snippet.contains("not json"), "snippet was {snippet}");
        }
        other => panic!("expected AdminError::Decode, got {other:?}"),
    }
}

#[tokio::test]
async fn decode_error_truncates_long_body_snippet() {
    let mock = MockServer::start().await;
    // 1 KiB of HTML — well past the 256-byte snippet cap.
    let big = format!("<html>{}</html>", "x".repeat(1024));
    Mock::given(method("GET"))
        .and(path("/admin/v2/clusters"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(big, "text/html"))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let err = admin.cluster_list().await.unwrap_err();

    match &err {
        AdminError::Decode { snippet, .. } => {
            assert!(
                snippet.contains("… (truncated)"),
                "long body should be truncated, snippet was {snippet}"
            );
        }
        other => panic!("expected AdminError::Decode, got {other:?}"),
    }
}

// --- Tolerant paths must NOT regress into a Decode error -----------------

#[tokio::test]
async fn no_content_on_or_default_endpoint_yields_default_not_decode() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/public/default/retention"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let policy = admin
        .namespace_get_retention("public/default")
        .await
        .expect("204 must fold to the default, not a Decode error");
    assert_eq!(policy.retention_time_in_minutes, 0);
    assert_eq!(policy.retention_size_in_mb, 0);
}

#[tokio::test]
async fn empty_body_on_or_default_endpoint_yields_default_not_decode() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/public/default/retention"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let policy = admin
        .namespace_get_retention("public/default")
        .await
        .expect("empty 200 body must fold to the default, not a Decode error");
    assert_eq!(policy.retention_time_in_minutes, 0);
    assert_eq!(policy.retention_size_in_mb, 0);
}

#[tokio::test]
async fn null_body_on_or_default_endpoint_yields_default_not_decode() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/public/default/retention"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_string("null"),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let policy = admin
        .namespace_get_retention("public/default")
        .await
        .expect("literal null must fold to the default, not a Decode error");
    assert_eq!(policy.retention_time_in_minutes, 0);
    assert_eq!(policy.retention_size_in_mb, 0);
}

#[tokio::test]
async fn no_content_on_optional_endpoint_yields_none_not_decode() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/public/default/messageTTL"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let ttl = admin
        .namespace_get_message_ttl("public/default")
        .await
        .expect("204 must fold to None, not a Decode error");
    assert_eq!(ttl, None);
}

#[tokio::test]
async fn empty_body_on_optional_endpoint_yields_none_not_decode() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/public/default/messageTTL"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let ttl = admin
        .namespace_get_message_ttl("public/default")
        .await
        .expect("empty 200 body must fold to None, not a Decode error");
    assert_eq!(ttl, None);
}

#[tokio::test]
async fn null_body_on_optional_endpoint_yields_none_not_decode() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/namespaces/public/default/messageTTL"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_string("null"),
        )
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let ttl = admin
        .namespace_get_message_ttl("public/default")
        .await
        .expect("literal null must fold to None, not a Decode error");
    assert_eq!(ttl, None);
}

// --- Status error now also names method + URL ----------------------------

#[tokio::test]
async fn status_error_names_method_and_url() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/clusters"))
        .respond_with(ResponseTemplate::new(404).set_body_string("cluster set not found"))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let err = admin.cluster_list().await.unwrap_err();

    match &err {
        AdminError::Status {
            method,
            url,
            code,
            body,
        } => {
            assert_eq!(method, "GET");
            assert!(url.contains("/admin/v2/clusters"), "url was {url}");
            assert_eq!(*code, 404);
            assert!(body.contains("cluster set not found"), "body was {body}");
        }
        other => panic!("expected AdminError::Status, got {other:?}"),
    }

    let display = err.to_string();
    assert!(display.contains("GET"), "display: {display}");
    assert!(display.contains("/admin/v2/clusters"), "display: {display}");
    assert!(display.contains("404"), "display: {display}");
}
