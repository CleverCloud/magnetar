// SPDX-License-Identifier: Apache-2.0

//! Construction-only tests for the admin REST client.
//!
//! These do not exchange HTTP traffic — there is no mockito dependency. They
//! exercise builder semantics and the small set of name-splitting helpers the
//! public API depends on. URL/verb correctness is asserted by reading the
//! Java endpoint annotations cited in the rustdoc; round-trip wire tests live
//! in M11 (e2e) once the broker fake is wired in.

use magnetar_admin::{AdminAuth, AdminClient, AdminError, TenantInfo};

#[test]
fn builder_defaults_no_auth() {
    let client = AdminClient::builder()
        .service_url("http://localhost:8080".parse().unwrap())
        .build()
        .unwrap();
    assert!(matches!(client.auth(), AdminAuth::None));
}

#[test]
fn builder_with_https_url() {
    let client = AdminClient::builder()
        .service_url("https://broker.example:443".parse().unwrap())
        .build()
        .unwrap();
    assert_eq!(
        client.base_url().as_str(),
        "https://broker.example/admin/v2/"
    );
}

#[test]
fn builder_with_trailing_slash() {
    let client = AdminClient::builder()
        .service_url("http://localhost:8080/".parse().unwrap())
        .build()
        .unwrap();
    assert_eq!(
        client.base_url().as_str(),
        "http://localhost:8080/admin/v2/"
    );
}

#[test]
fn builder_with_path_prefix_is_preserved() {
    // The previous build path called `Url::join("admin/v2/")` directly,
    // which per WHATWG semantics REPLACES the last path segment when
    // the base has no trailing slash — `http://host/something` +
    // `admin/v2/` collapsed to `http://host/admin/v2/`. The builder now
    // normalises the trailing slash first so a path-prefixed admin URL
    // (`http://gateway/pulsar`, common K8s ingress shape) survives the
    // join intact.
    let client = AdminClient::builder()
        .service_url("http://localhost:8080/pulsar".parse().unwrap())
        .build()
        .unwrap();
    assert_eq!(
        client.base_url().as_str(),
        "http://localhost:8080/pulsar/admin/v2/"
    );
}

#[test]
fn tenant_info_serializes_with_java_keys() {
    let info = TenantInfo {
        admin_roles: vec!["admin".into()],
        allowed_clusters: vec!["standalone".into()],
    };
    let json = serde_json::to_string(&info).unwrap();
    assert!(json.contains("\"adminRoles\""));
    assert!(json.contains("\"allowedClusters\""));
}

#[test]
fn tenant_info_deserializes_with_java_keys() {
    let json = r#"{"adminRoles":["a"],"allowedClusters":["c"]}"#;
    let info: TenantInfo = serde_json::from_str(json).unwrap();
    assert_eq!(info.admin_roles, vec!["a".to_owned()]);
    assert_eq!(info.allowed_clusters, vec!["c".to_owned()]);
}

#[test]
fn builder_requires_url() {
    let err = AdminClient::builder().build().unwrap_err();
    assert!(matches!(err, AdminError::Builder(_)));
}

#[test]
fn builder_timeout_accepts_value() {
    // The builder swallows the timeout into the inner reqwest client — there's
    // no public getter, but we can at least assert that `build` succeeds with a
    // custom timeout.
    let client = AdminClient::builder()
        .service_url("http://localhost:8080".parse().unwrap())
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();
    assert_eq!(
        client.base_url().as_str(),
        "http://localhost:8080/admin/v2/"
    );
}
