// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the `handshake_failure_reason` enrichment
//! against a real Apache Pulsar 4.x standalone broker.
//!
//! The test stands up a token-auth-enabled broker, connects with a
//! deliberately invalid JWT, and asserts that the resulting connect
//! failure carries:
//!
//! - the `"handshake failed"` envelope prefix the runtime crates emit once the proto-layer capture
//!   lands, AND
//! - some broker-side authentication marker (`"auth"`, `"token"`, or `"invalid"`).
//!
//! The substring assertion is intentionally tolerant — the broker's
//! exact rejection text varies across Pulsar versions (`AuthenticationError`,
//! `Unable to authenticate`, `Failed to authenticate with token …` …) and
//! the contract this test pins is the *envelope* (the proto-layer reason
//! reaches the façade error verbatim instead of being swallowed by the
//! generic `"handshake failed"` string), not the exact broker phrasing.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_handshake_error -- --nocapture
//! ```
//!
//! Requires Docker on the host.
//!
//! ## Image
//!
//! Uses `apachepulsar/pulsar:4.0.4` (Pulsar 4.0 LTS, our minimum supported
//! broker version). Override with `MAGNETAR_PULSAR_IMAGE_REPO` /
//! `MAGNETAR_PULSAR_IMAGE_TAG` env vars if you need a different tag
//! locally.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use magnetar::proto::TokenAuth;
use magnetar::{AuthProvider, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "4.0.4";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

/// Fixed 32-byte HS256 signing secret. Test-only. The broker is
/// configured with the same bytes via `tokenSecretKey=data:base64,…` so a
/// JWT signed with a *different* key is guaranteed to be rejected.
const TOKEN_SECRET: &[u8; 32] = b"magnetar-handshake-err-e2e-bytes";

fn image_repo() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_REPO").unwrap_or_else(|_| DEFAULT_IMAGE_REPO.to_owned())
}

fn image_tag() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_TAG").unwrap_or_else(|_| DEFAULT_IMAGE_TAG.to_owned())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("magnetar=info")),
        )
        .with_test_writer()
        .try_init();
}

/// Spin up a Pulsar 4.x standalone container with token-auth enabled.
/// The broker rejects any CONNECT carrying a malformed / unsigned token,
/// which is exactly the condition we want to exercise the enrichment for.
async fn start_pulsar_with_token_auth()
-> Result<(String, testcontainers::ContainerAsync<GenericImage>), Box<dyn std::error::Error>> {
    init_tracing();

    let secret_b64 = URL_SAFE_NO_PAD.encode(TOKEN_SECRET);
    let token_secret_key = format!("data:;base64,{secret_b64}");

    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
        .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
        .with_wait_for(WaitFor::message_on_stdout(
            "Created namespace public/default",
        ))
        .with_startup_timeout(Duration::from_secs(180))
        // Token-auth on. The broker's internal `brokerClient*` is given a
        // dummy admin token signed with `TOKEN_SECRET` so the standalone
        // bootstrap path (which creates `public/default`) can authenticate
        // against itself; the EXTERNAL client we'll launch from this test
        // intentionally signs with garbage so its CONNECT is rejected.
        .with_env_var("PULSAR_PREFIX_authenticationEnabled", "true")
        .with_env_var(
            "PULSAR_PREFIX_authenticationProviders",
            "org.apache.pulsar.broker.authentication.AuthenticationProviderToken",
        )
        .with_env_var("PULSAR_PREFIX_tokenSecretKey", token_secret_key)
        .with_env_var(
            "PULSAR_PREFIX_brokerClientAuthenticationPlugin",
            "org.apache.pulsar.client.impl.auth.AuthenticationToken",
        )
        .with_env_var(
            "PULSAR_PREFIX_brokerClientAuthenticationParameters",
            format!("token:{}", mint_internal_admin_token()),
        )
        .with_env_var("PULSAR_PREFIX_superUserRoles", "admin")
        // The apachepulsar image's CMD is `sh` (no entrypoint that wires
        // `apply-config-from-env*`), so the `PULSAR_PREFIX_*` env vars above
        // would never reach `conf/standalone.conf` if we launched the broker
        // directly. Apply them explicitly first — same pattern as
        // `e2e_pattern_auto_reconcile.rs`. Without this step the broker boots
        // with `authenticationEnabled=false` and the `INVALID_JWT` connect
        // below is accepted, defeating the test.
        .with_cmd(vec![
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            "bin/apply-config-from-env-with-prefix.py PULSAR_PREFIX_ \
                 conf/standalone.conf && bin/pulsar standalone \
                 --no-functions-worker --no-stream-storage"
                .to_owned(),
        ])
        .start()
        .await?;

    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let service_url = format!("pulsar://{host}:{binary_port}");
    Ok((service_url, container))
}

/// Mint the broker's internal admin token (HS256 signed with
/// `TOKEN_SECRET`, `sub: admin`). Used ONLY so the standalone bootstrap
/// path can authenticate against itself — the external client we test
/// signs its token with a different key, so its CONNECT is rejected.
fn mint_internal_admin_token() -> String {
    use aws_lc_rs::hmac;

    let header = r#"{"alg":"HS256","typ":"JWT"}"#;
    let claims = r#"{"sub":"admin"}"#;
    let header_b64 = URL_SAFE_NO_PAD.encode(header.as_bytes());
    let claims_b64 = URL_SAFE_NO_PAD.encode(claims.as_bytes());
    let signing_input = format!("{header_b64}.{claims_b64}");
    let key = hmac::Key::new(hmac::HMAC_SHA256, TOKEN_SECRET);
    let tag = hmac::sign(&key, signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(tag.as_ref());
    format!("{signing_input}.{sig_b64}")
}

/// Connect with a clearly invalid token and assert the broker-side
/// rejection reason rides the runtime's connect error verbatim. The
/// assertion is intentionally tolerant on the broker phrasing — what we
/// pin is the *envelope* (the enriched `"handshake failed: …"` carries a
/// broker-side auth marker instead of being swallowed by the legacy
/// opaque `"handshake failed"` string).
///
/// Runs as a regular test under `cargo test` (ADR-0046). Requires
/// Docker on the host.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_invalid_token_surfaces_broker_handshake_reason()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _container) = start_pulsar_with_token_auth().await?;

    // A bare `"INVALID_JWT"` is neither a valid HS256 token nor parseable
    // as compact JWS, so the broker's `AuthenticationProviderToken`
    // rejects it during CONNECT processing. The exact rejection text
    // varies across versions; the assertion below is tolerant on
    // phrasing.
    let provider: Arc<dyn AuthProvider> =
        Arc::new(TokenAuth::from_string("INVALID_JWT".to_owned()));

    let result = tokio::time::timeout(
        Duration::from_secs(30),
        PulsarClient::builder()
            .service_url(service_url)
            .auth(provider)
            .build(),
    )
    .await?;

    let err = result.expect_err(
        "PulsarClient::build must fail when the broker rejects CONNECT for an invalid token",
    );
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("handshake failed"),
        "façade error must carry the enriched \"handshake failed\" envelope from the proto-layer \
         capture (got: {err})"
    );
    // The broker's exact rejection text varies by version
    // (`AuthenticationError`, `Failed to authenticate with token …`,
    // `Unable to authenticate`, etc.) — tolerate any of three common
    // markers so the test stays stable across point releases.
    assert!(
        msg.contains("auth") || msg.contains("token") || msg.contains("invalid"),
        "façade error must contain some broker-side auth marker (auth/token/invalid) \
         to prove the proto-layer reason rode the connect error (got: {err})"
    );
    Ok(())
}
