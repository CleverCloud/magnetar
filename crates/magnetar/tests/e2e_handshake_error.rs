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
//! The broker image comes from the shared fixture in `common/pulsar.rs`
//! (ADR-0115): its `DEFAULT_TAG`, overridable with `MAGNETAR_PULSAR_IMAGE_REPO` /
//! `MAGNETAR_PULSAR_IMAGE_TAG` if you need a different image locally.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use magnetar::proto::TokenAuth;
use magnetar::{AuthProvider, PulsarClient};
use testcontainers::GenericImage;

mod common;

use common::pulsar::{Broker, Pulsar, init_tracing};

/// Fixed 32-byte HS256 signing secret. Test-only. The broker is
/// configured with the same bytes via `tokenSecretKey=data:base64,…` so a
/// JWT signed with a *different* key is guaranteed to be rejected.
const TOKEN_SECRET: &[u8; 32] = b"magnetar-handshake-err-e2e-bytes";

/// Spin up a Pulsar standalone container with token-auth enabled.
/// The broker rejects any CONNECT carrying a malformed / unsigned token,
/// which is exactly the condition we want to exercise the enrichment for.
async fn start_pulsar_with_token_auth()
-> Result<(String, testcontainers::ContainerAsync<GenericImage>), Box<dyn std::error::Error>> {
    init_tracing();

    let secret_b64 = URL_SAFE_NO_PAD.encode(TOKEN_SECRET);
    let token_secret_key = format!("data:;base64,{secret_b64}");
    let admin_token = mint_internal_admin_token();

    // Token-auth on. The broker's internal `brokerClient*` is given a
    // dummy admin token signed with `TOKEN_SECRET` so the standalone
    // bootstrap path (which creates `public/default`) can authenticate
    // against itself; the EXTERNAL client we'll launch from this test
    // intentionally signs with garbage so its CONNECT is rejected.
    //
    // The shared fixture applies this configuration to `conf/standalone.conf`
    // before the broker boots; without that step the broker would run with
    // `authenticationEnabled=false` and the `INVALID_JWT` connect below would
    // be accepted, defeating the test. The same admin token (`superUserRoles`)
    // authenticates the fixture's own readiness probe against the now-secured
    // admin REST.
    let broker = Pulsar::new(Broker::Default)
        .config("authenticationEnabled", "true")
        .config(
            "authenticationProviders",
            "org.apache.pulsar.broker.authentication.AuthenticationProviderToken",
        )
        .config("tokenSecretKey", token_secret_key)
        .config(
            "brokerClientAuthenticationPlugin",
            "org.apache.pulsar.client.impl.auth.AuthenticationToken",
        )
        .config(
            "brokerClientAuthenticationParameters",
            format!("token:{admin_token}"),
        )
        .config("superUserRoles", "admin")
        .flag("--no-functions-worker")
        .flag("--no-stream-storage")
        .admin_token(admin_token)
        .start()
        .await?;
    Ok((broker.service_url, broker.container))
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
