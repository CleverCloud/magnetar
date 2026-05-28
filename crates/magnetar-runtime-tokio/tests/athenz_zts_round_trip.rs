// SPDX-License-Identifier: Apache-2.0

//! Tokio integration coverage for the Athenz ZTS round-trip (ADR-0030, ADR-0024 layer b).
//!
//! Drives the **real** [`magnetar_auth_athenz::zts::HttpZtsClient`] (wired via
//! [`magnetar_auth_athenz::AthenzProvider::with_default_signer`], which mints the bearer JWT with
//! the in-tree aws-lc-rs RS256 signer) against a `wiremock` ZTS stub over plain HTTP:
//!
//! 1. **Happy path.** A first `ensure_role_token` mints a JWT, POSTs it to the ZTS
//!    `/zts/v1/oauth2/token` endpoint exactly once, and exposes the role token through
//!    [`AuthProvider::initial`]. The bearer credential is a compact three-segment JWS.
//! 2. **Cache hit.** A second `ensure_role_token` issued well inside the refresh window is absorbed
//!    by the cache — wiremock records exactly one request.
//! 3. **Refresh on expiry.** Driving the injected `now: Instant` past the cached deadline (`t0 +
//!    (ttl − refresh_margin)`) triggers a second exchange and rotates the cached bytes.

use std::time::{Duration, Instant};

use magnetar_auth_athenz::{AthenzConfig, AthenzProvider};
use magnetar_proto::AuthProvider;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// PKCS#8 v1 RSA-2048 test key (fixture-only, never a production credential).
/// Same body as the `magnetar-auth-athenz` per-backend signer fixtures.
const TEST_PKCS8_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCxJud1eHdqMtxK
hTb7LKgcRuw1k/3/e2aOIPzgPOc3nGTgh+AgsSz5VCPVoqsub/ipbWU/3u5rN6pa
aSXxCRSdKF1LCTD+Qrp4T86W9vgBQeCiw61YyjTQQ55naN9Sngy6V+JzQOBOYqrY
i67ppWIebI5ThK/a0KbqFI+btDwt0W285c9h+/HIPrGWU1JokWuBzJW7DHgv7rLc
euEXPQqaHoMLZFgDsD7zvyOsVod+gbbMIhRJ72G0R18XBchwOfnbRZkDjiIVR7bI
uad+zPoWZLnxXZvvIOm0twkRtCoM0qcVzuAsDxuVfG9OGarQvk1p4lnALEb8Zl1J
+n7qKoCRAgMBAAECggEATIxyEcmnWCV4GV9s/aYzUly3LwOvCtmo3BuXCdJnWxli
Yb908st8kpRwE52B+MP7oEKcMLhFL+FS5FRxR7FTzgEmJwlmuUfeSaS6sXMwgWKV
DeAeJLLjlWbSqP6hGZMgDtlxCbpr8pMiHgZl46JKPrlL2v0H/DaTGa0ezPpZ0rXl
MWkHieSaGaC5oxoB/khxk22tZYn7XR0E78/w1k3JZr6tiHHPRZGCU8dpl3xRowfp
76JEEkf7ZosLtw+rigU5D44vIcUVJUbweNy/Ad2CzL7hGvdeXOjCRLjhOdbVwwzw
yNcsCK1qNq5YlieSuFVBT89OuAeYuzqDhc47serLnQKBgQD5Oa1n9JuKTc1nkiFz
p7x4n7503p5fpwDPtIrUBjEX+xkFAV+1ujBtiLbMgkga4dMz3UBbRCx/ip7THhVt
8THYMILZ5jzw6AeO5jQYsb92jRf7VNLa9/F2jSzQnUdNHwJFh7rx1Zeg1SoCpt34
wk0fNfufvTWCJ1lDI1kjn+aOlwKBgQC1963OI5CWNlsYBwlaQyfYeo4yn1ghPUoK
Dlshpe16HWzaBxhaOhaanaYuqFXGW082plgQ+bg8w7rXU+mhpt0S6n1VDNg2WnVN
rq0Uz73yq44Dhhd7w0ugH5oBLbSwOAQSkQ1nxYtng3g1akaiiSrmErOHcIPmxPww
2NzOqiD3FwKBgG7IoLhxFyLnasL7Rjtu+Gx2Neclfijux4GMs5mEFxad24VKEw1o
8lX+S6Ok1gB9GbEYTJ9FMrKPIAKggM4aRRnglonduoEr4xA2bDn96Sn9lgd2sTP8
uy0DnEQvZZ52hj/6EbOmSnyHxODg5BLL7BRPnsZnCP4OF7OsZtdbINWJAoGAVvCP
Sf4UBrDRtRknjsinMPbdGbKoGLl/tm5FfD4ayE1mxIS/TdyTECxiSciDstHNdv7i
9LlbHS0nB9o/tcxTs8X1O713UADIKuVaLKdUyazNnUFj1u3oJAj1O7rqqYcZ6wUC
sqHfiQV3WY39UYrXxDULMZrAanGTTINQfC0ssuECgYEAzXpBkVWCu5VTcTNxrCOl
+btMpklzgovlpZWNxo8gDW6iNV2q3FcUjwxM4KRaxjKEKtmpi7HBDgLbDUxL3GffI
6Rc4ifbJEa41FakC7MGusbsyqeS2e0nF8WUn1fRgoBxARezLU9gVv/JpGDBSyt8
VKO8LwAfoAvnoIH0CDFftdg=
-----END PRIVATE KEY-----";

fn sample_config(zts_url: &str) -> AthenzConfig {
    AthenzConfig {
        tenant_domain: "mydomain".to_owned(),
        tenant_service: "myservice".to_owned(),
        provider_domain: "pulsar.tenant".to_owned(),
        key_id: "key0".to_owned(),
        private_key_pem: TEST_PKCS8_PEM.to_owned(),
        zts_url: zts_url.to_owned(),
        principal_header: None,
        role_header: None,
    }
}

/// One-shot ZTS stub: serves `(access_token, expires_in)` on the first
/// `POST /zts/v1/oauth2/token` only, so a subsequent matcher can rotate.
async fn start_zts_stub(access_token: &str, expires_in: u64) -> MockServer {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/zts/v1/oauth2/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": access_token,
            "token_type":   "Bearer",
            "expires_in":   expires_in,
        })))
        .up_to_n_times(1)
        .mount(&mock)
        .await;
    mock
}

/// Mount a follow-on matcher with a rotated token, picked up by POSTs
/// after the one-shot above is exhausted.
async fn mount_rotated_response(mock: &MockServer, access_token: &str, expires_in: u64) {
    Mock::given(method("POST"))
        .and(path("/zts/v1/oauth2/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": access_token,
            "token_type":   "Bearer",
            "expires_in":   expires_in,
        })))
        .mount(mock)
        .await;
}

fn build_provider(zts_base_url: &str) -> AthenzProvider {
    let config = sample_config(&format!("{zts_base_url}/zts/v1/"));
    AthenzProvider::with_default_signer(config).expect("default aws-lc-rs signer constructs")
}

/// Happy path — one mint + cached reads, role token exposed via `initial()`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn athenz_provider_round_trip_mints_and_caches() {
    let mock = start_zts_stub("athenz-role-token-1", 3600).await;
    let provider = build_provider(&mock.uri());

    // Before any fetch: `initial()` surfaces the documented recovery hint.
    let err = provider.initial().unwrap_err();
    assert!(
        err.to_string().contains("role token"),
        "pre-fetch initial() must surface the role-token hint: {err}",
    );

    let t0 = Instant::now();
    provider
        .ensure_role_token(t0)
        .await
        .expect("ensure_role_token");

    // Cached bytes surfaced; a second `initial()` stays cached.
    assert_eq!(
        provider.initial().expect("initial").as_ref(),
        b"athenz-role-token-1"
    );
    assert_eq!(
        provider.initial().expect("initial").as_ref(),
        b"athenz-role-token-1"
    );

    let received = mock.received_requests().await.unwrap_or_default();
    assert_eq!(received.len(), 1, "exactly one ZTS POST on the happy path");

    // The bearer credential is a compact three-segment JWS minted by the
    // in-tree aws-lc-rs signer — proves the crypto reached the HTTP layer.
    let auth_header = received[0]
        .headers
        .get("authorization")
        .expect("Authorization header present")
        .to_str()
        .expect("ascii bearer header");
    let bearer = auth_header
        .strip_prefix("Bearer ")
        .expect("bearer-prefixed authorization");
    assert_eq!(
        bearer.matches('.').count(),
        2,
        "expected compact JWS, got {bearer}"
    );
}

/// Cache hit — a second `ensure_role_token` inside the refresh window must not hit ZTS.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn athenz_provider_cache_absorbs_second_call_within_window() {
    let mock = start_zts_stub("cached-token", 3600).await;
    let provider = build_provider(&mock.uri());

    let t0 = Instant::now();
    provider.ensure_role_token(t0).await.expect("first");
    provider
        .ensure_role_token(t0 + Duration::from_secs(60))
        .await
        .expect("second (cache hit)");

    let received = mock.received_requests().await.unwrap_or_default();
    assert_eq!(
        received.len(),
        1,
        "cache must absorb the second ensure_role_token inside the window",
    );
}

/// Refresh on expiry — driving `now` past the cached deadline must trigger a
/// fresh exchange and rotate the cached bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn athenz_provider_refresh_on_expiry_rotates_cached_token() {
    // TTL 600 s, default refresh margin 300 s → deadline at t0 + 300 s.
    let mock = start_zts_stub("first-role-token", 600).await;
    mount_rotated_response(&mock, "second-role-token", 600).await;
    let provider = build_provider(&mock.uri());

    let t0 = Instant::now();
    provider.ensure_role_token(t0).await.expect("first");
    assert_eq!(
        provider.initial().expect("initial").as_ref(),
        b"first-role-token"
    );

    let past_deadline = t0 + Duration::from_secs(600 - 300 + 1);
    assert!(
        provider.needs_refresh(past_deadline),
        "must be inside the refresh window past the deadline",
    );
    provider
        .ensure_role_token(past_deadline)
        .await
        .expect("refresh");
    assert_eq!(
        provider.initial().expect("initial").as_ref(),
        b"second-role-token"
    );

    let received = mock.received_requests().await.unwrap_or_default();
    assert_eq!(
        received.len(),
        2,
        "expected two POSTs (initial + expiry-driven refresh)"
    );
}
