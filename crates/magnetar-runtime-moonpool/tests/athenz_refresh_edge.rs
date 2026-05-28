// SPDX-License-Identifier: Apache-2.0

//! Refresh-edge coverage for [`magnetar_auth_athenz::AthenzProvider`] (ADR-0030, ADR-0024
//! layer c). The moonpool engine cannot speak HTTPS, so — exactly as ADR-0030 §moonpool
//! prescribes — we drive the **real** provider refresh + cache state machine with a scripted
//! [`magnetar_auth_athenz::zts::ZtsClient`] fake and an injected `now: Instant` schedule. The
//! aws-lc-rs RS256 signer still mints a real JWT (deterministic per RFC 8017 §8.2 under the
//! frozen `wall_clock`), so the only thing stubbed out is the HTTPS transport.
//!
//! Mirrors the shape of `oauth_refresh_edge.rs`, adapted for Athenz's
//! `(signed-JWT mint, RoleTokenResponse)` exchange.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, UNIX_EPOCH};

use async_trait::async_trait;
use magnetar_auth_athenz::zts::{RoleTokenResponse, ZtsClient};
use magnetar_auth_athenz::{
    AthenzConfig, AthenzError, AthenzProvider, DEFAULT_REFRESH_MARGIN, jwt_signer,
};
use magnetar_proto::AuthProvider;
use parking_lot::Mutex;

/// PKCS#8 v1 RSA-2048 test key (fixture-only; never a production credential).
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

/// Scripted [`ZtsClient`] — pops a pre-queued [`RoleTokenResponse`] per
/// `exchange`, counts invocations, and records the last JWT it was handed.
#[derive(Debug)]
struct ScriptedZts {
    responses: Mutex<Vec<RoleTokenResponse>>,
    calls: AtomicU64,
    last_jwt: Mutex<Option<String>>,
}

impl ScriptedZts {
    fn new(responses: Vec<RoleTokenResponse>) -> Arc<Self> {
        Arc::new(Self {
            responses: Mutex::new(responses),
            calls: AtomicU64::new(0),
            last_jwt: Mutex::new(None),
        })
    }
}

#[async_trait]
impl ZtsClient for ScriptedZts {
    async fn exchange(&self, signed_jwt: &str) -> Result<RoleTokenResponse, AthenzError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.last_jwt.lock() = Some(signed_jwt.to_owned());
        let mut queue = self.responses.lock();
        if queue.is_empty() {
            return Err(AthenzError::ZtsRejected(
                "scripted ZTS exhausted — test fixture mismatch".to_owned(),
            ));
        }
        Ok(queue.remove(0))
    }
}

fn role(token: &str, expires_in: u64) -> RoleTokenResponse {
    RoleTokenResponse {
        access_token: token.to_owned(),
        expires_in,
    }
}

fn sample_config() -> AthenzConfig {
    AthenzConfig {
        tenant_domain: "mydomain".to_owned(),
        tenant_service: "myservice".to_owned(),
        provider_domain: "pulsar.tenant".to_owned(),
        key_id: "key0".to_owned(),
        private_key_pem: TEST_PKCS8_PEM.to_owned(),
        zts_url: "https://zts.example.invalid/zts/v1/".to_owned(),
        principal_header: None,
        role_header: None,
    }
}

fn fixed_wall_clock(secs: Arc<AtomicU64>) -> magnetar_auth_athenz::WallClock {
    Arc::new(move || UNIX_EPOCH + Duration::from_secs(secs.load(Ordering::SeqCst)))
}

fn build_provider(zts: Arc<dyn ZtsClient>, wall: Arc<AtomicU64>) -> AthenzProvider {
    let config = sample_config();
    let signer = jwt_signer::default_signer_for(&config).expect("aws-lc-rs signer");
    AthenzProvider::builder()
        .config(config)
        .signer(signer)
        .zts_client(zts)
        .wall_clock(fixed_wall_clock(wall))
        .refresh_margin(DEFAULT_REFRESH_MARGIN)
        .build()
        .expect("build provider")
}

/// Cold cache → mint, exchange, cache. Re-issuing well inside the refresh window must absorb;
/// crossing the deadline (`now + (ttl − margin)`) must trigger a second exchange and rotate the
/// cached bytes — exactly at the injected virtual instant.
#[tokio::test]
async fn athenz_token_refresh_fires_exactly_at_virtual_deadline() {
    let zts = ScriptedZts::new(vec![role("role-#1", 3_600), role("role-#2", 3_600)]);
    let wall = Arc::new(AtomicU64::new(1_700_000_000));
    let provider = build_provider(zts.clone() as Arc<dyn ZtsClient>, wall.clone());

    let t0 = Instant::now();
    provider.ensure_role_token(t0).await.expect("first");
    assert_eq!(zts.calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.initial().expect("initial").as_ref(), b"role-#1");

    // Inside the fresh window: cache must absorb (deadline = t0 + 3600 − 300).
    let mid = t0 + Duration::from_secs(1_000);
    provider.ensure_role_token(mid).await.expect("mid");
    assert_eq!(zts.calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.initial().expect("initial").as_ref(), b"role-#1");

    // Cross the refresh boundary (deadline + 1s).
    let boundary = t0
        + Duration::from_secs(3_600)
            .checked_sub(DEFAULT_REFRESH_MARGIN)
            .expect("ttl > margin")
        + Duration::from_secs(1);
    assert!(provider.needs_refresh(boundary));
    provider.ensure_role_token(boundary).await.expect("refresh");
    assert_eq!(zts.calls.load(Ordering::SeqCst), 2);
    assert_eq!(provider.initial().expect("initial").as_ref(), b"role-#2");

    // The minted bearer is a compact three-segment JWS.
    let jwt = zts.last_jwt.lock().clone().expect("a jwt was minted");
    assert_eq!(
        jwt.matches('.').count(),
        2,
        "expected compact JWS, got {jwt}"
    );
}

/// `with_role_token` short-circuits the round-trip entirely — `ensure_role_token` is a no-op,
/// `needs_refresh` is always `false`, and `initial` always returns the supplied bytes.
#[tokio::test]
async fn athenz_provider_with_role_token_bypasses_round_trip() {
    let provider = AthenzProvider::with_role_token(
        sample_config(),
        bytes::Bytes::from_static(b"out-of-band-role-token"),
    );
    let now = Instant::now();
    provider
        .ensure_role_token(now)
        .await
        .expect("ensure is a no-op");
    assert!(!provider.needs_refresh(now));
    assert_eq!(
        provider.initial().expect("initial").as_ref(),
        b"out-of-band-role-token"
    );
    assert_eq!(provider.method(), "athenz");
}

/// Scripted ZTS failure → the cache stays empty; `initial()` keeps surfacing the
/// not-yet-fetched error so the connection layer can retry once upstream recovers.
#[tokio::test]
async fn athenz_provider_surfaces_zts_failure_without_poisoning_cache() {
    let zts = ScriptedZts::new(vec![]);
    let wall = Arc::new(AtomicU64::new(1_700_000_000));
    let provider = build_provider(zts as Arc<dyn ZtsClient>, wall);

    let err = provider
        .ensure_role_token(Instant::now())
        .await
        .expect_err("empty scripted queue must error");
    assert!(format!("{err}").contains("scripted ZTS"), "err={err}");

    let initial_err = provider.initial().expect_err("cache must still be empty");
    assert!(
        initial_err.to_string().contains("role token"),
        "err={initial_err}",
    );
}
