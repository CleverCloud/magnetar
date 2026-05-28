// SPDX-License-Identifier: Apache-2.0

//! Tokio ↔ moonpool equivalence for the Athenz auth-data byte stream (ADR-0030, ADR-0024
//! layer d).
//!
//! [`magnetar_auth_athenz::AthenzProvider`] is engine-agnostic by construction — it carries an
//! injected [`magnetar_auth_athenz::zts::ZtsClient`] (or a scripted fake) plus a `wall_clock`
//! provider, so the same provider behaves identically under either runtime. The differential
//! cell that matters is *byte identity* of (a) the signed JWT presented to ZTS and (b) the
//! cached CONNECT `auth_data` payload, for a given schedule of ZTS responses + a given
//! `wall_clock` history. RSASSA-PKCS1-v1_5 + SHA-256 is deterministic (RFC 8017 §8.2), so two
//! independently-built providers driven through the same `(now, action)` schedule must hand the
//! broker the same bytes.
//!
//! Both providers run under a `tokio` current-thread runtime here; the moonpool deterministic
//! scheduler reduces to the same future-driving contract once the only I/O is the scripted fake.
//! The point is not "which executor" but "given the same schedule, do the user-visible bytes
//! match" — which is what the moonpool engine relies on for cross-engine reproducibility.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, UNIX_EPOCH};

use async_trait::async_trait;
use magnetar_auth_athenz::zts::{RoleTokenResponse, ZtsClient};
use magnetar_auth_athenz::{AthenzConfig, AthenzError, AthenzProvider, jwt_signer};
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

#[derive(Debug)]
struct ScriptedZts {
    responses: Mutex<Vec<RoleTokenResponse>>,
    last_jwt: Mutex<Option<String>>,
}

impl ScriptedZts {
    fn new(responses: Vec<RoleTokenResponse>) -> Arc<Self> {
        Arc::new(Self {
            responses: Mutex::new(responses),
            last_jwt: Mutex::new(None),
        })
    }
}

#[async_trait]
impl ZtsClient for ScriptedZts {
    async fn exchange(&self, signed_jwt: &str) -> Result<RoleTokenResponse, AthenzError> {
        *self.last_jwt.lock() = Some(signed_jwt.to_owned());
        let mut queue = self.responses.lock();
        if queue.is_empty() {
            return Err(AthenzError::ZtsRejected(
                "scripted ZTS exhausted in differential harness".to_owned(),
            ));
        }
        Ok(queue.remove(0))
    }
}

fn canned() -> Vec<RoleTokenResponse> {
    vec![
        RoleTokenResponse {
            access_token: "differential-role-1".to_owned(),
            expires_in: 3_600,
        },
        RoleTokenResponse {
            access_token: "differential-role-2".to_owned(),
            expires_in: 7_200,
        },
    ]
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

fn build_provider(wall_secs: u64) -> (AthenzProvider, Arc<ScriptedZts>) {
    let zts = ScriptedZts::new(canned());
    let config = sample_config();
    let signer = jwt_signer::default_signer_for(&config).expect("aws-lc-rs signer");
    let wall = Arc::new(AtomicU64::new(wall_secs));
    let provider = AthenzProvider::builder()
        .config(config)
        .signer(signer)
        .zts_client(zts.clone() as Arc<dyn ZtsClient>)
        .wall_clock(Arc::new(move || {
            UNIX_EPOCH + Duration::from_secs(wall.load(Ordering::SeqCst))
        }))
        .refresh_margin(Duration::from_secs(300))
        .build()
        .expect("build provider");
    (provider, zts)
}

/// Same `(now, action)` schedule on both engine-shaped drivers → byte-identical signed JWT
/// presented to ZTS *and* byte-identical cached CONNECT `auth_data`.
#[tokio::test]
async fn athenz_auth_data_bytes_match_across_engines_for_identical_schedule() {
    // Engine A (tokio proxy) and engine B (moonpool proxy) — seeded identically.
    let (provider_a, zts_a) = build_provider(1_700_000_000);
    let (provider_b, zts_b) = build_provider(1_700_000_000);

    let t0 = Instant::now();
    let mid = t0 + Duration::from_secs(1_000);
    // Past the deadline (ttl 3600 − margin 300) to force the second exchange.
    let boundary = t0 + Duration::from_secs(3_600 - 300 + 1);

    for provider in [&provider_a, &provider_b] {
        provider.ensure_role_token(t0).await.expect("first");
        provider
            .ensure_role_token(mid)
            .await
            .expect("mid (cache hit)");
        provider.ensure_role_token(boundary).await.expect("refresh");
    }

    // The JWT minted at each engine must be byte-identical (deterministic RS256 + fixed claims).
    let jwt_a = zts_a.last_jwt.lock().clone().expect("jwt a");
    let jwt_b = zts_b.last_jwt.lock().clone().expect("jwt b");
    assert_eq!(jwt_a, jwt_b, "signed JWT bytes must match across engines");

    // Cached role-token bytes — i.e. the CONNECT `auth_data` payload — must match.
    let initial_a = provider_a.initial().expect("initial a");
    let initial_b = provider_b.initial().expect("initial b");
    assert_eq!(
        initial_a, initial_b,
        "CONNECT auth_data must match across engines"
    );
    assert_eq!(initial_a.as_ref(), b"differential-role-2");

    // The method discriminator the broker keys on must be stable.
    assert_eq!(provider_a.method(), provider_b.method());
    assert_eq!(provider_a.method(), "athenz");
}
