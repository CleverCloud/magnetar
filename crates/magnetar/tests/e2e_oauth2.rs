// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the `OAuth2` `ClientCredentialsFlow` auth provider
//! ([`magnetar_auth_oauth2`], ADR-0014).
//!
//! The Pulsar broker is left in its default (unauthenticated) standalone mode
//! — the assertions exclusively target the provider's behaviour:
//!
//! 1. The provider hits the IDP token endpoint once on the first connection, surfaces the JWT
//!    through [`magnetar_proto::AuthProvider::initial`], and the broker accepts the resulting
//!    `CommandConnect.auth_data`.
//! 2. A second connection re-uses the cached token (zero additional token exchanges).
//! 3. Advancing the injected [`magnetar_auth_oauth2::Clock`] past the refresh leeway window forces
//!    a second `/token` POST on the next
//!    [`magnetar_auth_oauth2::ClientCredentialsFlow::ensure_fresh`] call.
//!
//! Gated behind `e2e` + `auth-oauth2`. Run with:
//!
//! ```sh
//! cargo test --features e2e,auth-oauth2 \
//!   -p magnetar --test e2e_oauth2 -- --nocapture
//! ```

#![cfg(all(feature = "e2e", feature = "auth-oauth2"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::{OutgoingMessage, PulsarClient};
use magnetar_auth_oauth2::{ClientCredentialsFlow, Clock, Credentials, REFRESH_LEEWAY};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use url::Url;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "4.0.4";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

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

/// Start an unauthenticated Pulsar 4.x standalone container. The broker happily
/// accepts a token-method CONNECT — the standalone image has no
/// `AuthenticationProvider` plugged in by default, so the bearer bytes are
/// stored on the session without further validation. That is *exactly* what we
/// want here: every other axis except the auth-provider behaviour is held
/// fixed.
async fn start_pulsar()
-> Result<(String, testcontainers::ContainerAsync<GenericImage>), Box<dyn std::error::Error>> {
    init_tracing();
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
        .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
        .with_wait_for(WaitFor::message_on_stdout("messaging service is ready"))
        .with_startup_timeout(Duration::from_secs(120))
        .with_cmd(vec!["bin/pulsar".to_owned(), "standalone".to_owned()])
        .start()
        .await?;
    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let service_url = format!("pulsar://{host}:{binary_port}");
    Ok((service_url, container))
}

/// Stand up a wiremock instance that impersonates an OIDC `/token` endpoint.
/// `expires_in` is the broker-advertised lifetime so we can drive cache /
/// refresh behaviour by injecting a virtual clock.
async fn start_idp(access_token: &str, expires_in: u64) -> MockServer {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/oauth/token"))
        .and(body_string_contains("grant_type=client_credentials"))
        .and(body_string_contains("client_id=test-client"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": expires_in,
        })))
        .mount(&mock)
        .await;
    mock
}

/// Test clock that only moves via [`VirtualClock::advance`]. Mirrors the
/// pattern used by `magnetar-auth-oauth2`'s in-crate tests so the assertion
/// surface stays identical.
#[derive(Debug)]
struct VirtualClock {
    base: Instant,
    offset_ms: AtomicU64,
}

impl VirtualClock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            base: Instant::now(),
            offset_ms: AtomicU64::new(0),
        })
    }

    fn advance(&self, by: Duration) {
        self.offset_ms
            .fetch_add(by.as_millis() as u64, Ordering::SeqCst);
    }
}

impl Clock for VirtualClock {
    fn now(&self) -> Instant {
        self.base + Duration::from_millis(self.offset_ms.load(Ordering::SeqCst))
    }
}

fn build_flow(
    idp: &MockServer,
    clock: Arc<VirtualClock>,
) -> Result<ClientCredentialsFlow, Box<dyn std::error::Error>> {
    let issuer = Url::parse(&format!("{}/", idp.uri()))?;
    let token_endpoint = issuer.join("oauth/token")?;
    Ok(ClientCredentialsFlow::builder()
        .issuer_url(issuer)
        .token_endpoint(token_endpoint)
        .audience("urn:pulsar:broker")
        .credentials(Credentials::ClientSecret {
            client_id: "test-client".to_owned(),
            client_secret: "super-secret".to_owned(),
        })
        .clock(clock as Arc<dyn Clock>)
        .build()?)
}

/// Happy path: the IDP is hit exactly once on the first
/// `ensure_fresh`; a producer + consumer round-trip against the broker
/// succeeds with the JWT carried in `CommandConnect.auth_data`.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_oauth2_happy_path_produces_and_consumes() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _container) = start_pulsar().await?;
    let idp = start_idp("magnetar-jwt-1", 3600).await;

    let clock = VirtualClock::new();
    let flow = build_flow(&idp, clock.clone())?;
    flow.ensure_fresh().await?;
    let provider: Arc<dyn magnetar::AuthProvider> = Arc::new(flow);

    let client = PulsarClient::builder()
        .service_url(service_url)
        .auth(provider)
        .build()
        .await?;

    let topic = "persistent://public/default/magnetar-e2e-oauth2-happy";
    let producer = client.producer(topic).create().await?;
    producer
        .send(OutgoingMessage::with_payload(b"hello-oauth2".to_vec()).into())
        .await?;
    producer.close().await?;

    let consumer = client
        .consumer(topic)
        .subscription("magnetar-e2e-oauth2")
        .subscription_type(SubType::Exclusive)
        .subscribe()
        .await?;
    let msg = tokio::time::timeout(Duration::from_secs(10), consumer.receive()).await??;
    assert_eq!(msg.payload.as_ref(), b"hello-oauth2");
    consumer.ack(msg.message_id).await?;
    consumer.close().await?;
    client.close().await;

    // The provider must have hit `/oauth/token` exactly once — the cache is
    // primed on `ensure_fresh` and reused for every CONNECT.
    let received = idp.received_requests().await.unwrap_or_default();
    assert_eq!(
        received.len(),
        1,
        "expected exactly one /token POST, got {} (paths: {:?})",
        received.len(),
        received
            .iter()
            .map(|r| r.url.path().to_owned())
            .collect::<Vec<_>>(),
    );
    Ok(())
}

/// Cache hit: two clients sharing the same `ClientCredentialsFlow` instance
/// must consume the same cached token. The IDP records exactly one
/// `/oauth/token` POST across the entire run.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_oauth2_token_cache_reuses_across_connections() -> Result<(), Box<dyn std::error::Error>>
{
    let (service_url, _container) = start_pulsar().await?;
    let idp = start_idp("magnetar-jwt-cached", 3600).await;

    let clock = VirtualClock::new();
    let flow = Arc::new(build_flow(&idp, clock.clone())?);
    flow.ensure_fresh().await?;
    let provider: Arc<dyn magnetar::AuthProvider> = flow.clone();

    // First client + producer.
    let client_a = PulsarClient::builder()
        .service_url(service_url.clone())
        .auth(provider.clone())
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-oauth2-cache";
    let producer_a = client_a.producer(topic).create().await?;
    producer_a
        .send(OutgoingMessage::with_payload(b"a".to_vec()).into())
        .await?;
    producer_a.close().await?;

    // Second client — independent connection, same provider. Driver calls
    // `provider.initial()` for the bytes; the cache fills it from the same
    // `Arc<Mutex<Option<CachedToken>>>` so no fresh IDP POST is needed.
    let client_b = PulsarClient::builder()
        .service_url(service_url)
        .auth(provider)
        .build()
        .await?;
    let producer_b = client_b.producer(topic).create().await?;
    producer_b
        .send(OutgoingMessage::with_payload(b"b".to_vec()).into())
        .await?;
    producer_b.close().await?;

    client_a.close().await;
    client_b.close().await;

    let received = idp.received_requests().await.unwrap_or_default();
    assert_eq!(
        received.len(),
        1,
        "token cache must absorb the second client's CONNECT; got {} POSTs",
        received.len(),
    );
    Ok(())
}

/// Refresh on expiry: advance the virtual clock past
/// `deadline - REFRESH_LEEWAY`, call `ensure_fresh` again, and verify a
/// *second* `/token` POST occurs. Mirrors ADR-0014's "refresh within 30 s of
/// deadline" guarantee.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_oauth2_refresh_on_expiry_reissues_token() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _container) = start_pulsar().await?;
    // Short lifetime so the virtual-clock advance crosses the leeway boundary
    // without overflowing `Instant`. 120 s lifetime → cross at 90 s + 1 s.
    let lifetime_secs: u64 = 120;
    let idp = start_idp("magnetar-jwt-fresh-1", lifetime_secs).await;

    let clock = VirtualClock::new();
    // The provider must outlive both `Arc<dyn AuthProvider>` handed to the
    // client and the typed `ensure_fresh` driver, so wrap once in `Arc` and
    // hand both views a clone.
    let flow = Arc::new(build_flow(&idp, clock.clone())?);
    flow.ensure_fresh().await?;

    // Sanity: needs_refresh is `false` immediately after fetch.
    assert!(
        !flow.needs_refresh(),
        "fresh token should sit outside the leeway window",
    );

    let provider: Arc<dyn magnetar::AuthProvider> = flow.clone();
    let client = PulsarClient::builder()
        .service_url(service_url)
        .auth(provider)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-oauth2-refresh";
    let producer = client.producer(topic).create().await?;
    producer
        .send(OutgoingMessage::with_payload(b"first".to_vec()).into())
        .await?;
    producer.close().await?;
    client.close().await;

    // Re-arm the wiremock route with a *different* token to confirm the
    // refresh actually rotates the cached bytes.
    Mock::given(method("POST"))
        .and(path("/oauth/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "magnetar-jwt-fresh-2",
            "token_type": "Bearer",
            "expires_in": lifetime_secs,
        })))
        .mount(&idp)
        .await;

    // Advance the clock past `lifetime - REFRESH_LEEWAY`. The next
    // `ensure_fresh` must hit the IDP again.
    let advance = Duration::from_secs(lifetime_secs)
        .checked_sub(REFRESH_LEEWAY)
        .expect("test leeway < deadline")
        + Duration::from_secs(1);
    clock.advance(advance);

    assert!(
        flow.needs_refresh(),
        "after clock advance, the cached token must be inside the leeway window",
    );
    flow.ensure_fresh().await?;
    let cached = flow.cached_access_token().expect("token must be cached");
    assert_eq!(
        cached.as_ref(),
        b"magnetar-jwt-fresh-2".as_ref(),
        "second exchange must rotate the cached JWT",
    );

    let received = idp.received_requests().await.unwrap_or_default();
    let token_posts = received
        .iter()
        .filter(|r| r.url.path() == "/oauth/token")
        .count();
    assert!(
        token_posts >= 2,
        "expected at least two /token POSTs after clock advance; got {token_posts}",
    );
    Ok(())
}
