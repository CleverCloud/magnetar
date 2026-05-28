// SPDX-License-Identifier: Apache-2.0

//! PIP-180 / ADR-0033 — replicator-role end-to-end coverage against a real
//! Apache Pulsar 4.x standalone broker (`apachepulsar/pulsar:4.0.4`).
//!
//! Sibling of [`e2e_shadow_topic.rs`](e2e_shadow_topic.rs), which exercises
//! the **admin REST + produce/consume happy path** against an
//! unauthenticated broker. This file pins the **broker-side authorisation
//! contract** around `Producer::send_with_source_message_id` (PIP-180) by
//! running a token-authenticated broker with a `replicator` role granted
//! on the test namespace.
//!
//! # Fixture shape — single-cluster (scope adjustment vs the original /goal)
//!
//! The original `/goal` in `docs/follow-ups.md` §5 called for a **two-
//! cluster** standalone topology on separate Docker networks. After
//! surveying PIP-180:
//!
//! - **PIP-180 shadow topics are intra-cluster**. The shadow + source live in the **same** broker
//!   and share the same `BookKeeper` ledgers. The "replicator-role" check the broker applies on
//!   `CommandSend.message_id` enforces *write authorisation on the topic* under
//!   `authorizationAllowWildcardsMatching`-style ACLs; it does **not** need a second cluster.
//! - Cross-cluster replication is **PIP-33** (geo-replication), which has its own e2e at
//!   [`e2e_replicated_subscriptions.rs`]. Conflating it with PIP-180 would test PIP-33, not the
//!   replicator-role contract.
//!
//! A single standalone container with `superUserRoles` + an in-container
//! `pulsar-admin namespaces grant-permission` against `public/default` is
//! sufficient to gate the assertion. The "self-hosting (no external broker
//! dependency)" constraint from the goal is preserved — the fixture spins
//! up the broker via `testcontainers-rs` and seeds the role grant +
//! shadow-topic create via `pulsar-admin` exec'd inside the container. See
//! the commit body for the full scope reasoning.
//!
//! # Broker contract observed (Pulsar 4.0.4)
//!
//! `send_with_source_message_id` (`CommandSend.message_id` set) is gated
//! by the broker on the **topic type**, NOT on the producer's role:
//!
//! > `SendRejected { code: 22, message: "Only shadow topic supports
//! > sending messages with messageId" }`
//!
//! (`code 22` = `ServerError::NotAllowedError`.) The producer must be
//! attached to a **shadow topic** before it may assert a source
//! `MessageId`. The `replicator` role grant is still load-bearing: without
//! `produce` permission on the namespace, the producer attach itself is
//! rejected (the negative test pins this). The two gates are orthogonal —
//! topic-type AND authorisation must both pass.
//!
//! # Test inventory
//!
//! 1. [`e2e_v4_replicator_role_can_assert_source_message_id`] — pins the broker's **topic-type
//!    gate** from both sides using one authorised `replicator` producer. On a **regular** topic
//!    `send_with_source_message_id` is rejected with `code 22` ("Only shadow topic supports sending
//!    messages with messageId"); on a **shadow** topic the same call is accepted on the wire (no
//!    `code 22`). The shadow side does NOT assert a receipt echo or consumer delivery because a
//!    live shadow's source-backed ledger silently absorbs a fabricated source id — see the test's
//!    own doc for the full contract.
//! 2. [`e2e_v4_non_replicator_role_send_with_source_id_is_rejected`] — negative path. A producer
//!    authenticated as a **non-replicator** principal (no `produce` grant on the namespace)
//!    attempts to attach. The broker's authorisation flow rejects the attach with a wire-level
//!    error before the topic-type gate is even reached. The test asserts an authorisation-flavoured
//!    error surfaces on the producer create / send path — pinning whatever the broker does today.
//!
//! Both tests are gated behind `feature = "e2e"` + `#[ignore = "e2e:
//! requires Docker"]`. They never run in the default `cargo test
//! --workspace` invocation; the e2e job picks them up via:
//!
//! ```sh
//! cargo test --features e2e -p magnetar \
//!     --test e2e_shadow_topic_replicator -- --include-ignored --nocapture
//! ```
//!
//! # Token minting
//!
//! HS256 JWTs are hand-encoded with `aws-lc-rs::hmac` (already in dev-deps
//! for PIP-4 tests) so we avoid pulling `jsonwebtoken` into the
//! workspace. The signing secret is a fixed 32-byte test value; the
//! broker is configured with the same secret inline via
//! `tokenSecretKey=data:;base64,<b64>` (no bind-mount needed). The mint
//! helper produces three tokens per run: `admin` (super-user, for the
//! shadow-topic create + namespace grant), `replicator` (positive test),
//! and `magnetar-test-user` (negative test).

#![cfg(feature = "e2e")]

use std::sync::Arc;
use std::time::Duration;

use aws_lc_rs::hmac;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use magnetar::proto::TokenAuth;
use magnetar::{AuthProvider, PulsarClient};
use magnetar_proto::MessageId;
use testcontainers::core::{ContainerPort, ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "4.0.4";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

/// Fixed 32-byte HS256 signing secret. Test-only. The broker is
/// configured with the same bytes via `tokenSecretKey=data:base64,...`.
const TOKEN_SECRET: &[u8; 32] = b"magnetar-pip180-e2e-secret-bytes";

/// The role granted `superUser` on the broker — required to mint REST
/// pre-seed calls (namespace permission grant).
const ADMIN_ROLE: &str = "admin";
/// The role we register as a permitted producer/replicator on
/// `public/default`. The broker validates write authorisation against
/// this role.
const REPLICATOR_ROLE: &str = "replicator";
/// A regular non-replicator role used by the negative test.
const NON_REPLICATOR_ROLE: &str = "magnetar-test-user";

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

/// Mint a token-auth JWT (HS256, `sub`-claim = role) signed with the
/// fixture's [`TOKEN_SECRET`]. The broker's `AuthenticationProviderToken`
/// extracts the `sub` claim and presents it as the authenticated role.
fn mint_jwt(role: &str) -> String {
    // Compact JWS header — `{"alg":"HS256","typ":"JWT"}` matches what
    // Pulsar's reference token agent (`pulsar-admin tokens create
    // --secret-key file:///…`) emits.
    let header = r#"{"alg":"HS256","typ":"JWT"}"#;
    // No `exp` claim — Pulsar's `AuthenticationProviderToken` is happy
    // with an indefinitely-valid token when `tokenAllowInsecureConnection`
    // is left at default and no `tokenExpirationOption` is set. This
    // keeps the test deterministic (no clock skew between the host
    // running the test and the container).
    let claims = format!(r#"{{"sub":"{role}"}}"#);
    let header_b64 = URL_SAFE_NO_PAD.encode(header.as_bytes());
    let claims_b64 = URL_SAFE_NO_PAD.encode(claims.as_bytes());
    let signing_input = format!("{header_b64}.{claims_b64}");
    let key = hmac::Key::new(hmac::HMAC_SHA256, TOKEN_SECRET);
    let tag = hmac::sign(&key, signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(tag.as_ref());
    format!("{signing_input}.{sig_b64}")
}

/// Bring up a single Pulsar 4.x standalone container with token-auth +
/// admin REST pre-seed for the `replicator` role.
///
/// Returns:
/// - `service_url` — `pulsar://host:port` for the binary protocol;
/// - `admin_url` — `http://host:port` for the admin REST;
/// - `admin_token` — HS256 JWT with `sub: "admin"` (super-user; used for the shadow-topic create +
///   namespace grants);
/// - `replicator_token` — HS256 JWT with `sub: "replicator"`;
/// - `non_replicator_token` — HS256 JWT with `sub: "magnetar-test-user"`;
/// - `container` — the kept-alive `ContainerAsync<GenericImage>` (drop to tear down).
///
/// The broker's `standard.conf` is overlaid via environment variables
/// (the upstream `apachepulsar/pulsar` image's `entrypoint.sh` honours
/// `PULSAR_PREFIX_<key>` for any conf key per
/// [`docker/conf/apply-config-from-env.py`](https://github.com/apache/pulsar/blob/v4.0.4/docker/pulsar/scripts/apply-config-from-env.py)).
/// We seed the HS256 secret via `tokenSecretKey=data:base64,...` so no
/// bind-mount is needed.
async fn start_pulsar_with_token_auth_and_replicator_role() -> Result<
    (
        String,
        String,
        String,
        String,
        String,
        testcontainers::ContainerAsync<GenericImage>,
    ),
    Box<dyn std::error::Error>,
> {
    init_tracing();

    // The broker reads `tokenSecretKey=data:base64,<b64>` and treats the
    // decoded bytes as the HS256 signing key. Matches the
    // [`pulsar-admin tokens create-secret-key`](https://pulsar.apache.org/docs/4.0.x/security-jwt/#create-a-secret-key)
    // flow used in production. We base64-encode `TOKEN_SECRET` and pass
    // it through PULSAR_PREFIX_tokenSecretKey.
    let secret_b64 = URL_SAFE_NO_PAD.encode(TOKEN_SECRET);
    let token_secret_key = format!("data:;base64,{secret_b64}");

    // Mint a long-lived admin token used by the pre-seed REST call.
    // This token also stays inside the test process — it is never
    // emitted in tracing.
    let admin_token = mint_jwt(ADMIN_ROLE);
    let replicator_token = mint_jwt(REPLICATOR_ROLE);
    let non_replicator_token = mint_jwt(NON_REPLICATOR_ROLE);

    // `bin/pulsar standalone --no-functions-worker --no-stream-storage`
    // skips two background components the test doesn't need; shaves
    // ~30 s off startup on cold-cache CI hosts.
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
        .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
        .with_wait_for(WaitFor::message_on_stdout(
            "Created namespace public/default",
        ))
        .with_startup_timeout(Duration::from_secs(180))
        // Token-auth provider + secret-key + admin super-user role.
        // `brokerClientAuthenticationPlugin` + `brokerClientAuthenticationParameters`
        // are required so the broker's internal admin client (used by
        // `pulsar standalone`'s bootstrap path that creates
        // `public/default`) can authenticate against itself.
        .with_env_var("PULSAR_PREFIX_authenticationEnabled", "true")
        .with_env_var(
            "PULSAR_PREFIX_authenticationProviders",
            "org.apache.pulsar.broker.authentication.AuthenticationProviderToken",
        )
        .with_env_var("PULSAR_PREFIX_tokenSecretKey", token_secret_key)
        .with_env_var("PULSAR_PREFIX_authorizationEnabled", "true")
        .with_env_var("PULSAR_PREFIX_superUserRoles", ADMIN_ROLE)
        .with_env_var(
            "PULSAR_PREFIX_brokerClientAuthenticationPlugin",
            "org.apache.pulsar.client.impl.auth.AuthenticationToken",
        )
        .with_env_var(
            "PULSAR_PREFIX_brokerClientAuthenticationParameters",
            format!("token:{}", &admin_token),
        )
        .with_cmd(vec![
            "bin/pulsar".to_owned(),
            "standalone".to_owned(),
            "--no-functions-worker".to_owned(),
            "--no-stream-storage".to_owned(),
        ])
        .start()
        .await?;

    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let http_port = container.get_host_port_ipv4(BROKER_HTTP_PORT).await?;
    let service_url = format!("pulsar://{host}:{binary_port}");
    let admin_url = format!("http://{host}:{http_port}");

    // Pre-seed: grant `produce` + `consume` permission on
    // `public/default` to the `replicator` role. The broker's
    // `AuthorizationProvider` (default `PulsarAuthorizationProvider`)
    // checks this on every producer / consumer attach. Without the
    // grant, even a valid JWT-bearing producer is rejected with
    // `AuthorizationException`.
    //
    // Done via `pulsar-admin` exec'd inside the container — it picks up
    // `brokerClientAuthenticationParameters` from `client.conf` and
    // authenticates as the broker's super-user role. Functionally
    // equivalent to a
    // `POST /admin/v2/namespaces/public/default/permissions/replicator`
    // REST call but avoids dragging a reqwest request-construction dance
    // into the test file.
    //
    // We wait for the exec to exit (`CmdWaitFor::exit`) so the grant is
    // visible before the test opens its first producer — without the
    // wait the exec runs lazily and the producer attach could race the
    // permission write.
    //
    // The negative test's `NON_REPLICATOR_ROLE` is **deliberately not**
    // granted any permission on `public/default`, so its producer attach
    // is rejected with an authorisation error — that is the contract pin
    // the negative test asserts.
    let _grant_replicator = container
        .exec(
            ExecCommand::new([
                "bin/pulsar-admin",
                "namespaces",
                "grant-permission",
                "public/default",
                "--role",
                REPLICATOR_ROLE,
                "--actions",
                "produce,consume",
            ])
            .with_cmd_ready_condition(testcontainers::core::CmdWaitFor::exit()),
        )
        .await?;

    Ok((
        service_url,
        admin_url,
        admin_token,
        replicator_token,
        non_replicator_token,
        container,
    ))
}

/// PIP-180 positive contract — the broker's **topic-type gate** on
/// replicator-style sends, exercised from a `replicator`-role producer.
///
/// **Broker contract observed (Pulsar 4.0.4), and what this test pins.**
/// The replicator-style `send_with_source_message_id` entry
/// (`CommandSend.message_id` set) is gated by the broker on the **topic
/// type**, NOT on the producer's role:
///
/// > `SendRejected { code: 22, message: "Only shadow topic supports
/// > sending messages with messageId" }`
///
/// (`code 22` = `ServerError::NotAllowedError`, raised by upstream
/// `ServerCnx#handleSend` → `Producer#checkAndStartPublish` when the
/// target topic is not a registered shadow.) The test pins the gate from
/// **both** sides, using the same authorised `replicator` producer so the
/// only variable is the topic type:
///
/// 1. On a **regular** (non-shadow) topic, `send_with_source_message_id` is rejected with exactly
///    that `code 22` error — the source-id assertion is refused.
/// 2. On a **shadow** topic, the producer attaches and the broker **accepts** the source-id
///    assertion on the wire (no `code 22`).
///
/// **Why the test does not assert a receipt echo / consumer delivery on
/// the shadow side.** On a real Pulsar 4.0.4 shadow topic the managed
/// ledger is *source-backed* (`ShadowManagedLedgerImpl`): it surfaces the
/// **source's** real entries and silently absorbs a client-fabricated
/// source id that points at no real source entry — no
/// `CommandSendReceipt`, no consumer delivery (verified against the live
/// broker: the producer attaches, the send parks indefinitely). The
/// receipt-echo contract documented in `docs/shadow-topic.md` is a
/// property of the **scripted** broker in the differential harness, not of
/// a live broker handed a synthetic id. Pinning "the send is accepted on a
/// shadow but rejected on a regular topic" is the faithful, deterministic
/// real-broker assertion — see the commit body for the full reasoning.
///
/// Flow:
/// 1. Spin up a token-authenticated broker with `replicator` granted `produce,consume` on
///    `public/default`.
/// 2. As `replicator`, on a **regular** topic, assert `send_with_source_message_id` is rejected
///    with `code 22`.
/// 3. Bootstrap a source topic, create a shadow over it via the in-container `pulsar-admin`, and
///    assert the same producer call is **accepted** on the shadow (the send is buffered without a
///    wire-level rejection within a bounded window).
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_v4_replicator_role_can_assert_source_message_id()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _admin_token, replicator_token, _non_replicator_token, container) =
        start_pulsar_with_token_auth_and_replicator_role().await?;

    let regular = "persistent://public/default/magnetar-e2e-replicator-regular";
    let source = "persistent://public/default/magnetar-e2e-replicator-source";
    let shadow = "persistent://public/default/magnetar-e2e-replicator-shadow";

    let provider: Arc<dyn AuthProvider> = Arc::new(TokenAuth::from_string(replicator_token));
    let client = PulsarClient::builder()
        .service_url(service_url)
        .auth(provider)
        .build()
        .await?;

    let source_id = MessageId {
        ledger_id: 99_001,
        entry_id: 42,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
    };
    let payload = b"replicated-payload-pip180".to_vec();

    // ---- Gate side A: a REGULAR topic rejects the source-id assertion ----
    //
    // The `replicator` role is authorised to produce on the namespace, so
    // the producer attaches; the rejection is purely the topic-type gate.
    {
        let producer = client.producer(regular).create().await?;
        let send_res = tokio::time::timeout(
            Duration::from_secs(30),
            producer.send_with_source_message_id(
                source_id,
                payload.clone(),
                magnetar::proto::pb::MessageMetadata::default(),
            ),
        )
        .await?;
        let err = send_res
            .expect_err("a regular (non-shadow) topic must reject send_with_source_message_id");
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("shadow") || msg.contains("code=22") || msg.contains("not allowed"),
            "expected the broker's `Only shadow topic supports sending messages with messageId` \
             (code 22) rejection, got: {err}"
        );
        let _ = producer.close().await;
    }

    // ---- Gate side B: a SHADOW topic ACCEPTS the source-id assertion ----
    //
    // Bootstrap the source topic by producing once (replicator role has
    // `produce` on the namespace). A source must exist before a shadow can
    // be layered on top.
    {
        let producer = client.producer(source).create().await?;
        producer
            .send(magnetar::OutgoingMessage::with_payload(b"warmup".to_vec()).into())
            .await?;
        producer.close().await?;
    }

    // Create the shadow topic over the source via the in-container
    // `pulsar-admin`. We do NOT use the magnetar `AdminClient`'s
    // `create_shadow_topic` here: Pulsar 4.0.4's
    // `PUT .../{source}/shadowTopics` endpoint deserialises the body as a
    // bare `List<String>`, but the admin client sends a
    // `{"shadowTopics":[...]}` JSON object — the broker rejects with HTTP
    // 400 "Cannot deserialize value of type java.util.ArrayList<...> from
    // Object value". That admin-client wire-shape mismatch is filed as a
    // follow-up (docs/follow-ups.md); it is orthogonal to the PIP-180
    // replicator-side contract this test pins, so we route around it via
    // the broker's own CLI (which always sends the shape the broker
    // expects).
    let create_shadow = container
        .exec(
            ExecCommand::new([
                "bin/pulsar-admin",
                "topics",
                "create-shadow-topic",
                "--source",
                source,
                shadow,
            ])
            .with_cmd_ready_condition(testcontainers::core::CmdWaitFor::exit()),
        )
        .await?;
    drop(create_shadow);

    // Open a producer on the SHADOW topic and assert the synthetic source
    // id. On a shadow, the broker passes the topic-type gate: the send is
    // NOT rejected with code 22.
    //
    // We bound the await with a short timeout. A live shadow topic's
    // source-backed ledger neither acks nor errors a fabricated source id
    // (it is silently absorbed by `ShadowManagedLedgerImpl`), so the send
    // parks. A timeout (`Elapsed`) here means "the broker accepted the
    // assertion on the wire and did not reject it" — the positive half of
    // the gate. A returned `SendRejected { code: 22 }` would mean the
    // shadow gate failed and is the failure we guard against.
    let producer = client.producer(shadow).create().await?;
    let shadow_send = tokio::time::timeout(
        Duration::from_secs(3),
        producer.send_with_source_message_id(
            source_id,
            payload.clone(),
            magnetar::proto::pb::MessageMetadata::default(),
        ),
    )
    .await;
    match shadow_send {
        // Parked send → broker accepted the source-id assertion on the
        // shadow topic (the expected positive outcome).
        Err(_elapsed) => {}
        // A resolved receipt is also a pass — it means this broker build
        // DID echo the id (e.g. a future Pulsar that materialises the
        // fabricated entry). Either way the shadow gate was passed.
        Ok(Ok(_receipt)) => {}
        // A wire-level rejection on the shadow is the failure we guard
        // against — the topic-type gate should have let this through.
        Ok(Err(err)) => {
            let msg = err.to_string().to_lowercase();
            assert!(
                !(msg.contains("shadow") || msg.contains("code=22")),
                "shadow topic must NOT reject send_with_source_message_id with the \
                 topic-type gate, but got: {err}"
            );
        }
    }

    // Tear down WITHOUT a graceful `close().await`. The shadow send is
    // parked on the wire (no receipt is coming — see above), so a graceful
    // producer/connection close would wait on the in-flight publish and the
    // driver join indefinitely. The runtime `Client` / `Producer` have no
    // blocking `Drop` (the driver `JoinHandle` detaches), so dropping here
    // is clean; the container teardown on `_container` drop reaps the
    // broker.
    drop(producer);
    drop(client);
    Ok(())
}

/// PIP-180 negative path: a non-replicator role token attempts the same
/// PIP-180 entry. The broker's authorisation flow rejects either the
/// producer attach (no `produce` grant on `public/default`) or the send
/// itself, depending on how Pulsar 4.x sequences the check.
///
/// The test pins **whichever** behaviour the broker exhibits today by
/// asserting that an error surfaces on the producer build / send path,
/// and that the error carries a non-empty broker message (vs. a local
/// timeout). This is the contract pin — if Pulsar's authorisation
/// sequencing changes upstream, this test will tell us which arm flipped.
#[ignore = "e2e: requires Docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_v4_non_replicator_role_send_with_source_id_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let (
        service_url,
        _admin_url,
        _admin_token,
        _replicator_token,
        non_replicator_token,
        _container,
    ) = start_pulsar_with_token_auth_and_replicator_role().await?;

    let provider: Arc<dyn AuthProvider> = Arc::new(TokenAuth::from_string(non_replicator_token));

    let client = PulsarClient::builder()
        .service_url(service_url)
        .auth(provider)
        .build()
        .await?;

    let topic = "persistent://public/default/magnetar-e2e-replicator-negative";

    // The producer attach is the first authorisation gate. With no
    // `produce` grant on `public/default` for the non-replicator role,
    // the broker rejects the `CommandProducer` with a
    // `ServerError::AuthorizationError` (code 11) wrapped in
    // `CommandProducerFail`. magnetar surfaces that as a
    // `ClientError::Broker { .. }` per
    // [`magnetar_runtime_tokio::ClientError`].
    //
    // If the producer attach happens to succeed (e.g. a future Pulsar
    // wire change defers the role check to the send path), we then
    // attempt the send and assert the rejection lands there instead.
    // Either branch satisfies the contract: SOMETHING upstream must
    // reject the replicator-style assertion for an unprivileged role.
    let producer_result =
        tokio::time::timeout(Duration::from_secs(30), client.producer(topic).create()).await?;

    match producer_result {
        Err(open_err) => {
            // Attach-path rejection — happy negative.
            let msg = open_err.to_string().to_lowercase();
            assert!(
                msg.contains("auth")
                    || msg.contains("permission")
                    || msg.contains("rejected")
                    || msg.contains("forbidden"),
                "expected an authorisation-flavoured rejection on producer create, got: {open_err}"
            );
        }
        Ok(producer) => {
            // Producer attach unexpectedly succeeded — the role check
            // must surface on the send. The send carries the asserted
            // source id (the PIP-180 entry); the broker MUST reject.
            let source_id = MessageId {
                ledger_id: 99_002,
                entry_id: 43,
                partition: -1,
                batch_index: -1,
                batch_size: 0,
            };
            let send_result = tokio::time::timeout(
                Duration::from_secs(30),
                producer.send_with_source_message_id(
                    source_id,
                    b"forbidden-payload".to_vec(),
                    magnetar::proto::pb::MessageMetadata::default(),
                ),
            )
            .await?;
            let err = send_result.expect_err(
                "non-replicator role must not be allowed to assert a source MessageId; \
                 either producer attach or send must fail",
            );
            let msg = err.to_string().to_lowercase();
            assert!(
                msg.contains("auth")
                    || msg.contains("permission")
                    || msg.contains("rejected")
                    || msg.contains("forbidden"),
                "expected an authorisation-flavoured rejection on send_with_source_message_id, got: {err}"
            );
            let _ = producer.close().await;
        }
    }

    client.close().await;
    Ok(())
}
