// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for Stage 2 + Stage 3 supervised reconnect.
//!
//! Pins the contract that magnetar's
//! [`magnetar_proto::SupervisorConfig`]-driven reconnect loop transparently
//! rebuilds producers and consumers across a broker restart:
//!
//! * **Stage 2** — when the underlying TCP socket drops (broker stopped), the driver backs off,
//!   redials, and re-handshakes; the user-facing `Producer` / `Consumer` handles stay live.
//! * **Stage 3** — on every reconnect, `Connection::rebuild_producers` and `rebuild_consumers`
//!   re-issue `CommandProducer` / `CommandSubscribe`, so a `send` / `receive` issued after the
//!   restart succeeds without the user re-creating the handles.
//!
//! The simulation we can realistically run with `testcontainers` is "stop
//! the broker, start it back up." Mid-frame chaos (in-flight ops cut
//! mid-byte, virtual-clock backoff jitter) is moonpool territory and
//! lives in the deterministic-simulation engine.
//!
//! A forced `Connection::reset()` sub-test would require a public test
//! hook to drive the reset path from outside the driver loop; no such
//! hook is exposed today, so that path is exercised only indirectly via
//! the broker restart below.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_reconnect -- --nocapture
//! ```

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient, SupervisorConfig};
use magnetar_proto::{ControlledClusterFailover, ServiceUrlProvider};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::net::TcpListener;
use uuid::Uuid;

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
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

/// Start a Pulsar 4.x standalone container and return
/// (`service_url`, `admin_url`, `container_handle`). Mirrors the helper
/// in `e2e_pulsar.rs`; duplicated rather than shared because integration
/// test files cannot share modules without a `tests/common/` layout that
/// the rest of the suite does not adopt.
async fn start_pulsar() -> Result<
    (String, String, testcontainers::ContainerAsync<GenericImage>),
    Box<dyn std::error::Error>,
> {
    init_tracing();
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
        .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
        .with_wait_for(WaitFor::message_on_stdout(
            "Created namespace public/default",
        ))
        .with_startup_timeout(Duration::from_secs(120))
        .with_cmd(vec!["bin/pulsar".to_owned(), "standalone".to_owned()])
        .start()
        .await?;
    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let http_port = container.get_host_port_ipv4(BROKER_HTTP_PORT).await?;
    let service_url = format!("pulsar://{host}:{binary_port}");
    let admin_url = format!("http://{host}:{http_port}");
    Ok((service_url, admin_url, container))
}

/// Generous reconnect budget — the broker takes several seconds to come
/// back online after a restart, so we widen the backoff schedule beyond
/// the default. `max_attempts = None` keeps the supervised driver
/// redialing forever, mirroring the Java client default.
fn supervisor_for_e2e() -> SupervisorConfig {
    SupervisorConfig {
        initial_backoff: Duration::from_millis(200),
        max_backoff: Duration::from_secs(5),
        mandatory_stop: Duration::from_secs(180),
        max_attempts: None,
        ..SupervisorConfig::default()
    }
}

/// Stage 2 + Stage 3: stop the broker mid-session, restart it, verify
/// that producers and consumers built before the outage successfully
/// round-trip a message after the broker returns. Pins the
/// supervised-reconnect + transparent-rebuild contract end-to-end.
///
/// `testcontainers` 0.27 has no `restart_async`. `stop_with_timeout` +
/// `start` doesn't work either: `bin/pulsar standalone` exits cleanly on
/// `SIGTERM` and `container.start()` only re-runs `docker start`, which does
/// NOT re-execute the entrypoint. The container would come back alive but
/// with no broker inside, and the supervisor would spin on `Connection
/// refused` until the test budget ran out (the symptom we observed before
/// this fix). We shell out to `docker restart` instead — that re-runs the
/// entrypoint and brings the broker back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_supervised_reconnect_across_broker_restart() -> Result<(), Box<dyn std::error::Error>>
{
    let (service_url, _admin_url, container) = start_pulsar().await?;

    // testcontainers maps the broker's 6650 to a random host port and reuses
    // that port across `docker restart` only when the port is explicitly
    // pinned. Default `-P` random binding gets a fresh host port on every
    // restart — so wrap the URL in a `ControlledClusterFailover` and bump it
    // after the restart. The supervisor calls `get_service_url()` on every
    // redial, so a single `set_url` is enough to redirect the loop to the
    // new port.
    let failover = ControlledClusterFailover::new(service_url);
    let provider: std::sync::Arc<dyn ServiceUrlProvider> = std::sync::Arc::new(failover.clone());
    let client = PulsarClient::builder()
        .service_url_provider(provider)
        .enable_reconnect(supervisor_for_e2e())
        .operation_timeout(Duration::from_secs(60))
        .build()
        .await?;

    let topic = format!(
        "persistent://public/default/magnetar-e2e-reconnect-{}",
        Uuid::new_v4()
    );
    let subscription = format!("magnetar-e2e-reconnect-sub-{}", Uuid::new_v4());

    let producer = client.producer(&topic).create().await?;
    let consumer = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    // Sanity round-trip before the restart so we know the session is healthy.
    producer
        .send(OutgoingMessage::with_payload(b"before-restart".to_vec()).into())
        .await?;
    let pre = tokio::time::timeout(Duration::from_secs(10), consumer.receive()).await??;
    assert_eq!(pre.payload.as_ref(), b"before-restart");
    consumer.ack(pre.message_id).await?;

    // `docker restart` re-runs the entrypoint, so `bin/pulsar standalone`
    // comes back. SIGTERM with a 5 s grace mimics a real transient outage.
    tracing::info!("restarting pulsar container to force reconnect");
    let container_id = container.id().to_string();
    let status = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["restart", "--time", "5", &container_id])
            .status()
    })
    .await??;
    assert!(status.success(), "docker restart failed: {status:?}");
    // Re-query the (possibly new) host port and feed it to the supervisor's
    // failover provider — `docker restart` against an `-P` binding picks a
    // fresh random port.
    let new_host = container.get_host().await?;
    let new_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let new_url = format!("pulsar://{new_host}:{new_port}");
    tracing::info!(%new_url, "redirecting supervisor to post-restart port");
    failover.set_url(new_url);

    // The broker takes a few seconds to come back. The supervisor
    // handles retries; we poll send() until it succeeds or the budget
    // runs out so the test fails fast if the supervisor gave up.
    //
    // Each attempt is TIMEOUT-BOUNDED: a send future stays pending across
    // reconnects by design (transparent replay), so an environmental broker
    // death here would otherwise hang the binary forever instead of failing
    // the test (observed: a crashed standalone container under full-suite
    // load turned this loop into a 20-hour hang). Dropping a timed-out
    // `SendFut` is safe — its waker unregisters on drop, and the retry
    // publishes a fresh copy of the same payload (at-least-once; the
    // receive below tolerates duplicates by asserting payload equality).
    let payload = b"after-restart".to_vec();
    let mut attempts = 0u32;
    let send_outcome: Result<(), Box<dyn std::error::Error>> = loop {
        attempts += 1;
        if attempts > 30 {
            break Err(
                "send did not complete within 30 bounded attempts after broker restart".into(),
            );
        }
        match tokio::time::timeout(
            Duration::from_secs(10),
            producer.send(OutgoingMessage::with_payload(payload.clone()).into()),
        )
        .await
        {
            Ok(Ok(_message_id)) => break Ok(()),
            Ok(Err(e)) => {
                tracing::info!(?e, attempts, "producer send retry after broker restart");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(_elapsed) => {
                tracing::info!(attempts, "producer send attempt timed out; retrying");
            }
        }
    };
    send_outcome?;

    // The supervisor + rebuild path re-subscribes the consumer; the
    // message above must arrive without us re-creating the handle.
    let post = tokio::time::timeout(Duration::from_secs(60), consumer.receive()).await??;
    assert_eq!(
        post.payload.as_ref(),
        payload.as_slice(),
        "consumer must receive the post-restart message after supervised reconnect",
    );
    consumer.ack(post.message_id).await?;

    consumer.close().await?;
    producer.close().await?;
    client.close().await;
    Ok(())
}

/// Stage 3 transparent in-flight publish replay: queue several publishes while the
/// broker is stopped, then verify they all transparently complete on the user-facing
/// `SendFut`s after the broker restarts (no `Err` surfacing to the caller). Mirrors
/// Java `ProducerImpl#resendMessages` at-least-once parity: the user sees one
/// `SendFut` per call, and each one resolves with the broker-assigned `MessageId`
/// once the new session ack-cycles the replayed publish.
///
/// The publishes are issued *while the broker is stopped*. The driver enqueues each
/// one into the `ProducerState::pending` slab (after the `Producer` handle's send
/// future resolves the reservation half — see `ProducerImpl#sendAsync`). The reset
/// path on the next reconnect attempt snapshots them, and the post-handshake
/// `rebuild_producers` re-issues them onto the new session. The user's
/// `SendFut::poll` returns `Pending` across the whole cycle and resolves with
/// `Ok(MessageId)` when the broker's `CommandSendReceipt` arrives on the new
/// session.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_transparent_inflight_publish_replay_across_broker_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, container) = start_pulsar().await?;

    // See the producer-restart test for why we wrap in
    // `ControlledClusterFailover` — `docker restart` against testcontainers'
    // random port binding picks a fresh host port, and the supervisor must
    // be redirected.
    let failover = ControlledClusterFailover::new(service_url);
    let provider: std::sync::Arc<dyn ServiceUrlProvider> = std::sync::Arc::new(failover.clone());
    let client = PulsarClient::builder()
        .service_url_provider(provider)
        .enable_reconnect(supervisor_for_e2e())
        .operation_timeout(Duration::from_secs(120))
        .build()
        .await?;

    let topic = format!(
        "persistent://public/default/magnetar-e2e-inflight-{}",
        Uuid::new_v4()
    );
    let subscription = format!("magnetar-e2e-inflight-sub-{}", Uuid::new_v4());

    let producer = client.producer(&topic).create().await?;
    let consumer = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    // Sanity: pre-restart round-trip so we know the producer + consumer pair
    // is wired up.
    producer
        .send(OutgoingMessage::with_payload(b"sanity".to_vec()).into())
        .await?;
    let sanity = tokio::time::timeout(Duration::from_secs(10), consumer.receive()).await??;
    assert_eq!(sanity.payload.as_ref(), b"sanity");
    consumer.ack(sanity.message_id).await?;

    // Now stop the broker and fire several publishes while it's down. The driver
    // accepts them into `ProducerState::pending`; the reconnect path snapshots
    // them and rebuild_producers replays them on the new session.
    //
    // `docker restart` (not `stop_with_timeout` + `start`) is required — see
    // the long comment on the producer-restart test above for why
    // `container.start()` doesn't actually re-run `bin/pulsar standalone`.
    tracing::info!("stopping pulsar container to force in-flight replay");
    let container_id = container.id().to_string();
    container.stop_with_timeout(Some(5)).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Spawn `N` concurrent send futures. None of them complete until the
    // broker returns + replays.
    let n: usize = 5;
    let mut send_futs = Vec::with_capacity(n);
    for i in 0..n {
        let p = producer.clone();
        let payload = format!("replay-{i}").into_bytes();
        send_futs.push(tokio::spawn(async move {
            p.send(OutgoingMessage::with_payload(payload).into()).await
        }));
    }

    // `docker restart` re-runs `bin/pulsar standalone`. The
    // `container.stop_with_timeout` already stopped the container; `docker
    // restart` against a stopped container is equivalent to `docker start`
    // and runs the CMD again, which is what we need.
    tracing::info!("restarting pulsar container to validate transparent replay");
    let status = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["restart", "--time", "5", &container_id])
            .status()
    })
    .await??;
    assert!(status.success(), "docker restart failed: {status:?}");
    // Redirect supervisor to the post-restart port (random-mapped, see the
    // producer-restart test for the rationale).
    let new_host = container.get_host().await?;
    let new_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    failover.set_url(format!("pulsar://{new_host}:{new_port}"));

    // Each `SendFut` MUST resolve `Ok(_)` — no `Err` surfaces to the caller.
    // Stage 3 transparent replay = the user's future never observed the reset.
    for (i, fut) in send_futs.into_iter().enumerate() {
        let outcome = tokio::time::timeout(Duration::from_secs(120), fut)
            .await
            .unwrap_or_else(|_| panic!("send {i} did not resolve within 2 min"))?;
        if let Err(e) = outcome.as_ref() {
            panic!("send {i} failed after transparent replay: {e:?}");
        }
    }

    // Drain the consumer — the broker eventually delivers every replayed
    // payload (potentially with duplicates if the broker had already
    // persisted a publish before the disconnect; at-least-once semantics).
    // We assert that at minimum every replay-{i} payload arrives.
    let mut received: std::collections::HashSet<String> = std::collections::HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while received.len() < n && std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(10), consumer.receive()).await {
            Ok(Ok(msg)) => {
                let s = String::from_utf8_lossy(msg.payload.as_ref()).to_string();
                if s.starts_with("replay-") {
                    received.insert(s);
                }
                consumer.ack(msg.message_id).await?;
            }
            _ => break,
        }
    }
    for i in 0..n {
        let expected = format!("replay-{i}");
        assert!(
            received.contains(&expected),
            "broker must deliver every replayed payload {expected}, received={received:?}"
        );
    }

    consumer.close().await?;
    producer.close().await?;
    client.close().await;
    Ok(())
}

/// follow-ups §3.1 end-to-end: the MOONPOOL engine's transient
/// producer-open retry arm, exercised against a real restarting broker.
///
/// The other reconnect e2e tests drive the tokio façade
/// (`PulsarClient::builder()`), whose transient-retry arms have always been
/// wired. This one drives a `PulsarClient<MoonpoolEngine<TokioProviders>>`
/// (the moonpool engine over real host sockets) so the NEW moonpool arms run
/// against a real broker. The post-`docker restart` window is the natural
/// trigger: the broker answers the rebuild's `CommandProducer` with
/// `ServiceNotReady` ("Please redo the lookup") while the namespace bundle is
/// being re-acquired, the proto layer emits `ProducerOpenFailedTransient`, and
/// the moonpool driver's §3.1 leg re-looks-up + retries until the open is
/// served. A post-restart send round-trip succeeding proves the arm recovers
/// the connection rather than dead-ending the re-attach.
///
/// Gated on `feature = "moonpool"` because a moonpool-engine client cannot
/// compile without it (engine selection, not test-hiding — every other
/// `PulsarClient<MoonpoolEngine>` item in the façade carries the same gate; the
/// e2e suite always runs under `cargo test --all-features` per ADR-0046, so the
/// gate never hides the test from a normal run). No `#[ignore]`.
#[cfg(feature = "moonpool")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_moonpool_transient_producer_open_retry_across_broker_restart()
-> Result<(), Box<dyn std::error::Error>> {
    // The runtime engine (`runtime_moonpool::MoonpoolEngine`, with `::new`) is
    // the one `Client::connect_plain_supervised` consumes; `magnetar::MoonpoolEngine`
    // is the façade's engine MARKER, a different type.
    use magnetar::runtime_moonpool::{Client as MoonpoolClient, MoonpoolEngine};
    use moonpool_core::TokioProviders;

    let (service_url, _admin_url, container) = start_pulsar().await?;

    // Strip the `pulsar://` scheme: the moonpool engine dials a raw
    // `host:port` authority. The failover provider keeps the scheme — the
    // supervised reconnect path strips it on every redial.
    let host_port = service_url
        .strip_prefix("pulsar://")
        .unwrap_or(&service_url)
        .to_owned();
    let failover = ControlledClusterFailover::new(service_url);
    let provider: std::sync::Arc<dyn ServiceUrlProvider> = std::sync::Arc::new(failover.clone());

    let config = magnetar_proto::ConnectionConfig {
        supervisor: Some(supervisor_for_e2e()),
        operation_timeout: Duration::from_secs(120),
        ..Default::default()
    };
    let runtime_client = MoonpoolClient::connect_plain_supervised(
        &MoonpoolEngine::new(TokioProviders::new()),
        &host_port,
        config,
        Some(provider),
        None,
    )
    .await?;
    let client = PulsarClient::from_runtime_client(runtime_client);

    let topic = format!(
        "persistent://public/default/magnetar-e2e-mp-transient-{}",
        Uuid::new_v4()
    );
    let producer = client.producer(&topic).create().await?;

    // Sanity round-trip before the restart so we know the session is healthy.
    producer
        .send(OutgoingMessage::with_payload(b"before-restart".to_vec()).into())
        .await?;

    // `docker restart` re-runs `bin/pulsar standalone`; the post-restart
    // bundle-not-served window is what drives the broker to answer the
    // rebuild's `CommandProducer` with a transient `ServiceNotReady`.
    tracing::info!("restarting pulsar container to drive the moonpool transient-retry arm");
    let container_id = container.id().to_string();
    let status = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["restart", "--time", "5", &container_id])
            .status()
    })
    .await??;
    assert!(status.success(), "docker restart failed: {status:?}");
    let new_host = container.get_host().await?;
    let new_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    failover.set_url(format!("pulsar://{new_host}:{new_port}"));

    // Poll send() until it succeeds: the supervised driver redials, rebuilds
    // the producer, hits the transient `ServiceNotReady` on the rebuild open,
    // and the §3.1 leg re-looks-up + retries until the broker serves the
    // attach. Each attempt is timeout-bounded so an environmental broker death
    // fails the test fast instead of hanging.
    let payload = b"after-restart".to_vec();
    let mut attempts = 0u32;
    let send_outcome: Result<(), Box<dyn std::error::Error>> = loop {
        attempts += 1;
        if attempts > 30 {
            break Err(
                "moonpool send did not complete within 30 bounded attempts after broker restart"
                    .into(),
            );
        }
        match tokio::time::timeout(
            Duration::from_secs(10),
            producer.send(OutgoingMessage::with_payload(payload.clone()).into()),
        )
        .await
        {
            Ok(Ok(_message_id)) => break Ok(()),
            Ok(Err(e)) => {
                tracing::info!(
                    ?e,
                    attempts,
                    "moonpool producer send retry after broker restart"
                );
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(_elapsed) => {
                tracing::info!(
                    attempts,
                    "moonpool producer send attempt timed out; retrying"
                );
            }
        }
    };
    send_outcome?;

    producer.close().await?;
    client.close().await;
    Ok(())
}

/// ADR-0060 regression guard: a transient `SessionLost` on a lookup behind
/// `subscribe` / `producer.create()` must NOT leak to the caller as
/// `ClientError::Other("unexpected lookup outcome: SessionLost…")`. Asserts the
/// error string carries neither marker — the engine must have re-issued the
/// lookup transparently instead.
fn assert_no_session_lost_leak<E: std::fmt::Debug>(e: &E, op: &str) {
    let msg = format!("{e:?}");
    assert!(
        !msg.contains("unexpected lookup outcome") && !msg.contains("SessionLost"),
        "{op} during reconnect leaked a SessionLost-derived error (ADR-0060 regression): {e:?}",
    );
}

/// ADR-0060 / follow-ups §4.1: a `subscribe()` / `producer.create()` issued
/// DURING a supervised reconnect — so its in-flight `CommandLookupTopic` races
/// the supervisor's `reset()` and is severed with `OpOutcome::SessionLost` —
/// must SUCCEED transparently once the broker is back, NOT surface
/// `ClientError::Other("unexpected lookup outcome: SessionLost…")`. The engine
/// re-issues the severed lookup against the fresh session (bounded by
/// `MAX_LOOKUP_SESSION_REISSUES`), mirroring Java's lookup-after-reset.
///
/// Strategy: warm up a healthy session, restart the broker, then immediately —
/// while the supervisor is mid-reconnect — open a NEW producer and subscribe a
/// NEW consumer on a fresh topic. The open/subscribe future stays pending
/// across the reconnect and resolves `Ok` via the re-issued lookup; a regression
/// (the pre-ADR-0060 catch-all) would surface `ClientError::Other` instead. A
/// round-trip on the new handles confirms they are genuinely live.
///
/// Runs as a regular test under `cargo test` (ADR-0046; no `#[ignore]`, no
/// feature gate). Needs Docker + `apachepulsar/pulsar`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_subscribe_during_reconnect_reissues_lookup_transparently()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, container) = start_pulsar().await?;

    let failover = ControlledClusterFailover::new(service_url);
    let provider: std::sync::Arc<dyn ServiceUrlProvider> = std::sync::Arc::new(failover.clone());
    let client = PulsarClient::builder()
        .service_url_provider(provider)
        .enable_reconnect(supervisor_for_e2e())
        .operation_timeout(Duration::from_secs(120))
        .build()
        .await?;

    // Warm up a healthy session so the supervised driver is running before we
    // perturb it (a fresh-topic lookup is what we want to race against reset).
    let warmup_topic = format!(
        "persistent://public/default/magnetar-e2e-reconnect-warmup-{}",
        Uuid::new_v4()
    );
    let warmup = client.producer(&warmup_topic).create().await?;
    warmup
        .send(OutgoingMessage::with_payload(b"warmup".to_vec()).into())
        .await?;

    // Restart the broker to open the reconnect window.
    tracing::info!("restarting pulsar container to race a fresh lookup against reset");
    let container_id = container.id().to_string();
    let status = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["restart", "--time", "5", &container_id])
            .status()
    })
    .await??;
    assert!(status.success(), "docker restart failed: {status:?}");
    let new_host = container.get_host().await?;
    let new_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    failover.set_url(format!("pulsar://{new_host}:{new_port}"));

    // Open a NEW producer + subscribe a NEW consumer on a fresh topic right now,
    // while the supervisor is reconnecting. Their in-flight lookups race the
    // reset; ADR-0060 must re-issue them transparently. Each attempt is
    // timeout-bounded (a lookup parks across the reconnect by design); we retry
    // until the broker is fully back, then assert SUCCESS — and crucially, that
    // no attempt ever surfaced a `SessionLost`-derived `Other` error.
    let topic = format!(
        "persistent://public/default/magnetar-e2e-reconnect-newtopic-{}",
        Uuid::new_v4()
    );
    let subscription = format!("magnetar-e2e-reconnect-newsub-{}", Uuid::new_v4());

    let mut attempts = 0u32;
    let consumer = loop {
        attempts += 1;
        assert!(
            attempts <= 30,
            "subscribe during reconnect did not complete within 30 bounded attempts"
        );
        match tokio::time::timeout(
            Duration::from_secs(10),
            client
                .consumer(&topic)
                .subscription(&subscription)
                .subscription_type(SubType::Exclusive)
                .initial_position(InitialPosition::Earliest)
                .subscribe(),
        )
        .await
        {
            Ok(Ok(c)) => break c,
            Ok(Err(e)) => {
                // ADR-0060 contract: a transient SessionLost must NOT leak as
                // `Other("unexpected lookup outcome: SessionLost…")`.
                assert_no_session_lost_leak(&e, "subscribe");
                tracing::info!(?e, attempts, "subscribe retry while broker comes back");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(_elapsed) => {
                tracing::info!(attempts, "subscribe attempt timed out; retrying");
            }
        }
    };

    let producer = loop {
        attempts += 1;
        assert!(
            attempts <= 60,
            "producer create during reconnect did not complete within the attempt budget"
        );
        match tokio::time::timeout(Duration::from_secs(10), client.producer(&topic).create()).await
        {
            Ok(Ok(p)) => break p,
            Ok(Err(e)) => {
                assert_no_session_lost_leak(&e, "producer create");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(_elapsed) => {}
        }
    };

    // The new handles are genuinely live: a round-trip succeeds.
    producer
        .send(OutgoingMessage::with_payload(b"after-reconnect-newtopic".to_vec()).into())
        .await?;
    let got = tokio::time::timeout(Duration::from_secs(60), consumer.receive()).await??;
    assert_eq!(
        got.payload.as_ref(),
        b"after-reconnect-newtopic",
        "the new consumer subscribed during reconnect must receive on the fresh topic",
    );
    consumer.ack(got.message_id).await?;

    consumer.close().await?;
    producer.close().await?;
    let _ = warmup.close().await;
    client.close().await;
    Ok(())
}

/// Spawn a loopback "handshake-failing" stub that models a docker-proxy / LB
/// whose backend is down: it ACCEPTS the TCP connection (so the supervisor's
/// `Transport::connect` dial succeeds) but immediately CLOSES it without ever
/// speaking the Pulsar `CONNECTED` reply, so the post-dial handshake fails on
/// every reconnect cycle. Returns the `pulsar://host:port` URL to point the
/// supervisor's `ServiceUrlProvider` at.
///
/// A real broker always completes the handshake, so this stub is the only way
/// to exercise the ADR-0061 budget end-to-end: counting EVERY post-dial failure
/// (here: TCP-accept-then-close) against `max_attempts`, the supervisor gives up
/// at the budget instead of retrying forever (the pre-ADR-0061 livelock behind a
/// TCP-accepting proxy).
async fn spawn_handshake_failing_stub() -> Result<String, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        // Accept-then-drop forever: each accepted socket is closed the instant
        // it leaves scope, so the client's CONNECT is met with a peer close.
        while let Ok((stream, _peer)) = listener.accept().await {
            drop(stream);
        }
    });
    Ok(format!("pulsar://{}:{}", addr.ip(), addr.port()))
}

/// ADR-0061 / follow-ups §3.2: the supervisor's `max_attempts` budget
/// must count POST-DIAL handshake failures, not just TCP-dial failures. Behind a
/// TCP-accepting proxy whose backend is down — the storm class the anti-thrash
/// supervision was built for — the dial always succeeds while the Pulsar
/// handshake never completes; pre-ADR-0061 the per-cycle counter reset to 0 each
/// outer iteration, so the budget never fired and the driver retried forever.
///
/// Strategy: establish a healthy session against the real broker (so the
/// supervised driver is running), then redirect the supervisor at a loopback
/// stub that accepts TCP and immediately closes (handshake fails every cycle),
/// drop the real connection by restarting the broker, and assert the client's
/// operations TERMINALLY fail PROMPTLY (the supervisor gave up at `max_attempts`,
/// latched `no_driver`, and `fail_all_pending` fired) rather than hanging. With
/// `max_attempts = None` (the default) this loop would never terminate.
///
/// Runs as a regular test under `cargo test` (ADR-0046; no `#[ignore]`, no
/// feature gate). Needs Docker + `apachepulsar/pulsar`. A localhost stub-acceptor
/// stands in for the unavailable "TCP-accept-but-handshake-fail" real broker
/// (documented above).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_supervisor_gives_up_at_max_attempts_behind_handshake_failing_endpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, container) = start_pulsar().await?;

    let failover = ControlledClusterFailover::new(service_url);
    let provider: std::sync::Arc<dyn ServiceUrlProvider> = std::sync::Arc::new(failover.clone());
    // Finite, small budget so the give-up fires fast. Tight backoff so the
    // five exhausting cycles complete well within the test timeout.
    let supervisor = SupervisorConfig {
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(200),
        mandatory_stop: Duration::from_secs(30),
        max_attempts: Some(3),
        ..SupervisorConfig::default()
    };
    let client = PulsarClient::builder()
        .service_url_provider(provider)
        .enable_reconnect(supervisor)
        .operation_timeout(Duration::from_secs(30))
        .build()
        .await?;

    // Warm up a healthy session so the supervised driver is running before we
    // perturb it.
    let topic = format!(
        "persistent://public/default/magnetar-e2e-giveup-{}",
        Uuid::new_v4()
    );
    let producer = client.producer(&topic).create().await?;
    producer
        .send(OutgoingMessage::with_payload(b"before-giveup".to_vec()).into())
        .await?;

    // Redirect the supervisor at the handshake-failing stub: every future
    // redial dials the stub (TCP accepts) and is met with a peer close (no
    // CONNECTED), so each reconnect cycle is a post-dial handshake failure that
    // counts against the budget.
    let stub_url = spawn_handshake_failing_stub().await?;
    tracing::info!(%stub_url, "redirecting supervisor at the handshake-failing stub");
    failover.set_url(stub_url);

    // Drop the real connection so the supervisor enters its reconnect loop and
    // starts dialing the stub. `docker stop` (not restart) keeps the broker
    // down so the only reachable endpoint is the stub.
    let container_id = container.id().to_string();
    let status = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["stop", "--time", "5", &container_id])
            .status()
    })
    .await??;
    assert!(status.success(), "docker stop failed: {status:?}");

    // The supervisor now dials the stub: TCP accept + immediate close = a
    // post-dial handshake failure on every cycle. With `max_attempts = 3` it
    // gives up by the 4th cycle, latches `no_driver`, and `fail_all_pending`
    // terminalizes pending ops. A `send()` issued after give-up must therefore
    // resolve with a TERMINAL ERROR PROMPTLY — the timeout is the no-hang guard,
    // the regression this test exists to catch (pre-ADR-0061 it looped forever).
    let send_res = tokio::time::timeout(
        Duration::from_secs(20),
        producer.send(OutgoingMessage::with_payload(b"after-giveup".to_vec()).into()),
    )
    .await
    .expect("send must resolve PROMPTLY once the supervisor gives up, not hang forever");
    assert!(
        send_res.is_err(),
        "send must surface a terminal error once the supervisor exhausts max_attempts \
         behind a TCP-accepting handshake-failing endpoint, got Ok({:?})",
        send_res.ok(),
    );

    // The client reports the connection as down after the give-up.
    assert!(
        !client.is_connected(),
        "connection must be down after the supervisor gives up"
    );

    client.close().await;
    Ok(())
}
