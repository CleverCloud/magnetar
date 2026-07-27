// SPDX-License-Identifier: Apache-2.0

//! End-to-end test for `ConsumerEventListener` push delivery (issue #348,
//! ADR-0081) against a real Apache Pulsar 4.x standalone broker via
//! `testcontainers-rs`.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Requires Docker +
//! `apachepulsar/pulsar` reachable.
//!
//! ## What this proves end-to-end
//!
//! 1. Two `Failover` consumers on the same subscription: the broker elects one active, the other
//!    stand-by.
//! 2. Each consumer's `ConsumerEventListener` observes its own election outcome.
//! 3. Closing the active consumer promotes the stand-by — its listener fires
//!    `ConsumerEvent::BecameActive` and `Consumer::is_active()` flips to `Some(true)`, both within
//!    a generous bounded timeout (issue #307's flow re-arm plus this issue's event surface, driven
//!    by the SAME `CommandActiveConsumerChange` frame).

use std::sync::Arc;
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{ConsumerEvent, ConsumerEventListener, PulsarClient};
use parking_lot::Mutex;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use uuid::Uuid;

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

/// JVM budget for the `pulsar standalone` container.
/// The image default (`-Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g`) costs ~2.3 GiB RSS per
/// container; libtest runs up to `nproc` e2e tests in parallel and the PIP-33 compose fixture
/// stays up for the whole run, which overcommits the 16 GiB GitHub runner and stalls brokers
/// into `operation_timeout` failures. See `docs/testing.md` § "e2e container memory budget".
const PULSAR_MEM_LIMIT: &str = "-Xms256m -Xmx1g -XX:MaxDirectMemorySize=1g";

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
        .with_startup_timeout(Duration::from_mins(2))
        .with_env_var("PULSAR_MEM", PULSAR_MEM_LIMIT)
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

/// Poll `probe` every 200ms until it returns `Some`, or panic with `msg` once
/// `deadline` elapses. Bounded busy-wait — the broker-side election / promotion
/// settle time is not otherwise observable from the client.
async fn wait_for<T, F: Fn() -> Option<T>>(deadline: Duration, msg: &str, probe: F) -> T {
    let start = std::time::Instant::now();
    loop {
        if let Some(v) = probe() {
            return v;
        }
        assert!(start.elapsed() < deadline, "{msg}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn e2e_consumer_event_listener_fires_on_failover_promotion()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let suffix = Uuid::new_v4().simple().to_string();
    let topic = format!("persistent://public/default/magnetar-e2e-cel-{suffix}");
    let subscription = format!("magnetar-e2e-cel-{suffix}");

    let consumer_a = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Failover)
        .name("cel-consumer-a")
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let consumer_b = client
        .consumer(&topic)
        .subscription(&subscription)
        .subscription_type(SubType::Failover)
        .name("cel-consumer-b")
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    // Attach an event listener to each, recording every observed transition
    // into its own edge log.
    let edges_a: Arc<Mutex<Vec<ConsumerEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let edges_b: Arc<Mutex<Vec<ConsumerEvent>>> = Arc::new(Mutex::new(Vec::new()));

    let listener_a: ConsumerEventListener = {
        let edges = edges_a.clone();
        Arc::new(move |ev: ConsumerEvent| {
            edges.lock().push(ev);
        })
    };
    let listener_b: ConsumerEventListener = {
        let edges = edges_b.clone();
        Arc::new(move |ev: ConsumerEvent| {
            edges.lock().push(ev);
        })
    };

    let handle_a = magnetar::spawn_consumer_event_listener(consumer_a.clone(), listener_a);
    let handle_b = magnetar::spawn_consumer_event_listener(consumer_b.clone(), listener_b);

    // Broker takes a beat to elect the active consumer once both have
    // registered (`activeConsumerFailoverDelayTimeMillis`, default 1s).
    // `is_active()` reads the per-slot state directly — no probe message
    // needed to determine which side won the election.
    let active_is_a = wait_for(
        Duration::from_secs(15),
        "broker never elected an active Failover consumer (no CommandActiveConsumerChange \
         observed by either side)",
        || match (consumer_a.is_active(), consumer_b.is_active()) {
            (Some(true), _) => Some(true),
            (_, Some(true)) => Some(false),
            _ => None,
        },
    )
    .await;
    let active_name = if active_is_a {
        "cel-consumer-a"
    } else {
        "cel-consumer-b"
    };
    let standby_name = if active_is_a {
        "cel-consumer-b"
    } else {
        "cel-consumer-a"
    };
    eprintln!("phase-1: broker elected {active_name} as active");

    // The winner's own listener must have observed its promotion. `is_active()`
    // and the ring entry the listener pops are set atomically together, but the
    // listener poller task still needs its own scheduler turn to pop + fire the
    // callback — bound the wait instead of asserting synchronously.
    let winner_edges = if active_is_a {
        edges_a.clone()
    } else {
        edges_b.clone()
    };
    wait_for(
        Duration::from_secs(5),
        &format!("{active_name}'s listener never observed its own BecameActive promotion"),
        || {
            winner_edges
                .lock()
                .contains(&ConsumerEvent::BecameActive)
                .then_some(())
        },
    )
    .await;

    // Split into active vs stand-by so the close-self move is unambiguous.
    let (active_consumer, standby_consumer, standby_edges) = if active_is_a {
        (consumer_a, consumer_b, edges_b.clone())
    } else {
        (consumer_b, consumer_a, edges_a.clone())
    };

    // Close the active consumer → broker promotes the stand-by.
    eprintln!("phase-2: closing {active_name}, expecting {standby_name} to take over");
    active_consumer.close().await?;

    // The stand-by's `is_active()` must flip to `Some(true)`.
    wait_for(
        Duration::from_secs(20),
        &format!(
            "{standby_name} never observed promotion after {active_name} closed \
             (is_active() stayed != Some(true))"
        ),
        || (standby_consumer.is_active() == Some(true)).then_some(()),
    )
    .await;

    // And the stand-by's listener must have fired BecameActive for the
    // promotion (in addition to whatever it may have logged during the
    // initial election, which for the loser is nothing — Failover only
    // notifies the side whose state actually changed).
    wait_for(
        Duration::from_secs(5),
        &format!("{standby_name}'s listener never observed BecameActive after promotion"),
        || {
            standby_edges
                .lock()
                .contains(&ConsumerEvent::BecameActive)
                .then_some(())
        },
    )
    .await;

    handle_a.close().await;
    handle_b.close().await;
    standby_consumer.close().await?;
    client.close().await;

    Ok(())
}
