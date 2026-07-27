// SPDX-License-Identifier: Apache-2.0

//! End-to-end reproduction for issue #314: a single `PulsarClient` is forced
//! onto ONE TCP connection per broker, so every producer to that broker shares
//! the same connection (single-connection-per-broker, ADR-0039).
//!
//! ## How we observe the connection count
//!
//! Each TCP connection from the client has a distinct source `host:port`. The
//! broker reports, per topic, the source address of every publisher in its
//! topic-stats `publishers[].address`. So the number of *distinct* publisher
//! addresses the broker sees for a topic equals the number of TCP connections
//! the client opened to that broker for those producers.
//!
//! Today that number is always `1`, no matter how many producers an application
//! opens — which is exactly what forces apps to hand-roll a pool of
//! `PulsarClient`s to get produce parallelism (the motivation for #314). The
//! `connections_per_broker` knob (Java `ClientBuilder#connectionsPerBroker`)
//! will let it grow.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Requires Docker.

use std::collections::BTreeSet;
use std::time::Duration;

use magnetar::{OutgoingMessage, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

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

/// Start a Pulsar 4.x standalone container and return (`service_url`,
/// `admin_url`, `container_handle`).
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

/// Distinct client source addresses the broker sees for a topic's publishers.
/// One distinct address == one TCP connection the client opened to the broker.
async fn distinct_publisher_addresses(
    admin: &magnetar_admin::AdminClient,
    topic: &str,
) -> Result<BTreeSet<String>, Box<dyn std::error::Error>> {
    let stats = admin.topic_stats(topic).await?;
    let mut addrs = BTreeSet::new();
    for publisher in &stats.publishers {
        if let Some(addr) = publisher.get("address").and_then(serde_json::Value::as_str) {
            addrs.insert(addr.to_owned());
        }
    }
    Ok(addrs)
}

/// Reproduction: every producer a single client opens to a broker is forced
/// onto ONE TCP connection. Opening four named producers on one topic still
/// shows the broker exactly one distinct publisher source address.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_314_all_producers_share_one_connection() -> Result<(), Box<dyn std::error::Error>> {
    const PRODUCERS: usize = 4;

    let (service_url, admin_url, _container) = start_pulsar().await?;

    let admin = magnetar_admin::AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-314-single-conn";

    // Open several distinct producers on the SAME topic. Each carries a unique
    // name (the broker rejects duplicate producer names on one topic), and each
    // sends one message so the broker registers and flushes the publisher.
    let mut producers = Vec::with_capacity(PRODUCERS);
    for i in 0..PRODUCERS {
        let producer = client
            .producer(topic)
            .name(format!("repro-314-{i}"))
            .create()
            .await?;
        producer
            .send(OutgoingMessage::with_payload(format!("hello-{i}").into_bytes()).into())
            .await?;
        producers.push(producer);
    }

    let addrs = distinct_publisher_addresses(&admin, topic).await?;
    eprintln!(
        "issue #314 repro: {PRODUCERS} producers -> {} distinct connection(s): {addrs:?}",
        addrs.len()
    );

    // The reproduction: all four producers are forced onto a single TCP
    // connection to the broker. There is no client-side knob to spread them.
    assert_eq!(
        addrs.len(),
        1,
        "issue #314: all {PRODUCERS} producers share a single connection to the broker, \
         leaving applications no way to get connection-level produce parallelism"
    );

    for producer in producers {
        producer.close().await?;
    }
    client.close().await;
    Ok(())
}

/// The fix: `ClientBuilder::connections_per_broker(n)` (Java
/// `ClientBuilder#connectionsPerBroker`) makes one client open up to `n`
/// connections per broker and round-robin producers across them — so the same
/// four producers now spread over multiple distinct broker connections instead
/// of contending on one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_314_connections_per_broker_spreads_producers() -> Result<(), Box<dyn std::error::Error>>
{
    const PRODUCERS: usize = 4;

    let (service_url, admin_url, _container) = start_pulsar().await?;

    let admin = magnetar_admin::AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .connections_per_broker(PRODUCERS)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-314-fanout";

    let mut producers = Vec::with_capacity(PRODUCERS);
    for i in 0..PRODUCERS {
        let producer = client
            .producer(topic)
            .name(format!("fanout-314-{i}"))
            .create()
            .await?;
        producer
            .send(OutgoingMessage::with_payload(format!("hello-{i}").into_bytes()).into())
            .await?;
        producers.push(producer);
    }

    let addrs = distinct_publisher_addresses(&admin, topic).await?;
    eprintln!(
        "issue #314 fan-out: {PRODUCERS} producers, connections_per_broker={PRODUCERS} -> {} \
         distinct connection(s): {addrs:?}",
        addrs.len()
    );

    // The fix: the producers no longer all share a single connection — they are
    // spread across multiple broker connections (up to `connections_per_broker`).
    assert!(
        addrs.len() > 1,
        "connections_per_broker({PRODUCERS}) must spread producers across multiple broker \
         connections, but they all shared {} address(es): {addrs:?}",
        addrs.len()
    );

    for producer in producers {
        producer.close().await?;
    }
    client.close().await;
    Ok(())
}
