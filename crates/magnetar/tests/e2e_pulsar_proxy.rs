// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the Apache Pulsar Proxy connection model
//! (ADR-0039 / issue #15).
//!
//! Spins up:
//!
//! 1. `apachepulsar/pulsar:4.0.4` in **standalone** mode (embedded
//!    Zookeeper on `2181`, broker binary on `6650`, admin REST on
//!    `8080`).
//! 2. `apachepulsar/pulsar:4.0.4` in **proxy** mode, pointed at the
//!    standalone container's Zookeeper via `--zookeeper-servers
//!    <standalone-host>:<mapped-zk-port>` and serving the proxy binary
//!    protocol on a random host port.
//!
//! The client connects to the **proxy** address, opens a producer, sends
//! a payload, then opens a consumer and reads it back. The whole
//! round-trip rides on the proxy multi-connection path: the bootstrap
//! connection handles `CommandLookupTopic` (answered with
//! `proxy_through_service_url = true`), and a pinned per-broker pool
//! entry handles `CommandProducer` + `CommandSend` + `CommandSubscribe`.
//! Without ADR-0039's pool wiring this test would observe the
//! ~90 ms reconnect storm from issue #15.
//!
//! ## Gating + runtime
//!
//! Gated behind the `e2e` feature flag (ADR-0021 allows `#[ignore]` for
//! environment dependencies). Run with:
//!
//! ```sh
//! cargo test --features e2e -p magnetar --test e2e_pulsar_proxy -- --include-ignored --nocapture
//! ```
//!
//! Requires Docker on the host. Two containers are started: a standalone
//! broker (~30 s startup), and a proxy (~10 s startup once the broker is
//! healthy). The proxy needs network reachability back to the
//! standalone's mapped Zookeeper port. On Linux the test uses the
//! standalone container's bridge IP (`get_bridge_ip_address`) so the
//! proxy can reach Zookeeper directly. Falls back to `host.docker.internal`
//! when the bridge IP is unavailable (Docker Desktop on macOS / Windows).
//!
//! CI runs these only in the dedicated `e2e` workflow.

#![cfg(feature = "e2e")]

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "4.0.4";
const STANDALONE_BINARY_PORT: u16 = 6650;
const STANDALONE_ZK_PORT: u16 = 2181;
const STANDALONE_HTTP_PORT: u16 = 8080;
const PROXY_BINARY_PORT: u16 = 6650;

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

/// Start the Pulsar standalone container and return its host-mapped binary
/// port, admin port, zk port, plus the container handle (drop = stop).
async fn start_standalone() -> Result<
    (
        String,                                                  // service_url (host:port for binary protocol)
        u16,                                                     // mapped zk port
        testcontainers::ContainerAsync<GenericImage>,            // standalone handle
    ),
    Box<dyn std::error::Error>,
> {
    init_tracing();
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(STANDALONE_BINARY_PORT))
        .with_exposed_port(ContainerPort::Tcp(STANDALONE_HTTP_PORT))
        .with_exposed_port(ContainerPort::Tcp(STANDALONE_ZK_PORT))
        .with_wait_for(WaitFor::message_on_stdout(
            "Created namespace public/default",
        ))
        .with_startup_timeout(Duration::from_secs(120))
        .with_cmd(vec!["bin/pulsar".to_owned(), "standalone".to_owned()])
        .start()
        .await?;
    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(STANDALONE_BINARY_PORT).await?;
    let zk_port = container.get_host_port_ipv4(STANDALONE_ZK_PORT).await?;
    let service_url = format!("pulsar://{host}:{binary_port}");
    Ok((service_url, zk_port, container))
}

/// Start the Pulsar proxy pointed at the standalone's Zookeeper. Returns
/// the proxy's `pulsar://...` URL plus the container handle.
async fn start_proxy(
    zk_host: &str,
    zk_port: u16,
) -> Result<
    (
        String, // proxy service_url
        testcontainers::ContainerAsync<GenericImage>,
    ),
    Box<dyn std::error::Error>,
> {
    let zk_servers = format!("{zk_host}:{zk_port}");
    // The Apache Pulsar Proxy reads its config from env vars when run with
    // `bin/apply-config-from-env.py conf/proxy.conf`. We point
    // `zookeeperServers` at the standalone container's bridge address and
    // start the proxy.
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(PROXY_BINARY_PORT))
        .with_wait_for(WaitFor::message_on_stdout(
            "Started ProxyService at",
        ))
        .with_startup_timeout(Duration::from_secs(60))
        .with_env_var("PULSAR_PREFIX_zookeeperServers", &zk_servers)
        .with_env_var("PULSAR_PREFIX_configurationStoreServers", &zk_servers)
        .with_env_var("PULSAR_PREFIX_servicePort", PROXY_BINARY_PORT.to_string())
        .with_cmd(vec![
            "bash".to_owned(),
            "-c".to_owned(),
            "bin/apply-config-from-env.py conf/proxy.conf && bin/pulsar proxy".to_owned(),
        ])
        .start()
        .await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(PROXY_BINARY_PORT).await?;
    let url = format!("pulsar://{host}:{port}");
    Ok((url, container))
}

/// Resolve a reachable hostname for the proxy container to use to talk to
/// the standalone container's mapped Zookeeper port. On Linux we use the
/// host's bridge IP (`172.17.0.1` by default); on Docker Desktop we fall
/// back to `host.docker.internal`. The env var
/// `MAGNETAR_E2E_DOCKER_HOST_GATEWAY` lets the user override.
fn docker_host_gateway() -> String {
    if let Ok(s) = std::env::var("MAGNETAR_E2E_DOCKER_HOST_GATEWAY") {
        return s;
    }
    // 172.17.0.1 is the default Docker bridge gateway on Linux. On
    // Docker Desktop the user should override via the env var.
    "172.17.0.1".to_owned()
}

#[ignore = "e2e: requires Docker + reachable host gateway between proxy and standalone"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_produce_consume_through_pulsar_proxy() -> Result<(), Box<dyn std::error::Error>> {
    // Step 1: standalone broker.
    let (_standalone_url, zk_port, _standalone) = start_standalone().await?;
    let gateway = docker_host_gateway();

    // Step 2: proxy in front, talking to standalone's mapped zk.
    let (proxy_url, _proxy) = start_proxy(&gateway, zk_port).await?;

    // Step 3: connect to the PROXY and exercise the multi-conn path.
    let client = PulsarClient::builder()
        .service_url(&proxy_url)
        .operation_timeout(Duration::from_secs(30))
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-e2e-proxy-roundtrip";

    let producer = client.producer(topic).create().await?;
    let payloads: &[&[u8]] = &[b"hello-proxy", b"pulsar-4", b"magnetar"];
    for p in payloads {
        producer
            .send(OutgoingMessage::with_payload(p.to_vec()).into())
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .consumer(topic)
        .subscription("magnetar-e2e-proxy")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    let mut received = Vec::new();
    for _ in 0..payloads.len() {
        let msg = consumer.receive().await?;
        received.push(msg.payload.to_vec());
        consumer.ack(msg.message_id).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(
        received,
        payloads.iter().map(|p| p.to_vec()).collect::<Vec<_>>(),
        "messages produced through the proxy must round-trip end-to-end"
    );
    Ok(())
}
