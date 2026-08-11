// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the Apache Pulsar Proxy connection model
//! (ADR-0039 / issue #15).
//!
//! Spins up:
//!
//! 1. The shared fixture's default Pulsar image (`common/pulsar.rs`, ADR-0115) in **standalone**
//!    mode (embedded Zookeeper on `2181`, broker binary on `6650`, admin REST on `8080`).
//! 2. The same image in **proxy** mode, pointed at the standalone container's Zookeeper via
//!    `--zookeeper-servers <standalone-host>:<mapped-zk-port>` and serving the proxy binary
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
//! ## Runtime
//!
//! Runs as a regular test under `cargo test` (ADR-0046 — no feature
//! flag, no `#[ignore]`). Requires Docker on the host: two containers
//! are started — a standalone broker (~30 s startup), and a proxy
//! (~10 s startup once the broker is healthy). The proxy needs network
//! reachability back to the standalone's mapped Zookeeper port. On
//! Linux the operator supplies the routable Docker bridge gateway through
//! `MAGNETAR_E2E_DOCKER_HOST_GATEWAY`; Docker Desktop may use
//! `host.docker.internal`. The proxy uses the standalone's `standalone`
//! cluster name, required by its OpenTelemetry initialization. The standalone
//! explicitly enables `ZooKeeper` so the proxy can use the exposed metadata port.

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
use testcontainers::GenericImage;

mod common;

use common::pulsar::{BROKER_BINARY_PORT, Broker, Pulsar, init_tracing};

/// Start the Pulsar standalone container and return its host-mapped binary
/// port, admin port, zk port, plus the container handle (drop = stop).
///
/// The standalone keeps its metadata in `ZooKeeper` (`PULSAR_STANDALONE_USE_ZOOKEEPER`)
/// and advertises its own container address, so the proxy can reach the metadata
/// store and follow the broker's lookup answers over the bridge network.
async fn start_standalone() -> Result<
    (
        String,                                       /* service_url (host:port for binary
                                                       * protocol) */
        u16,                                          // mapped zk port
        testcontainers::ContainerAsync<GenericImage>, // standalone handle
    ),
    Box<dyn std::error::Error>,
> {
    init_tracing();
    let broker = Pulsar::new(Broker::Default)
        .zookeeper_metadata()
        .advertise_container_address()
        .start()
        .await?;
    let zookeeper_port = broker
        .zookeeper_port
        .ok_or("the fixture published no ZooKeeper port")?;
    Ok((broker.service_url, zookeeper_port, broker.container))
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
    // `bin/apply-config-from-env.py conf/proxy.conf`, which the shared fixture
    // does for a proxy. We point `zookeeperServers` at the standalone container's
    // bridge address and start the proxy.
    let proxy = Pulsar::new(Broker::Default)
        .config("clusterName", "standalone")
        .config("zookeeperServers", &zk_servers)
        .config("configurationStoreServers", &zk_servers)
        // The fixture publishes and maps the proxy on the broker binary port.
        .config("servicePort", BROKER_BINARY_PORT.to_string())
        .start_proxy()
        .await?;
    Ok((proxy.service_url, proxy.container))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_produce_consume_through_pulsar_proxy() -> Result<(), Box<dyn std::error::Error>> {
    // Skip when the gateway env var isn't set — the proxy container needs a
    // reachable host gateway back to the standalone's mapped Zookeeper port,
    // which only works on hosts where the bridge IP is routable (Linux with
    // default Docker bridge) or via Docker Desktop's `host.docker.internal`.
    // CI runs without this gateway, so without an explicit override we skip
    // cleanly — the env var is the operator's affirmative "yes, my Docker
    // network is configured for the standalone↔proxy hop".
    let Ok(gateway) = std::env::var("MAGNETAR_E2E_DOCKER_HOST_GATEWAY") else {
        eprintln!(
            "skipping e2e_produce_consume_through_pulsar_proxy: \
             MAGNETAR_E2E_DOCKER_HOST_GATEWAY not set. Run with e.g. \
             `MAGNETAR_E2E_DOCKER_HOST_GATEWAY=172.17.0.1 cargo test \
             --test e2e_pulsar_proxy -- --include-ignored`."
        );
        return Ok(());
    };

    // Step 1: standalone broker.
    let (_standalone_url, zk_port, _standalone) = start_standalone().await?;

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
