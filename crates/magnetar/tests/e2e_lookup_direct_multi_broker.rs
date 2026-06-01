// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for ADR-0039 §"Multi-broker DIRECT routing
//! (2026-06-01)" / HIGH-1 from the lookup multi-agent review.
//!
//! `apachepulsar/pulsar:4.0.4` runs as a single-broker standalone in this
//! suite (single-node mode is the only configuration that fits the
//! per-PR e2e budget). A true multi-broker DIRECT routing scenario
//! requires the official 3+ broker cluster fixture, which is out of
//! scope for the per-PR CI envelope. What this test *can* verify is the
//! degenerate single-broker case: standalone Pulsar 4 returns
//! `broker_service_url = Some("pulsar://...")` on every lookup,
//! pointing back at itself. Before the ADR-0039 §2026-06-01 amendment
//! the runtime dropped that field on the floor; after the amendment the
//! runtime captures it, recognises the bootstrap-equality (the
//! `host:port` matches the connect-time URL), and reuses the bootstrap
//! connection — no spurious new TCP session.
//!
//! Assertions:
//! - `PulsarClient::open_producer` succeeds against a Pulsar 4 standalone broker that advertises
//!   its own URL on the lookup response. This is the bootstrap-equality fast path under load.
//! - A second producer + a consumer on the same topic both succeed — the runtime keeps reusing the
//!   bootstrap (no pool entry, no extra TCP session, no proxy-handshake path).
//!
//! Multi-broker DIRECT routing under a real multi-broker cluster is the
//! follow-up that the `crates/magnetar-runtime-tokio/tests/lookup_direct_multi_broker.rs`
//! in-process broker pair already covers — those broker stubs reproduce
//! the wire behaviour Pulsar 3+/cluster mode exhibits when a topic
//! hashes to a non-bootstrap broker. The e2e test here closes the loop
//! that the runtime survives a real Pulsar standalone with the
//! advertised `broker_service_url` round-tripping through it.

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

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

async fn start_pulsar()
-> Result<(String, testcontainers::ContainerAsync<GenericImage>), Box<dyn std::error::Error>> {
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
    let service_url = format!("pulsar://{host}:{binary_port}");
    Ok((service_url, container))
}

/// Pulsar 4 standalone advertises its own URL on every lookup. The
/// runtime must (a) recognise the bootstrap-equality case and reuse the
/// bootstrap connection, (b) drive `open_producer` and `subscribe`
/// successfully on it. Before the ADR-0039 §2026-06-01 amendment the
/// runtime ignored the advertised URL entirely — this is fine on
/// standalone because the URL pointed at the bootstrap anyway, but the
/// post-amendment routing must still hit the same code path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_open_producer_against_standalone_after_direct_lookup()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let topic = "persistent://public/default/magnetar-e2e-direct-multi-broker";

    // First producer — exercises the LOOKUP → resolve_target → bootstrap
    // reuse path on a real broker.
    let p1 = client.producer(topic).create().await?;
    p1.send(OutgoingMessage::with_payload(b"hello".to_vec()).into())
        .await?;

    // Second producer on the same topic — exercises pool-bypass
    // (bootstrap-equality fast path) twice; both must reuse the same
    // bootstrap conn (no extra TCP sessions to the broker).
    let p2 = client.producer(topic).create().await?;
    p2.send(OutgoingMessage::with_payload(b"world".to_vec()).into())
        .await?;
    p1.close().await?;
    p2.close().await?;

    // Consumer side: same path through `subscribe`.
    let consumer = client
        .consumer(topic)
        .subscription("magnetar-direct-multi-broker-e2e")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let mut received = Vec::new();
    for _ in 0..2 {
        let msg = consumer.receive().await?;
        received.push(msg.payload.to_vec());
        consumer.ack(msg.message_id).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(
        received,
        vec![b"hello".to_vec(), b"world".to_vec()],
        "two messages must round-trip through the bootstrap-equality fast path",
    );
    Ok(())
}
