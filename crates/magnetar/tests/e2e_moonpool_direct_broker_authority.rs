// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for `magnetar_runtime_moonpool::client::direct_broker_authority`
//! (private helper) after its issue #364 hardening.
//!
//! `direct_broker_authority` (and its sibling `proxy_broker_authority`, both
//! in `crates/magnetar-runtime-moonpool/src/client.rs`) changed from
//! `fn(&str) -> String` to `fn(&str) -> Result<String, ClientError>`: an
//! input containing `"://"` with an unrecognised scheme (a single-bit
//! corruption of `pulsar`, e.g. `"ptlsar://..."`) now fails explicitly
//! instead of falling through to a naive `split('/')` that silently
//! truncated it into a nonsense authority.
//!
//! **Honest scope note: this test is NOT the fix's regression proof, and
//! was never red pre-fix.** It only exercises `PulsarClient`'s public
//! surface against a well-formed broker URL, never calling
//! `proxy_broker_authority` / `direct_broker_authority` directly or
//! indirectly through a corrupted input — verified empirically by
//! reverting the `client.rs` signature change back to `fn(&str) -> String`
//! and confirming this file still compiles and behaves identically (it
//! does). A genuinely corrupted scheme is NOT reproducible against a real
//! broker anyway (TCP checksums make single-bit command-frame corruption
//! practically unreachable in production — see `docs/follow-ups.md`'s
//! citation of this same reasoning). The actual red/green regression proof
//! for this fix lives in
//! `crates/magnetar-runtime-moonpool/tests/proxy_multi_conn.rs`'s
//! `open_producer_through_proxy_rejects_corrupted_broker_scheme` (verified
//! red pre-fix, green post-fix — see the commit message). What THIS test
//! proves is narrower but still load-bearing for the e2e obligation: since
//! neither engine's unit-test suite nor the moonpool integration suite
//! touches a REAL broker, this test closes the loop against production
//! infrastructure by proving the `Result`-returning signature change's
//! happy path (a real Pulsar 4 standalone broker, which always advertises a
//! well-formed `pulsar://host:port` `broker_service_url`) still
//! round-trips end-to-end through `MoonpoolEngine<TokioProviders>` — the
//! moonpool engine driven over real host sockets against real Docker
//! infrastructure, not the deterministic `SimProviders` the chaos suite
//! uses.
//!
//! Mirrors `e2e_lookup_direct_multi_broker.rs`'s single-standalone-container
//! DIRECT-routing scenario (a real Pulsar 4 standalone advertises its own
//! URL on every lookup, exercising the bootstrap-equality fast path through
//! `direct_broker_authority`), but drives `PulsarClient<MoonpoolEngine<TokioProviders>>`
//! instead of the tokio façade — see `e2e_reconnect.rs`'s
//! `e2e_moonpool_transient_producer_open_retry_across_broker_restart` for
//! the construction pattern this test reuses.
//!
//! Gated on `feature = "moonpool"` because a moonpool-engine client cannot
//! compile without it (engine selection, not test-hiding — every other
//! `PulsarClient<MoonpoolEngine>` item in the façade carries the same gate;
//! the e2e suite always runs under `cargo test --all-features` per
//! ADR-0046, so the gate never hides the test from a normal run). No
//! `#[ignore]`.

#![cfg(feature = "moonpool")]

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::runtime_moonpool::{Client as MoonpoolClient, MoonpoolEngine};
use magnetar::{OutgoingMessage, PulsarClient};
use moonpool_core::TokioProviders;
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

async fn start_pulsar()
-> Result<(String, testcontainers::ContainerAsync<GenericImage>), Box<dyn std::error::Error>> {
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
    let service_url = format!("pulsar://{host}:{binary_port}");
    Ok((service_url, container))
}

/// Pulsar 4 standalone advertises its own well-formed `pulsar://host:port`
/// URL on every lookup response's `broker_service_url`. The moonpool
/// engine's `Client::resolve_direct_broker` feeds that value through
/// `direct_broker_authority` (now `Result`-returning per issue #364's
/// hardening) to derive the DIRECT-routing dial target and the
/// bootstrap-equality comparison. This proves that happy path — a
/// real broker's real advertised URL — still resolves to `Ok(_)` and the
/// producer/consumer round-trip completes, i.e. the signature change from
/// `String` to `Result<String, ClientError>` did not regress ordinary
/// operation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_moonpool_open_producer_against_standalone_after_direct_lookup()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _container) = start_pulsar().await?;
    let host_port = service_url
        .strip_prefix("pulsar://")
        .unwrap_or(&service_url)
        .to_owned();

    let runtime_client = MoonpoolClient::connect_plain_supervised(
        &MoonpoolEngine::new(TokioProviders::new()),
        &host_port,
        magnetar_proto::ConnectionConfig {
            operation_timeout: Duration::from_secs(30),
            ..Default::default()
        },
        None,
        None,
    )
    .await?;
    let client = PulsarClient::from_runtime_client(runtime_client);

    let topic = "persistent://public/default/magnetar-e2e-moonpool-direct-broker-authority";

    // First producer — exercises LOOKUP -> resolve_target ->
    // resolve_direct_broker -> direct_broker_authority(..)? on a real
    // broker's real advertised URL.
    let p1 = client.producer(topic).create().await?;
    p1.send(OutgoingMessage::with_payload(b"hello".to_vec()).into())
        .await?;

    // Second producer on the same topic — exercises the bootstrap-equality
    // fast path (physical == pool.bootstrap_addr()) a second time.
    let p2 = client.producer(topic).create().await?;
    p2.send(OutgoingMessage::with_payload(b"world".to_vec()).into())
        .await?;
    p1.close().await?;
    p2.close().await?;

    let consumer = client
        .consumer(topic)
        .subscription("magnetar-e2e-moonpool-direct-broker-authority-sub")
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
        "two messages must round-trip through direct_broker_authority's Ok(_) happy path",
    );
    Ok(())
}
