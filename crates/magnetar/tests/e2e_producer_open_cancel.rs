// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests for the cancelled producer open (issue #406, ADR-0100).
//!
//! Production repro: an `open_producer` lost client-side while completing
//! broker-side. The broker keeps the `(topic, producer_name)` registration,
//! so every later open under that name fails with:
//!
//! ```text
//! NamingException: Producer with name 'X' is already connected to topic 'T'
//! ```
//!
//! `ProducerBusy` (code 16) is retryable for `ProducerOpen` (ADR-0080), so
//! the retry loop re-hits the zombie with a fresh producer id until the
//! budget runs out; only `topics unload` recovered the name.
//!
//! Two user-visible contracts are covered here:
//!
//! 1. **Close before giving up (ADR-0100).** A producer open abandoned on its deadline pushes a
//!    best-effort `CommandCloseProducer` for the abandoned producer id, so a same-name open on the
//!    same connection succeeds. The deterministic proof lives in the in-process layers
//!    (`crates/magnetar-proto/src/conn.rs` unit tests, the two engines'
//!    `producer_open_cancel_close.rs`, and
//!    `crates/magnetar-differential/tests/producer_open_cancel_close_equivalence.rs`), which script
//!    the withheld `CommandProducerSuccess` exactly. Against a real broker the interleaving cannot
//!    be scripted, so this test sweeps a range of tight deadlines to land inside the window and
//!    asserts the recovery contract. It is one-sided: with the fix the final open always succeeds;
//!    without it, any attempt that stranded a registration fails it.
//! 2. **Opt-in unique name suffix.** `ProducerBuilder::unique_name_suffix(true)` makes each open
//!    claim its own name, so a registration stranded by a dead client — or by a proxy-mediated
//!    connection that outlived it, where no close can ever be sent — cannot collide with the next
//!    one. Default off: a pinned name stays pinned, and a second live open under it is still
//!    rejected.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar-driver --test e2e_producer_open_cancel -- --nocapture
//! ```
//!
//! Requires Docker on the host. See `e2e_pulsar.rs` for the broker
//! container plumbing; this file uses the same image/wait strategy via a
//! local helper.

use std::time::Duration;

use magnetar::PulsarClient;
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

/// Deadlines swept by [`e2e_timed_out_producer_open_does_not_poison_the_name`].
/// A warm topic answers a producer open in single-digit milliseconds, so this
/// range brackets the window where the `CommandProducer` is on the wire and
/// the `CommandProducerSuccess` is not back yet.
const TIGHT_DEADLINES_MS: [u64; 6] = [1, 2, 3, 5, 8, 13];

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

/// Start a Pulsar 4.x standalone container and return (`service_url`, `admin_url`,
/// `container_handle`).
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

fn unique_topic(prefix: &str) -> String {
    format!(
        "persistent://public/default/{prefix}-{}",
        uuid::Uuid::new_v4().simple()
    )
}

/// Issue #406: a producer open abandoned on its deadline must not poison the
/// pinned name. Each abandoned attempt pushes a `CommandCloseProducer` for
/// its producer id, and the close precedes the next open on the same
/// connection (FIFO), so the final open observes a free name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_timed_out_producer_open_does_not_poison_the_name()
-> Result<(), Box<dyn std::error::Error>> {
    const PRODUCER_NAME: &str = "open-cancel-hostname";
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = unique_topic("magnetar-e2e-producer-open-cancel");

    // Warm the topic so the namespace bundle is loaded and the lookup is a
    // cheap round-trip: the swept deadlines below must expire during the
    // producer open, not during bundle assignment.
    let warmup = client.producer(&topic).create().await?;
    warmup.close().await?;

    for budget_ms in TIGHT_DEADLINES_MS {
        // Dropping the open future is what arms the engine's cancellation
        // guard — exactly the path a lost `open_producer` takes.
        let attempt = tokio::time::timeout(
            Duration::from_millis(budget_ms),
            client.producer(&topic).name(PRODUCER_NAME).create(),
        )
        .await;
        // An attempt that beat its deadline is released the same way the
        // production repro releases it: no explicit close (ADR-0057's
        // last-clone drop guard frees the name for the next attempt).
        if let Ok(Ok(producer)) = attempt {
            drop(producer);
        }
    }

    // The contract: whatever the sweep did to the broker, the pinned name is
    // reusable on the same connection.
    let recreated = client.producer(&topic).name(PRODUCER_NAME).create().await?;
    recreated
        .send_bytes(b"after-cancelled-opens".to_vec())
        .await?;
    recreated.close().await?;
    client.close().await;
    Ok(())
}

/// The opt-in escape hatch for a registration nothing can close — a dead
/// client, or a proxy-mediated connection that outlives it.
/// `unique_name_suffix(true)` gives every open its own name, so two live
/// producers built from the same caller-supplied name coexist. Default off:
/// the same pair without the suffix collides on `ProducerBusy`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_unique_name_suffix_avoids_a_pinned_name_collision()
-> Result<(), Box<dyn std::error::Error>> {
    const PRODUCER_NAME: &str = "unique-suffix-hostname";
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = unique_topic("magnetar-e2e-producer-unique-suffix");

    // Default (suffix off): the name is pinned, so a second live open under
    // it is rejected by the broker.
    let pinned = client.producer(&topic).name(PRODUCER_NAME).create().await?;
    let collision = client.producer(&topic).name(PRODUCER_NAME).create().await;
    assert!(
        collision.is_err(),
        "a pinned name held by a live producer must not open a second time"
    );
    pinned.close().await?;

    // Suffix on: both opens claim distinct names and coexist.
    let first = client
        .producer(&topic)
        .name(PRODUCER_NAME)
        .unique_name_suffix(true)
        .create()
        .await?;
    let second = client
        .producer(&topic)
        .name(PRODUCER_NAME)
        .unique_name_suffix(true)
        .create()
        .await?;
    first.send_bytes(b"first".to_vec()).await?;
    second.send_bytes(b"second".to_vec()).await?;
    first.close().await?;
    second.close().await?;
    client.close().await;
    Ok(())
}
