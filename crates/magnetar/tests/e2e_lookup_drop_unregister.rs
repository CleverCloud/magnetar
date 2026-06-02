// SPDX-License-Identifier: Apache-2.0

//! E2E: dropping a `partitions_for_topic` future mid-flight against a real
//! Apache Pulsar 4.x standalone broker must not hang, panic, or leave the
//! client in a wedged state.
//!
//! ADR-0024 layer 5 of the lookup multi-agent review MEDIUM-4 fix
//! (defense-in-depth `Drop` on the tokio + moonpool `RequestFut`).
//! The companion layers are:
//!
//! - `crates/magnetar-proto/src/conn.rs` (proto unit tests
//!   `unregister_waker_drops_request_entry_without_disturbing_siblings` and
//!   `unregister_waker_clears_producer_slot_send_waker`)
//! - `crates/magnetar-runtime-tokio/tests/lookup_drop_unregister.rs`
//! - `crates/magnetar-runtime-moonpool/tests/lookup_drop_unregister.rs`
//! - `crates/magnetar-differential/tests/lookup_drop_unregister_equivalence.rs`
//!
//! Strategy: issue two `partitions_for_topic` calls back to back. The first
//! is wrapped in a 1-millisecond timeout (almost certainly cancelled
//! before the broker replies — drops `RequestFut` mid-poll
//! and triggers the new `Drop::drop` path), the second is allowed to
//! complete. If the cleanup hook regressed the second call would hang or
//! report wedged state.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Requires Docker
//! on the host with `apachepulsar/pulsar:latest` reachable.

use std::time::Duration;

use magnetar::PulsarClient;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_cancelled_partitions_lookup_does_not_wedge_client()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let topic = "persistent://public/default/magnetar-e2e-lookup-drop-unregister";

    // (1) Cancelled lookup — 1 ms is shorter than any realistic broker RTT,
    // so the future is almost always dropped mid-poll. Either way, success
    // or timeout, the test is meaningful: success exercises the normal
    // dispatch path, timeout exercises the Drop::drop cleanup hook. We
    // intentionally do NOT assert on this result.
    let cancelled =
        tokio::time::timeout(Duration::from_millis(1), client.partitions_for_topic(topic)).await;
    drop(cancelled);

    // (2) Healthy lookup — must complete cleanly. If the cancelled future
    // left the connection's waker slab wedged (e.g. a stale entry that
    // confused the dispatcher) this call would hang or error out.
    let partitions =
        tokio::time::timeout(Duration::from_secs(10), client.partitions_for_topic(topic))
            .await
            .expect("partitions_for_topic must not hang after a cancelled sibling call")?;

    // Non-partitioned topic → 0 partitions. The contract we care about is
    // that the call returned a real broker answer (not a timeout), so any
    // u32 the broker yields is acceptable here; we just want a value.
    let _ = partitions;
    Ok(())
}
