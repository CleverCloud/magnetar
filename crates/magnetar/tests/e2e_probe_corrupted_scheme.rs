// SPDX-License-Identifier: Apache-2.0

//! End-to-end regression test for the ADR-0085 hardening: a PIP-121
//! [`AutoClusterFailover`] whose PRIMARY entry carries a corrupted URL scheme
//! must read that entry unhealthy and serve traffic from the healthy standby.
//!
//! **Scenario** — one Pulsar 4.x standalone container. The failover URL list is
//! `["ptlsar://<unroutable>:6650", "pulsar://<container>:6650"]`: a single-bit
//! corruption of the `pulsar` scheme word (the exact shape moonpool-sim's
//! bit-flip chaos produced for issue #364) in front of the real broker.
//!
//! Before the hardening, [`TokioHealthProbe`] truncated that corrupted URL into
//! the nonsense authority `"ptlsar:"` and ran a DNS lookup against it; the
//! lookup failed, so the probe *happened* to report unhealthy. The verdict was
//! right by accident. It now refuses the endpoint at parse time, with no I/O
//! at all — see `magnetar-runtime-tokio/tests/probe_corrupted_scheme.rs` for
//! the tracing witness that discriminates the two, which is the assertion that
//! actually goes red on a revert.
//!
//! What THIS test adds on top of that in-process proof is the end-to-end
//! consequence against a live broker: the policy converges on the standby, and
//! a real client built on the resulting service URL round-trips a message. It
//! pins that refusing the endpoint keeps the failover contract intact rather
//! than stalling the probe loop.
//!
//! This also closes, for the narrow corrupted-endpoint case, the gap
//! `e2e_cluster_failover.rs` records as deferred under "Skipped sub-tests":
//! `AutoClusterFailover` + `HealthProbe` had no end-to-end coverage. A
//! corrupted primary needs no lock-step verdict flipping — it is unhealthy by
//! construction — so it sidesteps the plumbing that made the general case
//! unattractive.
//!
//! Runs as a regular test under `cargo test` (ADR-0046 — no `#[ignore]`, no
//! feature gate). Run with:
//!
//! ```sh
//! cargo test -p magnetar-driver --test e2e_probe_corrupted_scheme -- --nocapture
//! ```

use std::sync::Arc;
use std::time::Duration;

use magnetar::proto::ServiceUrlProvider;
use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::runtime_tokio::auto_cluster_failover::{AutoClusterFailover, TokioHealthProbe};
use magnetar::{OutgoingMessage, PulsarClient};
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

/// The corrupted primary. `.invalid` is the RFC 2606 reserved TLD, so this
/// name can never resolve even if the scheme guard were removed — the test
/// asserts the endpoint is refused, and must not become a live DNS query
/// against someone's real host if it regresses.
const CORRUPTED_PRIMARY: &str = "ptlsar://magnetar-corrupted-primary.invalid:6650";

/// Probe cadence. Short enough that the loop settles quickly, long enough not
/// to spin against the container.
const PROBE_INTERVAL: Duration = Duration::from_millis(200);

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupted_scheme_primary_fails_over_to_healthy_standby()
-> Result<(), Box<dyn std::error::Error>> {
    let (broker_url, _container) = start_pulsar().await?;

    let failover = Arc::new(AutoClusterFailover::new(
        vec![CORRUPTED_PRIMARY.to_owned(), broker_url.clone()],
        Arc::new(TokioHealthProbe::new()),
    ));
    // The policy starts optimistic: index 0 (the corrupted primary) is active
    // until the first probe cycle rules it out.
    assert_eq!(
        failover.active_index(),
        0,
        "the policy must start on the primary before any probe verdict lands",
    );

    let prober = failover.start(PROBE_INTERVAL);

    // Wait for the probe loop to rule out the corrupted primary. Bounded so a
    // regression that stalls the loop fails loudly instead of hanging.
    let settled = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if failover.active_index() == 1 {
                return;
            }
            tokio::time::sleep(PROBE_INTERVAL).await;
        }
    })
    .await;
    assert!(
        settled.is_ok(),
        "the failover policy never moved off the corrupted primary \
         '{CORRUPTED_PRIMARY}' — the probe must report it unhealthy",
    );
    assert_eq!(
        failover.get_service_url(),
        broker_url,
        "the active service URL must be the healthy standby",
    );

    // End-to-end consequence: a real client built on the failover provider
    // reaches the live broker and round-trips a message.
    let provider: Arc<dyn ServiceUrlProvider> = failover.clone();
    let client = PulsarClient::builder()
        .service_url_provider(provider)
        .operation_timeout(Duration::from_mins(1))
        .build()
        .await?;

    let topic = format!(
        "persistent://public/default/magnetar-e2e-probe-corrupted-scheme-{}",
        Uuid::new_v4()
    );
    let consumer = client
        .consumer(&topic)
        .subscription("probe-corrupted-scheme")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let producer = client.producer(&topic).create().await?;

    let payload = b"probe-corrupted-scheme".to_vec();
    producer
        .send(OutgoingMessage::with_payload(payload.clone()).into())
        .await?;

    let message = tokio::time::timeout(Duration::from_secs(30), consumer.receive())
        .await
        .map_err(|_| "timed out awaiting the round-tripped message")??;
    assert_eq!(
        message.payload.as_ref(),
        payload.as_slice(),
        "the message must round-trip through the standby broker",
    );
    consumer.ack(message.message_id).await?;

    prober.abort();
    Ok(())
}
