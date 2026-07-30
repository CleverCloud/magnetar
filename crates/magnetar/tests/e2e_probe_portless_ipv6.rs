// SPDX-License-Identifier: Apache-2.0

//! End-to-end regression test for the ADR-0087 unification: a PIP-121
//! [`AutoClusterFailover`] whose PRIMARY entry is a **port-less bracketed IPv6
//! literal** (`pulsar://[::1]`) must probe that entry as healthy while
//! something is listening on the scheme's default port, and fail over to the
//! real broker only once it stops.
//!
//! # Why this is red before the fix
//!
//! ADR-0085 moved the endpoint parse into `magnetar_proto::probe_authority` but
//! carried one limitation forward: default-port synthesis triggered on "the
//! authority contains no `:`", never true of a bracketed IPv6 literal whose
//! colons belong to the address. So `pulsar://[::1]` produced the port-less
//! authority `"[::1]"`, [`tokio::net::lookup_host`] rejected it as an invalid
//! socket address, and the probe reported the primary unhealthy **no matter
//! what was running there**.
//!
//! Pre-fix the policy therefore leaves index 0 immediately and the first
//! assertion below fails. Post-fix the probe resolves `[::1]:6650`, connects to
//! the listener this test binds, and the policy stays put.
//!
//! # Scenario
//!
//! 1. Bind a plain TCP listener on `[::1]:6650` — the target the synthesised default port must
//!    produce. It never speaks Pulsar; a `HealthProbe` verdict is a TCP reachability check
//!    (ADR-0023), so a bound socket is exactly what "healthy" means here.
//! 2. One real Pulsar 4.x standalone container as the standby at index 1.
//! 3. Assert the policy **stays on index 0** across several probe cycles — only reachable if the
//!    port-less IPv6 primary parsed to `[::1]:6650`.
//! 4. Drop the listener, and assert the policy fails over to the real broker and a client built on
//!    the resulting service URL round-trips a message.
//!
//! Step 4 is what makes step 3 meaningful: without it, a policy that ignored
//! its probe entirely would also "stay on index 0". Together they pin that the
//! verdict is being computed and acted on, in both directions.
//!
//! # Why the fixed port is unavoidable here, and what happens if it is taken
//!
//! Default-port synthesis by definition produces `6650` (or `6651` for
//! `pulsar+ssl://`), so a test of that synthesis cannot use an ephemeral port.
//! Per [ADR-0021] this test must not be `#[ignore]`d or silently skipped, so an
//! occupied `[::1]:6650` fails loudly with a message naming the cause rather
//! than passing vacuously. In CI nothing binds it — the Pulsar container's
//! `6650` is published on a random host port by testcontainers.
//!
//! Runs as a regular test under `cargo test` (ADR-0046 — no `#[ignore]`, no
//! feature gate). Run with:
//!
//! ```sh
//! cargo test -p magnetar-driver --test e2e_probe_portless_ipv6 -- --nocapture
//! ```
//!
//! [ADR-0021]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0021-no-silent-test-ignore-or-remove.md

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

/// The port-less bracketed IPv6 primary — the shape ADR-0085 recorded as an
/// accepted limitation and ADR-0087 closed.
const PORTLESS_IPV6_PRIMARY: &str = "pulsar://[::1]";

/// Where the synthesised `pulsar://` default port must land, and so where this
/// test binds its stand-in listener.
const SYNTHESISED_TARGET: &str = "[::1]:6650";

/// Probe cadence. Short enough that the loop settles quickly, long enough not
/// to spin against the container.
const PROBE_INTERVAL: Duration = Duration::from_millis(200);

/// How many probe cycles the policy must hold index 0 before we accept that it
/// really considers the port-less IPv6 primary healthy.
const HOLD_CYCLES: u32 = 10;

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

/// Bind the stand-in for "a broker reachable at the synthesised default port".
///
/// Fails loudly rather than skipping (ADR-0021): the two ways this can fail are
/// a host with no IPv6 loopback and a process already holding the port, and
/// both mean the assertions below would be vacuous.
async fn bind_synthesised_target() -> tokio::net::TcpListener {
    match tokio::net::TcpListener::bind(SYNTHESISED_TARGET).await {
        Ok(listener) => listener,
        Err(e) => panic!(
            "cannot bind '{SYNTHESISED_TARGET}', so this test cannot tell a working \
             default-port synthesis from a broken one: {e}.\n\
             Default-port synthesis always yields 6650, so an ephemeral port is not an \
             option here. Either IPv6 loopback is unavailable on this host, or something \
             already holds the port — a local `pulsar standalone` is the usual culprit \
             (`ss -tlnp 'sport = :6650'`). Per ADR-0021 this fails instead of skipping.",
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portless_ipv6_primary_is_probed_at_the_scheme_default_port()
-> Result<(), Box<dyn std::error::Error>> {
    let listener = bind_synthesised_target().await;
    let (broker_url, _container) = start_pulsar().await?;

    let failover = Arc::new(AutoClusterFailover::new(
        vec![PORTLESS_IPV6_PRIMARY.to_owned(), broker_url.clone()],
        Arc::new(TokioHealthProbe::new()),
    ));
    // The policy starts optimistic on index 0 regardless of any verdict, so
    // this is a precondition, not yet evidence.
    assert_eq!(failover.active_index(), 0);

    let prober = failover.start(PROBE_INTERVAL);

    // (1) The primary must be probed HEALTHY and hold. Pre-ADR-0087 the probe
    // derived the port-less authority `[::1]`, could not resolve it, and the
    // policy left for the standby on the first cycle — so this loop is the
    // red/green witness.
    for cycle in 0..HOLD_CYCLES {
        tokio::time::sleep(PROBE_INTERVAL).await;
        assert_eq!(
            failover.active_index(),
            0,
            "the policy left the port-less IPv6 primary '{PORTLESS_IPV6_PRIMARY}' on cycle \
             {cycle} even though '{SYNTHESISED_TARGET}' is listening — the probe must \
             synthesise the scheme default port instead of dialling a port-less authority",
        );
    }
    assert_eq!(
        failover.get_service_url(),
        PORTLESS_IPV6_PRIMARY,
        "the active service URL must still be the healthy primary",
    );

    // (2) Now make the primary genuinely unreachable. This is what proves (1)
    // was a real verdict rather than a policy that never looked: the same loop
    // must now move off index 0.
    drop(listener);

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
        "the policy never failed over after '{SYNTHESISED_TARGET}' stopped listening — the \
         probe verdict is not being acted on, which would make assertion (1) vacuous",
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
        "persistent://public/default/magnetar-e2e-probe-portless-ipv6-{}",
        Uuid::new_v4()
    );
    let consumer = client
        .consumer(&topic)
        .subscription("probe-portless-ipv6")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let producer = client.producer(&topic).create().await?;

    let payload = b"probe-portless-ipv6".to_vec();
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
