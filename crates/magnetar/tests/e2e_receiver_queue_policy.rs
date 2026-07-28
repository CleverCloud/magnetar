// SPDX-License-Identifier: Apache-2.0

//! End-to-end test for the pluggable receiver-queue policy (issue #301, PIP-74
//! `autoScaledReceiverQueueSizeEnabled` parity) against a real Apache Pulsar 4.x
//! standalone broker.
//!
//! A consumer configured with [`magnetar_proto::Auto`] drains a pre-produced
//! backlog. The claims are:
//!
//! 1. **Forward progress**: the consumer drains the whole backlog.
//! 2. **Bounded memory**: its buffered-queue byte footprint stays bounded — it never balloons to
//!    hold the entire backlog at once.
//! 3. **Real growth (issue #349)**: with a deliberately slow consumer facing a 500-message backlog,
//!    `current_receiver_queue_size()` must be OBSERVED exceeding the floor at some point during the
//!    drain — proving the target genuinely ramps under real dispatch-driven starvation, not merely
//!    that its final value happens to sit at or above the floor (which was already trivially true
//!    even before the #349 fix, since `Auto` never shrinks below the floor).
//!
//! The auto-adjust tick rides the tokio driver's existing
//! `poll_timeout`/`handle_timeout` loop, so no manual ticking is needed here.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_receiver_queue_policy -- --nocapture
//! ```
//!
//! Requires Docker on the host.

use std::sync::Arc;
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
use magnetar_proto::Auto;
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

/// End-to-end: produce a backlog, subscribe with an `Auto` receiver-queue
/// policy and a DELIBERATELY SLOW consumer, drain the whole backlog, and
/// assert (a) every message was received (forward progress), (b) the
/// buffered-queue depth never approached the full backlog (bounded memory —
/// the `Auto` floor + byte budget keep prefetch modest while still letting
/// the broker stream ahead), and (c) the receiver-queue target was OBSERVED
/// growing past the floor at some point (issue #349: real dispatch-driven
/// starvation must be visible, not just a `>= floor` truism).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_auto_receiver_queue_drains_backlog_without_unbounded_memory()
-> Result<(), Box<dyn std::error::Error>> {
    const BACKLOG: usize = 500;
    // A small floor so the prefetch window stays well below the backlog.
    const FLOOR: usize = 20;
    // Issue #349: with real dispatch-driven growth now genuinely working
    // (rather than never firing), an effectively-unlimited byte budget lets
    // the target keep doubling every adjust tick for as long as the
    // deliberately-slow consumer keeps observing starvation — which, against
    // a small 500-message backlog, quickly outgrows the bounded-memory
    // assertion below. `MESSAGE_PAYLOAD_BYTES` (~52) times a target of ~115
    // keeps the projected buffered bytes comfortably under this budget, so
    // the OOM guard (not the floor) governs the ceiling here, settling well
    // under `BACKLOG / 2`.
    const MESSAGE_PAYLOAD_BYTES: usize = 52;
    const MAX_BYTES: usize = 115 * MESSAGE_PAYLOAD_BYTES;

    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let id = uuid::Uuid::new_v4().simple();
    let topic = format!("persistent://public/default/magnetar-e2e-rq-policy-{id}");

    let client = PulsarClient::builder()
        .service_url(service_url)
        // A short keepalive so the connection's `poll_timeout`/`handle_timeout`
        // loop wakes frequently enough to arm the `Auto` adjust schedule during
        // the natural gaps between the broker's permit-limited dispatch bursts.
        // The adjust clock's FIRST arm happens inside `handle_timeout`, which is
        // itself only invoked when some deadline elapses; with the 30s default
        // keepalive and a busy consumer whose reads keep refreshing the
        // keepalive baseline, that first arm can be deferred well past this
        // test's window. A short keepalive is a realistic production tuning
        // choice (fast failure detection) that also happens to close this gap —
        // no internal state is touched, the driver loop and the `Auto` policy
        // logic being exercised are both the real ones.
        .keepalive(Duration::from_millis(100))
        .build()
        .await?;

    // Produce the backlog FIRST so the consumer faces a full queue to drain.
    let producer = client.producer(&topic).create().await?;
    for i in 0..BACKLOG {
        let payload = format!("rq-policy-msg-{i:05}-padding-XXXXXXXXXXXXXXXXXXXXXXXX").into_bytes();
        producer
            .send(OutgoingMessage::with_payload(payload).into())
            .await?;
    }
    producer.flush().await?;

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-rq-policy")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .receiver_queue_policy(Arc::new(Auto::new(FLOOR, MAX_BYTES)))
        // Fast adjust cadence so the ramp is observable within the test window.
        .receiver_queue_adjust_interval(Duration::from_millis(200))
        .subscribe()
        .await?;

    let mut received = 0usize;
    let mut max_observed_queue_depth = 0usize;
    let mut max_observed_target = consumer.current_receiver_queue_size();
    let mut last_message_id = None;
    while received < BACKLOG {
        let msg = tokio::time::timeout(Duration::from_secs(30), consumer.receive())
            .await
            .expect("consumer.receive timeout")
            .expect("consumer.receive error");
        // Issue #349: acking is deliberately NOT awaited per-message here.
        // `consumer.ack(...).await` round-trips a `CommandAckResponse` from
        // the broker (ADR-0082's sibling ack-deadline work, #346) — inbound
        // traffic on the SAME connection the adjust-tick timer watches. Acks
        // are orthogonal to flow control (which is driven by `receive()`
        // pops, not acks), so acking per-message here would keep refreshing
        // the connection's keepalive baseline on every iteration and starve
        // the timer arm that bootstraps the `Auto` adjust schedule, masking
        // the very starvation this test exists to observe. A single
        // cumulative ack after the drain (below) is enough to advance the
        // subscription cursor for this test's purposes.
        last_message_id = Some(msg.message_id);
        received += 1;
        // Sample the buffered-queue depth — the prefetch the broker has pushed
        // but the user has not yet drained. The Auto floor + byte budget keep
        // this bounded; it must never approach the whole backlog.
        max_observed_queue_depth = max_observed_queue_depth.max(consumer.available_in_queue());
        // Issue #349: sample the auto-tuned target too, so we can prove real
        // growth was observed rather than only checking the final value.
        max_observed_target = max_observed_target.max(consumer.current_receiver_queue_size());
        // Deliberately slow consumer: without this, local Docker round-trips
        // are fast enough that the pop-driven `maybe_flow` refill usually
        // outraces the 200ms adjust tick, so the tick rarely (and racily)
        // observes a genuinely zero permit balance. This delay guarantees the
        // broker's initial FLOOR-sized grant is fully dispatched and drained
        // to zero for long enough that at least one adjust tick catches real
        // starvation and doubles the target.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if let Some(id) = last_message_id {
        consumer.ack_cumulative(id).await.ok();
    }

    let current_target = consumer.current_receiver_queue_size();

    consumer.close().await?;
    client.close().await;

    // (a) Forward progress: the whole backlog drained.
    assert_eq!(
        received, BACKLOG,
        "Auto-policy consumer must drain the entire backlog"
    );
    // (b) Bounded memory: the buffered prefetch never held more than a small
    // fraction of the backlog at once. The Auto floor (20) plus bounded growth
    // keep it well under half the backlog even after ramping.
    assert!(
        max_observed_queue_depth < BACKLOG / 2,
        "Auto receiver queue must stay bounded: observed max buffered depth \
         {max_observed_queue_depth} approached the full backlog of {BACKLOG}"
    );
    // (c) Issue #349: real growth was observed at some point — the
    // dispatch-driven starvation signal actually fired and doubled the
    // target past the floor, not merely "the final value happens to sit at
    // or above the floor" (trivially true even with the bug).
    assert!(
        max_observed_target > FLOOR,
        "the auto-tuned target must be OBSERVED growing past the floor ({FLOOR}) under \
         real dispatch-driven starvation; max observed was {max_observed_target} (issue #349)"
    );
    // The target stayed within the sane band (floor .. a bounded multiple),
    // never running away.
    assert!(
        current_target >= FLOOR,
        "the auto-tuned target {current_target} must not drop below the floor {FLOOR}"
    );
    Ok(())
}
