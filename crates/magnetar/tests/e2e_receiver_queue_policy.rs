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
//! A second test, `e2e_auto_adjust_arms_under_continuous_ack_response_traffic`,
//! pins the `docs/follow-ups.md` §4 arming bootstrap: an `Auto` consumer on the
//! DEFAULT keepalive that awaits every individual ack — continuous inbound
//! `CommandAckResponse` traffic that used to defer the schedule's only arming
//! site indefinitely — must still be observed ramping past the floor.
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
        // A short keepalive — a realistic production tuning choice (fast failure
        // detection) that also keeps the driver's timer arm ticking briskly.
        // This used to be load-bearing: while the adjust schedule's first arm
        // lived only in `handle_timeout`'s fallback, a busy consumer refreshing
        // the keepalive baseline could defer that arm past this test's window.
        // `Connection::initial_flow` now arms the schedule at subscribe-ack time
        // (follow-ups §4), so the tuning is no longer required for correctness —
        // the sibling `e2e_auto_adjust_arms_under_continuous_ack_response_traffic`
        // proves the ramp on the DEFAULT keepalive. It is kept here so this test
        // keeps measuring the bounded-memory drain it was written for.
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
        // Acking is not awaited per-message here: acks are orthogonal to flow
        // control (driven by `receive()` pops, not acks), and a single
        // cumulative ack after the drain (below) advances the subscription
        // cursor well enough for this test's purposes. Historically this was
        // also a workaround — per-message ack awaits round-trip a
        // `CommandAckResponse` on the SAME connection the adjust-tick timer
        // watches, which used to starve the schedule's only arming site. That
        // is fixed (follow-ups §4) and pinned by the sibling
        // `e2e_auto_adjust_arms_under_continuous_ack_response_traffic`, which
        // deliberately DOES await every ack.
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

/// End-to-end regression for `docs/follow-ups.md` §4: the `Auto` adjust
/// schedule must arm on a connection that is never quiet.
///
/// The failure this pins was reproduced twice while writing the issue #349 e2e
/// above. Every decoded inbound frame refreshes the connection's `last_activity`
/// keepalive baseline (ADR-0058's single refresh site). While the adjust
/// schedule's first arm lived only in `Connection::handle_timeout`'s fallback
/// arm — and `handle_timeout` runs only when a `poll_timeout()` deadline
/// actually elapses — a consumer that awaits each individual ack produces a
/// continuous `CommandAckResponse` stream that pushes the keepalive deadline
/// (the ONLY deadline an unarmed `Auto` consumer has) permanently out of reach.
/// `handle_timeout` never ran, the schedule never armed, and `Auto` never
/// scaled, regardless of the configured `keepalive_interval`.
///
/// So this test deliberately does the two things the sibling test avoids:
///
/// 1. it leaves `keepalive` at the 30 s default — no short-keepalive tuning,
/// 2. it `await`s EVERY individual ack inside the receive loop.
///
/// Under those conditions the auto-tuned target must still be observed growing
/// past the floor. `Connection::initial_flow` arming the schedule at
/// subscribe-ack time is what makes that true.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_auto_adjust_arms_under_continuous_ack_response_traffic()
-> Result<(), Box<dyn std::error::Error>> {
    const BACKLOG: usize = 300;
    /// A small floor so the prefetch window stays well below the backlog and
    /// the deliberately-slow consumer keeps hitting genuine starvation.
    const FLOOR: usize = 20;
    const MESSAGE_PAYLOAD_BYTES: usize = 52;
    /// Byte budget wide enough that the doubling rule, not the OOM guard,
    /// governs the first few growth steps this test needs to observe.
    const MAX_BYTES: usize = 115 * MESSAGE_PAYLOAD_BYTES;

    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let id = uuid::Uuid::new_v4().simple();
    let topic = format!("persistent://public/default/magnetar-e2e-rq-arming-{id}");

    // NOTE: no `.keepalive(...)` call. The 30 s default is the whole point —
    // combined with the per-message ack awaits below, the pre-fix client could
    // never arm the adjust schedule within this test's window.
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let producer = client.producer(&topic).create().await?;
    for i in 0..BACKLOG {
        let payload = format!("rq-arming-msg-{i:05}-padding-XXXXXXXXXXXXXXXXXXXXXXXX").into_bytes();
        producer
            .send(OutgoingMessage::with_payload(payload).into())
            .await?;
    }
    producer.flush().await?;

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-e2e-rq-arming")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .receiver_queue_policy(Arc::new(Auto::new(FLOOR, MAX_BYTES)))
        // Fast adjust cadence so the ramp is observable within the test window.
        .receiver_queue_adjust_interval(Duration::from_millis(200))
        .subscribe()
        .await?;

    let mut received = 0usize;
    let mut max_observed_target = consumer.current_receiver_queue_size();
    while received < BACKLOG {
        let msg = tokio::time::timeout(Duration::from_secs(30), consumer.receive())
            .await
            .expect("consumer.receive timeout")
            .expect("consumer.receive error");
        // The traffic shape that used to defeat the arming: await the broker's
        // `CommandAckResponse` for every single message, so the connection is
        // never idle long enough for the keepalive deadline to elapse.
        consumer.ack(msg.message_id).await?;
        received += 1;
        max_observed_target = max_observed_target.max(consumer.current_receiver_queue_size());
        // Deliberately slow consumer, matching the sibling test: guarantees the
        // broker's grant is fully dispatched and drained to zero for long enough
        // that an adjust tick catches real starvation.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    consumer.close().await?;
    client.close().await;

    assert_eq!(
        received, BACKLOG,
        "Auto-policy consumer must drain the entire backlog while awaiting every ack"
    );
    assert!(
        max_observed_target > FLOOR,
        "the adjust schedule must arm on a connection kept busy by continuous ack-response \
         traffic on the DEFAULT keepalive: the auto-tuned target must be OBSERVED growing past \
         the floor ({FLOOR}), but the max observed was {max_observed_target} \
         (docs/follow-ups.md §4)"
    );
    Ok(())
}
