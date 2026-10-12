// SPDX-License-Identifier: Apache-2.0

//! Issue #451 e2e — a producer detached by `topics unload` on a connection that
//! stays up, against a real Apache Pulsar 4.x standalone broker.
//!
//! `bin/pulsar-admin topics unload <topic>-partition-1` makes the owning broker
//! remove the producer and write `CommandCloseProducer` on a connection that
//! keeps serving every other partition. A standalone broker has nowhere to
//! reassign the bundle to, so there is no TCP drop and no supervised reconnect —
//! exactly the production shape of issue #451.
//!
//! Before ADR-0106 the detached child producer stayed at `broker_ready = false`
//! for the life of the connection: `PartitionedProducer`'s round-robin router has
//! no readiness input, so every publish it routed to that partition failed
//! `code=-1 "send timeout"` after the configured `send_timeout`, forever.
//!
//! Issue #463 extends the first test: after the unload, one at a time (an
//! in-flight window of one), publishes must keep progressing within a single
//! bounded deadline, and both children must be ready again afterward. The
//! real broker's re-attach window is brief; the held-not-ready state and the
//! skip itself are pinned deterministically by the facade regression.
//!
//! Two tests:
//!
//! 1. `e2e_producer_partition_unload_reattach` — the regression. 20 publishes across 2 partitions
//!    after the unload must ALL resolve `Ok`, and no child may report a transport disconnect.
//! 2. `e2e_producer_exclusive_unload_outcome` — observability only, for an `Exclusive`-access
//!    single-topic producer. `emit_command_producer` sends `topic_epoch: None` on the re-attach, so
//!    the broker arbitrates; parity with Java on that path is UNVERIFIED (ADR-0106 § Consequences).
//!    The outcome is printed, not asserted.
//!
//! A standalone broker runs the `ModularLoadManager`, so the close carries
//! `assigned_broker_service_url = None`. The `Some(url)` shape an
//! Extensible-Load-Manager multi-phase unload produces is covered by the proto,
//! runtime and differential layers instead.
//!
//! Runs as a regular test under `cargo test` (ADR-0046) — no `#[ignore]`, no
//! feature gate. Requires Docker on the host.

use std::time::Duration;

use magnetar::{MessageRoutingMode, OutgoingMessage, PulsarClient};
use testcontainers::core::{CmdWaitFor, ExecCommand};

mod common;

use common::pulsar::start_pulsar;

const PARTITIONS: usize = 2;

/// REGRESSION (issue #451): unload ONE partition of a partitioned topic and keep
/// publishing. The round-robin router has no readiness input, so every
/// `1/PARTITIONS`-th publish lands on the detached child — before ADR-0106 each
/// of those failed `code=-1 "send timeout"` forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_producer_partition_unload_reattach() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, container) = start_pulsar().await?;
    let topic = "persistent://public/default/magnetar-e2e-producer-unload-reattach";

    // `pulsar-admin` inside the container owns partitioned-topic creation here:
    // the unload below is exec'd the same way, so both go through one surface.
    container
        .exec(
            ExecCommand::new([
                "bin/pulsar-admin",
                "topics",
                "create-partitioned-topic",
                "-p",
                "2",
                topic,
            ])
            .with_cmd_ready_condition(CmdWaitFor::exit()),
        )
        .await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let producer = client
        .partitioned_producer(topic.to_owned())
        .routing(MessageRoutingMode::RoundRobin)
        .send_timeout(Duration::from_secs(10))
        .create()
        .await?;

    // Warm up so every child producer is attached before the unload.
    for i in 0..(PARTITIONS * 2) {
        producer
            .send(OutgoingMessage::with_payload(
                format!("warmup-{i}").into_bytes(),
            ))
            .await?;
    }

    // Detach the producer on ONE partition. The standalone broker reloads the
    // bundle on itself: no TCP drop, so nothing resets the connection.
    container
        .exec(
            ExecCommand::new([
                "bin/pulsar-admin",
                "topics",
                "unload",
                &format!("{topic}-partition-1"),
            ])
            .with_cmd_ready_condition(CmdWaitFor::exit()),
        )
        .await?;

    // A sequential caller has W=1: a single send parked on the detached
    // child blocks its whole window. Bound the ENTIRE post-unload workload,
    // not just each send, so healthy progress cannot be hidden by repeated
    // per-send timeouts.
    tokio::time::timeout(Duration::from_secs(30), async {
        for i in 0..20 {
            let payload = format!("post-unload-{i}").into_bytes();
            producer
                .send(OutgoingMessage::with_payload(payload))
                .await
                .unwrap_or_else(|err| {
                    panic!("publish {i} failed after unloading {topic}-partition-1: {err:?}")
                });
        }
    })
    .await
    .expect("bounded W=1 publish window stalled after partition unload (issues #451 and #463)");
    assert!(
        producer.is_connected(),
        "partition unload did not drop the connection"
    );

    // A real unload may answer the first re-attach with ServiceNotReady;
    // ADR-0080 then holds the child behind a bounded retry backoff. The 20
    // publishes above can complete through the healthy child before that
    // backoff expires. Require eventual re-entry instead of assuming it has
    // already completed at the end of the fast publish loop.
    tokio::time::timeout(Duration::from_secs(30), async {
        while !producer.not_ready_partitions().is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("detached child did not become ready after bounded re-attach retry");

    let recovered_before = producer.child_producers()[1].stats().total_msgs_sent;
    for i in 0..4 {
        producer
            .send(OutgoingMessage::with_payload(
                format!("after-reattach-{i}").into_bytes(),
            ))
            .await?;
    }
    assert!(
        producer.child_producers()[1].stats().total_msgs_sent > recovered_before,
        "the recovered child must rejoin the round-robin rotation"
    );

    producer.close().await?;
    drop(container);
    Ok(())
}

/// OBSERVABILITY (issue #451, ADR-0106 § Consequences): the re-attach sends
/// `topic_epoch: None`, so for an `Exclusive`-access producer the broker
/// arbitrates and may answer `ProducerFenced`, which terminalizes the slot.
/// Parity with Java on that path is UNVERIFIED, so the outcome is printed rather
/// than asserted — the assertion is only that the publish RESOLVES, either way,
/// instead of hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_producer_exclusive_unload_outcome() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, container) = start_pulsar().await?;
    let topic = "persistent://public/default/magnetar-e2e-producer-unload-exclusive";

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let producer = client
        .producer(topic)
        .access_mode(magnetar::proto::pb::ProducerAccessMode::Exclusive)
        .send_timeout(Duration::from_secs(10))
        .create()
        .await?;
    producer
        .send(OutgoingMessage::with_payload(b"exclusive-warmup".to_vec()).into())
        .await?;

    container
        .exec(
            ExecCommand::new(["bin/pulsar-admin", "topics", "unload", topic])
                .with_cmd_ready_condition(CmdWaitFor::exit()),
        )
        .await?;

    let outcome = tokio::time::timeout(
        Duration::from_secs(30),
        producer.send(OutgoingMessage::with_payload(b"exclusive-post-unload".to_vec()).into()),
    )
    .await
    .expect("an Exclusive producer's post-unload publish must RESOLVE, not hang");
    match outcome {
        Ok(id) => println!("exclusive-access re-attach after unload: published {id:?}"),
        Err(err) => println!("exclusive-access re-attach after unload: rejected {err:?}"),
    }

    drop(container);
    Ok(())
}
