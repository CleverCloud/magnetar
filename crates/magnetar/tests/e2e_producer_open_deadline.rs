// SPDX-License-Identifier: Apache-2.0

//! Issue #887 e2e — the `operation_timeout` deadline ADR-0125 puts on a pending
//! `CommandProducer` must not pre-empt a re-attach a real broker DOES answer.
//!
//! ADR-0125 arms a deadline on every pending `CommandProducer` whose producer has attached
//! at least once: the reconnect rebuilds, the ADR-0080 retry leg, and ADR-0106's in-place
//! re-attach. Its expiry consumes one ADR-0080 attempt and, once the budget is gone,
//! terminalizes the producer. That makes an over-eager deadline a regression in its own
//! right — a real `topics unload`, whose re-attach the broker answers in milliseconds (or
//! rejects `ServiceNotReady` and answers the retry a backoff later), must still recover in
//! place rather than burn attempts or terminalize.
//!
//! This suite runs the ADR-0106 unload against a real broker with an `operation_timeout`
//! well below the 30 s default, so the new deadline is genuinely armed and close to the
//! broker's own re-attach window, and asserts the producer recovers with its partitions
//! ready and its connection intact.
//!
//! **What no e2e can cover.** The deadline's own firing needs a broker that receives a
//! `CommandProducer` and never answers it — neither `CommandProducerSuccess` nor
//! `CommandError`. No `pulsar-admin` surface produces that, and faking it would mean
//! replacing the broker, which is what the proto, two runtime and differential layers do
//! (`crates/magnetar-proto/src/conn.rs`,
//! `crates/magnetar-runtime-{tokio,moonpool}/tests/producer_open_deadline.rs`,
//! `crates/magnetar-differential/tests/producer_open_deadline_equivalence.rs`). This layer
//! owns the other half: that the deadline is inert on a healthy broker.
//!
//! Runs as a regular test under `cargo test` (ADR-0046) — no `#[ignore]`, no feature gate.
//! Requires Docker on the host.

use std::time::Duration;

use magnetar::{MessageRoutingMode, OutgoingMessage, PulsarClient};
use testcontainers::core::{CmdWaitFor, ExecCommand};

mod common;

use common::pulsar::start_pulsar;

const PARTITIONS: usize = 2;

/// Well under the 30 s default, so the pending-open deadline is armed near the broker's own
/// re-attach window instead of three orders of magnitude above it — while still leaving a
/// container-sized margin for connect, partition metadata and lookup, which the same budget
/// bounds.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(10);

/// NO FALSE POSITIVE (issue #887): a `topics unload` the broker answers must recover through
/// ADR-0106's in-place re-attach with the ADR-0125 deadline armed — publishes all `Ok`, both
/// partitions ready again, the connection never dropped, and no child terminalized.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_producer_open_deadline_does_not_preempt_an_answered_reattach()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, container) = start_pulsar().await?;
    let topic = "persistent://public/default/magnetar-e2e-producer-open-deadline";

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
        .operation_timeout(OPERATION_TIMEOUT)
        .build()
        .await?;

    let producer = client
        .partitioned_producer(topic.to_owned())
        .routing(MessageRoutingMode::RoundRobin)
        .send_timeout(Duration::from_secs(30))
        .create()
        .await?;

    // Warm up so both children are attached — the deadline only arms for a producer that
    // has attached at least once.
    for i in 0..(PARTITIONS * 2) {
        producer
            .send(OutgoingMessage::with_payload(
                format!("warmup-{i}").into_bytes(),
            ))
            .await?;
    }

    // Detach the producer on ONE partition. The standalone broker reloads the bundle on
    // itself: no TCP drop, so nothing resets the connection and the recovery is ADR-0106's
    // in-place re-attach, now running under an armed deadline.
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

    // Bound the whole post-unload workload, not each send: a sequential caller has an
    // in-flight window of one, so a single parked publish blocks all of them.
    tokio::time::timeout(Duration::from_secs(60), async {
        for i in 0..20 {
            let payload = format!("post-unload-{i}").into_bytes();
            producer
                .send(OutgoingMessage::with_payload(payload))
                .await
                .unwrap_or_else(|err| {
                    panic!(
                        "publish {i} failed after unloading {topic}-partition-1 — an \
                         over-eager pending-open deadline would terminalize the child \
                         instead of letting the re-attach land (issue #887): {err:?}"
                    )
                });
        }
    })
    .await
    .expect("bounded publish window stalled after partition unload");

    assert!(
        producer.is_connected(),
        "partition unload did not drop the connection"
    );

    // A real unload may answer the first re-attach `ServiceNotReady`, which holds the child
    // behind one bounded ADR-0080 backoff; require eventual re-entry rather than assuming
    // it already happened by the end of the publish loop.
    tokio::time::timeout(Duration::from_secs(60), async {
        while !producer.not_ready_partitions().is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect(
        "detached child never became ready again: with the ADR-0125 deadline armed, a \
         re-attach the broker answers must still reopen the drain gate",
    );

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
