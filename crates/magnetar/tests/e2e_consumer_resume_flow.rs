// SPDX-License-Identifier: Apache-2.0

//! End-to-end regression for `Consumer::resume` after a paused consumer drained its window.
//!
//! `pause()` stops the automatic flow refill, so a consumer that receives its whole window while
//! paused leaves the broker with no permits and owes it the window. Before the fix `resume()`
//! only cleared the flag: no `CommandFlow` went out, and a `receive()` parked during the pause in
//! another task, which only an arrival wakes, waited for good. After the fix `resume()` sends the
//! flow the pause withheld, the broker dispatches again, and the parked `receive()` gets the next
//! message.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Requires Docker on the host.

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};

mod common;

use common::pulsar::start_pulsar;

/// The receiver queue: the broker holds at most this many permits for the consumer.
const WINDOW: usize = 4;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_resume_releases_the_flow_a_drained_pause_withheld()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let id = uuid::Uuid::new_v4().simple();
    let topic = format!("persistent://public/default/magnetar-e2e-resume-flow-{id}");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let producer = client.producer(&topic).create().await?;
    for i in 0..3 * WINDOW {
        producer
            .send(OutgoingMessage::with_payload(format!("m-{i}").into_bytes()).into())
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-resume-flow-sub")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .receiver_queue_size(WINDOW)
        .subscribe()
        .await?;

    // Drain the whole window while paused: the refill those receives would trigger is withheld.
    consumer.pause();
    for i in 0..WINDOW {
        let msg = tokio::time::timeout(Duration::from_secs(20), consumer.receive())
            .await
            .map_err(|_| format!("message {i} of the first window never arrived"))??;
        consumer.ack(msg.message_id).await?;
    }

    // The next receive parks in a task of its own, which only an arrival wakes: the broker holds
    // no permit while the consumer is paused.
    let parked = tokio::spawn({
        let consumer = consumer.clone();
        async move { consumer.receive().await }
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        !parked.is_finished(),
        "a paused consumer that drained its window must not receive more"
    );

    consumer.resume();
    let next = tokio::time::timeout(Duration::from_secs(20), parked)
        .await
        .map_err(|_| {
            "the receive parked during the pause never completed after resume(): the flow the \
             pause withheld was never sent"
        })???;
    assert_eq!(next.payload.as_ref(), format!("m-{WINDOW}").as_bytes());
    consumer.ack(next.message_id).await?;

    // And the consumer keeps flowing after that.
    for i in WINDOW + 1..3 * WINDOW {
        let msg = tokio::time::timeout(Duration::from_secs(20), consumer.receive())
            .await
            .map_err(|_| format!("message {i} never arrived after the resume"))??;
        assert_eq!(msg.payload.as_ref(), format!("m-{i}").as_bytes());
        consumer.ack(msg.message_id).await?;
    }

    consumer.close().await?;
    client.close().await;
    Ok(())
}
