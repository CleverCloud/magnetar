// SPDX-License-Identifier: Apache-2.0

//! End-to-end regression for a batched send that timed out inside the batch container.
//!
//! A batching producer without a publish delay keeps a message in its batch container until a
//! limit or a `flush()` turns the container into a frame. When `send_timeout` elapses first, the
//! caller is told `send timeout`, and the message must never reach the broker afterwards. It used
//! to stay in the container, and the next `flush()` published it although its caller had already
//! been told it failed, so a caller that retried published the message twice.
//!
//! The scenario needs no fault injection: the producer is the only thing that can publish the
//! message, and it publishes it only at the flush.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Requires Docker on the host.

use std::time::Duration;

use magnetar::{OutgoingMessage, PulsarClient};

mod common;

use common::pulsar::start_pulsar;

/// Short, explicit per-producer send timeout, so the deadline fires inside the test budget.
const SEND_TIMEOUT: Duration = Duration::from_secs(2);

/// Upper bound for a message that is expected to arrive.
const WAIT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_a_batched_send_that_timed_out_in_the_container_is_not_published_by_the_next_flush()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let id = uuid::Uuid::new_v4().simple();
    let topic = format!("persistent://public/default/magnetar-e2e-batched-send-timeout-{id}");
    let consumer = client
        .consumer(&topic)
        .subscription(format!("sub-{id}"))
        .subscribe()
        .await?;
    // No publish delay: nothing but a flush publishes the container.
    let producer = client
        .producer(&topic)
        .batching(100, 1024 * 1024)
        .send_timeout(SEND_TIMEOUT)
        .create()
        .await?;

    let doomed = producer
        .send(OutgoingMessage::with_payload(b"doomed".to_vec()).into())
        .await;
    assert!(
        doomed.is_err(),
        "a message that sits in the batch container past send_timeout must fail with a send timeout"
    );

    // The container is flushed now. The message whose caller was told it failed must not be in it.
    producer.flush().await?;
    let control = producer.send(OutgoingMessage::with_payload(b"control".to_vec()).into());
    producer.flush().await?;
    control.await?;

    // Both messages go through the same producer, so a message published by the first flush would
    // reach the consumer before the control.
    let first = tokio::time::timeout(WAIT, consumer.receive()).await??;
    assert_eq!(
        String::from_utf8_lossy(&first.payload),
        "control",
        "the message whose caller was told 'send timeout' was published by the next flush"
    );
    consumer.ack(first.message_id).await?;

    producer.close().await?;
    consumer.close().await?;
    client.close().await;
    Ok(())
}
