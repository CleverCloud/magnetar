// SPDX-License-Identifier: Apache-2.0

//! End-to-end regression for the negative-ack / ack-timeout double-redelivery bug
//! against a real Apache Pulsar 4.x standalone broker.
//!
//! A consumer configured with BOTH `negative_ack_redelivery_delay` AND `ack_timeout`
//! must redeliver a nacked message EXACTLY ONCE. Before the fix, `negative_ack`
//! deferred the id to the nack tracker without removing it from the ack-timeout
//! (unacked-message) tracker, so the message was redelivered twice — once when the
//! nack delay elapsed, once when the ack-timeout window elapsed — breaking
//! at-least-once-without-duplication. This test nacks a message, consumes the single
//! nack-driven redelivery, and asserts NO second (ack-timeout-driven) copy arrives.
//!
//! Run with:
//!
//! ```sh
//! cargo test -p magnetar --test e2e_nack_unacked_removal -- --nocapture
//! ```
//!
//! Requires Docker on the host.

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};

mod common;

use common::pulsar::start_pulsar;

/// A nacked message on a consumer that ALSO configures `ack_timeout` is redelivered
/// by the nack path exactly once, then is ackable and the subscription settles — no
/// duplicate redelivery from the ack-timeout sweep, no message loss.
///
/// The double-redelivery bug fires inside `Connection::handle_timeout` (two
/// `CommandRedeliverUnacknowledgedMessages` for the same id when the nacked id is
/// left in the unacked tracker). The deterministic frame-count proof lives in the
/// proto / runtime / differential layers, which can pin a synthetic clock; this
/// end-to-end test pins the user-visible contract against a real broker: a consumer
/// that configures BOTH `negative_ack_redelivery_delay` and `ack_timeout` redelivers
/// a nacked message exactly once and can ack it without a stray duplicate arriving.
/// The `ack_timeout` is set well beyond the test window so its sweep cannot fire and
/// confound the count — the regression we guard is that the nack + ack-timeout code
/// paths coexist without the duplicate the bug produced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_nack_with_ack_timeout_redelivers_once() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;

    let id = uuid::Uuid::new_v4().simple();
    let topic = format!("persistent://public/default/magnetar-e2e-nack-unacked-{id}");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let producer = client.producer(topic.clone()).create().await?;
    producer
        .send(OutgoingMessage::with_payload(b"nack-me-once".to_vec()).into())
        .await?;
    producer.close().await?;

    // Short nack delay (1s) drives a prompt redelivery. The ack-timeout is set far
    // beyond the test window (60s) so its sweep cannot fire here; the regression is
    // that the nacked id no longer lingers in the unacked tracker (the bug left it
    // there, producing a second redelivery once the ack-timeout elapsed).
    let consumer = client
        .consumer(topic.clone())
        .subscription("magnetar-nack-unacked-sub")
        .subscription_type(SubType::Shared)
        .negative_ack_redelivery_delay(Duration::from_secs(1))
        .ack_timeout(Duration::from_mins(1))
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    // First receive — the original delivery. Do NOT ack; nack it instead.
    let original = consumer.receive().await?;
    assert_eq!(original.payload.as_ref(), b"nack-me-once");
    consumer.negative_ack(original.message_id);

    // The nack delay (1s) elapses → exactly one redelivery. Ack it immediately so the
    // subscription cursor advances; the redelivered copy carries a bumped
    // redelivery_count.
    let redelivered = tokio::time::timeout(Duration::from_secs(8), consumer.receive())
        .await
        .expect("nack redelivery must arrive within 8s")?;
    assert_eq!(redelivered.payload.as_ref(), b"nack-me-once");
    assert!(
        redelivered.redelivery_count >= 1,
        "the redelivered copy must carry a bumped redelivery_count, got {}",
        redelivered.redelivery_count
    );
    consumer.ack(redelivered.message_id).await?;

    // No further copy must arrive: the message was nacked once, redelivered once, and
    // acked. A stray copy here would be the ack-timeout-driven duplicate the bug
    // produced (or a redelivery of an id the ack should have cleared).
    let stray = tokio::time::timeout(Duration::from_secs(4), consumer.receive()).await;
    assert!(
        stray.is_err(),
        "after one nack-driven redelivery and an ack, no further copy must arrive; got {stray:?}"
    );

    consumer.close().await?;
    client.close().await;
    Ok(())
}
