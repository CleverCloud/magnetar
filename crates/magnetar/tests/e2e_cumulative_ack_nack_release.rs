// SPDX-License-Identifier: Apache-2.0

//! End-to-end regression for a cumulative ack that covers a negatively acknowledged message.
//!
//! A cumulative ack acknowledges every earlier delivery, so the client must stop tracking all of
//! them. Before the fix it released only the id the ack named: a message nacked earlier stayed in
//! the nack tracker, and when its delay elapsed the client asked the broker to redeliver it. On an
//! Exclusive subscription the broker does not redeliver single ids, it rewinds the cursor, so
//! every delivery after the acknowledged position came back a second time although the
//! application already held it.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Requires Docker on the host.

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};

mod common;

use common::pulsar::start_pulsar;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_cumulative_ack_releases_a_nacked_message_it_covers()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let id = uuid::Uuid::new_v4().simple();
    let topic = format!("persistent://public/default/magnetar-e2e-cumulative-nack-{id}");

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let producer = client.producer(&topic).create().await?;
    for i in 0..5 {
        producer
            .send(OutgoingMessage::with_payload(format!("m-{i}").into_bytes()).into())
            .await?;
    }
    producer.close().await?;

    let consumer = client
        .consumer(&topic)
        .subscription("magnetar-cumulative-nack-sub")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .negative_ack_redelivery_delay(Duration::from_secs(1))
        .subscribe()
        .await?;

    let mut received = Vec::new();
    for i in 0..5 {
        let msg = tokio::time::timeout(Duration::from_secs(20), consumer.receive())
            .await
            .map_err(|_| format!("message {i} never arrived"))??;
        assert_eq!(msg.payload.as_ref(), format!("m-{i}").as_bytes());
        received.push(msg);
    }

    // Nack m-1, then acknowledge m-0..=m-2 with one cumulative ack: m-1 is acknowledged, and its
    // nack must not fire. m-3 and m-4 stay unacknowledged and held by the application.
    consumer.negative_ack(received[1].message_id);
    consumer.ack_cumulative(received[2].message_id).await?;

    // Well past the nack delay, nothing comes back: a redelivery request for m-1 would make the
    // broker rewind and send m-3 and m-4 a second time.
    let stray = tokio::time::timeout(Duration::from_secs(5), consumer.receive()).await;
    assert!(
        stray.is_err(),
        "after a cumulative ack that covers a nacked message, nothing must be redelivered; got \
         {stray:?}"
    );

    consumer.ack(received[3].message_id).await?;
    consumer.ack(received[4].message_id).await?;
    consumer.close().await?;
    client.close().await;
    Ok(())
}
