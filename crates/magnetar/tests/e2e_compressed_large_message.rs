// SPDX-License-Identifier: Apache-2.0

//! A compressed message can exceed the default inflated-payload ceiling while its wire frame
//! remains below the broker frame limit. The default consumer rejects it; an explicitly larger
//! per-consumer ceiling receives and acknowledges the exact payload.

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::proto::types::CompressionKind;
use magnetar::{OutgoingMessage, PulsarClient};

mod common;

use common::pulsar::start_pulsar;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_consumer_receives_and_acks_compressed_payload_above_default_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = "persistent://public/default/magnetar-large-compressed-message";
    let payload = vec![0x5a; 6 * 1024 * 1024];

    let producer = client
        .producer(topic)
        .compression(CompressionKind::Zstd)
        .create()
        .await?;
    producer
        .send(OutgoingMessage::with_payload(payload.clone()).into())
        .await?;
    producer.close().await?;

    let default_consumer = client
        .consumer(topic)
        .subscription("large-message-default")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let error = default_consumer
        .receive()
        .await
        .expect_err("default ceiling rejects 6 MiB");
    assert!(
        error
            .to_string()
            .contains("configured decompression ceiling")
    );
    default_consumer.close().await?;

    let consumer = client
        .consumer(topic)
        .subscription("large-message-configured")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .max_uncompressed_message_size(8 * 1024 * 1024)
        .subscribe()
        .await?;
    let message = consumer.receive().await?;
    assert_eq!(message.payload, payload);
    consumer.ack(message.message_id).await?;
    consumer.close().await?;
    client.close().await;
    Ok(())
}
