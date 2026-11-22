// SPDX-License-Identifier: Apache-2.0

//! `republish_dead_letters` after a failed publish — differential equivalence (ADR-0024 layer d).
//!
//! `Consumer::republish_dead_letters` drains the consumer's whole dead-letter buffer before its
//! first await and then publishes and acks the messages one by one. This file proves what a
//! caller is left holding when the DLQ publish fails: the messages the call never republished
//! must not have vanished with the drained buffer, because they are in no queue, no unacked
//! tracker and not acked, so the broker keeps them unacked against this consumer.
//!
//! ## The scenario
//!
//! Three messages are published to a topic. A `Shared` consumer with `max_redeliver_count = 1`
//! receives and nacks each of them twice, so their third dispatch (`redelivery_count` 2) crosses
//! the threshold and the client routes all three to its dead-letter buffer instead of the queue.
//! A sentinel published behind them, received through the queue, proves the three redeliveries
//! have all been processed (the scripted broker serves its redelivery pool before new entries).
//!
//! The DLQ producer is then closed through a clone, so the first `send` of the republish fails
//! synchronously and nothing is published. The call must return an error, and the dead letters
//! it never reached must still be owed to the application: `drain_dead_letter` afterwards
//! returns at least those two. Whether the message whose send failed is re-buffered too (the
//! better outcome here, since nothing was published) is the fix's design choice, and so is HOW
//! the fix keeps them (re-buffering on error, popping one at a time); the assertion only says
//! that none of the unattempted ones was lost.

use std::time::Duration;

use bytes::Bytes;
use magnetar_differential::broker::ScriptedBroker;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{ConnectionConfig, CreateProducerRequest, SubscribeRequest, pb};

const TOPIC: &str = "persistent://public/default/dlr-error-source";
const DLQ_TOPIC: &str = "persistent://public/default/dlr-error-source-DLQ";
const SUBSCRIPTION: &str = "dlr-error-sub";
const POISON: [&[u8]; 3] = [b"poison-0", b"poison-1", b"poison-2"];
const SENTINEL: &[u8] = b"sentinel";
/// Receive budget of one hop; a healthy hop resolves at once.
const RECV_WAIT: Duration = Duration::from_secs(5);

fn outgoing(payload: &'static [u8]) -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(payload),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: u32::try_from(payload.len()).unwrap_or(u32::MAX),
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

fn subscribe_request() -> SubscribeRequest {
    SubscribeRequest {
        topic: TOPIC.to_owned(),
        subscription: SUBSCRIPTION.to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        receiver_queue_size: 8,
        max_redeliver_count: 1,
        durable: true,
        ..Default::default()
    }
}

fn producer_request(topic: &str) -> CreateProducerRequest {
    CreateProducerRequest {
        topic: topic.to_owned(),
        ..Default::default()
    }
}

/// What the caller of `republish_dead_letters` observed after the DLQ publish failed.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    /// The republish call returned an error.
    republish_failed: bool,
    /// How many dead letters `drain_dead_letter` still returned afterwards.
    still_owed: usize,
}

async fn run_tokio(pulsar_url: &str) -> Observed {
    use magnetar_runtime_tokio::Client;

    let client = Client::connect(pulsar_url, ConnectionConfig::default())
        .await
        .expect("tokio connect");
    let producer = client
        .open_producer(producer_request(TOPIC))
        .await
        .expect("tokio open_producer");
    for payload in POISON {
        producer.send(outgoing(payload)).await.expect("tokio send");
    }
    let consumer = client
        .subscribe(subscribe_request())
        .await
        .expect("tokio subscribe");
    // Two rounds of receive-and-nack; the third dispatch of each entry is dead-lettered.
    for _round in 0..2 {
        let mut ids = Vec::new();
        for _ in POISON {
            let msg = consumer
                .receive_with_timeout(RECV_WAIT)
                .await
                .expect("tokio receive")
                .expect("tokio: a poison dispatch below the threshold must reach the queue");
            ids.push(msg.message_id);
        }
        for id in ids {
            consumer.negative_ack(id);
        }
    }
    producer
        .send(outgoing(SENTINEL))
        .await
        .expect("tokio send sentinel");
    let sentinel = consumer
        .receive_with_timeout(RECV_WAIT)
        .await
        .expect("tokio receive sentinel")
        .expect("tokio: the sentinel must be dispatched behind the dead-lettered entries");
    assert_eq!(&sentinel.payload[..], SENTINEL);

    let dlq = client
        .open_producer(producer_request(DLQ_TOPIC))
        .await
        .expect("tokio open dlq producer");
    let closed_clone = dlq.clone();
    dlq.close().await.expect("tokio close dlq producer");

    let republish_failed = consumer
        .republish_dead_letters(&closed_clone)
        .await
        .is_err();
    let still_owed = consumer.drain_dead_letter().len();
    client.close().await;
    Observed {
        republish_failed,
        still_owed,
    }
}

async fn run_moonpool(host_port: &str) -> Observed {
    use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
    use moonpool_core::TokioProviders;

    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = Client::connect_plain(&engine, host_port, ConnectionConfig::default())
        .await
        .expect("moonpool connect_plain");
    let producer = client
        .open_producer(producer_request(TOPIC))
        .await
        .expect("moonpool open_producer");
    for payload in POISON {
        producer
            .send(outgoing(payload))
            .await
            .expect("moonpool send");
    }
    let consumer = client
        .subscribe(subscribe_request())
        .await
        .expect("moonpool subscribe");
    for _round in 0..2 {
        let mut ids = Vec::new();
        for _ in POISON {
            let msg = consumer
                .receive_with_timeout(RECV_WAIT)
                .await
                .expect("moonpool receive")
                .expect("moonpool: a poison dispatch below the threshold must reach the queue");
            ids.push(msg.message_id);
        }
        for id in ids {
            consumer.negative_ack(id);
        }
    }
    producer
        .send(outgoing(SENTINEL))
        .await
        .expect("moonpool send sentinel");
    let sentinel = consumer
        .receive_with_timeout(RECV_WAIT)
        .await
        .expect("moonpool receive sentinel")
        .expect("moonpool: the sentinel must be dispatched behind the dead-lettered entries");
    assert_eq!(&sentinel.payload[..], SENTINEL);

    let dlq = client
        .open_producer(producer_request(DLQ_TOPIC))
        .await
        .expect("moonpool open dlq producer");
    let closed_clone = dlq.clone();
    dlq.close().await.expect("moonpool close dlq producer");

    let republish_failed = consumer
        .republish_dead_letters(&closed_clone)
        .await
        .is_err();
    let still_owed = consumer.drain_dead_letter().len();
    client.close().await;
    Observed {
        republish_failed,
        still_owed,
    }
}

/// A failed DLQ publish neither loses the dead letters the call never reached nor goes
/// unreported, and both engines agree on what the caller was left with.
#[tokio::test(flavor = "current_thread")]
async fn republish_dead_letters_failed_publish_event_stream_parity() {
    let tokio_broker = ScriptedBroker::bind().await.expect("broker bind");
    let tokio_observed = run_tokio(&tokio_broker.pulsar_url()).await;
    tokio_broker.shutdown().await;

    let moonpool_broker = ScriptedBroker::bind().await.expect("broker bind");
    let moonpool_observed = run_moonpool(&moonpool_broker.host_port()).await;
    moonpool_broker.shutdown().await;

    assert_eq!(
        tokio_observed, moonpool_observed,
        "engine outcomes diverged for republish_dead_letters after a failed DLQ publish",
    );
    assert!(
        tokio_observed.republish_failed,
        "a send on a closed DLQ producer must fail the republish: {tokio_observed:?}",
    );
    // Re-buffering the message whose send failed is the fix's design choice; the ones behind it
    // were never attempted and must still be owed.
    assert!(
        tokio_observed.still_owed >= POISON.len() - 1,
        "the dead letters the failed call never reached were dropped with its drained buffer: \
         {tokio_observed:?}",
    );
}
