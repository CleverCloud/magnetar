// SPDX-License-Identifier: Apache-2.0

//! Raw ordinary and transactional batch-ack parity.

#![allow(clippy::expect_used)]

use std::time::Duration;

use bytes::Bytes;
use magnetar_differential::broker::ScriptedBroker;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{ConnectionConfig, CreateProducerRequest, SubscribeRequest, TxnAction};
use magnetar_runtime_moonpool::{Client as MoonpoolClient, MoonpoolEngine};
use magnetar_runtime_tokio::Client as TokioClient;
use moonpool_core::TokioProviders;

#[derive(Debug, PartialEq, Eq)]
struct BatchAckTrace {
    ordinary_ids: usize,
    transactional_ids: usize,
    committed: bool,
}

fn outgoing(payload: &'static [u8]) -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(payload),
        metadata: magnetar_proto::pb::MessageMetadata::default(),
        uncompressed_size: u32::try_from(payload.len()).expect("small fixture"),
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

fn subscribe_request(topic: &str, subscription: &str) -> SubscribeRequest {
    SubscribeRequest {
        topic: topic.to_owned(),
        subscription: subscription.to_owned(),
        receiver_queue_size: 8,
        ..Default::default()
    }
}

async fn run_tokio(url: &str) -> BatchAckTrace {
    let topic = "persistent://public/default/tokio-batch-ack";
    let client = TokioClient::connect(url, ConnectionConfig::default())
        .await
        .expect("connect Tokio batch-ack client");
    let producer = client
        .open_producer(CreateProducerRequest {
            topic: topic.to_owned(),
            ..Default::default()
        })
        .await
        .expect("open Tokio batch-ack producer");
    let consumer = client
        .subscribe(subscribe_request(topic, "tokio-batch-ack-sub"))
        .await
        .expect("open Tokio batch-ack consumer");

    producer
        .send(outgoing(b"ordinary-one"))
        .await
        .expect("send");
    producer
        .send(outgoing(b"ordinary-two"))
        .await
        .expect("send");
    let ordinary = vec![
        consumer.receive().await.expect("receive").message_id,
        consumer.receive().await.expect("receive").message_id,
    ];
    consumer
        .ack_batch(ordinary.clone())
        .await
        .expect("Tokio ordinary batch ack");

    producer.send(outgoing(b"txn-one")).await.expect("send");
    producer.send(outgoing(b"txn-two")).await.expect("send");
    let transactional = vec![
        consumer.receive().await.expect("receive").message_id,
        consumer.receive().await.expect("receive").message_id,
    ];
    let txn = client
        .new_txn(Duration::from_secs(30))
        .await
        .expect("open Tokio transaction");
    consumer
        .ack_batch_with_txn(transactional.clone(), txn)
        .await
        .expect("Tokio transactional batch ack");
    let committed = client
        .end_txn(txn, TxnAction::Commit)
        .await
        .expect("commit Tokio transaction")
        == magnetar_proto::TxnState::Committed;

    consumer.close().await.expect("close Tokio consumer");
    producer.close().await.expect("close Tokio producer");
    client.close().await;
    BatchAckTrace {
        ordinary_ids: ordinary.len(),
        transactional_ids: transactional.len(),
        committed,
    }
}

async fn run_moonpool(host_port: &str) -> BatchAckTrace {
    let topic = "persistent://public/default/moonpool-batch-ack";
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = MoonpoolClient::connect_plain(&engine, host_port, ConnectionConfig::default())
        .await
        .expect("connect Moonpool batch-ack client");
    let producer = client
        .open_producer(CreateProducerRequest {
            topic: topic.to_owned(),
            ..Default::default()
        })
        .await
        .expect("open Moonpool batch-ack producer");
    let consumer = client
        .subscribe(subscribe_request(topic, "moonpool-batch-ack-sub"))
        .await
        .expect("open Moonpool batch-ack consumer");

    producer
        .send(outgoing(b"ordinary-one"))
        .await
        .expect("send");
    producer
        .send(outgoing(b"ordinary-two"))
        .await
        .expect("send");
    let ordinary = vec![
        consumer.receive().await.expect("receive").message_id,
        consumer.receive().await.expect("receive").message_id,
    ];
    consumer
        .ack_batch(ordinary.clone())
        .await
        .expect("Moonpool ordinary batch ack");

    producer.send(outgoing(b"txn-one")).await.expect("send");
    producer.send(outgoing(b"txn-two")).await.expect("send");
    let transactional = vec![
        consumer.receive().await.expect("receive").message_id,
        consumer.receive().await.expect("receive").message_id,
    ];
    let txn = client
        .new_txn(Duration::from_secs(30))
        .await
        .expect("open Moonpool transaction");
    consumer
        .ack_batch_with_txn(transactional.clone(), txn)
        .await
        .expect("Moonpool transactional batch ack");
    let committed = client
        .end_txn(txn, TxnAction::Commit)
        .await
        .expect("commit Moonpool transaction")
        == magnetar_proto::TxnState::Committed;

    consumer.close().await.expect("close Moonpool consumer");
    producer.close().await.expect("close Moonpool producer");
    client.close().await;
    BatchAckTrace {
        ordinary_ids: ordinary.len(),
        transactional_ids: transactional.len(),
        committed,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_and_transactional_batch_acks_are_equivalent() {
    let broker = ScriptedBroker::bind().await.expect("bind scripted broker");
    let tokio = run_tokio(&broker.pulsar_url()).await;
    let moonpool = run_moonpool(&broker.host_port()).await;
    assert_eq!(tokio, moonpool);
    assert_eq!(tokio.ordinary_ids, 2);
    assert_eq!(tokio.transactional_ids, 2);
    assert!(tokio.committed);
    broker.shutdown().await;
}
