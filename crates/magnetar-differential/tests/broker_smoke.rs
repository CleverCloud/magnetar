// SPDX-License-Identifier: Apache-2.0

//! Smoke test for the scripted broker: confirm a tokio engine can
//! connect and handshake without timing out.

use std::time::Duration;

use bytes::Bytes;
use magnetar_differential::broker::ScriptedBroker;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{ConnectionConfig, CreateProducerRequest};
use magnetar_runtime_tokio::Client;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_client_handshakes() {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let url = broker.pulsar_url();
    eprintln!("[test] broker @ {url}");

    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect succeeded");

    eprintln!("[test] connected, is_connected={}", client.is_connected());
    assert!(client.is_connected());

    // Background kicker — periodically pulses `driver_waker.notify_one()`
    // so orphan tasks spawned by the engine's wait_* futures get a
    // chance to drain. See runner_tokio::Kicker for rationale.
    let shared = client.shared().clone();
    let kicker = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(25)).await;
            shared.driver_waker.notify_one();
        }
    });

    // Try opening a producer.
    eprintln!("[test] opening producer");
    let producer = tokio::time::timeout(
        Duration::from_secs(3),
        client.open_producer_with(
            CreateProducerRequest {
                topic: "persistent://public/default/smoke".to_owned(),
                ..Default::default()
            },
            None,
        ),
    )
    .await
    .expect("producer open did not time out")
    .expect("producer opened");
    eprintln!("[test] producer ready, name={:?}", producer.name());

    // Send a message.
    eprintln!("[test] sending");
    let mid = tokio::time::timeout(
        Duration::from_secs(3),
        producer.send(OutgoingMessage {
            payload: Bytes::from_static(b"smoke"),
            metadata: magnetar_proto::pb::MessageMetadata::default(),
            uncompressed_size: 5,
            num_messages: 1,
            txn_id: None,
        }),
    )
    .await
    .expect("send did not time out")
    .expect("send ok");
    eprintln!("[test] sent, mid={mid:?}");

    // Don't call close — just drop and let the broker session task be reaped.
    drop(producer);
    kicker.abort();
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
    broker.shutdown().await;
    eprintln!("[test] done");
}
