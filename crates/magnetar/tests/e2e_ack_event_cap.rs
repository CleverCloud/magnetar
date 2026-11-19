// SPDX-License-Identifier: Apache-2.0

//! Exercise bounded observations and terminal ownership against a real broker.

use std::sync::Arc;
use std::time::Duration;

use magnetar::OutgoingMessage;
use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::proto::{
    ConnectionConfig, ConnectionEvent, CreateProducerRequest, PendingOpKey, SubscribeRequest,
};
use magnetar::runtime_tokio::{Client, ConnectionShared};

mod common;

use common::pulsar::{Broker, Pulsar};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const WAIT: Duration = Duration::from_secs(15);

async fn direct_client(running: &common::pulsar::Running) -> TestResult<Client> {
    let socket =
        tokio::net::TcpStream::connect((running.host.as_str(), running.binary_port)).await?;
    Ok(Client::from_socket(
        socket,
        ConnectionConfig {
            memory_limit_bytes: 1024 * 1024,
            ..Default::default()
        },
    )
    .await?)
}

fn topic(prefix: &str) -> String {
    format!(
        "persistent://public/default/{prefix}-{}",
        uuid::Uuid::new_v4().simple()
    )
}

async fn next_event(
    shared: &Arc<ConnectionShared>,
    predicate: impl Fn(&ConnectionEvent) -> bool,
) -> TestResult<ConnectionEvent> {
    Ok(tokio::time::timeout(WAIT, async {
        loop {
            if let Some(event) = shared.inner.lock().poll_event_if(&predicate) {
                return event;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn answered_acks_are_bounded_without_losing_completions() -> TestResult {
    let running = Pulsar::new(Broker::Default).start().await?;
    let topic = topic("magnetar-ack-cap");
    let publisher = direct_client(&running).await?;
    let producer = publisher
        .open_producer(CreateProducerRequest {
            topic: topic.clone(),
            ..Default::default()
        })
        .await?;
    assert!(
        publisher
            .shared()
            .inner
            .lock()
            .producer(producer.handle())
            .is_some()
    );
    let total = magnetar::proto::conn::EVENT_QUEUE_OBSERVATIONAL_CAP + 100;
    for _ in 0..total {
        tokio::time::timeout(
            WAIT,
            producer.send(OutgoingMessage::with_payload(b"ack-cap".as_slice()).into()),
        )
        .await??;
    }
    producer.close().await?;
    publisher.close().await;

    let client = direct_client(&running).await?;
    let consumer = client
        .subscribe(SubscribeRequest {
            topic,
            subscription: "ack-cap".to_owned(),
            sub_type: SubType::Exclusive,
            initial_position: InitialPosition::Earliest,
            receiver_queue_size: 1,
            ..Default::default()
        })
        .await?;
    assert!(
        client
            .shared()
            .inner
            .lock()
            .consumer(consumer.handle())
            .is_some()
    );
    for _ in 0..total {
        let message = tokio::time::timeout(WAIT, consumer.receive()).await??;
        while client
            .shared()
            .inner
            .lock()
            .poll_event_if(|event| matches!(event, ConnectionEvent::Message { .. }))
            .is_some()
        {}
        // Even after the observation queue fills, every broker response must resolve its waiter.
        tokio::time::timeout(WAIT, consumer.ack(message.message_id)).await??;
    }
    {
        let mut connection = client.shared().inner.lock();
        let dropped = connection.dropped_observational_events();
        let mut acknowledgements = 0;
        while connection
            .poll_event_if(|event| matches!(event, ConnectionEvent::AckResponse { .. }))
            .is_some()
        {
            acknowledgements += 1;
        }
        assert!(
            dropped > 0,
            "the bounded queue never dropped an observation"
        );
        assert!(acknowledgements <= magnetar::proto::conn::EVENT_QUEUE_OBSERVATIONAL_CAP);
        assert!(acknowledgements > 0, "no ACK responses were observed");
    }
    consumer.close().await?;
    client.close().await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn answered_unawaited_operations_and_closed_handles_are_released() -> TestResult {
    let running = Pulsar::new(Broker::Default).start().await?;
    let client = direct_client(&running).await?;
    let topic = topic("magnetar-release");
    let producer = client
        .open_producer(CreateProducerRequest {
            topic: topic.clone(),
            ..Default::default()
        })
        .await?;
    let producer_handle = producer.handle();
    assert!(
        client
            .shared()
            .inner
            .lock()
            .producer(producer_handle)
            .is_some()
    );

    // SendFut is enqueued synchronously. Dropping it must not cancel the wire send,
    // and the real receipt must not leave an outcome for the absent caller.
    for _ in 0..20 {
        let mut send = Box::pin(producer.send_bytes(b"abandoned".as_slice()));
        assert!(futures_util::poll!(send.as_mut()).is_pending());
        drop(send);
        assert_eq!(client.shared().memory_limit.used_bytes(), 9);
        let event = next_event(client.shared(), |event| {
            matches!(event, ConnectionEvent::SendReceipt { handle, .. } if *handle == producer_handle)
        })
        .await?;
        let ConnectionEvent::SendReceipt { sequence_id, .. } = event else {
            unreachable!();
        };
        assert_eq!(client.shared().memory_limit.used_bytes(), 0);
        assert!(
            client
                .shared()
                .inner
                .lock()
                .take_outcome(PendingOpKey::Send(producer_handle, sequence_id))
                .is_none()
        );
    }
    producer.send_bytes(b"awaited".as_slice()).await?;

    // One subscription tests immediate fallback and one tests timed grouping.
    // In each case the real broker response supplies the precise request key.
    for (subscription, group_time) in [
        ("fallback", None),
        ("grouped", Some(Duration::from_millis(20))),
    ] {
        let consumer = client
            .subscribe(SubscribeRequest {
                topic: topic.clone(),
                subscription: subscription.to_owned(),
                sub_type: SubType::Exclusive,
                initial_position: InitialPosition::Earliest,
                ack_group_time: group_time,
                ..Default::default()
            })
            .await?;
        let handle = consumer.handle();
        assert!(client.shared().inner.lock().consumer(handle).is_some());
        for _ in 0..20 {
            let message = tokio::time::timeout(WAIT, consumer.receive()).await??;
            assert_eq!(message.payload.as_ref(), b"abandoned");
            consumer.ack_grouped(message.message_id);
            let event = next_event(client.shared(), |event| {
                matches!(event, ConnectionEvent::AckResponse { .. })
            })
            .await?;
            let ConnectionEvent::AckResponse {
                request_id: Some(id),
                result,
            } = event
            else {
                return Err("broker ACK response had no request id".into());
            };
            result.map_err(|error| format!("broker rejected grouped ACK: {error}"))?;
            assert!(
                client
                    .shared()
                    .inner
                    .lock()
                    .take_outcome(PendingOpKey::Request(id))
                    .is_none()
            );
        }
        let survivor = consumer.clone();
        consumer.close().await?;
        drop(survivor);
        assert!(client.shared().inner.lock().consumer(handle).is_none());
    }
    producer.close().await?;
    assert!(
        client
            .shared()
            .inner
            .lock()
            .producer(producer_handle)
            .is_none()
    );

    // Churn both explicit-close and final-clone paths on the same live connection.
    for index in 0..20 {
        let producer = client
            .open_producer(CreateProducerRequest {
                topic: topic.clone(),
                ..Default::default()
            })
            .await?;
        let handle = producer.handle();
        assert!(client.shared().inner.lock().producer(handle).is_some());
        producer.send_bytes(b"churn".as_slice()).await?;
        if index % 2 == 0 {
            producer.close().await?;
        } else {
            drop(producer);
        }
        tokio::time::timeout(WAIT, async {
            while client.shared().inner.lock().producer(handle).is_some() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;

        let consumer = client
            .subscribe(SubscribeRequest {
                topic: topic.clone(),
                subscription: format!("churn-{index}"),
                ..Default::default()
            })
            .await?;
        let handle = consumer.handle();
        assert!(client.shared().inner.lock().consumer(handle).is_some());
        if index % 2 == 0 {
            consumer.close().await?;
        } else {
            drop(consumer);
        }
        tokio::time::timeout(WAIT, async {
            while client.shared().inner.lock().consumer(handle).is_some() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
    }
    assert!(
        client.is_connected(),
        "churn must leave the connection usable"
    );
    client.close().await;
    Ok(())
}
