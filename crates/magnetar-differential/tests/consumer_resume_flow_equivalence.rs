// SPDX-License-Identifier: Apache-2.0

//! `Consumer::resume` after the receive loop drained a paused window — differential equivalence
//! (ADR-0024 layer d).
//!
//! `pause()` withholds the automatic flow refill, so a consumer whose receive loop drains the
//! whole window while paused owes the broker that window and the broker holds zero permits.
//! `resume()` is the only thing that can release the debt: no arrival comes (the broker has
//! nothing it may dispatch), the stall watchdog skips a consumer with no permits, and a
//! `receive()` parked on the empty queue is woken by arrivals only.
//!
//! ## The scenario
//!
//! Eight messages are published, a consumer with an eight-permit receiver queue subscribes and
//! receives all of them, then:
//!
//! 1. the consumer is paused and the receive loop drains the window (the broker has seen only the
//!    initial grant, because a paused consumer refills nothing);
//! 2. a ninth message is published, which the broker cannot dispatch because it holds no permit;
//! 3. a `receive()` in its own task parks on the empty queue and, a moment later, the test task
//!    resumes the consumer.
//!
//! The ninth message reaches the parked `receive()` only if `resume()` released the withheld window
//! to the broker. Both engines must deliver it and must agree on every step.

use std::time::Duration;

use bytes::Bytes;
use magnetar_differential::broker::ScriptedBroker;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{ConnectionConfig, CreateProducerRequest, SubscribeRequest, pb};

const TOPIC: &str = "persistent://public/default/resume-owed-flow";
const SUBSCRIPTION: &str = "resume-owed-flow-sub";

/// The consumer's receiver queue, and therefore the size of the window the pause withholds.
const RQ: usize = 8;

/// Budget for a receive of a message that is already queued. A healthy leg resolves at once.
const QUEUED_RECV_TIMEOUT: Duration = Duration::from_secs(5);

/// Budget for the parked receive after `resume()`. A wedged leg waits it out once per engine; a
/// healthy leg resolves within a few round trips of the loopback broker.
const RESUMED_RECV_TIMEOUT: Duration = Duration::from_secs(3);

/// How long the receive stays parked before the consumer is resumed, so the resume can never
/// run ahead of the park.
const PARK_SETTLE: Duration = Duration::from_millis(50);

/// What a caller of the paused-then-resumed consumer observed on one engine.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    /// Payloads the receive loop drained while the consumer was paused, in order.
    drained: Vec<String>,
    /// Every `CommandFlow` grant the broker had seen once the paused drain was done.
    grants_while_paused: Vec<u32>,
    /// Payload the parked `receive()` resolved with after `resume()`, or `None` when it was still
    /// parked at the end of its budget.
    delivered_after_resume: Option<String>,
    /// Every `CommandFlow` grant the broker saw by the end of the scenario.
    grants_final: Vec<u32>,
}

fn payload(index: usize) -> Vec<u8> {
    format!("m{index}").into_bytes()
}

fn outgoing(payload: Vec<u8>) -> OutgoingMessage {
    OutgoingMessage {
        uncompressed_size: u32::try_from(payload.len()).unwrap_or(u32::MAX),
        payload: Bytes::from(payload),
        metadata: pb::MessageMetadata::default(),
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

fn subscribe_request() -> SubscribeRequest {
    SubscribeRequest {
        topic: TOPIC.to_owned(),
        subscription: SUBSCRIPTION.to_owned(),
        receiver_queue_size: RQ,
        durable: true,
        ..Default::default()
    }
}

fn grants(broker: &ScriptedBroker) -> Vec<u32> {
    broker
        .flow_grant_log_snapshot()
        .into_iter()
        .map(|(_, permits)| permits)
        .collect()
}

/// Bounded wait for the broker's dispatch of the window to land in the consumer's queue.
async fn wait_until_queued(available_in_queue: impl Fn() -> usize) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while available_in_queue() < RQ {
        assert!(
            std::time::Instant::now() < deadline,
            "the broker's {RQ} messages never reached the consumer's receiver queue",
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn text(payload: &Bytes) -> String {
    String::from_utf8_lossy(payload).into_owned()
}

async fn run_tokio(broker: &ScriptedBroker) -> Observed {
    use magnetar_runtime_tokio::Client;

    let client = Client::connect(&broker.pulsar_url(), ConnectionConfig::default())
        .await
        .expect("tokio connect");
    let producer = client
        .open_producer(CreateProducerRequest {
            topic: TOPIC.to_owned(),
            ..Default::default()
        })
        .await
        .expect("tokio open_producer");
    for index in 0..RQ {
        producer
            .send(outgoing(payload(index)))
            .await
            .expect("tokio send");
    }
    let consumer = client
        .subscribe(subscribe_request())
        .await
        .expect("tokio subscribe");
    wait_until_queued(|| consumer.available_in_queue()).await;

    consumer.pause();
    let mut drained = Vec::new();
    for _ in 0..RQ {
        let message = tokio::time::timeout(QUEUED_RECV_TIMEOUT, consumer.receive())
            .await
            .expect("a queued message must be received at once")
            .expect("tokio receive");
        drained.push(text(&message.payload));
    }
    let grants_while_paused = grants(broker);

    producer
        .send(outgoing(payload(RQ)))
        .await
        .expect("tokio send behind the drained window");
    // The receive loop runs in its own task, so it is polled again only when its waker fires,
    // exactly like an application's receive loop. Polling it from the resuming task (a `join!`
    // re-polls every branch) would run `pop_message` after the resume and mask the defect.
    let receiving = consumer.clone();
    let mut parked = tokio::spawn(async move { receiving.receive().await });
    tokio::time::sleep(PARK_SETTLE).await;
    consumer.resume();
    let resolved = tokio::time::timeout(RESUMED_RECV_TIMEOUT, &mut parked).await;
    if resolved.is_err() {
        parked.abort();
    }
    let delivered_after_resume = resolved.ok().map(|joined| {
        let message = joined
            .expect("tokio parked receive task")
            .expect("tokio parked receive");
        text(&message.payload)
    });
    let grants_final = grants(broker);
    client.close().await;
    Observed {
        drained,
        grants_while_paused,
        delivered_after_resume,
        grants_final,
    }
}

async fn run_moonpool(broker: &ScriptedBroker) -> Observed {
    use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
    use moonpool_core::TokioProviders;

    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = Client::connect_plain(&engine, &broker.host_port(), ConnectionConfig::default())
        .await
        .expect("moonpool connect_plain");
    let producer = client
        .open_producer(CreateProducerRequest {
            topic: TOPIC.to_owned(),
            ..Default::default()
        })
        .await
        .expect("moonpool open_producer");
    for index in 0..RQ {
        producer
            .send(outgoing(payload(index)))
            .await
            .expect("moonpool send");
    }
    let consumer = client
        .subscribe(subscribe_request())
        .await
        .expect("moonpool subscribe");
    wait_until_queued(|| consumer.available_in_queue()).await;

    consumer.pause();
    let mut drained = Vec::new();
    for _ in 0..RQ {
        let message = tokio::time::timeout(QUEUED_RECV_TIMEOUT, consumer.receive())
            .await
            .expect("a queued message must be received at once")
            .expect("moonpool receive");
        drained.push(text(&message.payload));
    }
    let grants_while_paused = grants(broker);

    producer
        .send(outgoing(payload(RQ)))
        .await
        .expect("moonpool send behind the drained window");
    // The receive loop runs in its own task, so it is polled again only when its waker fires,
    // exactly like an application's receive loop. Polling it from the resuming task (a `join!`
    // re-polls every branch) would run `pop_message` after the resume and mask the defect.
    let receiving = consumer.clone();
    let mut parked = tokio::spawn(async move { receiving.receive().await });
    tokio::time::sleep(PARK_SETTLE).await;
    consumer.resume();
    let resolved = tokio::time::timeout(RESUMED_RECV_TIMEOUT, &mut parked).await;
    if resolved.is_err() {
        parked.abort();
    }
    let delivered_after_resume = resolved.ok().map(|joined| {
        let message = joined
            .expect("moonpool parked receive task")
            .expect("moonpool parked receive");
        text(&message.payload)
    });
    let grants_final = grants(broker);
    client.close().await;
    Observed {
        drained,
        grants_while_paused,
        delivered_after_resume,
        grants_final,
    }
}

/// Resuming a consumer whose receive loop drained the paused window releases the withheld flow,
/// so the broker dispatches the message queued behind the window to the parked `receive()`, and
/// both engines agree on every step.
#[tokio::test(flavor = "current_thread")]
async fn resume_after_a_drained_pause_releases_the_withheld_flow_event_stream_parity() {
    let tokio_broker = ScriptedBroker::bind().await.expect("broker bind");
    let tokio_observed = run_tokio(&tokio_broker).await;
    tokio_broker.shutdown().await;

    let moonpool_broker = ScriptedBroker::bind().await.expect("broker bind");
    let moonpool_observed = run_moonpool(&moonpool_broker).await;
    moonpool_broker.shutdown().await;

    assert_eq!(
        tokio_observed, moonpool_observed,
        "engine outcomes diverged for resume after a drained pause",
    );
    assert_eq!(
        tokio_observed.drained,
        (0..RQ).map(|index| format!("m{index}")).collect::<Vec<_>>(),
        "the paused consumer must still hand out the whole buffered window: {tokio_observed:?}",
    );
    assert_eq!(
        tokio_observed.grants_while_paused,
        vec![RQ as u32],
        "a paused consumer refills nothing: the broker saw only the initial grant: {tokio_observed:?}",
    );
    assert_eq!(
        tokio_observed.delivered_after_resume,
        Some(format!("m{RQ}")),
        "resume must release the window the pause withheld; the broker holds no permit and no \
         arrival wakes the parked receive, so without the flow it stays parked: {tokio_observed:?}",
    );
    assert_eq!(
        tokio_observed.grants_final,
        vec![RQ as u32, RQ as u32],
        "the resume returns the whole drained window in one grant: {tokio_observed:?}",
    );
}
