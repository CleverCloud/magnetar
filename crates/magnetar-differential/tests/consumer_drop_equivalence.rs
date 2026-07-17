// SPDX-License-Identifier: Apache-2.0

//! Last-clone consumer drop guard — tokio ↔ moonpool differential equivalence
//! (issue #342). Layer (d) of the ADR-0024 four-layer test policy.
//!
//! The first `Recv` opens the consumer, `DropConsumer` releases its final clone, and the second
//! `Recv` reopens the consumer on the same subscription. That second `Subscribe` round-trip is a
//! FIFO ordering barrier proving that the earlier best-effort `CloseConsumer` reached the broker.
//! Dropping the replacement's final clone then emits the second close, so each leg must contain
//! exactly two consumer-close frames ordered around the replacement subscribe. A producer `Send`
//! after the second drop supplies the final same-connection FIFO barrier without relying on
//! `Op::Close`.
//!
//! The grouped-ack scenario drives each runtime directly against the same scripted broker.
//! A long grouping window keeps the acknowledgement buffered until final-clone drop, and a second
//! subscribe provides the FIFO barrier needed to assert `Ack < CloseConsumer` wire order.

use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Event, Op, Trace, runner_moonpool, runner_tokio};
use magnetar_proto::{ConnectionConfig, MessageId, SubscribeRequest, pb};
use magnetar_runtime_moonpool::{Client as MoonpoolClient, MoonpoolEngine};
use magnetar_runtime_tokio::Client as TokioClient;
use moonpool_core::TokioProviders;

fn frame_positions(log: &[i32], kind: pb::base_command::Type) -> Vec<usize> {
    log.iter()
        .enumerate()
        .filter_map(|(index, frame)| (*frame == kind as i32).then_some(index))
        .collect()
}

fn grouped_ack_message_id() -> MessageId {
    MessageId {
        ledger_id: 7,
        entry_id: 11,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
    }
}

fn subscribe_request(topic_suffix: &str, ack_group_time: Option<Duration>) -> SubscribeRequest {
    SubscribeRequest {
        topic: format!("persistent://public/default/consumer-drop-{topic_suffix}"),
        subscription: format!("consumer-drop-{topic_suffix}"),
        receiver_queue_size: 16,
        durable: true,
        ack_group_time,
        ..Default::default()
    }
}

async fn run_tokio_grouped_ack_drop(pulsar_url: &str) {
    let client = TokioClient::connect(pulsar_url, ConnectionConfig::default())
        .await
        .expect("tokio connect");
    let consumer = client
        .subscribe(subscribe_request(
            "grouped-ack",
            Some(Duration::from_secs(60)),
        ))
        .await
        .expect("tokio grouped-ack subscribe");
    consumer.ack_grouped(grouped_ack_message_id());
    drop(consumer);

    let barrier = client
        .subscribe(subscribe_request("grouped-ack-barrier", None))
        .await
        .expect("tokio barrier subscribe");
    barrier.close().await.expect("tokio barrier close");
    client.close().await;
}

async fn run_moonpool_grouped_ack_drop(host_port: &str) {
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = MoonpoolClient::connect_plain(&engine, host_port, ConnectionConfig::default())
        .await
        .expect("moonpool connect");
    let consumer = client
        .subscribe(subscribe_request(
            "grouped-ack",
            Some(Duration::from_secs(60)),
        ))
        .await
        .expect("moonpool grouped-ack subscribe");
    consumer.ack_grouped(grouped_ack_message_id());
    drop(consumer);

    let barrier = client
        .subscribe(subscribe_request("grouped-ack-barrier", None))
        .await
        .expect("moonpool barrier subscribe");
    barrier.close().await.expect("moonpool barrier close");
    client.close().await;
}

fn assert_grouped_ack_close_order(engine: &str, frames: &[i32]) {
    let subscribes = frame_positions(frames, pb::base_command::Type::Subscribe);
    let acks = frame_positions(frames, pb::base_command::Type::Ack);
    let closes = frame_positions(frames, pb::base_command::Type::CloseConsumer);
    assert_eq!(subscribes.len(), 2, "{engine} frames: {frames:?}");
    assert_eq!(acks.len(), 1, "{engine} frames: {frames:?}");
    assert_eq!(closes.len(), 2, "{engine} frames: {frames:?}");
    assert!(
        subscribes[0] < acks[0] && acks[0] < closes[0] && closes[0] < subscribes[1],
        "{engine} must order Subscribe < Ack < CloseConsumer < barrier Subscribe, frames: \
         {frames:?}",
    );
}

#[tokio::test(flavor = "current_thread")]
async fn consumer_drop_event_stream_parity_and_ordered_close() {
    let trace = Trace::new(
        "persistent://public/default/consumer-drop-equiv",
        "sub-drop",
        vec![
            Op::Recv {
                timeout: Duration::from_millis(100),
            },
            Op::DropConsumer,
            Op::Recv {
                timeout: Duration::from_millis(100),
            },
            Op::DropConsumer,
            Op::Send {
                payload: b"after-second-drop".to_vec(),
            },
        ],
    );

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_stream = runner_tokio::run(&pulsar_url, &trace)
        .await
        .expect("tokio runner");
    let tokio_frames = broker.frame_log_snapshot();
    broker.clear_frame_log();

    let moonpool_stream = runner_moonpool::run(&host_port, &trace)
        .await
        .expect("moonpool runner");
    let moonpool_frames = broker.frame_log_snapshot();

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the consumer-drop sequence",
    );
    assert!(
        matches!(tokio_stream.events[1], Event::ConsumerDropped),
        "op 1 must resolve to ConsumerDropped, got {:?}",
        tokio_stream.events[1],
    );
    assert!(
        matches!(tokio_stream.events[3], Event::ConsumerDropped),
        "op 3 must drop the replacement consumer's final clone, got {:?}",
        tokio_stream.events[3],
    );
    assert!(
        matches!(tokio_stream.events[4], Event::Sent { .. }),
        "op 4 must acknowledge the post-second-drop FIFO barrier send, got {:?}",
        tokio_stream.events[4],
    );

    for (engine, frames) in [
        ("tokio", tokio_frames.as_slice()),
        ("moonpool", moonpool_frames.as_slice()),
    ] {
        let subscribes = frame_positions(frames, pb::base_command::Type::Subscribe);
        let closes = frame_positions(frames, pb::base_command::Type::CloseConsumer);
        let sends = frame_positions(frames, pb::base_command::Type::Send);
        assert_eq!(
            subscribes.len(),
            2,
            "{engine} leg must open the original and replacement consumers, frames: {frames:?}",
        );
        assert_eq!(
            closes.len(),
            2,
            "{engine} leg must emit one close for each final-clone drop, frames: {frames:?}",
        );
        assert_eq!(
            sends.len(),
            1,
            "{engine} leg must emit one post-second-drop barrier send, frames: {frames:?}",
        );
        assert!(
            subscribes[0] < closes[0]
                && closes[0] < subscribes[1]
                && subscribes[1] < closes[1]
                && closes[1] < sends[0],
            "{engine} leg must order original Subscribe < first drop CloseConsumer < replacement \
             Subscribe < second drop CloseConsumer < barrier Send, frames: {frames:?}",
        );
    }

    assert_eq!(
        tokio_frames, moonpool_frames,
        "engine frame sequences diverged for the consumer-drop trace",
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn consumer_drop_flushes_grouped_ack_before_close_on_both_engines() {
    let broker = ScriptedBroker::bind().await.expect("broker bind");

    run_tokio_grouped_ack_drop(&broker.pulsar_url()).await;
    let tokio_frames = broker.frame_log_snapshot();
    broker.clear_frame_log();

    run_moonpool_grouped_ack_drop(&broker.host_port()).await;
    let moonpool_frames = broker.frame_log_snapshot();

    assert_grouped_ack_close_order("tokio", &tokio_frames);
    assert_grouped_ack_close_order("moonpool", &moonpool_frames);
    assert_eq!(
        tokio_frames, moonpool_frames,
        "grouped-ack consumer-drop frame sequences diverged",
    );
    broker.shutdown().await;
}
