// SPDX-License-Identifier: Apache-2.0

//! Last-clone drop guard — tokio engine (issue #342).
//!
//! `Consumer` is cheap-clone; dropping the **last** clone must enqueue a
//! best-effort `CommandCloseConsumer` so the broker releases the
//! `(topic, subscription, consumer_name)` registration. Without it, a consumer
//! dropped without an explicit `close().await` leaks broker-side for as long as
//! the shared TCP connection stays open.
//!
//! Each test pairs with a same-named test on the moonpool side
//! (`crates/magnetar-runtime-moonpool/tests/consumer_drop_close.rs`) so
//! `cargo xtask check-runtime-test-parity` stays balanced 1:1
//! (ADR-0024). Layer (b) of the four-layer test policy.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, FrameError, MessageId, SubscribeRequest, decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::Client;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Shared log of every command type the mock broker received, in order.
type FrameLog = Arc<Mutex<Vec<i32>>>;

/// Mock broker recording every received frame type. Answers the minimal
/// verb set the engine needs: `CONNECT`, `PING`, `LOOKUP`, `SUBSCRIBE`,
/// `FLOW`, `CLOSE_CONSUMER`. Mirrors the `producer_drop_close.rs` broker
/// shape.
async fn spawn_recording_broker() -> (String, FrameLog) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let log: FrameLog = Arc::new(Mutex::new(Vec::new()));
    let log_task = log.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let log_conn = log_task.clone();
            tokio::spawn(async move {
                run_broker_conn(&mut stream, &log_conn).await;
            });
        }
    });
    (format!("pulsar://{addr}"), log)
}

async fn run_broker_conn(stream: &mut tokio::net::TcpStream, log: &FrameLog) {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out_buf = BytesMut::with_capacity(8 * 1024);
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return,
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            log.lock().push(frame.command.r#type);
            answer_frame(&frame, &mut out_buf);
        }
        if !out_buf.is_empty() {
            if stream.write_all(&out_buf).await.is_err() {
                return;
            }
            if stream.flush().await.is_err() {
                return;
            }
            out_buf.clear();
        }
        match stream.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

fn answer_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-consumer-drop".to_owned(),
                    protocol_version: Some(21),
                    max_message_size: Some(5 * 1024 * 1024),
                    feature_flags: Some(pb::FeatureFlags::default()),
                }),
                ..Default::default()
            };
            let _ = encode_command(out, &cmd);
        }
        pb::base_command::Type::Ping => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Pong as i32,
                pong: Some(pb::CommandPong {}),
                ..Default::default()
            };
            let _ = encode_command(out, &cmd);
        }
        pb::base_command::Type::Lookup => {
            if let Some(l) = &frame.command.lookup_topic {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::LookupResponse as i32,
                    lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                        broker_service_url: None,
                        broker_service_url_tls: None,
                        response: Some(
                            pb::command_lookup_topic_response::LookupType::Connect as i32,
                        ),
                        request_id: l.request_id,
                        authoritative: Some(true),
                        error: None,
                        message: None,
                        proxy_through_service_url: Some(false),
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::Subscribe => {
            if let Some(s) = &frame.command.subscribe {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::Success as i32,
                    success: Some(pb::CommandSuccess {
                        request_id: s.request_id,
                        schema: None,
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::CloseConsumer => {
            if let Some(c) = &frame.command.close_consumer {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::Success as i32,
                    success: Some(pb::CommandSuccess {
                        request_id: c.request_id,
                        schema: None,
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        _ => {}
    }
}

fn close_consumer_count(log: &FrameLog) -> usize {
    log.lock()
        .iter()
        .filter(|t| **t == pb::base_command::Type::CloseConsumer as i32)
        .count()
}

/// Poll until the broker has seen `expected` `CloseConsumer` frames, or
/// panic after `deadline`. The drop guard is fire-and-forget so the
/// frame lands asynchronously — bounded polling keeps the test honest
/// without an arbitrary fixed sleep.
async fn wait_close_consumer_count(log: &FrameLog, expected: usize, deadline: Duration) {
    let start = std::time::Instant::now();
    loop {
        if close_consumer_count(log) >= expected {
            return;
        }
        assert!(
            start.elapsed() < deadline,
            "broker saw {} CloseConsumer frame(s), expected {expected} within {deadline:?}",
            close_consumer_count(log),
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Dropping the last clone of a consumer enqueues a best-effort
/// `CloseConsumer` — the broker-side registration is released without an
/// explicit `close().await`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_last_clone_enqueues_close_consumer() {
    let (url, log) = spawn_recording_broker().await;
    let client = Client::connect(&url, ConnectionConfig::default())
        .await
        .expect("connect ok");
    let consumer = client
        .subscribe(SubscribeRequest {
            topic: "persistent://public/default/drop-last-clone".to_owned(),
            subscription: "drop-last-clone".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ack_group_time: Some(Duration::from_mins(1)),
            ..Default::default()
        })
        .await
        .expect("subscribe ok");
    assert_eq!(close_consumer_count(&log), 0, "no close before drop");
    consumer.ack_grouped(MessageId {
        ledger_id: 7,
        entry_id: 11,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
    });

    drop(consumer);

    wait_close_consumer_count(&log, 1, Duration::from_secs(3)).await;
    let barrier = client
        .subscribe(SubscribeRequest {
            topic: "persistent://public/default/drop-last-clone-barrier".to_owned(),
            subscription: "drop-last-clone-barrier".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        })
        .await
        .expect("barrier subscribe ok");
    assert_eq!(
        close_consumer_count(&log),
        1,
        "last-clone drop must enqueue exactly one CloseConsumer"
    );
    {
        let frames = log.lock();
        let ack_position = frames
            .iter()
            .position(|kind| *kind == pb::base_command::Type::Ack as i32)
            .expect("drop close must flush the grouped acknowledgement");
        let close_position = frames
            .iter()
            .position(|kind| *kind == pb::base_command::Type::CloseConsumer as i32)
            .expect("drop close must reach the broker");
        assert!(
            ack_position < close_position,
            "grouped Ack must precede CloseConsumer, frames: {frames:?}"
        );
    }
    barrier.close().await.expect("barrier close ok");
    client.close().await;
}

/// Dropping a non-last clone must NOT close the consumer: the surviving
/// clone stays open and usable; only the final drop releases the
/// broker-side registration (exactly one `CloseConsumer` total).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_of_non_last_clone_keeps_consumer_open() {
    let (url, log) = spawn_recording_broker().await;
    let client = Client::connect(&url, ConnectionConfig::default())
        .await
        .expect("connect ok");
    let consumer = client
        .subscribe(SubscribeRequest {
            topic: "persistent://public/default/drop-non-last-clone".to_owned(),
            subscription: "drop-non-last-clone".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        })
        .await
        .expect("subscribe ok");

    let clone = consumer.clone();
    drop(clone);
    assert!(
        !consumer.is_closed(),
        "dropping a non-last clone must not close the consumer"
    );
    let barrier = client
        .subscribe(SubscribeRequest {
            topic: "persistent://public/default/drop-non-last-clone-barrier".to_owned(),
            subscription: "drop-non-last-clone-barrier".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        })
        .await
        .expect("barrier subscribe ok");
    assert_eq!(
        close_consumer_count(&log),
        0,
        "dropping a non-last clone must not enqueue CloseConsumer"
    );
    consumer.pause();
    assert!(
        consumer.is_paused(),
        "the surviving consumer must remain usable after an intermediate clone drop"
    );
    consumer.resume();
    assert!(
        !consumer.is_paused(),
        "the surviving consumer must resume after an intermediate clone drop"
    );

    drop(consumer);
    wait_close_consumer_count(&log, 1, Duration::from_secs(3)).await;
    assert_eq!(
        close_consumer_count(&log),
        1,
        "exactly one CloseConsumer for the whole clone family"
    );
    barrier.close().await.expect("barrier close ok");
    client.close().await;
}

/// An explicit `close().await` followed by the last-clone drop sends a
/// single `CloseConsumer` — the guard observes the slot's `closed` flag
/// and skips the duplicate. A follow-up subscribe provides the ordering
/// barrier: its round-trip lands after any hypothetical duplicate close
/// on the same connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_close_then_drop_sends_single_close_consumer() {
    let (url, log) = spawn_recording_broker().await;
    let client = Client::connect(&url, ConnectionConfig::default())
        .await
        .expect("connect ok");
    let consumer = client
        .subscribe(SubscribeRequest {
            topic: "persistent://public/default/close-then-drop".to_owned(),
            subscription: "close-then-drop".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        })
        .await
        .expect("subscribe ok");

    let clone = consumer.clone();
    clone.close().await.expect("explicit close ok");
    assert_eq!(close_consumer_count(&log), 1, "explicit close round-trip");

    drop(consumer); // last clone — guard must skip (slot already closed)

    // Ordering barrier: this subscribe's round-trip reaches the broker
    // after any duplicate CloseConsumer the drop could have enqueued.
    let barrier = client
        .subscribe(SubscribeRequest {
            topic: "persistent://public/default/close-then-drop-barrier".to_owned(),
            subscription: "close-then-drop-barrier".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        })
        .await
        .expect("barrier subscribe ok");
    assert_eq!(
        close_consumer_count(&log),
        1,
        "drop after explicit close must not enqueue a duplicate CloseConsumer"
    );
    barrier.close().await.expect("barrier close ok");
    client.close().await;
}
