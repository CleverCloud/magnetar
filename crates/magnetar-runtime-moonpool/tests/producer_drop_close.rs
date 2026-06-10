// SPDX-License-Identifier: Apache-2.0

//! Last-clone drop guard — moonpool engine (issue #241).
//!
//! `Producer` is cheap-clone; dropping the **last** clone must enqueue a
//! best-effort `CommandCloseProducer` so the broker releases the
//! `(topic, producer_name)` registration. Without it, a producer dropped
//! without an explicit `close().await` leaks broker-side for as long as
//! the shared TCP connection stays open, and recreating a same-name
//! producer fails forever with `NamingException` (code 16).
//!
//! Each test pairs with a same-named test on the tokio side
//! (`crates/magnetar-runtime-tokio/tests/producer_drop_close.rs`) so
//! `cargo xtask check-runtime-test-parity` stays balanced 1:1
//! (ADR-0024). Layer (c) of the four-layer test policy.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, decode_one, encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Shared log of every command type the mock broker received, in order.
type FrameLog = Arc<Mutex<Vec<i32>>>;

/// Mock broker recording every received frame type. Answers the minimal
/// verb set the engine needs: `CONNECT`, `PING`, `LOOKUP`, `PRODUCER`,
/// `CLOSE_PRODUCER`. Mirrors the `coverage_close.rs` broker shape.
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
    (addr.to_string(), log)
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
                    server_version: "magnetar-producer-drop".to_owned(),
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
        pb::base_command::Type::Producer => {
            if let Some(p) = &frame.command.producer {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::ProducerSuccess as i32,
                    producer_success: Some(pb::CommandProducerSuccess {
                        request_id: p.request_id,
                        producer_name: "producer-drop".to_owned(),
                        last_sequence_id: Some(-1),
                        schema_version: None,
                        topic_epoch: Some(0),
                        producer_ready: Some(true),
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::CloseProducer => {
            if let Some(c) = &frame.command.close_producer {
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

fn close_producer_count(log: &FrameLog) -> usize {
    log.lock()
        .iter()
        .filter(|t| **t == pb::base_command::Type::CloseProducer as i32)
        .count()
}

/// Poll until the broker has seen `expected` `CloseProducer` frames, or
/// panic after `deadline`. The drop guard is fire-and-forget so the
/// frame lands asynchronously — bounded polling keeps the test honest
/// without an arbitrary fixed sleep.
async fn wait_close_producer_count(log: &FrameLog, expected: usize, deadline: Duration) {
    let start = std::time::Instant::now();
    loop {
        if close_producer_count(log) >= expected {
            return;
        }
        assert!(
            start.elapsed() < deadline,
            "broker saw {} CloseProducer frame(s), expected {expected} within {deadline:?}",
            close_producer_count(log),
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Dropping the last clone of a producer enqueues a best-effort
/// `CloseProducer` — the broker-side registration is released without an
/// explicit `close().await`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_last_clone_enqueues_close_producer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, log) = spawn_recording_broker().await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = Client::connect_plain(&engine, &host_port, ConnectionConfig::default())
                .await
                .expect("connect ok");
            let producer = client
                .open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/drop-last-clone".to_owned(),
                    ..Default::default()
                })
                .await
                .expect("open_producer ok");
            assert_eq!(close_producer_count(&log), 0, "no close before drop");

            drop(producer);

            wait_close_producer_count(&log, 1, Duration::from_secs(3)).await;
            client.close().await;
        })
        .await;
}

/// Dropping a non-last clone must NOT close the producer: the surviving
/// clone stays open and usable; only the final drop releases the
/// broker-side registration (exactly one `CloseProducer` total).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_of_non_last_clone_keeps_producer_open() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, log) = spawn_recording_broker().await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = Client::connect_plain(&engine, &host_port, ConnectionConfig::default())
                .await
                .expect("connect ok");
            let producer = client
                .open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/drop-non-last-clone".to_owned(),
                    ..Default::default()
                })
                .await
                .expect("open_producer ok");

            let clone = producer.clone();
            drop(clone);
            assert!(
                !producer.is_closed(),
                "dropping a non-last clone must not close the producer"
            );

            drop(producer);
            wait_close_producer_count(&log, 1, Duration::from_secs(3)).await;
            assert_eq!(
                close_producer_count(&log),
                1,
                "exactly one CloseProducer for the whole clone family"
            );
            client.close().await;
        })
        .await;
}

/// An explicit `close().await` followed by the last-clone drop sends a
/// single `CloseProducer` — the guard observes the slot's `closed` flag
/// and skips the duplicate. A follow-up producer open provides the
/// ordering barrier: its round-trip lands after any hypothetical
/// duplicate close on the same connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_close_then_drop_sends_single_close_producer() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, log) = spawn_recording_broker().await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = Client::connect_plain(&engine, &host_port, ConnectionConfig::default())
                .await
                .expect("connect ok");
            let producer = client
                .open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/close-then-drop".to_owned(),
                    ..Default::default()
                })
                .await
                .expect("open_producer ok");

            let clone = producer.clone();
            clone.close().await.expect("explicit close ok");
            assert_eq!(close_producer_count(&log), 1, "explicit close round-trip");

            drop(producer); // last clone — guard must skip (slot already closed)

            // Ordering barrier: this open's round-trip reaches the broker
            // after any duplicate CloseProducer the drop could have enqueued.
            let barrier = client
                .open_producer(CreateProducerRequest {
                    topic: "persistent://public/default/close-then-drop-barrier".to_owned(),
                    ..Default::default()
                })
                .await
                .expect("barrier open_producer ok");
            assert_eq!(
                close_producer_count(&log),
                1,
                "drop after explicit close must not enqueue a duplicate CloseProducer"
            );
            barrier.close().await.expect("barrier close ok");
            client.close().await;
        })
        .await;
}
