// SPDX-License-Identifier: Apache-2.0

//! Integration test mirror of
//! `magnetar-runtime-moonpool/tests/partitioned_metadata_fast_path.rs`.
//!
//! F11 fast-path verification at the tokio engine layer: when the
//! caller asks for partitioned-topic metadata on a topic name that
//! already encodes a partition index (`<base>-partition-<N>` per Java
//! `TopicName#isPartitioned`), the runtime MUST short-circuit to
//! `partitions = 0` synthetically — no `CommandPartitionedTopicMetadata`
//! frame is ever emitted to the broker. Mirrors streamnative-pulsar-rs
//! #327 and cuts the per-partition LOOKUP amplification observed on
//! partitioned-consumer fan-out from `N+1` round-trips to `1`.
//!
//! Strategy: stand up a tiny in-process TCP broker stub that records
//! the order of every `BaseCommand` it sees, drive the tokio engine
//! through `partitioned_topic_metadata` with a `-partition-0` topic,
//! then assert the recorded sequence contains NO
//! `PartitionedMetadata` frame. As a control, the same broker run
//! against a regular (non-partition) topic name MUST observe one
//! `PartitionedMetadata` frame.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, FrameError, decode_one, encode_command, pb};
use magnetar_runtime_tokio::Client;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordedKind(i32);

impl RecordedKind {
    fn kind(self) -> pb::base_command::Type {
        pb::base_command::Type::try_from(self.0).expect("known kind")
    }
}

async fn spawn_recording_broker() -> (String, Arc<Mutex<Vec<RecordedKind>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let log: Arc<Mutex<Vec<RecordedKind>>> = Arc::new(Mutex::new(Vec::new()));
    let log_clone = log.clone();
    tokio::spawn(async move {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
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
                log_clone.lock().push(RecordedKind(frame.command.r#type));
                handle_frame(&frame, &mut out_buf);
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
    });
    (format!("pulsar://{addr}"), log)
}

fn handle_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-fast-path-test".to_owned(),
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
        pb::base_command::Type::PartitionedMetadata => {
            // For the control (non-partition-suffix) path: reply with
            // partitions = 0 so the engine resolves the future.
            if let Some(p) = &frame.command.partition_metadata {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::PartitionedMetadataResponse as i32,
                    partition_metadata_response: Some(
                        pb::CommandPartitionedTopicMetadataResponse {
                            partitions: Some(0),
                            request_id: p.request_id,
                            response: Some(
                                pb::command_partitioned_topic_metadata_response::LookupType::Success
                                    as i32,
                            ),
                            error: None,
                            message: None,
                        },
                    ),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        _ => {}
    }
}

/// The F11 fast-path: calling `partitioned_topic_metadata` on a topic
/// whose name already matches `-partition-N` returns `Ok(0)`
/// immediately, and the broker MUST NOT see any
/// `CommandPartitionedTopicMetadata` frame.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partitioned_topic_metadata_short_circuits_on_partition_suffix() {
    let (url, log) = spawn_recording_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let count = tokio::time::timeout(
        Duration::from_secs(2),
        client.partitioned_topic_metadata("persistent://public/default/foo-partition-0"),
    )
    .await
    .expect("fast-path resolved without timing out")
    .expect("fast-path returns Ok");
    assert_eq!(count, 0, "fast-path always reports 0 partitions");

    // Give the recording broker a moment to flush any in-flight reads
    // (there shouldn't be any) then assert the wire history.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let seen = log.lock().clone();
    let kinds: Vec<pb::base_command::Type> = seen.iter().map(|r| r.kind()).collect();
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    assert!(
        !kinds.contains(&pb::base_command::Type::PartitionedMetadata),
        "broker must see ZERO PartitionedMetadata frames for a -partition-N topic, got {kinds:?}",
    );
}

/// Control: a non-partition topic name still issues a
/// `PartitionedMetadata` frame to the broker (i.e. the fast-path is
/// scoped and doesn't accidentally swallow regular metadata lookups).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partitioned_topic_metadata_still_emits_frame_for_non_partition_topic() {
    let (url, log) = spawn_recording_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let count = tokio::time::timeout(
        Duration::from_secs(2),
        client.partitioned_topic_metadata("persistent://public/default/orders"),
    )
    .await
    .expect("metadata resolved without timing out")
    .expect("metadata Ok from scripted broker");
    assert_eq!(count, 0, "scripted broker replies with 0 partitions");

    tokio::time::sleep(Duration::from_millis(50)).await;
    let seen = log.lock().clone();
    let kinds: Vec<pb::base_command::Type> = seen.iter().map(|r| r.kind()).collect();
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    assert!(
        kinds.contains(&pb::base_command::Type::PartitionedMetadata),
        "broker MUST see one PartitionedMetadata frame for a non-partition topic, got {kinds:?}",
    );
}
