// SPDX-License-Identifier: Apache-2.0

//! PIP-33 marker-accessor lost-wakeup regression — moonpool
//! engine twin of `crates/magnetar-runtime-tokio/tests/marker_lost_wakeup.rs`
//! (ADR-0024 cross-runtime test parity, 1:1 test names).
//!
//! `Client::next_replicated_subscription_marker` used to drain the buffer and
//! re-check `is_closed()` *before* enrolling on
//! `replicated_subscription_marker_notify`. The driver fires `notify_waiters()`
//! — which stores **no permit** — so a marker delivered between the accessor's
//! drain and its (too-late) enrollment was lost and the accessor hung. The fix
//! arms the `Notified` *before* the drain (the enroll-before-drain idiom shared
//! with `ConnectionShared::await_reconnect_or_terminal` and the
//! `SubscribeAckedFut` fix), mirrored 1:1 across both engines.
//!
//! The deterministic `SimProviders` coverage of the marker-observation path
//! lives in `replicated_subscriptions_sim.rs`; this file carries the
//! primitive-level mechanism statement plus the live-path positive twin.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Wake};
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, FrameError, ReplicatedSubscriptionMarkerKind, SubscribeRequest, decode_one,
    encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

/// Counting waker — records wake calls so a manual poll loop can assert a
/// `notify_waiters()` reached the parked future.
struct CountingWaker {
    woken: std::sync::atomic::AtomicUsize,
}

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// The primitive-level statement of the marker-lost-wakeup bug and its fix (engine-agnostic).
///
/// `Notify::notify_waiters()` stores no permit: it only wakes waiters already
/// enrolled at the instant it fires. A `Notified` constructed *after* a racing
/// `notify_waiters()` (the buggy drain-then-enroll shape) misses the wakeup and
/// parks forever; one armed (via `enable()`) *before* the notify (the
/// enroll-before-drain fix) captures it and resolves on the next poll.
#[test]
fn enroll_before_drain_catches_notify_that_drain_then_enroll_loses() {
    let waker = Arc::new(CountingWaker {
        woken: std::sync::atomic::AtomicUsize::new(0),
    });
    let std_waker = waker.clone().into();
    let mut cx = Context::from_waker(&std_waker);

    // BUGGY ordering: notify fires first, Notified constructed after -> lost.
    {
        let notify = Notify::new();
        notify.notify_waiters();
        let fut = pin!(notify.notified());
        assert!(
            fut.poll(&mut cx).is_pending(),
            "drain-then-enroll loses the wakeup: the Notified must park (the marker-lost-wakeup hang)"
        );
    }

    // FIXED ordering: Notified armed before the notify -> caught.
    {
        let notify = Notify::new();
        let mut fut = pin!(notify.notified());
        fut.as_mut().enable();
        notify.notify_waiters();
        assert!(
            fut.as_mut().poll(&mut cx).is_ready(),
            "enroll-before-drain catches the wakeup: the armed Notified must resolve"
        );
    }
}

/// Minimal broker: CONNECT -> CONNECTED, LOOKUP -> Connect, SUBSCRIBE ->
/// Success, then on the first FLOW push one regular message followed by a
/// `REPLICATED_SUBSCRIPTION_SNAPSHOT` marker.
async fn serve_marker_broker(mut stream: TcpStream) {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out = BytesMut::with_capacity(64 * 1024);
    let mut consumer_id = 0u64;
    let mut delivered = false;
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
            let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
                continue;
            };
            match kind {
                pb::base_command::Type::Connect => {
                    let cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::Connected as i32,
                        connected: Some(pb::CommandConnected {
                            server_version: "marker-broker".to_owned(),
                            protocol_version: Some(21),
                            max_message_size: Some(5 * 1024 * 1024),
                            feature_flags: Some(pb::FeatureFlags::default()),
                        }),
                        ..Default::default()
                    };
                    let _ = encode_command(&mut out, &cmd);
                }
                pb::base_command::Type::Ping => {
                    let cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::Pong as i32,
                        pong: Some(pb::CommandPong {}),
                        ..Default::default()
                    };
                    let _ = encode_command(&mut out, &cmd);
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
                        let _ = encode_command(&mut out, &cmd);
                    }
                }
                pb::base_command::Type::Subscribe => {
                    if let Some(s) = &frame.command.subscribe {
                        consumer_id = s.consumer_id;
                        let cmd = pb::BaseCommand {
                            r#type: pb::base_command::Type::Success as i32,
                            success: Some(pb::CommandSuccess {
                                request_id: s.request_id,
                                schema: None,
                            }),
                            ..Default::default()
                        };
                        let _ = encode_command(&mut out, &cmd);
                    }
                }
                pb::base_command::Type::Flow if !delivered => {
                    delivered = true;
                    let msg = pb::BaseCommand {
                        r#type: pb::base_command::Type::Message as i32,
                        message: Some(pb::CommandMessage {
                            consumer_id,
                            message_id: pb::MessageIdData {
                                ledger_id: 1,
                                entry_id: 1,
                                partition: None,
                                batch_index: None,
                                ack_set: Vec::new(),
                                batch_size: None,
                                first_chunk_message_id: None,
                            },
                            redelivery_count: Some(0),
                            ack_set: Vec::new(),
                            consumer_epoch: None,
                        }),
                        ..Default::default()
                    };
                    let regular_meta = pb::MessageMetadata {
                        producer_name: "regular".to_owned(),
                        sequence_id: 1,
                        publish_time: 0,
                        num_messages_in_batch: Some(1),
                        ..Default::default()
                    };
                    let _ = encode_payload(&mut out, &msg, &regular_meta, b"user-payload");

                    let marker_cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::Message as i32,
                        message: Some(pb::CommandMessage {
                            consumer_id,
                            message_id: pb::MessageIdData {
                                ledger_id: 1,
                                entry_id: 2,
                                partition: None,
                                batch_index: None,
                                ack_set: Vec::new(),
                                batch_size: None,
                                first_chunk_message_id: None,
                            },
                            redelivery_count: Some(0),
                            ack_set: Vec::new(),
                            consumer_epoch: None,
                        }),
                        ..Default::default()
                    };
                    let snapshot = pb::ReplicatedSubscriptionsSnapshot {
                        snapshot_id: "snap".to_owned(),
                        local_message_id: Some(pb::MarkersMessageIdData {
                            ledger_id: 1,
                            entry_id: 2,
                        }),
                        clusters: vec![pb::ClusterMessageId {
                            cluster: "cluster-b".to_owned(),
                            message_id: pb::MarkersMessageIdData {
                                ledger_id: 1,
                                entry_id: 2,
                            },
                        }],
                    };
                    let mut payload = Vec::new();
                    prost::Message::encode(&snapshot, &mut payload).expect("encode snapshot");
                    let marker_meta = pb::MessageMetadata {
                        producer_name: "broker-marker".to_owned(),
                        sequence_id: 0,
                        publish_time: 0,
                        marker_type: Some(ReplicatedSubscriptionMarkerKind::Snapshot.marker_type()),
                        ..Default::default()
                    };
                    let _ = encode_payload(&mut out, &marker_cmd, &marker_meta, &payload);
                }
                _ => {}
            }
        }
        if !out.is_empty() {
            if stream.write_all(&out).await.is_err() {
                return;
            }
            let _ = stream.flush().await;
            out.clear();
        }
        if matches!(stream.read_buf(&mut read_buf).await, Ok(0) | Err(_)) {
            return;
        }
    }
}

/// Live-path positive twin: the production accessor observes a broker-streamed
/// snapshot marker after the consumer has parked. Mirrors the tokio twin and
/// the moonpool `SimProviders` deterministic harness.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn positive_end_to_end_marker_observation() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
            let addr = listener.local_addr().expect("local_addr").to_string();
            tokio::spawn(async move {
                if let Ok((stream, _peer)) = listener.accept().await {
                    serve_marker_broker(stream).await;
                }
            });

            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                Duration::from_secs(5),
                Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");

            let consumer = tokio::time::timeout(
                Duration::from_secs(5),
                client.subscribe(SubscribeRequest {
                    topic: "persistent://public/default/marker-live".to_owned(),
                    subscription: "marker-live-sub".to_owned(),
                    receiver_queue_size: 32,
                    durable: true,
                    replicate_subscription_state: Some(true),
                    ..Default::default()
                }),
            )
            .await
            .expect("subscribe did not time out")
            .expect("subscribe ok");

            let msg = tokio::time::timeout(Duration::from_secs(5), consumer.receive())
                .await
                .expect("receive did not time out")
                .expect("receive ok");
            assert_eq!(msg.payload.as_ref(), b"user-payload");

            let observed = tokio::time::timeout(
                Duration::from_secs(10),
                client.next_replicated_subscription_marker(),
            )
            .await
            .expect("marker accessor did not time out (lost wakeup would hang here)")
            .expect("connection still open");
            assert_eq!(
                observed.marker.kind,
                ReplicatedSubscriptionMarkerKind::Snapshot
            );

            client.close().await;
        })
        .await;
}
