// SPDX-License-Identifier: Apache-2.0

//! PIP-33 marker-accessor lost-wakeup regression.
//!
//! `Client::next_replicated_subscription_marker` used to drain the buffer and
//! re-check `is_closed()` *before* enrolling on
//! `replicated_subscription_marker_notify`. The driver pushes an observation
//! and then fires `notify_waiters()` — which stores **no permit** — so a marker
//! delivered in the window between the accessor's drain and its (too-late)
//! enrollment was lost and the accessor hung forever. The fix arms the
//! `Notified` future *before* the drain (the enroll-before-drain idiom shared
//! with `ConnectionShared::await_reconnect_or_terminal` and the
//! `SubscribeAckedFut` parking-bug fix), mirrored 1:1 across both engines.
//!
//! Two layers of coverage:
//!
//! 1. [`enroll_before_drain_catches_notify_that_drain_then_enroll_loses`] pins the exact
//!    `tokio::sync::Notify` mechanism the fix relies on — a `Notified` armed (via `enable()`)
//!    *before* a `notify_waiters()` resolves on the next poll, whereas one constructed *after* the
//!    same `notify_waiters()` parks forever. This is deterministic (single `Notify`, no scheduler)
//!    and is the primitive-level statement of the bug.
//! 2. [`positive_end_to_end_marker_observation`] drives the **production accessor** against a real
//!    in-process broker that streams a marker, and asserts it is observed — the live-path twin of
//!    the moonpool `replicated_subscriptions_sim.rs` `SimProviders` deterministic harness.
//!
//! ADR-0024: this is the tokio integration twin; the moonpool twin is
//! `marker_lost_wakeup.rs` (1:1, `check-runtime-test-parity`).

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Wake};

use magnetar_proto::{ConnectionConfig, ReplicatedSubscriptionMarkerKind, encode_command, pb};
use magnetar_runtime_tokio::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
mod common;
use common::HANG_GUARD;

/// Counting waker — records how many times it was woken so a manual poll loop
/// can assert a `notify_waiters()` actually reached the parked future.
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

/// The primitive-level statement of the marker-lost-wakeup bug and its fix.
///
/// `Notify::notify_waiters()` stores no permit: it only wakes waiters already
/// enrolled at the instant it fires. The buggy accessor created its `Notified`
/// *after* the drain — i.e. potentially after a racing `notify_waiters()` —
/// and so could miss the wakeup and park forever. The fix arms (and
/// `enable()`s) the `Notified` *before* the drain, so a `notify_waiters()`
/// that fires in the window is captured and the next poll completes.
#[test]
fn enroll_before_drain_catches_notify_that_drain_then_enroll_loses() {
    let waker = Arc::new(CountingWaker {
        woken: std::sync::atomic::AtomicUsize::new(0),
    });
    let std_waker = waker.clone().into();
    let mut cx = Context::from_waker(&std_waker);

    // BUGGY ordering: the marker push + notify_waiters() happen first; only
    // *then* is the Notified constructed (the drain-then-enroll shape). The
    // wakeup is lost — the freshly-created Notified parks and never resolves.
    {
        let notify = Notify::new();
        notify.notify_waiters(); // no waiter enrolled yet -> no permit stored
        let fut = pin!(notify.notified()); // enroll happens AFTER the notify
        assert!(
            fut.poll(&mut cx).is_pending(),
            "drain-then-enroll loses the wakeup: the Notified must park (the marker-lost-wakeup hang)"
        );
    }

    // FIXED ordering: the Notified is armed (enable) BEFORE the notify fires,
    // so the wakeup is captured and the next poll resolves.
    {
        let notify = Notify::new();
        let mut fut = pin!(notify.notified());
        fut.as_mut().enable(); // arm BEFORE the notify (enroll-before-drain)
        notify.notify_waiters();
        assert!(
            fut.as_mut().poll(&mut cx).is_ready(),
            "enroll-before-drain catches the wakeup: the armed Notified must resolve"
        );
    }
}

/// Minimal broker: CONNECT -> CONNECTED, LOOKUP -> Connect, SUBSCRIBE ->
/// Success, then on the first FLOW push one regular message followed by a
/// `REPLICATED_SUBSCRIPTION_SNAPSHOT` marker. Mirrors the moonpool
/// `replicated_subscriptions.rs` scripted broker.
async fn serve_marker_broker(mut stream: TcpStream) {
    use bytes::BytesMut;
    use magnetar_proto::{FrameError, decode_one, encode_payload};

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
                    // One regular message, then the snapshot marker.
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

/// Live-path twin of the moonpool `SimProviders` harness: the production accessor
/// observes a broker-streamed snapshot marker after the consumer has parked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn positive_end_to_end_marker_observation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
    let port = listener.local_addr().expect("local_addr").port();
    let broker = tokio::spawn(async move {
        if let Ok((stream, _peer)) = listener.accept().await {
            serve_marker_broker(stream).await;
        }
    });

    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("client connect");
    let client = Client::from_socket(stream, ConnectionConfig::default())
        .await
        .expect("handshake");

    let consumer = tokio::time::timeout(
        HANG_GUARD,
        client.subscribe(magnetar_proto::SubscribeRequest {
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

    // Drain the one regular message so the consumer parks; the marker is
    // filtered off the receive stream and surfaces only via the accessor.
    let msg = tokio::time::timeout(HANG_GUARD, consumer.receive())
        .await
        .expect("receive did not time out")
        .expect("receive ok");
    assert_eq!(msg.payload.as_ref(), b"user-payload");

    let observed = tokio::time::timeout(HANG_GUARD, client.next_replicated_subscription_marker())
        .await
        .expect("marker accessor did not time out (lost wakeup would hang here)")
        .expect("connection still open");
    assert_eq!(
        observed.marker.kind,
        ReplicatedSubscriptionMarkerKind::Snapshot
    );

    client.close().await;
    broker.abort();
}
