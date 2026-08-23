// SPDX-License-Identifier: Apache-2.0

//! Consumer flow-replenishment starvation under sustained inbound load
//! (issue #307 production-symptom reproduction).
//!
//! ## Production symptom (ground truth)
//!
//! A `Failover` consumer on a backlogged topic consumes a burst right after
//! subscribe (~`receiver_queue_size` messages), then **stops receiving
//! entirely** and sits at `availablePermits = 0`, `msgRateOut = 0` against a
//! multi-million-message backlog — with NO `CommandCloseConsumer`, NO reconnect,
//! NO `ActiveConsumerChange` in between. It never recovers on its own. So after
//! the initial flow grant is drained, **flow is not being replenished** and the
//! broker stops dispatching.
//!
//! ## What this test does (UNLIKE the sans-io `consumer_flow_control_edge.rs`)
//!
//! `consumer_flow_control_edge.rs` drives the sans-io `ConnectionShared` seam
//! directly: it calls `pop_message` and then `poll_transmit` *by hand*, so the
//! `CommandFlow` queued by `maybe_flow` is always observed on the wire. That
//! proves the sans-io accounting is correct in isolation — but it bypasses the
//! real `Consumer::receive()` future and the real driver task the production
//! converter runs.
//!
//! This test models a **real TCP loopback broker** (after
//! `marker_lost_wakeup.rs::serve_marker_broker`) that **strictly enforces flow
//! control**: it tracks a per-consumer permit budget, dispatches a backlog
//! message ONLY when `permits > 0`, and decrements on every dispatch. It holds a
//! backlog of `10 * receiver_queue_size` entries — far more than one window — so
//! draining it requires MULTIPLE replenishment rounds. The client opens a real
//! `Failover` consumer via `Client::subscribe` and drains it with the real
//! `receive()` loop.
//!
//! ## Why a LONG keepalive
//!
//! Three things can wake the tokio driver task and flush a queued `CommandFlow`:
//! inbound bytes, a user future pulsing `driver_waker.notify_one()` (ack / flow /
//! …), and the keepalive timer. The default 30 s keepalive would eventually
//! flush a stranded flow and *un-wedge* the consumer — masking the bug behind a
//! ~30 s stall. We pin a 1 h keepalive so the timer cannot rescue the consumer
//! within the test's short per-receive guard: a stall here is a true wedge, not
//! a transient stutter. (In production, the consumer stalls for tens of seconds
//! per window and looks "stuck" — the same mechanism.)

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, FrameError, decode_one, encode_command, encode_payload, pb,
};
use magnetar_runtime_tokio::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const RECEIVER_QUEUE_SIZE: usize = 16;
const BACKLOG: u64 = (RECEIVER_QUEUE_SIZE as u64) * 10;

/// Short per-`receive()` guard. Well below the pinned 1 h keepalive so the
/// keepalive timer cannot mask a wedge: if a `receive()` does not resolve within
/// this window after the broker has dispatched its window, the consumer is
/// genuinely starved.
const RECV_GUARD: Duration = Duration::from_secs(10);

/// How long the issue #426 regression lets the socket settle once the FIRST
/// initial-grant `CommandFlow` has landed, before it reads the broker's permit
/// total. A redundant second grant is encoded into the same connection buffer
/// under the same lock and rides the same driver flush, so it is already on the
/// wire when the first one becomes observable — the window only removes any
/// dependence on how the kernel happened to segment that one write.
const GRANT_SETTLE: Duration = Duration::from_millis(250);

/// Connection config with a keepalive far longer than the test runtime, so the
/// keepalive timer never wakes the driver to flush a stranded flow (see module
/// docs).
fn long_keepalive_config() -> ConnectionConfig {
    ConnectionConfig {
        keepalive_interval: Duration::from_hours(1),
        ..Default::default()
    }
}

/// Shared counters the broker updates so the test can assert what actually went
/// over the wire.
#[derive(Default)]
struct BrokerStats {
    /// Total permits granted across every `CommandFlow` (initial + replenish).
    flow_permits_granted: AtomicU64,
    /// Total `CommandMessage` frames dispatched.
    messages_dispatched: AtomicU64,
}

/// A flow-control-strict mock broker for a single (non-partitioned) Failover
/// consumer.
///
/// - CONNECT  -> CONNECTED
/// - PING     -> PONG
/// - LOOKUP   -> Connect (serve here)
/// - SUBSCRIBE-> Success, then (when `announce_active`) `ActiveConsumerChange{is_active:true}`
/// - FLOW     -> add `message_permits` to the budget, then dispatch backlog entries one-per-permit
///   until the budget hits zero or the backlog drains.
/// - ACK      -> (when `ack_response`) reply `CommandAckResponse` echoing the request id, so the
///   client's awaited `ack()` resolves.
///
/// The broker NEVER dispatches without a positive permit budget — so if the
/// client stops sending `CommandFlow`, the broker goes quiet and the consumer
/// wedges.
///
/// `announce_active` is off in the issue #426 grant-count regression and on in the issue
/// #427 one. The issue #307 promotion re-arm (`Connection`'s `ActiveConsumerChange` arm) is
/// a second, independent grant site, so counting permits with it in play would have
/// measured two bugs at once while #426 was still open; with #426 fixed, turning it on is
/// exactly how #427's own double grant becomes visible.
async fn serve_failover_flow_strict_broker(
    mut stream: TcpStream,
    backlog: u64,
    ack_response: bool,
    announce_active: bool,
    stats: Arc<BrokerStats>,
) {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out = BytesMut::with_capacity(64 * 1024);
    let mut consumer_id = 0u64;
    let mut subscribed = false;
    let mut permits: u64 = 0;
    let mut next_entry: u64 = 0;

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
                            server_version: "flow-strict-broker".to_owned(),
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
                        subscribed = true;
                        let success = pb::BaseCommand {
                            r#type: pb::base_command::Type::Success as i32,
                            success: Some(pb::CommandSuccess {
                                request_id: s.request_id,
                                schema: None,
                            }),
                            ..Default::default()
                        };
                        let _ = encode_command(&mut out, &success);
                        // Failover: tell the client it is the ACTIVE consumer.
                        if announce_active {
                            let active = pb::BaseCommand {
                                r#type: pb::base_command::Type::ActiveConsumerChange as i32,
                                active_consumer_change: Some(pb::CommandActiveConsumerChange {
                                    consumer_id,
                                    is_active: Some(true),
                                }),
                                ..Default::default()
                            };
                            let _ = encode_command(&mut out, &active);
                        }
                    }
                }
                pb::base_command::Type::Flow => {
                    if let Some(f) = &frame.command.flow {
                        permits = permits.saturating_add(u64::from(f.message_permits));
                        stats
                            .flow_permits_granted
                            .fetch_add(u64::from(f.message_permits), Ordering::SeqCst);
                    }
                }
                pb::base_command::Type::Ack => {
                    if ack_response {
                        if let Some(a) = &frame.command.ack {
                            let resp = pb::BaseCommand {
                                r#type: pb::base_command::Type::AckResponse as i32,
                                ack_response: Some(pb::CommandAckResponse {
                                    consumer_id: a.consumer_id,
                                    request_id: a.request_id,
                                    txnid_least_bits: None,
                                    txnid_most_bits: None,
                                    error: None,
                                    message: None,
                                }),
                                ..Default::default()
                            };
                            let _ = encode_command(&mut out, &resp);
                        }
                    }
                }
                pb::base_command::Type::CloseConsumer => {
                    return;
                }
                _ => {}
            }
        }

        // Dispatch backlog entries, one permit each, while we have budget and
        // backlog. The broker dispatches ONLY against a positive permit budget —
        // this is the load-bearing flow-control enforcement.
        if subscribed {
            while permits > 0 && next_entry < backlog {
                let entry = next_entry;
                next_entry += 1;
                permits -= 1;
                stats.messages_dispatched.fetch_add(1, Ordering::SeqCst);

                let msg = pb::BaseCommand {
                    r#type: pb::base_command::Type::Message as i32,
                    message: Some(pb::CommandMessage {
                        consumer_id,
                        message_id: pb::MessageIdData {
                            ledger_id: 1,
                            entry_id: entry,
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
                let meta = pb::MessageMetadata {
                    producer_name: "backlog".to_owned(),
                    sequence_id: entry,
                    publish_time: 0,
                    num_messages_in_batch: Some(1),
                    ..Default::default()
                };
                let _ = encode_payload(&mut out, &msg, &meta, format!("entry-{entry}").as_bytes());
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

/// REPRODUCTION (no-ack variant — the cleanest isolation of the flow path).
///
/// The consumer drains a backlog `10×` larger than one receiver-queue window and
/// **never acks**. Replenishment is therefore driven *purely* by
/// `receive()` -> `pop_message` -> `maybe_flow`, with nothing else (no `ack()`,
/// no `flow()`, no short keepalive) waking the driver to flush the queued
/// `CommandFlow`. If `receive()`'s success path does not wake the driver, the
/// queued replenishment frames sit unflushed; the broker exhausts its permits
/// after the first window, goes quiet (no inbound bytes to re-wake the driver),
/// and the consumer wedges forever — the production symptom.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_no_ack_drains_full_backlog_without_wedge() {
    let stats = Arc::new(BrokerStats::default());
    let broker_stats = stats.clone();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
    let port = listener.local_addr().expect("local_addr").port();
    let broker = tokio::spawn(async move {
        if let Ok((stream, _peer)) = listener.accept().await {
            serve_failover_flow_strict_broker(stream, BACKLOG, false, true, broker_stats).await;
        }
    });

    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("client connect");
    let client = Client::from_socket(stream, long_keepalive_config())
        .await
        .expect("handshake");

    let consumer = tokio::time::timeout(
        RECV_GUARD,
        client.subscribe(magnetar_proto::SubscribeRequest {
            topic: "persistent://public/default/failover-backlog-noack".to_owned(),
            subscription: "failover-backlog-noack-sub".to_owned(),
            sub_type: pb::command_subscribe::SubType::Failover,
            receiver_queue_size: RECEIVER_QUEUE_SIZE,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");

    let mut received: u64 = 0;
    while received < BACKLOG {
        let _msg = tokio::time::timeout(RECV_GUARD, consumer.receive())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "receive() WEDGED after {received}/{BACKLOG} messages (no-ack drain): \
                     broker granted {} permits, dispatched {} messages — \
                     replenishment CommandFlow from receive()->maybe_flow was never flushed \
                     (the driver was not woken after pop_message)",
                    stats.flow_permits_granted.load(Ordering::SeqCst),
                    stats.messages_dispatched.load(Ordering::SeqCst),
                )
            })
            .expect("receive ok");
        received += 1;
    }

    assert_eq!(
        received, BACKLOG,
        "the Failover consumer must drain the entire backlog via receive() alone",
    );
    assert_eq!(
        stats.messages_dispatched.load(Ordering::SeqCst),
        BACKLOG,
        "the broker must have dispatched the entire backlog",
    );

    client.close().await;
    broker.abort();
}

/// REPRODUCTION (realistic variant — the production converter acks every
/// message). Same backlog + long keepalive, but the consumer acks each message
/// and awaits the broker's `CommandAckResponse`. Acks pulse the driver waker, so
/// this path may flush the replenishment flow as a side effect even if
/// `receive()` itself does not — exercising the converter's actual code path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_with_ack_drains_full_backlog_without_wedge() {
    let stats = Arc::new(BrokerStats::default());
    let broker_stats = stats.clone();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
    let port = listener.local_addr().expect("local_addr").port();
    let broker = tokio::spawn(async move {
        if let Ok((stream, _peer)) = listener.accept().await {
            serve_failover_flow_strict_broker(stream, BACKLOG, true, true, broker_stats).await;
        }
    });

    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("client connect");
    let client = Client::from_socket(stream, long_keepalive_config())
        .await
        .expect("handshake");

    let consumer = tokio::time::timeout(
        RECV_GUARD,
        client.subscribe(magnetar_proto::SubscribeRequest {
            topic: "persistent://public/default/failover-backlog-ack".to_owned(),
            subscription: "failover-backlog-ack-sub".to_owned(),
            sub_type: pb::command_subscribe::SubType::Failover,
            receiver_queue_size: RECEIVER_QUEUE_SIZE,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");

    let mut received: u64 = 0;
    while received < BACKLOG {
        let msg = tokio::time::timeout(RECV_GUARD, consumer.receive())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "receive() WEDGED after {received}/{BACKLOG} messages (ack drain): \
                     broker granted {} permits, dispatched {} messages",
                    stats.flow_permits_granted.load(Ordering::SeqCst),
                    stats.messages_dispatched.load(Ordering::SeqCst),
                )
            })
            .expect("receive ok");
        tokio::time::timeout(RECV_GUARD, consumer.ack(msg.message_id))
            .await
            .expect("ack did not time out")
            .expect("ack ok");
        received += 1;
    }

    assert_eq!(
        received, BACKLOG,
        "the Failover consumer must drain the entire backlog",
    );

    client.close().await;
    broker.abort();
}

/// Block until the broker has counted at least one `CommandFlow`, or panic at
/// `RECV_GUARD`. The initial grant is queued while `subscribe()` is resolving
/// and flushed by the driver task afterwards, so "subscribe resolved" is not
/// yet "the broker has seen the grant".
async fn wait_for_first_grant(stats: &BrokerStats) {
    let deadline = tokio::time::Instant::now() + RECV_GUARD;
    while stats.flow_permits_granted.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the broker never observed the initial CommandFlow within {RECV_GUARD:?}",
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// REGRESSION (issue #426 — the initial grant is issued EXACTLY once).
///
/// Both fresh-subscribe paths used to grant the initial permits TWICE: the
/// sans-io `Connection::initial_flow` emitted the `CommandFlow` and updated the
/// client-side mirrors, and a raw `Connection::flow(handle, receiver_queue_size)`
/// immediately behind it emitted a second, wire-only frame that no mirror
/// accounted for. The broker therefore held `2 × receiver_queue_size` permits
/// while `available_permits()` and `FlowStats` reported `1 ×`, so the consumer
/// could be handed twice the messages its own queue was sized for and the
/// client's view of the broker's balance was wrong from the first frame.
///
/// The broker here holds an EMPTY backlog, so it never spends a permit and its
/// running total is exactly what the client granted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_grants_initial_permits_exactly_once() {
    let stats = Arc::new(BrokerStats::default());
    let broker_stats = stats.clone();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
    let port = listener.local_addr().expect("local_addr").port();
    let broker = tokio::spawn(async move {
        if let Ok((stream, _peer)) = listener.accept().await {
            serve_failover_flow_strict_broker(stream, 0, false, false, broker_stats).await;
        }
    });

    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("client connect");
    let client = Client::from_socket(stream, long_keepalive_config())
        .await
        .expect("handshake");

    let consumer = tokio::time::timeout(
        RECV_GUARD,
        client.subscribe(magnetar_proto::SubscribeRequest {
            topic: "persistent://public/default/initial-grant-once".to_owned(),
            subscription: "initial-grant-once-sub".to_owned(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            receiver_queue_size: RECEIVER_QUEUE_SIZE,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");

    wait_for_first_grant(&stats).await;
    tokio::time::sleep(GRANT_SETTLE).await;

    assert_eq!(
        stats.flow_permits_granted.load(Ordering::SeqCst),
        RECEIVER_QUEUE_SIZE as u64,
        "a fresh subscribe must grant the receiver-queue size EXACTLY once (issue #426); \
         the broker observed {} permit(s) against a configured {RECEIVER_QUEUE_SIZE}",
        stats.flow_permits_granted.load(Ordering::SeqCst),
    );
    assert_eq!(
        u64::from(consumer.available_permits()),
        RECEIVER_QUEUE_SIZE as u64,
        "the client-side balance must equal what the broker was actually granted",
    );

    client.close().await;
    broker.abort();
}

/// REGRESSION (issue #427 — the active announcement does not add a second grant).
///
/// A real broker answers an `Exclusive` / `Failover` subscribe with `CommandSuccess` and
/// then `CommandActiveConsumerChange { is_active: true }` right behind it — which is what
/// `announce_active` turns on here, and what the issue #426 test above deliberately left
/// off to isolate one bug at a time.
///
/// Both frames arrive in one read, so the sans-io layer's issue #307 promotion re-arm runs
/// inside `handle_bytes` while this task is still parked on `subscribe()`; `granted_permits`
/// is legitimately `0` at that instant, so its gate passes. The engine then issued its own
/// post-ack `initial_flow` on top, and the broker held `2 × receiver_queue_size` for a fresh
/// consumer — measured 32 against a configured 16 — while `available_permits()` reported
/// `1 ×`.
///
/// The broker holds an EMPTY backlog, so it never spends a permit and its running total is
/// exactly what the client granted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_active_announcement_does_not_double_the_initial_grant() {
    let stats = Arc::new(BrokerStats::default());
    let broker_stats = stats.clone();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
    let port = listener.local_addr().expect("local_addr").port();
    let broker = tokio::spawn(async move {
        if let Ok((stream, _peer)) = listener.accept().await {
            serve_failover_flow_strict_broker(stream, 0, false, true, broker_stats).await;
        }
    });

    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("client connect");
    let client = Client::from_socket(stream, long_keepalive_config())
        .await
        .expect("handshake");

    let consumer = tokio::time::timeout(
        RECV_GUARD,
        client.subscribe(magnetar_proto::SubscribeRequest {
            topic: "persistent://public/default/active-change-grant-once".to_owned(),
            subscription: "active-change-grant-once-sub".to_owned(),
            sub_type: pb::command_subscribe::SubType::Failover,
            receiver_queue_size: RECEIVER_QUEUE_SIZE,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");

    wait_for_first_grant(&stats).await;
    tokio::time::sleep(GRANT_SETTLE).await;

    assert_eq!(
        stats.flow_permits_granted.load(Ordering::SeqCst),
        RECEIVER_QUEUE_SIZE as u64,
        "a Failover subscribe whose ack is followed by ActiveConsumerChange{{active}} must \
         still grant the receiver-queue size EXACTLY once (issue #427); the broker observed \
         {} permit(s) against a configured {RECEIVER_QUEUE_SIZE}",
        stats.flow_permits_granted.load(Ordering::SeqCst),
    );
    assert_eq!(
        u64::from(consumer.available_permits()),
        RECEIVER_QUEUE_SIZE as u64,
        "the client-side balance must equal what the broker was actually granted",
    );

    client.close().await;
    broker.abort();
}

/// Counts how many distinct `CommandSubscribe` the broker observed for the
/// consumer — the load-bearing signal that the client re-subscribed after the
/// broker-initiated close.
#[derive(Default)]
struct ResubscribeStats {
    subscribes_observed: AtomicU64,
    messages_dispatched: AtomicU64,
}

/// Flow-strict Failover broker that injects ONE broker-initiated
/// `CommandCloseConsumer{assigned_broker_service_url: None}` mid-stream — the
/// same-broker bundle reassignment (`code=6`) that issue #307's production trace
/// proved wedges an already-running consumer.
///
/// Sequence:
///   1. CONNECT/LOOKUP/SUBSCRIBE as usual (Success + ActiveConsumerChange{active}).
///   2. Dispatch backlog against the granted permits.
///   3. After `close_after` messages have been dispatched, send a single
///      `CommandCloseConsumer{url=None}` on the LIVE socket (NO disconnect) and tear the
///      broker-side consumer down: `subscribed=false`, permits=0, and the consumer id is forgotten.
///      From here a bare `CommandFlow` would be dropped ("Couldn't find consumer") — the consumer
///      can only resume if the client re-issues `CommandSubscribe`.
///   4. On the client's re-subscribe, re-ack (`Success` + `ActiveConsumerChange`) and resume
///      dispatching the REMAINING backlog against the re-armed flow.
///
/// Before the fix the client never re-subscribes, so the broker stays torn down,
/// no further messages are dispatched, and `receive()` wedges. After the fix the
/// client re-subscribes in place and drains the full backlog.
async fn serve_failover_broker_close_midstream(
    mut stream: TcpStream,
    backlog: u64,
    close_after: u64,
    stats: Arc<ResubscribeStats>,
) {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out = BytesMut::with_capacity(64 * 1024);
    let mut consumer_id = 0u64;
    let mut subscribed = false;
    let mut permits: u64 = 0;
    let mut next_entry: u64 = 0;
    let mut close_injected = false;

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
                            server_version: "close-midstream-broker".to_owned(),
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
                        // Both the initial subscribe AND the post-close
                        // re-subscribe land here — the latter is what we are
                        // proving the client issues.
                        consumer_id = s.consumer_id;
                        subscribed = true;
                        stats.subscribes_observed.fetch_add(1, Ordering::SeqCst);
                        let success = pb::BaseCommand {
                            r#type: pb::base_command::Type::Success as i32,
                            success: Some(pb::CommandSuccess {
                                request_id: s.request_id,
                                schema: None,
                            }),
                            ..Default::default()
                        };
                        let _ = encode_command(&mut out, &success);
                        let active = pb::BaseCommand {
                            r#type: pb::base_command::Type::ActiveConsumerChange as i32,
                            active_consumer_change: Some(pb::CommandActiveConsumerChange {
                                consumer_id,
                                is_active: Some(true),
                            }),
                            ..Default::default()
                        };
                        let _ = encode_command(&mut out, &active);
                    }
                }
                pb::base_command::Type::Flow => {
                    // Flow only counts once the consumer is registered — exactly
                    // like the real broker, a flow for a torn-down consumer id is
                    // dropped on the floor.
                    if subscribed {
                        if let Some(f) = &frame.command.flow {
                            permits = permits.saturating_add(u64::from(f.message_permits));
                        }
                    }
                }
                pb::base_command::Type::Ack => {
                    if let Some(a) = &frame.command.ack {
                        let resp = pb::BaseCommand {
                            r#type: pb::base_command::Type::AckResponse as i32,
                            ack_response: Some(pb::CommandAckResponse {
                                consumer_id: a.consumer_id,
                                request_id: a.request_id,
                                txnid_least_bits: None,
                                txnid_most_bits: None,
                                error: None,
                                message: None,
                            }),
                            ..Default::default()
                        };
                        let _ = encode_command(&mut out, &resp);
                    }
                }
                pb::base_command::Type::CloseConsumer => {
                    // Client-initiated graceful close (teardown at end of test).
                    return;
                }
                _ => {}
            }
        }

        // Inject the one broker-initiated close once enough has been dispatched.
        if subscribed && !close_injected && next_entry >= close_after && next_entry < backlog {
            close_injected = true;
            let close = pb::BaseCommand {
                r#type: pb::base_command::Type::CloseConsumer as i32,
                close_consumer: Some(pb::CommandCloseConsumer {
                    consumer_id,
                    request_id: 0,
                    // None == same-broker bundle reassignment (the #307 case).
                    assigned_broker_service_url: None,
                    assigned_broker_service_url_tls: None,
                }),
                ..Default::default()
            };
            let _ = encode_command(&mut out, &close);
            // Tear the broker-side consumer down: until the client re-subscribes,
            // no flow is honoured and nothing is dispatched.
            subscribed = false;
            permits = 0;
        }

        if subscribed {
            while permits > 0 && next_entry < backlog {
                let entry = next_entry;
                next_entry += 1;
                permits -= 1;
                stats.messages_dispatched.fetch_add(1, Ordering::SeqCst);

                let msg = pb::BaseCommand {
                    r#type: pb::base_command::Type::Message as i32,
                    message: Some(pb::CommandMessage {
                        consumer_id,
                        message_id: pb::MessageIdData {
                            ledger_id: 1,
                            entry_id: entry,
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
                let meta = pb::MessageMetadata {
                    producer_name: "backlog".to_owned(),
                    sequence_id: entry,
                    publish_time: 0,
                    num_messages_in_batch: Some(1),
                    ..Default::default()
                };
                let _ = encode_payload(&mut out, &msg, &meta, format!("entry-{entry}").as_bytes());
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

/// REPRODUCTION (issue #307 root cause — same-broker `CommandCloseConsumer`).
///
/// An already-running Failover consumer drains part of a backlog, then the
/// broker sends a single `CommandCloseConsumer{assigned_broker_service_url:
/// None}` on the LIVE socket (a same-broker bundle reassignment — NOT a socket
/// drop, so no supervised reconnect / `rebuild_consumers` fires). The broker
/// tears its consumer id down; the consumer can only resume if the client
/// re-issues `CommandSubscribe`.
///
/// BEFORE the fix: nothing re-subscribes the running consumer; the broker stays
/// torn down, never dispatches the rest of the backlog, and `receive()` wedges
/// (the test times out at `RECV_GUARD`).
///
/// AFTER the fix: the proto layer re-subscribes the consumer in place at
/// close-time; the broker observes a second `Subscribe`, re-arms flow on its
/// `Success`, and the consumer drains the FULL backlog.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_resubscribes_and_drains_after_same_broker_close() {
    // Close roughly a third of the way through so there is a substantial
    // remaining backlog that can ONLY be drained via a re-subscribe.
    const CLOSE_AFTER: u64 = BACKLOG / 3;

    let stats = Arc::new(ResubscribeStats::default());
    let broker_stats = stats.clone();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
    let port = listener.local_addr().expect("local_addr").port();
    let broker = tokio::spawn(async move {
        if let Ok((stream, _peer)) = listener.accept().await {
            serve_failover_broker_close_midstream(stream, BACKLOG, CLOSE_AFTER, broker_stats).await;
        }
    });

    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("client connect");
    let client = Client::from_socket(stream, long_keepalive_config())
        .await
        .expect("handshake");

    let consumer = tokio::time::timeout(
        RECV_GUARD,
        client.subscribe(magnetar_proto::SubscribeRequest {
            topic: "persistent://public/default/failover-broker-close".to_owned(),
            subscription: "failover-broker-close-sub".to_owned(),
            sub_type: pb::command_subscribe::SubType::Failover,
            receiver_queue_size: RECEIVER_QUEUE_SIZE,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");

    let mut received: u64 = 0;
    while received < BACKLOG {
        let msg = tokio::time::timeout(RECV_GUARD, consumer.receive())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "receive() WEDGED after {received}/{BACKLOG} messages: the broker sent a \
                     same-broker CommandCloseConsumer(url=None) after dispatching {CLOSE_AFTER}; \
                     the client did not re-subscribe ({} subscribes observed, {} dispatched), so \
                     the broker stayed torn down and the backlog never drained (issue #307)",
                    stats.subscribes_observed.load(Ordering::SeqCst),
                    stats.messages_dispatched.load(Ordering::SeqCst),
                )
            })
            .expect("receive ok");
        // Ack to mirror the production converter and pulse the driver.
        tokio::time::timeout(RECV_GUARD, consumer.ack(msg.message_id))
            .await
            .expect("ack did not time out")
            .expect("ack ok");
        received += 1;
    }

    assert_eq!(
        received, BACKLOG,
        "the Failover consumer must drain the ENTIRE backlog across the broker-initiated close",
    );
    assert!(
        stats.subscribes_observed.load(Ordering::SeqCst) >= 2,
        "the client MUST re-subscribe after the same-broker CommandCloseConsumer \
         (observed {} subscribes; expected >= 2: initial + re-subscribe)",
        stats.subscribes_observed.load(Ordering::SeqCst),
    );

    client.close().await;
    broker.abort();
}
