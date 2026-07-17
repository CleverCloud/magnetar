// SPDX-License-Identifier: Apache-2.0

//! Consumer flow-replenishment starvation under sustained inbound load —
//! moonpool engine twin of
//! `crates/magnetar-runtime-tokio/tests/consumer_flow_replenish_starvation.rs`
//! (ADR-0024 cross-runtime test parity, 1:1 test names).
//!
//! ## Production symptom (issue #307)
//!
//! A `Failover` consumer on a backlogged topic consumes a burst right after
//! subscribe (~`receiver_queue_size` messages), then stops receiving entirely
//! and sits at `availablePermits = 0`, `msgRateOut = 0` against a huge backlog —
//! with NO `CommandCloseConsumer`, NO reconnect, NO `ActiveConsumerChange` in
//! between. After the initial flow grant is drained, replenishment `CommandFlow`
//! is not reaching the broker and dispatch stops.
//!
//! The tokio engine reproduced this: its `Consumer::receive()` success path
//! popped the message (queuing the replenishment `CommandFlow` via `maybe_flow`)
//! but did NOT wake the driver task, so the queued flow sat unflushed once the
//! inbound stream ran dry at a window boundary. The moonpool engine's
//! `ReceiveFut::poll` already pulses `driver_waker.notify_one()` after
//! `pop_message` (consumer.rs), so it drains cleanly — this twin pins that
//! correct behaviour so the engines cannot drift back apart.
//!
//! A flow-control-strict broker tracks a per-consumer permit budget, dispatches
//! a backlog message ONLY when `permits > 0`, and holds a backlog `10×` larger
//! than one receiver-queue window so draining requires MULTIPLE replenishment
//! rounds. A LONG keepalive is pinned so the keepalive timer cannot mask a wedge
//! by flushing a stranded flow.

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, FrameError, SubscribeRequest, decode_one, encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const RECEIVER_QUEUE_SIZE: usize = 16;
const BACKLOG: u64 = (RECEIVER_QUEUE_SIZE as u64) * 10;

/// Short per-`receive()` guard, well below the pinned 1 h keepalive so the
/// keepalive timer cannot mask a wedge.
const RECV_GUARD: Duration = Duration::from_secs(10);

fn long_keepalive_config() -> ConnectionConfig {
    ConnectionConfig {
        keepalive_interval: Duration::from_hours(1),
        ..Default::default()
    }
}

#[derive(Default)]
struct BrokerStats {
    flow_permits_granted: AtomicU64,
    messages_dispatched: AtomicU64,
}

/// Flow-control-strict mock broker for a single Failover consumer. Identical
/// protocol to the tokio twin's broker.
async fn serve_failover_flow_strict_broker(
    mut stream: TcpStream,
    backlog: u64,
    ack_response: bool,
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

/// REPRODUCTION (no-ack variant). Drain a backlog `10×` larger than one
/// receiver-queue window with NO acks: replenishment is driven purely by
/// `receive()` -> `pop_message` -> `maybe_flow`. The moonpool engine wakes the
/// driver after `pop_message`, so the queued `CommandFlow` is flushed and the
/// full backlog drains. (The tokio engine had the wedge here before issue #307.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_no_ack_drains_full_backlog_without_wedge() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let stats = Arc::new(BrokerStats::default());
            let broker_stats = stats.clone();

            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
            let addr = listener.local_addr().expect("local_addr").to_string();
            tokio::spawn(async move {
                if let Ok((stream, _peer)) = listener.accept().await {
                    serve_failover_flow_strict_broker(stream, BACKLOG, false, broker_stats).await;
                }
            });

            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                RECV_GUARD,
                Client::connect_plain(&engine, &addr, long_keepalive_config()),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");

            let consumer = tokio::time::timeout(
                RECV_GUARD,
                client.subscribe(SubscribeRequest {
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
                             broker granted {} permits, dispatched {} messages",
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
        })
        .await;
}

/// REPRODUCTION (realistic variant — acks every message and awaits the
/// `CommandAckResponse`, like the production converter).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_with_ack_drains_full_backlog_without_wedge() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let stats = Arc::new(BrokerStats::default());
            let broker_stats = stats.clone();

            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
            let addr = listener.local_addr().expect("local_addr").to_string();
            tokio::spawn(async move {
                if let Ok((stream, _peer)) = listener.accept().await {
                    serve_failover_flow_strict_broker(stream, BACKLOG, true, broker_stats).await;
                }
            });

            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                RECV_GUARD,
                Client::connect_plain(&engine, &addr, long_keepalive_config()),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");

            let consumer = tokio::time::timeout(
                RECV_GUARD,
                client.subscribe(SubscribeRequest {
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
        })
        .await;
}

/// Counts distinct `CommandSubscribe` observed plus messages dispatched — the
/// load-bearing signal that the client re-subscribed after the broker close.
#[derive(Default)]
struct ResubscribeStats {
    subscribes_observed: AtomicU64,
    messages_dispatched: AtomicU64,
}

/// Flow-strict Failover broker that injects ONE broker-initiated
/// `CommandCloseConsumer{assigned_broker_service_url: None}` mid-stream (the
/// same-broker bundle reassignment from issue #307). Identical protocol to the
/// tokio twin's `serve_failover_broker_close_midstream`. After the close the
/// broker tears its consumer id down and only resumes once the client re-issues
/// `CommandSubscribe`.
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
                    return;
                }
                _ => {}
            }
        }

        if subscribed && !close_injected && next_entry >= close_after && next_entry < backlog {
            close_injected = true;
            let close = pb::BaseCommand {
                r#type: pb::base_command::Type::CloseConsumer as i32,
                close_consumer: Some(pb::CommandCloseConsumer {
                    consumer_id,
                    request_id: 0,
                    assigned_broker_service_url: None,
                    assigned_broker_service_url_tls: None,
                }),
                ..Default::default()
            };
            let _ = encode_command(&mut out, &close);
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

/// REPRODUCTION (issue #307 root cause — same-broker `CommandCloseConsumer`),
/// moonpool twin of the tokio
/// `failover_resubscribes_and_drains_after_same_broker_close`. The broker sends
/// one `CommandCloseConsumer{url=None}` mid-drain on the live socket; the
/// consumer can only resume if the client re-subscribes. Before the fix it
/// wedges; after, it re-subscribes in place and drains the full backlog.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_resubscribes_and_drains_after_same_broker_close() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            const CLOSE_AFTER: u64 = BACKLOG / 3;

            let stats = Arc::new(ResubscribeStats::default());
            let broker_stats = stats.clone();

            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
            let addr = listener.local_addr().expect("local_addr").to_string();
            tokio::spawn(async move {
                if let Ok((stream, _peer)) = listener.accept().await {
                    serve_failover_broker_close_midstream(stream, BACKLOG, CLOSE_AFTER, broker_stats)
                        .await;
                }
            });

            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                RECV_GUARD,
                Client::connect_plain(&engine, &addr, long_keepalive_config()),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");

            let consumer = tokio::time::timeout(
                RECV_GUARD,
                client.subscribe(SubscribeRequest {
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
                            "receive() WEDGED after {received}/{BACKLOG} messages: broker sent a \
                             same-broker CommandCloseConsumer(url=None) after {CLOSE_AFTER}; the \
                             client did not re-subscribe ({} subscribes, {} dispatched) (issue #307)",
                            stats.subscribes_observed.load(Ordering::SeqCst),
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
                "the Failover consumer must drain the ENTIRE backlog across the broker close",
            );
            assert!(
                stats.subscribes_observed.load(Ordering::SeqCst) >= 2,
                "the client MUST re-subscribe after the same-broker CommandCloseConsumer \
                 (observed {} subscribes; expected >= 2)",
                stats.subscribes_observed.load(Ordering::SeqCst),
            );

            client.close().await;
        })
        .await;
}
