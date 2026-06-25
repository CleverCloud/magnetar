// SPDX-License-Identifier: Apache-2.0

//! Partitioned-consumer flow-replenishment starvation under sustained inbound
//! load (issue #307 production-symptom reproduction — PARTITIONED variant).
//!
//! ## Production symptom (ground truth)
//!
//! The accesslogs converter subscribes to a 12-partition topic via
//! `client.partitioned_consumer(topic).subscription(..).subscription_type(Failover)
//! .receiver_queue_size(N)` and drains it with the real `receive()` loop. In
//! production (running c66b92e, the single-topic fix, build-confirmed) the
//! consumer reconnects, consumes for ~30 s, then WEDGES: `availablePermits = 0`
//! on the active sub-consumers, `msgRateOut = 0`, `lastConsumedTimestamp` frozen,
//! backlog growing — with NO `CommandCloseConsumer`, NO reconnect, NO
//! `ActiveConsumerChange` in between.
//!
//! The single-topic fix (c66b92e: `receive()` wakes the driver after
//! `pop_message` so the queued replenishment `CommandFlow` is flushed) is proven
//! by `magnetar-runtime-tokio/tests/consumer_flow_replenish_starvation.rs`. This
//! test asks the open question: does the PARTITIONED receive path
//! (`MultiTopicsConsumer::receive()` → `select_all` over per-partition child
//! `Consumer::receive()` futures, `crates/magnetar/src/multi_topics.rs:353`)
//! correctly replenish flow PER PARTITION, or does it wedge once the first grant
//! drains?
//!
//! ## What this test does
//!
//! A fully synthetic loopback broker (no testcontainers) that:
//!   - CONNECT  -> CONNECTED
//!   - PING     -> PONG
//!   - `PARTITIONED_METADATA` -> `partitions = N`
//!   - LOOKUP   -> Connect-here (`broker_service_url = None`) so every partition's data plane rides
//!     the single bootstrap connection (the converter's real single-broker topology).
//!   - SUBSCRIBE-> Success + `ActiveConsumerChange{is_active:true}` per consumer (Failover: every
//!     partition's sole consumer is the active one).
//!   - FLOW     -> tracked PER `consumer_id` (per partition). The broker dispatches a backlog entry
//!     for a partition ONLY when that partition's permit budget is positive, decrementing on each
//!     dispatch. Each partition holds a backlog `10×` larger than one receiver-queue window, so
//!     draining the topic requires MULTIPLE replenishment rounds per partition.
//!   - ACK      -> (no-ack variant omits) `CommandAckResponse`.
//!
//! A LONG (1 h) keepalive is pinned so the keepalive timer cannot mask a wedge by
//! flushing a stranded flow within the test's short per-receive guard — identical
//! rationale to the single-topic twin.

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]
#![allow(clippy::too_many_lines)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use magnetar::PulsarClient;
use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::proto::{
    ConnectionConfig, FrameError, decode_one, encode_command, encode_payload, pb,
};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const PARTITIONS: u32 = 12;
const RECEIVER_QUEUE_SIZE: usize = 16;
/// Per-partition backlog. The aggregate the partitioned consumer must drain is
/// `PARTITIONS * BACKLOG_PER_PARTITION`.
const BACKLOG_PER_PARTITION: u64 = (RECEIVER_QUEUE_SIZE as u64) * 10;
const TOTAL_BACKLOG: u64 = (PARTITIONS as u64) * BACKLOG_PER_PARTITION;

/// Short per-`receive()` guard, well below the pinned 1 h keepalive.
const RECV_GUARD: Duration = Duration::from_secs(10);

fn long_keepalive_url_client_config() -> ConnectionConfig {
    ConnectionConfig {
        keepalive_interval: Duration::from_secs(3600),
        ..Default::default()
    }
}

#[derive(Default)]
struct BrokerStats {
    flow_permits_granted: AtomicU64,
    messages_dispatched: AtomicU64,
}

/// Per-partition (per `consumer_id`) dispatch state the broker tracks.
struct PartitionState {
    permits: u64,
    next_entry: u64,
}

/// Flow-control-strict synthetic broker for a partitioned Failover consumer.
///
/// The broker NEVER dispatches a partition's backlog without a positive permit
/// budget for that partition's consumer — so if the client stops replenishing
/// flow for any partition, that partition goes quiet and the aggregate drain
/// wedges.
async fn serve_partitioned_failover_flow_strict_broker(
    mut stream: TcpStream,
    partitions: u32,
    backlog_per_partition: u64,
    ack_response: bool,
    stats: Arc<BrokerStats>,
) {
    let mut read_buf = BytesMut::with_capacity(256 * 1024);
    let mut out = BytesMut::with_capacity(256 * 1024);
    // consumer_id -> per-partition dispatch state.
    let mut consumers: HashMap<u64, PartitionState> = HashMap::new();

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
                            server_version: "partitioned-flow-strict-broker".to_owned(),
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
                pb::base_command::Type::PartitionedMetadata => {
                    if let Some(p) = &frame.command.partition_metadata {
                        let cmd = pb::BaseCommand {
                            r#type: pb::base_command::Type::PartitionedMetadataResponse as i32,
                            partition_metadata_response: Some(
                                pb::CommandPartitionedTopicMetadataResponse {
                                    partitions: Some(partitions),
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
                        let _ = encode_command(&mut out, &cmd);
                    }
                }
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        // Connect-here: serve every partition's data plane on this
                        // single bootstrap connection (single-broker topology).
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
                        let consumer_id = s.consumer_id;
                        consumers.entry(consumer_id).or_insert(PartitionState {
                            permits: 0,
                            next_entry: 0,
                        });
                        let success = pb::BaseCommand {
                            r#type: pb::base_command::Type::Success as i32,
                            success: Some(pb::CommandSuccess {
                                request_id: s.request_id,
                                schema: None,
                            }),
                            ..Default::default()
                        };
                        let _ = encode_command(&mut out, &success);
                        // Failover: this partition's sole consumer is active.
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
                        if let Some(state) = consumers.get_mut(&f.consumer_id) {
                            state.permits =
                                state.permits.saturating_add(u64::from(f.message_permits));
                            stats
                                .flow_permits_granted
                                .fetch_add(u64::from(f.message_permits), Ordering::SeqCst);
                        }
                    }
                }
                pb::base_command::Type::Ack => {
                    if let Some(a) = frame.command.ack.as_ref().filter(|_| ack_response) {
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
                // A genuine wedge produces NO `CloseConsumer`; if the client tears
                // down we just stop serving — same as any other unhandled command.
                _ => {}
            }
        }

        // Per-partition dispatch: for every subscribed consumer, dispatch backlog
        // entries one-per-permit while it has budget AND backlog. The broker
        // dispatches ONLY against a positive per-consumer permit budget — the
        // load-bearing flow-control enforcement, PER PARTITION.
        for (&consumer_id, state) in &mut consumers {
            while state.permits > 0 && state.next_entry < backlog_per_partition {
                let entry = state.next_entry;
                state.next_entry += 1;
                state.permits -= 1;
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
                let _ = encode_payload(
                    &mut out,
                    &msg,
                    &meta,
                    format!("c{consumer_id}-entry-{entry}").as_bytes(),
                );
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

/// Build a `PulsarClient` over a loopback socket to the synthetic broker.
async fn connect_client(port: u16) -> PulsarClient {
    PulsarClient::builder()
        .service_url(format!("pulsar://127.0.0.1:{port}"))
        .keepalive(long_keepalive_url_client_config().keepalive_interval)
        .build()
        .await
        .expect("client connect + handshake")
}

/// REPRODUCTION (no-ack variant). Drains the FULL aggregate backlog across all
/// partitions with NO acks, so replenishment is driven purely by the partitioned
/// receive path's per-partition `pop_message` -> `maybe_flow` + driver wake.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_failover_no_ack_drains_full_backlog_without_wedge() {
    let stats = Arc::new(BrokerStats::default());
    let broker_stats = stats.clone();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
    let port = listener.local_addr().expect("local_addr").port();
    let broker = tokio::spawn(async move {
        if let Ok((stream, _peer)) = listener.accept().await {
            serve_partitioned_failover_flow_strict_broker(
                stream,
                PARTITIONS,
                BACKLOG_PER_PARTITION,
                false,
                broker_stats,
            )
            .await;
        }
    });

    let client = connect_client(port).await;

    let consumer = tokio::time::timeout(
        RECV_GUARD,
        client
            .partitioned_consumer("persistent://public/default/partitioned-backlog-noack")
            .subscription("otelgw-accesslogs-reader-noack")
            .subscription_type(SubType::Failover)
            .receiver_queue_size(RECEIVER_QUEUE_SIZE)
            .subscribe(),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");

    let mut received: u64 = 0;
    while received < TOTAL_BACKLOG {
        let _msg = tokio::time::timeout(RECV_GUARD, consumer.receive())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "partitioned receive() WEDGED after {received}/{TOTAL_BACKLOG} messages \
                     (no-ack drain across {PARTITIONS} partitions): broker granted {} permits, \
                     dispatched {} messages — per-partition replenishment CommandFlow from the \
                     partitioned receive path was never flushed for at least one partition",
                    stats.flow_permits_granted.load(Ordering::SeqCst),
                    stats.messages_dispatched.load(Ordering::SeqCst),
                )
            })
            .expect("receive ok");
        received += 1;
    }

    assert_eq!(
        received, TOTAL_BACKLOG,
        "the partitioned Failover consumer must drain the entire aggregate backlog via receive()",
    );
    assert_eq!(
        stats.messages_dispatched.load(Ordering::SeqCst),
        TOTAL_BACKLOG,
        "the broker must have dispatched the entire aggregate backlog",
    );

    client.close().await;
    broker.abort();
}

/// REPRODUCTION (with-ack variant — the production converter acks every
/// message). Acks pulse the driver waker, so this may flush replenishment as a
/// side effect even if the partitioned receive path itself does not — exercising
/// the converter's actual code path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_failover_with_ack_drains_full_backlog_without_wedge() {
    let stats = Arc::new(BrokerStats::default());
    let broker_stats = stats.clone();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
    let port = listener.local_addr().expect("local_addr").port();
    let broker = tokio::spawn(async move {
        if let Ok((stream, _peer)) = listener.accept().await {
            serve_partitioned_failover_flow_strict_broker(
                stream,
                PARTITIONS,
                BACKLOG_PER_PARTITION,
                true,
                broker_stats,
            )
            .await;
        }
    });

    let client = connect_client(port).await;

    let consumer = tokio::time::timeout(
        RECV_GUARD,
        client
            .partitioned_consumer("persistent://public/default/partitioned-backlog-ack")
            .subscription("otelgw-accesslogs-reader-ack")
            .subscription_type(SubType::Failover)
            .receiver_queue_size(RECEIVER_QUEUE_SIZE)
            .subscribe(),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");

    let mut received: u64 = 0;
    while received < TOTAL_BACKLOG {
        let msg = tokio::time::timeout(RECV_GUARD, consumer.receive())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "partitioned receive() WEDGED after {received}/{TOTAL_BACKLOG} messages \
                     (ack drain): broker granted {} permits, dispatched {} messages",
                    stats.flow_permits_granted.load(Ordering::SeqCst),
                    stats.messages_dispatched.load(Ordering::SeqCst),
                )
            })
            .expect("receive ok");
        tokio::time::timeout(RECV_GUARD, consumer.ack(&msg.topic, msg.message.message_id))
            .await
            .expect("ack did not time out")
            .expect("ack ok");
        received += 1;
    }

    assert_eq!(
        received, TOTAL_BACKLOG,
        "the partitioned Failover consumer must drain the entire aggregate backlog",
    );

    client.close().await;
    broker.abort();
}

/// Persistent broker state keyed by per-partition topic name, so a re-subscribe
/// after a reconnect resumes the backlog cursor where the dropped session left
/// off (broker-side durable cursor). The map value is `next_entry` for that
/// partition.
type DurableCursors = Arc<Mutex<HashMap<String, u64>>>;

/// One-connection serve loop that tracks per-partition permits in-session but
/// reads/advances the DURABLE per-partition cursor in `cursors`. After this
/// connection has dispatched `drop_after` messages it returns (drops the socket)
/// — UNLESS `drop_after == 0`, in which case it serves to completion. The
/// `dropped_once` latch ensures only the FIRST connection drops; the reconnect
/// serves to completion.
async fn serve_partitioned_reconnect_conn(
    mut stream: TcpStream,
    partitions: u32,
    backlog_per_partition: u64,
    cursors: DurableCursors,
    drop_after: u64,
    dropped_once: Arc<AtomicBool>,
    stats: Arc<BrokerStats>,
) {
    let mut read_buf = BytesMut::with_capacity(256 * 1024);
    let mut out = BytesMut::with_capacity(256 * 1024);
    // consumer_id -> (topic_suffix, in-session permits)
    let mut consumer_topic: HashMap<u64, String> = HashMap::new();
    let mut permits: HashMap<u64, u64> = HashMap::new();
    let mut dispatched_this_conn: u64 = 0;
    let _ = partitions;

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
                            server_version: "partitioned-reconnect-broker".to_owned(),
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
                pb::base_command::Type::PartitionedMetadata => {
                    if let Some(p) = &frame.command.partition_metadata {
                        let cmd = pb::BaseCommand {
                            r#type: pb::base_command::Type::PartitionedMetadataResponse as i32,
                            partition_metadata_response: Some(
                                pb::CommandPartitionedTopicMetadataResponse {
                                    partitions: Some(partitions),
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
                        let _ = encode_command(&mut out, &cmd);
                    }
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
                        consumer_topic.insert(s.consumer_id, s.topic.clone());
                        permits.insert(s.consumer_id, 0);
                        // Pulsar redelivers every un-acked message to a
                        // (re)subscribing consumer. This consumer never acks, so
                        // a re-subscribe after a reconnect rewinds the partition
                        // cursor to 0 — modelling redelivery of the in-flight
                        // messages lost when the socket dropped. (The client may
                        // then see duplicates; the assertion is a liveness check
                        // — it must keep draining past the reconnect.)
                        cursors.lock().insert(s.topic.clone(), 0);
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
                                consumer_id: s.consumer_id,
                                is_active: Some(true),
                            }),
                            ..Default::default()
                        };
                        let _ = encode_command(&mut out, &active);
                    }
                }
                pb::base_command::Type::Flow => {
                    if let Some(f) = &frame.command.flow {
                        if let Some(p) = permits.get_mut(&f.consumer_id) {
                            *p = p.saturating_add(u64::from(f.message_permits));
                            stats
                                .flow_permits_granted
                                .fetch_add(u64::from(f.message_permits), Ordering::SeqCst);
                        }
                    }
                }
                // No-ack consumer: ACKs (none arrive) and any other command are
                // both no-ops here.
                _ => {}
            }
        }

        // Per-partition dispatch against the DURABLE cursor.
        let mut should_drop = false;
        for (&consumer_id, topic) in &consumer_topic {
            let p = permits.get_mut(&consumer_id).copied().unwrap_or(0);
            let mut granted = p;
            while granted > 0 {
                let mut cur = cursors.lock();
                let next = cur.entry(topic.clone()).or_insert(0);
                if *next >= backlog_per_partition {
                    break;
                }
                let entry = *next;
                *next += 1;
                drop(cur);
                granted -= 1;
                stats.messages_dispatched.fetch_add(1, Ordering::SeqCst);
                dispatched_this_conn += 1;

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

                if drop_after > 0
                    && dispatched_this_conn >= drop_after
                    && !dropped_once.swap(true, Ordering::SeqCst)
                {
                    should_drop = true;
                    break;
                }
            }
            *permits.get_mut(&consumer_id).unwrap() = granted;
            if should_drop {
                break;
            }
        }

        if !out.is_empty() {
            if stream.write_all(&out).await.is_err() {
                return;
            }
            let _ = stream.flush().await;
            out.clear();
        }

        if should_drop {
            // Hard-drop the socket mid-backlog to model the production
            // reconnect trigger. The supervisor reconnects and replays the
            // per-partition subscribes against the durable cursor.
            return;
        }

        if matches!(stream.read_buf(&mut read_buf).await, Ok(0) | Err(_)) {
            return;
        }
    }
}

/// RECONNECT PROBE (task branch 4): production wedges specifically AFTER a
/// reconnect — the consumer reconnects, consumes ~30 s, then stalls. Subscribe a
/// partitioned Failover consumer with the auto-reconnect supervisor enabled,
/// drain part of the backlog, force a mid-backlog socket drop, and assert the
/// consumer resumes draining the FULL aggregate backlog across the reconnect
/// (transparent per-partition re-subscribe + replenished flow).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_failover_resumes_drain_after_reconnect() {
    let stats = Arc::new(BrokerStats::default());
    let cursors: DurableCursors = Arc::new(Mutex::new(HashMap::new()));
    let dropped_once = Arc::new(AtomicBool::new(false));

    // Drop the first connection after ~1.5 windows per partition have been
    // dispatched, so the drop lands mid-backlog with replenishment in flight.
    let drop_after =
        u64::from(PARTITIONS) * (RECEIVER_QUEUE_SIZE as u64) + (RECEIVER_QUEUE_SIZE as u64) / 2;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
    let port = listener.local_addr().expect("local_addr").port();
    let broker_stats = stats.clone();
    let broker_cursors = cursors.clone();
    let broker_dropped = dropped_once.clone();
    let broker = tokio::spawn(async move {
        // Serve connections until the test aborts. The first one drops mid-drain;
        // the reconnect serves to completion (drop latch already set).
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            serve_partitioned_reconnect_conn(
                stream,
                PARTITIONS,
                BACKLOG_PER_PARTITION,
                broker_cursors.clone(),
                drop_after,
                broker_dropped.clone(),
                broker_stats.clone(),
            )
            .await;
        }
    });

    let client = PulsarClient::builder()
        .service_url(format!("pulsar://127.0.0.1:{port}"))
        .keepalive(Duration::from_secs(3600))
        .enable_reconnect(magnetar_proto::SupervisorConfig {
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(200),
            max_attempts: None,
            ..magnetar_proto::SupervisorConfig::default()
        })
        .build()
        .await
        .expect("client connect + handshake");

    let consumer = tokio::time::timeout(
        RECV_GUARD,
        client
            .partitioned_consumer("persistent://public/default/partitioned-backlog-reconnect")
            .subscription("otelgw-accesslogs-reader-reconnect")
            .subscription_type(SubType::Failover)
            .receiver_queue_size(RECEIVER_QUEUE_SIZE)
            .subscribe(),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");

    let mut received: u64 = 0;
    while received < TOTAL_BACKLOG {
        // A wider guard absorbs the reconnect backoff + re-subscribe round-trip.
        let _msg = tokio::time::timeout(Duration::from_secs(20), consumer.receive())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "partitioned receive() WEDGED after {received}/{TOTAL_BACKLOG} messages \
                     (post-reconnect drain): broker granted {} permits, dispatched {} messages — \
                     the consumer did not resume draining after the reconnect / re-subscribe",
                    stats.flow_permits_granted.load(Ordering::SeqCst),
                    stats.messages_dispatched.load(Ordering::SeqCst),
                )
            })
            .expect("receive ok");
        received += 1;
    }

    assert_eq!(
        received, TOTAL_BACKLOG,
        "the partitioned Failover consumer must resume and drain the full backlog after reconnect",
    );
    assert!(
        dropped_once.load(Ordering::SeqCst),
        "the test must have forced a mid-backlog socket drop (the reconnect trigger)",
    );

    client.close().await;
    broker.abort();
}
