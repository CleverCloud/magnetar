// SPDX-License-Identifier: Apache-2.0

//! Issue #348 — `ConsumerEventListener` parity (becameActive/becameInactive)
//! — tokio engine, driven over a real loopback broker + the production
//! driver loop.
//!
//! Two scenarios:
//!
//! 1. `promote_and_demote_resolve_via_next_active_change`: the broker pushes a
//!    `CommandActiveConsumerChange { is_active: true }` right alongside the subscribe ack, then (on
//!    a test-triggered signal) `is_active: false`. `Consumer::is_active()` and
//!    `Consumer::next_active_change().await` must track both transitions in order.
//! 2. `next_active_change_resolves_err_after_close`: a `next_active_change()` future parked on one
//!    consumer clone must resolve `Err` once a SIBLING clone closes the consumer — mirrors
//!    `ReceiveFut`'s close-wakes-parked- futures contract (issue #299 lineage), extended to the new
//!    active-change waker slab.
//!
//! The moonpool twin
//! (`crates/magnetar-runtime-moonpool/tests/consumer_active_change.rs`)
//! covers the same two scenarios over `TokioProviders` + a real loopback
//! listener (mirrors `reconnect_replay_gating.rs`'s harness) — keeps
//! `cargo xtask check-runtime-test-parity` 1:1 (ADR-0024).
//!
//! Scenario 3 (`next_active_change_manual_poll_covers_repark_and_drop_cancel`)
//! adds a manual `Future::poll` drive of the crate-private `ActiveChangeFut`
//! (reached only via the opaque `impl Future` `next_active_change()`
//! returns) so the re-park ("refresh the slab registration") branch and the
//! drop-while-parked cancel branch are hit deterministically — letting the
//! tokio executor pick when/how often `.await` polls a future cannot
//! guarantee either branch runs. Moonpool sibling of the same name.

#![forbid(unsafe_code)]

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Wake};

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, FrameError, SubscribeRequest, decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::{Client, ClientError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;

mod common;
use common::HANG_GUARD;

/// Counting waker — records wake calls (unused by the manual-poll assertions
/// below, but a real, inert `Waker` is still required to build a `Context`).
/// Mirrors the helper in `marker_lost_wakeup.rs`.
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

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-consumer-active-change".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_subscribe_success(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_close_success(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_lookup_response(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::LookupResponse as i32,
        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
            broker_service_url: None,
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
            request_id,
            authoritative: Some(true),
            error: None,
            message: None,
            proxy_through_service_url: Some(false),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_active_consumer_change(out: &mut BytesMut, consumer_id: u64, is_active: bool) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ActiveConsumerChange as i32,
        active_consumer_change: Some(pb::CommandActiveConsumerChange {
            consumer_id,
            is_active: Some(is_active),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Broker session for scenario 1: answers CONNECT / SUBSCRIBE normally,
/// piggy-backing an immediate promotion (`is_active: true`) on the subscribe
/// ack. On `demote_trigger`, pushes a standalone demotion
/// (`is_active: false`) for the same consumer id.
async fn run_promote_demote_broker_conn(
    stream: &mut tokio::net::TcpStream,
    demote_trigger: Arc<Notify>,
) {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out_buf = BytesMut::with_capacity(8 * 1024);
    let mut consumer_id: u64 = 0;
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
                pb::base_command::Type::Connect => emit_connected(&mut out_buf),
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        emit_lookup_response(&mut out_buf, l.request_id);
                    }
                }
                pb::base_command::Type::Subscribe => {
                    if let Some(s) = &frame.command.subscribe {
                        consumer_id = s.consumer_id;
                        emit_subscribe_success(&mut out_buf, s.request_id);
                        emit_active_consumer_change(&mut out_buf, consumer_id, true);
                    }
                }
                _ => {}
            }
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
        let mut probe = [0u8; 8 * 1024];
        tokio::select! {
            biased;
            () = demote_trigger.notified() => {
                emit_active_consumer_change(&mut out_buf, consumer_id, false);
                if stream.write_all(&out_buf).await.is_err() {
                    return;
                }
                let _ = stream.flush().await;
                out_buf.clear();
            }
            res = stream.read(&mut probe) => {
                match res {
                    Ok(0) | Err(_) => return,
                    Ok(n) => read_buf.extend_from_slice(&probe[..n]),
                }
            }
        }
    }
}

async fn spawn_promote_demote_broker() -> (String, Arc<Notify>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let demote_trigger = Arc::new(Notify::new());
    let accept_trigger = demote_trigger.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _peer)) = listener.accept().await else {
                return;
            };
            let trigger = accept_trigger.clone();
            tokio::spawn(async move {
                run_promote_demote_broker_conn(&mut stream, trigger).await;
            });
        }
    });
    (format!("pulsar://{addr}"), demote_trigger)
}

/// Broker session for scenario 2: answers CONNECT / SUBSCRIBE / `CLOSE_CONSUMER`
/// normally; never emits an `ActiveConsumerChange` frame.
async fn run_close_only_broker_conn(stream: &mut tokio::net::TcpStream) {
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
            let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
                continue;
            };
            match kind {
                pb::base_command::Type::Connect => emit_connected(&mut out_buf),
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        emit_lookup_response(&mut out_buf, l.request_id);
                    }
                }
                pb::base_command::Type::Subscribe => {
                    if let Some(s) = &frame.command.subscribe {
                        emit_subscribe_success(&mut out_buf, s.request_id);
                    }
                }
                pb::base_command::Type::CloseConsumer => {
                    if let Some(c) = &frame.command.close_consumer {
                        emit_close_success(&mut out_buf, c.request_id);
                    }
                }
                _ => {}
            }
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

async fn spawn_close_only_broker() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _peer)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                run_close_only_broker_conn(&mut stream).await;
            });
        }
    });
    format!("pulsar://{addr}")
}

/// Scenario 1: `is_active()` + `next_active_change().await` track both a
/// promotion and a subsequent demotion, in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promote_and_demote_resolve_via_next_active_change() {
    let (url, demote_trigger) = spawn_promote_demote_broker().await;
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let consumer = tokio::time::timeout(
        HANG_GUARD,
        client.subscribe(SubscribeRequest {
            topic: "persistent://public/default/consumer-active-change".to_owned(),
            subscription: "consumer-active-change".to_owned(),
            sub_type: pb::command_subscribe::SubType::Failover,
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");

    let promoted = tokio::time::timeout(HANG_GUARD, consumer.next_active_change())
        .await
        .expect("next_active_change (promote) did not time out")
        .expect("next_active_change (promote) resolved Ok");
    assert!(promoted, "the broker promoted this consumer to active");
    assert_eq!(
        consumer.is_active(),
        Some(true),
        "is_active() must reflect the promotion"
    );

    demote_trigger.notify_one();

    let demoted = tokio::time::timeout(HANG_GUARD, consumer.next_active_change())
        .await
        .expect("next_active_change (demote) did not time out")
        .expect("next_active_change (demote) resolved Ok");
    assert!(!demoted, "the broker demoted this consumer to standby");
    assert_eq!(
        consumer.is_active(),
        Some(false),
        "is_active() must reflect the demotion"
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
}

/// Scenario 2: a `next_active_change()` future parked on one clone resolves
/// `Err` once a sibling clone closes the consumer — mirrors `ReceiveFut`'s
/// close-wakes-parked-futures contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn next_active_change_resolves_err_after_close() {
    let url = spawn_close_only_broker().await;
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let consumer = tokio::time::timeout(
        HANG_GUARD,
        client.subscribe(SubscribeRequest {
            topic: "persistent://public/default/consumer-active-change-close".to_owned(),
            subscription: "consumer-active-change-close".to_owned(),
            sub_type: pb::command_subscribe::SubType::Failover,
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");

    let parked = consumer.clone();
    let parked_task = tokio::spawn(async move { parked.next_active_change().await });

    // Let the spawned task actually reach the parked `Pending` state before
    // closing — otherwise the close could win the race trivially without
    // exercising the waker wake-up path.
    tokio::task::yield_now().await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    tokio::time::timeout(HANG_GUARD, consumer.close())
        .await
        .expect("close did not time out")
        .expect("close ok");

    let result = tokio::time::timeout(HANG_GUARD, parked_task)
        .await
        .expect("parked next_active_change did not time out")
        .expect("join ok");
    assert!(
        matches!(result, Err(ClientError::Closed)),
        "a next_active_change() parked before close must resolve Err(Closed), got {result:?}"
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
}

/// Scenario 3: manually driving `Future::poll` on the future
/// `next_active_change()` returns exercises two branches `.await` cannot
/// reliably hit:
///
/// - polling a second time while still parked (nothing buffered, not terminal) takes the "refresh
///   the slab registration" path — the previously-installed waker slot is evicted and a fresh one
///   installed;
/// - dropping the future while parked (a registered slab slot, never resolved) exercises
///   `ActiveChangeFut::drop`'s cancel path.
///
/// The broker never emits `CommandActiveConsumerChange`, so every poll
/// parks — deterministic by construction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn next_active_change_manual_poll_covers_repark_and_drop_cancel() {
    let url = spawn_close_only_broker().await;
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let consumer = tokio::time::timeout(
        HANG_GUARD,
        client.subscribe(SubscribeRequest {
            topic: "persistent://public/default/consumer-active-change-manual-poll".to_owned(),
            subscription: "consumer-active-change-manual-poll".to_owned(),
            sub_type: pb::command_subscribe::SubType::Failover,
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");

    let waker = Arc::new(CountingWaker {
        woken: std::sync::atomic::AtomicUsize::new(0),
    });
    let std_waker: std::task::Waker = waker.into();
    let mut cx = Context::from_waker(&std_waker);

    // `Box::pin` (not `std::pin::pin!`) is required here: `pin!` produces a
    // `Pin<&mut T>` borrowing a hidden stack local, so `drop(fut)` below
    // would only drop the reference, not the future — the whole point of
    // this scenario is to actually invoke `ActiveChangeFut::drop`.
    let mut fut = Box::pin(consumer.next_active_change());

    // First poll: nothing buffered yet, not terminal -> registers a fresh
    // waker slot and parks.
    assert!(
        fut.as_mut().poll(&mut cx).is_pending(),
        "first poll must park: no buffered transition, broker never promotes/demotes"
    );

    // Second poll while still parked with nothing having changed: hits the
    // "refresh the slab registration" branch (evict the old slot, install a
    // new one) before parking again.
    assert!(
        fut.as_mut().poll(&mut cx).is_pending(),
        "second poll while parked must also park (still nothing buffered)"
    );

    // Drop while parked: `slab_key` is `Some`, so `Drop` must evict the
    // still-registered waker slot.
    drop(fut);

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
}
