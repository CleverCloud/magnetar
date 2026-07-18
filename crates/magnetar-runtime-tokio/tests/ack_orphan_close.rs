// SPDX-License-Identifier: Apache-2.0

//! Issue #346 — ack orphaned by same-broker `CloseConsumer` + no deadline —
//! tokio engine, driven over a real loopback broker + the production driver
//! loop.
//!
//! Two scenarios, mirroring the `send_timeout` shape in
//! `virtual_clock_driver_loop.rs`:
//!
//! 1. `ack_orphan_close_fails_fast_after_same_broker_close`: the broker answers one `CommandAck`
//!    with a same-broker `CommandCloseConsumer` (`assigned_broker_service_url = None`) instead of a
//!    `CommandAckResponse`. The proto layer's close-handler orphan sweep must resolve the caller's
//!    `ack().await` immediately with `ClientError::Broker{code: -1, message: "ack orphaned by
//!    broker consumer close"}` — not leave it parked until the `ack_response_timeout` backstop.
//! 2. `ack_response_timeout_fires_against_host_clock`: the broker never responds to `CommandAck` at
//!    all. With a short `ack_response_timeout` configured, `ack().await` must resolve
//!    `Err(ClientError::Broker{code: -1, message: "ack timeout"})` within a bounded window over the
//!    real host clock.
//!
//! The moonpool twin (`crates/magnetar-runtime-moonpool/tests/ack_orphan_close.rs`)
//! covers the same two scenarios by locking `ConnectionShared::inner`
//! directly and driving `handle_bytes` / `handle_timeout` with injected
//! `Instant`s (mirrors `virtual_clock_send_timeout.rs`) — keeps
//! `cargo xtask check-runtime-test-parity` 1:1 (ADR-0024) without requiring
//! a real host-clock wait on that side.

#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, FrameError, MessageId, SubscribeRequest, decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::{Client, ClientError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod common;
use common::HANG_GUARD;

fn ack_message_id() -> MessageId {
    MessageId {
        ledger_id: 1,
        entry_id: 1,
        partition: -1,
        batch_index: -1,
        batch_size: -1,
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
    }
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-ack-orphan-close".to_owned(),
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

fn emit_close_consumer(out: &mut BytesMut, consumer_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::CloseConsumer as i32,
        close_consumer: Some(pb::CommandCloseConsumer {
            consumer_id,
            request_id: 0,
            assigned_broker_service_url: None,
            assigned_broker_service_url_tls: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Broker session for scenario 1: answers CONNECT / SUBSCRIBE normally; the
/// FIRST `CommandAck` it sees gets a same-broker `CommandCloseConsumer`
/// instead of an ack response (orphaning it); any subsequent `CommandSubscribe`
/// (the client's in-place re-attach) is answered with `Success` too.
async fn run_orphan_close_broker_conn(stream: &mut tokio::net::TcpStream) {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out_buf = BytesMut::with_capacity(8 * 1024);
    let closed_once = AtomicBool::new(false);
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
                pb::base_command::Type::Ack => {
                    if let Some(a) = &frame.command.ack {
                        if !closed_once.swap(true, Ordering::SeqCst) {
                            emit_close_consumer(&mut out_buf, a.consumer_id);
                        }
                        // Any subsequent ack (there should be none in this
                        // test) is silently dropped — not the scenario under
                        // test.
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

async fn spawn_orphan_close_broker() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _peer)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                run_orphan_close_broker_conn(&mut stream).await;
            });
        }
    });
    format!("pulsar://{addr}")
}

/// Broker session for scenario 2: answers CONNECT / SUBSCRIBE normally but
/// NEVER responds to `CommandAck` — the `ack_response_timeout` backstop is
/// the gate this test exercises (mirrors `handle_session` in
/// `virtual_clock_driver_loop.rs`, which does the same for `CommandSend`).
async fn run_silent_ack_broker_conn(stream: &mut tokio::net::TcpStream) {
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
                // CommandAck observed but deliberately never answered.
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

async fn spawn_silent_ack_broker() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _peer)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                run_silent_ack_broker_conn(&mut stream).await;
            });
        }
    });
    format!("pulsar://{addr}")
}

/// Scenario 1: same-broker `CloseConsumer` orphans a pending ack — the
/// close-handler sweep must fail it fast instead of hanging the caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ack_orphan_close_fails_fast_after_same_broker_close() {
    let url = spawn_orphan_close_broker().await;
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
            topic: "persistent://public/default/ack-orphan-close".to_owned(),
            subscription: "ack-orphan-close".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");

    let result = tokio::time::timeout(HANG_GUARD, consumer.ack(ack_message_id()))
        .await
        .expect(
            "ack must resolve promptly via the orphan sweep, not hang until \
             ack_response_timeout",
        );

    match result {
        Err(ClientError::Broker { code, message }) => {
            assert_eq!(code, -1, "orphaned-ack uses the -1 sentinel");
            assert_eq!(message, "ack orphaned by broker consumer close");
        }
        other => panic!("expected an orphaned-ack Broker error, got {other:?}"),
    }
}

/// Scenario 2: `ack_response_timeout` backstop fires over the real host
/// clock when the broker never responds to `CommandAck` at all (no
/// `CloseConsumer` in play — the generic deadline is the only thing that
/// can resolve this).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ack_response_timeout_fires_against_host_clock() {
    const ACK_RESPONSE_TIMEOUT: Duration = Duration::from_millis(200);

    let url = spawn_silent_ack_broker().await;
    let config = ConnectionConfig {
        ack_response_timeout: Some(ACK_RESPONSE_TIMEOUT),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(HANG_GUARD, Client::connect(&url, config))
        .await
        .expect("connect did not time out")
        .expect("connect ok");

    let consumer = tokio::time::timeout(
        HANG_GUARD,
        client.subscribe(SubscribeRequest {
            topic: "persistent://public/default/ack-response-timeout".to_owned(),
            subscription: "ack-response-timeout".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");

    // The broker never responds to CommandAck, so the deadline must fire.
    // Wrap in an outer HANG_GUARD well above ACK_RESPONSE_TIMEOUT so a
    // regression surfaces as a `tokio::time::error::Elapsed` rather than
    // hanging the suite.
    let result = tokio::time::timeout(HANG_GUARD, consumer.ack(ack_message_id()))
        .await
        .expect("ack did not time out at the ack_response_timeout budget — driver-loop regression");

    match result {
        Err(ClientError::Broker { code, message }) => {
            assert_eq!(code, -1, "ack-timeout uses the -1 sentinel");
            assert_eq!(message, "ack timeout");
        }
        other => panic!("expected an ack-timeout Broker error, got {other:?}"),
    }

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
}
