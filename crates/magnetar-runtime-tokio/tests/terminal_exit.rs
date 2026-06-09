// SPDX-License-Identifier: Apache-2.0

//! Plain-connection terminal fail-fast — tokio engine, real loopback broker
//! (ADR-0024 layer b for the ADR-0055 §1 terminal-fail-fast fix). 1:1 twin of
//! `crates/magnetar-runtime-moonpool/tests/terminal_exit.rs`
//! (ADR-0024 runtime-test-parity).
//!
//! The scenario reproduces the no-progress stall ADR-0055 §1 kills: an
//! UNSUPERVISED (plain) client with in-flight `subscribe()` + `send()`
//! futures when the broker connection drops terminally. Before the fix, the
//! plain driver exited on the drop and left those futures parked forever (the
//! moonpool no-progress stall that surfaced as the swizzle-clog seed-replay
//! regression). After the fix, `Connection::fail_all_pending` resolves every
//! pending op so each future returns a terminal error PROMPTLY.
//!
//! Script:
//!
//! 1. connect → `CommandConnected`; lookup → use-current; producer open → `CommandProducerSuccess`
//!    (the healthy warm-up so we have a live producer + consumer handle);
//! 2. the client issues `subscribe()` and `send()` concurrently; on the FIRST data-plane frame it
//!    sees after the producer open (the `CommandSubscribe` or `CommandSend`), the broker drops the
//!    socket WITHOUT acking — a terminal peer close;
//! 3. both in-flight futures must resolve with a terminal `ClientError` (the subscribe with
//!    `PeerClosed` via the `ConnectionEvent::Closed { reason }` waiter unblock; the send with
//!    `PeerClosed` via `OpOutcome::Terminal`) — and they must resolve PROMPTLY, which the
//!    `tokio::time::timeout` wrappers enforce: a hang trips the timeout and fails the test.
//!
//! No supervisor is configured (default `ConnectionConfig`), so the driver
//! takes the plain terminal-exit path rather than reconnecting.
//!
//! A second test (`new_ops_after_terminal_drop_fail_fast`) extends the
//! scenario to ADR-0059: a `send()` / `subscribe()` / `producer.close()`
//! issued AFTER the plain connection is already terminal must fast-fail
//! SYNCHRONOUSLY with `PeerClosed` (via the `no_driver` latch + slot-close)
//! rather than register a doomed pending op that no driver is left to resolve.
//! 1:1 twin of the moonpool engine's same-named test.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, Frame, FrameError, SubscribeRequest, decode_one,
    encode_command, pb,
};
use magnetar_runtime_tokio::{Client, ClientError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn outgoing(payload: &'static [u8]) -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(payload),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: payload.len() as u32,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "terminal-exit-broker/0".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
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

fn emit_producer_success(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: "terminal-exit-producer".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_pong(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Pong as i32,
        pong: Some(pb::CommandPong {}),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Scripted broker: completes the handshake + producer open, then DROPS the
/// socket on the first `CommandSubscribe` / `CommandSend` it sees — a terminal
/// peer close with no ack. `data_plane_seen` flips so the test can assert the
/// drop fired on an in-flight op (not before).
async fn run_terminal_broker(listener: TcpListener, data_plane_seen: Arc<AtomicBool>) {
    let Ok((mut stream, _)) = listener.accept().await else {
        return;
    };
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    loop {
        let mut drop_now = false;
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame: Frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return,
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            match pb::base_command::Type::try_from(frame.command.r#type) {
                Ok(pb::base_command::Type::Connect) => emit_connected(&mut out_buf),
                Ok(pb::base_command::Type::Lookup) => {
                    if let Some(l) = &frame.command.lookup_topic {
                        emit_lookup_response(&mut out_buf, l.request_id);
                    }
                }
                Ok(pb::base_command::Type::Producer) => {
                    if let Some(p) = &frame.command.producer {
                        emit_producer_success(&mut out_buf, p.request_id);
                    }
                }
                Ok(pb::base_command::Type::Ping) => emit_pong(&mut out_buf),
                Ok(pb::base_command::Type::Subscribe | pb::base_command::Type::Send) => {
                    // Terminal peer close on the first in-flight data-plane op:
                    // drop the socket WITHOUT acking. Discard any pending
                    // replies — the point is that the peer vanished mid-op.
                    data_plane_seen.store(true, Ordering::SeqCst);
                    drop_now = true;
                    break;
                }
                _ => {}
            }
        }

        if drop_now {
            // Close the connection (drop the stream) to surface a terminal
            // peer close (read returns 0) on the client's driver.
            return;
        }

        if !out_buf.is_empty() {
            if stream.write_all(&out_buf).await.is_err() {
                return;
            }
            let _ = stream.flush().await;
            out_buf.clear();
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_connection_in_flight_ops_fail_fast_on_terminal_drop() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let url = format!("pulsar://{addr}");
    let data_plane_seen = Arc::new(AtomicBool::new(false));
    tokio::spawn(run_terminal_broker(listener, Arc::clone(&data_plane_seen)));

    // Default config = NO supervisor → the driver takes the plain
    // terminal-exit path (no reconnect). This is the connection shape
    // ADR-0055 §1 hardens.
    let config = ConnectionConfig::default();
    assert!(
        config.supervisor.is_none(),
        "this test pins the UNSUPERVISED plain path",
    );

    let client = tokio::time::timeout(Duration::from_secs(5), Client::connect(&url, config))
        .await
        .expect("connect did not time out")
        .expect("connect must succeed");

    let producer = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/terminal-exit".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("producer open did not time out")
    .expect("producer open must succeed");

    // Issue subscribe + send CONCURRENTLY. The broker drops the socket on the
    // first of these data-plane frames it sees; both in-flight futures must
    // then resolve with a terminal error PROMPTLY (the timeout enforces "no
    // hang" — the regression this test guards against was an infinite park).
    let subscribe_fut = client.subscribe(SubscribeRequest {
        topic: "persistent://public/default/terminal-exit".to_owned(),
        subscription: "sub-terminal-exit".to_owned(),
        receiver_queue_size: 16,
        durable: true,
        ..Default::default()
    });
    let send_fut = producer.send(outgoing(b"in-flight-when-peer-dies"));

    let (sub_res, send_res) = tokio::time::timeout(Duration::from_secs(10), async move {
        tokio::join!(subscribe_fut, send_fut)
    })
    .await
    .expect("in-flight subscribe + send must resolve promptly after the terminal drop, not hang");

    assert!(
        data_plane_seen.load(Ordering::SeqCst),
        "the broker must have dropped on an in-flight data-plane op (sanity: the terminal \
         exit fired mid-op, not during the handshake)",
    );
    assert!(
        matches!(sub_res, Err(ClientError::PeerClosed)),
        "in-flight subscribe must surface the terminal PeerClosed, got {sub_res:?}",
    );
    assert!(
        matches!(send_res, Err(ClientError::PeerClosed)),
        "in-flight send must surface the terminal PeerClosed, got {send_res:?}",
    );

    // The client itself reports the connection as no longer live.
    assert!(
        !client.is_connected(),
        "connection must be down after the terminal drop"
    );
}

/// ADR-0059 / follow-ups §4.1: a NEW op issued AFTER the plain connection has
/// gone terminal must fast-fail SYNCHRONOUSLY with `PeerClosed`, not register a
/// doomed pending op no driver is left to resolve. This is the new-op companion
/// to the in-flight contract above (ADR-0055 §1).
///
/// Script: drive the connection terminal exactly as the test above (one
/// in-flight send the broker drops on), wait for the in-flight send to resolve
/// (so the plain driver has run `fail_all_pending` + latched `no_driver`), then
/// issue a fresh `send()` on the same producer, a fresh `subscribe()`, and a
/// fresh `producer.close()`. Each must return `PeerClosed` — and PROMPTLY: the
/// tight `timeout` wrappers are the no-hang guard, since the regression is that
/// a post-terminal op registers and never resolves. 1:1 twin of the moonpool
/// engine's `new_ops_after_terminal_drop_fail_fast`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_ops_after_terminal_drop_fail_fast() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let url = format!("pulsar://{addr}");
    let data_plane_seen = Arc::new(AtomicBool::new(false));
    tokio::spawn(run_terminal_broker(listener, Arc::clone(&data_plane_seen)));

    let config = ConnectionConfig::default();
    assert!(
        config.supervisor.is_none(),
        "this test pins the UNSUPERVISED plain path",
    );

    let client = tokio::time::timeout(Duration::from_secs(5), Client::connect(&url, config))
        .await
        .expect("connect did not time out")
        .expect("connect must succeed");

    let producer = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/terminal-exit-newop".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("producer open did not time out")
    .expect("producer open must succeed");

    // One in-flight send drives the terminal drop; await it so the plain driver
    // has run `fail_all_pending` (slot closed) and latched `no_driver` before we
    // issue the fresh ops.
    let drop_res = tokio::time::timeout(
        Duration::from_secs(10),
        producer.send(outgoing(b"in-flight-trigger")),
    )
    .await
    .expect("the in-flight send must resolve promptly on the terminal drop, not hang");
    assert!(
        data_plane_seen.load(Ordering::SeqCst),
        "the broker must have dropped on the in-flight data-plane op",
    );
    assert!(
        matches!(drop_res, Err(ClientError::PeerClosed)),
        "the in-flight send surfaces the terminal PeerClosed, got {drop_res:?}",
    );
    assert!(
        !client.is_connected(),
        "connection must be terminal before the new-op assertions",
    );

    // (1) A FRESH send on the same producer fast-fails with PeerClosed: the
    // slot is `closed` and `no_driver` is latched, so the producer-send arm maps
    // the proto-layer rejection to PeerClosed without registering anything.
    let send_after = tokio::time::timeout(
        Duration::from_secs(5),
        producer.send(outgoing(b"after-terminal")),
    )
    .await
    .expect("a post-terminal send must fast-fail synchronously, not hang");
    assert!(
        matches!(send_after, Err(ClientError::PeerClosed)),
        "post-terminal send must fast-fail with PeerClosed, got {send_after:?}",
    );

    // (2) A FRESH subscribe fast-fails at the entry-point guard
    // (`fail_if_no_driver`: is_closed() AND no_driver), never parking a doomed
    // `CommandSubscribe`.
    let subscribe_after = tokio::time::timeout(
        Duration::from_secs(5),
        client.subscribe(SubscribeRequest {
            topic: "persistent://public/default/terminal-exit-newop".to_owned(),
            subscription: "sub-after-terminal".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("a post-terminal subscribe must fast-fail synchronously, not hang");
    assert!(
        matches!(subscribe_after, Err(ClientError::PeerClosed)),
        "post-terminal subscribe must fast-fail with PeerClosed, got {subscribe_after:?}",
    );

    // (3) A FRESH producer.close() fast-fails at its `fail_if_no_driver` guard
    // instead of registering a `CommandCloseProducer` no driver can resolve.
    let close_after = tokio::time::timeout(Duration::from_secs(5), producer.close())
        .await
        .expect("a post-terminal producer.close() must fast-fail synchronously, not hang");
    assert!(
        matches!(close_after, Err(ClientError::PeerClosed)),
        "post-terminal producer.close() must fast-fail with PeerClosed, got {close_after:?}",
    );
}
