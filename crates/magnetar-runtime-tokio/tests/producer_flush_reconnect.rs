// SPDX-License-Identifier: Apache-2.0

//! `Producer::flush` must not return while a send is still unacknowledged because a reconnect
//! relocated it — tokio engine, real loopback broker. 1:1 twin of
//! `crates/magnetar-runtime-moonpool/tests/producer_flush_reconnect.rs` (ADR-0024 runtime-test
//! parity).
//!
//! `Connection::reset()` moves every replayable in-flight publish out of the producer's `pending`
//! queue into the reset snapshot bucket. The sends return to `pending` only when the broker
//! acknowledges the re-attachment (`CommandProducerSuccess`) on the new session. `flush` is
//! documented as "wait for every in-flight send to be acknowledged by the broker" and as the call
//! to make before `close()` for at-least-once semantics on the trailing sends, yet it loops on
//! `pending.len()` alone: while the sends sit in the snapshot bucket it reads `0` and resolves
//! `Ok(())` with no send acknowledged.
//!
//! Scenario: a send reaches the broker and the socket drops before any receipt; the supervisor
//! redials, the broker answers the handshake and then WITHHOLDS the re-attachment's
//! `CommandProducerSuccess`; `flush()` is called inside that window. It must stay pending until the
//! replayed send's receipt has arrived.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, SupervisorConfig, decode_one,
    encode_command, pb,
};
use magnetar_runtime_tokio::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

mod common;
use common::HANG_GUARD;

/// How long `flush` is observed while it must stay pending. The defect resolves it on its first
/// poll, so this window only has to be long enough for any wake-up to have been delivered; it is
/// not a timing assertion on the fixed behaviour.
const STILL_PENDING_WINDOW: Duration = Duration::from_millis(500);

/// Script state shared between the broker task and the test body.
#[derive(Default)]
struct Script {
    /// The first session received the send and dropped without a receipt.
    first_session_send_seen: AtomicBool,
    /// The second session received the re-attachment's `CommandProducer`.
    reattach_seen: AtomicBool,
    /// Releases the withheld `CommandProducerSuccess` of the second session.
    release_reattach_ack: Notify,
    /// `CommandSend` frames the second session answered with a receipt.
    replayed_sends: AtomicU32,
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "flush-reconnect-broker/0".to_owned(),
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
            producer_name: "flush-reconnect-producer".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_send_receipt(out: &mut BytesMut, producer_id: u64, sequence_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::SendReceipt as i32,
        send_receipt: Some(pb::CommandSendReceipt {
            producer_id,
            sequence_id,
            message_id: Some(pb::MessageIdData {
                ledger_id: 7,
                entry_id: sequence_id,
                partition: None,
                batch_index: None,
                ack_set: vec![],
                batch_size: None,
                first_chunk_message_id: None,
            }),
            highest_sequence_id: None,
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

/// Serve one session. `second_session` selects the script: the first session acknowledges
/// everything except the send, on which it drops the socket; the second one holds the
/// re-attachment's `CommandProducerSuccess` until the test releases it, then answers every send
/// with a receipt.
async fn serve_session(stream: &mut TcpStream, script: &Script, second_session: bool) {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
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
            let mut out = BytesMut::new();
            match pb::base_command::Type::try_from(frame.command.r#type) {
                Ok(pb::base_command::Type::Connect) => emit_connected(&mut out),
                Ok(pb::base_command::Type::Ping) => emit_pong(&mut out),
                Ok(pb::base_command::Type::Lookup) => {
                    if let Some(l) = &frame.command.lookup_topic {
                        emit_lookup_response(&mut out, l.request_id);
                    }
                }
                Ok(pb::base_command::Type::Producer) => {
                    if let Some(p) = &frame.command.producer {
                        if second_session {
                            script.reattach_seen.store(true, Ordering::SeqCst);
                            script.release_reattach_ack.notified().await;
                        }
                        emit_producer_success(&mut out, p.request_id);
                    }
                }
                Ok(pb::base_command::Type::Send) => {
                    if let Some(send) = &frame.command.send {
                        if !second_session {
                            // The send is on the wire; drop without a receipt.
                            script.first_session_send_seen.store(true, Ordering::SeqCst);
                            return;
                        }
                        script.replayed_sends.fetch_add(1, Ordering::SeqCst);
                        emit_send_receipt(&mut out, send.producer_id, send.sequence_id);
                    }
                }
                _ => {}
            }
            if !out.is_empty() {
                if stream.write_all(&out).await.is_err() {
                    return;
                }
                let _ = stream.flush().await;
            }
        }
        match stream.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

async fn run_broker(listener: TcpListener, script: Arc<Script>) {
    let Ok((mut first, _)) = listener.accept().await else {
        return;
    };
    serve_session(&mut first, &script, false).await;
    drop(first);

    let Ok((mut second, _)) = listener.accept().await else {
        return;
    };
    serve_session(&mut second, &script, true).await;
}

/// Poll `condition` until it holds, bounded by [`HANG_GUARD`].
async fn wait_until(what: &str, condition: impl Fn() -> bool) {
    tokio::time::timeout(HANG_GUARD, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for: {what}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_stays_pending_while_the_reset_snapshot_holds_an_unacknowledged_send() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let script = Arc::new(Script::default());
    tokio::spawn(run_broker(listener, Arc::clone(&script)));

    let config = ConnectionConfig {
        supervisor: Some(SupervisorConfig {
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(100),
            ..Default::default()
        }),
        ..Default::default()
    };
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&format!("pulsar://{addr}"), config),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed");
    let producer = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/flush-reconnect".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("producer open did not time out")
    .expect("producer open must succeed");

    // One send reaches the broker; the socket drops before any receipt.
    let send = tokio::spawn({
        let producer = producer.clone();
        async move { producer.send_bytes(&b"in-flight"[..]).await }
    });
    wait_until("the first session to receive the send", || {
        script.first_session_send_seen.load(Ordering::SeqCst)
    })
    .await;

    // The supervisor reset the connection, redialled and re-handshaked: the re-attachment's
    // `CommandProducer` is on the wire and its ack is withheld, so the send is parked in the
    // reset snapshot bucket, in no live queue.
    wait_until("the second session to receive the re-attachment", || {
        script.reattach_seen.load(Ordering::SeqCst)
    })
    .await;

    let mut flush = tokio::spawn({
        let producer = producer.clone();
        async move { producer.flush().await }
    });
    let early = tokio::time::timeout(STILL_PENDING_WINDOW, &mut flush).await;
    assert!(
        early.is_err(),
        "flush() returned {early:?} while the replayed send was still unacknowledged: the send \
         sits in the reset snapshot bucket, out of `pending`, until the re-attachment is acked \
         (replayed sends so far: {})",
        script.replayed_sends.load(Ordering::SeqCst),
    );

    // Release the re-attachment ack: the snapshot is replayed, the receipt arrives, and only then
    // may flush resolve.
    script.release_reattach_ack.notify_one();
    tokio::time::timeout(HANG_GUARD, flush)
        .await
        .expect("flush must resolve once the replayed send is acknowledged")
        .expect("flush task must not panic")
        .expect("flush must succeed");
    assert_eq!(
        script.replayed_sends.load(Ordering::SeqCst),
        1,
        "flush resolved before the replayed send reached the broker and was acknowledged"
    );
    tokio::time::timeout(HANG_GUARD, send)
        .await
        .expect("the replayed send must resolve")
        .expect("send task must not panic")
        .expect("the replayed send must be acknowledged");

    client.close().await;
}
