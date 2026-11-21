// SPDX-License-Identifier: Apache-2.0

//! `Producer::flush` must not return while a send is still unacknowledged because a reconnect
//! relocated it, identically on both engines (ADR-0024 layer (d)).
//!
//! `Connection::reset()` moves every replayable in-flight publish out of the producer's `pending`
//! queue into the reset snapshot bucket; the sends return to `pending` only when the broker
//! acknowledges the re-attachment on the new session. Both engines' `flush` loop on the producer's
//! `pending` count alone, so inside that window they resolve `Ok(())` with no send acknowledged,
//! although `flush` is documented as waiting for every in-flight send to be acknowledged.
//!
//! The scenario is narrow (a broker that receives a send, drops the socket without a receipt and
//! then withholds the re-attachment's `CommandProducerSuccess`), so it keeps a small local
//! loopback broker instead of extending the shared `ScriptedBroker` and trace model: both engines
//! are driven through their public `Client` against a fresh instance of it and the observations
//! are compared. The moonpool leg runs over `TokioProviders` on a real loopback socket, like the
//! other loopback scenarios of this crate; the deterministic-simulation leg lives in
//! `crates/magnetar-runtime-moonpool/tests/producer_flush_reconnect.rs`.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, SupervisorConfig, decode_one,
    encode_command, pb,
};
use moonpool_core::TokioProviders;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

/// Wall-clock anti-hang backstop; mirrors `magnetar_differential::HANG_GUARD`.
const HANG_GUARD: Duration = magnetar_differential::HANG_GUARD;

/// How long `flush` is observed while it must stay pending. The defect resolves it on its first
/// poll, so this window only has to be long enough for any wake-up to have been delivered.
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

/// What one engine leg observed.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    /// `flush()` resolved inside the withheld-ack window.
    flush_resolved_in_window: bool,
    /// Replayed sends the broker had answered when `flush()` resolved.
    replayed_sends_at_flush_resolution: u32,
    /// `flush()` resolved with `Ok`.
    flush_ok: bool,
    /// The replayed send resolved with `Ok`.
    send_ok: bool,
}

/// The outcome both engines must report: `flush` waits for the replayed send's receipt.
const EXPECTED: Outcome = Outcome {
    flush_resolved_in_window: false,
    replayed_sends_at_flush_resolution: 1,
    flush_ok: true,
    send_ok: true,
};

/// Drive one leg: the send reaches the broker and is dropped without a receipt, the producer is
/// re-attached on the redialled session with its ack withheld, `flush` is started inside that
/// window, then the ack is released.
async fn observe<S, F>(script: &Arc<Script>, send: S, flush: F) -> Outcome
where
    S: Future<Output = bool> + Send + 'static,
    F: Future<Output = bool> + Send + 'static,
{
    let send = tokio::spawn(send);
    wait_until("the first session to receive the send", || {
        script.first_session_send_seen.load(Ordering::SeqCst)
    })
    .await;
    wait_until("the second session to receive the re-attachment", || {
        script.reattach_seen.load(Ordering::SeqCst)
    })
    .await;

    let mut flush = tokio::spawn(flush);
    let early = tokio::time::timeout(STILL_PENDING_WINDOW, &mut flush).await;
    let replayed_if_early = script.replayed_sends.load(Ordering::SeqCst);
    script.release_reattach_ack.notify_one();

    let flush_resolved_in_window = early.is_ok();
    let (flush_ok, replayed_sends_at_flush_resolution) = if let Ok(joined) = early {
        (joined.expect("flush task"), replayed_if_early)
    } else {
        let ok = tokio::time::timeout(HANG_GUARD, flush)
            .await
            .expect("flush must resolve once the replayed send is acknowledged")
            .expect("flush task");
        (ok, script.replayed_sends.load(Ordering::SeqCst))
    };
    let send_ok = tokio::time::timeout(HANG_GUARD, send)
        .await
        .expect("the replayed send must resolve")
        .expect("send task");
    Outcome {
        flush_resolved_in_window,
        replayed_sends_at_flush_resolution,
        flush_ok,
        send_ok,
    }
}

fn supervised() -> ConnectionConfig {
    ConnectionConfig {
        supervisor: Some(SupervisorConfig {
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(100),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn request() -> CreateProducerRequest {
    CreateProducerRequest {
        topic: "persistent://public/default/flush-reconnect-diff".to_owned(),
        ..Default::default()
    }
}

fn message() -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(b"in-flight"),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: 9,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

async fn spawn_broker() -> (String, Arc<Script>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr").to_string();
    let script = Arc::new(Script::default());
    tokio::spawn(run_broker(listener, Arc::clone(&script)));
    (addr, script)
}

async fn tokio_leg() -> Outcome {
    let (addr, script) = spawn_broker().await;
    let client = tokio::time::timeout(
        HANG_GUARD,
        magnetar_runtime_tokio::Client::connect(&format!("pulsar://{addr}"), supervised()),
    )
    .await
    .expect("tokio connect did not time out")
    .expect("tokio connect");
    let producer = tokio::time::timeout(HANG_GUARD, client.open_producer(request()))
        .await
        .expect("tokio producer open did not time out")
        .expect("tokio producer open");
    let outcome = observe(
        &script,
        {
            let producer = producer.clone();
            async move { producer.send(message()).await.is_ok() }
        },
        {
            let producer = producer.clone();
            async move { producer.flush().await.is_ok() }
        },
    )
    .await;
    client.close().await;
    outcome
}

async fn moonpool_leg() -> Outcome {
    let (addr, script) = spawn_broker().await;
    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let client = tokio::time::timeout(
        HANG_GUARD,
        magnetar_runtime_moonpool::Client::connect_plain_supervised(
            &engine,
            &addr,
            supervised(),
            None,
            None,
        ),
    )
    .await
    .expect("moonpool connect did not time out")
    .expect("moonpool connect");
    let producer = tokio::time::timeout(HANG_GUARD, client.open_producer(request()))
        .await
        .expect("moonpool producer open did not time out")
        .expect("moonpool producer open");
    let outcome = observe(
        &script,
        {
            let producer = producer.clone();
            async move { producer.send(message()).await.is_ok() }
        },
        {
            let producer = producer.clone();
            async move { producer.flush().await.is_ok() }
        },
    )
    .await;
    client.close().await;
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_waits_for_the_replayed_send_event_stream_parity() {
    let tokio_outcome = tokio_leg().await;
    let moonpool_outcome = moonpool_leg().await;
    assert_eq!(
        tokio_outcome, moonpool_outcome,
        "engines must agree on when flush() resolves across a reconnect"
    );
    assert_eq!(
        tokio_outcome, EXPECTED,
        "flush() must stay pending while the replayed send is unacknowledged in the reset \
         snapshot bucket and resolve once its receipt arrived (both engines report {tokio_outcome:?})"
    );
}
