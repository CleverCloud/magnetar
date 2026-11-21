// SPDX-License-Identifier: Apache-2.0

#![forbid(unsafe_code)]

//! A user `close()` that races a write the broker is not draining must stay a
//! user close.
//!
//! The driver keeps writing while bytes are queued, so a close issued while a
//! publish is parked mid-write waits for that write. The write deadline
//! (`operation_timeout`, ADR-0083) then fires and the driver maps it through
//! `mark_disconnected()`. That used to overwrite `Closing` with `Failed`, which
//! the supervisor reads as a transport drop: it redialled (a second broker
//! accept), re-attached the producer, replayed the publish, and
//! `Client::close().await` never returned.
//!
//! Tokio mirror of `close_during_stalled_write_is_not_redialled_sim` in
//! `magnetar-runtime-moonpool/tests/driver_write_deadline.rs`. The moonpool
//! engine stalls the write through the simulator's 64 KiB send buffer; here a
//! real loopback peer stops reading and the payload exceeds every buffer the
//! kernel can put between the two ends.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, SupervisorConfig, decode_one,
    encode_command, pb,
};
use magnetar_runtime_tokio::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Far above the kernel's loopback buffering (send buffer plus the receive
/// window of a socket that never reads), so the write cannot drain.
const STALLED_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// The broker's advertised `max_message_size`, above [`STALLED_PAYLOAD_BYTES`].
const BROKER_MAX_MESSAGE_SIZE: i32 = 32 * 1024 * 1024;

fn emit(out: &mut BytesMut, cmd: &pb::BaseCommand) {
    let _ = encode_command(out, cmd);
}

fn emit_connected(out: &mut BytesMut) {
    emit(
        out,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-close-stalled-write".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(BROKER_MAX_MESSAGE_SIZE),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        },
    );
}

fn emit_pong(out: &mut BytesMut) {
    emit(
        out,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Pong as i32,
            pong: Some(pb::CommandPong {}),
            ..Default::default()
        },
    );
}

fn emit_lookup_response(out: &mut BytesMut, request_id: u64) {
    emit(
        out,
        &pb::BaseCommand {
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
        },
    );
}

fn emit_producer_success(out: &mut BytesMut, request_id: u64) {
    emit(
        out,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id,
                producer_name: "close-stalled-write".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: Some(0),
                producer_ready: Some(true),
            }),
            ..Default::default()
        },
    );
}

/// Answer CONNECT / PING / LOOKUP, ack the first `CommandProducer`, then stop
/// reading entirely: the socket stays open (no EOF, no reset), so the stall is
/// one-directional and only a write deadline can detect it.
async fn handle_session_then_stop_reading(mut stream: TcpStream) {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    let mut producer_opened = false;
    loop {
        loop {
            let mut framed = ToOwned::to_owned(&read_buf).freeze();
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
                pb::base_command::Type::Ping => emit_pong(&mut out_buf),
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        emit_lookup_response(&mut out_buf, l.request_id);
                    }
                }
                pb::base_command::Type::Producer => {
                    if let Some(p) = &frame.command.producer {
                        emit_producer_success(&mut out_buf, p.request_id);
                        producer_opened = true;
                    }
                }
                _ => {}
            }
        }

        if !out_buf.is_empty() {
            if stream.write_all(&out_buf).await.is_err() || stream.flush().await.is_err() {
                return;
            }
            out_buf.clear();
        }

        if producer_opened {
            std::future::pending::<()>().await;
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

/// A closed client must not redial the broker, and `close()` must return once
/// the stalled write's deadline fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_during_stalled_write_is_not_redialled() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("pulsar://{}", listener.local_addr().expect("address"));
    let sessions = Arc::new(AtomicU32::new(0));
    let broker = {
        let sessions = ToOwned::to_owned(&sessions);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                sessions.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(handle_session_then_stop_reading(stream));
            }
        })
    };
    let config = ConnectionConfig {
        operation_timeout: Duration::from_millis(700),
        supervisor: Some(SupervisorConfig {
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            ..SupervisorConfig::default()
        }),
        ..ConnectionConfig::default()
    };
    let client = Client::connect(&url, config)
        .await
        .expect("initial handshake");
    let producer = client
        .open_producer(CreateProducerRequest {
            topic: "persistent://public/default/close-during-stalled-write".to_owned(),
            ..Default::default()
        })
        .await
        .expect("open producer");
    let shared = ToOwned::to_owned(client.shared());

    // Park the publish mid-write: the broker has stopped reading.
    let send_resolved = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let send_fut = producer.send(OutgoingMessage {
        payload: Bytes::from(vec![0xABu8; STALLED_PAYLOAD_BYTES]),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: u32::try_from(STALLED_PAYLOAD_BYTES).unwrap_or(u32::MAX),
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    });
    let stalled_send = {
        let send_resolved = ToOwned::to_owned(&send_resolved);
        tokio::spawn(async move {
            let _ = send_fut.await;
            send_resolved.store(true, Ordering::SeqCst);
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    // Still the first session: the write deadline has not fired (and been
    // redialled) before the close, which would leave nothing to race.
    let stalled_at_close = shared.inner.lock().is_connected()
        && !send_resolved.load(Ordering::SeqCst)
        && sessions.load(Ordering::SeqCst) == 1;

    // Exactly what the application does.
    let mut close_task = tokio::spawn(client.close());
    let closed = tokio::time::timeout(Duration::from_secs(10), &mut close_task).await;
    let observed_sessions = sessions.load(Ordering::SeqCst);
    let user_closed = shared.inner.lock().is_user_closed();
    close_task.abort();
    stalled_send.abort();
    broker.abort();
    let _ = broker.await;
    assert!(
        stalled_at_close,
        "precondition: the publish must be parked mid-write on the first, still \
         connected session when the user closes"
    );
    assert!(
        closed.is_ok(),
        "a user close must complete once the stalled write's deadline fires; instead the \
         write-deadline `mark_disconnected()` demoted `Closing` to `Failed` and the \
         supervisor resurrected the session (sessions_accepted={observed_sessions}, \
         user_closed={user_closed})"
    );
    assert_eq!(
        observed_sessions, 1,
        "a closed client must not redial the broker: the supervisor must stop on the \
         user's close, not treat the write-deadline failure as a drop"
    );
    assert!(
        user_closed,
        "the connection must still report the user's close"
    );
}
