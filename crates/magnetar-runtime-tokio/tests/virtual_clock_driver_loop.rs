// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::too_many_lines)]

//! Tokio mirror of the moonpool deterministic-simulation fixture
//! `crates/magnetar-runtime-moonpool/tests/virtual_clock_driver_loop.
//! rs::driver_loop_send_timeout_fires_against_virtual_clock`. Maintains the
//! tokio ↔ moonpool 1:1 test count required by ADR-0024.
//!
//! Both sides drive the production driver loop's `Connection::handle_timeout`
//! tick to enforce `send_timeout` end-to-end. The moonpool engine is the
//! canonical place for the *virtual-clock* assertion (the fix under test
//! routes the driver's `now` through `ConnectionShared::now_instant`, so
//! deterministic-sim runs see the deadline against virtual time). The tokio
//! engine reads the host clock at the same call boundary by design — this
//! mirror asserts the send-timeout sentinel still arrives over a real
//! loopback socket, i.e. that the matching code path on production tokio
//! stayed regression-free across the moonpool refactor.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::{Client, ClientError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const SEND_TIMEOUT_MS: u64 = 600;

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-virtual-clock-driver".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
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
            producer_name: "magnetar-virtual-clock-driver".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Broker session: replies to CONNECT / LOOKUP / PRODUCER opens but
/// **never** responds to SEND. The send-timeout path is the gate the test
/// exercises.
async fn handle_session(
    mut stream: tokio::net::TcpStream,
    sends_observed: Arc<AtomicU32>,
) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return Ok(()),
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
                    }
                }
                pb::base_command::Type::Send => {
                    // Observe but DO NOT respond. The send-timeout path is
                    // the gate this test exercises.
                    sends_observed.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }
        }

        if !out_buf.is_empty() {
            stream.write_all(&out_buf).await?;
            stream.flush().await?;
            out_buf.clear();
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(e) => return Err(e),
        }
    }
}

async fn spawn_send_timeout_broker() -> (String, Arc<AtomicU32>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let sends_observed = Arc::new(AtomicU32::new(0));
    let sends_for_task = sends_observed.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            let sends_clone = sends_for_task.clone();
            tokio::spawn(async move {
                let _ = handle_session(stream, sends_clone).await;
            });
        }
    });
    (format!("pulsar://{addr}"), sends_observed)
}

/// Drive the tokio engine's production driver loop through a
/// `send_timeout` cycle against a real loopback broker. The broker never
/// replies to `CommandSend`; the client-side `send_timeout` enforcement
/// (driven by `Connection::handle_timeout` on every driver-loop tick) must
/// fail the send within the configured window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn driver_loop_send_timeout_fires_against_host_clock() {
    let (url, sends_observed) = spawn_send_timeout_broker().await;

    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let producer = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/virtual-clock-driver".to_owned(),
            send_timeout: Some(Duration::from_millis(SEND_TIMEOUT_MS)),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out")
    .expect("open_producer ok");

    // The broker never responds to SEND, so the deadline must fire.
    // Wrap in an outer tokio timeout that's well above SEND_TIMEOUT_MS so
    // a regression surfaces as a `tokio::time::error::Elapsed` rather
    // than hanging the suite.
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        producer.send_bytes(Bytes::from_static(b"will-time-out")),
    )
    .await
    .expect("send did not time out at the host-budget level — driver-loop regression");

    match result {
        Err(
            ClientError::SendRejected { code, message } | ClientError::Broker { code, message },
        ) => {
            assert_eq!(code, -1, "Pulsar timeout sentinel is -1");
            assert!(
                message.to_lowercase().contains("timeout"),
                "expected timeout message, got {message:?}"
            );
        }
        Ok(_) => panic!("send returned Ok despite broker never responding to CommandSend"),
        Err(other) => panic!("expected timeout sentinel, got {other:?}"),
    }

    let observed = sends_observed.load(Ordering::SeqCst);
    assert!(
        observed >= 1,
        "broker must have seen at least one CommandSend (observed={observed})",
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
}
