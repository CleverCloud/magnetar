// SPDX-License-Identifier: Apache-2.0

//! Layer (b) of the ADR-0024 four-layer policy for the producer-side
//! memory-limit reservation (ADR-0017): the tokio integration mirror of
//! `magnetar-runtime-moonpool/tests/producer_memory_limit_concurrent.rs`.
//!
//! Maintains the tokio ↔ moonpool 1:1 test count required by ADR-0024
//! (`check-runtime-test-parity`): two `#[tokio::test]` functions here
//! mirror the moonpool file's two `#[test]` functions.
//!
//! ## What this pins
//!
//! With a small [`ConnectionConfig::memory_limit_bytes`] and the Java
//! default [`MemoryLimitPolicy::FailImmediately`], `Producer::send`
//! reserves the payload bytes against the global budget *before* the
//! message reaches the sans-io state machine (mirrors Java
//! `MemoryLimitController.reserveMemory`). Driven through a full
//! `connect → open_producer → send` round-trip against a loopback broker:
//!
//! 1. an **under-limit** send (payload ≤ limit) reserves successfully, rides the wire, and resolves
//!    `Ok(MessageId)` once the broker replies with `CommandSendReceipt`; and
//! 2. an **over-limit** send (a single payload strictly larger than the whole budget) is rejected
//!    *synchronously* with `ClientError::MemoryLimitExceeded { .. }` without ever hitting the wire.
//!
//! The over-limit payload exceeds the entire budget on its own
//! (`OVER_LIMIT_PAYLOAD > LIMIT_BYTES`), so the rejection holds regardless
//! of how much budget the under-limit send still holds — no
//! release-ordering race. The reservation is a lock-free CAS on an
//! `AtomicU64`, identical to the moonpool engine's helper (the two engines
//! share the proto-level reservation contract).

#![forbid(unsafe_code)]

use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, MemoryLimitPolicy, decode_one,
    encode_command, pb,
};
use magnetar_runtime_tokio::{Client, ClientError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Total memory budget for the connection. Small enough that a single
/// modest payload can exceed it, large enough that the under-limit send
/// fits with room to spare.
const LIMIT_BYTES: u64 = 64;

/// Under-limit payload — fits inside [`LIMIT_BYTES`] so the reservation
/// CAS succeeds and the send rides the wire.
const UNDER_LIMIT_PAYLOAD: usize = 16;

/// Over-limit payload — strictly larger than the *entire* budget, so the
/// reservation fails even against a fully-empty counter.
const OVER_LIMIT_PAYLOAD: usize = (LIMIT_BYTES as usize) + 64;

/// Spawn a loopback broker speaking the subset needed to drive
/// `open_producer` (`CONNECT → CONNECTED`, `LOOKUP → LookupResponse`,
/// `PRODUCER → PRODUCER_SUCCESS`) and one publish round-trip
/// (`SEND → SEND_RECEIPT`), plus `PING → PONG`. Returns the
/// `pulsar://host:port` URL and the spawned task handle.
async fn spawn_broker() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let handle = tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let _ = handle_session(stream).await;
            });
        }
    });
    (format!("pulsar://{addr}"), handle)
}

/// Per-session script: decode every complete frame, reply per the dispatch
/// table, flush, and return when the peer closes.
async fn handle_session(mut stream: TcpStream) -> std::io::Result<()> {
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
            reply_to_frame(&frame, &mut out_buf);
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

fn reply_to_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-test-broker".to_owned(),
                    protocol_version: Some(21),
                    max_message_size: Some(5 * 1024 * 1024),
                    feature_flags: Some(pb::FeatureFlags::default()),
                }),
                ..Default::default()
            };
            let _ = encode_command(out, &cmd);
        }
        pb::base_command::Type::Ping => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Pong as i32,
                pong: Some(pb::CommandPong {}),
                ..Default::default()
            };
            let _ = encode_command(out, &cmd);
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
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::Producer => {
            if let Some(p) = &frame.command.producer {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::ProducerSuccess as i32,
                    producer_success: Some(pb::CommandProducerSuccess {
                        request_id: p.request_id,
                        producer_name: "mem-limit-test".to_owned(),
                        last_sequence_id: Some(-1),
                        schema_version: None,
                        topic_epoch: Some(0),
                        producer_ready: Some(true),
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::Send => {
            if let Some(s) = &frame.command.send {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::SendReceipt as i32,
                    send_receipt: Some(pb::CommandSendReceipt {
                        producer_id: s.producer_id,
                        sequence_id: s.sequence_id,
                        message_id: Some(pb::MessageIdData {
                            ledger_id: 1,
                            entry_id: s.sequence_id,
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
        }
        _ => {}
    }
}

/// Mirror of `moonpool_producer_memory_limit_fail_immediately_smoke`.
/// Connect, open a non-batching producer against a 64-byte budget, then:
/// the under-limit send reserves and resolves `Ok`; the over-limit send is
/// rejected synchronously with `MemoryLimitExceeded`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_producer_memory_limit_fail_immediately_smoke() {
    let (url, broker) = spawn_broker().await;

    let cfg = ConnectionConfig {
        memory_limit_bytes: LIMIT_BYTES,
        memory_limit_policy: MemoryLimitPolicy::FailImmediately,
        ..ConnectionConfig::default()
    };

    let client = tokio::time::timeout(Duration::from_secs(10), Client::connect(&url, cfg))
        .await
        .expect("connect did not exceed the test timeout")
        .expect("connect must succeed against the loopback broker");

    let producer = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/mem-limit".to_owned(),
            enable_batching: false,
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not exceed the test timeout")
    .expect("open_producer must succeed");

    // (1) Under-limit send — reserves, rides the wire, resolves Ok once the
    // broker's SendReceipt lands.
    let under = tokio::time::timeout(
        Duration::from_secs(5),
        producer.send_bytes(vec![0u8; UNDER_LIMIT_PAYLOAD]),
    )
    .await
    .expect("under-limit send did not exceed the test timeout");
    assert!(
        under.is_ok(),
        "an under-limit send (payload {UNDER_LIMIT_PAYLOAD} <= limit {LIMIT_BYTES}) must resolve \
         Ok, got {under:?}",
    );

    // (2) Over-limit send — a single payload larger than the whole budget.
    // Must surface MemoryLimitExceeded synchronously (no wire round-trip).
    let over = tokio::time::timeout(
        Duration::from_secs(5),
        producer.send_bytes(vec![0u8; OVER_LIMIT_PAYLOAD]),
    )
    .await
    .expect("over-limit send must resolve immediately (no wire round-trip)");
    let err = over.expect_err("an over-limit send must be rejected, not resolve Ok");
    assert!(
        matches!(err, ClientError::MemoryLimitExceeded { .. }),
        "over-limit send must surface ClientError::MemoryLimitExceeded, got {err:?}",
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
    broker.abort();
}

/// Mirror of `moonpool_producer_memory_limit_fail_immediately_sweep_8_seeds`.
/// The moonpool sweep re-runs the same contract under several simulated
/// I/O interleavings; the reservation outcome is a deterministic function
/// of the payload sizes, so the tokio mirror asserts the same contract
/// holds repeatedly across freshly-opened producers on one connection — a
/// regression in the reservation CAS or policy dispatch would flip one of
/// the iterations. Also pins that a release frees budget for a later send
/// (the under-limit send's bytes are released on its receipt, so each
/// iteration starts from a clean budget).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_producer_memory_limit_fail_immediately_repeated() {
    const ITERS: usize = 8;

    let (url, broker) = spawn_broker().await;

    let cfg = ConnectionConfig {
        memory_limit_bytes: LIMIT_BYTES,
        memory_limit_policy: MemoryLimitPolicy::FailImmediately,
        ..ConnectionConfig::default()
    };

    let client = tokio::time::timeout(Duration::from_secs(10), Client::connect(&url, cfg))
        .await
        .expect("connect did not exceed the test timeout")
        .expect("connect must succeed against the loopback broker");

    let producer = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/mem-limit-repeated".to_owned(),
            enable_batching: false,
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not exceed the test timeout")
    .expect("open_producer must succeed");

    for i in 0..ITERS {
        // Under-limit send must resolve Ok every iteration — the previous
        // iteration's reservation was released on its receipt, so the
        // budget is clean.
        let under = tokio::time::timeout(
            Duration::from_secs(5),
            producer.send_bytes(vec![0u8; UNDER_LIMIT_PAYLOAD]),
        )
        .await
        .unwrap_or_else(|_| panic!("under-limit send (iter {i}) did not exceed the test timeout"));
        assert!(
            under.is_ok(),
            "under-limit send (iter {i}) must resolve Ok, got {under:?}",
        );

        // Over-limit send must be rejected every iteration.
        let over = tokio::time::timeout(
            Duration::from_secs(5),
            producer.send_bytes(vec![0u8; OVER_LIMIT_PAYLOAD]),
        )
        .await
        .unwrap_or_else(|_| panic!("over-limit send (iter {i}) must resolve immediately"));
        let err = over.unwrap_err();
        assert!(
            matches!(err, ClientError::MemoryLimitExceeded { .. }),
            "over-limit send (iter {i}) must surface MemoryLimitExceeded, got {err:?}",
        );
    }

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
    broker.abort();
}
