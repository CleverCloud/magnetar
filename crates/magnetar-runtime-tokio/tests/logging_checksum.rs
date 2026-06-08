// SPDX-License-Identifier: Apache-2.0

//! ADR-0054 checksum log-capture test — tokio engine, real loopback broker
//! (ADR-0024 layer b for the proto checksum point-of-detection diff).
//!
//! Drives the full engine driver through a plain `CommandConnect` →
//! `CommandConnected` handshake against a scripted loopback broker (harness
//! pattern: `tests/handshake_error_capture.rs`), after which the broker
//! writes ONE CRC32C-corrupted frame raw to the stream (construction per
//! the proto unit test `frame::tests::detects_crc32c_mismatch`). With a
//! **global** TRACE-level capturing subscriber installed, the test asserts:
//!
//! 1. the proto point-of-detection `error!` fires with structured `computed` / `expected` fields
//!    ("CRC32C checksum mismatch; corrupt frame dropped");
//! 2. it fires **exactly once** — the engine drains the companion
//!    `ConnectionEvent::ChecksumMismatch` silently (ADR-0054 single-owner rule: no duplicate
//!    engine-side record);
//! 3. the connection survives the drop and subsequent traffic flows: a full lookup + producer-open
//!    round-trip — whose broker replies sit BEHIND the corrupted frame on the wire — resolves
//!    successfully ("CRC32C verify or drop", workspace invariant 4).
//!
//! # Why this file is its own integration-test binary with ONE test fn
//!
//! The proto `error!` fires on the driver task (another tokio worker
//! thread), so the capturing subscriber must be **global**
//! (`fmt().init()`). A global subscriber can be installed exactly once per
//! process, so this file holds a single test fn and shares the binary with
//! no other test (mirrored 1:1 by
//! `crates/magnetar-runtime-moonpool/tests/logging_checksum.rs` for the
//! ADR-0024 runtime-test-parity count).

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, Frame, FrameError, decode_one, encode_command,
    encode_payload, pb,
};
use magnetar_runtime_tokio::Client;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// The proto point-of-detection record (`crates/magnetar-proto/src/conn.rs`
/// decode loop). Exactly one occurrence proves the corrupted frame was
/// detected AND that the engine drained the companion event silently.
const CHECKSUM_LOG: &str = "CRC32C checksum mismatch; corrupt frame dropped";

/// Shared in-memory sink for the global fmt subscriber.
#[derive(Clone, Default)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl CaptureWriter {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock()).into_owned()
    }
}

impl std::io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
    type Writer = CaptureWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// One deliberately CRC32C-corrupted payload frame: encode a SEND-shaped
/// payload frame, then flip the last payload byte to invalidate the CRC —
/// the exact construction of the proto unit test
/// `frame::tests::detects_crc32c_mismatch`.
fn corrupted_payload_frame() -> Bytes {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Send as i32,
        send: Some(pb::CommandSend {
            producer_id: 1,
            sequence_id: 1,
            num_messages: Some(1),
            ..Default::default()
        }),
        ..Default::default()
    };
    let meta = pb::MessageMetadata {
        producer_name: "p".to_owned(),
        sequence_id: 1,
        publish_time: 1_700_000_000_000,
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_payload(&mut buf, &cmd, &meta, b"corrupt-me").expect("encode_payload");
    let last = buf.len() - 1;
    buf[last] ^= 0xff;
    buf.freeze()
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "logging-checksum-broker/0".to_owned(),
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
            producer_name: "logging-checksum-producer".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_success(out: &mut BytesMut, request_id: u64) {
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

fn emit_pong(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Pong as i32,
        pong: Some(pb::CommandPong {}),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Build the scripted reply for one inbound frame. On `CommandConnect` the
/// handshake reply is followed by ONE raw CRC32C-corrupted frame in the
/// same flush — directly behind `CommandConnected` and ahead of any lookup
/// traffic, so TCP ordering guarantees the engine processes (and drops)
/// the corruption before the producer-open replies reach the proto layer.
fn reply_for(frame: &Frame) -> BytesMut {
    let mut out = BytesMut::new();
    match pb::base_command::Type::try_from(frame.command.r#type) {
        Ok(pb::base_command::Type::Connect) => {
            emit_connected(&mut out);
            out.extend_from_slice(&corrupted_payload_frame());
        }
        Ok(pb::base_command::Type::Lookup) => {
            if let Some(l) = &frame.command.lookup_topic {
                emit_lookup_response(&mut out, l.request_id);
            }
        }
        Ok(pb::base_command::Type::Producer) => {
            if let Some(p) = &frame.command.producer {
                emit_producer_success(&mut out, p.request_id);
            }
        }
        Ok(pb::base_command::Type::CloseProducer) => {
            if let Some(c) = &frame.command.close_producer {
                emit_success(&mut out, c.request_id);
            }
        }
        Ok(pb::base_command::Type::Ping) => emit_pong(&mut out),
        _ => {}
    }
    out
}

/// Scripted broker: `CommandConnect` → `CommandConnected` + ONE raw
/// CRC32C-corrupted frame, then a minimal lookup + producer-open script so
/// the test can prove subsequent traffic flows over the same connection.
async fn run_checksum_broker(listener: TcpListener) {
    let Ok((mut stream, _peer)) = listener.accept().await else {
        return;
    };
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
            let out = reply_for(&frame);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checksum_mismatch_logged_once_and_connection_survives() {
    let sink = CaptureWriter::default();
    tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::TRACE)
        .with_writer(sink.clone())
        .with_ansi(false)
        .init();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(run_checksum_broker(listener));
    let url = format!("pulsar://{addr}");

    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed");

    // Subsequent traffic flows: the lookup + producer-open replies sit
    // BEHIND the corrupted frame on the wire, so resolving this round-trip
    // proves the engine processed (and survived) the corruption first.
    let producer = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/logging-checksum".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("producer open did not time out")
    .expect("producer open must succeed after the corrupt-frame drop");
    let _ = producer.close().await;
    client.close().await;

    // ── Assertions on everything the process logged ──
    let captured = sink.contents();
    assert!(
        captured.contains(CHECKSUM_LOG),
        "the proto point-of-detection error! must be captured; got:\n{captured}",
    );
    assert!(
        captured.contains("ERROR"),
        "the checksum mismatch must log at error! level; got:\n{captured}",
    );
    assert!(
        captured.contains("computed=") && captured.contains("expected="),
        "the checksum error! must carry structured computed/expected fields; got:\n{captured}",
    );
    // Single-owner rule (ADR-0054 §5): exactly one record — proto owns the
    // point of detection; the engine drains the companion event silently.
    let hits = captured.matches(CHECKSUM_LOG).count();
    assert_eq!(
        hits, 1,
        "exactly one checksum record expected (engine must drain the event \
         silently, no duplicate log); got {hits}:\n{captured}",
    );
}
