// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::too_many_lines)]
#![forbid(unsafe_code)]

//! Issue #303 — driver-loop read fairness under sustained `driver_waker`
//! pressure.
//!
//! The single per-connection driver task multiplexes the outbound write path
//! and the inbound read path in one `tokio::select! { biased; … }`. Every
//! `Producer::send` pulses `shared.driver_waker.notify_one()`, so under
//! sustained publish load a waker permit is almost always pending on loop
//! entry. The pre-fix arm order polled the `driver_waker` arm FIRST, so whenever
//! a permit is pending the inbound read arm is deprioritised that iteration —
//! `CommandSendReceipt` bytes already back on the socket are read late and the
//! matching `SendFut`s resolve late (issue #303: send→ack inflated to seconds
//! under load while the broker acked in ~4ms).
//!
//! The fix (driver.rs) reorders the `select!` so the inbound read arm is polled
//! FIRST while keeping `biased;` (deterministic order for the moonpool mirror).
//! The outbound path is unaffected: `poll_transmit` + `write_all` already run at
//! the TOP of every loop iteration, so each tick still flushes pending sends.
//!
//! ## What this test asserts (and the limits of a black-box test here)
//!
//! `tokio::sync::Notify` stores a SINGLE permit, not a backlog. The pre-fix
//! waker-first order therefore costs *at most one extra loop iteration* of read
//! latency per permit refresh: once the waker arm consumes the permit, the next
//! select poll finds no permit and reads the socket. Unbounded read starvation
//! only emerges under genuine multi-threaded send concurrency that re-posts a
//! permit at essentially every select boundary — which is inherently racy and
//! not reproducible as a crisp pass/fail in a single test. So this test does NOT
//! claim to hang under the old order; it asserts the *positive invariant the fix
//! guarantees and the old order risked under load*: with `driver_waker`
//! perpetually armed (the worst-case signal a sustained publish stream emits), a
//! canary send's `CommandSendReceipt` is still read and resolved promptly. The
//! read-FIRST arm order is what makes that hold unconditionally; the differential
//! EventStream-parity test pins that the reorder changed no observable output.
//!
//! ADR-0024 layer (b). 1:1 mirror of
//! `magnetar-runtime-moonpool/tests/driver_read_fairness.rs`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
mod common;
use common::HANG_GUARD;

/// Tight per-receipt bound. A starved read arm (the regression) never resolves
/// the canary, so this fires well inside `HANG_GUARD` and fails fast.
const RECEIPT_BUDGET: Duration = Duration::from_secs(5);

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-read-fairness".to_owned(),
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
            producer_name: "magnetar-read-fairness".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_send_receipt(out: &mut BytesMut, producer_id: u64, sequence_id: u64, entry_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::SendReceipt as i32,
        send_receipt: Some(pb::CommandSendReceipt {
            producer_id,
            sequence_id,
            message_id: Some(pb::MessageIdData {
                ledger_id: 7,
                entry_id,
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

/// Broker session: replies to CONNECT / LOOKUP / PRODUCER opens and echoes a
/// `CommandSendReceipt` for EVERY `CommandSend`, so a `SendFut` can only resolve
/// if the driver loop actually reads the inbound receipt bytes.
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
                    if let Some(s) = &frame.command.send {
                        let n = sends_observed.fetch_add(1, Ordering::SeqCst);
                        emit_send_receipt(&mut out_buf, s.producer_id, s.sequence_id, u64::from(n));
                    }
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

async fn spawn_receipt_broker() -> (String, Arc<AtomicU32>) {
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

/// With `driver_waker` continuously armed (the worst-case signal a sustained
/// publish stream emits), a canary send's `CommandSendReceipt` must still be
/// read and resolved well within `RECEIPT_BUDGET`. The read-FIRST arm order is
/// what makes this hold unconditionally even under perpetual waker pressure;
/// see the module docs for why this asserts the positive invariant rather than
/// claiming a hang under the old order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canary_receipt_not_starved_by_perpetual_driver_waker() {
    let (url, sends_observed) = spawn_receipt_broker().await;

    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let producer = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/read-fairness".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out")
    .expect("open_producer ok");

    // Spawn a background task that keeps the driver waker permanently armed,
    // reproducing the "almost-always-ready driver_waker" of a sustained publish
    // stream WITHOUT relying on send throughput (which would collapse to a
    // single Notify permit). `notify_one` is idempotent on an already-pending
    // permit, so this models the worst case: a permit pending on every loop
    // entry.
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = stop.clone();
    let shared = client.shared().clone();
    let pressure = tokio::spawn(async move {
        while !stop_for_task.load(Ordering::Relaxed) {
            shared.driver_waker.notify_one();
            tokio::task::yield_now().await;
        }
    });

    // Single canary send. Its receipt can only resolve if the driver loop reads
    // the inbound bytes despite the perpetual waker permit.
    let result = tokio::time::timeout(
        RECEIPT_BUDGET,
        producer.send_bytes(Bytes::from_static(b"canary")),
    )
    .await
    .expect(
        "canary send→receipt did not resolve within the receipt budget under sustained \
         driver_waker pressure — the inbound read arm is starved (issue #303 regression)",
    );

    result.expect("canary send must resolve with a receipt, not an error");

    let observed = sends_observed.load(Ordering::SeqCst);
    assert!(
        observed >= 1,
        "broker must have observed the canary CommandSend (observed={observed})",
    );

    stop.store(true, Ordering::Relaxed);
    pressure.await.expect("pressure task panicked");
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
}
