// SPDX-License-Identifier: Apache-2.0

//! Producer-not-ready replay gating across a supervised reconnect —
//! moonpool engine, real loopback broker (ADR-0024 layer c for the proto
//! replay-gating fix). 1:1 twin of
//! `crates/magnetar-runtime-tokio/tests/reconnect_replay_gating.rs`
//! (ADR-0024 runtime-test-parity).
//!
//! Runs the moonpool engine over `TokioProviders` against a real loopback
//! `TcpListener` (the `tests/logging_checksum.rs` harness pattern — the
//! sim runner's thread-local `SimulationLayer` is irrelevant here since no
//! log capture is involved, but the provider choice keeps the harness
//! identical to its sibling capture tests). The scenario:
//!
//! 1. connect → producer open (acked) → one send → receipt — healthy session;
//! 2. the broker DROPS the connection; the client queues a second send whose future stays pending
//!    across the reconnect (transparent replay);
//! 3. the supervisor redials; the rebuild's `CommandProducer` is acked with
//!    `CommandProducerSuccess`;
//! 4. the queued send must reach the wire ONLY AFTER that ack, exactly once; the receipt resolves
//!    the user-facing future.
//!
//! Twin asymmetry (documented engine gap, mirrors the
//! `logging_no_secrets` precedent): the tokio twin additionally exercises
//! the TRANSIENT `ServiceNotReady` → lookup → retry leg; the moonpool
//! driver has no transient-retry arms (`ProducerOpenFailedTransient` /
//! `SubscribeFailedTransient` are not consumed — pre-existing engine
//! parity gap, tracked in `docs/follow-ups.md`), so this twin acks the
//! rebuild's open directly. The core contract under test — no send before
//! the re-attach ack, exactly-once replay — is identical.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, Frame, FrameError, decode_one, encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Shared script state across the two scripted connections.
#[derive(Default)]
struct Gating {
    /// Producer opens seen on connection #2 (the rebuild's open, acked).
    conn2_producer_opens: AtomicU32,
    /// Set once connection #2's `ProducerSuccess` has been written.
    conn2_success_sent: AtomicBool,
    /// Violation: a `CommandSend` arrived on connection #2 BEFORE the ack.
    premature_send: AtomicBool,
    /// `CommandSend` frames seen on connection #2 (must end at exactly 1).
    conn2_sends: AtomicU32,
}

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
            server_version: "replay-gating-broker/0".to_owned(),
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
            producer_name: "replay-gating-producer".to_owned(),
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

/// Serve one connection with a frame→reply closure; returns when the closure
/// signals end-of-session, the peer closes, or an I/O error occurs.
async fn serve_conn<F>(stream: &mut TcpStream, mut reply_for: F)
where
    F: FnMut(&Frame, &mut BytesMut) -> bool,
{
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
            let keep_going = reply_for(&frame, &mut out);
            if !out.is_empty() {
                if stream.write_all(&out).await.is_err() {
                    return;
                }
                let _ = stream.flush().await;
            }
            if !keep_going {
                return;
            }
        }
        match stream.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

/// Scripted broker: a healthy first session that drops after the first
/// receipt, then a second session that exercises the ack-gated-replay leg
/// (no transient leg on moonpool — see the module doc).
async fn run_gating_broker(listener: TcpListener, state: Arc<Gating>) {
    // ── Connection #1: healthy, then dropped after the first receipt. ──
    let Ok((mut s1, _)) = listener.accept().await else {
        return;
    };
    serve_conn(&mut s1, |frame, out| {
        match pb::base_command::Type::try_from(frame.command.r#type) {
            Ok(pb::base_command::Type::Connect) => emit_connected(out),
            Ok(pb::base_command::Type::Lookup) => {
                if let Some(l) = &frame.command.lookup_topic {
                    emit_lookup_response(out, l.request_id);
                }
            }
            Ok(pb::base_command::Type::Producer) => {
                if let Some(p) = &frame.command.producer {
                    emit_producer_success(out, p.request_id);
                }
            }
            Ok(pb::base_command::Type::Send) => {
                if let Some(send) = &frame.command.send {
                    emit_send_receipt(out, send.producer_id, send.sequence_id);
                    // Receipt written — end the session right after (drop).
                    return false;
                }
            }
            Ok(pb::base_command::Type::Ping) => emit_pong(out),
            _ => {}
        }
        true
    })
    .await;
    drop(s1);

    // ── Connection #2: supervisor redial; transient → retry → gated replay. ──
    let Ok((mut s2, _)) = listener.accept().await else {
        return;
    };
    let st = Arc::clone(&state);
    serve_conn(&mut s2, move |frame, out| {
        match pb::base_command::Type::try_from(frame.command.r#type) {
            Ok(pb::base_command::Type::Connect) => emit_connected(out),
            Ok(pb::base_command::Type::Lookup) => {
                if let Some(l) = &frame.command.lookup_topic {
                    emit_lookup_response(out, l.request_id);
                }
            }
            Ok(pb::base_command::Type::Producer) => {
                if let Some(p) = &frame.command.producer {
                    let _ = st.conn2_producer_opens.fetch_add(1, Ordering::SeqCst);
                    // The rebuild's open: ack it — the gate opens NOW.
                    // (No transient leg on moonpool — see the module doc.)
                    emit_producer_success(out, p.request_id);
                    st.conn2_success_sent.store(true, Ordering::SeqCst);
                }
            }
            Ok(pb::base_command::Type::Send) => {
                if let Some(send) = &frame.command.send {
                    if !st.conn2_success_sent.load(Ordering::SeqCst) {
                        // The livelock signature: a send before the ack.
                        st.premature_send.store(true, Ordering::SeqCst);
                    }
                    st.conn2_sends.fetch_add(1, Ordering::SeqCst);
                    emit_send_receipt(out, send.producer_id, send.sequence_id);
                }
            }
            Ok(pb::base_command::Type::Ping) => emit_pong(out),
            _ => {}
        }
        true
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_send_replays_only_after_retry_ack_across_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let state = Arc::new(Gating::default());
    tokio::spawn(run_gating_broker(listener, Arc::clone(&state)));

    // Supervised reconnect must be ENABLED — the default config exits the
    // driver on the first I/O failure (no redial, no replay to gate).
    let config = ConnectionConfig {
        supervisor: Some(magnetar_proto::SupervisorConfig {
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(250),
            ..Default::default()
        }),
        ..Default::default()
    };
    let engine = MoonpoolEngine::new(TokioProviders::new());
    // `connect_plain` is unsupervised (driver exits on the first I/O
    // failure); the supervised variant is the one that redials.
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect_plain_supervised(&engine, &addr.to_string(), config, None, None),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed");

    let producer = tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/replay-gating".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("producer open did not time out")
    .expect("producer open must succeed");

    // Healthy round-trip; the broker drops the connection right after this
    // receipt.
    let _ = tokio::time::timeout(Duration::from_secs(5), producer.send(outgoing(b"one")))
        .await
        .expect("first send did not time out")
        .expect("first send must succeed");

    // Give the driver a beat to observe the drop, then queue the second send
    // — its future stays pending across the supervised reconnect, through
    // the transient-error + lookup + retry leg, until the post-ack replay's
    // receipt arrives (transparent replay, Java resendMessages parity).
    tokio::time::sleep(Duration::from_millis(200)).await;
    let receipt = tokio::time::timeout(Duration::from_secs(20), producer.send(outgoing(b"two")))
        .await
        .expect(
            "replayed send must resolve after the retry ack — the \
             producer-not-ready gate must not starve it",
        )
        .expect("replayed send must succeed");
    let _ = receipt;

    assert!(
        !state.premature_send.load(Ordering::SeqCst),
        "no CommandSend may reach the broker before the retry's ProducerSuccess \
         (premature sends make a real broker close the connection — the livelock)"
    );
    assert_eq!(
        state.conn2_sends.load(Ordering::SeqCst),
        1,
        "the queued send must replay exactly once on the new session"
    );
    assert_eq!(
        state.conn2_producer_opens.load(Ordering::SeqCst),
        1,
        "rebuild open, acked directly (no transient leg on moonpool — module doc)"
    );

    client.close().await;
}
