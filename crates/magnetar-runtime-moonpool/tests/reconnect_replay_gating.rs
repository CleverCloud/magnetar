// SPDX-License-Identifier: Apache-2.0

//! Producer-not-ready replay gating across a supervised reconnect —
//! moonpool engine (ADR-0024 layer c for the proto replay-gating fix +
//! transient-retry wiring). 1:1 twin of
//! `crates/magnetar-runtime-tokio/tests/reconnect_replay_gating.rs`
//! (ADR-0024 runtime-test-parity).
//!
//! Two tests live here:
//!
//! ## `queued_send_replays_only_after_retry_ack_across_reconnect`
//!
//! Runs the moonpool engine over `TokioProviders` against a real loopback
//! `TcpListener` (the `tests/logging_checksum.rs` harness pattern). The
//! scenario — now a TRUE 1:1 twin of the tokio test (the moonpool
//! transient-retry arms are wired, so the documented engine asymmetry is
//! gone):
//!
//! 1. connect → producer open (acked) → one send → receipt — healthy session;
//! 2. the broker DROPS the connection; the client queues a second send whose future stays pending
//!    across the reconnect (transparent replay);
//! 3. the supervisor redials; the rebuild's `CommandProducer` is answered with a TRANSIENT
//!    `ServiceNotReady` ("Please redo the lookup"), forcing the lookup + retry leg;
//! 4. the retry's `CommandProducer` is acked with `CommandProducerSuccess`;
//! 5. the queued send must reach the wire ONLY AFTER that ack, exactly once; the receipt resolves
//!    the user-facing future.
//!
//! ## `transient_producer_open_retry_fires_under_virtual_time`
//!
//! The determinism proof for the transient-retry leg: drives the SAME transient → lookup →
//! retry leg under **`SimProviders` virtual time** (the
//! `driver_mid_session_reject.rs` harness pattern). The retry leg sleeps
//! `TRANSIENT_RETRY_DELAY` (2 s) through the INJECTED
//! [`moonpool_core::TimeProvider`]; under the single-threaded sim runtime a
//! host-clock sleep would never advance virtual time and the no-progress
//! detector would wedge the run. The run terminating — with the replayed
//! send resolved after exactly two producer-opens on the redialled session —
//! proves the retry fired on the injected clock (ADR-0011), not a host
//! `tokio::time::sleep`. This is the real backstop `check-no-internal-clock`
//! cannot provide (it greps only `Instant::now` / `SystemTime::now`).

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::io::{
    AsyncRead as FuturesRead, AsyncReadExt as _, AsyncWrite as FuturesWrite, AsyncWriteExt as _,
};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, ConsumerHandle, CreateProducerRequest, Frame, FrameError,
    OperationRetryConfig, SubscribeRequest, decode_one, encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait, TokioProviders};
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Shared script state across the two scripted connections.
#[derive(Default)]
struct Gating {
    /// Producer opens seen on connection #2 (1st → transient error, 2nd → ack).
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

fn emit_lookup_redirect(out: &mut BytesMut, request_id: u64, broker_url: &str) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::LookupResponse as i32,
        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
            broker_service_url: Some(broker_url.to_owned()),
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
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

fn emit_broker_error(out: &mut BytesMut, request_id: u64, error: pb::ServerError, message: &str) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Error as i32,
        error: Some(pb::CommandError {
            request_id,
            error: error as i32,
            message: message.to_owned(),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_transient_error(out: &mut BytesMut, request_id: u64) {
    emit_broker_error(
        out,
        request_id,
        pb::ServerError::ServiceNotReady,
        "Namespace bundle not served by this instance. Please redo the lookup.",
    );
}

fn emit_terminal_lookup_error(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Error as i32,
        error: Some(pb::CommandError {
            request_id,
            error: pb::ServerError::AuthorizationError as i32,
            message: "lookup denied after transient attachment failure".to_owned(),
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
/// receipt, then a second session that exercises the transient-error +
/// retry + ack-gated-replay leg (now wired on moonpool too).
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

    // ── Failed redial cycles: accept + drop a few dials mid-handshake,
    // mirroring the e2e's docker-restart window where the proxy accepts
    // while the broker is down (each cycle is a fresh reset + snapshot
    // round on the client). ──
    for _ in 0..3 {
        let Ok((s_dead, _)) = listener.accept().await else {
            return;
        };
        drop(s_dead);
    }

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
                    let n = st.conn2_producer_opens.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        // The rebuild's open: transient bundle-not-served.
                        emit_transient_error(out, p.request_id);
                    } else {
                        // The retry's open: ack it — the gate opens NOW.
                        emit_producer_success(out, p.request_id);
                        st.conn2_success_sent.store(true, Ordering::SeqCst);
                    }
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
        2,
        "rebuild open (transient-rejected) + retry open (acked)"
    );

    client.close().await;
}

// ============================================================================
// Issue #299 + #302 — moonpool engine mirrors of the tokio give-up + receive-
// across-drop tests (ADR-0024 1:1 runtime-test-parity). Driven over the real
// `TokioProviders` loopback harness like
// `queued_send_replays_only_after_retry_ack_across_reconnect`; the give-up
// tests run under `start_paused = true` so the transient-retry backoff sleeps
// (which route through the injected `TimeProvider`, i.e. `tokio::time::sleep`
// under `TokioProviders`) auto-advance in virtual time.
// ============================================================================

fn emit_subscribe_success(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            ..Default::default()
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// A `CommandMessage` + payload addressed to `handle` at entry `entry`.
fn emit_message_frame(out: &mut BytesMut, handle: ConsumerHandle, entry: u64, payload: &[u8]) {
    let msg_cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: 9,
                entry_id: entry,
                partition: None,
                batch_index: None,
                ack_set: vec![],
                batch_size: None,
                first_chunk_message_id: None,
            },
            redelivery_count: Some(0),
            ack_set: vec![],
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let metadata = pb::MessageMetadata {
        producer_name: "magnetar-test-prod".to_owned(),
        sequence_id: entry,
        publish_time: 0,
        ..Default::default()
    };
    encode_payload(out, &msg_cmd, &metadata, payload).expect("encode message frame");
}

#[derive(Default)]
struct GiveUpGating {
    producer_opens: AtomicU32,
    subscribes: AtomicU32,
}

fn fast_give_up_config() -> ConnectionConfig {
    ConnectionConfig {
        operation_timeout: Duration::from_secs(1),
        ..ConnectionConfig::default()
    }
}

fn fast_give_up_policy() -> OperationRetryConfig {
    OperationRetryConfig {
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        max_retries: Some(2),
    }
}

fn attachment_deadline_config() -> ConnectionConfig {
    ConnectionConfig {
        operation_timeout: Duration::from_millis(100),
        ..ConnectionConfig::default()
    }
}

fn retry_after_attachment_deadline_policy() -> OperationRetryConfig {
    OperationRetryConfig {
        initial_backoff: Duration::from_secs(5),
        max_backoff: Duration::from_secs(5),
        max_retries: Some(2),
    }
}

/// Broker that NEVER acks the producer-open — every `CommandProducer` is bounced
/// transiently. Mirror of the tokio engine's `run_always_transient_producer_broker`.
async fn run_always_transient_producer_broker(listener: TcpListener, state: Arc<GiveUpGating>) {
    let Ok((mut s, _)) = listener.accept().await else {
        return;
    };
    let st = Arc::clone(&state);
    serve_conn(&mut s, move |frame, out| {
        match pb::base_command::Type::try_from(frame.command.r#type) {
            Ok(pb::base_command::Type::Connect) => emit_connected(out),
            Ok(pb::base_command::Type::Lookup) => {
                if let Some(l) = &frame.command.lookup_topic {
                    emit_lookup_response(out, l.request_id);
                }
            }
            Ok(pb::base_command::Type::Producer) => {
                if let Some(p) = &frame.command.producer {
                    st.producer_opens.fetch_add(1, Ordering::SeqCst);
                    emit_transient_error(out, p.request_id);
                }
            }
            Ok(pb::base_command::Type::Ping) => emit_pong(out),
            _ => {}
        }
        true
    })
    .await;
}

/// #302 (moonpool integration, producer give-up): 1:1 twin of the tokio
/// `producer_open_gives_up_with_error_after_budget_exhausted`. The bounded
/// retry loop re-arms across the configured transient rejects, then surfaces
/// `Err` once the client-owned operation budget is exhausted — never hangs `open_producer`
/// forever.
#[tokio::test]
async fn producer_open_gives_up_with_error_after_budget_exhausted() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let state = Arc::new(GiveUpGating::default());
    tokio::spawn(run_always_transient_producer_broker(
        listener,
        Arc::clone(&state),
    ));

    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = Client::connect_plain_supervised(
        &engine,
        &addr.to_string(),
        fast_give_up_config(),
        None,
        None,
    )
    .await
    .expect("connect must succeed")
    .with_operation_retry(fast_give_up_policy());

    let result = client
        .open_producer(CreateProducerRequest {
            topic: "persistent://public/default/never-served".to_owned(),
            ..Default::default()
        })
        .await;
    assert!(
        result.is_err(),
        "open_producer must surface Err after the transient retry budget is exhausted, \
         not hang forever (issue #302); got {result:?}"
    );
    assert_eq!(
        state.producer_opens.load(Ordering::SeqCst),
        3,
        "one initial producer-open plus two configured retries must reach the wire"
    );

    client.close().await;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let deadline_state = Arc::new(GiveUpGating::default());
    tokio::spawn(run_always_transient_producer_broker(
        listener,
        Arc::clone(&deadline_state),
    ));
    let deadline_client = Client::connect_plain_supervised(
        &engine,
        &addr.to_string(),
        attachment_deadline_config(),
        None,
        None,
    )
    .await
    .expect("connect must succeed")
    .with_operation_retry(retry_after_attachment_deadline_policy());

    let deadline_error = deadline_client
        .open_producer(CreateProducerRequest {
            topic: "persistent://public/default/producer-deadline".to_owned(),
            ..Default::default()
        })
        .await
        .expect_err("producer-open must stop at operation_timeout during retry backoff");
    assert!(matches!(
        deadline_error,
        magnetar_runtime_moonpool::ClientError::Broker {
            code,
            ref message
        } if code == pb::ServerError::ServiceNotReady as i32
            && message == "Namespace bundle not served by this instance. Please redo the lookup."
    ));
    tokio::task::yield_now().await;
    assert_eq!(
        deadline_state.producer_opens.load(Ordering::SeqCst),
        1,
        "the operation deadline must stop provisional retries before a second producer-open"
    );
    deadline_client.close().await;
}

/// Broker that NEVER acks the subscribe — every `CommandSubscribe` is bounced
/// transiently. Mirror of the tokio engine's `run_always_transient_subscribe_broker`.
async fn run_always_transient_subscribe_broker(listener: TcpListener, state: Arc<GiveUpGating>) {
    let Ok((mut s, _)) = listener.accept().await else {
        return;
    };
    let st = Arc::clone(&state);
    serve_conn(&mut s, move |frame, out| {
        match pb::base_command::Type::try_from(frame.command.r#type) {
            Ok(pb::base_command::Type::Connect) => emit_connected(out),
            Ok(pb::base_command::Type::Lookup) => {
                if let Some(l) = &frame.command.lookup_topic {
                    emit_lookup_response(out, l.request_id);
                }
            }
            Ok(pb::base_command::Type::Subscribe) => {
                if let Some(sub) = &frame.command.subscribe {
                    st.subscribes.fetch_add(1, Ordering::SeqCst);
                    emit_transient_error(out, sub.request_id);
                }
            }
            Ok(pb::base_command::Type::Ping) => emit_pong(out),
            _ => {}
        }
        true
    })
    .await;
}

/// #302 (moonpool integration, consumer give-up): 1:1 twin of the tokio
/// `subscribe_gives_up_with_error_after_budget_exhausted`.
#[tokio::test]
async fn subscribe_gives_up_with_error_after_budget_exhausted() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let state = Arc::new(GiveUpGating::default());
    tokio::spawn(run_always_transient_subscribe_broker(
        listener,
        Arc::clone(&state),
    ));

    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = Client::connect_plain_supervised(
        &engine,
        &addr.to_string(),
        fast_give_up_config(),
        None,
        None,
    )
    .await
    .expect("connect must succeed")
    .with_operation_retry(fast_give_up_policy());

    let error = client
        .subscribe(SubscribeRequest {
            topic: "persistent://public/default/never-served-sub".to_owned(),
            subscription: "giveup-302".to_owned(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            ..Default::default()
        })
        .await
        .expect_err("provisional subscribe must return the final broker error");
    assert!(matches!(
        error,
        magnetar_runtime_moonpool::ClientError::Broker { .. }
    ));
    assert_eq!(
        state.subscribes.load(Ordering::SeqCst),
        3,
        "one initial subscribe plus two configured retries must reach the wire"
    );

    client.close().await;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let deadline_state = Arc::new(GiveUpGating::default());
    tokio::spawn(run_always_transient_subscribe_broker(
        listener,
        Arc::clone(&deadline_state),
    ));
    let deadline_client = Client::connect_plain_supervised(
        &engine,
        &addr.to_string(),
        attachment_deadline_config(),
        None,
        None,
    )
    .await
    .expect("connect must succeed")
    .with_operation_retry(retry_after_attachment_deadline_policy());

    let deadline_error = deadline_client
        .subscribe(SubscribeRequest {
            topic: "persistent://public/default/subscribe-deadline".to_owned(),
            subscription: "deadline".to_owned(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            ..Default::default()
        })
        .await
        .expect_err("subscribe must stop at operation_timeout during retry backoff");
    assert!(matches!(
        deadline_error,
        magnetar_runtime_moonpool::ClientError::Broker {
            code,
            ref message
        } if code == pb::ServerError::ServiceNotReady as i32
            && message == "Namespace bundle not served by this instance. Please redo the lookup."
    ));
    tokio::task::yield_now().await;
    assert_eq!(
        deadline_state.subscribes.load(Ordering::SeqCst),
        1,
        "the operation deadline must stop provisional retries before a second subscribe"
    );
    deadline_client.close().await;
}

const TERMINAL_LOOKUP_PRODUCER_TOPIC: &str = "persistent://public/default/terminal-lookup-producer";
const TERMINAL_LOOKUP_CONSUMER_TOPIC: &str = "persistent://public/default/terminal-lookup-consumer";

#[derive(Default)]
struct TerminalLookupGating {
    producer_lookups: AtomicU32,
    consumer_lookups: AtomicU32,
    producer_opens: AtomicU32,
    subscribes: AtomicU32,
}

async fn run_terminal_lookup_after_provisional_setup_broker(
    listener: TcpListener,
    state: Arc<TerminalLookupGating>,
) {
    let Ok((mut stream, _)) = listener.accept().await else {
        return;
    };
    serve_conn(&mut stream, move |frame, out| {
        match pb::base_command::Type::try_from(frame.command.r#type) {
            Ok(pb::base_command::Type::Connect) => emit_connected(out),
            Ok(pb::base_command::Type::Lookup) => {
                if let Some(lookup) = &frame.command.lookup_topic {
                    let attempts = if lookup.topic == TERMINAL_LOOKUP_PRODUCER_TOPIC {
                        &state.producer_lookups
                    } else {
                        &state.consumer_lookups
                    };
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        emit_lookup_response(out, lookup.request_id);
                    } else {
                        emit_terminal_lookup_error(out, lookup.request_id);
                    }
                }
            }
            Ok(pb::base_command::Type::Producer) => {
                if let Some(producer) = &frame.command.producer {
                    state.producer_opens.fetch_add(1, Ordering::SeqCst);
                    emit_transient_error(out, producer.request_id);
                }
            }
            Ok(pb::base_command::Type::Subscribe) => {
                if let Some(subscribe) = &frame.command.subscribe {
                    state.subscribes.fetch_add(1, Ordering::SeqCst);
                    emit_transient_error(out, subscribe.request_id);
                }
            }
            Ok(pb::base_command::Type::Ping) => emit_pong(out),
            _ => {}
        }
        true
    })
    .await;
}

/// A terminal lookup result in an attachment retry leg must surface that exact
/// broker error and must not send another producer-open or subscribe command.
#[tokio::test]
async fn terminal_lookup_failure_stops_provisional_setup_retries() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let state = Arc::new(TerminalLookupGating::default());
    tokio::spawn(run_terminal_lookup_after_provisional_setup_broker(
        listener,
        Arc::clone(&state),
    ));
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = Client::connect_plain_supervised(
        &engine,
        &addr.to_string(),
        fast_give_up_config(),
        None,
        None,
    )
    .await
    .expect("connect must succeed")
    .with_operation_retry(fast_give_up_policy());

    let producer_error = client
        .open_producer(CreateProducerRequest {
            topic: TERMINAL_LOOKUP_PRODUCER_TOPIC.to_owned(),
            ..Default::default()
        })
        .await
        .expect_err("terminal retry-path lookup must fail producer-open");
    assert!(matches!(
        producer_error,
        magnetar_runtime_moonpool::ClientError::Broker {
            code,
            ref message
        } if code == pb::ServerError::AuthorizationError as i32
            && message == "lookup denied after transient attachment failure"
    ));

    let subscribe_error = client
        .subscribe(SubscribeRequest {
            topic: TERMINAL_LOOKUP_CONSUMER_TOPIC.to_owned(),
            subscription: "terminal-lookup".to_owned(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            ..Default::default()
        })
        .await
        .expect_err("terminal retry-path lookup must fail subscribe");
    assert!(matches!(
        subscribe_error,
        magnetar_runtime_moonpool::ClientError::Broker {
            code,
            ref message
        } if code == pb::ServerError::AuthorizationError as i32
            && message == "lookup denied after transient attachment failure"
    ));

    assert_eq!(state.producer_opens.load(Ordering::SeqCst), 1);
    assert_eq!(state.subscribes.load(Ordering::SeqCst), 1);
    client.close().await;
}

#[derive(Clone, Copy)]
enum OwnershipOperation {
    Producer,
    Subscribe,
}

#[derive(Default)]
struct OwnershipMoveGating {
    a_lookups: AtomicU32,
    a_producers: AtomicU32,
    a_subscribes: AtomicU32,
    b_lookups: AtomicU32,
    b_producers: AtomicU32,
    b_subscribes: AtomicU32,
}

async fn run_previous_owner_broker(
    listener: TcpListener,
    redirect_url: String,
    operation: OwnershipOperation,
    state: Arc<OwnershipMoveGating>,
) {
    let Ok((mut stream, _)) = listener.accept().await else {
        return;
    };
    serve_conn(&mut stream, move |frame, out| {
        match pb::base_command::Type::try_from(frame.command.r#type) {
            Ok(pb::base_command::Type::Connect) => emit_connected(out),
            Ok(pb::base_command::Type::Lookup) => {
                if let Some(lookup) = &frame.command.lookup_topic {
                    let attempt = state.a_lookups.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        emit_lookup_response(out, lookup.request_id);
                    } else {
                        emit_lookup_redirect(out, lookup.request_id, &redirect_url);
                    }
                }
            }
            Ok(pb::base_command::Type::Producer)
                if matches!(operation, OwnershipOperation::Producer) =>
            {
                if let Some(producer) = &frame.command.producer {
                    state.a_producers.fetch_add(1, Ordering::SeqCst);
                    emit_broker_error(
                        out,
                        producer.request_id,
                        pb::ServerError::ProducerBusy,
                        "producer owner is moving",
                    );
                }
            }
            Ok(pb::base_command::Type::Subscribe)
                if matches!(operation, OwnershipOperation::Subscribe) =>
            {
                if let Some(subscribe) = &frame.command.subscribe {
                    state.a_subscribes.fetch_add(1, Ordering::SeqCst);
                    emit_broker_error(
                        out,
                        subscribe.request_id,
                        pb::ServerError::ConsumerBusy,
                        "consumer owner is moving",
                    );
                }
            }
            Ok(pb::base_command::Type::Ping) => emit_pong(out),
            _ => {}
        }
        true
    })
    .await;
}

async fn run_new_owner_broker(
    listener: TcpListener,
    operation: OwnershipOperation,
    state: Arc<OwnershipMoveGating>,
) {
    let Ok((mut stream, _)) = listener.accept().await else {
        return;
    };
    serve_conn(&mut stream, move |frame, out| {
        match pb::base_command::Type::try_from(frame.command.r#type) {
            Ok(pb::base_command::Type::Connect) => emit_connected(out),
            Ok(pb::base_command::Type::Lookup) => {
                if let Some(lookup) = &frame.command.lookup_topic {
                    state.b_lookups.fetch_add(1, Ordering::SeqCst);
                    emit_lookup_response(out, lookup.request_id);
                }
            }
            Ok(pb::base_command::Type::Producer)
                if matches!(operation, OwnershipOperation::Producer) =>
            {
                if let Some(producer) = &frame.command.producer {
                    state.b_producers.fetch_add(1, Ordering::SeqCst);
                    emit_producer_success(out, producer.request_id);
                }
            }
            Ok(pb::base_command::Type::Subscribe)
                if matches!(operation, OwnershipOperation::Subscribe) =>
            {
                if let Some(subscribe) = &frame.command.subscribe {
                    state.b_subscribes.fetch_add(1, Ordering::SeqCst);
                    emit_subscribe_success(out, subscribe.request_id);
                }
            }
            Ok(pb::base_command::Type::Ping) => emit_pong(out),
            _ => {}
        }
        true
    })
    .await;
}

async fn ownership_move_client(
    operation: OwnershipOperation,
) -> (Client<TokioProviders>, Arc<OwnershipMoveGating>) {
    let listener_b = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("broker B bind");
    let addr_b = listener_b.local_addr().expect("broker B address");
    let listener_a = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("broker A bind");
    let addr_a = listener_a.local_addr().expect("broker A address");
    let state = Arc::new(OwnershipMoveGating::default());
    tokio::spawn(run_new_owner_broker(
        listener_b,
        operation,
        Arc::clone(&state),
    ));
    tokio::spawn(run_previous_owner_broker(
        listener_a,
        format!("pulsar://{addr_b}"),
        operation,
        Arc::clone(&state),
    ));
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let mut config = fast_give_up_config();
    config.supervisor = Some(magnetar_proto::SupervisorConfig::default());
    let client = Client::connect_plain_supervised(&engine, &addr_a.to_string(), config, None, None)
        .await
        .expect("connect to broker A")
        .with_operation_retry(fast_give_up_policy());
    (client, state)
}

fn assert_ownership_move(
    state: &OwnershipMoveGating,
    expected_producers: u32,
    expected_subscribes: u32,
) {
    assert_eq!(state.a_lookups.load(Ordering::SeqCst), 2);
    assert_eq!(state.a_producers.load(Ordering::SeqCst), expected_producers);
    assert_eq!(
        state.a_subscribes.load(Ordering::SeqCst),
        expected_subscribes
    );
    assert_eq!(state.b_lookups.load(Ordering::SeqCst), 1);
    assert_eq!(state.b_producers.load(Ordering::SeqCst), expected_producers);
    assert_eq!(
        state.b_subscribes.load(Ordering::SeqCst),
        expected_subscribes
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
/// A provisional producer rejected by broker A is removed before the retry;
/// the client re-runs lookup and attaches a fresh handle on redirected broker B.
async fn producer_open_retry_follows_redirect_to_new_owner() {
    let (client, state) = ownership_move_client(OwnershipOperation::Producer).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/producer-owner-move".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("producer ownership retry must not hang")
    .expect("producer ownership retry must attach on broker B");
    assert_ownership_move(&state, 1, 0);
    client.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
/// A provisional consumer rejected by broker A is removed before the retry;
/// the client re-runs lookup and attaches a fresh handle on redirected broker B.
async fn subscribe_retry_follows_redirect_to_new_owner() {
    let (client, state) = ownership_move_client(OwnershipOperation::Subscribe).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        client.subscribe(SubscribeRequest {
            topic: "persistent://public/default/consumer-owner-move".to_owned(),
            subscription: "owner-move".to_owned(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe ownership retry must not hang")
    .expect("subscribe ownership retry must attach on broker B");
    assert_ownership_move(&state, 0, 1);
    client.close().await;
}

#[derive(Default)]
struct ReceiveAcrossDropGating {
    subscribes: AtomicU32,
    retry_lookups: AtomicU32,
    reattached: AtomicBool,
}

/// Two-session supervised broker for the #299 receive-across-drop test. Mirror
/// of the tokio engine's `run_receive_across_drop_broker`.
async fn run_receive_across_drop_broker(
    listener: TcpListener,
    state: Arc<ReceiveAcrossDropGating>,
    consumer_id: u64,
) {
    // Session 1: ack subscribe, drop on the initial flow.
    if let Ok((mut s1, _)) = listener.accept().await {
        let st = Arc::clone(&state);
        serve_conn(&mut s1, move |frame, out| {
            match pb::base_command::Type::try_from(frame.command.r#type) {
                Ok(pb::base_command::Type::Connect) => emit_connected(out),
                Ok(pb::base_command::Type::Lookup) => {
                    if let Some(l) = &frame.command.lookup_topic {
                        emit_lookup_response(out, l.request_id);
                    }
                }
                Ok(pb::base_command::Type::Subscribe) => {
                    if let Some(sub) = &frame.command.subscribe {
                        st.subscribes.fetch_add(1, Ordering::SeqCst);
                        emit_subscribe_success(out, sub.request_id);
                    }
                }
                Ok(pb::base_command::Type::Flow) => return false,
                Ok(pb::base_command::Type::Ping) => emit_pong(out),
                _ => {}
            }
            true
        })
        .await;
        drop(s1);
    }

    for _ in 0..2 {
        let Ok((s_dead, _)) = listener.accept().await else {
            return;
        };
        drop(s_dead);
    }

    // Session 2: reject the rebuilt subscribe once, then answer the driver's
    // retry lookup, re-ack subscribe, and deliver on the rebuild flow.
    if let Ok((mut s2, _)) = listener.accept().await {
        let st = Arc::clone(&state);
        serve_conn(&mut s2, move |frame, out| {
            match pb::base_command::Type::try_from(frame.command.r#type) {
                Ok(pb::base_command::Type::Connect) => emit_connected(out),
                Ok(pb::base_command::Type::Lookup) => {
                    if let Some(l) = &frame.command.lookup_topic {
                        st.retry_lookups.fetch_add(1, Ordering::SeqCst);
                        emit_lookup_response(out, l.request_id);
                    }
                }
                Ok(pb::base_command::Type::Subscribe) => {
                    if let Some(sub) = &frame.command.subscribe {
                        let previous = st.subscribes.fetch_add(1, Ordering::SeqCst);
                        if previous == 1 {
                            emit_transient_error(out, sub.request_id);
                        } else {
                            emit_subscribe_success(out, sub.request_id);
                            st.reattached.store(true, Ordering::SeqCst);
                        }
                    }
                }
                Ok(pb::base_command::Type::Flow) => {
                    if st.reattached.load(Ordering::SeqCst) {
                        emit_message_frame(out, ConsumerHandle(consumer_id), 1, b"after-reconnect");
                    }
                }
                Ok(pb::base_command::Type::Ping) => emit_pong(out),
                _ => {}
            }
            true
        })
        .await;
    }
}

/// #299 (moonpool integration): 1:1 twin of the tokio
/// `receive_across_supervised_drop_resolves_with_message_not_closed`. A
/// `receive()` outstanding across a supervised drop must re-park during the
/// recoverable `Failed` window and resolve with the post-reconnect message,
/// NOT `Err(Closed)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receive_across_supervised_drop_resolves_with_message_not_closed() {
    const CONSUMER_ID: u64 = 0;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let state = Arc::new(ReceiveAcrossDropGating::default());
    tokio::spawn(run_receive_across_drop_broker(
        listener,
        Arc::clone(&state),
        CONSUMER_ID,
    ));

    let config = ConnectionConfig {
        supervisor: Some(magnetar_proto::SupervisorConfig {
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(250),
            ..Default::default()
        }),
        ..Default::default()
    };
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect_plain_supervised(&engine, &addr.to_string(), config, None, None),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed")
    .with_operation_retry(fast_give_up_policy());

    let consumer = tokio::time::timeout(
        Duration::from_secs(5),
        client.subscribe(SubscribeRequest {
            topic: "persistent://public/default/recv-across-drop".to_owned(),
            subscription: "recv-299".to_owned(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe must succeed");

    let msg = tokio::time::timeout(Duration::from_secs(20), consumer.receive())
        .await
        .expect("receive() must resolve after the supervised reconnect, not hang")
        .expect(
            "receive() must transparently resume across a supervised reconnect and deliver \
             the post-reconnect message, NOT surface Err(Closed) during the recoverable \
             Failed window (issue #299)",
        );
    assert_eq!(
        msg.payload.as_ref(),
        b"after-reconnect",
        "the receive() must resolve with the message delivered on the rebuilt session"
    );
    assert_eq!(
        state.subscribes.load(Ordering::SeqCst),
        3,
        "initial attach, rejected rebuild, and successful retry must each reach the wire",
    );
    assert_eq!(
        state.retry_lookups.load(Ordering::SeqCst),
        1,
        "the established-consumer retry must re-run lookup exactly once",
    );

    client.close().await;
}

// ============================================================================
// Virtual-time determinism proof
// ============================================================================

/// Port the in-sim broker binds to (the sim hands every workload its own IP).
const SIM_BROKER_PORT: u16 = 6650;

/// Per-run virtual-time budget. The legitimate path is a handshake, one
/// round-trip, a drop, a redial (a few backoff sleeps), a transient reject,
/// the 2 s `TRANSIENT_RETRY_DELAY` retry sleep, and the re-attach — a handful
/// of simulated seconds. A generous ceiling still trips the orchestrator's
/// no-progress detector on a runaway; a host-clock retry sleep (which never
/// advances virtual time under the single-threaded sim runtime) would wedge
/// the run regardless. Pure function of the simulated schedule → never
/// perturbs replay determinism (ADR-0011).
const SIM_RUN_TIME_BUDGET: Duration = Duration::from_mins(2);

/// Shared script state for the in-sim broker, mirroring [`Gating`] but
/// `Mutex`-guarded so the broker session tasks (spawned on the sim
/// `TaskProvider`) can share it.
#[derive(Default)]
struct SimGating {
    /// `CommandProducer` frames seen on the SECOND session (1st → transient
    /// reject, 2nd → ack). The retry leg only issues the 2nd after its
    /// virtual-time `TRANSIENT_RETRY_DELAY` sleep + lookup.
    session2_producer_opens: u32,
    /// Set once the second session has acked a producer-open.
    session2_acked: bool,
    /// Violation latch: a `CommandSend` reached the broker on session 2 before
    /// the producer was acked (the livelock signature).
    premature_send: bool,
    /// `CommandSend` frames the second session answered with a receipt.
    session2_sends: u32,
}

/// In-sim broker: session 1 handshakes, acks a producer-open, answers one
/// send with a receipt, then DROPS; session 2 handshakes, transiently rejects
/// the rebuild's producer-open (forcing the lookup + retry leg), acks the
/// retry, and answers the replayed send.
struct SimBrokerWorkload {
    gating: Arc<Mutex<SimGating>>,
}

#[async_trait]
impl Workload for SimBrokerWorkload {
    fn name(&self) -> &str {
        "broker"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let network = ctx.network().clone();
        let bind_addr = format!("{}:{SIM_BROKER_PORT}", ctx.my_ip());
        let listener = network
            .bind(&bind_addr)
            .await
            .map_err(|e| SimulationError::InvalidState(format!("broker bind: {e}")))?;

        let shutdown = ctx.shutdown().clone();
        let task = ctx.providers().task().clone();
        // First fully-handshaked session is #1 (drop after the first receipt);
        // every later handshaked session runs the transient-retry script.
        // A shared counter assigns the role; the sim's probabilistic connect
        // faults mean some dials never reach `CONNECT`, so role assignment is
        // gated on the handshake actually completing inside `sim_session`.
        let session_role = Arc::new(Mutex::new(0u32));
        loop {
            moonpool_sim::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let gating = self.gating.clone();
                            let role = session_role.clone();
                            let _handle = task.spawn_task("broker-session", async move {
                                let _ = sim_session(stream, gating, role).await;
                            });
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

/// Drive one in-sim broker session. The session claims its role (first
/// handshaked session → drop-after-first-receipt; later → transient-retry
/// script) only once it answers `CONNECT`, so connect-faulted dials that never
/// handshake do not consume a role.
async fn sim_session<S>(
    mut stream: S,
    gating: Arc<Mutex<SimGating>>,
    session_role: Arc<Mutex<u32>>,
) -> SimulationResult<()>
where
    S: FuturesRead + FuturesWrite + Unpin + Send,
{
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    // 0 = role not yet claimed; 1 = drop-after-first-receipt; 2 = transient
    // retry script.
    let mut my_role = 0u32;
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
                pb::base_command::Type::Connect => {
                    // Claim the next role exactly when the handshake lands.
                    {
                        let mut r = session_role.lock();
                        *r += 1;
                        my_role = *r;
                    }
                    emit_connected(&mut out_buf);
                }
                pb::base_command::Type::Ping => emit_pong(&mut out_buf),
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        emit_lookup_response(&mut out_buf, l.request_id);
                    }
                }
                pb::base_command::Type::Producer => {
                    if let Some(p) = &frame.command.producer {
                        if my_role >= 2 {
                            let mut g = gating.lock();
                            g.session2_producer_opens += 1;
                            if g.session2_producer_opens == 1 {
                                // Rebuild open: transient bundle-not-served →
                                // forces the lookup + retry leg.
                                emit_transient_error(&mut out_buf, p.request_id);
                            } else {
                                // Retry open (issued only after the virtual
                                // `TRANSIENT_RETRY_DELAY` sleep + lookup): ack.
                                emit_producer_success(&mut out_buf, p.request_id);
                                g.session2_acked = true;
                            }
                        } else {
                            emit_producer_success(&mut out_buf, p.request_id);
                        }
                    }
                }
                pb::base_command::Type::Send => {
                    if let Some(send) = &frame.command.send {
                        if my_role >= 2 {
                            let mut g = gating.lock();
                            if !g.session2_acked {
                                g.premature_send = true;
                            }
                            g.session2_sends += 1;
                        }
                        emit_send_receipt(&mut out_buf, send.producer_id, send.sequence_id);
                        if my_role == 1 {
                            // Session 1: flush the receipt, then DROP so the
                            // supervisor redials into the transient script.
                            if stream.write_all(&out_buf).await.is_err() {
                                return Ok(());
                            }
                            let _ = stream.flush().await;
                            return Ok(());
                        }
                    }
                }
                _ => {}
            }
        }

        if !out_buf.is_empty() {
            if stream.write_all(&out_buf).await.is_err() {
                return Ok(());
            }
            if stream.flush().await.is_err() {
                return Ok(());
            }
            out_buf.clear();
        }

        let mut tmp = vec![0u8; 64 * 1024];
        match stream.read(&mut tmp).await {
            Ok(0) | Err(_) => return Ok(()),
            Ok(n) => read_buf.extend_from_slice(&tmp[..n]),
        }
    }
}

/// Client workload: supervised connect, open a producer, one healthy send,
/// then — after the broker drops — a second send whose future stays pending
/// across the redial + transient reject + virtual-time retry leg. Recording a
/// resolved second send proves the retry fired on the injected clock.
struct SimClientWorkload {
    gating: Arc<Mutex<SimGating>>,
    /// `Some(true)` once the replayed send resolved; `Some(false)` on a bounded
    /// failure; `None` if `run()` never reached the assertion (silent park).
    replay_ok: Arc<Mutex<Option<bool>>>,
}

#[async_trait]
impl Workload for SimClientWorkload {
    fn name(&self) -> &str {
        "client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let broker_ip = ctx
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".into()))?;
        let addr = format!("{broker_ip}:{SIM_BROKER_PORT}");
        let engine = MoonpoolEngine::new(ctx.providers().clone());

        let config = ConnectionConfig {
            supervisor: Some(magnetar_proto::SupervisorConfig {
                initial_backoff: Duration::from_millis(50),
                max_backoff: Duration::from_millis(250),
                ..Default::default()
            }),
            ..Default::default()
        };
        let client = Client::connect_plain_supervised(&engine, &addr, config, None, None)
            .await
            .map_err(|e| SimulationError::InvalidState(format!("connect: {e:?}")))?;

        let producer = client
            .open_producer(CreateProducerRequest {
                topic: "persistent://public/default/replay-gating-sim".to_owned(),
                ..Default::default()
            })
            .await
            .map_err(|e| SimulationError::InvalidState(format!("producer open: {e:?}")))?;

        // Healthy round-trip; the broker drops right after this receipt.
        producer
            .send(outgoing(b"one"))
            .await
            .map_err(|e| SimulationError::InvalidState(format!("first send: {e:?}")))?;

        // The second send's future stays pending across the supervised redial,
        // through the TRANSIENT reject + the virtual-time `TRANSIENT_RETRY_DELAY`
        // retry leg, until the post-ack replay's receipt lands. No host
        // `tokio::time::timeout` wrapper: the whole point is that the retry's
        // sleep is VIRTUAL, so the run advances and resolves; a host-clock
        // sleep would never advance the sim clock and the no-progress detector
        // would wedge the run instead.
        let replayed = producer.send(outgoing(b"two")).await;
        let ok = replayed.is_ok();
        *self.replay_ok.lock() = Some(ok);

        client.close().await;

        if ok {
            Ok(())
        } else {
            Err(SimulationError::InvalidState(format!(
                "replayed send failed instead of resolving after the virtual-time retry: \
                 {replayed:?}"
            )))
        }
    }

    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        let g = self.gating.lock();
        if g.premature_send {
            return Err(SimulationError::InvalidState(
                "a CommandSend reached the broker before the retry's ProducerSuccess (livelock)"
                    .into(),
            ));
        }
        if g.session2_producer_opens != 2 {
            return Err(SimulationError::InvalidState(format!(
                "expected 2 producer-opens on session 2 (transient reject + virtual-time retry \
                 ack); saw {}",
                g.session2_producer_opens
            )));
        }
        if g.session2_sends != 1 {
            return Err(SimulationError::InvalidState(format!(
                "the queued send must replay exactly once on the new session; saw {}",
                g.session2_sends
            )));
        }
        match *self.replay_ok.lock() {
            Some(true) => Ok(()),
            Some(false) => Err(SimulationError::InvalidState(
                "replayed send did not resolve after the virtual-time retry".into(),
            )),
            None => Err(SimulationError::InvalidState(
                "client recorded no replay outcome — the retry leg never completed (host-clock \
                 sleep that never advanced virtual time?)"
                    .into(),
            )),
        }
    }
}

/// Virtual-time determinism proof for the transient-retry leg: the transient producer-open retry
/// leg sleeps `TRANSIENT_RETRY_DELAY` on the INJECTED `TimeProvider`, so under
/// `SimProviders` the retry fires deterministically in virtual time. The run
/// terminating (the replayed send resolving after exactly two producer-opens
/// on the redialled session) proves the retry used the sim clock, not a host
/// `tokio::time::sleep` — the structural backstop `check-no-internal-clock`
/// cannot provide. A regression that routed the retry through a host clock
/// would never advance the single-threaded sim runtime's virtual clock and the
/// no-progress detector would wedge the run (caught as a non-terminating
/// `SimulationBuilder::run`).
#[test]
fn transient_producer_open_retry_fires_under_virtual_time() {
    let gating = Arc::new(Mutex::new(SimGating::default()));
    let replay_ok = Arc::new(Mutex::new(None));
    let report = SimulationBuilder::new()
        .run_time_budget(SIM_RUN_TIME_BUDGET)
        .workload(SimBrokerWorkload {
            gating: gating.clone(),
        })
        .workload(SimClientWorkload {
            gating: gating.clone(),
            replay_ok: replay_ok.clone(),
        })
        // Fixed seed: a single deterministic schedule is all the
        // virtual-time proof needs (the moonpool seed sweep in the validation
        // chain re-runs the whole suite across 32 seeds for flakiness).
        .set_debug_seeds(vec![0x4101_5eed_u64])
        .set_iterations(1)
        .run();
    // `run()` returning at all is the virtual-time proof: a host-clock retry
    // sleep would never advance the sim clock and the run would never
    // terminate. `check()` additionally pins the two-open / single-replay
    // shape and the resolved replayed send.
    assert_eq!(
        report.iterations, 1,
        "the run must dispatch and terminate (the retry sleep is virtual): {report:?}",
    );
    assert_eq!(
        report.failed_runs, 0,
        "the transient retry must fire under virtual time and the replayed send must resolve: \
         {report:?}",
    );
}
