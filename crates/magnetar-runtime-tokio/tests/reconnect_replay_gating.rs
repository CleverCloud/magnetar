// SPDX-License-Identifier: Apache-2.0

//! Producer-not-ready replay gating across a supervised reconnect — tokio
//! engine, real loopback broker (ADR-0024 layer b for the proto
//! replay-gating fix).
//!
//! Reproduces the `e2e_reconnect` livelock flow against a scripted broker,
//! with the REAL driver task + supervisor (the sibling
//! `reconnect_with_inflight.rs` pumps `ConnectionShared` manually and
//! cannot see driver-loop ordering):
//!
//! 1. connect → producer open (acked) → one send → receipt — healthy session;
//! 2. the broker DROPS the connection; the client queues a second send whose future stays pending
//!    across the reconnect (transparent replay);
//! 3. the supervisor redials; the rebuild's `CommandProducer` is answered with a TRANSIENT
//!    `ServiceNotReady` ("Please redo the lookup" — the post-restart bundle-not-served case),
//!    forcing the lookup + retry leg;
//! 4. the retry's `CommandProducer` is acked with `CommandProducerSuccess`;
//! 5. the queued send must reach the wire ONLY AFTER that ack (a premature send makes a real broker
//!    close the whole connection — the livelock), and exactly once; the receipt resolves the
//!    user-facing future.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, ConsumerHandle, CreateProducerRequest, Frame, FrameError,
    OperationRetryConfig, SubscribeRequest, decode_one, encode_command, encode_payload, pb,
};
use magnetar_runtime_tokio::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
mod common;
use common::HANG_GUARD;

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
/// signals end-of-session (`Ok(false)`), the peer closes, or an I/O error.
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
/// retry + ack-gated-replay leg.
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
    let url = format!("pulsar://{addr}");

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
    let client = tokio::time::timeout(HANG_GUARD, Client::connect(&url, config))
        .await
        .expect("connect did not time out")
        .expect("connect must succeed");

    let producer = tokio::time::timeout(
        HANG_GUARD,
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
    let _ = tokio::time::timeout(HANG_GUARD, producer.send_bytes(&b"one"[..]))
        .await
        .expect("first send did not time out")
        .expect("first send must succeed");

    // Give the driver a beat to observe the drop, then queue the second send
    // — its future stays pending across the supervised reconnect, through
    // the transient-error + lookup + retry leg, until the post-ack replay's
    // receipt arrives (transparent replay, Java resendMessages parity).
    tokio::time::sleep(Duration::from_millis(200)).await;
    let receipt = tokio::time::timeout(HANG_GUARD, producer.send_bytes(&b"two"[..]))
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

/// Shared script state for the dedicated transient-retry test.
#[derive(Default)]
struct TransientOpenGating {
    /// `CommandProducer` frames seen (1st → transient reject, 2nd → ack).
    producer_opens: AtomicU32,
    /// `CommandLookupTopic` frames seen (the open's lookup, then the retry
    /// leg's re-lookup) — at least 2 by the time the retry's open is acked.
    lookups: AtomicU32,
}

/// Scripted single-session broker for the dedicated transient-retry test:
/// handshake, answer every lookup, transiently reject the FIRST producer-open
/// (`ServiceNotReady` "Please redo the lookup"), then ack the SECOND — the one
/// the client-owned retry loop issues after its delay. The proto layer removes
/// the provisional handle, so the client re-runs lookup and routing, creates a
/// fresh handle, and resolves only on that retry's `ProducerSuccess`.
async fn run_transient_open_broker(listener: TcpListener, state: Arc<TransientOpenGating>) {
    let Ok((mut s, _)) = listener.accept().await else {
        return;
    };
    let st = Arc::clone(&state);
    serve_conn(&mut s, move |frame, out| {
        match pb::base_command::Type::try_from(frame.command.r#type) {
            Ok(pb::base_command::Type::Connect) => emit_connected(out),
            Ok(pb::base_command::Type::Lookup) => {
                if let Some(l) = &frame.command.lookup_topic {
                    st.lookups.fetch_add(1, Ordering::SeqCst);
                    emit_lookup_response(out, l.request_id);
                }
            }
            Ok(pb::base_command::Type::Producer) => {
                if let Some(p) = &frame.command.producer {
                    let n = st.producer_opens.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        emit_transient_error(out, p.request_id);
                    } else {
                        emit_producer_success(out, p.request_id);
                    }
                }
            }
            Ok(pb::base_command::Type::Ping) => emit_pong(out),
            _ => {}
        }
        true
    })
    .await;
}

/// Dedicated tokio coverage for the client-owned provisional producer-open
/// retry loop, the 1:1
/// twin of the moonpool engine's
/// `transient_producer_open_retry_fires_under_virtual_time`
/// (ADR-0024 runtime-test-parity). The combined
/// `queued_send_replays_only_after_retry_ack_across_reconnect` exercises this
/// leg only as one step of a reconnect-replay flow; this isolates it: a single
/// healthy session whose FIRST producer-open is transiently rejected must still
/// yield a live producer once the retry's open is acked — never surface the
/// transient reject as an open error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_open_recovers_after_transient_reject() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let state = Arc::new(TransientOpenGating::default());
    tokio::spawn(run_transient_open_broker(listener, Arc::clone(&state)));
    let url = format!("pulsar://{addr}");

    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed");

    // The transient reject must NOT fail the open: the retry leg re-looks-up
    // and re-opens after its delay, and the open resolves on the retry's ack.
    let producer = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/transient-open".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("producer open must resolve after the transient retry — not hang")
    .expect("producer open must succeed once the retry's open is acked");

    assert_eq!(
        state.producer_opens.load(Ordering::SeqCst),
        2,
        "first open transiently rejected + retry open acked",
    );
    assert!(
        state.lookups.load(Ordering::SeqCst) >= 2,
        "the retry leg must re-issue a lookup before re-opening (Pulsar 'redo the lookup')",
    );

    // Drop the producer rather than `close()`-ing it: the scripted broker does
    // not answer `CommandCloseProducer`, and `client.close()` tears the socket
    // down without needing a broker close-ack (the recovered-open assertion
    // above is the contract under test).
    drop(producer);
    client.close().await;
}

/// Shared script state for the #302 give-up tests: count transient rejects so
/// the test can assert the retry leg re-armed MULTIPLE times before the proto
/// layer terminalized the open / subscribe.
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

/// Broker that NEVER acks the producer-open: every `CommandProducer` is bounced
/// with a transient `ServiceNotReady`. The client-owned provisional retry loop
/// keeps re-issuing lookup + open; after the configured retry count it returns
/// the last broker error (issue #302, ADR-0080) so the user's `open_producer`
/// future resolves `Err`
/// instead of hanging forever.
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

/// #302 (tokio integration, producer give-up): the bounded retry loop must
/// re-arm across the configured transient rejects and then SURFACE an `Err`
/// once the client-owned operation budget is exhausted — never hang `open_producer` forever (the
/// pre-fix one-shot retry, after a single failure, left the future PENDING
/// behind the closed `broker_ready` drain gate).
/// Uses a 1 ms retry policy so the real-loopback test stays fast without
/// mixing Tokio's paused clock with OS socket readiness. 1:1 twin of the
/// moonpool engine's give-up test (ADR-0024).
#[tokio::test]
async fn producer_open_gives_up_with_error_after_budget_exhausted() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let state = Arc::new(GiveUpGating::default());
    tokio::spawn(run_always_transient_producer_broker(
        listener,
        Arc::clone(&state),
    ));
    let url = format!("pulsar://{addr}");

    let client = Client::connect(&url, fast_give_up_config())
        .await
        .expect("connect must succeed")
        .with_operation_retry(fast_give_up_policy());

    // The open must RESOLVE (with Err), not hang. The 1 ms retry policy keeps
    // the real-loopback test prompt.
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
}

/// Broker that NEVER acks the subscribe: every `CommandSubscribe` is bounced
/// with a transient `ServiceNotReady`. Consumer twin of
/// [`run_always_transient_producer_broker`].
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

/// #302 (tokio integration, consumer give-up): the bounded subscribe-retry
/// loop must re-arm across MANY transient rejects and then make `receive()`
/// surface an `Err` once the client-owned operation budget is exhausted — never block
/// `receive()` forever on a subscription that will never reattach. 1:1 twin of
/// the moonpool engine's give-up test (ADR-0024).
#[tokio::test]
async fn subscribe_gives_up_with_error_after_budget_exhausted() {
    use magnetar_proto::SubscribeRequest;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let state = Arc::new(GiveUpGating::default());
    tokio::spawn(run_always_transient_subscribe_broker(
        listener,
        Arc::clone(&state),
    ));
    let url = format!("pulsar://{addr}");

    let client = Client::connect(&url, fast_give_up_config())
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
        magnetar_runtime_tokio::ClientError::Broker { .. }
    ));
    assert_eq!(
        state.subscribes.load(Ordering::SeqCst),
        3,
        "one initial subscribe plus two configured retries must reach the wire"
    );

    client.close().await;
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
    let client = Client::connect(&format!("pulsar://{addr}"), fast_give_up_config())
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
        magnetar_runtime_tokio::ClientError::Broker {
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
        magnetar_runtime_tokio::ClientError::Broker {
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
) -> (Client, Arc<OwnershipMoveGating>) {
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
    let client = Client::connect(&format!("pulsar://{addr_a}"), fast_give_up_config())
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
        HANG_GUARD,
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
        HANG_GUARD,
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

/// A `CommandMessage` + payload addressed to `handle` at `(ledger, entry)`.
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

/// Script state for the #299 receive-across-drop test.
#[derive(Default)]
struct ReceiveAcrossDropGating {
    /// `CommandSubscribe` frames seen across both sessions (session 1 + the
    /// rebuild on session 2).
    subscribes: AtomicU32,
    /// Retry-path lookups issued after the rebuilt subscribe is rejected.
    retry_lookups: AtomicU32,
    /// Set after the retry subscribe is acknowledged; only then may a flow
    /// trigger post-reconnect delivery.
    reattached: AtomicBool,
}

/// Two-session supervised broker for the #299 receive-across-drop test.
///
/// Session 1: handshake, ack the subscribe (consumer is live), then DROP the
/// socket as soon as the consumer's initial `CommandFlow` arrives — the
/// `receive()` the test started is now parked across a transport drop, and the
/// connection sits in `HandshakeState::Failed` for the whole reconnect window.
///
/// Session 2: handshake, reject the rebuilt subscribe once, answer the
/// driver's retry lookup, re-ack subscribe, and on the rebuild's `CommandFlow`
/// deliver one `CommandMessage`. A correct client re-parks the
/// `receive()` during the recoverable `Failed` window (issue #299) and resolves
/// it with THIS message — the pre-fix `is_closed()` guard instead resolved
/// `Err(Closed)` the instant `reset()` woke the parked receiver mid-`Failed`.
async fn run_receive_across_drop_broker(
    listener: TcpListener,
    state: Arc<ReceiveAcrossDropGating>,
    consumer_id: u64,
) {
    // ── Session 1: ack subscribe, drop on the initial flow. ──
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
                Ok(pb::base_command::Type::Flow) => {
                    // Consumer is live and has granted permits; the receive() is
                    // now parked. Drop the session to trigger the supervised
                    // reconnect window.
                    return false;
                }
                Ok(pb::base_command::Type::Ping) => emit_pong(out),
                _ => {}
            }
            true
        })
        .await;
        drop(s1);
    }

    // A couple of failed redials (mirrors the docker-restart window).
    for _ in 0..2 {
        let Ok((s_dead, _)) = listener.accept().await else {
            return;
        };
        drop(s_dead);
    }

    // ── Session 2: reject rebuild once, then retry and deliver. ──
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
                    // Ignore any pre-ack flow; delivery starts only after the
                    // successful retry subscribe reopens the flow gate.
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

/// #299 (tokio integration): a `receive()` outstanding ACROSS a transport drop
/// on a SUPERVISED connection must NOT resolve `Err(Closed)` during the
/// recoverable `Failed` window — it must re-park and transparently resolve with
/// a delivered message once the supervisor reconnects and the rebuild replays
/// `CommandSubscribe`. The pre-fix `is_closed()`-based receive guard errored
/// the instant `reset()` woke the parked receiver mid-`Failed`; the
/// `consumer_handle_is_terminal` guard re-parks instead. No existing test polls
/// `receive()` during the `Failed` window. 1:1 twin of the moonpool engine's
/// receive-across-drop test (ADR-0024).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receive_across_supervised_drop_resolves_with_message_not_closed() {
    // The consumer id the broker addresses the post-reconnect message to. The
    // first consumer on a fresh client is id 0 (the proto allocates from 0).
    const CONSUMER_ID: u64 = 0;

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let state = Arc::new(ReceiveAcrossDropGating::default());
    tokio::spawn(run_receive_across_drop_broker(
        listener,
        Arc::clone(&state),
        CONSUMER_ID,
    ));
    let url = format!("pulsar://{addr}");

    let config = ConnectionConfig {
        supervisor: Some(magnetar_proto::SupervisorConfig {
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(250),
            ..Default::default()
        }),
        ..Default::default()
    };
    let client = tokio::time::timeout(HANG_GUARD, Client::connect(&url, config))
        .await
        .expect("connect did not time out")
        .expect("connect must succeed")
        .with_operation_retry(fast_give_up_policy());

    let consumer = tokio::time::timeout(
        HANG_GUARD,
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

    // The crux: this receive() is outstanding across the drop. It must resolve
    // with the post-reconnect message — NOT Err(Closed) during the recoverable
    // Failed window.
    let msg = tokio::time::timeout(HANG_GUARD, consumer.receive())
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
