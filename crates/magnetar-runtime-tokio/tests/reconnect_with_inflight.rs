// SPDX-License-Identifier: Apache-2.0

//! Stage 3 transparent in-flight publish replay across a supervised reset, exercised
//! through the tokio engine's [`ConnectionShared`] state. Mirrors the matching
//! `magnetar-runtime-moonpool` test (and its proto-level unit tests in
//! [`magnetar_proto::conn`]); the goal is to pin the at-least-once publish parity contract
//! end-to-end in the tokio engine's shared-state surface, without spinning up a TCP
//! listener (the wire surface is exercised by the e2e tests in
//! `crates/magnetar/tests/e2e_reconnect.rs`).
//!
//! Contract — mirrors Java `ProducerImpl#resendMessages`:
//!
//! 1. `Connection::reset` does NOT install
//!    [`OpOutcome::SessionLost`](magnetar_proto::OpOutcome::SessionLost) on the publish key. The
//!    user-facing `SendFut` polls, finds no outcome, re-registers, and stays pending across the
//!    reconnect.
//! 2. The in-flight publishes are snapshotted on the connection and `rebuild_producers` replays
//!    them onto the new session in original FIFO order with their original sequence ids.
//! 3. When the broker's `CommandSendReceipt` arrives for a replayed publish, the user-facing future
//!    resolves with `OpOutcome::SendReceipt` as if the original session had simply lasted longer.

use std::sync::Arc;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, OpOutcome, PendingOpKey, ProducerHandle, SequenceId,
    encode_command, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

mod common;
use common::handshake_response_bytes;

fn handshake_complete(at: Instant) -> Arc<ConnectionShared> {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(at, &handshake_response_bytes())
            .expect("connected");
        let _ = conn.poll_event();
    }
    shared
}

/// Feed the broker's `CommandProducerSuccess` for `request_id` — the ack
/// that opens the producer-not-ready drain gate and triggers the snapshot
/// replay (Java `handleProducerSuccess` parity). Every rebuild/retry leg in
/// these tests needs this step before replayed SEND frames may reach the
/// wire.
fn ack_producer_open(shared: &Arc<ConnectionShared>, request_id: u64, at: Instant) {
    let success = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: "magnetar-test-reattach".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: None,
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &success).expect("encode CommandProducerSuccess");
    let mut conn = shared.inner.lock();
    conn.handle_bytes(at, &buf).expect("apply ProducerSuccess");
    while conn.poll_event().is_some() {}
}

fn open_producer_ready(shared: &Arc<ConnectionShared>, topic: &str, at: Instant) -> ProducerHandle {
    let req = CreateProducerRequest {
        topic: topic.to_owned(),
        ..Default::default()
    };
    let (handle, request_id) = {
        let mut conn = shared.inner.lock();
        let request_id = conn.peek_next_request_id_for_test();
        let handle = conn.create_producer(req);
        (handle, request_id)
    };
    let success = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: format!("magnetar-test-{}", handle.0),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: None,
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &success).expect("encode CommandProducerSuccess");
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(at, &buf).expect("apply ProducerSuccess");
        let _ = conn.poll_event();
    }
    handle
}

fn send_receipt_bytes(producer: ProducerHandle, sequence_id: SequenceId) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::SendReceipt as i32,
        send_receipt: Some(pb::CommandSendReceipt {
            producer_id: producer.0,
            sequence_id: sequence_id.0,
            message_id: Some(pb::MessageIdData {
                ledger_id: 7,
                entry_id: sequence_id.0,
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
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandSendReceipt");
    buf
}

// End-to-end snapshot-and-replay scenario. The flow is linear by design
// (wire trace stays readable in one body); splitting would obscure the
// scenario. Silence the per-function line cap.
#[allow(clippy::too_many_lines)]
#[test]
fn reset_snapshots_inflight_publishes_for_transparent_replay() {
    const INFLIGHT_COUNT: u64 = 5;

    let t0 = Instant::now();
    let shared = handshake_complete(t0);
    let handle = open_producer_ready(&shared, "persistent://public/default/inflight", t0);

    // Queue several in-flight publishes — no receipt arrives.
    let mut seqs: Vec<SequenceId> = Vec::with_capacity(INFLIGHT_COUNT as usize);
    {
        let mut conn = shared.inner.lock();
        for i in 0..INFLIGHT_COUNT {
            let payload = Bytes::from(format!("in-flight-{i}"));
            let len = payload.len() as u32;
            let seq = conn
                .send(
                    handle,
                    OutgoingMessage {
                        payload,
                        metadata: pb::MessageMetadata::default(),
                        uncompressed_size: len,
                        num_messages: 1,
                        txn_id: None,
                        source_message_id: None,
                    },
                    0,
                    t0,
                )
                .expect("queue send");
            seqs.push(seq);
        }
    }
    assert_eq!(
        shared.inner.lock().producer_pending_count(handle),
        INFLIGHT_COUNT as usize,
    );

    // Drain the wire frames so we observe the post-rebuild wire activity in isolation.
    {
        let mut conn = shared.inner.lock();
        let _ = conn.poll_transmit();
    }

    let epoch_before = shared.inner.lock().session_epoch();

    // Supervised reset.
    shared.inner.lock().reset();

    // Stage 3 contract: no SessionLost outcome lands on the publish keys (transparent
    // replay).
    for seq in &seqs {
        let key = PendingOpKey::Send(handle, *seq);
        let outcome = shared.inner.lock().take_outcome(key);
        assert!(
            outcome.is_none(),
            "transparent replay — no SessionLost on publish key (got {outcome:?})"
        );
    }
    {
        let conn = shared.inner.lock();
        assert_eq!(conn.producer_pending_count(handle), 0);
        assert_eq!(
            conn.in_flight_publish_snapshot_len(handle),
            INFLIGHT_COUNT as usize,
            "every in-flight publish is snapshotted",
        );
        assert_eq!(conn.session_epoch(), epoch_before.wrapping_add(1));
    }

    // Walk a synthetic re-handshake + rebuild on the new session.
    let rebuild_rid = {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("Connected on retry");
        let _ = conn.poll_event();
        let rebuilt = conn.rebuild_producers();
        assert_eq!(rebuilt.len(), 1, "the surviving producer must be rebuilt");
        rebuilt[0]
    };
    // Producer-not-ready gate: until the broker acks the re-attachment, the
    // snapshots stay parked and no SEND may reach the wire — only the
    // rebuild's CommandProducer goes out.
    {
        let conn = shared.inner.lock();
        assert_eq!(
            conn.in_flight_publish_snapshot_len(handle),
            INFLIGHT_COUNT as usize,
            "snapshots stay parked until the broker acks the re-attachment"
        );
        assert_eq!(conn.producer_pending_count(handle), 0);
    }
    ack_producer_open(&shared, rebuild_rid.0, t0);
    {
        let conn = shared.inner.lock();
        assert_eq!(
            conn.in_flight_publish_snapshot_len(handle),
            0,
            "the re-attach ack consumes the snapshot"
        );
        assert_eq!(
            conn.producer_pending_count(handle),
            INFLIGHT_COUNT as usize,
            "the ack reinstalls every snapshotted OpSend"
        );
    }

    // Drain the post-ack wire frames — must include one CommandProducer (the
    // re-attach) + INFLIGHT_COUNT CommandSends in original sequence-id order.
    let mut cursor = {
        let mut conn = shared.inner.lock();
        conn.poll_transmit()
    };
    let mut sends: Vec<u64> = Vec::new();
    while !cursor.is_empty() {
        let frame = magnetar_proto::frame::decode_one(&mut cursor).expect("decode frame");
        if frame.command.r#type == pb::base_command::Type::Send as i32
            && let Some(s) = frame.command.send.as_ref()
        {
            sends.push(s.sequence_id);
        }
    }
    assert_eq!(
        sends,
        seqs.iter().map(|s| s.0).collect::<Vec<u64>>(),
        "replay preserves FIFO + original sequence ids"
    );

    // Feed the broker's CommandSendReceipt for each replayed sequence id — every
    // user-facing future would now resolve transparently.
    for seq in &seqs {
        let receipt = send_receipt_bytes(handle, *seq);
        shared
            .inner
            .lock()
            .handle_bytes(t0, &receipt)
            .expect("apply receipt");
    }
    {
        let mut conn = shared.inner.lock();
        for seq in &seqs {
            let key = PendingOpKey::Send(handle, *seq);
            match conn.take_outcome(key) {
                Some(OpOutcome::SendReceipt { sequence_id, .. }) => {
                    assert_eq!(sequence_id, *seq);
                }
                other => panic!("expected SendReceipt for {seq:?}, got {other:?}"),
            }
        }
        assert_eq!(
            conn.producer_pending_count(handle),
            0,
            "every replayed send is drained on its receipt"
        );
    }
}

/// Replayed publishes still resolve their user-facing futures when the broker's
/// `CommandSendReceipt` arrives on the new session. The tokio mirror of the
/// equivalent moonpool test — same shape, same assertions; the only thing that differs
/// is which engine owns the [`ConnectionShared`]. Pins the cross-engine equivalence
/// the differential harness relies on (ADR-0024).
#[test]
fn replayed_send_resolves_when_receipt_arrives_on_new_session() {
    let t0 = Instant::now();
    let shared = handshake_complete(t0);
    let handle = open_producer_ready(&shared, "persistent://public/default/replay-ok", t0);

    let seq = {
        let mut conn = shared.inner.lock();
        conn.send(
            handle,
            OutgoingMessage {
                payload: Bytes::from_static(b"survive-me"),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 10,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            },
            0,
            t0,
        )
        .expect("queue send")
    };

    {
        let mut conn = shared.inner.lock();
        let _ = conn.poll_transmit();
    }

    shared.inner.lock().reset();
    let key = PendingOpKey::Send(handle, seq);
    assert!(
        shared.inner.lock().take_outcome(key).is_none(),
        "transparent replay: no SessionLost outcome installed"
    );

    let rebuild_rids = {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("Connected on retry");
        let _ = conn.poll_event();
        conn.rebuild_producers()
    };
    // The replay materialises on the broker's re-attach ack
    // (producer-not-ready gate).
    ack_producer_open(&shared, rebuild_rids[0].0, t0);
    {
        let mut conn = shared.inner.lock();
        let _ = conn.poll_transmit();
    }

    {
        let mut conn = shared.inner.lock();
        let receipt = send_receipt_bytes(handle, seq);
        conn.handle_bytes(t0, &receipt).expect("apply receipt");
    }

    match shared.inner.lock().take_outcome(key) {
        Some(OpOutcome::SendReceipt {
            sequence_id,
            message_id,
        }) => {
            assert_eq!(sequence_id, seq);
            assert_eq!(message_id.ledger_id, 7);
            assert_eq!(message_id.entry_id, seq.0);
        }
        other => panic!("expected SendReceipt for replayed send, got {other:?}"),
    }
    assert_eq!(
        shared.inner.lock().producer_pending_count(handle),
        0,
        "the replayed OpSend drains on receipt"
    );
}

/// FIFO ordering invariant — tokio mirror of the moonpool ordering test. Three publishes,
/// reset mid-flight, rebuild must replay them in original order with original sequence
/// ids. Pins the cross-engine equivalence of `rebuild_producers` (ADR-0024).
#[test]
fn replay_preserves_fifo_ordering_across_rebuild() {
    let t0 = Instant::now();
    let shared = handshake_complete(t0);
    let handle = open_producer_ready(&shared, "persistent://public/default/replay-fifo", t0);

    let payloads: [&[u8]; 3] = [b"alpha", b"beta", b"gamma"];
    let mut seqs: Vec<SequenceId> = Vec::with_capacity(3);
    {
        let mut conn = shared.inner.lock();
        for p in &payloads {
            let seq = conn
                .send(
                    handle,
                    OutgoingMessage {
                        payload: Bytes::from(p.to_vec()),
                        metadata: pb::MessageMetadata::default(),
                        uncompressed_size: p.len() as u32,
                        num_messages: 1,
                        txn_id: None,
                        source_message_id: None,
                    },
                    0,
                    t0,
                )
                .expect("queue");
            seqs.push(seq);
        }
        let _ = conn.poll_transmit();
    }

    shared.inner.lock().reset();
    let rebuild_rids = {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("Connected on retry");
        let _ = conn.poll_event();
        conn.rebuild_producers()
    };
    // The replay materialises on the broker's re-attach ack
    // (producer-not-ready gate).
    ack_producer_open(&shared, rebuild_rids[0].0, t0);

    let mut cursor = {
        let mut conn = shared.inner.lock();
        conn.poll_transmit()
    };
    let mut send_seqs: Vec<u64> = Vec::new();
    let mut send_payloads: Vec<Vec<u8>> = Vec::new();
    while !cursor.is_empty() {
        let frame = magnetar_proto::frame::decode_one(&mut cursor).expect("decode frame");
        if frame.command.r#type == pb::base_command::Type::Send as i32 {
            if let Some(s) = frame.command.send.as_ref() {
                send_seqs.push(s.sequence_id);
            }
            if let Some(body) = frame.payload.as_ref() {
                send_payloads.push(body.body.to_vec());
            }
        }
    }
    assert_eq!(
        send_seqs,
        seqs.iter().map(|s| s.0).collect::<Vec<u64>>(),
        "rebuild must replay the OpSends in their original sequence-id order"
    );
    let expected_payloads: Vec<Vec<u8>> = payloads.iter().map(|p| p.to_vec()).collect();
    assert_eq!(
        send_payloads, expected_payloads,
        "rebuild must replay the OpSends in their original payload order"
    );
}

// ---------------------------------------------------------------------------
// Issue #369 — send_timeout must surface for a publish RELOCATED across a
// supervised reconnect, instead of hanging for the whole reconnect budget.
//
// Everything above this point in the file exercises `ConnectionShared`
// directly (no real socket, no supervisor). That harness cannot reach this
// bug: it never drives `magnetar_runtime_tokio::Client`'s supervised
// `driver_loop`, so it never observes the real reconnect timing this issue is
// about. This section adds a small real-socket, single-purpose fault
// injector (a fake broker over loopback TCP) — the file had no such harness
// to "reuse" for this scenario.
// ---------------------------------------------------------------------------

mod issue_369_send_timeout_across_reconnect {
    use std::net::SocketAddr;
    use std::time::Duration;

    use bytes::BytesMut;
    use magnetar_proto::{
        AntiThrashThreshold, ConnectionConfig, CreateProducerRequest, FrameError, SupervisorConfig,
        decode_one, encode_command, pb,
    };
    use magnetar_runtime_tokio::{Client, ClientError};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use crate::common::HANG_GUARD;

    fn emit_connected(out: &mut BytesMut) {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-issue-369-test".to_owned(),
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
                producer_name: "issue-369-test".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: Some(0),
                producer_ready: Some(true),
            }),
            ..Default::default()
        };
        let _ = encode_command(out, &cmd);
    }

    /// First session: answer CONNECT / LOOKUP / PRODUCER normally, then on the
    /// FIRST `CommandSend` frame close the socket immediately without ever
    /// sending a `CommandSendReceipt` or `CommandSendError` — a mid-publish
    /// drop. This is what drives the supervisor's `Connection::reset()`,
    /// which relocates the in-flight publish into `in_flight_publish_snapshots`
    /// (issue #369's root cause).
    async fn handle_first_session_drop_on_send(mut stream: TcpStream) -> std::io::Result<()> {
        let mut read_buf = BytesMut::with_capacity(64 * 1024);
        let mut out_buf = BytesMut::new();
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
                        // Mid-publish drop: no receipt, no error — just gone.
                        return Ok(());
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

    /// Every subsequent redial: accept the TCP connection (so the supervisor's
    /// dial genuinely succeeds and `Connection::reset()` fires) but never write
    /// a single byte back — a loopback black hole. The handshake never
    /// completes, so the supervisor's reconnect stays outstanding for as long
    /// as its (deliberately generous) attempt budget allows, while the
    /// relocated send's `send_timeout` deadline is what this test expects to
    /// fire first.
    async fn hold_blackhole(stream: TcpStream) {
        let _stream = stream;
        std::future::pending::<()>().await;
    }

    async fn spawn_broker() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            let mut first = true;
            loop {
                let Ok((stream, _peer)) = listener.accept().await else {
                    return;
                };
                if first {
                    first = false;
                    tokio::spawn(async move {
                        let _ = handle_first_session_drop_on_send(stream).await;
                    });
                } else {
                    tokio::spawn(hold_blackhole(stream));
                }
            }
        });
        addr
    }

    fn generous_supervisor() -> SupervisorConfig {
        SupervisorConfig {
            initial_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(20),
            // Comfortably longer than the send_timeout under test — the
            // supervisor must still be trying (not have given up) when the
            // send-timeout sweep fires, proving the fix (not a give-up path)
            // resolved the send.
            mandatory_stop: Duration::from_mins(1),
            max_attempts: Some(10_000),
            anti_thrash_threshold: Some(AntiThrashThreshold {
                successful_attaches: 3,
                window: Duration::from_secs(5),
                drop_within: Duration::from_millis(200),
            }),
            drop_grace: Duration::from_millis(500),
            max_backoff_after_thrash: Duration::from_millis(60),
        }
    }

    /// Issue #369 acceptance test: a publish relocated by `Connection::reset()`
    /// across a supervised reconnect surfaces its configured `send_timeout`
    /// error, instead of parking for the supervisor's entire (much longer)
    /// reconnect budget.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_times_out_for_publish_relocated_across_supervised_reconnect() {
        const SEND_TIMEOUT: Duration = Duration::from_millis(600);

        let addr = spawn_broker().await;
        let url = format!("pulsar://{addr}");

        let cfg = ConnectionConfig {
            supervisor: Some(generous_supervisor()),
            ..ConnectionConfig::default()
        };

        let client = tokio::time::timeout(HANG_GUARD, Client::connect(&url, cfg))
            .await
            .expect("connect did not time out")
            .expect("connect ok");

        let producer = tokio::time::timeout(
            HANG_GUARD,
            client.open_producer(CreateProducerRequest {
                topic: "persistent://public/default/issue-369-send-timeout".to_owned(),
                send_timeout: Some(SEND_TIMEOUT),
                ..Default::default()
            }),
        )
        .await
        .expect("open_producer did not time out")
        .expect("open_producer ok");

        let send_started = std::time::Instant::now();
        // Well before the supervisor's `mandatory_stop` / `max_attempts`
        // budget could plausibly be exhausted (60s / 10_000 attempts), but
        // generous enough that CI scheduling jitter cannot trip it as a false
        // hang — the assertion is on the ERROR arriving, not on tight timing.
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            producer.send_bytes("relocated-across-reconnect"),
        )
        .await
        .expect("send must resolve well before the supervisor's reconnect budget");
        let elapsed = send_started.elapsed();

        match result {
            Err(ClientError::SendRejected { code, message }) => {
                assert_eq!(code, -1, "send-timeout SendError uses the -1 sentinel");
                assert_eq!(message, "send timeout");
            }
            other => panic!("expected a send-timeout SendRejected error, got {other:?}"),
        }
        assert!(
            elapsed < Duration::from_secs(5),
            "send-timeout must fire on roughly the {SEND_TIMEOUT:?} deadline, not the \
             supervisor's reconnect budget (elapsed={elapsed:?})"
        );

        if let Some(d) = client.take_driver() {
            d.abort();
        }
        drop(client);
    }
}
