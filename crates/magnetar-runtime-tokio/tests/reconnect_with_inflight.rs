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

fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandConnected");
    buf
}

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
        let mut tx_buf: Vec<u8> = Vec::new();
        let _ = conn.poll_transmit(&mut tx_buf);
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
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("Connected on retry");
        let _ = conn.poll_event();
        let rebuilt = conn.rebuild_producers();
        assert_eq!(rebuilt.len(), 1, "the surviving producer must be rebuilt");
    }
    {
        let conn = shared.inner.lock();
        assert_eq!(
            conn.in_flight_publish_snapshot_len(handle),
            0,
            "rebuild_producers consumes the snapshot"
        );
        assert_eq!(
            conn.producer_pending_count(handle),
            INFLIGHT_COUNT as usize,
            "rebuild reinstalls every snapshotted OpSend"
        );
    }

    // Drain the post-rebuild wire frames — must include one CommandProducer (the
    // re-attach) + INFLIGHT_COUNT CommandSends in original sequence-id order.
    let raw_bytes = {
        let mut conn = shared.inner.lock();
        let mut buf: Vec<u8> = Vec::new();
        let _ = conn.poll_transmit(&mut buf);
        buf
    };
    let mut cursor = Bytes::copy_from_slice(&raw_bytes);
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
            },
            0,
            t0,
        )
        .expect("queue send")
    };

    {
        let mut conn = shared.inner.lock();
        let mut tx_buf: Vec<u8> = Vec::new();
        let _ = conn.poll_transmit(&mut tx_buf);
    }

    shared.inner.lock().reset();
    let key = PendingOpKey::Send(handle, seq);
    assert!(
        shared.inner.lock().take_outcome(key).is_none(),
        "transparent replay: no SessionLost outcome installed"
    );

    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("Connected on retry");
        let _ = conn.poll_event();
        let _ = conn.rebuild_producers();
    }
    {
        let mut conn = shared.inner.lock();
        let mut tx_buf: Vec<u8> = Vec::new();
        let _ = conn.poll_transmit(&mut tx_buf);
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
                    },
                    0,
                    t0,
                )
                .expect("queue");
            seqs.push(seq);
        }
        let mut tx_buf: Vec<u8> = Vec::new();
        let _ = conn.poll_transmit(&mut tx_buf);
    }

    shared.inner.lock().reset();
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("re-handshake");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("Connected on retry");
        let _ = conn.poll_event();
        let _ = conn.rebuild_producers();
    }

    let raw_bytes = {
        let mut conn = shared.inner.lock();
        let mut buf: Vec<u8> = Vec::new();
        let _ = conn.poll_transmit(&mut buf);
        buf
    };
    let mut cursor = Bytes::copy_from_slice(&raw_bytes);
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
