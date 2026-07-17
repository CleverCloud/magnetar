// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d): tokio ↔ moonpool parity for the issue-#326 fix — a
//! cumulative ack must prune every `batch_ack_tracker` entry it covers, not
//! just the entry of the acked id itself.
//!
//! This replays the production workload that surfaced the leak (the otelgw
//! accesslogs converter): a continuous stream of BATCHED broker entries
//! consumed by a watermark acker that only ever sends cumulative acks, one
//! every `ACK_EVERY` entries, and never an individual ack. Before the fix the
//! tracker kept every entry below the cumulative position — the
//! `pending_batch_acks` gauge grew monotonically with entries consumed
//! (~one `BatchAckEntry` per batch, forever) until reconnect; the converter
//! fleet ran out of memory at ~24 GiB every 4-6 h on exactly this pattern.
//!
//! Why this drives the engines' `ConnectionShared` directly instead of the
//! `ScriptedBroker` runner: the scripted broker dispatches only non-batched
//! entries (`batch_size: 0`), so it cannot stamp the PIP-54 per-batch tracker
//! at all. Both engines wrap the SAME `magnetar_proto::Connection` behind a
//! `parking_lot::Mutex`, so feeding both an identical synthetic frame sequence
//! and comparing the `pending_batch_acks` trajectory is the `EventStream`-parity
//! claim the harness exists to make — at the proto seam the two engines share,
//! exactly like `nack_unacked_removal_equivalence.rs`.

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    AckRequest, Connection, ConnectionConfig, ConsumerHandle, MessageId, SubscribeRequest,
    encode_command, encode_payload, pb,
};

/// Batched entries delivered over the run.
const ENTRIES: u64 = 100;
/// Sub-messages packed into each batched entry (`num_messages_in_batch`).
const BATCH_SIZE: i32 = 3;
/// Watermark cadence: one cumulative ack per this many entries.
const ACK_EVERY: u64 = 10;

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

/// Deliver one synthetic BATCH of [`BATCH_SIZE`] messages on `(ledger, entry)`:
/// `num_messages_in_batch` metadata plus a `(u32 single_size)(SingleMessageMetadata)
/// (payload)` packed body, matching the wire format `ConsumerState::deliver`
/// explodes — each batched entry stamps exactly one `batch_ack_tracker` entry.
fn deliver_batch(
    conn: &mut Connection,
    t0: Instant,
    handle: ConsumerHandle,
    ledger: u64,
    entry: u64,
) {
    let msg_cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: ledger,
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
        num_messages_in_batch: Some(BATCH_SIZE),
        ..Default::default()
    };
    let mut body = BytesMut::new();
    for idx in 0..BATCH_SIZE {
        let payload = format!("cumulative-prune-{entry}-{idx}").into_bytes();
        let sm = pb::SingleMessageMetadata {
            payload_size: payload.len() as i32,
            ..Default::default()
        };
        let sm_len = prost::Message::encoded_len(&sm);
        body.extend_from_slice(&(sm_len as u32).to_be_bytes());
        prost::Message::encode(&sm, &mut body).expect("encode SingleMessageMetadata");
        body.extend_from_slice(&payload);
    }
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &msg_cmd, &metadata, &body).expect("encode batch frame");
    conn.handle_bytes(t0, &frame).expect("deliver batch");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
}

/// Drive the batched-consume + cumulative-only-ack workload over one engine's
/// locked `Connection` and collect the `pending_batch_acks` gauge sampled after
/// every delivery and every cumulative ack. Engine-agnostic: the caller hands in
/// a `&mut Connection` obtained from either engine's `ConnectionShared`, so the
/// two trajectories are comparable element-for-element.
fn lock_and_run(conn: &mut Connection, t0: Instant) -> Vec<usize> {
    // Handshake.
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    // Plain durable subscription — no ack timeout, no nack delay: the workload
    // under test acks exclusively via cumulative watermarks.
    let req = SubscribeRequest {
        topic: "persistent://public/default/cumulative-prune-equiv".to_owned(),
        subscription: "sub-cumulative-prune-equiv".to_owned(),
        sub_type: pb::command_subscribe::SubType::Failover,
        ..Default::default()
    };
    let subscribe_request_id = conn.peek_next_request_id_for_test();
    let handle: ConsumerHandle = conn.subscribe(req);

    let success = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id: subscribe_request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &success).expect("encode CommandSuccess");
    conn.handle_bytes(t0, &buf).expect("Success");
    let _ = conn.poll_event();

    let gauge = |conn: &Connection| {
        conn.consumer_stats(handle)
            .expect("consumer stats")
            .pending_batch_acks
    };

    let mut trajectory = Vec::new();
    for entry in 0..ENTRIES {
        deliver_batch(conn, t0, handle, 12, entry);
        trajectory.push(gauge(conn));
        if (entry + 1) % ACK_EVERY == 0 {
            let _ = conn.ack(
                handle,
                AckRequest {
                    message_ids: vec![MessageId {
                        ledger_id: 12,
                        entry_id: entry,
                        partition: -1,
                        batch_index: -1,
                        batch_size: 0,
                        #[cfg(feature = "scalable-topics")]
                        segment_id: None,
                    }],
                    ack_type: pb::command_ack::AckType::Cumulative,
                    properties: Vec::new(),
                    txn_id: None,
                },
                t0,
            );
            let _ = conn.poll_transmit();
            trajectory.push(gauge(conn));
        }
    }
    trajectory
}

#[test]
fn cumulative_only_acking_batch_tracker_trajectories_agree_and_stay_bounded() {
    let t0 = Instant::now();

    // Tokio engine.
    let tokio_trajectory = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0)
    };

    // Moonpool engine.
    let moonpool_trajectory = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0)
    };

    // The differential equivalence claim: both engines walk the exact same
    // tracker-size trajectory for the batched + cumulative-only workload.
    assert_eq!(
        tokio_trajectory, moonpool_trajectory,
        "tokio and moonpool engines diverged on the pending_batch_acks trajectory"
    );

    // Sensitivity: between watermarks the tracker really fills (one entry per
    // batched delivery), so a regression cannot pass by the gauge reading 0.
    let max = tokio_trajectory.iter().copied().max().unwrap_or(0);
    assert_eq!(
        max as u64, ACK_EVERY,
        "the tracker must fill to the watermark window between cumulative acks"
    );

    // The #326 bound: every cumulative ack at the consume front empties the
    // tracker — it never carries entries across watermarks. Before the fix the
    // post-ack samples read 9, 19, 29, … (one leaked entry per batch, forever).
    for (i, window) in tokio_trajectory.chunks(ACK_EVERY as usize + 1).enumerate() {
        assert_eq!(
            window.last().copied(),
            Some(0),
            "watermark #{i}: a cumulative ack at the consume front must prune every \
             tracker entry it covers (issue #326), trajectory window {window:?}"
        );
    }
}
