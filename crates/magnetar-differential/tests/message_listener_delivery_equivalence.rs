// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d): tokio ↔ moonpool `EventStream` parity for consumer
//! push delivery (`MessageListener`, ADR-0064).
//!
//! The façade's listener poller (`ConsumerBuilder::subscribe_with_listener`)
//! hands the callback the messages it drains from `receive()`, in order. The
//! differential claim is that **the sequence of messages the listener would
//! observe is identical across the two engines** — same ids, same order, no
//! skips or dupes on one engine but not the other.
//!
//! The poller itself is engine-generic façade code (one `tokio::spawn`ed loop
//! over `ConsumerApi`), so the only thing that can diverge between engines is
//! the underlying consumer receive-drain seam in the sans-io
//! `magnetar_proto::Connection` both engines wrap behind a
//! `parking_lot::Mutex`. This test feeds both engines the SAME synthetic frame
//! sequence (handshake, subscribe, push N entries) and drains each the way the
//! poller does (`pop_message`, no auto-ack), collecting the delivered
//! `(ledger, entry, sequence_id)` slice — exactly the `EventStream` slice the
//! listener callback would see. A synthetic [`Instant`] both engines pin keeps
//! the runs byte-for-byte comparable.

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, SubscribeRequest, encode_command, encode_payload,
    pb,
};

const N: usize = 6;

/// One message as the listener callback would observe it: the ordered identity
/// the two engines must agree on.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Delivered {
    ledger: u64,
    entry: u64,
    sequence: u64,
}

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

/// Drive handshake + subscribe + push N + listener-style drain over one engine's
/// locked `Connection`, returning the ordered slice the listener callback would
/// observe. Engine-agnostic: the caller hands in a `&mut Connection` from either
/// engine's `ConnectionShared`, so the two runs are byte-for-byte comparable.
fn lock_and_run(conn: &mut Connection, t0: Instant) -> Vec<Delivered> {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let req = SubscribeRequest {
        topic: "persistent://public/default/listener-equiv".to_owned(),
        subscription: "sub-listener-equiv".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        receiver_queue_size: N,
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

    conn.initial_flow(handle, t0);
    let _ = conn.poll_transmit();

    // Broker pushes N entries.
    for i in 0..N {
        let msg_cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Message as i32,
            message: Some(pb::CommandMessage {
                consumer_id: handle.0,
                message_id: pb::MessageIdData {
                    ledger_id: 21,
                    entry_id: i as u64,
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
            sequence_id: i as u64,
            publish_time: 0,
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_payload(&mut frame, &msg_cmd, &metadata, format!("m{i}").as_bytes())
            .expect("encode message frame");
        conn.handle_bytes(t0, &frame).expect("deliver message");
        while conn.poll_event().is_some() {}
    }

    // Drain like the poller: pop -> callback (record the identity) -> no ack.
    let mut out = Vec::new();
    while let Some(msg) = conn.pop_message(handle) {
        out.push(Delivered {
            ledger: msg.message_id.ledger_id,
            entry: msg.message_id.entry_id,
            sequence: msg.metadata.sequence_id,
        });
    }
    out
}

#[test]
fn listener_delivery_sequence_event_streams_agree() {
    let t0 = Instant::now();

    let tokio_stream = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0)
    };

    let moonpool_stream = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0)
    };

    // The differential equivalence claim: the listener observes the SAME ordered
    // message sequence on both engines.
    assert_eq!(
        tokio_stream, moonpool_stream,
        "tokio and moonpool engines diverged on the listener delivery sequence"
    );
    // And it is the full, in-order push (sequence ids 0..N) on both.
    let expected: Vec<Delivered> = (0..N as u64)
        .map(|i| Delivered {
            ledger: 21,
            entry: i,
            sequence: i,
        })
        .collect();
    assert_eq!(
        tokio_stream, expected,
        "the listener must observe every pushed message once, in order, got {tokio_stream:?}"
    );
}
