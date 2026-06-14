// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d): tokio ↔ moonpool `EventStream` parity for **wrapper**
//! consumer push delivery (`WrapperMessageListener`, ADR-0064 wrapper extension).
//!
//! The façade's wrapper-listener poller
//! (`MultiTopicsConsumerBuilder::subscribe_with_listener`, and the partitioned /
//! pattern twins) hands the callback the messages it drains from the wrapper's
//! `receive()` — which fans across N child consumers, tagging each message with
//! its originating topic — in order. The differential claim is that **the
//! sequence of (topic, message) pairs the wrapper listener would observe is
//! identical across the two engines**.
//!
//! The poller itself is engine-generic façade code (one `tokio::spawn`ed loop
//! over the `WrapperReceiver` trait), so the only thing that can diverge between
//! engines is the per-consumer receive-drain seam in the sans-io
//! `magnetar_proto::Connection` both engines wrap behind a `parking_lot::Mutex`.
//! This test stands up two consumer handles (two topics) on one connection — the
//! shape a 2-topic `MultiTopicsConsumer` produces — feeds both engines the SAME
//! synthetic frame sequence, drains each child the way the wrapper poller does
//! (`pop_message`, no auto-ack), and collects the topic-tagged delivery slice.
//! A synthetic [`Instant`] both engines pin keeps the runs byte-for-byte
//! comparable.

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, SubscribeRequest, encode_command, encode_payload,
    pb,
};

const N: u64 = 5;
const TOPIC_A: &str = "persistent://public/default/wrap-equiv-a";
const TOPIC_B: &str = "persistent://public/default/wrap-equiv-b";

/// One message as the wrapper listener callback would observe it: the ordered,
/// topic-tagged identity the two engines must agree on.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Delivered {
    topic: String,
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

/// Subscribe one topic, ack the success, grant initial flow, return the handle.
fn open(conn: &mut Connection, topic: &str, t0: Instant) -> ConsumerHandle {
    let req = SubscribeRequest {
        topic: topic.to_owned(),
        subscription: "sub-wrapper-listener-equiv".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        receiver_queue_size: N as usize,
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

    conn.initial_flow(handle);
    let _ = conn.poll_transmit();
    handle
}

/// Drive handshake + two subscribes + push N to each + wrapper-style drain over
/// one engine's locked `Connection`, returning the ordered, topic-tagged slice
/// the wrapper listener callback would observe. Engine-agnostic.
fn lock_and_run(conn: &mut Connection, t0: Instant) -> Vec<Delivered> {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let handle_a = open(conn, TOPIC_A, t0);
    let handle_b = open(conn, TOPIC_B, t0);

    // Broker pushes N entries to each child (distinct ledgers).
    for (handle, ledger) in [(handle_a, 21u64), (handle_b, 42u64)] {
        for i in 0..N {
            let msg_cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Message as i32,
                message: Some(pb::CommandMessage {
                    consumer_id: handle.0,
                    message_id: pb::MessageIdData {
                        ledger_id: ledger,
                        entry_id: i,
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
                sequence_id: i,
                publish_time: 0,
                ..Default::default()
            };
            let mut frame = BytesMut::new();
            encode_payload(&mut frame, &msg_cmd, &metadata, format!("m{i}").as_bytes())
                .expect("encode message frame");
            conn.handle_bytes(t0, &frame).expect("deliver message");
            while conn.poll_event().is_some() {}
        }
    }

    // Drain like the wrapper poller: per child, pop -> callback (record the
    // topic-tagged identity) -> no ack.
    let mut out = Vec::new();
    for (handle, topic) in [(handle_a, TOPIC_A), (handle_b, TOPIC_B)] {
        while let Some(msg) = conn.pop_message(handle) {
            out.push(Delivered {
                topic: topic.to_owned(),
                ledger: msg.message_id.ledger_id,
                entry: msg.message_id.entry_id,
                sequence: msg.metadata.sequence_id,
            });
        }
    }
    out
}

#[test]
fn wrapper_listener_delivery_sequence_event_streams_agree() {
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

    // The differential equivalence claim: the wrapper listener observes the SAME
    // ordered, topic-tagged message sequence on both engines.
    assert_eq!(
        tokio_stream, moonpool_stream,
        "tokio and moonpool engines diverged on the wrapper listener delivery sequence"
    );
    // And it is the full, in-order push across both children on both engines.
    let mut expected: Vec<Delivered> = Vec::new();
    for (topic, ledger) in [(TOPIC_A, 21u64), (TOPIC_B, 42u64)] {
        for i in 0..N {
            expected.push(Delivered {
                topic: topic.to_owned(),
                ledger,
                entry: i,
                sequence: i,
            });
        }
    }
    assert_eq!(
        tokio_stream, expected,
        "the wrapper listener must observe every pushed message once, topic-tagged, in order, \
         got {tokio_stream:?}"
    );
}
