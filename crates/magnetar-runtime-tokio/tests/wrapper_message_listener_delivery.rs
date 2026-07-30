// SPDX-License-Identifier: Apache-2.0

//! Wrapper push-delivery (`WrapperMessageListener`) drain semantics — the tokio
//! mirror of `magnetar-runtime-moonpool/tests/wrapper_message_listener_delivery.rs`.
//!
//! Maintains the tokio <-> moonpool 1:1 test count required by ADR-0024
//! (`check-runtime-test-parity`): two `#[test]` functions here mirror the
//! moonpool file's two.
//!
//! ## What this pins
//!
//! The façade's `MultiTopicsConsumerBuilder::message_listener` /
//! `subscribe_with_listener` (and the `Partitioned` / `Pattern` twins) spawn a
//! background **wrapper** poller that drains the wrapper consumer's `receive()`
//! loop — which fans across N child consumers, returning a topic-tagged message —
//! and invokes the callback once per message, **sequentially, in order, with no
//! auto-ack**, stopping cleanly when the consumer set drains. The poller itself
//! lives in the façade (engine-generic over the `WrapperReceiver` trait), but the
//! load-bearing runtime-side behaviour it relies on is the per-consumer
//! receive-drain seam in the sans-io `magnetar_proto::consumer::ConsumerState`,
//! driven across **multiple** consumer handles on one connection through this
//! engine's `ConnectionShared` wrapper.
//!
//! This test stands up two consumer handles (two topics) on one connection, the
//! way a 2-topic `MultiTopicsConsumer` does, pushes entries to both, drains them
//! the way the wrapper poller does (`pop_message` per child -> callback with the
//! child's topic, **never acking**), and asserts:
//!
//! 1. the callback observes every message from both children exactly once, no skips, no duplicates,
//!    each tagged with its originating topic;
//! 2. **no `CommandAck` is emitted on the wire** by the drain — the listener contract is "callback
//!    acks explicitly", so the drain must leave the wire ack-free;
//! 3. once a child consumer is closed, its drain seam reports closed and yields no further message
//!    — the clean-shutdown signal the poller breaks its loop on.
//!
//! No driver task, no TCP listener, no wall clock. The moonpool sibling pins the
//! identical behaviour under the deterministic-simulation engine.

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use magnetar_proto::{
    ConnectionConfig, ConnectionEvent, SubscribeRequest, decode_one, encode_command,
    encode_payload, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

/// Drive handshake once on a fresh connection.
fn handshake(shared: &ConnectionShared, at: Instant) {
    let mut conn = shared.inner.lock();
    conn.begin_handshake().expect("handshake");
    let connected = handshake_response_bytes();
    conn.handle_bytes(at, &connected).expect("Connected");
    let _ = conn.poll_event();
}

/// Subscribe one topic and grant its receiver-queue permits, returning the handle.
fn open_consumer(
    shared: &ConnectionShared,
    topic: &str,
    receiver_queue_size: usize,
    at: Instant,
) -> magnetar_proto::ConsumerHandle {
    let req = SubscribeRequest {
        topic: topic.to_owned(),
        subscription: "magnetar-test-wrapper-listener".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        receiver_queue_size,
        ..Default::default()
    };
    let (handle, subscribe_request_id) = {
        let mut conn = shared.inner.lock();
        let request_id = conn.peek_next_request_id_for_test();
        let handle = conn.subscribe(req);
        (handle, request_id)
    };

    {
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
        let mut conn = shared.inner.lock();
        conn.handle_bytes(at, &buf).expect("Success");
        let _ = conn.poll_event();
    }

    {
        let mut conn = shared.inner.lock();
        conn.initial_flow(handle, at);
        // Drain the initial flow so later ack-absence assertions see the wire in isolation.
        let _ = conn.poll_transmit();
    }
    handle
}

/// One synthetic broker `CommandMessage` + payload addressed to `handle`.
fn message_frame(
    handle: magnetar_proto::ConsumerHandle,
    ledger_id: u64,
    entry_id: u64,
    payload: &[u8],
) -> BytesMut {
    let msg_cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id,
                entry_id,
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
        sequence_id: entry_id,
        publish_time: 0,
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &msg_cmd, &metadata, payload).expect("encode message frame");
    frame
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

/// `true` if any `CommandAck` frame is present on `out`.
fn outbound_has_ack(out: &mut Bytes) -> bool {
    let mut saw_ack = false;
    while !out.is_empty() {
        let frame = decode_one(out).expect("decode outbound frame");
        if frame.command.r#type == pb::base_command::Type::Ack as i32 {
            saw_ack = true;
        }
    }
    saw_ack
}

/// Two consumers (two topics) on one connection — the shape a 2-topic
/// `MultiTopicsConsumer` produces — each pushed N entries. Drain both the way the
/// wrapper poller does (`pop_message` per child -> callback with the child's
/// topic, no ack) and assert every message is delivered once, topic-tagged,
/// overlap-free, on a completely ack-free wire.
#[test]
fn wrapper_listener_drain_delivers_both_topics_without_auto_ack() {
    const N: u64 = 5;
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    handshake(&shared, t0);

    let topic_a = "persistent://public/default/wrap-a";
    let topic_b = "persistent://public/default/wrap-b";
    let handle_a = open_consumer(&shared, topic_a, N as usize, t0);
    let handle_b = open_consumer(&shared, topic_b, N as usize, t0);

    // Broker pushes N entries to each child (distinct ledgers so the union is
    // unambiguous).
    for i in 0..N {
        for (handle, ledger) in [(handle_a, 11u64), (handle_b, 22u64)] {
            let frame = message_frame(handle, ledger, i, format!("m{i}").as_bytes());
            let mut conn = shared.inner.lock();
            conn.handle_bytes(t0, &frame).expect("deliver message");
            while let Some(evt) = conn.poll_event() {
                let _ = matches!(evt, ConnectionEvent::Message { .. });
            }
        }
    }

    // Drain like the wrapper poller: for each child, pop -> callback (record the
    // (topic, sequence) pair) -> NEVER ack. Track overlap via an in-flight guard,
    // and collect the wire after each pop to prove no ack ever fires.
    let delivered: Arc<parking_lot::Mutex<Vec<(String, u64)>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));
    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_in_flight = Arc::new(AtomicUsize::new(0));
    let mut saw_ack = false;
    for (handle, topic) in [(handle_a, topic_a), (handle_b, topic_b)] {
        loop {
            let (msg, mut out) = {
                let mut conn = shared.inner.lock();
                let msg = conn.pop_message(handle, std::time::Instant::now());
                (msg, conn.poll_transmit())
            };
            saw_ack |= outbound_has_ack(&mut out);
            let Some(msg) = msg else { break };
            // Callback body — sequential, runs to completion before the next pop.
            let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            max_in_flight.fetch_max(now, Ordering::SeqCst);
            delivered
                .lock()
                .push((topic.to_owned(), msg.metadata.sequence_id));
            in_flight.fetch_sub(1, Ordering::SeqCst);
        }
    }

    let got = delivered.lock().clone();
    assert_eq!(
        got.len() as u64,
        2 * N,
        "the wrapper listener saw every message from both children, no skips/dupes",
    );
    // Per-topic, each child delivered sequence ids 0..N in order.
    let seq_a: Vec<u64> = got
        .iter()
        .filter(|(t, _)| t == topic_a)
        .map(|(_, s)| *s)
        .collect();
    let seq_b: Vec<u64> = got
        .iter()
        .filter(|(t, _)| t == topic_b)
        .map(|(_, s)| *s)
        .collect();
    assert_eq!(
        seq_a,
        (0..N).collect::<Vec<_>>(),
        "topic A delivered in order, once each",
    );
    assert_eq!(
        seq_b,
        (0..N).collect::<Vec<_>>(),
        "topic B delivered in order, once each",
    );
    // The union of topics seen is exactly the two children — every message is
    // topic-tagged so the callback can route an explicit ack.
    let topics: BTreeSet<&str> = got.iter().map(|(t, _)| t.as_str()).collect();
    assert_eq!(
        topics,
        BTreeSet::from([topic_a, topic_b]),
        "every delivered message carries its originating topic",
    );
    assert_eq!(
        max_in_flight.load(Ordering::SeqCst),
        1,
        "sequential delivery never overlaps two callback invocations",
    );
    assert!(
        !saw_ack,
        "the wrapper drain must NOT auto-ack — the callback acks explicitly (Java parity)",
    );
}

/// When one child consumer of the wrapper is closed, its drain seam reports
/// closed and yields no further message — the per-child clean-shutdown signal the
/// wrapper poller observes (the wrapper `receive()` propagates the child error,
/// which breaks the poller loop). The other child stays live and drainable.
#[test]
fn wrapper_listener_drain_stops_cleanly_on_child_close() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    handshake(&shared, t0);

    let topic_a = "persistent://public/default/wrap-close-a";
    let topic_b = "persistent://public/default/wrap-close-b";
    let handle_a = open_consumer(&shared, topic_a, 4, t0);
    let handle_b = open_consumer(&shared, topic_b, 4, t0);

    // Push one entry to each child and drain it (callback would ack; poller does not).
    for (handle, ledger) in [(handle_a, 12u64), (handle_b, 24u64)] {
        let frame = message_frame(handle, ledger, 0, b"only");
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &frame).expect("deliver message");
        let _ = conn.poll_event();
    }
    assert!(
        shared
            .inner
            .lock()
            .pop_message(handle_a, std::time::Instant::now())
            .is_some(),
        "child A's single message pops once",
    );
    assert!(
        shared
            .inner
            .lock()
            .pop_message(handle_b, std::time::Instant::now())
            .is_some(),
        "child B's single message pops once",
    );
    assert!(
        !shared.inner.lock().consumer_is_closed(handle_a),
        "child A is live before it is closed",
    );

    // Close child A the way a façade per-topic `Consumer::close()` does.
    {
        let mut conn = shared.inner.lock();
        let _request_id = conn.close_consumer(handle_a, t0);
        let _ = conn.poll_transmit();
    }

    let mut conn = shared.inner.lock();
    assert!(
        conn.consumer_is_closed(handle_a),
        "child A reports closed after close_consumer",
    );
    assert!(
        conn.pop_message(handle_a, std::time::Instant::now())
            .is_none(),
        "a closed child yields no further message to the wrapper drain",
    );
    // Child B is unaffected — the wrapper keeps serving its remaining children.
    assert!(
        !conn.consumer_is_closed(handle_b),
        "closing one child does not close its sibling",
    );
}
