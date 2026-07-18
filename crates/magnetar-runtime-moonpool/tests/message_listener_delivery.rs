// SPDX-License-Identifier: Apache-2.0

//! Push-delivery (`MessageListener`) drain semantics — the moonpool mirror of
//! `magnetar-runtime-tokio/tests/message_listener_delivery.rs`.
//!
//! Maintains the tokio <-> moonpool 1:1 test count required by ADR-0024
//! (`check-runtime-test-parity`): two `#[test]` functions here mirror the
//! moonpool file's two.
//!
//! ## What this pins
//!
//! The façade's `ConsumerBuilder::message_listener` / `subscribe_with_listener`
//! (ADR-0064) spawns a background poller that drains the consumer's `receive()`
//! loop and invokes the callback once per message, **sequentially, in order,
//! with no auto-ack**, stopping cleanly when the consumer closes. The poller
//! itself lives in the façade (it is engine-generic over `ConsumerApi`), but the
//! load-bearing runtime-side behaviour it relies on is the consumer
//! receive-drain seam in the sans-io [`magnetar_proto::consumer::ConsumerState`]
//! driven through this engine's [`magnetar_runtime_moonpool::ConnectionShared`]
//! wrapper. This test exercises that exact seam — push N entries, drain them
//! the way the poller does (`pop_message` -> callback, **never acking**), and
//! assert:
//!
//! 1. the callback observes every message exactly once, in delivery order (sequence ids 0..N), no
//!    skips, no duplicates;
//! 2. **no `CommandAck` is emitted on the wire** by the drain — the listener contract is "callback
//!    acks explicitly", so a poller that drained without the callback acking must leave the wire
//!    ack-free;
//! 3. once the consumer is closed (broker `CloseConsumer`), the drain seam reports closed and
//!    yields no further message — the clean-shutdown signal the poller breaks its loop on.
//!
//! No driver task, no TCP listener, no wall clock — same shape as
//! `consumer_flow_control_edge.rs`. The tokio sibling pins the identical
//! behaviour against the tokio engine.

#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use magnetar_proto::{
    ConnectionConfig, ConnectionEvent, SubscribeRequest, decode_one, encode_command,
    encode_payload, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

/// Drive handshake + subscribe + initial flow and return the consumer handle,
/// granted `receiver_queue_size` permits.
fn open_consumer(
    shared: &ConnectionShared,
    topic: &str,
    receiver_queue_size: usize,
    at: Instant,
) -> magnetar_proto::ConsumerHandle {
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        let connected = handshake_response_bytes();
        conn.handle_bytes(at, &connected).expect("Connected");
        let _ = conn.poll_event();
    }

    let req = SubscribeRequest {
        topic: topic.to_owned(),
        subscription: "magnetar-test-listener".to_owned(),
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
        conn.initial_flow(handle);
        // Drain the initial flow so later ack-absence assertions see the wire
        // in isolation.
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

/// Push N entries, drain them the way the listener poller does (`pop_message`
/// -> callback, no ack), and assert sequential in-order delivery plus a
/// completely ack-free wire (the callback would ack; the poller never does).
#[test]
fn listener_drain_delivers_sequentially_without_auto_ack() {
    const N: usize = 6;
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_consumer(&shared, "persistent://public/default/listener", N, t0);

    // Broker pushes N entries.
    for i in 0..N {
        let frame = message_frame(handle, 11, i as u64, format!("m{i}").as_bytes());
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &frame).expect("deliver message");
        while let Some(evt) = conn.poll_event() {
            let _ = matches!(evt, ConnectionEvent::Message { .. });
        }
    }
    assert_eq!(
        shared.inner.lock().consumer_queue_len(handle),
        N,
        "all pushed messages sit in the receiver queue awaiting the listener drain",
    );

    // Drain like the poller: pop -> callback (record the sequence id) -> NEVER
    // ack. Count overlap-free, in-order delivery; collect the wire after each
    // pop to prove no ack ever fires.
    let delivered: Arc<parking_lot::Mutex<Vec<u64>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));
    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_in_flight = Arc::new(AtomicUsize::new(0));
    let mut saw_ack = false;
    loop {
        let (msg, mut out) = {
            let mut conn = shared.inner.lock();
            let msg = conn.pop_message(handle);
            (msg, conn.poll_transmit())
        };
        saw_ack |= outbound_has_ack(&mut out);
        let Some(msg) = msg else { break };
        // Callback body — sequential, runs to completion before the next pop.
        let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        max_in_flight.fetch_max(now, Ordering::SeqCst);
        delivered.lock().push(msg.metadata.sequence_id);
        in_flight.fetch_sub(1, Ordering::SeqCst);
    }

    assert_eq!(
        *delivered.lock(),
        (0..N as u64).collect::<Vec<_>>(),
        "listener saw every message exactly once, in delivery order, no skips/dupes",
    );
    assert_eq!(
        max_in_flight.load(Ordering::SeqCst),
        1,
        "sequential delivery never overlaps two callback invocations",
    );
    assert!(
        !saw_ack,
        "the listener drain must NOT auto-ack — the callback acks explicitly (Java parity)",
    );
}

/// Once the consumer is closed (the local close that a façade
/// `Consumer::close()` / drop drives), the drain seam reports closed and yields
/// no further message — the clean-shutdown signal the poller breaks its loop on.
///
/// A *broker* `CommandCloseConsumer` is deliberately NOT this signal: the proto
/// layer treats it as transient (reconnect / post-seek re-attach) and only
/// surfaces `ConsumerClosedByBroker` for observability (`conn.rs` Task #65). The
/// terminal "stop the poller" path is the client-driven `close_consumer`, which
/// marks the slot closed so a parked `receive()` resolves terminally.
#[test]
fn listener_drain_stops_cleanly_on_consumer_close() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = open_consumer(&shared, "persistent://public/default/listener-close", 4, t0);

    // Push one entry and drain it (callback would ack; poller does not).
    {
        let frame = message_frame(handle, 12, 0, b"only");
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &frame).expect("deliver message");
        let _ = conn.poll_event();
    }
    assert!(
        shared.inner.lock().pop_message(handle).is_some(),
        "the single pushed message pops once",
    );
    assert!(
        !shared.inner.lock().consumer_is_closed(handle),
        "consumer is live before it is closed",
    );

    // Close the consumer the way the façade `Consumer::close()` / drop does.
    {
        let mut conn = shared.inner.lock();
        let _request_id = conn.close_consumer(handle, t0);
        let _ = conn.poll_transmit();
    }

    // The drain seam now reports closed and yields nothing — `receive()` would
    // resolve with a closed/EOF error, which is exactly what breaks the poller
    // loop for a clean, panic-free shutdown.
    let mut conn = shared.inner.lock();
    assert!(
        conn.consumer_is_closed(handle),
        "consumer reports closed after close_consumer",
    );
    assert!(
        conn.pop_message(handle).is_none(),
        "a closed consumer yields no further message to the listener drain",
    );
}
