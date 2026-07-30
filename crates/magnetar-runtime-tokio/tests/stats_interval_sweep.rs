// SPDX-License-Identifier: Apache-2.0

//! ADR-0089 layer (b): the connection-driven rolling-rate sweep, exercised
//! through **this engine's** `ConnectionShared`.
//!
//! `ConnectionConfig::stats_interval` turns `record_rate_window` from a
//! caller-only entry point into a deadline on the sans-io state machine's
//! existing `poll_timeout` / `handle_timeout` loop, which is what finally makes
//! `ProducerStats::msgs_per_sec` / `bytes_per_sec` and their `ConsumerStats`
//! counterparts nonzero without any caller, task, or engine-specific code.
//!
//! Mirrors the `stats_histogram_accessor.rs` / `cumulative_batch_ack_prune.rs`
//! convention: drive the sans-io `Connection` through the engine's
//! `ConnectionShared` wrapper, proving the sweep runs correctly under the real
//! per-slot `parking_lot::Mutex` this engine wraps every slot in (ADR-0038
//! lock ordering — `handle_timeout` takes each slot lock while holding the
//! connection-wide one, so a sweep that reached back for the connection mutex
//! would deadlock here rather than in review).
//!
//! Every instant is synthetic, so the published rates are exact rather than
//! wall-clock-dependent. Moonpool sibling (ADR-0024 1:1 runtime-test parity):
//! `crates/magnetar-runtime-moonpool/tests/stats_interval_sweep.rs` (layer c).

mod common;

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, CreateProducerRequest, ProducerHandle,
    SubscribeRequest, encode_command, encode_payload, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

use crate::common::handshake_response_bytes;

/// One synthetic second per window, so a delta of N reads as exactly N/sec.
const INTERVAL: Duration = Duration::from_secs(1);

/// Messages pushed and published per window.
const COUNT: u64 = 5;

/// Payload of every message on both sides, so the byte rate is a clean
/// `COUNT * PAYLOAD.len()` per window.
const PAYLOAD: &[u8] = b"rate-window";

/// [`PAYLOAD`]'s length, in the `u32` shape `OutgoingMessage` wants.
const PAYLOAD_LEN: u32 = PAYLOAD.len() as u32;

/// Subscribe and drive the handshake for the subscription to completion.
fn subscribe_ready(conn: &mut Connection, at: Instant, topic: &str, sub: &str) -> ConsumerHandle {
    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.subscribe(SubscribeRequest {
        topic: topic.to_owned(),
        subscription: sub.to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        receiver_queue_size: 100,
        ..Default::default()
    });
    let success = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &success).expect("encode CommandSuccess");
    conn.handle_bytes(at, &buf).expect("Success");
    let _ = conn.poll_event();
    conn.initial_flow(handle, at);
    let _ = conn.poll_transmit();
    handle
}

/// Open a producer and feed back the synthetic `CommandProducerSuccess`.
fn producer_ready(conn: &mut Connection, at: Instant, topic: &str) -> ProducerHandle {
    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.create_producer(CreateProducerRequest {
        topic: topic.to_owned(),
        ..Default::default()
    });
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
    conn.handle_bytes(at, &buf).expect("ProducerSuccess");
    let _ = conn.poll_event();
    let _ = conn.poll_transmit();
    handle
}

/// Push [`COUNT`] broker messages at the consumer and publish [`COUNT`] from
/// the producer, so both slots' rate-window counters move by a known amount.
///
/// Both sides go through the real production paths — `handle_bytes` for
/// delivery, `Connection::send` for the publish — rather than poking the
/// counters, so the test exercises the sweep rather than the arithmetic.
fn drive_traffic(
    conn: &mut Connection,
    at: Instant,
    consumer: ConsumerHandle,
    producer: ProducerHandle,
    base_entry: u64,
) {
    for i in 0..COUNT {
        let msg_cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Message as i32,
            message: Some(pb::CommandMessage {
                consumer_id: consumer.0,
                message_id: pb::MessageIdData {
                    ledger_id: 7,
                    entry_id: base_entry + i,
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
            sequence_id: base_entry + i,
            publish_time: 0,
            ..Default::default()
        };
        let mut frame = BytesMut::new();
        encode_payload(&mut frame, &msg_cmd, &metadata, PAYLOAD).expect("encode message frame");
        conn.handle_bytes(at, &frame).expect("deliver message");
        while conn.poll_event().is_some() {}
    }
    for i in 0..COUNT {
        conn.send(
            producer,
            OutgoingMessage {
                payload: bytes::Bytes::from_static(PAYLOAD),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: PAYLOAD_LEN,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            },
            base_entry + i,
            at,
        )
        .expect("queue send");
    }
    let _ = conn.poll_transmit();
}

/// `(consumer, producer)` `(msgs_per_sec, bytes_per_sec)` pairs.
fn rates(
    conn: &Connection,
    consumer: ConsumerHandle,
    producer: ProducerHandle,
) -> ((f64, f64), (f64, f64)) {
    let c = conn.consumer_stats(consumer).expect("consumer stats");
    let p = conn.producer_stats(producer).expect("producer stats");
    (
        (c.msgs_per_sec, c.bytes_per_sec),
        (p.msgs_per_sec, p.bytes_per_sec),
    )
}

/// With `stats_interval` armed, `handle_timeout` seeds each slot's baseline on
/// its first visit and publishes the real per-second rates one interval later —
/// for the producer and the consumer alike, with no caller ever touching
/// `record_rate_window`.
///
/// The absolute rate assertions are what make this a regression test rather
/// than a smoke test: a sweep that ticked the wrong slot, ticked at the wrong
/// cadence, or divided by the wrong window would still leave the fields
/// "nonzero".
#[test]
fn stats_interval_sweep_publishes_real_rates_through_engine_shared() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig {
        stats_interval: Some(INTERVAL),
        ..ConnectionConfig::default()
    });
    let mut conn = shared.inner.lock();

    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let consumer = subscribe_ready(
        &mut conn,
        t0,
        "persistent://public/default/stats-interval-sweep",
        "sub-stats-interval-sweep",
    );
    let producer = producer_ready(
        &mut conn,
        t0,
        "persistent://public/default/stats-interval-sweep",
    );

    // Both slots were seeded at creation (mirroring Java's recorder-per-producer),
    // so a rate is not published yet — one snapshot is not a window — and a tick
    // landing inside the first window must not change that.
    conn.handle_timeout(t0);
    let ((c_msgs, _), (p_msgs, _)) = rates(&conn, consumer, producer);
    assert!(
        c_msgs.abs() < f64::EPSILON && p_msgs.abs() < f64::EPSILON,
        "no rate before a full window has elapsed, got consumer={c_msgs} producer={p_msgs}"
    );

    // The next sample is armed off that creation-time baseline at `t0` and
    // preempts the 30 s keepalive deadline.
    assert_eq!(
        conn.poll_timeout(),
        Some(t0 + INTERVAL),
        "poll_timeout must arm the next sample at last_rate_snapshot + stats_interval"
    );

    drive_traffic(&mut conn, t0, consumer, producer, 0);
    conn.handle_timeout(t0 + INTERVAL);

    #[allow(
        clippy::cast_precision_loss,
        reason = "COUNT and PAYLOAD are single-digit test constants"
    )]
    let want_msgs = COUNT as f64;
    #[allow(clippy::cast_precision_loss, reason = "same as above")]
    let want_bytes = (COUNT * u64::from(PAYLOAD_LEN)) as f64;
    let ((c_msgs, c_bytes), (p_msgs, p_bytes)) = rates(&conn, consumer, producer);
    for (label, got, want) in [
        ("consumer msgs_per_sec", c_msgs, want_msgs),
        ("consumer bytes_per_sec", c_bytes, want_bytes),
        ("producer msgs_per_sec", p_msgs, want_msgs),
        ("producer bytes_per_sec", p_bytes, want_bytes),
    ] {
        assert!(
            (got - want).abs() < 1e-9,
            "{label}: expected {want} over the {INTERVAL:?} window, got {got}"
        );
    }

    // And it keeps sampling: a second window of identical traffic republishes
    // the same rate rather than accumulating into it.
    drive_traffic(&mut conn, t0 + INTERVAL, consumer, producer, COUNT);
    conn.handle_timeout(t0 + INTERVAL + INTERVAL);
    let ((c_msgs, _), (p_msgs, _)) = rates(&conn, consumer, producer);
    assert!(
        (c_msgs - want_msgs).abs() < 1e-9 && (p_msgs - want_msgs).abs() < 1e-9,
        "the second window must republish {want_msgs}/sec (a rolling window, not a \
         running total), got consumer={c_msgs} producer={p_msgs}"
    );
}

/// The default (`stats_interval: None`) must be bit-for-bit the pre-ADR-0089
/// behaviour on this engine: no baseline is ever installed, every rate stays
/// `0.0`, and — load-bearing for the moonpool sibling's determinism — no slot
/// contributes a deadline to `poll_timeout`, so the wake schedule is unchanged.
#[test]
fn stats_interval_disabled_leaves_rates_zero_and_wake_schedule_untouched() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();

    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let consumer = subscribe_ready(
        &mut conn,
        t0,
        "persistent://public/default/stats-interval-disabled",
        "sub-stats-interval-disabled",
    );
    let producer = producer_ready(
        &mut conn,
        t0,
        "persistent://public/default/stats-interval-disabled",
    );

    // The only armed deadline is keepalive. `last_activity` is stamped by the
    // subscribe / producer-ack frames above, so the bound is `>=` rather than
    // an equality — but a per-slot stats deadline would be armed off a baseline
    // at `t0` with an interval far below the 30 s keepalive, so it could only
    // land BELOW this bound.
    let deadline = conn
        .poll_timeout()
        .expect("keepalive is armed once connected");
    assert!(
        deadline >= t0 + ConnectionConfig::default().keepalive_interval,
        "a disabled sweep must contribute no deadline — an armed-but-never-firing \
         one would still perturb the simulated wake schedule"
    );

    drive_traffic(&mut conn, t0, consumer, producer, 0);
    // Far past any plausible interval: were the sweep armed at all, this single
    // tick would both seed and sample.
    conn.handle_timeout(t0 + Duration::from_hours(1));

    let ((c_msgs, c_bytes), (p_msgs, p_bytes)) = rates(&conn, consumer, producer);
    for (label, rate) in [
        ("consumer msgs_per_sec", c_msgs),
        ("consumer bytes_per_sec", c_bytes),
        ("producer msgs_per_sec", p_msgs),
        ("producer bytes_per_sec", p_bytes),
    ] {
        assert!(
            rate.abs() < f64::EPSILON,
            "{label} must stay 0.0 while stats_interval is None, got {rate}"
        );
    }
}
