// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d): tokio ↔ moonpool `EventStream` parity for the acknowledged-message release
//! from the `ack_timeout` (unacked-message) tracker. An acknowledged message must never be
//! redelivered by the ack-timeout sweep, and the two engines must agree on that outcome.
//!
//! Three scenarios, one per way an acknowledged id used to stay tracked:
//!
//! - a cumulative ack released only the id it named, so every earlier delivery it covers was
//!   redelivered when the `ack_timeout` window closed;
//! - reaping an empty tracker bucket left the surviving buckets' index stale, so the ack of an id
//!   held in a later bucket released nothing;
//! - the same cumulative ack left every earlier nacked delivery in the negative-ack tracker, so the
//!   nack delay redelivered messages the application had acknowledged.
//!
//! Why this drives the engines' `ConnectionShared` directly instead of the `ScriptedBroker`
//! runner: the scenarios hinge on synthetic delivery and sweep instants (a delivery after a
//! bucket deadline and before the sweep) that wall-clock scripting cannot pin without flaking.
//! Both engines wrap the SAME `magnetar_proto::Connection` behind a `parking_lot::Mutex`, so
//! feeding both an identical synthetic frame sequence and comparing the emitted
//! `RedeliverUnacknowledgedMessages` frames is exactly the `EventStream`-parity claim the harness
//! exists to make, at the proto seam the two engines share — exactly like
//! `nack_unacked_removal_equivalence.rs`.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::trackers::nack::MultiplierRedeliveryBackoff;
use magnetar_proto::{
    AckRequest, Connection, ConnectionConfig, ConsumerHandle, MessageId, SubscribeRequest,
    decode_one, encode_command, encode_payload, pb,
};

const ACK_TIMEOUT: Duration = Duration::from_secs(10);
const NACK_DELAY: Duration = Duration::from_secs(5);
/// Per-delivery ack-timeout backoff (PIP-37); a first delivery waits `min_delay`.
const ACK_BACKOFF: MultiplierRedeliveryBackoff = MultiplierRedeliveryBackoff {
    min_delay: Duration::from_secs(2),
    max_delay: Duration::from_mins(1),
    multiplier: 2.0,
};

/// One observed redelivery: the consumer it targeted plus the `(ledger, entry)` pairs it carried.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Redelivery {
    consumer_id: u64,
    ids: Vec<(u64, u64)>,
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

/// Handshake and subscribe a consumer armed with [`ACK_TIMEOUT`].
fn subscribed_consumer(conn: &mut Connection, t0: Instant) -> ConsumerHandle {
    subscribed_consumer_with_nack_delay(conn, t0, None)
}

/// Handshake and subscribe a consumer armed with [`ACK_TIMEOUT`] and, when `nack_delay` is set,
/// a negative-ack redelivery tracker.
fn subscribed_consumer_with_nack_delay(
    conn: &mut Connection,
    t0: Instant,
    nack_delay: Option<Duration>,
) -> ConsumerHandle {
    subscribed_consumer_with_options(conn, t0, nack_delay, None)
}

/// Handshake and subscribe a consumer armed with [`ACK_TIMEOUT`] and [`ACK_BACKOFF`].
fn subscribed_consumer_with_backoff(conn: &mut Connection, t0: Instant) -> ConsumerHandle {
    subscribed_consumer_with_options(conn, t0, None, Some(ACK_BACKOFF))
}

/// Handshake and subscribe a consumer armed with [`ACK_TIMEOUT`], an optional negative-ack
/// redelivery delay and an optional ack-timeout backoff.
fn subscribed_consumer_with_options(
    conn: &mut Connection,
    t0: Instant,
    nack_delay: Option<Duration>,
    ack_timeout_backoff: Option<MultiplierRedeliveryBackoff>,
) -> ConsumerHandle {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();
    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/unacked-release-equiv".to_owned(),
        subscription: "sub-unacked-release-equiv".to_owned(),
        sub_type: pb::command_subscribe::SubType::Exclusive,
        ack_timeout: Some(ACK_TIMEOUT),
        negative_ack_redelivery_delay: nack_delay,
        ack_timeout_backoff,
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
    conn.handle_bytes(t0, &buf).expect("Success");
    let _ = conn.poll_event();
    handle
}

/// Feed one synthetic non-batched delivery `(7, entry)` at `now`.
fn deliver(conn: &mut Connection, handle: ConsumerHandle, now: Instant, entry: u64) {
    let command = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: 7,
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
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &command, &metadata, b"unacked-release-payload")
        .expect("encode message frame");
    conn.handle_bytes(now, &frame).expect("deliver message");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
}

/// Acknowledge the non-batched delivery `(7, entry)` (normalised to `batch_index = -1,
/// batch_size = 0`, the key the unacked tracker holds) at `now`.
fn ack(
    conn: &mut Connection,
    handle: ConsumerHandle,
    now: Instant,
    entry: u64,
    ack_type: pb::command_ack::AckType,
) {
    let _ = conn.ack(
        handle,
        AckRequest {
            message_ids: vec![MessageId {
                ledger_id: 7,
                entry_id: entry,
                partition: -1,
                batch_index: -1,
                batch_size: 0,
            }],
            ack_type,
            properties: Vec::new(),
            txn_id: None,
        },
        now,
    );
    let _ = conn.poll_transmit();
}

/// Tick the connection at `now` and collect the redelivery frames it queues.
fn tick(conn: &mut Connection, now: Instant) -> Vec<Redelivery> {
    conn.handle_timeout(now);
    let mut src = conn.poll_transmit();
    let mut out = Vec::new();
    while !src.is_empty() {
        let decoded = decode_one(&mut src).expect("decode outbound frame");
        if let Some(body) = decoded.command.redeliver_unacknowledged_messages {
            out.push(Redelivery {
                consumer_id: body.consumer_id,
                ids: body
                    .message_ids
                    .iter()
                    .map(|m| (m.ledger_id, m.entry_id))
                    .collect(),
            });
        }
    }
    out
}

/// Four deliveries, a cumulative ack at the third, then the sweep past the window.
fn cumulative_ack_run(conn: &mut Connection, t0: Instant) -> Vec<Redelivery> {
    let handle = subscribed_consumer(conn, t0);
    for entry in 1..=4 {
        deliver(conn, handle, t0, entry);
    }
    ack(conn, handle, t0, 3, pb::command_ack::AckType::Cumulative);
    tick(conn, t0 + Duration::from_secs(11))
}

/// Four deliveries, three of them nacked, a cumulative ack at the third, then the sweep past the
/// nack delay.
fn cumulative_ack_nack_run(conn: &mut Connection, t0: Instant) -> Vec<Redelivery> {
    let handle = subscribed_consumer_with_nack_delay(conn, t0, Some(NACK_DELAY));
    for entry in 1..=4 {
        deliver(conn, handle, t0, entry);
    }
    let delivered = |entry| MessageId {
        ledger_id: 7,
        entry_id: entry,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
    };
    conn.negative_ack(handle, vec![delivered(1), delivered(2), delivered(4)], t0);
    ack(conn, handle, t0, 3, pb::command_ack::AckType::Cumulative);
    tick(conn, t0 + NACK_DELAY + Duration::from_secs(1))
}

/// A delivery acknowledged in its window leaves an empty head bucket; a second delivery after
/// that bucket's deadline and before the sweep lands in the next one. The sweep reaps the empty
/// head, the second delivery is acknowledged, and its own window closes. Returns every
/// redelivery from both sweeps.
fn reaped_empty_bucket_run(conn: &mut Connection, t0: Instant) -> Vec<Redelivery> {
    let handle = subscribed_consumer(conn, t0);
    deliver(conn, handle, t0, 1);
    ack(conn, handle, t0, 1, pb::command_ack::AckType::Individual);
    let t1 = t0 + Duration::from_secs(11);
    deliver(conn, handle, t1, 2);
    let mut out = tick(conn, t1);
    ack(conn, handle, t1, 2, pb::command_ack::AckType::Individual);
    out.extend(tick(conn, t1 + Duration::from_secs(11)));
    out
}

/// Run `scenario` once on each engine's locked `Connection` and return both streams.
fn on_both_engines(
    scenario: fn(&mut Connection, Instant) -> Vec<Redelivery>,
) -> (Vec<Redelivery>, Vec<Redelivery>) {
    let t0 = Instant::now();
    let tokio_stream = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        scenario(&mut conn, t0)
    };
    let moonpool_stream = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        scenario(&mut conn, t0)
    };
    (tokio_stream, moonpool_stream)
}

#[test]
fn cumulative_ack_unacked_release_event_streams_agree() {
    let (tokio_stream, moonpool_stream) = on_both_engines(cumulative_ack_run);
    assert_eq!(
        tokio_stream, moonpool_stream,
        "tokio and moonpool engines diverged on the cumulative-ack ack-timeout redelivery stream"
    );
    // Only the delivery above the cumulative horizon is still owed an ack.
    assert_eq!(
        tokio_stream.iter().flat_map(|r| &r.ids).collect::<Vec<_>>(),
        vec![&(7, 4)],
        "the sweep must redeliver only (7, 4), got {tokio_stream:?}"
    );
}

#[test]
fn reaped_empty_bucket_unacked_release_event_streams_agree() {
    let (tokio_stream, moonpool_stream) = on_both_engines(reaped_empty_bucket_run);
    assert_eq!(
        tokio_stream, moonpool_stream,
        "tokio and moonpool engines diverged on the reaped-empty-bucket redelivery stream"
    );
    assert_eq!(
        tokio_stream,
        Vec::<Redelivery>::new(),
        "every delivery was acknowledged, so no sweep may redeliver anything"
    );
}

#[test]
fn cumulative_ack_nack_release_event_streams_agree() {
    let (tokio_stream, moonpool_stream) = on_both_engines(cumulative_ack_nack_run);
    assert_eq!(
        tokio_stream, moonpool_stream,
        "tokio and moonpool engines diverged on the cumulative-ack nack redelivery stream"
    );
    // Only the nack above the cumulative horizon is still owed a redelivery.
    assert_eq!(
        tokio_stream.iter().flat_map(|r| &r.ids).collect::<Vec<_>>(),
        vec![&(7, 4)],
        "the nack sweep must redeliver only (7, 4), got {tokio_stream:?}"
    );
}

/// Four deliveries under an ack-timeout backoff, a cumulative ack at the third, then the sweep
/// past the first delivery's backoff delay.
fn cumulative_ack_backoff_run(conn: &mut Connection, t0: Instant) -> Vec<Redelivery> {
    let handle = subscribed_consumer_with_backoff(conn, t0);
    for entry in 1..=4 {
        deliver(conn, handle, t0, entry);
    }
    ack(conn, handle, t0, 3, pb::command_ack::AckType::Cumulative);
    tick(conn, t0 + ACK_BACKOFF.min_delay + Duration::from_secs(1))
}

#[test]
fn cumulative_ack_backoff_release_event_streams_agree() {
    let (tokio_stream, moonpool_stream) = on_both_engines(cumulative_ack_backoff_run);
    assert_eq!(
        tokio_stream, moonpool_stream,
        "tokio and moonpool engines diverged on the cumulative-ack backoff redelivery stream"
    );
    assert_eq!(
        tokio_stream.iter().flat_map(|r| &r.ids).collect::<Vec<_>>(),
        vec![&(7, 4)],
        "the backoff sweep must redeliver only (7, 4), got {tokio_stream:?}"
    );
}

/// Four deliveries, three of them nacked with their complete ids, a cumulative ack at the third,
/// then the sweep past the nack delay.
#[cfg(feature = "scalable-topics")]
fn cumulative_ack_canonical_nack_run(conn: &mut Connection, t0: Instant) -> Vec<Redelivery> {
    let handle = subscribed_consumer_with_nack_delay(conn, t0, Some(NACK_DELAY));
    for entry in 1..=4 {
        deliver(conn, handle, t0, entry);
    }
    let canonical = |entry_id| pb::MessageIdData {
        ledger_id: 7,
        entry_id,
        partition: Some(-1),
        batch_index: Some(-1),
        ack_set: Vec::new(),
        batch_size: Some(0),
        first_chunk_message_id: None,
    };
    conn.negative_ack_with_message_id_data(
        handle,
        vec![canonical(1), canonical(2), canonical(4)],
        t0,
    );
    ack(conn, handle, t0, 3, pb::command_ack::AckType::Cumulative);
    tick(conn, t0 + NACK_DELAY + Duration::from_secs(1))
}

#[cfg(feature = "scalable-topics")]
#[test]
fn cumulative_ack_canonical_nack_release_event_streams_agree() {
    let (tokio_stream, moonpool_stream) = on_both_engines(cumulative_ack_canonical_nack_run);
    assert_eq!(
        tokio_stream, moonpool_stream,
        "tokio and moonpool engines diverged on the cumulative-ack canonical nack redelivery stream"
    );
    assert_eq!(
        tokio_stream.iter().flat_map(|r| &r.ids).collect::<Vec<_>>(),
        vec![&(7, 4)],
        "the nack sweep must redeliver only (7, 4), got {tokio_stream:?}"
    );
}
