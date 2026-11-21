// SPDX-License-Identifier: Apache-2.0

//! Moonpool twin of the tokio `unacked_tracker_release` test (ADR-0024 1:1
//! runtime-test-parity). An acknowledged message must leave the `ack_timeout`
//! (unacked-message) tracker, so the sweep never asks the broker to redeliver it.
//!
//! Three ways an acknowledged id used to stay tracked:
//!
//! 1. A cumulative ack released only the id it named. Every earlier delivery it covers stayed in
//!    the tracker and was redelivered when the `ack_timeout` window closed.
//! 2. Reaping an empty tracker bucket left the surviving buckets' index stale, so the ack of an id
//!    held in a later bucket found no bucket and released nothing.
//! 3. The same cumulative ack left every earlier nacked delivery in the negative-ack tracker, so
//!    the nack delay asked the broker to redeliver messages the application had acknowledged.
//!
//! ## Shape
//!
//! Subscribe with `ack_timeout = 10s`, feed synthetic broker `CommandMessage` frames at synthetic
//! instants, acknowledge, tick `handle_timeout` past the window, and read the
//! [`pb::CommandRedeliverUnacknowledgedMessages`] frames the proto layer queued.

mod common;

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::trackers::nack::MultiplierRedeliveryBackoff;
use magnetar_proto::{
    AckRequest, ConnectionConfig, ConsumerHandle, MessageId, SubscribeRequest, encode_command,
    encode_payload, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

const ACK_TIMEOUT: Duration = Duration::from_secs(10);
const NACK_DELAY: Duration = Duration::from_secs(5);
/// Per-delivery ack-timeout backoff (PIP-37); a first delivery waits `min_delay`.
const ACK_BACKOFF: MultiplierRedeliveryBackoff = MultiplierRedeliveryBackoff {
    min_delay: Duration::from_secs(2),
    max_delay: Duration::from_mins(1),
    multiplier: 2.0,
};

/// Handshake and subscribe a consumer armed with [`ACK_TIMEOUT`].
fn subscribed_consumer(shared: &ConnectionShared, t0: Instant) -> ConsumerHandle {
    subscribed_consumer_with_nack_delay(shared, t0, None)
}

/// Handshake and subscribe a consumer armed with [`ACK_TIMEOUT`] and, when `nack_delay` is set,
/// a negative-ack redelivery tracker.
fn subscribed_consumer_with_nack_delay(
    shared: &ConnectionShared,
    t0: Instant,
    nack_delay: Option<Duration>,
) -> ConsumerHandle {
    subscribed_consumer_with_options(shared, t0, nack_delay, None)
}

/// Handshake and subscribe a consumer armed with [`ACK_TIMEOUT`] and [`ACK_BACKOFF`].
fn subscribed_consumer_with_backoff(shared: &ConnectionShared, t0: Instant) -> ConsumerHandle {
    subscribed_consumer_with_options(shared, t0, None, Some(ACK_BACKOFF))
}

/// Handshake and subscribe a consumer armed with [`ACK_TIMEOUT`], an optional negative-ack
/// redelivery delay and an optional ack-timeout backoff.
fn subscribed_consumer_with_options(
    shared: &ConnectionShared,
    t0: Instant,
    nack_delay: Option<Duration>,
    ack_timeout_backoff: Option<MultiplierRedeliveryBackoff>,
) -> ConsumerHandle {
    let mut conn = shared.inner.lock();
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();
    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/unacked-release".to_owned(),
        subscription: "magnetar-test-unacked-release".to_owned(),
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

/// Feed one synthetic non-batched delivery `(7, entry)` at `now`, then drain its events and
/// the outbound flow bytes.
fn deliver(shared: &ConnectionShared, handle: ConsumerHandle, now: Instant, entry: u64) {
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
    let mut conn = shared.inner.lock();
    conn.handle_bytes(now, &frame).expect("deliver message");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
}

/// The id a user holds for the non-batched delivery `(7, entry)`: the delivery path normalises
/// it to `batch_index = -1, batch_size = 0`, which is also the key the unacked tracker holds.
fn delivered_id(entry: u64) -> MessageId {
    MessageId {
        ledger_id: 7,
        entry_id: entry,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
    }
}

/// Acknowledge `entry` with `ack_type` at `now`, then drop the ack frame from the outbound buffer.
fn ack(
    shared: &ConnectionShared,
    handle: ConsumerHandle,
    now: Instant,
    entry: u64,
    ack_type: pb::command_ack::AckType,
) {
    let mut conn = shared.inner.lock();
    let _ = conn.ack(
        handle,
        AckRequest {
            message_ids: vec![delivered_id(entry)],
            ack_type,
            properties: Vec::new(),
            txn_id: None,
        },
        now,
    );
    let _ = conn.poll_transmit();
}

/// Tick the connection at `now` and return the `(ledger, entry)` pair of every id the queued
/// `CommandRedeliverUnacknowledgedMessages` frames carry.
fn redelivered_after_tick(shared: &ConnectionShared, now: Instant) -> Vec<(u64, u64)> {
    let mut conn = shared.inner.lock();
    conn.handle_timeout(now);
    let mut src = conn.poll_transmit();
    let mut ids = Vec::new();
    while !src.is_empty() {
        let decoded = magnetar_proto::decode_one(&mut src).expect("decode outbound frame");
        if let Some(body) = decoded.command.redeliver_unacknowledged_messages {
            ids.extend(body.message_ids.iter().map(|m| (m.ledger_id, m.entry_id)));
        }
    }
    ids
}

#[test]
fn cumulative_ack_releases_every_covered_id_from_the_unacked_tracker() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = subscribed_consumer(&shared, t0);
    for entry in 1..=4 {
        deliver(&shared, handle, t0, entry);
    }

    // Cumulative ack at (7, 3): (7, 1), (7, 2) and (7, 3) are acknowledged, (7, 4) is not.
    ack(&shared, handle, t0, 3, pb::command_ack::AckType::Cumulative);

    let redelivered = redelivered_after_tick(&shared, t0 + Duration::from_secs(11));
    assert_eq!(
        redelivered,
        vec![(7, 4)],
        "the ack-timeout sweep must redeliver only the delivery above the cumulative \
         horizon, not the ids the cumulative ack already covered"
    );
}

#[test]
fn ack_after_an_empty_bucket_was_reaped_releases_the_id() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = subscribed_consumer(&shared, t0);

    // (7, 1) is delivered and acknowledged inside its window: the head bucket is left empty.
    deliver(&shared, handle, t0, 1);
    ack(&shared, handle, t0, 1, pb::command_ack::AckType::Individual);

    // (7, 2) arrives after the head bucket's deadline and before the timer-driven sweep, which
    // then reaps the empty head bucket and emits nothing.
    let t1 = t0 + Duration::from_secs(11);
    deliver(&shared, handle, t1, 2);
    assert_eq!(
        redelivered_after_tick(&shared, t1),
        Vec::<(u64, u64)>::new(),
        "the sweep that reaps the empty head bucket redelivers nothing"
    );

    // (7, 2) is acknowledged; its own window then closes.
    ack(&shared, handle, t1, 2, pb::command_ack::AckType::Individual);
    assert_eq!(
        redelivered_after_tick(&shared, t1 + Duration::from_secs(11)),
        Vec::<(u64, u64)>::new(),
        "an acknowledged id must not be redelivered after the bucket ahead of it was reaped"
    );
}

#[test]
fn cumulative_ack_releases_every_covered_id_from_the_nack_tracker() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = subscribed_consumer_with_nack_delay(&shared, t0, Some(NACK_DELAY));
    for entry in 1..=4 {
        deliver(&shared, handle, t0, entry);
    }

    // (7, 1), (7, 2) and (7, 4) are nacked, then a cumulative ack at (7, 3) acknowledges
    // (7, 1), (7, 2) and (7, 3): only the nack above the horizon is still owed a redelivery.
    {
        let mut conn = shared.inner.lock();
        conn.negative_ack(
            handle,
            vec![delivered_id(1), delivered_id(2), delivered_id(4)],
            t0,
        );
    }
    ack(&shared, handle, t0, 3, pb::command_ack::AckType::Cumulative);

    let redelivered = redelivered_after_tick(&shared, t0 + NACK_DELAY + Duration::from_secs(1));
    assert_eq!(
        redelivered,
        vec![(7, 4)],
        "the nack sweep must redeliver only the nack above the cumulative horizon, \
         not the nacks the cumulative ack already covered"
    );
}

#[test]
fn cumulative_ack_releases_every_covered_id_from_the_backoff_tracker() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = subscribed_consumer_with_backoff(&shared, t0);
    for entry in 1..=4 {
        deliver(&shared, handle, t0, entry);
    }

    // With an ack-timeout backoff each delivery waits in its own deadline slot, not in a bucket.
    ack(&shared, handle, t0, 3, pb::command_ack::AckType::Cumulative);

    let redelivered =
        redelivered_after_tick(&shared, t0 + ACK_BACKOFF.min_delay + Duration::from_secs(1));
    assert_eq!(
        redelivered,
        vec![(7, 4)],
        "the backoff sweep must redeliver only the delivery above the cumulative horizon, \
         not the ids the cumulative ack already covered"
    );
}

#[cfg(feature = "scalable-topics")]
#[test]
fn cumulative_ack_releases_every_covered_canonical_nack_id() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let handle = subscribed_consumer_with_nack_delay(&shared, t0, Some(NACK_DELAY));
    for entry in 1..=4 {
        deliver(&shared, handle, t0, entry);
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
    {
        let mut conn = shared.inner.lock();
        conn.negative_ack_with_message_id_data(
            handle,
            vec![canonical(1), canonical(2), canonical(4)],
            t0,
        );
    }
    ack(&shared, handle, t0, 3, pb::command_ack::AckType::Cumulative);

    let redelivered = redelivered_after_tick(&shared, t0 + NACK_DELAY + Duration::from_secs(1));
    assert_eq!(
        redelivered,
        vec![(7, 4)],
        "the nack sweep must redeliver only the nack above the cumulative horizon, whose complete \
         id is the only one still parked"
    );
}
