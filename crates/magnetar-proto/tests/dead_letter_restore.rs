// SPDX-License-Identifier: Apache-2.0

//! Putting back the dead letters a failed or cancelled republish did not get to
//! (`Connection::restore_dead_letter`).
//!
//! `drain_dead_letter` takes the consumer's whole dead-letter buffer. A message taken out of it
//! is in no receiver queue and no unacked tracker, so when the republish that took it dies
//! halfway (a failed DLQ publish, a dropped future) nothing hands the rest back to the
//! application and the broker keeps them unacked against this consumer. The runtimes keep
//! the not-yet-republished messages in a guard and give them to `restore_dead_letter` when it
//! drops.
//!
//! The method has three obligations, one test each:
//!
//! - the messages go to the FRONT of the buffer, in their original order, ahead of whatever was
//!   classified as dead-letter meanwhile;
//! - they go straight into the buffer: they are not classified again, so their flow permit is not
//!   refunded a second time and `total_msgs_dead_lettered` does not count them twice;
//! - they are NOT put back after a session reset. `reset()` empties the buffer because the broker
//!   redelivers every unacked message to the new session; a restored copy would sit beside the
//!   redelivered one and the DLQ would receive both.
//!
//! Layer (a) of the ADR-0024 four-layer test policy for the dead-letter republish defect.

use std::time::Instant;

use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, IncomingMessage, SubscribeRequest,
    encode_command, encode_payload, pb,
};

const MAX_REDELIVER: u32 = 1;
/// Over the `MAX_REDELIVER` threshold, so the client routes the message to its dead-letter buffer.
const OVER_REDELIVERED: u32 = MAX_REDELIVER + 1;

/// A connection that has completed the handshake, with a `Shared` consumer that has a
/// dead-letter threshold (the subscribe is never acknowledged: dispatches are accepted anyway).
fn dlq_consumer(now: Instant) -> (Connection, ConsumerHandle) {
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(std::time::SystemTime::now),
    );
    conn.begin_handshake().expect("begin_handshake");
    let connected = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = bytes::BytesMut::new();
    encode_command(&mut buf, &connected).expect("encode connected");
    conn.handle_bytes(now, &buf).expect("apply connected");
    let _ = conn.poll_transmit();
    let handle = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/dlq-restore".to_owned(),
        subscription: "s".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        max_redeliver_count: MAX_REDELIVER,
        ..Default::default()
    });
    (conn, handle)
}

/// Deliver one over-redelivered `CommandMessage` (entry `entry_id`, payload `poison-<entry_id>`)
/// to `handle`, which the client routes to its dead-letter buffer.
fn dead_letter(conn: &mut Connection, handle: ConsumerHandle, entry_id: u64, now: Instant) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: 1,
                entry_id,
                ..Default::default()
            },
            redelivery_count: Some(OVER_REDELIVERED),
            ack_set: Vec::new(),
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let meta = pb::MessageMetadata {
        producer_name: "dlq-restore".to_owned(),
        sequence_id: entry_id,
        publish_time: 1_700_000_000,
        ..Default::default()
    };
    let mut buf = bytes::BytesMut::new();
    encode_payload(
        &mut buf,
        &cmd,
        &meta,
        format!("poison-{entry_id}").as_bytes(),
    )
    .expect("encode CommandMessage");
    conn.handle_bytes(now, &buf).expect("deliver dead letter");
}

fn entries(messages: &[IncomingMessage]) -> Vec<u64> {
    messages
        .iter()
        .map(|message| message.message_id.entry_id)
        .collect()
}

#[test]
fn restored_messages_go_back_ahead_of_the_ones_that_arrived_meanwhile() {
    let now = Instant::now();
    let (mut conn, handle) = dlq_consumer(now);
    for entry in 0..3 {
        dead_letter(&mut conn, handle, entry, now);
    }
    let epoch = conn.session_epoch();
    let mut taken = conn.drain_dead_letter(handle);
    assert_eq!(entries(&taken), [0, 1, 2]);
    // The first message was republished; the call died on the second one.
    taken.remove(0);
    // A dead letter that was classified while the call was awaiting its publish.
    dead_letter(&mut conn, handle, 3, now);

    conn.restore_dead_letter(handle, epoch, taken);

    assert_eq!(
        entries(&conn.drain_dead_letter(handle)),
        [1, 2, 3],
        "the unprocessed messages come back first, in order, ahead of the later arrival"
    );
}

#[test]
fn restoring_does_not_classify_the_messages_a_second_time() {
    let now = Instant::now();
    let (mut conn, handle) = dlq_consumer(now);
    for entry in 0..2 {
        dead_letter(&mut conn, handle, entry, now);
    }
    let epoch = conn.session_epoch();
    let permits_before = conn.consumer_available_permits(handle);
    let dead_lettered_before = conn
        .consumer_stats(handle)
        .expect("consumer stats")
        .total_msgs_dead_lettered;
    assert_eq!(dead_lettered_before, 2);
    let taken = conn.drain_dead_letter(handle);

    conn.restore_dead_letter(handle, epoch, taken);

    assert_eq!(
        conn.consumer_available_permits(handle),
        permits_before,
        "a restore must not refund the flow permit of a dispatch unit already refunded"
    );
    assert_eq!(
        conn.consumer_stats(handle)
            .expect("consumer stats")
            .total_msgs_dead_lettered,
        dead_lettered_before,
        "a restore must not count a dead letter twice"
    );
    assert_eq!(conn.drain_dead_letter(handle).len(), 2);
}

#[test]
fn nothing_is_restored_after_a_session_reset() {
    let now = Instant::now();
    let (mut conn, handle) = dlq_consumer(now);
    for entry in 0..2 {
        dead_letter(&mut conn, handle, entry, now);
    }
    let epoch = conn.session_epoch();
    let taken = conn.drain_dead_letter(handle);

    // The transport dropped while the republish was awaiting; the supervisor reset the
    // connection, and the broker redelivers both unacked messages to the new session.
    conn.reset();
    for entry in 0..2 {
        dead_letter(&mut conn, handle, entry, now);
    }
    conn.restore_dead_letter(handle, epoch, taken);

    assert_eq!(
        entries(&conn.drain_dead_letter(handle)),
        [0, 1],
        "the stale copies must be dropped, or the DLQ receives each message twice"
    );
}

#[test]
fn restoring_into_an_unknown_consumer_is_ignored() {
    let now = Instant::now();
    let (mut conn, handle) = dlq_consumer(now);
    dead_letter(&mut conn, handle, 0, now);
    let epoch = conn.session_epoch();
    let taken = conn.drain_dead_letter(handle);

    conn.restore_dead_letter(ConsumerHandle(handle.0 + 1), epoch, taken);

    assert!(
        conn.drain_dead_letter(handle).is_empty(),
        "the messages belong to the handle they were taken from, not to another consumer"
    );
}
