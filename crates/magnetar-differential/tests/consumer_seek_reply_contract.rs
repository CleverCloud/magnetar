// SPDX-License-Identifier: Apache-2.0

//! Direct sans-I/O seek-reply contracts executed by the simulation runner.
//!
//! Both runtimes leave the reply to a consumer seek to the shared `Connection`
//! ([ADR-0133](../../../specs/adr/0133-connection-owned-seek-reply-handling.md)): a seek the
//! broker rejects restores the consumer it froze, and a seek whose awaiting future was dropped is
//! handed over to the connection. These contracts drive that `Connection` through its public
//! API, reply by reply, the way the broker answers.

#![allow(clippy::expect_used)]
#![allow(clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::{Instant, SystemTime};

use bytes::{BufMut as _, BytesMut};
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, ConsumerHandle, MessageId, RequestId,
    SeekTarget, SubscribeRequest, decode_one, encode_command, encode_payload, pb,
};
use prost::Message as _;

/// The receiver queue of the consumers below.
const RQ: usize = 8;

/// Install, once per test binary, a subscriber that enables every event and discards the output,
/// so the fields of the `debug!` event a connection-owned re-attach logs are evaluated as they
/// are in a deployment that records them.
fn log_every_event() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::TRACE)
        .with_writer(std::io::sink)
        .try_init();
}

fn feed(conn: &mut Connection, command: &pb::BaseCommand) {
    let mut bytes = BytesMut::new();
    encode_command(&mut bytes, command).expect("encode broker command");
    conn.handle_bytes(Instant::now(), &bytes)
        .expect("handle broker command");
}

fn feed_success(conn: &mut Connection, request_id: u64) {
    feed(
        conn,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id,
                schema: None,
            }),
            ..Default::default()
        },
    );
}

fn feed_error(conn: &mut Connection, request_id: u64) {
    feed(
        conn,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id,
                error: pb::ServerError::UnknownError as i32,
                message: "broker says no".to_owned(),
            }),
            ..Default::default()
        },
    );
}

/// A broker-initiated same-broker `CommandCloseConsumer` for `handle`.
fn feed_broker_close(conn: &mut Connection, handle: ConsumerHandle) {
    feed(
        conn,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::CloseConsumer as i32,
            close_consumer: Some(pb::CommandCloseConsumer {
                consumer_id: handle.0,
                request_id: 0,
                assigned_broker_service_url: None,
                assigned_broker_service_url_tls: None,
            }),
            ..Default::default()
        },
    );
}

fn message_frame(
    consumer_id: u64,
    entry_id: u64,
    metadata: &pb::MessageMetadata,
    body: &[u8],
) -> BytesMut {
    let command = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id,
            message_id: pb::MessageIdData {
                ledger_id: 7,
                entry_id,
                ..Default::default()
            },
            redelivery_count: Some(0),
            ack_set: Vec::new(),
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &command, metadata, body).expect("encode message frame");
    frame
}

fn plain_metadata(entry_id: u64) -> pb::MessageMetadata {
    pb::MessageMetadata {
        producer_name: "seek-reply-contract".to_owned(),
        sequence_id: entry_id,
        publish_time: 1_700_000_000_000,
        ..Default::default()
    }
}

/// Dispatch entries `first..first + count` to `handle`.
fn deliver(conn: &mut Connection, handle: ConsumerHandle, first: u64, count: u64) {
    for entry in first..first + count {
        let frame = message_frame(handle.0, entry, &plain_metadata(entry), b"x");
        conn.handle_bytes(Instant::now(), &frame).expect("deliver");
    }
}

/// The `CommandSubscribe` request ids and `CommandFlow` permits staged for `handle`.
fn drain_outbound(conn: &mut Connection, handle: ConsumerHandle) -> (Vec<u64>, Vec<u32>) {
    let mut out = conn.poll_transmit();
    let (mut subscribes, mut grants) = (Vec::new(), Vec::new());
    while !out.is_empty() {
        let command = decode_one(&mut out).expect("decode outbound").command;
        if let Some(subscribe) = command.subscribe
            && subscribe.consumer_id == handle.0
        {
            subscribes.push(subscribe.request_id);
        }
        if let Some(flow) = command.flow
            && flow.consumer_id == handle.0
        {
            grants.push(flow.message_permits);
        }
    }
    (subscribes, grants)
}

/// The entry ids a `receive()` would take now, in order.
fn pop_entries(conn: &mut Connection, handle: ConsumerHandle) -> Vec<u64> {
    let mut entries = Vec::new();
    while let Some(message) = conn.pop_message(handle, Instant::now()) {
        entries.push(message.message_id.entry_id);
    }
    entries
}

/// `(queued, permit_balance, granted_permits)` of `handle`.
fn ledger(conn: &Connection, handle: ConsumerHandle) -> (usize, u32, u32) {
    let state = conn.consumer(handle).expect("registered").state.lock();
    (
        state.queue.len(),
        state.permit_balance,
        state.granted_permits,
    )
}

fn pending_seek(conn: &Connection, handle: ConsumerHandle) -> Option<RequestId> {
    conn.consumer(handle)
        .expect("registered")
        .state
        .lock()
        .pending_seek
}

/// An established consumer with its initial window of `receiver_queue_size` granted.
fn established_consumer(receiver_queue_size: usize) -> (Connection, ConsumerHandle) {
    let mut conn = Connection::new(
        ConnectionConfig {
            stats_interval: None,
            ..ConnectionConfig::default()
        },
        Arc::new(SystemTime::now),
    );
    conn.begin_handshake().expect("handshake");
    feed(
        &mut conn,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "seek-reply-contract".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        },
    );
    while conn.poll_event().is_some() {}
    let subscribe = conn.peek_next_request_id_for_test();
    let handle = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/seek-reply-contract".to_owned(),
        subscription: "seek-reply-contract".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        receiver_queue_size,
        ..Default::default()
    });
    feed_success(&mut conn, subscribe);
    assert!(conn.consume_initial_consumer_subscribe_completion(handle));
    while conn.poll_event().is_some() {}
    let _ = conn.initial_flow(handle, Instant::now());
    let _ = drain_outbound(&mut conn, handle);
    (conn, handle)
}

fn seek_earliest(conn: &mut Connection, handle: ConsumerHandle) -> RequestId {
    let seek = conn.seek(handle, SeekTarget::MessageId(MessageId::EARLIEST));
    let _ = drain_outbound(conn, handle);
    seek
}

#[test]
fn a_rejected_seek_holds_at_most_the_window_and_delivers_it_in_order() {
    let (mut conn, handle) = established_consumer(RQ);
    deliver(&mut conn, handle, 0, 6);
    let seek = seek_earliest(&mut conn, handle);

    // The broker holds two permits; the third and fourth frames exceed the window and are not held.
    deliver(&mut conn, handle, 6, 4);
    feed_error(&mut conn, seek.0);

    assert_eq!(
        pop_entries(&mut conn, handle),
        vec![0, 1, 2, 3, 4, 5, 6, 7],
        "the six queued messages, then the two the window allows, in order"
    );
}

#[test]
fn a_rejected_seek_keeps_the_flow_granted_while_it_was_in_flight() {
    let (mut conn, handle) = established_consumer(RQ);
    deliver(&mut conn, handle, 0, 3);
    let seek = seek_earliest(&mut conn, handle);
    conn.flow(handle, 4);
    let (_, grants) = drain_outbound(&mut conn, handle);
    assert_eq!(grants, vec![4], "the grant went out on the wire");

    feed_error(&mut conn, seek.0);

    assert_eq!(
        ledger(&conn, handle),
        (3, RQ as u32 - 3 + 4, RQ as u32 + 4),
        "the broker holds the snapshot's window plus the four it was granted mid-seek"
    );
}

#[test]
fn a_rejected_seek_keeps_the_markers_dispatched_while_it_was_in_flight() {
    let (mut conn, handle) = established_consumer(RQ);
    deliver(&mut conn, handle, 0, 3);
    let seek = seek_earliest(&mut conn, handle);
    let marker = pb::MessageMetadata {
        producer_name: "broker-marker".to_owned(),
        publish_time: 1_700_000_000_000,
        marker_type: Some(10),
        ..Default::default()
    };
    conn.handle_bytes(Instant::now(), &message_frame(handle.0, 3, &marker, b""))
        .expect("deliver a PIP-33 marker mid-seek");

    feed_error(&mut conn, seek.0);

    assert_eq!(
        ledger(&conn, handle),
        (3, RQ as u32 - 3 - 1, RQ as u32),
        "the marker spent one permit of the snapshot's window"
    );
}

#[test]
fn a_rejected_seek_holds_a_batch_by_its_unacknowledged_entries() {
    let (mut conn, handle) = established_consumer(RQ);
    // Six of the eight permits are spent before the seek, so the broker holds a window of two.
    deliver(&mut conn, handle, 0, 6);
    let seek = seek_earliest(&mut conn, handle);
    let mut body = BytesMut::new();
    for payload in [b"a".as_slice(), b"b", b"c"] {
        let single = pb::SingleMessageMetadata {
            payload_size: i32::try_from(payload.len()).expect("small payload"),
            ..Default::default()
        }
        .encode_to_vec();
        body.put_u32(u32::try_from(single.len()).expect("small metadata"));
        body.extend_from_slice(&single);
        body.extend_from_slice(payload);
    }
    let batch = pb::MessageMetadata {
        num_messages_in_batch: Some(3),
        ..plain_metadata(6)
    };
    // The three-entry batch is the frame that crosses the window: it is held, and it weighs
    // three units, so the single frame after it lies past the window and is not held.
    conn.handle_bytes(Instant::now(), &message_frame(handle.0, 6, &batch, &body))
        .expect("deliver a batch mid-seek");
    deliver(&mut conn, handle, 7, 1);

    feed_error(&mut conn, seek.0);

    assert_eq!(
        pop_entries(&mut conn, handle),
        vec![0, 1, 2, 3, 4, 5, 6, 6, 6],
        "the six queued messages, then the held batch whole; the frame after the batch was past \
         the window the batch's three entries filled"
    );
}

struct CountingWake(AtomicUsize);

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn a_rejected_seek_wakes_a_receive_parked_on_the_frozen_queue() {
    let (mut conn, handle) = established_consumer(RQ);
    deliver(&mut conn, handle, 0, 2);
    let seek = seek_earliest(&mut conn, handle);
    let wakes = Arc::new(CountingWake(AtomicUsize::new(0)));
    let _ = conn
        .consumer(handle)
        .expect("registered")
        .state
        .lock()
        .register_receive_waker(Waker::from(Arc::clone(&wakes)));

    feed_error(&mut conn, seek.0);

    assert_eq!(
        wakes.0.load(Ordering::SeqCst),
        1,
        "the restored messages are receivable again: the parked receive must be woken"
    );
    assert_eq!(pop_entries(&mut conn, handle), vec![0, 1]);
}

#[test]
fn a_second_seek_keeps_the_first_snapshot_and_the_older_rejection_thaws_nothing() {
    let (mut conn, handle) = established_consumer(RQ);
    deliver(&mut conn, handle, 0, 3);
    let first = conn.seek(handle, SeekTarget::MessageId(MessageId::EARLIEST));
    let second = conn.seek(handle, SeekTarget::MessageId(MessageId::LATEST));
    let _ = drain_outbound(&mut conn, handle);

    feed_error(&mut conn, first.0);
    assert_eq!(
        pending_seek(&conn, handle),
        Some(second),
        "the older seek's rejection must not thaw the newer seek"
    );
    deliver(&mut conn, handle, 3, 1);
    assert_eq!(
        ledger(&conn, handle).0,
        0,
        "still frozen for the newer seek"
    );

    feed_error(&mut conn, second.0);
    assert_eq!(
        pop_entries(&mut conn, handle),
        vec![0, 1, 2, 3],
        "the first seek's snapshot, then the dispatch held for the newer one"
    );
}

#[test]
fn a_seek_rejected_after_the_broker_closed_the_consumer_re_attaches_it() {
    log_every_event();
    let (mut conn, handle) = established_consumer(RQ);
    let seek = seek_earliest(&mut conn, handle);
    feed_broker_close(&mut conn, handle);
    let (subscribes, _) = drain_outbound(&mut conn, handle);
    assert!(
        subscribes.is_empty(),
        "mid-seek the close arm leaves the re-attach to the seek"
    );

    feed_error(&mut conn, seek.0);

    let (subscribes, grants) = drain_outbound(&mut conn, handle);
    assert_eq!(
        subscribes.len(),
        1,
        "the rejected seek must re-attach the lost consumer"
    );
    assert!(
        grants.is_empty(),
        "flow waits for the re-subscribe's Success"
    );
    feed_success(&mut conn, subscribes[0]);
    let (_, grants) = drain_outbound(&mut conn, handle);
    assert_eq!(
        grants,
        vec![RQ as u32],
        "a fresh dispatcher slot gets one full window"
    );
}

#[test]
fn an_abandoned_seek_of_a_zero_queue_consumer_releases_no_empty_flow() {
    log_every_event();
    let (mut conn, handle) = established_consumer(0);
    let seek = seek_earliest(&mut conn, handle);
    assert!(conn.abandon_consumer_seek_waiter(handle, seek));

    feed_success(&mut conn, seek.0);
    let (subscribes, _) = drain_outbound(&mut conn, handle);
    assert_eq!(
        subscribes.len(),
        1,
        "the connection re-attaches the consumer"
    );
    feed_success(&mut conn, subscribes[0]);
    let (_, grants) = drain_outbound(&mut conn, handle);
    assert!(grants.is_empty(), "no zero-permit CommandFlow: {grants:?}");
}

#[test]
fn abandoning_a_seek_waiter_hands_over_only_this_consumers_seek() {
    let (mut conn, handle) = established_consumer(RQ);
    let lookup = conn.lookup("persistent://public/default/seek-reply-contract", false);
    assert!(
        !conn.abandon_consumer_seek_waiter(handle, lookup),
        "a pending request of another kind is not a seek to hand over"
    );
    let seek = seek_earliest(&mut conn, handle);
    assert!(
        !conn.abandon_consumer_seek_waiter(ConsumerHandle(handle.0 + 1), seek),
        "another consumer's seek is not this consumer's to hand over"
    );
    assert!(
        !conn.abandon_consumer_seek_waiter(handle, RequestId(u64::MAX)),
        "a request id the connection holds no seek or owed re-attach for hands nothing over"
    );
    assert_eq!(
        pending_seek(&conn, handle),
        Some(seek),
        "the seek is untouched"
    );
}

#[test]
fn seek_replies_for_a_consumer_already_dropped_change_nothing() {
    // A rejection, an awaited seek's acknowledgement, and a forgotten seek's acknowledgement,
    // each arriving after the consumer was dropped from the connection.
    for reply in ["rejected", "acked", "forgotten-acked"] {
        let (mut conn, handle) = established_consumer(RQ);
        let seek = seek_earliest(&mut conn, handle);
        if reply == "forgotten-acked" {
            assert!(conn.abandon_consumer_seek_waiter(handle, seek));
        }
        conn.cancel_consumer_subscribe(handle);
        let _ = drain_outbound(&mut conn, handle);
        assert!(conn.consumer(handle).is_none(), "{reply}: the slot is gone");

        if reply == "rejected" {
            feed_error(&mut conn, seek.0);
        } else {
            feed_success(&mut conn, seek.0);
        }

        let (subscribes, grants) = drain_outbound(&mut conn, handle);
        assert!(
            subscribes.is_empty() && grants.is_empty(),
            "{reply}: a dropped consumer is not re-attached or credited"
        );
        assert!(
            conn.resubscribe_consumer_after_seek(handle).is_none(),
            "{reply}: an awaiting continuation finds nothing to re-attach"
        );
    }
}

#[test]
fn a_dispatch_for_a_consumer_the_connection_does_not_hold_is_dropped() {
    let (mut conn, handle) = established_consumer(RQ);
    while conn.poll_event().is_some() {}
    let stranger = handle.0 + 1;
    conn.handle_bytes(
        Instant::now(),
        &message_frame(stranger, 0, &plain_metadata(0), b"x"),
    )
    .expect("a dispatch for an unknown consumer id is not a protocol error");

    assert_eq!(
        ledger(&conn, handle).0,
        0,
        "the held consumer receives nothing"
    );
    let mut events = Vec::new();
    while let Some(event) = conn.poll_event() {
        events.push(event);
    }
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ConnectionEvent::Message { .. })),
        "no message event for a consumer nobody holds: {events:?}"
    );
}

#[test]
fn a_rejected_seek_repays_at_once_the_units_spent_past_the_window() {
    let (mut conn, handle) = established_consumer(RQ);
    deliver(&mut conn, handle, 0, RQ as u64);
    for expected in 0..3 {
        let message = conn
            .pop_message(handle, Instant::now())
            .expect("a queued message");
        assert_eq!(message.message_id.entry_id, expected);
    }
    let (_, grants) = drain_outbound(&mut conn, handle);
    assert!(
        grants.is_empty(),
        "three consumed units are below the refill threshold"
    );
    let seek = seek_earliest(&mut conn, handle);

    // The broker holds no permit left, so a dispatch during the seek is past the window.
    deliver(&mut conn, handle, RQ as u64, 1);
    feed_error(&mut conn, seek.0);

    let (_, grants) = drain_outbound(&mut conn, handle);
    assert_eq!(
        grants,
        vec![4],
        "the three units consumed before the seek and the one past the window reach the \
         threshold together and are repaid by the rejection itself"
    );
}

#[test]
fn after_the_broker_closed_the_consumer_mid_seek_nothing_is_set_aside_for_it() {
    let (mut conn, handle) = established_consumer(RQ);
    let first = seek_earliest(&mut conn, handle);
    feed_broker_close(&mut conn, handle);
    let _ = drain_outbound(&mut conn, handle);

    // A dispatch the broker sent before it closed the consumer is not held for it.
    deliver(&mut conn, handle, 0, 1);
    assert_eq!(ledger(&conn, handle).0, 0, "nothing is queued or held");
    // The application seeks again before the first seek's reply: there is no snapshot to fold.
    let second = seek_earliest(&mut conn, handle);

    feed_error(&mut conn, first.0);
    let (subscribes, _) = drain_outbound(&mut conn, handle);
    assert!(
        subscribes.is_empty(),
        "the older seek's rejection is superseded by the newer seek"
    );
    feed_error(&mut conn, second.0);
    let (subscribes, _) = drain_outbound(&mut conn, handle);
    assert_eq!(
        subscribes.len(),
        1,
        "the newer seek's rejection re-attaches the lost consumer once"
    );
    feed_success(&mut conn, subscribes[0]);
    let (_, grants) = drain_outbound(&mut conn, handle);
    assert_eq!(
        grants,
        vec![RQ as u32],
        "one full window for the re-attached consumer"
    );
    assert_eq!(pop_entries(&mut conn, handle), Vec::<u64>::new());
}
