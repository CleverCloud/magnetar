// SPDX-License-Identifier: Apache-2.0

//! A publish the client has given up on, or a producer the client has closed, must not reach the
//! wire through a frame that was still staged when the client gave up.
//!
//! A producer keeps the frames of its sends in a per-slot `outbound` queue until the driver
//! drains them, and the drain is gated on the broker having acknowledged the (re-)attachment
//! (`broker_ready`). Three paths leave such a frame, or the message it was built from, behind:
//!
//! - **A send that timed out behind the gate.** The `send_timeout` sweep removes the send's
//!   `OpSend` from `pending` and resolves the caller with `SendError(-1, "send timeout")`, but
//!   leaves the staged frame in `outbound`. When the broker's `CommandProducerSuccess` reopens the
//!   gate the frame is written, so a message the caller was told failed is published (and
//!   duplicated if the caller retried).
//! - **A batched send that timed out inside the batch container.** The sweep removes the op but
//!   leaves the message in the container, and the next flush publishes it.
//! - **A producer closed with frames still staged.** `CommandCloseProducer` is encoded straight
//!   into the connection buffer while the slot's staged `CommandSend` frames stay behind it, so the
//!   wire carries the close and then the publishes. A `CommandProducerSuccess` answering an open
//!   that was sent before the close also reopens the gate of the closed slot and replays the
//!   relocated sends after it.
//!
//! Exercised through the tokio engine's [`ConnectionShared`] state. Layer (b) of the ADR-0024
//! four-layer test policy. Mirrors the matching `magnetar-runtime-moonpool` test, the
//! `magnetar-proto` unit test (`crates/magnetar-proto/tests/staged_frames_lifecycle.rs`) and the
//! `magnetar-differential` equivalence scenario.

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, OpOutcome, PendingOpKey, ProducerHandle,
    decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const BATCHED_SEND_TIMEOUT: Duration = Duration::from_secs(1);
const TIMED_OUT_MARKER: &[u8] = b"timed-out-batch-member";

/// One command the connection handed to the transport, reduced to what the scenarios compare.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Sent {
    /// `pb::base_command::Type` as the wire carries it.
    kind: i32,
    /// `CommandSend.sequence_id`, for a `CommandSend`.
    sequence_id: Option<u64>,
    /// `CommandProducer.request_id`, for a `CommandProducer`.
    open_request_id: Option<u64>,
    /// The payload region of a `CommandSend` (a batch frame carries every member).
    body: Bytes,
}

impl Sent {
    fn is(&self, kind: pb::base_command::Type) -> bool {
        self.kind == kind as i32
    }

    fn carries(&self, needle: &[u8]) -> bool {
        self.body
            .windows(needle.len())
            .any(|window| window == needle)
    }
}

fn connected_frame() -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(
        &mut buf,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-test".to_owned(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        },
    )
    .expect("encode CommandConnected");
    buf
}

fn producer_success_frame(request_id: u64) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(
        &mut buf,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id,
                producer_name: "staged-frames-lifecycle".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: None,
                producer_ready: Some(true),
            }),
            ..Default::default()
        },
    )
    .expect("encode CommandProducerSuccess");
    buf
}

/// The broker closing `producer` on its own initiative (a bundle unload), which makes the client
/// re-attach it in place.
fn broker_close_producer_frame(producer: ProducerHandle) -> BytesMut {
    let mut buf = BytesMut::new();
    encode_command(
        &mut buf,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::CloseProducer as i32,
            close_producer: Some(pb::CommandCloseProducer {
                producer_id: producer.0,
                request_id: 0,
                assigned_broker_service_url: None,
                assigned_broker_service_url_tls: None,
            }),
            ..Default::default()
        },
    )
    .expect("encode CommandCloseProducer");
    buf
}

fn message(payload: &'static [u8]) -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(payload),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: payload.len() as u32,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

/// Everything the connection hands to the transport now, in wire order.
fn transmitted(conn: &mut Connection) -> Vec<Sent> {
    let mut bytes = conn.poll_transmit();
    let mut sent = Vec::new();
    while !bytes.is_empty() {
        let frame = decode_one(&mut bytes).expect("a transmitted frame must decode");
        sent.push(Sent {
            kind: frame.command.r#type,
            sequence_id: frame.command.send.as_ref().map(|send| send.sequence_id),
            open_request_id: frame.command.producer.as_ref().map(|open| open.request_id),
            body: frame
                .payload
                .map(|payload| payload.body)
                .unwrap_or_default(),
        });
    }
    sent
}

/// The sequence ids of the `CommandSend` frames in `wire`, in wire order.
fn send_ids(wire: &[Sent]) -> Vec<u64> {
    wire.iter().filter_map(|sent| sent.sequence_id).collect()
}

/// A handshaken connection with one acknowledged producer.
fn attached_producer(
    conn: &mut Connection,
    t0: Instant,
    request: CreateProducerRequest,
) -> ProducerHandle {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &connected_frame())
        .expect("Connected");
    let _ = conn.poll_transmit();
    let create_rid = conn.peek_next_request_id_for_test();
    let producer = conn.create_producer(request);
    let _ = conn.poll_transmit();
    conn.handle_bytes(t0, &producer_success_frame(create_rid))
        .expect("ProducerSuccess");
    while conn.poll_event().is_some() {}
    producer
}

/// What the broker's re-attach acknowledgement released onto the wire after a send timed out
/// while its frame was staged behind the drain gate.
#[derive(Debug, PartialEq, Eq)]
struct ReattachObservation {
    /// Sequence id of the send whose `send_timeout` elapsed behind the gate.
    timed_out: u64,
    /// Sequence id of the send staged beside it whose `send_timeout` had not elapsed.
    survivor: u64,
    /// `CommandSend` frames written while the gate was shut.
    sends_before_ack: Vec<u64>,
    /// The error code the timed-out send's caller was resolved with.
    timeout_code: Option<i32>,
    /// `CommandSend` frames written once the re-attach acknowledgement reopened the gate.
    sends_after_ack: Vec<u64>,
}

/// The broker closes an attached producer, so its gate shuts while the client re-attaches it in
/// place. Two sends are staged behind the gate: `timed_out` at `t0` and `survivor` five seconds
/// later. The sweep at `t0 + send_timeout` expires `timed_out` alone, and then the broker
/// acknowledges the re-attach.
fn timed_out_send_across_in_place_reattach(
    conn: &mut Connection,
    t0: Instant,
) -> ReattachObservation {
    let producer = attached_producer(
        conn,
        t0,
        CreateProducerRequest {
            topic: "persistent://public/default/staged-frames-timeout".to_owned(),
            send_timeout: Some(SEND_TIMEOUT),
            ..Default::default()
        },
    );

    conn.handle_bytes(t0, &broker_close_producer_frame(producer))
        .expect("broker CloseProducer");
    while conn.poll_event().is_some() {}

    let timed_out = conn
        .send(producer, message(b"expires behind the gate"), 0, t0)
        .expect("queue the send that times out")
        .0;
    let later = t0 + SEND_TIMEOUT;
    let survivor = conn
        .send(producer, message(b"survives the sweep"), 0, later)
        .expect("queue the send that survives")
        .0;

    let before_ack = transmitted(conn);
    let reattach_rid = before_ack
        .iter()
        .find_map(|sent| sent.open_request_id)
        .expect("the broker close must start an in-place re-attach");

    conn.handle_timeout(later);
    let timeout_code = match conn.take_outcome(PendingOpKey::Send(
        producer,
        magnetar_proto::SequenceId(timed_out),
    )) {
        Some(OpOutcome::SendError { code, .. }) => Some(code),
        _ => None,
    };

    conn.handle_bytes(later, &producer_success_frame(reattach_rid))
        .expect("re-attach ProducerSuccess");
    while conn.poll_event().is_some() {}
    let after_ack = transmitted(conn);

    ReattachObservation {
        timed_out,
        survivor,
        sends_before_ack: send_ids(&before_ack),
        timeout_code,
        sends_after_ack: send_ids(&after_ack),
    }
}

/// A send whose `send_timeout` elapsed while its frame was staged behind the drain gate must not
/// be written when the gate reopens: its caller was told it failed.
#[test]
fn timed_out_send_staged_behind_the_gate_is_not_written_when_the_reattach_is_acknowledged() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    let seen = timed_out_send_across_in_place_reattach(&mut conn, t0);

    assert_eq!(
        seen.sends_before_ack,
        Vec::<u64>::new(),
        "precondition: nothing may be written while the re-attach is unacknowledged"
    );
    assert_eq!(
        seen.timeout_code,
        Some(-1),
        "precondition: the sweep resolved the timed-out send with the send-timeout error"
    );
    assert!(
        seen.sends_after_ack.contains(&seen.survivor),
        "control: the send that had not timed out must be written once the gate reopens \
         (written after the ack: {:?})",
        seen.sends_after_ack
    );
    assert!(
        !seen.sends_after_ack.contains(&seen.timed_out),
        "the send whose caller was told `send timeout` (sequence id {}) must not be written when \
         the re-attach is acknowledged (written after the ack: {:?})",
        seen.timed_out,
        seen.sends_after_ack
    );
}

/// What a flush wrote after a batched send timed out inside the batch container.
#[derive(Debug, PartialEq, Eq)]
struct BatchObservation {
    /// The error code the timed-out send's caller was resolved with.
    timeout_code: Option<i32>,
    /// Messages still held by the batch container after the sweep.
    batch_len_after_sweep: usize,
    /// Whether a `CommandSend` written by the later flush carries the timed-out message.
    flush_wrote_the_message: bool,
}

/// A batching producer holds one message below every batch limit, with no publish delay, so the
/// message waits for a flush. Its `send_timeout` elapses and the sweep fails the caller; a later
/// flush then runs.
fn batched_send_that_times_out_in_the_container(
    conn: &mut Connection,
    t0: Instant,
) -> BatchObservation {
    let producer = attached_producer(
        conn,
        t0,
        CreateProducerRequest {
            topic: "persistent://public/default/staged-frames-batch".to_owned(),
            enable_batching: true,
            send_timeout: Some(BATCHED_SEND_TIMEOUT),
            ..Default::default()
        },
    );

    let seq = conn
        .send(producer, message(TIMED_OUT_MARKER), 0, t0)
        .expect("queue the batched send");
    assert!(
        transmitted(conn)
            .iter()
            .all(|sent| !sent.is(pb::base_command::Type::Send)),
        "precondition: a batch below every limit with no publish delay waits for a flush"
    );

    conn.handle_timeout(t0 + BATCHED_SEND_TIMEOUT + Duration::from_secs(1));
    let timeout_code = match conn.take_outcome(PendingOpKey::Send(producer, seq)) {
        Some(OpOutcome::SendError { code, .. }) => Some(code),
        _ => None,
    };
    let batch_len_after_sweep = conn.producer_batch_len(producer);

    let _ = conn.flush_producer(
        producer,
        0,
        t0 + BATCHED_SEND_TIMEOUT + Duration::from_secs(2),
    );
    let flush_wrote_the_message = transmitted(conn)
        .iter()
        .any(|sent| sent.is(pb::base_command::Type::Send) && sent.carries(TIMED_OUT_MARKER));

    BatchObservation {
        timeout_code,
        batch_len_after_sweep,
        flush_wrote_the_message,
    }
}

/// A batched send that timed out while it was still in the batch container must not be published
/// by the next flush: its caller was told it failed.
#[test]
fn batched_send_that_timed_out_in_the_container_is_not_published_by_the_next_flush() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    let seen = batched_send_that_times_out_in_the_container(&mut conn, t0);

    assert_eq!(
        seen.timeout_code,
        Some(-1),
        "precondition: the sweep resolved the batched send with the send-timeout error"
    );
    assert!(
        !seen.flush_wrote_the_message,
        "the batched message whose caller was told `send timeout` must not be written by the \
         next flush"
    );
    assert_eq!(
        seen.batch_len_after_sweep, 0,
        "the sweep that failed the batched send must not leave its message in the container"
    );
}

/// The command order the wire carries when a producer is closed with a send still staged.
#[derive(Debug, PartialEq, Eq)]
struct CloseObservation {
    /// The sequence id of the staged send.
    staged: u64,
    /// Command kinds in wire order.
    kinds: Vec<i32>,
    /// `CommandSend` frames that follow the `CommandCloseProducer` on the wire.
    sends_after_close: Vec<u64>,
}

/// A send is staged through the per-slot hot path (the call `Producer::send` makes, which takes no
/// connection lock and drains nothing), and the application closes the producer before the driver
/// runs.
fn close_with_a_send_still_staged(conn: &mut Connection, t0: Instant) -> CloseObservation {
    let producer = attached_producer(
        conn,
        t0,
        CreateProducerRequest {
            topic: "persistent://public/default/staged-frames-close".to_owned(),
            ..Default::default()
        },
    );
    let slot = conn.producer(producer).cloned().expect("producer slot");
    let staged = slot
        .queue_send(message(b"staged before close"), 0, t0)
        .expect("stage the send on the hot path")
        .0;

    let _ = conn.close_producer(producer);
    let wire = transmitted(conn);

    let close_at = wire
        .iter()
        .position(|sent| sent.is(pb::base_command::Type::CloseProducer))
        .expect("the close must reach the wire");
    CloseObservation {
        staged,
        kinds: wire.iter().map(|sent| sent.kind).collect(),
        sends_after_close: send_ids(&wire[close_at + 1..]),
    }
}

/// A `CommandSend` staged before the producer was closed must not follow the
/// `CommandCloseProducer` on the wire: the broker has already been told to drop the producer.
#[test]
fn send_staged_before_close_is_not_written_after_the_close_producer_command() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    let seen = close_with_a_send_still_staged(&mut conn, t0);

    assert!(
        seen.sends_after_close.is_empty(),
        "the send staged before close (sequence id {}) was written after CommandCloseProducer; \
         wire order: {:?}",
        seen.staged,
        seen.kinds
    );
}

/// The user closes a producer after the client sent its re-open and before the broker answered it.
/// The producer's in-flight send had been relocated by the reset, and the broker's
/// `CommandProducerSuccess` for the re-open arrives afterwards.
fn close_between_the_reopen_and_its_acknowledgement(
    conn: &mut Connection,
    t0: Instant,
) -> Vec<u64> {
    let producer = attached_producer(
        conn,
        t0,
        CreateProducerRequest {
            topic: "persistent://public/default/staged-frames-close-reopen".to_owned(),
            ..Default::default()
        },
    );
    let _ = conn
        .send(producer, message(b"in flight at the reset"), 0, t0)
        .expect("queue the send");
    let _ = transmitted(conn);

    conn.reset();
    conn.begin_handshake().expect("re-handshake");
    conn.handle_bytes(t0, &connected_frame())
        .expect("reconnect Connected");
    let _ = conn.poll_transmit();

    let reopen = conn.rebuild_producers();
    let reopen_rid = reopen.first().expect("the producer is re-attached").0;
    let _ = transmitted(conn);

    let _ = conn.close_producer(producer);

    conn.handle_bytes(t0, &producer_success_frame(reopen_rid))
        .expect("re-open ProducerSuccess");
    while conn.poll_event().is_some() {}
    send_ids(&transmitted(conn))
}

/// The acknowledgement of a re-open must not reopen the drain gate of a producer the user closed:
/// the relocated send must not be replayed onto the wire after the `CommandCloseProducer`.
#[test]
fn reopen_acknowledged_after_close_does_not_replay_sends_for_the_closed_producer() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    let replayed = close_between_the_reopen_and_its_acknowledgement(&mut conn, t0);

    assert!(
        replayed.is_empty(),
        "the re-open acknowledgement replayed sends {replayed:?} for a producer the user had \
         already closed"
    );
}
