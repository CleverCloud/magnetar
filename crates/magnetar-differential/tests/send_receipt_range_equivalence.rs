// SPDX-License-Identifier: Apache-2.0

//! A `CommandSendReceipt` must be applied in time bounded by the producer's own in-flight sends,
//! identically on both engines (ADR-0024 layer (d)).
//!
//! A batching producer folds its per-message publishes into one `CommandSend` carrying
//! `sequence_id = lowest` and `highest_sequence_id = highest`, and the broker's receipt echoes
//! that range, so the `SendReceipt` arm fans it out over `[lowest, highest]`. The arm walks the
//! range one sequence id at a time and bounds `highest` by nothing but `highest >= lowest` and
//! `highest != u64::MAX`, so a receipt that claims a range far wider than anything the producer
//! sent keeps the thread in the loop, with the connection mutex and the producer slot lock held,
//! for as long as the range is long. Both engines wrap the same `magnetar-proto` state machine
//! and apply the broker's frames under the same mutex, so both stop alike today; the scenario
//! pins that neither does.
//!
//! Each engine runs on its own worker thread and the test waits for it with a deadline, so a
//! regression fails here instead of hanging the run.

use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, OpOutcome, PendingOpKey, ProducerHandle,
    SequenceId, encode_command, pb,
};

/// How long one engine may take to apply the scenario. The correct client needs microseconds;
/// the bound only keeps a failing run from hanging.
const DEADLINE: Duration = Duration::from_secs(10);

/// How far past its own sequence id the hostile receipt claims the broker persisted: at tens of
/// nanoseconds per sequence id the unbounded walk needs hours.
const CLAIMED_SPAN: u64 = 1 << 40;

type Scenario = fn(&mut Connection, Instant) -> Vec<String>;
type Projection = fn(Scenario) -> Vec<String>;

fn connected_frame() -> BytesMut {
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-differential".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        },
    )
    .expect("encode Connected");
    frame
}

fn producer_success_frame(request_id: u64) -> BytesMut {
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id,
                producer_name: "send-receipt-range".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: None,
                producer_ready: Some(true),
            }),
            ..Default::default()
        },
    )
    .expect("encode ProducerSuccess");
    frame
}

/// The broker's `CommandSendReceipt` for `sequence_id`, claiming it persisted everything up to
/// and including `highest_sequence_id`.
fn ranged_receipt_frame(
    producer: ProducerHandle,
    sequence_id: SequenceId,
    highest_sequence_id: u64,
) -> BytesMut {
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::SendReceipt as i32,
            send_receipt: Some(pb::CommandSendReceipt {
                producer_id: producer.0,
                sequence_id: sequence_id.0,
                message_id: Some(pb::MessageIdData {
                    ledger_id: 7,
                    entry_id: sequence_id.0,
                    partition: None,
                    batch_index: None,
                    ack_set: vec![],
                    batch_size: None,
                    first_chunk_message_id: None,
                }),
                highest_sequence_id: Some(highest_sequence_id),
            }),
            ..Default::default()
        },
    )
    .expect("encode SendReceipt");
    frame
}

fn message() -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(b"ranged"),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: 6,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

fn tokio_projection(run: Scenario) -> Vec<String> {
    let at = Instant::now();
    let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    run(&mut conn, at)
}

fn moonpool_projection(run: Scenario) -> Vec<String> {
    let at = Instant::now();
    let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    run(&mut conn, at)
}

fn connect(conn: &mut Connection, at: Instant) {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(at, &connected_frame())
        .expect("Connected");
    let _ = conn.poll_event();
}

fn open_producer(conn: &mut Connection, at: Instant, batching: bool) -> ProducerHandle {
    let request_id = conn.peek_next_request_id_for_test();
    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/send-receipt-range".to_owned(),
        enable_batching: batching,
        max_batch_size_bytes: 4096,
        max_messages_in_batch: 100,
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(at, &producer_success_frame(request_id))
        .expect("ProducerSuccess");
    while conn.poll_event().is_some() {}
    producer
}

fn outcome_label(outcome: Option<&OpOutcome>) -> &'static str {
    match outcome {
        Some(OpOutcome::SendReceipt { .. }) => "receipt",
        Some(OpOutcome::SendError { .. }) => "send error",
        Some(_) => "other outcome",
        None => "no outcome",
    }
}

/// One send is in flight and the broker's receipt for it claims it persisted `CLAIMED_SPAN` more.
/// Whether the receipt is applied, clamped or refused is the fix's call: the scenario records
/// what each engine did so the two can be compared, and pins only that the call returned.
fn hostile_receipt(conn: &mut Connection, at: Instant) -> Vec<String> {
    connect(conn, at);
    let producer = open_producer(conn, at, false);
    let seq = conn.send(producer, message(), 0, at).expect("queue send");
    let _ = conn.poll_transmit();

    let applied = conn.handle_bytes(
        at,
        &ranged_receipt_frame(producer, seq, seq.0 + CLAIMED_SPAN),
    );
    while conn.poll_event().is_some() {}
    vec![
        "receipt applied: returned".to_owned(),
        format!(
            "handle_bytes: {}",
            if applied.is_ok() { "ok" } else { "refused" }
        ),
        format!(
            "named send: {}",
            outcome_label(
                conn.take_outcome(PendingOpKey::Send(producer, seq))
                    .as_ref()
            )
        ),
    ]
}

/// A genuine batch receipt for `[first, second]` resolves both members.
fn real_batch_receipt(conn: &mut Connection, at: Instant) -> Vec<String> {
    connect(conn, at);
    let producer = open_producer(conn, at, true);
    let first = conn.send(producer, message(), 0, at).expect("first");
    let second = conn.send(producer, message(), 0, at).expect("second");
    assert_eq!(conn.flush_producer(producer, 0, at), 1, "one batch frame");
    let _ = conn.poll_transmit();

    conn.handle_bytes(at, &ranged_receipt_frame(producer, first, second.0))
        .expect("batch SendReceipt");
    while conn.poll_event().is_some() {}
    vec![
        format!(
            "first: {}",
            outcome_label(
                conn.take_outcome(PendingOpKey::Send(producer, first))
                    .as_ref()
            )
        ),
        format!(
            "second: {}",
            outcome_label(
                conn.take_outcome(PendingOpKey::Send(producer, second))
                    .as_ref()
            )
        ),
    ]
}

/// One engine's scenario on its own thread.
struct Run {
    worker: JoinHandle<()>,
    trace: Arc<Mutex<Option<Vec<String>>>>,
    started: Instant,
}

impl Run {
    fn start(projection: Projection, scenario: Scenario) -> Self {
        let trace = Arc::new(Mutex::new(None));
        let worker_trace = Arc::clone(&trace);
        let worker = std::thread::spawn(move || {
            let result = projection(scenario);
            *worker_trace.lock().expect("trace slot") = Some(result);
        });
        Self {
            worker,
            trace,
            started: Instant::now(),
        }
    }

    /// The trace, or `None` when the scenario was still running at the deadline (its thread is
    /// abandoned and dies with the test process).
    fn finish(self) -> Option<Vec<String>> {
        loop {
            if self.worker.is_finished() {
                self.worker.join().expect("scenario thread panicked");
                return self.trace.lock().expect("trace slot").take();
            }
            if self.started.elapsed() >= DEADLINE {
                return None;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// Run `scenario` on both engines at once and return `(tokio, moonpool)`, each `None` when that
/// engine was still running at the deadline.
fn on_both_engines(scenario: Scenario) -> (Option<Vec<String>>, Option<Vec<String>>) {
    let tokio = Run::start(tokio_projection, scenario);
    let moonpool = Run::start(moonpool_projection, scenario);
    (tokio.finish(), moonpool.finish())
}

#[test]
fn engines_apply_a_receipt_with_an_unbounded_highest_sequence_id_in_bounded_time() {
    let (tokio, moonpool) = on_both_engines(hostile_receipt);

    let spinning: Vec<&str> = [("tokio", &tokio), ("moonpool", &moonpool)]
        .into_iter()
        .filter(|(_, trace)| trace.is_none())
        .map(|(engine, _)| engine)
        .collect();
    assert!(
        spinning.is_empty(),
        "engine(s) {spinning:?} were still applying a CommandSendReceipt whose \
         highest_sequence_id lies {CLAIMED_SPAN} past the only send in flight after {DEADLINE:?}: \
         the SendReceipt arm walks the broker-supplied range with the connection mutex held"
    );
    assert_eq!(
        tokio, moonpool,
        "engines must treat the unbounded receipt identically"
    );
}

#[test]
fn engines_resolve_every_member_of_a_real_batch_receipt() {
    let (tokio, moonpool) = on_both_engines(real_batch_receipt);

    let expected = vec!["first: receipt".to_owned(), "second: receipt".to_owned()];
    assert_eq!(tokio, Some(expected.clone()), "tokio trace");
    assert_eq!(moonpool, Some(expected), "moonpool trace");
}
