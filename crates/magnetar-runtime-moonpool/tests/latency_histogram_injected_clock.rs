// SPDX-License-Identifier: Apache-2.0

//! ADR-0011 / ADR-0086, ADR-0024 layer (c): the receive- and send-latency
//! histograms are a pure function of the INJECTED clock.
//!
//! Both `ConsumerState::pop_message` and `ProducerState::apply_receipt` used
//! to read the host clock via `Instant::elapsed()`, which made the latency
//! percentiles the one part of `ConsumerStats` / `ProducerStats` that was NOT
//! reproducible under simulation. These tests pin the fix by running one
//! identical script TWICE, with real host-wall-clock motion between the two
//! runs, and asserting the histograms are identical AND equal to the exact
//! scripted delta. Pre-fix, run A saw ~0 ms and run B saw ~`HOST_DRIFT` ms.
//!
//! Why not a full `SimulationBuilder` workload here: under `SimProviders`,
//! virtual time vastly outruns host time, so the pre-fix
//! `arrived_at.elapsed()` saturated to 0 on *every* iteration and two
//! iterations of the same seed agreed trivially. A cross-iteration equality
//! assertion inside a sim workload would therefore have been GREEN before the
//! fix — a characterization test wearing a regression test's clothes. Driving
//! `ConnectionShared` with an explicitly pinned `now_instant_provider` — the
//! same indirection `Consumer::receive` reads through
//! `self.shared.now_instant()` — is both red pre-fix and free of any workload
//! scaffolding.
//!
//! Shape borrowed from `poll_transmit_vectored_parity.rs` (two
//! `ConnectionShared`s driven identically, compare the observable) and
//! `clock_injection.rs` (pinned provider, two constructions, identical
//! observable state). Tokio sibling (ADR-0024 1:1 runtime-test-parity):
//! `crates/magnetar-runtime-tokio/tests/latency_histogram_injected_clock.rs`.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use hdrhistogram::Histogram;
use magnetar_proto::{
    Connection, ConnectionConfig, ConsumerHandle, CreateProducerRequest, SubscribeRequest,
    encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::{ConnectionShared, DETERMINISTIC_SIM_EPOCH_MS};

use crate::common::handshake_response_bytes;

/// Messages delivered/popped per run.
const N: u64 = 4;

/// Scripted virtual instants, all derived from ONE base captured before
/// either run. `ARRIVE_AT_MS > 0` matters: it puts the pre-fix `arrived_at`
/// in the host *future* during run A (so `elapsed()` saturates to 0) while
/// run B, taken after `HOST_DRIFT` of real sleep, sees a positive elapsed —
/// which is what makes the two runs disagree before the fix.
const ARRIVE_AT_MS: u64 = 100;
const POP_AT_MS: u64 = 350;

/// The value every sample must carry. `<= 2047` keeps `hdrhistogram`
/// sigfig-3 quantisation exact.
const EXPECTED_DWELL_MS: u64 = POP_AT_MS - ARRIVE_AT_MS;

/// Producer leg: enqueue at `ENQUEUE_AT_MS`, receipt at `RECEIPT_AT_MS`.
const ENQUEUE_AT_MS: u64 = 100;
const RECEIPT_AT_MS: u64 = 350;
const EXPECTED_RTT_MS: u64 = RECEIPT_AT_MS - ENQUEUE_AT_MS;

/// Real host-clock motion injected BETWEEN the two runs. Pre-fix this lands
/// straight in run B's histogram; post-fix it is invisible.
const HOST_DRIFT: Duration = Duration::from_millis(120);

/// Compact, fully-informative projection of a histogram: every recorded
/// `(value, count)` pair. Comparing these instead of the `Histogram` structs
/// directly keeps a failure message to one readable line — the raw `Debug`
/// impl prints all 2048 sub-buckets.
fn recorded(hist: &Histogram<u64>) -> Vec<(u64, u64)> {
    hist.iter_recorded()
        .map(|it| (it.value_iterated_to(), it.count_at_value()))
        .collect()
}

fn deliver_message(conn: &mut Connection, at: Instant, handle: ConsumerHandle, entry: u64) {
    let msg_cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: 9,
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
    encode_payload(
        &mut frame,
        &msg_cmd,
        &metadata,
        format!("m{entry}").as_bytes(),
    )
    .expect("encode message frame");
    conn.handle_bytes(at, &frame).expect("deliver message");
    while conn.poll_event().is_some() {}
}

fn subscribe_and_ack(conn: &mut Connection, at: Instant) -> ConsumerHandle {
    let req = SubscribeRequest {
        topic: "persistent://public/default/latency-hist-injected-clock".to_owned(),
        subscription: "magnetar-test-latency-hist-injected-clock".to_owned(),
        sub_type: pb::command_subscribe::SubType::Shared,
        receiver_queue_size: N as usize,
        ..Default::default()
    };
    let request_id = conn.peek_next_request_id_for_test();
    let handle = conn.subscribe(req);
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

/// Drive the deliver → pop script once, against a `ConnectionShared` whose
/// `now_instant_provider` is PINNED at `base + POP_AT_MS`. `pop_message` is
/// called with `shared.now_instant()` — exactly what the moonpool
/// `ReceiveFut::poll` does in production — so this exercises the real
/// injection path, not a hand-passed constant.
fn run_receive_script(base: Instant) -> Histogram<u64> {
    let pop_now = base + Duration::from_millis(POP_AT_MS);
    let provider: Arc<dyn Fn() -> Instant + Send + Sync> = Arc::new(move || pop_now);
    // NOTE: `with_auth_wall_clock_and_instant` is a CONSTRUCTOR, not a swap —
    // it installs through `Arc::get_mut` and `debug_assert!`s refcount 1, so
    // it must be called before the `Arc` is ever cloned.
    let shared = ConnectionShared::with_auth_wall_clock_and_instant(
        ConnectionConfig::default(),
        None,
        DETERMINISTIC_SIM_EPOCH_MS,
        provider,
    );

    let arrive = base + Duration::from_millis(ARRIVE_AT_MS);
    let mut conn = shared.inner.lock();
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(arrive, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let handle = subscribe_and_ack(&mut conn, arrive);
    for entry in 0..N {
        deliver_message(&mut conn, arrive, handle, entry);
    }
    for _ in 0..N {
        conn.pop_message(handle, shared.now_instant())
            .expect("queued message");
    }

    conn.consumer(handle)
        .expect("consumer slot registered")
        .state
        .lock()
        .receive_latency_histogram()
        .expect("receive_latency_hist initialised")
}

/// Producer leg. Unlike the consumer, this needs no pinned provider: the
/// receipt clock reaches `apply_receipt` through `handle_bytes(now, …)` →
/// `handle_frame(now, …)`, so the producer half of the leak was fixable
/// without touching the engine at all.
fn run_send_script(base: Instant) -> Histogram<u64> {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let enqueue_at = base + Duration::from_millis(ENQUEUE_AT_MS);
    let receipt_at = base + Duration::from_millis(RECEIPT_AT_MS);

    let mut conn = shared.inner.lock();
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(enqueue_at, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/latency-hist-injected-clock-producer".to_owned(),
        ..Default::default()
    });

    for entry in 0..N {
        let seq = conn
            .send(
                producer,
                magnetar_proto::producer::OutgoingMessage {
                    payload: bytes::Bytes::from_static(b"hi"),
                    metadata: pb::MessageMetadata::default(),
                    uncompressed_size: 2,
                    num_messages: 1,
                    txn_id: None,
                    source_message_id: None,
                },
                0,
                enqueue_at,
            )
            .expect("send queues");

        let receipt = pb::BaseCommand {
            r#type: pb::base_command::Type::SendReceipt as i32,
            send_receipt: Some(pb::CommandSendReceipt {
                producer_id: producer.0,
                sequence_id: seq.0,
                message_id: Some(pb::MessageIdData {
                    ledger_id: 7,
                    entry_id: entry,
                    partition: None,
                    batch_index: None,
                    ack_set: vec![],
                    batch_size: None,
                    first_chunk_message_id: None,
                }),
                highest_sequence_id: None,
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_command(&mut buf, &receipt).expect("encode CommandSendReceipt");
        conn.handle_bytes(receipt_at, &buf).expect("apply receipt");
        while conn.poll_event().is_some() {}
    }

    conn.producer(producer)
        .expect("producer slot registered")
        .state
        .lock()
        .send_latency_histogram()
        .expect("send_latency_hist initialised")
}

#[test]
fn moonpool_receive_latency_histogram_is_deterministic_across_identical_runs() {
    // ONE base for both runs: the script is byte-identical and only the host
    // clock differs between them.
    let base = Instant::now();

    let first = run_receive_script(base);
    std::thread::sleep(HOST_DRIFT);
    let second = run_receive_script(base);

    // (1) Determinism: identical script + injected clock => identical
    //     histogram, regardless of host-wall-clock motion. Compared via the
    //     compact `(value, count)` projection so a failure is readable; the
    //     full-struct equality below then rules out any bucket-level drift
    //     the projection could hide.
    assert_eq!(
        recorded(&first),
        recorded(&second),
        "receive_latency_hist moved with the HOST clock across two identical runs — \
         the Instant::elapsed() leak is back"
    );
    assert_eq!(
        first, second,
        "histograms differ beyond their recorded values"
    );

    // (2) Non-vacuity + exactness: the histogram is the SCRIPTED delta, not
    //     an accidental all-zero agreement. Assertion (1) alone could pass on
    //     a host where both runs happened to saturate to 0.
    assert_eq!(first.len(), N, "one sample per popped message");
    assert_eq!(
        first.max(),
        EXPECTED_DWELL_MS,
        "sample must be `now_instant() - arrived_at`"
    );
    assert_eq!(first.value_at_quantile(0.50), EXPECTED_DWELL_MS);
}

#[test]
fn moonpool_send_latency_histogram_is_deterministic_across_identical_runs() {
    let base = Instant::now();

    let first = run_send_script(base);
    std::thread::sleep(HOST_DRIFT);
    let second = run_send_script(base);

    assert_eq!(
        recorded(&first),
        recorded(&second),
        "send_latency_hist moved with the HOST clock across two identical runs — \
         the Instant::elapsed() leak is back"
    );
    assert_eq!(
        first, second,
        "histograms differ beyond their recorded values"
    );

    assert_eq!(first.len(), N, "one sample per applied CommandSendReceipt");
    assert_eq!(
        first.max(),
        EXPECTED_RTT_MS,
        "sample must be `now - enqueued_at`"
    );
    assert_eq!(first.value_at_quantile(0.50), EXPECTED_RTT_MS);
}

/// A pinned provider means a pinned sample: with `now_instant()` frozen at
/// the delivery instant, a real host sleep between deliver and pop must still
/// record 0 ms. Pre-fix this recorded the sleep duration instead.
#[test]
fn moonpool_latency_histogram_ignores_host_clock_under_pinned_provider() {
    let base = Instant::now();
    let provider: Arc<dyn Fn() -> Instant + Send + Sync> = Arc::new(move || base);
    let shared = ConnectionShared::with_auth_wall_clock_and_instant(
        ConnectionConfig::default(),
        None,
        DETERMINISTIC_SIM_EPOCH_MS,
        provider,
    );

    let mut conn = shared.inner.lock();
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(base, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    let handle = subscribe_and_ack(&mut conn, base);
    deliver_message(&mut conn, base, handle, 0);

    // The host clock genuinely advances; the injected provider does not.
    std::thread::sleep(HOST_DRIFT);
    conn.pop_message(handle, shared.now_instant())
        .expect("queued message");

    let hist = conn
        .consumer(handle)
        .expect("consumer slot registered")
        .state
        .lock()
        .receive_latency_histogram()
        .expect("receive_latency_hist initialised");
    assert_eq!(hist.len(), 1);
    assert_eq!(
        hist.max(),
        0,
        "a frozen injected clock must record 0 ms, not the host sleep"
    );
}
