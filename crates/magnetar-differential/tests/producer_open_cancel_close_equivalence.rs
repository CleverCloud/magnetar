// SPDX-License-Identifier: Apache-2.0

//! Close-before-retry on a cancelled producer open — tokio ↔ moonpool
//! differential equivalence (issue #406, ADR-0100). Layer (d) of the
//! ADR-0024 four-layer test policy.
//!
//! The scripted broker withholds the first `CommandProducerSuccess` for the
//! pinned name while keeping its `(topic, name)` registration, so the open
//! outlives `operation_timeout` and is abandoned client-side. Both engines
//! must then behave identically:
//!
//! 1. the user-visible [`EventStream`]s agree — the first open collapses to `timeout`, the second
//!    one under the SAME name succeeds,
//! 2. each leg pushes exactly one best-effort `CommandCloseProducer` for the abandoned producer id
//!    — observable on the scripted broker's frame log, and the only reason the second open is not
//!    rejected with `ProducerBusy`.
//!
//! The second open is its own ordering barrier: its `CommandProducer` rides
//! the same connection (FIFO) behind the cancellation's close, so a green
//! assertion needs no sleep-and-hope polling.

use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Event, Op, Trace, runner_moonpool, runner_tokio};
use magnetar_proto::pb;

/// Client-side budget for one open. Long enough for connect / lookup / open
/// on a loaded host, short enough that the withheld open gives up quickly.
const OPERATION_TIMEOUT: Duration = Duration::from_millis(750);

const PRODUCER_NAME: &str = "pinned-406";

fn close_producer_count(log: &[i32]) -> usize {
    log.iter()
        .filter(|t| **t == pb::base_command::Type::CloseProducer as i32)
        .count()
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_producer_open_frees_the_pinned_name_on_both_engines() {
    let trace = Trace::new(
        "persistent://public/default/producer-open-cancel-equiv",
        "sub-open-cancel",
        vec![
            // Withheld success → the open outlives the deadline and is
            // cancelled, which must reap the broker-side registration.
            Op::OpenNamedProducer {
                name: PRODUCER_NAME.to_owned(),
            },
            // Same name: only reachable because the cancellation closed it.
            // The handle is held for the rest of the trace.
            Op::OpenNamedProducer {
                name: PRODUCER_NAME.to_owned(),
            },
            // The name is now genuinely taken by a LIVE producer, so this one
            // is rejected with `ProducerBusy`. Two things ride on it: the
            // scripted broker really does enforce name exclusivity (without
            // which op 1 would pass vacuously), and a cancellation that
            // follows a broker rejection must emit NO close — the rejected
            // open holds no registration to reap.
            Op::OpenNamedProducer {
                name: PRODUCER_NAME.to_owned(),
            },
        ],
    );

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    broker.withhold_first_producer_success_for_name(PRODUCER_NAME);
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_stream =
        runner_tokio::run_with_operation_timeout(&pulsar_url, &trace, OPERATION_TIMEOUT)
            .await
            .expect("tokio runner");
    let tokio_frames = broker.frame_log_snapshot();
    broker.clear_frame_log();

    let moonpool_stream =
        runner_moonpool::run_with_operation_timeout(&host_port, &trace, OPERATION_TIMEOUT)
            .await
            .expect("moonpool runner");
    let moonpool_frames = broker.frame_log_snapshot();

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the cancelled-producer-open sequence",
    );
    assert_eq!(
        tokio_stream.events[0],
        Event::NamedProducerOpenError {
            kind: "timeout".to_owned()
        },
        "op 0 must exhaust the operation deadline on the withheld ProducerSuccess",
    );
    assert_eq!(
        tokio_stream.events[1],
        Event::NamedProducerOpened,
        "op 1 must reopen the pinned name the cancelled open released",
    );
    assert_eq!(
        tokio_stream.events[2],
        Event::NamedProducerOpenError {
            kind: format!("broker:{}", pb::ServerError::ProducerBusy as i32)
        },
        "op 2 must be rejected: op 1's live producer holds the name",
    );

    assert_eq!(
        close_producer_count(&tokio_frames),
        1,
        "tokio leg: exactly one CloseProducer — the cancelled open's, never the rejected open's, frames: {tokio_frames:?}",
    );
    assert_eq!(
        close_producer_count(&moonpool_frames),
        1,
        "moonpool leg: exactly one CloseProducer — the cancelled open's, never the rejected open's, frames: {moonpool_frames:?}",
    );

    // The EventStream equality above proves the two *runners* agree; the
    // engines' cancellation paths are compared on the wire. The frame log is
    // the ordered sequence of every command kind the broker received, so this
    // asserts both engines flush the reaping close at the same point relative
    // to the surrounding Lookup / Producer traffic — an engine that deferred
    // (or never flushed) its close on one leg diverges here even when both
    // event streams agree.
    assert_eq!(
        tokio_frames, moonpool_frames,
        "engine frame sequences diverged for the cancelled-producer-open trace",
    );

    broker.shutdown().await;
}
