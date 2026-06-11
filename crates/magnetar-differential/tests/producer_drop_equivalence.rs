// SPDX-License-Identifier: Apache-2.0

//! Last-clone drop guard — tokio ↔ moonpool differential equivalence
//! (issue #241). Layer (d) of the ADR-0024 four-layer test policy.
//!
//! Dropping every clone of a producer WITHOUT an explicit
//! `close().await` must behave identically on both engines:
//!
//! 1. the user-visible [`EventStream`]s agree (the post-drop send collapses to `SendError { kind:
//!    "producer-dropped" }` on both legs),
//! 2. each leg pushes exactly one best-effort `CommandCloseProducer` to the broker — observable on
//!    the scripted broker's frame log.
//!
//! The `Recv` op after the drop doubles as an ordering barrier: its
//! `Subscribe` round-trip rides the same connection (FIFO), so once it
//! resolves the earlier-enqueued `CloseProducer` has necessarily
//! reached the broker — no sleep-and-hope polling.

use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Event, Op, Trace, runner_moonpool, runner_tokio};
use magnetar_proto::pb;

fn close_producer_count(log: &[i32]) -> usize {
    log.iter()
        .filter(|t| **t == pb::base_command::Type::CloseProducer as i32)
        .count()
}

#[tokio::test(flavor = "current_thread")]
async fn producer_drop_event_stream_parity_and_single_close() {
    let trace = Trace::new(
        "persistent://public/default/producer-drop-equiv",
        "sub-drop",
        vec![
            Op::Send {
                payload: b"before-drop".to_vec(),
            },
            Op::DropProducer,
            Op::Send {
                payload: b"after-drop".to_vec(),
            },
            // Ordering barrier: the Subscribe round-trip lands after the
            // drop guard's CloseProducer on the same connection.
            Op::Recv {
                timeout: Duration::from_millis(500),
            },
        ],
    );

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_stream = runner_tokio::run(&pulsar_url, &trace)
        .await
        .expect("tokio runner");
    let tokio_frames = broker.frame_log_snapshot();
    broker.clear_frame_log();

    let moonpool_stream = runner_moonpool::run(&host_port, &trace)
        .await
        .expect("moonpool runner");
    let moonpool_frames = broker.frame_log_snapshot();

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the producer-drop sequence",
    );
    assert!(
        matches!(tokio_stream.events[1], Event::ProducerDropped),
        "op 1 must resolve to ProducerDropped, got {:?}",
        tokio_stream.events[1]
    );
    assert_eq!(
        tokio_stream.events[2],
        Event::SendError {
            kind: "producer-dropped".to_owned()
        },
        "send after drop must collapse to the producer-dropped bucket",
    );

    assert_eq!(
        close_producer_count(&tokio_frames),
        1,
        "tokio leg: drop guard must push exactly one CloseProducer, frames: {tokio_frames:?}",
    );
    assert_eq!(
        close_producer_count(&moonpool_frames),
        1,
        "moonpool leg: drop guard must push exactly one CloseProducer, frames: {moonpool_frames:?}",
    );

    // The EventStream equality above proves the two *runners* agree; the
    // engine drop-guards are compared on the wire. The frame log is the
    // ordered sequence of every command kind the broker received, so this
    // asserts the engines flush the guarded close at the same point
    // relative to the surrounding Producer / Send / Subscribe traffic —
    // an engine that deferred (or never flushed) its close on one leg
    // diverges here even when both event streams agree.
    assert_eq!(
        tokio_frames, moonpool_frames,
        "engine frame sequences diverged for the producer-drop trace",
    );

    broker.shutdown().await;
}
