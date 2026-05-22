// SPDX-License-Identifier: Apache-2.0

//! Golden traces for the M8 differential equivalence harness.
//!
//! Each test:
//!
//! 1. Spins up a fresh [`ScriptedBroker`] on `127.0.0.1:0`.
//! 2. Runs the trace twice — once against the tokio engine, once against the moonpool engine.
//! 3. Asserts the two [`EventStream`]s match byte-for-byte.
//!
//! Traces are deliberately small. Add new ones as new Pulsar feature
//! coverage lands on both engines.

use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Event, Op, Trace, runner_moonpool, runner_tokio};
use magnetar_proto::MessageId;

/// Helper: build a message id with default partition/batch fields so
/// tests can spell ids tersely.
fn mid(ledger_id: u64, entry_id: u64) -> MessageId {
    MessageId {
        ledger_id,
        entry_id,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
    }
}

/// Run `trace` against both engines and assert their event streams
/// match. Returns the (shared) event stream when the assertion
/// succeeds, so caller-side spot-checks can poke specific events.
async fn assert_equivalent(trace: &Trace) -> magnetar_differential::EventStream {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_stream = runner_tokio::run(&pulsar_url, trace)
        .await
        .expect("tokio runner");
    let moonpool_stream = runner_moonpool::run(&host_port, trace)
        .await
        .expect("moonpool runner");

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for trace {trace:?}",
    );

    broker.shutdown().await;
    tokio_stream
}

/// Golden 1 — simple round-trip: open, send one message, receive it,
/// ack it, close.
#[tokio::test(flavor = "current_thread")]
async fn round_trip_single_message() {
    let trace = Trace::new(
        "persistent://public/default/diff-rt",
        "sub-rt",
        vec![
            Op::Send {
                payload: b"hello".to_vec(),
            },
            Op::Recv {
                timeout: Duration::from_secs(2),
            },
            // Note: the recv event will surface the broker-assigned
            // message id; we ack that exact id (via `Recv`-then-ack
            // sequencing the engine does internally would be tighter,
            // but for the harness we just ack the known broker id).
            Op::Ack {
                message_id: mid(1, 0),
            },
            Op::Close,
        ],
    );
    let stream = assert_equivalent(&trace).await;
    assert_eq!(stream.events.len(), 4);
    assert!(matches!(stream.events[0], Event::Sent { .. }));
    assert!(matches!(stream.events[1], Event::Received { .. }));
    assert!(matches!(stream.events[2], Event::Acked));
    assert!(matches!(stream.events[3], Event::Closed));
}

/// Golden 2 — batch send of 5 messages followed by 5 recvs and 5 acks.
/// Exercises the per-consumer flow window and the ack-response
/// round-trip under load.
#[tokio::test(flavor = "current_thread")]
async fn batch_send_then_recv_all() {
    let mut ops = Vec::new();
    for i in 0..5u8 {
        ops.push(Op::Send {
            payload: vec![b'a' + i],
        });
    }
    for _ in 0..5 {
        ops.push(Op::Recv {
            timeout: Duration::from_secs(2),
        });
    }
    for i in 0..5 {
        ops.push(Op::Ack {
            message_id: mid(1, i),
        });
    }
    ops.push(Op::Close);
    let trace = Trace::new("persistent://public/default/diff-batch", "sub-batch", ops);
    let stream = assert_equivalent(&trace).await;
    assert_eq!(stream.events.len(), 16);
    let sent = stream
        .events
        .iter()
        .filter(|e| matches!(e, Event::Sent { .. }))
        .count();
    let received = stream
        .events
        .iter()
        .filter(|e| matches!(e, Event::Received { .. }))
        .count();
    let acked = stream
        .events
        .iter()
        .filter(|e| matches!(e, Event::Acked))
        .count();
    assert_eq!(sent, 5);
    assert_eq!(received, 5);
    assert_eq!(acked, 5);
}

/// Golden 3 — nack and redelivery: send, recv, nack, recv again
/// (the broker re-pushes the nacked message), then ack and close.
#[tokio::test(flavor = "current_thread")]
async fn nack_then_redelivery() {
    let trace = Trace::new(
        "persistent://public/default/diff-nack",
        "sub-nack",
        vec![
            Op::Send {
                payload: b"redeliver-me".to_vec(),
            },
            Op::Recv {
                timeout: Duration::from_secs(2),
            },
            Op::Nack {
                message_id: mid(1, 0),
            },
            Op::Recv {
                timeout: Duration::from_secs(2),
            },
            Op::Ack {
                message_id: mid(1, 0),
            },
            Op::Close,
        ],
    );
    let stream = assert_equivalent(&trace).await;
    assert_eq!(stream.events.len(), 6);
    assert!(matches!(stream.events[2], Event::Nacked));
    // After the nack, the broker re-pushes the message; the second
    // Recv should observe the same payload.
    if let (Event::Received { payload: a, .. }, Event::Received { payload: b, .. }) =
        (&stream.events[1], &stream.events[3])
    {
        assert_eq!(a, b, "redelivered payload should match original");
    } else {
        panic!("expected two Received events around the Nack");
    }
}

/// Golden 4 — seek to start then replay: send 3 messages, recv all 3,
/// seek to the first one, recv all 3 again, ack everything, close.
#[tokio::test(flavor = "current_thread")]
async fn seek_to_start_then_replay() {
    let mut ops = Vec::new();
    for i in 0..3u8 {
        ops.push(Op::Send {
            payload: vec![b'x' + i],
        });
    }
    for _ in 0..3 {
        ops.push(Op::Recv {
            timeout: Duration::from_secs(2),
        });
    }
    // Seek to the first message (entry id 0). The broker resets the
    // consumer cursor and re-pushes from there.
    ops.push(Op::Seek {
        message_id: mid(1, 0),
    });
    for _ in 0..3 {
        ops.push(Op::Recv {
            timeout: Duration::from_secs(2),
        });
    }
    ops.push(Op::Close);
    let trace = Trace::new("persistent://public/default/diff-seek", "sub-seek", ops);
    let stream = assert_equivalent(&trace).await;
    assert_eq!(stream.events.len(), 11);
    assert!(matches!(stream.events[6], Event::Seeked));
    // Confirm the post-seek recv events match the pre-seek ones (the
    // broker pushes the same payload sequence in the same order).
    let pre: Vec<_> = (3..6)
        .map(|i| match &stream.events[i] {
            Event::Received { payload, .. } => payload.clone(),
            _ => panic!("expected Received at index {i}"),
        })
        .collect();
    let post: Vec<_> = (7..10)
        .map(|i| match &stream.events[i] {
            Event::Received { payload, .. } => payload.clone(),
            _ => panic!("expected Received at index {i}"),
        })
        .collect();
    assert_eq!(pre, post, "post-seek replay should match pre-seek sequence");
}

/// Golden 5 — many small publishes back-to-back, exercising the replay-frame
/// storage path on each publish (Stage 3 transparent in-flight publish replay landed
/// `OpSend::replay_frames` so reset → `rebuild_producers` can re-issue every unconfirmed
/// publish on the new session). This golden does NOT trigger a reset — the differential
/// harness has no public hook to do so today (see follow-ups), so the new replay branch
/// is exercised by the unit + integration tests in the proto, tokio, and moonpool crates.
/// What this golden DOES guarantee: the post-replay-frame-storage code path produces
/// byte-identical `EventStream`s across the tokio and moonpool engines. Catches the easy
/// regression where the new path diverges between engines without altering
/// user-visible semantics.
#[tokio::test(flavor = "current_thread")]
async fn many_publishes_round_trip() {
    let mut ops = Vec::new();
    for i in 0..7u8 {
        ops.push(Op::Send {
            payload: vec![b'm', b'-', b'0' + i],
        });
    }
    for _ in 0..7 {
        ops.push(Op::Recv {
            timeout: Duration::from_secs(2),
        });
    }
    for i in 0..7 {
        ops.push(Op::Ack {
            message_id: mid(1, i),
        });
    }
    ops.push(Op::Close);
    let trace = Trace::new(
        "persistent://public/default/diff-many-publishes",
        "sub-many",
        ops,
    );
    let stream = assert_equivalent(&trace).await;
    let sent = stream
        .events
        .iter()
        .filter(|e| matches!(e, Event::Sent { .. }))
        .count();
    let received = stream
        .events
        .iter()
        .filter(|e| matches!(e, Event::Received { .. }))
        .count();
    let acked = stream
        .events
        .iter()
        .filter(|e| matches!(e, Event::Acked))
        .count();
    assert_eq!(sent, 7);
    assert_eq!(received, 7);
    assert_eq!(acked, 7);
}
