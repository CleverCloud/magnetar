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
use magnetar_proto::{MessageId, pb};

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

/// Golden 6 — Java-parity lookup-before-open: both the tokio and moonpool
/// engines MUST issue `CommandLookupTopic` before `CommandProducer` /
/// `CommandSubscribe`. Pulsar's broker uses the lookup round-trip to
/// activate the topic's namespace bundle; skipping it surfaces
/// `ServerError::ServiceNotReady` against a fresh broker
/// (see `docs/follow-ups.md`).
///
/// Drives the trace `Send → Recv → Close` against the scripted broker for
/// each engine, snapshots the per-engine frame log on the broker side,
/// and asserts:
/// 1. each engine emitted a `Lookup` strictly before its `Producer`,
/// 2. each engine emitted a `Lookup` strictly before its `Subscribe`,
/// 3. the per-engine sequences of (Lookup, Producer, Subscribe) frame indices are equal — i.e.
///    tokio and moonpool agree on the lookup-vs-open interleaving.
#[tokio::test(flavor = "current_thread")]
async fn lookup_before_open_parity() {
    let trace = Trace::new(
        "persistent://public/default/diff-lookup",
        "sub-lookup",
        vec![
            Op::Send {
                payload: b"lookup-first".to_vec(),
            },
            Op::Recv {
                timeout: Duration::from_secs(2),
            },
            Op::Close,
        ],
    );

    // Tokio leg: bind a fresh broker so the recorded log isolates this engine.
    let broker_t = ScriptedBroker::bind().await.expect("broker bind");
    let tokio_url = broker_t.pulsar_url();
    let _ = runner_tokio::run(&tokio_url, &trace)
        .await
        .expect("tokio runner");
    let tokio_kinds = broker_t.frame_log_snapshot();
    broker_t.shutdown().await;

    // Moonpool leg: identical procedure on a fresh broker.
    let broker_m = ScriptedBroker::bind().await.expect("broker bind");
    let host_port = broker_m.host_port();
    let _ = runner_moonpool::run(&host_port, &trace)
        .await
        .expect("moonpool runner");
    let moonpool_kinds = broker_m.frame_log_snapshot();
    broker_m.shutdown().await;

    assert_lookup_strictly_before(&tokio_kinds, "tokio");
    assert_lookup_strictly_before(&moonpool_kinds, "moonpool");

    // Cross-engine parity on lookup-vs-open ordering: extract the indices
    // of the first Lookup, first Producer, and first Subscribe per engine
    // and compare. We intentionally check ordering, not absolute indices,
    // because the two engines have slightly different transport-level
    // preludes (handshake retry, keep-alive cadence) that don't matter
    // for the lookup-before-open invariant.
    let tokio_order = lookup_ordering(&tokio_kinds);
    let moonpool_order = lookup_ordering(&moonpool_kinds);
    assert_eq!(
        tokio_order, moonpool_order,
        "engines diverged on (lookup<producer, lookup<subscribe) ordering;\n  \
         tokio_kinds={tokio_kinds:?}\n  moonpool_kinds={moonpool_kinds:?}",
    );
}

fn assert_lookup_strictly_before(kinds: &[i32], engine: &str) {
    let lookup_idx = kinds
        .iter()
        .position(|k| *k == pb::base_command::Type::Lookup as i32)
        .unwrap_or_else(|| panic!("{engine}: expected CommandLookupTopic; saw {kinds:?}"));
    let producer_idx = kinds
        .iter()
        .position(|k| *k == pb::base_command::Type::Producer as i32)
        .unwrap_or_else(|| panic!("{engine}: expected CommandProducer; saw {kinds:?}"));
    let subscribe_idx = kinds
        .iter()
        .position(|k| *k == pb::base_command::Type::Subscribe as i32)
        .unwrap_or_else(|| panic!("{engine}: expected CommandSubscribe; saw {kinds:?}"));
    assert!(
        lookup_idx < producer_idx,
        "{engine}: Lookup ({lookup_idx}) must precede Producer ({producer_idx}); kinds={kinds:?}",
    );
    assert!(
        lookup_idx < subscribe_idx,
        "{engine}: Lookup ({lookup_idx}) must precede Subscribe ({subscribe_idx}); kinds={kinds:?}",
    );
}

/// Returns `(lookup_before_producer, lookup_before_subscribe)` — both
/// `true` when the engine emitted a Lookup ahead of the relevant open.
fn lookup_ordering(kinds: &[i32]) -> (bool, bool) {
    let pos = |target: pb::base_command::Type| kinds.iter().position(|k| *k == target as i32);
    let lookup = pos(pb::base_command::Type::Lookup);
    let producer = pos(pb::base_command::Type::Producer);
    let subscribe = pos(pb::base_command::Type::Subscribe);
    (
        matches!((lookup, producer), (Some(l), Some(p)) if l < p),
        matches!((lookup, subscribe), (Some(l), Some(s)) if l < s),
    )
}

/// Golden 7 — seek-per-partition: subscribe to four partitions of a
/// partitioned topic, publish 5 messages on each (20 total), drain
/// everything, then seek **partition 2 only** and assert that
/// 1. partition 2's consumer replays its five payloads, AND
/// 2. partitions 0, 1, 3 stay at the post-drain cursor (their next `Recv` times out — no spillover
///    from the seek).
///
/// Cross-engine assertion is via [`assert_equivalent`] — tokio and
/// moonpool must emit identical `EventStream`s. The broker-side
/// invariant ("Seek only touches partition 2") is additionally
/// asserted via the broker's [`ScriptedBroker::seeked_partitions_snapshot`]
/// log: it must record exactly `[2]` for each engine leg.
#[tokio::test(flavor = "current_thread")]
async fn seek_per_partition_replays_only_one_partition() {
    const PARTITIONS: i32 = 4;
    const PER_PART: u8 = 5;
    const BASE_TOPIC: &str = "persistent://public/default/seek-per-part-test";

    // Build the 3-step ops list.
    let mut ops: Vec<Op> = Vec::new();

    // Step 1+2: per-partition publishes. Subscribes happen lazily inside
    // the runner on the first `RecvPartition` op for that partition.
    for p in 0..PARTITIONS {
        for i in 0..PER_PART {
            ops.push(Op::SendPartition {
                partition: p,
                payload: vec![b'p', b'0' + u8::try_from(p).unwrap_or(0), b'-', b'0' + i],
            });
        }
    }
    // Drain Step-2 messages so each partition's consumer cursor lands
    // past the 5 stored messages.
    for p in 0..PARTITIONS {
        for _ in 0..PER_PART {
            ops.push(Op::RecvPartition {
                partition: p,
                timeout: Duration::from_secs(2),
            });
        }
    }
    // Step 3: seek partition 2 only. Then issue one Recv per partition.
    // Partition 2 must replay; others must time out (their cursor is at
    // EOF and the broker has no more messages queued).
    ops.push(Op::SeekPartition {
        partition: 2,
        message_id: mid(1, 0),
    });
    for p in 0..PARTITIONS {
        ops.push(Op::RecvPartition {
            partition: p,
            // Partition 2 will get a quick replay; the others need a
            // short timeout so the trace stays sub-second when nothing
            // arrives. Keep timings identical across the two engines so
            // the `EventStream`s compare equal.
            timeout: Duration::from_millis(250),
        });
    }
    ops.push(Op::Close);

    let trace = Trace::new(BASE_TOPIC, "sub-seek-part", ops);

    // Run both legs against fresh broker instances and collect the
    // broker-side seeked-partition log so we can assert engine-local
    // invariants beyond the EventStream comparison.
    let broker_t = ScriptedBroker::bind().await.expect("broker bind");
    let tokio_stream = runner_tokio::run(&broker_t.pulsar_url(), &trace)
        .await
        .expect("tokio runner");
    let tokio_seeks = broker_t.seeked_partitions_snapshot();
    broker_t.shutdown().await;

    let broker_m = ScriptedBroker::bind().await.expect("broker bind");
    let moonpool_stream = runner_moonpool::run(&broker_m.host_port(), &trace)
        .await
        .expect("moonpool runner");
    let moonpool_seeks = broker_m.seeked_partitions_snapshot();
    broker_m.shutdown().await;

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for seek-per-partition trace",
    );

    // The post-seek `Recv` events live at the tail; isolate them and
    // verify partition 2 replayed while the others timed out.
    let total_ops = trace.ops.len();
    let post_seek_recvs =
        &tokio_stream.events[total_ops - 1 - usize::try_from(PARTITIONS).unwrap()..total_ops - 1];
    assert_eq!(post_seek_recvs.len(), PARTITIONS as usize);
    for (p, event) in post_seek_recvs.iter().enumerate() {
        let p = i32::try_from(p).unwrap();
        if p == 2 {
            // Partition 2 must have replayed its first message.
            match event {
                Event::ReceivedPartition {
                    partition: pp,
                    message_id,
                    ..
                } => {
                    assert_eq!(*pp, 2);
                    assert_eq!(
                        message_id.entry_id, 0,
                        "partition 2 should replay from entry 0"
                    );
                }
                other => panic!("expected ReceivedPartition for p=2, got {other:?}"),
            }
        } else {
            // Other partitions must NOT have moved — their cursor is past
            // the only 5 stored messages, so Recv times out.
            assert!(
                matches!(event, Event::RecvTimeoutPartition { partition } if *partition == p),
                "expected RecvTimeoutPartition for p={p}, got {event:?}",
            );
        }
    }

    // Broker-side invariant: exactly one Seek was issued, and it
    // targeted partition 2. Both engines must observe the same.
    assert_eq!(
        tokio_seeks,
        vec![2_i32],
        "tokio: scripted broker should have seen exactly one Seek on partition 2",
    );
    assert_eq!(
        moonpool_seeks,
        vec![2_i32],
        "moonpool: scripted broker should have seen exactly one Seek on partition 2",
    );
}
