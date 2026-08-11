// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d): tokio ↔ moonpool `EventStream` parity for issue #860 — a consumer over a
//! topic whose batched entries are COMPRESSED.
//!
//! ## What the reporter saw
//!
//! A magnetar `Reader` with `receiver_queue_size(1000)` over a topic a Java producer filled with
//! `pulsar-perf produce -bm 20 -z LZ4` delivered nothing, ever, with nothing logged; the broker
//! reported the reader's `availablePermits` at `-1000`. Uncompressed batches streamed fine.
//!
//! ## Why
//!
//! A Java producer compresses the WHOLE packed batch body, and `ConsumerState::deliver` split
//! the body before decompressing it. The compressed bytes were read as member sizes, a length
//! guard fired, no member was queued, so none was ever popped — and the `numMessagesInBatch`
//! permits the broker charged for the entry were never refunded. Once the loss passed half the
//! receiver queue, `maybe_flow` was unreachable for good.
//!
//! ## The desired behaviour this asserts
//!
//! The scripted broker charges one permit per MESSAGE of a batched entry and stops dispatching at
//! zero, so a consumer that loses a window's worth of members stalls here exactly as it does
//! against a real broker. The trace publishes every codec in both layouts a compressed batch can
//! arrive in — the Java one, and the one magnetar's producer wrote up to 1.7.2 — with an entry no
//! layout decodes in the middle, and then receives exactly one message per decodable member:
//!
//! 1. both engines receive every member of every decodable entry, in plaintext, in publish order;
//! 2. the undecodable entry is skipped and refunded rather than wedging everything behind it;
//! 3. no receive times out.
//!
//! All three are RED against the client before ADR-0112: the very first entry is a Java-layout
//! batch, nothing of it surfaces, the window is spent, and every receive behind it times out.
//! The proto/runtime half of the claim is pinned by the sibling
//! `crates/magnetar-runtime-{tokio,moonpool}/tests/compressed_batch_flow.rs`; this file is the
//! end-to-end statement of it, across both engines, over a real socket.

use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{BatchLayout, Event, Op, Trace, runner_moonpool, runner_tokio};
use magnetar_proto::types::CompressionKind;

/// Messages packed into each published entry, and the consumer's receiver queue: one entry
/// spends the whole window, so a lost entry is a lost window.
const BATCH_SIZE: usize = 8;

/// Receive budget. Generous — every receive of a healthy run resolves at once.
const RECV_TIMEOUT: Duration = Duration::from_secs(2);

const TOPIC: &str = "persistent://public/default/compressed-batch-860";
const SUBSCRIPTION: &str = "sub-compressed-batch-860";
const CONSUMER: &str = "reader";

/// The entries published, in order: every codec in the Java layout, then in the legacy layout,
/// with one undecodable entry right behind the first.
fn entries() -> Vec<(CompressionKind, BatchLayout)> {
    let codecs = [
        CompressionKind::Lz4,
        CompressionKind::Zlib,
        CompressionKind::Zstd,
        CompressionKind::Snappy,
    ];
    let mut entries: Vec<(CompressionKind, BatchLayout)> =
        codecs.iter().map(|c| (*c, BatchLayout::Java)).collect();
    entries.insert(1, (CompressionKind::Lz4, BatchLayout::Undecodable));
    entries.extend(codecs.iter().map(|c| (*c, BatchLayout::Legacy)));
    entries
}

/// Payloads of entry `entry`: distinct per entry and per position, compressible.
fn payloads(entry: usize) -> Vec<Vec<u8>> {
    (0..BATCH_SIZE)
        .map(|i| format!("entry-{entry}-member-{i}|").repeat(8).into_bytes())
        .collect()
}

/// Two one-member batches published behind [`entries`]: `num_messages_in_batch = Some(1)` is a
/// batch of one (Java reads it as a batch; `flush_batch` stamps it on a one-message flush), so
/// its member must arrive without the `[u32][SingleMessageMetadata]` framing.
fn one_member_payloads() -> [Vec<u8>; 2] {
    [
        b"one-member-uncompressed|".repeat(4),
        b"one-member-zstd|".repeat(4),
    ]
}

fn trace() -> Trace {
    let mut ops: Vec<Op> = entries()
        .into_iter()
        .enumerate()
        .map(|(entry, (codec, layout))| Op::SendCompressedBatch {
            payloads: payloads(entry),
            codec,
            layout,
        })
        .collect();
    let [uncompressed, zstd] = one_member_payloads();
    ops.push(Op::SendBatch {
        payloads: vec![uncompressed],
    });
    ops.push(Op::SendCompressedBatch {
        payloads: vec![zstd],
        codec: CompressionKind::Zstd,
        layout: BatchLayout::Java,
    });
    ops.push(Op::OpenSharedConsumer {
        name: CONSUMER.to_owned(),
        receiver_queue_size: BATCH_SIZE,
        max_redeliver_count: 0,
    });
    let decodable = entries()
        .iter()
        .filter(|(_, layout)| *layout != BatchLayout::Undecodable)
        .count();
    for _ in 0..decodable * BATCH_SIZE + one_member_payloads().len() {
        ops.push(Op::RecvShared {
            name: CONSUMER.to_owned(),
            timeout: RECV_TIMEOUT,
        });
    }
    ops.push(Op::Close);
    Trace::new(TOPIC, SUBSCRIPTION, ops)
}

#[tokio::test(flavor = "current_thread")]
async fn compressed_batch_flow_event_streams_agree() {
    let trace = trace();

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let tokio_stream = runner_tokio::run(&broker.pulsar_url(), &trace)
        .await
        .expect("tokio runner");
    broker.clear_frame_log();
    let moonpool_stream = runner_moonpool::run(&broker.host_port(), &trace)
        .await
        .expect("moonpool runner");

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged over compressed batched entries"
    );

    let received: Vec<Vec<u8>> = tokio_stream
        .events
        .iter()
        .filter_map(|event| match event {
            Event::Received { payload, .. } => Some(payload.clone()),
            _ => None,
        })
        .collect();
    let expected: Vec<Vec<u8>> = entries()
        .into_iter()
        .enumerate()
        .filter(|(_, (_, layout))| *layout != BatchLayout::Undecodable)
        .flat_map(|(entry, _)| payloads(entry))
        .chain(one_member_payloads())
        .collect();
    assert_eq!(
        received.len(),
        expected.len(),
        "every member of every decodable entry must reach the application; the stream was {:?}",
        tokio_stream.events
    );
    assert_eq!(
        received, expected,
        "members must arrive in plaintext and in publish order, with the undecodable entry \
         skipped rather than wedging the consumer"
    );
    assert!(
        !tokio_stream
            .events
            .iter()
            .any(|event| matches!(event, Event::RecvTimeout)),
        "no receive may time out: a lost batch must refund its permits"
    );
}

/// Payloads for the send-order trace: three 400-byte ones that overflow a 1000-byte batch on
/// the third, then one too large for any batch.
fn ordered_payloads() -> Vec<Vec<u8>> {
    let mut payloads: Vec<Vec<u8>> = (0..3u8).map(|i| vec![b'a' + i; 400]).collect();
    payloads.push(vec![b'z'; 1200]);
    payloads
}

/// Issue #860 review: a send that does not fit the pending batch must not overtake it. Before
/// the fix the third payload went out on its own while the first two still sat in the batch,
/// and the oversized fourth followed it, so the broker stored — and the consumer received —
/// `2, 3, 0, 1`. With broker deduplication on, the lower sequence ids arriving second would
/// have been dropped instead.
#[tokio::test(flavor = "current_thread")]
async fn batching_producer_keeps_send_order_event_streams_agree() {
    let payloads = ordered_payloads();
    let mut ops = vec![
        Op::SendThroughBatchingProducer {
            payloads: payloads.clone(),
            max_batch_bytes: 1000,
        },
        Op::OpenSharedConsumer {
            name: CONSUMER.to_owned(),
            receiver_queue_size: BATCH_SIZE,
            max_redeliver_count: 0,
        },
    ];
    ops.extend(payloads.iter().map(|_| Op::RecvShared {
        name: CONSUMER.to_owned(),
        timeout: RECV_TIMEOUT,
    }));
    ops.push(Op::Close);
    let trace = Trace::new(
        "persistent://public/default/batch-order-860",
        "sub-batch-order-860",
        ops,
    );

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let tokio_stream = runner_tokio::run(&broker.pulsar_url(), &trace)
        .await
        .expect("tokio runner");
    broker.clear_frame_log();
    let moonpool_stream = runner_moonpool::run(&broker.host_port(), &trace)
        .await
        .expect("moonpool runner");
    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged over a batching producer's send order"
    );
    assert!(
        matches!(&tokio_stream.events[0], Event::SentAll { outcomes } if outcomes.iter().all(Result::is_ok)),
        "every send must succeed, got {:?}",
        tokio_stream.events[0]
    );
    let received: Vec<Vec<u8>> = tokio_stream
        .events
        .iter()
        .filter_map(|event| match event {
            Event::Received { payload, .. } => Some(payload.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        received.iter().map(|p| (p[0], p.len())).collect::<Vec<_>>(),
        payloads.iter().map(|p| (p[0], p.len())).collect::<Vec<_>>(),
        "the consumer must receive the payloads in send order"
    );
}
