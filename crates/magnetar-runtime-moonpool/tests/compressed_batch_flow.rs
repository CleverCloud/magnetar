// SPDX-License-Identifier: Apache-2.0

// Each scenario is one readable step-by-step synthetic frame sequence. Splitting them into
// sub-helpers would hide the exact ordering the test pins. We accept the line count.
#![allow(clippy::too_many_lines)]
#![allow(clippy::expect_used)]

//! Issue #860: a magnetar `Reader` over a topic a Java producer filled with COMPRESSED batches
//! delivers nothing and stalls for good, the broker's `availablePermits` sitting at
//! `-receiver_queue_size`, with nothing logged. Measured on `apachepulsar/pulsar:4.0.4`:
//! `pulsar-perf produce -bm 20 -z LZ4` in, zero messages out of a `receiver_queue_size(1000)`
//! Reader. The mirror fault ran the other way: Java `pulsar-client consume` read 0/100 of a
//! magnetar LZ4 batched topic.
//!
//! ## The mechanism these tests pin
//!
//! A Java producer compresses the WHOLE packed batch body
//! (`BatchMessageContainerImpl.getCompressedBatchMetadataAndPayload`), and a Java consumer
//! decompresses it before it splits it (`ConsumerImpl.uncompressPayloadIfNeeded`, then
//! `receiveIndividualMessagesFromBatch`). `ConsumerState::deliver` split first: the compressed
//! bytes were read as member sizes, a length guard fired, and no member was queued — so none
//! was ever popped, the broker's `numMessagesInBatch` permits were never refunded, and once the
//! loss passed half the receiver queue `maybe_flow` could never fire again (ADR-0112).
//!
//! 1. A Java-layout batch surfaces every member in plaintext and re-arms flow at half the queue.
//! 2. The layout magnetar's producer wrote up to 1.7.2 — every member compressed on its own, the
//!    body raw — is now readable. It was not before: the post-pop decompression checked each member
//!    against the batch-level `uncompressed_size`, the SUM of the compressed sizes.
//! 3. An entry no layout decodes is debited AND refunded member by member, so flow continues.
//! 4. The producer emits the Java layout, and that frame round-trips through the consumer.
//!
//! Every inbound body is packed and compressed by hand, never by magnetar's own producer, so a
//! consumer test cannot pass because both ends share a mistake. The frames run through the
//! engine's `ConnectionShared` with synthetic [`Instant`]s — no listener, no wall clock, no
//! Docker; the mirrored `magnetar-runtime-tokio` file pins the identical behaviour against
//! the production engine, keeping the runtime 1:1 test count (ADR-0024).

mod common;

use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::{Buf, Bytes, BytesMut};
use magnetar_proto::compress::{compress, decompress, decompress_within};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::types::CompressionKind;
use magnetar_proto::{
    ConnectionConfig, ConsumerHandle, CreateProducerRequest, SubscribeRequest, decode_one,
    encode_command, encode_payload, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

/// Receiver queue every consumer opens with. `maybe_flow` re-arms at `RQ / 2` = 4.
const RQ: usize = 8;

/// The four Pulsar codecs.
const ALL_CODECS: [CompressionKind; 4] = [
    CompressionKind::Lz4,
    CompressionKind::Zlib,
    CompressionKind::Zstd,
    CompressionKind::Snappy,
];

/// The magnetar frame ceiling every decompression is bounded by (`MAX_FRAME_SIZE`, 5 MiB).
const CEILING: usize = magnetar_proto::MAX_FRAME_SIZE;

/// In-memory sink for a capturing `fmt` subscriber (the `crates/magnetar-proto/tests/
/// log_capture.rs` pattern): proto logs synchronously on the caller's thread, so a thread-local
/// `tracing::subscriber::with_default` sees every event the scenario emits.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Capture {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("capture sink")).into_owned()
    }
}

impl std::io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("capture sink").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// `n` distinct, compressible payloads, tagged so a mixed-up member is visible.
fn payloads(tag: &str, n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("{tag}-{i}|").repeat(12).into_bytes())
        .collect()
}

/// Pack `members` as `[u32 BE size][SingleMessageMetadata][payload]` — the layout
/// `BatchMessageContainerImpl` writes.
fn pack(members: &[&[u8]]) -> Bytes {
    let mut body = BytesMut::new();
    for member in members {
        let single = pb::SingleMessageMetadata {
            payload_size: member.len() as i32,
            ..Default::default()
        };
        let single_len = prost::Message::encoded_len(&single);
        body.extend_from_slice(&(single_len as u32).to_be_bytes());
        prost::Message::encode(&single, &mut body).expect("encode SingleMessageMetadata");
        body.extend_from_slice(member);
    }
    body.freeze()
}

/// Split a packed body back into its payloads, the way `receiveIndividualMessagesFromBatch`
/// does.
fn unpack(mut body: Bytes) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while body.has_remaining() {
        let size = body.get_u32() as usize;
        let single: pb::SingleMessageMetadata =
            prost::Message::decode(body.split_to(size)).expect("decode SingleMessageMetadata");
        out.push(body.split_to(single.payload_size as usize).to_vec());
    }
    out
}

/// A batched entry's body under its batch-level `(compression, uncompressed_size)` stamps.
type Layout = (Option<i32>, u32, Bytes);

/// Java layout: the packed body compressed ONCE; `uncompressed_size` = the packed length.
fn java_layout(codec: CompressionKind, members: &[Vec<u8>]) -> Layout {
    let refs: Vec<&[u8]> = members.iter().map(Vec::as_slice).collect();
    let packed = pack(&refs);
    let body = compress(codec, &packed).expect("compress packed body");
    (Some(codec.to_pb() as i32), packed.len() as u32, body)
}

/// magnetar ≤ 1.7.2 layout: every member compressed on its own, the packed body raw, and
/// `uncompressed_size` = the sum of the COMPRESSED member sizes.
fn legacy_layout(codec: CompressionKind, members: &[Vec<u8>]) -> Layout {
    let compressed: Vec<Bytes> = members
        .iter()
        .map(|m| compress(codec, m).expect("compress member"))
        .collect();
    let refs: Vec<&[u8]> = compressed.iter().map(Bytes::as_ref).collect();
    let total: usize = compressed.iter().map(Bytes::len).sum();
    (Some(codec.to_pb() as i32), total as u32, pack(&refs))
}

/// Handshake, subscribe a `Shared` consumer with `RQ` permits, ack the subscribe and grant the
/// initial flow. Drains the outbound buffer so later assertions see only scenario frames.
fn open_consumer(shared: &ConnectionShared, topic: &str, at: Instant) -> ConsumerHandle {
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(at, &handshake_response_bytes())
            .expect("Connected");
        while conn.poll_event().is_some() {}
    }
    let (handle, request_id) = {
        let mut conn = shared.inner.lock();
        let request_id = conn.peek_next_request_id_for_test();
        let handle = conn.subscribe(SubscribeRequest {
            topic: topic.to_owned(),
            subscription: "magnetar-test-860".to_owned(),
            sub_type: pb::command_subscribe::SubType::Shared,
            receiver_queue_size: RQ,
            ..Default::default()
        });
        (handle, request_id)
    };
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
    let mut conn = shared.inner.lock();
    conn.handle_bytes(at, &buf).expect("Success");
    while conn.poll_event().is_some() {}
    conn.initial_flow(handle, at);
    let _ = conn.poll_transmit();
    handle
}

/// One batched broker entry on `(1, entry)` declaring `num` members, with the given batch-level
/// `compression` / `uncompressed_size` stamps and `ack_set`.
fn entry_frame(
    handle: ConsumerHandle,
    entry: u64,
    num: i32,
    (compression, uncompressed_size, body): Layout,
    ack_set: Vec<i64>,
) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: pb::MessageIdData {
                ledger_id: 1,
                entry_id: entry,
                ..Default::default()
            },
            redelivery_count: Some(0),
            ack_set,
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let metadata = pb::MessageMetadata {
        producer_name: "java-producer".to_owned(),
        sequence_id: entry,
        publish_time: 0,
        num_messages_in_batch: Some(num),
        compression,
        uncompressed_size: Some(uncompressed_size),
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &cmd, &metadata, &body).expect("encode batched entry");
    frame
}

/// Every `CommandFlow` grant on the outbound buffer, in order.
fn drain_flow_permits(out: &mut Bytes) -> Vec<u32> {
    let mut grants = Vec::new();
    while !out.is_empty() {
        let Ok(frame) = decode_one(out) else { break };
        if let Some(flow) = frame.command.flow {
            grants.push(flow.message_permits);
        }
    }
    grants
}

/// Deliver `frame`, then pop everything it queued. Returns the popped payloads and every flow
/// grant emitted from delivery onwards; asserts no surfaced member is still marked compressed.
fn deliver_and_drain(
    shared: &ConnectionShared,
    handle: ConsumerHandle,
    frame: &BytesMut,
    at: Instant,
) -> (Vec<Vec<u8>>, Vec<u32>) {
    let mut conn = shared.inner.lock();
    conn.handle_bytes(at, frame).expect("deliver entry");
    while conn.poll_event().is_some() {}
    let mut grants = drain_flow_permits(&mut conn.poll_transmit());
    let mut popped = Vec::new();
    while let Some(msg) = conn.pop_message(handle, at) {
        assert_eq!(
            msg.metadata.compression, None,
            "a member surfaced from a decoded batch must not be decompressed a second time"
        );
        popped.push(msg.payload.to_vec());
        grants.extend(drain_flow_permits(&mut conn.poll_transmit()));
    }
    (popped, grants)
}

/// The issue #860 stall itself: a Java-layout compressed batch must surface every member and
/// re-arm flow after half the receiver queue is popped. Before ADR-0112 the split ran over the
/// compressed bytes, nothing was queued, nothing could be popped, and no flow ever followed.
#[test]
fn java_layout_compressed_batch_reflows_after_half_the_queue() {
    let at = Instant::now();
    for (entry, codec) in (1u64..).zip(ALL_CODECS) {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let handle = open_consumer(&shared, "persistent://public/default/860-java", at);
        let members = payloads(&format!("java-{codec:?}"), RQ);
        let frame = entry_frame(
            handle,
            entry,
            RQ as i32,
            java_layout(codec, &members),
            Vec::new(),
        );
        let (popped, grants) = deliver_and_drain(&shared, handle, &frame, at);
        assert_eq!(
            popped, members,
            "{codec:?}: every member of a Java-layout batch, in plaintext, in order"
        );
        assert_eq!(
            grants,
            vec![4, 4],
            "{codec:?}: popping the window re-arms flow at each half-queue boundary"
        );
    }
}

/// The layout magnetar's own producer wrote up to 1.7.2 is readable: each member decoded on its
/// own. Nothing read it before — the post-pop decompression checked every member against the
/// batch-level `uncompressed_size`, which held the sum of the compressed sizes.
#[test]
fn legacy_layout_compressed_batch_is_readable() {
    // The legacy decoder's own contract, before any frame: with no exact size on the wire it
    // passes `None` through, and it REFUSES — never truncates — output past its limit.
    assert_eq!(
        decompress_within(CompressionKind::None, b"raw", 3)
            .expect("pass-through")
            .as_ref(),
        b"raw"
    );
    assert!(decompress_within(CompressionKind::None, b"raw", 2).is_err());
    // LZ4 sizes its output buffer from the limit: a block that inflates past it must be an
    // `Err`, never a panic and never a truncated payload (invariant #6).
    let lz4_block = compress(CompressionKind::Lz4, &[0u8; 8192]).expect("lz4");
    assert!(decompress_within(CompressionKind::Lz4, &lz4_block, 64).is_err());
    let at = Instant::now();
    for (entry, codec) in (1u64..).zip(ALL_CODECS) {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let handle = open_consumer(&shared, "persistent://public/default/860-legacy", at);
        let members = payloads(&format!("legacy-{codec:?}"), 4);
        let frame = entry_frame(handle, entry, 4, legacy_layout(codec, &members), Vec::new());
        let mut conn = shared.inner.lock();
        conn.handle_bytes(at, &frame).expect("deliver entry");
        while conn.poll_event().is_some() {}
        let mut popped = Vec::new();
        while let Some(msg) = conn.pop_message(handle, at) {
            popped.push(msg.payload.to_vec());
            // ADR-0112 review: a queued member keeps only its decoded bytes alive, never the
            // headroom the decoder sized its buffer with (up to 255 x the member for LZ4).
            let len = msg.payload.len();
            let held = msg
                .payload
                .try_into_mut()
                .expect("a decoded legacy member owns its buffer")
                .capacity();
            assert_eq!(held, len, "{codec:?}: retained capacity of a legacy member");
        }
        assert_eq!(
            popped, members,
            "{codec:?}: legacy members are decoded one by one"
        );
    }
}

/// `num_messages_in_batch = Some(1)` is a batch of ONE — Java takes the single-message path only
/// when the field is absent (`ConsumerImpl.messageReceived`) — and it is exactly what
/// `flush_batch` stamps on a one-message flush. Its member must reach the application without
/// the `[u32][SingleMessageMetadata]` framing, uncompressed, Java-compressed, or written by
/// magnetar's own batching producer.
#[test]
fn one_member_batch_is_unframed() {
    let at = Instant::now();
    let only = payloads("only", 1);
    let packed = pack(&[only[0].as_slice()]);
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let consumer = open_consumer(&shared, "persistent://public/default/860-one", at);
    let producer = shared.inner.lock().create_producer(CreateProducerRequest {
        topic: "persistent://public/default/860-one".to_owned(),
        compression: CompressionKind::Zstd,
        enable_batching: true,
        ..Default::default()
    });
    let slot = shared
        .inner
        .lock()
        .producer(producer)
        .cloned()
        .expect("producer slot");
    slot.queue_send(outgoing(only[0].clone()), 1_700_000_000_000, at)
        .expect("batched send");
    assert_eq!(
        slot.flush_batch(1_700_000_000_000, at),
        1,
        "a one-message flush"
    );
    let flushed = slot.state.lock().next_outbound_frame().expect("one frame");
    assert_eq!(flushed.metadata.num_messages_in_batch, Some(1));
    let layouts: Vec<Layout> = vec![
        (None, packed.len() as u32, packed.clone()),
        java_layout(CompressionKind::Lz4, &only),
        (
            flushed.metadata.compression,
            flushed
                .metadata
                .uncompressed_size
                .expect("uncompressed_size"),
            flushed.payload.clone(),
        ),
    ];
    for (entry, layout) in (1u64..).zip(layouts) {
        let frame = entry_frame(consumer, entry, 1, layout, Vec::new());
        let (popped, _) = deliver_and_drain(&shared, consumer, &frame, at);
        assert_eq!(popped, only, "entry {entry}: the member, unframed");
    }
}

/// A send that does not fit the pending batch flushes it first (Java
/// `ProducerImpl.doBatchSendAndAdd`), so it never overtakes the batch on the wire — with broker
/// deduplication a lower sequence id arriving second is dropped. The batch byte budget counts
/// uncompressed bytes since ADR-0112, which made the overflow far more frequent.
#[test]
fn overflowing_send_does_not_overtake_the_pending_batch() {
    let at = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(at, &handshake_response_bytes())
            .expect("Connected");
    }
    let producer = shared.inner.lock().create_producer(CreateProducerRequest {
        topic: "persistent://public/default/860-order".to_owned(),
        compression: CompressionKind::Lz4,
        enable_batching: true,
        max_batch_size_bytes: 1000,
        ..Default::default()
    });
    let slot = shared
        .inner
        .lock()
        .producer(producer)
        .cloned()
        .expect("producer slot");
    for i in 0..3u8 {
        slot.queue_send(outgoing(vec![i; 400]), 1_700_000_000_000, at)
            .expect("send");
    }
    slot.queue_send(outgoing(vec![3; 1200]), 1_700_000_000_000, at)
        .expect("oversized send");
    let mut order = Vec::new();
    while let Some(frame) = slot.state.lock().next_outbound_frame() {
        order.push((
            frame.sequence_id.0,
            frame.metadata.num_messages_in_batch.unwrap_or(0),
        ));
    }
    assert_eq!(
        order,
        vec![(0, 2), (2, 1), (3, 0)],
        "frames leave in send order: the full batch, the batch the overflow started, then the \
         message too large for any batch"
    );
}

/// An entry no layout decodes surfaces nothing and is debited AND refunded per charged member,
/// so the refund alone re-arms flow. Covers garbage under a codec stamp, an unknown codec, an
/// `uncompressed_size` above the frame ceiling (refused before any codec allocates), a Zstd
/// decompression bomb in the Java layout, Snappy and Zstd bombs in the legacy layout (one
/// announcing, one streaming past the ceiling), and an ADR-0105 re-dispatch whose cleared
/// positions the broker never charged. Each such entry logs exactly one structured `warn!`.
#[test]
fn undecodable_compressed_batch_refunds_its_permits() {
    let capture = Capture::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(capture.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let at = Instant::now();
        let members = payloads("bad", RQ);
        let lz4 = Some(CompressionKind::Lz4.to_pb() as i32);
        let bomb = vec![0u8; CEILING + 1024];
        // A legacy entry whose FIRST member inflates past the ceiling; the other seven are valid.
        let legacy_bomb = |codec: CompressionKind| {
            let (stamp, size, _) = legacy_layout(codec, &members);
            let mut compressed = vec![compress(codec, &bomb).expect("bomb member")];
            compressed.extend(
                members[1..]
                    .iter()
                    .map(|m| compress(codec, m).expect("member")),
            );
            let refs: Vec<&[u8]> = compressed.iter().map(Bytes::as_ref).collect();
            (stamp, size, pack(&refs))
        };
        let cases: Vec<(&str, i32, Layout)> = vec![
            (
                "garbage under an LZ4 stamp",
                RQ as i32,
                (lz4, 512, Bytes::from(vec![0xA5u8; 96])),
            ),
            ("unknown codec", RQ as i32, {
                let (_, size, body) = java_layout(CompressionKind::Lz4, &members);
                (Some(99), size, body)
            }),
            ("uncompressed_size above the ceiling", RQ as i32, {
                let (codec, _, body) = java_layout(CompressionKind::Zlib, &members);
                (codec, u32::MAX, body)
            }),
            (
                "Java-layout Zstd bomb",
                RQ as i32,
                (
                    Some(CompressionKind::Zstd.to_pb() as i32),
                    1,
                    compress(CompressionKind::Zstd, &bomb).expect("zstd bomb"),
                ),
            ),
            (
                "legacy Snappy bomb",
                RQ as i32,
                legacy_bomb(CompressionKind::Snappy),
            ),
            (
                "legacy Zstd bomb",
                RQ as i32,
                legacy_bomb(CompressionKind::Zstd),
            ),
        ];
        for (entry, (case, num, layout)) in (1u64..).zip(cases) {
            let shared = ConnectionShared::new(ConnectionConfig::default());
            let handle = open_consumer(&shared, "persistent://public/default/860-bad", at);
            let frame = entry_frame(handle, entry, num, layout, Vec::new());
            let (popped, grants) = deliver_and_drain(&shared, handle, &frame, at);
            assert!(popped.is_empty(), "{case}: nothing decodable is surfaced");
            assert_eq!(
                grants,
                vec![num as u32],
                "{case}: every charged member is refunded at delivery, and the refund alone re-arms \
                 flow once it crosses the half-queue threshold"
            );
        }

        // ADR-0105 re-dispatch: positions 0..=3 already acked (bits clear), 4..=7 outstanding. The
        // broker charged only the four outstanding ones, so exactly four are refunded.
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let handle = open_consumer(&shared, "persistent://public/default/860-ack-set", at);
        let frame = entry_frame(
            handle,
            9,
            RQ as i32,
            (lz4, 512, Bytes::from(vec![0x5Au8; 64])),
            vec![0b1111_0000],
        );
        let (popped, grants) = deliver_and_drain(&shared, handle, &frame, at);
        assert_eq!(
            popped,
            Vec::<Vec<u8>>::new(),
            "nothing decodable is surfaced"
        );
        assert_eq!(
            grants,
            vec![4],
            "only the positions the broker charged are refunded"
        );

        // A Java-layout body that decodes to exactly `uncompressed_size` but packs 3 of the 8
        // members it declares: the 3 are delivered, as Java's split delivers what parses, and
        // the 5 missing ones are refunded at delivery.
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let handle = open_consumer(&shared, "persistent://public/default/860-short", at);
        let short = payloads("short", 3);
        let frame = entry_frame(
            handle,
            10,
            RQ as i32,
            java_layout(CompressionKind::Zstd, &short),
            Vec::new(),
        );
        let (popped, grants) = deliver_and_drain(&shared, handle, &frame, at);
        assert_eq!(popped, short, "the members that parse are delivered");
        assert_eq!(grants, vec![5], "the five missing ones are refunded");
    });
    // ADR-0112: exactly ONE structured `warn!` per entry with undecodable members — eight above.
    let log = capture.contents();
    assert_eq!(
        log.matches("dropped the undecodable members of a batched entry")
            .count(),
        8,
        "one warn per undecodable entry, got:\n{log}"
    );
    assert!(log.contains("undecoded=8"), "{log}");
    assert!(
        log.contains("undecoded=4"),
        "the ack_set case refunds four: {log}"
    );
}

/// The producer emits the Java layout — ONE compressed body, `uncompressed_size` its packed
/// length — and that frame, handed to a consumer, round-trips. An unbatched payload is
/// compressed once by the state machine; a payload an encrypting engine already compressed
/// (it carries `encryption_keys`) is stamped and left alone, batched or not.
#[test]
fn producer_emits_the_java_layout_and_it_round_trips() {
    let at = Instant::now();
    for (entry, codec) in (1u64..).zip(ALL_CODECS) {
        let shared = ConnectionShared::new(ConnectionConfig::default());
        let consumer = open_consumer(&shared, "persistent://public/default/860-produce", at);
        let producer = shared.inner.lock().create_producer(CreateProducerRequest {
            topic: "persistent://public/default/860-produce".to_owned(),
            compression: codec,
            enable_batching: true,
            max_messages_in_batch: 3,
            ..Default::default()
        });
        let slot = shared
            .inner
            .lock()
            .producer(producer)
            .cloned()
            .expect("producer slot");
        let members = payloads(&format!("produced-{codec:?}"), 3);
        for member in &members {
            slot.queue_send(outgoing(member.clone()), 1_700_000_000_000, at)
                .expect("batched send");
        }
        let frame = slot
            .state
            .lock()
            .next_outbound_frame()
            .expect("the third send fills the batch and flushes it");
        assert_eq!(frame.metadata.compression, Some(codec.to_pb() as i32));
        assert_eq!(frame.metadata.num_messages_in_batch, Some(3));
        let packed_len = frame.metadata.uncompressed_size.expect("uncompressed_size") as usize;
        let packed = decompress(codec, &frame.payload, packed_len)
            .unwrap_or_else(|e| panic!("{codec:?}: the body must be ONE compressed block: {e}"));
        assert_eq!(unpack(packed), members, "{codec:?}: plaintext members");

        // Loop the produced frame back as a dispatch: it must surface every member.
        let inbound = entry_frame(
            consumer,
            entry,
            3,
            (
                frame.metadata.compression,
                packed_len as u32,
                frame.payload.clone(),
            ),
            Vec::new(),
        );
        let (popped, _) = deliver_and_drain(&shared, consumer, &inbound, at);
        assert_eq!(
            popped, members,
            "{codec:?}: magnetar reads what magnetar writes"
        );
    }

    // Unbatched: compressed once, by the state machine.
    let shared = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(at, &handshake_response_bytes())
            .expect("Connected");
    }
    let single = shared.inner.lock().create_producer(CreateProducerRequest {
        topic: "persistent://public/default/860-single".to_owned(),
        compression: CompressionKind::Lz4,
        ..Default::default()
    });
    let slot = shared
        .inner
        .lock()
        .producer(single)
        .cloned()
        .expect("producer slot");
    let plain = b"unbatched-payload|".repeat(16);
    slot.queue_send(outgoing(plain.clone()), 1_700_000_000_000, at)
        .expect("unbatched send");
    let frame = slot.state.lock().next_outbound_frame().expect("one frame");
    assert_eq!(frame.metadata.uncompressed_size, Some(plain.len() as u32));
    assert_eq!(
        decompress(CompressionKind::Lz4, &frame.payload, plain.len())
            .expect("one LZ4 block")
            .as_ref(),
        plain.as_slice()
    );

    // Pre-encoded payloads. An encrypting engine compresses, names the codec, THEN encrypts:
    // the state machine ships that untouched. Ciphertext forwarded with no codec stamp is never
    // compressed — the codec would sit outside the envelope.
    let encrypted = |payload: &[u8], codec: Option<CompressionKind>| {
        let mut msg = outgoing(payload.to_vec());
        msg.metadata.encryption_keys = vec![pb::EncryptionKeys {
            key: "k".to_owned(),
            value: Bytes::from_static(b"v"),
            metadata: Vec::new(),
        }];
        msg.metadata.compression = codec.map(|c| c.to_pb() as i32);
        msg
    };
    slot.queue_send(
        encrypted(b"sealed", Some(CompressionKind::Lz4)),
        1_700_000_000_000,
        at,
    )
    .expect("encrypted unbatched send");
    let frame = slot.state.lock().next_outbound_frame().expect("one frame");
    assert_eq!(frame.payload.as_ref(), b"sealed");
    assert_eq!(
        frame.metadata.compression,
        Some(CompressionKind::Lz4.to_pb() as i32)
    );
    slot.queue_send(encrypted(b"forwarded", None), 1_700_000_000_000, at)
        .expect("forwarded ciphertext");
    let frame = slot.state.lock().next_outbound_frame().expect("one frame");
    assert_eq!(frame.payload.as_ref(), b"forwarded");
    assert_eq!(
        frame.metadata.compression, None,
        "never compressed outside the envelope"
    );
    // A batching producer never batches an encrypted message — a batch would drop its
    // `encryption_keys` (the batched + encrypted follow-up ADR-0112 records) — so each one is a
    // decryptable entry of its own, after whatever batch was pending.
    let batched = shared.inner.lock().create_producer(CreateProducerRequest {
        topic: "persistent://public/default/860-sealed-batch".to_owned(),
        compression: CompressionKind::Lz4,
        enable_batching: true,
        max_messages_in_batch: 2,
        ..Default::default()
    });
    let slot = shared
        .inner
        .lock()
        .producer(batched)
        .cloned()
        .expect("producer slot");
    slot.queue_send(outgoing(b"plain".to_vec()), 1_700_000_000_000, at)
        .expect("batched plain send");
    for payload in [b"s0".as_ref(), b"s1".as_ref()] {
        slot.queue_send(
            encrypted(payload, Some(CompressionKind::Lz4)),
            1_700_000_000_000,
            at,
        )
        .expect("encrypted send on a batching producer");
    }
    let mut frames = Vec::new();
    while let Some(frame) = slot.state.lock().next_outbound_frame() {
        frames.push((
            frame.metadata.num_messages_in_batch,
            frame.metadata.encryption_keys.is_empty(),
            frame.payload,
        ));
    }
    assert_eq!(
        frames.len(),
        3,
        "the pending batch, then one frame per encrypted message"
    );
    assert_eq!((frames[0].0, frames[0].1), (Some(1), true));
    assert_eq!(
        frames[1..]
            .iter()
            .map(|(batched, unkeyed, payload)| (*batched, *unkeyed, payload.to_vec()))
            .collect::<Vec<_>>(),
        vec![(None, false, b"s0".to_vec()), (None, false, b"s1".to_vec())]
    );
}

/// An uncompressed [`OutgoingMessage`] carrying `payload`.
fn outgoing(payload: Vec<u8>) -> OutgoingMessage {
    OutgoingMessage {
        uncompressed_size: payload.len() as u32,
        payload: Bytes::from(payload),
        metadata: pb::MessageMetadata::default(),
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}
