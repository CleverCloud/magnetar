// SPDX-License-Identifier: Apache-2.0

//! ADR-0040 waves 1.1 + 1.2 — runtime integration tests for
//! `Connection::poll_transmit_vectored` (layer (b) of the ADR-0024
//! four-layer policy on the tokio engine). Mirrored 1:1 by
//! `magnetar-runtime-moonpool/tests/poll_transmit_vectored_parity.rs`.
//!
//! Wave 1.1 coverage:
//! - Post-handshake byte equivalence with the legacy `poll_transmit` path (Contiguous arm, both
//!   buffers carry the pending Connect frame).
//! - Empty-buffer case (Contiguous arm returns empty slice).
//!
//! Wave 1.2 coverage:
//! - With a Ready producer carrying a queued send and the contiguous `outbound` drained,
//!   `poll_transmit_vectored` returns `Vectored` with the producer's `[head, payload]` segment
//!   pair, and concatenating the segments yields the same bytes the legacy `poll_transmit` path
//!   produces for the same publish.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, ProducerHandle, Transmit, encode_command, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

mod common;
use common::handshake_response_bytes;

/// Drive a `ConnectionShared` to `Connected` so the outbound buffer
/// carries the same post-handshake payload as a fresh handshake's
/// pending Connect frame.
fn handshake_complete_shared() -> Arc<ConnectionShared> {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("connected");
        let _ = conn.poll_event();
    }
    shared
}

/// Open a producer through the full Ready handshake (mirrors the
/// moonpool helper and the per-file copy in `reconnect_with_inflight.rs`).
fn open_producer_ready(shared: &Arc<ConnectionShared>, topic: &str, at: Instant) -> ProducerHandle {
    let req = CreateProducerRequest {
        topic: topic.to_owned(),
        ..Default::default()
    };
    let (handle, request_id) = {
        let mut conn = shared.inner.lock();
        let request_id = conn.peek_next_request_id_for_test();
        let handle = conn.create_producer(req);
        (handle, request_id)
    };
    let success = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: format!("magnetar-test-{}", handle.0),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: None,
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &success).expect("encode CommandProducerSuccess");
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(at, &buf).expect("apply ProducerSuccess");
        let _ = conn.poll_event();
    }
    handle
}

fn outgoing(payload: &'static [u8]) -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(payload),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: payload.len() as u32,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

#[test]
fn poll_transmit_vectored_matches_poll_transmit_post_handshake() {
    let legacy_shared = ConnectionShared::new(ConnectionConfig::default());
    let vectored_shared = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = legacy_shared.inner.lock();
        conn.begin_handshake().expect("handshake legacy");
    }
    {
        let mut conn = vectored_shared.inner.lock();
        conn.begin_handshake().expect("handshake vectored");
    }
    let legacy_bytes = {
        let mut conn = legacy_shared.inner.lock();
        conn.poll_transmit()
    };
    let vectored_owned = {
        let mut conn = vectored_shared.inner.lock();
        match conn.poll_transmit_vectored() {
            Transmit::Contiguous(slice) => slice.to_vec(),
            Transmit::Vectored(_) => panic!(
                "post-handshake outbound is non-empty (Connect frame) — wire-order requires the Contiguous arm"
            ),
        }
    };
    assert_eq!(
        &vectored_owned[..],
        &legacy_bytes[..],
        "tokio engine: poll_transmit_vectored::Contiguous bytes must match poll_transmit bytes for the pending Connect frame"
    );
    assert!(
        !vectored_owned.is_empty(),
        "tokio engine: handshake Connect frame is non-empty"
    );
}

#[test]
fn poll_transmit_vectored_is_empty_after_drain() {
    let shared = handshake_complete_shared();
    {
        let mut conn = shared.inner.lock();
        let _ = conn.poll_transmit();
    }
    let mut conn = shared.inner.lock();
    match conn.poll_transmit_vectored() {
        Transmit::Contiguous(slice) => {
            assert!(
                slice.is_empty(),
                "tokio engine: post-drain poll_transmit_vectored::Contiguous must be empty"
            );
        }
        Transmit::Vectored(segs) => panic!(
            "no producer segments queued — expected empty Contiguous, got {} segments",
            segs.len()
        ),
    }
}

#[test]
fn poll_transmit_vectored_emits_vectored_for_queued_producer_send() {
    // ADR-0040 wave 1.2: with a Ready producer carrying a queued send
    // and `outbound` drained, `poll_transmit_vectored` must return
    // `Vectored` carrying the producer's `[head, payload]` segment
    // pair. Concatenating the segments must equal the bytes the
    // legacy `poll_transmit` path would produce for the same publish.
    let at = Instant::now();
    let vectored_shared = handshake_complete_shared();
    let legacy_shared = handshake_complete_shared();
    let topic = "persistent://public/default/wave-1-2-tokio";
    let vec_producer = open_producer_ready(&vectored_shared, topic, at);
    let leg_producer = open_producer_ready(&legacy_shared, topic, at);
    let payload: &'static [u8] = b"wave-1-2-tokio-payload";

    // Drain all post-handshake / ProducerSuccess bytes on both sides
    // so we isolate the publish's bytes.
    {
        let mut conn = vectored_shared.inner.lock();
        let _ = conn.poll_transmit();
    }
    {
        let mut conn = legacy_shared.inner.lock();
        let _ = conn.poll_transmit();
    }

    // Capture the per-slot Arc and queue the send via `ProducerSlot::queue_send`
    // — the production hot path per ADR-0038, which (crucially) does NOT
    // trigger `Connection::drain_producer_outbound` like the
    // `Connection::send` shortcut does. With the contiguous `outbound`
    // staying empty, `poll_transmit_vectored` exercises the Vectored arm.
    let publish_at = at + Duration::from_millis(1);
    let vec_slot = vectored_shared
        .inner
        .lock()
        .producer(vec_producer)
        .cloned()
        .expect("vectored producer slot");
    let leg_slot = legacy_shared
        .inner
        .lock()
        .producer(leg_producer)
        .cloned()
        .expect("legacy producer slot");
    let seq_v = vec_slot
        .queue_send(outgoing(payload), 0, publish_at)
        .expect("vectored queue_send");
    let seq_l = leg_slot
        .queue_send(outgoing(payload), 0, publish_at)
        .expect("legacy queue_send");
    assert_eq!(
        seq_v, seq_l,
        "identical setup must allocate identical sequence ids"
    );

    let legacy_bytes = {
        let mut conn = legacy_shared.inner.lock();
        conn.poll_transmit()
    };
    let vectored_concat = {
        let mut conn = vectored_shared.inner.lock();
        match conn.poll_transmit_vectored() {
            Transmit::Vectored(segs) => {
                assert!(
                    segs.len() >= 2 && segs.len() % 2 == 0,
                    "tokio engine: producer batch emits paired [head, payload] segments — got {}",
                    segs.len()
                );
                let total: usize = segs.iter().map(Bytes::len).sum();
                let mut concat = BytesMut::with_capacity(total);
                for seg in segs {
                    concat.extend_from_slice(seg);
                }
                concat.freeze()
            }
            Transmit::Contiguous(_) => panic!(
                "wave 1.2: queued producer send must take the Vectored arm when outbound is empty"
            ),
        }
    };
    assert_eq!(
        &vectored_concat[..],
        &legacy_bytes[..],
        "tokio engine: vectored segment concatenation must equal contiguous poll_transmit bytes"
    );
}
