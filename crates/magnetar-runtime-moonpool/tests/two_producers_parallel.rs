// SPDX-License-Identifier: Apache-2.0

//! ADR-0038 Phase 3 — proves that `Producer::send` on the moonpool
//! engine does NOT take the global `ConnectionShared.inner` mutex.
//! Layer (c) of the ADR-0024 four-layer test policy; 1:1 mirror of
//! `magnetar-runtime-tokio/tests/two_producers_parallel.rs`.
//!
//! Test bodies operate against `magnetar_proto::ProducerSlot` directly
//! (the per-handle entry point both engines share), so the assertions
//! prove the sans-io invariant the moonpool driver inherits — no
//! engine-specific driver setup is required.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{ConnectionConfig, CreateProducerRequest, encode_command, pb};
use magnetar_runtime_moonpool::ConnectionShared;

fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandConnected");
    buf
}

fn handshake_complete_shared() -> Arc<ConnectionShared> {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(Instant::now(), &handshake_response_bytes())
            .expect("connected");
    }
    shared
}

fn producer_slot_for(
    shared: &Arc<ConnectionShared>,
    handle: magnetar_proto::ProducerHandle,
) -> Arc<magnetar_proto::ProducerSlot> {
    shared
        .inner
        .lock()
        .producer(handle)
        .cloned()
        .expect("producer slot must exist")
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producer_send_does_not_take_global_connection_lock() {
    let shared = handshake_complete_shared();
    let handle_a = shared.inner.lock().create_producer(CreateProducerRequest {
        topic: "persistent://public/default/parallel-a".to_owned(),
        ..Default::default()
    });
    let slot_a = producer_slot_for(&shared, handle_a);

    let hold_dur = Duration::from_millis(50);
    let blocked = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let blocked_clone = blocked.clone();
    let shared_for_holder = shared.clone();
    let holder = tokio::task::spawn_blocking(move || {
        let _guard = shared_for_holder.inner.lock();
        std::thread::sleep(hold_dur);
        blocked_clone.store(false, std::sync::atomic::Ordering::Release);
    });

    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(
        blocked.load(std::sync::atomic::Ordering::Acquire),
        "holder must still be in its lock-hold window"
    );

    let send_started = Instant::now();
    let seq = slot_a
        .queue_send(outgoing(b"parallel"), 1_700_000_000_000, Instant::now())
        .expect("send accepted");
    let send_elapsed = send_started.elapsed();
    assert_eq!(seq.0, 0, "first send on slot_a is seq-id 0");
    assert!(
        send_elapsed < hold_dur,
        "Phase 3 contract: per-slot send must complete WITHOUT waiting for the global lock \
         (elapsed: {send_elapsed:?}, holder window: {hold_dur:?})"
    );
    assert!(
        blocked.load(std::sync::atomic::Ordering::Acquire),
        "global-lock holder is still active — proves the send didn't go through `inner`"
    );

    holder.await.expect("holder task panicked");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_producers_parallel_sequence_id_allocation_does_not_collide() {
    const N: usize = 200;
    let shared = handshake_complete_shared();
    let handle_a = shared.inner.lock().create_producer(CreateProducerRequest {
        topic: "persistent://public/default/par-a".to_owned(),
        ..Default::default()
    });
    let handle_b = shared.inner.lock().create_producer(CreateProducerRequest {
        topic: "persistent://public/default/par-b".to_owned(),
        ..Default::default()
    });
    let first = producer_slot_for(&shared, handle_a);
    let second = producer_slot_for(&shared, handle_b);

    let first_for_task = first.clone();
    let task_first = tokio::task::spawn_blocking(move || {
        for _ in 0..N {
            first_for_task
                .queue_send(outgoing(b"a"), 1_700_000_000_000, Instant::now())
                .expect("send a accepted");
        }
    });

    let second_for_task = second.clone();
    let task_second = tokio::task::spawn_blocking(move || {
        for _ in 0..N {
            second_for_task
                .queue_send(outgoing(b"b"), 1_700_000_000_001, Instant::now())
                .expect("send b accepted");
        }
    });

    task_first.await.expect("task_first panicked");
    task_second.await.expect("task_second panicked");

    let first_state = first.state.lock();
    let second_state = second.state.lock();
    assert_eq!(
        first_state.pending.len(),
        N,
        "all of the first producer's sends are pending"
    );
    assert_eq!(
        second_state.pending.len(),
        N,
        "all of the second producer's sends are pending"
    );
    assert_eq!(first_state.last_sequence_id_pushed, (N as i64) - 1);
    assert_eq!(second_state.last_sequence_id_pushed, (N as i64) - 1);
}
