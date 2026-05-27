// SPDX-License-Identifier: Apache-2.0

//! ADR-0038 Phase 3 — proves that `Producer::send` on the tokio engine
//! does NOT take the global `ConnectionShared.inner` mutex. Layer (b) of
//! the ADR-0024 four-layer test policy.
//!
//! The test acquires `shared.inner.lock()` from the test thread and
//! holds it while a separate task drives `Producer::send` on a producer
//! whose slot was captured earlier. Pre-Phase-3 this would deadlock
//! (or sit in `lock()` until the test thread releases the global
//! mutex). Post-Phase-3 the send completes because it only touches the
//! per-slot mutex.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{ConnectionConfig, CreateProducerRequest, pb};
use magnetar_runtime_tokio::ConnectionShared;

mod common;
use common::handshake_response_bytes;

/// Bring a `ConnectionShared` to `Connected` so `create_producer` runs
/// cleanly.
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

/// Capture the per-slot Arc for a producer the test just registered.
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

/// While the test thread holds the global `ConnectionShared.inner`
/// mutex, a parallel `Producer::send` (via `ProducerSlot::queue_send`)
/// completes successfully. The pre-Phase-3 implementation would block
/// here until the global lock released. The fact that this test exits
/// is the proof.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producer_send_does_not_take_global_connection_lock() {
    let shared = handshake_complete_shared();
    let handle_a = shared.inner.lock().create_producer(CreateProducerRequest {
        topic: "persistent://public/default/parallel-a".to_owned(),
        ..Default::default()
    });
    let slot_a = producer_slot_for(&shared, handle_a);

    // Hold the global lock for a noticeable duration on a separate task,
    // then poke a parallel send and confirm it returns without waiting
    // for the lock to release.
    let hold_dur = Duration::from_millis(50);
    let blocked = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let blocked_clone = blocked.clone();
    let shared_for_holder = shared.clone();
    let holder = tokio::task::spawn_blocking(move || {
        let _guard = shared_for_holder.inner.lock();
        // Hold the global lock for the whole window — only release when
        // the cooperator has had time to finish its per-slot send.
        std::thread::sleep(hold_dur);
        blocked_clone.store(false, std::sync::atomic::Ordering::Release);
    });

    // Wait a short tick so the holder has time to grab the lock.
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(
        blocked.load(std::sync::atomic::Ordering::Acquire),
        "holder must still be in its lock-hold window"
    );

    // The hot path: queue a send purely through the per-slot mutex.
    // Pre-Phase-3 this would block on `shared.inner.lock()`.
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

    // Let the holder finish so we can clean up.
    holder.await.expect("holder task panicked");
}

/// Two producers each enqueue many sends in parallel via per-slot locks;
/// confirms that the per-slot mutex partitions sequence-id allocation
/// per producer (i.e. no cross-talk) and that both producers reach the
/// expected pending counts under contention.
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

    // Each producer independently allocated 0..N-1 — no cross-producer
    // collision because the slots are independent.
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
