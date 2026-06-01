// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer 4 — tokio ↔ moonpool equivalence on the
//! [`magnetar_proto::Connection::unregister_waker`] code path that the
//! `Drop` impls on the runtime crates' `RequestFut` /
//! `RequestFut` futures depend on.
//!
//! Companion to the lookup multi-agent review MEDIUM-4 fix.
//!
//! The drop-time cleanup is engine-agnostic — it runs on the
//! `magnetar_proto::Connection` the engine wraps — so the differential
//! is structural: both engines' [`ConnectionShared`] expose a
//! `Mutex<Connection>` and the same proto API, and the slab-shrink
//! behaviour observable through [`Connection::pending_waker_count`]
//! must be bit-identical step-by-step.
//!
//! The test does not drive any wire-protocol traffic — the runtime test
//! suites under `magnetar-runtime-tokio` / `magnetar-runtime-moonpool`
//! cover the integration-level path (silent broker, partitioned-metadata
//! call, timeout, slab count). Here we pin the helper-level invariant
//! at the [`Connection`] boundary so neither engine can regress in
//! isolation.

#![forbid(unsafe_code)]

use std::task::Waker;

use magnetar_proto::{ConnectionConfig, PendingOpKey, RequestId};

fn noop_waker() -> Waker {
    Waker::noop().clone()
}

/// Step-by-step waker-slab counts must agree between engines.
/// 1. fresh `Connection`: slab empty.
/// 2. register a request-keyed waker: slab grows by 1.
/// 3. `unregister_waker(same key)`: slab shrinks by 1.
/// 4. second unregister on the same (already-empty) key is a no-op.
/// 5. registering two distinct keys then unregistering the first leaves the slab at 1.
#[test]
fn unregister_waker_slab_evolution_is_byte_identical_across_engines() {
    let tokio_shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
    let moonpool_shared =
        magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());

    let mut tokio_conn = tokio_shared.inner.lock();
    let mut moonpool_conn = moonpool_shared.inner.lock();

    let key_a = PendingOpKey::Request(RequestId(7));
    let key_b = PendingOpKey::Request(RequestId(8));

    // (1) Both engines start with an empty slab.
    assert_eq!(tokio_conn.pending_waker_count(), 0);
    assert_eq!(moonpool_conn.pending_waker_count(), 0);

    // (2) Register one waker each.
    tokio_conn.register_waker(key_a, noop_waker());
    moonpool_conn.register_waker(key_a, noop_waker());
    assert_eq!(
        tokio_conn.pending_waker_count(),
        moonpool_conn.pending_waker_count(),
        "post-register slab size must match across engines"
    );
    assert_eq!(tokio_conn.pending_waker_count(), 1);

    // (3) Unregister: both slabs shrink to 0 in lockstep.
    tokio_conn.unregister_waker(key_a);
    moonpool_conn.unregister_waker(key_a);
    assert_eq!(
        tokio_conn.pending_waker_count(),
        moonpool_conn.pending_waker_count(),
        "post-unregister slab size must match across engines"
    );
    assert_eq!(tokio_conn.pending_waker_count(), 0);

    // (4) Double-unregister is a no-op on both sides.
    tokio_conn.unregister_waker(key_a);
    moonpool_conn.unregister_waker(key_a);
    assert_eq!(tokio_conn.pending_waker_count(), 0);
    assert_eq!(moonpool_conn.pending_waker_count(), 0);

    // (5) Two distinct keys → unregister the first → 1 left on both engines.
    tokio_conn.register_waker(key_a, noop_waker());
    tokio_conn.register_waker(key_b, noop_waker());
    moonpool_conn.register_waker(key_a, noop_waker());
    moonpool_conn.register_waker(key_b, noop_waker());
    assert_eq!(tokio_conn.pending_waker_count(), 2);
    assert_eq!(moonpool_conn.pending_waker_count(), 2);

    tokio_conn.unregister_waker(key_a);
    moonpool_conn.unregister_waker(key_a);
    assert_eq!(
        tokio_conn.pending_waker_count(),
        moonpool_conn.pending_waker_count(),
        "siblings must survive sibling-key unregister identically on both engines"
    );
    assert_eq!(tokio_conn.pending_waker_count(), 1);
}
