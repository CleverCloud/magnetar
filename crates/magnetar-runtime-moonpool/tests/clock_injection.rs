// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::too_many_lines)]

//! ADR-0011 — invariant #3 sans-io clock injection. Pins the moonpool
//! `Producer::send` / `Producer::flush` wall-clock + monotonic-clock
//! reads to the engine-provided clock providers so the stamped
//! `MessageMetadata.publish_time` and the proto-layer monotonic input
//! are deterministic under the moonpool engine — even though the
//! underlying host wall clock keeps moving.
//!
//! ## Shape
//!
//! The fixes in `crates/magnetar-runtime-moonpool/src/producer.rs` and
//! `…/src/consumer.rs` route every host-clock read through
//! [`ConnectionShared::now_wall_clock_ms`] /
//! [`ConnectionShared::now_instant`]. We exercise those two helpers
//! directly with two different wall-clock anchors and assert the
//! observable stamps move with the engine-provided clock, NOT the
//! host clock.
//!
//! Determinism evidence: re-running the helper after sleeping on the
//! host wall clock produces an unchanged stamp under the pinned
//! anchor — that's the bit-for-bit reproducibility ADR-0011 demands.

use std::sync::Arc;
use std::time::Instant;

use magnetar_proto::ConnectionConfig;
use magnetar_runtime_moonpool::{ConnectionShared, DETERMINISTIC_SIM_EPOCH_MS};

#[test]
fn moonpool_producer_send_uses_engine_wall_clock() {
    // V2 (Producer::send / Producer::flush wall-clock leg). Two
    // distinct anchors → two distinct stamped publish_times. Crucially
    // neither depends on the host's `SystemTime::now`.
    let anchor_a = DETERMINISTIC_SIM_EPOCH_MS;
    let anchor_b = DETERMINISTIC_SIM_EPOCH_MS + 1_000_000;

    let shared_a = ConnectionShared::with_auth_and_wall_clock_base(
        ConnectionConfig::default(),
        None,
        anchor_a,
    );
    let shared_b = ConnectionShared::with_auth_and_wall_clock_base(
        ConnectionConfig::default(),
        None,
        anchor_b,
    );

    // `Producer::send` reads `shared.now_wall_clock_ms()` for the
    // publish_time, exactly the helper we exercise here.
    assert_eq!(shared_a.now_wall_clock_ms(), anchor_a);
    assert_eq!(shared_b.now_wall_clock_ms(), anchor_b);

    // Determinism: re-reading after host wall-clock motion yields the
    // same anchored value (the driver advances `wall_clock_ms` from
    // `providers.time().now()`, and we have no driver running here so
    // it stays pinned to the base — exactly the moonpool-sim contract).
    let first = shared_a.now_wall_clock_ms();
    std::thread::sleep(std::time::Duration::from_millis(20));
    let second = shared_a.now_wall_clock_ms();
    assert_eq!(first, second);
    assert_eq!(first, anchor_a);
}

#[test]
fn moonpool_consumer_redelivery_uses_engine_wall_clock() {
    // V3 — `Consumer::redeliver_later` (DLQ delayed redelivery) used
    // to call `SystemTime::now()` to stamp
    // `MessageMetadata.deliver_at_time`. The fix routes the read
    // through `shared.now_wall_clock_ms()`. Same helper as V2 but
    // surfaced as `i64` after a saturating cast, matching the
    // production code.
    let anchor_a = DETERMINISTIC_SIM_EPOCH_MS;
    let anchor_b = DETERMINISTIC_SIM_EPOCH_MS + 5_000;

    let shared_a = ConnectionShared::with_auth_and_wall_clock_base(
        ConnectionConfig::default(),
        None,
        anchor_a,
    );
    let shared_b = ConnectionShared::with_auth_and_wall_clock_base(
        ConnectionConfig::default(),
        None,
        anchor_b,
    );

    let delay_ms = 1_000_i64;
    let stamp = |shared: &Arc<ConnectionShared>| -> i64 {
        let now_ms = i64::try_from(shared.now_wall_clock_ms()).unwrap_or(i64::MAX);
        now_ms.saturating_add(delay_ms)
    };

    assert_eq!(stamp(&shared_a), anchor_a as i64 + delay_ms);
    assert_eq!(stamp(&shared_b), anchor_b as i64 + delay_ms);

    // Determinism: stamping twice on the same shared yields the same
    // value even though host wall time has advanced between the calls.
    let first = stamp(&shared_a);
    std::thread::sleep(std::time::Duration::from_millis(20));
    let second = stamp(&shared_a);
    assert_eq!(first, second);
}

#[test]
fn moonpool_default_wall_clock_is_deterministic_epoch() {
    // V4 — the default constructor used to anchor `wall_clock_base_ms`
    // at `SystemTime::now`; it now anchors at the documented
    // `DETERMINISTIC_SIM_EPOCH_MS` so every test building a moonpool
    // engine with `ConnectionShared::new` is bit-for-bit reproducible
    // without any wall-clock setup.
    let a = ConnectionShared::new(ConnectionConfig::default());
    let b = ConnectionShared::new(ConnectionConfig::default());
    assert_eq!(a.wall_clock_base_ms, DETERMINISTIC_SIM_EPOCH_MS);
    assert_eq!(b.wall_clock_base_ms, DETERMINISTIC_SIM_EPOCH_MS);
    assert_eq!(a.now_wall_clock_ms(), b.now_wall_clock_ms());
    // Two constructions, identical observable state — no host-clock leak.
    assert_eq!(a.now_wall_clock_ms(), DETERMINISTIC_SIM_EPOCH_MS);
}

#[test]
fn moonpool_producer_send_now_instant_is_engine_supplied() {
    // V2 (Producer::send / Producer::flush monotonic-clock leg). The
    // `queue_send` / `flush_producer` / `Reserving → Pending`
    // transitions used to read `std::time::Instant::now()`; they now
    // route through `shared.now_instant()`. The default provider is
    // `Instant::now` (so the production tokio path is unchanged);
    // tests / engines can install a virtual-clock-backed provider via
    // `ConnectionShared::with_auth_wall_clock_and_instant`. We assert
    // the indirection point works by pinning the provider to a single
    // Instant and observing that two reads collapse to it.
    let anchor = Instant::now();
    let pinned = anchor + std::time::Duration::from_secs(42);
    let provider: Arc<dyn Fn() -> Instant + Send + Sync> = Arc::new(move || pinned);

    let shared = ConnectionShared::with_auth_wall_clock_and_instant(
        ConnectionConfig::default(),
        None,
        DETERMINISTIC_SIM_EPOCH_MS,
        provider,
    );
    // Two reads in any order observe the same pinned value — no host
    // `Instant::now` leak.
    let a = shared.now_instant();
    std::thread::sleep(std::time::Duration::from_millis(10));
    let b = shared.now_instant();
    assert_eq!(a, b);
    assert_eq!(a, pinned);
}
