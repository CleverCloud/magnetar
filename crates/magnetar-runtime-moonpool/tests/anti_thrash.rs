// SPDX-License-Identifier: Apache-2.0

//! ADR-0028 anti-thrash policy — moonpool engine integration coverage.
//!
//! Mirror of `crates/magnetar-runtime-tokio/tests/anti_thrash.rs`. Maintains
//! the tokio ↔ moonpool 1:1 test count required by ADR-0024.
//!
//! These tests drive the sans-io state machine through the moonpool engine's
//! [`ConnectionShared`] shared-state surface — the exact path the moonpool
//! supervised driver loop walks on every inbound frame. Sim-clock-driven
//! end-to-end behaviour is covered by the chaos sweep in
//! `crates/magnetar-runtime-moonpool/tests/sim_chaos.rs` (the
//! `DropsTcpAfterCreate` workload).

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    AntiThrashDisposition, AntiThrashThreshold, ConnectionConfig, ConnectionEvent,
    CreateProducerRequest, SupervisorConfig, encode_command, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

fn supervisor_with_anti_thrash() -> SupervisorConfig {
    SupervisorConfig {
        anti_thrash_threshold: Some(AntiThrashThreshold {
            successful_attaches: 3,
            window: Duration::from_secs(2),
            drop_within: Duration::from_millis(50),
        }),
        drop_grace: Duration::from_millis(500),
        max_backoff_after_thrash: Duration::from_secs(30),
        ..SupervisorConfig::default()
    }
}

fn producer_success_bytes(request_id: u64) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: "anti-thrash-test".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandProducerSuccess");
    buf
}

fn one_thrash_cycle(shared: &Arc<ConnectionShared>, idx: u32, now: Instant) -> Instant {
    let mut conn = shared.inner.lock();
    if idx > 0 {
        conn.reset();
    }
    conn.begin_handshake().expect("begin_handshake");
    let resp = common::handshake_response_bytes();
    conn.handle_bytes(now, &resp).expect("handshake");

    let req = CreateProducerRequest {
        topic: "persistent://public/default/anti-thrash".to_owned(),
        ..Default::default()
    };
    let request_id = conn.peek_next_request_id_for_test();
    let _handle = conn.create_producer(req);

    let _ = conn.poll_transmit();

    let attach_now = now + Duration::from_millis(1);
    let success = producer_success_bytes(request_id);
    conn.handle_bytes(attach_now, &success)
        .expect("producer success");

    let drop_now = attach_now + Duration::from_millis(10);
    conn.mark_disconnected();
    conn.record_reattach_outcome(
        drop_now,
        magnetar_proto::ReAttachHandle::Producer(magnetar_proto::ProducerHandle(0)),
        magnetar_proto::ReAttachOutcomeKind::TcpDropAfterReAttach,
    );
    drop_now
}

/// Like [`one_thrash_cycle`], but the session re-attaches `handles` producers before the broker
/// drops the socket. The driver records exactly ONE connection-wide drop per socket loss (it
/// carries a placeholder handle), whatever number of handles the socket had acked.
fn multi_handle_thrash_cycle(
    shared: &Arc<ConnectionShared>,
    idx: u32,
    handles: u32,
    now: Instant,
) -> Instant {
    let mut conn = shared.inner.lock();
    if idx > 0 {
        conn.reset();
    }
    conn.begin_handshake().expect("begin_handshake");
    let resp = common::handshake_response_bytes();
    conn.handle_bytes(now, &resp).expect("handshake");

    let mut attach_now = now;
    for _ in 0..handles {
        let request_id = conn.peek_next_request_id_for_test();
        let _handle = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/anti-thrash".to_owned(),
            ..Default::default()
        });
        conn.poll_transmit();
        attach_now += Duration::from_millis(1);
        conn.handle_bytes(attach_now, &producer_success_bytes(request_id))
            .expect("producer success");
    }

    // Within `drop_within` (50 ms) of every attach on this socket.
    let drop_now = attach_now + Duration::from_millis(10);
    conn.mark_disconnected();
    conn.record_reattach_outcome(
        drop_now,
        magnetar_proto::ReAttachHandle::Producer(magnetar_proto::ProducerHandle(0)),
        magnetar_proto::ReAttachOutcomeKind::TcpDropAfterReAttach,
    );
    drop_now
}

#[test]
fn default_supervisor_disables_detector_even_under_thrash() {
    let cfg = ConnectionConfig {
        supervisor: Some(SupervisorConfig::default()),
        ..ConnectionConfig::default()
    };
    let shared = ConnectionShared::new(cfg);
    let mut now = Instant::now();
    for i in 0..5 {
        now = one_thrash_cycle(&shared, i, now) + Duration::from_millis(20);
    }
    let conn = shared.inner.lock();
    assert!(
        matches!(conn.anti_thrash_tick(now), AntiThrashDisposition::Normal),
        "default config must leave the detector OFF"
    );
}

#[test]
fn opt_in_threshold_trips_cooldown_after_n_pairs() {
    let cfg = ConnectionConfig {
        supervisor: Some(supervisor_with_anti_thrash()),
        ..ConnectionConfig::default()
    };
    let shared = ConnectionShared::new(cfg);
    let mut now = Instant::now();
    let mut last_drop = now;
    for i in 0..3 {
        last_drop = one_thrash_cycle(&shared, i, now);
        now = last_drop + Duration::from_millis(20);
    }

    let mut conn = shared.inner.lock();
    let disp = conn.anti_thrash_tick(last_drop);
    let until = match disp {
        AntiThrashDisposition::Cooldown { until } => until,
        AntiThrashDisposition::Normal => panic!(
            "expected cooldown after 3 attach-then-drop pairs; got Normal. ring={:?}",
            conn.anti_thrash_state().ring()
        ),
    };
    assert!(
        until.saturating_duration_since(last_drop) >= Duration::from_secs(29),
        "cooldown should respect the configured floor (≈30 s)"
    );

    let mut saw_cooldown = false;
    while let Some(ev) = conn.poll_event() {
        if matches!(ev, ConnectionEvent::AntiThrashCooldown { .. }) {
            saw_cooldown = true;
        }
    }
    assert!(
        saw_cooldown,
        "expected an AntiThrashCooldown event after the detector tripped"
    );
}

#[test]
fn first_op_success_clears_cooldown_and_emits_cleared_event() {
    let cfg = ConnectionConfig {
        supervisor: Some(supervisor_with_anti_thrash()),
        ..ConnectionConfig::default()
    };
    let shared = ConnectionShared::new(cfg);
    let mut now = Instant::now();
    for i in 0..3 {
        now = one_thrash_cycle(&shared, i, now) + Duration::from_millis(20);
    }
    let mut conn = shared.inner.lock();
    assert!(matches!(
        conn.anti_thrash_tick(now),
        AntiThrashDisposition::Cooldown { .. }
    ));
    while conn.poll_event().is_some() {}
    conn.record_first_op_success(now);
    let mut saw_cleared = false;
    while let Some(ev) = conn.poll_event() {
        if matches!(ev, ConnectionEvent::AntiThrashCleared) {
            saw_cleared = true;
        }
    }
    assert!(
        saw_cleared,
        "first-op-success must publish AntiThrashCleared when a cooldown was active"
    );
    assert!(matches!(
        conn.anti_thrash_tick(now),
        AntiThrashDisposition::Normal
    ));
}

/// ADR-0028 §"Detector mechanics": the cooldown trips once `successful_attaches` ring entries all
/// carry a drop within `drop_within`. A supervised connection that re-attaches TWO handles per
/// socket and loses each socket inside `drop_within` is exactly the storm the detector exists for,
/// but the driver feeds one connection-wide drop per socket loss, so every earlier handle's ring
/// entry stays unstamped and the detector never leaves `Normal`.
#[test]
fn opt_in_threshold_trips_cooldown_when_each_socket_reattaches_two_handles() {
    let cfg = ConnectionConfig {
        supervisor: Some(supervisor_with_anti_thrash()),
        ..ConnectionConfig::default()
    };
    let shared = ConnectionShared::new(cfg);
    let mut now = Instant::now();
    let mut last_drop = now;
    let mut saw_cooldown = false;
    for i in 0..3 {
        last_drop = multi_handle_thrash_cycle(&shared, i, 2, now);
        now = last_drop + Duration::from_millis(20);
        // Drain per cycle: the next cycle's `reset()` discards undelivered events.
        let mut conn = shared.inner.lock();
        while let Some(ev) = conn.poll_event() {
            if matches!(ev, ConnectionEvent::AntiThrashCooldown { .. }) {
                saw_cooldown = true;
            }
        }
    }

    let conn = shared.inner.lock();
    assert!(
        matches!(
            conn.anti_thrash_tick(last_drop),
            AntiThrashDisposition::Cooldown { .. }
        ),
        "three sockets each re-attached two handles and dropped inside drop_within \
         (successful_attaches = 3), yet the detector stayed Normal; ring={:?}",
        conn.anti_thrash_state().ring()
    );
    assert!(
        saw_cooldown,
        "expected an AntiThrashCooldown event after the detector tripped"
    );
}
