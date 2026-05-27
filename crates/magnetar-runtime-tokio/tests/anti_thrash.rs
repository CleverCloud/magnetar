// SPDX-License-Identifier: Apache-2.0

//! ADR-0028 anti-thrash policy — integration coverage through the tokio
//! engine's [`ConnectionShared`].
//!
//! These tests drive the sans-io state machine directly through the engine's
//! shared-state surface — exactly the same handle the production driver loop
//! uses on every inbound frame — and assert:
//!
//! 1. With `anti_thrash_threshold = None` (the default), the detector is a no-op even when the
//!    broker drops the socket immediately after every ack — current behaviour is preserved.
//! 2. With the threshold opted in, the detector trips into `AntiThrashDisposition::Cooldown` after
//!    the configured number of create-then-drop pairs and emits the `AntiThrashCooldown` event with
//!    the cooldown deadline.
//! 3. The cooldown clears on a successful first-op-after-attach (`record_first_op_success`).
//!
//! End-to-end timing of the supervisor sleep is exercised by the moonpool
//! engine's deterministic-simulation test
//! (`crates/magnetar-runtime-moonpool/tests/anti_thrash.rs`); here we focus
//! on the sans-io state transitions and the engine-shared-state wiring.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    AntiThrashDisposition, AntiThrashThreshold, ConnectionConfig, ConnectionEvent,
    CreateProducerRequest, SupervisorConfig, encode_command, pb,
};
use magnetar_runtime_tokio::ConnectionShared;

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

/// Drive one full attach round-trip through the shared connection: handshake,
/// open a producer, feed the broker's success ack, mark the socket as dropped.
/// Returns the moment of the simulated drop.
fn one_thrash_cycle(shared: &Arc<ConnectionShared>, idx: u32, now: Instant) -> Instant {
    let mut conn = shared.inner.lock();
    // Fresh handshake on every cycle — the supervisor would have called
    // `reset()` + `begin_handshake()` between drops.
    if idx > 0 {
        conn.reset();
    }
    conn.begin_handshake().expect("begin_handshake");
    let resp = handshake_response_bytes();
    conn.handle_bytes(now, &resp).expect("handshake");

    let req = CreateProducerRequest {
        topic: "persistent://public/default/anti-thrash".to_owned(),
        ..Default::default()
    };
    let request_id = conn.peek_next_request_id_for_test();
    let _handle = conn.create_producer(req);

    // Drain the outbound bytes the state machine queued (the production driver
    // would write these to the socket; we simply discard them in this test).
    conn.poll_transmit();

    let attach_now = now + Duration::from_millis(1);
    let success = producer_success_bytes(request_id);
    conn.handle_bytes(attach_now, &success)
        .expect("producer success");

    // Within `drop_within` (50 ms) — simulate the broker RST-ing the socket.
    let drop_now = attach_now + Duration::from_millis(10);
    // The production driver detects this in the inner loop and calls
    // `mark_disconnected()`. The supervisor outer loop then attributes it to
    // the recent attach via the anti-thrash path:
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

    // The Connection must have emitted a single AntiThrashCooldown event on
    // the Normal → Cooldown transition.
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
    {
        let mut conn = shared.inner.lock();
        assert!(matches!(
            conn.anti_thrash_tick(now),
            AntiThrashDisposition::Cooldown { .. }
        ));
        // Drain queued events so we can assert the *next* event is Cleared.
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
}
