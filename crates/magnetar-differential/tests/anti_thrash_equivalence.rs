// SPDX-License-Identifier: Apache-2.0

//! ADR-0028 anti-thrash policy — tokio ↔ moonpool engine event-stream
//! equivalence.
//!
//! Per ADR-0024 four-layer test rule, the anti-thrash detector lives below
//! both engines (in `magnetar-proto::AntiThrashState`) and is driven by the
//! engine-shared-state surface. This test feeds the **same** scripted
//! attach-then-drop sequence into the tokio and moonpool `ConnectionShared`
//! variants and asserts they emit the same anti-thrash events in the same
//! order:
//!
//! ```text
//! [ProducerReady, ProducerReady, ProducerReady, AntiThrashCooldown { until: T }, AntiThrashCleared]
//! ```
//!
//! The `until` instants are necessarily host-specific (they are computed
//! against [`std::time::Instant`]); the test compares the disposition kind
//! plus the cooldown floor, not the absolute deadlines.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    AntiThrashDisposition, AntiThrashThreshold, ConnectionConfig, ConnectionEvent,
    CreateProducerRequest, SupervisorConfig, encode_command, pb,
};

#[derive(Debug, PartialEq, Eq)]
enum EventKind {
    Connected,
    ProducerReady,
    AntiThrashCooldown,
    AntiThrashCleared,
}

fn classify(ev: &ConnectionEvent) -> Option<EventKind> {
    match ev {
        ConnectionEvent::Connected { .. } => Some(EventKind::Connected),
        ConnectionEvent::ProducerReady { .. } => Some(EventKind::ProducerReady),
        ConnectionEvent::AntiThrashCooldown { .. } => Some(EventKind::AntiThrashCooldown),
        ConnectionEvent::AntiThrashCleared => Some(EventKind::AntiThrashCleared),
        _ => None,
    }
}

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
            server_version: "magnetar-diff".to_owned(),
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
            producer_name: "diff-test".to_owned(),
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

fn drive_scripted_thrash<F>(make_shared: F, start: Instant) -> Vec<EventKind>
where
    F: FnOnce(ConnectionConfig) -> Arc<dyn SharedConn>,
{
    let cfg = ConnectionConfig {
        supervisor: Some(supervisor_with_anti_thrash()),
        ..ConnectionConfig::default()
    };
    let shared = make_shared(cfg);
    let mut now = start;
    let mut events: Vec<EventKind> = Vec::new();
    for i in 0..3u32 {
        let mut conn = shared.lock();
        if i > 0 {
            conn.reset();
        }
        conn.begin_handshake().expect("begin_handshake");
        conn.handle_bytes(now, &handshake_response_bytes())
            .expect("handshake");
        let req = CreateProducerRequest {
            topic: "persistent://public/default/diff-anti-thrash".to_owned(),
            ..Default::default()
        };
        let request_id = conn.peek_next_request_id_for_test();
        let _h = conn.create_producer(req);
        let _ = conn.poll_transmit();
        let attach_now = now + Duration::from_millis(1);
        conn.handle_bytes(attach_now, &producer_success_bytes(request_id))
            .expect("producer success");
        let drop_now = attach_now + Duration::from_millis(10);
        conn.mark_disconnected();
        conn.record_reattach_outcome_producer_drop(drop_now);
        // Pull the queued events emitted on this iteration.
        while let Some(ev) = conn.poll_event() {
            if let Some(kind) = classify(&ev) {
                events.push(kind);
            }
        }
        now = drop_now + Duration::from_millis(20);
    }
    // After tripping, observe a clearing first-op success.
    {
        let mut conn = shared.lock();
        // Re-attach succeeded and a first-op (e.g. SendReceipt) landed.
        conn.record_first_op_success(now);
        while let Some(ev) = conn.poll_event() {
            if let Some(kind) = classify(&ev) {
                events.push(kind);
            }
        }
    }
    events
}

/// A tiny dyn-trait abstraction over the two engines' [`ConnectionShared`]
/// wrappers so the scripted-thrash driver can stay engine-agnostic.
trait SharedConn: Send + Sync {
    fn lock(&self) -> parking_lot::MutexGuard<'_, magnetar_proto::Connection>;
}

struct TokioShared(Arc<magnetar_runtime_tokio::ConnectionShared>);
struct MoonpoolShared(Arc<magnetar_runtime_moonpool::ConnectionShared>);

impl SharedConn for TokioShared {
    fn lock(&self) -> parking_lot::MutexGuard<'_, magnetar_proto::Connection> {
        self.0.inner.lock()
    }
}
impl SharedConn for MoonpoolShared {
    fn lock(&self) -> parking_lot::MutexGuard<'_, magnetar_proto::Connection> {
        self.0.inner.lock()
    }
}

trait ConnExt {
    fn record_reattach_outcome_producer_drop(&mut self, now: Instant);
}
impl ConnExt for magnetar_proto::Connection {
    fn record_reattach_outcome_producer_drop(&mut self, now: Instant) {
        self.record_reattach_outcome(
            now,
            magnetar_proto::ReAttachHandle::Producer(magnetar_proto::ProducerHandle(0)),
            magnetar_proto::ReAttachOutcomeKind::TcpDropAfterReAttach,
        );
    }
}

#[test]
fn tokio_and_moonpool_emit_the_same_anti_thrash_event_stream() {
    let start = Instant::now();
    let tokio_events = drive_scripted_thrash(
        |cfg| {
            Arc::new(TokioShared(magnetar_runtime_tokio::ConnectionShared::new(
                cfg,
            )))
        },
        start,
    );
    let moonpool_events = drive_scripted_thrash(
        |cfg| {
            Arc::new(MoonpoolShared(
                magnetar_runtime_moonpool::ConnectionShared::new(cfg),
            ))
        },
        start,
    );
    assert_eq!(
        tokio_events, moonpool_events,
        "tokio and moonpool anti-thrash event streams diverged:\n\
         tokio = {tokio_events:?}\nmoonpool = {moonpool_events:?}"
    );
    // Sanity: the stream must include at least one Cooldown and one Cleared
    // event in that order — the canonical ADR-0028 trace.
    let cooldown_idx = tokio_events
        .iter()
        .position(|e| matches!(e, EventKind::AntiThrashCooldown))
        .expect("expected AntiThrashCooldown in the trace");
    let cleared_idx = tokio_events
        .iter()
        .position(|e| matches!(e, EventKind::AntiThrashCleared))
        .expect("expected AntiThrashCleared in the trace");
    assert!(
        cooldown_idx < cleared_idx,
        "AntiThrashCleared must follow AntiThrashCooldown; got {tokio_events:?}",
    );
}

#[test]
fn cooldown_floor_matches_max_backoff_after_thrash() {
    let cfg = ConnectionConfig {
        supervisor: Some(supervisor_with_anti_thrash()),
        ..ConnectionConfig::default()
    };

    let tokio_until = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(cfg.clone());
        let mut now = Instant::now();
        let last = drive_one(&shared.inner, &mut now);
        match shared.inner.lock().anti_thrash_tick(last) {
            AntiThrashDisposition::Cooldown { until } => {
                Some(until.saturating_duration_since(last))
            }
            AntiThrashDisposition::Normal => None,
        }
    };
    let moonpool_until = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(cfg);
        let mut now = Instant::now();
        let last = drive_one(&shared.inner, &mut now);
        match shared.inner.lock().anti_thrash_tick(last) {
            AntiThrashDisposition::Cooldown { until } => {
                Some(until.saturating_duration_since(last))
            }
            AntiThrashDisposition::Normal => None,
        }
    };
    let tokio_d = tokio_until.expect("tokio cooldown active");
    let moonpool_d = moonpool_until.expect("moonpool cooldown active");
    assert!(
        tokio_d >= Duration::from_secs(29) && moonpool_d >= Duration::from_secs(29),
        "both engines must honour the ≈30 s cooldown floor (tokio={tokio_d:?}, moonpool={moonpool_d:?})"
    );
}

fn drive_one(inner: &parking_lot::Mutex<magnetar_proto::Connection>, now: &mut Instant) -> Instant {
    let mut last_drop = *now;
    for i in 0..3u32 {
        let mut conn = inner.lock();
        if i > 0 {
            conn.reset();
        }
        conn.begin_handshake().expect("begin_handshake");
        conn.handle_bytes(*now, &handshake_response_bytes())
            .expect("handshake");
        let request_id = conn.peek_next_request_id_for_test();
        let _h = conn.create_producer(CreateProducerRequest {
            topic: "persistent://public/default/diff-anti-thrash".to_owned(),
            ..Default::default()
        });
        let _ = conn.poll_transmit();
        let attach_now = *now + Duration::from_millis(1);
        conn.handle_bytes(attach_now, &producer_success_bytes(request_id))
            .expect("producer success");
        let drop_now = attach_now + Duration::from_millis(10);
        conn.mark_disconnected();
        conn.record_reattach_outcome(
            drop_now,
            magnetar_proto::ReAttachHandle::Producer(magnetar_proto::ProducerHandle(0)),
            magnetar_proto::ReAttachOutcomeKind::TcpDropAfterReAttach,
        );
        last_drop = drop_now;
        *now = drop_now + Duration::from_millis(20);
    }
    last_drop
}
