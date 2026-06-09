// SPDX-License-Identifier: Apache-2.0

//! Integration test for the lookup multi-agent review HIGH-3 finding:
//! a lookup or partitioned-metadata request that is in flight when
//! `Connection::reset` fires (e.g. supervised reconnect) MUST surface
//! `OpOutcome::SessionLost` to the user's future immediately. Without
//! the explicit drain in `reset`, the future stays parked on its
//! per-request waker until the runtime's `operation_timeout` (default
//! 30s) fires.
//!
//! Strategy (mirrors `reconnect_with_inflight.rs`): drive the tokio
//! engine's [`ConnectionShared`] directly — no TCP listener, no driver
//! task — and assert the outcome lands within the `take_outcome` slot
//! immediately after `reset`. That's the engine-surface contract the
//! production driver depends on; the wire-level smoke test for the
//! same race lives in `crates/magnetar/tests/e2e_lookup_reset_race.rs`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::lookup::MAX_LOOKUP_SESSION_REISSUES;
use magnetar_proto::{
    AntiThrashThreshold, ConnectionConfig, CreateProducerRequest, Frame, FrameError, OpOutcome,
    PendingOpKey, SupervisorConfig, decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::{Client, ClientError, ConnectionShared};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod common;
use common::handshake_response_bytes;

fn handshake_complete(at: Instant) -> Arc<ConnectionShared> {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(at, &handshake_response_bytes())
            .expect("connected");
        let _ = conn.poll_event();
    }
    shared
}

/// Lock-free counter waker — fires `wake_count.fetch_add(1)` each time
/// the proto layer wakes the future, so we can assert the synchronous
/// wake-up contract on `reset`.
struct CountingWake(AtomicUsize);

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn counting_waker() -> (Arc<CountingWake>, Waker) {
    let inner = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker: Waker = Arc::clone(&inner).into();
    (inner, waker)
}

/// Lookup multi-agent review HIGH-3: an in-flight lookup +
/// partitioned-metadata pair must both observe
/// [`OpOutcome::SessionLost`] on the supervised-reset boundary,
/// **synchronously**. The runtime's `RequestFut` would
/// otherwise sit on its waker until the engine's `operation_timeout`
/// fires (the production default is 30s); the fix in
/// `magnetar-proto`'s `Connection::reset` publishes the outcome +
/// wakes the waker before the registry is cleared, so the future's
/// next `take_outcome` poll resolves immediately.
#[test]
fn reset_surfaces_session_lost_on_in_flight_lookup_and_partition() {
    let t0 = Instant::now();
    let shared = handshake_complete(t0);

    // Issue two in-flight registry requests. The runtime's
    // `lookup_topic` / `get_partitioned_topic_metadata` calls allocate
    // exactly these keys against `pending_requests` + the lookup
    // registry; we register a waker the same way the runtime's
    // `RequestFut::poll` does.
    let (lookup_rid, partition_rid, lookup_counter, partition_counter) = {
        let mut conn = shared.inner.lock();
        let lookup_rid = conn.lookup("persistent://public/default/foo", false);
        let partition_rid = conn.get_partitioned_topic_metadata("persistent://public/default/bar");
        let (lookup_counter, lookup_waker) = counting_waker();
        let (partition_counter, partition_waker) = counting_waker();
        conn.register_waker(PendingOpKey::Request(lookup_rid), lookup_waker);
        conn.register_waker(PendingOpKey::Request(partition_rid), partition_waker);
        (lookup_rid, partition_rid, lookup_counter, partition_counter)
    };

    // Trigger the supervised-reconnect boundary.
    shared.inner.lock().reset();

    // (1) Both wakers fired exactly once — the runtime's executor will
    // schedule the futures' next poll, which sees `SessionLost`.
    assert_eq!(
        lookup_counter.0.load(Ordering::SeqCst),
        1,
        "lookup waker must fire exactly once on reset"
    );
    assert_eq!(
        partition_counter.0.load(Ordering::SeqCst),
        1,
        "partitioned-metadata waker must fire exactly once on reset"
    );

    // (2) `OpOutcome::SessionLost` is published synchronously — there
    // is no 30-second wait for `operation_timeout` to fire.
    {
        let mut conn = shared.inner.lock();
        let lookup_key = PendingOpKey::Request(lookup_rid);
        let partition_key = PendingOpKey::Request(partition_rid);
        match conn.take_outcome(lookup_key) {
            Some(OpOutcome::SessionLost { key }) => assert_eq!(key, lookup_key),
            other => panic!("expected SessionLost on lookup rid, got {other:?}"),
        }
        match conn.take_outcome(partition_key) {
            Some(OpOutcome::SessionLost { key }) => assert_eq!(key, partition_key),
            other => panic!("expected SessionLost on partitioned-metadata rid, got {other:?}"),
        }
    }
}

/// Companion to the test above: even when the user's future never
/// registered a waker before `reset` (e.g. the future was constructed
/// but the runtime never got to `poll` it before the supervisor
/// kicked in), the outcome MUST still be installed so the eventual
/// `poll` sees `SessionLost` rather than parking on a freshly-
/// registered waker. Mirrors the same invariant on the moonpool side.
#[test]
fn reset_publishes_outcome_even_when_no_waker_was_registered() {
    let t0 = Instant::now();
    let shared = handshake_complete(t0);

    let lookup_rid = {
        let mut conn = shared.inner.lock();
        conn.lookup("persistent://public/default/no-waker", false)
    };

    // No `register_waker` call — simulates the runtime constructing
    // the future but not having polled it yet.
    shared.inner.lock().reset();

    // The outcome MUST be present so the eventual poll sees it on its
    // first `take_outcome` call rather than registering a waker that
    // will never fire.
    let key = PendingOpKey::Request(lookup_rid);
    match shared.inner.lock().take_outcome(key) {
        Some(OpOutcome::SessionLost { key: k }) => assert_eq!(k, key),
        other => panic!("expected SessionLost outcome, got {other:?}"),
    }
}

// ───────────────────────────────────────────────────────────────────────────
// ADR-0060 / follow-ups §4.1 — engine-side bounded lookup-retry on SessionLost.
//
// These drive the production async `lookup_topic` retry loop through a real
// loopback broker + the supervised driver: a `CommandLookupTopic` that is
// in flight when the broker drops the socket is severed by the supervisor's
// `reset()` (→ `OpOutcome::SessionLost`), and the engine re-issues it against
// the fresh session instead of leaking `ClientError::Other`. 1:1 twin of the
// moonpool engine's same-named tests (ADR-0024 runtime-test-parity).
// ───────────────────────────────────────────────────────────────────────────

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "lookup-retry-broker/0".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_pong(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Pong as i32,
        pong: Some(pb::CommandPong {}),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_lookup_response(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::LookupResponse as i32,
        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
            broker_service_url: None,
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
            request_id,
            authoritative: Some(true),
            error: None,
            message: None,
            proxy_through_service_url: Some(false),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_producer_success(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: "lookup-retry-producer".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Supervisor schedule tuned for fast loopback runs: tiny backoff so the
/// redial body dominates, but `max_attempts` bounded so the "supervisor gives
/// up" terminal path is reachable in the flap tests.
fn supervisor(max_attempts: Option<u32>) -> SupervisorConfig {
    SupervisorConfig {
        initial_backoff: Duration::from_millis(2),
        max_backoff: Duration::from_millis(20),
        mandatory_stop: Duration::from_secs(60),
        max_attempts,
        anti_thrash_threshold: Some(AntiThrashThreshold {
            successful_attaches: 8,
            window: Duration::from_secs(5),
            drop_within: Duration::from_millis(50),
        }),
        drop_grace: Duration::from_millis(200),
        max_backoff_after_thrash: Duration::from_millis(30),
    }
}

/// Scripted broker for the lookup-retry scenarios. Every accepted session
/// answers CONNECT / PING. For `CommandLookupTopic`, the shared atomic
/// `lookups_seen` decides per-lookup whether to **respond** (Connect) or
/// **drop the socket without responding** — a drop severs the in-flight
/// lookup, the supervisor redials, and the broker re-accepts on the next loop.
///
/// `drop_first_n` lookups are dropped-on; the `(drop_first_n + 1)`-th and
/// beyond get a `LookupResponse`. `accepts`/`lookups_seen` are observable so
/// the test can assert the re-issue actually round-tripped a fresh session.
async fn spawn_lookup_flap_broker(drop_first_n: u32) -> (String, Arc<AtomicU32>, Arc<AtomicU32>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let accepts = Arc::new(AtomicU32::new(0));
    let lookups_seen = Arc::new(AtomicU32::new(0));

    let accepts_task = accepts.clone();
    let lookups_task = lookups_seen.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            accepts_task.fetch_add(1, Ordering::SeqCst);
            let lookups = lookups_task.clone();
            tokio::spawn(async move {
                let _ = handle_lookup_flap_session(stream, lookups, drop_first_n).await;
            });
        }
    });

    (format!("pulsar://{addr}"), accepts, lookups_seen)
}

async fn handle_lookup_flap_session(
    mut stream: tokio::net::TcpStream,
    lookups_seen: Arc<AtomicU32>,
    drop_first_n: u32,
) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    loop {
        let mut drop_now = false;
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame: Frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return Ok(()),
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
                continue;
            };
            match kind {
                pb::base_command::Type::Connect => emit_connected(&mut out_buf),
                pb::base_command::Type::Ping => emit_pong(&mut out_buf),
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        // 1-based ordinal of this lookup across ALL sessions.
                        let n = lookups_seen.fetch_add(1, Ordering::SeqCst) + 1;
                        if n <= drop_first_n {
                            // Sever the in-flight lookup: drop WITHOUT answering.
                            drop_now = true;
                            break;
                        }
                        emit_lookup_response(&mut out_buf, l.request_id);
                    }
                }
                pb::base_command::Type::Producer => {
                    if let Some(p) = &frame.command.producer {
                        emit_producer_success(&mut out_buf, p.request_id);
                    }
                }
                _ => {}
            }
        }

        if drop_now {
            // Drop the socket so the client's supervisor observes the terminal
            // peer close, calls `reset()` (→ SessionLost on the in-flight
            // lookup), and redials against the listener.
            return Ok(());
        }

        if !out_buf.is_empty() {
            stream.write_all(&out_buf).await?;
            stream.flush().await?;
            out_buf.clear();
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(e) => return Err(e),
        }
    }
}

/// (i) A producer open whose in-flight lookup hits ONE transient `SessionLost`
/// during a supervised reconnect must SUCCEED transparently — no
/// `ClientError::Other("unexpected lookup outcome: SessionLost…")` leak. The
/// broker drops on the first lookup, the supervisor redials, and the engine
/// re-issues the lookup against the fresh session; the second lookup is
/// answered and the producer opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_severed_by_reconnect_reissues_and_succeeds() {
    let (url, accepts, lookups_seen) = spawn_lookup_flap_broker(1).await;

    let config = ConnectionConfig {
        supervisor: Some(supervisor(Some(32))),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(Duration::from_secs(5), Client::connect(&url, config))
        .await
        .expect("connect did not time out")
        .expect("connect must succeed");

    let producer = tokio::time::timeout(
        Duration::from_secs(10),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/lookup-retry-transient".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer must resolve promptly via the re-issued lookup, not hang")
    .expect("open_producer must SUCCEED transparently after one transient SessionLost");

    drop(producer);

    // The transparent recovery genuinely round-tripped a fresh session: the
    // supervisor redialed (>= 2 accepts) and the engine re-issued the lookup
    // (>= 2 lookups across sessions: the dropped one + the answered one).
    assert!(
        accepts.load(Ordering::SeqCst) >= 2,
        "supervisor must have redialed after the lookup-severing drop"
    );
    assert!(
        lookups_seen.load(Ordering::SeqCst) >= 2,
        "the engine must have RE-ISSUED the lookup on the fresh session"
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

/// (ii) A TERMINAL `SessionLost` — the supervisor gives up (exhausts its
/// `max_attempts`, latches `no_driver`) while the lookup is parked waiting to
/// re-issue — must surface `ClientError::PeerClosed`. It must NOT loop to the
/// reissue bound and must NOT hang: the tight timeout is the no-hang guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_session_lost_surfaces_peer_closed_without_spin() {
    // Drop on EVERY lookup so no re-issue ever succeeds; bound the supervisor to
    // a couple of attempts so it gives up (no_driver latched) → the engine loop
    // takes the `Terminal` branch → PeerClosed.
    let (url, _accepts, _lookups_seen) = spawn_lookup_flap_broker(u32::MAX).await;

    let config = ConnectionConfig {
        supervisor: Some(supervisor(Some(2))),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(Duration::from_secs(5), Client::connect(&url, config))
        .await
        .expect("connect did not time out")
        .expect("connect must succeed");

    let res = tokio::time::timeout(
        Duration::from_secs(10),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/lookup-retry-terminal".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("a terminal SessionLost must surface PeerClosed PROMPTLY, not hang or spin");

    assert!(
        matches!(res, Err(ClientError::PeerClosed)),
        "terminal SessionLost (supervisor gave up, no_driver latched) must map to \
         PeerClosed, got {res:?}",
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

/// (iii) Flap the lookup-severing drop more than `MAX_LOOKUP_SESSION_REISSUES`
/// times: the caller must end with `PeerClosed` (the supervisor eventually
/// gives up, or the reissue budget is exhausted) — never an unbounded spin. The
/// budget is the ceiling, so the broker is allowed at most
/// `MAX_LOOKUP_SESSION_REISSUES + 1` lookups from a single `open_producer`
/// before it resolves one way or the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flapping_lookup_is_bounded_never_spins() {
    let drops = u32::from(MAX_LOOKUP_SESSION_REISSUES) + 1;
    let (url, _accepts, lookups_seen) = spawn_lookup_flap_broker(u32::MAX).await;

    // Supervisor budget large enough to outlast the reissue bound, so it is the
    // ENGINE-side reissue cap (not the supervisor) that bounds the loop here —
    // unless the supervisor gives up first; either way the result is PeerClosed.
    let config = ConnectionConfig {
        supervisor: Some(supervisor(Some(drops + 8))),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(Duration::from_secs(5), Client::connect(&url, config))
        .await
        .expect("connect did not time out")
        .expect("connect must succeed");

    let res = tokio::time::timeout(
        Duration::from_secs(15),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/lookup-retry-flap".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("a persistently flapping lookup must terminate (PeerClosed), never spin forever");

    assert!(
        matches!(res, Err(ClientError::PeerClosed)),
        "a flapping lookup beyond the reissue bound must surface PeerClosed, got {res:?}",
    );

    // Bounded: a single `open_producer` cannot have issued more than
    // `MAX_LOOKUP_SESSION_REISSUES + 1` lookups (the initial + one per allowed
    // re-issue) before short-circuiting. This is the "does not spin to infinity"
    // structural witness.
    let total_lookups = lookups_seen.load(Ordering::SeqCst);
    assert!(
        total_lookups <= u32::from(MAX_LOOKUP_SESSION_REISSUES) + 1,
        "lookup re-issues must be bounded by MAX_LOOKUP_SESSION_REISSUES + 1, \
         observed {total_lookups}",
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}
