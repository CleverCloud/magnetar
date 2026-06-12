// SPDX-License-Identifier: Apache-2.0

//! Moonpool mirror of
//! `magnetar-runtime-tokio/tests/lookup_reset_race.rs`.
//!
//! Pins the ADR-0024 cross-runtime parity contract for the lookup
//! multi-agent review HIGH-3 fix: every supervised-reset boundary
//! surfaces `OpOutcome::SessionLost` on every in-flight lookup +
//! partitioned-metadata request synchronously, regardless of which
//! engine drives the connection. The moonpool engine's virtual clock
//! lets us assert the "no 30-second wait" property structurally —
//! no host-clock `Instant::now()` is consulted; `reset()` runs and
//! the outcome is observable immediately, **at the same synthetic
//! tick**.
//!
//! Strategy (per moonpool-side `tests/common/mod.rs` doc): drive
//! [`ConnectionShared`] directly, no driver task, no TCP listener,
//! all timestamps synthetic.

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
use magnetar_runtime_moonpool::{Client, ClientError, MoonpoolEngine};
use moonpool_core::TokioProviders;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod common;
use common::handshake_complete_shared;

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

/// Lookup multi-agent review HIGH-3, moonpool side: an in-flight
/// lookup + partitioned-metadata pair must both observe
/// [`OpOutcome::SessionLost`] on the supervised-reset boundary, at
/// the **same** synthetic instant the reset fires.
///
/// Determinism property: the virtual clock never advances between
/// the `reset()` call and the `take_outcome()` calls — if the proto
/// layer dropped the outcome publish, the test would fail
/// deterministically across every seed. This is the lever ADR-0024's
/// moonpool seed sweep pulls.
#[test]
fn reset_surfaces_session_lost_on_in_flight_lookup_and_partition() {
    let t0 = Instant::now();
    let shared = handshake_complete_shared(t0);

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

    shared.inner.lock().reset();

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
/// registered a waker before `reset` (e.g. the executor never got to
/// `poll` the future before the supervisor kicked in), the outcome
/// MUST still be installed so the eventual `poll` sees `SessionLost`
/// instead of registering a fresh waker that will never fire.
/// Pairs 1:1 with the tokio sibling for ADR-0024 strict-parity.
#[test]
fn reset_publishes_outcome_even_when_no_waker_was_registered() {
    let t0 = Instant::now();
    let shared = handshake_complete_shared(t0);

    let lookup_rid = {
        let mut conn = shared.inner.lock();
        conn.lookup("persistent://public/default/no-waker", false)
    };

    shared.inner.lock().reset();

    let key = PendingOpKey::Request(lookup_rid);
    match shared.inner.lock().take_outcome(key) {
        Some(OpOutcome::SessionLost { key: k }) => assert_eq!(k, key),
        other => panic!("expected SessionLost outcome, got {other:?}"),
    }
}

// ───────────────────────────────────────────────────────────────────────────
// ADR-0060 / follow-ups §4.1 — engine-side bounded lookup-retry on SessionLost.
//
// 1:1 twin of `crates/magnetar-runtime-tokio/tests/lookup_reset_race.rs`'s
// `lookup_severed_by_reconnect_reissues_and_succeeds` /
// `terminal_session_lost_surfaces_peer_closed_without_spin` /
// `flapping_lookup_is_bounded_never_spins` (ADR-0024 runtime-test-parity, must
// stay strictly 1:1). The moonpool engine runs over [`TokioProviders`] against
// a real loopback `TcpListener` (the `terminal_exit.rs` harness pattern) — the
// engine surface differs from tokio (`lookup_topic` is a free `pub async fn`,
// `Client::connect_plain_supervised` is the supervised entry), but the
// bounded-loop contract is identical: an in-flight `CommandLookupTopic` severed
// by a supervised `reset()` is re-issued against the fresh session, bounded by
// `MAX_LOOKUP_SESSION_REISSUES`, with a terminal `SessionLost` short-circuiting
// to `PeerClosed`.
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

/// Scripted broker (1:1 with the tokio twin): every accepted session answers
/// CONNECT / PING; for `CommandLookupTopic`, the shared atomic `lookups_seen`
/// decides per-lookup whether to **respond** (Connect) or **drop the socket
/// without responding**. The first `drop_first_n` lookups across all sessions
/// are dropped-on (severing the in-flight lookup → supervised `reset()` →
/// `SessionLost` → redial); subsequent lookups get a `LookupResponse`.
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

    (format!("{addr}"), accepts, lookups_seen)
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
                        let n = lookups_seen.fetch_add(1, Ordering::SeqCst) + 1;
                        if n <= drop_first_n {
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

/// (i) Moonpool twin: a producer open whose in-flight lookup hits ONE transient
/// `SessionLost` during a supervised reconnect SUCCEEDS transparently — no
/// `ClientError::Other("unexpected lookup outcome: SessionLost…")` leak.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_severed_by_reconnect_reissues_and_succeeds() {
    let (addr, accepts, lookups_seen) = spawn_lookup_flap_broker(1).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());

    let config = ConnectionConfig {
        supervisor: Some(supervisor(Some(32))),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect_plain_supervised(&engine, &addr, config, None, None),
    )
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

/// (ii) Moonpool twin: a TERMINAL `SessionLost` — supervisor gives up
/// (`max_attempts` exhausted, `no_driver` latched) while the lookup is parked
/// — surfaces `ClientError::PeerClosed`, without spinning to the bound or
/// hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_session_lost_surfaces_peer_closed_without_spin() {
    let (addr, _accepts, _lookups_seen) = spawn_lookup_flap_broker(u32::MAX).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());

    let config = ConnectionConfig {
        supervisor: Some(supervisor(Some(2))),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect_plain_supervised(&engine, &addr, config, None, None),
    )
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

/// (iii) Moonpool twin: flapping the lookup-severing drop beyond
/// `MAX_LOOKUP_SESSION_REISSUES` terminates with `PeerClosed`, never an
/// unbounded spin; a single `open_producer` issues at most
/// `MAX_LOOKUP_SESSION_REISSUES + 1` lookups.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flapping_lookup_is_bounded_never_spins() {
    let drops = u32::from(MAX_LOOKUP_SESSION_REISSUES) + 1;
    let (addr, _accepts, lookups_seen) = spawn_lookup_flap_broker(u32::MAX).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());

    let config = ConnectionConfig {
        supervisor: Some(supervisor(Some(drops + 8))),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect_plain_supervised(&engine, &addr, config, None, None),
    )
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
