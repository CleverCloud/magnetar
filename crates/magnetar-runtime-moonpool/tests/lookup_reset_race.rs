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
use common::{HANG_GUARD, handshake_complete_shared};

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
// ADR-0060 — engine-side bounded lookup-retry on SessionLost.
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
        mandatory_stop: Duration::from_mins(1),
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
        HANG_GUARD,
        Client::connect_plain_supervised(&engine, &addr, config, None, None),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed");

    let producer = tokio::time::timeout(
        HANG_GUARD,
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

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("stalled-reconnect broker bind");
    let stalled_addr = listener.local_addr().expect("stalled listener addr");
    let stalled_lookups = Arc::new(AtomicU32::new(0));
    tokio::spawn(async move {
        let Ok((first, _peer)) = listener.accept().await else {
            return;
        };
        let _ = handle_lookup_flap_session(first, stalled_lookups, 1).await;
        let Ok((_second, _peer)) = listener.accept().await else {
            return;
        };
        tokio::time::sleep(HANG_GUARD).await;
    });

    let config = ConnectionConfig {
        operation_timeout: Duration::from_millis(50),
        supervisor: Some(supervisor(Some(32))),
        ..ConnectionConfig::default()
    };
    let client =
        Client::connect_plain_supervised(&engine, &stalled_addr.to_string(), config, None, None)
            .await
            .expect("initial stalled-reconnect session must connect");
    let error = client
        .lookup_topic(
            "persistent://public/default/lookup-reconnect-deadline",
            false,
        )
        .await
        .expect_err("stalled reconnect must be bounded by operation_timeout");
    assert!(
        matches!(error, ClientError::Other(ref message)
            if message == "topic lookup exceeded operation_timeout"),
        "unexpected reconnect deadline error: {error:?}"
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
        HANG_GUARD,
        Client::connect_plain_supervised(&engine, &addr, config, None, None),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed");

    let res = tokio::time::timeout(
        HANG_GUARD,
        client.lookup_topic("persistent://public/default/lookup-retry-terminal", false),
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
        HANG_GUARD,
        Client::connect_plain_supervised(&engine, &addr, config, None, None),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed");

    let res = tokio::time::timeout(
        HANG_GUARD,
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

// ───────────────────────────────────────────────────────────────────────────
// Partitioned-metadata and topic-list-snapshot requests severed by a reconnect.
// 1:1 twin of the tokio engine's same-named tests (ADR-0024).
//
// `lookup_topic` re-issues a request that `Connection::reset` failed with
// `OpOutcome::SessionLost` (ADR-0060). The two other registry requests a user
// call issues, `CommandPartitionedTopicMetadata` (the first step of
// `producer().create()`) and `CommandWatchTopicList` (the first step of a
// pattern-consumer subscribe), are failed the same way by `reset()`, but their
// engine loops have no `SessionLost` arm: the outcome falls into the
// `other =>` catch-all and is returned as `ClientError::Other`. A call issued
// while the client is reconnecting therefore fails instead of resolving
// transparently on the fresh session.
// ───────────────────────────────────────────────────────────────────────────

const METADATA_REISSUE_PARTITIONS: u32 = 3;

fn metadata_reissue_topics() -> Vec<String> {
    vec![
        "persistent://public/default/topic-list-reissue-a".to_owned(),
        "persistent://public/default/topic-list-reissue-b".to_owned(),
    ]
}

fn emit_partitioned_metadata_response(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::PartitionedMetadataResponse as i32,
        partition_metadata_response: Some(pb::CommandPartitionedTopicMetadataResponse {
            partitions: Some(METADATA_REISSUE_PARTITIONS),
            request_id,
            response: Some(
                pb::command_partitioned_topic_metadata_response::LookupType::Success as i32,
            ),
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_watch_topic_list_success(out: &mut BytesMut, request_id: u64, watcher_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::WatchTopicListSuccess as i32,
        watch_topic_list_success: Some(pb::CommandWatchTopicListSuccess {
            request_id,
            watcher_id,
            topic: metadata_reissue_topics(),
            topics_hash: "reissue-hash".to_owned(),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Scripted broker for the partitioned-metadata / topic-list scenarios. Same
/// shape as [`spawn_lookup_flap_broker`], but the counted request is the
/// `CommandPartitionedTopicMetadata` or `CommandWatchTopicList` frame: the
/// first `drop_first_n` of them (across all sessions) sever the socket without
/// an answer, every later one is answered.
async fn spawn_metadata_flap_broker(drop_first_n: u32) -> (String, Arc<AtomicU32>, Arc<AtomicU32>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let accepts = Arc::new(AtomicU32::new(0));
    let requests_seen = Arc::new(AtomicU32::new(0));

    let accepts_task = accepts.clone();
    let requests_task = requests_seen.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            accepts_task.fetch_add(1, Ordering::SeqCst);
            let requests = requests_task.clone();
            tokio::spawn(async move {
                let _ = handle_metadata_flap_session(stream, requests, drop_first_n).await;
            });
        }
    });

    (format!("{addr}"), accepts, requests_seen)
}

async fn handle_metadata_flap_session(
    mut stream: tokio::net::TcpStream,
    requests_seen: Arc<AtomicU32>,
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
                pb::base_command::Type::PartitionedMetadata => {
                    if let Some(p) = &frame.command.partition_metadata {
                        let n = requests_seen.fetch_add(1, Ordering::SeqCst) + 1;
                        if n <= drop_first_n {
                            drop_now = true;
                            break;
                        }
                        emit_partitioned_metadata_response(&mut out_buf, p.request_id);
                    }
                }
                pb::base_command::Type::WatchTopicList => {
                    if let Some(w) = &frame.command.watch_topic_list {
                        let n = requests_seen.fetch_add(1, Ordering::SeqCst) + 1;
                        if n <= drop_first_n {
                            drop_now = true;
                            break;
                        }
                        emit_watch_topic_list_success(&mut out_buf, w.request_id, w.watcher_id);
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

/// Moonpool twin. Control for the scripted broker: with no severing drop, the partitioned
/// metadata round-trip is answered and the engine returns the partition count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partitioned_metadata_answered_without_reconnect() {
    let (addr, _accepts, requests_seen) = spawn_metadata_flap_broker(0).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let config = ConnectionConfig {
        supervisor: Some(supervisor(Some(32))),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_plain_supervised(&engine, &addr, config, None, None),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed");

    let res = tokio::time::timeout(
        HANG_GUARD,
        client.partitioned_topic_metadata("persistent://public/default/metadata-control"),
    )
    .await
    .expect("partitioned_topic_metadata must resolve promptly");

    assert!(
        matches!(res, Ok(METADATA_REISSUE_PARTITIONS)),
        "an unsevered partitioned-metadata request must be answered, got {res:?}",
    );
    assert_eq!(requests_seen.load(Ordering::SeqCst), 1);

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

/// Moonpool twin: a `CommandPartitionedTopicMetadata` that is in flight when the broker
/// drops the socket is severed by the supervisor's `reset()`
/// (`OpOutcome::SessionLost`). It must be re-issued against the fresh session
/// like a lookup is (ADR-0060), so the caller gets the partition count. Fails
/// if it surfaces `ClientError::Other("unexpected partitioned metadata outcome:
/// SessionLost…")`, which is how `producer().create()` failed mid-reconnect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partitioned_metadata_severed_by_reconnect_reissues_and_succeeds() {
    let (addr, accepts, requests_seen) = spawn_metadata_flap_broker(1).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());

    let config = ConnectionConfig {
        supervisor: Some(supervisor(Some(32))),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_plain_supervised(&engine, &addr, config, None, None),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed");

    let res = tokio::time::timeout(
        HANG_GUARD,
        client.partitioned_topic_metadata("persistent://public/default/metadata-reissue"),
    )
    .await
    .expect("partitioned_topic_metadata must resolve promptly via the re-issued request");

    assert!(
        matches!(res, Ok(METADATA_REISSUE_PARTITIONS)),
        "a partitioned-metadata request severed by a supervised reconnect must be \
         re-issued on the fresh session and succeed, got {res:?}",
    );

    // The recovery genuinely round-tripped a fresh session.
    assert!(
        accepts.load(Ordering::SeqCst) >= 2,
        "supervisor must have redialed after the severing drop"
    );
    assert!(
        requests_seen.load(Ordering::SeqCst) >= 2,
        "the engine must have RE-ISSUED the partitioned-metadata request"
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

/// Moonpool twin. Control for the scripted broker: with no severing drop, the topic-list
/// snapshot is answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_list_snapshot_answered_without_reconnect() {
    let (addr, _accepts, requests_seen) = spawn_metadata_flap_broker(0).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let config = ConnectionConfig {
        supervisor: Some(supervisor(Some(32))),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_plain_supervised(&engine, &addr, config, None, None),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed");

    let res = tokio::time::timeout(
        HANG_GUARD,
        client.watch_topic_list("public/default", "persistent://public/default/.*"),
    )
    .await
    .expect("watch_topic_list must resolve promptly");

    assert!(
        matches!(&res, Ok(topics) if *topics == metadata_reissue_topics()),
        "an unsevered topic-list request must be answered, got {res:?}",
    );
    assert_eq!(requests_seen.load(Ordering::SeqCst), 1);

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

/// Moonpool twin: the watcher twin of the test above. A `CommandWatchTopicList` whose
/// snapshot is severed by a supervised reconnect (`OpOutcome::SessionLost`)
/// must be re-issued on the fresh session. Fails if
/// `PatternConsumerBuilder::subscribe` / `topic_list_snapshot` gets
/// `ClientError::Other("unexpected topic-list snapshot outcome: SessionLost…")`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_list_snapshot_severed_by_reconnect_reissues_and_succeeds() {
    let (addr, accepts, requests_seen) = spawn_metadata_flap_broker(1).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());

    let config = ConnectionConfig {
        supervisor: Some(supervisor(Some(32))),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_plain_supervised(&engine, &addr, config, None, None),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed");

    let res = tokio::time::timeout(
        HANG_GUARD,
        client.watch_topic_list("public/default", "persistent://public/default/.*"),
    )
    .await
    .expect("watch_topic_list must resolve promptly via the re-issued request");

    assert!(
        matches!(&res, Ok(topics) if *topics == metadata_reissue_topics()),
        "a topic-list snapshot severed by a supervised reconnect must be re-issued \
         on the fresh session and return the topics, got {res:?}",
    );

    assert!(
        accepts.load(Ordering::SeqCst) >= 2,
        "supervisor must have redialed after the severing drop"
    );
    assert!(
        requests_seen.load(Ordering::SeqCst) >= 2,
        "the engine must have RE-ISSUED the topic-list request"
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Topic-list snapshot failure paths: a request the broker refuses, answers
// with a reply of another kind, never answers, loses with an unsupervised
// connection, or keeps losing to reconnects, and a reconnect that never
// completes or never comes. 1:1 twin of the tokio engine's same-named tests
// (ADR-0024).
// ───────────────────────────────────────────────────────────────────────────

const WATCH_NAMESPACE: &str = "public/default";
const WATCH_PATTERN: &str = "persistent://public/default/.*";

/// How [`spawn_watch_broker`] answers `CommandWatchTopicList`.
#[derive(Clone, Copy)]
enum WatchReply {
    /// Drop the socket without answering.
    Sever,
    /// Answer with `CommandError`.
    Refuse,
    /// Answer with a bare `CommandSuccess`, which is no topic-list reply.
    Mismatched,
    /// Never answer.
    Silent,
}

/// Install, once per test binary, a subscriber that enables every event and discards the
/// output, so the fields of the `warn!` and `debug!` events on the re-issue path are evaluated
/// as they are in a deployment that records them.
fn log_every_event() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::TRACE)
        .with_writer(std::io::sink)
        .try_init();
}

fn emit_command_error(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Error as i32,
        error: Some(pb::CommandError {
            request_id,
            error: pb::ServerError::AuthorizationError as i32,
            message: "topic-list watch refused by the scripted broker".to_owned(),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_command_success(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// What [`spawn_watch_broker`] does with the sessions after the first.
#[derive(Clone, Copy)]
enum Redial {
    /// Every session answers CONNECT.
    Accepted,
    /// Later sessions accept the socket and never answer CONNECT, so a supervised redial reaches
    /// a fresh session that never becomes live.
    NeverCompletes,
    /// The second session closes the socket on CONNECT and every later dial is refused, so the
    /// supervisor runs out of attempts after its first redial.
    Refused,
}

/// Scripted broker that answers every `CommandWatchTopicList` as `reply` says, and the sessions
/// after the first as `redial` says. Returns the address and the count of `CommandWatchTopicList`
/// frames seen.
async fn spawn_watch_broker(reply: WatchReply, redial: Redial) -> (String, Arc<AtomicU32>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let requests_seen = Arc::new(AtomicU32::new(0));

    let requests_task = requests_seen.clone();
    tokio::spawn(async move {
        let mut session = 0u32;
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            let handshakes = session == 0 || matches!(redial, Redial::Accepted);
            let close_on_connect = session > 0 && matches!(redial, Redial::Refused);
            session = session.saturating_add(1);
            let requests = requests_task.clone();
            tokio::spawn(async move {
                let _ = handle_watch_session(stream, requests, reply, handshakes, close_on_connect)
                    .await;
            });
            if close_on_connect {
                // Dropping the listener refuses every later dial.
                return;
            }
        }
    });

    (format!("{addr}"), requests_seen)
}

async fn handle_watch_session(
    mut stream: tokio::net::TcpStream,
    requests_seen: Arc<AtomicU32>,
    reply: WatchReply,
    handshakes: bool,
    close_on_connect: bool,
) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    loop {
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
                pb::base_command::Type::Connect if handshakes => emit_connected(&mut out_buf),
                pb::base_command::Type::Connect if close_on_connect => return Ok(()),
                pb::base_command::Type::Ping => emit_pong(&mut out_buf),
                pb::base_command::Type::WatchTopicList => {
                    if let Some(w) = &frame.command.watch_topic_list {
                        requests_seen.fetch_add(1, Ordering::SeqCst);
                        match reply {
                            WatchReply::Sever => return Ok(()),
                            WatchReply::Refuse => emit_command_error(&mut out_buf, w.request_id),
                            WatchReply::Mismatched => {
                                emit_command_success(&mut out_buf, w.request_id);
                            }
                            WatchReply::Silent => {}
                        }
                    }
                }
                _ => {}
            }
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

/// A supervised client of `addr` whose every setup operation is bounded by `operation_timeout`.
async fn supervised_watch_client(
    engine: &MoonpoolEngine<TokioProviders>,
    addr: &str,
    max_attempts: Option<u32>,
    operation_timeout: Duration,
) -> Client<TokioProviders> {
    let config = ConnectionConfig {
        supervisor: Some(supervisor(max_attempts)),
        operation_timeout,
        ..ConnectionConfig::default()
    };
    tokio::time::timeout(
        HANG_GUARD,
        Client::connect_plain_supervised(engine, addr, config, None, None),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed")
}

/// Whether `err` is this engine's operation-deadline error for `operation`.
fn is_operation_deadline_error(err: &ClientError, operation: &str) -> bool {
    matches!(err, ClientError::Other(message)
        if *message == format!("{operation} exceeded operation_timeout"))
}

/// Moonpool twin: a topic-list snapshot severed by every reconnect is re-issued at most
/// `MAX_LOOKUP_SESSION_REISSUES` times and then surfaces `PeerClosed`, never an unbounded spin.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flapping_topic_list_snapshot_is_bounded_never_spins() {
    log_every_event();
    let drops = u32::from(MAX_LOOKUP_SESSION_REISSUES) + 1;
    let (addr, requests_seen) = spawn_watch_broker(WatchReply::Sever, Redial::Accepted).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client =
        supervised_watch_client(&engine, &addr, Some(drops + 8), Duration::from_secs(30)).await;

    let res = tokio::time::timeout(
        HANG_GUARD,
        client.watch_topic_list(WATCH_NAMESPACE, WATCH_PATTERN),
    )
    .await
    .expect("a persistently severed topic-list snapshot must terminate, never spin forever");

    assert!(
        matches!(res, Err(ClientError::PeerClosed)),
        "a topic-list snapshot severed beyond the re-issue bound must surface PeerClosed, \
         got {res:?}",
    );
    assert_eq!(
        requests_seen.load(Ordering::SeqCst),
        u32::from(MAX_LOOKUP_SESSION_REISSUES) + 1,
        "the first request and MAX_LOOKUP_SESSION_REISSUES re-issues, then the cap surfaces \
         PeerClosed",
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

/// Moonpool twin: a topic-list snapshot severed while the supervisor gives up (`max_attempts`
/// exhausted, `no_driver` latched) while it waits for the fresh session surfaces `PeerClosed`
/// promptly: the redial reaches a session that closes on CONNECT, and every later dial is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_list_snapshot_severed_for_good_surfaces_peer_closed() {
    let (addr, _requests_seen) = spawn_watch_broker(WatchReply::Sever, Redial::Refused).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = supervised_watch_client(&engine, &addr, Some(2), Duration::from_secs(30)).await;

    let res = tokio::time::timeout(
        HANG_GUARD,
        client.watch_topic_list(WATCH_NAMESPACE, WATCH_PATTERN),
    )
    .await
    .expect("a topic-list snapshot on a connection that went terminal must resolve promptly");

    assert!(
        matches!(res, Err(ClientError::PeerClosed)),
        "a topic-list snapshot whose connection the supervisor gave up on must surface \
         PeerClosed, got {res:?}",
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

/// Moonpool twin: a topic-list snapshot severed by a reconnect whose fresh session never
/// completes its handshake waits for that session no longer than `operation_timeout`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_list_snapshot_waiting_for_a_reconnect_honours_operation_timeout() {
    let (addr, requests_seen) = spawn_watch_broker(WatchReply::Sever, Redial::NeverCompletes).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = supervised_watch_client(&engine, &addr, None, Duration::from_millis(500)).await;

    let res = tokio::time::timeout(
        HANG_GUARD,
        client.watch_topic_list(WATCH_NAMESPACE, WATCH_PATTERN),
    )
    .await
    .expect("the wait for a reconnect must be bounded by operation_timeout");

    assert!(
        matches!(&res, Err(err) if is_operation_deadline_error(err, "topic-list snapshot")),
        "a topic-list snapshot waiting for a reconnect that never completes must fail with the \
         operation-deadline error, got {res:?}",
    );
    assert_eq!(
        requests_seen.load(Ordering::SeqCst),
        1,
        "the request was never re-issued: no fresh session became live"
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

/// Moonpool twin: a topic-list snapshot the broker never answers fails with the
/// operation-deadline error once `operation_timeout` elapses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unanswered_topic_list_snapshot_honours_operation_timeout() {
    let (addr, requests_seen) = spawn_watch_broker(WatchReply::Silent, Redial::Accepted).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client =
        supervised_watch_client(&engine, &addr, Some(32), Duration::from_millis(500)).await;

    let res = tokio::time::timeout(
        HANG_GUARD,
        client.watch_topic_list(WATCH_NAMESPACE, WATCH_PATTERN),
    )
    .await
    .expect("an unanswered topic-list snapshot must be bounded by operation_timeout");

    assert!(
        matches!(&res, Err(err) if is_operation_deadline_error(err, "topic-list snapshot")),
        "an unanswered topic-list snapshot must fail with the operation-deadline error, \
         got {res:?}",
    );
    assert_eq!(requests_seen.load(Ordering::SeqCst), 1);

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

/// Moonpool twin: a topic-list watch the broker refuses surfaces the broker's error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_topic_list_snapshot_surfaces_the_broker_error() {
    let (addr, _requests_seen) = spawn_watch_broker(WatchReply::Refuse, Redial::Accepted).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = supervised_watch_client(&engine, &addr, Some(32), Duration::from_secs(30)).await;

    let res = tokio::time::timeout(
        HANG_GUARD,
        client.watch_topic_list(WATCH_NAMESPACE, WATCH_PATTERN),
    )
    .await
    .expect("a refused topic-list snapshot must resolve promptly");

    assert!(
        matches!(&res, Err(ClientError::Broker { code, .. })
            if *code == pb::ServerError::AuthorizationError as i32),
        "a refused topic-list snapshot must surface the broker's error, got {res:?}",
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

/// Moonpool twin: a topic-list watch answered with a reply of another kind is reported as an
/// unexpected outcome, not mistaken for a snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_list_snapshot_answered_with_another_reply_kind_is_an_error() {
    let (addr, _requests_seen) = spawn_watch_broker(WatchReply::Mismatched, Redial::Accepted).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = supervised_watch_client(&engine, &addr, Some(32), Duration::from_secs(30)).await;

    let res = tokio::time::timeout(
        HANG_GUARD,
        client.watch_topic_list(WATCH_NAMESPACE, WATCH_PATTERN),
    )
    .await
    .expect("a mismatched topic-list reply must resolve promptly");

    assert!(
        matches!(&res, Err(ClientError::Other(message))
            if message.starts_with("unexpected topic-list snapshot outcome")),
        "a CommandSuccess answering a topic-list watch must be an unexpected outcome, \
         got {res:?}",
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

/// Moonpool twin: without a supervisor, a topic-list snapshot whose connection is lost
/// surfaces `PeerClosed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_list_snapshot_on_a_lost_unsupervised_connection_surfaces_peer_closed() {
    let (addr, _requests_seen) = spawn_watch_broker(WatchReply::Sever, Redial::Accepted).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_plain(&engine, &addr, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed");

    let res = tokio::time::timeout(
        HANG_GUARD,
        client.watch_topic_list(WATCH_NAMESPACE, WATCH_PATTERN),
    )
    .await
    .expect("a topic-list snapshot on a lost connection must resolve promptly");

    assert!(
        matches!(res, Err(ClientError::PeerClosed)),
        "a topic-list snapshot whose unsupervised connection was lost must surface PeerClosed, \
         got {res:?}",
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}
