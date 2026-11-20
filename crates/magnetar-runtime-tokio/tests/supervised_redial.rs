// SPDX-License-Identifier: Apache-2.0

//! Supervised-redial coverage — tokio engine, real loopback broker.
//!
//! Mirror of the moonpool deterministic-simulation fixture
//! `crates/magnetar-runtime-moonpool/tests/supervised_redial.
//! rs::supervised_loop_redials_under_drop_accept_cycle_sweep_8_seeds` (`DropAcceptCycleBroker` +
//! `SupervisedRedialClientWorkload`). Maintains the tokio ↔ moonpool 1:1 test count required by
//! ADR-0024.
//!
//! Both sides drive the production `supervised_driver_loop`'s reconnect body
//! — the anti-thrash cooldown sleep, the persisted-backoff reset gate, the
//! multi-attempt redial loop, and the `reset()` + `begin_handshake()` +
//! resume tail — through a broker that accepts → CONNECT/CONNECTED → LOOKUP →
//! `PRODUCER_SUCCESS` → drops the socket, then **re-accepts** the supervisor's
//! redial for several cycles (drop → accept → drop → accept).
//!
//! The moonpool engine is the canonical place for the deterministic *timing*
//! assertion (virtual `time.sleep` makes the backoff schedule reproducible);
//! this tokio mirror drives the same production reconnect path over a real
//! `127.0.0.1` socket and asserts the supervisor genuinely redialed
//! (`>= 2` accepted sessions) and torn down more than one socket
//! (`>= 2` drops). Wall-clock timing of the schedule is intentionally NOT
//! asserted here (it is flaky over loopback); the re-accept count is the
//! authoritative proof the reconnect body ran.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::task::Poll;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    AntiThrashThreshold, ConnectionConfig, CreateProducerRequest, FrameError, SupervisorConfig,
    decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::{Client, DnsResolveFuture, DnsResolver, ParsedUrl};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;
mod common;
use common::HANG_GUARD;

/// Spawn a fake broker on `127.0.0.1:0` that runs the canonical
/// create-then-drop session on every inbound connection and re-accepts the
/// next one. Returns the bound `pulsar://...` URL plus two cross-session
/// counters: how many connections it accepted and how many it tore down.
async fn spawn_drop_accept_cycle_broker() -> (String, Arc<AtomicU32>, Arc<AtomicU32>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let sessions_accepted = Arc::new(AtomicU32::new(0));
    let drops_performed = Arc::new(AtomicU32::new(0));

    let accepted = sessions_accepted.clone();
    let drops = drops_performed.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            accepted.fetch_add(1, Ordering::SeqCst);
            let drops_for_session = drops.clone();
            tokio::spawn(async move {
                let _ = handle_drop_after_create_session(stream, drops_for_session).await;
            });
        }
    });

    (
        format!("pulsar://{addr}"),
        sessions_accepted,
        drops_performed,
    )
}

/// One session: answer CONNECT / PING / LOOKUP, ack the first `CommandProducer`
/// with `ProducerSuccess`, then drop the socket a few ms later. Mirrors the
/// moonpool `handle_drop_after_create_session` helper.
async fn handle_drop_after_create_session(
    mut stream: tokio::net::TcpStream,
    drops_performed: Arc<AtomicU32>,
) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    let mut sent_producer_success = false;
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
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
                        emit_lookup_response(&mut out_buf, l.request_id);
                    }
                }
                pb::base_command::Type::Producer => {
                    if let Some(p) = &frame.command.producer {
                        emit_producer_success(&mut out_buf, p.request_id);
                        sent_producer_success = true;
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

        // Canonical ADR-0028 thrash: once we've acked the producer, sleep
        // briefly (well inside the supervisor's `drop_grace`) and tear the
        // socket down. The supervisor observes the drop and redials against
        // the listener, which re-accepts on the next loop turn.
        if sent_producer_success {
            tokio::time::sleep(Duration::from_millis(5)).await;
            drops_performed.fetch_add(1, Ordering::SeqCst);
            return Ok(());
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(e) => return Err(e),
        }
    }
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-supervised-redial-test".to_owned(),
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
            // No proxy redirect — keep the data plane on the bootstrap socket.
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
            producer_name: "supervised-redial-test".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn supervisor_with_anti_thrash() -> SupervisorConfig {
    SupervisorConfig {
        // Tiny schedule so the test runs in real time without slowing the
        // suite, yet long enough that the redial body dominates.
        initial_backoff: Duration::from_millis(5),
        max_backoff: Duration::from_millis(40),
        mandatory_stop: Duration::from_mins(1),
        max_attempts: Some(64),
        anti_thrash_threshold: Some(AntiThrashThreshold {
            successful_attaches: 3,
            window: Duration::from_secs(5),
            drop_within: Duration::from_millis(200),
        }),
        drop_grace: Duration::from_millis(500),
        max_backoff_after_thrash: Duration::from_millis(60),
    }
}

/// Drive the tokio engine's production `supervised_driver_loop` through
/// several drop → accept → drop → accept cycles and assert the supervisor
/// genuinely redialed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_loop_redials_under_drop_accept_cycle() {
    let (url, sessions_accepted, drops_performed) = spawn_drop_accept_cycle_broker().await;

    let cfg = ConnectionConfig {
        supervisor: Some(supervisor_with_anti_thrash()),
        ..ConnectionConfig::default()
    };

    // Supervised connect — `config.supervisor = Some` wires
    // `spawn_supervised_driver` (the reconnect body) on the tokio engine.
    let client = tokio::time::timeout(HANG_GUARD, Client::connect(&url, cfg))
        .await
        .expect("connect did not time out")
        .expect("connect ok");

    // Drive a sequence of opens. Every successful open is followed by the
    // broker dropping the socket, so the supervisor redials and resumes; the
    // next open rides a freshly-reconnected session.
    for _ in 0..12u32 {
        let _ = tokio::time::timeout(
            Duration::from_millis(800),
            client.open_producer(CreateProducerRequest {
                topic: "persistent://public/default/supervised-redial".to_owned(),
                ..Default::default()
            }),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    let accepts = sessions_accepted.load(Ordering::SeqCst);
    let drops = drops_performed.load(Ordering::SeqCst);

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    assert!(
        accepts >= 2,
        "supervisor must redial at least once (accepts={accepts})"
    );
    assert!(
        drops >= 2,
        "multi-cycle drop pattern must fire (drops={drops})"
    );
}

// -- A user close() that lands while a supervisor redial is dialling ---------

/// Every frame the broker decoded, tagged with the accept-order index of the
/// session it arrived on (0 = the bootstrap socket, 1.. = supervisor redials).
type SessionFrames = Arc<Mutex<Vec<(u32, pb::base_command::Type)>>>;

/// Resolver whose first lookup (the bootstrap dial) answers at once and whose
/// every later lookup (a supervisor redial) parks until [`Self::release`], so
/// the test can land `Client::close()` while the redial is in flight and then
/// let that dial succeed.
#[derive(Debug)]
struct GatedRedialResolver {
    addr: SocketAddr,
    lookups: AtomicU32,
    /// Notified (permit kept) the moment a redial lookup parks.
    redial_parked: Notify,
    released: AtomicBool,
    release: Notify,
}

impl GatedRedialResolver {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            lookups: AtomicU32::new(0),
            redial_parked: Notify::new(),
            released: AtomicBool::new(false),
            release: Notify::new(),
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

impl DnsResolver for GatedRedialResolver {
    fn resolve<'a>(&'a self, _host: &'a str, _port: u16) -> DnsResolveFuture<'a> {
        Box::pin(async move {
            if self.lookups.fetch_add(1, Ordering::SeqCst) > 0 {
                self.redial_parked.notify_one();
                loop {
                    // Created before the flag is read, so a `release()` between
                    // the read and the await cannot be missed.
                    let released = self.release.notified();
                    if self.released.load(Ordering::SeqCst) {
                        break;
                    }
                    released.await;
                }
            }
            Ok(vec![self.addr])
        })
    }
}

/// One broker session. Answers CONNECT / PING / LOOKUP / PRODUCER and records
/// every decoded frame under `session`. Session 0 holds its socket open until
/// `drop_session_zero` is notified, then drops it so the supervisor redials;
/// every other session stays open until the client hangs up.
async fn handle_recorded_session(
    mut stream: tokio::net::TcpStream,
    session: u32,
    seen: SessionFrames,
    drop_session_zero: Arc<Notify>,
) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    let mut sent_producer_success = false;
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return Ok(()),
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
                continue;
            };
            seen.lock().push((session, kind));
            match kind {
                pb::base_command::Type::Connect => emit_connected(&mut out_buf),
                pb::base_command::Type::Ping => emit_pong(&mut out_buf),
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        emit_lookup_response(&mut out_buf, l.request_id);
                    }
                }
                pb::base_command::Type::Producer => {
                    if let Some(p) = &frame.command.producer {
                        emit_producer_success(&mut out_buf, p.request_id);
                        sent_producer_success = true;
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

        if session == 0 && sent_producer_success {
            drop_session_zero.notified().await;
            return Ok(());
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(e) => return Err(e),
        }
    }
}

/// Broker that numbers its sessions in accept order and records their frames.
async fn spawn_recorded_session_broker() -> (SocketAddr, SessionFrames, Arc<Notify>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let frames: SessionFrames = Arc::new(Mutex::new(Vec::new()));
    let drop_session_zero = Arc::new(Notify::new());

    let recorded = frames.clone();
    let drop_zero = drop_session_zero.clone();
    tokio::spawn(async move {
        let mut session: u32 = 0;
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            let recorded = recorded.clone();
            let drop_zero = drop_zero.clone();
            let index = session;
            session += 1;
            tokio::spawn(async move {
                let _ = handle_recorded_session(stream, index, recorded, drop_zero).await;
            });
        }
    });

    (addr, frames, drop_session_zero)
}

/// A user `close()` that lands while the supervisor is inside a redial dial
/// must survive that dial succeeding: `Client::close()` has to resolve, and
/// the producer the application had open must not be re-attached on the
/// replacement socket. A supervisor that re-checks `is_user_closed()` only
/// before the dial runs `reset()` once a dial succeeds afterwards, which
/// snaps `Closing` back to `Uninitialized` and erases the close.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_during_redial_dial_is_not_erased_by_reset() {
    let (broker_addr, frames, drop_session_zero) = spawn_recorded_session_broker().await;
    let resolver = Arc::new(GatedRedialResolver::new(broker_addr));

    let cfg = ConnectionConfig {
        supervisor: Some(SupervisorConfig {
            initial_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(40),
            mandatory_stop: Duration::from_mins(1),
            max_attempts: Some(64),
            ..SupervisorConfig::default()
        }),
        // The parked redial lookup is inside this bound: it must outlast the
        // time the test needs to land the close and release the dial.
        connect_timeout: HANG_GUARD,
        ..ConnectionConfig::default()
    };
    let url = ParsedUrl::parse(&format!("pulsar://{broker_addr}")).expect("broker url");
    let dns: Arc<dyn DnsResolver> = resolver.clone();

    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_with_resolver_and_provider(url, None, cfg, None, None, Some(dns)),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    // A producer the application has open on the bootstrap session.
    let _producer = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/close-during-redial".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out")
    .expect("producer opens on the bootstrap session");

    // Drop the bootstrap socket and wait for the supervisor to park inside
    // the redial's lookup: the dial is now in flight.
    drop_session_zero.notify_one();
    tokio::time::timeout(HANG_GUARD, resolver.redial_parked.notified())
        .await
        .expect("the supervisor never started a redial dial");

    // `Client::close()` runs its synchronous prefix on the first poll: the
    // connection is `Closing` from here on, whatever the future does next.
    let mut close = Box::pin(client.close());
    let mut closed = std::future::poll_fn(|cx| Poll::Ready(close.as_mut().poll(cx)))
        .await
        .is_ready();

    // The redial dial now succeeds.
    resolver.release();
    if !closed {
        closed = tokio::time::timeout(HANG_GUARD, close).await.is_ok();
    }

    let replayed_producers = frames
        .lock()
        .iter()
        .filter(|(session, kind)| *session >= 1 && *kind == pb::base_command::Type::Producer)
        .count();
    let bootstrap_producers = frames
        .lock()
        .iter()
        .filter(|(session, kind)| *session == 0 && *kind == pb::base_command::Type::Producer)
        .count();
    assert_eq!(
        bootstrap_producers, 1,
        "precondition: the producer was opened on the bootstrap session"
    );
    assert!(
        closed && replayed_producers == 0,
        "a user close() issued while a supervisor redial was dialling was erased by the \
         redial's reset(): Client::close() resolved = {closed} (must be true), producers \
         re-attached on the replacement socket = {replayed_producers} (must be 0)"
    );
}

// -- The anti-thrash cooldown sleep is not socket lifetime -------------------

/// An attach-then-drop storm against a supervisor that bounds its reconnect
/// budget (`max_attempts`) and has the anti-thrash cooldown opted in, with a
/// cooldown longer than `drop_grace`.
///
/// Every socket here dies a few milliseconds after its producer is acked, so
/// none of them is "stable" (`should_reset_backoff` is false for a socket that
/// lived less than `drop_grace`): the ADR-0061 give-up budget must keep
/// counting across cycles and the driver must stop after `max_attempts`
/// redials. The cooldown the detector arms sleeps for longer than
/// `drop_grace`, though, and a supervisor that measures the dead socket's
/// lifetime AFTER that sleep reads the cooldown as socket lifetime: it resets
/// the backoff schedule and the give-up counter every cycle and redials for
/// ever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anti_thrash_cooldown_sleep_is_not_counted_as_socket_lifetime() {
    const MAX_ATTEMPTS: u32 = 3;
    // The bootstrap session plus one session per redial the budget allows.
    const SESSION_BUDGET: u32 = MAX_ATTEMPTS + 1;

    let (url, sessions_accepted, _drops_performed) = spawn_drop_accept_cycle_broker().await;

    let cfg = ConnectionConfig {
        supervisor: Some(SupervisorConfig {
            initial_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(40),
            mandatory_stop: Duration::from_mins(1),
            max_attempts: Some(MAX_ATTEMPTS),
            anti_thrash_threshold: Some(AntiThrashThreshold {
                successful_attaches: 2,
                window: Duration::from_mins(1),
                drop_within: Duration::from_millis(200),
            }),
            // A socket that dies within a few ms is far inside this grace; the
            // cooldown below is far outside it.
            drop_grace: Duration::from_millis(400),
            max_backoff_after_thrash: Duration::from_secs(1),
        }),
        ..ConnectionConfig::default()
    };

    let client = tokio::time::timeout(HANG_GUARD, Client::connect(&url, cfg))
        .await
        .expect("connect did not time out")
        .expect("connect ok");

    // The producer the supervisor re-attaches on every redial: it has to stay
    // open for the storm to continue.
    let _producer = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/cooldown-is-not-lifetime".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out")
    .expect("producer opens on the bootstrap session");

    let driver = client
        .take_driver()
        .expect("a supervised client keeps its driver handle");
    let joined = tokio::spawn(driver.join());

    // Wait for the driver to give up. Bail out as soon as the broker has seen
    // more sessions than the budget allows: the supervisor is then redialling
    // past `max_attempts` and waiting longer proves nothing.
    let started = tokio::time::Instant::now();
    while !joined.is_finished()
        && sessions_accepted.load(Ordering::SeqCst) <= SESSION_BUDGET
        && started.elapsed() < HANG_GUARD
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let terminated = joined.is_finished();
    let accepted = sessions_accepted.load(Ordering::SeqCst);
    joined.abort();

    assert!(
        terminated && accepted <= SESSION_BUDGET,
        "the supervisor kept redialling an attach-then-drop storm past max_attempts = \
         {MAX_ATTEMPTS}: driver terminated = {terminated} (must be true), broker sessions \
         accepted = {accepted} (must be <= {SESSION_BUDGET}); the anti-thrash cooldown sleep \
         was counted as socket lifetime and reset the give-up budget"
    );
}

// -- A socket the client abandons is closed before the redial delay -----------

/// Backoff before the first redial. The schedule subtracts up to 20 % of it as jitter, so the
/// redial cannot start earlier than 2.4 s after the socket was abandoned.
const REDIAL_BACKOFF: Duration = Duration::from_secs(3);

/// How soon after the client abandons a socket the broker must see it closed. Far below the
/// redial delay, far above any loopback scheduling latency: the socket has no reason to outlive
/// the decision to abandon it.
const ABANDONED_SOCKET_CLOSE_BOUND: Duration = Duration::from_secs(1);

/// What the broker saw of the bootstrap session, shared with the test.
#[derive(Debug, Default)]
struct AbandonedSocketWatch {
    /// The test releases the broker's `CommandTopicMigrated` push.
    push: Notify,
    /// The instant the broker pushed it.
    pushed_at: Mutex<Option<std::time::Instant>>,
    /// Time from the push to the broker reading EOF (or an error) on that same socket.
    closed_after_push: Mutex<Option<Duration>>,
    /// Notified (permit kept) once `closed_after_push` is set.
    closed: Notify,
    /// Connections accepted so far (the bootstrap socket plus every redial).
    sessions: AtomicU32,
}

/// One broker session. Answers CONNECT / PING / LOOKUP / PRODUCER. On the bootstrap session
/// (index 0) it waits for the test to release the push after acking the producer, then sends
/// `CommandTopicMigrated` for that producer (no allow-list is configured, so the client abandons
/// the socket and redials locally) and watches that socket until the client closes it.
async fn handle_migrating_session(
    mut stream: tokio::net::TcpStream,
    session: u32,
    watch: Arc<AbandonedSocketWatch>,
) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    let mut producer_id: Option<u64> = None;
    let mut pushed = false;
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
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
                        emit_lookup_response(&mut out_buf, l.request_id);
                    }
                }
                pb::base_command::Type::Producer => {
                    if let Some(p) = &frame.command.producer {
                        emit_producer_success(&mut out_buf, p.request_id);
                        producer_id = Some(p.producer_id);
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

        if session == 0 && !pushed {
            if let Some(producer_id) = producer_id {
                watch.push.notified().await;
                let migrated = pb::BaseCommand {
                    r#type: pb::base_command::Type::TopicMigrated as i32,
                    topic_migrated: Some(pb::CommandTopicMigrated {
                        resource_id: producer_id,
                        resource_type: pb::command_topic_migrated::ResourceType::Producer as i32,
                        broker_service_url: Some("pulsar://new-broker:6650".to_owned()),
                        broker_service_url_tls: None,
                    }),
                    ..Default::default()
                };
                let _ = encode_command(&mut out_buf, &migrated);
                stream.write_all(&out_buf).await?;
                stream.flush().await?;
                out_buf.clear();
                let now = std::time::Instant::now();
                *watch.pushed_at.lock() = Some(now);
                pushed = true;
            }
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => {
                let pushed_at = *watch.pushed_at.lock();
                if session == 0 {
                    if let Some(pushed_at) = pushed_at {
                        *watch.closed_after_push.lock() = Some(pushed_at.elapsed());
                        watch.closed.notify_one();
                    }
                }
                return Ok(());
            }
            Ok(_) => {}
        }
    }
}

/// Broker that numbers its sessions in accept order and watches the bootstrap one.
async fn spawn_migrating_broker(watch: Arc<AbandonedSocketWatch>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            let session = watch.sessions.fetch_add(1, Ordering::SeqCst);
            let watch = watch.clone();
            tokio::spawn(async move {
                let _ = handle_migrating_session(stream, session, watch).await;
            });
        }
    });
    format!("pulsar://{addr}")
}

/// A transport the driver abandoned on its own decision (here a `CommandTopicMigrated` with no
/// allow-list) must be closed when the driver abandons it, not when the supervisor finally
/// replaces it. The peer is still alive after such an exit, so a socket kept open through the
/// redial delay leaves the broker's connection, producers and consumers registered, and
/// dispatching, to a client that no longer reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_socket_is_closed_before_the_redial_backoff() {
    let watch = Arc::new(AbandonedSocketWatch::default());
    let url = spawn_migrating_broker(watch.clone()).await;

    let cfg = ConnectionConfig {
        supervisor: Some(SupervisorConfig {
            initial_backoff: REDIAL_BACKOFF,
            max_backoff: REDIAL_BACKOFF,
            ..SupervisorConfig::default()
        }),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(HANG_GUARD, Client::connect(&url, cfg))
        .await
        .expect("connect did not time out")
        .expect("connect ok");
    let _producer = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/abandoned-socket".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out")
    .expect("producer opens on the bootstrap session");

    // The broker tells the client the topic migrated: the client abandons this socket.
    watch.push.notify_one();
    let closed_in_time =
        tokio::time::timeout(ABANDONED_SOCKET_CLOSE_BOUND, watch.closed.notified())
            .await
            .is_ok();
    let closed_after_push = *watch.closed_after_push.lock();
    let sessions = watch.sessions.load(Ordering::SeqCst);
    let pushed = watch.pushed_at.lock().is_some();

    if let Some(driver) = client.take_driver() {
        driver.abort();
    }
    drop(client);

    assert!(
        pushed,
        "precondition: the broker pushed CommandTopicMigrated on the bootstrap session"
    );
    assert!(
        closed_in_time,
        "the driver abandoned the bootstrap socket on CommandTopicMigrated but the broker still \
         saw it open {ABANDONED_SOCKET_CLOSE_BOUND:?} later (closed after {closed_after_push:?}, \
         broker sessions accepted = {sessions}): the supervisor holds the abandoned socket \
         through its redial backoff of {REDIAL_BACKOFF:?} instead of closing it"
    );
}
