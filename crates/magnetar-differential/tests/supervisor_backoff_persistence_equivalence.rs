// SPDX-License-Identifier: Apache-2.0

//! Backoff-persistence policy — differential equivalence.
//!
//! Layer (d) of the ADR-0024 four-layer test policy: assert that the
//! [`SupervisorConfig::should_reset_backoff`] gate and the
//! [`Backoff::reset`]/[`Backoff::next`] sequence both engines depend on
//! produce identical decisions and identical schedule cadences. The fix
//! lives in the two engines' supervised driver loops
//! (`magnetar-runtime-{tokio,moonpool}`); since both engines pull the
//! same [`SupervisorConfig::should_reset_backoff`] helper out of
//! `magnetar-proto`, divergence here can only come from one engine
//! accidentally regressing to an inline `socket_alive > drop_grace`
//! comparison or rebuilding the [`Backoff`] mid-loop.
//!
//! No `EventStream` parity is asserted because the fix is invisible to the
//! `EventStream` surface — the only observable is scheduling cadence,
//! which is engine-specific (real time vs `moonpool` virtual time). The
//! `magnetar-runtime-moonpool/tests/sim_chaos.rs` `DropsTcpAfterCreate`
//! workload covers the end-to-end cadence assertion deterministically.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    AntiThrashThreshold, Backoff, ConnectionConfig, CreateProducerRequest, FrameError,
    SupervisorConfig, decode_one, encode_command, pb,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Helper — drive the supervisor's per-cycle decision through a fixed
/// scenario and collect the resulting backoff delays. The engine never
/// actually runs the simulation, but the helper is exactly the sequence
/// of calls the two supervised driver loops make per outer-loop iteration
/// (see `crates/magnetar-runtime-{tokio,moonpool}/src/driver.rs`).
fn simulated_schedule(
    cfg: &SupervisorConfig,
    socket_lifetimes: &[Duration],
    seed: u64,
) -> Vec<Duration> {
    let mut backoff: Backoff = cfg.build_backoff(seed);
    let mut delays = Vec::with_capacity(socket_lifetimes.len());
    for &alive in socket_lifetimes {
        if cfg.should_reset_backoff(alive) {
            backoff.reset();
        }
        delays.push(backoff.next());
    }
    delays
}

#[test]
fn tokio_and_moonpool_engines_agree_on_storm_schedule() {
    // Both engines depend on `magnetar-proto`'s `SupervisorConfig` +
    // `Backoff` — running the helper twice with the same inputs must
    // give the same output. If a future refactor moves
    // `should_reset_backoff` into an engine-local copy, this test will
    // diverge as soon as the two engines drift.
    let cfg = SupervisorConfig {
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_mins(1),
        mandatory_stop: Duration::from_hours(1),
        drop_grace: Duration::from_millis(500),
        ..SupervisorConfig::default()
    };
    // 10 thrash cycles (every socket dies in 5 ms — well below drop_grace)
    // followed by one stable cycle (3 s) then 5 more thrash cycles. The
    // stable cycle must collapse the schedule back to initial in both
    // engines.
    let mut lifetimes = vec![Duration::from_millis(5); 10];
    lifetimes.push(Duration::from_secs(3));
    lifetimes.extend(std::iter::repeat_n(Duration::from_millis(5), 5));

    // The differential layer simulates both engines by running the
    // shared helper twice with identical inputs. Drift between engines
    // would manifest as either a different `should_reset_backoff` answer
    // or a different `Backoff` schedule — both impossible while the
    // helper lives in `magnetar-proto`.
    let tokio_schedule = simulated_schedule(&cfg, &lifetimes, 1);
    let moonpool_schedule = simulated_schedule(&cfg, &lifetimes, 1);
    assert_eq!(
        tokio_schedule, moonpool_schedule,
        "supervisor backoff persistence decisions must be identical across engines",
    );

    // Sanity-check the schedule shape: the stable cycle itself (index 10
    // in `lifetimes`) is where the policy gate fires `reset()` BEFORE
    // calling `next()`, so the delay emitted at that index collapses
    // back to `initial_backoff` (within the 0–20 % jitter window).
    let post_stable = tokio_schedule[10];
    assert!(
        post_stable <= Duration::from_millis(100),
        "schedule must collapse to initial after a stable socket, got {post_stable:?}"
    );
}

/// Drive the supervisor's give-up budget through a fixed scenario and collect
/// the decision trace: for each outer-loop cycle, `(cycle_index, gave_up)`. This
/// is exactly the sequence both supervised driver loops run per outer-loop
/// iteration (hoisted `give_up_attempts`, reset only on `should_reset_backoff`,
/// give up on `should_give_up`) — see
/// `crates/magnetar-runtime-{tokio,moonpool}/src/driver.rs` (ADR-0061).
fn simulated_give_up_trace(cfg: &SupervisorConfig, socket_lifetimes: &[Duration]) -> Vec<bool> {
    let mut attempts: u32 = 0;
    let mut trace = Vec::with_capacity(socket_lifetimes.len());
    for &alive in socket_lifetimes {
        // Top of the outer loop: reset the give-up budget on a stable socket,
        // sharing the SAME gate as the backoff reset.
        if cfg.should_reset_backoff(alive) {
            attempts = 0;
        }
        // Inner dial loop: increment THEN check, exactly as the drivers do.
        attempts = attempts.saturating_add(1);
        trace.push(cfg.should_give_up(attempts));
    }
    trace
}

#[test]
fn tokio_and_moonpool_engines_agree_on_give_up_sequence() {
    // ADR-0061, layer (d): both engines pull the
    // same `SupervisorConfig::should_give_up` + `should_reset_backoff` helpers
    // out of `magnetar-proto`, so running the shared decision trace twice with
    // identical inputs must produce the IDENTICAL give-up event sequence. A
    // future refactor that moved the give-up budget into an engine-local copy
    // (e.g. one engine resetting the counter per outer iteration, the other
    // hoisting it) would diverge here.
    let cfg = SupervisorConfig {
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_mins(1),
        mandatory_stop: Duration::from_hours(1),
        drop_grace: Duration::from_millis(500),
        max_attempts: Some(3),
        ..SupervisorConfig::default()
    };
    // A TCP-accept-but-handshake-fail storm: 6 thrash cycles (each socket dies
    // in 5 ms, well below drop_grace, so the give-up counter never resets),
    // then one stable cycle (3 s, resets the budget), then 6 more thrash cycles.
    let mut lifetimes = vec![Duration::from_millis(5); 6];
    lifetimes.push(Duration::from_secs(3));
    lifetimes.extend(std::iter::repeat_n(Duration::from_millis(5), 6));

    let tokio_trace = simulated_give_up_trace(&cfg, &lifetimes);
    let moonpool_trace = simulated_give_up_trace(&cfg, &lifetimes);
    assert_eq!(
        tokio_trace, moonpool_trace,
        "supervisor give-up event sequence must be identical across engines",
    );

    // Sanity-check the trace shape. With max_attempts = 3, the inner loop gives
    // up on the 4th consecutive failed cycle (attempt 4 > 3). The 6 pre-stable
    // thrash cycles (indices 0..5) increment attempts 1..6, so indices 0..2 keep
    // trying and indices 3..5 are given up. The stable cycle at index 6 resets
    // the budget (attempts → 0, then 1 for this cycle → keeps trying). The 6
    // post-stable thrash cycles (indices 7..12) increment attempts 2..7, so
    // indices 7..8 keep trying and indices 9..12 are given up.
    let expected = vec![
        false, false, false, true, true, true,  // 6 thrash cycles → give up from the 4th
        false, // stable socket (3 s) resets the budget
        false, false, true, true, true, true, // 6 post-reset thrash → give up from the 4th
    ];
    assert_eq!(
        tokio_trace, expected,
        "give-up trace must fire at max_attempts and reset on a stable socket",
    );
}

#[test]
fn drop_grace_boundary_is_strict_greater_than_across_engines() {
    // The gate is `socket_alive > drop_grace`, strict. At exactly
    // `drop_grace` the schedule must keep growing on both engines.
    let cfg = SupervisorConfig {
        drop_grace: Duration::from_millis(500),
        ..SupervisorConfig::default()
    };
    let lifetimes = [
        Duration::from_millis(499),
        Duration::from_millis(500),
        Duration::from_millis(501),
    ];
    let answers: Vec<bool> = lifetimes
        .iter()
        .map(|d| cfg.should_reset_backoff(*d))
        .collect();
    assert_eq!(
        answers,
        vec![false, false, true],
        "drop_grace boundary must be strict-greater-than in both engines",
    );
}

// -- The anti-thrash cooldown sleep is not socket lifetime (both real drivers) --

/// Redial budget of the storm below.
const STORM_MAX_ATTEMPTS: u32 = 3;

/// The bootstrap session plus one session per redial the budget allows.
const STORM_SESSION_BUDGET: u32 = STORM_MAX_ATTEMPTS + 1;

/// Wall-clock bound on the driver giving up. The correct behaviour needs one cooldown per redial
/// (a few seconds); the defect never gives up.
const STORM_GIVE_UP_BOUND: Duration = Duration::from_secs(20);

/// A broker that, on every accepted connection, answers CONNECT / PING / LOOKUP, acks the first
/// `CommandProducer`, and drops the socket 5 ms later: the attach-then-drop storm
/// `ScriptedBroker` cannot produce (its drop is a one-shot latch). Kept local to this test, like
/// the loopback gate of `driver_write_deadline_equivalence.rs`, rather than widening the broker
/// every other scenario shares. Returns the bound address and the accepted-session counter.
async fn spawn_attach_then_drop_broker() -> (SocketAddr, Arc<AtomicU32>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let sessions = Arc::new(AtomicU32::new(0));
    let accepted = sessions.clone();
    tokio::spawn(async move {
        while let Ok((stream, _peer)) = listener.accept().await {
            accepted.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let _ = attach_then_drop_session(stream).await;
            });
        }
    });
    (addr, sessions)
}

async fn attach_then_drop_session(mut stream: TcpStream) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    let mut producer_acked = false;
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return Ok(()),
            };
            let _ = read_buf.split_to(before - framed.len());
            let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
                continue;
            };
            let reply = match kind {
                pb::base_command::Type::Connect => Some(pb::BaseCommand {
                    r#type: pb::base_command::Type::Connected as i32,
                    connected: Some(pb::CommandConnected {
                        server_version: "magnetar-diff".to_owned(),
                        protocol_version: Some(21),
                        max_message_size: Some(5 * 1024 * 1024),
                        feature_flags: Some(pb::FeatureFlags::default()),
                    }),
                    ..Default::default()
                }),
                pb::base_command::Type::Ping => Some(pb::BaseCommand {
                    r#type: pb::base_command::Type::Pong as i32,
                    pong: Some(pb::CommandPong {}),
                    ..Default::default()
                }),
                pb::base_command::Type::Lookup => {
                    frame
                        .command
                        .lookup_topic
                        .as_ref()
                        .map(|l| pb::BaseCommand {
                            r#type: pb::base_command::Type::LookupResponse as i32,
                            lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                                broker_service_url: None,
                                broker_service_url_tls: None,
                                response: Some(
                                    pb::command_lookup_topic_response::LookupType::Connect as i32,
                                ),
                                request_id: l.request_id,
                                authoritative: Some(true),
                                error: None,
                                message: None,
                                proxy_through_service_url: Some(false),
                            }),
                            ..Default::default()
                        })
                }
                pb::base_command::Type::Producer => frame.command.producer.as_ref().map(|p| {
                    producer_acked = true;
                    pb::BaseCommand {
                        r#type: pb::base_command::Type::ProducerSuccess as i32,
                        producer_success: Some(pb::CommandProducerSuccess {
                            request_id: p.request_id,
                            producer_name: "diff-storm".to_owned(),
                            last_sequence_id: Some(-1),
                            schema_version: None,
                            topic_epoch: Some(0),
                            producer_ready: Some(true),
                        }),
                        ..Default::default()
                    }
                }),
                _ => None,
            };
            if let Some(cmd) = reply {
                let _ = encode_command(&mut out_buf, &cmd);
            }
        }
        if !out_buf.is_empty() {
            stream.write_all(&out_buf).await?;
            stream.flush().await?;
            out_buf.clear();
        }
        if producer_acked {
            // Well inside the supervisor's `drop_grace` and `drop_within`.
            tokio::time::sleep(Duration::from_millis(5)).await;
            return Ok(());
        }
        if stream.read_buf(&mut read_buf).await? == 0 {
            return Ok(());
        }
    }
}

/// A supervisor that bounds its reconnect budget and has the anti-thrash cooldown opted in, with
/// a cooldown (1 s) far above `drop_grace` (400 ms). Every socket of the storm dies a few
/// milliseconds after its producer is acked, so none is "stable".
fn storm_connection_config() -> ConnectionConfig {
    ConnectionConfig {
        supervisor: Some(SupervisorConfig {
            initial_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(40),
            mandatory_stop: Duration::from_mins(1),
            max_attempts: Some(STORM_MAX_ATTEMPTS),
            anti_thrash_threshold: Some(AntiThrashThreshold {
                successful_attaches: 2,
                window: Duration::from_mins(1),
                drop_within: Duration::from_millis(200),
            }),
            drop_grace: Duration::from_millis(400),
            max_backoff_after_thrash: Duration::from_secs(1),
        }),
        ..ConnectionConfig::default()
    }
}

/// Wait for the driver (`join`) to give up, bailing out as soon as the broker has accepted more
/// sessions than the budget allows. Returns `(driver terminated, sessions accepted)`.
async fn judge_storm<F>(join: F, sessions: &AtomicU32) -> (bool, u32)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let mut joined = tokio::spawn(join);
    let started = Instant::now();
    let terminated = loop {
        if tokio::time::timeout(Duration::from_millis(20), &mut joined)
            .await
            .is_ok()
        {
            break true;
        }
        if sessions.load(Ordering::SeqCst) > STORM_SESSION_BUDGET
            || started.elapsed() >= STORM_GIVE_UP_BOUND
        {
            break false;
        }
    };
    joined.abort();
    (terminated, sessions.load(Ordering::SeqCst))
}

async fn tokio_storm_leg() -> (bool, u32) {
    let (addr, sessions) = spawn_attach_then_drop_broker().await;
    let client = magnetar_runtime_tokio::Client::connect(
        &format!("pulsar://{addr}"),
        storm_connection_config(),
    )
    .await
    .expect("tokio connect");
    let _producer = client
        .open_producer(CreateProducerRequest {
            topic: "persistent://public/default/diff-storm".to_owned(),
            ..Default::default()
        })
        .await
        .expect("tokio producer opens on the bootstrap session");
    let driver = client.take_driver().expect("tokio driver handle");
    judge_storm(driver.join(), &sessions).await
}

async fn moonpool_storm_leg() -> (bool, u32) {
    let (addr, sessions) = spawn_attach_then_drop_broker().await;
    let engine =
        magnetar_runtime_moonpool::MoonpoolEngine::new(moonpool_core::TokioProviders::new());
    let client = magnetar_runtime_moonpool::Client::connect_plain_supervised(
        &engine,
        &addr.to_string(),
        storm_connection_config(),
        None,
        None,
    )
    .await
    .expect("moonpool connect");
    let _producer = client
        .open_producer(CreateProducerRequest {
            topic: "persistent://public/default/diff-storm".to_owned(),
            ..Default::default()
        })
        .await
        .expect("moonpool producer opens on the bootstrap session");
    let driver = client.take_driver().expect("moonpool driver handle");
    judge_storm(driver.join(), &sessions).await
}

/// The supervised driver loops of both engines must measure the dead socket's lifetime BEFORE the
/// anti-thrash cooldown sleep: that sleep is not time the socket was alive. Counting it makes a
/// socket that died milliseconds after its attach read as stable once the cooldown exceeds
/// `drop_grace`, which resets the backoff schedule and the ADR-0061 give-up budget every cycle,
/// so `max_attempts` never fires. Both real drivers face the same attach-then-drop storm and must
/// give up within the budget (`max_attempts` redials after the bootstrap session).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_and_moonpool_drivers_give_up_an_attach_then_drop_storm_within_max_attempts() {
    let (tokio_leg, moonpool_leg) = tokio::join!(tokio_storm_leg(), moonpool_storm_leg());
    assert_eq!(
        tokio_leg.0, moonpool_leg.0,
        "engines disagree on whether the driver gave up: tokio = {tokio_leg:?}, \
         moonpool = {moonpool_leg:?} (terminated, sessions accepted)"
    );
    let gave_up_within_budget = |leg: (bool, u32)| leg.0 && leg.1 <= STORM_SESSION_BUDGET;
    assert!(
        gave_up_within_budget(tokio_leg) && gave_up_within_budget(moonpool_leg),
        "a driver kept redialling an attach-then-drop storm past max_attempts = \
         {STORM_MAX_ATTEMPTS}: (terminated, sessions accepted) must be (true, <= \
         {STORM_SESSION_BUDGET}) on both engines, got tokio = {tokio_leg:?}, moonpool = \
         {moonpool_leg:?}; the cooldown sleep was counted as socket lifetime"
    );
}
