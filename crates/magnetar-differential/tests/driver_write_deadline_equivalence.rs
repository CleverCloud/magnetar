// SPDX-License-Identifier: Apache-2.0

//! Issue #370 / ADR-0083 — driver write-deadline, differential equivalence
//! (ADR-0024 layer (d)).
//!
//! The bounded, cancellation-safe, `operation_timeout`-bounded write
//! `select!` arm is applied IDENTICALLY to both engines. This is the ONLY
//! layer that exercises the modified `magnetar-runtime-tokio` driver.rs
//! lines under the test binaries `check-sim-coverage` actually executes
//! (`-p magnetar-runtime-moonpool -p magnetar-differential`) — a Trace
//! that never triggers the stalled-write path would leave the tokio-side
//! `write_one_budget` (including its `Err(_elapsed)` deadline branch)
//! uncovered even with the moonpool engine's own tests green. Since the
//! gate's report was widened from those two packages to every package the
//! run compiles, `magnetar-runtime-tokio` carries `SF:` records, so such a
//! gap is now REPORTED as uncovered instead of being skipped as untracked.
//! That report landed advisory under ADR-0090; ADR-0092 flipped
//! `SIM_COVERAGE_ENFORCES_UNCOVERED` to `true` and put the check on every
//! pull request, so the gap fails the gate rather than exiting 0 — which is
//! what makes this test load-bearing rather than merely informative.
//!
//! ## How the stall is produced against a real broker
//!
//! `ScriptedBroker` speaks real TCP; this test interposes a small,
//! self-contained loopback gate (deliberately NOT a change to
//! `crates/magnetar-differential/src/broker.rs`, which is shared by every
//! other differential scenario — this stall behaviour is narrow enough to
//! keep local to this one test) between the client and the broker. The gate
//! proxies normally so CONNECT / LOOKUP / PRODUCER complete, then — on the
//! FIRST accepted connection only — stops reading from the client-facing
//! socket entirely (the socket stays open; nothing closes; no bytes are
//! read-and-dropped the way a black-hole gate would). That is exactly "a
//! peer that accepted the connection then stopped draining our writes",
//! the scenario #370 is about. A ~4 MiB payload (under the scripted
//! broker's 5 MiB `max_message_size`, so it is one frame, no chunking)
//! reliably exceeds the receive window the OS already granted before the
//! stall armed — window growth requires the receiver to actually read,
//! which never happens once the gate stops. The SECOND accepted connection
//! (the supervisor's post-deadline redial) proxies normally, so the
//! replayed send completes and the trace finishes.
//!
//! Both engines run with a short `operation_timeout` so the whole test
//! completes in low single-digit seconds rather than the 30s production
//! default — see [`runner_tokio::run_supervised_with_operation_timeout`] /
//! [`runner_moonpool::run_supervised_with_operation_timeout`].

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Op, Trace, runner_moonpool, runner_tokio};
use magnetar_proto::SupervisorConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Under the scripted broker's 5 MiB `max_message_size` (one frame, no
/// PIP-9 chunking involved) and comfortably larger than the TCP receive
/// window the gate has already been granted by the time it stops draining.
const STALLED_PAYLOAD_BYTES: usize = 4 * 1024 * 1024 - 4096;

/// Bytes forwarded client→broker before the stall arms — generous headroom
/// over the tiny CONNECT + PRODUCER open frames so both are safely through
/// before the gate stops reading, on any encoding-size variation.
const ARM_STALL_AFTER_BYTES: u64 = 4096;

const SHORT_OPERATION_TIMEOUT: Duration = Duration::from_millis(700);

fn short_supervisor() -> SupervisorConfig {
    SupervisorConfig {
        initial_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(100),
        max_attempts: Some(8),
        ..SupervisorConfig::default()
    }
}

/// One direction of the proxy: copies `src` → `dst` until EOF/error, OR,
/// once `stall_after_first_connection` is armed for THIS connection AND
/// `forwarded` has crossed [`ARM_STALL_AFTER_BYTES`], stops reading `src`
/// entirely — the socket is neither closed nor drained further, modelling a
/// peer that accepted the connection and then stopped listening.
async fn splice_with_stall<R, W>(mut src: R, mut dst: W, stall_this_connection: bool)
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut buf = vec![0u8; 64 * 1024];
    let mut forwarded: u64 = 0;
    loop {
        if stall_this_connection && forwarded >= ARM_STALL_AFTER_BYTES {
            // The stall: never read again. Parking forever (rather than
            // returning) keeps this task — and therefore this end of the
            // TCP connection — alive without closing it.
            std::future::pending::<()>().await;
        }
        let n = match src.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        forwarded += u64::try_from(n).unwrap_or(u64::MAX);
        if dst.write_all(&buf[..n]).await.is_err() {
            break;
        }
        if dst.flush().await.is_err() {
            break;
        }
    }
}

/// Spawn the gate. The FIRST accepted connection stalls its client→broker
/// direction once [`ARM_STALL_AFTER_BYTES`] have been forwarded (letting
/// CONNECT + PRODUCER-open through first); every later accepted connection
/// (the supervisor's redial) proxies normally in both directions.
async fn spawn_stall_once_gate(broker_addr: SocketAddr) -> std::io::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let gate_addr = listener.local_addr()?;
    let accept_count = Arc::new(AtomicU32::new(0));

    tokio::spawn(async move {
        loop {
            let Ok((inbound, _peer)) = listener.accept().await else {
                return;
            };
            let is_first = accept_count.fetch_add(1, Ordering::SeqCst) == 0;
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect(broker_addr).await else {
                    return;
                };
                let (ri, wi) = inbound.into_split();
                let (ro, wo) = outbound.into_split();
                // Only the client→broker (c2b) direction ever stalls — see
                // module docs on why the OTHER direction stalling would not
                // reproduce the bug at all (the client's own write would
                // never back up).
                let c2b = splice_with_stall(ri, wo, is_first);
                let b2c = splice_with_stall(ro, wi, false);
                tokio::join!(c2b, b2c);
            });
        }
    });

    Ok(format!("{}:{}", gate_addr.ip(), gate_addr.port()))
}

/// Issue #370 / ADR-0083 differential equivalence: a broker-side peer that
/// stops draining the client's socket after the producer opens must be
/// handled IDENTICALLY on both engines — the write-deadline fires, the
/// connection is marked disconnected, the supervisor redials through the
/// (still-healthy, for the second connection) gate, and the replayed send
/// completes. Both engines must therefore agree on the same terminal
/// `Event::Sent` (not a `SendError` — the redial + replay must succeed).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_write_deadline_event_stream_parity() {
    let payload = vec![0xCDu8; STALLED_PAYLOAD_BYTES];
    let ops = vec![Op::Send {
        payload: payload.clone(),
    }];
    let trace = Trace::new(
        "persistent://public/default/write-deadline-equiv",
        "sub-write-deadline",
        ops,
    );

    // Two independent brokers (and gates) — one per engine leg — so the
    // engines don't share a socket/gate and can run concurrently without
    // cross-talk. Each leg still exercises the identical stall shape.
    let tokio_broker = ScriptedBroker::bind().await.expect("tokio-leg broker bind");
    let tokio_broker_addr: SocketAddr = tokio_broker
        .host_port()
        .parse()
        .expect("scripted broker host_port parses");
    let tokio_gate = spawn_stall_once_gate(tokio_broker_addr)
        .await
        .expect("tokio-leg gate spawn");

    let moonpool_broker = ScriptedBroker::bind()
        .await
        .expect("moonpool-leg broker bind");
    let moonpool_broker_addr: SocketAddr = moonpool_broker
        .host_port()
        .parse()
        .expect("scripted broker host_port parses");
    let moonpool_gate = spawn_stall_once_gate(moonpool_broker_addr)
        .await
        .expect("moonpool-leg gate spawn");

    let tokio_pulsar_url = format!("pulsar://{tokio_gate}");
    let tokio_stream = tokio::time::timeout(
        Duration::from_secs(30),
        runner_tokio::run_supervised_with_operation_timeout(
            &tokio_pulsar_url,
            &trace,
            short_supervisor(),
            SHORT_OPERATION_TIMEOUT,
        ),
    )
    .await
    .expect("tokio runner must finish within the 30s test harness budget")
    .expect("tokio runner");

    let moonpool_stream = tokio::time::timeout(
        Duration::from_secs(30),
        runner_moonpool::run_supervised_with_operation_timeout(
            &moonpool_gate,
            &trace,
            short_supervisor(),
            SHORT_OPERATION_TIMEOUT,
        ),
    )
    .await
    .expect("moonpool runner must finish within the 30s test harness budget")
    .expect("moonpool runner");

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the write-deadline stall — the \
         bounded write select! arm must behave identically on both engines \
         (ADR-0083)",
    );

    // Both legs must have actually recovered via the redial + replay path
    // (a `SendError`/timeout collapse would also "compare equal" if both
    // engines failed the same way, which would silently defeat the point of
    // this test).
    assert_eq!(
        tokio_stream.events.len(),
        1,
        "expected exactly one Sent/SendError event for the single queued send"
    );
    assert!(
        matches!(
            tokio_stream.events[0],
            magnetar_differential::Event::Sent { .. }
        ),
        "issue #370: the write-deadline failure must be recoverable — the \
         supervisor redial + producer replay must land the stalled send \
         successfully once the fresh connection can actually drain it, not \
         terminalize it as an error (tokio_stream={tokio_stream:?})",
    );

    tokio_broker.shutdown().await;
    moonpool_broker.shutdown().await;
}
