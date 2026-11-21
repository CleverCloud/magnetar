// SPDX-License-Identifier: Apache-2.0

//! A user `close()` that races a write the broker is not draining ends the
//! client on both engines (ADR-0024 layer (d)).
//!
//! The driver keeps writing while bytes are queued, so a close issued while a
//! publish is parked mid-write waits for that write. The write deadline
//! (`operation_timeout`, ADR-0083) then fires and the driver maps it through
//! `mark_disconnected()`. That used to overwrite `Closing` with `Failed`, which
//! the supervisor reads as a transport drop: it redialled (a second broker
//! accept), re-attached the producer, replayed the publish, and
//! `Client::close().await` never returned. Both engines share the
//! `magnetar-proto` state machine that decides it, so they must agree.
//!
//! The stall is produced the way `driver_write_deadline_equivalence.rs`
//! produces it: a loopback gate in front of the scripted broker proxies
//! CONNECT / LOOKUP / PRODUCER, then stops reading from the client on the
//! first accepted connection while keeping the socket open. Unlike that test,
//! the later accepts here are the thing counted: a closed client must never
//! reach the second one.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use magnetar_differential::broker::ScriptedBroker;
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{ConnectionConfig, CreateProducerRequest, SupervisorConfig, pb};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Under the scripted broker's 5 MiB `max_message_size` (one frame, no
/// chunking) and larger than the receive window the gate already granted
/// before it stops reading (the sizing `driver_write_deadline_equivalence.rs`
/// documents).
const STALLED_PAYLOAD_BYTES: usize = 4 * 1024 * 1024 - 4096;

/// Bytes forwarded client to broker before the stall arms: headroom over the
/// small CONNECT, LOOKUP and PRODUCER frames.
const ARM_STALL_AFTER_BYTES: u64 = 4096;

const OPERATION_TIMEOUT: Duration = Duration::from_millis(700);

fn supervised_config() -> ConnectionConfig {
    ConnectionConfig {
        operation_timeout: OPERATION_TIMEOUT,
        supervisor: Some(SupervisorConfig {
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(100),
            max_attempts: Some(8),
            ..SupervisorConfig::default()
        }),
        ..ConnectionConfig::default()
    }
}

/// Copy `src` to `dst` until EOF or error. When `stall` is set and
/// [`ARM_STALL_AFTER_BYTES`] have been forwarded, stop reading `src` for good
/// without closing anything: a peer that accepted the connection and then
/// stopped draining it.
async fn splice(
    mut src: impl AsyncReadExt + Unpin,
    mut dst: impl AsyncWriteExt + Unpin,
    stall: bool,
) {
    let mut buf = vec![0u8; 64 * 1024];
    let mut forwarded: u64 = 0;
    loop {
        if stall && forwarded >= ARM_STALL_AFTER_BYTES {
            std::future::pending::<()>().await;
        }
        let n = match src.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        forwarded += u64::try_from(n).unwrap_or(u64::MAX);
        if dst.write_all(&buf[..n]).await.is_err() || dst.flush().await.is_err() {
            break;
        }
    }
}

/// Spawn the gate in front of `broker_addr`: the first accepted connection
/// stalls its client-to-broker direction, every later one proxies normally.
/// Returns the gate address and the number of connections it has accepted.
async fn spawn_stall_once_gate(
    broker_addr: SocketAddr,
) -> std::io::Result<(SocketAddr, Arc<AtomicU32>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let gate_addr = listener.local_addr()?;
    let accepted = Arc::new(AtomicU32::new(0));
    {
        let accepted = ToOwned::to_owned(&accepted);
        tokio::spawn(async move {
            while let Ok((inbound, _peer)) = listener.accept().await {
                let first = accepted.fetch_add(1, Ordering::SeqCst) == 0;
                tokio::spawn(async move {
                    let Ok(outbound) = TcpStream::connect(broker_addr).await else {
                        return;
                    };
                    let (ri, wi) = inbound.into_split();
                    let (ro, wo) = outbound.into_split();
                    tokio::join!(splice(ri, wo, first), splice(ro, wi, false));
                });
            }
        });
    }
    Ok((gate_addr, accepted))
}

fn stalled_message() -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from(vec![0xCDu8; STALLED_PAYLOAD_BYTES]),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: u32::try_from(STALLED_PAYLOAD_BYTES).unwrap_or(u32::MAX),
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

/// What the application can observe of one engine's run.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    /// The publish was parked mid-write on the first, connected session when
    /// the user closed, so the close really did race a draining write queue.
    stalled_at_close: bool,
    /// `Client::close().await` returned.
    close_returned: bool,
    /// Connections the gate accepted: 1 means the closed client never redialled.
    sessions_accepted: u32,
    /// The connection still reports the user's close at the end.
    user_closed_at_end: bool,
}

/// Close the client once its publish is parked mid-write, and observe it.
async fn close_while_stalled(
    sessions: Arc<AtomicU32>,
    send_resolved: Arc<AtomicBool>,
    is_connected: impl Fn() -> bool,
    is_user_closed: impl Fn() -> bool,
    close: impl std::future::Future<Output = ()> + Send + 'static,
) -> Outcome {
    // Park the publish mid-write: the gate has stopped reading.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let stalled_at_close = is_connected()
        && !send_resolved.load(Ordering::SeqCst)
        && sessions.load(Ordering::SeqCst) == 1;

    let mut close_task = tokio::spawn(close);
    let close_returned = tokio::time::timeout(Duration::from_secs(10), &mut close_task)
        .await
        .is_ok();
    let outcome = Outcome {
        stalled_at_close,
        close_returned,
        sessions_accepted: sessions.load(Ordering::SeqCst),
        user_closed_at_end: is_user_closed(),
    };
    close_task.abort();
    outcome
}

async fn tokio_leg() -> Outcome {
    let broker = ScriptedBroker::bind().await.expect("tokio-leg broker bind");
    let broker_addr: SocketAddr = broker.host_port().parse().expect("broker address parses");
    let (gate, sessions) = spawn_stall_once_gate(broker_addr)
        .await
        .expect("tokio-leg gate");
    let client =
        magnetar_runtime_tokio::Client::connect(&format!("pulsar://{gate}"), supervised_config())
            .await
            .expect("tokio initial handshake");
    let producer = client
        .open_producer(CreateProducerRequest {
            topic: "persistent://public/default/close-stalled-write-equiv".to_owned(),
            ..Default::default()
        })
        .await
        .expect("tokio open producer");
    let shared = ToOwned::to_owned(client.shared());
    let send_resolved = Arc::new(AtomicBool::new(false));
    let send = producer.send(stalled_message());
    let stalled_send = {
        let send_resolved = ToOwned::to_owned(&send_resolved);
        tokio::spawn(async move {
            let _ = send.await;
            send_resolved.store(true, Ordering::SeqCst);
        })
    };
    let (is_connected, is_user_closed) = (ToOwned::to_owned(&shared), ToOwned::to_owned(&shared));
    let outcome = close_while_stalled(
        sessions,
        send_resolved,
        move || is_connected.inner.lock().is_connected(),
        move || is_user_closed.inner.lock().is_user_closed(),
        client.close(),
    )
    .await;
    stalled_send.abort();
    broker.shutdown().await;
    outcome
}

async fn moonpool_leg() -> Outcome {
    let broker = ScriptedBroker::bind()
        .await
        .expect("moonpool-leg broker bind");
    let broker_addr: SocketAddr = broker.host_port().parse().expect("broker address parses");
    let (gate, sessions) = spawn_stall_once_gate(broker_addr)
        .await
        .expect("moonpool-leg gate");
    let engine =
        magnetar_runtime_moonpool::MoonpoolEngine::new(moonpool_core::TokioProviders::new());
    let client = magnetar_runtime_moonpool::Client::connect_plain_supervised(
        &engine,
        &gate.to_string(),
        supervised_config(),
        None,
        None,
    )
    .await
    .expect("moonpool initial handshake");
    let producer = client
        .open_producer(CreateProducerRequest {
            topic: "persistent://public/default/close-stalled-write-equiv".to_owned(),
            ..Default::default()
        })
        .await
        .expect("moonpool open producer");
    let shared = ToOwned::to_owned(client.shared());
    let send_resolved = Arc::new(AtomicBool::new(false));
    let send = producer.send(stalled_message());
    let stalled_send = {
        let send_resolved = ToOwned::to_owned(&send_resolved);
        tokio::spawn(async move {
            let _ = send.await;
            send_resolved.store(true, Ordering::SeqCst);
        })
    };
    let (is_connected, is_user_closed) = (ToOwned::to_owned(&shared), ToOwned::to_owned(&shared));
    let outcome = close_while_stalled(
        sessions,
        send_resolved,
        move || is_connected.inner.lock().is_connected(),
        move || is_user_closed.inner.lock().is_user_closed(),
        client.close(),
    )
    .await;
    stalled_send.abort();
    broker.shutdown().await;
    outcome
}

/// A close that lands while a publish is parked mid-write ends the client on
/// both engines: `close()` returns once the write deadline fires, the gate
/// sees no second connection, and the connection stays closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_during_stalled_write_ends_the_client_on_both_engines() {
    let tokio_outcome = tokio_leg().await;
    let moonpool_outcome = moonpool_leg().await;

    assert_eq!(
        tokio_outcome, moonpool_outcome,
        "the engines diverged on a user close that lands inside a stalled write"
    );
    assert!(
        tokio_outcome.stalled_at_close,
        "precondition: the publish must be parked mid-write on the first connected \
         session when the user closes ({tokio_outcome:?})"
    );
    assert!(
        tokio_outcome.close_returned,
        "Client::close() must return once the stalled write's deadline fires; the \
         write-deadline mark_disconnected() demoted Closing to Failed and the supervisor \
         resurrected the session ({tokio_outcome:?})"
    );
    assert_eq!(
        tokio_outcome.sessions_accepted, 1,
        "a closed client must not redial the broker ({tokio_outcome:?})"
    );
    assert!(
        tokio_outcome.user_closed_at_end,
        "the connection must still report the user's close ({tokio_outcome:?})"
    );
}
