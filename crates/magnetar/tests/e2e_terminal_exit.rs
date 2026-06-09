// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the plain-connection terminal fail-fast
//! (ADR-0055 §1), layer (e) of the ADR-0024 four-layer policy.
//!
//! Pins the production contract that a PLAIN (non-reconnecting) client whose
//! broker connection drops mid-flight surfaces a terminal error on the
//! in-flight op PROMPTLY — instead of parking it forever (the no-progress
//! stall ADR-0055 §1 kills) — AND that a NEW op issued AFTER the connection is
//! already terminal also fast-fails synchronously rather than registering a
//! doomed pending op (ADR-0059 / follow-ups §4.1). This is the e2e analogue of
//! the tokio + moonpool `terminal_exit.rs` integration tests and the
//! `magnetar-differential` terminal-error equivalence test.
//!
//! ## How a mid-flight outage is forced against a real broker
//!
//! testcontainers only hands back the broker's mapped port once it is up, and
//! a `docker stop` mid-test would be slow and racy. Instead we point the
//! client at a local loopback **kill-gate** that splices client↔broker bytes,
//! then drop every spliced connection on demand: after a healthy
//! produce/consume warm-up (proving the path is live), we trigger the gate to
//! tear down its sockets while a `consumer.receive()` is parked. The client's
//! plain driver sees the peer close (read returns 0), runs
//! `Connection::fail_all_pending`, and the in-flight receive resolves with a
//! terminal error rather than hanging.
//!
//! The client uses the default builder — `enable_reconnect` is NOT called, so
//! the supervisor is `None` and the driver takes the plain terminal-exit path.
//!
//! Runs as a regular test under `cargo test` (ADR-0046, no `#[ignore]`, no
//! feature gate). Requires Docker + a reachable `apachepulsar/pulsar` image.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::{OutgoingMessage, PulsarClient};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

fn image_repo() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_REPO").unwrap_or_else(|_| DEFAULT_IMAGE_REPO.to_owned())
}

fn image_tag() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_TAG").unwrap_or_else(|_| DEFAULT_IMAGE_TAG.to_owned())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("magnetar=info")),
        )
        .with_test_writer()
        .try_init();
}

/// Start a Pulsar 4.x standalone container; return (`broker_host`,
/// `broker_port`, `container_handle`). Dropping the guard stops the broker.
async fn start_pulsar()
-> Result<(String, u16, testcontainers::ContainerAsync<GenericImage>), Box<dyn std::error::Error>> {
    init_tracing();
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
        .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
        .with_wait_for(WaitFor::message_on_stdout(
            "Created namespace public/default",
        ))
        .with_startup_timeout(Duration::from_secs(120))
        .with_cmd(vec!["bin/pulsar".to_owned(), "standalone".to_owned()])
        .start()
        .await?;
    let host = container.get_host().await?.to_string();
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    Ok((host, binary_port, container))
}

/// Handle to a kill-gate: a loopback TCP proxy in front of the broker. Call
/// [`KillGate::kill`] to drop every spliced connection (and refuse new ones),
/// forcing a terminal peer close on the client's driver.
struct KillGate {
    /// `host:port` the client dials.
    host_port: String,
    /// Fired by `kill()` — each splice task races it against its copy loops and
    /// returns (closing its sockets) the moment it is notified.
    kill: Arc<Notify>,
    /// Latched so new accepts are refused after a kill.
    killed: Arc<AtomicBool>,
}

impl KillGate {
    /// Drop every live spliced connection and refuse future ones.
    fn kill(&self) {
        self.killed.store(true, Ordering::SeqCst);
        self.kill.notify_waiters();
    }
}

/// Bind a loopback gate that splices client↔broker bytes until [`KillGate::kill`]
/// is called, at which point every spliced connection is torn down.
async fn spawn_kill_gate(
    broker_host: String,
    broker_port: u16,
) -> Result<KillGate, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let gate_addr = listener.local_addr()?;
    let kill = Arc::new(Notify::new());
    let killed = Arc::new(AtomicBool::new(false));
    let kill_task = Arc::clone(&kill);
    let killed_task = Arc::clone(&killed);

    tokio::spawn(async move {
        loop {
            let Ok((inbound, _peer)) = listener.accept().await else {
                return;
            };
            if killed_task.load(Ordering::SeqCst) {
                // Gate killed — drop the inbound socket immediately.
                drop(inbound);
                continue;
            }
            let host = broker_host.clone();
            let kill = Arc::clone(&kill_task);
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect((host.as_str(), broker_port)).await else {
                    return;
                };
                let (mut ri, mut wi) = inbound.into_split();
                let (mut ro, mut wo) = outbound.into_split();
                let c2b = tokio::io::copy(&mut ri, &mut wo);
                let b2c = tokio::io::copy(&mut ro, &mut wi);
                // Race the splice against the kill signal: a kill returns
                // immediately, dropping all four halves → both peers see EOF.
                tokio::select! {
                    _ = c2b => {}
                    _ = b2c => {}
                    () = kill.notified() => {}
                }
            });
        }
    });

    Ok(KillGate {
        host_port: format!("{}:{}", gate_addr.ip(), gate_addr.port()),
        kill,
        killed,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_plain_client_in_flight_op_fails_fast_on_outage()
-> Result<(), Box<dyn std::error::Error>> {
    let (broker_host, broker_port, _container) = start_pulsar().await?;
    let gate = spawn_kill_gate(broker_host, broker_port).await?;
    let service_url = format!("pulsar://{}", gate.host_port);

    // PLAIN client: no `enable_reconnect`, so the supervisor is None and the
    // driver takes the terminal-exit path on a drop (ADR-0055 §1).
    let client = tokio::time::timeout(
        Duration::from_secs(40),
        PulsarClient::builder().service_url(service_url).build(),
    )
    .await
    .expect("client build must not exceed the test guard")
    .expect("plain client must connect through the live gate");

    let topic = "persistent://public/default/magnetar-e2e-terminal-exit";
    let producer = client.producer(topic).create().await?;
    let consumer = client
        .consumer(topic)
        .subscription("magnetar-e2e-terminal-exit")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;

    // Healthy warm-up: prove the spliced path is live end-to-end before we
    // kill it, so the terminal error below is unambiguously the outage.
    producer
        .send(OutgoingMessage::with_payload(b"before-outage".to_vec()).into())
        .await?;
    let warm = consumer.receive().await?;
    assert_eq!(warm.payload.to_vec(), b"before-outage".to_vec());
    consumer.ack(warm.message_id).await?;

    // Park an in-flight receive (no message pending), then KILL the gate so
    // the broker connection drops under the parked future. The plain driver
    // runs `fail_all_pending`; the receive must resolve with a terminal error
    // PROMPTLY — the timeout below is the no-hang guard, the regression this
    // test exists to catch.
    let recv_fut = consumer.receive();
    // Give the receive a beat to register its waker, then force the outage.
    tokio::time::sleep(Duration::from_millis(200)).await;
    gate.kill();

    let recv_res = tokio::time::timeout(Duration::from_secs(15), recv_fut)
        .await
        .expect("in-flight receive must resolve PROMPTLY after the outage, not hang");
    assert!(
        recv_res.is_err(),
        "the in-flight receive must surface a terminal error on the outage, got Ok({:?})",
        recv_res.ok().map(|m| m.message_id),
    );

    // The client reports the connection as down after the terminal drop.
    assert!(
        !client.is_connected(),
        "connection must be down after the outage"
    );

    // ADR-0059 / follow-ups §4.1: a NEW op issued AFTER the connection is
    // already terminal must fast-fail SYNCHRONOUSLY rather than register a
    // doomed pending op. `fail_all_pending` flipped the producer slot `closed`
    // and the plain driver latched `no_driver` on its terminal exit, so a fresh
    // `producer.send()` resolves with a terminal error PROMPTLY — the timeout is
    // the no-hang guard. (The earlier in-flight contract covered the op parked
    // AT the drop; this covers the op issued AFTER it.)
    let send_after = tokio::time::timeout(
        Duration::from_secs(15),
        producer.send(OutgoingMessage::with_payload(b"after-outage".to_vec()).into()),
    )
    .await
    .expect("a post-terminal send must fast-fail PROMPTLY, not hang");
    assert!(
        send_after.is_err(),
        "a send issued after the terminal drop must surface a terminal error, got Ok({:?})",
        send_after.ok(),
    );

    Ok(())
}
