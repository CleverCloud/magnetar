// SPDX-License-Identifier: Apache-2.0

//! A pool entry whose driver has exited must not be handed out again — moonpool engine over
//! `TokioProviders`, real loopback brokers. Twin of
//! `crates/magnetar-runtime-tokio/tests/pool_dead_entry.rs` (ADR-0024 runtime-test-parity).
//!
//! `pool::get_or_open` returns a `Ready` entry's connection without asking whether its driver is
//! still running, and the pool removes an entry only when its dial fails or at `close()`.
//! `Client::connect_plain_supervised` builds the pool whatever the supervisor setting, so an entry
//! becomes terminal on its first connection drop without a supervisor, and once its supervisor
//! exhausts `max_attempts` with one. Every later open that the lookup routes to that broker is
//! then served by the dead connection, although the broker is reachable again and the bootstrap
//! connection is healthy.
//!
//! Scenario (run once per way the entry's driver can end):
//!
//! 1. **Broker A** is the bootstrap broker and answers every lookup with a direct redirect to
//!    **broker B**.
//! 2. The first producer is opened: the client dials B and caches the pool entry.
//! 3. B drops that connection on the first data-plane frame and refuses handshakes while it is
//!    "down". The first producer's in-flight send resolves with an error once the entry's driver
//!    has exited for good.
//! 4. B recovers. A second producer is opened on a topic that resolves to B again.
//!
//! The second open must succeed on a fresh connection to B (a second completed handshake on B),
//! instead of being served by the dead entry.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, SupervisorConfig, decode_one,
    encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
mod common;
use common::HANG_GUARD;

/// Behaviour of one scripted broker.
#[derive(Clone)]
struct Broker {
    /// `Some(url)`: answer every lookup with a direct redirect to `url`.
    redirect_to: Option<String>,
    /// While `false` the broker closes a connection as soon as it reads the client's CONNECT, so
    /// no handshake can complete (the broker is "down" but still accepts TCP).
    up: Arc<AtomicBool>,
    /// Drop the connection on the first `CommandSend` and flip `up` to `false`: a terminal peer
    /// close followed by an outage.
    crash_on_send: bool,
    /// Number of `CommandConnected` answers written, i.e. completed handshakes.
    handshakes: Arc<AtomicUsize>,
    /// How long the broker holds its `CommandConnected` answer, so a dial stays in its handshake.
    handshake_delay: Duration,
}

impl Broker {
    fn new(redirect_to: Option<String>, crash_on_send: bool) -> Self {
        Self {
            redirect_to,
            up: Arc::new(AtomicBool::new(true)),
            crash_on_send,
            handshakes: Arc::new(AtomicUsize::new(0)),
            handshake_delay: Duration::ZERO,
        }
    }
}

/// Returns the broker's bare `host:port` (what the moonpool client dials) and the
/// `pulsar://host:port` URL a lookup response advertises it under.
async fn spawn_broker(broker: Broker) -> (String, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            let broker = broker.clone();
            tokio::spawn(async move {
                let _ = serve(stream, &broker).await;
            });
        }
    });
    (addr.to_string(), format!("pulsar://{addr}"))
}

async fn serve(mut stream: TcpStream, broker: &Broker) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out_buf = BytesMut::with_capacity(8 * 1024);
    let mut handshake = false;
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

            handshake |= frame.command.r#type == pb::base_command::Type::Connect as i32;
            if matches!(answer(&frame, broker, &mut out_buf), Flow::Close) {
                return Ok(());
            }
        }

        if handshake && !broker.handshake_delay.is_zero() {
            tokio::time::sleep(broker.handshake_delay).await;
        }
        handshake = false;
        if !out_buf.is_empty() {
            stream.write_all(&out_buf).await?;
            stream.flush().await?;
            out_buf.clear();
        }

        if stream.read_buf(&mut read_buf).await? == 0 {
            return Ok(());
        }
    }
}

/// What the connection does after a frame has been answered.
enum Flow {
    Keep,
    Close,
}

fn answer(frame: &magnetar_proto::Frame, broker: &Broker, out: &mut BytesMut) -> Flow {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return Flow::Keep;
    };
    match kind {
        pb::base_command::Type::Connect => {
            if !broker.up.load(Ordering::SeqCst) {
                return Flow::Close;
            }
            broker.handshakes.fetch_add(1, Ordering::SeqCst);
            reply(
                out,
                pb::base_command::Type::Connected,
                pb::BaseCommand {
                    connected: Some(pb::CommandConnected {
                        server_version: "pool-dead-entry-test".to_owned(),
                        protocol_version: Some(21),
                        max_message_size: Some(5 * 1024 * 1024),
                        feature_flags: Some(pb::FeatureFlags::default()),
                    }),
                    ..Default::default()
                },
            );
        }
        pb::base_command::Type::Ping => reply(
            out,
            pb::base_command::Type::Pong,
            pb::BaseCommand {
                pong: Some(pb::CommandPong {}),
                ..Default::default()
            },
        ),
        pb::base_command::Type::Lookup => {
            if let Some(l) = &frame.command.lookup_topic {
                reply(
                    out,
                    pb::base_command::Type::LookupResponse,
                    pb::BaseCommand {
                        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                            broker_service_url: broker.redirect_to.clone(),
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
                    },
                );
            }
        }
        pb::base_command::Type::Producer => {
            if let Some(p) = &frame.command.producer {
                reply(
                    out,
                    pb::base_command::Type::ProducerSuccess,
                    pb::BaseCommand {
                        producer_success: Some(pb::CommandProducerSuccess {
                            request_id: p.request_id,
                            producer_name: "pool-dead-entry-producer".to_owned(),
                            last_sequence_id: Some(-1),
                            schema_version: None,
                            topic_epoch: Some(0),
                            producer_ready: Some(true),
                        }),
                        ..Default::default()
                    },
                );
            }
        }
        pb::base_command::Type::Send if broker.crash_on_send => {
            // Terminal peer close, then an outage: the redial attempts of a supervised entry are
            // refused until the test flips `up` back.
            broker.up.store(false, Ordering::SeqCst);
            return Flow::Close;
        }
        _ => {}
    }
    Flow::Keep
}

fn reply(out: &mut BytesMut, kind: pb::base_command::Type, mut cmd: pb::BaseCommand) {
    cmd.r#type = kind as i32;
    let _ = encode_command(out, &cmd);
}

fn outgoing(payload: &'static [u8]) -> OutgoingMessage {
    OutgoingMessage {
        payload: Bytes::from_static(payload),
        metadata: pb::MessageMetadata::default(),
        uncompressed_size: payload.len() as u32,
        num_messages: 1,
        txn_id: None,
        source_message_id: None,
    }
}

fn producer_request(topic: &str) -> CreateProducerRequest {
    CreateProducerRequest {
        topic: topic.to_owned(),
        ..Default::default()
    }
}

/// Drive the scenario described in the module documentation with `config` on the bootstrap
/// connection (pool entries inherit it). `Err` carries the failed expectation about the second
/// open; the scenario's own preconditions are plain assertions.
async fn open_after_entry_death(config: ConnectionConfig) -> Result<(), String> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let broker_b = Broker::new(None, true);
            let (_host_b, url_b) = spawn_broker(broker_b.clone()).await;
            let (host_a, _url_a) = spawn_broker(Broker::new(Some(url_b), false)).await;

            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                HANG_GUARD,
                Client::connect_plain_supervised(&engine, &host_a, config, None, None),
            )
            .await
            .expect("connect did not time out")
            .expect("connect must succeed");

            let first = tokio::time::timeout(
                HANG_GUARD,
                client.open_producer(producer_request(
                    "persistent://public/default/pool-dead-entry-first",
                )),
            )
            .await
            .expect("first open did not time out")
            .expect("the first open must succeed on a fresh pool entry for broker B");
            assert_eq!(
                broker_b.handshakes.load(Ordering::SeqCst),
                1,
                "the first open must have dialled broker B exactly once",
            );

            // Broker B crashes on the first data-plane frame. The in-flight send resolves with an
            // error once the pool entry's driver has exited for good (immediately without a
            // supervisor, after the attempt budget with one).
            let send = tokio::time::timeout(HANG_GUARD, first.send(outgoing(b"crash-broker-b")))
                .await
                .expect(
                    "the in-flight send must resolve once the entry's driver is gone, not hang",
                );
            assert!(
                !broker_b.up.load(Ordering::SeqCst),
                "broker B must have crashed on the data-plane frame (sanity)",
            );
            assert!(
                send.is_err(),
                "the in-flight send must fail when its connection is terminal, got {send:?}",
            );
            // The driver wrapper latches `no_driver` right after it fails the pending sends; give
            // it the moment it needs so the entry is dead when the next open looks it up.
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Broker B is reachable again; the bootstrap connection never stopped being healthy.
            broker_b.up.store(true, Ordering::SeqCst);
            assert!(
                client.is_connected(),
                "the bootstrap connection must still be live (sanity)",
            );

            let second = tokio::time::timeout(
                HANG_GUARD,
                client.open_producer(producer_request(
                    "persistent://public/default/pool-dead-entry-second",
                )),
            )
            .await;
            let handshakes = broker_b.handshakes.load(Ordering::SeqCst);

            if let Some(d) = client.take_driver() {
                d.abort();
            }
            drop(client);

            let second = second.expect("second open did not time out");
            if let Err(error) = &second {
                return Err(format!(
                    "an open routed to a recovered broker must succeed on a fresh connection, \
                     but the pool handed out its dead entry: {error:?}"
                ));
            }
            if handshakes != 2 {
                return Err(format!(
                    "the second open must have re-dialled broker B instead of reusing the dead \
                     connection: {handshakes} completed handshakes on B, expected 2"
                ));
            }
            Ok(())
        })
        .await
}

/// Both ways a pool entry's driver ends for good. `Client::connect_plain_supervised` builds the
/// pool whatever the supervisor setting, so the unsupervised case (the first connection drop is
/// terminal) is reachable here as on tokio; a supervised entry is terminal once it exhausts
/// `max_attempts`. Both cases run so that one red run reports both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_after_entry_death_redials_the_broker() {
    let unsupervised = ConnectionConfig::default();
    assert!(
        unsupervised.supervisor.is_none(),
        "the first case pins the UNSUPERVISED default",
    );
    let give_up = ConnectionConfig {
        supervisor: Some(SupervisorConfig {
            initial_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(20),
            max_attempts: Some(2),
            ..SupervisorConfig::default()
        }),
        ..ConnectionConfig::default()
    };

    let mut failures = Vec::new();
    for (label, config) in [
        ("unsupervised", unsupervised),
        ("supervisor give-up", give_up),
    ] {
        if let Err(failure) = open_after_entry_death(config).await {
            failures.push(format!("{label}: {failure}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Moonpool twin: two opens that the lookup routes to broker B while the pool's first dial of B
/// is still in its handshake share that dial. Both opens succeed, and B completes exactly one
/// handshake: the second open joined the dial in flight instead of dialing B again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_opens_routed_to_one_broker_during_its_dial() {
    let mut broker_b = Broker::new(None, false);
    broker_b.handshake_delay = Duration::from_millis(200);
    let b_handshakes = broker_b.handshakes.clone();
    let (_b_addr, b_url) = spawn_broker(broker_b).await;
    let (a_addr, _a_url) = spawn_broker(Broker::new(Some(b_url), false)).await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_plain_supervised(&engine, &a_addr, ConnectionConfig::default(), None, None),
    )
    .await
    .expect("connect did not time out")
    .expect("connect to broker A");

    let (first, second) = tokio::time::timeout(HANG_GUARD, async {
        tokio::join!(
            client.open_producer(producer_request("persistent://public/default/pool-join-a")),
            client.open_producer(producer_request("persistent://public/default/pool-join-b")),
        )
    })
    .await
    .expect("both opens must resolve");
    first.expect("the first open on broker B");
    second.expect("the second open on broker B");
    assert_eq!(
        b_handshakes.load(Ordering::SeqCst),
        1,
        "the second open must join the dial of B already in flight, not dial B again"
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}
