// SPDX-License-Identifier: Apache-2.0

//! A cancelled `Client::close` must not leave pool connections open — moonpool engine over
//! `TokioProviders`, real loopback brokers. Twin of
//! `crates/magnetar-runtime-tokio/tests/client_close_cancel.rs` (ADR-0024 runtime-test-parity).
//!
//! Before ADR-0039's 2026-11-20 amendment, `Client::close(self)` closed the bootstrap connection
//! synchronously, but closed each proxy-pool connection only after awaiting the bootstrap driver's
//! join, and the pool's `close` latched `closed`, drained every entry and then closed them one at a
//! time, awaiting each driver's join before it asked the next entry to close. `close` consumes the
//! client, so nothing can resume
//! it: a caller that bounds the call (`timeout`, `select!`) and is cancelled at one of those joins
//! leaves the entries that were not reached open, with their drivers running and nothing able to
//! reach them. A dropped `DriverHandle` does not stop its task.
//!
//! Both tests run on the current-thread runtime inside a `LocalSet` and poll `close()` exactly
//! once, so the first join is parked (the driver task has not had a turn yet) and cancellation is
//! deterministic.
//!
//! 1. **Bootstrap join.** One pool entry; the single poll parks on the bootstrap driver's join,
//!    before the pool is looked at.
//! 2. **Pool join.** Two pool entries and the bootstrap handle taken away, so the single poll parks
//!    on the first pool entry's join, with the second already drained out of the map (and, before
//!    the fix, never asked to close).
//!
//! The control in both: the bootstrap broker sees its connection close, so the close request was
//! effective on the connection `close()` did reach. Every pooled broker must see its connection
//! close too.

#![forbid(unsafe_code)]

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, decode_one, encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine, Producer};
use moonpool_core::TokioProviders;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
mod common;
use common::HANG_GUARD;

/// How long a broker may take to see a connection close after the caller's `close()` was
/// cancelled. A close is a driver exit followed by a socket drop, so a healthy run answers in
/// milliseconds: this bound firing IS the defect (a connection nobody closed stays open), it is
/// not a timing assertion.
const PEER_EOF_BOUND: Duration = Duration::from_secs(5);

/// One scripted broker. It records how many connections completed the handshake and how many
/// were closed by the client.
#[derive(Clone)]
struct Broker {
    /// `(topic fragment, broker URL)`: a lookup of a topic containing the fragment is answered
    /// with a direct redirect to that URL. A lookup matching none is answered with itself.
    redirects: Vec<(&'static str, String)>,
    handshakes: Arc<AtomicUsize>,
    /// Sessions whose read side reached EOF (or failed): the client closed its socket.
    closed: Arc<AtomicUsize>,
}

impl Broker {
    fn new(redirects: Vec<(&'static str, String)>) -> Self {
        Self {
            redirects,
            handshakes: Arc::new(AtomicUsize::new(0)),
            closed: Arc::new(AtomicUsize::new(0)),
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
                broker.closed.fetch_add(1, Ordering::SeqCst);
            });
        }
    });
    (addr.to_string(), format!("pulsar://{addr}"))
}

async fn serve(mut stream: TcpStream, broker: &Broker) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out_buf = BytesMut::with_capacity(8 * 1024);
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
            answer(&frame, broker, &mut out_buf);
        }

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

fn reply(out: &mut BytesMut, kind: pb::base_command::Type, mut cmd: pb::BaseCommand) {
    cmd.r#type = kind as i32;
    let _ = encode_command(out, &cmd);
}

fn answer(frame: &magnetar_proto::Frame, broker: &Broker, out: &mut BytesMut) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            broker.handshakes.fetch_add(1, Ordering::SeqCst);
            reply(
                out,
                pb::base_command::Type::Connected,
                pb::BaseCommand {
                    connected: Some(pb::CommandConnected {
                        server_version: "client-close-cancel-test".to_owned(),
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
                let target = broker
                    .redirects
                    .iter()
                    .find(|(fragment, _)| l.topic.contains(fragment))
                    .map(|(_, url)| url.clone());
                reply(
                    out,
                    pb::base_command::Type::LookupResponse,
                    pb::BaseCommand {
                        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                            broker_service_url: target,
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
                            producer_name: "client-close-cancel-producer".to_owned(),
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
        _ => {}
    }
}

/// A bootstrap broker `A` that routes each topic fragment of `topics` to its own broker, a client
/// connected to it, and one open producer per pooled broker (so one pool entry each). The
/// producers are returned so they outlive the close.
async fn client_with_pool_entries(
    topics: &[&'static str],
) -> (
    Client<TokioProviders>,
    Broker,
    Vec<Broker>,
    Vec<Producer<TokioProviders>>,
) {
    let mut pooled = Vec::new();
    let mut redirects = Vec::new();
    for topic in topics {
        let broker = Broker::new(Vec::new());
        let (_host, url) = spawn_broker(broker.clone()).await;
        redirects.push((*topic, url));
        pooled.push(broker);
    }
    let bootstrap = Broker::new(redirects);
    let (host, _url) = spawn_broker(bootstrap.clone()).await;

    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect_plain_supervised(&engine, &host, ConnectionConfig::default(), None, None),
    )
    .await
    .expect("connect did not time out")
    .expect("connect must succeed");

    let mut producers = Vec::new();
    for (index, topic) in topics.iter().enumerate() {
        let producer = tokio::time::timeout(
            HANG_GUARD,
            client.open_producer(CreateProducerRequest {
                topic: format!("persistent://public/default/{topic}"),
                ..Default::default()
            }),
        )
        .await
        .expect("open did not time out")
        .expect("the open must succeed on a fresh pool entry");
        assert_eq!(
            pooled[index].handshakes.load(Ordering::SeqCst),
            1,
            "the open of `{topic}` must have dialled its broker exactly once (sanity)",
        );
        producers.push(producer);
    }
    (client, bootstrap, pooled, producers)
}

/// Poll `future` exactly once and drop it, returning whether it was still pending. This is what a
/// `timeout` or `select!` cancelling the call at its first suspension point does.
async fn poll_once_then_drop<F: Future>(future: F) -> bool {
    let mut future = Box::pin(future);
    let pending =
        std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await;
    drop(future);
    pending
}

/// Wait until `broker` has seen `expected` closed connections, for at most [`PEER_EOF_BOUND`].
async fn closed_connections(broker: &Broker, expected: usize) -> usize {
    let _ = tokio::time::timeout(PEER_EOF_BOUND, async {
        while broker.closed.load(Ordering::SeqCst) < expected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    broker.closed.load(Ordering::SeqCst)
}

/// Cancelling `close()` while it waits for the bootstrap driver must still close the pool
/// connection. The bootstrap connection is closed before the first await, so its broker sees EOF
/// (the control); the pool entry is closed only after that join, so its broker does not.
#[tokio::test]
async fn cancelled_close_at_the_bootstrap_join_still_closes_the_pool() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (client, bootstrap, pooled, _producers) = client_with_pool_entries(&["one"]).await;

            assert!(
                poll_once_then_drop(client.close()).await,
                "the bootstrap driver cannot have finished before its first turn: close() must \
                 be parked on its join (sanity)",
            );

            assert_eq!(
                closed_connections(&bootstrap, 1).await,
                1,
                "the bootstrap connection is closed synchronously by close(), so its broker must \
                 see it close even though the caller cancelled close() (control)",
            );
            let seen = closed_connections(&pooled[0], 1).await;
            assert_eq!(
                seen, 1,
                "the pooled broker never saw its connection close within {PEER_EOF_BOUND:?} of \
                 the caller cancelling close(): the pool connection is only closed after the \
                 bootstrap join, so it and its driver are leaked",
            );
        })
        .await;
}

/// Cancelling `close()` while it joins the first pool entry must still close the others. The pool
/// drains every entry out of its map, then closes and joins them one by one: the entries behind
/// the one being joined are dropped without ever being asked to close.
#[tokio::test]
async fn cancelled_close_at_a_pool_join_still_closes_every_entry() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (client, _bootstrap, pooled, _producers) =
                client_with_pool_entries(&["one", "two"]).await;
            // Keep the bootstrap driver handle (dropping it does not stop the driver) so that the
            // one poll goes straight to the pool, where it parks on the first entry's join.
            let _bootstrap_driver = client.take_driver();

            assert!(
                poll_once_then_drop(client.close()).await,
                "the first pool driver cannot have finished before its first turn: close() must \
                 be parked on its join (sanity)",
            );

            let mut seen = Vec::new();
            for broker in &pooled {
                seen.push(closed_connections(broker, 1).await);
            }
            assert_eq!(
                seen,
                vec![1, 1],
                "every pooled broker must see its connection close within {PEER_EOF_BOUND:?} of \
                 the caller cancelling close(); the entries drained behind the one being joined \
                 were never asked to close, so their connections and drivers are leaked",
            );
        })
        .await;
}
