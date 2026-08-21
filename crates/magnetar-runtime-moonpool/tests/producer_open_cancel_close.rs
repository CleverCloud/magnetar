// SPDX-License-Identifier: Apache-2.0

//! Close-before-retry on a cancelled producer open — moonpool engine
//! (issue #406, ADR-0100).
//!
//! An `open_producer` that loses its race with `operation_timeout` used to
//! be abandoned client-side only. The broker completes the open on its own
//! schedule and keeps the `(topic, producer_name)` registration, so every
//! later open under that name is rejected with `ProducerBusy` (code 16,
//! broker `NamingException`) until the topic is unloaded. `ProducerBusy` is
//! retryable for `ProducerOpen` (ADR-0080), so the engine's retry loop
//! re-hits that zombie with a fresh producer id until the budget is spent.
//!
//! The scripted broker below models exactly that: it keeps a
//! `(topic, name) → producer_id` registry, rejects a duplicate name with
//! `ProducerBusy`, and **withholds** the first `ProducerSuccess` for a
//! configured name until a `CommandCloseProducer` for the registered id
//! arrives — at which point it releases the name and finally flushes the
//! (now late) success. A client that never sends the close can therefore
//! never reopen the name.
//!
//! Each test pairs with a same-named test on the tokio side
//! (`crates/magnetar-runtime-tokio/tests/producer_open_cancel_close.rs`)
//! so `cargo run -p xtask -- check-runtime-test-parity` stays balanced 1:1
//! (ADR-0024). Layer (c) of the four-layer test policy.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, decode_one, encode_command, pb,
};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Client-side budget for one `open_producer`. Long enough for the
/// connect / lookup / open round-trips on a loaded host, short enough that
/// the withheld open fails quickly.
const OPERATION_TIMEOUT: Duration = Duration::from_millis(750);

/// Broker-side state shared by every session of one scripted broker.
struct BrokerState {
    /// Ordered log of every `BaseCommand` kind the broker received.
    log: Vec<i32>,
    /// Live `(topic, producer_name) → producer_id` registrations. This is
    /// the broker resource issue #406 leaks.
    names: HashMap<(String, String), u64>,
    /// Producer name whose FIRST open has its `ProducerSuccess` withheld.
    withhold_name: Option<String>,
    /// One-shot latch for [`Self::withhold_name`].
    withhold_fired: bool,
    /// The withheld `(producer_id, request_id, topic)`, released when the
    /// client closes the abandoned producer id.
    withheld: Option<(u64, u64, String)>,
}

type Shared = Arc<Mutex<BrokerState>>;

/// Bind a scripted broker. `withhold_name` arms the one-shot withheld
/// `ProducerSuccess`; `None` serves every open normally.
async fn spawn_broker(withhold_name: Option<&str>) -> (String, Shared) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let state: Shared = Arc::new(Mutex::new(BrokerState {
        log: Vec::new(),
        names: HashMap::new(),
        withhold_name: withhold_name.map(ToOwned::to_owned),
        withhold_fired: false,
        withheld: None,
    }));
    let state_task = state.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let state_conn = state_task.clone();
            tokio::spawn(async move {
                run_broker_conn(&mut stream, &state_conn).await;
            });
        }
    });
    (addr.to_string(), state)
}

async fn run_broker_conn(stream: &mut tokio::net::TcpStream, state: &Shared) {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out_buf = BytesMut::with_capacity(8 * 1024);
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return,
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            state.lock().log.push(frame.command.r#type);
            answer_frame(&frame, state, &mut out_buf);
        }
        if !out_buf.is_empty() {
            if stream.write_all(&out_buf).await.is_err() {
                return;
            }
            if stream.flush().await.is_err() {
                return;
            }
            out_buf.clear();
        }
        match stream.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

fn answer_frame(frame: &magnetar_proto::Frame, state: &Shared, out: &mut BytesMut) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-producer-open-cancel".to_owned(),
                    protocol_version: Some(21),
                    max_message_size: Some(5 * 1024 * 1024),
                    feature_flags: Some(pb::FeatureFlags::default()),
                }),
                ..Default::default()
            };
            let _ = encode_command(out, &cmd);
        }
        pb::base_command::Type::Ping => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Pong as i32,
                pong: Some(pb::CommandPong {}),
                ..Default::default()
            };
            let _ = encode_command(out, &cmd);
        }
        pb::base_command::Type::Lookup => {
            if let Some(l) = &frame.command.lookup_topic {
                let cmd = pb::BaseCommand {
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
                };
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::Producer => {
            if let Some(p) = &frame.command.producer {
                answer_producer(p, state, out);
            }
        }
        pb::base_command::Type::CloseProducer => {
            if let Some(c) = &frame.command.close_producer {
                answer_close_producer(c, state, out);
            }
        }
        _ => {}
    }
}

/// Register `(topic, name)` or reject the open with `ProducerBusy`, exactly
/// as a real broker's `Topic#addProducer` does when the name is taken.
fn answer_producer(p: &pb::CommandProducer, state: &Shared, out: &mut BytesMut) {
    let name = p
        .producer_name
        .clone()
        .filter(|candidate| !candidate.is_empty());
    let mut guard = state.lock();
    if let Some(name) = name.as_ref() {
        let key = (p.topic.clone(), name.clone());
        if guard.names.contains_key(&key) {
            drop(guard);
            emit_producer_busy(out, p.request_id, name);
            return;
        }
        guard.names.insert(key, p.producer_id);
    }
    let withhold = guard.withhold_name.is_some()
        && guard.withhold_name == name
        && !guard.withhold_fired
        && guard.withheld.is_none();
    if withhold {
        guard.withhold_fired = true;
        guard.withheld = Some((p.producer_id, p.request_id, p.topic.clone()));
        return;
    }
    drop(guard);
    let effective = name.unwrap_or_else(|| format!("broker-assigned-{}", p.producer_id));
    emit_producer_success(out, p.request_id, &effective);
}

/// Release every `(topic, name)` registration the closed producer id holds,
/// then flush a withheld success for that id — the late ack the client must
/// discard without resurrecting anything.
fn answer_close_producer(c: &pb::CommandCloseProducer, state: &Shared, out: &mut BytesMut) {
    let released = {
        let mut guard = state.lock();
        guard.names.retain(|_, id| *id != c.producer_id);
        match guard.withheld.as_ref() {
            Some((producer_id, _, _)) if *producer_id == c.producer_id => guard.withheld.take(),
            _ => None,
        }
    };
    if let Some((producer_id, request_id, _topic)) = released {
        emit_producer_success(out, request_id, &format!("late-{producer_id}"));
    }
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id: c.request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_producer_success(out: &mut BytesMut, request_id: u64, producer_name: &str) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: producer_name.to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_producer_busy(out: &mut BytesMut, request_id: u64, name: &str) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Error as i32,
        error: Some(pb::CommandError {
            request_id,
            error: pb::ServerError::ProducerBusy as i32,
            message: format!("Producer with name '{name}' is already connected to topic"),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn close_producer_count(state: &Shared) -> usize {
    state
        .lock()
        .log
        .iter()
        .filter(|t| **t == pb::base_command::Type::CloseProducer as i32)
        .count()
}

fn client_config() -> ConnectionConfig {
    ConnectionConfig {
        operation_timeout: OPERATION_TIMEOUT,
        ..Default::default()
    }
}

/// Issue #406 regression. The first open under a pinned name loses its race
/// with `operation_timeout`; the broker has already registered the name. The
/// second open under the same name must succeed, which is only possible
/// because the cancellation pushed a `CommandCloseProducer` for the
/// abandoned producer id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_producer_open_frees_the_pinned_name() {
    const PRODUCER_NAME: &str = "pinned-406";
    const TOPIC: &str = "persistent://public/default/open-cancel-frees-name";

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, state) = spawn_broker(Some(PRODUCER_NAME)).await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = Client::connect_plain(&engine, &host_port, client_config())
                .await
                .expect("connect ok");

            let timed_out = client
                .open_producer(CreateProducerRequest {
                    topic: TOPIC.to_owned(),
                    producer_name: Some(PRODUCER_NAME.to_owned()),
                    ..Default::default()
                })
                .await;
            assert!(
                timed_out.is_err(),
                "the withheld ProducerSuccess must exhaust the operation deadline"
            );

            let reopened = client
                .open_producer(CreateProducerRequest {
                    topic: TOPIC.to_owned(),
                    producer_name: Some(PRODUCER_NAME.to_owned()),
                    ..Default::default()
                })
                .await
                .expect("the pinned name must be reusable after the cancelled open closed it");

            assert_eq!(
                close_producer_count(&state),
                1,
                "the cancelled open must push exactly one CommandCloseProducer"
            );
            reopened.close().await.expect("close ok");
            client.close().await;
        })
        .await;
}

/// Negative-space control for the test above: while a producer holds the
/// name, the scripted broker really does reject a second open with
/// `ProducerBusy`. Without this the reopen assertion could pass vacuously
/// against a broker that never enforced name exclusivity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_name_held_by_a_live_producer_stays_busy() {
    const PRODUCER_NAME: &str = "pinned-406-live";
    const TOPIC: &str = "persistent://public/default/open-cancel-name-busy";

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, state) = spawn_broker(None).await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = Client::connect_plain(&engine, &host_port, client_config())
                .await
                .expect("connect ok");

            let held = client
                .open_producer(CreateProducerRequest {
                    topic: TOPIC.to_owned(),
                    producer_name: Some(PRODUCER_NAME.to_owned()),
                    ..Default::default()
                })
                .await
                .expect("first open ok");

            let rejected = client
                .open_producer(CreateProducerRequest {
                    topic: TOPIC.to_owned(),
                    producer_name: Some(PRODUCER_NAME.to_owned()),
                    ..Default::default()
                })
                .await;
            assert!(
                rejected.is_err(),
                "a name held by a live producer must not open a second time"
            );
            assert_eq!(
                close_producer_count(&state),
                0,
                "a rejected open holds no broker registration, so it must not close one"
            );

            held.close().await.expect("close ok");
            client.close().await;
        })
        .await;
}
