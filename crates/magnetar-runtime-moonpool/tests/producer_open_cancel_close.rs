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
//! [`NameScript::WithholdSurvivingClose`] models the harder interleaving that
//! ADR-0100's cancel-time close cannot cover, and which reproduced against a
//! real Pulsar 4.0.4 broker in CI: the close is consumed while the broker's
//! producer creation is still pending, so the broker acks it, drops its
//! connection-level record of that producer id, and lets the creation complete
//! anyway — leaving a registration no close can address. Recovery is then
//! two-phase (re-close, then successor re-attach under the abandoned id), and
//! the broker models Pulsar's successor rule: same producer id, strictly
//! higher epoch.
//!
//! Each test pairs with a same-named test on the tokio side
//! (`crates/magnetar-runtime-tokio/tests/producer_open_cancel_close.rs`)
//! so `cargo run -p xtask -- check-runtime-test-parity` stays balanced 1:1
//! (ADR-0024). Layer (c) of the four-layer test policy.

#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
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

/// What the scripted broker does with producer opens for one pinned name.
#[derive(Debug, Clone)]
enum NameScript {
    /// Serve every open normally.
    Plain,
    /// Withhold the FIRST `ProducerSuccess` for this name; the client's
    /// `CommandCloseProducer` for that producer id releases the registration
    /// and flushes the withheld success. This is the interleaving where the
    /// broker's own close-during-creation cleanup works.
    WithholdReleasedByClose(String),
    /// Withhold the FIRST `ProducerSuccess` for this name; the close is
    /// consumed while creation is still pending, so the broker acks it, stops
    /// mapping that producer id, and completes the registration anyway. The
    /// registration then outlives every close — only a successor re-attach
    /// under the abandoned id reclaims it (issue #406 CI reproduction).
    WithholdSurvivingClose(String),
}

impl NameScript {
    fn withhold_name(&self) -> Option<&str> {
        match self {
            Self::Plain => None,
            Self::WithholdReleasedByClose(name) | Self::WithholdSurvivingClose(name) => Some(name),
        }
    }

    fn registration_survives_close(&self) -> bool {
        matches!(self, Self::WithholdSurvivingClose(_))
    }
}

/// Broker-side state shared by every session of one scripted broker.
struct BrokerState {
    /// Ordered log of every `BaseCommand` kind the broker received.
    log: Vec<i32>,
    /// Live `(topic, producer_name) → (producer_id, epoch)` registrations.
    /// This is the broker resource issue #406 leaks. The epoch is what a
    /// successor re-attach has to beat.
    names: HashMap<(String, String), (u64, u64)>,
    /// The script for the pinned name.
    script: NameScript,
    /// One-shot latch for the withheld success.
    withhold_fired: bool,
    /// The withheld `(producer_id, request_id, producer_name)`, flushed when
    /// the client closes that producer id.
    withheld: Option<(u64, u64, String)>,
    /// Producer ids the broker no longer maps on this connection. A close for
    /// one is acked and does nothing — Pulsar drops its record of the id when
    /// it completes a close-before-creation.
    unmapped: HashSet<u64>,
}

type Shared = Arc<Mutex<BrokerState>>;

/// Bind a scripted broker running `script`.
async fn spawn_broker(script: NameScript) -> (String, Shared) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let state: Shared = Arc::new(Mutex::new(BrokerState {
        log: Vec::new(),
        names: HashMap::new(),
        script,
        withhold_fired: false,
        withheld: None,
        unmapped: HashSet::new(),
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

/// Register `(topic, name)` or reject the open with `ProducerBusy`, exactly as
/// a real broker's `Topic#addProducer` does when the name is taken.
///
/// A held name has one escape: Pulsar's successor rule. An open naming the
/// SAME producer id as the current owner with a strictly higher epoch
/// overwrites it (`AbstractTopic#tryOverwriteOldProducer` →
/// `Producer#isSuccessorTo`), which is what a client's re-attach relies on.
/// Any other id collides.
fn answer_producer(p: &pb::CommandProducer, state: &Shared, out: &mut BytesMut) {
    let name = p
        .producer_name
        .clone()
        .filter(|candidate| !candidate.is_empty());
    let epoch = p.epoch.unwrap_or(0);
    let mut guard = state.lock();
    if let Some(name) = name.as_ref() {
        let key = (p.topic.clone(), name.clone());
        if let Some(&(owner_id, owner_epoch)) = guard.names.get(&key) {
            if owner_id != p.producer_id || epoch <= owner_epoch {
                drop(guard);
                emit_producer_busy(out, p.request_id, name);
                return;
            }
            // Successor re-attach: the owner is overwritten in place.
            guard.names.insert(key, (p.producer_id, epoch));
            guard.unmapped.remove(&p.producer_id);
            drop(guard);
            emit_producer_success(out, p.request_id, name);
            return;
        }
        guard.names.insert(key, (p.producer_id, epoch));
    }
    let withhold = guard.script.withhold_name() == name.as_deref() && !guard.withhold_fired;
    if withhold {
        guard.withhold_fired = true;
        let effective = name.unwrap_or_default();
        guard.withheld = Some((p.producer_id, p.request_id, effective));
        return;
    }
    drop(guard);
    let effective = name.unwrap_or_else(|| format!("broker-assigned-{}", p.producer_id));
    emit_producer_success(out, p.request_id, &effective);
}

/// Close a producer id, then flush a withheld success for it — the late ack the
/// client must discard without resurrecting anything.
///
/// Under [`NameScript::WithholdSurvivingClose`] the first close for the
/// withheld id models Pulsar completing a close-before-creation: it is acked,
/// the connection stops mapping the id, and the registration the pending
/// creation goes on to make is left behind. Every later close for that id is
/// acked and does nothing, exactly as a close for an id the connection no
/// longer maps.
fn answer_close_producer(c: &pb::CommandCloseProducer, state: &Shared, out: &mut BytesMut) {
    let released = {
        let mut guard = state.lock();
        if guard.unmapped.contains(&c.producer_id) {
            None
        } else {
            let creation_pending = guard
                .withheld
                .as_ref()
                .is_some_and(|(producer_id, _, _)| *producer_id == c.producer_id);
            if creation_pending && guard.script.registration_survives_close() {
                guard.unmapped.insert(c.producer_id);
            } else {
                guard.names.retain(|_, (id, _)| *id != c.producer_id);
            }
            match guard.withheld.as_ref() {
                Some((producer_id, _, _)) if *producer_id == c.producer_id => guard.withheld.take(),
                _ => None,
            }
        }
    };
    if let Some((producer_id, request_id, _name)) = released {
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

/// Retry policy for the recovery scenario. The default 2 s initial backoff
/// does not fit inside [`OPERATION_TIMEOUT`], and the point under test is the
/// SEQUENCE of recovery attempts, not the wait between them.
fn fast_operation_retry() -> magnetar_proto::OperationRetryConfig {
    magnetar_proto::OperationRetryConfig {
        initial_backoff: Duration::from_millis(5),
        max_backoff: Duration::from_millis(20),
        max_retries: Some(8),
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
            let (host_port, state) = spawn_broker(NameScript::WithholdReleasedByClose(
                PRODUCER_NAME.to_owned(),
            ))
            .await;
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
            let (host_port, state) = spawn_broker(NameScript::Plain).await;
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

/// The interleaving that reproduced against a real Pulsar 4.0.4 broker in CI
/// after ADR-0100's cancel-time close landed: the broker consumes the close
/// while the producer creation is still pending, acks it, stops mapping that
/// producer id, and completes the registration anyway. No close can address
/// that registration any more, so the pinned name is wedged and every retry —
/// each with a fresh producer id — is answered `ProducerBusy` until the
/// operation budget runs out.
///
/// Recovery is two-phase and must complete inside one `open_producer` call:
/// the first `ProducerBusy` re-closes the abandoned id, and when the name is
/// still busy the next attempt re-attaches under that id as a strict
/// successor, which the broker accepts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_busy_from_a_surviving_registration_is_recovered() {
    const PRODUCER_NAME: &str = "pinned-406";
    const TOPIC: &str = "persistent://public/default/open-cancel-surviving-registration";

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, state) =
                spawn_broker(NameScript::WithholdSurvivingClose(PRODUCER_NAME.to_owned())).await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = Client::connect_plain(&engine, &host_port, client_config())
                .await
                .expect("connect ok")
                .with_operation_retry(fast_operation_retry());

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

            let recovered = client
                .open_producer(CreateProducerRequest {
                    topic: TOPIC.to_owned(),
                    producer_name: Some(PRODUCER_NAME.to_owned()),
                    ..Default::default()
                })
                .await
                .expect("a registration that outlived its close must still be recoverable");

            assert_eq!(
                close_producer_count(&state),
                2,
                "one close from the cancellation, one from the ProducerBusy recovery"
            );
            recovered.close().await.expect("close ok");
            client.close().await;
        })
        .await;
}
