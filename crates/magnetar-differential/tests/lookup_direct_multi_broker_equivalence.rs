// SPDX-License-Identifier: Apache-2.0

//! ADR-0039 §"Multi-broker DIRECT routing (2026-06-01)" — tokio ↔ moonpool
//! engine event-stream equivalence.
//!
//! Per ADR-0024 four-layer test rule. The HIGH-1 fix from the lookup
//! multi-agent review changes how the runtime routes data ops after a
//! `LookupOutcome::Connect { broker_service_url: Some(_),
//! proxy_through_service_url: false }` (DIRECT-with-a-broker-URL): both
//! engines open a pinned pool entry that dials the resolved broker
//! directly. Their *proto-level* outcome is identical — both observe the
//! same `OpOutcome::LookupResponse` and decode `broker_service_url` to
//! the same `Some(url)` value, which is the load-bearing field
//! `resolve_target` reads to pick the pool entry.
//!
//! This test feeds the same scripted lookup-response bytes into both
//! engines' [`magnetar_proto::Connection`] surface and asserts:
//!
//! 1. Both engines decode the response to the same `OpOutcome::LookupResponse` shape.
//! 2. Both surface `broker_service_url` (Some(_)) on the DIRECT path — the load-bearing field for
//!    the multi-broker DIRECT routing decision in `Client::lookup_topic`.
//!
//! The portless-authority test below additionally stands up a pair of brokers
//! per engine and compares the resolver request and successful data-plane
//! route. This client-level layer catches post-lookup divergence that the
//! shared proto outcome cannot observe.

#![forbid(unsafe_code)]

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_differential::HANG_GUARD;
use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, FrameError, LookupOutcome, OpOutcome,
    SupervisorConfig, decode_one, encode_command, pb,
};
use moonpool_core::TokioProviders;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, PartialEq, Eq, Clone)]
struct LookupSnapshot {
    /// `broker_service_url` from the response — Some(url) on DIRECT with a
    /// broker URL, None on bootstrap-only DIRECT.
    broker_service_url: Option<String>,
    /// `proxy_through_service_url` — always false here (DIRECT path).
    proxy_through_service_url: bool,
}

fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-diff-direct".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandConnected");
    buf
}

fn lookup_response_bytes(request_id: u64, broker_url: Option<&str>) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::LookupResponse as i32,
        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
            broker_service_url: broker_url.map(ToOwned::to_owned),
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
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandLookupTopicResponse");
    buf
}

trait SharedConn: Send + Sync {
    fn lock(&self) -> parking_lot::MutexGuard<'_, Connection>;
}

struct TokioShared(Arc<magnetar_runtime_tokio::ConnectionShared>);
struct MoonpoolShared(Arc<magnetar_runtime_moonpool::ConnectionShared>);

impl SharedConn for TokioShared {
    fn lock(&self) -> parking_lot::MutexGuard<'_, Connection> {
        self.0.inner.lock()
    }
}
impl SharedConn for MoonpoolShared {
    fn lock(&self) -> parking_lot::MutexGuard<'_, Connection> {
        self.0.inner.lock()
    }
}

/// Drive an engine's [`Connection`] through a handshake + a single LOOKUP
/// round-trip that resolves to `broker_url` on the DIRECT path. Returns
/// the user-facing [`LookupSnapshot`] the engine would feed into
/// `Client::resolve_target`.
fn drive_direct_lookup<F>(make_shared: F, broker_url: Option<&str>) -> LookupSnapshot
where
    F: FnOnce(ConnectionConfig) -> Arc<dyn SharedConn>,
{
    let shared = make_shared(ConnectionConfig::default());
    let start = Instant::now();

    {
        let mut conn = shared.lock();
        conn.begin_handshake().expect("begin_handshake");
        let _ = conn.poll_transmit();
        conn.handle_bytes(start, &handshake_response_bytes())
            .expect("handshake");
    }

    // Issue the LOOKUP.
    let request_id = {
        let mut conn = shared.lock();
        conn.lookup("persistent://public/default/diff-direct-lookup", false)
    };
    {
        let mut conn = shared.lock();
        let _ = conn.poll_transmit();
        conn.handle_bytes(start, &lookup_response_bytes(request_id.0, broker_url))
            .expect("lookup response");
    }

    // Drain events until we find the LookupResponse for our request_id.
    let mut conn = shared.lock();
    while conn.poll_event().is_some() {}
    // Pull the outcome directly (proto correlates by request_id).
    let outcome = conn
        .take_outcome(magnetar_proto::PendingOpKey::Request(request_id))
        .expect("lookup outcome present");
    match outcome {
        OpOutcome::LookupResponse {
            outcome:
                LookupOutcome::Connect {
                    broker_service_url,
                    proxy_through_service_url,
                    ..
                },
            ..
        } => LookupSnapshot {
            broker_service_url,
            proxy_through_service_url,
        },
        other => panic!("expected LookupResponse → Connect, got {other:?}"),
    }
}

/// Both engines must agree on the LOOKUP outcome the runtime uses to
/// pick its routing decision — the DIRECT with a specific broker URL
/// case, the load-bearing one for ADR-0039 §"Multi-broker DIRECT routing
/// (2026-06-01)".
#[test]
fn tokio_and_moonpool_observe_the_same_direct_lookup_outcome() {
    let broker_url = "pulsar://other-broker.cluster.internal:6650";

    let tokio_snap = drive_direct_lookup(
        |cfg| {
            Arc::new(TokioShared(magnetar_runtime_tokio::ConnectionShared::new(
                cfg,
            )))
        },
        Some(broker_url),
    );
    let moonpool_snap = drive_direct_lookup(
        |cfg| {
            Arc::new(MoonpoolShared(
                magnetar_runtime_moonpool::ConnectionShared::new(cfg),
            ))
        },
        Some(broker_url),
    );

    assert_eq!(
        tokio_snap, moonpool_snap,
        "tokio and moonpool engines decoded the DIRECT-with-broker-url lookup differently:\n\
         tokio    = {tokio_snap:?}\n\
         moonpool = {moonpool_snap:?}",
    );
    assert_eq!(
        tokio_snap.broker_service_url.as_deref(),
        Some(broker_url),
        "broker_service_url must be surfaced verbatim — the runtime parses it for the dial",
    );
    assert!(
        !tokio_snap.proxy_through_service_url,
        "DIRECT path implies proxy_through_service_url = false",
    );
}

/// And the degenerate single-broker case: both engines decode `None`
/// identically (this is the bootstrap-equality fast path on the runtime
/// side, observed at the proto layer as `broker_service_url = None`).
#[test]
fn tokio_and_moonpool_observe_the_same_lookup_outcome_without_broker_url() {
    let tokio_snap = drive_direct_lookup(
        |cfg| {
            Arc::new(TokioShared(magnetar_runtime_tokio::ConnectionShared::new(
                cfg,
            )))
        },
        None,
    );
    let moonpool_snap = drive_direct_lookup(
        |cfg| {
            Arc::new(MoonpoolShared(
                magnetar_runtime_moonpool::ConnectionShared::new(cfg),
            ))
        },
        None,
    );

    assert_eq!(tokio_snap, moonpool_snap);
    assert!(
        tokio_snap.broker_service_url.is_none(),
        "single-broker LOOKUP must surface None",
    );
}

const PORTLESS_BROKER_HOST: &str = "broker-b.internal";
const PORTLESS_TOPIC: &str = "persistent://public/default/diff-direct-portless-broker";

#[derive(Debug, PartialEq, Eq, Clone)]
struct PortlessDirectObservation {
    requested_authority: (String, u16),
    producer_reached_resolved_broker: bool,
}

#[derive(Debug, Default, Clone)]
struct BrokerSession {
    frames: Vec<i32>,
}

#[derive(Debug, Clone)]
struct BrokerRole {
    redirect_to: Option<String>,
}

async fn spawn_client_broker(
    role: BrokerRole,
) -> (
    SocketAddr,
    String,
    String,
    Arc<parking_lot::Mutex<Vec<BrokerSession>>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let address = listener.local_addr().expect("broker address");
    let sessions = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let sessions_for_task = sessions.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            let session_index = {
                let mut sessions = sessions_for_task.lock();
                sessions.push(BrokerSession::default());
                sessions.len() - 1
            };
            let sessions = sessions_for_task.clone();
            let role = role.clone();
            tokio::spawn(async move {
                let _ = handle_client_broker_session(stream, &sessions, session_index, &role).await;
            });
        }
    });
    (
        address,
        address.to_string(),
        format!("pulsar://{address}"),
        sessions,
    )
}

async fn handle_client_broker_session(
    mut stream: tokio::net::TcpStream,
    sessions: &Arc<parking_lot::Mutex<Vec<BrokerSession>>>,
    session_index: usize,
    role: &BrokerRole,
) -> std::io::Result<()> {
    let mut read_buffer = BytesMut::with_capacity(8 * 1024);
    let mut output_buffer = BytesMut::with_capacity(8 * 1024);
    loop {
        loop {
            let mut framed = read_buffer.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(frame) => frame,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return Ok(()),
            };
            let consumed = before - framed.len();
            let _ = read_buffer.split_to(consumed);
            if !matches!(
                pb::base_command::Type::try_from(frame.command.r#type).ok(),
                Some(pb::base_command::Type::Connect)
            ) {
                sessions.lock()[session_index]
                    .frames
                    .push(frame.command.r#type);
            }
            handle_client_broker_frame(&frame, &mut output_buffer, role);
        }

        if !output_buffer.is_empty() {
            stream.write_all(&output_buffer).await?;
            stream.flush().await?;
            output_buffer.clear();
        }
        match stream.read_buf(&mut read_buffer).await {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(error) => return Err(error),
        }
    }
}

fn handle_client_broker_frame(
    frame: &magnetar_proto::Frame,
    output: &mut BytesMut,
    role: &BrokerRole,
) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    let command = match kind {
        pb::base_command::Type::Connect => Some(pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-diff-portless-direct".to_owned(),
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
                .map(|lookup| pb::BaseCommand {
                    r#type: pb::base_command::Type::LookupResponse as i32,
                    lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                        broker_service_url: role.redirect_to.clone(),
                        broker_service_url_tls: None,
                        response: Some(
                            pb::command_lookup_topic_response::LookupType::Connect as i32,
                        ),
                        request_id: lookup.request_id,
                        authoritative: Some(true),
                        error: None,
                        message: None,
                        proxy_through_service_url: Some(false),
                    }),
                    ..Default::default()
                })
        }
        pb::base_command::Type::Producer => {
            frame
                .command
                .producer
                .as_ref()
                .map(|producer| pb::BaseCommand {
                    r#type: pb::base_command::Type::ProducerSuccess as i32,
                    producer_success: Some(pb::CommandProducerSuccess {
                        request_id: producer.request_id,
                        producer_name: "diff-portless-direct".to_owned(),
                        last_sequence_id: Some(-1),
                        schema_version: None,
                        topic_epoch: Some(0),
                        producer_ready: Some(true),
                    }),
                    ..Default::default()
                })
        }
        _ => None,
    };
    if let Some(command) = command {
        let _ = encode_command(output, &command);
    }
}

fn producer_reached(sessions: &[BrokerSession]) -> bool {
    sessions.iter().any(|session| {
        session
            .frames
            .contains(&(pb::base_command::Type::Producer as i32))
    })
}

#[derive(Debug)]
struct TokioRecordingResolver {
    mapped_address: SocketAddr,
    requests: Arc<parking_lot::Mutex<Vec<(String, u16)>>>,
}

impl magnetar_runtime_tokio::DnsResolver for TokioRecordingResolver {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> magnetar_runtime_tokio::DnsResolveFuture<'a> {
        let requested_host = host.to_owned();
        Box::pin(async move {
            self.requests.lock().push((requested_host, port));
            let address = if host == PORTLESS_BROKER_HOST {
                self.mapped_address
            } else {
                SocketAddr::new(
                    host.parse::<IpAddr>()
                        .expect("test resolver only receives its mapped host or an IP literal"),
                    port,
                )
            };
            Ok(vec![address])
        })
    }
}

#[derive(Debug)]
struct MoonpoolRecordingResolver {
    mapped_address: SocketAddr,
    requests: Arc<parking_lot::Mutex<Vec<(String, u16)>>>,
}

impl magnetar_runtime_moonpool::DnsResolver for MoonpoolRecordingResolver {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> magnetar_runtime_moonpool::DnsResolveFuture<'a> {
        let requested_host = host.to_owned();
        Box::pin(async move {
            self.requests.lock().push((requested_host, port));
            let address = if host == PORTLESS_BROKER_HOST {
                self.mapped_address
            } else {
                SocketAddr::new(
                    host.parse::<IpAddr>()
                        .expect("test resolver only receives its mapped host or an IP literal"),
                    port,
                )
            };
            Ok(vec![address])
        })
    }
}

fn resolved_request(requests: &parking_lot::Mutex<Vec<(String, u16)>>) -> Option<(String, u16)> {
    requests
        .lock()
        .iter()
        .find(|(host, _port)| host == PORTLESS_BROKER_HOST)
        .cloned()
}

async fn tokio_direct_observation(
    advertised_broker: &str,
) -> Result<PortlessDirectObservation, String> {
    let (broker_b_address, _host_b, _url_b, sessions_b) =
        spawn_client_broker(BrokerRole { redirect_to: None }).await;
    let (_broker_a_address, _host_a, url_a, _sessions_a) = spawn_client_broker(BrokerRole {
        redirect_to: Some(advertised_broker.to_owned()),
    })
    .await;
    let requests = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let resolver = Arc::new(TokioRecordingResolver {
        mapped_address: broker_b_address,
        requests: requests.clone(),
    });
    let client = tokio::time::timeout(
        HANG_GUARD,
        magnetar_runtime_tokio::Client::connect_with_resolver_and_provider(
            magnetar_runtime_tokio::ParsedUrl::parse(&url_a).map_err(|error| error.to_string())?,
            None,
            ConnectionConfig::default(),
            None,
            None,
            Some(resolver),
        ),
    )
    .await
    .map_err(|error| error.to_string())?
    .map_err(|error| error.to_string())?;
    let producer_result = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: PORTLESS_TOPIC.to_owned(),
            ..Default::default()
        }),
    )
    .await
    .map_err(|error| error.to_string())?;
    let observation = PortlessDirectObservation {
        requested_authority: resolved_request(&requests)
            .ok_or_else(|| "tokio never resolved the advertised broker".to_owned())?,
        producer_reached_resolved_broker: producer_reached(&sessions_b.lock()),
    };
    if let Some(driver) = client.take_driver() {
        driver.abort();
    }
    drop(client);
    producer_result.map_err(|error| error.to_string())?;
    Ok(observation)
}

async fn moonpool_direct_observation(
    advertised_broker: &str,
) -> Result<PortlessDirectObservation, String> {
    let (broker_b_address, _host_b, _url_b, sessions_b) =
        spawn_client_broker(BrokerRole { redirect_to: None }).await;
    let (_broker_a_address, host_a, _url_a, _sessions_a) = spawn_client_broker(BrokerRole {
        redirect_to: Some(advertised_broker.to_owned()),
    })
    .await;
    let requests = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let resolver = Arc::new(MoonpoolRecordingResolver {
        mapped_address: broker_b_address,
        requests: requests.clone(),
    });
    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let config = ConnectionConfig {
        supervisor: Some(SupervisorConfig::default()),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(
        HANG_GUARD,
        magnetar_runtime_moonpool::Client::connect_plain_supervised(
            &engine,
            &host_a,
            config,
            None,
            Some(resolver),
        ),
    )
    .await
    .map_err(|error| error.to_string())?
    .map_err(|error| error.to_string())?;
    let producer_result = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: PORTLESS_TOPIC.to_owned(),
            ..Default::default()
        }),
    )
    .await
    .map_err(|error| error.to_string())?;
    let requested_authority = resolved_request(&requests);
    let producer_reached_resolved_broker = producer_reached(&sessions_b.lock());
    if let Some(driver) = client.take_driver() {
        driver.abort();
    }
    drop(client);
    producer_result.map_err(|error| error.to_string())?;
    Ok(PortlessDirectObservation {
        requested_authority: requested_authority
            .ok_or_else(|| "moonpool never resolved the advertised broker".to_owned())?,
        producer_reached_resolved_broker,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn portless_direct_broker_resolution_is_engine_equivalent() {
    let expected = PortlessDirectObservation {
        requested_authority: (PORTLESS_BROKER_HOST.to_owned(), 6650),
        producer_reached_resolved_broker: true,
    };
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let tokio_observation = tokio_direct_observation(PORTLESS_BROKER_HOST)
                .await
                .expect("tokio must route through the portless broker");
            assert_eq!(tokio_observation, expected, "tokio observation changed");

            let moonpool_observation = moonpool_direct_observation(PORTLESS_BROKER_HOST).await;
            assert_eq!(
                moonpool_observation,
                Ok(expected),
                "moonpool must match Tokio's portless DIRECT routing observation",
            );
        })
        .await;
}

/// URI schemes are ASCII case-insensitive. Both runtime adapters must
/// therefore preserve the behavior Tokio's `url::Url` parser provided before
/// authority normalization moved into `magnetar-proto`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uppercase_pulsar_scheme_is_engine_equivalent() {
    let expected = PortlessDirectObservation {
        requested_authority: (PORTLESS_BROKER_HOST.to_owned(), 6650),
        producer_reached_resolved_broker: true,
    };
    let advertised = format!("PULSAR://{PORTLESS_BROKER_HOST}");
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let tokio_observation = tokio_direct_observation(&advertised)
                .await
                .expect("Tokio must accept an uppercase Pulsar scheme");
            let moonpool_observation = moonpool_direct_observation(&advertised)
                .await
                .expect("Moonpool must accept an uppercase Pulsar scheme");

            assert_eq!(tokio_observation, expected, "Tokio observation changed");
            assert_eq!(
                moonpool_observation, expected,
                "Moonpool must match Tokio for case-insensitive schemes",
            );
        })
        .await;
}

/// Tokio preserves an advertised TLS scheme while canonicalizing the
/// authority. The resolver request is the externally visible seam: a
/// portless `pulsar+ssl://` target must use 6651 even when the bootstrap was
/// plaintext. The deliberately closed socket keeps this a routing test rather
/// than a TLS-fixture test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_tls_direct_broker_uses_the_tls_default_port() {
    let closed_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("closed target bind");
    let closed_address = closed_listener.local_addr().expect("closed target address");
    drop(closed_listener);

    let advertised = format!("pulsar+ssl://{PORTLESS_BROKER_HOST}");
    let (_broker_a_address, _host_a, url_a, _sessions_a) = spawn_client_broker(BrokerRole {
        redirect_to: Some(advertised),
    })
    .await;
    let requests = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let resolver = Arc::new(TokioRecordingResolver {
        mapped_address: closed_address,
        requests: requests.clone(),
    });
    let config = ConnectionConfig {
        supervisor: Some(SupervisorConfig::default()),
        operation_timeout: Duration::from_secs(1),
        connect_timeout: Duration::from_millis(200),
        connect_max_retries: 0,
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(
        HANG_GUARD,
        magnetar_runtime_tokio::Client::connect_with_resolver_and_provider(
            magnetar_runtime_tokio::ParsedUrl::parse(&url_a).expect("bootstrap URL parse"),
            None,
            config,
            None,
            None,
            Some(resolver),
        ),
    )
    .await
    .expect("tokio connect did not time out")
    .expect("tokio bootstrap connect");

    let open_result = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: PORTLESS_TOPIC.to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("tokio TLS DIRECT routing did not time out");
    let requested = resolved_request(&requests);
    if let Some(driver) = client.take_driver() {
        driver.abort();
    }
    drop(client);

    assert!(
        open_result.is_err(),
        "the deliberately closed TLS target must refuse the producer dial",
    );
    assert_eq!(
        requested,
        Some((PORTLESS_BROKER_HOST.to_owned(), 6651)),
        "the advertised TLS scheme must select its own default port",
    );
}

/// The proto helper is intentionally sans-I/O and therefore does not validate
/// DNS label syntax. Tokio's retained URL adapter must turn an authority it
/// cannot represent into the documented `ClientError::Other`, before DNS or a
/// producer frame reaches any broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_rejects_a_canonical_authority_its_url_adapter_cannot_represent() {
    const UNREPRESENTABLE_BROKER: &str = "broker name";

    let (_broker_a_address, _host_a, url_a, sessions_a) = spawn_client_broker(BrokerRole {
        redirect_to: Some(UNREPRESENTABLE_BROKER.to_owned()),
    })
    .await;
    let config = ConnectionConfig {
        supervisor: Some(SupervisorConfig::default()),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(
        HANG_GUARD,
        magnetar_runtime_tokio::Client::connect(&url_a, config),
    )
    .await
    .expect("tokio connect did not time out")
    .expect("tokio bootstrap connect");
    let error = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: PORTLESS_TOPIC.to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("tokio adapter rejection did not time out")
    .expect_err("an unrepresentable canonical authority must be rejected");
    let snapshot = sessions_a.lock().clone();
    if let Some(driver) = client.take_driver() {
        driver.abort();
    }
    drop(client);

    assert!(
        error
            .to_string()
            .contains("could not be represented as a Tokio dial target"),
        "unexpected adapter rejection: {error}",
    );
    assert_eq!(snapshot.len(), 1, "only the bootstrap session may exist");
    assert!(
        !producer_reached(&snapshot),
        "the adapter must reject before any producer frame is sent",
    );
}
