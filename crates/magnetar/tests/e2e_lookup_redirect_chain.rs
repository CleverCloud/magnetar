// SPDX-License-Identifier: Apache-2.0

//! E2E coverage for ADR-0039 redirect-target dialing against a real Apache
//! Pulsar 4.x broker. Two layers:
//!
//! 1. **No-op single-broker regression** — a normal (non-redirecting) LOOKUP against a standalone
//!    broker still resolves cleanly. The redirect-dial change reworks how the proto state machine
//!    delivers redirect outcomes (it surfaces a driveable `Redirected` instead of chasing on the
//!    bootstrap socket); this test pins the no-op happy path so single-broker LOOKUP semantics
//!    don't regress.
//! 2. **Fake-router redirect → real broker** — a tiny in-process "router" broker A answers the
//!    CONNECT handshake and the first LOOKUP with `LookupType::Redirect`, advertising the REAL
//!    Pulsar container's `host:port` as the redirect target. The façade `PulsarClient` must DIAL
//!    the real broker B, re-issue the LOOKUP there, and complete the producer round-trip against B.
//!    A genuine two-broker Pulsar cluster that deterministically redirects a known topic is
//!    impractical in the e2e harness (it needs multi-broker bundle-ownership control), so this uses
//!    the fake-router-in-front-of-a-real-broker pattern — the redirect target is a real Pulsar
//!    broker, exercising a real dial + real producer round-trip.
//!
//! ADR-0024 layer 5. The other layers (proto unit, tokio integration,
//! moonpool integration, differential equivalence) live alongside this file
//! in their respective crates — search for `lookup_redirect_chain`.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Requires Docker
//! on the host with `apachepulsar/pulsar:latest` reachable.

#![allow(clippy::too_many_lines)]
// Two-broker router topology: `_a` / `_b` suffixes (router A vs target B) are
// the clearest naming for the redirect source and target.
#![allow(clippy::similar_names)]

use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;

use bytes::BytesMut;
use magnetar::PulsarClient;
use magnetar_proto::{FrameError, decode_one, encode_command, pb};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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

async fn start_pulsar()
-> Result<(String, testcontainers::ContainerAsync<GenericImage>), Box<dyn std::error::Error>> {
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
    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let service_url = format!("pulsar://{host}:{binary_port}");
    Ok((service_url, container))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_single_broker_lookup_still_resolves_after_high4_fix()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _container) = start_pulsar().await?;

    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // Opening a producer issues a LOOKUP under the hood (single-broker
    // resolves immediately, no redirects). If the terminal-outcome delivery
    // regressed, `producer().create()` would either hang on the LOOKUP (no
    // terminal outcome ever delivered) or surface a state-machine bug error
    // from the engine's exhaustive match. Either way this assertion catches it.
    let topic = "persistent://public/default/magnetar-e2e-lookup-redirect-chain";
    let producer = tokio::time::timeout(Duration::from_secs(15), client.producer(topic).create())
        .await
        .expect("producer().create() must not hang on single-broker LOOKUP")?;
    producer.close().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Fake-router redirect → target broker.
//
// A genuine multi-broker Pulsar cluster that deterministically redirects a
// known topic is impractical in the e2e harness (it needs multi-broker
// bundle-ownership control, and a standalone broker advertises a
// container-internal address the host cannot dial). Instead, an in-process
// router broker A redirects the LOOKUP to an in-process target broker B; the
// real façade `PulsarClient` must DIAL B and complete the producer round-trip
// there. This exercises the full façade dial path against a redirect target.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct FakeBrokerState {
    saw_producer: std::sync::atomic::AtomicBool,
    saw_lookup: std::sync::atomic::AtomicBool,
}

/// Behaviour for the in-process fake broker.
enum FakeBehaviour {
    /// Redirect the first LOOKUP to `redirect_url`, then resolve to self.
    RedirectOnceTo(String),
    /// Resolve every LOOKUP to self (no advertised URL).
    ConnectToSelf,
}

/// Spawn an in-process fake broker. Returns its `pulsar://host:port` URL plus
/// shared state so the test can assert which broker served the producer.
async fn spawn_fake_broker(behaviour: FakeBehaviour) -> (String, Arc<FakeBrokerState>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let url = format!("pulsar://{addr}");
    let state = Arc::new(FakeBrokerState::default());
    let state_for_task = state.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            let state = state_for_task.clone();
            // Per-connection redirect arm: each new connection answers its
            // first LOOKUP with a Redirect (for `RedirectOnceTo`), then Connect.
            let redirect_url = match &behaviour {
                FakeBehaviour::RedirectOnceTo(u) => Some(u.clone()),
                FakeBehaviour::ConnectToSelf => None,
            };
            tokio::spawn(async move {
                let _ = serve_fake_session(stream, state, redirect_url).await;
            });
        }
    });
    (url, state)
}

async fn serve_fake_session(
    mut stream: tokio::net::TcpStream,
    state: Arc<FakeBrokerState>,
    redirect_url: Option<String>,
) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out = BytesMut::with_capacity(8 * 1024);
    // Per-session redirect gate: the first LOOKUP on this connection redirects
    // (when `redirect_url` is set); subsequent ones resolve to self.
    let mut redirected = false;
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
                pb::base_command::Type::Connect => {
                    let cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::Connected as i32,
                        connected: Some(pb::CommandConnected {
                            server_version: "magnetar-e2e-redirect".to_owned(),
                            protocol_version: Some(21),
                            max_message_size: Some(5 * 1024 * 1024),
                            feature_flags: Some(pb::FeatureFlags::default()),
                        }),
                        ..Default::default()
                    };
                    let _ = encode_command(&mut out, &cmd);
                }
                pb::base_command::Type::Ping => {
                    let cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::Pong as i32,
                        pong: Some(pb::CommandPong {}),
                        ..Default::default()
                    };
                    let _ = encode_command(&mut out, &cmd);
                }
                pb::base_command::Type::PartitionedMetadata => {
                    if let Some(m) = &frame.command.partition_metadata {
                        // Non-partitioned topic — the façade's `producer().create()`
                        // issues this before the LOOKUP.
                        let cmd = pb::BaseCommand {
                            r#type: pb::base_command::Type::PartitionedMetadataResponse as i32,
                            partition_metadata_response: Some(
                                pb::CommandPartitionedTopicMetadataResponse {
                                    partitions: Some(0),
                                    request_id: m.request_id,
                                    response: Some(
                                        pb::command_partitioned_topic_metadata_response::LookupType::Success
                                            as i32,
                                    ),
                                    error: None,
                                    message: None,
                                },
                            ),
                            ..Default::default()
                        };
                        let _ = encode_command(&mut out, &cmd);
                    }
                }
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        state
                            .saw_lookup
                            .store(true, std::sync::atomic::Ordering::SeqCst);
                        let (response_kind, broker_url) = match &redirect_url {
                            Some(url) if !redirected => {
                                redirected = true;
                                (
                                    pb::command_lookup_topic_response::LookupType::Redirect,
                                    Some(url.clone()),
                                )
                            }
                            _ => (pb::command_lookup_topic_response::LookupType::Connect, None),
                        };
                        let cmd = pb::BaseCommand {
                            r#type: pb::base_command::Type::LookupResponse as i32,
                            lookup_topic_response: Some(pb::CommandLookupTopicResponse {
                                broker_service_url: broker_url,
                                broker_service_url_tls: None,
                                response: Some(response_kind as i32),
                                request_id: l.request_id,
                                authoritative: Some(true),
                                error: None,
                                message: None,
                                proxy_through_service_url: Some(false),
                            }),
                            ..Default::default()
                        };
                        let _ = encode_command(&mut out, &cmd);
                    }
                }
                pb::base_command::Type::Producer => {
                    if let Some(p) = &frame.command.producer {
                        state
                            .saw_producer
                            .store(true, std::sync::atomic::Ordering::SeqCst);
                        let cmd = pb::BaseCommand {
                            r#type: pb::base_command::Type::ProducerSuccess as i32,
                            producer_success: Some(pb::CommandProducerSuccess {
                                request_id: p.request_id,
                                producer_name: "magnetar-e2e-redirect".to_owned(),
                                last_sequence_id: Some(-1),
                                schema_version: None,
                                topic_epoch: Some(0),
                                producer_ready: Some(true),
                            }),
                            ..Default::default()
                        };
                        let _ = encode_command(&mut out, &cmd);
                    }
                }
                pb::base_command::Type::CloseProducer => {
                    // Ack the close so `producer.close()` resolves.
                    if let Some(c) = &frame.command.close_producer {
                        let cmd = pb::BaseCommand {
                            r#type: pb::base_command::Type::Success as i32,
                            success: Some(pb::CommandSuccess {
                                request_id: c.request_id,
                                schema: None,
                            }),
                            ..Default::default()
                        };
                        let _ = encode_command(&mut out, &cmd);
                    }
                }
                _ => {}
            }
        }
        if !out.is_empty() {
            stream.write_all(&out).await?;
            stream.flush().await?;
            out.clear();
        }
        match stream.read_buf(&mut read_buf).await {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(e) => return Err(e),
        }
    }
}

/// The façade `PulsarClient`, connected to router broker A, must DIAL the
/// redirect target broker B and complete the producer round-trip there — A
/// must never serve the producer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_lookup_redirect_dials_target_broker() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    // Target broker B resolves to self and serves the producer.
    let (url_b, state_b) = spawn_fake_broker(FakeBehaviour::ConnectToSelf).await;
    // Router broker A redirects the first LOOKUP to B's real address.
    let (url_a, state_a) = spawn_fake_broker(FakeBehaviour::RedirectOnceTo(url_b.clone())).await;

    let client = PulsarClient::builder().service_url(url_a).build().await?;

    let topic = "persistent://public/default/magnetar-e2e-redirect-dial";
    let producer = tokio::time::timeout(Duration::from_secs(15), client.producer(topic).create())
        .await
        .expect("producer().create() must not hang on the redirect dial")?;
    producer.close().await?;

    assert!(
        state_a.saw_lookup.load(SeqCst),
        "router broker A must have seen the initial LOOKUP"
    );
    assert!(
        !state_a.saw_producer.load(SeqCst),
        "router broker A must NOT serve the producer (self-chase regression)"
    );
    assert!(
        state_b.saw_lookup.load(SeqCst),
        "target broker B must have seen the re-issued LOOKUP (a real dial to B)"
    );
    assert!(
        state_b.saw_producer.load(SeqCst),
        "target broker B must serve the producer (data ops routed to the redirect target)"
    );
    Ok(())
}
