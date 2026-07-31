// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for Moonpool DIRECT broker-authority normalization
//! through the public `PulsarClient<MoonpoolEngine<TokioProviders>>` facade.
//!
//! One Docker Pulsar broker serves two sequential scenarios:
//!
//! 1. the real broker advertises its ordinary `pulsar://host:port` URL and the runtime exercises
//!    bootstrap equality; and
//! 2. an in-process lookup bootstrap advertises the defensive portless shape
//!    `resolved-broker.internal`, while a recording resolver maps the logical
//!    `resolved-broker.internal:6650` request to that same Docker broker.
//!
//! The second scenario is ADR-0024's facade e2e witness for ADR-0091. It
//! reaches a real broker only if Moonpool applies the plaintext bootstrap
//! default before resolver dispatch and pool insertion. Keeping both paths in
//! one test preserves the repository's one-container budget.
//!
//! Gated on `feature = "moonpool"` because a moonpool-engine client cannot
//! compile without it (engine selection, not test-hiding — every other
//! `PulsarClient<MoonpoolEngine>` item in the façade carries the same gate;
//! the e2e suite always runs under `cargo test --all-features` per
//! ADR-0046, so the gate never hides the test from a normal run). No
//! `#[ignore]`.

#![cfg(feature = "moonpool")]

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar::proto::pb::command_subscribe::{InitialPosition, SubType};
use magnetar::proto::{FrameError, decode_one, encode_command, pb};
use magnetar::runtime_moonpool::{
    Client as MoonpoolClient, DnsResolveFuture, DnsResolver, MoonpoolEngine,
};
use magnetar::{OutgoingMessage, PulsarClient};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "latest";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;
const PORTLESS_BROKER_HOST: &str = "resolved-broker.internal";

/// JVM budget for the `pulsar standalone` container.
/// The image default (`-Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g`) costs ~2.3 GiB RSS per
/// container; libtest runs up to `nproc` e2e tests in parallel and the PIP-33 compose fixture
/// stays up for the whole run, which overcommits the 16 GiB GitHub runner and stalls brokers
/// into `operation_timeout` failures. See `docs/testing.md` § "e2e container memory budget".
const PULSAR_MEM_LIMIT: &str = "-Xms256m -Xmx1g -XX:MaxDirectMemorySize=1g";

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
        .with_startup_timeout(Duration::from_mins(2))
        .with_env_var("PULSAR_MEM", PULSAR_MEM_LIMIT)
        .with_cmd(vec!["bin/pulsar".to_owned(), "standalone".to_owned()])
        .start()
        .await?;
    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let service_url = format!("pulsar://{host}:{binary_port}");
    Ok((service_url, container))
}

#[derive(Debug)]
struct RecordingResolver {
    mapped_address: SocketAddr,
    requests: Arc<Mutex<Vec<(String, u16)>>>,
}

impl DnsResolver for RecordingResolver {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> DnsResolveFuture<'a> {
        let requested_host = host.to_owned();
        Box::pin(async move {
            self.requests.lock().push((requested_host, port));
            let address = if host == PORTLESS_BROKER_HOST {
                self.mapped_address
            } else {
                SocketAddr::new(
                    host.parse::<IpAddr>().map_err(|error| {
                        std::io::Error::new(std::io::ErrorKind::InvalidInput, error)
                    })?,
                    port,
                )
            };
            Ok(vec![address])
        })
    }
}

async fn start_portless_lookup_broker()
-> Result<(String, tokio::task::JoinHandle<()>), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let Ok((stream, _peer)) = listener.accept().await else {
            return;
        };
        let _ = serve_portless_lookup(stream).await;
    });
    Ok((address.to_string(), task))
}

async fn serve_portless_lookup(mut stream: tokio::net::TcpStream) -> std::io::Result<()> {
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
            if let Some(command) = portless_lookup_response(&frame) {
                let _ = encode_command(&mut output_buffer, &command);
            }
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

fn portless_lookup_response(frame: &magnetar::proto::Frame) -> Option<pb::BaseCommand> {
    let kind = pb::base_command::Type::try_from(frame.command.r#type).ok()?;
    match kind {
        pb::base_command::Type::Connect => Some(pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-e2e-portless-lookup".to_owned(),
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
                        broker_service_url: Some(PORTLESS_BROKER_HOST.to_owned()),
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
        _ => None,
    }
}

/// Pulsar 4 standalone advertises its own well-formed `pulsar://host:port`
/// URL on every lookup response's `broker_service_url`. The moonpool
/// engine's `Client::resolve_direct_broker` feeds that value through
/// `direct_broker_authority` (now `Result`-returning per issue #364's
/// hardening) to derive the DIRECT-routing dial target and the
/// bootstrap-equality comparison. This proves that happy path — a
/// real broker's real advertised URL — still resolves to `Ok(_)` and the
/// producer/consumer round-trip completes, i.e. the signature change from
/// `String` to `Result<String, ClientError>` did not regress ordinary
/// operation. The same test then inserts a lookup-only bootstrap that
/// advertises a bare host and verifies the public facade reaches this real
/// broker through a resolver request for port 6650.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_moonpool_open_producer_against_standalone_after_direct_lookup()
-> Result<(), Box<dyn std::error::Error>> {
    let (service_url, _container) = start_pulsar().await?;
    let host_port = service_url
        .strip_prefix("pulsar://")
        .unwrap_or(&service_url)
        .to_owned();

    let runtime_client = MoonpoolClient::connect_plain_supervised(
        &MoonpoolEngine::new(TokioProviders::new()),
        &host_port,
        magnetar_proto::ConnectionConfig {
            operation_timeout: Duration::from_secs(30),
            ..Default::default()
        },
        None,
        None,
    )
    .await?;
    let client = PulsarClient::from_runtime_client(runtime_client);

    let topic = "persistent://public/default/magnetar-e2e-moonpool-direct-broker-authority";

    // First producer — exercises LOOKUP -> resolve_target ->
    // resolve_direct_broker -> direct_broker_authority(..)? on a real
    // broker's real advertised URL.
    let p1 = client.producer(topic).create().await?;
    p1.send(OutgoingMessage::with_payload(b"hello".to_vec()).into())
        .await?;

    // Second producer on the same topic — exercises the bootstrap-equality
    // fast path (physical == pool.bootstrap_addr()) a second time.
    let p2 = client.producer(topic).create().await?;
    p2.send(OutgoingMessage::with_payload(b"world".to_vec()).into())
        .await?;
    p1.close().await?;
    p2.close().await?;

    let consumer = client
        .consumer(topic)
        .subscription("magnetar-e2e-moonpool-direct-broker-authority-sub")
        .subscription_type(SubType::Exclusive)
        .initial_position(InitialPosition::Earliest)
        .subscribe()
        .await?;
    let mut received = Vec::new();
    for _ in 0..2 {
        let msg = consumer.receive().await?;
        received.push(msg.payload.to_vec());
        consumer.ack(msg.message_id).await?;
    }
    consumer.close().await?;
    client.close().await;

    assert_eq!(
        received,
        vec![b"hello".to_vec(), b"world".to_vec()],
        "two messages must round-trip through direct_broker_authority's Ok(_) happy path",
    );

    // Facade-level ADR-0024 witness for a portless DIRECT lookup target. The
    // bootstrap stub emits the defensive wire shape real brokers normally do
    // not: a scheme-less hostname with no port. The resolver records the
    // logical target and maps it to the same real Docker broker used above.
    let resolved_authority = service_url
        .strip_prefix("pulsar://")
        .unwrap_or(&service_url);
    let resolved_address = tokio::net::lookup_host(resolved_authority)
        .await?
        .next()
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                format!("Docker broker authority {resolved_authority:?} did not resolve"),
            )
        })?;
    let (bootstrap_address, _bootstrap_task) = start_portless_lookup_broker().await?;

    let requests = Arc::new(Mutex::new(Vec::new()));
    let resolver = Arc::new(RecordingResolver {
        mapped_address: resolved_address,
        requests: requests.clone(),
    });
    let runtime_client = MoonpoolClient::connect_plain_supervised(
        &MoonpoolEngine::new(TokioProviders::new()),
        &bootstrap_address,
        magnetar_proto::ConnectionConfig {
            supervisor: Some(magnetar_proto::SupervisorConfig::default()),
            operation_timeout: Duration::from_secs(30),
            ..Default::default()
        },
        None,
        Some(resolver),
    )
    .await?;
    let client = PulsarClient::from_runtime_client(runtime_client);
    let producer = client
        .producer("persistent://public/default/magnetar-e2e-moonpool-portless-direct")
        .create()
        .await?;
    producer
        .send(OutgoingMessage::with_payload(b"portless-direct".to_vec()).into())
        .await?;
    producer.close().await?;
    client.close().await;

    let resolver_requests = requests.lock().clone();
    assert!(
        resolver_requests
            .iter()
            .any(|(host, port)| host == PORTLESS_BROKER_HOST && *port == BROKER_BINARY_PORT),
        "the facade must resolve the portless DIRECT broker as \
         {PORTLESS_BROKER_HOST}:{BROKER_BINARY_PORT}; got {resolver_requests:?}",
    );
    Ok(())
}
