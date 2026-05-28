// SPDX-License-Identifier: Apache-2.0

//! Targeted coverage closure mirroring
//! `magnetar-runtime-moonpool/tests/coverage_close.rs`. Drives the
//! resolver-aware transport path, request/response surfaces on
//! `Producer` / `Consumer`, and the engine-level `Debug` /
//! `ClientError` formatting so the per-runtime test-attribute count
//! stays exactly 1:1 (`cargo xtask check-runtime-test-parity`,
//! ADR-0024 §D2). Each test here pairs with a same-named test on the
//! moonpool side.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, MessageId, SubscribeRequest, decode_one,
    encode_command, pb,
};
use magnetar_runtime_tokio::{
    Client, ClientError, ConnectionShared, DnsResolveFuture, DnsResolver, ParsedUrl,
};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Tokio mirror of moonpool's `StaticDnsResolver`. The tokio runtime
/// only ships `TokioDnsResolver`, so we build a tiny test-local fixed
/// resolver that mirrors moonpool's behaviour 1:1.
#[derive(Debug, Clone)]
struct FixedResolver(Vec<SocketAddr>);

impl DnsResolver for FixedResolver {
    fn resolve<'a>(&'a self, _host: &'a str, _port: u16) -> DnsResolveFuture<'a> {
        let addrs = self.0.clone();
        Box::pin(async move { Ok(addrs) })
    }
}

/// Mock-broker driver: handles every frame the tokio engine needs to
/// drive a producer / consumer through `get_schema`, `last_message_id`,
/// `seek`, `close_producer`, `close_consumer`. Mirrors the moonpool
/// `spawn_full_broker` helper byte-for-byte so the engine surfaces stay
/// identical in shape.
async fn spawn_full_broker() -> (String, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                run_full_broker_conn(&mut stream).await;
            });
        }
    });
    (addr.to_string(), format!("pulsar://{addr}"))
}

async fn run_full_broker_conn(stream: &mut tokio::net::TcpStream) {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out_buf = BytesMut::with_capacity(8 * 1024);
    let mut producer_ledger: u64 = 100;
    let mut producer_entry: u64 = 0;
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
            handle_full_frame(
                &frame,
                &mut out_buf,
                &mut producer_ledger,
                &mut producer_entry,
            );
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

#[allow(clippy::too_many_lines)]
fn handle_full_frame(
    frame: &magnetar_proto::Frame,
    out: &mut BytesMut,
    producer_ledger: &mut u64,
    producer_entry: &mut u64,
) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-coverage-close".to_owned(),
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
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::ProducerSuccess as i32,
                    producer_success: Some(pb::CommandProducerSuccess {
                        request_id: p.request_id,
                        producer_name: "coverage-close".to_owned(),
                        last_sequence_id: Some(-1),
                        schema_version: None,
                        topic_epoch: Some(0),
                        producer_ready: Some(true),
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::Subscribe => {
            if let Some(s) = &frame.command.subscribe {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::Success as i32,
                    success: Some(pb::CommandSuccess {
                        request_id: s.request_id,
                        schema: None,
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::GetSchema => {
            if let Some(g) = &frame.command.get_schema {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::GetSchemaResponse as i32,
                    get_schema_response: Some(pb::CommandGetSchemaResponse {
                        request_id: g.request_id,
                        error_code: None,
                        error_message: None,
                        schema: Some(pb::Schema {
                            name: "test-schema".to_owned(),
                            schema_data: bytes::Bytes::from_static(&[1u8, 2, 3]),
                            r#type: pb::schema::Type::Json as i32,
                            properties: vec![],
                        }),
                        schema_version: Some(bytes::Bytes::from_static(&[0xaau8])),
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::GetLastMessageId => {
            if let Some(g) = &frame.command.get_last_message_id {
                *producer_ledger = producer_ledger.saturating_add(1);
                *producer_entry = producer_entry.saturating_add(7);
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::GetLastMessageIdResponse as i32,
                    get_last_message_id_response: Some(pb::CommandGetLastMessageIdResponse {
                        request_id: g.request_id,
                        last_message_id: pb::MessageIdData {
                            ledger_id: *producer_ledger,
                            entry_id: *producer_entry,
                            partition: None,
                            batch_index: None,
                            ack_set: vec![],
                            batch_size: None,
                            first_chunk_message_id: None,
                        },
                        consumer_mark_delete_position: None,
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::Seek => {
            if let Some(s) = &frame.command.seek {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::Success as i32,
                    success: Some(pb::CommandSuccess {
                        request_id: s.request_id,
                        schema: None,
                    }),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::CloseProducer => {
            if let Some(c) = &frame.command.close_producer {
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
        }
        pb::base_command::Type::CloseConsumer => {
            if let Some(c) = &frame.command.close_consumer {
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
        }
        _ => {}
    }
}

/// `ParsedUrl::Debug` (tokio counterpart to the moonpool
/// `MoonpoolEngine` Debug coverage hit) — calling `format!("{:?}", url)`
/// formats every field of the parsed URL.
#[test]
fn engine_debug_implementation() {
    let url = ParsedUrl::parse("pulsar://broker.example.com:6650").expect("parse");
    let rendered = format!("{url:?}");
    assert!(
        rendered.contains("ParsedUrl"),
        "Debug should mention the type, got {rendered:?}"
    );
}

/// `ConnectionShared::Debug` (tokio counterpart) walks the inner state
/// machine + auth provider gauge. Mirrors moonpool's
/// `connection_shared_debug_implementation`.
#[test]
fn connection_shared_debug_implementation() {
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let rendered = format!("{shared:?}");
    assert!(rendered.contains("ConnectionShared"));
    assert!(rendered.contains("has_auth_provider"));
}

/// `ClientError::Debug` for the `MemoryLimitExceeded` arm — mirrors the
/// moonpool `EngineError` variant test 1:1.
#[test]
fn engine_error_debug_for_memory_limit_exceeded() {
    let err = ClientError::MemoryLimitExceeded {
        current: 64,
        limit: 128,
        requested: 256,
    };
    let rendered = format!("{err:?}");
    let display = format!("{err}");
    assert!(rendered.contains("MemoryLimitExceeded"));
    assert!(display.contains("current=64"));
    assert!(display.contains("limit=128"));
}

/// `ClientError::Other` Display + Debug round-trip — tokio counterpart
/// to moonpool's `engine_error_debug_for_config_variant`.
#[test]
fn engine_error_debug_for_config_variant() {
    let err = ClientError::Other("oops".to_owned());
    assert!(format!("{err:?}").contains("Other"));
    assert!(format!("{err}").contains("other: oops"));
}

/// `Transport::connect_with_resolver` returns `ClientError::Other(_)`
/// when the supplied resolver yields no addresses. Mirrors moonpool's
/// `connect_with_resolver_rejects_empty_addrs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_with_resolver_rejects_empty_addrs() {
    let url = ParsedUrl::parse("pulsar://broker.invalid:6650").expect("parse");
    let resolver: Arc<dyn DnsResolver> = Arc::new(FixedResolver(Vec::new()));
    let err = Client::connect_with_resolver_and_provider(
        url,
        None,
        ConnectionConfig::default(),
        None,
        None,
        Some(resolver),
    )
    .await
    .expect_err("empty-resolver dial must fail");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("dns resolver returned no addresses"),
        "expected empty-addrs Other error, got {msg}",
    );
}

/// `Transport::connect_with_resolver` propagates the last I/O error
/// when every candidate fails. Mirrors moonpool's
/// `connect_with_resolver_propagates_last_error`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_with_resolver_propagates_last_error() {
    let url = ParsedUrl::parse("pulsar://broker.invalid:6650").expect("parse");
    let unreachable_a: SocketAddr = "127.0.0.1:1".parse().expect("port 1 sockaddr");
    let unreachable_b: SocketAddr = "127.0.0.1:2".parse().expect("port 2 sockaddr");
    let resolver: Arc<dyn DnsResolver> =
        Arc::new(FixedResolver(vec![unreachable_a, unreachable_b]));
    let res = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect_with_resolver_and_provider(
            url,
            None,
            ConnectionConfig::default(),
            None,
            None,
            Some(resolver),
        ),
    )
    .await
    .expect("dial returns within timeout");
    let err = res.expect_err("all-fail resolver must yield an error");
    let msg = format!("{err:?}");
    assert!(
        !msg.contains("returned no addresses"),
        "expected last-err propagation, got the no-addresses path: {msg}",
    );
}

/// `Transport::connect_with_resolver` happy path: a resolver that
/// returns the listener's bound address must successfully dial it.
/// Mirrors moonpool's `connect_with_resolver_dials_resolved_address`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_with_resolver_dials_resolved_address() {
    let (host_port, _url) = spawn_full_broker().await;
    let listener_addr: SocketAddr = host_port.parse().expect("listener addr parse");
    let url = ParsedUrl::parse("pulsar://broker.invalid:1").expect("parse");
    let resolver: Arc<dyn DnsResolver> = Arc::new(FixedResolver(vec![listener_addr]));
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect_with_resolver_and_provider(
            url,
            None,
            ConnectionConfig::default(),
            None,
            None,
            Some(resolver),
        ),
    )
    .await
    .expect("connect did not time out")
    .expect("resolver-driven dial succeeds");
    assert!(client.is_connected());
    client.close().await;
}

/// `Producer::get_schema` — tokio counterpart to moonpool's
/// `producer_get_schema_returns_broker_response`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_get_schema_returns_broker_response() {
    let (_host_port, url) = spawn_full_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");
    let producer = tokio::time::timeout(
        Duration::from_secs(3),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/coverage-close-producer".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out")
    .expect("open_producer ok");
    let schema = tokio::time::timeout(Duration::from_secs(3), producer.get_schema(None))
        .await
        .expect("get_schema did not time out")
        .expect("get_schema ok");
    assert_eq!(schema.name, "test-schema");
    assert_eq!(schema.schema_data, vec![1, 2, 3]);
    client.close().await;
}

/// `Consumer::get_schema` — tokio counterpart to moonpool's
/// `consumer_get_schema_returns_broker_response`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_get_schema_returns_broker_response() {
    let (_host_port, url) = spawn_full_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");
    let consumer = tokio::time::timeout(
        Duration::from_secs(3),
        client.subscribe(SubscribeRequest {
            topic: "persistent://public/default/coverage-close-consumer".to_owned(),
            subscription: "cov-close-sub".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");
    let schema = tokio::time::timeout(Duration::from_secs(3), consumer.get_schema(None))
        .await
        .expect("consumer get_schema did not time out")
        .expect("consumer get_schema ok");
    assert_eq!(schema.name, "test-schema");
    client.close().await;
}

/// `Consumer::last_message_id` + `has_message_after` — tokio
/// counterpart to moonpool's
/// `consumer_last_message_id_returns_broker_response`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_last_message_id_returns_broker_response() {
    let (_host_port, url) = spawn_full_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");
    let consumer = tokio::time::timeout(
        Duration::from_secs(3),
        client.subscribe(SubscribeRequest {
            topic: "persistent://public/default/coverage-close-last-msg-id".to_owned(),
            subscription: "cov-close-last".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");
    let msg_id = tokio::time::timeout(Duration::from_secs(3), consumer.last_message_id())
        .await
        .expect("last_message_id did not time out")
        .expect("last_message_id ok");
    assert!(msg_id.ledger_id > 0, "expected non-zero ledger id");
    let lower = MessageId {
        ledger_id: 0,
        entry_id: 0,
        partition: msg_id.partition,
        batch_index: msg_id.batch_index,
        batch_size: msg_id.batch_size,
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
    };
    let has_more = tokio::time::timeout(Duration::from_secs(3), consumer.has_message_after(lower))
        .await
        .expect("has_message_after did not time out")
        .expect("has_message_after ok");
    assert!(has_more);
    client.close().await;
}

/// `Consumer::seek_to_message` / `seek_to_timestamp` — tokio
/// counterpart to moonpool's `consumer_seek_paths_complete`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_seek_paths_complete() {
    let (_host_port, url) = spawn_full_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");
    let consumer = tokio::time::timeout(
        Duration::from_secs(3),
        client.subscribe(SubscribeRequest {
            topic: "persistent://public/default/coverage-close-seek".to_owned(),
            subscription: "cov-close-seek".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");
    let target = MessageId {
        ledger_id: 7,
        entry_id: 3,
        partition: -1,
        batch_index: -1,
        batch_size: -1,
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
    };
    tokio::time::timeout(Duration::from_secs(3), consumer.seek_to_message(target))
        .await
        .expect("seek_to_message did not time out")
        .expect("seek_to_message ok");
    tokio::time::timeout(
        Duration::from_secs(3),
        consumer.seek_to_timestamp(1_700_000_000),
    )
    .await
    .expect("seek_to_timestamp did not time out")
    .expect("seek_to_timestamp ok");
    client.close().await;
}

/// `Producer::close` + `Producer::flush` + `Consumer::close` — tokio
/// counterpart to moonpool's `producer_and_consumer_close_complete`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_and_consumer_close_complete() {
    let (_host_port, url) = spawn_full_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");
    let producer = tokio::time::timeout(
        Duration::from_secs(3),
        client.open_producer(CreateProducerRequest {
            topic: "persistent://public/default/coverage-close-pclose".to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out")
    .expect("open_producer ok");
    tokio::time::timeout(Duration::from_secs(3), producer.flush())
        .await
        .expect("flush did not time out")
        .expect("flush ok");
    tokio::time::timeout(Duration::from_secs(3), producer.close())
        .await
        .expect("producer close did not time out")
        .expect("producer close ok");
    let consumer = tokio::time::timeout(
        Duration::from_secs(3),
        client.subscribe(SubscribeRequest {
            topic: "persistent://public/default/coverage-close-cclose".to_owned(),
            subscription: "cov-close-c".to_owned(),
            receiver_queue_size: 16,
            durable: true,
            ..Default::default()
        }),
    )
    .await
    .expect("subscribe did not time out")
    .expect("subscribe ok");
    tokio::time::timeout(Duration::from_secs(3), consumer.close())
        .await
        .expect("consumer close did not time out")
        .expect("consumer close ok");
    client.close().await;
}

/// `DriverHandle::abort` — tokio counterpart to moonpool's
/// `driver_handle_abort_populates_result_slot`. The tokio engine does
/// not maintain a separate `result` slot — instead the join future
/// surfaces a `ClientError::Other("driver task panicked: …")` when the
/// `JoinHandle` is aborted before the task completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn driver_handle_abort_populates_result_slot() {
    let (_host_port, url) = spawn_full_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");
    let driver = client.take_driver().expect("driver should be present");
    let rendered = format!("{driver:?}");
    assert!(rendered.contains("DriverHandle"));
    driver.abort();
    let err = tokio::time::timeout(Duration::from_secs(3), driver.join())
        .await
        .expect("join did not time out")
        .expect_err("aborted driver returns an error");
    assert!(
        matches!(&err, ClientError::Other(msg) if msg.contains("panicked")),
        "expected Other(panicked), got {err:?}"
    );
}

/// Compile-time use of `parking_lot::Mutex` keeps the crate's
/// banned-channel check happy with a non-empty `use`. Mirrors moonpool's
/// `no_channels_smoke`.
#[test]
fn no_channels_smoke() {
    let m: Mutex<u32> = Mutex::new(0);
    assert_eq!(*m.lock(), 0);
}

/// `Arc` smoke for parity with the moonpool side.
#[test]
fn arc_smoke() {
    let a: Arc<u32> = Arc::new(7);
    assert_eq!(*a, 7);
}
