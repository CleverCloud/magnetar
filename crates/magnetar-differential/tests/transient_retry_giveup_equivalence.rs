// SPDX-License-Identifier: Apache-2.0

//! Configured operation-retry give-up parity across the real Tokio and
//! Moonpool runtime clients (ADR-0024 layer d, issue #343).

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use magnetar_differential::{Event, EventStream, HANG_GUARD};
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, Frame, FrameError, OperationRetryConfig,
    SUPPORTED_PROTOCOL_VERSION, SubscribeRequest, decode_one, encode_command, pb,
};
use moonpool_core::TokioProviders;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const TOPIC: &str = "persistent://public/default/diff-operation-retry";

#[derive(Default)]
struct Attempts {
    producer_opens: AtomicU32,
    subscribes: AtomicU32,
}

fn retry_policy() -> OperationRetryConfig {
    OperationRetryConfig {
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        max_retries: Some(2),
    }
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-differential".to_owned(),
            protocol_version: Some(SUPPORTED_PROTOCOL_VERSION),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    encode_command(out, &cmd).expect("encode connected");
}

fn emit_lookup_response(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::LookupResponse as i32,
        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
            broker_service_url: None,
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
    encode_command(out, &cmd).expect("encode lookup response");
}

fn emit_transient_error(
    out: &mut BytesMut,
    request_id: u64,
    error: pb::ServerError,
    message: &str,
) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Error as i32,
        error: Some(pb::CommandError {
            request_id,
            error: error as i32,
            message: message.to_owned(),
        }),
        ..Default::default()
    };
    encode_command(out, &cmd).expect("encode transient error");
}

fn emit_pong(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Pong as i32,
        pong: Some(pb::CommandPong {}),
        ..Default::default()
    };
    encode_command(out, &cmd).expect("encode pong");
}

fn reply(frame: &Frame, out: &mut BytesMut, attempts: &Attempts) {
    match pb::base_command::Type::try_from(frame.command.r#type) {
        Ok(pb::base_command::Type::Connect) => emit_connected(out),
        Ok(pb::base_command::Type::Lookup) => {
            if let Some(lookup) = &frame.command.lookup_topic {
                emit_lookup_response(out, lookup.request_id);
            }
        }
        Ok(pb::base_command::Type::Producer) => {
            if let Some(producer) = &frame.command.producer {
                attempts.producer_opens.fetch_add(1, Ordering::SeqCst);
                emit_transient_error(
                    out,
                    producer.request_id,
                    pb::ServerError::ProducerBusy,
                    "producer owner is moving",
                );
            }
        }
        Ok(pb::base_command::Type::Subscribe) => {
            if let Some(subscribe) = &frame.command.subscribe {
                attempts.subscribes.fetch_add(1, Ordering::SeqCst);
                emit_transient_error(
                    out,
                    subscribe.request_id,
                    pb::ServerError::ConsumerBusy,
                    "consumer owner is moving",
                );
            }
        }
        Ok(pb::base_command::Type::Ping) => emit_pong(out),
        _ => {}
    }
}

async fn serve(mut stream: TcpStream, attempts: Arc<Attempts>) {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(frame) => frame,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return,
            };
            let consumed = before.saturating_sub(framed.len());
            let _ = read_buf.split_to(consumed);
            let mut out = BytesMut::new();
            reply(&frame, &mut out, &attempts);
            if !out.is_empty() && stream.write_all(&out).await.is_err() {
                return;
            }
        }
        match stream.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

async fn spawn_broker() -> (
    std::net::SocketAddr,
    Arc<Attempts>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("broker address");
    let attempts = Arc::new(Attempts::default());
    let task_attempts = attempts.clone();
    let task = tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        serve(stream, task_attempts).await;
    });
    (addr, attempts, task)
}

fn tokio_error_kind(err: &magnetar_runtime_tokio::ClientError) -> String {
    match err {
        magnetar_runtime_tokio::ClientError::Broker { code, .. } => format!("broker:{code}"),
        magnetar_runtime_tokio::ClientError::Timeout(_) => "timeout".to_owned(),
        _ => "other".to_owned(),
    }
}

fn moonpool_error_kind(err: &magnetar_runtime_moonpool::ClientError) -> String {
    match err {
        magnetar_runtime_moonpool::ClientError::Broker { code, .. } => {
            format!("broker:{code}")
        }
        magnetar_runtime_moonpool::ClientError::Other(message)
            if message.contains("exceeded operation_timeout") =>
        {
            "timeout".to_owned()
        }
        _ => "other".to_owned(),
    }
}

async fn run_tokio() -> (EventStream, u32, u32) {
    let (addr, attempts, broker) = spawn_broker().await;
    let client = magnetar_runtime_tokio::Client::connect(
        &format!("pulsar://{addr}"),
        ConnectionConfig {
            operation_timeout: Duration::from_secs(1),
            ..ConnectionConfig::default()
        },
    )
    .await
    .expect("tokio connect")
    .with_operation_retry(retry_policy());

    let producer_err = client
        .open_producer(CreateProducerRequest {
            topic: TOPIC.to_owned(),
            ..Default::default()
        })
        .await
        .expect_err("producer-open must exhaust its configured retry count");
    let subscribe_err = client
        .subscribe(SubscribeRequest {
            topic: TOPIC.to_owned(),
            subscription: "diff-operation-retry".to_owned(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            ..Default::default()
        })
        .await
        .expect_err("subscribe must exhaust its configured retry count");

    let mut stream = EventStream::empty();
    stream.push(Event::SendError {
        kind: tokio_error_kind(&producer_err),
    });
    stream.push(Event::AckError {
        kind: tokio_error_kind(&subscribe_err),
    });
    client.close().await;
    broker.abort();
    (
        stream,
        attempts.producer_opens.load(Ordering::SeqCst),
        attempts.subscribes.load(Ordering::SeqCst),
    )
}

async fn run_moonpool() -> (EventStream, u32, u32) {
    let (addr, attempts, broker) = spawn_broker().await;
    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let client = magnetar_runtime_moonpool::Client::connect_plain(
        &engine,
        &addr.to_string(),
        ConnectionConfig {
            operation_timeout: Duration::from_secs(1),
            ..ConnectionConfig::default()
        },
    )
    .await
    .expect("moonpool connect")
    .with_operation_retry(retry_policy());

    let producer_err = client
        .open_producer(CreateProducerRequest {
            topic: TOPIC.to_owned(),
            ..Default::default()
        })
        .await
        .expect_err("producer-open must exhaust its configured retry count");
    let subscribe_err = client
        .subscribe(SubscribeRequest {
            topic: TOPIC.to_owned(),
            subscription: "diff-operation-retry".to_owned(),
            sub_type: pb::command_subscribe::SubType::Exclusive,
            ..Default::default()
        })
        .await
        .expect_err("subscribe must exhaust its configured retry count");

    let mut stream = EventStream::empty();
    stream.push(Event::SendError {
        kind: moonpool_error_kind(&producer_err),
    });
    stream.push(Event::AckError {
        kind: moonpool_error_kind(&subscribe_err),
    });
    client.close().await;
    broker.abort();
    (
        stream,
        attempts.producer_opens.load(Ordering::SeqCst),
        attempts.subscribes.load(Ordering::SeqCst),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_retry_giveup_is_equivalent_across_real_engines() {
    let tokio = tokio::time::timeout(HANG_GUARD, run_tokio())
        .await
        .expect("tokio retry leg must not hang");
    let moonpool = tokio::time::timeout(HANG_GUARD, run_moonpool())
        .await
        .expect("moonpool retry leg must not hang");

    assert_eq!(tokio.0, moonpool.0, "user-visible error streams diverged");
    assert_eq!(tokio.1, 3, "tokio producer-open wire count");
    assert_eq!(tokio.2, 3, "tokio subscribe wire count");
    assert_eq!(moonpool.1, 3, "moonpool producer-open wire count");
    assert_eq!(moonpool.2, 3, "moonpool subscribe wire count");
    assert_eq!(
        tokio.0.events,
        vec![
            Event::SendError {
                kind: format!("broker:{}", pb::ServerError::ProducerBusy as i32),
            },
            Event::AckError {
                kind: format!("broker:{}", pb::ServerError::ConsumerBusy as i32),
            },
        ]
    );
}
