// SPDX-License-Identifier: Apache-2.0

//! Lookup-error propagation — tokio engine, real loopback.
//!
//! Mirror of `magnetar-runtime-moonpool/tests/lookup_error_propagation.rs`
//! (deterministic simulation). Maintains the tokio ↔ moonpool 1:1 test count
//! required by ADR-0024 (`check-runtime-test-parity`): five `#[tokio::test]`
//! functions here mirror the moonpool file's five `#[test]` functions.
//!
//! ## Coverage gap this pins
//!
//! The existing `lookup_redirect_chain.rs` pair covers a redirect chain that
//! *settles* and the redirect-cap diagnostic. What was *not* covered is the
//! two ways a `CommandLookupTopic` round-trip terminates in a **bounded
//! `ClientError::Broker`** rather than a hang:
//!
//! 1. **Broker-originated `Failed`** — the broker answers the LOOKUP with `LookupType::Failed`
//!    carrying an explicit `ServerError` code + message. `lookup_topic` (driven here through the
//!    public `open_producer` surface, since tokio's `lookup_topic` is private) must surface
//!    [`ClientError::Broker`] with the broker's verbatim code + message — not park the
//!    producer-open future forever.
//! 2. **Unbounded redirect loop** — the broker answers *every* LOOKUP with `LookupType::Redirect`
//!    advertising its own address. The engine's redirect-dial loop re-issues on the bootstrap
//!    (bootstrap-equality reuse) up to [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`] hops, then
//!    surfaces a bounded `ClientError::Broker` carrying the "redirect cap exceeded" diagnostic.
//!
//! The termination proof is that `open_producer` *resolves* under the
//! per-call `tokio::time::timeout`: a regression that dropped the `Failed`
//! translation or the redirect cap would leave the future parked and the
//! timeout would trip the `expect`.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, FrameError, OperationRetryConfig, decode_one,
    encode_command, pb,
};
use magnetar_runtime_tokio::{Client, ClientError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
mod common;
use common::HANG_GUARD;

/// Topic the producer targets. The broker answers by frame kind, not by
/// topic; a realistic name keeps logs readable.
const TOPIC: &str = "persistent://public/default/lookup-error-propagation";

/// Broker-side `ServerError` code echoed on the `Failed` lookup response.
/// `TopicNotFound` is the canonical "this lookup cannot resolve" answer.
const FAILED_CODE: i32 = pb::ServerError::TopicNotFound as i32;

/// Broker-side message echoed on the `Failed` lookup response — must
/// round-trip verbatim into the engine-surfaced `ClientError::Broker`.
const FAILED_MESSAGE: &str = "topic does not exist";

/// How the broker should answer `CommandLookupTopic` frames.
#[derive(Clone)]
enum LookupBehavior {
    /// Answer the LOOKUP with `LookupType::Failed { error, message }`.
    Failed,
    /// Reject the first LOOKUP with `ServiceNotReady`, then resolve it and
    /// acknowledge the producer open. The counter records wire attempts.
    RetryableThenConnect { attempts: Arc<AtomicUsize> },
    /// Reject the first partition-metadata request with `ServiceNotReady`,
    /// then return a partition count. The counter records wire attempts.
    MetadataRetryableThenSuccess { attempts: Arc<AtomicUsize> },
    /// Reject every lookup with `ServiceNotReady`.
    AlwaysRetryableLookup { attempts: Arc<AtomicUsize> },
    /// Answer *every* LOOKUP with `LookupType::Redirect` advertising the
    /// carried URL (the broker's own address — so the engine's dial loop
    /// re-issues on the bootstrap via bootstrap-equality and the redirect cap
    /// trips after `MAX_LOOKUP_REDIRECTS` hops rather than looping forever).
    AlwaysRedirect { redirect_url: String },
}

/// Spawn a loopback broker that completes the handshake and answers LOOKUPs
/// per `behavior`. Returns the dialable `pulsar://` URL. The accept loop and
/// each session run on detached tasks so the broker keeps servicing the
/// client until the test drops the connection.
///
/// For [`LookupBehavior::AlwaysRedirect`] the broker advertises its OWN URL as
/// the redirect target (the `redirect_url` field is filled in here once the
/// bound address is known).
async fn spawn_lookup_broker(mut behavior: LookupBehavior) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let url = format!("pulsar://{addr}");
    if let LookupBehavior::AlwaysRedirect { redirect_url } = &mut behavior {
        redirect_url.clone_from(&url);
    }
    tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            let behavior = behavior.clone();
            tokio::spawn(async move {
                let _ = handle_session(stream, behavior).await;
            });
        }
    });
    url
}

/// Per-session script: complete the handshake, then answer LOOKUPs per
/// `behavior`. Service `PING` → `PONG` so the connection stays live.
async fn handle_session(mut stream: TcpStream, behavior: LookupBehavior) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
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
            handle_frame(&frame, &mut out_buf, &behavior);
        }

        if !out_buf.is_empty() {
            stream.write_all(&out_buf).await?;
            stream.flush().await?;
            out_buf.clear();
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(e) => return Err(e),
        }
    }
}

fn handle_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut, behavior: &LookupBehavior) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => emit_connected(out),
        pb::base_command::Type::Ping => emit_pong(out),
        pb::base_command::Type::Lookup => {
            if let Some(lookup) = &frame.command.lookup_topic {
                let cmd = pb::BaseCommand {
                    r#type: pb::base_command::Type::LookupResponse as i32,
                    lookup_topic_response: Some(lookup_response(lookup.request_id, behavior)),
                    ..Default::default()
                };
                let _ = encode_command(out, &cmd);
            }
        }
        pb::base_command::Type::PartitionedMetadata => {
            emit_partitioned_metadata(frame, out, behavior);
        }
        pb::base_command::Type::Producer => emit_producer_success(frame, out),
        _ => {}
    }
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test-broker".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_pong(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Pong as i32,
        pong: Some(pb::CommandPong {}),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn lookup_response(request_id: u64, behavior: &LookupBehavior) -> pb::CommandLookupTopicResponse {
    let connect = || pb::CommandLookupTopicResponse {
        broker_service_url: None,
        broker_service_url_tls: None,
        response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
        request_id,
        authoritative: Some(true),
        error: None,
        message: None,
        proxy_through_service_url: Some(false),
    };
    match behavior {
        LookupBehavior::Failed => pb::CommandLookupTopicResponse {
            response: Some(pb::command_lookup_topic_response::LookupType::Failed as i32),
            error: Some(FAILED_CODE),
            message: Some(FAILED_MESSAGE.to_owned()),
            ..connect()
        },
        LookupBehavior::RetryableThenConnect { attempts } => {
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                pb::CommandLookupTopicResponse {
                    response: Some(pb::command_lookup_topic_response::LookupType::Failed as i32),
                    error: Some(pb::ServerError::ServiceNotReady as i32),
                    message: Some("bundle is loading".to_owned()),
                    ..connect()
                }
            } else {
                connect()
            }
        }
        LookupBehavior::MetadataRetryableThenSuccess { .. } => connect(),
        LookupBehavior::AlwaysRetryableLookup { attempts } => {
            attempts.fetch_add(1, Ordering::SeqCst);
            pb::CommandLookupTopicResponse {
                response: Some(pb::command_lookup_topic_response::LookupType::Failed as i32),
                error: Some(pb::ServerError::ServiceNotReady as i32),
                message: Some("bundle is still loading".to_owned()),
                ..connect()
            }
        }
        LookupBehavior::AlwaysRedirect { redirect_url } => pb::CommandLookupTopicResponse {
            broker_service_url: Some(redirect_url.clone()),
            response: Some(pb::command_lookup_topic_response::LookupType::Redirect as i32),
            ..connect()
        },
    }
}

fn emit_partitioned_metadata(
    frame: &magnetar_proto::Frame,
    out: &mut BytesMut,
    behavior: &LookupBehavior,
) {
    let Some(metadata) = &frame.command.partition_metadata else {
        return;
    };
    let (partitions, response, error, message) = match behavior {
        LookupBehavior::MetadataRetryableThenSuccess { attempts }
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 =>
        {
            (
                0,
                pb::command_partitioned_topic_metadata_response::LookupType::Failed as i32,
                Some(pb::ServerError::ServiceNotReady as i32),
                Some("metadata store is loading".to_owned()),
            )
        }
        LookupBehavior::MetadataRetryableThenSuccess { .. } => (
            3,
            pb::command_partitioned_topic_metadata_response::LookupType::Success as i32,
            None,
            None,
        ),
        _ => (
            0,
            pb::command_partitioned_topic_metadata_response::LookupType::Success as i32,
            None,
            None,
        ),
    };
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::PartitionedMetadataResponse as i32,
        partition_metadata_response: Some(pb::CommandPartitionedTopicMetadataResponse {
            partitions: Some(partitions),
            request_id: metadata.request_id,
            response: Some(response),
            error,
            message,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_producer_success(frame: &magnetar_proto::Frame, out: &mut BytesMut) {
    let Some(producer) = &frame.command.producer else {
        return;
    };
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id: producer.request_id,
            producer_name: "lookup-retry-test".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// An already-ready deadline wins before the first wire command is enqueued.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_expired_deadline_does_not_enqueue_lookup() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let url = spawn_lookup_broker(LookupBehavior::AlwaysRetryableLookup {
        attempts: attempts.clone(),
    })
    .await;
    let client = Client::connect(&url, ConnectionConfig::default())
        .await
        .expect("connect ok");
    let mut deadline = Box::pin(async {});
    let mut last_broker_error = None;

    let err = client
        .open_producer_with_operation_deadline(
            CreateProducerRequest {
                topic: TOPIC.to_owned(),
                ..Default::default()
            },
            None,
            deadline.as_mut(),
            &mut last_broker_error,
        )
        .await
        .expect_err("an already-expired deadline must fail before enqueue");
    assert!(matches!(err, ClientError::Timeout(_)), "got {err:?}");
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        0,
        "deadline preflight must prevent the first lookup command"
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

/// The total operation deadline wins while the retry is sleeping: no second
/// lookup reaches the wire, and the last broker error is preserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_lookup_deadline_during_backoff_returns_last_error_without_reissue() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let url = spawn_lookup_broker(LookupBehavior::AlwaysRetryableLookup {
        attempts: attempts.clone(),
    })
    .await;
    // The invariant is an ordering — the operation deadline expires while the
    // retry is still sleeping — not a pair of durations. Encoding it as 5 ms
    // against a 50 ms backoff *also* required the first lookup's real TCP
    // round-trip to land inside 5 ms, which the invariant never claimed and
    // which is false on a loaded machine: the deadline then fires before the
    // broker's `ServiceNotReady` arrives, and the assertion below sees a
    // timeout error instead of the preserved broker error. Observed twice
    // under a full `--all-features` run, green 3/3 in isolation both times.
    //
    // The constants below keep the same ordering with the margin on the
    // robust side: round-trip (~1 ms on loopback) << deadline << backoff.
    // The moonpool mirror deliberately keeps 5 ms / 50 ms — its clock is
    // virtual, so the ordering there is exact and no round-trip competes
    // with it.
    let config = ConnectionConfig {
        operation_timeout: Duration::from_millis(500),
        ..ConnectionConfig::default()
    };
    let client = tokio::time::timeout(HANG_GUARD, Client::connect(&url, config))
        .await
        .expect("connect did not time out")
        .expect("connect ok")
        .with_operation_retry(OperationRetryConfig {
            initial_backoff: Duration::from_secs(30),
            max_backoff: Duration::from_secs(30),
            max_retries: Some(1),
        });

    let err = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: TOPIC.to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("operation deadline did not resolve open_producer")
    .expect_err("an always-retryable lookup must not open a producer");

    assert!(
        matches!(
            err,
            ClientError::Broker { code, ref message }
                if code == pb::ServerError::ServiceNotReady as i32
                    && message == "bundle is still loading"
        ),
        "deadline must surface the last broker error, got {err:?}"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "deadline expiry during backoff must prevent the configured reissue"
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

/// A retryable partition-metadata rejection is re-issued and the eventual
/// broker count is returned to the caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_partition_metadata_service_not_ready_retries_then_succeeds() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let url = spawn_lookup_broker(LookupBehavior::MetadataRetryableThenSuccess {
        attempts: attempts.clone(),
    })
    .await;
    let config = ConnectionConfig::default();
    let client = tokio::time::timeout(HANG_GUARD, Client::connect(&url, config))
        .await
        .expect("connect did not time out")
        .expect("connect ok")
        .with_operation_retry(OperationRetryConfig {
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            max_retries: Some(1),
        });

    let partitions = tokio::time::timeout(HANG_GUARD, client.partitioned_topic_metadata(TOPIC))
        .await
        .expect("partition metadata did not time out")
        .expect("retryable metadata failure must be re-issued");
    assert_eq!(partitions, 3);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

/// A retryable lookup rejection is re-issued under the configured operation
/// policy, and the eventual success continues into producer attachment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_lookup_service_not_ready_retries_then_opens_producer() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let url = spawn_lookup_broker(LookupBehavior::RetryableThenConnect {
        attempts: attempts.clone(),
    })
    .await;
    let config = ConnectionConfig::default();

    let client = tokio::time::timeout(HANG_GUARD, Client::connect(&url, config))
        .await
        .expect("connect did not time out")
        .expect("connect ok")
        .with_operation_retry(OperationRetryConfig {
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            max_retries: Some(1),
        });

    let producer = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: TOPIC.to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out")
    .expect("retryable lookup failure must be re-issued");

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "one initial lookup plus one configured retry must reach the wire"
    );

    drop(producer);
    if let Some(d) = client.take_driver() {
        d.abort();
    }
}

/// A broker-originated `LookupType::Failed` response must surface as a
/// bounded [`ClientError::Broker`] carrying the broker's `ServerError` code
/// AND verbatim message — `open_producer` resolves with an error instead of
/// parking forever waiting for a `Connect`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_lookup_failed_response_surfaces_bounded_broker_error() {
    let url = spawn_lookup_broker(LookupBehavior::Failed).await;

    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let err = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: TOPIC.to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out — the lookup must surface a bounded error")
    .expect_err("open_producer must fail when the LOOKUP answers Failed");

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    match err {
        ClientError::Broker { code, message } => {
            assert_eq!(
                code, FAILED_CODE,
                "ClientError::Broker must carry the broker ServerError code",
            );
            assert_eq!(
                message, FAILED_MESSAGE,
                "ClientError::Broker must carry the verbatim broker message",
            );
        }
        other => {
            panic!("lookup Failed must surface as a bounded ClientError::Broker, got {other:?}")
        }
    }
}

/// A broker that answers *every* LOOKUP with `Redirect` (to its own address)
/// must NOT hang `open_producer`. The engine's redirect-dial loop re-issues on
/// the bootstrap (bootstrap-equality reuse) up to
/// [`magnetar_proto::lookup::MAX_LOOKUP_REDIRECTS`] hops and then surfaces a
/// bounded [`ClientError::Broker`] carrying the "redirect cap exceeded"
/// diagnostic — the redirect-loop `DoS` is bounded end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_lookup_redirect_loop_surfaces_bounded_cap_error() {
    // `redirect_url` is filled with the broker's own URL in `spawn_lookup_broker`.
    let url = spawn_lookup_broker(LookupBehavior::AlwaysRedirect {
        redirect_url: String::new(),
    })
    .await;

    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    let err = tokio::time::timeout(
        HANG_GUARD,
        client.open_producer(CreateProducerRequest {
            topic: TOPIC.to_owned(),
            ..Default::default()
        }),
    )
    .await
    .expect("open_producer did not time out — the redirect cap must bound the lookup")
    .expect_err("open_producer must fail when the redirect chain never resolves");

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    let msg = format!("{err}");
    assert!(
        msg.contains("redirect cap exceeded"),
        "expected the redirect-cap diagnostic to be surfaced to the user, got: {msg}",
    );
}
