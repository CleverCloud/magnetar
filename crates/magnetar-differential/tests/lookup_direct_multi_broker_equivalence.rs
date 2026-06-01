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
//! Adding a full client-level cross-engine assertion would require
//! standing up a pair of brokers for each engine; the proto-level
//! invariant — both engines see the same wire decision — is the
//! load-bearing equivalence. The engine-level routing decision (DIRECT
//! pool entry + bootstrap-equality fast path) is covered by the per-engine
//! integration tests
//! (`crates/magnetar-runtime-{tokio,moonpool}/tests/lookup_direct_multi_broker.rs`).

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{Connection, ConnectionConfig, LookupOutcome, OpOutcome, encode_command, pb};

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
