// SPDX-License-Identifier: Apache-2.0

//! Layer (d) of the ADR-0024 obligation for issue #364's production
//! hardening — `magnetar_runtime_moonpool::client::proxy_broker_authority`
//! / `direct_broker_authority` changed from `fn(&str) -> String` to
//! `fn(&str) -> Result<String, ClientError>`, rejecting a corrupted /
//! unrecognised URL scheme (e.g. a single-bit corruption of `pulsar`,
//! `"ptlsar://..."`) instead of silently truncating it into a nonsense
//! authority.
//!
//! **Honest scope note: this test is NOT the fix's regression proof.** It
//! asserts a proto-level invariant that is unaffected by, and true both
//! before AND after, the client-layer fix — reverting the `client.rs`
//! change would NOT turn this test red, because it never calls
//! `proxy_broker_authority` / `direct_broker_authority` at all; it only
//! drives `magnetar_proto::Connection` directly. The actual red/green
//! regression proof for this fix lives in
//! `crates/magnetar-runtime-moonpool/tests/proxy_multi_conn.rs`'s
//! `open_producer_through_proxy_rejects_corrupted_broker_scheme` (verified
//! red pre-fix, green post-fix — see the commit message). What THIS test
//! proves is narrower but still load-bearing for ADR-0024 layer (d): that
//! both engines' proto layers decode the identical corrupted wire bytes
//! identically, so whatever each engine's `Client` layer subsequently does
//! with that value is reacting to the same decoded input, not a
//! decode-level divergence between engines.
//!
//! **What this test does and, deliberately, does NOT assert.** Per the
//! `lookup_direct_multi_broker_equivalence.rs` precedent this file mirrors:
//! a full client-level cross-engine assertion would require standing up a
//! broker/proxy pair for each engine, so the load-bearing equivalence is at
//! the **proto** layer — both engines' [`magnetar_proto::Connection`] must
//! decode the SAME corrupted `CommandLookupTopicResponse` bytes to the SAME
//! `OpOutcome::LookupResponse` / `broker_service_url: Some("ptlsar://...")`
//! shape (the raw bytes on the wire are unaffected by this fix; only the
//! CLIENT-layer post-processing of that value changed, and only on
//! moonpool). This test does NOT assert that both engines' `Client`
//! layers behave the same way past that point — they don't, on purpose:
//! moonpool's `proxy_broker_authority` now rejects the corrupted scheme
//! explicitly, while the tokio engine's `preferred_broker_url` still
//! forwards the raw string unchanged (relying on the downstream Pulsar
//! Proxy's `validateBrokerTarget()` to reject it — see
//! `crates/magnetar-runtime-moonpool/src/client.rs`'s
//! `proxy_broker_authority` doc comment for the full split verdict). Neither
//! hardening is cross-engine parity restoration: tokio's `parse_direct_broker_url`
//! (the DIRECT-path sibling `direct_broker_authority` was compared against)
//! does not cleanly reject the equivalent corrupted-scheme input either — it
//! has its own distinct latent bug, silently mis-deriving a garbage host
//! with the WRONG default port (see `docs/follow-ups.md` §6, filed rather
//! than fixed here since `magnetar-runtime-tokio` is out of scope for this
//! changeset). Both moonpool hardenings go beyond what tokio currently does.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{Connection, ConnectionConfig, LookupOutcome, OpOutcome, encode_command, pb};

/// A single-bit corruption of the `pulsar` scheme word, matching the
/// shape moonpool-sim's bit-flip chaos actually produced for issue #364.
const CORRUPTED_SCHEME_BROKER_URL: &str = "ptlsar://broker-sim.proxy.internal:6650";

#[derive(Debug, PartialEq, Eq, Clone)]
struct LookupSnapshot {
    /// `broker_service_url` from the response, verbatim — both engines must
    /// decode the identical corrupted string at the proto layer; only the
    /// CLIENT layer's post-processing of it diverges (moonpool only).
    broker_service_url: Option<String>,
    proxy_through_service_url: bool,
}

fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-diff-corrupted-scheme".to_owned(),
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

fn proxy_lookup_response_bytes(request_id: u64, broker_url: &str) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::LookupResponse as i32,
        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
            broker_service_url: Some(broker_url.to_owned()),
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
            request_id,
            authoritative: Some(true),
            error: None,
            message: None,
            proxy_through_service_url: Some(true),
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

/// Drive an engine's [`Connection`] through a handshake + a single
/// PROXY-routed LOOKUP round-trip carrying [`CORRUPTED_SCHEME_BROKER_URL`].
/// Returns the [`LookupSnapshot`] the proto layer surfaced — this is the
/// value BOTH engines' `Client` layers receive before any
/// engine-specific post-processing (the point where moonpool's and
/// tokio's handling diverges).
fn drive_proxy_lookup_with_corrupted_scheme<F>(make_shared: F) -> LookupSnapshot
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

    let request_id = {
        let mut conn = shared.lock();
        conn.lookup("persistent://public/default/diff-corrupted-scheme", false)
    };
    {
        let mut conn = shared.lock();
        let _ = conn.poll_transmit();
        conn.handle_bytes(
            start,
            &proxy_lookup_response_bytes(request_id.0, CORRUPTED_SCHEME_BROKER_URL),
        )
        .expect("lookup response");
    }

    let mut conn = shared.lock();
    while conn.poll_event().is_some() {}
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
        other => panic!("expected LookupResponse -> Connect, got {other:?}"),
    }
}

/// Both engines must agree on the raw `broker_service_url` the proto layer
/// surfaces even when it carries a corrupted scheme — the wire bytes are
/// identical on both engines and unaffected by moonpool's client-layer
/// hardening. This is the proto-level invariant load-bearing for issue
/// #364: whatever each engine's `Client` layer subsequently does with this
/// value (moonpool: reject; tokio: forward raw), they must be reacting to
/// the SAME decoded input, not a decode-level divergence.
#[test]
fn tokio_and_moonpool_decode_the_same_corrupted_scheme_lookup_response() {
    let tokio_snap = drive_proxy_lookup_with_corrupted_scheme(|cfg| {
        Arc::new(TokioShared(magnetar_runtime_tokio::ConnectionShared::new(
            cfg,
        )))
    });
    let moonpool_snap = drive_proxy_lookup_with_corrupted_scheme(|cfg| {
        Arc::new(MoonpoolShared(
            magnetar_runtime_moonpool::ConnectionShared::new(cfg),
        ))
    });

    assert_eq!(
        tokio_snap, moonpool_snap,
        "tokio and moonpool engines decoded the corrupted-scheme lookup response \
         differently:\ntokio    = {tokio_snap:?}\nmoonpool = {moonpool_snap:?}",
    );
    assert_eq!(
        tokio_snap.broker_service_url.as_deref(),
        Some(CORRUPTED_SCHEME_BROKER_URL),
        "the corrupted scheme must be surfaced verbatim by the proto layer on both engines — \
         truncation only ever happened downstream, in moonpool's client-layer \
         proxy_broker_authority, which this differential test does not exercise (that's the \
         private-fn unit tests' job; see client.rs)"
    );
    assert!(
        tokio_snap.proxy_through_service_url,
        "PROXY routing must decode identically on both engines regardless of scheme corruption"
    );
}
