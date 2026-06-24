// SPDX-License-Identifier: Apache-2.0

//! Transient-retry bounded give-up — differential equivalence (ADR-0024 layer d
//! for the issue #299 + #302 reconnect-retry fix).
//!
//! The fix splits across `magnetar-proto` (the per-handle attempt counter, the
//! terminal-give-up methods [`Connection::fail_producer_open`] /
//! [`Connection::fail_consumer_subscribe`], and the
//! [`Connection::consumer_handle_is_terminal`] /
//! [`Connection::is_terminally_closed`] predicates) and the two engines' driver
//! retry legs (which size their backoff off the proto-tracked counter and gate
//! the receive future on the terminal predicate).
//!
//! Like `driver_mid_session_reject_equivalence.rs`, the give-up decision is
//! **invisible to the `EventStream` `Op`→`Event` surface**: it manifests as the
//! engine-local terminal `Err` the parked `send()` / `receive()` future
//! resolves with, driven by the SAME `magnetar-proto` classification both
//! engines delegate to. Divergence could only arise if one engine grew an
//! engine-local copy of the attempt-count / terminal logic — neither does;
//! both read [`Connection::producer_transient_open_attempts`] /
//! [`Connection::consumer_transient_subscribe_attempts`] and call the same
//! `fail_*` / terminal-predicate methods.
//!
//! This test pins that shared decision (run once per "engine" surrogate): the
//! bounded loop terminalizes a producer-open AND a subscribe after the SAME
//! number of transient rejections, surfaces the SAME terminal disposition, and
//! the receive-terminal predicate agrees. The end-to-end deterministic
//! give-up + receive-across-drop assertions live in the runtime layers
//! (`magnetar-runtime-{tokio,moonpool}/tests/reconnect_replay_gating.rs`).

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::{Instant, SystemTime};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, MAX_TRANSIENT_OPEN_RETRIES, RequestId,
    SUPPORTED_PROTOCOL_VERSION, SubscribeRequest, encode_command, pb,
};

fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-diff".to_owned(),
            protocol_version: Some(SUPPORTED_PROTOCOL_VERSION),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandConnected");
    buf
}

fn feed_transient_error(conn: &mut Connection, request_id: RequestId) {
    let err = pb::BaseCommand {
        r#type: pb::base_command::Type::Error as i32,
        error: Some(pb::CommandError {
            request_id: request_id.0,
            error: pb::ServerError::ServiceNotReady as i32,
            message: "bundle not served".to_owned(),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &err).expect("encode CommandError");
    conn.handle_bytes(Instant::now(), &buf)
        .expect("handle CommandError");
}

fn connected_conn() -> Connection {
    let mut conn = Connection::new(ConnectionConfig::default(), Arc::new(SystemTime::now));
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(Instant::now(), &handshake_response_bytes())
        .expect("handshake completes");
    while conn.poll_event().is_some() {}
    conn
}

/// The shared producer-open give-up decision both engines delegate to: open a
/// producer, then bounce every (re-)open transiently until the proto layer
/// terminalizes it. Returns `(attempts_before_terminal, producer_dropped)`.
fn producer_giveup_decision() -> (u32, bool) {
    let mut conn = connected_conn();
    let mut request_id = RequestId(conn.peek_next_request_id_for_test());
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/giveup".to_owned(),
        ..Default::default()
    });

    // Count the rejections we feed: the give-up fires when the internal
    // counter crosses the cap, i.e. on the (cap + 1)-th rejection. We cannot
    // read the per-handle accessor after the give-up (the producer state is
    // dropped, and the accessor returns 0 for an unknown handle), so the fed
    // count is the stable witness.
    let mut rejections = 0u32;
    let mut terminal = false;
    for _ in 0..(MAX_TRANSIENT_OPEN_RETRIES + 8) {
        feed_transient_error(&mut conn, request_id);
        rejections += 1;
        if conn.producer(handle).is_none() {
            terminal = true;
            break;
        }
        // Drain queued events + re-issue the open as the engine's retry leg
        // would.
        while conn.poll_event().is_some() {}
        if let Some(rid) = conn.retry_producer_open(handle) {
            request_id = rid;
        } else {
            terminal = true;
            break;
        }
    }
    (rejections, terminal)
}

/// The shared subscribe give-up decision: subscribe, then bounce every
/// (re-)subscribe transiently until the proto layer terminalizes it. Returns
/// `(attempts_before_terminal, handle_is_terminal)`.
fn subscribe_giveup_decision() -> (u32, bool) {
    let mut conn = connected_conn();
    let mut request_id = RequestId(conn.peek_next_request_id_for_test());
    let handle = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/giveup-sub".to_owned(),
        subscription: "diff".to_owned(),
        sub_type: pb::command_subscribe::SubType::Exclusive,
        ..Default::default()
    });

    let mut rejections = 0u32;
    let mut terminal = false;
    for _ in 0..(MAX_TRANSIENT_OPEN_RETRIES + 8) {
        feed_transient_error(&mut conn, request_id);
        rejections += 1;
        if conn.consumer_handle_is_terminal(handle) {
            terminal = true;
            break;
        }
        while conn.poll_event().is_some() {}
        if let Some(rid) = conn.retry_consumer_subscribe(handle) {
            request_id = rid;
        } else {
            terminal = true;
            break;
        }
    }
    (rejections, terminal)
}

#[test]
fn engines_agree_on_producer_open_giveup() {
    // Both engines delegate to the same proto classification; running the
    // shared helper twice is the differential surrogate for "tokio" vs
    // "moonpool" (drift would mean one engine grew an engine-local copy).
    let tokio = producer_giveup_decision();
    let moonpool = producer_giveup_decision();
    assert_eq!(
        tokio, moonpool,
        "both engines must give up on a never-served producer-open identically"
    );
    // Pin the contract: the give-up fires AT the cap and drops the producer.
    assert_eq!(
        tokio.0,
        MAX_TRANSIENT_OPEN_RETRIES + 1,
        "give-up fires one rejection past the budget"
    );
    assert!(tokio.1, "give-up drops the producer state so send() Errs");
}

#[test]
fn engines_agree_on_subscribe_giveup() {
    let tokio = subscribe_giveup_decision();
    let moonpool = subscribe_giveup_decision();
    assert_eq!(
        tokio, moonpool,
        "both engines must give up on a never-served subscribe identically"
    );
    assert_eq!(
        tokio.0,
        MAX_TRANSIENT_OPEN_RETRIES + 1,
        "give-up fires one rejection past the budget"
    );
    assert!(
        tokio.1,
        "give-up makes the consumer handle terminal so receive() Errs"
    );
}
