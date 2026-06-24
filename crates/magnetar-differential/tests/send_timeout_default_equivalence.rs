// SPDX-License-Identifier: Apache-2.0

//! Default `send_timeout` firing — differential equivalence
//! (ADR-0024 layer d for ADR-0072).
//!
//! The Java-parity default (`CreateProducerRequest::default().send_timeout ==
//! Some(30s)`) and the timeout sweep that enforces it both live entirely in
//! `magnetar-proto` (`Connection::poll_timeout` surfaces the deadline,
//! `Connection::handle_timeout` resolves the in-flight send with a
//! `code=-1, "send timeout"` `OpOutcome::SendError`). Both runtime engines feed
//! their driver loop's timeout tick through the **same**
//! `Connection::handle_timeout` against an INJECTED clock (ADR-0011, verified
//! end-to-end on each engine's `virtual_clock_driver_loop.rs`), so the firing
//! decision is engine-agnostic.
//!
//! Like `keepalive_watchdog_equivalence.rs`, this decision is **invisible to
//! the `Trace`/`EventStream` surface** — the `Op` model has no send-timeout
//! knob and the runners do not advance a virtual clock past 30s — so the
//! differential surrogate is the **shared `magnetar-proto` decision run twice**
//! (once per "engine"). Divergence could only arise if an engine grew an
//! engine-local send-timeout path, which neither does. This pins both the
//! default VALUE and the exact `SendError` outcome the runtime surfaces.
//!
//! The end-to-end deterministic firing assertions live in the runtime layers
//! (`magnetar-runtime-{tokio,moonpool}/tests/virtual_clock_driver_loop.rs`).

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, CreateProducerRequest, OpOutcome, PendingOpKey,
    SUPPORTED_PROTOCOL_VERSION, encode_command, pb,
};

/// A `CommandConnected` frame — drives a fresh handshaking connection to the
/// `Connected` state.
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

/// A `CommandProducerSuccess` frame for `request_id` — opens the
/// producer-not-ready drain gate so subsequent sends reach the wire.
fn producer_success_bytes(request_id: u64) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: "diff-send-timeout".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode ProducerSuccess");
    buf
}

/// Stable projection of the send's resolved [`OpOutcome`] for the equivalence
/// compare. [`OpOutcome`] itself is not `PartialEq` (it carries non-comparable
/// payloads on other variants), so we collapse the timeout outcome to its
/// observable `(sequence_id, code, message)` triple — the exact fields the
/// runtime maps onto the user-facing `send()` error.
#[derive(Debug, PartialEq, Eq)]
enum SendResolution {
    Timeout {
        sequence_id: u64,
        code: i32,
        message: String,
    },
    Other(String),
}

fn project(outcome: OpOutcome) -> SendResolution {
    match outcome {
        OpOutcome::SendError {
            sequence_id,
            code,
            message,
        } => SendResolution::Timeout {
            sequence_id: sequence_id.0,
            code,
            message,
        },
        other => SendResolution::Other(format!("{other:?}")),
    }
}

/// The shared `magnetar-proto` send-timeout decision both engines' driver loops
/// delegate to: open a producer with the DEFAULT `send_timeout` (ADR-0072, 30s),
/// enqueue one send at `t0`, then tick `handle_timeout` past the 30s deadline
/// with NO `CommandSendReceipt` ever delivered (the receipt was lost/corrupted
/// in flight). Returns the resolved outcome the in-flight send surfaced — the
/// exact value each engine surfaces to the user-facing `send()` future.
fn default_send_timeout_decision() -> SendResolution {
    let t0 = Instant::now();
    let mut conn = Connection::new(ConnectionConfig::default(), Arc::new(SystemTime::now));
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("handshake completes");
    assert!(conn.is_connected(), "precondition: Connected");

    // Open a producer with the canonical default request (send_timeout = 30s).
    let create_rid = conn.peek_next_request_id_for_test();
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/diff-send-timeout".to_owned(),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(t0, &producer_success_bytes(create_rid))
        .expect("producer ready");

    // Enqueue one send at t0; the broker never acks it.
    let seq = conn
        .send(
            handle,
            magnetar_proto::producer::OutgoingMessage {
                payload: bytes::Bytes::from_static(b"lost-receipt"),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 12,
                num_messages: 1,
                txn_id: None,
                source_message_id: None,
            },
            0,
            t0,
        )
        .expect("queue send");
    let _ = conn.poll_transmit();
    let key = PendingOpKey::Send(handle, seq);

    // Just before the 30s deadline: no outcome yet.
    conn.handle_timeout(t0 + Duration::from_secs(29));
    assert!(
        conn.take_outcome(key).is_none(),
        "the default send_timeout must not fire before 30s",
    );

    // Past the 30s deadline: the sweep resolves the send.
    conn.handle_timeout(t0 + Duration::from_secs(31));
    project(
        conn.take_outcome(key)
            .expect("the in-flight send must resolve with a timeout outcome"),
    )
}

#[test]
fn engines_agree_on_default_send_timeout_firing() {
    // Pin the default value itself — the Java client's sendTimeoutMs = 30000.
    assert_eq!(
        CreateProducerRequest::default().send_timeout,
        Some(Duration::from_secs(30)),
        "CreateProducerRequest::default() must carry the 30s Java-parity send_timeout (ADR-0072)",
    );

    // Both engines delegate to the same `magnetar-proto` timeout path; running
    // the shared helper twice with identical input is the differential
    // surrogate for "tokio engine" vs "moonpool engine".
    let tokio_outcome = default_send_timeout_decision();
    let moonpool_outcome = default_send_timeout_decision();
    assert_eq!(
        tokio_outcome, moonpool_outcome,
        "both engines must resolve the timed-out send identically (shared proto path)",
    );

    // Pin the exact outcome the runtime `send()` future consumes: the Pulsar
    // timeout sentinel (`code = -1`, message "send timeout").
    match tokio_outcome {
        SendResolution::Timeout { code, message, .. } => {
            assert_eq!(code, -1, "send-timeout SendError uses the -1 sentinel");
            assert_eq!(message, "send timeout");
        }
        other @ SendResolution::Other(_) => {
            panic!("expected a send-timeout SendError on both engines, got {other:?}")
        }
    }
}
