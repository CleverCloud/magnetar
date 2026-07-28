// SPDX-License-Identifier: Apache-2.0

//! `send_timeout` firing for a publish RELOCATED across a supervised
//! reconnect — differential equivalence (ADR-0024 layer d for issue #369).
//!
//! Root cause: `Connection::reset()` moves every in-flight `OpSend` out of
//! `ProducerState::pending` and into `Connection::in_flight_publish_snapshots`
//! (see `conn.rs`'s `reset()` doc comment). Before the fix,
//! `ProducerState::drain_timed_out_sends` — which only ever walks `pending` —
//! could not see a relocated send, so `send_timeout` never fired for a
//! publish parked across a reconnect; the caller's `send().await` stayed
//! `Pending` for the supervisor's entire reconnect budget instead of a
//! deterministic timeout error.
//!
//! Like `send_timeout_default_equivalence.rs`, this decision lives entirely
//! in `magnetar-proto` (`Connection::poll_timeout` / `Connection::handle_timeout`
//! now also sweep `in_flight_publish_snapshots`) and both runtime engines feed
//! their driver loop's timeout tick through the same `handle_timeout` against
//! an INJECTED clock (ADR-0011). The relocation-across-reset scenario is
//! **invisible to the `Trace`/`EventStream` surface** — the `Op` model has no
//! `reset()` / reconnect knob — so the differential surrogate is the
//! **shared `magnetar-proto` decision run twice** (once per "engine"),
//! exactly mirroring `send_timeout_default_equivalence.rs`'s surrogate
//! pattern. Divergence could only arise if an engine grew an engine-local
//! send-timeout or reset path, which neither does.

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
            producer_name: "diff-send-timeout-reconnect".to_owned(),
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

/// The shared `magnetar-proto` send-timeout decision both engines' driver
/// loops delegate to for issue #369: open a producer with the DEFAULT
/// `send_timeout` (ADR-0072, 30s), enqueue one send at `t0`, call
/// `Connection::reset()` to RELOCATE it into `in_flight_publish_snapshots`
/// (simulating a supervisor reconnect mid-publish), then tick
/// `handle_timeout` past the 30s deadline measured from the ORIGINAL
/// `enqueued_at` — no rebuild, no replayed receipt. Returns the resolved
/// outcome the relocated send surfaced.
fn relocated_send_timeout_decision() -> SendResolution {
    let t0 = Instant::now();
    let mut conn = Connection::new(ConnectionConfig::default(), Arc::new(SystemTime::now));
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("handshake completes");
    assert!(conn.is_connected(), "precondition: Connected");

    // Open a producer with the canonical default request (send_timeout = 30s).
    let create_rid = conn.peek_next_request_id_for_test();
    let handle = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/diff-send-timeout-reconnect".to_owned(),
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    conn.handle_bytes(t0, &producer_success_bytes(create_rid))
        .expect("producer ready");

    // Enqueue one send at t0.
    let seq = conn
        .send(
            handle,
            magnetar_proto::producer::OutgoingMessage {
                payload: bytes::Bytes::from_static(b"relocated-across-reconnect"),
                metadata: pb::MessageMetadata::default(),
                uncompressed_size: 27,
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

    // Relocate: the supervisor observed a mid-publish drop and reset the
    // session. No SessionLost outcome lands (transparent-replay contract);
    // the op moves out of `pending` into `in_flight_publish_snapshots`.
    conn.reset();
    assert!(
        conn.take_outcome(key).is_none(),
        "reset must not install an outcome on the relocated Send key",
    );
    assert_eq!(
        conn.in_flight_publish_snapshot_len(handle),
        1,
        "the relocated send must land in the snapshot bucket",
    );

    // Just before the 30s deadline (measured from the ORIGINAL enqueued_at,
    // not from reset): no outcome yet, snapshot survives.
    conn.handle_timeout(t0 + Duration::from_secs(29));
    assert!(
        conn.take_outcome(key).is_none(),
        "the send_timeout for a relocated send must not fire before 30s",
    );

    // Past the 30s deadline: the sweep resolves the relocated send.
    conn.handle_timeout(t0 + Duration::from_secs(31));
    project(
        conn.take_outcome(key)
            .expect("the relocated in-flight send must resolve with a timeout outcome"),
    )
}

#[test]
fn engines_agree_on_relocated_send_timeout_firing() {
    // Pin the default value itself — the Java client's sendTimeoutMs = 30000.
    assert_eq!(
        CreateProducerRequest::default().send_timeout,
        Some(Duration::from_secs(30)),
        "CreateProducerRequest::default() must carry the 30s Java-parity send_timeout (ADR-0072)",
    );

    // Both engines delegate to the same `magnetar-proto` timeout + reset
    // path; running the shared helper twice with identical input is the
    // differential surrogate for "tokio engine" vs "moonpool engine".
    let tokio_outcome = relocated_send_timeout_decision();
    let moonpool_outcome = relocated_send_timeout_decision();
    assert_eq!(
        tokio_outcome, moonpool_outcome,
        "both engines must resolve the relocated timed-out send identically (shared proto path)",
    );

    // Pin the exact outcome the runtime `send()` future consumes: the Pulsar
    // timeout sentinel (`code = -1`, message "send timeout") — the SAME
    // outcome the live-queue path installs (issue #369's fix must not invent
    // a different error shape for the relocated case).
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
