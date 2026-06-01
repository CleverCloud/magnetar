// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for the M7 chaos-pack integration tests.
//!
//! These tests drive the sans-io [`magnetar_proto::Connection`] state machine
//! through the moonpool engine's [`ConnectionShared`] wrapper *without*
//! spinning up a driver task or a TCP listener. The strategy mirrors the
//! per-module `#[cfg(test)] mod tests` blocks that already live inside
//! `magnetar-runtime-moonpool/src/**` — synthetic broker frames go in via
//! [`Connection::handle_bytes`], synthetic `Instant`s drive every timer, and
//! virtual deadlines never touch the host wall clock. That makes the
//! resulting tests deterministic in a way `testcontainers`-based e2e tests
//! never can be, regardless of whether the workspace eventually pulls in the
//! `moonpool-sim` providers.

// Each chaos test file lives in its own binary, so a `pub` helper in this
// module is "unreachable" from the perspective of any single test binary —
// the integration-test layout *requires* `pub` items in `tests/common/mod.rs`
// (rustc has no notion of "shared test helper").
#![allow(dead_code, unreachable_pub)]

use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, ProducerHandle, SequenceId, encode_command, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

/// Derive `count` deterministic `u64` seeds from the outer `MOONPOOL_SEED`
/// env var (default `0x4242_4242_4242_4242` when unset).
///
/// Uses splitmix64 — a stateless integer hash widely used for seed expansion
/// (no dep, public-domain construction). Daily-sweep failures reported via
/// `MOONPOOL_SEED=<X>` reproduce bit-for-bit under the same env value
/// (ADR-0036 / ADR-0047). Accepts both `0x…` hex and bare decimal — the
/// daily-sweep workflow echoes both forms.
///
/// Wire into a sweep test as:
///
/// ```ignore
/// let report = SimulationBuilder::new()
///     .workload(broker)
///     .workload(client)
///     .set_debug_seeds(sweep_seeds(16))
///     .set_iterations(16)
///     .run();
/// ```
pub fn sweep_seeds(count: usize) -> Vec<u64> {
    let base = std::env::var("MOONPOOL_SEED")
        .ok()
        .and_then(|s| {
            let s = s.trim();
            if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
                u64::from_str_radix(hex, 16).ok()
            } else {
                s.parse::<u64>().ok()
            }
        })
        .unwrap_or(0x4242_4242_4242_4242_u64);
    let mut x = base;
    (0..count)
        .map(|_| {
            x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = x;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        })
        .collect()
}

/// Build a synthetic `CommandConnected` frame matching the production engine's
/// expectations. Mirrors the helper used by the per-module tests so chaos
/// tests stay in lockstep when the handshake shape changes.
pub fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
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

/// Spin up a `ConnectionShared` whose inner state machine has completed the
/// handshake at the synthetic instant `at`, so `create_producer` /
/// `subscribe` run cleanly without erroring on protocol-state checks.
pub fn handshake_complete_shared(at: Instant) -> Arc<ConnectionShared> {
    handshake_complete_shared_with_config(at, ConnectionConfig::default())
}

/// Same as [`handshake_complete_shared`] but lets the caller customise the
/// underlying [`ConnectionConfig`] (used for the OAuth refresh-edge test that
/// needs an injectable wall-clock provider on the [`magnetar_proto::Connection`]
/// itself, and for any test that wants a non-default keepalive interval).
pub fn handshake_complete_shared_with_config(
    at: Instant,
    config: ConnectionConfig,
) -> Arc<ConnectionShared> {
    let shared = ConnectionShared::new(config);
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        let frame = handshake_response_bytes();
        conn.handle_bytes(at, &frame).expect("connected");
        // Drain the post-handshake `Connected` event so the test code sees a
        // clean queue.
        let _ = conn.poll_event();
    }
    shared
}

/// Open a producer against the supplied `ConnectionShared` and feed back a
/// synthetic `CommandProducerSuccess` so the state machine treats it as ready.
/// Returns the allocated [`ProducerHandle`].
pub fn open_producer_ready(
    shared: &Arc<ConnectionShared>,
    topic: &str,
    at: Instant,
) -> ProducerHandle {
    let req = CreateProducerRequest {
        topic: topic.to_owned(),
        ..Default::default()
    };
    let (handle, request_id) = {
        let mut conn = shared.inner.lock();
        // `create_producer` allocates one request id; peek the slot *before*
        // we open so we can correlate the synthetic `ProducerSuccess` we
        // feed back below.
        let request_id = conn.peek_next_request_id_for_test();
        let handle = conn.create_producer(req);
        (handle, request_id)
    };
    let success = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: format!("magnetar-test-{}", handle.0),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: None,
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &success).expect("encode CommandProducerSuccess");
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(at, &buf).expect("apply ProducerSuccess");
        // Drain the resulting `ProducerReady` event so the chaos test queue
        // starts clean.
        let _ = conn.poll_event();
    }
    handle
}

/// Encode a synthetic `CommandSendReceipt` for the given producer and
/// sequence id. The broker echoes the producer id back so the state machine
/// can route the receipt to the right slot.
pub fn send_receipt_bytes(
    producer_handle: ProducerHandle,
    sequence_id: SequenceId,
    ledger_id: u64,
    entry_id: u64,
) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::SendReceipt as i32,
        send_receipt: Some(pb::CommandSendReceipt {
            producer_id: producer_handle.0,
            sequence_id: sequence_id.0,
            message_id: Some(pb::MessageIdData {
                ledger_id,
                entry_id,
                partition: None,
                batch_index: None,
                ack_set: vec![],
                batch_size: None,
                first_chunk_message_id: None,
            }),
            highest_sequence_id: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandSendReceipt");
    buf
}

/// Encode a synthetic `CommandNewTxnResponse` for the given request id and
/// txn id parts. PIP-31 — the broker echoes back the request id so the proto
/// layer can route the result.
pub fn new_txn_response_bytes(request_id: u64, txn_most: u64, txn_least: u64) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::NewTxnResponse as i32,
        new_txn_response: Some(pb::CommandNewTxnResponse {
            request_id,
            txnid_most_bits: Some(txn_most),
            txnid_least_bits: Some(txn_least),
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandNewTxnResponse");
    buf
}

/// Encode a `CommandAddPartitionToTxnResponse` for the given request id.
pub fn add_partition_to_txn_response_bytes(request_id: u64) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::AddPartitionToTxnResponse as i32,
        add_partition_to_txn_response: Some(pb::CommandAddPartitionToTxnResponse {
            request_id,
            txnid_most_bits: None,
            txnid_least_bits: None,
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandAddPartitionToTxnResponse");
    buf
}

/// Encode a `CommandAddSubscriptionToTxnResponse` for the given request id.
pub fn add_subscription_to_txn_response_bytes(request_id: u64) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::AddSubscriptionToTxnResponse as i32,
        add_subscription_to_txn_response: Some(pb::CommandAddSubscriptionToTxnResponse {
            request_id,
            txnid_most_bits: None,
            txnid_least_bits: None,
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandAddSubscriptionToTxnResponse");
    buf
}

/// Encode a `CommandEndTxnResponse` for the given request id.
pub fn end_txn_response_bytes(request_id: u64) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::EndTxnResponse as i32,
        end_txn_response: Some(pb::CommandEndTxnResponse {
            request_id,
            txnid_most_bits: None,
            txnid_least_bits: None,
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandEndTxnResponse");
    buf
}

/// Encode a `CommandTopicMigrated` for a producer. The PIP-188 flow surfaces
/// a recoverable `EngineError::Config` from the moonpool driver loop, but
/// the proto state machine accepts the frame and emits a `TopicMigrated`
/// event that callers can pluck via `poll_event`.
pub fn topic_migrated_bytes(
    producer_handle: ProducerHandle,
    new_url: Option<&str>,
    new_url_tls: Option<&str>,
) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::TopicMigrated as i32,
        topic_migrated: Some(pb::CommandTopicMigrated {
            resource_id: producer_handle.0,
            resource_type: pb::command_topic_migrated::ResourceType::Producer as i32,
            broker_service_url: new_url.map(str::to_owned),
            broker_service_url_tls: new_url_tls.map(str::to_owned),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandTopicMigrated");
    buf
}
