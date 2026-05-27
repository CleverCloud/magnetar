// SPDX-License-Identifier: Apache-2.0
#![cfg(feature = "experimental-v5-client")]

//! PIP-466 V5 builder-defaults table-driven test.
//!
//! Table-driven assertion over every V5 default exposed by
//! `magnetar::v5::mapping`, pinning the Java parity values
//! enumerated in the mapping table at the top of
//! `crates/magnetar/src/v5/mapping.rs`. Re-asserts the per-translation
//! invariants the `mapping::tests` block already covers, framed as the
//! V5 → v4 contract surfaced at the integration boundary (so any
//! future builder-internal refactor that drifted from the mapping
//! constants would fail this test, not just the unit tests).
//!
//! Final file of the 5-test PIP-466 mapping suite.

use std::time::Duration;

use magnetar::v5::mapping::{
    DEFAULT_ACK_TIMEOUT, DEFAULT_MAX_PENDING_MESSAGES, DEFAULT_NEGATIVE_ACK_REDELIVERY_DELAY,
    DEFAULT_RECEIVER_QUEUE_SIZE, DEFAULT_SEND_TIMEOUT, V5SubscriptionInitialPosition,
    ack_timeout_to_ms, max_pending_messages_to_v4, negative_ack_redelivery_delay_to_ms,
    send_timeout_to_ms,
};
use magnetar_proto::pb;

/// One row of the V5 → v4 mapping table.
struct DefaultRow {
    name: &'static str,
    v4_value: u64,
}

#[test]
fn v5_default_table_matches_pip_466_spec() {
    // The table mirrors the doc comment at the top of
    // `crates/magnetar/src/v5/mapping.rs`. Each row asserts that the
    // V5 default constant, when fed through the matching translation
    // function, produces the v4 wire value enumerated in PIP-466.
    let rows = [
        DefaultRow {
            name: "send_timeout (default 30s → 30_000 ms)",
            v4_value: send_timeout_to_ms(DEFAULT_SEND_TIMEOUT),
        },
        DefaultRow {
            name: "ack_timeout (default None → 0 ms wire disabled-sentinel)",
            v4_value: ack_timeout_to_ms(DEFAULT_ACK_TIMEOUT),
        },
        DefaultRow {
            name: "negative_ack_redelivery_delay (default 60s → 60_000 ms)",
            v4_value: negative_ack_redelivery_delay_to_ms(DEFAULT_NEGATIVE_ACK_REDELIVERY_DELAY),
        },
        DefaultRow {
            name: "max_pending_messages (default Some(1000) → 1000)",
            v4_value: max_pending_messages_to_v4(DEFAULT_MAX_PENDING_MESSAGES) as u64,
        },
        DefaultRow {
            name: "receiver_queue_size (default 1000)",
            v4_value: DEFAULT_RECEIVER_QUEUE_SIZE as u64,
        },
    ];

    let expected: &[(&str, u64)] = &[
        ("send_timeout (default 30s → 30_000 ms)", 30_000),
        (
            "ack_timeout (default None → 0 ms wire disabled-sentinel)",
            0,
        ),
        (
            "negative_ack_redelivery_delay (default 60s → 60_000 ms)",
            60_000,
        ),
        ("max_pending_messages (default Some(1000) → 1000)", 1000),
        ("receiver_queue_size (default 1000)", 1000),
    ];

    assert_eq!(rows.len(), expected.len());
    for (row, (exp_name, exp_value)) in rows.iter().zip(expected.iter()) {
        assert_eq!(row.name, *exp_name, "row order matches expectation");
        assert_eq!(
            row.v4_value, *exp_value,
            "V5 default mismatch on row '{}': got {}, expected {}",
            row.name, row.v4_value, exp_value
        );
    }
}

#[test]
fn v5_subscription_initial_position_default_is_latest() {
    // PIP-466 spec: subscription_initial_position defaults to Latest.
    // Pinned at both the V5 enum level and the v4 wire enum level.
    assert_eq!(
        V5SubscriptionInitialPosition::default(),
        V5SubscriptionInitialPosition::Latest
    );
    assert_eq!(
        V5SubscriptionInitialPosition::default().into_pb(),
        pb::command_subscribe::InitialPosition::Latest
    );
}

#[test]
fn v5_translation_edge_cases() {
    // Edge cases worth pinning at the integration tier:
    //
    // 1. `ack_timeout = Some(Duration::ZERO)` and `ack_timeout = None` BOTH translate to wire `0`.
    //    The V5 type distinguishes them (None vs Some(0)) but the v4 wire collapses them — pinning
    //    avoids accidental future refactors that would treat Some(Duration::ZERO) as a tiny but
    //    non-disabled value.
    assert_eq!(ack_timeout_to_ms(None), 0);
    assert_eq!(ack_timeout_to_ms(Some(Duration::ZERO)), 0);
    // 2. `max_pending_messages = Some(0)` and `None` both translate to `0` (the v4 "unlimited"
    //    sentinel).
    assert_eq!(max_pending_messages_to_v4(None), 0);
    assert_eq!(max_pending_messages_to_v4(Some(0)), 0);
    // 3. `send_timeout` saturation: pathological `Duration` values clamp at u64::MAX rather than
    //    panic.
    let huge = Duration::from_secs(u64::MAX / 1000 + 1);
    assert_eq!(send_timeout_to_ms(huge), u64::MAX);
}

#[test]
fn v5_subscription_initial_position_both_variants_round_trip() {
    // Both V5 variants must map cleanly to the v4 wire enum.
    assert_eq!(
        V5SubscriptionInitialPosition::Latest.into_pb(),
        pb::command_subscribe::InitialPosition::Latest
    );
    assert_eq!(
        V5SubscriptionInitialPosition::Earliest.into_pb(),
        pb::command_subscribe::InitialPosition::Earliest
    );
}
