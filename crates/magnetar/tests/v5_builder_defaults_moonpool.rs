// SPDX-License-Identifier: Apache-2.0
#![cfg(all(feature = "experimental-v5-client", feature = "moonpool"))]

//! PIP-466 V5 builder-defaults table-driven test — moonpool engine mirror.
//!
//! Companion to `v5_builder_defaults.rs` (tokio mirror). The V5 → v4
//! mapping table is engine-agnostic, but we run the assertions a second
//! time from a moonpool-named test file to keep the V5 surface coverage
//! symmetric across engines (one of the WAVE 3 acceptance criteria for
//! flipping ADR-0032 from Proposed to Accepted).

use std::time::Duration;

use magnetar::v5::PulsarClientV5;
use magnetar::v5::mapping::{
    DEFAULT_ACK_TIMEOUT, DEFAULT_MAX_PENDING_MESSAGES, DEFAULT_NEGATIVE_ACK_REDELIVERY_DELAY,
    DEFAULT_RECEIVER_QUEUE_SIZE, DEFAULT_SEND_TIMEOUT, V5SubscriptionInitialPosition,
    ack_timeout_to_ms, max_pending_messages_to_v4, negative_ack_redelivery_delay_to_ms,
    send_timeout_to_ms,
};
use magnetar::{MoonpoolEngine, PulsarClient};
use magnetar_proto::pb;
use moonpool_core::TokioProviders;

// Local alias used inside inner test fns; declared via `#[allow]` so
// rustc doesn't flag it as "module-private alias only used in nested
// fns" (the `dead_code` lint is over-eager here).
#[allow(dead_code)]
type Mp = MoonpoolEngine<TokioProviders>;

struct DefaultRow {
    name: &'static str,
    v4_value: u64,
}

#[test]
fn v5_default_table_matches_pip_466_spec_under_moonpool() {
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
        assert_eq!(row.name, *exp_name);
        assert_eq!(row.v4_value, *exp_value);
    }
}

#[test]
fn v5_subscription_initial_position_default_is_latest_under_moonpool() {
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
fn v5_translation_edge_cases_under_moonpool() {
    assert_eq!(ack_timeout_to_ms(None), 0);
    assert_eq!(ack_timeout_to_ms(Some(Duration::ZERO)), 0);
    assert_eq!(max_pending_messages_to_v4(None), 0);
    assert_eq!(max_pending_messages_to_v4(Some(0)), 0);
    let huge = Duration::from_secs(u64::MAX / 1000 + 1);
    assert_eq!(send_timeout_to_ms(huge), u64::MAX);
}

#[test]
fn v5_full_builder_chain_compiles_against_moonpool_engine() {
    // The end-to-end WAVE 3 type-shape pinning: PulsarClientV5<Mp>
    // returns engine-parametric builders for every consumer / producer
    // family, and each builder's terminal `subscribe()` / `create()`
    // resolves through the engine-generic SubscribeApi /
    // CreateProducerApi extension traits.
    fn _producer_chain(c: &PulsarClientV5<Mp>) -> magnetar::v5::producer::ProducerBuilder<'_, Mp> {
        c.producer("t")
            .send_timeout(Duration::from_secs(5))
            .max_pending_messages(Some(100))
    }
    fn _stream_chain(
        c: &PulsarClientV5<Mp>,
    ) -> magnetar::v5::stream_consumer::StreamConsumerBuilder<'_, Mp> {
        c.stream_consumer("t")
            .subscription("s")
            .receiver_queue_size(500)
            .ack_timeout(Some(Duration::from_secs(30)))
    }
    fn _queue_chain(
        c: &PulsarClientV5<Mp>,
    ) -> magnetar::v5::queue_consumer::QueueConsumerBuilder<'_, Mp> {
        c.queue_consumer("t")
            .subscription("s")
            .receiver_queue_size(500)
    }
    fn _v4_escape(c: PulsarClientV5<Mp>) -> PulsarClient<Mp> {
        c.into_v4()
    }
}
