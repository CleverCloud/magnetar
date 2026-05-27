// SPDX-License-Identifier: Apache-2.0

//! **Experimental** — PIP-466 V5 ↔ v4 field translation table.
//!
//! Single source of truth for the V5 default values + the `Duration` /
//! `Option<usize>` / `Option<Duration>` translations the V5 builders
//! apply when delegating to the v4 surface. Centralised here so the
//! per-type tests in `tests/v5_*_mapping.rs` (a table-driven assertion
//! per default) can read the canonical values without duplication.
//!
//! Mapping table (V5 → v4):
//!
//! | V5 builder field                  | V5 type                  | V5 default        | v4 field on `CreateProducerRequest` / `SubscribeRequest` | v4 type   |
//! |-----------------------------------|--------------------------|-------------------|-----------------------------------------------------------|-----------|
//! | `send_timeout`                    | `Duration`               | `30 s`            | `send_timeout` (millis)                                   | `u64`     |
//! | `max_pending_messages`            | `Option<usize>`          | `Some(1000)`      | `max_pending_messages` (`0` = unlimited)                  | `usize`   |
//! | `ack_timeout`                     | `Option<Duration>`       | `None`            | `ack_timeout_ms` (`0` = disabled)                         | `u64`     |
//! | `negative_ack_redelivery_delay`   | `Duration`               | `60 s`            | `negative_ack_redelivery_delay_ms`                        | `u64`     |
//! | `receiver_queue_size`             | `usize`                  | `1000`            | `receiver_queue_size`                                     | `usize`   |
//! | `subscription_initial_position`   | `V5SubscriptionInitialPosition` | `Latest`   | `pb::command_subscribe::InitialPosition`                  | enum      |

use std::time::Duration;

/// V5 default `send_timeout`. Mirrors Java V5 `ProducerBuilder#sendTimeout(30s)`.
pub const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// V5 default `max_pending_messages`. `Some(1000)` matches Java V5
/// `ProducerBuilder#maxPendingMessages(1000)`; `None` is the V5 escape
/// hatch for unlimited (translates to `0` on the v4 wire field).
pub const DEFAULT_MAX_PENDING_MESSAGES: Option<usize> = Some(1000);

/// V5 default `ack_timeout`. `None` matches Java V5
/// `ConsumerBuilder#ackTimeout(0, …)` (the disabled state); the v4
/// translation is `0` millis.
pub const DEFAULT_ACK_TIMEOUT: Option<Duration> = None;

/// V5 default `negative_ack_redelivery_delay`. Mirrors Java V5
/// `ConsumerBuilder#negativeAckRedeliveryDelay(60s)`.
pub const DEFAULT_NEGATIVE_ACK_REDELIVERY_DELAY: Duration = Duration::from_secs(60);

/// V5 default `receiver_queue_size`. Mirrors Java V5
/// `ConsumerBuilder#receiverQueueSize(1000)`.
pub const DEFAULT_RECEIVER_QUEUE_SIZE: usize = 1000;

/// V5 `subscription_initial_position`. Type-level mirror of
/// `pb::command_subscribe::InitialPosition`. The two variants
/// translate 1:1 to the v4 wire enum; the wrapper exists to keep V5
/// callers off the `pb::` namespace.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum V5SubscriptionInitialPosition {
    /// Start from the most recent message at subscribe time.
    #[default]
    Latest,
    /// Start from the earliest available message.
    Earliest,
}

impl V5SubscriptionInitialPosition {
    /// Translate to the v4 wire enum.
    #[must_use]
    pub fn into_pb(self) -> magnetar_proto::pb::command_subscribe::InitialPosition {
        match self {
            Self::Latest => magnetar_proto::pb::command_subscribe::InitialPosition::Latest,
            Self::Earliest => magnetar_proto::pb::command_subscribe::InitialPosition::Earliest,
        }
    }
}

/// Translate V5's `Duration` send-timeout to the v4 millis-as-`u64`.
/// Saturating: durations beyond `u64::MAX` ms clamp at the ceiling
/// (rather than wrap) so callers passing nonsense get the most
/// permissive interpretation.
#[must_use]
pub fn send_timeout_to_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Translate V5's `Option<Duration>` ack-timeout to the v4
/// millis-as-`u64`. `None` becomes `0` (disabled, matching the v4
/// invariant on `ack_timeout_ms == 0`).
#[must_use]
pub fn ack_timeout_to_ms(d: Option<Duration>) -> u64 {
    match d {
        Some(d) => u64::try_from(d.as_millis()).unwrap_or(u64::MAX),
        None => 0,
    }
}

/// Translate V5's `Duration` negative-ack delay to the v4
/// millis-as-`u64`.
#[must_use]
pub fn negative_ack_redelivery_delay_to_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Translate V5's `Option<usize>` max-pending to the v4 `usize`.
/// `None` becomes `0` (the v4 invariant for "unlimited"). The
/// per-engine producer enforces the limit downstream.
#[must_use]
pub fn max_pending_messages_to_v4(v: Option<usize>) -> usize {
    v.unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_pip_466_spec() {
        assert_eq!(DEFAULT_SEND_TIMEOUT, Duration::from_secs(30));
        assert_eq!(DEFAULT_MAX_PENDING_MESSAGES, Some(1000));
        assert_eq!(DEFAULT_ACK_TIMEOUT, None);
        assert_eq!(
            DEFAULT_NEGATIVE_ACK_REDELIVERY_DELAY,
            Duration::from_secs(60)
        );
        assert_eq!(DEFAULT_RECEIVER_QUEUE_SIZE, 1000);
    }

    #[test]
    fn initial_position_default_is_latest() {
        assert_eq!(
            V5SubscriptionInitialPosition::default(),
            V5SubscriptionInitialPosition::Latest,
        );
    }

    #[test]
    fn duration_translations_match_wire() {
        assert_eq!(send_timeout_to_ms(Duration::from_secs(30)), 30_000);
        assert_eq!(ack_timeout_to_ms(None), 0);
        assert_eq!(ack_timeout_to_ms(Some(Duration::from_millis(750))), 750);
        assert_eq!(
            negative_ack_redelivery_delay_to_ms(Duration::from_secs(60)),
            60_000
        );
    }

    #[test]
    fn max_pending_none_maps_to_zero() {
        assert_eq!(max_pending_messages_to_v4(None), 0);
        assert_eq!(max_pending_messages_to_v4(Some(42)), 42);
    }

    #[test]
    fn initial_position_round_trips_to_pb() {
        use magnetar_proto::pb::command_subscribe::InitialPosition;
        assert_eq!(
            V5SubscriptionInitialPosition::Latest.into_pb(),
            InitialPosition::Latest,
        );
        assert_eq!(
            V5SubscriptionInitialPosition::Earliest.into_pb(),
            InitialPosition::Earliest,
        );
    }

    #[test]
    fn send_timeout_saturates_at_u64_max() {
        // The spec mandates saturation rather than wraparound for
        // pathological inputs (Duration::MAX has 2^64 seconds × 1000
        // ms which overflows u64). The saturating clamp is the
        // most-permissive interpretation.
        let huge = Duration::from_secs(u64::MAX / 1000 + 1);
        assert_eq!(send_timeout_to_ms(huge), u64::MAX);
    }

    #[test]
    fn ack_timeout_zero_duration_round_trips_to_zero() {
        // `Some(Duration::ZERO)` is distinct from `None` at the V5
        // type level but both translate to `0` on the v4 wire (the
        // "disabled" sentinel). Pinning this avoids accidental
        // future refactors that would treat zero-duration as a tiny
        // but non-disabled value.
        assert_eq!(ack_timeout_to_ms(Some(Duration::ZERO)), 0);
        assert_eq!(ack_timeout_to_ms(None), 0);
    }

    #[test]
    fn max_pending_messages_zero_via_some_round_trips_to_zero() {
        // Some(0) is the explicit "unlimited" spelling and matches None
        // at the v4 wire layer (where `0` is the unlimited sentinel).
        // Documenting the equivalence keeps callers from over-thinking
        // the Some/None distinction.
        assert_eq!(max_pending_messages_to_v4(Some(0)), 0);
        assert_eq!(max_pending_messages_to_v4(None), 0);
    }

    #[test]
    fn initial_position_is_copy_and_eq() {
        // Type-level guard: the wrapper enum stays Copy + Eq so
        // callers can pattern-match without ownership ceremony.
        fn assert_copy_eq<T: Copy + Eq>() {}
        assert_copy_eq::<V5SubscriptionInitialPosition>();
    }
}
