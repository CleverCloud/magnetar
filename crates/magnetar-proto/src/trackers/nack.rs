// SPDX-License-Identifier: Apache-2.0

//! Negative acknowledgment + redelivery tracker.
//!
//! Port of `org.apache.pulsar.client.impl.NegativeAcksTracker`. The Java tracker keeps a
//! `Map<MessageId, Long>` of "redeliver at" timestamps and groups them into a single
//! `CommandRedeliverUnacknowledgedMessages` per tick.
//!
//! # References
//!
//! - `NegativeAcksTracker.java:67-94` (add)
//! - `NegativeAcksTracker.java:114-148` (trigger redelivery)

use core::time::Duration;
use std::collections::HashMap;
use std::time::Instant;

use crate::types::{ConsumerHandle, MessageId};

/// Action emitted by the nack tracker on a tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NackAction {
    /// Emit a `CommandRedeliverUnacknowledgedMessages` for the given message ids.
    RedeliverUnacked {
        /// Consumer that owns the redelivery.
        handle: ConsumerHandle,
        /// Message ids whose redelivery delay has elapsed.
        message_ids: Vec<MessageId>,
    },
}

/// Negative-ack tracker for one consumer.
#[derive(Debug)]
pub struct NegativeAcksTracker {
    handle: ConsumerHandle,
    redelivery_delay: Duration,
    pending: HashMap<MessageId, Instant>,
}

impl NegativeAcksTracker {
    /// Construct a new tracker.
    pub fn new(handle: ConsumerHandle, redelivery_delay: Duration) -> Self {
        Self {
            handle,
            redelivery_delay,
            pending: HashMap::new(),
        }
    }

    /// Nack a message. Returns no immediate actions; the redelivery will fire on a later tick.
    pub fn add(&mut self, message_id: MessageId, now: Instant) {
        let deadline = now + self.redelivery_delay;
        self.pending.insert(message_id, deadline);
    }

    /// Nack a message with an explicit per-message delay. Bypasses the tracker's default
    /// `redelivery_delay`. Mirrors Java's PIP-37 `NegativeAckRedeliveryBackoff` flow where
    /// the per-message delay is computed from the broker-reported redelivery count via
    /// [`MultiplierRedeliveryBackoff::delay_for`].
    pub fn add_with_delay(&mut self, message_id: MessageId, delay: Duration, now: Instant) {
        self.pending.insert(message_id, now + delay);
    }

    /// Drop tracking for a message (e.g. positive ack arrived after the nack).
    pub fn remove(&mut self, message_id: &MessageId) {
        self.pending.remove(message_id);
    }

    /// Tick the tracker. Returns redelivery actions for any messages whose delay has elapsed.
    pub fn poll(&mut self, now: Instant) -> Vec<NackAction> {
        let mut due: Vec<MessageId> = self
            .pending
            .iter()
            .filter_map(|(id, deadline)| (*deadline <= now).then_some(*id))
            .collect();
        if due.is_empty() {
            return Vec::new();
        }
        for id in &due {
            self.pending.remove(id);
        }
        due.sort();
        vec![NackAction::RedeliverUnacked {
            handle: self.handle,
            message_ids: due,
        }]
    }

    /// Returns the earliest deadline, if any.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.pending.values().min().copied()
    }

    /// Returns whether any nacked messages are still pending.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

/// PIP-37 redelivery backoff. Mirrors Java's `MultiplierRedeliveryBackoff` —
/// `delay = clamp(min_delay * multiplier^redelivery_count, min_delay, max_delay)`.
/// The caller pre-computes the delay from the broker-reported redelivery count and hands
/// it to [`NegativeAcksTracker::add_with_delay`] (or the runtime-facing
/// `Consumer::negative_ack_with_delay`).
#[derive(Debug, Clone, Copy)]
pub struct MultiplierRedeliveryBackoff {
    /// Floor delay — the very first redelivery (count 0) waits this long.
    pub min_delay: Duration,
    /// Ceiling — the delay clamps here no matter how many times the message has cycled.
    pub max_delay: Duration,
    /// Geometric multiplier. Java default is 2.0 (delay doubles every cycle).
    pub multiplier: f64,
}

impl MultiplierRedeliveryBackoff {
    /// Compute the delay for the given redelivery count. `redelivery_count` is the broker's
    /// view (the `redelivery_count` field on the incoming message). Returns a value clamped
    /// to `[min_delay, max_delay]`.
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    #[must_use]
    pub fn delay_for(&self, redelivery_count: u32) -> Duration {
        // Short-circuit large counts (with `multiplier > 1.0` the result has already
        // exceeded `max_delay` by exponent ~64). This avoids relying on `f64::INFINITY`
        // surviving the `.powi -> mul -> as u64` cast chain — `f64::INFINITY as u64`
        // is platform-defined and on some hosts truncates to `0`, which would silently
        // collapse the delay back to `min_delay`. The threshold is conservative; in
        // practice broker-reported `redelivery_count` is bounded by `max_redeliver_count`
        // (DLQ kicks in long before this), so this branch is purely defensive.
        if self.multiplier > 1.0 && redelivery_count > 64 {
            return self.max_delay;
        }
        let exp = redelivery_count.min(i32::MAX as u32) as i32;
        let factor = self.multiplier.powi(exp);
        let min_ms_u128 = self.min_delay.as_millis();
        let min_ms = if min_ms_u128 > u128::from(u64::MAX) {
            u64::MAX as f64
        } else {
            min_ms_u128 as f64
        };
        let product = min_ms * factor;
        let scaled_ms = if !product.is_finite() || product > u64::MAX as f64 {
            u64::MAX as f64
        } else if product < 0.0 {
            0.0
        } else {
            product
        };
        let scaled = Duration::from_millis(scaled_ms as u64);
        if scaled < self.min_delay {
            self.min_delay
        } else if scaled > self.max_delay {
            self.max_delay
        } else {
            scaled
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mid(entry: u64) -> MessageId {
        MessageId {
            ledger_id: 1,
            entry_id: entry,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
        }
    }

    #[test]
    fn fires_after_delay() {
        let mut t = NegativeAcksTracker::new(ConsumerHandle(1), Duration::from_millis(100));
        let t0 = Instant::now();
        t.add(mid(1), t0);
        assert!(t.poll(t0 + Duration::from_millis(50)).is_empty());
        let actions = t.poll(t0 + Duration::from_millis(101));
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            NackAction::RedeliverUnacked { message_ids, .. } => {
                assert_eq!(message_ids.len(), 1);
                assert_eq!(message_ids[0].entry_id, 1);
            }
        }
        assert!(t.is_empty());
    }

    #[test]
    fn remove_cancels_pending() {
        let mut t = NegativeAcksTracker::new(ConsumerHandle(1), Duration::from_millis(100));
        let t0 = Instant::now();
        t.add(mid(1), t0);
        t.remove(&mid(1));
        assert!(t.poll(t0 + Duration::from_secs(10)).is_empty());
    }

    #[test]
    fn add_with_delay_overrides_default() {
        let mut t = NegativeAcksTracker::new(ConsumerHandle(1), Duration::from_secs(10));
        let t0 = Instant::now();
        // Override with a tiny delay so the redelivery fires almost immediately.
        t.add_with_delay(mid(7), Duration::from_millis(5), t0);
        // 4ms in — not yet due.
        assert!(t.poll(t0 + Duration::from_millis(4)).is_empty());
        // 10ms in — past the explicit deadline (well under the 10s default).
        let actions = t.poll(t0 + Duration::from_millis(10));
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn multiplier_backoff_grows_then_clamps() {
        let b = MultiplierRedeliveryBackoff {
            min_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        };
        assert_eq!(b.delay_for(0), Duration::from_millis(100));
        assert_eq!(b.delay_for(1), Duration::from_millis(200));
        assert_eq!(b.delay_for(3), Duration::from_millis(800));
        // Far past the ceiling — clamps to max_delay.
        assert_eq!(b.delay_for(40), Duration::from_secs(60));
    }

    #[test]
    fn empty_tracker_has_no_deadline() {
        let t = NegativeAcksTracker::new(ConsumerHandle(1), Duration::from_secs(1));
        assert!(t.is_empty());
        assert!(t.next_deadline().is_none());
    }

    #[test]
    fn duplicate_add_overwrites_deadline() {
        // Adding the same id twice with different times overwrites the deadline (the second
        // wins). Matches Java behavior where re-nacking pushes the redelivery time forward.
        let mut t = NegativeAcksTracker::new(ConsumerHandle(1), Duration::from_millis(100));
        let t0 = Instant::now();
        t.add(mid(1), t0);
        t.add(mid(1), t0 + Duration::from_millis(50)); // second add — later deadline
        // 110ms in: first add's deadline would fire, but the second add pushed it to 150ms.
        assert!(t.poll(t0 + Duration::from_millis(110)).is_empty());
        // 160ms in: now past the overwritten deadline.
        let actions = t.poll(t0 + Duration::from_millis(160));
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn next_deadline_returns_earliest() {
        let mut t = NegativeAcksTracker::new(ConsumerHandle(1), Duration::from_secs(10));
        let t0 = Instant::now();
        t.add(mid(1), t0);
        t.add_with_delay(mid(2), Duration::from_millis(50), t0); // earlier
        t.add_with_delay(mid(3), Duration::from_secs(5), t0);
        let next = t.next_deadline().unwrap();
        assert!(next <= t0 + Duration::from_millis(50));
    }

    #[test]
    fn poll_groups_co_due_messages_into_one_action() {
        // Three messages added at the same wall clock — one action with all three ids.
        let mut t = NegativeAcksTracker::new(ConsumerHandle(7), Duration::from_millis(50));
        let t0 = Instant::now();
        t.add(mid(1), t0);
        t.add(mid(2), t0);
        t.add(mid(3), t0);
        let actions = t.poll(t0 + Duration::from_millis(60));
        assert_eq!(actions.len(), 1, "co-due redeliveries must coalesce");
        match &actions[0] {
            NackAction::RedeliverUnacked {
                handle,
                message_ids,
            } => {
                assert_eq!(*handle, ConsumerHandle(7));
                assert_eq!(message_ids.len(), 3);
            }
        }
        assert!(t.is_empty());
    }

    #[test]
    fn remove_unknown_id_is_safe() {
        let mut t = NegativeAcksTracker::new(ConsumerHandle(1), Duration::from_millis(100));
        t.remove(&mid(42));
        assert!(t.is_empty());
        assert!(t.poll(Instant::now() + Duration::from_secs(1)).is_empty());
    }

    #[test]
    fn multiplier_with_unity_stays_at_min() {
        let b = MultiplierRedeliveryBackoff {
            min_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(60),
            multiplier: 1.0,
        };
        for n in 0..10u32 {
            assert_eq!(b.delay_for(n), Duration::from_millis(200));
        }
    }

    #[test]
    fn multiplier_zero_count_returns_min() {
        let b = MultiplierRedeliveryBackoff {
            min_delay: Duration::from_millis(750),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        };
        assert_eq!(b.delay_for(0), Duration::from_millis(750));
    }

    #[test]
    fn multiplier_max_redelivery_count_clamps_to_max() {
        // u32::MAX should not overflow / panic — the saturating math path clamps.
        let b = MultiplierRedeliveryBackoff {
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
        };
        assert_eq!(b.delay_for(u32::MAX), Duration::from_secs(10));
    }
}
