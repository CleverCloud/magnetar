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
        // `as i32` is fine even for huge `redelivery_count` because powi for any positive
        // i32 saturates the result well past max_delay (caught by the clamp below). Negative
        // outcomes do not occur — we never feed a negative count.
        let factor = self.multiplier.powi(redelivery_count as i32);
        // Saturate the millis-as-f64 conversion so an absurdly large min_delay does not
        // wrap. f64 covers Duration::MAX comfortably for any realistic Pulsar config.
        let min_ms_u128 = self.min_delay.as_millis();
        let min_ms = if min_ms_u128 > u128::from(u64::MAX) {
            u64::MAX as f64
        } else {
            min_ms_u128 as f64
        };
        let scaled_ms = (min_ms * factor).clamp(0.0, u64::MAX as f64);
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
}
