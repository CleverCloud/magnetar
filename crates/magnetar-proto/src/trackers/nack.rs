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
}
