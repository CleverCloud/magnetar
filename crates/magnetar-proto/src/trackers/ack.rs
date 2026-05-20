// SPDX-License-Identifier: Apache-2.0

//! Acknowledgment grouping.
//!
//! Port of `org.apache.pulsar.client.impl.PersistentAcknowledgmentsGroupingTracker`. The Java
//! tracker collects individual acks during an `ackGroupTimeMicros` window and flushes them in
//! a single `CommandAck`. Cumulative acks are flushed immediately for non-batched cases and on
//! the next tick for batched cases — we treat them like individuals here (the Java client does
//! the same when `ackGroupTimeMicros > 0`).
//!
//! # References
//!
//! - `PersistentAcknowledgmentsGroupingTracker.java:155-191` (add)
//! - `PersistentAcknowledgmentsGroupingTracker.java:265-318` (flush)
//! - `ConsumerImpl.java:528-531` (creation)

use core::time::Duration;
use std::collections::HashSet;
use std::time::Instant;

use crate::types::{ConsumerHandle, MessageId};

/// Action emitted by the ack tracker after a tick or an `add`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AckAction {
    /// Emit a `CommandAck` of `AckType::Individual` covering the given message ids.
    SendIndividualAck {
        /// The consumer the ack belongs to.
        handle: ConsumerHandle,
        /// Message ids to ack.
        message_ids: Vec<MessageId>,
    },
    /// Emit a `CommandAck` of `AckType::Cumulative` covering the given message id.
    SendCumulativeAck {
        /// The consumer the ack belongs to.
        handle: ConsumerHandle,
        /// Message id up to which to ack cumulatively.
        message_id: MessageId,
    },
}

/// Grouping tracker for one consumer.
#[derive(Debug)]
pub struct AckGroupingTracker {
    handle: ConsumerHandle,
    ack_group_time: Duration,
    pending_individuals: HashSet<MessageId>,
    pending_cumulative: Option<MessageId>,
    last_flush: Option<Instant>,
}

impl AckGroupingTracker {
    /// Construct a new tracker. `ack_group_time` of zero disables grouping (every `add` returns
    /// an immediate flush action).
    pub fn new(handle: ConsumerHandle, ack_group_time: Duration) -> Self {
        Self {
            handle,
            ack_group_time,
            pending_individuals: HashSet::new(),
            pending_cumulative: None,
            last_flush: None,
        }
    }

    /// Add an individual ack. Returns an immediate flush action if grouping is disabled.
    pub fn add_individual(&mut self, message_id: MessageId, now: Instant) -> Vec<AckAction> {
        if self.ack_group_time.is_zero() {
            return vec![AckAction::SendIndividualAck {
                handle: self.handle,
                message_ids: vec![message_id],
            }];
        }
        self.pending_individuals.insert(message_id);
        self.last_flush.get_or_insert(now);
        Vec::new()
    }

    /// Add a cumulative ack. Returns an immediate flush action if grouping is disabled.
    ///
    /// If a cumulative ack already exists, this replaces it iff the new one is "greater" (covers
    /// more).
    pub fn add_cumulative(&mut self, message_id: MessageId, now: Instant) -> Vec<AckAction> {
        if self.ack_group_time.is_zero() {
            return vec![AckAction::SendCumulativeAck {
                handle: self.handle,
                message_id,
            }];
        }
        if let Some(existing) = self.pending_cumulative {
            if cumulative_gt(message_id, existing) {
                self.pending_cumulative = Some(message_id);
            }
        } else {
            self.pending_cumulative = Some(message_id);
        }
        self.last_flush.get_or_insert(now);
        Vec::new()
    }

    /// Tick the tracker. If the ack-grouping window has elapsed, emit a flush.
    pub fn poll(&mut self, now: Instant) -> Vec<AckAction> {
        let Some(first) = self.last_flush else {
            return Vec::new();
        };
        if now.saturating_duration_since(first) < self.ack_group_time {
            return Vec::new();
        }
        self.flush()
    }

    /// Force a flush immediately (e.g. on consumer close).
    pub fn flush(&mut self) -> Vec<AckAction> {
        let mut out = Vec::new();
        if let Some(message_id) = self.pending_cumulative.take() {
            out.push(AckAction::SendCumulativeAck {
                handle: self.handle,
                message_id,
            });
        }
        if !self.pending_individuals.is_empty() {
            let mut ids: Vec<MessageId> = self.pending_individuals.drain().collect();
            // Sort to keep wire traffic deterministic across runs.
            ids.sort();
            out.push(AckAction::SendIndividualAck {
                handle: self.handle,
                message_ids: ids,
            });
        }
        self.last_flush = None;
        out
    }

    /// Returns when the next tick is due, or `None` if there's nothing pending.
    pub fn next_deadline(&self) -> Option<Instant> {
        let first = self.last_flush?;
        Some(first + self.ack_group_time)
    }
}

/// `>` over message ids on the "covers more" axis (ledger,entry,batch_index).
fn cumulative_gt(lhs: MessageId, rhs: MessageId) -> bool {
    match lhs.ledger_id.cmp(&rhs.ledger_id) {
        core::cmp::Ordering::Less => false,
        core::cmp::Ordering::Greater => true,
        core::cmp::Ordering::Equal => match lhs.entry_id.cmp(&rhs.entry_id) {
            core::cmp::Ordering::Less => false,
            core::cmp::Ordering::Greater => true,
            core::cmp::Ordering::Equal => lhs.batch_index > rhs.batch_index,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mid(ledger: u64, entry: u64) -> MessageId {
        MessageId {
            ledger_id: ledger,
            entry_id: entry,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
        }
    }

    #[test]
    fn zero_window_flushes_immediately() {
        let mut t = AckGroupingTracker::new(ConsumerHandle(1), Duration::ZERO);
        let now = Instant::now();
        let actions = t.add_individual(mid(1, 1), now);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            AckAction::SendIndividualAck {
                handle,
                message_ids,
            } => {
                assert_eq!(*handle, ConsumerHandle(1));
                assert_eq!(message_ids.len(), 1);
            }
            other => panic!("expected SendIndividualAck, got {other:?}"),
        }
    }

    #[test]
    fn groups_until_window_elapses() {
        let mut t = AckGroupingTracker::new(ConsumerHandle(2), Duration::from_millis(100));
        let t0 = Instant::now();
        assert!(t.add_individual(mid(1, 1), t0).is_empty());
        assert!(t.add_individual(mid(1, 2), t0).is_empty());
        assert!(t.poll(t0 + Duration::from_millis(50)).is_empty());
        let actions = t.poll(t0 + Duration::from_millis(101));
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            AckAction::SendIndividualAck { message_ids, .. } => {
                assert_eq!(message_ids.len(), 2);
            }
            other => panic!("expected SendIndividualAck, got {other:?}"),
        }
    }

    #[test]
    fn cumulative_keeps_the_greater() {
        let mut t = AckGroupingTracker::new(ConsumerHandle(3), Duration::from_millis(100));
        let now = Instant::now();
        let _ = t.add_cumulative(mid(1, 5), now);
        let _ = t.add_cumulative(mid(1, 3), now); // smaller, should not replace
        let _ = t.add_cumulative(mid(1, 7), now); // bigger, should replace
        let actions = t.flush();
        let cumulative_id = actions
            .iter()
            .find_map(|a| match a {
                AckAction::SendCumulativeAck { message_id, .. } => Some(*message_id),
                _ => None,
            })
            .expect("cumulative ack");
        assert_eq!(cumulative_id.entry_id, 7);
    }
}
