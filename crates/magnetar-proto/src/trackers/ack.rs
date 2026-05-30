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
use std::collections::BTreeSet;
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
    /// `BTreeSet` (not `HashSet`) so `flush()` can drain in already-sorted
    /// order — wire-traffic determinism without an extra `Vec::sort` per
    /// flush.
    pending_individuals: BTreeSet<MessageId>,
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
            pending_individuals: BTreeSet::new(),
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
            // `BTreeSet` drains in sorted order, so the previous explicit
            // `Vec::sort` is unnecessary.
            let ids: Vec<MessageId> = std::mem::take(&mut self.pending_individuals)
                .into_iter()
                .collect();
            out.push(AckAction::SendIndividualAck {
                handle: self.handle,
                message_ids: ids,
            });
        }
        self.last_flush = None;
        out
    }

    /// Returns when the next tick is due, or `None` if there's nothing pending.
    ///
    /// Uses [`crate::time::deadline_with_clamp`] so a `Duration::MAX`
    /// `ack_group_time` cannot panic (invariant #6).
    pub fn next_deadline(&self) -> Option<Instant> {
        let first = self.last_flush?;
        Some(crate::time::deadline_with_clamp(first, self.ack_group_time))
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
            #[cfg(feature = "scalable-topics")]
            segment_id: None,
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

    /// Java `AcknowledgementsGroupingTrackerTest#testAckTracker` lines 117-149: after a
    /// cumulative ack arrives, subsequent individual acks for ids beyond the cumulative
    /// position remain pending in the same group until the window elapses.
    #[test]
    fn individual_after_cumulative_stays_grouped() {
        let mut t = AckGroupingTracker::new(ConsumerHandle(1), Duration::from_millis(100));
        let t0 = Instant::now();
        assert!(t.add_cumulative(mid(5, 5), t0).is_empty());
        assert!(t.add_individual(mid(5, 6), t0).is_empty());
        // Still within the grouping window — nothing emitted.
        assert!(t.poll(t0 + Duration::from_millis(50)).is_empty());
    }

    /// Java `testAckTracker` line 127, 140, 150: a `flush` must surface both the pending
    /// cumulative ack AND the pending individual acks. Mirrors `PersistentAcknowledgments
    /// GroupingTracker#flush` writing one `CommandAck` per ack type.
    #[test]
    fn flush_emits_cumulative_and_individuals() {
        let mut t = AckGroupingTracker::new(ConsumerHandle(1), Duration::from_millis(100));
        let now = Instant::now();
        let _ = t.add_individual(mid(5, 1), now);
        let _ = t.add_cumulative(mid(5, 5), now);
        let _ = t.add_individual(mid(5, 6), now);
        let actions = t.flush();
        let mut saw_cumulative = false;
        let mut individual_ids: Vec<MessageId> = Vec::new();
        for a in &actions {
            match a {
                AckAction::SendCumulativeAck { message_id, .. } => {
                    assert_eq!(message_id.entry_id, 5);
                    saw_cumulative = true;
                }
                AckAction::SendIndividualAck { message_ids, .. } => {
                    individual_ids.extend_from_slice(message_ids);
                }
            }
        }
        assert!(saw_cumulative, "flush must emit pending cumulative ack");
        assert_eq!(individual_ids.len(), 2);
        // Sort guarantee from `flush`: ids must come out ascending.
        let mut sorted = individual_ids.clone();
        sorted.sort();
        assert_eq!(individual_ids, sorted);
    }

    /// A freshly constructed tracker has no scheduled flush. Mirrors Java where the timer is
    /// not armed until the first ack arrives.
    #[test]
    fn next_deadline_is_none_when_empty() {
        let t = AckGroupingTracker::new(ConsumerHandle(1), Duration::from_millis(100));
        assert!(t.next_deadline().is_none());
    }

    /// After `flush` drains everything, the next `add_*` must restart the timing window — not
    /// keep firing on the previous schedule. Java `PersistentAcknowledgmentsGroupingTracker`
    /// reschedules its timer after every flush; we mirror that by clearing `last_flush`.
    #[test]
    fn flush_resets_window() {
        let mut t = AckGroupingTracker::new(ConsumerHandle(1), Duration::from_millis(100));
        let t0 = Instant::now();
        let _ = t.add_individual(mid(1, 1), t0);
        assert_eq!(t.flush().len(), 1);
        assert!(t.next_deadline().is_none());
        // After flush, the tracker behaves like a fresh one for the next add.
        let t1 = t0 + Duration::from_millis(500);
        let _ = t.add_individual(mid(1, 2), t1);
        // The deadline must be t1 + 100ms, not t0 + 100ms.
        let deadline = t.next_deadline().expect("deadline after re-add");
        assert!(deadline >= t1 + Duration::from_millis(100));
        // Polling before the new window elapses must not emit anything.
        assert!(t.poll(t1 + Duration::from_millis(50)).is_empty());
    }

    /// Java `testAckTracker` lines 113-115: `isDuplicate(msg1)` must return `true` after the
    /// individual ack is recorded but before flush, AND `isDuplicate(msg2)` must remain
    /// `false`. Without exposing `isDuplicate` directly, we verify the underlying invariant:
    /// individual acks accumulate and only flush emits them.
    #[test]
    fn individual_acks_accumulate_until_window() {
        let mut t = AckGroupingTracker::new(ConsumerHandle(1), Duration::from_millis(100));
        let t0 = Instant::now();
        for entry in 1..=4 {
            let actions = t.add_individual(mid(5, entry), t0);
            assert!(actions.is_empty(), "no immediate emission within window");
        }
        assert!(t.poll(t0 + Duration::from_millis(99)).is_empty());
        let actions = t.poll(t0 + Duration::from_millis(101));
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            AckAction::SendIndividualAck { message_ids, .. } => {
                assert_eq!(message_ids.len(), 4);
            }
            other => panic!("expected SendIndividualAck, got {other:?}"),
        }
    }

    /// A zero-window cumulative ack flushes immediately, mirroring Java
    /// `testImmediateAckingTracker` for the cumulative path: when `ackGroupTimeMicros == 0`,
    /// the tracker fires the ack synchronously rather than queueing it.
    #[test]
    fn zero_window_cumulative_flushes_immediately() {
        let mut t = AckGroupingTracker::new(ConsumerHandle(1), Duration::ZERO);
        let actions = t.add_cumulative(mid(5, 3), Instant::now());
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            AckAction::SendCumulativeAck { message_id, .. } => {
                assert_eq!(message_id.entry_id, 3);
            }
            other => panic!("expected SendCumulativeAck, got {other:?}"),
        }
    }

    /// Cumulative-ack ordering across ledgers: a cumulative ack pointing at a higher ledger
    /// supersedes one in a lower ledger even when the lower ledger's entry id is numerically
    /// larger. Mirrors `MessageIdImpl#compareTo` in Java where ledger id is the primary key.
    #[test]
    fn cumulative_uses_ledger_then_entry_ordering() {
        let mut t = AckGroupingTracker::new(ConsumerHandle(1), Duration::from_millis(100));
        let now = Instant::now();
        let _ = t.add_cumulative(mid(1, 100), now); // small ledger, big entry
        let _ = t.add_cumulative(mid(2, 1), now); // bigger ledger wins
        let _ = t.add_cumulative(mid(2, 0), now); // smaller entry within same ledger — no-op
        let actions = t.flush();
        let cumulative_id = actions
            .iter()
            .find_map(|a| match a {
                AckAction::SendCumulativeAck { message_id, .. } => Some(*message_id),
                _ => None,
            })
            .expect("cumulative ack");
        assert_eq!(cumulative_id.ledger_id, 2);
        assert_eq!(cumulative_id.entry_id, 1);
    }
}
