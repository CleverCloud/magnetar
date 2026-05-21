// SPDX-License-Identifier: Apache-2.0

//! Unacked-message tracker — bounded sliding window per consumer.
//!
//! Port of `org.apache.pulsar.client.impl.UnAckedMessageTracker`. The Java tracker maintains a
//! time-bucketed queue of in-flight message ids; when a bucket ages out without a corresponding
//! ack, the tracker forces a redelivery.
//!
//! Buckets here use a simple sliding window: every `ack_timeout` window we shift the "oldest"
//! bucket out and emit its contents as a `RedeliverExpired` action.
//!
//! # References
//!
//! - `UnAckedMessageTracker.java:120-178` (add + redelivery)

use core::time::Duration;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::trackers::nack::MultiplierRedeliveryBackoff;
use crate::types::{ConsumerHandle, MessageId};

/// Action emitted by the tracker on a tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnackedAction {
    /// Emit a `CommandRedeliverUnacknowledgedMessages` for the given message ids.
    RedeliverExpired {
        /// Consumer that owns the redelivery.
        handle: ConsumerHandle,
        /// Expired message ids.
        message_ids: Vec<MessageId>,
    },
}

/// Tracker for one consumer.
#[derive(Debug)]
pub struct UnackedMessageTracker {
    handle: ConsumerHandle,
    ack_timeout: Duration,
    /// PIP-37 `AckTimeoutRedeliveryBackoff`. When set, every call to
    /// [`Self::add_with_redelivery_count`] picks a per-message deadline from the backoff
    /// instead of the default `ack_timeout` window. Messages tracked via
    /// [`Self::add`] still use the bucket path.
    backoff: Option<MultiplierRedeliveryBackoff>,
    /// Buckets ordered oldest first. The first bucket is always "current".
    buckets: Vec<Bucket>,
    /// Reverse map for fast `remove`.
    locator: HashMap<MessageId, usize>,
    /// Per-message deadline path — populated only by
    /// [`Self::add_with_redelivery_count`] when [`Self::backoff`] is set.
    backoff_pending: HashMap<MessageId, Instant>,
}

#[derive(Debug)]
struct Bucket {
    deadline: Instant,
    ids: HashSet<MessageId>,
}

impl UnackedMessageTracker {
    /// Construct a new tracker.
    ///
    /// `ack_timeout` is the maximum time a delivered-but-not-acked message will sit in the
    /// tracker before the broker is asked to redeliver it. A value of zero disables the
    /// tracker entirely (every `add` is a no-op).
    pub fn new(handle: ConsumerHandle, ack_timeout: Duration) -> Self {
        Self {
            handle,
            ack_timeout,
            backoff: None,
            buckets: Vec::new(),
            locator: HashMap::new(),
            backoff_pending: HashMap::new(),
        }
    }

    /// Attach an [`MultiplierRedeliveryBackoff`] used by
    /// [`Self::add_with_redelivery_count`] to compute per-message ack-timeout deadlines.
    /// Mirrors Java `ConsumerBuilder#ackTimeoutRedeliveryBackoff`.
    #[must_use]
    pub fn with_backoff(mut self, backoff: MultiplierRedeliveryBackoff) -> Self {
        self.backoff = Some(backoff);
        self
    }

    /// Returns `true` if the tracker is disabled (`ack_timeout == 0`).
    pub fn is_disabled(&self) -> bool {
        self.ack_timeout.is_zero()
    }

    /// Track a delivered message.
    pub fn add(&mut self, message_id: MessageId, now: Instant) {
        if self.is_disabled() {
            return;
        }
        if self.locator.contains_key(&message_id) || self.backoff_pending.contains_key(&message_id)
        {
            return;
        }
        let bucket_index = self.ensure_current_bucket(now);
        self.buckets[bucket_index].ids.insert(message_id);
        self.locator.insert(message_id, bucket_index);
    }

    /// Track a delivered message with an explicit broker-reported redelivery count. When the
    /// tracker has an `AckTimeoutRedeliveryBackoff` attached via [`Self::with_backoff`], the
    /// effective deadline is `now + backoff.delay_for(redelivery_count)`. Otherwise this falls
    /// through to [`Self::add`].
    pub fn add_with_redelivery_count(
        &mut self,
        message_id: MessageId,
        redelivery_count: u32,
        now: Instant,
    ) {
        if self.is_disabled() {
            return;
        }
        let Some(backoff) = self.backoff else {
            self.add(message_id, now);
            return;
        };
        if self.locator.contains_key(&message_id) || self.backoff_pending.contains_key(&message_id)
        {
            return;
        }
        let deadline = now + backoff.delay_for(redelivery_count);
        self.backoff_pending.insert(message_id, deadline);
    }

    /// Stop tracking a message (positive ack arrived).
    pub fn remove(&mut self, message_id: &MessageId) {
        if let Some(idx) = self.locator.remove(message_id) {
            if let Some(bucket) = self.buckets.get_mut(idx) {
                bucket.ids.remove(message_id);
            }
        }
        self.backoff_pending.remove(message_id);
    }

    /// Tick the tracker. Buckets whose deadline has passed are evicted and their contents
    /// surfaced as `RedeliverExpired`. When backoff is set, per-message deadlines past `now`
    /// are also drained into the action.
    pub fn poll(&mut self, now: Instant) -> Vec<UnackedAction> {
        if self.is_disabled() {
            return Vec::new();
        }
        let mut out = Vec::new();
        while let Some(bucket) = self.buckets.first() {
            if bucket.deadline > now {
                break;
            }
            let bucket = self.buckets.remove(0);
            if bucket.ids.is_empty() {
                continue;
            }
            let mut ids: Vec<MessageId> = bucket.ids.into_iter().collect();
            for id in &ids {
                self.locator.remove(id);
            }
            ids.sort();
            out.push(UnackedAction::RedeliverExpired {
                handle: self.handle,
                message_ids: ids,
            });
        }
        // After removing buckets we need to rebuild the locator indices.
        if !out.is_empty() {
            self.locator.clear();
            for (idx, bucket) in self.buckets.iter().enumerate() {
                for id in &bucket.ids {
                    self.locator.insert(*id, idx);
                }
            }
        }
        if !self.backoff_pending.is_empty() {
            let mut due: Vec<MessageId> = self
                .backoff_pending
                .iter()
                .filter_map(|(id, deadline)| (*deadline <= now).then_some(*id))
                .collect();
            if !due.is_empty() {
                for id in &due {
                    self.backoff_pending.remove(id);
                }
                due.sort();
                out.push(UnackedAction::RedeliverExpired {
                    handle: self.handle,
                    message_ids: due,
                });
            }
        }
        out
    }

    /// Returns the next deadline at which `poll` could emit an action.
    pub fn next_deadline(&self) -> Option<Instant> {
        let bucket = self.buckets.first().map(|b| b.deadline);
        let backoff = self.backoff_pending.values().min().copied();
        match (bucket, backoff) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    fn ensure_current_bucket(&mut self, now: Instant) -> usize {
        // Java uses 16 buckets per `ackTimeout`; we keep the simpler model: one bucket per
        // `ackTimeout` window. The driver layer can opt into finer granularity by ticking
        // more often. The semantics ("expired buckets are reaped") are identical.
        if let Some(last) = self.buckets.last_mut() {
            if last.deadline > now {
                return self.buckets.len() - 1;
            }
        }
        let bucket = Bucket {
            deadline: now + self.ack_timeout,
            ids: HashSet::new(),
        };
        self.buckets.push(bucket);
        self.buckets.len() - 1
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
    fn disabled_tracker_is_no_op() {
        let mut t = UnackedMessageTracker::new(ConsumerHandle(1), Duration::ZERO);
        assert!(t.is_disabled());
        t.add(mid(1), Instant::now());
        assert!(t.poll(Instant::now() + Duration::from_secs(60)).is_empty());
    }

    #[test]
    fn expires_after_timeout() {
        let mut t = UnackedMessageTracker::new(ConsumerHandle(1), Duration::from_millis(100));
        let t0 = Instant::now();
        t.add(mid(1), t0);
        t.add(mid(2), t0);
        assert!(t.poll(t0 + Duration::from_millis(50)).is_empty());
        let actions = t.poll(t0 + Duration::from_millis(101));
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            UnackedAction::RedeliverExpired { message_ids, .. } => {
                assert_eq!(message_ids.len(), 2);
            }
        }
    }

    #[test]
    fn remove_after_ack_prevents_redelivery() {
        let mut t = UnackedMessageTracker::new(ConsumerHandle(1), Duration::from_millis(100));
        let t0 = Instant::now();
        t.add(mid(1), t0);
        t.remove(&mid(1));
        let actions = t.poll(t0 + Duration::from_secs(10));
        assert!(actions.is_empty());
    }

    #[test]
    fn backoff_uses_per_message_deadline() {
        // ack_timeout = 1s default, but the backoff schedules redelivery at 100ms for
        // redelivery_count = 0 and 200ms for redelivery_count = 1.
        let backoff = MultiplierRedeliveryBackoff {
            min_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        };
        let mut t = UnackedMessageTracker::new(ConsumerHandle(1), Duration::from_secs(1))
            .with_backoff(backoff);
        let t0 = Instant::now();
        t.add_with_redelivery_count(mid(1), 0, t0);
        t.add_with_redelivery_count(mid(2), 1, t0);
        // 99ms: nothing due.
        assert!(t.poll(t0 + Duration::from_millis(99)).is_empty());
        // 110ms: mid(1) is due (100ms deadline), mid(2) is not yet (200ms deadline).
        let actions = t.poll(t0 + Duration::from_millis(110));
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            UnackedAction::RedeliverExpired { message_ids, .. } => {
                assert_eq!(message_ids.len(), 1);
                assert_eq!(message_ids[0].entry_id, 1);
            }
        }
        // 210ms: mid(2) is due now.
        let actions = t.poll(t0 + Duration::from_millis(210));
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            UnackedAction::RedeliverExpired { message_ids, .. } => {
                assert_eq!(message_ids.len(), 1);
                assert_eq!(message_ids[0].entry_id, 2);
            }
        }
    }

    #[test]
    fn backoff_remove_cancels_redelivery() {
        let backoff = MultiplierRedeliveryBackoff {
            min_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        };
        let mut t = UnackedMessageTracker::new(ConsumerHandle(1), Duration::from_secs(1))
            .with_backoff(backoff);
        let t0 = Instant::now();
        t.add_with_redelivery_count(mid(5), 0, t0);
        t.remove(&mid(5));
        assert!(t.poll(t0 + Duration::from_secs(10)).is_empty());
    }

    #[test]
    fn add_without_backoff_falls_through_to_bucket() {
        // Same call but without a backoff set — should land in the bucket path and fire on the
        // ack_timeout boundary.
        let mut t = UnackedMessageTracker::new(ConsumerHandle(1), Duration::from_millis(100));
        let t0 = Instant::now();
        t.add_with_redelivery_count(mid(1), 7, t0);
        assert!(t.poll(t0 + Duration::from_millis(50)).is_empty());
        let actions = t.poll(t0 + Duration::from_millis(101));
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn next_deadline_picks_earliest_across_paths() {
        let backoff = MultiplierRedeliveryBackoff {
            min_delay: Duration::from_millis(20),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        };
        let mut t = UnackedMessageTracker::new(ConsumerHandle(1), Duration::from_secs(1))
            .with_backoff(backoff);
        let t0 = Instant::now();
        // Bucket path message — deadline t0 + 1s.
        t.add(mid(1), t0);
        // Backoff path message — deadline t0 + 20ms (well before bucket deadline).
        t.add_with_redelivery_count(mid(2), 0, t0);
        let next = t.next_deadline().unwrap();
        assert!(next <= t0 + Duration::from_millis(20));
    }
}
