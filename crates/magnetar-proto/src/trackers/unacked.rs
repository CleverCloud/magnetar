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
use std::collections::HashSet;
use std::time::Instant;

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
    /// Buckets ordered oldest first. The first bucket is always "current".
    buckets: Vec<Bucket>,
    /// Reverse map for fast `remove`.
    locator: std::collections::HashMap<MessageId, usize>,
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
            buckets: Vec::new(),
            locator: std::collections::HashMap::new(),
        }
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
        if self.locator.contains_key(&message_id) {
            return;
        }
        let bucket_index = self.ensure_current_bucket(now);
        self.buckets[bucket_index].ids.insert(message_id);
        self.locator.insert(message_id, bucket_index);
    }

    /// Stop tracking a message (positive ack arrived).
    pub fn remove(&mut self, message_id: &MessageId) {
        if let Some(idx) = self.locator.remove(message_id) {
            if let Some(bucket) = self.buckets.get_mut(idx) {
                bucket.ids.remove(message_id);
            }
        }
    }

    /// Tick the tracker. Buckets whose deadline has passed are evicted and their contents
    /// surfaced as `RedeliverExpired`.
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
        out
    }

    /// Returns the next deadline at which `poll` could emit an action.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.buckets.first().map(|b| b.deadline)
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
}
