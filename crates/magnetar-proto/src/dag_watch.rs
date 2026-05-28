// SPDX-License-Identifier: Apache-2.0

//! PIP-460 segment-DAG-watch session state machine (sans-io).
//!
//! **Experimental** (PIP-460, ADR-0031). A [`DagWatchSession`] tracks the
//! current segment DAG for one scalable topic and applies broker-pushed
//! [`pb::scalable_topics::CommandSegmentDagUpdate`] frames against it. The
//! session is pure state — no I/O, no clock — matching the
//! [ADR-0004](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md)
//! sans-io contract. The runtime engines drive it from inside their
//! connection lock and translate the returned [`DagDelta`] into
//! [`ConnectionEvent`](crate::ConnectionEvent) variants.
//!
//! # Drop-on-change (v0.2.0 scope)
//!
//! Per ADR-0031 the v0.2.0 surface is **observation + drop-on-change**: the
//! session records the DAG, applies updates, and reports what changed, but
//! does not perform transparent segment failover. The runtime closes the
//! per-segment consumers and surfaces a `DagChangedDuringConsume` event when
//! a split / merge / removal lands while a `StreamConsumer` is active.
//! Transparent failover and in-place repartition are explicit v0.3.0+ work.

use std::collections::BTreeMap;

use crate::pb;
use crate::types::{SegmentDescriptor, SegmentId};

/// The delta produced by applying one `CommandSegmentDagUpdate` to a
/// [`DagWatchSession`]. Surfaced to the runtime so it can decide whether the
/// change is consume-affecting (split / merge / removal → drop) or benign.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagDelta {
    /// Segments added to the DAG by this update.
    pub added: Vec<SegmentDescriptor>,
    /// Segment ids removed from the DAG by this update.
    pub removed: Vec<SegmentId>,
    /// Split events carried by this update (parent → children).
    pub split_events: Vec<SplitEvent>,
    /// Merge events carried by this update (parents → child).
    pub merge_events: Vec<MergeEvent>,
}

impl DagDelta {
    /// `true` when the delta would force a `StreamConsumer` to drop its
    /// per-segment v4 consumers (any split, merge, or removal). A delta that
    /// only *adds* fresh segments is non-consume-affecting in v0.2.0 because
    /// the StreamConsumer attaches the new segment lazily.
    #[must_use]
    pub fn is_consume_affecting(&self) -> bool {
        !self.split_events.is_empty() || !self.merge_events.is_empty() || !self.removed.is_empty()
    }

    /// The reason classification surfaced alongside a drop. Split takes
    /// precedence over merge, which takes precedence over a bare removal.
    #[must_use]
    pub fn change_reason(&self) -> DagChangeReason {
        if !self.split_events.is_empty() {
            DagChangeReason::Split
        } else if !self.merge_events.is_empty() {
            DagChangeReason::Merge
        } else if !self.removed.is_empty() {
            DagChangeReason::SegmentRemoved
        } else {
            DagChangeReason::Unknown
        }
    }
}

/// A split event — a parent segment fans out into children (proposal §1.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitEvent {
    /// Parent segment id being split.
    pub parent_segment_id: SegmentId,
    /// Child segment ids produced by the split.
    pub child_segment_ids: Vec<SegmentId>,
    /// Entry id at which the split takes effect.
    pub split_at_entry: u64,
}

/// A merge event — parents fold into a single child (proposal §1.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeEvent {
    /// Parent segment ids being merged.
    pub parent_segment_ids: Vec<SegmentId>,
    /// Child segment id produced by the merge.
    pub child_segment_id: SegmentId,
    /// Entry id at which the merge takes effect.
    pub merge_at_entry: u64,
}

/// Why the segment DAG changed under a live consumer (v0.2.0 drop-on-change).
///
/// `#[non_exhaustive]` so future causes (e.g. a controller-broker hand-off in
/// v0.3.0+) can be added without breaking downstream `match`es.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DagChangeReason {
    /// A segment split into children.
    Split,
    /// Segments merged into a child.
    Merge,
    /// A segment was removed without a split / merge classification.
    SegmentRemoved,
    /// The cause could not be classified (defensive default).
    Unknown,
}

/// Errors raised while applying a `CommandSegmentDagUpdate`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DagError {
    /// The update's `update_seq` did not strictly advance the session's
    /// monotonic counter. Mirrors the broker's per-session ordering guarantee
    /// — a stale or replayed frame must be rejected, never applied.
    #[error("non-monotonic update_seq: got {got} expected > {prev}")]
    NonMonotonic {
        /// The `update_seq` the broker sent.
        got: u64,
        /// The highest `update_seq` already applied to this session.
        prev: u64,
    },

    /// The update referenced a segment id (in `removed`, a split parent, or a
    /// merge parent) that the session does not currently track.
    #[error("update for unknown segment_id {0}")]
    UnknownSegment(SegmentId),

    /// The update belonged to a different watch session than this one.
    #[error("update for watch session {got} does not match this session {expected}")]
    SessionMismatch {
        /// The `watch_session_id` the broker sent.
        got: u64,
        /// This session's id.
        expected: u64,
    },
}

/// A DAG-watch session: monotonic `update_seq` tracking + the current DAG.
///
/// Construct from the lookup snapshot via [`Self::new`], then feed each
/// inbound `CommandSegmentDagUpdate` to [`Self::handle_update`].
#[derive(Debug, Clone)]
pub struct DagWatchSession {
    /// Client-allocated watch session id (echoed by the broker).
    watch_session_id: u64,
    /// Token from the lookup response, carried into the subscribe frame.
    lookup_token: u64,
    /// Highest `update_seq` applied so far. `0` means "no update yet"; the
    /// first update must carry `update_seq >= 1`.
    last_update_seq: u64,
    /// Current DAG, keyed by segment id for O(log n) membership checks and a
    /// deterministic snapshot order.
    dag: BTreeMap<SegmentId, SegmentDescriptor>,
}

impl DagWatchSession {
    /// Open a session from the lookup-response DAG snapshot.
    #[must_use]
    pub fn new(
        watch_session_id: u64,
        lookup_token: u64,
        initial_dag: Vec<SegmentDescriptor>,
    ) -> Self {
        let dag = initial_dag.into_iter().map(|d| (d.segment_id, d)).collect();
        Self {
            watch_session_id,
            lookup_token,
            last_update_seq: 0,
            dag,
        }
    }

    /// This session's watch id.
    #[must_use]
    pub fn watch_session_id(&self) -> u64 {
        self.watch_session_id
    }

    /// The lookup token threaded into the subscribe frame.
    #[must_use]
    pub fn lookup_token(&self) -> u64 {
        self.lookup_token
    }

    /// The highest `update_seq` applied so far (`0` before any update).
    #[must_use]
    pub fn last_update_seq(&self) -> u64 {
        self.last_update_seq
    }

    /// Snapshot of the current DAG, ordered by segment id.
    #[must_use]
    pub fn snapshot(&self) -> Vec<SegmentDescriptor> {
        self.dag.values().cloned().collect()
    }

    /// `true` when `segment_id` is currently part of the DAG.
    #[must_use]
    pub fn contains(&self, segment_id: SegmentId) -> bool {
        self.dag.contains_key(&segment_id)
    }

    /// Apply a `CommandSegmentDagUpdate` to the session, mutating the DAG and
    /// returning the [`DagDelta`] for the runtime to translate into events.
    ///
    /// # Errors
    ///
    /// - [`DagError::SessionMismatch`] if the update targets a different watch session.
    /// - [`DagError::NonMonotonic`] if `update_seq` does not strictly advance.
    /// - [`DagError::UnknownSegment`] if a removed / split-parent / merge-parent id is not in the
    ///   DAG.
    ///
    /// On any error the session state is left **unchanged** (the update is
    /// validated fully before any mutation lands).
    pub fn handle_update(
        &mut self,
        upd: &pb::scalable_topics::CommandSegmentDagUpdate,
    ) -> Result<DagDelta, DagError> {
        if upd.watch_session_id != self.watch_session_id {
            return Err(DagError::SessionMismatch {
                got: upd.watch_session_id,
                expected: self.watch_session_id,
            });
        }
        if upd.update_seq <= self.last_update_seq {
            return Err(DagError::NonMonotonic {
                got: upd.update_seq,
                prev: self.last_update_seq,
            });
        }

        // Validate every referenced segment before mutating so a partial
        // update never corrupts the DAG.
        for &removed in &upd.removed {
            let sid = SegmentId(removed);
            if !self.dag.contains_key(&sid) {
                return Err(DagError::UnknownSegment(sid));
            }
        }
        for split in &upd.split_events {
            let sid = SegmentId(split.parent_segment_id);
            if !self.dag.contains_key(&sid) {
                return Err(DagError::UnknownSegment(sid));
            }
        }
        for merge in &upd.merge_events {
            for &parent in &merge.parent_segment_ids {
                let sid = SegmentId(parent);
                if !self.dag.contains_key(&sid) {
                    return Err(DagError::UnknownSegment(sid));
                }
            }
        }

        // All references valid — apply. Order: additions first (so split /
        // merge children that arrive in `added` are present), then removals,
        // then the parent segments named by split / merge events drop out.
        let added: Vec<SegmentDescriptor> =
            upd.added.iter().map(SegmentDescriptor::from_pb).collect();
        for d in &added {
            self.dag.insert(d.segment_id, d.clone());
        }

        let mut removed: Vec<SegmentId> = Vec::new();
        for &r in &upd.removed {
            let sid = SegmentId(r);
            if self.dag.remove(&sid).is_some() {
                removed.push(sid);
            }
        }

        let split_events: Vec<SplitEvent> = upd
            .split_events
            .iter()
            .map(|s| SplitEvent {
                parent_segment_id: SegmentId(s.parent_segment_id),
                child_segment_ids: s.child_segment_ids.iter().copied().map(SegmentId).collect(),
                split_at_entry: s.split_at_entry,
            })
            .collect();
        for s in &split_events {
            // The parent is replaced by its children; drop it from the DAG.
            if self.dag.remove(&s.parent_segment_id).is_some()
                && !removed.contains(&s.parent_segment_id)
            {
                removed.push(s.parent_segment_id);
            }
        }

        let merge_events: Vec<MergeEvent> = upd
            .merge_events
            .iter()
            .map(|m| MergeEvent {
                parent_segment_ids: m
                    .parent_segment_ids
                    .iter()
                    .copied()
                    .map(SegmentId)
                    .collect(),
                child_segment_id: SegmentId(m.child_segment_id),
                merge_at_entry: m.merge_at_entry,
            })
            .collect();
        for m in &merge_events {
            for parent in &m.parent_segment_ids {
                if self.dag.remove(parent).is_some() && !removed.contains(parent) {
                    removed.push(*parent);
                }
            }
        }

        self.last_update_seq = upd.update_seq;

        Ok(DagDelta {
            added,
            removed,
            split_events,
            merge_events,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::scalable_topics as st;
    use crate::types::{KeyRange, SegmentState};

    fn seg(id: u64, start: u32, end: u32) -> SegmentDescriptor {
        SegmentDescriptor {
            segment_id: SegmentId(id),
            key_range: KeyRange { start, end },
            broker_url: format!("pulsar://seg{id}:6650"),
            state: SegmentState::Active,
        }
    }

    fn session_with(initial: Vec<SegmentDescriptor>) -> DagWatchSession {
        DagWatchSession::new(99, 42, initial)
    }

    /// Layer (a) test: a non-monotonic `update_seq` is rejected and the
    /// session is left untouched.
    #[test]
    fn dag_watch_session_monotonic_update_seq() {
        let mut s = session_with(vec![seg(1, 0, 65_536)]);
        // First update advances 0 -> 5.
        let upd = st::CommandSegmentDagUpdate {
            watch_session_id: 99,
            update_seq: 5,
            added: vec![seg(2, 0, 32_768).to_pb()],
            removed: vec![],
            split_events: vec![],
            merge_events: vec![],
        };
        assert!(s.handle_update(&upd).is_ok());
        assert_eq!(s.last_update_seq(), 5);

        // A replayed / stale update (seq <= 5) is rejected.
        let stale = st::CommandSegmentDagUpdate {
            watch_session_id: 99,
            update_seq: 5,
            added: vec![seg(3, 0, 16_384).to_pb()],
            removed: vec![],
            split_events: vec![],
            merge_events: vec![],
        };
        let err = s.handle_update(&stale).expect_err("stale update rejected");
        assert_eq!(err, DagError::NonMonotonic { got: 5, prev: 5 });
        // Session unchanged — segment 3 never landed.
        assert!(!s.contains(SegmentId(3)));
        assert_eq!(s.last_update_seq(), 5);
    }

    /// Layer (a) test: an initial DAG plus a split event removes the parent
    /// and (via `added`) installs the two children.
    #[test]
    fn dag_watch_session_apply_split() {
        let mut s = session_with(vec![seg(1, 0, 65_536)]);
        let upd = st::CommandSegmentDagUpdate {
            watch_session_id: 99,
            update_seq: 1,
            added: vec![seg(2, 0, 32_768).to_pb(), seg(3, 32_768, 65_536).to_pb()],
            removed: vec![],
            split_events: vec![st::SplitEvent {
                parent_segment_id: 1,
                child_segment_ids: vec![2, 3],
                split_at_entry: 1000,
            }],
            merge_events: vec![],
        };
        let delta = s.handle_update(&upd).expect("split applies");
        assert!(delta.is_consume_affecting());
        assert_eq!(delta.change_reason(), DagChangeReason::Split);
        // Parent gone, two children present.
        assert!(!s.contains(SegmentId(1)), "parent removed");
        assert!(s.contains(SegmentId(2)));
        assert!(s.contains(SegmentId(3)));
        let snap = s.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(delta.removed, vec![SegmentId(1)]);
    }

    /// Layer (a) test: the inverse — a merge event removes the two parents
    /// and installs the single child.
    #[test]
    fn dag_watch_session_apply_merge() {
        let mut s = session_with(vec![seg(5, 0, 32_768), seg(6, 32_768, 65_536)]);
        let upd = st::CommandSegmentDagUpdate {
            watch_session_id: 99,
            update_seq: 1,
            added: vec![seg(7, 0, 65_536).to_pb()],
            removed: vec![],
            split_events: vec![],
            merge_events: vec![st::MergeEvent {
                parent_segment_ids: vec![5, 6],
                child_segment_id: 7,
                merge_at_entry: 2000,
            }],
        };
        let delta = s.handle_update(&upd).expect("merge applies");
        assert!(delta.is_consume_affecting());
        assert_eq!(delta.change_reason(), DagChangeReason::Merge);
        assert!(!s.contains(SegmentId(5)), "parent 5 removed");
        assert!(!s.contains(SegmentId(6)), "parent 6 removed");
        assert!(s.contains(SegmentId(7)), "child present");
        assert_eq!(s.snapshot().len(), 1);
        assert_eq!(delta.removed.len(), 2);
    }

    /// A split / merge / removal referencing an unknown segment is rejected
    /// before any mutation lands.
    #[test]
    fn dag_watch_session_unknown_segment_rejected() {
        let mut s = session_with(vec![seg(1, 0, 65_536)]);
        let upd = st::CommandSegmentDagUpdate {
            watch_session_id: 99,
            update_seq: 1,
            added: vec![],
            removed: vec![404],
            split_events: vec![],
            merge_events: vec![],
        };
        let err = s.handle_update(&upd).expect_err("unknown removal rejected");
        assert_eq!(err, DagError::UnknownSegment(SegmentId(404)));
        assert!(s.contains(SegmentId(1)), "DAG untouched on error");
    }

    /// An update targeting a different watch session is rejected.
    #[test]
    fn dag_watch_session_session_mismatch_rejected() {
        let mut s = session_with(vec![seg(1, 0, 65_536)]);
        let upd = st::CommandSegmentDagUpdate {
            watch_session_id: 1234,
            update_seq: 1,
            added: vec![],
            removed: vec![],
            split_events: vec![],
            merge_events: vec![],
        };
        let err = s
            .handle_update(&upd)
            .expect_err("session mismatch rejected");
        assert_eq!(
            err,
            DagError::SessionMismatch {
                got: 1234,
                expected: 99
            }
        );
    }

    /// A bare `added`-only update is not consume-affecting (no drop).
    #[test]
    fn dag_watch_session_add_only_is_benign() {
        let mut s = session_with(vec![seg(1, 0, 65_536)]);
        let upd = st::CommandSegmentDagUpdate {
            watch_session_id: 99,
            update_seq: 1,
            added: vec![seg(2, 65_536, 131_072).to_pb()],
            removed: vec![],
            split_events: vec![],
            merge_events: vec![],
        };
        let delta = s.handle_update(&upd).expect("add applies");
        assert!(!delta.is_consume_affecting(), "pure add does not drop");
        assert_eq!(delta.change_reason(), DagChangeReason::Unknown);
        assert!(s.contains(SegmentId(1)) && s.contains(SegmentId(2)));
    }
}
