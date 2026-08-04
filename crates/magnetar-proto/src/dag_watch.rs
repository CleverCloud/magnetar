// SPDX-License-Identifier: Apache-2.0

//! PIP-460 scalable-topic watch-session state machine (sans-io).
//!
//! **Experimental** (PIP-460, ADR-0093). A [`DagWatchSession`] tracks the
//! current segment DAG for one scalable topic and applies broker-pushed
//! [`pb::CommandScalableTopicUpdate`] frames against it. The session is pure
//! state — no I/O, no clock — matching the
//! [ADR-0004](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md)
//! sans-io contract. The runtime engines drive it from inside their
//! connection lock and translate the returned [`DagDelta`] into
//! [`ConnectionEvent`](crate::ConnectionEvent) variants.
//!
//! # One session, no separate subscribe
//!
//! Upstream has no distinct DAG-watch handshake. `CommandScalableTopicLookup`
//! carries a client-allocated `session_id` and **is** the subscribe;
//! `CommandScalableTopicUpdate` is the reply to it *and* every subsequent
//! pushed update; `CommandScalableTopicClose` ends it. So a session here is
//! created empty by the lookup and populated by the first update, rather than
//! being seeded from a lookup response and then subscribed separately.
//!
//! # Snapshots, not deltas
//!
//! Each update carries a **whole** [`pb::ScalableTopicDag`] stamped with a
//! monotonic `epoch`; there are no split / merge event frames on the wire. The
//! session replaces its layout wholesale and derives what changed by diffing
//! the old and new segment sets, reading split / merge structure off the
//! `parent_ids` / `child_ids` edges of the incoming layout. A stale or
//! replayed frame is rejected by the epoch guard, never applied.
//!
//! # Drop-on-change
//!
//! Per ADR-0031, carried forward by ADR-0093, the surface is **observation +
//! drop-on-change**: the session records the layout, applies updates, and
//! reports what changed, but does not perform transparent segment failover.
//! The runtime closes the per-segment consumers and surfaces a
//! `DagChangedDuringConsume` event when a split / merge / removal lands while a
//! `StreamConsumer` is active. Transparent failover and in-place repartition
//! are explicit future work.

use std::collections::{BTreeMap, BTreeSet};

use crate::pb;
use crate::types::{SegmentDescriptor, SegmentId};

/// The delta produced by applying one `CommandScalableTopicUpdate` to a
/// [`DagWatchSession`]. Surfaced to the runtime so it can decide whether the
/// change is consume-affecting (split / merge / removal → drop) or benign.
///
/// The split / merge classifications are **derived** from the incoming layout's
/// DAG edges, not read from dedicated wire events — upstream has none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagDelta {
    /// Layout epoch this delta moved the session to.
    pub epoch: u64,
    /// Segments present in the new layout that the session did not hold.
    pub added: Vec<SegmentDescriptor>,
    /// Segment ids the session held that the new layout dropped.
    pub removed: Vec<SegmentId>,
    /// Splits derived from the new layout (one parent → several children).
    pub split_events: Vec<SplitEvent>,
    /// Merges derived from the new layout (several parents → one child).
    pub merge_events: Vec<MergeEvent>,
}

impl DagDelta {
    /// `true` when the delta would force a `StreamConsumer` to drop its
    /// per-segment v4 consumers (any split, merge, or removal). A delta that
    /// only *adds* fresh segments is non-consume-affecting because
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

/// A split — one parent segment fans out into several children.
///
/// Derived from the incoming layout: the children name the parent in their
/// `parent_ids`, and the parent is no longer active.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitEvent {
    /// Parent segment id that was split.
    pub parent_segment_id: SegmentId,
    /// Child segment ids produced by the split, ascending.
    pub child_segment_ids: Vec<SegmentId>,
}

/// A merge — several parent segments fold into a single child.
///
/// Derived from the incoming layout: the child names every parent in its
/// `parent_ids`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeEvent {
    /// Parent segment ids that were merged, ascending.
    pub parent_segment_ids: Vec<SegmentId>,
    /// Child segment id produced by the merge.
    pub child_segment_id: SegmentId,
}

/// Why the segment DAG changed under a live consumer (drop-on-change).
///
/// `#[non_exhaustive]` so future causes (e.g. a controller-broker hand-off)
/// can be added without breaking downstream `match`es.
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

/// Errors raised while applying a `CommandScalableTopicUpdate`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DagError {
    /// The update's layout `epoch` did not strictly advance the session's
    /// monotonic counter. Mirrors the broker's per-session ordering guarantee
    /// — a stale or replayed frame must be rejected, never applied.
    #[error("non-monotonic layout epoch: got {got} expected > {prev}")]
    NonMonotonic {
        /// The `epoch` the broker sent.
        got: u64,
        /// The highest `epoch` already applied to this session.
        prev: u64,
    },

    /// The update belonged to a different watch session than this one.
    #[error("update for watch session {got} does not match this session {expected}")]
    SessionMismatch {
        /// The `session_id` the broker sent.
        got: u64,
        /// This session's id.
        expected: u64,
    },

    /// The broker answered with an error instead of a layout.
    #[error("broker rejected the scalable-topic session (code {code}): {message}")]
    Broker {
        /// `ServerError` code the broker returned.
        code: i32,
        /// Broker-supplied message, empty when it sent none.
        message: String,
    },

    /// The update carried neither a layout nor an error — a protocol-shape
    /// surprise the session refuses rather than silently treating as empty.
    #[error("update for session {session_id} carried neither a DAG nor an error")]
    Empty {
        /// The `session_id` the broker sent.
        session_id: u64,
    },
}

/// Errors raised when opening a scalable-topic session.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScalableTopicError {
    /// The connected broker did not advertise `supports_scalable_topics` in its
    /// `CommandConnected` feature flags.
    ///
    /// This is the v4-compatibility gate. A Pulsar 4.x broker has no PIP-460
    /// surface at all, and a 5.x broker started with `scalableTopicsEnabled=false`
    /// rejects scalable-topic commands, so the client refuses to emit one rather
    /// than writing a frame the peer cannot act on.
    #[error(
        "broker does not support scalable topics (PIP-460): CommandConnected did not advertise supports_scalable_topics"
    )]
    BrokerUnsupported,
}

/// A scalable-topic watch session: monotonic layout-epoch tracking plus the
/// current DAG.
///
/// Opened empty by `CommandScalableTopicLookup` via [`Self::new`], then fed
/// each inbound `CommandScalableTopicUpdate` through [`Self::handle_update`].
#[derive(Debug, Clone)]
pub struct DagWatchSession {
    /// Client-allocated session id, echoed by the broker on every update.
    session_id: u64,
    /// Highest layout `epoch` applied so far. `None` means "no layout yet" —
    /// the first update is accepted at any epoch, including `0`.
    epoch: Option<u64>,
    /// Canonical `topic://...` identity the broker resolved the request to,
    /// once an update has carried one.
    resolved_topic_name: Option<String>,
    /// Controller-broker URL from the most recent layout, when advertised.
    controller_broker_url: Option<String>,
    /// Current DAG, keyed by segment id for O(log n) membership checks and a
    /// deterministic snapshot order.
    dag: BTreeMap<SegmentId, SegmentDescriptor>,
}

impl DagWatchSession {
    /// Open an empty session for `session_id`. The layout arrives with the
    /// first [`Self::handle_update`].
    #[must_use]
    pub fn new(session_id: u64) -> Self {
        Self {
            session_id,
            epoch: None,
            resolved_topic_name: None,
            controller_broker_url: None,
            dag: BTreeMap::new(),
        }
    }

    /// `true` once a layout has landed.
    #[must_use]
    pub fn is_resolved(&self) -> bool {
        self.epoch.is_some()
    }

    /// The canonical `topic://...` identity the broker resolved to, if known.
    #[must_use]
    pub fn resolved_topic_name(&self) -> Option<&str> {
        self.resolved_topic_name.as_deref()
    }

    /// The controller-broker URL from the most recent layout, if advertised.
    #[must_use]
    pub fn controller_broker_url(&self) -> Option<&str> {
        self.controller_broker_url.as_deref()
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

    /// Apply a `CommandScalableTopicUpdate`, replacing the layout and
    /// returning the [`DagDelta`] for the runtime to translate into events.
    ///
    /// # Errors
    ///
    /// - [`DagError::SessionMismatch`] if the update targets a different session.
    /// - [`DagError::Broker`] if the update carries a `ServerError` instead of a layout.
    /// - [`DagError::Empty`] if it carries neither.
    /// - [`DagError::NonMonotonic`] if the layout `epoch` does not strictly advance.
    ///
    /// On any error the session state is left **unchanged** — the update is
    /// validated fully before any mutation lands.
    pub fn handle_update(
        &mut self,
        upd: &pb::CommandScalableTopicUpdate,
    ) -> Result<DagDelta, DagError> {
        if upd.session_id != self.session_id {
            return Err(DagError::SessionMismatch {
                got: upd.session_id,
                expected: self.session_id,
            });
        }

        let Some(dag) = upd.dag.as_ref() else {
            // An error-bearing update takes precedence; a bodyless one is a
            // protocol-shape surprise rather than an empty layout.
            if let Some(code) = upd.error {
                return Err(DagError::Broker {
                    code,
                    message: upd.message.clone().unwrap_or_default(),
                });
            }
            return Err(DagError::Empty {
                session_id: upd.session_id,
            });
        };

        if let Some(prev) = self.epoch
            && dag.epoch <= prev
        {
            return Err(DagError::NonMonotonic {
                got: dag.epoch,
                prev,
            });
        }

        // Placement is a parallel list keyed by segment id; index it once.
        let addresses: BTreeMap<u64, &pb::SegmentBrokerAddress> = dag
            .segment_brokers
            .iter()
            .map(|a| (a.segment_id, a))
            .collect();

        let incoming: BTreeMap<SegmentId, SegmentDescriptor> = dag
            .segments
            .iter()
            .map(|info| {
                let descriptor =
                    SegmentDescriptor::from_pb(info, addresses.get(&info.segment_id).copied());
                (descriptor.segment_id, descriptor)
            })
            .collect();

        let before: BTreeSet<SegmentId> = self.dag.keys().copied().collect();
        let after: BTreeSet<SegmentId> = incoming.keys().copied().collect();

        let added: Vec<SegmentDescriptor> = after
            .difference(&before)
            .filter_map(|id| incoming.get(id).cloned())
            .collect();
        let removed: Vec<SegmentId> = before.difference(&after).copied().collect();

        let (split_events, merge_events) = derive_topology_changes(&added, &before);

        self.epoch = Some(dag.epoch);
        self.dag = incoming;
        if let Some(name) = upd.resolved_topic_name.as_ref() {
            self.resolved_topic_name = Some(name.clone());
        }
        self.controller_broker_url = dag.controller_broker_url.clone();

        Ok(DagDelta {
            epoch: dag.epoch,
            added,
            removed,
            split_events,
            merge_events,
        })
    }
}

/// Read split / merge structure off the newly-added segments' DAG edges.
///
/// A child naming **one** parent that the session previously held is a split of
/// that parent; children are grouped so a 1→N split is a single event. A child
/// naming **several** parents is a merge. Parents that the session never held
/// are ignored — the edge points outside the observed window and classifying it
/// would be a guess.
fn derive_topology_changes(
    added: &[SegmentDescriptor],
    before: &BTreeSet<SegmentId>,
) -> (Vec<SplitEvent>, Vec<MergeEvent>) {
    let mut splits: BTreeMap<SegmentId, Vec<SegmentId>> = BTreeMap::new();
    let mut merges: Vec<MergeEvent> = Vec::new();

    for child in added {
        // Sorted, not merely filtered: `MergeEvent::parent_segment_ids` is
        // documented ascending, and nothing in the .proto requires the broker
        // to send `parent_ids` in order. Without this, two engines observing
        // the same merge could produce `MergeEvent`s that compare unequal.
        let mut known_parents: Vec<SegmentId> = child
            .parent_ids
            .iter()
            .copied()
            .filter(|p| before.contains(p))
            .collect();
        known_parents.sort_unstable();
        match known_parents.len() {
            0 => {}
            1 => splits
                .entry(known_parents[0])
                .or_default()
                .push(child.segment_id),
            _ => merges.push(MergeEvent {
                parent_segment_ids: known_parents,
                child_segment_id: child.segment_id,
            }),
        }
    }

    let split_events = splits
        .into_iter()
        .map(|(parent_segment_id, child_segment_ids)| SplitEvent {
            parent_segment_id,
            child_segment_ids,
        })
        .collect();

    (split_events, merges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{KeyRange, SegmentState};

    /// Build a `SegmentInfoProto` with the given topology edges.
    fn seg_info(id: u64, start: u32, end: u32, parents: &[u64]) -> pb::SegmentInfoProto {
        info(id, start, end, parents, &[])
    }

    fn info(
        id: u64,
        start: u32,
        end: u32,
        parents: &[u64],
        children: &[u64],
    ) -> pb::SegmentInfoProto {
        pb::SegmentInfoProto {
            segment_id: id,
            hash_start: start,
            hash_end: end,
            state: pb::SegmentState::Active as i32,
            parent_ids: parents.to_vec(),
            child_ids: children.to_vec(),
            created_at_epoch: 0,
            sealed_at_epoch: None,
            created_at_ms: 0,
            sealed_at_ms: None,
            legacy_topic_name: None,
        }
    }

    fn address(id: u64) -> pb::SegmentBrokerAddress {
        pb::SegmentBrokerAddress {
            segment_id: id,
            broker_url: format!("pulsar://seg{id}:6650"),
            broker_url_tls: None,
        }
    }

    /// Build an update carrying a layout at `epoch` made of `segments`.
    fn update(
        session_id: u64,
        epoch: u64,
        segments: Vec<pb::SegmentInfoProto>,
    ) -> pb::CommandScalableTopicUpdate {
        let segment_brokers = segments.iter().map(|s| address(s.segment_id)).collect();
        pb::CommandScalableTopicUpdate {
            session_id,
            dag: Some(pb::ScalableTopicDag {
                epoch,
                segments,
                segment_brokers,
                controller_broker_url: Some("pulsar://controller:6650".to_owned()),
                controller_broker_url_tls: None,
            }),
            error: None,
            message: None,
            resolved_topic_name: Some("topic://public/default/scaled".to_owned()),
        }
    }

    fn resolved_session() -> DagWatchSession {
        let mut s = DagWatchSession::new(7);
        s.handle_update(&update(7, 1, vec![info(1, 0, 65_536, &[], &[])]))
            .expect("initial layout applies");
        s
    }

    /// Layer (a) test: the first update resolves the session, carries the
    /// canonical topic identity, and installs the layout.
    #[test]
    fn scalable_session_first_update_resolves_layout() {
        let mut s = DagWatchSession::new(7);
        assert!(!s.is_resolved());
        assert!(!s.is_resolved());

        let delta = s
            .handle_update(&update(
                7,
                4,
                vec![
                    info(1, 0, 32_768, &[], &[]),
                    info(2, 32_768, 65_536, &[], &[]),
                ],
            ))
            .expect("layout applies");

        assert!(s.is_resolved());
        assert!(s.is_resolved());
        assert_eq!(delta.epoch, 4);
        assert_eq!(delta.added.len(), 2);
        assert!(delta.removed.is_empty());
        assert!(
            !delta.is_consume_affecting(),
            "initial layout is not a change"
        );
        assert_eq!(
            s.resolved_topic_name(),
            Some("topic://public/default/scaled")
        );
        assert_eq!(s.controller_broker_url(), Some("pulsar://controller:6650"));
        // Placement is joined onto the descriptor from the parallel address list.
        assert_eq!(
            s.snapshot()[0].broker_url.as_deref(),
            Some("pulsar://seg1:6650")
        );
    }

    /// Layer (a) test: a non-advancing layout epoch is rejected and the session
    /// is left untouched.
    #[test]
    fn scalable_session_monotonic_epoch() {
        let mut s = resolved_session();
        assert!(s.is_resolved());

        let stale = update(7, 1, vec![info(9, 0, 65_536, &[], &[])]);
        let err = s.handle_update(&stale).expect_err("stale layout rejected");
        assert_eq!(err, DagError::NonMonotonic { got: 1, prev: 1 });
        // Session unchanged — segment 9 never landed, segment 1 still there.
        assert!(!s.contains(SegmentId(9)));
        assert!(s.contains(SegmentId(1)));
        assert!(s.is_resolved());
    }

    /// Layer (a) test: a split is derived from the children's `parent_ids`
    /// and the parent leaves the layout.
    #[test]
    fn scalable_session_derives_split_from_edges() {
        let mut s = resolved_session();
        let delta = s
            .handle_update(&update(
                7,
                2,
                vec![
                    info(2, 0, 32_768, &[1], &[]),
                    info(3, 32_768, 65_536, &[1], &[]),
                ],
            ))
            .expect("split layout applies");

        assert!(delta.is_consume_affecting());
        assert_eq!(delta.change_reason(), DagChangeReason::Split);
        assert_eq!(delta.removed, vec![SegmentId(1)]);
        assert_eq!(delta.split_events.len(), 1, "1 -> {{2,3}} is one split");
        assert_eq!(delta.split_events[0].parent_segment_id, SegmentId(1));
        assert_eq!(
            delta.split_events[0].child_segment_ids,
            vec![SegmentId(2), SegmentId(3)]
        );
        assert!(!s.contains(SegmentId(1)), "parent left the layout");
        assert!(s.contains(SegmentId(2)) && s.contains(SegmentId(3)));
    }

    /// Layer (a) test: the inverse — a child naming several parents is a merge.
    #[test]
    fn scalable_session_derives_merge_from_edges() {
        let mut s = DagWatchSession::new(7);
        s.handle_update(&update(
            7,
            1,
            vec![
                info(5, 0, 32_768, &[], &[]),
                info(6, 32_768, 65_536, &[], &[]),
            ],
        ))
        .expect("initial layout");

        let delta = s
            .handle_update(&update(7, 2, vec![info(7, 0, 65_536, &[5, 6], &[])]))
            .expect("merge layout applies");

        assert!(delta.is_consume_affecting());
        assert_eq!(delta.change_reason(), DagChangeReason::Merge);
        assert_eq!(delta.merge_events.len(), 1);
        assert_eq!(
            delta.merge_events[0].parent_segment_ids,
            vec![SegmentId(5), SegmentId(6)]
        );
        assert_eq!(delta.merge_events[0].child_segment_id, SegmentId(7));
        assert_eq!(delta.removed.len(), 2);
        assert_eq!(s.snapshot().len(), 1);
    }

    /// A merge whose wire `parent_ids` arrive out of order still reports them
    /// ascending, as `MergeEvent` documents. Nothing in the .proto requires the
    /// broker to sort them, and two engines observing the same merge must not
    /// produce `MergeEvent`s that compare unequal.
    #[test]
    fn scalable_session_merge_parents_are_sorted() {
        let mut s = DagWatchSession::new(7);
        s.handle_update(&update(
            7,
            1,
            vec![
                seg_info(5, 0, 32_768, &[]),
                seg_info(6, 32_768, 65_536, &[]),
            ],
        ))
        .expect("initial layout");

        // Descending on the wire.
        let delta = s
            .handle_update(&update(7, 2, vec![seg_info(7, 0, 65_536, &[6, 5])]))
            .expect("merge layout applies");

        assert_eq!(
            delta.merge_events[0].parent_segment_ids,
            vec![SegmentId(5), SegmentId(6)],
            "parent ids are reported ascending regardless of wire order"
        );
    }

    /// An update targeting a different session is rejected.
    #[test]
    fn scalable_session_mismatch_rejected() {
        let mut s = resolved_session();
        let err = s
            .handle_update(&update(1234, 2, vec![]))
            .expect_err("session mismatch rejected");
        assert_eq!(
            err,
            DagError::SessionMismatch {
                got: 1234,
                expected: 7
            }
        );
        assert!(s.contains(SegmentId(1)), "layout untouched on error");
    }

    /// An error-bearing update surfaces the broker's code and message, and does
    /// not disturb the layout.
    #[test]
    fn scalable_session_broker_error_surfaces() {
        let mut s = resolved_session();
        let err = s
            .handle_update(&pb::CommandScalableTopicUpdate {
                session_id: 7,
                dag: None,
                error: Some(pb::ServerError::TopicNotFound as i32),
                message: Some("no such topic".to_owned()),
                resolved_topic_name: None,
            })
            .expect_err("broker error surfaces");
        assert_eq!(
            err,
            DagError::Broker {
                code: pb::ServerError::TopicNotFound as i32,
                message: "no such topic".to_owned(),
            }
        );
        assert!(s.contains(SegmentId(1)), "layout untouched on error");
    }

    /// An update with neither a layout nor an error is refused rather than
    /// silently emptying the layout.
    #[test]
    fn scalable_session_empty_update_rejected() {
        let mut s = resolved_session();
        let err = s
            .handle_update(&pb::CommandScalableTopicUpdate {
                session_id: 7,
                dag: None,
                error: None,
                message: None,
                resolved_topic_name: None,
            })
            .expect_err("bodyless update rejected");
        assert_eq!(err, DagError::Empty { session_id: 7 });
        assert!(s.contains(SegmentId(1)), "layout untouched on error");
    }

    /// A layout that only adds segments is not consume-affecting.
    #[test]
    fn scalable_session_add_only_is_benign() {
        let mut s = resolved_session();
        let delta = s
            .handle_update(&update(
                7,
                2,
                vec![
                    info(1, 0, 65_536, &[], &[]),
                    info(2, 65_536, 131_072, &[], &[]),
                ],
            ))
            .expect("add applies");
        assert!(!delta.is_consume_affecting(), "pure add does not drop");
        assert_eq!(delta.change_reason(), DagChangeReason::Unknown);
        assert!(s.contains(SegmentId(1)) && s.contains(SegmentId(2)));
    }

    /// The broker's synthetic single-segment layout for an unmigrated regular
    /// topic is carried through as a legacy segment, so the v4 topic name
    /// survives to the consumer.
    #[test]
    fn scalable_session_legacy_layout_is_marked() {
        let mut s = DagWatchSession::new(7);
        let mut legacy = info(0, 0, 65_536, &[], &[]);
        legacy.legacy_topic_name = Some("persistent://public/default/plain".to_owned());
        s.handle_update(&update(7, 0, vec![legacy]))
            .expect("legacy layout applies");

        let snap = s.snapshot();
        assert_eq!(snap.len(), 1);
        assert!(snap[0].is_legacy());
        assert_eq!(
            snap[0].legacy_topic_name.as_deref(),
            Some("persistent://public/default/plain")
        );
        // Epoch 0 is a legitimate first layout — the guard only rejects
        // non-advancing epochs *after* one has landed.
        assert!(s.is_resolved(), "epoch 0 is a legitimate first layout");
        assert_eq!(snap[0].state, SegmentState::Active);
        assert_eq!(
            snap[0].key_range,
            KeyRange {
                start: 0,
                end: 65_536
            }
        );
    }
}
