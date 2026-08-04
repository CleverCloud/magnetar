// SPDX-License-Identifier: Apache-2.0

//! PIP-460 scalable-consumer registration and namespace-watch state machines
//! (sans-io).
//!
//! **Experimental** (PIP-460, ADR-0093). Two sessions live here, both pure
//! state — no I/O, no clock — matching the
//! [ADR-0004](https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md)
//! sans-io contract:
//!
//! - [`ScalableConsumerSession`] — registration with the controller leader.
//!   `CommandScalableTopicSubscribe` → `…SubscribeResponse` carries the initial
//!   [`ConsumerAssignment`], and `…AssignmentUpdate` pushes a fresh one after every rebalance. This
//!   is what tells a consumer **which** `segment://` topics it owns; without it a client can read a
//!   topic's layout but has no share of it.
//! - [`ScalableTopicsWatch`] — the namespace-level membership watch. `CommandWatchScalableTopics`
//!   opens it and `…Update` delivers either a full snapshot or an incremental diff of the matching
//!   topic set.
//!
//! # Staleness
//!
//! Both sessions carry the layout epoch they were computed from.
//! An assignment whose `layout_epoch` does not advance is **rejected**, not
//! applied: the broker recomputes assignments per layout, so an out-of-order
//! push would hand the consumer segments that no longer exist. The
//! [`DagWatchSession`](crate::dag_watch::DagWatchSession) epoch guard and this
//! one are the same rule applied to the two halves of the protocol.

use std::collections::BTreeSet;

use crate::pb;
use crate::types::{KeyRange, SegmentId};

/// Which kind of scalable consumer registers with the controller leader.
///
/// A `QueueConsumer` never registers — it attaches directly to every active and
/// sealed segment topic — so it has no representation here, mirroring upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum ScalableConsumerType {
    /// Ordered consumer: the controller assigns it a disjoint share of the
    /// active segments and rebalances as peers join and leave.
    #[default]
    Stream,
    /// External checkpoint consumer.
    Checkpoint,
}

impl ScalableConsumerType {
    /// Convert to the wire enum integer.
    #[must_use]
    pub fn to_pb_i32(self) -> i32 {
        match self {
            Self::Stream => pb::ScalableConsumerType::Stream as i32,
            Self::Checkpoint => pb::ScalableConsumerType::Checkpoint as i32,
        }
    }

    /// Convert from the wire enum integer, saturating an unrecognised value to
    /// [`Self::Stream`] (forward-compatibility with a future broker enum).
    #[must_use]
    pub fn from_pb_i32(value: i32) -> Self {
        match pb::ScalableConsumerType::try_from(value) {
            Ok(pb::ScalableConsumerType::Checkpoint) => Self::Checkpoint,
            Ok(pb::ScalableConsumerType::Stream) | Err(_) => Self::Stream,
        }
    }
}

/// One segment assigned to this consumer, with the `segment://` topic to attach
/// to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignedSegment {
    /// Segment id within the topic's DAG.
    pub segment_id: SegmentId,
    /// Hash key range this segment serves.
    pub key_range: KeyRange,
    /// Fully-qualified `segment://...` topic the consumer attaches to.
    pub segment_topic: String,
}

impl AssignedSegment {
    /// Decode from the wire message.
    #[must_use]
    pub fn from_pb(pb: &pb::ScalableAssignedSegment) -> Self {
        Self {
            segment_id: SegmentId(pb.segment_id),
            key_range: KeyRange {
                start: pb.hash_start,
                end: pb.hash_end,
            },
            segment_topic: pb.segment_topic.clone(),
        }
    }

    /// Encode into the wire message.
    #[must_use]
    pub fn to_pb(&self) -> pb::ScalableAssignedSegment {
        pb::ScalableAssignedSegment {
            segment_id: self.segment_id.0,
            hash_start: self.key_range.start,
            hash_end: self.key_range.end,
            segment_topic: self.segment_topic.clone(),
        }
    }
}

/// The set of segments this consumer owns, stamped with the layout epoch it was
/// computed from.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConsumerAssignment {
    /// Layout epoch this assignment was computed against.
    pub layout_epoch: u64,
    /// Segments assigned to this consumer, ordered by segment id.
    pub segments: Vec<AssignedSegment>,
}

impl ConsumerAssignment {
    /// Decode from the wire message, ordering segments by id so two engines
    /// observing the same assignment compare equal regardless of wire order.
    #[must_use]
    pub fn from_pb(pb: &pb::ScalableConsumerAssignment) -> Self {
        let mut segments: Vec<AssignedSegment> =
            pb.segments.iter().map(AssignedSegment::from_pb).collect();
        segments.sort_by_key(|s| s.segment_id);
        Self {
            layout_epoch: pb.layout_epoch,
            segments,
        }
    }

    /// Encode into the wire message.
    #[must_use]
    pub fn to_pb(&self) -> pb::ScalableConsumerAssignment {
        pb::ScalableConsumerAssignment {
            layout_epoch: self.layout_epoch,
            segments: self.segments.iter().map(AssignedSegment::to_pb).collect(),
        }
    }

    /// The `segment://...` topics this assignment covers, in segment-id order.
    #[must_use]
    pub fn segment_topics(&self) -> Vec<&str> {
        self.segments
            .iter()
            .map(|s| s.segment_topic.as_str())
            .collect()
    }
}

/// What changed between two consecutive assignments.
///
/// The consumer attaches to `gained` and detaches from `lost`; a rebalance that
/// changes neither (same segments, newer epoch) is reported with both empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentDelta {
    /// Layout epoch the assignment moved to.
    pub layout_epoch: u64,
    /// Segments newly assigned to this consumer.
    pub gained: Vec<AssignedSegment>,
    /// Segment ids no longer assigned to this consumer.
    pub lost: Vec<SegmentId>,
}

impl AssignmentDelta {
    /// `true` when the consumer must attach to or detach from something.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.gained.is_empty() && self.lost.is_empty()
    }
}

/// Errors raised while applying a subscribe response or assignment update.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AssignmentError {
    /// The update targeted a different consumer than this session.
    #[error("assignment for consumer {got} does not match this session {expected}")]
    ConsumerMismatch {
        /// The `consumer_id` the broker sent.
        got: u64,
        /// This session's consumer id.
        expected: u64,
    },

    /// The assignment's `layout_epoch` did not strictly advance. The broker
    /// recomputes assignments per layout, so a stale push would hand the
    /// consumer segments that no longer exist.
    #[error("non-monotonic assignment layout epoch: got {got} expected > {prev}")]
    StaleEpoch {
        /// The `layout_epoch` the broker sent.
        got: u64,
        /// The highest `layout_epoch` already applied.
        prev: u64,
    },

    /// The broker rejected the registration.
    #[error("broker rejected the scalable subscribe (code {code}): {message}")]
    Broker {
        /// `ServerError` code the broker returned.
        code: i32,
        /// Broker-supplied message, empty when it sent none.
        message: String,
    },

    /// A success response carried no assignment.
    #[error(
        "subscribe response for request {request_id} carried neither an assignment nor an error"
    )]
    Empty {
        /// The `request_id` the broker echoed.
        request_id: u64,
    },
}

/// A consumer's registration with the scalable-topic controller leader.
///
/// Created by `Connection::scalable_topic_subscribe`, resolved by the
/// `CommandScalableTopicSubscribeResponse`, then fed each
/// `CommandScalableTopicAssignmentUpdate`.
#[derive(Debug, Clone)]
pub struct ScalableConsumerSession {
    consumer_id: u64,
    topic: String,
    subscription: String,
    consumer_name: String,
    consumer_type: ScalableConsumerType,
    /// `None` until the subscribe response lands.
    assignment: Option<ConsumerAssignment>,
}

impl ScalableConsumerSession {
    /// Open an unresolved session for `consumer_id`.
    #[must_use]
    pub fn new(
        consumer_id: u64,
        topic: String,
        subscription: String,
        consumer_name: String,
        consumer_type: ScalableConsumerType,
    ) -> Self {
        Self {
            consumer_id,
            topic,
            subscription,
            consumer_name,
            consumer_type,
            assignment: None,
        }
    }

    /// The topic this consumer registered against.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The subscription name.
    #[must_use]
    pub fn subscription(&self) -> &str {
        &self.subscription
    }

    /// The consumer name.
    #[must_use]
    pub fn consumer_name(&self) -> &str {
        &self.consumer_name
    }

    /// Which kind of consumer this registered as.
    #[must_use]
    pub fn consumer_type(&self) -> ScalableConsumerType {
        self.consumer_type
    }

    /// The current assignment, or `None` before the subscribe response lands.
    #[must_use]
    pub fn assignment(&self) -> Option<&ConsumerAssignment> {
        self.assignment.as_ref()
    }

    /// `true` once the broker has answered the subscribe.
    #[must_use]
    pub fn is_registered(&self) -> bool {
        self.assignment.is_some()
    }

    /// Apply the `CommandScalableTopicSubscribeResponse` that resolves this
    /// registration, returning the initial assignment.
    ///
    /// # Errors
    ///
    /// - [`AssignmentError::Broker`] when the broker rejected the registration.
    /// - [`AssignmentError::Empty`] when a success response carried no assignment.
    pub fn handle_subscribe_response(
        &mut self,
        resp: &pb::CommandScalableTopicSubscribeResponse,
    ) -> Result<ConsumerAssignment, AssignmentError> {
        let Some(assignment) = resp.assignment.as_ref() else {
            if let Some(code) = resp.error {
                return Err(AssignmentError::Broker {
                    code,
                    message: resp.message.clone().unwrap_or_default(),
                });
            }
            return Err(AssignmentError::Empty {
                request_id: resp.request_id,
            });
        };
        let assignment = ConsumerAssignment::from_pb(assignment);
        self.assignment = Some(assignment.clone());
        Ok(assignment)
    }

    /// Apply a pushed `CommandScalableTopicAssignmentUpdate`, returning what the
    /// consumer must attach to and detach from.
    ///
    /// # Errors
    ///
    /// - [`AssignmentError::ConsumerMismatch`] when the update targets another consumer.
    /// - [`AssignmentError::StaleEpoch`] when `layout_epoch` does not strictly advance.
    ///
    /// On either error the session is left **unchanged**.
    pub fn handle_assignment_update(
        &mut self,
        upd: &pb::CommandScalableTopicAssignmentUpdate,
    ) -> Result<AssignmentDelta, AssignmentError> {
        if upd.consumer_id != self.consumer_id {
            return Err(AssignmentError::ConsumerMismatch {
                got: upd.consumer_id,
                expected: self.consumer_id,
            });
        }
        let incoming = ConsumerAssignment::from_pb(&upd.assignment);
        if let Some(prev) = self.assignment.as_ref()
            && incoming.layout_epoch <= prev.layout_epoch
        {
            return Err(AssignmentError::StaleEpoch {
                got: incoming.layout_epoch,
                prev: prev.layout_epoch,
            });
        }

        let before: BTreeSet<SegmentId> = self
            .assignment
            .as_ref()
            .map(|a| a.segments.iter().map(|s| s.segment_id).collect())
            .unwrap_or_default();
        let after: BTreeSet<SegmentId> = incoming.segments.iter().map(|s| s.segment_id).collect();

        let gained: Vec<AssignedSegment> = incoming
            .segments
            .iter()
            .filter(|s| !before.contains(&s.segment_id))
            .cloned()
            .collect();
        let lost: Vec<SegmentId> = before.difference(&after).copied().collect();
        let layout_epoch = incoming.layout_epoch;
        self.assignment = Some(incoming);

        Ok(AssignmentDelta {
            layout_epoch,
            gained,
            lost,
        })
    }
}

/// What a namespace watch update changed in the matching topic set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicsChange {
    /// The broker replaced the whole set (initial subscribe or reconnect resync).
    Snapshot {
        /// The full matching set, sorted.
        topics: Vec<String>,
    },
    /// The broker applied an incremental membership change.
    Diff {
        /// Topics that entered the set, sorted.
        added: Vec<String>,
        /// Topics that left the set, sorted.
        removed: Vec<String>,
    },
}

/// Errors raised while applying a namespace-watch update.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TopicsWatchError {
    /// The update targeted a different watch than this one.
    #[error("watch update for {got} does not match this watch {expected}")]
    WatchMismatch {
        /// The `watch_id` the broker sent.
        got: u64,
        /// This watch's id.
        expected: u64,
    },

    /// The broker rejected the watch.
    #[error("broker rejected the scalable-topics watch (code {code}): {message}")]
    Broker {
        /// `ServerError` code the broker returned.
        code: i32,
        /// Broker-supplied message, empty when it sent none.
        message: String,
    },

    /// The update carried neither a snapshot nor a diff nor an error.
    #[error("watch update for {watch_id} carried no event")]
    Empty {
        /// The `watch_id` the broker sent.
        watch_id: u64,
    },
}

/// A namespace-level watch over the set of scalable topics matching a filter.
#[derive(Debug, Clone)]
pub struct ScalableTopicsWatch {
    watch_id: u64,
    namespace: String,
    /// Current matching set, kept sorted and deduplicated.
    topics: BTreeSet<String>,
}

impl ScalableTopicsWatch {
    /// Open an empty watch for `watch_id` over `namespace`.
    #[must_use]
    pub fn new(watch_id: u64, namespace: String) -> Self {
        Self {
            watch_id,
            namespace,
            topics: BTreeSet::new(),
        }
    }

    /// The namespace being watched.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The current matching topic set, sorted.
    #[must_use]
    pub fn topics(&self) -> Vec<String> {
        self.topics.iter().cloned().collect()
    }

    /// Apply a `CommandWatchScalableTopicsUpdate`.
    ///
    /// A snapshot replaces the set; a diff applies `removed` **before** `added`,
    /// which is the order upstream specifies — applying them the other way round
    /// would drop a topic that appears in both lists.
    ///
    /// # Errors
    ///
    /// - [`TopicsWatchError::WatchMismatch`] when the update targets another watch.
    /// - [`TopicsWatchError::Broker`] when the broker rejected the watch.
    /// - [`TopicsWatchError::Empty`] when the update carried no event at all.
    pub fn handle_update(
        &mut self,
        upd: &pb::CommandWatchScalableTopicsUpdate,
    ) -> Result<TopicsChange, TopicsWatchError> {
        if upd.watch_id != self.watch_id {
            return Err(TopicsWatchError::WatchMismatch {
                got: upd.watch_id,
                expected: self.watch_id,
            });
        }
        let Some(event) = upd.event.as_ref() else {
            if let Some(code) = upd.error {
                return Err(TopicsWatchError::Broker {
                    code,
                    message: upd.message.clone().unwrap_or_default(),
                });
            }
            return Err(TopicsWatchError::Empty {
                watch_id: upd.watch_id,
            });
        };

        match event {
            pb::command_watch_scalable_topics_update::Event::Snapshot(snap) => {
                self.topics = snap.topics.iter().cloned().collect();
                Ok(TopicsChange::Snapshot {
                    topics: self.topics(),
                })
            }
            pb::command_watch_scalable_topics_update::Event::Diff(diff) => {
                // Upstream: apply removed before added when both are present.
                let mut removed = Vec::new();
                for t in &diff.removed {
                    if self.topics.remove(t) {
                        removed.push(t.clone());
                    }
                }
                let mut added = Vec::new();
                for t in &diff.added {
                    if self.topics.insert(t.clone()) {
                        added.push(t.clone());
                    }
                }
                removed.sort();
                added.sort();
                Ok(TopicsChange::Diff { added, removed })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assigned(id: u64, start: u32, end: u32) -> pb::ScalableAssignedSegment {
        pb::ScalableAssignedSegment {
            segment_id: id,
            hash_start: start,
            hash_end: end,
            segment_topic: format!("segment://public/default/scaled/{id}"),
        }
    }

    fn assignment(
        epoch: u64,
        segs: Vec<pb::ScalableAssignedSegment>,
    ) -> pb::ScalableConsumerAssignment {
        pb::ScalableConsumerAssignment {
            layout_epoch: epoch,
            segments: segs,
        }
    }

    fn session() -> ScalableConsumerSession {
        ScalableConsumerSession::new(
            7,
            "topic://public/default/scaled".to_owned(),
            "sub".to_owned(),
            "consumer-a".to_owned(),
            ScalableConsumerType::Stream,
        )
    }

    fn registered_session() -> ScalableConsumerSession {
        let mut s = session();
        s.handle_subscribe_response(&pb::CommandScalableTopicSubscribeResponse {
            request_id: 1,
            error: None,
            message: None,
            assignment: Some(assignment(1, vec![assigned(1, 0, 32_768)])),
        })
        .expect("subscribe resolves");
        s
    }

    /// Layer (a) test: the subscribe response resolves the registration and
    /// carries the initial assignment plus its `segment://` topics.
    #[test]
    fn subscribe_response_resolves_assignment() {
        let mut s = session();
        assert!(!s.is_registered());
        let a = s
            .handle_subscribe_response(&pb::CommandScalableTopicSubscribeResponse {
                request_id: 1,
                error: None,
                message: None,
                assignment: Some(assignment(
                    3,
                    vec![assigned(2, 32_768, 65_536), assigned(1, 0, 32_768)],
                )),
            })
            .expect("subscribe resolves");
        assert!(s.is_registered());
        assert_eq!(a.layout_epoch, 3);
        // Sorted by segment id regardless of wire order, so engine comparisons
        // are order-independent.
        assert_eq!(
            a.segment_topics(),
            vec![
                "segment://public/default/scaled/1",
                "segment://public/default/scaled/2"
            ]
        );
        assert_eq!(
            a.segments[0].key_range,
            KeyRange {
                start: 0,
                end: 32_768
            }
        );
    }

    /// Layer (a) test: a rebalance reports exactly what to attach and detach.
    #[test]
    fn assignment_update_reports_gained_and_lost() {
        let mut s = registered_session();
        let delta = s
            .handle_assignment_update(&pb::CommandScalableTopicAssignmentUpdate {
                consumer_id: 7,
                assignment: assignment(2, vec![assigned(2, 32_768, 65_536)]),
            })
            .expect("rebalance applies");
        assert_eq!(delta.layout_epoch, 2);
        assert_eq!(delta.gained.len(), 1);
        assert_eq!(delta.gained[0].segment_id, SegmentId(2));
        assert_eq!(delta.lost, vec![SegmentId(1)]);
        assert!(!delta.is_empty());
    }

    /// A stale assignment is rejected and the session keeps the newer one — the
    /// broker recomputes per layout, so applying it would hand the consumer
    /// segments that no longer exist.
    #[test]
    fn stale_assignment_rejected() {
        let mut s = registered_session();
        let err = s
            .handle_assignment_update(&pb::CommandScalableTopicAssignmentUpdate {
                consumer_id: 7,
                assignment: assignment(1, vec![assigned(9, 0, 65_536)]),
            })
            .expect_err("stale assignment rejected");
        assert_eq!(err, AssignmentError::StaleEpoch { got: 1, prev: 1 });
        assert_eq!(
            s.assignment().expect("still registered").segments[0].segment_id,
            SegmentId(1)
        );
    }

    /// An update for another consumer is rejected.
    #[test]
    fn assignment_for_other_consumer_rejected() {
        let mut s = registered_session();
        let err = s
            .handle_assignment_update(&pb::CommandScalableTopicAssignmentUpdate {
                consumer_id: 999,
                assignment: assignment(2, vec![]),
            })
            .expect_err("consumer mismatch rejected");
        assert_eq!(
            err,
            AssignmentError::ConsumerMismatch {
                got: 999,
                expected: 7
            }
        );
    }

    /// A rejected registration surfaces the broker's code and message.
    #[test]
    fn subscribe_rejection_surfaces_broker_error() {
        let mut s = session();
        let err = s
            .handle_subscribe_response(&pb::CommandScalableTopicSubscribeResponse {
                request_id: 1,
                error: Some(pb::ServerError::AuthorizationError as i32),
                message: Some("not permitted".to_owned()),
                assignment: None,
            })
            .expect_err("rejection surfaces");
        assert_eq!(
            err,
            AssignmentError::Broker {
                code: pb::ServerError::AuthorizationError as i32,
                message: "not permitted".to_owned(),
            }
        );
        assert!(!s.is_registered());
    }

    /// Layer (a) test: a namespace-watch snapshot replaces the set.
    #[test]
    fn topics_watch_snapshot_replaces_set() {
        let mut w = ScalableTopicsWatch::new(3, "public/default".to_owned());
        assert!(
            w.topics().is_empty(),
            "no matching set before the first update"
        );
        let change = w
            .handle_update(&pb::CommandWatchScalableTopicsUpdate {
                watch_id: 3,
                error: None,
                message: None,
                event: Some(pb::command_watch_scalable_topics_update::Event::Snapshot(
                    pb::ScalableTopicsSnapshot {
                        topics: vec![
                            "topic://public/default/b".to_owned(),
                            "topic://public/default/a".to_owned(),
                        ],
                    },
                )),
            })
            .expect("snapshot applies");
        assert_eq!(
            change,
            TopicsChange::Snapshot {
                topics: vec![
                    "topic://public/default/a".to_owned(),
                    "topic://public/default/b".to_owned()
                ]
            }
        );
    }

    /// A diff applies `removed` before `added`, so a topic named in both stays
    /// in the set rather than being dropped.
    #[test]
    fn topics_watch_diff_applies_removed_before_added() {
        let mut w = ScalableTopicsWatch::new(3, "public/default".to_owned());
        w.handle_update(&pb::CommandWatchScalableTopicsUpdate {
            watch_id: 3,
            error: None,
            message: None,
            event: Some(pb::command_watch_scalable_topics_update::Event::Snapshot(
                pb::ScalableTopicsSnapshot {
                    topics: vec!["topic://public/default/a".to_owned()],
                },
            )),
        })
        .expect("snapshot");

        let change = w
            .handle_update(&pb::CommandWatchScalableTopicsUpdate {
                watch_id: 3,
                error: None,
                message: None,
                event: Some(pb::command_watch_scalable_topics_update::Event::Diff(
                    pb::ScalableTopicsDiff {
                        added: vec![
                            "topic://public/default/a".to_owned(),
                            "topic://public/default/c".to_owned(),
                        ],
                        removed: vec!["topic://public/default/a".to_owned()],
                    },
                )),
            })
            .expect("diff applies");
        assert_eq!(
            change,
            TopicsChange::Diff {
                added: vec![
                    "topic://public/default/a".to_owned(),
                    "topic://public/default/c".to_owned()
                ],
                removed: vec!["topic://public/default/a".to_owned()],
            }
        );
        // `a` was removed then re-added, so it survives — the reverse order
        // would have dropped it.
        assert_eq!(
            w.topics(),
            vec![
                "topic://public/default/a".to_owned(),
                "topic://public/default/c".to_owned()
            ]
        );
    }

    /// An update for another watch is rejected.
    #[test]
    fn topics_watch_mismatch_rejected() {
        let mut w = ScalableTopicsWatch::new(3, "public/default".to_owned());
        let err = w
            .handle_update(&pb::CommandWatchScalableTopicsUpdate {
                watch_id: 99,
                error: None,
                message: None,
                event: None,
            })
            .expect_err("watch mismatch rejected");
        assert_eq!(
            err,
            TopicsWatchError::WatchMismatch {
                got: 99,
                expected: 3
            }
        );
    }

    /// A rejected watch surfaces the broker's error rather than an empty set.
    #[test]
    fn topics_watch_rejection_surfaces_broker_error() {
        let mut w = ScalableTopicsWatch::new(3, "public/default".to_owned());
        let err = w
            .handle_update(&pb::CommandWatchScalableTopicsUpdate {
                watch_id: 3,
                error: Some(pb::ServerError::AuthorizationError as i32),
                message: Some("nope".to_owned()),
                event: None,
            })
            .expect_err("rejection surfaces");
        assert_eq!(
            err,
            TopicsWatchError::Broker {
                code: pb::ServerError::AuthorizationError as i32,
                message: "nope".to_owned(),
            }
        );
        assert!(
            w.topics().is_empty(),
            "no matching set before the first update"
        );
        assert!(w.topics().is_empty());
    }

    /// The consumer-type enum round-trips and saturates unknown wire values,
    /// so a future broker variant cannot break the client.
    #[test]
    fn consumer_type_roundtrips_and_saturates() {
        assert_eq!(
            ScalableConsumerType::from_pb_i32(ScalableConsumerType::Stream.to_pb_i32()),
            ScalableConsumerType::Stream
        );
        assert_eq!(
            ScalableConsumerType::from_pb_i32(ScalableConsumerType::Checkpoint.to_pb_i32()),
            ScalableConsumerType::Checkpoint
        );
        assert_eq!(
            ScalableConsumerType::from_pb_i32(99),
            ScalableConsumerType::Stream
        );
    }
}
