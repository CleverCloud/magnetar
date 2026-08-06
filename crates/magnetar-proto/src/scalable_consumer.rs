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
//! Assignment order is fenced by a caller-supplied controller incarnation.
//! Within one incarnation a lower layout epoch is rejected, an exact duplicate
//! is ignored, and a changed assignment at the current layout epoch is applied
//! in receive order. The epoch versions the layout, not group membership.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::pb;
use crate::types::{KeyRange, KeyRangeError, SegmentId};

/// Maximum assignment pushes retained while the subscribe response is pending.
pub const MAX_BUFFERED_ASSIGNMENT_UPDATES: usize = 1_024;

/// Local generation of the physical controller connection carrying an
/// assignment stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct ControllerIncarnation(pub u64);

impl core::fmt::Display for ControllerIncarnation {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Invalid canonical scalable-topic or segment attachment identity.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SegmentTopicError {
    /// A parent must be a fully-qualified `topic://tenant/namespace/name`.
    #[error("invalid canonical scalable topic {topic:?}")]
    InvalidParent {
        /// Rejected parent identity.
        topic: String,
    },
    /// A segment URI did not have the M1 canonical shape.
    #[error("invalid canonical segment topic {topic:?}")]
    InvalidSegment {
        /// Rejected segment identity.
        topic: String,
    },
    /// The URI was parseable but not byte-for-byte canonical.
    #[error("non-canonical segment topic {got:?}; expected {expected:?}")]
    NonCanonical {
        /// Canonical identity.
        expected: String,
        /// Wire identity.
        got: String,
    },
    /// The source wrapper and URI descriptor name different segments.
    #[error("segment topic names id {topic_id}, not {segment_id}")]
    SegmentIdMismatch {
        /// Explicit segment id.
        segment_id: SegmentId,
        /// Id parsed from the topic descriptor.
        topic_id: SegmentId,
    },
    /// The descriptor's inclusive range is invalid.
    #[error("invalid segment topic range: {0}")]
    InvalidRange(#[from] KeyRangeError),
}

/// Canonical, source-qualified segment identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SegmentSource {
    segment_id: SegmentId,
    topic: String,
    key_range: KeyRange,
}

impl SegmentSource {
    /// Validate a canonical M1 `segment://` URI and its explicit id.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentTopicError`] for malformed, non-canonical, or
    /// mismatched identities.
    pub fn new(segment_id: SegmentId, topic: String) -> Result<Self, SegmentTopicError> {
        let (_, key_range, topic_id) = parse_segment_topic(&topic)?;
        if topic_id != segment_id {
            return Err(SegmentTopicError::SegmentIdMismatch {
                segment_id,
                topic_id,
            });
        }
        Ok(Self {
            segment_id,
            topic,
            key_range,
        })
    }

    /// Segment id encoded by this source.
    #[must_use]
    pub const fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    /// Canonical `segment://` topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Inclusive range encoded in the topic descriptor.
    #[must_use]
    pub const fn key_range(&self) -> KeyRange {
        self.key_range
    }

    /// Canonical parent `topic://` identity.
    #[must_use]
    pub fn parent_topic(&self) -> String {
        let descriptor_len = self
            .topic
            .rsplit_once('/')
            .map_or(0, |(_, descriptor)| descriptor.len() + 1);
        let parent_path = self
            .topic
            .strip_prefix("segment://")
            .and_then(|rest| rest.get(..rest.len().saturating_sub(descriptor_len)))
            .unwrap_or_default();
        format!("topic://{parent_path}")
    }
}

/// Construct the canonical M1 segment identity for a parent, range, and id.
///
/// # Errors
///
/// Returns [`SegmentTopicError::InvalidParent`] unless `parent_topic` is a
/// canonical, fully-qualified scalable topic.
pub fn canonical_segment_topic(
    parent_topic: &str,
    key_range: KeyRange,
    segment_id: SegmentId,
) -> Result<String, SegmentTopicError> {
    let parent_path = canonical_parent_path(parent_topic)?;
    Ok(format!(
        "segment://{parent_path}/{}-{}",
        key_range.to_hex_string(),
        segment_id.0
    ))
}

fn canonical_parent_path(parent_topic: &str) -> Result<&str, SegmentTopicError> {
    let Some(path) = parent_topic.strip_prefix("topic://") else {
        return Err(SegmentTopicError::InvalidParent {
            topic: parent_topic.to_owned(),
        });
    };
    let mut parts = path.splitn(3, '/');
    let valid = parts.next().is_some_and(|part| !part.is_empty())
        && parts.next().is_some_and(|part| !part.is_empty())
        && parts.next().is_some_and(|part| {
            !part.is_empty() && !part.ends_with('/') && !part.contains(['?', '#'])
        });
    if !valid {
        return Err(SegmentTopicError::InvalidParent {
            topic: parent_topic.to_owned(),
        });
    }
    Ok(path)
}

fn parse_segment_topic(topic: &str) -> Result<(String, KeyRange, SegmentId), SegmentTopicError> {
    let Some(path) = topic.strip_prefix("segment://") else {
        return Err(SegmentTopicError::InvalidSegment {
            topic: topic.to_owned(),
        });
    };
    let Some((parent_path, descriptor)) = path.rsplit_once('/') else {
        return Err(SegmentTopicError::InvalidSegment {
            topic: topic.to_owned(),
        });
    };
    let parent = format!("topic://{parent_path}");
    canonical_parent_path(&parent)?;

    let mut descriptor_parts = descriptor.split('-');
    let (Some(start), Some(end), Some(id), None) = (
        descriptor_parts.next(),
        descriptor_parts.next(),
        descriptor_parts.next(),
        descriptor_parts.next(),
    ) else {
        return Err(SegmentTopicError::InvalidSegment {
            topic: topic.to_owned(),
        });
    };
    let (Some(start), Some(end)) = (
        parse_canonical_hex_word(start),
        parse_canonical_hex_word(end),
    ) else {
        return Err(SegmentTopicError::InvalidSegment {
            topic: topic.to_owned(),
        });
    };
    if !is_canonical_decimal(id) {
        return Err(SegmentTopicError::InvalidSegment {
            topic: topic.to_owned(),
        });
    }
    let id = id
        .parse::<u64>()
        .map_err(|_| SegmentTopicError::InvalidSegment {
            topic: topic.to_owned(),
        })?;
    Ok((parent, KeyRange::new(start, end)?, SegmentId(id)))
}

fn parse_canonical_hex_word(value: &str) -> Option<u32> {
    (value.len() == 4
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then(|| u32::from_str_radix(value, 16).ok())
    .flatten()
}

fn is_canonical_decimal(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && (value == "0" || !value.starts_with('0'))
}

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
    segment_id: SegmentId,
    /// Hash key range this segment serves.
    key_range: KeyRange,
    /// Fully-qualified `segment://...` topic the consumer attaches to.
    segment_topic: String,
}

impl AssignedSegment {
    /// Strictly decode and validate one wire assignment.
    ///
    /// # Errors
    ///
    /// Returns [`AssignmentError`] when the range or canonical attachment
    /// identity is invalid.
    pub fn try_from_pb(
        pb: &pb::ScalableAssignedSegment,
        parent_topic: &str,
    ) -> Result<Self, AssignmentError> {
        let segment_id = SegmentId(pb.segment_id);
        let key_range = KeyRange::new(pb.hash_start, pb.hash_end)
            .map_err(|source| AssignmentError::InvalidRange { segment_id, source })?;
        let expected = canonical_segment_topic(parent_topic, key_range, segment_id)?;
        if pb.segment_topic != expected {
            return Err(AssignmentError::AttachmentMismatch {
                segment_id,
                expected,
                got: pb.segment_topic.clone(),
            });
        }
        Ok(Self {
            segment_id,
            key_range,
            segment_topic: pb.segment_topic.clone(),
        })
    }

    /// Segment id.
    #[must_use]
    pub const fn segment_id(&self) -> SegmentId {
        self.segment_id
    }

    /// Inclusive M1 range.
    #[must_use]
    pub const fn key_range(&self) -> KeyRange {
        self.key_range
    }

    /// Canonical attachment topic.
    #[must_use]
    pub fn segment_topic(&self) -> &str {
        &self.segment_topic
    }

    /// Source-qualified identity for message delivery.
    #[must_use]
    pub fn source(&self) -> SegmentSource {
        SegmentSource {
            segment_id: self.segment_id,
            topic: self.segment_topic.clone(),
            key_range: self.key_range,
        }
    }

    /// Encode into the wire message.
    #[must_use]
    pub fn to_pb(&self) -> pb::ScalableAssignedSegment {
        pb::ScalableAssignedSegment {
            segment_id: self.segment_id.0,
            hash_start: self.key_range.start(),
            hash_end: self.key_range.end(),
            segment_topic: self.segment_topic.clone(),
        }
    }
}

/// The set of segments this consumer owns, stamped with the layout epoch it was
/// computed from.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConsumerAssignment {
    /// Layout epoch this assignment was computed against.
    layout_epoch: u64,
    /// Segments assigned to this consumer, ordered by segment id.
    segments: Vec<AssignedSegment>,
}

impl ConsumerAssignment {
    /// Strictly decode, validate, and normalize a wire assignment.
    ///
    /// # Errors
    ///
    /// Returns [`AssignmentError`] for an invalid range, attachment, or
    /// duplicate segment id.
    pub fn try_from_pb(
        pb: &pb::ScalableConsumerAssignment,
        parent_topic: &str,
    ) -> Result<Self, AssignmentError> {
        let mut segments = Vec::with_capacity(pb.segments.len());
        let mut ids = BTreeSet::new();
        for segment in &pb.segments {
            let segment = AssignedSegment::try_from_pb(segment, parent_topic)?;
            if !ids.insert(segment.segment_id) {
                return Err(AssignmentError::DuplicateSegment {
                    segment_id: segment.segment_id,
                });
            }
            segments.push(segment);
        }
        segments.sort_by_key(|s| s.segment_id);
        Ok(Self {
            layout_epoch: pb.layout_epoch,
            segments,
        })
    }

    /// Layout epoch this assignment was computed against.
    #[must_use]
    pub const fn layout_epoch(&self) -> u64 {
        self.layout_epoch
    }

    /// Normalized segments, ordered by id.
    #[must_use]
    pub fn segments(&self) -> &[AssignedSegment] {
        &self.segments
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

    /// The assignment's `layout_epoch` regressed within one connection
    /// incarnation.
    #[error("assignment layout epoch regressed: got {got} below {prev}")]
    StaleEpoch {
        /// The `layout_epoch` the broker sent.
        got: u64,
        /// The highest `layout_epoch` already applied.
        prev: u64,
    },

    /// A delayed callback belongs to another controller connection.
    #[error("assignment incarnation {got} does not match current incarnation {expected}")]
    IncarnationMismatch {
        /// Callback incarnation.
        got: ControllerIncarnation,
        /// Installed incarnation.
        expected: ControllerIncarnation,
    },

    /// Controller incarnations must advance when a replacement is installed.
    #[error("controller incarnation {got} did not advance beyond {prev}")]
    NonAdvancingIncarnation {
        /// Requested replacement incarnation.
        got: ControllerIncarnation,
        /// Current incarnation.
        prev: ControllerIncarnation,
    },

    /// A replacement connection cannot accept a layout older than any layout
    /// already accepted by this logical registration.
    #[error("assignment layout epoch {got} is below reconnect floor {floor}")]
    CrossIncarnationEpochRegression {
        /// Replacement baseline or push epoch.
        got: u64,
        /// Highest epoch accepted before reconnect.
        floor: u64,
    },

    /// A controller sent too many pushes before its subscribe response.
    #[error("pre-baseline assignment buffer reached its maximum of {max} updates")]
    PreBaselineBufferFull {
        /// Fixed queue bound.
        max: usize,
    },

    /// A reconnect attempted to reuse a consumer id for another registration.
    #[error("consumer {consumer_id} reconnect changed its registration identity")]
    RegistrationMismatch {
        /// Stable consumer id.
        consumer_id: u64,
    },

    /// The assignment repeated a segment id.
    #[error("assignment repeats segment {segment_id}")]
    DuplicateSegment {
        /// Repeated id.
        segment_id: SegmentId,
    },

    /// The assigned inclusive M1 range is invalid.
    #[error("assignment segment {segment_id} has an invalid range: {source}")]
    InvalidRange {
        /// Segment whose range was invalid.
        segment_id: SegmentId,
        /// Range validation failure.
        source: KeyRangeError,
    },

    /// The broker-authored attachment does not match the parent, range, and id.
    #[error("assignment segment {segment_id} names {got:?}; expected {expected:?}")]
    AttachmentMismatch {
        /// Segment id.
        segment_id: SegmentId,
        /// Canonical expected topic.
        expected: String,
        /// Broker-authored topic.
        got: String,
    },

    /// The parent or segment identity is malformed.
    #[error(transparent)]
    SegmentTopic(#[from] SegmentTopicError),

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

/// One changed assignment replayed after the subscribe-response baseline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentReplay {
    /// Complete authoritative assignment after applying this push.
    pub assignment: ConsumerAssignment,
    /// Delta from the preceding baseline or push.
    pub delta: AssignmentDelta,
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
    incarnation: ControllerIncarnation,
    /// `None` until the subscribe response lands.
    assignment: Option<ConsumerAssignment>,
    /// Highest layout epoch accepted by this logical registration, retained
    /// across physical controller incarnations.
    epoch_floor: Option<u64>,
    /// Validated pushes received before the subscribe response, in wire order.
    buffered_updates: VecDeque<ConsumerAssignment>,
    /// Changed pushes applied immediately after the latest response baseline.
    replayed_updates: Vec<AssignmentReplay>,
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
        incarnation: ControllerIncarnation,
    ) -> Self {
        Self {
            consumer_id,
            topic,
            subscription,
            consumer_name,
            consumer_type,
            incarnation,
            assignment: None,
            epoch_floor: None,
            buffered_updates: VecDeque::new(),
            replayed_updates: Vec::new(),
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

    /// Current local controller-connection incarnation.
    #[must_use]
    pub const fn incarnation(&self) -> ControllerIncarnation {
        self.incarnation
    }

    /// Highest accepted layout epoch across every controller incarnation.
    #[must_use]
    pub const fn epoch_floor(&self) -> Option<u64> {
        self.epoch_floor
    }

    /// Whether registration identity is unchanged for a reconnect attempt.
    #[must_use]
    pub fn matches_registration(
        &self,
        topic: &str,
        subscription: &str,
        consumer_name: &str,
        consumer_type: ScalableConsumerType,
    ) -> bool {
        self.topic == topic
            && self.subscription == subscription
            && self.consumer_name == consumer_name
            && self.consumer_type == consumer_type
    }

    /// Drain changed pushes replayed after the most recent response baseline.
    pub fn take_replayed_updates(&mut self) -> Vec<AssignmentReplay> {
        core::mem::take(&mut self.replayed_updates)
    }

    /// Fence the old connection and begin registration on a replacement.
    ///
    /// The replacement response becomes a fresh baseline, so the prior
    /// assignment is deliberately cleared.
    ///
    /// # Errors
    ///
    /// Returns [`AssignmentError::NonAdvancingIncarnation`] unless the local
    /// generation strictly advances.
    pub fn begin_incarnation(
        &mut self,
        incarnation: ControllerIncarnation,
    ) -> Result<(), AssignmentError> {
        if incarnation <= self.incarnation {
            return Err(AssignmentError::NonAdvancingIncarnation {
                got: incarnation,
                prev: self.incarnation,
            });
        }
        self.incarnation = incarnation;
        self.assignment = None;
        self.buffered_updates.clear();
        self.replayed_updates.clear();
        Ok(())
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
        self.handle_subscribe_response_for(self.incarnation, resp)
    }

    /// Incarnation-fenced form of [`Self::handle_subscribe_response`].
    ///
    /// # Errors
    ///
    /// Returns [`AssignmentError::IncarnationMismatch`] for a delayed response
    /// from an old connection, in addition to response and validation errors.
    pub fn handle_subscribe_response_for(
        &mut self,
        incarnation: ControllerIncarnation,
        resp: &pb::CommandScalableTopicSubscribeResponse,
    ) -> Result<ConsumerAssignment, AssignmentError> {
        self.require_incarnation(incarnation)?;
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
        let baseline = ConsumerAssignment::try_from_pb(assignment, &self.topic)?;
        self.enforce_epoch_floor(baseline.layout_epoch)?;

        // Baseline plus every pre-response push is one atomic state change. A
        // stale replay leaves the unresolved session and its queue untouched so
        // the caller can fail and resynchronize the registration.
        let mut staged = self.clone();
        staged.assignment = Some(baseline.clone());
        staged.epoch_floor = Some(staged.epoch_floor.map_or(baseline.layout_epoch, |floor| {
            floor.max(baseline.layout_epoch)
        }));
        staged.replayed_updates.clear();
        while let Some(incoming) = staged.buffered_updates.pop_front() {
            if let Some(delta) = staged.apply_incoming(incoming)? {
                let assignment = staged.assignment.clone().ok_or(AssignmentError::Empty {
                    request_id: resp.request_id,
                })?;
                staged
                    .replayed_updates
                    .push(AssignmentReplay { assignment, delta });
            }
        }
        *self = staged;
        Ok(baseline)
    }

    /// Apply a pushed `CommandScalableTopicAssignmentUpdate`, returning what the
    /// consumer must attach to and detach from.
    ///
    /// # Errors
    ///
    /// - [`AssignmentError::ConsumerMismatch`] when the update targets another consumer.
    /// - [`AssignmentError::StaleEpoch`] when `layout_epoch` regresses.
    ///
    /// On either error the session is left **unchanged**.
    pub fn handle_assignment_update(
        &mut self,
        upd: &pb::CommandScalableTopicAssignmentUpdate,
    ) -> Result<Option<AssignmentDelta>, AssignmentError> {
        self.handle_assignment_update_for(self.incarnation, upd)
    }

    /// Incarnation-fenced assignment update.
    ///
    /// `Ok(None)` is an exact duplicate. A changed assignment at the current
    /// layout epoch is applied; only a lower epoch is stale.
    pub fn handle_assignment_update_for(
        &mut self,
        incarnation: ControllerIncarnation,
        upd: &pb::CommandScalableTopicAssignmentUpdate,
    ) -> Result<Option<AssignmentDelta>, AssignmentError> {
        self.require_incarnation(incarnation)?;
        if upd.consumer_id != self.consumer_id {
            return Err(AssignmentError::ConsumerMismatch {
                got: upd.consumer_id,
                expected: self.consumer_id,
            });
        }
        let incoming = ConsumerAssignment::try_from_pb(&upd.assignment, &self.topic)?;
        if self.assignment.is_none() {
            self.enforce_epoch_floor(incoming.layout_epoch)?;
            if self.buffered_updates.len() == MAX_BUFFERED_ASSIGNMENT_UPDATES {
                return Err(AssignmentError::PreBaselineBufferFull {
                    max: MAX_BUFFERED_ASSIGNMENT_UPDATES,
                });
            }
            self.buffered_updates.push_back(incoming);
            return Ok(None);
        }

        self.apply_incoming(incoming)
    }

    fn apply_incoming(
        &mut self,
        incoming: ConsumerAssignment,
    ) -> Result<Option<AssignmentDelta>, AssignmentError> {
        if let Some(prev) = self.assignment.as_ref()
            && incoming.layout_epoch < prev.layout_epoch
        {
            return Err(AssignmentError::StaleEpoch {
                got: incoming.layout_epoch,
                prev: prev.layout_epoch,
            });
        }

        if self.assignment.as_ref() == Some(&incoming) {
            return Ok(None);
        }

        let before: BTreeMap<SegmentId, &AssignedSegment> = self
            .assignment
            .as_ref()
            .map(|a| a.segments.iter().map(|s| (s.segment_id, s)).collect())
            .unwrap_or_default();
        let after: BTreeMap<SegmentId, &AssignedSegment> = incoming
            .segments
            .iter()
            .map(|s| (s.segment_id, s))
            .collect();

        let gained: Vec<AssignedSegment> = incoming
            .segments
            .iter()
            .filter(|s| before.get(&s.segment_id).is_none_or(|before| *before != *s))
            .cloned()
            .collect();
        let lost: Vec<SegmentId> = before
            .iter()
            .filter(|(id, before)| after.get(id).is_none_or(|after| *after != **before))
            .map(|(id, _)| *id)
            .collect();
        let layout_epoch = incoming.layout_epoch;
        self.assignment = Some(incoming);
        self.epoch_floor = Some(
            self.epoch_floor
                .map_or(layout_epoch, |floor| floor.max(layout_epoch)),
        );

        Ok(Some(AssignmentDelta {
            layout_epoch,
            gained,
            lost,
        }))
    }

    fn enforce_epoch_floor(&self, epoch: u64) -> Result<(), AssignmentError> {
        if let Some(floor) = self.epoch_floor
            && epoch < floor
        {
            return Err(AssignmentError::CrossIncarnationEpochRegression { got: epoch, floor });
        }
        Ok(())
    }

    fn require_incarnation(
        &self,
        incarnation: ControllerIncarnation,
    ) -> Result<(), AssignmentError> {
        if incarnation != self.incarnation {
            return Err(AssignmentError::IncarnationMismatch {
                got: incarnation,
                expected: self.incarnation,
            });
        }
        Ok(())
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
        let range = KeyRange::new(start, end).expect("valid test range");
        pb::ScalableAssignedSegment {
            segment_id: id,
            hash_start: start,
            hash_end: end,
            segment_topic: canonical_segment_topic(
                "topic://public/default/scaled",
                range,
                SegmentId(id),
            )
            .expect("canonical test segment"),
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
            ControllerIncarnation(1),
        )
    }

    fn registered_session() -> ScalableConsumerSession {
        let mut s = session();
        s.handle_subscribe_response(&pb::CommandScalableTopicSubscribeResponse {
            request_id: 1,
            error: None,
            message: None,
            assignment: Some(assignment(1, vec![assigned(1, 0, 32_767)])),
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
                    vec![assigned(2, 32_768, 65_535), assigned(1, 0, 32_767)],
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
                "segment://public/default/scaled/0000-7fff-1",
                "segment://public/default/scaled/8000-ffff-2"
            ]
        );
        assert_eq!(
            a.segments[0].key_range,
            KeyRange::new(0, 32_767).expect("valid inclusive range")
        );
    }

    /// Layer (a) test: a rebalance reports exactly what to attach and detach.
    #[test]
    fn assignment_update_reports_gained_and_lost() {
        let mut s = registered_session();
        let delta = s
            .handle_assignment_update(&pb::CommandScalableTopicAssignmentUpdate {
                consumer_id: 7,
                assignment: assignment(2, vec![assigned(2, 32_768, 65_535)]),
            })
            .expect("rebalance applies")
            .expect("changed assignment");
        assert_eq!(delta.layout_epoch, 2);
        assert_eq!(delta.gained.len(), 1);
        assert_eq!(delta.gained[0].segment_id, SegmentId(2));
        assert_eq!(delta.lost, vec![SegmentId(1)]);
        assert!(!delta.is_empty());
    }

    /// A lower assignment epoch is rejected and the session keeps the newer one.
    #[test]
    fn stale_assignment_rejected() {
        let mut s = registered_session();
        let err = s
            .handle_assignment_update(&pb::CommandScalableTopicAssignmentUpdate {
                consumer_id: 7,
                assignment: assignment(0, vec![assigned(9, 0, 65_535)]),
            })
            .expect_err("stale assignment rejected");
        assert_eq!(err, AssignmentError::StaleEpoch { got: 0, prev: 1 });
        assert_eq!(
            s.assignment().expect("still registered").segments[0].segment_id,
            SegmentId(1)
        );
    }

    #[test]
    fn changed_equal_epoch_assignment_applies_and_exact_duplicate_is_ignored() {
        let mut session = registered_session();
        let changed = pb::CommandScalableTopicAssignmentUpdate {
            consumer_id: 7,
            assignment: assignment(1, vec![assigned(2, 32_768, 65_535)]),
        };
        let delta = session
            .handle_assignment_update(&changed)
            .expect("equal layout epoch is valid")
            .expect("membership changed");
        assert_eq!(delta.layout_epoch, 1);
        assert_eq!(delta.gained[0].segment_id(), SegmentId(2));
        assert_eq!(delta.lost, vec![SegmentId(1)]);
        assert_eq!(
            session.handle_assignment_update(&changed),
            Ok(None),
            "exact re-delivery is idempotent"
        );
    }

    #[test]
    fn same_id_attachment_change_is_reported_as_lost_and_gained() {
        let mut session = registered_session();
        let changed = pb::CommandScalableTopicAssignmentUpdate {
            consumer_id: 7,
            assignment: assignment(1, vec![assigned(1, 0, 65_535)]),
        };
        let delta = session
            .handle_assignment_update(&changed)
            .expect("valid update")
            .expect("descriptor changed");
        assert_eq!(delta.lost, vec![SegmentId(1)]);
        assert_eq!(delta.gained[0].segment_id(), SegmentId(1));
    }

    #[test]
    fn incarnation_fences_delayed_callbacks_and_replacement_resets_baseline() {
        let mut session = registered_session();
        session
            .begin_incarnation(ControllerIncarnation(2))
            .expect("advance incarnation");
        assert!(!session.is_registered());
        let update = pb::CommandScalableTopicAssignmentUpdate {
            consumer_id: 7,
            assignment: assignment(2, vec![]),
        };
        assert_eq!(
            session.handle_assignment_update_for(ControllerIncarnation(1), &update),
            Err(AssignmentError::IncarnationMismatch {
                got: ControllerIncarnation(1),
                expected: ControllerIncarnation(2),
            })
        );
        assert_eq!(
            session.begin_incarnation(ControllerIncarnation(2)),
            Err(AssignmentError::NonAdvancingIncarnation {
                got: ControllerIncarnation(2),
                prev: ControllerIncarnation(2),
            })
        );
    }

    #[test]
    fn pre_response_pushes_replay_after_baseline_in_wire_order() {
        let mut session = session();
        let first_push = pb::CommandScalableTopicAssignmentUpdate {
            consumer_id: 7,
            assignment: assignment(1, vec![assigned(2, 32_768, 65_535)]),
        };
        let second_push = pb::CommandScalableTopicAssignmentUpdate {
            consumer_id: 7,
            assignment: assignment(2, vec![assigned(1, 0, 32_767), assigned(2, 32_768, 65_535)]),
        };
        assert_eq!(session.handle_assignment_update(&first_push), Ok(None));
        assert_eq!(session.handle_assignment_update(&second_push), Ok(None));
        assert!(
            session.assignment().is_none(),
            "pushes cannot become the baseline"
        );

        let baseline = session
            .handle_subscribe_response(&pb::CommandScalableTopicSubscribeResponse {
                request_id: 1,
                error: None,
                message: None,
                assignment: Some(assignment(1, vec![assigned(1, 0, 32_767)])),
            })
            .expect("baseline and pushes apply atomically");
        assert_eq!(baseline.segments()[0].segment_id(), SegmentId(1));
        let replayed = session.take_replayed_updates();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].delta.lost, vec![SegmentId(1)]);
        assert_eq!(replayed[0].delta.gained[0].segment_id(), SegmentId(2));
        assert_eq!(replayed[1].delta.gained[0].segment_id(), SegmentId(1));
        assert_eq!(replayed[1].assignment.layout_epoch(), 2);
        assert_eq!(session.assignment(), Some(&replayed[1].assignment));
        assert!(session.take_replayed_updates().is_empty());
    }

    #[test]
    fn pre_response_push_queue_is_bounded_without_partial_enqueue() {
        let mut session = session();
        let update = pb::CommandScalableTopicAssignmentUpdate {
            consumer_id: 7,
            assignment: assignment(1, vec![assigned(1, 0, 65_535)]),
        };
        for _ in 0..MAX_BUFFERED_ASSIGNMENT_UPDATES {
            assert_eq!(session.handle_assignment_update(&update), Ok(None));
        }
        assert_eq!(
            session.buffered_updates.len(),
            MAX_BUFFERED_ASSIGNMENT_UPDATES
        );
        assert_eq!(
            session.handle_assignment_update(&update),
            Err(AssignmentError::PreBaselineBufferFull {
                max: MAX_BUFFERED_ASSIGNMENT_UPDATES,
            })
        );
        assert_eq!(
            session.buffered_updates.len(),
            MAX_BUFFERED_ASSIGNMENT_UPDATES
        );
        assert!(session.assignment().is_none());
    }

    #[test]
    fn invalid_buffered_wire_order_rejects_baseline_atomically() {
        let mut session = session();
        for update in [
            pb::CommandScalableTopicAssignmentUpdate {
                consumer_id: 7,
                assignment: assignment(2, vec![assigned(2, 32_768, 65_535)]),
            },
            pb::CommandScalableTopicAssignmentUpdate {
                consumer_id: 7,
                assignment: assignment(1, vec![assigned(1, 0, 32_767)]),
            },
        ] {
            assert_eq!(session.handle_assignment_update(&update), Ok(None));
        }

        let response = pb::CommandScalableTopicSubscribeResponse {
            request_id: 1,
            error: None,
            message: None,
            assignment: Some(assignment(1, vec![assigned(1, 0, 32_767)])),
        };
        assert_eq!(
            session.handle_subscribe_response(&response),
            Err(AssignmentError::StaleEpoch { got: 1, prev: 2 })
        );
        assert!(session.assignment().is_none());
        assert_eq!(session.epoch_floor(), None);
        assert_eq!(session.buffered_updates.len(), 2);
        assert!(session.take_replayed_updates().is_empty());
    }

    #[test]
    fn reconnect_baseline_cannot_regress_below_retained_epoch_floor() {
        let mut session = session();
        session
            .handle_subscribe_response(&pb::CommandScalableTopicSubscribeResponse {
                request_id: 1,
                error: None,
                message: None,
                assignment: Some(assignment(4, vec![assigned(1, 0, 32_767)])),
            })
            .expect("initial baseline");
        session
            .begin_incarnation(ControllerIncarnation(2))
            .expect("replacement incarnation");
        assert_eq!(session.epoch_floor(), Some(4));
        assert_eq!(
            session.handle_assignment_update(&pb::CommandScalableTopicAssignmentUpdate {
                consumer_id: 7,
                assignment: assignment(3, vec![assigned(1, 0, 32_767)]),
            }),
            Err(AssignmentError::CrossIncarnationEpochRegression { got: 3, floor: 4 })
        );
        assert!(session.buffered_updates.is_empty());
        assert_eq!(
            session.handle_subscribe_response(&pb::CommandScalableTopicSubscribeResponse {
                request_id: 2,
                error: None,
                message: None,
                assignment: Some(assignment(3, vec![assigned(1, 0, 32_767)])),
            }),
            Err(AssignmentError::CrossIncarnationEpochRegression { got: 3, floor: 4 })
        );
        assert!(session.assignment().is_none());
        assert_eq!(session.epoch_floor(), Some(4));
    }

    #[test]
    fn assignment_validation_rejects_duplicate_range_and_attachment_errors_atomically() {
        let mut session = registered_session();
        let before = session.assignment().cloned();

        let duplicate = assignment(2, vec![assigned(1, 0, 32_767), assigned(1, 0, 32_767)]);
        assert!(matches!(
            session.handle_assignment_update(&pb::CommandScalableTopicAssignmentUpdate {
                consumer_id: 7,
                assignment: duplicate,
            }),
            Err(AssignmentError::DuplicateSegment { .. })
        ));

        let mut invalid_range = assigned(2, 32_768, 65_535);
        invalid_range.hash_end = 65_536;
        assert!(matches!(
            session.handle_assignment_update(&pb::CommandScalableTopicAssignmentUpdate {
                consumer_id: 7,
                assignment: assignment(2, vec![invalid_range]),
            }),
            Err(AssignmentError::InvalidRange { .. })
        ));

        let mut invalid_topic = assigned(2, 32_768, 65_535);
        invalid_topic.segment_topic = "segment://public/default/scaled/8000-ffff-02".to_owned();
        assert!(matches!(
            session.handle_assignment_update(&pb::CommandScalableTopicAssignmentUpdate {
                consumer_id: 7,
                assignment: assignment(2, vec![invalid_topic]),
            }),
            Err(AssignmentError::AttachmentMismatch { .. })
        ));
        assert_eq!(session.assignment(), before.as_ref());
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
