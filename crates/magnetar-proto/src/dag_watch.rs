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
//! `parent_ids` / `child_ids` edges of the incoming layout. A frame whose
//! epoch does not advance is **ignored** — it is a re-sent snapshot the
//! session already holds, and Pulsar 5.0.0-M1 sends one routinely — so it
//! neither mutates state nor ends the session.
//!
//! # Drop-on-change
//!
//! Per ADR-0093, carried forward by ADR-0093, the surface is **observation +
//! drop-on-change**: the session records the layout, applies updates, and
//! reports what changed, but does not perform transparent segment failover.
//! The runtime closes the per-segment consumers and surfaces a
//! `DagChangedDuringConsume` event when a split / merge / removal lands while a
//! `StreamConsumer` is active. Transparent failover and in-place repartition
//! are explicit future work.

use std::collections::{BTreeMap, BTreeSet};

use prost::Message as _;

use crate::pb;
use crate::scalable_consumer::{AssignmentError, ConsumerAssignment};
use crate::types::{
    KeyRange, MAX_HASH, MIN_HASH, SegmentDescriptor, SegmentDescriptorError, SegmentId,
    SegmentState,
};

/// Maximum nodes accepted in one atomic M1 DAG snapshot.
pub const MAX_DAG_SEGMENTS: usize = 4_096;
/// Maximum logical edges accepted in one atomic M1 DAG snapshot.
pub const MAX_DAG_EDGES: usize = 16_384;
/// Maximum parent-to-child ancestry depth accepted in one M1 DAG snapshot.
pub const MAX_DAG_DEPTH: usize = 256;
/// Maximum encoded size of one M1 DAG snapshot.
pub const MAX_DAG_SERIALIZED_SIZE: usize = 1024 * 1024;

/// Resource bounds applied while validating an untrusted DAG snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DagLimits {
    /// Maximum segment descriptors.
    pub segments: usize,
    /// Maximum logical edges.
    pub edges: usize,
    /// Maximum ancestry depth.
    pub depth: usize,
    /// Maximum encoded protobuf bytes.
    pub serialized_size: usize,
}

impl Default for DagLimits {
    fn default() -> Self {
        Self {
            segments: MAX_DAG_SEGMENTS,
            edges: MAX_DAG_EDGES,
            depth: MAX_DAG_DEPTH,
            serialized_size: MAX_DAG_SERIALIZED_SIZE,
        }
    }
}

/// Parent-before-child policy for aggregate stream delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum OrderingMode {
    /// Require local proof that every transitive ancestor completed.
    #[default]
    Strict,
    /// Apply local barriers but rely on the broker for ancestors never owned by
    /// this aggregate.
    BrokerManaged,
}

/// Result of evaluating a segment's complete transitive ancestry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderingEligibility {
    /// Every ancestor was locally owned and completed.
    Eligible,
    /// Locally-owned ancestors still have unsettled work.
    Blocked {
        /// Incomplete local ancestors, sorted by id.
        incomplete_ancestors: Vec<SegmentId>,
        /// Remote ancestors delegated to the broker in broker-managed mode.
        broker_managed_ancestors: Vec<SegmentId>,
    },
    /// Delivery is eligible only under broker-managed ancestry semantics.
    BrokerManaged {
        /// Ancestors this aggregate never owned, sorted by id.
        remote_ancestors: Vec<SegmentId>,
    },
}

impl OrderingEligibility {
    /// Whether FLOW and application delivery are permitted.
    #[must_use]
    pub const fn permits_flow(&self) -> bool {
        matches!(self, Self::Eligible | Self::BrokerManaged { .. })
    }
}

/// An ancestry decision that cannot be proven from the validated local graph
/// and ownership history.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OrderingError {
    /// The requested segment is not in the current snapshot.
    #[error("segment {segment_id} is not in the current DAG")]
    UnknownSegment {
        /// Missing segment.
        segment_id: SegmentId,
    },
    /// Strict mode cannot prove remote ancestry complete.
    #[error(
        "ordering for segment {segment_id} is unprovable through remote ancestors {ancestors:?}"
    )]
    OrderingUnprovable {
        /// Descendant being evaluated.
        segment_id: SegmentId,
        /// Remote or pruned ancestry roots preventing proof.
        ancestors: Vec<SegmentId>,
    },
}

/// Full-snapshot validation failure. No variant permits partial installation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DagValidationError {
    /// Encoded input exceeds the fixed control-plane bound.
    #[error("DAG snapshot is {actual} bytes; maximum is {max}")]
    SerializedSize {
        /// Encoded bytes.
        actual: usize,
        /// Configured maximum.
        max: usize,
    },
    /// The snapshot contains too many segment nodes.
    #[error("DAG snapshot has {actual} segments; maximum is {max}")]
    SegmentCount {
        /// Segment count.
        actual: usize,
        /// Configured maximum.
        max: usize,
    },
    /// Placement count is bounded independently before indexing.
    #[error("DAG snapshot has {actual} placements; maximum is {max}")]
    PlacementCount {
        /// Placement count.
        actual: usize,
        /// Configured maximum.
        max: usize,
    },
    /// Segment ids must be unique.
    #[error("DAG snapshot repeats segment id {segment_id}")]
    DuplicateSegment {
        /// Repeated id.
        segment_id: SegmentId,
    },
    /// Placement ids must be unique.
    #[error("DAG snapshot repeats placement id {segment_id}")]
    DuplicatePlacement {
        /// Repeated id.
        segment_id: SegmentId,
    },
    /// A placement cannot name a segment absent from the snapshot.
    #[error("placement names unknown segment {segment_id}")]
    DanglingPlacement {
        /// Unknown id.
        segment_id: SegmentId,
    },
    /// A broker placement URL must not be empty.
    #[error("placement for segment {segment_id} has an empty broker URL")]
    EmptyPlacementUrl {
        /// Segment id.
        segment_id: SegmentId,
    },
    /// Descriptor range, state, placement, or legacy marker is invalid.
    #[error("segment {segment_id} is invalid: {source}")]
    InvalidDescriptor {
        /// Segment id.
        segment_id: SegmentId,
        /// Descriptor validation failure.
        source: SegmentDescriptorError,
    },
    /// A segment cannot be created after the containing layout epoch.
    #[error("segment {segment_id} was created at epoch {created}, after layout epoch {layout}")]
    CreatedAfterLayout {
        /// Segment id.
        segment_id: SegmentId,
        /// Creation epoch.
        created: u64,
        /// Layout epoch.
        layout: u64,
    },
    /// Active segments cannot carry a seal epoch.
    #[error("active segment {segment_id} carries sealed epoch {sealed}")]
    ActiveWithSealEpoch {
        /// Segment id.
        segment_id: SegmentId,
        /// Invalid seal epoch.
        sealed: u64,
    },
    /// Active segments are the current leaves.
    #[error("active segment {segment_id} has children")]
    ActiveWithChildren {
        /// Segment id.
        segment_id: SegmentId,
    },
    /// A sealed segment must state when it sealed.
    #[error("sealed segment {segment_id} has no seal epoch")]
    SealedWithoutEpoch {
        /// Segment id.
        segment_id: SegmentId,
    },
    /// A seal epoch must fall between creation and the snapshot epoch.
    #[error(
        "segment {segment_id} has invalid seal epoch {sealed} for creation {created} and layout {layout}"
    )]
    InvalidSealEpoch {
        /// Segment id.
        segment_id: SegmentId,
        /// Creation epoch.
        created: u64,
        /// Seal epoch.
        sealed: u64,
        /// Layout epoch.
        layout: u64,
    },
    /// Parent ids within a descriptor must be unique.
    #[error("segment {segment_id} repeats parent {parent_id}")]
    DuplicateParent {
        /// Child id.
        segment_id: SegmentId,
        /// Repeated parent.
        parent_id: SegmentId,
    },
    /// Child ids within a descriptor must be unique.
    #[error("segment {segment_id} repeats child {child_id}")]
    DuplicateChild {
        /// Parent id.
        segment_id: SegmentId,
        /// Repeated child.
        child_id: SegmentId,
    },
    /// Self edges are cycles and rejected directly.
    #[error("segment {segment_id} has a self edge")]
    SelfEdge {
        /// Segment id.
        segment_id: SegmentId,
    },
    /// Logical edge count exceeded the configured bound.
    #[error("DAG snapshot has {actual} edges; maximum is {max}")]
    EdgeCount {
        /// Edge count.
        actual: usize,
        /// Configured maximum.
        max: usize,
    },
    /// A parent reference names no node.
    #[error("segment {segment_id} names missing parent {parent_id}")]
    DanglingParent {
        /// Child id.
        segment_id: SegmentId,
        /// Missing parent.
        parent_id: SegmentId,
    },
    /// A child reference names no node.
    #[error("segment {segment_id} names missing child {child_id}")]
    DanglingChild {
        /// Parent id.
        segment_id: SegmentId,
        /// Missing child.
        child_id: SegmentId,
    },
    /// Parent and child descriptors disagree about an edge.
    #[error("edge {parent_id}->{child_id} is not reciprocal")]
    NonReciprocalEdge {
        /// Parent id.
        parent_id: SegmentId,
        /// Child id.
        child_id: SegmentId,
    },
    /// Child creation and parent sealing must describe the same transition.
    #[error(
        "edge {parent_id}->{child_id} has parent seal epoch {sealed} but child creation epoch {created}"
    )]
    EdgeEpochMismatch {
        /// Parent id.
        parent_id: SegmentId,
        /// Child id.
        child_id: SegmentId,
        /// Parent seal epoch.
        sealed: u64,
        /// Child creation epoch.
        created: u64,
    },
    /// The graph contains a cycle.
    #[error("DAG snapshot contains a cycle")]
    Cycle,
    /// Acyclic graph depth exceeded the configured bound.
    #[error("DAG ancestry depth {actual} exceeds maximum {max}")]
    Depth {
        /// Observed depth.
        actual: usize,
        /// Configured maximum.
        max: usize,
    },
    /// One parent cannot simultaneously describe split and merge children.
    #[error("segment {segment_id} has conflicting split/merge topology")]
    ConflictingTopology {
        /// Parent id.
        segment_id: SegmentId,
    },
    /// Split children must exactly partition their parent range.
    #[error("split children do not exactly cover parent segment {segment_id}")]
    InvalidSplitCoverage {
        /// Parent id.
        segment_id: SegmentId,
    },
    /// Merge parents must exactly partition their child range.
    #[error("merge parents do not exactly cover child segment {segment_id}")]
    InvalidMergeCoverage {
        /// Child id.
        segment_id: SegmentId,
    },
    /// At least one active leaf must serve the key space.
    #[error("DAG snapshot has no active segments")]
    NoActiveSegments,
    /// Active leaves overlap or leave a gap before a segment.
    #[error(
        "active key-space coverage expected start {expected}, got {actual} at segment {segment_id}"
    )]
    ActiveCoverageDiscontinuity {
        /// Next required hash.
        expected: u32,
        /// Actual next start.
        actual: u32,
        /// Segment exposing the discontinuity.
        segment_id: SegmentId,
    },
    /// Active leaves did not reach the final M1 hash.
    #[error("active key-space coverage ended at {actual}, expected {expected}")]
    ActiveCoverageEnd {
        /// Last covered hash.
        actual: u32,
        /// Required final hash.
        expected: u32,
    },
}

/// Assignment-to-DAG attachment validation failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AttachmentError {
    /// Assignment and graph must describe one layout generation.
    #[error("assignment epoch {assignment} does not match DAG epoch {dag}")]
    EpochMismatch {
        /// Assignment epoch.
        assignment: u64,
        /// DAG epoch.
        dag: u64,
    },
    /// Assigned segment is absent from the graph.
    #[error("assignment names unknown segment {segment_id}")]
    UnknownSegment {
        /// Missing id.
        segment_id: SegmentId,
    },
    /// Assignment and graph disagree about the inclusive range.
    #[error("assignment range for segment {segment_id} does not match the DAG")]
    RangeMismatch {
        /// Segment id.
        segment_id: SegmentId,
    },
    /// A synthetic legacy node has no `segment://` attachment.
    #[error("legacy segment {segment_id} cannot use a segment attachment")]
    LegacySegment {
        /// Legacy id.
        segment_id: SegmentId,
    },
}

/// Fully validated, immutable M1 DAG snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagSnapshot {
    epoch: u64,
    segments: BTreeMap<SegmentId, SegmentDescriptor>,
    topological_order: Vec<SegmentId>,
    incomplete_roots: BTreeSet<SegmentId>,
}

impl DagSnapshot {
    /// Validate one untrusted wire snapshot under the fixed production bounds.
    ///
    /// # Errors
    ///
    /// Returns [`DagValidationError`] for any malformed topology. Validation is
    /// all-or-nothing.
    pub fn try_from_pb(dag: &pb::ScalableTopicDag) -> Result<Self, DagValidationError> {
        Self::try_from_pb_with_limits(dag, DagLimits::default())
    }

    /// Validate with explicit bounds, primarily for deterministic boundary
    /// testing.
    ///
    /// # Errors
    ///
    /// Returns [`DagValidationError`] before constructing a snapshot.
    pub fn try_from_pb_with_limits(
        dag: &pb::ScalableTopicDag,
        limits: DagLimits,
    ) -> Result<Self, DagValidationError> {
        let encoded_size = dag.encoded_len();
        if encoded_size > limits.serialized_size {
            return Err(DagValidationError::SerializedSize {
                actual: encoded_size,
                max: limits.serialized_size,
            });
        }
        if dag.segments.len() > limits.segments {
            return Err(DagValidationError::SegmentCount {
                actual: dag.segments.len(),
                max: limits.segments,
            });
        }
        if dag.segment_brokers.len() > limits.segments {
            return Err(DagValidationError::PlacementCount {
                actual: dag.segment_brokers.len(),
                max: limits.segments,
            });
        }

        let mut placements = BTreeMap::new();
        for placement in &dag.segment_brokers {
            let segment_id = SegmentId(placement.segment_id);
            if placements.insert(segment_id, placement).is_some() {
                return Err(DagValidationError::DuplicatePlacement { segment_id });
            }
            if placement.broker_url.is_empty() {
                return Err(DagValidationError::EmptyPlacementUrl { segment_id });
            }
        }

        let mut segments = BTreeMap::new();
        let mut parent_references = 0usize;
        let mut child_references = 0usize;
        for info in &dag.segments {
            let segment_id = SegmentId(info.segment_id);
            if segments.contains_key(&segment_id) {
                return Err(DagValidationError::DuplicateSegment { segment_id });
            }
            let descriptor =
                SegmentDescriptor::try_from_pb(info, placements.get(&segment_id).copied())
                    .map_err(|source| DagValidationError::InvalidDescriptor {
                        segment_id,
                        source,
                    })?;
            validate_lifecycle(&descriptor, dag.epoch)?;
            validate_edge_list(&descriptor.parent_ids, segment_id, true)?;
            validate_edge_list(&descriptor.child_ids, segment_id, false)?;
            parent_references = parent_references.saturating_add(descriptor.parent_ids.len());
            child_references = child_references.saturating_add(descriptor.child_ids.len());
            if parent_references > limits.edges || child_references > limits.edges {
                return Err(DagValidationError::EdgeCount {
                    actual: parent_references.max(child_references),
                    max: limits.edges,
                });
            }
            segments.insert(segment_id, descriptor);
        }

        for segment_id in placements.keys() {
            if !segments.contains_key(segment_id) {
                return Err(DagValidationError::DanglingPlacement {
                    segment_id: *segment_id,
                });
            }
        }

        validate_reciprocal_edges(&segments)?;
        let topological_order = validate_acyclic_and_depth(&segments, limits.depth)?;
        validate_range_transitions(&segments)?;
        validate_active_coverage(&segments)?;
        let incomplete_roots = segments
            .values()
            .filter(|segment| segment.parent_ids.is_empty() && segment.created_at_epoch > 0)
            .map(|segment| segment.segment_id)
            .collect();

        Ok(Self {
            epoch: dag.epoch,
            segments,
            topological_order,
            incomplete_roots,
        })
    }

    /// Layout epoch.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Segment descriptor by id.
    #[must_use]
    pub fn segment(&self, segment_id: SegmentId) -> Option<&SegmentDescriptor> {
        self.segments.get(&segment_id)
    }

    /// Deterministic segment-id snapshot.
    #[must_use]
    pub fn segments(&self) -> Vec<SegmentDescriptor> {
        self.segments.values().cloned().collect()
    }

    /// Parent-before-child order computed during validation.
    #[must_use]
    pub fn topological_order(&self) -> &[SegmentId] {
        &self.topological_order
    }

    /// Validate every assignment attachment against this exact layout.
    ///
    /// # Errors
    ///
    /// Returns [`AttachmentError`] for epoch, membership, range, or legacy
    /// mismatches.
    pub fn validate_assignment(
        &self,
        assignment: &ConsumerAssignment,
    ) -> Result<(), AttachmentError> {
        if assignment.layout_epoch() != self.epoch {
            return Err(AttachmentError::EpochMismatch {
                assignment: assignment.layout_epoch(),
                dag: self.epoch,
            });
        }
        for assigned in assignment.segments() {
            let segment_id = assigned.segment_id();
            let Some(descriptor) = self.segments.get(&segment_id) else {
                return Err(AttachmentError::UnknownSegment { segment_id });
            };
            if descriptor.key_range != assigned.key_range() {
                return Err(AttachmentError::RangeMismatch { segment_id });
            }
            if descriptor.is_legacy() {
                return Err(AttachmentError::LegacySegment { segment_id });
            }
        }
        Ok(())
    }

    /// Evaluate all transitive ancestors for one assigned segment.
    ///
    /// # Errors
    ///
    /// Returns [`OrderingError::OrderingUnprovable`] for pruned ancestry in
    /// both modes and for remote ancestry in strict mode.
    pub fn ordering_eligibility(
        &self,
        segment_id: SegmentId,
        mode: OrderingMode,
        local_ownership_history: &BTreeSet<SegmentId>,
        completed: &BTreeSet<SegmentId>,
    ) -> Result<OrderingEligibility, OrderingError> {
        if !self.segments.contains_key(&segment_id) {
            return Err(OrderingError::UnknownSegment { segment_id });
        }
        let ancestors = self.ancestors(segment_id);
        let mut unprovable_roots: Vec<SegmentId> = ancestors
            .iter()
            .copied()
            .chain(core::iter::once(segment_id))
            .filter(|id| self.incomplete_roots.contains(id))
            .collect();
        unprovable_roots.sort_unstable();
        unprovable_roots.dedup();
        if !unprovable_roots.is_empty() {
            return Err(OrderingError::OrderingUnprovable {
                segment_id,
                ancestors: unprovable_roots,
            });
        }

        let incomplete: Vec<SegmentId> = ancestors
            .iter()
            .copied()
            .filter(|id| local_ownership_history.contains(id) && !completed.contains(id))
            .collect();
        let remote: Vec<SegmentId> = ancestors
            .iter()
            .copied()
            .filter(|id| !local_ownership_history.contains(id))
            .collect();
        if mode == OrderingMode::Strict && !remote.is_empty() {
            return Err(OrderingError::OrderingUnprovable {
                segment_id,
                ancestors: remote,
            });
        }
        if !incomplete.is_empty() {
            return Ok(OrderingEligibility::Blocked {
                incomplete_ancestors: incomplete,
                broker_managed_ancestors: remote,
            });
        }
        if remote.is_empty() {
            Ok(OrderingEligibility::Eligible)
        } else {
            Ok(OrderingEligibility::BrokerManaged {
                remote_ancestors: remote,
            })
        }
    }

    fn ancestors(&self, segment_id: SegmentId) -> Vec<SegmentId> {
        let mut pending: Vec<SegmentId> = self
            .segments
            .get(&segment_id)
            .map_or_else(Vec::new, |segment| segment.parent_ids.clone());
        let mut ancestors = BTreeSet::new();
        while let Some(parent_id) = pending.pop() {
            if ancestors.insert(parent_id)
                && let Some(parent) = self.segments.get(&parent_id)
            {
                pending.extend(parent.parent_ids.iter().copied());
            }
        }
        ancestors.into_iter().collect()
    }
}

fn validate_lifecycle(
    segment: &SegmentDescriptor,
    layout_epoch: u64,
) -> Result<(), DagValidationError> {
    if segment.created_at_epoch > layout_epoch {
        return Err(DagValidationError::CreatedAfterLayout {
            segment_id: segment.segment_id,
            created: segment.created_at_epoch,
            layout: layout_epoch,
        });
    }
    match segment.state {
        SegmentState::Active => {
            if let Some(sealed) = segment.sealed_at_epoch {
                return Err(DagValidationError::ActiveWithSealEpoch {
                    segment_id: segment.segment_id,
                    sealed,
                });
            }
            if !segment.child_ids.is_empty() {
                return Err(DagValidationError::ActiveWithChildren {
                    segment_id: segment.segment_id,
                });
            }
        }
        SegmentState::Sealed => {
            let Some(sealed) = segment.sealed_at_epoch else {
                return Err(DagValidationError::SealedWithoutEpoch {
                    segment_id: segment.segment_id,
                });
            };
            if sealed < segment.created_at_epoch || sealed > layout_epoch {
                return Err(DagValidationError::InvalidSealEpoch {
                    segment_id: segment.segment_id,
                    created: segment.created_at_epoch,
                    sealed,
                    layout: layout_epoch,
                });
            }
        }
    }
    Ok(())
}

fn validate_edge_list(
    ids: &[SegmentId],
    segment_id: SegmentId,
    parents: bool,
) -> Result<(), DagValidationError> {
    let mut unique = BTreeSet::new();
    for id in ids {
        if *id == segment_id {
            return Err(DagValidationError::SelfEdge { segment_id });
        }
        if !unique.insert(*id) {
            return Err(if parents {
                DagValidationError::DuplicateParent {
                    segment_id,
                    parent_id: *id,
                }
            } else {
                DagValidationError::DuplicateChild {
                    segment_id,
                    child_id: *id,
                }
            });
        }
    }
    Ok(())
}

fn validate_reciprocal_edges(
    segments: &BTreeMap<SegmentId, SegmentDescriptor>,
) -> Result<(), DagValidationError> {
    for segment in segments.values() {
        for parent_id in &segment.parent_ids {
            let Some(parent) = segments.get(parent_id) else {
                return Err(DagValidationError::DanglingParent {
                    segment_id: segment.segment_id,
                    parent_id: *parent_id,
                });
            };
            if !parent.child_ids.contains(&segment.segment_id) {
                return Err(DagValidationError::NonReciprocalEdge {
                    parent_id: *parent_id,
                    child_id: segment.segment_id,
                });
            }
            let sealed = parent.sealed_at_epoch.unwrap_or_default();
            if parent.sealed_at_epoch != Some(segment.created_at_epoch) {
                return Err(DagValidationError::EdgeEpochMismatch {
                    parent_id: *parent_id,
                    child_id: segment.segment_id,
                    sealed,
                    created: segment.created_at_epoch,
                });
            }
        }
        for child_id in &segment.child_ids {
            let Some(child) = segments.get(child_id) else {
                return Err(DagValidationError::DanglingChild {
                    segment_id: segment.segment_id,
                    child_id: *child_id,
                });
            };
            if !child.parent_ids.contains(&segment.segment_id) {
                return Err(DagValidationError::NonReciprocalEdge {
                    parent_id: segment.segment_id,
                    child_id: *child_id,
                });
            }
        }
    }
    Ok(())
}

fn validate_acyclic_and_depth(
    segments: &BTreeMap<SegmentId, SegmentDescriptor>,
    max_depth: usize,
) -> Result<Vec<SegmentId>, DagValidationError> {
    let mut indegree: BTreeMap<SegmentId, usize> = segments
        .iter()
        .map(|(id, segment)| (*id, segment.parent_ids.len()))
        .collect();
    let mut ready: BTreeSet<SegmentId> = indegree
        .iter()
        .filter(|(_, degree)| **degree == 0)
        .map(|(id, _)| *id)
        .collect();
    let mut depth: BTreeMap<SegmentId, usize> = ready.iter().map(|id| (*id, 0)).collect();
    let mut order = Vec::with_capacity(segments.len());
    while let Some(id) = ready.pop_first() {
        order.push(id);
        let parent_depth = depth.get(&id).copied().unwrap_or_default();
        let children = segments
            .get(&id)
            .map_or(&[][..], |segment| segment.child_ids.as_slice());
        for child_id in children {
            let child_depth = parent_depth.saturating_add(1);
            let entry = depth.entry(*child_id).or_default();
            *entry = (*entry).max(child_depth);
            if *entry > max_depth {
                return Err(DagValidationError::Depth {
                    actual: *entry,
                    max: max_depth,
                });
            }
            let degree = indegree.entry(*child_id).or_default();
            *degree = degree.saturating_sub(1);
            if *degree == 0 {
                ready.insert(*child_id);
            }
        }
    }
    if order.len() != segments.len() {
        return Err(DagValidationError::Cycle);
    }
    Ok(order)
}

fn validate_range_transitions(
    segments: &BTreeMap<SegmentId, SegmentDescriptor>,
) -> Result<(), DagValidationError> {
    for parent in segments
        .values()
        .filter(|segment| !segment.child_ids.is_empty())
    {
        let children: Vec<&SegmentDescriptor> = parent
            .child_ids
            .iter()
            .filter_map(|id| segments.get(id))
            .collect();
        let split = children.iter().all(|child| child.parent_ids.len() == 1);
        let merge = children.len() == 1 && children[0].parent_ids.len() > 1;
        if split {
            if !ranges_cover(
                parent.key_range,
                children.iter().map(|child| child.key_range),
            ) {
                return Err(DagValidationError::InvalidSplitCoverage {
                    segment_id: parent.segment_id,
                });
            }
        } else if !merge {
            return Err(DagValidationError::ConflictingTopology {
                segment_id: parent.segment_id,
            });
        }
    }
    for child in segments
        .values()
        .filter(|segment| segment.parent_ids.len() > 1)
    {
        if !ranges_cover(
            child.key_range,
            child
                .parent_ids
                .iter()
                .filter_map(|id| segments.get(id))
                .map(|parent| parent.key_range),
        ) {
            return Err(DagValidationError::InvalidMergeCoverage {
                segment_id: child.segment_id,
            });
        }
    }
    Ok(())
}

fn ranges_cover(target: KeyRange, ranges: impl Iterator<Item = KeyRange>) -> bool {
    let mut ranges: Vec<KeyRange> = ranges.collect();
    ranges.sort_unstable();
    let mut expected = target.start();
    for (index, range) in ranges.iter().copied().enumerate() {
        if !target.contains_range(range) || range.start() != expected {
            return false;
        }
        if range.end() == target.end() {
            return index + 1 == ranges.len();
        }
        expected = range.end() + 1;
    }
    false
}

fn validate_active_coverage(
    segments: &BTreeMap<SegmentId, SegmentDescriptor>,
) -> Result<(), DagValidationError> {
    let mut active: Vec<&SegmentDescriptor> = segments
        .values()
        .filter(|segment| segment.state == SegmentState::Active)
        .collect();
    if active.is_empty() {
        return Err(DagValidationError::NoActiveSegments);
    }
    active.sort_by_key(|segment| (segment.key_range, segment.segment_id));
    let mut expected = MIN_HASH;
    let mut complete = false;
    for segment in active {
        if complete {
            return Err(DagValidationError::ActiveCoverageDiscontinuity {
                expected: MAX_HASH,
                actual: segment.key_range.start(),
                segment_id: segment.segment_id,
            });
        }
        if segment.key_range.start() != expected {
            return Err(DagValidationError::ActiveCoverageDiscontinuity {
                expected,
                actual: segment.key_range.start(),
                segment_id: segment.segment_id,
            });
        }
        if segment.key_range.end() == MAX_HASH {
            complete = true;
        } else {
            expected = segment.key_range.end() + 1;
        }
    }
    if !complete {
        return Err(DagValidationError::ActiveCoverageEnd {
            actual: expected.saturating_sub(1),
            expected: MAX_HASH,
        });
    }
    Ok(())
}

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
    // NOTE: there is deliberately no `NonMonotonic` variant. A layout `epoch`
    // that does not advance means the broker re-sent a snapshot this session
    // already holds, which is idempotent rather than illegal —
    // `ScalableTopicSession::handle_update` returns `Ok(None)` and leaves the
    // state untouched. It was an error until 2026-08-04, and because the error
    // path closes the session, Pulsar 5.0.0-M1's duplicate initial snapshot
    // silently blinded the client to every subsequent layout change.
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

    /// The replacement snapshot failed complete structural validation.
    #[error("invalid scalable-topic DAG snapshot: {0}")]
    InvalidSnapshot(#[from] DagValidationError),
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
    /// A reconnect attempted an invalid consumer-incarnation transition.
    #[error(transparent)]
    Assignment(#[from] AssignmentError),
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
    /// Canonical `topic://...` identity the broker resolved the request to,
    /// once an update has carried one.
    resolved_topic_name: Option<String>,
    /// Controller-broker URL from the most recent layout, when advertised.
    controller_broker_url: Option<String>,
    /// TLS controller-broker URL from the most recent layout, when advertised.
    controller_broker_url_tls: Option<String>,
    /// Current fully validated DAG. `None` before the first successful update.
    dag: Option<DagSnapshot>,
}

impl DagWatchSession {
    /// Open an empty session for `session_id`. The layout arrives with the
    /// first [`Self::handle_update`].
    #[must_use]
    pub fn new(session_id: u64) -> Self {
        Self {
            session_id,
            resolved_topic_name: None,
            controller_broker_url: None,
            controller_broker_url_tls: None,
            dag: None,
        }
    }

    /// `true` once a layout has landed.
    #[must_use]
    pub fn is_resolved(&self) -> bool {
        self.dag.is_some()
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

    /// TLS controller-broker URL from the most recent layout, if advertised.
    #[must_use]
    pub fn controller_broker_url_tls(&self) -> Option<&str> {
        self.controller_broker_url_tls.as_deref()
    }

    /// Current validated snapshot.
    #[must_use]
    pub fn validated_snapshot(&self) -> Option<&DagSnapshot> {
        self.dag.as_ref()
    }

    /// Snapshot of the current DAG, ordered by segment id.
    #[must_use]
    pub fn snapshot(&self) -> Vec<SegmentDescriptor> {
        self.dag
            .as_ref()
            .map_or_else(Vec::new, DagSnapshot::segments)
    }

    /// `true` when `segment_id` is currently part of the DAG.
    #[must_use]
    pub fn contains(&self, segment_id: SegmentId) -> bool {
        self.dag
            .as_ref()
            .is_some_and(|dag| dag.segment(segment_id).is_some())
    }

    /// Apply a `CommandScalableTopicUpdate`, replacing the layout and
    /// returning the [`DagDelta`] for the runtime to translate into events.
    ///
    /// Returns `Ok(None)` when the update's layout `epoch` does not advance the
    /// session's counter. That is a **duplicate or stale snapshot, not an
    /// error**: the epoch is monotonic but nothing forbids the broker from
    /// re-sending the layout it already sent, and Pulsar 5.0.0-M1 does exactly
    /// that — it answers the lookup with the current layout and then pushes the
    /// same layout again at the same epoch on the newly-opened watch. The state
    /// is left untouched and the caller emits no event.
    ///
    /// Treating that as fatal is what made `e2e_scalable_topic_drops_on_broker_split`
    /// fail against a real broker: the duplicate closed the session, and the
    /// client never saw the epochs that carried the actual split.
    ///
    /// # Errors
    ///
    /// - [`DagError::SessionMismatch`] if the update targets a different session.
    /// - [`DagError::Broker`] if the update carries a `ServerError` instead of a layout.
    /// - [`DagError::Empty`] if it carries neither.
    ///
    /// On any error the session state is left **unchanged** — the update is
    /// validated fully before any mutation lands.
    pub fn handle_update(
        &mut self,
        upd: &pb::CommandScalableTopicUpdate,
    ) -> Result<Option<DagDelta>, DagError> {
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

        // Not an error: a re-sent or reordered snapshot carries a layout this
        // session has already applied, so there is nothing to do and nothing to
        // report. Only a *forward* epoch changes state.
        if let Some(prev) = self.dag.as_ref().map(DagSnapshot::epoch)
            && dag.epoch <= prev
        {
            return Ok(None);
        }

        // Validate the complete replacement before deriving a delta or mutating
        // any session field. A malformed snapshot cannot partially land.
        let incoming = DagSnapshot::try_from_pb(dag)?;

        let before: BTreeSet<SegmentId> =
            self.dag.as_ref().map_or_else(BTreeSet::new, |snapshot| {
                snapshot.segments.keys().copied().collect()
            });
        let after: BTreeSet<SegmentId> = incoming.segments.keys().copied().collect();

        let added: Vec<SegmentDescriptor> = after
            .difference(&before)
            .filter_map(|id| incoming.segments.get(id).cloned())
            .collect();
        let removed: Vec<SegmentId> = before.difference(&after).copied().collect();

        let (split_events, merge_events) = derive_topology_changes(&added, &before);

        self.dag = Some(incoming);
        if let Some(name) = upd.resolved_topic_name.as_ref() {
            self.resolved_topic_name = Some(name.clone());
        }
        self.controller_broker_url = dag.controller_broker_url.clone();
        self.controller_broker_url_tls = dag.controller_broker_url_tls.clone();

        Ok(Some(DagDelta {
            epoch: dag.epoch,
            added,
            removed,
            split_events,
            merge_events,
        }))
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

    fn sealed_info(
        id: u64,
        start: u32,
        end: u32,
        parents: &[u64],
        children: &[u64],
        created: u64,
        sealed: u64,
    ) -> pb::SegmentInfoProto {
        pb::SegmentInfoProto {
            segment_id: id,
            hash_start: start,
            hash_end: end,
            state: pb::SegmentState::Sealed as i32,
            parent_ids: parents.to_vec(),
            child_ids: children.to_vec(),
            created_at_epoch: created,
            sealed_at_epoch: Some(sealed),
            created_at_ms: 0,
            sealed_at_ms: Some(0),
            legacy_topic_name: None,
        }
    }

    fn child_info(
        id: u64,
        start: u32,
        end: u32,
        parents: &[u64],
        created: u64,
    ) -> pb::SegmentInfoProto {
        let mut child = info(id, start, end, parents, &[]);
        child.created_at_epoch = created;
        child
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
                controller_broker_url_tls: Some("pulsar+ssl://controller:6651".to_owned()),
            }),
            error: None,
            message: None,
            resolved_topic_name: Some("topic://public/default/scaled".to_owned()),
        }
    }

    fn resolved_session() -> DagWatchSession {
        let mut s = DagWatchSession::new(7);
        s.handle_update(&update(7, 1, vec![info(1, 0, 65_535, &[], &[])]))
            .expect("initial layout applies")
            .expect("the first layout yields a delta");
        s
    }

    /// Layer (a) test: the first update resolves the session, carries the
    /// canonical topic identity, and installs the layout.
    #[test]
    fn scalable_session_first_update_resolves_layout() {
        let mut s = DagWatchSession::new(7);
        assert!(!s.is_resolved());

        let delta = s
            .handle_update(&update(
                7,
                4,
                vec![
                    info(1, 0, 32_767, &[], &[]),
                    info(2, 32_768, 65_535, &[], &[]),
                ],
            ))
            .expect("layout applies")
            .expect("the first layout yields a delta");

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
        assert_eq!(
            s.controller_broker_url_tls(),
            Some("pulsar+ssl://controller:6651")
        );
        // Placement is joined onto the descriptor from the parallel address list.
        assert_eq!(
            s.snapshot()[0].broker_url.as_deref(),
            Some("pulsar://seg1:6650")
        );
    }

    #[test]
    fn scalable_session_preserves_absent_controller_authority() {
        let mut update = update(7, 1, vec![info(1, 0, 65_535, &[], &[])]);
        let dag = update.dag.as_mut().expect("layout");
        dag.controller_broker_url = None;
        dag.controller_broker_url_tls = None;
        let mut session = DagWatchSession::new(7);

        session
            .handle_update(&update)
            .expect("layout applies")
            .expect("initial layout yields a delta");

        assert_eq!(session.controller_broker_url(), None);
        assert_eq!(session.controller_broker_url_tls(), None);
        assert_eq!(
            session.snapshot()[0].broker_url.as_deref(),
            Some("pulsar://seg1:6650")
        );
    }

    /// Layer (a) test: a non-advancing layout epoch is ignored — no delta, no
    /// state change, and crucially the session survives.
    ///
    /// Pulsar 5.0.0-M1 answers the lookup with the current layout and then
    /// pushes that same layout again at the same epoch on the watch it just
    /// opened. This was `DagError::NonMonotonic` until 2026-08-04, and since
    /// the caller closes the session on any error, that duplicate blinded the
    /// client to every later epoch — including the one carrying a split.
    #[test]
    fn scalable_session_ignores_non_advancing_epoch_and_survives() {
        let mut s = resolved_session();
        assert!(s.is_resolved());

        // Equal epoch: the duplicate a real broker sends.
        let duplicate = update(7, 1, vec![info(9, 0, 65_536, &[], &[])]);
        assert_eq!(
            s.handle_update(&duplicate),
            Ok(None),
            "a re-sent snapshot is idempotent, not an error"
        );
        // Strictly older: a reordered frame, treated the same way.
        let stale = update(7, 0, vec![info(9, 0, 65_536, &[], &[])]);
        assert_eq!(
            s.handle_update(&stale),
            Ok(None),
            "a stale frame is ignored"
        );

        // Session unchanged — segment 9 never landed, segment 1 still there.
        assert!(!s.contains(SegmentId(9)));
        assert!(s.contains(SegmentId(1)));
        assert!(s.is_resolved());

        // And the session still accepts the next real advance.
        let advance = update(7, 2, vec![info(9, 0, 65_535, &[], &[])]);
        let delta = s
            .handle_update(&advance)
            .expect("a forward epoch applies")
            .expect("a forward epoch yields a delta");
        assert_eq!(delta.epoch, 2);
        assert!(s.contains(SegmentId(9)));
    }

    /// Layer (a) test: a split is derived from reciprocal parent/child edges.
    #[test]
    fn scalable_session_derives_split_from_edges() {
        let mut s = resolved_session();
        let delta = s
            .handle_update(&update(
                7,
                2,
                vec![
                    sealed_info(1, 0, 65_535, &[], &[2, 3], 0, 2),
                    child_info(2, 0, 32_767, &[1], 2),
                    child_info(3, 32_768, 65_535, &[1], 2),
                ],
            ))
            .expect("split layout applies")
            .expect("a split yields a delta");

        assert!(delta.is_consume_affecting());
        assert_eq!(delta.change_reason(), DagChangeReason::Split);
        assert!(delta.removed.is_empty());
        assert_eq!(delta.split_events.len(), 1, "1 -> {{2,3}} is one split");
        assert_eq!(delta.split_events[0].parent_segment_id, SegmentId(1));
        assert_eq!(
            delta.split_events[0].child_segment_ids,
            vec![SegmentId(2), SegmentId(3)]
        );
        assert!(s.contains(SegmentId(1)), "sealed parent remains a barrier");
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
                info(5, 0, 32_767, &[], &[]),
                info(6, 32_768, 65_535, &[], &[]),
            ],
        ))
        .expect("initial layout")
        .expect("the first layout yields a delta");

        let delta = s
            .handle_update(&update(
                7,
                2,
                vec![
                    sealed_info(5, 0, 32_767, &[], &[7], 0, 2),
                    sealed_info(6, 32_768, 65_535, &[], &[7], 0, 2),
                    child_info(7, 0, 65_535, &[5, 6], 2),
                ],
            ))
            .expect("merge layout applies")
            .expect("a merge yields a delta");

        assert!(delta.is_consume_affecting());
        assert_eq!(delta.change_reason(), DagChangeReason::Merge);
        assert_eq!(delta.merge_events.len(), 1);
        assert_eq!(
            delta.merge_events[0].parent_segment_ids,
            vec![SegmentId(5), SegmentId(6)]
        );
        assert_eq!(delta.merge_events[0].child_segment_id, SegmentId(7));
        assert!(delta.removed.is_empty());
        assert_eq!(s.snapshot().len(), 3);
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
                info(5, 0, 32_767, &[], &[]),
                info(6, 32_768, 65_535, &[], &[]),
            ],
        ))
        .expect("initial layout")
        .expect("the first layout yields a delta");

        // Descending on the wire.
        let delta = s
            .handle_update(&update(
                7,
                2,
                vec![
                    sealed_info(5, 0, 32_767, &[], &[7], 0, 2),
                    sealed_info(6, 32_768, 65_535, &[], &[7], 0, 2),
                    child_info(7, 0, 65_535, &[6, 5], 2),
                ],
            ))
            .expect("merge layout applies")
            .expect("a merge yields a delta");

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

    /// Adding a detached sealed history node is not consume-affecting.
    #[test]
    fn scalable_session_add_only_is_benign() {
        let mut s = resolved_session();
        let added_history = sealed_info(2, 0, 65_535, &[], &[], 0, 1);
        let delta = s
            .handle_update(&update(
                7,
                2,
                vec![info(1, 0, 65_535, &[], &[]), added_history],
            ))
            .expect("add applies")
            .expect("an add yields a delta");
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
        let mut legacy = info(0, 0, 65_535, &[], &[]);
        legacy.legacy_topic_name = Some("persistent://public/default/plain".to_owned());
        s.handle_update(&update(7, 0, vec![legacy]))
            .expect("legacy layout applies")
            .expect("the legacy layout yields a delta");

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
        assert_eq!(snap[0].key_range, KeyRange::FULL);
    }

    fn dag_from_segments(epoch: u64, segments: Vec<pb::SegmentInfoProto>) -> pb::ScalableTopicDag {
        update(7, epoch, segments).dag.expect("test update has DAG")
    }

    fn valid_split_dag() -> pb::ScalableTopicDag {
        dag_from_segments(
            1,
            vec![
                sealed_info(0, 0, 65_535, &[], &[1, 2], 0, 1),
                child_info(1, 0, 32_767, &[0], 1),
                child_info(2, 32_768, 65_535, &[0], 1),
            ],
        )
    }

    fn valid_merge_dag() -> pb::ScalableTopicDag {
        dag_from_segments(
            2,
            vec![
                sealed_info(0, 0, 32_767, &[], &[2], 0, 2),
                sealed_info(1, 32_768, 65_535, &[], &[2], 0, 2),
                child_info(2, 0, 65_535, &[0, 1], 2),
            ],
        )
    }

    fn assert_invalid(
        dag: &pb::ScalableTopicDag,
        predicate: impl FnOnce(&DagValidationError) -> bool,
    ) {
        let error = DagSnapshot::try_from_pb(dag).expect_err("snapshot must be rejected");
        assert!(predicate(&error), "unexpected validation error: {error:?}");
    }

    #[test]
    fn dag_validation_rejects_duplicate_and_dangling_identities() {
        let mut duplicate_segment = valid_split_dag();
        duplicate_segment
            .segments
            .push(duplicate_segment.segments[0].clone());
        assert_invalid(&duplicate_segment, |error| {
            matches!(error, DagValidationError::DuplicateSegment { .. })
        });

        let mut duplicate_placement = valid_split_dag();
        duplicate_placement
            .segment_brokers
            .push(duplicate_placement.segment_brokers[0].clone());
        assert_invalid(&duplicate_placement, |error| {
            matches!(error, DagValidationError::DuplicatePlacement { .. })
        });

        let mut dangling_placement = valid_split_dag();
        dangling_placement.segment_brokers.push(address(999));
        assert_invalid(&dangling_placement, |error| {
            matches!(error, DagValidationError::DanglingPlacement { .. })
        });

        let mut empty_placement = valid_split_dag();
        empty_placement.segment_brokers[0].broker_url.clear();
        assert_invalid(&empty_placement, |error| {
            matches!(error, DagValidationError::EmptyPlacementUrl { .. })
        });
    }

    #[test]
    fn dag_validation_rejects_every_edge_inconsistency_and_cycles() {
        let mut duplicate_parent = valid_split_dag();
        duplicate_parent.segments[1].parent_ids.push(0);
        assert_invalid(&duplicate_parent, |error| {
            matches!(error, DagValidationError::DuplicateParent { .. })
        });

        let mut duplicate_child = valid_split_dag();
        duplicate_child.segments[0].child_ids.push(1);
        assert_invalid(&duplicate_child, |error| {
            matches!(error, DagValidationError::DuplicateChild { .. })
        });

        let mut self_edge = valid_split_dag();
        self_edge.segments[1].parent_ids = vec![1];
        assert_invalid(&self_edge, |error| {
            matches!(error, DagValidationError::SelfEdge { .. })
        });

        let mut dangling_parent = valid_split_dag();
        dangling_parent.segments[0]
            .child_ids
            .retain(|child| *child != 1);
        dangling_parent.segments[1].parent_ids = vec![999];
        assert_invalid(&dangling_parent, |error| {
            matches!(error, DagValidationError::DanglingParent { .. })
        });

        let mut dangling_child = valid_split_dag();
        dangling_child.segments[0].child_ids.push(999);
        assert_invalid(&dangling_child, |error| {
            matches!(error, DagValidationError::DanglingChild { .. })
        });

        let mut non_reciprocal = valid_split_dag();
        non_reciprocal.segments[1].parent_ids.clear();
        assert_invalid(&non_reciprocal, |error| {
            matches!(error, DagValidationError::NonReciprocalEdge { .. })
        });

        let mut epoch_mismatch = valid_split_dag();
        epoch_mismatch.segments[1].created_at_epoch = 0;
        assert_invalid(&epoch_mismatch, |error| {
            matches!(error, DagValidationError::EdgeEpochMismatch { .. })
        });

        let cycle = dag_from_segments(
            1,
            vec![
                info(0, 0, 65_535, &[], &[]),
                sealed_info(10, 0, 65_535, &[11], &[11], 1, 1),
                sealed_info(11, 0, 65_535, &[10], &[10], 1, 1),
            ],
        );
        assert_invalid(&cycle, |error| matches!(error, DagValidationError::Cycle));
    }

    #[test]
    fn dag_validation_rejects_invalid_states_epochs_and_descriptors() {
        let mut invalid_range = dag_from_segments(0, vec![info(0, 0, 65_535, &[], &[])]);
        invalid_range.segments[0].hash_end = 65_536;
        assert_invalid(&invalid_range, |error| {
            matches!(error, DagValidationError::InvalidDescriptor { .. })
        });

        let mut unknown_state = dag_from_segments(0, vec![info(0, 0, 65_535, &[], &[])]);
        unknown_state.segments[0].state = 99;
        assert_invalid(&unknown_state, |error| {
            matches!(error, DagValidationError::InvalidDescriptor { .. })
        });

        let mut empty_legacy = dag_from_segments(0, vec![info(0, 0, 65_535, &[], &[])]);
        empty_legacy.segments[0].legacy_topic_name = Some(String::new());
        assert_invalid(&empty_legacy, |error| {
            matches!(error, DagValidationError::InvalidDescriptor { .. })
        });

        let created_after = dag_from_segments(0, vec![child_info(0, 0, 65_535, &[], 1)]);
        assert_invalid(&created_after, |error| {
            matches!(error, DagValidationError::CreatedAfterLayout { .. })
        });

        let mut active_with_seal = dag_from_segments(1, vec![info(0, 0, 65_535, &[], &[])]);
        active_with_seal.segments[0].sealed_at_epoch = Some(1);
        assert_invalid(&active_with_seal, |error| {
            matches!(error, DagValidationError::ActiveWithSealEpoch { .. })
        });

        let mut active_with_children = dag_from_segments(1, vec![info(0, 0, 65_535, &[], &[])]);
        active_with_children.segments[0].child_ids.push(1);
        assert_invalid(&active_with_children, |error| {
            matches!(error, DagValidationError::ActiveWithChildren { .. })
        });

        let mut sealed_without_epoch = dag_from_segments(1, vec![info(0, 0, 65_535, &[], &[])]);
        sealed_without_epoch.segments[0].state = pb::SegmentState::Sealed as i32;
        assert_invalid(&sealed_without_epoch, |error| {
            matches!(error, DagValidationError::SealedWithoutEpoch { .. })
        });

        let invalid_seal = dag_from_segments(2, vec![sealed_info(0, 0, 65_535, &[], &[], 2, 1)]);
        assert_invalid(&invalid_seal, |error| {
            matches!(error, DagValidationError::InvalidSealEpoch { .. })
        });
    }

    #[test]
    fn dag_validation_enforces_bounds_without_large_fixtures() {
        let dag = valid_split_dag();
        let encoded_size = dag.encoded_len();

        let error = DagSnapshot::try_from_pb_with_limits(
            &dag,
            DagLimits {
                serialized_size: encoded_size - 1,
                ..DagLimits::default()
            },
        )
        .expect_err("serialized size bound");
        assert!(matches!(error, DagValidationError::SerializedSize { .. }));

        for (limits, expected) in [
            (
                DagLimits {
                    segments: 2,
                    ..DagLimits::default()
                },
                "segments",
            ),
            (
                DagLimits {
                    edges: 0,
                    ..DagLimits::default()
                },
                "edges",
            ),
            (
                DagLimits {
                    depth: 0,
                    ..DagLimits::default()
                },
                "depth",
            ),
        ] {
            let error =
                DagSnapshot::try_from_pb_with_limits(&dag, limits).expect_err("bound must reject");
            assert_eq!(
                match error {
                    DagValidationError::SegmentCount { .. } => "segments",
                    DagValidationError::EdgeCount { .. } => "edges",
                    DagValidationError::Depth { .. } => "depth",
                    other => panic!("unexpected bounds error: {other:?}"),
                },
                expected
            );
        }

        let error = DagSnapshot::try_from_pb_with_limits(
            &dag,
            DagLimits {
                segments: 3,
                ..DagLimits::default()
            },
        )
        .expect("three segments and placements fit");
        assert_eq!(error.segments().len(), 3);

        let mut placement_heavy = dag.clone();
        placement_heavy.segment_brokers.push(address(99));
        let error = DagSnapshot::try_from_pb_with_limits(
            &placement_heavy,
            DagLimits {
                segments: 3,
                ..DagLimits::default()
            },
        )
        .expect_err("placement count checked before dangling id");
        assert!(matches!(error, DagValidationError::PlacementCount { .. }));
    }

    #[test]
    fn dag_validation_enforces_split_merge_and_active_leaf_coverage() {
        let mut invalid_split = valid_split_dag();
        invalid_split.segments[1].hash_end = 32_766;
        assert_invalid(&invalid_split, |error| {
            matches!(error, DagValidationError::InvalidSplitCoverage { .. })
        });

        let mut invalid_merge = valid_merge_dag();
        invalid_merge.segments[2].hash_end = 65_534;
        assert_invalid(&invalid_merge, |error| {
            matches!(error, DagValidationError::InvalidMergeCoverage { .. })
        });

        let conflicting = dag_from_segments(
            1,
            vec![
                sealed_info(0, 0, 32_767, &[], &[2, 3], 0, 1),
                sealed_info(1, 32_768, 65_535, &[], &[3], 0, 1),
                child_info(2, 0, 16_383, &[0], 1),
                child_info(3, 16_384, 65_535, &[0, 1], 1),
            ],
        );
        assert_invalid(&conflicting, |error| {
            matches!(error, DagValidationError::ConflictingTopology { .. })
        });

        let gap = dag_from_segments(
            0,
            vec![
                info(0, 0, 32_766, &[], &[]),
                info(1, 32_768, 65_535, &[], &[]),
            ],
        );
        assert_invalid(&gap, |error| {
            matches!(
                error,
                DagValidationError::ActiveCoverageDiscontinuity { .. }
            )
        });

        let overlap = dag_from_segments(
            0,
            vec![
                info(0, 0, 32_768, &[], &[]),
                info(1, 32_768, 65_535, &[], &[]),
            ],
        );
        assert_invalid(&overlap, |error| {
            matches!(
                error,
                DagValidationError::ActiveCoverageDiscontinuity { .. }
            )
        });

        let short = dag_from_segments(0, vec![info(0, 0, 65_534, &[], &[])]);
        assert_invalid(&short, |error| {
            matches!(error, DagValidationError::ActiveCoverageEnd { .. })
        });

        let none = dag_from_segments(1, vec![sealed_info(0, 0, 65_535, &[], &[], 0, 1)]);
        assert_invalid(&none, |error| {
            matches!(error, DagValidationError::NoActiveSegments)
        });
    }

    fn assignment(epoch: u64, id: u64, start: u32, end: u32) -> ConsumerAssignment {
        let range = KeyRange::new(start, end).expect("test assignment range");
        ConsumerAssignment::try_from_pb(
            &pb::ScalableConsumerAssignment {
                layout_epoch: epoch,
                segments: vec![pb::ScalableAssignedSegment {
                    segment_id: id,
                    hash_start: start,
                    hash_end: end,
                    segment_topic: crate::canonical_segment_topic(
                        "topic://public/default/scaled",
                        range,
                        SegmentId(id),
                    )
                    .expect("canonical attachment"),
                }],
            },
            "topic://public/default/scaled",
        )
        .expect("valid assignment value")
    }

    #[test]
    fn dag_attachment_validation_covers_epoch_membership_range_and_legacy() {
        let snapshot = DagSnapshot::try_from_pb(&valid_split_dag()).expect("snapshot");
        assert_eq!(
            snapshot.validate_assignment(&assignment(2, 1, 0, 32_767)),
            Err(AttachmentError::EpochMismatch {
                assignment: 2,
                dag: 1,
            })
        );
        assert_eq!(
            snapshot.validate_assignment(&assignment(1, 99, 0, 65_535)),
            Err(AttachmentError::UnknownSegment {
                segment_id: SegmentId(99),
            })
        );
        assert_eq!(
            snapshot.validate_assignment(&assignment(1, 1, 0, 32_768)),
            Err(AttachmentError::RangeMismatch {
                segment_id: SegmentId(1),
            })
        );

        let mut legacy = info(0, 0, 65_535, &[], &[]);
        legacy.legacy_topic_name = Some("persistent://public/default/plain".to_owned());
        let legacy =
            DagSnapshot::try_from_pb(&dag_from_segments(0, vec![legacy])).expect("legacy snapshot");
        assert_eq!(
            legacy.validate_assignment(&assignment(0, 0, 0, 65_535)),
            Err(AttachmentError::LegacySegment {
                segment_id: SegmentId(0),
            })
        );
    }

    #[test]
    fn ordering_eligibility_is_transitive_across_split_then_merge() {
        let dag = dag_from_segments(
            2,
            vec![
                sealed_info(0, 0, 65_535, &[], &[1, 2], 0, 1),
                sealed_info(1, 0, 32_767, &[0], &[3], 1, 2),
                sealed_info(2, 32_768, 65_535, &[0], &[3], 1, 2),
                child_info(3, 0, 65_535, &[1, 2], 2),
            ],
        );
        let snapshot = DagSnapshot::try_from_pb(&dag).expect("deep valid DAG");
        assert_eq!(
            snapshot.topological_order(),
            &[SegmentId(0), SegmentId(1), SegmentId(2), SegmentId(3)]
        );
        let owned = BTreeSet::from([SegmentId(0), SegmentId(1), SegmentId(2), SegmentId(3)]);
        let completed = BTreeSet::from([SegmentId(0), SegmentId(1)]);
        assert_eq!(
            snapshot.ordering_eligibility(SegmentId(3), OrderingMode::Strict, &owned, &completed,),
            Ok(OrderingEligibility::Blocked {
                incomplete_ancestors: vec![SegmentId(2)],
                broker_managed_ancestors: Vec::new(),
            })
        );
        let completed = BTreeSet::from([SegmentId(0), SegmentId(1), SegmentId(2)]);
        assert_eq!(
            snapshot.ordering_eligibility(SegmentId(3), OrderingMode::Strict, &owned, &completed,),
            Ok(OrderingEligibility::Eligible)
        );

        let only_child = BTreeSet::from([SegmentId(3)]);
        assert!(matches!(
            snapshot.ordering_eligibility(
                SegmentId(3),
                OrderingMode::Strict,
                &only_child,
                &BTreeSet::new(),
            ),
            Err(OrderingError::OrderingUnprovable { ancestors, .. })
                if ancestors == vec![SegmentId(0), SegmentId(1), SegmentId(2)]
        ));
        assert!(matches!(
            snapshot.ordering_eligibility(
                SegmentId(3),
                OrderingMode::BrokerManaged,
                &only_child,
                &BTreeSet::new(),
            ),
            Ok(OrderingEligibility::BrokerManaged { remote_ancestors })
                if remote_ancestors == vec![SegmentId(0), SegmentId(1), SegmentId(2)]
        ));
    }

    #[test]
    fn pruned_ancestry_is_unprovable_in_both_modes() {
        let dag = dag_from_segments(2, vec![child_info(3, 0, 65_535, &[], 2)]);
        let snapshot = DagSnapshot::try_from_pb(&dag).expect("pruned root is structurally valid");
        for mode in [OrderingMode::Strict, OrderingMode::BrokerManaged] {
            assert_eq!(
                snapshot.ordering_eligibility(
                    SegmentId(3),
                    mode,
                    &BTreeSet::from([SegmentId(3)]),
                    &BTreeSet::new(),
                ),
                Err(OrderingError::OrderingUnprovable {
                    segment_id: SegmentId(3),
                    ancestors: vec![SegmentId(3)],
                })
            );
        }
    }

    #[test]
    fn malformed_replacement_is_atomic_and_stops_delivery_state_change() {
        let mut session = resolved_session();
        let before = session.validated_snapshot().cloned();
        let topic_before = session.resolved_topic_name().map(str::to_owned);
        let mut invalid = valid_split_dag();
        invalid.epoch = 2;
        invalid.segments[1].hash_end = 32_766;
        let update = pb::CommandScalableTopicUpdate {
            session_id: 7,
            dag: Some(invalid),
            error: None,
            message: None,
            resolved_topic_name: Some("topic://other/ns/topic".to_owned()),
        };
        assert!(matches!(
            session.handle_update(&update),
            Err(DagError::InvalidSnapshot(
                DagValidationError::InvalidSplitCoverage { .. }
            ))
        ));
        assert_eq!(session.validated_snapshot(), before.as_ref());
        assert_eq!(session.resolved_topic_name(), topic_before.as_deref());
    }
}
