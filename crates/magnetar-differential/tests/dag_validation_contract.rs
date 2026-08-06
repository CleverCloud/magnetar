// SPDX-License-Identifier: Apache-2.0

//! Complete public DAG-validation and ordering contract under the simulation runner.

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]
#![allow(clippy::panic)]
#![allow(clippy::too_many_lines)]

use std::collections::BTreeSet;

use magnetar_proto::{
    AttachmentError, ConsumerAssignment, DagLimits, DagSnapshot, DagValidationError,
    DagWatchSession, KeyRange, OrderingEligibility, OrderingError, OrderingMode, SegmentId,
    canonical_segment_topic, pb,
};
use prost::Message as _;

const TOPIC: &str = "topic://public/default/scaled";

fn info(id: u64, start: u32, end: u32, parents: &[u64], children: &[u64]) -> pb::SegmentInfoProto {
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

fn dag(epoch: u64, segments: Vec<pb::SegmentInfoProto>) -> pb::ScalableTopicDag {
    let segment_brokers = segments
        .iter()
        .map(|segment| address(segment.segment_id))
        .collect();
    pb::ScalableTopicDag {
        epoch,
        segments,
        segment_brokers,
        controller_broker_url: Some("pulsar://controller:6650".to_owned()),
        controller_broker_url_tls: Some("pulsar+ssl://controller:6651".to_owned()),
    }
}

fn valid_split_dag() -> pb::ScalableTopicDag {
    dag(
        1,
        vec![
            sealed_info(0, 0, 65_535, &[], &[1, 2], 0, 1),
            child_info(1, 0, 32_767, &[0], 1),
            child_info(2, 32_768, 65_535, &[0], 1),
        ],
    )
}

fn valid_merge_dag() -> pb::ScalableTopicDag {
    dag(
        2,
        vec![
            sealed_info(0, 0, 32_767, &[], &[2], 0, 2),
            sealed_info(1, 32_768, 65_535, &[], &[2], 0, 2),
            child_info(2, 0, 65_535, &[0, 1], 2),
        ],
    )
}

fn assert_invalid(dag: &pb::ScalableTopicDag, predicate: impl FnOnce(&DagValidationError) -> bool) {
    let error = DagSnapshot::try_from_pb(dag).expect_err("snapshot must be rejected");
    assert!(predicate(&error), "unexpected validation error: {error:?}");
}

fn assignment(epoch: u64, id: u64, start: u32, end: u32) -> ConsumerAssignment {
    let range = KeyRange::new(start, end).expect("assignment range");
    ConsumerAssignment::try_from_pb(
        &pb::ScalableConsumerAssignment {
            layout_epoch: epoch,
            segments: vec![pb::ScalableAssignedSegment {
                segment_id: id,
                hash_start: start,
                hash_end: end,
                segment_topic: canonical_segment_topic(TOPIC, range, SegmentId(id))
                    .expect("canonical attachment"),
            }],
        },
        TOPIC,
    )
    .expect("assignment value")
}

#[test]
fn dag_rejects_duplicate_dangling_and_unbounded_resources() {
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

    let dag = valid_split_dag();
    let encoded_size = dag.encoded_len();
    assert!(matches!(
        DagSnapshot::try_from_pb_with_limits(
            &dag,
            DagLimits {
                serialized_size: encoded_size - 1,
                ..DagLimits::default()
            },
        ),
        Err(DagValidationError::SerializedSize { .. })
    ));
    assert!(matches!(
        DagSnapshot::try_from_pb_with_limits(
            &dag,
            DagLimits {
                segments: 2,
                ..DagLimits::default()
            },
        ),
        Err(DagValidationError::SegmentCount { .. })
    ));
    assert!(matches!(
        DagSnapshot::try_from_pb_with_limits(
            &dag,
            DagLimits {
                edges: 0,
                ..DagLimits::default()
            },
        ),
        Err(DagValidationError::EdgeCount { .. })
    ));
    assert!(matches!(
        DagSnapshot::try_from_pb_with_limits(
            &dag,
            DagLimits {
                depth: 0,
                ..DagLimits::default()
            },
        ),
        Err(DagValidationError::Depth { .. })
    ));

    let mut placement_heavy = dag;
    placement_heavy.segment_brokers.push(address(99));
    assert!(matches!(
        DagSnapshot::try_from_pb_with_limits(
            &placement_heavy,
            DagLimits {
                segments: 3,
                ..DagLimits::default()
            },
        ),
        Err(DagValidationError::PlacementCount { .. })
    ));
}

#[test]
fn dag_rejects_edge_inconsistencies_cycles_and_invalid_lifecycles() {
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

    let mut reverse_non_reciprocal = valid_split_dag();
    reverse_non_reciprocal.segments[0]
        .child_ids
        .retain(|child| *child != 1);
    assert_invalid(&reverse_non_reciprocal, |error| {
        matches!(error, DagValidationError::NonReciprocalEdge { .. })
    });

    let mut epoch_mismatch = valid_split_dag();
    epoch_mismatch.segments[1].created_at_epoch = 0;
    assert_invalid(&epoch_mismatch, |error| {
        matches!(error, DagValidationError::EdgeEpochMismatch { .. })
    });

    let cycle = dag(
        1,
        vec![
            info(0, 0, 65_535, &[], &[]),
            sealed_info(10, 0, 65_535, &[11], &[11], 1, 1),
            sealed_info(11, 0, 65_535, &[10], &[10], 1, 1),
        ],
    );
    assert_invalid(&cycle, |error| matches!(error, DagValidationError::Cycle));

    let mut invalid_range = dag(0, vec![info(0, 0, 65_535, &[], &[])]);
    invalid_range.segments[0].hash_end = 65_536;
    assert_invalid(&invalid_range, |error| {
        matches!(error, DagValidationError::InvalidDescriptor { .. })
    });

    let mut unknown_state = dag(0, vec![info(0, 0, 65_535, &[], &[])]);
    unknown_state.segments[0].state = 99;
    assert_invalid(&unknown_state, |error| {
        matches!(error, DagValidationError::InvalidDescriptor { .. })
    });

    let created_after = dag(0, vec![child_info(0, 0, 65_535, &[], 1)]);
    assert_invalid(&created_after, |error| {
        matches!(error, DagValidationError::CreatedAfterLayout { .. })
    });

    let mut active_with_seal = dag(1, vec![info(0, 0, 65_535, &[], &[])]);
    active_with_seal.segments[0].sealed_at_epoch = Some(1);
    assert_invalid(&active_with_seal, |error| {
        matches!(error, DagValidationError::ActiveWithSealEpoch { .. })
    });

    let mut active_with_children = dag(1, vec![info(0, 0, 65_535, &[], &[])]);
    active_with_children.segments[0].child_ids.push(1);
    assert_invalid(&active_with_children, |error| {
        matches!(error, DagValidationError::ActiveWithChildren { .. })
    });

    let mut sealed_without_epoch = dag(1, vec![info(0, 0, 65_535, &[], &[])]);
    sealed_without_epoch.segments[0].state = pb::SegmentState::Sealed as i32;
    assert_invalid(&sealed_without_epoch, |error| {
        matches!(error, DagValidationError::SealedWithoutEpoch { .. })
    });

    let invalid_seal = dag(2, vec![sealed_info(0, 0, 65_535, &[], &[], 2, 1)]);
    assert_invalid(&invalid_seal, |error| {
        matches!(error, DagValidationError::InvalidSealEpoch { .. })
    });
}

#[test]
fn dag_rejects_invalid_transitions_and_active_coverage() {
    let mut invalid_split = valid_split_dag();
    invalid_split.segments[1].hash_end = 32_766;
    assert_invalid(&invalid_split, |error| {
        matches!(error, DagValidationError::InvalidSplitCoverage { .. })
    });

    let mut short_split = valid_split_dag();
    short_split.segments[2].hash_end = 65_534;
    assert_invalid(&short_split, |error| {
        matches!(error, DagValidationError::InvalidSplitCoverage { .. })
    });

    let mut invalid_merge = valid_merge_dag();
    invalid_merge.segments[2].hash_end = 65_534;
    assert_invalid(&invalid_merge, |error| {
        matches!(error, DagValidationError::InvalidMergeCoverage { .. })
    });

    let conflicting = dag(
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

    let gap = dag(
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

    let overlap = dag(
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

    let extra_after_complete = dag(
        0,
        vec![
            info(0, 0, 65_535, &[], &[]),
            info(1, 65_535, 65_535, &[], &[]),
        ],
    );
    assert_invalid(&extra_after_complete, |error| {
        matches!(
            error,
            DagValidationError::ActiveCoverageDiscontinuity { .. }
        )
    });

    let short = dag(0, vec![info(0, 0, 65_534, &[], &[])]);
    assert_invalid(&short, |error| {
        matches!(error, DagValidationError::ActiveCoverageEnd { .. })
    });

    let no_active = dag(1, vec![sealed_info(0, 0, 65_535, &[], &[], 0, 1)]);
    assert_invalid(&no_active, |error| {
        matches!(error, DagValidationError::NoActiveSegments)
    });
}

#[test]
fn dag_assignment_and_transitive_ordering_are_strict() {
    assert!(!DagWatchSession::new(7).contains(SegmentId(1)));
    let snapshot = DagSnapshot::try_from_pb(&valid_split_dag()).expect("split snapshot");
    assert_eq!(snapshot.epoch(), 1);
    assert_eq!(snapshot.segments().len(), 3);
    assert!(snapshot.segment(SegmentId(1)).is_some());
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
    let legacy = DagSnapshot::try_from_pb(&dag(0, vec![legacy])).expect("legacy snapshot");
    assert_eq!(
        legacy.validate_assignment(&assignment(0, 0, 0, 65_535)),
        Err(AttachmentError::LegacySegment {
            segment_id: SegmentId(0),
        })
    );

    let deep = dag(
        2,
        vec![
            sealed_info(0, 0, 65_535, &[], &[1, 2], 0, 1),
            sealed_info(1, 0, 32_767, &[0], &[3], 1, 2),
            sealed_info(2, 32_768, 65_535, &[0], &[3], 1, 2),
            child_info(3, 0, 65_535, &[1, 2], 2),
        ],
    );
    let deep = DagSnapshot::try_from_pb(&deep).expect("deep snapshot");
    assert_eq!(
        deep.topological_order(),
        &[SegmentId(0), SegmentId(1), SegmentId(2), SegmentId(3)]
    );
    let owned = BTreeSet::from([SegmentId(0), SegmentId(1), SegmentId(2), SegmentId(3)]);
    let partially_completed = BTreeSet::from([SegmentId(0), SegmentId(1)]);
    let blocked = deep
        .ordering_eligibility(
            SegmentId(3),
            OrderingMode::Strict,
            &owned,
            &partially_completed,
        )
        .expect("locally blocked");
    assert_eq!(
        blocked,
        OrderingEligibility::Blocked {
            incomplete_ancestors: vec![SegmentId(2)],
            broker_managed_ancestors: Vec::new(),
        }
    );
    assert!(!blocked.permits_flow());
    let completed = BTreeSet::from([SegmentId(0), SegmentId(1), SegmentId(2)]);
    let eligible = deep
        .ordering_eligibility(SegmentId(3), OrderingMode::Strict, &owned, &completed)
        .expect("eligible");
    assert_eq!(eligible, OrderingEligibility::Eligible);
    assert!(eligible.permits_flow());

    let only_child = BTreeSet::from([SegmentId(3)]);
    assert!(matches!(
        deep.ordering_eligibility(
            SegmentId(3),
            OrderingMode::Strict,
            &only_child,
            &BTreeSet::new(),
        ),
        Err(OrderingError::OrderingUnprovable { ancestors, .. })
            if ancestors == vec![SegmentId(0), SegmentId(1), SegmentId(2)]
    ));
    let broker_managed = deep
        .ordering_eligibility(
            SegmentId(3),
            OrderingMode::BrokerManaged,
            &only_child,
            &BTreeSet::new(),
        )
        .expect("broker-managed ancestry");
    assert!(matches!(
        broker_managed,
        OrderingEligibility::BrokerManaged { ref remote_ancestors }
            if remote_ancestors.as_slice() == [SegmentId(0), SegmentId(1), SegmentId(2)]
    ));
    assert!(broker_managed.permits_flow());
    assert_eq!(
        deep.ordering_eligibility(
            SegmentId(99),
            OrderingMode::Strict,
            &BTreeSet::new(),
            &BTreeSet::new(),
        ),
        Err(OrderingError::UnknownSegment {
            segment_id: SegmentId(99),
        })
    );

    let pruned = DagSnapshot::try_from_pb(&dag(2, vec![child_info(3, 0, 65_535, &[], 2)]))
        .expect("pruned root is structurally valid");
    for mode in [OrderingMode::Strict, OrderingMode::BrokerManaged] {
        assert_eq!(
            pruned.ordering_eligibility(
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
