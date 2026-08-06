// SPDX-License-Identifier: Apache-2.0

//! Adversarial public control-plane contracts reached by the simulation runner.

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]

use magnetar_proto::{
    AssignmentError, ConsumerAssignment, ControllerIncarnation, KeyRange, KeyRangeError,
    MAX_BUFFERED_ASSIGNMENT_UPDATES, ScalableConsumerSession, ScalableConsumerType,
    SegmentDescriptor, SegmentDescriptorError, SegmentId, SegmentSource, SegmentState,
    SegmentTopicError, canonical_segment_topic, pb,
};

const TOPIC: &str = "topic://public/default/scaled";

fn assigned(id: u64, start: u32, end: u32) -> pb::ScalableAssignedSegment {
    let range = KeyRange::new(start, end).expect("valid range");
    pb::ScalableAssignedSegment {
        segment_id: id,
        hash_start: start,
        hash_end: end,
        segment_topic: canonical_segment_topic(TOPIC, range, SegmentId(id))
            .expect("canonical segment"),
    }
}

fn assignment(
    epoch: u64,
    segments: Vec<pb::ScalableAssignedSegment>,
) -> pb::ScalableConsumerAssignment {
    pb::ScalableConsumerAssignment {
        layout_epoch: epoch,
        segments,
    }
}

fn response(
    epoch: u64,
    segments: Vec<pb::ScalableAssignedSegment>,
) -> pb::CommandScalableTopicSubscribeResponse {
    pb::CommandScalableTopicSubscribeResponse {
        request_id: 1,
        error: None,
        message: None,
        assignment: Some(assignment(epoch, segments)),
    }
}

fn session() -> ScalableConsumerSession {
    ScalableConsumerSession::new(
        7,
        TOPIC.to_owned(),
        "sub".to_owned(),
        "consumer-a".to_owned(),
        ScalableConsumerType::Stream,
        ControllerIncarnation(1),
    )
}

#[test]
fn inclusive_key_ranges_reject_invalid_bounds_and_expose_relations() {
    assert_eq!(
        KeyRange::new(65_536, 65_536),
        Err(KeyRangeError::StartOutOfBounds { start: 65_536 })
    );
    assert_eq!(
        KeyRange::new(0, 65_536),
        Err(KeyRangeError::EndOutOfBounds { end: 65_536 })
    );
    assert_eq!(
        KeyRange::new(2, 1),
        Err(KeyRangeError::Reversed { start: 2, end: 1 })
    );

    let left = KeyRange::try_from((0, 32_767)).expect("left range");
    let right = KeyRange::new(32_768, 65_535).expect("right range");
    assert_eq!(left.start(), 0);
    assert_eq!(left.end(), 32_767);
    assert_eq!(left.len(), 32_768);
    assert!(!left.is_empty());
    assert!(left.contains(0));
    assert!(left.contains(32_767));
    assert!(!left.contains(32_768));
    assert!(KeyRange::FULL.contains_range(left));
    assert!(left.is_adjacent_to(right));
    assert!(right.is_adjacent_to(left));
    assert!(!KeyRange::FULL.is_adjacent_to(left));
}

#[test]
fn segment_identity_and_descriptor_validation_fail_closed() {
    assert!(matches!(
        canonical_segment_topic(
            "persistent://public/default/scaled",
            KeyRange::FULL,
            SegmentId(1),
        ),
        Err(SegmentTopicError::InvalidParent { .. })
    ));
    for parent in [
        "topic://",
        "topic://public",
        "topic://public/default/",
        "topic://p/n/x?q",
    ] {
        assert!(matches!(
            canonical_segment_topic(parent, KeyRange::FULL, SegmentId(1)),
            Err(SegmentTopicError::InvalidParent { .. })
        ));
    }
    for topic in [
        "topic://public/default/scaled/0000-ffff-1",
        "segment://missing-descriptor",
        "segment://public/default/scaled/0000-ffff",
        "segment://public/default/scaled/000-ffff-1",
        "segment://public/default/scaled/0000-FFFF-1",
        "segment://public/default/scaled/0000-ffff-01",
        "segment://public/default/scaled/0000-ffff-18446744073709551616",
    ] {
        assert!(matches!(
            SegmentSource::new(SegmentId(1), topic.to_owned()),
            Err(SegmentTopicError::InvalidSegment { .. })
        ));
    }
    let mismatched = canonical_segment_topic(TOPIC, KeyRange::FULL, SegmentId(2))
        .expect("canonical mismatched source");
    assert_eq!(
        SegmentSource::new(SegmentId(1), mismatched),
        Err(SegmentTopicError::SegmentIdMismatch {
            segment_id: SegmentId(1),
            topic_id: SegmentId(2),
        })
    );

    let info = pb::SegmentInfoProto {
        segment_id: 1,
        hash_start: 0,
        hash_end: 65_535,
        state: pb::SegmentState::Active as i32,
        parent_ids: Vec::new(),
        child_ids: Vec::new(),
        created_at_epoch: 0,
        sealed_at_epoch: None,
        created_at_ms: 0,
        sealed_at_ms: None,
        legacy_topic_name: None,
    };
    let mismatched_broker = pb::SegmentBrokerAddress {
        segment_id: 2,
        broker_url: "pulsar://broker:6650".to_owned(),
        broker_url_tls: None,
    };
    assert_eq!(
        SegmentDescriptor::try_from_pb(&info, Some(&mismatched_broker)),
        Err(SegmentDescriptorError::PlacementMismatch {
            segment_id: 1,
            placement_id: 2,
        })
    );
    let mut empty_legacy = info.clone();
    empty_legacy.legacy_topic_name = Some(String::new());
    assert_eq!(
        SegmentDescriptor::try_from_pb(&empty_legacy, None),
        Err(SegmentDescriptorError::EmptyLegacyTopic)
    );
    let descriptor = SegmentDescriptor::try_from_pb(&info, None).expect("descriptor");
    assert_eq!(descriptor.state, SegmentState::Active);
}

#[test]
fn assignments_validate_identity_duplicates_and_accessors() {
    let canonical = assigned(1, 0, 65_535);
    let decoded = ConsumerAssignment::try_from_pb(&assignment(3, vec![canonical.clone()]), TOPIC)
        .expect("assignment");
    assert_eq!(decoded.layout_epoch(), 3);
    assert_eq!(
        decoded.segment_topics(),
        vec![canonical.segment_topic.as_str()]
    );
    assert_eq!(decoded.to_pb(), assignment(3, vec![canonical.clone()]));
    let segment = &decoded.segments()[0];
    assert_eq!(segment.segment_id(), SegmentId(1));
    assert_eq!(segment.key_range(), KeyRange::FULL);
    assert_eq!(segment.segment_topic(), canonical.segment_topic);
    assert_eq!(segment.source().parent_topic(), TOPIC);

    assert_eq!(
        ConsumerAssignment::try_from_pb(
            &assignment(3, vec![canonical.clone(), canonical.clone()]),
            TOPIC,
        ),
        Err(AssignmentError::DuplicateSegment {
            segment_id: SegmentId(1),
        })
    );
    let mut wrong_attachment = canonical;
    wrong_attachment.segment_topic.push_str("-wrong");
    assert!(matches!(
        ConsumerAssignment::try_from_pb(&assignment(3, vec![wrong_attachment]), TOPIC),
        Err(AssignmentError::AttachmentMismatch { .. })
    ));
}

#[test]
fn controller_incarnations_fence_callbacks_and_retain_epoch_floor() {
    assert_eq!(ControllerIncarnation(42).to_string(), "42");
    assert_eq!(
        ScalableConsumerType::from_pb_i32(ScalableConsumerType::Checkpoint.to_pb_i32()),
        ScalableConsumerType::Checkpoint
    );
    assert_eq!(
        ScalableConsumerType::from_pb_i32(i32::MAX),
        ScalableConsumerType::Stream
    );

    let mut session = session();
    assert_eq!(session.topic(), TOPIC);
    assert_eq!(session.subscription(), "sub");
    assert_eq!(session.consumer_name(), "consumer-a");
    assert_eq!(session.consumer_type(), ScalableConsumerType::Stream);
    assert_eq!(session.incarnation(), ControllerIncarnation(1));
    assert!(
        session.matches_registration(TOPIC, "sub", "consumer-a", ScalableConsumerType::Stream,)
    );
    assert!(!session.matches_registration(
        TOPIC,
        "other",
        "consumer-a",
        ScalableConsumerType::Stream,
    ));
    session
        .handle_subscribe_response(&response(4, vec![assigned(1, 0, 65_535)]))
        .expect("initial baseline");
    assert_eq!(session.epoch_floor(), Some(4));
    session
        .begin_incarnation(ControllerIncarnation(2))
        .expect("advance incarnation");
    assert_eq!(session.incarnation(), ControllerIncarnation(2));
    assert!(!session.is_registered());
    assert_eq!(
        session.begin_incarnation(ControllerIncarnation(2)),
        Err(AssignmentError::NonAdvancingIncarnation {
            got: ControllerIncarnation(2),
            prev: ControllerIncarnation(2),
        })
    );
    assert!(matches!(
        session.handle_subscribe_response_for(
            ControllerIncarnation(1),
            &response(4, vec![assigned(1, 0, 65_535)]),
        ),
        Err(AssignmentError::IncarnationMismatch { .. })
    ));
    assert_eq!(
        session.handle_subscribe_response(&response(3, vec![assigned(1, 0, 65_535)])),
        Err(AssignmentError::CrossIncarnationEpochRegression { got: 3, floor: 4 })
    );
    session
        .handle_subscribe_response(&response(5, vec![assigned(1, 0, 65_535)]))
        .expect("advancing replacement baseline");
    assert_eq!(session.epoch_floor(), Some(5));
}

#[test]
fn prebaseline_assignment_buffer_is_bounded_and_consumer_scoped() {
    let mut buffered = session();
    let update = pb::CommandScalableTopicAssignmentUpdate {
        consumer_id: 7,
        assignment: assignment(1, vec![assigned(1, 0, 65_535)]),
    };
    for _ in 0..MAX_BUFFERED_ASSIGNMENT_UPDATES {
        assert_eq!(buffered.handle_assignment_update(&update), Ok(None));
    }
    assert_eq!(
        buffered.handle_assignment_update(&update),
        Err(AssignmentError::PreBaselineBufferFull {
            max: MAX_BUFFERED_ASSIGNMENT_UPDATES,
        })
    );

    let mut other = session();
    assert_eq!(
        other.handle_assignment_update(&pb::CommandScalableTopicAssignmentUpdate {
            consumer_id: 8,
            assignment: assignment(1, Vec::new()),
        }),
        Err(AssignmentError::ConsumerMismatch {
            got: 8,
            expected: 7,
        })
    );
    assert_eq!(
        other.handle_subscribe_response(&pb::CommandScalableTopicSubscribeResponse {
            request_id: 9,
            error: None,
            message: None,
            assignment: None,
        }),
        Err(AssignmentError::Empty { request_id: 9 })
    );

    let mut replaying = session();
    replaying
        .handle_assignment_update(&pb::CommandScalableTopicAssignmentUpdate {
            consumer_id: 7,
            assignment: assignment(2, vec![assigned(2, 0, 65_535)]),
        })
        .expect("buffer pre-baseline push");
    replaying
        .handle_subscribe_response(&response(1, vec![assigned(1, 0, 65_535)]))
        .expect("apply baseline and replay");
    let replayed = replaying.take_replayed_updates();
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].assignment.layout_epoch(), 2);
    assert_eq!(replayed[0].delta.lost, vec![SegmentId(1)]);
}
