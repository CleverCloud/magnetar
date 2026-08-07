// SPDX-License-Identifier: Apache-2.0

//! Pure aggregate-ledger contracts executed by the simulation runner.

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]
#![allow(clippy::match_wildcard_for_single_variants)]
#![allow(clippy::too_many_lines)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use bytes::{BufMut as _, Bytes, BytesMut};
use magnetar_proto::{
    AggregatePhase, AggregateTransaction, AggregateTransactionError, AggregateTransactionState,
    ArrivalFailureDisposition, BudgetError, BudgetReservationId, BudgetUse,
    CONTROL_PLANE_CLEANUP_RESERVE, ChildGeneration, ConsumerAssignment, ConsumerInstanceId,
    ControllerIncarnation, DECOMPRESSION_VALIDATION_SLACK, DagSnapshot, DeferredIncomingMessage,
    DeliveryEpoch, DeliveryToken, FlowBlock, IncomingMessage, KeyRange, MAX_FRAME_SIZE, MessageId,
    OrderingMode, PositionVector, ReceiverBudget, ReceiverBudgetState, SegmentId, SegmentPhase,
    StreamConsumerAction, StreamConsumerModel, StreamConsumerModelError, StreamEntryAcceptance,
    StreamMessageId, StreamReceiveState, TransactionAcknowledgementOutcome, TransactionDecision,
    TxnId, canonical_segment_topic, pb,
};
use prost::Message as _;

fn minimum_budget() -> ReceiverBudget {
    let minimum = match ReceiverBudget::bytes(0) {
        Err(BudgetError::BudgetTooSmall { minimum, .. }) => minimum,
        result => panic!("zero receiver budget returned an unexpected result: {result:?}"),
    };
    ReceiverBudget::bytes(minimum).expect("minimum aggregate budget")
}

#[allow(clippy::too_many_arguments)]
fn segment_info(
    id: u64,
    start: u32,
    end: u32,
    state: pb::SegmentState,
    parents: &[u64],
    children: &[u64],
    created: u64,
    sealed: Option<u64>,
) -> pb::SegmentInfoProto {
    pb::SegmentInfoProto {
        segment_id: id,
        hash_start: start,
        hash_end: end,
        state: state as i32,
        parent_ids: parents.to_vec(),
        child_ids: children.to_vec(),
        created_at_epoch: created,
        sealed_at_epoch: sealed,
        created_at_ms: 0,
        sealed_at_ms: sealed.map(|_| 0),
        legacy_topic_name: None,
    }
}

fn split_dag_at(epoch: u64, segment_one_broker: &str) -> DagSnapshot {
    DagSnapshot::try_from_pb(&pb::ScalableTopicDag {
        epoch,
        segments: vec![
            segment_info(
                0,
                0,
                65_535,
                pb::SegmentState::Sealed,
                &[],
                &[1, 2],
                0,
                Some(1),
            ),
            segment_info(1, 0, 32_767, pb::SegmentState::Active, &[0], &[], 1, None),
            segment_info(
                2,
                32_768,
                65_535,
                pb::SegmentState::Active,
                &[0],
                &[],
                1,
                None,
            ),
        ],
        segment_brokers: (0..=2)
            .map(|id| pb::SegmentBrokerAddress {
                segment_id: id,
                broker_url: if id == 1 {
                    segment_one_broker.to_owned()
                } else {
                    format!("pulsar://broker-{id}:6650")
                },
                broker_url_tls: None,
            })
            .collect(),
        controller_broker_url: None,
        controller_broker_url_tls: None,
    })
    .expect("valid split DAG")
}

fn split_dag() -> DagSnapshot {
    split_dag_at(1, "pulsar://broker-1:6650")
}

fn assignment_at(epoch: u64, ids: &[u64]) -> ConsumerAssignment {
    let segments = ids
        .iter()
        .map(|id| {
            let (start, end) = match *id {
                1 => (0, 32_767),
                2 => (32_768, 65_535),
                _ => (0, 65_535),
            };
            let range = KeyRange::new(start, end).expect("segment range");
            pb::ScalableAssignedSegment {
                segment_id: *id,
                hash_start: start,
                hash_end: end,
                segment_topic: canonical_segment_topic(
                    "topic://public/default/model",
                    range,
                    SegmentId(*id),
                )
                .expect("canonical segment topic"),
            }
        })
        .collect();
    ConsumerAssignment::try_from_pb(
        &pb::ScalableConsumerAssignment {
            layout_epoch: epoch,
            segments,
        },
        "topic://public/default/model",
    )
    .expect("valid assignment")
}

fn assignment(ids: &[u64]) -> ConsumerAssignment {
    assignment_at(1, ids)
}

fn foreign_assignment() -> ConsumerAssignment {
    let range = KeyRange::new(0, 32_767).expect("foreign range");
    ConsumerAssignment::try_from_pb(
        &pb::ScalableConsumerAssignment {
            layout_epoch: 1,
            segments: vec![pb::ScalableAssignedSegment {
                segment_id: 1,
                hash_start: range.start(),
                hash_end: range.end(),
                segment_topic: canonical_segment_topic(
                    "topic://public/default/foreign",
                    range,
                    SegmentId(1),
                )
                .expect("foreign segment topic"),
            }],
        },
        "topic://public/default/foreign",
    )
    .expect("foreign assignment")
}

fn model() -> StreamConsumerModel {
    model_with_data_capacity(MAX_FRAME_SIZE * 4)
}

fn model_with_data_capacity(data_capacity: usize) -> StreamConsumerModel {
    StreamConsumerModel::new(
        "topic://public/default/model".to_owned(),
        ConsumerInstanceId(10),
        ControllerIncarnation(3),
        OrderingMode::BrokerManaged,
        split_dag(),
        ReceiverBudget::bytes(
            data_capacity
                + magnetar_proto::stream_consumer::RECEIVER_BUDGET_AUTHORITY_HEADROOM
                + CONTROL_PLANE_CLEANUP_RESERVE,
        )
        .expect("aggregate budget"),
    )
    .expect("aggregate model")
}

fn message_id(entry_id: u64) -> MessageId {
    MessageId {
        ledger_id: 1,
        entry_id,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
    }
}

fn source(segment_id: u64) -> magnetar_proto::SegmentSource {
    assignment(&[segment_id]).segments()[0].source()
}

fn stream_id(segment_id: u64, entry_id: u64) -> StreamMessageId {
    StreamMessageId::new(source(segment_id), message_id(entry_id)).expect("stream message id")
}

fn opened_generation(action: &StreamConsumerAction) -> ChildGeneration {
    match action {
        StreamConsumerAction::OpenChild {
            child_generation, ..
        } => *child_generation,
        other => panic!("expected child open, got {other:?}"),
    }
}

fn flow_reservation(actions: &[StreamConsumerAction]) -> BudgetReservationId {
    match actions {
        [StreamConsumerAction::GrantFlow { reservation, .. }] => *reservation,
        other => panic!("expected one FLOW action, got {other:?}"),
    }
}

fn issue_delivery(
    model: &mut StreamConsumerModel,
    segment_id: u64,
    generation: ChildGeneration,
    flow: BudgetReservationId,
    entry_id: u64,
) -> (DeliveryToken, Vec<StreamConsumerAction>) {
    let arrival = model
        .message_arrived(SegmentId(segment_id), generation, flow, 128)
        .expect("message arrival");
    let token = model
        .issue_delivery(
            SegmentId(segment_id),
            generation,
            stream_id(segment_id, entry_id),
            arrival.retained,
        )
        .expect("issue delivery");
    (token, arrival.actions)
}

fn opened_one_child() -> (StreamConsumerModel, ChildGeneration, BudgetReservationId) {
    let mut model = model();
    let open = model
        .apply_assignment(assignment(&[1]))
        .expect("assignment");
    let generation = opened_generation(&open[0]);
    let flow = model
        .child_opened(SegmentId(1), generation)
        .expect("child open");
    (model, generation, flow_reservation(&flow))
}

fn deferred_entry(
    entry_id: u64,
    payload: Bytes,
    metadata: pb::MessageMetadata,
    ack_set: Vec<i64>,
    dispatch_permits: u32,
) -> DeferredIncomingMessage {
    let message_id_data = pb::MessageIdData {
        ledger_id: 1,
        entry_id,
        partition: Some(0),
        batch_index: Some(-1),
        ack_set: Vec::new(),
        batch_size: None,
        first_chunk_message_id: None,
    };
    DeferredIncomingMessage {
        message: IncomingMessage {
            message_id: MessageId::from_pb(&message_id_data),
            metadata: Arc::new(metadata),
            single_metadata: None,
            payload,
            redelivery_count: 0,
            broker_entry_metadata: None,
            arrived_at: Instant::now(),
        },
        message_id_data,
        ack_set,
        dispatch_permits,
    }
}

fn encoded_batch(payloads: &[&[u8]]) -> Bytes {
    let mut encoded = BytesMut::new();
    for payload in payloads {
        let single = pb::SingleMessageMetadata {
            payload_size: i32::try_from(payload.len()).expect("small payload"),
            ..Default::default()
        }
        .encode_to_vec();
        encoded.put_u32(u32::try_from(single.len()).expect("small metadata"));
        encoded.extend_from_slice(&single);
        encoded.extend_from_slice(payload);
    }
    encoded.freeze()
}

#[test]
fn receiver_budget_separates_flow_data_authority_and_cleanup_capacity() {
    let budget = minimum_budget();
    let mut state = ReceiverBudgetState::new(budget);
    assert_eq!(state.limit(), budget.limit());
    assert_eq!(state.data_available(), budget.data_limit());
    assert_eq!(state.data_used(), 0);
    assert_eq!(state.control_used(), 0);

    let flow = state.reserve_flow().expect("one max-frame FLOW fits");
    assert_eq!(state.data_used(), MAX_FRAME_SIZE);
    assert!(matches!(
        state.reserve_flow(),
        Err(BudgetError::Exhausted {
            requested: MAX_FRAME_SIZE,
            available: 0,
        })
    ));
    state.release(flow).expect("release FLOW reservation");

    state
        .reserve_control(CONTROL_PLANE_CLEANUP_RESERVE)
        .expect("the independent cleanup reserve is fully usable");
    assert_eq!(state.control_used(), CONTROL_PLANE_CLEANUP_RESERVE);
    assert!(matches!(
        state.reserve_control(1),
        Err(BudgetError::ControlReserveExhausted {
            requested: 1,
            available: 0,
        })
    ));
    state.release_control(usize::MAX);
    assert_eq!(state.control_used(), 0);

    assert!(matches!(
        state.reserve(BudgetUse::Decompression, budget.data_limit() + 1),
        Err(BudgetError::MessageTooLargeForBudget { .. })
    ));
    assert!(matches!(
        state.reserve(BudgetUse::Decompression, budget.data_limit()),
        Err(BudgetError::MessageTooLargeForBudget { .. })
    ));

    let released = state
        .reserve(BudgetUse::BatchAssembly, 1)
        .expect("reserve batch workspace");
    state.release(released).expect("release batch workspace");
    assert!(matches!(
        state.transfer(
            released,
            BudgetUse::BatchAssembly,
            BudgetUse::Decompression,
            1,
        ),
        Err(BudgetError::UnknownReservation { .. })
    ));
    assert!(matches!(
        state.release(released),
        Err(BudgetError::UnknownReservation { .. })
    ));

    let workspace = state
        .reserve(BudgetUse::BatchAssembly, 1)
        .expect("reserve another batch workspace");
    assert!(matches!(
        state.transfer(
            workspace,
            BudgetUse::Decompression,
            BudgetUse::BatchAssembly,
            1,
        ),
        Err(BudgetError::ReservationUseMismatch { .. })
    ));
    state
        .transfer(
            workspace,
            BudgetUse::BatchAssembly,
            BudgetUse::Decompression,
            128,
        )
        .expect("resize and reclassify workspace");
    state.release(workspace).expect("release resized workspace");

    let mut full = ReceiverBudgetState::new(budget);
    let first = full
        .reserve(BudgetUse::Decompression, MAX_FRAME_SIZE / 2)
        .expect("reserve first half");
    let second = full
        .reserve(
            BudgetUse::BatchAssembly,
            MAX_FRAME_SIZE - MAX_FRAME_SIZE / 2,
        )
        .expect("reserve second half");
    assert!(matches!(
        full.transfer(
            first,
            BudgetUse::Decompression,
            BudgetUse::Decompression,
            MAX_FRAME_SIZE / 2 + 1,
        ),
        Err(BudgetError::Exhausted { .. })
    ));
    full.release(first).expect("release first half");
    full.release(second).expect("release second half");
}

#[test]
fn aggregate_transaction_closes_admission_and_requires_a_valid_final_outcome() {
    let transaction = TxnId::new(7, 11);
    let mut poisoned = AggregateTransaction::new(transaction);
    assert_eq!(poisoned.state(), AggregateTransactionState::Open);
    assert_eq!(poisoned.pending(), 0);
    assert!(!poisoned.is_poisoned());
    assert_eq!(
        poisoned.decision(),
        TransactionDecision::Wait { pending: 0 }
    );

    let operation = poisoned.admit().expect("admit aggregate transaction work");
    assert_eq!(poisoned.pending(), 1);
    assert_eq!(
        poisoned.begin_commit().expect("begin commit"),
        TransactionDecision::Wait { pending: 1 }
    );
    assert_eq!(
        poisoned
            .begin_commit()
            .expect("commit closing is idempotent"),
        TransactionDecision::Wait { pending: 1 }
    );
    assert!(matches!(
        poisoned.admit(),
        Err(AggregateTransactionError::AdmissionClosed { .. })
    ));
    assert!(matches!(
        poisoned.finish(AggregateTransactionState::Committed),
        Err(AggregateTransactionError::InvalidTransition { .. })
    ));
    poisoned
        .settle(operation, false)
        .expect("settle failed aggregate work");
    assert!(poisoned.is_poisoned());
    assert_eq!(
        poisoned.decision(),
        TransactionDecision::TransactionPoisoned
    );
    assert!(matches!(
        poisoned.settle(operation, true),
        Err(AggregateTransactionError::UnknownOperation { .. })
    ));
    assert!(matches!(
        poisoned.finish(AggregateTransactionState::Committed),
        Err(AggregateTransactionError::InvalidTransition { .. })
    ));
    assert_eq!(
        poisoned.begin_abort().expect("poisoned commit may abort"),
        TransactionDecision::IssueAbort
    );
    assert!(matches!(
        poisoned.begin_abort(),
        Err(AggregateTransactionError::InvalidTransition { .. })
    ));
    poisoned
        .finish(AggregateTransactionState::Aborted)
        .expect("record abort outcome");
    assert!(matches!(
        poisoned.finish(AggregateTransactionState::Aborted),
        Err(AggregateTransactionError::InvalidTransition { .. })
    ));

    let mut committed = AggregateTransaction::new(TxnId::new(13, 17));
    assert_eq!(
        committed.begin_commit().expect("issue empty commit"),
        TransactionDecision::IssueCommit
    );
    assert!(matches!(
        committed.begin_commit(),
        Err(AggregateTransactionError::InvalidTransition { .. })
    ));
    assert!(matches!(
        committed.begin_abort(),
        Err(AggregateTransactionError::InvalidTransition { .. })
    ));
    committed
        .finish(AggregateTransactionState::Committed)
        .expect("record commit outcome");

    let mut invalid_outcome = AggregateTransaction::new(TxnId::new(17, 19));
    invalid_outcome
        .begin_commit()
        .expect("issue invalid-outcome commit");
    assert!(matches!(
        invalid_outcome.finish(AggregateTransactionState::CommitIssued),
        Err(AggregateTransactionError::InvalidTransition { .. })
    ));

    let mut unknown = AggregateTransaction::new(TxnId::new(19, 23));
    assert_eq!(
        unknown.begin_abort().expect("issue empty abort"),
        TransactionDecision::IssueAbort
    );
    unknown
        .finish(AggregateTransactionState::Unknown)
        .expect("record unknown coordinator outcome");

    let mut cross_outcome = AggregateTransaction::new(TxnId::new(29, 31));
    assert_eq!(
        cross_outcome.begin_commit(),
        Ok(TransactionDecision::IssueCommit)
    );
    cross_outcome
        .finish(AggregateTransactionState::Aborted)
        .expect("consume coordinator-reported abort after commit issue");
}

#[test]
fn aggregate_model_acknowledgement_seek_resync_and_close_are_one_shot() {
    let mut model = model();
    assert_eq!(model.generation().0, 0);
    assert_eq!(model.delivery_epoch(), DeliveryEpoch(0));
    assert_eq!(model.phase(), AggregatePhase::Open);
    assert_eq!(model.controller_incarnation(), ControllerIncarnation(3));
    assert_eq!(model.dag().epoch(), 1);
    assert!(model.assignment().is_none());
    assert!(model.delivered_position().is_empty());

    let opens = model
        .apply_assignment(assignment(&[1, 2]))
        .expect("initial assignment");
    let generation_one = opened_generation(&opens[0]);
    let generation_two = opened_generation(&opens[1]);
    assert_eq!(model.child_generation(&source(1)), Some(generation_one));
    assert!(model.accepts_child_result(&source(2), generation_two));
    assert!(!model.accepts_child_result(&source(2), ChildGeneration(u64::MAX)));
    assert!(model.pending_ownership().is_empty());
    let opening = model.status();
    assert_eq!(opening.phase(), AggregatePhase::Open);
    assert_eq!(opening.layout_epoch(), 1);
    assert_eq!(opening.assigned_segments(), 2);
    assert_eq!(opening.attached_segments(), 0);
    assert_eq!(opening.draining_segments(), 0);
    assert!(opening.pending_ownership().is_empty());
    assert!(opening.ordering_unprovable().is_empty());
    assert_eq!(opening.receiver_budget_limit(), model.budget().limit());
    assert_eq!(opening.receiver_budget_used(), 0);

    let flow_one = model
        .child_opened(SegmentId(1), generation_one)
        .expect("first child open");
    let flow_two = model
        .child_opened(SegmentId(2), generation_two)
        .expect("second child open");
    let (token_one, _) = issue_delivery(
        &mut model,
        1,
        generation_one,
        flow_reservation(&flow_one),
        5,
    );
    let (token_two, _) = issue_delivery(
        &mut model,
        2,
        generation_two,
        flow_reservation(&flow_two),
        7,
    );
    assert_eq!(token_one.dequeue_sequence().0, 0);
    assert_eq!(token_two.dequeue_sequence().0, 1);
    assert!(matches!(
        model.admit_batch_acknowledgement(&[&token_one, &token_one]),
        Err(StreamConsumerModelError::DeliveryOperationPending)
    ));

    let batch = model
        .admit_batch_acknowledgement(&[&token_one, &token_two])
        .expect("admit grouped acknowledgement");
    assert_eq!(batch.components.len(), 2);
    for component in &batch.components {
        assert_eq!(
            component.child_generation(),
            model.child_generation(component.source()).unwrap()
        );
        assert_eq!(component.message_ids().len(), 1);
        assert_eq!(component.message_id_bytes().len(), 1);
        assert_eq!(component.message_id_data().expect("complete ids").len(), 1);
        assert!(!component.cumulative());
    }
    let source_one = token_one.stream_message_id().source().clone();
    model
        .settle_acknowledgement(&batch.authority, &BTreeSet::from([source_one]))
        .expect("settle partial grouped acknowledgement");
    assert!(matches!(
        model.resolve_delivery(&token_one),
        Err(StreamConsumerModelError::StaleDeliveryToken)
    ));

    let cumulative = model
        .admit_cumulative_acknowledgement(&token_two)
        .expect("admit cumulative acknowledgement");
    assert!(
        cumulative
            .components
            .iter()
            .all(magnetar_proto::AcknowledgementComponent::cumulative)
    );
    let confirmed = cumulative
        .components
        .iter()
        .map(|component| component.source().clone())
        .collect();
    model
        .settle_acknowledgement(&cumulative.authority, &confirmed)
        .expect("settle cumulative acknowledgement");
    assert!(matches!(
        model.cancel_acknowledgement(&cumulative.authority),
        Err(StreamConsumerModelError::StaleAcknowledgementAuthority)
    ));

    let vector = model.delivered_position().clone();
    assert_eq!(vector.len(), 2);
    let wrong_epoch = PositionVector::new(
        2,
        vector
            .iter()
            .map(|(segment_source, id)| (segment_source.clone(), id)),
    )
    .expect("wrong-epoch vector");
    assert!(matches!(
        model.begin_seek(&wrong_epoch),
        Err(StreamConsumerModelError::SeekLayoutMismatch { .. })
    ));
    let seek = model.begin_seek(&vector).expect("begin aggregate seek");
    assert_eq!(seek.len(), 4);
    assert!(
        model
            .seek_completed(SegmentId(1), generation_one)
            .expect("first seek component")
            .is_empty()
    );
    assert_eq!(
        model
            .seek_completed(SegmentId(2), generation_two)
            .expect("second seek component")
            .len(),
        2
    );
    assert!(matches!(
        model.seek_completed(SegmentId(2), generation_two),
        Err(StreamConsumerModelError::InvalidChildTransition { .. })
    ));

    let resync = model.require_resync().expect("enter resynchronization");
    assert_eq!(model.phase(), AggregatePhase::ResyncRequired);
    assert_eq!(resync.len(), 2);
    assert!(
        model
            .require_resync()
            .expect("idempotent resync")
            .is_empty()
    );
    model
        .child_closed(SegmentId(1), generation_one)
        .expect("first child closes");
    model
        .child_closed(SegmentId(2), generation_two)
        .expect("second child closes");
    model
        .begin_controller_incarnation(ControllerIncarnation(4))
        .expect("replacement controller");
    assert!(matches!(
        model.apply_assignment_for(ControllerIncarnation(3), assignment(&[1, 2])),
        Err(StreamConsumerModelError::Assignment(_))
    ));
    let replacements = model
        .apply_assignment_for(ControllerIncarnation(4), assignment(&[1, 2]))
        .expect("replacement baseline");
    let replacement_one = opened_generation(&replacements[0]);
    let replacement_two = opened_generation(&replacements[1]);
    assert_eq!(model.close().expect("close aggregate").len(), 2);
    assert!(model.close().expect("idempotent close").is_empty());
    model
        .child_closed(SegmentId(1), replacement_one)
        .expect("first replacement closes");
    model
        .child_closed(SegmentId(2), replacement_two)
        .expect("second replacement closes");
    assert_eq!(model.phase(), AggregatePhase::Closed);
}

#[test]
fn aggregate_model_transaction_outcomes_commit_abort_cancel_and_fence_unknown() {
    fn fixture(entry_id: u64) -> (StreamConsumerModel, DeliveryToken) {
        let (mut model, generation, flow) = opened_one_child();
        let (token, _) = issue_delivery(&mut model, 1, generation, flow, entry_id);
        (model, token)
    }

    let (mut committed, committed_token) = fixture(1);
    let committed_ack = committed
        .admit_individual_transactional_acknowledgement(&committed_token)
        .expect("admit commit acknowledgement");
    assert_eq!(committed_ack.components.len(), 1);
    committed
        .settle_transactional_acknowledgement(
            &committed_ack.authority,
            TransactionAcknowledgementOutcome::Committed,
        )
        .expect("settle committed acknowledgement");
    assert!(matches!(
        committed.resolve_delivery(&committed_token),
        Err(StreamConsumerModelError::StaleDeliveryToken)
    ));

    let (mut aborted, aborted_token) = fixture(2);
    let aborted_ack = aborted
        .admit_cumulative_transactional_acknowledgement(&aborted_token)
        .expect("admit abort acknowledgement");
    aborted
        .settle_transactional_acknowledgement(
            &aborted_ack.authority,
            TransactionAcknowledgementOutcome::Aborted,
        )
        .expect("settle aborted acknowledgement");
    aborted
        .resolve_delivery(&aborted_token)
        .expect("abort retains delivery authority");

    let (mut cancelled, cancelled_token) = fixture(3);
    let positions = cancelled_token.position_vector().clone();
    let cancelled_ack = cancelled
        .admit_position_transactional_acknowledgement(&positions)
        .expect("admit restored transaction position");
    cancelled
        .cancel_transactional_acknowledgement(&cancelled_ack.authority)
        .expect("cancel transaction admission");
    assert!(matches!(
        cancelled.cancel_transactional_acknowledgement(&cancelled_ack.authority),
        Err(StreamConsumerModelError::StaleAcknowledgementAuthority)
    ));
    cancelled
        .resolve_delivery(&cancelled_token)
        .expect("cancel retains delivery authority");

    let (mut unknown, unknown_token) = fixture(4);
    let unknown_ack = unknown
        .admit_individual_transactional_acknowledgement(&unknown_token)
        .expect("admit unknown acknowledgement");
    assert!(matches!(
        unknown
            .settle_transactional_acknowledgement(
                &unknown_ack.authority,
                TransactionAcknowledgementOutcome::Unknown,
            )
            .expect("settle unknown acknowledgement")
            .as_slice(),
        [StreamConsumerAction::CloseChild { .. }]
    ));
    assert_eq!(unknown.phase(), AggregatePhase::ResyncRequired);
    assert!(matches!(
        unknown.resolve_delivery(&unknown_token),
        Err(StreamConsumerModelError::StaleDeliveryToken)
    ));

    let (mut failed, generation, flow) = opened_one_child();
    let (failed_token, next_flow) = issue_delivery(&mut failed, 1, generation, flow, 5);
    let failed_ack = failed
        .admit_individual_transactional_acknowledgement(&failed_token)
        .expect("admit acknowledgement before child failure");
    assert!(matches!(
        failed.message_arrived(
            SegmentId(1),
            generation,
            flow_reservation(&next_flow),
            usize::MAX,
        ),
        Err(StreamConsumerModelError::ArrivalAccountingFailed { .. })
    ));
    assert!(matches!(
        failed.settle_transactional_acknowledgement(
            &failed_ack.authority,
            TransactionAcknowledgementOutcome::Committed,
        ),
        Err(StreamConsumerModelError::StaleDeliveryToken)
    ));
}

#[test]
fn aggregate_model_failure_dispositions_and_reservation_owners_are_explicit() {
    let mut owners = model();
    let opens = owners
        .apply_assignment(assignment(&[1, 2]))
        .expect("owner assignment");
    let generation_one = opened_generation(&opens[0]);
    let generation_two = opened_generation(&opens[1]);
    let flow_one = owners
        .child_opened(SegmentId(1), generation_one)
        .expect("first owner child");
    let flow_two = owners
        .child_opened(SegmentId(2), generation_two)
        .expect("second owner child");
    let retained_two = owners
        .message_arrived(
            SegmentId(2),
            generation_two,
            flow_reservation(&flow_two),
            32,
        )
        .expect("second child arrival")
        .retained;
    assert!(matches!(
        owners.issue_delivery(SegmentId(1), generation_one, stream_id(1, 1), retained_two,),
        Err(StreamConsumerModelError::Budget(
            BudgetError::ReservationOwnerMismatch { .. }
        ))
    ));
    assert!(matches!(
        owners.issue_delivery(
            SegmentId(1),
            generation_one,
            stream_id(1, 2),
            flow_reservation(&flow_one),
        ),
        Err(StreamConsumerModelError::Budget(
            BudgetError::ReservationUseMismatch { .. }
        ))
    ));

    let mut permanent = model_with_data_capacity(MAX_FRAME_SIZE);
    let open = permanent
        .apply_assignment(assignment(&[1]))
        .expect("permanent assignment");
    let generation = opened_generation(&open[0]);
    let flow = permanent
        .child_opened(SegmentId(1), generation)
        .expect("permanent child");
    assert!(matches!(
        permanent.message_arrived(
            SegmentId(1),
            generation,
            flow_reservation(&flow),
            MAX_FRAME_SIZE + 1,
        ),
        Err(StreamConsumerModelError::ArrivalAccountingFailed {
            error: BudgetError::MessageTooLargeForBudget { .. },
            disposition: ArrivalFailureDisposition::Permanent,
            actions,
        }) if matches!(actions.as_slice(), [StreamConsumerAction::CloseChild { .. }])
    ));
    assert_eq!(
        permanent.segment_phase(SegmentId(1)),
        Some(&SegmentPhase::Failed)
    );
    permanent
        .child_closed(SegmentId(1), generation)
        .expect("permanently failed child closes");
    assert!(
        permanent
            .apply_assignment(assignment(&[1]))
            .expect("same permanent assignment remains fenced")
            .is_empty()
    );

    let mut retryable = model_with_data_capacity(MAX_FRAME_SIZE * 2);
    let opens = retryable
        .apply_assignment(assignment(&[1, 2]))
        .expect("retryable assignment");
    let generation_one = opened_generation(&opens[0]);
    let generation_two = opened_generation(&opens[1]);
    let flow_one = retryable
        .child_opened(SegmentId(1), generation_one)
        .expect("retryable first child");
    retryable
        .child_opened(SegmentId(2), generation_two)
        .expect("retryable second child");
    assert!(matches!(
        retryable.message_arrived(
            SegmentId(1),
            generation_one,
            flow_reservation(&flow_one),
            MAX_FRAME_SIZE + 1,
        ),
        Err(StreamConsumerModelError::ArrivalAccountingFailed {
            error: BudgetError::Exhausted { .. },
            disposition: ArrivalFailureDisposition::Retryable,
            actions,
        }) if matches!(actions.as_slice(), [StreamConsumerAction::CloseChild { .. }])
    ));
    assert!(matches!(
        retryable
            .child_closed(SegmentId(1), generation_one)
            .expect("retryable child closes")
            .as_slice(),
        [StreamConsumerAction::OpenChild { .. }]
    ));
}

#[test]
fn aggregate_model_orders_busy_children_and_fences_controller_replacement() {
    let mut strict = StreamConsumerModel::new(
        "topic://public/default/model".to_owned(),
        ConsumerInstanceId(11),
        ControllerIncarnation(3),
        OrderingMode::Strict,
        split_dag(),
        minimum_budget(),
    )
    .expect("strict model");
    let open = strict
        .apply_assignment(assignment(&[1]))
        .expect("strict child assignment");
    let generation = opened_generation(&open[0]);
    assert!(
        strict
            .child_opened(SegmentId(1), generation)
            .expect("strict child attaches")
            .is_empty()
    );
    assert!(matches!(
        strict.segment_phase(SegmentId(1)),
        Some(SegmentPhase::OpenBlocked(FlowBlock::OrderingUnprovable(_)))
    ));
    assert_eq!(strict.status().ordering_unprovable(), &[SegmentId(1)]);

    let mut busy = model();
    let open = busy
        .apply_assignment(assignment(&[1]))
        .expect("busy assignment");
    let generation = opened_generation(&open[0]);
    busy.child_open_busy(SegmentId(1), generation)
        .expect("busy result");
    busy.child_open_busy(SegmentId(1), generation)
        .expect("busy result is idempotent");
    assert_eq!(busy.status().pending_ownership(), &[source(1)]);
    assert!(matches!(
        busy.child_open_busy(SegmentId(1), ChildGeneration(generation.0 + 1)),
        Err(StreamConsumerModelError::StaleChildGeneration { .. })
    ));
    busy.child_opened(SegmentId(1), generation)
        .expect("busy child eventually opens");

    let mut replacement = model();
    let open = replacement
        .apply_assignment(assignment(&[1]))
        .expect("replacement assignment");
    let old_generation = opened_generation(&open[0]);
    let old_flow = replacement
        .child_opened(SegmentId(1), old_generation)
        .expect("old placement opens");
    let (old_token, _) = issue_delivery(
        &mut replacement,
        1,
        old_generation,
        flow_reservation(&old_flow),
        1,
    );
    replacement
        .resolve_delivery(&old_token)
        .expect("resolved delivery retains position metadata");
    assert!(matches!(
        replacement
            .apply_control_plane(
                split_dag_at(2, "pulsar://replacement:6650"),
                assignment_at(2, &[1]),
            )
            .expect("replace placement")
            .as_slice(),
        [StreamConsumerAction::StopFlow { .. }]
    ));
    assert_eq!(replacement.dag().epoch(), 2);
    assert_eq!(
        replacement
            .assignment()
            .map(ConsumerAssignment::layout_epoch),
        Some(2)
    );
    assert!(
        replacement
            .apply_control_plane(
                split_dag_at(3, "pulsar://replacement-again:6650"),
                assignment_at(3, &[1]),
            )
            .expect("replace an already-draining placement")
            .is_empty()
    );
    assert_eq!(replacement.dag().epoch(), 3);
    assert!(matches!(
        replacement
            .observe_terminal(SegmentId(1), old_generation)
            .expect("old placement terminal")
            .as_slice(),
        [StreamConsumerAction::CloseChild { .. }]
    ));
    let reopen = replacement
        .child_closed(SegmentId(1), old_generation)
        .expect("old placement closes");
    let replacement_generation = opened_generation(&reopen[0]);
    assert!(matches!(
        replacement.apply_control_plane_for(
            ControllerIncarnation(2),
            split_dag_at(3, "pulsar://replacement-again:6650"),
            assignment_at(3, &[1]),
        ),
        Err(StreamConsumerModelError::Assignment(_))
    ));
    assert!(matches!(
        replacement
            .begin_controller_incarnation(ControllerIncarnation(4))
            .expect("replace controller with open pending")
            .as_slice(),
        [StreamConsumerAction::CancelOpen { .. }]
    ));
    assert!(matches!(
        replacement.begin_controller_incarnation(ControllerIncarnation(4)),
        Err(StreamConsumerModelError::Assignment(_))
    ));
    assert!(replacement.close().is_ok());
    assert!(matches!(
        replacement.begin_controller_incarnation(ControllerIncarnation(5)),
        Err(StreamConsumerModelError::InvalidAggregatePhase { .. })
    ));
    assert_ne!(replacement_generation, old_generation);

    let (mut unassigned_replacement, generation, _flow) = opened_one_child();
    unassigned_replacement
        .apply_assignment(assignment(&[]))
        .expect("start an ordinary unassigned drain");
    assert!(
        unassigned_replacement
            .apply_control_plane(
                split_dag_at(2, "pulsar://unassigned-replacement:6650"),
                assignment_at(2, &[]),
            )
            .expect("upgrade the unassigned retained child replacement")
            .is_empty()
    );
    assert_eq!(
        unassigned_replacement.segment_phase(SegmentId(1)),
        Some(&SegmentPhase::Closing)
    );
    unassigned_replacement
        .child_closed(SegmentId(1), generation)
        .expect("unassigned replacement closes");
}

#[test]
fn aggregate_model_completion_barriers_balance_before_ancestor_completion() {
    let mut model = model();
    let open = model
        .apply_assignment(assignment(&[0]))
        .expect("parent assignment");
    let generation = opened_generation(&open[0]);
    model
        .child_opened(SegmentId(0), generation)
        .expect("parent child opens");
    model
        .begin_ack(SegmentId(0), generation)
        .expect("begin ack");
    model
        .begin_transactional_ack(SegmentId(0), generation)
        .expect("begin transaction ack");
    model
        .begin_pre_terminal_reservation(SegmentId(0), generation)
        .expect("begin reservation");
    model
        .observe_terminal(SegmentId(0), generation)
        .expect("observe terminal");
    assert!(matches!(
        model.complete_segment(SegmentId(0), generation),
        Err(StreamConsumerModelError::SegmentNotComplete { .. })
    ));
    model
        .settle_ack(SegmentId(0), generation)
        .expect("settle ack");
    model
        .settle_transactional_ack(SegmentId(0), generation)
        .expect("settle transaction ack");
    model
        .settle_pre_terminal_reservation(SegmentId(0), generation)
        .expect("settle reservation");
    model
        .complete_segment(SegmentId(0), generation)
        .expect("complete parent");
    assert!(matches!(
        model.settle_ack(SegmentId(0), generation),
        Err(StreamConsumerModelError::UnbalancedCompletionHook { .. })
    ));
    assert!(matches!(
        model.settle_transactional_ack(SegmentId(0), generation),
        Err(StreamConsumerModelError::UnbalancedCompletionHook { .. })
    ));
    assert!(matches!(
        model.settle_pre_terminal_reservation(SegmentId(0), generation),
        Err(StreamConsumerModelError::UnbalancedCompletionHook { .. })
    ));
}

#[test]
fn completed_sealed_segment_is_not_reopened_after_assignment_rebalance() {
    let mut sealed = model();
    let open = sealed
        .apply_assignment(assignment(&[0]))
        .expect("sealed parent assignment");
    let generation = opened_generation(&open[0]);
    sealed
        .child_opened(SegmentId(0), generation)
        .expect("sealed parent opens");
    assert!(
        sealed
            .observe_terminal(SegmentId(0), generation)
            .expect("sealed parent terminal")
            .is_empty()
    );
    sealed
        .complete_segment(SegmentId(0), generation)
        .expect("sealed parent completes");
    assert!(matches!(
        sealed
            .apply_assignment(assignment(&[]))
            .expect("transient empty rebalance")
            .as_slice(),
        [StreamConsumerAction::CloseChild { .. }]
    ));
    sealed
        .child_closed(SegmentId(0), generation)
        .expect("sealed parent closes");

    let actions = sealed
        .apply_assignment(assignment(&[0, 1]))
        .expect("balanced retained-parent assignment");
    assert!(actions.iter().all(|action| !matches!(
        action,
        StreamConsumerAction::OpenChild { source, .. } if source.segment_id() == SegmentId(0)
    )));
    assert!(actions.iter().any(|action| matches!(
        action,
        StreamConsumerAction::OpenChild { source, .. } if source.segment_id() == SegmentId(1)
    )));

    let mut deferred = model();
    let open = deferred
        .apply_assignment(assignment(&[0]))
        .expect("deferred sealed parent assignment");
    let deferred_generation = opened_generation(&open[0]);
    deferred
        .child_opened(SegmentId(0), deferred_generation)
        .expect("deferred sealed parent opens");
    deferred
        .apply_assignment(assignment(&[]))
        .expect("deferred sealed parent starts draining");
    deferred
        .apply_assignment(assignment(&[0]))
        .expect("deferred sealed parent regains assignment while draining");
    assert_eq!(deferred.pending_ownership(), vec![source(0)]);
    deferred
        .observe_terminal(SegmentId(0), deferred_generation)
        .expect("deferred sealed parent terminal");
    deferred
        .complete_segment(SegmentId(0), deferred_generation)
        .expect("deferred sealed parent completes");
    assert!(
        deferred
            .child_closed(SegmentId(0), deferred_generation)
            .expect("deferred sealed parent closes")
            .is_empty()
    );
    assert!(deferred.pending_ownership().is_empty());

    let (mut active, active_generation, _flow) = opened_one_child();
    active
        .observe_terminal(SegmentId(1), active_generation)
        .expect("active segment terminal");
    active
        .complete_segment(SegmentId(1), active_generation)
        .expect("active segment completes locally");
    active
        .apply_assignment(assignment(&[]))
        .expect("active segment loses assignment");
    active
        .child_closed(SegmentId(1), active_generation)
        .expect("active segment closes");
    assert!(matches!(
        active
            .apply_assignment(assignment(&[1]))
            .expect("active segment regains assignment")
            .as_slice(),
        [StreamConsumerAction::OpenChild { source, .. }]
            if source.segment_id() == SegmentId(1)
    ));
}

#[test]
fn aggregate_model_rejects_stale_lifecycle_position_and_acknowledgement_work() {
    let mut empty = model();
    assert!(matches!(
        empty.apply_assignment(foreign_assignment()),
        Err(StreamConsumerModelError::AssignmentParentMismatch { .. })
    ));
    assert!(empty.close().expect("close empty aggregate").is_empty());
    assert_eq!(empty.phase(), AggregatePhase::Closed);
    assert!(matches!(
        empty.require_resync(),
        Err(StreamConsumerModelError::InvalidAggregatePhase { .. })
    ));
    assert!(matches!(
        empty.child_opened(SegmentId(99), ChildGeneration(0)),
        Err(StreamConsumerModelError::InvalidAggregatePhase { .. })
    ));

    let mut opening = model();
    let open = opening
        .apply_assignment(assignment(&[1]))
        .expect("opening assignment");
    let generation = opened_generation(&open[0]);
    assert!(
        opening
            .apply_assignment(assignment(&[1]))
            .expect("identical assignment")
            .is_empty()
    );
    assert!(matches!(
        opening.child_opened(SegmentId(99), generation),
        Err(StreamConsumerModelError::UnknownChild { .. })
    ));
    assert!(matches!(
        opening
            .apply_assignment(assignment(&[]))
            .expect("revoke opening child")
            .as_slice(),
        [StreamConsumerAction::CancelOpen { .. }]
    ));
    assert!(matches!(
        opening.child_opened(SegmentId(1), generation),
        Err(StreamConsumerModelError::InvalidChildTransition { .. })
    ));
    opening
        .child_closed(SegmentId(1), generation)
        .expect("cancelled open closes");

    let (mut live, generation, flow) = opened_one_child();
    assert!(matches!(
        live.child_opened(SegmentId(1), generation),
        Err(StreamConsumerModelError::InvalidChildTransition { .. })
    ));
    assert!(matches!(
        live.child_open_busy(SegmentId(1), generation),
        Err(StreamConsumerModelError::InvalidChildTransition { .. })
    ));
    assert!(matches!(
        live.child_closed(SegmentId(1), generation),
        Err(StreamConsumerModelError::InvalidChildTransition { .. })
    ));
    assert!(matches!(
        live.seek_completed(SegmentId(1), ChildGeneration(generation.0 + 1)),
        Err(StreamConsumerModelError::StaleChildGeneration { .. })
    ));
    let (token, _) = issue_delivery(&mut live, 1, generation, flow, 70);
    let first_ack = live
        .admit_individual_acknowledgement(&token)
        .expect("admit first acknowledgement");
    assert_eq!(
        live.validate_delivery_restoration(&token),
        Err(StreamConsumerModelError::DeliveryOperationPending)
    );
    assert!(matches!(
        live.resolve_delivery(&token),
        Err(StreamConsumerModelError::DeliveryOperationPending)
    ));
    assert!(matches!(
        live.admit_individual_acknowledgement(&token),
        Err(StreamConsumerModelError::DeliveryOperationPending)
    ));
    live.cancel_acknowledgement(&first_ack.authority)
        .expect("cancel first acknowledgement");

    let wrong_layout =
        PositionVector::new(2, [(source(1), message_id(70))]).expect("wrong-layout position");
    assert!(matches!(
        live.admit_position_acknowledgement(&wrong_layout),
        Err(StreamConsumerModelError::PositionLayoutMismatch { .. })
    ));
    let unavailable =
        PositionVector::new(1, [(source(2), message_id(71))]).expect("unavailable position");
    assert!(matches!(
        live.admit_position_acknowledgement(&unavailable),
        Err(StreamConsumerModelError::PositionSourceUnavailable { .. })
    ));
    assert!(matches!(
        live.begin_seek(&PositionVector::new(1, []).expect("empty position")),
        Err(StreamConsumerModelError::SeekSourceMismatch)
    ));
    assert!(matches!(
        live.begin_seek(token.position_vector()),
        Err(StreamConsumerModelError::ConcurrentSeek)
    ));
    live.close().expect("close live aggregate");
    assert!(matches!(
        live.resolve_delivery(&token),
        Err(StreamConsumerModelError::StaleDeliveryToken)
    ));

    let (mut flowing, _generation, _flow) = opened_one_child();
    assert!(matches!(
        flowing
            .begin_controller_incarnation(ControllerIncarnation(4))
            .expect("replace flowing controller")
            .as_slice(),
        [StreamConsumerAction::CloseChild { .. }]
    ));
}

#[test]
fn aggregate_model_preallocation_is_owned_atomic_and_cancellable() {
    let mut opening = model();
    let open = opening
        .apply_assignment(assignment(&[1]))
        .expect("preallocation opening assignment");
    let generation = opened_generation(&open[0]);
    assert!(matches!(
        opening.reserve_decompression(SegmentId(1), generation, 1),
        Err(StreamConsumerModelError::InvalidChildTransition { .. })
    ));

    let mut model = model();
    let opens = model
        .apply_assignment(assignment(&[1, 2]))
        .expect("preallocation assignment");
    let generation_one = opened_generation(&opens[0]);
    let generation_two = opened_generation(&opens[1]);
    let flow_one = model
        .child_opened(SegmentId(1), generation_one)
        .expect("preallocation first child");
    let flow_two = model
        .child_opened(SegmentId(2), generation_two)
        .expect("preallocation second child");
    let flow_one = flow_reservation(&flow_one);
    let flow_two = flow_reservation(&flow_two);

    let cancelled = model
        .reserve_batch_assembly(SegmentId(1), generation_one, 64)
        .expect("reserve cancellable work");
    model
        .cancel_receive_work(SegmentId(1), generation_one, cancelled)
        .expect("cancel receive work");
    assert!(matches!(
        model.cancel_receive_work(SegmentId(1), generation_one, cancelled),
        Err(StreamConsumerModelError::Budget(
            BudgetError::UnknownReservation { .. }
        ))
    ));

    let foreign = model
        .reserve_decompression(SegmentId(2), generation_two, 64)
        .expect("reserve foreign work");
    assert!(matches!(
        model.message_arrived_preallocated(SegmentId(1), generation_one, flow_one, &[foreign], 64,),
        Err(StreamConsumerModelError::Budget(
            BudgetError::ReservationOwnerMismatch { .. }
        ))
    ));
    model
        .cancel_receive_work(SegmentId(2), generation_two, foreign)
        .expect("release foreign work");

    let duplicate = model
        .reserve_decompression(SegmentId(1), generation_one, 64)
        .expect("reserve duplicate work");
    assert!(matches!(
        model.message_arrived_preallocated(
            SegmentId(1),
            generation_one,
            flow_one,
            &[duplicate, duplicate],
            64,
        ),
        Err(StreamConsumerModelError::DuplicateReceiveWork { .. })
    ));
    model
        .cancel_receive_work(SegmentId(1), generation_one, duplicate)
        .expect("release duplicate work");

    let insufficient = model
        .reserve_decompression(SegmentId(1), generation_one, 1)
        .expect("reserve insufficient work");
    assert!(matches!(
        model.message_arrived_preallocated(
            SegmentId(1),
            generation_one,
            flow_one,
            &[insufficient],
            MAX_FRAME_SIZE + 2,
        ),
        Err(StreamConsumerModelError::Budget(
            BudgetError::PreallocationExceeded { .. }
        ))
    ));
    assert!(matches!(
        model.batch_arrived_preallocated(
            SegmentId(1),
            generation_one,
            flow_one,
            &[insufficient],
            &[],
        ),
        Err(StreamConsumerModelError::InvalidChildTransition { .. })
    ));
    assert!(matches!(
        model.batch_arrived_preallocated(SegmentId(1), generation_one, flow_two, &[], &[1],),
        Err(StreamConsumerModelError::InvalidChildTransition { .. })
    ));
    model
        .cancel_receive_work(SegmentId(1), generation_one, insufficient)
        .expect("release insufficient work");

    assert!(matches!(
        model.message_arrived(SegmentId(1), generation_one, flow_two, 1,),
        Err(StreamConsumerModelError::InvalidChildTransition { .. })
    ));

    let chunk = model
        .chunk_frame_buffered(SegmentId(1), generation_one, flow_one, None, MAX_FRAME_SIZE)
        .expect("start chunk preallocation");
    let continuation = flow_reservation(&chunk.actions);
    assert!(matches!(
        model.chunk_message_arrived(
            SegmentId(1),
            generation_one,
            continuation,
            chunk.assembly,
            &[],
            usize::MAX,
        ),
        Err(StreamConsumerModelError::Budget(
            BudgetError::PreallocationExceeded { .. }
        ))
    ));
    assert!(matches!(
        model.discard_preallocated_arrival(
            SegmentId(1),
            generation_one,
            continuation,
            magnetar_proto::FlowPurpose::Message,
            &[],
        ),
        Err(StreamConsumerModelError::FlowPurposeMismatch { .. })
    ));
    assert!(matches!(
        model.reserve_batch_assembly(SegmentId(1), generation_one, 1),
        Err(StreamConsumerModelError::FlowPurposeMismatch { .. })
    ));
    let decompression = model
        .reserve_decompression(SegmentId(1), generation_one, 1)
        .expect("chunk decompression work is permitted");
    model
        .cancel_receive_work(SegmentId(1), generation_one, decompression)
        .expect("cancel chunk decompression work");
    assert!(matches!(
        model.message_arrived(SegmentId(1), generation_one, continuation, 1),
        Err(StreamConsumerModelError::FlowPurposeMismatch { .. })
    ));
    assert!(matches!(
        model.chunk_frame_buffered(
            SegmentId(1),
            generation_one,
            continuation,
            None,
            MAX_FRAME_SIZE,
        ),
        Err(StreamConsumerModelError::FlowPurposeMismatch { .. })
    ));
    assert!(matches!(
        model.chunk_frame_buffered(
            SegmentId(1),
            generation_one,
            flow_two,
            Some(chunk.assembly),
            MAX_FRAME_SIZE,
        ),
        Err(StreamConsumerModelError::InvalidChildTransition { .. })
    ));
    assert!(matches!(
        model.chunk_frame_buffered(
            SegmentId(1),
            generation_one,
            continuation,
            Some(chunk.assembly),
            usize::MAX,
        ),
        Err(StreamConsumerModelError::Budget(_))
    ));
    let middle = model
        .chunk_frame_buffered(
            SegmentId(1),
            generation_one,
            continuation,
            Some(chunk.assembly),
            MAX_FRAME_SIZE,
        )
        .expect("rotate chunk continuation");
    assert_ne!(flow_reservation(&middle.actions), continuation);

    let (mut invalid_work, generation, flow) = opened_one_child();
    let retained = invalid_work
        .message_arrived(SegmentId(1), generation, flow, 64)
        .expect("reserve retained-message work");
    let next_flow = flow_reservation(&retained.actions);
    assert!(matches!(
        invalid_work.message_arrived_preallocated(
            SegmentId(1),
            generation,
            next_flow,
            &[retained.retained],
            64,
        ),
        Err(StreamConsumerModelError::InvalidReceiveWork { .. })
    ));
}

#[test]
fn aggregate_model_seek_and_position_edges_validate_current_sources() {
    let mut sealed = model();
    let open = sealed
        .apply_assignment(assignment(&[0]))
        .expect("sealed assignment");
    let generation = opened_generation(&open[0]);
    sealed
        .child_opened(SegmentId(0), generation)
        .expect("sealed child open");
    let sealed_position =
        PositionVector::new(1, [(source(0), message_id(1))]).expect("sealed position");
    assert!(matches!(
        sealed.begin_seek(&sealed_position),
        Err(StreamConsumerModelError::SeekNonActiveLeaf {
            segment_id: SegmentId(0)
        })
    ));

    let mut strict = StreamConsumerModel::new(
        "topic://public/default/model".to_owned(),
        ConsumerInstanceId(12),
        ControllerIncarnation(3),
        OrderingMode::Strict,
        split_dag(),
        minimum_budget(),
    )
    .expect("strict seek model");
    let open = strict
        .apply_assignment(assignment(&[1]))
        .expect("strict seek assignment");
    let generation = opened_generation(&open[0]);
    assert!(
        strict
            .child_opened(SegmentId(1), generation)
            .expect("strict seek child")
            .is_empty()
    );
    let strict_position =
        PositionVector::new(1, [(source(1), message_id(2))]).expect("strict position");
    assert!(strict.begin_seek(&strict_position).is_ok());
    assert!(matches!(
        strict.begin_seek(&strict_position),
        Err(StreamConsumerModelError::ConcurrentSeek)
    ));

    let mut closing_resync = model();
    let opening = closing_resync
        .apply_assignment(assignment(&[1]))
        .expect("closing resync assignment");
    let opening_generation = opened_generation(&opening[0]);
    assert!(matches!(
        closing_resync.observe_terminal(SegmentId(1), opening_generation),
        Err(StreamConsumerModelError::InvalidChildTransition { .. })
    ));
    closing_resync
        .apply_assignment(assignment(&[]))
        .expect("cancel opening child before resync");
    assert!(
        closing_resync
            .require_resync()
            .expect("resync ignores an already-closing child")
            .is_empty()
    );

    let (mut positions, generation, flow) = opened_one_child();
    let (token, _) = issue_delivery(&mut positions, 1, generation, flow, 3);
    let restored = token.position_vector().clone();
    let acknowledgement = positions
        .admit_position_acknowledgement(&restored)
        .expect("admit current restored position");
    let confirmed = acknowledgement
        .components
        .iter()
        .map(|component| component.source().clone())
        .collect();
    positions
        .settle_acknowledgement(&acknowledgement.authority, &confirmed)
        .expect("settle restored position");

    let (mut draining, generation, flow) = opened_one_child();
    let (token, _) = issue_delivery(&mut draining, 1, generation, flow, 4);
    draining
        .apply_assignment(assignment(&[]))
        .expect("revoke position source");
    assert!(matches!(
        draining.admit_position_acknowledgement(token.position_vector()),
        Err(StreamConsumerModelError::PositionSourceUnavailable { .. })
    ));

    let (mut authority_owner, generation, flow) = opened_one_child();
    let (token, _) = issue_delivery(&mut authority_owner, 1, generation, flow, 5);
    let authority = authority_owner
        .admit_individual_acknowledgement(&token)
        .expect("admit owner acknowledgement");
    let mut foreign_owner = StreamConsumerModel::new(
        "topic://public/default/model".to_owned(),
        ConsumerInstanceId(99),
        ControllerIncarnation(3),
        OrderingMode::BrokerManaged,
        split_dag(),
        minimum_budget(),
    )
    .expect("foreign authority model");
    assert!(matches!(
        foreign_owner.settle_acknowledgement(&authority.authority, &BTreeSet::new()),
        Err(StreamConsumerModelError::StaleAcknowledgementAuthority)
    ));

    let mut closing_child = model();
    let open = closing_child
        .apply_assignment(assignment(&[1]))
        .expect("closing-child assignment");
    let generation = opened_generation(&open[0]);
    closing_child
        .apply_assignment(assignment(&[]))
        .expect("close unopened child");
    assert!(
        closing_child
            .begin_controller_incarnation(ControllerIncarnation(4))
            .expect("replace controller around closing child")
            .is_empty()
    );
    assert!(matches!(
        closing_child.child_open_busy(SegmentId(99), generation),
        Err(StreamConsumerModelError::UnknownChild { .. })
    ));

    let (mut source_fence, generation, flow) = opened_one_child();
    let arrival = source_fence
        .message_arrived(SegmentId(1), generation, flow, 128)
        .expect("source-fence arrival");
    assert!(matches!(
        source_fence.issue_delivery(SegmentId(1), generation, stream_id(2, 10), arrival.retained,),
        Err(StreamConsumerModelError::DeliverySourceMismatch { .. })
    ));
    source_fence
        .issue_delivery(SegmentId(1), generation, stream_id(1, 10), arrival.retained)
        .expect("issue source-fenced delivery");
    let next_flow = flow_reservation(&arrival.actions);
    let next = source_fence
        .message_arrived(SegmentId(1), generation, next_flow, 128)
        .expect("second source-fence arrival");
    source_fence
        .issue_delivery(SegmentId(1), generation, stream_id(1, 11), next.retained)
        .expect("second delivery updates retained position metadata");
    let third_flow = flow_reservation(&next.actions);
    let third = source_fence
        .message_arrived(SegmentId(1), generation, third_flow, 128)
        .expect("third source-fence arrival");
    let non_advancing = source_fence
        .issue_delivery(SegmentId(1), generation, stream_id(1, 10), third.retained)
        .expect("older delivery does not replace the retained position");
    assert_eq!(
        non_advancing
            .position_vector()
            .iter()
            .next()
            .expect("one delivered position")
            .1
            .entry_id,
        11
    );
}

#[test]
fn receive_state_reassembles_chunks_and_expands_partial_batches() {
    let (mut chunk_model, generation, first_flow) = opened_one_child();
    let mut receive = StreamReceiveState::default();
    let first_metadata = pb::MessageMetadata {
        num_chunks_from_msg: Some(3),
        chunk_id: Some(0),
        total_chunk_msg_size: Some(6),
        uuid: Some("chunk-chain".to_owned()),
        ..Default::default()
    };
    let first = receive
        .accept_entry(
            &mut chunk_model,
            SegmentId(1),
            generation,
            first_flow,
            deferred_entry(10, Bytes::from_static(b"ab"), first_metadata, Vec::new(), 1),
        )
        .expect("buffer first chunk");
    let continuation_flow = match first {
        StreamEntryAcceptance::Buffered { actions } => flow_reservation(&actions),
        other => panic!("expected buffered chunk, got {other:?}"),
    };
    let second_metadata = pb::MessageMetadata {
        num_chunks_from_msg: Some(3),
        chunk_id: Some(1),
        total_chunk_msg_size: Some(6),
        uuid: Some("chunk-chain".to_owned()),
        ..Default::default()
    };
    let middle = match receive
        .accept_entry(
            &mut chunk_model,
            SegmentId(1),
            generation,
            continuation_flow,
            deferred_entry(
                11,
                Bytes::from_static(b"cd"),
                second_metadata,
                Vec::new(),
                1,
            ),
        )
        .expect("buffer middle chunk")
    {
        StreamEntryAcceptance::Buffered { actions } => flow_reservation(&actions),
        other => panic!("expected buffered middle chunk, got {other:?}"),
    };
    let final_metadata = pb::MessageMetadata {
        num_chunks_from_msg: Some(3),
        chunk_id: Some(2),
        total_chunk_msg_size: Some(6),
        uuid: Some("chunk-chain".to_owned()),
        ..Default::default()
    };
    let mut complete = match receive
        .accept_entry(
            &mut chunk_model,
            SegmentId(1),
            generation,
            middle,
            deferred_entry(12, Bytes::from_static(b"ef"), final_metadata, Vec::new(), 1),
        )
        .expect("complete chunk chain")
    {
        StreamEntryAcceptance::Complete(entry) => entry,
        other => panic!("expected completed chunk, got {other:?}"),
    };
    assert_eq!(complete.message_id().entry_id, 12);
    assert_eq!(complete.message_id_data().entry_id, 12);
    assert_eq!(
        complete
            .message_id_data()
            .first_chunk_message_id
            .as_deref()
            .map(|id| id.entry_id),
        Some(10)
    );
    assert_eq!(complete.transform_reservation_bytes(), Ok(0));
    complete.message_mut().redelivery_count = 2;
    let chunk = receive
        .finalize_entry(&mut chunk_model, SegmentId(1), generation, complete, &[])
        .expect("account assembled chunk");
    assert_eq!(chunk.messages.len(), 1);
    assert_eq!(
        chunk.messages[0].message.payload,
        Bytes::from_static(b"abcdef")
    );
    assert_eq!(chunk.messages[0].message.redelivery_count, 2);
    assert_eq!(chunk.permit_debt, 0);
    receive.remove_child(SegmentId(1), generation);

    let (mut batch_model, generation, flow) = opened_one_child();
    let mut batch_receive = StreamReceiveState::default();
    let batch_metadata = pb::MessageMetadata {
        num_messages_in_batch: Some(2),
        ..Default::default()
    };
    let complete = match batch_receive
        .accept_entry(
            &mut batch_model,
            SegmentId(1),
            generation,
            flow,
            deferred_entry(
                20,
                encoded_batch(&[b"one", b"two"]),
                batch_metadata.clone(),
                vec![1],
                2,
            ),
        )
        .expect("accept partial batch")
    {
        StreamEntryAcceptance::Complete(entry) => entry,
        other => panic!("expected complete batch, got {other:?}"),
    };
    let batch = batch_receive
        .finalize_entry(&mut batch_model, SegmentId(1), generation, complete, &[])
        .expect("expand partial batch");
    assert_eq!(batch.messages.len(), 1);
    assert_eq!(
        batch.messages[0].message.payload,
        Bytes::from_static(b"one")
    );
    assert_eq!(batch.messages[0].message_id_data.batch_index, Some(0));
    assert_eq!(batch.messages[0].message_id_data.batch_size, Some(2));
    assert_eq!(batch.messages[0].message_id_data.ack_set, vec![1]);
    assert_eq!(batch.permit_debt, 1);

    let next_flow = flow_reservation(&batch.actions);
    let all_settled = match batch_receive
        .accept_entry(
            &mut batch_model,
            SegmentId(1),
            generation,
            next_flow,
            deferred_entry(
                21,
                encoded_batch(&[b"three", b"four"]),
                batch_metadata,
                vec![0],
                2,
            ),
        )
        .expect("accept fully settled batch")
    {
        StreamEntryAcceptance::Complete(entry) => entry,
        other => panic!("expected complete settled batch, got {other:?}"),
    };
    let all_settled = batch_receive
        .finalize_entry(&mut batch_model, SegmentId(1), generation, all_settled, &[])
        .expect("discard fully settled batch");
    assert!(all_settled.messages.is_empty());
    assert_eq!(all_settled.permit_debt, 1);
}

#[test]
fn receive_state_accounts_transform_work_and_discards_ordinary_and_chunk_entries() {
    let (mut zstd_model, zstd_generation, zstd_flow) = opened_one_child();
    let mut zstd_receive = StreamReceiveState::default();
    let zstd = match zstd_receive
        .accept_entry(
            &mut zstd_model,
            SegmentId(1),
            zstd_generation,
            zstd_flow,
            deferred_entry(
                0,
                Bytes::from_static(b"zstd"),
                pb::MessageMetadata {
                    compression: Some(pb::CompressionType::Zstd as i32),
                    uncompressed_size: Some(1_024),
                    ..Default::default()
                },
                Vec::new(),
                1,
            ),
        )
        .expect("accept zstd frame")
    {
        StreamEntryAcceptance::Complete(entry) => entry,
        other => panic!("expected complete zstd entry, got {other:?}"),
    };
    assert_eq!(
        zstd.transform_reservation_bytes()
            .expect("bounded zstd reservation"),
        1_024
            + DECOMPRESSION_VALIDATION_SLACK
            + magnetar_proto::stream_consumer::ZSTD_DECOMPRESSION_CONTEXT_WORKSPACE
            + magnetar_proto::frame::ZSTD_MIN_WINDOW_SIZE
    );

    let (mut ordinary_model, generation, flow) = opened_one_child();
    let mut ordinary_receive = StreamReceiveState::default();
    let metadata = pb::MessageMetadata {
        encryption_keys: vec![pb::EncryptionKeys {
            key: "key".to_owned(),
            value: Bytes::from_static(b"wrapped"),
            metadata: Vec::new(),
        }],
        compression: Some(pb::CompressionType::Lz4 as i32),
        uncompressed_size: Some(512),
        ..Default::default()
    };
    let ordinary = match ordinary_receive
        .accept_entry(
            &mut ordinary_model,
            SegmentId(1),
            generation,
            flow,
            deferred_entry(
                60,
                Bytes::from_static(b"encrypted"),
                metadata,
                Vec::new(),
                1,
            ),
        )
        .expect("accept transformed ordinary entry")
    {
        StreamEntryAcceptance::Complete(entry) => entry,
        other => panic!("expected complete ordinary entry, got {other:?}"),
    };
    let transform_bytes = ordinary
        .transform_reservation_bytes()
        .expect("bounded transform bytes");
    assert_eq!(
        transform_bytes,
        b"encrypted".len() + 512 + DECOMPRESSION_VALIDATION_SLACK
    );
    let transform = ordinary_model
        .reserve_decompression(SegmentId(1), generation, transform_bytes)
        .expect("reserve transform workspace");
    assert!(matches!(
        ordinary_receive
            .discard_entry(
                &mut ordinary_model,
                SegmentId(1),
                generation,
                ordinary,
                &[transform],
            )
            .expect("discard transformed ordinary entry")
            .as_slice(),
        [StreamConsumerAction::GrantFlow { .. }]
    ));

    let (mut chunk_model, generation, flow) = opened_one_child();
    let mut chunk_receive = StreamReceiveState::default();
    let first = pb::MessageMetadata {
        num_chunks_from_msg: Some(2),
        chunk_id: Some(0),
        total_chunk_msg_size: Some(2),
        uuid: Some("discard-chain".to_owned()),
        ..Default::default()
    };
    let continuation = match chunk_receive
        .accept_entry(
            &mut chunk_model,
            SegmentId(1),
            generation,
            flow,
            deferred_entry(61, Bytes::from_static(b"a"), first, Vec::new(), 1),
        )
        .expect("buffer discarded chunk")
    {
        StreamEntryAcceptance::Buffered { actions } => flow_reservation(&actions),
        other => panic!("expected buffered chunk, got {other:?}"),
    };
    let second = pb::MessageMetadata {
        num_chunks_from_msg: Some(2),
        chunk_id: Some(1),
        total_chunk_msg_size: Some(2),
        uuid: Some("discard-chain".to_owned()),
        ..Default::default()
    };
    let complete = match chunk_receive
        .accept_entry(
            &mut chunk_model,
            SegmentId(1),
            generation,
            continuation,
            deferred_entry(62, Bytes::from_static(b"b"), second, Vec::new(), 1),
        )
        .expect("complete discarded chunk")
    {
        StreamEntryAcceptance::Complete(entry) => entry,
        other => panic!("expected complete chunk, got {other:?}"),
    };
    assert!(matches!(
        chunk_receive
            .discard_entry(&mut chunk_model, SegmentId(1), generation, complete, &[],)
            .expect("discard complete chunk")
            .as_slice(),
        [StreamConsumerAction::GrantFlow { .. }]
    ));

    let (mut one_chunk_model, generation, flow) = opened_one_child();
    let mut one_chunk_receive = StreamReceiveState::default();
    let one_chunk = pb::MessageMetadata {
        num_chunks_from_msg: Some(1),
        chunk_id: Some(0),
        total_chunk_msg_size: Some(1),
        uuid: Some("single-frame".to_owned()),
        ..Default::default()
    };
    let complete = match one_chunk_receive
        .accept_entry(
            &mut one_chunk_model,
            SegmentId(1),
            generation,
            flow,
            deferred_entry(63, Bytes::from_static(b"x"), one_chunk, Vec::new(), 1),
        )
        .expect("single chunk is an ordinary entry")
    {
        StreamEntryAcceptance::Complete(entry) => entry,
        other => panic!("expected ordinary single chunk, got {other:?}"),
    };
    one_chunk_receive
        .discard_entry(
            &mut one_chunk_model,
            SegmentId(1),
            generation,
            complete,
            &[],
        )
        .expect("discard single chunk entry");

    let (mut finalized_model, generation, flow) = opened_one_child();
    let mut finalized_receive = StreamReceiveState::default();
    let transformed = match finalized_receive
        .accept_entry(
            &mut finalized_model,
            SegmentId(1),
            generation,
            flow,
            deferred_entry(
                64,
                Bytes::from_static(b"compressed"),
                pb::MessageMetadata {
                    compression: Some(pb::CompressionType::Lz4 as i32),
                    uncompressed_size: Some(256),
                    ..Default::default()
                },
                Vec::new(),
                1,
            ),
        )
        .expect("accept transformed entry")
    {
        StreamEntryAcceptance::Complete(entry) => entry,
        other => panic!("expected transformed entry, got {other:?}"),
    };
    let transform = finalized_model
        .reserve_decompression(
            SegmentId(1),
            generation,
            transformed
                .transform_reservation_bytes()
                .expect("transform reservation"),
        )
        .expect("reserve finalized transform work");
    assert_eq!(
        finalized_receive
            .finalize_entry(
                &mut finalized_model,
                SegmentId(1),
                generation,
                transformed,
                &[transform],
            )
            .expect("finalize transformed entry")
            .messages
            .len(),
        1
    );
}

#[test]
fn repeated_near_limit_batch_ids_exhaust_budget_atomically() {
    const BATCH_SIZE: usize = 32;
    const DATA_HEADROOM: usize = 1024 * 1024;

    let mut compact_model = model_with_data_capacity(MAX_FRAME_SIZE + DATA_HEADROOM);
    let compact_open = compact_model
        .apply_assignment(assignment(&[1]))
        .expect("compact assignment");
    let compact_generation = opened_generation(&compact_open[0]);
    let compact_flow = compact_model
        .child_opened(SegmentId(1), compact_generation)
        .expect("compact child open");
    let payloads = vec![b"x".as_slice(); BATCH_SIZE];
    let mut compact_receive = StreamReceiveState::default();
    let compact = match compact_receive
        .accept_entry(
            &mut compact_model,
            SegmentId(1),
            compact_generation,
            flow_reservation(&compact_flow),
            deferred_entry(
                69,
                encoded_batch(&payloads),
                pb::MessageMetadata {
                    num_messages_in_batch: Some(BATCH_SIZE as i32),
                    ..Default::default()
                },
                vec![-1],
                BATCH_SIZE as u32,
            ),
        )
        .expect("accept compact-id batch frame")
    {
        StreamEntryAcceptance::Complete(entry) => entry,
        other => panic!("expected complete compact batch, got {other:?}"),
    };
    assert_eq!(
        compact_receive
            .finalize_entry(
                &mut compact_model,
                SegmentId(1),
                compact_generation,
                compact,
                &[],
            )
            .expect("compact-id batch fits the calibrated headroom")
            .messages
            .len(),
        BATCH_SIZE
    );

    let mut model = model_with_data_capacity(MAX_FRAME_SIZE + DATA_HEADROOM);
    let open = model
        .apply_assignment(assignment(&[1]))
        .expect("large-id assignment");
    let generation = opened_generation(&open[0]);
    let flow_actions = model
        .child_opened(SegmentId(1), generation)
        .expect("large-id child open");
    let flow = flow_reservation(&flow_actions);
    let mut receive = StreamReceiveState::default();
    let mut entry = deferred_entry(
        70,
        encoded_batch(&payloads),
        pb::MessageMetadata {
            num_messages_in_batch: Some(BATCH_SIZE as i32),
            ..Default::default()
        },
        vec![-1],
        BATCH_SIZE as u32,
    );
    entry.message_id_data.first_chunk_message_id = Some(Box::new(pb::MessageIdData {
        ledger_id: 1,
        entry_id: 69,
        partition: Some(0),
        batch_index: Some(-1),
        ack_set: vec![-1; 5_900],
        batch_size: None,
        first_chunk_message_id: None,
    }));
    let complete = match receive
        .accept_entry(&mut model, SegmentId(1), generation, flow, entry)
        .expect("accept large-id batch frame")
    {
        StreamEntryAcceptance::Complete(entry) => entry,
        other => panic!("expected complete batch, got {other:?}"),
    };
    let ordinary_size = complete.message_id_data().encoded_len();
    assert!(
        ordinary_size <= magnetar_proto::MAX_ORDINARY_MESSAGE_ID_SIZE,
        "canonical id uses {ordinary_size} bytes"
    );
    assert!(
        ordinary_size > magnetar_proto::MAX_ORDINARY_MESSAGE_ID_SIZE - 1_024,
        "canonical id uses {ordinary_size} bytes"
    );
    let budget_before = model.status().receiver_budget_used();

    match receive.finalize_entry(&mut model, SegmentId(1), generation, complete, &[]) {
        Err(StreamConsumerModelError::Budget(BudgetError::Exhausted { .. })) => {}
        unexpected => panic!("unexpected repeated-id batch result: {unexpected:?}"),
    }
    assert_eq!(model.status().receiver_budget_used(), budget_before);
    assert_eq!(model.status().attached_segments(), 1);
}

#[test]
fn receive_state_rejects_malformed_chunks_and_batches_without_partial_accounting() {
    let mut bounded = model_with_data_capacity(MAX_FRAME_SIZE);
    let open = bounded
        .apply_assignment(assignment(&[1]))
        .expect("bounded chunk assignment");
    let bounded_generation = opened_generation(&open[0]);
    let bounded_flow = bounded
        .child_opened(SegmentId(1), bounded_generation)
        .expect("bounded chunk child");
    let mut bounded_receive = StreamReceiveState::default();
    assert!(matches!(
        bounded_receive.accept_entry(
            &mut bounded,
            SegmentId(1),
            bounded_generation,
            flow_reservation(&bounded_flow),
            deferred_entry(
                29,
                Bytes::from_static(b"x"),
                pb::MessageMetadata {
                    num_chunks_from_msg: Some(2),
                    chunk_id: Some(0),
                    total_chunk_msg_size: Some(
                        i32::try_from(MAX_FRAME_SIZE).expect("frame size fits i32"),
                    ),
                    uuid: Some("over-budget-chain".to_owned()),
                    ..Default::default()
                },
                Vec::new(),
                1,
            ),
        ),
        Err(StreamConsumerModelError::Budget(_))
    ));

    let (mut model, generation, flow) = opened_one_child();
    let mut receive = StreamReceiveState::default();
    let used = model.budget().data_used();
    let invalid = [
        pb::MessageMetadata {
            num_chunks_from_msg: Some(magnetar_proto::consumer::MAX_CHUNK_TOTAL + 1),
            chunk_id: Some(0),
            total_chunk_msg_size: Some(1),
            uuid: Some("too-many".to_owned()),
            ..Default::default()
        },
        pb::MessageMetadata {
            num_chunks_from_msg: Some(2),
            num_messages_in_batch: Some(2),
            chunk_id: Some(0),
            total_chunk_msg_size: Some(1),
            uuid: Some("batch-chunk".to_owned()),
            ..Default::default()
        },
        pb::MessageMetadata {
            num_chunks_from_msg: Some(2),
            total_chunk_msg_size: Some(1),
            uuid: Some("missing-id".to_owned()),
            ..Default::default()
        },
        pb::MessageMetadata {
            num_chunks_from_msg: Some(2),
            chunk_id: Some(2),
            total_chunk_msg_size: Some(1),
            uuid: Some("bad-id".to_owned()),
            ..Default::default()
        },
        pb::MessageMetadata {
            num_chunks_from_msg: Some(2),
            chunk_id: Some(0),
            total_chunk_msg_size: Some(0),
            uuid: Some("bad-size".to_owned()),
            ..Default::default()
        },
        pb::MessageMetadata {
            num_chunks_from_msg: Some(2),
            chunk_id: Some(0),
            total_chunk_msg_size: Some(1),
            ..Default::default()
        },
        pb::MessageMetadata {
            num_chunks_from_msg: Some(2),
            chunk_id: Some(1),
            total_chunk_msg_size: Some(1),
            uuid: Some("late-start".to_owned()),
            ..Default::default()
        },
    ];
    for (index, metadata) in invalid.into_iter().enumerate() {
        assert!(matches!(
            receive.accept_entry(
                &mut model,
                SegmentId(1),
                generation,
                flow,
                deferred_entry(
                    30 + index as u64,
                    Bytes::from_static(b"x"),
                    metadata,
                    Vec::new(),
                    1,
                ),
            ),
            Err(StreamConsumerModelError::InvalidChunkFrame(_))
        ));
        assert_eq!(model.budget().data_used(), used);
    }
    let oversized_first = pb::MessageMetadata {
        num_chunks_from_msg: Some(2),
        chunk_id: Some(0),
        total_chunk_msg_size: Some(1),
        uuid: Some("oversized-first".to_owned()),
        ..Default::default()
    };
    assert!(matches!(
        receive.accept_entry(
            &mut model,
            SegmentId(1),
            generation,
            flow,
            deferred_entry(
                39,
                Bytes::from_static(b"too large"),
                oversized_first,
                Vec::new(),
                1,
            ),
        ),
        Err(StreamConsumerModelError::InvalidChunkFrame(_))
    ));

    let first_metadata = pb::MessageMetadata {
        num_chunks_from_msg: Some(2),
        chunk_id: Some(0),
        total_chunk_msg_size: Some(2),
        uuid: Some("valid-chain".to_owned()),
        ..Default::default()
    };
    let continuation = match receive
        .accept_entry(
            &mut model,
            SegmentId(1),
            generation,
            flow,
            deferred_entry(
                40,
                Bytes::from_static(b"a"),
                first_metadata.clone(),
                Vec::new(),
                1,
            ),
        )
        .expect("buffer valid first chunk")
    {
        StreamEntryAcceptance::Buffered { actions } => flow_reservation(&actions),
        other => panic!("expected buffered chunk, got {other:?}"),
    };
    assert!(matches!(
        receive.accept_entry(
            &mut model,
            SegmentId(1),
            generation,
            continuation,
            deferred_entry(40, Bytes::from_static(b"a"), first_metadata, Vec::new(), 1,),
        ),
        Err(StreamConsumerModelError::InvalidChunkFrame(_))
    ));
    let wrong_final_size = pb::MessageMetadata {
        num_chunks_from_msg: Some(2),
        chunk_id: Some(1),
        total_chunk_msg_size: Some(2),
        uuid: Some("valid-chain".to_owned()),
        ..Default::default()
    };
    assert!(matches!(
        receive.accept_entry(
            &mut model,
            SegmentId(1),
            generation,
            continuation,
            deferred_entry(41, Bytes::new(), wrong_final_size, Vec::new(), 1,),
        ),
        Err(StreamConsumerModelError::InvalidChunkFrame(_))
    ));
    let mismatched = pb::MessageMetadata {
        num_chunks_from_msg: Some(2),
        chunk_id: Some(1),
        total_chunk_msg_size: Some(2),
        uuid: Some("another-chain".to_owned()),
        ..Default::default()
    };
    assert!(matches!(
        receive.accept_entry(
            &mut model,
            SegmentId(1),
            generation,
            continuation,
            deferred_entry(41, Bytes::from_static(b"b"), mismatched, Vec::new(), 1,),
        ),
        Err(StreamConsumerModelError::InvalidChunkFrame(_))
    ));
    let correct_final = pb::MessageMetadata {
        num_chunks_from_msg: Some(2),
        chunk_id: Some(1),
        total_chunk_msg_size: Some(2),
        uuid: Some("valid-chain".to_owned()),
        ..Default::default()
    };
    let mut chunked_batch = match receive
        .accept_entry(
            &mut model,
            SegmentId(1),
            generation,
            continuation,
            deferred_entry(42, Bytes::from_static(b"b"), correct_final, Vec::new(), 1),
        )
        .expect("complete valid chain")
    {
        StreamEntryAcceptance::Complete(entry) => entry,
        other => panic!("expected completed chain, got {other:?}"),
    };
    Arc::make_mut(&mut chunked_batch.message_mut().metadata).num_messages_in_batch = Some(2);
    assert!(matches!(
        receive.finalize_entry(&mut model, SegmentId(1), generation, chunked_batch, &[],),
        Err(StreamConsumerModelError::InvalidBatchFrame(_))
    ));

    let (mut batch_model, batch_generation, batch_flow) = opened_one_child();
    let mut batch_receive = StreamReceiveState::default();
    let metadata = pb::MessageMetadata {
        num_messages_in_batch: Some(2),
        ..Default::default()
    };
    let truncated = match batch_receive
        .accept_entry(
            &mut batch_model,
            SegmentId(1),
            batch_generation,
            batch_flow,
            deferred_entry(50, Bytes::new(), metadata.clone(), Vec::new(), 2),
        )
        .expect("accept structurally deferred batch")
    {
        StreamEntryAcceptance::Complete(entry) => entry,
        other => panic!("expected complete batch, got {other:?}"),
    };
    assert!(matches!(
        batch_receive.finalize_entry(
            &mut batch_model,
            SegmentId(1),
            batch_generation,
            truncated,
            &[],
        ),
        Err(StreamConsumerModelError::InvalidBatchFrame(_))
    ));

    let mut trailing_payload = BytesMut::from(encoded_batch(&[b"one", b"two"]).as_ref());
    trailing_payload.extend_from_slice(b"trailing");
    let trailing = match batch_receive
        .accept_entry(
            &mut batch_model,
            SegmentId(1),
            batch_generation,
            batch_flow,
            deferred_entry(51, trailing_payload.freeze(), metadata, Vec::new(), 2),
        )
        .expect("accept trailing batch")
    {
        StreamEntryAcceptance::Complete(entry) => entry,
        other => panic!("expected complete batch, got {other:?}"),
    };
    assert!(matches!(
        batch_receive.finalize_entry(
            &mut batch_model,
            SegmentId(1),
            batch_generation,
            trailing,
            &[],
        ),
        Err(StreamConsumerModelError::InvalidBatchFrame(_))
    ));

    let mut metadata_truncated = BytesMut::new();
    metadata_truncated.put_u32(8);
    metadata_truncated.extend_from_slice(b"x");
    let mut invalid_protobuf = BytesMut::new();
    invalid_protobuf.put_u32(1);
    invalid_protobuf.extend_from_slice(&[0xff]);
    let negative_single = pb::SingleMessageMetadata {
        payload_size: -1,
        ..Default::default()
    }
    .encode_to_vec();
    let mut negative_payload = BytesMut::new();
    negative_payload.put_u32(u32::try_from(negative_single.len()).expect("small metadata"));
    negative_payload.extend_from_slice(&negative_single);
    let short_single = pb::SingleMessageMetadata {
        payload_size: 4,
        ..Default::default()
    }
    .encode_to_vec();
    let mut short_payload = BytesMut::new();
    short_payload.put_u32(u32::try_from(short_single.len()).expect("small metadata"));
    short_payload.extend_from_slice(&short_single);
    short_payload.extend_from_slice(b"x");
    for (index, payload) in [
        metadata_truncated.freeze(),
        invalid_protobuf.freeze(),
        negative_payload.freeze(),
        short_payload.freeze(),
    ]
    .into_iter()
    .enumerate()
    {
        let (mut invalid_model, invalid_generation, invalid_flow) = opened_one_child();
        let mut invalid_receive = StreamReceiveState::default();
        let invalid = match invalid_receive
            .accept_entry(
                &mut invalid_model,
                SegmentId(1),
                invalid_generation,
                invalid_flow,
                deferred_entry(
                    60 + index as u64,
                    payload,
                    pb::MessageMetadata {
                        num_messages_in_batch: Some(2),
                        ..Default::default()
                    },
                    Vec::new(),
                    2,
                ),
            )
            .expect("accept malformed deferred batch")
        {
            StreamEntryAcceptance::Complete(entry) => entry,
            other => panic!("expected complete malformed batch, got {other:?}"),
        };
        assert!(matches!(
            invalid_receive.finalize_entry(
                &mut invalid_model,
                SegmentId(1),
                invalid_generation,
                invalid,
                &[],
            ),
            Err(StreamConsumerModelError::InvalidBatchFrame(_))
        ));
    }
}
