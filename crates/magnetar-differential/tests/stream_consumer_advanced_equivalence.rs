// SPDX-License-Identifier: Apache-2.0

//! Advanced public scalable-consumer ordering, seek, transaction, and control-plane parity.

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]
#![allow(clippy::panic)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::await_holding_lock)]

mod stream_consumer_support;

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use magnetar::proto::schema::BytesSchema;
use magnetar::scalable::{
    BatchReceivePolicy, SegmentSubscriberApi, StreamConsumer, StreamConsumerEvent, StreamMessage,
};
use magnetar::{Engine, PulsarClient, TransactionApi};
use magnetar_fakes::m1::{
    BrokerFailure, DrainEligibility, Endpoint, FakeTransactionState, FullAssignment, M1FakeCluster,
    M1Segment, OperationKind, PendingCompletion, ResourceCounts, ScriptedBehavior,
};
use prost::Message as _;
use stream_consumer_support::client::{
    connect_moonpool, connect_moonpool_with_terminal_reconnect_budget, connect_tokio,
    connect_tokio_with_terminal_reconnect_budget,
};
use stream_consumer_support::server::M1SocketCluster;

const TOPIC: &str = "topic://public/default/scaled";

fn two_frame_receiver_budget() -> magnetar::proto::ReceiverBudget {
    magnetar::proto::ReceiverBudget::bytes(32 * 1024 * 1024)
        .expect("two-frame differential receive budget")
}

static ADVANCED_SOCKET_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn advanced_socket_test_guard() -> std::sync::MutexGuard<'static, ()> {
    // LLVM coverage makes these socket-heavy scenarios interfere when the test
    // harness runs all advanced independent Tokio runtimes at once.
    ADVANCED_SOCKET_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalAncestryTrace {
    split_barriers: Vec<bool>,
    merge_barriers: Vec<bool>,
    split_flowing: Vec<u64>,
    merge_flowing: Vec<u64>,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SealedPlacementTrace {
    descendants_blocked_before_ack: bool,
    parent_subscribes: usize,
    descendant_subscribes: Vec<u64>,
    descendant_flow_commands: Vec<u64>,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderingModeTrace {
    ordering_unprovable: Vec<u64>,
    event_ancestors: Vec<u64>,
    descendant_flow_commands: usize,
    independent_cross_member_ancestry: bool,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CrossMemberTrace {
    strict: OrderingModeTrace,
    broker_managed: OrderingModeTrace,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SeekTrace {
    live_delivery_seek_limitation: String,
    successful_seek_segments: Vec<u64>,
    first_replayed_entries: Vec<(u64, u64)>,
    stale_token_failures: Vec<String>,
    failed_seek_segments: Vec<u64>,
    failed_seek_error: String,
    resync_events: usize,
    controller_incarnations: usize,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
struct TransactionTrace {
    commit_waited_without_wire_end: bool,
    commit_registrations: usize,
    commit_staged_acks: usize,
    committed_cursors: Vec<u64>,
    abort_redeliveries: Vec<(u64, u32)>,
    aborted_cursors: Vec<u64>,
    poison_error: String,
    poison_commit_commands: usize,
    poison_abort_commands: usize,
    registration_failure_error: String,
    registration_failure_aborted: bool,
    concurrent_registration_shared: bool,
    cancelled_commit_commands: usize,
    pending_commit_cancelled: bool,
    confirmed_commit_retried_without_wire_end: bool,
    outcome_retry_reused_retained_close: bool,
    failed_finalization_errors: (bool, bool),
    failed_commit_reported_unknown: bool,
    unknown_outcome_event: bool,
    unknown_resync_event: bool,
    confirmed_abort_retried_without_wire_end: bool,
    failed_abort_reported_unknown: bool,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
struct CancellationTrace {
    ordinary_ack_retried: bool,
    transaction_registration_cancelled: bool,
    transaction_poisoned: String,
    transaction_aborted: bool,
    seek_cancellation_resync: bool,
    stale_seek_completion_fenced: bool,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
struct ControlPlaneTrace {
    push_preceded_response: bool,
    assignments: Vec<(u64, Vec<u64>)>,
    controller_incarnations: usize,
    replacement_baseline: (u64, Vec<u64>),
    final_status_epoch: Option<u64>,
    equal_epoch_ack_retained: bool,
    lower_epoch_fenced: bool,
    alignment_failure_reported: bool,
    alignment_failure_applied_epoch_three: bool,
    alignment_retry_reported: bool,
    alignment_retry_baseline: (u64, Vec<u64>),
    close_interrupted_alignment: bool,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TerminalControllerTrace {
    terminal_failure_reported: bool,
    replacement_assignment_applied: bool,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TerminalDagReconnectTrace {
    terminal_reason: String,
    replacement_assignment_applied: bool,
    queued_delivery_fenced: bool,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeliveryShapeTrace {
    compressed_payloads: Vec<Vec<u8>>,
    compressed_batch_indexes: Vec<i32>,
    partial_payloads: Vec<Vec<u8>>,
    partial_batch_indexes: Vec<i32>,
    chunk_payload: Vec<u8>,
    chunk_first_entry: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
struct ChildOpenTrace {
    busy_segment_attempts: usize,
    permanent_failure_resynced: bool,
    permanent_failure_retried: bool,
    permanent_failure_attempts: usize,
    cancelled_open_removed: bool,
    cancelled_open_close_failed: bool,
    provisional_close_failures: usize,
    provisional_close_routes: usize,
    after_close: ResourceCounts,
}

async fn withdraw_failed_child_open<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
    suffix: &str,
    completion: BrokerFailure,
) where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let subscription = format!("withdraw-{suffix}-sub");
    let member_name = format!("withdraw-{suffix}-member");
    let takeover_name = format!("withdraw-{suffix}-takeover");
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::SegmentOpen,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::ConsumerBusy,
                    "park child ownership retry",
                )),
            )?;
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::SegmentOpen,
                ScriptedBehavior::Delay,
            )
        })
        .expect("script stale child-open failure");
    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription(subscription.clone())
        .consumer_name(member_name.clone())
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .subscribe()
        .await
        .expect("subscribe stale child-open member");
    cluster
        .wait_for("pending stale child-open retry", |fake| {
            fake.pending_operations().iter().any(|pending| {
                pending.endpoint == Endpoint::Segment(1)
                    && pending.kind == OperationKind::SegmentOpen
            })
        })
        .await;
    let member = cluster
        .inspect(|fake| fake.member(&subscription, &member_name))
        .expect("stale child-open member");
    let pending = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| {
                    pending.endpoint == Endpoint::Segment(1)
                        && pending.kind == OperationKind::SegmentOpen
                })
                .map(|pending| pending.id)
        })
        .expect("pending stale child-open retry id");
    let takeover = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription(subscription.clone())
        .consumer_name(takeover_name.clone())
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .subscribe()
        .await
        .expect("subscribe stale child-open takeover");
    let takeover_member = cluster
        .inspect(|fake| fake.member(&subscription, &takeover_name))
        .expect("stale child-open takeover member");
    cluster.hold_command(
        Endpoint::Segment(1),
        magnetar::proto::pb::base_command::Type::Error,
    );
    cluster
        .update(|fake| {
            fake.complete_pending(pending, PendingCompletion::Fail(completion))?;
            fake.publish_assignment_plan(
                1,
                vec![
                    FullAssignment::new(member, [2]),
                    FullAssignment::new(takeover_member, [1]),
                ],
            )
        })
        .expect("withdraw pending child-open authority");
    loop {
        match next_event(&consumer).await {
            StreamConsumerEvent::AssignmentApplied { sources, .. }
                if sources.iter().map(|source| source.segment_id().0).eq([2]) =>
            {
                break;
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected stale child-open event: {unexpected:?}"),
        }
    }
    cluster.release_command(
        Endpoint::Segment(1),
        magnetar::proto::pb::base_command::Type::Error,
    );
    cluster
        .wait_for("stale child-open failure cleanup", |fake| {
            fake.resource_counts().pending_operations == 0
                && fake.active_child_owner(&subscription, 1) == Some(takeover_member)
                && fake.active_child_owner(&subscription, 2) == Some(member)
        })
        .await;
    let status = consumer.status();
    assert_eq!(status.attached_segments(), 1);
    assert!(status.pending_ownership().is_empty());
    consumer
        .close()
        .await
        .expect("close stale child-open member");
    takeover
        .close()
        .await
        .expect("close stale child-open takeover");
    cluster
        .wait_for("stale child-open aggregate cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0
                && counts.layout_sessions == 0
                && counts.pending_operations == 0
        })
        .await;
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
struct CloseStateTrace {
    concurrent_close_succeeded: bool,
    receive_closed: bool,
    event_stream_closed: bool,
    repeated_failure: bool,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MalformedDeliveryTrace {
    resync_reasons: Vec<String>,
    close_routes: Vec<usize>,
    after_close: Vec<ResourceCounts>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
struct DagWatchRecoveryTrace {
    watch_failure_reported: bool,
    baseline_reported: bool,
    replacement_session: u64,
    controller_opens: usize,
    terminal_watch_failure_reported: bool,
    terminal_reopen_failed: bool,
    replacement_assignment_after_terminal: bool,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AcknowledgementFailureTrace {
    partial_confirmed: usize,
    partial_failed: usize,
    registration_disconnect_error: String,
    registration_disconnect_recovered: bool,
    close_during_ack_fenced: bool,
    close_after_ack_success: String,
    close_after_ack_failure: String,
    shared_registration_close: SharedRegistrationCloseTrace,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
struct SharedRegistrationCloseTrace {
    one_registration: bool,
    no_transactional_acks: bool,
    waiter_closed_before_registration_completion: bool,
    leader_closed_after_registration_completion: bool,
}

fn encoded_batch(payloads: &[&[u8]]) -> Bytes {
    let mut bytes = BytesMut::new();
    for payload in payloads {
        let metadata = magnetar::proto::pb::SingleMessageMetadata {
            payload_size: i32::try_from(payload.len()).expect("test payload fits i32"),
            ..Default::default()
        }
        .encode_to_vec();
        bytes.put_u32(u32::try_from(metadata.len()).expect("batch metadata fits u32"));
        bytes.extend_from_slice(&metadata);
        bytes.extend_from_slice(payload);
    }
    bytes.freeze()
}

fn zlib(payload: &[u8]) -> Bytes {
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(payload)
        .expect("compress delivery-shape payload");
    Bytes::from(encoder.finish().expect("finish delivery-shape compression"))
}

fn lz4(payload: &[u8]) -> Bytes {
    Bytes::from(lz4_flex::block::compress(payload))
}

fn zstd(payload: &[u8]) -> Bytes {
    Bytes::from(zstd::bulk::compress(payload, 0).expect("compress zstd fixture"))
}

fn snappy(payload: &[u8]) -> Bytes {
    Bytes::from(
        snap::raw::Encoder::new()
            .compress_vec(payload)
            .expect("compress snappy fixture"),
    )
}

fn split_layout() -> Vec<M1Segment> {
    vec![
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0)
            .with_children([3, 4])
            .sealed_at(2),
        M1Segment::active(2, 32_768, 65_535, Endpoint::Segment(2), 0),
        M1Segment::active(3, 0, 16_383, Endpoint::Segment(1), 2).with_parents([1]),
        M1Segment::active(4, 16_384, 32_767, Endpoint::Segment(2), 2).with_parents([1]),
    ]
}

fn merge_layout() -> Vec<M1Segment> {
    vec![
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0)
            .with_children([3, 4])
            .sealed_at(2),
        M1Segment::active(2, 32_768, 65_535, Endpoint::Segment(2), 0),
        M1Segment::active(3, 0, 16_383, Endpoint::Segment(1), 2)
            .with_parents([1])
            .with_children([5])
            .sealed_at(3),
        M1Segment::active(4, 16_384, 32_767, Endpoint::Segment(2), 2)
            .with_parents([1])
            .with_children([5])
            .sealed_at(3),
        M1Segment::active(5, 0, 32_767, Endpoint::Segment(1), 3).with_parents([3, 4]),
    ]
}

fn same_topology_at_epoch_two() -> Vec<M1Segment> {
    vec![
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0),
        M1Segment::active(2, 32_768, 65_535, Endpoint::Segment(2), 0),
    ]
}

async fn receive<E>(consumer: &StreamConsumer<BytesSchema, E>) -> StreamMessage<BytesSchema>
where
    E: Engine,
{
    tokio::time::timeout(magnetar_differential::HANG_GUARD, consumer.receive())
        .await
        .expect("aggregate receive timed out")
        .expect("aggregate receive failed")
}

async fn wait_for_initial_flow<E>(
    consumer: &StreamConsumer<BytesSchema, E>,
    expected_sources: &[u64],
) where
    E: Engine,
{
    let expected: BTreeSet<_> = expected_sources.iter().copied().collect();
    let mut assigned = None;
    let mut flowing = BTreeSet::new();
    while assigned.as_ref() != Some(&expected) || !expected.is_subset(&flowing) {
        match next_event(consumer).await {
            StreamConsumerEvent::AssignmentApplied { sources, .. } => {
                assigned = Some(sources.iter().map(|source| source.segment_id().0).collect());
            }
            StreamConsumerEvent::SegmentPhaseChanged {
                source,
                phase: magnetar::proto::SegmentPhase::Flowing,
            } => {
                flowing.insert(source.segment_id().0);
            }
            StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected initial event: {unexpected:?}"),
        }
    }
}

async fn next_event<E>(consumer: &StreamConsumer<BytesSchema, E>) -> StreamConsumerEvent
where
    E: Engine,
{
    tokio::time::timeout(magnetar_differential::HANG_GUARD, consumer.next_event())
        .await
        .expect("aggregate event timed out")
        .expect("aggregate event failed")
        .expect("aggregate closed before expected event")
}

async fn wait_for_assignment<E>(
    consumer: &StreamConsumer<BytesSchema, E>,
    epoch: u64,
    expected_sources: &[u64],
) -> (u64, Vec<u64>)
where
    E: Engine,
{
    loop {
        match next_event(consumer).await {
            StreamConsumerEvent::AssignmentApplied {
                layout_epoch,
                sources,
            } if layout_epoch == epoch => {
                let sources = sources
                    .iter()
                    .map(|source| source.segment_id().0)
                    .collect::<Vec<_>>();
                assert_eq!(sources, expected_sources);
                return (layout_epoch, sources);
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected assignment event: {unexpected:?}"),
        }
    }
}

async fn wait_for_flowing_sources<E>(consumer: &StreamConsumer<BytesSchema, E>, expected: &[u64])
where
    E: Engine,
{
    let expected: BTreeSet<_> = expected.iter().copied().collect();
    let mut flowing = BTreeSet::new();
    while !expected.is_subset(&flowing) {
        match next_event(consumer).await {
            StreamConsumerEvent::SegmentPhaseChanged {
                source,
                phase: magnetar::proto::SegmentPhase::Flowing,
            } => {
                flowing.insert(source.segment_id().0);
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            StreamConsumerEvent::ResyncRequired { reason } => {
                panic!("aggregate resynchronized before descendant FLOW: {reason}")
            }
            unexpected => panic!("unexpected descendant FLOW event: {unexpected:?}"),
        }
    }
}

fn command_segment_count(
    fake: &M1FakeCluster,
    command: magnetar::proto::pb::base_command::Type,
    segment_id: u64,
) -> usize {
    let topic = fake
        .segment_topic(segment_id)
        .expect("segment has canonical topic");
    fake.routes()
        .iter()
        .filter(|route| {
            route.command == command
                && route.resource.as_deref().is_some_and(|resource| {
                    resource == topic
                        || resource
                            .strip_prefix(&topic)
                            .is_some_and(|rest| rest.starts_with(':'))
                })
        })
        .count()
}

fn flow_command_count(fake: &M1FakeCluster, segment_id: u64) -> usize {
    command_segment_count(
        fake,
        magnetar::proto::pb::base_command::Type::Flow,
        segment_id,
    )
}

fn command_segments(
    fake: &M1FakeCluster,
    command: magnetar::proto::pb::base_command::Type,
) -> Vec<u64> {
    let topics: BTreeMap<_, _> = (1..=5)
        .filter_map(|segment_id| {
            fake.segment_topic(segment_id)
                .map(|topic| (topic, segment_id))
        })
        .collect();
    let mut segments = fake
        .routes()
        .iter()
        .filter(|route| route.command == command)
        .filter_map(|route| route.resource.as_ref())
        .filter_map(|resource| {
            topics.iter().find_map(|(topic, segment_id)| {
                (resource == topic
                    || resource
                        .strip_prefix(topic)
                        .is_some_and(|rest| rest.starts_with(':')))
                .then_some(segment_id)
            })
        })
        .copied()
        .collect::<Vec<_>>();
    segments.sort_unstable();
    segments.dedup();
    segments
}

fn stale_token_kind(error: magnetar::scalable::StreamConsumerError) -> String {
    match error {
        magnetar::scalable::StreamConsumerError::Model(
            magnetar::proto::StreamConsumerModelError::StaleDeliveryToken,
        ) => "stale-token".to_owned(),
        unexpected => panic!("unexpected stale-token error: {unexpected:?}"),
    }
}

fn engine_error_kind(error: magnetar::scalable::StreamConsumerError) -> String {
    match error {
        magnetar::scalable::StreamConsumerError::Engine { .. } => "engine".to_owned(),
        magnetar::scalable::StreamConsumerError::Model(_) => "model".to_owned(),
        unexpected => panic!("unexpected engine operation error: {unexpected:?}"),
    }
}

fn live_seek_limitation_kind(error: magnetar::scalable::StreamConsumerError) -> String {
    match error {
        magnetar::scalable::StreamConsumerError::Model(
            magnetar::proto::StreamConsumerModelError::ConcurrentSeek,
        ) => "concurrent-seek".to_owned(),
        unexpected => panic!("unexpected live-delivery seek result: {unexpected:?}"),
    }
}

fn poisoned_commit_kind(error: magnetar::PulsarError) -> String {
    match error {
        magnetar::PulsarError::StreamConsumer(
            magnetar::scalable::StreamConsumerError::TransactionPoisoned { .. },
        ) => "transaction-poisoned".to_owned(),
        unexpected => panic!("unexpected poisoned commit result: {unexpected:?}"),
    }
}

fn already_ending_kind(error: magnetar::PulsarError) -> String {
    match error {
        magnetar::PulsarError::StreamConsumer(
            magnetar::scalable::StreamConsumerError::TransactionAlreadyEnding { .. },
        ) => "transaction-already-ending".to_owned(),
        unexpected => panic!("unexpected concurrent transaction end result: {unexpected:?}"),
    }
}

fn end_transaction_command_count(fake: &M1FakeCluster, action: &str) -> usize {
    fake.routes()
        .iter()
        .filter(|route| {
            route.command == magnetar::proto::pb::base_command::Type::EndTxn
                && route.resource.as_deref() == Some(action)
        })
        .count()
}

fn transaction_registration_command_count(fake: &M1FakeCluster) -> usize {
    fake.routes()
        .iter()
        .filter(|route| {
            route.command == magnetar::proto::pb::base_command::Type::AddSubscriptionToTxn
        })
        .count()
}

fn position_from_messages(
    layout_epoch: u64,
    messages: &[StreamMessage<BytesSchema>],
) -> magnetar::proto::PositionVector {
    magnetar::proto::PositionVector::new(
        layout_epoch,
        messages.iter().map(|message| {
            (
                message.source().clone(),
                message.message_id().ordinary_message_id(),
            )
        }),
    )
    .expect("valid source-complete position vector")
}

async fn close_and_count<E>(
    consumer: StreamConsumer<BytesSchema, E>,
    cluster: &M1SocketCluster,
) -> ResourceCounts
where
    E: Engine,
{
    let failure_expected =
        consumer.status().phase() == magnetar::proto::AggregatePhase::ResyncRequired;
    let close = tokio::time::timeout(magnetar_differential::HANG_GUARD, consumer.close())
        .await
        .expect("aggregate close timed out");
    if !failure_expected {
        close.expect("close aggregate");
    }
    cluster
        .wait_for("aggregate child cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0
                && counts.layout_sessions == 0
                && counts.pending_operations == 0
                && counts.permits == 0
        })
        .await;
    cluster.inspect(M1FakeCluster::resource_counts)
}

async fn observe_local_ancestry<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> LocalAncestryTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("local-ancestry-sub")
        .consumer_name("local-ancestry-member")
        .ordering_mode(magnetar::proto::OrderingMode::Strict)
        .receiver_budget(
            magnetar::proto::ReceiverBudget::bytes(64 * 1024 * 1024)
                .expect("large ancestry receive budget"),
        )
        .subscribe()
        .await
        .expect("subscribe local-ancestry aggregate");
    cluster
        .wait_for("local-ancestry initial children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;
    cluster.retain_sealed_placements();

    for payload in [b"delayed-ack".as_slice(), b"second-ack", b"delivered"] {
        cluster
            .update(|fake| fake.enqueue_message(1, Bytes::copy_from_slice(payload)))
            .expect("enqueue parent barrier message");
    }
    cluster
        .wait_for("three parent deliveries", |fake| {
            fake.resource_counts().unacked_messages == 3
        })
        .await;
    let messages = [
        receive(&consumer).await,
        receive(&consumer).await,
        receive(&consumer).await,
    ];
    assert!(
        messages
            .iter()
            .all(|message| message.source().segment_id().0 == 1)
    );

    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::Ack,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay ordinary parent acknowledgement");
    let mut delayed_ack = Box::pin(consumer.acknowledge(&messages[0]));
    tokio::select! {
        biased;
        result = &mut delayed_ack => panic!("delayed acknowledgement completed early: {result:?}"),
        () = cluster.wait_for("delayed parent acknowledgement", |fake| {
            fake.pending_operations().iter().any(|pending| pending.kind == OperationKind::Ack)
        }) => {}
    }
    let member = cluster
        .inspect(|fake| fake.member("local-ancestry-sub", "local-ancestry-member"))
        .expect("local member observable");
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.advance_layout(2, split_layout())?;
            fake.publish_early_descendant_assignment_plan(
                2,
                vec![FullAssignment::new(member, [1, 2, 3, 4])],
            )?;
            fake.terminate_segment(1)
        })
        .expect("publish early split descendants and parent terminal");
    let _ = wait_for_assignment(&consumer, 2, &[1, 2, 3, 4]).await;
    cluster
        .wait_for("split descendants attach without FLOW", |fake| {
            let subscribed =
                command_segments(fake, magnetar::proto::pb::base_command::Type::Subscribe);
            subscribed.contains(&3) && subscribed.contains(&4)
        })
        .await;
    let mut split_barriers = vec![cluster.inspect(|fake| {
        assert!(!fake.segment_is_complete("local-ancestry-sub", 1));
        flow_command_count(fake, 3) > 0 || flow_command_count(fake, 4) > 0
    })];
    assert_eq!(split_barriers, vec![false]);

    let pending_ack = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::Ack)
                .map(|pending| pending.id)
        })
        .expect("delayed ACK id");
    cluster
        .update(|fake| fake.complete_pending(pending_ack, PendingCompletion::Succeed))
        .expect("complete delayed ordinary ACK");
    delayed_ack
        .await
        .expect("delayed ordinary parent acknowledgement succeeds");
    split_barriers.push(cluster.inspect(|fake| {
        assert!(!fake.segment_is_complete("local-ancestry-sub", 1));
        flow_command_count(fake, 3) > 0 || flow_command_count(fake, 4) > 0
    }));

    consumer
        .acknowledge(&messages[1])
        .await
        .expect("settle second parent acknowledgement");
    split_barriers.push(cluster.inspect(|fake| {
        assert!(!fake.segment_is_complete("local-ancestry-sub", 1));
        flow_command_count(fake, 3) > 0 || flow_command_count(fake, 4) > 0
    }));

    assert_eq!(split_barriers, vec![false, false, false]);
    consumer
        .acknowledge(&messages[2])
        .await
        .expect("settle final parent delivery");
    drop(messages);
    cluster
        .wait_for("split parent completes at the broker", |fake| {
            fake.segment_is_complete("local-ancestry-sub", 1)
        })
        .await;
    wait_for_flowing_sources(&consumer, &[3, 4]).await;
    cluster
        .wait_for("split descendants receive FLOW", |fake| {
            flow_command_count(fake, 3) > 0 && flow_command_count(fake, 4) > 0
        })
        .await;
    split_barriers.push(true);
    let split_flowing = cluster.inspect(|fake| {
        command_segments(fake, magnetar::proto::pb::base_command::Type::Flow)
            .into_iter()
            .filter(|segment_id| matches!(segment_id, 3 | 4))
            .collect::<Vec<_>>()
    });
    assert_eq!(split_flowing, vec![3, 4]);
    assert_eq!(split_barriers, vec![false, false, false, true]);

    cluster
        .update(|fake| fake.enqueue_message(3, Bytes::from_static(b"merge-parent-three")))
        .expect("enqueue first merge parent");
    cluster
        .update(|fake| fake.enqueue_message(4, Bytes::from_static(b"merge-parent-four")))
        .expect("enqueue second merge parent");
    cluster
        .wait_for("merge parent deliveries", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let mut merge_parents = [receive(&consumer).await, receive(&consumer).await];
    merge_parents.sort_by_key(|message| message.source().segment_id().0);
    assert_eq!(
        merge_parents
            .iter()
            .map(|message| message.source().segment_id().0)
            .collect::<Vec<_>>(),
        vec![3, 4]
    );

    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.advance_layout(3, merge_layout())?;
            fake.publish_early_descendant_assignment_plan(
                3,
                vec![FullAssignment::new(member, [1, 2, 3, 4, 5])],
            )?;
            fake.terminate_segment(3)?;
            fake.terminate_segment(4)
        })
        .expect("publish merge descendant and parent terminals");
    let _ = wait_for_assignment(&consumer, 3, &[1, 2, 3, 4, 5]).await;
    cluster
        .wait_for("merge descendant attaches without FLOW", |fake| {
            command_segments(fake, magnetar::proto::pb::base_command::Type::Subscribe).contains(&5)
        })
        .await;
    let mut merge_barriers = vec![cluster.inspect(|fake| flow_command_count(fake, 5) > 0)];
    assert_eq!(merge_barriers, vec![false]);

    consumer
        .acknowledge(&merge_parents[0])
        .await
        .expect("acknowledge first merge parent");
    cluster
        .wait_for("first merge parent completes", |fake| {
            fake.segment_is_complete("local-ancestry-sub", 3)
        })
        .await;
    merge_barriers.push(cluster.inspect(|fake| {
        assert!(!fake.segment_is_complete("local-ancestry-sub", 4));
        flow_command_count(fake, 5) > 0
    }));
    assert_eq!(merge_barriers, vec![false, false]);

    consumer
        .acknowledge(&merge_parents[1])
        .await
        .expect("acknowledge second merge parent");
    wait_for_flowing_sources(&consumer, &[5]).await;
    cluster
        .wait_for("merge descendant receives FLOW", |fake| {
            fake.segment_is_complete("local-ancestry-sub", 4) && flow_command_count(fake, 5) > 0
        })
        .await;
    merge_barriers.push(true);
    let merge_flowing = cluster.inspect(|fake| {
        command_segments(fake, magnetar::proto::pb::base_command::Type::Flow)
            .into_iter()
            .filter(|segment_id| *segment_id == 5)
            .collect::<Vec<_>>()
    });
    assert_eq!(merge_flowing, vec![5]);

    cluster
        .update(|fake| fake.enqueue_message(5, Bytes::from_static(b"merged-descendant")))
        .expect("enqueue merged descendant");
    let merged = receive(&consumer).await;
    assert_eq!(merged.source().segment_id().0, 5);
    consumer
        .acknowledge(&merged)
        .await
        .expect("acknowledge merged descendant");

    LocalAncestryTrace {
        split_barriers,
        merge_barriers,
        split_flowing,
        merge_flowing,
        after_close: close_and_count(consumer, cluster).await,
    }
}

async fn run_tokio_local_ancestry() -> LocalAncestryTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_local_ancestry(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_local_ancestry() -> LocalAncestryTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_local_ancestry(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_exact_m1_sealed_placement<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> SealedPlacementTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("sealed-placement-sub")
        .consumer_name("sealed-placement-member")
        .ordering_mode(magnetar::proto::OrderingMode::Strict)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe exact-M1 aggregate");
    cluster
        .wait_for("exact-M1 initial children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;
    let sink = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("sealed-placement-sub")
        .consumer_name("sealed-placement-sink")
        .ordering_mode(magnetar::proto::OrderingMode::Strict)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe exact-M1 sink member");
    let _ = wait_for_assignment(&sink, 1, &[]).await;
    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"sealed-parent")))
        .expect("enqueue exact-M1 parent delivery");
    cluster
        .wait_for("exact-M1 parent delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let parent = receive(&consumer).await;
    assert_eq!(parent.source().segment_id().0, 1);
    let member = cluster
        .inspect(|fake| fake.member("sealed-placement-sub", "sealed-placement-member"))
        .expect("exact-M1 member observable");
    let sink_member = cluster
        .inspect(|fake| fake.member("sealed-placement-sub", "sealed-placement-sink"))
        .expect("exact-M1 sink member observable");
    cluster
        .update(|fake| {
            fake.advance_layout(2, split_layout())?;
            fake.publish_early_descendant_assignment_plan(
                2,
                vec![
                    FullAssignment::new(member, [1, 2, 3, 4]),
                    FullAssignment::new(sink_member, []),
                ],
            )?;
            fake.terminate_segment(1)
        })
        .expect("publish exact-M1 sealed assignment");
    let _ = wait_for_assignment(&consumer, 2, &[1, 2, 3, 4]).await;
    cluster
        .wait_for("exact-M1 descendants attach without FLOW", |fake| {
            let subscribed =
                command_segments(fake, magnetar::proto::pb::base_command::Type::Subscribe);
            subscribed.contains(&3) && subscribed.contains(&4)
        })
        .await;
    let descendants_blocked_before_ack = cluster
        .inspect(|fake| flow_command_count(fake, 3) == 0 && flow_command_count(fake, 4) == 0);
    assert!(descendants_blocked_before_ack);
    cluster
        .update(|fake| {
            fake.publish_early_descendant_assignment_plan(
                2,
                vec![
                    FullAssignment::new(member, [2, 3, 4]),
                    FullAssignment::new(sink_member, [1]),
                ],
            )
        })
        .expect("temporarily revoke exact-M1 sealed parent");
    let _ = wait_for_assignment(&consumer, 2, &[2, 3, 4]).await;
    cluster
        .update(|fake| {
            fake.publish_early_descendant_assignment_plan(
                2,
                vec![
                    FullAssignment::new(member, [1, 2, 3, 4]),
                    FullAssignment::new(sink_member, []),
                ],
            )
        })
        .expect("restore exact-M1 sealed parent while it drains");
    let _ = wait_for_assignment(&consumer, 2, &[1, 2, 3, 4]).await;
    assert_eq!(
        consumer.status().pending_ownership(),
        &[parent.source().clone()]
    );
    consumer
        .acknowledge(&parent)
        .await
        .expect("acknowledge exact-M1 parent");
    drop(parent);
    cluster
        .wait_for("exact-M1 parent completion", |fake| {
            fake.segment_is_complete("sealed-placement-sub", 1)
        })
        .await;
    wait_for_flowing_sources(&consumer, &[3, 4]).await;
    cluster
        .wait_for("exact-M1 descendants receive FLOW", |fake| {
            flow_command_count(fake, 3) > 0 && flow_command_count(fake, 4) > 0
        })
        .await;
    let (descendant_subscribes, descendant_flow_commands) = cluster.inspect(|fake| {
        let subscribed = command_segments(fake, magnetar::proto::pb::base_command::Type::Subscribe);
        let descendant_subscribes = subscribed
            .into_iter()
            .filter(|segment_id| matches!(segment_id, 3 | 4))
            .collect::<Vec<_>>();
        let descendant_flow_commands = [3, 4]
            .into_iter()
            .filter(|segment_id| flow_command_count(fake, *segment_id) > 0)
            .collect::<Vec<_>>();
        (descendant_subscribes, descendant_flow_commands)
    });
    assert_eq!(descendant_subscribes, vec![3, 4]);
    assert_eq!(descendant_flow_commands, vec![3, 4]);
    sink.close().await.expect("close exact-M1 sink member");
    let after_close = close_and_count(consumer, cluster).await;
    let parent_subscribes = cluster.inspect(|fake| {
        command_segment_count(fake, magnetar::proto::pb::base_command::Type::Subscribe, 1)
    });
    assert_eq!(
        parent_subscribes, 1,
        "the completed sealed parent never reopens after assignment regain"
    );
    SealedPlacementTrace {
        descendants_blocked_before_ack,
        parent_subscribes,
        descendant_subscribes,
        descendant_flow_commands,
        after_close,
    }
}

async fn run_tokio_exact_m1_sealed_placement() -> SealedPlacementTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_exact_m1_sealed_placement(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_exact_m1_sealed_placement() -> SealedPlacementTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_exact_m1_sealed_placement(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_cross_member_mode<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
    ordering_mode: magnetar::proto::OrderingMode,
) -> OrderingModeTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer_a = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("cross-member-sub")
        .consumer_name("cross-member-a")
        .ordering_mode(ordering_mode)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe cross-member parent owner");
    cluster
        .wait_for("cross-member initial children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer_a, &[1, 2]).await;
    cluster
        .wait_for("cross-member initial FLOW reaches both brokers", |fake| {
            fake.segment_permits("cross-member-sub", 1) > 0
                && fake.segment_permits("cross-member-sub", 2) > 0
        })
        .await;
    let consumer_b = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("cross-member-sub")
        .consumer_name("cross-member-b")
        .ordering_mode(ordering_mode)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe cross-member child owner");
    let _ = wait_for_assignment(&consumer_b, 1, &[]).await;
    let member_a = cluster
        .inspect(|fake| fake.member("cross-member-sub", "cross-member-a"))
        .expect("parent member observable");
    let member_b = cluster
        .inspect(|fake| fake.member("cross-member-sub", "cross-member-b"))
        .expect("child member observable");
    cluster
        .update(|fake| {
            fake.publish_assignment_plan(
                1,
                vec![
                    FullAssignment::new(member_a, [1]),
                    FullAssignment::new(member_b, [2]),
                ],
            )
        })
        .expect("split root ownership between members");
    let _ = wait_for_assignment(&consumer_a, 1, &[1]).await;
    let _ = wait_for_assignment(&consumer_b, 1, &[2]).await;
    cluster
        .wait_for("second member acquires root child", |fake| {
            fake.resource_counts().child_consumers == 2
                && fake.segment_permits("cross-member-sub", 2) > 0
        })
        .await;

    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.advance_layout(2, split_layout())?;
            fake.publish_early_descendant_assignment_plan(
                2,
                vec![
                    FullAssignment::new(member_a, [1, 4]),
                    FullAssignment::new(member_b, [2, 3]),
                ],
            )
        })
        .expect("assign child across its parent member");
    let _ = wait_for_assignment(&consumer_a, 2, &[1, 4]).await;
    let _ = wait_for_assignment(&consumer_b, 2, &[2, 3]).await;
    cluster
        .wait_for("cross-member descendants attach", |fake| {
            let subscribed =
                command_segments(fake, magnetar::proto::pb::base_command::Type::Subscribe);
            subscribed.contains(&3) && subscribed.contains(&4)
        })
        .await;
    let proof = cluster
        .inspect(|fake| fake.drain_eligibility(member_b, 3))
        .expect("fake independently classifies child ancestry");
    assert_eq!(
        proof,
        DrainEligibility::CrossMemberUnprovable {
            segment_ids: vec![1]
        }
    );

    let event_ancestors = if ordering_mode == magnetar::proto::OrderingMode::Strict {
        loop {
            match next_event(&consumer_b).await {
                StreamConsumerEvent::OrderingUnprovable {
                    segment_id,
                    ancestors,
                } if segment_id.0 == 3 => {
                    break ancestors
                        .iter()
                        .map(|ancestor| ancestor.0)
                        .collect::<Vec<_>>();
                }
                StreamConsumerEvent::AssignmentApplied { .. }
                | StreamConsumerEvent::SegmentPhaseChanged { .. }
                | StreamConsumerEvent::OrderingUnprovable { .. } => {}
                unexpected => panic!("unexpected strict cross-member event: {unexpected:?}"),
            }
        }
    } else {
        cluster
            .wait_for("broker-managed descendant FLOW", |fake| {
                flow_command_count(fake, 3) > 0
            })
            .await;
        Vec::new()
    };
    let status = consumer_b.status();
    let ordering_unprovable = status
        .ordering_unprovable()
        .iter()
        .map(|segment| segment.0)
        .collect::<Vec<_>>();
    let descendant_flow_commands = cluster.inspect(|fake| flow_command_count(fake, 3));
    if ordering_mode == magnetar::proto::OrderingMode::Strict {
        assert_eq!(event_ancestors, vec![1]);
        assert_eq!(ordering_unprovable, vec![3]);
        assert_eq!(descendant_flow_commands, 0);
    } else {
        assert!(ordering_unprovable.is_empty());
        assert!(descendant_flow_commands > 0);
    }

    consumer_a.close().await.expect("close parent owner");
    consumer_b.close().await.expect("close child owner");
    cluster
        .wait_for("cross-member cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0
                && counts.layout_sessions == 0
                && counts.pending_operations == 0
                && counts.permits == 0
        })
        .await;
    OrderingModeTrace {
        ordering_unprovable,
        event_ancestors,
        descendant_flow_commands,
        independent_cross_member_ancestry: true,
        after_close: cluster.inspect(M1FakeCluster::resource_counts),
    }
}

async fn run_tokio_cross_member() -> CrossMemberTrace {
    let strict_cluster = M1SocketCluster::bind().await;
    let strict_client = connect_tokio(&strict_cluster).await;
    let strict = observe_cross_member_mode(
        &strict_client,
        &strict_cluster,
        magnetar::proto::OrderingMode::Strict,
    )
    .await;
    strict_client.close().await;

    let broker_cluster = M1SocketCluster::bind().await;
    let broker_client = connect_tokio(&broker_cluster).await;
    let broker_managed = observe_cross_member_mode(
        &broker_client,
        &broker_cluster,
        magnetar::proto::OrderingMode::BrokerManaged,
    )
    .await;
    broker_client.close().await;
    CrossMemberTrace {
        strict,
        broker_managed,
    }
}

async fn run_moonpool_cross_member() -> CrossMemberTrace {
    let strict_cluster = M1SocketCluster::bind().await;
    let strict_client = connect_moonpool(&strict_cluster).await;
    let strict = observe_cross_member_mode(
        &strict_client,
        &strict_cluster,
        magnetar::proto::OrderingMode::Strict,
    )
    .await;
    strict_client.close().await;

    let broker_cluster = M1SocketCluster::bind().await;
    let broker_client = connect_moonpool(&broker_cluster).await;
    let broker_managed = observe_cross_member_mode(
        &broker_client,
        &broker_cluster,
        magnetar::proto::OrderingMode::BrokerManaged,
    )
    .await;
    broker_client.close().await;
    CrossMemberTrace {
        strict,
        broker_managed,
    }
}

async fn observe_vector_seek<E>(client: &PulsarClient<E>, cluster: &M1SocketCluster) -> SeekTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer = tokio::time::timeout(
        magnetar_differential::HANG_GUARD,
        client
            .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
            .subscription("vector-seek-sub")
            .consumer_name("vector-seek-member")
            .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
            .receiver_budget(two_frame_receiver_budget())
            .subscribe(),
    )
    .await
    .expect("vector-seek subscribe timed out")
    .expect("subscribe vector-seek aggregate");
    cluster
        .wait_for("vector-seek initial children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;
    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"one-zero")))
        .expect("enqueue segment-one seek target");
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"two-zero")))
        .expect("enqueue segment-two seek target");
    cluster
        .wait_for("both seek targets dispatch", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let mut initial = vec![receive(&consumer).await, receive(&consumer).await];
    initial.sort_by_key(|message| message.source().segment_id().0);
    assert_eq!(
        initial
            .iter()
            .map(|message| message.source().segment_id().0)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let positions = magnetar::proto::PositionVector::new(
        1,
        initial.iter().map(|message| {
            (
                message.source().clone(),
                message.message_id().ordinary_message_id(),
            )
        }),
    )
    .expect("all-current-leaf seek vector");

    cluster
        .update(|fake| {
            fake.clear_routes();
            Ok(())
        })
        .expect("clear routes before live-delivery limitation");
    let live_delivery_seek_limitation = live_seek_limitation_kind(
        tokio::time::timeout(
            magnetar_differential::HANG_GUARD,
            consumer.seek_positions(&positions),
        )
        .await
        .expect("live-delivery vector seek timed out")
        .expect_err("the public model rejects seek while delivery tokens are live"),
    );
    assert!(
        cluster.inspect(|fake| {
            command_segments(fake, magnetar::proto::pb::base_command::Type::Seek).is_empty()
        }),
        "ConcurrentSeek is decided before ordinary child wire I/O"
    );
    tokio::time::timeout(
        magnetar_differential::HANG_GUARD,
        consumer.acknowledge_batch(&initial),
    )
    .await
    .expect("initial vector-seek acknowledgement timed out")
    .expect("settle live deliveries before supported vector seek");

    cluster.hold_messages(Endpoint::Segment(1));
    cluster.hold_messages(Endpoint::Segment(2));
    for (segment_id, payload) in [
        (1, b"one-buffered-one".as_slice()),
        (1, b"one-buffered-two".as_slice()),
        (2, b"two-buffered-one".as_slice()),
        (2, b"two-buffered-two".as_slice()),
    ] {
        cluster
            .update(|fake| fake.enqueue_message(segment_id, Bytes::copy_from_slice(payload)))
            .expect("enqueue buffered pre-seek message");
    }
    cluster
        .wait_for("pre-seek messages buffered by the aggregate", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    cluster
        .update(|fake| {
            fake.clear_routes();
            Ok(())
        })
        .expect("clear pre-seek routes");
    let mut successful_seek = Box::pin(consumer.seek_positions(&positions));
    tokio::select! {
        biased;
        result = &mut successful_seek => panic!("seek completed before held child responses: {result:?}"),
        () = cluster.wait_for("all ordinary child SEEK commands", |fake| {
            command_segments(fake, magnetar::proto::pb::base_command::Type::Seek) == vec![1, 2]
        }) => {}
    }
    cluster.release_messages(Endpoint::Segment(1));
    cluster.release_messages(Endpoint::Segment(2));
    let successful_seek_result =
        tokio::time::timeout(magnetar_differential::HANG_GUARD, &mut successful_seek)
            .await
            .expect("successful vector seek timed out");
    if successful_seek_result.is_err() {
        cluster.assert_healthy();
    }
    if let Err(error) = successful_seek_result {
        let (resources, routes) =
            cluster.inspect(|fake| (fake.resource_counts(), fake.routes().to_vec()));
        panic!(
            "all-current-leaf vector seek failed: {error:?}; resources={resources:?}, routes={routes:?}"
        );
    }
    drop(successful_seek);
    let successful_seek_segments = cluster
        .inspect(|fake| command_segments(fake, magnetar::proto::pb::base_command::Type::Seek));
    assert_eq!(successful_seek_segments, vec![1, 2]);
    assert_eq!(
        cluster.inspect(|fake| {
            command_segments(fake, magnetar::proto::pb::base_command::Type::Subscribe)
        }),
        vec![1, 2],
        "each successful SEEK is followed by ordinary child reattachment"
    );

    let acknowledgement_routes_before = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .filter(|route| route.command == magnetar::proto::pb::base_command::Type::Ack)
            .count()
    });
    let mut stale_token_failures = Vec::new();
    for message in &initial {
        stale_token_failures.push(stale_token_kind(
            tokio::time::timeout(
                magnetar_differential::HANG_GUARD,
                consumer.acknowledge(message),
            )
            .await
            .expect("stale pre-seek acknowledgement timed out")
            .expect_err("pre-seek token must be stale"),
        ));
    }
    assert_eq!(
        cluster.inspect(|fake| {
            fake.routes()
                .iter()
                .filter(|route| route.command == magnetar::proto::pb::base_command::Type::Ack)
                .count()
        }),
        acknowledgement_routes_before,
        "stale delivery authority fails before ordinary ACK wire I/O"
    );

    let mut first_replayed_entries = BTreeMap::new();
    let mut replayed = Vec::new();
    while first_replayed_entries.len() < 2 || replayed.len() < 6 {
        let message = receive(&consumer).await;
        first_replayed_entries
            .entry(message.source().segment_id().0)
            .or_insert(message.message_id().ordinary_message_id().entry_id);
        replayed.push(message);
    }
    let first_replayed_entries = first_replayed_entries.into_iter().collect::<Vec<_>>();
    assert_eq!(
        first_replayed_entries,
        vec![(1, 0), (2, 0)],
        "the first post-seek message per source is replayed target zero, not a buffered entry"
    );
    let replay_acknowledgement = tokio::time::timeout(
        magnetar_differential::HANG_GUARD,
        consumer.acknowledge_batch(&replayed),
    )
    .await
    .expect("replayed vector-seek acknowledgement timed out");
    if replay_acknowledgement.is_err() {
        cluster.assert_healthy();
    }
    replay_acknowledgement.expect("settle replayed live deliveries before failed vector seek");

    let old_controller = cluster
        .inspect(|fake| fake.member("vector-seek-sub", "vector-seek-member"))
        .expect("initial controller incarnation");
    let old_controller = old_controller.connection;
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.script_next(
                Endpoint::Segment(2),
                OperationKind::Seek,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "second child seek failed",
                )),
            )
        })
        .expect("script second-child seek failure");
    let failed_seek_error = engine_error_kind(
        tokio::time::timeout(
            magnetar_differential::HANG_GUARD,
            consumer.seek_positions(&positions),
        )
        .await
        .expect("failed vector seek timed out")
        .expect_err("one failed child seek fails the vector operation"),
    );
    let failed_seek_segments = cluster
        .inspect(|fake| command_segments(fake, magnetar::proto::pb::base_command::Type::Seek));
    assert_eq!(failed_seek_segments, vec![1, 2]);
    cluster
        .update(|fake| fake.disconnect_connection(old_controller))
        .expect("disconnect controller that M1 cannot unregister");
    let mut resync_reasons = Vec::new();
    let replacement_baseline = loop {
        match next_event(&consumer).await {
            StreamConsumerEvent::ResyncRequired { reason } => {
                resync_reasons.push(reason);
                assert!(
                    resync_reasons.len() <= 10,
                    "seek failure entered a resync loop: {resync_reasons:?}"
                );
            }
            StreamConsumerEvent::AssignmentApplied {
                layout_epoch,
                sources,
            } if !resync_reasons.is_empty() => {
                break (
                    layout_epoch,
                    sources
                        .iter()
                        .map(|source| source.segment_id().0)
                        .collect::<Vec<_>>(),
                );
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. } => {}
            unexpected => panic!("unexpected failed-seek recovery event: {unexpected:?}"),
        }
    };
    assert_eq!(replacement_baseline, (1, vec![1, 2]));
    let resync_events = usize::from(!resync_reasons.is_empty());
    cluster
        .wait_for("failed seek cleanup and replacement baseline", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 2
                && counts.pending_operations == 0
                && fake.routes().iter().any(|route| {
                    route.endpoint == Endpoint::Controller
                        && route.command == magnetar::proto::pb::base_command::Type::Connect
                        && route.connection != old_controller
                })
        })
        .await;
    let replacement_controller = cluster
        .inspect(|fake| {
            fake.routes()
                .iter()
                .find(|route| {
                    route.endpoint == Endpoint::Controller
                        && route.command == magnetar::proto::pb::base_command::Type::Connect
                        && route.connection != old_controller
                })
                .map(|route| route.connection)
        })
        .expect("replacement controller incarnation");
    assert_ne!(old_controller, replacement_controller);

    let ack_routes_before_stale_replay = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .filter(|route| route.command == magnetar::proto::pb::base_command::Type::Ack)
            .count()
    });
    stale_token_failures.push(stale_token_kind(
        tokio::time::timeout(
            magnetar_differential::HANG_GUARD,
            consumer.acknowledge(&replayed[0]),
        )
        .await
        .expect("stale pre-resync acknowledgement timed out")
        .expect_err("pre-resync replay token must be stale"),
    ));
    assert_eq!(
        cluster.inspect(|fake| {
            fake.routes()
                .iter()
                .filter(|route| route.command == magnetar::proto::pb::base_command::Type::Ack)
                .count()
        }),
        ack_routes_before_stale_replay
    );

    SeekTrace {
        live_delivery_seek_limitation,
        successful_seek_segments,
        first_replayed_entries,
        stale_token_failures,
        failed_seek_segments,
        failed_seek_error,
        resync_events,
        controller_incarnations: 2,
        after_close: close_and_count(consumer, cluster).await,
    }
}

async fn run_tokio_vector_seek() -> SeekTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_vector_seek(&client, &cluster).await;
    tokio::time::timeout(magnetar_differential::HANG_GUARD, client.close())
        .await
        .expect("tokio vector-seek client close timed out");
    trace
}

async fn run_moonpool_vector_seek() -> SeekTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_vector_seek(&client, &cluster).await;
    tokio::time::timeout(magnetar_differential::HANG_GUARD, client.close())
        .await
        .expect("moonpool vector-seek client close timed out");
    trace
}

async fn observe_transactions<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> TransactionTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi + TransactionApi + std::fmt::Debug,
{
    assert!(format!("{client:?}").contains("TransactionCoordinator"));
    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("transaction-sub")
        .consumer_name("transaction-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe transaction aggregate");
    cluster
        .wait_for("transaction initial children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"commit-one")))
        .expect("enqueue first commit participant");
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"commit-two")))
        .expect("enqueue second commit participant");
    cluster
        .wait_for("commit participant deliveries", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let mut commit_messages = vec![receive(&consumer).await, receive(&consumer).await];
    commit_messages.sort_by_key(|message| message.source().segment_id().0);
    let commit_positions = position_from_messages(1, &commit_messages);
    let commit_txn = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open commit transaction");
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::Ack,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay first vector transactional ACK");
    let mut vector_ack =
        Box::pin(consumer.acknowledge_positions_in_transaction(&commit_positions, commit_txn));
    tokio::select! {
        biased;
        result = &mut vector_ack => panic!("delayed vector ACK completed early: {result:?}"),
        () = cluster.wait_for("delayed first vector transactional ACK", |fake| {
            fake.pending_operations().iter().any(|pending| pending.kind == OperationKind::Ack)
        }) => {}
    }
    let mut commit = Box::pin(client.commit_transaction(commit_txn));
    tokio::select! {
        biased;
        result = &mut commit => panic!("commit completed before admitted vector ACK: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    let commit_waited_without_wire_end =
        cluster.inspect(|fake| end_transaction_command_count(fake, "commit") == 0);
    assert!(commit_waited_without_wire_end);
    drop(commit);
    let pending_commit_cancelled = true;
    let delayed_ack = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::Ack)
                .map(|pending| pending.id)
        })
        .expect("delayed transaction ACK id");
    cluster
        .update(|fake| fake.complete_pending(delayed_ack, PendingCompletion::Succeed))
        .expect("complete first vector transaction ACK");
    vector_ack
        .await
        .expect("both vector transaction components are admitted");
    let staged = cluster
        .inspect(|fake| fake.transaction_observation(commit_txn.id()))
        .expect("open commit transaction observation");
    let commit_registrations = staged.registered_subscriptions.len();
    let commit_staged_acks = staged.staged_acknowledgements;
    assert_eq!(commit_registrations, 2);
    assert_eq!(commit_staged_acks, 2);
    assert_eq!(
        cluster.inspect(|fake| {
            vec![
                fake.durable_cursor("transaction-sub", 1)
                    .expect("segment-one cursor"),
                fake.durable_cursor("transaction-sub", 2)
                    .expect("segment-two cursor"),
            ]
        }),
        vec![0, 0],
        "transactional ACK admission does not advance durable cursors"
    );
    assert_eq!(
        client
            .commit_transaction(commit_txn)
            .await
            .expect("commit coordinator response after cancelling the local wait"),
        magnetar::TxnState::Committed
    );
    let committed_cursors = cluster.inspect(|fake| {
        vec![
            fake.durable_cursor("transaction-sub", 1)
                .expect("segment-one cursor"),
            fake.durable_cursor("transaction-sub", 2)
                .expect("segment-two cursor"),
        ]
    });
    assert_eq!(committed_cursors, vec![1, 1]);
    assert_eq!(
        cluster
            .inspect(|fake| fake.transaction_observation(commit_txn.id()))
            .expect("committed transaction")
            .state,
        FakeTransactionState::Committed
    );

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"cancelled-end")))
        .expect("enqueue cancelled EndTxn participant");
    cluster
        .wait_for("cancelled EndTxn participant delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let cancelled_message = receive(&consumer).await;
    let cancelled_txn = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open cancellation transaction");
    consumer
        .acknowledge_in_transaction(&cancelled_message, cancelled_txn)
        .await
        .expect("stage cancellation transaction acknowledgement");
    cluster
        .update(|fake| {
            fake.clear_routes();
            Ok(())
        })
        .expect("clear routes before EndTxn cancellation");
    cluster.hold_command(
        Endpoint::Controller,
        magnetar::proto::pb::base_command::Type::EndTxnResponse,
    );
    let mut first_commit = Box::pin(client.commit_transaction(cancelled_txn));
    tokio::select! {
        biased;
        result = &mut first_commit => panic!("held EndTxn response completed early: {result:?}"),
        () = cluster.wait_for("first cancelled EndTxn command", |fake| {
            end_transaction_command_count(fake, "commit") == 1
        }) => {}
    }
    assert_eq!(
        already_ending_kind(
            client
                .commit_transaction(cancelled_txn)
                .await
                .expect_err("a concurrent transaction end is fenced"),
        ),
        "transaction-already-ending"
    );
    drop(first_commit);

    let mut resumed_commit = Box::pin(client.commit_transaction(cancelled_txn));
    tokio::select! {
        biased;
        result = &mut resumed_commit => panic!("held resumed EndTxn completed early: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    let cancelled_commit_commands =
        cluster.inspect(|fake| end_transaction_command_count(fake, "commit"));
    assert_eq!(cancelled_commit_commands, 1);
    cluster.release_command(
        Endpoint::Controller,
        magnetar::proto::pb::base_command::Type::EndTxnResponse,
    );
    assert_eq!(
        resumed_commit.await.expect("resume pending EndTxn request"),
        magnetar::TxnState::Committed
    );
    assert_eq!(
        cluster.inspect(|fake| end_transaction_command_count(fake, "commit")),
        1,
        "cancellation must not duplicate the broker EndTxn mutation"
    );

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"abort-one")))
        .expect("enqueue first abort participant");
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"abort-two")))
        .expect("enqueue second abort participant");
    cluster
        .wait_for("abort participant deliveries", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let mut abort_messages = vec![receive(&consumer).await, receive(&consumer).await];
    abort_messages.sort_by_key(|message| message.source().segment_id().0);
    let abort_positions = position_from_messages(1, &abort_messages);
    let abort_txn = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open abort transaction");
    consumer
        .acknowledge_positions_in_transaction(&abort_positions, abort_txn)
        .await
        .expect("stage abort vector acknowledgements");
    assert_eq!(
        cluster.inspect(|fake| {
            fake.transaction_observation(abort_txn.id())
                .expect("open abort transaction")
                .staged_acknowledgements
        }),
        2
    );
    assert_eq!(
        client
            .abort_transaction(abort_txn)
            .await
            .expect("abort coordinator response"),
        magnetar::TxnState::Aborted
    );
    let aborted_cursors = cluster.inspect(|fake| {
        vec![
            fake.durable_cursor("transaction-sub", 1)
                .expect("segment-one cursor"),
            fake.durable_cursor("transaction-sub", 2)
                .expect("segment-two cursor"),
        ]
    });
    assert_eq!(aborted_cursors, vec![2, 1]);
    let first_redelivery = receive(&consumer).await;
    let second_redelivery = receive(&consumer).await;
    let mut abort_redeliveries = vec![
        (
            first_redelivery.source().segment_id().0,
            first_redelivery.raw().redelivery_count,
        ),
        (
            second_redelivery.source().segment_id().0,
            second_redelivery.raw().redelivery_count,
        ),
    ];
    abort_redeliveries.sort_unstable();
    assert_eq!(abort_redeliveries, vec![(1, 1), (2, 1)]);
    consumer
        .acknowledge_cumulative(&second_redelivery)
        .await
        .expect("cumulative acknowledgement resolves originals and redeliveries");
    cluster
        .wait_for("abort redeliveries acknowledged", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"poison")))
        .expect("enqueue poison participant");
    cluster
        .wait_for("poison participant delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let poison_message = receive(&consumer).await;
    let poison_txn = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open poison transaction");
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::Ack,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "transactional ACK failed",
                )),
            )
        })
        .expect("script transactional ACK failure");
    assert_eq!(
        engine_error_kind(
            consumer
                .acknowledge_in_transaction(&poison_message, poison_txn)
                .await
                .expect_err("transactional ACK failure poisons commit"),
        ),
        "engine"
    );
    let poison_error = poisoned_commit_kind(
        client
            .commit_transaction(poison_txn)
            .await
            .expect_err("poisoned commit is refused locally"),
    );
    let poison_commit_commands =
        cluster.inspect(|fake| end_transaction_command_count(fake, "commit"));
    assert_eq!(poison_commit_commands, 0);
    assert_eq!(
        client
            .abort_transaction(poison_txn)
            .await
            .expect("abort remains admissible after poison"),
        magnetar::TxnState::Aborted
    );
    let poison_abort_commands =
        cluster.inspect(|fake| end_transaction_command_count(fake, "abort"));
    assert_eq!(poison_abort_commands, 1);
    consumer
        .acknowledge(&poison_message)
        .await
        .expect("failed transactional ACK leaves delivery ordinarily acknowledgeable");

    cluster
        .wait_for("ordinary poison acknowledgement", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"registration-failure")))
        .expect("enqueue transaction-registration failure participant");
    cluster
        .wait_for("transaction-registration failure delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let registration_failure_message = receive(&consumer).await;
    let registration_failure_txn = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open transaction-registration failure transaction");
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.script_next(
                Endpoint::Controller,
                OperationKind::TransactionRegistration,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "transaction registration failed",
                )),
            )
        })
        .expect("script transaction-registration failure");
    let registration_failure_error = engine_error_kind(
        consumer
            .acknowledge_in_transaction(&registration_failure_message, registration_failure_txn)
            .await
            .expect_err("failed registration rejects transactional acknowledgement"),
    );
    assert_eq!(
        cluster.inspect(transaction_registration_command_count),
        1,
        "registration failure must stop before the transactional ACK"
    );
    let registration_observation = cluster
        .inspect(|fake| fake.transaction_observation(registration_failure_txn.id()))
        .expect("registration-failure transaction observation");
    assert!(registration_observation.registered_subscriptions.is_empty());
    assert_eq!(registration_observation.staged_acknowledgements, 0);
    assert_eq!(
        poisoned_commit_kind(
            client
                .commit_transaction(registration_failure_txn)
                .await
                .expect_err("failed registration poisons commit"),
        ),
        "transaction-poisoned"
    );
    let registration_failure_aborted = client
        .abort_transaction(registration_failure_txn)
        .await
        .expect("abort transaction after registration failure")
        == magnetar::TxnState::Aborted;
    consumer
        .acknowledge(&registration_failure_message)
        .await
        .expect("registration failure leaves delivery ordinarily acknowledgeable");
    cluster
        .wait_for("transaction-registration failure cleanup", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"shared-registration-one")))
        .expect("enqueue first shared-registration participant");
    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"shared-registration-two")))
        .expect("enqueue second shared-registration participant");
    cluster
        .wait_for("shared-registration deliveries", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let shared_registration_messages = [receive(&consumer).await, receive(&consumer).await];
    let shared_registration_txn = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open shared-registration transaction");
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.script_next(
                Endpoint::Controller,
                OperationKind::TransactionRegistration,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay shared transaction registration");
    let mut first_shared_ack = Box::pin(
        consumer
            .acknowledge_in_transaction(&shared_registration_messages[0], shared_registration_txn),
    );
    tokio::select! {
        biased;
        result = &mut first_shared_ack => panic!("delayed first shared-registration ACK completed early: {result:?}"),
        () = cluster.wait_for("pending shared transaction registration", |fake| {
            fake.pending_operations().iter().any(|pending| {
                pending.kind == OperationKind::TransactionRegistration
            })
        }) => {}
    }
    let mut second_shared_ack = Box::pin(
        consumer
            .acknowledge_in_transaction(&shared_registration_messages[1], shared_registration_txn),
    );
    tokio::select! {
        biased;
        result = &mut second_shared_ack => panic!("shared-registration follower completed early: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    let pending_registration = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::TransactionRegistration)
                .map(|pending| pending.id)
        })
        .expect("shared transaction registration id");
    cluster
        .update(|fake| fake.complete_pending(pending_registration, PendingCompletion::Succeed))
        .expect("complete shared transaction registration");
    first_shared_ack
        .await
        .expect("first shared-registration acknowledgement");
    second_shared_ack
        .await
        .expect("second shared-registration acknowledgement");
    let concurrent_registration_shared =
        cluster.inspect(|fake| transaction_registration_command_count(fake) == 1);
    assert!(concurrent_registration_shared);
    assert_eq!(
        client
            .commit_transaction(shared_registration_txn)
            .await
            .expect("commit shared-registration transaction"),
        magnetar::TxnState::Committed
    );
    cluster
        .wait_for("shared-registration transaction cleanup", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    let outcome_consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("outcome-retry-sub")
        .consumer_name("outcome-retry-owner")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe transaction-outcome retry owner");
    cluster
        .wait_for("transaction-outcome retry children", |fake| {
            fake.resource_counts().child_consumers == 4
        })
        .await;
    wait_for_initial_flow(&outcome_consumer, &[1, 2]).await;
    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"outcome-retry")))
        .expect("enqueue transaction-outcome retry participant");
    cluster
        .wait_for("transaction-outcome retry delivery", |fake| {
            fake.resource_counts().unacked_messages >= 1
        })
        .await;
    let outcome_message = receive(&outcome_consumer).await;
    let outcome_txn = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open transaction-outcome retry transaction");
    outcome_consumer
        .acknowledge_in_transaction(&outcome_message, outcome_txn)
        .await
        .expect("stage transaction-outcome retry acknowledgement");

    let takeover_consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("outcome-retry-sub")
        .consumer_name("outcome-retry-takeover")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe transaction-outcome retry takeover");
    let _ = wait_for_assignment(&takeover_consumer, 1, &[]).await;
    let outcome_member = cluster
        .inspect(|fake| fake.member("outcome-retry-sub", "outcome-retry-owner"))
        .expect("transaction-outcome retry owner member");
    let takeover_member = cluster
        .inspect(|fake| fake.member("outcome-retry-sub", "outcome-retry-takeover"))
        .expect("transaction-outcome retry takeover member");
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::Close,
                ScriptedBehavior::Delay,
            )?;
            fake.publish_assignment_plan(
                1,
                vec![
                    FullAssignment::new(outcome_member, [2]),
                    FullAssignment::new(takeover_member, [1]),
                ],
            )
        })
        .expect("transfer transaction-bearing segment before outcome");
    let _ = wait_for_assignment(&outcome_consumer, 1, &[2]).await;
    let _ = wait_for_assignment(&takeover_consumer, 1, &[1]).await;

    let mut outcome_commit = Box::pin(client.commit_transaction(outcome_txn));
    tokio::select! {
        biased;
        result = &mut outcome_commit => panic!("delayed outcome close completed early: {result:?}"),
        () = cluster.wait_for("retained transaction-outcome close", |fake| {
            end_transaction_command_count(fake, "commit") == 1
                && fake.pending_operations().iter().any(|pending| {
                    pending.kind == OperationKind::Close
                        && pending.endpoint == Endpoint::Segment(1)
                })
        }) => {}
    }
    let pending_outcome_close = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| {
                    pending.kind == OperationKind::Close && pending.endpoint == Endpoint::Segment(1)
                })
                .map(|pending| pending.id)
        })
        .expect("retained transaction-outcome close id");
    drop(outcome_commit);
    let mut resumed_outcome_commit = Box::pin(client.commit_transaction(outcome_txn));
    tokio::select! {
        biased;
        result = &mut resumed_outcome_commit => panic!("retained outcome retry completed early: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    let outcome_retry_reused_retained_close = cluster.inspect(|fake| {
        end_transaction_command_count(fake, "commit") == 1
            && fake
                .routes()
                .iter()
                .filter(|route| {
                    route.command == magnetar::proto::pb::base_command::Type::CloseConsumer
                })
                .count()
                == 1
            && fake
                .pending_operations()
                .iter()
                .any(|pending| pending.id == pending_outcome_close)
    });
    assert!(outcome_retry_reused_retained_close);
    cluster
        .update(|fake| fake.complete_pending(pending_outcome_close, PendingCompletion::Succeed))
        .expect("complete retained transaction-outcome close");
    assert_eq!(
        resumed_outcome_commit
            .await
            .expect("resume confirmed transaction outcome"),
        magnetar::TxnState::Committed
    );
    assert_eq!(
        cluster.inspect(|fake| fake.segment_unacked("outcome-retry-sub", 1)),
        0,
        "confirmed outcome clears the transferred segment delivery"
    );
    cluster
        .wait_for("transaction-outcome takeover", |fake| {
            fake.active_child_owner("outcome-retry-sub", 1) == Some(takeover_member)
                && fake.resource_counts().pending_operations == 0
        })
        .await;
    outcome_consumer
        .close()
        .await
        .expect("close transaction-outcome retry owner");
    takeover_consumer
        .close()
        .await
        .expect("close transaction-outcome retry takeover");
    let remaining_unacked = cluster.inspect(|fake| {
        (
            fake.segment_unacked("transaction-sub", 1),
            fake.segment_unacked("transaction-sub", 2),
            fake.segment_unacked("outcome-retry-sub", 1),
            fake.segment_unacked("outcome-retry-sub", 2),
        )
    });
    assert_eq!(
        remaining_unacked,
        (1, 0, 0, 0),
        "the independent transaction subscription receives its own published copy"
    );
    let independent_copy = receive(&consumer).await;
    assert_eq!(independent_copy.source().segment_id().0, 1);
    assert_eq!(independent_copy.value().as_ref(), b"outcome-retry");
    consumer
        .acknowledge(&independent_copy)
        .await
        .expect("acknowledge independent transaction-sub copy");
    cluster
        .wait_for("transaction-outcome retry unacked baseline", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"confirmed-retry")))
        .expect("enqueue confirmed-retry participant");
    cluster
        .wait_for("confirmed-retry participant delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let confirmed_message = receive(&consumer).await;
    let confirmed_txn = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open confirmed-retry transaction");
    consumer
        .acknowledge_in_transaction(&confirmed_message, confirmed_txn)
        .await
        .expect("stage confirmed-retry acknowledgement");
    cluster
        .update(|fake| {
            fake.clear_routes();
            Ok(())
        })
        .expect("clear routes before confirmed commit retry");
    cluster.hold_command(
        Endpoint::Controller,
        magnetar::proto::pb::base_command::Type::EndTxnResponse,
    );
    let mut confirmed_commit = Box::pin(client.commit_transaction(confirmed_txn));
    tokio::select! {
        biased;
        result = &mut confirmed_commit => panic!("held confirmed commit completed early: {result:?}"),
        () = cluster.wait_for("confirmed EndTxn command", |fake| {
            end_transaction_command_count(fake, "commit") == 1
        }) => {}
    }
    consumer
        .close()
        .await
        .expect("close participant after broker confirmed the commit");
    cluster.release_command(
        Endpoint::Controller,
        magnetar::proto::pb::base_command::Type::EndTxnResponse,
    );
    assert_eq!(
        confirmed_commit
            .await
            .expect("broker-confirmed commit survives local finalization failure"),
        magnetar::TxnState::Committed
    );
    assert!(
        client.abort_transaction(confirmed_txn).await.is_err(),
        "a broker-confirmed commit rejects the opposite finalization"
    );
    assert_eq!(
        client
            .commit_transaction(confirmed_txn)
            .await
            .expect("retry broker-confirmed commit"),
        magnetar::TxnState::Committed
    );
    let confirmed_commit_retried_without_wire_end =
        cluster.inspect(|fake| end_transaction_command_count(fake, "commit") == 1);
    assert!(confirmed_commit_retried_without_wire_end);

    let failed_finalization_consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("failed-finalization-sub")
        .consumer_name("failed-finalization-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe failed-finalization aggregate");
    cluster
        .wait_for("failed-finalization initial children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&failed_finalization_consumer, &[1, 2]).await;
    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"failed-commit-finalization")))
        .expect("enqueue failed-commit finalization participant");
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"failed-abort-finalization")))
        .expect("enqueue failed-abort finalization participant");
    cluster
        .wait_for("failed-finalization deliveries", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let mut failed_finalization_messages = [
        receive(&failed_finalization_consumer).await,
        receive(&failed_finalization_consumer).await,
    ];
    failed_finalization_messages.sort_by_key(|message| message.source().segment_id().0);
    let failed_commit_txn = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open failed-commit finalization transaction");
    let failed_abort_txn = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open failed-abort finalization transaction");
    failed_finalization_consumer
        .acknowledge_in_transaction(&failed_finalization_messages[0], failed_commit_txn)
        .await
        .expect("stage failed-commit finalization acknowledgement");
    failed_finalization_consumer
        .acknowledge_in_transaction(&failed_finalization_messages[1], failed_abort_txn)
        .await
        .expect("stage failed-abort finalization acknowledgement");
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.script_next(
                Endpoint::Controller,
                OperationKind::EndTransaction,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "commit failed before local finalization",
                )),
            )
        })
        .expect("script failed commit before local finalization");
    cluster.hold_command(
        Endpoint::Controller,
        magnetar::proto::pb::base_command::Type::EndTxnResponse,
    );
    let mut failed_commit = Box::pin(client.commit_transaction(failed_commit_txn));
    tokio::select! {
        biased;
        result = &mut failed_commit => panic!("held failed commit completed early: {result:?}"),
        () = cluster.wait_for("failed commit reached the broker", |fake| {
            end_transaction_command_count(fake, "commit") == 1
        }) => {}
    }
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Controller,
                OperationKind::EndTransaction,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "abort failed before local finalization",
                )),
            )
        })
        .expect("script failed abort before local finalization");
    let mut failed_abort = Box::pin(client.abort_transaction(failed_abort_txn));
    tokio::select! {
        biased;
        result = &mut failed_abort => panic!("held failed abort completed early: {result:?}"),
        () = cluster.wait_for("failed abort reached the broker", |fake| {
            end_transaction_command_count(fake, "abort") == 1
        }) => {}
    }
    failed_finalization_consumer
        .close()
        .await
        .expect("close failed-finalization aggregate");
    cluster.release_command(
        Endpoint::Controller,
        magnetar::proto::pb::base_command::Type::EndTxnResponse,
    );
    let failed_finalization_errors = (failed_commit.await.is_err(), failed_abort.await.is_err());
    assert_eq!(failed_finalization_errors, (true, true));
    cluster
        .wait_for("failed-finalization aggregate cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0 && counts.unacked_messages == 0
        })
        .await;

    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("unknown-transaction-sub")
        .consumer_name("unknown-transaction-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe unknown-outcome aggregate");
    cluster
        .wait_for("unknown-outcome initial children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"unknown-outcome")))
        .expect("enqueue unknown-outcome participant");
    cluster
        .wait_for("unknown-outcome participant delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let unknown_message = receive(&consumer).await;
    let unknown_txn = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open unknown-outcome transaction");
    consumer
        .acknowledge_in_transaction(&unknown_message, unknown_txn)
        .await
        .expect("stage unknown-outcome acknowledgement");
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Controller,
                OperationKind::EndTransaction,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "commit outcome is unknown",
                )),
            )
        })
        .expect("script failed transaction commit");
    let failed_commit_reported_unknown = matches!(
        client
            .commit_transaction(unknown_txn)
            .await
            .expect_err("scripted EndTxn commit must fail"),
        magnetar::PulsarError::Other(message) if message.contains("commit_transaction")
    );
    assert!(failed_commit_reported_unknown);

    let mut unknown_outcome_event = false;
    let mut unknown_resync_event = false;
    while !(unknown_outcome_event && unknown_resync_event) {
        match tokio::time::timeout(magnetar_differential::HANG_GUARD, consumer.next_event())
            .await
            .expect("unknown transaction event timed out")
            .expect("read unknown transaction event")
        {
            Some(StreamConsumerEvent::TransactionOutcome {
                txn_id,
                outcome: magnetar::scalable::TransactionOutcome::Unknown,
            }) if txn_id == unknown_txn.id() => unknown_outcome_event = true,
            Some(StreamConsumerEvent::ResyncRequired { .. }) => unknown_resync_event = true,
            Some(_) => {}
            None => panic!("aggregate event stream closed before unknown outcome propagation"),
        }
    }

    consumer
        .close()
        .await
        .expect("close unknown-outcome aggregate");
    cluster
        .wait_for("unknown-outcome aggregate cleanup", |fake| {
            fake.resource_counts().child_consumers == 0
        })
        .await;

    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("abort-finalization-sub")
        .consumer_name("abort-finalization-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe abort-finalization aggregate");
    cluster
        .wait_for("abort-finalization initial children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;
    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"confirmed-abort")))
        .expect("enqueue confirmed-abort participant");
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"failed-abort")))
        .expect("enqueue failed-abort participant");
    cluster
        .wait_for("abort-finalization deliveries", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let mut abort_messages = [receive(&consumer).await, receive(&consumer).await];
    abort_messages.sort_by_key(|message| message.source().segment_id().0);

    let confirmed_abort_txn = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open confirmed-abort transaction");
    consumer
        .acknowledge_in_transaction(&abort_messages[0], confirmed_abort_txn)
        .await
        .expect("stage confirmed-abort acknowledgement");
    let failed_abort_txn = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open failed-abort transaction");
    consumer
        .acknowledge_in_transaction(&abort_messages[1], failed_abort_txn)
        .await
        .expect("stage failed-abort acknowledgement");

    cluster
        .update(|fake| {
            fake.clear_routes();
            Ok(())
        })
        .expect("clear routes before confirmed abort retry");
    cluster.hold_command(
        Endpoint::Controller,
        magnetar::proto::pb::base_command::Type::EndTxnResponse,
    );
    let mut confirmed_abort = Box::pin(client.abort_transaction(confirmed_abort_txn));
    tokio::select! {
        biased;
        result = &mut confirmed_abort => panic!("held confirmed abort completed early: {result:?}"),
        () = cluster.wait_for("confirmed abort EndTxn command", |fake| {
            end_transaction_command_count(fake, "abort") == 1
        }) => {}
    }
    consumer
        .close()
        .await
        .expect("close participant after broker confirmed the abort");
    cluster.release_command(
        Endpoint::Controller,
        magnetar::proto::pb::base_command::Type::EndTxnResponse,
    );
    assert_eq!(
        confirmed_abort
            .await
            .expect("broker-confirmed abort survives local finalization failure"),
        magnetar::TxnState::Aborted
    );
    assert!(
        client
            .commit_transaction(confirmed_abort_txn)
            .await
            .is_err(),
        "a broker-confirmed abort rejects the opposite finalization"
    );
    assert_eq!(
        client
            .abort_transaction(confirmed_abort_txn)
            .await
            .expect("retry broker-confirmed abort"),
        magnetar::TxnState::Aborted
    );
    let confirmed_abort_retried_without_wire_end =
        cluster.inspect(|fake| end_transaction_command_count(fake, "abort") == 1);
    assert!(confirmed_abort_retried_without_wire_end);

    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.script_next(
                Endpoint::Controller,
                OperationKind::EndTransaction,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay failed transaction abort");
    let mut failed_abort = Box::pin(client.abort_transaction(failed_abort_txn));
    tokio::select! {
        biased;
        result = &mut failed_abort => panic!("delayed failed abort completed early: {result:?}"),
        () = cluster.wait_for("delayed failed abort", |fake| {
            fake.pending_operations()
                .iter()
                .any(|pending| pending.kind == OperationKind::EndTransaction)
        }) => {}
    }
    let pending_abort = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::EndTransaction)
                .map(|pending| pending.id)
        })
        .expect("delayed failed-abort operation");
    drop(failed_abort);
    let mut resumed_abort = Box::pin(client.abort_transaction(failed_abort_txn));
    tokio::select! {
        biased;
        result = &mut resumed_abort => panic!("resumed failed abort completed early: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    assert_eq!(
        cluster.inspect(|fake| end_transaction_command_count(fake, "abort")),
        1,
        "a cancelled abort waiter must resume the canonical wire request"
    );
    cluster
        .update(|fake| {
            fake.complete_pending(
                pending_abort,
                PendingCompletion::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "abort outcome is unknown",
                )),
            )
        })
        .expect("complete failed transaction abort");
    let failed_abort_reported_unknown = matches!(
        resumed_abort
            .await
            .expect_err("scripted EndTxn abort must fail"),
        magnetar::PulsarError::Other(message) if message.contains("abort_transaction")
    );
    assert!(failed_abort_reported_unknown);
    cluster
        .wait_for("abort-finalization aggregate cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0
                && counts.layout_sessions == 0
                && counts.pending_operations == 0
                && counts.permits == 0
        })
        .await;
    let after_close = cluster.inspect(M1FakeCluster::resource_counts);

    TransactionTrace {
        commit_waited_without_wire_end,
        commit_registrations,
        commit_staged_acks,
        committed_cursors,
        abort_redeliveries,
        aborted_cursors,
        poison_error,
        poison_commit_commands,
        poison_abort_commands,
        registration_failure_error,
        registration_failure_aborted,
        concurrent_registration_shared,
        cancelled_commit_commands,
        pending_commit_cancelled,
        confirmed_commit_retried_without_wire_end,
        outcome_retry_reused_retained_close,
        failed_finalization_errors,
        failed_commit_reported_unknown,
        unknown_outcome_event,
        unknown_resync_event,
        confirmed_abort_retried_without_wire_end,
        failed_abort_reported_unknown,
        after_close,
    }
}

async fn run_tokio_transactions() -> TransactionTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_transactions(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_transactions() -> TransactionTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_transactions(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_operation_cancellation<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> CancellationTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi + TransactionApi,
{
    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("cancellation-sub")
        .consumer_name("cancellation-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe cancellation aggregate");
    cluster
        .wait_for("cancellation initial children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"cancel-ack-one")))
        .expect("enqueue cancelled ordinary acknowledgement");
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"cancel-ack-two")))
        .expect("enqueue ordinary acknowledgement peer");
    cluster
        .wait_for("ordinary cancellation deliveries", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let mut messages = [receive(&consumer).await, receive(&consumer).await];
    messages.sort_by_key(|message| message.source().segment_id().0);

    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::Ack,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay ordinary acknowledgement");
    let mut ordinary_ack = Box::pin(consumer.acknowledge(&messages[0]));
    tokio::select! {
        biased;
        result = &mut ordinary_ack => panic!("delayed ordinary ACK completed early: {result:?}"),
        () = cluster.wait_for("pending cancelled ordinary ACK", |fake| {
            fake.pending_operations().iter().any(|pending| pending.kind == OperationKind::Ack)
        }) => {}
    }
    let pending_ack = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::Ack)
                .map(|pending| pending.id)
        })
        .expect("cancelled ordinary ACK id");
    drop(ordinary_ack);
    cluster
        .update(|fake| {
            fake.complete_pending(
                pending_ack,
                PendingCompletion::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "cancelled ordinary acknowledgement",
                )),
            )
        })
        .expect("complete cancelled ordinary acknowledgement");
    consumer
        .acknowledge(&messages[0])
        .await
        .expect("retry cancelled ordinary acknowledgement");
    consumer
        .acknowledge(&messages[1])
        .await
        .expect("acknowledge ordinary peer");
    cluster
        .wait_for("ordinary cancellation retry", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;
    let ordinary_ack_retried = true;

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"cancel-txn-registration")))
        .expect("enqueue cancelled transaction registration");
    cluster
        .wait_for("transaction-registration cancellation delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let registration_message = receive(&consumer).await;
    let registration_transaction = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open transaction-registration cancellation transaction");
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.script_next(
                Endpoint::Controller,
                OperationKind::TransactionRegistration,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay transaction registration for cancellation");
    let mut registration_ack = Box::pin(
        consumer.acknowledge_in_transaction(&registration_message, registration_transaction),
    );
    tokio::select! {
        biased;
        result = &mut registration_ack => panic!("delayed transaction registration completed early: {result:?}"),
        () = cluster.wait_for("pending cancelled transaction registration", |fake| {
            fake.pending_operations().iter().any(|pending| {
                pending.kind == OperationKind::TransactionRegistration
            })
        }) => {}
    }
    let pending_registration = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::TransactionRegistration)
                .map(|pending| pending.id)
        })
        .expect("cancelled transaction registration id");
    drop(registration_ack);
    cluster
        .update(|fake| {
            fake.complete_pending(
                pending_registration,
                PendingCompletion::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "cancelled transaction registration",
                )),
            )
        })
        .expect("complete cancelled transaction registration");
    assert_eq!(
        poisoned_commit_kind(
            client
                .commit_transaction(registration_transaction)
                .await
                .expect_err("cancelled transaction registration poisons commit"),
        ),
        "transaction-poisoned"
    );
    let transaction_registration_cancelled = client
        .abort_transaction(registration_transaction)
        .await
        .expect("abort transaction-registration cancellation")
        == magnetar::TxnState::Aborted;
    consumer
        .acknowledge(&registration_message)
        .await
        .expect("cancelled transaction registration leaves delivery live");
    cluster
        .wait_for("transaction-registration cancellation cleanup", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"cancel-txn-ack")))
        .expect("enqueue cancelled transactional acknowledgement");
    cluster
        .wait_for("transaction cancellation delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let transaction_message = receive(&consumer).await;
    let transaction = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open cancellation transaction");
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::Ack,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay transactional acknowledgement");
    let mut transactional_ack =
        Box::pin(consumer.acknowledge_in_transaction(&transaction_message, transaction));
    tokio::select! {
        biased;
        result = &mut transactional_ack => panic!("delayed transactional ACK completed early: {result:?}"),
        () = cluster.wait_for("pending cancelled transactional ACK", |fake| {
            fake.pending_operations().iter().any(|pending| pending.kind == OperationKind::Ack)
        }) => {}
    }
    let pending_transactional_ack = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::Ack)
                .map(|pending| pending.id)
        })
        .expect("cancelled transactional ACK id");
    drop(transactional_ack);
    cluster
        .update(|fake| {
            fake.complete_pending(
                pending_transactional_ack,
                PendingCompletion::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "cancelled transactional acknowledgement",
                )),
            )
        })
        .expect("complete cancelled transactional acknowledgement");
    let transaction_poisoned = poisoned_commit_kind(
        client
            .commit_transaction(transaction)
            .await
            .expect_err("cancelled transactional acknowledgement poisons commit"),
    );
    let transaction_aborted = client
        .abort_transaction(transaction)
        .await
        .expect("abort cancellation transaction")
        == magnetar::TxnState::Aborted;
    consumer
        .acknowledge(&transaction_message)
        .await
        .expect("cancelled transactional delivery remains live");
    cluster
        .wait_for("transaction cancellation cleanup", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    let positions = consumer.delivered_position();
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::Seek,
                ScriptedBehavior::Delay,
            )?;
            fake.script_next(
                Endpoint::Segment(2),
                OperationKind::Seek,
                ScriptedBehavior::Delay,
            )
        })
        .expect("clear routes before cancelled seek");
    let mut seek = Box::pin(consumer.seek_positions(&positions));
    tokio::select! {
        biased;
        result = &mut seek => panic!("held vector seek completed early: {result:?}"),
        () = cluster.wait_for("cancelled vector SEEK commands", |fake| {
            command_segments(fake, magnetar::proto::pb::base_command::Type::Seek) == vec![1, 2]
                && fake.pending_operations().iter().filter(|pending| {
                    pending.kind == OperationKind::Seek
                }).count() == 2
        }) => {}
    }
    let pending_seeks = cluster.inspect(|fake| {
        fake.pending_operations()
            .into_iter()
            .filter(|pending| pending.kind == OperationKind::Seek)
            .map(|pending| pending.id)
            .collect::<Vec<_>>()
    });
    drop(seek);
    for pending_seek in pending_seeks {
        cluster
            .update(|fake| {
                fake.complete_pending(
                    pending_seek,
                    PendingCompletion::Fail(BrokerFailure::new(
                        magnetar::proto::pb::ServerError::PersistenceError,
                        "cancelled aggregate seek",
                    )),
                )
            })
            .expect("complete cancelled seek operation");
    }
    assert_eq!(
        cluster
            .update(|fake| fake.disconnect_endpoint(Endpoint::Controller))
            .expect("disconnect cancelled-seek controller incarnation"),
        1,
        "M1 controller replacement releases the retained member"
    );
    let mut seek_cancellation_resync = false;
    let mut replacement_assignment = false;
    while !seek_cancellation_resync || !replacement_assignment {
        match next_event(&consumer).await {
            StreamConsumerEvent::ResyncRequired { reason } => {
                seek_cancellation_resync |= reason.contains("seek was cancelled");
            }
            StreamConsumerEvent::AssignmentApplied { .. } if seek_cancellation_resync => {
                replacement_assignment = true;
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected cancellation event: {unexpected:?}"),
        }
    }
    cluster
        .wait_for("cancelled seek replacement children", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 2 && counts.pending_operations == 0
        })
        .await;

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"stale-seek-one")))
        .expect("enqueue first current-generation seek target");
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"stale-seek-two")))
        .expect("enqueue second current-generation seek target");
    cluster
        .wait_for("current-generation seek targets", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let mut stale_targets = vec![receive(&consumer).await, receive(&consumer).await];
    stale_targets.sort_by_key(|message| message.source().segment_id().0);
    let stale_positions = position_from_messages(1, &stale_targets);
    consumer
        .acknowledge_batch(&stale_targets)
        .await
        .expect("acknowledge current-generation seek targets");
    cluster
        .wait_for("current-generation seek target acknowledgements", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;
    cluster
        .update(|fake| {
            fake.clear_routes();
            for endpoint in [Endpoint::Segment(1), Endpoint::Segment(2)] {
                fake.script_next(endpoint, OperationKind::Seek, ScriptedBehavior::Delay)?;
                fake.script_next(
                    endpoint,
                    OperationKind::SegmentOpen,
                    ScriptedBehavior::Delay,
                )?;
            }
            Ok(())
        })
        .expect("delay stale seek completions and replacement child opens");
    let mut stale_seek = Box::pin(consumer.seek_positions(&stale_positions));
    tokio::select! {
        biased;
        result = &mut stale_seek => panic!("stale-completion seek completed before fencing: {result:?}"),
        () = cluster.wait_for("stale-completion vector SEEK commands", |fake| {
            fake.pending_operations().iter().filter(|pending| {
                pending.kind == OperationKind::Seek
            }).count() == 2
        }) => {}
    }
    let stale_pending_seeks = cluster.inspect(|fake| {
        fake.pending_operations()
            .into_iter()
            .filter(|pending| pending.kind == OperationKind::Seek)
            .map(|pending| pending.id)
            .collect::<Vec<_>>()
    });
    for endpoint in [Endpoint::Segment(1), Endpoint::Segment(2)] {
        cluster.hold_command(endpoint, magnetar::proto::pb::base_command::Type::Success);
    }
    for pending_seek in stale_pending_seeks {
        cluster
            .update(|fake| fake.complete_pending(pending_seek, PendingCompletion::Succeed))
            .expect("queue successful stale seek response");
    }
    assert_eq!(
        cluster
            .update(|fake| fake.disconnect_endpoint(Endpoint::Controller))
            .expect("disconnect stale-seek controller incarnation"),
        1
    );
    loop {
        match next_event(&consumer).await {
            StreamConsumerEvent::ResyncRequired { .. } => break,
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected stale-seek fencing event: {unexpected:?}"),
        }
    }
    for endpoint in [Endpoint::Segment(1), Endpoint::Segment(2)] {
        cluster.release_command(endpoint, magnetar::proto::pb::base_command::Type::Success);
    }
    assert!(
        stale_seek.await.is_err(),
        "successful old-generation SEEK completion must fail after controller fencing"
    );
    let stale_seek_completion_fenced = cluster.inspect(|fake| {
        !fake
            .routes()
            .iter()
            .any(|route| route.command == magnetar::proto::pb::base_command::Type::Flow)
    });
    assert!(
        stale_seek_completion_fenced,
        "successful stale SEEK completions must not revive old-generation FLOW"
    );
    cluster
        .wait_for("delayed replacement child opens", |fake| {
            fake.pending_operations()
                .iter()
                .filter(|pending| pending.kind == OperationKind::SegmentOpen)
                .count()
                == 2
        })
        .await;
    let replacement_opens = cluster.inspect(|fake| {
        fake.pending_operations()
            .into_iter()
            .filter(|pending| pending.kind == OperationKind::SegmentOpen)
            .map(|pending| pending.id)
            .collect::<Vec<_>>()
    });
    for pending_open in replacement_opens {
        cluster
            .update(|fake| fake.complete_pending(pending_open, PendingCompletion::Succeed))
            .expect("complete replacement child open");
    }
    cluster
        .wait_for("stale-seek replacement children", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 2 && counts.pending_operations == 0
        })
        .await;

    CancellationTrace {
        ordinary_ack_retried,
        transaction_registration_cancelled,
        transaction_poisoned,
        transaction_aborted,
        seek_cancellation_resync,
        stale_seek_completion_fenced,
        after_close: close_and_count(consumer, cluster).await,
    }
}

async fn run_tokio_operation_cancellation() -> CancellationTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_operation_cancellation(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_operation_cancellation() -> CancellationTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_operation_cancellation(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_control_plane<E>(
    client_a: &PulsarClient<E>,
    client_b: &PulsarClient<E>,
    replacement_client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> ControlPlaneTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer_a = client_a
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("control-sub")
        .consumer_name("control-a")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe first control-plane member");
    cluster
        .wait_for("first control-plane children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer_a, &[1, 2]).await;
    cluster
        .update(|fake| {
            fake.clear_broker_frames();
            fake.script_next(
                Endpoint::Controller,
                OperationKind::ScalableOpen,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay second scalable subscribe response");
    let mut subscribe_b = Box::pin(
        client_b
            .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
            .subscription("control-sub")
            .consumer_name("control-b")
            .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
            .subscribe(),
    );
    tokio::select! {
        biased;
        result = &mut subscribe_b => panic!("delayed scalable subscribe completed early: {result:?}"),
        () = cluster.wait_for("delayed second scalable open", |fake| {
            fake.pending_operations().iter().any(|pending| {
                pending.kind == OperationKind::ScalableOpen
            })
        }) => {}
    }
    let member_a = cluster
        .inspect(|fake| fake.member("control-sub", "control-a"))
        .expect("first control member observable");
    let member_b = cluster
        .inspect(|fake| fake.member("control-sub", "control-b"))
        .expect("delayed control member observable");
    assert_ne!(member_a.connection, member_b.connection);
    let delayed_open = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::ScalableOpen)
                .map(|pending| pending.id)
        })
        .expect("delayed scalable open id");
    cluster
        .update(|fake| {
            fake.publish_assignment_plan(
                1,
                vec![
                    FullAssignment::new(member_a, [1]),
                    FullAssignment::new(member_b, [2]),
                ],
            )?;
            fake.complete_pending(delayed_open, PendingCompletion::Succeed)
        })
        .expect("push changed assignment before delayed response");
    let push_preceded_response = cluster.inspect(|fake| {
        let push = fake
            .broker_frames()
            .iter()
            .rposition(|frame| {
                frame.command
                    == magnetar::proto::pb::base_command::Type::ScalableTopicAssignmentUpdate
            })
            .expect("assignment push observation");
        let response = fake
            .broker_frames()
            .iter()
            .position(|frame| {
                frame.command
                    == magnetar::proto::pb::base_command::Type::ScalableTopicSubscribeResponse
            })
            .expect("subscribe response observation");
        push < response
    });
    assert!(push_preceded_response);
    let consumer_b = subscribe_b.await.expect("second control member subscribes");
    let mut assignments = vec![wait_for_assignment(&consumer_b, 1, &[2]).await];
    let _ = wait_for_assignment(&consumer_a, 1, &[1]).await;
    consumer_a
        .close()
        .await
        .expect("close first control member after observing pushed assignment");
    wait_for_flowing_sources(&consumer_b, &[2]).await;
    cluster
        .wait_for("initial reassigned child FLOW reaches the broker", |fake| {
            fake.active_child_owner("control-sub", 2) == Some(member_b)
                && fake.segment_permits("control-sub", 2) > 0
                && fake.resource_counts().pending_operations == 0
        })
        .await;
    cluster
        .update(|fake| {
            fake.advance_layout(2, same_topology_at_epoch_two())?;
            fake.publish_assignment_plan(
                2,
                vec![
                    FullAssignment::new(member_a, [1]),
                    FullAssignment::new(member_b, [2]),
                ],
            )
        })
        .expect("DAG push precedes matching epoch assignment");
    assignments.push(wait_for_assignment(&consumer_b, 2, &[2]).await);
    let sessions = cluster.inspect(M1FakeCluster::layout_session_ids);
    assert_eq!(sessions.len(), 1);
    cluster
        .update(|fake| {
            for (connection, session_id) in &sessions {
                fake.resend_layout(*connection, *session_id)?;
            }
            fake.resend_assignment(member_b)
        })
        .expect("resend exact layout and assignment duplicates");
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"detached-owner")))
        .expect("enqueue delivery before equal-epoch migration");
    cluster
        .wait_for("delivery before equal-epoch migration", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let detached = receive(&consumer_b).await;
    assert_eq!(detached.source().segment_id().0, 2);
    cluster
        .wait_for("pre-migration FLOW refill", |fake| {
            fake.segment_permits("control-sub", 2) > 0
        })
        .await;
    cluster
        .update(|fake| {
            fake.publish_assignment_plan(
                2,
                vec![
                    FullAssignment::new(member_a, [2]),
                    FullAssignment::new(member_b, [1]),
                ],
            )
        })
        .expect("publish changed equal-epoch ownership");
    assignments.push(wait_for_assignment(&consumer_b, 2, &[1]).await);
    loop {
        match next_event(&consumer_b).await {
            StreamConsumerEvent::SegmentPhaseChanged {
                source,
                phase: magnetar::proto::SegmentPhase::Draining,
            } if source.segment_id().0 == 2 => break,
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected equal-epoch drain event: {unexpected:?}"),
        }
    }
    consumer_b
        .acknowledge(&detached)
        .await
        .expect("detached child retains acknowledgement authority while draining");
    let equal_epoch_ack_retained = true;
    cluster
        .wait_for("equal-epoch ownership migration", |fake| {
            fake.active_child_owner("control-sub", 2) != Some(member_b)
                && fake.active_child_owner("control-sub", 1) == Some(member_b)
                && fake.resource_counts().pending_operations == 0
        })
        .await;
    wait_for_flowing_sources(&consumer_b, &[1]).await;
    assert_eq!(consumer_b.status().attached_segments(), 1);
    assert!(consumer_b.status().pending_ownership().is_empty());
    assert_eq!(
        assignments,
        vec![(1, vec![2]), (2, vec![2]), (2, vec![1])],
        "the pushed assignment, next layout epoch, and equal-epoch migration are applied"
    );
    let final_status_epoch = consumer_b.status().layout_epoch();
    assert_eq!(final_status_epoch, Some(2));

    let segment_opens_before_stale = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .filter(|route| route.command == magnetar::proto::pb::base_command::Type::Subscribe)
            .count()
    });
    cluster
        .update(|fake| fake.push_stale_assignment(member_b, 1, [1]))
        .expect("push lower-epoch assignment");
    let lower_epoch_reason = loop {
        match next_event(&consumer_b).await {
            StreamConsumerEvent::ResyncRequired { reason } => break reason,
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected lower-epoch fence event: {unexpected:?}"),
        }
    };
    assert!(lower_epoch_reason.contains("regressed"));
    cluster
        .wait_for("lower-epoch fail-closed child fence", |fake| {
            fake.active_child_owner("control-sub", 1) != Some(member_b)
                && fake.resource_counts().pending_operations == 0
        })
        .await;
    tokio::task::yield_now().await;
    assert_eq!(
        cluster.inspect(|fake| {
            fake.routes()
                .iter()
                .filter(|route| route.command == magnetar::proto::pb::base_command::Type::Subscribe)
                .count()
        }),
        segment_opens_before_stale,
        "a lower epoch must not enter a ConsumerBusy reopen loop"
    );
    let lower_epoch_fenced = matches!(
        consumer_b.status().phase(),
        magnetar::proto::AggregatePhase::Closing | magnetar::proto::AggregatePhase::Closed
    );
    assert!(lower_epoch_fenced);
    consumer_b
        .close()
        .await
        .expect("close epoch-ordering member");
    cluster
        .wait_for("epoch-ordering child cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0 && counts.pending_operations == 0 && counts.permits == 0
        })
        .await;

    let replacement_consumer = replacement_client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("control-replacement-sub")
        .consumer_name("control-replacement-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe isolated controller-replacement aggregate");
    cluster
        .wait_for("replacement aggregate initial children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&replacement_consumer, &[1, 2]).await;
    let replacement_components = [1, 2].into_iter().map(|segment_id| {
        let source = magnetar::proto::SegmentSource::new(
            magnetar::proto::SegmentId(segment_id),
            cluster.inspect(|fake| {
                fake.segment_topic(segment_id)
                    .expect("replacement segment topic")
            }),
        )
        .expect("canonical replacement segment source");
        (
            source,
            magnetar::proto::MessageId {
                ledger_id: segment_id,
                entry_id: 0,
                partition: -1,
                batch_index: -1,
                batch_size: 0,
            },
        )
    });
    let replacement_seek = magnetar::proto::PositionVector::new(2, replacement_components)
        .expect("all-current-leaf replacement seek");
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Segment(2),
                OperationKind::Seek,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "force controller replacement",
                )),
            )
        })
        .expect("script public resynchronization trigger");
    replacement_consumer
        .seek_positions(&replacement_seek)
        .await
        .expect_err("failed child operation requests a clean controller baseline");

    let old_controller = cluster
        .inspect(|fake| fake.member("control-replacement-sub", "control-replacement-member"))
        .expect("old controller member")
        .connection;
    cluster
        .update(|fake| fake.disconnect_connection(old_controller))
        .expect("disconnect old control-plane connection");
    let mut saw_resync = false;
    let replacement_baseline = loop {
        match next_event(&replacement_consumer).await {
            StreamConsumerEvent::ResyncRequired { .. } => saw_resync = true,
            StreamConsumerEvent::AssignmentApplied {
                layout_epoch,
                sources,
            } if saw_resync => {
                break (
                    layout_epoch,
                    sources
                        .iter()
                        .map(|source| source.segment_id().0)
                        .collect::<Vec<_>>(),
                );
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. } => {}
            unexpected => panic!("unexpected controller replacement event: {unexpected:?}"),
        }
    };
    assert_eq!(replacement_baseline, (2, vec![1, 2]));
    cluster
        .wait_for(
            "replacement controller baseline and child ownership",
            |fake| {
                fake.member("control-replacement-sub", "control-replacement-member")
                    .is_some_and(|member| {
                        member.connection != old_controller
                            && fake.assigned_owner("control-replacement-sub", 1) == Some(member)
                            && fake.assigned_owner("control-replacement-sub", 2) == Some(member)
                            && fake.active_child_owner("control-replacement-sub", 1) == Some(member)
                            && fake.active_child_owner("control-replacement-sub", 2) == Some(member)
                    })
                    && fake.resource_counts().child_consumers == 2
            },
        )
        .await;
    wait_for_flowing_sources(&replacement_consumer, &[1, 2]).await;
    let replacement_controller = cluster
        .inspect(|fake| fake.member("control-replacement-sub", "control-replacement-member"))
        .expect("replacement controller member")
        .connection;
    assert_ne!(old_controller, replacement_controller);
    let replacement_status = replacement_consumer.status();
    assert_eq!(replacement_status.layout_epoch(), Some(2));
    assert_eq!(replacement_status.assigned_segments(), 2);
    assert_eq!(replacement_status.attached_segments(), 2);

    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Controller,
                OperationKind::ScalableOpen,
                ScriptedBehavior::Delay,
            )?;
            fake.script_next(
                Endpoint::Segment(2),
                OperationKind::Seek,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "force alignment-failure reconnect",
                )),
            )
        })
        .expect("script delayed replacement controller and failed seek");
    replacement_consumer
        .seek_positions(&replacement_seek)
        .await
        .expect_err("failed child operation requests another controller baseline");
    let prior_controller = cluster
        .inspect(|fake| fake.member("control-replacement-sub", "control-replacement-member"))
        .expect("controller before alignment-failure reconnect")
        .connection;
    cluster
        .update(|fake| fake.disconnect_connection(prior_controller))
        .expect("disconnect controller before alignment-failure reconnect");
    cluster
        .wait_for("delayed alignment-failure controller open", |fake| {
            fake.pending_operations()
                .iter()
                .any(|pending| pending.kind == OperationKind::ScalableOpen)
                && fake
                    .member("control-replacement-sub", "control-replacement-member")
                    .is_some_and(|member| member.connection != prior_controller)
                && fake.resource_counts().child_consumers == 0
        })
        .await;
    let pending_open = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::ScalableOpen)
                .map(|pending| pending.id)
        })
        .expect("delayed alignment-failure controller open id");
    let pending_member = cluster
        .inspect(|fake| fake.member("control-replacement-sub", "control-replacement-member"))
        .expect("pending alignment-failure controller member");
    let (dag_connection, dag_session) = cluster
        .inspect(M1FakeCluster::layout_session_ids)
        .into_iter()
        .find(|(connection, _)| *connection == pending_member.connection)
        .expect("replacement DAG session on pending controller connection");
    cluster
        .update(|fake| {
            fake.fail_layout_session(
                dag_connection,
                dag_session,
                BrokerFailure::new(
                    magnetar::proto::pb::ServerError::ServiceNotReady,
                    "alignment DAG closed before epoch catch-up",
                ),
            )?;
            fake.advance_layout(3, same_topology_at_epoch_two())?;
            fake.publish_assignment_plan(3, vec![FullAssignment::new(pending_member, [1, 2])])?;
            fake.complete_pending(pending_open, PendingCompletion::Succeed)
        })
        .expect("complete mismatched replacement controller baseline");
    let mut alignment_failure_applied_epoch_three = false;
    let alignment_failure_reported = loop {
        match next_event(&replacement_consumer).await {
            StreamConsumerEvent::ResyncRequired { reason }
                if reason.contains("alignment DAG closed before epoch catch-up") =>
            {
                break true;
            }
            StreamConsumerEvent::AssignmentApplied {
                layout_epoch: 3, ..
            } => {
                alignment_failure_applied_epoch_three = true;
            }
            StreamConsumerEvent::ResyncRequired { .. }
            | StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected alignment-failure event: {unexpected:?}"),
        }
    };
    assert!(alignment_failure_reported);
    assert!(!alignment_failure_applied_epoch_three);

    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Controller,
                OperationKind::ScalableOpen,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::ServiceNotReady,
                    "retry controller registration",
                )),
            )?;
            fake.disconnect_connection(pending_member.connection)
        })
        .expect("release failed alignment authority and reject one retry");
    let mut alignment_retry_reported = false;
    let alignment_retry_baseline = loop {
        match next_event(&replacement_consumer).await {
            StreamConsumerEvent::ResyncRequired { reason } => {
                alignment_retry_reported |= reason.contains("retry controller registration");
            }
            StreamConsumerEvent::AssignmentApplied {
                layout_epoch,
                sources,
            } if alignment_retry_reported => {
                break (
                    layout_epoch,
                    sources
                        .iter()
                        .map(|source| source.segment_id().0)
                        .collect::<Vec<_>>(),
                );
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected alignment-retry event: {unexpected:?}"),
        }
    };
    assert!(alignment_retry_reported);
    assert_eq!(alignment_retry_baseline, (3, vec![1, 2]));

    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Controller,
                OperationKind::ScalableOpen,
                ScriptedBehavior::Delay,
            )?;
            fake.script_next(
                Endpoint::Segment(2),
                OperationKind::Seek,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "force close-during-alignment reconnect",
                )),
            )
        })
        .expect("script delayed controller baseline before close");
    replacement_consumer
        .seek_positions(&replacement_seek)
        .await
        .expect_err("failed seek requests close-during-alignment reconnect");
    let closing_controller = cluster
        .inspect(|fake| fake.member("control-replacement-sub", "control-replacement-member"))
        .expect("controller before close-during-alignment reconnect")
        .connection;
    cluster
        .update(|fake| fake.disconnect_connection(closing_controller))
        .expect("disconnect controller before close-during-alignment reconnect");
    cluster
        .wait_for(
            "delayed controller baseline before aggregate close",
            |fake| {
                fake.pending_operations()
                    .iter()
                    .any(|pending| pending.kind == OperationKind::ScalableOpen)
                    && fake.resource_counts().child_consumers == 0
            },
        )
        .await;
    let delayed_baseline = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::ScalableOpen)
                .map(|pending| pending.id)
        })
        .expect("delayed close-during-alignment baseline id");
    let mut close = Box::pin(replacement_consumer.clone().close());
    tokio::select! {
        biased;
        result = &mut close => panic!("aggregate close completed before delayed baseline: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    cluster
        .update(|fake| fake.complete_pending(delayed_baseline, PendingCompletion::Succeed))
        .expect("release controller baseline after aggregate close starts");
    close
        .await
        .expect("close interrupts replacement control-plane alignment");
    let close_interrupted_alignment = true;
    cluster
        .wait_for("control-plane cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0
                && counts.layout_sessions == 0
                && counts.pending_operations == 0
                && counts.permits == 0
        })
        .await;

    ControlPlaneTrace {
        push_preceded_response,
        assignments,
        controller_incarnations: 2,
        replacement_baseline,
        final_status_epoch,
        equal_epoch_ack_retained,
        lower_epoch_fenced,
        alignment_failure_reported,
        alignment_failure_applied_epoch_three,
        alignment_retry_reported,
        alignment_retry_baseline,
        close_interrupted_alignment,
        after_close: cluster.inspect(M1FakeCluster::resource_counts),
    }
}

async fn run_tokio_control_plane() -> ControlPlaneTrace {
    let cluster = M1SocketCluster::bind().await;
    let client_a = connect_tokio(&cluster).await;
    let client_b = connect_tokio(&cluster).await;
    let replacement_client = connect_tokio(&cluster).await;
    let trace = observe_control_plane(&client_a, &client_b, &replacement_client, &cluster).await;
    client_a.close().await;
    client_b.close().await;
    replacement_client.close().await;
    trace
}

async fn run_moonpool_control_plane() -> ControlPlaneTrace {
    let cluster = M1SocketCluster::bind().await;
    let client_a = connect_moonpool(&cluster).await;
    let client_b = connect_moonpool(&cluster).await;
    let replacement_client = connect_moonpool(&cluster).await;
    let trace = observe_control_plane(&client_a, &client_b, &replacement_client, &cluster).await;
    client_a.close().await;
    client_b.close().await;
    replacement_client.close().await;
    trace
}

async fn observe_terminal_controller_failure<E, Close, CloseFuture>(
    client: PulsarClient<E>,
    close_client: Close,
    cluster: &M1SocketCluster,
) -> TerminalControllerTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
    Close: FnOnce(PulsarClient<E>) -> CloseFuture,
    CloseFuture: std::future::Future<Output = ()>,
{
    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("terminal-controller-sub")
        .consumer_name("terminal-controller-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe terminal-controller aggregate");
    cluster
        .wait_for("terminal-controller initial children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;
    let seek = magnetar::proto::PositionVector::new(
        1,
        [1, 2].into_iter().map(|segment_id| {
            let source = magnetar::proto::SegmentSource::new(
                magnetar::proto::SegmentId(segment_id),
                cluster.inspect(|fake| {
                    fake.segment_topic(segment_id)
                        .expect("terminal-controller segment topic")
                }),
            )
            .expect("canonical terminal-controller source");
            (
                source,
                magnetar::proto::MessageId {
                    ledger_id: segment_id,
                    entry_id: 0,
                    partition: -1,
                    batch_index: -1,
                    batch_size: 0,
                },
            )
        }),
    )
    .expect("terminal-controller all-leaf seek");
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Controller,
                OperationKind::ScalableOpen,
                ScriptedBehavior::Delay,
            )?;
            fake.script_next(
                Endpoint::Segment(2),
                OperationKind::Seek,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "force terminal controller reconnect",
                )),
            )
        })
        .expect("script terminal replacement controller and failed seek");
    consumer
        .seek_positions(&seek)
        .await
        .expect_err("failed seek requests terminal controller reconnect");
    let controller = cluster
        .inspect(|fake| fake.member("terminal-controller-sub", "terminal-controller-member"))
        .expect("controller before terminal reconnect")
        .connection;
    cluster
        .update(|fake| fake.disconnect_connection(controller))
        .expect("disconnect controller before terminal reconnect");
    cluster
        .wait_for("pending terminal controller registration", |fake| {
            fake.pending_operations()
                .iter()
                .any(|pending| pending.kind == OperationKind::ScalableOpen)
                && fake.resource_counts().child_consumers == 0
        })
        .await;
    close_client(client).await;
    let mut replacement_assignment_applied = false;
    let terminal_failure_reported = loop {
        match next_event(&consumer).await {
            StreamConsumerEvent::ResyncRequired { reason }
                if reason.contains("closed") || reason.contains("driver") =>
            {
                break true;
            }
            StreamConsumerEvent::AssignmentApplied { .. } => {
                replacement_assignment_applied = true;
            }
            StreamConsumerEvent::ResyncRequired { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected terminal-controller event: {unexpected:?}"),
        }
    };
    cluster
        .wait_for("terminal-controller cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.connections == 0
                && counts.child_consumers == 0
                && counts.layout_sessions == 0
                && counts.pending_operations == 0
                && counts.permits == 0
        })
        .await;
    TerminalControllerTrace {
        terminal_failure_reported,
        replacement_assignment_applied,
        after_close: cluster.inspect(M1FakeCluster::resource_counts),
    }
}

async fn run_tokio_terminal_controller_failure() -> TerminalControllerTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    observe_terminal_controller_failure(
        client,
        PulsarClient::<magnetar::TokioEngine>::close,
        &cluster,
    )
    .await
}

async fn run_moonpool_terminal_controller_failure() -> TerminalControllerTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    observe_terminal_controller_failure(
        client,
        PulsarClient::<magnetar::MoonpoolEngine<moonpool_core::TokioProviders>>::close,
        &cluster,
    )
    .await
}

async fn observe_terminal_dag_reconnect<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> TerminalDagReconnectTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("terminal-dag-reconnect-sub")
        .consumer_name("terminal-dag-reconnect-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe terminal-DAG-reconnect aggregate");
    cluster
        .wait_for("terminal-DAG-reconnect children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.enqueue_message(2, Bytes::from_static(b"terminal-dag-queued"))
        })
        .expect("enqueue queued delivery before terminal-DAG resync");
    cluster
        .wait_for("queued delivery reaches terminal-DAG aggregate", |fake| {
            fake.resource_counts().unacked_messages == 1 && flow_command_count(fake, 2) > 0
        })
        .await;
    cluster
        .update(|fake| {
            for endpoint in [Endpoint::Segment(1), Endpoint::Segment(2)] {
                fake.script_next(endpoint, OperationKind::Close, ScriptedBehavior::Delay)?;
            }
            fake.enqueue_message_with_metadata(
                1,
                magnetar::proto::pb::MessageMetadata {
                    compression: Some(magnetar::proto::pb::CompressionType::Snappy as i32),
                    uncompressed_size: Some(4),
                    ..Default::default()
                },
                snappy(b"terminal-dag"),
                Vec::new(),
            )
        })
        .expect("enqueue terminal-DAG-reconnect malformed delivery");
    cluster
        .wait_for("terminal-DAG-reconnect delivery", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    loop {
        match next_event(&consumer).await {
            StreamConsumerEvent::ResyncRequired { reason } if reason.contains("decompress") => {
                break;
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. }
            | StreamConsumerEvent::ResyncRequired { .. } => {}
            unexpected => panic!("unexpected terminal-DAG-reconnect event: {unexpected:?}"),
        }
    }
    cluster
        .wait_for("first held terminal-DAG-reconnect child close", |fake| {
            fake.pending_operations()
                .iter()
                .any(|pending| pending.kind == OperationKind::Close)
        })
        .await;
    for close_index in 0..2 {
        cluster
            .wait_for("held terminal-DAG-reconnect child close", |fake| {
                fake.pending_operations()
                    .iter()
                    .any(|pending| pending.kind == OperationKind::Close)
            })
            .await;
        let pending = cluster
            .inspect(|fake| {
                fake.pending_operations()
                    .into_iter()
                    .find(|pending| pending.kind == OperationKind::Close)
                    .map(|pending| pending.id)
            })
            .expect("held terminal-DAG-reconnect child close id");
        if close_index == 0 {
            cluster
                .update(|fake| fake.complete_pending(pending, PendingCompletion::Succeed))
                .expect("complete first terminal-DAG-reconnect child close");
        } else {
            cluster.hold_command(
                Endpoint::Controller,
                magnetar::proto::pb::base_command::Type::ScalableTopicUpdate,
            );
            cluster
                .update(|fake| fake.complete_pending(pending, PendingCompletion::Succeed))
                .expect("complete final terminal-DAG-reconnect child close");
            cluster
                .wait_for("replacement terminal-DAG-reconnect controller lookup", |fake| {
                    fake.routes().iter().any(|route| {
                        route.endpoint == Endpoint::Controller
                            && route.command
                                == magnetar::proto::pb::base_command::Type::ScalableTopicLookup
                            && route.resource.as_deref() == Some(TOPIC)
                    })
                })
                .await;
            assert_eq!(
                cluster
                    .update(|fake| fake.disconnect_endpoint(Endpoint::Controller))
                    .expect("disconnect replacement terminal-DAG-reconnect controller"),
                1
            );
        }
    }
    let mut replacement_assignment_applied = false;
    let terminal_reason = loop {
        match next_event(&consumer).await {
            StreamConsumerEvent::ResyncRequired { reason }
                if reason.contains("closed") || reason.contains("driver") =>
            {
                break reason;
            }
            StreamConsumerEvent::AssignmentApplied { .. } => {
                replacement_assignment_applied = true;
            }
            StreamConsumerEvent::ResyncRequired { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected terminal-DAG-reconnect event: {unexpected:?}"),
        }
    };
    assert!(!replacement_assignment_applied);
    let receive_error = tokio::time::timeout(magnetar_differential::HANG_GUARD, consumer.receive())
        .await
        .expect("terminal DAG recovery must resolve aggregate receive")
        .expect_err("terminal DAG recovery must fence the queued delivery");
    assert!(!receive_error.to_string().is_empty());
    let queued_delivery_fenced = true;
    let after_close = close_and_count(consumer, cluster).await;
    cluster.release_command(
        Endpoint::Controller,
        magnetar::proto::pb::base_command::Type::ScalableTopicUpdate,
    );
    TerminalDagReconnectTrace {
        terminal_reason,
        replacement_assignment_applied,
        queued_delivery_fenced,
        after_close,
    }
}

async fn run_tokio_terminal_dag_reconnect() -> TerminalDagReconnectTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio_with_terminal_reconnect_budget(&cluster).await;
    let trace = observe_terminal_dag_reconnect(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_terminal_dag_reconnect() -> TerminalDagReconnectTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool_with_terminal_reconnect_budget(&cluster).await;
    let trace = observe_terminal_dag_reconnect(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_delivery_shapes<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> DeliveryShapeTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("delivery-shapes-sub")
        .consumer_name("delivery-shapes-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe delivery-shape aggregate");
    cluster
        .wait_for("delivery-shape children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;

    let compressed_plain = encoded_batch(&[b"first", b"second"]);
    cluster
        .update(|fake| {
            fake.enqueue_message_with_metadata(
                1,
                magnetar::proto::pb::MessageMetadata {
                    compression: Some(magnetar::proto::pb::CompressionType::Zlib as i32),
                    uncompressed_size: Some(
                        u32::try_from(compressed_plain.len()).expect("compressed batch fits u32"),
                    ),
                    num_messages_in_batch: Some(2),
                    ..Default::default()
                },
                zlib(&compressed_plain),
                vec![0b11],
            )
        })
        .expect("enqueue compressed batch");
    cluster
        .wait_for("compressed batch dispatch", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let mut compressed = consumer
        .receive_batch(
            BatchReceivePolicy::new(2, 5, Duration::from_secs(2))
                .expect("valid byte-capped batch policy"),
        )
        .await
        .expect("receive byte-capped compressed batch");
    assert_eq!(
        compressed.len(),
        1,
        "the byte cap stops before the second item"
    );
    compressed.push(receive(&consumer).await);
    let compressed_payloads = compressed
        .iter()
        .map(|message| message.payload().to_vec())
        .collect::<Vec<_>>();
    let compressed_batch_indexes = compressed
        .iter()
        .map(|message| {
            let ordinary = message
                .message_id()
                .ordinary_message_id_data()
                .expect("canonical compressed-batch id");
            assert_eq!(ordinary.batch_size, Some(2));
            assert_eq!(ordinary.ack_set, vec![0b11]);
            ordinary.batch_index.expect("compressed-batch index")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        compressed_payloads,
        vec![b"first".to_vec(), b"second".to_vec()]
    );
    assert_eq!(compressed_batch_indexes, vec![0, 1]);
    consumer
        .acknowledge_batch(&compressed)
        .await
        .expect("acknowledge compressed batch components");
    cluster
        .wait_for("compressed batch acknowledgement", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    for (name, compression, payload) in [
        (
            "lz4",
            magnetar::proto::pb::CompressionType::Lz4,
            lz4(b"codec"),
        ),
        (
            "zstd",
            magnetar::proto::pb::CompressionType::Zstd,
            zstd(b"codec"),
        ),
        (
            "snappy",
            magnetar::proto::pb::CompressionType::Snappy,
            snappy(b"codec"),
        ),
    ] {
        cluster
            .update(|fake| {
                fake.enqueue_message_with_metadata(
                    1,
                    magnetar::proto::pb::MessageMetadata {
                        compression: Some(compression as i32),
                        uncompressed_size: Some(5),
                        ..Default::default()
                    },
                    payload,
                    Vec::new(),
                )
            })
            .unwrap_or_else(|error| panic!("enqueue {name} delivery: {error}"));
        cluster
            .wait_for(&format!("{name} delivery dispatch"), |fake| {
                fake.resource_counts().unacked_messages == 1
            })
            .await;
        let message = receive(&consumer).await;
        assert_eq!(message.payload(), b"codec");
        consumer
            .acknowledge(&message)
            .await
            .unwrap_or_else(|error| panic!("acknowledge {name} delivery: {error}"));
        cluster
            .wait_for(&format!("{name} delivery acknowledgement"), |fake| {
                fake.resource_counts().unacked_messages == 0
            })
            .await;
    }

    cluster
        .update(|fake| {
            fake.enqueue_message_with_metadata(
                1,
                magnetar::proto::pb::MessageMetadata {
                    num_messages_in_batch: Some(3),
                    ..Default::default()
                },
                encoded_batch(&[b"left", b"omitted", b"right"]),
                vec![0b101],
            )
        })
        .expect("enqueue partial batch");
    cluster
        .wait_for("partial batch dispatch", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let partial = consumer
        .receive_batch(
            BatchReceivePolicy::new(3, 1024, Duration::from_secs(2))
                .expect("valid partial-batch policy"),
        )
        .await
        .expect("receive selected partial-batch members");
    let partial_payloads = partial
        .iter()
        .map(|message| message.payload().to_vec())
        .collect::<Vec<_>>();
    let partial_batch_indexes = partial
        .iter()
        .map(|message| {
            let ordinary = message
                .message_id()
                .ordinary_message_id_data()
                .expect("canonical partial-batch id");
            assert_eq!(ordinary.batch_size, Some(3));
            assert_eq!(ordinary.ack_set, vec![0b101]);
            ordinary.batch_index.expect("partial-batch index")
        })
        .collect::<Vec<_>>();
    assert_eq!(partial_payloads, vec![b"left".to_vec(), b"right".to_vec()]);
    assert_eq!(partial_batch_indexes, vec![0, 2]);
    consumer
        .acknowledge_batch(&partial)
        .await
        .expect("acknowledge partial batch components");
    cluster
        .wait_for("partial batch acknowledgement", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    let chunk_metadata = |chunk_id| magnetar::proto::pb::MessageMetadata {
        uuid: Some("delivery-shape-chunk".to_owned()),
        num_chunks_from_msg: Some(2),
        chunk_id: Some(chunk_id),
        total_chunk_msg_size: Some(4),
        ..Default::default()
    };
    cluster
        .update(|fake| {
            fake.enqueue_message_with_metadata(
                1,
                chunk_metadata(0),
                Bytes::from_static(b"ab"),
                vec![],
            )?;
            fake.enqueue_message_with_metadata(
                1,
                chunk_metadata(1),
                Bytes::from_static(b"cd"),
                vec![],
            )
        })
        .expect("enqueue chunk chain");
    cluster
        .wait_for("chunk chain dispatch", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let chunk = receive(&consumer).await;
    let chunk_payload = chunk.payload().to_vec();
    let chunk_ordinary = chunk
        .message_id()
        .ordinary_message_id_data()
        .expect("canonical chunk id");
    let chunk_first_entry = chunk_ordinary
        .first_chunk_message_id
        .as_deref()
        .map(|first| first.entry_id);
    assert_eq!(chunk_payload, b"abcd");
    assert_eq!(chunk_first_entry, Some(5));
    consumer
        .acknowledge(&chunk)
        .await
        .expect("acknowledge reassembled chunk chain");
    cluster
        .wait_for("chunk-chain acknowledgement", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    let _after_close = close_and_count(consumer, cluster).await;
    DeliveryShapeTrace {
        compressed_payloads,
        compressed_batch_indexes,
        partial_payloads,
        partial_batch_indexes,
        chunk_payload,
        chunk_first_entry,
    }
}

async fn run_tokio_delivery_shapes() -> DeliveryShapeTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_delivery_shapes(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_delivery_shapes() -> DeliveryShapeTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_delivery_shapes(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_child_open_lifecycle<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> ChildOpenTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::SegmentOpen,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::ConsumerBusy,
                    "retry child ownership",
                )),
            )
        })
        .expect("script one busy child open");
    let busy = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("busy-open-sub")
        .consumer_name("busy-open-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe busy-retry aggregate");
    cluster
        .wait_for("busy child retry succeeds", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&busy, &[1, 2]).await;
    let busy_segment_attempts = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .filter(|route| {
                route.endpoint == Endpoint::Segment(1)
                    && route.command == magnetar::proto::pb::base_command::Type::Subscribe
            })
            .count()
    });
    assert_eq!(busy_segment_attempts, 2);
    close_and_count(busy, cluster).await;

    let opens_before_permanent_failure = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .filter(|route| {
                route.endpoint == Endpoint::Segment(1)
                    && route.command == magnetar::proto::pb::base_command::Type::Subscribe
            })
            .count()
    });
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::SegmentOpen,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "permanent current child-open failure",
                )),
            )
        })
        .expect("script permanent current child-open failure");
    let failed = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("current-open-failure-sub")
        .consumer_name("current-open-failure-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe current child-open failure aggregate");
    let permanent_failure_resynced = loop {
        match next_event(&failed).await {
            StreamConsumerEvent::ResyncRequired { reason } => {
                assert!(
                    reason.contains("permanent current child-open failure"),
                    "unexpected permanent child-open failure reason: {reason}"
                );
                break true;
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected permanent child-open event: {unexpected:?}"),
        }
    };
    let permanent_failure_retried = loop {
        match next_event(&failed).await {
            StreamConsumerEvent::ResyncRequired { reason }
                if reason.contains("scalable member is busy") =>
            {
                break true;
            }
            StreamConsumerEvent::ResyncRequired { .. }
            | StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected replacement-registration event: {unexpected:?}"),
        }
    };
    assert_eq!(
        failed.status().phase(),
        magnetar::proto::AggregatePhase::ResyncRequired
    );
    let permanent_failure_attempts = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .filter(|route| {
                route.endpoint == Endpoint::Segment(1)
                    && route.command == magnetar::proto::pb::base_command::Type::Subscribe
            })
            .count()
            - opens_before_permanent_failure
    });
    assert_eq!(permanent_failure_attempts, 1);
    close_and_count(failed, cluster).await;

    withdraw_failed_child_open(
        client,
        cluster,
        "busy",
        BrokerFailure::new(
            magnetar::proto::pb::ServerError::ConsumerBusy,
            "withdrawn busy child-open retry",
        ),
    )
    .await;
    withdraw_failed_child_open(
        client,
        cluster,
        "failure",
        BrokerFailure::new(
            magnetar::proto::pb::ServerError::PersistenceError,
            "withdrawn failed child-open retry",
        ),
    )
    .await;

    cluster.hold_command(
        Endpoint::Segment(1),
        magnetar::proto::pb::base_command::Type::Success,
    );
    let cancelled = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("cancel-open-sub")
        .consumer_name("cancel-open-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .subscribe()
        .await
        .expect("subscribe cancellation aggregate");
    cluster
        .wait_for("held child-open response", |fake| {
            fake.resource_counts().child_consumers == 2
                && fake.active_child_owner("cancel-open-sub", 1).is_some()
        })
        .await;
    let member = cluster
        .inspect(|fake| fake.member("cancel-open-sub", "cancel-open-member"))
        .expect("cancellation aggregate member");
    let takeover = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("cancel-open-sub")
        .consumer_name("cancel-open-takeover")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .subscribe()
        .await
        .expect("subscribe takeover member");
    let takeover_member = cluster
        .inspect(|fake| fake.member("cancel-open-sub", "cancel-open-takeover"))
        .expect("takeover aggregate member");
    cluster
        .update(|fake| {
            fake.publish_assignment_plan(
                1,
                vec![
                    FullAssignment::new(member, [2]),
                    FullAssignment::new(takeover_member, [1]),
                ],
            )
        })
        .expect("withdraw held segment ownership");
    loop {
        match next_event(&cancelled).await {
            StreamConsumerEvent::AssignmentApplied { sources, .. }
                if sources.iter().map(|source| source.segment_id().0).eq([2]) =>
            {
                break;
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected cancelled-open event: {unexpected:?}"),
        }
    }
    cluster.release_command(
        Endpoint::Segment(1),
        magnetar::proto::pb::base_command::Type::Success,
    );
    cluster
        .wait_for("withdrawn child is closed without attachment", |fake| {
            fake.resource_counts().pending_operations == 0
                && fake.resource_counts().child_consumers == 2
                && fake.active_child_owner("cancel-open-sub", 1) == Some(takeover_member)
                && fake.active_child_owner("cancel-open-sub", 2) == Some(member)
        })
        .await;
    let cancelled_open_removed = true;
    cancelled
        .close()
        .await
        .expect("close cancellation aggregate");
    takeover.close().await.expect("close takeover aggregate");

    cluster.hold_command(
        Endpoint::Segment(1),
        magnetar::proto::pb::base_command::Type::Success,
    );
    let failing = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("failed-cancel-open-sub")
        .consumer_name("failed-cancel-open-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .subscribe()
        .await
        .expect("subscribe failed-cancellation aggregate");
    cluster
        .wait_for("held failed-cancellation child-open response", |fake| {
            fake.resource_counts().child_consumers == 2
                && fake
                    .active_child_owner("failed-cancel-open-sub", 1)
                    .is_some()
        })
        .await;
    let failing_member = cluster
        .inspect(|fake| fake.member("failed-cancel-open-sub", "failed-cancel-open-member"))
        .expect("failed-cancellation aggregate member");
    let failing_takeover = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("failed-cancel-open-sub")
        .consumer_name("failed-cancel-open-takeover")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .subscribe()
        .await
        .expect("subscribe failed-cancellation takeover");
    let failing_takeover_member = cluster
        .inspect(|fake| fake.member("failed-cancel-open-sub", "failed-cancel-open-takeover"))
        .expect("failed-cancellation takeover member");
    cluster
        .update(|fake| {
            fake.publish_assignment_plan(
                1,
                vec![
                    FullAssignment::new(failing_member, [2]),
                    FullAssignment::new(failing_takeover_member, [1]),
                ],
            )
        })
        .expect("withdraw the failing provisional child");
    loop {
        match next_event(&failing).await {
            StreamConsumerEvent::AssignmentApplied { sources, .. }
                if sources.iter().map(|source| source.segment_id().0).eq([2]) =>
            {
                break;
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected failed-cancellation event: {unexpected:?}"),
        }
    }
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::Close,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "cancelled child-open close failure",
                )),
            )
        })
        .expect("fail the cancelled child's compensating close");
    cluster.release_command(
        Endpoint::Segment(1),
        magnetar::proto::pb::base_command::Type::Success,
    );
    let cancelled_open_close_failed = loop {
        match next_event(&failing).await {
            StreamConsumerEvent::ResyncRequired { reason } => {
                assert!(
                    reason.contains("cancelled child-open close failure"),
                    "unexpected failed-cancellation resync reason: {reason}"
                );
                break true;
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected failed-close event: {unexpected:?}"),
        }
    };
    failing
        .close()
        .await
        .expect("close failed-cancellation aggregate after resync request");
    failing_takeover
        .close()
        .await
        .expect("close failed-cancellation takeover");
    cluster
        .wait_for("child-open lifecycle cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0
                && counts.layout_sessions == 0
                && counts.pending_operations == 0
                && counts.permits == 0
        })
        .await;

    cluster.hold_command(
        Endpoint::Segment(1),
        magnetar::proto::pb::base_command::Type::Success,
    );
    let provisional = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("provisional-close-sub")
        .consumer_name("provisional-close-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .subscribe()
        .await
        .expect("subscribe provisional-close aggregate");
    cluster
        .wait_for("held provisional child-open response", |fake| {
            fake.resource_counts().child_consumers == 2
                && fake
                    .active_child_owner("provisional-close-sub", 1)
                    .is_some()
        })
        .await;
    let close_routes_before = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .filter(|route| {
                route.endpoint == Endpoint::Segment(1)
                    && route.command == magnetar::proto::pb::base_command::Type::CloseConsumer
            })
            .count()
    });
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::Close,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay provisional child's compensating close");
    let repeated_provisional = provisional.clone();
    let mut first_close = Box::pin(provisional.close());
    let mut second_close = Box::pin(repeated_provisional.close());
    cluster.release_command(
        Endpoint::Segment(1),
        magnetar::proto::pb::base_command::Type::Success,
    );
    tokio::select! {
        biased;
        result = &mut first_close => panic!("first provisional close completed before child confirmation: {result:?}"),
        result = &mut second_close => panic!("second provisional close completed before child confirmation: {result:?}"),
        () = cluster.wait_for("pending provisional child close", |fake| {
            fake.pending_operations().iter().any(|pending| {
                pending.endpoint == Endpoint::Segment(1)
                    && pending.kind == OperationKind::Close
            })
        }) => {}
    }
    let pending_close = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| {
                    pending.endpoint == Endpoint::Segment(1) && pending.kind == OperationKind::Close
                })
                .map(|pending| pending.id)
        })
        .expect("pending provisional child close id");
    cluster
        .update(|fake| {
            fake.complete_pending(
                pending_close,
                PendingCompletion::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "provisional close barrier failure",
                )),
            )
        })
        .expect("fail provisional child's compensating close");
    let (first_result, second_result) = tokio::join!(&mut first_close, &mut second_close);
    let provisional_close_errors = [
        first_result
            .expect_err("first provisional close fails")
            .to_string(),
        second_result
            .expect_err("second provisional close repeats failure")
            .to_string(),
    ];
    assert!(
        provisional_close_errors
            .iter()
            .all(|error| error.contains("provisional close barrier failure"))
    );
    assert_eq!(
        provisional_close_errors
            .iter()
            .filter(|error| error.contains("stream consumer failed"))
            .count(),
        1
    );
    let provisional_close_failures = provisional_close_errors.len();
    cluster
        .wait_for("provisional-close lifecycle cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0
                && counts.layout_sessions == 0
                && counts.pending_operations == 0
                && counts.permits == 0
        })
        .await;
    let provisional_close_routes = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .filter(|route| {
                route.endpoint == Endpoint::Segment(1)
                    && route.command == magnetar::proto::pb::base_command::Type::CloseConsumer
            })
            .count()
            - close_routes_before
    });
    assert_eq!(provisional_close_routes, 2);

    let after_close = cluster.inspect(M1FakeCluster::resource_counts);
    ChildOpenTrace {
        busy_segment_attempts,
        permanent_failure_resynced,
        permanent_failure_retried,
        permanent_failure_attempts,
        cancelled_open_removed,
        cancelled_open_close_failed,
        provisional_close_failures,
        provisional_close_routes,
        after_close,
    }
}

async fn run_tokio_child_open_lifecycle() -> ChildOpenTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_child_open_lifecycle(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_child_open_lifecycle() -> ChildOpenTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_child_open_lifecycle(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_close_states<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> CloseStateTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("concurrent-close-sub")
        .consumer_name("concurrent-close-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe concurrent-close aggregate");
    cluster
        .wait_for("concurrent-close children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;
    let second_close = consumer.clone();
    let observer = consumer.clone();
    cluster
        .update(|fake| {
            for endpoint in [Endpoint::Segment(1), Endpoint::Segment(2)] {
                fake.script_next(endpoint, OperationKind::Close, ScriptedBehavior::Delay)?;
            }
            Ok(())
        })
        .expect("delay both child closes");
    let mut first = Box::pin(consumer.close());
    let mut second = Box::pin(second_close.close());
    for _ in 0..2 {
        tokio::select! {
            biased;
            result = &mut first => panic!("first aggregate close completed before confirmations: {result:?}"),
            result = &mut second => panic!("second aggregate close completed before confirmations: {result:?}"),
            () = cluster.wait_for("pending child close", |fake| {
                fake.pending_operations().iter().any(|pending| pending.kind == OperationKind::Close)
            }) => {}
        }
        let pending = cluster
            .inspect(|fake| {
                fake.pending_operations()
                    .into_iter()
                    .find(|pending| pending.kind == OperationKind::Close)
                    .map(|pending| pending.id)
            })
            .expect("pending child-close operation id");
        cluster
            .update(|fake| fake.complete_pending(pending, PendingCompletion::Succeed))
            .expect("confirm delayed child close");
    }
    let (first_result, second_result) = tokio::join!(&mut first, &mut second);
    first_result.expect("first concurrent close succeeds");
    second_result.expect("second concurrent close succeeds");
    let concurrent_close_succeeded = true;
    let receive_closed = matches!(
        observer.receive().await,
        Err(magnetar::scalable::StreamConsumerError::Engine { message, .. })
            if message.contains("closed")
    );
    assert!(receive_closed);
    let event_stream_closed = loop {
        match observer
            .next_event()
            .await
            .expect("observe closed event stream")
        {
            Some(StreamConsumerEvent::Closed) => {
                break observer
                    .next_event()
                    .await
                    .expect("closed event stream remains readable")
                    .is_none();
            }
            Some(_) => {}
            None => break true,
        }
    };
    assert!(event_stream_closed);

    let failing = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("failed-close-sub")
        .consumer_name("failed-close-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe failed-close aggregate");
    cluster
        .wait_for("failed-close children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&failing, &[1, 2]).await;
    let repeated = failing.clone();
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::Close,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "persist close failure",
                )),
            )
        })
        .expect("fail one child close");
    assert!(failing.close().await.is_err());
    let repeated_failure = repeated.close().await.is_err();
    assert!(repeated_failure);
    cluster
        .wait_for("close-state cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0
                && counts.layout_sessions == 0
                && counts.pending_operations == 0
                && counts.permits == 0
        })
        .await;
    CloseStateTrace {
        concurrent_close_succeeded,
        receive_closed,
        event_stream_closed,
        repeated_failure,
        after_close: cluster.inspect(M1FakeCluster::resource_counts),
    }
}

async fn run_tokio_close_states() -> CloseStateTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_close_states(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_close_states() -> CloseStateTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_close_states(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_malformed_delivery<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> MalformedDeliveryTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let cases = [
        (
            "decompression",
            magnetar::proto::pb::MessageMetadata {
                compression: Some(magnetar::proto::pb::CompressionType::Snappy as i32),
                uncompressed_size: Some(4),
                ..Default::default()
            },
            snappy(b"codec"),
            "decompress",
            true,
        ),
        (
            "chunk",
            magnetar::proto::pb::MessageMetadata {
                num_chunks_from_msg: Some(2),
                total_chunk_msg_size: Some(1),
                uuid: Some("malformed-chunk".to_owned()),
                ..Default::default()
            },
            Bytes::from_static(b"x"),
            "chunk id is absent",
            false,
        ),
        (
            "transform-budget",
            magnetar::proto::pb::MessageMetadata {
                compression: Some(magnetar::proto::pb::CompressionType::Snappy as i32),
                uncompressed_size: Some(u32::MAX),
                ..Default::default()
            },
            snappy(b"x"),
            "data budget is",
            false,
        ),
        (
            "batch",
            magnetar::proto::pb::MessageMetadata {
                num_messages_in_batch: Some(2),
                ..Default::default()
            },
            Bytes::new(),
            "single-message metadata length is truncated",
            false,
        ),
    ];
    let mut resync_reasons = Vec::with_capacity(cases.len());
    let mut close_routes = Vec::with_capacity(cases.len());
    let mut after_close = Vec::with_capacity(cases.len());
    for (label, metadata, payload, reason_fragment, fail_child_close) in cases {
        let consumer = client
            .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
            .subscription(format!("malformed-{label}-sub"))
            .consumer_name(format!("malformed-{label}-member"))
            .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
            .receiver_budget(two_frame_receiver_budget())
            .subscribe()
            .await
            .expect("subscribe malformed-delivery aggregate");
        cluster
            .wait_for("malformed-delivery children", |fake| {
                fake.resource_counts().child_consumers == 2
            })
            .await;
        wait_for_initial_flow(&consumer, &[1, 2]).await;
        let closes_before = cluster.inspect(|fake| {
            fake.routes()
                .iter()
                .filter(|route| {
                    route.endpoint == Endpoint::Segment(1)
                        && route.command == magnetar::proto::pb::base_command::Type::CloseConsumer
                })
                .count()
        });
        cluster
            .update(|fake| {
                if fail_child_close {
                    fake.script_next(
                        Endpoint::Segment(1),
                        OperationKind::Close,
                        ScriptedBehavior::Fail(BrokerFailure::new(
                            magnetar::proto::pb::ServerError::PersistenceError,
                            "resync child close rejected",
                        )),
                    )?;
                }
                fake.enqueue_message_with_metadata(1, metadata, payload, Vec::new())
            })
            .expect("enqueue malformed delivery");
        cluster
            .wait_for("malformed delivery reaches child", |fake| {
                fake.resource_counts().unacked_messages == 1
            })
            .await;
        let reason = loop {
            match next_event(&consumer).await {
                StreamConsumerEvent::ResyncRequired { reason } => break reason,
                StreamConsumerEvent::AssignmentApplied { .. }
                | StreamConsumerEvent::SegmentPhaseChanged { .. }
                | StreamConsumerEvent::OrderingUnprovable { .. }
                | StreamConsumerEvent::TransactionOutcome { .. } => {}
                unexpected => panic!("unexpected malformed-delivery event: {unexpected:?}"),
            }
        };
        assert!(
            reason.contains(reason_fragment),
            "unexpected {label} resync reason: {reason}"
        );
        assert_eq!(
            consumer.status().phase(),
            magnetar::proto::AggregatePhase::ResyncRequired
        );
        resync_reasons.push(reason);
        after_close.push(close_and_count(consumer, cluster).await);
        let closes = cluster.inspect(|fake| {
            fake.routes()
                .iter()
                .filter(|route| {
                    route.endpoint == Endpoint::Segment(1)
                        && route.command == magnetar::proto::pb::base_command::Type::CloseConsumer
                })
                .count()
                .saturating_sub(closes_before)
        });
        assert_eq!(closes, if fail_child_close { 2 } else { 1 });
        close_routes.push(closes);
    }
    MalformedDeliveryTrace {
        resync_reasons,
        close_routes,
        after_close,
    }
}

async fn run_tokio_malformed_delivery() -> MalformedDeliveryTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_malformed_delivery(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_malformed_delivery() -> MalformedDeliveryTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_malformed_delivery(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_dag_watch_recovery<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> DagWatchRecoveryTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("dag-watch-recovery-sub")
        .consumer_name("dag-watch-recovery-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe DAG-watch recovery aggregate");
    cluster
        .wait_for("DAG-watch recovery children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;
    let (connection, session_id) = cluster
        .inspect(M1FakeCluster::layout_session_ids)
        .into_iter()
        .next()
        .expect("live DAG watch");
    cluster
        .update(|fake| {
            fake.fail_layout_session(
                connection,
                session_id,
                BrokerFailure::new(
                    magnetar::proto::pb::ServerError::ServiceNotReady,
                    "replace failed DAG watch",
                ),
            )
        })
        .expect("fail live DAG watch");
    let mut watch_failure_reported = false;
    let baseline_reported = loop {
        match next_event(&consumer).await {
            StreamConsumerEvent::ResyncRequired { reason } => {
                watch_failure_reported = reason.contains("replace failed DAG watch");
            }
            StreamConsumerEvent::AssignmentApplied { sources, .. } if watch_failure_reported => {
                assert_eq!(
                    sources
                        .iter()
                        .map(|source| source.segment_id().0)
                        .collect::<Vec<_>>(),
                    vec![1, 2]
                );
                break true;
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected DAG-watch recovery event: {unexpected:?}"),
        }
    };
    assert!(watch_failure_reported);
    assert!(baseline_reported);
    cluster
        .wait_for("DAG-watch replacement", |fake| {
            fake.resource_counts().child_consumers == 2
                && fake.resource_counts().pending_operations == 0
                && fake
                    .layout_session_ids()
                    .iter()
                    .any(|(_, replacement)| *replacement != session_id)
        })
        .await;
    let (replacement_connection, replacement_session) = cluster
        .inspect(M1FakeCluster::layout_session_ids)
        .into_iter()
        .find(|(_, replacement)| *replacement != session_id)
        .expect("replacement DAG-watch session");
    let controller_opens = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .filter(|route| {
                route.command == magnetar::proto::pb::base_command::Type::ScalableTopicSubscribe
            })
            .count()
    });
    assert_eq!(controller_opens, 1);
    cluster
        .update(|fake| {
            fake.fail_layout_session(
                replacement_connection,
                replacement_session,
                BrokerFailure::new(
                    magnetar::proto::pb::ServerError::ServiceNotReady,
                    "terminal replacement DAG watch",
                ),
            )
        })
        .expect("fail replacement DAG watch");
    let terminal_watch_failure_reported = loop {
        match next_event(&consumer).await {
            StreamConsumerEvent::ResyncRequired { reason }
                if reason.contains("terminal replacement DAG watch") =>
            {
                break true;
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. }
            | StreamConsumerEvent::ResyncRequired { .. } => {}
            unexpected => panic!("unexpected replacement DAG-watch event: {unexpected:?}"),
        }
    };
    cluster
        .update(|fake| fake.disconnect_connection(replacement_connection))
        .expect("disconnect replacement DAG-watch controller");
    let mut terminal_reopen_failed = false;
    let mut replacement_assignment_after_terminal = false;
    while !terminal_reopen_failed {
        match tokio::time::timeout(magnetar_differential::HANG_GUARD, consumer.next_event())
            .await
            .expect("terminal DAG-watch event timed out")
        {
            Ok(Some(StreamConsumerEvent::ResyncRequired { reason })) => {
                terminal_reopen_failed |= reason.contains("closed") || reason.contains("driver");
            }
            Ok(Some(StreamConsumerEvent::AssignmentApplied { .. })) => {
                replacement_assignment_after_terminal = true;
            }
            Ok(Some(
                StreamConsumerEvent::SegmentPhaseChanged { .. }
                | StreamConsumerEvent::OrderingUnprovable { .. }
                | StreamConsumerEvent::TransactionOutcome { .. },
            )) => {}
            Ok(Some(unexpected)) => {
                panic!("unexpected terminal DAG-watch event: {unexpected:?}");
            }
            Ok(None) => terminal_reopen_failed = true,
            Err(error) => {
                let error = error.to_string();
                assert!(error.contains("closed") || error.contains("driver"));
                terminal_reopen_failed = true;
            }
        }
    }
    assert!(!replacement_assignment_after_terminal);
    let after_close = close_and_count(consumer, cluster).await;
    DagWatchRecoveryTrace {
        watch_failure_reported,
        baseline_reported,
        replacement_session,
        controller_opens,
        terminal_watch_failure_reported,
        terminal_reopen_failed,
        replacement_assignment_after_terminal,
        after_close,
    }
}

async fn run_tokio_dag_watch_recovery() -> DagWatchRecoveryTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio_with_terminal_reconnect_budget(&cluster).await;
    let trace = observe_dag_watch_recovery(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_dag_watch_recovery() -> DagWatchRecoveryTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool_with_terminal_reconnect_budget(&cluster).await;
    let trace = observe_dag_watch_recovery(&client, &cluster).await;
    client.close().await;
    trace
}

async fn close_during_transactional_ack<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
    suffix: &str,
    completion: PendingCompletion,
) -> String
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi + TransactionApi,
{
    let subscription = format!("closing-transactional-ack-{suffix}");
    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription(subscription)
        .consumer_name(format!("closing-transactional-ack-{suffix}-member"))
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe closing-transactional-ack aggregate");
    cluster
        .wait_for("closing-transactional-ack children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;
    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from(suffix.to_owned())))
        .expect("enqueue closing transactional acknowledgement");
    cluster
        .wait_for("closing transactional acknowledgement delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let message = receive(&consumer).await;
    let transaction = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open closing-ack transaction");
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::Ack,
                ScriptedBehavior::Delay,
            )?;
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::Close,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay transactional acknowledgement and close");
    let mut acknowledgement = Box::pin(consumer.acknowledge_in_transaction(&message, transaction));
    tokio::select! {
        biased;
        result = &mut acknowledgement => panic!("transactional acknowledgement completed before delay: {result:?}"),
        () = cluster.wait_for("pending transactional acknowledgement", |fake| {
            fake.pending_operations().iter().any(|pending| pending.kind == OperationKind::Ack)
        }) => {}
    }
    let pending_ack = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::Ack)
                .map(|pending| pending.id)
        })
        .expect("pending transactional acknowledgement id");
    let mut close = Box::pin(consumer.clone().close());
    tokio::select! {
        biased;
        result = &mut close => panic!("aggregate close completed before child confirmation: {result:?}"),
        () = cluster.wait_for("pending close during transactional acknowledgement", |fake| {
            fake.pending_operations().iter().any(|pending| pending.kind == OperationKind::Close)
        }) => {}
    }
    let pending_close = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::Close)
                .map(|pending| pending.id)
        })
        .expect("pending close during transactional acknowledgement id");
    cluster
        .update(|fake| fake.complete_pending(pending_ack, completion))
        .expect("settle delayed transactional acknowledgement");
    let error = engine_error_kind(
        acknowledgement
            .await
            .expect_err("closing aggregate rejects transactional acknowledgement"),
    );
    client
        .abort_transaction(transaction)
        .await
        .expect("abort closing-ack transaction");
    cluster
        .update(|fake| fake.complete_pending(pending_close, PendingCompletion::Succeed))
        .expect("confirm delayed aggregate child close");
    close.await.expect("close transactional-ack aggregate");
    cluster
        .wait_for("closing transactional acknowledgement cleanup", |fake| {
            fake.resource_counts().child_consumers == 0
                && fake.resource_counts().pending_operations == 0
        })
        .await;
    error
}

async fn close_wakes_shared_transaction_registration_waiter<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> SharedRegistrationCloseTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi + TransactionApi,
{
    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("closing-shared-registration-sub")
        .consumer_name("closing-shared-registration-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe closing shared-registration aggregate");
    cluster
        .wait_for("closing shared-registration children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;
    for payload in ["shared-close-leader", "shared-close-waiter"] {
        cluster
            .update(|fake| fake.enqueue_message(1, Bytes::from_static(payload.as_bytes())))
            .expect("enqueue closing shared-registration delivery");
    }
    cluster
        .wait_for("closing shared-registration deliveries", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let messages = [receive(&consumer).await, receive(&consumer).await];
    let transaction = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open closing shared-registration transaction");
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.script_next(
                Endpoint::Controller,
                OperationKind::TransactionRegistration,
                ScriptedBehavior::Delay,
            )?;
            for segment in [1, 2] {
                fake.script_next(
                    Endpoint::Segment(segment),
                    OperationKind::Close,
                    ScriptedBehavior::Delay,
                )?;
            }
            Ok(())
        })
        .expect("delay shared registration and child closes");
    let mut leader = Box::pin(consumer.acknowledge_in_transaction(&messages[0], transaction));
    tokio::select! {
        biased;
        result = &mut leader => panic!("shared-registration leader completed early: {result:?}"),
        () = cluster.wait_for("pending closing shared registration", |fake| {
            fake.pending_operations().iter().any(|pending| {
                pending.kind == OperationKind::TransactionRegistration
            })
        }) => {}
    }
    let registration = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::TransactionRegistration)
                .map(|pending| pending.id)
        })
        .expect("pending closing shared-registration id");
    let mut waiter = Box::pin(consumer.acknowledge_in_transaction(&messages[1], transaction));
    tokio::select! {
        biased;
        result = &mut waiter => panic!("shared-registration waiter completed early: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    let one_registration =
        cluster.inspect(|fake| transaction_registration_command_count(fake) == 1);
    assert!(one_registration);
    assert_eq!(
        cluster.inspect(|fake| {
            fake.routes()
                .iter()
                .filter(|route| route.command == magnetar::proto::pb::base_command::Type::Ack)
                .count()
        }),
        0
    );
    let mut close = Box::pin(consumer.clone().close());
    tokio::select! {
        biased;
        result = &mut close => panic!("shared-registration close completed early: {result:?}"),
        () = cluster.wait_for("first closing shared-registration child close", |fake| {
            fake.pending_operations().iter().any(|pending| pending.kind == OperationKind::Close)
        }) => {}
    }
    let waiter_closed_before_registration_completion = match waiter
        .await
        .expect_err("aggregate close wakes shared-registration waiter")
    {
        magnetar::scalable::StreamConsumerError::Engine { message, .. } => {
            assert!(message.contains("closed"));
            true
        }
        unexpected => panic!("unexpected shared-registration waiter error: {unexpected:?}"),
    };
    assert!(cluster.inspect(|fake| {
        fake.pending_operations()
            .iter()
            .any(|pending| pending.id == registration)
    }));
    cluster
        .update(|fake| fake.complete_pending(registration, PendingCompletion::Succeed))
        .expect("complete shared registration after aggregate close");
    let leader_closed_after_registration_completion = match leader
        .await
        .expect_err("aggregate close fences shared-registration leader")
    {
        magnetar::scalable::StreamConsumerError::Engine { message, .. } => {
            assert!(message.contains("closed"));
            true
        }
        unexpected => panic!("unexpected shared-registration leader error: {unexpected:?}"),
    };
    assert_eq!(
        client
            .abort_transaction(transaction)
            .await
            .expect("abort closing shared-registration transaction"),
        magnetar::TxnState::Aborted
    );
    for _ in 0..2 {
        tokio::select! {
            biased;
            result = &mut close => panic!("shared-registration close completed before both child confirmations: {result:?}"),
            () = cluster.wait_for("pending closing shared-registration child close", |fake| {
                fake.pending_operations()
                    .iter()
                    .any(|pending| pending.kind == OperationKind::Close)
            }) => {}
        }
        let pending_close = cluster
            .inspect(|fake| {
                fake.pending_operations()
                    .into_iter()
                    .find(|pending| pending.kind == OperationKind::Close)
                    .map(|pending| pending.id)
            })
            .expect("pending closing shared-registration child close id");
        cluster
            .update(|fake| fake.complete_pending(pending_close, PendingCompletion::Succeed))
            .expect("complete closing shared-registration child close");
    }
    close.await.expect("close shared-registration aggregate");
    cluster
        .wait_for("closing shared-registration cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0
                && counts.pending_operations == 0
                && counts.unacked_messages == 0
        })
        .await;
    let no_transactional_acks = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .all(|route| route.command != magnetar::proto::pb::base_command::Type::Ack)
    });
    assert!(no_transactional_acks);
    SharedRegistrationCloseTrace {
        one_registration,
        no_transactional_acks,
        waiter_closed_before_registration_completion,
        leader_closed_after_registration_completion,
    }
}

async fn close_during_ordinary_ack<E>(client: &PulsarClient<E>, cluster: &M1SocketCluster) -> bool
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("closing-ordinary-ack-sub")
        .consumer_name("closing-ordinary-ack-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe closing ordinary-ack aggregate");
    cluster
        .wait_for("closing ordinary-ack children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;
    for segment in [1, 2] {
        cluster
            .update(|fake| {
                fake.enqueue_message(segment, Bytes::from(format!("closing-ack-{segment}")))
            })
            .expect("enqueue closing ordinary acknowledgement component");
    }
    cluster
        .wait_for("closing ordinary acknowledgement deliveries", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let mut messages = vec![receive(&consumer).await, receive(&consumer).await];
    messages.sort_by_key(|message| message.source().segment_id().0);
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::Ack,
                ScriptedBehavior::Delay,
            )?;
            for segment in [1, 2] {
                fake.script_next(
                    Endpoint::Segment(segment),
                    OperationKind::Close,
                    ScriptedBehavior::Delay,
                )?;
            }
            Ok(())
        })
        .expect("delay ordinary acknowledgement and child closes");
    let mut acknowledgement = Box::pin(consumer.acknowledge_batch(&messages));
    tokio::select! {
        biased;
        result = &mut acknowledgement => panic!("ordinary acknowledgement completed before delay: {result:?}"),
        () = cluster.wait_for("pending ordinary acknowledgement", |fake| {
            fake.pending_operations().iter().any(|pending| pending.kind == OperationKind::Ack)
        }) => {}
    }
    let pending_ack = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::Ack)
                .map(|pending| pending.id)
        })
        .expect("pending ordinary acknowledgement id");
    cluster.hold_command(
        Endpoint::Segment(1),
        magnetar::proto::pb::base_command::Type::AckResponse,
    );
    cluster
        .update(|fake| fake.complete_pending(pending_ack, PendingCompletion::Succeed))
        .expect("settle delayed ordinary acknowledgement behind the wire");
    let mut close = Box::pin(consumer.clone().close());
    tokio::select! {
        biased;
        result = &mut close => panic!("aggregate close completed before child confirmations: {result:?}"),
        () = cluster.wait_for("first pending close during ordinary acknowledgement", |fake| {
            fake.pending_operations().iter().any(|pending| pending.kind == OperationKind::Close)
        }) => {}
    }
    let first_close = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::Close)
                .map(|pending| pending.id)
        })
        .expect("first pending ordinary-ack child close");
    cluster.release_command(
        Endpoint::Segment(1),
        magnetar::proto::pb::base_command::Type::AckResponse,
    );
    let fenced = match acknowledgement
        .await
        .expect_err("closing aggregate fences ordinary acknowledgement settlement")
    {
        magnetar::scalable::StreamConsumerError::Model(
            magnetar::proto::StreamConsumerModelError::InvalidAggregatePhase {
                phase: magnetar::proto::AggregatePhase::Closing,
            },
        ) => true,
        unexpected => panic!("unexpected closing ordinary acknowledgement error: {unexpected:?}"),
    };
    assert!(fenced);
    cluster
        .update(|fake| fake.complete_pending(first_close, PendingCompletion::Succeed))
        .expect("confirm first ordinary-ack child close");
    tokio::select! {
        biased;
        result = &mut close => panic!("aggregate close completed before second child confirmation: {result:?}"),
        () = cluster.wait_for("second pending close during ordinary acknowledgement", |fake| {
            fake.pending_operations().iter().any(|pending| pending.kind == OperationKind::Close)
        }) => {}
    }
    let second_close = cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::Close)
                .map(|pending| pending.id)
        })
        .expect("second pending ordinary-ack child close");
    cluster
        .update(|fake| fake.complete_pending(second_close, PendingCompletion::Succeed))
        .expect("confirm second ordinary-ack child close");
    close.await.expect("close ordinary-ack aggregate");
    cluster
        .wait_for("closing ordinary acknowledgement cleanup", |fake| {
            fake.resource_counts().child_consumers == 0
                && fake.resource_counts().pending_operations == 0
        })
        .await;
    fenced
}

async fn disconnect_during_transaction_registration<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> (String, bool)
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi + TransactionApi,
{
    const SUBSCRIPTION: &str = "registration-disconnect-sub";
    const MEMBER: &str = "registration-disconnect-member";
    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription(SUBSCRIPTION)
        .consumer_name(MEMBER)
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe registration-disconnect aggregate");
    cluster
        .wait_for("registration-disconnect children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;
    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"registration-disconnect")))
        .expect("enqueue registration-disconnect delivery");
    cluster
        .wait_for("registration-disconnect delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let message = receive(&consumer).await;
    let transaction = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open registration-disconnect transaction");
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Controller,
                OperationKind::TransactionRegistration,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay transaction registration before disconnect");
    let mut acknowledgement = Box::pin(consumer.acknowledge_in_transaction(&message, transaction));
    tokio::select! {
        biased;
        result = &mut acknowledgement => panic!("transaction registration completed before disconnect: {result:?}"),
        () = cluster.wait_for("pending transaction registration before disconnect", |fake| {
            fake.pending_operations().iter().any(|pending| {
                pending.kind == OperationKind::TransactionRegistration
            })
        }) => {}
    }
    let old_controller = cluster
        .inspect(|fake| fake.member(SUBSCRIPTION, MEMBER))
        .expect("registration-disconnect member")
        .connection;
    cluster
        .update(|fake| fake.disconnect_connection(old_controller))
        .expect("disconnect controller during transaction registration");
    let registration_disconnect_error = match acknowledgement
        .await
        .expect_err("controller disconnect rejects transaction registration")
    {
        magnetar::scalable::StreamConsumerError::Engine { .. }
        | magnetar::scalable::StreamConsumerError::Model(
            magnetar::proto::StreamConsumerModelError::StaleAcknowledgementAuthority
            | magnetar::proto::StreamConsumerModelError::InvalidAggregatePhase {
                phase: magnetar::proto::AggregatePhase::ResyncRequired,
            },
        ) => "registration-disconnected".to_owned(),
        unexpected => panic!("unexpected registration-disconnect error: {unexpected:?}"),
    };
    let mut saw_resync = false;
    let registration_disconnect_recovered = loop {
        match next_event(&consumer).await {
            StreamConsumerEvent::ResyncRequired { .. } => saw_resync = true,
            StreamConsumerEvent::AssignmentApplied { sources, .. } if saw_resync => {
                assert_eq!(
                    sources
                        .iter()
                        .map(|source| source.segment_id().0)
                        .collect::<Vec<_>>(),
                    vec![1, 2]
                );
                break true;
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected registration-disconnect event: {unexpected:?}"),
        }
    };
    assert_eq!(registration_disconnect_error, "registration-disconnected");
    assert!(registration_disconnect_recovered);
    cluster
        .wait_for("registration-disconnect recovery", |fake| {
            fake.resource_counts().child_consumers == 2
                && fake.resource_counts().pending_operations == 0
                && fake
                    .member(SUBSCRIPTION, MEMBER)
                    .is_some_and(|member| member.connection != old_controller)
        })
        .await;
    client
        .abort_transaction(transaction)
        .await
        .expect("abort transaction after registration disconnect");
    assert_eq!(
        stale_token_kind(
            consumer
                .acknowledge(&message)
                .await
                .expect_err("registration disconnect invalidates the old delivery lease"),
        ),
        "stale-token"
    );
    close_and_count(consumer, cluster).await;
    (
        registration_disconnect_error,
        registration_disconnect_recovered,
    )
}

async fn observe_acknowledgement_failures<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> AcknowledgementFailureTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi + TransactionApi,
{
    let (registration_disconnect_error, registration_disconnect_recovered) =
        disconnect_during_transaction_registration(client, cluster).await;
    let close_during_ack_fenced = close_during_ordinary_ack(client, cluster).await;
    let consumer = client
        .scalable_stream_consumer(TOPIC, Arc::new(BytesSchema::new()))
        .subscription("partial-ack-sub")
        .consumer_name("partial-ack-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe partial-ack aggregate");
    cluster
        .wait_for("partial-ack children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer, &[1, 2]).await;
    for segment in [1, 2] {
        cluster
            .update(|fake| fake.enqueue_message(segment, Bytes::from(format!("partial-{segment}"))))
            .expect("enqueue partial acknowledgement component");
    }
    cluster
        .wait_for("partial acknowledgement deliveries", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let mut messages = vec![receive(&consumer).await, receive(&consumer).await];
    messages.sort_by_key(|message| message.source().segment_id().0);
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::Ack,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "fail one aggregate acknowledgement component",
                )),
            )
        })
        .expect("fail one acknowledgement component");
    let (partial_confirmed, partial_failed) = match consumer
        .acknowledge_batch(&messages)
        .await
        .expect_err("cross-segment acknowledgement partially fails")
    {
        magnetar::scalable::StreamConsumerError::PartialAcknowledgement { confirmed, failed } => {
            (confirmed.len(), failed.len())
        }
        unexpected => panic!("unexpected partial acknowledgement error: {unexpected:?}"),
    };
    assert_eq!((partial_confirmed, partial_failed), (1, 1));
    consumer
        .acknowledge(&messages[0])
        .await
        .expect("retry failed acknowledgement component");
    cluster
        .wait_for("partial acknowledgement retry", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;
    close_and_count(consumer, cluster).await;

    let close_after_ack_success =
        close_during_transactional_ack(client, cluster, "success", PendingCompletion::Succeed)
            .await;
    let close_after_ack_failure = close_during_transactional_ack(
        client,
        cluster,
        "failure",
        PendingCompletion::Fail(BrokerFailure::new(
            magnetar::proto::pb::ServerError::PersistenceError,
            "fail acknowledgement while closing",
        )),
    )
    .await;
    let shared_registration_close =
        close_wakes_shared_transaction_registration_waiter(client, cluster).await;
    AcknowledgementFailureTrace {
        partial_confirmed,
        partial_failed,
        registration_disconnect_error,
        registration_disconnect_recovered,
        close_during_ack_fenced,
        close_after_ack_success,
        close_after_ack_failure,
        shared_registration_close,
        after_close: cluster.inspect(M1FakeCluster::resource_counts),
    }
}

async fn run_tokio_acknowledgement_failures() -> AcknowledgementFailureTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_acknowledgement_failures(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_acknowledgement_failures() -> AcknowledgementFailureTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_acknowledgement_failures(&client, &cluster).await;
    client.close().await;
    trace
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_local_ancestry_waits_for_every_parent_and_merge_barrier() {
    let _serial = advanced_socket_test_guard();
    let tokio_trace = run_tokio_local_ancestry().await;
    let moonpool_trace = run_moonpool_local_ancestry().await;
    assert_eq!(tokio_trace, moonpool_trace);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_m1_sealed_assignment_drains_without_parent_reopen() {
    let _serial = advanced_socket_test_guard();
    let tokio_trace = run_tokio_exact_m1_sealed_placement().await;
    let moonpool_trace = run_moonpool_exact_m1_sealed_placement().await;
    assert_eq!(tokio_trace, moonpool_trace);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_and_broker_managed_cross_member_ordering_are_explicit() {
    let _serial = advanced_socket_test_guard();
    let tokio_trace = run_tokio_cross_member().await;
    let moonpool_trace = run_moonpool_cross_member().await;
    assert_eq!(tokio_trace, moonpool_trace);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_current_leaf_vector_seek_reattaches_and_partial_failure_resynchronizes() {
    let _serial = advanced_socket_test_guard();
    let tokio_trace = run_tokio_vector_seek().await;
    let moonpool_trace = run_moonpool_vector_seek().await;
    assert_eq!(tokio_trace, moonpool_trace);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_transactions_commit_abort_redeliver_and_poison_equivalently() {
    let _serial = advanced_socket_test_guard();
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    let tokio_trace = run_tokio_transactions().await;
    let moonpool_trace = run_moonpool_transactions().await;
    assert_eq!(tokio_trace, moonpool_trace);
    assert!(tokio_trace.outcome_retry_reused_retained_close);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_flight_operation_cancellation_is_recoverable_and_equivalent() {
    let _serial = advanced_socket_test_guard();
    let tokio_trace = run_tokio_operation_cancellation().await;
    let moonpool_trace = run_moonpool_operation_cancellation().await;
    assert_eq!(tokio_trace, moonpool_trace);
    assert!(tokio_trace.ordinary_ack_retried);
    assert_eq!(tokio_trace.transaction_poisoned, "transaction-poisoned");
    assert!(tokio_trace.transaction_registration_cancelled);
    assert!(tokio_trace.transaction_aborted);
    assert!(tokio_trace.seek_cancellation_resync);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_plane_push_epoch_order_and_replacement_are_equivalent() {
    let _serial = advanced_socket_test_guard();
    let tokio_trace = run_tokio_control_plane().await;
    let moonpool_trace = run_moonpool_control_plane().await;
    assert_eq!(tokio_trace, moonpool_trace);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_controller_registration_failure_is_equivalent() {
    let _serial = advanced_socket_test_guard();
    let tokio_trace = run_tokio_terminal_controller_failure().await;
    let moonpool_trace = run_moonpool_terminal_controller_failure().await;
    assert_eq!(tokio_trace, moonpool_trace);
    assert!(tokio_trace.terminal_failure_reported);
    assert!(!tokio_trace.replacement_assignment_applied);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_dag_reconnect_failure_is_equivalent() {
    let _serial = advanced_socket_test_guard();
    let tokio_trace = run_tokio_terminal_dag_reconnect().await;
    let moonpool_trace = run_moonpool_terminal_dag_reconnect().await;
    assert_eq!(tokio_trace, moonpool_trace);
    assert_eq!(
        tokio_trace.terminal_reason,
        "scalable route connection closed"
    );
    assert_eq!(
        moonpool_trace.terminal_reason,
        "scalable route connection closed"
    );
    assert!(!tokio_trace.replacement_assignment_applied);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compressed_partial_batch_and_chunk_delivery_are_equivalent() {
    let _serial = advanced_socket_test_guard();
    let tokio_trace = run_tokio_delivery_shapes().await;
    let moonpool_trace = run_moonpool_delivery_shapes().await;
    assert_eq!(tokio_trace, moonpool_trace);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn busy_and_withdrawn_child_opens_are_equivalent() {
    let _serial = advanced_socket_test_guard();
    let tokio_trace = run_tokio_child_open_lifecycle().await;
    let moonpool_trace = run_moonpool_child_open_lifecycle().await;
    assert_eq!(tokio_trace, moonpool_trace);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_and_failed_close_states_are_equivalent() {
    let _serial = advanced_socket_test_guard();
    let tokio_trace = run_tokio_close_states().await;
    let moonpool_trace = run_moonpool_close_states().await;
    assert_eq!(tokio_trace, moonpool_trace);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_compressed_delivery_resynchronizes_equivalently() {
    let _serial = advanced_socket_test_guard();
    let tokio_trace = run_tokio_malformed_delivery().await;
    let moonpool_trace = run_moonpool_malformed_delivery().await;
    assert_eq!(tokio_trace, moonpool_trace);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dag_watch_failure_reopens_the_control_plane_equivalently() {
    let _serial = advanced_socket_test_guard();
    let tokio_trace = run_tokio_dag_watch_recovery().await;
    let moonpool_trace = run_moonpool_dag_watch_recovery().await;
    assert_eq!(tokio_trace, moonpool_trace);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_and_closing_acknowledgement_failures_are_equivalent() {
    let _serial = advanced_socket_test_guard();
    let tokio_trace = run_tokio_acknowledgement_failures().await;
    let moonpool_trace = run_moonpool_acknowledgement_failures().await;
    assert_eq!(tokio_trace, moonpool_trace);
}
