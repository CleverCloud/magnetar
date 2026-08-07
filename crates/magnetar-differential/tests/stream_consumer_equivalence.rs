// SPDX-License-Identifier: Apache-2.0

//! Public scalable `StreamConsumer` parity over the stateful M1 fake cluster.

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]
#![allow(clippy::panic)]
#![allow(clippy::too_many_lines)]

mod stream_consumer_support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use magnetar::proto::schema::{AutoConsumeSchema, BytesSchema, Schema, SchemaError};
use magnetar::scalable::{
    BatchReceivePolicy, SegmentSubscriberApi, StreamConsumer, StreamConsumerEvent, StreamMessage,
};
use magnetar::{Engine, PulsarClient, TransactionApi};
use magnetar_fakes::m1::{
    BrokerFailure, Endpoint, FullAssignment, M1FakeCluster, M1Segment, OperationKind,
    ResourceCounts, ScriptedBehavior,
};
use stream_consumer_support::client::{
    connect_moonpool, connect_moonpool_with_keepalive, connect_tokio, connect_tokio_with_keepalive,
};
use stream_consumer_support::server::M1SocketCluster;

fn two_frame_receiver_budget() -> magnetar::proto::ReceiverBudget {
    magnetar::proto::ReceiverBudget::bytes(32 * 1024 * 1024)
        .expect("two-frame differential receive budget")
}

fn relocated_root_layout() -> Vec<M1Segment> {
    vec![
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0),
        M1Segment::active(2, 32_768, 65_535, Endpoint::Segment(1), 0),
    ]
}

fn original_root_layout() -> Vec<M1Segment> {
    vec![
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0),
        M1Segment::active(2, 32_768, 65_535, Endpoint::Segment(2), 0),
    ]
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SourcePosition {
    segment_id: u64,
    topic: String,
    ledger_id: u64,
    entry_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MessageIdentity {
    source: SourcePosition,
    payload: Vec<u8>,
    redelivery_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BaselineTrace {
    messages: Vec<MessageIdentity>,
    final_position: Vec<SourcePosition>,
    status: StatusTrace,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReceiveTrace {
    messages: Vec<MessageIdentity>,
    dequeue_sequences: Vec<u64>,
    status: StatusTrace,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BudgetTrace {
    delivery_order: Vec<u64>,
    messages: Vec<MessageIdentity>,
    initial_permits: u64,
    status: StatusTrace,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AssignmentTrace {
    assignments: Vec<(u64, Vec<u64>)>,
    messages: Vec<MessageIdentity>,
    old_owner_status: StatusTrace,
    new_owner_status: StatusTrace,
    final_status: StatusTrace,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReconnectTrace {
    messages: Vec<MessageIdentity>,
    segment_one_incarnations: usize,
    post_reconnect_permits: u64,
    status: StatusTrace,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AcknowledgementTrace {
    first_failure: String,
    stale_failure: String,
    acknowledgement_routes: usize,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NegativeAcknowledgementTrace {
    first: MessageIdentity,
    redelivered: MessageIdentity,
    dequeue_sequences: Vec<u64>,
    stale_failure: String,
    redelivery_routes: usize,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CloseTrace {
    receive_failure: String,
    status: StatusTrace,
    after_close: ResourceCounts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StatusTrace {
    layout_epoch: Option<u64>,
    assigned_segments: usize,
    attached_segments: usize,
    draining_segments: usize,
    pending_ownership: Vec<u64>,
    ordering_unprovable: Vec<u64>,
    budget_limit: usize,
    budget_used: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
struct PublicSurfaceTrace {
    builder_debug: bool,
    missing_subscription: bool,
    empty_subscription: bool,
    empty_consumer_name: bool,
    consumer_debug: bool,
    accessors: (String, String, String),
    empty_batch: bool,
    message_debug: bool,
    position_acknowledgements: usize,
    batch_acknowledged: bool,
    stale_position_token: bool,
    cumulative_transaction_registrations: usize,
    cumulative_transaction_acks: usize,
    cumulative_transaction_committed: bool,
    position_transaction_committed: bool,
    individual_transaction_aborted: bool,
    transaction_outcome_event: bool,
    aborted_outcome_event: bool,
    closed_event: bool,
    closed_stream: bool,
    default_name_generated: bool,
    broker_schema_resolved: bool,
    broker_schema_lookups: usize,
    schema_prepare_failures_nacked: [bool; 2],
    schema_cancellation_restored: bool,
    schema_restoration_failure_resynced: bool,
    decode_failure_nacked: bool,
    final_drop_fenced: bool,
    after_close: ResourceCounts,
}

#[derive(Debug)]
struct RejectingSchema;

impl Schema for RejectingSchema {
    type Owned = Bytes;

    fn schema_type(&self) -> magnetar::proto::pb::schema::Type {
        magnetar::proto::pb::schema::Type::None
    }

    fn schema_data(&self) -> Bytes {
        Bytes::new()
    }

    fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError> {
        Ok(value.clone())
    }

    fn decode(&self, _bytes: &[u8]) -> Result<Self::Owned, SchemaError> {
        Err(SchemaError::Decoding(
            "scripted decode rejection".to_owned(),
        ))
    }
}

fn source_position(
    source: &magnetar::proto::SegmentSource,
    message_id: magnetar::proto::MessageId,
) -> SourcePosition {
    SourcePosition {
        segment_id: source.segment_id().0,
        topic: source.topic().to_owned(),
        ledger_id: message_id.ledger_id,
        entry_id: message_id.entry_id,
    }
}

fn message_identity(message: &StreamMessage<BytesSchema>) -> MessageIdentity {
    MessageIdentity {
        source: source_position(message.source(), message.message_id().ordinary_message_id()),
        payload: message.value().to_vec(),
        redelivery_count: message.raw().redelivery_count,
    }
}

fn position_trace(position: &magnetar::proto::PositionVector) -> Vec<SourcePosition> {
    position
        .iter()
        .map(|(source, message_id)| source_position(source, message_id))
        .collect()
}

fn status_trace(status: &magnetar::scalable::StreamConsumerStatus) -> StatusTrace {
    StatusTrace {
        layout_epoch: status.layout_epoch(),
        assigned_segments: status.assigned_segments(),
        attached_segments: status.attached_segments(),
        draining_segments: status.draining_segments(),
        pending_ownership: status
            .pending_ownership()
            .iter()
            .map(|source| source.segment_id().0)
            .collect(),
        ordering_unprovable: status
            .ordering_unprovable()
            .iter()
            .map(|segment| segment.0)
            .collect(),
        budget_limit: status.receiver_budget_limit(),
        budget_used: status.receiver_budget_used(),
    }
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

async fn wait_for_initial_flow<S, E>(consumer: &StreamConsumer<S, E>)
where
    S: Schema,
    E: Engine,
{
    let mut assigned = None;
    let mut flowing = BTreeSet::new();
    let mut ordering_unprovable = BTreeSet::new();
    while assigned.is_none() || flowing.len() < 2 {
        let event = tokio::time::timeout(magnetar_differential::HANG_GUARD, consumer.next_event())
            .await
            .expect("aggregate lifecycle event timed out")
            .expect("aggregate lifecycle event failed")
            .expect("aggregate closed before initial flow");
        match event {
            StreamConsumerEvent::AssignmentApplied {
                layout_epoch,
                sources,
            } => {
                assert_eq!(layout_epoch, 1);
                assigned = Some(
                    sources
                        .iter()
                        .map(|source| source.segment_id().0)
                        .collect::<Vec<_>>(),
                );
            }
            StreamConsumerEvent::SegmentPhaseChanged {
                source,
                phase: magnetar::proto::SegmentPhase::Flowing,
            } => {
                flowing.insert(source.segment_id().0);
            }
            StreamConsumerEvent::SegmentPhaseChanged { .. } => {}
            StreamConsumerEvent::OrderingUnprovable { segment_id, .. } => {
                ordering_unprovable.insert(segment_id.0);
            }
            unexpected => panic!("unexpected initial aggregate event: {unexpected:?}"),
        }
    }
    assert_eq!(assigned, Some(vec![1, 2]));
    assert_eq!(flowing, BTreeSet::from([1, 2]));
    assert!(ordering_unprovable.is_empty());
}

async fn wait_for_assignment<E>(
    consumer: &StreamConsumer<BytesSchema, E>,
    expected_epoch: u64,
    expected_sources: &[u64],
) -> ((u64, Vec<u64>), Vec<String>)
where
    E: Engine,
{
    let mut resync_reasons = Vec::new();
    loop {
        let event = tokio::time::timeout(magnetar_differential::HANG_GUARD, consumer.next_event())
            .await
            .expect("assignment event timed out")
            .expect("assignment event failed")
            .expect("aggregate closed before assignment");
        match event {
            StreamConsumerEvent::AssignmentApplied {
                layout_epoch,
                sources,
            } if layout_epoch == expected_epoch => {
                let source_ids = sources
                    .iter()
                    .map(|source| source.segment_id().0)
                    .collect::<Vec<_>>();
                assert_eq!(source_ids, expected_sources);
                return ((layout_epoch, source_ids), resync_reasons);
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. } => {}
            StreamConsumerEvent::ResyncRequired { reason } => {
                resync_reasons.push(reason);
                assert!(
                    resync_reasons.len() <= 4,
                    "aggregate entered a resync loop: {resync_reasons:?}"
                );
            }
            unexpected => panic!("unexpected assignment event: {unexpected:?}"),
        }
    }
}

async fn wait_for_flowing_segment<E>(
    consumer: &StreamConsumer<BytesSchema, E>,
    cluster: &M1SocketCluster,
    expected_segment: u64,
) where
    E: Engine,
{
    loop {
        let event = tokio::time::timeout(
            magnetar_differential::HANG_GUARD,
            consumer.next_event(),
        )
        .await
        .unwrap_or_else(|_| {
            let (resources, routes) =
                cluster.inspect(|fake| (fake.resource_counts(), fake.routes().to_vec()));
            panic!(
                "segment phase event timed out: status={:?}, resources={resources:?}, routes={routes:?}",
                consumer.status()
            );
        })
        .expect("segment phase event failed")
        .expect("aggregate closed before segment flowed");
        match event {
            StreamConsumerEvent::SegmentPhaseChanged {
                source,
                phase: magnetar::proto::SegmentPhase::Flowing,
            } if source.segment_id().0 == expected_segment => {
                return;
            }
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. } => {}
            unexpected => panic!("unexpected segment phase event: {unexpected:?}"),
        }
    }
}

async fn observe_baseline<E>(client: &PulsarClient<E>, cluster: &M1SocketCluster) -> BaselineTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer = tokio::time::timeout(
        magnetar_differential::HANG_GUARD,
        client
            .scalable_stream_consumer(
                "topic://public/default/scaled",
                Arc::new(BytesSchema::new()),
            )
            .subscription("baseline-sub")
            .consumer_name("baseline-consumer")
            .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
            .receiver_budget(two_frame_receiver_budget())
            .subscribe(),
    )
    .await
    .expect("aggregate subscribe timed out")
    .expect("aggregate subscribe failed");
    cluster
        .wait_for("both segment children to attach", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    cluster.assert_healthy();
    wait_for_initial_flow(&consumer).await;
    cluster
        .wait_for("initial aggregate FLOW", |fake| {
            fake.resource_counts().permits >= 2
        })
        .await;

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"segment-one")))
        .expect("enqueue segment one");
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"segment-two")))
        .expect("enqueue segment two");
    cluster
        .wait_for("both segment messages to be dispatched", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    cluster.assert_healthy();

    let first = receive(&consumer).await;
    let second = receive(&consumer).await;
    let mut sequences = vec![
        first.delivery_token().dequeue_sequence().0,
        second.delivery_token().dequeue_sequence().0,
    ];
    sequences.sort_unstable();
    assert_eq!(sequences, vec![0, 1], "dequeue reservations are linearized");
    assert_eq!(first.position().layout_epoch(), 1);
    assert_eq!(second.position().layout_epoch(), 1);
    assert!(first.position().get(first.source()).is_some());
    assert!(second.position().get(second.source()).is_some());
    assert_eq!(
        consumer.delivered_position().len(),
        2,
        "both sources contribute to the aggregate position"
    );

    let mut messages = vec![message_identity(&first), message_identity(&second)];
    messages.sort();
    assert_eq!(
        messages
            .iter()
            .map(|message| message.source.segment_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        messages
            .iter()
            .map(|message| message.payload.as_slice())
            .collect::<Vec<_>>(),
        vec![b"segment-one".as_slice(), b"segment-two".as_slice()]
    );

    consumer
        .acknowledge(&first)
        .await
        .expect("acknowledge first delivery");
    consumer
        .acknowledge(&second)
        .await
        .expect("acknowledge second delivery");
    cluster
        .wait_for("all fake deliveries to be acknowledged", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    let status = status_trace(&consumer.status());
    assert_eq!(status.layout_epoch, Some(1));
    assert_eq!(status.assigned_segments, 2);
    assert_eq!(status.attached_segments, 2);
    assert_eq!(status.draining_segments, 0);
    assert!(status.pending_ownership.is_empty());
    assert!(status.ordering_unprovable.is_empty());
    assert!(status.budget_used <= status.budget_limit);
    let final_position = position_trace(&consumer.delivered_position());

    consumer.close().await.expect("close aggregate");
    cluster
        .wait_for("aggregate child cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0 && counts.pending_operations == 0 && counts.permits == 0
        })
        .await;
    let after_close = cluster.inspect(M1FakeCluster::resource_counts);
    assert_eq!(after_close.child_consumers, 0);
    assert_eq!(after_close.permits, 0);

    BaselineTrace {
        messages,
        final_position,
        status,
        after_close,
    }
}

async fn run_tokio_baseline() -> BaselineTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_baseline(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_baseline() -> BaselineTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_baseline(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_receive_matrix<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> ReceiveTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer = client
        .scalable_stream_consumer(
            "topic://public/default/scaled",
            Arc::new(BytesSchema::new()),
        )
        .subscription("receive-sub")
        .consumer_name("receive-consumer")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe receive matrix aggregate");
    cluster
        .wait_for("receive matrix children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer).await;

    for (segment, payload) in [
        (1, b"one-a".as_slice()),
        (2, b"two-a".as_slice()),
        (1, b"one-b".as_slice()),
        (2, b"two-b".as_slice()),
    ] {
        cluster
            .update(|fake| fake.enqueue_message(segment, Bytes::copy_from_slice(payload)))
            .expect("enqueue receive matrix message");
    }

    let (first, second) = tokio::join!(receive(&consumer), receive(&consumer));
    cluster
        .wait_for("all receive matrix messages to dispatch", |fake| {
            fake.resource_counts().unacked_messages == 4
        })
        .await;
    let mut batch = consumer
        .receive_batch(
            BatchReceivePolicy::new(2, 1024, Duration::from_secs(2)).expect("valid batch policy"),
        )
        .await
        .expect("receive aggregate batch");
    let batch_size = batch.len();
    assert!(
        (1..=2).contains(&batch_size),
        "the atomic batch respects both its non-empty and count bounds"
    );

    let mut messages = vec![first, second];
    messages.append(&mut batch);
    while messages.len() < 4 {
        messages.push(receive(&consumer).await);
    }
    let mut dequeue_sequences = messages
        .iter()
        .map(|message| message.delivery_token().dequeue_sequence().0)
        .collect::<Vec<_>>();
    dequeue_sequences.sort_unstable();
    assert_eq!(dequeue_sequences, vec![0, 1, 2, 3]);
    let mut identities = messages.iter().map(message_identity).collect::<Vec<_>>();
    identities.sort();
    assert_eq!(
        identities
            .iter()
            .map(|message| message.payload.as_slice())
            .collect::<Vec<_>>(),
        vec![
            b"one-a".as_slice(),
            b"one-b".as_slice(),
            b"two-a".as_slice(),
            b"two-b".as_slice(),
        ]
    );

    consumer
        .acknowledge_batch(&messages)
        .await
        .expect("acknowledge cross-source aggregate batch");
    cluster
        .wait_for("receive matrix acknowledgements", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;
    let status = status_trace(&consumer.status());
    assert_eq!(status.assigned_segments, 2);
    assert_eq!(status.attached_segments, 2);
    assert!(status.budget_used <= status.budget_limit);

    consumer
        .close()
        .await
        .expect("close receive matrix aggregate");
    cluster
        .wait_for("receive matrix cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0 && counts.pending_operations == 0 && counts.permits == 0
        })
        .await;
    ReceiveTrace {
        messages: identities,
        dequeue_sequences,
        status,
        after_close: cluster.inspect(M1FakeCluster::resource_counts),
    }
}

async fn run_tokio_receive_matrix() -> ReceiveTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_receive_matrix(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_receive_matrix() -> ReceiveTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_receive_matrix(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_single_permit_budget<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> BudgetTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let minimum_budget = match magnetar::proto::ReceiverBudget::bytes(0) {
        Err(magnetar::proto::BudgetError::BudgetTooSmall { minimum, .. }) => minimum,
        result => panic!("zero receiver budget returned an unexpected result: {result:?}"),
    };
    let consumer = client
        .scalable_stream_consumer(
            "topic://public/default/scaled",
            Arc::new(BytesSchema::new()),
        )
        .subscription("budget-sub")
        .consumer_name("budget-consumer")
        .receiver_budget(
            magnetar::proto::ReceiverBudget::bytes(minimum_budget)
                .expect("minimum aggregate budget is valid"),
        )
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .subscribe()
        .await
        .expect("subscribe budget aggregate");
    cluster
        .wait_for("budget children and one FLOW reservation", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 2 && counts.permits == 1
        })
        .await;
    let initial_permits = cluster.inspect(|fake| fake.resource_counts().permits);
    assert_eq!(initial_permits, 1);
    let initial_status = consumer.status();
    assert_eq!(initial_status.receiver_budget_limit(), minimum_budget);
    assert!(initial_status.receiver_budget_used() <= minimum_budget);

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"budget-one")))
        .expect("enqueue first budget message");
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"budget-two")))
        .expect("enqueue second budget message");
    cluster
        .wait_for("one budgeted dispatch", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;

    let first = receive(&consumer).await;
    consumer
        .acknowledge(&first)
        .await
        .expect("acknowledge first budget delivery");
    cluster
        .wait_for("budget rotates FLOW after lease resolution", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let second = receive(&consumer).await;
    let delivery_order = vec![
        first.source().segment_id().0,
        second.source().segment_id().0,
    ];
    let mut delivered_segments = delivery_order.clone();
    delivered_segments.sort_unstable();
    assert_eq!(delivered_segments, vec![1, 2]);
    let messages = [first, second];
    let mut identities = messages.iter().map(message_identity).collect::<Vec<_>>();
    identities.sort();
    consumer
        .acknowledge(&messages[1])
        .await
        .expect("acknowledge second budget delivery");
    cluster
        .wait_for("budget acknowledgements", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;
    let status = status_trace(&consumer.status());
    assert_eq!(status.budget_limit, minimum_budget);
    assert!(status.budget_used <= status.budget_limit);

    consumer.close().await.expect("close budget aggregate");
    cluster
        .wait_for("budget cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0 && counts.permits == 0
        })
        .await;
    BudgetTrace {
        delivery_order: delivered_segments,
        messages: identities,
        initial_permits,
        status,
        after_close: cluster.inspect(M1FakeCluster::resource_counts),
    }
}

async fn run_tokio_budget() -> BudgetTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_single_permit_budget(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_budget() -> BudgetTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_single_permit_budget(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_assignment_and_drain<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> AssignmentTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer_a = client
        .scalable_stream_consumer(
            "topic://public/default/scaled",
            Arc::new(BytesSchema::new()),
        )
        .subscription("assignment-sub")
        .consumer_name("assignment-a")
        .ordering_mode(magnetar::proto::OrderingMode::Strict)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe first assignment aggregate");
    cluster
        .wait_for("initial assignment children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer_a).await;
    cluster
        .wait_for("initial assignment FLOW reaches both brokers", |fake| {
            fake.segment_permits("assignment-sub", 1) > 0
                && fake.segment_permits("assignment-sub", 2) > 0
        })
        .await;

    let consumer_b = client
        .scalable_stream_consumer(
            "topic://public/default/scaled",
            Arc::new(BytesSchema::new()),
        )
        .subscription("assignment-sub")
        .consumer_name("assignment-b")
        .ordering_mode(magnetar::proto::OrderingMode::Strict)
        .subscribe()
        .await
        .expect("subscribe second assignment aggregate");
    let (initial_b, initial_b_resyncs) = wait_for_assignment(&consumer_b, 1, &[]).await;
    assert!(initial_b_resyncs.is_empty());

    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"old-owner")))
        .expect("enqueue old-owner delivery");
    cluster
        .wait_for("old owner delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let old_owner_message = receive(&consumer_a).await;
    assert_eq!(old_owner_message.source().segment_id().0, 2);
    cluster
        .wait_for("old-owner refill reaches the broker", |fake| {
            fake.segment_permits("assignment-sub", 2) > 0
        })
        .await;

    let member_a = cluster
        .inspect(|fake| fake.member("assignment-sub", "assignment-a"))
        .expect("first controller member is observable");
    let member_b = cluster
        .inspect(|fake| fake.member("assignment-sub", "assignment-b"))
        .expect("second controller member is observable");
    cluster
        .update(|fake| {
            fake.advance_layout(2, relocated_root_layout())?;
            fake.publish_assignment_plan(
                2,
                vec![
                    FullAssignment::new(member_a, [1, 2]),
                    FullAssignment::new(member_b, []),
                ],
            )
        })
        .expect("start descriptor replacement with outstanding FLOW");
    let (first_descriptor_assignment_a, first_descriptor_resync_a) =
        wait_for_assignment(&consumer_a, 2, &[1, 2]).await;
    let (first_descriptor_assignment_b, first_descriptor_resync_b) =
        wait_for_assignment(&consumer_b, 2, &[]).await;
    assert!(first_descriptor_resync_a.is_empty());
    assert!(first_descriptor_resync_b.is_empty());
    assert_eq!(consumer_a.status().draining_segments(), 1);

    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.advance_layout(3, original_root_layout())?;
            fake.publish_assignment_plan(
                3,
                vec![
                    FullAssignment::new(member_a, [1, 2]),
                    FullAssignment::new(member_b, []),
                ],
            )
        })
        .expect("change the descriptor again while the old child is draining");
    let (second_descriptor_assignment_a, second_descriptor_resync_a) =
        wait_for_assignment(&consumer_a, 3, &[1, 2]).await;
    let (second_descriptor_assignment_b, second_descriptor_resync_b) =
        wait_for_assignment(&consumer_b, 3, &[]).await;
    assert!(second_descriptor_resync_a.is_empty());
    assert!(second_descriptor_resync_b.is_empty());
    assert!(
        !cluster.inspect(|fake| {
            fake.routes().iter().any(|route| {
                route.command == magnetar::proto::pb::base_command::Type::Subscribe
                    && route.resource.as_deref().is_some_and(|resource| {
                        resource.starts_with("segment://public/default/scaled/8000-ffff-2:")
                    })
            })
        }),
        "the latest descriptor must not open before obsolete FLOW is consumed"
    );

    consumer_a
        .acknowledge(&old_owner_message)
        .await
        .expect("old owner retains acknowledgement authority while draining");
    cluster
        .wait_for("old-owner acknowledgement", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;
    assert_eq!(
        cluster.inspect(|fake| fake.active_child_owner("assignment-sub", 2)),
        Some(member_a),
        "descriptor replacement retains the old child until obsolete FLOW is terminal"
    );
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"old-flow-fence")))
        .expect("consume the obsolete descriptor FLOW");
    cluster
        .wait_for("obsolete FLOW delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let flow_fence_message = receive(&consumer_a).await;
    assert_eq!(flow_fence_message.source().segment_id().0, 2);
    consumer_a
        .acknowledge(&flow_fence_message)
        .await
        .expect("resolve the obsolete FLOW delivery");
    cluster
        .wait_for("obsolete FLOW acknowledgement", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;
    wait_for_flowing_segment(&consumer_a, cluster, 2).await;
    cluster
        .wait_for("replacement FLOW reaches the relocated broker", |fake| {
            fake.assigned_owner("assignment-sub", 2) == Some(member_a)
                && fake.active_child_owner("assignment-sub", 2) == Some(member_a)
                && fake.segment_permits("assignment-sub", 2) > 0
        })
        .await;
    assert!(cluster.inspect(|fake| {
        fake.routes().iter().any(|route| {
            route.endpoint == Endpoint::Segment(2)
                && route.command == magnetar::proto::pb::base_command::Type::Subscribe
                && route.resource.as_deref().is_some_and(|resource| {
                    resource.starts_with("segment://public/default/scaled/8000-ffff-2:")
                })
        })
    }));

    let old_owner_status = status_trace(&consumer_a.status());
    assert_eq!(old_owner_status.assigned_segments, 2);
    assert_eq!(old_owner_status.attached_segments, 2);
    assert_eq!(old_owner_status.draining_segments, 0);
    let new_owner_status = status_trace(&consumer_b.status());
    assert_eq!(new_owner_status.assigned_segments, 0);
    assert_eq!(new_owner_status.attached_segments, 0);
    assert_eq!(new_owner_status.draining_segments, 0);
    assert!(new_owner_status.pending_ownership.is_empty());

    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"new-owner")))
        .expect("enqueue new-owner delivery");
    cluster
        .wait_for("new owner delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let new_owner_message = receive(&consumer_a).await;
    assert_eq!(new_owner_message.source().segment_id().0, 2);
    let mut messages = [&old_owner_message, &flow_fence_message]
        .into_iter()
        .map(message_identity)
        .chain(core::iter::once(message_identity(&new_owner_message)))
        .collect::<Vec<_>>();
    messages.sort();
    consumer_a
        .acknowledge(&new_owner_message)
        .await
        .expect("acknowledge replacement delivery");
    cluster
        .wait_for("new-owner acknowledgement", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;
    let final_status = status_trace(&consumer_a.status());
    assert_eq!(final_status.layout_epoch, Some(3));
    assert_eq!(final_status.assigned_segments, 2);
    assert_eq!(final_status.attached_segments, 2);
    assert_eq!(final_status.draining_segments, 0);
    assert!(final_status.pending_ownership.is_empty());

    consumer_a
        .close()
        .await
        .expect("close first assignment aggregate");
    consumer_b
        .close()
        .await
        .expect("close second assignment aggregate");
    cluster
        .wait_for("assignment cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0
                && counts.layout_sessions == 0
                && counts.pending_operations == 0
                && counts.permits == 0
        })
        .await;
    AssignmentTrace {
        assignments: vec![
            initial_b,
            first_descriptor_assignment_a,
            first_descriptor_assignment_b,
            second_descriptor_assignment_a,
            second_descriptor_assignment_b,
        ],
        messages,
        old_owner_status,
        new_owner_status,
        final_status,
        after_close: cluster.inspect(M1FakeCluster::resource_counts),
    }
}

async fn run_tokio_assignment() -> AssignmentTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_assignment_and_drain(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_assignment() -> AssignmentTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_assignment_and_drain(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_segment_reconnect<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> ReconnectTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer = client
        .scalable_stream_consumer(
            "topic://public/default/scaled",
            Arc::new(BytesSchema::new()),
        )
        .subscription("reconnect-sub")
        .consumer_name("reconnect-consumer")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe reconnect aggregate");
    cluster
        .wait_for("reconnect children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer).await;

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"before-reconnect")))
        .expect("enqueue pre-reconnect message");
    cluster
        .wait_for("pre-reconnect dispatch", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let before = receive(&consumer).await;
    assert_eq!(before.source().segment_id().0, 1);
    let before_identity = message_identity(&before);
    consumer
        .acknowledge(&before)
        .await
        .expect("acknowledge pre-reconnect delivery");
    cluster
        .wait_for("pre-reconnect acknowledgement", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;
    assert_eq!(
        cluster.inspect(|fake| fake.durable_cursor("reconnect-sub", 1)),
        Some(1)
    );
    drop(before);
    let old_connection = cluster
        .inspect(|fake| {
            fake.routes()
                .iter()
                .find(|route| {
                    route.endpoint == Endpoint::Segment(1)
                        && route.command == magnetar::proto::pb::base_command::Type::Connect
                })
                .map(|route| route.connection)
        })
        .expect("initial segment-one connection");

    let disconnected = cluster
        .update(|fake| fake.disconnect_endpoint(Endpoint::Segment(1)))
        .expect("disconnect segment-one endpoint");
    assert_eq!(disconnected, 1);
    cluster
        .wait_for("segment-one supervised reconnect", |fake| {
            let replacement_connected = fake.routes().iter().any(|route| {
                route.endpoint == Endpoint::Segment(1)
                    && route.command == magnetar::proto::pb::base_command::Type::Connect
                    && route.connection != old_connection
            });
            let replacement_flowing = fake.routes().iter().any(|route| {
                route.endpoint == Endpoint::Segment(1)
                    && route.command == magnetar::proto::pb::base_command::Type::Flow
                    && route.connection != old_connection
            });
            replacement_connected
                && replacement_flowing
                && fake.resource_counts().child_consumers == 2
        })
        .await;
    assert_eq!(
        cluster.inspect(|fake| fake.durable_cursor("reconnect-sub", 1)),
        Some(1)
    );
    let post_reconnect_permits = cluster.inspect(|fake| fake.segment_permits("reconnect-sub", 1));
    assert_eq!(post_reconnect_permits, 1);
    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"after-reconnect")))
        .expect("enqueue post-reconnect message");
    cluster
        .wait_for("post-reconnect dispatch", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let after = receive(&consumer).await;
    assert_eq!(after.source().segment_id().0, 1);
    assert_eq!(after.raw().payload.as_ref(), b"after-reconnect");
    let after_identity = message_identity(&after);
    consumer
        .acknowledge(&after)
        .await
        .expect("acknowledge post-reconnect delivery");
    cluster
        .wait_for("post-reconnect acknowledgement", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;
    assert_eq!(
        cluster.inspect(|fake| fake.durable_cursor("reconnect-sub", 1)),
        Some(2)
    );
    let messages = vec![before_identity, after_identity];
    let segment_one_incarnations = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .filter(|route| {
                route.endpoint == Endpoint::Segment(1)
                    && route.command == magnetar::proto::pb::base_command::Type::Connect
            })
            .map(|route| route.connection)
            .collect::<BTreeSet<_>>()
            .len()
    });
    assert_eq!(segment_one_incarnations, 2);
    let status = status_trace(&consumer.status());
    assert_eq!(status.assigned_segments, 2);
    assert_eq!(status.attached_segments, 2);

    consumer.close().await.expect("close reconnect aggregate");
    cluster
        .wait_for("reconnect cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0 && counts.pending_operations == 0
        })
        .await;
    ReconnectTrace {
        messages,
        segment_one_incarnations,
        post_reconnect_permits,
        status,
        after_close: cluster.inspect(M1FakeCluster::resource_counts),
    }
}

async fn run_tokio_reconnect() -> ReconnectTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_segment_reconnect(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_reconnect() -> ReconnectTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_segment_reconnect(&client, &cluster).await;
    client.close().await;
    trace
}

fn acknowledgement_failure(error: magnetar::scalable::StreamConsumerError) -> String {
    match error {
        magnetar::scalable::StreamConsumerError::PartialAcknowledgement { confirmed, failed } => {
            format!("partial:{}:{}", confirmed.len(), failed.len())
        }
        magnetar::scalable::StreamConsumerError::Model(_) => "model".to_owned(),
        magnetar::scalable::StreamConsumerError::Engine { .. } => "engine".to_owned(),
        unexpected => panic!("unexpected acknowledgement failure: {unexpected:?}"),
    }
}

async fn observe_acknowledgement_failure<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> AcknowledgementTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer = client
        .scalable_stream_consumer(
            "topic://public/default/scaled",
            Arc::new(BytesSchema::new()),
        )
        .subscription("ack-failure-sub")
        .consumer_name("ack-failure-consumer")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe acknowledgement-failure aggregate");
    cluster
        .wait_for("acknowledgement-failure children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer).await;
    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"retry-ack")))
        .expect("enqueue acknowledgement-failure message");
    cluster
        .wait_for("acknowledgement-failure dispatch", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let message = receive(&consumer).await;

    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Segment(1),
                OperationKind::Ack,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar::proto::pb::ServerError::PersistenceError,
                    "scripted acknowledgement failure",
                )),
            )
        })
        .expect("script one acknowledgement failure");
    let first_failure = acknowledgement_failure(
        consumer
            .acknowledge(&message)
            .await
            .expect_err("scripted acknowledgement must fail"),
    );
    assert_eq!(first_failure, "partial:0:1");
    assert_eq!(
        cluster.inspect(|fake| fake.resource_counts().unacked_messages),
        1,
        "failed acknowledgement keeps broker and token authority live"
    );

    consumer
        .acknowledge(&message)
        .await
        .expect("retry acknowledgement succeeds");
    cluster
        .wait_for("retried acknowledgement", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;
    let stale_failure = acknowledgement_failure(
        consumer
            .acknowledge(&message)
            .await
            .expect_err("resolved token cannot be acknowledged twice"),
    );
    assert_eq!(stale_failure, "model");
    let acknowledgement_routes = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .filter(|route| route.command == magnetar::proto::pb::base_command::Type::Ack)
            .count()
    });
    assert_eq!(acknowledgement_routes, 2, "stale retry emits no wire ACK");

    consumer
        .close()
        .await
        .expect("close acknowledgement-failure aggregate");
    cluster
        .wait_for("acknowledgement-failure cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0 && counts.pending_operations == 0
        })
        .await;
    AcknowledgementTrace {
        first_failure,
        stale_failure,
        acknowledgement_routes,
        after_close: cluster.inspect(M1FakeCluster::resource_counts),
    }
}

async fn run_tokio_acknowledgement_failure() -> AcknowledgementTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_acknowledgement_failure(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_acknowledgement_failure() -> AcknowledgementTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_acknowledgement_failure(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_negative_acknowledgement<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> NegativeAcknowledgementTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer = client
        .scalable_stream_consumer(
            "topic://public/default/scaled",
            Arc::new(BytesSchema::new()),
        )
        .subscription("negative-ack-sub")
        .consumer_name("negative-ack-consumer")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe negative-acknowledgement aggregate");
    cluster
        .wait_for("negative-acknowledgement children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer).await;
    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"redeliver-me")))
        .expect("enqueue negative-acknowledgement message");
    cluster
        .wait_for("negative-acknowledgement dispatch", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let first_message = receive(&consumer).await;
    consumer
        .negative_acknowledge(&first_message)
        .expect("negative acknowledgement is admitted");
    let stale_failure = acknowledgement_failure(
        consumer
            .negative_acknowledge(&first_message)
            .expect_err("negative acknowledgement resolves the original token"),
    );
    assert_eq!(stale_failure, "model");
    cluster
        .wait_for("negative acknowledgement reaches the broker", |fake| {
            fake.routes().iter().any(|route| {
                route.command
                    == magnetar::proto::pb::base_command::Type::RedeliverUnacknowledgedMessages
            })
        })
        .await;
    let replay = receive(&consumer).await;
    assert_eq!(replay.raw().redelivery_count, 1);
    assert_eq!(
        message_identity(&first_message).payload,
        message_identity(&replay).payload
    );
    let dequeue_sequences = vec![
        first_message.delivery_token().dequeue_sequence().0,
        replay.delivery_token().dequeue_sequence().0,
    ];
    assert_eq!(dequeue_sequences, vec![0, 1]);
    consumer
        .acknowledge(&replay)
        .await
        .expect("acknowledge redelivered message");
    cluster
        .wait_for("redelivered acknowledgement", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;
    let redelivery_routes = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .filter(|route| {
                route.command
                    == magnetar::proto::pb::base_command::Type::RedeliverUnacknowledgedMessages
            })
            .count()
    });
    assert_eq!(redelivery_routes, 1);

    let first = message_identity(&first_message);
    let redelivered = message_identity(&replay);
    consumer
        .close()
        .await
        .expect("close negative-acknowledgement aggregate");
    cluster
        .wait_for("negative-acknowledgement cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0 && counts.pending_operations == 0
        })
        .await;
    NegativeAcknowledgementTrace {
        first,
        redelivered,
        dequeue_sequences,
        stale_failure,
        redelivery_routes,
        after_close: cluster.inspect(M1FakeCluster::resource_counts),
    }
}

async fn run_tokio_negative_acknowledgement() -> NegativeAcknowledgementTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_negative_acknowledgement(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_negative_acknowledgement() -> NegativeAcknowledgementTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_negative_acknowledgement(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_close_cancellation<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> CloseTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi,
{
    let consumer = client
        .scalable_stream_consumer(
            "topic://public/default/scaled",
            Arc::new(BytesSchema::new()),
        )
        .subscription("close-sub")
        .consumer_name("close-consumer")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe close aggregate");
    cluster
        .wait_for("close children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer).await;

    let closer = consumer.clone();
    let (receive_result, close_result) =
        tokio::time::timeout(magnetar_differential::HANG_GUARD, async {
            tokio::join!(consumer.receive(), closer.close())
        })
        .await
        .expect("global close did not wake the parked aggregate receive");
    close_result.expect("global close succeeds while receive is parked");
    let receive_failure = acknowledgement_failure(
        receive_result.expect_err("global close wakes a parked aggregate receive"),
    );
    assert!(matches!(receive_failure.as_str(), "model" | "engine"));
    let status = status_trace(&consumer.status());
    assert_eq!(status.assigned_segments, 0);
    assert_eq!(status.attached_segments, 0);
    assert_eq!(status.draining_segments, 0);
    assert!(status.pending_ownership.is_empty());
    consumer
        .close()
        .await
        .expect("repeated close through another clone is idempotent");
    let close_commands = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .filter(|route| route.command == magnetar::proto::pb::base_command::Type::CloseConsumer)
            .count()
    });
    assert_eq!(close_commands, 2, "each child must be closed exactly once");
    cluster
        .wait_for("global close cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.layout_sessions == 0
                && counts.child_consumers == 0
                && counts.pending_operations == 0
                && counts.permits == 0
        })
        .await;
    CloseTrace {
        receive_failure,
        status,
        after_close: cluster.inspect(M1FakeCluster::resource_counts),
    }
}

async fn run_tokio_close_cancellation() -> CloseTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_close_cancellation(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_close_cancellation() -> CloseTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_close_cancellation(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_public_surface<E>(
    client: &PulsarClient<E>,
    cluster: &M1SocketCluster,
) -> PublicSurfaceTrace
where
    E: Engine,
    E::ClientState: SegmentSubscriberApi + TransactionApi,
{
    let builder = client.scalable_stream_consumer(
        "topic://public/default/scaled",
        Arc::new(BytesSchema::new()),
    );
    let builder_debug = format!("{builder:?}").contains("StreamConsumerBuilder");
    let missing_subscription = matches!(
        builder.subscribe().await,
        Err(magnetar::scalable::StreamConsumerError::MissingSubscription)
    );
    let empty_subscription = matches!(
        client
            .scalable_stream_consumer(
                "topic://public/default/scaled",
                Arc::new(BytesSchema::new()),
            )
            .subscription("")
            .subscribe()
            .await,
        Err(magnetar::scalable::StreamConsumerError::EmptySubscription)
    );
    let empty_consumer_name = matches!(
        client
            .scalable_stream_consumer(
                "topic://public/default/scaled",
                Arc::new(BytesSchema::new()),
            )
            .subscription("surface-invalid-sub")
            .consumer_name("")
            .subscribe()
            .await,
        Err(magnetar::scalable::StreamConsumerError::EmptyConsumerName)
    );

    let consumer = client
        .scalable_stream_consumer(
            "topic://public/default/scaled",
            Arc::new(BytesSchema::new()),
        )
        .subscription("surface-position-sub")
        .consumer_name("surface-position-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe public position surface");
    cluster
        .wait_for("public position children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&consumer).await;
    let consumer_debug = format!("{consumer:?}").contains("StreamConsumer");
    let accessors = (
        consumer.topic().to_owned(),
        consumer.subscription().to_owned(),
        consumer.consumer_name().to_owned(),
    );
    assert_eq!(
        consumer.schema().schema_type(),
        magnetar::proto::pb::schema::Type::None
    );
    let empty_batch = consumer
        .receive_batch(
            BatchReceivePolicy::messages(2, Duration::from_millis(5))
                .expect("valid empty-batch policy"),
        )
        .await
        .expect("empty batch timeout is not an error")
        .is_empty();

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"position-one")))
        .expect("enqueue first position message");
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"position-two")))
        .expect("enqueue second position message");
    cluster
        .wait_for("public position deliveries", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let first = receive(&consumer).await;
    let second = receive(&consumer).await;
    let message_debug = format!("{first:?}").contains("StreamMessage");
    let position = consumer.delivered_position();
    let restored = magnetar::proto::PositionVector::from_bytes(
        &position.to_bytes().expect("serialize public position"),
    )
    .expect("restore public position");
    consumer
        .acknowledge_positions(&restored)
        .await
        .expect("acknowledge restored public position");
    cluster
        .wait_for("public position acknowledgements", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;
    let position_acknowledgements = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .filter(|route| route.command == magnetar::proto::pb::base_command::Type::Ack)
            .count()
    });
    let stale_position_token = matches!(
        consumer
            .acknowledge(&first)
            .await
            .expect_err("position acknowledgement resolves live tokens"),
        magnetar::scalable::StreamConsumerError::Model(
            magnetar::proto::StreamConsumerModelError::StaleDeliveryToken
        )
    );
    drop(second);
    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"batch-one")))
        .expect("enqueue first batch-ack message");
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"batch-two")))
        .expect("enqueue second batch-ack message");
    cluster
        .wait_for("public batch-ack deliveries", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let batch_first = receive(&consumer).await;
    let batch_second = receive(&consumer).await;
    consumer
        .acknowledge_batch(&[batch_first, batch_second])
        .await
        .expect("acknowledge public message batch");
    cluster
        .wait_for("public batch acknowledgements", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;
    let batch_acknowledged = true;

    let closed_observer = consumer.clone();
    consumer
        .close()
        .await
        .expect("close public position surface");
    let closed_event = loop {
        match closed_observer
            .next_event()
            .await
            .expect("read public close event")
        {
            Some(StreamConsumerEvent::Closed) => break true,
            Some(_) => {}
            None => break false,
        }
    };
    let closed_stream = closed_observer
        .next_event()
        .await
        .expect("read closed public event stream")
        .is_none();
    cluster
        .wait_for("public position cleanup", |fake| {
            fake.resource_counts().child_consumers == 0
        })
        .await;

    let transaction_consumer = client
        .scalable_stream_consumer(
            "topic://public/default/scaled",
            Arc::new(BytesSchema::new()),
        )
        .subscription("surface-transaction-sub")
        .consumer_name("surface-transaction-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe cumulative transaction surface");
    cluster
        .wait_for("cumulative transaction children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&transaction_consumer).await;
    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"transaction-one")))
        .expect("enqueue first cumulative transaction message");
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"transaction-two")))
        .expect("enqueue second cumulative transaction message");
    cluster
        .wait_for("cumulative transaction deliveries", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let _transaction_first = receive(&transaction_consumer).await;
    let transaction_second = receive(&transaction_consumer).await;
    assert_eq!(transaction_second.position().len(), 2);
    let transaction = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open cumulative acknowledgement transaction");
    transaction_consumer
        .acknowledge_cumulative_in_transaction(&transaction_second, transaction)
        .await
        .expect("stage cumulative transactional acknowledgement");
    let observation = cluster
        .inspect(|fake| fake.transaction_observation(transaction.id()))
        .expect("observe cumulative acknowledgement transaction");
    let cumulative_transaction_registrations = observation.registered_subscriptions.len();
    let cumulative_transaction_acks = observation.staged_acknowledgements;
    let cumulative_transaction_committed = client
        .commit_transaction(transaction)
        .await
        .expect("commit cumulative acknowledgement transaction")
        == magnetar::TxnState::Committed;
    cluster
        .wait_for("cumulative transaction commit", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;
    let transaction_outcome_event = loop {
        match transaction_consumer
            .next_event()
            .await
            .expect("read cumulative transaction event")
        {
            Some(StreamConsumerEvent::TransactionOutcome {
                txn_id,
                outcome: magnetar::scalable::TransactionOutcome::Committed,
            }) if txn_id == transaction.id() => break true,
            Some(_) => {}
            None => break false,
        }
    };

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"txn-position-one")))
        .expect("enqueue first position-transaction message");
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"txn-position-two")))
        .expect("enqueue second position-transaction message");
    cluster
        .wait_for("position-transaction deliveries", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    let _position_first = receive(&transaction_consumer).await;
    let _position_second = receive(&transaction_consumer).await;
    let transaction_position = transaction_consumer.delivered_position();
    let position_transaction = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open position acknowledgement transaction");
    transaction_consumer
        .acknowledge_positions_in_transaction(&transaction_position, position_transaction)
        .await
        .expect("stage transactional position acknowledgement");
    let position_transaction_committed = client
        .commit_transaction(position_transaction)
        .await
        .expect("commit position acknowledgement transaction")
        == magnetar::TxnState::Committed;
    cluster
        .wait_for("position transaction commit", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"txn-abort")))
        .expect("enqueue individual abort message");
    cluster
        .wait_for("individual abort delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let abort_message = receive(&transaction_consumer).await;
    let abort_transaction = client
        .new_transaction(Duration::from_secs(30))
        .await
        .expect("open individual abort transaction");
    transaction_consumer
        .acknowledge_in_transaction(&abort_message, abort_transaction)
        .await
        .expect("stage individual transactional acknowledgement");
    let individual_transaction_aborted = client
        .abort_transaction(abort_transaction)
        .await
        .expect("abort individual acknowledgement transaction")
        == magnetar::TxnState::Aborted;
    let aborted_outcome_event = loop {
        match transaction_consumer
            .next_event()
            .await
            .expect("read aborted transaction event")
        {
            Some(StreamConsumerEvent::TransactionOutcome {
                txn_id,
                outcome: magnetar::scalable::TransactionOutcome::Aborted,
            }) if txn_id == abort_transaction.id() => break true,
            Some(_) => {}
            None => break false,
        }
    };
    transaction_consumer
        .close()
        .await
        .expect("close cumulative transaction surface");
    cluster
        .wait_for("cumulative transaction cleanup", |fake| {
            fake.resource_counts().child_consumers == 0
        })
        .await;

    let generated_name_consumer = client
        .scalable_stream_consumer(
            "topic://public/default/scaled",
            Arc::new(BytesSchema::new()),
        )
        .subscription("surface-generated-name-sub")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .subscribe()
        .await
        .expect("subscribe with generated aggregate name");
    let default_name_generated = generated_name_consumer
        .consumer_name()
        .starts_with("magnetar-stream-");
    generated_name_consumer
        .close()
        .await
        .expect("close generated-name aggregate");
    cluster
        .wait_for("generated-name cleanup", |fake| {
            fake.resource_counts().child_consumers == 0
        })
        .await;

    let auto_schema = Arc::new(AutoConsumeSchema::new());
    let schema_consumer = client
        .scalable_stream_consumer("topic://public/default/scaled", auto_schema.clone())
        .subscription("surface-schema-sub")
        .consumer_name("surface-schema-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe broker-schema aggregate");
    cluster
        .wait_for("broker-schema children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&schema_consumer).await;
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.enqueue_message(1, Bytes::from_static(b"schema-payload"))
        })
        .expect("enqueue broker-schema message");
    let schema_message =
        tokio::time::timeout(magnetar_differential::HANG_GUARD, schema_consumer.receive())
            .await
            .expect("broker-schema receive timed out")
            .expect("broker-schema receive failed");
    let broker_schema_resolved =
        auto_schema.has_cached_schema() && schema_message.value().as_ref() == b"schema-payload";
    let broker_schema_lookups = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .filter(|route| route.command == magnetar::proto::pb::base_command::Type::GetSchema)
            .count()
    });
    schema_consumer
        .acknowledge(&schema_message)
        .await
        .expect("acknowledge broker-schema message");
    schema_consumer
        .close()
        .await
        .expect("close broker-schema aggregate");
    cluster
        .wait_for("broker-schema cleanup", |fake| {
            fake.resource_counts().child_consumers == 0
        })
        .await;

    let mut schema_prepare_failures_nacked = [false; 2];
    for (index, batch) in [false, true].into_iter().enumerate() {
        let label = if batch { "batch" } else { "single" };
        let prepare_failure_consumer = client
            .scalable_stream_consumer(
                "topic://public/default/scaled",
                Arc::new(AutoConsumeSchema::new()),
            )
            .subscription(format!("surface-{label}-schema-prepare-failure-sub"))
            .consumer_name(format!("surface-{label}-schema-prepare-failure-member"))
            .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
            .receiver_budget(two_frame_receiver_budget())
            .subscribe()
            .await
            .expect("subscribe schema-prepare failure aggregate");
        cluster
            .wait_for("schema-prepare failure children", |fake| {
                fake.resource_counts().child_consumers == 2
            })
            .await;
        wait_for_initial_flow(&prepare_failure_consumer).await;
        cluster
            .update(|fake| {
                fake.clear_routes();
                fake.script_next(
                    Endpoint::Segment(1),
                    OperationKind::GetSchema,
                    ScriptedBehavior::Fail(BrokerFailure::new(
                        magnetar::proto::pb::ServerError::MetadataError,
                        "scripted schema preparation failure",
                    )),
                )?;
                fake.enqueue_message(1, Bytes::from_static(b"schema-prepare-failure-one"))?;
                if batch {
                    fake.enqueue_message(1, Bytes::from_static(b"schema-prepare-failure-two"))?;
                }
                Ok(())
            })
            .expect("enqueue schema-prepare failure delivery");
        let result = async {
            if batch {
                prepare_failure_consumer
                    .receive_batch(
                        BatchReceivePolicy::messages(2, Duration::from_secs(1))
                            .expect("valid schema-prepare failure batch policy"),
                    )
                    .await
                    .map(|_| ())
            } else {
                prepare_failure_consumer.receive().await.map(|_| ())
            }
        }
        .await;
        let error = result.expect_err("schema preparation rejection reaches caller");
        assert!(
            error
                .to_string()
                .contains("scripted schema preparation failure")
        );
        cluster
            .wait_for("schema-prepare failure negative acknowledgement", |fake| {
                fake.routes().iter().any(|route| {
                    route.command
                        == magnetar::proto::pb::base_command::Type::RedeliverUnacknowledgedMessages
                })
            })
            .await;
        schema_prepare_failures_nacked[index] = cluster.inspect(|fake| {
            fake.routes()
                .iter()
                .filter(|route| {
                    route.command
                        == magnetar::proto::pb::base_command::Type::RedeliverUnacknowledgedMessages
                })
                .count()
                == 1
        });
        assert!(schema_prepare_failures_nacked[index]);
        prepare_failure_consumer
            .close()
            .await
            .expect("close schema-prepare failure aggregate");
        cluster
            .wait_for("schema-prepare failure cleanup", |fake| {
                let counts = fake.resource_counts();
                counts.child_consumers == 0
                    && counts.pending_operations == 0
                    && counts.unacked_messages == 0
            })
            .await;
    }

    let cancellation_schema = Arc::new(AutoConsumeSchema::new());
    let cancellation_consumer = client
        .scalable_stream_consumer("topic://public/default/scaled", cancellation_schema.clone())
        .subscription("surface-schema-cancellation-sub")
        .consumer_name("surface-schema-cancellation-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe schema-cancellation aggregate");
    cluster
        .wait_for("schema-cancellation children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&cancellation_consumer).await;
    cluster.hold_command(
        Endpoint::Segment(1),
        magnetar::proto::pb::base_command::Type::GetSchemaResponse,
    );
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.enqueue_message(1, Bytes::from_static(b"schema-cancellation"))
        })
        .expect("enqueue schema-cancellation message");
    let mut cancelled_receive = Box::pin(cancellation_consumer.receive());
    tokio::select! {
        biased;
        result = &mut cancelled_receive => panic!("held schema preparation completed early: {result:?}"),
        () = cluster.wait_for("held schema preparation", |fake| {
            fake.routes().iter().any(|route| {
                route.endpoint == Endpoint::Segment(1)
                    && route.command == magnetar::proto::pb::base_command::Type::GetSchema
            })
        }) => {}
    }
    drop(cancelled_receive);
    cluster.release_command(
        Endpoint::Segment(1),
        magnetar::proto::pb::base_command::Type::GetSchemaResponse,
    );
    let restored_schema_message = tokio::time::timeout(
        magnetar_differential::HANG_GUARD,
        cancellation_consumer.receive(),
    )
    .await
    .expect("restored schema-cancellation receive timed out")
    .expect("restored schema-cancellation receive failed");
    let schema_cancellation_restored = restored_schema_message.value().as_ref()
        == b"schema-cancellation"
        && restored_schema_message
            .delivery_token()
            .dequeue_sequence()
            .0
            == 0
        && cancellation_schema.has_cached_schema()
        && cluster.inspect(|fake| {
            !fake.routes().iter().any(|route| {
                route.command
                    == magnetar::proto::pb::base_command::Type::RedeliverUnacknowledgedMessages
            })
        });
    assert!(schema_cancellation_restored);
    cancellation_consumer
        .acknowledge(&restored_schema_message)
        .await
        .expect("acknowledge restored schema-cancellation message");
    cancellation_consumer
        .close()
        .await
        .expect("close schema-cancellation aggregate");
    cluster
        .wait_for("schema-cancellation cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0 && counts.unacked_messages == 0
        })
        .await;

    let rejecting_consumer = client
        .scalable_stream_consumer("topic://public/default/scaled", Arc::new(RejectingSchema))
        .subscription("surface-rejecting-schema-sub")
        .consumer_name("surface-rejecting-schema-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe rejecting-schema aggregate");
    cluster
        .wait_for("rejecting-schema children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&rejecting_consumer).await;
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.enqueue_message(1, Bytes::from_static(b"reject-me"))
        })
        .expect("enqueue rejecting-schema message");
    assert!(matches!(
        tokio::time::timeout(
            magnetar_differential::HANG_GUARD,
            rejecting_consumer.receive(),
        )
        .await
        .expect("rejecting-schema receive timed out")
        .expect_err("schema rejection reaches the caller"),
        magnetar::scalable::StreamConsumerError::Schema(SchemaError::Decoding(_))
    ));
    cluster
        .wait_for("schema-rejected negative acknowledgement", |fake| {
            fake.routes().iter().any(|route| {
                route.command
                    == magnetar::proto::pb::base_command::Type::RedeliverUnacknowledgedMessages
            })
        })
        .await;
    let decode_failure_nacked = true;
    rejecting_consumer
        .close()
        .await
        .expect("close rejecting-schema aggregate");
    cluster
        .wait_for("rejecting-schema cleanup", |fake| {
            fake.resource_counts().child_consumers == 0
        })
        .await;

    let rejecting_batch_consumer = client
        .scalable_stream_consumer("topic://public/default/scaled", Arc::new(RejectingSchema))
        .subscription("surface-rejecting-batch-schema-sub")
        .consumer_name("surface-rejecting-batch-schema-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe rejecting batch-schema aggregate");
    cluster
        .wait_for("rejecting batch-schema children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&rejecting_batch_consumer).await;
    cluster
        .update(|fake| fake.enqueue_message(1, Bytes::from_static(b"reject-batch-one")))
        .expect("enqueue first rejecting batch message");
    cluster
        .update(|fake| fake.enqueue_message(2, Bytes::from_static(b"reject-batch-two")))
        .expect("enqueue second rejecting batch message");
    cluster
        .wait_for("rejecting batch-schema deliveries", |fake| {
            fake.resource_counts().unacked_messages == 2
        })
        .await;
    assert!(matches!(
        rejecting_batch_consumer
            .receive_batch(
                BatchReceivePolicy::messages(2, Duration::from_secs(1))
                    .expect("valid rejecting batch policy"),
            )
            .await
            .expect_err("batch schema rejection reaches the caller"),
        magnetar::scalable::StreamConsumerError::Schema(SchemaError::Decoding(_))
    ));
    rejecting_batch_consumer
        .close()
        .await
        .expect("close rejecting batch-schema aggregate");
    cluster
        .wait_for("rejecting batch-schema cleanup", |fake| {
            fake.resource_counts().child_consumers == 0
        })
        .await;

    let dropped_consumer = client
        .scalable_stream_consumer(
            "topic://public/default/scaled",
            Arc::new(BytesSchema::new()),
        )
        .subscription("surface-final-drop-sub")
        .consumer_name("surface-final-drop-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe final-drop aggregate");
    cluster
        .wait_for("final-drop children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&dropped_consumer).await;
    drop(dropped_consumer);
    cluster
        .wait_for("synchronous final-drop fencing", |fake| {
            let counts = fake.resource_counts();
            counts.layout_sessions == 0
                && counts.child_consumers == 0
                && counts.pending_operations == 0
                && counts.permits == 0
        })
        .await;
    let final_drop_fenced = true;

    let fencing_schema = Arc::new(AutoConsumeSchema::new());
    let fencing_consumer = client
        .scalable_stream_consumer("topic://public/default/scaled", fencing_schema)
        .subscription("surface-schema-fencing-sub")
        .consumer_name("surface-schema-fencing-member")
        .ordering_mode(magnetar::proto::OrderingMode::BrokerManaged)
        .receiver_budget(two_frame_receiver_budget())
        .subscribe()
        .await
        .expect("subscribe schema-fencing aggregate");
    cluster
        .wait_for("schema-fencing children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    wait_for_initial_flow(&fencing_consumer).await;
    cluster.hold_command(
        Endpoint::Segment(1),
        magnetar::proto::pb::base_command::Type::GetSchemaResponse,
    );
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.enqueue_message(1, Bytes::from_static(b"schema-fencing"))
        })
        .expect("enqueue schema-fencing message");
    let mut fencing_receive = Box::pin(fencing_consumer.receive());
    tokio::select! {
        biased;
        result = &mut fencing_receive => panic!("held fencing schema preparation completed early: {result:?}"),
        () = cluster.wait_for("held fencing schema preparation", |fake| {
            fake.routes().iter().any(|route| {
                route.endpoint == Endpoint::Segment(1)
                    && route.command == magnetar::proto::pb::base_command::Type::GetSchema
            })
        }) => {}
    }
    assert_eq!(
        cluster
            .update(|fake| fake.disconnect_endpoint(Endpoint::Controller))
            .expect("disconnect schema-fencing controller incarnation"),
        1
    );
    loop {
        match fencing_consumer
            .next_event()
            .await
            .expect("read schema-fencing event")
            .expect("schema-fencing aggregate remains open")
        {
            StreamConsumerEvent::ResyncRequired { .. } => break,
            StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected schema-fencing event: {unexpected:?}"),
        }
    }
    drop(fencing_receive);
    let schema_restoration_failure_resynced = loop {
        match fencing_consumer
            .next_event()
            .await
            .expect("read restoration-failure event")
            .expect("restoration-failure aggregate remains open")
        {
            StreamConsumerEvent::ResyncRequired { reason }
                if reason.contains("delivery restoration failed") =>
            {
                break true;
            }
            StreamConsumerEvent::ResyncRequired { .. }
            | StreamConsumerEvent::AssignmentApplied { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected restoration-failure event: {unexpected:?}"),
        }
    };
    cluster.release_command(
        Endpoint::Segment(1),
        magnetar::proto::pb::base_command::Type::GetSchemaResponse,
    );
    loop {
        match fencing_consumer
            .next_event()
            .await
            .expect("read schema-fencing replacement event")
            .expect("schema-fencing aggregate remains open during replacement")
        {
            StreamConsumerEvent::AssignmentApplied { .. } => break,
            StreamConsumerEvent::ResyncRequired { .. }
            | StreamConsumerEvent::SegmentPhaseChanged { .. }
            | StreamConsumerEvent::OrderingUnprovable { .. }
            | StreamConsumerEvent::TransactionOutcome { .. } => {}
            unexpected => panic!("unexpected schema-fencing replacement event: {unexpected:?}"),
        }
    }
    cluster
        .wait_for("schema-fencing replacement", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 2 && counts.pending_operations == 0
        })
        .await;
    fencing_consumer
        .close()
        .await
        .expect("close schema-fencing aggregate after replacement");
    cluster
        .wait_for("schema-fencing cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 0
                && counts.layout_sessions == 0
                && counts.unacked_messages == 0
        })
        .await;

    PublicSurfaceTrace {
        builder_debug,
        missing_subscription,
        empty_subscription,
        empty_consumer_name,
        consumer_debug,
        accessors,
        empty_batch,
        message_debug,
        position_acknowledgements,
        batch_acknowledged,
        stale_position_token,
        cumulative_transaction_registrations,
        cumulative_transaction_acks,
        cumulative_transaction_committed,
        position_transaction_committed,
        individual_transaction_aborted,
        transaction_outcome_event,
        aborted_outcome_event,
        closed_event,
        closed_stream,
        default_name_generated,
        broker_schema_resolved,
        broker_schema_lookups,
        schema_prepare_failures_nacked,
        schema_cancellation_restored,
        schema_restoration_failure_resynced,
        decode_failure_nacked,
        final_drop_fenced,
        after_close: cluster.inspect(M1FakeCluster::resource_counts),
    }
}

async fn run_tokio_public_surface() -> PublicSurfaceTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio(&cluster).await;
    let trace = observe_public_surface(&client, &cluster).await;
    client.close().await;
    trace
}

async fn run_moonpool_public_surface() -> PublicSurfaceTrace {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool(&cluster).await;
    let trace = observe_public_surface(&client, &cluster).await;
    client.close().await;
    trace
}

async fn observe_tokio_keepalive_tick() -> bool {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_tokio_with_keepalive(&cluster, Duration::from_millis(10)).await;
    cluster
        .wait_for("Tokio driver keepalive tick", |fake| {
            fake.routes()
                .iter()
                .any(|route| route.command == magnetar::proto::pb::base_command::Type::Ping)
        })
        .await;
    let observed = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .any(|route| route.command == magnetar::proto::pb::base_command::Type::Ping)
    });
    client.close().await;
    observed
}

async fn observe_moonpool_keepalive_tick() -> bool {
    let cluster = M1SocketCluster::bind().await;
    let client = connect_moonpool_with_keepalive(&cluster, Duration::from_millis(10)).await;
    cluster
        .wait_for("Moonpool driver keepalive tick", |fake| {
            fake.routes()
                .iter()
                .any(|route| route.command == magnetar::proto::pb::base_command::Type::Ping)
        })
        .await;
    let observed = cluster.inspect(|fake| {
        fake.routes()
            .iter()
            .any(|route| route.command == magnetar::proto::pb::base_command::Type::Ping)
    });
    client.close().await;
    observed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_stream_consumer_baseline_is_equivalent() {
    let tokio = run_tokio_baseline().await;
    let moonpool = run_moonpool_baseline().await;
    assert_eq!(tokio, moonpool, "public aggregate traces diverged");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn driver_keepalive_timer_ticks_are_equivalent() {
    assert!(observe_tokio_keepalive_tick().await);
    assert!(observe_moonpool_keepalive_tick().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_and_batch_receive_are_equivalent() {
    let tokio = run_tokio_receive_matrix().await;
    let moonpool = run_moonpool_receive_matrix().await;
    assert_eq!(tokio, moonpool, "receive traces diverged");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_receiver_budget_is_equivalent() {
    let tokio = run_tokio_budget().await;
    let moonpool = run_moonpool_budget().await;
    assert_eq!(tokio, moonpool, "receiver budget traces diverged");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn descriptor_change_and_in_flight_fence_are_equivalent() {
    let tokio = run_tokio_assignment().await;
    let moonpool = run_moonpool_assignment().await;
    assert_eq!(tokio, moonpool, "assignment/fence traces diverged");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segment_reconnect_is_equivalent() {
    let tokio = run_tokio_reconnect().await;
    let moonpool = run_moonpool_reconnect().await;
    assert_eq!(tokio, moonpool, "segment reconnect traces diverged");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledgement_failure_and_retry_are_equivalent() {
    let tokio = run_tokio_acknowledgement_failure().await;
    let moonpool = run_moonpool_acknowledgement_failure().await;
    assert_eq!(tokio, moonpool, "acknowledgement traces diverged");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negative_acknowledgement_redelivery_is_equivalent() {
    let tokio = run_tokio_negative_acknowledgement().await;
    let moonpool = run_moonpool_negative_acknowledgement().await;
    assert_eq!(tokio, moonpool, "negative acknowledgement traces diverged");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_close_wakes_pending_receive_equivalently() {
    let tokio = run_tokio_close_cancellation().await;
    let moonpool = run_moonpool_close_cancellation().await;
    assert_eq!(tokio, moonpool, "global close traces diverged");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_builder_position_transaction_and_drop_surfaces_are_equivalent() {
    let tokio = run_tokio_public_surface().await;
    let moonpool = run_moonpool_public_surface().await;
    assert_eq!(tokio, moonpool, "public edge-surface traces diverged");
    assert!(tokio.builder_debug);
    assert!(tokio.missing_subscription);
    assert!(tokio.empty_subscription);
    assert!(tokio.empty_consumer_name);
    assert!(tokio.consumer_debug);
    assert!(tokio.empty_batch);
    assert!(tokio.message_debug);
    assert_eq!(tokio.position_acknowledgements, 2);
    assert!(tokio.batch_acknowledged);
    assert!(tokio.stale_position_token);
    assert_eq!(tokio.cumulative_transaction_registrations, 2);
    assert_eq!(tokio.cumulative_transaction_acks, 2);
    assert!(tokio.cumulative_transaction_committed);
    assert!(tokio.position_transaction_committed);
    assert!(tokio.individual_transaction_aborted);
    assert!(tokio.transaction_outcome_event);
    assert!(tokio.aborted_outcome_event);
    assert!(tokio.closed_event);
    assert!(tokio.closed_stream);
    assert!(tokio.default_name_generated);
    assert!(tokio.broker_schema_resolved);
    assert_eq!(tokio.broker_schema_lookups, 1);
    assert!(tokio.decode_failure_nacked);
    assert!(tokio.final_drop_fenced);
}

#[test]
fn public_batch_and_acknowledgement_failure_values_are_typed() {
    assert_eq!(
        BatchReceivePolicy::new(0, 1, Duration::ZERO),
        Err(magnetar::scalable::BatchReceivePolicyError::ZeroMessages)
    );
    assert_eq!(
        BatchReceivePolicy::new(1, 0, Duration::ZERO),
        Err(magnetar::scalable::BatchReceivePolicyError::ZeroBytes)
    );
    let policy = BatchReceivePolicy::messages(3, Duration::from_millis(4))
        .expect("valid public batch policy");
    assert_eq!(policy.max_messages(), 3);
    assert_eq!(policy.max_bytes(), usize::MAX);
    assert_eq!(policy.max_wait(), Duration::from_millis(4));

    let source = magnetar::proto::SegmentSource::new(
        magnetar::proto::SegmentId(1),
        "segment://public/default/scaled/0000-7fff-1".to_owned(),
    )
    .expect("canonical public failure source");
    let position = magnetar::proto::StreamMessageId::new(
        source,
        magnetar::proto::MessageId {
            ledger_id: 1,
            entry_id: 2,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
        },
    )
    .expect("valid public failure position");
    let model = magnetar::scalable::StreamAckFailure::model(
        position.clone(),
        magnetar::proto::StreamConsumerModelError::StaleDeliveryToken,
    );
    assert_eq!(model.position(), &position);
    assert!(matches!(
        model.error(),
        magnetar::scalable::StreamAckFailureReason::Model(
            magnetar::proto::StreamConsumerModelError::StaleDeliveryToken
        )
    ));
    let engine =
        magnetar::scalable::StreamAckFailure::engine(position.clone(), "test", "component failed");
    assert_eq!(engine.position(), &position);
    assert_eq!(
        engine.error().to_string(),
        "test engine error: component failed"
    );
    assert_eq!(
        magnetar::scalable::StreamConsumerError::engine("test", "aggregate failed").to_string(),
        "test stream-consumer error: aggregate failed"
    );
}
