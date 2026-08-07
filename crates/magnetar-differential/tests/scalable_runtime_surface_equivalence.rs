// SPDX-License-Identifier: Apache-2.0

//! Owned low-level scalable-runtime capability parity over the stateful M1 fake.

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]
#![allow(clippy::struct_excessive_bools)]
#![allow(clippy::too_many_lines)]

#[allow(dead_code)]
#[path = "stream_consumer_support/server.rs"]
mod server;

use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_fakes::m1::{
    BrokerFailure, Endpoint, M1FakeCluster, M1Segment, OperationKind, PendingCompletion,
    ScriptedBehavior,
};
use magnetar_proto::{ConnectionConfig, ControllerIncarnation, RedirectUrlAllowList};
use moonpool_core::TokioProviders;
use server::M1SocketCluster;

const TOPIC: &str = "topic://public/default/scaled";

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeSurfaceTrace {
    subscriber_debug: bool,
    task_debug: bool,
    task_completed: bool,
    route_keys: Vec<(String, u64, u64)>,
    route_errors: Vec<String>,
    dag_session_id: u64,
    resolved_topic: Option<String>,
    layout_epoch: u64,
    consumer_id: u64,
    controller_incarnation: u64,
    assigned_segments: Vec<u64>,
    registration_topic: String,
    subscription: String,
    consumer_name: String,
    mismatched_source_rejected: bool,
    missing_authority_rejected: bool,
    wrong_scheme_rejected: bool,
    disallowed_authority_rejected: bool,
    child_topic: String,
    replacement_rejected: bool,
    route_tombstones_bounded: bool,
    direct_aggregate_debug: bool,
    zero_message_batch: bool,
    zero_byte_batch: bool,
    cleaned_up: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeFailureTrace {
    lookup_rejected: bool,
    lookup_timed_out: bool,
    lookup_pending_cancelled: bool,
    controller_rejected: bool,
    controller_timed_out: bool,
    controller_pending_cancelled: bool,
    segment_timed_out: bool,
    missing_controller_authority_reused_bootstrap: bool,
    proxy_target_rejected: bool,
    close_failure_recovered: bool,
    plain_driver_closed: bool,
    route_overflowed: bool,
    route_connection_closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeTransactionTrace {
    failed_registration_cached: bool,
    repeated_outcome_completed: bool,
    conflicting_outcome_rejected: bool,
    close_during_registration_fenced: bool,
    closed_outcome_rejected: bool,
    cleaned_up: bool,
}

fn supervised_config() -> ConnectionConfig {
    ConnectionConfig {
        operation_timeout: Duration::from_secs(2),
        supervisor: Some(magnetar_proto::SupervisorConfig::default()),
        redirect_url_allow_list: Some(RedirectUrlAllowList::Hosts(vec!["127.0.0.1".to_owned()])),
        ..ConnectionConfig::default()
    }
}

fn unchanged_layout() -> Vec<M1Segment> {
    vec![
        M1Segment::active(1, 0, 32_767, Endpoint::Segment(1), 0),
        M1Segment::active(2, 32_768, 65_535, Endpoint::Segment(2), 0),
    ]
}

fn route_error_strings_tokio() -> Vec<String> {
    use magnetar_runtime_tokio::ScalableRouteError;

    vec![
        ScalableRouteError::Overflow { capacity: 64 },
        ScalableRouteError::ConnectionReplaced,
        ScalableRouteError::ConnectionClosed,
        ScalableRouteError::Closed,
    ]
    .into_iter()
    .map(|error| error.to_string())
    .collect()
}

fn route_error_strings_moonpool() -> Vec<String> {
    use magnetar_runtime_moonpool::ScalableRouteError;

    vec![
        ScalableRouteError::Overflow { capacity: 64 },
        ScalableRouteError::ConnectionReplaced,
        ScalableRouteError::ConnectionClosed,
        ScalableRouteError::Closed,
    ]
    .into_iter()
    .map(|error| error.to_string())
    .collect()
}

fn transaction_registration_command_count(fake: &M1FakeCluster) -> usize {
    fake.routes()
        .iter()
        .filter(|route| {
            route.command == magnetar_proto::pb::base_command::Type::AddSubscriptionToTxn
        })
        .count()
}

async fn run_tokio_surface(cluster: &M1SocketCluster) -> RuntimeSurfaceTrace {
    let client =
        magnetar_runtime_tokio::Client::connect(cluster.controller_url(), supervised_config())
            .await
            .expect("connect Tokio runtime client");
    let subscriber = client
        .segment_subscriber()
        .expect("Tokio segment subscriber");
    let subscriber_debug = format!("{subscriber:?}").contains("SegmentSubscriber");
    subscriber.sleep(Duration::ZERO).await;

    let task = subscriber.spawn_task(async {});
    let task_debug = format!("{task:?}").contains("ScalableTaskHandle");
    task.join()
        .await
        .expect("join completed Tokio scalable task");
    let mut aborted = subscriber.spawn_task(std::future::pending());
    aborted.abort();
    aborted.abort();
    aborted
        .join()
        .await
        .expect("joining an already-aborted Tokio task is idempotent");

    let direct_aggregate = subscriber
        .subscribe_stream_consumer(magnetar_runtime_tokio::StreamConsumerOptions {
            topic: TOPIC.to_owned(),
            subscription: "runtime-zero-batch-sub".to_owned(),
            consumer_name: "runtime-zero-batch-member".to_owned(),
            schema: magnetar_proto::pb::Schema::default(),
            receiver_budget: magnetar_proto::ReceiverBudget::bytes(16 * 1024 * 1024)
                .expect("valid direct aggregate budget"),
            ordering_mode: magnetar_proto::OrderingMode::BrokerManaged,
        })
        .await
        .expect("open direct Tokio aggregate");
    cluster
        .wait_for("direct Tokio aggregate children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    let direct_aggregate_debug = format!("{direct_aggregate:?}").contains("StreamConsumer");
    let zero_message_batch = direct_aggregate
        .receive_batch(0, 1, Duration::ZERO)
        .await
        .expect("zero-message Tokio batch")
        .is_empty();
    let zero_byte_batch = direct_aggregate
        .receive_batch(1, 0, Duration::ZERO)
        .await
        .expect("zero-byte Tokio batch")
        .is_empty();
    cluster
        .wait_for("Tokio restoration permit", |fake| {
            fake.resource_counts().permits == 1
        })
        .await;
    let restore_segment = cluster.inspect(|fake| {
        if fake.segment_permits("runtime-zero-batch-sub", 1) > 0 {
            1
        } else {
            2
        }
    });
    cluster
        .update(|fake| fake.enqueue_message(restore_segment, bytes::Bytes::from_static(b"restore")))
        .expect("enqueue Tokio restoration delivery");
    cluster
        .wait_for("Tokio restoration delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let restored = direct_aggregate
        .receive()
        .await
        .expect("receive Tokio restoration delivery");
    let restored_sequence = restored.token.dequeue_sequence();
    direct_aggregate
        .restore_deliveries(vec![restored])
        .expect("restore Tokio delivery");
    let restored = direct_aggregate
        .receive()
        .await
        .expect("receive restored Tokio delivery");
    assert_eq!(restored.token.dequeue_sequence(), restored_sequence);
    direct_aggregate
        .acknowledge(&restored.token)
        .await
        .expect("acknowledge restored Tokio delivery");
    direct_aggregate
        .close()
        .await
        .expect("close direct Tokio aggregate");
    cluster
        .wait_for("direct Tokio aggregate cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.layout_sessions == 0 && counts.child_consumers == 0
        })
        .await;

    let dag_key = magnetar_runtime_tokio::ScalableRouteKey::dag(7, ControllerIncarnation(9));
    let consumer_key =
        magnetar_runtime_tokio::ScalableRouteKey::consumer(8, ControllerIncarnation(10));
    let route_keys = [dag_key, consumer_key]
        .into_iter()
        .map(|key| (format!("{:?}", key.family()), key.id(), key.incarnation().0))
        .collect();

    let dag = subscriber
        .open_dag_session(TOPIC)
        .await
        .expect("open Tokio DAG session");
    let dag_session_id = dag.session_id();
    let resolved_topic = dag.resolved_topic_name().map(str::to_owned);
    let layout_epoch = dag.snapshot().epoch();
    let controller = subscriber
        .open_controller_session(&dag, "runtime-surface-sub", "runtime-surface-member")
        .await
        .expect("open Tokio controller session");
    let consumer_id = controller.consumer_id();
    let controller_incarnation = controller.incarnation().0;
    let assigned_segments = controller
        .assignment()
        .segments()
        .iter()
        .map(|segment| segment.segment_id().0)
        .collect::<Vec<_>>();
    let registration_topic = controller.registration_topic().to_owned();
    let subscription = controller.subscription().to_owned();
    let consumer_name = controller.consumer_name().to_owned();
    let source = controller.assignment().segments()[0].source();
    let descriptor = dag
        .snapshot()
        .segment(source.segment_id())
        .expect("assigned descriptor")
        .clone();
    let options = magnetar_runtime_tokio::SegmentConsumerOptions {
        subscription: subscription.clone(),
        consumer_name: consumer_name.clone(),
        schema: magnetar_proto::pb::Schema::default(),
    };

    let mut mismatched = descriptor.clone();
    mismatched.key_range = dag
        .snapshot()
        .segments()
        .iter()
        .find(|candidate| candidate.segment_id != source.segment_id())
        .expect("second segment descriptor")
        .key_range;
    let mismatched_source_rejected = matches!(
        subscriber
            .open_segment_consumer(&source, &mismatched, &options)
            .await,
        Err(magnetar_runtime_tokio::ClientError::Other(_))
    );
    let mut missing = descriptor.clone();
    missing.broker_url = None;
    let missing_authority_rejected = matches!(
        subscriber
            .open_segment_consumer(&source, &missing, &options)
            .await,
        Err(magnetar_runtime_tokio::ClientError::ControllerUnavailable)
    );
    let mut wrong_scheme = descriptor.clone();
    wrong_scheme.broker_url = descriptor.broker_url_tls.clone();
    let wrong_scheme_rejected = matches!(
        subscriber
            .open_segment_consumer(&source, &wrong_scheme, &options)
            .await,
        Err(magnetar_runtime_tokio::ClientError::ControllerRoutingUnsupported { .. })
    );
    let mut disallowed = descriptor.clone();
    disallowed.broker_url = Some("pulsar://outside.example:6650".to_owned());
    let disallowed_authority_rejected = matches!(
        subscriber
            .open_segment_consumer(&source, &disallowed, &options)
            .await,
        Err(magnetar_runtime_tokio::ClientError::ScalableAuthorityRejected)
    );

    let child = subscriber
        .open_segment_consumer(&source, &descriptor, &options)
        .await
        .expect("open valid Tokio segment consumer");
    let child_topic = child.topic();
    child.close().await.expect("close Tokio segment consumer");
    let replacement_rejected = matches!(
        subscriber.reopen_controller_session(&dag, controller).await,
        Err(magnetar_runtime_tokio::ClientError::ScalableAssignmentRejected { .. })
    );
    dag.close();
    for _ in 0..=70 {
        subscriber
            .open_dag_session(TOPIC)
            .await
            .expect("open Tokio route-retirement session")
            .close();
    }
    let route_tombstones_bounded = true;
    client.close().await;
    cluster
        .wait_for("Tokio runtime-surface cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.connections == 0 && counts.child_consumers == 0 && counts.pending_operations == 0
        })
        .await;
    let cleaned_up = cluster.inspect(|fake| {
        let counts = fake.resource_counts();
        counts.connections == 0 && counts.child_consumers == 0 && counts.pending_operations == 0
    });

    RuntimeSurfaceTrace {
        subscriber_debug,
        task_debug,
        task_completed: true,
        route_keys,
        route_errors: route_error_strings_tokio(),
        dag_session_id,
        resolved_topic,
        layout_epoch,
        consumer_id,
        controller_incarnation,
        assigned_segments,
        registration_topic,
        subscription,
        consumer_name,
        mismatched_source_rejected,
        missing_authority_rejected,
        wrong_scheme_rejected,
        disallowed_authority_rejected,
        child_topic,
        replacement_rejected,
        route_tombstones_bounded,
        direct_aggregate_debug,
        zero_message_batch,
        zero_byte_batch,
        cleaned_up,
    }
}

async fn run_moonpool_surface(cluster: &M1SocketCluster) -> RuntimeSurfaceTrace {
    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let address = cluster
        .controller_url()
        .strip_prefix("pulsar://")
        .expect("plaintext controller URL");
    let client = magnetar_runtime_moonpool::Client::connect_plain_supervised(
        &engine,
        address,
        supervised_config(),
        None,
        None,
    )
    .await
    .expect("connect Moonpool runtime client");
    let subscriber = client
        .segment_subscriber()
        .expect("Moonpool segment subscriber");
    let subscriber_debug = format!("{subscriber:?}").contains("SegmentSubscriber");
    subscriber
        .sleep(Duration::ZERO)
        .await
        .expect("Moonpool zero-duration sleep");

    let task = subscriber.spawn_task("surface-complete", async {});
    let task_debug = format!("{task:?}").contains("ScalableTaskHandle");
    task.join()
        .await
        .expect("join completed Moonpool scalable task");

    let direct_aggregate = subscriber
        .subscribe_stream_consumer(magnetar_runtime_moonpool::StreamConsumerOptions {
            topic: TOPIC.to_owned(),
            subscription: "runtime-zero-batch-sub".to_owned(),
            consumer_name: "runtime-zero-batch-member".to_owned(),
            schema: magnetar_proto::pb::Schema::default(),
            receiver_budget: magnetar_proto::ReceiverBudget::bytes(16 * 1024 * 1024)
                .expect("valid direct aggregate budget"),
            ordering_mode: magnetar_proto::OrderingMode::BrokerManaged,
        })
        .await
        .expect("open direct Moonpool aggregate");
    cluster
        .wait_for("direct Moonpool aggregate children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;
    let direct_aggregate_debug = format!("{direct_aggregate:?}").contains("StreamConsumer");
    let zero_message_batch = direct_aggregate
        .receive_batch(0, 1, Duration::ZERO)
        .await
        .expect("zero-message Moonpool batch")
        .is_empty();
    let zero_byte_batch = direct_aggregate
        .receive_batch(1, 0, Duration::ZERO)
        .await
        .expect("zero-byte Moonpool batch")
        .is_empty();
    cluster
        .wait_for("Moonpool restoration permit", |fake| {
            fake.resource_counts().permits == 1
        })
        .await;
    let restore_segment = cluster.inspect(|fake| {
        if fake.segment_permits("runtime-zero-batch-sub", 1) > 0 {
            1
        } else {
            2
        }
    });
    cluster
        .update(|fake| fake.enqueue_message(restore_segment, bytes::Bytes::from_static(b"restore")))
        .expect("enqueue Moonpool restoration delivery");
    cluster
        .wait_for("Moonpool restoration delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let restored = direct_aggregate
        .receive()
        .await
        .expect("receive Moonpool restoration delivery");
    let restored_sequence = restored.token.dequeue_sequence();
    direct_aggregate
        .restore_deliveries(vec![restored])
        .expect("restore Moonpool delivery");
    let restored = direct_aggregate
        .receive()
        .await
        .expect("receive restored Moonpool delivery");
    assert_eq!(restored.token.dequeue_sequence(), restored_sequence);
    direct_aggregate
        .acknowledge(&restored.token)
        .await
        .expect("acknowledge restored Moonpool delivery");
    direct_aggregate
        .close()
        .await
        .expect("close direct Moonpool aggregate");
    cluster
        .wait_for("direct Moonpool aggregate cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.layout_sessions == 0 && counts.child_consumers == 0
        })
        .await;

    let dag_key = magnetar_runtime_moonpool::ScalableRouteKey::dag(7, ControllerIncarnation(9));
    let consumer_key =
        magnetar_runtime_moonpool::ScalableRouteKey::consumer(8, ControllerIncarnation(10));
    let route_keys = [dag_key, consumer_key]
        .into_iter()
        .map(|key| (format!("{:?}", key.family()), key.id(), key.incarnation().0))
        .collect();

    let dag = subscriber
        .open_dag_session(TOPIC)
        .await
        .expect("open Moonpool DAG session");
    let dag_session_id = dag.session_id();
    let resolved_topic = dag.resolved_topic_name().map(str::to_owned);
    let layout_epoch = dag.snapshot().epoch();
    let controller = subscriber
        .open_controller_session(&dag, "runtime-surface-sub", "runtime-surface-member")
        .await
        .expect("open Moonpool controller session");
    let consumer_id = controller.consumer_id();
    let controller_incarnation = controller.incarnation().0;
    let assigned_segments = controller
        .assignment()
        .segments()
        .iter()
        .map(|segment| segment.segment_id().0)
        .collect::<Vec<_>>();
    let registration_topic = controller.registration_topic().to_owned();
    let subscription = controller.subscription().to_owned();
    let consumer_name = controller.consumer_name().to_owned();
    let source = controller.assignment().segments()[0].source();
    let descriptor = dag
        .snapshot()
        .segment(source.segment_id())
        .expect("assigned descriptor")
        .clone();
    let options = magnetar_runtime_moonpool::SegmentConsumerOptions {
        subscription: subscription.clone(),
        consumer_name: consumer_name.clone(),
        schema: magnetar_proto::pb::Schema::default(),
    };

    let mut mismatched = descriptor.clone();
    mismatched.key_range = dag
        .snapshot()
        .segments()
        .iter()
        .find(|candidate| candidate.segment_id != source.segment_id())
        .expect("second segment descriptor")
        .key_range;
    let mismatched_source_rejected = matches!(
        subscriber
            .open_segment_consumer(&source, &mismatched, &options)
            .await,
        Err(magnetar_runtime_moonpool::ClientError::Other(_))
    );
    let mut missing = descriptor.clone();
    missing.broker_url = None;
    let missing_authority_rejected = matches!(
        subscriber
            .open_segment_consumer(&source, &missing, &options)
            .await,
        Err(magnetar_runtime_moonpool::ClientError::ControllerUnavailable)
    );
    let mut wrong_scheme = descriptor.clone();
    wrong_scheme.broker_url = descriptor.broker_url_tls.clone();
    let wrong_scheme_rejected = matches!(
        subscriber
            .open_segment_consumer(&source, &wrong_scheme, &options)
            .await,
        Err(magnetar_runtime_moonpool::ClientError::ControllerRoutingUnsupported { .. })
    );
    let mut disallowed = descriptor.clone();
    disallowed.broker_url = Some("pulsar://outside.example:6650".to_owned());
    let disallowed_authority_rejected = matches!(
        subscriber
            .open_segment_consumer(&source, &disallowed, &options)
            .await,
        Err(magnetar_runtime_moonpool::ClientError::ScalableAuthorityRejected)
    );

    let child = subscriber
        .open_segment_consumer(&source, &descriptor, &options)
        .await
        .expect("open valid Moonpool segment consumer");
    let child_topic = child.topic();
    child
        .close()
        .await
        .expect("close Moonpool segment consumer");
    let replacement_rejected = matches!(
        subscriber.reopen_controller_session(&dag, controller).await,
        Err(magnetar_runtime_moonpool::ClientError::ScalableAssignmentRejected { .. })
    );
    dag.close();
    for _ in 0..=70 {
        subscriber
            .open_dag_session(TOPIC)
            .await
            .expect("open Moonpool route-retirement session")
            .close();
    }
    let route_tombstones_bounded = true;
    client.close().await;
    cluster
        .wait_for("Moonpool runtime-surface cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.connections == 0 && counts.child_consumers == 0 && counts.pending_operations == 0
        })
        .await;
    let cleaned_up = cluster.inspect(|fake| {
        let counts = fake.resource_counts();
        counts.connections == 0 && counts.child_consumers == 0 && counts.pending_operations == 0
    });

    RuntimeSurfaceTrace {
        subscriber_debug,
        task_debug,
        task_completed: true,
        route_keys,
        route_errors: route_error_strings_moonpool(),
        dag_session_id,
        resolved_topic,
        layout_epoch,
        consumer_id,
        controller_incarnation,
        assigned_segments,
        registration_topic,
        subscription,
        consumer_name,
        mismatched_source_rejected,
        missing_authority_rejected,
        wrong_scheme_rejected,
        disallowed_authority_rejected,
        child_topic,
        replacement_rejected,
        route_tombstones_bounded,
        direct_aggregate_debug,
        zero_message_batch,
        zero_byte_batch,
        cleaned_up,
    }
}

async fn run_tokio_failures() -> RuntimeFailureTrace {
    let lookup_cluster = M1SocketCluster::bind().await;
    let mut short_config = supervised_config();
    short_config.operation_timeout = Duration::from_millis(250);
    let lookup_client =
        magnetar_runtime_tokio::Client::connect(lookup_cluster.controller_url(), short_config)
            .await
            .expect("connect Tokio lookup-failure client");
    let lookup_subscriber = lookup_client
        .segment_subscriber()
        .expect("Tokio lookup-failure subscriber");
    let lookup_rejected = matches!(
        lookup_subscriber
            .open_dag_session("topic://public/default/missing")
            .await,
        Err(magnetar_runtime_tokio::ClientError::Other(_))
    );
    lookup_cluster.hold_command(
        Endpoint::Controller,
        magnetar_proto::pb::base_command::Type::ScalableTopicUpdate,
    );
    let lookup_timed_out = matches!(
        lookup_subscriber.open_dag_session(TOPIC).await,
        Err(magnetar_runtime_tokio::ClientError::Timeout(_))
    );
    lookup_cluster.release_command(
        Endpoint::Controller,
        magnetar_proto::pb::base_command::Type::ScalableTopicUpdate,
    );
    lookup_client.close().await;
    lookup_cluster
        .wait_for("Tokio timed-out lookup cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.connections == 0 && counts.pending_operations == 0 && counts.layout_sessions == 0
        })
        .await;
    let lookup_pending_cancelled = true;
    lookup_cluster.assert_healthy();

    let controller_cluster = M1SocketCluster::bind().await;
    let mut controller_config = supervised_config();
    controller_config.operation_timeout = Duration::from_millis(250);
    let controller_client = magnetar_runtime_tokio::Client::connect(
        controller_cluster.controller_url(),
        controller_config,
    )
    .await
    .expect("connect Tokio controller-failure client");
    let controller_subscriber = controller_client
        .segment_subscriber()
        .expect("Tokio controller-failure subscriber");
    let dag = controller_subscriber
        .open_dag_session(TOPIC)
        .await
        .expect("open Tokio failure-matrix DAG");
    controller_cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Controller,
                OperationKind::ScalableOpen,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar_proto::pb::ServerError::NotAllowedError,
                    "scripted controller rejection",
                )),
            )
        })
        .expect("reject Tokio controller registration");
    let controller_error = controller_subscriber
        .open_controller_session(&dag, "failure-sub", "rejected-member")
        .await
        .expect_err("Tokio controller registration is rejected");
    let controller_rejected = matches!(
        controller_error,
        magnetar_runtime_tokio::ClientError::ScalableAssignmentRejected { .. }
    );
    controller_cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Controller,
                OperationKind::ScalableOpen,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay Tokio controller registration");
    let controller_timed_out = matches!(
        controller_subscriber
            .open_controller_session(&dag, "failure-sub", "delayed-member")
            .await,
        Err(magnetar_runtime_tokio::ClientError::Timeout(_))
    );
    let pending_controller = controller_cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::ScalableOpen)
                .map(|pending| pending.id)
        })
        .expect("timed-out Tokio controller operation remains observable");
    controller_cluster
        .update(|fake| {
            fake.complete_pending(
                pending_controller,
                PendingCompletion::Fail(BrokerFailure::new(
                    magnetar_proto::pb::ServerError::ServiceNotReady,
                    "timed-out controller registration",
                )),
            )
        })
        .expect("settle timed-out Tokio controller operation");
    let controller = controller_subscriber
        .open_controller_session(&dag, "failure-sub", "segment-timeout-member")
        .await
        .expect("open Tokio controller for child timeout");
    let source = controller.assignment().segments()[0].source();
    let descriptor = dag
        .snapshot()
        .segment(source.segment_id())
        .expect("timed-out Tokio child descriptor")
        .clone();
    controller_cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Segment(source.segment_id().0),
                OperationKind::SegmentOpen,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay Tokio child subscribe");
    let segment_timed_out = matches!(
        controller_subscriber
            .open_segment_consumer(
                &source,
                &descriptor,
                &magnetar_runtime_tokio::SegmentConsumerOptions {
                    subscription: "failure-sub".to_owned(),
                    consumer_name: "segment-timeout-member".to_owned(),
                    schema: magnetar_proto::pb::Schema::default(),
                },
            )
            .await,
        Err(magnetar_runtime_tokio::ClientError::Timeout(_))
    );
    controller.close();
    dag.close();
    controller_client.close().await;
    controller_cluster
        .wait_for("Tokio timed-out controller cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.connections == 0
                && counts.pending_operations == 0
                && counts.scalable_members == 0
        })
        .await;
    let controller_pending_cancelled = true;
    controller_cluster.assert_healthy();

    let fallback_cluster = M1SocketCluster::bind_without_controller_authority().await;
    let fallback_client = magnetar_runtime_tokio::Client::connect(
        fallback_cluster.controller_url(),
        supervised_config(),
    )
    .await
    .expect("connect Tokio controller-fallback client");
    let fallback_subscriber = fallback_client
        .segment_subscriber()
        .expect("Tokio controller-fallback subscriber");
    let fallback_dag = fallback_subscriber
        .open_dag_session(TOPIC)
        .await
        .expect("open Tokio controller-fallback DAG");
    let fallback_controller = fallback_subscriber
        .open_controller_session(&fallback_dag, "fallback-sub", "fallback-member")
        .await
        .expect("reuse Tokio direct bootstrap for controller registration");
    let missing_controller_authority_reused_bootstrap =
        fallback_cluster.inspect(|fake| fake.resource_counts().connections == 1);
    fallback_controller.close();
    fallback_dag.close();
    fallback_client.close().await;
    fallback_cluster
        .wait_for("Tokio controller-fallback cleanup", |fake| {
            fake.resource_counts().connections == 0
        })
        .await;
    fallback_cluster.assert_healthy();

    let proxy_broker = ScriptedBroker::bind()
        .await
        .expect("bind Tokio proxy broker");
    let mut proxy_config = supervised_config();
    proxy_config.proxy_to_broker_url = Some("pulsar://logical-controller:6650".to_owned());
    let proxy_client =
        magnetar_runtime_tokio::Client::connect(&proxy_broker.pulsar_url(), proxy_config)
            .await
            .expect("connect Tokio proxy-target client");
    let proxy_subscriber = proxy_client
        .segment_subscriber()
        .expect("Tokio proxy-target subscriber");
    let proxy_dag = proxy_subscriber
        .open_dag_session(TOPIC)
        .await
        .expect("open Tokio proxy-target DAG");
    let proxy_target_rejected = matches!(
        proxy_subscriber
            .open_controller_session(&proxy_dag, "proxy-sub", "proxy-member")
            .await,
        Err(magnetar_runtime_tokio::ClientError::ControllerRoutingUnsupported { .. })
    );
    proxy_dag.close();
    proxy_client.close().await;
    proxy_broker.shutdown().await;

    let close_cluster = M1SocketCluster::bind().await;
    let close_client = magnetar_runtime_tokio::Client::connect(
        close_cluster.controller_url(),
        supervised_config(),
    )
    .await
    .expect("connect Tokio close-failure client");
    let close_consumer = close_client
        .segment_subscriber()
        .expect("Tokio close-failure subscriber")
        .subscribe_stream_consumer(magnetar_runtime_tokio::StreamConsumerOptions {
            topic: TOPIC.to_owned(),
            subscription: "runtime-close-failure-sub".to_owned(),
            consumer_name: "runtime-close-failure-member".to_owned(),
            schema: magnetar_proto::pb::Schema::default(),
            receiver_budget: magnetar_proto::ReceiverBudget::bytes(16 * 1024 * 1024)
                .expect("valid close-failure budget"),
            ordering_mode: magnetar_proto::OrderingMode::BrokerManaged,
        })
        .await
        .expect("open Tokio close-failure aggregate");
    close_cluster
        .wait_for("Tokio close-failure children", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 2 && counts.permits > 0
        })
        .await;
    close_cluster
        .update(|fake| {
            for endpoint in [Endpoint::Segment(1), Endpoint::Segment(2)] {
                fake.script_next(
                    endpoint,
                    OperationKind::Close,
                    ScriptedBehavior::Fail(BrokerFailure::new(
                        magnetar_proto::pb::ServerError::PersistenceError,
                        "scripted child close failure",
                    )),
                )?;
            }
            Ok(())
        })
        .expect("script Tokio child close failures");
    let close_failure_recovered = close_consumer.close().await.is_err();
    assert!(close_failure_recovered);
    close_cluster
        .wait_for("Tokio forced close cleanup", |fake| {
            fake.resource_counts().child_consumers == 0
        })
        .await;
    close_client.close().await;
    close_cluster
        .wait_for("Tokio close-failure client cleanup", |fake| {
            fake.resource_counts().connections == 0
        })
        .await;
    close_cluster.assert_healthy();

    let plain_cluster = M1SocketCluster::bind().await;
    let plain_socket = tokio::net::TcpStream::connect(
        plain_cluster
            .controller_url()
            .strip_prefix("pulsar://")
            .expect("plaintext plain-driver URL"),
    )
    .await
    .expect("dial plain Tokio shutdown socket");
    let plain_client = magnetar_runtime_tokio::Client::from_socket(
        plain_socket,
        ConnectionConfig {
            supervisor: None,
            ..ConnectionConfig::default()
        },
    )
    .await
    .expect("connect plain Tokio shutdown client");
    plain_client.close().await;
    plain_cluster
        .wait_for("plain Tokio driver cleanup", |fake| {
            fake.resource_counts().connections == 0
        })
        .await;
    let plain_driver_closed = true;
    plain_cluster.assert_healthy();

    let route_cluster = M1SocketCluster::bind().await;
    let route_client = magnetar_runtime_tokio::Client::connect(
        route_cluster.controller_url(),
        supervised_config(),
    )
    .await
    .expect("connect Tokio route-bound client");
    let mut route_dag = route_client
        .segment_subscriber()
        .expect("Tokio route-bound subscriber")
        .open_dag_session(TOPIC)
        .await
        .expect("open Tokio route-bound DAG");
    for epoch in 2..=67 {
        route_cluster
            .update(|fake| fake.advance_layout(epoch, unchanged_layout()))
            .expect("push Tokio route-bound layout");
    }
    let barrier_txn = route_client
        .new_txn(Duration::from_secs(1))
        .await
        .expect("Tokio route-bound wire-order barrier");
    route_client
        .end_txn(barrier_txn, magnetar_proto::TxnAction::Abort)
        .await
        .expect("close Tokio route-bound barrier transaction");
    let route_overflowed = matches!(
        tokio::time::timeout(magnetar_differential::HANG_GUARD, route_dag.next())
            .await
            .expect("Tokio route overflow surfaced"),
        Err(magnetar_runtime_tokio::ClientError::ScalableRoute(
            magnetar_runtime_tokio::ScalableRouteError::Overflow { capacity: 64 }
        ))
    );
    assert!(route_overflowed);
    route_client.close().await;
    let route_connection_closed = matches!(
        route_dag.next().await,
        Err(magnetar_runtime_tokio::ClientError::ScalableRoute(
            magnetar_runtime_tokio::ScalableRouteError::ConnectionClosed
        ))
    );
    assert!(route_connection_closed);
    drop(route_dag);
    route_cluster
        .wait_for("Tokio route-bound cleanup", |fake| {
            fake.resource_counts().connections == 0
        })
        .await;
    route_cluster.assert_healthy();

    RuntimeFailureTrace {
        lookup_rejected,
        lookup_timed_out,
        lookup_pending_cancelled,
        controller_rejected,
        controller_timed_out,
        controller_pending_cancelled,
        segment_timed_out,
        missing_controller_authority_reused_bootstrap,
        proxy_target_rejected,
        close_failure_recovered,
        plain_driver_closed,
        route_overflowed,
        route_connection_closed,
    }
}

async fn run_moonpool_failures() -> RuntimeFailureTrace {
    let lookup_cluster = M1SocketCluster::bind().await;
    let mut short_config = supervised_config();
    short_config.operation_timeout = Duration::from_millis(250);
    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let address = lookup_cluster
        .controller_url()
        .strip_prefix("pulsar://")
        .expect("plaintext lookup controller URL");
    let lookup_client = magnetar_runtime_moonpool::Client::connect_plain_supervised(
        &engine,
        address,
        short_config,
        None,
        None,
    )
    .await
    .expect("connect Moonpool lookup-failure client");
    let lookup_subscriber = lookup_client
        .segment_subscriber()
        .expect("Moonpool lookup-failure subscriber");
    let lookup_rejected = matches!(
        lookup_subscriber
            .open_dag_session("topic://public/default/missing")
            .await,
        Err(magnetar_runtime_moonpool::ClientError::Other(_))
    );
    lookup_cluster.hold_command(
        Endpoint::Controller,
        magnetar_proto::pb::base_command::Type::ScalableTopicUpdate,
    );
    let lookup_timed_out = matches!(
        lookup_subscriber.open_dag_session(TOPIC).await,
        Err(magnetar_runtime_moonpool::ClientError::Other(_))
    );
    lookup_cluster.release_command(
        Endpoint::Controller,
        magnetar_proto::pb::base_command::Type::ScalableTopicUpdate,
    );
    lookup_client.close().await;
    lookup_cluster
        .wait_for("Moonpool timed-out lookup cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.connections == 0 && counts.pending_operations == 0 && counts.layout_sessions == 0
        })
        .await;
    let lookup_pending_cancelled = true;
    lookup_cluster.assert_healthy();

    let controller_cluster = M1SocketCluster::bind().await;
    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let address = controller_cluster
        .controller_url()
        .strip_prefix("pulsar://")
        .expect("plaintext controller-failure URL");
    let mut controller_config = supervised_config();
    controller_config.operation_timeout = Duration::from_millis(250);
    let controller_client = magnetar_runtime_moonpool::Client::connect_plain_supervised(
        &engine,
        address,
        controller_config,
        None,
        None,
    )
    .await
    .expect("connect Moonpool controller-failure client");
    let controller_subscriber = controller_client
        .segment_subscriber()
        .expect("Moonpool controller-failure subscriber");
    let dag = controller_subscriber
        .open_dag_session(TOPIC)
        .await
        .expect("open Moonpool failure-matrix DAG");
    controller_cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Controller,
                OperationKind::ScalableOpen,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar_proto::pb::ServerError::NotAllowedError,
                    "scripted controller rejection",
                )),
            )
        })
        .expect("reject Moonpool controller registration");
    let controller_error = controller_subscriber
        .open_controller_session(&dag, "failure-sub", "rejected-member")
        .await
        .expect_err("Moonpool controller registration is rejected");
    let controller_rejected = matches!(
        controller_error,
        magnetar_runtime_moonpool::ClientError::ScalableAssignmentRejected { .. }
    );
    controller_cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Controller,
                OperationKind::ScalableOpen,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay Moonpool controller registration");
    let controller_timed_out = matches!(
        controller_subscriber
            .open_controller_session(&dag, "failure-sub", "delayed-member")
            .await,
        Err(magnetar_runtime_moonpool::ClientError::Other(_))
    );
    let pending_controller = controller_cluster
        .inspect(|fake| {
            fake.pending_operations()
                .into_iter()
                .find(|pending| pending.kind == OperationKind::ScalableOpen)
                .map(|pending| pending.id)
        })
        .expect("timed-out Moonpool controller operation remains observable");
    controller_cluster
        .update(|fake| {
            fake.complete_pending(
                pending_controller,
                PendingCompletion::Fail(BrokerFailure::new(
                    magnetar_proto::pb::ServerError::ServiceNotReady,
                    "timed-out controller registration",
                )),
            )
        })
        .expect("settle timed-out Moonpool controller operation");
    let controller = controller_subscriber
        .open_controller_session(&dag, "failure-sub", "segment-timeout-member")
        .await
        .expect("open Moonpool controller for child timeout");
    let source = controller.assignment().segments()[0].source();
    let descriptor = dag
        .snapshot()
        .segment(source.segment_id())
        .expect("timed-out Moonpool child descriptor")
        .clone();
    controller_cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Segment(source.segment_id().0),
                OperationKind::SegmentOpen,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay Moonpool child subscribe");
    let segment_timed_out = matches!(
        controller_subscriber
            .open_segment_consumer(
                &source,
                &descriptor,
                &magnetar_runtime_moonpool::SegmentConsumerOptions {
                    subscription: "failure-sub".to_owned(),
                    consumer_name: "segment-timeout-member".to_owned(),
                    schema: magnetar_proto::pb::Schema::default(),
                },
            )
            .await,
        Err(magnetar_runtime_moonpool::ClientError::Other(_))
    );
    controller.close();
    dag.close();
    controller_client.close().await;
    controller_cluster
        .wait_for("Moonpool timed-out controller cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.connections == 0
                && counts.pending_operations == 0
                && counts.scalable_members == 0
        })
        .await;
    let controller_pending_cancelled = true;
    controller_cluster.assert_healthy();

    let fallback_cluster = M1SocketCluster::bind_without_controller_authority().await;
    let fallback_address = fallback_cluster
        .controller_url()
        .strip_prefix("pulsar://")
        .expect("plaintext controller-fallback URL");
    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let fallback_client = magnetar_runtime_moonpool::Client::connect_plain_supervised(
        &engine,
        fallback_address,
        supervised_config(),
        None,
        None,
    )
    .await
    .expect("connect Moonpool controller-fallback client");
    let fallback_subscriber = fallback_client
        .segment_subscriber()
        .expect("Moonpool controller-fallback subscriber");
    let fallback_dag = fallback_subscriber
        .open_dag_session(TOPIC)
        .await
        .expect("open Moonpool controller-fallback DAG");
    let fallback_controller = fallback_subscriber
        .open_controller_session(&fallback_dag, "fallback-sub", "fallback-member")
        .await
        .expect("reuse Moonpool direct bootstrap for controller registration");
    let missing_controller_authority_reused_bootstrap =
        fallback_cluster.inspect(|fake| fake.resource_counts().connections == 1);
    fallback_controller.close();
    fallback_dag.close();
    fallback_client.close().await;
    fallback_cluster
        .wait_for("Moonpool controller-fallback cleanup", |fake| {
            fake.resource_counts().connections == 0
        })
        .await;
    fallback_cluster.assert_healthy();

    let proxy_broker = ScriptedBroker::bind()
        .await
        .expect("bind Moonpool proxy broker");
    let mut proxy_config = supervised_config();
    proxy_config.proxy_to_broker_url = Some("pulsar://logical-controller:6650".to_owned());
    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let proxy_client = magnetar_runtime_moonpool::Client::connect_plain_supervised(
        &engine,
        &proxy_broker.host_port(),
        proxy_config,
        None,
        None,
    )
    .await
    .expect("connect Moonpool proxy-target client");
    let proxy_subscriber = proxy_client
        .segment_subscriber()
        .expect("Moonpool proxy-target subscriber");
    let proxy_dag = proxy_subscriber
        .open_dag_session(TOPIC)
        .await
        .expect("open Moonpool proxy-target DAG");
    let proxy_target_rejected = matches!(
        proxy_subscriber
            .open_controller_session(&proxy_dag, "proxy-sub", "proxy-member")
            .await,
        Err(magnetar_runtime_moonpool::ClientError::ControllerRoutingUnsupported { .. })
    );
    proxy_dag.close();
    proxy_client.close().await;
    proxy_broker.shutdown().await;

    let close_cluster = M1SocketCluster::bind().await;
    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let close_address = close_cluster
        .controller_url()
        .strip_prefix("pulsar://")
        .expect("plaintext close-failure URL");
    let close_client = magnetar_runtime_moonpool::Client::connect_plain_supervised(
        &engine,
        close_address,
        supervised_config(),
        None,
        None,
    )
    .await
    .expect("connect Moonpool close-failure client");
    let close_consumer = close_client
        .segment_subscriber()
        .expect("Moonpool close-failure subscriber")
        .subscribe_stream_consumer(magnetar_runtime_moonpool::StreamConsumerOptions {
            topic: TOPIC.to_owned(),
            subscription: "runtime-close-failure-sub".to_owned(),
            consumer_name: "runtime-close-failure-member".to_owned(),
            schema: magnetar_proto::pb::Schema::default(),
            receiver_budget: magnetar_proto::ReceiverBudget::bytes(16 * 1024 * 1024)
                .expect("valid close-failure budget"),
            ordering_mode: magnetar_proto::OrderingMode::BrokerManaged,
        })
        .await
        .expect("open Moonpool close-failure aggregate");
    close_cluster
        .wait_for("Moonpool close-failure children", |fake| {
            let counts = fake.resource_counts();
            counts.child_consumers == 2 && counts.permits > 0
        })
        .await;
    close_cluster
        .update(|fake| {
            for endpoint in [Endpoint::Segment(1), Endpoint::Segment(2)] {
                fake.script_next(
                    endpoint,
                    OperationKind::Close,
                    ScriptedBehavior::Fail(BrokerFailure::new(
                        magnetar_proto::pb::ServerError::PersistenceError,
                        "scripted child close failure",
                    )),
                )?;
            }
            Ok(())
        })
        .expect("script Moonpool child close failures");
    let close_failure_recovered = close_consumer.close().await.is_err();
    assert!(close_failure_recovered);
    close_cluster
        .wait_for("Moonpool forced close cleanup", |fake| {
            fake.resource_counts().child_consumers == 0
        })
        .await;
    close_client.close().await;
    close_cluster
        .wait_for("Moonpool close-failure client cleanup", |fake| {
            fake.resource_counts().connections == 0
        })
        .await;
    close_cluster.assert_healthy();

    let plain_cluster = M1SocketCluster::bind().await;
    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let plain_address = plain_cluster
        .controller_url()
        .strip_prefix("pulsar://")
        .expect("plaintext plain-driver URL");
    let plain_client = magnetar_runtime_moonpool::Client::connect_plain(
        &engine,
        plain_address,
        ConnectionConfig {
            supervisor: None,
            ..ConnectionConfig::default()
        },
    )
    .await
    .expect("connect plain Moonpool shutdown client");
    plain_client.close().await;
    plain_cluster
        .wait_for("plain Moonpool driver cleanup", |fake| {
            fake.resource_counts().connections == 0
        })
        .await;
    let plain_driver_closed = true;
    plain_cluster.assert_healthy();

    let route_cluster = M1SocketCluster::bind().await;
    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let route_address = route_cluster
        .controller_url()
        .strip_prefix("pulsar://")
        .expect("plaintext route-bound URL");
    let route_client = magnetar_runtime_moonpool::Client::connect_plain_supervised(
        &engine,
        route_address,
        supervised_config(),
        None,
        None,
    )
    .await
    .expect("connect Moonpool route-bound client");
    let mut route_dag = route_client
        .segment_subscriber()
        .expect("Moonpool route-bound subscriber")
        .open_dag_session(TOPIC)
        .await
        .expect("open Moonpool route-bound DAG");
    for epoch in 2..=67 {
        route_cluster
            .update(|fake| fake.advance_layout(epoch, unchanged_layout()))
            .expect("push Moonpool route-bound layout");
    }
    let barrier_txn = route_client
        .new_txn(Duration::from_secs(1))
        .await
        .expect("Moonpool route-bound wire-order barrier");
    route_client
        .end_txn(barrier_txn, magnetar_proto::TxnAction::Abort)
        .await
        .expect("close Moonpool route-bound barrier transaction");
    let route_overflowed = matches!(
        tokio::time::timeout(magnetar_differential::HANG_GUARD, route_dag.next())
            .await
            .expect("Moonpool route overflow surfaced"),
        Err(magnetar_runtime_moonpool::ClientError::ScalableRoute(
            magnetar_runtime_moonpool::ScalableRouteError::Overflow { capacity: 64 }
        ))
    );
    assert!(route_overflowed);
    route_client.close().await;
    let route_connection_closed = matches!(
        route_dag.next().await,
        Err(magnetar_runtime_moonpool::ClientError::ScalableRoute(
            magnetar_runtime_moonpool::ScalableRouteError::ConnectionClosed
        ))
    );
    assert!(route_connection_closed);
    drop(route_dag);
    route_cluster
        .wait_for("Moonpool route-bound cleanup", |fake| {
            fake.resource_counts().connections == 0
        })
        .await;
    route_cluster.assert_healthy();

    RuntimeFailureTrace {
        lookup_rejected,
        lookup_timed_out,
        lookup_pending_cancelled,
        controller_rejected,
        controller_timed_out,
        controller_pending_cancelled,
        segment_timed_out,
        missing_controller_authority_reused_bootstrap,
        proxy_target_rejected,
        close_failure_recovered,
        plain_driver_closed,
        route_overflowed,
        route_connection_closed,
    }
}

async fn run_tokio_transactions(cluster: &M1SocketCluster) -> RuntimeTransactionTrace {
    let client =
        magnetar_runtime_tokio::Client::connect(cluster.controller_url(), supervised_config())
            .await
            .expect("connect Tokio transaction-surface client");
    let aggregate = client
        .segment_subscriber()
        .expect("Tokio transaction-surface subscriber")
        .subscribe_stream_consumer(magnetar_runtime_tokio::StreamConsumerOptions {
            topic: TOPIC.to_owned(),
            subscription: "runtime-transaction-sub".to_owned(),
            consumer_name: "runtime-transaction-member".to_owned(),
            schema: magnetar_proto::pb::Schema::default(),
            receiver_budget: magnetar_proto::ReceiverBudget::bytes(32 * 1024 * 1024)
                .expect("valid Tokio transaction-surface budget"),
            ordering_mode: magnetar_proto::OrderingMode::BrokerManaged,
        })
        .await
        .expect("open Tokio transaction-surface aggregate");
    cluster
        .wait_for("Tokio transaction-surface children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;

    cluster
        .update(|fake| fake.enqueue_message(1, bytes::Bytes::from_static(b"cached-failure")))
        .expect("enqueue Tokio cached-registration failure");
    cluster
        .wait_for("Tokio cached-registration delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let failed_message = aggregate
        .receive()
        .await
        .expect("receive Tokio cached-registration delivery");
    let failed_txn = client
        .new_txn(Duration::from_secs(30))
        .await
        .expect("open Tokio cached-registration transaction");
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.script_next(
                Endpoint::Controller,
                OperationKind::TransactionRegistration,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar_proto::pb::ServerError::PersistenceError,
                    "cached registration failure",
                )),
            )
        })
        .expect("script Tokio cached-registration failure");
    let first_registration_failed = aggregate
        .acknowledge_in_transaction(&failed_message.token, failed_txn)
        .await
        .is_err();
    let cached_registration_failed = aggregate
        .acknowledge_in_transaction(&failed_message.token, failed_txn)
        .await
        .is_err();
    let failed_registration_cached = first_registration_failed
        && cached_registration_failed
        && cluster.inspect(|fake| transaction_registration_command_count(fake) == 1);
    assert_eq!(
        client
            .end_txn(failed_txn, magnetar_proto::TxnAction::Abort)
            .await
            .expect("abort Tokio cached-registration transaction"),
        magnetar_proto::TxnState::Aborted
    );
    aggregate
        .transaction_outcome(
            failed_txn,
            magnetar_proto::TransactionAcknowledgementOutcome::Aborted,
        )
        .await
        .expect("settle Tokio cached-registration abort");
    aggregate
        .acknowledge(&failed_message.token)
        .await
        .expect("acknowledge Tokio cached-registration delivery ordinarily");
    cluster
        .wait_for("Tokio cached-registration cleanup", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    cluster
        .update(|fake| fake.enqueue_message(1, bytes::Bytes::from_static(b"repeated-outcome")))
        .expect("enqueue Tokio repeated-outcome delivery");
    cluster
        .wait_for("Tokio repeated-outcome delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let committed_message = aggregate
        .receive()
        .await
        .expect("receive Tokio repeated-outcome delivery");
    let committed_txn = client
        .new_txn(Duration::from_secs(30))
        .await
        .expect("open Tokio repeated-outcome transaction");
    aggregate
        .acknowledge_in_transaction(&committed_message.token, committed_txn)
        .await
        .expect("stage Tokio repeated-outcome acknowledgement");
    assert_eq!(
        client
            .end_txn(committed_txn, magnetar_proto::TxnAction::Commit)
            .await
            .expect("commit Tokio repeated-outcome transaction"),
        magnetar_proto::TxnState::Committed
    );
    aggregate
        .transaction_outcome(
            committed_txn,
            magnetar_proto::TransactionAcknowledgementOutcome::Committed,
        )
        .await
        .expect("settle Tokio committed outcome");
    let repeated_outcome_completed = aggregate
        .transaction_outcome(
            committed_txn,
            magnetar_proto::TransactionAcknowledgementOutcome::Committed,
        )
        .await
        .is_ok();
    let conflicting_outcome_rejected = aggregate
        .transaction_outcome(
            committed_txn,
            magnetar_proto::TransactionAcknowledgementOutcome::Aborted,
        )
        .await
        .is_err();
    cluster
        .wait_for("Tokio repeated-outcome cleanup", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    cluster
        .update(|fake| fake.enqueue_message(1, bytes::Bytes::from_static(b"close-registration")))
        .expect("enqueue Tokio close-during-registration delivery");
    cluster
        .wait_for("Tokio close-during-registration delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let closing_message = aggregate
        .receive()
        .await
        .expect("receive Tokio close-during-registration delivery");
    let closing_txn = client
        .new_txn(Duration::from_secs(30))
        .await
        .expect("open Tokio close-during-registration transaction");
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Controller,
                OperationKind::TransactionRegistration,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay Tokio close-during-registration operation");
    let mut closing_ack =
        Box::pin(aggregate.acknowledge_in_transaction(&closing_message.token, closing_txn));
    tokio::select! {
        biased;
        result = &mut closing_ack => panic!("Tokio close-during-registration ACK completed early: {result:?}"),
        () = cluster.wait_for("pending Tokio close-during-registration operation", |fake| {
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
        .expect("Tokio close-during-registration operation id");
    aggregate
        .close()
        .await
        .expect("close Tokio aggregate during transaction registration");
    cluster
        .update(|fake| fake.complete_pending(pending_registration, PendingCompletion::Succeed))
        .expect("complete Tokio registration after aggregate close");
    let closing_ack_result = closing_ack.await;
    let close_during_registration_fenced = matches!(
        &closing_ack_result,
        Err(magnetar_runtime_tokio::StreamConsumerError::Closed)
    );
    assert!(
        close_during_registration_fenced,
        "unexpected Tokio close-during-registration result: {closing_ack_result:?}"
    );
    let closed_outcome_rejected = aggregate
        .transaction_outcome(
            closing_txn,
            magnetar_proto::TransactionAcknowledgementOutcome::Aborted,
        )
        .await
        .is_err();
    assert_eq!(
        client
            .end_txn(closing_txn, magnetar_proto::TxnAction::Abort)
            .await
            .expect("abort Tokio close-during-registration transaction"),
        magnetar_proto::TxnState::Aborted
    );
    client.close().await;
    cluster
        .wait_for("Tokio transaction-surface cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.connections == 0 && counts.child_consumers == 0 && counts.pending_operations == 0
        })
        .await;
    let cleaned_up = cluster.inspect(|fake| {
        let counts = fake.resource_counts();
        counts.connections == 0 && counts.child_consumers == 0 && counts.pending_operations == 0
    });

    RuntimeTransactionTrace {
        failed_registration_cached,
        repeated_outcome_completed,
        conflicting_outcome_rejected,
        close_during_registration_fenced,
        closed_outcome_rejected,
        cleaned_up,
    }
}

async fn run_moonpool_transactions(cluster: &M1SocketCluster) -> RuntimeTransactionTrace {
    let engine = magnetar_runtime_moonpool::MoonpoolEngine::new(TokioProviders::new());
    let address = cluster
        .controller_url()
        .strip_prefix("pulsar://")
        .expect("plaintext Moonpool transaction-surface URL");
    let client = magnetar_runtime_moonpool::Client::connect_plain_supervised(
        &engine,
        address,
        supervised_config(),
        None,
        None,
    )
    .await
    .expect("connect Moonpool transaction-surface client");
    let aggregate = client
        .segment_subscriber()
        .expect("Moonpool transaction-surface subscriber")
        .subscribe_stream_consumer(magnetar_runtime_moonpool::StreamConsumerOptions {
            topic: TOPIC.to_owned(),
            subscription: "runtime-transaction-sub".to_owned(),
            consumer_name: "runtime-transaction-member".to_owned(),
            schema: magnetar_proto::pb::Schema::default(),
            receiver_budget: magnetar_proto::ReceiverBudget::bytes(32 * 1024 * 1024)
                .expect("valid Moonpool transaction-surface budget"),
            ordering_mode: magnetar_proto::OrderingMode::BrokerManaged,
        })
        .await
        .expect("open Moonpool transaction-surface aggregate");
    cluster
        .wait_for("Moonpool transaction-surface children", |fake| {
            fake.resource_counts().child_consumers == 2
        })
        .await;

    cluster
        .update(|fake| fake.enqueue_message(1, bytes::Bytes::from_static(b"cached-failure")))
        .expect("enqueue Moonpool cached-registration failure");
    cluster
        .wait_for("Moonpool cached-registration delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let failed_message = aggregate
        .receive()
        .await
        .expect("receive Moonpool cached-registration delivery");
    let failed_txn = client
        .new_txn(Duration::from_secs(30))
        .await
        .expect("open Moonpool cached-registration transaction");
    cluster
        .update(|fake| {
            fake.clear_routes();
            fake.script_next(
                Endpoint::Controller,
                OperationKind::TransactionRegistration,
                ScriptedBehavior::Fail(BrokerFailure::new(
                    magnetar_proto::pb::ServerError::PersistenceError,
                    "cached registration failure",
                )),
            )
        })
        .expect("script Moonpool cached-registration failure");
    let first_registration_failed = aggregate
        .acknowledge_in_transaction(&failed_message.token, failed_txn)
        .await
        .is_err();
    let cached_registration_failed = aggregate
        .acknowledge_in_transaction(&failed_message.token, failed_txn)
        .await
        .is_err();
    let failed_registration_cached = first_registration_failed
        && cached_registration_failed
        && cluster.inspect(|fake| transaction_registration_command_count(fake) == 1);
    assert_eq!(
        client
            .end_txn(failed_txn, magnetar_proto::TxnAction::Abort)
            .await
            .expect("abort Moonpool cached-registration transaction"),
        magnetar_proto::TxnState::Aborted
    );
    aggregate
        .transaction_outcome(
            failed_txn,
            magnetar_proto::TransactionAcknowledgementOutcome::Aborted,
        )
        .await
        .expect("settle Moonpool cached-registration abort");
    aggregate
        .acknowledge(&failed_message.token)
        .await
        .expect("acknowledge Moonpool cached-registration delivery ordinarily");
    cluster
        .wait_for("Moonpool cached-registration cleanup", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    cluster
        .update(|fake| fake.enqueue_message(1, bytes::Bytes::from_static(b"repeated-outcome")))
        .expect("enqueue Moonpool repeated-outcome delivery");
    cluster
        .wait_for("Moonpool repeated-outcome delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let committed_message = aggregate
        .receive()
        .await
        .expect("receive Moonpool repeated-outcome delivery");
    let committed_txn = client
        .new_txn(Duration::from_secs(30))
        .await
        .expect("open Moonpool repeated-outcome transaction");
    aggregate
        .acknowledge_in_transaction(&committed_message.token, committed_txn)
        .await
        .expect("stage Moonpool repeated-outcome acknowledgement");
    assert_eq!(
        client
            .end_txn(committed_txn, magnetar_proto::TxnAction::Commit)
            .await
            .expect("commit Moonpool repeated-outcome transaction"),
        magnetar_proto::TxnState::Committed
    );
    aggregate
        .transaction_outcome(
            committed_txn,
            magnetar_proto::TransactionAcknowledgementOutcome::Committed,
        )
        .await
        .expect("settle Moonpool committed outcome");
    let repeated_outcome_completed = aggregate
        .transaction_outcome(
            committed_txn,
            magnetar_proto::TransactionAcknowledgementOutcome::Committed,
        )
        .await
        .is_ok();
    let conflicting_outcome_rejected = aggregate
        .transaction_outcome(
            committed_txn,
            magnetar_proto::TransactionAcknowledgementOutcome::Aborted,
        )
        .await
        .is_err();
    cluster
        .wait_for("Moonpool repeated-outcome cleanup", |fake| {
            fake.resource_counts().unacked_messages == 0
        })
        .await;

    cluster
        .update(|fake| fake.enqueue_message(1, bytes::Bytes::from_static(b"close-registration")))
        .expect("enqueue Moonpool close-during-registration delivery");
    cluster
        .wait_for("Moonpool close-during-registration delivery", |fake| {
            fake.resource_counts().unacked_messages == 1
        })
        .await;
    let closing_message = aggregate
        .receive()
        .await
        .expect("receive Moonpool close-during-registration delivery");
    let closing_txn = client
        .new_txn(Duration::from_secs(30))
        .await
        .expect("open Moonpool close-during-registration transaction");
    cluster
        .update(|fake| {
            fake.script_next(
                Endpoint::Controller,
                OperationKind::TransactionRegistration,
                ScriptedBehavior::Delay,
            )
        })
        .expect("delay Moonpool close-during-registration operation");
    let mut closing_ack =
        Box::pin(aggregate.acknowledge_in_transaction(&closing_message.token, closing_txn));
    tokio::select! {
        biased;
        result = &mut closing_ack => panic!("Moonpool close-during-registration ACK completed early: {result:?}"),
        () = cluster.wait_for("pending Moonpool close-during-registration operation", |fake| {
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
        .expect("Moonpool close-during-registration operation id");
    aggregate
        .close()
        .await
        .expect("close Moonpool aggregate during transaction registration");
    cluster
        .update(|fake| fake.complete_pending(pending_registration, PendingCompletion::Succeed))
        .expect("complete Moonpool registration after aggregate close");
    let closing_ack_result = closing_ack.await;
    let close_during_registration_fenced = matches!(
        &closing_ack_result,
        Err(magnetar_runtime_moonpool::StreamConsumerError::Closed)
    );
    assert!(
        close_during_registration_fenced,
        "unexpected Moonpool close-during-registration result: {closing_ack_result:?}"
    );
    let closed_outcome_rejected = aggregate
        .transaction_outcome(
            closing_txn,
            magnetar_proto::TransactionAcknowledgementOutcome::Aborted,
        )
        .await
        .is_err();
    assert_eq!(
        client
            .end_txn(closing_txn, magnetar_proto::TxnAction::Abort)
            .await
            .expect("abort Moonpool close-during-registration transaction"),
        magnetar_proto::TxnState::Aborted
    );
    client.close().await;
    cluster
        .wait_for("Moonpool transaction-surface cleanup", |fake| {
            let counts = fake.resource_counts();
            counts.connections == 0 && counts.child_consumers == 0 && counts.pending_operations == 0
        })
        .await;
    let cleaned_up = cluster.inspect(|fake| {
        let counts = fake.resource_counts();
        counts.connections == 0 && counts.child_consumers == 0 && counts.pending_operations == 0
    });

    RuntimeTransactionTrace {
        failed_registration_cached,
        repeated_outcome_completed,
        conflicting_outcome_rejected,
        close_during_registration_fenced,
        closed_outcome_rejected,
        cleaned_up,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owned_scalable_runtime_surfaces_are_equivalent() {
    let tokio_cluster = M1SocketCluster::bind().await;
    let tokio_trace = run_tokio_surface(&tokio_cluster).await;
    tokio_cluster.assert_healthy();

    let moonpool_cluster = M1SocketCluster::bind().await;
    let moonpool_trace = run_moonpool_surface(&moonpool_cluster).await;
    moonpool_cluster.assert_healthy();

    assert_eq!(tokio_trace, moonpool_trace);
    assert!(tokio_trace.subscriber_debug);
    assert!(tokio_trace.task_debug);
    assert!(tokio_trace.task_completed);
    assert!(tokio_trace.mismatched_source_rejected);
    assert!(tokio_trace.missing_authority_rejected);
    assert!(tokio_trace.wrong_scheme_rejected);
    assert!(tokio_trace.disallowed_authority_rejected);
    assert!(tokio_trace.replacement_rejected);
    assert!(tokio_trace.direct_aggregate_debug);
    assert!(tokio_trace.zero_message_batch);
    assert!(tokio_trace.zero_byte_batch);
    assert!(tokio_trace.cleaned_up);
    assert_eq!(tokio_trace.assigned_segments, vec![1, 2]);
    assert_eq!(tokio_trace.resolved_topic.as_deref(), Some(TOPIC));
    assert_eq!(tokio_trace.registration_topic, TOPIC);
    assert_eq!(
        tokio_trace.child_topic,
        "segment://public/default/scaled/0000-7fff-1"
    );
    assert_eq!(
        tokio_cluster.inspect(M1FakeCluster::resource_counts),
        moonpool_cluster.inspect(M1FakeCluster::resource_counts)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owned_scalable_runtime_failures_are_bounded_and_equivalent() {
    let tokio_trace = run_tokio_failures().await;
    let moonpool_trace = run_moonpool_failures().await;

    assert_eq!(tokio_trace, moonpool_trace);
    assert!(tokio_trace.lookup_rejected);
    assert!(tokio_trace.lookup_timed_out);
    assert!(tokio_trace.lookup_pending_cancelled);
    assert!(tokio_trace.controller_rejected);
    assert!(tokio_trace.controller_timed_out);
    assert!(tokio_trace.controller_pending_cancelled);
    assert!(tokio_trace.segment_timed_out);
    assert!(tokio_trace.missing_controller_authority_reused_bootstrap);
    assert!(tokio_trace.proxy_target_rejected);
    assert!(tokio_trace.close_failure_recovered);
    assert!(tokio_trace.plain_driver_closed);
    assert!(tokio_trace.route_overflowed);
    assert!(tokio_trace.route_connection_closed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owned_scalable_runtime_transaction_failures_are_equivalent() {
    let tokio_cluster = M1SocketCluster::bind().await;
    let tokio_trace = run_tokio_transactions(&tokio_cluster).await;
    tokio_cluster.assert_healthy();

    let moonpool_cluster = M1SocketCluster::bind().await;
    let moonpool_trace = run_moonpool_transactions(&moonpool_cluster).await;
    moonpool_cluster.assert_healthy();

    assert_eq!(tokio_trace, moonpool_trace);
    assert!(tokio_trace.failed_registration_cached);
    assert!(tokio_trace.repeated_outcome_completed);
    assert!(tokio_trace.conflicting_outcome_rejected);
    assert!(tokio_trace.close_during_registration_fenced);
    assert!(tokio_trace.closed_outcome_rejected);
    assert!(tokio_trace.cleaned_up);
}
