// SPDX-License-Identifier: Apache-2.0

//! Owned, bounded routes for scalable-topic control-plane events.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::client::{parse_direct_broker_url, subscribe_manual_flow_on};
use crate::pool::ProxyConnectionPool;
use crate::url_parse::Scheme;
use crate::{ClientError, ConnectionShared, ScalableEvent};

const MAX_ROUTE_EVENTS: usize = 64;
const MAX_AGGREGATE_EVENTS: usize = 256;
const MAX_RETIRED_ROUTES: usize = 256;
const DISCARD_POLICY_ERROR: &str = "scalable child used discard policy";

fn route_error_is_recoverable(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::ScalableRoute(
            ScalableRouteError::ConnectionReplaced | ScalableRouteError::Overflow { .. }
        )
    )
}

fn control_plane_error_is_terminal(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::PeerClosed
            | ClientError::Closed
            | ClientError::ScalableRoute(
                ScalableRouteError::ConnectionClosed | ScalableRouteError::Closed
            )
    )
}

/// Complete runtime configuration for one assignment-driven aggregate.
#[derive(Debug, Clone)]
pub struct StreamConsumerOptions {
    /// Requested scalable parent topic.
    pub topic: String,
    /// Subscription shared by all children.
    pub subscription: String,
    /// Stable aggregate consumer name.
    pub consumer_name: String,
    /// Broker schema metadata copied to every child.
    pub schema: magnetar_proto::pb::Schema,
    /// One aggregate receive budget.
    pub receiver_budget: magnetar_proto::ReceiverBudget,
    /// Parent-before-child ordering contract.
    pub ordering_mode: magnetar_proto::OrderingMode,
}

/// One raw delivery reserved from the aggregate queue.
#[derive(Debug)]
pub struct StreamConsumerMessage {
    /// Ordinary child message.
    pub message: magnetar_proto::IncomingMessage,
    /// Process-local acknowledgement authority.
    pub token: magnetar_proto::DeliveryToken,
}

/// One source-qualified component that failed after admission.
#[derive(Debug)]
pub struct StreamAckFailure {
    /// Component position.
    pub position: magnetar_proto::StreamMessageId,
    /// Secret-free runtime diagnostic.
    pub message: String,
}

impl StreamAckFailure {
    fn from_error(
        position: magnetar_proto::StreamMessageId,
        error: &impl std::fmt::Display,
    ) -> Self {
        Self {
            position,
            message: error.to_string(),
        }
    }
}

/// Runtime aggregate operation failure.
#[derive(Debug, thiserror::Error)]
pub enum StreamConsumerError {
    /// Authoritative model rejected the operation.
    #[error(transparent)]
    Model(#[from] magnetar_proto::StreamConsumerModelError),
    /// Ordinary child or control-plane operation failed.
    #[error(transparent)]
    Client(#[from] ClientError),
    /// Some fan-out components confirmed and others failed.
    #[error("aggregate acknowledgement partially failed")]
    PartialAcknowledgement {
        /// Durable components.
        confirmed: Vec<magnetar_proto::StreamMessageId>,
        /// Failed components.
        failed: Vec<StreamAckFailure>,
    },
    /// Aggregate is locally closed.
    #[error("stream consumer is closed")]
    Closed,
    /// Background aggregate work failed.
    #[error("stream consumer failed: {0}")]
    Failed(String),
}

/// Owned aggregate lifecycle event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamConsumerEvent {
    /// A full assignment became authoritative.
    AssignmentApplied {
        /// Validated layout epoch.
        layout_epoch: u64,
        /// Assigned sources in deterministic order.
        sources: Vec<magnetar_proto::SegmentSource>,
    },
    /// One child changed lifecycle phase.
    SegmentPhaseChanged {
        /// Child source.
        source: magnetar_proto::SegmentSource,
        /// Current phase.
        phase: magnetar_proto::SegmentPhase,
    },
    /// Strict ancestry cannot currently be proved.
    OrderingUnprovable {
        /// Blocked descendant.
        segment_id: magnetar_proto::SegmentId,
        /// Unknown ancestors.
        ancestors: Vec<magnetar_proto::SegmentId>,
    },
    /// Authority was fenced pending a fresh controller baseline.
    ResyncRequired {
        /// Secret-free reason.
        reason: String,
    },
    /// A transaction participant reached a final outcome.
    TransactionOutcome {
        /// Pulsar transaction id.
        txn_id: magnetar_proto::TxnId,
        /// Confirmed or unknown result.
        outcome: magnetar_proto::TransactionAcknowledgementOutcome,
    },
    /// Aggregate cleanup completed locally.
    Closed,
}

/// Frozen ordinary-consumer settings applied to every assigned segment.
#[derive(Debug, Clone)]
pub struct SegmentConsumerOptions {
    /// Subscription shared by all children.
    pub subscription: String,
    /// Stable aggregate name; the segment id is appended for each child.
    pub consumer_name: String,
    /// Broker schema metadata copied to every child subscribe.
    pub schema: magnetar_proto::pb::Schema,
}

/// Narrow owned capability used after the public [`crate::Client`] borrow ends.
#[derive(Debug, Clone)]
pub struct SegmentSubscriber {
    bootstrap: Arc<ConnectionShared>,
    pool: Arc<ProxyConnectionPool>,
    operation_timeout: std::time::Duration,
}

impl SegmentSubscriber {
    pub(crate) fn new(
        bootstrap: Arc<ConnectionShared>,
        pool: Arc<ProxyConnectionPool>,
        operation_timeout: std::time::Duration,
    ) -> Self {
        Self {
            bootstrap,
            pool,
            operation_timeout,
        }
    }

    /// Spawn aggregate control work on Tokio without retaining the public
    /// client. The returned handle aborts the task on drop.
    // This deliberately remains an instance method to keep spawning scoped to
    // the owned capability and aligned with the provider-backed runtime.
    #[allow(clippy::unused_self)]
    pub fn spawn_task<F>(&self, future: F) -> ScalableTaskHandle
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.spawn_task_with_completion(future, || {})
    }

    #[allow(clippy::unused_self)]
    fn spawn_task_with_completion<F, C>(&self, future: F, on_complete: C) -> ScalableTaskHandle
    where
        F: std::future::Future<Output = ()> + Send + 'static,
        C: FnOnce() + Send + 'static,
    {
        let completed = Arc::new(AtomicBool::new(false));
        let completion = TaskCompletion::new(completed.clone(), on_complete);
        let join = tokio::spawn(async move {
            future.await;
            drop(completion);
        });
        ScalableTaskHandle {
            join: Some(join),
            completed,
        }
    }

    /// Runtime-native sleep used by ownership retry loops.
    pub async fn sleep(&self, duration: std::time::Duration) {
        tokio::time::sleep(duration).await;
    }

    /// Open a complete assignment-driven aggregate from this owned capability.
    pub async fn subscribe_stream_consumer(
        &self,
        options: StreamConsumerOptions,
    ) -> Result<StreamConsumer, StreamConsumerError> {
        StreamConsumer::open(self.clone(), options).await
    }

    async fn add_subscription_to_txn(
        &self,
        txn_id: magnetar_proto::TxnId,
        topic: String,
        subscription: String,
    ) -> Result<(), ClientError> {
        let request_id = {
            let mut conn = self.bootstrap.inner.lock();
            conn.add_subscription_to_txn(txn_id, subscription, topic)
        };
        self.bootstrap.driver_waker.notify_one();
        match crate::client::RequestFut::new(self.bootstrap.clone(), request_id).await {
            magnetar_proto::OpOutcome::AddSubscriptionToTxn { result, .. } => result
                .map_err(|error| ClientError::Other(format!("add_subscription_to_txn: {error}"))),
            magnetar_proto::OpOutcome::Terminal { .. } => Err(ClientError::PeerClosed),
            magnetar_proto::OpOutcome::Error { code, message, .. } => {
                Err(ClientError::Broker { code, message })
            }
            other => Err(ClientError::Other(format!(
                "unexpected add_subscription_to_txn outcome: {other:?}"
            ))),
        }
    }

    /// Open and exclusively claim a DAG-watch session before its command can
    /// leave the bootstrap connection.
    pub async fn open_dag_session(&self, topic: &str) -> Result<DagSession, ClientError> {
        self.bootstrap.fail_if_no_driver()?;
        let (session_id, route) = {
            let mut conn = self.bootstrap.inner.lock();
            let session_id = conn
                .open_scalable_topic_session(topic)
                .map_err(|error| ClientError::Other(error.to_string()))?;
            let session_epoch = conn.session_epoch();
            let key = ScalableRouteKey::dag(
                session_id,
                magnetar_proto::ControllerIncarnation(session_epoch),
            );
            let route = self.bootstrap.scalable_routes.claim_at_epoch(
                self.bootstrap.clone(),
                key,
                session_epoch,
            );
            (session_id, route)
        };
        self.bootstrap.driver_waker.notify_one();
        let pending = PendingDagSession::new(self.bootstrap.clone(), session_id, route);

        let initial =
            match tokio::time::timeout(self.operation_timeout, pending.route().next()).await {
                Ok(Ok(event)) => event,
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => {
                    return Err(ClientError::Timeout(
                        "scalable DAG lookup exceeded operation_timeout".to_owned(),
                    ));
                }
            };
        match initial {
            ScalableEvent::LookupResolved {
                resolved_topic_name,
                controller_broker_url,
                controller_broker_url_tls,
                snapshot,
                ..
            } => Ok(DagSession {
                shared: self.bootstrap.clone(),
                route: pending.into_route(),
                session_id,
                requested_topic: topic.to_owned(),
                resolved_topic_name,
                controller_broker_url,
                controller_broker_url_tls,
                snapshot,
                closed: false,
            }),
            ScalableEvent::DagWatchClosed { reason, .. } => Err(ClientError::Other(
                reason.unwrap_or("scalable DAG watch closed before baseline".to_owned()),
            )),
            other => Err(ClientError::Other(format!(
                "unexpected scalable DAG baseline event: {other:?}"
            ))),
        }
    }

    /// Open the retained DAG watch and controller registration, then align
    /// their authoritative epochs. Missing controller authority reuses the
    /// authenticated direct bootstrap; invalid published authority fails.
    pub async fn open_control_plane(
        &self,
        topic: &str,
        subscription: &str,
        consumer_name: &str,
    ) -> Result<(DagSession, ControllerSession), ClientError> {
        let operation = async {
            let mut dag = self.open_dag_session(topic).await?;
            let mut controller = self
                .open_controller_session(&dag, subscription, consumer_name)
                .await?;
            Self::align_control_plane(&mut dag, &mut controller).await?;
            Ok((dag, controller))
        };
        match tokio::time::timeout(self.operation_timeout, operation).await {
            Ok(result) => result,
            Err(_) => Err(ClientError::ControllerUnavailable),
        }
    }

    async fn align_control_plane(
        dag: &mut DagSession,
        controller: &mut ControllerSession,
    ) -> Result<(), ClientError> {
        loop {
            let dag_epoch = dag.snapshot().epoch();
            let assignment_epoch = controller.assignment().layout_epoch();
            if dag_epoch == assignment_epoch {
                return Ok(());
            }
            if dag_epoch < assignment_epoch {
                if let ScalableEvent::DagWatchClosed { reason, .. } = dag.next().await? {
                    return Err(ClientError::Other(reason.unwrap_or_else(|| {
                        "scalable DAG watch closed while aligning control-plane epochs".to_owned()
                    })));
                }
            } else {
                controller.next_assignment().await?;
            }
        }
    }

    /// Register on the directly-addressed controller selected by the DAG.
    pub async fn open_controller_session(
        &self,
        dag: &DagSession,
        subscription: &str,
        consumer_name: &str,
    ) -> Result<ControllerSession, ClientError> {
        let consumer_id = self.bootstrap.allocate_scalable_consumer_id()?;
        self.open_controller_session_with_id(dag, subscription, consumer_name, consumer_id)
            .await
    }

    /// Replace a fenced controller connection while retaining the logical wire
    /// consumer id and registration identity. The local incarnation always
    /// advances from the originating client capability, even if the controller
    /// moved to another pooled broker connection.
    pub async fn reopen_controller_session(
        &self,
        dag: &DagSession,
        previous: ControllerSession,
    ) -> Result<ControllerSession, ClientError> {
        let registration_topic = dag
            .resolved_topic_name
            .as_deref()
            .unwrap_or(&dag.requested_topic);
        if previous.registration_topic != registration_topic {
            return Err(ClientError::ControllerRoutingUnsupported {
                reason: "replacement controller changed the scalable registration topic",
            });
        }
        let consumer_id = previous.consumer_id;
        let subscription = previous.subscription.clone();
        let consumer_name = previous.consumer_name.clone();
        previous.close();
        self.open_controller_session_with_id(dag, &subscription, &consumer_name, consumer_id)
            .await
    }

    async fn open_controller_session_with_id(
        &self,
        dag: &DagSession,
        subscription: &str,
        consumer_name: &str,
        consumer_id: u64,
    ) -> Result<ControllerSession, ClientError> {
        if self.pool.bootstrap_uses_proxy_target() {
            return Err(ClientError::ControllerRoutingUnsupported {
                reason: "proxy-any-broker controller registration is not defined by M1",
            });
        }
        let controller_url = match self.pool.bootstrap_scheme() {
            Scheme::Plain => dag.controller_broker_url.as_deref(),
            Scheme::Tls => dag.controller_broker_url_tls.as_deref(),
        };
        let shared = match controller_url {
            Some(controller_url) => self.resolve_direct_url(controller_url).await?,
            // M1 uses the configured service connection until leader election publishes a URL.
            None => self.bootstrap.clone(),
        };
        shared.fail_if_no_driver()?;
        let incarnation = self.bootstrap.allocate_controller_incarnation()?;
        let registration_topic = dag
            .resolved_topic_name
            .as_deref()
            .unwrap_or(&dag.requested_topic)
            .to_owned();
        let route = {
            let mut conn = shared.inner.lock();
            let session_epoch = conn.session_epoch();
            let route = shared.scalable_routes.claim_at_epoch(
                shared.clone(),
                ScalableRouteKey::consumer(consumer_id, incarnation),
                session_epoch,
            );
            if let Err(error) = conn.scalable_topic_subscribe(
                &registration_topic,
                subscription,
                consumer_name,
                consumer_id,
                magnetar_proto::ScalableConsumerType::Stream,
                incarnation,
            ) {
                route.close();
                return Err(ClientError::Other(error.to_string()));
            }
            route
        };
        shared.driver_waker.notify_one();
        let pending =
            PendingControllerSession::new(shared.clone(), route, consumer_id, incarnation);

        let baseline = tokio::time::timeout(self.operation_timeout, async {
            loop {
                match pending.route().next().await? {
                    ScalableEvent::ConsumerAssigned {
                        incarnation: event_incarnation,
                        assignment,
                        ..
                    } if event_incarnation == incarnation => return Ok(assignment),
                    ScalableEvent::ConsumerRejected {
                        incarnation: event_incarnation,
                        reason,
                        ..
                    } if event_incarnation == incarnation => {
                        return Err(ClientError::ScalableAssignmentRejected { reason });
                    }
                    _ => {}
                }
            }
        })
        .await
        .map_err(|_| {
            ClientError::Timeout(
                "scalable controller subscribe exceeded operation_timeout".to_owned(),
            )
        })??;
        Ok(ControllerSession {
            shared,
            route: pending.into_route(),
            consumer_id,
            incarnation,
            assignment: baseline,
            registration_topic,
            subscription: subscription.to_owned(),
            consumer_name: consumer_name.to_owned(),
        })
    }

    /// Open one paused, zero-queue Exclusive child against its validated DAG
    /// placement. Only explicit aggregate FLOW can grant a message permit.
    pub async fn open_segment_consumer(
        &self,
        source: &magnetar_proto::SegmentSource,
        descriptor: &magnetar_proto::SegmentDescriptor,
        options: &SegmentConsumerOptions,
    ) -> Result<crate::Consumer, ClientError> {
        if descriptor.segment_id != source.segment_id()
            || descriptor.key_range != source.key_range()
        {
            return Err(ClientError::Other(
                "assigned segment source does not match its DAG descriptor".to_owned(),
            ));
        }
        let broker_url = match self.pool.bootstrap_scheme() {
            Scheme::Plain => descriptor.broker_url.as_deref(),
            Scheme::Tls => descriptor.broker_url_tls.as_deref(),
        }
        .ok_or(ClientError::ControllerUnavailable)?;
        let shared = self.resolve_direct_url(broker_url).await?;
        let request = magnetar_proto::SubscribeRequest {
            topic: source.topic().to_owned(),
            subscription: options.subscription.clone(),
            sub_type: magnetar_proto::pb::command_subscribe::SubType::Exclusive,
            receiver_queue_size: 0,
            defer_payload_processing: true,
            consumer_name: Some(format!(
                "{}-seg-{}",
                options.consumer_name,
                source.segment_id().0
            )),
            schema: Some(options.schema.clone()),
            ..Default::default()
        };
        subscribe_manual_flow_on(shared, request, self.operation_timeout).await
    }

    async fn resolve_direct_url(
        &self,
        broker_url: &str,
    ) -> Result<Arc<ConnectionShared>, ClientError> {
        if !self.pool.scalable_url_allowed(broker_url) {
            return Err(ClientError::ScalableAuthorityRejected);
        }
        let expected = self.pool.bootstrap_scheme();
        match (expected, magnetar_proto::broker_endpoint_scheme(broker_url)) {
            (Scheme::Plain, Some(magnetar_proto::BrokerEndpointScheme::PulsarTls))
            | (Scheme::Tls, Some(magnetar_proto::BrokerEndpointScheme::Pulsar))
            | (_, None) => {
                return Err(ClientError::ControllerRoutingUnsupported {
                    reason: "broker authority scheme does not match the bootstrap transport",
                });
            }
            _ => {}
        }
        let parsed = parse_direct_broker_url(broker_url, expected)?;
        let bootstrap = self.pool.bootstrap_url();
        if parsed.host == bootstrap.host && parsed.port == bootstrap.port {
            return Ok(self.bootstrap.clone());
        }
        self.pool.get_or_open(broker_url, &parsed, None, 0).await
    }
}

struct PendingDagSession {
    shared: Arc<ConnectionShared>,
    route: Option<ScalableRoute>,
    session_id: u64,
    active: bool,
}

impl PendingDagSession {
    fn new(shared: Arc<ConnectionShared>, session_id: u64, route: ScalableRoute) -> Self {
        Self {
            shared,
            route: Some(route),
            session_id,
            active: true,
        }
    }

    fn route(&self) -> &ScalableRoute {
        self.route
            .as_ref()
            .expect("pending DAG route must exist until committed")
    }

    fn into_route(mut self) -> ScalableRoute {
        self.active = false;
        self.route
            .take()
            .expect("pending DAG route must exist when committed")
    }
}

impl Drop for PendingDagSession {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Some(route) = &self.route {
            route.close();
        }
        self.shared
            .inner
            .lock()
            .close_scalable_topic_session(self.session_id);
        self.shared.driver_waker.notify_one();
    }
}

struct PendingControllerSession {
    shared: Arc<ConnectionShared>,
    route: Option<ScalableRoute>,
    consumer_id: u64,
    incarnation: magnetar_proto::ControllerIncarnation,
    active: bool,
}

impl PendingControllerSession {
    fn new(
        shared: Arc<ConnectionShared>,
        route: ScalableRoute,
        consumer_id: u64,
        incarnation: magnetar_proto::ControllerIncarnation,
    ) -> Self {
        Self {
            shared,
            route: Some(route),
            consumer_id,
            incarnation,
            active: true,
        }
    }

    fn route(&self) -> &ScalableRoute {
        self.route
            .as_ref()
            .expect("pending controller route must exist until committed")
    }

    fn into_route(mut self) -> ScalableRoute {
        self.active = false;
        self.route
            .take()
            .expect("pending controller route must exist when committed")
    }
}

impl Drop for PendingControllerSession {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Some(route) = &self.route {
            route.close();
        }
        self.shared
            .inner
            .lock()
            .remove_scalable_consumer_registration(self.consumer_id, self.incarnation);
        self.shared.driver_waker.notify_one();
    }
}

#[derive(Debug, Clone)]
struct ChildRuntime {
    source: magnetar_proto::SegmentSource,
    generation: magnetar_proto::ChildGeneration,
    consumer: crate::Consumer,
}

#[derive(Debug)]
enum QueuedDelivery {
    Fresh {
        reservation: magnetar_proto::BudgetReservationId,
        message_id_data: magnetar_proto::pb::MessageIdData,
    },
    Restored {
        token: magnetar_proto::DeliveryToken,
    },
}

#[derive(Debug)]
struct QueuedMessage {
    source: magnetar_proto::SegmentSource,
    generation: magnetar_proto::ChildGeneration,
    message: magnetar_proto::IncomingMessage,
    delivery: QueuedDelivery,
}

#[derive(Debug, Clone)]
struct ControllerRegistration {
    shared: Arc<ConnectionShared>,
    consumer_id: u64,
    incarnation: magnetar_proto::ControllerIncarnation,
}

impl ControllerRegistration {
    fn from_session(controller: &ControllerSession) -> Self {
        Self {
            shared: controller.shared.clone(),
            consumer_id: controller.consumer_id,
            incarnation: controller.incarnation,
        }
    }

    fn close(&self) {
        self.shared
            .inner
            .lock()
            .remove_scalable_consumer_registration(self.consumer_id, self.incarnation);
        self.shared.driver_waker.notify_one();
    }
}

#[derive(Debug, Clone)]
enum TransactionRegistration {
    Pending,
    Registered,
    Failed(String),
}

#[derive(Debug)]
struct TransactionOutcomeCompletion {
    outcome: magnetar_proto::TransactionAcknowledgementOutcome,
    state: Mutex<TransactionOutcomeState>,
    work: Mutex<Option<TransactionOutcomeWork>>,
    notify: Notify,
}

#[derive(Debug, Default)]
struct TransactionOutcomeState {
    result: Option<Result<(), String>>,
    running: bool,
}

#[derive(Debug)]
struct TransactionOutcomeWork {
    actions: VecDeque<magnetar_proto::StreamConsumerAction>,
    completions: VecDeque<(magnetar_proto::SegmentId, magnetar_proto::ChildGeneration)>,
}

impl TransactionOutcomeCompletion {
    fn new(outcome: magnetar_proto::TransactionAcknowledgementOutcome) -> Self {
        Self {
            outcome,
            state: Mutex::new(TransactionOutcomeState::default()),
            work: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    fn try_start(&self) -> bool {
        let mut state = self.state.lock();
        if matches!(state.result, Some(Ok(()))) || state.running {
            return false;
        }
        state.result = None;
        state.running = true;
        true
    }

    fn finish(&self, result: Result<(), String>) {
        let mut state = self.state.lock();
        if state.result.is_none() {
            state.result = Some(result);
        }
        state.running = false;
        drop(state);
        self.notify.notify_waiters();
    }

    async fn wait(&self) -> Result<(), String> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.state.lock().result.clone() {
                return result;
            }
            notified.await;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateCloseState {
    Open,
    Fenced,
    Closing,
    Closed,
}

impl AggregateCloseState {
    const fn is_closing(self) -> bool {
        !matches!(self, Self::Open)
    }
}

enum ControlUpdate {
    Wake,
    Dag(Result<ScalableEvent, ClientError>),
    Assignment(Result<magnetar_proto::ConsumerAssignment, ClientError>),
}

#[cfg(test)]
#[derive(Debug, Default)]
struct ControlParkHook {
    reached: Notify,
    release: Notify,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct TransactionOutcomeParkHook {
    reached: Notify,
    release: Notify,
}

#[derive(Debug)]
struct AggregateState {
    model: magnetar_proto::StreamConsumerModel,
    receive: magnetar_proto::StreamReceiveState,
    children: BTreeMap<magnetar_proto::SegmentId, ChildRuntime>,
    flow_reservations: BTreeMap<
        (magnetar_proto::SegmentId, magnetar_proto::ChildGeneration),
        magnetar_proto::BudgetReservationId,
    >,
    dispatch_permit_debt:
        BTreeMap<(magnetar_proto::SegmentId, magnetar_proto::ChildGeneration), DispatchPermitDebt>,
    queue: VecDeque<QueuedMessage>,
    events: VecDeque<StreamConsumerEvent>,
    pending_transactions:
        BTreeMap<magnetar_proto::TxnId, Vec<magnetar_proto::TransactionAcknowledgementAuthority>>,
    transaction_registrations:
        BTreeMap<(magnetar_proto::TxnId, magnetar_proto::SegmentSource), TransactionRegistration>,
    transaction_outcomes: BTreeMap<magnetar_proto::TxnId, Arc<TransactionOutcomeCompletion>>,
    controller_registration: Option<ControllerRegistration>,
    terminal_error: Option<String>,
    reconnect_requested: bool,
    open_tasks: usize,
    close_state: AggregateCloseState,
    close_error: Option<String>,
    tasks: Vec<ScalableTaskHandle>,
}

impl AggregateState {
    fn new(
        model: magnetar_proto::StreamConsumerModel,
        controller_registration: ControllerRegistration,
    ) -> Self {
        Self {
            model,
            receive: magnetar_proto::StreamReceiveState::default(),
            children: BTreeMap::new(),
            flow_reservations: BTreeMap::new(),
            dispatch_permit_debt: BTreeMap::new(),
            queue: VecDeque::new(),
            events: VecDeque::new(),
            pending_transactions: BTreeMap::new(),
            transaction_registrations: BTreeMap::new(),
            transaction_outcomes: BTreeMap::new(),
            controller_registration: Some(controller_registration),
            terminal_error: None,
            reconnect_requested: false,
            open_tasks: 0,
            close_state: AggregateCloseState::Open,
            close_error: None,
            tasks: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DispatchPermitDebt {
    session_epoch: u64,
    permits: u32,
}

struct AcknowledgementCancellation<'a> {
    inner: Weak<StreamConsumerInner>,
    authority: &'a magnetar_proto::AcknowledgementAuthority,
    armed: bool,
}

struct SeekCancellation {
    inner: Weak<StreamConsumerInner>,
    armed: bool,
}

impl SeekCancellation {
    fn new(inner: &Arc<StreamConsumerInner>) -> Self {
        Self {
            inner: Arc::downgrade(inner),
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SeekCancellation {
    fn drop(&mut self) {
        if self.armed
            && let Some(inner) = self.inner.upgrade()
        {
            inner.request_resync("aggregate seek was cancelled".to_owned());
        }
    }
}

impl<'a> AcknowledgementCancellation<'a> {
    fn new(
        inner: &Arc<StreamConsumerInner>,
        authority: &'a magnetar_proto::AcknowledgementAuthority,
    ) -> Self {
        Self {
            inner: Arc::downgrade(inner),
            authority,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for AcknowledgementCancellation<'_> {
    fn drop(&mut self) {
        if self.armed
            && let Some(inner) = self.inner.upgrade()
        {
            let actions = inner
                .state
                .lock()
                .model
                .cancel_acknowledgement(self.authority);
            if let Ok(actions) = actions {
                inner.spawn_actions(actions);
            }
        }
    }
}

struct TransactionAcknowledgementCancellation {
    inner: Weak<StreamConsumerInner>,
    authority: Option<magnetar_proto::TransactionAcknowledgementAuthority>,
}

impl TransactionAcknowledgementCancellation {
    fn new(
        inner: &Arc<StreamConsumerInner>,
        authority: magnetar_proto::TransactionAcknowledgementAuthority,
    ) -> Self {
        Self {
            inner: Arc::downgrade(inner),
            authority: Some(authority),
        }
    }

    fn authority(
        &self,
    ) -> Result<&magnetar_proto::TransactionAcknowledgementAuthority, StreamConsumerError> {
        self.authority
            .as_ref()
            .ok_or_else(|| StreamConsumerError::Failed("transaction authority is disarmed".into()))
    }

    fn disarm(&mut self) {
        self.authority = None;
    }

    fn take(
        &mut self,
    ) -> Result<magnetar_proto::TransactionAcknowledgementAuthority, StreamConsumerError> {
        self.authority
            .take()
            .ok_or_else(|| StreamConsumerError::Failed("transaction authority is disarmed".into()))
    }
}

impl Drop for TransactionAcknowledgementCancellation {
    fn drop(&mut self) {
        if let (Some(authority), Some(inner)) = (self.authority.as_ref(), self.inner.upgrade()) {
            let actions = inner
                .state
                .lock()
                .model
                .cancel_transactional_acknowledgement(authority);
            if let Ok(actions) = actions {
                inner.spawn_actions(actions);
            }
        }
    }
}

struct TransactionRegistrationCancellation {
    inner: Weak<StreamConsumerInner>,
    key: (magnetar_proto::TxnId, magnetar_proto::SegmentSource),
    armed: bool,
}

impl TransactionRegistrationCancellation {
    fn new(
        inner: &Arc<StreamConsumerInner>,
        key: (magnetar_proto::TxnId, magnetar_proto::SegmentSource),
    ) -> Self {
        Self {
            inner: Arc::downgrade(inner),
            key,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TransactionRegistrationCancellation {
    fn drop(&mut self) {
        if self.armed
            && let Some(inner) = self.inner.upgrade()
        {
            let mut state = inner.state.lock();
            state.transaction_registrations.remove(&self.key);
            drop(state);
            inner.notify.notify_waiters();
        }
    }
}

#[derive(Debug)]
struct StreamConsumerInner {
    subscriber: SegmentSubscriber,
    child_options: SegmentConsumerOptions,
    topic: String,
    subscription: String,
    consumer_name: String,
    consumer_id: u64,
    state: Mutex<AggregateState>,
    notify: Notify,
    #[cfg(test)]
    control_park_hook: Option<Arc<ControlParkHook>>,
    #[cfg(test)]
    transaction_outcome_park_hook: Option<Arc<TransactionOutcomeParkHook>>,
}

/// Production Tokio aggregate over assigned ordinary child consumers.
#[derive(Debug)]
pub struct StreamConsumer {
    inner: Arc<StreamConsumerInner>,
}

impl StreamConsumer {
    async fn open(
        subscriber: SegmentSubscriber,
        options: StreamConsumerOptions,
    ) -> Result<Self, StreamConsumerError> {
        let (dag, controller) = subscriber
            .open_control_plane(
                &options.topic,
                &options.subscription,
                &options.consumer_name,
            )
            .await?;
        let topic = controller.registration_topic().to_owned();
        let consumer_id = controller.consumer_id();
        let incarnation = controller.incarnation();
        let assignment = controller.assignment().clone();
        let mut model = magnetar_proto::StreamConsumerModel::new(
            topic.clone(),
            magnetar_proto::ConsumerInstanceId(consumer_id),
            incarnation,
            options.ordering_mode,
            dag.snapshot().clone(),
            options.receiver_budget,
        )?;
        let actions = model.apply_control_plane(dag.snapshot().clone(), assignment.clone())?;
        let inner = Arc::new(StreamConsumerInner {
            subscriber,
            child_options: SegmentConsumerOptions {
                subscription: options.subscription.clone(),
                consumer_name: options.consumer_name.clone(),
                schema: options.schema,
            },
            topic,
            subscription: options.subscription,
            consumer_name: options.consumer_name,
            consumer_id,
            state: Mutex::new(AggregateState::new(
                model,
                ControllerRegistration::from_session(&controller),
            )),
            notify: Notify::new(),
            #[cfg(test)]
            control_park_hook: None,
            #[cfg(test)]
            transaction_outcome_park_hook: None,
        });
        inner.push_assignment_event(&assignment);
        inner.execute_actions(actions).await?;
        inner.spawn_control_task(dag, controller);
        Ok(Self { inner })
    }

    /// Await and atomically reserve one aggregate delivery.
    pub async fn receive(&self) -> Result<StreamConsumerMessage, StreamConsumerError> {
        let mut messages = self.inner.reserve_batch(1, usize::MAX).await?;
        messages.pop().ok_or(StreamConsumerError::Closed)
    }

    /// Restore cancelled schema/decode preparation to its original local order.
    pub fn restore_deliveries(
        &self,
        messages: Vec<StreamConsumerMessage>,
    ) -> Result<(), StreamConsumerError> {
        self.inner.restore_deliveries(messages)
    }

    /// Fence and resynchronize when a facade cancellation races authority
    /// invalidation and the original delivery can no longer be requeued.
    pub fn delivery_restoration_failed(&self, error: &StreamConsumerError) {
        self.inner
            .request_resync(format!("delivery restoration failed: {error}"));
    }

    /// Await the first message up to `max_wait`, then atomically reserve a
    /// count/byte-bounded batch.
    pub async fn receive_batch(
        &self,
        max_messages: usize,
        max_bytes: usize,
        max_wait: std::time::Duration,
    ) -> Result<Vec<StreamConsumerMessage>, StreamConsumerError> {
        if max_messages == 0 || max_bytes == 0 {
            return Ok(Vec::new());
        }
        match tokio::time::timeout(max_wait, self.inner.reserve_batch(max_messages, max_bytes))
            .await
        {
            Ok(result) => result,
            Err(_) => Ok(Vec::new()),
        }
    }

    /// Resolve one individual live delivery after broker confirmation.
    pub async fn acknowledge(
        &self,
        token: &magnetar_proto::DeliveryToken,
    ) -> Result<(), StreamConsumerError> {
        let transition = self
            .inner
            .state
            .lock()
            .model
            .admit_individual_acknowledgement(token)?;
        self.inner.execute_acknowledgement(transition).await
    }

    /// Cumulatively acknowledge every represented segment position.
    pub async fn acknowledge_cumulative(
        &self,
        token: &magnetar_proto::DeliveryToken,
    ) -> Result<(), StreamConsumerError> {
        let transition = self
            .inner
            .state
            .lock()
            .model
            .admit_cumulative_acknowledgement(token)?;
        self.inner.execute_acknowledgement(transition).await
    }

    /// Acknowledge a restored current position vector.
    pub async fn acknowledge_positions(
        &self,
        positions: &magnetar_proto::PositionVector,
    ) -> Result<(), StreamConsumerError> {
        let transition = self
            .inner
            .state
            .lock()
            .model
            .admit_position_acknowledgement(positions)?;
        self.inner.execute_acknowledgement(transition).await
    }

    /// Validate every token before issuing grouped child acknowledgements.
    pub async fn acknowledge_batch(
        &self,
        tokens: &[&magnetar_proto::DeliveryToken],
    ) -> Result<(), StreamConsumerError> {
        let transition = self
            .inner
            .state
            .lock()
            .model
            .admit_batch_acknowledgement(tokens)?;
        self.inner.execute_acknowledgement(transition).await
    }

    /// Fire-and-forget negative acknowledgement after model validation.
    pub fn negative_acknowledge(
        &self,
        token: &magnetar_proto::DeliveryToken,
    ) -> Result<(), StreamConsumerError> {
        let source = token.stream_message_id().source().clone();
        let message_id = token
            .stream_message_id()
            .ordinary_message_id_data()
            .map_err(magnetar_proto::StreamConsumerModelError::from)?;
        let (consumer, actions) = {
            let mut state = self.inner.state.lock();
            let generation = state
                .model
                .child_generation(&source)
                .ok_or(magnetar_proto::StreamConsumerModelError::StaleDeliveryToken)?;
            let consumer = state
                .children
                .get(&source.segment_id())
                .filter(|child| child.source == source && child.generation == generation)
                .map(|child| child.consumer.clone())
                .ok_or(magnetar_proto::StreamConsumerModelError::StaleDeliveryToken)?;
            let actions = state.model.resolve_delivery(token)?;
            (consumer, actions)
        };
        consumer.negative_ack_message_id_data(message_id);
        self.inner.spawn_actions(actions);
        Ok(())
    }

    /// Admit one individual transactional acknowledgement.
    pub async fn acknowledge_in_transaction(
        &self,
        token: &magnetar_proto::DeliveryToken,
        txn_id: magnetar_proto::TxnId,
    ) -> Result<(), StreamConsumerError> {
        let transition = self
            .inner
            .state
            .lock()
            .model
            .admit_individual_transactional_acknowledgement(token)?;
        self.inner
            .execute_transactional_acknowledgement(transition, txn_id)
            .await
    }

    /// Admit a cumulative transactional position acknowledgement.
    pub async fn acknowledge_cumulative_in_transaction(
        &self,
        token: &magnetar_proto::DeliveryToken,
        txn_id: magnetar_proto::TxnId,
    ) -> Result<(), StreamConsumerError> {
        let transition = self
            .inner
            .state
            .lock()
            .model
            .admit_cumulative_transactional_acknowledgement(token)?;
        self.inner
            .execute_transactional_acknowledgement(transition, txn_id)
            .await
    }

    /// Admit a restored transactional position acknowledgement.
    pub async fn acknowledge_positions_in_transaction(
        &self,
        positions: &magnetar_proto::PositionVector,
        txn_id: magnetar_proto::TxnId,
    ) -> Result<(), StreamConsumerError> {
        let transition = self
            .inner
            .state
            .lock()
            .model
            .admit_position_transactional_acknowledgement(positions)?;
        self.inner
            .execute_transactional_acknowledgement(transition, txn_id)
            .await
    }

    /// Propagate a coordinator outcome to every retained authority.
    pub async fn transaction_outcome(
        &self,
        txn_id: magnetar_proto::TxnId,
        outcome: magnetar_proto::TransactionAcknowledgementOutcome,
    ) -> Result<(), StreamConsumerError> {
        self.inner.transaction_outcome(txn_id, outcome).await
    }

    /// Current delivered-position snapshot.
    #[must_use]
    pub fn delivered_position(&self) -> magnetar_proto::PositionVector {
        self.inner.state.lock().model.delivered_position().clone()
    }

    /// Current aggregate status.
    #[must_use]
    pub fn status(&self) -> magnetar_proto::StreamConsumerStatusSnapshot {
        self.inner.state.lock().model.status()
    }

    /// Resolve schema metadata through the exact child route.
    pub async fn get_schema(
        &self,
        source: &magnetar_proto::SegmentSource,
        version: Option<bytes::Bytes>,
    ) -> Result<magnetar_proto::pb::Schema, StreamConsumerError> {
        let consumer = self.inner.child_consumer(source, None)?;
        Ok(consumer.get_schema(version).await?)
    }

    /// Apply an all-current-leaves aggregate seek.
    pub async fn seek_positions(
        &self,
        positions: &magnetar_proto::PositionVector,
    ) -> Result<(), StreamConsumerError> {
        let actions = {
            let mut state = self.inner.state.lock();
            let actions = state.model.begin_seek(positions)?;
            for action in &actions {
                if let magnetar_proto::StreamConsumerAction::SeekChild {
                    source,
                    child_generation,
                    ..
                } = action
                {
                    let key = (source.segment_id(), *child_generation);
                    state.flow_reservations.remove(&key);
                    state.dispatch_permit_debt.remove(&key);
                }
            }
            actions
        };
        let mut cancellation = SeekCancellation::new(&self.inner);
        let result = self.inner.execute_actions(actions).await;
        cancellation.disarm();
        if let Err(error) = &result {
            self.inner.request_resync(error.to_string());
        }
        result
    }

    /// Await one owned aggregate event.
    pub async fn next_event(&self) -> Result<Option<StreamConsumerEvent>, StreamConsumerError> {
        self.inner.next_event().await
    }

    /// Globally close and join every owned task.
    pub async fn close(&self) -> Result<(), StreamConsumerError> {
        self.inner.close().await
    }

    /// Synchronous final-guard fencing.
    pub fn close_best_effort(&self) {
        self.inner.close_best_effort();
    }
}

impl Drop for StreamConsumer {
    fn drop(&mut self) {
        self.inner.close_best_effort();
    }
}

impl StreamConsumerInner {
    fn track_task(state: &mut AggregateState, handle: ScalableTaskHandle) {
        state.tasks.retain(|task| !task.is_finished());
        if !handle.is_finished() {
            state.tasks.push(handle);
        }
    }

    fn reap_completed_tasks(&self) {
        self.state.lock().tasks.retain(|task| !task.is_finished());
    }

    fn push_event(&self, event: StreamConsumerEvent) {
        let mut state = self.state.lock();
        let excess = state
            .events
            .len()
            .saturating_add(1)
            .saturating_sub(MAX_AGGREGATE_EVENTS);
        state.events.drain(..excess);
        state.events.push_back(event);
        drop(state);
        self.notify.notify_waiters();
    }

    fn push_assignment_event(&self, assignment: &magnetar_proto::ConsumerAssignment) {
        self.push_event(StreamConsumerEvent::AssignmentApplied {
            layout_epoch: assignment.layout_epoch(),
            sources: assignment
                .segments()
                .iter()
                .map(magnetar_proto::AssignedSegment::source)
                .collect(),
        });
    }

    fn push_phase_event(&self, source: &magnetar_proto::SegmentSource) {
        let phase = self
            .state
            .lock()
            .model
            .segment_phase(source.segment_id())
            .cloned();
        let phases = phase.into_iter();
        for phase in phases {
            if let magnetar_proto::SegmentPhase::OpenBlocked(
                magnetar_proto::FlowBlock::OrderingUnprovable(ancestors),
            ) = &phase
            {
                self.push_event(StreamConsumerEvent::OrderingUnprovable {
                    segment_id: source.segment_id(),
                    ancestors: ancestors.clone(),
                });
            }
            self.push_event(StreamConsumerEvent::SegmentPhaseChanged {
                source: source.clone(),
                phase,
            });
        }
    }

    fn child_consumer(
        &self,
        source: &magnetar_proto::SegmentSource,
        generation: Option<magnetar_proto::ChildGeneration>,
    ) -> Result<crate::Consumer, StreamConsumerError> {
        let unavailable = magnetar_proto::StreamConsumerModelError::PositionSourceUnavailable {
            segment_source: source.clone(),
        };
        self.state
            .lock()
            .children
            .get(&source.segment_id())
            .filter(|child| {
                child.source == *source && generation.is_none_or(|value| value == child.generation)
            })
            .map(|child| child.consumer.clone())
            .ok_or(StreamConsumerError::Model(unavailable))
    }

    fn child_open_is_current(
        &self,
        source: &magnetar_proto::SegmentSource,
        controller_incarnation: magnetar_proto::ControllerIncarnation,
        child_generation: magnetar_proto::ChildGeneration,
    ) -> bool {
        let state = self.state.lock();
        Self::child_open_is_current_state(&state, source, controller_incarnation, child_generation)
    }

    fn child_open_is_current_state(
        state: &AggregateState,
        source: &magnetar_proto::SegmentSource,
        controller_incarnation: magnetar_proto::ControllerIncarnation,
        child_generation: magnetar_proto::ChildGeneration,
    ) -> bool {
        !state.close_state.is_closing()
            && state.model.controller_incarnation() == controller_incarnation
            && state.model.accepts_child_result(source, child_generation)
            && state.model.segment_phase(source.segment_id())
                == Some(&magnetar_proto::SegmentPhase::Opening)
    }

    fn spawn_control_task(self: &Arc<Self>, dag: DagSession, controller: ControllerSession) {
        let mut state = self.state.lock();
        if !state.close_state.is_closing() {
            let inner = self.clone();
            let completion_inner = Arc::downgrade(self);
            let handle = self.subscriber.spawn_task_with_completion(
                async move {
                    inner.control_loop(dag, controller).await;
                },
                move || {
                    if let Some(inner) = completion_inner.upgrade() {
                        inner.notify.notify_waiters();
                    }
                },
            );
            Self::track_task(&mut state, handle);
        }
    }

    fn spawn_open_task(
        self: &Arc<Self>,
        source: magnetar_proto::SegmentSource,
        controller_incarnation: magnetar_proto::ControllerIncarnation,
        child_generation: magnetar_proto::ChildGeneration,
    ) {
        let mut state = self.state.lock();
        if !state.close_state.is_closing()
            && state.model.controller_incarnation() == controller_incarnation
            && state.model.accepts_child_result(&source, child_generation)
        {
            let inner = self.clone();
            let completion_inner = Arc::downgrade(self);
            let handle = self.subscriber.spawn_task_with_completion(
                async move {
                    if let Err(error) = inner
                        .open_child(source, controller_incarnation, child_generation)
                        .await
                    {
                        inner.handle_background_error(error.to_string());
                    }
                },
                move || {
                    if let Some(inner) = completion_inner.upgrade() {
                        let mut state = inner.state.lock();
                        state.open_tasks = state.open_tasks.saturating_sub(1);
                        drop(state);
                        inner.notify.notify_waiters();
                    }
                },
            );
            state.open_tasks = state.open_tasks.saturating_add(1);
            Self::track_task(&mut state, handle);
        }
    }

    fn spawn_child_task(
        self: &Arc<Self>,
        source: magnetar_proto::SegmentSource,
        generation: magnetar_proto::ChildGeneration,
        consumer: crate::Consumer,
    ) {
        let mut state = self.state.lock();
        if !state.close_state.is_closing() {
            let inner = self.clone();
            let completion_inner = Arc::downgrade(self);
            let handle = self.subscriber.spawn_task_with_completion(
                async move {
                    inner.child_loop(source, generation, consumer).await;
                },
                move || {
                    if let Some(inner) = completion_inner.upgrade() {
                        inner.notify.notify_waiters();
                    }
                },
            );
            Self::track_task(&mut state, handle);
        }
    }

    fn spawn_actions(self: &Arc<Self>, actions: Vec<magnetar_proto::StreamConsumerAction>) {
        if actions.is_empty() {
            return;
        }
        let mut state = self.state.lock();
        if !state.close_state.is_closing() {
            let inner = self.clone();
            let completion_inner = Arc::downgrade(self);
            let handle = self.subscriber.spawn_task_with_completion(
                async move {
                    if let Err(error) = inner.execute_actions(actions).await {
                        inner.request_resync(error.to_string());
                    }
                },
                move || {
                    if let Some(inner) = completion_inner.upgrade() {
                        inner.notify.notify_waiters();
                    }
                },
            );
            Self::track_task(&mut state, handle);
        }
    }

    fn handle_background_error(self: &Arc<Self>, error: String) {
        let closing = {
            let mut state = self.state.lock();
            let closing = state.close_state.is_closing();
            state.close_error = state
                .close_error
                .take()
                .or(closing.then_some(error.clone()));
            closing
        };
        self.notify.notify_waiters();
        if !closing {
            self.request_resync(error);
        }
    }

    fn fail_closed(&self, reason: String) {
        let should_fence = {
            let mut state = self.state.lock();
            let should_fence = !state.close_state.is_closing();
            state.close_error = state
                .close_error
                .take()
                .or((!should_fence).then_some(reason.clone()));
            if should_fence {
                state.terminal_error = Some(reason.clone());
            }
            should_fence
        };
        if should_fence {
            self.push_event(StreamConsumerEvent::ResyncRequired { reason });
            self.close_best_effort();
        }
        self.notify.notify_waiters();
    }

    async fn open_child(
        self: &Arc<Self>,
        source: magnetar_proto::SegmentSource,
        controller_incarnation: magnetar_proto::ControllerIncarnation,
        child_generation: magnetar_proto::ChildGeneration,
    ) -> Result<(), StreamConsumerError> {
        let descriptor = self
            .state
            .lock()
            .model
            .dag()
            .segment(source.segment_id())
            .expect("model-generated child source exists in its DAG")
            .clone();
        loop {
            if !self.child_open_is_current(&source, controller_incarnation, child_generation) {
                return self
                    .finish_cancelled_open(source, child_generation, None)
                    .await;
            }
            match self
                .subscriber
                .open_segment_consumer(&source, &descriptor, &self.child_options)
                .await
            {
                Ok(consumer) => {
                    let actions = {
                        let mut state = self.state.lock();
                        if Self::child_open_is_current_state(
                            &state,
                            &source,
                            controller_incarnation,
                            child_generation,
                        ) {
                            let actions = state
                                .model
                                .child_opened(source.segment_id(), child_generation)?;
                            state.children.insert(
                                source.segment_id(),
                                ChildRuntime {
                                    source: source.clone(),
                                    generation: child_generation,
                                    consumer: consumer.clone(),
                                },
                            );
                            Some(actions)
                        } else {
                            None
                        }
                    };
                    let Some(actions) = actions else {
                        return self
                            .finish_cancelled_open(source, child_generation, Some(consumer))
                            .await;
                    };
                    self.spawn_child_task(source.clone(), child_generation, consumer);
                    self.push_phase_event(&source);
                    return self.execute_actions(actions).await;
                }
                Err(ClientError::Broker { code, .. })
                    if code == magnetar_proto::pb::ServerError::ConsumerBusy as i32 =>
                {
                    let current = {
                        let mut state = self.state.lock();
                        if Self::child_open_is_current_state(
                            &state,
                            &source,
                            controller_incarnation,
                            child_generation,
                        ) {
                            state
                                .model
                                .child_open_busy(source.segment_id(), child_generation)?;
                            true
                        } else {
                            false
                        }
                    };
                    if !current {
                        return self
                            .finish_cancelled_open(source, child_generation, None)
                            .await;
                    }
                    self.subscriber
                        .sleep(std::time::Duration::from_millis(100))
                        .await;
                }
                Err(error) => {
                    if self.child_open_is_current(&source, controller_incarnation, child_generation)
                    {
                        return Err(error.into());
                    }
                    return self
                        .finish_cancelled_open(source, child_generation, None)
                        .await;
                }
            }
        }
    }

    async fn finish_cancelled_open(
        self: &Arc<Self>,
        source: magnetar_proto::SegmentSource,
        child_generation: magnetar_proto::ChildGeneration,
        consumer: Option<crate::Consumer>,
    ) -> Result<(), StreamConsumerError> {
        let mut close_error = None;
        if let Some(consumer) = consumer {
            let retry = consumer.clone();
            if let Err(error) = consumer.close().await {
                retry.force_close_best_effort();
                close_error = Some(StreamConsumerError::Client(error));
            }
        }
        let closed = {
            let mut state = self.state.lock();
            state
                .model
                .child_closed(source.segment_id(), child_generation)
        };
        let result = match closed {
            Ok(actions) => self.execute_actions(actions).await,
            Err(
                magnetar_proto::StreamConsumerModelError::UnknownChild { .. }
                | magnetar_proto::StreamConsumerModelError::StaleChildGeneration { .. },
            ) => Ok(()),
            Err(error) => Err(error.into()),
        };
        match close_error {
            Some(error) if result.is_ok() => Err(error),
            _ => result,
        }
    }

    async fn execute_actions(
        self: &Arc<Self>,
        actions: Vec<magnetar_proto::StreamConsumerAction>,
    ) -> Result<(), StreamConsumerError> {
        let mut staged_seeks = BTreeMap::new();
        for action in &actions {
            if let magnetar_proto::StreamConsumerAction::SeekChild {
                source,
                child_generation,
                stream_message_id,
                ..
            } = action
            {
                let staged = (|| {
                    let consumer = self.child_consumer(source, Some(*child_generation))?;
                    let message_id = stream_message_id
                        .ordinary_message_id_data()
                        .map_err(magnetar_proto::StreamConsumerModelError::from)?;
                    let seek = consumer.stage_seek_to_message_id_data(message_id);
                    Ok::<_, StreamConsumerError>((consumer, seek))
                })();
                staged_seeks.insert((source.segment_id(), *child_generation), staged);
            }
        }
        let mut actions: VecDeque<_> = actions.into();
        let mut execution_error = None;
        while let Some(action) = actions.pop_front() {
            match action {
                magnetar_proto::StreamConsumerAction::OpenChild {
                    source,
                    controller_incarnation,
                    child_generation,
                    ..
                } => {
                    self.spawn_open_task(source, controller_incarnation, child_generation);
                }
                magnetar_proto::StreamConsumerAction::CancelOpen {
                    source,
                    child_generation,
                    ..
                } => {
                    if let Ok(next) = self
                        .state
                        .lock()
                        .model
                        .child_closed(source.segment_id(), child_generation)
                    {
                        actions.extend(next);
                    }
                }
                magnetar_proto::StreamConsumerAction::StopFlow { source, .. } => {
                    self.push_phase_event(&source);
                }
                magnetar_proto::StreamConsumerAction::GrantFlow {
                    source,
                    child_generation,
                    reservation,
                    ..
                } => {
                    {
                        let mut state = self.state.lock();
                        let key = (source.segment_id(), child_generation);
                        let accepts_flow = !matches!(
                            state.model.segment_phase(source.segment_id()),
                            Some(
                                magnetar_proto::SegmentPhase::Closing
                                    | magnetar_proto::SegmentPhase::Failed
                            )
                        );
                        let consumer = state
                            .children
                            .get(&source.segment_id())
                            .filter(|child| accepts_flow && child.generation == child_generation)
                            .map(|child| child.consumer.clone());
                        if let Some(consumer) = consumer {
                            state.flow_reservations.insert(key, reservation);
                            let debt = state
                                .dispatch_permit_debt
                                .remove(&key)
                                .map(|debt| (debt.session_epoch, debt.permits));
                            consumer.flow_for_aggregate_with_debt(1, debt);
                        }
                    }
                    self.push_phase_event(&source);
                }
                magnetar_proto::StreamConsumerAction::CloseChild {
                    source,
                    child_generation,
                    ..
                } => {
                    let child = {
                        let mut state = self.state.lock();
                        state
                            .flow_reservations
                            .remove(&(source.segment_id(), child_generation));
                        state
                            .dispatch_permit_debt
                            .remove(&(source.segment_id(), child_generation));
                        state
                            .receive
                            .remove_child(source.segment_id(), child_generation);
                        state
                            .children
                            .get(&source.segment_id())
                            .filter(|child| child.generation == child_generation)
                            .cloned()
                    };
                    if let Some(child) = child {
                        let retry = child.consumer.clone();
                        if let Err(error) = child.consumer.close().await {
                            retry.force_close_best_effort();
                            execution_error = Some(StreamConsumerError::Client(error));
                        }
                    }
                    let closed = {
                        let mut state = self.state.lock();
                        if state
                            .children
                            .get(&source.segment_id())
                            .is_some_and(|child| child.generation == child_generation)
                        {
                            state.children.remove(&source.segment_id());
                        }
                        state
                            .model
                            .child_closed(source.segment_id(), child_generation)
                    };
                    if let Ok(next) = closed {
                        actions.extend(next);
                    }
                }
                magnetar_proto::StreamConsumerAction::SeekChild {
                    source,
                    child_generation,
                    ..
                } => {
                    let staged = staged_seeks
                        .remove(&(source.segment_id(), child_generation))
                        .expect("every model-generated seek was staged before execution");
                    let result = match staged {
                        Ok((consumer, seek)) => consumer
                            .complete_staged_seek(seek)
                            .await
                            .map_err(StreamConsumerError::from),
                        Err(error) => Err(error),
                    };
                    match result {
                        Ok(()) => match self
                            .state
                            .lock()
                            .model
                            .seek_completed(source.segment_id(), child_generation)
                        {
                            Ok(next) => actions.extend(next),
                            Err(error) => {
                                let next = self.seek_failed_actions()?;
                                self.spawn_actions(next);
                                actions.clear();
                                execution_error = Some(error.into());
                            }
                        },
                        Err(error) => {
                            let next = self.seek_failed_actions()?;
                            self.spawn_actions(next);
                            actions.clear();
                            execution_error = Some(error);
                        }
                    }
                }
            }
        }
        match execution_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn child_loop(
        self: Arc<Self>,
        source: magnetar_proto::SegmentSource,
        generation: magnetar_proto::ChildGeneration,
        consumer: crate::Consumer,
    ) {
        loop {
            let shutdown = self.notify.notified();
            tokio::pin!(shutdown);
            shutdown.as_mut().enable();
            let should_stop = {
                let state = self.state.lock();
                state.close_state.is_closing() || state.reconnect_requested
            };
            if should_stop {
                return;
            }
            let result = tokio::select! {
                biased;
                () = &mut shutdown => continue,
                result = consumer.receive_deferred_until_end() => result,
            };
            let current = {
                let state = self.state.lock();
                !state.close_state.is_closing()
                    && !state.reconnect_requested
                    && state
                        .children
                        .get(&source.segment_id())
                        .is_some_and(|child| {
                            child.source == source && child.generation == generation
                        })
                    && !matches!(
                        state.model.segment_phase(source.segment_id()),
                        Some(
                            magnetar_proto::SegmentPhase::Closing
                                | magnetar_proto::SegmentPhase::Failed
                        )
                    )
            };
            if !current {
                return;
            }
            match result {
                Ok(Some((session_epoch, message))) => {
                    if let Err(error) = self
                        .message_arrived(
                            source.clone(),
                            generation,
                            session_epoch,
                            &consumer,
                            message,
                        )
                        .await
                    {
                        self.request_resync(error.to_string());
                        return;
                    }
                }
                Ok(None) => {
                    let actions = {
                        let mut state = self.state.lock();
                        state
                            .model
                            .observe_terminal(source.segment_id(), generation)
                            .unwrap_or_default()
                    };
                    if let Err(error) = self.execute_actions(actions).await {
                        self.request_resync(error.to_string());
                    }
                    self.try_complete(source.segment_id(), generation).await;
                    return;
                }
                Err(error) => {
                    self.request_resync(error.to_string());
                    return;
                }
            }
        }
    }

    async fn message_arrived(
        self: &Arc<Self>,
        source: magnetar_proto::SegmentSource,
        generation: magnetar_proto::ChildGeneration,
        session_epoch: u64,
        consumer: &crate::Consumer,
        message: magnetar_proto::DeferredIncomingMessage,
    ) -> Result<(), StreamConsumerError> {
        let acceptance = {
            let mut state = self.state.lock();
            let reservation = state
                .flow_reservations
                .remove(&(source.segment_id(), generation))
                .ok_or_else(|| {
                    StreamConsumerError::Failed(
                        "segment delivered without an aggregate FLOW reservation".to_owned(),
                    )
                })?;
            let state = &mut *state;
            state.receive.accept_entry(
                &mut state.model,
                source.segment_id(),
                generation,
                reservation,
                message,
            )?
        };
        let mut complete = match acceptance {
            magnetar_proto::StreamEntryAcceptance::Buffered { actions } => {
                self.execute_actions(actions).await?;
                return Ok(());
            }
            magnetar_proto::StreamEntryAcceptance::Complete(complete) => complete,
        };

        let transform_bytes = complete.transform_reservation_bytes()?;
        let mut work = Vec::new();
        if transform_bytes > 0 {
            work.push(self.state.lock().model.reserve_decompression(
                source.segment_id(),
                generation,
                transform_bytes,
            )?);
        }

        // Scalable children are opened with the default `Fail` crypto policy.
        match consumer.post_process_deferred(complete.message_mut()) {
            crate::consumer::PostProcessOutcome::Fail(error) => return Err(error.into()),
            crate::consumer::PostProcessOutcome::Discard => {
                return Err(StreamConsumerError::Failed(DISCARD_POLICY_ERROR.to_owned()));
            }
            crate::consumer::PostProcessOutcome::Deliver => {}
        }

        let actions = {
            let mut state = self.state.lock();
            let state = &mut *state;
            let transition = state.receive.finalize_entry(
                &mut state.model,
                source.segment_id(),
                generation,
                complete,
                &work,
            )?;
            for queued in transition.messages {
                state.queue.push_back(QueuedMessage {
                    source: source.clone(),
                    generation,
                    message: queued.message,
                    delivery: QueuedDelivery::Fresh {
                        reservation: queued.reservation,
                        message_id_data: queued.message_id_data,
                    },
                });
            }
            if transition.permit_debt > 0 {
                state.dispatch_permit_debt.insert(
                    (source.segment_id(), generation),
                    DispatchPermitDebt {
                        session_epoch,
                        permits: transition.permit_debt,
                    },
                );
            }
            transition.actions
        };
        self.notify.notify_waiters();
        self.execute_actions(actions).await?;
        Ok(())
    }

    async fn reserve_batch(
        &self,
        max_messages: usize,
        max_bytes: usize,
    ) -> Result<Vec<StreamConsumerMessage>, StreamConsumerError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let result = {
                let mut state = self.state.lock();
                if !state.queue.is_empty() {
                    let mut count = 0usize;
                    let mut bytes = 0usize;
                    for queued in &state.queue {
                        if count == max_messages {
                            break;
                        }
                        let next = bytes.saturating_add(queued.message.payload.len());
                        if count > 0 && next > max_bytes {
                            break;
                        }
                        count += 1;
                        bytes = next;
                    }
                    let mut staged = state.model.clone();
                    let mut fresh_tokens = VecDeque::new();
                    for queued in state.queue.iter().take(count) {
                        if let QueuedDelivery::Fresh {
                            reservation,
                            message_id_data,
                        } = &queued.delivery
                        {
                            let stream_message_id =
                                magnetar_proto::StreamMessageId::from_message_id_data(
                                    queued.source.clone(),
                                    message_id_data,
                                )
                                .map_err(magnetar_proto::StreamConsumerModelError::from)?;
                            fresh_tokens.push_back(staged.issue_delivery(
                                queued.source.segment_id(),
                                queued.generation,
                                stream_message_id,
                                *reservation,
                            )?);
                        }
                    }
                    state.model = staged;
                    let messages = state
                        .queue
                        .drain(..count)
                        .map(|queued| {
                            let token = match queued.delivery {
                                QueuedDelivery::Fresh { .. } => fresh_tokens
                                    .pop_front()
                                    .expect("every fresh queue entry issued one token"),
                                QueuedDelivery::Restored { token } => token,
                            };
                            StreamConsumerMessage {
                                message: queued.message,
                                token,
                            }
                        })
                        .collect();
                    Some(Ok(messages))
                } else if let Some(error) = &state.terminal_error {
                    Some(Err(StreamConsumerError::Failed(error.clone())))
                } else if state.close_state.is_closing() {
                    Some(Err(StreamConsumerError::Closed))
                } else {
                    None
                }
            };
            if let Some(result) = result {
                return result;
            }
            notified.await;
        }
    }

    fn restore_deliveries(
        &self,
        messages: Vec<StreamConsumerMessage>,
    ) -> Result<(), StreamConsumerError> {
        let mut state = self.state.lock();
        if state.close_state.is_closing() {
            return Err(StreamConsumerError::Closed);
        }
        let mut restored = Vec::with_capacity(messages.len());
        for message in messages {
            let source = message.token.stream_message_id().source().clone();
            let generation = state.model.validate_delivery_restoration(&message.token)?;
            let sequence = message.token.dequeue_sequence();
            restored.push((
                sequence,
                QueuedMessage {
                    source,
                    generation,
                    message: message.message,
                    delivery: QueuedDelivery::Restored {
                        token: message.token,
                    },
                },
            ));
        }
        restored.sort_by_key(|(sequence, _)| *sequence);
        for (sequence, queued) in restored {
            let index = state
                .queue
                .iter()
                .position(|existing| match &existing.delivery {
                    QueuedDelivery::Fresh { .. } => true,
                    QueuedDelivery::Restored { token } => token.dequeue_sequence() > sequence,
                })
                .unwrap_or(state.queue.len());
            state.queue.insert(index, queued);
        }
        drop(state);
        self.notify.notify_waiters();
        Ok(())
    }

    async fn execute_acknowledgement(
        self: &Arc<Self>,
        transition: magnetar_proto::AcknowledgementTransition,
    ) -> Result<(), StreamConsumerError> {
        let mut cancellation = AcknowledgementCancellation::new(self, &transition.authority);
        let mut confirmed_sources = BTreeSet::new();
        let mut confirmed = Vec::new();
        let mut failed = Vec::new();
        for component in &transition.components {
            let positions = component_positions(component)?;
            let message_id_data = component
                .message_id_data()
                .map_err(magnetar_proto::StreamConsumerModelError::from)?;
            let result =
                match self.child_consumer(component.source(), Some(component.child_generation())) {
                    Ok(consumer) => {
                        let ack_type = if component.cumulative() {
                            magnetar_proto::pb::command_ack::AckType::Cumulative
                        } else {
                            magnetar_proto::pb::command_ack::AckType::Individual
                        };
                        consumer
                            .ack_stream_component(
                                component.message_ids().to_vec(),
                                message_id_data,
                                ack_type,
                                None,
                            )
                            .await
                    }
                    Err(error) => {
                        failed.extend(
                            positions
                                .into_iter()
                                .map(|position| StreamAckFailure::from_error(position, &error)),
                        );
                        continue;
                    }
                };
            match result {
                Ok(()) => {
                    confirmed_sources.insert(component.source().clone());
                    confirmed.extend(positions);
                }
                Err(error) => {
                    failed.extend(
                        positions
                            .into_iter()
                            .map(|position| StreamAckFailure::from_error(position, &error)),
                    );
                }
            }
        }
        let actions = self
            .state
            .lock()
            .model
            .settle_acknowledgement(&transition.authority, &confirmed_sources)?;
        cancellation.disarm();
        self.try_complete_components(&transition.components).await;
        self.execute_actions(actions).await?;
        if failed.is_empty() {
            Ok(())
        } else {
            Err(StreamConsumerError::PartialAcknowledgement { confirmed, failed })
        }
    }

    async fn execute_transactional_acknowledgement(
        self: &Arc<Self>,
        transition: magnetar_proto::TransactionAcknowledgementTransition,
        txn_id: magnetar_proto::TxnId,
    ) -> Result<(), StreamConsumerError> {
        let magnetar_proto::TransactionAcknowledgementTransition {
            authority,
            components,
        } = transition;
        let mut cancellation = TransactionAcknowledgementCancellation::new(self, authority);
        for component in &components {
            if let Err(error) = self
                .ensure_transaction_registration(txn_id, component.source().clone())
                .await
            {
                let actions = {
                    let mut state = self.state.lock();
                    if state.close_state.is_closing() {
                        Vec::new()
                    } else {
                        state
                            .model
                            .cancel_transactional_acknowledgement(cancellation.authority()?)?
                    }
                };
                cancellation.disarm();
                self.execute_actions(actions).await?;
                return Err(error);
            }
            let consumer =
                self.child_consumer(component.source(), Some(component.child_generation()))?;
            let message_id_data = component
                .message_id_data()
                .map_err(magnetar_proto::StreamConsumerModelError::from)?;
            let ack_type = if component.cumulative() {
                magnetar_proto::pb::command_ack::AckType::Cumulative
            } else {
                magnetar_proto::pb::command_ack::AckType::Individual
            };
            let result = consumer
                .ack_stream_component(
                    component.message_ids().to_vec(),
                    message_id_data,
                    ack_type,
                    Some(txn_id),
                )
                .await;
            if let Err(error) = result {
                let actions = {
                    let mut state = self.state.lock();
                    if state.close_state.is_closing() {
                        Vec::new()
                    } else {
                        state
                            .model
                            .cancel_transactional_acknowledgement(cancellation.authority()?)?
                    }
                };
                cancellation.disarm();
                self.execute_actions(actions).await?;
                return Err(error.into());
            }
        }
        let actions = {
            let mut state = self.state.lock();
            if state.close_state.is_closing() {
                cancellation.disarm();
                Some(Vec::new())
            } else {
                state
                    .pending_transactions
                    .entry(txn_id)
                    .or_default()
                    .push(cancellation.take()?);
                None
            }
        };
        if let Some(actions) = actions {
            self.execute_actions(actions).await?;
            return Err(StreamConsumerError::Closed);
        }
        Ok(())
    }

    async fn ensure_transaction_registration(
        self: &Arc<Self>,
        txn_id: magnetar_proto::TxnId,
        source: magnetar_proto::SegmentSource,
    ) -> Result<(), StreamConsumerError> {
        let key = (txn_id, source.clone());
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let leader = {
                let mut state = self.state.lock();
                if state.close_state.is_closing() {
                    return Err(StreamConsumerError::Closed);
                }
                match state.transaction_registrations.get(&key) {
                    Some(TransactionRegistration::Registered) => return Ok(()),
                    Some(TransactionRegistration::Failed(error)) => {
                        return Err(StreamConsumerError::Failed(error.clone()));
                    }
                    Some(TransactionRegistration::Pending) => false,
                    None => {
                        state
                            .transaction_registrations
                            .insert(key.clone(), TransactionRegistration::Pending);
                        true
                    }
                }
            };
            if !leader {
                notified.await;
                continue;
            }
            let mut cancellation = TransactionRegistrationCancellation::new(self, key.clone());
            let result = self
                .subscriber
                .add_subscription_to_txn(
                    txn_id,
                    source.topic().to_owned(),
                    self.subscription.clone(),
                )
                .await;
            let mut state = self.state.lock();
            if state.close_state.is_closing() {
                state.transaction_registrations.remove(&key);
                cancellation.disarm();
                drop(state);
                self.notify.notify_waiters();
                return Err(StreamConsumerError::Closed);
            }
            state.transaction_registrations.insert(
                key.clone(),
                match &result {
                    Ok(()) => TransactionRegistration::Registered,
                    Err(error) => TransactionRegistration::Failed(error.to_string()),
                },
            );
            cancellation.disarm();
            drop(state);
            self.notify.notify_waiters();
            return result.map_err(Into::into);
        }
    }

    async fn transaction_outcome(
        self: &Arc<Self>,
        txn_id: magnetar_proto::TxnId,
        outcome: magnetar_proto::TransactionAcknowledgementOutcome,
    ) -> Result<(), StreamConsumerError> {
        let completion = {
            let mut state = self.state.lock();
            if state.close_state.is_closing() {
                return Err(StreamConsumerError::Closed);
            }
            let completion = match state.transaction_outcomes.get(&txn_id) {
                Some(completion) if completion.outcome == outcome => completion.clone(),
                Some(completion) => {
                    return Err(StreamConsumerError::Failed(format!(
                        "transaction {txn_id:?} outcome changed from {:?} to {outcome:?}",
                        completion.outcome
                    )));
                }
                None => {
                    let completion = Arc::new(TransactionOutcomeCompletion::new(outcome));
                    state
                        .transaction_outcomes
                        .insert(txn_id, completion.clone());
                    completion
                }
            };
            if completion.try_start() {
                let inner = self.clone();
                let task_completion = completion.clone();
                let dropped_completion = completion.clone();
                let completion_inner = Arc::downgrade(self);
                let handle = self.subscriber.spawn_task_with_completion(
                    async move {
                        let result = inner
                            .propagate_transaction_outcome(txn_id, outcome, &task_completion)
                            .await;
                        task_completion.finish(result.map_err(|error| error.to_string()));
                    },
                    move || {
                        dropped_completion.finish(Err(
                            "transaction outcome propagation was interrupted".to_owned(),
                        ));
                        if let Some(inner) = completion_inner.upgrade() {
                            inner.notify.notify_waiters();
                        }
                    },
                );
                Self::track_task(&mut state, handle);
            }
            completion
        };
        completion.wait().await.map_err(StreamConsumerError::Failed)
    }

    async fn propagate_transaction_outcome(
        self: &Arc<Self>,
        txn_id: magnetar_proto::TxnId,
        outcome: magnetar_proto::TransactionAcknowledgementOutcome,
        completion: &TransactionOutcomeCompletion,
    ) -> Result<(), StreamConsumerError> {
        let work_installed = completion.work.lock().is_some();
        if !work_installed {
            let (work, consumers) = {
                let mut state = self.state.lock();
                state
                    .transaction_registrations
                    .retain(|(registered_txn, _), _| *registered_txn != txn_id);
                let authorities = state
                    .pending_transactions
                    .remove(&txn_id)
                    .unwrap_or_default();
                let actions =
                    if outcome == magnetar_proto::TransactionAcknowledgementOutcome::Unknown {
                        state.pending_transactions.clear();
                        state.transaction_registrations.clear();
                        state.reconnect_requested = true;
                        state.model.require_resync()?
                    } else {
                        let mut actions = Vec::new();
                        for authority in authorities {
                            actions.extend(
                                state
                                    .model
                                    .settle_transactional_acknowledgement(&authority, outcome)?,
                            );
                        }
                        actions
                    };
                let completions = state
                    .children
                    .values()
                    .map(|child| (child.source.segment_id(), child.generation))
                    .collect::<Vec<_>>();
                let consumers = state
                    .children
                    .values()
                    .map(|child| child.consumer.clone())
                    .collect::<Vec<_>>();
                (
                    TransactionOutcomeWork {
                        actions: actions.into(),
                        completions: completions.into(),
                    },
                    consumers,
                )
            };
            let committed = outcome == magnetar_proto::TransactionAcknowledgementOutcome::Committed;
            for consumer in consumers {
                consumer.settle_transactional_acks(txn_id, committed);
            }
            *completion.work.lock() = Some(work);
        }
        #[cfg(test)]
        if let Some(hook) = self.transaction_outcome_park_hook.clone() {
            hook.reached.notify_one();
            hook.release.notified().await;
        }
        loop {
            let action = completion
                .work
                .lock()
                .as_ref()
                .and_then(|work| work.actions.front().cloned());
            let Some(action) = action else { break };
            self.execute_actions(vec![action]).await?;
            completion
                .work
                .lock()
                .as_mut()
                .expect("transaction outcome work remains installed")
                .actions
                .pop_front();
        }
        if outcome != magnetar_proto::TransactionAcknowledgementOutcome::Unknown {
            loop {
                let component = completion
                    .work
                    .lock()
                    .as_ref()
                    .and_then(|work| work.completions.front().copied());
                let Some((segment_id, generation)) = component else {
                    break;
                };
                self.try_complete(segment_id, generation).await;
                completion
                    .work
                    .lock()
                    .as_mut()
                    .expect("transaction outcome work remains installed")
                    .completions
                    .pop_front();
            }
        }
        self.push_event(StreamConsumerEvent::TransactionOutcome { txn_id, outcome });
        Ok(())
    }

    async fn try_complete_components(
        self: &Arc<Self>,
        components: &[magnetar_proto::AcknowledgementComponent],
    ) {
        for component in components {
            self.try_complete(
                component.source().segment_id(),
                component.child_generation(),
            )
            .await;
        }
    }

    async fn try_complete(
        self: &Arc<Self>,
        segment_id: magnetar_proto::SegmentId,
        generation: magnetar_proto::ChildGeneration,
    ) {
        let actions = {
            let mut state = self.state.lock();
            match state.model.complete_segment(segment_id, generation) {
                Ok(actions) => Some(actions),
                Err(
                    magnetar_proto::StreamConsumerModelError::SegmentNotComplete { .. }
                    | magnetar_proto::StreamConsumerModelError::InvalidAggregatePhase { .. }
                    | magnetar_proto::StreamConsumerModelError::UnknownChild { .. }
                    | magnetar_proto::StreamConsumerModelError::StaleChildGeneration { .. },
                ) => None,
                Err(error) => {
                    state.terminal_error = Some(error.to_string());
                    None
                }
            }
        };
        self.notify.notify_waiters();
        if let Some(actions) = actions
            && let Err(error) = self.execute_actions(actions).await
        {
            self.request_resync(error.to_string());
        }
    }

    fn request_resync(self: &Arc<Self>, reason: String) {
        let actions = {
            let mut state = self.state.lock();
            if state.close_state.is_closing() {
                return;
            }
            state.reconnect_requested = true;
            state.queue.clear();
            state.receive = magnetar_proto::StreamReceiveState::default();
            state.dispatch_permit_debt.clear();
            match state.model.require_resync() {
                Ok(actions) => actions,
                Err(error) => {
                    state.terminal_error = Some(error.to_string());
                    Vec::new()
                }
            }
        };
        self.push_event(StreamConsumerEvent::ResyncRequired { reason });
        self.notify.notify_waiters();
        self.spawn_actions(actions);
    }

    fn seek_failed_actions(
        &self,
    ) -> Result<Vec<magnetar_proto::StreamConsumerAction>, StreamConsumerError> {
        let actions = {
            let mut state = self.state.lock();
            state.reconnect_requested = true;
            state.queue.clear();
            state.receive = magnetar_proto::StreamReceiveState::default();
            state.dispatch_permit_debt.clear();
            state.model.seek_failed()?
        };
        self.notify.notify_waiters();
        Ok(actions)
    }

    async fn apply_aligned_control_plane(
        self: &Arc<Self>,
        dag: &DagSession,
        controller: &ControllerSession,
    ) -> Result<(), StreamConsumerError> {
        let snapshot = dag.snapshot().clone();
        let assignment = controller.assignment().clone();
        if snapshot.epoch() != assignment.layout_epoch() {
            return Ok(());
        }
        let (actions, assignment_changed) = {
            let mut state = self.state.lock();
            let assignment_changed = state.model.assignment() != Some(&assignment);
            let actions = state.model.apply_control_plane_for(
                controller.incarnation(),
                snapshot,
                assignment.clone(),
            )?;
            (actions, assignment_changed)
        };
        if assignment_changed {
            self.push_assignment_event(&assignment);
        }
        self.execute_actions(actions).await
    }

    async fn reopen_dag_watch(
        self: &Arc<Self>,
        controller: &mut ControllerSession,
    ) -> Result<DagSession, StreamConsumerError> {
        let mut replacement = self.subscriber.open_dag_session(&self.topic).await?;
        SegmentSubscriber::align_control_plane(&mut replacement, controller).await?;
        self.apply_aligned_control_plane(&replacement, controller)
            .await?;
        Ok(replacement)
    }

    async fn control_loop(self: Arc<Self>, mut dag: DagSession, mut controller: ControllerSession) {
        loop {
            self.reap_completed_tasks();
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().close_state.is_closing() {
                break;
            }
            let (reconnect_requested, children_fenced) = {
                let state = self.state.lock();
                (
                    state.reconnect_requested,
                    state.open_tasks == 0 && state.children.is_empty(),
                )
            };
            if reconnect_requested && !children_fenced {
                notified.await;
                continue;
            }
            if reconnect_requested {
                match self.reconnect_control_plane(dag, controller).await {
                    Some((new_dag, new_controller)) => {
                        dag = new_dag;
                        controller = new_controller;
                        continue;
                    }
                    None => return,
                }
            }
            #[cfg(test)]
            if let Some(hook) = &self.control_park_hook {
                hook.reached.notify_one();
                hook.release.notified().await;
            }
            let update = tokio::select! {
                biased;
                () = &mut notified => ControlUpdate::Wake,
                result = dag.next() => ControlUpdate::Dag(result),
                result = controller.next_assignment() => ControlUpdate::Assignment(result),
            };
            match update {
                ControlUpdate::Dag(Ok(
                    ScalableEvent::DagUpdated { .. } | ScalableEvent::LookupResolved { .. },
                ))
                | ControlUpdate::Assignment(Ok(_)) => {
                    if let Err(error) = self.apply_aligned_control_plane(&dag, &controller).await {
                        self.request_resync(error.to_string());
                    }
                }
                ControlUpdate::Dag(Ok(ScalableEvent::DagWatchClosed { reason, .. })) => {
                    self.push_event(StreamConsumerEvent::ResyncRequired {
                        reason: reason.unwrap_or_else(|| "scalable DAG watch closed".to_owned()),
                    });
                    match self.reopen_dag_watch(&mut controller).await {
                        Ok(replacement) => {
                            let previous = core::mem::replace(&mut dag, replacement);
                            previous.close();
                            self.push_assignment_event(controller.assignment());
                        }
                        Err(error) => {
                            self.fail_closed(error.to_string());
                            break;
                        }
                    }
                }
                ControlUpdate::Wake | ControlUpdate::Dag(Ok(_)) => {}
                ControlUpdate::Dag(Err(error)) | ControlUpdate::Assignment(Err(error)) => {
                    if route_error_is_recoverable(&error) {
                        self.request_resync(error.to_string());
                    } else {
                        self.fail_closed(error.to_string());
                        break;
                    }
                }
            }
        }
        controller.close();
        dag.close();
    }

    async fn wait_for_close(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().close_state.is_closing() {
                return;
            }
            notified.await;
        }
    }

    async fn retry_control_plane_error(&self, error: ClientError) -> bool {
        if control_plane_error_is_terminal(&error) {
            self.fail_closed(error.to_string());
            return false;
        }
        let reason = match error {
            ClientError::ScalableAssignmentRejected { reason } => reason,
            error => error.to_string(),
        };
        self.push_event(StreamConsumerEvent::ResyncRequired { reason });
        self.subscriber
            .sleep(std::time::Duration::from_millis(100))
            .await;
        true
    }

    async fn reconnect_control_plane(
        self: &Arc<Self>,
        dag: DagSession,
        controller: ControllerSession,
    ) -> Option<(DagSession, ControllerSession)> {
        controller.close();
        dag.close();
        loop {
            if self.state.lock().close_state.is_closing() {
                return None;
            }
            let mut opened_dag = match self.subscriber.open_dag_session(&self.topic).await {
                Ok(dag) => dag,
                Err(error) => {
                    if !self.retry_control_plane_error(error).await {
                        return None;
                    }
                    continue;
                }
            };
            match self
                .subscriber
                .open_controller_session_with_id(
                    &opened_dag,
                    &self.subscription,
                    &self.consumer_name,
                    self.consumer_id,
                )
                .await
            {
                Ok(mut controller) => {
                    let aligned = {
                        let alignment = SegmentSubscriber::align_control_plane(
                            &mut opened_dag,
                            &mut controller,
                        );
                        tokio::pin!(alignment);
                        tokio::select! {
                            biased;
                            () = self.wait_for_close() => return None,
                            result = &mut alignment => result,
                        }
                    };
                    if let Err(error) = aligned {
                        controller.close();
                        opened_dag.close();
                        if !self.retry_control_plane_error(error).await {
                            return None;
                        }
                        continue;
                    }
                    let incarnation = controller.incarnation();
                    let assignment = controller.assignment().clone();
                    let transition = {
                        let mut state = self.state.lock();
                        let transition = state.model.apply_reconnect_baseline(
                            incarnation,
                            opened_dag.snapshot().clone(),
                            assignment.clone(),
                        );
                        if transition.is_ok() {
                            state.controller_registration =
                                Some(ControllerRegistration::from_session(&controller));
                            state.reconnect_requested = false;
                        }
                        transition
                    };
                    let actions = match transition {
                        Ok(actions) => actions,
                        Err(error) => {
                            controller.close();
                            opened_dag.close();
                            self.fail_closed(error.to_string());
                            return None;
                        }
                    };
                    self.push_assignment_event(&assignment);
                    if let Err(error) = self.execute_actions(actions).await {
                        self.request_resync(error.to_string());
                        continue;
                    }
                    return Some((opened_dag, controller));
                }
                Err(error) => {
                    opened_dag.close();
                    if !self.retry_control_plane_error(error).await {
                        return None;
                    }
                }
            }
        }
    }

    async fn next_event(&self) -> Result<Option<StreamConsumerEvent>, StreamConsumerError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let ready = {
                let mut state = self.state.lock();
                if let Some(event) = state.events.pop_front() {
                    Some(Ok(Some(event)))
                } else if state.close_state == AggregateCloseState::Closed {
                    Some(Ok(None))
                } else {
                    state
                        .terminal_error
                        .as_ref()
                        .map(|error| Err(StreamConsumerError::Failed(error.clone())))
                }
            };
            if let Some(result) = ready {
                return result;
            }
            notified.await;
        }
    }

    async fn close(self: &Arc<Self>) -> Result<(), StreamConsumerError> {
        let (children, registration, tasks, cancelled_opens) = loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let cleanup = {
                let mut state = self.state.lock();
                if state.close_state == AggregateCloseState::Closed {
                    return match &state.close_error {
                        Some(error) => Err(StreamConsumerError::Failed(error.clone())),
                        None => Ok(()),
                    };
                }
                if state.close_state == AggregateCloseState::Closing {
                    None
                } else {
                    let actions = if state.close_state == AggregateCloseState::Fenced {
                        Vec::new()
                    } else {
                        state.model.close()?
                    };
                    state.close_state = AggregateCloseState::Closing;
                    state.queue.clear();
                    state.receive = magnetar_proto::StreamReceiveState::default();
                    state.flow_reservations.clear();
                    state.dispatch_permit_debt.clear();
                    state.pending_transactions.clear();
                    state.transaction_registrations.clear();
                    state.transaction_outcomes.clear();
                    let cancelled_opens: Vec<_> = actions
                        .into_iter()
                        .filter_map(|action| match action {
                            magnetar_proto::StreamConsumerAction::CancelOpen {
                                source,
                                child_generation,
                                ..
                            } => Some((source.segment_id(), child_generation)),
                            _ => None,
                        })
                        .collect();
                    Some((
                        core::mem::take(&mut state.children),
                        state.controller_registration.take(),
                        core::mem::take(&mut state.tasks),
                        cancelled_opens,
                    ))
                }
            };
            if let Some(cleanup) = cleanup {
                break cleanup;
            }
            notified.await;
        };
        if let Some(registration) = registration {
            registration.close();
        }
        self.notify.notify_waiters();
        let mut first_error = None;
        for (segment_id, child) in children {
            let generation = child.generation;
            if !child.consumer.is_closed() {
                let retry = child.consumer.clone();
                if let Err(error) = child.consumer.close().await {
                    retry.force_close_best_effort();
                    if first_error.is_none() {
                        first_error = Some(StreamConsumerError::Client(error));
                    }
                }
            }
            let _ = self.state.lock().model.child_closed(segment_id, generation);
        }
        for task in tasks {
            let _ = task.join().await;
        }
        for (segment_id, generation) in cancelled_opens {
            let _ = self.state.lock().model.child_closed(segment_id, generation);
        }
        {
            let mut state = self.state.lock();
            if first_error.is_none()
                && let Some(error) = &state.close_error
            {
                first_error = Some(StreamConsumerError::Failed(error.clone()));
            }
            if let Some(error) = &first_error {
                state.close_error = Some(error.to_string());
            }
            state.close_state = AggregateCloseState::Closed;
            state.events.push_back(StreamConsumerEvent::Closed);
        }
        self.notify.notify_waiters();
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn close_best_effort(&self) {
        let (children, registration) = {
            let mut state = self.state.lock();
            if state.close_state.is_closing() {
                return;
            }
            let _ = state.model.close();
            state.close_state = AggregateCloseState::Fenced;
            state.queue.clear();
            state.receive = magnetar_proto::StreamReceiveState::default();
            state.flow_reservations.clear();
            state.dispatch_permit_debt.clear();
            state.pending_transactions.clear();
            state.transaction_registrations.clear();
            state.transaction_outcomes.clear();
            (
                state
                    .children
                    .values()
                    .map(|child| child.consumer.clone())
                    .collect::<Vec<_>>(),
                state.controller_registration.clone(),
            )
        };
        if let Some(registration) = registration {
            registration.close();
        }
        for child in children {
            child.close_best_effort();
        }
        self.notify.notify_waiters();
    }
}

fn component_positions(
    component: &magnetar_proto::AcknowledgementComponent,
) -> Result<Vec<magnetar_proto::StreamMessageId>, StreamConsumerError> {
    component
        .message_id_data()
        .map_err(magnetar_proto::StreamConsumerModelError::from)?
        .iter()
        .map(|message_id| {
            magnetar_proto::StreamMessageId::from_message_id_data(
                component.source().clone(),
                message_id,
            )
            .map_err(magnetar_proto::StreamConsumerModelError::from)
            .map_err(StreamConsumerError::from)
        })
        .collect()
}

struct TaskCompletion<C: FnOnce()> {
    completed: Arc<AtomicBool>,
    on_complete: Option<C>,
}

impl<C: FnOnce()> TaskCompletion<C> {
    fn new(completed: Arc<AtomicBool>, on_complete: C) -> Self {
        Self {
            completed,
            on_complete: Some(on_complete),
        }
    }
}

impl<C: FnOnce()> Drop for TaskCompletion<C> {
    fn drop(&mut self) {
        self.completed.store(true, AtomicOrdering::Release);
        if let Some(on_complete) = self.on_complete.take() {
            on_complete();
        }
    }
}

/// Cooperative aggregate-task ownership handle.
#[derive(Debug)]
pub struct ScalableTaskHandle {
    join: Option<tokio::task::JoinHandle<()>>,
    completed: Arc<AtomicBool>,
}

impl ScalableTaskHandle {
    fn is_finished(&self) -> bool {
        self.completed.load(AtomicOrdering::Acquire)
    }

    /// Abort the task. Idempotent.
    pub fn abort(&mut self) {
        if let Some(handle) = self.join.take() {
            handle.abort();
        }
    }

    /// Await provider-confirmed task termination. Aggregate close first
    /// signals cooperative shutdown, then joins every owned task through this
    /// operation.
    pub async fn join(mut self) -> Result<(), ClientError> {
        let Some(handle) = self.join.take() else {
            return Ok(());
        };
        handle
            .await
            .map_err(|error| ClientError::Other(format!("scalable task failed: {error}")))
    }
}

impl Drop for ScalableTaskHandle {
    fn drop(&mut self) {
        self.abort();
    }
}

/// Exclusively-owned DAG route and its latest validated snapshot.
#[derive(Debug)]
pub struct DagSession {
    shared: Arc<ConnectionShared>,
    route: ScalableRoute,
    session_id: u64,
    requested_topic: String,
    resolved_topic_name: Option<String>,
    controller_broker_url: Option<String>,
    controller_broker_url_tls: Option<String>,
    snapshot: magnetar_proto::DagSnapshot,
    closed: bool,
}

impl DagSession {
    /// Client-allocated watch id.
    #[must_use]
    pub const fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Canonical parent identity returned by the broker.
    #[must_use]
    pub fn resolved_topic_name(&self) -> Option<&str> {
        self.resolved_topic_name.as_deref()
    }

    /// Latest atomically validated DAG.
    #[must_use]
    pub const fn snapshot(&self) -> &magnetar_proto::DagSnapshot {
        &self.snapshot
    }

    /// Await and apply the next DAG event from this route.
    pub async fn next(&mut self) -> Result<ScalableEvent, ClientError> {
        let event = self.route.next().await?;
        match &event {
            ScalableEvent::DagUpdated { snapshot, .. } => self.snapshot = snapshot.clone(),
            ScalableEvent::DagWatchClosed { .. } => self.closed = true,
            _ => {}
        }
        Ok(event)
    }

    /// Close the watch locally and stage the protocol close command.
    pub fn close(mut self) {
        self.close_inner();
    }

    fn close_inner(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.route.close();
        self.shared
            .inner
            .lock()
            .close_scalable_topic_session(self.session_id);
        self.shared.driver_waker.notify_one();
    }
}

impl Drop for DagSession {
    fn drop(&mut self) {
        self.close_inner();
    }
}

/// Exclusively-owned controller registration route.
#[derive(Debug)]
pub struct ControllerSession {
    shared: Arc<ConnectionShared>,
    route: ScalableRoute,
    consumer_id: u64,
    incarnation: magnetar_proto::ControllerIncarnation,
    assignment: magnetar_proto::ConsumerAssignment,
    registration_topic: String,
    subscription: String,
    consumer_name: String,
}

impl ControllerSession {
    /// Runtime-owned wire consumer id.
    #[must_use]
    pub const fn consumer_id(&self) -> u64 {
        self.consumer_id
    }

    /// Local controller-connection incarnation.
    #[must_use]
    pub const fn incarnation(&self) -> magnetar_proto::ControllerIncarnation {
        self.incarnation
    }

    /// Latest authoritative full assignment.
    #[must_use]
    pub const fn assignment(&self) -> &magnetar_proto::ConsumerAssignment {
        &self.assignment
    }

    /// Canonical parent topic retained across controller reconnects.
    #[must_use]
    pub fn registration_topic(&self) -> &str {
        &self.registration_topic
    }

    /// Stable subscription retained across controller reconnects.
    #[must_use]
    pub fn subscription(&self) -> &str {
        &self.subscription
    }

    /// Stable consumer name retained across controller reconnects.
    #[must_use]
    pub fn consumer_name(&self) -> &str {
        &self.consumer_name
    }

    /// Await the next accepted full assignment. Exact duplicates and stale
    /// incarnations are filtered by the proto session and typed route.
    pub async fn next_assignment(
        &mut self,
    ) -> Result<magnetar_proto::ConsumerAssignment, ClientError> {
        loop {
            match self.route.next().await? {
                ScalableEvent::AssignmentChanged {
                    incarnation,
                    assignment,
                    ..
                } if incarnation == self.incarnation => {
                    self.assignment = assignment.clone();
                    return Ok(assignment);
                }
                ScalableEvent::ConsumerRejected {
                    incarnation,
                    reason,
                    ..
                } if incarnation == self.incarnation => {
                    return Err(ClientError::ScalableAssignmentRejected { reason });
                }
                _ => {}
            }
        }
    }

    /// Remove the local route. M1 has no wire unregister command, so this does
    /// not claim broker-side membership removal while the pooled connection lives.
    pub fn close(&self) {
        self.route.close();
        self.shared
            .inner
            .lock()
            .remove_scalable_consumer_registration(self.consumer_id, self.incarnation);
        self.shared.driver_waker.notify_one();
    }
}

impl Drop for ControllerSession {
    fn drop(&mut self) {
        self.close();
    }
}

/// Scalable control-plane event family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScalableRouteFamily {
    /// Topic-layout lookup and DAG updates.
    Dag,
    /// Controller assignment registration and updates.
    Consumer,
}

/// Typed single-owner route key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScalableRouteKey {
    family: ScalableRouteFamily,
    id: u64,
    incarnation: magnetar_proto::ControllerIncarnation,
}

impl ScalableRouteKey {
    /// Route one DAG-watch session.
    #[must_use]
    pub const fn dag(session_id: u64, incarnation: magnetar_proto::ControllerIncarnation) -> Self {
        Self {
            family: ScalableRouteFamily::Dag,
            id: session_id,
            incarnation,
        }
    }

    /// Route one scalable-consumer registration.
    #[must_use]
    pub const fn consumer(
        consumer_id: u64,
        incarnation: magnetar_proto::ControllerIncarnation,
    ) -> Self {
        Self {
            family: ScalableRouteFamily::Consumer,
            id: consumer_id,
            incarnation,
        }
    }

    /// Event family.
    #[must_use]
    pub const fn family(self) -> ScalableRouteFamily {
        self.family
    }

    /// Session or consumer id.
    #[must_use]
    pub const fn id(self) -> u64 {
        self.id
    }

    /// Local controller-connection incarnation.
    #[must_use]
    pub const fn incarnation(self) -> magnetar_proto::ControllerIncarnation {
        self.incarnation
    }
}

/// Failure of an owned scalable route.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScalableRouteError {
    /// The bounded route overflowed and the owner must resynchronize.
    #[error("scalable route overflowed its {capacity}-event bound")]
    Overflow {
        /// Fixed route capacity.
        capacity: usize,
    },
    /// The physical connection was replaced; old-incarnation events are fenced.
    #[error("scalable route belongs to a replaced connection")]
    ConnectionReplaced,
    /// The physical connection terminated permanently.
    #[error("scalable route connection closed")]
    ConnectionClosed,
    /// The route was explicitly closed.
    #[error("scalable route closed")]
    Closed,
}

#[derive(Debug, Default)]
struct RouteState {
    events: VecDeque<ScalableEvent>,
    terminal: Option<ScalableRouteError>,
}

#[derive(Debug, Default)]
struct Route {
    state: Mutex<RouteState>,
    notify: Notify,
}

impl Route {
    fn publish(&self, event: ScalableEvent) {
        let mut state = self.state.lock();
        if state.terminal.is_some() {
            return;
        }
        if state.events.len() == MAX_ROUTE_EVENTS {
            state.events.clear();
            state.terminal = Some(ScalableRouteError::Overflow {
                capacity: MAX_ROUTE_EVENTS,
            });
        } else {
            state.events.push_back(event);
        }
        drop(state);
        self.notify.notify_waiters();
    }

    fn terminate(&self, error: ScalableRouteError) {
        let mut state = self.state.lock();
        if state.terminal.is_none() {
            state.terminal = Some(error);
        }
        drop(state);
        self.notify.notify_waiters();
    }
}

#[derive(Debug, Default)]
struct RegistryState {
    routes: HashMap<ScalableRouteKey, Arc<Route>>,
    active: HashMap<(ScalableRouteFamily, u64), ScalableRouteKey>,
    retired: VecDeque<ScalableRouteKey>,
}

/// Per-connection registry used by the driver to enforce one event owner.
#[derive(Debug, Default)]
pub(crate) struct ScalableRouteRegistry {
    state: Mutex<RegistryState>,
}

impl ScalableRouteRegistry {
    pub(crate) fn claim_at_epoch(
        self: &Arc<Self>,
        shared: Arc<ConnectionShared>,
        key: ScalableRouteKey,
        session_epoch: u64,
    ) -> ScalableRoute {
        let mut state = self.state.lock();
        let logical = (key.family, key.id);
        debug_assert!(!state.active.contains_key(&logical));
        let route = Arc::new(Route::default());
        debug_assert!(state.active.insert(logical, key).is_none());
        debug_assert!(state.routes.insert(key, route.clone()).is_none());
        ScalableRoute {
            key,
            session_epoch,
            route,
            registry: Arc::downgrade(self),
            shared,
        }
    }

    pub(crate) fn publish(&self, event: ScalableEvent) -> Option<ScalableEvent> {
        let Some((family, id, incarnation)) = event_route(&event) else {
            return Some(event);
        };
        let route = {
            let state = self.state.lock();
            let active_key = state.active.get(&(family, id)).copied();
            if let Some(key) = active_key {
                incarnation
                    .is_none_or(|value| value == key.incarnation)
                    .then(|| state.routes.get(&key).cloned())
                    .flatten()
            } else {
                let retired = state
                    .retired
                    .iter()
                    .rev()
                    .any(|key| key.family == family && key.id == id);
                if retired {
                    return None;
                }
                return Some(event);
            }
        };
        if let Some(route) = route {
            route.publish(event);
        }
        None
    }

    pub(crate) fn notify_waiters(&self) {
        let routes: Vec<Arc<Route>> = self.state.lock().routes.values().cloned().collect();
        for route in routes {
            route.notify.notify_waiters();
        }
    }

    pub(crate) fn close_all(&self) {
        let routes: Vec<Arc<Route>> = self.state.lock().routes.values().cloned().collect();
        for route in routes {
            route.terminate(ScalableRouteError::ConnectionClosed);
        }
    }

    fn retire(&self, key: ScalableRouteKey, route: &Arc<Route>) {
        let mut state = self.state.lock();
        if state
            .routes
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, route))
        {
            state.routes.remove(&key);
            let logical = (key.family, key.id);
            if state.active.get(&logical) == Some(&key) {
                state.active.remove(&logical);
            }
            if let Some(position) = state
                .retired
                .iter()
                .position(|retired| retired.family == key.family && retired.id == key.id)
            {
                state.retired.remove(position);
                state.retired.push_back(key);
            } else {
                if state.retired.len() == MAX_RETIRED_ROUTES {
                    state.retired.pop_front();
                }
                state.retired.push_back(key);
            }
        }
    }
}

/// Single-owner, cancellation-safe scalable event route.
#[derive(Debug)]
pub struct ScalableRoute {
    key: ScalableRouteKey,
    session_epoch: u64,
    route: Arc<Route>,
    registry: Weak<ScalableRouteRegistry>,
    shared: Arc<ConnectionShared>,
}

impl ScalableRoute {
    /// Drain one already-buffered event without waiting.
    pub fn poll(&self) -> Result<Option<ScalableEvent>, ScalableRouteError> {
        // A reset may race an event already queued on this route. Fence the
        // physical session before exposing any buffered control-plane data.
        self.check_connection()?;
        let mut state = self.route.state.lock();
        if let Some(event) = state.events.pop_front() {
            drop(state);
            self.check_connection()?;
            return Ok(Some(event));
        }
        if let Some(error) = state.terminal.clone() {
            return Err(error);
        }
        Ok(None)
    }

    /// Await the next event. The waiter is armed before state is inspected, so
    /// a publish racing the empty check cannot strand this future.
    pub async fn next(&self) -> Result<ScalableEvent, ScalableRouteError> {
        loop {
            let notified = self.route.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(event) = self.poll()? {
                return Ok(event);
            }
            notified.await;
        }
    }

    /// Explicitly terminate and unregister this route.
    pub fn close(&self) {
        self.route.terminate(ScalableRouteError::Closed);
        if let Some(registry) = self.registry.upgrade() {
            registry.retire(self.key, &self.route);
        }
    }

    fn check_connection(&self) -> Result<(), ScalableRouteError> {
        let conn = self.shared.inner.lock();
        if conn.session_epoch() != self.session_epoch {
            return Err(ScalableRouteError::ConnectionReplaced);
        }
        if conn.is_closed() && self.shared.is_no_driver() {
            return Err(ScalableRouteError::ConnectionClosed);
        }
        Ok(())
    }
}

impl Drop for ScalableRoute {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.retire(self.key, &self.route);
        }
    }
}

fn event_route(
    event: &ScalableEvent,
) -> Option<(
    ScalableRouteFamily,
    u64,
    Option<magnetar_proto::ControllerIncarnation>,
)> {
    match event {
        ScalableEvent::LookupResolved { session_id, .. }
        | ScalableEvent::DagUpdated { session_id, .. }
        | ScalableEvent::DagChangedDuringConsume { session_id, .. }
        | ScalableEvent::DagWatchClosed { session_id, .. } => {
            Some((ScalableRouteFamily::Dag, *session_id, None))
        }
        ScalableEvent::ConsumerAssigned {
            consumer_id,
            incarnation,
            ..
        }
        | ScalableEvent::AssignmentChanged {
            consumer_id,
            incarnation,
            ..
        }
        | ScalableEvent::ConsumerRejected {
            consumer_id,
            incarnation,
            ..
        } => Some((
            ScalableRouteFamily::Consumer,
            *consumer_id,
            Some(*incarnation),
        )),
        ScalableEvent::TopicsChanged { .. }
        | ScalableEvent::TopicsWatchClosed { .. }
        | ScalableEvent::TcAssignmentsChanged { .. }
        | ScalableEvent::TcAssignmentsWatchClosed { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use bytes::BufMut as _;
    use prost::Message as _;

    use super::*;

    fn encode(command: &magnetar_proto::pb::BaseCommand) -> bytes::BytesMut {
        let mut bytes = bytes::BytesMut::new();
        magnetar_proto::encode_command(&mut bytes, command).expect("encode command");
        bytes
    }

    fn deferred_message(
        entry_id: u64,
        metadata: magnetar_proto::pb::MessageMetadata,
        payload: bytes::Bytes,
        dispatch_permits: u32,
    ) -> magnetar_proto::DeferredIncomingMessage {
        let message_id_data = magnetar_proto::pb::MessageIdData {
            ledger_id: 1,
            entry_id,
            partition: Some(-1),
            batch_index: None,
            ack_set: vec![3, 5],
            batch_size: None,
            first_chunk_message_id: None,
        };
        magnetar_proto::DeferredIncomingMessage {
            message: magnetar_proto::IncomingMessage {
                message_id: magnetar_proto::MessageId::from_pb(&message_id_data),
                metadata: Arc::new(metadata),
                single_metadata: None,
                payload,
                redelivery_count: 0,
                broker_entry_metadata: None,
                arrived_at: std::time::Instant::now(),
            },
            message_id_data,
            ack_set: Vec::new(),
            dispatch_permits,
        }
    }

    fn batch_payload(payloads: &[&[u8]]) -> bytes::Bytes {
        let mut bytes = bytes::BytesMut::new();
        for payload in payloads {
            let metadata = magnetar_proto::pb::SingleMessageMetadata {
                payload_size: i32::try_from(payload.len()).expect("test payload fits i32"),
                ..Default::default()
            }
            .encode_to_vec();
            bytes.put_u32(u32::try_from(metadata.len()).expect("metadata fits u32"));
            bytes.extend_from_slice(&metadata);
            bytes.extend_from_slice(payload);
        }
        bytes.freeze()
    }

    fn zlib(payload: &[u8]) -> bytes::Bytes {
        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(payload).expect("compress test payload");
        bytes::Bytes::from(encoder.finish().expect("finish test compression"))
    }

    fn connect_shared(shared: &Arc<ConnectionShared>) {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("begin handshake");
        let _ = conn.poll_transmit();
        let connected = encode(&magnetar_proto::pb::BaseCommand {
            r#type: magnetar_proto::pb::base_command::Type::Connected as i32,
            connected: Some(magnetar_proto::pb::CommandConnected {
                server_version: "test".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(magnetar_proto::pb::FeatureFlags {
                    supports_scalable_topics: Some(true),
                    ..Default::default()
                }),
            }),
            ..Default::default()
        });
        conn.handle_bytes(std::time::Instant::now(), &connected)
            .expect("connected");
        while conn.poll_event().is_some() {}
        drop(conn);
    }

    fn connected_shared() -> Arc<ConnectionShared> {
        let shared = shared();
        connect_shared(&shared);
        shared
    }

    fn control_plane_fixture_at(
        epoch: u64,
    ) -> (
        magnetar_proto::DagSnapshot,
        magnetar_proto::ConsumerAssignment,
    ) {
        let parent = "topic://public/default/scaled";
        let segment_topic = magnetar_proto::canonical_segment_topic(
            parent,
            magnetar_proto::KeyRange::FULL,
            magnetar_proto::SegmentId(1),
        )
        .expect("canonical segment topic");
        let dag = magnetar_proto::pb::ScalableTopicDag {
            epoch,
            segments: vec![magnetar_proto::pb::SegmentInfoProto {
                segment_id: 1,
                hash_start: 0,
                hash_end: 65_535,
                state: magnetar_proto::pb::SegmentState::Active as i32,
                parent_ids: Vec::new(),
                child_ids: Vec::new(),
                created_at_epoch: 0,
                sealed_at_epoch: None,
                created_at_ms: 0,
                sealed_at_ms: None,
                legacy_topic_name: None,
            }],
            segment_brokers: vec![magnetar_proto::pb::SegmentBrokerAddress {
                segment_id: 1,
                broker_url: "pulsar://allowed.example:6650".to_owned(),
                broker_url_tls: None,
            }],
            controller_broker_url: Some("pulsar://allowed.example:6650".to_owned()),
            controller_broker_url_tls: None,
        };
        let snapshot = magnetar_proto::DagSnapshot::try_from_pb(&dag).expect("valid DAG");
        let assignment = magnetar_proto::ConsumerAssignment::try_from_pb(
            &magnetar_proto::pb::ScalableConsumerAssignment {
                layout_epoch: epoch,
                segments: vec![magnetar_proto::pb::ScalableAssignedSegment {
                    segment_id: 1,
                    hash_start: 0,
                    hash_end: 65_535,
                    segment_topic,
                }],
            },
            parent,
        )
        .expect("valid assignment");
        (snapshot, assignment)
    }

    fn control_plane_fixture() -> (
        magnetar_proto::DagSnapshot,
        magnetar_proto::ConsumerAssignment,
    ) {
        control_plane_fixture_at(1)
    }

    fn sealed_parent_fixture() -> (
        magnetar_proto::DagSnapshot,
        magnetar_proto::ConsumerAssignment,
    ) {
        let parent = "topic://public/default/scaled";
        let segment = |segment_id, hash_start, hash_end, state, parents, children, sealed| {
            magnetar_proto::pb::SegmentInfoProto {
                segment_id,
                hash_start,
                hash_end,
                state: state as i32,
                parent_ids: parents,
                child_ids: children,
                created_at_epoch: if segment_id == 1 { 0 } else { 2 },
                sealed_at_epoch: sealed,
                created_at_ms: 0,
                sealed_at_ms: sealed.map(|_| 0),
                legacy_topic_name: None,
            }
        };
        let dag = magnetar_proto::DagSnapshot::try_from_pb(&magnetar_proto::pb::ScalableTopicDag {
            epoch: 2,
            segments: vec![
                segment(
                    1,
                    0,
                    65_535,
                    magnetar_proto::pb::SegmentState::Sealed,
                    Vec::new(),
                    vec![2, 3],
                    Some(2),
                ),
                segment(
                    2,
                    0,
                    32_767,
                    magnetar_proto::pb::SegmentState::Active,
                    vec![1],
                    Vec::new(),
                    None,
                ),
                segment(
                    3,
                    32_768,
                    65_535,
                    magnetar_proto::pb::SegmentState::Active,
                    vec![1],
                    Vec::new(),
                    None,
                ),
            ],
            segment_brokers: (1..=3)
                .map(|segment_id| magnetar_proto::pb::SegmentBrokerAddress {
                    segment_id,
                    broker_url: "pulsar://allowed.example:6650".to_owned(),
                    broker_url_tls: None,
                })
                .collect(),
            controller_broker_url: Some("pulsar://allowed.example:6650".to_owned()),
            controller_broker_url_tls: None,
        })
        .expect("valid sealed-parent DAG");
        let assignment = magnetar_proto::ConsumerAssignment::try_from_pb(
            &magnetar_proto::pb::ScalableConsumerAssignment {
                layout_epoch: 2,
                segments: vec![magnetar_proto::pb::ScalableAssignedSegment {
                    segment_id: 1,
                    hash_start: 0,
                    hash_end: 65_535,
                    segment_topic: magnetar_proto::canonical_segment_topic(
                        parent,
                        magnetar_proto::KeyRange::FULL,
                        magnetar_proto::SegmentId(1),
                    )
                    .expect("canonical parent segment topic"),
                }],
            },
            parent,
        )
        .expect("valid sealed-parent assignment");
        (dag, assignment)
    }

    fn shared() -> Arc<ConnectionShared> {
        ConnectionShared::new(magnetar_proto::ConnectionConfig::default())
    }

    fn claim_dag(shared: &Arc<ConnectionShared>, session_id: u64) -> ScalableRoute {
        let epoch = shared.inner.lock().session_epoch();
        shared.scalable_routes.claim_at_epoch(
            shared.clone(),
            ScalableRouteKey::dag(session_id, magnetar_proto::ControllerIncarnation(epoch)),
            epoch,
        )
    }

    fn subscriber_with_allow_list() -> SegmentSubscriber {
        let bootstrap = shared();
        let config = magnetar_proto::ConnectionConfig {
            redirect_url_allow_list: Some(magnetar_proto::RedirectUrlAllowList::Exact(vec![
                "pulsar://allowed.example:6650".to_owned(),
                "pulsar+ssl://allowed.example:6651".to_owned(),
            ])),
            ..Default::default()
        };
        let pool = ProxyConnectionPool::new(crate::pool::ConnectionFactory {
            url: crate::ParsedUrl {
                host: "allowed.example".to_owned(),
                port: 6650,
                scheme: Scheme::Plain,
            },
            tls_config: None,
            bootstrap_config: config,
            operation_retry: Arc::new(Mutex::new(magnetar_proto::OperationRetryConfig::default())),
            auth_provider: None,
            service_url_provider: None,
            dns_resolver: None,
        });
        SegmentSubscriber::new(bootstrap, pool, std::time::Duration::from_secs(1))
    }

    fn aggregate_inner_with_two_messages() -> Arc<StreamConsumerInner> {
        let subscriber = subscriber_with_allow_list();
        let (snapshot, assignment) = control_plane_fixture();
        let source = assignment.segments()[0].source();
        let mut model = magnetar_proto::StreamConsumerModel::new(
            "topic://public/default/scaled".to_owned(),
            magnetar_proto::ConsumerInstanceId(42),
            magnetar_proto::ControllerIncarnation(1),
            magnetar_proto::OrderingMode::Strict,
            snapshot.clone(),
            magnetar_proto::ReceiverBudget::bytes(16 * 1024 * 1024).expect("valid budget"),
        )
        .expect("aggregate model");
        let actions = model
            .apply_control_plane(snapshot, assignment)
            .expect("initial assignment");
        let generation = actions
            .iter()
            .find_map(|action| match action {
                magnetar_proto::StreamConsumerAction::OpenChild {
                    child_generation, ..
                } => Some(*child_generation),
                _ => None,
            })
            .expect("child open action");
        let mut actions = model
            .child_opened(source.segment_id(), generation)
            .expect("child opened");
        let mut queue = VecDeque::new();
        for entry_id in 1..=2 {
            let reservation = actions
                .iter()
                .find_map(|action| match action {
                    magnetar_proto::StreamConsumerAction::GrantFlow { reservation, .. } => {
                        Some(*reservation)
                    }
                    _ => None,
                })
                .expect("flow grant");
            let message = magnetar_proto::IncomingMessage {
                message_id: magnetar_proto::MessageId {
                    ledger_id: 1,
                    entry_id,
                    partition: -1,
                    batch_index: -1,
                    batch_size: 0,
                },
                metadata: Arc::new(magnetar_proto::pb::MessageMetadata::default()),
                single_metadata: None,
                payload: bytes::Bytes::from_static(b"data"),
                redelivery_count: 0,
                broker_entry_metadata: None,
                arrived_at: std::time::Instant::now(),
            };
            let transition = model
                .message_arrived(
                    source.segment_id(),
                    generation,
                    reservation,
                    message.retained_bytes(),
                )
                .expect("message retained");
            let message_id_data = message.message_id.to_pb();
            queue.push_back(QueuedMessage {
                source: source.clone(),
                generation,
                message,
                delivery: QueuedDelivery::Fresh {
                    reservation: transition.retained,
                    message_id_data,
                },
            });
            actions = transition.actions;
        }
        Arc::new(StreamConsumerInner {
            subscriber,
            child_options: SegmentConsumerOptions {
                subscription: "workers".to_owned(),
                consumer_name: "worker-a".to_owned(),
                schema: magnetar_proto::pb::Schema::default(),
            },
            topic: "topic://public/default/scaled".to_owned(),
            subscription: "workers".to_owned(),
            consumer_name: "worker-a".to_owned(),
            consumer_id: 42,
            state: Mutex::new(AggregateState {
                model,
                receive: magnetar_proto::StreamReceiveState::default(),
                children: BTreeMap::new(),
                flow_reservations: BTreeMap::new(),
                dispatch_permit_debt: BTreeMap::new(),
                queue,
                events: VecDeque::new(),
                pending_transactions: BTreeMap::new(),
                transaction_registrations: BTreeMap::new(),
                transaction_outcomes: BTreeMap::new(),
                controller_registration: None,
                terminal_error: None,
                reconnect_requested: false,
                open_tasks: 0,
                close_state: AggregateCloseState::Open,
                close_error: None,
                tasks: Vec::new(),
            }),
            notify: Notify::new(),
            control_park_hook: None,
            transaction_outcome_park_hook: None,
        })
    }

    fn empty_aggregate_inner() -> Arc<StreamConsumerInner> {
        let subscriber = subscriber_with_allow_list();
        let (snapshot, assignment) = control_plane_fixture();
        let mut model = magnetar_proto::StreamConsumerModel::new(
            "topic://public/default/scaled".to_owned(),
            magnetar_proto::ConsumerInstanceId(42),
            magnetar_proto::ControllerIncarnation(1),
            magnetar_proto::OrderingMode::BrokerManaged,
            snapshot.clone(),
            magnetar_proto::ReceiverBudget::bytes(16 * 1024 * 1024).expect("valid budget"),
        )
        .expect("aggregate model");
        model
            .apply_control_plane(snapshot, assignment)
            .expect("initial control plane");
        Arc::new(StreamConsumerInner {
            subscriber,
            child_options: SegmentConsumerOptions {
                subscription: "workers".to_owned(),
                consumer_name: "worker-a".to_owned(),
                schema: magnetar_proto::pb::Schema::default(),
            },
            topic: "topic://public/default/scaled".to_owned(),
            subscription: "workers".to_owned(),
            consumer_name: "worker-a".to_owned(),
            consumer_id: 42,
            state: Mutex::new(AggregateState {
                model,
                receive: magnetar_proto::StreamReceiveState::default(),
                children: BTreeMap::new(),
                flow_reservations: BTreeMap::new(),
                dispatch_permit_debt: BTreeMap::new(),
                queue: VecDeque::new(),
                events: VecDeque::new(),
                pending_transactions: BTreeMap::new(),
                transaction_registrations: BTreeMap::new(),
                transaction_outcomes: BTreeMap::new(),
                controller_registration: None,
                terminal_error: None,
                reconnect_requested: false,
                open_tasks: 0,
                close_state: AggregateCloseState::Open,
                close_error: None,
                tasks: Vec::new(),
            }),
            notify: Notify::new(),
            control_park_hook: None,
            transaction_outcome_park_hook: None,
        })
    }

    fn attached_child_consumer(
        source: &magnetar_proto::SegmentSource,
    ) -> (crate::Consumer, Arc<ConnectionShared>) {
        let shared = connected_shared();
        let (handle, slot) = {
            let mut conn = shared.inner.lock();
            let request_id = conn.peek_next_request_id_for_test();
            let handle = conn.subscribe(magnetar_proto::SubscribeRequest {
                topic: source.topic().to_owned(),
                subscription: "workers".to_owned(),
                receiver_queue_size: 0,
                ..Default::default()
            });
            let slot = conn.consumer(handle).expect("child slot").clone();
            let _ = conn.poll_transmit();
            let success = magnetar_proto::pb::BaseCommand {
                r#type: magnetar_proto::pb::base_command::Type::Success as i32,
                success: Some(magnetar_proto::pb::CommandSuccess {
                    request_id,
                    schema: None,
                }),
                ..Default::default()
            };
            conn.handle_bytes(std::time::Instant::now(), &encode(&success))
                .expect("establish child");
            assert!(conn.consume_initial_consumer_subscribe_completion(handle));
            while conn.poll_event().is_some() {}
            (handle, slot)
        };
        (
            crate::Consumer::assemble(shared.clone(), handle, slot, None),
            shared,
        )
    }

    fn aggregate_inner_with_child_from(
        snapshot: magnetar_proto::DagSnapshot,
        assignment: magnetar_proto::ConsumerAssignment,
    ) -> (Arc<StreamConsumerInner>, Arc<ConnectionShared>) {
        let subscriber = subscriber_with_allow_list();
        let source = assignment.segments()[0].source();
        let segment_id = source.segment_id();
        let mut model = magnetar_proto::StreamConsumerModel::new(
            "topic://public/default/scaled".to_owned(),
            magnetar_proto::ConsumerInstanceId(42),
            magnetar_proto::ControllerIncarnation(1),
            magnetar_proto::OrderingMode::BrokerManaged,
            snapshot.clone(),
            magnetar_proto::ReceiverBudget::bytes(16 * 1024 * 1024).expect("valid budget"),
        )
        .expect("aggregate model");
        let open = model
            .apply_control_plane(snapshot, assignment)
            .expect("initial control plane");
        let generation = open
            .iter()
            .find_map(|action| match action {
                magnetar_proto::StreamConsumerAction::OpenChild {
                    child_generation, ..
                } => Some(*child_generation),
                _ => None,
            })
            .expect("child generation");
        let flow = model
            .child_opened(source.segment_id(), generation)
            .expect("child opened");
        let reservation = flow
            .iter()
            .find_map(|action| match action {
                magnetar_proto::StreamConsumerAction::GrantFlow { reservation, .. } => {
                    Some(*reservation)
                }
                _ => None,
            })
            .expect("flow reservation");

        let (child, child_shared) = attached_child_consumer(&source);
        let inner = Arc::new(StreamConsumerInner {
            subscriber,
            child_options: SegmentConsumerOptions {
                subscription: "workers".to_owned(),
                consumer_name: "worker-a".to_owned(),
                schema: magnetar_proto::pb::Schema::default(),
            },
            topic: "topic://public/default/scaled".to_owned(),
            subscription: "workers".to_owned(),
            consumer_name: "worker-a".to_owned(),
            consumer_id: 42,
            state: Mutex::new(AggregateState {
                model,
                receive: magnetar_proto::StreamReceiveState::default(),
                children: BTreeMap::from([(
                    source.segment_id(),
                    ChildRuntime {
                        source,
                        generation,
                        consumer: child,
                    },
                )]),
                flow_reservations: BTreeMap::from([((segment_id, generation), reservation)]),
                dispatch_permit_debt: BTreeMap::new(),
                queue: VecDeque::new(),
                events: VecDeque::new(),
                pending_transactions: BTreeMap::new(),
                transaction_registrations: BTreeMap::new(),
                transaction_outcomes: BTreeMap::new(),
                controller_registration: None,
                terminal_error: None,
                reconnect_requested: false,
                open_tasks: 0,
                close_state: AggregateCloseState::Open,
                close_error: None,
                tasks: Vec::new(),
            }),
            notify: Notify::new(),
            control_park_hook: None,
            transaction_outcome_park_hook: None,
        });
        (inner, child_shared)
    }

    fn aggregate_inner_with_child() -> (Arc<StreamConsumerInner>, Arc<ConnectionShared>) {
        let (snapshot, assignment) = control_plane_fixture();
        aggregate_inner_with_child_from(snapshot, assignment)
    }

    #[test]
    fn claimed_event_has_exactly_one_owner() {
        let shared = shared();
        let route = claim_dag(&shared, 7);
        let event = ScalableEvent::DagWatchClosed {
            session_id: 7,
            reason: None,
        };
        assert!(shared.scalable_routes.publish(event).is_none());
        assert!(matches!(
            route.poll().expect("route open"),
            Some(ScalableEvent::DagWatchClosed { session_id: 7, .. })
        ));
    }

    #[test]
    fn unclaimed_event_stays_on_raw_path() {
        let shared = shared();
        let event = ScalableEvent::DagWatchClosed {
            session_id: 8,
            reason: None,
        };
        assert!(matches!(
            shared.scalable_routes.publish(event),
            Some(ScalableEvent::DagWatchClosed { session_id: 8, .. })
        ));
    }

    #[test]
    fn old_controller_incarnation_is_fenced() {
        let shared = shared();
        let epoch = shared.inner.lock().session_epoch();
        let route = shared.scalable_routes.claim_at_epoch(
            shared.clone(),
            ScalableRouteKey::consumer(9, magnetar_proto::ControllerIncarnation(2)),
            epoch,
        );
        let stale = ScalableEvent::ConsumerRejected {
            consumer_id: 9,
            incarnation: magnetar_proto::ControllerIncarnation(1),
            reason: "stale".to_owned(),
        };
        assert!(shared.scalable_routes.publish(stale).is_none());
        assert!(route.poll().expect("route open").is_none());
    }

    #[test]
    fn buffered_event_is_fenced_after_connection_replacement() {
        let shared = shared();
        let route = claim_dag(&shared, 8);
        assert!(
            shared
                .scalable_routes
                .publish(ScalableEvent::DagWatchClosed {
                    session_id: 8,
                    reason: None,
                })
                .is_none()
        );
        shared.inner.lock().reset();

        assert!(matches!(
            route.poll(),
            Err(ScalableRouteError::ConnectionReplaced)
        ));
    }

    #[tokio::test]
    async fn restored_delivery_precedes_later_fresh_delivery() {
        let consumer = StreamConsumer {
            inner: aggregate_inner_with_two_messages(),
        };
        let first = consumer.receive().await.expect("first delivery");
        let second = consumer.receive().await.expect("second delivery");
        assert_eq!(first.message.message_id.entry_id, 1);
        assert_eq!(second.message.message_id.entry_id, 2);

        consumer
            .restore_deliveries(vec![second, first])
            .expect("restore cancelled deliveries in reverse order");

        let restored_first = consumer.receive().await.expect("first restored delivery");
        let restored_second = consumer.receive().await.expect("second restored delivery");
        assert_eq!(restored_first.message.message_id.entry_id, 1);
        assert_eq!(restored_first.token.dequeue_sequence().0, 0);
        assert_eq!(restored_second.message.message_id.entry_id, 2);
        assert_eq!(restored_second.token.dequeue_sequence().0, 1);

        let closing = StreamConsumer {
            inner: aggregate_inner_with_two_messages(),
        };
        let rejected = closing.receive().await.expect("delivery before close");
        closing.inner.state.lock().close_state = AggregateCloseState::Closing;
        assert!(matches!(
            closing.restore_deliveries(vec![rejected]),
            Err(StreamConsumerError::Closed)
        ));
        assert_eq!(closing.inner.state.lock().queue.len(), 1);

        let failed = StreamConsumer {
            inner: empty_aggregate_inner(),
        };
        failed.inner.state.lock().terminal_error = Some("retained receive failure".to_owned());
        assert!(matches!(
            failed.receive().await,
            Err(StreamConsumerError::Failed(message)) if message == "retained receive failure"
        ));
    }

    #[tokio::test]
    async fn delivery_without_flow_reservation_fails_without_queue_mutation() {
        let (inner, child_shared) = aggregate_inner_with_child();
        let child = inner
            .state
            .lock()
            .children
            .values()
            .next()
            .expect("child")
            .clone();
        inner.state.lock().flow_reservations.clear();
        let session_epoch = child_shared.inner.lock().session_epoch();
        assert!(matches!(
            inner
                .message_arrived(
                    child.source.clone(),
                    child.generation,
                    session_epoch,
                    &child.consumer,
                    deferred_message(
                        7,
                        magnetar_proto::pb::MessageMetadata::default(),
                        bytes::Bytes::from_static(b"missing-flow"),
                        1,
                    ),
                )
                .await,
            Err(StreamConsumerError::Failed(message))
                if message.contains("without an aggregate FLOW reservation")
        ));
        assert!(inner.state.lock().queue.is_empty());
    }

    #[tokio::test]
    async fn failed_delivery_restoration_requests_resynchronization() {
        let inner = empty_aggregate_inner();
        let consumer = StreamConsumer {
            inner: inner.clone(),
        };

        consumer.delivery_restoration_failed(&StreamConsumerError::Failed(
            "stale cancellation authority".to_owned(),
        ));

        let state = inner.state.lock();
        assert_eq!(
            state.model.phase(),
            magnetar_proto::AggregatePhase::ResyncRequired
        );
        assert!(state.reconnect_requested);
        assert!(matches!(
            state.events.back(),
            Some(StreamConsumerEvent::ResyncRequired { reason })
                if reason.contains("delivery restoration failed")
        ));
        let event_count = state.events.len();
        drop(state);

        inner.state.lock().close_state = AggregateCloseState::Closing;
        inner.request_resync("ignored after close".to_owned());

        let state = inner.state.lock();
        assert_eq!(state.events.len(), event_count);
        assert_eq!(
            state.model.phase(),
            magnetar_proto::AggregatePhase::ResyncRequired
        );
        assert!(state.reconnect_requested);
    }

    #[tokio::test]
    async fn closing_aggregate_rejects_new_child_work() {
        let (inner, _child_shared) = aggregate_inner_with_child();
        let child = inner
            .state
            .lock()
            .children
            .values()
            .next()
            .expect("attached child")
            .clone();
        let stop_flow = magnetar_proto::StreamConsumerAction::StopFlow {
            source: child.source.clone(),
            controller_incarnation: magnetar_proto::ControllerIncarnation(1),
            child_generation: child.generation,
        };
        inner.state.lock().close_state = AggregateCloseState::Closing;

        inner.spawn_open_task(
            child.source.clone(),
            magnetar_proto::ControllerIncarnation(1),
            child.generation,
        );
        inner.spawn_child_task(child.source, child.generation, child.consumer);
        inner.spawn_actions(vec![stop_flow]);

        let state = inner.state.lock();
        assert_eq!(state.open_tasks, 0);
        assert!(state.tasks.is_empty());
        assert_eq!(state.children.len(), 1);
    }

    #[tokio::test]
    async fn parked_route_is_woken_after_connection_replacement() {
        let shared = shared();
        let route = claim_dag(&shared, 18);
        let mut next = Box::pin(route.next());
        std::future::poll_fn(|context| {
            assert!(matches!(
                std::future::Future::poll(next.as_mut(), context),
                std::task::Poll::Pending
            ));
            std::task::Poll::Ready(())
        })
        .await;

        shared.inner.lock().reset();
        crate::driver::notify_scalable_connection_replaced(&shared);

        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), next)
                .await
                .expect("replacement wake must resolve the parked route"),
            Err(ScalableRouteError::ConnectionReplaced)
        ));
    }

    #[tokio::test]
    async fn cancelled_dag_setup_closes_staged_protocol_session() {
        let subscriber = subscriber_with_allow_list();
        connect_shared(&subscriber.bootstrap);
        let mut open = Box::pin(subscriber.open_dag_session("topic://public/default/scaled"));
        std::future::poll_fn(|context| {
            assert!(matches!(
                std::future::Future::poll(open.as_mut(), context),
                std::task::Poll::Pending
            ));
            std::task::Poll::Ready(())
        })
        .await;
        assert!(subscriber.bootstrap.inner.lock().dag_snapshot(1).is_some());

        drop(open);

        assert!(subscriber.bootstrap.inner.lock().dag_snapshot(1).is_none());
    }

    #[tokio::test]
    async fn cancelled_controller_setup_removes_protocol_registration() {
        let subscriber = subscriber_with_allow_list();
        connect_shared(&subscriber.bootstrap);
        let (snapshot, _) = control_plane_fixture();
        let dag = DagSession {
            shared: subscriber.bootstrap.clone(),
            route: claim_dag(&subscriber.bootstrap, 41),
            session_id: 41,
            requested_topic: "topic://public/default/scaled".to_owned(),
            resolved_topic_name: Some("topic://public/default/scaled".to_owned()),
            controller_broker_url: Some("pulsar://allowed.example:6650".to_owned()),
            controller_broker_url_tls: None,
            snapshot,
            closed: true,
        };
        let consumer_id = 73;
        let mut open = Box::pin(subscriber.open_controller_session_with_id(
            &dag,
            "workers",
            "worker-a",
            consumer_id,
        ));
        std::future::poll_fn(|context| {
            assert!(matches!(
                std::future::Future::poll(open.as_mut(), context),
                std::task::Poll::Pending
            ));
            std::task::Poll::Ready(())
        })
        .await;

        drop(open);

        assert!(
            !subscriber
                .bootstrap
                .inner
                .lock()
                .remove_scalable_consumer_registration(
                    consumer_id,
                    magnetar_proto::ControllerIncarnation(1),
                )
        );
    }

    #[tokio::test]
    async fn controller_reopen_keeps_identity_and_advances_incarnation() {
        let subscriber = subscriber_with_allow_list();
        connect_shared(&subscriber.bootstrap);
        let (snapshot, assignment) = control_plane_fixture();
        let dag = DagSession {
            shared: subscriber.bootstrap.clone(),
            route: claim_dag(&subscriber.bootstrap, 41),
            session_id: 41,
            requested_topic: "topic://public/default/scaled".to_owned(),
            resolved_topic_name: Some("topic://public/default/scaled".to_owned()),
            controller_broker_url: Some("pulsar://allowed.example:6650".to_owned()),
            controller_broker_url_tls: None,
            snapshot,
            closed: true,
        };
        let epoch = subscriber.bootstrap.inner.lock().session_epoch();
        let previous = ControllerSession {
            shared: subscriber.bootstrap.clone(),
            route: subscriber.bootstrap.scalable_routes.claim_at_epoch(
                subscriber.bootstrap.clone(),
                ScalableRouteKey::consumer(42, magnetar_proto::ControllerIncarnation(0)),
                epoch,
            ),
            consumer_id: 42,
            incarnation: magnetar_proto::ControllerIncarnation(0),
            assignment: assignment.clone(),
            registration_topic: "topic://public/default/scaled".to_owned(),
            subscription: "workers".to_owned(),
            consumer_name: "worker-a".to_owned(),
        };
        let task_subscriber = subscriber.clone();
        let task = tokio::spawn(async move {
            task_subscriber
                .reopen_controller_session(&dag, previous)
                .await
        });
        tokio::task::yield_now().await;

        let subscribe = {
            let mut transmit = subscriber.bootstrap.inner.lock().poll_transmit();
            magnetar_proto::decode_one(&mut transmit)
                .expect("controller subscribe frame")
                .command
                .scalable_topic_subscribe
                .expect("controller subscribe payload")
        };
        assert_eq!(subscribe.consumer_id, 42);
        assert_eq!(subscribe.subscription, "workers");
        assert_eq!(subscribe.consumer_name, "worker-a");
        assert!(
            subscriber
                .bootstrap
                .scalable_routes
                .publish(ScalableEvent::ConsumerAssigned {
                    consumer_id: 42,
                    incarnation: magnetar_proto::ControllerIncarnation(1),
                    assignment,
                })
                .is_none()
        );
        let reopened = task.await.expect("task").expect("reopened controller");
        assert_eq!(reopened.consumer_id(), 42);
        assert_eq!(
            reopened.incarnation(),
            magnetar_proto::ControllerIncarnation(1)
        );
        assert_eq!(
            reopened.registration_topic(),
            "topic://public/default/scaled"
        );
    }

    #[tokio::test]
    async fn aggregate_defers_control_plane_until_epochs_match() {
        let inner = empty_aggregate_inner();
        let (snapshot, assignment) = control_plane_fixture_at(2);
        let shared = inner.subscriber.bootstrap.clone();
        let dag = DagSession {
            shared: shared.clone(),
            route: claim_dag(&shared, 51),
            session_id: 51,
            requested_topic: "topic://public/default/scaled".to_owned(),
            resolved_topic_name: Some("topic://public/default/scaled".to_owned()),
            controller_broker_url: Some("pulsar://allowed.example:6650".to_owned()),
            controller_broker_url_tls: None,
            snapshot,
            closed: true,
        };
        let epoch = shared.inner.lock().session_epoch();
        let mut controller = ControllerSession {
            shared: shared.clone(),
            route: shared.scalable_routes.claim_at_epoch(
                shared.clone(),
                ScalableRouteKey::consumer(42, magnetar_proto::ControllerIncarnation(1)),
                epoch,
            ),
            consumer_id: 42,
            incarnation: magnetar_proto::ControllerIncarnation(1),
            assignment: control_plane_fixture_at(1).1,
            registration_topic: "topic://public/default/scaled".to_owned(),
            subscription: "workers".to_owned(),
            consumer_name: "worker-a".to_owned(),
        };

        inner
            .apply_aligned_control_plane(&dag, &controller)
            .await
            .expect("mismatch is deferred");
        {
            let state = inner.state.lock();
            assert_eq!(state.model.dag().epoch(), 1);
            assert!(!state.reconnect_requested);
            assert!(state.events.is_empty());
        }

        controller.assignment = assignment;
        inner
            .apply_aligned_control_plane(&dag, &controller)
            .await
            .expect("aligned pair applies");
        let state = inner.state.lock();
        assert_eq!(state.model.dag().epoch(), 2);
        assert!(!state.reconnect_requested);
        assert!(matches!(
            state.events.back(),
            Some(StreamConsumerEvent::AssignmentApplied {
                layout_epoch: 2,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn baseline_alignment_waits_for_whichever_epoch_lags() {
        let (snapshot_one, assignment_one) = control_plane_fixture_at(1);
        let (snapshot_two, assignment_two) = control_plane_fixture_at(2);

        let first_shared = shared();
        let mut dag = DagSession {
            shared: first_shared.clone(),
            route: claim_dag(&first_shared, 61),
            session_id: 61,
            requested_topic: "topic://public/default/scaled".to_owned(),
            resolved_topic_name: Some("topic://public/default/scaled".to_owned()),
            controller_broker_url: Some("pulsar://allowed.example:6650".to_owned()),
            controller_broker_url_tls: None,
            snapshot: snapshot_one.clone(),
            closed: true,
        };
        let session_epoch = first_shared.inner.lock().session_epoch();
        let mut controller = ControllerSession {
            shared: first_shared.clone(),
            route: first_shared.scalable_routes.claim_at_epoch(
                first_shared.clone(),
                ScalableRouteKey::consumer(62, magnetar_proto::ControllerIncarnation(1)),
                session_epoch,
            ),
            consumer_id: 62,
            incarnation: magnetar_proto::ControllerIncarnation(1),
            assignment: assignment_two.clone(),
            registration_topic: "topic://public/default/scaled".to_owned(),
            subscription: "workers".to_owned(),
            consumer_name: "worker-a".to_owned(),
        };
        assert!(
            first_shared
                .scalable_routes
                .publish(ScalableEvent::DagUpdated {
                    session_id: 61,
                    delta: magnetar_proto::DagDelta {
                        epoch: 2,
                        added: Vec::new(),
                        removed: Vec::new(),
                        split_events: Vec::new(),
                        merge_events: Vec::new(),
                    },
                    snapshot: snapshot_two.clone(),
                })
                .is_none()
        );
        SegmentSubscriber::align_control_plane(&mut dag, &mut controller)
            .await
            .expect("DAG catches up");
        assert_eq!(dag.snapshot().epoch(), 2);

        let second_shared = shared();
        let mut dag = DagSession {
            shared: second_shared.clone(),
            route: claim_dag(&second_shared, 71),
            session_id: 71,
            requested_topic: "topic://public/default/scaled".to_owned(),
            resolved_topic_name: Some("topic://public/default/scaled".to_owned()),
            controller_broker_url: Some("pulsar://allowed.example:6650".to_owned()),
            controller_broker_url_tls: None,
            snapshot: snapshot_two,
            closed: true,
        };
        let session_epoch = second_shared.inner.lock().session_epoch();
        let mut controller = ControllerSession {
            shared: second_shared.clone(),
            route: second_shared.scalable_routes.claim_at_epoch(
                second_shared.clone(),
                ScalableRouteKey::consumer(72, magnetar_proto::ControllerIncarnation(1)),
                session_epoch,
            ),
            consumer_id: 72,
            incarnation: magnetar_proto::ControllerIncarnation(1),
            assignment: assignment_one,
            registration_topic: "topic://public/default/scaled".to_owned(),
            subscription: "workers".to_owned(),
            consumer_name: "worker-a".to_owned(),
        };
        assert!(
            second_shared
                .scalable_routes
                .publish(ScalableEvent::AssignmentChanged {
                    consumer_id: 72,
                    incarnation: magnetar_proto::ControllerIncarnation(1),
                    assignment: assignment_two,
                    delta: magnetar_proto::AssignmentDelta {
                        layout_epoch: 2,
                        gained: Vec::new(),
                        lost: Vec::new(),
                    },
                })
                .is_none()
        );
        SegmentSubscriber::align_control_plane(&mut dag, &mut controller)
            .await
            .expect("assignment catches up");
        assert_eq!(controller.assignment().layout_epoch(), 2);

        let closed_shared = shared();
        let mut dag = DagSession {
            shared: closed_shared.clone(),
            route: claim_dag(&closed_shared, 81),
            session_id: 81,
            requested_topic: "topic://public/default/scaled".to_owned(),
            resolved_topic_name: Some("topic://public/default/scaled".to_owned()),
            controller_broker_url: Some("pulsar://allowed.example:6650".to_owned()),
            controller_broker_url_tls: None,
            snapshot: snapshot_one,
            closed: true,
        };
        let session_epoch = closed_shared.inner.lock().session_epoch();
        let mut controller = ControllerSession {
            shared: closed_shared.clone(),
            route: closed_shared.scalable_routes.claim_at_epoch(
                closed_shared.clone(),
                ScalableRouteKey::consumer(82, magnetar_proto::ControllerIncarnation(1)),
                session_epoch,
            ),
            consumer_id: 82,
            incarnation: magnetar_proto::ControllerIncarnation(1),
            assignment: control_plane_fixture_at(2).1,
            registration_topic: "topic://public/default/scaled".to_owned(),
            subscription: "workers".to_owned(),
            consumer_name: "worker-a".to_owned(),
        };
        assert!(
            closed_shared
                .scalable_routes
                .publish(ScalableEvent::DagWatchClosed {
                    session_id: 81,
                    reason: Some("closed while catching up".to_owned()),
                })
                .is_none()
        );
        assert!(matches!(
            SegmentSubscriber::align_control_plane(&mut dag, &mut controller).await,
            Err(ClientError::Other(message)) if message == "closed while catching up"
        ));
        assert!(
            closed_shared
                .scalable_routes
                .publish(ScalableEvent::DagWatchClosed {
                    session_id: 81,
                    reason: None,
                })
                .is_none()
        );
        assert!(matches!(
            SegmentSubscriber::align_control_plane(&mut dag, &mut controller).await,
            Err(ClientError::Other(message))
                if message == "scalable DAG watch closed while aligning control-plane epochs"
        ));
    }

    #[tokio::test]
    async fn control_loop_retains_close_notification_while_parking() {
        let mut inner = empty_aggregate_inner();
        let hook = Arc::new(ControlParkHook::default());
        Arc::get_mut(&mut inner)
            .expect("fixture has one aggregate owner")
            .control_park_hook = Some(hook.clone());
        let shared = inner.subscriber.bootstrap.clone();
        connect_shared(&shared);
        let (snapshot, assignment) = control_plane_fixture();
        let epoch = shared.inner.lock().session_epoch();
        let dag = DagSession {
            shared: shared.clone(),
            route: claim_dag(&shared, 81),
            session_id: 81,
            requested_topic: "topic://public/default/scaled".to_owned(),
            resolved_topic_name: Some("topic://public/default/scaled".to_owned()),
            controller_broker_url: Some("pulsar://allowed.example:6650".to_owned()),
            controller_broker_url_tls: None,
            snapshot,
            closed: true,
        };
        let controller = ControllerSession {
            shared: shared.clone(),
            route: shared.scalable_routes.claim_at_epoch(
                shared.clone(),
                ScalableRouteKey::consumer(42, magnetar_proto::ControllerIncarnation(1)),
                epoch,
            ),
            consumer_id: 42,
            incarnation: magnetar_proto::ControllerIncarnation(1),
            assignment,
            registration_topic: "topic://public/default/scaled".to_owned(),
            subscription: "workers".to_owned(),
            consumer_name: "worker-a".to_owned(),
        };
        let task_inner = inner.clone();
        let task = tokio::spawn(async move {
            task_inner.control_loop(dag, controller).await;
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), hook.reached.notified())
            .await
            .expect("control loop reaches the post-check parking window");

        inner.state.lock().close_state = AggregateCloseState::Closing;
        inner.notify.notify_waiters();
        hook.release.notify_one();

        tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("armed close notification releases control loop")
            .expect("control task");
    }

    #[tokio::test]
    async fn cancel_open_without_runtime_child_completes_model_cleanup() {
        let inner = empty_aggregate_inner();
        let actions = inner
            .state
            .lock()
            .model
            .require_resync()
            .expect("begin resync");
        assert!(matches!(
            actions.as_slice(),
            [magnetar_proto::StreamConsumerAction::CancelOpen { .. }]
        ));

        inner
            .execute_actions(actions)
            .await
            .expect("cancelled open is finalized without a runtime child");

        assert!(
            inner
                .state
                .lock()
                .model
                .segment_phase(magnetar_proto::SegmentId(1))
                .is_none()
        );
    }

    #[tokio::test]
    async fn rejected_child_close_releases_model_ownership_and_retries_wire_close() {
        let (inner, child_shared) = aggregate_inner_with_child();
        let actions = inner
            .state
            .lock()
            .model
            .require_resync()
            .expect("begin resync");
        let mut operation = Box::pin(inner.execute_actions(actions));
        std::future::poll_fn(|context| {
            assert!(matches!(
                std::future::Future::poll(operation.as_mut(), context),
                std::task::Poll::Pending
            ));
            std::task::Poll::Ready(())
        })
        .await;
        let close_request_id = {
            let mut staged = child_shared.inner.lock().poll_transmit();
            let mut request_id = None;
            while !staged.is_empty() {
                let command = magnetar_proto::decode_one(&mut staged)
                    .expect("decode child close")
                    .command;
                if let Some(close) = command.close_consumer {
                    request_id = Some(close.request_id);
                }
            }
            request_id.expect("confirmation-bearing child close")
        };
        let error = magnetar_proto::pb::BaseCommand {
            r#type: magnetar_proto::pb::base_command::Type::Error as i32,
            error: Some(magnetar_proto::pb::CommandError {
                request_id: close_request_id,
                error: magnetar_proto::pb::ServerError::MetadataError as i32,
                message: "close failed".to_owned(),
            }),
            ..Default::default()
        };
        child_shared
            .inner
            .lock()
            .handle_bytes(std::time::Instant::now(), &encode(&error))
            .expect("reject child close");

        assert!(matches!(
            operation.await,
            Err(StreamConsumerError::Client(ClientError::Broker { code, .. }))
                if code == magnetar_proto::pb::ServerError::MetadataError as i32
        ));
        {
            let state = inner.state.lock();
            assert!(state.children.is_empty());
            assert!(
                state
                    .model
                    .segment_phase(magnetar_proto::SegmentId(1))
                    .is_none()
            );
        }
        let mut staged = child_shared.inner.lock().poll_transmit();
        assert!(
            magnetar_proto::decode_one(&mut staged)
                .expect("forced best-effort retry")
                .command
                .close_consumer
                .is_some()
        );
    }

    #[tokio::test]
    async fn final_ack_completes_sealed_parent_before_deferred_close_confirmation() {
        let (snapshot, assignment) = sealed_parent_fixture();
        let (inner, child_shared) = aggregate_inner_with_child_from(snapshot, assignment.clone());
        let child = inner
            .state
            .lock()
            .children
            .values()
            .next()
            .expect("sealed parent child")
            .clone();
        let session_epoch = child_shared.inner.lock().session_epoch();
        inner
            .message_arrived(
                child.source.clone(),
                child.generation,
                session_epoch,
                &child.consumer,
                deferred_message(
                    71,
                    magnetar_proto::pb::MessageMetadata::default(),
                    bytes::Bytes::from_static(b"terminal"),
                    1,
                ),
            )
            .await
            .expect("retain sealed-parent delivery");
        let message = inner
            .reserve_batch(1, usize::MAX)
            .await
            .expect("reserve sealed-parent delivery")
            .pop()
            .expect("one sealed-parent delivery");
        let empty = magnetar_proto::ConsumerAssignment::try_from_pb(
            &magnetar_proto::pb::ScalableConsumerAssignment {
                layout_epoch: assignment.layout_epoch(),
                segments: Vec::new(),
            },
            "topic://public/default/scaled",
        )
        .expect("empty rebalance assignment");
        let stop = inner
            .state
            .lock()
            .model
            .apply_assignment(empty)
            .expect("sealed parent loses ownership while delivery is live");
        inner.execute_actions(stop).await.expect("stop parent flow");
        assert!(
            inner
                .state
                .lock()
                .model
                .apply_assignment(assignment)
                .expect("sealed parent regains ownership while draining")
                .is_empty()
        );
        {
            let mut state = inner.state.lock();
            assert_eq!(state.model.pending_ownership(), vec![child.source.clone()]);
            assert!(
                state
                    .model
                    .observe_terminal(child.source.segment_id(), child.generation)
                    .expect("sealed parent terminal with live acknowledgement")
                    .is_empty()
            );
        }
        let _ = child_shared.inner.lock().poll_transmit();

        let consumer = StreamConsumer {
            inner: inner.clone(),
        };
        let mut acknowledgement = Box::pin(consumer.acknowledge(&message.token));
        std::future::poll_fn(|context| {
            assert!(matches!(
                std::future::Future::poll(acknowledgement.as_mut(), context),
                std::task::Poll::Pending
            ));
            std::task::Poll::Ready(())
        })
        .await;
        let ack = magnetar_proto::decode_one(&mut child_shared.inner.lock().poll_transmit())
            .expect("decode terminal acknowledgement")
            .command
            .ack
            .expect("CommandAck");
        let ack_response = magnetar_proto::pb::BaseCommand {
            r#type: magnetar_proto::pb::base_command::Type::AckResponse as i32,
            ack_response: Some(magnetar_proto::pb::CommandAckResponse {
                consumer_id: ack.consumer_id,
                request_id: ack.request_id,
                ..Default::default()
            }),
            ..Default::default()
        };
        child_shared
            .inner
            .lock()
            .handle_bytes(std::time::Instant::now(), &encode(&ack_response))
            .expect("confirm terminal acknowledgement");
        std::future::poll_fn(|context| {
            assert!(matches!(
                std::future::Future::poll(acknowledgement.as_mut(), context),
                std::task::Poll::Pending
            ));
            std::task::Poll::Ready(())
        })
        .await;
        let close = magnetar_proto::decode_one(&mut child_shared.inner.lock().poll_transmit())
            .expect("decode sealed-parent close")
            .command
            .close_consumer
            .expect("CommandCloseConsumer");
        let close_response = magnetar_proto::pb::BaseCommand {
            r#type: magnetar_proto::pb::base_command::Type::Success as i32,
            success: Some(magnetar_proto::pb::CommandSuccess {
                request_id: close.request_id,
                schema: None,
            }),
            ..Default::default()
        };
        child_shared
            .inner
            .lock()
            .handle_bytes(std::time::Instant::now(), &encode(&close_response))
            .expect("confirm sealed-parent close");
        acknowledgement
            .await
            .expect("terminal acknowledgement settles");

        let state = inner.state.lock();
        assert!(state.children.is_empty());
        assert!(state.model.pending_ownership().is_empty());
        assert!(
            state
                .model
                .segment_phase(child.source.segment_id())
                .is_none()
        );
        assert_eq!(state.open_tasks, 0, "completed sealed parent never reopens");
    }

    #[tokio::test]
    async fn compressed_batch_is_transformed_atomically_and_repays_dispatch_debt() {
        let (inner, child_shared) = aggregate_inner_with_child();
        let child = inner
            .state
            .lock()
            .children
            .values()
            .next()
            .expect("child")
            .clone();
        let plain = batch_payload(&[b"first", b"second"]);
        let message = deferred_message(
            8,
            magnetar_proto::pb::MessageMetadata {
                compression: Some(magnetar_proto::pb::CompressionType::Zlib as i32),
                uncompressed_size: Some(u32::try_from(plain.len()).expect("batch fits u32")),
                num_messages_in_batch: Some(2),
                ..Default::default()
            },
            zlib(&plain),
            2,
        );

        let session_epoch = child_shared.inner.lock().session_epoch();
        inner
            .message_arrived(
                child.source.clone(),
                child.generation,
                session_epoch,
                &child.consumer,
                message,
            )
            .await
            .expect("process compressed batch");
        let messages = inner
            .reserve_batch(2, usize::MAX)
            .await
            .expect("reserve expanded batch");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message.payload, b"first".as_slice());
        assert_eq!(messages[1].message.payload, b"second".as_slice());
        for (index, message) in messages.iter().enumerate() {
            let ordinary = message
                .token
                .stream_message_id()
                .ordinary_message_id_data()
                .expect("canonical batch id");
            assert_eq!(ordinary.batch_index, Some(index as i32));
            assert_eq!(ordinary.batch_size, Some(2));
            assert_eq!(ordinary.ack_set, vec![3]);
        }

        let mut outbound = child_shared.inner.lock().poll_transmit();
        let mut permits = Vec::new();
        while !outbound.is_empty() {
            let command = magnetar_proto::decode_one(&mut outbound)
                .expect("decode aggregate flow")
                .command;
            if let Some(flow) = command.flow {
                permits.push(flow.message_permits);
            }
        }
        assert_eq!(permits, vec![2]);
    }

    #[tokio::test]
    async fn partial_batch_delivers_selected_members_with_exact_ids_and_debt() {
        let (inner, child_shared) = aggregate_inner_with_child();
        let child = inner
            .state
            .lock()
            .children
            .values()
            .next()
            .expect("child")
            .clone();
        let mut message = deferred_message(
            9,
            magnetar_proto::pb::MessageMetadata {
                num_messages_in_batch: Some(3),
                ..Default::default()
            },
            batch_payload(&[b"first", b"omitted", b"third"]),
            2,
        );
        message.ack_set = vec![0b101];

        let session_epoch = child_shared.inner.lock().session_epoch();
        inner
            .message_arrived(
                child.source.clone(),
                child.generation,
                session_epoch,
                &child.consumer,
                message,
            )
            .await
            .expect("process partial batch");
        let messages = inner
            .reserve_batch(3, usize::MAX)
            .await
            .expect("reserve selected members");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message.payload, b"first".as_slice());
        assert_eq!(messages[1].message.payload, b"third".as_slice());
        for (message, batch_index) in messages.iter().zip([0, 2]) {
            let ordinary = message
                .token
                .stream_message_id()
                .ordinary_message_id_data()
                .expect("canonical partial-batch id");
            assert_eq!(ordinary.batch_index, Some(batch_index));
            assert_eq!(ordinary.batch_size, Some(3));
            assert_eq!(ordinary.ack_set, vec![0b101]);
        }
        let mut outbound = child_shared.inner.lock().poll_transmit();
        let mut permits = Vec::new();
        while !outbound.is_empty() {
            if let Some(flow) = magnetar_proto::decode_one(&mut outbound)
                .expect("decode aggregate flow")
                .command
                .flow
            {
                permits.push(flow.message_permits);
            }
        }
        assert_eq!(permits, vec![2]);
    }

    #[test]
    fn reconnect_drops_old_session_batch_debt_from_fresh_flow() {
        let (inner, child_shared) = aggregate_inner_with_child();
        let child = inner
            .state
            .lock()
            .children
            .values()
            .next()
            .expect("child")
            .consumer
            .clone();
        let old_epoch = child_shared.inner.lock().session_epoch();
        child_shared.inner.lock().reset();

        child.flow_for_aggregate_with_debt(1, Some((old_epoch, 3)));

        let command = magnetar_proto::decode_one(&mut child_shared.inner.lock().poll_transmit())
            .expect("decode post-reconnect flow")
            .command
            .flow
            .expect("CommandFlow");
        assert_eq!(command.message_permits, 1);
    }

    #[tokio::test]
    async fn chunk_continuation_reassembles_and_retains_first_chunk_id() {
        let (inner, child_shared) = aggregate_inner_with_child();
        let child = inner
            .state
            .lock()
            .children
            .values()
            .next()
            .expect("child")
            .clone();
        let chunk_metadata = |chunk_id| magnetar_proto::pb::MessageMetadata {
            uuid: Some("aggregate-chunk".to_owned()),
            num_chunks_from_msg: Some(2),
            chunk_id: Some(chunk_id),
            total_chunk_msg_size: Some(4),
            ..Default::default()
        };

        let session_epoch = child_shared.inner.lock().session_epoch();
        inner
            .message_arrived(
                child.source.clone(),
                child.generation,
                session_epoch,
                &child.consumer,
                deferred_message(10, chunk_metadata(0), bytes::Bytes::from_static(b"ab"), 1),
            )
            .await
            .expect("buffer first chunk");
        let _ = child_shared.inner.lock().poll_transmit();
        let session_epoch = child_shared.inner.lock().session_epoch();
        inner
            .message_arrived(
                child.source.clone(),
                child.generation,
                session_epoch,
                &child.consumer,
                deferred_message(11, chunk_metadata(1), bytes::Bytes::from_static(b"cd"), 1),
            )
            .await
            .expect("complete chunk chain");
        let messages = inner
            .reserve_batch(1, usize::MAX)
            .await
            .expect("reserve reassembled message");
        assert_eq!(messages[0].message.payload, b"abcd".as_slice());
        let ordinary = messages[0]
            .token
            .stream_message_id()
            .ordinary_message_id_data()
            .expect("canonical chunk id");
        assert_eq!(ordinary.entry_id, 11);
        assert_eq!(
            ordinary
                .first_chunk_message_id
                .as_deref()
                .map(|first| first.entry_id),
            Some(10)
        );
    }

    #[tokio::test]
    async fn cancelled_aggregate_seek_requests_resynchronization() {
        let (inner, _child_shared) = aggregate_inner_with_child();
        let source = inner
            .state
            .lock()
            .model
            .assignment()
            .expect("assignment")
            .segments()[0]
            .source();
        let vector =
            magnetar_proto::PositionVector::new(1, [(source, magnetar_proto::MessageId::EARLIEST)])
                .expect("seek vector");
        let consumer = StreamConsumer {
            inner: inner.clone(),
        };
        let mut seek = Box::pin(consumer.seek_positions(&vector));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(seek.as_mut(), &mut context).is_pending());

        drop(seek);

        let state = inner.state.lock();
        assert_eq!(
            state.model.phase(),
            magnetar_proto::AggregatePhase::ResyncRequired
        );
        assert!(state.reconnect_requested);
        assert!(matches!(
            state.events.back(),
            Some(StreamConsumerEvent::ResyncRequired { reason })
                if reason == "aggregate seek was cancelled"
        ));
    }

    #[tokio::test]
    async fn execute_actions_arms_all_seek_children_before_first_wait() {
        let (inner, first_shared) = aggregate_inner_with_child();
        let (first_source, generation, controller_incarnation) = {
            let state = inner.state.lock();
            let child = state.children.values().next().expect("first child");
            (
                child.source.clone(),
                child.generation,
                state.model.controller_incarnation(),
            )
        };
        let second_source = magnetar_proto::SegmentSource::new(
            magnetar_proto::SegmentId(2),
            magnetar_proto::canonical_segment_topic(
                "topic://public/default/scaled",
                magnetar_proto::KeyRange::FULL,
                magnetar_proto::SegmentId(2),
            )
            .expect("second segment topic"),
        )
        .expect("second source");
        let (second_consumer, second_shared) = attached_child_consumer(&second_source);
        inner.state.lock().children.insert(
            second_source.segment_id(),
            ChildRuntime {
                source: second_source.clone(),
                generation,
                consumer: second_consumer,
            },
        );
        let target = |source: magnetar_proto::SegmentSource, entry_id| {
            magnetar_proto::StreamMessageId::new(
                source,
                magnetar_proto::MessageId {
                    ledger_id: 3,
                    entry_id,
                    partition: -1,
                    batch_index: -1,
                    batch_size: 0,
                },
            )
            .expect("seek target")
        };
        let actions = vec![
            magnetar_proto::StreamConsumerAction::SeekChild {
                source: first_source.clone(),
                controller_incarnation,
                child_generation: generation,
                stream_message_id: target(first_source, 5),
            },
            magnetar_proto::StreamConsumerAction::SeekChild {
                source: second_source.clone(),
                controller_incarnation,
                child_generation: generation,
                stream_message_id: target(second_source, 6),
            },
        ];
        let mut operation = Box::pin(inner.execute_actions(actions));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(operation.as_mut(), &mut context).is_pending());

        for shared in [first_shared, second_shared] {
            let mut bytes = shared.inner.lock().poll_transmit();
            let mut seeks = 0;
            while !bytes.is_empty() {
                if magnetar_proto::decode_one(&mut bytes)
                    .expect("decode staged seek")
                    .command
                    .seek
                    .is_some()
                {
                    seeks += 1;
                }
            }
            assert_eq!(seeks, 1);
        }
        drop(operation);

        let (missing_inner, _) = aggregate_inner_with_child();
        let missing_source = missing_inner
            .state
            .lock()
            .model
            .assignment()
            .expect("missing-child assignment")
            .segments()[0]
            .source();
        let vector = magnetar_proto::PositionVector::new(
            1,
            [(missing_source.clone(), magnetar_proto::MessageId::EARLIEST)],
        )
        .expect("missing-child seek vector");
        let actions = {
            let mut state = missing_inner.state.lock();
            let actions = state
                .model
                .begin_seek(&vector)
                .expect("begin missing-child seek");
            state.children.clear();
            actions
        };
        assert!(matches!(
            missing_inner.execute_actions(actions).await,
            Err(StreamConsumerError::Model(
                magnetar_proto::StreamConsumerModelError::PositionSourceUnavailable {
                    segment_source,
                }
            )) if segment_source == missing_source
        ));
        let state = missing_inner.state.lock();
        assert_eq!(
            state.model.phase(),
            magnetar_proto::AggregatePhase::ResyncRequired
        );
        assert!(state.reconnect_requested);
    }

    #[tokio::test]
    async fn failed_aggregate_seek_closes_children_before_requesting_resync() {
        let (inner, child_shared) = aggregate_inner_with_child();
        let source = inner
            .state
            .lock()
            .model
            .assignment()
            .expect("assignment")
            .segments()[0]
            .source();
        let vector = magnetar_proto::PositionVector::new(
            1,
            [(
                source,
                magnetar_proto::MessageId {
                    ledger_id: 3,
                    entry_id: 5,
                    partition: -1,
                    batch_index: -1,
                    batch_size: 0,
                },
            )],
        )
        .expect("seek vector");
        let actions = inner
            .state
            .lock()
            .model
            .begin_seek(&vector)
            .expect("begin seek");
        let task_inner = inner.clone();
        let task = tokio::spawn(async move {
            let result = task_inner.execute_actions(actions).await;
            if let Err(error) = &result {
                task_inner.request_resync(error.to_string());
            }
            result
        });
        tokio::task::yield_now().await;

        let seek_request_id = {
            let mut staged = child_shared.inner.lock().poll_transmit();
            let mut request_id = None;
            while !staged.is_empty() {
                let command = magnetar_proto::decode_one(&mut staged)
                    .expect("decode seek frame")
                    .command;
                if let Some(seek) = command.seek {
                    request_id = Some(seek.request_id);
                }
            }
            request_id.expect("seek command")
        };
        let error = magnetar_proto::pb::BaseCommand {
            r#type: magnetar_proto::pb::base_command::Type::Error as i32,
            error: Some(magnetar_proto::pb::CommandError {
                request_id: seek_request_id,
                error: magnetar_proto::pb::ServerError::MetadataError as i32,
                message: "seek failed".to_owned(),
            }),
            ..Default::default()
        };
        let frame = encode(&error);
        child_shared
            .inner
            .lock()
            .handle_bytes(std::time::Instant::now(), &frame)
            .expect("reject seek");
        tokio::task::yield_now().await;
        assert!(
            inner.state.lock().reconnect_requested,
            "seek failure fences child loops before close confirmation"
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed seek returns while owned teardown retains the child close");
        assert_eq!(inner.state.lock().children.len(), 1);

        let close_request_id = {
            let mut staged = child_shared.inner.lock().poll_transmit();
            let mut request_id = None;
            while !staged.is_empty() {
                let command = magnetar_proto::decode_one(&mut staged)
                    .expect("decode cleanup frame")
                    .command;
                if let Some(close) = command.close_consumer {
                    request_id = Some(close.request_id);
                }
            }
            request_id.expect("confirmation-bearing child close")
        };
        let success = magnetar_proto::pb::BaseCommand {
            r#type: magnetar_proto::pb::base_command::Type::Success as i32,
            success: Some(magnetar_proto::pb::CommandSuccess {
                request_id: close_request_id,
                schema: None,
            }),
            ..Default::default()
        };
        let frame = encode(&success);
        child_shared
            .inner
            .lock()
            .handle_bytes(std::time::Instant::now(), &frame)
            .expect("confirm child close");
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .expect("seek cleanup completes")
                .expect("seek task"),
            Err(StreamConsumerError::Client(ClientError::Broker {
                code,
                message,
            })) if code == magnetar_proto::pb::ServerError::MetadataError as i32
                 && message == "seek failed"
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !inner.state.lock().children.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned seek teardown completes");

        let state = inner.state.lock();
        assert_eq!(
            state.model.phase(),
            magnetar_proto::AggregatePhase::ResyncRequired
        );
        assert!(state.model.status().pending_ownership().is_empty());
        assert!(state.children.is_empty());
        assert!(state.flow_reservations.is_empty());
        assert!(state.reconnect_requested);
        assert!(matches!(
            state.events.back(),
            Some(StreamConsumerEvent::ResyncRequired { .. })
        ));
    }

    #[tokio::test]
    async fn scalable_task_handle_joins_owned_work() {
        let subscriber = subscriber_with_allow_list();
        let handle = subscriber.spawn_task(async {});
        handle.join().await.expect("task joins");
    }

    #[tokio::test]
    async fn completed_aggregate_tasks_are_reaped_when_tracking_new_work() {
        let inner = empty_aggregate_inner();
        let completed = inner.subscriber.spawn_task(async {});
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !completed.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first task completes");
        inner.state.lock().tasks.push(completed);

        let gate = Arc::new(Notify::new());
        let task_gate = gate.clone();
        let pending = inner.subscriber.spawn_task(async move {
            task_gate.notified().await;
        });
        {
            let mut state = inner.state.lock();
            StreamConsumerInner::track_task(&mut state, pending);
            assert_eq!(state.tasks.len(), 1);
        }

        gate.notify_one();
        let tasks = core::mem::take(&mut inner.state.lock().tasks);
        for task in tasks {
            task.join().await.expect("tracked task joins");
        }
    }

    #[test]
    fn transaction_outcome_admission_is_singleflight_and_retryable() {
        let completion = TransactionOutcomeCompletion::new(
            magnetar_proto::TransactionAcknowledgementOutcome::Committed,
        );
        assert!(completion.try_start());
        assert!(!completion.try_start(), "one worker owns propagation");

        completion.finish(Err("retryable failure".to_owned()));
        assert!(completion.try_start(), "a failed worker may be retried");
        completion.finish(Ok(()));
        assert!(!completion.try_start(), "success is terminal");

        let state = completion.state.lock();
        assert!(matches!(state.result, Some(Ok(()))));
        assert!(!state.running);
    }

    #[tokio::test]
    async fn transaction_outcome_continues_after_waiter_cancellation() {
        let mut inner = empty_aggregate_inner();
        let hook = Arc::new(TransactionOutcomeParkHook::default());
        Arc::get_mut(&mut inner)
            .expect("fixture has one aggregate owner")
            .transaction_outcome_park_hook = Some(hook.clone());
        let txn_id = magnetar_proto::TxnId::new(17, 23);
        let waiter_inner = inner.clone();
        let waiter = tokio::spawn(async move {
            waiter_inner
                .transaction_outcome(
                    txn_id,
                    magnetar_proto::TransactionAcknowledgementOutcome::Committed,
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), hook.reached.notified())
            .await
            .expect("outcome worker reaches participant propagation");
        assert!(
            inner
                .state
                .lock()
                .tasks
                .iter()
                .any(|task| !task.is_finished()),
            "outcome work is tracked before it can race aggregate close"
        );
        waiter.abort();
        assert!(waiter.await.expect_err("waiter cancelled").is_cancelled());
        hook.release.notify_one();

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            inner.transaction_outcome(
                txn_id,
                magnetar_proto::TransactionAcknowledgementOutcome::Committed,
            ),
        )
        .await
        .expect("retry observes owned outcome task")
        .expect("outcome propagation completes");
        let state = inner.state.lock();
        assert!(matches!(
            state
                .transaction_outcomes
                .get(&txn_id)
                .expect("tracked outcome")
                .state
                .lock()
                .result
                .clone(),
            Some(Ok(()))
        ));
        assert_eq!(
            state
                .events
                .iter()
                .filter(|event| matches!(event, StreamConsumerEvent::TransactionOutcome { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn transaction_outcome_controls_child_reconnect_ack_state() {
        let (inner, child_shared) = aggregate_inner_with_child();
        let child = inner
            .state
            .lock()
            .children
            .values()
            .next()
            .expect("attached child")
            .consumer
            .clone();
        let message_id = magnetar_proto::MessageId {
            ledger_id: 31,
            entry_id: 41,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
        };
        let aborted = magnetar_proto::TxnId::new(19, 29);
        let ack_child = child.clone();
        let ack = tokio::spawn(async move {
            ack_child
                .ack_stream_component(
                    vec![message_id],
                    vec![message_id.to_pb()],
                    magnetar_proto::pb::command_ack::AckType::Individual,
                    Some(aborted),
                )
                .await
        });
        tokio::task::yield_now().await;
        let command = magnetar_proto::decode_one(&mut child_shared.inner.lock().poll_transmit())
            .expect("decode transactional acknowledgement")
            .command
            .ack
            .expect("CommandAck");
        let response = magnetar_proto::pb::BaseCommand {
            r#type: magnetar_proto::pb::base_command::Type::AckResponse as i32,
            ack_response: Some(magnetar_proto::pb::CommandAckResponse {
                consumer_id: command.consumer_id,
                txnid_least_bits: command.txnid_least_bits,
                txnid_most_bits: command.txnid_most_bits,
                error: None,
                message: None,
                request_id: command.request_id,
            }),
            ..Default::default()
        };
        child_shared
            .inner
            .lock()
            .handle_bytes(std::time::Instant::now(), &encode(&response))
            .expect("accept transactional acknowledgement");
        ack.await.expect("ack task").expect("ack accepted");
        assert_eq!(child.last_acked_message_id_for_test(), None);

        inner
            .transaction_outcome(
                aborted,
                magnetar_proto::TransactionAcknowledgementOutcome::Aborted,
            )
            .await
            .expect("abort propagation");
        assert_eq!(child.last_acked_message_id_for_test(), None);

        let committed = magnetar_proto::TxnId::new(20, 30);
        let ack_child = child.clone();
        let ack = tokio::spawn(async move {
            ack_child
                .ack_stream_component(
                    vec![message_id],
                    vec![message_id.to_pb()],
                    magnetar_proto::pb::command_ack::AckType::Individual,
                    Some(committed),
                )
                .await
        });
        tokio::task::yield_now().await;
        let command = magnetar_proto::decode_one(&mut child_shared.inner.lock().poll_transmit())
            .expect("decode committed acknowledgement")
            .command
            .ack
            .expect("CommandAck");
        let response = magnetar_proto::pb::BaseCommand {
            r#type: magnetar_proto::pb::base_command::Type::AckResponse as i32,
            ack_response: Some(magnetar_proto::pb::CommandAckResponse {
                consumer_id: command.consumer_id,
                txnid_least_bits: command.txnid_least_bits,
                txnid_most_bits: command.txnid_most_bits,
                error: None,
                message: None,
                request_id: command.request_id,
            }),
            ..Default::default()
        };
        child_shared
            .inner
            .lock()
            .handle_bytes(std::time::Instant::now(), &encode(&response))
            .expect("accept committed acknowledgement");
        ack.await.expect("ack task").expect("ack accepted");
        assert_eq!(child.last_acked_message_id_for_test(), None);

        inner
            .transaction_outcome(
                committed,
                magnetar_proto::TransactionAcknowledgementOutcome::Committed,
            )
            .await
            .expect("commit propagation");
        assert_eq!(child.last_acked_message_id_for_test(), Some(message_id));
    }

    #[tokio::test]
    async fn interrupted_transaction_outcome_fences_stale_flow_and_retries_close() {
        let (inner, child_shared) = aggregate_inner_with_child();
        let actions = {
            let mut state = inner.state.lock();
            let child = state.children.values().next().expect("attached child");
            let key = (child.source.segment_id(), child.generation);
            let flow = magnetar_proto::StreamConsumerAction::GrantFlow {
                source: child.source.clone(),
                controller_incarnation: state.model.controller_incarnation(),
                child_generation: child.generation,
                reservation: state.flow_reservations[&key],
                purpose: magnetar_proto::FlowPurpose::Message,
            };
            let mut actions = state
                .model
                .require_resync()
                .expect("produce confirmation-bearing close");
            actions.insert(0, flow);
            actions
        };
        assert!(matches!(
            actions.as_slice(),
            [
                magnetar_proto::StreamConsumerAction::GrantFlow { .. },
                magnetar_proto::StreamConsumerAction::CloseChild { .. }
            ]
        ));
        let completion = Arc::new(TransactionOutcomeCompletion::new(
            magnetar_proto::TransactionAcknowledgementOutcome::Committed,
        ));
        *completion.work.lock() = Some(TransactionOutcomeWork {
            actions: actions.into(),
            completions: VecDeque::new(),
        });
        let txn_id = magnetar_proto::TxnId::new(21, 31);
        let mut first = Box::pin(inner.propagate_transaction_outcome(
            txn_id,
            magnetar_proto::TransactionAcknowledgementOutcome::Committed,
            &completion,
        ));
        std::future::poll_fn(|context| {
            assert!(matches!(
                std::future::Future::poll(first.as_mut(), context),
                std::task::Poll::Pending
            ));
            std::task::Poll::Ready(())
        })
        .await;
        drop(first);
        assert_eq!(inner.state.lock().children.len(), 1);
        let mut first_staged = child_shared.inner.lock().poll_transmit();
        let mut first_flows = 0;
        while !first_staged.is_empty() {
            if magnetar_proto::decode_one(&mut first_staged)
                .expect("decode first outcome work")
                .command
                .flow
                .is_some()
            {
                first_flows += 1;
            }
        }
        assert_eq!(first_flows, 0, "closing children reject stale FLOW work");

        let mut retry = Box::pin(inner.propagate_transaction_outcome(
            txn_id,
            magnetar_proto::TransactionAcknowledgementOutcome::Committed,
            &completion,
        ));
        std::future::poll_fn(|context| {
            assert!(matches!(
                std::future::Future::poll(retry.as_mut(), context),
                std::task::Poll::Pending
            ));
            std::task::Poll::Ready(())
        })
        .await;
        let close_request_id = {
            let mut staged = child_shared.inner.lock().poll_transmit();
            let mut request_id = None;
            let mut retried_flows = 0;
            while !staged.is_empty() {
                let command = magnetar_proto::decode_one(&mut staged)
                    .expect("decode child close")
                    .command;
                if let Some(close) = command.close_consumer {
                    request_id = Some(close.request_id);
                }
                if command.flow.is_some() {
                    retried_flows += 1;
                }
            }
            assert_eq!(retried_flows, 0, "completed FLOW prefix is not replayed");
            request_id.expect("retry owns a confirmation-bearing close")
        };
        let success = magnetar_proto::pb::BaseCommand {
            r#type: magnetar_proto::pb::base_command::Type::Success as i32,
            success: Some(magnetar_proto::pb::CommandSuccess {
                request_id: close_request_id,
                schema: None,
            }),
            ..Default::default()
        };
        child_shared
            .inner
            .lock()
            .handle_bytes(std::time::Instant::now(), &encode(&success))
            .expect("confirm retried close");
        retry.await.expect("retained action completes");
        assert!(inner.state.lock().children.is_empty());
    }

    #[test]
    fn local_close_discards_runtime_transaction_state() {
        let inner = empty_aggregate_inner();
        let txn_id = magnetar_proto::TxnId::new(7, 11);
        let source = inner
            .state
            .lock()
            .model
            .assignment()
            .expect("assignment")
            .segments()[0]
            .source();
        {
            let mut state = inner.state.lock();
            state.pending_transactions.insert(txn_id, Vec::new());
            state
                .transaction_registrations
                .insert((txn_id, source), TransactionRegistration::Registered);
        }

        inner.close_best_effort();

        let state = inner.state.lock();
        assert!(state.pending_transactions.is_empty());
        assert!(state.transaction_registrations.is_empty());
    }

    #[test]
    fn retired_controller_route_swallows_late_events_and_allows_new_incarnation() {
        let shared = shared();
        let epoch = shared.inner.lock().session_epoch();
        let route = shared.scalable_routes.claim_at_epoch(
            shared.clone(),
            ScalableRouteKey::consumer(11, magnetar_proto::ControllerIncarnation(1)),
            epoch,
        );
        drop(route);
        assert!(
            shared
                .scalable_routes
                .publish(ScalableEvent::ConsumerRejected {
                    consumer_id: 11,
                    incarnation: magnetar_proto::ControllerIncarnation(1),
                    reason: "late".to_owned(),
                })
                .is_none()
        );
        shared.scalable_routes.claim_at_epoch(
            shared.clone(),
            ScalableRouteKey::consumer(11, magnetar_proto::ControllerIncarnation(2)),
            epoch,
        );
    }

    #[test]
    fn control_plane_route_errors_distinguish_resync_from_terminal_closure() {
        let replaced = ClientError::ScalableRoute(ScalableRouteError::ConnectionReplaced);
        let overflow = ClientError::ScalableRoute(ScalableRouteError::Overflow { capacity: 1 });
        let closed = ClientError::ScalableRoute(ScalableRouteError::ConnectionClosed);

        assert!(route_error_is_recoverable(&replaced));
        assert!(route_error_is_recoverable(&overflow));
        assert!(!control_plane_error_is_terminal(&replaced));
        assert!(!control_plane_error_is_terminal(&overflow));
        assert!(control_plane_error_is_terminal(&closed));
        assert!(control_plane_error_is_terminal(&ClientError::PeerClosed));
    }

    #[test]
    fn retired_route_tombstones_are_bounded_and_fence_recent_events() {
        let shared = shared();
        let epoch = shared.inner.lock().session_epoch();
        let first_id = 10_000;
        for offset in 0..=MAX_RETIRED_ROUTES as u64 {
            let route = shared.scalable_routes.claim_at_epoch(
                shared.clone(),
                ScalableRouteKey::consumer(
                    first_id + offset,
                    magnetar_proto::ControllerIncarnation(1),
                ),
                epoch,
            );
            drop(route);
        }

        {
            let state = shared.scalable_routes.state.lock();
            assert!(state.routes.is_empty());
            assert!(state.active.is_empty());
            assert_eq!(state.retired.len(), MAX_RETIRED_ROUTES);
        }

        assert!(
            shared
                .scalable_routes
                .publish(ScalableEvent::ConsumerRejected {
                    consumer_id: first_id + MAX_RETIRED_ROUTES as u64,
                    incarnation: magnetar_proto::ControllerIncarnation(1),
                    reason: "recent late event".to_owned(),
                })
                .is_none(),
            "the newest bounded tombstone must still swallow late events"
        );
        assert!(matches!(
            shared
                .scalable_routes
                .publish(ScalableEvent::ConsumerRejected {
                    consumer_id: first_id,
                    incarnation: magnetar_proto::ControllerIncarnation(1),
                    reason: "expired late event".to_owned(),
                }),
            Some(ScalableEvent::ConsumerRejected { .. })
        ));
    }

    #[tokio::test]
    async fn scalable_authorities_preserve_scheme_and_obey_allow_list() {
        let subscriber = subscriber_with_allow_list();
        let bootstrap = subscriber
            .resolve_direct_url("pulsar://allowed.example:6650")
            .await
            .expect("matching bootstrap authority");
        assert!(Arc::ptr_eq(&bootstrap, &subscriber.bootstrap));
        assert!(matches!(
            subscriber
                .resolve_direct_url("pulsar://rejected.example:6650")
                .await,
            Err(ClientError::ScalableAuthorityRejected)
        ));
        assert!(matches!(
            subscriber
                .resolve_direct_url("pulsar+ssl://allowed.example:6651")
                .await,
            Err(ClientError::ControllerRoutingUnsupported { .. })
        ));
    }

    #[tokio::test]
    async fn segment_subscribe_starts_paused_without_initial_flow() {
        let shared = connected_shared();
        let request = magnetar_proto::SubscribeRequest {
            topic: "segment://public/default/scaled/0000-ffff-1".to_owned(),
            subscription: "sub".to_owned(),
            receiver_queue_size: 0,
            ..Default::default()
        };
        let task_shared = shared.clone();
        let task = tokio::spawn(async move {
            subscribe_manual_flow_on(task_shared, request, std::time::Duration::from_secs(1)).await
        });
        tokio::task::yield_now().await;
        let request_id = {
            let mut conn = shared.inner.lock();
            let mut transmit = conn.poll_transmit();
            magnetar_proto::decode_one(&mut transmit)
                .expect("subscribe frame")
                .command
                .subscribe
                .expect("subscribe payload")
                .request_id
        };
        let success = encode(&magnetar_proto::pb::BaseCommand {
            r#type: magnetar_proto::pb::base_command::Type::Success as i32,
            success: Some(magnetar_proto::pb::CommandSuccess {
                request_id,
                schema: None,
            }),
            ..Default::default()
        });
        shared
            .inner
            .lock()
            .handle_bytes(std::time::Instant::now(), &success)
            .expect("subscribe success");
        shared.event_waker.notify_waiters();
        let consumer = task.await.expect("task").expect("consumer");
        assert!(consumer.is_paused());
        assert_eq!(consumer.current_receiver_queue_size(), 0);
        assert_eq!(consumer.available_permits(), 0);
        assert!(shared.inner.lock().poll_transmit().is_empty());
    }

    #[tokio::test]
    async fn aggregate_batch_reservation_is_atomic() {
        let inner = aggregate_inner_with_two_messages();
        {
            let mut state = inner.state.lock();
            let QueuedDelivery::Fresh {
                message_id_data, ..
            } = &mut state.queue[1].delivery
            else {
                panic!("aggregate fixture queue entries are fresh");
            };
            message_id_data.partition = Some(-2);
        }

        assert!(matches!(
            inner.reserve_batch(2, usize::MAX).await,
            Err(StreamConsumerError::Model(
                magnetar_proto::StreamConsumerModelError::Position(
                    magnetar_proto::StreamPositionError::ImpossibleOrdinaryId {
                        field: "partition",
                        value: -2,
                    }
                )
            ))
        ));
        {
            let state = inner.state.lock();
            assert_eq!(state.queue.len(), 2);
            assert!(state.model.delivered_position().is_empty());
        }

        {
            let mut state = inner.state.lock();
            let QueuedDelivery::Fresh {
                message_id_data, ..
            } = &mut state.queue[1].delivery
            else {
                panic!("aggregate fixture queue entries are fresh");
            };
            message_id_data.partition = Some(-1);
        }
        let messages = inner
            .reserve_batch(2, usize::MAX)
            .await
            .expect("valid batch");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message.message_id.entry_id, 1);
        assert_eq!(messages[1].message.message_id.entry_id, 2);
        let state = inner.state.lock();
        assert!(state.queue.is_empty());
        assert_eq!(state.model.delivered_position().len(), 1);
    }

    #[tokio::test]
    async fn cancelled_acknowledgement_releases_operation_authority() {
        let inner = aggregate_inner_with_two_messages();
        let messages = inner.reserve_batch(1, usize::MAX).await.expect("delivery");
        let transition = inner
            .state
            .lock()
            .model
            .admit_individual_acknowledgement(&messages[0].token)
            .expect("acknowledgement admission");

        drop(AcknowledgementCancellation::new(
            &inner,
            &transition.authority,
        ));

        let retry = inner
            .state
            .lock()
            .model
            .admit_individual_acknowledgement(&messages[0].token)
            .expect("cancelled operation is retryable");
        inner
            .state
            .lock()
            .model
            .cancel_acknowledgement(&retry.authority)
            .expect("retry cleanup");
    }

    #[tokio::test]
    async fn cancelled_transaction_registration_releases_singleflight_slot() {
        let inner = aggregate_inner_with_two_messages();
        let source = inner
            .state
            .lock()
            .model
            .assignment()
            .expect("assignment")
            .segments()[0]
            .source();
        let key = (magnetar_proto::TxnId::new(1, 2), source);
        inner
            .state
            .lock()
            .transaction_registrations
            .insert(key.clone(), TransactionRegistration::Pending);

        drop(TransactionRegistrationCancellation::new(
            &inner,
            key.clone(),
        ));

        assert!(
            !inner
                .state
                .lock()
                .transaction_registrations
                .contains_key(&key)
        );
    }

    #[tokio::test]
    async fn concurrent_close_waits_for_owned_task_barrier() {
        let inner = aggregate_inner_with_two_messages();
        let gate = Arc::new(Notify::new());
        let task_gate = gate.clone();
        let handle = inner.subscriber.spawn_task(async move {
            task_gate.notified().await;
        });
        inner.state.lock().tasks.push(handle);

        let first_inner = inner.clone();
        let first = tokio::spawn(async move { first_inner.close().await });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while inner.state.lock().close_state != AggregateCloseState::Closing {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first close owns cleanup");
        let second_inner = inner.clone();
        let second = tokio::spawn(async move { second_inner.close().await });
        tokio::task::yield_now().await;
        assert!(!second.is_finished());

        gate.notify_one();
        first.await.expect("first task").expect("first close");
        second.await.expect("second task").expect("second close");
    }

    #[tokio::test]
    async fn buffered_event_wakes_without_a_lost_notification() {
        let shared = shared();
        let route = claim_dag(&shared, 10);
        for _ in 0..MAX_ROUTE_EVENTS {
            assert!(
                shared
                    .scalable_routes
                    .publish(ScalableEvent::DagWatchClosed {
                        session_id: 10,
                        reason: None,
                    })
                    .is_none()
            );
        }
        assert!(
            shared
                .scalable_routes
                .publish(ScalableEvent::DagWatchClosed {
                    session_id: 10,
                    reason: None,
                })
                .is_none()
        );
        assert!(matches!(
            route.next().await,
            Err(ScalableRouteError::Overflow {
                capacity: MAX_ROUTE_EVENTS
            })
        ));
    }
}
