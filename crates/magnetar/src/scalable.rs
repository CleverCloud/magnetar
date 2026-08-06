// SPDX-License-Identifier: Apache-2.0

//! **Experimental** PIP-460 assignment-driven scalable stream consumption.
//!
//! A [`crate::scalable::StreamConsumer`] keeps the controller assignment and validated segment
//! DAG as its control plane while ordinary Exclusive consumers provide the data
//! plane. The runtime owns keyed controller and child routes for each aggregate;
//! high-level consumers never compete for the low-level client-global scalable
//! event queue.
//!
//! Delivery is ordered within a segment. [`crate::scalable::OrderingMode::Strict`] additionally
//! requires local proof that every transitive ancestor completed before a
//! descendant receives FLOW. [`crate::scalable::OrderingMode::BrokerManaged`] applies every
//! locally provable barrier but makes no cross-member ancestry guarantee.
//!
//! [`crate::scalable::StreamConsumer`] is cheap to clone. Every clone targets one shared
//! aggregate, so explicit close through any clone is globally definitive.
//! Dropping the final user guard performs synchronous best-effort fencing and
//! never blocks or spawns.

#![allow(clippy::doc_markdown)]

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

/// Parent-before-child policy. Strict local proof is the default.
pub use magnetar_proto::OrderingMode;
use magnetar_proto::schema::{Schema, SchemaError};
/// Validated aggregate receive budget and its errors.
pub use magnetar_proto::{BudgetError, ReceiverBudget};
/// Canonical segment identity and validated topology types.
pub use magnetar_proto::{DagSnapshot, SegmentDescriptor, SegmentId, SegmentSource, SegmentState};
/// Source-qualified serializable positions and process-local delivery authority.
pub use magnetar_proto::{DeliveryToken, PositionVector, StreamMessageId};

use crate::engine::{RawStreamMessage, StreamConsumerBackend, StreamConsumerOptions};
/// Low-level PIP-460 lookup/watch types. These are independent from the owned
/// high-level aggregate route.
pub use crate::engine::{ScalableEvent, ScalableLookup, ScalableTopicsApi, SegmentSubscriberApi};
use crate::{Engine, PulsarClient};

/// Default aggregate receive budget: 16 MiB including the fixed cleanup reserve.
pub const DEFAULT_RECEIVER_BUDGET_BYTES: usize = 16 * 1024 * 1024;

/// Invalid atomic batch-receive policy.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BatchReceivePolicyError {
    /// A batch must be able to return at least one message.
    #[error("batch receive max_messages must be greater than zero")]
    ZeroMessages,
    /// A batch must be able to retain at least one payload byte.
    #[error("batch receive max_bytes must be greater than zero")]
    ZeroBytes,
}

/// Count, retained-payload-byte, and first-message-wait bounds for one atomic
/// aggregate receive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_field_names)]
pub struct BatchReceivePolicy {
    max_messages: usize,
    max_bytes: usize,
    max_wait: Duration,
}

impl BatchReceivePolicy {
    /// Validate all three batch caps.
    ///
    /// # Errors
    ///
    /// Returns [`BatchReceivePolicyError`] if either capacity is zero.
    pub const fn new(
        max_messages: usize,
        max_bytes: usize,
        max_wait: Duration,
    ) -> Result<Self, BatchReceivePolicyError> {
        if max_messages == 0 {
            return Err(BatchReceivePolicyError::ZeroMessages);
        }
        if max_bytes == 0 {
            return Err(BatchReceivePolicyError::ZeroBytes);
        }
        Ok(Self {
            max_messages,
            max_bytes,
            max_wait,
        })
    }

    /// Build a count-and-wait policy without a practical byte cap.
    ///
    /// # Errors
    ///
    /// Returns [`BatchReceivePolicyError::ZeroMessages`] for zero capacity.
    pub const fn messages(
        max_messages: usize,
        max_wait: Duration,
    ) -> Result<Self, BatchReceivePolicyError> {
        Self::new(max_messages, usize::MAX, max_wait)
    }

    /// Maximum messages returned by one call.
    #[must_use]
    pub const fn max_messages(self) -> usize {
        self.max_messages
    }

    /// Maximum retained payload bytes returned by one call.
    #[must_use]
    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    /// Maximum wait for the first message. Once it arrives, the complete batch
    /// reservation is one atomic aggregate transition.
    #[must_use]
    pub const fn max_wait(self) -> Duration {
        self.max_wait
    }
}

/// One failed component of a fan-out position acknowledgement.
#[derive(Debug)]
pub struct StreamAckFailure {
    position: StreamMessageId,
    error: StreamAckFailureReason,
}

impl StreamAckFailure {
    /// Component that failed.
    #[must_use]
    pub const fn position(&self) -> &StreamMessageId {
        &self.position
    }

    /// Typed component failure.
    #[must_use]
    pub const fn error(&self) -> &StreamAckFailureReason {
        &self.error
    }

    /// Construct a component failure from a proto-model error.
    #[doc(hidden)]
    #[must_use]
    pub fn model(
        position: StreamMessageId,
        error: magnetar_proto::StreamConsumerModelError,
    ) -> Self {
        Self {
            position,
            error: StreamAckFailureReason::Model(error),
        }
    }

    /// Construct a component failure from an engine operation.
    #[doc(hidden)]
    #[must_use]
    pub fn engine(
        position: StreamMessageId,
        engine: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self {
            position,
            error: StreamAckFailureReason::Engine {
                engine,
                message: message.into(),
            },
        }
    }
}

/// Why one source-qualified acknowledgement component failed.
#[derive(Debug, thiserror::Error)]
pub enum StreamAckFailureReason {
    /// Authority, generation, DAG, budget, or lifecycle failure from the pure
    /// aggregate model.
    #[error(transparent)]
    Model(#[from] magnetar_proto::StreamConsumerModelError),
    /// Runtime route or broker operation failure.
    #[error("{engine} engine error: {message}")]
    Engine {
        /// Runtime engine name.
        engine: &'static str,
        /// Runtime diagnostic without credential material.
        message: String,
    },
}

/// Typed failures surfaced by the experimental aggregate API.
#[derive(Debug, thiserror::Error)]
pub enum StreamConsumerError {
    /// `.subscription(...)` was omitted.
    #[error("a scalable stream consumer requires a subscription")]
    MissingSubscription,
    /// Subscription names cannot be empty.
    #[error("scalable stream consumer subscription cannot be empty")]
    EmptySubscription,
    /// Explicit consumer names cannot be empty.
    #[error("scalable stream consumer name cannot be empty")]
    EmptyConsumerName,
    /// Batch policy is invalid.
    #[error(transparent)]
    BatchPolicy(#[from] BatchReceivePolicyError),
    /// Receiver-budget validation or accounting failed.
    #[error(transparent)]
    Budget(#[from] magnetar_proto::BudgetError),
    /// Aggregate authority, lifecycle, DAG, assignment, seek, or ordering
    /// transition failed in the pure proto model.
    #[error(transparent)]
    Model(#[from] magnetar_proto::StreamConsumerModelError),
    /// Canonical `MSTR` position validation failed.
    #[error(transparent)]
    Position(#[from] magnetar_proto::StreamPositionError),
    /// Schema metadata, decoding, or owned-value construction failed.
    #[error(transparent)]
    Schema(#[from] SchemaError),
    /// Aggregate transaction admission/lifecycle failed.
    #[error(transparent)]
    Transaction(#[from] magnetar_proto::AggregateTransactionError),
    /// The transaction was not opened through this facade client.
    #[error("transaction {txn_id} is unknown to this client")]
    UnknownTransaction {
        /// Foreign or already-finalized transaction id.
        txn_id: magnetar_proto::TxnId,
    },
    /// Commit admission closed while an aggregate registration or ack failed.
    /// No commit command was issued; abort remains possible.
    #[error("transaction {txn_id} is poisoned by a failed aggregate acknowledgement")]
    TransactionPoisoned {
        /// Poisoned transaction.
        txn_id: magnetar_proto::TxnId,
    },
    /// Another caller already owns commit or abort for this transaction.
    #[error("transaction {txn_id} is already ending")]
    TransactionAlreadyEnding {
        /// Transaction being ended.
        txn_id: magnetar_proto::TxnId,
    },
    /// A cumulative/vector acknowledgement partially succeeded. Confirmed
    /// components are durable and retrying the failed components is idempotent.
    #[error("position acknowledgement partially failed")]
    PartialAcknowledgement {
        /// Components confirmed by their ordinary child consumers.
        confirmed: Vec<StreamMessageId>,
        /// Components that failed.
        failed: Vec<StreamAckFailure>,
    },
    /// Runtime route, child, or broker operation failed without a more precise
    /// proto-model classification.
    #[error("{engine} stream-consumer error: {message}")]
    Engine {
        /// Runtime engine name.
        engine: &'static str,
        /// Secret-free runtime diagnostic.
        message: String,
    },
}

impl StreamConsumerError {
    /// Map a runtime-specific error without erasing typed proto-model variants
    /// that adapters can map directly through the other constructors.
    #[doc(hidden)]
    #[must_use]
    pub fn engine(engine: &'static str, error: impl std::fmt::Display) -> Self {
        Self::Engine {
            engine,
            message: error.to_string(),
        }
    }
}

/// Aggregate transaction result propagated to every participating consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionOutcome {
    /// The coordinator confirmed commit; staged positions may now advance.
    Committed,
    /// The coordinator confirmed abort; staged positions remain uncommitted.
    Aborted,
    /// The coordinator outcome could not be established; the participant must
    /// fail closed and resynchronize.
    Unknown,
}

/// Current aggregate lifecycle and bounded-resource snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamConsumerStatus {
    phase: magnetar_proto::AggregatePhase,
    layout_epoch: Option<u64>,
    assigned_segments: usize,
    attached_segments: usize,
    draining_segments: usize,
    pending_ownership: Vec<magnetar_proto::SegmentSource>,
    ordering_unprovable: Vec<SegmentId>,
    receiver_budget_limit: usize,
    receiver_budget_used: usize,
}

impl StreamConsumerStatus {
    /// Construct a status snapshot in a runtime adapter.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        phase: magnetar_proto::AggregatePhase,
        layout_epoch: Option<u64>,
        assigned_segments: usize,
        attached_segments: usize,
        draining_segments: usize,
        pending_ownership: Vec<magnetar_proto::SegmentSource>,
        ordering_unprovable: Vec<SegmentId>,
        receiver_budget_limit: usize,
        receiver_budget_used: usize,
    ) -> Self {
        Self {
            phase,
            layout_epoch,
            assigned_segments,
            attached_segments,
            draining_segments,
            pending_ownership,
            ordering_unprovable,
            receiver_budget_limit,
            receiver_budget_used,
        }
    }

    /// Aggregate lifecycle.
    #[must_use]
    pub const fn phase(&self) -> magnetar_proto::AggregatePhase {
        self.phase
    }

    /// Current validated layout epoch, or `None` while reconnecting before a
    /// replacement baseline.
    #[must_use]
    pub const fn layout_epoch(&self) -> Option<u64> {
        self.layout_epoch
    }

    /// Segments in the current authoritative assignment.
    #[must_use]
    pub const fn assigned_segments(&self) -> usize {
        self.assigned_segments
    }

    /// Ordinary child consumers currently attached.
    #[must_use]
    pub const fn attached_segments(&self) -> usize {
        self.attached_segments
    }

    /// Lost children retained only as acknowledgement/close targets.
    #[must_use]
    pub const fn draining_segments(&self) -> usize {
        self.draining_segments
    }

    /// Gained sources waiting for an old `Exclusive` child to release
    /// ownership.
    #[must_use]
    pub fn pending_ownership(&self) -> &[magnetar_proto::SegmentSource] {
        &self.pending_ownership
    }

    /// Assigned descendants currently blocked because strict ancestry cannot
    /// be proved locally.
    #[must_use]
    pub fn ordering_unprovable(&self) -> &[SegmentId] {
        &self.ordering_unprovable
    }

    /// Configured aggregate bytes, including cleanup reserve.
    #[must_use]
    pub const fn receiver_budget_limit(&self) -> usize {
        self.receiver_budget_limit
    }

    /// Bytes currently retained or reserved by the aggregate data plane.
    #[must_use]
    pub const fn receiver_budget_used(&self) -> usize {
        self.receiver_budget_used
    }
}

/// Observable event from one owned aggregate route.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StreamConsumerEvent {
    /// A full controller assignment became authoritative.
    AssignmentApplied {
        /// Layout epoch used to validate the assignment.
        layout_epoch: u64,
        /// Current assigned sources in deterministic segment order.
        sources: Vec<SegmentSource>,
    },
    /// One ordinary child changed lifecycle phase.
    SegmentPhaseChanged {
        /// Child source.
        source: SegmentSource,
        /// Current phase.
        phase: magnetar_proto::SegmentPhase,
    },
    /// Strict mode cannot prove remote/pruned ancestry complete.
    OrderingUnprovable {
        /// Blocked descendant.
        segment_id: SegmentId,
        /// Ancestors preventing local proof.
        ancestors: Vec<SegmentId>,
    },
    /// Aggregate authority was fenced and a fresh controller baseline is
    /// required.
    ResyncRequired {
        /// Secret-free diagnostic.
        reason: String,
    },
    /// A participating transaction reached a final or unknown outcome.
    TransactionOutcome {
        /// Pulsar transaction id.
        txn_id: magnetar_proto::TxnId,
        /// Propagated result.
        outcome: TransactionOutcome,
    },
    /// Explicit or final-drop cleanup made the aggregate locally definitive.
    Closed,
}

/// One schema-decoded aggregate delivery.
///
/// The value is owned. Neither the schema, `S::Owned`, the message, nor the
/// runtime client needs `Clone`.
pub struct StreamMessage<S: Schema> {
    value: S::Owned,
    raw: magnetar_proto::IncomingMessage,
    token: DeliveryToken,
}

impl<S: Schema> std::fmt::Debug for StreamMessage<S>
where
    S::Owned: std::fmt::Debug,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamMessage")
            .field("value", &self.value)
            .field("source", self.token.stream_message_id().source())
            .field("message_id", self.token.stream_message_id())
            .field("payload_len", &self.raw.payload.len())
            .finish_non_exhaustive()
    }
}

impl<S: Schema> StreamMessage<S> {
    /// Decoded owned value.
    #[must_use]
    pub const fn value(&self) -> &S::Owned {
        &self.value
    }

    /// Canonical segment source.
    #[must_use]
    pub fn source(&self) -> &SegmentSource {
        self.token.stream_message_id().source()
    }

    /// Serializable source-qualified message position.
    #[must_use]
    pub const fn message_id(&self) -> &StreamMessageId {
        self.token.stream_message_id()
    }

    /// Delivered-position vector at this message's dequeue linearization point.
    #[must_use]
    pub const fn position(&self) -> &PositionVector {
        self.token.position_vector()
    }

    /// Process-local acknowledgement authority. It cannot be serialized or
    /// reconstructed from [`Self::message_id`] or [`Self::position`].
    #[must_use]
    pub const fn delivery_token(&self) -> &DeliveryToken {
        &self.token
    }

    /// Raw ordinary child message, including payload and broker metadata.
    #[must_use]
    pub const fn raw(&self) -> &magnetar_proto::IncomingMessage {
        &self.raw
    }

    /// Post-decryption and post-decompression payload bytes decoded by the
    /// retained schema instance.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.raw.payload
    }

    /// Consume the message without requiring any component to implement
    /// `Clone`. The caller retains the live token and can keep acknowledging
    /// through runtime-specific tooling if it deliberately dismantles the
    /// high-level message.
    #[must_use]
    pub fn into_parts(self) -> (S::Owned, magnetar_proto::IncomingMessage, DeliveryToken) {
        (self.value, self.raw, self.token)
    }
}

struct BackendTransactionParticipant {
    backend: Arc<dyn StreamConsumerBackend>,
}

impl crate::transaction::TransactionParticipant for BackendTransactionParticipant {
    fn transaction_outcome(
        &self,
        txn_id: magnetar_proto::TxnId,
        outcome: TransactionOutcome,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), StreamConsumerError>> + Send + '_>,
    > {
        self.backend.transaction_outcome(txn_id, outcome)
    }
}

struct StreamConsumerShared<S: Schema> {
    backend: Arc<dyn StreamConsumerBackend>,
    transaction_participant: Arc<BackendTransactionParticipant>,
    schema: Arc<S>,
    topic: String,
    subscription: String,
    consumer_name: String,
    transactions: Arc<crate::transaction::TransactionCoordinator>,
    participant_id: u64,
}

struct RawDeliveryLease {
    backend: Arc<dyn StreamConsumerBackend>,
    messages: Vec<RawStreamMessage>,
    armed: bool,
}

impl RawDeliveryLease {
    fn new(backend: Arc<dyn StreamConsumerBackend>, messages: Vec<RawStreamMessage>) -> Self {
        Self {
            backend,
            messages,
            armed: true,
        }
    }

    fn into_messages(mut self) -> Vec<RawStreamMessage> {
        self.armed = false;
        core::mem::take(&mut self.messages)
    }

    fn negative_acknowledge(&mut self) -> Result<(), StreamConsumerError> {
        for message in &self.messages {
            self.backend.negative_acknowledge(&message.token)?;
        }
        self.armed = false;
        Ok(())
    }
}

impl Drop for RawDeliveryLease {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.backend
            .restore_messages(core::mem::take(&mut self.messages));
    }
}

impl<S: Schema> std::fmt::Debug for StreamConsumerShared<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamConsumerShared")
            .field("topic", &self.topic)
            .field("subscription", &self.subscription)
            .field("consumer_name", &self.consumer_name)
            .field("status", &self.backend.status())
            .finish_non_exhaustive()
    }
}

impl<S: Schema> Drop for StreamConsumerShared<S> {
    fn drop(&mut self) {
        self.backend.close_best_effort();
    }
}

/// Assignment-driven aggregate over a PIP-460 scalable topic.
///
/// Cloning this value clones one `Arc`; it does not clone a schema, payload, or
/// runtime client. All clones share receive reservations, authority, status,
/// events, transactions, and close state.
pub struct StreamConsumer<S: Schema, E: Engine = crate::TokioEngine> {
    shared: Arc<StreamConsumerShared<S>>,
    _engine: PhantomData<fn() -> E>,
}

impl<S: Schema, E: Engine> Clone for StreamConsumer<S, E> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            _engine: PhantomData,
        }
    }
}

impl<S: Schema, E: Engine> std::fmt::Debug for StreamConsumer<S, E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamConsumer")
            .field("topic", &self.shared.topic)
            .field("subscription", &self.shared.subscription)
            .field("consumer_name", &self.shared.consumer_name)
            .field("status", &self.shared.backend.status())
            .finish_non_exhaustive()
    }
}

impl<S: Schema, E: Engine> StreamConsumer<S, E> {
    /// Scalable parent topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.shared.topic
    }

    /// Subscription shared by every ordinary Exclusive child.
    #[must_use]
    pub fn subscription(&self) -> &str {
        &self.shared.subscription
    }

    /// Stable aggregate consumer name.
    #[must_use]
    pub fn consumer_name(&self) -> &str {
        &self.shared.consumer_name
    }

    /// Borrow the retained schema/decode state.
    #[must_use]
    pub fn schema(&self) -> &Arc<S> {
        &self.shared.schema
    }

    /// Concurrently await and reserve one message.
    ///
    /// Cancellation during schema preparation restores the reserved delivery
    /// locally; an explicit schema or decode failure negatively acknowledges it.
    pub async fn receive(&self) -> Result<StreamMessage<S>, StreamConsumerError> {
        let raw = self.shared.backend.receive().await?;
        self.decode_one(raw).await
    }

    /// Concurrently receive one atomic batch.
    ///
    /// Once the first message arrives, the backend reserves the complete batch
    /// in one aggregate transition. If schema resolution or any decode fails,
    /// every reserved member is negatively acknowledged and no partial batch is
    /// returned.
    pub async fn receive_batch(
        &self,
        policy: BatchReceivePolicy,
    ) -> Result<Vec<StreamMessage<S>>, StreamConsumerError> {
        let raw = self.shared.backend.receive_batch(policy).await?;
        self.decode_batch(raw).await
    }

    /// Individually acknowledge one live delivery.
    pub async fn acknowledge(&self, message: &StreamMessage<S>) -> Result<(), StreamConsumerError> {
        self.shared
            .backend
            .acknowledge(message.delivery_token())
            .await
    }

    /// Cumulatively acknowledge the delivery's complete position vector.
    pub async fn acknowledge_cumulative(
        &self,
        message: &StreamMessage<S>,
    ) -> Result<(), StreamConsumerError> {
        self.shared
            .backend
            .acknowledge_cumulative(message.delivery_token())
            .await
    }

    /// Acknowledge a restored serialized position vector after current
    /// assignment/layout/generation validation.
    pub async fn acknowledge_positions(
        &self,
        positions: &PositionVector,
    ) -> Result<(), StreamConsumerError> {
        self.shared.backend.acknowledge_positions(positions).await
    }

    /// Validate and acknowledge a group of live deliveries. Runtime fan-out
    /// reports confirmed and failed source components explicitly.
    pub async fn acknowledge_batch(
        &self,
        messages: &[StreamMessage<S>],
    ) -> Result<(), StreamConsumerError> {
        let tokens = messages.iter().map(StreamMessage::delivery_token).collect();
        self.shared.backend.acknowledge_batch(tokens).await
    }

    /// Negatively acknowledge one live delivery using the configured/default
    /// delay.
    pub fn negative_acknowledge(
        &self,
        message: &StreamMessage<S>,
    ) -> Result<(), StreamConsumerError> {
        self.shared
            .backend
            .negative_acknowledge(message.delivery_token())
    }

    /// Acknowledge one delivery inside a Pulsar transaction.
    ///
    /// Admission is registered before any runtime I/O. Cancellation or failure
    /// poisons commit; abort remains available.
    pub async fn acknowledge_in_transaction(
        &self,
        message: &StreamMessage<S>,
        transaction: crate::Transaction,
    ) -> Result<(), StreamConsumerError> {
        let participant: Arc<dyn crate::transaction::TransactionParticipant> =
            self.shared.transaction_participant.clone();
        let admission = self.shared.transactions.admit(
            transaction.id(),
            self.shared.participant_id,
            Arc::downgrade(&participant),
        )?;
        let result = self
            .shared
            .backend
            .acknowledge_in_transaction(message.delivery_token(), transaction.id())
            .await;
        admission.finish(result.is_ok())?;
        result
    }

    /// Cumulatively acknowledge a live delivery's position vector inside a
    /// Pulsar transaction.
    pub async fn acknowledge_cumulative_in_transaction(
        &self,
        message: &StreamMessage<S>,
        transaction: crate::Transaction,
    ) -> Result<(), StreamConsumerError> {
        let participant: Arc<dyn crate::transaction::TransactionParticipant> =
            self.shared.transaction_participant.clone();
        let admission = self.shared.transactions.admit(
            transaction.id(),
            self.shared.participant_id,
            Arc::downgrade(&participant),
        )?;
        let result = self
            .shared
            .backend
            .acknowledge_cumulative_in_transaction(message.delivery_token(), transaction.id())
            .await;
        admission.finish(result.is_ok())?;
        result
    }

    /// Acknowledge a restored position vector inside a Pulsar transaction.
    pub async fn acknowledge_positions_in_transaction(
        &self,
        positions: &PositionVector,
        transaction: crate::Transaction,
    ) -> Result<(), StreamConsumerError> {
        let participant: Arc<dyn crate::transaction::TransactionParticipant> =
            self.shared.transaction_participant.clone();
        let admission = self.shared.transactions.admit(
            transaction.id(),
            self.shared.participant_id,
            Arc::downgrade(&participant),
        )?;
        let result = self
            .shared
            .backend
            .acknowledge_positions_in_transaction(positions, transaction.id())
            .await;
        admission.finish(result.is_ok())?;
        result
    }

    /// Highest position delivered to the application for every represented
    /// segment. This is not an acknowledged cursor or durable checkpoint.
    #[must_use]
    pub fn delivered_position(&self) -> PositionVector {
        self.shared.backend.delivered_position()
    }

    /// Apply the M1-limited vector seek across exactly the current assigned
    /// active leaves.
    pub async fn seek_positions(
        &self,
        positions: &PositionVector,
    ) -> Result<(), StreamConsumerError> {
        self.shared.backend.seek_positions(positions).await
    }

    /// Snapshot aggregate lifecycle, ownership, ordering, and receive-budget
    /// state.
    #[must_use]
    pub fn status(&self) -> StreamConsumerStatus {
        self.shared.backend.status()
    }

    /// Await the next event from this aggregate's owned typed route.
    pub async fn next_event(&self) -> Result<Option<StreamConsumerEvent>, StreamConsumerError> {
        self.shared.backend.next_event().await
    }

    /// Globally close the aggregate through every clone and await typed-route,
    /// task, and ordinary child cleanup.
    pub async fn close(self) -> Result<(), StreamConsumerError> {
        self.shared.backend.close().await
    }

    async fn decode_one(
        &self,
        raw: RawStreamMessage,
    ) -> Result<StreamMessage<S>, StreamConsumerError> {
        let mut lease = RawDeliveryLease::new(self.shared.backend.clone(), vec![raw]);
        let raw = &lease.messages[0];
        if let Err(error) = self.prepare_schema(raw).await {
            lease.negative_acknowledge()?;
            return Err(error);
        }
        let value = match self.shared.schema.decode(&raw.message.payload) {
            Ok(value) => value,
            Err(error) => {
                lease.negative_acknowledge()?;
                return Err(error.into());
            }
        };
        let mut messages = lease.into_messages();
        let raw = messages.swap_remove(0);
        Ok(StreamMessage {
            value,
            raw: raw.message,
            token: raw.token,
        })
    }

    async fn decode_batch(
        &self,
        raw: Vec<RawStreamMessage>,
    ) -> Result<Vec<StreamMessage<S>>, StreamConsumerError> {
        let mut lease = RawDeliveryLease::new(self.shared.backend.clone(), raw);
        let mut values = Vec::with_capacity(lease.messages.len());
        for message in &lease.messages {
            if let Err(error) = self.prepare_schema(message).await {
                lease.negative_acknowledge()?;
                return Err(error);
            }
            match self.shared.schema.decode(&message.message.payload) {
                Ok(value) => values.push(value),
                Err(error) => {
                    lease.negative_acknowledge()?;
                    return Err(error.into());
                }
            }
        }
        Ok(lease
            .into_messages()
            .into_iter()
            .zip(values)
            .map(|(raw, value)| StreamMessage {
                value,
                raw: raw.message,
                token: raw.token,
            })
            .collect())
    }

    async fn prepare_schema(&self, raw: &RawStreamMessage) -> Result<(), StreamConsumerError> {
        if self.shared.schema.needs_broker_schema() {
            let resolved = self
                .shared
                .backend
                .get_schema(raw.token.stream_message_id().source(), None)
                .await?;
            self.shared.schema.store_resolved_schema(resolved);
        }
        Ok(())
    }
}

/// Assignment-driven scalable stream-consumer builder.
///
/// A subscription is required. Consumer id allocation, controller incarnation,
/// and ordinary child ids remain runtime-owned; every child is Exclusive.
pub struct StreamConsumerBuilder<'a, S: Schema, E: Engine = crate::TokioEngine> {
    client: &'a PulsarClient<E>,
    topic: String,
    schema: Arc<S>,
    subscription: Option<String>,
    consumer_name: Option<String>,
    receiver_budget: ReceiverBudget,
    ordering_mode: OrderingMode,
}

impl<S: Schema, E: Engine> std::fmt::Debug for StreamConsumerBuilder<'_, S, E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamConsumerBuilder")
            .field("topic", &self.topic)
            .field("schema_type", &self.schema.schema_type())
            .field("subscription", &self.subscription)
            .field("consumer_name", &self.consumer_name)
            .field("receiver_budget", &self.receiver_budget)
            .field("ordering_mode", &self.ordering_mode)
            .finish_non_exhaustive()
    }
}

impl<'a, S: Schema, E: Engine> StreamConsumerBuilder<'a, S, E>
where
    E::ClientState: SegmentSubscriberApi,
{
    fn new(client: &'a PulsarClient<E>, topic: String, schema: Arc<S>) -> Self {
        let receiver_budget = ReceiverBudget::bytes(DEFAULT_RECEIVER_BUDGET_BYTES)
            .expect("the fixed default stream receiver budget must remain valid");
        Self {
            client,
            topic,
            schema,
            subscription: None,
            consumer_name: None,
            receiver_budget,
            ordering_mode: OrderingMode::Strict,
        }
    }

    /// Set the subscription shared by every ordinary Exclusive child.
    #[must_use]
    pub fn subscription(mut self, subscription: impl Into<String>) -> Self {
        self.subscription = Some(subscription.into());
        self
    }

    /// Override the stable aggregate consumer name. Child names are derived as
    /// `<consumer-name>-seg-<segment-id>`.
    #[must_use]
    pub fn consumer_name(mut self, consumer_name: impl Into<String>) -> Self {
        self.consumer_name = Some(consumer_name.into());
        self
    }

    /// Set one validated aggregate receive budget. Adding assigned segments
    /// redistributes this capacity and never multiplies it.
    #[must_use]
    pub const fn receiver_budget(mut self, receiver_budget: ReceiverBudget) -> Self {
        self.receiver_budget = receiver_budget;
        self
    }

    /// Select parent-before-child behavior. Defaults to
    /// [`OrderingMode::Strict`]; broker-managed ancestry must be explicit.
    #[must_use]
    pub const fn ordering_mode(mut self, ordering_mode: OrderingMode) -> Self {
        self.ordering_mode = ordering_mode;
        self
    }

    /// Register with the controller and open the initial assigned segment
    /// children through an owned runtime capability.
    ///
    /// # Errors
    ///
    /// Returns [`StreamConsumerError::MissingSubscription`] when
    /// [`Self::subscription`] was omitted, a typed configuration/model error,
    /// or the runtime adapter's controller/child error.
    pub async fn subscribe(self) -> Result<StreamConsumer<S, E>, StreamConsumerError> {
        let subscription = self
            .subscription
            .ok_or(StreamConsumerError::MissingSubscription)?;
        if subscription.is_empty() {
            return Err(StreamConsumerError::EmptySubscription);
        }
        if self.consumer_name.as_deref() == Some("") {
            return Err(StreamConsumerError::EmptyConsumerName);
        }
        let consumer_name = self.consumer_name.unwrap_or_else(|| {
            format!(
                "magnetar-stream-{}",
                <E as Engine>::random_subscription_suffix()
            )
        });
        let schema = magnetar_proto::pb::Schema {
            name: self.topic.clone(),
            schema_data: self.schema.schema_data(),
            r#type: self.schema.schema_type() as i32,
            properties: self
                .schema
                .properties()
                .into_iter()
                .map(|(key, value)| magnetar_proto::pb::KeyValue { key, value })
                .collect(),
        };
        let options = StreamConsumerOptions {
            topic: self.topic.clone(),
            subscription: subscription.clone(),
            consumer_name: consumer_name.clone(),
            schema,
            receiver_budget: self.receiver_budget,
            ordering_mode: self.ordering_mode,
        };
        let backend = crate::engine::SegmentSubscriberApi::subscribe_stream_consumer(
            &self.client.inner,
            options,
        )
        .await?;
        let backend: Arc<dyn StreamConsumerBackend> = Arc::new(backend);
        let transaction_participant = Arc::new(BackendTransactionParticipant {
            backend: backend.clone(),
        });
        let participant_id = self.client.transactions.next_participant_id();
        Ok(StreamConsumer {
            shared: Arc::new(StreamConsumerShared {
                backend,
                transaction_participant,
                schema: self.schema,
                topic: self.topic,
                subscription,
                consumer_name,
                transactions: self.client.transactions.clone(),
                participant_id,
            }),
            _engine: PhantomData,
        })
    }
}

impl<E: Engine> PulsarClient<E>
where
    E::ClientState: SegmentSubscriberApi,
{
    /// Start building an assignment-driven PIP-460 aggregate.
    ///
    /// This call only borrows the public client and never requires its runtime
    /// state to implement `Clone`. The schema `Arc` is retained for child wire
    /// metadata and decode state.
    #[must_use]
    pub fn scalable_stream_consumer<S: Schema>(
        &self,
        topic: impl Into<String>,
        schema: Arc<S>,
    ) -> StreamConsumerBuilder<'_, S, E> {
        StreamConsumerBuilder::new(self, topic.into(), schema)
    }
}

// The raw PIP-460 lookup/watch surface remains available for diagnostics and
// tooling. Owned high-level consumers above never call `next_scalable_event`.
impl<E: Engine> PulsarClient<E>
where
    E::ClientState: ScalableTopicsApi,
{
    /// Register a raw scalable consumer session and await its initial
    /// assignment. This low-level hook is intended for protocol tooling; normal
    /// applications use [`Self::scalable_stream_consumer`], whose ids and typed
    /// routes are runtime-owned.
    #[doc(hidden)]
    pub async fn scalable_topic_subscribe(
        &self,
        topic: &str,
        subscription: &str,
        consumer_name: &str,
        consumer_id: u64,
        consumer_type: magnetar_proto::ScalableConsumerType,
    ) -> Result<magnetar_proto::ConsumerAssignment, <E::ClientState as ScalableTopicsApi>::Error>
    {
        self.inner
            .scalable_topic_subscribe(
                topic,
                subscription,
                consumer_name,
                consumer_id,
                consumer_type,
            )
            .await
    }

    /// Await the next unclaimed low-level event. Events owned by a high-level
    /// aggregate route are never duplicated here.
    pub async fn next_scalable_event(&self) -> Option<ScalableEvent> {
        self.inner.next_scalable_event().await
    }

    /// Close a low-level scalable DAG session.
    pub fn close_scalable_topic_session(&self, session_id: u64) {
        self.inner.close_scalable_topic_session(session_id);
    }

    /// Whether the connected broker advertised PIP-460.
    #[must_use]
    pub fn broker_supports_scalable_topics(&self) -> bool {
        self.inner.broker_supports_scalable_topics()
    }

    /// Open a namespace-level low-level scalable-topic watch.
    pub fn watch_scalable_topics(
        &self,
        namespace: &str,
        property_filters: Vec<(String, String)>,
    ) -> Result<u64, <E::ClientState as ScalableTopicsApi>::Error> {
        self.inner
            .watch_scalable_topics(namespace, property_filters)
    }

    /// Close a namespace-level low-level watch.
    pub fn close_scalable_topics_watch(&self, watch_id: u64) {
        self.inner.close_scalable_topics_watch(watch_id);
    }

    /// Current matching topic set for a low-level namespace watch.
    #[must_use]
    pub fn scalable_topics_snapshot(&self, watch_id: u64) -> Option<Vec<String>> {
        self.inner.scalable_topics_snapshot(watch_id)
    }

    /// Whether the broker advertised metadata-driven transaction-coordinator
    /// discovery.
    #[must_use]
    pub fn broker_supports_tc_metadata_discovery(&self) -> bool {
        self.inner.broker_supports_tc_metadata_discovery()
    }

    /// Open a low-level transaction-coordinator assignment watch.
    pub fn watch_tc_assignments(
        &self,
    ) -> Result<u64, <E::ClientState as ScalableTopicsApi>::Error> {
        self.inner.watch_tc_assignments()
    }

    /// Close a transaction-coordinator assignment watch.
    pub fn close_tc_assignments_watch(&self, watch_id: u64) {
        self.inner.close_tc_assignments_watch(watch_id);
    }

    /// Resolve and retain a low-level scalable DAG session.
    pub async fn lookup_scalable_topic(
        &self,
        topic: &str,
    ) -> Result<ScalableLookup, <E::ClientState as ScalableTopicsApi>::Error> {
        self.inner.scalable_topic_lookup(topic).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::Poll;

    use bytes::Bytes;
    use magnetar_proto::schema::Schema;

    use super::*;
    use crate::engine::{MessageDecryptorApi, MessageEncryptorApi, NoEncryption};

    #[derive(Debug, thiserror::Error)]
    #[error("fake runtime failure: {0}")]
    struct FakeRuntimeError(&'static str);

    #[derive(Debug)]
    struct FakeEngine;

    impl MessageEncryptorApi for FakeEngine {
        type Encryptor = NoEncryption;
    }

    impl MessageDecryptorApi for FakeEngine {
        type Decryptor = NoEncryption;
    }

    impl Engine for FakeEngine {
        type ClientState = FakeClient;
        type TaskHandle = ();
        type Interval = ();

        fn name() -> &'static str {
            "fake"
        }

        fn spawn<F>(future: F) -> Self::TaskHandle
        where
            F: Future<Output = ()> + Send + 'static,
        {
            drop(future);
        }

        fn abort_task(_handle: &mut Self::TaskHandle) {}

        fn new_interval(_period: Duration) -> Self::Interval {}

        fn interval_tick<'a>(
            _interval: &'a mut Self::Interval,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async {})
        }

        fn random_subscription_suffix() -> String {
            "stable-fake-id".to_owned()
        }
    }

    #[derive(Debug, Default)]
    struct FakeTransactionState {
        end_calls: AtomicUsize,
    }

    #[derive(Debug)]
    struct FakeClient {
        backend: FakeBackend,
        options: Arc<parking_lot::Mutex<Option<StreamConsumerOptions>>>,
        transactions: Arc<FakeTransactionState>,
    }

    impl crate::TransactionApi for FakeClient {
        type Error = FakeRuntimeError;

        fn new_txn(
            &self,
            _timeout: Duration,
        ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::TxnId, Self::Error>> + Send + '_>>
        {
            Box::pin(async { Ok(magnetar_proto::TxnId::new(9, 4)) })
        }

        fn add_partition_to_txn(
            &self,
            _txn: magnetar_proto::TxnId,
            _topic: String,
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn add_subscription_to_txn(
            &self,
            _txn: magnetar_proto::TxnId,
            _topic: String,
            _subscription: String,
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn end_txn(
            &self,
            _txn: magnetar_proto::TxnId,
            action: magnetar_proto::TxnAction,
        ) -> Pin<Box<dyn Future<Output = Result<magnetar_proto::TxnState, Self::Error>> + Send + '_>>
        {
            self.transactions.end_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(match action {
                    magnetar_proto::TxnAction::Commit => magnetar_proto::TxnState::Committed,
                    magnetar_proto::TxnAction::Abort => magnetar_proto::TxnState::Aborted,
                })
            })
        }
    }

    impl SegmentSubscriberApi for FakeClient {
        type StreamConsumer = FakeBackend;

        fn subscribe_stream_consumer(
            &self,
            options: StreamConsumerOptions,
        ) -> Pin<
            Box<dyn Future<Output = Result<Self::StreamConsumer, StreamConsumerError>> + Send + '_>,
        > {
            *self.options.lock() = Some(options);
            let backend = self.backend.clone();
            Box::pin(async move { Ok(backend) })
        }
    }

    struct FakeBackendState {
        messages: parking_lot::Mutex<VecDeque<RawStreamMessage>>,
        model: parking_lot::Mutex<Option<magnetar_proto::StreamConsumerModel>>,
        delivered: parking_lot::Mutex<PositionVector>,
        events: parking_lot::Mutex<VecDeque<StreamConsumerEvent>>,
        closed: AtomicBool,
        close_calls: AtomicUsize,
        best_effort_calls: AtomicUsize,
        transaction_ack_blocked: AtomicBool,
        transaction_ack_fails: AtomicBool,
        transaction_ack_started: tokio::sync::Notify,
        transaction_ack_release: tokio::sync::Notify,
        transaction_outcome_blocked: AtomicBool,
        transaction_outcome_release: tokio::sync::Notify,
        transaction_outcome_calls: AtomicUsize,
        transaction_outcomes: parking_lot::Mutex<Vec<(magnetar_proto::TxnId, TransactionOutcome)>>,
        schema_blocked: AtomicBool,
        schema_started: tokio::sync::Notify,
        schema_release: tokio::sync::Notify,
        negative_ack_calls: AtomicUsize,
        restore_calls: AtomicUsize,
    }

    #[derive(Clone)]
    struct FakeBackend {
        state: Arc<FakeBackendState>,
    }

    impl std::fmt::Debug for FakeBackend {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("FakeBackend")
                .finish_non_exhaustive()
        }
    }

    impl FakeBackend {
        fn empty() -> Self {
            let delivered = PositionVector::new(1, []).expect("empty position vector");
            Self {
                state: Arc::new(FakeBackendState {
                    messages: parking_lot::Mutex::new(VecDeque::new()),
                    model: parking_lot::Mutex::new(None),
                    delivered: parking_lot::Mutex::new(delivered),
                    events: parking_lot::Mutex::new(VecDeque::new()),
                    closed: AtomicBool::new(false),
                    close_calls: AtomicUsize::new(0),
                    best_effort_calls: AtomicUsize::new(0),
                    transaction_ack_blocked: AtomicBool::new(false),
                    transaction_ack_fails: AtomicBool::new(false),
                    transaction_ack_started: tokio::sync::Notify::new(),
                    transaction_ack_release: tokio::sync::Notify::new(),
                    transaction_outcome_blocked: AtomicBool::new(false),
                    transaction_outcome_release: tokio::sync::Notify::new(),
                    transaction_outcome_calls: AtomicUsize::new(0),
                    transaction_outcomes: parking_lot::Mutex::new(Vec::new()),
                    schema_blocked: AtomicBool::new(false),
                    schema_started: tokio::sync::Notify::new(),
                    schema_release: tokio::sync::Notify::new(),
                    negative_ack_calls: AtomicUsize::new(0),
                    restore_calls: AtomicUsize::new(0),
                }),
            }
        }

        fn with_delivery(payload: &[u8], instance: u64) -> Self {
            let backend = Self::empty();
            let (model, message) = delivery(payload, instance);
            *backend.state.model.lock() = Some(model);
            *backend.state.delivered.lock() = message.token.position_vector().clone();
            backend.state.messages.lock().push_back(message);
            backend
        }

        fn resolve(&self, token: &DeliveryToken) -> Result<(), StreamConsumerError> {
            self.state
                .model
                .lock()
                .as_mut()
                .ok_or_else(|| StreamConsumerError::engine("fake", "no aggregate model"))?
                .resolve_delivery(token)?;
            Ok(())
        }

        async fn transactional_ack(&self) -> Result<(), StreamConsumerError> {
            self.state.transaction_ack_started.notify_one();
            if self.state.transaction_ack_blocked.load(Ordering::SeqCst) {
                self.state.transaction_ack_release.notified().await;
            }
            if self.state.transaction_ack_fails.load(Ordering::SeqCst) {
                Err(StreamConsumerError::engine(
                    "fake",
                    "transaction ack failed",
                ))
            } else {
                Ok(())
            }
        }
    }

    impl StreamConsumerBackend for FakeBackend {
        fn receive(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<RawStreamMessage, StreamConsumerError>> + Send + '_>>
        {
            Box::pin(async move {
                self.state
                    .messages
                    .lock()
                    .pop_front()
                    .ok_or_else(|| StreamConsumerError::engine("fake", "no message"))
            })
        }

        fn receive_batch(
            &self,
            policy: BatchReceivePolicy,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<Vec<RawStreamMessage>, StreamConsumerError>> + Send + '_,
            >,
        > {
            Box::pin(async move {
                let mut messages = self.state.messages.lock();
                let mut bytes = 0usize;
                let mut batch = Vec::new();
                while batch.len() < policy.max_messages() {
                    let Some(next) = messages.front() else {
                        break;
                    };
                    if !batch.is_empty()
                        && bytes.saturating_add(next.message.payload.len()) > policy.max_bytes()
                    {
                        break;
                    }
                    let Some(next) = messages.pop_front() else {
                        break;
                    };
                    bytes = bytes.saturating_add(next.message.payload.len());
                    batch.push(next);
                }
                if batch.is_empty() {
                    Err(StreamConsumerError::engine("fake", "no message"))
                } else {
                    Ok(batch)
                }
            })
        }

        fn restore_messages(&self, mut restored: Vec<RawStreamMessage>) {
            self.state.restore_calls.fetch_add(1, Ordering::SeqCst);
            restored.sort_by_key(|message| message.token.dequeue_sequence());
            let mut messages = self.state.messages.lock();
            for message in restored.into_iter().rev() {
                messages.push_front(message);
            }
        }

        fn get_schema<'a>(
            &'a self,
            _source: &'a SegmentSource,
            _version: Option<Bytes>,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<magnetar_proto::pb::Schema, StreamConsumerError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                self.state.schema_started.notify_one();
                if self.state.schema_blocked.load(Ordering::SeqCst) {
                    self.state.schema_release.notified().await;
                }
                Ok(magnetar_proto::pb::Schema {
                    name: "fake".to_owned(),
                    schema_data: Bytes::new(),
                    r#type: magnetar_proto::pb::schema::Type::String as i32,
                    properties: Vec::new(),
                })
            })
        }

        fn acknowledge<'a>(
            &'a self,
            token: &'a DeliveryToken,
        ) -> Pin<Box<dyn Future<Output = Result<(), StreamConsumerError>> + Send + 'a>> {
            Box::pin(async move { self.resolve(token) })
        }

        fn acknowledge_cumulative<'a>(
            &'a self,
            token: &'a DeliveryToken,
        ) -> Pin<Box<dyn Future<Output = Result<(), StreamConsumerError>> + Send + 'a>> {
            Box::pin(async move { self.resolve(token) })
        }

        fn acknowledge_positions<'a>(
            &'a self,
            positions: &'a PositionVector,
        ) -> Pin<Box<dyn Future<Output = Result<(), StreamConsumerError>> + Send + 'a>> {
            Box::pin(async move {
                if positions.layout_epoch() != self.state.delivered.lock().layout_epoch() {
                    return Err(StreamConsumerError::Model(
                        magnetar_proto::StreamConsumerModelError::SeekLayoutMismatch {
                            vector: positions.layout_epoch(),
                            dag: self.state.delivered.lock().layout_epoch(),
                        },
                    ));
                }
                *self.state.delivered.lock() = positions.clone();
                Ok(())
            })
        }

        fn acknowledge_batch<'a>(
            &'a self,
            tokens: Vec<&'a DeliveryToken>,
        ) -> Pin<Box<dyn Future<Output = Result<(), StreamConsumerError>> + Send + 'a>> {
            Box::pin(async move {
                for token in tokens {
                    self.resolve(token)?;
                }
                Ok(())
            })
        }

        fn negative_acknowledge(&self, token: &DeliveryToken) -> Result<(), StreamConsumerError> {
            self.state.negative_ack_calls.fetch_add(1, Ordering::SeqCst);
            self.resolve(token)
        }

        fn acknowledge_in_transaction<'a>(
            &'a self,
            _token: &'a DeliveryToken,
            _txn_id: magnetar_proto::TxnId,
        ) -> Pin<Box<dyn Future<Output = Result<(), StreamConsumerError>> + Send + 'a>> {
            Box::pin(self.transactional_ack())
        }

        fn acknowledge_cumulative_in_transaction<'a>(
            &'a self,
            _token: &'a DeliveryToken,
            _txn_id: magnetar_proto::TxnId,
        ) -> Pin<Box<dyn Future<Output = Result<(), StreamConsumerError>> + Send + 'a>> {
            Box::pin(self.transactional_ack())
        }

        fn acknowledge_positions_in_transaction<'a>(
            &'a self,
            _positions: &'a PositionVector,
            _txn_id: magnetar_proto::TxnId,
        ) -> Pin<Box<dyn Future<Output = Result<(), StreamConsumerError>> + Send + 'a>> {
            Box::pin(self.transactional_ack())
        }

        fn delivered_position(&self) -> PositionVector {
            self.state.delivered.lock().clone()
        }

        fn status(&self) -> StreamConsumerStatus {
            StreamConsumerStatus::new(
                if self.state.closed.load(Ordering::SeqCst) {
                    magnetar_proto::AggregatePhase::Closed
                } else {
                    magnetar_proto::AggregatePhase::Open
                },
                Some(self.state.delivered.lock().layout_epoch()),
                1,
                usize::from(!self.state.closed.load(Ordering::SeqCst)),
                0,
                Vec::new(),
                Vec::new(),
                DEFAULT_RECEIVER_BUDGET_BYTES,
                0,
            )
        }

        fn next_event(
            &self,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<Option<StreamConsumerEvent>, StreamConsumerError>>
                    + Send
                    + '_,
            >,
        > {
            Box::pin(async move { Ok(self.state.events.lock().pop_front()) })
        }

        fn seek_positions<'a>(
            &'a self,
            positions: &'a PositionVector,
        ) -> Pin<Box<dyn Future<Output = Result<(), StreamConsumerError>> + Send + 'a>> {
            Box::pin(async move {
                *self.state.delivered.lock() = positions.clone();
                Ok(())
            })
        }

        fn transaction_outcome(
            &self,
            txn_id: magnetar_proto::TxnId,
            outcome: TransactionOutcome,
        ) -> Pin<Box<dyn Future<Output = Result<(), StreamConsumerError>> + Send + '_>> {
            Box::pin(async move {
                self.state
                    .transaction_outcome_calls
                    .fetch_add(1, Ordering::SeqCst);
                if self
                    .state
                    .transaction_outcome_blocked
                    .load(Ordering::SeqCst)
                {
                    self.state.transaction_outcome_release.notified().await;
                }
                if self.state.closed.load(Ordering::SeqCst) {
                    return Err(StreamConsumerError::engine(
                        "fake",
                        "transaction outcome arrived after local close",
                    ));
                }
                self.state
                    .transaction_outcomes
                    .lock()
                    .push((txn_id, outcome));
                Ok(())
            })
        }

        fn close(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<(), StreamConsumerError>> + Send + '_>> {
            Box::pin(async move {
                if !self.state.closed.swap(true, Ordering::SeqCst) {
                    self.state.close_calls.fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            })
        }

        fn close_best_effort(&self) {
            self.state.closed.store(true, Ordering::SeqCst);
            self.state.best_effort_calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct NonCloneValue(String);

    #[derive(Debug)]
    struct NonCloneSchema;

    impl Schema for NonCloneSchema {
        type Owned = NonCloneValue;

        fn schema_type(&self) -> magnetar_proto::pb::schema::Type {
            magnetar_proto::pb::schema::Type::String
        }

        fn schema_data(&self) -> Bytes {
            Bytes::new()
        }

        fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError> {
            Ok(Bytes::copy_from_slice(value.0.as_bytes()))
        }

        fn decode(&self, bytes: &[u8]) -> Result<Self::Owned, SchemaError> {
            String::from_utf8(bytes.to_vec())
                .map(NonCloneValue)
                .map_err(|error| SchemaError::Decoding(error.to_string()))
        }
    }

    #[derive(Debug)]
    struct NeedsBrokerSchema;

    impl Schema for NeedsBrokerSchema {
        type Owned = Bytes;

        fn schema_type(&self) -> magnetar_proto::pb::schema::Type {
            magnetar_proto::pb::schema::Type::None
        }

        fn schema_data(&self) -> Bytes {
            Bytes::new()
        }

        fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError> {
            Ok(value.clone())
        }

        fn decode(&self, bytes: &[u8]) -> Result<Self::Owned, SchemaError> {
            Ok(Bytes::copy_from_slice(bytes))
        }

        fn needs_broker_schema(&self) -> bool {
            true
        }
    }

    fn facade_client(backend: FakeBackend) -> PulsarClient<FakeEngine> {
        PulsarClient {
            inner: FakeClient {
                backend,
                options: Arc::new(parking_lot::Mutex::new(None)),
                transactions: Arc::new(FakeTransactionState::default()),
            },
            memory_limit: None,
            transactions: Arc::new(crate::transaction::TransactionCoordinator::default()),
        }
    }

    fn single_segment_snapshot() -> DagSnapshot {
        let dag = magnetar_proto::pb::ScalableTopicDag {
            epoch: 1,
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
                broker_url: "pulsar://segment:6650".to_owned(),
                broker_url_tls: None,
            }],
            controller_broker_url: Some("pulsar://controller:6650".to_owned()),
            controller_broker_url_tls: None,
        };
        DagSnapshot::try_from_pb(&dag).expect("valid one-segment DAG")
    }

    fn delivery(
        payload: &[u8],
        instance: u64,
    ) -> (magnetar_proto::StreamConsumerModel, RawStreamMessage) {
        let source_topic = "topic://public/default/scaled";
        let segment_topic = magnetar_proto::canonical_segment_topic(
            source_topic,
            magnetar_proto::KeyRange::FULL,
            SegmentId(1),
        )
        .expect("canonical segment topic");
        let assignment = magnetar_proto::ConsumerAssignment::try_from_pb(
            &magnetar_proto::pb::ScalableConsumerAssignment {
                layout_epoch: 1,
                segments: vec![magnetar_proto::pb::ScalableAssignedSegment {
                    segment_id: 1,
                    hash_start: 0,
                    hash_end: 65_535,
                    segment_topic,
                }],
            },
            source_topic,
        )
        .expect("valid one-segment assignment");
        let source = assignment.segments()[0].source();
        let mut model = magnetar_proto::StreamConsumerModel::new(
            source_topic.to_owned(),
            magnetar_proto::ConsumerInstanceId(instance),
            magnetar_proto::ControllerIncarnation(1),
            OrderingMode::BrokerManaged,
            single_segment_snapshot(),
            ReceiverBudget::bytes(DEFAULT_RECEIVER_BUDGET_BYTES).expect("valid default budget"),
        )
        .expect("valid aggregate model");
        let opens = model
            .apply_assignment(assignment)
            .expect("assignment opens child");
        let generation = match opens.as_slice() {
            [
                magnetar_proto::StreamConsumerAction::OpenChild {
                    child_generation, ..
                },
            ] => *child_generation,
            other => panic!("expected one child open, got {other:?}"),
        };
        let flows = model
            .child_opened(SegmentId(1), generation)
            .expect("child opens");
        let reservation = match flows.as_slice() {
            [magnetar_proto::StreamConsumerAction::GrantFlow { reservation, .. }] => *reservation,
            other => panic!("expected one flow grant, got {other:?}"),
        };
        let retained = model
            .message_arrived(SegmentId(1), generation, reservation, payload.len())
            .expect("message retained")
            .retained;
        let ordinary = magnetar_proto::MessageId {
            ledger_id: 1,
            entry_id: instance,
            partition: -1,
            batch_index: -1,
            batch_size: 0,
        };
        let stream_id = StreamMessageId::new(source, ordinary).expect("stream id");
        let token = model
            .issue_delivery(SegmentId(1), generation, stream_id, retained)
            .expect("delivery authority");
        let incoming = magnetar_proto::IncomingMessage {
            message_id: ordinary,
            metadata: Arc::new(magnetar_proto::pb::MessageMetadata::default()),
            single_metadata: None,
            payload: Bytes::copy_from_slice(payload),
            redelivery_count: 0,
            broker_entry_metadata: None,
            arrived_at: std::time::Instant::now(),
        };
        (
            model,
            RawStreamMessage {
                message: incoming,
                token,
            },
        )
    }

    #[tokio::test]
    async fn builder_requires_subscription_and_freezes_child_options() {
        let backend = FakeBackend::empty();
        let client = facade_client(backend);
        let schema = Arc::new(NonCloneSchema);

        let error = client
            .scalable_stream_consumer("topic://public/default/scaled", schema.clone())
            .subscribe()
            .await
            .expect_err("subscription is required");
        assert!(matches!(error, StreamConsumerError::MissingSubscription));
        assert!(client.inner.options.lock().is_none());

        let budget = ReceiverBudget::bytes(DEFAULT_RECEIVER_BUDGET_BYTES + 1024)
            .expect("valid custom budget");
        let consumer = client
            .scalable_stream_consumer("topic://public/default/scaled", schema)
            .subscription("workers")
            .consumer_name("worker-a")
            .receiver_budget(budget)
            .ordering_mode(OrderingMode::BrokerManaged)
            .subscribe()
            .await
            .expect("fake subscribe");
        let options = client.inner.options.lock();
        let options = options.as_ref().expect("captured options");
        assert_eq!(options.subscription, "workers");
        assert_eq!(options.consumer_name, "worker-a");
        assert_eq!(options.receiver_budget, budget);
        assert_eq!(options.ordering_mode, OrderingMode::BrokerManaged);
        assert_eq!(consumer.subscription(), "workers");
    }

    #[tokio::test]
    async fn schema_and_owned_payload_need_no_clone_and_authority_is_live() {
        let backend = FakeBackend::with_delivery(b"hello", 1);
        let client = facade_client(backend);
        let schema = Arc::new(NonCloneSchema);
        let consumer = client
            .scalable_stream_consumer("topic://public/default/scaled", schema.clone())
            .subscription("workers")
            .subscribe()
            .await
            .expect("subscribe");
        assert!(Arc::ptr_eq(consumer.schema(), &schema));

        let message = consumer.receive().await.expect("typed delivery");
        assert_eq!(message.value().0, "hello");
        assert_eq!(message.source().segment_id(), SegmentId(1));
        let serialized = message.position().to_bytes().expect("serialize vector");
        let restored = PositionVector::from_bytes(&serialized).expect("restore vector");
        assert_eq!(&restored, message.position());
        consumer.acknowledge(&message).await.expect("live ack");
        assert!(matches!(
            consumer.acknowledge(&message).await,
            Err(StreamConsumerError::Model(
                magnetar_proto::StreamConsumerModelError::StaleDeliveryToken
            ))
        ));
    }

    #[tokio::test]
    async fn cancelled_schema_resolution_restores_reserved_delivery() {
        let backend = FakeBackend::with_delivery(b"cancel", 1);
        backend.state.schema_blocked.store(true, Ordering::SeqCst);
        let state = backend.state.clone();
        let client = facade_client(backend);
        let consumer = client
            .scalable_stream_consumer("topic://public/default/scaled", Arc::new(NeedsBrokerSchema))
            .subscription("workers")
            .subscribe()
            .await
            .expect("subscribe");

        let mut receive = Box::pin(consumer.receive());
        assert!(matches!(
            futures_util::poll!(receive.as_mut()),
            Poll::Pending
        ));
        state.schema_started.notified().await;
        drop(receive);

        assert_eq!(state.negative_ack_calls.load(Ordering::SeqCst), 0);
        assert_eq!(state.restore_calls.load(Ordering::SeqCst), 1);
        state.schema_blocked.store(false, Ordering::SeqCst);
        let restored = consumer.receive().await.expect("restored delivery");
        assert_eq!(restored.value(), &Bytes::from_static(b"cancel"));
        assert_eq!(restored.delivery_token().dequeue_sequence().0, 0);
    }

    #[tokio::test]
    async fn foreign_token_is_rejected_before_backend_io() {
        let first = FakeBackend::with_delivery(b"first", 1);
        let second = FakeBackend::with_delivery(b"second", 2);
        let first_client = facade_client(first);
        let second_client = facade_client(second);
        let first_consumer = first_client
            .scalable_stream_consumer("topic://public/default/scaled", Arc::new(NonCloneSchema))
            .subscription("workers")
            .subscribe()
            .await
            .expect("first subscribe");
        let second_consumer = second_client
            .scalable_stream_consumer("topic://public/default/scaled", Arc::new(NonCloneSchema))
            .subscription("workers")
            .subscribe()
            .await
            .expect("second subscribe");
        let foreign = second_consumer.receive().await.expect("foreign delivery");
        assert!(matches!(
            first_consumer.acknowledge(&foreign).await,
            Err(StreamConsumerError::Model(
                magnetar_proto::StreamConsumerModelError::StaleDeliveryToken
            ))
        ));
    }

    #[tokio::test]
    async fn clone_shares_globally_definitive_async_close() {
        let backend = FakeBackend::empty();
        let state = backend.state.clone();
        let client = facade_client(backend);
        let consumer = client
            .scalable_stream_consumer("topic://public/default/scaled", Arc::new(NonCloneSchema))
            .subscription("workers")
            .subscribe()
            .await
            .expect("subscribe");
        let intermediate = consumer.clone();
        let closer = consumer.clone();
        drop(intermediate);
        assert_eq!(state.best_effort_calls.load(Ordering::SeqCst), 0);

        closer.close().await.expect("explicit close");
        assert_eq!(
            consumer.status().phase(),
            magnetar_proto::AggregatePhase::Closed
        );
        assert_eq!(state.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(state.best_effort_calls.load(Ordering::SeqCst), 0);
        drop(consumer);
        assert_eq!(state.best_effort_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn commit_waits_for_admitted_ack_and_propagates_outcome() {
        let backend = FakeBackend::with_delivery(b"txn", 1);
        backend
            .state
            .transaction_ack_blocked
            .store(true, Ordering::SeqCst);
        let backend_state = backend.state.clone();
        let client = facade_client(backend);
        let transaction_state = client.inner.transactions.clone();
        let consumer = client
            .scalable_stream_consumer("topic://public/default/scaled", Arc::new(NonCloneSchema))
            .subscription("workers")
            .subscribe()
            .await
            .expect("subscribe");
        let message = consumer.receive().await.expect("delivery");
        let transaction = client
            .new_transaction(Duration::from_secs(30))
            .await
            .expect("transaction");

        let ack = consumer.acknowledge_in_transaction(&message, transaction);
        tokio::pin!(ack);
        assert!(matches!(futures_util::poll!(ack.as_mut()), Poll::Pending));
        let commit = client.commit_transaction(transaction);
        tokio::pin!(commit);
        assert!(matches!(
            futures_util::poll!(commit.as_mut()),
            Poll::Pending
        ));
        assert_eq!(transaction_state.end_calls.load(Ordering::SeqCst), 0);

        backend_state.transaction_ack_release.notify_one();
        ack.await.expect("transactional ack settles");
        assert_eq!(
            commit.await.expect("commit"),
            magnetar_proto::TxnState::Committed
        );
        assert_eq!(transaction_state.end_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            backend_state.transaction_outcomes.lock().as_slice(),
            &[(transaction.id(), TransactionOutcome::Committed)]
        );
    }

    #[tokio::test]
    async fn failed_ack_poisons_commit_but_abort_remains_available() {
        let backend = FakeBackend::with_delivery(b"txn", 1);
        backend
            .state
            .transaction_ack_fails
            .store(true, Ordering::SeqCst);
        let backend_state = backend.state.clone();
        let client = facade_client(backend);
        let transaction_state = client.inner.transactions.clone();
        let consumer = client
            .scalable_stream_consumer("topic://public/default/scaled", Arc::new(NonCloneSchema))
            .subscription("workers")
            .subscribe()
            .await
            .expect("subscribe");
        let message = consumer.receive().await.expect("delivery");
        let transaction = client
            .new_transaction(Duration::from_secs(30))
            .await
            .expect("transaction");

        assert!(
            consumer
                .acknowledge_in_transaction(&message, transaction)
                .await
                .is_err()
        );
        assert!(matches!(
            client.commit_transaction(transaction).await,
            Err(crate::PulsarError::StreamConsumer(
                StreamConsumerError::TransactionPoisoned { txn_id }
            )) if txn_id == transaction.id()
        ));
        assert_eq!(transaction_state.end_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            client
                .abort_transaction(transaction)
                .await
                .expect("abort poisoned transaction"),
            magnetar_proto::TxnState::Aborted
        );
        assert_eq!(transaction_state.end_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            backend_state.transaction_outcomes.lock().as_slice(),
            &[(transaction.id(), TransactionOutcome::Aborted)]
        );
    }

    #[tokio::test]
    async fn confirmed_commit_survives_local_participant_close() {
        let backend = FakeBackend::with_delivery(b"txn", 1);
        let backend_state = backend.state.clone();
        let client = facade_client(backend);
        let consumer = client
            .scalable_stream_consumer("topic://public/default/scaled", Arc::new(NonCloneSchema))
            .subscription("workers")
            .subscribe()
            .await
            .expect("subscribe");
        let message = consumer.receive().await.expect("delivery");
        let transaction = client
            .new_transaction(Duration::from_secs(30))
            .await
            .expect("transaction");
        consumer
            .acknowledge_in_transaction(&message, transaction)
            .await
            .expect("transactional acknowledgement");

        consumer.clone().close().await.expect("local close");

        assert_eq!(
            client
                .commit_transaction(transaction)
                .await
                .expect("broker-confirmed commit remains authoritative"),
            magnetar_proto::TxnState::Committed
        );
        assert_eq!(
            backend_state
                .transaction_outcome_calls
                .load(Ordering::SeqCst),
            1
        );
    }

    #[tokio::test]
    async fn cancelled_transaction_finalization_can_retry() {
        let backend = FakeBackend::with_delivery(b"txn", 1);
        backend
            .state
            .transaction_outcome_blocked
            .store(true, Ordering::SeqCst);
        let backend_state = backend.state.clone();
        let client = facade_client(backend);
        let transaction_state = client.inner.transactions.clone();
        let consumer = client
            .scalable_stream_consumer("topic://public/default/scaled", Arc::new(NonCloneSchema))
            .subscription("workers")
            .subscribe()
            .await
            .expect("subscribe");
        let message = consumer.receive().await.expect("delivery");
        let transaction = client
            .new_transaction(Duration::from_secs(30))
            .await
            .expect("transaction");
        consumer
            .acknowledge_in_transaction(&message, transaction)
            .await
            .expect("transactional acknowledgement");

        let mut first = Box::pin(client.commit_transaction(transaction));
        std::future::poll_fn(|context| {
            assert!(matches!(
                std::future::Future::poll(first.as_mut(), context),
                Poll::Pending
            ));
            Poll::Ready(())
        })
        .await;
        assert_eq!(transaction_state.end_calls.load(Ordering::SeqCst), 1);
        drop(first);

        backend_state
            .transaction_outcome_blocked
            .store(false, Ordering::SeqCst);
        assert_eq!(
            client
                .commit_transaction(transaction)
                .await
                .expect("cancelled local propagation is retryable"),
            magnetar_proto::TxnState::Committed
        );
        assert_eq!(
            transaction_state.end_calls.load(Ordering::SeqCst),
            1,
            "broker-confirmed EndTxn must not be reissued"
        );
        assert_eq!(
            backend_state
                .transaction_outcome_calls
                .load(Ordering::SeqCst),
            2
        );
    }

    #[test]
    fn generic_stream_consumer_types_name_without_clone_bounds() {
        fn assert_send<T: Send>() {}
        assert_send::<StreamConsumer<NonCloneSchema, FakeEngine>>();
        let _: Option<StreamConsumer<NonCloneSchema, crate::TokioEngine>> = None;
        #[cfg(feature = "moonpool")]
        let _: Option<
            StreamConsumer<NonCloneSchema, crate::MoonpoolEngine<moonpool_core::TokioProviders>>,
        > = None;
    }
}
