// SPDX-License-Identifier: Apache-2.0

//! Pure aggregate scalable stream-consumer model.
//!
//! The model owns no tasks, channels, clocks, or I/O. It emits generation-
//! fenced child actions for a runtime to execute and accepts the corresponding
//! results back. Child attachment may happen before ancestry is deliverable;
//! FLOW is a distinct transition guarded by the validated DAG and aggregate
//! receive budget.

use std::collections::{BTreeMap, BTreeSet};

use bytes::{Buf as _, Bytes, BytesMut};
use prost::Message as _;

use crate::consumer::MAX_CHUNK_TOTAL;
use crate::dag_watch::{
    AttachmentError, DagSnapshot, OrderingEligibility, OrderingError, OrderingMode,
};
use crate::frame::{DECOMPRESSION_VALIDATION_SLACK, MAX_FRAME_SIZE, ZSTD_MIN_WINDOW_SIZE};
use crate::scalable_consumer::{
    AssignmentError, ConsumerAssignment, ControllerIncarnation, SegmentSource, SegmentTopicError,
    canonical_segment_topic,
};
use crate::stream_position::{
    MAX_POSITION_COMPONENTS, MAX_STREAM_POSITION_SIZE, PositionVector, StreamMessageId,
    StreamPositionError,
};
use crate::txn::TxnId;
use crate::types::{KeyRange, SegmentId, SegmentState};
use crate::{DeferredIncomingMessage, IncomingMessage};

/// Data-independent reserve that remains available for close and revocation.
pub const CONTROL_PLANE_CLEANUP_RESERVE: usize = 64 * 1024;
/// Minimum data charge retained for an empty message and its queue metadata.
pub const MIN_RETAINED_MESSAGE_RESERVATION: usize = 64;
/// Fixed per-allocation authority bookkeeping charged beside encoded position data.
pub const DELIVERY_AUTHORITY_OVERHEAD: usize = 64;
/// Conservative allocation charge for one `BTreeMap` position component node.
pub const POSITION_COMPONENT_NODE_OVERHEAD: usize = 256;
/// Conservative fixed workspace for zlib inflate state and its 32 KiB window.
pub const ZLIB_DECOMPRESSION_WORKSPACE: usize = 96 * 1024;
/// Conservative fixed zstd context workspace, excluding its bounded window.
pub const ZSTD_DECOMPRESSION_CONTEXT_WORKSPACE: usize = 256 * 1024;
/// Worst-case live and retained authority metadata kept beside one maximum frame.
pub const RECEIVER_BUDGET_AUTHORITY_HEADROOM: usize = 5 * MAX_STREAM_POSITION_SIZE
    + 3 * MAX_POSITION_COMPONENTS * POSITION_COMPONENT_NODE_OVERHEAD
    + 2 * DELIVERY_AUTHORITY_OVERHEAD;

/// Process-local aggregate consumer identity supplied by the owning runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConsumerInstanceId(pub u64);

/// Generation of the aggregate assignment/lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct AggregateGeneration(pub u64);

/// Generation of one ordinary child consumer attachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChildGeneration(pub u64);

/// Epoch invalidating authority after seek, reset, or aggregate close.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct DeliveryEpoch(pub u64);

/// Linearized aggregate dequeue sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DequeueSequence(pub u64);

/// Opaque receive-budget reservation id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BudgetReservationId(u64);

/// Child identity owning one aggregate data reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BudgetReservationOwner {
    segment_id: SegmentId,
    child_generation: ChildGeneration,
}

impl BudgetReservationOwner {
    const fn new(segment_id: SegmentId, child_generation: ChildGeneration) -> Self {
        Self {
            segment_id,
            child_generation,
        }
    }

    /// Segment holding the reservation.
    #[must_use]
    pub const fn segment_id(self) -> SegmentId {
        self.segment_id
    }

    /// Child generation holding the reservation.
    #[must_use]
    pub const fn child_generation(self) -> ChildGeneration {
        self.child_generation
    }
}

/// What one data reservation currently accounts for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BudgetUse {
    /// One granted-but-unconsumed message permit, reserved at max-frame size.
    FlowPermit,
    /// Encoded/decompressed message retained before application delivery.
    RetainedMessage,
    /// Announced chunk assembly.
    ChunkAssembly,
    /// Decompression workspace.
    Decompression,
    /// Structural storage created while exploding one broker batch.
    BatchAssembly,
    /// Message held behind an ordering barrier.
    OrderingBarrier,
    /// Message leased to the application.
    DeliveryLease,
    /// Aggregate canonical delivered-position metadata retained between deliveries.
    DeliveredPositionMetadata,
    /// Message retained while a lost child drains.
    RetiringChild,
}

/// Validated aggregate receive budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiverBudget {
    bytes: usize,
}

impl ReceiverBudget {
    /// Validate a byte budget.
    ///
    /// The total must fit one maximum Pulsar frame, worst-case live and retained
    /// authority metadata, and the fixed control-plane cleanup reserve.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError::BudgetTooSmall`] below the required minimum.
    pub fn bytes(bytes: usize) -> Result<Self, BudgetError> {
        let minimum = MAX_FRAME_SIZE
            .saturating_add(RECEIVER_BUDGET_AUTHORITY_HEADROOM)
            .saturating_add(CONTROL_PLANE_CLEANUP_RESERVE);
        if bytes < minimum {
            return Err(BudgetError::BudgetTooSmall { bytes, minimum });
        }
        Ok(Self { bytes })
    }

    /// Configured aggregate bytes, including cleanup reserve.
    #[must_use]
    pub const fn limit(self) -> usize {
        self.bytes
    }

    /// Bytes available to data-plane reservations.
    #[must_use]
    pub const fn data_limit(self) -> usize {
        self.bytes - CONTROL_PLANE_CLEANUP_RESERVE
    }
}

/// Pure receive-budget accounting failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BudgetError {
    /// Configuration cannot admit one max-size frame and cleanup.
    #[error("receiver budget {bytes} is below required minimum {minimum}")]
    BudgetTooSmall {
        /// Configured bytes.
        bytes: usize,
        /// Required bytes.
        minimum: usize,
    },
    /// Aggregate data capacity is currently exhausted.
    #[error("receiver budget exhausted: requested {requested}, available {available}")]
    Exhausted {
        /// Requested bytes.
        requested: usize,
        /// Currently available bytes.
        available: usize,
    },
    /// A single retained object cannot fit the configured data budget.
    #[error("message requires {required} bytes but data budget is {limit}")]
    MessageTooLargeForBudget {
        /// Required bytes.
        required: usize,
        /// Data-plane limit.
        limit: usize,
    },
    /// Completed allocations exceeded the bytes reserved before allocation.
    #[error("receive work requires {required} bytes but only {reserved} were preallocated")]
    PreallocationExceeded {
        /// Exact retained charge required by completed work.
        required: usize,
        /// Bytes reserved before the controlled allocations occurred.
        reserved: usize,
    },
    /// Cleanup itself exceeded its independent fixed reserve.
    #[error("control-plane reserve exhausted: requested {requested}, available {available}")]
    ControlReserveExhausted {
        /// Requested bytes.
        requested: usize,
        /// Remaining cleanup bytes.
        available: usize,
    },
    /// Runtime returned a reservation the model did not issue or already freed.
    #[error("unknown receive-budget reservation {reservation:?}")]
    UnknownReservation {
        /// Unknown id.
        reservation: BudgetReservationId,
    },
    /// Reservation belongs to another accounting class.
    #[error("reservation {reservation:?} has use {actual:?}, expected {expected:?}")]
    ReservationUseMismatch {
        /// Reservation id.
        reservation: BudgetReservationId,
        /// Current class.
        actual: BudgetUse,
        /// Required class.
        expected: BudgetUse,
    },
    /// Reservation belongs to another child generation.
    #[error("reservation {reservation:?} belongs to {actual:?}, expected {expected:?}")]
    ReservationOwnerMismatch {
        /// Reservation id.
        reservation: BudgetReservationId,
        /// Bound child, or `None` for a standalone ledger reservation.
        actual: Option<BudgetReservationOwner>,
        /// Required child.
        expected: BudgetReservationOwner,
    },
    /// Monotonic reservation ids cannot advance further.
    #[error("receive-budget reservation id exhausted")]
    ReservationIdExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DataReservation {
    use_: BudgetUse,
    bytes: usize,
    authority_bytes: usize,
    owner: Option<BudgetReservationOwner>,
}

/// Aggregate budget ledger with explicit reservation and transfer operations.
#[derive(Debug, Clone)]
pub struct ReceiverBudgetState {
    budget: ReceiverBudget,
    data_used: usize,
    control_used: usize,
    next_id: u64,
    reservations: BTreeMap<BudgetReservationId, DataReservation>,
}

impl ReceiverBudgetState {
    /// Start an empty ledger.
    #[must_use]
    pub fn new(budget: ReceiverBudget) -> Self {
        Self {
            budget,
            data_used: 0,
            control_used: 0,
            next_id: 0,
            reservations: BTreeMap::new(),
        }
    }

    /// Bytes currently accounted to data.
    #[must_use]
    pub const fn data_used(&self) -> usize {
        self.data_used
    }

    /// Configured aggregate bytes, including the cleanup reserve.
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.budget.limit()
    }

    /// Unreserved data bytes.
    #[must_use]
    pub const fn data_available(&self) -> usize {
        self.budget.data_limit() - self.data_used
    }

    /// Bytes currently borrowed from the cleanup reserve.
    #[must_use]
    pub const fn control_used(&self) -> usize {
        self.control_used
    }

    fn authority_used(&self) -> usize {
        self.reservations
            .values()
            .fold(0usize, |used, reservation| {
                used.saturating_add(reservation.authority_bytes)
            })
    }

    /// Reserve one max-frame message permit before emitting FLOW.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError::Exhausted`] without mutating the ledger.
    pub fn reserve_flow(&mut self) -> Result<BudgetReservationId, BudgetError> {
        let authority_used = self.authority_used();
        let authority_remaining = RECEIVER_BUDGET_AUTHORITY_HEADROOM.saturating_sub(authority_used);
        let flow_available = self.data_available().saturating_sub(authority_remaining);
        if MAX_FRAME_SIZE > flow_available {
            return Err(BudgetError::Exhausted {
                requested: MAX_FRAME_SIZE,
                available: flow_available,
            });
        }
        self.reserve(BudgetUse::FlowPermit, MAX_FRAME_SIZE)
    }

    /// Reserve a known data allocation.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError`] without partial accounting.
    pub fn reserve(
        &mut self,
        use_: BudgetUse,
        bytes: usize,
    ) -> Result<BudgetReservationId, BudgetError> {
        self.reserve_inner(use_, bytes, None)
    }

    fn reserve_owned(
        &mut self,
        owner: BudgetReservationOwner,
        use_: BudgetUse,
        bytes: usize,
    ) -> Result<BudgetReservationId, BudgetError> {
        self.reserve_inner(use_, bytes, Some(owner))
    }

    fn reserve_inner(
        &mut self,
        use_: BudgetUse,
        bytes: usize,
        owner: Option<BudgetReservationOwner>,
    ) -> Result<BudgetReservationId, BudgetError> {
        let bytes = if use_ == BudgetUse::RetainedMessage {
            bytes.max(MIN_RETAINED_MESSAGE_RESERVATION)
        } else {
            bytes
        };
        if bytes > self.budget.data_limit() {
            return Err(BudgetError::MessageTooLargeForBudget {
                required: bytes,
                limit: self.budget.data_limit(),
            });
        }
        let authority_bytes = if use_ == BudgetUse::DeliveredPositionMetadata {
            bytes
        } else {
            0
        };
        let effective_limit = self
            .budget
            .data_limit()
            .saturating_sub(RECEIVER_BUDGET_AUTHORITY_HEADROOM.saturating_sub(authority_bytes));
        if bytes > effective_limit {
            return Err(BudgetError::MessageTooLargeForBudget {
                required: bytes,
                limit: effective_limit,
            });
        }
        let authority_remaining = RECEIVER_BUDGET_AUTHORITY_HEADROOM
            .saturating_sub(self.authority_used().saturating_add(authority_bytes));
        let available = self.data_available().saturating_sub(authority_remaining);
        if bytes > available {
            return Err(BudgetError::Exhausted {
                requested: bytes,
                available,
            });
        }
        let id = BudgetReservationId(self.next_id);
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or(BudgetError::ReservationIdExhausted)?;
        self.data_used += bytes;
        self.reservations.insert(
            id,
            DataReservation {
                use_,
                bytes,
                authority_bytes,
                owner,
            },
        );
        Ok(id)
    }

    /// Transfer one reservation between lifecycle states and resize it to exact
    /// retained bytes. The old bytes count toward capacity during the atomic
    /// resize, so a move never double-counts.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError`] and leaves the reservation unchanged.
    pub fn transfer(
        &mut self,
        reservation: BudgetReservationId,
        expected: BudgetUse,
        new_use: BudgetUse,
        bytes: usize,
    ) -> Result<(), BudgetError> {
        self.transfer_inner(reservation, expected, new_use, bytes, None)
    }

    fn transfer_owned(
        &mut self,
        reservation: BudgetReservationId,
        owner: BudgetReservationOwner,
        expected: BudgetUse,
        new_use: BudgetUse,
        bytes: usize,
    ) -> Result<(), BudgetError> {
        self.owned(reservation, owner, expected)?;
        self.transfer_inner(reservation, expected, new_use, bytes, None)
    }

    fn transfer_owned_with_authority(
        &mut self,
        reservation: BudgetReservationId,
        owner: BudgetReservationOwner,
        expected: BudgetUse,
        new_use: BudgetUse,
        bytes: usize,
        authority_bytes: usize,
    ) -> Result<(), BudgetError> {
        self.owned(reservation, owner, expected)?;
        self.transfer_inner(reservation, expected, new_use, bytes, Some(authority_bytes))
    }

    fn transfer_inner(
        &mut self,
        reservation: BudgetReservationId,
        expected: BudgetUse,
        new_use: BudgetUse,
        bytes: usize,
        authority_bytes: Option<usize>,
    ) -> Result<(), BudgetError> {
        let Some(current) = self.reservations.get(&reservation).copied() else {
            return Err(BudgetError::UnknownReservation { reservation });
        };
        if current.use_ != expected {
            return Err(BudgetError::ReservationUseMismatch {
                reservation,
                actual: current.use_,
                expected,
            });
        }
        let bytes = if new_use == BudgetUse::RetainedMessage {
            bytes.max(MIN_RETAINED_MESSAGE_RESERVATION)
        } else {
            bytes
        };
        let authority_bytes = authority_bytes.unwrap_or_else(|| {
            if new_use == BudgetUse::DeliveredPositionMetadata {
                bytes
            } else {
                0
            }
        });
        let effective_limit = self
            .budget
            .data_limit()
            .saturating_sub(RECEIVER_BUDGET_AUTHORITY_HEADROOM.saturating_sub(authority_bytes));
        if bytes > effective_limit {
            return Err(BudgetError::MessageTooLargeForBudget {
                required: bytes,
                limit: effective_limit,
            });
        }
        let available_with_current = self.data_available().saturating_add(current.bytes);
        let authority_used = self
            .authority_used()
            .saturating_sub(current.authority_bytes)
            .saturating_add(authority_bytes);
        let authority_remaining = RECEIVER_BUDGET_AUTHORITY_HEADROOM.saturating_sub(authority_used);
        let available = available_with_current.saturating_sub(authority_remaining);
        if bytes > available {
            return Err(BudgetError::Exhausted {
                requested: bytes,
                available,
            });
        }
        self.data_used = self.data_used - current.bytes + bytes;
        self.reservations.insert(
            reservation,
            DataReservation {
                use_: new_use,
                bytes,
                authority_bytes,
                owner: current.owner,
            },
        );
        Ok(())
    }

    /// Release one data reservation exactly once.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError::UnknownReservation`] for stale ids.
    pub fn release(&mut self, reservation: BudgetReservationId) -> Result<(), BudgetError> {
        let Some(reservation) = self.reservations.remove(&reservation) else {
            return Err(BudgetError::UnknownReservation { reservation });
        };
        self.data_used -= reservation.bytes;
        Ok(())
    }

    /// Borrow from the cleanup reserve independently of data pressure.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError::ControlReserveExhausted`] without mutation.
    pub fn reserve_control(&mut self, bytes: usize) -> Result<(), BudgetError> {
        let available = CONTROL_PLANE_CLEANUP_RESERVE - self.control_used;
        if bytes > available {
            return Err(BudgetError::ControlReserveExhausted {
                requested: bytes,
                available,
            });
        }
        self.control_used += bytes;
        Ok(())
    }

    /// Return bytes to the cleanup reserve. Over-release saturates at empty
    /// because cleanup is aggregate accounting, not externally-held authority.
    pub fn release_control(&mut self, bytes: usize) {
        self.control_used = self.control_used.saturating_sub(bytes);
    }

    #[cfg(test)]
    fn use_of(&self, reservation: BudgetReservationId) -> Option<BudgetUse> {
        self.reservations.get(&reservation).map(|entry| entry.use_)
    }

    fn owned(
        &self,
        reservation: BudgetReservationId,
        owner: BudgetReservationOwner,
        expected: BudgetUse,
    ) -> Result<DataReservation, BudgetError> {
        let entry = self
            .reservations
            .get(&reservation)
            .copied()
            .ok_or(BudgetError::UnknownReservation { reservation })?;
        if entry.owner != Some(owner) {
            return Err(BudgetError::ReservationOwnerMismatch {
                reservation,
                actual: entry.owner,
                expected: owner,
            });
        }
        if entry.use_ != expected {
            return Err(BudgetError::ReservationUseMismatch {
                reservation,
                actual: entry.use_,
                expected,
            });
        }
        Ok(entry)
    }

    fn release_owner(&mut self, owner: BudgetReservationOwner) -> Vec<BudgetReservationId> {
        let ids: Vec<BudgetReservationId> = self
            .reservations
            .iter()
            .filter_map(|(id, entry)| (entry.owner == Some(owner)).then_some(*id))
            .collect();
        for id in &ids {
            if let Some(entry) = self.reservations.remove(id) {
                self.data_used = self.data_used.saturating_sub(entry.bytes);
            }
        }
        ids
    }
}

/// Opaque id of an admitted aggregate transaction operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TransactionOperationId(u64);

/// Aggregate acknowledgement admission state for one Pulsar transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateTransactionState {
    /// New registration/ack operations are accepted.
    Open,
    /// Commit closed admission and is waiting for admitted work.
    CommitClosing,
    /// Commit was issued exactly once and awaits the coordinator outcome.
    CommitIssued,
    /// Abort closed admission and is waiting for admitted work.
    AbortClosing,
    /// Abort was issued exactly once and awaits the coordinator outcome.
    AbortIssued,
    /// Coordinator confirmed commit.
    Committed,
    /// Coordinator confirmed abort.
    Aborted,
    /// Coordinator outcome cannot be established.
    Unknown,
}

/// Whether the runtime may issue an end-transaction command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionDecision {
    /// Admitted registration/ack results remain outstanding.
    Wait {
        /// Pending operation count.
        pending: usize,
    },
    /// Every admitted operation succeeded; issue commit.
    IssueCommit,
    /// Admission is closed; issue abort after pending work settles.
    IssueAbort,
    /// A failed admitted operation forbids commit. Abort remains available.
    TransactionPoisoned,
}

/// Pure aggregate transaction-gate error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AggregateTransactionError {
    /// Admission is already closed.
    #[error("transaction {txn_id} is not open for aggregate acknowledgements")]
    AdmissionClosed {
        /// Transaction id.
        txn_id: TxnId,
    },
    /// Operation result was duplicated or belongs to another transaction.
    #[error("unknown aggregate transaction operation {operation:?}")]
    UnknownOperation {
        /// Unknown operation.
        operation: TransactionOperationId,
    },
    /// The lifecycle does not admit the requested transition.
    #[error("invalid aggregate transaction transition from {state:?}")]
    InvalidTransition {
        /// Current state.
        state: AggregateTransactionState,
    },
    /// Monotonic operation ids cannot advance further.
    #[error("aggregate transaction operation id exhausted")]
    OperationIdExhausted,
}

/// Single-flight admission gate for aggregate transaction work.
#[derive(Debug, Clone)]
pub struct AggregateTransaction {
    txn_id: TxnId,
    state: AggregateTransactionState,
    next_operation: u64,
    pending: BTreeSet<TransactionOperationId>,
    poisoned: bool,
}

impl AggregateTransaction {
    /// Open a transaction-local aggregate coordinator.
    #[must_use]
    pub fn new(txn_id: TxnId) -> Self {
        Self {
            txn_id,
            state: AggregateTransactionState::Open,
            next_operation: 0,
            pending: BTreeSet::new(),
            poisoned: false,
        }
    }

    /// Current admission lifecycle.
    #[must_use]
    pub const fn state(&self) -> AggregateTransactionState {
        self.state
    }

    /// Number of admitted operations whose result is outstanding.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Whether any admitted registration or acknowledgement failed.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Admit one aggregate registration/ack operation before its runtime I/O.
    ///
    /// # Errors
    ///
    /// Returns [`AggregateTransactionError::AdmissionClosed`] after commit or
    /// abort begins.
    pub fn admit(&mut self) -> Result<TransactionOperationId, AggregateTransactionError> {
        if self.state != AggregateTransactionState::Open {
            return Err(AggregateTransactionError::AdmissionClosed {
                txn_id: self.txn_id,
            });
        }
        let operation = TransactionOperationId(self.next_operation);
        self.next_operation = self
            .next_operation
            .checked_add(1)
            .ok_or(AggregateTransactionError::OperationIdExhausted)?;
        self.pending.insert(operation);
        Ok(operation)
    }

    /// Settle one admitted operation. Failure permanently poisons commit.
    ///
    /// # Errors
    ///
    /// Returns [`AggregateTransactionError::UnknownOperation`] for duplicate or
    /// foreign results.
    pub fn settle(
        &mut self,
        operation: TransactionOperationId,
        succeeded: bool,
    ) -> Result<(), AggregateTransactionError> {
        if !self.pending.remove(&operation) {
            return Err(AggregateTransactionError::UnknownOperation { operation });
        }
        if !succeeded {
            self.poisoned = true;
        }
        Ok(())
    }

    /// Atomically close admission for commit and report readiness.
    ///
    /// # Errors
    ///
    /// Returns [`AggregateTransactionError::InvalidTransition`] when commit was
    /// already issued or the transaction terminated.
    pub fn begin_commit(&mut self) -> Result<TransactionDecision, AggregateTransactionError> {
        match self.state {
            AggregateTransactionState::Open => {
                self.state = AggregateTransactionState::CommitClosing;
            }
            AggregateTransactionState::CommitClosing => {}
            state => return Err(AggregateTransactionError::InvalidTransition { state }),
        }
        Ok(self.decision())
    }

    /// Close admission for abort. A poisoned commit-closing transaction may
    /// still switch to abort, as required by the aggregate contract.
    ///
    /// # Errors
    ///
    /// Returns [`AggregateTransactionError::InvalidTransition`] after a final
    /// outcome or after a non-poisoned commit became authoritative.
    pub fn begin_abort(&mut self) -> Result<TransactionDecision, AggregateTransactionError> {
        match self.state {
            AggregateTransactionState::Open | AggregateTransactionState::AbortClosing => {
                self.state = AggregateTransactionState::AbortClosing;
            }
            AggregateTransactionState::CommitClosing if self.poisoned => {
                self.state = AggregateTransactionState::AbortClosing;
            }
            state => return Err(AggregateTransactionError::InvalidTransition { state }),
        }
        Ok(self.decision())
    }

    /// Re-evaluate readiness after an admitted result settles.
    #[must_use]
    pub fn decision(&mut self) -> TransactionDecision {
        if !self.pending.is_empty() {
            return TransactionDecision::Wait {
                pending: self.pending.len(),
            };
        }
        match self.state {
            AggregateTransactionState::CommitClosing if self.poisoned => {
                TransactionDecision::TransactionPoisoned
            }
            AggregateTransactionState::CommitClosing => {
                self.state = AggregateTransactionState::CommitIssued;
                TransactionDecision::IssueCommit
            }
            AggregateTransactionState::AbortClosing => {
                self.state = AggregateTransactionState::AbortIssued;
                TransactionDecision::IssueAbort
            }
            _ => TransactionDecision::Wait { pending: 0 },
        }
    }

    /// Record the transaction coordinator's final result.
    ///
    /// # Errors
    ///
    /// Refuses a final outcome while admitted work remains or from the wrong
    /// closing state.
    pub fn finish(
        &mut self,
        outcome: AggregateTransactionState,
    ) -> Result<(), AggregateTransactionError> {
        if !self.pending.is_empty() {
            return Err(AggregateTransactionError::InvalidTransition { state: self.state });
        }
        if self.state == AggregateTransactionState::CommitClosing && self.poisoned {
            return Err(AggregateTransactionError::InvalidTransition { state: self.state });
        }
        let valid = matches!(
            self.state,
            AggregateTransactionState::CommitIssued | AggregateTransactionState::AbortIssued
        ) && matches!(
            outcome,
            AggregateTransactionState::Committed
                | AggregateTransactionState::Aborted
                | AggregateTransactionState::Unknown
        );
        if !valid {
            return Err(AggregateTransactionError::InvalidTransition { state: self.state });
        }
        self.state = outcome;
        Ok(())
    }
}

/// Why an attached child currently receives no FLOW.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowBlock {
    /// Locally-owned ancestors have not completed.
    Predecessors(Vec<SegmentId>),
    /// Strict or pruned ancestry cannot be proven.
    OrderingUnprovable(Vec<SegmentId>),
    /// Aggregate receive budget cannot reserve a max-size frame.
    Budget,
}

/// Why one manually controlled child permit was issued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowPurpose {
    /// Admit one ordinary complete-message dispatch.
    Message,
    /// Admit only the next frame needed by an already-reserved chunk assembly.
    ChunkContinuation {
        /// Reservation covering the complete announced assembly and workspace.
        assembly: BudgetReservationId,
    },
}

/// Runtime-visible child lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentPhase {
    /// Ordinary child subscribe is in flight.
    Opening,
    /// Child is open but deliberately has no FLOW.
    OpenBlocked(FlowBlock),
    /// Child has one outstanding max-frame FLOW reservation.
    Flowing,
    /// Child observed terminal and will receive no further FLOW.
    Terminal,
    /// Aggregate seek is in flight for this child.
    Seeking,
    /// Child failed locally and awaits confirmation-bearing close.
    Failed,
    /// Ownership was lost; only existing deliveries/acks may settle.
    Draining,
    /// Confirmation-bearing ordinary close is in flight.
    Closing,
}

/// Deterministic runtime work emitted by the aggregate model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamConsumerAction {
    /// Open an ordinary Exclusive child, without FLOW yet.
    OpenChild {
        /// Source to subscribe.
        source: SegmentSource,
        /// Controller connection whose assignment authorized this child.
        controller_incarnation: ControllerIncarnation,
        /// Aggregate generation that requested the open.
        aggregate_generation: AggregateGeneration,
        /// Child generation fencing the result.
        child_generation: ChildGeneration,
    },
    /// Ignore/cancel an opening child removed before attach completed.
    CancelOpen {
        /// Source being cancelled.
        source: SegmentSource,
        /// Controller connection that authorized the opening child.
        controller_incarnation: ControllerIncarnation,
        /// Child generation.
        child_generation: ChildGeneration,
    },
    /// Stop new FLOW before draining lost ownership.
    StopFlow {
        /// Source losing ownership.
        source: SegmentSource,
        /// Controller connection that authorized the child.
        controller_incarnation: ControllerIncarnation,
        /// Child generation.
        child_generation: ChildGeneration,
    },
    /// Grant exactly one purpose-fenced child permit.
    GrantFlow {
        /// Eligible source.
        source: SegmentSource,
        /// Controller connection that authorized the child.
        controller_incarnation: ControllerIncarnation,
        /// Child generation.
        child_generation: ChildGeneration,
        /// Max-frame reservation transferred when the authorized frame arrives.
        reservation: BudgetReservationId,
        /// Work this one permit is allowed to advance.
        purpose: FlowPurpose,
    },
    /// Close an attached ordinary child.
    CloseChild {
        /// Source to close.
        source: SegmentSource,
        /// Controller connection that authorized the child.
        controller_incarnation: ControllerIncarnation,
        /// Child generation.
        child_generation: ChildGeneration,
    },
    /// Apply one component of an aggregate position-vector seek.
    SeekChild {
        /// Current source.
        source: SegmentSource,
        /// Controller connection authorizing the child.
        controller_incarnation: ControllerIncarnation,
        /// Child generation.
        child_generation: ChildGeneration,
        /// Canonical source-qualified target, including every ordinary protobuf field.
        stream_message_id: StreamMessageId,
    },
}

/// Aggregate consumer lifecycle, including failure-driven resynchronization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregatePhase {
    /// Assignments, receives, and user operations are accepted.
    Open,
    /// Explicit close is waiting for child confirmations.
    Closing,
    /// Every child close was confirmed and cleanup is complete.
    Closed,
    /// A failed transition fenced authority and requires controller resync.
    ResyncRequired,
}

#[derive(Debug, Clone, Default)]
struct CompletionBarrier {
    terminal: bool,
    deliveries: usize,
    acknowledgements: usize,
    transactional_acknowledgements: usize,
    pre_terminal_reservations: usize,
}

impl CompletionBarrier {
    const fn settled(&self) -> bool {
        self.deliveries == 0
            && self.acknowledgements == 0
            && self.transactional_acknowledgements == 0
            && self.pre_terminal_reservations == 0
    }

    const fn complete(&self) -> bool {
        self.terminal && self.settled()
    }
}

#[derive(Debug, Clone)]
struct ChildState {
    source: SegmentSource,
    controller_incarnation: ControllerIncarnation,
    generation: ChildGeneration,
    phase: SegmentPhase,
    flow_reservation: Option<BudgetReservationId>,
    flow_purpose: Option<FlowPurpose>,
    completion: CompletionBarrier,
    wait_for_flow_drain: bool,
}

impl ChildState {
    const fn owner(&self) -> BudgetReservationOwner {
        BudgetReservationOwner::new(self.source.segment_id(), self.generation)
    }

    fn handoff_settled(&self) -> bool {
        let flow_reservations = usize::from(self.flow_reservation.is_some());
        self.completion.deliveries == 0
            && self.completion.acknowledgements == 0
            && self.completion.transactional_acknowledgements == 0
            && self.completion.pre_terminal_reservations == flow_reservations
    }

    fn drain_settled(&self) -> bool {
        if self.wait_for_flow_drain {
            self.completion.settled()
        } else {
            self.handoff_settled()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LiveDelivery {
    owner: BudgetReservationOwner,
    reservation: BudgetReservationId,
    message_id: crate::MessageId,
}

#[derive(Debug, Clone)]
struct ValidatedDelivery {
    source: SegmentSource,
    child_generation: ChildGeneration,
    message_id: crate::MessageId,
    message_id_bytes: Vec<u8>,
}

/// Opaque live acknowledgement authority. It intentionally implements no
/// serialization trait and cannot be reconstructed from a position value.
#[derive(Debug)]
pub struct DeliveryToken {
    consumer_instance: ConsumerInstanceId,
    controller_incarnation: ControllerIncarnation,
    child_generation: ChildGeneration,
    stream_message_id: StreamMessageId,
    position_vector: PositionVector,
    delivery_epoch: DeliveryEpoch,
    dequeue_sequence: DequeueSequence,
    reservation: BudgetReservationId,
}

impl DeliveryToken {
    /// Source-qualified position value projected from this live token.
    #[must_use]
    pub const fn stream_message_id(&self) -> &StreamMessageId {
        &self.stream_message_id
    }

    /// Delivered-position snapshot at dequeue linearization.
    #[must_use]
    pub const fn position_vector(&self) -> &PositionVector {
        &self.position_vector
    }

    /// Aggregate dequeue sequence, useful for deterministic diagnostics.
    #[must_use]
    pub const fn dequeue_sequence(&self) -> DequeueSequence {
        self.dequeue_sequence
    }
}

/// One validated ordinary-child acknowledgement component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcknowledgementComponent {
    source: SegmentSource,
    child_generation: ChildGeneration,
    message_ids: Vec<crate::MessageId>,
    message_id_bytes: Vec<Vec<u8>>,
    cumulative: bool,
}

impl AcknowledgementComponent {
    /// Exact child source validated by the aggregate model.
    #[must_use]
    pub const fn source(&self) -> &SegmentSource {
        &self.source
    }

    /// Child generation that must execute this component.
    #[must_use]
    pub const fn child_generation(&self) -> ChildGeneration {
        self.child_generation
    }

    /// Ordinary message ids to acknowledge in one child operation.
    #[must_use]
    pub fn message_ids(&self) -> &[crate::MessageId] {
        &self.message_ids
    }

    /// Complete ordinary protobuf ids used for wire acknowledgement.
    pub fn message_id_data(&self) -> Result<Vec<crate::pb::MessageIdData>, StreamPositionError> {
        self.message_id_bytes
            .iter()
            .map(|bytes| {
                crate::pb::MessageIdData::decode(bytes.as_slice())
                    .map_err(|_| StreamPositionError::InvalidOrdinaryId)
            })
            .collect()
    }

    /// Canonical ordinary-id bytes retained by this component.
    #[must_use]
    pub fn message_id_bytes(&self) -> &[Vec<u8>] {
        &self.message_id_bytes
    }

    /// Whether this component uses cumulative child semantics.
    #[must_use]
    pub const fn cumulative(&self) -> bool {
        self.cumulative
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct AcknowledgementOperationId(u64);

/// One-shot authority for a non-transactional aggregate acknowledgement.
///
/// The value is intentionally non-`Clone` and non-serializable. The model
/// retains the operation record; consuming this authority settles it exactly
/// once after runtime I/O.
#[derive(Debug)]
pub struct AcknowledgementAuthority {
    consumer_instance: ConsumerInstanceId,
    delivery_epoch: DeliveryEpoch,
    operation_id: AcknowledgementOperationId,
}

/// Runtime work admitted atomically for one aggregate acknowledgement.
#[derive(Debug)]
pub struct AcknowledgementTransition {
    /// One-shot settlement authority.
    pub authority: AcknowledgementAuthority,
    /// Validated ordinary-child operations.
    pub components: Vec<AcknowledgementComponent>,
}

/// One-shot authority retained across asynchronous transaction completion.
///
/// It carries no serializable delivery material and cannot be cloned. The
/// model-owned pending record is the only source of settlement truth.
#[derive(Debug)]
pub struct TransactionAcknowledgementAuthority {
    consumer_instance: ConsumerInstanceId,
    delivery_epoch: DeliveryEpoch,
    operation_id: AcknowledgementOperationId,
}

/// Runtime work admitted atomically for one transactional acknowledgement.
#[derive(Debug)]
pub struct TransactionAcknowledgementTransition {
    /// One-shot authority retained until commit, abort, or unknown outcome.
    pub authority: TransactionAcknowledgementAuthority,
    /// Validated ordinary-child transaction operations.
    pub components: Vec<AcknowledgementComponent>,
}

/// Confirmed coordinator outcome applied to one transaction authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionAcknowledgementOutcome {
    /// Every admitted child acknowledgement became durable.
    Committed,
    /// The coordinator confirmed abort; delivery leases remain unresolved.
    Aborted,
    /// Durability is unknown; aggregate authority is fenced for resync.
    Unknown,
}

#[derive(Debug, Clone)]
struct PendingAcknowledgement {
    components: Vec<AcknowledgementComponent>,
    deliveries: Vec<(DequeueSequence, SegmentSource)>,
}

/// Runtime-facing immutable aggregate status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamConsumerStatusSnapshot {
    phase: AggregatePhase,
    layout_epoch: u64,
    assigned_segments: usize,
    attached_segments: usize,
    draining_segments: usize,
    pending_ownership: Vec<SegmentSource>,
    ordering_unprovable: Vec<SegmentId>,
    receiver_budget_limit: usize,
    receiver_budget_used: usize,
}

impl StreamConsumerStatusSnapshot {
    /// Aggregate lifecycle.
    #[must_use]
    pub const fn phase(&self) -> AggregatePhase {
        self.phase
    }

    /// Current validated DAG epoch.
    #[must_use]
    pub const fn layout_epoch(&self) -> u64 {
        self.layout_epoch
    }

    /// Sources in the authoritative assignment.
    #[must_use]
    pub const fn assigned_segments(&self) -> usize {
        self.assigned_segments
    }

    /// Ordinary children that completed subscribe.
    #[must_use]
    pub const fn attached_segments(&self) -> usize {
        self.attached_segments
    }

    /// Lost children retained for settlement and close.
    #[must_use]
    pub const fn draining_segments(&self) -> usize {
        self.draining_segments
    }

    /// Gained sources waiting for the old Exclusive child to close.
    #[must_use]
    pub fn pending_ownership(&self) -> &[SegmentSource] {
        &self.pending_ownership
    }

    /// Strict-mode descendants whose ancestry cannot be proved locally.
    #[must_use]
    pub fn ordering_unprovable(&self) -> &[SegmentId] {
        &self.ordering_unprovable
    }

    /// Configured aggregate bytes, including cleanup reserve.
    #[must_use]
    pub const fn receiver_budget_limit(&self) -> usize {
        self.receiver_budget_limit
    }

    /// Data-plane bytes currently reserved or retained.
    #[must_use]
    pub const fn receiver_budget_used(&self) -> usize {
        self.receiver_budget_used
    }
}

/// Successful message-arrival accounting and any immediately-restored FLOW.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrivalTransition {
    /// Reservation now accounting exact retained bytes.
    pub retained: BudgetReservationId,
    /// Usually a replacement one-message FLOW grant when capacity remains.
    pub actions: Vec<StreamConsumerAction>,
}

/// One incomplete chunk frame was retained and another bounded frame may flow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkAssemblyTransition {
    /// Reservation covering retained chunks and final assembly workspace.
    pub assembly: BudgetReservationId,
    /// Exactly one chunk-continuation FLOW when aggregate capacity permits it.
    pub actions: Vec<StreamConsumerAction>,
}

/// Atomically-accounted arrival of every logical message exploded from a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchArrivalTransition {
    /// One exact retained reservation per logical message, in batch order.
    pub retained: Vec<BudgetReservationId>,
    /// Replacement ordinary FLOW after the complete batch is accounted.
    pub actions: Vec<StreamConsumerAction>,
}

/// One aggregate-accounted message ready for application delivery.
#[derive(Debug)]
pub struct StreamQueuedMessage {
    /// Ordinary child message after transformation and expansion.
    pub message: IncomingMessage,
    /// Complete canonical ordinary protobuf id.
    pub message_id_data: crate::pb::MessageIdData,
    /// Reservation transferred to exact retained bytes.
    pub reservation: BudgetReservationId,
}

// Covers the runtime VecDeque slot plus its reservation/live-delivery ledger
// nodes. Dynamic payload, protobuf-id, and source storage are charged exactly
// below. Keep this deliberately conservative across both runtime layouts.
const STREAM_QUEUE_NODE_OVERHEAD: usize = 1024;

fn message_id_data_heap_bytes(message_id: &crate::pb::MessageIdData) -> usize {
    message_id
        .ack_set
        .capacity()
        .saturating_mul(core::mem::size_of::<i64>())
        .saturating_add(
            message_id
                .first_chunk_message_id
                .as_ref()
                .map_or(0, |first| {
                    core::mem::size_of::<crate::pb::MessageIdData>()
                        .saturating_add(message_id_data_heap_bytes(first))
                }),
        )
}

fn queued_message_retained_bytes(
    message: &IncomingMessage,
    message_id: &crate::pb::MessageIdData,
    source: &SegmentSource,
) -> Result<usize, StreamConsumerModelError> {
    message
        .retained_bytes()
        .checked_add(STREAM_QUEUE_NODE_OVERHEAD)
        .and_then(|bytes| bytes.checked_add(source.topic().len()))
        .and_then(|bytes| bytes.checked_add(message_id_data_heap_bytes(message_id)))
        .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)
}

fn prospective_position_heap_bytes(
    delivered: &BTreeMap<SegmentSource, StreamMessageId>,
    source: &SegmentSource,
    message_id: &StreamMessageId,
    replace: bool,
) -> Result<(usize, usize), StreamConsumerModelError> {
    let mut vector = 0usize;
    let mut canonical = 0usize;
    let mut found = false;
    for (existing_source, existing_id) in delivered {
        let selected = if existing_source == source && replace {
            found = true;
            message_id
        } else {
            if existing_source == source {
                found = true;
            }
            existing_id
        };
        vector = vector
            .checked_add(POSITION_COMPONENT_NODE_OVERHEAD)
            .and_then(|bytes| bytes.checked_add(existing_source.topic().len()))
            .and_then(|bytes| bytes.checked_add(selected.ordinary_message_id_bytes().len()))
            .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)?;
        canonical = canonical
            .checked_add(POSITION_COMPONENT_NODE_OVERHEAD)
            .and_then(|bytes| bytes.checked_add(existing_source.topic().len()))
            .and_then(|bytes| bytes.checked_add(selected.source().topic().len()))
            .and_then(|bytes| bytes.checked_add(selected.ordinary_message_id_bytes().len()))
            .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)?;
    }
    if !found {
        vector = vector
            .checked_add(POSITION_COMPONENT_NODE_OVERHEAD)
            .and_then(|bytes| bytes.checked_add(source.topic().len()))
            .and_then(|bytes| bytes.checked_add(message_id.ordinary_message_id_bytes().len()))
            .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)?;
        canonical = canonical
            .checked_add(POSITION_COMPONENT_NODE_OVERHEAD)
            .and_then(|bytes| bytes.checked_add(source.topic().len().saturating_mul(2)))
            .and_then(|bytes| bytes.checked_add(message_id.ordinary_message_id_bytes().len()))
            .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)?;
    }
    Ok((vector, canonical))
}

/// Result of accepting one raw broker entry.
#[derive(Debug)]
pub enum StreamEntryAcceptance {
    /// An incomplete chunk was retained and continuation FLOW was staged.
    Buffered {
        /// Purpose-fenced continuation action.
        actions: Vec<StreamConsumerAction>,
    },
    /// A complete broker entry is ready for runtime transformation.
    Complete(StreamCompleteEntry),
}

/// Fully assembled broker entry awaiting decrypt/decompress and batch expansion.
#[derive(Debug)]
pub struct StreamCompleteEntry {
    message: IncomingMessage,
    message_id_data: crate::pb::MessageIdData,
    ack_set: Vec<i64>,
    dispatch_permits: u32,
    completion: StreamEntryCompletion,
}

impl StreamCompleteEntry {
    /// Projected ordinary id used by the low-level child handle.
    #[must_use]
    pub const fn message_id(&self) -> crate::MessageId {
        self.message.message_id
    }

    /// Complete broker-authored id retained for acknowledgement.
    #[must_use]
    pub const fn message_id_data(&self) -> &crate::pb::MessageIdData {
        &self.message_id_data
    }

    /// Mutable message passed through the runtime's transform pipeline.
    pub fn message_mut(&mut self) -> &mut IncomingMessage {
        &mut self.message
    }

    /// Bytes that must be reserved before decrypt/decompress can allocate.
    pub fn transform_reservation_bytes(&self) -> Result<usize, StreamConsumerModelError> {
        let encrypted = if self.message.metadata.encryption_keys.is_empty() {
            0
        } else {
            self.message.payload.len()
        };
        let compressed = self
            .message
            .metadata
            .compression
            .and_then(|kind| crate::pb::CompressionType::try_from(kind).ok())
            .filter(|kind| *kind != crate::pb::CompressionType::None)
            .map_or(Ok(0), |kind| {
                let output = self
                    .message
                    .metadata
                    .uncompressed_size
                    .map_or(self.message.payload.len(), |size| size as usize)
                    .checked_add(DECOMPRESSION_VALIDATION_SLACK)
                    .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)?;
                let workspace = match kind {
                    crate::pb::CompressionType::Zlib => ZLIB_DECOMPRESSION_WORKSPACE,
                    crate::pb::CompressionType::Zstd => {
                        let advertised = output.saturating_sub(DECOMPRESSION_VALIDATION_SLACK);
                        let window = advertised
                            .max(ZSTD_MIN_WINDOW_SIZE)
                            .checked_next_power_of_two()
                            .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)?;
                        ZSTD_DECOMPRESSION_CONTEXT_WORKSPACE
                            .checked_add(window)
                            .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)?
                    }
                    crate::pb::CompressionType::None
                    | crate::pb::CompressionType::Lz4
                    | crate::pb::CompressionType::Snappy => 0,
                };
                output
                    .checked_add(workspace)
                    .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)
            })?;
        encrypted
            .checked_add(compressed)
            .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)
    }
}

#[derive(Debug)]
enum StreamEntryCompletion {
    Message {
        flow: BudgetReservationId,
    },
    Chunk {
        flow: BudgetReservationId,
        assembly: BudgetReservationId,
    },
}

/// Complete processing result for one broker entry.
#[derive(Debug)]
pub struct StreamEntryTransition {
    /// Logical messages in broker order.
    pub messages: Vec<StreamQueuedMessage>,
    /// FLOW/close actions resulting from exact accounting.
    pub actions: Vec<StreamConsumerAction>,
    /// Batch permits already spent by the broker beyond the one aggregate
    /// frame reservation. These are repaid with the next bounded FLOW.
    pub permit_debt: u32,
}

#[derive(Debug)]
struct PendingStreamChunk {
    uuid: String,
    expected_chunks: i32,
    total_size: usize,
    payloads: BTreeMap<i32, Bytes>,
    first_metadata: std::sync::Arc<crate::pb::MessageMetadata>,
    first_message_id: crate::pb::MessageIdData,
    broker_entry_metadata: Option<std::sync::Arc<crate::pb::BrokerEntryMetadata>>,
    redelivery_count: u32,
    arrived_at: std::time::Instant,
    assembly: BudgetReservationId,
}

impl PendingStreamChunk {
    fn buffered_bytes(&self) -> usize {
        self.payloads
            .values()
            .fold(0usize, |total, payload| total.saturating_add(payload.len()))
    }
}

/// Aggregate-owned chunk state. One purpose-fenced chain may be live per child.
#[derive(Debug, Default)]
pub struct StreamReceiveState {
    chunks: BTreeMap<BudgetReservationOwner, PendingStreamChunk>,
}

impl StreamReceiveState {
    /// Forget receive work for a child after confirmation-bearing close.
    pub fn remove_child(&mut self, segment_id: SegmentId, generation: ChildGeneration) {
        self.chunks
            .remove(&BudgetReservationOwner::new(segment_id, generation));
    }

    /// Accept one raw broker entry before any chunk retention.
    pub fn accept_entry(
        &mut self,
        model: &mut StreamConsumerModel,
        segment_id: SegmentId,
        generation: ChildGeneration,
        flow: BudgetReservationId,
        entry: DeferredIncomingMessage,
    ) -> Result<StreamEntryAcceptance, StreamConsumerModelError> {
        let Some(total_chunks) = entry.message.metadata.num_chunks_from_msg else {
            return Ok(StreamEntryAcceptance::Complete(StreamCompleteEntry {
                message: entry.message,
                message_id_data: entry.message_id_data,
                ack_set: entry.ack_set,
                dispatch_permits: entry.dispatch_permits,
                completion: StreamEntryCompletion::Message { flow },
            }));
        };
        if total_chunks <= 1 {
            return Ok(StreamEntryAcceptance::Complete(StreamCompleteEntry {
                message: entry.message,
                message_id_data: entry.message_id_data,
                ack_set: entry.ack_set,
                dispatch_permits: entry.dispatch_permits,
                completion: StreamEntryCompletion::Message { flow },
            }));
        }
        if total_chunks > MAX_CHUNK_TOTAL {
            return Err(StreamConsumerModelError::InvalidChunkFrame(
                "chunk count exceeds the fixed bound",
            ));
        }
        if entry.message.metadata.num_messages_in_batch.unwrap_or(1) > 1 {
            return Err(StreamConsumerModelError::InvalidChunkFrame(
                "chunked entries cannot also be batched",
            ));
        }
        let chunk_id =
            entry
                .message
                .metadata
                .chunk_id
                .ok_or(StreamConsumerModelError::InvalidChunkFrame(
                    "chunk id is absent",
                ))?;
        if chunk_id < 0 || chunk_id >= total_chunks {
            return Err(StreamConsumerModelError::InvalidChunkFrame(
                "chunk id is outside the announced range",
            ));
        }
        let total_size = entry
            .message
            .metadata
            .total_chunk_msg_size
            .and_then(|size| usize::try_from(size).ok())
            .filter(|size| *size > 0)
            .ok_or(StreamConsumerModelError::InvalidChunkFrame(
                "total chunk message size is absent or invalid",
            ))?;
        let uuid = entry.message.metadata.uuid.clone().ok_or(
            StreamConsumerModelError::InvalidChunkFrame("chunk uuid is absent"),
        )?;
        let owner = BudgetReservationOwner::new(segment_id, generation);
        let existing = self.chunks.get(&owner);
        if existing.is_none() && chunk_id != 0 {
            return Err(StreamConsumerModelError::InvalidChunkFrame(
                "a chunk chain must begin with chunk zero",
            ));
        }
        if let Some(existing) = existing {
            if existing.uuid != uuid
                || existing.expected_chunks != total_chunks
                || existing.total_size != total_size
            {
                return Err(StreamConsumerModelError::InvalidChunkFrame(
                    "chunk continuation does not match its reserved chain",
                ));
            }
            if existing.payloads.contains_key(&chunk_id) {
                return Err(StreamConsumerModelError::InvalidChunkFrame(
                    "duplicate chunk id",
                ));
            }
        }
        let received = existing.map_or(0, |chunk| chunk.payloads.len());
        let complete = received.saturating_add(1)
            == usize::try_from(total_chunks)
                .map_err(|_| StreamConsumerModelError::ReceiveSizeOverflow)?;
        if !complete {
            let buffered_after = existing
                .map_or(0, PendingStreamChunk::buffered_bytes)
                .checked_add(entry.message.payload.len())
                .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)?;
            if buffered_after > total_size {
                return Err(StreamConsumerModelError::InvalidChunkFrame(
                    "buffered chunks exceed the announced message size",
                ));
            }
            let allocation_bytes = buffered_after
                .checked_add(total_size)
                .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)?;
            let current_assembly = existing.map(|chunk| chunk.assembly);
            let transition = model.chunk_frame_buffered(
                segment_id,
                generation,
                flow,
                current_assembly,
                allocation_bytes,
            )?;
            if let Some(chunk) = self.chunks.get_mut(&owner) {
                chunk.payloads.insert(chunk_id, entry.message.payload);
                chunk.assembly = transition.assembly;
            } else {
                let mut payloads = BTreeMap::new();
                payloads.insert(chunk_id, entry.message.payload);
                self.chunks.insert(
                    owner,
                    PendingStreamChunk {
                        uuid,
                        expected_chunks: total_chunks,
                        total_size,
                        payloads,
                        first_metadata: entry.message.metadata,
                        first_message_id: entry.message_id_data,
                        broker_entry_metadata: entry.message.broker_entry_metadata,
                        redelivery_count: entry.message.redelivery_count,
                        arrived_at: entry.message.arrived_at,
                        assembly: transition.assembly,
                    },
                );
            }
            return Ok(StreamEntryAcceptance::Buffered {
                actions: transition.actions,
            });
        }

        let chunk = self
            .chunks
            .get(&owner)
            .ok_or(StreamConsumerModelError::InvalidChunkFrame(
                "final chunk has no reserved assembly",
            ))?;
        let assembled_size = chunk
            .buffered_bytes()
            .checked_add(entry.message.payload.len())
            .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)?;
        if assembled_size != total_size {
            return Err(StreamConsumerModelError::InvalidChunkFrame(
                "completed chunks do not match the announced message size",
            ));
        }
        let mut chunk =
            self.chunks
                .remove(&owner)
                .ok_or(StreamConsumerModelError::InvalidChunkFrame(
                    "final chunk assembly disappeared",
                ))?;
        chunk.payloads.insert(chunk_id, entry.message.payload);
        let mut full = BytesMut::with_capacity(total_size);
        for payload in chunk.payloads.into_values() {
            full.extend_from_slice(&payload);
        }
        let mut metadata = chunk.first_metadata;
        {
            let metadata = std::sync::Arc::make_mut(&mut metadata);
            metadata.num_chunks_from_msg = None;
            metadata.chunk_id = None;
            metadata.total_chunk_msg_size = None;
        }
        let mut message_id_data = entry.message_id_data;
        message_id_data.first_chunk_message_id = Some(Box::new(chunk.first_message_id));
        let message = IncomingMessage {
            message_id: crate::MessageId::from_pb(&message_id_data),
            metadata,
            single_metadata: None,
            payload: full.freeze(),
            redelivery_count: chunk.redelivery_count,
            broker_entry_metadata: chunk.broker_entry_metadata,
            arrived_at: chunk.arrived_at,
        };
        Ok(StreamEntryAcceptance::Complete(StreamCompleteEntry {
            message,
            message_id_data,
            ack_set: Vec::new(),
            dispatch_permits: entry.dispatch_permits,
            completion: StreamEntryCompletion::Chunk {
                flow,
                assembly: chunk.assembly,
            },
        }))
    }

    /// Expand and account a transformed complete entry before it becomes
    /// visible to aggregate receive.
    pub fn finalize_entry(
        &mut self,
        model: &mut StreamConsumerModel,
        segment_id: SegmentId,
        generation: ChildGeneration,
        entry: StreamCompleteEntry,
        transform_work: &[BudgetReservationId],
    ) -> Result<StreamEntryTransition, StreamConsumerModelError> {
        let source = model.child(segment_id, generation)?.source.clone();
        let count = entry.message.metadata.num_messages_in_batch.unwrap_or(1);
        if count > 1 {
            let StreamEntryCompletion::Message { flow } = entry.completion else {
                return Err(StreamConsumerModelError::InvalidBatchFrame(
                    "chunked entries cannot also be batched",
                ));
            };
            let count = usize::try_from(count)
                .map_err(|_| StreamConsumerModelError::InvalidBatchFrame("negative batch size"))?;
            let repeated_id_heap = message_id_data_heap_bytes(&entry.message_id_data)
                .saturating_sub(
                    entry
                        .message_id_data
                        .ack_set
                        .capacity()
                        .saturating_mul(core::mem::size_of::<i64>()),
                )
                .checked_add(
                    entry
                        .message_id_data
                        .ack_set
                        .capacity()
                        .max(entry.ack_set.len())
                        .checked_mul(core::mem::size_of::<i64>())
                        .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)?,
                )
                .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)?;
            let per_member = core::mem::size_of::<IncomingMessage>()
                .checked_add(core::mem::size_of::<crate::pb::SingleMessageMetadata>())
                .and_then(|bytes| bytes.checked_add(STREAM_QUEUE_NODE_OVERHEAD))
                .and_then(|bytes| bytes.checked_add(source.topic().len()))
                .and_then(|bytes| bytes.checked_add(repeated_id_heap))
                .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)?;
            let structural = entry
                .message
                .payload
                .len()
                .checked_add(
                    count
                        .checked_mul(per_member)
                        .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)?,
                )
                .ok_or(StreamConsumerModelError::ReceiveSizeOverflow)?;
            let mut staged = model.clone();
            let batch_work = staged.reserve_batch_assembly(segment_id, generation, structural)?;
            let messages =
                expand_batch(entry.message, entry.message_id_data, entry.ack_set, count)?;
            let retained_bytes: Vec<_> = messages
                .iter()
                .map(|(message, message_id)| {
                    queued_message_retained_bytes(message, message_id, &source)
                })
                .collect::<Result<_, _>>()?;
            let mut work = transform_work.to_vec();
            work.push(batch_work);
            if messages.is_empty() {
                return staged
                    .discard_preallocated_arrival(
                        segment_id,
                        generation,
                        flow,
                        FlowPurpose::Message,
                        &work,
                    )
                    .map(|actions| {
                        *model = staged;
                        StreamEntryTransition {
                            messages: Vec::new(),
                            actions,
                            permit_debt: entry.dispatch_permits.saturating_sub(1),
                        }
                    });
            }
            return staged
                .batch_arrived_preallocated(segment_id, generation, flow, &work, &retained_bytes)
                .map(|transition| {
                    let messages = messages
                        .into_iter()
                        .zip(transition.retained)
                        .map(
                            |((message, message_id_data), reservation)| StreamQueuedMessage {
                                message,
                                message_id_data,
                                reservation,
                            },
                        )
                        .collect();
                    *model = staged;
                    StreamEntryTransition {
                        messages,
                        actions: transition.actions,
                        permit_debt: entry.dispatch_permits.saturating_sub(1),
                    }
                });
        }

        let retained_bytes =
            queued_message_retained_bytes(&entry.message, &entry.message_id_data, &source)?;
        let mut staged = model.clone();
        let transition = match entry.completion {
            StreamEntryCompletion::Message { flow } => staged.message_arrived_preallocated(
                segment_id,
                generation,
                flow,
                transform_work,
                retained_bytes,
            ),
            StreamEntryCompletion::Chunk { flow, assembly } => staged.chunk_message_arrived(
                segment_id,
                generation,
                flow,
                assembly,
                transform_work,
                retained_bytes,
            ),
        };
        transition.map(|transition| {
            let message = StreamQueuedMessage {
                message: entry.message,
                message_id_data: entry.message_id_data,
                reservation: transition.retained,
            };
            *model = staged;
            StreamEntryTransition {
                messages: vec![message],
                actions: transition.actions,
                permit_debt: entry.dispatch_permits.saturating_sub(1),
            }
        })
    }

    /// Release a complete entry that the runtime's crypto policy discarded.
    pub fn discard_entry(
        &mut self,
        model: &mut StreamConsumerModel,
        segment_id: SegmentId,
        generation: ChildGeneration,
        entry: StreamCompleteEntry,
        transform_work: &[BudgetReservationId],
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        match entry.completion {
            StreamEntryCompletion::Message { flow } => model.discard_preallocated_arrival(
                segment_id,
                generation,
                flow,
                FlowPurpose::Message,
                transform_work,
            ),
            StreamEntryCompletion::Chunk { flow, assembly } => {
                let mut work = Vec::with_capacity(transform_work.len().saturating_add(1));
                work.push(assembly);
                work.extend_from_slice(transform_work);
                model.discard_preallocated_arrival(
                    segment_id,
                    generation,
                    flow,
                    FlowPurpose::ChunkContinuation { assembly },
                    &work,
                )
            }
        }
    }
}

fn expand_batch(
    message: IncomingMessage,
    message_id_data: crate::pb::MessageIdData,
    ack_set: Vec<i64>,
    count: usize,
) -> Result<Vec<(IncomingMessage, crate::pb::MessageIdData)>, StreamConsumerModelError> {
    let shared_metadata = message.metadata;
    let shared_broker_metadata = message.broker_entry_metadata;
    let mut cursor = message.payload;
    let mut messages = Vec::with_capacity(count);
    let batch_size =
        i32::try_from(count).map_err(|_| StreamConsumerModelError::ReceiveSizeOverflow)?;
    let ack_state = crate::consumer::BatchAckEntry::from_ack_set(batch_size, &ack_set);
    let effective_ack_set = ack_state.ack_set_i64();
    for index in 0..count {
        if cursor.remaining() < 4 {
            return Err(StreamConsumerModelError::InvalidBatchFrame(
                "single-message metadata length is truncated",
            ));
        }
        let single_size = cursor.get_u32() as usize;
        if cursor.remaining() < single_size {
            return Err(StreamConsumerModelError::InvalidBatchFrame(
                "single-message metadata is truncated",
            ));
        }
        let single = crate::pb::SingleMessageMetadata::decode(cursor.split_to(single_size))
            .map_err(|_| {
                StreamConsumerModelError::InvalidBatchFrame(
                    "single-message metadata is not valid protobuf",
                )
            })?;
        let payload_size = usize::try_from(single.payload_size).map_err(|_| {
            StreamConsumerModelError::InvalidBatchFrame("single-message payload size is negative")
        })?;
        if cursor.remaining() < payload_size {
            return Err(StreamConsumerModelError::InvalidBatchFrame(
                "single-message payload is truncated",
            ));
        }
        let payload = cursor.split_to(payload_size);
        if ack_state.is_unacked(index) {
            let mut ordinary = message_id_data.clone();
            ordinary.batch_index = Some(
                i32::try_from(index).map_err(|_| StreamConsumerModelError::ReceiveSizeOverflow)?,
            );
            ordinary.batch_size = Some(batch_size);
            ordinary.ack_set.clone_from(&effective_ack_set);
            let incoming = IncomingMessage {
                message_id: crate::MessageId::from_pb(&ordinary),
                metadata: shared_metadata.clone(),
                single_metadata: Some(single),
                payload: Bytes::copy_from_slice(&payload),
                redelivery_count: message.redelivery_count,
                broker_entry_metadata: shared_broker_metadata.clone(),
                arrived_at: message.arrived_at,
            };
            messages.push((incoming, ordinary));
        }
    }
    if cursor.has_remaining() {
        return Err(StreamConsumerModelError::InvalidBatchFrame(
            "batch payload has trailing bytes",
        ));
    }
    Ok(messages)
}

/// Whether an arrival-accounting failure may be retried after capacity changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrivalFailureDisposition {
    /// Aggregate capacity pressure may clear after another reservation settles.
    Retryable,
    /// The same arrival cannot fit or violated an accounting invariant.
    Permanent,
}

/// Aggregate lifecycle or authority transition failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamConsumerModelError {
    /// Parent scalable topic is not canonical.
    #[error(transparent)]
    SegmentTopic(#[from] SegmentTopicError),
    /// Assignment cannot attach to the current graph.
    #[error(transparent)]
    Attachment(#[from] AttachmentError),
    /// Assignment callback failed incarnation or wire-order validation.
    #[error(transparent)]
    Assignment(#[from] AssignmentError),
    /// Receive-budget operation failed.
    #[error(transparent)]
    Budget(#[from] BudgetError),
    /// Position projection failed.
    #[error(transparent)]
    Position(#[from] StreamPositionError),
    /// Arrival accounting failed after the issued permit was terminalized.
    #[error("message arrival accounting failed ({disposition:?}): {error}")]
    ArrivalAccountingFailed {
        /// Budget failure that rejected retention.
        error: BudgetError,
        /// Whether a fresh child may retry after capacity changes.
        disposition: ArrivalFailureDisposition,
        /// Deterministic close/refill work resulting from terminalization.
        actions: Vec<StreamConsumerAction>,
    },
    /// An otherwise canonical assignment names another parent topic.
    #[error("assignment segment {segment_id} belongs to {got:?}, expected {expected:?}")]
    AssignmentParentMismatch {
        /// Segment id.
        segment_id: SegmentId,
        /// Parent encoded by the assigned source.
        got: String,
        /// Aggregate parent.
        expected: String,
    },
    /// Segment has no live child in the aggregate.
    #[error("unknown aggregate child segment {segment_id}")]
    UnknownChild {
        /// Missing segment.
        segment_id: SegmentId,
    },
    /// Runtime result belongs to an older child generation.
    #[error("stale child generation {got:?}; expected {expected:?} for segment {segment_id}")]
    StaleChildGeneration {
        /// Segment id.
        segment_id: SegmentId,
        /// Runtime result generation.
        got: ChildGeneration,
        /// Current generation.
        expected: ChildGeneration,
    },
    /// Child lifecycle does not admit the callback.
    #[error("invalid child transition for segment {segment_id} from {phase:?}")]
    InvalidChildTransition {
        /// Segment id.
        segment_id: SegmentId,
        /// Current phase.
        phase: SegmentPhase,
    },
    /// A complete message arrived under a permit reserved only for chunk continuation.
    #[error(
        "flow purpose mismatch for segment {segment_id}: got {actual:?}, expected {expected:?}"
    )]
    FlowPurposeMismatch {
        /// Segment receiving the callback.
        segment_id: SegmentId,
        /// Purpose attached to the live permit.
        actual: Option<FlowPurpose>,
        /// Purpose required by the callback.
        expected: FlowPurpose,
    },
    /// Reservation is not a preallocated chunk, decompression, or batch workspace.
    #[error("reservation {reservation:?} has non-work use {use_:?}")]
    InvalidReceiveWork {
        /// Reservation supplied by the runtime.
        reservation: BudgetReservationId,
        /// Current accounting class.
        use_: BudgetUse,
    },
    /// Wire-derived allocation arithmetic exceeded `usize`.
    #[error("stream receive allocation size overflow")]
    ReceiveSizeOverflow,
    /// A purpose-fenced chunk chain was malformed.
    #[error("invalid scalable chunk frame: {0}")]
    InvalidChunkFrame(&'static str),
    /// A broker batch could not be expanded exactly as announced.
    #[error("invalid scalable batch frame: {0}")]
    InvalidBatchFrame(&'static str),
    /// A completion listed the same preallocated workspace more than once.
    #[error("receive work repeats reservation {reservation:?}")]
    DuplicateReceiveWork {
        /// Repeated reservation.
        reservation: BudgetReservationId,
    },
    /// Completion was requested before all barriers settled.
    #[error("segment {segment_id} is not terminal and fully settled")]
    SegmentNotComplete {
        /// Segment id.
        segment_id: SegmentId,
    },
    /// Counter result arrived without a matching admitted operation.
    #[error("segment {segment_id} has no pending {kind}")]
    UnbalancedCompletionHook {
        /// Segment id.
        segment_id: SegmentId,
        /// Hook category.
        kind: &'static str,
    },
    /// A completion counter cannot advance further.
    #[error("segment {segment_id} {kind} counter exhausted")]
    CompletionCounterExhausted {
        /// Segment id.
        segment_id: SegmentId,
        /// Counter category.
        kind: &'static str,
    },
    /// Token belongs to another consumer, incarnation, generation, or epoch.
    #[error("delivery token is stale or foreign")]
    StaleDeliveryToken,
    /// Delivery already belongs to an admitted acknowledgement operation.
    #[error("delivery already has a pending acknowledgement operation")]
    DeliveryOperationPending,
    /// One-shot acknowledgement settlement authority is stale or foreign.
    #[error("acknowledgement authority is stale or foreign")]
    StaleAcknowledgementAuthority,
    /// A restored position names a source that is not currently addressable.
    #[error("position source {segment_source:?} is not a current aggregate child")]
    PositionSourceUnavailable {
        /// Unavailable source.
        segment_source: SegmentSource,
    },
    /// A restored acknowledgement vector belongs to another DAG layout.
    #[error("position vector layout epoch {vector} does not match DAG epoch {dag}")]
    PositionLayoutMismatch {
        /// Vector epoch.
        vector: u64,
        /// Current DAG epoch.
        dag: u64,
    },
    /// A prevalidated stream position names another child source.
    #[error("delivery source {got:?} does not match child source {expected:?}")]
    DeliverySourceMismatch {
        /// Source carried by the stream position.
        got: SegmentSource,
        /// Source owned by the child generation.
        expected: SegmentSource,
    },
    /// Aggregate or child generation id cannot advance further.
    #[error("stream-consumer generation exhausted")]
    GenerationExhausted,
    /// Aggregate dequeue sequence cannot advance further.
    #[error("stream-consumer dequeue sequence exhausted")]
    DequeueSequenceExhausted,
    /// Aggregate acknowledgement operation ids cannot advance further.
    #[error("stream-consumer acknowledgement operation id exhausted")]
    AcknowledgementOperationExhausted,
    /// Aggregate lifecycle does not admit the operation.
    #[error("stream consumer is in {phase:?} state")]
    InvalidAggregatePhase {
        /// Current lifecycle.
        phase: AggregatePhase,
    },
    /// A seek cannot race retained messages, deliveries, acknowledgements, or
    /// transactional work.
    #[error("aggregate seek has concurrent data-plane work")]
    ConcurrentSeek,
    /// Position vector belongs to another layout.
    #[error("seek vector layout epoch {vector} does not match DAG epoch {dag}")]
    SeekLayoutMismatch {
        /// Vector epoch.
        vector: u64,
        /// Current DAG epoch.
        dag: u64,
    },
    /// Seek vector sources differ from the currently assigned active leaves.
    #[error("seek vector does not exactly cover current assigned sources")]
    SeekSourceMismatch,
    /// A seek component names a non-active or non-leaf segment.
    #[error("seek segment {segment_id} is not an active leaf")]
    SeekNonActiveLeaf {
        /// Invalid segment.
        segment_id: SegmentId,
    },
}

/// Pure state packet consumed by later Tokio and Moonpool runtime ports.
#[derive(Debug, Clone)]
pub struct StreamConsumerModel {
    parent_topic: String,
    consumer_instance: ConsumerInstanceId,
    controller_incarnation: ControllerIncarnation,
    generation: AggregateGeneration,
    delivery_epoch: DeliveryEpoch,
    next_child_generation: u64,
    next_dequeue_sequence: u64,
    next_acknowledgement_operation: u64,
    ordering_mode: OrderingMode,
    dag: DagSnapshot,
    assignment: Option<ConsumerAssignment>,
    children: BTreeMap<SegmentId, ChildState>,
    pending_ownership: BTreeMap<SegmentId, SegmentSource>,
    ownership_history: BTreeSet<SegmentId>,
    completed: BTreeSet<SegmentId>,
    delivered_positions: BTreeMap<SegmentSource, StreamMessageId>,
    delivered_position: PositionVector,
    delivered_positions_reservation: Option<BudgetReservationId>,
    live_deliveries: BTreeMap<DequeueSequence, LiveDelivery>,
    pending_acknowledgements: BTreeMap<AcknowledgementOperationId, PendingAcknowledgement>,
    pending_transaction_acknowledgements:
        BTreeMap<AcknowledgementOperationId, PendingAcknowledgement>,
    budget: ReceiverBudgetState,
    phase: AggregatePhase,
    flow_cursor: Option<SegmentId>,
}

impl StreamConsumerModel {
    /// Construct an aggregate around one validated DAG snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentTopicError`] if `parent_topic` is not canonical.
    pub fn new(
        parent_topic: String,
        consumer_instance: ConsumerInstanceId,
        controller_incarnation: ControllerIncarnation,
        ordering_mode: OrderingMode,
        dag: DagSnapshot,
        budget: ReceiverBudget,
    ) -> Result<Self, StreamConsumerModelError> {
        // Reuse the one canonical constructor rather than maintaining a second
        // parent-topic parser.
        let _ = canonical_segment_topic(&parent_topic, KeyRange::FULL, SegmentId(0))?;
        let delivered_position = PositionVector::new(dag.epoch(), std::iter::empty())?;
        Ok(Self {
            parent_topic,
            consumer_instance,
            controller_incarnation,
            generation: AggregateGeneration(0),
            delivery_epoch: DeliveryEpoch(0),
            next_child_generation: 0,
            next_dequeue_sequence: 0,
            next_acknowledgement_operation: 0,
            ordering_mode,
            dag,
            assignment: None,
            children: BTreeMap::new(),
            pending_ownership: BTreeMap::new(),
            ownership_history: BTreeSet::new(),
            completed: BTreeSet::new(),
            delivered_positions: BTreeMap::new(),
            delivered_position,
            delivered_positions_reservation: None,
            live_deliveries: BTreeMap::new(),
            pending_acknowledgements: BTreeMap::new(),
            pending_transaction_acknowledgements: BTreeMap::new(),
            budget: ReceiverBudgetState::new(budget),
            phase: AggregatePhase::Open,
            flow_cursor: None,
        })
    }

    /// Current aggregate generation.
    #[must_use]
    pub const fn generation(&self) -> AggregateGeneration {
        self.generation
    }

    /// Current delivery-authority epoch.
    #[must_use]
    pub const fn delivery_epoch(&self) -> DeliveryEpoch {
        self.delivery_epoch
    }

    /// Current aggregate lifecycle.
    #[must_use]
    pub const fn phase(&self) -> AggregatePhase {
        self.phase
    }

    /// Current child phase.
    #[must_use]
    pub fn segment_phase(&self, segment_id: SegmentId) -> Option<&SegmentPhase> {
        self.children.get(&segment_id).map(|child| &child.phase)
    }

    /// Read-only aggregate budget state.
    #[must_use]
    pub const fn budget(&self) -> &ReceiverBudgetState {
        &self.budget
    }

    /// Current local controller-connection incarnation.
    #[must_use]
    pub const fn controller_incarnation(&self) -> ControllerIncarnation {
        self.controller_incarnation
    }

    /// Current atomically validated DAG snapshot.
    #[must_use]
    pub const fn dag(&self) -> &DagSnapshot {
        &self.dag
    }

    /// Current authoritative assignment, once registration has resolved.
    #[must_use]
    pub const fn assignment(&self) -> Option<&ConsumerAssignment> {
        self.assignment.as_ref()
    }

    /// Highest positions delivered to the application at the latest dequeue
    /// linearization point.
    #[must_use]
    pub const fn delivered_position(&self) -> &PositionVector {
        &self.delivered_position
    }

    /// Child generation currently owning `source`, including a draining child.
    #[must_use]
    pub fn child_generation(&self, source: &SegmentSource) -> Option<ChildGeneration> {
        self.children
            .get(&source.segment_id())
            .filter(|child| child.source == *source)
            .map(|child| child.generation)
    }

    /// Whether a delayed runtime result still targets the current child.
    #[must_use]
    pub fn accepts_child_result(
        &self,
        source: &SegmentSource,
        generation: ChildGeneration,
    ) -> bool {
        self.child_generation(source) == Some(generation)
    }

    /// Sources waiting for confirmation-bearing release of an old Exclusive
    /// child.
    #[must_use]
    pub fn pending_ownership(&self) -> Vec<SegmentSource> {
        self.pending_ownership.values().cloned().collect()
    }

    /// Immutable lifecycle/resource snapshot for runtime status surfaces.
    #[must_use]
    pub fn status(&self) -> StreamConsumerStatusSnapshot {
        let assigned_segments = self
            .assignment
            .as_ref()
            .map_or(0, |assignment| assignment.segments().len());
        let attached_segments = self
            .children
            .values()
            .filter(|child| child.phase != SegmentPhase::Opening)
            .count();
        let draining_segments = self
            .children
            .values()
            .filter(|child| {
                matches!(
                    child.phase,
                    SegmentPhase::Draining | SegmentPhase::Closing | SegmentPhase::Failed
                )
            })
            .count();
        let ordering_unprovable = self
            .children
            .iter()
            .filter_map(|(segment_id, child)| {
                matches!(
                    child.phase,
                    SegmentPhase::OpenBlocked(FlowBlock::OrderingUnprovable(_))
                )
                .then_some(*segment_id)
            })
            .collect();
        StreamConsumerStatusSnapshot {
            phase: self.phase,
            layout_epoch: self.dag.epoch(),
            assigned_segments,
            attached_segments,
            draining_segments,
            pending_ownership: self.pending_ownership(),
            ordering_unprovable,
            receiver_budget_limit: self.budget.limit(),
            receiver_budget_used: self.budget.data_used(),
        }
    }

    /// Atomically replace the DAG and assignment for the current controller
    /// incarnation. Children whose descriptor or source changed drain and
    /// reopen through the existing confirmation-bearing close fence.
    ///
    /// # Errors
    ///
    /// Rejects a malformed or mismatched DAG/assignment pair without mutation.
    pub fn apply_control_plane(
        &mut self,
        dag: DagSnapshot,
        assignment: ConsumerAssignment,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.apply_control_plane_for(self.controller_incarnation, dag, assignment)
    }

    /// Incarnation-fenced form of [`Self::apply_control_plane`].
    ///
    /// # Errors
    ///
    /// Rejects delayed state from another controller incarnation and leaves the
    /// current DAG, assignment, children, and budget untouched.
    pub fn apply_control_plane_for(
        &mut self,
        incarnation: ControllerIncarnation,
        dag: DagSnapshot,
        assignment: ConsumerAssignment,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.require_open()?;
        if incarnation != self.controller_incarnation {
            return Err(AssignmentError::IncarnationMismatch {
                got: incarnation,
                expected: self.controller_incarnation,
            }
            .into());
        }

        let replacements: BTreeSet<SegmentId> = self
            .children
            .keys()
            .copied()
            .filter(|segment_id| self.dag.segment(*segment_id) != dag.segment(*segment_id))
            .collect();
        let mut staged = self.clone();
        staged.dag = dag;
        let mut actions = staged.reconcile_assignment(assignment, &replacements)?;
        staged.refresh_delivered_position()?;
        actions.extend(staged.arbitrate_flow()?);
        *self = staged;
        Ok(actions)
    }

    /// Fence a lost controller connection and prepare for a replacement
    /// registration baseline. Existing children remain as close-confirmation
    /// fences; a new assignment records matching sources as pending ownership
    /// until those closes settle.
    ///
    /// # Errors
    ///
    /// The new incarnation must strictly advance, and both aggregate authority
    /// counters must have room to advance atomically.
    pub fn begin_controller_incarnation(
        &mut self,
        incarnation: ControllerIncarnation,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        if !matches!(
            self.phase,
            AggregatePhase::Open | AggregatePhase::ResyncRequired
        ) {
            return Err(StreamConsumerModelError::InvalidAggregatePhase { phase: self.phase });
        }
        if incarnation <= self.controller_incarnation {
            return Err(AssignmentError::NonAdvancingIncarnation {
                got: incarnation,
                prev: self.controller_incarnation,
            }
            .into());
        }
        let generation = self
            .generation
            .0
            .checked_add(1)
            .ok_or(StreamConsumerModelError::GenerationExhausted)?;
        let delivery_epoch = self
            .delivery_epoch
            .0
            .checked_add(1)
            .ok_or(StreamConsumerModelError::GenerationExhausted)?;

        let mut actions = Vec::new();
        for child in self.children.values() {
            match child.phase {
                SegmentPhase::Opening => actions.push(StreamConsumerAction::CancelOpen {
                    source: child.source.clone(),
                    controller_incarnation: child.controller_incarnation,
                    child_generation: child.generation,
                }),
                SegmentPhase::OpenBlocked(_)
                | SegmentPhase::Flowing
                | SegmentPhase::Terminal
                | SegmentPhase::Seeking
                | SegmentPhase::Failed
                | SegmentPhase::Draining => actions.push(StreamConsumerAction::CloseChild {
                    source: child.source.clone(),
                    controller_incarnation: child.controller_incarnation,
                    child_generation: child.generation,
                }),
                SegmentPhase::Closing => {}
            }
        }

        self.clear_delivered_positions()?;
        self.pending_acknowledgements.clear();
        self.pending_transaction_acknowledgements.clear();
        self.controller_incarnation = incarnation;
        self.phase = AggregatePhase::Open;
        self.generation = AggregateGeneration(generation);
        self.delivery_epoch = DeliveryEpoch(delivery_epoch);
        self.assignment = None;
        self.pending_ownership.clear();
        for child in self.children.values_mut() {
            child.phase = SegmentPhase::Closing;
        }
        Ok(actions)
    }

    /// Atomically begin an aggregate vector seek across every currently
    /// assigned active leaf.
    ///
    /// # Errors
    ///
    /// Rejects vectors from another layout, incomplete source sets, non-leaf
    /// assignments, and concurrent retained/delivery/acknowledgement work.
    pub fn begin_seek(
        &mut self,
        vector: &PositionVector,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.require_open()?;
        if vector.layout_epoch() != self.dag.epoch() {
            return Err(StreamConsumerModelError::SeekLayoutMismatch {
                vector: vector.layout_epoch(),
                dag: self.dag.epoch(),
            });
        }
        let assignment = self
            .assignment
            .as_ref()
            .ok_or(StreamConsumerModelError::SeekSourceMismatch)?;
        let positions: BTreeMap<SegmentSource, StreamMessageId> = vector
            .stream_message_ids()
            .map(|stream_message_id| (stream_message_id.source().clone(), stream_message_id))
            .collect();
        let expected: BTreeSet<SegmentSource> = assignment
            .segments()
            .iter()
            .map(crate::scalable_consumer::AssignedSegment::source)
            .collect();
        if positions.len() != expected.len()
            || positions.keys().any(|source| !expected.contains(source))
        {
            return Err(StreamConsumerModelError::SeekSourceMismatch);
        }
        if !self.live_deliveries.is_empty()
            || !self.pending_ownership.is_empty()
            || self
                .children
                .values()
                .any(|child| !expected.contains(&child.source))
        {
            return Err(StreamConsumerModelError::ConcurrentSeek);
        }
        for source in &expected {
            let segment_id = source.segment_id();
            let descriptor = self
                .dag
                .segment(segment_id)
                .ok_or(StreamConsumerModelError::SeekSourceMismatch)?;
            if descriptor.state != SegmentState::Active || !descriptor.child_ids.is_empty() {
                return Err(StreamConsumerModelError::SeekNonActiveLeaf { segment_id });
            }
            let child = self
                .children
                .get(&segment_id)
                .filter(|child| child.source == *source)
                .ok_or(StreamConsumerModelError::SeekSourceMismatch)?;
            let flow_count = usize::from(child.flow_reservation.is_some());
            if child.completion.deliveries != 0
                || child.completion.acknowledgements != 0
                || child.completion.transactional_acknowledgements != 0
                || child.completion.pre_terminal_reservations != flow_count
                || !self.child_has_only_seek_flow_reservation(child)
                || matches!(
                    child.phase,
                    SegmentPhase::Opening
                        | SegmentPhase::Draining
                        | SegmentPhase::Closing
                        | SegmentPhase::Seeking
                        | SegmentPhase::Failed
                )
            {
                return Err(StreamConsumerModelError::ConcurrentSeek);
            }
        }
        let generation = self
            .generation
            .0
            .checked_add(1)
            .ok_or(StreamConsumerModelError::GenerationExhausted)?;
        let delivery_epoch = self
            .delivery_epoch
            .0
            .checked_add(1)
            .ok_or(StreamConsumerModelError::GenerationExhausted)?;

        let mut staged = self.clone();
        let mut actions = Vec::new();
        for (source, stream_message_id) in positions {
            let segment_id = source.segment_id();
            let child = staged
                .children
                .get(&segment_id)
                .ok_or(StreamConsumerModelError::SeekSourceMismatch)?;
            let owner = child.owner();
            let controller_incarnation = child.controller_incarnation;
            let child_generation = child.generation;
            if let Some(reservation) = child.flow_reservation {
                staged
                    .budget
                    .owned(reservation, owner, BudgetUse::FlowPermit)?;
                staged.budget.release(reservation)?;
                actions.push(StreamConsumerAction::StopFlow {
                    source: source.clone(),
                    controller_incarnation,
                    child_generation,
                });
            }
            let child = staged
                .children
                .get_mut(&segment_id)
                .ok_or(StreamConsumerModelError::SeekSourceMismatch)?;
            child.flow_reservation = None;
            child.flow_purpose = None;
            child.completion = CompletionBarrier::default();
            child.phase = SegmentPhase::Seeking;
            staged.completed.remove(&segment_id);
            actions.push(StreamConsumerAction::SeekChild {
                source,
                controller_incarnation,
                child_generation,
                stream_message_id,
            });
        }
        staged.generation = AggregateGeneration(generation);
        staged.delivery_epoch = DeliveryEpoch(delivery_epoch);
        staged.clear_delivered_positions()?;
        *self = staged;
        Ok(actions)
    }

    /// Confirm one ordinary child seek. FLOW resumes only after every child
    /// component confirms.
    ///
    /// # Errors
    ///
    /// Rejects stale generations and callbacks outside `Seeking`.
    pub fn seek_completed(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.require_open()?;
        let child = self.child_mut(segment_id, generation)?;
        if child.phase != SegmentPhase::Seeking {
            return Err(StreamConsumerModelError::InvalidChildTransition {
                segment_id,
                phase: child.phase.clone(),
            });
        }
        child.phase = SegmentPhase::OpenBlocked(FlowBlock::Budget);
        if self
            .children
            .values()
            .any(|child| child.phase == SegmentPhase::Seeking)
        {
            Ok(Vec::new())
        } else {
            self.arbitrate_flow()
        }
    }

    /// Fence all authority after an unrecoverable child, seek, DAG, or
    /// controller failure. Child reservations remain charged until close
    /// confirmation; a later controller incarnation starts resynchronization.
    ///
    /// # Errors
    ///
    /// Returns generation exhaustion atomically. Closing and closed aggregates
    /// reject a new failure transition.
    pub fn require_resync(
        &mut self,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        if self.phase == AggregatePhase::ResyncRequired {
            return Ok(Vec::new());
        }
        if self.phase != AggregatePhase::Open {
            return Err(StreamConsumerModelError::InvalidAggregatePhase { phase: self.phase });
        }
        let generation = self
            .generation
            .0
            .checked_add(1)
            .ok_or(StreamConsumerModelError::GenerationExhausted)?;
        let delivery_epoch = self
            .delivery_epoch
            .0
            .checked_add(1)
            .ok_or(StreamConsumerModelError::GenerationExhausted)?;
        let mut actions = Vec::new();
        for child in self.children.values() {
            match child.phase {
                SegmentPhase::Opening => actions.push(StreamConsumerAction::CancelOpen {
                    source: child.source.clone(),
                    controller_incarnation: child.controller_incarnation,
                    child_generation: child.generation,
                }),
                SegmentPhase::Closing | SegmentPhase::Failed => {}
                _ => actions.push(StreamConsumerAction::CloseChild {
                    source: child.source.clone(),
                    controller_incarnation: child.controller_incarnation,
                    child_generation: child.generation,
                }),
            }
        }
        self.clear_delivered_positions()?;
        self.pending_acknowledgements.clear();
        self.pending_transaction_acknowledgements.clear();
        self.generation = AggregateGeneration(generation);
        self.delivery_epoch = DeliveryEpoch(delivery_epoch);
        self.phase = AggregatePhase::ResyncRequired;
        self.assignment = None;
        self.pending_ownership.clear();
        for child in self.children.values_mut() {
            child.phase = SegmentPhase::Closing;
        }
        Ok(actions)
    }

    /// A failed seek always enters fail-closed resynchronization.
    pub fn seek_failed(&mut self) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.require_resync()
    }

    /// Reconcile a fully validated assignment. Opens are emitted before any
    /// ancestry decision; FLOW waits for [`Self::child_opened`].
    ///
    /// # Errors
    ///
    /// Returns [`StreamConsumerModelError`] without partial assignment mutation.
    pub fn apply_assignment(
        &mut self,
        assignment: ConsumerAssignment,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.apply_assignment_for(self.controller_incarnation, assignment)
    }

    /// Incarnation-fenced form of [`Self::apply_assignment`].
    ///
    /// # Errors
    ///
    /// Rejects delayed assignments from an older or unrelated controller
    /// connection without mutating aggregate state.
    pub fn apply_assignment_for(
        &mut self,
        incarnation: ControllerIncarnation,
        assignment: ConsumerAssignment,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        if incarnation != self.controller_incarnation {
            return Err(AssignmentError::IncarnationMismatch {
                got: incarnation,
                expected: self.controller_incarnation,
            }
            .into());
        }
        let mut staged = self.clone();
        let mut actions = staged.reconcile_assignment(assignment, &BTreeSet::new())?;
        actions.extend(staged.arbitrate_flow()?);
        *self = staged;
        Ok(actions)
    }

    fn reconcile_assignment(
        &mut self,
        assignment: ConsumerAssignment,
        replacements: &BTreeSet<SegmentId>,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.require_open()?;
        self.dag.validate_assignment(&assignment)?;
        for segment in assignment.segments() {
            let got = segment.source().parent_topic();
            if got != self.parent_topic {
                return Err(StreamConsumerModelError::AssignmentParentMismatch {
                    segment_id: segment.segment_id(),
                    got,
                    expected: self.parent_topic.clone(),
                });
            }
        }
        if self.assignment.as_ref() == Some(&assignment) && replacements.is_empty() {
            return Ok(Vec::new());
        }
        let next_generation = self
            .generation
            .0
            .checked_add(1)
            .ok_or(StreamConsumerModelError::GenerationExhausted)?;
        let before: BTreeMap<SegmentId, SegmentSource> =
            self.assignment
                .as_ref()
                .map_or_else(BTreeMap::new, |current| {
                    current
                        .segments()
                        .iter()
                        .map(|segment| (segment.segment_id(), segment.source()))
                        .collect()
                });
        let after: BTreeMap<SegmentId, SegmentSource> = assignment
            .segments()
            .iter()
            .map(|segment| (segment.segment_id(), segment.source()))
            .collect();
        let gained_count = after
            .iter()
            .filter(|(id, source)| {
                (replacements.contains(id) || before.get(id) != Some(*source))
                    && !self.children.contains_key(id)
            })
            .count();
        let gained_count = u64::try_from(gained_count)
            .map_err(|_| StreamConsumerModelError::GenerationExhausted)?;
        self.next_child_generation
            .checked_add(gained_count)
            .ok_or(StreamConsumerModelError::GenerationExhausted)?;

        // Assignment reconciliation is infrequent and must be atomic. Stage
        // every child/budget mutation before replacing aggregate state.
        let mut children = self.children.clone();
        let mut pending_ownership = self.pending_ownership.clone();
        let mut ownership_history = self.ownership_history.clone();
        let budget = self.budget.clone();
        let mut next_child_generation = self.next_child_generation;
        let mut actions = Vec::new();
        let mut retiring: BTreeMap<SegmentId, SegmentSource> = before
            .iter()
            .filter(|(id, source)| replacements.contains(id) || after.get(id) != Some(*source))
            .map(|(id, source)| (*id, source.clone()))
            .collect();
        for segment_id in replacements {
            if let Some(child) = children.get(segment_id) {
                retiring
                    .entry(*segment_id)
                    .or_insert_with(|| child.source.clone());
            }
        }
        for (lost, source) in &retiring {
            pending_ownership.remove(lost);
            children
                .get_mut(lost)
                .filter(|child| child.source == *source)
                .into_iter()
                .for_each(|child| {
                    let mut stop_flow = false;
                    let wait_for_flow_drain = replacements.contains(lost);
                    if child.phase == SegmentPhase::Opening {
                        actions.push(StreamConsumerAction::CancelOpen {
                            source: child.source.clone(),
                            controller_incarnation: child.controller_incarnation,
                            child_generation: child.generation,
                        });
                        child.phase = SegmentPhase::Closing;
                    } else if child.phase == SegmentPhase::Flowing {
                        stop_flow = true;
                        child.wait_for_flow_drain = wait_for_flow_drain;
                        child.phase = SegmentPhase::Draining;
                    } else if matches!(
                        child.phase,
                        SegmentPhase::OpenBlocked(_)
                            | SegmentPhase::Terminal
                            | SegmentPhase::Seeking
                    ) {
                        child.wait_for_flow_drain = wait_for_flow_drain;
                        child.phase = SegmentPhase::Draining;
                    } else if child.phase == SegmentPhase::Draining {
                        child.wait_for_flow_drain |= wait_for_flow_drain;
                    }
                    if child.drain_settled() && child.phase == SegmentPhase::Draining {
                        child.phase = SegmentPhase::Closing;
                        actions.push(StreamConsumerAction::CloseChild {
                            source: child.source.clone(),
                            controller_incarnation: child.controller_incarnation,
                            child_generation: child.generation,
                        });
                    } else if stop_flow {
                        actions.push(StreamConsumerAction::StopFlow {
                            source: child.source.clone(),
                            controller_incarnation: child.controller_incarnation,
                            child_generation: child.generation,
                        });
                    }
                });
        }

        for (segment_id, source) in &after {
            if !replacements.contains(segment_id) && before.get(segment_id) == Some(source) {
                continue;
            }
            if children.contains_key(segment_id) {
                pending_ownership.insert(*segment_id, source.clone());
                continue;
            }
            let child_generation = ChildGeneration(next_child_generation);
            next_child_generation = next_child_generation
                .checked_add(1)
                .ok_or(StreamConsumerModelError::GenerationExhausted)?;
            ownership_history.insert(*segment_id);
            children.insert(
                *segment_id,
                ChildState {
                    source: source.clone(),
                    controller_incarnation: self.controller_incarnation,
                    generation: child_generation,
                    phase: SegmentPhase::Opening,
                    flow_reservation: None,
                    flow_purpose: None,
                    completion: CompletionBarrier::default(),
                    wait_for_flow_drain: false,
                },
            );
            actions.push(StreamConsumerAction::OpenChild {
                source: source.clone(),
                controller_incarnation: self.controller_incarnation,
                aggregate_generation: AggregateGeneration(next_generation),
                child_generation,
            });
        }

        self.generation = AggregateGeneration(next_generation);
        self.next_child_generation = next_child_generation;
        self.children = children;
        self.pending_ownership = pending_ownership;
        self.ownership_history = ownership_history;
        self.budget = budget;
        self.assignment = Some(assignment);
        Ok(actions)
    }

    /// Commit a successful ordinary child open and independently evaluate FLOW.
    ///
    /// # Errors
    ///
    /// Rejects stale generations and invalid lifecycle callbacks.
    pub fn child_opened(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.require_open()?;
        let source = {
            let child = self.child_mut(segment_id, generation)?;
            if child.phase != SegmentPhase::Opening {
                return Err(StreamConsumerModelError::InvalidChildTransition {
                    segment_id,
                    phase: child.phase.clone(),
                });
            }
            child.phase = SegmentPhase::OpenBlocked(FlowBlock::Budget);
            child.source.clone()
        };
        if self.pending_ownership.get(&segment_id) == Some(&source) {
            self.pending_ownership.remove(&segment_id);
        }
        self.arbitrate_flow()
    }

    /// Record that an opening Exclusive child is waiting for broker ownership.
    /// The source remains assigned and the same generation keeps retrying until
    /// attach succeeds or a control-plane transition fences it.
    ///
    /// # Errors
    ///
    /// Rejects stale generations and children that are no longer opening.
    pub fn child_open_busy(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
    ) -> Result<(), StreamConsumerModelError> {
        self.require_open()?;
        let child = self.child(segment_id, generation)?;
        if child.phase != SegmentPhase::Opening {
            return Err(StreamConsumerModelError::InvalidChildTransition {
                segment_id,
                phase: child.phase.clone(),
            });
        }
        let source = child.source.clone();
        self.pending_ownership.insert(segment_id, source);
        Ok(())
    }

    /// Commit a confirmation-bearing ordinary child close. If the assignment
    /// regained the same segment while its old exclusive child was draining,
    /// this is the only transition that emits the replacement open.
    ///
    /// # Errors
    ///
    /// Rejects stale generations and close confirmations for a child that was
    /// not closing.
    pub fn child_closed(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.require_callback_phase()?;
        let child = self.child(segment_id, generation)?;
        if !matches!(child.phase, SegmentPhase::Closing | SegmentPhase::Failed) {
            return Err(StreamConsumerModelError::InvalidChildTransition {
                segment_id,
                phase: child.phase.clone(),
            });
        }

        let owner = child.owner();
        let source = child.source.clone();
        let pending = (self.phase == AggregatePhase::Open)
            .then(|| self.pending_ownership.get(&segment_id).cloned())
            .flatten();
        let replacement = pending
            .map(|source| {
                self.next_child_generation
                    .checked_add(1)
                    .map(|next| (source, ChildGeneration(self.next_child_generation), next))
                    .ok_or(StreamConsumerModelError::GenerationExhausted)
            })
            .transpose()?;

        let released = self.budget.release_owner(owner);
        self.live_deliveries
            .retain(|_, delivery| delivery.owner != owner);
        self.children.remove(&segment_id);
        self.pending_ownership.remove(&segment_id);
        let mut actions = Vec::new();
        if let Some((source, child_generation, next_child_generation)) = replacement {
            self.next_child_generation = next_child_generation;
            self.ownership_history.insert(segment_id);
            self.children.insert(
                segment_id,
                ChildState {
                    source: source.clone(),
                    controller_incarnation: self.controller_incarnation,
                    generation: child_generation,
                    phase: SegmentPhase::Opening,
                    flow_reservation: None,
                    flow_purpose: None,
                    completion: CompletionBarrier::default(),
                    wait_for_flow_drain: false,
                },
            );
            actions.push(StreamConsumerAction::OpenChild {
                source,
                controller_incarnation: self.controller_incarnation,
                aggregate_generation: self.generation,
                child_generation,
            });
        }
        if self.phase == AggregatePhase::Closing && self.children.is_empty() {
            self.phase = AggregatePhase::Closed;
            self.assignment = None;
            self.clear_delivered_positions()?;
        } else {
            if self.delivered_positions.remove(&source).is_some() {
                if self.delivered_positions.is_empty() {
                    self.clear_delivered_positions()?;
                } else {
                    self.refresh_delivered_position()?;
                }
            }
            if !released.is_empty() {
                actions.extend(self.arbitrate_flow()?);
            }
        }
        Ok(actions)
    }

    /// Reserve a decompression output/workspace before the runtime allocates it.
    ///
    /// # Errors
    ///
    /// Rejects stale children, chunk-only permits, and exhausted capacity.
    pub fn reserve_decompression(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
        bytes: usize,
    ) -> Result<BudgetReservationId, StreamConsumerModelError> {
        self.reserve_receive_work(segment_id, generation, BudgetUse::Decompression, bytes)
    }

    /// Reserve structural batch-expansion storage before decoding members.
    ///
    /// # Errors
    ///
    /// Rejects stale children, chunk-only permits, and exhausted capacity.
    pub fn reserve_batch_assembly(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
        bytes: usize,
    ) -> Result<BudgetReservationId, StreamConsumerModelError> {
        self.reserve_receive_work(segment_id, generation, BudgetUse::BatchAssembly, bytes)
    }

    fn reserve_receive_work(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
        use_: BudgetUse,
        bytes: usize,
    ) -> Result<BudgetReservationId, StreamConsumerModelError> {
        self.require_open()?;
        let child = self.child(segment_id, generation)?;
        if !matches!(child.phase, SegmentPhase::Flowing | SegmentPhase::Draining)
            || child.flow_reservation.is_none()
        {
            return Err(StreamConsumerModelError::InvalidChildTransition {
                segment_id,
                phase: child.phase.clone(),
            });
        }
        let purpose_matches = child.flow_purpose == Some(FlowPurpose::Message)
            || matches!(
                (use_, child.flow_purpose),
                (
                    BudgetUse::Decompression,
                    Some(FlowPurpose::ChunkContinuation { .. })
                )
            );
        if !purpose_matches {
            return Err(StreamConsumerModelError::FlowPurposeMismatch {
                segment_id,
                actual: child.flow_purpose,
                expected: FlowPurpose::Message,
            });
        }
        let owner = child.owner();
        let exhausted = StreamConsumerModelError::CompletionCounterExhausted {
            segment_id,
            kind: "pre-terminal reservation",
        };
        let pre_terminal_reservations = child
            .completion
            .pre_terminal_reservations
            .checked_add(1)
            .ok_or(exhausted)?;
        let reservation = self.budget.reserve_owned(owner, use_, bytes)?;
        self.child_mut(segment_id, generation)?
            .completion
            .pre_terminal_reservations = pre_terminal_reservations;
        Ok(reservation)
    }

    /// Release a preallocated receive workspace after allocation is cancelled.
    ///
    /// # Errors
    ///
    /// Rejects foreign, stale, and non-work reservations.
    pub fn cancel_receive_work(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
        reservation: BudgetReservationId,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.require_open()?;
        let child = self.child(segment_id, generation)?;
        let owner = child.owner();
        self.receive_work(reservation, owner)?;
        let pre_terminal_reservations =
            child
                .completion
                .pre_terminal_reservations
                .checked_sub(1)
                .ok_or(StreamConsumerModelError::UnbalancedCompletionHook {
                    segment_id,
                    kind: "pre-terminal reservation",
                })?;
        self.budget.release(reservation)?;
        self.child_mut(segment_id, generation)?
            .completion
            .pre_terminal_reservations = pre_terminal_reservations;
        let mut actions = self.close_if_drained(segment_id, generation)?;
        actions.extend(self.arbitrate_flow()?);
        Ok(actions)
    }

    /// Convert one consumed frame into a bounded chunk assembly and grant only
    /// the next continuation frame. `allocation_bytes` must cover retained
    /// chunks plus the runtime's final contiguous-assembly workspace.
    ///
    /// # Errors
    ///
    /// Rejects stale flow/assembly reservations and insufficient preallocation.
    pub fn chunk_frame_buffered(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
        flow_reservation: BudgetReservationId,
        assembly: Option<BudgetReservationId>,
        allocation_bytes: usize,
    ) -> Result<ChunkAssemblyTransition, StreamConsumerModelError> {
        self.require_open()?;
        let child = self.child(segment_id, generation)?;
        if !matches!(child.phase, SegmentPhase::Flowing | SegmentPhase::Draining)
            || child.flow_reservation != Some(flow_reservation)
        {
            return Err(StreamConsumerModelError::InvalidChildTransition {
                segment_id,
                phase: child.phase.clone(),
            });
        }
        let owner = child.owner();
        let expected_purpose = assembly.map_or(FlowPurpose::Message, |assembly| {
            FlowPurpose::ChunkContinuation { assembly }
        });
        if child.flow_purpose != Some(expected_purpose) {
            return Err(StreamConsumerModelError::FlowPurposeMismatch {
                segment_id,
                actual: child.flow_purpose,
                expected: expected_purpose,
            });
        }
        self.budget
            .owned(flow_reservation, owner, BudgetUse::FlowPermit)?;

        let mut staged = self.clone();
        let (assembly, pre_terminal_reservations) = if let Some(assembly) = assembly {
            let assembly_state = staged
                .budget
                .owned(assembly, owner, BudgetUse::ChunkAssembly)?;
            staged.budget.release(flow_reservation)?;
            staged.budget.transfer_owned(
                assembly,
                owner,
                BudgetUse::ChunkAssembly,
                BudgetUse::ChunkAssembly,
                allocation_bytes.max(assembly_state.bytes),
            )?;
            let remaining = child
                .completion
                .pre_terminal_reservations
                .checked_sub(1)
                .ok_or(StreamConsumerModelError::UnbalancedCompletionHook {
                    segment_id,
                    kind: "pre-terminal reservation",
                })?;
            (assembly, remaining)
        } else {
            staged.budget.transfer_owned(
                flow_reservation,
                owner,
                BudgetUse::FlowPermit,
                BudgetUse::ChunkAssembly,
                allocation_bytes,
            )?;
            (flow_reservation, child.completion.pre_terminal_reservations)
        };
        let next_flow =
            staged
                .budget
                .reserve_owned(owner, BudgetUse::FlowPermit, MAX_FRAME_SIZE)?;
        let exhausted = StreamConsumerModelError::CompletionCounterExhausted {
            segment_id,
            kind: "pre-terminal reservation",
        };
        let pre_terminal_reservations =
            pre_terminal_reservations.checked_add(1).ok_or(exhausted)?;
        let child = staged.child_mut(segment_id, generation)?;
        child.flow_reservation = Some(next_flow);
        child.flow_purpose = Some(FlowPurpose::ChunkContinuation { assembly });
        child.completion.pre_terminal_reservations = pre_terminal_reservations;
        let action = StreamConsumerAction::GrantFlow {
            source: child.source.clone(),
            controller_incarnation: child.controller_incarnation,
            child_generation: child.generation,
            reservation: next_flow,
            purpose: FlowPurpose::ChunkContinuation { assembly },
        };
        *self = staged;
        Ok(ChunkAssemblyTransition {
            assembly,
            actions: vec![action],
        })
    }

    /// Complete an already-preallocated chunk assembly into one retained message.
    ///
    /// # Errors
    ///
    /// Rejects unrelated complete messages, stale reservations, and retained
    /// charges larger than the preallocated frame plus assembly work.
    pub fn chunk_message_arrived(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
        flow_reservation: BudgetReservationId,
        assembly: BudgetReservationId,
        work: &[BudgetReservationId],
        retained_bytes: usize,
    ) -> Result<ArrivalTransition, StreamConsumerModelError> {
        let owner = self.child(segment_id, generation)?.owner();
        self.budget
            .owned(assembly, owner, BudgetUse::ChunkAssembly)?;
        let mut all_work = Vec::with_capacity(work.len().saturating_add(1));
        all_work.push(assembly);
        all_work.extend_from_slice(work);
        self.complete_preallocated_arrival(
            segment_id,
            generation,
            flow_reservation,
            FlowPurpose::ChunkContinuation { assembly },
            &all_work,
            &[retained_bytes],
        )
        .and_then(|(mut retained, actions)| {
            retained
                .pop()
                .ok_or(StreamConsumerModelError::UnbalancedCompletionHook {
                    segment_id,
                    kind: "preallocated message",
                })
                .map(|retained| ArrivalTransition { retained, actions })
        })
    }

    /// Complete one decompressed/preallocated message atomically.
    ///
    /// # Errors
    ///
    /// Rejects stale work and retained bytes beyond the preallocated bound.
    pub fn message_arrived_preallocated(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
        flow_reservation: BudgetReservationId,
        work: &[BudgetReservationId],
        retained_bytes: usize,
    ) -> Result<ArrivalTransition, StreamConsumerModelError> {
        let (mut retained, actions) = self.complete_preallocated_arrival(
            segment_id,
            generation,
            flow_reservation,
            FlowPurpose::Message,
            work,
            &[retained_bytes],
        )?;
        let retained =
            retained
                .pop()
                .ok_or(StreamConsumerModelError::UnbalancedCompletionHook {
                    segment_id,
                    kind: "preallocated message",
                })?;
        Ok(ArrivalTransition { retained, actions })
    }

    /// Settle a transformed entry that the runtime intentionally discarded.
    /// The permit and every preallocated workspace are released before FLOW is
    /// reconsidered, and no delivery authority is minted.
    pub fn discard_preallocated_arrival(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
        flow_reservation: BudgetReservationId,
        expected_purpose: FlowPurpose,
        work: &[BudgetReservationId],
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        let (retained, mut actions) = self.complete_preallocated_arrival(
            segment_id,
            generation,
            flow_reservation,
            expected_purpose,
            work,
            &[0],
        )?;
        retained
            .into_iter()
            .try_for_each(|reservation| self.budget.release(reservation))
            .map_err(StreamConsumerModelError::from)
            .and_then(|()| {
                actions.extend(self.close_if_drained(segment_id, generation)?);
                actions.extend(self.arbitrate_flow()?);
                Ok(actions)
            })
    }

    /// Complete every member of an already-preallocated broker batch in one
    /// transition, before any member becomes visible to aggregate receive.
    ///
    /// # Errors
    ///
    /// Rejects an empty batch, stale work, and aggregate retained charges beyond
    /// the frame plus preallocated decompression/batch work.
    pub fn batch_arrived_preallocated(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
        flow_reservation: BudgetReservationId,
        work: &[BudgetReservationId],
        retained_bytes: &[usize],
    ) -> Result<BatchArrivalTransition, StreamConsumerModelError> {
        if retained_bytes.is_empty() {
            let phase = self.child(segment_id, generation)?.phase.clone();
            return Err(StreamConsumerModelError::InvalidChildTransition { segment_id, phase });
        }
        let (retained, actions) = self.complete_preallocated_arrival(
            segment_id,
            generation,
            flow_reservation,
            FlowPurpose::Message,
            work,
            retained_bytes,
        )?;
        Ok(BatchArrivalTransition { retained, actions })
    }

    fn complete_preallocated_arrival(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
        flow_reservation: BudgetReservationId,
        expected_purpose: FlowPurpose,
        work: &[BudgetReservationId],
        retained_bytes: &[usize],
    ) -> Result<(Vec<BudgetReservationId>, Vec<StreamConsumerAction>), StreamConsumerModelError>
    {
        self.require_open()?;
        let child = self.child(segment_id, generation)?;
        if !matches!(child.phase, SegmentPhase::Flowing | SegmentPhase::Draining)
            || child.flow_reservation != Some(flow_reservation)
        {
            return Err(StreamConsumerModelError::InvalidChildTransition {
                segment_id,
                phase: child.phase.clone(),
            });
        }
        if child.flow_purpose != Some(expected_purpose) {
            return Err(StreamConsumerModelError::FlowPurposeMismatch {
                segment_id,
                actual: child.flow_purpose,
                expected: expected_purpose,
            });
        }
        let owner = child.owner();
        let flow = self
            .budget
            .owned(flow_reservation, owner, BudgetUse::FlowPermit)?;
        let mut seen = BTreeSet::from([flow_reservation]);
        let mut reserved = flow.bytes;
        for reservation in work {
            if !seen.insert(*reservation) {
                return Err(StreamConsumerModelError::DuplicateReceiveWork {
                    reservation: *reservation,
                });
            }
            reserved = reserved.saturating_add(self.receive_work(*reservation, owner)?.bytes);
        }
        let required = retained_bytes.iter().fold(0usize, |total, bytes| {
            total.saturating_add((*bytes).max(MIN_RETAINED_MESSAGE_RESERVATION))
        });
        if required > reserved {
            return Err(BudgetError::PreallocationExceeded { required, reserved }.into());
        }
        let exhausted = StreamConsumerModelError::CompletionCounterExhausted {
            segment_id,
            kind: "pre-terminal reservation",
        };
        let consumed = work.len().checked_add(1).ok_or(exhausted)?;
        let pre_terminal_reservations = child
            .completion
            .pre_terminal_reservations
            .checked_sub(consumed)
            .and_then(|remaining| remaining.checked_add(retained_bytes.len()))
            .ok_or(StreamConsumerModelError::UnbalancedCompletionHook {
                segment_id,
                kind: "pre-terminal reservation",
            })?;

        let mut staged = self.clone();
        staged.budget.release(flow_reservation)?;
        for reservation in work {
            staged.budget.release(*reservation)?;
        }
        retained_bytes
            .iter()
            .try_fold(
                Vec::with_capacity(retained_bytes.len()),
                |mut retained, bytes| {
                    staged
                        .budget
                        .reserve_owned(owner, BudgetUse::RetainedMessage, *bytes)
                        .map(|reservation| {
                            retained.push(reservation);
                            retained
                        })
                },
            )
            .map_err(StreamConsumerModelError::from)
            .and_then(|retained| {
                let child = staged.child_mut(segment_id, generation)?;
                child.flow_reservation = None;
                child.flow_purpose = None;
                child.completion.pre_terminal_reservations = pre_terminal_reservations;
                if child.phase == SegmentPhase::Flowing {
                    child.phase = SegmentPhase::OpenBlocked(FlowBlock::Budget);
                }
                let actions = staged.arbitrate_flow()?;
                *self = staged;
                Ok((retained, actions))
            })
    }

    fn receive_work(
        &self,
        reservation: BudgetReservationId,
        owner: BudgetReservationOwner,
    ) -> Result<DataReservation, StreamConsumerModelError> {
        let state = self
            .budget
            .reservations
            .get(&reservation)
            .copied()
            .ok_or(BudgetError::UnknownReservation { reservation })?;
        if state.owner != Some(owner) {
            return Err(BudgetError::ReservationOwnerMismatch {
                reservation,
                actual: state.owner,
                expected: owner,
            }
            .into());
        }
        if !matches!(
            state.use_,
            BudgetUse::ChunkAssembly | BudgetUse::Decompression | BudgetUse::BatchAssembly
        ) {
            return Err(StreamConsumerModelError::InvalidReceiveWork {
                reservation,
                use_: state.use_,
            });
        }
        Ok(state)
    }

    /// Transfer a max-frame FLOW reservation to exact retained bytes and grant
    /// another permit only if eligibility and capacity still allow it.
    ///
    /// # Errors
    ///
    /// Rejects stale generations, foreign reservations, and over-budget frames.
    pub fn message_arrived(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
        reservation: BudgetReservationId,
        retained_bytes: usize,
    ) -> Result<ArrivalTransition, StreamConsumerModelError> {
        self.require_open()?;
        let child = self.child(segment_id, generation)?;
        if !matches!(child.phase, SegmentPhase::Flowing | SegmentPhase::Draining)
            || child.flow_reservation != Some(reservation)
        {
            return Err(StreamConsumerModelError::InvalidChildTransition {
                segment_id,
                phase: child.phase.clone(),
            });
        }
        if child.flow_purpose != Some(FlowPurpose::Message) {
            return Err(StreamConsumerModelError::FlowPurposeMismatch {
                segment_id,
                actual: child.flow_purpose,
                expected: FlowPurpose::Message,
            });
        }
        let owner = child.owner();
        let pre_terminal_reservations =
            child
                .completion
                .pre_terminal_reservations
                .checked_sub(1)
                .ok_or(StreamConsumerModelError::UnbalancedCompletionHook {
                    segment_id,
                    kind: "pre-terminal reservation",
                })?;
        self.budget
            .owned(reservation, owner, BudgetUse::FlowPermit)?;
        let retained_bytes = retained_bytes.max(MIN_RETAINED_MESSAGE_RESERVATION);
        if let Err(error) = self.budget.transfer_owned(
            reservation,
            owner,
            BudgetUse::FlowPermit,
            BudgetUse::RetainedMessage,
            retained_bytes,
        ) {
            self.budget.release(reservation)?;
            let disposition = if matches!(&error, BudgetError::Exhausted { .. }) {
                ArrivalFailureDisposition::Retryable
            } else {
                ArrivalFailureDisposition::Permanent
            };
            let pending_source = (disposition == ArrivalFailureDisposition::Retryable)
                .then(|| {
                    self.assignment.as_ref().and_then(|assignment| {
                        assignment
                            .segments()
                            .iter()
                            .find(|assigned| assigned.segment_id() == segment_id)
                            .map(crate::scalable_consumer::AssignedSegment::source)
                    })
                })
                .flatten();
            let child = self.child_mut(segment_id, generation)?;
            child.flow_reservation = None;
            child.flow_purpose = None;
            child.completion.pre_terminal_reservations = pre_terminal_reservations;
            child.phase = SegmentPhase::Failed;
            let close = StreamConsumerAction::CloseChild {
                source: child.source.clone(),
                controller_incarnation: child.controller_incarnation,
                child_generation: child.generation,
            };
            if let Some(source) = pending_source {
                self.pending_ownership.insert(segment_id, source);
            } else {
                self.pending_ownership.remove(&segment_id);
            }
            let mut actions = vec![close];
            actions.extend(self.arbitrate_flow()?);
            return Err(StreamConsumerModelError::ArrivalAccountingFailed {
                error,
                disposition,
                actions,
            });
        }
        let child = self.child_mut(segment_id, generation)?;
        child.flow_reservation = None;
        child.flow_purpose = None;
        if child.phase == SegmentPhase::Flowing {
            child.phase = SegmentPhase::OpenBlocked(FlowBlock::Budget);
        }
        let actions = self.arbitrate_flow()?;
        Ok(ArrivalTransition {
            retained: reservation,
            actions,
        })
    }

    /// Linearize one application delivery and mint process-local authority.
    ///
    /// # Errors
    ///
    /// Rejects stale child generations and reservations not holding retained
    /// message bytes.
    pub fn issue_delivery(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
        stream_message_id: StreamMessageId,
        reservation: BudgetReservationId,
    ) -> Result<DeliveryToken, StreamConsumerModelError> {
        self.require_open()?;
        let child = self.child(segment_id, generation)?;
        let source = child.source.clone();
        let controller_incarnation = child.controller_incarnation;
        let owner = child.owner();
        if stream_message_id.source() != &source {
            return Err(StreamConsumerModelError::DeliverySourceMismatch {
                got: stream_message_id.source().clone(),
                expected: source,
            });
        }
        let message_id = stream_message_id.ordinary_message_id();
        let exhausted = StreamConsumerModelError::CompletionCounterExhausted {
            segment_id,
            kind: "delivery",
        };
        let delivery_count = child
            .completion
            .deliveries
            .checked_add(1)
            .ok_or(exhausted)?;
        let pre_terminal_reservations =
            child
                .completion
                .pre_terminal_reservations
                .checked_sub(1)
                .ok_or(StreamConsumerModelError::UnbalancedCompletionHook {
                    segment_id,
                    kind: "pre-terminal reservation",
                })?;
        let reservation_state =
            self.budget
                .owned(reservation, owner, BudgetUse::RetainedMessage)?;
        let replace = self
            .delivered_positions
            .get(&source)
            .is_none_or(|current| current.ordinary_message_id() < message_id);
        let (position_vector_heap, delivered_positions_heap) = prospective_position_heap_bytes(
            &self.delivered_positions,
            &source,
            &stream_message_id,
            replace,
        )?;
        let stream_message_id_bytes = stream_message_id.encoded_len()?;
        let stream_message_id_heap = source
            .topic()
            .len()
            .checked_add(stream_message_id.ordinary_message_id_bytes().len())
            .ok_or(StreamPositionError::LengthOverflow)?;
        let provisional_authority_bytes = stream_message_id_bytes
            .checked_add(stream_message_id_heap)
            .and_then(|bytes| bytes.checked_add(MAX_STREAM_POSITION_SIZE))
            .and_then(|bytes| bytes.checked_add(position_vector_heap))
            .and_then(|bytes| bytes.checked_add(DELIVERY_AUTHORITY_OVERHEAD))
            .ok_or(StreamPositionError::LengthOverflow)?;
        let provisional_lease_bytes = reservation_state
            .bytes
            .checked_add(provisional_authority_bytes)
            .ok_or(StreamPositionError::LengthOverflow)?;
        let provisional_delivered_positions_bytes = MAX_STREAM_POSITION_SIZE
            .checked_add(position_vector_heap)
            .and_then(|bytes| bytes.checked_add(delivered_positions_heap))
            .and_then(|bytes| bytes.checked_add(DELIVERY_AUTHORITY_OVERHEAD))
            .ok_or(StreamPositionError::LengthOverflow)?;
        let dequeue_sequence = DequeueSequence(self.next_dequeue_sequence);
        let next_dequeue_sequence = self
            .next_dequeue_sequence
            .checked_add(1)
            .ok_or(StreamConsumerModelError::DequeueSequenceExhausted)?;

        let mut budget = self.budget.clone();
        let delivered_positions_reservation = budget
            .transfer_owned_with_authority(
                reservation,
                owner,
                BudgetUse::RetainedMessage,
                BudgetUse::DeliveryLease,
                provisional_lease_bytes,
                provisional_authority_bytes,
            )
            .and_then(|()| match self.delivered_positions_reservation {
                Some(metadata_reservation) => budget
                    .transfer(
                        metadata_reservation,
                        BudgetUse::DeliveredPositionMetadata,
                        BudgetUse::DeliveredPositionMetadata,
                        provisional_delivered_positions_bytes,
                    )
                    .map(|()| metadata_reservation),
                None => budget.reserve(
                    BudgetUse::DeliveredPositionMetadata,
                    provisional_delivered_positions_bytes,
                ),
            })
            .map_err(StreamConsumerModelError::from)?;

        let mut delivered_positions = self.delivered_positions.clone();
        if replace {
            delivered_positions.insert(source.clone(), stream_message_id.clone());
        }
        let position_vector =
            PositionVector::from_canonical(self.dag.epoch(), &delivered_positions)?;
        let position_vector_bytes = position_vector.encoded_len()?;
        let authority_bytes = stream_message_id_bytes
            .checked_add(stream_message_id_heap)
            .and_then(|bytes| bytes.checked_add(position_vector_bytes))
            .and_then(|bytes| bytes.checked_add(position_vector_heap))
            .and_then(|bytes| bytes.checked_add(DELIVERY_AUTHORITY_OVERHEAD))
            .ok_or(StreamPositionError::LengthOverflow)?;
        let lease_bytes = reservation_state
            .bytes
            .checked_add(authority_bytes)
            .ok_or(StreamPositionError::LengthOverflow)?;
        let delivered_positions_bytes = position_vector_bytes
            .checked_add(position_vector_heap)
            .and_then(|bytes| bytes.checked_add(delivered_positions_heap))
            .and_then(|bytes| bytes.checked_add(DELIVERY_AUTHORITY_OVERHEAD))
            .ok_or(StreamPositionError::LengthOverflow)?;
        budget
            .transfer_owned_with_authority(
                reservation,
                owner,
                BudgetUse::DeliveryLease,
                BudgetUse::DeliveryLease,
                lease_bytes,
                authority_bytes,
            )
            .and_then(|()| {
                budget.transfer(
                    delivered_positions_reservation,
                    BudgetUse::DeliveredPositionMetadata,
                    BudgetUse::DeliveredPositionMetadata,
                    delivered_positions_bytes,
                )
            })
            .map_err(StreamConsumerModelError::from)
            .and_then(|()| {
                let child = self.child_mut(segment_id, generation)?;
                child.completion.deliveries = delivery_count;
                child.completion.pre_terminal_reservations = pre_terminal_reservations;
                self.budget = budget;
                self.delivered_positions = delivered_positions;
                self.delivered_position = position_vector.clone();
                self.delivered_positions_reservation = Some(delivered_positions_reservation);
                self.next_dequeue_sequence = next_dequeue_sequence;
                self.live_deliveries.insert(
                    dequeue_sequence,
                    LiveDelivery {
                        owner,
                        reservation,
                        message_id,
                    },
                );
                Ok(DeliveryToken {
                    consumer_instance: self.consumer_instance,
                    controller_incarnation,
                    child_generation: generation,
                    stream_message_id,
                    position_vector,
                    delivery_epoch: self.delivery_epoch,
                    dequeue_sequence,
                    reservation,
                })
            })
    }

    /// Validate and resolve one live delivery lease, releasing its budget.
    ///
    /// # Errors
    ///
    /// Rejects foreign, stale, and already-resolved tokens before any runtime
    /// wire operation.
    pub fn resolve_delivery(
        &mut self,
        token: &DeliveryToken,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.validate_delivery_token(token)?;
        if self.delivery_operation_pending(token.dequeue_sequence) {
            return Err(StreamConsumerModelError::DeliveryOperationPending);
        }
        self.resolve_delivery_sequence(token.dequeue_sequence)
    }

    /// Validate that a cancelled receive may return this still-live delivery
    /// to the local ordered queue without changing its authority or budget.
    pub fn validate_delivery_restoration(
        &self,
        token: &DeliveryToken,
    ) -> Result<ChildGeneration, StreamConsumerModelError> {
        let validated = self.validate_delivery_token(token)?;
        if self.delivery_operation_pending(token.dequeue_sequence) {
            return Err(StreamConsumerModelError::DeliveryOperationPending);
        }
        Ok(validated.child_generation)
    }

    /// Atomically admit one individual acknowledgement for a live delivery.
    pub fn admit_individual_acknowledgement(
        &mut self,
        token: &DeliveryToken,
    ) -> Result<AcknowledgementTransition, StreamConsumerModelError> {
        let validated = self.validate_delivery_token(token)?;
        let component = AcknowledgementComponent {
            source: validated.source.clone(),
            child_generation: validated.child_generation,
            message_ids: vec![validated.message_id],
            message_id_bytes: vec![validated.message_id_bytes],
            cumulative: false,
        };
        self.admit_acknowledgement(
            vec![component],
            vec![(token.dequeue_sequence, validated.source)],
            false,
        )
        .map(|(authority, components)| AcknowledgementTransition {
            authority: AcknowledgementAuthority {
                consumer_instance: authority.consumer_instance,
                delivery_epoch: authority.delivery_epoch,
                operation_id: authority.operation_id,
            },
            components,
        })
    }

    /// Atomically admit cumulative acknowledgement of a live delivery's
    /// complete delivered-position vector.
    pub fn admit_cumulative_acknowledgement(
        &mut self,
        token: &DeliveryToken,
    ) -> Result<AcknowledgementTransition, StreamConsumerModelError> {
        self.validate_delivery_token(token)?;
        let components = self.position_components(token.position_vector(), false)?;
        let deliveries = self.deliveries_covered_by(&components);
        self.admit_acknowledgement(components, deliveries, false)
            .map(|(authority, components)| AcknowledgementTransition {
                authority: AcknowledgementAuthority {
                    consumer_instance: authority.consumer_instance,
                    delivery_epoch: authority.delivery_epoch,
                    operation_id: authority.operation_id,
                },
                components,
            })
    }

    /// Validate every live token before admitting a grouped individual batch.
    pub fn admit_batch_acknowledgement(
        &mut self,
        tokens: &[&DeliveryToken],
    ) -> Result<AcknowledgementTransition, StreamConsumerModelError> {
        type GroupedIds =
            BTreeMap<(SegmentSource, ChildGeneration), Vec<(crate::MessageId, Vec<u8>)>>;

        let mut grouped = GroupedIds::new();
        let mut deliveries = Vec::with_capacity(tokens.len());
        let mut sequences = BTreeSet::new();
        for token in tokens {
            let validated = self.validate_delivery_token(token)?;
            if !sequences.insert(token.dequeue_sequence) {
                return Err(StreamConsumerModelError::DeliveryOperationPending);
            }
            grouped
                .entry((validated.source.clone(), validated.child_generation))
                .or_default()
                .push((validated.message_id, validated.message_id_bytes));
            deliveries.push((token.dequeue_sequence, validated.source));
        }
        let components = grouped
            .into_iter()
            .map(|((source, child_generation), message_ids)| {
                let (message_ids, message_id_bytes) = message_ids.into_iter().unzip();
                AcknowledgementComponent {
                    source,
                    child_generation,
                    message_ids,
                    message_id_bytes,
                    cumulative: false,
                }
            })
            .collect();
        self.admit_acknowledgement(components, deliveries, false)
            .map(|(authority, components)| AcknowledgementTransition {
                authority: AcknowledgementAuthority {
                    consumer_instance: authority.consumer_instance,
                    delivery_epoch: authority.delivery_epoch,
                    operation_id: authority.operation_id,
                },
                components,
            })
    }

    /// Admit cumulative acknowledgement of a restored canonical vector after
    /// current layout, assignment, source, and child-generation validation.
    pub fn admit_position_acknowledgement(
        &mut self,
        positions: &PositionVector,
    ) -> Result<AcknowledgementTransition, StreamConsumerModelError> {
        let components = self.position_components(positions, true)?;
        let deliveries = self.deliveries_covered_by(&components);
        self.admit_acknowledgement(components, deliveries, false)
            .map(|(authority, components)| AcknowledgementTransition {
                authority: AcknowledgementAuthority {
                    consumer_instance: authority.consumer_instance,
                    delivery_epoch: authority.delivery_epoch,
                    operation_id: authority.operation_id,
                },
                components,
            })
    }

    /// Settle an admitted non-transactional acknowledgement. Only deliveries
    /// whose source is confirmed are resolved; every admitted completion
    /// counter settles so partial failure remains retryable.
    pub fn settle_acknowledgement(
        &mut self,
        authority: &AcknowledgementAuthority,
        confirmed_sources: &BTreeSet<SegmentSource>,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        let pending = self.validate_acknowledgement_authority(
            authority.consumer_instance,
            authority.delivery_epoch,
            authority.operation_id,
            false,
        )?;
        let mut staged = self.clone();
        staged
            .pending_acknowledgements
            .remove(&authority.operation_id);
        let mut actions = staged.settle_acknowledgement_counters(&pending.components, false)?;
        for (sequence, source) in pending.deliveries {
            if confirmed_sources.contains(&source) {
                actions.extend(staged.resolve_delivery_sequence(sequence)?);
            }
        }
        *self = staged;
        Ok(actions)
    }

    /// Cancel an admitted non-transactional operation without resolving any
    /// delivery lease.
    pub fn cancel_acknowledgement(
        &mut self,
        authority: &AcknowledgementAuthority,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.settle_acknowledgement(authority, &BTreeSet::new())
    }

    /// Admit an individual transactional acknowledgement for a live delivery.
    pub fn admit_individual_transactional_acknowledgement(
        &mut self,
        token: &DeliveryToken,
    ) -> Result<TransactionAcknowledgementTransition, StreamConsumerModelError> {
        let validated = self.validate_delivery_token(token)?;
        let component = AcknowledgementComponent {
            source: validated.source.clone(),
            child_generation: validated.child_generation,
            message_ids: vec![validated.message_id],
            message_id_bytes: vec![validated.message_id_bytes],
            cumulative: false,
        };
        self.admit_acknowledgement(
            vec![component],
            vec![(token.dequeue_sequence, validated.source)],
            true,
        )
        .map(
            |(authority, components)| TransactionAcknowledgementTransition {
                authority,
                components,
            },
        )
    }

    /// Admit cumulative transactional acknowledgement of a live position.
    pub fn admit_cumulative_transactional_acknowledgement(
        &mut self,
        token: &DeliveryToken,
    ) -> Result<TransactionAcknowledgementTransition, StreamConsumerModelError> {
        self.validate_delivery_token(token)?;
        let components = self.position_components(token.position_vector(), false)?;
        let deliveries = self.deliveries_covered_by(&components);
        self.admit_acknowledgement(components, deliveries, true)
            .map(
                |(authority, components)| TransactionAcknowledgementTransition {
                    authority,
                    components,
                },
            )
    }

    /// Admit transactional acknowledgement of a restored position vector.
    pub fn admit_position_transactional_acknowledgement(
        &mut self,
        positions: &PositionVector,
    ) -> Result<TransactionAcknowledgementTransition, StreamConsumerModelError> {
        let components = self.position_components(positions, true)?;
        let deliveries = self.deliveries_covered_by(&components);
        self.admit_acknowledgement(components, deliveries, true)
            .map(
                |(authority, components)| TransactionAcknowledgementTransition {
                    authority,
                    components,
                },
            )
    }

    /// Cancel failed transaction registration/ack admission while retaining
    /// every delivery lease for retry or redelivery.
    pub fn cancel_transactional_acknowledgement(
        &mut self,
        authority: &TransactionAcknowledgementAuthority,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        let pending = self.validate_acknowledgement_authority(
            authority.consumer_instance,
            authority.delivery_epoch,
            authority.operation_id,
            true,
        )?;
        let mut staged = self.clone();
        staged
            .pending_transaction_acknowledgements
            .remove(&authority.operation_id);
        let actions = staged.settle_acknowledgement_counters(&pending.components, true)?;
        *self = staged;
        Ok(actions)
    }

    /// Apply commit, abort, or unknown outcome to one retained transaction
    /// authority. Commit alone resolves represented live delivery leases.
    pub fn settle_transactional_acknowledgement(
        &mut self,
        authority: &TransactionAcknowledgementAuthority,
        outcome: TransactionAcknowledgementOutcome,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.validate_acknowledgement_authority(
            authority.consumer_instance,
            authority.delivery_epoch,
            authority.operation_id,
            true,
        )
        .and_then(|pending| {
            let mut staged = self.clone();
            staged
                .pending_transaction_acknowledgements
                .remove(&authority.operation_id);
            let mut actions = staged.settle_acknowledgement_counters(&pending.components, true)?;
            if outcome == TransactionAcknowledgementOutcome::Committed {
                for (sequence, _) in pending.deliveries {
                    actions.extend(staged.resolve_delivery_sequence(sequence)?);
                }
            } else if outcome == TransactionAcknowledgementOutcome::Unknown {
                actions.extend(staged.require_resync()?);
            }
            *self = staged;
            Ok(actions)
        })
    }

    /// Record ordinary terminal/end-of-topic for a child.
    ///
    /// # Errors
    ///
    /// Rejects stale child generations.
    pub fn observe_terminal(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.require_callback_phase()?;
        let child = self.child(segment_id, generation)?;
        let phase = child.phase.clone();
        let reservation = child.flow_reservation;
        let owner = child.owner();
        if phase == SegmentPhase::Opening {
            return Err(StreamConsumerModelError::InvalidChildTransition { segment_id, phase });
        }
        let pre_terminal_reservations = reservation
            .map(|_| {
                child
                    .completion
                    .pre_terminal_reservations
                    .checked_sub(1)
                    .ok_or(StreamConsumerModelError::UnbalancedCompletionHook {
                        segment_id,
                        kind: "pre-terminal reservation",
                    })
            })
            .transpose()?;
        reservation
            .map_or(Ok(()), |reservation| {
                self.budget
                    .owned(reservation, owner, BudgetUse::FlowPermit)
                    .and_then(|_| self.budget.release(reservation))
                    .map_err(StreamConsumerModelError::from)
            })
            .and_then(|()| {
                let child = self.child_mut(segment_id, generation)?;
                child.flow_reservation = None;
                child.flow_purpose = None;
                if let Some(pre_terminal_reservations) = pre_terminal_reservations {
                    child.completion.pre_terminal_reservations = pre_terminal_reservations;
                }
                child.completion.terminal = true;
                if !matches!(
                    child.phase,
                    SegmentPhase::Draining | SegmentPhase::Closing | SegmentPhase::Failed
                ) {
                    child.phase = SegmentPhase::Terminal;
                }
                let mut actions = self.close_if_drained(segment_id, generation)?;
                actions.extend(self.arbitrate_flow()?);
                Ok(actions)
            })
    }

    /// Admit an acknowledgement operation that must settle before completion.
    ///
    /// # Errors
    ///
    /// Rejects stale child generations.
    pub fn begin_ack(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
    ) -> Result<(), StreamConsumerModelError> {
        self.require_open()?;
        let child = self.child_mut(segment_id, generation)?;
        let exhausted = StreamConsumerModelError::CompletionCounterExhausted {
            segment_id,
            kind: "acknowledgement",
        };
        child.completion.acknowledgements = child
            .completion
            .acknowledgements
            .checked_add(1)
            .ok_or(exhausted)?;
        Ok(())
    }

    /// Settle one admitted acknowledgement operation.
    ///
    /// # Errors
    ///
    /// Rejects unbalanced or stale callbacks.
    pub fn settle_ack(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.require_open()?;
        let child = self.child_mut(segment_id, generation)?;
        child.completion.acknowledgements =
            child.completion.acknowledgements.checked_sub(1).ok_or(
                StreamConsumerModelError::UnbalancedCompletionHook {
                    segment_id,
                    kind: "acknowledgement",
                },
            )?;
        self.close_if_drained(segment_id, generation)
    }

    /// Admit a transactional acknowledgement. Admission alone never completes
    /// an ancestry barrier.
    pub fn begin_transactional_ack(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
    ) -> Result<(), StreamConsumerModelError> {
        self.require_open()?;
        let child = self.child_mut(segment_id, generation)?;
        let exhausted = StreamConsumerModelError::CompletionCounterExhausted {
            segment_id,
            kind: "transactional acknowledgement",
        };
        child.completion.transactional_acknowledgements = child
            .completion
            .transactional_acknowledgements
            .checked_add(1)
            .ok_or(exhausted)?;
        Ok(())
    }

    /// Settle one transactional acknowledgement after a confirmed transaction
    /// outcome. Abort and unknown outcomes leave delivery resolution to the
    /// caller and therefore cannot mark the segment complete by themselves.
    pub fn settle_transactional_ack(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.require_open()?;
        let child = self.child_mut(segment_id, generation)?;
        child.completion.transactional_acknowledgements = child
            .completion
            .transactional_acknowledgements
            .checked_sub(1)
            .ok_or(StreamConsumerModelError::UnbalancedCompletionHook {
                segment_id,
                kind: "transactional acknowledgement",
            })?;
        self.close_if_drained(segment_id, generation)
    }

    /// Reserve work accepted before terminal observation.
    pub fn begin_pre_terminal_reservation(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
    ) -> Result<(), StreamConsumerModelError> {
        self.require_open()?;
        let child = self.child_mut(segment_id, generation)?;
        let exhausted = StreamConsumerModelError::CompletionCounterExhausted {
            segment_id,
            kind: "pre-terminal reservation",
        };
        child.completion.pre_terminal_reservations = child
            .completion
            .pre_terminal_reservations
            .checked_add(1)
            .ok_or(exhausted)?;
        Ok(())
    }

    /// Settle one pre-terminal reservation.
    pub fn settle_pre_terminal_reservation(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.require_open()?;
        let child = self.child_mut(segment_id, generation)?;
        child.completion.pre_terminal_reservations = child
            .completion
            .pre_terminal_reservations
            .checked_sub(1)
            .ok_or(StreamConsumerModelError::UnbalancedCompletionHook {
                segment_id,
                kind: "pre-terminal reservation",
            })?;
        self.close_if_drained(segment_id, generation)
    }

    /// Atomically prove one segment complete and re-evaluate every attached
    /// descendant in deterministic id order.
    ///
    /// # Errors
    ///
    /// Returns [`StreamConsumerModelError::SegmentNotComplete`] until terminal,
    /// deliveries, acks, transactions, and reservations all settle.
    pub fn complete_segment(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.require_open()?;
        if !self
            .child_mut(segment_id, generation)?
            .completion
            .complete()
        {
            return Err(StreamConsumerModelError::SegmentNotComplete { segment_id });
        }
        self.completed.insert(segment_id);
        self.arbitrate_flow()
    }

    /// Locally close every child, fence delayed results, and invalidate all
    /// delivery authority. No wire unregister is invented.
    ///
    /// # Errors
    ///
    /// Returns generation exhaustion only; repeated close is an empty no-op.
    pub fn close(&mut self) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        if matches!(self.phase, AggregatePhase::Closing | AggregatePhase::Closed) {
            return Ok(Vec::new());
        }
        let generation = self
            .generation
            .0
            .checked_add(1)
            .ok_or(StreamConsumerModelError::GenerationExhausted)?;
        let delivery_epoch = self
            .delivery_epoch
            .0
            .checked_add(1)
            .ok_or(StreamConsumerModelError::GenerationExhausted)?;
        let mut actions = Vec::new();
        for child in self.children.values() {
            match child.phase {
                SegmentPhase::Opening => actions.push(StreamConsumerAction::CancelOpen {
                    source: child.source.clone(),
                    controller_incarnation: child.controller_incarnation,
                    child_generation: child.generation,
                }),
                SegmentPhase::OpenBlocked(_)
                | SegmentPhase::Flowing
                | SegmentPhase::Terminal
                | SegmentPhase::Seeking
                | SegmentPhase::Failed
                | SegmentPhase::Draining => actions.push(StreamConsumerAction::CloseChild {
                    source: child.source.clone(),
                    controller_incarnation: child.controller_incarnation,
                    child_generation: child.generation,
                }),
                SegmentPhase::Closing => {}
            }
        }

        self.clear_delivered_positions()?;
        self.pending_acknowledgements.clear();
        self.pending_transaction_acknowledgements.clear();
        self.generation = AggregateGeneration(generation);
        self.delivery_epoch = DeliveryEpoch(delivery_epoch);
        self.phase = if self.children.is_empty() {
            AggregatePhase::Closed
        } else {
            AggregatePhase::Closing
        };
        self.pending_ownership.clear();
        self.assignment = None;
        for child in self.children.values_mut() {
            child.phase = SegmentPhase::Closing;
        }
        Ok(actions)
    }

    fn validate_delivery_token(
        &self,
        token: &DeliveryToken,
    ) -> Result<ValidatedDelivery, StreamConsumerModelError> {
        let source = token.stream_message_id.source();
        let segment_id = source.segment_id();
        let owner = BudgetReservationOwner::new(segment_id, token.child_generation);
        let current = self.children.get(&segment_id);
        let valid = token.consumer_instance == self.consumer_instance
            && token.controller_incarnation == self.controller_incarnation
            && token.delivery_epoch == self.delivery_epoch
            && current.is_some_and(|child| {
                child.generation == token.child_generation
                    && child.controller_incarnation == token.controller_incarnation
                    && child.source == *source
                    && !matches!(child.phase, SegmentPhase::Closing | SegmentPhase::Failed)
            })
            && self.live_deliveries.get(&token.dequeue_sequence)
                == Some(&LiveDelivery {
                    owner,
                    reservation: token.reservation,
                    message_id: token.stream_message_id.ordinary_message_id(),
                });
        if !valid {
            return Err(StreamConsumerModelError::StaleDeliveryToken);
        }
        self.require_open()?;
        self.budget
            .owned(token.reservation, owner, BudgetUse::DeliveryLease)?;
        Ok(ValidatedDelivery {
            source: source.clone(),
            child_generation: token.child_generation,
            message_id: token.stream_message_id.ordinary_message_id(),
            message_id_bytes: token.stream_message_id.ordinary_message_id_bytes().to_vec(),
        })
    }

    fn delivery_operation_pending(&self, sequence: DequeueSequence) -> bool {
        self.pending_acknowledgements
            .values()
            .chain(self.pending_transaction_acknowledgements.values())
            .any(|pending| {
                pending
                    .deliveries
                    .iter()
                    .any(|(pending_sequence, _)| *pending_sequence == sequence)
            })
    }

    fn deliveries_covered_by(
        &self,
        components: &[AcknowledgementComponent],
    ) -> Vec<(DequeueSequence, SegmentSource)> {
        self.live_deliveries
            .iter()
            .filter_map(|(sequence, delivery)| {
                let child = self
                    .children
                    .get(&delivery.owner.segment_id())
                    .filter(|child| child.generation == delivery.owner.child_generation())?;
                components
                    .iter()
                    .any(|component| {
                        component.cumulative
                            && component.source == child.source
                            && component.child_generation == child.generation
                            && component
                                .message_ids
                                .first()
                                .is_some_and(|target| delivery.message_id <= *target)
                    })
                    .then(|| (*sequence, child.source.clone()))
            })
            .collect()
    }

    fn position_components(
        &self,
        positions: &PositionVector,
        require_assignment: bool,
    ) -> Result<Vec<AcknowledgementComponent>, StreamConsumerModelError> {
        self.require_open()?;
        if require_assignment && positions.layout_epoch() != self.dag.epoch() {
            return Err(StreamConsumerModelError::PositionLayoutMismatch {
                vector: positions.layout_epoch(),
                dag: self.dag.epoch(),
            });
        }
        let assigned: BTreeSet<SegmentSource> =
            self.assignment
                .as_ref()
                .map_or_else(BTreeSet::new, |assignment| {
                    assignment
                        .segments()
                        .iter()
                        .map(crate::scalable_consumer::AssignedSegment::source)
                        .collect()
                });
        positions
            .stream_message_ids()
            .map(|stream_message_id| {
                let source = stream_message_id.source().clone();
                let child = self
                    .children
                    .get(&source.segment_id())
                    .filter(|child| {
                        child.source == source
                            && !matches!(
                                child.phase,
                                SegmentPhase::Opening
                                    | SegmentPhase::Closing
                                    | SegmentPhase::Seeking
                                    | SegmentPhase::Failed
                            )
                    })
                    .ok_or_else(|| StreamConsumerModelError::PositionSourceUnavailable {
                        segment_source: source.clone(),
                    })?;
                if require_assignment && !assigned.contains(&source) {
                    return Err(StreamConsumerModelError::PositionSourceUnavailable {
                        segment_source: source,
                    });
                }
                Ok(AcknowledgementComponent {
                    source,
                    child_generation: child.generation,
                    message_ids: vec![stream_message_id.ordinary_message_id()],
                    message_id_bytes: vec![stream_message_id.ordinary_message_id_bytes().to_vec()],
                    cumulative: true,
                })
            })
            .collect()
    }

    fn admit_acknowledgement(
        &mut self,
        components: Vec<AcknowledgementComponent>,
        deliveries: Vec<(DequeueSequence, SegmentSource)>,
        transactional: bool,
    ) -> Result<
        (
            TransactionAcknowledgementAuthority,
            Vec<AcknowledgementComponent>,
        ),
        StreamConsumerModelError,
    > {
        self.require_open()?;
        if deliveries
            .iter()
            .any(|(sequence, _)| self.delivery_operation_pending(*sequence))
        {
            return Err(StreamConsumerModelError::DeliveryOperationPending);
        }
        let next = self
            .next_acknowledgement_operation
            .checked_add(1)
            .ok_or(StreamConsumerModelError::AcknowledgementOperationExhausted)?;
        let operation_id = AcknowledgementOperationId(self.next_acknowledgement_operation);
        let mut staged = self.clone();
        components
            .iter()
            .try_for_each(|component| {
                if transactional {
                    staged.begin_transactional_ack(
                        component.source.segment_id(),
                        component.child_generation,
                    )
                } else {
                    staged.begin_ack(component.source.segment_id(), component.child_generation)
                }
            })
            .map(|()| {
                let pending = PendingAcknowledgement {
                    components: components.clone(),
                    deliveries,
                };
                if transactional {
                    staged
                        .pending_transaction_acknowledgements
                        .insert(operation_id, pending);
                } else {
                    staged
                        .pending_acknowledgements
                        .insert(operation_id, pending);
                }
                staged.next_acknowledgement_operation = next;
                *self = staged;
                (
                    TransactionAcknowledgementAuthority {
                        consumer_instance: self.consumer_instance,
                        delivery_epoch: self.delivery_epoch,
                        operation_id,
                    },
                    components,
                )
            })
    }

    fn validate_acknowledgement_authority(
        &self,
        consumer_instance: ConsumerInstanceId,
        delivery_epoch: DeliveryEpoch,
        operation_id: AcknowledgementOperationId,
        transactional: bool,
    ) -> Result<PendingAcknowledgement, StreamConsumerModelError> {
        self.require_open()?;
        if consumer_instance != self.consumer_instance || delivery_epoch != self.delivery_epoch {
            return Err(StreamConsumerModelError::StaleAcknowledgementAuthority);
        }
        let pending = if transactional {
            self.pending_transaction_acknowledgements.get(&operation_id)
        } else {
            self.pending_acknowledgements.get(&operation_id)
        };
        pending
            .cloned()
            .ok_or(StreamConsumerModelError::StaleAcknowledgementAuthority)
    }

    fn settle_acknowledgement_counters(
        &mut self,
        components: &[AcknowledgementComponent],
        transactional: bool,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        components
            .iter()
            .try_fold(Vec::new(), |mut actions, component| {
                let settled = if transactional {
                    self.settle_transactional_ack(
                        component.source.segment_id(),
                        component.child_generation,
                    )
                } else {
                    self.settle_ack(component.source.segment_id(), component.child_generation)
                };
                settled.map(|settled| {
                    actions.extend(settled);
                    actions
                })
            })
    }

    fn resolve_delivery_sequence(
        &mut self,
        sequence: DequeueSequence,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        self.require_open()?;
        let delivery = self
            .live_deliveries
            .get(&sequence)
            .copied()
            .ok_or(StreamConsumerModelError::StaleDeliveryToken)?;
        let segment_id = delivery.owner.segment_id();
        let generation = delivery.owner.child_generation();
        let child = self.child(segment_id, generation)?;
        if matches!(child.phase, SegmentPhase::Closing | SegmentPhase::Failed) {
            return Err(StreamConsumerModelError::StaleDeliveryToken);
        }
        let unbalanced = StreamConsumerModelError::UnbalancedCompletionHook {
            segment_id,
            kind: "delivery",
        };
        let delivery_count = child
            .completion
            .deliveries
            .checked_sub(1)
            .ok_or(unbalanced)?;
        self.budget
            .owned(
                delivery.reservation,
                delivery.owner,
                BudgetUse::DeliveryLease,
            )
            .and_then(|_| self.budget.release(delivery.reservation))
            .map_err(StreamConsumerModelError::from)
            .and_then(|()| {
                self.live_deliveries.remove(&sequence);
                let child = self.child_mut(segment_id, generation)?;
                child.completion.deliveries = delivery_count;
                let mut actions = self.close_if_drained(segment_id, generation)?;
                actions.extend(self.arbitrate_flow()?);
                Ok(actions)
            })
    }

    fn refresh_delivered_position(&mut self) -> Result<(), StreamConsumerModelError> {
        let position = PositionVector::from_canonical(self.dag.epoch(), &self.delivered_positions)?;
        let (position_heap, canonical_heap) = self.delivered_positions.first_key_value().map_or(
            Ok((0, 0)),
            |(source, message_id)| {
                prospective_position_heap_bytes(
                    &self.delivered_positions,
                    source,
                    message_id,
                    false,
                )
            },
        )?;
        self.delivered_positions_reservation
            .map_or(Ok(()), |reservation| {
                position
                    .encoded_len()
                    .map_err(StreamConsumerModelError::from)
                    .and_then(|bytes| {
                        bytes
                            .checked_add(position_heap)
                            .and_then(|bytes| bytes.checked_add(canonical_heap))
                            .and_then(|bytes| bytes.checked_add(DELIVERY_AUTHORITY_OVERHEAD))
                            .ok_or(StreamPositionError::LengthOverflow)
                            .map_err(StreamConsumerModelError::from)
                    })
                    .and_then(|bytes| {
                        self.budget
                            .transfer(
                                reservation,
                                BudgetUse::DeliveredPositionMetadata,
                                BudgetUse::DeliveredPositionMetadata,
                                bytes,
                            )
                            .map_err(StreamConsumerModelError::from)
                    })
            })
            .map(|()| self.delivered_position = position)
    }

    fn refresh_ordering(&mut self, segment_id: SegmentId) -> bool {
        if !self
            .children
            .get(&segment_id)
            .is_some_and(|child| matches!(child.phase, SegmentPhase::OpenBlocked(_)))
        {
            return false;
        }
        let eligibility = self.dag.ordering_eligibility(
            segment_id,
            self.ordering_mode,
            &self.ownership_history,
            &self.completed,
        );
        if let Err(OrderingError::OrderingUnprovable { ancestors, .. }) = &eligibility {
            if let Some(child) = self.children.get_mut(&segment_id) {
                child.phase =
                    SegmentPhase::OpenBlocked(FlowBlock::OrderingUnprovable(ancestors.clone()));
            }
            return false;
        }
        eligibility
            .into_iter()
            .any(|eligibility| match eligibility {
                OrderingEligibility::Blocked {
                    incomplete_ancestors,
                    ..
                } => {
                    if let Some(child) = self.children.get_mut(&segment_id) {
                        child.phase = SegmentPhase::OpenBlocked(FlowBlock::Predecessors(
                            incomplete_ancestors,
                        ));
                    }
                    false
                }
                OrderingEligibility::Eligible | OrderingEligibility::BrokerManaged { .. } => {
                    if let Some(child) = self.children.get_mut(&segment_id) {
                        child.phase = SegmentPhase::OpenBlocked(FlowBlock::Budget);
                    }
                    true
                }
            })
    }

    fn arbitrate_flow(&mut self) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        if self.phase != AggregatePhase::Open {
            return Ok(Vec::new());
        }
        let ids: Vec<SegmentId> = self.children.keys().copied().collect();
        let mut eligible = BTreeSet::new();
        for segment_id in &ids {
            if self.refresh_ordering(*segment_id) {
                eligible.insert(*segment_id);
            }
        }
        if eligible.is_empty() {
            return Ok(Vec::new());
        }

        let start = self.flow_cursor.map_or(0, |cursor| {
            ids.iter().position(|id| *id > cursor).unwrap_or(0)
        });
        let ordered = ids[start..].iter().chain(ids[..start].iter()).copied();
        let mut actions = Vec::new();
        let candidates: Vec<_> = ordered
            .filter(|id| eligible.contains(id))
            .filter_map(|segment_id| {
                self.children.get(&segment_id).map(|child| {
                    (
                        segment_id,
                        child.owner(),
                        child.completion.pre_terminal_reservations,
                    )
                })
            })
            .collect();
        for (segment_id, owner, current_pre_terminal_reservations) in candidates {
            let exhausted = StreamConsumerModelError::CompletionCounterExhausted {
                segment_id,
                kind: "pre-terminal reservation",
            };
            let pre_terminal_reservations = current_pre_terminal_reservations
                .checked_add(1)
                .ok_or(exhausted)?;
            let reservation =
                match self
                    .budget
                    .reserve_owned(owner, BudgetUse::FlowPermit, MAX_FRAME_SIZE)
                {
                    Ok(reservation) => reservation,
                    Err(_) => break,
                };
            self.children
                .get_mut(&segment_id)
                .into_iter()
                .for_each(|child| {
                    child.phase = SegmentPhase::Flowing;
                    child.flow_reservation = Some(reservation);
                    child.flow_purpose = Some(FlowPurpose::Message);
                    child.completion.pre_terminal_reservations = pre_terminal_reservations;
                    self.flow_cursor = Some(segment_id);
                    actions.push(StreamConsumerAction::GrantFlow {
                        source: child.source.clone(),
                        controller_incarnation: child.controller_incarnation,
                        child_generation: child.generation,
                        reservation,
                        purpose: FlowPurpose::Message,
                    });
                });
        }
        Ok(actions)
    }

    fn close_if_drained(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
    ) -> Result<Vec<StreamConsumerAction>, StreamConsumerModelError> {
        let child = self.child_mut(segment_id, generation)?;
        if child.phase != SegmentPhase::Draining || !child.drain_settled() {
            return Ok(Vec::new());
        }
        child.phase = SegmentPhase::Closing;
        Ok(vec![StreamConsumerAction::CloseChild {
            source: child.source.clone(),
            controller_incarnation: child.controller_incarnation,
            child_generation: child.generation,
        }])
    }

    fn child_has_only_seek_flow_reservation(&self, child: &ChildState) -> bool {
        let owner = child.owner();
        let mut owned = self
            .budget
            .reservations
            .iter()
            .filter(|(_, reservation)| reservation.owner == Some(owner));
        let actual = owned.next();
        (child.flow_reservation.is_none() && actual.is_none())
            || child
                .flow_reservation
                .zip(actual)
                .is_some_and(|(expected, (reservation, state))| {
                    *reservation == expected
                        && state.use_ == BudgetUse::FlowPermit
                        && owned.next().is_none()
                })
    }

    fn clear_delivered_positions(&mut self) -> Result<(), StreamConsumerModelError> {
        if let Some(reservation) = self.delivered_positions_reservation {
            self.budget.release(reservation)?;
            self.delivered_positions_reservation = None;
        }
        self.delivered_positions.clear();
        self.delivered_position = PositionVector::new(self.dag.epoch(), std::iter::empty())?;
        Ok(())
    }

    fn child(
        &self,
        segment_id: SegmentId,
        generation: ChildGeneration,
    ) -> Result<&ChildState, StreamConsumerModelError> {
        let Some(child) = self.children.get(&segment_id) else {
            return Err(StreamConsumerModelError::UnknownChild { segment_id });
        };
        if child.generation != generation {
            return Err(StreamConsumerModelError::StaleChildGeneration {
                segment_id,
                got: generation,
                expected: child.generation,
            });
        }
        Ok(child)
    }

    fn child_mut(
        &mut self,
        segment_id: SegmentId,
        generation: ChildGeneration,
    ) -> Result<&mut ChildState, StreamConsumerModelError> {
        let Some(child) = self.children.get_mut(&segment_id) else {
            return Err(StreamConsumerModelError::UnknownChild { segment_id });
        };
        if child.generation != generation {
            return Err(StreamConsumerModelError::StaleChildGeneration {
                segment_id,
                got: generation,
                expected: child.generation,
            });
        }
        Ok(child)
    }

    fn require_open(&self) -> Result<(), StreamConsumerModelError> {
        if self.phase == AggregatePhase::Open {
            Ok(())
        } else {
            Err(StreamConsumerModelError::InvalidAggregatePhase { phase: self.phase })
        }
    }

    fn require_callback_phase(&self) -> Result<(), StreamConsumerModelError> {
        if self.phase == AggregatePhase::Closed {
            Err(StreamConsumerModelError::InvalidAggregatePhase { phase: self.phase })
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use prost::Message as _;

    use super::*;
    use crate::dag_watch::DagSnapshot;
    use crate::pb;
    use crate::scalable_consumer::ConsumerAssignment;
    use crate::types::MessageId;

    #[allow(clippy::too_many_arguments)] // Compact generated-wire topology fixture.
    fn info(
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
        let dag = pb::ScalableTopicDag {
            epoch,
            segments: vec![
                info(
                    0,
                    0,
                    65_535,
                    pb::SegmentState::Sealed,
                    &[],
                    &[1, 2],
                    0,
                    Some(1),
                ),
                info(1, 0, 32_767, pb::SegmentState::Active, &[0], &[], 1, None),
                info(
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
        };
        DagSnapshot::try_from_pb(&dag).expect("valid split DAG")
    }

    fn split_dag() -> DagSnapshot {
        split_dag_at(1, "pulsar://broker-1:6650")
    }

    fn assignment(epoch: u64, ids: &[u64]) -> ConsumerAssignment {
        let segments = ids
            .iter()
            .map(|id| {
                let (start, end) = match *id {
                    0 => (0, 65_535),
                    1 => (0, 32_767),
                    2 => (32_768, 65_535),
                    _ => (0, 65_535),
                };
                let range = KeyRange::new(start, end).expect("range");
                pb::ScalableAssignedSegment {
                    segment_id: *id,
                    hash_start: start,
                    hash_end: end,
                    segment_topic: canonical_segment_topic("topic://t/n/x", range, SegmentId(*id))
                        .expect("topic"),
                }
            })
            .collect();
        ConsumerAssignment::try_from_pb(
            &pb::ScalableConsumerAssignment {
                layout_epoch: epoch,
                segments,
            },
            "topic://t/n/x",
        )
        .expect("assignment")
    }

    fn model(mode: OrderingMode) -> StreamConsumerModel {
        model_with_data_capacity(mode, MAX_FRAME_SIZE * 3)
    }

    fn model_with_data_capacity(mode: OrderingMode, data_capacity: usize) -> StreamConsumerModel {
        StreamConsumerModel::new(
            "topic://t/n/x".to_owned(),
            ConsumerInstanceId(10),
            ControllerIncarnation(3),
            mode,
            split_dag(),
            ReceiverBudget::bytes(
                data_capacity + RECEIVER_BUDGET_AUTHORITY_HEADROOM + CONTROL_PLANE_CLEANUP_RESERVE,
            )
            .expect("budget"),
        )
        .expect("model")
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

    fn canonical_ordinary_bytes() -> Vec<u8> {
        pb::MessageIdData {
            ledger_id: 4,
            entry_id: 5,
            partition: Some(0),
            batch_index: Some(1),
            ack_set: vec![3, 5],
            batch_size: Some(2),
            first_chunk_message_id: Some(Box::new(pb::MessageIdData {
                ledger_id: 4,
                entry_id: 3,
                partition: Some(0),
                batch_index: Some(-1),
                ack_set: Vec::new(),
                batch_size: None,
                first_chunk_message_id: None,
            })),
        }
        .encode_to_vec()
    }

    fn stream_id(
        model: &StreamConsumerModel,
        segment_id: SegmentId,
        message_id: MessageId,
    ) -> StreamMessageId {
        let source = model
            .children
            .get(&segment_id)
            .expect("test child")
            .source
            .clone();
        StreamMessageId::new(source, message_id).expect("valid test stream id")
    }

    fn opened_generation(action: &StreamConsumerAction) -> ChildGeneration {
        match action {
            StreamConsumerAction::OpenChild {
                child_generation, ..
            } => *child_generation,
            other => panic!("expected open action, got {other:?}"),
        }
    }

    fn flow_reservation(actions: &[StreamConsumerAction]) -> BudgetReservationId {
        match actions {
            [StreamConsumerAction::GrantFlow { reservation, .. }] => *reservation,
            other => panic!("expected one FLOW action, got {other:?}"),
        }
    }

    fn issue_test_delivery(
        model: &mut StreamConsumerModel,
        segment_id: SegmentId,
        generation: ChildGeneration,
        reservation: BudgetReservationId,
        entry_id: u64,
    ) -> (DeliveryToken, Vec<StreamConsumerAction>) {
        let arrival = model
            .message_arrived(segment_id, generation, reservation, 128)
            .expect("arrival");
        let stream_message_id = stream_id(model, segment_id, message_id(entry_id));
        let token = model
            .issue_delivery(segment_id, generation, stream_message_id, arrival.retained)
            .expect("delivery");
        (token, arrival.actions)
    }

    #[test]
    fn child_opens_early_but_flow_waits_for_local_ancestor_completion() {
        let mut model = model(OrderingMode::Strict);
        let parent_open = model
            .apply_assignment(assignment(1, &[0]))
            .expect("parent assignment");
        let parent_generation = opened_generation(&parent_open[0]);
        assert!(matches!(
            model.child_opened(SegmentId(0), parent_generation),
            Ok(actions) if matches!(actions.as_slice(), [StreamConsumerAction::GrantFlow { .. }])
        ));

        let child_open = model
            .apply_assignment(assignment(1, &[0, 1]))
            .expect("changed equal-epoch assignment");
        let child_generation = opened_generation(&child_open[0]);
        assert!(
            model
                .child_opened(SegmentId(1), child_generation)
                .expect("child attach")
                .is_empty(),
            "attachment is early, FLOW is blocked"
        );
        assert_eq!(
            model.segment_phase(SegmentId(1)),
            Some(&SegmentPhase::OpenBlocked(FlowBlock::Predecessors(vec![
                SegmentId(0)
            ])))
        );

        model
            .observe_terminal(SegmentId(0), parent_generation)
            .expect("terminal");
        let flow = model
            .complete_segment(SegmentId(0), parent_generation)
            .expect("complete parent");
        assert!(flow.iter().any(|action| matches!(
            action,
            StreamConsumerAction::GrantFlow { source, .. }
                if source.segment_id() == SegmentId(1)
        )));
    }

    #[test]
    fn strict_remote_ancestor_is_unprovable_broker_managed_is_eligible() {
        let mut strict = model(OrderingMode::Strict);
        let open = strict
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        assert!(
            strict
                .child_opened(SegmentId(1), generation)
                .expect("open")
                .is_empty()
        );
        assert_eq!(
            strict.segment_phase(SegmentId(1)),
            Some(&SegmentPhase::OpenBlocked(FlowBlock::OrderingUnprovable(
                vec![SegmentId(0)]
            )))
        );

        let mut broker_managed = model(OrderingMode::BrokerManaged);
        let open = broker_managed
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        assert!(matches!(
            broker_managed.child_opened(SegmentId(1), generation),
            Ok(actions) if matches!(actions.as_slice(), [StreamConsumerAction::GrantFlow { .. }])
        ));
    }

    #[test]
    fn consumer_busy_is_pending_until_the_same_open_generation_attaches() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let source = model.children[&SegmentId(1)].source.clone();
        assert!(model.status().pending_ownership().is_empty());

        model
            .child_open_busy(SegmentId(1), generation)
            .expect("busy open remains current");
        model
            .child_open_busy(SegmentId(1), generation)
            .expect("repeated busy result is idempotent");
        assert_eq!(model.status().pending_ownership(), &[source]);

        assert!(matches!(
            model.child_open_busy(SegmentId(1), ChildGeneration(generation.0 + 1)),
            Err(StreamConsumerModelError::StaleChildGeneration { .. })
        ));
        assert!(matches!(
            model.child_opened(SegmentId(1), generation),
            Ok(actions) if matches!(actions.as_slice(), [StreamConsumerAction::GrantFlow { .. }])
        ));
        assert!(model.status().pending_ownership().is_empty());
    }

    #[test]
    fn delivery_authority_is_source_generation_incarnation_and_epoch_bound() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = model.child_opened(SegmentId(1), generation).expect("open");
        let reservation = match &flow[0] {
            StreamConsumerAction::GrantFlow { reservation, .. } => *reservation,
            other => panic!("expected flow, got {other:?}"),
        };
        let arrival = model
            .message_arrived(SegmentId(1), generation, reservation, 128)
            .expect("arrival");
        let stream_message_id = stream_id(&model, SegmentId(1), message_id(5));
        let token = model
            .issue_delivery(
                SegmentId(1),
                generation,
                stream_message_id,
                arrival.retained,
            )
            .expect("delivery");
        assert_eq!(
            token.stream_message_id().source().segment_id(),
            SegmentId(1)
        );
        assert_eq!(token.position_vector().len(), 1);
        model.resolve_delivery(&token).expect("first resolution");
        assert_eq!(
            model.resolve_delivery(&token),
            Err(StreamConsumerModelError::StaleDeliveryToken)
        );
    }

    #[test]
    fn delivery_restoration_validation_is_non_mutating_and_rejects_pending_operations() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = model.child_opened(SegmentId(1), generation).expect("open");
        let (token, _) = issue_test_delivery(
            &mut model,
            SegmentId(1),
            generation,
            flow_reservation(&flow),
            5,
        );

        assert_eq!(model.validate_delivery_restoration(&token), Ok(generation));
        let acknowledgement = model
            .admit_individual_acknowledgement(&token)
            .expect("admit acknowledgement");
        assert_eq!(
            model.validate_delivery_restoration(&token),
            Err(StreamConsumerModelError::DeliveryOperationPending)
        );
        model
            .cancel_acknowledgement(&acknowledgement.authority)
            .expect("cancel acknowledgement");
        assert_eq!(model.validate_delivery_restoration(&token), Ok(generation));
        model
            .resolve_delivery(&token)
            .expect("live token remains resolvable");
    }

    #[test]
    fn individual_acknowledgement_admission_and_settlement_are_one_shot() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = model.child_opened(SegmentId(1), generation).expect("open");
        let (token, _) = issue_test_delivery(
            &mut model,
            SegmentId(1),
            generation,
            flow_reservation(&flow),
            5,
        );
        let source = token.stream_message_id().source().clone();

        let transition = model
            .admit_individual_acknowledgement(&token)
            .expect("admit acknowledgement");
        assert_eq!(transition.components.len(), 1);
        assert_eq!(transition.components[0].source(), &source);
        assert!(!transition.components[0].cumulative());
        assert_eq!(transition.components[0].message_ids(), &[message_id(5)]);
        assert_eq!(
            model.resolve_delivery(&token),
            Err(StreamConsumerModelError::DeliveryOperationPending)
        );

        model
            .settle_acknowledgement(&transition.authority, &BTreeSet::from([source]))
            .expect("settle acknowledgement");
        assert_eq!(
            model.resolve_delivery(&token),
            Err(StreamConsumerModelError::StaleDeliveryToken)
        );
        assert_eq!(
            model.cancel_acknowledgement(&transition.authority),
            Err(StreamConsumerModelError::StaleAcknowledgementAuthority)
        );
    }

    #[test]
    fn batch_partial_failure_resolves_only_confirmed_sources() {
        let mut model = model(OrderingMode::BrokerManaged);
        let opens = model
            .apply_assignment(assignment(1, &[1, 2]))
            .expect("assignment");
        let generation_one = opened_generation(&opens[0]);
        let generation_two = opened_generation(&opens[1]);
        let flow_one = model
            .child_opened(SegmentId(1), generation_one)
            .expect("first open");
        let flow_two = model
            .child_opened(SegmentId(2), generation_two)
            .expect("second open");
        let (token_one, _) = issue_test_delivery(
            &mut model,
            SegmentId(1),
            generation_one,
            flow_reservation(&flow_one),
            5,
        );
        let (token_two, _) = issue_test_delivery(
            &mut model,
            SegmentId(2),
            generation_two,
            flow_reservation(&flow_two),
            7,
        );
        let source_one = token_one.stream_message_id().source().clone();

        let transition = model
            .admit_batch_acknowledgement(&[&token_one, &token_two])
            .expect("admit batch");
        assert_eq!(transition.components.len(), 2);
        model
            .settle_acknowledgement(&transition.authority, &BTreeSet::from([source_one.clone()]))
            .expect("settle partial batch");

        assert_eq!(
            model.resolve_delivery(&token_one),
            Err(StreamConsumerModelError::StaleDeliveryToken)
        );
        model
            .resolve_delivery(&token_two)
            .expect("failed component remains live");
        assert_eq!(
            model.children[&source_one.segment_id()]
                .completion
                .acknowledgements,
            0
        );
        assert_eq!(model.children[&SegmentId(2)].completion.acknowledgements, 0);
    }

    #[test]
    fn cumulative_acknowledgement_resolves_every_covered_delivery() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = model.child_opened(SegmentId(1), generation).expect("open");
        let (first, refill) = issue_test_delivery(
            &mut model,
            SegmentId(1),
            generation,
            flow_reservation(&flow),
            5,
        );
        let (second, _) = issue_test_delivery(
            &mut model,
            SegmentId(1),
            generation,
            flow_reservation(&refill),
            7,
        );
        let source = second.stream_message_id().source().clone();

        let transition = model
            .admit_cumulative_acknowledgement(&second)
            .expect("admit cumulative");
        assert_eq!(transition.components.len(), 1);
        assert!(transition.components[0].cumulative());
        model
            .settle_acknowledgement(&transition.authority, &BTreeSet::from([source]))
            .expect("settle cumulative");

        assert_eq!(
            model.resolve_delivery(&first),
            Err(StreamConsumerModelError::StaleDeliveryToken)
        );
        assert_eq!(
            model.resolve_delivery(&second),
            Err(StreamConsumerModelError::StaleDeliveryToken)
        );
    }

    #[test]
    fn transaction_outcomes_commit_abort_and_fence_unknown() {
        fn transaction_fixture() -> (StreamConsumerModel, DeliveryToken) {
            let mut model = model(OrderingMode::BrokerManaged);
            let open = model
                .apply_assignment(assignment(1, &[1]))
                .expect("assignment");
            let generation = opened_generation(&open[0]);
            let flow = model.child_opened(SegmentId(1), generation).expect("open");
            let (token, _) = issue_test_delivery(
                &mut model,
                SegmentId(1),
                generation,
                flow_reservation(&flow),
                5,
            );
            (model, token)
        }

        let (mut committed, committed_token) = transaction_fixture();
        let transition = committed
            .admit_individual_transactional_acknowledgement(&committed_token)
            .expect("admit committed acknowledgement");
        committed
            .settle_transactional_acknowledgement(
                &transition.authority,
                TransactionAcknowledgementOutcome::Committed,
            )
            .expect("commit");
        assert_eq!(
            committed.resolve_delivery(&committed_token),
            Err(StreamConsumerModelError::StaleDeliveryToken)
        );

        let (mut aborted, aborted_token) = transaction_fixture();
        let transition = aborted
            .admit_individual_transactional_acknowledgement(&aborted_token)
            .expect("admit aborted acknowledgement");
        aborted
            .settle_transactional_acknowledgement(
                &transition.authority,
                TransactionAcknowledgementOutcome::Aborted,
            )
            .expect("abort");
        aborted
            .resolve_delivery(&aborted_token)
            .expect("abort retains delivery authority");

        let (mut unknown, unknown_token) = transaction_fixture();
        let transition = unknown
            .admit_individual_transactional_acknowledgement(&unknown_token)
            .expect("admit unknown acknowledgement");
        let actions = unknown
            .settle_transactional_acknowledgement(
                &transition.authority,
                TransactionAcknowledgementOutcome::Unknown,
            )
            .expect("unknown outcome");
        assert_eq!(unknown.phase(), AggregatePhase::ResyncRequired);
        assert!(matches!(
            actions.as_slice(),
            [StreamConsumerAction::CloseChild { .. }]
        ));
        assert_eq!(
            unknown.resolve_delivery(&unknown_token),
            Err(StreamConsumerModelError::StaleDeliveryToken)
        );
    }

    #[test]
    fn restored_position_requires_current_layout_and_assignment() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = model.child_opened(SegmentId(1), generation).expect("open");
        let (token, _) = issue_test_delivery(
            &mut model,
            SegmentId(1),
            generation,
            flow_reservation(&flow),
            5,
        );
        let positions = token.position_vector().clone();
        let wrong_layout = PositionVector::new(
            positions.layout_epoch() + 1,
            positions.iter().map(|(source, id)| (source.clone(), id)),
        )
        .expect("canonical wrong-layout vector");
        assert!(matches!(
            model.admit_position_acknowledgement(&wrong_layout),
            Err(StreamConsumerModelError::PositionLayoutMismatch { .. })
        ));

        let transition = model
            .admit_position_acknowledgement(&positions)
            .expect("admit restored position");
        let confirmed = transition
            .components
            .iter()
            .map(|component| component.source().clone())
            .collect();
        model
            .settle_acknowledgement(&transition.authority, &confirmed)
            .expect("settle restored position");
        assert_eq!(
            model.resolve_delivery(&token),
            Err(StreamConsumerModelError::StaleDeliveryToken)
        );
    }

    #[test]
    fn live_delivery_preserves_complete_ordinary_message_id_bytes() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = model.child_opened(SegmentId(1), generation).expect("open");
        let retained = model
            .message_arrived(SegmentId(1), generation, flow_reservation(&flow), 128)
            .expect("arrival")
            .retained;
        let source = model.children[&SegmentId(1)].source.clone();
        let ordinary = canonical_ordinary_bytes();
        let stream_message_id =
            StreamMessageId::from_ordinary_bytes(source.clone(), &ordinary).expect("stream id");

        let token = model
            .issue_delivery(SegmentId(1), generation, stream_message_id, retained)
            .expect("delivery");

        assert_eq!(
            token.stream_message_id().ordinary_message_id_bytes(),
            ordinary
        );
        assert_eq!(
            token.position_vector().ordinary_message_id_bytes(&source),
            Some(ordinary.as_slice())
        );
        let acknowledgement = model
            .admit_individual_acknowledgement(&token)
            .expect("live acknowledgement");
        assert_eq!(
            acknowledgement.components[0].message_id_bytes(),
            &[ordinary]
        );
        let decoded = acknowledgement.components[0]
            .message_id_data()
            .expect("complete acknowledgement id");
        assert_eq!(decoded[0].ack_set, vec![3, 5]);
        assert_eq!(
            decoded[0]
                .first_chunk_message_id
                .as_deref()
                .map(|first| first.entry_id),
            Some(3)
        );
    }

    #[test]
    fn failed_arrival_transfer_terminalizes_and_releases_issued_flow() {
        let mut model = model_with_data_capacity(OrderingMode::BrokerManaged, MAX_FRAME_SIZE);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = model.child_opened(SegmentId(1), generation).expect("open");
        let reservation = flow_reservation(&flow);
        let limit = model
            .budget
            .budget
            .data_limit()
            .saturating_sub(RECEIVER_BUDGET_AUTHORITY_HEADROOM);

        assert!(matches!(
            model.message_arrived(SegmentId(1), generation, reservation, limit + 1),
            Err(StreamConsumerModelError::ArrivalAccountingFailed {
                error: BudgetError::MessageTooLargeForBudget { required, limit: got },
                disposition: ArrivalFailureDisposition::Permanent,
                actions,
            }) if required == limit + 1
                && got == limit
                && matches!(actions.as_slice(), [StreamConsumerAction::CloseChild { .. }])
        ));
        assert_eq!(model.budget.data_used(), 0);
        assert_eq!(model.budget.use_of(reservation), None);
        assert_eq!(
            model.segment_phase(SegmentId(1)),
            Some(&SegmentPhase::Failed)
        );
        assert_eq!(model.children[&SegmentId(1)].flow_reservation, None);
        assert_eq!(
            model.children[&SegmentId(1)]
                .completion
                .pre_terminal_reservations,
            0
        );
        assert!(!model.pending_ownership.contains_key(&SegmentId(1)));
        assert!(
            model
                .child_closed(SegmentId(1), generation)
                .expect("permanently failed child closes")
                .is_empty(),
            "permanent failure must not queue a replacement open"
        );
        assert!(model.segment_phase(SegmentId(1)).is_none());
        assert!(
            model
                .apply_assignment(assignment(1, &[1]))
                .expect("same authoritative assignment remains fenced")
                .is_empty()
        );
    }

    #[test]
    fn retryable_arrival_exhaustion_is_explicit_and_reopens_after_close() {
        let mut model = model_with_data_capacity(OrderingMode::BrokerManaged, MAX_FRAME_SIZE * 2);
        let opens = model
            .apply_assignment(assignment(1, &[1, 2]))
            .expect("assignment");
        let generation_one = opened_generation(&opens[0]);
        let generation_two = opened_generation(&opens[1]);
        let flow_one = model
            .child_opened(SegmentId(1), generation_one)
            .expect("first open");
        model
            .child_opened(SegmentId(2), generation_two)
            .expect("second open");

        assert!(matches!(
            model.message_arrived(
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
        assert!(model.pending_ownership.contains_key(&SegmentId(1)));
        assert!(matches!(
            model
                .child_closed(SegmentId(1), generation_one)
                .expect("retryable child close")
                .as_slice(),
            [StreamConsumerAction::OpenChild { .. }]
        ));
    }

    #[test]
    fn failed_delivery_source_validation_leaves_authority_and_position_unchanged() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = model.child_opened(SegmentId(1), generation).expect("open");
        let retained = model
            .message_arrived(SegmentId(1), generation, flow_reservation(&flow), 128)
            .expect("arrival")
            .retained;
        let foreign_source = assignment(1, &[2]).segments()[0].source();
        let foreign = StreamMessageId::new(foreign_source.clone(), message_id(2))
            .expect("valid foreign stream id");

        let expected_source = model.children[&SegmentId(1)].source.clone();
        assert!(matches!(
            model.issue_delivery(SegmentId(1), generation, foreign, retained),
            Err(StreamConsumerModelError::DeliverySourceMismatch { got, expected })
                if got == foreign_source && expected == expected_source
        ));
        assert_eq!(
            model.budget.use_of(retained),
            Some(BudgetUse::RetainedMessage)
        );
        assert_eq!(model.children[&SegmentId(1)].completion.deliveries, 0);
        assert!(model.delivered_positions.is_empty());
        assert!(model.live_deliveries.is_empty());
        assert_eq!(model.next_dequeue_sequence, 0);

        model.next_dequeue_sequence = u64::MAX;
        let valid = stream_id(&model, SegmentId(1), message_id(2));
        assert!(matches!(
            model.issue_delivery(SegmentId(1), generation, valid, retained,),
            Err(StreamConsumerModelError::DequeueSequenceExhausted)
        ));
        assert_eq!(
            model.budget.use_of(retained),
            Some(BudgetUse::RetainedMessage)
        );
        assert_eq!(model.children[&SegmentId(1)].completion.deliveries, 0);
        assert!(model.delivered_positions.is_empty());
        assert!(model.live_deliveries.is_empty());
    }

    #[test]
    fn equal_epoch_revocation_fences_flow_until_close_confirmation() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = model.child_opened(SegmentId(1), generation).expect("open");
        let reservation = flow_reservation(&flow);
        let owner = BudgetReservationOwner::new(SegmentId(1), generation);
        assert_eq!(model.budget.reservations[&reservation].owner, Some(owner));
        assert_eq!(
            model.children[&SegmentId(1)]
                .completion
                .pre_terminal_reservations,
            1
        );

        let actions = model.apply_assignment(assignment(1, &[])).expect("revoke");
        assert!(matches!(
            actions.as_slice(),
            [StreamConsumerAction::CloseChild { .. }]
        ));
        assert_eq!(model.budget.reservations[&reservation].owner, Some(owner));
        assert_eq!(model.budget.data_used(), MAX_FRAME_SIZE);
        assert_eq!(
            model.segment_phase(SegmentId(1)),
            Some(&SegmentPhase::Closing)
        );

        assert!(matches!(
            model.message_arrived(SegmentId(1), generation, reservation, 0),
            Err(StreamConsumerModelError::InvalidChildTransition { .. })
        ));
        model
            .child_closed(SegmentId(1), generation)
            .expect("close confirmation releases detached authority");
        assert_eq!(model.budget.data_used(), 0);
    }

    #[test]
    fn equal_epoch_revocation_retains_live_acknowledgement_authority_while_draining() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = model.child_opened(SegmentId(1), generation).expect("open");
        let (token, _) = issue_test_delivery(
            &mut model,
            SegmentId(1),
            generation,
            flow_reservation(&flow),
            7,
        );

        let actions = model.apply_assignment(assignment(1, &[])).expect("revoke");
        assert!(
            actions
                .iter()
                .all(|action| !matches!(action, StreamConsumerAction::CloseChild { .. }))
        );
        assert_eq!(
            model.segment_phase(SegmentId(1)),
            Some(&SegmentPhase::Draining)
        );

        let acknowledgement = model
            .admit_individual_acknowledgement(&token)
            .expect("draining child retains live acknowledgement authority");
        let confirmed = BTreeSet::from([token.stream_message_id().source().clone()]);
        let close = model
            .settle_acknowledgement(&acknowledgement.authority, &confirmed)
            .expect("acknowledgement settles draining delivery");
        assert!(matches!(
            close.as_slice(),
            [StreamConsumerAction::CloseChild { .. }]
        ));
    }

    #[test]
    fn chunk_assembly_preallocates_each_frame_and_fences_complete_delivery() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = model.child_opened(SegmentId(1), generation).expect("open");
        let first_flow = flow_reservation(&flow);

        let first = model
            .chunk_frame_buffered(SegmentId(1), generation, first_flow, None, MAX_FRAME_SIZE)
            .expect("first chunk reserves complete assembly");
        assert_eq!(first.assembly, first_flow);
        assert_eq!(
            model.budget.use_of(first.assembly),
            Some(BudgetUse::ChunkAssembly)
        );
        let continuation_flow = match first.actions.as_slice() {
            [
                StreamConsumerAction::GrantFlow {
                    reservation,
                    purpose: FlowPurpose::ChunkContinuation { assembly },
                    ..
                },
            ] if *assembly == first.assembly => *reservation,
            other => panic!("expected bounded chunk continuation, got {other:?}"),
        };
        assert!(matches!(
            model.message_arrived(SegmentId(1), generation, continuation_flow, 64),
            Err(StreamConsumerModelError::FlowPurposeMismatch {
                expected: FlowPurpose::Message,
                ..
            })
        ));

        let second = model
            .chunk_frame_buffered(
                SegmentId(1),
                generation,
                continuation_flow,
                Some(first.assembly),
                MAX_FRAME_SIZE,
            )
            .expect("second chunk rotates only its frame reservation");
        let final_flow = flow_reservation(&second.actions);
        let arrival = model
            .chunk_message_arrived(
                SegmentId(1),
                generation,
                final_flow,
                first.assembly,
                &[],
                128,
            )
            .expect("final chunk converts preallocation to retention");
        assert_eq!(model.budget.use_of(first.assembly), None);
        assert_eq!(
            model.budget.use_of(arrival.retained),
            Some(BudgetUse::RetainedMessage)
        );
        assert_eq!(
            model.children[&SegmentId(1)]
                .completion
                .pre_terminal_reservations,
            2,
            "one retained message and one ordinary refill remain"
        );
    }

    #[test]
    fn reservation_owner_rejects_cross_child_delivery() {
        let mut model = model(OrderingMode::BrokerManaged);
        let opens = model
            .apply_assignment(assignment(1, &[1, 2]))
            .expect("assignment");
        let generation_one = opened_generation(&opens[0]);
        let generation_two = opened_generation(&opens[1]);
        let flow_one = model
            .child_opened(SegmentId(1), generation_one)
            .expect("first open");
        let flow_two = model
            .child_opened(SegmentId(2), generation_two)
            .expect("second open");
        let retained_two = model
            .message_arrived(
                SegmentId(2),
                generation_two,
                flow_reservation(&flow_two),
                32,
            )
            .expect("second arrival")
            .retained;
        let stream_one = stream_id(&model, SegmentId(1), message_id(1));

        assert!(matches!(
            model.issue_delivery(
                SegmentId(1),
                generation_one,
                stream_one,
                retained_two,
            ),
            Err(StreamConsumerModelError::Budget(
                BudgetError::ReservationOwnerMismatch { expected, .. }
            )) if expected == BudgetReservationOwner::new(SegmentId(1), generation_one)
        ));
        assert_eq!(
            model.budget.reservations[&retained_two].owner,
            Some(BudgetReservationOwner::new(SegmentId(2), generation_two))
        );
        assert_eq!(
            model.children[&SegmentId(1)]
                .completion
                .pre_terminal_reservations,
            1
        );
        assert!(matches!(
            flow_one.as_slice(),
            [StreamConsumerAction::GrantFlow { .. }]
        ));
    }

    #[test]
    fn zero_byte_message_gets_bounded_retention_and_delivery_metadata_lease() {
        let mut model = model_with_data_capacity(OrderingMode::BrokerManaged, MAX_FRAME_SIZE);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = model.child_opened(SegmentId(1), generation).expect("open");
        let retained = model
            .message_arrived(SegmentId(1), generation, flow_reservation(&flow), 0)
            .expect("empty arrival")
            .retained;
        assert_eq!(
            model.budget.reservations[&retained].bytes,
            MIN_RETAINED_MESSAGE_RESERVATION
        );
        let stream_message_id = stream_id(&model, SegmentId(1), message_id(1));
        let token = model
            .issue_delivery(SegmentId(1), generation, stream_message_id, retained)
            .expect("delivery authority");
        let lease = model.budget.reservations[&retained];
        let vector_bytes = token
            .position_vector()
            .encoded_len()
            .expect("vector length");
        let source = token.stream_message_id().source();
        let (vector_heap, canonical_heap) = prospective_position_heap_bytes(
            &model.delivered_positions,
            source,
            token.stream_message_id(),
            false,
        )
        .expect("position allocation accounting");
        let stream_message_id_heap =
            source.topic().len() + token.stream_message_id().ordinary_message_id_bytes().len();
        let authority_bytes = token
            .stream_message_id()
            .encoded_len()
            .expect("stream id length")
            + stream_message_id_heap
            + vector_bytes
            + vector_heap
            + DELIVERY_AUTHORITY_OVERHEAD;
        let delivered_positions_bytes =
            vector_bytes + vector_heap + canonical_heap + DELIVERY_AUTHORITY_OVERHEAD;
        let delivered_positions_reservation = model
            .delivered_positions_reservation
            .expect("position metadata reservation");
        assert_eq!(lease.use_, BudgetUse::DeliveryLease);
        assert_eq!(lease.authority_bytes, authority_bytes);
        assert_eq!(
            lease.bytes,
            MIN_RETAINED_MESSAGE_RESERVATION + authority_bytes,
            "coexisting payload and live authority are summed"
        );
        assert_eq!(
            model.budget.reservations[&delivered_positions_reservation],
            DataReservation {
                use_: BudgetUse::DeliveredPositionMetadata,
                bytes: delivered_positions_bytes,
                authority_bytes: delivered_positions_bytes,
                owner: None,
            }
        );
        assert_eq!(
            model.budget.data_used(),
            lease.bytes + delivered_positions_bytes
        );
        assert_eq!(
            model.children[&SegmentId(1)]
                .completion
                .pre_terminal_reservations,
            0
        );
        assert_eq!(model.children[&SegmentId(1)].completion.deliveries, 1);
        let refill = model.resolve_delivery(&token).expect("resolve");
        assert!(!model.budget.reservations.contains_key(&retained));
        assert_eq!(
            model.budget.reservations[&delivered_positions_reservation].bytes,
            delivered_positions_bytes,
            "resolved authority releases its lease but retained positions stay charged"
        );
        assert_eq!(
            model.budget.data_used(),
            delivered_positions_bytes + MAX_FRAME_SIZE,
            "retained positions and the next issued permit are both charged"
        );
        assert!(
            matches!(refill.as_slice(), [StreamConsumerAction::GrantFlow { .. }]),
            "resolving live authority must free enough headroom for replacement FLOW"
        );
        assert!(
            model.children[&SegmentId(1)].flow_reservation.is_some(),
            "the minimum budget must retain permanent positions and one maximum-frame permit"
        );
    }

    #[test]
    fn freed_capacity_rotates_fairly_across_eligible_children() {
        let data_capacity = MAX_FRAME_SIZE + 2 * MIN_RETAINED_MESSAGE_RESERVATION;
        let mut model = model_with_data_capacity(OrderingMode::BrokerManaged, data_capacity);
        let opens = model
            .apply_assignment(assignment(1, &[1, 2]))
            .expect("assignment");
        let generation_one = opened_generation(&opens[0]);
        let generation_two = opened_generation(&opens[1]);
        let first = model
            .child_opened(SegmentId(1), generation_one)
            .expect("first child");
        assert!(
            model
                .child_opened(SegmentId(2), generation_two)
                .expect("second child")
                .is_empty()
        );

        let to_two = model
            .message_arrived(SegmentId(1), generation_one, flow_reservation(&first), 0)
            .expect("first arrival");
        let reservation_two = match to_two.actions.as_slice() {
            [
                StreamConsumerAction::GrantFlow {
                    source,
                    reservation,
                    ..
                },
            ] if source.segment_id() == SegmentId(2) => *reservation,
            other => panic!("expected fair grant to segment 2, got {other:?}"),
        };
        let back_to_one = model
            .message_arrived(SegmentId(2), generation_two, reservation_two, 0)
            .expect("second arrival");
        assert!(matches!(
            back_to_one.actions.as_slice(),
            [StreamConsumerAction::GrantFlow { source, .. }]
                if source.segment_id() == SegmentId(1)
        ));
    }

    #[test]
    fn terminal_releases_unconsumed_flow_and_prevents_refill() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = model.child_opened(SegmentId(1), generation).expect("open");
        let reservation = flow_reservation(&flow);
        assert_eq!(model.budget.data_used(), MAX_FRAME_SIZE);

        assert!(
            model
                .observe_terminal(SegmentId(1), generation)
                .expect("terminal")
                .is_empty()
        );
        assert_eq!(model.budget.data_used(), 0);
        assert_eq!(model.budget.use_of(reservation), None);
        assert_eq!(
            model.segment_phase(SegmentId(1)),
            Some(&SegmentPhase::Terminal)
        );
        assert!(
            model
                .complete_segment(SegmentId(1), generation)
                .expect("complete")
                .is_empty()
        );
        assert_eq!(model.budget.data_used(), 0);
    }

    #[test]
    fn replacement_open_waits_for_confirmation_bearing_old_close() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let old_generation = opened_generation(&open[0]);
        let flow = model
            .child_opened(SegmentId(1), old_generation)
            .expect("open");
        let (old_token, _) = issue_test_delivery(
            &mut model,
            SegmentId(1),
            old_generation,
            flow_reservation(&flow),
            3,
        );

        let lost = model
            .apply_assignment(assignment(1, &[]))
            .expect("equal-epoch assignment removes ownership");
        assert!(matches!(
            lost.as_slice(),
            [StreamConsumerAction::StopFlow { .. }]
        ));
        assert_eq!(
            model.segment_phase(SegmentId(1)),
            Some(&SegmentPhase::Draining)
        );

        assert!(
            model
                .apply_assignment(assignment(1, &[1]))
                .expect("regain ownership")
                .is_empty(),
            "the old exclusive child still owns the segment"
        );
        assert!(model.pending_ownership.contains_key(&SegmentId(1)));
        let acknowledgement = model
            .admit_individual_acknowledgement(&old_token)
            .expect("draining delivery remains acknowledgeable");
        let confirmed = BTreeSet::from([old_token.stream_message_id().source().clone()]);
        assert!(matches!(
            model
                .settle_acknowledgement(&acknowledgement.authority, &confirmed)
                .expect("draining acknowledgement settles")
                .as_slice(),
            [StreamConsumerAction::CloseChild { .. }]
        ));
        let replacement = model
            .child_closed(SegmentId(1), old_generation)
            .expect("old close confirmation");
        let new_generation = opened_generation(&replacement[0]);
        assert_ne!(new_generation, old_generation);
        assert_eq!(
            model.segment_phase(SegmentId(1)),
            Some(&SegmentPhase::Opening)
        );
        assert!(!model.pending_ownership.contains_key(&SegmentId(1)));
        assert!(matches!(
            model.child_closed(SegmentId(1), old_generation),
            Err(StreamConsumerModelError::StaleChildGeneration { .. })
        ));
    }

    #[test]
    fn retired_source_is_pruned_before_future_cumulative_positions() {
        let mut model = model(OrderingMode::BrokerManaged);
        let opens = model
            .apply_assignment(assignment(1, &[1, 2]))
            .expect("assignment");
        let generation_one = opened_generation(&opens[0]);
        let generation_two = opened_generation(&opens[1]);
        let flow_one = model
            .child_opened(SegmentId(1), generation_one)
            .expect("first child");
        let flow_two = model
            .child_opened(SegmentId(2), generation_two)
            .expect("second child");
        let (first, _) = issue_test_delivery(
            &mut model,
            SegmentId(1),
            generation_one,
            flow_reservation(&flow_one),
            5,
        );
        let first_source = first.stream_message_id().source().clone();
        let acknowledgement = model
            .admit_individual_acknowledgement(&first)
            .expect("first acknowledgement");
        model
            .settle_acknowledgement(
                &acknowledgement.authority,
                &BTreeSet::from([first_source.clone()]),
            )
            .expect("first acknowledgement settles");

        let close = model
            .apply_assignment(assignment(1, &[2]))
            .expect("first source is revoked");
        assert!(matches!(
            close.as_slice(),
            [StreamConsumerAction::CloseChild { .. }]
        ));
        model
            .child_closed(SegmentId(1), generation_one)
            .expect("retired child closes");
        assert!(!model.delivered_positions.contains_key(&first_source));

        let (second, _) = issue_test_delivery(
            &mut model,
            SegmentId(2),
            generation_two,
            flow_reservation(&flow_two),
            7,
        );
        assert_eq!(second.position_vector().len(), 1);
        assert_eq!(
            second
                .position_vector()
                .iter()
                .next()
                .map(|(source, _)| source.segment_id()),
            Some(SegmentId(2))
        );
        model
            .admit_cumulative_acknowledgement(&second)
            .expect("retired source no longer poisons cumulative acknowledgement");
    }

    #[test]
    fn control_plane_replacement_is_atomic_and_reopens_changed_placement() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("initial assignment");
        let old_generation = opened_generation(&open[0]);
        model
            .child_opened(SegmentId(1), old_generation)
            .expect("old child open");

        let actions = model
            .apply_control_plane_for(
                ControllerIncarnation(3),
                split_dag_at(2, "pulsar://replacement:6650"),
                assignment(2, &[1]),
            )
            .expect("atomic control-plane replacement");
        assert!(matches!(
            actions.as_slice(),
            [StreamConsumerAction::StopFlow {
                controller_incarnation: ControllerIncarnation(3),
                ..
            }]
        ));
        assert_eq!(model.dag().epoch(), 2);
        assert_eq!(
            model.assignment().map(ConsumerAssignment::layout_epoch),
            Some(2)
        );
        assert!(model.pending_ownership.contains_key(&SegmentId(1)));
        assert!(matches!(
            model
                .observe_terminal(SegmentId(1), old_generation)
                .expect("old flow terminal")
                .as_slice(),
            [StreamConsumerAction::CloseChild { .. }]
        ));

        let replacement = model
            .child_closed(SegmentId(1), old_generation)
            .expect("old close confirmation");
        assert!(matches!(
            replacement.as_slice(),
            [StreamConsumerAction::OpenChild {
                controller_incarnation: ControllerIncarnation(3),
                ..
            }]
        ));

        let generation = model.generation();
        assert!(matches!(
            model.apply_control_plane(
                split_dag_at(3, "pulsar://replacement:6650"),
                assignment(2, &[1]),
            ),
            Err(StreamConsumerModelError::Attachment(
                AttachmentError::EpochMismatch {
                    assignment: 2,
                    dag: 3,
                }
            ))
        ));
        assert_eq!(model.dag().epoch(), 2, "failed replacement is atomic");
        assert_eq!(model.generation(), generation);
    }

    #[test]
    fn descriptor_change_upgrades_an_already_draining_child_flow_fence() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("initial assignment");
        let generation = opened_generation(&open[0]);
        let flow = model
            .child_opened(SegmentId(1), generation)
            .expect("child open");
        let arrival = model
            .message_arrived(SegmentId(1), generation, flow_reservation(&flow), 128)
            .expect("retain one delivery so ordinary handoff remains draining");
        let stream_message_id = stream_id(&model, SegmentId(1), message_id(5));
        let token = model
            .issue_delivery(
                SegmentId(1),
                generation,
                stream_message_id,
                arrival.retained,
            )
            .expect("live delivery");
        let flow = flow_reservation(&arrival.actions);

        assert!(matches!(
            model
                .apply_assignment(assignment(1, &[]))
                .expect("ordinary handoff drain")
                .as_slice(),
            [StreamConsumerAction::StopFlow { .. }]
        ));
        assert!(!model.children[&SegmentId(1)].wait_for_flow_drain);

        let actions = model
            .apply_control_plane_for(
                ControllerIncarnation(3),
                split_dag_at(2, "pulsar://replacement:6650"),
                assignment(2, &[]),
            )
            .expect("descriptor replacement upgrades the retained drain");
        assert!(actions.is_empty());
        assert!(model.children[&SegmentId(1)].wait_for_flow_drain);
        assert_eq!(model.budget().use_of(flow), Some(BudgetUse::FlowPermit));
        assert!(
            model
                .resolve_delivery(&token)
                .expect("delivery resolution waits for old descriptor flow")
                .is_empty()
        );
        assert_eq!(
            model.segment_phase(SegmentId(1)),
            Some(&SegmentPhase::Draining)
        );
        assert!(matches!(
            model
                .observe_terminal(SegmentId(1), generation)
                .expect("terminal fences the old descriptor flow")
                .as_slice(),
            [StreamConsumerAction::CloseChild { .. }]
        ));
        assert_eq!(model.budget().use_of(flow), None);
        assert_eq!(
            model.segment_phase(SegmentId(1)),
            Some(&SegmentPhase::Closing)
        );
    }

    #[test]
    fn controller_replacement_fences_tokens_callbacks_and_child_reopen() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("initial assignment");
        let old_generation = opened_generation(&open[0]);
        let flow = model
            .child_opened(SegmentId(1), old_generation)
            .expect("old child open");
        let retained = model
            .message_arrived(SegmentId(1), old_generation, flow_reservation(&flow), 128)
            .expect("arrival")
            .retained;
        let stream_message_id = stream_id(&model, SegmentId(1), message_id(2));
        let token = model
            .issue_delivery(SegmentId(1), old_generation, stream_message_id, retained)
            .expect("delivery");

        let close = model
            .begin_controller_incarnation(ControllerIncarnation(4))
            .expect("new incarnation");
        assert!(matches!(
            close.as_slice(),
            [StreamConsumerAction::CloseChild {
                controller_incarnation: ControllerIncarnation(3),
                ..
            }]
        ));
        assert_eq!(model.controller_incarnation(), ControllerIncarnation(4));
        assert!(model.budget().data_used() > 0);
        assert_eq!(
            model.resolve_delivery(&token),
            Err(StreamConsumerModelError::StaleDeliveryToken)
        );
        assert!(matches!(
            model.apply_assignment_for(ControllerIncarnation(3), assignment(1, &[1])),
            Err(StreamConsumerModelError::Assignment(
                AssignmentError::IncarnationMismatch { .. }
            ))
        ));

        assert!(
            model
                .apply_assignment_for(ControllerIncarnation(4), assignment(1, &[1]))
                .expect("replacement baseline")
                .is_empty(),
            "the replacement must wait for the old exclusive child"
        );
        let reopen = model
            .child_closed(SegmentId(1), old_generation)
            .expect("old close confirmation");
        assert_eq!(model.budget().data_used(), 0);
        assert!(matches!(
            reopen.as_slice(),
            [StreamConsumerAction::OpenChild {
                controller_incarnation: ControllerIncarnation(4),
                ..
            }]
        ));
        assert!(matches!(
            model.begin_controller_incarnation(ControllerIncarnation(4)),
            Err(StreamConsumerModelError::Assignment(
                AssignmentError::NonAdvancingIncarnation { .. }
            ))
        ));
    }

    #[test]
    fn aggregate_seek_is_atomic_fences_epoch_and_waits_for_all_confirmations() {
        let mut model = model(OrderingMode::BrokerManaged);
        let opens = model
            .apply_assignment(assignment(1, &[1, 2]))
            .expect("assignment");
        let generation_one = opened_generation(&opens[0]);
        let generation_two = opened_generation(&opens[1]);
        model
            .child_opened(SegmentId(1), generation_one)
            .expect("first open");
        model
            .child_opened(SegmentId(2), generation_two)
            .expect("second open");
        let sources = [
            model.children[&SegmentId(1)].source.clone(),
            model.children[&SegmentId(2)].source.clone(),
        ];
        let wrong_epoch = PositionVector::new(
            2,
            [
                (sources[0].clone(), message_id(10)),
                (sources[1].clone(), message_id(20)),
            ],
        )
        .expect("wrong-epoch vector");
        let delivery_epoch = model.delivery_epoch();
        let data_used = model.budget().data_used();
        assert_eq!(
            model.begin_seek(&wrong_epoch),
            Err(StreamConsumerModelError::SeekLayoutMismatch { vector: 2, dag: 1 })
        );
        assert_eq!(model.delivery_epoch(), delivery_epoch);
        assert_eq!(model.budget().data_used(), data_used);

        let vector = PositionVector::new(
            1,
            [
                (sources[0].clone(), message_id(10)),
                (sources[1].clone(), message_id(20)),
            ],
        )
        .expect("seek vector");
        let actions = model.begin_seek(&vector).expect("seek begins atomically");
        assert_eq!(actions.len(), 4);
        assert_eq!(model.delivery_epoch(), DeliveryEpoch(delivery_epoch.0 + 1));
        assert_eq!(model.budget().data_used(), 0);
        assert_eq!(
            model.segment_phase(SegmentId(1)),
            Some(&SegmentPhase::Seeking)
        );
        assert_eq!(
            model.segment_phase(SegmentId(2)),
            Some(&SegmentPhase::Seeking)
        );
        assert!(
            model
                .seek_completed(SegmentId(1), generation_one)
                .expect("first confirmation")
                .is_empty()
        );
        let flow = model
            .seek_completed(SegmentId(2), generation_two)
            .expect("second confirmation");
        assert_eq!(
            flow.iter()
                .filter(|action| matches!(action, StreamConsumerAction::GrantFlow { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn seek_action_preserves_ack_set_and_first_chunk_message_id() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        model.child_opened(SegmentId(1), generation).expect("open");
        let source = model.children[&SegmentId(1)].source.clone();
        let ordinary = canonical_ordinary_bytes();
        let stream_message_id =
            StreamMessageId::from_ordinary_bytes(source.clone(), &ordinary).expect("stream id");
        let vector = PositionVector::from_canonical(
            1,
            &BTreeMap::from([(source.clone(), stream_message_id)]),
        )
        .expect("canonical vector");

        let actions = model.begin_seek(&vector).expect("seek");
        let target = actions
            .iter()
            .find_map(|action| match action {
                StreamConsumerAction::SeekChild {
                    stream_message_id, ..
                } => Some(stream_message_id),
                _ => None,
            })
            .expect("seek action");
        assert_eq!(target.source(), &source);
        assert_eq!(target.ordinary_message_id_bytes(), ordinary);
        let decoded = pb::MessageIdData::decode(target.ordinary_message_id_bytes())
            .expect("canonical ordinary id");
        assert_eq!(decoded.ack_set, vec![3, 5]);
        assert_eq!(
            decoded.first_chunk_message_id.map(|first| first.entry_id),
            Some(3)
        );
    }

    #[test]
    fn seek_rejects_retained_work_on_assigned_and_draining_children() {
        let mut assigned = model_with_data_capacity(OrderingMode::BrokerManaged, MAX_FRAME_SIZE);
        let open = assigned
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = assigned
            .child_opened(SegmentId(1), generation)
            .expect("open");
        let source = assigned.children[&SegmentId(1)].source.clone();
        assigned
            .message_arrived(SegmentId(1), generation, flow_reservation(&flow), 128)
            .expect("retained message");
        let vector = PositionVector::new(1, [(source, message_id(10))]).expect("vector");
        let epoch = assigned.delivery_epoch();
        let used = assigned.budget().data_used();
        assert_eq!(
            assigned.begin_seek(&vector),
            Err(StreamConsumerModelError::ConcurrentSeek)
        );
        assert_eq!(assigned.delivery_epoch(), epoch);
        assert_eq!(assigned.budget().data_used(), used);

        let mut draining = model(OrderingMode::BrokerManaged);
        let opens = draining
            .apply_assignment(assignment(1, &[1, 2]))
            .expect("assignment");
        let generation_one = opened_generation(&opens[0]);
        let generation_two = opened_generation(&opens[1]);
        draining
            .child_opened(SegmentId(1), generation_one)
            .expect("first open");
        let flow_two = draining
            .child_opened(SegmentId(2), generation_two)
            .expect("second open");
        draining
            .message_arrived(
                SegmentId(2),
                generation_two,
                flow_reservation(&flow_two),
                128,
            )
            .expect("retained draining message");
        draining
            .apply_control_plane(
                split_dag_at(2, "pulsar://broker-1:6650"),
                assignment(2, &[1]),
            )
            .expect("revoke second child");
        assert_eq!(
            draining.segment_phase(SegmentId(2)),
            Some(&SegmentPhase::Draining)
        );
        let vector = PositionVector::new(
            2,
            [(
                draining.children[&SegmentId(1)].source.clone(),
                message_id(10),
            )],
        )
        .expect("current assignment vector");
        assert_eq!(
            draining.begin_seek(&vector),
            Err(StreamConsumerModelError::ConcurrentSeek)
        );
    }

    #[test]
    fn failed_seek_enters_resync_and_rejects_late_component_success() {
        let mut model = model(OrderingMode::BrokerManaged);
        let opens = model
            .apply_assignment(assignment(1, &[1, 2]))
            .expect("assignment");
        let generation_one = opened_generation(&opens[0]);
        let generation_two = opened_generation(&opens[1]);
        model
            .child_opened(SegmentId(1), generation_one)
            .expect("first open");
        model
            .child_opened(SegmentId(2), generation_two)
            .expect("second open");
        let vector = PositionVector::new(
            1,
            [
                (model.children[&SegmentId(1)].source.clone(), message_id(10)),
                (model.children[&SegmentId(2)].source.clone(), message_id(20)),
            ],
        )
        .expect("seek vector");
        model.begin_seek(&vector).expect("seek begins");
        model
            .seek_completed(SegmentId(1), generation_one)
            .expect("first seek succeeds");

        let close = model.seek_failed().expect("second seek fails closed");
        assert_eq!(model.phase(), AggregatePhase::ResyncRequired);
        assert_eq!(
            close
                .iter()
                .filter(|action| matches!(action, StreamConsumerAction::CloseChild { .. }))
                .count(),
            2
        );
        assert!(matches!(
            model.seek_completed(SegmentId(2), generation_two),
            Err(StreamConsumerModelError::InvalidAggregatePhase {
                phase: AggregatePhase::ResyncRequired,
            })
        ));
    }

    #[test]
    fn failure_resync_fences_tokens_retains_leases_and_recovers_on_new_incarnation() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = model.child_opened(SegmentId(1), generation).expect("open");
        let retained = model
            .message_arrived(SegmentId(1), generation, flow_reservation(&flow), 32)
            .expect("arrival")
            .retained;
        let stream_message_id = stream_id(&model, SegmentId(1), message_id(1));
        let token = model
            .issue_delivery(SegmentId(1), generation, stream_message_id, retained)
            .expect("delivery");
        let epoch = model.delivery_epoch();
        let actions = model.require_resync().expect("enter resync");
        assert!(matches!(
            actions.as_slice(),
            [StreamConsumerAction::CloseChild { .. }]
        ));
        assert_eq!(model.phase(), AggregatePhase::ResyncRequired);
        assert_eq!(model.delivery_epoch(), DeliveryEpoch(epoch.0 + 1));
        assert!(model.budget().data_used() > 0);
        assert_eq!(
            model.resolve_delivery(&token),
            Err(StreamConsumerModelError::StaleDeliveryToken)
        );
        model
            .child_closed(SegmentId(1), generation)
            .expect("close confirmation");
        assert_eq!(model.budget().data_used(), 0);
        model
            .begin_controller_incarnation(ControllerIncarnation(4))
            .expect("new controller incarnation");
        assert_eq!(model.phase(), AggregatePhase::Open);
        assert!(matches!(
            model
                .apply_assignment_for(ControllerIncarnation(4), assignment(1, &[1]))
                .expect("replacement baseline")
                .as_slice(),
            [StreamConsumerAction::OpenChild {
                controller_incarnation: ControllerIncarnation(4),
                ..
            }]
        ));
    }

    #[test]
    fn failure_resync_generation_exhaustion_is_atomic() {
        let mut model = model(OrderingMode::BrokerManaged);
        model.delivery_epoch = DeliveryEpoch(u64::MAX);
        assert_eq!(
            model.require_resync(),
            Err(StreamConsumerModelError::GenerationExhausted)
        );
        assert_eq!(model.phase(), AggregatePhase::Open);
        assert_eq!(model.generation(), AggregateGeneration(0));
        assert_eq!(model.delivery_epoch(), DeliveryEpoch(u64::MAX));
    }

    #[test]
    fn draining_child_closes_when_its_last_ack_settles() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        model.child_opened(SegmentId(1), generation).expect("open");
        model.begin_ack(SegmentId(1), generation).expect("ack");

        let lost = model
            .apply_control_plane(
                split_dag_at(2, "pulsar://broker-1:6650"),
                assignment(2, &[]),
            )
            .expect("remove ownership");
        assert!(matches!(
            lost.as_slice(),
            [StreamConsumerAction::StopFlow { .. }]
        ));
        assert_eq!(
            model.segment_phase(SegmentId(1)),
            Some(&SegmentPhase::Draining)
        );
        assert!(matches!(
            model
                .settle_ack(SegmentId(1), generation)
                .expect("ack settled")
                .as_slice(),
            [StreamConsumerAction::CloseChild { .. }]
        ));
        assert_eq!(
            model.segment_phase(SegmentId(1)),
            Some(&SegmentPhase::Closing)
        );
    }

    #[test]
    fn assignment_and_close_generation_failures_are_atomic() {
        let mut assignment_model = model(OrderingMode::BrokerManaged);
        assignment_model.next_child_generation = u64::MAX;
        assert_eq!(
            assignment_model.apply_assignment(assignment(1, &[1])),
            Err(StreamConsumerModelError::GenerationExhausted)
        );
        assert_eq!(assignment_model.generation(), AggregateGeneration(0));
        assert!(assignment_model.assignment.is_none());
        assert!(assignment_model.children.is_empty());

        let mut close_model = model(OrderingMode::BrokerManaged);
        close_model.delivery_epoch = DeliveryEpoch(u64::MAX);
        assert_eq!(
            close_model.close(),
            Err(StreamConsumerModelError::GenerationExhausted)
        );
        assert_eq!(close_model.generation(), AggregateGeneration(0));
        assert_eq!(close_model.delivery_epoch(), DeliveryEpoch(u64::MAX));
        assert_eq!(close_model.phase(), AggregatePhase::Open);
    }

    #[test]
    fn foreign_parent_assignment_is_rejected_without_state_change() {
        let range = KeyRange::new(0, 32_767).expect("range");
        let foreign_parent = "topic://t/n/y";
        let foreign = ConsumerAssignment::try_from_pb(
            &pb::ScalableConsumerAssignment {
                layout_epoch: 1,
                segments: vec![pb::ScalableAssignedSegment {
                    segment_id: 1,
                    hash_start: range.start(),
                    hash_end: range.end(),
                    segment_topic: canonical_segment_topic(foreign_parent, range, SegmentId(1))
                        .expect("topic"),
                }],
            },
            foreign_parent,
        )
        .expect("foreign assignment");
        let mut model = model(OrderingMode::BrokerManaged);

        assert_eq!(
            model.apply_assignment(foreign),
            Err(StreamConsumerModelError::AssignmentParentMismatch {
                segment_id: SegmentId(1),
                got: foreign_parent.to_owned(),
                expected: "topic://t/n/x".to_owned(),
            })
        );
        assert_eq!(model.generation(), AggregateGeneration(0));
        assert!(model.assignment.is_none());
        assert!(model.children.is_empty());
    }

    #[test]
    fn close_releases_every_data_reservation_and_invalidates_delivery() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = model.child_opened(SegmentId(1), generation).expect("open");
        let retained = model
            .message_arrived(SegmentId(1), generation, flow_reservation(&flow), 128)
            .expect("arrival")
            .retained;
        let stream_message_id = stream_id(&model, SegmentId(1), message_id(2));
        let token = model
            .issue_delivery(SegmentId(1), generation, stream_message_id, retained)
            .expect("delivery");
        assert!(model.budget.data_used() > 0);

        assert!(matches!(
            model.close().expect("close").as_slice(),
            [StreamConsumerAction::CloseChild { .. }]
        ));
        assert_eq!(model.phase(), AggregatePhase::Closing);
        assert!(model.budget.data_used() > 0);
        assert_eq!(
            model.resolve_delivery(&token),
            Err(StreamConsumerModelError::StaleDeliveryToken)
        );
        model
            .child_closed(SegmentId(1), generation)
            .expect("close confirmation");
        assert_eq!(model.phase(), AggregatePhase::Closed);
        assert_eq!(model.budget.data_used(), 0);
        assert!(model.budget.reservations.is_empty());
        assert!(model.live_deliveries.is_empty());
    }

    #[test]
    fn completion_counter_overflow_is_reported_without_mutation() {
        let mut model = model(OrderingMode::BrokerManaged);
        let open = model
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        model
            .children
            .get_mut(&SegmentId(1))
            .expect("child")
            .completion
            .acknowledgements = usize::MAX;
        assert_eq!(
            model.begin_ack(SegmentId(1), generation),
            Err(StreamConsumerModelError::CompletionCounterExhausted {
                segment_id: SegmentId(1),
                kind: "acknowledgement",
            })
        );
        assert_eq!(
            model.children[&SegmentId(1)].completion.acknowledgements,
            usize::MAX
        );
    }

    #[test]
    fn every_completion_barrier_blocks_ancestor_completion() {
        let mut model = model(OrderingMode::Strict);
        let open = model
            .apply_assignment(assignment(1, &[0]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        model.child_opened(SegmentId(0), generation).expect("open");
        model
            .observe_terminal(SegmentId(0), generation)
            .expect("terminal");
        model.begin_ack(SegmentId(0), generation).expect("ack");
        model
            .begin_transactional_ack(SegmentId(0), generation)
            .expect("txn ack");
        model
            .begin_pre_terminal_reservation(SegmentId(0), generation)
            .expect("reservation");
        assert_eq!(
            model.complete_segment(SegmentId(0), generation),
            Err(StreamConsumerModelError::SegmentNotComplete {
                segment_id: SegmentId(0)
            })
        );
        model
            .settle_ack(SegmentId(0), generation)
            .expect("ack done");
        model
            .settle_transactional_ack(SegmentId(0), generation)
            .expect("txn done");
        model
            .settle_pre_terminal_reservation(SegmentId(0), generation)
            .expect("reservation done");
        model
            .complete_segment(SegmentId(0), generation)
            .expect("complete");
    }

    #[test]
    fn controlled_receive_allocations_are_reserved_before_atomic_batch_arrival() {
        let mut bounded = model_with_data_capacity(OrderingMode::BrokerManaged, MAX_FRAME_SIZE);
        let open = bounded
            .apply_assignment(assignment(1, &[1]))
            .expect("bounded assignment");
        let generation = opened_generation(&open[0]);
        bounded
            .child_opened(SegmentId(1), generation)
            .expect("bounded child");
        let used = bounded.budget.data_used();
        assert!(matches!(
            bounded.reserve_decompression(SegmentId(1), generation, 1),
            Err(StreamConsumerModelError::Budget(
                BudgetError::Exhausted { .. }
            ))
        ));
        assert_eq!(bounded.budget.data_used(), used);
        assert_eq!(
            bounded.children[&SegmentId(1)]
                .completion
                .pre_terminal_reservations,
            1,
            "failed preallocation leaves the issued frame reservation unchanged"
        );

        let mut roomy = model(OrderingMode::BrokerManaged);
        let open = roomy
            .apply_assignment(assignment(1, &[1]))
            .expect("assignment");
        let generation = opened_generation(&open[0]);
        let flow = roomy.child_opened(SegmentId(1), generation).expect("open");
        let flow = flow_reservation(&flow);
        let decompression = roomy
            .reserve_decompression(SegmentId(1), generation, 2 * 1024 * 1024)
            .expect("decompression preallocation");
        let batch = roomy
            .reserve_batch_assembly(SegmentId(1), generation, 1024 * 1024)
            .expect("batch preallocation");
        let arrival = roomy
            .batch_arrived_preallocated(
                SegmentId(1),
                generation,
                flow,
                &[decompression, batch],
                &[1024 * 1024, 1024 * 1024],
            )
            .expect("batch converts all work atomically");
        assert_eq!(arrival.retained.len(), 2);
        assert_eq!(roomy.budget.use_of(decompression), None);
        assert_eq!(roomy.budget.use_of(batch), None);
        assert!(arrival.retained.iter().all(
            |reservation| roomy.budget.use_of(*reservation) == Some(BudgetUse::RetainedMessage)
        ));

        let mut insufficient = model(OrderingMode::BrokerManaged);
        let open = insufficient
            .apply_assignment(assignment(1, &[1]))
            .expect("insufficient assignment");
        let generation = opened_generation(&open[0]);
        let flow = insufficient
            .child_opened(SegmentId(1), generation)
            .expect("insufficient child");
        let flow = flow_reservation(&flow);
        let workspace = insufficient
            .reserve_decompression(SegmentId(1), generation, 64)
            .expect("small workspace");
        let before = insufficient.budget.data_used();
        assert_eq!(
            insufficient.message_arrived_preallocated(
                SegmentId(1),
                generation,
                flow,
                &[workspace],
                MAX_FRAME_SIZE + 128,
            ),
            Err(StreamConsumerModelError::Budget(
                BudgetError::PreallocationExceeded {
                    required: MAX_FRAME_SIZE + 128,
                    reserved: MAX_FRAME_SIZE + 64,
                }
            ))
        );
        assert_eq!(insufficient.budget.data_used(), before);
        assert_eq!(
            insufficient.budget.use_of(workspace),
            Some(BudgetUse::Decompression)
        );
    }

    #[test]
    fn budget_transfers_conserve_bytes_and_cleanup_is_independent() {
        let minimum =
            MAX_FRAME_SIZE + RECEIVER_BUDGET_AUTHORITY_HEADROOM + CONTROL_PLANE_CLEANUP_RESERVE;
        assert_eq!(minimum, 13_697_152);
        assert!(ReceiverBudget::bytes(16 * 1024 * 1024).is_ok());
        assert_eq!(
            ReceiverBudget::bytes(minimum - 1),
            Err(BudgetError::BudgetTooSmall {
                bytes: minimum - 1,
                minimum,
            })
        );
        let budget = ReceiverBudget::bytes(minimum).expect("minimum budget");
        let mut state = ReceiverBudgetState::new(budget);
        let flow = state.reserve_flow().expect("one flow");
        assert_eq!(state.data_used(), MAX_FRAME_SIZE);
        assert!(matches!(
            state.reserve_flow(),
            Err(BudgetError::Exhausted { .. })
        ));
        state
            .transfer(flow, BudgetUse::FlowPermit, BudgetUse::RetainedMessage, 0)
            .expect("empty messages retain bounded metadata capacity");
        assert_eq!(state.data_used(), MIN_RETAINED_MESSAGE_RESERVATION);
        let independently_retained = state
            .reserve(BudgetUse::RetainedMessage, 0)
            .expect("standalone empty retention");
        assert_eq!(state.data_used(), 2 * MIN_RETAINED_MESSAGE_RESERVATION);
        state
            .release(independently_retained)
            .expect("release standalone retention");
        state
            .reserve_control(CONTROL_PLANE_CLEANUP_RESERVE)
            .expect("cleanup remains available");
        state.release(flow).expect("release");
        assert_eq!(state.data_used(), 0);
        assert!(matches!(
            state.release(flow),
            Err(BudgetError::UnknownReservation { .. })
        ));

        let retained_positions = state
            .reserve(
                BudgetUse::DeliveredPositionMetadata,
                MAX_STREAM_POSITION_SIZE + DELIVERY_AUTHORITY_OVERHEAD,
            )
            .expect("minimum budget retains delivered positions");
        let next_flow = state
            .reserve_flow()
            .expect("permanent position metadata leaves room for another maximum frame");
        state.release(next_flow).expect("release next flow");
        state
            .release(retained_positions)
            .expect("release retained positions");
    }

    #[test]
    fn transaction_commit_waits_and_failure_poisoning_leaves_abort() {
        let mut transaction = AggregateTransaction::new(TxnId::new(1, 2));
        let registration = transaction.admit().expect("registration");
        let ack = transaction.admit().expect("ack");
        assert_eq!(
            transaction.begin_commit(),
            Ok(TransactionDecision::Wait { pending: 2 })
        );
        assert!(matches!(
            transaction.admit(),
            Err(AggregateTransactionError::AdmissionClosed { .. })
        ));
        transaction
            .settle(registration, true)
            .expect("registration");
        transaction.settle(ack, false).expect("ack failure");
        assert_eq!(
            transaction.decision(),
            TransactionDecision::TransactionPoisoned
        );
        assert_eq!(
            transaction.finish(AggregateTransactionState::Committed),
            Err(AggregateTransactionError::InvalidTransition {
                state: AggregateTransactionState::CommitClosing,
            })
        );
        assert_eq!(
            transaction.begin_abort(),
            Ok(TransactionDecision::IssueAbort)
        );
        transaction
            .finish(AggregateTransactionState::Aborted)
            .expect("abort outcome");
        assert_eq!(transaction.state(), AggregateTransactionState::Aborted);
        assert!(matches!(
            transaction.settle(ack, true),
            Err(AggregateTransactionError::UnknownOperation { .. })
        ));
    }

    #[test]
    fn transaction_successfully_finishes_commit_and_abort() {
        let mut commit = AggregateTransaction::new(TxnId::new(1, 2));
        assert_eq!(commit.begin_commit(), Ok(TransactionDecision::IssueCommit));
        commit
            .finish(AggregateTransactionState::Committed)
            .expect("commit outcome");
        assert_eq!(commit.state(), AggregateTransactionState::Committed);

        let mut abort = AggregateTransaction::new(TxnId::new(3, 4));
        let operation = abort.admit().expect("operation");
        assert_eq!(
            abort.begin_abort(),
            Ok(TransactionDecision::Wait { pending: 1 })
        );
        abort.settle(operation, true).expect("operation result");
        assert_eq!(abort.decision(), TransactionDecision::IssueAbort);
        abort
            .finish(AggregateTransactionState::Aborted)
            .expect("abort outcome");
        assert_eq!(abort.state(), AggregateTransactionState::Aborted);
    }

    #[test]
    fn transaction_end_commands_are_single_shot_and_consume_all_final_outcomes() {
        let mut commit = AggregateTransaction::new(TxnId::new(5, 6));
        assert_eq!(commit.begin_commit(), Ok(TransactionDecision::IssueCommit));
        assert_eq!(commit.state(), AggregateTransactionState::CommitIssued);
        assert_eq!(commit.decision(), TransactionDecision::Wait { pending: 0 });
        assert_eq!(
            commit.begin_commit(),
            Err(AggregateTransactionError::InvalidTransition {
                state: AggregateTransactionState::CommitIssued,
            })
        );
        commit
            .finish(AggregateTransactionState::Aborted)
            .expect("coordinator-reported abort is consumed");
        assert_eq!(commit.state(), AggregateTransactionState::Aborted);

        let mut abort = AggregateTransaction::new(TxnId::new(7, 8));
        assert_eq!(abort.begin_abort(), Ok(TransactionDecision::IssueAbort));
        assert_eq!(abort.state(), AggregateTransactionState::AbortIssued);
        assert_eq!(abort.decision(), TransactionDecision::Wait { pending: 0 });
        abort
            .finish(AggregateTransactionState::Unknown)
            .expect("unknown coordinator outcome is consumed");
        assert_eq!(abort.state(), AggregateTransactionState::Unknown);
    }
}
