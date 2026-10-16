// SPDX-License-Identifier: Apache-2.0

//! Pulsar transactions (PIP-31).
//!
//! Mirrors Java's `org.apache.pulsar.client.api.transaction.Transaction`. A
//! [`Transaction`] is a thin token over a [`magnetar_proto::TxnId`]. Stamp the
//! id on an [`crate::OutgoingMessage`] via `.txn(id)` (producer side) or on a
//! consumer ack via the runtime engine's `ack_with_txn` family; then commit
//! or abort via [`PulsarClient::commit_transaction`] /
//! [`PulsarClient::abort_transaction`].
//!
//! The five façade methods are generic over [`crate::Engine`] (D1 phase 4 of
//! the lift train, ADR-0026 §D1). Both `PulsarClient<TokioEngine>` and
//! `PulsarClient<MoonpoolEngine<P>>` carry the same Transaction surface by
//! dispatching through the [`crate::TransactionApi`] extension trait
//! implemented per engine on its `ClientState` type.

#[cfg(feature = "scalable-topics")]
use std::collections::BTreeMap;
#[cfg(feature = "scalable-topics")]
use std::future::Future;
#[cfg(feature = "scalable-topics")]
use std::pin::Pin;
#[cfg(feature = "scalable-topics")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "scalable-topics")]
use std::sync::{Arc, Weak};
#[cfg(feature = "scalable-topics")]
use std::task::{Context, Poll, Waker};

/// Result of committing or aborting a [`Transaction`]. Re-exported from `magnetar-proto`.
pub use magnetar_proto::TxnState;

use crate::client::PulsarError;
use crate::{Engine, PulsarClient, TransactionApi};

/// A live Pulsar transaction token. Holds the broker-assigned [`magnetar_proto::TxnId`].
///
/// `Transaction` is `Copy` (`TxnId` is 128 bits of plain data) so it can be passed to
/// multiple producers / consumers without juggling references.
#[derive(Debug, Clone, Copy)]
pub struct Transaction {
    id: magnetar_proto::TxnId,
}

impl Transaction {
    pub(crate) fn new(id: magnetar_proto::TxnId) -> Self {
        Self { id }
    }

    /// The transaction id — stamp this on producer sends via
    /// [`crate::OutgoingMessage::txn`] and on consumer acks via the runtime
    /// engine's `ack_with_txn` family.
    #[must_use]
    pub fn id(&self) -> magnetar_proto::TxnId {
        self.id
    }
}

impl From<Transaction> for magnetar_proto::TxnId {
    fn from(txn: Transaction) -> Self {
        txn.id
    }
}

/// Outcome sink retained weakly by the client transaction coordinator.
///
/// Implemented by a scalable aggregate's final user guard. Runtime tasks do not
/// retain this guard, and stale weak participants disappear naturally after
/// close/drop.
#[cfg(feature = "scalable-topics")]
pub(crate) trait TransactionParticipant: Send + Sync {
    fn transaction_outcome(
        &self,
        txn_id: magnetar_proto::TxnId,
        outcome: crate::scalable::TransactionOutcome,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::scalable::StreamConsumerError>> + Send + '_>>;
}

#[cfg(feature = "scalable-topics")]
struct TransactionEntryState {
    gate: magnetar_proto::AggregateTransaction,
    participants: BTreeMap<u64, Weak<dyn TransactionParticipant>>,
    waiters: Vec<Waker>,
    end_claimed: bool,
}

#[cfg(feature = "scalable-topics")]
struct TransactionEntry {
    state: parking_lot::Mutex<TransactionEntryState>,
}

/// Client-local admission coordinator tying aggregate acknowledgements to the
/// existing `commit_transaction` / `abort_transaction` paths.
#[cfg(feature = "scalable-topics")]
pub(crate) struct TransactionCoordinator {
    entries: parking_lot::Mutex<BTreeMap<magnetar_proto::TxnId, Arc<TransactionEntry>>>,
    next_participant_id: AtomicU64,
}

#[cfg(feature = "scalable-topics")]
impl std::fmt::Debug for TransactionCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransactionCoordinator")
            .field("transactions", &self.entries.lock().len())
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "scalable-topics")]
impl Default for TransactionCoordinator {
    fn default() -> Self {
        Self {
            entries: parking_lot::Mutex::new(BTreeMap::new()),
            next_participant_id: AtomicU64::new(0),
        }
    }
}

#[cfg(feature = "scalable-topics")]
impl TransactionCoordinator {
    pub(crate) fn open(&self, txn_id: magnetar_proto::TxnId) {
        self.entries.lock().insert(
            txn_id,
            Arc::new(TransactionEntry {
                state: parking_lot::Mutex::new(TransactionEntryState {
                    gate: magnetar_proto::AggregateTransaction::new(txn_id),
                    participants: BTreeMap::new(),
                    waiters: Vec::new(),
                    end_claimed: false,
                }),
            }),
        );
    }

    pub(crate) fn next_participant_id(&self) -> u64 {
        self.next_participant_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn admit(
        &self,
        txn_id: magnetar_proto::TxnId,
        participant_id: u64,
        participant: Weak<dyn TransactionParticipant>,
    ) -> Result<TransactionAdmission, crate::scalable::StreamConsumerError> {
        let entry = self.entry(txn_id)?;
        let operation = {
            let mut state = entry.state.lock();
            let operation = state.gate.admit()?;
            state.participants.insert(participant_id, participant);
            operation
        };
        Ok(TransactionAdmission {
            entry,
            operation,
            settled: false,
        })
    }

    pub(crate) async fn prepare_commit(
        &self,
        txn_id: magnetar_proto::TxnId,
    ) -> Result<EndPreparation, crate::scalable::StreamConsumerError> {
        self.prepare(txn_id, EndKind::Commit).await
    }

    pub(crate) async fn prepare_abort(
        &self,
        txn_id: magnetar_proto::TxnId,
    ) -> Result<EndPreparation, crate::scalable::StreamConsumerError> {
        self.prepare(txn_id, EndKind::Abort).await
    }

    async fn prepare(
        &self,
        txn_id: magnetar_proto::TxnId,
        kind: EndKind,
    ) -> Result<EndPreparation, crate::scalable::StreamConsumerError> {
        let entry = self.entry(txn_id)?;
        let decision = {
            let mut state = entry.state.lock();
            if state.end_claimed {
                return Err(
                    crate::scalable::StreamConsumerError::TransactionAlreadyEnding { txn_id },
                );
            }
            state.end_claimed = true;
            let confirmed_state = match (kind, state.gate.state()) {
                (EndKind::Commit, magnetar_proto::AggregateTransactionState::Committed) => {
                    Some(TxnState::Committed)
                }
                (EndKind::Abort, magnetar_proto::AggregateTransactionState::Aborted) => {
                    Some(TxnState::Aborted)
                }
                _ => None,
            };
            if let Some(broker_state) = confirmed_state {
                let participants = pending_participants(&mut state);
                drop(state);
                return Ok(EndPreparation {
                    txn_id,
                    entry,
                    participants,
                    broker_state: Some(broker_state),
                    claimed: true,
                });
            }
            let decision = match (kind, state.gate.state()) {
                (EndKind::Commit, magnetar_proto::AggregateTransactionState::CommitIssued) => {
                    Ok(magnetar_proto::TransactionDecision::IssueCommit)
                }
                (EndKind::Abort, magnetar_proto::AggregateTransactionState::AbortIssued) => {
                    Ok(magnetar_proto::TransactionDecision::IssueAbort)
                }
                (EndKind::Commit, _) => state.gate.begin_commit(),
                (EndKind::Abort, _) => state.gate.begin_abort(),
            };
            match decision {
                Ok(decision) => decision,
                Err(error) => {
                    state.end_claimed = false;
                    return Err(error.into());
                }
            }
        };
        EndWait {
            entry,
            txn_id,
            decision: Some(decision),
            completed: false,
        }
        .await
    }

    pub(crate) async fn complete(
        &self,
        mut preparation: EndPreparation,
        outcome: crate::scalable::TransactionOutcome,
    ) -> Result<(), crate::scalable::StreamConsumerError> {
        let aggregate_outcome = match outcome {
            crate::scalable::TransactionOutcome::Committed => {
                magnetar_proto::AggregateTransactionState::Committed
            }
            crate::scalable::TransactionOutcome::Aborted => {
                magnetar_proto::AggregateTransactionState::Aborted
            }
            crate::scalable::TransactionOutcome::Unknown => {
                magnetar_proto::AggregateTransactionState::Unknown
            }
        };
        let confirmed = matches!(
            aggregate_outcome,
            magnetar_proto::AggregateTransactionState::Committed
                | magnetar_proto::AggregateTransactionState::Aborted
        );
        if confirmed {
            let finish_result = {
                let mut state = preparation.entry.state.lock();
                if state.gate.state() == aggregate_outcome {
                    Ok(())
                } else {
                    state.gate.finish(aggregate_outcome)
                }
            };
            finish_result?;
        }

        let mut first_error = None;
        for (participant_id, participant) in &preparation.participants {
            if let Err(error) = participant
                .transaction_outcome(preparation.txn_id, outcome)
                .await
            {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            } else {
                preparation
                    .entry
                    .state
                    .lock()
                    .participants
                    .remove(participant_id);
            }
        }
        let (finish_result, propagation_complete) = {
            let mut state = preparation.entry.state.lock();
            let result = if confirmed {
                Ok(())
            } else {
                state.gate.finish(aggregate_outcome).map_err(Into::into)
            };
            state.end_claimed = false;
            (result, state.participants.is_empty())
        };
        preparation.claimed = false;
        let result = first_error.map_or(finish_result, Err);
        if propagation_complete && result.is_ok() {
            self.remove_if_same(preparation.txn_id, &preparation.entry);
        }
        result
    }

    fn entry(
        &self,
        txn_id: magnetar_proto::TxnId,
    ) -> Result<Arc<TransactionEntry>, crate::scalable::StreamConsumerError> {
        self.entries
            .lock()
            .get(&txn_id)
            .cloned()
            .ok_or(crate::scalable::StreamConsumerError::UnknownTransaction { txn_id })
    }

    fn remove_if_same(&self, txn_id: magnetar_proto::TxnId, entry: &Arc<TransactionEntry>) {
        let mut entries = self.entries.lock();
        if entries
            .get(&txn_id)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            entries.remove(&txn_id);
        }
    }
}

#[cfg(feature = "scalable-topics")]
fn pending_participants(
    state: &mut TransactionEntryState,
) -> Vec<(u64, Arc<dyn TransactionParticipant>)> {
    let mut participants = Vec::with_capacity(state.participants.len());
    let mut dropped = Vec::new();
    for (participant_id, participant) in &state.participants {
        if let Some(participant) = participant.upgrade() {
            participants.push((*participant_id, participant));
        } else {
            dropped.push(*participant_id);
        }
    }
    for participant_id in dropped {
        state.participants.remove(&participant_id);
    }
    participants
}

#[cfg(feature = "scalable-topics")]
pub(crate) struct TransactionAdmission {
    entry: Arc<TransactionEntry>,
    operation: magnetar_proto::TransactionOperationId,
    settled: bool,
}

#[cfg(feature = "scalable-topics")]
impl TransactionAdmission {
    pub(crate) fn finish(
        mut self,
        succeeded: bool,
    ) -> Result<(), magnetar_proto::AggregateTransactionError> {
        self.settled = true;
        settle_admission(&self.entry, self.operation, succeeded)
    }
}

#[cfg(feature = "scalable-topics")]
impl Drop for TransactionAdmission {
    fn drop(&mut self) {
        if !self.settled {
            let _ = settle_admission(&self.entry, self.operation, false);
        }
    }
}

#[cfg(feature = "scalable-topics")]
fn settle_admission(
    entry: &TransactionEntry,
    operation: magnetar_proto::TransactionOperationId,
    succeeded: bool,
) -> Result<(), magnetar_proto::AggregateTransactionError> {
    let waiters = {
        let mut state = entry.state.lock();
        state.gate.settle(operation, succeeded)?;
        core::mem::take(&mut state.waiters)
    };
    for waiter in waiters {
        waiter.wake();
    }
    Ok(())
}

#[cfg(feature = "scalable-topics")]
#[derive(Clone, Copy)]
enum EndKind {
    Commit,
    Abort,
}

#[cfg(feature = "scalable-topics")]
pub(crate) struct EndPreparation {
    txn_id: magnetar_proto::TxnId,
    entry: Arc<TransactionEntry>,
    participants: Vec<(u64, Arc<dyn TransactionParticipant>)>,
    broker_state: Option<TxnState>,
    claimed: bool,
}

#[cfg(feature = "scalable-topics")]
impl EndPreparation {
    const fn broker_state(&self) -> Option<TxnState> {
        self.broker_state
    }
}

#[cfg(feature = "scalable-topics")]
impl Drop for EndPreparation {
    fn drop(&mut self) {
        if self.claimed {
            self.entry.state.lock().end_claimed = false;
        }
    }
}

#[cfg(feature = "scalable-topics")]
struct EndWait {
    entry: Arc<TransactionEntry>,
    txn_id: magnetar_proto::TxnId,
    decision: Option<magnetar_proto::TransactionDecision>,
    completed: bool,
}

#[cfg(feature = "scalable-topics")]
impl Future for EndWait {
    type Output = Result<EndPreparation, crate::scalable::StreamConsumerError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let initial_decision = self.decision.take();
        let mut state = self.entry.state.lock();
        let decision = initial_decision.unwrap_or_else(|| state.gate.decision());
        match decision {
            magnetar_proto::TransactionDecision::Wait { pending } => {
                debug_assert!(pending > 0, "a transaction wait must retain admitted work");
                if !state
                    .waiters
                    .iter()
                    .any(|waiter| waiter.will_wake(context.waker()))
                {
                    state.waiters.push(context.waker().clone());
                }
                Poll::Pending
            }
            magnetar_proto::TransactionDecision::TransactionPoisoned => {
                state.end_claimed = false;
                drop(state);
                self.completed = true;
                Poll::Ready(Err(
                    crate::scalable::StreamConsumerError::TransactionPoisoned {
                        txn_id: self.txn_id,
                    },
                ))
            }
            magnetar_proto::TransactionDecision::IssueCommit
            | magnetar_proto::TransactionDecision::IssueAbort => {
                let participants = pending_participants(&mut state);
                drop(state);
                self.completed = true;
                Poll::Ready(Ok(EndPreparation {
                    txn_id: self.txn_id,
                    entry: self.entry.clone(),
                    participants,
                    broker_state: None,
                    claimed: true,
                }))
            }
        }
    }
}

#[cfg(feature = "scalable-topics")]
impl Drop for EndWait {
    fn drop(&mut self) {
        if !self.completed {
            let mut state = self.entry.state.lock();
            state.end_claimed = false;
        }
    }
}

impl<E: Engine> PulsarClient<E>
where
    E::ClientState: TransactionApi,
{
    /// Open a new Pulsar transaction at the broker-side transaction coordinator (PIP-31).
    /// Mirrors Java `PulsarClient#newTransaction()`.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] (with the runtime's error stringified) on broker rejection or wire
    ///   failure.
    pub async fn new_transaction(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Transaction, PulsarError> {
        let id = TransactionApi::new_txn(&self.inner, timeout)
            .await
            .map_err(|err| PulsarError::Other(format!("new_transaction: {err}")))?;
        #[cfg(feature = "scalable-topics")]
        self.transactions.open(id);
        Ok(Transaction::new(id))
    }

    /// Register a partition that the given transaction will write to.
    /// Mirrors Java `Transaction#registerProducedTopic`.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] on broker rejection or wire failure.
    pub async fn register_partition_to_transaction(
        &self,
        txn: Transaction,
        topic: impl Into<String>,
    ) -> Result<(), PulsarError> {
        TransactionApi::add_partition_to_txn(&self.inner, txn.id(), topic.into())
            .await
            .map_err(|err| PulsarError::Other(format!("register_partition_to_transaction: {err}")))
    }

    /// Register a subscription that the given transaction will acknowledge on.
    /// Mirrors Java `Transaction#registerSubscriptionToTxn`.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] on broker rejection or wire failure.
    pub async fn register_subscription_to_transaction(
        &self,
        txn: Transaction,
        topic: impl Into<String>,
        subscription: impl Into<String>,
    ) -> Result<(), PulsarError> {
        TransactionApi::add_subscription_to_txn(
            &self.inner,
            txn.id(),
            topic.into(),
            subscription.into(),
        )
        .await
        .map_err(|err| PulsarError::Other(format!("register_subscription_to_transaction: {err}")))
    }

    /// Commit a transaction at the TC. Returns the final state reported by the TC.
    /// Mirrors Java `Transaction#commit`.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] on broker rejection or wire failure.
    pub async fn commit_transaction(&self, txn: Transaction) -> Result<TxnState, PulsarError> {
        #[cfg(feature = "scalable-topics")]
        let preparation = self.transactions.prepare_commit(txn.id()).await?;
        #[cfg(feature = "scalable-topics")]
        let result = match preparation.broker_state() {
            Some(state) => Ok(state),
            None => {
                TransactionApi::end_txn(&self.inner, txn.id(), magnetar_proto::TxnAction::Commit)
                    .await
            }
        };
        #[cfg(not(feature = "scalable-topics"))]
        let result =
            TransactionApi::end_txn(&self.inner, txn.id(), magnetar_proto::TxnAction::Commit).await;
        #[cfg(feature = "scalable-topics")]
        match result {
            Ok(state) => {
                let outcome = crate::scalable::TransactionOutcome::Committed;
                if let Err(error) = self.transactions.complete(preparation, outcome).await {
                    tracing::warn!(
                        txn_id = ?txn.id(),
                        error = %error,
                        "transaction committed but local participant finalization failed"
                    );
                }
                Ok(state)
            }
            Err(error) => {
                if let Err(completion_error) = self
                    .transactions
                    .complete(preparation, crate::scalable::TransactionOutcome::Unknown)
                    .await
                {
                    tracing::warn!(
                        txn_id = ?txn.id(),
                        error = %completion_error,
                        "transaction commit failed and local participant finalization also failed"
                    );
                }
                Err(PulsarError::Other(format!("commit_transaction: {error}")))
            }
        }
        #[cfg(not(feature = "scalable-topics"))]
        result.map_err(|err| PulsarError::Other(format!("commit_transaction: {err}")))
    }

    /// Abort a transaction at the TC. Returns the final state reported by the TC. Mirrors
    /// Java `Transaction#abort`.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] on broker rejection or wire failure.
    pub async fn abort_transaction(&self, txn: Transaction) -> Result<TxnState, PulsarError> {
        #[cfg(feature = "scalable-topics")]
        let preparation = self.transactions.prepare_abort(txn.id()).await?;
        #[cfg(feature = "scalable-topics")]
        let result = match preparation.broker_state() {
            Some(state) => Ok(state),
            None => {
                TransactionApi::end_txn(&self.inner, txn.id(), magnetar_proto::TxnAction::Abort)
                    .await
            }
        };
        #[cfg(not(feature = "scalable-topics"))]
        let result =
            TransactionApi::end_txn(&self.inner, txn.id(), magnetar_proto::TxnAction::Abort).await;
        #[cfg(feature = "scalable-topics")]
        match result {
            Ok(state) => {
                let outcome = crate::scalable::TransactionOutcome::Aborted;
                if let Err(error) = self.transactions.complete(preparation, outcome).await {
                    tracing::warn!(
                        txn_id = ?txn.id(),
                        error = %error,
                        "transaction aborted but local participant finalization failed"
                    );
                }
                Ok(state)
            }
            Err(error) => {
                if let Err(completion_error) = self
                    .transactions
                    .complete(preparation, crate::scalable::TransactionOutcome::Unknown)
                    .await
                {
                    tracing::warn!(
                        txn_id = ?txn.id(),
                        error = %completion_error,
                        "transaction abort failed and local participant finalization also failed"
                    );
                }
                Err(PulsarError::Other(format!("abort_transaction: {error}")))
            }
        }
        #[cfg(not(feature = "scalable-topics"))]
        result.map_err(|err| PulsarError::Other(format!("abort_transaction: {err}")))
    }
}
