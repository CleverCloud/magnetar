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
        TransactionApi::end_txn(&self.inner, txn.id(), magnetar_proto::TxnAction::Commit)
            .await
            .map_err(|err| PulsarError::Other(format!("commit_transaction: {err}")))
    }

    /// Abort a transaction at the TC. Returns the final state reported by the TC. Mirrors
    /// Java `Transaction#abort`.
    ///
    /// # Errors
    /// - [`PulsarError::Other`] on broker rejection or wire failure.
    pub async fn abort_transaction(&self, txn: Transaction) -> Result<TxnState, PulsarError> {
        TransactionApi::end_txn(&self.inner, txn.id(), magnetar_proto::TxnAction::Abort)
            .await
            .map_err(|err| PulsarError::Other(format!("abort_transaction: {err}")))
    }
}
