// SPDX-License-Identifier: Apache-2.0

//! Pulsar transactions (PIP-31).
//!
//! Mirrors Java's `org.apache.pulsar.client.api.transaction.Transaction`. A [`Transaction`]
//! is a thin token over a [`magnetar_proto::TxnId`]. Stamp the id on an
//! [`crate::OutgoingMessage`] via `.txn(id)` (producer side) or on a consumer ack via
//! [`magnetar_runtime_tokio::Consumer::ack_with_txn`]; then commit or abort via
//! [`PulsarClient::commit_transaction`] / [`PulsarClient::abort_transaction`].

/// Result of committing or aborting a [`Transaction`]. Re-exported from `magnetar-proto`.
pub use magnetar_proto::TxnState;

use crate::PulsarClient;
use crate::client::PulsarError;

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

    /// The transaction id — stamp this on producer sends via [`crate::OutgoingMessage::txn`]
    /// and on consumer acks via
    /// [`magnetar_runtime_tokio::Consumer::ack_with_txn`].
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

impl PulsarClient {
    /// Open a new Pulsar transaction at the broker-side transaction coordinator (PIP-31).
    /// Mirrors Java `PulsarClient#newTransaction()`. Returns a [`Transaction`] token that
    /// can be passed to producers (via [`crate::OutgoingMessage::txn`]) and consumers
    /// (via [`magnetar_runtime_tokio::Consumer::ack_with_txn`]); commit or abort it via
    /// [`Self::commit_transaction`] / [`Self::abort_transaction`].
    pub async fn new_transaction(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Transaction, PulsarError> {
        let id = self
            .runtime_client()
            .new_txn(timeout)
            .await
            .map_err(PulsarError::Client)?;
        Ok(Transaction::new(id))
    }

    /// Register a partition that the given transaction will write to. Mirrors Java
    /// `Transaction#registerProducedTopic`. Optional — Pulsar's TC can discover the
    /// partitions from `CommandSend` frames carrying the txn id, but explicit
    /// registration is the safer pattern in test code.
    pub async fn register_partition_to_transaction(
        &self,
        txn: Transaction,
        topic: impl Into<String>,
    ) -> Result<(), PulsarError> {
        self.runtime_client()
            .add_partition_to_txn(txn.id(), topic.into())
            .await
            .map_err(PulsarError::Client)
    }

    /// Register a subscription that the given transaction will acknowledge on. Mirrors
    /// Java `Transaction#registerSubscriptionToTxn`.
    pub async fn register_subscription_to_transaction(
        &self,
        txn: Transaction,
        topic: impl Into<String>,
        subscription: impl Into<String>,
    ) -> Result<(), PulsarError> {
        self.runtime_client()
            .add_subscription_to_txn(txn.id(), topic.into(), subscription.into())
            .await
            .map_err(PulsarError::Client)
    }

    /// Commit a transaction at the TC. Returns the final state reported by the TC.
    /// Mirrors Java `Transaction#commit`.
    pub async fn commit_transaction(&self, txn: Transaction) -> Result<TxnState, PulsarError> {
        self.runtime_client()
            .end_txn(txn.id(), magnetar_proto::TxnAction::Commit)
            .await
            .map_err(PulsarError::Client)
    }

    /// Abort a transaction at the TC. Returns the final state reported by the TC. Mirrors
    /// Java `Transaction#abort`.
    pub async fn abort_transaction(&self, txn: Transaction) -> Result<TxnState, PulsarError> {
        self.runtime_client()
            .end_txn(txn.id(), magnetar_proto::TxnAction::Abort)
            .await
            .map_err(PulsarError::Client)
    }
}
