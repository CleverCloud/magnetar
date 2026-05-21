// SPDX-License-Identifier: Apache-2.0

//! Transactional client state machine (PIP-31).
//!
//! Pulsar's transactional API (`TC` = Transaction Coordinator) lets a single client publish to
//! multiple partitions and acknowledge messages on multiple subscriptions atomically. The wire
//! protocol introduces five RPC pairs in `BaseCommand` (raw integers `50` … `61`):
//!
//! | Outgoing                                | Incoming                                          | Purpose                                          |
//! |-----------------------------------------|---------------------------------------------------|--------------------------------------------------|
//! | [`pb::CommandNewTxn`]                   | [`pb::CommandNewTxnResponse`]                     | Allocate a new transaction id at the TC.         |
//! | [`pb::CommandAddPartitionToTxn`]        | [`pb::CommandAddPartitionToTxnResponse`]          | Register a topic-partition the txn will write.   |
//! | [`pb::CommandAddSubscriptionToTxn`]     | [`pb::CommandAddSubscriptionToTxnResponse`]       | Register a subscription the txn will ack.        |
//! | [`pb::CommandEndTxn`]                   | [`pb::CommandEndTxnResponse`]                     | Commit or abort the transaction.                 |
//! | [`pb::CommandEndTxnOnPartition`] / `…OnSubscription` | matching responses                            | Broker-fanned-out commit / abort (out of scope). |
//!
//! The state machine lives in [`TxnClient`]. It is **sans-io and channel-free**: every request
//! returns a [`pb::BaseCommand`] (or more precisely the inner protobuf message — the connection
//! wraps it) that the caller is expected to wire onto the connection's outbound buffer; every
//! response is consumed via the matching `handle_…_response` method which transitions the
//! [`TransactionMetadata`] and returns the user-facing outcome. Waker slabs let user futures
//! observe completion without involving channels (see [GUIDELINES.md]
//! §"No-channels rule"). Mirrors `TransactionImpl.java` in the Java client.
//!
//! # State diagram
//!
//! ```text
//!                 ┌─────────┐ end_txn(Commit)      ┌────────────┐ Success ┌────────────┐
//!                 │  Open   │ ───────────────────▶ │ Committing │ ──────▶ │ Committed  │
//!                 └─────────┘                      └────────────┘         └────────────┘
//!                       │  end_txn(Abort)             ┌────────────┐ Success ┌────────────┐
//!                       └────────────────────────────▶│  Aborting  │ ──────▶ │  Aborted   │
//!                                                     └────────────┘         └────────────┘
//!                                                              │
//!                                                  Broker error│ on commit/abort
//!                                                              ▼
//!                                                       ┌────────────┐
//!                                                       │  Errored   │
//!                                                       └────────────┘
//! ```
//!
//! [GUIDELINES.md]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/GUIDELINES.md

use core::time::Duration;
use std::collections::{HashMap, HashSet};
use std::task::Waker;

use slab::Slab;

use crate::pb;
use crate::types::RequestId;

/// A Pulsar transaction id (`128-bit`, split into two 64-bit halves on the wire).
///
/// Mirrors `org.apache.pulsar.client.api.transaction.TxnID` — `mostSigBits` (TC node id) plus
/// `leastSigBits` (sequence within the TC).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TxnId {
    /// Most-significant 64 bits — typically encodes the originating transaction-coordinator id.
    pub most_sig_bits: u64,
    /// Least-significant 64 bits — typically encodes the sequence within the TC.
    pub least_sig_bits: u64,
}

impl TxnId {
    /// Construct a `TxnId` from the protobuf-wire halves.
    pub const fn new(most_sig_bits: u64, least_sig_bits: u64) -> Self {
        Self {
            most_sig_bits,
            least_sig_bits,
        }
    }
}

impl core::fmt::Display for TxnId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}:{}", self.most_sig_bits, self.least_sig_bits)
    }
}

/// Lifecycle of a transaction tracked by [`TxnClient`].
///
/// Mirrors `TransactionImpl.State` in the Java client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    /// Transaction is open and accepting `add_partition` / `add_subscription` / sends.
    Open,
    /// `end_txn(Commit)` has been issued; awaiting `CommandEndTxnResponse`.
    Committing,
    /// Transaction has been committed by the TC.
    Committed,
    /// `end_txn(Abort)` has been issued; awaiting `CommandEndTxnResponse`.
    Aborting,
    /// Transaction has been aborted (either by the user or due to an error).
    Aborted,
    /// Transaction terminated due to a broker-side error (unrecoverable).
    Errored,
}

/// User-visible action passed to [`TxnClient::end_txn`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnAction {
    /// Commit the transaction (best-effort 2PC: TC writes the commit marker on every
    /// partition + subscription).
    Commit,
    /// Abort the transaction.
    Abort,
}

impl TxnAction {
    /// Convert to the wire protobuf enum.
    pub const fn to_pb(self) -> pb::TxnAction {
        match self {
            Self::Commit => pb::TxnAction::Commit,
            Self::Abort => pb::TxnAction::Abort,
        }
    }
}

/// Errors that a TC RPC can surface to the user future.
///
/// The numeric codes come from [`pb::ServerError`]; we keep the broker message so consumers do
/// not lose diagnostics. The mapping mirrors `TransactionCoordinatorClientException` in the Java
/// client.
#[derive(Debug, Clone, thiserror::Error)]
pub enum TxnError {
    /// `ServerError::TransactionConflict` — a competing transaction holds the resource (e.g.
    /// a subscription).
    #[error("transaction conflict")]
    Conflict,
    /// `ServerError::TransactionNotFound` / `ServerError::TransactionCoordinatorNotFound` — the
    /// TC does not know about the txn we referenced.
    #[error("transaction not found")]
    NotFound,
    /// Driver-side timeout — the broker did not respond within the configured operation timeout.
    /// The TC itself does not return this; the driver layer surfaces it.
    #[error("transaction timed out")]
    Timeout,
    /// The transaction has already been aborted (locally or remotely).
    #[error("transaction aborted")]
    Aborted,
    /// Any other broker error — `ServerError` code and message.
    #[error("broker error {0}: {1}")]
    Broker(i32, String),
}

impl TxnError {
    /// Translate a `(ServerError, message)` pair from a TC response into a [`TxnError`].
    pub fn from_broker(code: i32, message: String) -> Self {
        // `try_from` is generated by prost for the enum; an unknown code falls through to
        // `Broker(code, message)`.
        match pb::ServerError::try_from(code) {
            Ok(pb::ServerError::TransactionConflict) => Self::Conflict,
            Ok(
                pb::ServerError::TransactionNotFound
                | pb::ServerError::TransactionCoordinatorNotFound,
            ) => Self::NotFound,
            Ok(pb::ServerError::InvalidTxnStatus) => Self::Aborted,
            _ => Self::Broker(code, message),
        }
    }
}

/// Per-transaction bookkeeping.
///
/// Tracks the lifecycle state plus the set of topics published to and subscriptions acked under
/// this transaction. The TC needs every partition + subscription explicitly registered before
/// the end-txn marker so it can fan out commit / abort markers correctly.
#[derive(Debug, Clone)]
pub struct TransactionMetadata {
    /// Transaction identifier.
    pub id: TxnId,
    /// Lifecycle state.
    pub state: TxnState,
    /// TC node id (`tc_id` from `CommandNewTxn`) — held so we can re-target retries.
    pub coordinator_id: u64,
    /// Transaction timeout configured on `new_txn`.
    pub timeout: Duration,
    /// Set of topics (partitions) that the transaction has been registered against.
    pub produced_topics: HashSet<String>,
    /// Map of subscription name → topics the subscription was registered against under this txn.
    pub acked_subscriptions: HashMap<String, Vec<String>>,
}

impl TransactionMetadata {
    fn new(id: TxnId, coordinator_id: u64, timeout: Duration) -> Self {
        Self {
            id,
            state: TxnState::Open,
            coordinator_id,
            timeout,
            produced_topics: HashSet::new(),
            acked_subscriptions: HashMap::new(),
        }
    }
}

/// Per-request bookkeeping so we can correlate inbound responses with the originating call and
/// figure out which transaction metadata to mutate.
#[derive(Debug, Clone)]
struct PendingNewTxn {
    request_id: RequestId,
    waker_key: usize,
}

#[derive(Debug, Clone)]
struct PendingAddPartition {
    request_id: RequestId,
    txn: TxnId,
    topic: String,
    waker_key: usize,
}

#[derive(Debug, Clone)]
struct PendingAddSubscription {
    request_id: RequestId,
    txn: TxnId,
    subscription: String,
    topic: String,
    waker_key: usize,
}

#[derive(Debug, Clone)]
struct PendingEndTxn {
    request_id: RequestId,
    txn: TxnId,
    action: TxnAction,
    waker_key: usize,
}

/// Transaction-coordinator client state machine.
///
/// The driver owns one `TxnClient` per `Connection` that has talked to a transaction
/// coordinator. Each method either *encodes* a TC command (producing a `pb::Command…`) or
/// *consumes* a TC response and surfaces a `Result` to the caller. No I/O. No channels.
#[derive(Debug)]
pub struct TxnClient {
    coordinator_id: u64,
    /// Wakers for in-flight `CommandNewTxn` requests, keyed by request id.
    pending_new_txn: Slab<Waker>,
    new_txn_by_request: HashMap<RequestId, PendingNewTxn>,
    /// Wakers for in-flight `CommandAddPartitionToTxn` requests.
    pending_add_partition: Slab<Waker>,
    add_partition_by_request: HashMap<RequestId, PendingAddPartition>,
    /// Wakers for in-flight `CommandAddSubscriptionToTxn` requests.
    pending_add_subscription: Slab<Waker>,
    add_subscription_by_request: HashMap<RequestId, PendingAddSubscription>,
    /// Wakers for in-flight `CommandEndTxn` requests.
    pending_end_txn: Slab<Waker>,
    end_txn_by_request: HashMap<RequestId, PendingEndTxn>,
    /// Live transactions keyed by id.
    transactions: HashMap<TxnId, TransactionMetadata>,
}

impl TxnClient {
    /// Construct a fresh client bound to a specific TC node id (`tc_id`).
    pub fn new(coordinator_id: u64) -> Self {
        Self {
            coordinator_id,
            pending_new_txn: Slab::new(),
            new_txn_by_request: HashMap::new(),
            pending_add_partition: Slab::new(),
            add_partition_by_request: HashMap::new(),
            pending_add_subscription: Slab::new(),
            add_subscription_by_request: HashMap::new(),
            pending_end_txn: Slab::new(),
            end_txn_by_request: HashMap::new(),
            transactions: HashMap::new(),
        }
    }

    /// Returns the TC node id this client targets.
    pub const fn coordinator_id(&self) -> u64 {
        self.coordinator_id
    }

    /// Look up an in-flight transaction by id (read-only).
    pub fn transaction(&self, id: TxnId) -> Option<&TransactionMetadata> {
        self.transactions.get(&id)
    }

    /// Number of currently-tracked transactions.
    pub fn len(&self) -> usize {
        self.transactions.len()
    }

    /// `true` if no transactions are tracked.
    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    /// Register a waker that should be woken when the matching `CommandNewTxnResponse` arrives.
    ///
    /// Returns a slab key; the response handler discards the waker once it has fired.
    pub fn register_new_txn_waker(&mut self, request_id: RequestId, waker: Waker) {
        if let Some(pending) = self.new_txn_by_request.get_mut(&request_id) {
            self.pending_new_txn[pending.waker_key] = waker;
        }
    }

    /// Register a waker for `CommandAddPartitionToTxnResponse`.
    pub fn register_add_partition_waker(&mut self, request_id: RequestId, waker: Waker) {
        if let Some(pending) = self.add_partition_by_request.get_mut(&request_id) {
            self.pending_add_partition[pending.waker_key] = waker;
        }
    }

    /// Register a waker for `CommandAddSubscriptionToTxnResponse`.
    pub fn register_add_subscription_waker(&mut self, request_id: RequestId, waker: Waker) {
        if let Some(pending) = self.add_subscription_by_request.get_mut(&request_id) {
            self.pending_add_subscription[pending.waker_key] = waker;
        }
    }

    /// Register a waker for `CommandEndTxnResponse`.
    pub fn register_end_txn_waker(&mut self, request_id: RequestId, waker: Waker) {
        if let Some(pending) = self.end_txn_by_request.get_mut(&request_id) {
            self.pending_end_txn[pending.waker_key] = waker;
        }
    }

    /// Build a [`pb::CommandNewTxn`] to open a new transaction with the configured timeout.
    ///
    /// The caller must wrap the result in a `BaseCommand` of type `NEW_TXN` and place it onto
    /// the connection's outbound buffer. The transaction-id and `Open` state are only recorded
    /// once the matching response is consumed via [`Self::handle_new_txn_response`].
    pub fn new_txn(&mut self, request_id: u64, timeout_ms: u64) -> pb::CommandNewTxn {
        let waker_key = self.pending_new_txn.insert(noop_waker());
        let pending = PendingNewTxn {
            request_id: RequestId(request_id),
            waker_key,
        };
        self.new_txn_by_request
            .insert(RequestId(request_id), pending);
        pb::CommandNewTxn {
            request_id,
            txn_ttl_seconds: Some(timeout_ms.div_ceil(1000)),
            tc_id: Some(self.coordinator_id),
        }
    }

    /// Consume a `CommandNewTxnResponse`. On success the transaction is registered as
    /// `TxnState::Open` and its id is returned. On broker error a [`TxnError`] is returned and
    /// no metadata is stored.
    ///
    /// Returns `Ok(None)` if the request id is unknown (stale response — the caller can ignore).
    pub fn handle_new_txn_response(
        &mut self,
        resp: pb::CommandNewTxnResponse,
    ) -> Result<Option<TxnId>, TxnError> {
        let request_id = RequestId(resp.request_id);
        let Some(pending) = self.new_txn_by_request.remove(&request_id) else {
            return Ok(None);
        };
        let waker = self.pending_new_txn.try_remove(pending.waker_key);

        if let Some(code) = resp.error {
            if let Some(w) = waker {
                w.wake();
            }
            return Err(TxnError::from_broker(
                code,
                resp.message.unwrap_or_default(),
            ));
        }

        let txn_id = TxnId::new(
            resp.txnid_most_bits.unwrap_or(0),
            resp.txnid_least_bits.unwrap_or(0),
        );
        let timeout = Duration::from_secs(0); // populated by the caller via `set_timeout`
        let metadata = TransactionMetadata::new(txn_id, self.coordinator_id, timeout);
        self.transactions.insert(txn_id, metadata);
        if let Some(w) = waker {
            w.wake();
        }
        Ok(Some(txn_id))
    }

    /// Build a `CommandAddPartitionToTxn`. The topic is stored locally so the response handler
    /// can mark it as registered on the transaction metadata.
    pub fn add_partition(
        &mut self,
        request_id: u64,
        txn: TxnId,
        topic: String,
    ) -> pb::CommandAddPartitionToTxn {
        let waker_key = self.pending_add_partition.insert(noop_waker());
        let pending = PendingAddPartition {
            request_id: RequestId(request_id),
            txn,
            topic: topic.clone(),
            waker_key,
        };
        self.add_partition_by_request
            .insert(RequestId(request_id), pending);
        pb::CommandAddPartitionToTxn {
            request_id,
            txnid_least_bits: Some(txn.least_sig_bits),
            txnid_most_bits: Some(txn.most_sig_bits),
            partitions: vec![topic],
        }
    }

    /// Consume a `CommandAddPartitionToTxnResponse`. On success the topic is recorded in
    /// `produced_topics`. On broker error a [`TxnError`] is returned and the transaction is
    /// transitioned to `Errored`.
    pub fn handle_add_partition_response(
        &mut self,
        resp: pb::CommandAddPartitionToTxnResponse,
    ) -> Result<(), TxnError> {
        let request_id = RequestId(resp.request_id);
        let Some(pending) = self.add_partition_by_request.remove(&request_id) else {
            return Ok(());
        };
        let waker = self.pending_add_partition.try_remove(pending.waker_key);

        if let Some(code) = resp.error {
            if let Some(meta) = self.transactions.get_mut(&pending.txn) {
                meta.state = TxnState::Errored;
            }
            if let Some(w) = waker {
                w.wake();
            }
            return Err(TxnError::from_broker(
                code,
                resp.message.unwrap_or_default(),
            ));
        }

        if let Some(meta) = self.transactions.get_mut(&pending.txn) {
            meta.produced_topics.insert(pending.topic);
        }
        if let Some(w) = waker {
            w.wake();
        }
        Ok(())
    }

    /// Build a `CommandAddSubscriptionToTxn`. The `(subscription, topic)` pair is stored locally
    /// for the response handler.
    pub fn add_subscription(
        &mut self,
        request_id: u64,
        txn: TxnId,
        subscription: String,
        topic: String,
    ) -> pb::CommandAddSubscriptionToTxn {
        let waker_key = self.pending_add_subscription.insert(noop_waker());
        let pending = PendingAddSubscription {
            request_id: RequestId(request_id),
            txn,
            subscription: subscription.clone(),
            topic: topic.clone(),
            waker_key,
        };
        self.add_subscription_by_request
            .insert(RequestId(request_id), pending);
        pb::CommandAddSubscriptionToTxn {
            request_id,
            txnid_least_bits: Some(txn.least_sig_bits),
            txnid_most_bits: Some(txn.most_sig_bits),
            subscription: vec![pb::Subscription {
                topic,
                subscription,
            }],
        }
    }

    /// Consume a `CommandAddSubscriptionToTxnResponse`. On success the `(subscription, topic)`
    /// pair is recorded.
    pub fn handle_add_subscription_response(
        &mut self,
        resp: pb::CommandAddSubscriptionToTxnResponse,
    ) -> Result<(), TxnError> {
        let request_id = RequestId(resp.request_id);
        let Some(pending) = self.add_subscription_by_request.remove(&request_id) else {
            return Ok(());
        };
        let waker = self.pending_add_subscription.try_remove(pending.waker_key);

        if let Some(code) = resp.error {
            if let Some(meta) = self.transactions.get_mut(&pending.txn) {
                meta.state = TxnState::Errored;
            }
            if let Some(w) = waker {
                w.wake();
            }
            return Err(TxnError::from_broker(
                code,
                resp.message.unwrap_or_default(),
            ));
        }

        if let Some(meta) = self.transactions.get_mut(&pending.txn) {
            meta.acked_subscriptions
                .entry(pending.subscription)
                .or_default()
                .push(pending.topic);
        }
        if let Some(w) = waker {
            w.wake();
        }
        Ok(())
    }

    /// Build a `CommandEndTxn` transitioning the transaction to `Committing` / `Aborting`.
    ///
    /// Returns the wire command. The transaction stays in the intermediate state until
    /// [`Self::handle_end_txn_response`] is called with the broker's reply.
    pub fn end_txn(&mut self, request_id: u64, txn: TxnId, action: TxnAction) -> pb::CommandEndTxn {
        let waker_key = self.pending_end_txn.insert(noop_waker());
        let pending = PendingEndTxn {
            request_id: RequestId(request_id),
            txn,
            action,
            waker_key,
        };
        self.end_txn_by_request
            .insert(RequestId(request_id), pending);
        if let Some(meta) = self.transactions.get_mut(&txn) {
            meta.state = match action {
                TxnAction::Commit => TxnState::Committing,
                TxnAction::Abort => TxnState::Aborting,
            };
        }
        pb::CommandEndTxn {
            request_id,
            txnid_least_bits: Some(txn.least_sig_bits),
            txnid_most_bits: Some(txn.most_sig_bits),
            txn_action: Some(action.to_pb() as i32),
        }
    }

    /// Consume a `CommandEndTxnResponse`. On success the transaction transitions to
    /// `Committed` / `Aborted`. On broker error it transitions to `Errored`.
    ///
    /// Returns the resulting [`TxnState`] (so the caller can wake the user future with the final
    /// outcome).
    pub fn handle_end_txn_response(
        &mut self,
        resp: pb::CommandEndTxnResponse,
    ) -> Result<TxnState, TxnError> {
        let request_id = RequestId(resp.request_id);
        let Some(pending) = self.end_txn_by_request.remove(&request_id) else {
            // Stale response — best we can do is invent a benign state. Real drivers should
            // never see this because they index responses by request id before delegating here.
            return Ok(TxnState::Errored);
        };
        let waker = self.pending_end_txn.try_remove(pending.waker_key);

        if let Some(code) = resp.error {
            if let Some(meta) = self.transactions.get_mut(&pending.txn) {
                meta.state = TxnState::Errored;
            }
            if let Some(w) = waker {
                w.wake();
            }
            return Err(TxnError::from_broker(
                code,
                resp.message.unwrap_or_default(),
            ));
        }

        let final_state = match pending.action {
            TxnAction::Commit => TxnState::Committed,
            TxnAction::Abort => TxnState::Aborted,
        };
        if let Some(meta) = self.transactions.get_mut(&pending.txn) {
            meta.state = final_state;
        }
        if let Some(w) = waker {
            w.wake();
        }
        Ok(final_state)
    }

    /// Drop a transaction from the local registry. The caller is responsible for ensuring the TC
    /// has been notified (via `end_txn`) — this is purely a memory hygiene operation.
    pub fn forget(&mut self, txn: TxnId) {
        self.transactions.remove(&txn);
    }
}

/// Construct a no-op [`Waker`] used as a placeholder slot in the pending-op slabs.
///
/// We populate the slab immediately when a request is enqueued (so the slab key is stable for
/// later `register_*_waker` calls). The placeholder is overwritten before the first poll
/// completes; if no waker is ever registered, dropping the slot is harmless. `Waker::noop`
/// has been stable since Rust 1.85 (our MSRV).
fn noop_waker() -> Waker {
    Waker::noop().clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_new_txn_response(request_id: u64, most: u64, least: u64) -> pb::CommandNewTxnResponse {
        pb::CommandNewTxnResponse {
            request_id,
            txnid_most_bits: Some(most),
            txnid_least_bits: Some(least),
            error: None,
            message: None,
        }
    }

    #[test]
    fn new_txn_round_trip_returns_id_and_marks_open() {
        let mut client = TxnClient::new(7);
        let cmd = client.new_txn(1, 30_000);
        assert_eq!(cmd.request_id, 1);
        assert_eq!(cmd.tc_id, Some(7));
        assert_eq!(cmd.txn_ttl_seconds, Some(30));

        let id = client
            .handle_new_txn_response(ok_new_txn_response(1, 99, 42))
            .expect("ok")
            .expect("txn id present");
        assert_eq!(id, TxnId::new(99, 42));

        let meta = client.transaction(id).expect("registered");
        assert_eq!(meta.state, TxnState::Open);
        assert_eq!(meta.coordinator_id, 7);
        assert!(meta.produced_topics.is_empty());
        assert!(meta.acked_subscriptions.is_empty());
    }

    #[test]
    fn add_partition_records_topic_on_success() {
        let mut client = TxnClient::new(0);
        let _ = client.new_txn(1, 0);
        let id = client
            .handle_new_txn_response(ok_new_txn_response(1, 0, 1))
            .unwrap()
            .unwrap();

        let cmd = client.add_partition(2, id, "persistent://p/n/t".to_owned());
        assert_eq!(cmd.request_id, 2);
        assert_eq!(cmd.txnid_least_bits, Some(1));
        assert_eq!(cmd.partitions, vec!["persistent://p/n/t".to_owned()]);

        client
            .handle_add_partition_response(pb::CommandAddPartitionToTxnResponse {
                request_id: 2,
                txnid_least_bits: Some(1),
                txnid_most_bits: Some(0),
                error: None,
                message: None,
            })
            .expect("ok");

        let meta = client.transaction(id).unwrap();
        assert!(meta.produced_topics.contains("persistent://p/n/t"));
        assert_eq!(meta.state, TxnState::Open);
    }

    #[test]
    fn add_subscription_records_subscription_on_success() {
        let mut client = TxnClient::new(0);
        let _ = client.new_txn(1, 0);
        let id = client
            .handle_new_txn_response(ok_new_txn_response(1, 0, 2))
            .unwrap()
            .unwrap();

        let cmd =
            client.add_subscription(3, id, "sub-a".to_owned(), "persistent://p/n/t".to_owned());
        assert_eq!(cmd.request_id, 3);
        assert_eq!(cmd.subscription.len(), 1);
        assert_eq!(cmd.subscription[0].subscription, "sub-a");

        client
            .handle_add_subscription_response(pb::CommandAddSubscriptionToTxnResponse {
                request_id: 3,
                txnid_least_bits: Some(2),
                txnid_most_bits: Some(0),
                error: None,
                message: None,
            })
            .expect("ok");

        let meta = client.transaction(id).unwrap();
        let topics = meta.acked_subscriptions.get("sub-a").expect("present");
        assert_eq!(topics, &vec!["persistent://p/n/t".to_owned()]);
    }

    #[test]
    fn end_txn_commit_happy_path_marks_committed() {
        let mut client = TxnClient::new(0);
        let _ = client.new_txn(1, 0);
        let id = client
            .handle_new_txn_response(ok_new_txn_response(1, 0, 10))
            .unwrap()
            .unwrap();

        let cmd = client.end_txn(2, id, TxnAction::Commit);
        assert_eq!(cmd.txn_action, Some(pb::TxnAction::Commit as i32));
        assert_eq!(client.transaction(id).unwrap().state, TxnState::Committing);

        let final_state = client
            .handle_end_txn_response(pb::CommandEndTxnResponse {
                request_id: 2,
                txnid_least_bits: Some(10),
                txnid_most_bits: Some(0),
                error: None,
                message: None,
            })
            .expect("ok");
        assert_eq!(final_state, TxnState::Committed);
        assert_eq!(client.transaction(id).unwrap().state, TxnState::Committed);
    }

    #[test]
    fn end_txn_abort_happy_path_marks_aborted() {
        let mut client = TxnClient::new(0);
        let _ = client.new_txn(1, 0);
        let id = client
            .handle_new_txn_response(ok_new_txn_response(1, 0, 11))
            .unwrap()
            .unwrap();

        let cmd = client.end_txn(2, id, TxnAction::Abort);
        assert_eq!(cmd.txn_action, Some(pb::TxnAction::Abort as i32));
        assert_eq!(client.transaction(id).unwrap().state, TxnState::Aborting);

        let final_state = client
            .handle_end_txn_response(pb::CommandEndTxnResponse {
                request_id: 2,
                txnid_least_bits: Some(11),
                txnid_most_bits: Some(0),
                error: None,
                message: None,
            })
            .expect("ok");
        assert_eq!(final_state, TxnState::Aborted);
        assert_eq!(client.transaction(id).unwrap().state, TxnState::Aborted);
    }

    #[test]
    fn broker_transaction_conflict_maps_to_conflict_error() {
        let mut client = TxnClient::new(0);
        let _ = client.new_txn(1, 0);
        let err = client
            .handle_new_txn_response(pb::CommandNewTxnResponse {
                request_id: 1,
                txnid_most_bits: None,
                txnid_least_bits: None,
                error: Some(pb::ServerError::TransactionConflict as i32),
                message: Some("concurrent txn".to_owned()),
            })
            .expect_err("conflict");
        assert!(matches!(err, TxnError::Conflict));
        // No metadata should be inserted on error.
        assert!(client.is_empty());
    }

    #[test]
    fn broker_transaction_not_found_maps_to_not_found_error() {
        let mut client = TxnClient::new(0);
        let _ = client.new_txn(1, 0);
        let id = client
            .handle_new_txn_response(ok_new_txn_response(1, 0, 4))
            .unwrap()
            .unwrap();
        let _ = client.end_txn(2, id, TxnAction::Commit);

        let err = client
            .handle_end_txn_response(pb::CommandEndTxnResponse {
                request_id: 2,
                txnid_least_bits: Some(4),
                txnid_most_bits: Some(0),
                error: Some(pb::ServerError::TransactionNotFound as i32),
                message: Some("gc'd".to_owned()),
            })
            .expect_err("not found");
        assert!(matches!(err, TxnError::NotFound));
        assert_eq!(client.transaction(id).unwrap().state, TxnState::Errored);
    }

    #[test]
    fn unknown_broker_code_falls_through_to_broker_variant() {
        let mut client = TxnClient::new(0);
        let _ = client.new_txn(1, 0);
        let err = client
            .handle_new_txn_response(pb::CommandNewTxnResponse {
                request_id: 1,
                txnid_most_bits: None,
                txnid_least_bits: None,
                error: Some(pb::ServerError::PersistenceError as i32),
                message: Some("bookie down".to_owned()),
            })
            .expect_err("broker");
        match err {
            TxnError::Broker(code, msg) => {
                assert_eq!(code, pb::ServerError::PersistenceError as i32);
                assert_eq!(msg, "bookie down");
            }
            other => panic!("expected Broker variant, got {other:?}"),
        }
    }

    #[test]
    fn forget_drops_metadata() {
        let mut client = TxnClient::new(0);
        let _ = client.new_txn(1, 0);
        let id = client
            .handle_new_txn_response(ok_new_txn_response(1, 0, 1))
            .unwrap()
            .unwrap();
        assert!(client.transaction(id).is_some());
        client.forget(id);
        assert!(client.transaction(id).is_none());
    }

    /// Stale response (unknown request id) returns `Ok(None)` rather than producing a fresh
    /// `TxnId` — mirrors Java `TransactionImpl#handleResponse` which drops unknown ids on
    /// the floor instead of spuriously committing. Pinned because the driver dispatcher
    /// relies on this distinguishing "stale" from "broker said no".
    #[test]
    fn handle_new_txn_response_drops_unknown_request_id() {
        let mut client = TxnClient::new(0);
        // No `new_txn` was issued; an unsolicited response with request_id=42 should be
        // silently dropped — `Ok(None)` means "stale, ignore".
        let result = client.handle_new_txn_response(ok_new_txn_response(42, 0, 1));
        assert!(matches!(result, Ok(None)));
        // No metadata leaked.
        assert!(client.is_empty());
    }

    /// `TxnError::from_broker` must map `InvalidTxnStatus` to the `Aborted` variant so the
    /// user future surfaces a recoverable "transaction has been ended" error rather than
    /// the generic broker fall-through. Mirrors Java
    /// `TransactionCoordinatorClientException.translateException`.
    #[test]
    fn txn_error_invalid_status_maps_to_aborted() {
        let err = TxnError::from_broker(pb::ServerError::InvalidTxnStatus as i32, "ended".into());
        assert!(matches!(err, TxnError::Aborted));
    }

    /// `TxnError::from_broker` must map `TransactionCoordinatorNotFound` to `NotFound` —
    /// alongside the more obvious `TransactionNotFound` — so callers can use a single arm
    /// for the "TC has forgotten about this txn" failure mode. Mirrors the Java mapping in
    /// `TransactionCoordinatorClientException`.
    #[test]
    fn txn_error_tc_not_found_maps_to_not_found() {
        let err = TxnError::from_broker(
            pb::ServerError::TransactionCoordinatorNotFound as i32,
            "gc'd".into(),
        );
        assert!(matches!(err, TxnError::NotFound));
        // Plain TransactionNotFound also maps the same way.
        let err2 =
            TxnError::from_broker(pb::ServerError::TransactionNotFound as i32, "gc'd".into());
        assert!(matches!(err2, TxnError::NotFound));
    }

    /// `TxnId` derives `Display` formatting that mirrors Java
    /// `TxnID#toString` ("`mostSigBits:leastSigBits`"). Pinned because it appears in log
    /// lines + error messages and callers may parse it.
    #[test]
    fn txn_id_display_uses_colon_separator() {
        let id = TxnId::new(7, 42);
        assert_eq!(format!("{id}"), "7:42");
        // Sorted/Hashed consistently.
        assert_eq!(id, TxnId::new(7, 42));
    }

    /// After a broker error on `add_partition`, the transaction must transition to
    /// `Errored` and the topic must NOT be recorded in `produced_topics`. Pinned because
    /// the runtime relies on `Errored` to refuse subsequent `end_txn(Commit)` calls and
    /// surfaces the rollback path. Mirrors Java
    /// `TransactionImpl#registerProducedTopic` failure handling.
    #[test]
    fn add_partition_broker_error_marks_errored_and_skips_topic() {
        let mut client = TxnClient::new(0);
        let _ = client.new_txn(1, 0);
        let id = client
            .handle_new_txn_response(ok_new_txn_response(1, 0, 5))
            .unwrap()
            .unwrap();
        let _ = client.add_partition(2, id, "persistent://p/n/t".to_owned());

        let err = client
            .handle_add_partition_response(pb::CommandAddPartitionToTxnResponse {
                request_id: 2,
                txnid_least_bits: Some(5),
                txnid_most_bits: Some(0),
                error: Some(pb::ServerError::PersistenceError as i32),
                message: Some("bookie down".to_owned()),
            })
            .expect_err("broker error");
        assert!(matches!(err, TxnError::Broker(..)));

        let meta = client.transaction(id).expect("txn still tracked");
        assert_eq!(meta.state, TxnState::Errored);
        assert!(
            meta.produced_topics.is_empty(),
            "topic must NOT be recorded on broker error"
        );
    }

    /// Same as above but for `add_subscription`. The subscription must not be recorded and
    /// the txn must transition to `Errored`. Mirrors Java
    /// `TransactionImpl#registerAckedTopic` failure handling.
    #[test]
    fn add_subscription_broker_error_marks_errored_and_skips_subscription() {
        let mut client = TxnClient::new(0);
        let _ = client.new_txn(1, 0);
        let id = client
            .handle_new_txn_response(ok_new_txn_response(1, 0, 6))
            .unwrap()
            .unwrap();
        let _ = client.add_subscription(2, id, "sub-x".to_owned(), "persistent://p/n/t".to_owned());

        let err = client
            .handle_add_subscription_response(pb::CommandAddSubscriptionToTxnResponse {
                request_id: 2,
                txnid_least_bits: Some(6),
                txnid_most_bits: Some(0),
                error: Some(pb::ServerError::TransactionConflict as i32),
                message: Some("conflict".to_owned()),
            })
            .expect_err("broker error");
        assert!(matches!(err, TxnError::Conflict));

        let meta = client.transaction(id).expect("txn still tracked");
        assert_eq!(meta.state, TxnState::Errored);
        assert!(
            meta.acked_subscriptions.is_empty(),
            "subscription must NOT be recorded on broker error"
        );
    }
}
