// SPDX-License-Identifier: Apache-2.0

//! Regex-pattern consumer — subscribes to every topic in a namespace whose name matches a
//! broker-side regex pattern, then reconciles the subscription set against PIP-145
//! `TopicListChanged` deltas.
//!
//! Mirrors Java's `PatternMultiTopicsConsumerImpl`. The broker filters the watch by the
//! supplied pattern, so this client does not re-validate matches locally — it trusts the
//! broker's view.
//!
//! Reconciliation model
//! --------------------
//! - Initial snapshot: `topic_list_snapshot(namespace, pattern)` returns the current matching topic
//!   set; the builder subscribes to each and keeps the id of the broker-side watcher it opened.
//! - Streaming deltas: every `TopicListChanged` delta the broker pushes names its watcher, and the
//!   connection buffers the deltas of all of its watchers together. Each
//!   [`PatternConsumer::update`] call moves the pending deltas addressed to this consumer's own
//!   watcher (and no other's) from the connection buffer into this consumer's own backlog, in
//!   arrival order, and reconciles the consumer set from it: newly-added topics are subscribed,
//!   removed topics are closed and dropped. A step leaves the backlog only once it has taken
//!   effect, so a subscribe that fails or an `update` future that is dropped loses nothing: the
//!   next call resumes from what is left.
//! - Callers drive reconciliation explicitly (no spawned task) — call `update()` from a timer or
//!   before/after blocking work.
//!
//! Engine genericity (pass-2)
//! --------------------------
//! [`PatternConsumer<C>`] is generic over `C: crate::ConsumerApi`; child consumers are
//! subscribed through the engine-generic [`crate::ConsumerBuilder`] (which dispatches via
//! [`crate::SubscribeApi`]). The companion [`PatternConsumerBuilder<'a, E>`] is generic
//! over `E: crate::Engine` (default [`crate::TokioEngine`]) and looks up the initial topic
//! snapshot via [`crate::BrokerMetadataApi`]. The PIP-145 child-subscribe in
//! [`PatternConsumer::update`] also routes through `<E::ClientState as SubscribeApi>::subscribe`.
//!
//! Cancel safety
//! -------------
//! [`PatternConsumer::receive`] is cancel-safe in the same sense as
//! [`crate::MultiTopicsConsumer::receive`]: dropping the future without polling it to
//! completion leaves un-popped messages in their per-topic consumer queues.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::future::select_all;
use magnetar_proto::{IncomingMessage, MessageId};
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::client::PulsarError;
use crate::consumer_template::ConsumerTemplate;
use crate::{BrokerMetadataApi, ConsumerApi, Engine, PulsarClient, SubscribeApi, TopicListChange};

/// Regex-pattern consumer. Holds one consumer per matching topic and reconciles the set
/// against PIP-145 deltas on `update()`.
///
/// Generic over `C: crate::ConsumerApi` — defaults to the tokio runtime's `Consumer`.
/// The companion [`PatternConsumerBuilder<'a, E>`] selects the engine and produces a
/// `PatternConsumer<<E::ClientState as SubscribeApi>::Consumer>` on `.subscribe()`.
#[derive(Debug)]
pub struct PatternConsumer<C: ConsumerApi = magnetar_runtime_tokio::Consumer> {
    inner: Arc<Inner<C>>,
}

#[derive(Debug)]
struct Inner<C: ConsumerApi> {
    /// Active consumer set, keyed by topic name. Wrapped in an `Arc`
    /// so read-heavy callers (`receive`, `stats`, iteration) snapshot
    /// via a cheap `Arc::clone` instead of a deep `Vec::clone`; writes
    /// in the pattern-reconcile loop use `Arc::make_mut` to
    /// copy-on-write.
    consumers: Mutex<Arc<Vec<NamedConsumer<C>>>>,
    /// Signalled whenever a child consumer is added to the set (an addition in
    /// [`PatternConsumer::update`] — a topic that newly matched the pattern). The
    /// push-delivery poller races its in-flight `receive()` against this so a
    /// pattern child discovered *after* the poller parked is swept on the next
    /// iteration (pattern-child inheritance, ADR-0064). `Notify` (not a channel)
    /// keeps ADR-0003 intact; it stores one permit so an add that races a wait is
    /// not lost.
    membership_changed: Arc<Notify>,
    /// Round-robin cursor used by `receive`: the index, in the snapshot, of the child
    /// to start the next sweep at (the one after the child that answered last). Mirrors
    /// the cursor of [`crate::MultiTopicsConsumer`]; without it `select_all` always favours
    /// child 0 and a child that keeps a backlog starves every later one.
    cursor: AtomicUsize,
    /// Reconcile steps the broker announced and that have not taken effect yet, in arrival
    /// order. [`PatternConsumer::update`] moves every delta it polls from the connection into
    /// this queue before it applies anything, and removes a step only once it has taken effect,
    /// so a failed subscribe or a dropped `update` future leaves the rest for the next call:
    /// discovery is delta-only after the first snapshot, so a delta lost here is never
    /// announced again. The async mutex is held for the whole of an `update`, which serialises
    /// concurrent reconciles (they walk this queue in place) and is released when the future is
    /// dropped.
    pending: tokio::sync::Mutex<VecDeque<PendingOp>>,
    /// The id the connection allocated to this consumer's topic-list watcher. The connection's
    /// delta buffer is shared by every watcher on the client, and each delta is addressed to one
    /// of them: [`PatternConsumer::update`] takes only the deltas carrying this id, so it never
    /// applies another consumer's (a topic outside this pattern) and never starves their owner.
    watcher_id: u64,
    /// Namespace + pattern recorded for diagnostics and for re-snapshot operations.
    namespace: String,
    pattern: String,
    /// Template for subscribing newly-discovered topics. Captures every
    /// [`crate::ConsumerBuilder`] knob the user set on the original
    /// [`PatternConsumerBuilder`].
    template: ConsumerTemplate,
}

#[derive(Debug, Clone)]
struct NamedConsumer<C: ConsumerApi> {
    topic: String,
    consumer: C,
}

/// One reconcile step announced by the broker that has not taken effect yet.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PendingOp {
    /// A topic that newly matches the pattern: subscribe a child for it.
    Add(String),
    /// A topic that no longer matches the pattern: close its child and drop it.
    Remove(String),
}

/// Append one `TopicListChanged` delta to the backlog, keeping the order the broker sent it in.
///
/// Within a delta the removals come first, then the additions, as before. Across deltas the
/// order is the arrival order: merging them into one added list and one removed list, with the
/// removals applied first, would subscribe a topic the broker created and then deleted. A removal
/// also supersedes an addition of the same topic that is still waiting (it never took effect, or
/// it failed and was kept for a retry): applied in order, the pair leaves the topic out of the
/// set, and keeping the addition would retry a subscribe for a topic that no longer exists
/// behind the removal. The removal itself is kept, because the topic may have been subscribed
/// by an earlier step. An addition already waiting is not queued twice.
fn enqueue_change(pending: &mut VecDeque<PendingOp>, change: TopicListChange) {
    pending.retain(|op| !matches!(op, PendingOp::Add(topic) if change.removed.contains(topic)));
    pending.extend(change.removed.into_iter().map(PendingOp::Remove));
    for topic in change.added {
        let op = PendingOp::Add(topic);
        if !pending.contains(&op) {
            pending.push_back(op);
        }
    }
}

/// A message yielded by [`PatternConsumer::receive`], carrying the topic it came from.
#[derive(Debug)]
pub struct PatternMessage {
    /// The topic the message originated from.
    pub topic: String,
    /// Underlying message + payload.
    pub message: IncomingMessage,
}

/// Outcome of a single [`PatternConsumer::update`] reconciliation cycle.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Number of newly-subscribed topics.
    pub added: usize,
    /// Number of topics closed and dropped.
    pub removed: usize,
}

impl<C: ConsumerApi + Clone> PatternConsumer<C> {
    /// Namespace this consumer is watching, as supplied to the builder.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.inner.namespace
    }

    /// Regex pattern this consumer is watching, as supplied to the builder.
    #[must_use]
    pub fn pattern(&self) -> &str {
        &self.inner.pattern
    }

    /// Subscription name shared across every per-topic child.
    #[must_use]
    pub fn subscription(&self) -> &str {
        &self.inner.template.subscription
    }

    /// Snapshot of the topics currently subscribed, in the order they were added.
    #[must_use]
    pub fn topics(&self) -> Vec<String> {
        self.inner
            .consumers
            .lock()
            .iter()
            .map(|c| c.topic.clone())
            .collect()
    }

    /// Number of underlying consumers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.consumers.lock().len()
    }

    /// `true` if the consumer set is empty (no topic in the namespace currently matches).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.consumers.lock().is_empty()
    }

    /// Move the pending PIP-145 `TopicListChanged` deltas addressed to this consumer's watcher from
    /// the underlying connection into this consumer's backlog and reconcile the consumer set from
    /// it: newly-added topics are subscribed, removed topics are closed and dropped.
    ///
    /// Only the deltas of the broker-side watcher this consumer's builder opened are taken. Other
    /// watchers on the client (another pattern consumer, a `topic_list_snapshot`) keep theirs
    /// buffered for their own owners, so a consumer never subscribes a topic the broker announced
    /// for somebody else's pattern. `client` must be the client that built the consumer: a watcher
    /// id is only meaningful on the connection that allocated it.
    ///
    /// Deltas are applied in the order the broker sent them, so a topic created and then deleted
    /// between two calls is never subscribed. Idempotent; returns the count of additions and
    /// removals applied during this call. Mirrors Java's internal
    /// `PatternMultiTopicsConsumerImpl#recheckTopics` cycle.
    ///
    /// A step leaves the backlog only once it has taken effect. A subscribe that fails keeps its
    /// topic waiting (the broker announces a topic once, so dropping it would lose it for the
    /// life of the consumer) and the walk goes on with the steps behind it, so one refused topic
    /// does not hold up the others; the next call retries it, until the broker reports the topic
    /// deleted. Dropping the returned future between two steps likewise leaves every step that has
    /// not completed for the next call. One `update` runs at a time: a concurrent call waits for
    /// the running one.
    ///
    /// PIP-145 child-subscribe routes through the engine-generic
    /// [`crate::ConsumerBuilder`] which dispatches via
    /// [`crate::SubscribeApi`].
    ///
    /// # Errors
    ///
    /// Returns the first [`PulsarError`] encountered while subscribing a new topic, after the
    /// walk has reached the end of the backlog; every other step is applied, and the topics that
    /// failed are retried by the next call.
    pub async fn update<E>(&self, client: &PulsarClient<E>) -> Result<ReconcileReport, PulsarError>
    where
        E: Engine,
        E::ClientState: SubscribeApi<Consumer = C> + BrokerMetadataApi,
    {
        let mut report = ReconcileReport::default();
        let mut pending = self.inner.pending.lock().await;
        // Move every polled delta into the backlog before anything is applied. No await sits
        // between a delta leaving the connection and landing in the backlog, so a cancelled
        // call cannot lose one in between.
        while let Some(change) =
            crate::BrokerMetadataApi::poll_topic_list_change(&client.inner, self.inner.watcher_id)
        {
            enqueue_change(&mut pending, change);
        }
        if pending.is_empty() {
            return Ok(report);
        }
        let mut first_error: Option<PulsarError> = None;
        // Walk the backlog in order. A step that takes effect is removed at the position being
        // read; a subscribe that fails stays, and the walk moves on past it.
        let mut next = 0;
        while let Some(op) = pending.get(next).cloned() {
            match op {
                PendingOp::Remove(topic) => {
                    // Leave the backlog before the close is awaited: the child is already out of
                    // the set, so a dropped close must not be repeated against a topic that is
                    // gone.
                    pending.remove(next);
                    let drained: Vec<NamedConsumer<C>> = {
                        let mut guard = self.inner.consumers.lock();
                        let mut drained = Vec::new();
                        Arc::make_mut(&mut guard).retain(|nc| {
                            if nc.topic == topic {
                                drained.push(nc.clone());
                                false
                            } else {
                                true
                            }
                        });
                        drained
                    };
                    for nc in drained {
                        let _ = ConsumerApi::close_owned(nc.consumer).await;
                        report.removed += 1;
                    }
                }
                PendingOp::Add(topic) => {
                    // Skip a topic already in the set (the broker can resend a topic if multiple
                    // watch responses overlap during reconnects).
                    let already_subscribed = self
                        .inner
                        .consumers
                        .lock()
                        .iter()
                        .any(|nc| nc.topic == topic);
                    if already_subscribed {
                        pending.remove(next);
                        continue;
                    }
                    let builder = self.inner.template.apply(client.consumer(topic.clone()));
                    match builder.subscribe().await {
                        Ok(consumer) => {
                            // No await between the subscribe completing and the child joining
                            // the set, or a dropped call would leave a live child outside it.
                            Arc::make_mut(&mut self.inner.consumers.lock())
                                .push(NamedConsumer { topic, consumer });
                            pending.remove(next);
                            report.added += 1;
                            // A new child joined the set — wake any parked wrapper-listener
                            // poller so it re-snapshots and starts draining the new child.
                            self.inner.membership_changed.notify_one();
                        }
                        Err(err) => {
                            tracing::warn!(
                                target: "magnetar::pattern_consumer",
                                namespace = %self.namespace(),
                                pattern = %self.pattern(),
                                topic = %topic,
                                error = %err,
                                "pattern child subscribe failed; kept for the next update",
                            );
                            first_error.get_or_insert(err);
                            next += 1;
                        }
                    }
                }
            }
        }
        // Discovery-update record (ADR-0054) — only reached when at least
        // one step was waiting, so this never fires on quiescent ticks.
        tracing::debug!(
            target: "magnetar::pattern_consumer",
            namespace = %self.namespace(),
            pattern = %self.pattern(),
            added = report.added,
            removed = report.removed,
            "pattern discovery delta"
        );
        match first_error {
            Some(err) => Err(err),
            None => Ok(report),
        }
    }

    /// Receive the next message across any currently-subscribed topic. The future is
    /// cancel-safe: dropping it leaves un-popped messages in their respective per-consumer
    /// queues.
    ///
    /// The sweep is fair, with the same round-robin cursor as
    /// [`crate::MultiTopicsConsumer::receive`]: it starts after the topic served last, so a
    /// topic that keeps a backlog cannot starve the others. It does not stall on a dead
    /// child either: a child that is terminal (closed, a terminal subscribe failure, or its
    /// connection lost for good) resolves `Err` at once and forever, so it leaves the sweep and
    /// the wait goes on over the remaining topics. Messages still queued on such a child are
    /// delivered first. The failure is therefore invisible to this call as long as one topic
    /// is live; poll [`PatternConsumer::is_connected`] to observe it. The terminal child stays
    /// in the set, unserved, until the broker removes its topic ([`PatternConsumer::update`]
    /// applies only the broker's topic-list deltas and does not re-create it).
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Config`] if the consumer set is empty. A child that fails on one
    /// message (a decrypt or decompress failure under the default
    /// `CryptoFailureAction::Fail`) while it stays open returns that error, naming its topic;
    /// the next call can succeed. Returns an error once every child of the snapshot is
    /// terminal, naming the last one.
    pub async fn receive(&self) -> Result<PatternMessage, PulsarError> {
        // Snapshot the consumer set under the lock, then release before awaiting — holding the
        // mutex across an await would serialise receive against update.
        let snapshot: Arc<Vec<NamedConsumer<C>>> = self.inner.consumers.lock().clone();
        if snapshot.is_empty() {
            return Err(PulsarError::Config(
                "no topics matched the pattern — nothing to receive".to_owned(),
            ));
        }
        if snapshot.len() == 1 {
            let nc = &snapshot[0];
            let msg = nc
                .consumer
                .receive()
                .await
                .map_err(|e| PulsarError::Other(format!("receive: {}: {e}", nc.topic)))?;
            return Ok(PatternMessage {
                topic: nc.topic.clone(),
                message: msg,
            });
        }
        // Rotate the snapshot by the round-robin cursor BEFORE building the futures: `select_all`
        // returns the first ready future in input order, so without the rotation a child that
        // always has a message ready starves every later child.
        let len = snapshot.len();
        let start = self.inner.cursor.load(Ordering::Relaxed) % len;
        // Children whose `receive()` resolved `Err` while terminal. Such a child resolves `Err`
        // on its first poll, forever, so left in the sweep it would win every round and hide
        // every sibling behind it. Only an `Err` is ever discarded here: an `Ok` has already
        // popped its message and returns in the iteration that produced it.
        let mut terminal = vec![false; len];
        loop {
            // `rotated_indices[i]` is the snapshot index of the i-th future `select_all` sees,
            // in sweep order, so a winning result maps back to the right topic. It is rebuilt
            // each round rather than patched after `select_all`, which documents no order for
            // the futures it hands back.
            let rotated_indices: Vec<usize> = (0..len)
                .map(|i| (start + i) % len)
                .filter(|&i| !terminal[i])
                .collect();
            let futures: Vec<_> = rotated_indices
                .iter()
                .map(|&i| snapshot[i].consumer.receive())
                .collect();
            let (result, rotated_idx, _rest) = select_all(futures).await;
            let original_idx = rotated_indices[rotated_idx];
            let nc = &snapshot[original_idx];
            let err = match result {
                Ok(message) => {
                    // Advance the cursor past the child we just served so the next `receive()`
                    // starts the sweep at the child *after* this one.
                    self.inner
                        .cursor
                        .store((original_idx + 1) % len, Ordering::Relaxed);
                    return Ok(PatternMessage {
                        topic: nc.topic.clone(),
                        message,
                    });
                }
                Err(err) => err,
            };
            if !ConsumerApi::is_terminal(&nc.consumer) {
                // An error about one message: the child is still open and its next `receive()`
                // can succeed. Move the cursor past it as for a message, so a child that keeps
                // failing cannot hold the sweep either.
                self.inner
                    .cursor
                    .store((original_idx + 1) % len, Ordering::Relaxed);
                return Err(PulsarError::Other(format!("receive: {}: {err}", nc.topic)));
            }
            terminal[original_idx] = true;
            if terminal.iter().all(|&done| done) {
                return Err(PulsarError::Other(format!(
                    "receive: every matched topic is terminal, last error on {}: {err}",
                    nc.topic
                )));
            }
        }
    }

    /// Acknowledge a message. The caller supplies the topic the message came from
    /// (carried by [`PatternMessage::topic`]) so the ack routes to the right child.
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Config`] if the topic is no longer in the active set (e.g. a
    /// concurrent `update()` removed it). Otherwise returns the child consumer's ack error.
    pub async fn ack(&self, topic: &str, message_id: MessageId) -> Result<(), PulsarError> {
        let consumer = self
            .lookup(topic)
            .map_err(|err| PulsarError::Config(format!("ack: {err}")))?;
        consumer
            .ack(message_id)
            .await
            .map_err(|e| PulsarError::Other(format!("ack: {e}")))
    }

    /// Cumulative ack on the per-topic child that produced `message_id`.
    ///
    /// # Errors
    ///
    /// See [`Self::ack`].
    pub async fn ack_cumulative(
        &self,
        topic: &str,
        message_id: MessageId,
    ) -> Result<(), PulsarError> {
        let consumer = self
            .lookup(topic)
            .map_err(|err| PulsarError::Config(format!("ack_cumulative: {err}")))?;
        consumer
            .ack_cumulative(message_id)
            .await
            .map_err(|e| PulsarError::Other(format!("ack_cumulative: {e}")))
    }

    /// Negatively acknowledge a message on the per-topic child that produced it.
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Config`] if the topic is no longer in the active set.
    pub fn negative_ack(&self, topic: &str, message_id: MessageId) -> Result<(), PulsarError> {
        let consumer = self
            .lookup(topic)
            .map_err(|err| PulsarError::Config(format!("negative_ack: {err}")))?;
        consumer.negative_ack(message_id);
        Ok(())
    }

    /// Redeliver every unacked message across every child consumer. Mirrors Java
    /// `Consumer#redeliverUnacknowledgedMessages` at the pattern scope.
    pub fn redeliver_unacked(&self) {
        for nc in self.inner.consumers.lock().iter() {
            nc.consumer.redeliver_unacked();
        }
    }

    /// Aggregate cumulative stats across every child consumer (issue #347).
    /// Thin wrapper over [`magnetar_proto::ConsumerStats::fold`], and the exact
    /// analogue of [`crate::MultiTopicsConsumer::aggregate_stats`] at the pattern
    /// scope: collect each child's `(stats(), receive_latency_histogram())`
    /// snapshot — taken under the same lock acquisition so the pair is mutually
    /// consistent — and fold them per that function's documented per-field rule.
    /// The six cumulative totals + `pending_batch_acks` sum; `msgs_per_sec` /
    /// `bytes_per_sec` sum as f64 (fan-in throughput); `receive_latency_max_ms` is
    /// the exact max; `receive_latency_p50_ms` / `receive_latency_p99_ms` are
    /// recomputed from a REAL merge of every child's receive-latency histogram
    /// rather than summed or maxed, which is not statistically sound.
    ///
    /// A child subscribed mid-window by [`PatternConsumer::update`] contributes a
    /// full snapshot of its counters immediately, but `0.0` to the rate fields
    /// until it has been sampled twice — the rate window needs a baseline first.
    /// Java's `MultiTopicConsumerStatsRecorderImpl` behaves identically.
    ///
    /// The rate fields are populated by the client-wide sweep armed with
    /// [`crate::ClientBuilder::stats_interval`], which reaches every child of
    /// this subscription because it ticks each slot on the connection rather
    /// than fanning out from here (ADR-0089 — Java's wrappers have no fan-out
    /// either). With that knob unset they stay caller-driven; see the note on
    /// [`magnetar_proto::consumer::ConsumerState::record_rate_window`].
    #[must_use]
    pub fn aggregate_stats(&self) -> magnetar_proto::ConsumerStats {
        let children: Vec<_> = self
            .inner
            .consumers
            .lock()
            .iter()
            .map(|nc| (nc.consumer.stats(), nc.consumer.receive_latency_histogram()))
            .collect();
        magnetar_proto::ConsumerStats::fold(children)
    }

    /// `true` while every child consumer reports the underlying connection is up.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        let guard = self.inner.consumers.lock();
        !guard.is_empty() && guard.iter().all(|c| c.consumer.is_connected())
    }

    /// `true` once every child consumer is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        let guard = self.inner.consumers.lock();
        guard.iter().all(|c| c.consumer.is_closed())
    }

    /// Spawn a background tokio task that drives [`PatternConsumer::update`] on a periodic
    /// ticker, mirroring Java's `PatternMultiTopicsConsumerImpl` internal reconciliation
    /// timer.
    ///
    /// The task ticks every `interval`, calls `update(&client)`, swallows errors (logged
    /// at `warn`), and exits cleanly once [`PatternConsumer::is_closed`] returns `true`.
    /// The caller can also stop the loop early by calling
    /// [`tokio::task::JoinHandle::abort`] on the returned handle — the task is not stored
    /// inside the consumer, so it never outlives the caller's intent.
    ///
    /// The returned [`tokio::task::JoinHandle`] is detached from the consumer: dropping
    /// the consumer does not abort the task on its own, but the next tick after every
    /// child is closed will observe `is_closed()` and return. For deterministic teardown,
    /// abort the handle before dropping the consumer.
    ///
    /// `client` is taken as [`Arc`] because the task captures it for the lifetime of the
    /// ticker loop; callers typically already hold the client behind an [`Arc`].
    ///
    /// Engine-generic: works under any `E` whose `ClientState` implements both
    /// [`crate::SubscribeApi`] (for child-subscribes) and
    /// [`crate::BrokerMetadataApi`] (for delta polling) — both runtimes do.
    #[must_use = "the JoinHandle must be retained to abort the auto-reconcile task; dropping it detaches the task"]
    pub fn start_auto_reconcile<E>(
        &self,
        client: std::sync::Arc<PulsarClient<E>>,
        interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()>
    where
        E: Engine,
        E::ClientState: SubscribeApi<Consumer = C> + BrokerMetadataApi,
    {
        let consumer = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // Skip missed ticks if the previous reconciliation overran — we only care about
            // catching up to the current topic set, not replaying every missed deadline.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // The first tick fires immediately; consume it so we don't reconcile twice in
            // quick succession when the caller has just built the consumer.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                // Exit once every child consumer is closed — but only after the set has
                // been non-empty at least once. A pattern that has not yet matched any
                // topic must keep ticking; otherwise `is_closed()` (vacuously `true` on
                // an empty set) would terminate the task before the first match.
                if !consumer.is_empty() && consumer.is_closed() {
                    break;
                }
                if let Err(err) = consumer.update(&client).await {
                    tracing::warn!(
                        target: "magnetar::pattern_consumer",
                        error = %err,
                        namespace = %consumer.namespace(),
                        pattern = %consumer.pattern(),
                        "auto-reconcile tick failed",
                    );
                }
            }
        })
    }

    /// Close every underlying consumer. Drops the consumer set and returns the first
    /// per-child error encountered. Mirrors `MultiTopicsConsumer::close` semantics: best-effort
    /// teardown — every child gets a chance to close.
    ///
    /// # Errors
    ///
    /// Returns the first child-close error; subsequent errors are swallowed.
    pub async fn close(self) -> Result<(), PulsarError> {
        let inner = match Arc::try_unwrap(self.inner) {
            Ok(i) => i,
            Err(arc) => {
                drop(arc);
                return Ok(());
            }
        };
        let consumers_arc = inner.consumers.into_inner();
        let consumers = Arc::try_unwrap(consumers_arc).unwrap_or_else(|arc| (*arc).clone());
        let mut first_err: Result<(), PulsarError> = Ok(());
        for nc in consumers {
            if let Err(e) = ConsumerApi::close_owned(nc.consumer).await
                && first_err.is_ok()
            {
                first_err = Err(PulsarError::Other(format!("close: {e}")));
            }
        }
        first_err
    }

    fn lookup(&self, topic: &str) -> Result<C, String> {
        self.inner
            .consumers
            .lock()
            .iter()
            .find(|c| c.topic == topic)
            .map(|c| c.consumer.clone())
            .ok_or_else(|| format!("unknown topic {topic} on pattern consumer"))
    }
}

impl<C: ConsumerApi> Clone for PatternConsumer<C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Push-delivery support: a [`PatternConsumer`] drives the wrapper listener
/// poller via its topic-fanning [`Self::receive`]. Children discovered after
/// subscribe — when a [`Self::update`] reconciliation cycle subscribes a topic
/// that newly matched the pattern (PIP-145 `TopicListChanged`) — **inherit** the
/// listener: `receive()` re-snapshots the child set on every call, so a child
/// added between two `receive()` calls is delivered on the next sweep. This
/// mirrors Java `PatternMultiTopicsConsumerImpl`, where the parent owns the
/// single listener executor and routes every child (initial or later-discovered)
/// through it.
impl<C> crate::consumer_listener::WrapperReceiver for PatternConsumer<C>
where
    C: ConsumerApi + Clone + Send + Sync + 'static,
{
    async fn wrapper_receive(&self) -> Result<(String, IncomingMessage), PulsarError> {
        let m = self.receive().await?;
        Ok((m.topic, m.message))
    }

    fn is_empty(&self) -> bool {
        PatternConsumer::is_empty(self)
    }

    fn is_terminal(&self) -> bool {
        // Clone the snapshot and release the set's mutex before asking the children: each
        // `ConsumerApi::is_terminal` takes the global Connection mutex (ADR-0038 order), and a
        // child the wrapper has already removed is not in the snapshot, so closing it on
        // purpose never reads as a terminal wrapper. `receive()` skips a terminal child and
        // keeps serving the live ones, so the wrapper can fail for good only once every child is
        // terminal; an empty set is not terminal (the poller parks on `membership_changed`).
        let snapshot: Arc<Vec<NamedConsumer<C>>> = self.inner.consumers.lock().clone();
        !snapshot.is_empty()
            && snapshot
                .iter()
                .all(|nc| ConsumerApi::is_terminal(&nc.consumer))
    }

    async fn membership_changed(&self) {
        let notify = self.inner.membership_changed.clone();
        notify.notified().await;
    }
}

/// Builder for [`PatternConsumer`]. Mirrors Java's
/// `PulsarClient#newConsumer().topicsPattern(...)`.
///
/// Generic over `E: crate::Engine` (default [`crate::TokioEngine`]). The
/// `.subscribe()` method snapshots the matching topics via
/// [`crate::BrokerMetadataApi::watch_topic_list`] and opens each per-topic child
/// through the engine-generic [`crate::ConsumerBuilder`].
pub struct PatternConsumerBuilder<'a, E: Engine = crate::TokioEngine> {
    client: &'a PulsarClient<E>,
    namespace: Option<String>,
    pattern: Option<String>,
    subscription: Option<String>,
    consumer_name: Option<String>,
    sub_type: magnetar_proto::pb::command_subscribe::SubType,
    receiver_queue_size: usize,
    max_uncompressed_message_size: Option<usize>,
    /// Issue #301: pluggable receiver-queue policy propagated to every discovered-topic child.
    receiver_queue_policy: Option<std::sync::Arc<dyn magnetar_proto::ReceiverQueuePolicy>>,
    receiver_queue_adjust_interval: Option<std::time::Duration>,
    initial_position: magnetar_proto::pb::command_subscribe::InitialPosition,
    durable: bool,
    properties: Vec<(String, String)>,
    negative_ack_redelivery_delay: Option<std::time::Duration>,
    ack_timeout: Option<std::time::Duration>,
    ack_group_time: Option<std::time::Duration>,
    dlq_policy: Option<(u32, Option<String>)>,
    max_pending_chunked_message: Option<usize>,
    auto_ack_oldest_chunked_message_on_queue_full: Option<bool>,
    expire_time_of_incomplete_chunked_message: Option<std::time::Duration>,
    read_compacted: bool,
    priority_level: Option<i32>,
    subscription_properties: Vec<(String, String)>,
    key_shared: Option<magnetar_proto::KeySharedConfig>,
    replicate_subscription_state: Option<bool>,
    force_topic_creation: Option<bool>,
    start_message_rollback_duration_sec: Option<u64>,
    /// Optional push-delivery callback (Java `ConsumerBuilder#messageListener` at
    /// the pattern scope). Set via [`Self::message_listener`] and subscribe via
    /// [`Self::subscribe_with_listener`]; the plain [`Self::subscribe`] ignores it.
    listener: Option<crate::consumer_listener::WrapperMessageListener>,
}

impl<E: Engine> std::fmt::Debug for PatternConsumerBuilder<'_, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PatternConsumerBuilder")
            .field("namespace", &self.namespace)
            .field("pattern", &self.pattern)
            .field("subscription", &self.subscription)
            .finish_non_exhaustive()
    }
}

impl<'a, E: Engine> PatternConsumerBuilder<'a, E> {
    pub(crate) fn new(client: &'a PulsarClient<E>) -> Self {
        Self {
            client,
            namespace: None,
            pattern: None,
            subscription: None,
            consumer_name: None,
            sub_type: magnetar_proto::pb::command_subscribe::SubType::Exclusive,
            receiver_queue_size: 1000,
            max_uncompressed_message_size: None,
            receiver_queue_policy: None,
            receiver_queue_adjust_interval: None,
            initial_position: magnetar_proto::pb::command_subscribe::InitialPosition::Latest,
            durable: true,
            properties: Vec::new(),
            negative_ack_redelivery_delay: None,
            ack_timeout: None,
            ack_group_time: None,
            dlq_policy: None,
            max_pending_chunked_message: None,
            auto_ack_oldest_chunked_message_on_queue_full: None,
            expire_time_of_incomplete_chunked_message: None,
            read_compacted: false,
            priority_level: None,
            subscription_properties: Vec::new(),
            key_shared: None,
            replicate_subscription_state: None,
            force_topic_creation: None,
            start_message_rollback_duration_sec: None,
            listener: None,
        }
    }

    /// Test-support seam (`#[doc(hidden)]`, not part of the stable API):
    /// `true` once a push-delivery listener has been set via
    /// [`Self::message_listener`].
    #[doc(hidden)]
    #[must_use]
    pub fn has_listener_for_test(&self) -> bool {
        self.listener.is_some()
    }

    /// Test-support seam (`#[doc(hidden)]`): the consumer name this builder will
    /// propagate to every per-topic child. Lets the builder-surface guard test
    /// pin the `name()` → field plumbing without a broker.
    #[doc(hidden)]
    #[must_use]
    pub fn consumer_name_for_test(&self) -> Option<&str> {
        self.consumer_name.as_deref()
    }

    /// Register a push-delivery callback (Java `ConsumerBuilder#messageListener`
    /// at the pattern scope). Once set, subscribe via
    /// [`Self::subscribe_with_listener`] to start a background poller over the
    /// matched-topic consumer set, handing every message to `listener`
    /// sequentially and in order, with no auto-ack. The callback receives the
    /// originating topic so it can ack against the right child via
    /// [`PatternConsumer::ack`]. Topics discovered after subscribe (on
    /// [`PatternConsumer::update`] reconciliation) inherit the listener. The plain
    /// [`Self::subscribe`] ignores the listener and returns a pull-mode consumer.
    #[must_use]
    pub fn message_listener(
        mut self,
        listener: crate::consumer_listener::WrapperMessageListener,
    ) -> Self {
        self.listener = Some(listener);
        self
    }

    /// Required: pulsar namespace to watch, e.g. `public/default`.
    #[must_use]
    pub fn namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = Some(ns.into());
        self
    }

    /// Required: broker-side regex pattern. Pulsar applies Java regex semantics on the broker
    /// — confirm any regex you rely on parses identically there.
    #[must_use]
    pub fn pattern(mut self, regex: impl Into<String>) -> Self {
        self.pattern = Some(regex.into());
        self
    }

    /// Required: subscription name applied to every per-topic child.
    #[must_use]
    pub fn subscription(mut self, name: impl Into<String>) -> Self {
        self.subscription = Some(name.into());
        self
    }

    /// Mirrors `ConsumerBuilder::name`. Propagated verbatim to every per-topic
    /// child (including topics discovered later on reconciliation), so broker
    /// `topics stats` reports the same `consumerName` for each. Default `None`
    /// lets the broker assign a name.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.consumer_name = Some(name.into());
        self
    }

    /// Test-support seam (`#[doc(hidden)]`): the bounded-chunk-reassembly knobs
    /// propagated to every per-topic child. Lets the builder-surface guard test
    /// pin the setter → field plumbing without a broker.
    #[doc(hidden)]
    #[must_use]
    pub fn chunk_knobs_for_test(
        &self,
    ) -> (Option<usize>, Option<bool>, Option<std::time::Duration>) {
        (
            self.max_pending_chunked_message,
            self.auto_ack_oldest_chunked_message_on_queue_full,
            self.expire_time_of_incomplete_chunked_message,
        )
    }

    /// Set the subscription type applied to every per-topic child.
    #[must_use]
    pub fn subscription_type(
        mut self,
        sub_type: magnetar_proto::pb::command_subscribe::SubType,
    ) -> Self {
        self.sub_type = sub_type;
        self
    }

    /// Set the receiver queue size on every per-topic child.
    #[must_use]
    pub fn receiver_queue_size(mut self, size: usize) -> Self {
        self.receiver_queue_size = size;
        self
    }

    /// Set the initial position on every per-topic child.
    #[must_use]
    pub fn initial_position(
        mut self,
        position: magnetar_proto::pb::command_subscribe::InitialPosition,
    ) -> Self {
        self.initial_position = position;
        self
    }

    /// Toggle durability of the underlying subscriptions.
    #[must_use]
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// Mirrors `ConsumerBuilder::property` — forwarded onto every per-topic child.
    #[must_use]
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.push((key.into(), value.into()));
        self
    }

    /// Mirrors `ConsumerBuilder::negative_ack_redelivery_delay`.
    #[must_use]
    pub fn negative_ack_redelivery_delay(mut self, delay: std::time::Duration) -> Self {
        self.negative_ack_redelivery_delay = Some(delay);
        self
    }

    /// Mirrors `ConsumerBuilder::ack_timeout`.
    #[must_use]
    pub fn ack_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.ack_timeout = Some(timeout);
        self
    }

    /// Mirrors `ConsumerBuilder::ack_group_time`. Applied to every per-topic child.
    #[must_use]
    pub fn ack_group_time(mut self, window: std::time::Duration) -> Self {
        self.ack_group_time = Some(window);
        self
    }

    /// Mirrors `ConsumerBuilder::dead_letter_policy`.
    #[must_use]
    pub fn dead_letter_policy(
        mut self,
        max_redeliver_count: u32,
        dead_letter_topic: Option<String>,
    ) -> Self {
        self.dlq_policy = Some((max_redeliver_count, dead_letter_topic));
        self
    }

    /// Mirrors `ConsumerBuilder::max_pending_chunked_message`.
    #[must_use]
    pub fn max_pending_chunked_message(mut self, max: usize) -> Self {
        self.max_pending_chunked_message = Some(max);
        self
    }

    /// Set the per-child decompressed payload ceiling in bytes. Defaults to 5 MiB.
    #[must_use]
    pub fn max_uncompressed_message_size(mut self, bytes: usize) -> Self {
        self.max_uncompressed_message_size = Some(bytes);
        self
    }

    /// Mirrors `ConsumerBuilder::auto_ack_oldest_chunked_message_on_queue_full`.
    #[must_use]
    pub fn auto_ack_oldest_chunked_message_on_queue_full(mut self, auto_ack: bool) -> Self {
        self.auto_ack_oldest_chunked_message_on_queue_full = Some(auto_ack);
        self
    }

    /// Mirrors `ConsumerBuilder::expire_time_of_incomplete_chunked_message`.
    #[must_use]
    pub fn expire_time_of_incomplete_chunked_message(
        mut self,
        expire: std::time::Duration,
    ) -> Self {
        self.expire_time_of_incomplete_chunked_message = Some(expire);
        self
    }

    /// Mirrors `ConsumerBuilder::read_compacted`.
    #[must_use]
    pub fn read_compacted(mut self, on: bool) -> Self {
        self.read_compacted = on;
        self
    }

    /// Mirrors `ConsumerBuilder::priority_level`.
    #[must_use]
    pub fn priority_level(mut self, level: i32) -> Self {
        self.priority_level = Some(level);
        self
    }

    /// Mirrors `ConsumerBuilder::subscription_property` — appends a (key, value) pair to
    /// every per-topic child's subscription metadata.
    #[must_use]
    pub fn subscription_property(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.subscription_properties
            .push((key.into(), value.into()));
        self
    }

    /// Mirrors `ConsumerBuilder::key_shared_policy`.
    #[must_use]
    pub fn key_shared_policy(mut self, cfg: magnetar_proto::KeySharedConfig) -> Self {
        self.key_shared = Some(cfg);
        self
    }

    /// Mirrors `ConsumerBuilder::replicate_subscription_state`.
    #[must_use]
    pub fn replicate_subscription_state(mut self, on: bool) -> Self {
        self.replicate_subscription_state = Some(on);
        self
    }

    /// Mirrors `ConsumerBuilder::force_topic_creation`.
    #[must_use]
    pub fn force_topic_creation(mut self, on: bool) -> Self {
        self.force_topic_creation = Some(on);
        self
    }

    /// Mirrors `ConsumerBuilder::start_message_rollback_duration`.
    #[must_use]
    pub fn start_message_rollback_duration(mut self, seconds: u64) -> Self {
        self.start_message_rollback_duration_sec = Some(seconds);
        self
    }
}

impl<E> PatternConsumerBuilder<'_, E>
where
    E: Engine,
    E::ClientState: SubscribeApi + BrokerMetadataApi,
    <E::ClientState as SubscribeApi>::Consumer: Clone,
{
    /// Take an initial snapshot of matching topics, subscribe to each, and return the
    /// [`PatternConsumer`]. Call [`PatternConsumer::update`] periodically to reconcile
    /// against newly-emitted PIP-145 deltas — newly-discovered topics inherit every knob
    /// configured here.
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Config`] if a required field is missing, [`PulsarError::Other`]
    /// if the broker refuses the watch, or the first per-topic subscribe error if a topic in
    /// the snapshot cannot be opened (already-opened topics are torn down before the error).
    pub async fn subscribe(
        self,
    ) -> Result<PatternConsumer<<E::ClientState as SubscribeApi>::Consumer>, PulsarError> {
        let mut deadline =
            crate::SubscribeApi::new_subscribe_operation_deadline(&self.client.inner);
        self.subscribe_with_deadline(&mut deadline).await
    }

    async fn subscribe_with_deadline(
        self,
        deadline: &mut crate::OperationDeadline,
    ) -> Result<PatternConsumer<<E::ClientState as SubscribeApi>::Consumer>, PulsarError> {
        let namespace = self
            .namespace
            .ok_or_else(|| PulsarError::Config("namespace is required".to_owned()))?;
        let pattern = self
            .pattern
            .ok_or_else(|| PulsarError::Config("pattern is required".to_owned()))?;
        let subscription = self
            .subscription
            .ok_or_else(|| PulsarError::Config("subscription name is required".to_owned()))?;

        let template = ConsumerTemplate {
            subscription,
            consumer_name: self.consumer_name,
            sub_type: self.sub_type,
            receiver_queue_size: self.receiver_queue_size,
            max_uncompressed_message_size: self.max_uncompressed_message_size,
            receiver_queue_policy: self.receiver_queue_policy,
            receiver_queue_adjust_interval: self.receiver_queue_adjust_interval,
            initial_position: self.initial_position,
            durable: self.durable,
            properties: self.properties,
            negative_ack_redelivery_delay: self.negative_ack_redelivery_delay,
            ack_timeout: self.ack_timeout,
            ack_group_time: self.ack_group_time,
            dlq_policy: self.dlq_policy,
            max_pending_chunked_message: self.max_pending_chunked_message,
            auto_ack_oldest_chunked_message_on_queue_full: self
                .auto_ack_oldest_chunked_message_on_queue_full,
            expire_time_of_incomplete_chunked_message: self
                .expire_time_of_incomplete_chunked_message,
            read_compacted: self.read_compacted,
            priority_level: self.priority_level,
            subscription_properties: self.subscription_properties,
            key_shared: self.key_shared,
            replicate_subscription_state: self.replicate_subscription_state,
            force_topic_creation: self.force_topic_creation,
            start_message_rollback_duration_sec: self.start_message_rollback_duration_sec,
        };

        let (watcher_id, topics) = self
            .client
            .topic_list_snapshot_with_deadline(&namespace, &pattern, deadline)
            .await?;

        let mut opened: Vec<NamedConsumer<<E::ClientState as SubscribeApi>::Consumer>> =
            Vec::with_capacity(topics.len());
        for topic in &topics {
            let builder = template.apply(self.client.consumer(topic.clone()));
            match builder.subscribe_with_deadline(deadline).await {
                Ok(c) => opened.push(NamedConsumer {
                    topic: topic.clone(),
                    consumer: c,
                }),
                Err(e) => {
                    for nc in opened {
                        let _ = ConsumerApi::close_owned(nc.consumer).await;
                    }
                    return Err(e);
                }
            }
        }

        Ok(PatternConsumer {
            inner: Arc::new(Inner {
                consumers: Mutex::new(Arc::new(opened)),
                membership_changed: Arc::new(Notify::new()),
                cursor: AtomicUsize::new(0),
                pending: tokio::sync::Mutex::new(VecDeque::new()),
                watcher_id,
                namespace,
                pattern,
                template,
            }),
        })
    }
}

impl<E> PatternConsumerBuilder<'_, E>
where
    E: Engine,
    E::ClientState: SubscribeApi + BrokerMetadataApi,
    <E::ClientState as SubscribeApi>::Consumer: Clone + Send + Sync + 'static,
{
    /// Take the initial topic snapshot, subscribe to each match, and start a
    /// push-delivery poller over the resulting [`PatternConsumer`], returning the
    /// owning [`crate::MessageListenerHandle`]. Mirrors Java's
    /// `PatternMultiTopicsConsumerImpl` listener path.
    ///
    /// Same semantics as [`crate::MultiTopicsConsumerBuilder::subscribe_with_listener`]:
    /// sequential, in-order delivery, no auto-ack (the callback acks via
    /// [`PatternConsumer::ack`]), clean shutdown once every child is terminal / on handle
    /// drop (a terminal child among live ones is skipped, see [`PatternConsumer::receive`],
    /// and so is an error on one message). **Pattern
    /// children discovered after subscribe inherit the listener** —
    /// call [`PatternConsumer::update`] (or
    /// [`PatternConsumer::start_auto_reconcile`]) to subscribe newly-matched
    /// topics; the running poller picks them up on its next `receive()` sweep, since
    /// it shares the consumer's `Arc<Inner>` and `receive()` re-snapshots the child
    /// set each call. (Hold an extra [`PatternConsumer`] clone before
    /// `subscribe_with_listener` if you need to drive `update()` yourself — the
    /// poller consumes the consumer it is handed.)
    ///
    /// # Errors
    /// - [`PulsarError::Config`] if no listener was set via [`Self::message_listener`].
    /// - any subscribe / watch error from [`Self::subscribe`].
    pub async fn subscribe_with_listener(
        self,
    ) -> Result<crate::MessageListenerHandle, PulsarError> {
        let Some(listener) = self.listener.clone() else {
            return Err(PulsarError::Config(
                "subscribe_with_listener() requires a listener — \
                 call message_listener(...) first (or use subscribe() for pull mode)"
                    .to_owned(),
            ));
        };
        let consumer = self.subscribe().await?;
        Ok(crate::consumer_listener::spawn_wrapper_message_listener(
            consumer, listener,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use parking_lot::Mutex;

    use super::{
        ConsumerTemplate, Inner, PatternConsumer, PendingOp, ReconcileReport, enqueue_change,
    };
    use crate::TopicListChange;

    fn change(added: &[&str], removed: &[&str]) -> TopicListChange {
        TopicListChange {
            added: added.iter().map(|t| (*t).to_owned()).collect(),
            removed: removed.iter().map(|t| (*t).to_owned()).collect(),
        }
    }

    fn add(topic: &str) -> PendingOp {
        PendingOp::Add(topic.to_owned())
    }

    fn remove(topic: &str) -> PendingOp {
        PendingOp::Remove(topic.to_owned())
    }

    /// Deltas keep the order the broker sent them in, and within one delta the removals come
    /// before the additions.
    #[test]
    fn enqueue_keeps_arrival_order_across_and_within_deltas() {
        let mut pending = VecDeque::new();
        enqueue_change(&mut pending, change(&["a", "b"], &["x"]));
        enqueue_change(&mut pending, change(&["c"], &["y"]));
        assert_eq!(
            Vec::from(pending),
            [remove("x"), add("a"), add("b"), remove("y"), add("c")]
        );
    }

    /// A removal supersedes a still-waiting addition of the same topic (a topic created then
    /// deleted, or an addition kept for a retry after a refused subscribe), and is itself kept.
    /// The addition of another topic is untouched.
    #[test]
    fn enqueue_removal_drops_the_waiting_addition_of_that_topic_only() {
        let mut pending = VecDeque::from([add("fleeting"), add("kept")]);
        enqueue_change(&mut pending, change(&[], &["fleeting"]));
        assert_eq!(Vec::from(pending), [add("kept"), remove("fleeting")]);
    }

    /// A delta that removes and adds the same topic (deleted then re-created within the delta)
    /// ends with the topic added: the removal of the delta does not take its own addition with it.
    #[test]
    fn enqueue_same_delta_removal_does_not_drop_its_own_addition() {
        let mut pending = VecDeque::from([add("t")]);
        enqueue_change(&mut pending, change(&["t"], &["t"]));
        assert_eq!(Vec::from(pending), [remove("t"), add("t")]);
    }

    /// An addition already waiting (kept after a refused subscribe) is not queued a second time
    /// when the broker announces the topic again, and the same topic twice in one delta is queued
    /// once.
    #[test]
    fn enqueue_does_not_queue_a_waiting_addition_twice() {
        let mut pending = VecDeque::from([add("t")]);
        enqueue_change(&mut pending, change(&["t", "u", "u"], &[]));
        assert_eq!(Vec::from(pending), [add("t"), add("u")]);
    }

    #[test]
    fn reconcile_report_default_is_zero() {
        let r = ReconcileReport::default();
        assert_eq!(r.added, 0);
        assert_eq!(r.removed, 0);
    }

    #[test]
    fn reconcile_report_eq() {
        let a = ReconcileReport {
            added: 2,
            removed: 1,
        };
        let b = ReconcileReport {
            added: 2,
            removed: 1,
        };
        assert_eq!(a, b);
    }

    /// Helper: build an empty [`PatternConsumer`] for tests. The set is empty and the
    /// embedded template is a defaults-only stub — sufficient for tests that never reach
    /// `update()` or any per-topic dispatch.
    fn empty_pattern_consumer() -> PatternConsumer<magnetar_runtime_tokio::Consumer> {
        let template = ConsumerTemplate {
            subscription: "test-sub".to_owned(),
            consumer_name: None,
            sub_type: magnetar_proto::pb::command_subscribe::SubType::Exclusive,
            receiver_queue_size: 1000,
            max_uncompressed_message_size: None,
            receiver_queue_policy: None,
            receiver_queue_adjust_interval: None,
            initial_position: magnetar_proto::pb::command_subscribe::InitialPosition::Latest,
            durable: true,
            properties: Vec::new(),
            negative_ack_redelivery_delay: None,
            ack_timeout: None,
            ack_group_time: None,
            dlq_policy: None,
            max_pending_chunked_message: None,
            auto_ack_oldest_chunked_message_on_queue_full: None,
            expire_time_of_incomplete_chunked_message: None,
            read_compacted: false,
            priority_level: None,
            subscription_properties: Vec::new(),
            key_shared: None,
            replicate_subscription_state: None,
            force_topic_creation: None,
            start_message_rollback_duration_sec: None,
        };
        PatternConsumer {
            inner: Arc::new(Inner {
                consumers: Mutex::new(Arc::new(Vec::new())),
                membership_changed: Arc::new(tokio::sync::Notify::new()),
                cursor: std::sync::atomic::AtomicUsize::new(0),
                pending: tokio::sync::Mutex::new(std::collections::VecDeque::new()),
                watcher_id: 0,
                namespace: "public/default".to_owned(),
                pattern: "persistent://public/default/test-.*".to_owned(),
                template,
            }),
        }
    }

    /// Exercise the [`PatternConsumer::start_auto_reconcile`] abort path.
    ///
    /// We cannot construct a live [`crate::PulsarClient`] without a broker, so this test
    /// reproduces the auto-reconcile loop body inline using the same primitives
    /// (`tokio::time::interval`, clone of `Arc<Inner>`, `is_empty()` + `is_closed()`
    /// guard). With an empty consumer set the loop never reaches `update()` — the first
    /// `tick()` fires immediately and the second blocks on the long interval, so abort
    /// catches the task parked on the second tick and tears it down cleanly.
    ///
    /// This guards against three regressions: (1) the abort path producing a panic
    /// instead of a `JoinError::is_cancelled()`, (2) the empty-set early-exit short
    /// circuit firing despite the `!is_empty()` guard, and (3) the consumer's `Arc<Inner>`
    /// not being captured by the task (which would force the test to keep a separate
    /// reference alive).
    #[tokio::test(flavor = "current_thread")]
    async fn auto_reconcile_aborts_cleanly_on_empty_set() {
        let consumer = empty_pattern_consumer();
        // Sanity check: an empty pattern consumer reports empty and vacuously closed.
        assert!(consumer.is_empty(), "fresh test consumer should be empty");
        assert!(
            consumer.is_closed(),
            "empty consumer is vacuously closed; the auto-reconcile loop must \
             not treat that as an exit condition while the set is empty",
        );

        let consumer_for_task = consumer.clone();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_hours(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Mirror `start_auto_reconcile`: consume the immediate first tick, then loop
            // on the long interval and exit only when the set has become non-empty and
            // every child reports closed.
            ticker.tick().await;
            #[allow(clippy::never_loop)]
            loop {
                ticker.tick().await;
                if !consumer_for_task.is_empty() && consumer_for_task.is_closed() {
                    break;
                }
                // No client is held in this test surrogate — the real
                // `start_auto_reconcile` would invoke `update(&client)` here. Reaching
                // this point inside the test would be a bug: it means the long-interval
                // tick fired prematurely.
                unreachable!("auto-reconcile tick fired before abort");
            }
        });

        // Yield once so the spawned task progresses past the immediate first tick and
        // parks on the long-interval second tick.
        tokio::task::yield_now().await;

        handle.abort();
        let join = handle.await;
        match join {
            Err(err) => assert!(
                err.is_cancelled(),
                "join error must signal cancellation, got: {err:?}",
            ),
            Ok(()) => panic!("auto-reconcile task completed instead of being cancelled"),
        }

        // The consumer must still be usable after the task is torn down — the abort
        // tears down only the spawned future, not the shared `Arc<Inner>`.
        assert!(consumer.is_empty());
        assert_eq!(consumer.len(), 0);
    }

    /// Compile-time witness that [`PatternConsumer::start_auto_reconcile`] has the
    /// exact signature required by the Java-parity API: takes `&self`, an
    /// `Arc<PulsarClient>`, a `Duration`, and returns `JoinHandle<()>`.
    #[test]
    fn start_auto_reconcile_signature() {
        let _: fn(
            &PatternConsumer<magnetar_runtime_tokio::Consumer>,
            std::sync::Arc<crate::PulsarClient>,
            std::time::Duration,
        ) -> tokio::task::JoinHandle<()> =
            PatternConsumer::start_auto_reconcile::<crate::TokioEngine>;
    }
}
