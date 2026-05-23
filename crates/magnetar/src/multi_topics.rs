// SPDX-License-Identifier: Apache-2.0

//! Multi-topics consumer — subscribes to N topics and merges their delivery streams.
//!
//! Mirrors Java's `MultiTopicsConsumerImpl`. The consumer is a thin coordinator over a
//! `Vec<Consumer>` with `receive()` returning the first message ready across all underlying
//! consumers. Cancelling the future leaves un-popped messages in their respective consumer
//! queues — see the `cancel-safe` discussion in [`magnetar_runtime_tokio::Consumer::receive`].
//!
//! Dynamic membership
//! ------------------
//! The consumer set is held under a [`parking_lot::Mutex`], so callers can subscribe new
//! topics via [`MultiTopicsConsumer::add_topic`] and tear them down via
//! [`MultiTopicsConsumer::remove_topic`] without rebuilding the consumer. New topics inherit
//! every knob set on the original [`MultiTopicsConsumerBuilder`] (captured as a template
//! inside [`Inner`], mirroring `PatternConsumer`). Mirrors Java
//! `MultiTopicsConsumerImpl#subscribeAsync(String)` / `#unsubscribeAsync(String)`.
//!
//! No regex / pattern subscription (yet); callers pass an explicit topic list. Regex /
//! pattern support layers on top via a broker-side topic-list-watcher (PIP-145), which is
//! exposed by [`magnetar_proto::Connection`] but not wired through this facade — see
//! [`crate::PatternConsumer`].

use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::future::select_all;
use magnetar_proto::{IncomingMessage, MessageId};
use magnetar_runtime_tokio::Consumer;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::PulsarClient;
use crate::client::{PulsarError, SeekTarget};

/// Multi-topics consumer. Each contained consumer subscribes to one topic; `receive()`
/// returns the next message across the whole set.
///
/// Phantom-generic over `C: ConsumerApi` per ADR-0026 §D1 — the
/// type parameter is present (defaulting to
/// `magnetar_runtime_tokio::Consumer`) but the inherent impl is
/// currently bound to the default. Full lift requires lifting
/// `ConsumerBuilder` itself (which `add_topic` uses to subscribe
/// new children), which is a separate sub-PR. See
/// `docs/follow-ups.md`.
#[derive(Debug)]
pub struct MultiTopicsConsumer<C: crate::ConsumerApi = Consumer> {
    inner: Arc<Inner<C>>,
}

#[derive(Debug)]
struct Inner<C: crate::ConsumerApi = Consumer> {
    /// Active consumer set. Held under a mutex so [`MultiTopicsConsumer::add_topic`] /
    /// [`MultiTopicsConsumer::remove_topic`] can mutate the set without rebuilding the
    /// consumer. Every other method snapshots the Vec under the lock and releases before
    /// awaiting — the mutex is never held across `.await`.
    consumers: Mutex<Vec<NamedConsumer<C>>>,
    /// Round-robin cursor used by `receive` to record the index of the topic that produced
    /// the last message. Wrapped in a Mutex because [`MultiTopicsConsumer`] is `&self` —
    /// cloning the handle should not require mutable access.
    cursor: Mutex<usize>,
    /// Template for subscribing newly-added topics. Captures every
    /// [`crate::ConsumerBuilder`] knob the user set on the original
    /// [`MultiTopicsConsumerBuilder`].
    template: ConsumerTemplate,
    /// Optional background partition-watcher task. `Some` when the builder
    /// configured
    /// [`MultiTopicsConsumerBuilder::auto_update_partitions_interval`], `None`
    /// otherwise (default). Tracks one watched topic — the "base" topic used by a
    /// [`crate::PartitionedConsumer`] (the underlying surface). Aborts on drop.
    auto_update: Option<Arc<AutoUpdateTask>>,
}

/// Background partition-watcher (Java parity:
/// `ConsumerBuilder#autoUpdatePartitionsInterval`).
///
/// Spawned by [`MultiTopicsConsumerBuilder::subscribe`] /
/// [`crate::PartitionedConsumerBuilder::subscribe`] when the builder records a
/// non-zero interval via
/// [`MultiTopicsConsumerBuilder::auto_update_partitions_interval`]. The spawned task
/// is a pure timer that signals [`Self::changed`] every `interval`; the actual
/// `PulsarClient::partitions_for_topic` call is driven by user code via
/// [`MultiTopicsConsumer::refresh_partitions`] (the crate-wide
/// `#![forbid(unsafe_code)]` rules out punning the `&PulsarClient` lifetime into a
/// `'static` spawn).
///
/// Lifetime is bounded by the [`MultiTopicsConsumer`]: dropping every clone of the
/// consumer drops the `Arc<Inner>`, which drops the `Arc<AutoUpdateTask>`, which
/// aborts the spawned tokio task in [`Drop`]. No channels — coordination is
/// `Arc<Mutex<...>>` + [`tokio::sync::Notify`] + [`tokio::time::interval`] (per the
/// project's "no channels in Rust async code" policy).
#[derive(Debug)]
struct AutoUpdateTask {
    /// Topic the watcher polls — typically the base topic the
    /// [`crate::PartitionedConsumer`] was built against (without the
    /// `-partition-N` suffix).
    topic: String,
    /// Last partition count observed by the watcher.
    observed_partitions: Arc<Mutex<u32>>,
    /// Monotonic counter of "partition count changed" events. Useful for tests and
    /// "did anything change since I last looked?" probes.
    change_count: Arc<Mutex<u64>>,
    /// Signalled every time the internal timer fires, and every time
    /// [`MultiTopicsConsumer::refresh_partitions`] detects a real partition-count
    /// change.
    changed: Arc<Notify>,
    /// Signalled on drop to cooperatively wake the loop sleeping on [`Notify`] so it
    /// can notice it has been aborted promptly.
    shutdown: Arc<Notify>,
    /// The spawned task. Held in a [`tokio::sync::Mutex`] so [`Drop`] can take it on
    /// the best-effort path without blocking.
    handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl Drop for AutoUpdateTask {
    fn drop(&mut self) {
        self.shutdown.notify_waiters();
        if let Ok(mut g) = self.handle.try_lock()
            && let Some(h) = g.take()
        {
            h.abort();
        }
    }
}

/// Spawn the partition-watcher *timer* task. See the doc on
/// [`crate::partitioned_producer::spawn_auto_update_task`] for the shape — same
/// pattern.
fn spawn_auto_update_task(
    topic: String,
    interval: Duration,
    initial_partitions: u32,
) -> Arc<AutoUpdateTask> {
    let observed_partitions = Arc::new(Mutex::new(initial_partitions));
    let change_count = Arc::new(Mutex::new(0u64));
    let changed = Arc::new(Notify::new());
    let shutdown = Arc::new(Notify::new());

    let changed_task = changed.clone();
    let shutdown_task = shutdown.clone();

    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                () = shutdown_task.notified() => break,
                _ = ticker.tick() => {}
            }
            changed_task.notify_waiters();
        }
    });

    Arc::new(AutoUpdateTask {
        topic,
        observed_partitions,
        change_count,
        changed,
        shutdown,
        handle: tokio::sync::Mutex::new(Some(handle)),
    })
}

/// Frozen [`crate::ConsumerBuilder`] template propagated to every per-topic child. Stored
/// inside [`Inner`] so [`MultiTopicsConsumer::add_topic`] can subscribe newly-added topics
/// with the same configuration as the initial set.
#[derive(Debug, Clone)]
struct ConsumerTemplate {
    subscription: String,
    sub_type: magnetar_proto::pb::command_subscribe::SubType,
    receiver_queue_size: usize,
    initial_position: magnetar_proto::pb::command_subscribe::InitialPosition,
    durable: bool,
    properties: Vec<(String, String)>,
    negative_ack_redelivery_delay: Option<std::time::Duration>,
    ack_timeout: Option<std::time::Duration>,
    ack_group_time: Option<std::time::Duration>,
    dlq_policy: Option<(u32, Option<String>)>,
    read_compacted: bool,
    priority_level: Option<i32>,
    subscription_properties: Vec<(String, String)>,
    key_shared: Option<magnetar_proto::KeySharedConfig>,
    replicate_subscription_state: Option<bool>,
    force_topic_creation: Option<bool>,
    start_message_rollback_duration_sec: Option<u64>,
}

impl ConsumerTemplate {
    /// Apply the template to a [`crate::ConsumerBuilder`] for the given topic.
    fn apply<'a>(&self, mut builder: crate::ConsumerBuilder<'a>) -> crate::ConsumerBuilder<'a> {
        builder = builder
            .subscription(self.subscription.clone())
            .subscription_type(self.sub_type)
            .durable(self.durable)
            .initial_position(self.initial_position)
            .receiver_queue_size(self.receiver_queue_size)
            .read_compacted(self.read_compacted);
        for (k, v) in &self.properties {
            builder = builder.property(k.clone(), v.clone());
        }
        if let Some(d) = self.negative_ack_redelivery_delay {
            builder = builder.negative_ack_redelivery_delay(d);
        }
        if let Some(t) = self.ack_timeout {
            builder = builder.ack_timeout(t);
        }
        if let Some(w) = self.ack_group_time {
            builder = builder.ack_group_time(w);
        }
        if let Some((max, topic_opt)) = &self.dlq_policy {
            builder = builder.dead_letter_policy(*max, topic_opt.clone());
        }
        if let Some(level) = self.priority_level {
            builder = builder.priority_level(level);
        }
        for (k, v) in &self.subscription_properties {
            builder = builder.subscription_property(k.clone(), v.clone());
        }
        if let Some(cfg) = self.key_shared.clone() {
            builder = builder.key_shared_policy(cfg);
        }
        if let Some(on) = self.replicate_subscription_state {
            builder = builder.replicate_subscription_state(on);
        }
        if let Some(on) = self.force_topic_creation {
            builder = builder.force_topic_creation(on);
        }
        if let Some(s) = self.start_message_rollback_duration_sec {
            builder = builder.start_message_rollback_duration(s);
        }
        builder
    }
}

#[derive(Debug, Clone)]
struct NamedConsumer<C: crate::ConsumerApi = Consumer> {
    topic: String,
    consumer: C,
}

/// A message yielded by [`MultiTopicsConsumer::receive`], carrying the topic it came from.
#[derive(Debug)]
pub struct MultiTopicsMessage {
    /// The topic the message originated from (the same string the caller supplied to the
    /// builder).
    pub topic: String,
    /// Underlying message + payload.
    pub message: IncomingMessage,
}

impl MultiTopicsConsumer {
    /// Topics this consumer is currently subscribed to, in the order they were added (initial
    /// builder order followed by [`Self::add_topic`] insertions, minus any topic removed via
    /// [`Self::remove_topic`]).
    #[must_use]
    pub fn topics(&self) -> Vec<String> {
        self.inner
            .consumers
            .lock()
            .iter()
            .map(|c| c.topic.clone())
            .collect()
    }

    /// Number of underlying consumers (one per topic).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.consumers.lock().len()
    }

    /// `true` if the consumer set is currently empty (e.g. every topic has been removed).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.consumers.lock().is_empty()
    }

    /// Shared subscription name across every per-topic child. Mirrors Java
    /// `Consumer#getSubscription` at the multi-topic / partitioned scope.
    #[must_use]
    pub fn subscription(&self) -> &str {
        &self.inner.template.subscription
    }

    /// Subscribe a new per-topic child against the current consumer set. The new child
    /// inherits every knob configured on the original [`MultiTopicsConsumerBuilder`].
    /// Mirrors Java `MultiTopicsConsumerImpl#subscribeAsync(String topicName)`.
    ///
    /// Idempotent: if `topic` is already in the set the call is a no-op and returns `Ok(())`
    /// — mirrors Java's behaviour of refusing to double-subscribe the same topic.
    ///
    /// # Errors
    ///
    /// Returns the underlying subscribe error if the broker refuses the new subscription.
    /// The consumer set is left untouched on error.
    pub async fn add_topic(
        &self,
        client: &PulsarClient,
        topic: impl Into<String>,
    ) -> Result<(), PulsarError> {
        let topic = topic.into();
        // Check membership under the lock and release before awaiting — never hold the
        // mutex across an `.await`.
        let already_subscribed = self
            .inner
            .consumers
            .lock()
            .iter()
            .any(|nc| nc.topic == topic);
        if already_subscribed {
            return Ok(());
        }
        let builder = self.inner.template.apply(client.consumer(topic.clone()));
        let consumer = builder.subscribe().await?;
        // Re-check membership under the lock to handle a concurrent `add_topic(topic)` —
        // if a peer raced us, drop our newly-subscribed consumer and close it; otherwise
        // push it. The guard is released before any `.await`
        // (clippy::await_holding_lock).
        let to_close: Option<Consumer> = {
            let mut guard = self.inner.consumers.lock();
            if guard.iter().any(|nc| nc.topic == topic) {
                Some(consumer)
            } else {
                guard.push(NamedConsumer { topic, consumer });
                None
            }
        };
        if let Some(c) = to_close {
            let _ = c.close().await;
        }
        Ok(())
    }

    /// Tear down the per-topic child subscribed to `topic` and remove it from the set.
    /// Mirrors Java `MultiTopicsConsumerImpl#unsubscribeAsync(String topicName)`.
    ///
    /// No-op if `topic` is not currently in the set.
    ///
    /// # Errors
    ///
    /// Returns the underlying close error from the per-topic consumer.
    pub async fn remove_topic(&self, topic: &str) -> Result<(), PulsarError> {
        // Remove under the lock, release, then close — never hold the mutex across await.
        let removed: Option<NamedConsumer> = {
            let mut guard = self.inner.consumers.lock();
            guard
                .iter()
                .position(|nc| nc.topic == topic)
                .map(|pos| guard.remove(pos))
        };
        if let Some(nc) = removed {
            nc.consumer.close().await.map_err(PulsarError::Client)?;
        }
        Ok(())
    }

    /// Negatively acknowledge a message. The caller supplies the topic the message came
    /// from (returned alongside the message in [`MultiTopicsMessage::topic`]) so the nack
    /// goes to the correct per-topic consumer.
    pub fn negative_ack(&self, topic: &str, message_id: MessageId) -> Result<(), PulsarError> {
        let consumer = self
            .lookup(topic)
            .map_err(|err| PulsarError::Config(format!("negative_ack: {err}")))?;
        consumer.negative_ack(message_id);
        Ok(())
    }

    /// Negatively acknowledge with an explicit per-message redelivery delay. Mirrors
    /// Java's PIP-37 backoff path at the multi-topic / partitioned scope. The caller
    /// supplies the topic the message came from so the nack routes to the correct child.
    pub fn negative_ack_with_delay(
        &self,
        topic: &str,
        message_id: MessageId,
        delay: std::time::Duration,
    ) -> Result<(), PulsarError> {
        let consumer = self
            .lookup(topic)
            .map_err(|err| PulsarError::Config(format!("negative_ack_with_delay: {err}")))?;
        consumer.negative_ack_with_delay(message_id, delay);
        Ok(())
    }

    /// Cumulative ack. The caller supplies the topic the message came from so the ack
    /// routes to the correct child. Mirrors Java
    /// `Consumer#acknowledgeCumulativeAsync(MessageId)` at the multi-topic scope.
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
            .map_err(PulsarError::Client)
    }

    /// Fire-and-forget ack into the per-topic child's ack-grouping tracker (opt-in via
    /// `MultiTopicsConsumerBuilder::ack_group_time`). The caller supplies the topic the
    /// message came from so the ack routes to the correct child. See
    /// [`magnetar_runtime_tokio::Consumer::ack_grouped`].
    pub fn ack_grouped(&self, topic: &str, message_id: MessageId) -> Result<(), PulsarError> {
        let consumer = self
            .lookup(topic)
            .map_err(|err| PulsarError::Config(format!("ack_grouped: {err}")))?;
        consumer.ack_grouped(message_id);
        Ok(())
    }

    /// Fire-and-forget cumulative ack into the per-topic child's ack-grouping tracker.
    /// See [`Self::ack_grouped`] for the routing semantics.
    pub fn ack_grouped_cumulative(
        &self,
        topic: &str,
        message_id: MessageId,
    ) -> Result<(), PulsarError> {
        let consumer = self
            .lookup(topic)
            .map_err(|err| PulsarError::Config(format!("ack_grouped_cumulative: {err}")))?;
        consumer.ack_grouped_cumulative(message_id);
        Ok(())
    }

    /// Republish `msg` via `retry_producer` with a delay, then ack the original on the
    /// per-topic child. Mirrors Java `Consumer#reconsumeLater` at the multi-topic scope.
    /// The caller supplies the topic the message came from (returned alongside the
    /// message in [`MultiTopicsMessage::topic`]) so the ack routes to the correct child.
    pub async fn reconsume_later(
        &self,
        topic: &str,
        retry_producer: &magnetar_runtime_tokio::Producer,
        msg: IncomingMessage,
        delay: std::time::Duration,
    ) -> Result<(), PulsarError> {
        let consumer = self
            .lookup(topic)
            .map_err(|err| PulsarError::Config(format!("reconsume_later: {err}")))?;
        consumer
            .reconsume_later(retry_producer, msg, delay)
            .await
            .map_err(PulsarError::Client)
    }

    /// Same as [`Self::reconsume_later`] but stamps custom properties on the republished
    /// message. Mirrors Java's properties-aware reconsumeLater overload.
    pub async fn reconsume_later_with_properties(
        &self,
        topic: &str,
        retry_producer: &magnetar_runtime_tokio::Producer,
        msg: IncomingMessage,
        custom_properties: Vec<(String, String)>,
        delay: std::time::Duration,
    ) -> Result<(), PulsarError> {
        let consumer = self.lookup(topic).map_err(|err| {
            PulsarError::Config(format!("reconsume_later_with_properties: {err}"))
        })?;
        consumer
            .reconsume_later_with_properties(retry_producer, msg, custom_properties, delay)
            .await
            .map_err(PulsarError::Client)
    }

    /// Tell the broker to redeliver every unacked message across every child consumer.
    /// Mirrors Java `Consumer#redeliverUnacknowledgedMessages` at the multi-topic scope.
    pub fn redeliver_unacked(&self) {
        for nc in self.inner.consumers.lock().iter() {
            nc.consumer.redeliver_unacked();
        }
    }

    /// Receive the next message across any subscribed topic. The future is cancel-safe:
    /// dropping it without polling to completion leaves all unpopped messages in their
    /// respective per-consumer queues.
    pub async fn receive(&self) -> Result<MultiTopicsMessage, PulsarError> {
        // Snapshot the consumer set under the lock and release before awaiting — holding
        // the mutex across an await would serialise receive against add_topic /
        // remove_topic.
        let snapshot: Vec<NamedConsumer> = { self.inner.consumers.lock().clone() };
        if snapshot.is_empty() {
            return Err(PulsarError::Config(
                "no topics subscribed to receive from".to_owned(),
            ));
        }
        if snapshot.len() == 1 {
            let nc = &snapshot[0];
            let msg = nc.consumer.receive().await?;
            *self.inner.cursor.lock() = 0;
            return Ok(MultiTopicsMessage {
                topic: nc.topic.clone(),
                message: msg,
            });
        }

        let futures: Vec<_> = snapshot
            .iter()
            .map(|nc| nc.consumer.receive().boxed())
            .collect();
        let (result, idx, _rest) = select_all(futures).await;
        let topic = snapshot[idx].topic.clone();
        let message = result?;
        *self.inner.cursor.lock() = (idx + 1) % snapshot.len();
        Ok(MultiTopicsMessage { topic, message })
    }

    /// Acknowledge a message. The caller supplies the topic the message came from (returned
    /// alongside the message in [`MultiTopicsMessage::topic`]) so we can route the ack to
    /// the correct per-topic consumer.
    pub async fn ack(&self, topic: &str, message_id: MessageId) -> Result<(), PulsarError> {
        let consumer = self
            .lookup(topic)
            .map_err(|err| PulsarError::Config(format!("ack on multi-consumer: {err}")))?;
        consumer.ack(message_id).await.map_err(PulsarError::Client)
    }

    /// `true` while every child consumer reports the underlying connection is up.
    /// Mirrors Java `Consumer#isConnected` at the multi-topic / partitioned scope.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        let guard = self.inner.consumers.lock();
        !guard.is_empty() && guard.iter().all(|c| c.consumer.is_connected())
    }

    /// Earliest disconnect wall-clock across all child consumers. `None` if no child has
    /// ever disconnected.
    #[must_use]
    pub fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        self.inner
            .consumers
            .lock()
            .iter()
            .filter_map(|c| c.consumer.last_disconnected_timestamp())
            .min()
    }

    /// Aggregate cumulative stats across all child consumers. Sums the totals; useful
    /// for monitoring fan-in throughput on the multi-topic / partitioned scope.
    #[must_use]
    pub fn aggregate_stats(&self) -> magnetar_proto::ConsumerStats {
        let mut agg = magnetar_proto::ConsumerStats::default();
        for nc in self.inner.consumers.lock().iter() {
            let s = nc.consumer.stats();
            agg.total_msgs_received = agg
                .total_msgs_received
                .saturating_add(s.total_msgs_received);
            agg.total_bytes_received = agg
                .total_bytes_received
                .saturating_add(s.total_bytes_received);
            agg.total_acks_sent = agg.total_acks_sent.saturating_add(s.total_acks_sent);
            agg.total_acks_failed = agg.total_acks_failed.saturating_add(s.total_acks_failed);
            agg.total_msgs_dead_lettered = agg
                .total_msgs_dead_lettered
                .saturating_add(s.total_msgs_dead_lettered);
            agg.total_chunked_msgs_received = agg
                .total_chunked_msgs_received
                .saturating_add(s.total_chunked_msgs_received);
        }
        agg
    }

    /// Sum of buffered messages across every child consumer's receiver queue. Mirrors
    /// Java `Consumer#getNumMessagesInQueue` aggregated over partitions/topics.
    #[must_use]
    pub fn available_in_queue(&self) -> usize {
        self.inner
            .consumers
            .lock()
            .iter()
            .map(|c| c.consumer.available_in_queue())
            .sum()
    }

    /// Sum of outstanding broker permits across every child consumer. Mirrors Java
    /// `ConsumerBase#getAvailablePermits` aggregated over partitions/topics.
    #[must_use]
    pub fn available_permits(&self) -> u32 {
        self.inner
            .consumers
            .lock()
            .iter()
            .map(|c| c.consumer.available_permits())
            .fold(0u32, u32::saturating_add)
    }

    /// `true` if any child consumer has received at least one message. Mirrors Java
    /// `Consumer#hasReceivedAnyMessage` at the multi-topic / partitioned scope.
    #[must_use]
    pub fn has_received_any_message(&self) -> bool {
        self.inner
            .consumers
            .lock()
            .iter()
            .any(|c| c.consumer.has_received_any_message())
    }

    /// `true` once every child consumer is closed. Mirrors Java `Consumer#isClosed` at the
    /// multi-topic / partitioned scope.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        let guard = self.inner.consumers.lock();
        guard.iter().all(|c| c.consumer.is_closed())
    }

    /// Pause every child consumer. Mirrors Java `Consumer#pause` at the multi-topic scope.
    pub fn pause(&self) {
        for nc in self.inner.consumers.lock().iter() {
            nc.consumer.pause();
        }
    }

    /// Resume every child consumer.
    pub fn resume(&self) {
        for nc in self.inner.consumers.lock().iter() {
            nc.consumer.resume();
        }
    }

    /// `true` once every child consumer has reached end-of-topic. Mirrors Java
    /// `Consumer#hasReachedEndOfTopic` at the multi-topic scope.
    #[must_use]
    pub fn has_reached_end_of_topic(&self) -> bool {
        let guard = self.inner.consumers.lock();
        !guard.is_empty() && guard.iter().all(|c| c.consumer.has_reached_end_of_topic())
    }

    /// Close every underlying consumer. Returns the first error encountered; the rest are
    /// dropped (every child still gets a chance to close).
    pub async fn close(self) -> Result<(), PulsarError> {
        let inner = match Arc::try_unwrap(self.inner) {
            Ok(i) => i,
            Err(arc) => {
                // Clones outlive us; nothing safe to close concurrently.
                drop(arc);
                return Ok(());
            }
        };
        let consumers = inner.consumers.into_inner();
        let mut first_err: Result<(), PulsarError> = Ok(());
        for nc in consumers {
            if let Err(e) = nc.consumer.close().await {
                if first_err.is_ok() {
                    first_err = Err(PulsarError::Client(e));
                }
            }
        }
        first_err
    }

    /// Unsubscribe every child subscription. Mirrors Java `Consumer#unsubscribe` at the
    /// multi-topic / partitioned scope. Returns the first error encountered; the rest are
    /// dropped (every child still gets a chance to issue its unsubscribe).
    pub async fn unsubscribe(&self, force: bool) -> Result<(), PulsarError> {
        // Snapshot under the lock and release before awaiting — never hold the mutex
        // across an `.await`.
        let snapshot: Vec<NamedConsumer> = { self.inner.consumers.lock().clone() };
        let mut first_err: Result<(), PulsarError> = Ok(());
        for nc in &snapshot {
            if let Err(e) = nc.consumer.unsubscribe(force).await {
                if first_err.is_ok() {
                    first_err = Err(PulsarError::Client(e));
                }
            }
        }
        first_err
    }

    /// Seek every child consumer to the given publish-time deadline. Mirrors Java
    /// `Consumer#seek(long)` at the multi-topic scope.
    pub async fn seek_to_timestamp(&self, publish_time_ms: u64) -> Result<(), PulsarError> {
        let snapshot: Vec<NamedConsumer> = { self.inner.consumers.lock().clone() };
        let mut first_err: Result<(), PulsarError> = Ok(());
        for nc in &snapshot {
            if let Err(e) = nc.consumer.seek_to_timestamp(publish_time_ms).await {
                if first_err.is_ok() {
                    first_err = Err(PulsarError::Client(e));
                }
            }
        }
        first_err
    }

    /// Seek every child consumer to the earliest message. Mirrors Java
    /// `Consumer#seek(MessageId.earliest)` at the multi-topic scope.
    pub async fn seek_to_earliest(&self) -> Result<(), PulsarError> {
        let snapshot: Vec<NamedConsumer> = { self.inner.consumers.lock().clone() };
        let mut first_err: Result<(), PulsarError> = Ok(());
        for nc in &snapshot {
            if let Err(e) = nc.consumer.seek_to_earliest().await {
                if first_err.is_ok() {
                    first_err = Err(PulsarError::Client(e));
                }
            }
        }
        first_err
    }

    /// Seek every child consumer to the latest (head) position. Mirrors Java
    /// `Consumer#seek(MessageId.latest)` at the multi-topic scope.
    pub async fn seek_to_latest(&self) -> Result<(), PulsarError> {
        let snapshot: Vec<NamedConsumer> = { self.inner.consumers.lock().clone() };
        let mut first_err: Result<(), PulsarError> = Ok(());
        for nc in &snapshot {
            if let Err(e) = nc.consumer.seek_to_latest().await {
                if first_err.is_ok() {
                    first_err = Err(PulsarError::Client(e));
                }
            }
        }
        first_err
    }

    /// Seek every child consumer to a per-topic target computed by `f`. Mirrors Java's
    /// `Consumer#seek(Function<String, Object>)` (where the function returns either a
    /// `MessageId` or a `Long` publish-time millis-since-epoch).
    ///
    /// `f` is invoked synchronously per child, in the order supplied to the builder, with
    /// the child's topic name (matching what `topics()` returns — for a
    /// [`crate::PartitionedConsumer`] this is `<topic>-partition-N`). The returned
    /// [`SeekTarget`] is then dispatched to the appropriate per-topic seek primitive.
    ///
    /// All children are attempted even if one fails; the first error encountered is
    /// returned and subsequent errors are dropped (every child still gets a chance to
    /// issue its seek). This matches the existing [`Self::seek_to_timestamp`] semantics.
    pub async fn seek_per_partition<F>(&self, mut f: F) -> Result<(), PulsarError>
    where
        F: FnMut(&str) -> SeekTarget,
    {
        let snapshot: Vec<NamedConsumer> = { self.inner.consumers.lock().clone() };
        let mut first_err: Result<(), PulsarError> = Ok(());
        for nc in &snapshot {
            let target = f(nc.topic.as_str());
            let res = match target {
                SeekTarget::MessageId(id) => nc.consumer.seek_to_message(id).await,
                SeekTarget::PublishTimeMs(ts) => nc.consumer.seek_to_timestamp(ts).await,
            };
            if let Err(e) = res
                && first_err.is_ok()
            {
                first_err = Err(PulsarError::Client(e));
            }
        }
        first_err
    }

    /// Ask the broker for each topic's last-published message id. Returns one `(topic, id)`
    /// per child consumer, in the order they appear in the current consumer set. Mirrors
    /// Java `Consumer#getLastMessageIds` for partitioned/multi-topic consumers.
    pub async fn last_message_ids(&self) -> Result<Vec<(String, MessageId)>, PulsarError> {
        let snapshot: Vec<NamedConsumer> = { self.inner.consumers.lock().clone() };
        let mut out = Vec::with_capacity(snapshot.len());
        for nc in &snapshot {
            let id = nc
                .consumer
                .last_message_id()
                .await
                .map_err(PulsarError::Client)?;
            out.push((nc.topic.clone(), id));
        }
        Ok(out)
    }

    fn lookup(&self, topic: &str) -> Result<Consumer, String> {
        self.inner
            .consumers
            .lock()
            .iter()
            .find(|c| c.topic == topic)
            .map(|c| c.consumer.clone())
            .ok_or_else(|| format!("unknown topic {topic} on multi-consumer"))
    }

    /// Returns `true` if a background partition-watcher was spawned for this
    /// consumer (i.e.
    /// [`MultiTopicsConsumerBuilder::auto_update_partitions_interval`] was set on
    /// the builder, or the surface was opened as a
    /// [`crate::PartitionedConsumer`] with
    /// [`crate::PartitionedConsumerBuilder::auto_update_partitions_interval`]).
    #[must_use]
    pub fn has_auto_update_partitions(&self) -> bool {
        self.inner.auto_update.is_some()
    }

    /// Most recent partition count observed by the background partition watcher.
    /// `None` when no watcher was configured.
    #[must_use]
    pub fn observed_partitions(&self) -> Option<u32> {
        self.inner
            .auto_update
            .as_ref()
            .map(|t| *t.observed_partitions.lock())
    }

    /// Monotonic count of partition-change events observed by the background
    /// watcher. Returns `None` when no watcher was configured.
    #[must_use]
    pub fn partition_change_count(&self) -> Option<u64> {
        self.inner
            .auto_update
            .as_ref()
            .map(|t| *t.change_count.lock())
    }

    /// `Arc<Notify>` signalled by the background partition-watcher on every timer
    /// tick and on every observed partition-count change driven by
    /// [`Self::refresh_partitions`]. Returns `None` when no watcher was
    /// configured. Callers may `await` `notified()` on the returned handle to
    /// react to ticks without polling [`Self::partition_change_count`].
    #[must_use]
    pub fn partitions_changed_notify(&self) -> Option<Arc<Notify>> {
        self.inner.auto_update.as_ref().map(|t| t.changed.clone())
    }

    /// Query the broker for the current partition count of the topic this
    /// consumer was opened against (the base topic, for a
    /// [`crate::PartitionedConsumer`]), and update [`Self::observed_partitions`] /
    /// [`Self::partition_change_count`] in place if the count differs from the
    /// last observation.
    ///
    /// This is the user-driven half of the
    /// [`MultiTopicsConsumerBuilder::auto_update_partitions_interval`] machinery.
    /// Returns the freshly-observed count on success, or `Ok(None)` if no watcher
    /// was configured.
    ///
    /// **Note**: this method only updates the observed count. It does *not*
    /// itself subscribe to new per-partition topics that show up after creation —
    /// the surface still subscribes to its initial set. Callers that need the
    /// expanded set should add the new per-partition topics via
    /// [`Self::add_topic`] in response to the signal.
    ///
    /// # Errors
    ///
    /// Surfaces [`PulsarError::Client`] when the broker metadata lookup fails.
    pub async fn refresh_partitions(
        &self,
        client: &PulsarClient,
    ) -> Result<Option<u32>, PulsarError> {
        let Some(task) = self.inner.auto_update.as_ref() else {
            return Ok(None);
        };
        let count = client.partitions_for_topic(&task.topic).await?;
        let mut observed = task.observed_partitions.lock();
        if *observed != count {
            *observed = count;
            drop(observed);
            let mut cnt = task.change_count.lock();
            *cnt = cnt.saturating_add(1);
            drop(cnt);
            task.changed.notify_waiters();
        }
        Ok(Some(count))
    }
}

impl Clone for MultiTopicsConsumer {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Builder for [`MultiTopicsConsumer`]. Mirrors `org.apache.pulsar.client.api.ConsumerBuilder`
/// at the multi-topic layer.
#[derive(Debug)]
pub struct MultiTopicsConsumerBuilder<'a> {
    client: &'a PulsarClient,
    topics: Vec<String>,
    subscription: Option<String>,
    sub_type: magnetar_proto::pb::command_subscribe::SubType,
    receiver_queue_size: usize,
    initial_position: magnetar_proto::pb::command_subscribe::InitialPosition,
    durable: bool,
    properties: Vec<(String, String)>,
    negative_ack_redelivery_delay: Option<std::time::Duration>,
    ack_timeout: Option<std::time::Duration>,
    ack_group_time: Option<std::time::Duration>,
    dlq_policy: Option<(u32, Option<String>)>,
    read_compacted: bool,
    priority_level: Option<i32>,
    subscription_properties: Vec<(String, String)>,
    key_shared: Option<magnetar_proto::KeySharedConfig>,
    replicate_subscription_state: Option<bool>,
    force_topic_creation: Option<bool>,
    start_message_rollback_duration_sec: Option<u64>,
    auto_update_partitions_interval: Option<Duration>,
    /// Base topic recorded for the partition-watcher when this builder is driven by
    /// [`crate::PartitionedConsumerBuilder`]. `None` for direct multi-topic use —
    /// callers there pass the explicit topic list, so there is no single base topic
    /// to watch.
    auto_update_base_topic: Option<String>,
}

impl<'a> MultiTopicsConsumerBuilder<'a> {
    pub(crate) fn new(client: &'a PulsarClient) -> Self {
        Self {
            client,
            topics: Vec::new(),
            subscription: None,
            sub_type: magnetar_proto::pb::command_subscribe::SubType::Exclusive,
            receiver_queue_size: 1000,
            initial_position: magnetar_proto::pb::command_subscribe::InitialPosition::Latest,
            durable: true,
            properties: Vec::new(),
            negative_ack_redelivery_delay: None,
            ack_timeout: None,
            ack_group_time: None,
            dlq_policy: None,
            read_compacted: false,
            priority_level: None,
            subscription_properties: Vec::new(),
            key_shared: None,
            replicate_subscription_state: None,
            force_topic_creation: None,
            start_message_rollback_duration_sec: None,
            auto_update_partitions_interval: None,
            auto_update_base_topic: None,
        }
    }

    /// Append a topic. Subscribing to the same topic twice yields two separate
    /// per-topic consumer sessions.
    #[must_use]
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.topics.push(topic.into());
        self
    }

    /// Append multiple topics from any iterable.
    #[must_use]
    pub fn topics(mut self, topics: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.topics.extend(topics.into_iter().map(Into::into));
        self
    }

    /// Required: set the subscription name.
    #[must_use]
    pub fn subscription(mut self, name: impl Into<String>) -> Self {
        self.subscription = Some(name.into());
        self
    }

    /// Set the subscription type.
    #[must_use]
    pub fn subscription_type(
        mut self,
        sub_type: magnetar_proto::pb::command_subscribe::SubType,
    ) -> Self {
        self.sub_type = sub_type;
        self
    }

    /// Set the receiver queue size per consumer.
    #[must_use]
    pub fn receiver_queue_size(mut self, size: usize) -> Self {
        self.receiver_queue_size = size;
        self
    }

    /// Set the initial position.
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

    /// Enable a background timer that signals every `interval`, intended to drive
    /// re-checks of the topic's partition count. Mirrors Java
    /// `ConsumerBuilder#autoUpdatePartitionsInterval`.
    ///
    /// The internal timer task signals
    /// [`MultiTopicsConsumer::partitions_changed_notify`] on every tick. Callers
    /// run [`MultiTopicsConsumer::refresh_partitions`] in response to the signal
    /// (or on their own cadence) to actually call
    /// [`PulsarClient::partitions_for_topic`].
    ///
    /// Default `None` — no timer is spawned. Pass a non-zero `Duration` to opt
    /// in. The timer is aborted when the [`MultiTopicsConsumer`] (and every
    /// clone) is dropped.
    ///
    /// Setting a zero `interval` is treated as "disable" — same as the default.
    ///
    /// **Note**: for direct multi-topic use, the watcher polls the *first* topic
    /// supplied to the builder (single watched topic is sufficient for the
    /// partitioned-consumer case which is the main use site).
    #[must_use]
    pub fn auto_update_partitions_interval(mut self, interval: Duration) -> Self {
        self.auto_update_partitions_interval = if interval.is_zero() {
            None
        } else {
            Some(interval)
        };
        self
    }

    /// Record the base topic the partition watcher should poll. Called by
    /// [`crate::PartitionedConsumerBuilder::subscribe`] so the watcher polls the
    /// base topic (e.g. `persistent://t`) rather than the first partition topic
    /// (`persistent://t-partition-0`). Crate-internal — direct multi-topic users
    /// don't need this knob; the watcher falls back to the first topic.
    #[must_use]
    pub(crate) fn auto_update_base_topic(mut self, topic: String) -> Self {
        self.auto_update_base_topic = Some(topic);
        self
    }

    /// Open every per-topic subscription concurrently. If any subscribe fails the others
    /// that already succeeded are torn down before the error is returned.
    pub async fn subscribe(self) -> Result<MultiTopicsConsumer, PulsarError> {
        let subscription = self
            .subscription
            .ok_or_else(|| PulsarError::Config("subscription name is required".to_owned()))?;
        if self.topics.is_empty() {
            return Err(PulsarError::Config(
                "at least one topic is required".to_owned(),
            ));
        }

        let template = ConsumerTemplate {
            subscription,
            sub_type: self.sub_type,
            receiver_queue_size: self.receiver_queue_size,
            initial_position: self.initial_position,
            durable: self.durable,
            properties: self.properties,
            negative_ack_redelivery_delay: self.negative_ack_redelivery_delay,
            ack_timeout: self.ack_timeout,
            ack_group_time: self.ack_group_time,
            dlq_policy: self.dlq_policy,
            read_compacted: self.read_compacted,
            priority_level: self.priority_level,
            subscription_properties: self.subscription_properties,
            key_shared: self.key_shared,
            replicate_subscription_state: self.replicate_subscription_state,
            force_topic_creation: self.force_topic_creation,
            start_message_rollback_duration_sec: self.start_message_rollback_duration_sec,
        };

        // Subscribe sequentially — the first failure short-circuits, and on failure we close
        // the consumers we already opened.
        let mut consumers: Vec<NamedConsumer> = Vec::with_capacity(self.topics.len());
        for topic in &self.topics {
            let builder = template.apply(self.client.consumer(topic.clone()));
            let result = builder.subscribe().await;
            match result {
                Ok(c) => consumers.push(NamedConsumer {
                    topic: topic.clone(),
                    consumer: c,
                }),
                Err(e) => {
                    // Best-effort teardown of previously-opened consumers.
                    for nc in consumers {
                        let _ = nc.consumer.close().await;
                    }
                    return Err(e);
                }
            }
        }

        // Spawn the partition-watcher timer iff the builder configured a non-zero
        // interval. The timer itself only emits ticks via `Notify`; callers drive
        // the actual `partitions_for_topic` call via
        // [`MultiTopicsConsumer::refresh_partitions`].
        let auto_update = self.auto_update_partitions_interval.map(|interval| {
            let watched_topic = self
                .auto_update_base_topic
                .unwrap_or_else(|| self.topics[0].clone());
            // We do not have an initial partition count for the direct multi-topic
            // case (each topic was passed explicitly); seed with 0 so the first
            // refresh always logs a change.
            spawn_auto_update_task(watched_topic, interval, 0)
        });

        Ok(MultiTopicsConsumer {
            inner: Arc::new(Inner {
                consumers: Mutex::new(consumers),
                cursor: Mutex::new(0),
                template,
                auto_update,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use magnetar_proto::MessageId;

    use super::*;
    use crate::SeekTarget;

    fn empty_template() -> ConsumerTemplate {
        ConsumerTemplate {
            subscription: "sub".to_owned(),
            sub_type: magnetar_proto::pb::command_subscribe::SubType::Exclusive,
            receiver_queue_size: 1000,
            initial_position: magnetar_proto::pb::command_subscribe::InitialPosition::Latest,
            durable: true,
            properties: Vec::new(),
            negative_ack_redelivery_delay: None,
            ack_timeout: None,
            ack_group_time: None,
            dlq_policy: None,
            read_compacted: false,
            priority_level: None,
            subscription_properties: Vec::new(),
            key_shared: None,
            replicate_subscription_state: None,
            force_topic_creation: None,
            start_message_rollback_duration_sec: None,
        }
    }

    /// Mutex round-trip: build an `Inner` with no consumers and verify the dynamic-membership
    /// helpers (`topics`, `len`, `is_empty`, `lookup`) operate consistently against the
    /// `Mutex<Vec<NamedConsumer>>` and that the template-stored subscription name is
    /// reachable via [`MultiTopicsConsumer::subscription`] even with an empty set.
    #[test]
    fn empty_inner_is_consistent() {
        let inner = Arc::new(Inner {
            consumers: Mutex::new(Vec::new()),
            cursor: Mutex::new(0),
            template: empty_template(),
            auto_update: None,
        });
        let consumer = MultiTopicsConsumer {
            inner: inner.clone(),
        };
        assert_eq!(consumer.len(), 0);
        assert!(consumer.is_empty());
        assert!(consumer.topics().is_empty());
        assert_eq!(consumer.subscription(), "sub");
        let lookup = consumer.lookup("missing");
        assert!(lookup.is_err());
        // Cloning the handle shares the same Inner.
        let cloned = consumer.clone();
        assert!(cloned.is_empty());
        assert_eq!(cloned.subscription(), "sub");
    }

    #[test]
    fn template_clone_preserves_settings() {
        let mut t = empty_template();
        t.properties.push(("k".to_owned(), "v".to_owned()));
        t.subscription_properties
            .push(("sk".to_owned(), "sv".to_owned()));
        let clone = t.clone();
        assert_eq!(clone.subscription, "sub");
        assert_eq!(clone.properties, vec![("k".to_owned(), "v".to_owned())]);
        assert_eq!(
            clone.subscription_properties,
            vec![("sk".to_owned(), "sv".to_owned())]
        );
    }

    /// Mirror of the dispatch arm inside [`super::MultiTopicsConsumer::seek_per_partition`].
    /// Records the routing decision per topic instead of issuing a real seek so the routing
    /// logic can be exercised without spinning up a broker.
    fn dispatch<F>(topics: &[&str], mut f: F) -> Vec<(String, DispatchKind)>
    where
        F: FnMut(&str) -> SeekTarget,
    {
        topics
            .iter()
            .map(|t| {
                let kind = match f(t) {
                    SeekTarget::MessageId(id) => DispatchKind::MessageId(id),
                    SeekTarget::PublishTimeMs(ts) => DispatchKind::PublishTimeMs(ts),
                };
                ((*t).to_owned(), kind)
            })
            .collect()
    }

    #[derive(Debug, PartialEq, Eq)]
    enum DispatchKind {
        MessageId(MessageId),
        PublishTimeMs(u64),
    }

    #[test]
    fn seek_per_partition_routes_each_topic_via_closure() {
        let topics = [
            "persistent://public/default/orders-partition-0",
            "persistent://public/default/orders-partition-1",
            "persistent://public/default/orders-partition-2",
        ];

        // Track what topics the closure was called with — mirrors Java's
        // Function<String, Object> semantics where each partition gets its own decision.
        let seen = RefCell::new(Vec::<String>::new());
        let mid = MessageId::EARLIEST;
        let decisions = dispatch(&topics, |t| {
            seen.borrow_mut().push(t.to_owned());
            if t.ends_with("-partition-1") {
                SeekTarget::PublishTimeMs(1_700_000_000_000)
            } else {
                SeekTarget::MessageId(mid)
            }
        });

        // Closure was called exactly once per topic, in builder order.
        assert_eq!(seen.borrow().len(), 3);
        assert_eq!(seen.borrow()[0], topics[0]);
        assert_eq!(seen.borrow()[1], topics[1]);
        assert_eq!(seen.borrow()[2], topics[2]);

        // Routing: partition-0 / partition-2 -> MessageId seek; partition-1 -> timestamp seek.
        assert_eq!(decisions.len(), 3);
        assert_eq!(decisions[0].1, DispatchKind::MessageId(mid));
        assert_eq!(
            decisions[1].1,
            DispatchKind::PublishTimeMs(1_700_000_000_000)
        );
        assert_eq!(decisions[2].1, DispatchKind::MessageId(mid));
    }

    #[test]
    fn seek_target_enum_variants_are_constructible() {
        let by_id = SeekTarget::MessageId(MessageId::LATEST);
        let by_ts = SeekTarget::PublishTimeMs(42);
        // PartialEq + Copy: derived impls round-trip without surprises.
        assert_eq!(by_id, SeekTarget::MessageId(MessageId::LATEST));
        assert_ne!(by_id, by_ts);
    }
}
