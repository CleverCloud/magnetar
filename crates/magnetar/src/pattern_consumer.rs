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
//!   set; the builder subscribes to each.
//! - Streaming deltas: each [`PatternConsumer::update`] call drains any pending `TopicListChanged`
//!   deltas from the connection buffer and reconciles the consumer set: newly-added topics are
//!   subscribed, removed topics are closed and dropped.
//! - Callers drive reconciliation explicitly (no spawned task) — call `update()` from a timer or
//!   before/after blocking work.
//!
//! Cancel safety
//! -------------
//! [`PatternConsumer::receive`] is cancel-safe in the same sense as
//! [`crate::MultiTopicsConsumer::receive`]: dropping the future without polling it to
//! completion leaves un-popped messages in their per-topic consumer queues.

use std::sync::Arc;

use futures_util::FutureExt;
use futures_util::future::select_all;
use magnetar_proto::{IncomingMessage, MessageId};
use magnetar_runtime_tokio::Consumer;
use parking_lot::Mutex;

use crate::PulsarClient;
use crate::client::PulsarError;

/// Regex-pattern consumer. Holds one [`Consumer`] per matching topic and reconciles the set
/// against PIP-145 deltas on `update()`.
#[derive(Debug)]
pub struct PatternConsumer {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    /// Active consumer set, keyed by topic name.
    consumers: Mutex<Vec<NamedConsumer>>,
    /// Namespace + pattern recorded for diagnostics and for re-snapshot operations.
    namespace: String,
    pattern: String,
    /// Template for subscribing newly-discovered topics. Captures every
    /// [`crate::ConsumerBuilder`] knob the user set on the original
    /// [`PatternConsumerBuilder`].
    template: ConsumerTemplate,
}

/// Frozen [`crate::ConsumerBuilder`] template propagated to every per-topic child. Stored
/// inside [`Inner`] so [`PatternConsumer::update`] can subscribe newly-discovered topics
/// with the same configuration as the initial snapshot.
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
struct NamedConsumer {
    topic: String,
    consumer: Consumer,
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

impl PatternConsumer {
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

    /// Drain pending PIP-145 `TopicListChanged` deltas from the underlying connection and
    /// reconcile the consumer set: newly-added topics are subscribed, removed topics are
    /// closed and dropped.
    ///
    /// Idempotent; returns the count of additions and removals applied during this call.
    /// Mirrors Java's internal `PatternMultiTopicsConsumerImpl#recheckTopics` cycle.
    ///
    /// # Errors
    ///
    /// Returns the first [`PulsarError`] encountered while subscribing a new topic; topics
    /// successfully reconciled before the error remain subscribed.
    pub async fn update(&self, client: &PulsarClient) -> Result<ReconcileReport, PulsarError> {
        let runtime = client.runtime_client();
        let mut report = ReconcileReport::default();
        // Drain every pending delta synchronously, then apply the reconciliation.
        let mut added: Vec<String> = Vec::new();
        let mut removed: Vec<String> = Vec::new();
        while let Some(change) = runtime.poll_topic_list_change() {
            added.extend(change.added);
            removed.extend(change.removed);
        }
        if added.is_empty() && removed.is_empty() {
            return Ok(report);
        }
        // Removals first — close the consumer, drop from the set.
        if !removed.is_empty() {
            let drained: Vec<NamedConsumer> = {
                let mut guard = self.inner.consumers.lock();
                let mut drained = Vec::new();
                guard.retain(|nc| {
                    if removed.iter().any(|t| t == &nc.topic) {
                        drained.push(nc.clone());
                        false
                    } else {
                        true
                    }
                });
                drained
            };
            for nc in drained {
                let _ = nc.consumer.close().await;
                report.removed += 1;
            }
        }
        // Additions — subscribe each, skipping topics already in the set (the broker can
        // resend a topic if multiple watch responses overlap during reconnects).
        if !added.is_empty() {
            for topic in added {
                let already_subscribed = self
                    .inner
                    .consumers
                    .lock()
                    .iter()
                    .any(|nc| nc.topic == topic);
                if already_subscribed {
                    continue;
                }
                let builder = self.inner.template.apply(client.consumer(topic.clone()));
                let consumer = builder.subscribe().await?;
                self.inner
                    .consumers
                    .lock()
                    .push(NamedConsumer { topic, consumer });
                report.added += 1;
            }
        }
        Ok(report)
    }

    /// Receive the next message across any currently-subscribed topic. The future is
    /// cancel-safe: dropping it leaves un-popped messages in their respective per-consumer
    /// queues.
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Config`] if the consumer set is empty. Otherwise propagates the
    /// first per-topic receive error.
    pub async fn receive(&self) -> Result<PatternMessage, PulsarError> {
        // Snapshot the consumer set under the lock, then release before awaiting — holding the
        // mutex across an await would serialise receive against update.
        let snapshot: Vec<NamedConsumer> = { self.inner.consumers.lock().clone() };
        if snapshot.is_empty() {
            return Err(PulsarError::Config(
                "no topics matched the pattern — nothing to receive".to_owned(),
            ));
        }
        if snapshot.len() == 1 {
            let nc = &snapshot[0];
            let msg = nc.consumer.receive().await?;
            return Ok(PatternMessage {
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
        Ok(PatternMessage { topic, message })
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
        consumer.ack(message_id).await.map_err(PulsarError::Client)
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
            .map_err(PulsarError::Client)
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

    fn lookup(&self, topic: &str) -> Result<Consumer, String> {
        self.inner
            .consumers
            .lock()
            .iter()
            .find(|c| c.topic == topic)
            .map(|c| c.consumer.clone())
            .ok_or_else(|| format!("unknown topic {topic} on pattern consumer"))
    }
}

impl Clone for PatternConsumer {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Builder for [`PatternConsumer`]. Mirrors Java's
/// `PulsarClient#newConsumer().topicsPattern(...)`.
#[derive(Debug)]
pub struct PatternConsumerBuilder<'a> {
    client: &'a PulsarClient,
    namespace: Option<String>,
    pattern: Option<String>,
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
}

impl<'a> PatternConsumerBuilder<'a> {
    pub(crate) fn new(client: &'a PulsarClient) -> Self {
        Self {
            client,
            namespace: None,
            pattern: None,
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
        }
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

    /// Take an initial snapshot of matching topics, subscribe to each, and return the
    /// [`PatternConsumer`]. Call [`PatternConsumer::update`] periodically to reconcile
    /// against newly-emitted PIP-145 deltas — newly-discovered topics inherit every knob
    /// configured here.
    ///
    /// # Errors
    ///
    /// Returns [`PulsarError::Config`] if a required field is missing, [`PulsarError::Client`]
    /// if the broker refuses the watch, or the first per-topic subscribe error if a topic in
    /// the snapshot cannot be opened (already-opened topics are torn down before the error).
    pub async fn subscribe(self) -> Result<PatternConsumer, PulsarError> {
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

        let topics = self
            .client
            .topic_list_snapshot(&namespace, &pattern)
            .await?;

        let mut opened: Vec<NamedConsumer> = Vec::with_capacity(topics.len());
        for topic in &topics {
            let builder = template.apply(self.client.consumer(topic.clone()));
            match builder.subscribe().await {
                Ok(c) => opened.push(NamedConsumer {
                    topic: topic.clone(),
                    consumer: c,
                }),
                Err(e) => {
                    for nc in opened {
                        let _ = nc.consumer.close().await;
                    }
                    return Err(e);
                }
            }
        }

        Ok(PatternConsumer {
            inner: Arc::new(Inner {
                consumers: Mutex::new(opened),
                namespace,
                pattern,
                template,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ReconcileReport;

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
}
