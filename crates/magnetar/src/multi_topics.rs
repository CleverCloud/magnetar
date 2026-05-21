// SPDX-License-Identifier: Apache-2.0

//! Multi-topics consumer — subscribes to N topics and merges their delivery streams.
//!
//! Mirrors Java's `MultiTopicsConsumerImpl`. The consumer is a thin coordinator over a
//! `Vec<Consumer>` with `receive()` returning the first message ready across all underlying
//! consumers. Cancelling the future leaves un-popped messages in their respective consumer
//! queues — see the `cancel-safe` discussion in [`magnetar_runtime_tokio::Consumer::receive`].
//!
//! No regex / pattern subscription (yet); callers pass an explicit topic list. Regex /
//! pattern support layers on top via a broker-side topic-list-watcher (PIP-145), which is
//! exposed by [`magnetar_proto::Connection`] but not wired through this facade — the
//! follow-up patch will subscribe via [`Connection::watch_topics`].

use std::sync::Arc;

use futures_util::FutureExt;
use futures_util::future::select_all;
use magnetar_proto::{IncomingMessage, MessageId};
use magnetar_runtime_tokio::Consumer;
use parking_lot::Mutex;

use crate::PulsarClient;
use crate::client::PulsarError;

/// Multi-topics consumer. Each contained [`Consumer`] subscribes to one topic; `receive()`
/// returns the next message across the whole set.
#[derive(Debug)]
pub struct MultiTopicsConsumer {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    consumers: Vec<NamedConsumer>,
    /// Round-robin cursor used by `receive_round_robin` to give every topic an opportunity
    /// to make progress. Wrapped in a Mutex because [`MultiTopicsConsumer`] is `&self` —
    /// cloning the handle should not require mutable access.
    cursor: Mutex<usize>,
}

#[derive(Debug)]
struct NamedConsumer {
    topic: String,
    consumer: Consumer,
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
    /// Topics this consumer is subscribed to, in the order supplied to the builder.
    #[must_use]
    pub fn topics(&self) -> Vec<&str> {
        self.inner
            .consumers
            .iter()
            .map(|c| c.topic.as_str())
            .collect()
    }

    /// Number of underlying consumers (one per topic).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.consumers.len()
    }

    /// `true` if the consumer was built with an empty topic list.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.consumers.is_empty()
    }

    /// Shared subscription name across every per-topic child. Returns an empty string if
    /// no child consumer exists (empty topic list — should not happen post-builder).
    /// Mirrors Java `Consumer#getSubscription` at the multi-topic / partitioned scope.
    #[must_use]
    pub fn subscription(&self) -> String {
        self.inner
            .consumers
            .first()
            .map(|c| c.consumer.subscription())
            .unwrap_or_default()
    }

    /// Negatively acknowledge a message. The caller supplies the topic the message came
    /// from (returned alongside the message in [`MultiTopicsMessage::topic`]) so the nack
    /// goes to the correct per-topic consumer.
    pub fn negative_ack(&self, topic: &str, message_id: MessageId) -> Result<(), PulsarError> {
        let consumer = self
            .inner
            .consumers
            .iter()
            .find(|c| c.topic == topic)
            .ok_or_else(|| {
                PulsarError::Config(format!("negative_ack for unknown topic {topic}"))
            })?;
        consumer.consumer.negative_ack(message_id);
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
            .inner
            .consumers
            .iter()
            .find(|c| c.topic == topic)
            .ok_or_else(|| {
                PulsarError::Config(format!("negative_ack_with_delay for unknown topic {topic}"))
            })?;
        consumer.consumer.negative_ack_with_delay(message_id, delay);
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
            .inner
            .consumers
            .iter()
            .find(|c| c.topic == topic)
            .ok_or_else(|| {
                PulsarError::Config(format!("ack_cumulative for unknown topic {topic}"))
            })?;
        consumer
            .consumer
            .ack_cumulative(message_id)
            .await
            .map_err(PulsarError::Client)
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
            .inner
            .consumers
            .iter()
            .find(|c| c.topic == topic)
            .ok_or_else(|| {
                PulsarError::Config(format!("reconsume_later for unknown topic {topic}"))
            })?;
        consumer
            .consumer
            .reconsume_later(retry_producer, msg, delay)
            .await
            .map_err(PulsarError::Client)
    }

    /// Tell the broker to redeliver every unacked message across every child consumer.
    /// Mirrors Java `Consumer#redeliverUnacknowledgedMessages` at the multi-topic scope.
    pub fn redeliver_unacked(&self) {
        for nc in &self.inner.consumers {
            nc.consumer.redeliver_unacked();
        }
    }

    /// Receive the next message across any subscribed topic. The future is cancel-safe:
    /// dropping it without polling to completion leaves all unpopped messages in their
    /// respective per-consumer queues.
    pub async fn receive(&self) -> Result<MultiTopicsMessage, PulsarError> {
        if self.inner.consumers.is_empty() {
            return Err(PulsarError::Config(
                "no topics subscribed to receive from".to_owned(),
            ));
        }
        if self.inner.consumers.len() == 1 {
            let c = &self.inner.consumers[0];
            let msg = c.consumer.receive().await?;
            return Ok(MultiTopicsMessage {
                topic: c.topic.clone(),
                message: msg,
            });
        }

        let futures: Vec<_> = self
            .inner
            .consumers
            .iter()
            .map(|nc| nc.consumer.receive().boxed())
            .collect();
        let (result, idx, _rest) = select_all(futures).await;
        let topic = self.inner.consumers[idx].topic.clone();
        let message = result?;
        *self.inner.cursor.lock() = (idx + 1) % self.inner.consumers.len();
        Ok(MultiTopicsMessage { topic, message })
    }

    /// Acknowledge a message. The caller supplies the topic the message came from (returned
    /// alongside the message in [`MultiTopicsMessage::topic`]) so we can route the ack to
    /// the correct per-topic consumer.
    pub async fn ack(&self, topic: &str, message_id: MessageId) -> Result<(), PulsarError> {
        let consumer = self
            .inner
            .consumers
            .iter()
            .find(|c| c.topic == topic)
            .ok_or_else(|| {
                PulsarError::Config(format!("ack for unknown topic {topic} on multi-consumer"))
            })?;
        consumer
            .consumer
            .ack(message_id)
            .await
            .map_err(PulsarError::Client)
    }

    /// `true` while every child consumer reports the underlying connection is up.
    /// Mirrors Java `Consumer#isConnected` at the multi-topic / partitioned scope.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.inner
            .consumers
            .iter()
            .all(|c| c.consumer.is_connected())
    }

    /// Earliest disconnect wall-clock across all child consumers. `None` if no child has
    /// ever disconnected.
    #[must_use]
    pub fn last_disconnected_timestamp(&self) -> Option<std::time::SystemTime> {
        self.inner
            .consumers
            .iter()
            .filter_map(|c| c.consumer.last_disconnected_timestamp())
            .min()
    }

    /// Aggregate cumulative stats across all child consumers. Sums the totals; useful
    /// for monitoring fan-in throughput on the multi-topic / partitioned scope.
    #[must_use]
    pub fn aggregate_stats(&self) -> magnetar_proto::ConsumerStats {
        let mut agg = magnetar_proto::ConsumerStats::default();
        for nc in &self.inner.consumers {
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
        }
        agg
    }

    /// Sum of buffered messages across every child consumer's receiver queue. Mirrors
    /// Java `Consumer#getNumMessagesInQueue` aggregated over partitions/topics.
    #[must_use]
    pub fn available_in_queue(&self) -> usize {
        self.inner
            .consumers
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
            .iter()
            .any(|c| c.consumer.has_received_any_message())
    }

    /// `true` once every child consumer is closed. Mirrors Java `Consumer#isClosed` at the
    /// multi-topic / partitioned scope.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.consumers.iter().all(|c| c.consumer.is_closed())
    }

    /// Pause every child consumer. Mirrors Java `Consumer#pause` at the multi-topic scope.
    pub fn pause(&self) {
        for nc in &self.inner.consumers {
            nc.consumer.pause();
        }
    }

    /// Resume every child consumer.
    pub fn resume(&self) {
        for nc in &self.inner.consumers {
            nc.consumer.resume();
        }
    }

    /// `true` once every child consumer has reached end-of-topic. Mirrors Java
    /// `Consumer#hasReachedEndOfTopic` at the multi-topic scope.
    #[must_use]
    pub fn has_reached_end_of_topic(&self) -> bool {
        self.inner
            .consumers
            .iter()
            .all(|c| c.consumer.has_reached_end_of_topic())
    }

    /// Close every underlying consumer concurrently. Returns the first error encountered;
    /// the rest are dropped.
    pub async fn close(self) -> Result<(), PulsarError> {
        let inner = match Arc::try_unwrap(self.inner) {
            Ok(i) => i,
            Err(arc) => {
                // Clones outlive us; nothing safe to close concurrently.
                drop(arc);
                return Ok(());
            }
        };
        let mut first_err: Result<(), PulsarError> = Ok(());
        for nc in inner.consumers {
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
        let mut first_err: Result<(), PulsarError> = Ok(());
        for nc in &self.inner.consumers {
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
        let mut first_err: Result<(), PulsarError> = Ok(());
        for nc in &self.inner.consumers {
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
        let mut first_err: Result<(), PulsarError> = Ok(());
        for nc in &self.inner.consumers {
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
        let mut first_err: Result<(), PulsarError> = Ok(());
        for nc in &self.inner.consumers {
            if let Err(e) = nc.consumer.seek_to_latest().await {
                if first_err.is_ok() {
                    first_err = Err(PulsarError::Client(e));
                }
            }
        }
        first_err
    }

    /// Ask the broker for each topic's last-published message id. Returns one `(topic, id)`
    /// per child consumer, in the order supplied to the builder. Mirrors Java
    /// `Consumer#getLastMessageIds` for partitioned/multi-topic consumers.
    pub async fn last_message_ids(&self) -> Result<Vec<(String, MessageId)>, PulsarError> {
        let mut out = Vec::with_capacity(self.inner.consumers.len());
        for nc in &self.inner.consumers {
            let id = nc
                .consumer
                .last_message_id()
                .await
                .map_err(PulsarError::Client)?;
            out.push((nc.topic.clone(), id));
        }
        Ok(out)
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
    dlq_policy: Option<(u32, Option<String>)>,
    read_compacted: bool,
    priority_level: Option<i32>,
    subscription_properties: Vec<(String, String)>,
    key_shared: Option<magnetar_proto::KeySharedConfig>,
    replicate_subscription_state: Option<bool>,
    force_topic_creation: Option<bool>,
    start_message_rollback_duration_sec: Option<u64>,
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

        // Subscribe sequentially — the first failure short-circuits, and on failure we close
        // the consumers we already opened.
        let mut consumers: Vec<NamedConsumer> = Vec::with_capacity(self.topics.len());
        for topic in &self.topics {
            let mut builder = self
                .client
                .consumer(topic.clone())
                .subscription(subscription.clone())
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

        Ok(MultiTopicsConsumer {
            inner: Arc::new(Inner {
                consumers,
                cursor: Mutex::new(0),
            }),
        })
    }
}
