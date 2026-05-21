// SPDX-License-Identifier: Apache-2.0

//! Partition-aware consumer.
//!
//! Mirrors Java's `PartitionedConsumerImpl`. On `subscribe()` the builder queries the broker
//! for the topic's partition count via `CommandPartitionedTopicMetadata`. If the count is
//! `> 0` it opens one child consumer per partition (`<topic>-partition-N`) and merges
//! their delivery streams under a single subscription name. Otherwise it subscribes to the
//! original topic directly.
//!
//! Under the hood this is a [`crate::MultiTopicsConsumer`] with broker-discovered topics, so
//! the receive-side semantics (cancel-safe `select_all`, per-topic ack routing) are shared
//! with the manually-listed multi-topics case.

use crate::client::PulsarError;
use crate::{MultiTopicsConsumer, PulsarClient};

/// Partition-aware consumer. Effectively a [`crate::MultiTopicsConsumer`] whose topic list
/// was auto-discovered from a partitioned topic.
pub type PartitionedConsumer = MultiTopicsConsumer;

/// Builder for a partition-aware consumer.
pub struct PartitionedConsumerBuilder<'a> {
    client: &'a PulsarClient,
    topic: String,
    subscription: Option<String>,
    sub_type: magnetar_proto::pb::command_subscribe::SubType,
    receiver_queue_size: usize,
    initial_position: magnetar_proto::pb::command_subscribe::InitialPosition,
    durable: bool,
    priority_level: Option<i32>,
    properties: Vec<(String, String)>,
    subscription_properties: Vec<(String, String)>,
    read_compacted: bool,
    negative_ack_redelivery_delay: Option<std::time::Duration>,
    ack_timeout: Option<std::time::Duration>,
    dlq_policy: Option<(u32, Option<String>)>,
    key_shared: Option<magnetar_proto::KeySharedConfig>,
    replicate_subscription_state: Option<bool>,
    force_topic_creation: Option<bool>,
    start_message_rollback_duration_sec: Option<u64>,
}

impl std::fmt::Debug for PartitionedConsumerBuilder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionedConsumerBuilder")
            .field("topic", &self.topic)
            .field("subscription", &self.subscription)
            .field("sub_type", &self.sub_type)
            .finish()
    }
}

impl<'a> PartitionedConsumerBuilder<'a> {
    pub(crate) fn new(client: &'a PulsarClient, topic: String) -> Self {
        Self {
            client,
            topic,
            subscription: None,
            sub_type: magnetar_proto::pb::command_subscribe::SubType::Exclusive,
            receiver_queue_size: 1000,
            initial_position: magnetar_proto::pb::command_subscribe::InitialPosition::Latest,
            durable: true,
            priority_level: None,
            properties: Vec::new(),
            subscription_properties: Vec::new(),
            read_compacted: false,
            negative_ack_redelivery_delay: None,
            ack_timeout: None,
            dlq_policy: None,
            key_shared: None,
            replicate_subscription_state: None,
            force_topic_creation: None,
            start_message_rollback_duration_sec: None,
        }
    }

    /// Required: set the subscription name (shared across every per-partition child consumer).
    #[must_use]
    pub fn subscription(mut self, name: impl Into<String>) -> Self {
        self.subscription = Some(name.into());
        self
    }

    /// Set the subscription type for every per-partition child consumer.
    #[must_use]
    pub fn subscription_type(
        mut self,
        sub_type: magnetar_proto::pb::command_subscribe::SubType,
    ) -> Self {
        self.sub_type = sub_type;
        self
    }

    /// Per-partition receiver queue size.
    #[must_use]
    pub fn receiver_queue_size(mut self, size: usize) -> Self {
        self.receiver_queue_size = size;
        self
    }

    /// Initial position for every per-partition child consumer.
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

    /// Mirrors `ConsumerBuilder::priority_level`.
    #[must_use]
    pub fn priority_level(mut self, level: i32) -> Self {
        self.priority_level = Some(level);
        self
    }

    /// Mirrors `ConsumerBuilder::property`.
    #[must_use]
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.push((key.into(), value.into()));
        self
    }

    /// Mirrors `ConsumerBuilder::subscription_property`.
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

    /// Mirrors `ConsumerBuilder::read_compacted`.
    #[must_use]
    pub fn read_compacted(mut self, on: bool) -> Self {
        self.read_compacted = on;
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

    /// Query partition count, then open one per-partition consumer. If the broker reports
    /// `0` partitions the builder falls back to a single consumer on the original topic.
    pub async fn subscribe(self) -> Result<PartitionedConsumer, PulsarError> {
        let subscription = self
            .subscription
            .ok_or_else(|| PulsarError::Config("subscription name is required".to_owned()))?;
        let partitions = self
            .client
            .runtime_client()
            .partitioned_topic_metadata(&self.topic)
            .await?;
        let topics: Vec<String> = if partitions == 0 {
            vec![self.topic.clone()]
        } else {
            (0..partitions)
                .map(|i| format!("{}-partition-{}", self.topic, i))
                .collect()
        };
        let mut builder = self
            .client
            .multi_topics_consumer()
            .topics(topics)
            .subscription(subscription)
            .subscription_type(self.sub_type)
            .receiver_queue_size(self.receiver_queue_size)
            .initial_position(self.initial_position)
            .durable(self.durable)
            .read_compacted(self.read_compacted);
        if let Some(level) = self.priority_level {
            builder = builder.priority_level(level);
        }
        for (k, v) in self.properties {
            builder = builder.property(k, v);
        }
        for (k, v) in self.subscription_properties {
            builder = builder.subscription_property(k, v);
        }
        if let Some(d) = self.negative_ack_redelivery_delay {
            builder = builder.negative_ack_redelivery_delay(d);
        }
        if let Some(t) = self.ack_timeout {
            builder = builder.ack_timeout(t);
        }
        if let Some((max, topic_opt)) = self.dlq_policy {
            builder = builder.dead_letter_policy(max, topic_opt);
        }
        if let Some(cfg) = self.key_shared {
            builder = builder.key_shared_policy(cfg);
        }
        if let Some(on) = self.replicate_subscription_state {
            builder = builder.replicate_subscription_state(on);
        }
        if let Some(on) = self.force_topic_creation {
            builder = builder.force_topic_creation(on);
        }
        if let Some(sec) = self.start_message_rollback_duration_sec {
            builder = builder.start_message_rollback_duration(sec);
        }
        builder.subscribe().await
    }
}
