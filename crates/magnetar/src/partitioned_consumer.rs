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
        self.client
            .multi_topics_consumer()
            .topics(topics)
            .subscription(subscription)
            .subscription_type(self.sub_type)
            .receiver_queue_size(self.receiver_queue_size)
            .initial_position(self.initial_position)
            .durable(self.durable)
            .subscribe()
            .await
    }
}
