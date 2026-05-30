// SPDX-License-Identifier: Apache-2.0

//! Shared private [`ConsumerTemplate`] used by [`crate::MultiTopicsConsumer`] and
//! [`crate::PatternConsumer`] to propagate a frozen [`crate::ConsumerBuilder`]
//! configuration to each per-topic child consumer.
//!
//! Both surfaces capture the same superset of [`crate::ConsumerBuilder`] knobs
//! and replay them through [`Self::apply`] when subscribing newly-added
//! (multi-topics) or newly-discovered (pattern) topics. Keeping a single
//! definition here means the two call sites cannot drift out of sync as new
//! consumer knobs land.
//!
//! Private — no public API surface.

/// Frozen [`crate::ConsumerBuilder`] template propagated to every per-topic child.
#[derive(Debug, Clone)]
pub(crate) struct ConsumerTemplate {
    pub(crate) subscription: String,
    pub(crate) sub_type: magnetar_proto::pb::command_subscribe::SubType,
    pub(crate) receiver_queue_size: usize,
    pub(crate) initial_position: magnetar_proto::pb::command_subscribe::InitialPosition,
    pub(crate) durable: bool,
    pub(crate) properties: Vec<(String, String)>,
    pub(crate) negative_ack_redelivery_delay: Option<std::time::Duration>,
    pub(crate) ack_timeout: Option<std::time::Duration>,
    pub(crate) ack_group_time: Option<std::time::Duration>,
    pub(crate) dlq_policy: Option<(u32, Option<String>)>,
    pub(crate) read_compacted: bool,
    pub(crate) priority_level: Option<i32>,
    pub(crate) subscription_properties: Vec<(String, String)>,
    pub(crate) key_shared: Option<magnetar_proto::KeySharedConfig>,
    pub(crate) replicate_subscription_state: Option<bool>,
    pub(crate) force_topic_creation: Option<bool>,
    pub(crate) start_message_rollback_duration_sec: Option<u64>,
}

impl ConsumerTemplate {
    /// Apply the template to a [`crate::ConsumerBuilder`] for the given topic.
    pub(crate) fn apply<'a, E: crate::Engine>(
        &self,
        mut builder: crate::ConsumerBuilder<'a, E>,
    ) -> crate::ConsumerBuilder<'a, E> {
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
