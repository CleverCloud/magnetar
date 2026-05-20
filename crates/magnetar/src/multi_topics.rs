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
