// SPDX-License-Identifier: Apache-2.0

//! Compacted-topic key/value view. Mirrors `org.apache.pulsar.client.api.TableView`.
//!
//! A [`TableView`] subscribes to a topic (compacted, earliest position) and projects each
//! delivered message into a `HashMap<key, value>` where `key` is the message's `partition_key`
//! and `value` is its raw payload. Late-bound listeners can react to mutations. The view
//! lives as long as the [`TableView`] handle; dropping it tears down the background drain
//! task.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::task::JoinHandle;

use crate::PulsarClient;
use crate::client::PulsarError;

/// Callback fired for every mutation applied to the table view.
///
/// `key` is the message's `partition_key` (empty messages without a key are skipped).
/// `value` is the message's raw payload (`None` when the producer sent a tombstone — a
/// keyed message with empty payload, the Pulsar compaction convention for deletes).
pub type TableViewListener = Arc<dyn Fn(&str, Option<&Bytes>) + Send + Sync>;

/// Compacted-topic key/value view.
#[derive(Clone)]
pub struct TableView {
    state: Arc<RwLock<HashMap<String, Bytes>>>,
    drain: Arc<DrainTask>,
}

impl std::fmt::Debug for TableView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableView")
            .field("size", &self.state.read().len())
            .finish_non_exhaustive()
    }
}

struct DrainTask {
    handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl Drop for DrainTask {
    fn drop(&mut self) {
        if let Ok(mut g) = self.handle.try_lock() {
            if let Some(h) = g.take() {
                h.abort();
            }
        }
    }
}

impl TableView {
    /// Number of distinct keys currently materialised.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state.read().len()
    }

    /// `true` if no key has been observed yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.state.read().is_empty()
    }

    /// Lookup the most recent value for the given key, if any.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.state.read().get(key).cloned()
    }

    /// `true` if the key has at least one materialised value.
    #[must_use]
    pub fn contains_key(&self, key: &str) -> bool {
        self.state.read().contains_key(key)
    }

    /// Snapshot every currently-known (key, value) pair. Allocates — use [`Self::for_each`]
    /// for hot paths.
    #[must_use]
    pub fn snapshot(&self) -> HashMap<String, Bytes> {
        self.state.read().clone()
    }

    /// Snapshot every currently-known key. Mirrors Java `TableView#keySet`.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        self.state.read().keys().cloned().collect()
    }

    /// Snapshot every currently-known value. Mirrors Java `TableView#values`.
    #[must_use]
    pub fn values(&self) -> Vec<Bytes> {
        self.state.read().values().cloned().collect()
    }

    /// Returns `true` if any key maps to a value equal to `value`. Mirrors Java
    /// `TableView#containsValue`.
    #[must_use]
    pub fn contains_value(&self, value: &[u8]) -> bool {
        self.state.read().values().any(|v| v.as_ref() == value)
    }

    /// Iterate every currently-known (key, value) pair under a shared read lock. The
    /// callback must not call back into the [`TableView`] or it will deadlock.
    pub fn for_each<F: FnMut(&str, &Bytes)>(&self, mut f: F) {
        for (k, v) in self.state.read().iter() {
            f(k, v);
        }
    }

    /// Tear down the background drain task. The view's snapshot remains queryable.
    pub async fn close(self) {
        let mut g = self.drain.handle.lock().await;
        if let Some(h) = g.take() {
            h.abort();
            let _ = h.await;
        }
    }
}

/// Builder for a [`TableView`]. Mirrors `org.apache.pulsar.client.api.TableViewBuilder`.
pub struct TableViewBuilder<'a> {
    client: &'a PulsarClient,
    topic: String,
    subscription: Option<String>,
    receiver_queue_size: usize,
    listener: Option<TableViewListener>,
}

impl std::fmt::Debug for TableViewBuilder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableViewBuilder")
            .field("topic", &self.topic)
            .field("subscription", &self.subscription)
            .field("receiver_queue_size", &self.receiver_queue_size)
            .field("has_listener", &self.listener.is_some())
            .finish()
    }
}

impl<'a> TableViewBuilder<'a> {
    pub(crate) fn new(client: &'a PulsarClient, topic: String) -> Self {
        Self {
            client,
            topic,
            subscription: None,
            receiver_queue_size: 1000,
            listener: None,
        }
    }

    /// Override the subscription name used by the underlying reader. Defaults to a unique
    /// per-instance `table-view-<uuid>` so two views over the same topic do not share
    /// dispatch state.
    #[must_use]
    pub fn subscription_name(mut self, name: impl Into<String>) -> Self {
        self.subscription = Some(name.into());
        self
    }

    /// Override the receiver-queue size used by the underlying consumer.
    #[must_use]
    pub fn receiver_queue_size(mut self, size: usize) -> Self {
        self.receiver_queue_size = size;
        self
    }

    /// Install a listener invoked for every materialised update. The callback runs inside
    /// the drain task; keep it fast and non-blocking.
    #[must_use]
    pub fn on_update(mut self, listener: TableViewListener) -> Self {
        self.listener = Some(listener);
        self
    }

    /// Subscribe, drain backlog, and return the view. The future resolves once the
    /// background drain task is running — the initial snapshot continues to populate in
    /// the background as compacted messages arrive.
    pub async fn create(self) -> Result<TableView, PulsarError> {
        let subscription = self
            .subscription
            .unwrap_or_else(|| format!("table-view-{}", uuid::Uuid::new_v4().simple()));
        let consumer = self
            .client
            .consumer(self.topic)
            .subscription(subscription)
            .subscription_type(magnetar_proto::pb::command_subscribe::SubType::Exclusive)
            .durable(false)
            .initial_position(magnetar_proto::pb::command_subscribe::InitialPosition::Earliest)
            .read_compacted(true)
            .receiver_queue_size(self.receiver_queue_size)
            .subscribe()
            .await?;

        let state: Arc<RwLock<HashMap<String, Bytes>>> = Arc::new(RwLock::new(HashMap::new()));
        let state_drain = state.clone();
        let listener = self.listener.clone();
        let join = tokio::spawn(async move {
            loop {
                let Ok(msg) = consumer.receive().await else {
                    break;
                };
                let key = msg
                    .single_metadata
                    .as_ref()
                    .and_then(|sm| sm.partition_key.clone())
                    .or_else(|| msg.metadata.partition_key.clone());
                let Some(key) = key else {
                    // Pulsar compaction key is required; messages without one are skipped.
                    let _ = consumer.ack(msg.message_id).await;
                    continue;
                };
                let payload = msg.payload.clone();
                let is_tombstone = payload.is_empty();
                {
                    let mut s = state_drain.write();
                    if is_tombstone {
                        s.remove(&key);
                    } else {
                        s.insert(key.clone(), payload.clone());
                    }
                }
                if let Some(l) = &listener {
                    if is_tombstone {
                        l(&key, None);
                    } else {
                        l(&key, Some(&payload));
                    }
                }
                let _ = consumer.ack(msg.message_id).await;
            }
        });

        Ok(TableView {
            state,
            drain: Arc::new(DrainTask {
                handle: tokio::sync::Mutex::new(Some(join)),
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_view_snapshot_returns_empty_map() {
        // We can't trivially construct a TableView without a broker, but we can verify the
        // map operations on the inner state.
        let state: Arc<RwLock<HashMap<String, Bytes>>> = Arc::new(RwLock::new(HashMap::new()));
        assert!(state.read().is_empty());
        state
            .write()
            .insert("a".to_owned(), Bytes::from_static(b"1"));
        state
            .write()
            .insert("b".to_owned(), Bytes::from_static(b"2"));
        // Tombstone "a" — remove
        state.write().remove("a");
        assert_eq!(state.read().len(), 1);
        assert!(state.read().contains_key("b"));
        assert_eq!(state.read().get("b").unwrap().as_ref(), b"2");
    }
}
