// SPDX-License-Identifier: Apache-2.0

//! PIP-145 topic-list watcher state machine.
//!
//! Tracks watcher subscriptions and translates `CommandWatchTopicListSuccess` /
//! `CommandWatchTopicUpdate` into the
//! [`ConnectionEvent::TopicListChanged`](crate::ConnectionEvent::TopicListChanged) deltas.
//!
//! # Wire flow
//!
//! 1. Client sends `CommandWatchTopicList { request_id, watcher_id, namespace, topics_pattern }`.
//! 2. Broker responds with `CommandWatchTopicListSuccess { request_id, watcher_id, topic,
//!    topics_hash }`.
//! 3. As the namespace changes, broker pushes `CommandWatchTopicUpdate { watcher_id, new_topics,
//!    deleted_topics, topics_hash }`.
//! 4. Client may send `CommandWatchTopicListClose { request_id, watcher_id }` to deregister.

use std::collections::HashMap;

use crate::types::RequestId;

/// In-flight state for a topic-list watcher.
#[derive(Debug, Clone)]
pub(crate) struct TopicWatcher {
    /// Pattern requested by the user (re-resolved server-side; the state machine just echoes it).
    pub(crate) pattern: String,
    /// Namespace the pattern applies to.
    pub(crate) namespace: String,
    /// Topics-hash from the last broker response (used to skip no-op updates).
    pub(crate) topics_hash: Option<String>,
    /// Whether the initial snapshot has been received.
    pub(crate) initialised: bool,
}

/// Registry of watchers indexed by `watcher_id`.
#[derive(Debug, Default)]
pub(crate) struct TopicWatcherRegistry {
    pub(crate) by_watcher: HashMap<u64, TopicWatcher>,
    pub(crate) by_request: HashMap<RequestId, u64>,
}

impl TopicWatcherRegistry {
    pub(crate) fn insert(&mut self, watcher_id: u64, request_id: RequestId, watcher: TopicWatcher) {
        self.by_watcher.insert(watcher_id, watcher);
        self.by_request.insert(request_id, watcher_id);
    }

    pub(crate) fn close(&mut self, watcher_id: u64) {
        self.by_watcher.remove(&watcher_id);
        self.by_request.retain(|_, v| *v != watcher_id);
    }

    pub(crate) fn lookup_by_request(&mut self, request_id: RequestId) -> Option<&mut TopicWatcher> {
        let watcher_id = self.by_request.get(&request_id)?;
        self.by_watcher.get_mut(watcher_id)
    }

    pub(crate) fn lookup_by_watcher_id(&mut self, watcher_id: u64) -> Option<&mut TopicWatcher> {
        self.by_watcher.get_mut(&watcher_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_roundtrip() {
        let mut r = TopicWatcherRegistry::default();
        r.insert(
            7,
            RequestId(11),
            TopicWatcher {
                pattern: "public/default/foo-.*".to_owned(),
                namespace: "public/default".to_owned(),
                topics_hash: None,
                initialised: false,
            },
        );
        let w = r.lookup_by_request(RequestId(11)).expect("watcher present");
        assert_eq!(w.pattern, "public/default/foo-.*");
        r.close(7);
        assert!(r.lookup_by_request(RequestId(11)).is_none());
    }
}
