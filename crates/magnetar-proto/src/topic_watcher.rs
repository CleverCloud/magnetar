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
    // reason: carried for the derived `Debug` trace context and read by unit tests; future
    // re-subscribe paths re-issue with the original pattern.
    #[allow(dead_code)]
    pub(crate) pattern: String,
    /// Namespace the pattern applies to.
    // reason: same as `pattern` — kept for Debug + future re-subscribe payload.
    #[allow(dead_code)]
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

    // reason: called only from unit tests today; the engine hooks the close path through
    // `Connection`'s pending-request dispatch instead, but the registry helper stays as the
    // single ownership-clearing surface for the upcoming `CommandWatchTopicListClose` wiring.
    #[allow(dead_code)]
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

    fn watcher(pattern: &str, namespace: &str) -> TopicWatcher {
        TopicWatcher {
            pattern: pattern.to_owned(),
            namespace: namespace.to_owned(),
            topics_hash: None,
            initialised: false,
        }
    }

    #[test]
    fn registry_roundtrip() {
        let mut r = TopicWatcherRegistry::default();
        r.insert(
            7,
            RequestId(11),
            watcher("public/default/foo-.*", "public/default"),
        );
        let w = r.lookup_by_request(RequestId(11)).expect("watcher present");
        assert_eq!(w.pattern, "public/default/foo-.*");
        r.close(7);
        assert!(r.lookup_by_request(RequestId(11)).is_none());
    }

    #[test]
    fn lookup_by_watcher_id_returns_inserted_watcher() {
        let mut r = TopicWatcherRegistry::default();
        r.insert(42, RequestId(99), watcher("ns/.*", "public/ns"));
        let w = r.lookup_by_watcher_id(42).expect("watcher present");
        assert_eq!(w.namespace, "public/ns");
        assert!(!w.initialised);
        assert!(w.topics_hash.is_none());
    }

    #[test]
    fn lookup_by_unknown_watcher_id_is_none() {
        let mut r = TopicWatcherRegistry::default();
        assert!(r.lookup_by_watcher_id(1).is_none());
        assert!(r.lookup_by_request(RequestId(1)).is_none());
    }

    #[test]
    fn close_removes_all_request_mappings_for_watcher() {
        // A single watcher_id can correspond to multiple in-flight requests; close must
        // clear every mapping that points at the closed watcher.
        let mut r = TopicWatcherRegistry::default();
        r.insert(5, RequestId(100), watcher("a/.*", "a"));
        r.by_request.insert(RequestId(101), 5);
        r.by_request.insert(RequestId(102), 5);
        r.insert(6, RequestId(200), watcher("b/.*", "b"));
        assert_eq!(r.by_request.len(), 4);
        r.close(5);
        assert!(r.lookup_by_request(RequestId(100)).is_none());
        assert!(r.lookup_by_request(RequestId(101)).is_none());
        assert!(r.lookup_by_request(RequestId(102)).is_none());
        assert!(r.lookup_by_request(RequestId(200)).is_some());
        assert!(r.lookup_by_watcher_id(5).is_none());
        assert!(r.lookup_by_watcher_id(6).is_some());
    }

    #[test]
    fn topics_hash_mutation_persists() {
        let mut r = TopicWatcherRegistry::default();
        r.insert(11, RequestId(50), watcher("foo/.*", "public/foo"));
        let w = r.lookup_by_watcher_id(11).unwrap();
        w.topics_hash = Some("abc123".to_owned());
        w.initialised = true;
        let w2 = r.lookup_by_watcher_id(11).unwrap();
        assert_eq!(w2.topics_hash.as_deref(), Some("abc123"));
        assert!(w2.initialised);
    }

    #[test]
    fn distinct_watchers_dont_share_state() {
        let mut r = TopicWatcherRegistry::default();
        r.insert(1, RequestId(10), watcher("a/.*", "a-ns"));
        r.insert(2, RequestId(20), watcher("b/.*", "b-ns"));
        r.lookup_by_watcher_id(1).unwrap().topics_hash = Some("hash1".to_owned());
        assert_eq!(
            r.lookup_by_watcher_id(1).unwrap().topics_hash.as_deref(),
            Some("hash1"),
        );
        assert!(r.lookup_by_watcher_id(2).unwrap().topics_hash.is_none());
        assert_eq!(r.lookup_by_watcher_id(2).unwrap().pattern, "b/.*");
    }

    #[test]
    fn re_insert_same_watcher_id_overwrites() {
        // Idempotency: re-inserting overwrites.
        let mut r = TopicWatcherRegistry::default();
        r.insert(3, RequestId(30), watcher("v1/.*", "v1-ns"));
        r.insert(3, RequestId(31), watcher("v2/.*", "v2-ns"));
        let w = r.lookup_by_watcher_id(3).unwrap();
        assert_eq!(w.pattern, "v2/.*");
        assert_eq!(w.namespace, "v2-ns");
        assert!(r.lookup_by_request(RequestId(30)).is_some());
        assert!(r.lookup_by_request(RequestId(31)).is_some());
    }
}
