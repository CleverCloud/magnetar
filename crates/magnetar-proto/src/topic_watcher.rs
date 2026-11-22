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
//! 4. Client sends `CommandWatchTopicListClose { request_id, watcher_id }` to deregister, once the
//!    last user of the watcher is gone
//!    ([`Connection::close_topic_watcher`](crate::Connection::close_topic_watcher), ADR-0156). The
//!    broker's reply is not tracked: no future awaits it.
//!
//! # Across a reconnect
//!
//! The broker keeps a watcher per connection, so one dies with the session that registered it and
//! the new session pushes no update until it is registered again. A watcher the broker had
//! acknowledged therefore survives `TopicWatcherRegistry::begin_session` (the connection's
//! `reset()`) with its namespace, pattern and last topics hash, and the connection's
//! `rebuild_consumers` registers it again under a fresh request id
//! (`TopicWatcherRegistry::begin_reattach`). Nobody awaits that request, so its reply is
//! consumed here instead of being recorded as an outcome.

use std::collections::{BTreeMap, HashMap};

use crate::types::RequestId;

/// Where a watcher stands against the broker session the connection is on now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Registration {
    /// The broker holds it for this session, or its first reply is still owed to the caller that
    /// registered it.
    Live,
    /// A reset took the registration down with the old session and no `CommandWatchTopicList`
    /// has gone out for it yet.
    Owed,
    /// The registration went out again as this request and has no reply yet.
    Awaiting(RequestId),
}

/// In-flight state for a topic-list watcher.
#[derive(Debug, Clone)]
pub(crate) struct TopicWatcher {
    /// Pattern requested by the user (re-resolved server-side; the state machine just echoes it).
    pub(crate) pattern: String,
    /// Namespace the pattern applies to.
    pub(crate) namespace: String,
    /// Topics-hash from the last broker response (used to skip no-op updates, and sent back to
    /// the broker when the watcher is registered again on a new session).
    pub(crate) topics_hash: Option<String>,
    /// Whether the initial snapshot has been received. Survives a reset: it is what tells a
    /// watcher the caller holds (and must keep hearing from) from one still awaiting its first
    /// reply, whose caller is told the session was lost.
    pub(crate) initialised: bool,
    /// Whether the broker's current session has this watcher.
    pub(crate) registration: Registration,
}

/// One watcher registration to put on the wire again, as returned by
/// [`TopicWatcherRegistry::begin_reattach`].
#[derive(Debug)]
pub(crate) struct Reattach {
    pub(crate) watcher_id: u64,
    pub(crate) request_id: RequestId,
    pub(crate) namespace: String,
    pub(crate) pattern: String,
    pub(crate) topics_hash: Option<String>,
}

/// Registry of watchers indexed by `watcher_id`.
#[derive(Debug, Default)]
pub(crate) struct TopicWatcherRegistry {
    /// Ordered by id, so registrations go out in a deterministic order.
    pub(crate) by_watcher: BTreeMap<u64, TopicWatcher>,
    pub(crate) by_request: HashMap<RequestId, u64>,
}

impl TopicWatcherRegistry {
    pub(crate) fn insert(&mut self, watcher_id: u64, request_id: RequestId, watcher: TopicWatcher) {
        self.by_watcher.insert(watcher_id, watcher);
        self.by_request.insert(request_id, watcher_id);
    }

    /// Start a new broker session: keep the watchers the broker had acknowledged, mark each as
    /// owing a registration, and forget every request id of the dead session.
    ///
    /// A watcher still awaiting its first reply is dropped: its pending request is failed with
    /// `SessionLost`, so its caller holds no watcher to keep alive. Idempotent across several
    /// resets before a rebuild, and a watcher whose re-registration is unanswered is owed again.
    pub(crate) fn begin_session(&mut self) {
        self.by_watcher.retain(|_, watcher| watcher.initialised);
        for watcher in self.by_watcher.values_mut() {
            watcher.registration = Registration::Owed;
        }
        self.by_request.clear();
    }

    /// Take every watcher that owes a registration, in ascending id order, giving each the next
    /// request id from `next_request_id` and marking it as awaiting that reply.
    pub(crate) fn begin_reattach(
        &mut self,
        mut next_request_id: impl FnMut() -> RequestId,
    ) -> Vec<Reattach> {
        let mut reattach = Vec::new();
        for (watcher_id, watcher) in self
            .by_watcher
            .iter_mut()
            .filter(|(_, watcher)| watcher.registration == Registration::Owed)
        {
            let request_id = next_request_id();
            watcher.registration = Registration::Awaiting(request_id);
            self.by_request.insert(request_id, *watcher_id);
            reattach.push(Reattach {
                watcher_id: *watcher_id,
                request_id,
                namespace: watcher.namespace.clone(),
                pattern: watcher.pattern.clone(),
                topics_hash: watcher.topics_hash.clone(),
            });
        }
        reattach
    }

    /// Settle the broker's success for `request_id` when it answers a re-registration. Returns
    /// `true` when it did, and the caller must record nothing for it: no future awaits it.
    pub(crate) fn complete_reattach(&mut self, request_id: RequestId, topics_hash: &str) -> bool {
        self.lookup_by_request(request_id)
            .filter(|watcher| watcher.registration == Registration::Awaiting(request_id))
            .map(|watcher| {
                watcher.registration = Registration::Live;
                watcher.topics_hash = Some(topics_hash.to_owned());
            })
            .is_some()
    }

    /// Settle the broker's error for `request_id` when it answers a re-registration. Returns
    /// `true` when it did. The watcher owes a registration again, so the next rebuild retries it.
    pub(crate) fn reject_reattach(&mut self, request_id: RequestId) -> bool {
        self.lookup_by_request(request_id)
            .filter(|watcher| watcher.registration == Registration::Awaiting(request_id))
            .map(|watcher| watcher.registration = Registration::Owed)
            .is_some()
    }

    /// Stop tracking `watcher_id`: forget it and every request id that resolves to it. Returns
    /// where its registration stood against the broker's current session, or `None` when the
    /// registry held nothing for it, which is how the caller tells a watcher it must close at
    /// the broker from one that is already gone.
    pub(crate) fn close(&mut self, watcher_id: u64) -> Option<Registration> {
        let watcher = self.by_watcher.remove(&watcher_id);
        self.by_request.retain(|_, v| *v != watcher_id);
        watcher.map(|watcher| watcher.registration)
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
            registration: Registration::Live,
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
        assert_eq!(r.close(7), Some(Registration::Live));
        assert!(r.lookup_by_request(RequestId(11)).is_none());
    }

    #[test]
    fn close_reports_where_the_registration_stood_and_nothing_for_an_unknown_watcher() {
        let mut r = TopicWatcherRegistry::default();
        r.insert(1, RequestId(10), watcher("a/.*", "a"));
        r.insert(2, RequestId(20), watcher("b/.*", "b"));
        r.insert(3, RequestId(30), watcher("c/.*", "c"));
        r.lookup_by_watcher_id(2).unwrap().registration = Registration::Owed;
        r.lookup_by_watcher_id(3).unwrap().registration = Registration::Awaiting(RequestId(31));

        assert_eq!(r.close(1), Some(Registration::Live));
        assert_eq!(r.close(2), Some(Registration::Owed));
        assert_eq!(r.close(3), Some(Registration::Awaiting(RequestId(31))));
        assert_eq!(r.close(1), None, "a watcher closes once");
        assert_eq!(r.close(99), None, "an id never registered closes nothing");
        assert!(r.by_watcher.is_empty());
        assert!(r.by_request.is_empty());
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
