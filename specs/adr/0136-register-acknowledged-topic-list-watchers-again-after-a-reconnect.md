# ADR-0136 — Register acknowledged topic-list watchers again after a reconnect

- **Status**: Accepted
- **Date**: 2026-11-20
- **Decider**: Florentin Dubois
- **Tags**: pip-145, pattern-consumer, topic-watcher, reconnect, state-machine, tokio, moonpool, sans-io
- **Amends**: [ADR-0037](0037-multi-topics-pattern-consumer-pass-2-lift.md)

## Context

A PIP-145 topic-list watcher is a registration on one broker connection: `CommandWatchTopicList` creates it, the broker answers with `CommandWatchTopicListSuccess` and then pushes `CommandWatchTopicUpdate` deltas for as long as the connection lives.
`Connection::reset()` replaced the whole watcher registry, and the two supervisors re-attach only producers (`rebuild_producers`) and consumers (`rebuild_consumers`) once the new handshake completes.
Nothing sent `CommandWatchTopicList` on the new session, so a `PatternConsumer` built before the first supervised reconnect received no delta afterwards: new matching topics were never subscribed and deleted ones never closed.
`PatternConsumer::update` and `start_auto_reconcile` only drain deltas, and there is no re-snapshot.
Its existing children kept working, so nothing reported the loss, and no test reset a connection that held a watcher.

The reset's own comment justified the replacement for lookups, whose `Connect` / `Redirect` traffic is stale on a new socket; a watcher is different because its caller still holds it.

## Decision

A watcher the broker acknowledged survives `reset()` and is registered again by `rebuild_consumers()`.

- **What survives.**
  `TopicWatcherRegistry::begin_session` keeps every watcher whose first reply arrived (`initialised`), with its id, namespace, pattern and last topics hash, marks it `Registration::Owed`, and forgets every request id of the dead session.
  A watcher still awaiting its first reply is dropped: its pending request resolves with `OpOutcome::SessionLost`, so its caller holds no watcher.
  The state is kept across several resets before a rebuild, and a re-registration the broker never answered is owed again.
- **Where it is sent.**
  `rebuild_consumers()` calls `rebuild_topic_watchers()` after the consumers.
  Both supervisors already call that function after the handshake, and so does every test that drives the connection in the supervisor's order, so neither driver changes and there is no new call to forget.
  Registrations go out in ascending watcher id order, each under a fresh request id, with the original watcher id and the last hash the broker reported (`CommandWatchTopicList.topics_hash` is documented as present only when the client reconnects).
  Keeping the id keeps the deltas of the new session addressed to the same owner ([ADR-0132](0132-address-pip-145-topic-list-deltas-to-their-watcher.md)).
  The returned `Vec<RequestId>` is unchanged: it lists the re-subscribed consumers, and callers index it.
- **How the reply is handled.**
  Nobody awaits a re-registration, so recording an outcome for it would leave one entry per watcher per reconnect that nothing takes.
  The watcher tracks the request itself (`Registration::Awaiting(request_id)`), not the connection's `pending_requests`, so `reset()` and `fail_all_pending` need no new case.
  The success arm settles it first (`complete_reattach`): it stores the new hash, returns the watcher to `Live`, and returns without an outcome or an event.
  The error arm's no-pending-request branch settles a refusal (`reject_reattach`): it logs a `warn!` with the broker's bounded message and leaves the watcher `Owed`, so the next rebuild tries again.
- **What is not done.**
  The topic list the broker returns is discarded.
  It is a whole list, not a delta: `PatternConsumer::update` applies deltas only, and the connection keeps no per-watcher topic set to diff the list against.
  A topic created or deleted while the connection was down is therefore still not reported.
  Nothing deregisters a watcher (`CommandWatchTopicListClose` has no emitter), so watchers acknowledged for `topic_list_snapshot` are registered again too.
  Both are recorded in `docs/follow-ups.md` §31.
- No new public API, no wire change beyond the optional field the protocol documents.

## Consequences

- A `PatternConsumer` keeps receiving deltas after any number of supervised reconnects, on a flapping link included.
- A reconnect costs one `CommandWatchTopicList` per acknowledged watcher, including the ones `topic_list_snapshot` left behind: the watcher growth that a reconnect used to reset now lasts for the client's life.
- The same holds for the deltas of those watchers: nothing reads them, they pass the connection's filter because the watcher is held, and they queue in each runtime's `ConnectionShared::topic_list_changes`, which has no bound; that queue used to stop growing at the first reconnect and now grows for the client's life.
  This is the PIP-145 watcher-lifetime gap of `docs/follow-ups.md` §31, left for a follow-up that deregisters watchers.
- A broker that refuses the re-registration leaves the pattern consumer without updates until the next reconnect, and says so in the log.
- Cost: one enum field on `TopicWatcher`, three registry methods, one rebuild step and two branches in the reply arms.
- Incompatible with: a supervisor that re-attaches producers and consumers through a path other than `rebuild_consumers`; both supervised loops and the proto API use it.

### ADR-0024 test layers

(a) `magnetar-proto/tests/topic_watcher_reconnect.rs` (the registration goes out again) and `topic_watcher_reattach.rs` (reply consumed in place with the watcher still receiving deltas and carrying its newest hash, refusal retried, unacknowledged watcher dropped, unanswered registration owed again); (b) `magnetar-runtime-tokio/tests/topic_watcher_reconnect.rs` and `topic_watcher_reattach.rs`; (c) the moonpool twins; (d) `magnetar-differential/tests/topic_watcher_reconnect_equivalence.rs` and `topic_watcher_reattach_equivalence.rs`.
End-to-end: `crates/magnetar/tests/e2e_reconnect.rs::e2e_pattern_consumer_discovers_topics_created_after_a_reconnect` cuts every connection of a supervised client holding a `PatternConsumer` through a TCP proxy, with the broker left running, creates a matching topic once the session is back and expects `PatternConsumer::update` to subscribe it.

## References

- `crates/magnetar-proto/src/topic_watcher.rs` — `Registration`, `begin_session`, `begin_reattach`, `complete_reattach`, `reject_reattach`.
- `crates/magnetar-proto/src/conn.rs` — `reset`, `rebuild_consumers`, `rebuild_topic_watchers`, the `WatchTopicListSuccess` and `Error` arms.
- [ADR-0037](0037-multi-topics-pattern-consumer-pass-2-lift.md) — the pattern consumer and `BrokerMetadataApi` whose delta stream this keeps alive.
- [ADR-0132](0132-address-pip-145-topic-list-deltas-to-their-watcher.md) — deltas addressed to their watcher id, which the re-registration keeps.
- `docs/follow-ups.md` §31 — watcher lifetime and the outage window.
