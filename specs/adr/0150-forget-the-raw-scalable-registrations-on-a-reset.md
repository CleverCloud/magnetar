# ADR-0150 — Forget the raw scalable registrations on a reset

- **Status**: Accepted
- **Date**: 2026-11-21
- **Decider**: Florentin Dubois
- **Tags**: scalable-topics, pip-460, dag-watch, reset, supervisor, state-machine, tokio, moonpool, sans-io
- **Amends**: [ADR-0093](0093-pip-460-upstream-wire-surface.md)

## Context

A DAG session, a namespace scalable-topics watch and a TC-assignment watch live at the broker only as long as the TCP connection that carried their subscribe command.
The broker reaps every watch of a dead connection, and nothing in the client writes the subscribe again: `Connection::watch_scalable_topics` sends `CommandWatchScalableTopics` with no `current_hash`, because it is "a fresh subscribe, not a reconnect resync", and it is the only send site of that command.
The same holds for `CommandScalableTopicLookup` and `CommandWatchTcAssignments`.
The owned aggregate (`StreamConsumer`) re-opens its own DAG session under a fresh id after a reconnect, and fences the old one by `session_epoch`, so nothing replays a raw registration.

`Connection::reset()` is what the supervisor calls between the drop and the new handshake.
It clears the events, the outbound buffer and `scalable_subscribe_requests`, and it replaces the PIP-145 topic-watcher registry through `TopicWatcherRegistry::begin_session`.
It left `scalable_sessions`, `scalable_topic_watches` and `tc_assignment_watches` registered, so after a supervised reconnect:

- `dag_snapshot`, `scalable_resolved_topic_name` and `scalable_topics_snapshot` kept answering with the layout and the topic set the broker last pushed on the dead session.
  They are public through the façade's raw scalable API, so a caller that polls them, or that was not parked on `next_scalable_event` when the connection dropped, cannot learn that the watch is over.
- A later `close_scalable_topic_session`, `close_scalable_topics_watch` or `close_tc_assignments_watch` found the id still registered and staged a close frame for an id the new session never opened.
- A watch opened while the connection was `Failed` was registered locally although `reset()` then wiped the outbound buffer that held its subscribe frame (`mark_disconnected` keeps the feature flags, so the capability check still passes), so it existed only locally from birth and survived the reset.
- A raw lookup that raced the disconnect left its empty session registered for the life of the `Connection`, with an id its caller never received.

The parked reader is not left hanging: every disconnect path wakes the close waiters and `next_scalable_event` returns `None` on a closed connection, which is the documented close signal.
The defect is the accessors and the close frames, not a missing event.

The id counters (`next_scalable_session_id`, `next_scalable_watch_id`) are not reset, so an id of the dead session is never handed to a new registration.

## Decision

`Connection::reset()` forgets the three raw registries, next to `scalable_subscribe_requests.clear()`.

- `scalable_sessions`, `scalable_topic_watches` and `tc_assignment_watches` are cleared under `scalable-topics`.
  The raw watches die with the physical session and nothing re-arms them.
  This differs from the PIP-145 topic-list watchers, which [ADR-0136](0136-register-acknowledged-topic-list-watchers-again-after-a-reconnect.md) keeps across `reset()` because `rebuild_consumers()` registers them again; no rebuild step does that for a raw scalable registration.
- No event is queued for them.
  The reset already drops the queued events, the supervisor's wake of the close waiters ends a parked `next_scalable_event`, and a client-initiated close that finds nothing to close is a no-op.
- After the reset the accessors answer `None` for an id of the dead session, an update the broker pushes for such an id has no owner and is ignored, and the three close methods stage no frame for it.
- The id counters and `scalable_consumers` are untouched.
  The consumer registrations are not a raw watch: their reset behaviour is the incarnation-fenced resynchronization of [ADR-0120](0120-assignment-driven-m1-hardened-stream-consumer.md).
- Both engines run this `Connection` code, so neither runtime source changes.
  There is no wire change, no new dependency, no public API change and no change to the `magnetar` façade.

## Consequences

- After a supervised reconnect a raw scalable accessor reports `None` instead of the dead session's data, and a late close of an old id writes nothing.
- A caller that wants the watch on the new session opens it again and receives a fresh id.
  This is what the owned aggregate already does.
- The in-flight leave intent held by `scalable_subscribe_requests` was already dropped by `reset()`; that is unchanged and still covered by the physical-connection cleanup.
- Cost: three `clear()` calls on a path that already clears larger registries; nothing changes on the steady-state path.
- Not changed: the raw lookup has no deadline of its own, and a raw lookup that fails on a closed connection returns an error without the session id.
  With this decision the leftover session no longer outlives the connection's next reset.

### ADR-0024 test layers

(a) `magnetar-proto/tests/scalable_reset_registries.rs`: `reset_forgets_a_namespace_watch`, `reset_forgets_a_dag_session`, `reset_forgets_a_tc_watch`, `close_after_a_reset_sends_nothing_for_an_id_the_new_session_never_opened`, `a_watch_opened_while_the_connection_is_failed_does_not_survive_the_reset`, and the control `registrations_opened_after_a_reset_are_tracked_under_fresh_ids`.
(b) and (c) `reset_forgets_every_raw_scalable_registration`, `a_watch_opened_while_the_connection_is_failed_does_not_survive_the_reset` and the control in `magnetar-runtime-tokio/tests/scalable_reset_registries.rs` and `magnetar-runtime-moonpool/tests/scalable_reset_registries.rs`.
(d) `magnetar-differential/tests/scalable_reset_registries_equivalence.rs`: `engines_forget_the_raw_scalable_registrations_of_a_dead_session`.
The end-to-end layer is not added in this change.
A real broker reaches the path whenever a raw scalable session, namespace watch or TC watch is open across a supervised reconnect, and the observable difference is an accessor answering `None` and a close frame not written; a test is possible and not written here: through the frame proxy against the scalable-topics broker it would open a namespace watch, `cut()` the connection, wait for the supervised reconnect, assert that `scalable_topics_snapshot` answers `None`, and check with `drop_next_client_frame(Type::WatchScalableTopicsClose)` and `dropped()` that a late close writes no frame.

## References

- `crates/magnetar-proto/src/conn.rs` — `Connection::reset`, `open_scalable_topic_session`, `watch_scalable_topics`, `watch_tc_assignments` and their close methods.
- [ADR-0093](0093-pip-460-upstream-wire-surface.md) § D2 — the session lifecycle: only a close command ends a session at the broker, otherwise the connection does.
- [ADR-0136](0136-register-acknowledged-topic-list-watchers-again-after-a-reconnect.md) — the PIP-145 watcher registry, the other watch family `reset()` handles, which it keeps and registers again.
- [ADR-0149](0149-close-a-scalable-topic-watch-the-client-rejects-at-the-broker.md) — the other end of the raw watch lifecycle.
- [ADR-0120](0120-assignment-driven-m1-hardened-stream-consumer.md) — the owned aggregate that re-opens its session after a reconnect.
