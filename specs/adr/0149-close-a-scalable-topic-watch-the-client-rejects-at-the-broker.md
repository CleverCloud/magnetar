# ADR-0149 — Close a scalable-topic watch the client rejects at the broker

- **Status**: Accepted
- **Date**: 2026-11-21
- **Decider**: Florentin Dubois
- **Tags**: scalable-topics, pip-460, dag-watch, state-machine, wire, tokio, moonpool, sans-io
- **Amends**: [ADR-0093](0093-pip-460-upstream-wire-surface.md)

## Context

[ADR-0093](0093-pip-460-upstream-wire-surface.md) § D2 says the broker keeps pushing on a DAG session until the client sends `CommandScalableTopicClose`.
ADR-0093 does not say how the other two PIP-460 watches end.
The upstream `PulsarApi.proto` describes the namespace scalable-topics watch and the transaction-coordinator discovery watch as broker-pushed sessions.
Each has its own client-to-broker close command (`CommandWatchScalableTopicsClose`, `CommandWatchTcAssignmentsClose`) and carries an `error` only when the initial watch fails, so this ADR applies the DAG-session lifecycle to both by analogy.
The in-repo broker (`magnetar-fakes`) holds a DAG session until it receives that command or fails the session itself, which is the same model; it does not implement the other two watches.

`Connection` ends a watch on the client side when it rejects an inbound update, in three arms:

- `handle_scalable_topic_update` removes the DAG session on any `DagError`.
- `handle_scalable_topics_watch_update` removes the namespace watch on any `TopicsWatchError`.
- `handle_tc_assignments_update` removes the TC watch when the update carries no snapshot.

Each arm queued the `DagWatchClosed`, `ScalableTopicsWatchClosed` or `TcAssignmentsWatchClosed` event and encoded no frame.
The close command is encoded only by `close_scalable_topic_session`, `close_scalable_topics_watch` and `close_tc_assignments_watch`, and each of them returns at once for an id the registry no longer holds.
So after a rejection every later close is a no-op: the explicit close of a handle the runtime still holds, the `DagSession::next` close after it saw the event, and `PendingDagSession::drop`.

Two kinds of rejection reach those arms and they are not alike:

- A failure the **broker** reported (`DagError::Broker`, `TopicsWatchError::Broker`, a TC update carrying `error`) is the broker ending that watch, so nothing is left to close.
- A rejection the **client** decided on is an update that carries neither a layout, an event or a snapshot nor an error (`DagError::Empty`, `TopicsWatchError::Empty`, a TC update with neither field), or a forward-epoch DAG layout that fails whole-snapshot validation (`DagError::InvalidSnapshot`, for example `ActiveCoverageEnd`).
  The broker has not ended anything, so the watch stays live there and the client has forgotten it.

The leak is bounded per rejection but not per connection.
The aggregate's `control_loop` re-opens a rejected DAG session once and fails the aggregate closed if the replacement is rejected too, which orphans at most two sessions.
`reconnect_control_plane` instead retries `open_dag_session` through `retry_control_plane_error`, which treats the rejection as non-terminal, pushes `ResyncRequired`, sleeps 100 ms and tries again with no attempt cap.
A layout that is still rejectable when the reconnect runs therefore abandons one more broker-side session per pass for the life of the bootstrap TCP connection.

A real 5.0.0 broker is not known to send an unusable update; the client validator may be stricter than the broker, or the broker may have a defect.
The feature is default-off and experimental, so the severity is low.

## Decision

A rejection the client decided on stages the watch's close command; a failure the broker reported stages nothing.

- `Connection` gains the private `encode_scalable_topic_close`, the one place `CommandScalableTopicClose` is built.
  `close_scalable_topic_session` calls it, and so does the DAG rejection arm.
- The DAG arm removes the session as before, stages the close unless the error is `DagError::Broker`, and queues the same `DagWatchClosed` event with the same reason.
- The namespace-watch arm calls `close_scalable_topics_watch` for every error except `TopicsWatchError::Broker`, and removes the watch without a frame for that one.
- The TC arm calls `close_tc_assignments_watch` when the update carries no snapshot and no error, and removes the watch without a frame when it carries `error`.
- No second close follows: the watch is gone from the registry after the arm, so `close_scalable_topic_session`, `close_scalable_topics_watch` and `close_tc_assignments_watch` stay no-ops for it, which is what keeps the runtimes' own closes from writing a second frame.
- The events, their reasons, the runtimes' reaction to them and the reconnect retry schedule are unchanged.
  Capping that retry is a separate decision and is not made here.
- Both engines run this `Connection` code, so neither runtime source changes.
  There is no wire change (the three close commands already exist), no new dependency, no public API change and no change to the `magnetar` façade.

## Consequences

- A watch the client refuses no longer outlives the client's record of it: the broker is told to stop pushing on it in the same pass that drops it, so the reconnect retry no longer accumulates broker-side sessions.
- A broker that reports a failure still receives nothing back, so a conformant broker sees no new frame in the common case.
- Cost: one extra close frame per client-side rejection, on a path that is already a protocol-shape surprise; nothing changes on the steady-state path.
- `docs/pip-features.md` now says a client-side rejection closes the raw session at the broker as well as locally; it previously said only "closes that raw session".

### ADR-0024 test layers

(a) `magnetar-proto/tests/scalable_rejection_close.rs`: `bodyless_dag_update_closes_the_session_at_the_broker`, `invalid_dag_layout_closes_the_session_at_the_broker`, `bodyless_topics_update_closes_the_watch_at_the_broker`, `bodyless_tc_update_closes_the_watch_at_the_broker`, and the control `broker_reported_failure_is_not_closed_again`.
(b) and (c) the same five tests in `magnetar-runtime-tokio/tests/scalable_rejection_close.rs` and `magnetar-runtime-moonpool/tests/scalable_rejection_close.rs`.
(d) `magnetar-differential/tests/scalable_rejection_close_equivalence.rs`: `engines_close_a_client_rejected_watch_at_the_broker_and_only_that`.
The end-to-end layer is not added in this change.
A real broker reaches the path only if it sends a bodyless update or a layout the client validator refuses, which no available broker has been observed to do, and no test under `crates/magnetar/tests/` can make one.

## References

- `crates/magnetar-proto/src/conn.rs` — `Connection::encode_scalable_topic_close`, `handle_scalable_topic_update`, `handle_scalable_topics_watch_update`, `handle_tc_assignments_update`.
- `crates/magnetar-proto/src/dag_watch.rs` — `DagError`, `DagWatchSession::handle_update`.
- [ADR-0093](0093-pip-460-upstream-wire-surface.md) § D2 — the session lifecycle this ADR makes the client honour on a rejection.
- [ADR-0120](0120-assignment-driven-m1-hardened-stream-consumer.md) — the aggregate whose reconnect path retries the DAG session open.
