# ADR-0153 — Bound a pending seek by `operation_timeout`, measured from the caller's clock

- **Status**: Accepted
- **Date**: 2026-11-21
- **Decider**: Florentin Dubois
- **Tags**: consumer, seek, timeouts, sans-io, tokio, moonpool
- **Amends**: [ADR-0133](0133-connection-owned-seek-reply-handling.md)

## Context

[ADR-0133](0133-connection-owned-seek-reply-handling.md) made the connection answer a seek the broker rejects (`CommandError`) or applies (`CommandSuccess`), whether the awaiting future is alive or was dropped.
Both answers need a reply.
A seek the broker never answers — a dropped reply on an otherwise healthy connection, or a broker that tore the consumer down mid-seek and went quiet — still left the consumer frozen:

- `Connection::seek` calls `ConsumerState::begin_seek`, which sets `pending_seek`, parks the queue and the four flow-control mirrors in `seek_rollback` and holds every later dispatch.
  Only a reply lifts that.
- `Connection::handle_timeout` swept two kinds of pending request against a deadline, `Ack` (`ack_response_timeout`, issue #346) and `ProducerOpen` (`operation_timeout`, [ADR-0125](0125-deadline-on-a-pending-producer-open.md)), and no `ConsumerSeek`.
- So the consumer dropped every later delivery, granted no flow, was invisible to the stall watchdog (a candidate needs `pending_seek.is_none()`) and refused every in-place re-attach, for the life of the connection; and the awaiting `seek_*` future parked on `RequestFut` forever, because `complete_staged_seek` has no deadline of its own.
- A dropped future changed nothing: ADR-0133 retags it `ConsumerSeekForgotten`, which also waits for a reply that never comes.

Java's `ConsumerImpl` completes a failed seek exceptionally through `failSeek` and lets the consumer carry on, which is the shape ADR-0133 already follows for a rejection and the one an expiry should take.

ADR-0125 measured its deadline from the connection's `last_activity` because the producer re-attach sites it covers are each triggered by a frame that just refreshed it, and excluded a first attachment because `create_producer` is user-driven and its baseline "can legitimately be a whole keepalive interval old".
It also called an early firing safe: it spends one accounted attempt and re-issues.
Neither holds for a seek.
A seek is user-driven, and it is not re-issued: an expiry that fires early rolls the consumer back while a slow but live broker goes on to apply the seek, which leaves a consumer the broker dropped and the client believes it still holds, the wedge ADR-0133 closed.
With the defaults (`keepalive_interval` and `operation_timeout` both 30 s) a `last_activity` baseline can be 30 s old on an idle connection, so a healthy seek could fail immediately.

Alternatives considered:

- **Add a `now: Instant` to `Connection::seek`.** The sans-io rule is that a user-driven entry takes the engine's clock, but `seek` already has callers in the workspace's tests that name the two-argument form, and a signature change buys nothing the new entry below does not.
- **A dedicated `seek_timeout` knob.** `operation_timeout` already documents itself as the budget for a request the broker must acknowledge; a second knob would let the two disagree.
- **A `last_activity` baseline alone, as ADR-0125 does.** Rejected for the reason above.
- **Sweep with no `poll_timeout` arm.** The sweep would then ride an unrelated tick, up to a keepalive interval late in production and seed-divergent under the moonpool engine.

## Decision

A pending seek has the connection's `operation_timeout` as its deadline, and its expiry is a rejection the broker never sent.

- `PendingRequestKind::ConsumerSeek` and `ConsumerSeekForgotten` carry `enqueued_at: Option<Instant>`; the retag in `abandon_consumer_seek_waiter` copies it, so dropping the future does not restart the clock.
- **`Connection::seek_at(handle, target, now)`** stamps the caller's own instant, and both runtimes call it from `stage_seek` (`Instant::now()` on tokio, `ConnectionShared::now_instant()` on moonpool, as `initial_flow` already is).
  `Connection::seek(handle, target)` keeps its signature for sans-io callers with no clock and falls back to `last_activity`, which can fire up to one keepalive interval early; `None` (nothing decoded yet) arms no deadline, and the reset that follows takes the request.
- `Connection::pending_seek_deadline` is the single predicate behind both halves: `poll_timeout` arms exactly what `handle_timeout` sweeps, for the reason ADR-0125 gives.
- On expiry `Connection::expire_pending_seek` removes the request and runs `recover_consumer_after_rejected_seek`, the function a `CommandError` runs, so there is no second thaw path:
  - a consumer the broker still held gets its queue and flow ledger back, and the dispatches held meanwhile are delivered in order behind them;
  - a consumer the broker closed mid-seek has no snapshot and is re-attached in place, with its flow released on that subscribe's own acknowledgement;
  - an older seek expiring does not thaw a newer one still in flight (`SeekRejection::Superseded`).
- An awaited seek gets `OpOutcome::Error { code: -1, message: "seek timeout" }`, the `-1` sentinel of the ack, send and producer-open timeouts under a string of its own, so the future resolves with `ClientError::Broker`.
  The outcome is recorded whether or not a waker is parked, like the `CommandError` arm: a seek an aggregate consumer staged and has not polled yet would otherwise find neither a reply nor a pending request on its first poll and park forever.
  A forgotten seek has no future and records none.
- Both drivers already drain driver-actionable work after a timer tick ([ADR-0125](0125-deadline-on-a-pending-producer-open.md)), and each loop iteration begins by taking `poll_transmit_owned` and `poll_timeout` under the connection lock, so the re-subscribe an expiry stages is written on the next iteration, like the keepalive ping the same sweep can stage. The inbound arm runs `handle_pending_events` and the same event wake after `handle_bytes`; the timer arm does the first and the event wake, and the extra `driver_waker.notify_waiters()` of the inbound arm only re-enters the loop, which a timer tick does anyway.

## Consequences

- An unanswered seek costs `operation_timeout` instead of the life of the connection, and the consumer carries on exactly as after a rejection.
- **A reply that arrives after the expiry is ignored.** It finds no pending request, records no outcome, thaws nothing and emits no re-subscribe or flow.
  If the broker did apply the seek and only answered late, the client is in the pre-seek state while the broker's consumer was dropped by the seek, and the consumer receives nothing until something re-attaches it.
  This is the price of failing a request whose outcome is unknown; the opt-in stall watchdog ([ADR-0101](0101-consumer-stall-detection-and-in-place-recovery.md), [ADR-0103](0103-bounded-automatic-consumer-stall-recovery.md)) sees the restored positive permit balance again and can re-subscribe in place.
  What a real broker does for a seek it applied after `operation_timeout` is UNVERIFIED here; no broker is reachable from the unit and simulation layers, and a seek that slow means `operation_timeout` is set too low for the broker.
- The runtimes' wake schedule changes only while a seek is pending; the moonpool seed sweep covers it.
- `magnetar-proto` gains the public `Connection::seek_at`; no façade or runtime API changes.
- A seek issued through the clock-free `Connection::seek` can fail early, as described; the runtimes do not use it.

## References

- `crates/magnetar-proto/src/conn.rs` — `PendingRequestKind::{ConsumerSeek, ConsumerSeekForgotten}`, `seek`, `seek_at`, `pending_seek_deadline`, `expire_pending_seek`, the `poll_timeout` arm and the `handle_timeout` sweep.
- `crates/magnetar-runtime-tokio/src/consumer.rs`, `crates/magnetar-runtime-moonpool/src/consumer.rs` — `stage_seek` and the twin tests `an_unanswered_seek_fails_its_future_and_thaws_the_consumer`, `an_abandoned_unanswered_seek_thaws_the_consumer`, `an_unanswered_seek_skips_no_dispatch`, `a_pending_seek_arms_a_wake_at_its_operation_timeout` (ADR-0024, 1:1).
- `crates/magnetar-differential/tests/unanswered_seek_equivalence.rs`, `crates/magnetar-differential/tests/seek_operation_timeout_equivalence.rs` — tokio ↔ moonpool parity.
- [ADR-0133](0133-connection-owned-seek-reply-handling.md), [ADR-0125](0125-deadline-on-a-pending-producer-open.md), [ADR-0011](0011-clock-injection-sans-io.md), [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md).
