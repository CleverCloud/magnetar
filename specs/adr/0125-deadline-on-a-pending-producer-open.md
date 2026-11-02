# ADR-0125 — Give a pending `ProducerOpen` an `operation_timeout` deadline

- **Status**: Accepted
- **Date**: 2026-11-02
- **Decider**: Florentin Dubois
- **Tags**: producer, recovery, sans-io, timeouts, bundle-unload, issue-887

## Context

[Issue #887](https://github.com/CleverCloud/magnetar/issues/887): magnetar 1.7.2 in production on the tokio engine.
A `PartitionedProducer` on a 12-partition topic lost its publisher on 2 of 12 partitions; broker stats confirmed magnetar held no publisher there.
Neither the reconnect supervisor, nor `auto_update_partitions_interval`, nor a broker-side `unload` of the whole topic re-created them, and every publish the round-robin router sent to those two partitions failed `send rejected: code=-1 message=send timeout` — roughly one message in six — for as long as the process lived.
Two other partitions on the same broker kept their publishers and recovered from that unload within two seconds.
The preceding log carried `send timed out while relocated across reconnect` from `magnetar_proto::conn`.

This is exactly the residual [ADR-0106](0106-reattach-broker-closed-producer-in-place.md) recorded and declined to fix:

> **A pending `ProducerOpen` still carries no deadline** — `handle_timeout` sweeps only `PendingRequestKind::Ack`.
> A re-attach the broker never answers leaves the slot at `broker_ready = false, open_request_id = Some(_)`, reproducing the old symptom and making later closes ineligible.
> Shared with the #307 consumer path and with every reconnect rebuild; recorded, not fixed here.

The mechanism, verified at `f8b7465`:

- A producer slot's lifecycle is the tuple `(closed, broker_ready, has_ever_attached, open_request_id)` on `ProducerState` (`crates/magnetar-proto/src/producer.rs:183` ff), mirrored into `ProducerSlot::routing_ready` for the lock-free routing read.
- `Connection::handle_timeout` swept exactly one `PendingRequestKind` — `Ack`, the issue #346 `ack_response_timeout` backstop. A pending `ProducerOpen` had no deadline in `poll_timeout` and no sweep in `handle_timeout`.
- `open_request_id` was written only by `emit_command_producer` and cleared only by the `ProducerSuccess` arm. `Connection::reset` takes `pending_requests` wholesale but never clears it.
- So a `CommandProducer` the broker never answered parked the slot at `broker_ready = false, open_request_id = Some(r)` permanently, and every recovery path was then gated off:
  - the in-place re-attach of ADR-0106 refuses `open_request_id.is_some()` (`producer_reattach_in_place_is_eligible`), so a later `CommandCloseProducer` took the ineligible branch, which shuts the drain gate and arms no retry;
  - the per-slot ADR-0080 retry leg is armed only from a `CommandError` naming a currently-pending open id;
  - `PartitionedProducer::refresh_partitions` only swaps an `AtomicU32` count and notifies — it creates no child, by its own documentation.
- It explains the asymmetry in the report: on the unload the two wedged slots were ineligible and silently stayed shut, while the two healthy slots on the same broker re-attached in place within two seconds.

The discriminator against the other candidate mechanism — a slot permanently removed from `producer_create_requests` — is the user-visible error string.
A removed slot has `closed = true`, so `ProducerSlot::queue_send_reserved` maps the refusal to `ProtocolError::InvariantViolation("producer rejected send")`.
The report says `send timeout`, which is `SEND_TIMEOUT_MESSAGE` paired with `SEND_TIMEOUT_CODE = -1`, so the wedged pending-open slot is the mechanism and the removed slot is not.

Alternatives considered:

- **Thread a `now: Instant` into the emit path.** `GUIDELINES.md` puts the injected clock last in the argument list, which would be the textbook shape. It also means a signature change on `create_producer`, `rebuild_producers`, `retry_producer_open` and `retry_producer_open_if_current` — roughly 200 call sites across the workspace (159 outside `conn.rs` alone) for one timestamp, on four surfaces that are not clock-injected entries in the first place.
- **A dedicated `producer_open_timeout` knob.** `ConnectionConfig::operation_timeout` already documents itself as the total budget for "producer-open or subscribe acknowledgement" and already bounds the runtime's first-attachment open loop. A second knob would let the two disagree.
- **Sweep opportunistically, with no `poll_timeout` arm.** The sweep would then ride an unrelated tick — up to a whole keepalive interval late in production, and seed-divergent under the moonpool engine.

## Decision

Give a pending `CommandProducer` the connection's existing `operation_timeout` as its deadline, measured on the injected clock, and dispose of its expiry through the ADR-0080 legs that already exist.

- `PendingRequestKind::ProducerOpen` carries `enqueued_at: Option<Instant>`, taken from the connection's `last_activity` — the timestamp the last decoded frame handed in — at `emit_command_producer` time.
  Every established re-attach site is triggered by a frame that just refreshed it (`Connected` → `rebuild_producers`, `LookupTopicResponse` → the retry leg, `CloseProducer` → the in-place re-attach), so the baseline is the re-attach's own instant there.
  `None` before any frame is decoded arms nothing, the same "no instant to anchor to" shape `seed_rate_window_baseline` has.
  No new clock reaches `magnetar-proto`, and no public signature changes.
- `Connection::pending_producer_open_deadline` is the single predicate behind both halves: `poll_timeout` arms exactly what `handle_timeout` sweeps.
  A deadline computed on one side alone would either never fire or, worse, fire forever — `poll_timeout` returning an elapsed instant that the sweep refuses to clear spins the driver loop.
- The deadline arms only for a producer that `has_ever_attached`.
  A first attachment belongs to the routing-aware client open loop, which bounds it with its own provider-backed `operation_timeout` timer and tears the slot and the request down through `cancel_producer_open`; its `last_activity` baseline can also legitimately be a whole keepalive interval old, because `create_producer` is user-driven rather than frame-driven.
  The wedge of issue #887 is an established slot by construction — the reconnect rebuilds, the ADR-0080 retry leg and ADR-0106's in-place re-attach all require it.
- On expiry, `Connection::expire_pending_producer_open` removes the request, shuts the drain gate, records `last_open_error`, bumps `transient_open_attempts` under the same slot lock, and then lets `OperationRetryConfig::should_retry_after_failure` choose: one more `DriverRetry::Producer` leg plus its public `ProducerOpenFailedTransient` twin, or `fail_producer_open_with_broker_error`.
  Nothing new supervises, retries, or backs off; the sweep is a synthetic transient open failure fed into the `CommandError` arm's own machinery.
- The expiry carries `PRODUCER_OPEN_TIMEOUT_CODE = -1` and `PRODUCER_OPEN_TIMEOUT_MESSAGE = "producer open timeout"`.
  Same `-1` sentinel as the send timeout — Pulsar's `ServerError` has no timeout variant — under a deliberately distinct string, so an expired attachment is tellable apart from an expired publish in the next bug report.
- `open_request_id` is deliberately **left** at the expired generation, exactly as the `CommandError` arm leaves it.
  It is the retry leg's liveness token: `producer_open_retry_is_current` requires `open_request_id == Some(failed_request_id)` before `retry_producer_open_if_current` re-emits, so clearing it would arm a leg that then refuses to fire and leave the slot with neither a pending open nor a retry — a quieter version of the same wedge.
  In-place re-attach eligibility returns when the retry's own `CommandProducerSuccess` clears the id, or when the budget runs out and the slot is terminalized.
- Both engines' driver loops drain driver-actionable work after a timer tick, not only after an inbound frame.
  Until now every retry was raised by a decoded `CommandError`, which always arrived through the inbound arm; an expiry is raised by the timer, and leaving the drain out parks its retry leg until the next inbound frame — one whole keepalive interval away on the silent broker that caused it.

## Consequences

- The production symptom is bounded. A `CommandProducer` nobody answers costs `operation_timeout` plus one ADR-0080 backoff per attempt instead of a permanent per-partition publish outage, and the slot either re-attaches or terminalizes within the configured budget (eight re-issues by default).
- **A terminalized partition still needs the application to act.** Once the budget is exhausted the child producer is dropped and its publishes resolve `Err` rather than hanging; `PartitionedProducer` does not re-create a child, which remains follow-up #19's cross-broker re-home decision. The change is from a silent permanent timeout to a surfaced bounded failure, not to transparent recovery.
- The `send timeout` string is no longer the only `-1` a wedged producer can produce. A pending open that expires reports `producer open timeout`; the publishes it fails on the terminal path keep their own reason.
- One armed deadline is added per pending producer open on a connected session. It exists only inside the re-attach window and `operation_timeout` has no disabled form, so unlike `ack_response_timeout` the arm is unconditional. The moonpool wake schedule changes accordingly, which is why the seed sweep is part of this changeset's evidence rather than a follow-up.
- The baseline can be up to one keepalive interval behind the actual emit when a public `retry_producer_open` is driven by a bare `magnetar-proto` caller with no intervening frame, which makes the deadline fire early rather than late. Early is safe: it spends one accounted attempt and re-issues.
- `expire_pending_producer_open` restates the `CommandError` arm's budget-and-disposition shape rather than sharing one helper with it. The arm's own code additionally classifies a broker error code and reclaims a busy producer name, neither of which a timeout has; factoring the common middle out of a 90-line arm is a refactor this changeset did not take. The two must stay in step if ADR-0080's policy changes.
- The consumer twin is untouched: a pending `CommandSubscribe` still carries no deadline. That is the remaining half of follow-up #20 and the #307 path ADR-0106 named alongside this one.
- A broker that answers every re-attach promptly sees no behaviour change at all, which is what the e2e layer asserts.

## References

- `crates/magnetar-proto/src/conn.rs` — `PendingRequestKind::ProducerOpen`, `pending_producer_open_deadline`, the `poll_timeout` arm, the `handle_timeout` sweep, `expire_pending_producer_open`, and the two proto unit tests.
- `crates/magnetar-runtime-tokio/src/driver.rs`, `crates/magnetar-runtime-moonpool/src/driver.rs` — the timer arm's driver-event drain.
- `crates/magnetar-runtime-tokio/tests/producer_open_deadline.rs`, `crates/magnetar-runtime-moonpool/tests/producer_open_deadline.rs` — the two engine legs, 1:1 (ADR-0024).
- `crates/magnetar-differential/tests/producer_open_deadline_equivalence.rs` — tokio ↔ moonpool parity for the armed deadline, the swept request, the attempt accounting and both dispositions.
- `crates/magnetar/tests/e2e_producer_open_deadline.rs` — a real `topics unload` under a short `operation_timeout`: the deadline must not pre-empt a re-attach the broker answers.
- [ADR-0106](0106-reattach-broker-closed-producer-in-place.md) — the in-place re-attach whose recorded residual this closes for the producer path.
- [ADR-0080](0080-configurable-operation-retry-policy.md) — the retry budget, backoff and terminalization this expiry feeds into.
- [ADR-0011](0011-clock-injection-sans-io.md) — why the baseline is a handed-in instant rather than a clock read.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the layered test policy this change ships against.
