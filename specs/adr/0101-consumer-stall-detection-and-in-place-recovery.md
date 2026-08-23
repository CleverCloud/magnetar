# ADR-0101 — Make a wedged consumer detectable and recoverable from the client

- **Status**: Accepted (amended by [ADR-0103](0103-bounded-automatic-consumer-stall-recovery.md), the rejected "have the watchdog re-subscribe automatically" alternative and the "emitting the event is the only effect" clause — everything else below remains binding)
- **Date**: 2026-08-21
- **Decider**: Florentin Dubois
- **Tags**: consumer, flow-control, observability, resilience, sans-io, shared-subscription

## Context

Issue #414: a Pulsar `Shared` subscription wedged broker-side after a consumer-churn window — a cursor reset performed with consumers still attached, a 12 → 1 scale-down, and an instance recycle mid-drain.
The survivors received roughly twenty messages and then nothing, permanently.
The broker's own `availablePermits` for the subscription was observed at `-177300`, `acks_failed` was `0`, and the client raised no error of any kind.
Only a superuser `pulsar-admin topics unload` recovered the topic.

**The root cause is broker-side, and the client cannot have caused it.**
The wire protocol carries only monotonic client → broker permit increments (`CommandFlow`, `crates/magnetar-proto/src/consumer.rs`); there is no decrement on the wire, so no sequence of client behaviour drives the broker's counter negative.
The client's own mirrors are zeroed in lock-step at every churn boundary — full reconnect reset, same-broker `CommandCloseConsumer` (the ADR-0069 / issue #307 arm), and terminal subscribe failure — so client-side drift is not a candidate either.

What issue #414 exposes is not a client bug but three client-side gaps, each of which turns a broker fault into an invisible, unrecoverable one:

1. **No detection.** `Connection::consumer_available_permits` — and the `Consumer::available_permits()` chain on both engines above it — read `ConsumerState::granted_permits`, the purely-ADDITIVE grant mirror. [ADR-0082](0082-consumer-permit-balance-split.md) split that field from the real decrementing `permit_balance` for issue #349 but deliberately left this accessor on the additive one, recording the change as "a separate, unscoped change" (`specs/adr/0082-consumer-permit-balance-split.md`, §Decision and §Consequences). The additive value never moves under dispatch: it reads `receiver_queue_size` forever whether the broker is streaming or has gone silent. An application polling it cannot distinguish a healthy consumer from a dead one.
2. **No signal.** ADR-0058's connection keepalive watchdog cannot see this. It ages `last_activity` off every decoded inbound frame, and a broker whose dispatcher has wedged for ONE subscription keeps answering `PING` with `PONG` — so the baseline never ages, no connection-level deadline fires, and the connection is by every measure healthy.
3. **No cheap recovery.** Issue #307 built exactly the right machinery for a consumer whose broker-side dispatcher slot went bad — zero the permit mirrors, re-emit `CommandSubscribe` for the same consumer id, defer the initial `CommandFlow` to the broker's `Success` — but wired it to exactly one trigger: an inbound same-broker `CommandCloseConsumer` with `assigned_broker_service_url = None`. Issue #307's own re-arm on `CommandActiveConsumerChange` never fires for a `Shared` subscription; the broker only sends that command for `Failover`. A `Shared` consumer therefore had no self-healing path and no caller-driven one either. The only lever was `topics unload`, which is superuser-only and disrupts every other subscription on the topic.

The scripted broker in `magnetar-differential` could not even express the failure: it kept per-session state with one ledger cursor per consumer, so two `Shared` consumers on one subscription each walked the whole ledger independently.

### Alternatives considered

- **Add a sibling accessor** (`available_permit_balance()`) and leave `available_permits()` additive. Rejected: it leaves the Java-parity name (`ConsumerBase#getAvailablePermits`, which IS a decrementing counter) on the wrong field, and every existing caller — including the parity matrix's own claim — keeps reading the value that cannot detect the fault.
- **Have the watchdog re-subscribe automatically.** Rejected: a broker hiccup would become a re-subscribe storm across every partition simultaneously, and it would hide the broker-side defect that issue #414 is actually about. A signal the operator can correlate and act on is worth more than an automatic action with no diagnosis.

  > **Amended by [ADR-0103](0103-bounded-automatic-consumer-stall-recovery.md).**
  > This rejection stands verbatim for the unconditional, unbounded form it was written about, and it stands as the shipped default.
  > ADR-0103 admits the automatic re-subscribe only opt-in (`ConnectionConfig::consumer_stall_auto_recovery`, default `None`) and only bounded — at most one attempt per stall episode, at most `max_attempts` per stall streak, the budget resetting only on a real dispatch unit — and the `ConsumerStalled` event plus its `warn!` are still emitted on every episode, so the diagnosis is not hidden.
  > With the knob unset, this clause is unchanged down to the byte: the watchdog reports and puts nothing on the wire.

- **Extend ADR-0058's connection keepalive to notice per-consumer silence.** Rejected: the keepalive's whole design is one connection-wide baseline refreshed by any inbound frame. Making it per-consumer means a per-consumer baseline, which is this ADR's watchdog under a misleading name.
- **Ship the watchdog armed by default.** Rejected — see §Decision.

## Decision

### 1. `available_permits()` reports the real, decrementing balance

`Connection::consumer_available_permits` now reads `ConsumerState::permit_balance`: the grants issued, minus one per broker dispatch unit that has actually arrived (plain message, batch member, buffered chunk, PIP-33 marker), force-zeroed at every churn boundary.
Both engines' `Consumer::available_permits()` and the façade's `ConsumerApi::available_permits` inherit it unchanged, since they delegate.

This is a deliberate **semantic change**, not an addition.
It makes the accessor mean what Java's `ConsumerBase#getAvailablePermits` means, and it makes a value pinned at the receiver-queue size while messages stop arriving a usable client-side signature of the #414 wedge.

`ConsumerState::granted_permits` keeps its additive semantics and its two callers — the issue #307 failover-reflow gate and the `adjust_receiver_queue` want-have delta — both of which ask "how much have we told the broker it may use", which is exactly what an additive mirror answers correctly.

### 2. A per-consumer stall watchdog, off by default, event-only

`ConnectionConfig::consumer_stall_timeout: Option<Duration>` (façade: `ClientBuilder::consumer_stall_timeout`, `Duration::ZERO` disables).
When set, `Connection::handle_timeout` emits one `ConnectionEvent::ConsumerStalled { handle, permit_balance, stalled_for }` per stall episode for a consumer that, for the whole window, held un-spent broker permits over an empty receive queue in a dispatch-eligible state without a single dispatch unit arriving.

The machine is progress-based, the ADR-0058 shape scoped to one consumer:

- `ConsumerState::dispatch_units_received` is a monotonic `u64` bumped by `record_dispatch_unit`, the single helper that also decrements `permit_balance` — one call so a future dispatch site cannot update one and forget the other. It carries no clock, so no dispatch site needed a `now` parameter (ADR-0011).
- `ConsumerState::stall_watch` holds the open window: the progress mark latched when it opened, the injected instant it opened at, and a `reported` latch. `poll_stall(window, now)` seeds it, advances it, and returns `Some(silence)` on exactly the one tick that closes an episode.
- The window is discarded whenever candidacy ends (a queued message, a pause, a seek, end-of-topic, a terminal failure, a re-attach in flight) and at every grant site (`initial_flow`, `maybe_flow`, `adjust_receiver_queue`'s growth branch). A fresh grant is a fresh promise from the broker and deserves a fresh window, not an inherited start instant.
- `next_stall_deadline` is surfaced through `Connection::poll_timeout`, so a driver wakes for the sweep deterministically rather than opportunistically on an unrelated deadline — the seed-divergence rule ADR-0011 and the `next_adjust_deadline` / chunk-expiry arms already follow.
- `Connection::initial_flow` **seeds** the window at grant time (`arm_stall_watch(now)`), alongside the `arm_adjust_clock(now)` bootstrap it already performed. It is the only grant site handed an injected clock, and a full grant is exactly where a wedge begins — the broker acknowledged permits it will never spend. Without it `poll_timeout` has no instant to arm from until some other deadline produces a first sweep, and on an otherwise idle connection that is the keepalive: detection would take `consumer_stall_timeout + keepalive_interval` instead of `consumer_stall_timeout`, and a client that opened a consumer and got nothing would wait 30 s longer than the window it configured.

**The knob defaults to `None`.** Two reasons, both precedented in this tree: an armed deadline perturbs the moonpool engine's simulated wake schedule even when it never fires (the rationale `ack_response_timeout` and `stats_interval` both carry on their own doc comments), and [ADR-0089](0089-client-driven-rate-window-sampling.md) deliberately landed its sweep off so the flip could be its own bisectable commit with its own seed sweep.
There is no Java counterpart to inherit a parity default from — the Java client has no per-consumer dispatch watchdog.
`Duration::from_secs(30)` is the documented recommended production value: it matches the keepalive and ack-response cadences, so a stall is judged on the cadence at which every other silence on the connection already is.

**Emitting the event is the only effect.**
Recovery stays explicit, per the alternatives above.

> **Amended by [ADR-0103](0103-bounded-automatic-consumer-stall-recovery.md)** for the opt-in case only.
> When `ConnectionConfig::consumer_stall_auto_recovery` is set, the same `handle_timeout` sweep also drives a bounded number of `resubscribe_consumer_in_place` calls for the reporting consumer.
> The event is emitted either way, and with the knob unset — the default — this clause is unchanged.

### 3. `Connection::resubscribe_consumer_in_place`, and `Consumer::resubscribe()` on both engines

The issue #307 same-broker re-attach becomes callable.
`resubscribe_consumer_in_place(handle)` runs the same three steps in the same order the `CommandCloseConsumer` arm runs them:

1. zero `granted_permits`, `permit_balance`, `consumed_since_flow`, and drop any open stall window — the broker recreates its dispatcher slot at `availablePermits = 0`, so the mirrors must follow;
2. fail every in-flight ack for the handle (the issue #346 sweep) — their responses can never arrive against the retired consumer generation;
3. re-emit `CommandSubscribe` for the SAME consumer id and defer the initial `CommandFlow` to its `Success`, because Pulsar silently drops flow for a consumer id whose subscribe is still being processed (`ServerCnx.handleFlow`, "Couldn't find consumer").

Eligibility is checked **before** anything is mutated, and the refused set is exactly the states that have another owner: closed, unsubscribing, terminally failed, mid-seek (the seek's own re-attach owns the next `CommandSubscribe`), or a re-attach already in flight.
Nothing is touched on the refusal path — zeroing the mirrors for a consumer we then decline to re-subscribe would leave it strictly worse off than the stall it was in.
The receiver queue is left intact, so already-buffered messages stay receivable (the #65 / `duringSeek` invariant).

The three shared steps are factored out of the `CommandCloseConsumer` arm into `emit_in_place_consumer_resubscribe`, `consumer_reattach_in_place_is_eligible`, and `fail_acks_orphaned_by_consumer_reattach`; the #307 behaviour is byte-identical.

### 4. The differential broker models a real Shared dispatcher

`magnetar-differential`'s scripted broker gains `SharedDispatcher`, keyed by `(topic, subscription)` — the same stable-identity-key precedent the issue #406 `(topic, producer_name)` registry set.
One cursor per subscription, round-robin over attached consumers holding permits, and a detaching consumer's un-acked in-flight entries returned to a redelivery pool the survivors drain ahead of the cursor.
Non-`Shared` subscriptions keep the historical per-consumer walk verbatim, so every pre-existing golden trace is unchanged.

## Consequences

- **An application can now detect the #414 wedge.** Poll `available_permits()`: a balance that stops falling while the broker's backlog is non-empty is the signature. Arm `consumer_stall_timeout` and the client reports it for you, once per episode, with the un-spent balance and the silence duration.
- **`available_permits()` returns different numbers than before.** A caller that read it as "the cumulative grant" gets the un-spent balance instead. Three tests pinned the old arithmetic and were updated to the new — each to an exact value, not a loosened one; two of them (`consumer_flow_control_edge.rs`, both engines) became strictly stronger, since "every grant minus every dispatch" pins the dispatch side the cumulative form ignored. `ConsumerState::granted_permits` remains available for a caller that genuinely wants the cumulative grant.
- **The watchdog reports silence, not fault.** A consumer that has drained its backlog on an idle topic satisfies the predicate exactly as a wedged one does: the client cannot see the broker's backlog, so it cannot tell them apart. That is why the event carries no verdict, why it never recovers on its own, and why the knob ships off. Correlate it with `AdminClient::topic_stats` (`subscriptions[].msgBacklog`, and the broker-truth `availablePermits`) before acting.
- **`resubscribe()` repairs this client's slot, not the dispatcher.** Issue #414's production failure was dispatcher-WIDE — `availablePermits = -177300` across every attached consumer — and one consumer re-attaching does not necessarily clear that. The escalation ladder stays: `resubscribe()`, then `pulsar-admin topics unload`. `docs/consumer-stall-recovery.md` is the operator-facing form of that ladder.
- **One extra `u64` and one `Option<StallWatch>` per consumer.** No allocation, no task, no `select!` arm — the watchdog rides the existing `poll_timeout` / `handle_timeout` deadline loop, exactly as ADR-0089's rate sampling does.
- **`resubscribe()` is on both runtime `Consumer` types, not on the façade's `ConsumerApi` trait.** Deliberate scope boundary: `ConsumerApi` is also the fan-out surface for `MultiTopicsConsumer` / `PatternConsumer`, and what a re-subscribe should mean across N children (all of them? only the stalled ones? what if one refuses?) is a product decision this ADR does not make. The runtime `Consumer` is what `ConsumerBuilder::subscribe()` hands back, so the method is reachable from the façade today without it.
- **The event is drained silently by both drivers**, per ADR-0054's single-owner rule: `magnetar-proto` holds the richest context at the point of detection and emits the `warn!` there, and the engines drain the event only so it cannot accumulate in the proto queue.

### Amends ADR-0082

[ADR-0082](0082-consumer-permit-balance-split.md) § Decision states:

> **`Connection::consumer_available_permits()`** (and the façade's `Consumer::available_permits()` chain on both engines) is intentionally left reading `granted_permits` — unchanged behaviour. Extending Java-parity semantics (a genuinely decrementing counter matching `ConsumerBase#getAvailablePermits`) to this public accessor is a separate, unscoped change; this ADR's fix is confined to the `FlowStats::available_permits` signal `Auto::adjust` consumes.

and § Consequences states:

> `Connection::consumer_available_permits()` keeps its pre-existing (additive, non-decrementing) semantics; a reader expecting Java-parity decrementing behaviour from that specific accessor will not get it from this change.

That deferral is what this ADR takes up: issue #414 is the concrete cost of it.
Both clauses are superseded — the accessor now reads `permit_balance`.
Every other ADR-0082 decision remains binding: the two counters stay split, `granted_permits` keeps its additive semantics and its two callers, `flow_stats` still feeds `permit_balance` into `FlowStats::available_permits`, and the churn-window guard on `granted_permits == 0` in `adjust_receiver_queue` is unchanged.

## References

- `crates/magnetar-proto/src/consumer.rs` — `StallWatch`, `record_dispatch_unit`, `is_stall_candidate`, `next_stall_deadline`, `poll_stall`, `clear_stall_watch`.
- `crates/magnetar-proto/src/conn.rs` — `consumer_available_permits`, the `poll_timeout` / `handle_timeout` watchdog arms, `resubscribe_consumer_in_place`, `emit_in_place_consumer_resubscribe`, `consumer_reattach_in_place_is_eligible`, `fail_acks_orphaned_by_consumer_reattach`.
- `crates/magnetar-proto/src/conn_types.rs` — `ConnectionConfig::consumer_stall_timeout`.
- `crates/magnetar-proto/src/event.rs` — `ConnectionEvent::ConsumerStalled`.
- `crates/magnetar/src/client_builder.rs` — `ClientBuilder::consumer_stall_timeout`.
- `crates/magnetar-differential/src/broker.rs` — `SharedDispatcher` and `push_pending_shared`.
- `crates/magnetar-runtime-tokio/tests/consumer_stall_recovery.rs`, `crates/magnetar-runtime-moonpool/tests/consumer_stall_recovery.rs`, `crates/magnetar-differential/tests/shared_subscription_churn_equivalence.rs`, `crates/magnetar/tests/e2e_shared_subscription_churn.rs` — ADR-0024 layers (b), (c), (d) and the e2e layer.
- `docs/consumer-stall-recovery.md` — the operator-facing detection and recovery ladder.
- [ADR-0082](0082-consumer-permit-balance-split.md) — the permit split, whose accessor deferral this amends.
- [ADR-0058](0058-keepalive-watchdog-progress-based.md) — the connection keepalive this watchdog is modelled on and deliberately does not extend.
- [ADR-0089](0089-client-driven-rate-window-sampling.md) — the deadline-on-the-existing-loop pattern, and the precedent for landing a sweep disarmed.
- [ADR-0011](0011-clock-injection-sans-io.md) — why the progress signal is a counter rather than a timestamp.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the four-layer test policy this change lands under.
