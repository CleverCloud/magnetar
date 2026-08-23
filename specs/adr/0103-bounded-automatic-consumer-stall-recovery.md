# ADR-0103 — Let the stall watchdog recover a wedged consumer, opt-in and bounded

- **Status**: Accepted (amends [ADR-0101](0101-consumer-stall-detection-and-in-place-recovery.md), the rejected "have the watchdog re-subscribe automatically" alternative and the "emitting the event is the only effect" clause)
- **Date**: 2026-08-22
- **Decider**: Florentin Dubois
- **Tags**: consumer, flow-control, resilience, sans-io, shared-subscription, failover, issue-414

## Context

[ADR-0101](0101-consumer-stall-detection-and-in-place-recovery.md) shipped the two halves of issue #414's client-side answer and deliberately left them unconnected.
The per-consumer stall watchdog detects the wedge and emits `ConnectionEvent::ConsumerStalled`; `Connection::resubscribe_consumer_in_place` (and `Consumer::resubscribe()` on both engines) repairs this client's dispatcher slot.
Nothing joins them: an application that arms `consumer_stall_timeout` still has to observe the event, decide, and call the recovery itself.

ADR-0101 rejected joining them, in these words:

> **Have the watchdog re-subscribe automatically.** Rejected: a broker hiccup would become a re-subscribe storm across every partition simultaneously, and it would hide the broker-side defect that issue #414 is actually about. A signal the operator can correlate and act on is worth more than an automatic action with no diagnosis.

Both objections are real and both are about an **unconditional, unbounded** automatic re-subscribe. Neither survives contact with an opt-in, bounded one:

- **The storm is already rate-limited by the mechanism that would drive it.** `poll_stall` returns `Some` exactly once per stall episode, and an episode cannot close more often than once per `consumer_stall_timeout` — the recommended value being 30 s. A partition set that all wedge together therefore emits at most one `CommandSubscribe` per consumer per 30 s, which is quieter than the reconnect storm the same event set would produce if the application reacted to every `ConsumerStalled` itself. What is genuinely unbounded is the _duration_: without a cap the client would keep re-subscribing once per window forever against a fault it may not be able to repair at all.
- **The diagnosis is not hidden, because the event still fires.** The `warn!` and the `ConsumerStalled` are emitted on every episode whether or not recovery acts, and each attempt logs its own `info!` with the attempt number. An operator sees strictly more than before, not less.

What tips the balance is the arithmetic of the recovery itself, which ADR-0101 states as a scope limit but does not quantify.
An in-place re-subscribe zeroes this consumer's permit mirrors, the broker recreates its dispatcher slot at zero permits, and the client answers the re-subscribe `Success` with one fresh `CommandFlow` of a full receiver-queue window.
Against a subscription whose **aggregate** permit counter has been corrupted — issue #414 observed it at `-177300` — one attempt therefore credits back exactly `receiver_queue_size`.
A leak of `L` needs `ceil(L / receiver_queue_size)` attempts: with the reported numbers and a 1000-message queue, about 178 of them.
No sane client budget reaches that, which is precisely why the correct behaviour is a small number of attempts followed by an honest escalation, not a retry loop.

`magnetar-differential`'s scripted broker could state the wedge but not produce it.
ADR-0101 gave it a real `SharedDispatcher` per `(topic, subscription)` — one cursor, round-robin over permitted consumers, detach returns un-acked entries to the survivors — but no aggregate permit counter, so there was nothing that could go negative and nothing that could stop dispatching while the survivors still held permits.

### Alternatives considered

- **Leave it caller-driven (the status quo).** Rejected as the only option, kept as the default. Every application that wants the first rung of the recovery ladder has to write the same event loop, and the event is drained silently by both engine drivers — so reaching it means driving the sans-io `Connection` directly, which is not what `ClientBuilder` users do.
- **Recover unconditionally, no bound.** Rejected: against a dispatcher-wide fault it is an infinite re-subscribe loop that never succeeds and never escalates. The bound is what makes the feature honest.
- **Reset the attempt budget at every churn boundary** (wherever the permit mirrors are zeroed). Rejected, and it is a trap rather than a preference: the recovery's own `resubscribe_consumer_in_place` is one of those boundaries, so the counter would clear itself on every attempt and the bound would not exist.
- **A separate cool-off / backoff knob between attempts.** Rejected as redundant: `consumer_stall_timeout` already is the interval. An attempt can only follow a closed episode, and an episode takes a full window.
- **A new `ConsumerStallRecoveryExhausted` event.** Rejected for now. `ConsumerStalled` already fires on the exhausted episode, and the exhaustion carries its own `warn!` with the escalation. Adding an event to make a log line programmatic is public surface this ADR does not need.
- **Arm `consumer_stall_timeout` implicitly when auto-recovery is set.** Rejected: two knobs that silently set each other are harder to reason about than one that is documented as inert without the other.
- **Put the Failover-standby skip (§4) in `consumer_reattach_in_place_is_eligible`** rather than in the auto-recovery arm. Rejected, and it would be a regression rather than mere scope creep: that helper is shared with issue #307's same-broker `CommandCloseConsumer` arm, where a broker-closed **standby** must re-attach or it never rejoins the failover group, and with `Consumer::resubscribe()`, where an application asking to re-attach a standby is making an explicit, legitimate request. Only the automatic path has a reason to decline, because only it is reacting to silence the broker is producing on purpose.

## Decision

### 1. `ConnectionConfig::consumer_stall_auto_recovery: Option<u32>`, default `None`

The maximum number of in-place re-subscribes the watchdog may drive **per consumer, per stall streak**.
The façade knob is `ClientBuilder::consumer_stall_auto_recovery(max_attempts: u32)`, where `0` disables — the same zero-disables shape `consumer_stall_timeout` and `stats_interval` already use, spelled in attempts rather than in a `Duration`.
`Some(0)` reaching `ConnectionConfig` directly needs no special case: a budget of zero fails the `attempts < max_attempts` gate on the first episode.

It is **inert without `consumer_stall_timeout`** — no window, no episode, nothing to recover from — and documented that way rather than implicitly arming the other knob.

The default is `None` for ADR-0101's reasons plus one of its own: this knob acts on the broker, and an action nobody asked for is not a default.

### 2. The recovery runs in `handle_timeout`, in the existing stall-report drain

`Connection::handle_timeout` already stages stall reports inside the per-slot consumer loop and drains them afterwards under `&mut self`, emitting the `warn!` and pushing the `ConsumerStalled`.
The recovery is appended to that same drain, which is the whole implementation:

- read this consumer's `stall_recovery_attempts` under its slot lock;
- while it is below the budget, call `resubscribe_consumer_in_place(handle)` — the identical, unchanged path `Consumer::resubscribe()` takes;
- bump the counter **only when that call returned `Some`**, and log the attempt with its number and the budget;
- when the budget is spent, log one `warn!` naming `pulsar-admin topics unload`.

Two properties fall out of putting it there rather than in the engines.

**Both engines inherit it with no per-engine bookkeeping.** The tokio and moonpool drivers already drain `ConsumerStalled` and do nothing with it (ADR-0054's single-owner rule: the proto layer holds the richest context and owns the log line). Driving recovery from those two drain arms would mean two copies of the attempt counter, free to drift; driving it from the sans-io sweep means the 1:1 test parity ADR-0024 demands is a statement about one implementation.

**Emitting frames from `handle_timeout` is established.** The issue #301 receiver-queue adjust flows and the chunk auto-ack sweep already encode commands from this drain, and every driver calls `poll_transmit` after `handle_timeout`.

An attempt that the eligibility gate refuses — closed, unsubscribing, terminally failed, mid-seek, or a re-attach already in flight — spends no budget and mutates nothing.
That matters for one state in particular: `unsubscribe_request_id` is the only refusal that is _also_ a stall candidate, so an unrelated teardown race could otherwise burn the recovery a genuinely wedged consumer still needs.

### 3. The budget resets on one broker dispatch unit, and on nothing else

`ConsumerState::stall_recovery_attempts` is zeroed in exactly one place: `record_dispatch_unit`, the single helper that already decrements `permit_balance` and bumps `dispatch_units_received`.
It is a third field on the same call for the same reason the first two share it — one dispatch unit arriving is the only definition of progress that means the broker started serving this consumer again, and a dispatch site cannot update one of the three and forget the others.

Everything else is deliberately excluded, and the exclusion is load-bearing rather than conservative.
`clear_stall_watch` and `initial_flow` are exactly what a recovery attempt performs on its way out — an attempt that zeroed the mirrors and got a `Success` back would refund itself, and the bound would silently not exist.
A re-subscribe the broker acked but never dispatched against is not progress.

### 4. A reported Failover standby is skipped, ahead of everything else

A `Failover` standby satisfies the stall predicate exactly as a wedged consumer does, and it does so permanently: it holds the initial grant the broker acknowledged at subscribe time, over an empty queue, in a dispatch-eligible state, and the broker never dispatches to it because the active consumer owns the subscription.
It is also the one shape that can never clear itself, since §3's only reset is a dispatch unit and a standby receives none.
An armed recovery therefore spends its **entire** budget re-subscribing every healthy standby in a failover group, once per window, and never gets it back — the exact failure mode the bound exists to prevent, arrived at from the other direction.

The `handle_timeout` recovery arm gains one pre-check, reading the `ConsumerState::is_active` mirror issue #348 already maintains from `CommandActiveConsumerChange`:

- **`Some(false)`** — the broker has reported this consumer as standby. Skip: no `CommandSubscribe`, no budget spent, nothing mutated, one `debug!` explaining why.
- **`Some(true)`** and **`None`** — active, or never announced, which is every `Shared` and `Exclusive` subscription plus a `Failover` one before its first announcement. Unchanged behaviour.

Only a _reported_ standby is skipped. Inferring standby-ness from anything else would put the watchdog in the business of guessing the subscription's topology, and `None` is deliberately the permissive value: the overwhelming majority of consumers never receive that command at all, and issue #414's own failure is a `Shared` subscription.

**The pre-check runs before the exhausted-budget arm.** A consumer that spent its budget while active and was then demoted therefore gets this skip rather than the `pulsar-admin topics unload` escalation — the right order, because escalation guidance about a consumer whose silence the broker is producing deliberately is misleading operator advice.

**The report is unchanged.** `ConsumerStalled` and its `warn!` still fire for a standby exactly as they did in 1.5.0. That is deliberate rather than an oversight: ADR-0101's event means _silence_, not _fault_, and a standby is genuinely silent. Suppressing it here would make the event mean something different depending on a knob the event does not carry.

### 5. The differential broker models the aggregate permit counter, and can corrupt it

`SharedDispatcher` gains `total_available_permits: i64` — the scripted analogue of the number a real broker reports as `availablePermits` for a `Shared` subscription.
It is **signed on purpose**: a `u32` could not express the failure, and a negative value is by construction a broker-side accounting fault, since the wire protocol carries only monotonic client → broker permit increments and no decrement of any kind.

It is credited by `CommandFlow`, charged one per dispatched entry, and charged the departing consumer's remaining permits on detach.
A re-registration of an already-attached consumer id zeroes that consumer's permits without crediting the aggregate back — mirroring the client zeroing its own mirrors, with the fresh full-window `CommandFlow` after the `Success` being what puts them back.
`dispatch_gate_open()` is the read gate: a dispatcher at or below zero hands out nothing regardless of what its consumers individually hold.

With correct accounting the gate can never be the deciding factor — the counter is then the sum of the attached consumers' permits plus a non-negative re-registration drift, so it is non-positive only when the round-robin scan would have found nobody anyway.
Every pre-existing golden trace is byte-identical, and the differential suite passing unchanged is the evidence.

`ScriptedBroker::leak_shared_permits_on_consumer_churn()` (off by default) makes a detach subtract the departing consumer's remaining permits **twice**.
The second subtraction removes permits no credit was ever issued for, so each churn event leaks exactly that many, permanently.
Once the aggregate crosses zero the subscription stops dispatching while its survivors still hold permits, the backlog is non-empty, and the connection stays healthy — the client-visible shape issue #414 reports.

**This is a hypothesis, not a verified reading of any Apache Pulsar source.**
It is the accounting shape that reproduces the reported signature, and `UPSTREAM-ISSUE-DRAFT.md` frames it to Pulsar's maintainers as a question about the Shared dispatcher's churn-path permit accounting rather than as a claim about a specific method.

## Consequences

- **`consumer_stall_auto_recovery` is new public API on a minor version.** One `ConnectionConfig` field, one `ClientBuilder` method. No event, no trait, no accessor: the observability is the existing `ConsumerStalled` plus three structured log lines (`info!` per attempt with `attempt` / `max_attempts`, the unchanged stall `warn!`, and the exhaustion `warn!`).
- **The exhaustion warning fires exactly once per streak, not once per window.** The last attempt is the last thing that re-arms the stall window, so with no dispatch no further episode can open. A consumer that gives up goes quiet rather than logging forever — and the moment the broker dispatches again, the budget resets and the whole ladder is available for the next wedge.
- **It cannot repair a dispatcher-wide corruption, and the bound is how it says so.** ADR-0101's scope limit is unchanged; this ADR quantifies it. One attempt buys `receiver_queue_size` of aggregate. `docs/consumer-stall-recovery.md` carries the ladder, and the exhaustion log names `pulsar-admin topics unload` in the message itself so an operator reading logs alone reaches the next rung.
- **A small budget is the right budget.** Three attempts at a 30 s window spends ninety seconds before escalating. Larger values do not become useful — the failures a single re-attach clears are cleared on the first attempt — they only delay the escalation.
- **The watchdog is still a silence detector, not a fault detector.** A consumer that has drained its backlog on an idle topic satisfies the predicate exactly as a wedged one does, so an armed budget will occasionally spend an attempt re-subscribing a perfectly healthy idle consumer. That is cheap (one `CommandSubscribe`, one `CommandFlow`, the receiver queue untouched) and self-limiting (the first dispatch resets the budget), but it is why the knob is opt-in and why the recommended window is long.
- **The differential harness can now express the broker-side fault**, which is what makes the upstream report evidence-backed rather than anecdotal: the same trace and the same leak recover under a sufficient budget and stay wedged under an insufficient one, identically on both engines.
- **One extra `u32` per consumer.** No allocation, no task, no new deadline — the recovery rides the sweep that already detected the stall.
- **Promotion and demotion need no repair, because a skip spends nothing.** The standby pre-check emits nothing and mutates nothing, so a consumer that was standby throughout arrives at promotion with its budget untouched and gets the complete ladder if it then genuinely wedges. Demotion does not refund attempts already spent while active either; §3's single reset site is unchanged, and only a dispatch unit gives the budget back — which for a promoted consumer is exactly the event that proves the broker started serving it. No transition handling was added anywhere, and that is the point: the rule is that `is_active` gates whether an attempt is _made_, never what the budget _is_.
- **Promotion does not restart the stall window, and a promoted-but-still-silent consumer stays quiet.** Issue #307's re-arm calls `initial_flow` only at `granted_permits == 0`, and ADR-0102 makes that a no-op for a consumer that already holds its grant — so no `arm_stall_watch` fires on promotion, and a report the standby already latched stays latched. The next episode opens only when candidacy is lost and regained or a fresh grant lands. This is pre-existing ADR-0101 latch semantics, unchanged by this guard and stated here only so it is not read as new behaviour: promotion is not evidence that the broker started dispatching, so it is correctly not treated as progress.

### Amends ADR-0101

[ADR-0101](0101-consumer-stall-detection-and-in-place-recovery.md) § Context, "Alternatives considered", states:

> **Have the watchdog re-subscribe automatically.** Rejected: a broker hiccup would become a re-subscribe storm across every partition simultaneously, and it would hide the broker-side defect that issue #414 is actually about. A signal the operator can correlate and act on is worth more than an automatic action with no diagnosis.

and § Decision, part 2, states:

> **Emitting the event is the only effect.**
> Recovery stays explicit, per the alternatives above.

Both are superseded **only for the opt-in, bounded form defined above**, and only when `consumer_stall_auto_recovery` is set.
The rejection stands verbatim for the unconditional unbounded form it was written about, and it stands as the default behaviour: with the knob unset, ADR-0101's contract is unchanged down to the byte — the watchdog reports and puts nothing on the wire.

Every other ADR-0101 decision remains binding: `available_permits()` still reads the decrementing `permit_balance`, the watchdog is still progress-based and still emits exactly one event per stall episode, `consumer_stall_timeout` still defaults to `None`, the eligibility set for an in-place re-attach is unchanged and still refuses without mutating, the receiver queue is still left intact, and `resubscribe()` still repairs this client's slot rather than the dispatcher — with `topics unload` as the escalation.
ADR-0101's amendment of [ADR-0082](0082-consumer-permit-balance-split.md) is untouched.

## References

- `crates/magnetar-proto/src/conn_types.rs` — `ConnectionConfig::consumer_stall_auto_recovery`.
- `crates/magnetar-proto/src/consumer.rs` — `ConsumerState::stall_recovery_attempts` and its single reset in `record_dispatch_unit`.
- `crates/magnetar-proto/src/conn.rs` — the recovery arm in `handle_timeout`'s stall-report drain, its Failover-standby pre-check, and `resubscribe_consumer_in_place`'s second caller.
- `crates/magnetar/src/client_builder.rs` — `ClientBuilder::consumer_stall_auto_recovery`.
- `crates/magnetar-differential/src/broker.rs` — `SharedDispatcher::total_available_permits`, `dispatch_gate_open`, `ScriptedBroker::leak_shared_permits_on_consumer_churn`.
- `crates/magnetar-differential/src/runner_{tokio,moonpool}.rs` — `run_with_stall_auto_recovery`.
- `crates/magnetar-proto/src/conn.rs` (`consumer_stall_and_recovery_tests`), `crates/magnetar-runtime-{tokio,moonpool}/tests/consumer_stall_recovery.rs`, `crates/magnetar-differential/tests/shared_subscription_churn_equivalence.rs`, `crates/magnetar/tests/e2e_shared_subscription_churn.rs` — ADR-0024's five layers.
- `docs/consumer-stall-recovery.md` — the operator-facing ladder, with automatic recovery as rung 0.
- [ADR-0101](0101-consumer-stall-detection-and-in-place-recovery.md) — the watchdog and the in-place re-attach this connects, and whose rejected alternative this amends.
- [ADR-0082](0082-consumer-permit-balance-split.md) — the permit split the detection signal rests on.
- `crates/magnetar-proto/src/consumer.rs` — `ConsumerState::is_active`, the issue #348 mirror the standby pre-check reads.
- [ADR-0058](0058-keepalive-watchdog-progress-based.md) — the connection keepalive the watchdog is modelled on and which cannot see a per-subscription wedge.
- [ADR-0054](0054-logging-policy.md) — the single-owner logging rule that keeps the recovery's log lines in `magnetar-proto`.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the test policy this change lands under.
