# ADR-0107 — Refund the flow permit of a dead-lettered dispatch unit

- **Status**: Accepted
- **Date**: 2026-09-17
- **Decider**: Florentin Dubois
- **Tags**: consumer, flow-control, dead-letter, java-parity, issue-437

## Context

The broker charges one permit per dispatch unit.
`org.apache.pulsar.broker.service.Consumer#sendMessages` debits `ackedCount - totalMessages`, and a unit the client will route to its dead-letter buffer is inside `totalMessages` — the broker has no idea what the client intends to do with the entry.

`ConsumerState` mirrors that debit correctly.
`classify_and_queue` (`crates/magnetar-proto/src/consumer.rs`) calls `record_dispatch_unit()` unconditionally, before the queued-versus-dead-lettered branch, so `permit_balance` falls by exactly one per arriving unit either way.

What the client did not do was hand the permit back.
`consumed_since_flow` — the counter `maybe_flow` compares against `flow_threshold()`, i.e. `max(receiver_queue_size / 2, 1)` — has exactly one writer, `record_broker_permit_consumed()`, and only three callers reached it: `pop_message`, the incomplete-chunk buffer in `deliver`, and `record_marker_consumed`.
A dead-lettered unit is never queued, so it is never popped, and no later path compensated.
`Connection::drain_dead_letter` is a bare `std::mem::take`, `Connection::ack` never touches the flow ledger, and both runtimes' `republish_dead_letters_with_properties` is drain, send, ack.

So each dead-lettered unit was one permit the broker had spent that the client never re-granted: a one-way drift of exactly the dead-letter count, recovered only at a churn boundary that zeroes both mirrors (session reset, same-broker `CommandCloseConsumer`, in-place resubscribe).
A receiver queue's worth of poison drove `permit_balance` to zero with `consumed_since_flow` still at zero, and every recovery mechanism declined:
`maybe_flow` was unreachable because nothing was left to pop, the issue #414 stall watchdog requires `permit_balance > 0` for candidacy so `is_stall_candidate` was false and ADR-0103's automatic recovery never fired, and `is_flow_starved` — shipped by the issue #443 starved-reflow fix (PR #444) — is read only by the Failover promotion re-arm and `Connection::initial_flow`, both of which are churn or election boundaries a `Shared` subscription never reaches on its own.
The broker showed `availablePermits=0` and `msgRateOut=0` with no error and no reconnect, and `Consumer::available_permits()` read `0`.

The two precedents for the correct shape were already in the same file.
`record_marker_consumed` and the incomplete-chunk path both call `record_broker_permit_consumed()` **and** `record_dispatch_unit()`, for the same stated reason: the broker spent a permit on an entry the client will never hand to the application.
The dead-letter branch was the only debit site without the matching refund.

Java has no such gap, and refunds in both dispatch shapes.
`ConsumerImpl.messageReceived` (apache/pulsar master `2c3133a5`, lines 1555-1565), on `redeliveryCount > deadLetterPolicy.getMaxRedeliverCount()`, calls `redeliverUnacknowledgedMessages(Collections.singleton(id))`, then — under the comment "The message is skipped due to reaching the max redelivery count, so we need to increase the available permits" — calls `increaseAvailablePermits(cnx)` and returns without enqueueing.
`receiveIndividualMessagesFromBatch` (lines 1811-1866) counts an over-threshold batch member into `skippedMessages` and ends with `increaseAvailablePermits(cnx, skippedMessages)`.
`increaseAvailablePermits(ClientCnx, int)` (lines 1961-1973) adds the delta and sends `CommandFlow` when `available >= getCurrentReceiverQueueSize() / 2 && !paused` — the same half-queue trigger and the same pause gate `maybe_flow` already has.
Neither Java site is subscription-type-conditional: only the DLQ **republish** (`processPossibleToDLQ`) is `Shared`/`Key_Shared`-specific, so magnetar diverting on every subscription type is not a delivery divergence.
Ack-set-cleared positions are deliberately kept out of `skippedMessages` (lines 1819-1826), which is the ADR-0105 case and stays exempt here.

Alternatives considered and rejected:

- **Refund at drain time** (`drain_dead_letter`).
  User-driven and optional, needs a flow-emission path outside the frame handler, and the session reset clears `dead_letter_pending` so the count is lost across a redial.
  Diverges from Java.
- **Refund at ack time.**
  Acks are not 1:1 with dispatch units (cumulative, batch-index) and a drained dead letter may never be acked at all.
- **Stop debiting `permit_balance` for a dead-lettered unit.**
  Wrong: the broker did spend the permit, and `available_permits()` would over-report.
  ADR-0105 exempts only positions the broker never charged.
- **Widen the issue #443 `is_flow_starved` exits to `Shared`.**
  A symptom patch that re-grants a whole window instead of the exact deficit, which is the issue #427 double-grant class.

## Decision

`ConsumerState::classify_and_queue`'s dead-letter branch calls `self.record_broker_permit_consumed()` as its first statement, before `total_msgs_dead_lettered` and `dead_letter_pending.push(msg)`.

- **One site, one line.**
  `record_dispatch_unit()` stays unconditional above the branch: it owns `permit_balance`, the issue #414 progress mark, and ADR-0103's attempt reset, all of which a dead-lettered unit genuinely satisfies.
  `classify_and_queue` remains the single place that decides a unit's fate.
- **No new emission site.**
  `conn.rs`'s `Message` arm already calls `consumer.maybe_flow()` after every `deliver`, whatever the outcome, and stages the `CommandFlow`, so the inbound frame that pushes `consumed_since_flow` across `flow_threshold()` carries the grant out — including a batched entry whose members all dead-letter, because the batch loop calls `classify_and_queue` per member before returning.
  `maybe_flow` already honours `closed` / `pending_seek` / `paused`, matching Java's `!paused` gate.
  The mutation happens under the already-held slot lock and takes no connection lock, so ADR-0038's ordering is untouched, and `saturating_add` cannot panic (invariant #6).
- **The invariant.**
  On every frame path, `granted window == permit_balance + consumed_since_flow + queue.len()`.
- **The per-outcome refund rule**, now uniform: `Delivered` is refunded at pop; `Buffered` — incomplete chunk and dead letter — is refunded at buffering; `Dropped` (a pre-seek straggler, a malformed or duplicate chunk) moves neither side of the mirror; and an ADR-0105 ack-set-cleared position moves neither side, because the broker never charged it.

## Consequences

- **`is_flow_starved` is downgraded to defence-in-depth** and its predicate is unchanged.
  No well-formed wire frame can reach that state any more: every unit the broker charges is refunded the moment the client decides its fate.
  What the predicate still covers is the second cause the issue #443 fix (PR #444) named — a broker-side debit the client mirror missed — so it stays, and its twin tests (`crates/magnetar-runtime-{tokio,moonpool}/tests/failover_starved_reflow.rs`) now manufacture starvation by zeroing `permit_balance` on the slot directly instead of by feeding dead-letter frames.
  Their previous fixture asserted `"dead-lettered dispatch must not replenish flow on its own"`, which pinned this defect as expected behaviour; that assertion is deleted.
- **The accidental backpressure is gone.**
  A poison-heavy topic with a dead-letter policy keeps dispatching, and `dead_letter_pending` — an unbounded `Vec<IncomingMessage>` — grows until the application calls `drain_dead_letter` or `republish_dead_letters`.
  Nothing in the façade auto-drains it.
  Bounding or auto-republishing that buffer is a product decision (Java has no client-side buffer at all) and is recorded in `docs/follow-ups.md`, not decided here.
- **Permits alone no longer wedge the consumer** — worded precisely, because a dead-lettered unit stays UNACKED at the broker until `republish_dead_letters` acks it.
  An application that never drains still stops at the broker's per-consumer unacked-message limit (`maxUnackedMessagesPerConsumer`).
  This ADR removes the permit wedge and nothing else.
- **Operator-visible behaviour change on upgrade.**
  A `Shared` subscription that had parked at `availablePermits=0` resumes draining, and its DLQ topic may receive a burst.
  The behaviour is Java-identical; it belongs in the release notes.
- Ships the ADR-0024 five layers: proto unit tests in `consumer.rs` and a `conn.rs` wire-level module, the tokio/moonpool 1:1 twins `dead_letter_flow_refund.rs`, the differential `dead_letter_flow_refund_equivalence.rs`, and `e2e_dlq_flow_survives_a_receiver_queue_of_dead_letters`.
  The differential harness gained three additions mirrored in both runners: `max_redeliver_count` on `Op::OpenSharedConsumer`, `Op::NackShared`, and `Op::DrainDeadLettersShared` with `Event::DeadLettersDrained`.
  The scripted broker needed no change — it already counts redeliveries per entry, stamps the count on re-dispatch, and gates dispatch on the consumer holding a permit.

### Amendments

ADR-0082 § Decision says `permit_balance`'s decrement sites are deliberately not routed through `record_broker_permit_consumed`, "which only tracks the pop-driven `consumed_since_flow` counter".
That parenthetical is amended: `consumed_since_flow` is not pop-driven, it is refund-driven, and it has four writers — pop, incomplete chunk, PIP-33 marker, and dead letter.
ADR-0082's actual decision, that the two counters stay independent and that a dispatch site updates both explicitly, is unchanged and is what this ADR follows.

ADR-0105 § Decision says "`consumed_since_flow` only moves on `pop_message`, so a position that never entered the queue never enters the flow ledger".
That is amended to the rule this ADR states: a position the broker never charged is never refunded, and a unit the broker charged is refunded when the client decides it will never be popped.
ADR-0105's own behaviour is unaffected — an ack-set-cleared position is never charged, so it is still never refunded.

This ADR also retro-records the rationale of the issue #443 starved-reflow fix (PR #444) (`is_flow_starved` routed into both `initial_flow` gates), which shipped with no ADR entry of its own.

## References

- `crates/magnetar-proto/src/consumer.rs` — `classify_and_queue`, `record_broker_permit_consumed`, `record_dispatch_unit`, `maybe_flow`, `is_flow_starved`.
- `crates/magnetar-proto/src/conn.rs` — the `Message` arm's `maybe_flow()` call (the emission site) and `dead_letter_flow_refund_tests`.
- `crates/magnetar-differential/src/trace.rs`, `runner_tokio.rs`, `runner_moonpool.rs` — the three harness additions.
- `ARCHITECTURE.md` § Permit accounting, `docs/consumer-stall-recovery.md`, `docs/follow-ups.md`.
- [ADR-0076](0076-conserve-flow-permits-across-chunk-reassembly.md), [ADR-0082](0082-consumer-permit-balance-split.md), [ADR-0101](0101-consumer-stall-detection-and-in-place-recovery.md), [ADR-0102](0102-grant-the-initial-consumer-flow-once-per-attach.md), [ADR-0103](0103-bounded-automatic-consumer-stall-recovery.md), [ADR-0105](0105-read-the-delivered-batch-index-ack-set.md).
