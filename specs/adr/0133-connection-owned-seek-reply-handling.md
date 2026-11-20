# ADR-0133 — Handle a seek's reply in the connection, not in the awaiting future

- **Status**: Accepted
- **Date**: 2026-11-19
- **Decider**: Florentin Dubois
- **Tags**: consumer, seek, cancellation, flow-control, tokio, moonpool, sans-io
- **Amends**: [ADR-0102](0102-grant-the-initial-consumer-flow-once-per-attach.md) and [ADR-0107](0107-refund-the-flow-permit-of-a-dead-lettered-dispatch-unit.md)

## Context

`Connection::seek` freezes the consumer when the request is issued: `ConsumerState::begin_seek` sets `pending_seek`, clears the receiver queue and zeroes `manual_flow_permits`, `granted_permits`, `permit_balance` and `consumed_since_flow`, and `deliver` queued nothing for the application while the seek was in flight, dropping every dispatch as a pre-seek message.
Two things lifted that freeze or acted on the reply, and both were fragile.

- The `CommandSuccess` arm called `seek_acked`, but the `CommandError` arm only recorded an `OpOutcome::Error`.
  A seek the broker rejects — `resetCursor` failing, a fenced subscription (`PersistentSubscription.resetCursorInternal` fails with `SubscriptionBusyException` before it disconnects anyone), "Consumer not found" — left `pending_seek` set forever.
  Every later delivery was dropped, `maybe_flow` returned `None`, and both the stall watchdog and the in-place recovery refuse a consumer with a pending seek, so nothing else would ever take it.
- The broker drops the consumer while it applies a seek, and the only code that re-subscribed it and granted flow was the awaiting runtime future's continuation.
  A `seek_*` future dropped by a `timeout`, a `select!` or an aborted task before the reply left a consumer attached to nothing, holding no permits, with a permit balance of zero that excludes it from every stall candidacy (ADR-0101).

The broker disconnects a consumer before it resets the cursor, and a reset that then fails answers the seek with `CommandError`, so a rejection can find the consumer either still held (fenced or busy) or already gone.

Java's `ConsumerImpl` (apache/pulsar `c1aaf96108`) shows the shape a rejection must leave: `messageReceived` has no seek check, `clearIncomingMessages` runs only when the seek succeeds, and a failure only calls `failSeek`, which completes the seek future exceptionally and lets the consumer carry on.

## Decision

The connection owns the reply to a seek; the awaiting future owns only the happy path it already had.

- **Rejection restores and replays, it does not re-grant and it does not skip.**
  `begin_seek` moves the queue and the four mirrors into `ConsumerState::seek_rollback` instead of discarding them, once per chain of seeks (a second `begin_seek` folds what the live mirrors recorded since the freeze into the snapshot).
  While the seek is in flight `deliver` holds each dispatch in that snapshot as a `HeldDispatch` — the owned `CommandMessage`, metadata, broker-entry metadata, payload and arrival instant — in arrival order, instead of dropping it, because whether it is stale is the broker's answer to the seek.
  The hold is a memory guard sized by the window the broker holds (the snapshot's `permit_balance` plus what was granted since the freeze), counted in dispatch units with at least one per frame; the frame that crosses the bound is still admitted, because a broker's dispatcher may overshoot its permits by the tail of one batch entry, and what follows is dropped with its units counted (`dropped_units`), charged at the rollback on both sides of the ledger — `permit_balance` down, `consumed_since_flow` up, the same pair every other refund site writes — so they are repaid and never re-granted.
  `CommandError` for the seek currently in flight (`pending_seek == Some(request_id)`, so the reply to an older seek cannot thaw a newer one) restores the snapshot, adds onto it the writes the live mirrors took since the freeze (a manual `flow`, an aggregate's, the refund of a PIP-33 marker; a marker dispatched while frozen debits the snapshot's `permit_balance` directly, because a debit against the zero live balance would saturate and vanish), lets `maybe_flow` repay what is due, and returns the held dispatches.
  The connection delivers them in order behind the restored queue through `deliver_to_consumer`, the body the live `CommandMessage` arm now shares, so each unit is accounted by the ordinary `deliver` path, each `Message` event is emitted once, and nothing is skipped: on an Exclusive or Failover subscription a later cumulative acknowledgement would otherwise cover messages the application never saw.
  Re-running `initial_flow` would pass its `granted_permits == 0` gate and stack a full window on the one the broker still holds, the double grant of issue #427.
  A seek the broker applies discards the snapshot and the hold together (`seek_acked`): the old cursor sent them.
- **A consumer the broker dropped mid-seek is re-attached.**
  A `CommandCloseConsumer` (same-broker or migration) clears the snapshot, and so does the acknowledgement of an in-place recovery close (ADR-0108) that lands after the application seeked, because the broker removed the consumer either way.
  A rejection with no snapshot then re-attaches the consumer in place with `SubscribeAckAction::ReleaseFlow` and its original resume position, the re-attach the close arm had left to the seek.
  Mid-seek, a migration close followed by a rejection therefore re-subscribes on the current socket and drains the `ConsumerClosedByBroker` event, as the awaited `Success` path already does.
- **A dropped seek future hands the seek to the connection.**
  Both runtimes' seek futures carry a drop guard calling `Connection::abandon_consumer_seek_waiter(handle, request_id)`, idempotent and keyed by request id.
  A seek still awaiting its reply is retagged `PendingRequestKind::ConsumerSeekForgotten`: its `Success` calls `seek_acked` and re-subscribes with `ReleaseFlow`, its `Error` runs the rejection recovery above, and neither records an outcome.
  A future dropped after the `Success` landed but before it was polled is covered by `ConsumerState::seek_reattach_owed`, set by the `Success` arm and cleared by the re-subscribe.
  Waker presence is not the signal: `stage_seek` and `complete_staged_seek` are separate, and the aggregate consumer stages every child seek before the first poll, so a live seek can have no waker.
  The corollary is that a staged aggregate seek dropped before `complete_staged_seek` is first polled has no guard, and needs none: the aggregate's own resynchronization covers it (`cancelled_aggregate_seek_requests_resynchronization`).
- **The connection-owned re-attach does not redeliver, and withdraws the flow release for a zero-queue consumer.**
  It follows the phase-2 subscribe-waiter hand-off and issue #307's in-place re-attach, which also release flow alone; a zero-queue consumer is credited by its owner and a `CommandFlow` for zero permits is a protocol error.
  The awaiting continuation is unchanged and still follows its acknowledgement with `redeliver_unacked_all`.
- `reset` clears the snapshot, the hold, the marker and the forgotten request; `fail_all_pending` and `reset` consume a forgotten seek without an outcome, like `ConsumerCloseForgotten`.

## Consequences

A rejected seek is invisible to the consumer apart from the error the caller sees: its queued messages are receivable again, the dispatches the broker sent while the seek was in flight follow them in order, and its flow window is the one the broker holds.
The price is memory: a frozen consumer holds the queue it had plus at most the window of dispatches that arrive, both bounded by what the broker was granted, where the earlier freeze held neither.
A cancelled seek no longer needs the caller to re-seek or the transport to reset.
`ConsumerState` gains two fields and the connection one request kind and one delivery helper; the lock order is unchanged (the snapshot is moved under the slot lock, user wakers run after it is released, and the replay takes the slot lock once per frame like a live delivery).
The aggregate consumer closes and rebuilds its children on a failed or cancelled seek and may now see a re-attach or a refund frame on a child it is about to close, which the broker orders before that close.
A forgotten seek records no outcome, which also closes the permanent `outcomes` entry a cancelled seek left behind for the pre-reply drop.

## References

- `crates/magnetar-proto/src/consumer.rs` — `begin_seek`, `seek_acked`, `seek_rejected`, `deliver`'s hold, `SeekRollback`, `HeldDispatch`, `SeekRejection`.
- `crates/magnetar-proto/src/conn.rs` — the `Success` and `Error` arms, `abandon_consumer_seek_waiter`, `reattach_after_unowned_seek`, `recover_consumer_after_rejected_seek`, `deliver_to_consumer`.
- `crates/magnetar-runtime-tokio/src/consumer.rs`, `crates/magnetar-runtime-moonpool/src/consumer.rs` — `SeekAbandonGuard`.
- [ADR-0101](0101-consumer-stall-detection-and-in-place-recovery.md), [ADR-0102](0102-grant-the-initial-consumer-flow-once-per-attach.md), [ADR-0107](0107-refund-the-flow-permit-of-a-dead-lettered-dispatch-unit.md), [ADR-0108](0108-close-then-resubscribe-for-in-place-consumer-recovery.md).
