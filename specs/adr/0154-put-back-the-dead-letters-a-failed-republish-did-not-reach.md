# ADR-0154 — Put back the dead letters a failed republish did not reach

- **Status**: Accepted
- **Date**: 2026-11-21
- **Decider**: Florentin Dubois
- **Tags**: consumer, dead-letter, cancellation, tokio, moonpool, sans-io
- **Amends**: [ADR-0104](0104-aggregate-dlq-republish-semantics.md)

## Context

`Consumer::republish_dead_letters` and `republish_dead_letters_with_properties` take the consumer's whole dead-letter buffer with `drain_dead_letter` (a `std::mem::take`) before their first await, then publish each message to the DLQ producer and ack the original, one at a time.
A `?` on the first failing publish or ack, or a future dropped at any of those awaits, dropped the rest of the taken messages with the local iterator.

Those messages were then in no place that could hand them back:

- they are out of `dead_letter_pending`, so a retry saw an empty buffer and returned `Ok(0)`, and `drain_dead_letter` returned nothing;
- they were never in the receiver queue or the unacked tracker, because a dead-lettered dispatch unit bypasses both and its flow permit is refunded at routing time ([ADR-0107](0107-refund-the-flow-permit-of-a-dead-lettered-dispatch-unit.md));
- they were not acked, so the broker kept each of them unacked against this consumer, invisible to the application, until the next reconnect redelivered them or the consumer closed.

The documented contract only promised that already-republished messages stay republished.
[ADR-0104](0104-aggregate-dlq-republish-semantics.md) calls the runtime drain "destructive", but only to rule out cross-call deduplication in the aggregate; it does not accept the loss of unattempted messages.
The residual bound of `docs/follow-ups.md` §18 (an application that never drains stops at the broker's `maxUnackedMessagesPerConsumer`) got worse, because messages lost this way could not be drained at all.

Putting the messages back is not free of its own defect.
`Connection::reset()` clears `dead_letter_pending` together with the receiver queue because the broker redelivers every unacked message to the new session, and the most likely reason for a republish to fail is that same reset (its `ack` resolves with `SessionLost`).
A put-back that ignored the reset would leave every such message buffered twice, once as the stale copy and once as the redelivered one, and the next republish would publish both to the DLQ.

## Decision

A message is owed to the application until its publish to the DLQ producer has returned `Ok`; a republish that stops early puts back everything it still owed.

- **`Connection::restore_dead_letter(handle, drained_in_epoch, unprocessed)`** inserts the messages at the front of the consumer's `dead_letter_pending`, in their original order, ahead of whatever was classified as dead-letter in the meantime.
  It writes the buffer directly and does not route the messages through `classify_and_queue`, which would refund their flow permit and count them in `total_msgs_dead_lettered` a second time.
  It does nothing when `drained_in_epoch` is not the connection's current `session_epoch`, and nothing for an unknown consumer.
  `drain_dead_letter` is unchanged, and so is its public contract for applications that drain and republish themselves.
- **Both runtimes hold the taken messages in an `OwedDeadLetters` guard** (`magnetar-runtime-tokio` and `magnetar-runtime-moonpool`, one mirror each).
  `OwedDeadLetters::take` reads `session_epoch` and drains the buffer in the same connection-lock section.
  The republish loop works on a clone of the oldest owed message and calls `settle()` once its publish returned `Ok`; the guard's `Drop` hands whatever is left to `restore_dead_letter`.
  One exit mechanism therefore covers the failing publish, the failing ack and the dropped future.
  The `Drop` takes the connection lock and, inside it, the slot lock, in the global-then-slot order of ADR-0038; it runs from a future's drop or return, never under either lock.
- **The message whose publish failed, or was in flight when the future dropped, goes back**; the one whose publish succeeded does not, even if its ack then fails.
  `Consumer::ack` stages the `CommandAck` as the very next step after the publish (when it is called in the tokio engine, on its first poll in the moonpool engine, with no await in between), so a future dropped after the publish still acks it, and re-publishing a message already on the DLQ would only duplicate it.
  If that ack fails the message stays unacked at the broker and is redelivered after the next reconnect, as before; this ADR does not change that.
- **A publish in flight when the future was dropped may still reach the DLQ**, so that one message can be republished twice.
  The pipeline is at-least-once (the original stays unacked until a republish completes), and the alternative is the loss this ADR removes.

## Consequences

A failed or cancelled `republish_dead_letters*` leaves its buffer as the application would have had to rebuild it by hand: a retry republishes the same messages in the same order, and `drain_dead_letter` shows them.
A concurrent `drain_dead_letter` or `republish_dead_letters*` on a clone of the consumer still takes the buffer as a whole while a call is in progress and sees only what was classified after it; the restored messages reappear ahead of those when the first call stops.
`MultiTopicsConsumer::republish_dead_letters` and the façade delegate to the per-consumer helper, so each child is covered; its "no rollback of completed children" and "no cross-call deduplication" statements are unchanged.
`reconsume_later*` takes its message from the caller, so the caller still holds it and it has no equivalent defect.

The cost is one clone of an `IncomingMessage` per republished message (a refcount for the payload and the metadata) and a lock round for the guard's drop.

Left open, deliberately:

- A reset between the take and the drop discards the taken messages rather than buffering them again; nothing is lost, because the broker redelivers them, but they reappear only when it does.
- `docs/follow-ups.md` §18 (the unbounded, never auto-drained buffer) is unchanged.

A real broker reaches every path above: a DLQ topic that rejects a publish (a closed producer, a schema or authorization refusal, `send_timeout`), a connection that drops mid-republish, and a caller that wraps the call in a `timeout`.
The loss is internal state, observable only through `drain_dead_letter` and the broker's unacked counters, so the layers below assert it directly and no end-to-end test was added.

## References

- `crates/magnetar-proto/src/conn.rs` — `restore_dead_letter`, `drain_dead_letter`, `reset`, `session_epoch`.
- `crates/magnetar-runtime-tokio/src/consumer.rs`, `crates/magnetar-runtime-moonpool/src/consumer.rs` — `OwedDeadLetters`, `republish_dead_letters_with_properties`.
- Tests: `crates/magnetar-proto/tests/dead_letter_restore.rs`; the `republish_dead_letters_*` tests of both runtimes' `consumer::tests`; `crates/magnetar-differential/tests/dead_letter_republish_error_equivalence.rs`.
- [ADR-0038](0038-split-connection-mutex.md), [ADR-0104](0104-aggregate-dlq-republish-semantics.md), [ADR-0107](0107-refund-the-flow-permit-of-a-dead-lettered-dispatch-unit.md), [ADR-0152](0152-retire-the-waiter-of-a-request-whose-future-was-dropped.md).
