# ADR-0152 — Retire the waiter of a request whose future was dropped

- **Status**: Accepted
- **Date**: 2026-11-21
- **Decider**: Florentin Dubois
- **Tags**: producer, consumer, cancellation, outcomes, memory, tokio, moonpool, sans-io
- **Amends**: [ADR-0057](0057-producer-last-clone-drop-close.md) and [ADR-0111](0111-share-one-memory-limit-controller-per-client.md)

## Context

`Connection::outcomes` holds the `OpOutcome` of a request-id-correlated operation until a future calls `take_outcome`, and nothing else removes it except `cancel_request`.
A reply arm that records an outcome for a request nobody awaits any more therefore parks one entry for the life of the connection.
Two sources of such entries were open, each one entry per occurrence:

- **A request future dropped before its reply.**
  The producer `RequestFut::drop` only unregistered the waker, and the consumer one also discarded an outcome that had already landed and relinquished a pending ack (`Connection::abandon_ack`); neither touched any other pending request, so the reply was recorded afterwards by the `Success` and `Error` arms (a producer close, which under `timeout(producer.close())` is the usual case), the `GetSchemaResponse` arm and the `GetLastMessageIdResponse` arm.
  An awaited ack was already covered: the consumer `RequestFut` is built when `Consumer::ack` stages the ack, so a future dropped unpolled runs its `Drop`, and `abandon_ack` leaves the request pending without an owner.
  The last of these did not even check that the request was still pending, so a reply to a request `cancel_request` had retired — whose documented contract is "a late broker reply is ignored" — was recorded all the same, and so was a reply to an id this connection never issued.
- **`Connection::reset()`.**
  It fails every pending request with `SessionLost`, including a producer open and a consumer subscribe.
  Nothing takes those two: the runtimes wait for `ProducerReady` / `SubscribeAcked` (or their failures) through events and the attachment state, and the driver discards the request ids `rebuild_producers` and `rebuild_consumers` return.
  Each reset with an open or a subscribe in flight cost two permanent entries.

`cancel_request` cannot be the fix for the first source.
It removes the pending kind, and two reply arms read that kind after the answer: the `Success` arm's `fail_sends_of_closed_producer` for `ProducerClose`, which resolves the sends a closed producer still has in flight (ADR-0111), and the seek recovery.
Waker presence cannot be the signal either: an ack is staged before its future's first poll, so a live request can have no waker ([ADR-0133](0133-connection-owned-seek-reply-handling.md) makes the same point for seeks).

## Decision

The connection owns what happens to a request whose future is gone; the future's `Drop` says so.

- **`Connection::abandon_request_waiter(request_id)`** is called by the `Drop` of the producer and consumer `RequestFut`s in both runtimes.
  It unregisters the waker, discards an outcome that landed before the drop, and then acts on the kind still pending:
  - `ProducerClose` is retagged `ProducerCloseForgotten`, never removed: the `Success` arm still fails the producer's pending sends, and neither arm records an outcome.
  - `Ack` has its waiter relinquished with `abandon_ack`, which the `AckResponse` arm, `reset` and `fail_all_pending` already consume in place (`has_waiter: false`).
  - `GetSchema` and `ConsumerGetLastMessageId` are removed, as `cancel_request` would.
  - `ConsumerSeek` is left alone: `SeekAbandonGuard` owns its hand-off and the re-attach it carries, and its documented order-independence against the request future is unchanged.
  - A request that is no longer pending (answered, retired, never issued) is a no-op, so the call is idempotent and harmless after a normal completion.
- **The `GetLastMessageIdResponse` arm records only for a request that is still pending as `ConsumerGetLastMessageId`**, as the `GetSchemaResponse` arm already did.
  A reply to a request that is not pending — retired by `cancel_request` or `abandon_request_waiter`, failed by a `reset`, or never issued — is ignored.
- **`reset()` records no `SessionLost` for a `ProducerOpen` or a `ConsumerSubscribe`.**
  It still removes and wakes any waker for the id, so nothing that could be parked is stranded; no waiter exists for these kinds on the outcome slab.
- **The producer `RequestFut` holds a `RequestId` rather than a `PendingOpKey`**, as the consumer one already does: `Drop` needs the id, and no request future of either holds a `Send` key.
- The `warn!` for a rejected `ProducerCloseForgotten` no longer says the producer was dropped: it also covers a `close()` future that was.

## Consequences

A cancelled `close()`, `get_schema()` or `last_message_id()` costs the connection nothing once the broker answers, where each used to leave an entry for good.
A cancelled `close()` still resolves the sends its producer had in flight when the broker acknowledges it, and a cancelled close that the broker rejects fails them as well, exactly as an awaited one does.
A reset costs no entry for the opens and subscribes it retires.

The cost is that the broker's answer to an abandoned close is no longer visible to anyone but the `warn!` on a rejection; that is what the caller chose by dropping the future.

Left open, deliberately:

- `fail_all_pending` still records `Terminal` for a pending open and subscribe.
  It runs once, when the connection is terminal and admits nothing new (`terminal_admission_reason`), so the entries are bounded by what was pending then and die with the shared state, where a reset repeats for the life of the connection.
- `SendFut` outcomes (`PendingOpKey::Send`) are untouched (`docs/follow-ups.md` §23).

A real broker reaches every path above (a `timeout(producer.close())` whose ack arrives later), but the retained entry is internal state with no public accessor, so an end-to-end test could only observe it as memory growth; the layers below assert it directly.

## References

- `crates/magnetar-proto/src/conn.rs` — `abandon_request_waiter`, `reset`, the `GetLastMessageIdResponse`, `GetSchemaResponse`, `Success`, `Error` and `AckResponse` arms.
- `crates/magnetar-runtime-tokio/src/{producer,consumer}.rs`, `crates/magnetar-runtime-moonpool/src/{producer,consumer}.rs` — `RequestFut::drop`.
- Tests: `crates/magnetar-proto/tests/{abandoned_request_waiter,request_outcome_retention}.rs`; the producer and consumer `tests` modules of both runtimes (`cancelled_close_*`, `cancelled_get_schema_*`, `cancelled_last_message_id_*`, `dropped_request_future_discards_*`); `crates/magnetar-runtime-{tokio,moonpool}/tests/request_outcome_retention.rs`; `crates/magnetar-differential/tests/{abandoned_request_waiter,request_outcome_retention}_equivalence.rs`; `crates/magnetar-differential/tests/abandoned_close_rejection_log_contract.rs`, which reads back the `warn!` of a rejected close nobody awaits for both ways of losing its waiter (a dropped `close()` future and the producer's last-clone drop guard).
- [ADR-0057](0057-producer-last-clone-drop-close.md), [ADR-0111](0111-share-one-memory-limit-controller-per-client.md), [ADR-0133](0133-connection-owned-seek-reply-handling.md).
