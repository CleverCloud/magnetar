# ADR-0104 — Make ordinary consumer close a cancellation-safe singleflight

- **Status**: In-flight
- **Date**: 2026-08-12
- **Decider**: Florentin Dubois
- **Tags**: consumer, close, cancellation, tokio, moonpool, sans-io
- **Amends**: [ADR-0003](0003-no-channels-rule.md), [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md), and [ADR-0038](0038-split-connection-mutex.md)

## Context

Each `Consumer::close` call previously allocated a new request and waited through the connection-wide destructive `OpOutcome` map.
Dropping one waiter removed the operation's waker and any result that had raced with cancellation, so one clone could cancel another clone's reliable close.
Scalable resynchronization and aggregate teardown can close the same ordinary child concurrently, which also emitted duplicate `CommandCloseConsumer` frames and could flush grouped acknowledgements repeatedly.

## Decision

An ordinary consumer slot owns one lifecycle-long terminal operation with `Open`, close `Pending`, reusable close `Complete`, and `UnsubscribePending` states, plus slabs of independently cancellable close and unsubscribe waiter wakers.
Close pending and completion retain their `Reliable` or `BestEffort` admission origin.
Connection admission holds the global lock and then the slot lock, flushes grouped acknowledgements once, allocates and stages one request, marks the consumer closed, and installs `Pending` atomically with respect to every other admission.
Later reliable, best-effort, scalable, and aggregate close calls reuse the pending request or cached completion.
Close and unsubscribe admission are mutually exclusive under global-then-slot locking: close pending rejects unsubscribe, unsubscribe pending rejects close, close complete rejects unsubscribe as closed, and rejected admission allocates no request and stages no frame.
Unsubscribe owns a reusable slot operation retained by its runtime future, so success, rejection, reset, and terminal failure publish even with zero current waiters and a future first polled later resolves immediately.
An unsubscribe rejection or reset restores `Open`; unsubscribe success publishes and wakes before removing the slot after its request wins the lifecycle guard.

Tokio and Moonpool use dedicated close and unsubscribe futures.
Each future registers one waker under its operation lock and removes only that waiter on drop; cancellation never removes request correlation or completion.
Correlated success, broker error, session reset, and terminal connection failure publish a cloneable internal completion to the slot and drain its waiters, which are woken after the slot guard is released.
The public runtime error types remain non-`Clone`.
`Connection::fail_all_pending` first latches its terminal reason while holding the global connection mutex, before sweeping pending work or terminalizing open consumer slots.
Close, unsubscribe, and forced-close retry admission consult that proto-owned latch under the same mutex, before even requiring the consumer to remain in the registry and before allocating a request, encoding a frame, or mutating retry state; post-terminal calls resolve as `PeerClosed`, while `reset` remains reconnectable and never sets or clears the latch.
The latch therefore closes the interval between `fail_all_pending` and the runtime's later `no_driver` publication: admission on either side of that runtime publication observes the proto terminal state and cannot enter behind the final sweep.

Broker-originated uncorrelated `CommandCloseConsumer` remains a transient detach and reattachment signal and never completes a client close.
A forced best-effort cleanup may emit one distinct forgotten retry only after a `Reliable` close cached a failure; a failed `BestEffort` close, `Open`, unsubscribe-pending, close-pending, and successful-close states cannot use this surface, and the retry gate is consumed at most once per consumer lifecycle.

## Consequences

Concurrent clones observe one reliable broker operation and the same reusable result even when any subset of waiters is cancelled.
Grouped acknowledgement ordering and global-before-slot locking remain unchanged.
The slot retains one small completion value for the consumer lifetime.

Proto tests cover admission, grouped acknowledgement ordering, reusable success/error/session-lost/terminal outcomes, unsolicited broker close separation, and the forced retry gate.
Mirrored Tokio and Moonpool tests cover exact request identity, waiter cancellation, cached completion, shared broker and peer-close errors, forced retry count, and exact unsubscribe reset or terminal failure before first future poll under a hang guard; those tests construct the dedicated future directly because the public async method performs admission on first poll and cannot expose the deterministic admission-to-first-poll seam.
The direct ordinary-close differential case compares concurrent close traces and asserts one exact broker-observed consumer/request correlation per two callers across both engines, including that the consumer id was bound to the trace's topic and subscription when the close arrived; the existing aggregate concurrent-close and malformed-compressed resynchronization cases exercise ordinary children through both engines, and the existing façade end-to-end close coverage exercises the public broker round trip; extending those existing unit and differential targets is more deterministic than adding a duplicate Docker scenario.
