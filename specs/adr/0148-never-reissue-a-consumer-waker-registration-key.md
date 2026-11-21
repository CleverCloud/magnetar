# ADR-0148 — Never reissue a consumer waker registration key

- **Status**: Accepted
- **Date**: 2026-11-20
- **Decider**: Florentin Dubois
- **Tags**: consumer, waker, receive, active-change, state-machine, tokio, moonpool, sans-io

## Context

`ConsumerState` parks the futures that wait on one consumer in two per-consumer waker sets: `receive_wakers` for every in-flight `receive()` (and the scalable-topics deferred receive) and `active_change_wakers` for every `next_active_change()`.
Both were a `Slab<Waker>`, and the future held the raw slab key it was given as `Option<usize>`.
A message arrival, a recorded transition, a close, an end-of-topic and a reset **drain the whole set** (`wake_receivers`, `wake_active_change_waiters`, and the direct drains in `Connection::reset`, `Connection::fail_all_pending`, `Connection::fail_consumer_subscribe_with_broker_error`, the end-of-topic arm of `handle_frame` and both runtimes' `park_batch_failure`), and `Slab::drain` empties the entries and rewinds its next-key to `0`, so the first registration after a drain receives key `0` again.

Each woken future still holds the key it was given before the drain, and cancels it on its next poll (`ReceiveFut::poll` and `ActiveChangeFut::poll` of both engines cancel the stored key before they register, after a successful pop and on a terminal result) and on `Drop`.
`cancel_receive_waker` and `cancel_active_change_waker` checked only that the slot was occupied.
The sequence that follows needs no unusual timing, only that a woken task does not run first, which a multi-thread executor does not order:

1. F1 and F2 park on the consumer under keys 0 and 1.
2. A message arrives, the drain wakes both, and the slab is empty.
3. F2 runs first, pops the message, resolves, and its task calls `receive()` again (F2'), which finds the queue empty and registers: the slab hands F2' key `0`.
4. F1, woken by the same arrival, polls with its stale key `0`, finds the queue empty and cancels it: the slab removes F2''s waker.
5. F1 registers again.
   The next arrival wakes F1 only, and the task of F2' is parked for good while messages keep flowing through F1 — permanently, once F1's task stops receiving.

A future dropped by `select!` or `JoinHandle::abort` between the wake and its re-poll reaches the same removal from `Drop`.
Concurrent `receive()` on clones of one consumer is a documented, supported pattern, and `next_active_change()` is its exact mirror, so both sets had the defect.

[ADR-0111](0111-share-one-memory-limit-controller-per-client.md) met the same mechanism in the client-wide memory waiters and fixed it by identity: `MemoryWaiterId`s drawn from a monotonically increasing counter and never reused, held in a `BTreeMap`, so cancelling a stale id after a drain removes nothing.
The per-consumer sets were not moved to it.

Alternatives considered:

- **Keep the slab and verify ownership on cancel** (compare the registered waker with `Waker::will_wake`).
  Rejected: `cancel_*` receives no waker, so the signature and both engines' futures change, and two futures of one task share a waker, so the check cannot tell them apart.
- **Stop draining and wake by reference.**
  Rejected: the sets would keep a woken future's waker until its next poll, `receive_wakers.len()` would no longer be the number of futures awaiting a wake, and every direct drain site would change its meaning.
- **Bump a generation at every drain site.**
  Rejected: a drain is written at more than ten sites, in `magnetar-proto` and in both runtimes through the public field, and a new one that forgot the bump would reintroduce the defect silently.
- **Make the key a struct carrying the slot and a registration counter.**
  Rejected: it changes the `usize` the three futures of each engine store, for the same effect as a key that is never reissued.

## Decision

`receive_wakers` and `active_change_wakers` are now a `ParkedWakers` (`magnetar_proto::consumer`): a `BTreeMap<usize, Waker>` plus a counter that only moves forward.

- `insert(waker)` returns the counter's value as the key and advances it with `wrapping_add`.
- `remove(key)` removes that key if it is present and is a no-op otherwise.
- `drain()` takes every waker, in registration order, and **never rewinds the counter**.
- Reads (`len`, `is_empty`, `contains_key`) are the map's own through `Deref`, so the existing `receive_wakers.len()` assertions and every `receive_wakers.drain().collect()` site compile unchanged.
  No mutation reaches the map except through the three methods, so a key cannot be minted outside the counter.

A key therefore names one registration, forever: cancelling it after a drain, cancelling it twice, and cancelling it after the same future registered again are all no-ops, and none can remove the entry of another registration.
The key stays a `usize`, so `Connection::register_consumer_receive_waker`, `Connection::cancel_consumer_receive_waker`, their active-change twins and the `slab_key: Option<usize>` fields of the futures keep their signatures and neither runtime source changes.
The wake order is registration order, which keeps the moonpool simulation reproducible per seed, as in ADR-0111.

- Not changed, checked: `close_waiters` (`ConsumerState::complete_close` and the terminal arm of `Connection::fail_all_pending`) and `ConsumerUnsubscribeOperation::waiters` are drained once, on the transition to a terminal state (`ConsumerTerminalOperation::Complete`, or a completed unsubscribe) that never reverts and that every `ConsumerCloseFut` / `ConsumerUnsubscribeFut` poll resolves from before it registers, so no future registers after the drain and no stale key can meet a reissued one.
  The transaction client's `pending_*` slabs are keyed per pending request, removed with `try_remove` when the reply arrives, and never drained as a set.
- The counter wraps instead of overflowing.
  A wrap can only meet a registration that is still parked, which needs 2^64 registrations on a 64-bit target, and on a 32-bit one 2^32 registrations with no arrival, transition or close in between, each of which drains the set.

## Consequences

- A `receive()` or `next_active_change()` woken by a drain and polled late no longer evicts the waker of a sibling that registered after the drain, so the next arrival wakes every parked future and a task is no longer left parked beside a queued message.
- The keys of one set are unique for the life of the consumer slot instead of being recycled.
  The cost is memory churn: a registration is a `BTreeMap` insertion, which allocates a node on the first registration after a drain, where the slab kept its capacity.
  That is on the path of a `receive()` that finds the queue empty, not of one that pops a ready message.
- `ConsumerState::receive_wakers` and `active_change_wakers` change type from `Slab<Waker>` to `ParkedWakers`.
  `len`, `is_empty` and `drain` keep their meaning for callers; the `Slab`-only methods (`contains`, `try_remove`, indexing) are gone, and `drain` returns an iterator of the wakers.
- Incompatible with: code that relied on a drained set handing key `0` out again, which was the defect.

### ADR-0024 test layers

Receive: (a) `magnetar-proto/tests/receive_waker_stale_key.rs`; (b) `magnetar-runtime-tokio/tests/receive_waker_stale_key.rs`; (c) `magnetar-runtime-moonpool/tests/receive_waker_stale_key.rs`; (d) `magnetar-differential/tests/receive_waker_stale_key_equivalence.rs`.
Active change: (a) `magnetar-proto/tests/active_change_waker_stale_key.rs`; (b) and (c) the tests of the same name in the two runtimes; (d) `magnetar-differential/tests/active_change_waker_stale_key_equivalence.rs`.
Each pair drives the same `Connection` calls the engines' futures make, in the order that triggers the eviction.
The end-to-end layer is not added in this change.
A real broker could reach the path — two tasks calling `receive()` on clones of one consumer on a multi-thread runtime — but the order that evicts depends on the executor polling the woken tasks in a particular order, which no broker script can force, so the poll order is driven directly.

## References

- `crates/magnetar-proto/src/consumer.rs` — `ParkedWakers`, `ConsumerState::register_receive_waker`, `cancel_receive_waker`, `register_active_change_waker`, `cancel_active_change_waker`.
- `crates/magnetar-proto/src/conn.rs` — `Connection::register_consumer_receive_waker`, `cancel_consumer_receive_waker` and the active-change twins.
- `crates/magnetar-runtime-tokio/src/consumer.rs` and `crates/magnetar-runtime-moonpool/src/consumer.rs` — `ReceiveFut`, `DeferredReceiveFut`, `ActiveChangeFut`.
- [ADR-0003](0003-no-channels-rule.md) — the waker pattern this keeps.
- [ADR-0111](0111-share-one-memory-limit-controller-per-client.md) — the same identity rule for the client-wide memory waiters.
