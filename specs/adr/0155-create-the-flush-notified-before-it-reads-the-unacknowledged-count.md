# ADR-0155 — Create `flush`'s `Notified` before it reads the unacknowledged count

- **Status**: Accepted
- **Date**: 2026-11-21
- **Decider**: Florentin Dubois
- **Tags**: producer, flush, notify, lost-wakeup, tokio, moonpool

## Context

`Producer::flush` waits for the producer's unacknowledged count to reach zero.
Both engines parked it on `ConnectionShared::driver_waker` in the order read, then create, then park: `pending_count()` (which takes the connection lock and releases it), then `driver_waker.notified()`, then `enable()`, then `.await`.

The driver ends every inbound packet with `driver_waker.notify_waiters()` (`crates/magnetar-runtime-tokio/src/driver.rs` and `crates/magnetar-runtime-moonpool/src/driver.rs`, after `handle_pending_events`).
`notify_waiters()` stores no permit: it wakes only the `Notified` futures that exist when it runs.
No lock covers the span between the end of `pending_count()` and the creation of the `Notified`, so the driver could apply the last `CommandSendReceipt` and pulse inside it.
A `flush` that had read `1` then created its `Notified` after the pulse, parked with nothing left to acknowledge, and stayed pending until the next inbound frame (a keep-alive away), or for as long as the connection stayed quiet.

The repository already treats this shape as a defect class and closes it by arming before the check (`ARCHITECTURE.md`, "Enroll-before-drain wakeup discipline"; `Client::await_reconnect_or_terminal` in both engines).
`flush` was not on that list.
The cost is latency, not a hang: any later inbound frame, or a `notify_one` from another user-facing future, wakes the parked flush, which then reads zero.

## Decision

`flush` creates its `Notified` at the top of every loop iteration, before it reads the count, and awaits that same future when the count is not zero:

```rust
loop {
    let notified = self.shared.driver_waker.notified();
    if self.pending_count() == 0 {
        return Ok(());
    }
    notified.await;
}
```

- `Notify::notified()` stores the number of `notify_waiters()` calls made so far in the future it returns, and the first poll completes at once when that number has moved.
  A pulse that lands after the read, whether before or after the first poll, is therefore observed.
  This was read in the locked tokio (1.54.1, `src/sync/notify.rs`: `notified()` and the `State::Init` arm of `poll_notified`).
- The future is not `enable()`d before the read.
  The `State::Init` arm of `poll_notified` takes a stored `notify_one` permit before it registers a waiter, and a `Notified` that took one is `Done`, so dropping it forwards nothing (`drop_notified` forwards only from `State::Waiting`).
  The driver parks on the same `Notify` and relies on such a permit when a `notify_one` reaches it while it is busy (a `send`, a `close`, or the `notify_one` that follows `flush_producer`).
  Enabling ahead of the read would let a flush that finds nothing to wait for take a permit stored by another task's `notify_one` and return, and the driver's next `notified()` would then not complete: it would sleep until its next wake with that task's frame staged.
  This is where `flush` differs from the create-and-`enable()` rule of `ARCHITECTURE.md`; `Client::await_reconnect_or_terminal`, which also parks on `driver_waker`, keeps its `enable()` and is not changed here.
- The park is unchanged otherwise: the same `Notify`, the same wake source (the driver's pulse after each inbound packet, ADR-0137), the same re-check after every wake.
  The loop creates a new `Notified` after each wake, so there is no gap between a wake and the next read either.
- Both engines change identically, and `flush_with_timeout` (which wraps `flush` in a timer) inherits the fix.

## Consequences

- A `flush` whose last receipt is applied between its count and its park resolves at that receipt instead of at the next inbound frame.
- No new lock, `Notify`, dependency, wire change or public API, and the lock order (global → per-slot, ADR-0038) is untouched: the count is read exactly as before.
- Not changed, and not claimed: when the count is not zero the first poll of the `Notified` still takes a stored `notify_one` permit, exactly as the old `enable()` did, and a parked `flush` still competes with the driver for a `notify_one` that reaches `driver_waker` while both are enrolled.
  Whether that costs the driver a wake depends on `tokio::sync::Notify`'s waiter order and is not established by this repository; a dedicated `Notify` for `flush` would remove it and would need a pulse at every site that pulses `driver_waker` today.
- Not changed, and not claimed: the send-timeout sweep still does not pulse `driver_waker` (`docs/follow-ups.md` §32).

### ADR-0024 test layers

(a) `magnetar-proto` is not changed, so it has no unit test.
(b) `magnetar-runtime-tokio/src/producer.rs` `tests::flush_is_not_stranded_by_a_pulse_between_its_count_and_its_park`; (c) the same test in `magnetar-runtime-moonpool/src/producer.rs`.
Both race the window with real threads (no lock sits in it, so a test cannot pin the flush inside it) and are a bounded race: they fail against the old order and cannot fail against the new one, but a host with few CPUs can pass them against the old order.
(d) No `magnetar-differential` scenario is added.
`producer_flush_reconnect_equivalence.rs` reaches `flush`, but its two engines are driven through one scripted broker, and the defect is an interleaving between two statements of one task and the driver that no script can place; a scenario written for it would pass against the old order on both engines.
The end-to-end layer is not added: a real broker reaches the path whenever a receipt arrives while a `flush` is between its read and its park, but not deterministically, and nothing under `crates/magnetar/tests/` can hold it there.

## References

- `crates/magnetar-runtime-tokio/src/producer.rs` and `crates/magnetar-runtime-moonpool/src/producer.rs` — `Producer::flush`.
- `crates/magnetar-runtime-tokio/src/driver.rs` and `crates/magnetar-runtime-moonpool/src/driver.rs` — the `notify_waiters()` pulse after each inbound packet, and the `driver_waker` arm.
- `ARCHITECTURE.md` — "Enroll-before-drain wakeup discipline".
- [ADR-0137](0137-count-the-reset-snapshot-in-producer-flush-and-pending-count.md) — the wake source `flush` waits on and the count it reads.
- [ADR-0038](0038-split-connection-mutex.md) — the lock order the count read follows.
