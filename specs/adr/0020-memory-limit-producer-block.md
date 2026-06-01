# ADR-0020 — `MemoryLimitPolicy::ProducerBlock` back-pressure via Waker slab

- **Status**: Accepted
- **Date**: 2026-05-21
- **Decider**: Florentin Dubois
- **Tags**: memory-limit, back-pressure, sans-io, no-channels, java-parity

## Context

Java's `org.apache.pulsar.client.api.MemoryLimitPolicy` exposes two strategies for what to do when `ClientBuilder#memoryLimit` is exhausted:

1. `FAIL_IMMEDIATELY` — reject the publish synchronously with a `MemoryBufferIsFullError`.
   Default in Java.
2. `PRODUCER_BLOCK` — the producer's `send`/`sendAsync` future does not complete until the budget frees up.

[ADR-0017](0017-memory-limit-atomic-reservation.md) shipped the `FailImmediately` half via a single atomic-CAS on `ConnectionShared.memory_used`.
The `ProducerBlock` half was tagged as a polish follow-up in `README.md#open-structural-gaps`.

Today (gate (d) cleared by Florentin on 2026-05-21) `ProducerBlock` is in scope — full Java parity per [ADR-0010](0010-v0-1-full-java-parity.md).

Constraints from prior ADRs:

- [ADR-0003 no-channels](0003-no-channels-rule.md): no `tokio::sync::{mpsc, broadcast, watch, oneshot}`, no `crossbeam-channel`, no `flume`.
  The signal "budget is now available, please retry" must travel without a channel.
- [ADR-0004 sans-io](0004-sans-io-protocol-core.md): `magnetar-proto` has zero I/O deps, no `tokio`, no `async-trait`.
  The atomic-CAS reservation already lives at the proto layer (per ADR-0017); the back-pressure mechanism added here lives at the **runtime** layer.
- [ADR-0011 clock injection](0011-clock-injection-sans-io.md): no `Instant::now()` / `SystemTime::now()` in `magnetar-proto` outside `#[cfg(test)]`.
  The wait is event-driven via `Waker::wake()`, not time-driven; nothing in this ADR reaches for a clock.

## Decision

Add a `MemoryLimitPolicy::ProducerBlock` variant.
The wait uses a `parking_lot::Mutex<Slab<Waker>>` ("MemoryWakers") on the runtime's `ConnectionShared`.
The proto-side atomic-CAS reservation is **unchanged**.

### Public API

```rust
// in magnetar-proto
pub enum MemoryLimitPolicy {
    FailImmediately, // Java default; preserves ADR-0017 semantics.
    ProducerBlock,   // new; runtime parks the send future on the Waker slab.
}

pub struct ConnectionConfig {
    pub memory_limit_bytes: u64,
    pub memory_limit_policy: MemoryLimitPolicy, // new field
    // ...
}
```

```rust
// in magnetar
ClientBuilder::new()
    .memory_limit(64 * 1024 * 1024, MemoryLimitPolicy::ProducerBlock)
    .build()
    .await?;
```

### Runtime-side mechanism (`magnetar-runtime-tokio::ConnectionShared`)

```rust
pub fn try_reserve_memory_or_register(
    self: &Arc<Self>,
    bytes: u64,
    waker: &Waker,
) -> Result<(), usize>; // Ok = reservation taken; Err = waker registered at slab_key.

pub fn cancel_memory_waker(&self, slab_key: usize); // evict on cancel/drop.
```

`release_memory(bytes)` drains the slab and calls `Waker::wake()` on every registered slot after the CAS release.
Drains everyone (not one-at-a-time) so all parked sends compete for the freed budget through the same CAS retry; producers with the smallest payloads tend to win, which matches Java's behaviour under contention.

### `Producer::send` decision tree

```text
match policy {
    FailImmediately => {
        if try_reserve_memory(bytes).is_err() {
            return SendFut { state: Failed(MemoryLimitExceeded) };
        }
        queue_send(...)            // existing ADR-0017 path
    }
    ProducerBlock => {
        if try_reserve_memory(bytes).is_ok() {
            return queue_send(...) // fast path: budget had room
        }
        // slow path: park on the slab, retry inside `SendFut::poll`
        SendFut { state: Reserving { msg, bytes, slab_key: None } }
    }
}
```

`SendFut::poll` for `Reserving`:

1. Calls `try_reserve_memory_or_register(bytes, cx.waker())`.
2. On `Ok(())`: cancels the prior slab slot (if any), invokes the inner `queue_send` path, transitions to `Pending`.
3. On `Err(new_key)`: replaces the prior slab slot with `new_key`, returns `Poll::Pending`.

`SendFut::drop` evicts any active slab slot via `cancel_memory_waker` so a future-cancellation cannot wake a dead future.

### Why a `Mutex<Slab<Waker>>` and not a channel

Channels are forbidden by ADR-0003.
The slab + Waker pattern is the documented `quinn`/`h2`-style escape hatch for the same shape of problem: many producers parked on a shared resource, dispatched in batch when capacity reappears.
The mutex hold time is `O(slab size)` and the slab only grows under live contention, so the contention cost is bounded by concurrent in-flight `Reserving` futures, not by total producers.

## Consequences

**Positive**

- Closes the last Java-parity gap on `memoryLimit` per ADR-0010 + README parity-matrix row at `:613`.
- Preserves ADR-0017's atomic-CAS hot path: `FailImmediately` is unchanged, no extra synchronization on the fast path.
- Cancellation-safe: `SendFut::drop` evicts the slab slot; future cancellation never leaks a stale waker.
- Survives sans-io invariants — the proto crate gains only a type (the `MemoryLimitPolicy` enum + the field on `ConnectionConfig`).
  All async / scheduling machinery stays in `magnetar-runtime-tokio`.

**Negative**

- A spurious-wake storm is possible if many parked producers compete for tiny capacity windows: `release_memory(bytes)` wakes them all and only one wins the next CAS.
  Acceptable trade-off (fair under contention; matches Java) but worth a follow-up if profiling shows hot-loop thrash.
  Documented in [`docs/follow-ups.md`](../../docs/follow-ups.md) as a polish opportunity, not a regression.
- The slab grows under live contention but never shrinks below high-water mark.
  `Slab` uses a free-list so this is a memory-only cost; no per-iteration allocation.

**Neutral**

- Moonpool engine: when ADR-0019 milestones M5/M6 land, the moonpool side gets the same surface via the engine adapter.
  The proto-side enum + config field carry across; only the `ConnectionShared` slab + `SendFut::poll` need a moonpool mirror.

## Alternatives considered

- **Semaphore over budget bytes** (`tokio::sync::Semaphore`).
  Rejected: although semaphores are not in the no-channels list verbatim, ADR-0003 bans the broader pattern of "async primitive that delivers values between tasks".
  A `Semaphore::acquire_many(bytes)` is functionally a channel pulling tokens.
- **`Notify`-and-wake-all on release**. Rejected: `Notify` only wakes the first awaiter unless `notify_waiters()` is used, and even with `notify_waiters` the producer would have to choose between always re-polling on every release (livelock under contention) and missing a wake when the registration races.
  The slab pattern lets each registered waker be discriminated.
- **Block synchronously on a `parking_lot::Condvar`**. Rejected: blocks the executor thread; defeats the async runtime entirely.

## References

- [ADR-0003 — no-channels rule](0003-no-channels-rule.md).
- [ADR-0004 — sans-io protocol core](0004-sans-io-protocol-core.md).
- [ADR-0010 — full Java parity](0010-v0-1-full-java-parity.md).
- [ADR-0011 — clock injection](0011-clock-injection-sans-io.md).
- [ADR-0017 — memory_limit atomic CAS reservation](0017-memory-limit-atomic-reservation.md) (this ADR extends ADR-0017's mechanism rather than replacing it).
- `crates/magnetar-runtime-tokio/src/lib.rs` — `ConnectionShared` + `MemoryWakers` + `try_reserve_memory_or_register` + `cancel_memory_waker`.
- `crates/magnetar-runtime-tokio/src/producer.rs` — `SendState::Reserving` variant + the `Producer::send` decision tree + the `SendFut::poll` retry path.
- Java reference: `MemoryLimitController` / `MemoryLimitPolicy.PRODUCER_BLOCK` in `org.apache.pulsar.client.impl`.
