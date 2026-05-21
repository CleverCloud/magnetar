# ADR-0017 — `memory_limit` runtime accounting via atomic CAS reservation

- **Status**: Accepted
- **Date**: 2026-05-21
- **Decider**: Florentin Dubois
- **Tags**: memory, backpressure, producer, java-parity

## Context

Apache Pulsar's Java client supports `ClientBuilder.memoryLimit(long, MemoryLimitPolicy)`.
The client tracks the sum of in-flight publish bytes across all
producers on the `PulsarClient`. When a `send()` would push the total
above the limit, the policy decides:

- `MemoryLimitPolicy.WAIT` — block (async) until enough room frees.
- `MemoryLimitPolicy.NONE` — fail-fast with `ProducerQueueIsFullError`.

Magnetar previously surfaced the builder but had no runtime check, so
the parity matrix correctly listed it as `🟡`.

Constraints:

- [ADR-0003 no-channels-rule](0003-no-channels-rule.md): no
  `tokio::sync::Semaphore` (semaphores are sometimes argued to be
  "not channels", but they share the same blocking-coordination shape
  and the project bans channel-shaped coordination primitives entirely).
- Accounting must be lock-free on the hot path (every `send` touches it).
- The reservation must be atomically released when the `SendFut` resolves
  or is dropped — including on error / cancellation.

## Decision

`ConnectionShared` carries:

```rust
pub struct ConnectionShared {
    /* … */
    memory_limit_bytes: u64,                  // 0 = unlimited
    memory_used: AtomicU64,
}

impl ConnectionShared {
    pub fn try_reserve_memory(&self, bytes: u64) -> Result<(), MemoryFull> {
        if self.memory_limit_bytes == 0 { return Ok(()); }
        let mut current = self.memory_used.load(Ordering::Relaxed);
        loop {
            let new = current.checked_add(bytes).ok_or(MemoryFull)?;
            if new > self.memory_limit_bytes { return Err(MemoryFull); }
            match self.memory_used.compare_exchange_weak(
                current, new, Ordering::AcqRel, Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }
    pub fn release_memory(&self, bytes: u64) {
        if self.memory_limit_bytes == 0 { return; }
        self.memory_used.fetch_sub(bytes, Ordering::AcqRel);
    }
}
```

`Producer::send` calls `try_reserve_memory(payload.len() as u64)` *before*
queueing the send. If full and the policy is `WAIT`, it currently
returns `MemoryFull` immediately (an opt-in async wait will follow). On
return / drop, `SendFut::Drop` calls `release_memory` exactly once via
a `released: bool` flag.

```rust
pub struct SendFut { /* … */
    reserved_bytes: u64,
    released: bool,
}

impl Drop for SendFut {
    fn drop(&mut self) {
        if !self.released && self.reserved_bytes > 0 {
            shared.release_memory(self.reserved_bytes);
        }
    }
}
```

`ClientBuilder::memory_limit(bytes: u64, policy: MemoryLimitPolicy)`
plumbs the configuration down to `ConnectionConfig::memory_limit_bytes`
which seeds `ConnectionShared`.

## Consequences

- Zero allocation, zero locking on the hot path — just a CAS loop.
- The CAS loop is well-bounded under contention: each retry sees a
  monotonically increasing `current`, so the loop terminates when
  either a competing reservation pushes the total above the limit (we
  reject) or our reservation wins.
- `Drop`-based release survives panics and future cancellations — every
  reserved byte is accounted for.
- The `WAIT` policy is currently degraded to fail-fast (returns
  `MemoryFull`). A future change can layer a `Notify`-based wait without
  changing the reservation primitive.
- `memory_limit_bytes = 0` is the "unlimited" sentinel (matches Java's
  default).

## References

- `crates/magnetar-runtime-tokio/src/lib.rs` — `ConnectionShared`
  accounting fields + `try_reserve_memory` / `release_memory`
- `crates/magnetar-runtime-tokio/src/producer.rs` — `SendFut` reservation
  + Drop
- `crates/magnetar-proto/src/conn.rs` — `ConnectionConfig::memory_limit_bytes`
- `crates/magnetar/src/client.rs` — `ClientBuilder::memory_limit`
- Commit `703744e` — "feat(client): memory_limit runtime enforcement via AtomicU64 CAS reservation"
- Commit `6b2fa8e` — "feat(client): ClientBuilder::memory_limit + MemoryLimitPolicy"
- Java reference: `org.apache.pulsar.client.api.ClientBuilder#memoryLimit`
- [ADR-0003 no-channels-rule](0003-no-channels-rule.md)
