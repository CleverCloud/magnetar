# Memory Limit Accounting

`ClientBuilder::memory_limit(bytes, MemoryLimitPolicy)` enforces a
global publish-bytes budget across every producer on a connection. The
budget mirrors Java's `org.apache.pulsar.client.api.MemoryLimitPolicy`.
Two policies ship; the choice is sticky for the lifetime of the
connection.

## Surface

```rust
use magnetar::PulsarClient;
use magnetar_proto::MemoryLimitPolicy;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = PulsarClient::builder()
    .service_url("pulsar://localhost:6650")
    .memory_limit(64 * 1024 * 1024, MemoryLimitPolicy::ProducerBlock)
    .build()
    .await?;
# Ok(()) }
```

| Policy | Behavior |
| --- | --- |
| `FailImmediately` | Overflow returns `ClientError::MemoryLimitExceeded { current, limit, requested }` synchronously from `Producer::send`. Java default. |
| `ProducerBlock` | Overflow parks the `SendFut` until enough budget frees up. The future is woken when another in-flight publish completes. |

A `memory_limit` of `0` means unlimited and bypasses both reservation
paths entirely.

## FailImmediately — atomic CAS reservation

Source: [ADR-0017](../specs/adr/0017-memory-limit-atomic-reservation.md).

`ConnectionShared` carries:

- `memory_limit_bytes: u64` — copied from `ConnectionConfig` at
  construction.
- `memory_used: AtomicU64` — currently reserved bytes.

`Producer::send` reserves before queuing:

```text
let n = msg.payload.len() as u64;
let reserved = shared.try_reserve_memory(n)?;
    // CAS loop: load(Acquire) -> check current + n <= limit
    //           -> compare_exchange(AcqRel)
    //           -> Err(MemoryLimitExceeded { current, limit, requested })
let result = lock(conn).send(handle, msg, ..., now);
match result {
    Ok(seq) => SendFut { reserved_bytes: n, ... },     // released on Ready/Drop
    Err(_)  => { shared.release_memory(n); ... }
}
```

`SendFut::poll` on `Ready` calls `release_memory(self.reserved_bytes)`.
`SendFut::drop` also releases (the future was cancelled or dropped
without polling). Double-release is guarded by zeroing
`reserved_bytes` after the first release.

## ProducerBlock — Waker slab

Source: [ADR-0020](../specs/adr/0020-memory-limit-producer-block.md).

`FailImmediately` returns an error on overflow. `ProducerBlock` parks
the future on a Waker slab until budget frees up. The slab lives on
`ConnectionShared`:

```rust
// magnetar-runtime-tokio::ConnectionShared
pub struct ConnectionShared {
    pub memory_limit_bytes: u64,
    pub memory_limit_policy: MemoryLimitPolicy,
    pub memory_used: AtomicU64,
    pub memory_wakers: parking_lot::Mutex<Slab<Waker>>,
    // ...
}
```

`try_reserve_memory_or_register(bytes, waker)` is the new entry. The
CAS loop runs first; on overflow the waker is inserted into the slab
and a `MemoryPending(slab_key)` token returned. `release_memory`
performs the CAS release **and** drains the slab so every parked
producer re-polls:

```text
release_memory(bytes):
    memory_used.fetch_sub(bytes, AcqRel)
    let mut slab = memory_wakers.lock();
    for (_, waker) in slab.drain() {
        waker.wake();
    }
```

Drain-all (not "wake one") is deliberate — multiple parked producers
race for the freed budget; the first to win the CAS proceeds, the
others re-park. Mirrors Java's `MemoryLimitController` fairness
contract.

`MemoryReserveFut::poll`:

```text
loop {
    match shared.try_reserve_memory_or_register(self.bytes, cx.waker()) {
        Ok(reserved) => return Poll::Ready(reserved),
        Err(MemoryPending { slab_key }) => {
            self.slab_key = Some(slab_key);
            return Poll::Pending;
        }
    }
}
```

`MemoryReserveFut::drop` calls `cancel_memory_waker(slab_key)` to
evict the slot — without it, a cancelled producer would leave a stale
waker behind that the next `release_memory` would needlessly wake.

The slab + waker fan-out is no-channels-clean. No `Notify`, no `mpsc`,
no `oneshot` — every signal is a `core::task::Waker` registered in a
`Slab` behind a `parking_lot::Mutex`.

## Where the code lives

| Type | File |
| --- | --- |
| `MemoryLimitPolicy` enum | [`crates/magnetar-proto/src/conn.rs`](../crates/magnetar-proto/src/conn.rs) |
| `ConnectionConfig::{memory_limit_bytes, memory_limit_policy}` | [`crates/magnetar-proto/src/conn.rs`](../crates/magnetar-proto/src/conn.rs) |
| `ConnectionShared` + atomic + slab (tokio engine) | [`crates/magnetar-runtime-tokio/src/lib.rs`](../crates/magnetar-runtime-tokio/src/lib.rs) |
| `Producer::send` reservation path | [`crates/magnetar-runtime-tokio/src/producer.rs`](../crates/magnetar-runtime-tokio/src/producer.rs) |
| `MemoryLimitExceeded` error variant | [`crates/magnetar-runtime-tokio/src/error.rs`](../crates/magnetar-runtime-tokio/src/error.rs) |

## End-to-end test coverage

[`crates/magnetar/tests/e2e_memory_limit.rs`](../crates/magnetar/tests/e2e_memory_limit.rs)
exercises both policies against a live broker. Unit tests for the CAS
loop and the slab drain order live next to the implementations.

## Moonpool engine

The moonpool engine implements `FailImmediately` only. `ProducerBlock`
parity on moonpool is tracked in [`follow-ups.md`](follow-ups.md) — the
slab+waker fan-out is sans-io-clean but the drain-order determinism
story under `SimProviders` is not yet specified.
