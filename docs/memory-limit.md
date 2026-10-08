# Memory Limit Accounting

`ClientBuilder::memory_limit(bytes, MemoryLimitPolicy)` bounds the bytes of pending publishes across the whole client.
It mirrors Java's `ClientBuilder#memoryLimit` and its one `MemoryLimitController` per `PulsarClientImpl`: every producer, partition and topic of the client draws on the same budget, whichever physical connection it rides.
That includes the bootstrap connection, every `connections_per_broker` sibling, every proxy pool entry and every replacement connection the pool opens later ([ADR-0111](../specs/adr/0111-share-one-memory-limit-controller-per-client.md), issue #867).
Two separately built clients never share a budget.

## Surface

```rust
use magnetar::{MemoryLimitPolicy, PulsarClient};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = PulsarClient::builder()
    .service_url("pulsar://localhost:6650")
    .memory_limit(64 * 1024 * 1024, MemoryLimitPolicy::ProducerBlock)
    .build()
    .await?;
# Ok(()) }
```

| Policy            | Behavior                                                                                                                                                                                    |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `FailImmediately` | Overflow returns `MemoryLimitExceeded { current, limit, requested }` synchronously from `Producer::send`; `current` is the client-wide aggregate. Java default.                             |
| `ProducerBlock`   | Overflow parks the `SendFut` until enough budget frees up. A release anywhere in the client — any producer, any connection — wakes every parked send, and each one retries its reservation. |

The tokio engine surfaces the error as `ClientError::MemoryLimitExceeded`, the moonpool engine as `ClientError::Engine(EngineError::MemoryLimitExceeded)`.
A `memory_limit` of `0` means unlimited: nothing is reserved and nothing is counted.
The check is strict: a reservation is refused when `current + requested > limit`, so a single payload larger than the whole budget is always refused under `FailImmediately` and never fits under `ProducerBlock`.
`ClientBuilder::build` converts the public policy exhaustively into `magnetar_proto::ConnectionConfig::memory_limit_policy`; the façade getter and runtime policy therefore cannot diverge.

## What is counted

`Producer::send` reserves the length of the payload it hands to the producer state machine: after the tokio engine's per-message compression and after encryption (the moonpool engine refuses compression).
Wire framing, `MessageMetadata`, batch-container overhead and consumer receive queues are not counted.
The budget bounds pending publish payloads; it is not a limit on the process's memory (RSS).

## When the bytes are released

A reservation is held for exactly as long as the client retains the publish.
It travels with the publish's pending op (`magnetar_proto::producer::OpSend`) and is released, exactly once, when that op leaves the client:

- the broker's `CommandSendReceipt` — for a batch, each message's op on its own, as the receipt fans out;
- the broker's `CommandSendError`;
- the producer's `send_timeout` firing, including for a publish held across a reconnect;
- the broker acknowledging the producer's `close()`, which fails every send still pending — typically messages left in the batch container, which `close` does not flush — with a terminal error;
- the producer failing to open or re-attach for good, or the connection failing terminally;
- a reconnect that cannot replay a batched send, which fails it.

The bytes are **not** released when the caller's send future completes or is dropped while its publish is still queued or in flight: a fire-and-forget send keeps its bytes until the broker answers.
A reconnect that replays a retained publish neither releases nor charges it again.
A send the producer state machine rejects synchronously (closed producer, oversized payload with chunking off) never holds anything.

**Fire-and-forget under `ProducerBlock` loses messages once the budget is full.**
A send that does not fit yet is held back inside its future, which owns the message until it reserves.
If that future is dropped before then, the send is cancelled: the message is never published and no error surfaces (a `debug!` records "send dropped while waiting for memory budget; message not published").
A producer that drops the futures `send` returns reaches that state as soon as its in-flight publishes fill the budget, because those publishes now keep their bytes until the broker answers.
Await the send future under `ProducerBlock`, or use `FailImmediately` and observe `MemoryLimitExceeded`.
`cancelled_parked_send_leaks_nothing_and_never_reaches_the_wire` (both engines' `tests/client_wide_memory_limit.rs`) pins that such a send never reaches the wire and leaks nothing.

This is Java's lifetime: `ProducerImpl` releases a publish's memory when the op leaves `pendingMessages`, never when the caller's `CompletableFuture` is cancelled.

## Mechanism

Source: [ADR-0111](../specs/adr/0111-share-one-memory-limit-controller-per-client.md), which amends [ADR-0017](../specs/adr/0017-memory-limit-atomic-reservation.md), [ADR-0020](../specs/adr/0020-memory-limit-producer-block.md) and [ADR-0022](../specs/adr/0022-memory-limit-producer-block-moonpool.md).

`magnetar_proto::MemoryLimitController` is implemented once, in the sans-io core, and driven identically by both engines:

```text
MemoryLimitController
  limit, policy
  used:    AtomicU64                                  (lock-free reserve / release)
  waiters: Mutex<{ next_id, BTreeMap<id, Waker> }>    (leaf lock, ProducerBlock only)

try_reserve(n)          -> Ok(MemoryReservation) | Err(MemoryLimitExceeded)
poll_reserve(n, &mut id, waker)
                        -> Ready(MemoryReservation)   (registration removed)
                         | Pending                    (registered, or refreshed in place)
cancel_waiter(id)                                     (only that id; a stale id is a no-op)
drop(MemoryReservation) -> used -= n; wake every parked waker
```

The bootstrap connection's `ConnectionShared` builds the controller from its config and the runtime `Client` hands the same `Arc` to its connection pool, which builds every pooled connection against it.
`ConnectionShared::memory_limit` exposes it on every connection of the client.

`Producer::send` tries `try_reserve` once.
On success the `MemoryReservation` is moved into the state machine with the message (`ProducerSlot::queue_send_reserved`, `Connection::send_reserved`) and from there into the publish's `OpSend`; dropping the op releases it.
Under `ProducerBlock` an overflowing send instead returns a `SendFut` in the `Reserving` state, whose every poll calls `poll_reserve`; once it reserves, the message is queued with its reservation like any other.

Waiter ids come from a counter and are never reused, so a send that is woken, re-checks, and parks again gets a fresh id, and cancelling its previous id cannot remove anyone's live registration.
The slab this replaced freed its keys on every drain, so a re-parked send could receive its old key back and then cancel its own fresh registration — a lost wakeup that parked it forever.

### Locking

- `used` is a compare-and-swap counter; `FailImmediately` never takes a lock.
- The waiter mutex is a leaf lock: nothing else is acquired while it is held, and no waker is woken or dropped under it.
  An attempt plus its registration happen under it, and a release takes it after decrementing the counter, so a release either lands before the attempt (which then sees the bytes) or after the registration (which it then wakes).
- A reservation may be released under the connection mutex — where every receipt already wakes its future — but never under a per-slot mutex: the state machine hands every removed op back to the connection, which drops it after the slot guard is gone.
  The order is connection → slot → controller, never the reverse ([ADR-0038](../specs/adr/0038-split-connection-mutex.md)).

Waking every parked send on each release (not one) mirrors `MemoryLimitController#releaseMemory`: any released byte may unblock several smaller sends, and a send that still does not fit simply parks again.
Parked sends are woken in registration order, which keeps `moonpool_sim::SimProviders` runs reproducible per seed; how the woken tasks are then scheduled is the provider's call ([ADR-0022](../specs/adr/0022-memory-limit-producer-block-moonpool.md)).
Nothing here is a channel: every signal is a `core::task::Waker` behind a `parking_lot::Mutex` ([ADR-0003](../specs/adr/0003-no-channels-rule.md)).

## Known limits

- Parked `ProducerBlock` sends are woken only by a release: closing the client or a terminal connection failure does not wake them.
- A chunked publish's single op leaves the pending queue on the first receipt carrying its sequence id, so its bytes are released when the first chunk is persisted rather than the last, as Java does.
- Java reserves the uncompressed size before compression; magnetar counts the payload after the runtime's compression.
- A pending op the broker will never answer holds its bytes against the whole client until its `send_timeout` (30 s by default) fails it — with `send_timeout: None`, for the life of the client.
  Examples: batched sends already flushed but unacknowledged when the broker closes the producer and it re-attaches in place (they cannot be replayed), and the sends of a producer whose close the broker rejected ([follow-ups §22](follow-ups.md#22-memory-reservations-of-publishes-the-broker-will-never-answer)).
- Only the broker's acknowledgement of the close this client last issued for a producer fails its pending sends; the late acknowledgement of an older close for a reused producer id (issue #406) does not.

## Where the code lives

| Type                                                           | File                                                                                                                                                                                                                            |
| -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `MemoryLimitController`, `MemoryReservation`, `MemoryWaiterId` | [`crates/magnetar-proto/src/memory_limit.rs`](../crates/magnetar-proto/src/memory_limit.rs)                                                                                                                                     |
| `OpSend` reservation, `queue_send_reserved`                    | [`crates/magnetar-proto/src/producer.rs`](../crates/magnetar-proto/src/producer.rs)                                                                                                                                             |
| `Connection::send_reserved`, release points, close ack         | [`crates/magnetar-proto/src/conn.rs`](../crates/magnetar-proto/src/conn.rs)                                                                                                                                                     |
| `MemoryLimitPolicy`, `ConnectionConfig::memory_limit_*`        | [`crates/magnetar-proto/src/conn_types.rs`](../crates/magnetar-proto/src/conn_types.rs)                                                                                                                                         |
| `ConnectionShared::memory_limit`, pool sharing (tokio)         | [`crates/magnetar-runtime-tokio/src/lib.rs`](../crates/magnetar-runtime-tokio/src/lib.rs), [`client.rs`](../crates/magnetar-runtime-tokio/src/client.rs), [`pool.rs`](../crates/magnetar-runtime-tokio/src/pool.rs)             |
| `Producer::send`, `SendFut` (tokio)                            | [`crates/magnetar-runtime-tokio/src/producer.rs`](../crates/magnetar-runtime-tokio/src/producer.rs)                                                                                                                             |
| `ConnectionShared::memory_limit`, pool sharing (moonpool)      | [`crates/magnetar-runtime-moonpool/src/lib.rs`](../crates/magnetar-runtime-moonpool/src/lib.rs), [`client.rs`](../crates/magnetar-runtime-moonpool/src/client.rs), [`pool.rs`](../crates/magnetar-runtime-moonpool/src/pool.rs) |
| `Producer::send`, `SendFut` (moonpool)                         | [`crates/magnetar-runtime-moonpool/src/producer.rs`](../crates/magnetar-runtime-moonpool/src/producer.rs)                                                                                                                       |

## Test coverage

- Controller and op-scoped release unit tests sit next to the code in `magnetar-proto` (`memory_limit.rs`, `producer.rs`, `conn.rs`'s `memory_limit_release_tests`).
- `tests/client_wide_memory_limit.rs` runs the same nine scenarios on both engines: eight against a broker that holds receipts, with two producers on two physical connections, and one that walks every enqueue path of the state machine.
- `crates/magnetar-differential/tests/client_wide_memory_limit_equivalence.rs` asserts both engines walk the client-wide budget identically.
- [`crates/magnetar/tests/e2e_memory_limit.rs`](../crates/magnetar/tests/e2e_memory_limit.rs) exercises both policies against a live broker, including one budget across `connections_per_broker(2)` connections.
