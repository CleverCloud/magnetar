# ADR-0111 — Share one memory-limit controller per client and hold each reservation for the life of its publish

- **Status**: Accepted (amends [ADR-0017](0017-memory-limit-atomic-reservation.md), the per-connection counter and the `SendFut`-scoped release; [ADR-0020](0020-memory-limit-producer-block.md), the `ConnectionShared` waker slab and its `SendFut::drop` release; [ADR-0022](0022-memory-limit-producer-block-moonpool.md), the moonpool mirror of that slab; [ADR-0073](0073-connections-per-broker.md), whose sibling connections each carried their own budget)
- **Date**: 2026-10-08
- **Decider**: Florentin Dubois
- **Tags**: memory-limit, back-pressure, producer, connection-pool, sans-io, no-channels, java-parity, issue-867

## Context

`ClientBuilder::memory_limit(bytes, policy)` is documented as Java's `ClientBuilder#memoryLimit`: one pending-publish budget for the whole client.
Java builds exactly one `MemoryLimitController` per `PulsarClientImpl` and every `ProducerImpl` of that client reserves against it, whatever connection the producer rides.
Magnetar kept the counter on each physical connection instead: `memory_limit_bytes`, `memory_used`, `memory_limit_policy` and the `memory_wakers` slab were fields of the runtime `ConnectionShared` (tokio `crates/magnetar-runtime-tokio/src/lib.rs`, moonpool `crates/magnetar-runtime-moonpool/src/lib.rs`), copied from `ConnectionConfig` at construction.
Every pool entry builds a fresh `ConnectionShared` from a clone of the bootstrap config (tokio `pool.rs` `build_entry`, moonpool `pool.rs` `build_entry_async`), so a client with N physical connections — `connections_per_broker > 1` siblings ([ADR-0073](0073-connections-per-broker.md)), proxy pool entries ([ADR-0039](0039-pulsar-proxy-multi-broker-connection-model.md)), replacement connections — admitted N times the configured bytes (issue #867).

Two further defects sat in the same mechanism.

- **The reservation was scoped to the caller's future, not to the publish.**
  `SendFut` released its bytes when it returned `Ready` and in its `Drop`, so a fire-and-forget send (the future dropped right after `send`) freed its budget at once while the payload stayed retained in the producer's `OpSend` for receipt correlation and reconnect replay.
  Java releases a publish's memory inside `ProducerImpl` when the op leaves `pendingMessages` — `ackReceived` → `releaseSemaphoreForSendOp`, the send-error path, `failPendingMessages` / `failPendingBatchMessages` — and never on cancellation of the caller's `CompletableFuture`.
- **`ProducerBlock` could lose a wakeup.**
  `release_memory` drained the `Slab<Waker>`, which frees its keys.
  A woken `Reserving` future whose re-check still failed re-registered and usually received the SAME key, then called `cancel_memory_waker(prior)` with its old key — removing its own fresh registration and parking forever.
  In the success arm the same stale cancel could evict a different future that had reused the key.
  The red test `parked_send_survives_a_partial_release` reproduces it on a single connection, on both engines, at 15e6ed6.

Alternatives considered:

- **Sum the per-connection counters.**
  Rejected: a reservation is a compare-and-swap against one number; N counters cannot enforce one limit without a lock across all of them.
- **A new `ConnectionConfig` field carrying a shared handle.**
  Rejected for the same reason `operation_retry` rides the runtime `ConnectionFactory`: `ConnectionConfig` is not `#[non_exhaustive]`, so a field breaks every downstream exhaustive literal, and it is a single-connection configuration, not a client-topology one ([ADR-0073](0073-connections-per-broker.md)).
- **Keep the controller in each runtime crate.**
  Rejected: two copies of the same lock-and-wake protocol drift; the defect above shipped identically in both.
- **Release explicitly at every place an op leaves the pending queue.**
  Rejected: there are a dozen such places (receipt fan-out, send error, the live and the relocated send-timeout sweeps, `fail_all_pending`, producer-open failure, reset of a non-replayable batch, snapshot buckets, slot removal), and a missed one leaks budget silently.
  Ownership gives exactly-once by construction.
- **Java's "admit one request over the limit when the counter is below it".**
  Rejected: magnetar has always refused strictly (`current + requested > limit`) and the e2e suite pins that.

## Decision

One `MemoryLimitController` per runtime `Client`, implemented once in `magnetar-proto` (`crates/magnetar-proto/src/memory_limit.rs`), and a reservation that lives exactly as long as the publish the client retains.

- **The controller.**
  It holds the limit, the policy, an `AtomicU64` usage counter, and the parked `ProducerBlock` callers in a `BTreeMap<u64, Waker>` keyed by `MemoryWaiterId`s drawn from a monotonically increasing counter and never reused.
  It has no clock and no I/O; it uses only `parking_lot` and `core::task::Waker`, both already in the sans-io core.
  `try_reserve(bytes)` refuses strictly with `MemoryLimitExceeded { current, limit, requested }`, where `current` is the client-wide aggregate; a limit of `0` reserves nothing and always succeeds.
  `poll_reserve(bytes, &mut Option<MemoryWaiterId>, &Waker)` attempts and, on failure, registers — or refreshes the caller's still-live registration — under one leaf lock; `cancel_waiter(id)` removes only that id.
- **Placement.**
  The bootstrap `ConnectionShared` builds the controller from its config (`MemoryLimitController::from_config`), and the runtime `Client` lifts `shared.memory_limit` into its `ConnectionFactory` beside `operation_retry`.
  Every pool entry is built against that one controller: tokio `ConnectionShared::with_auth_and_memory_limit`, moonpool `make_shared_with_providers(.., memory_limit)`.
  A `ConnectionShared` constructed without a client (`new`, `with_auth`) keeps a private controller, so single-connection behaviour is unchanged.
  Two separately constructed clients never share a budget.
  The four per-connection fields and their methods are replaced by one `pub memory_limit: Arc<MemoryLimitController>`.
- **What is counted.**
  The length of the payload the runtime hands to the producer state machine: after the tokio engine's per-message compression and after encryption (the moonpool engine refuses compression).
  Wire framing, `MessageMetadata`, batch-container overhead, consumer receive queues and everything else are not counted.
  This bounds pending publish payloads; it is not a process memory (RSS) limit.
- **Reservation lifetime (op-scoped).**
  A successful reservation is a `MemoryReservation` that releases its bytes exactly once, when dropped.
  `ProducerSlot::queue_send_reserved` / `Connection::send_reserved` move it into the publish's `OpSend` — one op per logical message, so a batched message holds its own reservation and a chunked message holds one for all its chunks.
  The bytes are released when the op leaves the client: `CommandSendReceipt` (including the batch fan-out, one op at a time), `CommandSendError`, the send-timeout sweep over live ops and over reset snapshots, `fail_all_pending`, producer-open failure, the reset of a non-replayable batch, a slot removed by `cancel_producer_open`, a snapshot bucket `rebuild_producers` discards for a closed producer, and — new — the broker's acknowledgement of a `CommandCloseProducer`, which now fails the producer's still-pending sends exactly like Java's `closeAndClearPendingMessages` (`Connection::fail_sends_of_closed_producer`).
  They are never released by the caller's future completing or being dropped while its op is still queued or in flight, so a fire-and-forget send no longer bypasses the budget.
  A reconnect moves the op into its replay snapshot and back: no release, no second charge.
  A reservation that never reaches an op — the state machine rejected the send — is released by whoever still holds it, after the per-slot guard is gone.
- **Lock rule.**
  The counter is lock-free.
  The waiter mutex is a leaf lock: nothing else is acquired while it is held, and no waker is woken or dropped under it.
  A release decrements the counter, then takes the waiter lock, takes the whole parked set and wakes it after unlocking; an attempt plus registration happens under the same lock, so a release either lands before the attempt (which then sees the bytes) or after the registration (which it then wakes).
  A reservation may be dropped under the connection mutex — the same place every receipt already wakes its future — but never under a per-slot mutex: `apply_receipt`, `apply_send_error`, `drain_timed_out_sends`, `drain_pending_sends` and the non-replayable half of `snapshot_pending_sends` now hand the removed `OpSend` back to their `Connection` caller, which drops it after the slot guard ([ADR-0038](0038-split-connection-mutex.md) order: connection → slot → controller).
- **`ProducerBlock`.**
  `Producer::send` tries once synchronously; on a full budget the `SendFut` enters `Reserving` and polls `poll_reserve`.
  Every release anywhere in the client wakes every parked caller on every connection, in registration order — deterministic under `moonpool_sim::SimProviders` ([ADR-0022](0022-memory-limit-producer-block-moonpool.md)'s eventual-progress contract still governs scheduling).
  Dropping a parked future cancels its own id; a stale id is a no-op.
- **Errors.**
  The proto `MemoryLimitExceeded` maps to `ClientError::MemoryLimitExceeded` on tokio and `EngineError::MemoryLimitExceeded` on moonpool, unchanged.

## Consequences

- `memory_limit` is now client-wide on both engines: `connections_per_broker`, proxy pool entries and replacement connections no longer multiply it, and `MemoryLimitExceeded.current` reports the aggregate.
- Behaviour change: dropping a `SendFut` no longer frees budget while its publish is pending.
  With no receipt and `send_timeout: None`, those bytes stay reserved until the producer closes or the connection fails terminally — which is exactly the memory the client still retains.
- Behaviour change, fire-and-forget under `ProducerBlock`: a send held back in `Reserving` owns its message until it reserves, so dropping its future before then cancels the send — the message is never published and no error surfaces (one structured `debug!`, "send dropped while waiting for memory budget; message not published", records it).
  Before this ADR a fire-and-forget producer (`let _ = producer.send(msg)`) never reached that state, because every dropped future gave its bytes back; now its in-flight publishes keep their bytes, so once they fill the budget every further dropped send is cancelled.
  Applications must await the send future under `ProducerBlock`, or use `FailImmediately` and observe `MemoryLimitExceeded`.
  `cancelled_parked_send_leaks_nothing_and_never_reaches_the_wire` (both engines) pins that a dropped parked send never reaches the wire and leaks nothing.
- Behaviour change: a send still pending when the broker acknowledges its producer's close — in practice a message left in the batch container, which `close` stops flushing — now resolves with `OpOutcome::Terminal` (`PeerClosed` on both engines) instead of hanging forever, and releases its bytes.
  On the fire-and-forget close of a dropped producer the outcome is recorded even if no future remains to take it, the same as the receipt arm already does for any dropped send future; it is not a new leak class.
  Only the acknowledgement of the close this client last issued for the slot drains it (`ProducerState::close_request`), because the fire-and-forget re-close of an abandoned producer id ([ADR-0100](0100-close-cancelled-producer-open-before-retry.md), issue #406) can be acknowledged after `create_producer` re-attached a new producer under the same id — open, or since closed by its own user.
- Low-level API changes: the runtime `ConnectionShared` loses `memory_limit_bytes`, `memory_used`, `memory_limit_policy`, `memory_wakers` and `try_reserve_memory` / `release_memory` / `try_reserve_memory_or_register` / `cancel_memory_waker` in favour of `memory_limit`; `OpSend` gains a crate-private reservation (so it can no longer be built outside `magnetar-proto`) and `reserved_bytes()`; the five op-removing `ProducerState` methods return `OpSend`s.
- Cost: an `Arc` clone per limited reservation and one uncontended waiter-lock acquisition per release while a limit is configured; nothing at all when the limit is `0`.
- Follow-ups, deliberately out of scope:
  - Parked `ProducerBlock` callers are not woken when the client closes or its connections fail terminally; they wake only on a release.
  - A payload larger than the whole budget parks forever under `ProducerBlock` (it can never fit); `FailImmediately` rejects it.
  - A chunked publish's single op leaves the pending queue on the FIRST `CommandSendReceipt` carrying its sequence id, so its reservation is released when chunk 0 is persisted, while later chunks may still be in flight; Java releases on the last chunk.
    The release is still exactly once (`chunked_send_reserves_once_and_releases_once`), and the same early removal drops the replay frames of the remaining chunks — a pre-existing reconnect concern independent of the budget.
  - Java reserves the uncompressed size before compression; magnetar counts the payload after the runtime's compression, and the batch-compression move of issue #860 does not change that here.
  - A pending op the broker will never answer holds its bytes against the WHOLE client until it times out: for example batched ops already flushed but unacknowledged when the broker closes the producer and it re-attaches in place ([ADR-0106](0106-reattach-broker-closed-producer-in-place.md)) — their `replay_frames` are empty, so `replay_pending_outbound` never re-sends them — or the sends of a producer whose close the broker rejected.
    The default 30 s `send_timeout` clears them; with `send_timeout: None` they are held for the life of the client ([`docs/follow-ups.md`](../../docs/follow-ups.md) §22).
  - A dropped `SendFut` still leaves its outcome in the connection's `outcomes` map when its op resolves (the issue #241 shape); the close-ack drain records such outcomes too ([`docs/follow-ups.md`](../../docs/follow-ups.md) §23).

Tests (ADR-0024 layers, all red first where the old API compiled):

- (a) `crates/magnetar-proto/src/memory_limit.rs` controller tests, including `re_park_after_a_partial_release_keeps_a_live_registration`; `producer.rs` op-scoped tests (queued, rejected, batched, chunked, snapshot/replay, timeout, drain); `conn.rs` `memory_limit_release_tests` (receipt, send error, rejection, close ack, reset, timeout, producer-open failure, terminal failure).
- (b)/(c) `tests/client_wide_memory_limit.rs` on both engines, nine scenarios each: eight against a broker that holds receipts, with two producers on two physical connections, and one that walks every enqueue path (plain, batched, chunked, oversized-but-batched, unreserved).
- (d) `crates/magnetar-differential/tests/client_wide_memory_limit_equivalence.rs`.
- e2e `crates/magnetar/tests/e2e_memory_limit.rs::e2e_memory_limit_is_one_budget_across_connections`.

## References

- `crates/magnetar-proto/src/memory_limit.rs` — `MemoryLimitController`, `MemoryReservation`, `MemoryWaiterId`, `MemoryLimitExceeded`.
- `crates/magnetar-proto/src/producer.rs` — `OpSend::reservation`, `queue_send_reserved`, the op-returning removal methods.
- `crates/magnetar-proto/src/conn.rs` — `send_reserved`, `terminalize_drained_sends`, `fail_sends_of_closed_producer`.
- `crates/magnetar-runtime-tokio/src/{lib,producer,client,pool}.rs` and `crates/magnetar-runtime-moonpool/src/{lib,producer,client,pool}.rs` — controller placement, `Producer::send`, `SendFut`.
- [`docs/memory-limit.md`](../../docs/memory-limit.md) — user-facing semantics.
- Java reference: `PulsarClientImpl` (one `MemoryLimitController` per client), `ProducerImpl#releaseSemaphoreForSendOp`, `ProducerImpl#closeAndClearPendingMessages`, `MemoryLimitController#releaseMemory`.
- [ADR-0003](0003-no-channels-rule.md), [ADR-0004](0004-sans-io-protocol-core.md), [ADR-0017](0017-memory-limit-atomic-reservation.md), [ADR-0020](0020-memory-limit-producer-block.md), [ADR-0022](0022-memory-limit-producer-block-moonpool.md), [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md), [ADR-0038](0038-split-connection-mutex.md), [ADR-0039](0039-pulsar-proxy-multi-broker-connection-model.md), [ADR-0073](0073-connections-per-broker.md).
