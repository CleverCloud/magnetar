# ADR-0038 — Split the global Connection mutex into per-handle slots

- **Status**: Accepted
- **Date**: 2026-05-27
- **Decider**: Florentin Dubois
- **Tags**: architecture, performance, concurrency, sans-io

## Context

Until this change, every operation on a `magnetar::PulsarClient` went
through one `parking_lot::Mutex<magnetar_proto::Connection>` held in
`ConnectionShared.inner` (see `magnetar-runtime-tokio/src/lib.rs` and
`magnetar-runtime-moonpool/src/lib.rs`). The 2026-05-27 multi-agent
code audit (codex P1 finding; the audit narrative is preserved in the
`docs/follow-ups.md` "Audit 2026-05-27" section) measured ~290 lock
acquisition sites in the workspace that funnelled through this one
mutex: every `producer.send`, every `consumer.next`, every ack, every
stats read, plus the driver loop.

Critical sections are short and synchronous (no `.await` inside the
mutex), so the lock itself doesn't serialise on I/O. But it does
serialise the hot paths of unrelated handles — two producers on the
same `PulsarClient` cannot run their `.send()` paths in parallel,
even when their queues and waker slabs are entirely independent. On
hosts where the broker round-trip latency dominates this isn't
visible, but anywhere lock contention measurably bites (CPU-bound
client, many producers fanning out, sim runs under
`MoonpoolEngine<SimProviders>`) the global mutex caps fan-out.

Adjacent state machines facing the same pressure (quinn-proto,
H2-frame state machines, FoundationDB's nio) sidestep the problem by
keeping per-stream / per-handle state behind its own lock and reserving
the connection-wide lock for true cross-cutting work (transport-level
state, framing buffers, request-id allocation, event/outcome queues).

Alternatives considered:

- **DashMap for `producers` / `consumers`.** Rejected: the map lookups
  aren't the contention point — the global lock serialised every
  *mutation* below the lookup, not the lookup itself. Sharding the map
  while everything else still funnels through the global lock changes
  nothing.
- **`RwLock<Connection>`.** Rejected: virtually every protocol-level
  operation needs `&mut self` on `Connection`, so the read-side path
  would be empty.
- **Single-thread driver with channels into it.** Forbidden by ADR-0003
  (no channels) and would introduce the latency hop the sans-io split
  is designed to avoid.
- **Per-handle channels into the driver.** Same ADR-0003 violation,
  plus loses the synchronous-protocol-progress property
  `Connection::send` currently provides.

## Decision

Introduce `magnetar_proto::ProducerSlot` and
`magnetar_proto::ConsumerSlot`:

```rust
pub struct ProducerSlot {
    pub identity: ProducerIdentity,                 // immutable
    pub state: parking_lot::Mutex<ProducerState>,   // per-handle mutex
}
```

`Connection` stores `HashMap<Handle, Arc<Slot>>` rather than
`HashMap<Handle, State>`. The runtime `Producer` / `Consumer` handles
in both engines hold their own `Arc<Slot>` clone, captured at
create-producer / subscribe time. Identity reads (topic, access mode,
subscription, handle) read `slot.identity.*` and take NO lock at all.
State-machine reads / writes take only the per-slot mutex via
`slot.state.lock()`. The global Connection mutex is still required for
true cross-cutting work — frame buffers, handshake state,
`pending_requests`, the events / outcomes / wakers slabs, the
handle registry.

**Lock-ordering invariant (project-wide):**

1. **Global Connection mutex → per-slot mutex** is safe and is the only
   path the codebase takes.
2. **Per-slot mutex → global Connection mutex is FORBIDDEN.** A holder
   of `slot.state.lock()` MUST release the slot lock before touching
   Connection-level state.

The producer hot path (`Producer::send`) bypasses the global mutex
entirely via `ProducerSlot::queue_send`, which takes only the per-slot
mutex and stages outbound frames on the slot's own `state.outbound`
queue. The driver's next tick calls `Connection::poll_transmit` (which
in turn calls `Connection::drain_producer_outbound`) to merge per-slot
staged frames into the connection-wide outbound buffer before
flushing to the socket. This is the parallelism win — two producers
each running on their own slot lock no longer serialise against each
other.

The change landed in four phases on `refactor/split-connection-mutex`
and `refactor/split-connection-mutex-p2`:

1. **Phase 1 — Foundation.** `Connection` rebuilt around `Arc<Slot>`
   registries; every internal accessor goes through `slot.state.lock()`
   under the global mutex. Zero behavioral impact.
2. **Phase 2 — Cold-path direct access.** Runtime `Producer` /
   `Consumer` carry a direct `Arc<Slot>`; observability getters
   (`topic`, `name`, `stats`, `pending_count`, `batch_*`,
   `available_in_queue`, `is_paused`, …) bypass the global lock.
3. **Phase 3 — Hot-path split.** `Producer::send` calls
   `ProducerSlot::queue_send` directly. The driver merges per-slot
   staged frames in `poll_transmit`.
4. **Phase 4 — Tests + this ADR + docs.** Four-layer test coverage
   (proto unit, tokio integration, moonpool integration, differential
   equivalence), lock-ordering smoke test, and architectural
   documentation.

## Consequences

**Easier:**

- Two producers on the same `PulsarClient` connection run `send`
  hot paths in parallel — they contend only on their own per-slot
  mutexes. Verified by
  `crates/magnetar-runtime-{tokio,moonpool}/tests/two_producers_parallel.rs`.
- Cold-path observability (`Producer::stats()`, `Consumer::topic()`,
  `Producer::pending_count()`, …) doesn't compete with the producer /
  driver hot paths. Identity reads are lock-free.
- Adding a third runtime engine (e.g. glommio) doesn't change the lock
  story — the per-slot vs. global split is sans-io.

**Harder:**

- Any future code path that mutates per-handle state from a
  Connection-wide context MUST take the global mutex first and the
  per-slot mutex second. Wrong-order acquisition deadlocks under
  contention. Enforced by documentation + the dedicated
  `lock_ordering_global_then_per_slot_does_not_deadlock` proto unit
  test.
- `Connection::producer(handle)` and `consumer(handle)` now return
  `Option<&Arc<Slot>>` rather than `Option<&State>` — callers must
  `slot.state.lock()` to reach the state-machine fields. Mechanical
  API change.
- `producer_name` / `consumer_name` returns `Option<String>` rather
  than `Option<&str>` because the underlying field is mutex-guarded;
  borrowed returns cannot outlive the lock guard. Identity reads
  (`producer_topic`, `consumer_subscription`) keep `Option<&str>`
  because they read the immutable identity.
- `magnetar-proto` now depends on `parking_lot` (sync primitive only).
  `xtask check-no-io-deps` continues to pass — `parking_lot` is not on
  the I/O-deps banlist.

**Cost:**

- One extra `Arc` clone per `Producer` / `Consumer` construction
  (taken under the global lock at create / subscribe time).
- One extra `parking_lot::Mutex` per registered handle. Trivial memory
  cost (~40 bytes per slot mutex header).
- `Producer::send`: one mutex acquisition (per-slot) instead of one
  (global). No change in lock count; the difference is which lock it
  is, and therefore who it contends with.

**Parity guarantees:**

- `magnetar-differential::two_producers_parallel_equivalence` asserts
  the tokio and moonpool engines produce byte-identical event streams
  through the per-slot hot path. Hot-path output is bit-stable across
  engines.
- `xtask check-runtime-test-parity` continues to enforce the 1:1
  tokio↔moonpool test count per ADR-0024.

**Compatibility:**

- ADR-0026 D-series compound operations (`SendReceipt`, `SendError`,
  `Message` dispatch) collect their work under the per-slot lock, then
  release it before touching the connection-wide events / outcomes /
  wakers slabs. This preserves the wake / outcome ordering Java parity
  depends on.
- ADR-0028 supervised reconnect (`rebuild_producers` /
  `rebuild_consumers`) still walks every slot and replays the
  snapshotted in-flight publishes. The rebuild path takes the global
  lock + each per-slot lock in the canonical order, so the
  reconnect-time guarantees are unchanged.

## References

- [`docs/follow-ups.md` "Audit 2026-05-27"](../../docs/follow-ups.md) —
  the codex P1 finding that triggered this refactor.
- `crates/magnetar-proto/src/{producer,consumer}.rs` — `ProducerSlot` /
  `ConsumerSlot` types and the lock-ordering documentation.
- `crates/magnetar-proto/src/conn.rs::drain_producer_outbound` — the
  global-lock merge step the drivers call via `poll_transmit`.
- `crates/magnetar-runtime-{tokio,moonpool}/src/{producer,consumer}.rs`
  — the runtime hot paths that bypass the global mutex.
- `crates/magnetar-proto/tests/slot_hot_path.rs` — proto unit layer.
- `crates/magnetar-runtime-{tokio,moonpool}/tests/two_producers_parallel.rs`
  — runtime integration layer (1:1 parity).
- `crates/magnetar-differential/tests/two_producers_parallel_equivalence.rs`
  — tokio↔moonpool equivalence layer.
- [ADR-0003 — no-channels rule](0003-no-channels-rule.md) — the
  canonical concurrency primitive (`parking_lot::Mutex` + `Notify` +
  `Slab<Waker>`) the split is built from.
- [ADR-0004 — sans-io protocol core](0004-sans-io-protocol-core.md) —
  the I/O-isolation contract `magnetar-proto` upholds; the per-slot
  mutex sits inside `magnetar-proto` without violating the zero-I/O
  rule (`parking_lot` is sync-only).
- [ADR-0024 — cross-runtime test + coverage policy](0024-cross-runtime-test-and-coverage-policy.md)
  — the four-layer test policy this refactor satisfies.
- [ADR-0026 — design decisions D1–D4](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
  — preserves D-series compound-operation semantics.
- [ADR-0028 — supervised reconnect anti-thrash policy](0028-supervised-reconnect-anti-thrash-policy.md)
  — `rebuild_producers` / `rebuild_consumers` still walks every slot.
