# Split-Connection-Mutex Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move per-handle hot state (queues, wakers, `pending_index`, per-handle outbound staging) out of the global `Arc<parking_lot::Mutex<magnetar_proto::Connection>>` so independent producer/consumer hot paths stop serialising through one lock.

**Architecture:** Introduce `Arc<ProducerSlot>` / `Arc<ConsumerSlot>` in `magnetar-proto`, each holding (a) immutable identity, (b) a per-slot `parking_lot::Mutex<HotState>`, and (c) atomic counters for cheap reads. `Connection` keeps a `HashMap<Handle, Arc<Slot>>` for registration + frame routing; the protocol-mutation lock guards state-machine transitions, frame buffers, `pending_requests`, the handle registry, and the `events` queue. Runtime handles (`Producer` / `Consumer`) hold a direct `Arc<Slot>` cloned at create time. Hot paths take the per-slot lock only. Lock ordering is **global → per-slot, never the reverse**.

**Tech Stack:** Rust 2024, `parking_lot::Mutex`, `tokio::sync::Notify`, `Slab<Waker>` (per ADR-0003). No new dependencies.

---

## Realistic scope note

This is a **major architectural refactor** affecting ~290 lock-acquisition sites across `magnetar-proto`, `magnetar-runtime-tokio`, and `magnetar-runtime-moonpool`. The ADR-0024 four-layer test policy (proto unit + tokio integration + moonpool integration + differential equivalence + 100 %-on-diff sim coverage + tokio↔moonpool 1:1 test parity) means **every behavioural change ships with four tests in the same commit**.

The plan is divided into **four phases**, each landable as a self-contained commit (or small commit series) on `refactor/split-connection-mutex`:

- **Phase 1 — Foundation (zero behaviour change).** Wrap `ProducerState` / `ConsumerState` in `Arc<…Slot>` inside `Connection`. All existing `Connection::*_producer / *_consumer` accessors keep the same signatures and call sites; internally they go through the slot's mutex (still held under the global mutex). This is a pure indirection layer — no parallelism win yet, but enables phases 2-4.
- **Phase 2 — Direct slot access on cold metadata.** Expose `Arc<ProducerSlot>` / `Arc<ConsumerSlot>` to the runtime handles. Convert observability getters (`is_connected` excepted: it reads `Connection.state`) like `producer_topic`, `producer_name`, `producer_stats`, `consumer_queue_len`, `producer_pending_count`, `producer_batch_*` to read directly from the slot, bypassing the global lock. Lock-ordering invariant is documented in the module header.
- **Phase 3 — Hot path: producer send + flow control + consumer receive.** Move queue mutation + waker registration to the per-slot lock; the driver does the remaining "merge per-slot outbound staging into `Connection.outbound`" work. This is the phase that produces the throughput win.
- **Phase 4 — Tests + ADR.** Parallel-throughput benchmark, lock-ordering test, differential parity test, ADR-0038 landing, follow-ups bookkeeping.

The phases can be reviewed independently. **Stop after each phase, ask Florentin to merge it back to `main` via `wt merge`, then start the next phase on a fresh worktree** — this keeps PR review tractable and limits blast radius.

---

## File Structure

**Phase 1 — Foundation files**

- Modify: `crates/magnetar-proto/src/producer.rs` — wrap state in `ProducerSlot { state: Mutex<ProducerState>, ... }` + define identity-stripped `ProducerIdentity`.
- Modify: `crates/magnetar-proto/src/consumer.rs` — symmetric `ConsumerSlot { state: Mutex<ConsumerState>, ... }`.
- Modify: `crates/magnetar-proto/src/conn.rs` — replace `HashMap<ProducerHandle, ProducerState>` with `HashMap<ProducerHandle, Arc<ProducerSlot>>`; rewrite every internal `self.producers.get_mut(&handle)` to lock through the slot. Same for consumers.
- Modify: `crates/magnetar-proto/src/lib.rs` — `pub use` the new slot types.
- Test: `crates/magnetar-proto/tests/slot_indirection.rs` (new) — assert lock layering on a focused scenario; differential test added in Phase 4.

**Phase 2 — Cold-path direct access**

- Modify: `crates/magnetar-runtime-tokio/src/producer.rs` — replace observability `inner.lock().producer_*(handle)` calls with direct `slot.*` reads. Keep `is_connected` / `last_disconnected_timestamp` (they read `Connection`-level state).
- Modify: `crates/magnetar-runtime-tokio/src/consumer.rs` — same for `consumer_*` observability.
- Modify: `crates/magnetar-runtime-moonpool/src/{producer,consumer}.rs` — same. Must keep 1:1 test count vs tokio (`cargo xtask check-runtime-test-parity`).
- Modify: `crates/magnetar-runtime-tokio/src/lib.rs` + `crates/magnetar-runtime-moonpool/src/lib.rs` — module-level comment documenting the lock-ordering invariant.

**Phase 3 — Hot-path split**

- Modify: `crates/magnetar-proto/src/producer.rs` — `ProducerState::queue_send` returns a `Vec<OutboundFrame>` of staged frames; the slot holds the queue. Atomics for `pending_count` / `closed` so cold paths can skip the lock.
- Modify: `crates/magnetar-proto/src/consumer.rs` — `ConsumerState::pop_message` decoupled from frame emission; the slot exposes a per-slot `wants_flow: AtomicBool` that the driver picks up.
- Modify: `crates/magnetar-proto/src/conn.rs` — driver-facing `drain_all_outbound` helper that takes the global lock once + walks every slot, taking the per-slot lock briefly to drain staging.
- Modify: `crates/magnetar-runtime-tokio/src/producer.rs::queue_send` — no longer takes `inner.lock()`; takes only `slot.state.lock()`, then `driver_waker.notify_one()`.
- Modify: `crates/magnetar-runtime-tokio/src/consumer.rs::recv_one` — analogous.
- Modify: `crates/magnetar-runtime-moonpool/src/{producer,consumer,driver}.rs` — mirror.

**Phase 4 — Tests + ADR**

- Create: `crates/magnetar-runtime-tokio/tests/two_producers_parallel.rs` — drive 2 producers from 2 tasks, assert wall-clock parallelism.
- Create: `crates/magnetar-runtime-moonpool/tests/two_producers_parallel.rs` — symmetric (must keep 1:1 count).
- Create: `crates/magnetar-differential/tests/two_producers_parallel.rs` — tokio↔moonpool equivalence.
- Create: `crates/magnetar-proto/tests/lock_ordering.rs` — exercise reconnect rebuild path that touches every slot; assert no deadlock under contention.
- Create: `specs/adr/0038-split-connection-mutex.md` + update `specs/README.md`.
- Modify: `ARCHITECTURE.md`, `CLAUDE.md` (invariants list), `docs/architecture-overview.md` — document the new lock layering.
- Modify: `docs/follow-ups.md` — close the P1 audit finding.

---

## Constraints (carry through every phase)

1. **No channels.** Per ADR-0003 — `parking_lot::Mutex` + `tokio::sync::Notify` + `Slab<Waker>`.
2. **`magnetar-proto` zero I/O deps.** Per ADR-0004 — verified by `cargo xtask check-no-io-deps`.
3. **Sans-io clock injection.** Per ADR-0011 — every new path that needs a clock takes it as a parameter; the slot mutex never reads the host clock.
4. **Moonpool parity.** Per ADR-0019 — the differential harness stays green; if you touch one engine, you touch both.
5. **No panics in `magnetar-proto`.** All new locks return `Result`/`Option` where the invariant can't be otherwise proven.
6. **Compound invariants stay intact.** Frame processing in `Connection::handle_bytes` still takes the global lock; only when it needs to enqueue into a per-handle queue does it drop the per-slot lock briefly. Lock-ordering: **global → per-slot, never the reverse**. Document in a module comment.
7. **Cross-runtime + coverage policy.** Per ADR-0024 — four test layers per behavioural change, 100 % moonpool sim coverage on diff, 1:1 tokio↔moonpool test count.

---

## Validation (run at the end of each phase)

```
cargo +nightly fmt --all
cargo build --workspace --all-features
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features
cargo deny check
RUSTDOCFLAGS="-D warnings --cfg tokio_unstable" cargo doc --workspace --all-features --no-deps --locked
cargo xtask check-no-channels
cargo xtask check-no-io-deps
cargo xtask check-no-internal-clock
cargo xtask codegen --check
cargo xtask check-sim-coverage
cargo xtask check-runtime-test-parity
cargo xtask check-crypto-matrix
```

Skip the local seed sweep — the daily CI sweep covers it (per `feedback-skip-local-seed-sweep` memory).

---

# Phase 1 — Foundation (Arc<Slot> indirection)

**Goal:** Zero behavioural change. After Phase 1, `Connection` holds `HashMap<Handle, Arc<Slot>>` instead of `HashMap<Handle, State>`; every accessor goes through the slot's mutex, still under the global mutex.

### Task 1.1: Introduce `ProducerSlot` and `ProducerIdentity`

**Files:**
- Modify: `crates/magnetar-proto/src/producer.rs`

- [ ] **Step 1: Add `ProducerIdentity` (immutable per-handle metadata)**

After the existing `ProducerState` struct, add:

```rust
/// Immutable per-producer metadata, set at create time and never mutated.
/// Held inside [`ProducerSlot`] so cold-path observers can read it without
/// taking the slot's mutex.
#[derive(Debug, Clone)]
pub struct ProducerIdentity {
    /// Producer id assigned by the connection.
    pub handle: ProducerHandle,
    /// Topic name.
    pub topic: String,
    /// Access mode the producer was opened with. Snapshotted from
    /// [`crate::conn::CreateProducerRequest`] at create time.
    pub access_mode: pb::ProducerAccessMode,
}

/// Per-producer slot: immutable identity + mutex-guarded hot state.
/// `Arc<ProducerSlot>` is the long-lived handle the runtime engines clone
/// into their `Producer` value; `Connection` stores the same `Arc` in its
/// producer registry. Cold-path observability (topic, access mode) reads the
/// identity without locking; mutable operations take `state.lock()`.
///
/// Lock-ordering invariant (project-wide): **global `Connection` mutex →
/// per-slot mutex, never the reverse.** Concretely: a thread that holds
/// `state.lock()` MUST NOT then take the connection-wide mutex; a thread
/// that holds the connection mutex MAY drop it before taking
/// `state.lock()` on any slot. See ADR-0038.
#[derive(Debug)]
pub struct ProducerSlot {
    /// Immutable identifying metadata.
    pub identity: ProducerIdentity,
    /// Mutex-guarded state-machine state. Hot path for queue / waker /
    /// outbound-staging operations.
    pub state: parking_lot::Mutex<ProducerState>,
}

impl ProducerSlot {
    /// Construct a slot for a newly-opened producer.
    pub fn new(identity: ProducerIdentity, state: ProducerState) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            identity,
            state: parking_lot::Mutex::new(state),
        })
    }
}
```

- [ ] **Step 2: Run `cargo build -p magnetar-proto` — expected: compiles.**

```
cargo build -p magnetar-proto
```

(Build only the proto crate to catch type errors fast before touching call sites.)

- [ ] **Step 3: Commit (scaffold-only)**

```
git add crates/magnetar-proto/src/producer.rs
git commit -s -S -m "refactor(proto): add ProducerSlot + ProducerIdentity scaffolding

Adds the per-producer indirection types that Phase 1 of the
split-connection-mutex refactor will route Connection accesses
through. Pure additive — no call sites use them yet."
```

### Task 1.2: Introduce `ConsumerSlot` and `ConsumerIdentity`

Symmetric to 1.1. Same shape: `ConsumerIdentity { handle, topic, subscription }`, `ConsumerSlot { identity, state: Mutex<ConsumerState> }`.

- [ ] **Step 1: Add the types to `crates/magnetar-proto/src/consumer.rs`.** Follow the exact same shape as Task 1.1.
- [ ] **Step 2: `cargo build -p magnetar-proto`.**
- [ ] **Step 3: Commit `refactor(proto): add ConsumerSlot + ConsumerIdentity scaffolding`.**

### Task 1.3: Switch `Connection.producers` to `HashMap<ProducerHandle, Arc<ProducerSlot>>`

**Files:**
- Modify: `crates/magnetar-proto/src/conn.rs`

This is the load-bearing change. Every `Connection::*_producer*` method that reaches into `self.producers.get_mut(&handle)` becomes `self.producers.get(&handle).map(|slot| { let state = slot.state.lock(); ... })`.

- [ ] **Step 1: Change the struct field type.**

```rust
producers: HashMap<ProducerHandle, std::sync::Arc<ProducerSlot>>,
```

- [ ] **Step 2: Update `Connection::new` and `Connection::reset` to construct slots through `ProducerSlot::new`.**

- [ ] **Step 3: For each Connection method that mutated `ProducerState`, switch to slot-mediated access. Compile incrementally — `cargo check -p magnetar-proto` after each handful.**

Affected methods (audit before code, prevent missing one):
- `create_producer` — constructs the slot
- `send`, `flush_producer`, `producer_pending_count`, `producer_batch_len`, `producer_batch_bytes`, `producer_access_mode`, `producer_last_sequence_id_pushed`, `producer_last_sequence_id_published`, `producer_stats`, `producer_record_rate_window`, `producer_is_closed`, `producer_topic`, `producer_name`, `close_producer`, `rebuild_producers`, `drain_producer_outbound`
- Every `CommandSendReceipt`, `CommandSendError`, `CommandAckResponse` handler inside `handle_bytes` / `process_command` that walks `self.producers`
- Reconnect path (`reset`, `in_flight_publish_snapshots` snapshotting)

- [ ] **Step 4: Run `cargo test -p magnetar-proto` — expected: every pre-existing proto test still passes. Zero behaviour change.**

- [ ] **Step 5: Commit `refactor(proto): route Connection.producers through Arc<ProducerSlot>`.**

### Task 1.4: Same for `Connection.consumers`

Symmetric to Task 1.3. After this, every reach into `self.consumers.get_mut(&handle)` goes through `slot.state.lock()`.

- [ ] **Step 1: Audit `consumer_*` methods + frame-handling sites in `handle_bytes` + `rebuild_consumers`.**
- [ ] **Step 2: Switch field type + update accessors.**
- [ ] **Step 3: `cargo test -p magnetar-proto`.**
- [ ] **Step 4: Commit.**

### Task 1.5: Phase 1 cross-runtime sanity

After 1.3 + 1.4, **no runtime call site changes** — the runtime crates still call `conn.send(handle, …)` etc. Run the full validation chain:

- [ ] **Step 1: `cargo build --workspace --all-features`.**
- [ ] **Step 2: `cargo clippy --workspace --all-features --all-targets -- -D warnings`.**
- [ ] **Step 3: `cargo test --workspace --all-features`.**
- [ ] **Step 4: `cargo xtask check-no-channels`.**
- [ ] **Step 5: `cargo xtask check-no-io-deps`.**
- [ ] **Step 6: `cargo xtask check-no-internal-clock`.**
- [ ] **Step 7: `cargo xtask check-sim-coverage` — should already be green; Phase 1 doesn't add lines, only re-arranges existing ones.**
- [ ] **Step 8: `cargo xtask check-runtime-test-parity` — should still be 1:1.**

If anything regresses, fix in-place before phase transition.

- [ ] **Step 9: Pause for `wt step diff -- --stat` review and `wt merge -y` before starting Phase 2.**

---

# Phase 2 — Direct slot access for cold-path observability

**Goal:** Skip the global lock on `Producer::topic()`, `Producer::name()`, `Producer::stats()`, `Producer::access_mode()`, `Consumer::topic()`, `Consumer::subscription()`, `Consumer::queue_len()` (read-only counters). After Phase 2, these methods take only the per-slot lock (or no lock at all, for identity reads).

### Task 2.1: Expose `Arc<ProducerSlot>` on `Producer`

**Files:**
- Modify: `crates/magnetar-runtime-tokio/src/producer.rs`
- Modify: `crates/magnetar-runtime-moonpool/src/producer.rs`

- [ ] **Step 1: Add `pub(crate) slot: Arc<magnetar_proto::ProducerSlot>` to `Producer`.**
- [ ] **Step 2: Update the constructor path in `Client::create_producer` to capture the slot from `Connection::create_producer` and stash it on the `Producer`.**
  - This needs a new `Connection::producer_slot(&self, handle) -> Option<Arc<ProducerSlot>>` accessor (no lock — read from the registry).
- [ ] **Step 3: `cargo build -p magnetar-runtime-tokio -p magnetar-runtime-moonpool`.**
- [ ] **Step 4: Commit `refactor(runtime): wire Arc<ProducerSlot> onto Producer`.**

### Task 2.2: Convert cold-path observability methods (tokio + moonpool)

- [ ] **Step 1: Replace `self.shared.inner.lock().producer_topic(self.handle)` with `Some(self.slot.identity.topic.as_str())` etc.**
- [ ] **Step 2: For methods that need the state mutex (e.g. `producer_pending_count`), call `self.slot.state.lock().pending.len()` directly.**
- [ ] **Step 3: Add tests:**
  - `crates/magnetar-runtime-tokio/tests/producer_observability_no_global_lock.rs` — assert that observability methods complete while a long-running global-lock holder is in flight (prove no global-lock contention).
  - `crates/magnetar-runtime-moonpool/tests/producer_observability_no_global_lock.rs` — moonpool mirror with deterministic timing.
- [ ] **Step 4: Run validation chain. Coverage on the diff must stay 100 % for the moonpool runtime.**
- [ ] **Step 5: Commit.**

### Task 2.3: Same for consumer observability

Symmetric. `Consumer::queue_len`, `Consumer::available_permits` (atomic), `Consumer::topic`, `Consumer::subscription`.

### Task 2.4: Document lock-ordering at the module level

- [ ] **Step 1: Add a module-level comment to `crates/magnetar-proto/src/conn.rs`:**

```rust
//! ## Lock-ordering invariant
//!
//! `Connection` is guarded by an outer `parking_lot::Mutex` in the runtime
//! engines; each [`ProducerSlot`] / [`ConsumerSlot`] carries its own
//! `parking_lot::Mutex`. The acquisition order is:
//!
//! 1. **Global → per-slot** is safe (and the only path the codebase takes).
//! 2. **Per-slot → global is FORBIDDEN.** A holder of `slot.state.lock()`
//!    that needs Connection-level state must release the slot lock first.
//!
//! Violating this rule will deadlock under contention.
```

- [ ] **Step 2: Same comment block on `magnetar-runtime-tokio/src/lib.rs` (next to `ConnectionShared`) + `magnetar-runtime-moonpool/src/lib.rs`.**
- [ ] **Step 3: Commit `docs(arch): document lock-ordering invariant for ProducerSlot/ConsumerSlot`.**

### Task 2.5: Phase 2 sanity

Full validation chain. Pause for review + `wt merge -y`.

---

# Phase 3 — Hot-path split (producer send, consumer receive, flow control)

**Goal:** The producer-send and consumer-receive hot paths no longer take the global lock. The driver merges per-slot outbound staging into the connection's byte buffer.

### Task 3.1: Per-slot outbound staging

**Files:**
- Modify: `crates/magnetar-proto/src/producer.rs`

- [ ] **Step 1: Replace `ProducerState.outbound: VecDeque<OutboundFrame>` with `ProducerSlot.outbound: parking_lot::Mutex<VecDeque<OutboundFrame>>`.**
  - Reason: the queue is per-slot; the global lock no longer needs to be involved.
- [ ] **Step 2: Update `ProducerState::queue_send` to return staged frames; the caller (slot wrapper) appends them to `slot.outbound`.**
- [ ] **Step 3: New `Connection::drain_all_producer_outbound` walks the registry, taking each `slot.outbound.lock()` briefly to merge into `self.outbound` byte buffer.**
- [ ] **Step 4: Driver loop calls `drain_all_producer_outbound()` right before encoding to the socket.**
- [ ] **Step 5: Four-layer tests:**
  - proto unit: `crates/magnetar-proto/tests/producer_outbound_staging.rs` — exercise the staging + drain flow.
  - tokio integration: `crates/magnetar-runtime-tokio/tests/producer_outbound_staging.rs`.
  - moonpool integration: `crates/magnetar-runtime-moonpool/tests/producer_outbound_staging.rs`.
  - differential: `crates/magnetar-differential/tests/producer_outbound_staging.rs`.
- [ ] **Step 6: Validation chain.**
- [ ] **Step 7: Commit.**

### Task 3.2: `Producer::queue_send` takes only the slot lock

**Files:**
- Modify: `crates/magnetar-runtime-tokio/src/producer.rs`
- Modify: `crates/magnetar-runtime-moonpool/src/producer.rs`

- [ ] **Step 1: Replace the `self.shared.inner.lock()` in `queue_send` with `self.slot.state.lock()` + `self.slot.outbound.lock()`.**
- [ ] **Step 2: Drive the driver via `self.shared.driver_waker.notify_one()` as before.**
- [ ] **Step 3: Add the parallel-throughput moonpool integration test that two producers on the same connection make wall-clock-parallel progress.**
- [ ] **Step 4: Mirror in tokio.**
- [ ] **Step 5: Differential test that the `EventStream` is identical.**
- [ ] **Step 6: Validation chain.**
- [ ] **Step 7: Commit.**

### Task 3.3: Consumer receive — per-slot queue + waker

**Files:**
- Modify: `crates/magnetar-proto/src/consumer.rs`
- Modify: `crates/magnetar-runtime-tokio/src/consumer.rs`
- Modify: `crates/magnetar-runtime-moonpool/src/consumer.rs`

- [ ] **Step 1: Move `ConsumerState.queue` + `ConsumerState.receive_wakers` into `ConsumerSlot.queue: Mutex<VecDeque<…>>` + `ConsumerSlot.receive_wakers: Mutex<Slab<Waker>>`. Provide `slot.try_pop()` / `slot.park_waker()` / `slot.wake_one()` helpers.**
- [ ] **Step 2: Driver, on inbound message: takes global lock to find the slot, drops it, takes per-slot lock to push to queue + wake one waker. Per-handle flow control: set `slot.wants_flow: AtomicBool` so the next driver tick emits the CommandFlow.**
- [ ] **Step 3: `Consumer::recv_one` takes only the per-slot lock.**
- [ ] **Step 4: Four-layer tests for: parallel-receive (two consumers progress in wall-clock parallel) + flow-control-still-fires + no-lost-wakeup races.**
- [ ] **Step 5: Validation chain.**
- [ ] **Step 6: Commit.**

### Task 3.4: Phase 3 sanity

Full validation chain + pause for review.

---

# Phase 4 — Tests, ADR, docs, follow-ups

### Task 4.1: Lock-ordering test under contention

- [ ] **Step 1: Create `crates/magnetar-proto/tests/lock_ordering.rs`.**
  - Exercise the supervisor reconnect path (`rebuild_producers` + `rebuild_consumers`) — the most complex multi-handle compound op — under simulated contention.
  - Use `parking_lot::Mutex::try_lock_for(Duration)` to detect deadlocks deterministically.
- [ ] **Step 2: Commit.**

### Task 4.2: ADR-0038 — Split Connection Mutex

- [ ] **Step 1: Write `specs/adr/0038-split-connection-mutex.md` covering:**
  - Status (Accepted), date.
  - Context: P1 audit finding 2026-05-27, ~290 lock sites funnelling through one mutex.
  - Decision: per-slot mutex with strict global → per-slot lock ordering.
  - Alternatives considered: DashMap (rejected — no shard contention while everything funnels through global lock); RwLock (rejected — every op is `&mut self`); one-big-lock (status quo).
  - Consequences: parallel-throughput win, lock-ordering invariant, doc + comment + test enforcement.
  - Parity guarantees: tokio↔moonpool differential test landed alongside.
- [ ] **Step 2: Update `specs/README.md` ADR index.**
- [ ] **Step 3: Update `ARCHITECTURE.md` (lock-layering section) + `docs/architecture-overview.md`.**
- [ ] **Step 4: Update `CLAUDE.md` invariants list to mention the lock-ordering rule.**
- [ ] **Step 5: Close the P1 entry in `docs/follow-ups.md`.**
- [ ] **Step 6: Commit `docs(adr): land ADR-0038 split connection mutex`.**

### Task 4.3: Final validation + merge

- [ ] **Step 1: Full validation chain.**
- [ ] **Step 2: `wt step diff -- --stat` + final review.**
- [ ] **Step 3: `wt merge -y` after Florentin confirms.**

---

## Self-review checklist

- [ ] Every spec acceptance criterion (1-5 from the user prompt) is covered by at least one task above.
- [ ] No "TBD", "TODO", "implement later" placeholders in the body.
- [ ] Types referenced in later tasks (`ProducerSlot`, `ProducerIdentity`, `ConsumerSlot`, …) are defined in earlier tasks.
- [ ] Every behavioural-change task lists all four test layers + the validation chain.
- [ ] The lock-ordering rule (global → per-slot, never reverse) is documented in: module comment (Task 2.4) + ADR (Task 4.2) + dedicated test (Task 4.1).
- [ ] `wt merge -y` pause points after each phase keep PR review tractable.
