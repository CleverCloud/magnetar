# ADR-0025 — Engine trait extension: task + timer primitives (façade-lift phase 1)

- **Status**: Accepted
- **Date**: 2026-05-23
- **Decider**: Florentin Dubois
- **Tags**: engine, façade, moonpool-parity, task-spawn, interval, adr-0019-followup

## Context

[ADR-0019](0019-engine-scope-and-moonpool-parity.md) committed magnetar
to keep the user-visible `PulsarClient<E>` API identical between
`PulsarClient<TokioEngine>` and `PulsarClient<MoonpoolEngine<P>>`. The
`Engine` trait at
[`crates/magnetar/src/engine.rs`](../../crates/magnetar/src/engine.rs)
landed as a marker trait with a single associated type
(`ClientState`). Every façade method that touches a producer or
consumer surface — `PartitionedProducer`, `MultiTopicsConsumer`,
`PatternConsumer`, `TableView`, `Reader`, `Transaction`, typed
schemas — currently lives in an `impl PulsarClient<TokioEngine>`
block because the implementations call `tokio::spawn`,
`tokio::time::interval`, `tokio::task::JoinHandle::abort`, etc.

`docs/follow-ups.md` sketched three trait-extension shapes:

1. **Minimal** — `TaskHandle` + `Interval` + `spawn` + `abort_task` +
   `new_interval` + `interval_tick`. Six methods. Lets background-task
   sites (`PartitionedProducer::health_loop`,
   `TableView::drain_task`, `MultiTopicsConsumer::auto_update`)
   compile against either engine. Producers + consumers still flow
   through engine-specific types.
2. **Medium** — Minimal + `Producer<T>` / `Consumer<T>` generic
   associated types. Façade surfaces hold `E::Producer<T>` /
   `E::Consumer<T>` instead of `magnetar_runtime_tokio::Producer`.
3. **Maximal** — Medium + per-surface associated types
   (`E::PartitionedProducer`, `E::TableView`, `E::Reader`, etc.).

The 8-surface façade lift train (~6.4k LOC + matching test counts per
[ADR-0024](0024-cross-runtime-test-and-coverage-policy.md)) cannot
start without picking a shape.

## Decision

Land **option 1 first** as ADR-0025 Phase 1: `TaskHandle` +
`Interval` + `spawn` + `abort_task` + `new_interval` +
`interval_tick`. The producer/consumer associated types from option 2
are deferred to a follow-up ADR-0025-amendment after the first 1–2
surface lifts (Transaction, Reader) prove that the task/timer
abstraction alone unblocks a meaningful subset of the train.

**Trait surface added.**

```rust
pub trait Engine: 'static + Send + Sync + Debug {
    type ClientState: 'static + Send + Sync;          // already there
    fn name() -> &'static str { … }                   // already there

    // ====== Phase 1 additions ======

    /// Opaque, cancel-safe handle to a spawned background task.
    /// Dropping the handle ABORTS the task (tokio) or marks it for
    /// cleanup (moonpool). Use `abort_task` for explicit aborts that
    /// happen-before-Drop.
    type TaskHandle: 'static + Send;

    /// Opaque periodic timer.
    type Interval: 'static + Send;

    /// Spawn an async future on the engine's executor. Returns a
    /// cancel-safe handle. Tokio: wraps `tokio::spawn`. Moonpool:
    /// wraps the providers' `TaskProvider::spawn_task` (tokio under
    /// `TokioProviders`, moonpool-sim under `SimProviders`).
    fn spawn<F>(fut: F) -> Self::TaskHandle
    where
        F: Future<Output = ()> + Send + 'static;

    /// Abort a spawned task. Idempotent — calling on an already-
    /// completed or already-aborted handle is a no-op.
    fn abort_task(handle: &mut Self::TaskHandle);

    /// Create a periodic timer with `period` between ticks. The
    /// first tick fires immediately. Mirrors tokio's
    /// `tokio::time::interval`.
    fn new_interval(period: Duration) -> Self::Interval;

    /// Await the next tick. Returns a `Send` boxed future so the
    /// caller can `.await` from a generic context.
    fn interval_tick(
        interval: &mut Self::Interval,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}
```

**Engine implementations.**

- `TokioEngine`:
  - `type TaskHandle = tokio::task::JoinHandle<()>;`
  - `type Interval = tokio::time::Interval;`
  - `spawn` → `tokio::spawn`.
  - `abort_task` → `JoinHandle::abort`.
  - `new_interval` → `tokio::time::interval`.
  - `interval_tick` → `Box::pin(async move { interval.tick().await; })`.

- `MoonpoolEngine<P>`:
  - Same shape since `moonpool_core::Providers::TaskProvider` is
    tokio-backed under both `TokioProviders` and `SimProviders` (the
    determinism comes from substituting the providers, not from
    replacing tokio). Spawn delegates through `P::task_provider()`
    so simulator-scheduled tasks honour the seed.

**Boxing the tick future** is the only ergonomic compromise — a GAT-
based "associated future type" would avoid the allocation, but pins
us to `feature(generic_associated_types)` semantics around higher-
ranked lifetimes that are still rough. The tick is at most 1/period
of the wall-clock budget; the boxing overhead is irrelevant in
practice.

## Consequences

**Easier:**

- `PartitionedProducer::start_health_loop`,
  `TableView::start_drain_task`, `MultiTopicsConsumer::auto_update`,
  `PatternConsumer::reconcile_loop` move out of
  `impl PulsarClient<TokioEngine>` once their `tokio::spawn` /
  `tokio::time::interval` call sites swap to `E::spawn` /
  `E::new_interval`. Each surface lift becomes a mechanical port
  from this PR's pattern.
- New engines (e.g. a `magnetar-runtime-glommio` follow-up) can
  ship by implementing six well-scoped methods.

**Harder:**

- Boxed-future return on `interval_tick` allocates one `Box` per
  tick. For the background-task sites that already poll on
  millisecond cadences (health loop ≥ 5 s; auto-update ≥ 30 s) the
  allocation is negligible; for any future tight-loop ticker we
  revisit with a custom `Future` type.
- The associated `TaskHandle` and `Interval` types are opaque
  (`type Foo: 'static + Send;`). Façade code can call the methods
  declared here but cannot reach inside the wrapper. That is
  intentional — any leak of engine-specific shape (e.g. tokio's
  `JoinHandle::abort_handle`) would defeat the abstraction.

**Cost:**

- ~120 LOC added to `crates/magnetar/src/engine.rs` (trait
  declaration + two engine impls).
- 4 new unit tests on the `magnetar` crate side (one per primitive
  pair on each engine: spawn/abort, interval tick) — exercises both
  engines so the test gate stays balanced even though we have not
  yet lifted a façade surface that uses them.
- One new ADR file + one row in `specs/README.md`.

**Incompatible with:**

- Engines whose task model is genuinely not tokio-shaped (e.g. a
  hypothetical io_uring single-threaded runtime). Such engines would
  need an ADR-0025 amendment that swaps to associated-future-type
  return values. None are in scope.
- Producer/Consumer generic associated types (option 2). Deferred
  until the first 1–2 façade-surface lifts (Transaction, Reader)
  prove the task/timer primitives alone unblock a meaningful subset.

## References

- [ADR-0019](0019-engine-scope-and-moonpool-parity.md) — Engine
  scope; this ADR is the first follow-up.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) —
  Cross-runtime test parity; the new primitives ship with mirrored
  tests on both engines.
- [`docs/follow-ups.md`](../../docs/follow-ups.md) — three trait-
  extension shapes considered; this ADR selects option 1.
- [`crates/magnetar/src/engine.rs`](../../crates/magnetar/src/engine.rs) —
  trait definition.
