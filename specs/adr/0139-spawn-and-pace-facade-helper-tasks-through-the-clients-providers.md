# ADR-0139 — Spawn and pace façade helper tasks through the client's providers

- **Status**: Accepted (applies the provider boundary of [ADR-0078](0078-adopt-moonpool-0-8-native-deterministic-runtime.md) to the façade's helper tasks; amends the façade's use of the `Engine` task and timer primitives described in [ADR-0025](0025-engine-trait-task-and-timer-primitives.md): those primitives take no client, so a helper task that must run on a client's providers does not use them; the rest of ADR-0025 remains binding)
- **Date**: 2026-11-20
- **Decider**: Florentin Dubois
- **Tags**: engine, facade, moonpool-parity, determinism, task-spawn, table-view, auto-update

## Context

Four façade helper tasks were started with `tokio::spawn` and paced with `tokio::time::interval`: the `TableView` drain (`spawn_drain`) and the auto-update partition ticker (`spawn_auto_update_task`) of `TableView`, `PartitionedProducer` and `MultiTopicsConsumer`.
All of them are reachable from `PulsarClient<MoonpoolEngine<P>>` for any provider bundle `P`, because `TableViewBuilder::create`, `MultiTopicsConsumerBuilder::subscribe` and `PartitionedProducerBuilder::create` are generic over `E: Engine` and `SubscribeApi` / `CreateProducerApi` are implemented for `magnetar_runtime_moonpool::Client<P>` for every `P`.

Under `TokioProviders` that is correct: Tokio is the provider.
Under `SimProviders` it is not.
Since ADR-0078, `SimTaskProvider` runs tasks on Moonpool's own single-threaded seeded executor, with no Tokio runtime and no reactor.
`tokio::spawn` therefore panics ("there is no reactor running"), and `tokio::time::interval`, were a runtime present, would read the host clock and pace the ticker outside the seed.
ADR-0078 states that code depending on `tokio::time` or ambient Tokio task state is incompatible with the Moonpool engine.
Without this change the test `table_view_sim_providers.rs` reproduces both panics: `table_view.rs` `spawn_drain` for the drain and `auto_update_task.rs` for the ticker.

The `Engine` trait already carries `spawn`, `abort_task`, `new_interval` and `interval_tick`, but they are associated functions with no client argument, and `MoonpoolEngine<P>` implements them with `tokio::spawn` / `tokio::time::interval` (ADR-0025's "Tokio shapes" for both engines).
They have no production caller, and they cannot be retrofitted in place: the providers live in the client, not in the marker type.

Alternatives considered:

1. **Refuse these surfaces on non-Tokio providers.** Rejected: `TableView` and the partitioned surfaces are part of the engine-generic façade (ADR-0019), and a seeded simulation of an application that uses them is the point of `SimProviders`.
2. **Give `PulsarClient<E>` a providers field.** Rejected: it adds a type parameter, or a type-erased bundle, to the one type every user names, to serve four internal tasks.
3. **Add the capability to the client state, beside the sleep the client already carries.** Chosen.

## Decision

- **`BackgroundApi`**, a new extension trait, is implemented on each runtime client and is a supertrait of `SubscribeApi` and `CreateProducerApi`, the two bounds every surface that owns a helper task already requires. It has two `#[doc(hidden)]` methods: `spawn_background(name, task) -> join` and `background_sleeper() -> sleep`, where the join future resolves once the task has ended and the sleeper is an owned `Fn(Duration)` on the client's clock whose future resolves `true` once the duration has elapsed and `false` if the clock is gone for good (a time provider that has shut down, as `SimTimeProvider` does once its world is dropped).
- **The Tokio client** spawns with `tokio::spawn` and sleeps with `tokio::time::sleep` (ADR-0078: those are the Tokio providers).
- **The Moonpool client** captures a type-erased task spawner at construction, next to the sleep provider it already captures from the same bundle: `connect_plain`, the supervised and TLS connects, and `from_parts_with_providers` bind `providers.task()`; `from_parts`, the Tokio-backed convenience, binds `TokioTaskProvider`, as it binds `TokioTimeProvider` for the sleep. `spawn_background` calls `TaskProvider::spawn_task`, `background_sleeper` calls `TimeProvider::sleep`. Under `SimProviders` the task runs on the seeded executor and the wait is virtual time.
- **`BackgroundTask`** (façade, private) is the one owner of a spawned helper task. A `TaskProvider` join handle cannot cancel its task, and `tokio::task::JoinHandle::abort` is not reachable through `BackgroundApi`, so the stop is cooperative on both engines: the task body is raced, biased towards the stop, against a `Notify` that `stop()` signals with `notify_one` (which keeps a permit, so a stop that lands before the first poll or between two awaits is not lost). Dropping the `BackgroundTask` stops the task; `shutdown()` stops it and waits for it to end. `TableView::close` uses `shutdown()` for the drain and for the ticker, so it still returns only once both have ended.
- **The ticker** is `while sleep(interval).await { changed.notify_waiters() }`, so a clock that is gone ends the task instead of leaving a loop that never yields and that no stop signal could interrupt. Otherwise it is the former `interval` with `MissedTickBehavior::Delay` and its immediate first tick consumed: the first signal comes one interval after the spawn, each next one an interval after the previous. The ticker's own `shutdown` notify and the `Drop` of `AutoUpdateTask` are gone; `BackgroundTask` owns both.
- `spawn_auto_update_task` and `spawn_drain` take the owning client's state (`&self.client.inner`) as their first argument. The Tokio-only builder paths (`TableViewBuilder::create_with_decryption`, `PartitionedProducerBuilder::create_with_encryption`) pass the Tokio client.

## Consequences

- **A required supertrait is a break for an external implementor** of `SubscribeApi` or `CreateProducerApi`, who must now implement `BackgroundApi` too. The workspace has no other implementor, and the traits were never documented as an extension point.
- **Tokio teardown moves from `abort()` to a cooperative stop.** The task ends at the same point: the next time the runtime polls it, which `notify_one` schedules exactly as `abort` does. `AutoUpdateTask::drop_aborts_spawned_task` keeps its assertion that the task is finished after a drop and one yield.
- **`SimProviders` replay.** The drain and the ticker are spawned and paced inside the simulation, so the same `(commit, seed)` replays them. `background_tasks_equivalence.rs` shows the ticker firing on virtual time and the drain applying a pushed message with no Tokio runtime at all.
- **The `Engine` primitives are unchanged and remain unused.** Their documentation is corrected to say what they do: dropping a `TaskHandle` detaches the task (`tokio::task::JoinHandle`) on both engines, and `MoonpoolEngine::spawn` is `tokio::spawn`, not a `TaskProvider` call. New code that needs a task or a timer on a client's providers uses `BackgroundApi`.
- **Not addressed.** Other façade tasks still use ambient Tokio and so still need a Tokio runtime under `SimProviders`: the push-delivery pollers (`consumer_listener.rs`) and `PatternConsumer::start_auto_reconcile`. They are separate call sites with their own lifetimes (a returned `JoinHandle` in the second case) and were not part of this change.
- **Test layers, as they are.** (a) `magnetar-proto`: not applicable, nothing changed there. (b) `magnetar-runtime-tokio`: not applicable, the Tokio client is unchanged; its `BackgroundApi` impl is façade code. (c) `magnetar-runtime-moonpool`: `Client::spawn_background` and `background_sleeper` are `doc(hidden)` seams over the client's captured providers and are exercised by the simulated leg below; a runtime test of its own would need a Tokio twin to keep `check-runtime-test-parity`, and there is no Tokio method to mirror. (d) `magnetar-differential`: `table_view_sim_providers.rs` (the spawn under `SimProviders` with no Tokio runtime) and `background_tasks_equivalence.rs` (one scenario generic over the engine: a keyed message drains into the `TableView`, each ticker fires once its interval elapsed, `TableView::close` leaves no tick behind, and dropping a `MultiTopicsConsumer` or `PartitionedProducer` stops its ticker; run on `PulsarClient<TokioEngine>` against a loopback broker and on `PulsarClient<MoonpoolEngine<SimProviders>>` inside a seeded simulation, both required to equal the same trace). Unit tests in `background_task.rs` and `auto_update_task.rs` cover the stop signal, the early stop, `shutdown`, and a ticker that ends by itself when its clock is gone. No `e2e_*.rs` test was added: `e2e_compacted.rs` and `e2e_partitioned_deep.rs` already reach these surfaces through a real broker on the Tokio engine, and a real broker cannot reach the `SimProviders` path, which is the changed behaviour.

## References

- `crates/magnetar/src/background_task.rs` — `BackgroundTask`.
- `crates/magnetar/src/engine/mod.rs` — `BackgroundApi`; `crates/magnetar/src/engine/tokio.rs`, `crates/magnetar/src/engine/moonpool.rs` — the two impls.
- `crates/magnetar-runtime-moonpool/src/client.rs`, `crates/magnetar-runtime-moonpool/src/lib.rs` — `Client::spawn_background`, `Client::background_sleeper`, the captured task spawner.
- `crates/magnetar/src/auto_update_task.rs`, `crates/magnetar/src/table_view.rs`, `crates/magnetar/src/multi_topics.rs`, `crates/magnetar/src/partitioned_producer.rs` — the call sites.
- [ADR-0078](0078-adopt-moonpool-0-8-native-deterministic-runtime.md), [ADR-0025](0025-engine-trait-task-and-timer-primitives.md), [ADR-0037](0037-multi-topics-pattern-consumer-pass-2-lift.md), [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md).
