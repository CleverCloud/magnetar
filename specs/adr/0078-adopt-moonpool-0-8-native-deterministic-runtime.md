# ADR-0078 — Adopt Moonpool 0.8's native deterministic runtime

- **Status**: Accepted
- **Date**: 2026-07-16
- **Decider**: Florentin Dubois
- **Tags**: dependencies, moonpool, simulation, determinism, observability

## Context

[ADR-0056](0056-moonpool-0-7-crates-io-repin.md) moved Magnetar from a temporary Moonpool git dependency to the published Moonpool 0.7 crates.
Moonpool 0.7 still ran simulation actors through `TokioTaskProvider`, and Magnetar's Moonpool runtime retained ambient Tokio task, timer, and `select!` assumptions in paths that also execute under `SimProviders`.
Its observability API exposed typed trails through `TrailQuery` / `TrailQueryExt`, `Valuable`, and a Serde bridge.

Moonpool 0.8 changes the simulation contract:

- `SimTaskProvider` schedules tasks on Moonpool's single-threaded seeded deterministic executor rather than an ambient Tokio runtime.
- `NetworkProvider`, `TimeProvider`, `TaskProvider`, and `Providers` are `Send + Sync`, and provider futures are `Send`.
- `moonpool_core::select!` uses Moonpool's seeded branch-order source while retaining Tokio's `select!` branch semantics.
- `TimeProvider::sleep` and `TimeProvider::timeout` are the runtime-independent timer boundary.
- Simulation observability captures ordinary named `tracing` events inside actor spans and exposes flat `TraceEvent` values through `TraceQuery`.

Keeping Tokio task or timer calls inside provider-generic runtime paths would bypass the deterministic executor or panic when a simulation runs without a Tokio reactor.
Keeping `tokio::select!` would also make fair branch selection depend on Tokio's process-local randomness instead of the simulation seed.

## Decision

Upgrade `moonpool-core` and `moonpool-sim` together to the published crates.io `0.8` line and make the provider boundary authoritative throughout the Moonpool engine.

- Spawn Moonpool runtime work through `TaskProvider::spawn_task`.
- Drive runtime sleeps and timeouts through `TimeProvider`.
- Use `moonpool_core::select!` in provider-generic Moonpool code so branch ordering is reproducible from the simulation seed.
- Run `SimProviders` workloads on Moonpool's deterministic executor without an ambient Tokio runtime.
- Keep `TokioProviders` for production-style and differential tests that intentionally use real Tokio networking and wall-clock time.
- Keep provider-owned deadline functions in private `Client` / `Consumer` runtime state; `Client::from_parts_with_providers` preserves custom and simulation clocks without adding a private field to the public `ConnectionShared` layout.
- Emit ordinary constant-name `tracing` events with flat structured fields inside an actor span carrying `ip`; query them through `TraceQuery`.
- Compare `TraceEvent::seq` when an invariant relates different event names, because `TraceQuery` cursors are per name and query order is not temporal order.
- Remove the Moonpool 0.7 `TrailQuery` / `TrailQueryExt`, `Valuable`, and Serde payload bridge.
- Keep caret requirements in `Cargo.toml`; `Cargo.lock` plus `--locked` validation remains the reproducibility anchor for a reviewed `(commit, seed)` pair.

## Consequences

- The same Moonpool runtime code now executes on real Tokio providers and on the native deterministic executor without a hidden Tokio-reactor dependency.
- Task scheduling, time advancement, timeout races, and unbiased `select!` branch order are controlled by the simulation seed.
- `biased;` selections retain explicit source order where protocol fairness or shutdown priority requires it.
- Simulation invariants consume the same structured `tracing` signals that production observability can export, without a simulation-only event payload type.
- Provider-generic futures must remain `Send`; code that depends on `spawn_local`, `tokio::time`, or ambient Tokio task state is incompatible with the Moonpool engine.
- Pool dials own one provider-native operation deadline around connect plus handshake, are generation-checked before promotion, and expose a cancellation/completion handshake so pool close resolves waiters and waits for pending work to exit.
- [ADR-0056](0056-moonpool-0-7-crates-io-repin.md) is superseded because its `0.7` dependency decision is no longer current.
- [ADR-0039](0039-pulsar-proxy-multi-broker-connection-model.md) remains the binding proxy connection model, but its Moonpool-specific `Send` propagation implementation note is amended by this decision.
- [ADR-0036](0036-moonpool-seed-sweep-daily-random.md) remains in force: the daily random-seed cadence and lockfile-based replay contract are unchanged.

## References

- `Cargo.toml` — `moonpool-core = "^0.8.0"` and `moonpool-sim = "^0.8.0"`.
- `crates/magnetar-runtime-moonpool/src/driver.rs` — provider-native task, time, and deterministic selection.
- `crates/magnetar-runtime-moonpool/src/transport.rs` — provider-native connection timeout selection.
- `crates/magnetar-runtime-moonpool/tests/sim_chaos.rs` — native deterministic-executor workload and flat `TraceQuery` invariants.
- [`docs/moonpool-engine.md`](../../docs/moonpool-engine.md) — current engine and simulation contract.
- Supersedes [ADR-0056](0056-moonpool-0-7-crates-io-repin.md).
