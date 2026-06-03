# ADR-0052 — Initial-connect timeout + bounded retry

- **Status**: Accepted
- **Date**: 2026-06-03
- **Decider**: Florentin Dubois
- **Tags**: runtime, connection, resilience, moonpool, java-parity, determinism

## Context

The daily `moonpool-seed-sweep` had been red for four consecutive runs, and several seeds (e.g. `0x26b52c2c3cc5bf73`) deadlocked `sim_chaos_produce_consume_with_invariants` and the `*_sweep_16_seeds` tests deterministically.
The orchestrator reported a no-progress / DEADLOCK early-exit (`moonpool-sim/src/runner/orchestrator.rs`), and the failing `SimulationReport` carried the all-default metrics (`events_processed: 0`, empty `individual_metrics`) that the failure path leaves behind.

The seeds are **not** linked to any recent magnetar change.
`git diff --stat 4cda3e6..HEAD` shows the entire `magnetar-runtime-moonpool` crate **and** `Cargo.lock` are byte-identical across the range, so no commit in it — including the ADR-0051 partition-metadata precheck (`ca4ad17`), which touches only the `magnetar` façade — can affect the sim.
The failure reproduces on `HEAD` while the default seed passes.

Root cause, traced with broker- and client-side instrumentation: the client workload's `run()` starts, calls `Client::connect_plain`, and **hangs inside the moonpool `NetworkProvider::connect`** — the broker's `listener.accept()` never returns and no `CommandConnect` is ever sent.
moonpool's sim network defaults to `ConnectFailureMode::Probabilistic` (`moonpool-sim/src/network/config.rs`, a FoundationDB `SIM_CONNECT_ERROR_MODE = 2` port).
When the connect `buggify!()` location is active for a run, each `connect()` either fails fast or **hangs forever** (`std::future::pending()`), by design, _"to test timeout handling in connection retry logic."_

magnetar's initial dial (`Transport::connect` → `NetworkProvider::connect`, a bare `.await`) had **no connect timeout and no retry**.
The sim workload's `tokio::time::timeout(30s, …)` wrapper does not rescue it: `tokio::time` is not driven by moonpool's virtual clock, so the timer never fires, and the hung `pending()` connect schedules no event for virtual time to advance to.
The simulation goes quiescent → orchestrator no-progress → deadlock.

This is therefore a real **initial-connect resilience gap** in magnetar, surfaced (not caused) by moonpool's by-design fault injection.
In production it is latent: a real OS `connect()` eventually errors rather than hanging indefinitely, but a broker that accepts the SYN and then stalls would still block the caller until the OS connect timeout (minutes), and a transient connect failure aborts the client build outright.
The Java client guards this with `connectionTimeoutMs` (default 10 s) plus connection retry within `operationTimeout`.

### Why a magnetar-only fix is not perturbation-safe (the whack-a-mole)

The first instinct — "give magnetar a connect timeout + retry and the sim goes green" — does **not** hold under moonpool's deterministic schedule.
Any timer magnetar schedules to bound or re-drive a connect is itself an event on the virtual clock.
Adding it shifts every subsequent event's ordering, which moves where moonpool's `buggify!()` connect-fault location next fires.
The deadlock does not disappear; it relocates to a different seed.

We watched this happen twice:

1. Wrapping the dial in a **chokepoint** connect-timeout cleared the originally-failing seeds (`0x26b52c2c3cc5bf73` and the `*_sweep_16_seeds` set) but introduced a fresh hang on a previously-green seed, because the chokepoint's `sleep` rescheduled the storm.
2. Adding the **`operation_timeout`** total-budget cap on top cleared _that_ seed and again re-triggered a hang elsewhere.

Each magnetar-side timer is a schedule perturbation, and the busy-peer keepalive/connect storm is self-perpetuating: as long as the workload keeps re-dialling under fault injection, _some_ seed will line the storm up against the orchestrator's quiescence window.
There is no magnetar-only configuration of timeouts and retries that is green across the full seed space, because the very mechanism that bounds the hang is what moves it.
The fix has to live where it cannot perturb the schedule: in the orchestrator's own progress accounting.

(Distinct from the still-open "supervised consumer replay drops flow permits" follow-up referenced by `e2e_cluster_failover.rs` — that is a flow-permit re-grant bug on _reconnect_, not an _initial_-connect concern, and is unaffected by this ADR.)

## Decision

The decision splits along the line the whack-a-mole exposed: **the sim is made green in moonpool, not in magnetar**, and magnetar carries an independent **production** resilience layer that no longer pretends to be the sim fix.

### Sim-green: the moonpool orchestrator `run_time_budget` detector (no magnetar timers)

The deterministic-simulation deadlock is fixed in moonpool, in a separate PR (branch `fix/no-progress-detector-busy-peer`).
The orchestrator gains a **run-phase virtual-time budget** (`SimulationBuilder::run_time_budget`): a deterministic ceiling on how much _virtual_ time the run phase may consume.
A busy-peer connect/keepalive storm that re-arms itself forever now trips this budget and the orchestrator terminates the run as a **deterministic DEADLOCK** — the same bit-for-bit outcome on every seed, every replay.
Crucially this needs **no magnetar timer**: the detector reads the orchestrator's own virtual clock, so it adds no event to the schedule and cannot perturb where the connect-fault fires.
This is what returns the `moonpool-seed-sweep` to green; the magnetar resilience below is _not_ load-bearing for sim termination.

The magnetar `sim_chaos` and `connect_resilience` tests set a **tight `run_time_budget`** (`30 s` virtual) so a storming seed terminates fast instead of pegging a core, and assert the DEADLOCK/timeout outcome deterministically rather than hanging.

### Production resilience (Java parity, both engines)

Add two fields to the sans-io `ConnectionConfig` (the policy lives in `magnetar-proto`; the engines execute it).
These exist for **production correctness and Java-client parity**, independent of the sim fix above:

- `connect_timeout: Duration` — per-attempt budget for the initial TCP/TLS dial. Default `10 s`, matching Java's `connectionTimeoutMs` exactly.
- `connect_max_retries: u32` — bounded re-dials when an attempt times out or fails with a transient I/O error. Default `8`. `0` means a single attempt.

The total connect-operation budget is the pre-existing `operation_timeout: Duration` (default `30 s`, Java's `operationTimeoutMs`); the retry loop reads it without adding a new field.

Both runtime engines wrap **only the initial dial** (`MoonpoolEngine::connect_plain*` / `Client::connect_with_resolver_and_provider`) plus the multi-broker pool dial (`pool::get_or_open` / `build_entry`) in a `dial_with_retry` helper that:

1. bounds each attempt with `connect_timeout`, driven by the engine's time source — the moonpool engine routes every dial through the `Transport::connect` chokepoint using `moonpool_core::TimeProvider::sleep` in a `select!` so the budget fires under virtual time (ADR-0011 clock injection); the tokio engine applies the same chokepoint inside `Transport::connect_with_resolver` via `tokio::time::timeout`;
2. enforces a **dual cap** — the retry loop ends as soon as **either** the count cap (`connect_max_retries`) **or** the total-budget cap (`operation_timeout`, measured from the first attempt) is reached, whichever trips first. `operation_timeout` bounds the **operation** (the caller-visible connect, including the pool-dial wait), not the lifetime of any one connection. This mirrors Java, where connection retry is bounded by both the attempt count and the surrounding `operationTimeout`. The elapsed check is a `now_instant()` **comparison** (`TimeProvider::now()` delta in the moonpool engine, `Instant::elapsed()` in tokio), **never a newly-scheduled timer**: a fresh moonpool `sleep`/`timeout` would add an event to the deterministic virtual schedule and re-trigger the busy-peer connect-hang on some seed (the whack-a-mole above). Every deadline in the moonpool engine path is a comparison against the injected clock, not a new timer;
3. retries **only transient failures** — a per-attempt timeout (surfaced as `Io(TimedOut)`) or an `EngineError::Io` / `ClientError::Io`. A permanent error (bad TLS server name/cert → `Config`/`Tls`, or a protocol/peer-closed outcome) is surfaced immediately, so misconfiguration still fails fast;
4. backs off between attempts with a deterministic exponential schedule (50 ms doubling, capped at 1 s — no jitter, so moonpool replays bit-for-bit).

The multi-broker pool additionally bounds the _pool-dial wait_ (the park until the dialled connection finishes its handshake) with `operation_timeout` so a connection that storms on a connect-hang surfaces as a timeout ERROR to the caller — the operation terminates instead of parking forever, while a merely-flaky connection keeps reconnecting under the supervisor. The moonpool pool parks on `TimeProvider::sleep(operation_timeout)`; the tokio pool wraps the handshake wait in `tokio::time::timeout(operation_timeout, …)`.

The Pulsar handshake that follows a successful dial is **not** retried here; surviving mid-stream transport drops remains the supervisor's job (`ConnectionConfig::supervisor`, the reconnect path).

`connect_max_retries = 8` is a production default sized for transient-failure recovery without unbounded re-dialling: nine attempts under an independent ~75 %-success draw drive the residual to `0.25^9 ≈ 4·10⁻⁶` per connect. It is **not** the lever that greens the seed sweep — the moonpool `run_time_budget` detector is — but it gives production clients the same multi-attempt headroom the Java client has.

## Consequences

- The deadlocking seeds now pass **because of the moonpool `run_time_budget` detector**, not the magnetar retry: a self-perpetuating busy-peer storm trips the orchestrator's run-phase virtual-time budget and terminates deterministically as a DEADLOCK on every seed. The `moonpool-seed-sweep` returns to green without any magnetar-side timer. (This depends on the moonpool pin including branch `fix/no-progress-detector-busy-peer`; the local `[patch]` carrying it is removed when the moonpool change is released.)
- Production clients gain bounded initial-connect resilience matching the Java client: a stalled or transiently-refused initial dial is retried instead of hanging or aborting the build. A genuinely-unreachable broker still fails, after `connect_max_retries` bounded attempts. This is the production payoff and is **orthogonal** to sim termination.
- The moonpool engine path uses `now_instant()` clock **comparisons** for every deadline (per-attempt budget, dual-cap elapsed, pool-dial wait), **never a newly-scheduled `tokio::time` timer and never a fresh `sleep`/`timeout` that adds a schedule event** — a new timer perturbs the deterministic order and relocates the connect-hang to another seed (the whack-a-mole). Determinism is the load-bearing constraint on this engine path.
- Default behaviour changes for all callers (retry is on by default). Callers that want fail-fast set `connect_max_retries = 0`. Permanent errors are unaffected (surfaced immediately).
- The dual cap keeps a pathological broker from stretching the initial connect past `operation_timeout`: even if every attempt is a transient `Io` failure inside the count budget, the loop stops once the total elapsed time reaches `operation_timeout`. The count and the budget are independent backstops; the tighter one wins per call.
- Engine parity holds: both engines consume the same two `ConnectionConfig` fields and share the backoff schedule, keeping the tokio ↔ moonpool 1:1 behaviour ADR-0024 requires.

## Test coverage (ADR-0024)

- **`magnetar-proto`**: `conn_types.rs::connect_resilience_config_tests` — asserts the `ConnectionConfig` defaults (`connect_timeout = 10 s`, `connect_max_retries = 8`, `operation_timeout = 30 s`), that the dual-cap fields round-trip through `Clone`, and that `connect_max_retries = 0` is a valid fail-fast config.
- **`magnetar-runtime-moonpool`**: `tests/connect_resilience.rs` — `moonpool_connect_hang_is_bounded_smoke` + `moonpool_connect_hang_is_bounded_sweep_16_seeds` keep the default `ConnectFailureMode::Probabilistic` connect-fault active and assert a connect-hang on the supervised / pool dial is recovered, surfaces a bounded `operation_timeout` error within the dual cap, or terminates as a deterministic DEADLOCK once the tight `run_time_budget` trips — never a silent park. Both that suite and the `sim_chaos_*` suite set a tight `run_time_budget` (`30 s` virtual, the `RUN_TIME_BUDGET` / `CHAOS_RUN_TIME_BUDGET` constants) so a storming seed terminates fast instead of pegging a core; the previously-deadlocking seeds now terminate deterministically.
- **`magnetar-runtime-tokio`**: `tests/connect_resilience.rs` — `tokio_connect_retries_until_broker_listens` (a connect against an initially-closed port retries and resolves once the broker binds) + `tokio_connect_zero_retries_fails_fast` (`connect_max_retries = 0` fails fast with a bounded `Io` error). Mirrors the moonpool shape, keeping the tokio ↔ moonpool 1:1 test count.
- **`magnetar-differential`**: `tests/connect_resilience_equivalence.rs::fault_free_connect_event_stream_parity` — a fault-free connect → producer-open → close leaves the tokio and moonpool `EventStream`s byte-identical (the dual-cap retry is a transparent pass-through on the happy path).
- **e2e**: `crates/magnetar/tests/e2e_connect_resilience.rs::e2e_client_retries_until_broker_reachable` — a client started before its gate (a delayed loopback proxy to the real broker) is reachable rides out the `ConnectionRefused` window under the default dual cap, connects, and completes a produce/consume round-trip.
