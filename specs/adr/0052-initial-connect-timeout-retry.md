# ADR-0052 — Initial-connect timeout + bounded retry

- **Status**: Accepted (amended 2026-06-12 — post-dial handshake bound)
- **Date**: 2026-06-03
- **Decider**: Florentin Dubois
- **Tags**: runtime, connection, resilience, moonpool, java-parity, determinism

## Context

The daily `moonpool-seed-sweep` had been red for four consecutive runs, and several seeds (e.g. `0x26b52c2c3cc5bf73`) deadlocked `sim_chaos_produce_consume_with_invariants` and the `*_sweep_16_seeds` tests deterministically.
The orchestrator reported a no-progress / DEADLOCK early-exit (`moonpool-sim/src/runner/orchestrator.rs`), and the failing `SimulationReport` carried the all-default metrics (`events_processed: 0`, empty `individual_metrics`) that the failure path leaves behind.

The seeds are **not** linked to any recent magnetar change.
`git diff --stat 4cda3e6..HEAD` shows the entire `magnetar-runtime-moonpool` crate **and** `Cargo.lock` are byte-identical across the range, so no commit in it — including the ADR-0051 partition-metadata precheck (`ca4ad17`), which touches only the `magnetar` façade — can affect the sim.
The failure reproduces on `HEAD` while the default seed passes.

Root cause, traced with broker- and client-side instrumentation: the client workload's `run()` starts, calls `Client::connect_plain`, and **hangs inside the moonpool `NetworkProvider::connect`** — the broker's `listener.accept()` never returns and no `CommandConnect` is ever sent. moonpool's sim network defaults to `ConnectFailureMode::Probabilistic` (`moonpool-sim/src/network/config.rs`, a FoundationDB `SIM_CONNECT_ERROR_MODE = 2` port).
When the connect `buggify!()` location is active for a run, each `connect()` either fails fast or **hangs forever** (`std::future::pending()`), by design, _"to test timeout handling in connection retry logic."_

magnetar's initial dial (`Transport::connect` → `NetworkProvider::connect`, a bare `.await`) had **no connect timeout and no retry**. The sim workload's `tokio::time::timeout(30s, …)` wrapper does not rescue it: `tokio::time` is not driven by moonpool's virtual clock, so the timer never fires, and the hung `pending()` connect schedules no event for virtual time to advance to.
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
The orchestrator gains a **simulation-run virtual-time budget** (`SimulationBuilder::run_time_budget`): a deterministic ceiling on how much _virtual_ time the simulation run may consume.
A busy-peer connect/keepalive storm that re-arms itself forever now trips this budget and the orchestrator terminates the run as a **deterministic DEADLOCK** — the same bit-for-bit outcome on every seed, every replay.
Crucially this needs **no magnetar timer**: the detector reads the orchestrator's own virtual clock, so it adds no event to the schedule and cannot perturb where the connect-fault fires.
This is what returns the `moonpool-seed-sweep` to green; the magnetar resilience below is _not_ load-bearing for sim termination.

The magnetar `sim_chaos` and `connect_resilience` tests set a **tight `run_time_budget`** (`30 s` virtual) so a storming seed terminates fast instead of pegging a core, and assert the DEADLOCK/timeout outcome deterministically rather than hanging.

### Production resilience (Java parity, both engines)

Add two fields to the sans-io `ConnectionConfig` (the policy lives in `magnetar-proto`; the engines execute it).
These exist for **production correctness and Java-client parity**, independent of the sim fix above:

- `connect_timeout: Duration` — per-attempt budget for the initial TCP/TLS dial. Default `10 s`, matching Java's `connectionTimeoutMs` exactly.
- `connect_max_retries: u32` — bounded re-dials when an attempt times out or fails with a transient I/O error.
  Default `8`.
  `0` means a single attempt.

The total connect-operation budget is the pre-existing `operation_timeout: Duration` (default `30 s`, Java's `operationTimeoutMs`); the retry loop reads it without adding a new field.

Both runtime engines wrap **only the initial dial** (`MoonpoolEngine::connect_plain*` / `Client::connect_with_resolver_and_provider`) plus the multi-broker pool dial (`pool::get_or_open` / `build_entry`) in a `dial_with_retry` helper that:

1. bounds each attempt with `connect_timeout`, driven by the engine's time source — the moonpool engine routes every dial through the `Transport::connect` chokepoint using `moonpool_core::TimeProvider::sleep` in a `select!` so the budget fires under virtual time (ADR-0011 clock injection); the tokio engine applies the same chokepoint inside `Transport::connect_with_resolver` via `tokio::time::timeout`;
2. enforces a **dual cap** — the retry loop ends as soon as **either** the count cap (`connect_max_retries`) **or** the total-budget cap (`operation_timeout`, measured from the first attempt) is reached, whichever trips first.
   `operation_timeout` bounds the **operation** (the caller-visible connect, including the pool-dial wait), not the lifetime of any one connection.
   This mirrors Java, where connection retry is bounded by both the attempt count and the surrounding `operationTimeout`.
   The elapsed check is a `now_instant()` **comparison** (`TimeProvider::now()` delta in the moonpool engine, `Instant::elapsed()` in tokio), **never a newly-scheduled timer**: a fresh moonpool `sleep`/`timeout` would add an event to the deterministic virtual schedule and re-trigger the busy-peer connect-hang on some seed (the whack-a-mole above).
   Every deadline in the moonpool engine path is a comparison against the injected clock, not a new timer;
3. retries **only transient failures** — a per-attempt timeout (surfaced as `Io(TimedOut)`) or an `EngineError::Io` / `ClientError::Io`.
   A permanent error (bad TLS server name/cert → `Config`/`Tls`, or a protocol/peer-closed outcome) is surfaced immediately, so misconfiguration still fails fast;
4. backs off between attempts with a deterministic exponential schedule (50 ms doubling, capped at 1 s — no jitter, so moonpool replays bit-for-bit).

The multi-broker pool additionally bounds the _pool-dial wait_ (the park until the dialled connection finishes its handshake) with `operation_timeout` so a connection that storms on a connect-hang surfaces as a timeout ERROR to the caller — the operation terminates instead of parking forever, while a merely-flaky connection keeps reconnecting under the supervisor.
The moonpool pool parks on `TimeProvider::sleep(operation_timeout)`; the tokio pool wraps the handshake wait in `tokio::time::timeout(operation_timeout, …)`.

The Pulsar handshake that follows a successful dial is **not** retried here; surviving mid-stream transport drops remains the supervisor's job (`ConnectionConfig::supervisor`, the reconnect path).

### Amendment (2026-06-12): the post-dial handshake is bounded too

The dual cap above scopes to the **dial** — the TCP/TLS establishment.
A separate gap remained downstream of a successful dial: once `dial_with_retry` returns `Ok`, the engines drive the `CONNECT` → `CONNECTED` handshake (moonpool `handshake_plain`, tokio `wait_connected`) in an **unbounded** read loop.
A broker that accepts the SYN but never replies to `CommandConnect` — moonpool-sim's connect-hang fault landing _after_ accept rather than during it, or a wedged real broker — parked that loop forever.
The bootstrap tokio path (`Client::start_handshake` / `start_supervised_handshake`) and every moonpool `handshake_plain` caller had no timeout; only the multi-broker **pool** path already bounded its handshake wait (the `operation_timeout` deadline added for the pool-dial wait above).
GitHub #177 reproduced this on moonpool seed `0x269b4b0a1c962f41`: the dial succeeded, the silent broker never answered, and the run went quiescent until the `run_time_budget` detector tripped a DEADLOCK.

The fix extends the **same `operation_timeout` total budget** to the post-dial handshake (Java `operationTimeoutMs` parity — `operationTimeoutMs` bounds the whole connect operation, not just the socket open):

- **moonpool** (`handshake_plain`): `handshake_plain` takes a `bound: Option<Duration>`.
  When `Some(operation_timeout)`, it arms **exactly one** `TimeProvider::sleep(operation_timeout)` deadline _before_ the read loop, `Box::pin`s it, and polls it via `select! { biased; r = read => …, _ = deadline => Err(Io(TimedOut)) }` across every iteration.
  This is the single-deadline shape the pool's `await_ready` already uses.
  A fresh `sleep` armed **per read-loop iteration** would schedule a `Timer` event on every green-seed handshake and perturb the deterministic schedule — the exact whack-a-mole footgun this ADR warns about — so the single armed deadline is load-bearing for determinism, not just tidiness.
- **moonpool determinism: `None` on the pool path.** The **direct** dial paths (`connect_plain` / `connect_plain_with_resolver` / `connect_tls` / `connect_plain_supervised`) pass `Some(operation_timeout)` — there `handshake_plain` is the _only_ bound on the read loop.
  The **pool** path (`build_entry_async`) passes **`None`**: its waiter is already bounded by `await_ready`'s `time.sleep(operation_timeout)` deadline, so a silent broker already surfaces a bounded timeout to the caller.
  Arming a _second_ `TimeProvider::sleep` inside the pool's `handshake_plain` adds a redundant timer event to the deterministic schedule on every pooled dial, and that extra event reorders the schedule enough to intermittently break the pinned-pool lifecycle seed under load — a fresh instance of the very whack-a-mole this ADR documents.
  Passing `None` keeps the pool path at exactly one handshake timer (the `await_ready` one), which the 16-seed sweep confirms is determinism-stable.
- **tokio** (`bounded_wait_connected`): wrap `wait_connected` in `tokio::time::timeout(operation_timeout, …)`, mirroring what the tokio pool path already does.
  (The tokio engine is on wall time, not a deterministic virtual schedule, so the redundant-timer concern is moonpool-specific; the tokio pool path keeps its own `tokio::time::timeout`, and the two bootstrap call sites gain one.)

Both engines surface a bounded `Io(TimedOut)` when the budget is spent.
The handshake bound is a **transparent pass-through on the happy path** (the broker answers CONNECT before the deadline, which is then dropped un-fired), so it does not change observable behaviour for a healthy broker — the differential equivalence test pins that.
Like the dial cap, the moonpool direct path never arms a per-iteration timer; the one deadline is a single scheduled event, identical across replays of a given seed, and the pool path arms no new timer at all.

`connect_max_retries = 8` is a production default sized for transient-failure recovery without unbounded re-dialling: nine attempts under an independent ~75 %-success draw drive the residual to `0.25^9 ≈ 4·10⁻⁶` per connect.
It is **not** the lever that greens the seed sweep — the moonpool `run_time_budget` detector is — but it gives production clients the same multi-attempt headroom the Java client has.

## Consequences

- The deadlocking seeds now pass **because of the moonpool `run_time_budget` detector**, not the magnetar retry: a self-perpetuating busy-peer storm trips the orchestrator's simulation-run virtual-time budget and terminates deterministically as a DEADLOCK on every seed.
  The `moonpool-seed-sweep` returns to green without any magnetar-side timer.
  The temporary local `[patch]` that carried this moonpool fix was removed when moonpool `0.7.0` was adopted from crates.io ([ADR-0056](0056-moonpool-0-7-crates-io-repin.md)).
- Production clients gain bounded initial-connect resilience matching the Java client: a stalled or transiently-refused initial dial is retried instead of hanging or aborting the build.
  A genuinely-unreachable broker still fails, after `connect_max_retries` bounded attempts.
  This is the production payoff and is **orthogonal** to sim termination.
- The moonpool engine path uses `now_instant()` clock **comparisons** for every deadline (per-attempt budget, dual-cap elapsed, pool-dial wait), **never a newly-scheduled `tokio::time` timer and never a fresh `sleep`/`timeout` that adds a schedule event** — a new timer perturbs the deterministic order and relocates the connect-hang to another seed (the whack-a-mole).
  Determinism is the load-bearing constraint on this engine path.
- Default behaviour changes for all callers (retry is on by default).
  Callers that want fail-fast set `connect_max_retries = 0`.
  Permanent errors are unaffected (surfaced immediately).
- The dual cap keeps a pathological broker from stretching the initial connect past `operation_timeout`: even if every attempt is a transient `Io` failure inside the count budget, the loop stops once the total elapsed time reaches `operation_timeout`.
  The count and the budget are independent backstops; the tighter one wins per call.
- Engine parity holds: both engines consume the same two `ConnectionConfig` fields and share the backoff schedule, keeping the tokio ↔ moonpool 1:1 behaviour ADR-0024 requires.

## Test coverage (ADR-0024)

- **`magnetar-proto`**: `conn_types.rs::connect_resilience_config_tests` — asserts the `ConnectionConfig` defaults (`connect_timeout = 10 s`, `connect_max_retries = 8`, `operation_timeout = 30 s`), that the dual-cap fields round-trip through `Clone`, and that `connect_max_retries = 0` is a valid fail-fast config.
  The 2026-06-12 amendment adds `operation_timeout_is_the_post_dial_handshake_budget`, pinning that `operation_timeout` is the single total budget the engines arm over the post-dial handshake and that it stays independent of the two dial caps.
- **`magnetar-runtime-moonpool`**: `tests/connect_resilience.rs` — `moonpool_connect_hang_is_bounded_smoke` + `moonpool_connect_hang_is_bounded_sweep_16_seeds` keep the default `ConnectFailureMode::Probabilistic` connect-fault active and assert a connect-hang on the supervised / pool dial is recovered, surfaces a bounded `operation_timeout` error within the dual cap, or terminates as a deterministic DEADLOCK once the tight `run_time_budget` trips — never a silent park.
  Both that suite and the `sim_chaos_*` suite set a tight `run_time_budget` (`30 s` virtual, the `RUN_TIME_BUDGET` / `CHAOS_RUN_TIME_BUDGET` constants) so a storming seed terminates fast instead of pegging a core; the previously-deadlocking seeds now terminate deterministically.
  The 2026-06-12 amendment adds `moonpool_silent_broker_handshake_is_bounded` + `moonpool_silent_broker_error_is_timed_out_io`: a broker that accepts the TCP connection but never replies to `CommandConnect` makes the dial succeed yet the handshake stall, and the client must surface a bounded `Io(TimedOut)` (the seed-`0x269b4b0a1c962f41` / #177 gap), not park.
- **`magnetar-runtime-tokio`**: `tests/connect_resilience.rs` — `tokio_connect_retries_until_broker_listens` (a connect against an initially-closed port retries and resolves once the broker binds) + `tokio_connect_zero_retries_fails_fast` (`connect_max_retries = 0` fails fast with a bounded `Io` error).
  The 2026-06-12 amendment adds `tokio_silent_broker_handshake_times_out` + `tokio_silent_broker_error_is_timed_out_io`, the silent-broker mirror of the moonpool pair.
  Mirrors the moonpool shape, keeping the tokio ↔ moonpool 1:1 test count (four each).
- **`magnetar-differential`**: `tests/connect_resilience_equivalence.rs::fault_free_connect_event_stream_parity` — a fault-free connect → producer-open → close leaves the tokio and moonpool `EventStream`s byte-identical (the dual-cap retry **and** the post-dial handshake bound are transparent pass-throughs on the happy path: the handshake deadline is armed but never fires).
- **e2e**: `crates/magnetar/tests/e2e_connect_resilience.rs::e2e_client_retries_until_broker_reachable` — a client started before its gate (a delayed loopback proxy to the real broker) is reachable rides out the `ConnectionRefused` window under the default dual cap, connects, and completes a produce/consume round-trip.
