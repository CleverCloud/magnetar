# Simulation Patterns: FoundationDB, moonpool, TigerBeetle

> **Audience.** Engineers evaluating where magnetar's deterministic
> simulation infrastructure should evolve next. This is a research
> note, not a binding spec — for binding decisions see
> [`../specs/adr/`](../specs/adr/).

Magnetar already runs deterministic simulation via
[`magnetar-runtime-moonpool`](../crates/magnetar-runtime-moonpool/)
backed by `moonpool-core`'s `Providers` bundle. This note compares
that with two reference systems — Apple FoundationDB's simulator and
TigerBeetle's VOPR — to identify which patterns are worth adopting
next.

---

## 1. FoundationDB simulator (the reference implementation)

The FoundationDB simulator is the canonical example of "the test
strategy that made it possible to ship a production distributed
database with a small team." Source:
[apple.github.io/foundationdb/testing.html](https://apple.github.io/foundationdb/testing.html).

### Determinism architecture

- **Single-threaded Flow execution.** FoundationDB is written in
  *Flow*, an actor-based language atop C++. The simulator runs the
  full cluster (all servers + all clients) in a single OS thread.
  No threading primitives, no preemption — every interleaving is a
  deterministic function of the seed.
- **Synchronized time stepping.** The simulator advances a virtual
  clock and dispatches actor wake-ups in deterministic order. Real
  durations are compressed (~10×) so a "one-day" outage in
  simulation completes in a few minutes of wall time.
- **Production code IS the test target.** Flow is the same language
  used in production binaries. There is no separate "mock" — the
  simulator replaces the I/O / time / random primitives only.

### Fault injection — "buggify"

- **Buggify points** are explicit `if (BUGGIFY) { ... }` blocks
  spread throughout the production code: rare delays, dropped
  messages, partial writes, restarts. Under simulation each
  buggify-block fires with controlled probability per seed; in
  production they never fire.
- **Multi-layer faults**: network (packet loss, reorder, partition,
  delay), machine (process crash, reboot, slow disk, full disk),
  datacenter (full-DC partition, asymmetric routing). Each layer is
  modelled independently and composes.
- **Swizzle-clogging**: stop random subsets of nodes' network
  traffic, then restart them in a different random order. Exposes
  reconnection-ordering bugs that pure crash-restart misses.

### Volume + workloads

- "Tens of thousands of simulations every night." A new commit is
  expected to soak through that swarm before reaching production.
- **Workload reuse**: the same workload definitions drive
  performance tests (real cluster, real time) and simulation
  (virtual cluster, virtual time). One spec, two regimes.

### Takeaway for magnetar

What magnetar already shares: single-threaded sim execution,
virtual clock, network simulation, seeded RNG. What we don't have
(yet) is: **buggify-style fault-injection points scattered through
production code**. The
[`magnetar-runtime-moonpool/tests/sim_chaos.rs`](../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs)
suite drives faults from the *outside* (the chaos broker workload);
FoundationDB would also pepper the inside (e.g. random producer
flush delays inside `magnetar-proto`, random TLS read short-reads
inside the byte-pipe).

---

## 2. moonpool — where we are today

`moonpool-core` (vendored at `=0.6`) exposes a
[`Providers`](https://crates.io/crates/moonpool-core) trait bundle:

| Provider | Production (`TokioProviders`) | Simulation (`SimProviders`) |
| --- | --- | --- |
| `TimeProvider` | wall clock | virtual clock advanced by the simulator |
| `NetworkProvider` | real TCP via `tokio::net` | in-process byte pipe with controlled drops / delays |
| `TaskProvider` | `tokio::task::spawn_local` | deterministic ready-queue |
| `RandomProvider` | OS RNG | `ChaCha8Rng` seeded by `MOONPOOL_SEED` |
| `StorageProvider` | host filesystem | in-memory virtual filesystem |

`magnetar-runtime-moonpool` is generic over the `Providers` bundle.
The exact same engine code runs both regimes; the only difference
is which providers get plugged in. This already matches
FoundationDB's "production code IS the test target" principle.

### What's wired today

- **Deterministic chaos pack** at
  [`crates/magnetar-runtime-moonpool/tests/`](../crates/magnetar-runtime-moonpool/tests/):
  mid-handshake partitions, frame reordering, OAuth token-refresh
  edge cases, PIP-121 oscillation, PIP-188 migrate-then-migrate,
  in-flight publish replay on reconnect, virtual-clock ack/send
  timeouts, ADR-0028 anti-thrash policy.
- **Stateful chaos broker** in
  [`tests/sim_chaos.rs`](../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs)
  with `BrokerWorkload` + `ClientWorkload` + invariants
  (at-least-once delivery, monotonic message-id,
  no-dup-on-acked, supervisor-recovers-within-N-ticks).
- **Differential equivalence harness** in
  [`magnetar-differential`](../crates/magnetar-differential/): the
  same trace (`Op` sequence) is replayed against both engines and
  the `EventStream`s are byte-compared.
- **Seed sweep**: per
  [ADR-0036](../specs/adr/0036-moonpool-seed-sweep-daily-random.md)
  a daily 16-random-seed CI job covers the moonpool suite; the
  per-PR matrix was retired because each `(commit, seed)` pair is
  bit-for-bit reproducible.

### What's missing vs. FoundationDB

1. **No buggify points in `magnetar-proto`.** All fault injection
   today comes from the chaos broker. Real FoundationDB sprinkles
   `if (BUGGIFY)` everywhere — even inside the protocol state
   machine. Equivalent for magnetar would be feature-flagged
   `#[cfg(feature = "buggify")]` blocks at known choice points
   (e.g. inside `Connection::reset()`, inside
   `BatchContainer::flush`).
2. **No swizzle-clogging.** The chaos broker can drop a connection,
   but stopping N random consumers' permits in order and then
   restoring them in a different random order is not yet
   expressible.
3. **No long-running soak.** "Tens of thousands of nightly sims" is
   beyond a 16-seed daily job. A longer-running soak (e.g. nightly
   1 000-seed sweep on a beefier runner) would close this gap.

---

## 3. TigerBeetle — the assertion-first philosophy

[TigerStyle](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md)
is the explicit set of coding rules that make deterministic
simulation actually work on TigerBeetle's codebase. It is *not*
just about the simulator — it's about how production code is
written so that simulation discovers bugs cheaply.

### Coding rules that make simulation effective

- **Assertion density ≥ 2 per function.** Pre/postconditions,
  invariants, compile-time relationships. Assertions downgrade
  silent correctness bugs into loud liveness bugs (crashes), which
  the simulator catches immediately.
- **Pair assertions (positive + negative space).** Don't just
  assert what you expect — also assert what you don't. "Data
  movement across trust boundaries" gets both sides asserted.
- **Run-to-completion functions.** Functions that don't suspend
  preserve their preconditions throughout the body — no need to
  re-assert after every await point. Maps directly to magnetar's
  sans-io `Connection` entries: `handle_bytes(now, &[u8])` runs to
  completion under the caller's lock.
- **Static memory only on hot paths.** No heap allocations after
  startup — preallocate all buffers. Removes one entire class of
  failure mode from the simulator's surface area.
- **No shared mutable state between actors.** Each actor owns its
  state; message-passing for coordination. Magnetar already
  enforces this via [ADR-0003 no-channels](../specs/adr/0003-no-channels-rule.md)
  + Waker-slab pattern (the closest Rust analog).

### VOPR — the simulator

VOPR (Viewstamped Operations Replicator) is TigerBeetle's
simulator. Key properties:

- **VOPR is the final line of defence, not the first.** "Assertions
  are a safety net, not a substitute for human understanding."
  Engineers reason about correctness first; VOPR catches the
  residual.
- **Single-threaded simulation of a full replica set.** Same
  pattern as FoundationDB.
- **Deterministic state-machine fuzzing.** Random client workloads
  + random network faults + assertion density = bugs found in
  minutes that would take days of customer traffic.
- **VOPR runs continuously on dedicated hardware.** Higher
  throughput than nightly sweeps because the cost of one bug
  escaping to production is operationally catastrophic.

### Takeaway for magnetar

Two TigerBeetle patterns transfer cleanly to magnetar:

1. **Assertion density** — `magnetar-proto::Connection` has many
   `Option::expect` / `unwrap` paths in tests but few `debug_assert!`
   in production code. Doubling those would let `cargo xtask
   check-sim-coverage` discover invariant violations the
   differential harness doesn't catch.
2. **Pair assertions** — the in-flight publish snapshot
   accumulation (commit `0e47e14`) had a subtle bug: `reset()`
   used to *clear* snapshots, masking the user-queued send. A
   negative-space assertion like
   `debug_assert!(self.in_flight_publish_snapshots.is_empty() ||
   self.session_epoch == 0)` at the entry of `rebuild_producers`
   would have caught it earlier.

The `static memory` rule does NOT transfer — magnetar uses
`Vec<u8>` buffers for arbitrary-sized Pulsar payloads, and Rust's
allocator is fast enough that pre-allocation is not the lever it
is on TigerBeetle's small fixed-size messages.

---

## 4. Magnetar's next-up simulation moves

Mapped to the patterns above, the highest-leverage additions in
priority order:

| Priority | Pattern | Source | Magnetar surface |
| --- | --- | --- | --- |
| 1 | Buggify points in `magnetar-proto` | FDB | `#[cfg(feature = "buggify")]` blocks at: `Connection::reset()`, `BatchContainer::flush`, `Connection::handle_bytes` (short-read split), `RetryClock::next_delay` (clock skew) |
| 2 | Assertion density in `magnetar-proto` | TigerBeetle | `debug_assert!` pairs on every `Connection::record_*` entry; positive + negative space |
| 3 | Swizzle-clog workload in sim_chaos | FDB | New `BrokerWorkload::Swizzle { n_clogged, order }` variant — drops N random consumers' permits in order, restores them in a different order |
| 4 | Long-running soak (nightly 1 000 seeds) | FDB | New GH Actions workflow `moonpool-soak.yml` on a beefier runner; alerts on any seed failure |
| 5 | Per-handle invariant assertions in tests | TigerBeetle | Extend `sim_chaos.rs` invariants with per-(producer, consumer) handle assertions — every `OpSend` must resolve to exactly one of `Sent` / `SessionLost` / `MemoryLimitExceeded` |

None of these requires changes to the `Engine` trait or the
sans-io boundary. They're all additive enrichments of the
simulation surface, easily landed as separate ADRs + commits.

### Out of scope

- **Replacing moonpool with a different sim crate.** moonpool
  already gives us the FDB+TB primitives (single-threaded executor,
  seeded RNG, virtual clock, in-process network). Rewriting on top
  of e.g. `madsim` or `loom` would be a 6-12 month project for
  questionable incremental win.
- **VOPR-equivalent dedicated runner.** TigerBeetle runs VOPR on
  dedicated bare-metal because every seed costs hours; magnetar's
  current sim runs in ~50 ms per seed. The 16-random-seed daily
  job is the correct shape until a multi-minute-per-seed regression
  appears.

---

## References

### FoundationDB
- [apple.github.io/foundationdb/testing.html](https://apple.github.io/foundationdb/testing.html)
- Will Wilson, *Testing Distributed Systems w/ Deterministic Simulation*
  (Strange Loop 2014) — the canonical talk.
- Buggify pattern: `BUGGIFY()` macro in
  [`apple/foundationdb/fdbrpc`](https://github.com/apple/foundationdb).

### TigerBeetle
- [TigerStyle](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md)
- VOPR: `src/vopr.zig` in the
  [`tigerbeetle/tigerbeetle`](https://github.com/tigerbeetle/tigerbeetle)
  repo.
- TigerBeetle blog: *It Takes Two To Contract* (pair assertions),
  *Testing Made Easy By VOPR*.

### moonpool / magnetar
- [`docs/moonpool-engine.md`](moonpool-engine.md) — current
  surface.
- [`ADR-0019`](../specs/adr/0019-engine-scope-and-moonpool-parity.md)
  — engine scope.
- [`ADR-0024`](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md)
  — four-layer test policy + 1:1 runtime parity.
- [`ADR-0026 §D2`](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md#d2-vendor-moonpool-sim-into-the-workspace)
  — chaos pack scope.
- [`ADR-0036`](../specs/adr/0036-moonpool-seed-sweep-daily-random.md)
  — daily 16-random-seed CI policy.
- Commit `aaa0661` — stateful chaos broker + invariants.
- [`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`](../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs)
  — current chaos suite.
