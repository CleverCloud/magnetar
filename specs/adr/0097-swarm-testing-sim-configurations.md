# ADR-0097 — Swarm-test the moonpool simulation: per-seed feature subsets

- **Status**: Accepted
- **Date**: 2026-08-07
- **Decider**: Florentin Dubois
- **Tags**: simulation, testing, moonpool, buggify, fault-injection, swarm

## Context

Swarm testing (Groce et al., ISSTA 2012, <https://users.cs.utah.edu/~regehr/papers/swarm12.pdf>) shows that always enabling every generator feature suppresses bugs: a feature that repairs or advances state can mask defects that need its absence, and one universal mixture explores one region of the behaviour space forever.
The paper's remedy is to draw a random feature subset per test configuration, and its own data shows the subset campaign must not replace the inclusive one — a bug needing `k` features together appears in a coin-toss subset with probability `1/2^k`, and the paper's combined run (120 distinct bugs) beat swarm alone (104).

Today every moonpool simulation seed runs the single all-features configuration:

- The four ADR-0048 buggify labels are wired but **never armed inside a running simulation** — `grep -n buggify crates/magnetar-runtime-moonpool/tests/sim_chaos.rs` returns zero hits, and the only test that arms a real `Connection` (`tests/buggify_sim_sweep.rs:141`) drives `reset()` + `Backoff::next` directly, not the driver loop.
- The `sim_chaos.rs` workloads drive a fixed operation alphabet with fixed constants (`PRODUCE_COUNT` at `sim_chaos.rs:1566`, receive budget at `:1702`); no operation is ever omitted, and no weights exist.
- The workload alphabet contains documented suppressors: the broker send-dedup (`sim_chaos.rs:1029-1031`) explicitly skips the `SendEvent` on a replay so `MonotonicMsgIdInvariant` cannot see `prev == got` (`:1046-1052`); the client-side receive dedup (`:1701`, `:1713`) swallows genuine duplicates before any assertion sees them; constant acking keeps `NoDupOnAckedInvariant`'s redelivery trigger rare.

Three analysis passes over the labels, the operation alphabet, and the determinism contract established the constraints a swarm design must satisfy here:

1. **Purity.** ADR-0036/ADR-0047 make `(commit, MOONPOOL_SEED)` the complete reproduction key: the daily sweep's issue bodies, the `seed-replay` CI job, and local triage all replay from the seed alone.
   A configuration drawn from anything but the seed breaks every one of those consumers (unreproducible issues, per-PR replay verdicts that change on re-run), and the repo has already shipped that bug once — the unpinned-builder hazard documented at `tests/pool_lifecycle.rs:90-105` (issue #309).
2. **Stream isolation.** `tests/common/mod.rs:78` expands `MOONPOOL_SEED` into per-iteration sub-seeds via splitmix64; moonpool-sim seeds its in-run `SIM_RNG` from each iteration seed.
   Any config draw that consumes `SIM_RNG` shifts every later in-run draw and silently re-routes the trajectories of all twelve `seeds/known-failing.toml` anchors.
   moonpool-sim 0.8 exposes `current_sim_seed()` (crate root re-export), so a config can be a pure hash of the iteration seed and consume **zero** RNG stream.
3. **Anchor preservation.** Every `known-failing.toml` entry is a post-fix regression anchor; four are additionally pinned by dedicated regression tests that replay exact derived sub-seeds (`sim_chaos_produce_consume_sub_seed_366`, `sim_chaos_pulsar_proxy_bootstrap_sub_seeds_362_363_364_367`, the two `sim_delayed_marker_regression_*` tests).
   Those pinned tests exist to re-execute a recorded trajectory; a swarm subset must never apply to them.
4. **Repair features are not swarm features.** The 2026-07-27 registry batch (#362–#368) records seven false failures caused by workload setup paths that could not survive default-on chaos; the cure was the `retry_setup` / `retry_supervised_connect` helpers and the swizzle FLOW pump.
   Omitting those repair mechanisms recreates exactly that false-failure class, because the workloads' own success gates assume them.
5. **Invariant vacuity.** `AckAfterReceiveInvariant` is 100% vacuous when the client never acks; `NoDupOnAckedInvariant` is a no-op on any drop-free seed; `MonotonicMsgIdInvariant` asserts nothing until a second send lands on one producer.
   An omitted operation must therefore be _visible_ — a run whose config disables an operation must say which invariants that starves, and an enabled operation must leave evidence it actually fed its invariant.

## Decision

Introduce **swarm configurations** for the moonpool deterministic-simulation suite: each simulation iteration derives a `SwarmConfig` from its seed and runs a subset of the optional campaign features, while a reserved slice of seeds and every pinned regression test keep inclusive or recorded-baseline behaviour.

### What a feature is

A **feature** is an independently omittable element of a simulation campaign, in one of two classes:

- **Fault features** — the four ADR-0048 buggify labels (`connection.reset.delay`, `batch_container.flush.split`, `handle_bytes.short_read`, `retry_clock.skew`).
  All four are omit-safe: the not-fired branch is the production path, and no bootstrap depends on any of them.
- **Operation features** — optional operations of the `ProducerConsumerWorkload` in `sim_chaos.rs`: `op.client_ack` (the ack step at `sim_chaos.rs:1718`) and `op.client_close` (an explicit producer + consumer close after the workload's gates, an operation the workload never performed before).

Everything else is **mandatory** and always retained: the bootstrap chain (connect, lookup, producer open, subscribe), send, receive, FLOW accounting, MESSAGE dispatch, and — deliberately — the repair mechanisms (`retry_setup`, `retry_supervised_connect`, the swizzle FLOW pump, client receive dedup, the broker dispatch heartbeat).
Repair mechanisms are known suppressors, but constraint 4 above makes them false-failure generators when omitted; swarming them requires per-feature success-gate redesign and is explicitly future work, not part of this decision.

### The draw

- `crates/magnetar-runtime-moonpool/src/swarm.rs` provides `SwarmConfig` with `from_seed(seed: u64)`, `full()`, and `baseline()` constructors plus a `Display` impl that renders the canonical one-line description.
- `from_seed` is a **pure splitmix64 hash chain** over `(seed XOR per-purpose salt)` — it reads no environment, consumes neither moonpool's `SIM_RNG` nor its `CONFIG_RNG`, and draws nothing from `Providers::Random`, so the in-run randomness of every existing seed is untouched by construction.
- The client workload draws the config in `Workload::setup` from `moonpool_sim::current_sim_seed()` — moonpool guarantees every `setup` completes before any `run` starts, so the draw happens before the first operation.
- **Slice rule**: a first hash decides the campaign slot — 1 in 4 seeds runs `SwarmConfig::full()` (every feature enabled), the rest draw each feature independently at **50% inclusion**, the paper's default coin-toss.
  A draw that comes up all-off collapses to `full()` so the subset is never empty.
  25% inclusive is deliberate: the inclusive configuration owned 100% of all historical seed coverage, so exploration is where fresh volume pays, while the paper's own data forbids dropping inclusive runs from the campaign.
- `SwarmConfig::baseline()` reproduces the pre-swarm workload shape — ack enabled, close absent, buggify unarmed — and is what the sub-seed-pinning regression tests force, so their recorded trajectories stay byte-identical.

### Per-label buggify arming (amends ADR-0048)

- `magnetar-proto`'s `Buggify` gains an optional **label filter**: `Buggify::with_rng_and_filter(rng, filter)` with `filter: Arc<dyn Fn(&'static str) -> bool + Send + Sync>`.
  A filtered-out label returns `false` **before** consuming the RNG, exactly like the existing `p <= 0.0` short-circuit; `with_rng` keeps its all-labels behaviour.
  This is the `BuggifyConfig`-shaped extension ADR-0048 §Alternatives deferred "until a real test needs it"; the swarm campaign is that test.
  The proto crate still draws no randomness of its own and gains no dependency — the filter mirrors the ADR-0011/ADR-0048 closure-injection idiom.
- `magnetar_proto::ConnectionConfig` gains a `buggify: Buggify` slot (default `Buggify::disabled()`); **only the moonpool engine reads it** — `ConnectionShared::with_auth_and_wall_clock_base` installs the slot via the existing `Connection::set_buggify`, and the moonpool driver shares the installed helper with its reconnect `Backoff` via `install_buggify` at the schedule's lazy-init.
  This is what lets an **engine-created** connection be armed: the sim workload builds one armed `Buggify` from its `SwarmConfig` (RNG from `Providers::Random`, filter from the enabled-label set) and places it in the `ConnectionConfig`; supervised re-dials reuse the config, so every reconnect is armed identically and the shared fire-counter map survives resets.
- The tokio engine **ignores the slot entirely** — its construction path never calls `set_buggify`, so even a config carrying an armed helper leaves a tokio connection disarmed (pinned by `swarm_off_is_nop.rs`).
  `Buggify::disabled()` stays the default everywhere, the four call-site probabilities stay `0.05`, and the `buggify_off_is_nop.rs` / `buggify_disabled_equivalence.rs` contracts do not move.

### Invariant honesty under omission

- The config line is printed **before the workload runs** and names the seed, the slot (`full` / `swarm` / `baseline`), every enabled label, every enabled operation, the effective weights, and the invariants the drawn config leaves vacuous (`op.client_ack` off ⇒ `AckAfterReceiveInvariant` + `NoDupOnAckedInvariant`).
- Every gate and invariant failure message embeds that same config line; a failing seed whose configuration is not printed is not reproducible, and ADR-0047 triage depends on it.
- **Non-vacuity evidence**: when `op.client_ack` is enabled and the run received messages, the workload's `check()` asserts the run actually issued at least one ack call — an enabled operation must prove it ran, so a gating bug cannot silently starve `AckAfterReceiveInvariant` while the config line claims otherwise.
  (Broker-side ack arrival is deliberately not the evidence: chaos can legitimately eat an ack frame, and a sweep must not false-fail on that.)
- **Effective weights**: with `op.client_ack` off, the durable cursor never advances and every reconnect redelivers the full ledger, so the receive-attempt budget scales from `2 × PRODUCE_COUNT` to `4 × PRODUCE_COUNT`; the scaled budget is part of the printed config.

### Campaign composition and replay contract

- Sweep and smoke tests that use `ProducerConsumerWorkload` draw per-iteration swarm configs; the pinned regression tests (`sim_chaos_produce_consume_sub_seed_366` and the seed-2 reproducer) force `baseline()`; every other workload (swizzle, anti-thrash, proxy, marker) is out of scope in this decision and keeps today's behaviour and trajectories bit-for-bit.
- The workload gains a per-iteration state reset in `Workload::setup` — required for the draw to mean anything: `SimulationBuilder::workload` reuses one instance across iterations, and without the reset every iteration ≥ 2 of a sweep saw iteration 1's received set, broke out of the receive loop immediately, and exercised no operation at all.
  Multi-iteration sweeps therefore now run real workloads on every iteration, which is itself a trajectory change the anchor re-validation below covers.
- `MOONPOOL_SEED=<s>` reproduces the same subset **and** the same run every time: config purity is pinned by a unit test asserting `SwarmConfig::from_seed` is stable across draws, and subset diversity by a test asserting the local `1..=32` sweep range yields at least 8 distinct enabled-feature sets.
- The daily sweep (`.github/workflows/moonpool-seed-sweep.yml`) and the per-PR `seed-replay` job (`.github/workflows/ci.yml`) are **unchanged**: both only set `MOONPOOL_SEED`, so 128 fresh daily seeds now also mean ~96 fresh swarm subsets plus ~32 inclusive runs, and the printed config line lands in the workflow log the discovery issue already links.
  The buggify Cargo feature stays off in both jobs' invocations; under a no-buggify build the drawn label set is printed but inert, and the fault-feature dimension is exercised by every `--all-features` run (the per-PR `test` job included).
- The registry contract of ADR-0047 is unchanged: if a swarm subset trips an invariant, that is the campaign working — the seed and its printed configuration are recorded in `seeds/known-failing.toml` and triaged; no assertion is weakened and no `#[ignore]` is added.
- Gating an operation changes the trajectories of the swarm-drawing tests under the twelve anchor seeds, so this changeset re-validates every `known-failing.toml` entry by replay before landing; the anchors whose workloads never draw a swarm config (`swizzle`, `proxy`, `marker` — ten of twelve) are unaffected by construction.

### `check-known-failing-seeds` lands (implements ADR-0047 §5)

ADR-0047 §5 specified a local `cargo xtask check-known-failing-seeds` mirroring the CI replay job; it was never implemented — the `Cmd` dispatch in `xtask/src/main.rs` has no such subcommand and CI is the sole enforcement point.
Because this decision leans on anchor replay as its safety net, the missing command lands in this changeset: it parses `seeds/known-failing.toml`, replays every `status = "open"` entry with the exact CI invocation (`MOONPOOL_SEED=<value> cargo test -p magnetar-runtime-moonpool --no-default-features --features crypto-aws-lc-rs --locked`), and exits non-zero on any failure.

## Consequences

**Positive**

- Each daily-sweep seed now explores a distinct point in configuration space as well as schedule space; suppressor-masked states (unacked flows, never-closed sessions, per-label fault subsets) become reachable.
- The four buggify labels finally fire inside a full driver-loop simulation instead of only in the isolated helper test.
- Reproduction stays a one-liner: the seed alone still determines everything, and the printed config makes the drawn subset part of the failure report.
- The inclusive configuration keeps a fixed share of every campaign, per the paper's own evidence.

**Negative**

- The `ProducerConsumerWorkload` trajectories change for every historical seed (operation gating), so the two produce-consume anchors are re-validated by replay rather than preserved bit-for-bit; their pinned regression tests carry the recorded trajectories forward.
- `ConnectionConfig` grows a field and `Buggify` a constructor — small, stable engine-facing surface, but surface nonetheless.
- A swarm subset can surface a latent workload-tolerance gap (the #362–#368 class) as well as a product bug; triage must classify which it is, per ADR-0047 §4.

**Neutral**

- Suppressor-class repair mechanisms stay always-on; swarming them is future work gated on per-feature success-gate redesign.
- Under builds without the `buggify` feature the label dimension is printed but inert; the operation dimension alone still varies the campaign.

## Alternatives considered

- **moonpool-sim's native `ChaosMode::Swarm`** (`SimulationBuilder::enable_chaos`).
  It swarms moonpool's _network/storage_ chaos families, not magnetar's buggify labels or workload operations, and flipping the chaos config from `None` to `Some` replaces `NetworkConfiguration::default()` with `random_for_seed()`, which consumes a block of `SIM_RNG` before workloads start — shifting every in-run draw of all twelve anchors at once.
  Rejected for this decision; adopting it is possible later as its own registry-invalidating event with a fresh anchor re-validation.
- **Drawing the config from `Providers::Random` in the workload.**
  Consumes `SIM_RNG` ahead of the swizzle controller's existing draws and would silently change the pinned regression sub-seeds' plans.
  Rejected in favour of the pure `current_sim_seed()` hash.
- **Swarming the repair/suppressor mechanisms in v1.**
  Highest masking power on paper, but constraint 4's evidence (#362–#368) shows omission fails runs for harness reasons; needs redesigned per-config success gates first.
  Deferred.
- **Enabling the `buggify` feature in the CI seed jobs.**
  Would put the fault dimension in the daily sweep at the cost of changing both workflow invocations (they must stay mirrored) and invalidating the replay regime of all twelve anchors in CI.
  Deferred; the `--all-features` per-PR `test` job already exercises the armed path on the default seed, and the op dimension keeps the daily configs fresh.
- **Per-label RNG substreams inside `Buggify`.**
  Would make each label's roll sequence independent of the others' enablement, allowing cross-config differential comparison of a single label.
  Rejected for now: replay identity only requires `(seed → config → arming)` to be deterministic, which the filter short-circuit already guarantees; substreams triple the helper's surface for a comparison mode no test performs yet.

## References

- Groce, Zhang, Eide, Chen, Regehr — _Swarm Testing_, ISSTA 2012.
- [ADR-0026](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) §D2 — the pure-sim chaos suite this campaign runs in.
- [ADR-0036](0036-moonpool-seed-sweep-daily-random.md) — daily random discovery cadence; unchanged.
- [ADR-0047](0047-failing-seed-registry-per-pr-replay.md) — failing-seed registry + replay; §5's local command lands here.
- [ADR-0048](0048-buggify-fault-injection.md) — the four labels; **amended**: per-label filtering via `Buggify::with_rng_and_filter`.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — four-layer policy this changeset ships under.
- `crates/magnetar-runtime-moonpool/src/swarm.rs` — the config type and draw.
- `crates/magnetar-proto/src/buggify.rs` — the label filter.
- `crates/magnetar-runtime-moonpool/tests/sim_chaos.rs` — the swarm-drawing workload.
- `crates/magnetar-runtime-moonpool/seeds/known-failing.toml` — the anchors re-validated by this changeset.
- `xtask/src/main.rs` — `check-known-failing-seeds`.
