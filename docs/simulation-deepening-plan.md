# Simulation Deepening — Implementation Plan

> **Status.** Planning artifact. Will be deleted once the five priorities
> below land. The binding decisions live in their respective ADRs
> (0048–0050 + amended 0036); this file is only the orchestration sheet.
>
> **Renumber note (2026-06-01).** Origin/main landed new ADR-0046 (e2e
> tests as casual) and ADR-0047 (failing-seed registry) while these
> waves were in flight. The three ADRs this plan introduces were
> renumbered from the originally-planned 0046/0047/0048 to **0048
> (buggify), 0049 (assertion density), 0050 (swizzle-clog)** at rebase
> time. Commit messages and ADR headers were rewritten to match;
> in-code `// ADR-XXXX` comments where present were updated in the same
> rebase pass.
>
> **Source spec.**
> [`docs/simulation-patterns.md`](simulation-patterns.md) §4 is the
> research-grade comparison of FoundationDB, moonpool, and TigerBeetle
> simulation patterns. That doc ranks five gaps; this doc turns the
> ranking into a sequenced execution plan.

## Goal

Close the five FDB+TigerBeetle simulation gaps called out in
[`docs/simulation-patterns.md`](simulation-patterns.md) §4, plus bump
moonpool's CI seed-coverage budget from 16 to 128 daily random seeds.

Per [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md),
each behavioral change ships with the **four-layer test set** (proto unit
test + tokio integration + moonpool integration + differential
equivalence) in the same commit, plus an end-to-end test where
applicable.

## Workflow shape — three waves

P1 + P2 both touch `magnetar-proto` (`conn.rs`, `producer.rs`) so they
serialise. P3 + P5 both extend `crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`
so they serialise. P4 is pure CI YAML. Result: three waves, each owned
by one subagent in its own `wt` worktree.

| Wave | Agent | Priorities | Files touched (primary) |
| --- | --- | --- | --- |
| W1 | A | P1 buggify + P2 assertion density | `crates/magnetar-proto/src/{conn.rs,producer.rs}`, new feature flag, new ADR-0048/0049 |
| W2 | B | P3 swizzle-clog + P5 per-handle invariants | `crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`, new ADR-0050 |
| W3 | C | P4 daily seed bump | `.github/workflows/moonpool-seed-sweep.yml`, ADR-0036 amendment |

W3 runs in parallel with W1+W2 (no source overlap). The supervisor (main
session) runs the validation chain between waves and resolves any
merge sequence.

## Per-priority deliverables

### P1 — Buggify points (FDB pattern, ADR-0048)

A new `magnetar-proto` feature `buggify` adds `#[cfg(feature = "buggify")]`
blocks at four named choice points. Each block consults the
`Providers::Random` stream via a `Buggify::should_fire(label)` helper so
firing is **seed-controlled** under simulation and **always off** in
production builds.

**Choice points** (with file:line anchors from current HEAD):

| Label | Location | Effect under simulation |
| --- | --- | --- |
| `connection.reset.delay` | [`conn.rs:523`](../crates/magnetar-proto/src/conn.rs) `Connection::reset` | inject a synthetic `Timer` arming so reset takes one extra tick before publishing the event |
| `batch_container.flush.split` | [`conn.rs:2462`](../crates/magnetar-proto/src/conn.rs) `flush_producer` | flush one fewer message than would otherwise leave, deferring the tail |
| `handle_bytes.short_read` | `Connection::handle_bytes` entry | force a single-byte split of the incoming buffer to exercise framing-resume paths |
| `retry_clock.skew` | `RetryClock::next_delay` | scale the returned `Duration` by a seed-driven jitter (e.g. `×0.5..×2.0`) |

**Test footprint** (per ADR-0024):
- `magnetar-proto`: unit tests for each `Buggify::should_fire` label under fixed `ChaCha8Rng`.
- `magnetar-runtime-tokio`: integration test asserting feature flag off → zero behavior delta (NOP build).
- `magnetar-runtime-moonpool`: integration test under `buggify` feature confirming each label fires across a seed sweep.
- `magnetar-differential`: equivalence — without `buggify`, tokio and moonpool stay byte-identical.

### P2 — Assertion density (TigerBeetle pattern, ADR-0049)

Pair-assertions (positive + negative space) on every `Connection::record_*`
entry in `crates/magnetar-proto/src/conn.rs`. Specifically the
negative-space guard the source doc calls out:

```rust
debug_assert!(
    self.in_flight_publish_snapshots.is_empty()
        || self.session_epoch == 0,
    "rebuild_producers entered with non-empty snapshot map and non-zero epoch"
);
```

Plus matching guards at every `Connection::record_send`,
`record_receipt`, `record_session_lost` entry: assert what we expect *and*
assert what we forbid. `debug_assert!` only — no production cost.

**Test footprint**: extend existing `conn.rs` unit tests with an
assertion-firing test using a constructed bad state; no new wire
behavior, so no differential change needed beyond rerunning the existing
diff parity gate. Justify the no-differential exemption in the commit
message per ADR-0024 "Exemptions" clause.

### P3 — Swizzle-clog workload (FDB pattern, ADR-0050)

New `BrokerWorkload::Swizzle { n_clogged: usize, restore_order: Vec<u64> }`
variant in [`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`](../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs).

Behaviour:
1. Workload picks `n_clogged` random consumers (seed-driven), stops
   their permit issuance.
2. Holds the clog for a seed-driven duration (e.g. 50–500 virtual ms).
3. Restores them in the order in `restore_order` — different from the
   stop order — so reconnection-ordering bugs surface.

**Invariants asserted** during swizzle:
- No duplicate messages on the unaffected consumers.
- No deadlock: every clogged consumer eventually receives or surfaces
  `SessionLost`.
- Monotonic message-id holds across the swizzle window.

**Test footprint**: new test `sim_chaos_swizzle_clog_sweep_16_seeds` in
`sim_chaos.rs` — the moonpool layer carries this; ADR-0024 §"chaos pack"
exempts pure simulation-only extensions from cross-runtime parity
because there is no tokio counterpart.

### P5 — Per-handle invariants (TigerBeetle pattern, no new ADR)

Extend `sim_chaos.rs`'s `Invariant` set with a per-(producer, consumer)
handle assertion: every `OpSend` recorded in the workload's send log
must resolve to exactly one of `Sent` / `SessionLost` /
`MemoryLimitExceeded`. Drop, double-resolve, or `Pending`-forever fails
the invariant.

Implementation: a `HandleResolutionInvariant` struct alongside the
existing `MonotonicMsgIdInvariant` (sim_chaos.rs:832), wired into the
existing run loop.

**Test footprint**: no new test file; the invariant fires inside the
existing `sim_chaos_produce_consume_*` tests. ADR-0024 exempts
invariant-pack extensions from cross-runtime parity.

### P4 — CI seed coverage bump (amends ADR-0036)

[`.github/workflows/moonpool-seed-sweep.yml`](../.github/workflows/moonpool-seed-sweep.yml)
moves from **16 → 128 random `u64` seeds nightly**.

Rationale (carried into amended ADR-0036):
- ~50 ms per moonpool test × ~30 tests × build/cache overhead ⇒ each
  matrix runner finishes well inside the 60-minute timeout.
- 128 seeds × ~6 minutes ≈ 12.8 runner-hours/night — comfortable on
  ubuntu-latest under GH Actions' default concurrency cap.
- 128 × 365 ≈ 46,720 distinct seeds/year vs. 5,840 at the current 16.

No new workflow file; no weekly soak. Just amend the existing seed-roll
job and update ADR-0036's "Decision" §2 + "Alternatives considered"
("More seeds per day" flips from rejected to accepted with the runtime
evidence).

## Validation between waves (supervisor)

Skipping local seed sweep per `feedback-skip-local-seed-sweep.md`; FIPS
on Linux uses clang per `reference-fips-needs-clang-on-linux.md`.

```bash
cargo +nightly fmt --all
cargo build --workspace --all-features
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features
cargo run -p xtask -- check-no-channels
cargo run -p xtask -- check-no-io-deps
cargo run -p xtask -- check-no-internal-clock
cargo run -p xtask -- check-runtime-test-parity
cargo run -p xtask -- codegen --check
```

`check-sim-coverage` and `check-crypto-matrix` are diff-shaped /
FIPS-blocked locally per the memory notes; they run via
`.github/workflows/xtask-gates.yml` once the branches push.

## Subagent dispatch protocol

Each wave-owning agent receives:

1. A self-contained prompt restating the priority from this doc.
2. The exact file paths to touch (anchored above).
3. The four-layer test list from ADR-0024.
4. Instructions to: (a) `wt switch --create feat/<scope> -y` first;
   (b) land ADR + code + tests in **one signed-off + GPG-signed commit
   per priority**, no Claude attribution per ADR-0012; (c) report back
   the worktree path and validation-chain output, not a free-form
   summary.

Per supervisor pattern (CLAUDE.md): the supervisor validates against
source, retries each agent up to 2× on failure, and only merges after
the validation chain passes.

## Merge sequence

1. W3 (CI seed bump) lands first — cheapest, unblocks the bigger
   coverage net for everything else.
2. W1 (buggify + assertions) lands next — adds new fault-injection
   surface that W2's swizzle-clog and P5's invariants can leverage if
   they want.
3. W2 (swizzle-clog + per-handle invariants) lands last.

After W1 lands, the supervisor reruns the daily-sweep manually via
`workflow_dispatch` to confirm the bumped 128-seed sweep stays green
under the new buggify points.

## Out of scope (per ADR-0026 §D2 and the source doc)

- Replacing moonpool with `madsim` or `loom`.
- VOPR-equivalent dedicated hardware runner (per-seed runtime stays in
  the sub-second regime, so a 50ms-per-test soak job belongs on shared
  CI, not bare-metal).
- New buggify points outside the four named in P1 — additions are a
  follow-up tracked in `docs/follow-ups.md` once the surface is in.

## References

- [`docs/simulation-patterns.md`](simulation-patterns.md) — research note this plan operationalises.
- [ADR-0003](../specs/adr/0003-no-channels-rule.md) — channels-ban (applies inside buggify helpers).
- [ADR-0011](../specs/adr/0011-clock-injection-sans-io.md) — clock injection (applies to `retry_clock.skew` buggify).
- [ADR-0012](../specs/adr/0012-no-claude-attribution.md) — commit hygiene.
- [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md) — four-layer test policy.
- [ADR-0036](../specs/adr/0036-moonpool-seed-sweep-daily-random.md) — to be amended by P4.
- [`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`](../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs) — touched by P3 + P5.
