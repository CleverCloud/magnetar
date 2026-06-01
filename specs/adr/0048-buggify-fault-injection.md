# ADR-0048 — Buggify fault-injection points in `magnetar-proto`

- **Status**: Accepted
- **Date**: 2026-06-01
- **Decider**: Florentin Dubois
- **Tags**: simulation, fault-injection, testing, magnetar-proto, moonpool

## Context

[`docs/moonpool-engine.md`](../../docs/moonpool-engine.md#appendix--reference-patterns-foundationdb-and-tigerbeetle) documents FoundationDB's **buggify** pattern: scatter `if BUGGIFY then …` blocks at named choice points in the production state machine.
Each block is gated on a seed-controlled RNG roll; with simulation enabled, the simulator flips the gate true at a tunable probability so the surrounding code takes the rarely-exercised branch.
With simulation off, the gate is a constant `false` and the alternate path is dead code.

The pattern compounds with deterministic simulation: every (seed, buggify-point) pair is a bug-finding lever the simulator owns, and the cost-per-lever is one `if` block.

The simulation-deepening rollout (now consolidated into ADRs 0047–0050) named four magnetar choice points where the pattern transfers directly:

| Label                         | Location                               | Effect under simulation                                                                                       |
| ----------------------------- | -------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `connection.reset.delay`      | `Connection::reset`                    | Preserve `last_activity` so the post-reset keepalive baseline is older.                                       |
| `batch_container.flush.split` | `Connection::flush_producer`           | When the batch holds >1 message, skip the flush; the batch survives untouched and the next caller flushes it. |
| `handle_bytes.short_read`     | `Connection::handle_bytes_decode_loop` | Return after the first decoded frame even if `inbound` holds more, exercising the framing-resume path.        |
| `retry_clock.skew`            | `Backoff::next`                        | Scale the returned `Duration` by a seed-driven factor in `[0.5, 2.0]`.                                        |

[ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) sets the test-layer policy this change ships with (proto unit + tokio integration + moonpool integration + differential equivalence).
[ADR-0011](0011-clock-injection-sans-io.md) sets the precedent: the RNG injection mirrors the wall-clock / monotonic-clock injection already in place — the engine plugs in a closure, the proto crate does not depend on `rand`.

[ADR-0003](0003-no-channels-rule.md) bans channels; the buggify fire-counter map sits behind a [`parking_lot::Mutex`], the same primitive the rest of the proto crate uses.

[ADR-0004](0004-sans-io-protocol-core.md) bans I/O deps in `magnetar-proto`; the helper accepts an `Arc<dyn Fn() -> u64>` so the proto crate never depends on `rand` even with the feature on.

## Decision

Add a new opt-in `buggify` feature on `magnetar-proto`.
Default OFF.
When the feature is OFF, every [`Buggify::should_fire`] call is `#[inline(always)] -> false` and the four choice-point `if` branches compile to dead code.
Production builds pay nothing.

When the feature is ON, the helper carries an `Arc<dyn Fn() -> u64 + Send + Sync>` RNG handle, supplied by the engine via [`Connection::set_buggify`] (and shared with [`Backoff::install_buggify`] for the `retry_clock.skew` label).

Concretely:

- **New module** `crates/magnetar-proto/src/buggify.rs` exports [`Buggify`], the label constants in `buggify::labels`, and (under the feature) the `BuggifyRng` alias and `Buggify::with_rng` / `Buggify::roll_u64` / `Buggify::fire_count` methods.
- **New field** `Connection::buggify: Buggify`.
  Initialised to `Buggify::disabled()` in `Connection::new`.
  Engines opt in via `Connection::set_buggify(rng)` after construction.
- **New field** `Backoff::buggify: Buggify`.
  Initialised to `Buggify::disabled()` in `Backoff::new`.
  Engines opt in via `Backoff::install_buggify(buggify)` so the same helper instance (and the same fire-counter map) is shared between the connection's three choice points and the Backoff's skew layer.
- **Four labels** in the order above, each at the documented choice point.
  Default fire-probability is **5%** for all four.
- **Engine wiring**. The tokio runtime ships `Buggify::disabled` and never calls `set_buggify`; the moonpool runtime ships `Buggify::disabled` by default too, and integration tests opt in by calling `Connection::set_buggify` with a seeded RNG handle derived from `Providers::Random`.

Probability picks (5% across the board) follow FoundationDB's "low probability, many opportunities" stance: per-call odds are small enough that no single test run is dominated by buggified behaviour, but seed-sweeping discovers most failure modes within a daily 128-seed run.

## Consequences

**Positive**

- Each of the four labels becomes a bug-finding lever the moonpool simulator owns, at the cost of one `if` block in production code.
- The Buggify helper itself is unit-tested (`buggify.rs::tests`), deterministic for a fixed RNG, and threaded through the proto crate without a new dependency.
- Production builds compile the buggified branches to dead code (no runtime overhead).
- ADR-0024 4-layer parity is preserved: proto-unit + tokio + moonpool
  - differential tests all ship in the same commit.

**Negative**

- Two new methods on the public surface (`Connection::set_buggify`, `Backoff::install_buggify`) — small, but they belong to the proto-crate's "engine-facing wiring" set and need to stay stable.
- The `Buggify` struct grows a `parking_lot::Mutex<HashMap>` under the feature for the fire-counter map.
  Allocation cost is paid only when the feature compiles.

**Neutral**

- The `buggify` feature is additive across all workspace crates (proto + tokio + moonpool + façade + differential).
  `--all-features` builds compile every branch.
- Differential equivalence under the default (no wiring) is trivial: both engines ship `Buggify::disabled`, so the `should_fire` short-circuit makes the proto state machine byte-identical to its pre-ADR-0046 self.

## Alternatives considered

- **`rand`-crate dependency in `magnetar-proto`**. Rejected: violates ADR-0004 (zero I/O deps in proto); pulling `rand` would force the proto crate to track an RNG state, which leaks the engine-side seed contract into the sans-io core.
- **Closure-free trait `BuggifyRng: Send + Sync`**. Rejected: forces a dyn-trait surface on the proto crate for a one-method trait; the `Arc<dyn Fn>` shape matches the existing `wall_clock` injection and keeps the engine wiring consistent.
- **More than four labels in the initial cut**. Rejected: the source spec in [`docs/moonpool-engine.md`](../../docs/moonpool-engine.md#appendix--reference-patterns-foundationdb-and-tigerbeetle) named these four as the highest-leverage points; additional labels are a tracked follow-up once the surface lands (per the simulation-deepening rollout, now consolidated into ADRs 0047–0050).
- **Tunable per-label probability**. Rejected for now: a single 5% default keeps the call sites simple; tunable probabilities belong behind a `BuggifyConfig` struct that we'll add only when a real test needs it.

## References

- [`docs/moonpool-engine.md` — appendix](../../docs/moonpool-engine.md#appendix--reference-patterns-foundationdb-and-tigerbeetle) — source comparison of FoundationDB, moonpool, and TigerBeetle simulation patterns.
- Simulation-deepening rollout (removed after landing) — sequenced this ADR alongside ADR-0047, ADR-0049, ADR-0050, and the ADR-0036 amendment; consolidated history lives in those ADRs.
- `crates/magnetar-proto/src/buggify.rs` — helper implementation.
- `crates/magnetar-proto/src/conn.rs` — three of the four choice points (`reset`, `flush_producer`, `handle_bytes_decode_loop`).
- `crates/magnetar-proto/src/backoff.rs` — `retry_clock.skew` point.
- [ADR-0003](0003-no-channels-rule.md) — channels ban; applies to the buggify fire-counter map.
- [ADR-0004](0004-sans-io-protocol-core.md) — sans-io constraint; applies to the RNG closure (no `rand` dep in proto).
- [ADR-0011](0011-clock-injection-sans-io.md) — clock injection; buggify RNG injection follows the same shape.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — four-layer test policy.
- [ADR-0028](0028-supervised-reconnect-anti-thrash-policy.md) — pair of buggify with the anti-thrash detector (both observe re-attach behaviour).
