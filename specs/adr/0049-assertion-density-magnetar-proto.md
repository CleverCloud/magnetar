# ADR-0049 — Assertion density on `Connection` state machine

- **Status**: Accepted
- **Date**: 2026-06-01
- **Decider**: Florentin Dubois
- **Tags**: testing, simulation, magnetar-proto, tigerbeetle-pattern

## Context

`docs/simulation-patterns.md` §3 documents
[TigerStyle](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md)'s
**assertion-density** rule: a function should carry ≥2
`debug_assert!`s, one stating the positive expectation and one
covering the negative space. Under deterministic simulation,
assertions downgrade silent correctness bugs into loud liveness
bugs the simulator catches in seconds.

`magnetar-proto`'s `Connection` state machine has the right shape
for this — every entry runs to completion under the caller's
mutex, no `.await` in the middle, so preconditions hold throughout
the body. But until this ADR the proto crate had **few**
`debug_assert!`s in production code. The
`in_flight_publish_snapshots` regression captured in commit
`0e47e14` is the canonical example: a second `reset()` wiped the
first reset's snapshots and silently dropped a user-queued send.
A negative-space assertion at `rebuild_producers` entry — `the
snapshot map is empty OR session_epoch > 0` — would have caught
it at the first failing test rather than during ADR review.

The four-layer test policy in
[ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) means
the assertion-density rule applies once on the proto crate and
the assertion fires across every test layer — proto unit,
runtime integration, differential, e2e — for free.

Scope: this ADR covers `Connection::record_*` and
`Connection::rebuild_*` entries plus the canonical negative-space
guard called out in the source spec. Additional assertion sites
are a tracked follow-up.

## Decision

Add pair-`debug_assert!`s (positive + negative space) on the
following `Connection` entries:

1. **`Connection::record_reattach_outcome`**
   - *Positive*: a `ReAttachOk` outcome's handle must reference a
     producer or consumer this Connection actually has open
     (`producers.contains_key` / `consumers.contains_key`).
     `TcpDropAfterReAttach` is exempt — engine drivers record it
     with a placeholder handle.
   - *Negative space*: `TcpDropAfterReAttach` requires
     `session_epoch > 0`. A drop recorded against a fresh
     never-reset connection means the driver mis-classified the
     first connect as a re-attach.

2. **`Connection::record_first_op_success`**
   - *Positive*: the connection must be in
     `HandshakeState::Connected` — the only state from which the
     broker dispatches `SendReceipt` / `Message` frames.
   - *Negative space*: at least one producer OR one consumer must
     be open. There is no first-op to succeed against an empty
     slot map.

3. **`Connection::rebuild_producers` entry** (the canonical guard
   from `docs/simulation-patterns.md` §3 takeaway 2):
   - *Negative space*:
     `in_flight_publish_snapshots.is_empty() || session_epoch > 0`.
   - *Positive*: every snapshot key must reference a producer in
     the `producers` map (no orphan snapshots).

Every guard is `debug_assert!` only — no production cost. Tests
under `mod conn_state_tests` construct the bad state manually and
assert each guard panics under `cfg(debug_assertions)`.

## Consequences

**Positive**

- The `0e47e14` regression pattern can no longer recur silently —
  the negative-space assert at `rebuild_producers` panics
  immediately under any test layer.
- The five new asserts double as documentation: future maintainers
  read the invariants in-source instead of having to spelunk
  through ADRs.
- TigerStyle "≥2 per function" reaches the four most load-bearing
  entries on the connection state machine. Future entries follow
  the same template.
- ADR-0024 §"Exemptions" clause applies: pure `debug_assert!`
  additions don't change wire output, so no differential
  equivalence test is needed for the assertion-density change.
  The test footprint is one proto unit test per assertion (six
  total: four positive + four negative + the two canonical guards
  at `rebuild_producers`, deduplicated to four). Documented in
  the commit message.

**Negative**

- Tests that legitimately exercise post-close ghost handles (if
  any) need to be updated to avoid the `record_reattach_outcome`
  positive guard. None observed in the current test corpus.
- Debug-mode performance is microscopically affected by the
  per-call `HashMap::contains_key` lookups. Production binaries
  (release mode) compile the asserts away entirely.

**Neutral**

- The assertions do not change the production behaviour. A
  `cargo build --release` is byte-for-byte unchanged.
- ADR-0021 (no silent `#[ignore]`) still applies: a failing
  assertion-density test is a bug to fix, not a candidate for
  ignoring.

## Alternatives considered

- **Move assertions to runtime `assert!` (production-on)**.
  Rejected: would make a production binary panic on a state the
  rest of the surface can sometimes recover from. Tests catch
  the same bugs at debug; the simulator catches them across a
  seed sweep.
- **Single positive-only assertion per function**. Rejected: the
  whole TigerStyle takeaway is the negative-space half. The
  `0e47e14` regression specifically needed the negative-space
  guard — a positive `snapshot map matches producer set` assert
  would have passed in that buggy state.
- **Wait for the buggify rollout (ADR-0046)**. Rejected: the
  asserts are independent of buggify; buggify amplifies them
  under simulation but they are valuable on their own (every
  test layer benefits, not just the moonpool sim).

## References

- `docs/simulation-patterns.md` §3 — TigerStyle reference and the
  canonical guard text.
- `docs/simulation-deepening-plan.md` §P2 — scheduling for this
  ADR alongside ADR-0046.
- `crates/magnetar-proto/src/conn.rs` — call sites where the
  asserts are wired in (search for `ADR-0047`).
- [ADR-0011](0011-clock-injection-sans-io.md) — `Instant` is
  passed in; asserts on `last_activity` use that injection
  point.
- [ADR-0021](0021-no-silent-test-ignore-or-remove.md) — failing
  asserts are fixed, not papered over.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) —
  "Exemptions" clause this change leans on.
- [ADR-0028](0028-supervised-reconnect-anti-thrash-policy.md) —
  context for `record_reattach_outcome` semantics.
- [ADR-0046](0046-buggify-fault-injection.md) — buggify
  scaffolding pairs with the assertion-density layer to catch
  the bugs the simulator otherwise misses.
