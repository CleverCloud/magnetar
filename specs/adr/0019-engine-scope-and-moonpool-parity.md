# ADR-0019 — Engine scope: tokio is the primary engine; moonpool parity is a follow-up train

- **Status**: Accepted (§gate (e) "façade surface stays bound to `PulsarClient<TokioEngine>`" partially superseded by [ADR-0027](0027-moonpool-engine-clientstate-is-runtime-client.md), 2026-05-23 — the engine-generic builder entry points now also dispatch on `MoonpoolEngine<P>`)
- **Date**: 2026-05-21
- **Decider**: Florentin Dubois
- **Tags**: scope, engines, moonpool, java-parity

## Context

[ADR-0010](0010-v0-1-full-java-parity.md) states magnetar ships **full Java parity, no deferrals**. It does _not_ specify which engine satisfies the parity matrix.
Magnetar ships two engines:

- `magnetar-runtime-tokio` — production engine. ~4,500 LOC.
  Every parity-matrix row that is marked ✅ in [`README.md#java-client-parity-matrix`](../../README.md) is satisfied by this engine.
- `magnetar-runtime-moonpool` — deterministic-simulation engine for invariant testing. ~2,800 LOC.
  M1–M4 landed (engine, client, producer, consumer).
  Significant surface still missing: supervised reconnect, DNS resolver injection, driver-level TLS, `memory_limit` accounting, `ServiceUrlProvider` plumbing, PIP-188 `TOPIC_MIGRATED` handling, and the entire façade layer (partitioned / multi-topics / pattern / reader / table-view / transactions / typed schemas), all of which today live only in `crates/magnetar/src/*` against the tokio engine.

[ADR-0004](0004-sans-io-protocol-core.md) makes the engines swappable behind `magnetar-proto`, so dual-engine parity is _possible_ but the work is large (planned as moonpool-M5..M8).
Forcing dual-engine parity on the same cadence as the tokio engine would block the user-visible release indefinitely.

The reviewer + auditor of the Phase 2 plan (2026-05-21) flagged this as the highest-impact undecided question.

## Decision

The Java parity matrix in [`README.md`](../../README.md) is satisfied **by the tokio engine**. Moonpool-engine parity with tokio is tracked as a follow-up train — moonpool-M5 through moonpool-M8 — and is **not** a release gate.

Concretely:

1. The parity-matrix row `✅` / `🟡` / `❌` markers reflect _tokio_-engine coverage.
   A row is `✅` iff the feature works end-to-end on `magnetar-runtime-tokio`.
2. Moonpool gaps relative to tokio are tracked in [`README.md` §"Engine-by-engine surface coverage"](../../README.md#engine-by-engine-surface-coverage).
3. ADR-0010 is **clarified, not weakened**: "full Java parity" still holds; the qualifier is "as exposed by `PulsarClient<TokioEngine>`".
   The moonpool engine remained a _test-only_ deterministic-simulation surface until the façade was lifted onto an `Engine` trait.
   That lift landed on 2026-05-22: the façade now ships `PulsarClient<E: Engine = TokioEngine>` with a moonpool branch that re-exports the engine's shared-state and driver-handle plumbing without lifting the producer / consumer surface (that is the M7–M8 work).
   When dual-engine parity is reached, this ADR will be superseded by an explicit "dual-engine parity reached" ADR.
4. Public crate surface:
   - `magnetar` ships `PulsarClient<E: Engine>` where `E` defaults to `TokioEngine` (Option A per gate (e) 2026-05-21, landed 2026-05-22).
     Users targeting production use the default; users running deterministic tests parametrise with `MoonpoolEngine<P>`.
     The `Engine` trait carries an associated `ClientState` type so each engine plugs in its own per-client storage.
   - Moonpool-only callers that need features not yet implemented in moonpool (partitioned, multi-topics, pattern, reader, table view, transactions, typed schemas, supervised reconnect, DNS, TLS, memory_limit, ServiceUrlProvider, PIP-188) get `compile_error!` or trait-bound failures, not silent fallbacks.

## Consequences

**Positive**

- Unblocks the user-visible release from waiting on moonpool surface completion.
- Clarifies the parity matrix without weakening ADR-0010.
- Makes the moonpool train an explicit follow-up, with M5–M8 milestones the reader can track.
- Generic `PulsarClient<E>` (Option A) keeps both engines available in the same binary — moonpool can be used for in-process simulation alongside live tokio I/O if a downstream test harness wants both.

**Negative**

- Generic `PulsarClient<E>` adds turbofish noise (`PulsarClient::<TokioEngine>::new(...)`) to user code that wants to be explicit.
  The default type parameter mitigates this for the common case.
- Two parity matrices conceptually exist (tokio-side + moonpool-side).
  Both live in `README.md`: the headline matrix tracks tokio and the [engine-by-engine surface coverage](../../README.md#engine-by-engine-surface-coverage) table tracks the moonpool gap.
  Future audits must keep both honest.
- Trait-bound failures when a moonpool user reaches for an unimplemented feature are a learning curve.
  Documented in `crates/magnetar-runtime-moonpool/README.md`.

**Neutral**

- The sans-io invariants ([ADR-0003](0003-no-channels-rule.md), [ADR-0004](0004-sans-io-protocol-core.md), [ADR-0011](0011-clock-injection-sans-io.md)) are unchanged.
  The engine split lives entirely above `magnetar-proto`.

## Alternatives considered

- **Option B — Duplicate façade per engine.** Rejected: ~6,400 LOC copy-paste blast radius in `crates/magnetar/src/*`, and every future parity row would have to land twice.
- **Option C — Feature-gated façade alias.** Discussed; would keep the public API non-generic but forces one engine per build.
  Rejected at gate (e) on 2026-05-21 in favour of Option A (generics) because the user wanted both engines simultaneously available.
- **Refuse moonpool until parity** (require moonpool to ship M5..M8 before the tokio engine reaches user-visible release).
  Rejected: blocks the release indefinitely; defeats the purpose of separating the production engine from the simulation engine.

## References

- [ADR-0003](0003-no-channels-rule.md) — no-channels rule.
- [ADR-0004](0004-sans-io-protocol-core.md) — sans-io protocol core + swappable engines.
- [ADR-0010](0010-v0-1-full-java-parity.md) — full Java parity (this ADR clarifies _which engine satisfies it_).
- [ADR-0011](0011-clock-injection-sans-io.md) — clock injection.
- `tasks/todo.md` Phase 2 M5–M8 — moonpool parity train.
- [`README.md` §"Engine-by-engine surface coverage"](../../README.md#engine-by-engine-surface-coverage) — moonpool gap table.
