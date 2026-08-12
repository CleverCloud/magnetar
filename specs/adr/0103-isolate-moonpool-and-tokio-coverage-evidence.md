# ADR-0103 — Isolate Moonpool and Tokio patch-coverage evidence

- **Status**: Accepted
- **Date**: 2026-08-12
- **Decider**: Florentin Dubois
- **Tags**: testing, coverage, tokio, moonpool, artifact-provenance, ci
- **Amends**: [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md), [ADR-0090](0090-widen-sim-coverage-report-to-compiled-closure.md), [ADR-0092](0092-enforce-sim-coverage-and-gate-every-pull-request.md), [ADR-0100](0100-isolate-sim-coverage-current-pass-artifacts.md), and [ADR-0102](0102-assignment-driven-m1-hardened-stream-consumer.md)

This decision historically amends ADR-0102 lines 176-190 and 232 only: its single eight-package report topology becomes seven Moonpool/shared packages plus one isolated Tokio package, and its “unchanged two-root execution” reference no longer governs.
ADR-0102's record-less-file rule remains binding.

## Context

Repeated fresh measurements proved a contradiction in `check-sim-coverage`.
The gate executed only `magnetar-runtime-moonpool` and `magnetar-differential`, yet hard-gated private `magnetar-runtime-tokio` adapter source.
Tokio unit and integration tests therefore could not satisfy the gate, while private TLS, defensive, and runtime-adapter branches could be reached only by artificial differential hooks or not at all.
That is not a coverage deficit in Tokio evidence; it is an evidence-provenance defect in the gate.

The same review found two independent integrity holes.
Lines containing `unreachable!`, `unimplemented!`, or `todo!` were removed lexically even though LLVM treats executable panic and placeholder lines as coverage mappings.
A file with some `DA:` records can still hide an added executable mapping gap because the existing hard failure operates at file level.
Dependency-free lexical function scanning was measured as unsound for this purpose, so this decision records the gap without claiming to close it.

## Decision

`check-sim-coverage` remains one command and one hard final verdict, but owns two isolated evidence domains.

1. The Moonpool domain executes `magnetar-runtime-moonpool` and `magnetar-differential` tests and reports only `magnetar-proto`, `magnetar-runtime-moonpool`, `magnetar-differential`, `magnetar-auth-athenz`, `magnetar-auth-sasl`, `magnetar-driver`, and `magnetar-fakes`.
2. The Tokio domain executes `magnetar-runtime-tokio` unit and integration tests plus the existing differential tests, but reports only `magnetar-runtime-tokio` adapter source.
   Differential hits in this domain are Tokio-side adapter evidence; the domain's profiles can never satisfy shared or proto coverage.
3. The domains have disjoint package and source ownership.
   A Tokio hit cannot satisfy Moonpool/shared/proto coverage, and a Moonpool or differential hit cannot satisfy Tokio adapter coverage.
4. One invocation-owned scratch root contains separate `moonpool/` and `tokio/` subtargets.
   Each domain has independent objects, raw profiles, profdata, and LCOV export; no artifacts are merged or reused across domains.
5. Locked metadata, `opt-level = 0`, clang/FIPS selection, artifact-flag rejection, output-only diagnostic LCOV files, and fail-closed cleanup remain binding.
6. `unreachable!`, `unimplemented!`, and `todo!` receive no lexical exclusion.
7. ADR-0102's record-less-file behavior remains unchanged.
   Same-file missing-`DA:` detection remains an unresolved follow-up and has no bespoke parser or hard verdict in this decision.
8. The command succeeds only when both domains have zero uncovered added executable `DA:` lines and no ADR-0102 record-less-file failure.

Each report is generated inside its domain scratch, validated, read immediately, and retained in memory through the verdict.
Only after both authoritative byte sets survive scratch cleanup are `target/sim-coverage.lcov` and `target/tokio-coverage.lcov` atomically replaced as diagnostics; neither is reread by a fresh verdict.

## Historical pre-final measurement

The following 2026-08-12 figures came from a pre-final uncommitted pipeline with a warm dependency cache and fresh invocation-owned first-party targets:

| Domain   | Fresh execution + report | Uncovered added `DA:` lines |
| -------- | -----------------------: | --------------------------: |
| Moonpool |                 158.021s |              27 in one file |
| Tokio    |                 156.275s |              32 in one file |
| Command  |                 321.888s |                      failed |

The historical Moonpool observation was `magnetar-runtime-moonpool/src/scalable.rs:259,1145,1558,1810,1972-1976,2127,2213,2693-2695,2702-2703,2719-2721,2951,2973-2977,2982-2983`.
The historical Tokio observation was `magnetar-runtime-tokio/src/scalable.rs:243,333,411,441-442,504,1069,1478,1727,1889-1893,2042,2128,2607-2609,2616-2617,2633-2635,2852,2874-2878,2883-2884`.
These timings and line observations are explicitly non-authoritative for the current pipeline and are not current deficits.
Fresh current-HEAD timings and deficits remain pending until the implementation is committed; no zero-coverage claim is made.

## Consequences

Tokio adapter changes can now be discharged by honest Tokio tests, including private runtime and TLS behavior, without weakening Moonpool evidence for shared and simulation-owned source.
The command costs a second isolated all-feature instrumented execution and cannot reuse first-party objects across domains.
That cost is accepted because provenance is part of the hard-gate claim.

## References

- `xtask/src/main.rs` — `SIM_COVERAGE_DOMAINS`, isolated domain targets, retained report bytes, atomic diagnostic publication, and aggregate verdict.
- `.github/workflows/ci.yml` and `.github/workflows/xtask-gates.yml` — per-PR and dispatchable command contracts.
- [`docs/testing.md`](../../docs/testing.md) — contributor-facing evidence ownership.
