# ADR-0123 — Quarantine exact intermittent test scenarios with visible diagnostics

- **Status**: Accepted
- **Date**: 2026-10-16
- **Decider**: Florentin Dubois
- **Tags**: testing, quarantine, ci, coverage
- **Amends**: [ADR-0021](0021-no-silent-test-ignore-or-remove.md), [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md), and [ADR-0121](0121-isolate-moonpool-and-tokio-coverage-evidence.md)

## Context

CI run [38863061843](https://github.com/CleverCloud/magnetar/actions/runs/38863061843) has two intermittent differential failures against the fake broker and one Pulsar 5.0.0 e2e failure. The exact scenarios, observed symptoms, CI job evidence, owners, review date, and reactivation conditions are maintained in [`docs/test-quarantine.md`](../../docs/test-quarantine.md).

The coverage command also stopped after a test execution returned nonzero, before exporting the current Moonpool report or attempting the Tokio domain. That hid useful fresh evidence and left older diagnostic LCOV files in place.

## Decision

Only the three exact scenarios in the quarantine register receive `#[ignore]`. Their assertions remain unchanged, and the existing ACK/NACK ignore remains a separate exception.

CI runs each ignored scenario by exact name in a visible diagnostic. Test results alone are non-gating inside these diagnostic jobs. Compilation, selection checks, toolchain setup, and required e2e image preparation stay blocking. The e2e diagnostic runs on the existing Pulsar 5.0.0 CI path and is not added to the 4.0.14 nightly. The e2e result is non-gating only after Pulsar startup reaches the scenario's readiness marker; a failure before that is an infrastructure failure and keeps the job red. Every other test and suite remains blocking.

`check-sim-coverage` records test execution, infrastructure, report collection, and coverage verdicts separately. Even when a test command exits nonzero, it attempts fresh reports in both isolated domains, evaluates every valid report, atomically publishes only valid reports from the current invocation, and fails the final verdict. A failed or missing report removes the corresponding stale LCOV diagnostic. The ADR-0121 domain ownership, 100%-on-the-diff threshold, and `SIM_COVERAGE_ENFORCES_UNCOVERED` remain unchanged.

## Consequences

The exact failure scenarios remain reproducible and visible without making unrelated tests non-blocking. Valid fresh reports remain available after a failed test run, while stale reports cannot be mistaken for current coverage evidence. Each quarantine is reviewed by the date and reactivated only under the scenario-specific conditions in the canonical register.

## References

- [`docs/test-quarantine.md`](../../docs/test-quarantine.md) — canonical exact-scenario register and reactivation criteria.
- `.github/workflows/ci.yml` and `.github/workflows/e2e-pulsar.yml` — blocking setup and visible targeted diagnostic jobs.
- `xtask/src/main.rs` — separate test, infrastructure, collection, and coverage verdicts.
