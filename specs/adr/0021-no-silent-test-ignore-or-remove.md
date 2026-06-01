# ADR-0021 — Tests are fixed, not silently ignored or removed

- **Status**: Accepted, **amended by [ADR-0046](0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md)** (the env-dep `#[ignore]` carve-out in §1 is removed for end-to-end tests; the bug-hide ban §2 + surface-and-wait §4 remain in force)
- **Date**: 2026-05-22
- **Decider**: Florentin Dubois
- **Tags**: testing, quality, process, agents

## Context

Multiple agent dispatches during the parity finish-line wave hit their per-agent turn budget mid-debug and applied an `#[ignore]` to keep CI green rather than finishing the underlying fix (most recently `crates/magnetar-differential/tests/broker_smoke.rs`, where the scripted-broker handshake stalled the producer-open round-trip).

`#[ignore]` is a sharp tool: applied for the right reason (an environment dependency the build host can't satisfy, e.g. Docker for the e2e suite) it is the project's only honest way to surface a test that needs a special harness.
Applied for the wrong reason (time pressure, a hard-to-debug failure, an unfinished feature) it silently erodes the safety net every other contributor relies on.

Today the magnetar workspace has ~55 `#[ignore]` annotations.
Almost all are environment gates (`#[ignore = "e2e: requires Docker"]`); a small minority are bug-hiders that were never tracked back to a fix.
Without a policy, that minority compounds: every dropped test is one more silent regression vector.

This ADR sets the binding rule for the workspace and for any agent operating in it.

## Decision

Tests are **fixed**, not silently ignored or removed.

Concretely:

1. **`#[ignore]` is permitted only for environment / fixture gating**, and the annotation must carry an explicit `reason` string that names the gating dependency.
   Acceptable shapes:
   - `#[ignore = "e2e: requires Docker"]`
   - `#[ignore = "requires GSSAPI broker"]`
   - `#[ignore = "needs TLS-fronted broker container"]`

The reason string is the audit signal: anyone running `cargo test --workspace --all-features -- --include-ignored` knows immediately what the gating dependency is.
CI jobs that provide the gate (the `e2e` job for Docker, future TLS / SASL jobs) run the ignored set explicitly with `--include-ignored`.

2. **`#[ignore]` for any other reason — including "the test is flaky", "I don't have time to debug", "the feature isn't finished yet", "we'll come back to it" — is forbidden**. If the test reveals a bug, fix the bug.
   If the test is wrong, fix the test.
   If neither is possible right now, **stop and surface the failure to the user for an explicit yes/no on a tracked deferral** (see point 4).

3. **Removing or `delete`-ing a test is forbidden** without explicit user confirmation in the same changeset.
   Tests document intent and prevent regressions; removing one quietly drops both.
   The exception: a test that is _strictly superseded_ by a renamed / refactored equivalent in the same commit is allowed when the commit body explicitly documents the rename.

4. **When neither fixing the bug nor fixing the test is feasible in the current dispatch**, the agent (or human contributor) must:
   1. **Stop** the implementation work.
   2. **Surface the failure** to the user with a short summary of what's failing, why a fix isn't possible right now, and a proposed gate (environment-gated `#[ignore]` with reason / new ADR / scope deferral).
   3. **Wait for explicit yes/no**. A vague nod is not consent — the policy is "explicit confirmation in the conversation, or the test stays red".
   4. **Track the follow-up** by adding a `TODO(<short-tag>):` next to the `#[ignore]` AND a task in the workspace's task list.

5. **Agent prompt template** for implementer agents (`worktrunk:`, `guidelines:`, `testing-guide:`, `tdd-parallel:`): every implementer prompt MUST reiterate "If a test fails, fix it.
   Don't `#[ignore]` it without surfacing the failure to the orchestrator first."
   This is repeated in `~/.claude/CLAUDE.md` § "Build & Validation".

6. **The `cargo xtask check-no-channels` / `check-no-io-deps` family** gets a sibling `check-no-bug-hide-ignores` job (follow-up): scans `crates/**/*.rs` for `#[ignore]` annotations whose reason string does not match a workspace-approved gate dependency list (Docker, GSSAPI, ZTS, TLS-fronted broker).
   Anything outside that list triggers a CI failure.
   Until the lint exists, the policy is enforced by code review + this ADR.

## Consequences

**Positive**

- The safety net stays load-bearing.
  Every red test means a real bug, not a someone-else's-problem time bomb.
- Audit is mechanical: `rg '#\[ignore' crates/ | grep -v "reason ="` surfaces every annotation lacking a reason string; `cargo xtask check-no-bug-hide-ignores` (when it lands) will fail CI on any annotation whose reason doesn't match an approved gate.
- Agents that can't finish a debug cycle have a documented escape hatch (surface + wait for explicit go-ahead) instead of a silent one (slap `#[ignore]` and hope).

**Negative**

- Slower close-out for hard-to-debug failures.
  Agents that would have shipped a passing-but-narrower test now have to escalate.
  The trade-off favours quality.
- The check-no-bug-hide-ignores lint adds CI complexity.
  M9 work.

**Neutral**

- Existing `#[ignore = "e2e: requires Docker"]` annotations (~50 in `crates/magnetar/tests/e2e_*.rs`) all comply already.
  The one outlier added on 2026-05-22 (`crates/magnetar-differential/tests/broker_smoke.rs` — `m8-followup: producer-open stalls under scripted broker`) is tracked in the workspace task list and gated until the underlying scripted-broker bug is fixed.

## Alternatives considered

- **Outright ban `#[ignore]`**. Rejected: real environment dependencies exist (Docker, GSSAPI, ZTS).
  The e2e suite is legitimately gated, and forcing every contributor to install Docker before running `cargo test` would be hostile to onboarding.
- **Allow `#[ignore]` freely; rely on code review**. Rejected: the parity finish-line wave proved that agents under budget pressure default to the easy out.
  Without an explicit policy, the pattern recurs.
- **Quarantine ignored tests in a separate suite that is not part of `cargo test`**. Rejected: the existing `--include-ignored` mechanism already supports this with much less ceremony.
  The point is not to hide ignored tests but to make sure they are ignored for the right reason.

## References

- [ADR-0010 — full Java parity](0010-v0-1-full-java-parity.md) (parity gaps surface via failing tests; ignoring them defeats the ADR).
- `~/.claude/CLAUDE.md` § "Build & Validation" — references this ADR.
- `tasks/todo.md` — task #24 tracks the audit and fix of all bug-hide ignores (broker_smoke + any peers discovered during the audit).
- `xtask/src/main.rs` — future home of the `check-no-bug-hide-ignores` subcommand.
