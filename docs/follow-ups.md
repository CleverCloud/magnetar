# Open Follow-Ups

Consolidated tracker for known open work.
Each entry lists the gap, the reason it stays open, and (where actionable) a `/goal …` block ready to be copy-pasted verbatim into a fresh session for an agent team to pick up.

For the public-facing parity status, see the [parity matrix in the README](../README.md#java-client-parity-matrix).

This file is the **single source of truth** for what is intentionally deferred or blocked.
Anything not listed below is either already shipped (check `git log` for the implementation reference) or explicitly out of scope ([ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) §D-series, [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md), [ADR-0032](../specs/adr/0032-pip-466-v5-client-surface-scope.md)).

When a PR closes an item, the entry is **removed** (git log + the ADR / docs file carry the post-implementation reference); partially-closed items are trimmed to their remaining open residual.

**API stability stance.** The crates are published (`magnetar-driver`, `magnetar-proto`, and the rest of the workspace).
Breaking API changes are still acceptable when they improve correctness, ergonomics, or layering, but each one must carry a `BREAKING CHANGE:` footer in the commit body, a `CHANGELOG.md` entry, and an explicit statement of whether the ergonomic façade surface is affected or only the low-level `magnetar-proto` API (re-exported as `magnetar::proto`).
See [ADR-0086](../specs/adr/0086-inject-now-into-proto-latency-recording.md) for a worked example.

---

## Index

Status tags: ⚡ ready to dispatch · 🔗 blocked on external dep · ⏳ blocked on upstream PIP release · 🧠 needs design decision · 🟡 deferred (not load-bearing).

| #   | Item                                                                                                                  | Status                                                                                               |
| --- | --------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| 1   | [PIP-460 scalable-topics e2e](#1-pip-460-scalable-topics-e2e)                                                         | ⏳ scaffold in place; stub bodies trivially pass; flesh out once a Pulsar 5.0 RC carries PIP-460     |
| 8   | [Broker-URL authority parser unification — residual](#8-broker-url-authority-parser-unification--residual)            | 🟡 deferred (three of four parsers unified; `parse_direct_broker_url` audited, not unified)          |
| 10  | [`check-sim-coverage` scope closed — enforcement residual](#10-check-sim-coverage-scope-closed--enforcement-residual) | ⚡ report now covers six crates; uncovered lines still only print, and the gate has never run per-PR |

---

## 1. PIP-460 scalable-topics e2e

**Gap.** The PIP-460 scalable-topics surface scaffold is in place across proto / façade / both engines / CLI with the binding 4-layer in-process tests (proto unit + tokio + moonpool 1:1 + differential + golden trace), behind `feature = "scalable-topics"` (default off, [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md)).
The **e2e** tests in `crates/magnetar/tests/e2e_scalable_topic.rs` have stub bodies that touch a constant and return — per [ADR-0046](../specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) they run on every `cargo test --features scalable-topics` and trivially pass.
Three named tests are wired but un-fleshed; no released broker speaks PIP-460.

**Why it stays open.** Upstream PIP-460 is `Draft`, targeting Pulsar 5.0 LTS with phased rollout.
The wire surface is hand-encoded in `crates/magnetar-proto/src/pb/scalable_topics.rs` until a real RC ships.

**`/goal` (once a Pulsar 5.0 RC carries PIP-460).**

```text
/goal flesh out the PIP-460 e2e per docs/follow-ups.md §1 once upstream cuts a Pulsar 5.0 RC carrying PIP-460. First, as a dedicated commit per ADR-0026 §D4, run `cargo run -p xtask -- vendor-proto --rev <pulsar-5.0-rc-sha>` to replace the hand-encoded crates/magnetar-proto/src/pb/scalable_topics.rs module and reconcile field numbers against the vendored proto. Then implement the bodies of the three stub tests in crates/magnetar/tests/e2e_scalable_topic.rs against a real broker spawned via testcontainers-rs (file is gated `feature = "scalable-topics"` per ADR-0046; no `#[ignore]`, no `feature = "e2e"`). Validation chain per CLAUDE.md.
```

---

## 8. Broker-URL authority parser unification — residual

**Closed.** The three parsers that re-implemented `magnetar_proto::probe_authority`'s rule arm-for-arm now delegate to it: `proxy_broker_authority` / `direct_broker_authority` (`crates/magnetar-runtime-moonpool/src/client.rs`) and `strip_url_to_host_port` (`crates/magnetar-runtime-moonpool/src/driver.rs`, which gates its mandatory scheme and its `?` / `#` trim locally, then delegates, so it keeps its stricter contract without keeping a copy of the rule).
That also closed the shared port-less bracketed-IPv6 gap and an empty-authority hole in `proxy_broker_authority` that let `""` and `"pulsar://"` reach `CommandConnect.proxy_to_broker_url` as `""` and `":6650"`.
See [ADR-0087](../specs/adr/0087-unify-broker-url-authority-parsers.md) for the post-implementation reference.

**Remaining.** `magnetar_runtime_tokio::client::parse_direct_broker_url` is a fifth application of the same rule, and stays independent.
It parses via the `url` crate into `ParsedUrl { host, port }` rather than producing a `host:port` string, so folding it in would mean either giving up the struct return or wrapping `probe_authority` and re-splitting its output — trading a real seam for a cosmetic one.

It is **audited rather than unified**: `parse_direct_broker_url_agrees_with_probe_authority` is a table-driven test pinning, row by row, where the two agree and where they deliberately diverge (a scheme-less input takes the _bootstrap_ scheme's default port here but passes through port-less in `probe_authority`; a malformed bracket like `pulsar://[::1` is rejected by `url` but returned verbatim by `probe_authority`).
So a divergence can still be introduced, but not without editing a table that states the rationale.

One cosmetic residual inside it: its rejection message says an input "carries an unrecognised scheme" even for `"pulsar://"`, whose actual fault is the missing authority — the same imprecision ADR-0087 fixed on the moonpool side.
Left alone deliberately, since changing it is a user-visible text change with no correctness content.

**Why the residual stays open.** No behavioural bug and no drift that a test cannot see; closing it is an API-shape question (does the DIRECT path want an authority string or a parsed struct?) rather than a correctness one.

**Site inventory**, kept so a future unifier does not merge parsers that are deliberately different:

| Site                                                                                           | Contract                                                                              | Status                                                                                                                                            |
| ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `magnetar_proto::probe_authority`                                                              | scheme optional; bare `host:port` accepted; default port synthesised                  | canonical — the single implementation                                                                                                             |
| `magnetar-runtime-moonpool/src/client.rs` `proxy_broker_authority` / `direct_broker_authority` | same rules, `Result<_, ClientError>`                                                  | **unified** (ADR-0087) — delegates, maps `None` to one `ClientError::Other`                                                                       |
| `magnetar-runtime-moonpool/src/driver.rs` `strip_url_to_host_port`                             | scheme **required** (a bare `host:port` returns `None`); also trims `?` / `#`         | **unified** (ADR-0087) — gates its stricter contract locally, then delegates                                                                      |
| `magnetar-proto/src/conn_types.rs` `extract_pulsar_host`                                       | returns the **host only**, no port; IPv6-bracket carve-out                            | **no** — different job (allow-list host matching, ADR-0044 redirect gate)                                                                         |
| `magnetar-runtime-tokio/src/client.rs` `parse_direct_broker_url`                               | returns `ParsedUrl { host, port }`, not an authority string; `Result<_, ClientError>` | **audited, not unified** — this residual; equivalence pinned by `parse_direct_broker_url_agrees_with_probe_authority` rather than by construction |

---

## 10. `check-sim-coverage` scope closed — enforcement residual

**Closed (the report scope).** `cargo run -p xtask -- check-sim-coverage` no longer reports on two crates only.
It now runs two steps: execution is unchanged — `cargo llvm-cov -p magnetar-runtime-moonpool -p magnetar-differential --all-features --locked`, so only those two crates' test binaries ever run — and a new second step re-exports the _same_ profile data and object files with `cargo llvm-cov report`, one `-p` per entry of `SIM_COVERAGE_REPORT_PACKAGES` (`xtask/src/main.rs`) plus `--ignore-filename-regex 'crates/magnetar-proto/src/pb/'`.
That list is the six crates the sim run actually compiles, derived from measurement and corroborated by reading the `--extern` flags on the rustc invocations for the moonpool / differential test targets.
Measured 2026-07-31 on `fix/sim-coverage-scope`, `target/sim-coverage.lcov` went from the 16 `SF:` records ADR-0088 recorded on 2026-07-30 to 63:

```
rg -o '^SF:.*' target/sim-coverage.lcov | sed 's|^SF:.*/crates/||' | cut -d/ -f1 | sort | uniq -c
      5 magnetar-auth-athenz
      2 magnetar-auth-sasl
      4 magnetar-differential
     28 magnetar-proto
     12 magnetar-runtime-moonpool
     12 magnetar-runtime-tokio
```

`magnetar-proto` — the crate invariant #9 in [`CLAUDE.md`](../CLAUDE.md) singles out — is in the report for the first time, and so is `magnetar-runtime-tokio`, deliberately: it is a regular dependency of `magnetar-differential` (`crates/magnetar-differential/Cargo.toml`), so the equivalence suite drives it, and [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md) already requires a differential test for every behavioural change.
`magnetar-admin`, `magnetar-auth-oauth2`, `magnetar-fakes` and `magnetar-messagecrypto` were tried in the `-p` list and emit zero records — they are not linked into the sim binaries — so they are not listed as gated.
The `magnetar` façade stays out for the same reason: nothing in the sim closure depends on it, so step 1 never compiles it and its 58 Docker-bound `crates/magnetar/tests/e2e_*.rs` ([ADR-0046](../specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md): no `#[ignore]`, no feature gate) never enter the coverage run.
Façade additions keep printing `not gated (outside the sim-coverage report)` and exiting 0.
See [ADR-0090](../specs/adr/0090-widen-sim-coverage-report-to-compiled-closure.md) for the post-implementation reference.

**The premise this entry was written on was wrong.** It said fixing the scope was "a rework of the gate's mechanics, not a flag change", and priced the stitched-report option at "a longer run".
Both are false, and the correction is worth carrying because it changes how a similar widening should be sized: the change cost **zero** recompilation.
`cargo-llvm-cov`'s `RUSTC_WRAPPER` instruments every workspace member regardless of `-p` (`cargo-llvm-cov-0.8.7`, `src/wrapper.rs:63-83`); `-p` only selects which test binaries run, what gets cleaned, and the `--ignore-filename-regex` handed to `llvm-cov export` (`src/report.rs:869-986`).
`magnetar-proto`'s counters were always in the profile data — only the report filter hid them.

**Remaining (the enforcement half).** `const SIM_COVERAGE_ENFORCES_UNCOVERED: bool = false`.
Uncovered added lines are printed in full with a count, and the check **exits 0**; the new `--enforce` flag restores the failing exit code per invocation, which is how the fail path stays exercised.
So while that constant is false, a green `check-sim-coverage` is **not** evidence of ADR-0024 patch coverage — it is evidence that the gate ran and printed its findings.
That is the same shape of over-claim [ADR-0088](../specs/adr/0088-sim-coverage-gate-scope-report-ungated-additions.md) was written to stop, accepted temporarily and with eyes open rather than overlooked.
One thing does hard-fail regardless of the advisory setting: a file whose whole gated crate emitted no records at all, which signals a broken or misconfigured gate rather than a missing test.

The backlog, measured 2026-07-31 by replaying real history through the widened gate:

| Diff base                                                        | Uncovered added lines | Files                                             | Where they sit                                                                                                                                                                           |
| ---------------------------------------------------------------- | --------------------- | ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--base HEAD~10` — roughly the last ten merged PRs               | 6                     | 4                                                 | `magnetar-proto/src/conn.rs` (284, 1528), `magnetar-proto/src/health_probe.rs` (204), `magnetar-runtime-tokio/src/client.rs` (2036), `magnetar-runtime-tokio/src/consumer.rs` (228, 231) |
| `--base HEAD~25` — back to 2026-07-01, six commits past `v1.2.0` | 450                   | 15, plus 20 more on the advisory `not gated` path | dominated by `magnetar-runtime-tokio/src/client.rs` (191) and `magnetar-runtime-tokio/src/consumer.rs` (61), then `magnetar-proto/src/conn.rs` (142)                                     |

The 450 is an artifact of diffing against a 25-commit-old base, which no ordinary workflow does.
Because the gate is a _patch_ gate against `git merge-base origin/main HEAD`, that backlog is never charged to a future PR: each PR is measured only on its own added lines.

**Second blocker, newly measured: the gate has never run per-PR at all.** [`.github/workflows/xtask-gates.yml`](../.github/workflows/xtask-gates.yml) runs `check-sim-coverage` on a daily cron and on `workflow_dispatch` only, and [`ci.yml`](../.github/workflows/ci.yml) carries no sim-coverage job.
Both scheduled runs target `main`, where `merge-base(origin/main, HEAD) == HEAD` makes the diff empty and the check short-circuits with "nothing to verify" before it builds anything.
That is also why `magnetar-runtime-moonpool` — gated since ADR-0024 — itself carries 43 uncovered added lines over `HEAD~25`.
Flipping `SIM_COVERAGE_ENFORCES_UNCOVERED` without also wiring the gate into per-PR CI would therefore change nothing in practice, so the two must land in one changeset.

**Why the residual stays open.** Not a design question any more — the scope, the gated crate list, and the advisory landing are all settled ([ADR-0090](../specs/adr/0090-widen-sim-coverage-report-to-compiled-closure.md)).
What is left is a cost call that wants a measurement first: an instrumented `--all-features` build of the sim closure (aws-lc-fips-sys included) has never been timed in CI on a branch whose diff touches a gated crate, and that number decides whether the per-PR job blocks merge or runs advisory-in-CI for a while.
Note also that execution scope is unchanged: `magnetar-proto`'s and `magnetar-runtime-tokio`'s own unit tests never run under this gate and can never satisfy it, so burning the backlog down means writing moonpool or differential tests that reach those lines.

**`/goal`.**

```text
/goal make check-sim-coverage enforce ADR-0024 instead of merely reporting it, per docs/follow-ups.md §10. Two things must land in the SAME changeset or neither is worth anything: flip `SIM_COVERAGE_ENFORCES_UNCOVERED` from false to true in xtask/src/main.rs, and give the gate a per-PR home. Today .github/workflows/xtask-gates.yml runs it on a daily cron plus workflow_dispatch only, and .github/workflows/ci.yml has no sim-coverage job at all; because it is a patch gate against `git merge-base origin/main HEAD`, the scheduled `main` run short-circuits with "nothing to verify", so the gate has never measured a real PR. Start with CI: add a check-sim-coverage job on `pull_request`, copying the `sim-coverage` job out of xtask-gates.yml step for step (fetch-depth 0 plus the explicit `git fetch origin main`, the free-disk-space step, `clang llvm libclang-dev libkrb5-dev`, cargo-llvm-cov), then measure its wall clock on a branch that touches a gated crate before deciding whether it blocks merge — it budgets 90 minutes today against 180 for the comparable `--all-features` jobs. Then flip the constant, keeping `--enforce` as the per-invocation override so the fail path stays exercised, and prove the flip red-then-green: add a deliberately-uncovered line under crates/magnetar-proto/src/, show the gate exits non-zero on it, remove it, show it passes. Size the burn-down first — measured 2026-07-31, `--base HEAD~10` reports 6 uncovered added lines across 4 files and `--base HEAD~25` reports 450 across 15, and the second number is an artifact of a 25-commit-old base that is never charged to a future PR since each PR is measured on its own added lines only. Covering any of them means writing a moonpool or differential test: this gate never runs magnetar-proto's or magnetar-runtime-tokio's own unit tests, so a line there counts only when a sim or equivalence test reaches it. Do not edit ADR-0090 in place (CLAUDE.md) — record the flip in a new ADR that supersedes its advisory decision, and update CLAUDE.md invariant #9, the SIM_COVERAGE_ENFORCES_UNCOVERED doc comment, the xtask-gates.yml header note, and this entry in the same commit. Validation chain per CLAUDE.md, and note that check-sim-coverage is only runnable on Linux because the gate now pins CC/CXX/ASM to clang for aws-lc-fips-sys's delocate pass.
```

---

## Notes on this file

Items move from this file to `git log` when their commit ships.
The expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays open** + (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the post-implementation reference); partially-closed items are trimmed to their remaining residual.

§1 is a fully external blocker (the PIP-460 e2e flesh-out waits on a Pulsar 5.0 RC carrying PIP-460); §8 is trimmed to one audited-not-unified parser whose closure is an API-shape question; §10 is trimmed to its enforcement half — the report scope landed ([ADR-0090](../specs/adr/0090-widen-sim-coverage-report-to-compiled-closure.md)), but uncovered added lines still only print, and the gate has never run per-PR. §10 is the only dispatch-ready item.
Numbering is stable, not contiguous: closed items are removed and their number is retired rather than reused, so a `§N` reference in a commit, ADR, or code comment keeps pointing at the same item forever.
