# CI failure monitor — `/loop` pattern

This is an **operational note**, not a hook or a check-in script. It
documents the agreed-upon pattern for using Claude Code's `/loop` skill
(see the global skill matrix in `~/.claude/CLAUDE.md`) to keep a light
eye on GitHub Actions after Phase 4 lands.

## Pattern

```text
/loop 1h "check CI status"
```

When invoked, the loop agent should:

1. Run `gh run list --workflow=ci.yml --limit 5 --json status,conclusion,headBranch,headSha,createdAt,displayTitle`
   and inspect the result.
2. For any run with `conclusion in {failure, cancelled, timed_out}`:
   - identify the failing job(s) via
     `gh run view <run-id> --json jobs --jq '.jobs[] | select(.conclusion != "success")'`
   - pull the failing job's log tail
     (`gh run view <run-id> --log-failed | tail -200`).
3. If the failure is on `main`, surface it immediately — that is a
   bisect-worthy red-line event.
4. If the failure is on a PR branch, note the PR number and the
   smallest reproduction step you can find from the log tail. Do not
   auto-push fixes; report and wait.
5. If everything is green, return a single-line summary and re-arm.

## Schedule

The actual `/loop` invocation is **not committed** anywhere — it lives
in the operator's session. Florentin will arm it on demand. The
expected cadence is once per hour during active development, off on
weekends and during freeze windows.

## What this is not

- It is **not** a GitHub Actions workflow that polls itself; CI cannot
  observe its own runs without a feedback loop and rate-limit risk.
- It is **not** a substitute for the per-job `timeout-minutes` budgets
  already in `.github/workflows/ci.yml`.
- It does **not** push to remote, comment on PRs, or close issues.
  Read-only triage only.

## Manual fallback

If `/loop` is not running, the same triage flow can be invoked ad-hoc:

```bash
gh run list --workflow=ci.yml --limit 10
gh run view <run-id> --log-failed | less
```

## See also

- `.github/workflows/ci.yml` — the CI surface being monitored.
- `.github/dependabot.yml` — weekly bump source; expect a flurry of
  PR-triggered runs each Monday.
- `~/.claude/CLAUDE.md` § "Skills" — `/loop` is a Tier 1 recurring-work
  skill.
