# ADR-0013 — Worktree-first development via `wt`

- **Status**: Accepted
- **Date**: 2026-05-20
- **Decider**: Florentin Dubois
- **Tags**: process, git, worktree

## Context

After the initial `M0` commit lands on `main`, every subsequent change goes through a branch.
Naively, contributors would `git checkout -b feat/xxx`, edit, push, open a PR.
Two problems:

- The `main` working tree is a "clean room" — we want to keep it browsable for incidental reads (grep, git log) without it being littered with WIP changes.
- Multiple in-flight features need physical isolation when agents are running in parallel (the supervised swarm pattern).
  A single working tree cannot host two simultaneous worktree-agent-\* worktrees plus an uncommitted edit by the user.

[`worktrunk`](https://github.com/clever-cloud/worktrunk) (`wt`) is a thin wrapper around `git worktree` that makes the per-feature worktree pattern ergonomic.

## Decision

**Default behaviour**: every code-modifying change in a git repository goes through a `wt`-managed worktree.

Workflow:

```
wt switch --create feat/<scope> -y      # isolated worktree off main
# edit
wt step diff -- --stat
# (user reviews)
wt merge -y                              # after Florentin confirms
```

The `~/.claude/hooks/pre-edit-default-branch.sh` hook **blocks** direct edits on `main`/`master`/`trunk`/`develop`.
Trying to `Edit` a file there returns an error pointing to `wt`.

Exempt (no worktree needed):

- Non-git directories (e.g., `~/.claude/`, `/tmp/`).
- Trivial single-file config edits explicitly authorised by the user.

Branch naming follows conventional-commit types: `feat/<scope>`, `fix/<scope>`, `refactor/<scope>`, `chore/<scope>`, `docs/<scope>`, `test/<scope>`.

## Consequences

- Parallel agent swarms get conflict-free physical isolation (each agent works in its own `.claude/worktrees/agent-*` worktree).
- The `main` working tree always reflects merged state — reviewers can trust `cd main; git log` without surprises.
- The first action on any non-trivial task is `wt switch --create`.
  Skipping this hits the pre-edit hook.
- `wt merge -y` is approval-gated by the global engineering rules — the user confirms before each merge to `main`.

## References

- `~/.claude/CLAUDE.md` §"Worktree-First Development" (global rule)
- [`CLAUDE.md` §"Workflow"](../../CLAUDE.md) (project reiteration)
- [`GUIDELINES.md` §"Worktree workflow"](../../GUIDELINES.md)
- `~/.claude/hooks/pre-edit-default-branch.sh` (enforcement)
