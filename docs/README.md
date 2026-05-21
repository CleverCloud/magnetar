# Magnetar — Documentation

This folder collects the long-form documentation behind the magnetar workspace.
It is the durable home of material that used to live in `~/.claude/plans/` and
`tasks/`. The top-level `README.md`, `ARCHITECTURE.md`, `GUIDELINES.md`,
`CONTRIBUTING.md`, and `CLAUDE.md` files stay where they are — those serve a
different audience (end users, contributors, Claude) and are linked from here.

## Layout

| File | Purpose | Source |
| --- | --- | --- |
| [`implementation-plan.md`](implementation-plan.md) | The full M0 → M9 plan: phasing, milestones, validation gates, risk table. **The canonical "what are we building?" document**. | Promoted from `tasks/todo.md`. |
| [`decisions-log.md`](decisions-log.md) | Florentin's signed-off decisions on the audit questions: project name, license, no-channels, PIP scope, etc. **Binding when it disagrees with the plan**. See also the [ADR series](../specs/adr/) which atomises each decision into its own file. | Promoted from `tasks/decisions.md`. |
| [`parity-status.md`](parity-status.md) | Java client parity snapshot — what's landed, what's open, recent commits. Refreshed periodically. | Promoted from `tasks/parity-status.md`. |
| [`research.md`](research.md) | Research dossier consulted before the plan: Pulsar PIPs, moonpool maturity, crate-name landscape, prior-art comparison (`pulsar-rs`, `apache/pulsar-client-cpp`, …). | Copied from `~/.claude/plans/ask-magnetar-research.md`. |
| [`review.md`](review.md) | Reviewer report — first independent review of the plan + research. | Copied from `~/.claude/plans/ask-magnetar-review.md`. |
| [`audit.md`](audit.md) | Auditor verdict — risk register + the 12 open questions that became `decisions-log.md`. | Copied from `~/.claude/plans/ask-magnetar-audit.md`. |
| [`codex-cross-check.md`](codex-cross-check.md) | Codex cross-check — independent second-opinion review (called out specific protocol-correctness invariants). | Copied from `~/.claude/plans/ask-magnetar-codex.md`. |
| [`swarm-history.md`](swarm-history.md) | Snapshot of one parallel-implementer swarm run — kept as an example of the orchestration pattern. Not authoritative; meant as a record of how 4–6 agents were dispatched + tracked. | Promoted from `tasks/swarm-status.md`. |

## Companion documents (top-level)

| File | Purpose |
| --- | --- |
| [`../README.md`](../README.md) | Public-facing project README + Java parity matrix + supported PIPs. |
| [`../ARCHITECTURE.md`](../ARCHITECTURE.md) | Architectural deep dive: sans-io rationale, driver loop, protocol state machine, schema canonicalisation, trackers. |
| [`../GUIDELINES.md`](../GUIDELINES.md) | **Binding** project conventions: no-channels rule, I/O isolation, TLS, worktree workflow, commit hygiene, validation chain. |
| [`../CONTRIBUTING.md`](../CONTRIBUTING.md) | Toolchain, commit hygiene, branch naming, dependency-allow-list pointer. |
| [`../CLAUDE.md`](../CLAUDE.md) | Claude-facing project memory: workspace layout, invariants, validation chain, slash workflows, reading order. |
| [`../specs/`](../specs/) | Architecture Decision Records — atomised, one decision per file, stable identifiers. |

## How to update

These documents are not auto-generated — when a decision changes or a milestone
ships, edit the relevant file in the **same** changeset that lands the code.
`GUIDELINES.md` calls this "docs are code" — stale docs are bugs.

Specifically:

- A new PIP or Java-parity feature → update `parity-status.md` AND the
  parity matrix in `README.md` in the same commit.
- An architectural decision overruling something in the plan → add a new
  numbered file in `specs/adr/` AND append a one-line entry to
  `decisions-log.md` AND update the relevant section of
  `implementation-plan.md`.
- A risk that materialised or got mitigated → update the risk register in
  `implementation-plan.md` (and in `audit.md` if it was tracked there).

## Why a `docs/` folder at all?

These files used to live under `tasks/` (auto-loaded by the `ask:planner` skill)
and `~/.claude/plans/` (Claude-local). Both are out of band — neither shows up
on `github.com/FlorentinDUBOIS/magnetar`, neither is grep-able by a contributor
who cloned the repo, neither survives a fresh checkout. Promoting them to
`docs/` (committed to the tree) makes the design history first-class.
