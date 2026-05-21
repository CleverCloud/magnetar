# Audit — Magnetar v0.1.0 finish-line plan

Final-gate pass over `tasks/todo.md` (the plan) and `docs/review.md`
(the reviewer's report), against the three researcher dossiers
(`docs/research-worktrees.md`, `docs/research-parity-simulator.md`,
`docs/research-e2e-ci.md`), live source at HEAD `37d3c3e`, and the
binding rule set in `GUIDELINES.md`, `CLAUDE.md`, `specs/adr/*`.

Date: 2026-05-21.

---

## A. Final verdict

**APPROVED with required fixes already folded in.** The plan + reviewer's
report together describe a coherent, citation-backed, scope-appropriate
finish-line for v0.1.0 plus simulator parity, e2e expansion, and CI
hardening. The reviewer's 2 CRITICAL findings (C1, C2) and 3 MAJOR
findings (M1, M2, M3) were applied directly to `tasks/todo.md` and are
verified below. Five MINOR notes are folded in as in-line comments;
none change the plan shape. Six items the reviewer called OK are
preserved untouched.

The plan is **ready for user approval** on the approval-gate checklist
in §C. Phase 0 can start once gate (a) and per-WIP gates (b1..b4)
resolve.

---

## B. Reviewer findings — disposition

| ID | Reviewer finding | Resolution | Location in `tasks/todo.md` |
|---|---|---|---|
| C1 | Phase 1.3 targets non-existent `crates/magnetar-proto/src/client/memory_limit.rs` | **APPLIED** — corrected to `crates/magnetar-proto/src/conn.rs` (ConnectionShared.memory_limit_bytes + memory_used per ADR-0017) | lines 260–299 |
| C2 | Phase 0b W6 (`feat/auto-schema-runtime-wire`, ahead=1) is byte-equivalent to landed `010e252` — drop, do not cherry-pick | **APPLIED** — W6 marked SAFE-TO-DROP; treatment folded into 0a with a one-shot diff-confirmation step kept for traceability | line 159 |
| M1 | M6 engine-trait Option A/B underestimates `<E>` blast radius | **APPLIED** — Option C (feature-gated façade alias) added and marked RECOMMENDED; gate (e) picks A/B/C | lines 393–410, 881 |
| M2 | Phase 2 M7 chaos pack overlaps Phase 3 Batch C on 6 of 8 cases; testcontainers cannot inject mid-frame partition / virtual-clock OAuth2 expiry | **APPLIED** — sub-tests 1c + 2c removed from Batch C; explicit note that those belong to M7 only | lines 617–639 |
| M3 | Locked `.claude/worktrees/agent-*` cleanup ambiguous between `wt remove` and `git worktree remove --force` | **APPLIED** — `wt drop --force` is primary, `git worktree remove --force` is fallback; `lsof +D <path>` safety gate before any force-removal | lines 91, 98–103 |

MINOR fixes folded as in-line comments (not approval-blocking):

- README parity-matrix has two contradictory `Producer::stats` rows
  (`README.md:418` says rolling windows still pending; `:447` says they
  ship). Doc-only resolution in Phase 1.2.
- `memoryLimit` row at `README.md:611` mentions `FailImmediately` only —
  `ProducerBlock` is the polish follow-up gated on (d).
- `rust-toolchain.toml` and `Cargo.toml` have no MSRV pin today; Phase 4
  splits into manifest pin + CI job.
- Slack-webhook CI notifier reframed as Option A (`/loop` agent,
  default) vs Option B (Slack webhook, opt-in). Gate (g).
- `tlsAllowInsecureConnection` wording at `README.md:606` is intentional
  — no change.

---

## C. Approval-gate checklist (user input, in order)

Gates the user MUST clear **before any swarm can start**:

| Gate | Decision | Blocks |
|---|---|---|
| (a) | Bulk drop of ~28 worktrees that are subsets of main (Phase 0a) | All Phase 0 + downstream |
| (b1) | Finish + merge `agent-aa655e6a5c1167e82` (feat/partitioned-auto-update-tickers, +538/-2) | Phase 0b complete |
| (b2) | Finish + merge `agent-aa5f3f8c161e60829` (test/e2e-compacted-tableview, untracked `tests/e2e_compacted.rs`) | Phase 0b complete |
| (b3) | Supersede-or-drop the two `service_url.rs` WIPs (likely superseded by PIP-121 `7b8d3e6`) after diff-check | Phase 0b complete |
| (b4) | Finish + merge `agent-a842215fabac3e8ea` (+200/-71 on `getProducerAccessMode`); verify whether main already covers the row | Phase 0b complete |
| (c) | ADR-0019 — "v0.1.0 parity = tokio-engine satisfied; moonpool parity = M6 follow-up train" | All Phase 2 milestones |
| (h) | **User-only**: raise GitHub Actions spending limit so CI signal returns | Phase 4 monitoring is meaningful |

Gates the user clears **later, when their phase starts**:

| Gate | Decision | Phase |
|---|---|---|
| (d) | Scope-add `MemoryLimitPolicy::ProducerBlock` for v0.1.0 or punt to v0.1.1 | 1.3 |
| (e) | M6 engine shape — Option A (user-visible `<E>`), B (duplicate façade), or **C** (feature-gated alias, recommended) | 2 M6 |
| (f) | TLS-fronted broker container + wiremock IDP as dev-deps | 3 Batch D |
| (g) | CI notifier — `/loop` agent (A, recommended) vs Slack webhook (B) | 4 |
| (i) | Each `git push` to `origin/main` (per-merge confirmation per ADR-0013) | every phase |

---

## D. Open questions only the user can answer

1. **Engine surface (gate e)** — A / B / **C**? Planner + auditor
   recommend **C**: public `PulsarClient` stays non-generic, only one
   engine active per build, internal `<E>` only inside the workspace.
2. **`MemoryLimitPolicy::ProducerBlock` (gate d)** — in scope for
   v0.1.0 or punt? The sans-io extension is small (`WakerSlab` on
   `ConnectionShared`) but it does change the proto-side surface.
3. **CI notifier (gate g)** — `/loop` agent (in-Claude) or Slack
   webhook (out-of-Claude)? `/loop` does not commit to a Slack-app
   deployment.
4. **Per-WIP terminations (gates b1..b4)** — confirm each merge
   individually. b3 may be a no-op once diffed against main, but you
   still own the call.

Nothing else is genuinely open. Everything inside the swarm is
mechanical once gates (a) and (b1..b4) clear.

---

## E. Standards / RFC / guideline compliance

| Concern | Verdict | Justification |
|---|---|---|
| Sans-io invariants (ADR-0003 no-channels, ADR-0004 zero I/O in `magnetar-proto`, ADR-0011 clock injection) | **PASS** | Phase 1.3 adds a `WakerSlab` to `ConnectionShared` (no Notify, no I/O). Phase 2 M5 ports only adapter code into `magnetar-runtime-moonpool/src/*`. Nothing in the plan reaches `Instant::now()` or `SystemTime::now()` inside `magnetar-proto/`. |
| `rustls` only (ADR-0005) | **PASS** | Phase 3 Batch D's TLS-fronted broker is a test fixture; in-driver TLS stays `tokio-rustls` (tokio) + rustls-over-bytepipe (moonpool). |
| No "Generated by Claude" trailers (ADR-0012) | **PASS** | Plan reiterates the rule per-commit. |
| Worktree-first (ADR-0013) | **PASS** | Every code-touching agent is dispatched into `wt switch --create … -y`; every merge is `wt merge -y` after gate (i). |
| GPG sign-off + conventional commits | **PASS** | Plan reiterates `git commit -s -S -m "<type>(<scope>): <subject>"`. |
| Validation chain | **PASS** | Each phase calls out `cargo build / clippy / +nightly fmt / test / deny check / xtask check-no-channels / check-no-io-deps / check-no-internal-clock / codegen --check`, plus `RUSTDOCFLAGS=... cargo doc`. |
| ADR co-location with the changeset that lands the decision | **PASS** | ADR-0019 lands in Phase 1.1; ADR-0020 in Phase 1.3 (gated); ADR-0021 in Phase 4 (gated). Each is referenced by filename. |
| `specs/README.md` index updated | **TRACKED** | Phase 5 updates the index per landed ADR. |

---

## F. Documentation work — full follow-up list

| File | Owner phase | Action |
|---|---|---|
| `specs/adr/0019-engine-scope-and-moonpool-parity.md` | 1.1 | new |
| `specs/adr/0020-memory-limit-producer-block.md` (gate d) | 1.3 | new |
| `specs/adr/0021-codegen-check-in-ci.md` (optional) | 4 | new |
| `specs/README.md` | 5 | append new ADRs to index |
| `docs/parity-status.md` | 5 | refresh snapshot date + "recently landed" |
| `README.md` parity matrix | 1.2 | reconcile `Producer::stats` rows; clarify `memoryLimit` row |
| `docs/implementation-plan.md` | 5 | mark M9 deferrals against landed v0.1.0 |
| `ARCHITECTURE.md` | 2 M5/M6 | moonpool-engine surface notes; engine-trait shape if Option B/C lands |
| `tasks/todo.md` | this audit pass | already edited |
| `~/.claude/plans/ask-magnetar-finish-plan.md` | 6 (presentation) | stable copy |

---

## G. Gaps not caught by researchers / reviewer

None material. Two small forward-looking notes:

1. The `.claude/worktrees/agent-*` directory carries a footprint the
   user may want to reset entirely after Phase 0 — not in scope, but
   worth a follow-up `chore: reset .claude/worktrees`.
2. The auto-update tickers added across Pattern / Partitioned /
   MultiTopics / TableView (commit `f09f23c`) are wired such that
   `Instant::now()` lives at the runtime layer — not in `magnetar-proto`.
   Worth a one-line note in `ARCHITECTURE.md` (Phase 5) so future
   contributors do not push them into `magnetar-proto`.

---

## H. Closing

Approved. Surface §C (gates), §D (open questions), and §F (doc
follow-up) to the user in Phase 6 (presentation). Codex cross-check
runs next.
