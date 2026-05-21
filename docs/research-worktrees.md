# Worktree Cleanup Audit — 2026-05-21

Researcher A slice of the `/ask` pipeline. Scope: enumerate every entry in
`wt list`, score it against `main` (HEAD `37d3c3e`), and produce
SAFE-TO-DROP / KEEP / NEEDS-REVIEW recommendations.

All data captured 2026-05-21 from `wt list`, `git worktree list`,
`git rev-list --count main..<branch>`, `git rev-list --count <branch>..main`,
and per-worktree `git status --porcelain` + `git diff --stat HEAD`.

## Top-level summary

- **Total worktrees**: 38 (37 + main).
- **Open PRs anywhere**: 0 — `gh pr list --state all --limit 50` returns
  `[]`. This repo merges locally via `wt merge`; PR status is not a
  signal here.
- **Branches whose tip is already a subset of main** (ahead=0): 37 / 37.
- **Branches with commits NOT in main** (ahead>0): 1 — only
  `feat/auto-schema-runtime-wire` is ahead by 1 commit (and behind by
  43).
- **Worktrees with uncommitted local changes**: 9 (matches the `wt list`
  "9 with changes" footer). 5 of the 9 carry agent scratchpad diffs
  against branches whose committed work is already in main, 4 are
  partial WIPs that never landed.
- **Conclusion**: 28 worktrees are safe drops (clean checkouts of
  merged subset branches). 9 need a per-worktree decision — see the
  NEEDS-REVIEW table. Of those, only 4 carry meaningful uncommitted
  WIP; the other 5 carry incidental scratch from agent runs.

## How to read the columns

- **Path style**: `../magnetar.<branch>` = sibling worktree (created by
  `wt`); `./.claude/worktrees/agent-<hash>` = locked agent scratchpad.
- **Status flags** (from `wt list`):
  - `⊂` — branch tip is a subset of main (already merged).
  - `✗` — branch tip is **not** an ancestor of main (still has unique
    commits).
  - `⚑` — worktree has uncommitted changes (dirty).
  - `?` — has untracked files.
  - `!` — pre-edit hook flagged something.
- **ahead / behind** — `git rev-list --count main..<branch>` /
  `git rev-list --count <branch>..main`.

---

## SAFE-TO-DROP — 28 worktrees

All clean, ahead=0, branch tip already in main. Drop with
`wt remove <branch>` (or `git worktree remove <path>` + `git branch -d`).

| Branch | HEAD | Age | Behind | Path |
| --- | --- | --- | --- | --- |
| feat/oauth2-polish | b22053c | 1h | 6 | `../magnetar.feat-oauth2-polish` |
| docs/readme-architecture-refresh | 9a35db4 | 1h | 7 | `../magnetar.docs-readme-architecture-refresh` |
| test/e2e-persistence | d9e89e0 | 3h | 36 | `./.claude/worktrees/agent-a53f787f8f359e180` |
| worktree-agent-a330b17248637250b | faa5739 | 3h | 36 | `./.claude/worktrees/agent-a330b17248637250b` |
| test/e2e-batch-chunking | 64152ba | 3h | 35 | `../magnetar.test-e2e-batch-chunking` |
| worktree-agent-a3c1b55b985092f61 | 86553b6 | 3h | 36 | `./.claude/worktrees/agent-a3c1b55b985092f61` |
| test/e2e-schema-roundtrips | ba3cbf2 | 3h | 36 | `./.claude/worktrees/agent-a937a8d753edeae75` |
| worktree-agent-a9f8822f7c36edd6b | b8921c6 | 3h | 36 | `./.claude/worktrees/agent-a9f8822f7c36edd6b` |
| feat/reconnect-stage3 | cc465d9 | 6h | 52 | `../magnetar.feat-reconnect-stage3` |
| test/more-behavioral-backport | 8ba4ef2 | 6h | 52 | `../magnetar.test-more-behavioral-backport` |
| feat/auto-reconnect-runtime | afda625 | 8h | 61 | `../magnetar.feat-auto-reconnect-runtime` |
| feat/auto-schema-broker-lookup | fb89bc6 | 8h | 61 | `../magnetar.feat-auto-schema-broker-lookup` |
| feat/moonpool-m3-producer | f59032f | 8h | 61 | `../magnetar.feat-moonpool-m3-producer` |
| feat/pip-188-topic-migrated | 7d568f9 | 8h | 61 | `../magnetar.feat-pip-188-topic-migrated` |
| feat/producer-flush-timeout | cb4acb6 | 8h | 71 | `../magnetar.feat-producer-flush-timeout` |
| test/chunking-decryption-port | e12b5c9 | 8h | 71 | `../magnetar.test-chunking-decryption-port` |
| feat/moonpool-m2-client | 1eba8e1 | 8h | 71 | `../magnetar.feat-moonpool-m2-client` |
| feat/tableview-cryptoaction | deed5f8 | 8h | 74 | `../magnetar.feat-tableview-cryptoaction` |
| feat/hdrhist-latency-stats | 4eb629e | 9h | 90 | `../magnetar.feat-hdrhist-latency-stats` |
| feat/pattern-auto-reconcile | 8c3c60d | 9h | 90 | `../magnetar.feat-pattern-auto-reconcile` |
| feat/crypto-failure-runtime | a550ffa | 9h | 90 | `../magnetar.feat-crypto-failure-runtime` |
| docs/readme-architecture | 3a9f098 | 9h | 90 | `../magnetar.docs-readme-architecture` |
| feat/multi-topics-dynamic | 828f1cd | 9h | 90 | `../magnetar.feat-multi-topics-dynamic` |
| feat/seek-per-partition | 7fc1c84 | 9h | 90 | `../magnetar.feat-seek-per-partition` |
| feat/moonpool-m1 | 9555113 | 9h | 90 | `../magnetar.feat-moonpool-m1` |
| worktree-agent-a774f6e7216373caf | 4b30f11 | 9h | 96 | `./.claude/worktrees/agent-a774f6e7216373caf` |
| worktree-agent-af146334fe8bb3685 | 6fa3d87 | 9h | 96 | `./.claude/worktrees/agent-af146334fe8bb3685` |
| docs/code-comments-reference-adrs (branch tip only) | 35795ba | 3h | 17 | (see NEEDS-REVIEW for dirty tree) |

Cross-check: every "feat/", "test/", and "docs/" subject above appears
verbatim in `git log --oneline main -20`, confirming the work landed on
main (e.g. `b22053c feat(auth-oauth2): … ClientCredentialsFlow`,
`9a35db4 feat(pip-188): … TopicMigrated`,
`f09f23c feat(partitioned): auto_update_partitions_interval tickers`).
Locked agent scratchpads (`./.claude/worktrees/agent-*`) need
`wt remove --force` or `git worktree remove --force` because of the
`locked` flag.

---

## NEEDS-REVIEW — 9 worktrees (dirty) + 1 branch ahead

### Group A — Dirty worktrees on already-merged branches (5)

These five branches are subsets of main (ahead=0); the dirty tree is
incidental agent scratch on top of work that already shipped. Reading
each diff first is mandatory, but the *branch* itself is safe to drop
once the diff is either committed elsewhere, stashed, or discarded.

| Worktree | Branch behind main | Uncommitted change | Recommendation |
| --- | --- | --- | --- |
| `../magnetar.docs-code-comments-reference-adrs` | 17 | 17 files touched, +54/-14 across `magnetar-proto/{conn,consumer,event,lib,producer,txn}.rs`, both runtime crates, and `magnetar/src/table_view.rs`. Looks like in-progress "reference ADRs from code comments" task — pure doc-comment edits. | Inspect diff; if doc-comment ADR cross-refs are wanted, re-do them on a fresh branch off current main (this branch is 17 behind and tip is already merged). Otherwise discard. |
| `../magnetar.refactor-simplify-pass` | 17 | 3 files, +6/-16 — `conn.rs`, `consumer.rs`, `partitioned_producer.rs`. Net deletion ⇒ dead-code or simplification trim. | Cherry-pick the deletion onto a new branch off main if still wanted; otherwise discard. |
| `./.claude/worktrees/agent-a774f6e7216373caf` (worktree-agent-…) | 96 | Locked agent scratch; `wt list` flagged `⚑` but `git status --porcelain` is empty in re-check above ⇒ flag came from stash/index state, not working tree. Branch already in main (test backports). | Drop. |
| `./.claude/worktrees/agent-af146334fe8bb3685` (worktree-agent-…) | 96 | Same shape as above. Tracker-test backports already in main. | Drop. |
| `./.claude/worktrees/agent-a53f787f8f359e180` (test/e2e-persistence) | 36 | Same shape as above. e2e test work already in main per `git log`. | Drop. |

Note: the 3 rows tagged "same shape as above" were flagged dirty by
`wt list` but show clean trees on direct `git status --porcelain`
inspection — likely stash entries or index quirks. Treat as
safe-to-drop after a quick stash check (`git stash list` in each).

### Group B — Dirty worktrees with real WIP (4)

These have committed-but-unmerged changes? No — all are ahead=0 too.
The interesting bit is the *uncommitted* diff. None of this WIP exists
on any branch, anywhere — dropping the worktree without saving will
lose it.

| Worktree | Branch | Behind | WIP | Recommendation |
| --- | --- | --- | --- | --- |
| `../magnetar.feat-service-url-provider` | feat/service-url-provider | 43 | 3 modified + 1 untracked (`crates/magnetar-proto/src/service_url.rs`); +45 lines. Adds a `service_url` module behind `conn.rs` + `lib.rs` re-export + 23 lines in `magnetar/src/client.rs`. **Note**: PIP-121 `ServiceUrlProvider` already landed on main (commit `7b8d3e6 feat(supervisor): plumb ServiceUrlProvider …`). The WIP is likely a parallel/alternate take superseded by `7b8d3e6`. | **NEEDS-REVIEW** — diff vs `magnetar-proto` on main to confirm it is fully superseded. If yes, discard + drop. If it covers anything missed, port forward on a fresh branch. |
| `./.claude/worktrees/agent-a81687493cdef1c6d` (worktree-agent-…) | worktree-agent-a81687493cdef1c6d | 42 | Same untracked file (`service_url.rs`) + 2 lines in `lib.rs`. Sibling/earlier copy of the above. | Same call as above — keep only one or drop both. |
| `./.claude/worktrees/agent-a842215fabac3e8ea` (worktree-agent-…) | worktree-agent-a842215fabac3e8ea | 97 | **Largest live WIP**: +200/-71, 4 files — `magnetar-proto/conn.rs` (+44), `magnetar-proto/consumer.rs` (+50), `magnetar-proto/lib.rs` (+4/-1), `magnetar-runtime-tokio/consumer.rs` (+106/-67). Subject line: "access_mode getter (Java Producer#getProducerAccessMode parity)". | **NEEDS-REVIEW** — check whether `getProducerAccessMode` parity has been added on main since. If not, this is real parity work worth preserving on a rebased branch. |
| `./.claude/worktrees/agent-aa655e6a5c1167e82` (feat/partitioned-auto-update-tickers) | 17 | +538/-2, 4 files — `multi_topics.rs` (+246), `partitioned_producer.rs` (+257), `partitioned_consumer.rs` (+32), `README.md` (+5/-2). Subject: auto-update-partitions tickers (PIP-145 / Java parity). | **NEEDS-REVIEW high-priority** — main already has `f09f23c feat(partitioned): auto_update_partitions_interval tickers`, but the size of this WIP (+538) suggests an alternate / more complete implementation. Diff vs current `multi_topics.rs` / `partitioned_*.rs` on main before discarding. |
| `./.claude/worktrees/agent-a1b132124465b1300` (worktree-agent-…) | worktree-agent-a1b132124465b1300 | 37 | One untracked file: `crates/magnetar/tests/e2e_partitioned_deep.rs`. | **NEEDS-REVIEW** — read the test file; if it duplicates landed e2e tests (which include partitioned roundtrips already), discard. Otherwise port to a new e2e branch — feeds directly into the user's "add e2e tests" objective. |
| `./.claude/worktrees/agent-aa5f3f8c161e60829` (test/e2e-compacted-tableview) | 37 | One untracked test: `crates/magnetar/tests/e2e_compacted.rs`, plus `Cargo.lock` + `crates/magnetar/Cargo.toml` (+5). | **NEEDS-REVIEW high-priority** — compacted topic + TableView e2e coverage is a real Java-parity gap. Preserve the test, rebase onto main. |
| `../magnetar.feat-moonpool-m4-consumer` | feat/moonpool-m4-consumer | 62 | Untracked `crates/magnetar-runtime-moonpool/src/consumer.rs` + 2 lines `lib.rs`. The committed M4 already landed on main, but this untracked file may be an earlier draft of the moonpool consumer. | Diff vs landed `magnetar-runtime-moonpool/src/consumer.rs` on main; if obsolete, discard. |

### Group C — Branch tip ahead of main (1)

| Branch | HEAD | Ahead | Behind | Notes |
| --- | --- | --- | --- | --- |
| feat/auto-schema-runtime-wire | e3d6dd3 | **1** | 43 | The one branch with a committed change not in main. `git diff main...feat/auto-schema-runtime-wire --stat`: 6 files, +336 lines: `magnetar-proto/src/{conn.rs (+18), schema/auto_consume.rs (+61), schema/auto_produce_bytes.rs (+12), schema/mod.rs (+22)}`, `magnetar-runtime-tokio/src/consumer.rs (+208)`, `magnetar/src/typed.rs (+15)`. Subject: "wire AutoConsumeSchema runtime auto-fetch on first receive (PIP-87)". |

Recommendation: **NEEDS-REVIEW**. Main already has `feat(schema):
AutoConsumeSchema + AutoProduceBytesSchema broker lookup (PIP-87)`
(commit `fb89bc6`) and `feat(typed): wire AutoConsumeSchema runtime
auto-fetch …` cited in the branch message, but ahead=1 says **one
commit on this branch is not in main**. Could be (a) a fixup that got
dropped by a stale-base merge — the docs commit message
`810278f docs(parity-matrix): resync rows clobbered by stale-base
merges` strongly suggests this happened at least once — or (b)
genuinely-superseded by a later squash. Diff and decide.

---

## Key findings

1. **Worktree blast radius is purely housekeeping.** 28 of 38
   worktrees are mechanical drops; the only judgement calls are the
   9 dirty ones + the 1 ahead branch. Recovering them is a focused
   30-minute audit.
2. **Two pieces of WIP are worth saving outright**:
   `./.claude/worktrees/agent-aa655e6a5c1167e82` (+538 lines, partitioned
   auto-update tickers) and `./.claude/worktrees/agent-aa5f3f8c161e60829`
   (compacted+TableView e2e tests). Both align with active objectives
   ("finish Java parity" and "add e2e tests").
3. **One commit may have been lost to a stale-base merge.**
   `feat/auto-schema-runtime-wire` is ahead=1. Worth confirming before
   the branch is deleted — `810278f` commit message explicitly mentions
   stale-base merges clobbering things.
4. **`gh pr list` is a non-signal** for this repo (zero PRs). Don't
   gate cleanup on PR status.
5. **Two `feat/service-url-provider`-shaped WIPs exist in parallel**
   (`../magnetar.feat-service-url-provider` and
   `./.claude/worktrees/agent-a81687493cdef1c6d`). Both add the same
   untracked `service_url.rs`. Resolve to one path.
6. **Locked agent worktrees under `./.claude/worktrees/`** need
   `--force` to remove. They will not be cleaned by a plain `wt remove`.

## Suggested cleanup order

1. **Cheap wins**: drop the 28 SAFE-TO-DROP worktrees. Use
   `wt remove <branch>` for sibling paths, `git worktree remove --force`
   for the locked `./.claude/worktrees/agent-*` ones. Then
   `git branch -d <branch>` for the local refs.
2. **Diff + discard or salvage** the 5 Group-A dirty-on-merged trees
   (mostly comment / refactor scratch).
3. **Save before delete** for Group B (the four real WIPs):
   - `agent-aa655e6a5c1167e82` (partitioned tickers expansion).
   - `agent-aa5f3f8c161e60829` (compacted/TableView e2e).
   - `agent-a842215fabac3e8ea` (access_mode getter parity).
   - `agent-a1b132124465b1300` (deep partitioned e2e test).
   For each: rebase onto current main, run validation chain, then drop
   the worktree.
4. **Resolve the ahead-by-1 branch** `feat/auto-schema-runtime-wire`:
   `git log main..feat/auto-schema-runtime-wire` → if the commit is
   already on main under a different SHA, drop; otherwise cherry-pick
   onto a fresh branch.

## Open questions

- Did `810278f` ("resync rows clobbered by stale-base merges") swallow
  more than docs? Worth a `git show 810278f --stat` before deleting
  any branch that is ahead of main. (Only one such branch exists today,
  but the lesson is general.)
- Are the locked `./.claude/worktrees/agent-*` lock files held by an
  agent that is *currently running*? If so, force-removing them will
  break that agent's session. Check `lsof` / process list before
  using `--force` on a path that doesn't belong to a finished run.
- Is there an expected naming convention for which work goes under
  `./.claude/worktrees/agent-*` vs. sibling `../magnetar.<branch>`?
  Today both shapes coexist; standardising would simplify future audits.
