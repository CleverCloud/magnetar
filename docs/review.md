# Review — tasks/todo.md against HEAD `37d3c3e`

**Reviewer**: reviewer agent
**Date**: 2026-05-21
**Plan reviewed**: `tasks/todo.md` (979 lines)
**Departure ref**: `main @ 37d3c3e`

Verdict: the plan is well structured and overwhelmingly correct on the
factual claims (worktree census, parity status, supervisor pattern,
approval gates). It has **two CRITICAL file-path errors** that will
make Phase 1.3 / Phase 2 M5 dispatch fail on first command, several
**MAJOR** scoping or sequencing issues, and a handful of MINOR polish
items. Address the criticals before any agent is dispatched.

---

## CRITICAL — plan must change

### C1 — `MemoryLimitPolicy` lives in `conn.rs`, not `client/memory_limit.rs`

`tasks/todo.md:250` instructs P3 to *"Extend
`crates/magnetar-proto/src/client/memory_limit.rs` (or wherever the
accounting lives — confirm with grep on `MemoryLimitPolicy`)"*.

There is no `client/` directory in `magnetar-proto`:

```
crates/magnetar-proto/src/ → auth/  pb/  schema/  trackers/
                            auth.rs  backoff.rs  cluster_failover.rs
                            conn.rs  consumer.rs  error.rs  event.rs
                            frame.rs  lib.rs  lookup.rs  producer.rs
                            service_url.rs  supervisor.rs
                            topic_watcher.rs  txn.rs  types.rs
```

The actual accounting site is `crates/magnetar-proto/src/conn.rs:113-117`
(`ConnectionShared.memory_limit_bytes` + `memory_used: AtomicU64`) —
the canonical home documented in `specs/adr/0017-memory-limit-atomic-reservation.md:30-52`.
The runtime path is `crates/magnetar-runtime-tokio/src/producer.rs:171`
(the `try_reserve_memory` no-op comment cited in ADR-0017).

Action: rewrite Phase 1.3's file-list (`tasks/todo.md:265-277`) to
target `crates/magnetar-proto/src/conn.rs` (extend `ConnectionShared`
with a `WakerSlab` field, add `try_reserve_with_waker(bytes, waker)`,
release path stays in the existing `SendFut::Drop` cited in ADR-0017
§Decision). The "client/memory_limit.rs" filename is hallucinated.

### C2 — `feat/auto-schema-runtime-wire` is *already landed* — Phase 0b W6 has no port to do

`tasks/todo.md:145` says W6 should diff and possibly cherry-pick the
one extra commit on `feat/auto-schema-runtime-wire`.

`git log feat/auto-schema-runtime-wire --not main` shows the single
ahead-commit is `e3d6dd3 feat(typed): wire AutoConsumeSchema runtime
auto-fetch on first receive (PIP-87)`. The same patch was squashed
onto main as `010e252 feat(typed): wire AutoConsumeSchema runtime
auto-fetch on first receive (PIP-87)` — identical subject, identical
commit body (the first lines match verbatim).

The `git diff main..feat/auto-schema-runtime-wire --stat` output is
**deletions of every ADR file and `xtask/src/main.rs`** —
`79 files changed, 196 insertions(+), 11057 deletions(-)`. That diff
is purely *stale base*, not "missed work". The branch is rooted before
`99b7b04 docs: promote plan + annexes to docs/, atomise decisions into
specs/adr/` and looks ahead-by-1 only because of that promotion.

Action: change W6's anticipated outcome from "needs check / likely
cherry-pick" to **drop without cherry-pick**. Add a one-line stash-list
re-check (per R2 mitigation) and discard. The 810278f "stale-base
merges" mention in the plan rationale is exactly this — the diff is
*resync noise*, not pending work.

---

## MAJOR — plan should change

### M1 — `PulsarClient` is not generic today; Phase 2 M6 needs a smaller-step option

`tasks/todo.md:351-376` mandates `PulsarClient<E: Engine>` and threads
`<E>` through `partitioned_producer.rs`, `partitioned_consumer.rs`,
`multi_topics.rs`, `pattern_consumer.rs`, `table_view.rs`,
`transaction.rs`, `typed.rs`.

Current shape at `crates/magnetar/src/client.rs:736`:

```rust
pub struct PulsarClient { ... }                              // line 736
impl PulsarClient { ... }                                    // line 741
```

No `<E>` generic, no `Engine` trait, no associated types. Importantly,
`ConsumerInterceptor` and `ProducerInterceptor` (lib.rs:78-80) are
re-exported as the public SPI surface, and the typed schemas (`Schema<T>`
mirrored at `client.rs:875,886`) take `<T>` already. Adding `<E>` makes
the public types `PulsarClient<E>`, `TypedConsumer<T, E>`, and the
interceptor traits would need either `<E>` everywhere or
type-erasure — non-trivial churn.

The plan's Option B ("duplicate façade") is dismissed in one line at
`tasks/todo.md:357`. Reconsider a third option:

- **Option C — feature-gated re-export**: keep `PulsarClient` non-generic.
  Behind `#[cfg(feature = "tokio")]` the type aliases to
  `PulsarClient<TokioEngine>` internally; behind `#[cfg(feature =
  "moonpool")]` it aliases to `PulsarClient<MoonpoolEngine<P>>`. Users
  only ever see one variant per build. Engine selection is a build-time
  switch, not a runtime generic. This is what the README "default tokio,
  opt-in moonpool" feature already implies — and it's the
  `magnetar`-façade's existing pattern (`magnetar/src/lib.rs`
  re-exports `magnetar_runtime_tokio::*`).

Option C gets >80% of the moonpool-engine-driveable-from-façade goal
without exploding generics across `ConsumerInterceptor`,
`TypedConsumer<T>`, and the in-flight e2e suites. The plan should
present A vs. B vs. C to Florentin at gate (e), not A vs. B.

### M2 — Phase 2 M7 (moonpool chaos) and Phase 3 Batch C (broker-restart e2e) are 80% the same scenario set

`tasks/todo.md:390-419` (M7) lists 8 chaos scenarios; 6 of them
(handshake partition, frame reorder, send timeout, OAuth2 token refresh,
PIP-188 topic-migrated, PIP-121 failover oscillation) overlap with
Batch C's `e2e_reconnect.rs` + `e2e_cluster_failover.rs`
(`tasks/todo.md:557-575`).

Moonpool gives bit-for-bit reproducibility for these; testcontainers
can stop/start a broker but cannot inject mid-frame partition,
mid-handshake drop, or virtual-clock OAuth2 expiry. The work to make
Batch C reliable on docker (the R4 "5% flake threshold" the plan
already raises) is significant; the same coverage from M7 is *more
reliable* and *strictly cheaper*.

Action: drop sub-tests 1c (in-flight reconnect epoch bump),
2c (PIP-188 via fakes) from Batch C — they're moonpool-native. Keep
1a/1b/2a/2b as smoke-only ("does it survive a full broker bounce"). M7
owns the deterministic side. Update R4 mitigation accordingly.

### M3 — Locked agent worktrees: the `lsof`/`ps` safety step is documented but the loop is missing

`tasks/todo.md:91-93` correctly calls for `lsof +D <path>` and
`ps -eo pid,cmd | rg <path>` before any `--force` remove. R1
(`tasks/todo.md:936`) repeats the safeguard.

But the procedure at step 3 says *"git worktree remove --force <path>
(these carry a .git/locked marker from agent runs)"* — that is a `git`
command, **not** `wt remove`. The `wt` CLI has its own locked-worktree
semantics (see CLAUDE.md "Worktree-First Development"). The plan should
say which path is authoritative for the 14 locked
`.claude/worktrees/agent-*` entries:

- If `wt remove` honours `.git/locked` and refuses, the plan should
  default to `wt remove`, fall back to `git worktree remove --force`
  only after lsof clears.
- If `wt remove` blindly force-removes, the plan should say so
  explicitly so a junior agent doesn't assume otherwise.

Action: pick one tool, document the fallback ladder.

### M4 — `cargo xtask codegen --check` is genuinely absent from CI — Phase 4.1 is correct, but mind ordering

`.github/workflows/ci.yml` (full file read) confirms no `codegen` step.
Present jobs: `fmt`, `clippy`, `build`, `test`, `doc`, `deny`,
`no-channels`, `no-io-deps`, `no-internal-clock`, `moonpool-sim`,
`e2e`, `mutants-smoke` (workflow_dispatch), `fuzz-smoke`. The
`no-internal-clock` job exists at lines 92-104. Phase 4.1's claim is
**OK** on substance but the YAML stub at `tasks/todo.md:661-670` should
add `Swatinem/rust-cache@v2` *before* the run step (it shows
`uses: Swatinem/rust-cache@v2` already — verify the indentation and
job-order in the actual patch).

### M5 — MSRV-1.85 job: no MSRV declared anywhere

Phase 4.2 (`tasks/todo.md:679-689`) is correct that the MSRV job is
missing. Also note: `rust-toolchain.toml` declares `channel = "stable"`
with no MSRV pin, and `Cargo.toml` has zero `rust-version` matches
(verified via `rg -n 'rust-version'`). The Phase 4.2 step needs a
companion edit to add `rust-version = "1.85.0"` to the workspace
manifest *first*, otherwise `cargo build` on a 1.85 toolchain may
succeed today but `cargo` will not refuse a 1.84 build later when
nothing pins the lower bound. ADR-0007 says "MSRV 1.85" but doesn't
materialize the pin.

Action: split Phase 4.2 into two steps: (a) add `rust-version =
"1.85.0"` to root `Cargo.toml`, (b) add the MSRV CI job.

### M6 — Slack-webhook CI watcher is overreach for the user's ask

User asked to "monitor CI to tackle errors if any". Plan §4.5
(`tasks/todo.md:718-748`) proposes both Option A (cron `/loop`) and
Option B (Slack webhook), then recommends B. The webhook commits a
*recurring* obligation (channel hygiene, secret rotation, spam
discipline), needs a private Slack channel + secret, and per R6
(`tasks/todo.md:941`) carries a metadata-leak risk.

The user's hard requirement is "tackle errors if any" — that fits the
existing `gh run watch` / `gh run list` workflow inside a `/loop`
session (Option A), no secret needed. The webhook is a separate
discussion. Recommend Option A as the *plan default*, present B as a
follow-up if Florentin asks for proactive paging.

---

## MINOR — nice-to-have

### N1 — Recommendation: bump Phase 5 "co-located ADRs" out of separate phase

`tasks/todo.md:761-792` treats Phase 5 as a separate stream. GUIDELINES
"Docs are code" and CLAUDE.md project memory both require ADRs in the
*same* changeset as the code. Phase 5 is structurally a *check* on
phases 1–4, not a phase. Recasting it as "Phase 0 acceptance gate per
merge" reduces D1's coordination cost.

### N2 — `0c` mentions `magnetar.docs-code-comments-reference-adrs` and `magnetar.refactor-simplify-pass` but `git worktree list` shows their tips at `35795ba` (= main parent). Verify these are *behind* main, not ahead-by-1

`git worktree list` shows both at sha `35795ba`. `git log main..35795ba`
will be empty (35795ba is an ancestor of main). They are safe-drop
candidates and belong in 0a, not 0c. Move them to the 0a batch and
drop 0c entirely (or absorb it into 0a — same agent W1 owns both).

### N3 — Phase 3 Batch D dependency adds (`rcgen`, `wiremock`)

Plan `tasks/todo.md:592-596` correctly gates dep-adds (gate (f)). Spot
check: `wiremock` is *already* a dev-dep in `magnetar-auth-oauth2`
(`tasks/todo.md:595` acknowledges this with "verify"). The plan should
just verify and inline-cite. Minor — does not change the plan, only
preempts the verification step.

### N4 — Supervisor designation per phase

CLAUDE.md "Supervisor pattern (4+ agents)" is honoured for Phase 0
(S0 supervises W1..W7). Phase 2 has 4 active agents (S5–S8) but no
designated supervisor. Phase 3 has 5 active agents (E1–E5) — `SP`
("planner-supervisor") is named at `tasks/todo.md:846` to cover the
overlap, but no per-phase explicit designation. Add an "S2-supervisor"
and "S3-supervisor" line, or one `SP` covering both with explicit
"≥4 ⇒ supervisor active" assertion.

### N5 — Phase 0a step 4 escalation path

`tasks/todo.md:94-96` says `git branch -D` triggers escalation if `-d`
complains. Good. Add: also escalate if `git rev-list <branch>..main`
is **empty in both directions** (= branch carries genuine work not on
main); the merge-base check at step 1 catches most of these but a
divergent branch could pass step 1 and still need attention. Belt and
braces.

---

## OK — already correct, called out for completeness

- **ServiceUrlProvider claim**: plan `tasks/todo.md:52-54` cites
  `crates/magnetar-runtime-tokio/src/auto_cluster_failover.rs:40-91`.
  Verified: the trait is actually at
  `crates/magnetar-proto/src/service_url.rs:50` (ServiceUrlProvider
  sync trait + `StaticServiceUrlProvider` impl), consumed via
  `magnetar/src/client.rs:968` and `:1066`. The plan's claim that
  `feat/service-url-provider` is superseded is correct; the file path
  it cites for the production surface is close enough (runtime side)
  but the trait itself lives in proto. Not a fix needed, just a polish
  if the plan ever re-cites.
- **Worktree census**: 39 entries on disk (`wt list`); plan claims 38.
  Off-by-one is irrelevant given 28 safe-drop bulk. OK.
- **No-channels rule + Waker slab**: plan §1.3 explicitly references
  ADR-0011 + the no-channels rule and prescribes
  `parking_lot::Mutex<Vec<Waker>>` — exactly what ADR-0017 already
  uses. Correct sans-io discipline, no I/O leak risk.
- **Approval gates a..i**: each user-irreversible action *is* gated.
  ADR creation gating (M4 here notwithstanding) is implicit in "ADR
  written in same changeset as code"; the gate is on the merge, which
  is gate (b)/(c)/(d)/(e) depending on phase. OK.
- **Agent-team multiplicity**: plan acknowledges
  `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1` (line 852–853, line 978). OK.
- **WIP termination is parallel**: §0b assigns one agent per worktree
  (W2..W7), explicitly "all running in parallel under supervisor S0"
  (line 113). User's intent ("schedule across the team of agent") is
  honoured.
- **PIP-415 + AutoProduceBytesSchema** correctly listed as out-of-scope
  (`tasks/todo.md:968-975`). OK.

---

## Summary

Top 5 issues, in priority order:

1. **C1** — Phase 1.3 file paths are wrong (`client/memory_limit.rs`
   does not exist; accounting is in `magnetar-proto/src/conn.rs`).
2. **C2** — W6's `feat/auto-schema-runtime-wire` ahead-commit
   `e3d6dd3` is already landed as `010e252`; the apparent
   "+336 over 6 files" is stale-base resync, not pending work. Drop,
   don't port.
3. **M1** — `PulsarClient<E>` blast radius is bigger than the plan
   acknowledges (interceptor SPI, typed schemas). Present Option C
   (feature-gated façade alias) at gate (e), not just A vs. B.
4. **M2** — Phase 2 M7 and Phase 3 Batch C overlap. Pull frame-level
   reconnect cases out of Batch C; let M7 own them (deterministic and
   cheaper).
5. **M3** — Locked-worktree teardown procedure mixes `wt remove` and
   `git worktree remove --force`. Pick one ladder, document the
   fallback explicitly so an agent doesn't pick the wrong tool.

Once C1 + C2 are corrected and M1/M2/M3 are at least surfaced for
Florentin's gate decision, the plan is **ready to dispatch**. The
structural backbone (5 phases, supervisor pattern at ≥4 agents,
approval gates a..i, validation chain) is sound.

File paths for follow-up:

- `/home/florentin/Sources/github.com/FlorentinDUBOIS/magnetar/tasks/todo.md`
- `/home/florentin/Sources/github.com/FlorentinDUBOIS/magnetar/crates/magnetar-proto/src/conn.rs:113-117`
- `/home/florentin/Sources/github.com/FlorentinDUBOIS/magnetar/specs/adr/0017-memory-limit-atomic-reservation.md:30-52`
- `/home/florentin/Sources/github.com/FlorentinDUBOIS/magnetar/crates/magnetar/src/client.rs:736-741` (current non-generic `PulsarClient`)
- `/home/florentin/Sources/github.com/FlorentinDUBOIS/magnetar/.github/workflows/ci.yml` (no codegen, no MSRV job — confirmed)
- `/home/florentin/Sources/github.com/FlorentinDUBOIS/magnetar/rust-toolchain.toml` (no MSRV pin)
- `/home/florentin/Sources/github.com/FlorentinDUBOIS/magnetar/Cargo.toml` (no `rust-version`)
