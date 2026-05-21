# WIP triage — W4 (PIP-121 ServiceUrlProvider duplicates)

Scope: terminate two stale `service_url.rs` work-in-progress worktrees on
the magnetar checkout and confirm their content is fully superseded by
landed work on `main`.

Main reference point: HEAD `37d3c3e` (2026-05-21).

## TL;DR

Both worktrees were SUPERSEDED. Both have been removed. No code was
salvaged; nothing was merged forward.

## Per-worktree decision

### 1. `../magnetar.feat-service-url-provider` (branch `feat/service-url-provider`)

- **State observed**: 43 commits behind `main`. Modified
  `crates/magnetar-proto/src/conn.rs`, `crates/magnetar-proto/src/lib.rs`,
  `crates/magnetar/src/client.rs`; untracked
  `crates/magnetar-proto/src/service_url.rs` (177 LoC).
- **API shape**: trait with `next_url(&self) -> Option<String>` +
  `record_outcome(&self, url: &str, connected: bool)` + reference impls
  `StaticServiceUrl` / `RoundRobinServiceUrls`. Aimed at a *Stage 1
  skeleton* with a "supervisor calls between reconnect cycles" design
  note explicitly marked as a future follow-up.
- **Diff sanity**: `git diff main` shows the WIP would delete ~11,150
  lines — every doc, every ADR, the entire moonpool engine, every e2e
  test, and `auto_cluster_failover.rs`. The branch is pinned to a stale
  ancestor that pre-dates M2 → M9.
- **Superseded by**:
  - `7b8d3e6 feat(supervisor): plumb ServiceUrlProvider through the
    supervised reconnect path (PIP-121)`
  - `c978288 feat(client): TLS hostname-only-skip + PIP-121
    AutoClusterFailover + ControlledClusterFailover (Java parity)`
- **Production surface on `main`**:
  `crates/magnetar-proto/src/service_url.rs` ships a `Send + Sync +
  Debug` trait with `get_service_url(&self) -> String` (the Java parity
  contract — the runtime polls per-attempt) plus
  `StaticServiceUrlProvider` and `static_service_url_provider(...)`
  helper. `cluster_failover.rs` carries the policy types and
  `magnetar-runtime-tokio/src/auto_cluster_failover.rs` ships the
  latency-driven `AutoClusterFailover` + signal-driven
  `ControlledClusterFailover` policies (the very things the WIP's doc
  comments deferred to a follow-up).
- **Decision**: SUPERSEDED. Different API shape, older ancestor,
  no salvageable bits — the production version already covers the WIP's
  "Stage 2" follow-up.
- **Action**: `wt remove --force --force-delete feat/service-url-provider`.

### 2. `./.claude/worktrees/agent-a81687493cdef1c6d` (branch `worktree-agent-a81687493cdef1c6d`)

- **State observed**: 42 commits behind `main`. Modified
  `crates/magnetar-proto/src/lib.rs` (2-line module export); untracked
  `crates/magnetar-proto/src/service_url.rs` (155 LoC).
- **API shape**: trait with `get_service_url(&self) -> String` plus
  `StaticServiceUrlProvider` and helper
  `static_service_url_provider(...)` — same surface as the production
  version on `main`, byte-for-byte equivalent semantics.
- **Diff sanity**: `git diff main` shows the WIP would delete ~11,150
  lines of subsequent work, same as worktree 1 (pre-dates M2 → M9).
- **Superseded by**: same two commits (`7b8d3e6`, `c978288`).
  Functionally this WIP **is** the seed that landed on `main` — but as
  a worktree it now sits behind 42 commits of integration work that
  rebasing it forward would require us to throw away.
- **Decision**: SUPERSEDED. The trait + static impl + helper already
  live on `main` at `crates/magnetar-proto/src/service_url.rs`. No
  drift to salvage.
- **Action**: `git worktree unlock` then
  `wt remove --force --force-delete worktree-agent-a81687493cdef1c6d`.

## Outcome

- Both worktrees removed; no follow-up branch needed.
- `magnetar.feat-service-url-provider` directory: gone.
- `.claude/worktrees/agent-a81687493cdef1c6d`: gone.
- No commits authored, no pushes performed.
- Main is the single source of truth for PIP-121 `ServiceUrlProvider`,
  `AutoClusterFailover`, and `ControlledClusterFailover`.

## Validation

Nothing on `main` was modified. The WIPs were untracked/uncommitted
scratch state; removal is a no-op from `main`'s perspective. No
formatter / clippy / test run was required.
