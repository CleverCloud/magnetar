# W5 WIP triage report — `worktree-agent-a842215fabac3e8ea`

- **Date**: 2026-05-21
- **Agent**: W5 (Phase 0b)
- **Worktree**: `.claude/worktrees/agent-a842215fabac3e8ea`
- **Branch**: `worktree-agent-a842215fabac3e8ea`
- **Departure (HEAD on main)**: `37d3c3e`

## Decision: DROP

The worktree's working tree carried +200/-71 across 4 files implementing
**PIP-4 consumer-side `CryptoFailureAction`** — despite the misleading
last commit subject ("access_mode getter (Java Producer#getProducerAccessMode parity)")
which describes a different change that is already merged in main as commit
`15fe1b6`.

Branch state:

- `git log main..HEAD` → empty (the access_mode commit is the merge-base).
- The +200/-71 delta is **uncommitted working-tree changes** on
  `worktree-agent-a842215fabac3e8ea`, not new commits.
- Files touched by the WIP:
  - `crates/magnetar-proto/src/conn.rs` +44 — `CryptoFailureAction` enum,
    `SubscribeRequest.crypto_failure_action`, `Connection::consumer_crypto_failure_action`.
  - `crates/magnetar-proto/src/consumer.rs` +50 — `ConsumerState.crypto_failure_action`
    field, getter, two unit tests.
  - `crates/magnetar-proto/src/lib.rs` +4/-1 — re-export `CryptoFailureAction`.
  - `crates/magnetar-runtime-tokio/src/consumer.rs` +106/-67 — runtime
    `PostProcessOutcome::{Deliver, Discard}` enum, threading the policy
    through `post_process_message`, replacing the inline decrypt logic in
    `ReceiveFut::poll`.

## Why drop: main already implements a strictly broader version

A grep of main confirms `CryptoFailureAction` is fully wired across the
workspace, and the implementation on main is more complete than the WIP:

- `crates/magnetar-proto/src/conn.rs:408` — `CryptoFailureAction` enum
  (`Fail` / `Discard` / `Consume`) with a `default()` impl, identical to
  the WIP.
- `crates/magnetar-proto/src/conn.rs:1877` — `subscribe()` persists the
  action onto `ConsumerState` (same as WIP).
- `crates/magnetar-proto/src/conn.rs:2138` — `Connection::consumer_crypto_failure_action(handle)`
  with the same fail-safe default for unknown handles (same as WIP).
- `crates/magnetar-proto/src/conn.rs:3081`-`3110` — two unit tests
  (unknown-handle default, round-trip through subscribe) — same coverage
  as WIP's two tests.
- `crates/magnetar-runtime-tokio/src/consumer.rs:897` — `PostProcessOutcome`
  enum, but with **three variants** in main (`Deliver`, `Discard`,
  `Fail(ClientError)`) vs. the WIP's two (early-`return Err` on Fail).
  Main's shape lets the caller centralise error propagation; the WIP's
  shape forces error-return inside `post_process_message`, which is
  strictly less flexible.
- `crates/magnetar-runtime-tokio/src/consumer.rs:1059`-`1099` — the
  `ReceiveFut::poll` loop already honours `Discard`/`Consume` semantics
  (the WIP's main behavioural addition).
- `crates/magnetar/src/client.rs:1683` — top-level `ConsumerBuilder::crypto_failure_action`
  setter (the WIP did not touch the builder; main carries the full
  user-facing surface).
- `crates/magnetar/src/table_view.rs` (multiple sites) —
  `TableViewBuilder::crypto_failure_action` propagation to the underlying
  consumer (entirely missing from the WIP).

In short: every line the WIP would add either already exists verbatim
on main, or exists in a strictly more capable form. Nothing in the WIP
adds coverage that main does not already have.

## Sans-io check

Main's implementation respects the invariants from `GUIDELINES.md`:

- `CryptoFailureAction` enum lives in `magnetar-proto` with no I/O deps.
- No `Instant::now()` / `SystemTime::now()` calls were added in
  `magnetar-proto` for this feature (the dispatch is pure state).
- No channels introduced.

The WIP would not have improved on any of those.

## Action taken

- Identified WIP scope: uncommitted PIP-4 consumer-side
  `CryptoFailureAction` plumbing.
- Verified main already implements a superset.
- No salvage commit needed.
- Worktree to be removed per the W5 directive (operator step:
  `wt remove agent-a842215fabac3e8ea`).

## Residual risks

None found. Main's parity matrix at `README.md` covers
`cryptoFailureAction` on the consumer surface, and the producer-side
`access_mode` getter referenced in the misleading branch commit subject
is also already merged (commit `15fe1b6`).
