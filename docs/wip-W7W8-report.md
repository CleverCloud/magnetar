# Phase 0b — W7 + W8 disposition

Date: 2026-05-21
Decision: **DROP both**, archive raw salvage attempts to `/tmp/magnetar-w7-recover/`.

## W7 — `worktree-agent-a1b132124465b1300`

Source WIP: one untracked file `crates/magnetar/tests/e2e_partitioned_deep.rs`.

The agent attempted a substantial rewrite of the on-main version (337
lines, +202/-135) adding:

- A `HashMap`-based per-partition delivery-order assertion.
- A `MessageRouter` override test pinning every send to partition 1.
- A `RoundRobin` "visits every partition" test.
- Improved docstrings + `--nocapture` invocation note.

The agent failed to compile the rewrite and aborted partway through
debugging a closure return-type mismatch (Bash session terminated with
the agent still investigating).

Main already has `crates/magnetar/tests/e2e_partitioned_deep.rs`
landed via an earlier commit and the file builds cleanly under
`cargo build --features e2e --tests`. Researcher A had flagged the WIP
as "likely duplicate".

Disposition: **DROP**. The salvage attempt's value is the test-design
inspiration (per-partition order assertion, router override coverage),
which can be lifted into a future `e2e_partitioned_deep` extension by
Phase 3 Batch B rather than rescued mid-failure.

Archive: `/tmp/magnetar-w7-recover/e2e_partitioned_deep.rs.w7-salvage`.

## W8 — `feat/moonpool-m4-consumer`

Source WIP: one untracked file `crates/magnetar-runtime-moonpool/src/consumer.rs` + 2 lines in `lib.rs`.

Main already has the moonpool M4 consumer landed (commit `7f6eca21
merge(runtime): T13 — Producer flush_with_timeout + Consumer
drain_messages`). The untracked file is an earlier draft.

Disposition: **DROP**. The on-main consumer is the canonical M4
implementation.

Archive: `/tmp/magnetar-w7-recover/consumer.rs.w8-salvage`.

## Summary table — all Phase 0b WIP terminations

| Agent | Worktree | Decision | Result |
|---|---|---|---|
| W2 | feat/partitioned-auto-update-tickers | DROP | superseded by `f09f23c` |
| W3 | test/e2e-compacted-tableview | DROP | byte-identical to main |
| W4 | feat/service-url-provider + agent-a81687493cdef1c6d | DROP both | superseded by `7b8d3e6` + `c978288` |
| W5 | worktree-agent-a842215fabac3e8ea | DROP | uncommitted PIP-4 work superseded; access_mode getter already landed |
| W7 | worktree-agent-a1b132124465b1300 | DROP (archived) | substantial salvage attempted but didn't compile; main already covers the surface |
| W8 | feat/moonpool-m4-consumer | DROP (archived) | superseded by main's `7f6eca21` |

Phase 0 final state: **38 → 1 worktrees** (`main` only). Zero unmerged
work remains. Both archived salvage attempts (W7 + W8) are kept under
`/tmp/magnetar-w7-recover/` for cross-reference if Phase 3 Batch B
or moonpool M5 want to revisit them.
