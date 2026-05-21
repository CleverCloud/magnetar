# Magnetar v0.1.0 Finish-Line Plan — Execution Status

Date: 2026-05-21
Driven by: `/ask --with-codex` plan in `~/.claude/plans/ask-magnetar-finish-plan.md`
User decisions captured: Option A (user-visible `<E>` generics), ProducerBlock in scope, `/loop` notifier, all WIP terminations approved.

## What landed (7 commits ahead of `origin/main`)

| Commit | Phase | Status |
|---|---|---|
| `8698961` | 0b W3 | ✅ DROP duplicate `e2e_compacted` test |
| `2191176` | 0b W2 | ✅ DROP `feat/partitioned-auto-update-tickers` (superseded by `f09f23c`) |
| `8821ab0` | 0b W4 | ✅ DROP both `service_url` WIPs (superseded by PIP-121) |
| `1c9bb5d` | 1.1 + planning | ✅ ADR-0019 engine scope + research/audit/review/W5 artefacts |
| `8ecdbeb` | 1.2 + 0b W7/W8 | ✅ README parity-matrix cleanups + W7/W8 disposition |
| `6b387b2` | 4 | ✅ CI hardening (codegen-check, MSRV-1.85, dependabot, /loop note) |
| `ee13d29` | 3 Batch E | ✅ e2e_crypto.rs (PIP-4 + Fail/Discard/Consume) |

### Phase status

| Phase | Status | Notes |
|---|---|---|
| 0a — bulk-drop 28 subset worktrees | ✅ complete | 38 → 1 worktree (main only after Phase 0) |
| 0b — WIP terminations W2..W8 | ✅ complete | All 5 dropped/superseded; W7/W8 archived to `/tmp/magnetar-w7-recover/` |
| 1.1 — ADR-0019 engine scope | ✅ complete | `specs/adr/0019-engine-scope-and-moonpool-parity.md` |
| 1.2 — README parity-matrix cleanups | ✅ complete | Producer::stats row reconciled; stale "Open structural gaps" updated |
| 1.3 — `MemoryLimitPolicy::ProducerBlock` | 🟡 **partial scaffold** | Type stubs only in `feat/memory-limit-producer-block` (uncommitted) — see below |
| 2 M5 — moonpool engine surface parity | 🟡 **partial scaffold** | DNS stub only in `feat/moonpool-m5-engine-parity` (uncommitted) — see below |
| 2 M6 — engine trait Option A | ❌ not dispatched | Depends on M5 |
| 2 M7 — moonpool chaos pack | ❌ not dispatched | Depends on M6 |
| 2 M8 — differential equivalence harness | ❌ not dispatched | Depends on M6 |
| 3 Batch A — 6 e2e tests (no fixtures) | ❌ no progress | Agent explored but produced nothing; needs redispatch |
| 3 Batch B — e2e auto-update tickers | ❌ not dispatched | |
| 3 Batch C — e2e reconnect + failover | ❌ not dispatched | |
| 3 Batch D — e2e TLS + OAuth2 fixtures | ❌ not dispatched | Gate (f) approved |
| 3 Batch E — e2e PIP-4 crypto | ✅ complete | 453 LOC, 4-5 test cases, gated `e2e` + `encryption` |
| 4 — CI hardening + /loop monitor | ✅ complete | `cargo xtask codegen --check` + MSRV-1.85 + dependabot wired |
| 5 — docs + ADR catch-up | ⌛ partial | This status report itself; remaining docs co-locate with future phases |

## Partial worktrees still open

Both kept intentionally — the auto-mode classifier blocked dropping
unmerged WIP, and the type stubs may be useful starting points.

### `feat/memory-limit-producer-block` (Phase 1.3 partial)

What's in: `MemoryLimitPolicy::{FailImmediately, ProducerBlock}` enum
added to `crates/magnetar-proto/src/conn.rs`, re-exported via
`crates/magnetar-proto/src/lib.rs`. `slab` + `Waker` imports added to
`crates/magnetar-runtime-tokio` Cargo.toml + `lib.rs`.

What's TODO:

1. `WakerSlab` field on `ConnectionShared` in `magnetar-proto/src/conn.rs`.
2. `try_reserve_memory_or_register(&self, bytes, waker) -> Result<Token, MemoryPending>`.
3. `release_memory(&self, bytes)` drain-and-wake fan-out.
4. `MemoryReservationGuard` Drop releases CAS + clears waker slot.
5. Runtime side in `magnetar-runtime-tokio/src/producer.rs` — `MemoryReserveFut` future + `Producer::send` integration when policy is `ProducerBlock`.
6. Tests: register-on-full, wake-on-release, cancel-clears-slot, runtime two-producer blocking.
7. ADR-0020 `0020-memory-limit-producer-block.md`.
8. README `:613` `memoryLimit` row update; `docs/parity-status.md` "Recently landed" row.

### `feat/moonpool-m5-engine-parity` (Phase 2 M5 partial)

What's in: scaffold `crates/magnetar-runtime-moonpool/src/dns.rs` (new
file, untracked), minor edits in `lib.rs` and `transport.rs`.

What's TODO: the 6 surfaces (supervised reconnect, DNS, driver-TLS,
memory_limit, ServiceUrlProvider, PIP-188) — see plan `~/.claude/plans/ask-magnetar-finish-plan.md` Phase 2 M5.

## Why parallel-large-batch failed

5 background agents were dispatched in parallel with substantial
scopes. Outcome:

- Phase 4 (CI hardening): ✅ completed — smallest scope.
- Phase 3 Batch E (e2e_crypto): ⌛ wrote the file but ran out of turns before commit; I salvaged inline.
- Phase 1.3 (ProducerBlock): scaffolded enum but ran out of turns before the WakerSlab implementation.
- Phase 2 M5 (6 surfaces): wrote DNS scaffold, ran out of turns at the second surface.
- Phase 3 Batch A (6 e2e tests): explored the codebase, ran out of turns before writing any test.

Root cause: each agent's token + tool-use budget is finite (~85K tokens
/ ~50 tool uses). Tasks that need 6+ files of new code with reads,
edits, validation runs, and commit/merge ceremony exceed that budget.

## Recommended continuation strategy

Each remaining phase should be re-dispatched in a fresh session with
**ONE focused agent per logical sub-unit**:

- **Phase 1.3**: split into 3 sequential dispatches —
  1. Proto-side WakerSlab + `try_reserve_or_register` + unit tests.
  2. Runtime-side `MemoryReserveFut` future + producer-send integration + integration test.
  3. ADR-0020 + README + parity-status update.
- **Phase 2 M5**: split into 6 dispatches, one per surface. Each is
  ~150-300 LOC, fits in one agent budget.
- **Phase 2 M6**: 1 dispatch (large, may need 2 — engine trait first,
  then generic-ification).
- **Phase 2 M7**: 8 dispatches, one chaos scenario per agent.
- **Phase 2 M8**: 1 dispatch.
- **Phase 3 Batch A**: 6 dispatches, one e2e test per agent.
- **Phase 3 Batch B**: 4 dispatches.
- **Phase 3 Batch C**: 2 dispatches (e2e_reconnect, e2e_cluster_failover).
- **Phase 3 Batch D**: 2 dispatches (e2e_tls, e2e_oauth2). Plus fixture
  setup (TLS-fronted broker, wiremock IDP) may need its own prep dispatch.

Total remaining: ~32 focused dispatches across multiple sessions.

## Approval gates still open

| Gate | Item | Status |
|---|---|---|
| (h) | Raise GitHub Actions spending limit | **user action**, untouched |
| (i) | Push to `origin/main` | not done; main is 7 commits ahead local-only |
| (a..g) | All other gates | already cleared via the plan-approval pass on 2026-05-21 |

## What's safe to push now

The 7 landed commits are all reviewed, signed off, GPG-signed, no
Claude attribution, and pass the validation chain individually. Push
is gated by (i) per ADR-0013 — user confirmation required.
