# Open Follow-Ups

Consolidated tracker for known open work. Each entry lists the gap,
the reason it stays open, and (where actionable) a `/goal …` block
ready to be copy-pasted verbatim into a fresh session for an agent
team to pick up.

For the public-facing parity status, see
[`parity-status.md`](parity-status.md) and the
[parity matrix in the README](../README.md#java-client-parity-matrix).

This file is the **single source of truth** for what is intentionally
deferred or blocked. Anything not listed below is either landed
(check `git log` for the implementation reference), or explicitly out
of scope for v0.2.0 ([ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
§D-series, [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md),
[ADR-0032](../specs/adr/0032-pip-466-v5-client-surface-scope.md)).

When a PR lands an item, the entry is **removed** (git log + the ADR /
docs file carry the post-implementation reference); partially-landed
items are trimmed to their remaining open residual.

**API stability stance.** The crate is not yet published. Breaking
API changes are acceptable when they improve correctness, ergonomics,
or layering; ship with `BREAKING CHANGE:` in the commit body so the
eventual changelog flags them.

---

## Index

Status tags: ⚡ ready to dispatch · 🔗 blocked on external dep ·
⏳ blocked on upstream PIP release · 🧠 needs design decision ·
🟡 deferred (not load-bearing).

| # | Item | Status |
| - | --- | --- |
| 1 | [PIP-460 scalable-topics e2e](#1-pip-460-scalable-topics-e2e) | ⏳ scaffold landed; e2e blocked on a Pulsar 5.0 RC shipping PIP-460 |
| 2 | [Re-pin moonpool off git `branch = "main"`](#2-re-pin-moonpool-off-git-branch-main) | ⏳ blocked on a moonpool crates.io release carrying [PR #113](https://github.com/PierreZ/moonpool/pull/113) |

---

## 1. PIP-460 scalable-topics e2e

**Gap.** The PIP-460 scalable-topics surface scaffold has landed across
proto / façade / both engines / CLI with the binding 4-layer in-process
tests (proto unit + tokio + moonpool 1:1 + differential + golden trace),
behind `feature = "scalable-topics"` (default off,
[ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md)). The
**e2e** test (`crates/magnetar/tests/e2e_scalable_topic.rs`) is
`#[ignore]`'d behind `feature = "e2e,scalable-topics"` with three named
tests that cannot run today — no broker ships PIP-460.

**Why it stays open.** Upstream PIP-460 is `Draft`, targeting Pulsar 5.0
LTS with phased rollout. The wire surface is hand-encoded in
`crates/magnetar-proto/src/pb/scalable_topics.rs` until a real RC ships.
Does **not** block the v0.2.0 release-cut.

**`/goal` (once a Pulsar 5.0 RC ships PIP-460).**

```text
/goal flesh out the PIP-460 e2e per docs/follow-ups.md §1 once upstream cuts a Pulsar 5.0 RC carrying PIP-460. First, as a dedicated commit per ADR-0026 §D4, run `cargo run -p xtask -- vendor-proto --rev <pulsar-5.0-rc-sha>` to replace the hand-encoded crates/magnetar-proto/src/pb/scalable_topics.rs module and reconcile field numbers against the vendored proto. Then implement the bodies of the three `#[ignore]`'d tests in crates/magnetar/tests/e2e_scalable_topic.rs against a real broker spawned via testcontainers-rs (gated `feature = "e2e,scalable-topics"`). Validation chain per CLAUDE.md.
```

---

## 2. Re-pin moonpool off git `branch = "main"`

**Gap.** `Cargo.toml`'s `[workspace.dependencies]` tracks `moonpool-core`
/ `moonpool-sim` on `{ version = "0.6.0", git = "…", branch = "main" }` to
consume the futures-io `TcpStream` + segment-granular `write_vectored`
change ahead of a crates.io release. This is a **documented, time-boxed
exception** to ADR-0036's exact-pin reproducibility discipline
([ADR-0043](../specs/adr/0043-temporary-floating-moonpool-git-dep.md)).
While it stands, `cargo update -p moonpool-core` can advance the rev to an
arbitrary later `main` commit; `Cargo.lock`'s concrete rev is the only
reproducibility anchor. (The `version = "0.6.0"` constraint trips
resolution if `main` crosses to 0.7, surfacing the trigger automatically.)

**Why it stays open.** Blocked on upstream cutting a **moonpool crates.io
release that contains [PR #113](https://github.com/PierreZ/moonpool/pull/113)**.
The last published release is `0.6.0`, which predates both the futures-io
migration and the vectored entry.

**`/goal` (post-release).**

```text
/goal re-pin moonpool off the git `branch = "main"` floating dependency per docs/follow-ups.md §2, once a moonpool crates.io release ships PR #113 (futures-io `NetworkProvider::TcpStream` + `SimTcpStream::poll_write_vectored`). In Cargo.toml `[workspace.dependencies]`, replace the two `{ version = "0.6.0", git = "https://github.com/PierreZ/moonpool", branch = "main" }` entries for `moonpool-core` / `moonpool-sim` with exact `=x.y.z` version pins matching the release that carries PR #113. Run `cargo update -p moonpool-core -p moonpool-sim` to refresh Cargo.lock to the released artefact. Confirm the transport still compiles against the `futures::io` ext traits (the release keeps the same surface). Remove the `[sources].allow-git` entry in deny.toml. Flip specs/adr/0043-temporary-floating-moonpool-git-dep.md Status to `Superseded by ADR-NNNN` and write the re-pin ADR (restores ADR-0036 exact-pin in full); flip the ADR-0036 amendment pointer + index status accordingly; update specs/README.md index. Update docs/simulation-patterns.md and any other version statement. Validation chain per CLAUDE.md (incl. `cargo deny check` — the release re-enables the version/advisory gates the git dep bypassed).
```

---

## Notes on this file

Items move from this file to `git log` when their commit lands. The
expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays open** +
   (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the
   post-implementation reference); partially-landed items are trimmed to
   their remaining residual.

All remaining items carry either a `/goal …` block ready to dispatch or an
explicit external blocker (upstream moonpool / Pulsar release). The only
fully-external blockers are the PIP-460 e2e
([§1](#1-pip-460-scalable-topics-e2e)) and the moonpool re-pin
([§2](#2-re-pin-moonpool-off-git-branch-main)), both pending an upstream
release.
