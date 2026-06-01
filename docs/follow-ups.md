# Open Follow-Ups

Consolidated tracker for known open work. Each entry lists the gap,
the reason it stays open, and (where actionable) a `/goal …` block
ready to be copy-pasted verbatim into a fresh session for an agent
team to pick up.

For the public-facing parity status, see
[`parity-status.md`](parity-status.md) and the
[parity matrix in the README](../README.md#java-client-parity-matrix).

This file is the **single source of truth** for what is intentionally
deferred or blocked. Anything not listed below is either already
shipped (check `git log` for the implementation reference) or
explicitly out of scope
([ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
§D-series, [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md),
[ADR-0032](../specs/adr/0032-pip-466-v5-client-surface-scope.md)).

When a PR closes an item, the entry is **removed** (git log + the ADR /
docs file carry the post-implementation reference); partially-closed
items are trimmed to their remaining open residual.

**API stability stance.** The crate is not yet published. Breaking
API changes are acceptable when they improve correctness, ergonomics,
or layering; flag them with `BREAKING CHANGE:` in the commit body so
the eventual changelog picks them up.

---

## Index

Status tags: ⚡ ready to dispatch · 🔗 blocked on external dep ·
⏳ blocked on upstream PIP release · 🧠 needs design decision ·
🟡 deferred (not load-bearing).

| # | Item | Status |
| - | --- | --- |
| 1 | [PIP-460 scalable-topics e2e](#1-pip-460-scalable-topics-e2e) | ⏳ scaffold in place; stub bodies trivially pass; flesh out once a Pulsar 5.0 RC carries PIP-460 |
| 2 | [Re-pin moonpool off git `branch = "main"`](#2-re-pin-moonpool-off-git-branch-main) | ⏳ blocked on a moonpool crates.io release carrying [PR #113](https://github.com/PierreZ/moonpool/pull/113) |
| 3 | [Moonpool `ProxyConnectionPool` parity](#3-moonpool-proxyconnectionpool-parity) | ⚡ ready to dispatch — tokio ships the pool ([ADR-0039](../specs/adr/0039-pulsar-proxy-multi-broker-connection-model.md)); moonpool returns `ProxyUnsupportedOnUnsupervisedClient` |

---

## 1. PIP-460 scalable-topics e2e

**Gap.** The PIP-460 scalable-topics surface scaffold is in place across
proto / façade / both engines / CLI with the binding 4-layer in-process
tests (proto unit + tokio + moonpool 1:1 + differential + golden trace),
behind `feature = "scalable-topics"` (default off,
[ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md)). The
**e2e** tests in `crates/magnetar/tests/e2e_scalable_topic.rs` have
stub bodies that touch a constant and return — per
[ADR-0046](../specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md)
they run on every `cargo test --features scalable-topics` and trivially
pass. Three named tests are wired but un-fleshed; no released broker
speaks PIP-460.

**Why it stays open.** Upstream PIP-460 is `Draft`, targeting Pulsar 5.0
LTS with phased rollout. The wire surface is hand-encoded in
`crates/magnetar-proto/src/pb/scalable_topics.rs` until a real RC ships.

**`/goal` (once a Pulsar 5.0 RC carries PIP-460).**

```text
/goal flesh out the PIP-460 e2e per docs/follow-ups.md §1 once upstream cuts a Pulsar 5.0 RC carrying PIP-460. First, as a dedicated commit per ADR-0026 §D4, run `cargo run -p xtask -- vendor-proto --rev <pulsar-5.0-rc-sha>` to replace the hand-encoded crates/magnetar-proto/src/pb/scalable_topics.rs module and reconcile field numbers against the vendored proto. Then implement the bodies of the three stub tests in crates/magnetar/tests/e2e_scalable_topic.rs against a real broker spawned via testcontainers-rs (file is gated `feature = "scalable-topics"` per ADR-0046; no `#[ignore]`, no `feature = "e2e"`). Validation chain per CLAUDE.md.
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

## 3. Moonpool `ProxyConnectionPool` parity

**Gap.** ADR-0039 (Pulsar Proxy multi-broker connection model) ships a
`ProxyConnectionPool` on the tokio engine
(`crates/magnetar-runtime-tokio/src/pool.rs`, ~337 LOC) that pins a
per-broker connection on `proxy_through_service_url = true` lookups and
avoids the ~90 ms reconnect storm from issue #15. The moonpool engine
does **not** have a counterpart yet — the lookup path returns
`ClientError::ProxyUnsupportedOnUnsupervisedClient`
(`crates/magnetar-runtime-moonpool/src/client.rs:313-326`,
`crates/magnetar-runtime-moonpool/src/producer.rs:595`) and the
`crates/magnetar-runtime-moonpool/src/lib.rs:70` carries a `TODO(proxy)`
flagging the work. Both [`docs/parity-status.md`](parity-status.md)
and [the README parity
matrix](../README.md#java-client-parity-matrix) currently mis-state
moonpool's binary-proxy row as ✅ — fixed in the same changeset that
adds this entry.

**Why it stays open.** Implementation work, not external blocker. The
tokio pool is the reference; the moonpool variant needs to be ported
on top of `moonpool_core::Providers` (network + clock) and wired into
the supervised-redial path.

**`/goal` (ready to dispatch).**

```text
/goal land a moonpool flavour of magnetar_runtime_tokio::pool::ProxyConnectionPool per docs/follow-ups.md §3 / ADR-0039. Read crates/magnetar-runtime-tokio/src/pool.rs as the reference implementation, then port the pin-per-broker pool to crates/magnetar-runtime-moonpool/src/pool.rs over moonpool_core::Providers (NetworkProvider + clock injection per ADR-0011). Wire it into the moonpool client's lookup path so `proxy_through_service_url = true` responses route through the pool instead of returning ClientError::ProxyUnsupportedOnUnsupervisedClient (crates/magnetar-runtime-moonpool/src/client.rs:313-326, crates/magnetar-runtime-moonpool/src/producer.rs:595). Remove the `TODO(proxy)` at crates/magnetar-runtime-moonpool/src/lib.rs:70. Land the four-layer test parity per ADR-0024 (proto unit, tokio integration, moonpool 1:1 integration, differential equivalence) plus an e2e exercise that piggybacks the existing crates/magnetar/tests/e2e_pulsar_proxy.rs fixture. After this lands, flip docs/parity-status.md + README.md's parity-matrix proxy row to genuinely ✅ on moonpool. Validation chain per CLAUDE.md.
```

---

## Notes on this file

Items move from this file to `git log` when their commit ships. The
expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays open** +
   (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the
   post-implementation reference); partially-closed items are trimmed
   to their remaining residual.

All remaining items carry either a `/goal …` block ready to dispatch or
an explicit external blocker (upstream moonpool / Pulsar release). The
only fully-external blockers are the PIP-460 e2e flesh-out
([§1](#1-pip-460-scalable-topics-e2e)) and the moonpool re-pin
([§2](#2-re-pin-moonpool-off-git-branch-main)). The moonpool
`ProxyConnectionPool` parity ([§3](#3-moonpool-proxyconnectionpool-parity))
is dispatchable today.
