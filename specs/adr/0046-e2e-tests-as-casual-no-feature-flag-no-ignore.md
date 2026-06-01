# ADR-0045 — e2e tests are casual: no feature flag, no `#[ignore]`

- **Status**: Accepted
- **Date**: 2026-06-01
- **Decider**: Florentin Dubois
- **Tags**: testing, ci, process, agents

## Context

Until this ADR, the workspace gated end-to-end tests with **two**
overlapping mechanisms:

1. A Cargo feature, `e2e` (and a sibling `e2e-multi-cluster`,
   ADR-0034), put `#![cfg(feature = "e2e")]` at the top of every
   `crates/magnetar/tests/e2e_*.rs` file. The feature itself gates
   nothing else — `testcontainers`, `wiremock`, `aws-lc-rs`, `base64`
   are all unconditional `[dev-dependencies]` of the `magnetar` crate,
   so the feature is purely a per-file compile switch for tests.
2. A runtime `#[ignore = "e2e: requires Docker..."]` on every test in
   those files, blessed by ADR-0021 §1 as the workspace-approved way to
   surface an environment dependency.

The runtime `#[ignore]` is therefore **redundant ceremony** layered on
top of the compile-time gate: a contributor who has already opted in
via `--features e2e` immediately has to opt in *again* via
`--include-ignored`. That double opt-in offers no protection the
compile-time gate doesn't already give, while requiring two distinct
invocations of the validation chain (`cargo test --workspace
--all-features` vs `cargo test --workspace --features e2e --
--include-ignored`) and a dedicated `e2e` CI job
(`.github/workflows/ci.yml:212-248`) that the regular `test` job
duplicates byte-for-byte once Docker setup is added.

ADR-0036 (cost-shifting) keeps the heaviest checks off the per-PR
hot-path on the grounds that they don't shape PR diffs. The e2e suite
**does** shape PR diffs — broker-behavior regressions are exactly the
class of bug it exists to catch — and it already runs on every PR and
every push to `main` per the `e2e` job's `on: { push, pull_request }`
trigger. The only reason it remains a separate job is the redundant
feature gate. Folding it into the regular `test` job removes a
duplicate Rust compile pass per PR (Docker pre-pulls are cheap;
rebuilding the workspace under a different feature set is not).

The PIP-33 two-cluster e2e
(`.github/workflows/e2e-replicated-subs.yml`) runs **weekly** today,
gated by the additional `e2e-multi-cluster` feature on the same
grounds. With e2e folded into per-PR, keeping a separate weekly job
for the two-cluster fixture is the only remaining cost-shifting case
worth preserving — but the user-facing call here is that PIP-33
regressions are also worth catching per-PR. The two-cluster
docker-compose fixture costs ~60s to spin up; that's acceptable on
top of the ~60s single-broker startup the e2e job already pays.

## Decision

End-to-end tests run as **regular tests**, with no compile-time feature
gate and no runtime `#[ignore]`.

Concretely:

1. **Remove the `e2e` Cargo feature** from
   `crates/magnetar/Cargo.toml`. It gates nothing outside the
   `#![cfg(feature = "e2e")]` lines in test files.
2. **Remove the `e2e-multi-cluster` Cargo feature**. The PIP-33
   two-cluster fixture moves to per-PR CI alongside the rest of the
   e2e suite.
3. **Strip `#![cfg(feature = "e2e")]`** from every
   `crates/magnetar/tests/e2e_*.rs` file. Compound gates that combine
   `e2e` with a *production-surface* feature (`auth-oauth2`,
   `encryption`, `auth-sasl-kerberos`, `experimental-v5-client`,
   `auth-athenz-zts`, `scalable-topics`) retain the production-surface
   feature and drop the `e2e` clause.
4. **Strip every `#[ignore = "e2e: ..."]` annotation** from those
   files, including:
   - `"e2e: requires Docker"` (and qualified variants like `+ libkrb5`,
     `+ transaction-coordinator-enabled broker`,
     `+ reachable host gateway`)
   - `"e2e: requires Pulsar 5.0 with PIP-460"` — the three PIP-460
     tests in `e2e_scalable_topic.rs` have stub bodies that touch a
     constant and return; with no `#[ignore]` they trivially pass.
     When upstream cuts an RC, the bodies get fleshed out per
     `docs/follow-ups.md §1`.
   - The standalone `#[ignore]` on the multi-cluster file (now ungated
     and run as part of the regular `test` job once the two-cluster
     docker-compose fixture is up).
5. **Fold the `e2e` CI job into the `test` job** in
   `.github/workflows/ci.yml`. The single `test` job runs `cargo test
   --workspace --all-features --locked` and pre-pulls the Pulsar 4.0.4
   broker image, the KDC image, and brings up the two-cluster
   docker-compose fixture before invocation.
6. **Delete `.github/workflows/e2e-replicated-subs.yml`**. PIP-33
   two-cluster coverage is now part of the per-PR `test` job.
7. **`#[ignore]` is forbidden for end-to-end tests.** The
   "environment-dependency" carve-out in ADR-0021 §1 is *removed* for
   e2e. If a test cannot run on the CI host, the fix is to provision
   the dependency on the host (the `test` job already installs
   `libkrb5-dev`, `libclang-dev`, pulls Docker images, etc.), not to
   silently skip the test at runtime. `#[ignore]` for non-e2e tests
   covered by ADR-0021's other clauses (a future GSSAPI-only test that
   genuinely cannot be provisioned, the `broker_smoke` deferral) is
   unchanged.

`testcontainers`, `wiremock`, `aws-lc-rs`, `base64`, and friends stay
where they are — unconditional `[dev-dependencies]` of the `magnetar`
crate. They are already pulled in for every `cargo test`; the
`#![cfg(feature = "e2e")]` line was the only thing preventing the
tests that consume them from being compiled and run.

The `scalable-topics` and `experimental-v5-client` features remain
"default off" per ADR-0031 / ADR-0032 because they gate the
**production surface**, not the test gate. Under `--all-features`
(the validation chain default) their e2e tests still run.

## Consequences

**Positive**

- One way to run the test suite: `cargo test --workspace
  --all-features`. No `--features e2e`, no `--include-ignored`. The
  validation chain in `CLAUDE.md` collapses to a single line.
- One Cargo profile per CI run. The duplicate `e2e` job goes away; the
  `test` job rebuilds the workspace once and runs everything against
  the live broker.
- PR-shaped broker regressions are caught per-PR instead of per-week
  (PIP-33).
- No more "is this `#[ignore]` legitimate?" code-review question for
  e2e tests — there are no `#[ignore]`s on e2e tests, full stop.

**Negative**

- Every developer's `cargo test` now requires Docker on the host. The
  pre-edit hook can't enforce this; if Docker isn't running, the
  workspace's e2e tests will fail loudly on the developer's machine.
  This is the trade-off the user has explicitly chosen — "casual"
  e2e is the goal, and the cost of Docker on the dev host is the
  cost. Devs without Docker can run `cargo test -p magnetar-proto`,
  `-p magnetar-runtime-tokio`, `-p magnetar-runtime-moonpool`,
  `-p magnetar-differential` to exercise everything below the
  network boundary.
- Per-PR CI gets a one-off cost (~60s Pulsar startup + ~60s
  two-cluster compose startup + ~10 min e2e run) on every PR. The
  existing `e2e` job already paid the single-broker cost; the only
  new cost is the two-cluster fixture.
- ADR-0021's `#[ignore]` carve-out for "environment dependency the
  build host can't satisfy" no longer applies to e2e tests. ADR-0021
  is **amended** (not superseded) — the `#[ignore]` ban for bug-hide
  reasons (§2) and the surface-and-wait protocol (§4) remain. Only the
  e2e carve-out is removed.
- ADR-0036's cost-shifting precedent is **partially superseded** for
  e2e specifically. The moonpool seed sweep and the diff-shaped xtask
  gates still follow ADR-0036; the e2e suite no longer does.
- ADR-0034 (e2e-multi-cluster scoping) is **partially superseded**:
  the Cargo feature goes away; the docker-compose fixture and the
  test file are unchanged.

**Neutral**

- The PIP-460 stub tests in `e2e_scalable_topic.rs` trivially pass.
  They consume ~10ms of e2e wall time each and serve as
  named-test-name placeholders until upstream PIP-460 ships. When
  `docs/follow-ups.md §1` unblocks, the bodies get fleshed out in the
  same commit as the proto re-vendor.
- The `magnetar-runtime-tokio` proxy parity claim in
  `docs/parity-status.md` + `README.md` is corrected in the same
  changeset (audit finding — moonpool returns
  `ProxyUnsupportedOnUnsupervisedClient`, not ✅). The moonpool
  `ProxyConnectionPool` gap is added to `docs/follow-ups.md §3` as a
  new actionable item.

## Alternatives considered

- **Keep `#![cfg(feature = "e2e")]`, remove only `#[ignore]`**. This
  was the earlier draft of this ADR before the user clarified intent.
  Rejected: the compile-time gate is the part the user explicitly
  asked to remove ("no compilation feature flags"), and keeping it
  would preserve the two-invocation problem.
- **Keep `#[ignore]`, remove only the feature flag**. Rejected: the
  user explicitly asked to remove both ("nor ignore"). It would also
  reintroduce the two-step opt-in (`cargo test ... --
  --include-ignored`).
- **Keep both, fold the CI job only**. Rejected for the same reason.
- **Keep `e2e-multi-cluster` as a weekly schedule**. Rejected: the
  point of folding e2e into per-PR is to catch broker regressions on
  the PR that introduces them, and PIP-33 regressions are exactly
  that. Paying ~60s of compose startup per PR is acceptable.
- **Probe-and-skip pattern for PIP-460 tests** (runtime
  `if !broker_supports_pip_460() { return Ok(()); }`). Rejected: the
  test bodies are stubs that already pass trivially. A runtime probe
  would just be an alternate spelling of `#[ignore]` and pulls in a
  capability-detection layer for zero benefit.

## References

- [ADR-0021](0021-no-silent-test-ignore-or-remove.md) — **amended** by
  this ADR. The bug-hide ban + surface-and-wait protocol remain; the
  e2e carve-out is removed.
- [ADR-0034](0034-pip-33-replicated-subscriptions-scope.md) —
  **partially superseded** by this ADR. The `e2e-multi-cluster` Cargo
  feature is removed; the docker-compose fixture is unchanged.
- [ADR-0036](0036-moonpool-seed-sweep-daily-random.md) — cost-shifting
  precedent **partially superseded** for e2e specifically. The
  moonpool seed sweep and diff-shaped xtask gates still follow
  ADR-0036.
- [ADR-0031](0031-pip-460-scalable-subscription-scope.md) — PIP-460
  scope unchanged; the stub `e2e_scalable_topic.rs` tests now run
  (and trivially pass) on every PR.
- `crates/magnetar/Cargo.toml` — `e2e` + `e2e-multi-cluster` features
  deleted by this ADR.
- `.github/workflows/ci.yml` — `e2e` job folded into `test`; Docker +
  KDC pre-pulls + two-cluster compose setup added to `test`.
- `.github/workflows/e2e-replicated-subs.yml` — deleted by this ADR.
- `docs/follow-ups.md` — §1 (PIP-460 e2e) rewritten; new §3
  (moonpool `ProxyConnectionPool`) added.
- `docs/parity-status.md`, `README.md` — moonpool proxy parity claim
  corrected (audit finding).
- Project `CLAUDE.md` — validation chain collapsed; invariant #8
  (no silent `#[ignore]`) text updated to reflect the e2e ban.
