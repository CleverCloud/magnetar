# CLAUDE.md — magnetar workspace memory

Quick orientation for Claude when working in this repo.
Stacks additively on top of `~/.claude/CLAUDE.md` and `GUIDELINES.md`; when they disagree, `GUIDELINES.md` (the binding spec) wins.

## What this is

**Magnetar** is a from-scratch Apache Pulsar client driver in Rust.
The architecture is **sans-io + multi-engine**:

- `magnetar-proto` — pure state machine.
  No I/O, no `tokio`, no `async`, no sockets.
  `quinn-proto`-style API: `handle_bytes`, `poll_transmit`, `poll_event`, `poll_timeout`.
- `magnetar-runtime-tokio` — production tokio engine.
- `magnetar-runtime-moonpool` — deterministic-simulation engine over `moonpool_core::Providers`.
- `magnetar` — top-level façade.
  `PulsarClient<E: Engine = TokioEngine>` is generic over an `Engine` marker trait that selects per-engine storage.
  Engine-specific methods live in concrete `impl PulsarClient<TokioEngine>` / `impl PulsarClient<MoonpoolEngine<P>>` blocks.
  Published on crates.io as **`magnetar-driver`** (the `magnetar` name is taken); the library/import name is still `magnetar`, so `use magnetar::*` is unchanged.
  The CLI ships as **`magnetarctl`** (binary command `magnetarctl`).
  See [ADR-0067](specs/adr/0067-publish-facade-as-magnetar-driver-cli-as-magnetarctl.md).

The user-visible parity target is the Apache Pulsar Java client.
The parity matrix lives in [`README.md#java-client-parity-matrix`](README.md).

## Workspace layout

The workspace ships 12 crates plus `xtask/`.
See [`ARCHITECTURE.md#crate-topology`](ARCHITECTURE.md#crate-topology) for the full breakdown — the canonical listing of crate roles, layering constraints, and which crates depend on which.
The two engines (`magnetar-runtime-tokio`, `magnetar-runtime-moonpool`) sit behind the `Engine` trait selected on `PulsarClient<E>`; sans-io state lives in `magnetar-proto` (zero I/O deps, ADR-0004).

## Non-negotiable invariants

These are the **workspace-wide rules**. The protocol-correctness subset (CRC32C, no-panics-in-proto, schema parity, etc.) overlaps with [`GUIDELINES.md`](GUIDELINES.md), which is the binding spec for wire-format and code rules; the test-policy + lock-ordering items here are workspace-process additions enforced via xtask.

1. **No channels.** `tokio::sync::{mpsc,broadcast,watch,oneshot}`, `std::sync::mpsc`, `crossbeam-channel`, `flume`, `async-channel`, `kanal`, `postage`, `tachyonix`, `thingbuf` — banned everywhere.
   Replace with `Arc<parking_lot::Mutex<...>>` + `tokio::sync::Notify` + `core::task::Waker` slabs inside the state machine.
   ([ADR-0003](specs/adr/0003-no-channels-rule.md))
2. **`magnetar-proto` has zero I/O deps.** No `tokio`, no `mio`, no `socket2`, no `async-trait`.
   Enforced via `cargo run -p xtask -- check-no-io-deps`.
   ([ADR-0004](specs/adr/0004-sans-io-protocol-core.md))
3. **Sans-io clock injection.** `Instant` is passed in via `now: Instant` parameters on every user-driven entry; `SystemTime` via the `wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>` provider.
   Engines snapshot the host clocks at the call site; moonpool plugs in virtual clocks.
   `Instant::now()`, `SystemTime::now()` and `.elapsed()` are all forbidden in `magnetar-proto/src/**` outside `#[cfg(test)]`, with **no** file allowlist; `Instant` arithmetic goes through `saturating_duration_since` / `crate::time::deadline_with_clamp` so it cannot panic (invariant #6).
   Two documented **non-time** leaks remain (uuid in chunked emit, env::var in `TokenAuth` bootstrap); both are listed in [`ARCHITECTURE.md`](ARCHITECTURE.md#known-non-determinism-leaks-documented), which is their sole inventory — the gate has never scanned for either.
   ([ADR-0011](specs/adr/0011-clock-injection-sans-io.md), [ADR-0086](specs/adr/0086-inject-now-into-proto-latency-recording.md))
4. **CRC32C verify or drop.** Frames with magic `0x0e01` must pass CRC32C; mismatch → `ChecksumMismatch` event + drop.
5. **`rustls` only.** No `native-tls`.
   `openssl` / `openssl-sys` are admitted only as transitive deps of `rustls-openssl` under the `crypto-openssl` feature ([ADR-0035](specs/adr/0035-pluggable-crypto-provider.md)); the active rustls crypto provider is picked at compile time via the façade's mutually-pluggable `crypto-aws-lc-rs` (default) / `crypto-ring` / `crypto-openssl` / `crypto-fips` features.
   Enforced via `deny.toml`'s scoped `wrappers = ["rustls-openssl"]` carve-out.
   ([ADR-0005](specs/adr/0005-rustls-only-tls.md) amended, [ADR-0035](specs/adr/0035-pluggable-crypto-provider.md))
6. **No panics in `magnetar-proto`** except inside `#[cfg(test)]`.
   All code paths return `Result` or `Option`.
7. **Schema canonicalisation.** AVRO/JSON/PROTOBUF go through the broker canonical form; PROTOBUF_NATIVE + KeyValue must be byte-identical to Java output.
8. **No silent `#[ignore]`.** Tests are fixed, not papered over.
   E2e tests carry **no `#[ignore]` and no dedicated `e2e` feature**; feature-specific targets still use their owning product feature, and `cargo test --all-features` activates all of them on every CI push ([ADR-0046](specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) supersedes ADR-0021's env-dep carve-out for e2e).
   `#[ignore]` for the bug-hide cases ADR-0021 §2 covers is still forbidden; the surface-and-wait protocol (ADR-0021 §4) is unchanged.
   ([ADR-0021](specs/adr/0021-no-silent-test-ignore-or-remove.md), [ADR-0046](specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md))
9. **Cross-runtime test + coverage policy.** Every behavioral change (runtime behavior, public API, wire format) and every change inside `magnetar-proto` ships with **all four** test layers in the same commit: (a) `magnetar-proto` unit test, (b) `magnetar-runtime-tokio` integration test, (c) `magnetar-runtime-moonpool` integration test, (d) `magnetar-differential` equivalence test asserting tokio ↔ moonpool `EventStream` parity, plus an end-to-end test under `crates/magnetar/tests/e2e_*.rs`.
   Moonpool sim coverage is **100% on the diff** (`cargo run -p xtask -- check-sim-coverage`, `cargo-llvm-cov` patch-coverage style).
   Its LCOV report covers exactly eight crates the run compiles: `magnetar-proto`, `magnetar-runtime-tokio`, `magnetar-runtime-moonpool`, `magnetar-differential`, `magnetar-auth-athenz`, `magnetar-auth-sasl`, `magnetar-driver` (directory `crates/magnetar`), and `magnetar-fakes`.
   The original six-crate widening measured 63 `SF:` records on 2026-07-31 (`magnetar-proto` 28, `magnetar-runtime-tokio` 12, `magnetar-runtime-moonpool` 12, `magnetar-auth-athenz` 5, `magnetar-differential` 4, `magnetar-auth-sasl` 2); ADR-0098 adds the façade and fakes through `magnetar-differential`'s public aggregate tests without inventing a later record total. The measurement runs at `opt-level = 0` ([ADR-0094](specs/adr/0094-measure-sim-coverage-unoptimized.md)), overriding the workspace's `[profile.test] opt-level = 1` for its duration.
   Above zero the MIR inliner folds a callee into its caller and the callee's coverage counter never fires, so the report shows zero hits for a line a passing test provably executed — and, in the other direction, can credit a line that never ran.
   Because inlining follows codegen-unit partitioning that verdict was not even stable: the same commit produced 63, 70 and 81 `SF:` records warm, cold and on CI, with three different uncovered sets, which is why the record counts above are historical rather than a target to reproduce.
   Execution is unchanged and still runs only the `magnetar-runtime-moonpool` + `magnetar-differential` test binaries, so a `magnetar-proto` or `magnetar-runtime-tokio` line counts as covered only when a sim test reaches it transitively; those crates' own unit tests never run under the gate and can never satisfy it.
   Execution and report share one locked, invocation-owned Cargo/llvm-cov target outside the cached target and build trees on the configured build filesystem; `target/sim-coverage.lcov` is output-only, injected LLVM artifact flags are rejected, and no first-party profile or object input is reused across passes ([ADR-0096](specs/adr/0096-isolate-sim-coverage-current-pass-artifacts.md)).
   **The uncovered-line verdict is enforced**: `SIM_COVERAGE_ENFORCES_UNCOVERED` is `true`, so an uncovered added line inside the reported scope is printed in full with a count and **fails** the check.
   It ran advisory from ADR-0090 until [ADR-0092](specs/adr/0092-enforce-sim-coverage-and-gate-every-pull-request.md), which flipped the constant and gave the gate a per-PR home in the same changeset — a flip alone would have changed nothing, because the scheduled `main` run diffs `main` against itself and short-circuits with "nothing to verify".
   `.github/workflows/ci.yml` now runs `check-sim-coverage --enforce` on every `pull_request`, so the verdict reaches every change rather than only a local run — though `main` carries no branch protection as of 2026-08-01, so a red job does not yet mechanically block a merge (ADR-0092 § Required check records the admin step that would).
   `--enforce` is redundant since that flip — it only ORs into the constant — and is retained so existing invocations keep working and so the CI job states its own intent.
   A second case hard-fails at any setting: a gated file with no `SF:` record when its crate is wholly absent or the file contains a non-test function body, which means the gate could not measure rather than that a test is merely missing. A module/export/constant/bodyless-declaration-only file remains advisory because it has no executable mapping.
   The façade library and `magnetar-fakes` are now measured because `magnetar-differential` declares both as dev-dependencies and its tests exercise the public aggregate; the façade's Docker-bound `crates/magnetar/tests/e2e_*.rs` targets still do not run because execution remains limited to the two roots above.
   What stays ungated is everything those roots never compile — `magnetar-admin`, `magnetarctl`, `magnetar-auth-oauth2`, `magnetar-messagecrypto`, and similar packages — plus generated `crates/magnetar-proto/src/pb/`, which is excluded outright.
   Those additions print as `not gated` and do **not** fail the check, `--enforce` included; the ungated report is a scope limit, not a verdict ([ADR-0088](specs/adr/0088-sim-coverage-gate-scope-report-ungated-additions.md), [ADR-0090](specs/adr/0090-widen-sim-coverage-report-to-compiled-closure.md), [ADR-0098](specs/adr/0098-assignment-driven-m1-hardened-stream-consumer.md)).
   `magnetar-runtime-tokio` and `magnetar-runtime-moonpool` keep a **strict 1:1 test count** (`cargo run -p xtask -- check-runtime-test-parity`).
   Both `check-runtime-test-parity` and `check-sim-coverage` are hard-failing in the local + CI validation chain.
   Exemptions: docs-only, comment-only, formatter-only, and dependency bumps with no functional impact — justify in the commit message.
   The gate excludes `xtask/`, `.github/`, `docs/`, `specs/`, `tasks/`, `.claude/`, `crates/magnetar-proto/src/pb/` and every `/tests/`, `/benches/`, `/examples/` path, so a change confined to those short-circuits before it builds anything.
   ([ADR-0024](specs/adr/0024-cross-runtime-test-and-coverage-policy.md), [ADR-0088](specs/adr/0088-sim-coverage-gate-scope-report-ungated-additions.md), [ADR-0090](specs/adr/0090-widen-sim-coverage-report-to-compiled-closure.md), [ADR-0092](specs/adr/0092-enforce-sim-coverage-and-gate-every-pull-request.md), [ADR-0096](specs/adr/0096-isolate-sim-coverage-current-pass-artifacts.md), [ADR-0098](specs/adr/0098-assignment-driven-m1-hardened-stream-consumer.md))
10. **Lock-ordering: global → per-slot, never the reverse.** `Connection` is wrapped in a `parking_lot::Mutex` by the runtime engines; every `ProducerSlot` / `ConsumerSlot` carries its own `parking_lot::Mutex`.
    A holder of `slot.state.lock()` MUST NOT then take the connection-wide mutex.
    The hot path (`Producer::send` → `ProducerSlot::queue_send`) takes only the per-slot mutex; the driver merges per-slot staged frames into the connection buffer under the global lock via `poll_transmit`.
    The reverse acquisition order deadlocks under contention.
    ([ADR-0038](specs/adr/0038-split-connection-mutex.md))

## Workflow

Always use `wt` for edits.
The pre-edit hook blocks direct work on `main`/`master`/`trunk`/`develop`.

```
wt switch --create feat/<scope> -y
# edit
wt step diff -- --stat
wt merge -y     # after Florentin confirms
```

Conventional commits, signed-off + GPG-signed:

```
git commit -s -S -m "feat(scope): subject"
```

No "Generated by Claude" trailers.
Anywhere.
Ever.
([ADR-0012](specs/adr/0012-no-claude-attribution.md))

## Markdown style

All `*.md` files in the repo are formatted with **Prettier** and follow **semantic line breaks** (one sentence per line, no column limit).

- Config: [`.prettierrc.json`](.prettierrc.json) — `proseWrap: preserve` + `printWidth: 100000`.
  Prettier never re-wraps paragraphs; it only normalises code blocks, tables, links, and emphasis style.
- Ignore: [`.prettierignore`](.prettierignore) — excludes `target/`, `Cargo.lock`, `node_modules/`, and `AGENTS.md` (symlink to `CLAUDE.md`).
- One-shot reformat: [`scripts/markdown-sembr.py`](scripts/markdown-sembr.py) joins hard-wrapped paragraphs / list items / blockquotes and re-splits at sentence boundaries.
  Run it on edited files when adding new prose, then `prettier --write` to normalise the rest.

Authoring rules:

- Write one sentence per line.
  Long sentences stay on one long line — there is no 80-column hard limit.
- Backtick `snake_case` identifiers (function names, filenames) when they sit next to italic emphasis on the same line.
  Prettier's emphasis normaliser is non-idempotent on `*italic*` adjacent to `snake_case` underscores in plain prose; backticking the identifier sidesteps it (this is how `ARCHITECTURE.md:422` and `specs/adr/0050-swizzle-clog-workload.md:18` are written).
- Code fences, YAML frontmatter, tables, headings, horizontal rules, HTML blocks, and reference-link definitions are passed through untouched by both the script and Prettier.

Validation:

```
find . -name '*.md' -not -path './target/*' -not -path './.git/*' -not -name AGENTS.md \
  -print0 | xargs -0 npx prettier --check
```

## Validation chain

Run before declaring a task done (in this order):

> **Linux + FIPS note**: every `--all-features` command pulls in `crypto-fips`, which builds `aws-lc-fips-sys`.
> Its `delocate` step requires clang-emitted assembly, and a gcc-emitted `.data.rel.ro.local` section aborts it with `.data section found in module`.
> That is not a gcc-version threshold: it reproduced on gcc 14.4.0 on 2026-07-31, and whether a given build trips it depends on which aws-lc sources cargo's feature unification pulls into `bcm.c`.
> A Linux FIPS build therefore pins the C/asm toolchain to clang whatever the host gcc version is.
> Prefix the build / test / clippy commands below with `CC=clang CXX=clang++ ASM=clang AR=llvm-ar RANLIB=llvm-ranlib` on Linux.
> `cargo run -p xtask -- check-crypto-matrix` (for its `crypto-fips` cells) and `cargo run -p xtask -- check-sim-coverage` (for its `--all-features` coverage run) set those five variables themselves, so neither takes the manual prefix.

```
cargo +nightly fmt --all
cargo build --workspace --all-features
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features
# Moonpool seed sweep — catches seed-dependent flakiness in the
# deterministic-simulation suite. Local-only per ADR-0036 (fixed seeds
# in per-PR CI were wasted compute since each (commit, seed) pair is
# bit-for-bit reproducible). CI runs a 128-random-seed sweep daily in
# `.github/workflows/moonpool-seed-sweep.yml`.
for seed in $(seq 1 32); do
  MOONPOOL_SEED=$seed cargo test -p magnetar-runtime-moonpool \
    --no-default-features --features crypto-aws-lc-rs \
    --locked -- --quiet \
    || { echo "seed $seed FAILED"; exit 1; }
done
cargo deny check
RUSTDOCFLAGS="-D warnings" \
  cargo doc --workspace --all-features --no-deps --locked
# xtask gates — invoke via `cargo run -p xtask --` (there is no `cargo xtask` alias).
cargo run -p xtask -- check-no-channels         # banned-channel grep
cargo run -p xtask -- check-no-io-deps          # magnetar-proto = zero I/O deps
cargo run -p xtask -- check-no-internal-clock   # no Instant::now() / SystemTime::now() / .elapsed() in proto
cargo run -p xtask -- check-log-fields          # error!/warn!/info! carry ≥1 structured field (ADR-0054)
cargo run -p xtask -- check-e2e-container-memory # every Pulsar e2e container caps PULSAR_MEM (docs/testing.md)
cargo run -p xtask -- codegen --check           # proto codegen drift
cargo run -p xtask -- check-sim-coverage        # patch coverage on diff over the 8 crates the sim run compiles; ENFORCING — an uncovered added line fails (ADR-0024, ADR-0088, ADR-0090, ADR-0092, ADR-0098)
cargo run -p xtask -- check-runtime-test-parity # tokio ↔ moonpool 1:1 test count (ADR-0024)
cargo run -p xtask -- check-crypto-matrix       # per-provider build matrix (ADR-0035)
# (known-failing seed replay runs in CI via the per-PR `seed-replay` job; ADR-0047)
```

Per [ADR-0046](specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) the e2e suite is **already included** in `cargo test --workspace --all-features` above (no dedicated `e2e` feature and no `--include-ignored`; owning product features such as `scalable-topics` still gate their targets).
The local run still needs Docker; stable targets use suite-specific Pulsar 4.x images and the scalable target uses both `apachepulsar/pulsar:5.0.0-M1` and `apachepulsar/pulsar:4.0.4`.
The PIP-33 two-cluster tests additionally require the `crates/magnetar/tests/fixtures/docker-compose.replicated-subs.yml` fixture to be up before `cargo test` — CI brings it up automatically.
Locally that is **two** steps, not one: `docker compose -f docker-compose.replicated-subs.yml up -d` **and then** `./configure_replicated_subs.sh`.
`up -d` alone leaves both brokers healthy but not registered as each other's peers, and the replicated-subscription tests then have nothing to replicate between.

The auto-format hook handles `cargo fmt` / `gofmt` / `ruff format` on edited files; lints and tests stay manual.

Two of the three heavy / diff-shaped xtask gates (`check-runtime-test-parity`, `check-crypto-matrix`) are local-first but also run in CI via the scheduled [`.github/workflows/xtask-gates.yml`](.github/workflows/xtask-gates.yml) (daily cron + `workflow_dispatch`), which keeps per-PR [`ci.yml`](.github/workflows/ci.yml) fast.
`check-sim-coverage` runs in both places: [ADR-0092](specs/adr/0092-enforce-sim-coverage-and-gate-every-pull-request.md) added a per-PR `check-sim-coverage` job to `ci.yml`, and the scheduled copy stays for dispatching against a branch with no PR open.
It is a diff gate, so its scheduled `main` run short-circuits ("nothing to verify"), and so does any PR whose added production `.rs` lines are all excluded — that is why a job this heavy can sit on every pull request.
The bail is keyed on the exclusion lists, **not** on the gated crates: a PR touching only `magnetar-admin`, `magnetarctl`, or another uncompiled package still pays the full instrumented build and then prints those files as advisory `not gated`; façade and fakes changes are now gated through the differential aggregate tests (ADR-0098).
Every invocation now enforces, the chain entry above included; `--enforce` is redundant and kept only as an explicit override.

## Common slash workflows

Project layers on top of the global skill set:

| Command      | Use                                            |
| ------------ | ---------------------------------------------- |
| `/ask`       | Strategic / architectural questions.           |
| `/search`    | "Where is X?" / cross-file lookups.            |
| `/review`    | Review a branch or PR.                         |
| `/audit`     | Final pre-merge audit pass.                    |
| `/triage`    | New issue or stack-trace triage.               |
| `/loop`      | Recurring or self-paced background work.       |
| `/commit`    | Conventional + signed-off + GPG-signed commit. |
| `/worktrunk` | Worktree maintenance.                          |

For 4+ parallel agents, use the **supervisor pattern** — one `guidelines:supervisor` tracks progress, validates against source, and retries failed sub-agents up to 2× (`CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1`).

## Reading order for a new session

1. This file.
2. [`GUIDELINES.md`](GUIDELINES.md) — binding rules.
3. [`ARCHITECTURE.md`](ARCHITECTURE.md) — sans-io rationale, driver loop, protocol state machine, schemas, trackers.
4. [`README.md`](README.md) — public-facing usage + parity matrix.
5. The crate you're working in — start at its `lib.rs`.

## Documentation + ADRs

[`docs/`](docs/) — reference documentation, indexed at [`docs/README.md`](docs/README.md).
The load-bearing ones for everyday work:

- Architecture: [`ARCHITECTURE.md`](ARCHITECTURE.md) (Overview section is the 10-minute read), [`memory-limit.md`](docs/memory-limit.md), [`moonpool-engine.md`](docs/moonpool-engine.md).
- Testing + simulation: [`testing.md`](docs/testing.md), [`moonpool-engine.md`](docs/moonpool-engine.md) (engine surface + appendix on TigerBeetle / FDB patterns).
- Status + roadmap: [`README.md#java-client-parity-matrix`](README.md#java-client-parity-matrix) (canonical parity matrix + engine-by-engine coverage), [`follow-ups.md`](docs/follow-ups.md).
- PIP features + auth: [`pip-features.md`](docs/pip-features.md) (V5 / PIP-466, shadow-topics / PIP-180, replicated-subs / PIP-33, scalable-topics / PIP-460 experimental, Athenz), [`cli.md`](docs/cli.md).

[`specs/adr/`](specs/adr/) — Architecture Decision Records, one binding decision per file.
Index at [`specs/README.md`](specs/README.md).
When you change a load-bearing decision, add the corresponding ADR in **the same** changeset that lands the code, and update the index in [`specs/README.md`](specs/README.md).
Old ADRs flip to `Superseded by ADR-NNNN`; they are never edited in place.

This repo has no production credentials, no broker URLs, no PII; do not add any.
The e2e suite runs against a local container.
