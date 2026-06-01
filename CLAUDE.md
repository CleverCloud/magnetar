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

The user-visible parity target is the Apache Pulsar Java client.
The parity matrix lives in [`README.md#java-client-parity-matrix`](README.md).

## Workspace layout

```
crates/
  magnetar/                       — top-level façade + PulsarClient<E> + builders
  magnetar-admin/                 — reqwest-backed REST admin client
  magnetar-auth-athenz/           — Athenz auth scaffold
  magnetar-auth-oauth2/           — OAuth2 ClientCredentialsFlow
  magnetar-auth-sasl/             — SASL PLAIN + Kerberos/GSSAPI (libgssapi behind `kerberos` feature, ADR-0029)
  magnetar-cli/                   — `magnetar` binary
  magnetar-differential/          — tokio ↔ moonpool differential harness (test-only)
  magnetar-fakes/                 — in-process broker stub for tests
  magnetar-messagecrypto/         — PIP-4 AES-GCM
  magnetar-proto/                 — sans-io state machine + codec + trackers
  magnetar-runtime-tokio/         — production engine (default)
  magnetar-runtime-moonpool/      — deterministic-simulation engine (carries the PIP-4 crypto bridge, ADR-0044)
xtask/                            — workspace automation
```

## Non-negotiable invariants

These come from [`GUIDELINES.md`](GUIDELINES.md); read it once per session.

1. **No channels.** `tokio::sync::{mpsc,broadcast,watch,oneshot}`, `std::sync::mpsc`, `crossbeam-channel`, `flume`, `async-channel`, `kanal`, `postage`, `tachyonix`, `thingbuf` — banned everywhere.
   Replace with `Arc<parking_lot::Mutex<...>>` + `tokio::sync::Notify` + `core::task::Waker` slabs inside the state machine.
   ([ADR-0003](specs/adr/0003-no-channels-rule.md))
2. **`magnetar-proto` has zero I/O deps.** No `tokio`, no `mio`, no `socket2`, no `async-trait`.
   Enforced via `cargo run -p xtask -- check-no-io-deps`.
   ([ADR-0004](specs/adr/0004-sans-io-protocol-core.md))
3. **Sans-io clock injection.** `Instant` is passed in via `now: Instant` parameters on every user-driven entry; `SystemTime` via the `wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>` provider.
   Engines snapshot the host clocks at the call site; moonpool plugs in virtual clocks.
   Two documented leaks remain (uuid in chunked emit, env::var in `TokenAuth` bootstrap); both are listed in [`ARCHITECTURE.md`](ARCHITECTURE.md#known-non-determinism-leaks-documented) and allowlisted in `xtask`.
   ([ADR-0011](specs/adr/0011-clock-injection-sans-io.md))
4. **CRC32C verify or drop.** Frames with magic `0x0e01` must pass CRC32C; mismatch → `ChecksumMismatch` event + drop.
5. **`rustls` only.** No `native-tls`.
   `openssl` / `openssl-sys` are admitted only as transitive deps of `rustls-openssl` under the `crypto-openssl` feature ([ADR-0035](specs/adr/0035-pluggable-crypto-provider.md)); the active rustls crypto provider is picked at compile time via the façade's mutually-pluggable `crypto-aws-lc-rs` (default) / `crypto-ring` / `crypto-openssl` / `crypto-fips` features.
   Enforced via `deny.toml`'s scoped `wrappers = ["rustls-openssl"]` carve-out.
   ([ADR-0005](specs/adr/0005-rustls-only-tls.md) amended, [ADR-0035](specs/adr/0035-pluggable-crypto-provider.md))
6. **No panics in `magnetar-proto`** except inside `#[cfg(test)]`.
   All code paths return `Result` or `Option`.
7. **Schema canonicalisation.** AVRO/JSON/PROTOBUF go through the broker canonical form; PROTOBUF_NATIVE + KeyValue must be byte-identical to Java output.
8. **No silent `#[ignore]`.** Tests are fixed, not papered over.
   E2e tests carry **no `#[ignore]` and no compile-time feature gate** — they run on every `cargo test --all-features` and on every CI push ([ADR-0046](specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) supersedes ADR-0021's env-dep carve-out for e2e).
   `#[ignore]` for the bug-hide cases ADR-0021 §2 covers is still forbidden; the surface-and-wait protocol (ADR-0021 §4) is unchanged.
   ([ADR-0021](specs/adr/0021-no-silent-test-ignore-or-remove.md), [ADR-0046](specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md))
9. **Cross-runtime test + coverage policy.** Every behavioral change (runtime behavior, public API, wire format) and every change inside `magnetar-proto` ships with **all four** test layers in the same commit: (a) `magnetar-proto` unit test, (b) `magnetar-runtime-tokio` integration test, (c) `magnetar-runtime-moonpool` integration test, (d) `magnetar-differential` equivalence test asserting tokio ↔ moonpool `EventStream` parity, plus an end-to-end test under `crates/magnetar/tests/e2e_*.rs`.
   Moonpool sim coverage is **100% on the diff** (`cargo run -p xtask -- check-sim-coverage`, `cargo-llvm-cov` patch-coverage style).
   `magnetar-runtime-tokio` and `magnetar-runtime-moonpool` keep a **strict 1:1 test count** (`cargo run -p xtask -- check-runtime-test-parity`).
   Both checks are hard-failing in the local + CI validation chain.
   Exemptions: docs-only, comment-only, formatter-only, and dependency bumps with no functional impact — justify in the commit message.
   ([ADR-0024](specs/adr/0024-cross-runtime-test-and-coverage-policy.md))
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
> Its `delocate` step requires clang-emitted assembly — gcc 16+ (Fedora 44 default) emits `.data.rel.ro.local` sections that delocate rejects.
> Prefix the build / test / clippy commands below with `CC=clang CXX=clang++ ASM=clang AR=llvm-ar RANLIB=llvm-ranlib` on Linux.
> `cargo run -p xtask -- check-crypto-matrix` sets these automatically for its `crypto-fips` cells.

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
    --all-features --locked -- --quiet \
    || { echo "seed $seed FAILED"; exit 1; }
done
cargo deny check
RUSTDOCFLAGS="-D warnings --cfg tokio_unstable --cfg tracing_unstable" \
  cargo doc --workspace --all-features --no-deps --locked
# xtask gates — invoke via `cargo run -p xtask --` (there is no `cargo xtask` alias).
cargo run -p xtask -- check-no-channels         # banned-channel grep
cargo run -p xtask -- check-no-io-deps          # magnetar-proto = zero I/O deps
cargo run -p xtask -- check-no-internal-clock   # Instant::now() / SystemTime::now() outside the allowlist
cargo run -p xtask -- codegen --check           # proto codegen drift
cargo run -p xtask -- check-sim-coverage        # 100% moonpool coverage on diff (ADR-0024)
cargo run -p xtask -- check-runtime-test-parity # tokio ↔ moonpool 1:1 test count (ADR-0024)
cargo run -p xtask -- check-crypto-matrix       # per-provider build matrix (ADR-0035)
cargo run -p xtask -- check-known-failing-seeds # replay registry seeds (ADR-0047) — mirrors the per-PR `seed-replay` CI job
```

Per [ADR-0046](specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md) the e2e suite is **already included** in `cargo test --workspace --all-features` above (no separate command, no `--features e2e`, no `--include-ignored`).
The local run still needs Docker + `apachepulsar/pulsar:4.0.4` reachable.
The PIP-33 two-cluster tests additionally require the `crates/magnetar/tests/fixtures/docker-compose.replicated-subs.yml` fixture to be up before `cargo test` — CI brings it up automatically.

The auto-format hook handles `cargo fmt` / `gofmt` / `ruff format` on edited files; lints and tests stay manual.

The three heavy / diff-shaped xtask gates (`check-sim-coverage`, `check-runtime-test-parity`, `check-crypto-matrix`) are local-first but also run in CI via the scheduled [`.github/workflows/xtask-gates.yml`](.github/workflows/xtask-gates.yml) (daily cron + `workflow_dispatch`), which keeps per-PR [`ci.yml`](.github/workflows/ci.yml) fast.
`check-sim-coverage` is a diff gate, so its scheduled `main` run short-circuits ("nothing to verify"); dispatch it from a feature branch for real patch-coverage gating.

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
