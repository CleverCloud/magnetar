# Contributing to magnetar

## Toolchain

- Rust **stable ≥ 1.91** (edition 2024; [ADR-0079](specs/adr/0079-raise-msrv-to-rust-1-91.md)).
  `Cargo.toml` declares the minimum; `rust-toolchain.toml` selects the rolling stable toolchain for development.
- Rust **nightly** is needed only for `cargo +nightly fmt` (unstable rustfmt features).
- **`protoc` ≥ 3.19** if you re-run `xtask codegen`.
  End users don't need it — generated code is committed to `crates/magnetar-proto/src/pb/`.
- **`cargo-deny`**, **`cargo-mutants`** for the relevant CI jobs.
  Install via `cargo install`.

## Validation chain

Before opening a PR:

Pick a routine feature subset that pulls in every magnetar facet EXCEPT `crypto-fips` and `auth-sasl-kerberos` (those need native FIPS / Kerberos toolchains that not every contributor has installed):

```
FEATURES="tokio,moonpool,admin,auth-oauth2,auth-sasl,auth-athenz,auth-athenz-zts,encryption,experimental-v5-client,scalable-topics,crypto-aws-lc-rs"

cargo +nightly fmt --check
cargo clippy --workspace --no-default-features --features "$FEATURES" --all-targets -- -D warnings
cargo build --workspace --no-default-features --features "$FEATURES"
cargo test --workspace --no-default-features --features "$FEATURES" --locked
cargo deny check
RUSTDOCFLAGS="-D warnings" \
  cargo doc --workspace --no-default-features --features "$FEATURES" --no-deps --locked
cargo run -p xtask -- check-no-channels         # banned-channel grep (ADR-0003)
cargo run -p xtask -- check-no-io-deps          # magnetar-proto = zero I/O deps (ADR-0004)
cargo run -p xtask -- check-no-internal-clock   # no host-clock reads in proto (ADR-0011, ADR-0086)
cargo run -p xtask -- codegen --check           # proto codegen drift
cargo run -p xtask -- check-sim-coverage        # patch coverage over the 8 sim-compiled crates; enforcing (ADR-0024, ADR-0088, ADR-0090, ADR-0092, ADR-0102)
cargo run -p xtask -- check-runtime-test-parity # tokio ↔ moonpool 1:1 test count (ADR-0024)
cargo run -p xtask -- check-crypto-matrix       # per-provider build matrix incl. FIPS (ADR-0035)
```

`check-crypto-matrix` exhaustively covers ALL providers — including `crypto-fips` — in a controlled CI environment where the native build toolchain is available.
Contributors with a FIPS toolchain locally can substitute `--all-features` for `--no-default-features --features "$FEATURES"` above.
Per-package invocations (`cargo test -p <crate>`) need an explicit crypto feature because dependency features don't transitively activate under `-p`.

`check-sim-coverage` executes only the `magnetar-runtime-moonpool` + `magnetar-differential` test binaries but reports over exactly eight crates: the original six plus `magnetar-driver` (directory `crates/magnetar`) and `magnetar-fakes`.
The differential crate's dev-dependencies and public aggregate tests compile and exercise the façade and fakes without running the façade's Docker e2e targets; the two scopes are spelled out in `GUIDELINES.md#cross-runtime-test--coverage-policy`.
Its uncovered-line verdict is **enforcing** ([ADR-0092](specs/adr/0092-enforce-sim-coverage-and-gate-every-pull-request.md)): an uncovered added line is printed with a count and fails the check, and the same job runs on every pull request in [`ci.yml`](.github/workflows/ci.yml).
Note what it can and cannot be satisfied by: only the moonpool and differential binaries execute, so covering a `magnetar-proto` or `magnetar-runtime-tokio` line means writing a sim or equivalence test that reaches it — that crate's own unit tests never run under this gate.
`--enforce` is redundant now and accepted for compatibility.
An added gated file with no `SF:` record fails when its whole crate emitted no records or when that file contains a non-test function body, even if siblings reported; module/export/constant/bodyless-declaration-only files remain advisory.
A diff confined to `xtask/`, `.github/`, `docs/`, `specs/`, `tasks/`, `.claude/`, `crates/magnetar-proto/src/pb/`, any `/tests/`, `/benches/`, `/examples/` path, or inside a `#[cfg(test)]` span short-circuits with "nothing to verify" and never pays the build.
That bail is keyed on those exclusions and not on the gated crates, so a PR touching only `magnetar-admin`, `magnetarctl`, `magnetar-auth-oauth2`, `magnetar-messagecrypto`, or another uncompiled package does pay the full build before printing it as `not gated`; façade and fakes changes are gated under ADR-0102.
It builds with `--all-features`, so `crypto-fips` and its `aws-lc-fips-sys` build come along.
On Linux the gate applies `CC=clang CXX=clang++ ASM=clang AR=llvm-ar RANLIB=llvm-ranlib` to that build itself — the same toolchain `check-crypto-matrix` sets for its FIPS cells — so no command prefix is needed, but clang and the LLVM binutils must be installed: aws-lc's `delocate` step rejects the `.data.rel.ro.local` sections gcc emits, at any gcc version.
Execution and report use one locked, invocation-owned target outside cached Cargo storage, so restored or locally stale first-party objects cannot affect the verdict; `target/sim-coverage.lcov` is only the final output ([ADR-0100](specs/adr/0100-isolate-sim-coverage-current-pass-artifacts.md)).
Do not set LLVM coverage/profdata flag variables around this command: the gate rejects non-empty values because cargo-llvm-cov would otherwise append arbitrary artifact paths to its tool invocations.

Moonpool seed sweep: CI runs a daily 128-random-seed job per [ADR-0036](specs/adr/0036-moonpool-seed-sweep-daily-random.md); locally you can reproduce a flaky run with:

```
MOONPOOL_SEED=0xdeadbeef cargo test -p magnetar-runtime-moonpool \
  --features crypto-aws-lc-rs --locked -- --nocapture
```

(Using a single-provider feature flag here avoids pulling `crypto-fips` for the per-package run, which would otherwise need a native FIPS build toolchain locally.)

E2e tests (require Docker; auto-pull suite-specific Pulsar 4.x images plus `apachepulsar/pulsar:5.0.0-M1` for the `scalable-topics` target):

```
cargo test --workspace --all-features
```

## Commit hygiene

- **Conventional commits**: `feat(scope): subject`, `fix(scope): subject`, etc.
- **Sign-off + GPG**: `git commit -s -S`.
  The repo's pre-commit hook enforces sign-off.
- **No bot trailers** ("Generated by …", "Co-Authored-By:" except for real co-authors).
- One logical change per commit; rebase noise out before push.

## Branch naming

`feat/<scope>`, `fix/<scope>`, `refactor/<scope>`, `chore/<scope>`, `docs/<scope>`, `test/<scope>`.

## Worktree workflow

For non-trivial work, use [`wt`](https://github.com/clever-cloud/worktrunk):

```
wt switch --create feat/producer-batching -y
# edit
wt step diff -- --stat
wt merge -y     # after review
```

Edits on `main` are blocked by the global pre-edit hook.

## Updating the vendored Pulsar proto

```
cargo run -p xtask -- vendor-proto --rev <apache/pulsar commit SHA>
cargo run -p xtask -- codegen
```

Then commit `crates/magnetar-proto/proto/PulsarApi.proto`, `PulsarMarkers.proto`, `SOURCE`, and the regenerated `crates/magnetar-proto/src/pb/*.rs`.
CI's `codegen-drift` job runs `cargo run -p xtask -- codegen --check` and fails if the generated files diverge from what is committed.

## Adding a dependency

See `GUIDELINES.md#adding-a-dependency`.
Short version: only allow-list deps without explicit approval.

## Reporting bugs

Open an issue with: the magnetar version, the Pulsar broker version, the engine in use, a minimal reproduction, and (if applicable) a packet capture or `tracing=trace` log.
For protocol-level bugs, also include the relevant `BaseCommand` hex dump.

## Reviewing PRs

Reviewers verify:

- The PR matches an item in `tasks/todo.md` (or adds a justified scope deviation).
- Validation chain passes.
- Documentation in the same changeset (per "docs are code").
- No new channels.
  No new I/O deps in `magnetar-proto`.
  No new dependencies outside the allow-list.

## License

By contributing you agree your work is licensed under Apache-2.0.
