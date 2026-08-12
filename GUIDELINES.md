# Magnetar — Project Guidelines

Stack additively with `~/.claude/CLAUDE.md`; when this file and the global file disagree, this file wins for code inside the magnetar workspace.

## Protocol-correctness invariants

1. **CRC32C verify or drop.** When the payload frame contains the `0x0e01` magic, recompute CRC32C (Castagnoli) over `[METADATA_SIZE][METADATA][PAYLOAD]` and compare.
   Mismatch → emit `ConnectionEvent::ChecksumMismatch` and drop the frame.
   Never deliver a payload whose CRC failed.
2. **Magic-byte guard.** A consumer that reads `0x0e02` at the head of the metadata-region must peel `BrokerEntryMetadata` _before_ parsing the standard frame.
   A producer must never emit `0x0e02`.
3. **No panics in `magnetar-proto`.** Every code path must return `Result` or `Option`.
   Tests assert with `unwrap` only in `#[cfg(test)]` modules.
4. **Request-id monotonicity.** Producer-side `request_id` and `sequence_id` are monotonically non-decreasing per connection, per producer.
   Resend reuses the original sequence id.
5. **`canAddToBatch ⇒ totalChunks == 1`.** Enforced in `ProducerState::queue_send` and asserted via unit test.
   Mirrors `ProducerImpl.java:630-654` (Apache Pulsar Java reference, external).
6. **Schema bytes parity.** AVRO/JSON/PROTOBUF schemas are canonicalised broker-side; PROTOBUF_NATIVE + KeyValue use raw-byte equality.
   Magnetar serialisers must emit byte-identical Java output for the latter two.

## No-channels rule

`tokio::sync::mpsc`, `tokio::sync::broadcast`, `tokio::sync::watch`, `tokio::sync::oneshot`, `std::sync::mpsc`, `crossbeam-channel`, `flume`, `async-channel`, `kanal`, `postage`, `tachyonix`, `thingbuf` — **forbidden everywhere**.

**Why**: avoids hidden backpressure, channel leaks, deadlocks on close, and the "where did this message go?" debugging mode.
The sans-io split makes the alternative natural: state lives in `magnetar-proto::Connection`, the engine owns one driver task, user-facing futures register their `Waker` in a slab inside the state machine.
The driver dispatches wakers as events arrive.

**How to apply**:

- Producer-to-driver path → `Arc<parking_lot::Mutex<ConnectionShared>>` + `tokio::sync::Notify`.
- Future completion → in-state `Waker` slabs keyed by `op_id` / `sequence_id` / `request_id`.
- Inter-task multiplexing → the owning runtime's `select!` (`tokio::select!` in the tokio engine, `moonpool_core::select!` in provider-generic Moonpool code); this is control flow, not a channel.
- Enforcement → `cargo deny check` bans the crates; `clippy.toml`'s `disallowed-types` covers `tokio::sync::*` channel paths; `xtask check-no-channels` greps `src/**` as belt-and-braces.

## I/O isolation

`magnetar-proto/Cargo.toml` may not depend on `tokio`, `mio`, `socket2`, `async-trait`, `futures-util` (executor pieces are ok if no actual I/O), or any runtime-bound crate.
CI runs `cargo tree -p magnetar-proto -e features` and fails if forbidden names appear.

## Sans-io clock injection

The state machine never reads a clock it was not handed.

- Monotonic time arrives as an explicit `now: Instant` parameter, placed **last** in the argument list (`queue_send(msg, publish_time_ms, now)`, `Connection::ack(handle, ack, now)`, `pop_message(handle, now)`).
  Wall-clock time arrives through the `wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>` provider.
- `Instant::now()`, `SystemTime::now()`, and `.elapsed()` are forbidden in `crates/magnetar-proto/src/**` outside `#[cfg(test)]`.
  There is no file allowlist.
  Engines snapshot the clock at the call boundary — and **before** taking the connection mutex, per the lock ordering below — so `magnetar-runtime-moonpool` can substitute a virtual clock and reproduce every derived value bit-for-bit per seed.
- `Instant` arithmetic must not panic (invariant: no panics in `magnetar-proto`).
  Use `now.saturating_duration_since(base)` instead of `now - base`, and `crate::time::deadline_with_clamp(base, delta)` instead of `base + delta`.
- Enforcement → `cargo run -p xtask -- check-no-internal-clock`, whose scanner skips `#[cfg(test)]` spans and comments / string literals and is unit-tested in `xtask/src/main.rs`.
- Two documented non-time leaks (`uuid::Uuid::new_v4()` in chunked emit, `std::env::var()` in `TokenAuth` bootstrap) are tracked in [`ARCHITECTURE.md`](ARCHITECTURE.md#known-non-determinism-leaks-documented) and are **not** mechanically enforced.
- See [ADR-0011](specs/adr/0011-clock-injection-sans-io.md) and [ADR-0086](specs/adr/0086-inject-now-into-proto-latency-recording.md).

## TLS

`rustls` is the only TLS implementation.
No `native-tls`.
`openssl` / `openssl-sys` are admitted **only** as transitive deps of `rustls-openssl` under the `crypto-openssl` feature ([ADR-0035](specs/adr/0035-pluggable-crypto-provider.md)) — `deny.toml` enforces this via `wrappers = ["rustls-openssl"]`.
The moonpool engine drives `rustls::ClientConnection` by hand (`read_tls` / `process_new_packets` / `write_tls`) over the moonpool byte pipe.

### Crypto provider selection

The active rustls crypto backend is picked at compile time on the `magnetar` façade via four mutually-pluggable features:

| Feature            | Backend         | Post-quantum KEX     | FIPS validated           | Default |
| ------------------ | --------------- | -------------------- | ------------------------ | ------- |
| `crypto-aws-lc-rs` | aws-lc-rs       | yes (X25519MLKEM768) | no                       | ✓       |
| `crypto-ring`      | ring            | no                   | no                       |         |
| `crypto-openssl`   | rustls-openssl  | yes                  | depends on OpenSSL build |         |
| `crypto-fips`      | aws-lc-fips-sys | (FIPS-approved only) | yes                      |         |

Production callsites must use `magnetar_runtime_tokio::tls_crypto::active_provider()` (or the moonpool sibling) rather than `CryptoProvider::get_default()` or `ring::default_provider()`.
The shim is idempotent and installs the provider on first call.
Under `--all-features` the cfg cascade resolves to aws-lc-rs.

A single `compile_error!` fires if no `crypto-*` feature is selected.
The per-cell matrix is enforced by `cargo xtask check-crypto-matrix`.

## Worktree workflow

Per `~/.claude/CLAUDE.md`: every change to the workspace goes through a worktree:

```
wt switch --create feat/<scope> -y
# edit
wt step diff -- --stat
# user reviews
wt merge -y    # confirmed with user
```

The pre-edit hook blocks edits on `main`/`master`/`trunk`/`develop`.

## Commits

- Conventional: `feat(<scope>): subject`, `fix(<scope>): subject`, `refactor(<scope>): subject`, `chore(<scope>): subject`, `docs(<scope>): subject`, `test(<scope>): subject`.
- `git commit -s -S` always (signed-off + GPG-signed by Florentin's key `B426D94AC023FFA4`).
- **No "Generated by Claude" trailers.** Anywhere.
  Commits, PR titles/descriptions, MR descriptions, issue comments.

## Validation

**Validation chain.** Run before declaring a task done — see [CLAUDE.md § Validation chain](CLAUDE.md#validation-chain) for the authoritative full chain (xtask gates, FIPS clang env vars, e2e pre-reqs, etc.).
The `GUIDELINES.md` scope is the binding code rules (no-channels, I/O isolation, TLS, protocol invariants); the build / test invocation order lives in `CLAUDE.md`.

Mutation testing (optional, for deeper coverage on `magnetar-proto`):

```
cargo mutants --package magnetar-proto --timeout 60 --shard 1/4
```

## Cross-runtime test + coverage policy

Any change that alters runtime behavior, public API, wire format, or touches `magnetar-proto` MUST land with the full four-layer test set in the same commit:

1. **`magnetar-proto` unit test** — sans-io state-machine behavior in isolation (feed bytes, assert events / transmit / state).
2. **`magnetar-runtime-tokio` integration test** under `crates/magnetar-runtime-tokio/tests/`.
3. **`magnetar-runtime-moonpool` integration test** under `crates/magnetar-runtime-moonpool/tests/`.
4. **`magnetar-differential` equivalence test** asserting tokio ↔ moonpool user-visible `EventStream` parity.
5. **Docker end-to-end test** under `crates/magnetar/tests/e2e_*.rs` — no dedicated `e2e` Cargo feature and no `#[ignore]`.
   Owning product features still gate feature-specific targets (`e2e_scalable_topic.rs` requires `scalable-topics`); `cargo test --workspace --all-features` activates them all on every CI push, and a host without Docker fails instead of skipping ([ADR-0046](specs/adr/0046-e2e-tests-as-casual-no-feature-flag-no-ignore.md)).

**Patch coverage** — every executable line the diff (`merge-base origin/main HEAD`) adds inside either reported domain must have **100% line coverage on the diff**. `cargo run -p xtask -- check-sim-coverage` runs Moonpool+differential evidence for shared/proto/Moonpool/differential/façade/fakes/auth source and a separate Tokio unit/integration+differential pass for Tokio adapter source (ADR-0103).
One locked, invocation-owned scratch root contains separate targets, objects, profiles, profdata, and reports for the two domains; neither can discharge the other, LCOV outputs are diagnostic only, and no cached or cross-domain input is reused ([ADR-0100](specs/adr/0100-isolate-sim-coverage-current-pass-artifacts.md), [ADR-0103](specs/adr/0103-isolate-moonpool-and-tokio-coverage-evidence.md)).
The requirement on the author is hard, and since [ADR-0092](specs/adr/0092-enforce-sim-coverage-and-gate-every-pull-request.md) the gate's enforcement of it is too — see **Enforcing landing** below.

Execution scope and report scope are two different sets, and the distinction is binding:

- **Moonpool domain** — executes `-p magnetar-runtime-moonpool -p magnetar-differential` and reports `magnetar-proto`, `magnetar-runtime-moonpool`, `magnetar-differential`, `magnetar-auth-athenz`, `magnetar-auth-sasl`, `magnetar-driver`, and `magnetar-fakes`.
  A proto or shared line still requires Moonpool or differential evidence.
- **Tokio domain** — executes `-p magnetar-runtime-tokio -p magnetar-differential` but reports only `magnetar-runtime-tokio`.
  Honest Tokio unit and integration tests discharge private adapter lines, while Tokio profiles can never satisfy the Moonpool/shared domain.
  The original six-package widening measured 63 `SF:` records on 2026-07-31 against 16 before; that is historical evidence, not the current seven-plus-one topology. Each current domain performs its own execution and report in a distinct target.
  Reports are read inside scratch and retained in memory; diagnostics are atomically published only after cleanup.
  Generated code under `crates/magnetar-proto/src/pb/` stays excluded, as is every line inside a `#[cfg(test)]` span — span membership via the shared `cfg_test_line_flags`, the same scanner `check-no-internal-clock` and `check-log-fields` use.
  Until [ADR-0092](specs/adr/0092-enforce-sim-coverage-and-gate-every-pull-request.md) this gate instead cut at a file's **first** `#[cfg(test)]` line and dropped everything below it; because that line is usually a gated `use` or helper rather than the bottom `mod tests`, it exempted 48% of all gated lines and 71% of those added over the preceding ten merged PRs.
  Do not reintroduce a line-cut heuristic here.

The `magnetar` façade library and `magnetar-fakes` are in the reported and hard-gated set because `magnetar-differential` now compiles and exercises both.
The façade's Docker-bound `crates/magnetar/tests/e2e_*.rs` targets do not run under either coverage domain.
Additions in `magnetar-admin`, `magnetarctl`, `magnetar-auth-oauth2`, `magnetar-messagecrypto`, and any other uncompiled package still print as `not gated` and do not fail the check — advisory only, per [ADR-0088](specs/adr/0088-sim-coverage-gate-scope-report-ungated-additions.md) as amended by [ADR-0102](specs/adr/0102-assignment-driven-m1-hardened-stream-consumer.md).
That holds under `--enforce` too: the ungated report is a scope limit, not a verdict, so no flag turns it fatal. See [ADR-0024](specs/adr/0024-cross-runtime-test-and-coverage-policy.md) for the policy the gate serves.

**Enforcing landing** — `SIM_COVERAGE_ENFORCES_UNCOVERED = true` in `xtask/src/main.rs`, so an uncovered added line inside the reported scope is printed in full, with a count, and **fails** the check ([ADR-0092](specs/adr/0092-enforce-sim-coverage-and-gate-every-pull-request.md)).
A green `check-sim-coverage` is therefore evidence of the 100%-on-the-diff requirement above, for the lines in the reported scope — and still says nothing about anything outside it, which is what the `not gated` lines are for.
The gate ran advisory from [ADR-0090](specs/adr/0090-widen-sim-coverage-report-to-compiled-closure.md) until ADR-0092, for one reason worth remembering: it had no per-PR home, so enforcing it would have changed nothing.
`.github/workflows/xtask-gates.yml` ran it on a daily cron against `main`, where the merge-base is `HEAD`, the diff is empty and the check short-circuits with "nothing to verify" before building anything.
ADR-0092 landed both halves together — the flip, and a `check-sim-coverage` job in [`ci.yml`](.github/workflows/ci.yml) on every `pull_request`.
Making that job actually block a merge is a branch-protection step in repository settings, not in this tree; `main` had no protection at all as of 2026-08-01, so treat a red run as a real verdict that a human can still merge past (ADR-0092 § Required check).
`--enforce` now only ORs into the constant, so it is redundant; it is retained because existing invocations keep working, the CI job passes it to state its own intent, and it stays the explicit way to ask for the verdict if the constant is ever flipped back.
Because the flag would mask exactly that regression, the constant is pinned outside the CI job by a `const` assertion in `sim_coverage_enforces_uncovered_by_default`: reverting the flip stops the `xtask` **test** build compiling (`cargo test` / `clippy --all-targets`, both of which CI runs workspace-wide), while a plain `cargo build` is unaffected since the assertion lives in a `#[cfg(test)]` module.
Cutting the call site instead — `let enforcing = enforce;` — slips past that assertion and past the whole test; what catches it is `dead_code` under `-D warnings`, which is why `sim_coverage_enforcing` exists as a named `const fn` with one production call site.

ADR-0102's missing-file evidence rule stays unconditional: an added file in a gated crate with no `SF:` record fails when its crate emitted no records or the file contains a non-test function body.
Executable `unreachable!`, `unimplemented!`, and `todo!` lines have no lexical exclusion.
Missing `DA:` mappings inside an otherwise recorded file remain an unresolved follow-up; no lexical function parser is part of this gate.
That signals a broken or incomplete measurement rather than a missing test, and a gate that cannot measure must never report success.
A record-less file with no non-test function body stays advisory because module/export/constant/bodyless-declaration source has no executable coverage mapping.

**Runtime parity** — `magnetar-runtime-tokio` and `magnetar-runtime-moonpool` keep **strict 1:1 test count** (`#[test]`

- `#[tokio::test]` + `#[moonpool::test]`).
  Enforced by `cargo xtask check-runtime-test-parity`.
  Hard requirement.

**Seed sweep** — the local validation pass runs `MOONPOOL_SEED=$seed cargo test -p magnetar-runtime-moonpool` for `seed ∈ 1..32` to catch seed-dependent flakiness in the deterministic-simulation suite.
**CI cadence is different**: per [ADR-0036](specs/adr/0036-moonpool-seed-sweep-daily-random.md), the sweep runs **daily** with **128 freshly-rolled random seeds in parallel** in [`.github/workflows/moonpool-seed-sweep.yml`](.github/workflows/moonpool-seed-sweep.yml), not on every PR / push.
Reason: fixed `(commit, seed)` pairs are bit-for-bit reproducible, so re-running them on every PR is wasted compute — random seeds rolled daily cover the seed space far better over time.

**Exemptions** — docs-only, comment-only, formatter-only, and dependency bumps with no functional impact.
Author justifies in the commit message; reviewer enforces.

**Why**: the parity matrix in [`README.md`](README.md#java-client-parity-matrix) is the binding Java-parity contract.
Without coverage + count parity, moonpool silently falls behind tokio and the differential harness loses its value as an equivalence oracle.
See [ADR-0024](specs/adr/0024-cross-runtime-test-and-coverage-policy.md).

## Naming

- Crate names: `magnetar`, `magnetar-<scope>`.
  No hyphen-in-hyphen abuse (`magnetar-foo-bar` is fine; `magnetar-foo-bar-baz` is suspicious).
- Module names: `snake_case`, terse, no `_impl` or `_base` suffixes (idiomatic Rust, not Java).
- Types: `CamelCase`.
  Acronyms ≤ 2 letters are uppercase (`MessageId`, `ClientCnx` → `ClientConn`).

## Adding a dependency

All new dependencies go through these steps:

1. Check it's in the allow-list (the `deny.toml` `[bans]` allow-list governs which crates may appear in `Cargo.toml`).
2. If not, propose to Florentin with: crate name, version, why it's needed, what it replaces, license, maintenance signal.
3. Wait for explicit approval before adding to `Cargo.toml`.
4. After adding, run `cargo deny check bans licenses sources` and verify.

## Editing PIP-vendored proto

`crates/magnetar-proto/proto/PulsarApi.proto` and `PulsarMarkers.proto` are vendored verbatim from `apache/pulsar`.
Update via:

```
cargo xtask vendor-proto --rev <pulsar-commit-sha>
```

Never hand-edit.
Record the source commit in `crates/magnetar-proto/proto/SOURCE`.
