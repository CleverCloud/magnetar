# Magnetar — Project Guidelines

Stack additively with `~/.claude/CLAUDE.md`; when this file and the global file disagree, this file wins for code inside the magnetar workspace.

## Protocol-correctness invariants

1. **CRC32C verify or drop.** When the payload frame contains the `0x0e01` magic, recompute CRC32C (Castagnoli) over `[METADATA_SIZE][METADATA][PAYLOAD]` and compare. Mismatch → emit `ConnectionEvent::ChecksumMismatch` and drop the frame. Never deliver a payload whose CRC failed.
2. **Magic-byte guard.** A consumer that reads `0x0e02` at the head of the metadata-region must peel `BrokerEntryMetadata` *before* parsing the standard frame. A producer must never emit `0x0e02`.
3. **No panics in `magnetar-proto`.** Every code path must return `Result` or `Option`. Tests assert with `unwrap` only in `#[cfg(test)]` modules.
4. **Request-id monotonicity.** Producer-side `request_id` and `sequence_id` are monotonically non-decreasing per connection, per producer. Resend reuses the original sequence id.
5. **`canAddToBatch ⇒ totalChunks == 1`.** Enforced in `ProducerState::queue_send` and asserted via unit test. Mirrors `ProducerImpl.java:630-654`.
6. **Schema bytes parity.** AVRO/JSON/PROTOBUF schemas are canonicalised broker-side; PROTOBUF_NATIVE + KeyValue use raw-byte equality. Magnetar serialisers must emit byte-identical Java output for the latter two.

## No-channels rule

`tokio::sync::mpsc`, `tokio::sync::broadcast`, `tokio::sync::watch`, `tokio::sync::oneshot`, `std::sync::mpsc`, `crossbeam-channel`, `flume`, `async-channel`, `kanal`, `postage`, `tachyonix`, `thingbuf` — **forbidden everywhere**.

**Why**: avoids hidden backpressure, channel leaks, deadlocks on close, and the "where did this message go?" debugging mode. The sans-io split makes the alternative natural: state lives in `magnetar-proto::Connection`, the engine owns one driver task, user-facing futures register their `Waker` in a slab inside the state machine. The driver dispatches wakers as events arrive.

**How to apply**:
- Producer-to-driver path → `Arc<parking_lot::Mutex<ConnectionShared>>` + `tokio::sync::Notify`.
- Future completion → in-state `Waker` slabs keyed by `op_id` / `sequence_id` / `request_id`.
- Inter-task multiplexing → `tokio::select!` (control-flow, not a channel).
- Enforcement → `cargo deny check` bans the crates; `clippy.toml`'s `disallowed-types` covers `tokio::sync::*` channel paths; `xtask check-no-channels` greps `src/**` as belt-and-braces.

## I/O isolation

`magnetar-proto/Cargo.toml` may not depend on `tokio`, `mio`, `socket2`, `async-trait`, `futures-util` (executor pieces are ok if no actual I/O), or any runtime-bound crate. CI runs `cargo tree -p magnetar-proto -e features` and fails if forbidden names appear.

## TLS

`rustls` is the only TLS implementation. No `native-tls`. `openssl` /
`openssl-sys` are admitted **only** as transitive deps of
`rustls-openssl` under the `crypto-openssl` feature
([ADR-0035](specs/adr/0035-pluggable-crypto-provider.md)) — `deny.toml`
enforces this via `wrappers = ["rustls-openssl"]`. The moonpool engine
drives `rustls::ClientConnection` by hand (`read_tls` /
`process_new_packets` / `write_tls`) over the moonpool byte pipe.

### Crypto provider selection

The active rustls crypto backend is picked at compile time on the
`magnetar` façade via four mutually-pluggable features:

| Feature              | Backend           | Post-quantum KEX     | FIPS validated | Default |
|----------------------|-------------------|----------------------|----------------|---------|
| `crypto-aws-lc-rs`   | aws-lc-rs         | yes (X25519MLKEM768) | no             | ✓       |
| `crypto-ring`        | ring              | no                   | no             |         |
| `crypto-openssl`     | rustls-openssl    | yes                  | depends on OpenSSL build | |
| `crypto-fips`        | aws-lc-fips-sys   | (FIPS-approved only) | yes            |         |

Production callsites must use
`magnetar_runtime_tokio::tls_crypto::active_provider()` (or the
moonpool sibling) rather than `CryptoProvider::get_default()` or
`ring::default_provider()`. The shim is idempotent and installs the
provider on first call. Under `--all-features` the cfg cascade
resolves to aws-lc-rs.

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
- **No "Generated by Claude" trailers.** Anywhere. Commits, PR titles/descriptions, MR descriptions, issue comments.

## Validation

Before declaring a task done:

```
cargo build --workspace --all-features
cargo clippy --workspace --all-features -- -D warnings
cargo +nightly fmt --check
cargo test --workspace
for seed in $(seq 1 32); do                              # local-only sweep (ADR-0024 §3 / ADR-0036)
  MOONPOOL_SEED=$seed cargo test -p magnetar-runtime-moonpool \
    --all-features --locked -- --quiet \
    || { echo "seed $seed FAILED"; exit 1; }
done                                                     # CI: 128 random seeds daily, .github/workflows/moonpool-seed-sweep.yml
cargo deny check
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo xtask check-sim-coverage        # ADR-0024: 100% moonpool coverage on diff
cargo xtask check-runtime-test-parity # ADR-0024: tokio ↔ moonpool 1:1 test count
```

Plus when `magnetar-proto` changes:

```
cargo xtask check-no-channels
cargo xtask codegen --check       # asserts no proto codegen drift
cargo xtask check-no-io-deps      # asserts magnetar-proto has no I/O deps
```

Mutation testing (optional, for deeper coverage on `magnetar-proto`):

```
cargo mutants --package magnetar-proto --timeout 60 --shard 1/4
```

## Cross-runtime test + coverage policy

Any change that alters runtime behavior, public API, wire format, or
touches `magnetar-proto` MUST land with the full four-layer test set
in the same commit:

1. **`magnetar-proto` unit test** — sans-io state-machine behavior in
   isolation (feed bytes, assert events / transmit / state).
2. **`magnetar-runtime-tokio` integration test** under
   `crates/magnetar-runtime-tokio/tests/`.
3. **`magnetar-runtime-moonpool` integration test** under
   `crates/magnetar-runtime-moonpool/tests/`.
4. **`magnetar-differential` equivalence test** asserting tokio ↔
   moonpool user-visible `EventStream` parity.
5. **Docker end-to-end test** under `crates/magnetar/tests/e2e_*.rs`
   (`#[cfg(feature = "e2e")] + #[ignore = "e2e: requires Docker"]`).

**Sim coverage** — `magnetar-runtime-moonpool` must hit **100% line
coverage on the diff** (`merge-base origin/main HEAD`). Enforced by
`cargo xtask check-sim-coverage`, which wraps `cargo-llvm-cov --json`
on the moonpool test runner and diffs against the merge base. Hard
requirement in local + CI.

**Runtime parity** — `magnetar-runtime-tokio` and
`magnetar-runtime-moonpool` keep **strict 1:1 test count** (`#[test]`
+ `#[tokio::test]` + `#[moonpool::test]`). Enforced by
`cargo xtask check-runtime-test-parity`. Hard requirement.

**Seed sweep** — the local validation pass runs
`MOONPOOL_SEED=$seed cargo test -p magnetar-runtime-moonpool` for
`seed ∈ 1..32` to catch seed-dependent flakiness in the
deterministic-simulation suite. **CI cadence is different**: per
[ADR-0036](specs/adr/0036-moonpool-seed-sweep-daily-random.md), the
sweep runs **daily** with **128 freshly-rolled random seeds in
parallel** in
[`.github/workflows/moonpool-seed-sweep.yml`](.github/workflows/moonpool-seed-sweep.yml),
not on every PR / push. Reason: fixed `(commit, seed)` pairs are
bit-for-bit reproducible, so re-running them on every PR is wasted
compute — random seeds rolled daily cover the seed space far better
over time.

**Exemptions** — docs-only, comment-only, formatter-only, and
dependency bumps with no functional impact. Author justifies in the
commit message; reviewer enforces.

**Why**: the parity matrix in
[`README.md`](README.md#java-client-parity-matrix) is the binding
Java-parity contract. Without coverage + count parity, moonpool silently
falls behind tokio and the differential harness loses its value as an
equivalence oracle. See
[ADR-0024](specs/adr/0024-cross-runtime-test-and-coverage-policy.md).

## Naming

- Crate names: `magnetar`, `magnetar-<scope>`. No hyphen-in-hyphen abuse (`magnetar-foo-bar` is fine; `magnetar-foo-bar-baz` is suspicious).
- Module names: `snake_case`, terse, no `_impl` or `_base` suffixes (idiomatic Rust, not Java).
- Types: `CamelCase`. Acronyms ≤ 2 letters are uppercase (`MessageId`, `ClientCnx` → `ClientConn`).

## Adding a dependency

All new dependencies go through these steps:

1. Check it's in the allow-list (the `deny.toml` `[bans]` allow-list governs which crates may appear in `Cargo.toml`).
2. If not, propose to Florentin with: crate name, version, why it's needed, what it replaces, license, maintenance signal.
3. Wait for explicit approval before adding to `Cargo.toml`.
4. After adding, run `cargo deny check bans licenses sources` and verify.

## Editing PIP-vendored proto

`crates/magnetar-proto/proto/PulsarApi.proto` and `PulsarMarkers.proto` are vendored verbatim from `apache/pulsar`. Update via:

```
cargo xtask vendor-proto --rev <pulsar-commit-sha>
```

Never hand-edit. Record the source commit in `crates/magnetar-proto/proto/SOURCE`.
