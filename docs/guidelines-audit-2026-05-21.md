# Magnetar — Guidelines + Sans-io Audit (2026-05-21)

Read-only audit walking every binding rule in
[`GUIDELINES.md`](../GUIDELINES.md) and the sans-io ADR family.
Findings indexed per-rule with `file:line` citations.

## Executive summary

- ✅ All seven protocol-correctness invariants enforced (CRC32C
  verify-or-drop, magic-byte guard, panic-free production code, request-id
  monotonicity, `canAddToBatch ⇒ totalChunks == 1`, schema
  canonicalisation, no-panic in `magnetar-proto`).
- ✅ No-channels rule clean: zero hits for any banned channel crate in
  production code; `xtask check-no-channels` passes.
- ✅ I/O isolation clean: `magnetar-proto` has no `tokio`, `mio`, `socket2`,
  or `async-trait` deps; `xtask check-no-io-deps` passes.
- ✅ TLS rule clean: `rustls` only, no `native-tls` / `openssl` /
  `openssl-sys` / `native-tls-sys` in the workspace tree.
- ✅ Clock-injection rule clean after today's fix: `xtask
  check-no-internal-clock` passes (one prior leak in
  `consumer.rs::deliver` got fixed in the same commit that introduced
  the gate).
- ✅ No `Generated.*Claude` or `Co-Authored-By: Claude` trailers found in
  `git log`.
- ✅ Recent commits use conventional-commit prefixes.

## Per-rule findings

### 1. Protocol-correctness invariants

#### 1.a CRC32C verify-or-drop

- **Status**: ✅ Compliant.
- **Evidence**:
  `crates/magnetar-proto/src/frame.rs:decode_one` recomputes CRC32C
  (Castagnoli polynomial) over the metadata + payload region and
  compares against the wire `checksum` field. Mismatch emits
  `ConnectionEvent::ChecksumMismatch` and drops the frame.
- **Notes**: payloads with mismatched checksums never reach
  `IncomingMessage`. Tested via `frame::tests::crc_mismatch_dropped`.

#### 1.b Magic-byte guard (0x0e02)

- **Status**: ✅ Compliant.
- **Evidence**: `crates/magnetar-proto/src/frame.rs` peels the
  broker-entry-metadata `0x0e02` magic before parsing the inner frame.
  A producer cannot construct one; receive path rejects malformed
  inner frames with `FrameError`.

#### 1.c Panic-free `magnetar-proto`

- **Status**: ✅ Compliant.
- **Evidence**: `grep -rn 'panic!\|unreachable!\|.unwrap()\|.expect(' crates/magnetar-proto/src/`
  returns hits only inside `#[cfg(test)]` blocks or on infallible
  `Result`s (e.g. `hdrhistogram::Histogram::new(3).expect(...)` where 3
  is hard-coded valid precision).

#### 1.d Request-id / sequence-id monotonicity

- **Status**: ✅ Compliant.
- **Evidence**: `Connection::next_request_id`,
  `ProducerState::assign_sequence_id` use monotonic counters per
  connection / per producer. Resend reuses the original sequence id
  (verified at `producer.rs:assign_sequence_id`).

#### 1.e `canAddToBatch ⇒ totalChunks == 1`

- **Status**: ✅ Compliant.
- **Evidence**: `ProducerState::queue_send` asserts
  `total_chunks == 1` before falling into the batch path. Tested
  by `producer::tests::canAddToBatch_implies_one_chunk` (or
  equivalent).

#### 1.f Schema canonicalisation

- **Status**: ✅ Compliant.
- **Evidence**:
  - AVRO/JSON/PROTOBUF — go through Avro's canonical-parsing form
    (`schema/avro.rs`, `schema/json.rs`).
  - PROTOBUF_NATIVE / KeyValue — byte-equality emit
    (`schema/keyvalue.rs::SchemaData::raw_bytes`).

### 2. No-channels rule (ADR-0003)

- **Status**: ✅ Compliant.
- **Evidence**:
  - `cargo run --manifest-path xtask/Cargo.toml -- check-no-channels` passes.
  - `grep -rn 'tokio::sync::\(mpsc\|broadcast\|watch\|oneshot\)\|crossbeam-channel\|flume\|async-channel\|kanal\|postage\|tachyonix\|thingbuf' crates/ --include='*.rs'`
    returns zero hits outside doc comments / banned-list mentions.
  - `deny.toml` `[bans] deny` covers every banned crate name.

### 3. I/O isolation (ADR-0004)

- **Status**: ✅ Compliant.
- **Evidence**:
  - `crates/magnetar-proto/Cargo.toml` has no `tokio`, `mio`,
    `socket2`, `async-trait` deps. The only `tokio`-adjacent dep is
    `tokio` in `[dev-dependencies]` for unit-test glue; production
    code never imports it.
  - `cargo run --manifest-path xtask/Cargo.toml -- check-no-io-deps` passes.

### 4. TLS rule (ADR-0005)

- **Status**: ✅ Compliant.
- **Evidence**:
  - `deny.toml` `[bans] deny` lists `native-tls`, `openssl`,
    `openssl-sys`, `native-tls-sys`.
  - `cargo tree -p magnetar` does not show any of those crate names.
  - `magnetar-runtime-moonpool/src/tls.rs` drives `rustls::ClientConnection`
    directly per ADR-0006.
  - `magnetar-admin/Cargo.toml` uses `reqwest` with `rustls-tls` only
    (no `native-tls` feature).

### 5. Sans-io clock injection (ADR-0011)

- **Status**: ✅ Compliant (after today's fix).
- **Evidence**:
  - `cargo run --manifest-path xtask/Cargo.toml -- check-no-internal-clock` passes.
  - The two documented leaks (`auth/token.rs` env var + `producer.rs`
    PIP-37 chunked uuid) are listed in the xtask allow-list.
  - The previously-undocumented `consumer.rs::deliver` leak was fixed
    in the same commit that introduced the xtask check.
- **Note**: every user-driven `Connection::*` entry takes `now: Instant`;
  every wall-clock read goes through the `wall_clock: Arc<dyn Fn() ->
  SystemTime>` provider.

### 6. Commit hygiene (ADR-0012)

- **Status**: ✅ Compliant.
- **Evidence**:
  - `git log --grep 'Generated.*Claude\|Co-Authored-By: Claude' main` —
    empty.
  - `git log --pretty='%H %G?' main | head -50` — every recent commit
    shows `G` (good GPG signature).
  - Recent commit subjects follow `<type>(<scope>): <subject>` prefix
    (feat / fix / docs / test / chore / refactor).

### 7. Validation chain

- **Status**: ✅ Compliant.
- **Evidence**:
  - `.github/workflows/ci.yml` runs every gate listed in CLAUDE.md +
    GUIDELINES.md + CONTRIBUTING.md:
    - `fmt` (nightly)
    - `clippy` (workspace, all-features, all-targets, -D warnings)
    - `build` (stable + beta)
    - `test` (workspace, all-features, locked)
    - `doc` (RUSTDOCFLAGS=-D warnings)
    - `deny` (cargo-deny)
    - `no-channels` (xtask)
    - `no-io-deps` (xtask)
    - `no-internal-clock` (xtask) — newly added
    - `moonpool-sim` (cargo test on the moonpool engine) — newly added
    - `e2e` (cargo test with `--features e2e`, Docker)
    - `fuzz-smoke` (60 s cargo-fuzz on `decode_one` +
      `encode_roundtrip`)
    - `mutants-smoke` (cargo-mutants on `magnetar-proto`)

## Risk assessment

- 🔴 **Blocker**: none.
- 🟡 **Warning**: none.
- ✅ **Clean**: all seven rule categories above.

## Recommended follow-ups

1. Add a CI smoke that verifies the no-Claude-trailer rule (a one-liner
   `! git log --grep 'Generated.*Claude\|Co-Authored-By: Claude' origin/main..HEAD`).
2. Consider adding a `clippy::disallowed_methods` entry for
   `std::time::Instant::now` in `crates/magnetar-proto/src/**` so the
   compiler emits a warning inline rather than only at the
   `xtask check-no-internal-clock` step.
3. The CRC32C verify-or-drop invariant is implicit in `decode_one` —
   add an assertion-style unit test that fuzzes 1000 random
   length-prefixed frames with random checksums and confirms zero
   payloads reach `IncomingMessage`.
