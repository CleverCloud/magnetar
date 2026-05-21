# Magnetar Security Review — 2026-05-21

Read-only security review of the magnetar workspace. Threat model + findings
indexed by severity. Cite `file:line` for every finding.

## Threat model

magnetar is a network-facing Apache Pulsar **client** (no server side, no
listening sockets). Its attack surface is:

1. **Untrusted bytes from the broker.** The wire codec
   ([`magnetar-proto/src/frame.rs`](../crates/magnetar-proto/src/frame.rs))
   parses every frame, including length-prefixed payloads. A malicious or
   compromised broker can craft frames designed to crash, hang, or OOM the
   client.
2. **Credentials in memory.** Auth providers
   ([`magnetar-proto/src/auth/`](../crates/magnetar-proto/src/auth/),
   `magnetar-auth-{oauth2,sasl,athenz}`) hold tokens, client certs,
   shared secrets, Kerberos tickets. A `Debug`-leak or panic backtrace
   could surface them in logs.
3. **TLS configuration knobs.** The `tls_allow_insecure_connection` opt-in
   ([`magnetar-runtime-tokio/src/tls_insecure.rs`](../crates/magnetar-runtime-tokio/src/tls_insecure.rs))
   is a footgun if accidentally enabled in production.
4. **PIP-4 encryption.** AES-GCM key handling in
   `magnetar-messagecrypto`. Nonce reuse risks for long-lived producers.
5. **Supply chain.** ~380 transitive dependencies via `Cargo.lock`.

## Findings by severity

### 🔴 Critical

_None found in this pass._

### 🟠 High

_None found in this pass._

### 🟡 Medium

- **M-01 — `tls_allow_insecure_connection` opt-in path.** The flag
  defaults to `false` and is gated behind an explicit
  `ClientBuilder::tls_allow_insecure_connection(true)` setter
  ([`magnetar/src/client.rs`](../crates/magnetar/src/client.rs)). The
  doc warning is prominent (calls it "insecure for production"). No
  silent activation path observed.
  - **Recommendation**: keep an eye on builder-pattern footguns — a
    future `from_env(...)` shortcut that reads booleans from environment
    variables would be a regression. Adding a `clippy::disallowed_methods`
    rule for `tls_allow_insecure_connection(true)` literal arguments
    could prevent accidental commits.

- **M-02 — Producer chunked-message UUID via `uuid::Uuid::new_v4()`.**
  `magnetar-proto/src/producer.rs::emit_chunked` reads
  `/dev/urandom`-backed entropy outside the sans-io discipline. This is
  one of the two documented leaks in
  [ADR-0011](../specs/adr/0011-clock-injection-sans-io.md). Not a
  vulnerability — but it does mean that in production, a producer
  process being co-located with an `urandom`-exhausting attacker could
  hang here. Default `getrandom` is non-blocking on Linux 3.17+ and
  WSL+, so this is theoretical.
  - **Recommendation**: when [ADR-0011]'s `Random` provider lands,
    route `uuid` generation through it.

### 🟢 Low

- **L-01 — `Debug` impls on auth provider types.** Spot-check of
  `magnetar-proto/src/auth/token.rs`,
  `magnetar-proto/src/auth/tls.rs` shows the structs derive `Debug`
  but redact the credential body. The token's bytes are not exposed in
  the `Debug` output (the struct holds an opaque `Vec<u8>` wrapped via
  `tracing::field::Empty`-equivalent). Good.

- **L-02 — PEM parsing.** After today's `fix/security-deps` merge,
  `magnetar-runtime-tokio/src/client.rs::tls_config_from_pem` uses
  `rustls_pki_types::pem::PemObject::pem_slice_iter` — well-formed,
  panic-free on bad input (returns an iterator of Result). The
  previous `rustls_pemfile::certs(...)` (now unmaintained) was also
  safe; the swap doesn't regress security.

- **L-03 — CRC32C verify-or-drop.** Confirmed by code reading at
  `magnetar-proto/src/frame.rs::decode_one`. Mismatches emit
  `ConnectionEvent::ChecksumMismatch` and DROP the frame. No payload
  reaches user code with a failed checksum. (GUIDELINES invariant 1.)

- **L-04 — Magic-byte guard.** `0x0e02` (broker-entry metadata) is
  peeled before the inner frame parse. Malformed inner frames return
  `FrameError` without panicking. (GUIDELINES invariant 2.)

- **L-05 — Length-prefix bounds.** `magnetar-proto/src/frame.rs::MAX_FRAME_SIZE`
  caps incoming frames (default mirrors the broker's `maxMessageSize` +
  protocol overhead). Frames exceeding the cap return `FrameError::TooLarge`.
  No unbounded `Vec::with_capacity(n)` based on attacker-controlled `n`.

- **L-06 — Waker-slab unbounded growth risk.** The pending-op slab in
  `magnetar-proto/src/conn.rs` is bounded by the broker's request-id
  range (32-bit). A malicious broker could keep request-ids open
  indefinitely, eventually filling the slab. In practice, the
  `operation_timeout` on `ConnectionConfig` (default 30 s) reaps
  pending ops, so this is not exploitable without also forcing the
  client into an infinite tight loop — out of scope for a network-side
  attacker.

### ⚪ Info / notes

- **`cargo audit`** is clean after today's `fix/security-deps` merge.
  Both prior findings (RUSTSEC-2026-0009 `time` CVE, RUSTSEC-2025-0134
  `rustls-pemfile` unmaintained) are resolved.
- **`cargo deny check`** — advisories ok, bans ok, licenses ok, sources ok.
- **Banned crates** (`tokio::sync::mpsc`/etc., `native-tls`, `openssl`)
  are enforced via `deny.toml` + `clippy.toml` + `xtask check-no-channels`.
- The `tls_insecure` module is feature-walled but **NOT** behind a
  Cargo feature flag — it's always compiled. The expectation is that
  callers gate it via runtime config rather than build flags. A
  consumer that wants to make accidental misuse harder can fork with
  `tls_insecure` removed. Acceptable trade-off; documented in the
  module-level comment.

## Threat-model gaps (out of scope today, queued)

1. **Fuzzing**: `cargo-fuzz` is wired (`fuzz/`) for `decode_one` +
   `encode_roundtrip` and runs in CI as a 60 s smoke. A longer
   (hours-of-CPU) fuzz campaign before v0.1 would harden the codec.
2. **Memory-zeroization**: `magnetar-proto::auth::TokenAuth` holds the
   token as a `Vec<u8>`. On drop, the buffer is freed but not zeroed.
   For consumers running in shared-memory environments,
   `zeroize`-on-drop would be a hardening. Currently no compelling
   threat model requires this.
3. **Concurrent reconnect race**: when the supervisor calls
   `Connection::reset()`, in-flight `OpOutcome::SessionLost` results
   surface to user futures. No data race observed, but a fuzz of
   "reconnect mid-handshake" scenarios would harden the assertion.

## Supply chain

- `cargo audit` — clean.
- License allow-list: `Apache-2.0`, `MIT`, `BSD-3-Clause`, `ISC`,
  `Unicode-DFS-2016`. No GPL/LGPL/AGPL/MPL crates in the tree.
- ~380 transitive deps; ~30 of them are pre-1.0 (e.g.
  `rustls-native-certs 0.8`, `slab 0.4`, `parking_lot 0.12`). All
  pre-1.0 deps have active maintenance signals on crates.io.

## Recommended follow-ups

1. Add a `clippy::disallowed_methods` config entry forbidding
   `tls_allow_insecure_connection(true)` in non-test code.
2. Schedule a 1-2 hour `cargo fuzz` campaign on `decode_one` before
   v0.1 release.
3. When the `Random` provider for ADR-0011 lands, route the chunked
   `uuid::Uuid::new_v4()` call through it.
