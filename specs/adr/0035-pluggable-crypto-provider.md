# ADR-0035 — Pluggable rustls crypto provider

- **Status**: Accepted
- **Date**: 2026-05-26
- **Decider**: Florentin Dubois
- **Tags**: tls, dependencies, supply-chain, features, post-quantum, fips

## Context

[ADR-0005](0005-rustls-only-tls.md) made `rustls` the only TLS
implementation and banned `native-tls` / `openssl` / `openssl-sys`
outright via `deny.toml`. Issue [#9] later asked for a
compile-time-pluggable rustls crypto backend, matrix
`aws-lc-rs` / `ring` / `openssl` / `fips`, with post-quantum hybrid key
exchange (X25519MLKEM768) under the aws-lc-rs path.

The state of play before this ADR:

- `Cargo.toml:63-68` hard-wired `rustls = { features = ["std", "ring"]
  }` and `tokio-rustls = { features = ["ring"] }`.
- `Cargo.toml:98` set `reqwest = { features = ["rustls-tls", "json"] }`
  which in reqwest 0.12 resolves to aws-lc-rs internally — so cargo was
  silently unifying *two* providers into the build (`ring` from rustls,
  `aws-lc-rs` from reqwest's `__rustls-aws-lc-rs`). The runtime default
  was whoever called `install_default()` first; production code never
  did, leaving four production callsites
  (`tls_insecure.rs`, `tls_no_hostname.rs`, `transport.rs`,
  `client.rs`) to fall back to `ring::default_provider()` explicitly.
- `prefer-post-quantum` rustls feature was inert because it only fires
  under `aws-lc-rs`.

rustls's own crypto-provider design — `rustls::crypto::CryptoProvider`
plus `install_default()` — already supports the four backends without
forking. The only thing missing was workspace plumbing.

[#9]: https://github.com/FlorentinDUBOIS/magnetar/issues/9

## Decision

1. **Four mutually-pluggable façade features** on `crates/magnetar`:
   - `crypto-aws-lc-rs` (default): aws-lc-rs, pulls rustls 0.23's
     default-on `prefer-post-quantum` hybrid X25519MLKEM768 KEX.
   - `crypto-ring`: ring.
   - `crypto-openssl`: `rustls-openssl` 0.3 (wraps system OpenSSL — see
     point 4).
   - `crypto-fips`: aws-lc-rs FIPS-validated module
     (`aws-lc-fips-sys`). Requires `cmake` + a C toolchain at build
     time.

   Default chain becomes `default = ["tokio", "crypto-aws-lc-rs"]`.

2. **`dep?/feature` propagation** down the optional dependency chain so
   `magnetar --no-default-features --features tokio,crypto-ring`
   doesn't drag in moonpool / admin / oauth2 / athenz. The
   `magnetar-runtime-tokio`, `magnetar-runtime-moonpool`,
   `magnetar-auth-athenz`, `magnetar-admin`, and `magnetar-auth-oauth2`
   crates each carry the matching `crypto-*` features locally.

3. **cfg-cascade in `tls_crypto.rs`** (one copy per runtime crate)
   resolves provider priority deterministically under
   `--all-features`:

   ```
   aws-lc-rs  >  fips  >  openssl  >  ring
   ```

   The shim exposes `install_default_provider()` (idempotent,
   `Once::call_once`) and `active_provider() -> Arc<CryptoProvider>`.
   The four production callsites replace
   `CryptoProvider::get_default().cloned().unwrap_or_else(|| Arc::new(
   ring::default_provider()))` with `active_provider()`. The only
   compile-time guard is a single
   `compile_error!("magnetar: enable at least one of crypto-{...}")`
   that fires when nothing is selected — no four-way mutex, so
   `cargo build --workspace --all-features` stays green.

4. **`deny.toml` carve-out** via cargo-deny's `wrappers = [...]`
   syntax:
   ```toml
   { name = "openssl",     wrappers = ["rustls-openssl"] }
   { name = "openssl-sys", wrappers = ["rustls-openssl"] }
   ```
   Admits `openssl` / `openssl-sys` only when their direct parent in
   the dep graph is `rustls-openssl`. Any other parent (e.g. a future
   regression to `native-tls`) still trips the deny.
   `[graph] all-features = true` is preserved.

5. **Workspace dep churn**:
   - `rustls = { features = ["std", "tls12", "logging"] }` (no
     provider sub-feature; downstream crates pick).
   - `tokio-rustls = { default-features = false }` (same reason).
   - `reqwest = { features = ["json"] }` (TLS sub-feature picked per
     `crypto-*` by the consuming crate).
   - `rustls-openssl = "0.3"` joins workspace deps, optional everywhere.

6. **ADR-0024 (cross-runtime test + coverage policy)** is invoked
   under the "Dependency bumps with no functional impact" exemption
   bullet — `magnetar-proto/` is untouched, wire bytes are unchanged
   (every provider drives the same TLS 1.3 / 1.2 state machine), the
   public `PulsarClient` API is unchanged. One smoke-test pair
   (`tests/tls_crypto_provider_smoke.rs`, 1:1 across tokio and
   moonpool) keeps `check-runtime-test-parity` honest and proves
   `active_provider()` works under whichever cell the per-provider
   matrix selects.

7. **New `cargo xtask check-crypto-matrix`** subcommand iterates the
   four `crypto-*` features in isolation
   (`tokio`-only + `tokio,moonpool` variants) so each cell is
   independently buildable. The check joins the existing local + CI
   validation chain.

## Consequences

**Easier**

- Users who need post-quantum hybrid KEX get it by default (aws-lc-rs
  + rustls 0.23's `prefer-post-quantum`) — no per-callsite code.
- FIPS deployments compile by flipping `--features
  tokio,crypto-fips`. The toolchain dependency (cmake) is the only
  build-time cost; runtime API is identical.
- Operators that need OpenSSL for organisational reasons can opt in
  via `crypto-openssl`; the rest of the dep graph stays free of
  openssl.

**Harder**

- One more dimension of CI matrix (four cells, eight if moonpool
  variants are tracked separately). Mitigated by `cargo xtask
  check-crypto-matrix` and the per-provider runner gate (PR labels
  can opt into the OpenSSL / FIPS rows on demand).
- `crypto-fips` requires `cmake` + a C toolchain at build time. Fedora
  dev hosts and most CI runners ship these by default; document the
  requirement in `README.md` § TLS crypto provider.
- The default-feature shift (`default = ["tokio", "crypto-aws-lc-rs"]`)
  silently switches downstream users from a ring-implicit build to an
  aws-lc-rs default build. This is functionally correct
  (post-quantum default is intentional), but worth calling out in
  `README.md` and PR descriptions.

**Incompatible with**

- Any future contributor who copies the historical
  `CryptoProvider::get_default().unwrap_or_else(ring)` pattern instead
  of `tls_crypto::active_provider()` — they bypass the explicit
  install path and reintroduce silent `ring` fallback.

## Partial supersession of ADR-0005

[ADR-0005](0005-rustls-only-tls.md) banned `openssl` / `openssl-sys`
outright. This ADR narrows that ban: those crates re-enter the dep
graph **only** as transitive deps of `rustls-openssl`, scoped via the
`deny.toml` `wrappers = [...]` carve-out. The rest of ADR-0005 stays in
force:

- `rustls` is still the only TLS implementation.
- `native-tls` stays banned.
- TLS-1.3 stays the wire default; TLS-1.2 stays enabled.
- The moonpool byte-pipe adapter (ADR-0006) is unchanged — it drives
  `rustls::ClientConnection` regardless of which crypto primitives
  back the session.

ADR-0005's status header is amended (not flipped to `Superseded`)
because only the openssl ban portion is touched.

## References

- [`crates/magnetar/Cargo.toml`](../../crates/magnetar/Cargo.toml) —
  `crypto-*` feature definitions.
- [`crates/magnetar-runtime-tokio/src/tls_crypto.rs`](../../crates/magnetar-runtime-tokio/src/tls_crypto.rs) —
  the cfg cascade + `active_provider()` shim.
- [`crates/magnetar-runtime-moonpool/src/tls_crypto.rs`](../../crates/magnetar-runtime-moonpool/src/tls_crypto.rs) —
  moonpool sibling.
- [`deny.toml`](../../deny.toml) — the `wrappers = ["rustls-openssl"]`
  carve-out.
- [`xtask/src/main.rs`](../../xtask/src/main.rs) —
  `check-crypto-matrix` subcommand.
- [`README.md` § TLS crypto provider](../../README.md) — user-facing
  matrix.
- [ADR-0005](0005-rustls-only-tls.md) — partially amended.
- [ADR-0006](0006-moonpool-tls-byte-pipe.md) — byte-pipe TLS adapter
  (unchanged).
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) —
  exemption invoked under "Dependency bumps with no functional
  impact".
- [Issue #9](https://github.com/FlorentinDUBOIS/magnetar/issues/9).
