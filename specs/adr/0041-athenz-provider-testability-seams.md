# ADR-0041 — Athenz provider testability seams + aws-lc-rs JWT exchange

- **Status**: Accepted
- **Date**: 2026-05-28
- **Decider**: Florentin Dubois
- **Tags**: auth, athenz, zts, testing, sans-io

## Context

[ADR-0030](0030-athenz-zts-round-trip-scope.md) (Proposed) scoped the Athenz ZTS round-trip with a specific implementation shape: an `AthenzNTokenSigner` built on the pure-Rust **`rsa`** crate minting an Athenz **N-Token**, an `AthenzZtsClient` doing the reqwest exchange, an `AthenzZtsClientFake` for the deterministic-simulation engine, and `AthenzProvider::ensure_role_token(now: Instant)` driving an expiry-aware cache — with the four-layer ADR-0024 test plan riding on those seams.

The code that actually landed diverged from that proposal on two axes:

1. **Crypto.** The N-Token / `rsa`-crate path was dropped in favour of an RS256 **JWT** signed by the in-tree [`jwt_signer`](../../crates/magnetar-auth-athenz/src/jwt_signer/) backends (`AwsLcRsSigner` / `RingSigner`, [ADR-0035](0035-pluggable-crypto-provider.md)), exchanged via the OAuth2 `client_credentials` grant.
   This is the right call: the `rsa` crate carries an unfixed timing side-channel advisory (RUSTSEC-2023-0071, Marvin attack) that `cargo deny` rejects under the workspace's empty `advisories.ignore`, and `getrandom`-sourced N-Token salt would re-introduce an un-injected entropy source forbidden by [ADR-0011](0011-clock-injection-sans-io.md). aws-lc-rs/ring RS256 is deny-clean, FIPS-capable, and deterministic (RFC 8017 §8.2).

2. **Seams never landed.** The shipped `ZtsClient` was a _concrete_ reqwest struct that read `SystemTime::now()` / `Instant::now()` internally and never received the tenant config, so its JWT claims went out with empty `iss`/`sub`/`kid` and the moonpool / differential test layers (ADR-0024 c/d) were impossible to write.
   Only the e2e layer (`crates/magnetar/tests/e2e_athenz_zts.rs`) existed.

This ADR records the as-built reconciliation: keep the landed aws-lc-rs JWT crypto, and complete the ADR-0030 _seam_ design so the cross-runtime coverage can land.

## Decision

- **`ZtsClient` is a trait.** `async fn exchange(&self, signed_jwt: &str) -> Result<RoleTokenResponse, AthenzError>`.
  Production wires `zts::HttpZtsClient` (the reqwest POST + grant-path selection); the moonpool / differential test layers inject a scripted fake.
  This is the `AthenzZtsClient` / `AthenzZtsClientFake` split ADR-0030 §moonpool prescribed, generalised to a trait object.

- **The refresh + cache state machine lives on `AthenzProvider`.** It owns the tenant `AthenzConfig`, the `Arc<dyn JwtSigner>`, the `Arc<dyn ZtsClient>`, the injected `wall_clock`, and a `parking_lot::Mutex`-guarded cached role token.
  `build_claims` stamps `iss`/`sub` = `tenant_domain.tenant_service`, `kid` = `key_id`, `aud` = `zts_url`, and `iat`/`exp` from the injected `wall_clock` (fixing the empty-claims defect).

- **Clock injection follows [ADR-0011](0011-clock-injection-sans-io.md).** `ensure_role_token(now: Instant)` / `needs_refresh(now: Instant)` take the monotonic instant as a parameter (the engine snapshots `Instant::now()` at the call site) — not an internal clock and not a `Clock` trait.
  `wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>` is the canonical `SystemTime` provider.
  Refresh fires when `now >= deadline`, where `deadline = fetch_now + (server_ttl − refresh_margin)`; default margin 5 min (matches the Athenz Java client and ADR-0030).

- **Construction surface.** `with_role_token` (pinned token, never refreshes); `with_default_signer` (cfg-active in-tree signer + `HttpZtsClient`); `builder()` (custom signer / `ZtsClient` / `wall_clock`).
  `AthenzProvider::with_zts_client` is removed — the builder subsumes the custom-client path now that the signer and client are separate objects.

- **No production caller breakage.** No code outside the auth crate's own tests + the e2e fixture constructs `AthenzProvider`, so this is a free refactor of a not-yet-consumed surface.

## Consequences

- **Four-layer ADR-0024 coverage now lands** (the gap this ADR closes): (a) `magnetar-auth-athenz` units (`build_claims`, `needs_refresh`, `HttpZtsClient` URL validation, the existing signer byte-identity); (b) `magnetar-runtime-tokio/tests/athenz_zts_round_trip.rs` — real `HttpZtsClient` against wiremock; (c) `magnetar-runtime-moonpool/tests/athenz_refresh_edge.rs` — scripted fake + injected `now`; (d) `magnetar-differential/tests/athenz_auth_data_equivalence.rs` — byte-identical JWT + auth_data across engine-shaped drivers; plus the pre-existing e2e fixture.
  The tokio and moonpool layers add three tests each, preserving the 1:1 `check-runtime-test-parity` count.

- **`rsa` / `sha2` / `getrandom` are NOT added** to the dependency graph — the security and determinism reasons above stand.
  ADR-0030's dependency-choice clause (b)/(c) is void.

- **`magnetar-auth-athenz` deps**: `+ parking_lot` (always; the cache mutex) and `+ async-trait` (optional, `zts` feature; the `ZtsClient` trait).
  `tokio` is dropped (the cache moved from `tokio::sync::Mutex` to `parking_lot::Mutex`).

- **`check-no-internal-clock`** is unaffected: it scans `magnetar-proto` only, and the auth crate's production `SystemTime::now` lives behind the injectable `wall_clock` default.

## Status

Accepted (2026-05-28).
Supersedes [ADR-0030](0030-athenz-zts-round-trip-scope.md).

## References

- [ADR-0030](0030-athenz-zts-round-trip-scope.md) — superseded; original ZTS round-trip scope (rsa-crate N-Token shape).
- [ADR-0011](0011-clock-injection-sans-io.md) — `now: Instant` + `wall_clock` injection.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — four-layer test plan.
- [ADR-0035](0035-pluggable-crypto-provider.md) — aws-lc-rs / ring crypto-provider matrix that backs the signer.
- [`docs/pip-features.md#athenz-auth-provider`](../../docs/pip-features.md#athenz-auth-provider) — client-side configuration + cross-runtime coverage.
