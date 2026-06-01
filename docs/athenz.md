# Athenz auth provider

The [`magnetar-auth-athenz`](../crates/magnetar-auth-athenz/) crate ships the client side of Apache Pulsar's Athenz authentication method: the tenant signs an N-token / OAuth2 `client_assertion` JWT with its RSA private key, exchanges it at the Athenz ZTS endpoint for a role token, and presents the role-token bytes as the Pulsar CONNECT `auth_data` payload.

This document covers the **client-side configuration matrix** — which backend signs the JWT, how to wire it from the [`magnetar`](../crates/magnetar/) façade, and what the deterministic- signature guarantee buys callers that run the moonpool simulation engine.
The as-built design — the testability seams (`ensure_role_token`, injected `wall_clock`, the pluggable `ZtsClient` trait) and the aws-lc-rs / ring RS256 JWT exchange — is locked in [ADR-0041](../specs/adr/0041-athenz-provider-testability-seams.md), which supersedes the originally-proposed [ADR-0030](../specs/adr/0030-athenz-zts-round-trip-scope.md); the cross-workspace crypto-provider selection is locked in [ADR-0035](../specs/adr/0035-pluggable-crypto-provider.md).

## Surface at a glance

```text
AthenzProvider::with_role_token(config, role_token)   ← out-of-band sidecar (pinned token)
AthenzProvider::with_default_signer(config)            ← in-tree backend + HttpZtsClient
AthenzProvider::builder()                              ← custom signer / ZtsClient / wall_clock
    .config(config).signer(signer).zts_client(client).build()
```

The refresh + cache state machine lives on the provider: `ensure_role_token(now: Instant)` performs a ZTS exchange when the cache is missing or within `refresh_margin` of expiry; `needs_refresh(now)` queries that decision; `AuthProvider::initial()` returns the cached role-token bytes (or `AuthError::Unsupported` before the first fetch).

- `with_role_token` skips the JWT signer entirely — useful when a sidecar (`zts-agent`, custom mint service) already holds the role token; the pinned token never expires and `ensure_role_token` is a no-op.
- `with_default_signer` wires the cfg-active in-tree signer to a production [`zts::HttpZtsClient`].
- `builder()` is the general path: supply a custom [`zts::JwtSigner`] (HSM, `jsonwebtoken`, …), a custom [`zts::ZtsClient`] (the deterministic-simulation tests inject a scripted fake here), and an injected `wall_clock` for reproducible JWT `iat` / `exp`.

## Crypto-provider matching

The two concrete signer backends are gated on the same feature flags that select the rustls crypto provider (ADR-0035).
The mapping is deliberately 1:1 so a single feature flip switches every consumer (rustls + Athenz signer + PIP-4 message encryption) at once and the workspace stays internally consistent.

| Workspace feature            | rustls provider                          | Athenz signer            | PIP-4 message crypto |
| ---------------------------- | ---------------------------------------- | ------------------------ | -------------------- |
| `crypto-aws-lc-rs` (default) | aws-lc-rs (with post-quantum hybrid KEX) | [`AwsLcRsSigner`]        | aws-lc-rs (always)   |
| `crypto-ring`                | ring                                     | [`RingSigner`]           | aws-lc-rs (always)   |
| `crypto-openssl`             | rustls-openssl                           | _none_ (use [`builder`]) | aws-lc-rs (always)   |
| `crypto-fips`                | aws-lc-rs FIPS                           | _none_ (use [`builder`]) | aws-lc-rs (always)   |

`crypto-openssl` and `crypto-fips` do not currently ship an Athenz signer because:

- `crypto-openssl` carves OpenSSL into the graph **only** as a transitive dep of `rustls-openssl` (ADR-0035 §4 `deny.toml` `wrappers = [...]` carve-out).
  Adding an `openssl`-backed signer would re-open the ban; callers wanting it should use [`builder`] with their own `openssl::sign` implementation.
- `crypto-fips` already pulls aws-lc-rs (FIPS module).
  FIPS callers who also want the in-tree signer should enable `crypto-aws-lc-rs` + `crypto-fips` simultaneously; the cfg cascade picks the FIPS-validated aws-lc-rs provider for rustls and the same library backs the signer (FIPS-validated RSA sign path).

When both `crypto-aws-lc-rs` and `crypto-ring` are enabled (e.g. `--all-features`) the cfg cascade in [`crates/magnetar-auth-athenz/src/jwt_signer/mod.rs`](../crates/magnetar-auth-athenz/src/jwt_signer/mod.rs) picks aws-lc-rs first, matching the ADR-0035 priority `aws-lc-rs > fips > openssl > ring`.
The ring path stays compiled and publicly callable via [`RingSigner`] in case a downstream consumer wants to instantiate it explicitly.

[`AwsLcRsSigner`]: ../crates/magnetar-auth-athenz/src/jwt_signer/aws_lc_rs.rs
[`RingSigner`]: ../crates/magnetar-auth-athenz/src/jwt_signer/ring.rs
[`builder`]: ../crates/magnetar-auth-athenz/src/lib.rs

## Usage

### From the façade with the default backend

```rust
use magnetar_auth_athenz::{AthenzConfig, AthenzProvider};

let config = AthenzConfig {
    tenant_domain:    "mydomain".to_owned(),
    tenant_service:   "myservice".to_owned(),
    provider_domain:  "pulsar.tenant".to_owned(),
    key_id:           "key0".to_owned(),
    private_key_pem:  std::fs::read_to_string("tenant.pkcs8.pem")?,
    zts_url:          "https://zts.example.com:4443/zts/v1/".to_owned(),
    principal_header: None,
    role_header:      None,
};
let provider = AthenzProvider::with_default_signer(config)?;
// pump the cache before the connection's first use; `now` is the
// engine-snapshotted monotonic instant (sans-io clock injection).
provider.ensure_role_token(std::time::Instant::now()).await?;
```

Requires `magnetar-auth-athenz` to be built with both `crypto-aws-lc-rs` (or `crypto-ring`) **and** `zts`.
The façade's `auth-athenz-zts` feature propagates `zts`; the workspace's `crypto-*` features propagate the matching backend.

### With a caller-supplied signer

```rust
use std::sync::Arc;
use magnetar_auth_athenz::{AthenzConfig, AthenzProvider, zts::{HttpZtsClient, JwtSigner, ZtsGrant}};

#[derive(Debug)]
struct HsmSigner { /* ... */ }
impl JwtSigner for HsmSigner { /* ... */ }

let signer: Arc<dyn JwtSigner> = Arc::new(HsmSigner { /* ... */ });
let client = Arc::new(HttpZtsClient::new(&config.zts_url, ZtsGrant::default())?);
let provider = AthenzProvider::builder()
    .config(config)
    .signer(signer)
    .zts_client(client)
    .build()?;
```

The `ZtsClient` trait is the HTTPS seam: production wires [`zts::HttpZtsClient`], while the moonpool / differential test layers inject a scripted fake so the refresh + cache mechanics are exercised without an HTTP endpoint (ADR-0030 §moonpool, ADR-0041).

## ADR-0030 close-out: zeroization

Both backends wrap the parsed PKCS#8 DER bytes in [`zeroize::Zeroizing`] so the secret material is wiped from memory when the signer drops.
The aws-lc-rs / ring `RsaKeyPair` types themselves are opaque wrappers around C-allocated `EVP_PKEY` / BIGNUM structures and cannot be made `Zeroize`-friendly from Rust.
The implementation therefore stores the **DER bytes** under `Zeroizing<Vec<u8>>` and reconstructs the keypair on each sign.
The trade-off:

- **Cost.** One PKCS#8 ASN.1 parse + RSA structure rebuild per sign call.
  Negligible alongside the 2048-bit modular exponentiation that the signature itself drives.
- **Benefit.** A hard guarantee that the parsed private key does not linger in memory after the signer drops, closing the deferral recorded in [ADR-0030 §Security implications (a)](../specs/adr/0030-athenz-zts-round-trip-scope.md).

The `AthenzConfig::private_key_pem` field itself is **not** zeroized — the PEM string is owned by the caller's configuration scope and is expected to be redacted via the `Debug` impl (`<redacted>` sentinel) rather than wiped on drop.
Callers handling rotating secrets should zero their own PEM after constructing the signer.

## Deterministic signatures

RSASSA-PKCS1-v1_5 with SHA-256 is deterministic per RFC 8017 §8.2 — the same key + payload produces byte-identical signature bytes across calls and across libraries.
This buys two properties:

1. **moonpool reproducibility.** With `wall_clock` frozen at the call site (sans-io clock injection per [ADR-0011](../specs/adr/0011-clock-injection-sans-io.md)) the entire JWT emission is bit-for-bit deterministic.
   The same `(seed, commit)` pair always produces the same network bytes — load-bearing for the [moonpool-engine](moonpool-engine.md) chaos pack.
2. **Cross-backend equivalence.** aws-lc-rs and ring must produce identical signature bytes for the same key + payload.
   Pinned by the [`magnetar_auth_athenz::jwt_signer::ring::tests::cross_backend_signature_byte_identity`](../crates/magnetar-auth-athenz/src/jwt_signer/ring.rs) test (gated on both features enabled).
   If this assertion ever fails, that is a bug in one of the libraries (we have produced a reproducer).

## End-to-end testing against a real ZTS

End-to-end coverage lives in [`crates/magnetar/tests/e2e_athenz_zts.rs`](../crates/magnetar/tests/e2e_athenz_zts.rs) behind `feature = "e2e,auth-athenz-zts"` and is `#[ignore]`'d by default (parity with every other `e2e_*.rs` test).
Run with:

```sh
cargo test --features auth-athenz-zts \
  -p magnetar --test e2e_athenz_zts -- --nocapture --include-ignored
```

### Hybrid fixture shape

The Athenz ZTS server is operationally non-trivial to spin up in testcontainers — the upstream image expects a co-deployed ZMS (manager), per-tenant public-key seeding via the ZMS admin REST, and a chained TLS server certificate (Athenz's [`make deploy-dev`](https://github.com/AthenZ/athenz/blob/master/docker/README.md) orchestrates four containers + a cert-bootstrap pre-flight that together take ~15 minutes to build).
The test file therefore takes a hybrid shape:

| Test                                                       | Fixture                                     | What it proves                                                                                                                                                                                       |
| ---------------------------------------------------------- | ------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `e2e_athenz_zts_refresh_then_cached_initial`               | wiremock-stub                               | `ensure_role_token` populates the cache; `AuthProvider::initial()` returns the cached bytes; bearer header is a compact-JWS three-segment payload from the §3 signer.                                |
| `e2e_athenz_zts_expiry_aware_refresh_fires_on_near_expiry` | wiremock-stub                               | Driving the injected `now: Instant` past the cached deadline (`t0 + ttl − refresh_margin`) triggers a fresh exchange and rotates the cached bytes — no wall-clock wait.                              |
| `e2e_athenz_zts_cached_token_used_on_auth_challenge`       | wiremock-stub                               | `AuthChallengeState::handle_challenge` routes through `respond_to_challenge`, which echoes the cached role-token bytes verbatim; no extra ZTS round-trip.                                            |
| `e2e_athenz_zts_image_pulls_and_serves_status`             | Docker (`athenz/athenz-zts-server:1.12.41`) | The upstream image is pullable and `testcontainers-rs` port mapping works; if the host lacks a co-deployed ZMS the test surfaces the documented "expected without ZMS bootstrap" warning and passes. |

The wiremock tests run against a real `reqwest` client + real HTTP server (deterministic responses, no Docker dep — wiremock binds an ephemeral local port).
They cover every behavioural assertion the follow-up `/goal` enumerates.
The Docker probe wires the upstream image into the e2e surface so a downstream consumer with a fully-bootstrapped ZMS+ZTS topology can layer their own pre-seed step on top.

### Full ZMS+ZTS topology

Full ZMS+ZTS+cert-bootstrap testing requires running the Athenz `make deploy-dev` topology as a shared CI fixture (four containers, MySQL persistence, a CA hierarchy, ZMS-side `zms-cli add-public-key` seeding for the tenant).
Adding it would replace the `#[ignore]`'d Docker probe with a full multi-container compose fixture similar to [`crates/magnetar/tests/fixtures/docker-compose.replicated-subs.yml`](../crates/magnetar/tests/fixtures/docker-compose.replicated-subs.yml).
That work is out of scope for the current Athenz surface.

## Cross-runtime test coverage (ADR-0024)

The testability seams (injected `wall_clock`, the `ZtsClient` trait, and `ensure_role_token(now)`) let the Athenz provider carry the full four-layer coverage ADR-0024 mandates — the same bar SASL meets:

| Layer            | File                                                                                                                                   | What it pins                                                                                                                                                              |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| (a) unit         | [`src/lib.rs`](../crates/magnetar-auth-athenz/src/lib.rs) / [`src/zts.rs`](../crates/magnetar-auth-athenz/src/zts.rs)                  | `build_claims` populates `iss`/`sub`/`kid` + the `iat`/`exp` window; `needs_refresh` cache transitions; `HttpZtsClient` URL validation.                                   |
| (b) tokio        | [`magnetar-runtime-tokio/tests/athenz_zts_round_trip.rs`](../crates/magnetar-runtime-tokio/tests/athenz_zts_round_trip.rs)             | The real `HttpZtsClient` against a `wiremock` ZTS stub: mint+cache, cache-hit absorption, expiry-driven rotation.                                                         |
| (c) moonpool     | [`magnetar-runtime-moonpool/tests/athenz_refresh_edge.rs`](../crates/magnetar-runtime-moonpool/tests/athenz_refresh_edge.rs)           | A scripted `ZtsClient` fake + injected `now: Instant`: refresh fires exactly at the virtual deadline; `with_role_token` bypass; ZTS failure leaves the cache un-poisoned. |
| (d) differential | [`magnetar-differential/tests/athenz_auth_data_equivalence.rs`](../crates/magnetar-differential/tests/athenz_auth_data_equivalence.rs) | Two independently-built providers on the same `(now, action)` schedule mint byte-identical JWTs and cache byte-identical CONNECT `auth_data`.                             |

The moonpool / differential layers never speak HTTPS — they inject the scripted `ZtsClient` fake, exactly as [ADR-0030 §moonpool](../specs/adr/0030-athenz-zts-round-trip-scope.md) and [ADR-0041](../specs/adr/0041-athenz-provider-testability-seams.md) prescribe — while the aws-lc-rs signer still mints a real, deterministic RS256 JWT.

## What is _not_ here

- **ES256 (EC) keys.** The /goal mentioned ES256 as a fallback for EC keys, but Pulsar's Athenz integration and the Athenz Java client itself only emit RS256.
  The shape is ready (the JWS header builder already takes the alg as a parameter) but no consumer requests ES256 today.
- **SVC-token flow.** Out of scope per [ADR-0030](../specs/adr/0030-athenz-zts-round-trip-scope.md).
  Requires ZMS-side provisioning and an `instance_id` claim that the current `ZtsClaims` struct does not model.
