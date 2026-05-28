# Athenz auth provider

The [`magnetar-auth-athenz`](../crates/magnetar-auth-athenz/) crate
ships the client side of Apache Pulsar's Athenz authentication method:
the tenant signs an N-token / OAuth2 `client_assertion` JWT with its
RSA private key, exchanges it at the Athenz ZTS endpoint for a role
token, and presents the role-token bytes as the Pulsar CONNECT
`auth_data` payload.

This document covers the **client-side configuration matrix** — which
backend signs the JWT, how to wire it from the
[`magnetar`](../crates/magnetar/) façade, and what the deterministic-
signature guarantee buys callers that run the moonpool simulation
engine. The protocol-level scope (N-token format, refresh policy,
zeroize posture) is locked in
[ADR-0030](../specs/adr/0030-athenz-zts-round-trip-scope.md); the
cross-workspace crypto-provider selection is locked in
[ADR-0035](../specs/adr/0035-pluggable-crypto-provider.md).

## Surface at a glance

```text
AthenzProvider::with_role_token(config, role_token)   ← out-of-band sidecar
AthenzProvider::with_zts_client(config, zts_client)   ← caller-supplied JwtSigner
AthenzProvider::with_default_signer(config)            ← in-tree backend, cfg-active
```

The first path skips the JWT signer entirely — useful when a sidecar
(`zts-agent`, custom mint service) already holds the role token. The
second is the escape hatch for installations that need an HSM-backed
signer, a `jsonwebtoken` integration, or a key-management story the
in-tree backends do not cover. The third is the new path landed in
this changeset.

## Crypto-provider matching

The two concrete signer backends are gated on the same feature flags
that select the rustls crypto provider (ADR-0035). The mapping is
deliberately 1:1 so a single feature flip switches every consumer
(rustls + Athenz signer + PIP-4 message encryption) at once and the
workspace stays internally consistent.

| Workspace feature | rustls provider | Athenz signer            | PIP-4 message crypto |
| ----------------- | --------------- | ------------------------ | -------------------- |
| `crypto-aws-lc-rs` (default) | aws-lc-rs (with post-quantum hybrid KEX) | [`AwsLcRsSigner`] | aws-lc-rs (always)   |
| `crypto-ring`     | ring            | [`RingSigner`]           | aws-lc-rs (always)   |
| `crypto-openssl`  | rustls-openssl  | _none_ (use [`with_zts_client`]) | aws-lc-rs (always) |
| `crypto-fips`     | aws-lc-rs FIPS  | _none_ (use [`with_zts_client`]) | aws-lc-rs (always) |

`crypto-openssl` and `crypto-fips` do not currently ship an Athenz
signer because:

- `crypto-openssl` carves OpenSSL into the graph **only** as a
  transitive dep of `rustls-openssl` (ADR-0035 §4 `deny.toml`
  `wrappers = [...]` carve-out). Adding an `openssl`-backed
  signer would re-open the ban; callers wanting it should wire
  [`with_zts_client`] with their own `openssl::sign` implementation.
- `crypto-fips` already pulls aws-lc-rs (FIPS module). FIPS callers
  who also want the in-tree signer should enable
  `crypto-aws-lc-rs` + `crypto-fips` simultaneously; the cfg cascade
  picks the FIPS-validated aws-lc-rs provider for rustls and the
  same library backs the signer (FIPS-validated RSA sign path).

When both `crypto-aws-lc-rs` and `crypto-ring` are enabled (e.g.
`--all-features`) the cfg cascade in
[`crates/magnetar-auth-athenz/src/jwt_signer/mod.rs`](../crates/magnetar-auth-athenz/src/jwt_signer/mod.rs)
picks aws-lc-rs first, matching the ADR-0035 priority
`aws-lc-rs > fips > openssl > ring`. The ring path stays compiled and
publicly callable via [`RingSigner`] in case a downstream consumer
wants to instantiate it explicitly.

[`AwsLcRsSigner`]: ../crates/magnetar-auth-athenz/src/jwt_signer/aws_lc_rs.rs
[`RingSigner`]: ../crates/magnetar-auth-athenz/src/jwt_signer/ring.rs
[`with_zts_client`]: ../crates/magnetar-auth-athenz/src/lib.rs

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
// pump the cache before the connection's first use
provider.refresh_via_zts().await?;
```

Requires `magnetar-auth-athenz` to be built with both
`crypto-aws-lc-rs` (or `crypto-ring`) **and** `zts`. The façade's
`auth-athenz-zts` feature propagates `zts`; the workspace's
`crypto-*` features propagate the matching backend.

### With a caller-supplied signer

```rust
use std::sync::Arc;
use magnetar_auth_athenz::{AthenzConfig, AthenzProvider, zts::{JwtSigner, ZtsClient, ZtsGrant}};

#[derive(Debug)]
struct HsmSigner { /* ... */ }
impl JwtSigner for HsmSigner { /* ... */ }

let signer: Arc<dyn JwtSigner> = Arc::new(HsmSigner { /* ... */ });
let client = Arc::new(ZtsClient::new(&config.zts_url, ZtsGrant::default(), signer)?);
let provider = AthenzProvider::with_zts_client(config, client);
```

## ADR-0030 close-out: zeroization

Both backends wrap the parsed PKCS#8 DER bytes in
[`zeroize::Zeroizing`] so the secret material is wiped from memory
when the signer drops. The aws-lc-rs / ring `RsaKeyPair` types
themselves are opaque wrappers around C-allocated `EVP_PKEY` / BIGNUM
structures and cannot be made `Zeroize`-friendly from Rust. The
implementation therefore stores the **DER bytes** under
`Zeroizing<Vec<u8>>` and reconstructs the keypair on each sign. The
trade-off:

- **Cost.** One PKCS#8 ASN.1 parse + RSA structure rebuild per sign
  call. Negligible alongside the 2048-bit modular exponentiation that
  the signature itself drives.
- **Benefit.** A hard guarantee that the parsed private key does not
  linger in memory after the signer drops, closing the deferral
  recorded in
  [ADR-0030 §Security implications (a)](../specs/adr/0030-athenz-zts-round-trip-scope.md).

The `AthenzConfig::private_key_pem` field itself is **not** zeroized —
the PEM string is owned by the caller's configuration scope and is
expected to be redacted via the `Debug` impl (`<redacted>` sentinel)
rather than wiped on drop. Callers handling rotating secrets should
zero their own PEM after constructing the signer.

## Deterministic signatures

RSASSA-PKCS1-v1_5 with SHA-256 is deterministic per RFC 8017 §8.2 —
the same key + payload produces byte-identical signature bytes
across calls and across libraries. This buys two properties:

1. **moonpool reproducibility.** With `wall_clock` frozen at the call
   site (sans-io clock injection per [ADR-0011](../specs/adr/0011-clock-injection-sans-io.md))
   the entire JWT emission is bit-for-bit deterministic. The same
   `(seed, commit)` pair always produces the same network bytes —
   load-bearing for the [moonpool-engine](moonpool-engine.md) chaos
   pack.
2. **Cross-backend equivalence.** aws-lc-rs and ring must produce
   identical signature bytes for the same key + payload. Pinned by
   the
   [`magnetar_auth_athenz::jwt_signer::ring::tests::cross_backend_signature_byte_identity`](../crates/magnetar-auth-athenz/src/jwt_signer/ring.rs)
   test (gated on both features enabled). If this assertion ever
   fails, that is a bug in one of the libraries (we have produced a
   reproducer).

## What is _not_ here

- **ZTS e2e against a real broker.** Tracked in
  [`follow-ups.md` §4](follow-ups.md#4-athenz-zts-e2e-fixture).
  Blocked on the Dockerised `athenz/athenz-zts-server` fixture; the
  in-tree tests cover the JWT mint + the `ZtsClient` cache.
- **ES256 (EC) keys.** The /goal mentioned ES256 as a fallback for EC
  keys, but Pulsar's Athenz integration and the Athenz Java client
  itself only emit RS256. The shape is ready (the JWS header builder
  already takes the alg as a parameter) but no consumer requests ES256
  today.
- **SVC-token flow.** Locked in
  [ADR-0030](../specs/adr/0030-athenz-zts-round-trip-scope.md) as
  v0.3.0+ scope. Requires ZMS-side provisioning and an `instance_id`
  claim that the current `ZtsClaims` struct does not model.
