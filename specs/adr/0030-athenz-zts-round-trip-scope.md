# ADR-0030 — Athenz ZTS round-trip scope for v0.2.0

- **Status**: Proposed
- **Date**: 2026-05-26
- **Decider**: Florentin Dubois
- **Tags**: auth, athenz, zts, v0.2.0, scope

## Context

[ADR-0010](0010-v0-1-full-java-parity.md) ships full Java-client
parity at v0.1.0, but explicitly defers the **Athenz ZTS round-trip**
to v0.2.0. The deferral was locked in
[ADR-0026 §D3](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md):
shipping a fake-token stub and calling Athenz "✅" would lie to the
parity matrix. The honest position is "PLAIN + pre-fetched role
token in v0.1.0; ZTS round-trip in v0.2.0", and that is what
`docs/parity-status.md` and `README.md`'s parity matrix already
record.

Today the scaffolding lives in
[`crates/magnetar-auth-athenz/src/lib.rs`](../../crates/magnetar-auth-athenz/src/lib.rs).
`AthenzProvider::new(AthenzConfig { tenant_domain, tenant_service,
provider_domain, key_id, private_key_pem, zts_url, principal_header,
role_header })` builds a provider that — without a pre-fetched token —
returns `AuthError::Unsupported("Athenz ZTS round-trip not yet
implemented; provide a pre-fetched role token via
AthenzProvider::with_role_token")`
(see `lib.rs:91-100`). The `with_role_token` escape hatch keeps the
auth method usable for callers who run a sidecar ZTS client. The
`method()` returns `"athenz"`, matching Pulsar Java's
`AuthenticationAthenz`.

Apache Pulsar's reference is
`org.apache.pulsar.client.impl.auth.AuthenticationAthenz`. On the
client side it loads tenant + provider configuration from a JSON
parameter blob, mints an Athenz N-Token (PrincipalToken) signed with
the tenant private key, exchanges the N-Token for a RoleToken at
the ZTS HTTP endpoint, caches the role token until expiry minus a
refresh margin, and presents the role token bytes as the Pulsar
`auth_data`. The Athenz client side itself is the
`com.yahoo.athenz:athenz-zts-java-client` Java library, which
magnetar would have to reimplement in Rust.

This ADR locks the scope of the v0.2.0 ZTS round-trip implementation
so the milestone has a sizing contract and a concrete acceptance
boundary.

## Decision

- **Wire-protocol delta vs. current vendored PulsarApi.proto: none.**
  Athenz authentication sits **above** the Pulsar wire: the role
  token bytes are passed as `auth_data` on the existing CONNECT
  frame (already supported since M0). The ZTS round-trip is an HTTPS
  exchange between client and the Athenz ZTS service, not between
  client and broker. No `magnetar-proto` proto bump required.

- **`magnetar-proto` state-machine additions.** No new commands
  emitted or consumed. The only proto-level change is **token
  expiry-aware refresh**: `AuthProvider` already has an `initial()`
  contract; for Athenz we add a `refresh(now: Instant) -> Option<Bytes>`
  hook to the existing provider trait (already present today for
  `OAuth2ClientCredentials` per [ADR-0014](0014-oauth2-client-credentials-caching.md)),
  so the proto state machine treats RoleToken refresh the same way
  it treats OAuth2 access-token refresh. No new events, no new
  handle types.

- **`magnetar-runtime-tokio` surface.** The new public method is
  `AthenzProvider::ensure_role_token(now: Instant)` which, when no
  cached unexpired token exists, performs:
  1. Mint an Athenz **N-Token** (PrincipalToken) signed with the
     tenant RSA private key. Format: semicolon-delimited
     `v=S1;d=<tenant_domain>;n=<tenant_service>;h=<host>;a=<rand>;t=<now>;e=<expiry>;k=<key_id>;s=<base64-rsa-sha256-sig>`.
  2. HTTP GET `${zts_url}/domain/${provider_domain}/token` with the
     N-Token as the `Athenz-Principal-Auth` header, requesting a
     **RoleToken** for the provider domain.
  3. Parse the RoleToken response (JSON: `{ "token": "...",
     "expiryTime": ... }`).
  4. Cache the token in an `Arc<parking_lot::Mutex<Option<RoleToken>>>`
     with the expiry timestamp; refresh on `now + refresh_margin >=
     expiry`. Default refresh margin: 5 minutes (matches Athenz Java
     client's default).
  No new builder; `AthenzProvider::new(AthenzConfig)` already exists
  and now becomes fully functional (no `with_role_token` escape
  hatch required, but the existing one stays for users running a
  sidecar).

- **`magnetar-runtime-moonpool` port.** ZTS round-trips are HTTPS;
  HTTPS is out of scope for moonpool's sans-io simulator. Two
  components separate cleanly:
  (a) **`AthenzNTokenSigner`** — pure RSA-SHA256 sign + token
  marshalling. **Sans-io component.** Lives in `magnetar-auth-athenz`
  and is exercised in both engines via mirrored unit tests.
  (b) **`AthenzZtsClient`** — the `reqwest`-backed HTTPS exchange.
  Tokio-only at runtime. The moonpool port ships an
  `AthenzZtsClientFake` that returns scripted RoleToken responses
  with controllable expiry timestamps, exercising the **refresh
  logic and cache mechanics** without HTTPS.

- **Dependency choice: `reqwest` (already in the workspace) +
  `rsa` + `sha2` + `base64`.** Reasons:
  (a) `reqwest` is already pulled in by `magnetar-admin` and
  `magnetar-auth-oauth2`; reusing it avoids a second HTTPS stack
  and preserves [ADR-0005](0005-rustls-only-tls.md) (`reqwest` is
  pinned to `rustls-tls`, no `native-tls`).
  (b) `rsa` crate (pure-Rust RSA) for the N-Token signature.
  Preserves the no-`openssl` invariant. Pulsar Java's Athenz client
  uses Bouncy Castle; magnetar uses the `rsa` crate's `Pkcs1v15Sign`
  + SHA-256 path which produces the same RSASSA-PKCS1-v1_5
  signatures.
  (c) `base64` for the signature encoding (URL-safe-without-padding
  per Athenz spec).
  (d) `serde_json` for parsing RoleToken JSON.

- **N-Token vs. SVC-Token.** Athenz has two principal-token flavours.
  v0.2.0 implements **N-Token only** (tenant private key path).
  SVC-Token (the Athenz-issued service-identity token) requires
  ZMS-side provisioning, an `instance_id` claim, and the
  `Athenz-Service-Auth` header semantics — out of scope (deferred to
  v0.3.0+).

## Consequences

- **Test layers per ADR-0024 (4-layer):**
  (a) `magnetar-proto` unit: provider `refresh(now)` contract;
  cache-hit / cache-miss / expiry-window transitions; injected
  `now: Instant` per [ADR-0011](0011-clock-injection-sans-io.md).
  (b) `magnetar-runtime-tokio`: integration test using `wiremock`
  to stand in for the ZTS endpoint, asserts N-Token signature
  format on the request and that the role token is presented as
  `auth_data` on CONNECT.
  (c) `magnetar-runtime-moonpool`: identical refresh / cache
  test driven by `AthenzZtsClientFake` + scripted virtual time.
  (d) `magnetar-differential`: equivalence test that, given the
  same scripted ZTS responses and the same `Instant` schedule,
  produces the same CONNECT auth_data bytes on both engines.

- **E2E fixture needs.** A `docker compose` bringing up
  `apachepulsar/pulsar:4.0.4` configured with
  `authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderAthenz`
  + an Athenz ZTS server (`athenz/athenz-zts-server`) +
  `athenz/athenz-zms-server` on a shared Docker network. The
  Athenz server is a non-trivial fixture: it needs initial tenant
  + provider domains seeded via ZMS admin REST. A helper script
  (`tests/fixtures/athenz-bootstrap.sh`) seeds the test domain
  and writes the tenant RSA private key. Gated by
  `#[ignore = "e2e: requires Docker + Athenz ZMS/ZTS"]` and the
  `e2e` cargo feature. <!-- TODO: verify athenz-zts-server image
  tag; the Athenz upstream publishes under `athenz/athenz-zts-server`
  but version pinning is the test-author's call. -->

- **LOC estimate.** ~700–1000 LOC total.
  Breakdown: ~200 LOC N-Token signer + format; ~250 LOC
  `AthenzZtsClient` (reqwest + retry + cache); ~100 LOC
  `AthenzZtsClientFake`; ~150 LOC tests (4-layer); ~100 LOC
  e2e fixture + bootstrap.

- **Security implications.** The tenant **RSA private key** is the
  hot artefact. Decisions: (a) the key is held as a `String` PEM in
  `AthenzConfig` per the current scaffold (no zeroization), but the
  parsed PKCS#8 DER bytes live behind `Zeroizing<Vec<u8>>` in the
  concrete signer backends — **closed by the
  `crates/magnetar-auth-athenz/src/jwt_signer/{aws_lc_rs,ring}.rs`
  landing**. Both `AwsLcRsSigner` and `RingSigner` wrap the parsed
  DER under `zeroize::Zeroizing` and rebuild the `RsaKeyPair` on each
  sign (the aws-lc-rs / ring `RsaKeyPair` types are opaque C-allocated
  wrappers around `EVP_PKEY` / BIGNUM and cannot be made
  `Zeroize`-friendly from Rust); the extra cost is one PKCS#8 ASN.1
  parse per N-token mint, negligible alongside the 2048-bit modular
  exponentiation. (b) N-Token signature uses
  RSASSA-PKCS1-v1_5 with SHA-256, matching Athenz; the `rsa` crate
  is reviewed and pinned; (c) the cached RoleToken is held in
  memory only — never written to disk; (d) ZTS HTTPS uses
  `rustls` with the system root CA set, not a private CA bundle,
  unless `AthenzConfig` is extended with a `zts_ca_bundle` field
  (deferred to v0.2.x); (e) clock injection via
  [ADR-0011](0011-clock-injection-sans-io.md) means N-Token
  `t` / `e` claims are deterministic in simulation but real
  `SystemTime` in production. **No `openssl`**: preserves
  [ADR-0005](0005-rustls-only-tls.md).

## Status

Proposed (awaiting Florentin sign-off, 2026-05-26)

## References

- [ADR-0005](0005-rustls-only-tls.md) — `rustls` only, no
  `openssl`.
- [ADR-0009](0009-pulsar-4-minimum.md) — Pulsar 4.0+ minimum.
- [ADR-0010](0010-v0-1-full-java-parity.md) — v0.1.0 full Java
  parity; Athenz deferral.
- [ADR-0011](0011-clock-injection-sans-io.md) — clock injection
  for token timestamps + cache expiry.
- [ADR-0014](0014-oauth2-client-credentials-caching.md) —
  OAuth2 token cache with injectable `Clock`; same pattern reused
  here.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) —
  four-layer test plan binding.
- [ADR-0026 §D3](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
  — design-time deferral rationale.
- Apache Pulsar Java —
  `org.apache.pulsar.client.impl.auth.AuthenticationAthenz`.
- Athenz ZTS docs —
  <https://athenz.github.io/athenz/zts_api/>
  <!-- TODO: verify the precise ZTS REST endpoint path matches
  pulsar-java client's request. -->
- Pulsar Athenz docs —
  <https://pulsar.apache.org/docs/security-athenz/>
- `crates/magnetar-auth-athenz/src/lib.rs:91-100` — current
  `AuthError::Unsupported` surface.
