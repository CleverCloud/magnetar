# Open Follow-Ups

Consolidated tracker for known open work. Each entry lists the gap,
the reason it stays open, and (where actionable) a `/goal …` block
ready to be copy-pasted verbatim into a fresh session for an agent
team to pick up.

For the public-facing parity status, see
[`parity-status.md`](parity-status.md) and the
[parity matrix in the README](../README.md#java-client-parity-matrix).

This file is the **single source of truth** for what is intentionally
deferred or blocked. Anything not listed below is either landed
(check `git log` for the implementation reference), or explicitly out
of scope for v0.2.0 ([ADR-0026](../specs/adr/0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
§D-series, [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md),
[ADR-0032](../specs/adr/0032-pip-466-v5-client-surface-scope.md)).

**API stability stance.** The crate is not yet published. Breaking
API changes are acceptable when they improve correctness, ergonomics,
or layering; ship with `BREAKING CHANGE:` in the commit body so the
eventual changelog flags them.

---

## Index

Status tags: ⚡ ready to dispatch · 🔗 blocked on external dep ·
⏳ blocked on upstream PIP release · 🧠 needs design decision ·
🟡 deferred (not load-bearing).

| # | Item | Status |
| - | --- | --- |
| 1 | [Moonpool vectored I/O](#1-moonpool-vectored-io) | 🔗 [PierreZ/moonpool#111](https://github.com/PierreZ/moonpool/issues/111) |
| 2 | [Engine-generic builder & V5 unified lift (§2 phantom-E + §3 per-surface lifts + §4 V5)](#2-engine-generic-builder--v5-unified-lift) | ✅ landed 2026-05-28 |
| 3 | [Athenz concrete `JwtSigner`](#3-athenz-concrete-jwtsigner) | ✅ landed 2026-05-28 |
| 4 | [Athenz ZTS e2e fixture](#4-athenz-zts-e2e-fixture) | ✅ landed 2026-05-28 |
| 5 | [PIP-180 replicator-side e2e](#5-pip-180-replicator-side-e2e) | ⚡ (self-hosting fixture) |
| 6 | [PIP-460 scalable topics scaffold](#6-pip-460-scalable-topics-scaffold) | ⚡ (scaffold-now / e2e-later) |
| 7 | [Moonpool transport TLS + supervised-loop coverage](#7-moonpool-transport-tls--supervised-loop-coverage) | ✅ landed 2026-05-28 (TLS hunk); supervised-loop driver lines remain |
| 8 | [Golden trace catalog — transactional ack + cryptoFailureAction](#8-golden-trace-catalog-extension) | ⚡ (cryptoFailureAction remains; txn ack-within-txn landed 2026-05-28) |
| 9 | [Differential runner: plain `tokio::spawn` restructure](#9-differential-runner-plain-tokiospawn-restructure) | 🔗 (blocked on upstream moonpool TaskProvider — see in-section investigation note) |
| 10 | [`engine.rs` split](#10-enginers-split) | ⚡ |
| 11 | [`ProducerExt` trait inline — DECISION: accept as layering artefact](#11-producerext-trait-inline) | ✅ decided (doc-only) |

---

## 1. Moonpool vectored I/O

**Gap.** `magnetar-runtime-moonpool/src/driver.rs` coalesces
`TransmitOwned::Vectored` segment lists into a single `BytesMut`
before calling `transport.write_all`, because
`moonpool_sim::network::sim::SimTcpStream` implements only
`poll_write` (no `poll_write_vectored`). The local coalesce
preserves byte correctness but the chaos pack only ever sees one
`poll_write` call — we lose per-`IoSlice` partial-write modelling,
fragmentation, reordering. The tokio engine already dispatches the
Vectored arm via real `writev(2)` through
`AsyncWriteExt::write_vectored`.

**Why it stays open.** Needs upstream change in
[`moonpool-sim`](https://github.com/PierreZ/moonpool): override
`AsyncWrite::poll_write_vectored` + `is_write_vectored` on
`SimTcpStream`, with `writev(2)`-style short-accept semantics. Once
the trait surface lands, magnetar drops the local coalesce.

**Filed.** [PierreZ/moonpool#111](https://github.com/PierreZ/moonpool/issues/111)
(cc'd, awaiting PierreZ — direction to land it or to send a PR from
our side).

**`/goal` (post-upstream).**

```text
/goal flip magnetar-runtime-moonpool to true vectored dispatch once PierreZ/moonpool#111 lands. Replace the local coalesce in crates/magnetar-runtime-moonpool/src/driver.rs::driver_loop_inner's `Vectored` arm with a `write_all_vectored` helper mirroring crates/magnetar-runtime-tokio/src/driver.rs::write_all_vectored — loop `AsyncWriteExt::write_vectored` with per-IoSlice offset advancement, handle partial accepts, WriteZero on n==0 with non-empty slices. Test layers per ADR-0024: extend crates/magnetar-runtime-moonpool/tests/poll_transmit_vectored_parity.rs::poll_transmit_vectored_emits_vectored_for_queued_producer_send to assert the underlying transport observed N separate segment events (not one coalesced write). Validation chain per CLAUDE.md. ADR-0039 wave 2 chaos-fidelity gap closes when this lands.
```

---

## 2. Engine-generic builder & V5 unified lift

**Status update (2026-05-28).** ✅ Landed. The unified refactor shipped
in `feat/engine-generic-unified` (commits squashed for the merge).
Summary of what landed:

- **WAVE 1** — `MessageEncryptorApi` / `MessageDecryptorApi` /
  `NoEncryption` extension traits added as supertraits of `Engine` in
  `crates/magnetar/src/engine/mod.rs`. Tokio plugs in
  `Arc<dyn magnetar_runtime_tokio::MessageEncryptor>` /
  `Arc<dyn …::MessageDecryptor>`; moonpool plugs in `NoEncryption`
  (no-op stub). Broker-metadata lookups already had real moonpool
  impls via the existing `BrokerMetadataApi`; reused without change.
- **WAVE 2** — `ProducerBuilder<'_, E>` / `ConsumerBuilder<'_, E>` /
  `ReaderBuilder<'_, E>` lifted: encryptor / decryptor storage is now
  engine-typed (`Option<<E as MessageEncryptorApi>::Encryptor>`);
  tokio-only `encryption()` / `create_with_encryption()` /
  `subscribe_with_decryption()` setters live in
  `impl …Builder<'_, TokioEngine>` blocks. `PartitionedProducerBuilder<'a, E>` /
  `TableViewBuilder<'a, E>` / `TypedTableViewBuilder<'a, S, E>` gained the
  `E` parameter; their `.create()` paths dispatch through
  `CreateProducerApi` / `SubscribeApi` / `BrokerMetadataApi`.
  `PulsarClient::partitioned_producer` / `.table_view` /
  `.typed_table_view` lifted from `impl PulsarClient<TokioEngine>` to
  `impl<E: Engine> PulsarClient<E>`.
- **WAVE 3** — `PulsarClientV5<E: Engine = TokioEngine>` parametric,
  same for `v5::Producer<E>`, `v5::StreamConsumer<E>`,
  `v5::QueueConsumer<E>` and their builders. Five moonpool 1:1
  mirrors landed at `crates/magnetar/tests/v5_*_moonpool.rs`. ADR-0032
  flipped Proposed → Accepted; README parity matrix row flipped 🟡 →
  ✅; `docs/v5-client.md` Roadmap updated.

**BREAKING CHANGES** (crate unpublished, accepted per the API stability
stance at the top of this file):

- `PartitionedProducerBuilder` / `TableViewBuilder` /
  `TypedTableViewBuilder` gain an `E: Engine` parameter (defaults
  preserve most call sites).
- `PulsarClientV5` and the V5 `Producer` / `StreamConsumer` /
  `QueueConsumer` types gain an `E: Engine` parameter.
- The encryptor / decryptor types move behind per-engine extension
  traits. Tokio call sites that imported
  `magnetar_runtime_tokio::MessageEncryptor` /
  `MessageDecryptor` continue to compile (the trait objects are still
  the tokio-engine's associated types via `MessageEncryptorApi`).
  Moonpool callers that want a no-op stub get `magnetar::NoEncryption`.
- `TableViewBuilder` / `TypedTableViewBuilder`: the generic `.create()`
  now ignores the encryption field. Tokio callers that need PIP-4
  decryption call `.create_with_decryption()` (new tokio-specialised
  method, parity with `ConsumerBuilder::subscribe_with_decryption`).
- `PartitionedProducerBuilder`: same split — generic `.create()` is
  encryption-free; tokio callers that need PIP-4 encryption call
  `.create_with_encryption()`.

---

### Original entry (kept for historical reference)

**Gap (combined — replaces former §2 + §3 + §4).** The v4 builders
(`ProducerBuilder`, `ConsumerBuilder`, `ReaderBuilder` in
`crates/magnetar/src/builders.rs`) carry a phantom `E: Engine` on
every chainable method even though the impl bodies are 95% tokio-
bound. The per-surface builders
(`PartitionedProducerBuilder`, `TableViewBuilder`,
`TypedTableViewBuilder` in `partitioned_producer.rs`,
`table_view.rs`) reference tokio-only types directly
(`Arc<dyn magnetar_runtime_tokio::MessageEncryptor>`,
`magnetar_runtime_tokio::MessageDecryptor`). The V5 wrapper
(`PulsarClientV5`) hard-wires `PulsarClient<TokioEngine>` because
of those leaks, which blocks the moonpool 1:1 mirror of the 5 V5
mapping tests and ADR-0032's promotion to Accepted.

All three problems share one root: the `MessageEncryptor` /
`MessageDecryptor` / `MessageRouter` types are tokio-defined. Lift
them to per-engine extension traits and every dependent surface
becomes engine-generic for free.

**Decision (Florentin, this session).** Land all three as ONE PR.
Breaking API change accepted (crate unpublished).

**`/goal`.**

```text
/goal land the unified engine-genericity refactor per docs/follow-ups.md §2 — combines what used to be §2 (builder phantom-E cleanup), §3 (per-surface builder lifts: partitioned_producer / table_view / typed_table_view), and §4 (V5 engine-genericity for PIP-466 promotion) into one PR. The shared scaffolding is per-engine extension traits for `MessageEncryptor` / `MessageDecryptor` / `MessageRouter` / partitioned-topic-metadata lookup.

WAVE 1 — extension trait scaffolding
1. Add `MessageEncryptorApi`, `MessageDecryptorApi`, `MessageRouterApi`, `PartitionedTopicMetadataApi` to crates/magnetar/src/engine.rs (or a new `crates/magnetar/src/engine/api.rs` if the file is already wide). Each carries the associated types the v4 builders need (Encryptor, Decryptor, Router, metadata fetcher).
2. Impl the traits on `TokioEngine` in crates/magnetar/src/engine.rs (or sibling) so the existing tokio surface compiles against the trait shapes.
3. Add a no-op / unimplemented impl on `MoonpoolEngine<P>` for the encryption types if real moonpool encryption is out of scope — gate the unimplemented paths behind `#[cfg(feature = "encryption")]` so feature-off builds compile cleanly. broker-metadata lookup MUST have a real moonpool impl because partitioned producer needs it.

WAVE 2 — v4 builder lift (former §2 + §3)
4. crates/magnetar/src/builders.rs: drop the `<E: Engine>` parameter from `ProducerBuilder<'a>` / `ConsumerBuilder<'a>` / `ReaderBuilder<'a>` chainable surface. Move the `E` only to the final `.create()` / `.subscribe()` dispatch via the existing `CreateProducerApi` / `SubscribeApi` / `ReaderApi` extension traits. Store the per-engine `Encryptor` / `Decryptor` / `Router` behind the new per-engine API traits, not the tokio-concrete types.
5. crates/magnetar/src/partitioned_producer.rs + crates/magnetar/src/table_view.rs: same lift for `PartitionedProducerBuilder<'a, E: Engine>` / `TableViewBuilder<'a, E: Engine>` / `TypedTableViewBuilder<'a, E: Engine, S>`. They DO carry the engine parameter (they need the per-engine API traits for the inner builds).
6. Lift `PulsarClient::partitioned_producer/.table_view/.typed_table_view` from `impl PulsarClient<TokioEngine>` to `impl<E: Engine> PulsarClient<E>`.
7. crates/magnetar/src/client.rs: update `producer/consumer/reader` entry points to return the new non-generic chainable builders.

WAVE 3 — V5 lift (former §4)
8. crates/magnetar/src/v5/client.rs: `PulsarClientV5<E: Engine>` (parametric). Replace `pub struct PulsarClientV5 { inner: PulsarClient }` with `pub struct PulsarClientV5<E: Engine = TokioEngine> { inner: PulsarClient<E> }`. Same for v5/producer.rs / v5/stream_consumer.rs / v5/queue_consumer.rs and their builder types — parametrise by `<E>`.
9. Keep the `.v4()` / `.into_v4()` / `.from_v4(...)` escape hatch contract zero-overhead.
10. Add moonpool 1:1 mirrors of the existing 5 V5 mapping/wire tests under crates/magnetar-runtime-moonpool/tests/v5_*_moonpool.rs. Each exercises the V5 surface against `MoonpoolEngine<TokioProviders>` + `SimulationBuilder` (or against a sans-io `Connection` via `magnetar_fakes::FrameRecorder` for parity with the magnetar-tier tests).
11. Flip specs/adr/0032-pip-466-v5-client-surface-scope.md Status from Proposed → Accepted; update specs/README.md ADR index.
12. Update README.md PIP-466 parity matrix row from 🟡 experimental → ✅ (the `experimental-v5-client` feature can stay default-off; the ADR-0032 acceptance is what flips the matrix).
13. Update docs/v5-client.md "Roadmap" section: mark items #1, #4, #5 as landed.

TEST LAYERS per ADR-0024 — all binding:
- (a) `magnetar-proto` unchanged (no proto-layer change; sans-io stays sans-io).
- (b) crates/magnetar-runtime-tokio/tests/ — verify the existing tokio integration tests still pass; the lift is a pure delegate.
- (c) crates/magnetar-runtime-moonpool/tests/ — runtime parity test count stays at tokio=moonpool. Add per-builder shape tests if the new generic surface needs explicit type-shape pinning.
- (d) crates/magnetar-differential/ — no new traces needed (no wire-format change); existing equivalence tests stay green.

VALIDATION CHAIN per CLAUDE.md (full chain — cargo +nightly fmt, build, clippy -D warnings, test, xtask check-no-channels / check-no-io-deps / check-no-internal-clock / check-runtime-test-parity / check-sim-coverage / check-crypto-matrix). Run docs build with RUSTDOCFLAGS="-D warnings --cfg tokio_unstable".

BREAKING CHANGE in the commit body: ProducerBuilder/ConsumerBuilder/ReaderBuilder drop the Engine type parameter from chainable methods; PartitionedProducerBuilder/TableViewBuilder/TypedTableViewBuilder gain an `E: Engine` parameter; PulsarClientV5 gains an `E: Engine` parameter (defaulting to TokioEngine to preserve most call sites); MessageEncryptor/MessageDecryptor/MessageRouter types move behind per-engine extension traits (callers that imported them from magnetar_runtime_tokio must switch to the trait-based API).

Land in a single PR — partial landings would leave the API in an inconsistent state across surfaces.
```

---

## 3. Athenz concrete `JwtSigner`

**Status update (2026-05-28).** ✅ Landed in `feat/athenz-jwt-signer`.

Two concrete backends ship in
`crates/magnetar-auth-athenz/src/jwt_signer/{aws_lc_rs,ring}.rs`,
gated on the workspace crypto-provider feature matrix per
[ADR-0035](../specs/adr/0035-pluggable-crypto-provider.md). New
`AthenzProvider::with_default_signer(config)` constructor selects
the cfg-active backend (aws-lc-rs first when both are enabled).
Parsed PKCS#8 DER bytes wrapped in `zeroize::Zeroizing<Vec<u8>>`
(closes the ADR-0030 deferral note); the keypair is rebuilt on each
sign because aws-lc-rs / ring `RsaKeyPair` types are opaque
C-allocated `EVP_PKEY`/`BIGNUM` wrappers that cannot be made
`Zeroize`-friendly from Rust.

Tests: 11 per-backend (parse / round-trip-verify / payload-claims /
deterministic-signature / debug-redaction / zeroize-type-pin) plus 1
cross-backend byte-identity test gated on both features (RSASSA-
PKCS1-v1_5 SHA-256 is deterministic per RFC 8017 §8.2, so aws-lc-rs
and ring must produce identical signature bytes). `xtask
check-crypto-matrix` extended from 8 to 16 cells (façade ×
`{none, aws-lc-rs, ring, both}` × `{zts off, zts on}` for the
athenz crate). Docs: [`docs/athenz.md`](athenz.md), parity-status
row + README parity row flipped to ✅.

§4 is unblocked.

### Original entry (kept for historical reference)

**Gap.** `crates/magnetar-auth-athenz/src/zts.rs` ships the
`ZtsClient` + `JwtSigner` trait, but no concrete signer
implementation. Without one, the parity matrix row for Athenz stays
at 🟡 — callers either supply their own signer (documented external
pattern using `with_role_token` + sidecar mint) or the feature is
unusable end-to-end.

ADR-0030 also defers the parsed-key `zeroize::Zeroizing<…>` wrap
here because the parsed RSA key only materialises once a concrete
signer exists.

**Decision (Florentin, this session).** Implement BOTH `aws-lc-rs`
and `ring` backends, gated on the workspace crypto-provider feature
matrix per [ADR-0035](../specs/adr/0035-pluggable-crypto-provider.md):
`crypto-aws-lc-rs` selects the aws-lc-rs signer (FIPS-capable path),
`crypto-ring` selects the ring signer. Mirrors the rustls
crypto-provider selection so the workspace stays consistent.

**`/goal`.**

```text
/goal land the concrete Athenz JwtSigner per docs/follow-ups.md §3 — ship BOTH `aws-lc-rs` and `ring` backends, gated on the workspace crypto-provider feature matrix per ADR-0035 (mirrors the rustls provider selection so the whole workspace stays consistent).

Module layout:
- crates/magnetar-auth-athenz/src/jwt_signer/mod.rs (NEW) — re-export the active backend behind cfg gates.
- crates/magnetar-auth-athenz/src/jwt_signer/aws_lc_rs.rs (NEW) — `pub struct AwsLcRsSigner`, gated `#[cfg(feature = "crypto-aws-lc-rs")]`.
- crates/magnetar-auth-athenz/src/jwt_signer/ring.rs (NEW) — `pub struct RingSigner`, gated `#[cfg(feature = "crypto-ring")]`.

Implementation per backend:
1. Constructor parses the PEM RSA key once; wraps the parsed key in `zeroize::Zeroizing<…>` (closes ADR-0030 deferral).
2. `impl JwtSigner for <Backend>Signer` — sign the JWS header + payload per Athenz N-token spec (RFC 7519 base64url segments; RS256 default, ES256 if the key is EC). The Athenz ZTS spec is at https://github.com/AthenZ/athenz/blob/master/docs/zts_api.md — match the existing `zts::ZtsClient` token-exchange flow.
3. The two backends produce byte-identical signature bytes for the same key + payload + timestamp (deterministic when wall_clock is frozen).

Features on `crates/magnetar-auth-athenz/Cargo.toml`:
- `crypto-aws-lc-rs = ["dep:aws-lc-rs"]`
- `crypto-ring = ["dep:ring"]`
- Default: neither (preserves today's "ship the trait, downstream picks the signer" stance).
- Mutually-exclusive runtime check (if both enabled, prefer aws-lc-rs and `#[deprecated]` note on the ring path) OR compile_error! via const assertion — pick the one matching ADR-0035's pattern in magnetar-proto's crypto feature handling.

Wire the backend into AthenzProvider via a new constructor `AthenzProvider::with_default_signer(config) -> Self` that selects the cfg-active backend; falls back to `with_role_token` documentation if neither feature is on.

Test layers per ADR-0024:
- (a) crates/magnetar-auth-athenz/src/jwt_signer/aws_lc_rs.rs `#[cfg(test)] mod tests` — round-trip the signed JWT through the same crate's verify path; assert iss/sub/aud/exp; assert deterministic signature with frozen wall_clock; assert the Zeroizing wrap is correctly applied (Drop check via a stand-in type or a comment-pin to a `#[deny(unused_must_use)]` proxy).
- Same shape for crates/magnetar-auth-athenz/src/jwt_signer/ring.rs.
- (b)/(c) Existing static-signer integration tests stay; no runtime-layer change.
- (d) Differential — the JWT bytes are deterministic given a fixed key + fixed timestamp; a frozen wall_clock makes the assertion stable across engines. Optional new differential test if there's appetite; not load-bearing because Athenz lives above the proto layer.

Build matrix:
- Update xtask `check-crypto-matrix` to include the Athenz crate's two crypto features in the cartesian product (verifies neither / aws-lc-rs / ring / both builds cleanly).

Docs:
- Update docs/parity-status.md Athenz row from 🟡 to ✅.
- Update README parity matrix row for Athenz.
- Update specs/adr/0030-athenz-private-key-zeroize-deferral.md — flip the deferral note to "closed by the concrete signer landing".
- New section in docs/auth.md (or NEW docs/athenz.md if no auth doc exists yet) explaining the crypto-provider matching choice.

Validation chain per CLAUDE.md.

BREAKING CHANGE: `magnetar-auth-athenz` gains `crypto-aws-lc-rs` / `crypto-ring` mutually-exclusive features; callers wanting the new built-in signer must enable one of them. Existing `with_role_token` / `AthenzProvider::new` paths unchanged.
```

---

## 4. Athenz ZTS e2e fixture

**Status update (2026-05-28).** ✅ Landed in `test/athenz-zts-e2e`.

The e2e suite lives at
[`crates/magnetar/tests/e2e_athenz_zts.rs`](../crates/magnetar/tests/e2e_athenz_zts.rs)
behind `feature = "e2e,auth-athenz-zts"`. Four tests, all
`#[ignore = "e2e: requires Docker"]` for parity with the sibling e2e
files:

1. `e2e_athenz_zts_refresh_then_cached_initial` — wiremock-stub.
   `ZtsClient::refresh_via_zts` → cached role token returned by
   `AuthProvider::initial()`. Asserts the bearer JWT is a compact-JWS
   three-segment payload from the §3 signer.
2. `e2e_athenz_zts_expiry_aware_refresh_fires_on_near_expiry` —
   wiremock-stub. TTL-0 first response forces the cache to treat the
   token as past-deadline; the next `fetch_role_token` round-trips
   again and rotates the cached bytes.
3. `e2e_athenz_zts_cached_token_used_on_auth_challenge` — wiremock-stub.
   `AuthChallengeState::handle_challenge` routes through
   `respond_to_challenge` and echoes the cached role-token bytes; no
   extra ZTS round-trip.
4. `e2e_athenz_zts_image_pulls_and_serves_status` — real Docker probe
   against `athenz/athenz-zts-server:1.12.41`. Proves the upstream
   image is pullable and `testcontainers-rs` wiring works. The probe
   gracefully surfaces the "no ZMS co-deployed → readiness probe times
   out" branch as an expected warning, mirroring the SASL Kerberos
   pattern for unprovisioned hosts.

**Hybrid rationale.** The upstream
[`athenz/athenz-zts-server`](https://hub.docker.com/r/athenz/athenz-zts-server)
image's full production bring-up needs a co-deployed ZMS, per-tenant
public-key seeding via the ZMS admin REST API, and a chained TLS
server certificate — Athenz's
[`make deploy-dev`](https://github.com/AthenZ/athenz/blob/master/docker/README.md)
orchestrates four containers (ZMS DB, ZMS, ZTS DB, ZTS) plus a
cert-bootstrap pre-flight, taking ~15 minutes to build. That topology
does not fit cleanly behind a single `testcontainers-rs` spawn, so per
the goal's "honest scope check" we split the suite: wiremock-stub
tests against a real `reqwest` client cover every behavioural
assertion the `/goal` enumerates, and the real-image probe wires the
upstream image into the e2e surface without requiring a pre-seeded ZMS
on the build host. Closing the deferred "full production-style
ZMS+ZTS+cert-bootstrap" slice would require shipping the Athenz
`make deploy-dev` topology as a shared CI fixture (similar to
`docker-compose.replicated-subs.yml`); tracked here if it ever becomes
load-bearing.

Docs: [`docs/athenz.md` §End-to-end testing](athenz.md#end-to-end-testing-against-a-real-zts);
parity-status row updated.

### Original entry (kept for historical reference)

**Gap.** No end-to-end test exercises the Athenz ZTS round-trip
against a real ZTS server. Tests today are unit-level against the
`zts::ZtsClient` + a static `JwtSigner` mock.

**Why it stays open.** Blocked on §3 (a real signer) and on the
Dockerised ZTS fixture image (`athenz/athenz-zts-server`).

**`/goal` (post-§3).**

```text
/goal stand up the Athenz ZTS e2e fixture per docs/follow-ups.md §4. Add the `athenz/athenz-zts-server` Docker image as a testcontainers-rs spawn under crates/magnetar/tests/e2e_athenz_zts.rs (NEW), gated `feature = "e2e,auth-athenz-zts"` and `#[ignore = "e2e: requires Docker"]`. Tests: (1) ZtsClient::refresh_via_zts → cached role token returned by initial(); (2) cached token's expiry-aware refresh fires when expiry approaches; (3) the cached token is used in a subsequent AuthProvider::respond_to_challenge round-trip (mock challenge). Pre-seed the fixture with a tenant principal + role binding via the ZTS admin API on container startup. Use the §3-landed concrete JwtSigner (either backend — aws-lc-rs or ring — gated behind whichever crypto-provider feature the test runs with). Validation chain per CLAUDE.md. Update docs/parity-status.md.
```

---

## 5. PIP-180 replicator-side e2e

**Gap.** `crates/magnetar/tests/e2e_shadow_topic.rs` exercises the
admin REST cycle + a regular produce-on-source / consume-on-shadow
round-trip. The replicator-style `send_with_source_message_id`
path is covered by the differential equivalence test against the
scripted broker that echoes the source id back; against real
Pulsar 4.x, the broker's authorisation flow may reject a
client-asserted source id that doesn't match a registered
replicator producer.

**Decision (Florentin, this session).** Build the self-hosting
fixture as part of the test: a 2-cluster Pulsar standalone in
testcontainers-rs with a custom auth config registering a
`replicator` role on the source namespace. No external dependency.

**`/goal`.**

```text
/goal add the PIP-180 replicator-side e2e assertion per docs/follow-ups.md §5 with a self-hosting 2-cluster fixture (no external broker dependency).

Test infrastructure (NEW under crates/magnetar/tests/):
- e2e_shadow_topic_replicator.rs (NEW), gated `feature = "e2e"`, `#[ignore = "e2e: requires Docker"]`.
- Helper: `start_pulsar_two_cluster_with_replicator_role()` in a shared test-helper module under crates/magnetar/tests/common/ (NEW if no such directory exists in magnetar/tests/) that:
  1. Spins up TWO `apachepulsar/pulsar:4.0.4` standalone containers via testcontainers-rs, on separate networks.
  2. Configures the source cluster's broker.conf with `authenticationEnabled=true`, `authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken`, and a token-secret-key seeded with a deterministic test secret.
  3. Configures the source cluster's namespace-policy to register `replicator` as a recognised role on `public/default` (via `pulsar-admin namespaces grant-permission public/default --role replicator --actions produce`).
  4. Returns `(source_service_url, source_admin_url, dest_service_url, dest_admin_url, source_container, dest_container)`.

Test cases:
1. `e2e_v4_replicator_role_can_assert_source_message_id` — open a producer authenticated as `replicator` against the source cluster; call `producer.send_with_source_message_id(payload, synthetic_source_id)` (the existing PIP-180 entry); subscribe via the consumer on a shadow topic of the SAME cluster (PIP-180's `MessageReceivedFromShadow` semantics on the shadow topic, not the source); assert the received message's `replicated_from` field carries the synthetic source id verbatim.
2. `e2e_v4_non_replicator_role_send_with_source_id_is_rejected` — repeat with a non-replicator-role token; assert the broker rejects with the expected `AuthorizationException` (negative test pins the broker contract).

Documentation:
- docs/shadow-topic.md: NEW section "Replicator-role e2e setup" describing the 2-cluster + role-grant fixture and pointing at the test file as the executable reference.
- docs/parity-status.md: PIP-180 row gets a footnote that the replicator-side e2e is exercised by `e2e_shadow_topic_replicator.rs` (self-hosting fixture).

Validation chain per CLAUDE.md (the `#[ignore]` keeps it out of the default test run; `cargo test --features e2e -- --include-ignored` exercises it).
```

---

## 6. PIP-460 scalable topics scaffold

**Gap.** PIP-460 surface entirely. Wire-protocol delta is
significant (3 new commands + optional `MessageId.segment_id`); the
proto bump is gated on upstream cutting an RC.

**Why it stays open.** Upstream PIP is **`Draft`**, targets Pulsar
5.0 LTS (Oct 2026) with phased rollout via 4.3.0 / 4.4.0. No broker
ships PIP-460 today, so this is scaffold-now / e2e-later. Estimate
~2080 LOC.

**Scope locked.** [ADR-0031](../specs/adr/0031-pip-460-scalable-subscription-scope.md)
+ [`specs/proposals/pip-460-scalable-topics.md`](../specs/proposals/pip-460-scalable-topics.md):
StreamConsumer-only, drops-on-DAG-change (no transparent failover),
behind `feature = "scalable-topics"` (default off). QueueConsumer,
CheckpointConsumer, controller-election, in-place repartition are
explicit v0.3.0+. **E2E is best-effort and does not block release**;
the 4-layer in-process tests are the binding acceptance gate.

**`/goal` (scaffold-now path; e2e once a 5.0 RC ships).**

```text
/goal implement PIP-460 scalable-topics surface per specs/proposals/pip-460-scalable-topics.md and ADR-0031. Upstream is `Draft` and no broker ships PIP-460 today, so this is scaffold-now / e2e-later. Waves: (0) PREREQ — separate commit per ADR-0026 §D4: `cargo run -p xtask -- vendor-proto --rev <pulsar-5.0-rc-sha>` ONCE upstream cuts a 5.0 RC; until that lands, hand-encode the new commands behind a `cfg(feature = "scalable-topics")` gate in magnetar-proto/src/pb/scalable_topics.rs (NEW) using prost-build manual definitions; (1) magnetar-proto/src/types.rs extend `MessageId { segment_id: Option<SegmentId> }`, new types `SegmentId(u64)`, `KeyRange { start: u32, end: u32 }`, `SegmentState { Active, Splitting, Merging, Sealed }` (`#[non_exhaustive]`), `SegmentDescriptor`; equality rules: `None`-segment ignored for v4 invariant, `Some(_)` vs `None` returns false (cross-mode); (2) magnetar-proto/src/dag_watch.rs (NEW) — `DagWatchSession` with monotonic update_seq tracking, `handle_update(SegmentDagUpdate) -> Result<DagDelta, DagError>`, `DagError::{NonMonotonic, UnknownSegment, ...}`; (3) magnetar-proto/src/conn.rs — new entries `send_scalable_topic_lookup`, `open_dag_watch`, `close_dag_watch`; magnetar-proto/src/event.rs — new variants `ScalableTopicLookupResolved`, `SegmentDagUpdated`, `DagChangedDuringConsume { reason: DagChangeReason }`; magnetar-proto/src/lib.rs — new `SUPPORTED_PROTOCOL_VERSION_SCALABLE_TOPICS` constant; (4) magnetar::scalable module (NEW) behind `feature = "scalable-topics"` (default off) exposing `ScalableTopicsApi` extension trait + `StreamConsumer<T, E> where E::ClientState: ScalableTopicsApi`; on `DagChangedDuringConsume` close all per-segment v4 consumers and surface `ConsumerEvent::DagChanged`; (5) magnetar-runtime-tokio — `topic://` URL parser branch; impl `ScalableTopicsApi for TokioRuntimeState`; driver translates DagWatch events into consumer wake-ups; (6) magnetar-runtime-moonpool — impl `ScalableTopicsApi for Client<P>`; crates/magnetar-runtime-moonpool/tests/scalable_topic_broker.rs (NEW) — scripted controller-broker (replies to lookup, opens DagWatch, pushes 2 updates: 1 split + 1 merge, then closes); `BrokerWorkload::ScalableTopic` variant in sim_chaos.rs; (7) magnetar-cli `topic-info <topic://...>` subcommand (~80 LOC, prints segment DAG). Test layers per ADR-0024 — all binding: (a) proto unit (9 tests incl. encoder roundtrip + v4-shape byte-identical guard + monotonic update_seq + split/merge), (b) tokio integration in crates/magnetar-runtime-tokio/tests/scalable_topic.rs (4 tests incl. `scalable_topics_feature_off_does_not_export` compile_error proof), (c) moonpool 1:1 mirror with 100% diff coverage via `check-sim-coverage`, (d) differential equivalence + golden trace crates/magnetar-differential/tests/golden/scalable_topic_drop_on_split.json. E2E gated behind `#[ignore = "e2e: requires Pulsar 5.0 with PIP-460"]` + `feature = "e2e,scalable-topics"` — crates/magnetar/tests/e2e_scalable_topic.rs (NEW) does NOT block v0.2.0 release-cut. Docs: docs/scalable-topics.md (NEW with experimental banner + drop-on-change semantics), parity-status.md row → 🟡 experimental, README parity matrix row, flip ADR-0031 to Accepted. Land in this exact order to keep `check-runtime-test-parity` green: (a) before (b); moonpool ScalableTopicBroker fake before any tokio test; differential after both engines have green tests. Out of scope (v0.3.0+ markers): QueueConsumer, CheckpointConsumer, controller-election awareness, transparent segment failover, in-place repartition, segment-aware sticky-key dispatch.
```

---

## 7. Moonpool transport TLS + supervised-loop coverage

**Status update (2026-05-28).** ✅ TLS hunk landed in
`test/moonpool-tls-coverage`. The in-process rustls broker fixture
(self-signed cert via `rcgen` + `tokio_rustls::TlsAcceptor`) lives in
`crates/magnetar-runtime-moonpool/tests/tls_transport_coverage.rs`.
Four end-to-end tests drive the moonpool engine's `connect_tls`
against the fixture:

- `connect_tls_completes_handshake_then_drives_pulsar_connected` —
  full TLS handshake + Pulsar `CommandConnect` / `CommandConnected`
  round-trip over the encrypted channel.
- `connect_tls_rejects_invalid_server_name` — invalid SNI surfaces as
  `EngineError::Config` before any wire traffic.
- `connect_tls_propagates_peer_drop_after_tls_handshake` — the broker
  drops mid-Pulsar-handshake; the engine surfaces `PeerClosed` (or
  `Io` if a TCP RST beats the TLS `close_notify`).
- `connect_tls_clean_shutdown_releases_resources` — user-initiated
  close exercises the TLS `Transport::shutdown` arm without hanging.

Paired tokio mirrors with the same names live in
`crates/magnetar-runtime-tokio/tests/tls_transport_coverage.rs` (Debug
/ fmt / crypto-provider smoke against `rustls::ClientConnection`),
keeping `xtask check-runtime-test-parity` balanced at 167/167.

**Engine bug fixed alongside.** `Transport::read_buf`'s TLS arm
previously returned `Ok(0)` when a TLS record decrypted to no
application bytes (e.g. TLS 1.3 `NewSessionTicket`). The driver loop
treats `0` as `PeerClosed`, so every TLS connection broke on the
first post-handshake server message. The arm now loops until
plaintext is available or the wire actually EOFs — matching
`tokio_rustls::TlsStream` semantics.

**Remaining gap.** The supervised reconnect loop in
`src/driver.rs::supervised_driver_loop` (anti-thrash cooldown,
multi-attempt redial) still carries uncovered lines. Closing them
needs a multi-cycle peer-drop fixture (drop, accept, drop, …) which
is mechanically straightforward but was descoped from this PR. Open
follow-up if the diff coverage gate flags it.

---

## 8. Golden trace catalog extension

**Gap.** The differential harness ships **eleven** golden traces
(round-trip, batch, nack-redelivery, seek-to-start, many-publishes,
lookup-before-open, seek-per-partition, **txn-new-then-commit**,
**txn-new-then-abort**, **txn-send-ack-then-commit**,
**txn-send-ack-then-abort**). Missing:

- **Transactional ack-within-txn paths** — **landed** in full. The
  txn-lifecycle round-trip (NewTxn → EndTxn(commit/abort)) lives in
  `txn_new_then_commit_round_trip` + `txn_new_then_abort_round_trip`;
  the produce/ack drain semantics live in `txn_send_ack_then_commit`
  + `txn_send_ack_then_abort`. The scripted broker handles
  `CommandTcClientConnectRequest`, `CommandNewTxn`,
  `CommandAddPartitionToTxn`, `CommandAddSubscriptionToTxn`,
  `CommandEndTxn`, observes `CommandAck` carrying a `txn_id` into a
  per-txn ack ledger (drained on commit, dropped on abort), and
  surfaces the drain/drop count via
  `ScriptedBroker::txn_drain_log_snapshot` (mirrors the existing
  `seeked_partitions` log). The runner-side `Op::SendInTxn` /
  `Op::AckInTxn` variants route the publish / ack through the
  `OutgoingMessage::txn_id` and `Consumer::ack_with_txn` entries
  respectively. The differential equivalence claim is now: both
  engines emit identical event streams AND the scripted broker
  observes identical (drained, ack_count) tuples on commit/abort.
- **`cryptoFailureAction` matrix** — ~240 LOC; **blocked** on
  porting the PIP-4 crypto bridge to moonpool.

**`/goal` (cryptoFailureAction — blocked on crypto bridge port).**

```text
/goal add the cryptoFailureAction matrix golden trace per docs/follow-ups.md §8 — DEPENDS on porting the PIP-4 message crypto bridge (currently in magnetar-messagecrypto + magnetar-runtime-tokio) to the moonpool runtime first. Once the moonpool MessageEncryptor/Decryptor are in place, extend the scripted broker to deliver a payload with intentionally-corrupt ciphertext and assert each `CryptoFailureAction` arm (Fail / Discard / Consume) at the consumer surface. Golden trace at crates/magnetar-differential/tests/golden/crypto_failure_action.json. Validation chain per CLAUDE.md.
```

---

## 9. Differential runner: plain `tokio::spawn` restructure

**Gap.** The differential moonpool runner's driver task is
`spawn_local`'d into a [`tokio::task::LocalSet`](https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html)
because [`moonpool_core::TokioProviders`]'s `TaskProvider` uses
`tokio::task::Builder::new().spawn_local(...)`. While the test outer
task is parked on `consumer.receive()`, the spawn_local'd driver
only runs when the LocalSet's `run_until` is polled — and the proto
slab waker we now fire on delivery is dispatched from the driver
task, which itself isn't being polled. Result: ~30 s stall per
`Recv` until the proto keepalive deadline elapses and pumps the
chain.
[`crates/magnetar-differential/src/runner_moonpool.rs`](../crates/magnetar-differential/src/runner_moonpool.rs)
keeps a 25 ms `Kicker` pulsing `driver_waker.notify_one()` to bridge
the LocalSet pump gap.

**Decision (Florentin, this session).** Restructure the
differential runner to spawn the driver via plain `tokio::spawn`.
**Investigation result (this session, before dispatch).** The
restructure as originally framed is structurally blocked: the
driver task is not spawned BY `runner_moonpool.rs` — it is spawned
INSIDE `magnetar_runtime_moonpool::Client::connect_plain` via the
engine's `TaskProvider`, which is the moonpool-core
`TokioTaskProvider` and hardcodes `tokio::task::Builder::new().spawn_local(...)`.
The `TaskProvider` trait itself is `#[async_trait(?Send)]` with
`spawn_task<F>(...) -> JoinHandle<()> where F: Future<Output = ()> + 'static`
— no `Send` bound. `tokio::spawn` requires `Send`, so a drop-in
`tokio::spawn` provider is not possible at the trait level without
upstream changes.

Two real paths forward, both substantial:

1. **Upstream moonpool change.** Extend `TaskProvider` (or add a
   sibling `SendTaskProvider`) so the trait accepts `Send + 'static`
   futures and a tokio-side impl can use `tokio::spawn`. Files in
   the magnetar workspace stay sim-compatible via the original
   provider; the differential runner picks the Send-bound provider.
   Coordinate with [PierreZ/moonpool](https://github.com/PierreZ/moonpool/) —
   could ride on the same window as
   [#111](https://github.com/PierreZ/moonpool/issues/111).
2. **Bypass `Client::connect_plain` in the differential runner.**
   Rebuild the driver-spawn path manually in
   `runner_moonpool.rs` — call `Transport::connect_plain` directly,
   construct `ConnectionShared` ourselves, `tokio::spawn` the
   `driver_loop_inner` future. Substantial duplication of the
   engine's wiring; brittle against future engine changes.

Until one of those lands, keep the 25 ms `Kicker` workaround. It's
correct, just ugly. Updated `/goal` (post-upstream-or-bypass-
decision) below.

**`/goal` (post-upstream).**

```text
/goal restructure the differential moonpool runner per docs/follow-ups.md §9 ONCE the upstream moonpool TaskProvider gains a Send-bound spawn entry point (see the investigation note in §9 — magnetar cannot land this in-tree without either upstream change or duplicating the engine's driver-spawn wiring). When the upstream lands: (1) construct a custom Providers type in crates/magnetar-differential/src/runner_moonpool.rs that uses the Send-bound provider for Task and reuses TokioNetworkProvider / TokioTimeProvider / TokioRandomProvider / TokioStorageProvider for the rest; (2) drop the LocalSet wrapper in `pub async fn run(...)` — `local.run_until(run_inner(...))` becomes `run_inner(...).await`; (3) delete the Kicker struct + 25 ms pulse loop; (4) update the module doc comment to document the trade-off (differential harness uses Send-bound provider for liveness; production engine usage stays sim-compatible via TokioProviders); (5) run golden_traces, verify no regression. Validation chain per CLAUDE.md.
```

---

## 10. `engine.rs` split

**Gap.** `crates/magnetar/src/engine.rs` is 2148 lines. Pure
refactor candidate.

**Decision (Florentin, this session).** Dispatch the split now —
landing it before §2 (which extends `engine.rs` with the new
per-engine extension traits) keeps that PR's diff focused on the
genuine API change rather than mixing in a 2k-line move.

**`/goal`.**

```text
/goal split crates/magnetar/src/engine.rs (2148 lines) into a module per docs/follow-ups.md §10. Target layout: `crates/magnetar/src/engine/` directory with `mod.rs` (the `Engine` trait + the marker types), `tokio.rs` (the `TokioEngine` impl block + tokio-specific helpers), `moonpool.rs` (the `MoonpoolEngine<P>` impl block). Re-export every previously-public symbol from `crates/magnetar/src/engine/mod.rs` via `pub use` so the existing `magnetar::Engine` / `magnetar::TokioEngine` / `magnetar::MoonpoolEngine` paths and every internal `crate::engine::...` reference stays unchanged. Pure mechanical refactor — NO behaviour change, NO signature change. Validation chain per CLAUDE.md (the workspace should compile + test unchanged because every symbol stays at the same canonical path). ADR-0024 exemption justified: pure code-move refactor, no proto or runtime change, no test layer change. Land this BEFORE §2 (engine-generic builder lift) so the §2 PR can focus on the genuine API change rather than mixing in a 2k-line move.
```

---

## 11. `ProducerExt` trait inline

**Gap.** `crates/magnetar/src/client.rs::ProducerExt` is a single-
impl extension trait that exists only to satisfy Rust's orphan rule
for the façade-defined `MessageBuilder` against the runtime-defined
`Producer`.

**Decision (Florentin, this session).** Accept the trait as the
layering artefact. Zero-cost; the trait + single impl is the
canonical Rust workaround for the orphan rule. No code change
needed beyond documenting the rationale in-line.

**`/goal` (trivial doc).**

```text
/goal document the §11 `ProducerExt` decision per docs/follow-ups.md. Add a doc comment above `crates/magnetar/src/client.rs::ProducerExt` explaining: (1) why the trait exists (Rust orphan rule — `MessageBuilder` lives in the façade crate, `Producer` lives in `magnetar-runtime-tokio`, neither side can directly impl the conversion); (2) the two rejected alternatives (move `MessageBuilder` to a shared crate / move `MessageBuilder` down into `magnetar-runtime-tokio`); (3) the chosen path: accept the trait as a zero-cost layering artefact. Comment-only — no behaviour or signature change. Validation chain per CLAUDE.md (fmt + clippy only required). ADR-0024 exemption: comment-only.
```

---

## Notes on this file

Items move from this file to `git log` when their commit lands. The
expected churn:

1. New gap surfaces → entry added with **Gap** + **Why it stays
   open** + (where actionable) a `/goal …` block.
2. Agent team picks up the `/goal …` block in a fresh session.
3. PR merges → entry removed (the ADR / docs file carries the
   post-implementation reference).

All open items now carry either a `/goal …` block ready to dispatch
or an explicit blocker (external upstream, prior-PR dependency).
Decision pendings from prior cuts of this doc (Athenz crypto crate,
`ProducerExt` layering, builder-lift granularity, PIP-180 fixture,
PIP-460 scaffold scope, deferred items §9 / §10) have all been
resolved in the session that produced this consolidated doc.

The vectored I/O moonpool primitive ([§1](#1-moonpool-vectored-io))
remains the only fully-external blocker — tracked in
[PierreZ/moonpool#111](https://github.com/PierreZ/moonpool/issues/111).
