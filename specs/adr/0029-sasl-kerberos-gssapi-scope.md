# ADR-0029 — SASL Kerberos / GSSAPI binding

- **Status**: Accepted (landed 2026-05-26)
- **Date**: 2026-05-26
- **Decider**: Florentin Dubois
- **Tags**: auth, sasl, kerberos, gssapi, landed

## Context

[ADR-0010](0010-v0-1-full-java-parity.md) commits magnetar to full Apache Pulsar Java client parity against a Pulsar 4.0+ broker.
[ADR-0026 §D3](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) carved out one explicit exception: SASL `PLAIN` (RFC 4616) shipped first, with **SASL Kerberos / GSSAPI deferred** as follow-up work.
The rationale recorded there was that GSSAPI is a large, multi-stakeholder external dependency (`libgssapi`, MIT/Heimdal KRB5 runtime, JAAS-style JAAS section semantics) whose scope was not proportional to the near-term demand from Clever Cloud's use cases.
The parity matrix in `README.md` previously marked the mechanism `🟡 partial — PLAIN only, GSSAPI follow-up`.

Today the scaffolding for the deferred mechanism is in place: `magnetar-auth-sasl` ships `SaslKerberos` (see [`crates/magnetar-auth-sasl/src/kerberos.rs`](../../crates/magnetar-auth-sasl/src/kerberos.rs), `pub struct SaslKerberos` + `impl AuthProvider for SaslKerberos`) whose `AuthProvider::initial` returns `AuthError::Unsupported("Kerberos/GSSAPI requires the kerberos feature flag")` unconditionally — even with the `kerberos` feature enabled the message just changes to "feature-gated but the implementation is not yet wired up".
The protocol layer already understands the asynchronous auth challenge/response handshake: `CommandAuthChallenge` / `CommandAuthResponse` are present in the vendored proto ([`crates/magnetar-proto/proto/PulsarApi.proto:329-337,1300-1301`](../../crates/magnetar-proto/proto/PulsarApi.proto)), and protocol version V14 is already advertised on `CONNECT`.
The wire is ready; the mechanism implementation is not.

Apache Pulsar's reference is `org.apache.pulsar.client.impl.auth.AuthenticationSasl` (artifact `pulsar-client-auth-sasl`).
On the client side it consumes `saslJaasClientSectionName` + `serverType` configuration, performs a GSSAPI exchange via JDK's `Sasl.createSaslClient(["GSSAPI"], …)`, and drives a multi-step CONNECT → AuthChallenge → AuthResponse → CONNECTED handshake until the SASL state machine reports complete.

This ADR locks the scope of the deferred work so the follow-up plan has a concrete sizing and acceptance contract.

## Decision

- **Wire-protocol delta vs. current vendored PulsarApi.proto: none.** `CommandAuthChallenge` / `CommandAuthResponse` already exist on the vendored proto; magnetar's connection state machine has handled them since M2.
  No proto bump is required for SASL Kerberos.
  The handshake exchange (CONNECT[auth_data=initial token] → AUTH_CHALLENGE → AUTH_RESPONSE[continuation tokens] → … → CONNECTED) is reused verbatim from the SASL-PLAIN path; only the token-producing side changes.

- **`magnetar-proto` state-machine additions.** Introduce a `SaslMechanism` trait inside `magnetar-proto::auth` that accepts a challenge byte slice and returns the next response (or "complete").
  The `Conn` state machine grows a multi-step auth driver: on `Event::AuthChallenge { server_token }`, look up the active mechanism, call `mechanism.step(server_token, now)`, emit `Action::SendAuthResponse { client_token }`.
  Today the driver short-circuits on the first `AuthChallenge` because `AuthProvider` only exposes `initial()`.
  The `SaslMechanism` trait is sans-io; the GSSAPI runtime lives in the auth crate (engine side), not in `magnetar-proto`.

- **`magnetar-runtime-tokio` surface.** A new `kerberos` cargo feature on the `magnetar-auth-sasl` crate compiles in the GSSAPI binding.
  `SaslKerberos::with_config(SaslKerberosConfig { jaas_section, server_type, server_principal, keytab: Option<PathBuf>, ticket_cache: Option<PathBuf> })` becomes the constructor.
  The provider exposes `initial()` returning the GSSAPI initial token and `step(server_token)` for the continuation tokens.
  `ClientBuilder` gains no new method; users pass `SaslKerberos` through the existing `with_auth(Arc<dyn AuthProvider>)` slot.

- **`magnetar-runtime-moonpool` port.** The GSSAPI library calls touch the filesystem (krb5.conf, ticket cache) and the network (KDC exchange).
  Both are out of scope for moonpool's sans-io contract.
  The moonpool port ships a `SaslKerberosFake` that drives the multi-step challenge/response over a deterministic scripted token sequence, exercising the **state machine** without the GSSAPI runtime.
  Real-Kerberos coverage is tokio-only and e2e-only; parity holds at the wire driver level.

- **Dependency choice: `libgssapi`, not `libgssapi-sys`.** Reasons: (a) `libgssapi` provides a safe Rust wrapper over the FFI surface with `Drop`-correct lifetimes for `Credential` / `Name` / `Oid` / `SecurityContext`; (b) raw `libgssapi-sys` would force every caller to write the same `unsafe { gss_init_sec_context }` loop magnetar would otherwise write once; (c) `libgssapi`'s API models the iterative step-token-step exchange directly, which maps 1:1 onto our `SaslMechanism::step`.
  The runtime KRB5 dependency remains an OS package (MIT KRB5 or Heimdal), pinned via `system-deps` checks at crate build time.
  **No `openssl`** — `libgssapi` builds against the system KRB5 without pulling `openssl-sys`, preserving [ADR-0005](0005-rustls-only-tls.md).

- **JAAS subset.** Java's `saslJaasClientSectionName` resolves to a full JAAS section with `Krb5LoginModule` options. magnetar reads a **scoped subset** from `SaslKerberosConfig` rather than a JAAS file: `principal`, `keytab`, `ticket_cache`, `use_keytab`, `use_ticket_cache`.
  A JAAS-file parser is **out of scope** here; the equivalence table will land alongside any future parser work.

- **Service principal naming.** Pulsar's `serverType=broker` resolves to a service principal of the form `<serverType>/<broker-hostname>@<realm>`.
  The `Conn` state machine already exposes the broker's TLS-validated hostname; the auth crate consumes it via a new `target_service_name(hostname)` hook on `SaslMechanism` so the GSSAPI context is bound to the correct service.

## Consequences

- **Test layers per ADR-0024 (4-layer):** (a) `magnetar-proto` unit: feed scripted `AuthChallenge`/`AuthResponse` byte sequences through the `Conn` state machine; assert the emitted `Action::SendAuthResponse` tokens match the `SaslMechanism::step` output; assert terminal `CONNECTED` after the mechanism reports complete.
  (b) `magnetar-runtime-tokio`: integration test against `magnetar-fakes`' in-process broker stub configured for SASL challenge/response, using `SaslKerberosFake` so the test stays hermetic (no real KDC).
  (c) `magnetar-runtime-moonpool`: identical fake-mechanism test driven by `MoonpoolEngine<SimProviders>`.
  (d) `magnetar-differential`: equivalence test asserting the challenge/response `EventStream` parity across engines on the same scripted token transcript.

- **E2E fixture needs.** A separate `e2e_kerberos_*.rs` test brings up a KDC alongside the broker: `docker compose` with `apachepulsar/pulsar:4.0.4` (Pulsar's `conf/standalone.conf` enables SASL with `authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderSasl`)
  - `gcavalcante8808/krb5-server:latest` (MIT KRB5 KDC) on a shared
    Docker network.
    Test fixture writes a `krb5.conf` pointing at the KDC, kinit-fetches a ticket cache, runs a producer+consumer round-trip.
    Gated by `#[ignore = "e2e: requires Docker + KDC"]` and the `e2e` cargo feature.

- **LOC estimate.** ~900–1300 LOC total. Breakdown: ~250 LOC `SaslMechanism` trait + `Conn` driver changes in `magnetar-proto`; ~400 LOC GSSAPI binding in `magnetar-auth-sasl` (config struct, `libgssapi` wrapper, `step` loop, error mapping); ~150 LOC `SaslKerberosFake`; ~200 LOC tests (4-layer); ~100 LOC e2e fixture + docs.

- **Security implications.** GSSAPI integration crosses a process boundary into a C runtime (libkrb5).
  Mitigations: (a) `libgssapi` is the only `unsafe` crate magnetar pulls in for this surface, reviewed and pinned to a known version; (b) keytab paths are read at config time only — magnetar never writes ticket caches; (c) `forbid(unsafe_code)` stays at the magnetar-auth-sasl crate root; the only `unsafe` is internal to `libgssapi`; (d) the KDC fixture in e2e is firewalled inside the Docker network.
  Mutual auth verification (`gss_inquire_context` for the verified peer name) is asserted **before** the connection completes the pulsar CONNECTED step, surfacing principal mismatch as a connection failure rather than a silent succeed.

- **Confined deferral.** A later follow-up may add JAAS-file parsing if a downstream user requires drop-in compatibility with a Java application's JAAS config.
  Not in scope here.

## As-implemented amendments (2026-05-26)

The design under "Decision" was tightened during implementation in two places — both simplifications, neither changes the wire behaviour described in this ADR:

1. **No new `SaslMechanism` trait in `magnetar-proto`.** The provided design proposed introducing a `SaslMechanism` trait inside `magnetar-proto::auth` so the `Conn` state machine could drive multi-step authentication.
   As-shipped the existing `AuthProvider::respond_to_challenge` hook (already used by PIP-30 / PIP-292 token refresh) is reused verbatim: each `CommandAuthChallenge` is funneled through the same `AuthChallengeState::handle_challenge` path, and `SaslKerberos` threads continuation tokens by forwarding into the wrapped `GssapiClient`.
   No protocol-layer changes were necessary; `magnetar-proto` already supports arbitrary multi-round SASL handshakes via `AuthChallengeState` (verified by the new `multi_round_handshake_threads_continuation_tokens` test on `magnetar-proto::auth`).

2. **No `SaslKerberosConfig` JAAS-subset struct (yet).** The provided design proposed surfacing `SaslKerberosConfig` with `jaas_section` / `server_type` / `server_principal` / `keytab` / `ticket_cache` fields.
   As-shipped the constructor is the simpler `SaslKerberos::with_principal(spn: &str)`, which delegates to `libgssapi::credential::Cred::acquire` with default initiator credentials — i.e. the host's default credential cache or keytab as resolved by the system Kerberos config.
   Explicit keytab and ticket-cache injection becomes a follow-up if a downstream user needs it; the trade-off is documented here so the broader scope is preserved.

The `SaslKerberosFake` from the original design ships as [`magnetar_auth_sasl::ScriptedGssapiClient`] — a reusable `GssapiClient` implementation that replays a fixed transcript of `(challenge, reply, continue_needed)` triples.
All four sans-io test layers per ADR-0024 (proto, tokio, moonpool, differential) drive `ScriptedGssapiClient`; the e2e layer (`crates/magnetar/tests/e2e_sasl_kerberos.rs`) drives `LibGssapiClient` against a Dockerised KDC.

## Status

Accepted — landed via `feat/sasl-kerberos` on 2026-05-26.

## References

- [ADR-0009](0009-pulsar-4-minimum.md) — Pulsar 4.0+ minimum.
- [ADR-0010](0010-v0-1-full-java-parity.md) — full Java parity, earlier SASL Kerberos deferral now lifted.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — cross-runtime test policy; binding test plan for the four-layer set above.
- [ADR-0026 §D3](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md) — design-time deferral rationale.
- Apache Pulsar Java — `org.apache.pulsar.client.impl.auth.AuthenticationSasl` (artifact `pulsar-client-auth-sasl`).
- Apache Pulsar SASL docs —
  <https://pulsar.apache.org/docs/security-kerberos/>
- `libgssapi` (Rust binding) —
  <https://crates.io/crates/libgssapi>
- `crates/magnetar-auth-sasl/src/kerberos.rs` — `SaslKerberos` provider + `GssapiClient` sans-io seam + `ScriptedGssapiClient` test driver; `crates/magnetar-auth-sasl/src/gssapi.rs` carries the `LibGssapiClient` production binding under the `kerberos` cargo feature.
- `crates/magnetar-proto/proto/PulsarApi.proto:329-337,1300-1301` — `CommandAuthChallenge` / `CommandAuthResponse` already vendored.
