# Phase 3 Batch D — status (2026-05-21)

Tracks the implementation outcome for Phase 3 Batch D
(`docs/research-e2e-ci.md` §2: `e2e_oauth2` + `e2e_tls`).

## Landed

- **`e2e_oauth2`** — `crates/magnetar/tests/e2e_oauth2.rs`, gated on
  `#[cfg(all(feature = "e2e", feature = "auth-oauth2"))]`. Three test
  cases covering the ADR-0014 surface:
  - `e2e_oauth2_happy_path_produces_and_consumes` — full producer +
    consumer round-trip with `ClientCredentialsFlow` driving
    `CommandConnect.auth_data`. Verifies the IDP `/oauth/token` is hit
    exactly once.
  - `e2e_oauth2_token_cache_reuses_across_connections` — two
    independent `PulsarClient`s share a single primed
    `ClientCredentialsFlow`; the IDP still records exactly one POST.
  - `e2e_oauth2_refresh_on_expiry_reissues_token` — advance the
    injected [`magnetar_auth_oauth2::Clock`] past
    `deadline - REFRESH_LEEWAY` and assert a second `/token` POST.
    Confirms the cached JWT bytes rotate.

  Workspace dev-deps gained `wiremock = "0.6"`; pulled in only for
  `magnetar`'s test target via `[dev-dependencies]`.

## Deferred — `e2e_tls`

Fixture cost overshoots the ~30-minute budget called out in the Batch D
prompt. The minimum-viable TLS broker requires all of:

1. A self-signed CA + broker certificate generated at test-bootstrap
   time (no `rcgen` in the workspace today, and `aws-lc-rs` does not
   ship a high-level certificate builder).
2. Mounting the resulting `cacert.pem` / `broker.cert.pem` /
   `broker.key-pk8.pem` triplet into the `apachepulsar/pulsar:4.0.4`
   container via `testcontainers::core::Mount`.
3. Enabling roughly half a dozen `PULSAR_PREFIX_*` env vars
   (`tlsEnabled`, `brokerServicePortTls=6651`, `tlsCertificateFilePath`,
   `tlsKeyFilePath`, `tlsTrustCertsFilePath`, …) so `bin/pulsar
   standalone` brings up the TLS listener alongside the binary port.
4. Four distinct fixture variants for the four planned test cases:
   - `tls_allow_insecure_connection(true)` over `pulsar+ssl://`.
   - `tls_allow_insecure_connection(false)` + valid trust store.
   - `tls_allow_insecure_connection(false)` + invalid trust store.
   - `tls_hostname_verification_enable(false)` with broker SAN
     mismatch.

Each axis multiplies (cert SAN, mount layout, listener wait condition),
and `wait_for(message_on_stdout(...))` does not work out of the box for
the TLS listener log line. Folding cert generation, mount, and env-var
plumbing into the standalone driver is *exactly* the kind of work
prompt (f) is asking us to spike before committing to a four-case
matrix.

Action: capture this as a follow-up in `docs/research-e2e-ci.md`
(`e2e_tls` row already marked **Confirm — needs work**) and revisit
together with the planned `e2e_proxy_sni` suite, which shares 80% of
the same TLS plumbing.

## Validation

- `cargo +nightly fmt --all`
- `cargo build --workspace --all-features --locked`
- `cargo clippy --workspace --all-features --all-targets -- -D warnings`
- `cargo test --workspace --all-features --locked`

The three `#[ignore = "e2e: requires Docker"]` test cases stay opt-in
— matching `e2e_pulsar.rs` and every other suite under
`crates/magnetar/tests/`. Local runs go through `cargo test
--features e2e,auth-oauth2 -p magnetar --test e2e_oauth2 --
--include-ignored --nocapture`.
