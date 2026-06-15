# ADR-0068 — Read the pulsarctl config file and manage contexts in `magnetarctl`

- **Status**: Accepted
- **Date**: 2026-06-15
- **Decider**: Florentin Dubois
- **Tags**: cli, config, contexts, pulsarctl, oauth2, tls, admin

## Context

`magnetarctl` reads its connection settings only from flags / env: `--admin-url` / `MAGNETAR_ADMIN_URL` (default `http://localhost:8080`), `--service-url` / `MAGNETAR_SERVICE_URL` (default `pulsar://localhost:6650`), and `--token` / `MAGNETAR_TOKEN` (`crates/magnetar-cli/src/main.rs` globals).
It has no awareness of the standard pulsarctl config file, so a user with a working pulsarctl setup gets the built-in localhost default with no credentials:

```console
❯ magnetarctl admin clusters list -vvvv
magnetar: json decode: expected value at line 1 column 1
```

(reported by @Miton18 while onboarding `magnetarctl` for the otelgw / logs-api work — GitHub issue [CleverCloud/magnetar#281](https://github.com/CleverCloud/magnetar/issues/281)).
The cryptic decode error itself — the admin client should surface the HTTP status + body snippet on a non-JSON response — is tracked separately in #282 and is out of scope here.

The drop-in goal is twofold: (1) **read** an existing `~/.config/pulsar/config` as-is, and (2) **manage** contexts with a `magnetarctl context` group mirroring `pulsarctl context`.
The file format is fixed by `streamnative/pulsarctl` [`pkg/cmdutils/ctx_conf.go`](https://github.com/streamnative/pulsarctl/blob/master/pkg/cmdutils/ctx_conf.go).
Its casing is intentionally mixed — kebab-case at the top level (`auth-info`, `contexts`, `current-context`), snake_case inside `auth-info` with a lone camelCase outlier (`tokenFile`) — and must be reproduced verbatim or pulsarctl can no longer read a file `magnetarctl` wrote.
Full `auth-info` parity means token + TLS (custom CA trust + allow-insecure) + OAuth2 `client_credentials`, none of which the admin client (`magnetar-admin::AdminClientBuilder`, only `service_url` / `token` / `timeout`) supported.

The building blocks were partly in-tree but unwired: `magnetar-auth-oauth2` (`ClientCredentialsFlow`) and `magnetar-runtime-tokio/src/tls_insecure.rs`.
What was missing: admin-client custom-CA / allow-insecure / OAuth2, a YAML (de)serializer with the exact tags, a context resolver, and the `context` command group.

## Decision

Teach `magnetarctl` to read and write the pulsarctl config and manage contexts, with full `auth-info` parity, while keeping the no-config path byte-identical to today.

- **Config format — reproduced verbatim.** Serde structs with explicit `#[serde(rename)]` for every documented key (`auth-info`, `contexts`, `current-context`; inside `auth-info`: `locationoforigin`, `tls_trust_certs_file_path`, `tls_allow_insecure_connection`, `token`, `tokenFile`, `issuer_endpoint`, `client_id`, `audience`, `scope`, `key_file`; inside `contexts`: `admin-service-url`, `bookie-service-url` — the only two keys, there is no `broker-service-url`).
  Each struct carries a `#[serde(flatten)] BTreeMap<String, serde_norway::Value>` so any unknown key round-trips untouched.
  A file written by `magnetarctl context set` stays readable by pulsarctl and vice-versa.
- **YAML crate: `serde_norway`** — the actively-maintained fork of the now-unmaintained `serde_yaml`; its untagged `serde_norway::Value` backs the unknown-key preservation.
  Added to `[workspace.dependencies]` and used by `magnetar-cli` only (MIT OR Apache-2.0, no banned channel crates).
- **Path resolution** (most-specific first): `--config <path>` › `MAGNETAR_CONFIG` › `$XDG_CONFIG_HOME/pulsar/config` (only when `XDG_CONFIG_HOME` is set) › `$HOME/.config/pulsar/config` (the pulsarctl-hardcoded default).
  A missing file at the default path is **not** an error (fall back to built-in defaults); a missing file at an **explicit** `--config` / `MAGNETAR_CONFIG` path **is** an error.
  `$HOME` / `$XDG_CONFIG_HOME` are read with `std::env::var` — the no-internal-clock / env allowlist (ADR-0011) governs `magnetar-proto`, not the CLI.
- **`context` command group**: `use <name>` (prints `Switched to context "<name>".`), `set <name>` (alias `create`, merges flag values onto existing fields), `delete <name>` (alias `del`, removes from BOTH `contexts` and `auth-info`, warns when it was the current context), `get` (table `CURRENT(*) NAME / ADMIN SERVICE URL / BOOKIE SERVICE URL`), `current` (errors when unset), `rename <old> <new>` (alias `update`, updates `current-context` when it pointed at `<old>`).
  Writes go to the resolved path, creating the parent dir and the file `0600` on Unix.
  The credential / TLS write values on `set` (`--token`, `--token-file`, `--tls-trust-cert-path`, `--tls-allow-insecure`) are the **global** connection flags — clap forbids re-declaring a global long name on a subcommand, so they are threaded in from the globals rather than redeclared; the context-only flags (`--admin-service-url`, `--bookie-service-url`, `--issuer-endpoint`/`-i`, `--client-id`/`-c`, `--audience`/`-a`, `--scope`, `--key-file`/`-k`) live on `set`.
- **Admin client extension** (`magnetar-admin`): `AdminAuth` gains an `OAuth2(Arc<ClientCredentialsFlow>)` arm (refresh-on-demand at request time → `Authorization: Bearer <access-token>`); `AdminClientBuilder` gains `oauth2(...)`, `tls_trust_cert_pem(Vec<u8>)`, and `tls_allow_insecure(bool)`, applied to the reqwest client via `add_root_certificate` / `danger_accept_invalid_certs` (reqwest already wraps rustls under the `crypto-*` features; no hand-built `ClientConfig`).
  `magnetar-admin` now depends on `magnetar-auth-oauth2` (acyclic) and forwards each `crypto-*` feature to it.
- **Data-plane URL derivation** (`produce` / `consume`): pulsarctl stores no `pulsar://` URL, so when a context is active and `--service-url` is absent, derive it from `admin-service-url` — keep the host, **always** substitute the default binary port, map the scheme: `http://host[:p]` → `pulsar://host:6650`, `https://host[:p]` → `pulsar+ssl://host:6651` (the canonical TLS scheme magnetar parses; NOT `pulsar+tls://`).
  The derived value is logged at startup (structured field, ADR-0054) so a wrong guess is visible.
- **Precedence — explicit always wins.** Per setting: explicit flag / env › active context › built-in localhost default.
  `--service-url` / `MAGNETAR_SERVICE_URL` and `--admin-url` / `-s` / `MAGNETAR_ADMIN_URL` dropped their clap `default_value` so a context can override the built-in default but a user-provided value cannot; the localhost fallback is applied in code after context resolution.
  No config file **and** no context → byte-identical to the pre-context behavior.

## Consequences

- A working pulsarctl config "just works" with `magnetarctl` (zero extra flags), and the config can be edited via the CLI instead of by hand.
- **Round-trip fidelity is a hard constraint**: the exact key casing plus the `#[serde(flatten)]` unknown-key capture are load-bearing — a regression that drops `locationoforigin` or normalises `tokenFile` to snake_case silently corrupts a shared pulsarctl config.
  Covered by a golden round-trip test (`crates/magnetar-cli/src/config/model.rs`).
- The data-plane derivation is a **heuristic, not a guarantee**: many deployments (incl. Clever Cloud behind the Pulsar Proxy) expose the binary protocol on a different host/port than the admin endpoint.
  An explicit `--service-url` / `MAGNETAR_SERVICE_URL` always overrides, and the derived value is logged so a wrong guess is obvious.
- New workspace dependency (`serde_norway`) and a new `magnetar-admin → magnetar-auth-oauth2` edge.
- OAuth2 from a context requires `key_file` (a Pulsar-style `client_id` + `client_secret` JSON blob): the on-disk format carries no inline `client_secret`, so the key file is the only secret source; a context with `issuer_endpoint` but no `key_file` fails with a clear error rather than a silent no-credential connection.
- The `--admin-service-url` long spelling is the `context set` write flag, not a global connection alias (clap name-clash with the global `--admin-url`); the global connection flag keeps `-s` + `--admin-url`.
- Cross-runtime four-layer test policy (ADR-0024) does **not** apply: this is a CLI + admin-HTTP change, not a `magnetar-proto` / runtime / wire change — no proto↔tokio↔moonpool↔differential parity surface is touched (justified per ADR-0024's exemption clause).

## References

- GitHub issue [CleverCloud/magnetar#281](https://github.com/CleverCloud/magnetar/issues/281) — the demand + the exact format spec; #282 — the out-of-scope non-JSON-response follow-up.
- `streamnative/pulsarctl` [`pkg/cmdutils/ctx_conf.go`](https://github.com/streamnative/pulsarctl/blob/master/pkg/cmdutils/ctx_conf.go) — config-format source of truth.
- `crates/magnetar-cli/src/config/` — `model.rs` (serde tags + round-trip), `file.rs` (path resolution + load/save), `resolve.rs` (context selection + data-plane derivation).
- `crates/magnetar-cli/src/main.rs` — `context` command group, connection resolution, precedence.
- `crates/magnetar-admin/src/lib.rs` — `AdminAuth::OAuth2`, builder `oauth2` / `tls_trust_cert_pem` / `tls_allow_insecure`.
- `docs/cli.md` — "Config file & contexts" + the `context` command reference.
- [ADR-0011](0011-clock-injection-sans-io.md) (env allowlist scope), [ADR-0014](0014-oauth2-client-credentials-caching.md) (OAuth2 flow), [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) (test-policy exemption), [ADR-0035](0035-pluggable-crypto-provider.md) (crypto-feature forwarding), [ADR-0054](0054-logging-policy.md) (structured derived-URL log).
