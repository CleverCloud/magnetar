# ADR-0008 — 11-crate workspace topology

- **Status**: Accepted
- **Date**: 2026-05-20
- **Decider**: Florentin Dubois
- **Tags**: workspace, dependency-direction, packaging

## Context

We could have shipped magnetar as a single crate with feature flags. That
flattens dependencies but pulls every optional path into every consumer's
compile time (and into the dep tree of every consumer of the consumer).

Splitting on natural seams keeps the surface area small per crate and makes
the no-I/O-deps gate on `magnetar-proto` enforceable in `Cargo.toml` rather
than convention.

## Decision

Eleven crates plus an internal `xtask`:

| Crate | Role | Public? |
| --- | --- | --- |
| `magnetar` | Top-level façade + builders + interceptors + typed schemas. Pins the active engine via `tokio` (default) or `moonpool` feature. | Yes |
| `magnetar-proto` | Sans-io state machine + codec + trackers. **Zero I/O deps**. | Yes |
| `magnetar-runtime-tokio` | Production engine: TCP + `tokio-rustls`, driver loop, producer / consumer / reader / table-view façades. | Yes |
| `magnetar-runtime-moonpool` | Deterministic-simulation engine over `moonpool_core::Providers`, mirror of the tokio engine surface. | Yes |
| `magnetar-admin` | `reqwest`-backed REST admin client. | Yes |
| `magnetar-cli` | `magnetar` binary — admin lookups, stats, data-plane subcommands. | Yes |
| `magnetar-fakes` | In-process broker stub for sans-io unit tests. **Dev-dep only**. | Yes (cargo dev-dep) |
| `magnetar-messagecrypto` | PIP-4 AES-GCM encryption bridge. | Yes (behind `encryption` feature) |
| `magnetar-auth-oauth2` | OAuth2 `ClientCredentialsFlow` provider. | Yes (behind `auth-oauth2` feature) |
| `magnetar-auth-sasl` | SASL/Kerberos provider. | Yes (behind `auth-sasl` feature) |
| `magnetar-auth-athenz` | Athenz provider. | Yes (behind `auth-athenz` feature) |
| `xtask` | `cargo xtask <task>` automation (codegen, no-channels grep, no-io-deps check, vendor-proto). | No (workspace-internal) |

Dependency direction is **strict**:

```
magnetar           — depends on magnetar-runtime-tokio | -moonpool, magnetar-admin, magnetar-messagecrypto, magnetar-auth-*
magnetar-runtime-* — depends on magnetar-proto
magnetar-proto     — depends on nothing magnetar-specific; **no I/O deps**
magnetar-admin     — depends on nothing magnetar-specific; reqwest+rustls-tls
magnetar-cli       — depends on magnetar + magnetar-admin
magnetar-fakes     — depends on magnetar-proto (sans-io fake broker)
magnetar-auth-*    — depends on magnetar-proto (auth trait lives there)
```

No cycle, no upward arrow.

## Consequences

- `magnetar-proto` can be published independently — anyone wiring their own
  engine can.
- `cargo tree -p magnetar-proto -e features` is the gate enforcing the
  no-I/O-deps rule (`xtask check-no-io-deps`).
- A user who only wants the admin REST client picks `magnetar-admin` directly,
  no producer/consumer code compiled.
- Adding a new auth flow is a self-contained sub-crate, no façade changes.

## References

- [`ARCHITECTURE.md` §"Crate topology"](../../ARCHITECTURE.md)
- `Cargo.toml` (workspace `[workspace.members]`)
- `xtask/src/main.rs` — `check-no-io-deps` command
- [ADR-0004 sans-io](0004-sans-io-protocol-core.md)
