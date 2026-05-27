# ADR-0005 — TLS via `rustls` only, no `native-tls` / `openssl`

- **Status**: Accepted (amended by [ADR-0035](0035-pluggable-crypto-provider.md), openssl ban portion)
- **Date**: 2026-05-20
- **Decider**: Florentin Dubois
- **Tags**: tls, dependencies, supply-chain

> **Amendment (2026-05-26, [ADR-0035](0035-pluggable-crypto-provider.md)).**
> The `openssl` / `openssl-sys` ban is narrowed: those crates re-enter
> the dep graph **only** as transitive deps of `rustls-openssl` (the
> rustls crypto-provider wrapper around system OpenSSL), gated by the
> `crypto-openssl` Cargo feature and scoped in `deny.toml` via
> `wrappers = ["rustls-openssl"]`. Everything else in this ADR stays in
> force: `rustls` is still the only TLS implementation, `native-tls`
> stays banned, TLS-1.3 stays the wire default, and the moonpool
> byte-pipe adapter is unchanged.

## Context

Pulsar brokers commonly use TLS. The Rust ecosystem has three contenders:

| Backend | Build | Memory safety | Notes |
| --- | --- | --- | --- |
| `rustls` (+ `ring` / `aws-lc-rs`) | Pure Rust | Yes | Sans-io, drives itself from byte buffers — composes well with [ADR-0004](0004-sans-io-protocol-core.md). |
| `native-tls` (wraps SChannel / Secure Transport / OpenSSL) | Depends on system libs | Inherits the system crypto's bugs | Cross-platform variance. |
| `openssl` / `openssl-sys` | Links OpenSSL | C codebase | Audited but historically the source of most CVE traffic in this space. |

`rustls` is the only one that:
- Is itself sans-io, so the moonpool engine can drive
  `rustls::ClientConnection` over a byte pipe with no async runtime
  ([ADR-0006](0006-moonpool-tls-byte-pipe.md)).
- Has no system-library dependency, so cross-compiling and reproducibility
  are trivial.
- Doesn't pull in a giant C codebase that nobody on the team owns.

## Decision

- **Only `rustls`** is allowed for TLS.
- `tokio-rustls` is the tokio-side adapter (the workspace dep is
  `tokio-rustls = "0.26"` with the `ring` provider).
- `magnetar-admin` uses `reqwest` with `rustls-tls` (also via `ring`).
- `rustls-pemfile` parses trust chains supplied by the user.
- `rustls-native-certs` is the bridge to the system trust store (default).
- **No `native-tls`. No `openssl`. No `openssl-sys`. No `native-tls-sys`.**
  Banned via `deny.toml` (`[bans] deny`).
- TLS-1.3 is the wire default; TLS-1.2 stays enabled because some on-prem
  brokers still ship with it.

For the moonpool engine the byte-pipe adapter lives at
`crates/magnetar-runtime-moonpool/src/tls.rs` — see [ADR-0006](0006-moonpool-tls-byte-pipe.md).

## Consequences

- We can ship statically-linked binaries against musl / glibc without
  fighting OpenSSL ABI breaks.
- A user who insists on the system OpenSSL has to fork — but that's an
  acceptable cost.
- Per-connection TLS context can be customised with
  `rustls::ClientConfig::builder()` for advanced cases like client certs
  ([ADR-0010](0010-v0-1-full-java-parity.md) committed to mTLS parity).

## References

- [`GUIDELINES.md` §"TLS"](../../GUIDELINES.md)
- [ADR-0006 moonpool TLS](0006-moonpool-tls-byte-pipe.md)
- [ADR-0035 pluggable crypto provider](0035-pluggable-crypto-provider.md)
  (amendment carving out `rustls-openssl` for the `crypto-openssl` feature)
- `deny.toml` — `[bans] deny` entries for native-tls/openssl
