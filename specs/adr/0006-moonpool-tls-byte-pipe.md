# ADR-0006 — moonpool engine drives `rustls::ClientConnection` directly

- **Status**: Accepted
- **Date**: 2026-05-20
- **Decider**: Florentin Dubois
- **Tags**: tls, moonpool, simulation

## Context

The moonpool deterministic-simulation engine talks to brokers via a virtual
network provider. The simulator doesn't have a `tokio::io::AsyncRead +
AsyncWrite` socket — it has a byte pipe with its own `Providers::Network`
abstractions. We can't drop `tokio-rustls` in front of that — `tokio-rustls`
assumes a `tokio` `AsyncRead + AsyncWrite`.

Four options were considered (in [`docs/research.md`](../../docs/research.md)):

- **(a)** skip TLS in the moonpool engine — punt TLS handshake testing
  entirely.
- **(b)** write a tokio-shim around moonpool's network provider, then use
  `tokio-rustls` as-is.
- **(c)** abstract TLS behind a trait, swap implementations per engine.
- **(d)** drive `rustls::ClientConnection` (which is itself sans-io) by hand
  over the moonpool byte pipe.

(d) is the only one that preserves the deterministic-simulation guarantee.
`rustls::ClientConnection` accepts `read_tls(&mut io)` / `process_new_packets`
/ `write_tls(&mut io)` calls — the engine can pump these from the simulator's
byte queues, no async runtime needed.

## Decision

- **Option (d).** `crates/magnetar-runtime-moonpool/src/tls.rs` implements
  `RustlsByteAdapter`, which:
  - Reads TLS bytes from the moonpool socket into `rustls::ClientConnection`
    via `read_tls`.
  - Calls `process_new_packets` synchronously to advance the handshake.
  - Drains decrypted plaintext via `reader().read_to_end()` into a
    `plaintext_in` buffer for the driver loop to consume.
  - Mirror-symmetric for the write side (`write_tls`).
- The tokio engine keeps using `tokio-rustls` (it's the standard tokio
  pattern + easier to maintain).
- Three TLS sites total in the workspace: `magnetar-runtime-tokio` (via
  `tokio-rustls`), `magnetar-runtime-moonpool` (via the byte-pipe adapter),
  `magnetar-admin` (via `reqwest`/`rustls-tls`). All three use the **same**
  `rustls` crate version + provider.

## Consequences

- TLS handshakes survive `moonpool-sim` chaos with bit-for-bit determinism —
  bug reproductions are repeatable.
- The moonpool engine carries ~215 lines of TLS adapter code we maintain. It
  is bounded but non-trivial; reviewers familiar with `rustls` are required
  for changes there.
- A `rustls` major-version bump touches all three sites at once. That's
  acceptable — `rustls` major bumps land roughly yearly.

## References

- [`docs/research.md`](../../docs/research.md) §"moonpool TLS options"
- [`docs/decisions-log.md` §"moonpool TLS strategy (option d)"](../../docs/decisions-log.md)
- [`ARCHITECTURE.md` §"TLS sites"](../../ARCHITECTURE.md)
- `crates/magnetar-runtime-moonpool/src/tls.rs`
- [ADR-0005 rustls-only](0005-rustls-only-tls.md)
