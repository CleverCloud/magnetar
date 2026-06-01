# ADR-0004 — Sans-io `magnetar-proto` + swappable I/O engines

- **Status**: Accepted
- **Date**: 2026-05-20
- **Decider**: Florentin Dubois
- **Tags**: architecture, testing, determinism

## Context

The Apache Pulsar Java client tightly couples the protocol state machine to Netty.
Most existing Rust client experiments (`pulsar-rs`, `pulsar-client`) couple it to `tokio`.
Both designs make it impossible to:

- Drive the protocol under a deterministic simulator (for chaos / partition / ordering tests).
- Swap the I/O backend without rewriting the protocol layer.
- Mock the network in unit tests cleanly (today either you spin up a real broker or you mock at the trait-object level, which leaks impl detail).

`quinn-proto` (the QUIC implementation) demonstrates the alternative: a pure state machine with a `handle_bytes` / `poll_transmit` / `poll_event` / `poll_timeout` shape, fed by an I/O engine in a separate crate.
The Java client gets to assume Netty; the Rust client doesn't have a comparable hegemonic dependency, so we may as well be principled.

## Decision

Three-layer split:

```
+------------------+        +---------------------+
| magnetar-runtime |        | magnetar-runtime-   |
| -tokio           |        | moonpool            |
|                  |        | (deterministic sim) |
+------------------+        +---------------------+
         |                            |
         +-----------+    +-----------+
                     |    |
                +----v----v----+
                | magnetar-proto |  <- pure state machine
                +----------------+
                     ^
                     | (façade re-exports)
                +----+-----+
                | magnetar |  (top-level crate, behind `tokio` / `moonpool` features)
                +----------+
```

- `magnetar-proto` exposes `Connection`, `Producer`/`ConsumerHandle`, `OpOutcome`, `ConnectionEvent`, `Frame` codec.
  API surface: `handle_bytes`, `poll_transmit`, `poll_event`, `poll_timeout`, `handle_timeout`.
  Zero I/O dependencies (no `tokio`, no `mio`, no `socket2`, no `async-trait`).
  Enforced by `xtask check-no-io-deps`.
- `magnetar-runtime-tokio` owns one driver task per connection, lock + wake the state machine, do TCP / TLS (`tokio-rustls`) I/O.
- `magnetar-runtime-moonpool` does the same against `moonpool_core::Providers` for deterministic-simulation runs (chaos tests reproduce bit-for-bit under a given seed).

## Consequences

- The protocol layer is unit-testable without spinning up a runtime or a broker.
  The bulk of `magnetar-proto`'s tests are pure state-machine prods.
- Clock-reads inside `magnetar-proto` are forbidden — engines pass `now: Instant` and a `wall_clock` provider in (see [ADR-0011](0011-clock-injection-sans-io.md)).
- We pay a small lock + wake-up cost in the engines because the state machine must run under a `parking_lot::Mutex` (no internal sync primitives).
- Adding a third engine (e.g. `magnetar-runtime-glommio`) is a self-contained task — no protocol code changes.

## References

- [`ARCHITECTURE.md` §"Sans-io design"](../../ARCHITECTURE.md)
- [`GUIDELINES.md` §"I/O isolation"](../../GUIDELINES.md)
- `quinn-proto` (the architectural template for the `handle_bytes` / `poll_transmit` / `poll_event` / `poll_timeout` shape)
- [ADR-0003 no-channels](0003-no-channels-rule.md) (related concurrency rule)
- [ADR-0011 clock injection](0011-clock-injection-sans-io.md) (related sans-io rule)
