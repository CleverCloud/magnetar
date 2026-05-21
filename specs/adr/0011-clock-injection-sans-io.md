# ADR-0011 — Clock injection on `magnetar-proto` entries

- **Status**: Accepted
- **Date**: 2026-05-21
- **Decider**: Florentin Dubois
- **Tags**: sans-io, determinism, simulation

## Context

[ADR-0004](0004-sans-io-protocol-core.md) bans I/O from `magnetar-proto`.
For a long time we considered "the host clock is fine — that's not I/O" and
let the state machine call `std::time::Instant::now()` / `SystemTime::now()`
directly.

That was a mistake. Reading the host clock IS a host call. Once the state
machine reads the clock:

- The moonpool simulator can no longer reproduce runs bit-for-bit (the clock
  shifts between runs).
- Unit tests for time-dependent behaviour (ack timeout, redelivery backoff,
  send timeout) need real `tokio::time::sleep` calls instead of advancing a
  virtual clock.
- A `cargo xtask check-no-internal-clock` enforcement is not possible.

The user (Florentin) caught this gap in the `ARCHITECTURE.md` claim and asked
for a fix.

## Decision

Every entry into `magnetar-proto::Connection` that previously read the host
clock now takes an `Instant` (and where applicable a `SystemTime`-providing
`Arc<dyn Fn() -> SystemTime + Send + Sync>`).

| Entry | Clock parameter |
| --- | --- |
| `Connection::send(now: Instant, …)` | `Instant` |
| `Connection::flush_producer(now: Instant, …)` | `Instant` |
| `Connection::handle_timeout(now: Instant)` | `Instant` |
| `Connection::ack_grouped_individual(now: Instant, …)` | `Instant` |
| `Connection::ack_grouped_cumulative(now: Instant, …)` | `Instant` |
| `Connection::negative_ack(now: Instant, …)` | `Instant` |
| `Connection::negative_ack_with_delay(now: Instant, …)` | `Instant` |
| `ConsumerState::deliver(now: Instant, …)` | `Instant` |
| `ProducerState::queue_send(now: Instant, …)` etc. | `Instant` |
| `wall_clock` provider (constructor + setter) | `Arc<dyn Fn() -> SystemTime + Send + Sync>` |

Engines snapshot the host clock at the call boundary:

- `magnetar-runtime-tokio`: `std::time::Instant::now()` immediately before
  taking the connection lock; default `wall_clock = std::sync::Arc::new(SystemTime::now)`.
- `magnetar-runtime-moonpool`: the virtual clock provider does the same;
  moonpool's `TimeProvider` plugs into both.

**Two documented non-time leaks remain** (tracked, not closed):
- `uuid::Uuid::new_v4()` in `ProducerState::emit_chunked` (PIP-37 chunked
  messages need a uuid for the chunk-set id).
- `std::env::var()` in `crates/magnetar-proto/src/auth/token.rs` for one-shot
  bootstrap.

Both are listed under "Known non-determinism leaks (documented)" in
[`ARCHITECTURE.md`](../../ARCHITECTURE.md) and would require a `Random`/`Env`
provider to close.

A `cargo xtask check-no-internal-clock` enforcement is planned to grep
`crates/magnetar-proto/src/**` for direct `Instant::now()` / `SystemTime::now()`
calls outside `#[cfg(test)]`.

## Consequences

- Adding a new entry to `Connection` requires a `now: Instant` parameter
  (and a `wall_clock` parameter when it stamps a `publish_time_ms`).
- Test code in `magnetar-proto` builds an explicit `Instant` per test (no
  global clock pollution).
- moonpool-sim runs reproduce bit-for-bit under a given seed for everything
  except the two documented leaks.

## References

- [`ARCHITECTURE.md` §"Sans-io design"](../../ARCHITECTURE.md) (clock
  injection table + non-determinism leaks)
- `crates/magnetar-proto/src/conn.rs` — `Connection::send` etc.
- `crates/magnetar-runtime-tokio/src/consumer.rs`,
  `crates/magnetar-runtime-tokio/src/producer.rs` — clock snapshot sites
- `crates/magnetar-runtime-moonpool/src/consumer.rs`,
  `crates/magnetar-runtime-moonpool/src/producer.rs` — virtual clock plumbing
- Commit `2c47af9` — "fix(proto): close sans-io clock leaks — inject now:
  Instant + wall_clock provider"
- [ADR-0004 sans-io](0004-sans-io-protocol-core.md)
