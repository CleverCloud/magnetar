# ADR-0003 — Ban channel crates from the workspace

- **Status**: Accepted
- **Date**: 2026-05-20
- **Decider**: Florentin Dubois
- **Tags**: concurrency, architecture, async

## Context

The straightforward way to wire a Pulsar driver in Rust is producer → mpsc →
driver task → mpsc → consumer future, à la most existing pulsar / kafka client
crates. But channels carry well-known pathologies in long-lived network
clients:

- **Hidden backpressure.** An mpsc with a finite capacity stalls the producer
  when the driver falls behind; an unbounded mpsc OOMs instead. Neither is
  what the broker semantics imply.
- **Channel leaks.** A future dropped mid-flight leaves an orphan sender — the
  driver eventually sees `Disconnected` but only after the next send attempt,
  so close races become "where did this message go?" debugging sessions.
- **Deadlocks on close.** Bidirectional channels need careful drop ordering;
  the driver task must drain remaining items before close.

The sans-io split (per [ADR-0004](0004-sans-io-protocol-core.md)) makes the
alternative natural: state lives in `magnetar-proto::Connection`, the engine
owns one driver task, user-facing futures register their `Waker` in a slab
inside the state machine. The driver dispatches wakers as events arrive.

## Decision

The following crates are **banned everywhere** in the workspace:

- `tokio::sync::mpsc`, `tokio::sync::broadcast`, `tokio::sync::watch`,
  `tokio::sync::oneshot`
- `std::sync::mpsc`
- `crossbeam-channel`
- `flume`
- `async-channel`
- `kanal`, `postage`, `tachyonix`, `thingbuf`

Replacement pattern:

- **Producer-to-driver path** → `Arc<parking_lot::Mutex<ConnectionShared>>`
  + `tokio::sync::Notify`.
- **Future completion** → in-state `Waker` slabs keyed by `op_id` /
  `sequence_id` / `request_id`.
- **Inter-task multiplexing** → `tokio::select!` (this is control-flow, not a
  channel).

Enforcement is belt-and-braces:

1. `deny.toml` bans the crates outright.
2. `clippy.toml`'s `disallowed-types` covers `tokio::sync::*` channel paths.
3. `cargo xtask check-no-channels` greps `src/**` so a hand-roll can't slip in.

**Note**: `tokio::sync::Notify` is NOT a channel — it's a wake-up flag. It is
allowed. `tokio::task::JoinSet` is allowed too.

## Consequences

- Anyone porting code from `pulsar-rs` or another Pulsar client has to rethink
  the dispatch shape. The README + `GUIDELINES.md` carry pointers.
- The waker-slab pattern shows up in every state machine in `magnetar-proto`
  (consumer, producer, transaction). It is the single most repeated pattern
  in the crate.
- We pay a `parking_lot::Mutex` hop on every user → driver transition. In
  practice this is faster than mpsc because the mutex is uncontended (one
  driver task owns the wake-up side).
- Reviewers must understand the rule before approving any changes inside
  `magnetar-proto` or the runtime crates.

## References

- [`docs/decisions-log.md` §"Architecture: no channels"](../../docs/decisions-log.md)
- [`docs/research.md`](../../docs/research.md) (channel pathology survey)
- [`ARCHITECTURE.md` §"The no-channels rationale"](../../ARCHITECTURE.md)
- `deny.toml` — `[bans] deny` entries
- `clippy.toml` — `disallowed-types`
- `xtask/src/main.rs` — `check-no-channels` command
- [ADR-0004 sans-io](0004-sans-io-protocol-core.md) (the architectural pre-req)
