# ADR-0097 — Use Tokio time for driver write deadlines

- **Status**: Accepted
- **Date**: 2026-08-05
- **Decider**: Florentin Dubois
- **Tags**: runtime, tokio, deadline, determinism, testing
- **Amends**: [ADR-0083](0083-bounded-cancellable-driver-write.md)

## Context

ADR-0083 anchored one fixed `operation_timeout` deadline outside the driver's `select!` so a logical write keeps the same budget when another arm wins and its write future is reconstructed.
The Tokio implementation mixed two monotonic clock domains: it stored that anchor as `std::time::Instant`, recomputed a real-clock remaining duration on every reconstruction, and handed that duration to `tokio::time::timeout`.

That is approximately coherent in an ordinary production runtime because both clocks advance with the host monotonic clock.
It is incoherent under `#[tokio::test(start_paused = true)]`: Tokio virtual time advances while `std::time::Instant` does not.
After a higher-priority arm cancelled the pending write future, reconstruction observed almost the full real-clock duration again and armed a fresh Tokio timer, while the test's 90-second virtual-time harness could expire first.
Host load changed task scheduling around Tokio's automatic time advance, which made `stalled_write_is_bounded_by_operation_timeout` intermittent despite running on a paused current-thread runtime.

The previous test did not prove ADR-0083's fixed-deadline property.
It waited under a broad relative harness timeout but never forced another arm to cancel and reconstruct the write future before the original deadline.

## Decision

The Tokio driver's write-local deadline uses Tokio time end to end.

1. Store `write_deadline` as `Option<tokio::time::Instant>` and arm it with `tokio::time::Instant::now() + operation_timeout` when a logical write first gains work.
2. Retain that one absolute deadline while bytes or a pending TLS flush remain, exactly as ADR-0083 requires.
3. Race each reconstructed `write_one_budget` future with `tokio::time::timeout_at(deadline, ...)`, not a newly armed relative timeout.
4. Keep expiry behavior unchanged: return `io::ErrorKind::TimedOut`, call `mark_disconnected()`, and let the existing supervisor reconnect.

This is a write-local runtime scheduling clock only.
Proto-facing `std::time::Instant` values remain unchanged, and the Moonpool driver continues to use its injected `TimeProvider` clock.

The regression test now proves the property directly on paused Tokio time.
It observes the first stalled write poll, advances to one second before the original deadline, wakes the higher-priority driver-waker arm to force cancellation, observes the reconstructed write poll, then advances beyond the original deadline while remaining strictly before a freshly re-armed deadline.
The driver must return `Io(TimedOut)` and disconnect before that absolute guard.

## Consequences

**Deterministic test.** The deadline under test and the harness use the same controllable Tokio clock, so host load cannot race virtual time against a host-clock deadline.

**Production behavior preserved.** A logical write still receives one `operation_timeout` budget across every `select!` reconstruction; `timeout_at` states that absolute contract directly.

**Narrow scope.** No public API, wire format, proto state, Moonpool behavior, timeout value, select ordering, or reconnect path changes.
Other Tokio runtime deadlines are not migrated by this decision.

**Test evidence.** The cancellation-and-reconstruction test passed 128 consecutive isolated repetitions, the Tokio driver test module passed all 14 tests, and runtime test parity remained 362/362.

## References

- `crates/magnetar-runtime-tokio/src/driver.rs` — `driver_loop_inner`, `write_one_budget`, and `stalled_write_is_bounded_by_operation_timeout`.
- [ADR-0083](0083-bounded-cancellable-driver-write.md) — the fixed logical-write deadline and cancellation-safe write arm retained here.
- `docs/follow-ups.md` §15 — the diagnosed mixed-clock flake closed by this ADR.
