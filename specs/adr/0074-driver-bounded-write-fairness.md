# ADR-0074 — Bound driver write turns before returning to reads

- **Status**: Accepted; amended by [ADR-0083](0083-bounded-cancellable-driver-write.md)
- **Date**: 2026-07-01
- **Decider**: Florentin Dubois
- **Tags**: runtime, driver-loop, performance, determinism

## Context

ADR-0070 fixed the `tokio::select!` arm-order part of issue #303 by polling inbound reads before `driver_waker`.
That still left one coupling in the single-task driver: each loop iteration drained every staged outbound producer frame into an owned transmit and awaited `write_all` for the entire transmit before the task could read any `CommandSendReceipt`.
Issue #319 reported the remaining production symptom after `connections_per_broker` shipped: multiplying connections raised the aggregate floor, but the per-connection send-to-ack path still behaved like a latency-bound serial path under sustained producer load.

The full reader/writer task split remains the clean long-term architecture, but it is a larger change because TLS streams are not cleanly split and supervised reconnect would need to coordinate two tasks per socket.
A smaller fix is available inside the existing single task: retain the owned transmit in the driver and write only a bounded byte slice before returning to the existing read-first `select!`.

## Decision

Keep one driver task per connection, but make each write turn bounded.

- Both runtime drivers store an owned `PendingDriverWrite` queue outside `Connection`.
- A loop iteration pulls a new `poll_transmit_owned()` only when the previous queue is empty.
- Each iteration writes at most `DRIVER_WRITE_BUDGET_BYTES` bytes, currently 256 KiB.
- If bytes remain after that write, a ready continuation arm keeps the driver flushing on later iterations, but the inbound read arm stays first in the biased `select!`.
- Shutdown is delayed until the pending write queue drains, so close frames are not dropped.

The change is applied identically to `magnetar-runtime-tokio` and `magnetar-runtime-moonpool`.
The moonpool engine keeps deterministic ordering because the `select!` remains biased and the continuation arm has a fixed position after reads and `driver_waker`.

## Consequences

Send receipts can be read between large outbound write slices instead of waiting for the whole staged burst to be accepted by the socket.
This closes the remaining single-task write monopolisation reported in issue #319 without introducing channels or a second socket owner.

Large publishes can now span multiple driver iterations.
That slightly increases loop overhead for very large queued bursts, but bounds receipt-read latency and keeps the runtime responsive under sustained producer pressure.

Vectored producer segments remain zero-copy inside the pending queue.
The Tokio writer currently writes each retained segment slice via `write_all` rather than `write_vectored`; this is an intentional local tradeoff for bounded fairness and can be revisited if profiling shows syscall count matters more than receipt latency.

## References

- `crates/magnetar-runtime-tokio/src/driver.rs` — `PendingDriverWrite`, `DRIVER_WRITE_BUDGET_BYTES`, and the bounded write turn.
- `crates/magnetar-runtime-moonpool/src/driver.rs` — deterministic mirror of the bounded write turn.
- `crates/magnetar-runtime-tokio/src/driver.rs::tests::driver_write_budget_leaves_tail_for_next_tick` — Tokio unit guard for retained tails.
- `crates/magnetar-runtime-moonpool/src/driver.rs::tests::driver_write_budget_leaves_tail_for_next_tick` — moonpool unit guard for retained tails.
- `crates/magnetar-runtime-tokio/tests/driver_read_fairness.rs` and `crates/magnetar-runtime-moonpool/tests/driver_read_fairness.rs` — runtime coverage for receipt progress under driver-waker pressure.
- `crates/magnetar-differential/tests/driver_read_fairness_equivalence.rs` — cross-engine event-stream parity for the send burst.
- `crates/magnetar/tests/e2e_pulsar.rs::e2e_send_burst_all_receipts_resolve` — Docker e2e receipt-resolution guard for issues #303 and #319.
- [ADR-0070](0070-driver-read-arm-fairness.md) — the read-arm ordering that this bounded write turn composes with.
- [ADR-0073](0073-connections-per-broker.md) — connection fan-out that multiplies capacity but does not itself fix per-connection write/read coupling.
