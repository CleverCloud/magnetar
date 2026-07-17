# ADR-0077 — Close consumers when the last clone drops

- **Status**: Accepted
- **Date**: 2026-07-16
- **Decider**: Florentin Dubois
- **Tags**: consumer, lifecycle, raii, runtime-tokio, runtime-moonpool, proto, issue-342

## Context

[Issue #342](https://github.com/CleverCloud/magnetar/issues/342) reports that dropping every clone of a `Consumer` without calling `close().await` leaves the broker-side consumer registered while the shared connection remains alive.
That registration keeps receiving flow permits and prevents applications from treating handle abandonment as resource release.
It also breaks the lifecycle expectation of applications migrating from pulsar-rs, where Rust ownership is used to release consumer resources.

The Java client API defines an explicit `Consumer#close` operation, but it does not define Rust-style ownership or guarantee that abandoning a Java object closes the broker-side consumer.
Automatic close on drop is therefore Rust RAII and pulsar-rs migration parity, beyond Java abandonment semantics; Magnetar must not describe it as Java garbage-collection behavior.

The alternatives have the same fundamental constraints as the producer lifecycle decision in [ADR-0057](0057-producer-last-clone-drop-close.md).

- **Explicit close only** leaves early returns, cancellation, and ownership-driven teardown vulnerable to broker-side consumer leaks.
- **Blocking or spawning from `Drop`** either deadlocks an async runtime, requires a runtime that may already be unavailable, or detaches work whose completion cannot be guaranteed.
- **A naive `Drop` implementation on `Consumer`** closes the resource when any cheap clone drops, invalidating surviving clones.
- **Reusing the awaited close bookkeeping** records an `OpOutcome` that no future can drain, leaking one outcome for every abandoned consumer.

## Decision

Dropping the last clone of a runtime `Consumer` synchronously stages a best-effort, fire-and-forget `CommandCloseConsumer` and wakes the existing connection driver.

- Every Tokio and Moonpool consumer clone shares one `Arc<ConsumerCloseGuard>`, created at the engine's single consumer assembly point.
  Intermediate clone drops only decrement the `Arc`; the guard runs exactly once when the final clone disappears.
- The guard never spawns and never blocks.
  It probes the per-consumer slot's `closed` flag, releases that lock, calls `Connection::close_consumer_forget` under the global connection lock, and wakes the existing driver.
- The sequential slot probe followed by the global lock avoids reverse nested acquisition and preserves the global-to-per-slot ordering contract from [ADR-0038](0038-split-connection-mutex.md).
- If the connection has reached the terminal no-driver state, the guard is a no-op because no task remains to flush staged bytes.
- `Connection::close_consumer_forget` registers `PendingRequestKind::ConsumerCloseForgotten`.
  Broker success consumes that request in place, and broker rejection emits a bounded structured warning under [ADR-0054](0054-logging-policy.md); neither path creates an undrained `OpOutcome`.
- Connection reset and terminal `fail_all_pending` cleanup also discard forgotten producer and consumer closes without materializing outcomes.
- Explicit `Consumer::close().await` remains the reliable path.
  It awaits the broker acknowledgement and returns broker or terminal errors to the caller.
- A completed explicit close sets the slot's `closed` flag, so dropping the consumed final handle does not stage a duplicate close.
  The guard remains best-effort against concurrent close races.

This deliberately mirrors the producer last-clone lifecycle from [ADR-0057](0057-producer-last-clone-drop-close.md), with consumer identity fields and consumer-specific close bookkeeping.

## Consequences

- Ownership-driven teardown now attempts to unregister consumers while the shared client connection remains alive.
  When the staged frame reaches the broker and is accepted, the consumer is unregistered, typically enabling clean same-name recreation and preventing the abandoned consumer from retaining broker flow.
- Clone semantics remain safe: dropping an intermediate handle does not affect the surviving consumer, which can continue receiving and controlling flow.
- The best-effort path cannot report success or failure to the caller.
  Applications that require confirmation must call and await `close()`.
- A process exit, runtime teardown, terminal driver failure, or socket failure may prevent the staged close from reaching the broker; the broker then cleans the resource when the connection disappears.
- The protocol layer has separate awaited and forgotten consumer-close entry points, sharing the wire operation while keeping their completion bookkeeping distinct.
- Both runtime engines intentionally carry mirrored guards because their `ConnectionShared` types are concrete engine-specific types.
- The behavior follows [ADR-0054](0054-logging-policy.md) for structured lifecycle diagnostics and [ADR-0057](0057-producer-last-clone-drop-close.md) for the established last-clone RAII pattern.

## Verification

The change ships with the five behavioral layers required by [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md).

- The `#[cfg(test)]` unit-test module in `crates/magnetar-proto/src/conn.rs` verifies awaited and forgotten close bookkeeping, including reset and terminal cleanup.
- `crates/magnetar-runtime-{tokio,moonpool}/tests/consumer_drop_close.rs` verify final-clone close, intermediate-clone safety, continued use by a surviving clone, and explicit-close deduplication.
- `crates/magnetar-differential/tests/consumer_drop_equivalence.rs` verifies identical Tokio and Moonpool event streams and `Subscribe < CloseConsumer < Subscribe < CloseConsumer < Send` wire ordering.
- `crates/magnetar/tests/e2e_consumer_drop.rs` verifies against Pulsar 4.0.4 that final-clone drop unregisters the consumer while the client remains alive, same-name recreation succeeds, and explicit close remains the confirmation-bearing baseline.

## References

- `crates/magnetar-proto/src/conn.rs` — `close_consumer_forget`, `ConsumerCloseForgotten`, in-place response handling, cleanup behavior, and adjacent `#[cfg(test)]` unit coverage.
- `crates/magnetar-runtime-tokio/src/consumer.rs`, `crates/magnetar-runtime-moonpool/src/consumer.rs` — shared last-clone guards and public lifecycle contract.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — cross-runtime behavioral coverage.
- [ADR-0038](0038-split-connection-mutex.md) — lock ordering.
- [ADR-0054](0054-logging-policy.md) — bounded structured close diagnostics.
- [ADR-0057](0057-producer-last-clone-drop-close.md) — producer last-clone precedent.
- [Issue #342](https://github.com/CleverCloud/magnetar/issues/342).
