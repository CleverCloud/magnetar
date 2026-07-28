# ADR-0070 — Driver-loop read-arm fairness: poll inbound before the waker arm, keep `biased`

- **Status**: Accepted; amended by [ADR-0083](0083-bounded-cancellable-driver-write.md)
- **Date**: 2026-06-22
- **Decider**: Florentin Dubois
- **Tags**: runtime, driver-loop, performance, determinism

## Context

Each connection runs ONE driver task that multiplexes the outbound write path and the inbound read path in a single `tokio::select!` loop (`driver_loop_inner` in `crates/magnetar-runtime-tokio/src/driver.rs` and `crates/magnetar-runtime-moonpool/src/driver.rs`).
Each loop iteration drains staged outbound bytes (`poll_transmit_owned` + `write_all`) at the TOP, then parks in `tokio::select! { biased; … }` over three arms: the `driver_waker` notification, the socket read, and the next timeout.

Issue #303 (observed in production at magnetar `1.1.1`, rev `dd717db`): under sustained publish load a producer's `send().await` latency climbed to hundreds of milliseconds — seconds, while the broker persisted each entry in ~4 ms.
The multi-second time was entirely client-side.
Every `Producer::send` pulses `shared.driver_waker.notify_one()`, so under load a waker permit is almost always pending on loop entry.
The pre-fix arm order polled the `driver_waker` arm FIRST; with `biased;` the first ready arm wins, so whenever a permit was pending the inbound `socket.read_buf` arm was deprioritised that iteration.
Already-arrived `CommandSendReceipt`s sat unread in the kernel socket buffer and the matching `SendFut`s resolved late.
Little's law was self-consistent and pointed client-side: `in-flight ≈ throughput × latency` → `1150/s × 0.46 s ≈ 530 ≈ pending`.

Mature drivers avoid this by making read and write independent: `apache/pulsar-client-go` runs a dedicated reader goroutine (`readFromConnection` → `handleSendReceipt`) separate from the write event loop; the Java/Netty client dispatches inbound frames via `channelRead` while writes flush on the event loop.

Alternatives considered:

- **Full two-task read/write split** (a dedicated reader, mirroring pulsar-client-go). Highest blast radius: the rustls TLS stream cannot `into_split` cleanly, and the supervised reconnect path (`supervised_driver_loop`) would need to coordinate two tasks across redials. Deferred.
- **Drop `biased;`** so the select picks a ready arm fairly. Rejected: a non-biased `tokio::select!` chooses arms via an uncontrolled thread-local RNG, which would break the moonpool engine's bit-for-bit reproducibility (the whole deterministic-simulation suite and seed registry depend on it).
- **Opportunistic non-blocking read-drain before the select.** Viable but adds a second read path to keep in lockstep across both engines.

## Decision

Keep the single driver task and keep `biased;`, but **reorder the `select!` so the inbound read arm is polled FIRST**, before the `driver_waker` arm.
Applied IDENTICALLY to both engines so the differential `EventStream` parity (ADR-0024) holds.

- The read arm drains already-arrived `CommandSendReceipt` (and every other inbound frame) before the waker arm can win, so receipts are correlated to `SendFut`s promptly regardless of how busy the send path is.
- The outbound path is NOT starved by giving reads priority: `poll_transmit` + `write_all` run at the TOP of every loop iteration regardless of which arm wins, so each tick still flushes pending sends.
- `biased;` is retained, so the arm order is deterministic — required for moonpool reproducibility.
- The read arm is cancel-safe: bytes land in the persistent `read_buf` and are consumed via `read_buf.split()` only AFTER the arm wins, so reordering drops no bytes.
- The full read/write task split is recorded as a deferred follow-up, not half-built here.

## Consequences

- **Easier**: under sustained publish load, `send→ack` tracks broker latency instead of inflating as the in-flight queue deepens — the receipt path is no longer behind the outbound path in the poll order.
- **Harder / deferred**: this does not give per-connection read/write _parallelism_. While the single task `await`s a large or back-pressured `write_all`, it still cannot read; the full reader-task split (deferred) is what removes that coupling. The localized reorder removes only the `select!`-bias starvation.
- **Cost / bound**: `tokio::sync::Notify` stores a SINGLE permit, so the pre-fix order cost at most one extra loop iteration of read latency per permit refresh; the unbounded latency in #303 came from a permit being re-armed at essentially every select boundary under real multi-threaded send concurrency. The reorder removes that whole class.
- **Determinism**: the reordered arm interleaving is a different but still fully deterministic ordering under moonpool; the 32-seed sweep and the same-seed double-run stay reproducible.
- **Incompatible with**: dropping `biased;` (would break moonpool determinism) — that path is explicitly closed off.

## References

- `ARCHITECTURE.md` — "The driver loop" → "Read fairness" (the promoted description + state diagram).
- `crates/magnetar-runtime-tokio/src/driver.rs` — `driver_loop_inner` `select!` (read arm first, `biased;` retained).
- `crates/magnetar-runtime-moonpool/src/driver.rs` — identical reorder for differential parity.
- `crates/magnetar-runtime-tokio/tests/driver_read_fairness.rs`, `crates/magnetar-runtime-moonpool/tests/driver_read_fairness.rs`, `crates/magnetar-differential/tests/driver_read_fairness_equivalence.rs`, `crates/magnetar/tests/e2e_pulsar.rs::e2e_send_burst_all_receipts_resolve` — ADR-0024 four-layer coverage.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — cross-runtime test + parity policy this change satisfies.
- [ADR-0038](0038-split-connection-mutex.md) — the per-slot send hot path whose `notify_one` pulses drive the waker arm.
- [ADR-0003](0003-no-channels-rule.md) — why the driver wakes on `Notify` rather than a channel.
- Issue #303 — original report (production evidence, Little's-law analysis, pulsar-client-go / Java comparison).
