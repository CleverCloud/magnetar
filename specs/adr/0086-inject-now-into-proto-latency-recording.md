# ADR-0086 — Record latency against the injected clock, ban `.elapsed()` in the sans-io core

- **Status**: Accepted (amends [ADR-0011](0011-clock-injection-sans-io.md))
- **Date**: 2026-07-29
- **Decider**: Florentin Dubois
- **Tags**: sans-io, determinism, simulation, observability

## Context

[ADR-0011](0011-clock-injection-sans-io.md) requires every `magnetar-proto` entry point that needs time to take an explicit `now: Instant`, so the engines snapshot the host clock at the call boundary and moonpool can plug in a virtual one.
Two latency-recording sites escaped that sweep, because they read the clock through `Instant::elapsed()` rather than `Instant::now()`:

- `ConsumerState::pop_message` recorded `msg.arrived_at.elapsed()`.
- `ProducerState::apply_receipt` recorded `op.enqueued_at.elapsed()`.

Both _write_ ends were already injected — `IncomingMessage::arrived_at` is stamped from the `now` passed to `ConsumerState::deliver`, and `OpSend::enqueued_at` from the `now` passed to `ProducerState::queue_send`.
Only the read side leaked.

Three consequences, all observed rather than theorised:

1. **Moonpool's latency histograms were not reproducible per seed.** They were the only part of `ConsumerStats` / `ProducerStats` that was not a pure function of the simulated clock. Worse than noisy: under `SimProviders` virtual time outruns host time, so `arrived_at.elapsed()` saturated to `0` on essentially every sample — the histograms were not merely non-deterministic, they were empty of signal.
2. **The gate could not see it.** `cargo run -p xtask -- check-no-internal-clock` matched only the literal strings `Instant::now()` and `SystemTime::now()`. On top of that, `crates/magnetar-proto/src/producer.rs` sat in `CLOCK_LEAK_ALLOWLIST` — a whole-file skip whose stated rationale was the `uuid::Uuid::new_v4()` leak, a pattern the gate has never scanned for. The entry bought no enforcement and cost real blindness over one of the two leak sites.
3. **Tests were written around it.** `crates/magnetar-differential/tests/aggregate_stats_equivalence.rs` carried a `seed_deterministic_latency` helper that overwrote each consumer's `receive_latency_hist` with a synthetic distribution before snapshotting, and both `stats_histogram_accessor.rs` twins carried a header note stating they assert sample _counts_, "never specific millisecond values".

Alternatives considered and rejected:

- **Cache a `now` on `Connection`, refreshed by `handle_bytes` / `handle_timeout`, and read it at pop time.** Avoids the signature change, but reads a stale clock whose staleness depends on inbound traffic — precisely the coupling ADR-0011 exists to remove — and hides the dependency from the caller.
- **Compute the latency at delivery time.** Wrong quantity: receive latency _is_ the queue dwell between arrival and pop, so it is only knowable at pop.
- **`now - msg.arrived_at` using the `Sub` impl.** Panics on underflow, violating invariant #6.

## Decision

Latency in `magnetar-proto` is measured against the caller-supplied instant, and `.elapsed()` is banned from the crate.

- Three signatures gain an explicit `now`, placed last to match the crate's convention (`queue_send(msg, publish_time_ms, now)`, `Connection::ack(handle, ack, now)`, `Connection::consumer_record_rate_window(handle, now)`):
  - `ConsumerState::pop_message(&mut self, now: Instant)`
  - `Connection::pop_message(&mut self, handle: ConsumerHandle, now: Instant)`
  - `ProducerState::apply_receipt(&mut self, receipt: &pb::CommandSendReceipt, now: Instant)`
- Both sites compute the sample with `now.saturating_duration_since(base)`, never the `Sub` impl. A `now` behind the base instant records `0` rather than panicking (invariant #6; same panic-free-time discipline as `crate::time::deadline_with_clamp`).
- The engines supply `now` at the call boundary, before taking the connection mutex ([ADR-0038](0038-split-connection-mutex.md) lock ordering): `magnetar-runtime-tokio` reads `std::time::Instant::now()`, `magnetar-runtime-moonpool` reads its injected `now_instant_provider` via `shared.now_instant()`. The moonpool path is what makes the histograms reproducible per seed.
- `check-no-internal-clock` gains `.elapsed()` as a third needle. The **leading dot is load-bearing**: a bare `elapsed()` would match legitimate method names such as `batch_deadline_elapsed(now)`.
- `CLOCK_LEAK_ALLOWLIST` becomes **empty** and should stay that way. The `uuid` and `env::var` leaks it nominally tracked are documented in [`ARCHITECTURE.md`](../../ARCHITECTURE.md) under "Known non-determinism leaks", which is the inventory of record; the gate does not scan for them and never claimed to.
- The gate's scanner is now the pure, unit-tested `scan_clock_violations`, backed by the same `cfg_test_line_flags` and `skip_inert_region` helpers `check-log-fields` uses, replacing a hand-rolled duplicate whose `line.find("//")` comment strip exempted any line containing `//` inside a string literal.

`ProducerState::apply_send_error` is deliberately untouched: it resolves a pending op without recording latency, so it needs no clock.

## Consequences

**Easier.** Moonpool latency histograms are now a pure function of the simulated clock, so they can be asserted exactly, folded, and compared across engines. The differential suite compares real percentiles produced by the production pop path instead of a synthetic fixture, and both `stats_histogram_accessor.rs` twins now assert exact millisecond values rather than bare counts.

**Harder / cost.** This is a breaking change to `magnetar-proto`'s public API, re-exported through `magnetar::proto`, so any direct caller of the three functions outside the two engines must thread an instant. The ergonomic façade surface (`Consumer::{receive, receive_batch, drain_messages}`, `ConsumerApi`, `ProducerApi`, `aggregate_stats`, `receive_latency_histogram`, `send_latency_histogram`) is unchanged.

**Incompatible with.** Any future helper that wants to record latency without a caller-supplied instant. That is the point.

**Watch out for.** The in-process test layers all compare against a _scripted_ delta, so they cannot catch a regression that pins `now` to a constant (`pop_message(msg.arrived_at)`, or an engine snapshotting once at subscribe time) — that shape zeroes the histogram forever while every simulation test still passes. `e2e_receive_latency_reflects_real_queue_dwell` in `crates/magnetar/tests/e2e_aggregate_stats.rs` is the only layer that observes a real dwell, and exists for exactly that reason.

## References

- `docs/follow-ups.md` §3 — the tracker entry this closes.
- `crates/magnetar-proto/src/consumer.rs` — `ConsumerState::pop_message`.
- `crates/magnetar-proto/src/producer.rs` — `ProducerState::apply_receipt`.
- `crates/magnetar-proto/src/conn.rs` — `Connection::pop_message` and the `SendReceipt` fan-out that forwards `now` from `handle_frame`.
- `crates/magnetar-runtime-tokio/src/consumer.rs`, `crates/magnetar-runtime-moonpool/src/consumer.rs` — the five engine call sites.
- `xtask/src/main.rs` — `CLOCK_NEEDLES`, `CLOCK_LEAK_ALLOWLIST`, `scan_clock_violations` and its unit tests.
- `crates/magnetar-runtime-{tokio,moonpool}/tests/latency_histogram_injected_clock.rs` — the determinism proof (ADR-0024 layers (b) and (c)).
- `crates/magnetar-differential/tests/latency_histogram_call_boundary_equivalence.rs`, `crates/magnetar-differential/tests/aggregate_stats_equivalence.rs` — layer (d).
- `crates/magnetar/tests/e2e_aggregate_stats.rs` — layer (e).
- [ADR-0011](0011-clock-injection-sans-io.md) — the clock-injection rule this amends.
- [ADR-0004](0004-sans-io-protocol-core.md) — the sans-io core.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the four-layer test policy this changeset follows.
- [ADR-0038](0038-split-connection-mutex.md) — the lock ordering the engine call sites respect.
