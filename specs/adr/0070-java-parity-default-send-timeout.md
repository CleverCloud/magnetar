# ADR-0070 — Default the producer send-timeout to the Java client's 30 s

- **Status**: Accepted
- **Date**: 2026-06-22
- **Decider**: Florentin Dubois
- **Tags**: producer, parity, resilience, sans-io, determinism

## Context

`magnetar-proto`'s `CreateProducerRequest::send_timeout` was `None` by default (`crates/magnetar-proto/src/conn_types.rs`), which disables the per-send timeout sweep entirely: `ProducerState::drain_timed_out_sends` early-returns when `send_timeout` is `None` (`crates/magnetar-proto/src/producer.rs`).
A send whose `CommandSendReceipt` never arrives therefore stayed `Poll::Pending` **forever** — no error, no progress.

The Apache Pulsar Java client defaults `ProducerBuilder#sendTimeout` to `sendTimeoutMs = 30000` (30 s).
The PIP-466 V5 surface in this repo already mirrors that (`DEFAULT_SEND_TIMEOUT = Duration::from_secs(30)`, `crates/magnetar/src/v5/mapping.rs`), but the v4 builders inherited the proto `None`, so the two surfaces diverged: V5 producers timed out after 30 s, v4 producers hung indefinitely.

The gap surfaced as a real liveness failure under deterministic simulation.
Moonpool seed `0x4402f874c43758d1` failed `sim_chaos_produce_consume_with_invariants` / `sim_chaos_produce_consume_sweep_16_seeds`: the default-network bit-flip chaos corrupted a `CommandSendReceipt`'s `sequence_id` in flight (observed 7 → 15).
A `CommandSendReceipt` carries **no CRC32C** — only `CommandSend` payloads with magic `0x0e01` are checksum-protected (invariant #4) — so the structurally-valid-but-wrong receipt was delivered, `apply_receipt(15)` missed in `pending_index`, the receipt was dropped as genuinely-unknown, and the matching `send(seq=7)` future never resolved.
With `send_timeout = None` the orphaned send could never fail, so the chaos workload never completed and the run was scored a liveness failure.

Alternatives considered:

- **Leave the default `None`, fix only the test.** Rejected: it leaves every v4 producer able to hang forever on a lost receipt, contradicting the user-visible Java-parity target and diverging from the V5 surface.
- **Make the lost/corrupted receipt recoverable instead (reconnect + replay).** Rejected for this layer: a non-CRC-protected receipt corruption is structurally undetectable at the frame layer, so there is nothing to "recover" — the receipt was delivered and consumed. A deterministic timeout is the correct backstop, exactly as the Java client does.

## Decision

Default `CreateProducerRequest::send_timeout` to `Some(Duration::from_secs(30))` — byte-for-byte the Java client's `sendTimeoutMs = 30000`.

- The single canonical default lives in `CreateProducerRequest::default()` (`crates/magnetar-proto/src/conn_types.rs`).
  The v4 `ProducerBuilder` inherits it; `PartitionedProducerBuilder` seeds its field from `CreateProducerRequest::default().send_timeout` so it never silently pins the old no-timeout semantics; the `TypedProducerBuilder` only overrides `send_timeout` when the caller set it, so an unset typed builder also inherits the 30 s default.
  The V5 surface keeps its own `DEFAULT_SEND_TIMEOUT = 30 s` and now agrees with v4.
- The enforcement machinery is unchanged and clock-injected (ADR-0011): `ProducerState::next_send_deadline` surfaces `enqueued_at + send_timeout` via `Connection::poll_timeout`, and `Connection::handle_timeout(now)` resolves each timed-out in-flight send with `OpOutcome::SendError { code: -1, message: "send timeout" }`, wakes the parked waker, and drains the send from the producer's pending queue.
  Both runtime engines already drive `handle_timeout` against their injected clock.
- `ProducerBuilder::disable_send_timeout()` is added as the explicit escape hatch for callers that genuinely want the unbounded (never-times-out) semantics (mirrors Java `sendTimeout(0, …)`).

This is a **public default-behavior change**: a send now fails after 30 s by default instead of hanging.

## Consequences

Easier:

- A producer whose receipt is lost, dropped, or corrupted in flight fails deterministically with a clear timeout error rather than stranding the `send()` future forever.
- v4 and V5 producer defaults match each other and the Java client.

Harder / cost:

- Callers that relied on the old "a send never times out" behavior must now opt in via `ProducerBuilder::disable_send_timeout()` (or set `CreateProducerRequest::send_timeout = None`).
  In-tree tests that intentionally park a send forever set `None` explicitly.
- The moonpool `sim_chaos` produce/consume workload pins an explicit, shorter `send_timeout` (a few seconds) so a chaos-lost receipt fails fast **within** the run budget, and classifies the resulting timeout `SendError` as a bounded/handled outcome (the same spirit as the bit-flip clean-exit handling) — the genuine safety invariants (no double-resolve, monotonic broker `sequence_id`, no redelivery of acked messages) stay intact.

## References

- `crates/magnetar-proto/src/conn_types.rs` — the canonical default (`CreateProducerRequest::default`).
- `crates/magnetar-proto/src/producer.rs` — `next_send_deadline` / `drain_timed_out_sends` enforcement.
- `crates/magnetar-proto/src/conn.rs` — `poll_timeout` / `handle_timeout` sweep + the proto unit tests (`default_send_timeout_fires_when_receipt_lost`, `send_resolves_before_default_deadline_without_false_timeout`).
- `crates/magnetar/src/builders.rs` — `ProducerBuilder::send_timeout` / `disable_send_timeout`.
- `crates/magnetar-runtime-{tokio,moonpool}/tests/virtual_clock_driver_loop.rs` — per-engine firing tests (ADR-0024 layers b/c).
- `crates/magnetar-differential/tests/send_timeout_default_equivalence.rs` — tokio ↔ moonpool equivalence (ADR-0024 layer d).
- `crates/magnetar/tests/e2e_send_timeout.rs` — black-hole-gate end-to-end against a real broker (ADR-0024 layer e).
- `crates/magnetar-runtime-moonpool/tests/sim_chaos.rs` — the seed `0x4402f874c43758d1` fix (Part B).
- [ADR-0011](0011-clock-injection-sans-io.md) — the injected-clock contract the sweep fires against.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the five-layer test policy this change ships under.
