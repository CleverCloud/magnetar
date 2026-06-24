# ADR-0071 — Pluggable receiver-queue-size policy (PIP-74 auto-scaled queue)

- **Status**: Accepted
- **Date**: 2026-06-22
- **Decider**: Florentin Dubois
- **Tags**: consumer, flow-control, sans-io, determinism, parity
- **Numbering note**: a sibling change (issue #304, producer send-timeout drain) may also introduce an ADR around this number. If both land, renumber whichever merges second; this file is self-contained and carries no cross-ADR ordinal dependency.

## Context

Until issue #301 the consumer receiver queue size was a single immutable `usize` (`ConsumerState.receiver_queue_size`, default `1000`), set once at subscribe time and consumed at four flow-control sites: `initial_flow` (the first grant), `maybe_flow` (the half-drain refill, threshold `(size/2).max(1)`), and the two runtime safety-net flows in each engine's `subscribe_with`.
A fixed queue forces a static memory-vs-throughput trade-off: too small starves a fast consumer against a deep backlog; too large pins memory the consumer may never use.
The Apache Pulsar Java client solves this with PIP-74 `autoScaledReceiverQueueSizeEnabled` — a self-tuning queue that grows under starvation and is bounded by a memory budget — which magnetar's parity matrix listed as unsupported.

The constraint that shapes the design: the queue-sizing decision must run inside `magnetar-proto`, which is sans-io (ADR-0004) and must stay bit-for-bit reproducible under the moonpool deterministic-simulation engine (ADR-0011, ADR-0024).
A policy that read a clock or an RNG inside its decision function would diverge the production tokio engine from the moonpool engine and break differential parity.

Alternatives considered:

- **Keep the raw `usize`, add a separate `auto` boolean + hidden heuristic.** Rejected: not extensible (users cannot supply their own sizing strategy), and it scatters the heuristic across the four consumption sites instead of centralising it.
- **Put the policy trait in the façade (`magnetar`) crate.** Rejected: `ConsumerState` lives in `magnetar-proto` and must hold the policy to call it from the sans-io timeout tick; the façade is a downstream crate, so the trait must live in proto to avoid an inverted dependency.
- **Drive `adjust()` on a wall-clock timer inside the policy.** Rejected: that reintroduces an internal clock into proto (violates ADR-0011) and is non-deterministic under moonpool. The tick must ride the injected `now` of `handle_timeout`.

## Decision

Introduce a `ReceiverQueuePolicy` trait in `magnetar-proto` (`crates/magnetar-proto/src/receiver_queue.rs`) and make `ConsumerState.receiver_queue_size` the policy's _current_ target rather than a fixed user setting.

```rust
pub trait ReceiverQueuePolicy: Send + Sync + Debug {
    fn initial(&self) -> usize;                  // target at subscribe time
    fn adjust(&self, flow: &FlowStats) -> usize; // pure recompute from observed signals
}
```

- **Two built-in policies.** `Fixed(usize)` returns the wrapped size from both `initial()` and every `adjust()` — the historical behaviour, and the **default**, so an un-opted-in consumer is byte-for-byte identical to the pre-#301 client. `Auto { min, max_bytes }` grows the target (bounded doubling) while `available_permits == 0` (starvation) and the byte budget has room, and shrinks (gentle halving toward `min`) when the buffered-queue bytes reach the budget (OOM guard).
- **Purity contract.** `adjust` and `initial` MUST be pure functions of their inputs — no clock, no RNG, no I/O, no timing-dependent interior mutability. The two built-ins satisfy this; custom user policies are contractually required to as well. This is what keeps the tokio and moonpool engines bit-reproducible (ADR-0024).
- **`Arc<dyn>` + `Clone`.** The policy is carried as `Arc<dyn ReceiverQueuePolicy>` on `ConsumerState` and `SubscribeRequest`. `SubscribeRequest` derives `Clone`; `Arc` is `Clone`, so a reconnect subscribe replay clones the _handle_ to the same policy, never the policy object. The policy holds only immutable configuration; all mutable target state lives in `ConsumerState.receiver_queue_size`.
- **The adjust tick rides `handle_timeout(now)`.** `Connection::handle_timeout` already iterates every consumer slot under the per-slot lock (ADR-0038) for the nack/unacked/ack/chunk-expiry sweeps. The new adjust tick slots into that same CONSUMER-slot loop, with the same injected `now`. A grown target stages an incremental `CommandFlow` (the broker grant cannot be un-granted, so a shrink emits nothing and lets permits drain). `Connection::poll_timeout` surfaces the next adjust deadline (`ConsumerState::next_adjust_deadline`) so the driver wakes deterministically. The default `Fixed` policy disables auto-adjust (`adjust_interval = None`), so its tick is a no-op.
- **Lock ordering preserved.** `adjust_receiver_queue` runs entirely under the per-slot lock and never takes the connection-wide mutex (ADR-0038); the staged flow commands are emitted after the loop under `&mut self`.
- **`FlowStats` plumbing.** The signals the issue's `FlowStats` needs that did not previously exist: a running `queued_bytes` counter on `ConsumerState` (bumped on enqueue in `classify_and_queue`, decremented on dequeue in `pop_message`) supplies `in_flight_bytes`; `avg_message_bytes` is derived from `total_bytes_received / total_msgs_received`; `partitions` is supplied by the connection (always `1` at the per-partition proto level — each `ConsumerState` is a single partition), and the façade scopes `Auto.max_bytes` per-partition so the aggregate buffered bytes across all partitions stay within budget.
- **Builder sugar.** `receiver_queue_size(usize)` stays and resolves to `Fixed(usize)` (last-setter-wins with `receiver_queue_policy`); `receiver_queue_policy(Arc<dyn …>)` opts into a policy and turns on auto-adjust with a default 5-second tick (overridable via `receiver_queue_adjust_interval`). Threaded through `ConsumerBuilder`, `PartitionedConsumerBuilder`, `MultiTopicsConsumerBuilder`, `PatternConsumerBuilder`, and the shared `ConsumerTemplate` so partitioned / multi-topics / pattern consumers self-tune too.

## Consequences

- **Easier**: a consumer draining a deep backlog can opt into `Auto` and have the queue ramp under starvation without manual tuning, while the byte budget caps memory; the default path is unchanged, so existing code keeps its exact behaviour and wire bytes.
- **Integrates with #307**: the Failover active-consumer-change re-arm and the reconnect re-attach both route through `Connection::initial_flow` → `ConsumerState::initial_flow`, which grants the policy's CURRENT target — so a re-flow after promotion or reconnect grants the auto-tuned size, not a stale raw value.
- **Harder / bounded**: `Auto`'s ramp is intentionally conservative (bounded doubling, gentle decay, only on a clear starve/OOM signal) to avoid flow thrash; it is not an instantaneous optimum. The absolute per-consumer cap (`Auto::ABSOLUTE_MAX = 1_000_000`) guards the degenerate `avg_message_bytes == 0` case before any message is seen.
- **Determinism**: every decision is pure over `FlowStats` + the injected `now`; the 32-seed moonpool sweep and the differential flow-grant-trajectory test confirm the tokio and moonpool engines ramp identically.
- **Incompatible with**: a policy that reads a clock / RNG / I/O inside `adjust` (would break moonpool reproducibility) — closed off by the documented purity contract and enforced indirectly by `check-no-internal-clock` over the proto crate.

## References

- `crates/magnetar-proto/src/receiver_queue.rs` — the trait, `FlowStats`, `Fixed`, `Auto`, and their unit tests.
- `crates/magnetar-proto/src/consumer.rs` — `ConsumerState` policy field + `queued_bytes`, `flow_stats`, `adjust_receiver_queue`, `next_adjust_deadline`, `arm_adjust_clock`.
- `crates/magnetar-proto/src/conn.rs` — the adjust tick in `handle_timeout`'s consumer loop, the `next_adjust_deadline` surfacing in `poll_timeout`, and `consumer_receiver_queue_size`.
- `crates/magnetar/src/builders.rs`, `consumer_template.rs`, `partitioned_consumer.rs`, `multi_topics.rs`, `pattern_consumer.rs` — builder threading.
- ADR-0024 four/five-layer coverage: `crates/magnetar-proto/src/conn.rs` (`auto_policy_*`, `fixed_policy_default_never_adjusts`), `crates/magnetar-runtime-tokio/src/consumer.rs` + `crates/magnetar-runtime-moonpool/src/consumer.rs` (`auto_receiver_queue_policy_grows_target_under_starvation`, 1:1), `crates/magnetar-differential/tests/receiver_queue_policy_equivalence.rs`, `crates/magnetar/tests/e2e_receiver_queue_policy.rs`.
- [ADR-0004](0004-sans-io-protocol-core.md) — why the policy lives in proto and does no I/O.
- [ADR-0011](0011-clock-injection-sans-io.md) — the injected `now` the adjust tick rides.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the cross-runtime test + parity policy this change satisfies.
- [ADR-0038](0038-split-connection-mutex.md) — the per-slot lock the adjust tick runs under.
- Issue #301 — the feature request (PIP-74 parity, the proposed `ReceiverQueuePolicy` / `FlowStats` / `Fixed` / `Auto` shapes).
