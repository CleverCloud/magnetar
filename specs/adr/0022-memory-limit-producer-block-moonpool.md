# ADR-0022 — `MemoryLimitPolicy::ProducerBlock` on the moonpool engine

- **Status**: Accepted
- **Date**: 2026-05-22
- **Decider**: Florentin Dubois
- **Tags**: memory-limit, back-pressure, moonpool, sans-io, no-channels, java-parity

## Context

[ADR-0020](0020-memory-limit-producer-block.md) shipped
`MemoryLimitPolicy::ProducerBlock` on the tokio engine via a
`parking_lot::Mutex<Slab<Waker>>` on `ConnectionShared`. The moonpool
engine intentionally deferred the same surface — the original
follow-up flagged the drain-order determinism story under
`moonpool_core::SimProviders` as the open question.

The Java parity matrix tracks `MemoryLimitPolicy::ProducerBlock` per
engine (see [`docs/parity-status.md`](../../docs/parity-status.md));
shipping the moonpool half closes the last back-pressure gap between
the engines.

Constraints inherited from the workspace ADRs:

- [ADR-0003 — no-channels](0003-no-channels-rule.md). No
  `tokio::sync::{mpsc, broadcast, watch, oneshot}`, no
  `crossbeam-channel`, no `flume`. The signal "budget is now
  available" travels through a `Slab<Waker>` behind a
  `parking_lot::Mutex`, identical to the tokio engine.
- [ADR-0004 — sans-io](0004-sans-io-protocol-core.md). The reservation
  primitive (`AtomicU64::compare_exchange`) lives at the proto layer
  unchanged; the back-pressure machinery is engine-local.
- [ADR-0011 — clock injection](0011-clock-injection-sans-io.md). The
  ProducerBlock wait is event-driven; nothing in this ADR reaches for
  `Instant::now()` or `SystemTime::now()` outside the engine's existing
  allowlisted sites.

## Decision

Add the same `memory_limit_policy` field and `memory_wakers:
Mutex<Slab<Waker>>` to `magnetar_runtime_moonpool::ConnectionShared`
that the tokio engine already carries. The helper methods
(`try_reserve_memory_or_register`, `cancel_memory_waker`,
`drain_memory_wakers`) and the `SendFut::poll` retry path are
verbatim ports of the tokio implementation — both engines park on the
same shape of slab and resolve wakes through `Waker::wake()`.

Diverging from a verbatim port would introduce a behavioural delta
between the two engines, which is exactly what the
`magnetar-differential` harness exists to prevent.

### Fairness contract under `moonpool_core::Providers`

`Slab::drain()` visits slots in insertion order (the slab's free-list
is FIFO across removals, so an insert that follows a drained slot
fills the lowest free slot first). `release_memory` therefore wakes
parked sends in the order they registered.

`Waker::wake()` then hands the woken task off to the wrapping
`Providers::task` runtime:

- Under `TokioProviders`, this is the live tokio scheduler. Same
  semantics as the tokio engine.
- Under `SimProviders` (`moonpool-sim`), the simulator scheduler
  resumes tasks per its own policy (typically FIFO inside a tick,
  but the simulator is free to reorder for fault-injection sweeps).

**Test contract.** Tests against the ProducerBlock path must depend on
**eventual** progress: every parked send eventually observes either a
successful reservation or its own cancellation. Tests must NOT depend
on a specific drain-then-resume order across multiple parked
producers — that ordering is the simulator's call.

This matches the Java client: `MemoryLimitController` documents
fairness as "best-effort first-come-first-served" rather than a
strict ordering guarantee.

### Public surface

No new public types. `magnetar_proto::MemoryLimitPolicy` already
carries the `ProducerBlock` variant; the moonpool runtime now reads
the policy off `ConnectionConfig` and honours it. Callers wiring
`ClientBuilder::memory_limit(bytes, MemoryLimitPolicy::ProducerBlock)`
against `PulsarClient<MoonpoolEngine<P>>` get the parked-future
behaviour automatically.

## Consequences

**Positive**

- Closes the last engine-parity gap on `memory_limit` per
  [ADR-0019](0019-engine-scope-and-moonpool-parity.md).
- Differential harness (`magnetar-differential`) can now exercise the
  back-pressure path uniformly across both engines.
- The implementation is a one-to-one mirror of the tokio half so the
  cognitive cost is bounded — debugging one engine surfaces issues
  in the other.

**Negative**

- The slab grows under live contention but never shrinks below its
  high-water mark. `Slab` uses a free-list, so this is a memory-only
  cost; no per-iteration allocation. Identical to the tokio engine.
- Under heavy contention against a tiny budget, a release can wake
  every parked sender even though only one will win the next CAS.
  Acceptable trade-off (matches Java and the tokio engine); follow
  up only if profiling shows hot-loop thrash.

**Neutral**

- This ADR **extends** [ADR-0020](0020-memory-limit-producer-block.md);
  it does not supersede it. The tokio engine's documented behaviour
  is unchanged. Both ADRs apply.

## References

- [ADR-0003 — no-channels rule](0003-no-channels-rule.md).
- [ADR-0017 — memory_limit atomic CAS reservation](0017-memory-limit-atomic-reservation.md).
- [ADR-0019 — engine scope and moonpool parity](0019-engine-scope-and-moonpool-parity.md).
- [ADR-0020 — `MemoryLimitPolicy::ProducerBlock` back-pressure via Waker slab](0020-memory-limit-producer-block.md)
  (this ADR extends ADR-0020's mechanism to the moonpool engine).
- `crates/magnetar-runtime-moonpool/src/lib.rs` — `ConnectionShared`
  with the `memory_limit_policy` field, the `memory_wakers` slab, and
  the `try_reserve_memory_or_register` /
  `cancel_memory_waker` / `drain_memory_wakers` helpers.
- `crates/magnetar-runtime-moonpool/src/producer.rs` —
  `SendState::Reserving` variant + the `Producer::send` decision tree
  + the `SendFut::poll` retry path. Tests in the same file pin both
  policies.
- `docs/memory-limit.md` — public-facing surface and reservation
  semantics (engine-agnostic).
- `docs/parity-status.md` — engine-by-engine parity matrix.
