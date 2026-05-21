# Moonpool M5b deferrals

Status anchor for the moonpool engine surfaces that were intentionally
**not** ported in M5b because they would either compromise determinism
under `moonpool-sim` or duplicate runtime-specific machinery that has no
clean moonpool analogue. Tracks blockers, not regressions — each entry
explains *why* it stays tokio-only today and what the unblock looks like.

## AutoClusterFailover (PIP-121 health-probe-driven failover)

**Tokio path (M4):** `magnetar_runtime_tokio::auto_cluster_failover` —
spawns a background tokio task that probes broker URLs on a schedule
(via `HealthProbe::probe()`), maintains a primary/secondary preference
table, and swaps the URL surfaced through `ServiceUrlProvider` based on
probe results. Pulled into the supervised reconnect path through the
existing `service_url_provider` hook.

**Why moonpool gets `ControlledClusterFailover` instead:** the auto
variant requires a tokio-scheduled probe loop with its own backoff +
TTL. Reproducing it in moonpool would need:

1. a `moonpool_core::TaskProvider::spawn_task` long-running probe loop,
2. a `HealthProbe` abstraction that the moonpool runtime can stub for
   deterministic simulation (no real DNS, no real TCP),
3. a way to express "probe failed N times in a row, swap primary" under
   `moonpool-sim`'s virtual clock — the existing tokio impl uses
   `tokio::time::interval` directly.

None of these are blockers in principle, but landing them properly
inside M5b would have pushed the surface count past 5, so the M5b plan
called the trade explicitly: ship `ControlledClusterFailover` (which is
sans-io and already lives in `magnetar-proto`) and defer the auto
variant.

**Usable today for moonpool:** wire a
`magnetar_proto::ControlledClusterFailover` via
`MoonpoolEngine::connect_plain_supervised(..., Some(provider), None)`.
Tests / control-plane sidecars can drive `set_url(...)` between
reconnects to exercise PIP-121 cluster failover.

**Unblock criteria:** when `moonpool-sim` lands a deterministic
`HealthProbe` analogue (or we agree on a simpler "polled probe" surface
that the moonpool runtime can drive via its task provider), port
`AutoClusterFailover` to moonpool. The supervised reconnect path
already pulls `service_url_provider` on every attempt, so the trait
wiring is in place.

## MemoryLimitPolicy::ProducerBlock

**Tokio path:** `magnetar_runtime_tokio::ConnectionShared` carries a
`memory_wakers: Mutex<Slab<Waker>>` slab + `try_reserve_memory_or_register`
helper. `MemoryReserveFut` polls the CAS, registers a waker on the slab
when overflow is detected, and re-polls after each `release_memory()`
drains the slab. Mirrors Java's `MemoryLimitController` ProducerBlock
mode.

**Moonpool today:** only `FailImmediately` — an overflow returns
`EngineError::MemoryLimitExceeded` synchronously. The tokio
slab+waker machinery is reachable in principle (parking_lot mutex +
Waker slab is no-channels-clean), but driving its wakeups
deterministically under `moonpool-sim` requires confirming that the
`Notify`-free waker fan-out doesn't reintroduce hidden non-determinism
via the order parked tasks see wake events. M5b kept the surface
minimal; ProducerBlock parity is a follow-up.

**Unblock criteria:** an explicit determinism story for the waker-slab
drain order under moonpool-sim, or a moonpool-native equivalent of the
fairness contract Java's `MemoryLimitController` expects. Until then,
the moonpool engine documents `FailImmediately` as the only supported
policy and surfaces an `EngineError::MemoryLimitExceeded` on overflow —
callers that need `ProducerBlock` semantics today must use the tokio
engine.

## In-flight publish replay across reconnect

**Tokio path:** Stage 3 of the supervisor work re-emits
`CommandProducer` / `CommandSubscribe` on reconnect via
`Connection::rebuild_producers` / `rebuild_consumers`. In-flight sends
that the broker had not yet acked still surface
`OpOutcome::SessionLost` — full at-least-once replay (resubmitting the
unconfirmed sends on the new session) is documented as Stage 3
follow-up.

**Moonpool today:** the supervised driver loop ported in M5b includes
the same Stage 3 rebuild flag — producer + consumer handles survive
the reconnect; in-flight publishes still surface SessionLost. Parity
with tokio.

**Unblock criteria:** lands once the tokio engine's Stage 3 follow-up
lands. The moonpool driver loop will pick up the new
`Connection::*` API change for free; no extra moonpool-specific work
expected.
