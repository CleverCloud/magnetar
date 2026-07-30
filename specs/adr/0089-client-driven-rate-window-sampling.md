# ADR-0089 — Drive rolling-rate sampling from the sans-io deadline loop, not from a wrapper method

- **Status**: Accepted
- **Date**: 2026-07-30
- **Decider**: Florentin Dubois
- **Tags**: stats, sans-io, parity, moonpool, adr-0024

## Context

`ConsumerStats::msgs_per_sec` / `bytes_per_sec` and the matching `ProducerStats` pair are computed by `record_rate_window`, which needs two snapshots of the same slot to publish anything: the first call seeds a baseline, the second divides the counter delta by the elapsed time.

Nothing in either engine ever made the second call.
The production entry points were `magnetar_proto::Connection::{consumer,producer}_record_rate_window` and two tokio pass-throughs on `Producer` / `Consumer`; every caller in the tree was a test.
Sampling was therefore 100% caller-driven, and a client that never called it reported `0.0` forever.

The visible consequence was on the three aggregating wrappers.
`PartitionedProducer::aggregate_stats`, `MultiTopicsConsumer::aggregate_stats` (and its `PartitionedConsumer` alias) and `PatternConsumer::aggregate_stats` fold their children through `ProducerStats::fold` / `ConsumerStats::fold`, which sum the rates as plain `f64`.
Structurally correct since the #347 fold fix — and summing zeros for every wrapper type, because no child was ever ticked.
Only `PartitionedProducer` even exposed its children (`child_producers()`, and only on the tokio engine); the two consumer wrappers keep theirs behind a `Mutex<Arc<Vec<_>>>` with no public accessor, and the moonpool `Producer<P>` / `Consumer<P>` carry no `record_rate_window` method at all.
So on the moonpool engine the rates were unreachable at every layer.

`crates/magnetar/tests/e2e_aggregate_stats.rs` shipped with a header disclaimer deferring its rate assertion for exactly this reason.

### What the Java client does

The parity target drives this from the leaf, not the wrapper.

| Java site                                                                   | Behaviour                                                                                                                                                                  |
| --------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ProducerStatsRecorderImpl` / `ConsumerStatsRecorderImpl`                   | schedules `pulsarClient.timer().newTimeout(stat, statsIntervalSeconds, SECONDS)` and re-arms itself in the task's `finally` — the leaf self-ticks on the client-wide timer |
| `ClientConfigurationData.statsIntervalSeconds`                              | `60` by default (`ClientConfigurationData.java:144`); `0` disables the recorders entirely                                                                                  |
| `PartitionedProducerImpl.getStats()` / `MultiTopicsConsumerImpl.getStats()` | `stats.reset()` then fold each child's `getStats()` — **no timer of their own, and no per-child tick**                                                                     |

magnetar's `aggregate_stats()` is already the exact analogue of Java's wrapper `getStats()`.
What was missing is not a wrapper surface Java does not have — it is the leaf-level tick Java does have.

### The structural observation

`record_rate_window` was the only periodic obligation in the client not expressed as a deadline.
Keepalive, the nack / unacked / ack-grouping trackers, chunk expiry, receiver-queue auto-adjust, batch flush, send timeout, relocated in-flight sends and `ack_response_timeout` are all armed in `Connection::poll_timeout` and swept in `Connection::handle_timeout`.
That loop is magnetar's structural equivalent of Java's client-wide `HashedWheelTimer`.

## Decision

Drive the tick from the sans-io core as a deadline source, behind `ConnectionConfig::stats_interval: Option<Duration>`, and add **no wrapper API at all**.

- `Connection::poll_timeout` arms, per producer and consumer slot, a deadline at `last_rate_snapshot + stats_interval`.
- `Connection::handle_timeout` re-samples each due slot inside the per-slot loops it already runs, under the `slot.state.lock()` it already holds (ADR-0038 lock ordering is preserved — `record_rate_window` touches only the slot's own state and never reaches back for the connection-wide mutex).
- The baseline is each slot's existing `last_rate_snapshot` timestamp. No new state, no new task, no new `select!` arm, and no emitted frame or `ConnectionEvent` — so the golden `EventStream` traces are untouched.
- `ClientBuilder::stats_interval(Duration)` exposes it. `Duration::ZERO` disables, spelling Java's `statsIntervalSeconds = 0`; leaving the knob unset inherits the `ConnectionConfig` default.

Two details are load-bearing rather than incidental.

**`None` skips the deadline entirely.** Gated exactly like the `ack_response_timeout` arm: when the knob is `None`, `poll_timeout` computes no deadline and `handle_timeout` runs no sweep. An armed-but-never-firing deadline would still perturb the moonpool wake schedule, so a disabled sweep has to be invisible, not merely inert.

**The baseline is installed at slot creation, not on the first sweep.** `Connection::create_producer` / `subscribe` seed `last_rate_snapshot` to `(0, 0, last_activity)` when the knob is armed, mirroring Java, whose recorder is constructed with the producer/consumer and arms its first tick one interval later.
Seeding lazily instead — letting the first `handle_timeout` install the baseline — looks equivalent and is not: the only deadline a bare producer/consumer connection arms is keepalive, whose base (`last_activity`) is refreshed by every decoded frame (ADR-0058), so on a continuously busy connection it slides forward indefinitely and `handle_timeout` may not run for a very long time. A slot left unseeded would go unswept for exactly as long. A baseline is a fixed instant, so the deadline armed from it cannot slide.
A slot opened before the handshake response has no `last_activity` to anchor to; `handle_timeout` treats an unseeded slot as due, so that one case seeds on the first sweep instead of being stranded at `0.0`.

**The default ships as `None`.** Java's value is `Some(60 s)`, and that is the intended end state. Landing the mechanism with the sweep off keeps this commit a no-op for every existing caller and makes the default flip a one-line diff, so a moonpool seed regression bisects to it rather than to 500 lines of mechanism. The flip is gated on a clean 1..32 seed sweep.

### Why no wrapper fan-out method

A `record_rate_window`-style fan-out on `PartitionedProducer` / `MultiTopicsConsumer` / `PatternConsumer` was rejected. Java's wrappers have none, so it would be a magnetar-only divergence — and it would be a worse one than it looks:

- `fold` sums the rates as plain `f64` with no window metadata, so a caller who ticks three children of four, or ticks them at different cadences, gets an authoritative-looking total that is meaningless. One clock ticking every slot is what makes the sum well-defined.
- A ticker holding a clone of a child keeps that child's `Arc<ProducerCloseGuard>` alive, so the fire-and-forget `CommandCloseProducer` never fires and issue #241 regresses ([ADR-0057](0057-producer-last-clone-drop-close.md)).
- A ticker holding an `Arc<Inner>` pushes `MultiTopicsConsumer::close` into its "clones outlive us" branch, which returns `Ok(())` without closing a single child.
- `PartitionedProducer` is not `Clone` and owns its `Vec<P>` inline, so any `'static` handle to its children means restructuring a published type.

Every wrapper's children are slots on some connection, so driving the tick from the connection gives `PartitionedProducer`, `MultiTopicsConsumer`, `PatternConsumer` and any future aggregator correct rates without a line of new façade code.

### Rejected alternatives

| Option                                                                                     | Why not                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Tokio-only carve-out `impl` blocks on the wrappers                                         | [ADR-0037](0037-multi-topics-pattern-consumer-pass-2-lift.md) is the record of removing exactly that shape from these two types, and the moonpool engine would still gain nothing                                                                                                                                                                                                                                                                                        |
| A public child accessor on `MultiTopicsConsumer` only                                      | exports child clone semantics ([ADR-0057](0057-producer-last-clone-drop-close.md), [ADR-0077](0077-consumer-last-clone-drop-close.md)) to user code purely to work around a missing engine method, and it cannot mirror `child_producers()` anyway — the children sit behind a `Mutex<Arc<Vec<_>>>`, so the signature has to be a snapshot clone                                                                                                                         |
| An auto-ticker knob on the wrapper builders, shaped like `auto_update_partitions_interval` | determinism-hostile and currently unimplementable: `MoonpoolEngine::new_interval` is `tokio::time::interval` and `spawn` is `tokio::spawn`, but [ADR-0078](0078-adopt-moonpool-0-8-native-deterministic-runtime.md) runs `SimProviders` workloads on Moonpool's own deterministic executor with no ambient tokio runtime. `Engine::spawn` / `new_interval` are static with no providers argument, so fixing them is an `Engine`-trait signature change, not an impl swap |
| Read-triggered sampling — `aggregate_stats(now)` ticks then folds                          | two readers at different cadences destroy each other's window (each call re-seeds `last_rate_snapshot`), it makes a `&self` getter mutate observable state, and it breaks an existing published signature on two types                                                                                                                                                                                                                                                   |
| Lift `record_rate_window` onto `ProducerApi` / `ConsumerApi`                               | worth a follow-on commit but not a substitute: it is a breaking trait change, and it does not on its own make the wrappers correct — a caller still has to reach every child at one cadence. Its value is an explicit synchronous sample point for tests, which the opportunistic sweep cannot offer                                                                                                                                                                     |

## Consequences

**Easier.** Rolling rates now work the way Java's do: set one client-wide knob and every producer and consumer publishes a real rate, including per-partition and per-topic children no public API can reach. `aggregate_stats()` on all three wrappers folds real numbers instead of zeros.

**Harder.** Nothing. The default is `None`, so no existing behaviour changes until the follow-on flip.

**Cost.** One `ConnectionConfig` field, one `poll_timeout` arm, two `handle_timeout` statements inside existing loops, two small free functions (`rate_window_due`, `seed_rate_window_baseline`), one `ClientBuilder` setter.

**Determinism.** With the knob armed the sweep adds deadlines to the moonpool wake schedule; that is intended and reproducible, since the driver already passes a virtual `now`. With the knob at its `None` default the schedule is bit-for-bit unchanged, which the disabled-arm test on each engine pins.

**Known property, shared with Java.** A child added mid-window — `add_topic`, partition growth, a pattern-matched topic appearing — is seeded at its own creation, so it contributes `0.0` for its first full interval before publishing a rate. Java's recorders behave identically. `aggregate_stats` documents it.

**Interleaving.** `record_rate_window` remains public and callable. Calling it while the sweep is also running re-seeds the window, so the two cadences interfere; pick one. The doc comments on both methods say so.

**Residual.** The default is still `None`, so out of the box the rates remain zero. The follow-on commit flips it to `Some(60 s)` for Java parity after a clean 1..32 moonpool seed sweep.

## References

- `crates/magnetar-proto/src/conn_types.rs` — `ConnectionConfig::stats_interval` and its default; `stats_interval_config_tests`.
- `crates/magnetar-proto/src/conn.rs` — `rate_window_due`, `seed_rate_window_baseline`, the `poll_timeout` arm, the consumer and producer sweeps in `handle_timeout`; `conn_state_tests::stats_interval_*` (layer a).
- `crates/magnetar/src/client_builder.rs` — `ClientBuilder::stats_interval`.
- `crates/magnetar-runtime-tokio/tests/stats_interval_sweep.rs` (layer b) and `crates/magnetar-runtime-moonpool/tests/stats_interval_sweep.rs` (layer c) — 1:1 under `check-runtime-test-parity`.
- `crates/magnetar-differential/tests/stats_interval_sweep_equivalence.rs` (layer d) — cross-engine parity plus the absolute folded-rate assertions.
- `crates/magnetar/tests/e2e_aggregate_stats.rs` (layer e) — the partitioned wrappers folding a real rate against a live broker.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the four-layer test policy this ships against.
- [ADR-0011](0011-clock-injection-sans-io.md) — why the sweep takes an injected `now`.
- [ADR-0038](0038-split-connection-mutex.md) — the lock ordering the sweep sites preserve.
- [ADR-0058](0058-keepalive-watchdog-progress-based.md) — `last_activity` as the single keepalive-baseline refresh site, which is why lazy seeding is unsafe.
- [ADR-0086](0086-inject-now-into-proto-latency-recording.md) — the sibling fix that made latency recording clock-injected.
