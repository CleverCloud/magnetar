# ADR-0028 — Supervised reconnect anti-thrash policy

- **Status**: Proposed
- **Date**: 2026-05-26
- **Decider**: Florentin Dubois
- **Tags**: supervisor, reconnect, ha, broker-cascade, follow-up-74

## Context

After the supervised reconnect path stabilised through 2026-05 (commits
`6da2e80`, `c1bc2c6`, `0e47e14`, `86398a8`, `f4872d7`), the magnetar
side of an `apachepulsar/pulsar:4.0.4` restart cycle is correct:

- `Connection::handle_command_error` classifies `MetadataError` (1) /
  `ServiceNotReady` (6) / `TopicNotFound` (11) as transient and
  retains the producer / consumer state.
- The driver runs `lookup_then(topic)` before
  `retry_producer_open` / `retry_consumer_subscribe`, mirroring
  Java's `ProducerImpl.connectionOpened` → `lookupRequest`.
- `Connection::in_flight_publish_snapshots` accumulates across
  multiple `reset()` cycles (no longer cleared on each cycle), so a
  user send queued during the transient window survives an
  arbitrary number of `rebuild_producers` rounds.
- Consumers replay `initial_flow` + `CommandRedeliverUnacknowledgedMessages`
  AFTER `SubscribeAcked` (Pulsar's `ServerCnx.handleFlow` silently
  drops `CommandFlow` for an unregistered consumer id, so the
  ack-then-flow ordering is mandatory).
- `Connection::is_user_closed()` gates the supervisor on the
  user-initiated `Closing` / `Closed` states only; a transport drop
  (`Failed`) falls into backoff / redial.

What still breaks is **broker-side**. After `docker restart` of the
testcontainers broker:

1. magnetar reconnects, runs `lookup_topic`, gets the new broker
   service URL.
2. magnetar sends `CommandProducer`; broker logs `"Created new
   producer …"` and acks.
3. ~10 ms later the broker drops the TCP connection.
4. Broker logs cycle: `"Subscribing on topic …"` → `"Cleared producer
   created after connection was closed"` → repeat, several
   iterations per second.
5. magnetar's per-handle retry path treats each drop as a transient
   error and re-attaches — feeding the cascade.

E2E impact: `e2e_supervised_reconnect_across_broker_restart`
(timeout) and `e2e_cluster_failover` (fail). All 19 other e2e files
(51 tests) pass.

The broker code paths most likely involved (in Pulsar 4.0.x):

- `org.apache.pulsar.broker.service.ServerCnx#handleProducer` —
  produces the `CommandProducerSuccess` ack on the executor thread;
  if the channel is closed between the ack write and `addProducer`
  finishing, `AbstractTopic#addProducer` runs the
  `"Cleared producer created after connection was closed"` log site
  (consistent with the observed log line).
- `org.apache.pulsar.broker.loadbalance.LoadManagerShared#unloadNamespaceBundlesGracefully`
  and `ModularLoadManagerImpl#doNamespaceBundleSplit` — bundle
  ownership churn during the broker's post-restart "warm-up" window
  while the load manager re-discovers ownership.
- `org.apache.pulsar.zookeeper.ZooKeeperSessionWatcher` — if the
  testcontainers `docker restart` races the ZooKeeper session
  timeout, the broker self-fences (`Watcher.Event.KeeperState.Expired`)
  and dispatches a connection close from the executor.

A fully conclusive diagnosis needs broker-side TRACE logging, which
is out of scope for this ADR — the policy proposed here is
intentionally agnostic about which broker path is to blame, because
the user-observable symptom is the same regardless of cause: rapid
"ack then TCP-drop" oscillation that magnetar's current retry loop
cannot escape.

Two prior ADRs frame the supervisor:

- [ADR-0018 PIP-188 reconnect-on-migrate](0018-pip-188-reconnect-on-migrate.md)
  — single `TopicMigrated` event escalates to a supervised reset.
- [ADR-0024 cross-runtime test + coverage policy](0024-cross-runtime-test-and-coverage-policy.md)
  — any behavioural change ships with all four test layers.

## Decision

Add an opt-in anti-thrash policy to `SupervisorConfig`. Default off
(preserves the current behaviour). Two knobs and one state machine.

### Knobs

```rust
// crates/magnetar-runtime-tokio/src/supervisor.rs
// + crates/magnetar-runtime-moonpool/src/supervisor.rs
pub struct SupervisorConfig {
    // ...existing fields...

    /// If `Some((n, window))`, observe per-handle re-attaches:
    /// if `n` successful re-attaches occur within `window` and each
    /// one is followed by a TCP drop within `drop_grace`, escalate
    /// to a connection-level cooldown. `None` disables anti-thrash.
    pub anti_thrash_threshold: Option<(u32, Duration)>,

    /// How long a successful re-attach has to "stick" before the
    /// anti-thrash window forgets it. Default: 500 ms.
    pub drop_grace: Duration,

    /// Cooldown applied on the connection-level redial loop after
    /// anti-thrash fires. Default: 30 s.
    pub max_backoff_after_thrash: Duration,
}
```

### State machine (per `Connection`)

A bounded ring of `(timestamp, handle, outcome)` events on
`Connection`. Outcomes:

- `ReAttachOk { handle }` — `CommandProducer` / `CommandSubscribe`
  acked on the new session.
- `TcpDropAfterReAttach { handle, elapsed_since_attach }` —
  driver loop observes the socket closing within `drop_grace` of a
  prior `ReAttachOk`.

On every `mark_disconnected(now, wall_now)` the supervisor inspects
the ring:

1. Count successful re-attaches in the trailing `window`.
2. If count ≥ `n` AND every one of those `n` was followed by a
   `TcpDropAfterReAttach`, raise a `ConnectionEvent::AntiThrashCooldown`.
3. The tokio + moonpool driver loops observe the event and sleep
   for `max_backoff_after_thrash` before the next `Transport::connect`
   attempt.
4. The ring is cleared on the next successful re-attach that
   survives `drop_grace`.

### Knob defaults

- `anti_thrash_threshold: None` — anti-thrash OFF by default;
  no behavioural change for existing callers.
- `drop_grace: 500ms` — generous enough to ride normal Pulsar load
  variance; tight enough to detect the observed ~10 ms cascade.
- `max_backoff_after_thrash: 30s` — chosen to outlast a typical
  broker bundle-rebalance window.

### Scope

Strictly the supervisor. Nothing in `magnetar-proto` changes other
than:

- Two new variants on `ConnectionEvent`: `AntiThrashCooldown` and
  `AntiThrashCleared`.
- A `Connection::record_reattach_outcome(now, handle, kind)` hook
  the runtime calls on each re-attach result and on each socket
  drop.

This keeps the policy testable from the sans-io layer and lets the
moonpool chaos broker drive the test (see Consequences).

## Consequences

**Easier**

- Users hitting a broker-side cascade (bundle churn, ZooKeeper
  session-timeout race, anti-affinity unload mid-create) can opt in
  and get a stable client that backs off rather than melting CPU.
- `e2e_supervised_reconnect_across_broker_restart` and
  `e2e_cluster_failover` become recoverable inside the existing
  test budget.

**Harder**

- New configuration surface. `SupervisorConfig` already has six
  fields; this adds three more. Documented under
  `crates/magnetar-runtime-tokio/src/supervisor.rs` and mirrored on
  the moonpool side.
- The anti-thrash state machine is itself a tracked invariant —
  per ADR-0024, every change ships with all four test layers + the
  moonpool 32-seed sweep.

**Cost**

- A bounded ring (size = `n * 2`, so default `8` slots if `n=4`)
  per `Connection`. `Vec<(Instant, ProducerHandle | ConsumerHandle,
  Outcome)>` with FIFO eviction — `O(n)` per insert, no allocations
  in the hot path.
- The user-facing observability: surface `AntiThrashCooldown` as a
  `tracing::warn!` and as a `Producer::last_disconnected_timestamp`
  bump so dashboards see it.

**Incompatibilities**

- None. Default is OFF.

### Test plan (per ADR-0024)

1. **Proto unit** (`crates/magnetar-proto/src/conn.rs`): the
   ring eviction + threshold trigger, with frozen `now`. Asserts
   `AntiThrashCooldown` event surfaces only when all `n` re-attaches
   in `window` are followed by `TcpDropAfterReAttach`.
2. **Tokio integration**
   (`crates/magnetar-runtime-tokio/tests/`): drives the supervisor
   against a fake transport that ack-then-drops on `n` consecutive
   re-attaches; asserts the connect cadence after cooldown matches
   `max_backoff_after_thrash`.
3. **Moonpool integration**
   (`crates/magnetar-runtime-moonpool/tests/`): mirror of (2)
   under deterministic `SimProviders`.
4. **Differential** (`crates/magnetar-differential/tests/`): a
   golden trace where the scripted broker ack-then-drops three
   times; tokio and moonpool both transition into cooldown with the
   same wall-clock offset (modulo engine jitter).
5. **Sim chaos**
   (`crates/magnetar-runtime-moonpool/tests/sim_chaos.rs`): extend
   the existing `BrokerWorkload` with a `DropAfterAttach { delay }`
   variant. 16-seed sweep (default) + 32-seed local sweep
   (`MOONPOOL_SEED=1..=32`) must converge to the same cooldown
   decision on every seed.
6. **E2E** (`crates/magnetar/tests/e2e_reconnect.rs`,
   `e2e_cluster_failover.rs`): enable the knob with
   `anti_thrash_threshold = Some((3, Duration::from_secs(2)))`;
   both tests must reach `Ok(())` within the existing test budget.

### Out of scope for this ADR

- Pulsar broker-side fix. Bundle-churn churn during `docker
  restart` may have a real broker bug behind it; that investigation
  is tracked separately under #74 and may produce upstream PRs.
- A more general circuit-breaker (failed-open / failed-closed /
  half-open). The policy here is the narrowest mitigation that
  matches the observed cascade pattern. A broader breaker would
  belong in a future ADR after we have field data on other
  cascade shapes.

## References

- `docs/follow-ups.md` §"#74 — Post-restart disconnect cascade
  (broker-driven)" — investigation summary + e2e impact
- `crates/magnetar-proto/src/conn.rs` —
  `Connection::in_flight_publish_snapshots`,
  `Connection::handle_command_error` (transient classification),
  `Connection::is_user_closed`
- `crates/magnetar-runtime-tokio/src/driver.rs` —
  `supervised_driver_loop`, transient-retry path
- `crates/magnetar-runtime-moonpool/src/driver.rs` — mirror
- `ARCHITECTURE.md#supervised-reconnect` — the full reconnect
  story this ADR amends
- [ADR-0010 v0.1.0 full Java parity](0010-v0-1-full-java-parity.md)
  — why this is a stability fix, not a feature
- [ADR-0018 PIP-188 reconnect-on-migrate](0018-pip-188-reconnect-on-migrate.md)
  — supervised-reset precedent
- [ADR-0024 cross-runtime test + coverage policy](0024-cross-runtime-test-and-coverage-policy.md)
  — binding test plan above
- [ADR-0026 D1–D4 design synthesis](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
  — pure-sim chaos suite hosting the new `DropAfterAttach`
  workload
- Apache Pulsar 4.0.x — `ServerCnx`, `AbstractTopic`,
  `LoadManagerShared`, `ZooKeeperSessionWatcher`
