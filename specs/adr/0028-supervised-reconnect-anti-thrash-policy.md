# ADR-0028 — Supervised-reconnect anti-thrash policy

- **Status**: Accepted
- **Date**: 2026-05-26
- **Decider**: Florentin Dubois
- **Tags**: supervisor, reconnect, ha, e2e, broker-quirks, follow-up-74

## Context

E2E investigation of follow-up #74 (post-restart disconnect cascade,
[`docs/follow-ups.md`](../../docs/follow-ups.md))
established that the magnetar-side supervised reconnect path is now
correct: transient `CommandError` retains state, `lookup_then(topic)`
re-acquires bundle ownership before `retry_producer_open` /
`retry_consumer_subscribe`, `Connection::in_flight_publish_snapshots`
accumulates across multiple `reset()` cycles (commit `0e47e14`),
consumer rebuild waits for `SubscribeAcked` before issuing
`CommandFlow` + `CommandRedeliverUnacknowledgedMessages` (commit
`f4872d7`), and producer rebuild calls
`ProducerState::replay_pending_outbound`. End-to-end, this lets
`e2e_supervised_reconnect_across_broker_restart` reach the rebuild
step.

What still fails is **broker-side**. After `docker restart` of
`apachepulsar/pulsar:4.0.4` (standalone, ZK + broker + bookie in one
JVM) the broker accepts the `CommandProducer`, ACKs it, then drops the
TCP connection within ~10 ms. Broker logs cycle several iterations per
second:

```
Subscribing on topic …
Cleared producer created after connection was closed
Subscribing on topic …
Cleared producer created after connection was closed
…
```

The two failing e2e suites are
`e2e_supervised_reconnect_across_broker_restart` (timeout) and
`e2e_cluster_failover` (fail). All 19 other e2e files (51 tests) pass.

### Broker-side root-cause investigation

Tracing the log surface and the producer lifecycle through the Apache
Pulsar `branch-4.0` source narrows the cascade to a well-documented
race in `ServerCnx.handleProducer()` ↔ `AbstractTopic.addProducer()` ↔
`BrokerService.getOrCreateTopic()`, aggravated in single-node
standalone mode by the pre-restart ZooKeeper ephemeral nodes outliving
the JVM:

- **(d) The log site itself.** The string `Cleared producer created
  after connection was closed` is emitted from the `producerFuture`
  completion handler inside `ServerCnx.handleProducer()`. The handler
  fires *after* `Topic.addProducer()` resolves on the topic executor.
  At that point it checks whether the channel is still active; if not,
  it tears down the just-installed producer and logs this line. The
  same pattern is documented in `channelInactive` (`ServerCnx.java`
  ~L470, where the per-producerFuture cleanup lambda calls
  `closeNow(true)` on any future that resolved successfully after the
  channel was already inactive — see Apache Pulsar PR
  [#13428](https://github.com/apache/pulsar/pull/13428)).
- **(b) The driver of the race — dangling `producerFuture` from the
  prior session epoch.** `ServerCnx` keys its producer cache by raw
  `producerId` and tracks completion via a per-id `CompletableFuture`.
  When `BrokerService.checkTopicNsOwnership` throws (because the
  freshly-restarted broker has not yet reclaimed the bundle), the
  exception sometimes fails to propagate cleanly out of
  `getOrCreateTopic`, leaving the future un-completed (see Apache
  Pulsar issue [#6416](https://github.com/apache/pulsar/issues/6416),
  [#9792](https://github.com/apache/pulsar/issues/9792), and the
  remediation in PR [#14467](https://github.com/apache/pulsar/pull/14467)
  — "Fix producerFuture not completed in ServerCnx#handleProducer").
  The client then reconnects, re-issues `CommandProducer`, the future
  finally resolves, but the original `ctx.channel()` is gone — broker
  logs the "Cleared producer" line and the cascade restarts.
- **(c) The standalone-mode ZK session race.**
  `apachepulsar/pulsar:4.0.4` standalone collocates ZooKeeper, broker
  and bookie in one JVM. A `docker restart` SIGTERMs the JVM, but the
  pre-restart broker's ZK ephemeral nodes (bundle ownership markers
  under `/namespace/.../0x.../data`) remain registered against the
  *previous* ZK session until `tickTime × syncLimit` elapses
  (~30 s with defaults). The new JVM's broker therefore observes its
  own pre-restart instance as still owning the bundles, refuses
  ownership, and the load manager iterates trying to reacquire — each
  attempt creating a window where `getOrCreateTopic` resolves but the
  client's TCP session has already been torn down by the prior
  rejection. This matches Apache Pulsar issue
  [#3566](https://github.com/apache/pulsar/issues/3566) and the
  Clever Cloud production note in Slack
  ([archives/C9D4X6TL1/p1777460888080179](https://clevercloud.slack.com/archives/C9D4X6TL1/p1777460888080179?thread_ts=1777452698.077029)):
  > "Pulsar […] galère quand tu coupe des consumers/producer
  > brutalement, ta lease ZK n'a pas encore expiré. du coup quand tu
  > reboot, il te dis qu'il est déjà connecté."
- **(a) `LoadManagerShared.shouldAntiAffinityNamespaceUnload`** is
  *not* the trigger here. Anti-affinity unload only fires when the
  load manager sees a healthier broker in the same anti-affinity
  group; in single-node standalone there are no peers. Ruled out for
  this scenario.

### Why magnetar must mitigate even after the broker stabilises

Even with the well-known fixes landed upstream
([PR #14467](https://github.com/apache/pulsar/pull/14467),
[PR #13428](https://github.com/apache/pulsar/pull/13428),
[PR #12846](https://github.com/apache/pulsar/pull/12846)), the
re-attach cascade remains observable on `4.0.4`, on any standalone
restart, and on any cluster-level rolling restart where the
ZK-session-vs-broker-startup race window opens. Magnetar's current
supervisor retries each handle as fast as `Backoff::next_delay`
allows; the retries themselves stress the broker further (each one
allocates a `Topic` future on the broker executor before the channel
is closed), keeping the broker in the bundle-acquire churn.

Magnetar's contract per
[ADR-0010](0010-v0-1-full-java-parity.md) is Java-client parity —
the Java client backs off the *connection* (not just the handle) once
it observes repeated successful-create-then-dropped sessions
(`PulsarClientImpl.tryReconnect` → `ClientCnx.handleCloseProducer` →
`Backoff.next`). Magnetar today only backs off per handle. The gap is
the missing connection-scoped cooldown.

## Decision

Add an **opt-in anti-thrash policy** on `SupervisorConfig` that escalates
from per-handle retry to a connection-level cooldown when the broker
exhibits the create-then-drop pattern. Default **OFF** to preserve
current behaviour; users opt in by setting the threshold knobs.

### API additions (no code in this commit)

In `crates/magnetar-runtime-tokio/src/supervisor.rs` (and the moonpool
counterpart per [ADR-0019](0019-engine-scope-and-moonpool-parity.md)
parity train):

```rust
pub struct SupervisorConfig {
    // ... existing fields ...

    /// Anti-thrash detector window.
    ///
    /// `None` → disabled (default; current per-handle retry behavior).
    /// `Some(threshold)` → if `threshold.successful_attaches` re-attaches
    /// succeed in `threshold.window` wall-clock and each is followed by
    /// a TCP-level drop within `threshold.drop_within`, escalate from
    /// per-handle retry to connection-level cooldown.
    pub anti_thrash_threshold: Option<AntiThrashThreshold>,

    /// Connection-level cooldown applied once `anti_thrash_threshold`
    /// trips. Stacks above the per-handle backoff; the supervisor
    /// re-redials only after this delay even if individual handles
    /// would have retried sooner. Default `Duration::from_secs(30)`.
    pub max_backoff_after_thrash: Duration,
}

pub struct AntiThrashThreshold {
    pub successful_attaches: u32,   // N
    pub window: Duration,           // M
    pub drop_within: Duration,      // K
}
```

### Detector mechanics

- The detector lives **per `Connection`** (not per handle) inside
  `magnetar-proto::Connection`'s supervisor-observed state, behind a
  small `AntiThrashState { ring: VecDeque<AttachOutcome>, … }`. No
  channels (per [ADR-0003](0003-no-channels-rule.md)); state is read
  under the existing `parking_lot::Mutex<ConnectionShared>`.
- On every successful `CommandProducerSuccess` /
  `CommandSubscribeAcked` the supervisor records
  `(now, handle_id)` in the ring.
- On every subsequent TCP-level transport drop (i.e. `EngineEvent::
  TransportClosed` arriving before the corresponding handle's first
  user-driven op resolves), the supervisor records the elapsed delta.
- If `successful_attaches` entries in the ring all have
  `transport_drop_delta ≤ drop_within` and the ring window
  `≤ M`, the connection enters **`Cooldown(max_backoff_after_thrash)`**.
- In `Cooldown`, the driver loop pauses redial. Handles see the same
  user-visible state as a normal supervised reconnect; the only
  difference is the floor on retry latency.
- The detector resets on any successful attach + first-op-success
  pair (proves the broker has stabilised).

### Defaults and migration

- `anti_thrash_threshold: None`, `max_backoff_after_thrash:
  Duration::from_secs(30)` ship as the new defaults. Existing user
  configs compile unchanged.
- Recommended starting values when opting in (documented in
  rustdoc + `docs/architecture-overview.md` supervisor section):
  `successful_attaches = 5`, `window = Duration::from_secs(2)`,
  `drop_within = Duration::from_millis(50)`.

## Consequences

**Makes easier.**
- `e2e_supervised_reconnect_across_broker_restart` and
  `e2e_cluster_failover` pass under simulated broker churn once the
  detector is enabled — the test harness sets a tight threshold and
  asserts the cooldown engages within the test budget.
- Java-client behavioural parity for the connection-level backoff
  arm; documented in the parity matrix.
- A broker restart under load stops amplifying its own churn from the
  magnetar side. Fewer half-open producer futures land on
  `ServerCnx.producers`, easing the upstream race window itself.

**Makes harder / costs.**
- Adds one config knob to the supervisor surface. Default-OFF means
  no behaviour change for users who don't opt in, but the surface area
  grows.
- Adds `AntiThrashState` to `Connection`. Sized to a small ring
  (default capacity = `successful_attaches × 2`); ~hundreds of bytes
  per connection, no allocation on the hot path.
- Detector semantics are subtle — operators who tune
  `drop_within` too low will miss the cascade; too high and they pay
  a 30 s reconnect floor on any healthy short-lived attach.
- Documentation debt: requires a new section in
  `docs/architecture-overview.md` and a parity-matrix row update.
  Both land in the same changeset as the implementation, per the
  [`docs are code`](../../CLAUDE.md#principles) principle.

**Incompatibilities.**
- None. The knob is additive; supervisors that don't set it observe
  current behaviour exactly. The detector is connection-scoped, so
  no interaction with the PIP-188 `TopicMigrated` path
  ([ADR-0018](0018-pip-188-reconnect-on-migrate.md)) — a migration
  *is* a deliberate broker-side drop after a successful attach, but
  the new URL bypasses the cooldown because the connection identity
  changes.

## Test plan

Per [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md)
cross-runtime test + coverage policy, the implementation changeset
ships **all four layers** plus an e2e:

1. **`magnetar-proto` unit test.** Feed the supervisor a synthetic
   ring of `(now, attach_ok, transport_drop_delta)` triples and
   assert `AntiThrashState::tick` enters `Cooldown` exactly when the
   threshold conditions are met, and exits on a successful
   first-op-after-attach.
2. **`magnetar-runtime-tokio` integration test** under
   `crates/magnetar-runtime-tokio/tests/anti_thrash.rs`. Drives a
   `magnetar-fakes` broker variant that ACKs `CommandProducer` then
   `RST`s the TCP socket within 10 ms; asserts the supervisor
   transitions to `Cooldown` and that the cooldown latency floor is
   honoured.
3. **`magnetar-runtime-moonpool` integration test** under
   `crates/magnetar-runtime-moonpool/tests/anti_thrash.rs`. Extends the
   chaos-pack `BrokerWorkload` introduced in
   [ADR-0026](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
   §D2 (commit `aaa0661`) with a `DropsTcpAfterCreate { delay_ms }`
   variant. Asserts cooldown engages and that the deterministic seed
   sweep (1..=32) reproduces the same engagement count.
4. **`magnetar-differential` equivalence test** asserting
   tokio ↔ moonpool `EventStream` parity for the
   attach-then-drop sequence, confirming the user-visible event
   ordering (`Connected → ProducerSuccess → TransportClosed →
   ReconnectScheduled { in: ≥ max_backoff_after_thrash } → Connected →
   ProducerSuccess`).
5. **Docker e2e.** Re-runs `e2e_supervised_reconnect_across_broker_restart`
   and `e2e_cluster_failover` with the supervisor configured at
   `(N=5, M=2s, K=50ms, cooldown=30s)`; both must reach `Ok(())`
   within the existing test budget.

Sim coverage on the diff must remain 100 % (`cargo xtask
check-sim-coverage`) and the tokio↔moonpool 1:1 count must hold
(`cargo xtask check-runtime-test-parity`).

## References

- [`docs/follow-ups.md`](../../docs/follow-ups.md)
  — failure description + unblock plan.
- [`ADR-0010 full Java parity`](0010-v0-1-full-java-parity.md)
  — connection-level backoff is part of the parity contract.
- [`ADR-0018 PIP-188 reconnect on migrate`](0018-pip-188-reconnect-on-migrate.md)
  — the supervised-reset primitive this builds on; explicit
  non-interaction with `TopicMigrated`.
- [`ADR-0024 cross-runtime test + coverage policy`](0024-cross-runtime-test-and-coverage-policy.md)
  — four-layer test set + diff coverage gate.
- [`ADR-0026 D1–D4 design decisions`](0026-design-decisions-d1-d4-from-fdb-pulsar-codex-review.md)
  — chaos-pack `BrokerWorkload` foundation (D2).
- Apache Pulsar issue [#6416](https://github.com/apache/pulsar/issues/6416)
  — dangling `producerFuture` after bundle unload.
- Apache Pulsar issue [#9792](https://github.com/apache/pulsar/issues/9792)
  — "Producer cannot connect after broker load shedding".
- Apache Pulsar issue [#3566](https://github.com/apache/pulsar/issues/3566)
  — ZK session race triggers broker shutdown / fence.
- Apache Pulsar PR [#14467](https://github.com/apache/pulsar/pull/14467)
  — fix for `producerFuture` not completed in
  `ServerCnx#handleProducer`.
- Apache Pulsar PR [#13428](https://github.com/apache/pulsar/pull/13428)
  — race conditions in closing producers and consumers
  (`channelInactive` cleanup, completion lambda).
- Apache Pulsar PR [#12846](https://github.com/apache/pulsar/pull/12846)
  — producer incorrectly removed from topic's producer map after
  unload race.
- `crates/magnetar-runtime-tokio/src/supervisor.rs` — supervisor
  surface this extends.
- `crates/magnetar-proto/src/conn.rs` — `Connection` state hosting
  `AntiThrashState`.
