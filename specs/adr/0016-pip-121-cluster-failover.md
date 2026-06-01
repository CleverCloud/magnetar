# ADR-0016 — PIP-121 cluster failover (Auto + Controlled)

- **Status**: Accepted
- **Date**: 2026-05-21
- **Decider**: Florentin Dubois
- **Tags**: pip-121, ha, failover, java-parity, reconnect

## Context

[PIP-121](https://github.com/apache/pulsar/wiki/PIP-121:-Pulsar-cluster-level-auto-failover-on-client-side) introduces client-side cluster failover.
Two policies in the Java client:

- **ControlledClusterFailover** — caller manually swaps the service URL (e.g. ops decide to drain a region).
- **AutoClusterFailover** — a background health-prober flips between primary and secondary URLs based on probe outcomes.

Both flow through a `ServiceUrlProvider` interface on `ClientBuilder` that returns the current URL on each reconnect.

The previous magnetar implementation had a hard-coded `String` `service_url` on `Client`.
Reconnect always used the same URL, so the parity matrix correctly listed PIP-121 as `❌`.

Constraints from prior ADRs:

- [ADR-0003 no-channels-rule](0003-no-channels-rule.md): no `tokio::sync::watch` to broadcast "current URL".
- [ADR-0004 sans-io-protocol-core](0004-sans-io-protocol-core.md): the failover prober runs in the engine, not in `magnetar-proto`.

## Decision

### Sans-io surface (`magnetar-proto`)

```rust
// service_url.rs
pub trait ServiceUrlProvider: Send + Sync + 'static {
    fn current(&self) -> String;
}

pub struct StaticServiceUrlProvider(String);

// cluster_failover.rs
pub struct ControlledClusterFailover {
    current: std::sync::Mutex<String>,
}

impl ControlledClusterFailover {
    pub fn new(initial: impl Into<String>) -> Self { /* … */ }
    pub fn switch(&self, new_url: impl Into<String>) { /* … */ }
}

impl ServiceUrlProvider for ControlledClusterFailover { /* … */ }
```

`std::sync::Mutex<String>` (not `parking_lot::Mutex`) — the `magnetar-proto` crate does not depend on `parking_lot` and never will (its dependency-allow-list is the tightest in the workspace).
The mutex is held only across a `.clone()` on a `String`, so std is fine.

### Engine surface (`magnetar-runtime-tokio`)

```rust
// auto_cluster_failover.rs
pub trait HealthProbe: Send + Sync + 'static {
    fn probe(&self, url: &str)
        -> Pin<Box<dyn Future<Output = bool> + Send + '_>>;
}

pub struct AutoClusterFailover {
    primary: String,
    secondary: String,
    probe: Arc<dyn HealthProbe>,
    current: Arc<std::sync::Mutex<String>>,
    /* check_interval, failover_delay, recovery_delay, etc. */
}

impl AutoClusterFailover {
    pub fn spawn(self, wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>);
}

impl ServiceUrlProvider for AutoClusterFailover { /* delegates to current */ }
```

The background prober is a regular `tokio::spawn`ed task that:

- probes the primary every `check_interval`
- on `failover_delay` consecutive failures → flips current to secondary
- when primary recovers for `recovery_delay` → flips back

### Driver plumbing

`ReconnectContext` (the per-reconnect carrier struct on `crates/magnetar-runtime-tokio/src/driver.rs`) gains a `service_url_provider: Arc<dyn ServiceUrlProvider>` field.
Each reconnect attempt calls `provider.current()` to get the URL — that's the sole behavioural change in the supervisor loop.

`ClientBuilder::service_url_provider(provider: Arc<dyn ServiceUrlProvider>)` plumbs the override.

## Consequences

- Manual failover is one method call: `failover.switch("pulsar+ssl://dr.cluster:6651")`.
- Auto failover is a spawned task; no channels, no `watch`.
- The supervisor loop has one indirection on the URL string; no new state machine in `magnetar-proto`.
- `magnetar-proto` gets _only_ the `ServiceUrlProvider` trait and the `ControlledClusterFailover` (which doesn't need any I/O).
  The `AutoClusterFailover` lives in the engine.
- Tests:
  - 4 unit tests on `ControlledClusterFailover` (manual swap semantics).
  - 3 unit tests on `AutoClusterFailover` (failover, recovery, threshold) using `tokio::test(start_paused = true)`.

## References

- `crates/magnetar-proto/src/service_url.rs` — trait + `StaticServiceUrlProvider`
- `crates/magnetar-proto/src/cluster_failover.rs` — `ControlledClusterFailover`
- `crates/magnetar-runtime-tokio/src/auto_cluster_failover.rs` — `AutoClusterFailover` + `HealthProbe`
- `crates/magnetar-runtime-tokio/src/driver.rs` — `ReconnectContext` carries the provider
- `crates/magnetar/src/client.rs` — `ClientBuilder::service_url_provider`
- Commit `87f2080` — "feat(client): ServiceUrlProvider trait + StaticServiceUrlProvider"
- Commit `7b8d3e6` — "feat(supervisor): plumb ServiceUrlProvider through the supervised reconnect path"
- Commit `c978288` — "feat(client): TLS hostname-only-skip + PIP-121 AutoClusterFailover + ControlledClusterFailover"
- Apache PIP-121
- [ADR-0003 no-channels-rule](0003-no-channels-rule.md)
- [ADR-0004 sans-io-protocol-core](0004-sans-io-protocol-core.md)
- [ADR-0018 pip-188-reconnect-on-migrate](0018-pip-188-reconnect-on-migrate.md) (shares the supervised reconnect path)
