# ADR-0022 — Extract sans-io `HealthProbe` trait into `magnetar-proto`

- **Status**: Accepted
- **Date**: 2026-05-22
- **Decider**: Florentin Dubois
- **Tags**: pip-121, ha, failover, sans-io, java-parity

## Context

[ADR-0016](0016-pip-121-cluster-failover.md) landed PIP-121 cluster
failover with the policy machinery split across two crates:

- `magnetar-proto::ServiceUrlProvider` + `ControlledClusterFailover` —
  the sans-io provider trait and the manual-swap policy.
- `magnetar-runtime-tokio::auto_cluster_failover::{AutoClusterFailover,
  HealthProbe}` — the auto-failover policy and its probe callback.

The probe trait was placed in the tokio engine crate for a pragmatic
reason: its previous shape returned a `Pin<Box<dyn Future<Output = bool>>>`,
which presupposes an async runtime to execute. Putting that in
`magnetar-proto` would either pull `futures` into the proto crate's
dependency surface or force every implementor to box a future against
an unknown executor — neither acceptable under
[ADR-0004](0004-sans-io-protocol-core.md).

That tokio-only placement has costs:

- The moonpool engine cannot host its own `AutoClusterFailover`
  implementation without re-declaring a near-identical trait, breaking
  the "one probe contract, multiple engines" mental model.
- Differential testing (`magnetar-differential`) cannot drive auto
  failover deterministically — every probe today is intrinsically
  async.
- Engines outside the workspace (a future glommio engine,
  third-party experiments) have nowhere to hang their probe.

`quinn-proto` solved exactly this shape problem in its driver loop with
the `poll_*` family: a trait method returning `Poll<T>` registers a
`Waker` while pending and is otherwise runtime-agnostic. That pattern
is already in use across `magnetar-proto` (`Connection::poll_event`,
`Connection::poll_timeout`, the consumer/producer waker slabs). Lifting
`HealthProbe` to the same idiom unblocks the moonpool implementation
without dragging tokio into the proto crate.

## Decision

### Sans-io trait in `magnetar-proto`

```rust
// crates/magnetar-proto/src/health_probe.rs
pub trait HealthProbe: Send + Sync + std::fmt::Debug {
    fn poll_probe(
        &self,
        endpoint: &str,
        deadline: std::time::Instant,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<bool>;
}
```

Re-exported from `magnetar-proto::lib` so callers can spell it as
`magnetar_proto::HealthProbe`. The trait pulls in no new crate
dependency — `std::task::{Context, Poll}` is already used inside
`magnetar-proto` (consumer/producer/txn waker slabs, see ADR-0003).

### Engine surface (`magnetar-runtime-tokio`)

`AutoClusterFailover` now stores `probe: Arc<dyn
magnetar_proto::HealthProbe>` and adapts the `poll_probe` contract into
its async background loop via `std::future::poll_fn`. The shape mirrors
how tokio adapters bridge its own async I/O onto a `poll_read` /
`poll_write` substrate.

```rust
// crates/magnetar-runtime-tokio/src/auto_cluster_failover.rs
let healthy = poll_fn(|cx| probe.poll_probe(url, deadline, cx)).await;
```

The crate ships `TokioHealthProbe` as the stock implementation:

- Parses `pulsar://host:port`, `pulsar+ssl://host:port`, and bare
  `host:port` strings.
- Spawns a tokio task per endpoint that runs
  `tokio::net::lookup_host` + `tokio::net::TcpStream::connect` wrapped
  in `tokio::time::timeout_at(deadline, ...)`.
- Tracks in-flight handles in an `Arc<Mutex<HashMap<String,
  JoinHandle<bool>>>>` so concurrent `poll_probe` calls against the
  same endpoint coalesce; `Pin::new(handle).poll(cx)` polls the
  handle to completion.

### Backwards-compat shim

`AutoClusterFailover::new(urls, probe)` keeps its previous signature
(only the meaning of `Arc<dyn HealthProbe>` changes from "tokio-only
boxed-future trait" to "proto sans-io trait"). A `new_with_probe(urls,
probe)` constructor is added as a clearer-named alias for the new
mental model. Existing in-tree call sites (tests) updated in the same
commit.

The old `HealthProbe` and `HealthProbeFuture` exports from
`magnetar-runtime-tokio::auto_cluster_failover` are removed. Callers
that hand-rolled an implementation against the tokio-only async-future
trait must migrate to `magnetar_proto::HealthProbe`. The migration is
mechanical: replace the `probe<'a>(&'a self, url) ->
HealthProbeFuture<'a>` body returning `Box::pin(async { ... })` with a
`poll_probe(&self, endpoint, deadline, cx) -> Poll<bool>` body that
parks `cx.waker()` while pending.

### Moonpool — explicit follow-up

A moonpool-flavoured `MoonpoolHealthProbe` is not part of this change.
The trait lives where moonpool can implement it; the actual moonpool
engine impl is a follow-up tracked alongside the rest of the moonpool
parity train (see [ADR-0019](0019-engine-scope-and-moonpool-parity.md)).

## Consequences

- Moonpool can now host an `AutoClusterFailover` peer without dragging
  tokio into `magnetar-proto`; the trait is the contract, the engine
  is the implementation.
- Differential testing gains a path to deterministic auto-failover
  scenarios: the moonpool probe will be driven by the simulator's
  virtual clock and synthetic verdicts.
- The trait's `Poll`-shaped surface is more verbose for implementors
  than the previous `async fn`-via-boxed-future style. Tokio
  implementors pay a small bridging cost (`poll_fn` adapter,
  `JoinHandle` slab) compared to the previous direct-`await`. In
  exchange the engine boundary is honest about who owns the executor.
- `xtask check-no-io-deps` keeps passing — the new module imports only
  `core::fmt`, `std::task`, and `std::time`. No new dependencies.
- Existing third-party implementors of the tokio-only `HealthProbe`
  trait must migrate. The migration is one mechanical rewrite per
  impl, documented in this ADR.

## References

- `crates/magnetar-proto/src/health_probe.rs` — sans-io trait + unit tests
- `crates/magnetar-proto/src/lib.rs` — `pub use crate::health_probe::HealthProbe;`
- `crates/magnetar-runtime-tokio/src/auto_cluster_failover.rs` —
  refactored policy + new `TokioHealthProbe`
- [ADR-0003 no-channels](0003-no-channels-rule.md) — justifies the
  `Waker`-based completion plumbing inside the trait
- [ADR-0004 sans-io-protocol-core](0004-sans-io-protocol-core.md) —
  the rule the previous trait placement was forced to bend around
- [ADR-0016 PIP-121 cluster failover](0016-pip-121-cluster-failover.md) —
  the policy this trait serves
- [ADR-0019 engine scope](0019-engine-scope-and-moonpool-parity.md) —
  defers the moonpool implementation
- Apache PIP-121
