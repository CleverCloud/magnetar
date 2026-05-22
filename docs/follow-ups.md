# Open Follow-Ups

Consolidated tracker for known open work. Each entry lists the gap, the
reason it stays open, and where the unblock lives.

For the public-facing parity status, see
[`parity-status.md`](parity-status.md) and the
[parity matrix in the README](../README.md#java-client-parity-matrix).

## Moonpool engine

### `AutoClusterFailover` (PIP-121, health-probe-driven)

**Status.** Tokio-only. The moonpool engine ships
`StaticServiceUrlProvider` and `ControlledClusterFailover` (both in
`magnetar-proto`); the auto variant lives in
[`crates/magnetar-runtime-tokio/src/auto_cluster_failover.rs`](../crates/magnetar-runtime-tokio/src/auto_cluster_failover.rs)
and spawns its own probe loop on `tokio::spawn`.

**Unblock.** Define a `HealthProbe` abstraction the moonpool runtime
can stub deterministically (no real DNS, no real TCP), plus a
`moonpool_core::TaskProvider`-driven probe loop with backoff. The
supervised reconnect path already pulls `service_url_provider` on
every attempt, so the trait wiring is in place.

See [ADR-0016](../specs/adr/0016-pip-121-cluster-failover.md).

### `MemoryLimitPolicy::ProducerBlock` on moonpool

**Status.** Tokio-only. The moonpool engine returns
`EngineError::MemoryLimitExceeded` synchronously on overflow
regardless of policy.

**Unblock.** The `Waker` slab fan-out is sans-io-clean
([ADR-0020](../specs/adr/0020-memory-limit-producer-block.md)), but
the drain-order determinism story under `moonpool_core::SimProviders`
is not yet specified. Either confirm that the slab drain order is
stable under sim, or document a moonpool-native equivalent of Java's
`MemoryLimitController` fairness contract.

See [`memory-limit.md`](memory-limit.md).

### Façade surface bound to `PulsarClient<MoonpoolEngine<P>>`

**Status.** Partitioned producer / partitioned consumer /
MultiTopicsConsumer / PatternConsumer / Reader / TableView /
transactions / typed schemas do not compile against the moonpool
engine.

**Unblock.** Lift each surface from
[`crates/magnetar/src/*`](../crates/magnetar/src) into an engine-agnostic
shape, or duplicate the façade for the moonpool engine. The constraint
is keeping the user-visible API identical between
`PulsarClient<TokioEngine>` and `PulsarClient<MoonpoolEngine<P>>`.

See [ADR-0019](../specs/adr/0019-engine-scope-and-moonpool-parity.md)
§Consequences.

### Property-based seed sweeps in CI

**Status.** The CI `moonpool-sim` job runs the chaos pack with a
single seed. Multi-seed scheduling is a manual loop today
(see [`testing.md`](testing.md)).

**Unblock.** Add a CI matrix axis on `MOONPOOL_SEED` (or move to
`proptest` so seed sweeping happens inside the test binary). Decide
the budget: ~32 seeds in PR mode, ~512 nightly.

### TLS handshake byte-level chaos

**Status.** Handshake correctness is verified end-to-end; adversarial
byte mutations (corrupted handshake records, partial-read sequencing)
are not yet swept.

**Unblock.** Extend the chaos pack with a fixture that drives
mid-handshake byte mutation via `moonpool_core::NetworkProvider`.

## Reconnect / supervision

### Transparent in-flight publish replay across reconnect

**Status.** The sans-io machinery is there
(`Connection::reset`, epoch bump,
`Connection::rebuild_producers` / `rebuild_consumers`). In-flight
publishes that the broker had not yet acked surface
`OpOutcome::SessionLost`; the caller must retry. Java's at-least-once
guarantee is **not** met on the publish side until the engine
re-queues the unconfirmed sends on the new session.

**Unblock.** Implement Stage 3 follow-up: on reset, snapshot the
in-flight publish slab keyed by `(producer_handle, sequence_id)`,
re-issue the sends with the original sequence ids on the new session,
and re-resolve the future when the new `CommandSendReceipt` arrives.
Broker-side dedup (sequence-id ordering) handles the
de-duplication.

Once this lands on tokio, the moonpool engine inherits it for free
(the supervised driver loop is shared logic).

## Differential equivalence harness

### Consumer-receive orphan-task wake path

**Status.** `broker_smoke` passes without any test-local kicker — the
production driver loop's `driver_waker.notify_waiters()` after every
`handle_bytes` is sufficient for the handshake + producer-open
round-trip. The `Kicker` in
[`crates/magnetar-differential/src/runner_tokio.rs`](../crates/magnetar-differential/src/runner_tokio.rs)
stays in for the longer `golden_traces` multi-op sequences (`Recv` with
2 s timeouts, seek replay, nack redelivery) which regress to ~30 s
wall-clock runs without the 25 ms pulse: `consumer.receive()` futures
observe a queued message only when the per-op `tokio::time::timeout`
re-polls, not at delivery time.

**Unblock.** Register the `Recv` future's waker against the consumer's
per-message waker slab so the sans-io layer wakes it directly on
delivery. Once that lands, both runners can drop the `Kicker` entirely.

### Expand the golden-trace catalog

**Status.** The harness ships four golden traces. They cover the basic
producer / consumer / seek / close shapes but not the seek-per-
partition flow, the transactional ack paths, or the
`cryptoFailureAction` matrix.

**Unblock.** Each new trace extends the scripted broker as needed (the
broker speaks a deliberately minimal subset of the wire protocol; new
opcodes get added per trace).

### Swap `TokioProviders` for `SimProviders` in the moonpool runner

**Status.** Both engine runners drive the same scripted broker; the
moonpool runner uses `moonpool_core::TokioProviders` because
`moonpool-sim` is not yet a workspace dependency. The harness already
exercises the engine surface that diverges between tokio and moonpool
(memory-limit plumbing, future shapes, generic bounds).

**Unblock.** Vendor `moonpool-sim` (one-line addition to the workspace
`Cargo.toml` allow-list, followed by an `Arc<SimProviders>` swap in
[`runner_moonpool.rs`](../crates/magnetar-differential/src/runner_moonpool.rs)).

## Auth

### SASL (Kerberos)

**Status.** `magnetar-auth-sasl` is scaffolded; the public API surface
is in place but the GSSAPI plumbing is pre-alpha.

**Unblock.** Full GSSAPI integration. Large scope — pulls in
`libgssapi` and the SASL token exchange wire format.

### Athenz

**Status.** `magnetar-auth-athenz` is scaffolded; pre-alpha.

**Unblock.** ZTS/ZMS token plumbing.

## Schemas

### `AutoProduceBytesSchema`

**Status.** Trait surface only. The consumer-side equivalent
(`AutoConsumeSchema`) is shipped end-to-end with broker-driven lookup.

**Unblock.** Implement producer-side schema-on-first-send. Lower
priority because `AutoConsumeSchema` covers the common Pulsar use
case; producers usually know their schema at construction time.

## Protocol

### Moonpool engine: lookup before producer/consumer open

**Status.** The tokio engine issues `CommandLookupTopic` before every
`open_producer` / `subscribe` so the broker activates the topic's
namespace bundle (Java parity). The moonpool engine still calls
`Connection::create_producer` / `Connection::subscribe` directly. This
is fine for deterministic-simulation tests that script the broker side
explicitly, but diverges from Java + tokio engine behaviour.

**Unblock.** Mirror
[`crates/magnetar-runtime-tokio/src/client.rs`](../crates/magnetar-runtime-tokio/src/client.rs)'s
`lookup_topic` step into the moonpool `Client::open_producer` /
`subscribe`. Tests under
[`crates/magnetar-runtime-moonpool/tests/`](../crates/magnetar-runtime-moonpool/tests/)
that drive the proto state machine synthetically will need to also
feed a synthetic `CommandLookupTopicResponse` (the moonpool engine
exposes `Client::lookup_topic` already; the change is wiring it into
the open paths).

### PIP-460 scalable topics, PIP-466 V5 surface, PIP-180 shadow topic, PIP-33 replicated subscriptions

**Status.** Out of scope for v0.1.0. PIP-466 is "inspired by, not
adopted verbatim" per
[ADR-0010](../specs/adr/0010-v0-1-full-java-parity.md).

**Unblock.** Scoped for v0.2.0. PIP-460 carries an experimental tag in
Apache Pulsar itself.
