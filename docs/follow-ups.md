# Open Follow-Ups

Consolidated tracker for known open work. Each entry lists the gap, the
reason it stays open, and where the unblock lives.

For the public-facing parity status, see
[`parity-status.md`](parity-status.md) and the
[parity matrix in the README](../README.md#java-client-parity-matrix).

## Moonpool engine

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

### Moonpool runner LocalSet pump

**Status.** The consumer-receive orphan-task wake path is closed at the
sans-io layer:
[`magnetar_proto::consumer::ConsumerState`](../crates/magnetar-proto/src/consumer.rs)
exposes a per-consumer `Slab<Waker>` populated by
`register_consumer_receive_waker` / drained by `wake_receivers` on every
delivery, close, and end-of-topic. Both the tokio and moonpool runtime
`Consumer::receive()` futures register their `cx.waker()` into that slab
on first poll and evict it on `Drop`. The tokio differential runner's
`Kicker` is gone — `golden_traces` runs sub-millisecond on the tokio
engine.

What remains is structural to the differential moonpool runner: its
driver task is `spawn_local`'d into a
[`tokio::task::LocalSet`](https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html)
because [`moonpool_core::TokioProviders`]'s `TaskProvider` uses
`tokio::task::Builder::new().spawn_local(...)`. While the test outer
task is parked on `consumer.receive()`, the spawn_local'd driver task
only runs when the LocalSet's `run_until` is polled — and the proto
slab waker that we now fire on delivery is dispatched from the driver
task, which itself isn't being polled. The result is a ~30 s stall per
`Recv` until the proto keepalive deadline elapses and pumps the chain.
[`crates/magnetar-differential/src/runner_moonpool.rs`](../crates/magnetar-differential/src/runner_moonpool.rs)
keeps a 25 ms `Kicker` to pulse `driver_waker.notify_one()` and bridge
the LocalSet pump gap.

**Unblock.** Either (a) swap `TokioProviders` for `SimProviders` in the
moonpool runner so the simulator's deterministic scheduler drives both
sides without `spawn_local` (already tracked below), or (b) restructure
the runner to spawn the driver via plain `tokio::spawn`, giving up
moonpool-sim compatibility for the differential harness specifically.

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

## Testing + coverage

### Cross-runtime test + coverage closure (ADR-0024)

**Status.** [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md)
landed 2026-05-22 with both `cargo xtask check-sim-coverage` and
`cargo xtask check-runtime-test-parity` enabled and hard-failing. On
landing day the baseline is `tokio=65 moonpool=61` (gap of 4) and
moonpool patch-coverage of pre-existing surface is unmeasured. Every
merge to `main` that touches production code fails the validation
chain until both gaps close — by design.

**Unblock.** Dedicated session driven by the local prompt at
`tasks/coverage-closure-prompt.md` (gitignored). Phases:
(1) bring tokio↔moonpool counts to 1:1 — easiest;
(2) close pre-existing moonpool coverage gaps file by file using the
`cargo llvm-cov --html` report; (3) full validation chain green
including 32-seed sweep. ADR-0021 still applies — failing tests are
fixed, not `#[ignore]`-d.
