# ADR-0073 — `connections_per_broker`: round-robin producer/consumer fan-out across N connections per broker

- **Status**: Accepted
- **Date**: 2026-06-24
- **Decider**: Florentin Dubois
- **Tags**: runtime, connection-pool, throughput, determinism, java-parity

## Context

magnetar opens **one connection per broker** (ADR-0039): a single eager bootstrap connection for the single-broker / direct data plane, plus a lazy per-broker `ProxyConnectionPool` keyed `(logical, physical)` for the proxy and multi-broker-direct cases.
Every producer and consumer a client opens to a given broker therefore rides the **same** TCP connection — `Client::resolve_target` returns one `Arc<ConnectionShared>` per `(logical, physical)`.

Issue #314 (observed at magnetar `1.1.1`, rev `dd717db`): to reach acceptable produce throughput on a single topic an application was forced to hand-roll a pool of `PulsarClient`s (one TCP connection each) and round-robin produces across them, because a single connection's produce throughput is capped.
Spreading produce over multiple connections removed the send-side back-pressure, confirming the limit is per-connection — but connection-level parallelism should be the client's responsibility, not the application's.
The Java client exposes `ClientBuilder#connectionsPerBroker` for exactly this.

Two distinct bottlenecks were conflated in the original report:

1. **Per-connection driver throughput** — a single driver task that starved `CommandSendReceipt` reads under sustained sends. This is the root cause of the multi-second `send→ack` latency and is **already fixed** by [ADR-0070](0070-driver-read-arm-fairness.md) (issue #303, read-arm-first `select!`).
2. **No connection-level produce parallelism** — even with a healthy per-connection driver, one logical producer fleet is pinned to one connection, so it cannot use more than one connection's worth of send pipeline, socket buffer, or (on the broker) per-connection processing. This is the remaining gap and the subject of this ADR.

The pool key was always documented as a deliberate collapse of the Java client's `(logical, physical, randomKey)` triple to `(logical, physical)`, with the `randomKey` multiplexing called out as a follow-up to be done "once we measure contention warranting it" (`crates/magnetar-runtime-tokio/src/pool.rs`). #314 is that measurement.

## Decision

Add a **`connections_per_broker`** knob (Java `ClientBuilder#connectionsPerBroker`), default **1**.
With `n > 1` the client opens up to `n` connections per broker and **round-robins** producers and consumers across them.

Concrete design:

- **Runtime-level, not sans-io.** `connections_per_broker` is a multi-connection pool-management policy; it never reaches the `magnetar-proto` core, which is sans-io and models exactly one connection's state machine (ADR-0004). It lives on the runtime `Client` (both engines) and is applied via `Client::with_connections_per_broker(n)`; the `magnetar` façade `ClientBuilder::connections_per_broker(n)` calls it after the bootstrap connection is established. `0`/`1` are equivalent (Java's floor is one connection).
- **Pool key gains the index.** `(logical, physical)` → `(logical, physical, connection_index)`, with `connection_index ∈ [0, connections_per_broker)`, identically in both engines. At the default `1` the index is always `0`, so the key collapses to the historical one-entry-per-broker model and behaviour is byte-identical to the pre-#314 client.
- **Round-robin, deterministic.** `Client` holds an `AtomicUsize` cursor; `resolve_target` calls `pick_connection_index()` once per producer/consumer open (`cursor.fetch_add(1) % n`). Because `resolve_target` is the single chokepoint both `open_producer` and `subscribe` funnel through, one change fans out **both** producers and consumers. A plain atomic counter — not an RNG — is used deliberately: the spread is deterministic, which the moonpool engine mirrors bit-for-bit so the differential `EventStream` parity (ADR-0024) and the seed registry hold (ADR-0011).
- **Bootstrap is index 0.** The eager bootstrap connection serves `connection_index = 0` for its broker; indices `1..n` are lazy pool siblings (`get_or_open_bootstrap_sibling`) that dial the bootstrap's physical address and replicate the bootstrap CONNECT (same `proxy_to_broker_url`). The proxy and multi-broker-direct paths simply thread the index into the existing `get_or_open`.
- **Lookups and redirects stay on the primary.** `CommandLookupTopic` rides the bootstrap; the redirect-dial loop pins `index = 0`. Only data-plane producer/consumer placement consumes a fan-out slot — lookups never do.
- **`from_socket` clamps to 1.** A client with no dial-able URL (no pool) cannot open siblings, so `pick_connection_index` returns `0` regardless of the configured value.

Alternatives considered:

- **Random key per connection (Java-literal).** Java picks `randomKey = signSafeMod(random.nextInt(), connectionsPerBroker)`. Rejected: an RNG in the selection path would break moonpool's bit-for-bit reproducibility unless threaded through the injected RNG provider, which is more machinery for no benefit here — a round-robin counter spreads at least as evenly and is deterministic for free.
- **Thread `connections_per_broker` into `magnetar-proto::ConnectionConfig`.** Rejected: it is not a single-connection concern and would leak a pool-topology knob into the sans-io core (ADR-0004). It stays in the runtime, mirroring how ADR-0039's pool itself is a runtime-only construct.
- **Per-broker round-robin counters.** Marginally better balance on multi-broker clusters, but more state for no measurable gain on the dominant single-broker case; a single global cursor is simpler and deterministic. Can be revisited if multi-broker imbalance is ever measured.

## Consequences

- **Default is a no-op.** `connections_per_broker = 1` ⇒ index always `0` ⇒ the bootstrap-reuse / single-pool-entry behaviour is unchanged; every existing test stays green and no extra connection is dialled.
- **Applies to the whole data plane.** Producers and consumers both fan out; lookups and redirect dials stay on the primary connection.
- **Java parity.** `ClientBuilder#connectionsPerBroker` is now wired (README parity matrix, Client builder section).
- **Tests (ADR-0024 four layers + e2e).** No `magnetar-proto` unit layer — this change adds no proto/wire code (the pool is runtime-only, like ADR-0039); the omission is justified in the commit. Shipped: tokio integration (`connections_per_broker.rs`, 3 tests) + moonpool integration (`connections_per_broker.rs`, 3 tests, 1:1 parity) + differential equivalence (`connections_per_broker_equivalence.rs`, asserting both engines realize the same `[1,1,1]` fan-out layout) + e2e (`e2e_connections_per_broker.rs`: the #314 reproduction — four producers share one connection by default — plus the fix — `connections_per_broker(4)` spreads them across multiple broker-observed publisher source addresses).
- **Throughput, not latency.** This complements ADR-0070: ADR-0070 fixed per-connection `send→ack` latency; this lets a logical producer fleet use more than one connection's worth of pipeline. Applications no longer need to instantiate and round-robin a pool of `PulsarClient`s.
- **Deferred.** Per-broker counters and a random-key selection mode remain available as future refinements if multi-broker imbalance is ever measured.
