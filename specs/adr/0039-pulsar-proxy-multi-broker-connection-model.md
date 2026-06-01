# ADR-0039 — Per-broker connection pool for the Apache Pulsar Proxy

- **Status**: Accepted (amended by [ADR-0045](0045-proxy-to-broker-url-host-port-format.md), `proxy_to_broker_url` wire-format portion)
- **Date**: 2026-05-27
- **Decider**: Florentin Dubois
- **Tags**: architecture, proxy, lookup, connection-pool, runtime

> **Amendment (2026-06-01, [ADR-0045](0045-proxy-to-broker-url-host-port-format.md)).**
> The "Incompatibilities → None on the wire" claim below was inaccurate:
> the proxy requires `CommandConnect.proxy_to_broker_url` to be `host:port`
> (no scheme), parsed via `InetSocketAddress.createUnresolved`. Magnetar
> previously stuffed the broker's advertised `pulsar://host:port` value
> verbatim, which made the proxy reject the handshake with
> `ServerError.ServiceNotReady "Target broker cannot be validated"`.
> See [ADR-0045](0045-proxy-to-broker-url-host-port-format.md) for the
> scheme-strip helpers and their tests.

## Context

Magnetar currently runs a single `Arc<ConnectionShared>` per `PulsarClient` —
the connection that does the initial handshake to the configured
`service_url` is also the one every producer, consumer, lookup, and admin
op rides on. That model breaks against the official
[Apache Pulsar Proxy](https://pulsar.apache.org/docs/administration-proxy/),
which expects a per-broker-target connection (issue #15, otelgw 2026-05-27
incident).

The proxy's wire contract — derived from the upstream Java client
([`BinaryProtoLookupService.findBroker`](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/BinaryProtoLookupService.java),
[`ConnectionPool.getConnection`](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConnectionPool.java),
[`Commands.newConnect(..., targetBroker)`](https://github.com/apache/pulsar/blob/master/pulsar-common/src/main/java/org/apache/pulsar/common/protocol/Commands.java))
— is:

1. Client connects to the proxy and sends `CommandLookupTopic`.
2. Proxy answers with the resolved broker URL plus
   `proxy_through_service_url = true`.
3. Client must open a **new** connection — back to the **proxy** address
   (the `physicalAddress`) — and set `proxy_to_broker_url` on
   `CommandConnect` to the broker URL (the `logicalAddress`).
4. The proxy then forwards every frame on that new connection to the
   resolved broker.

Without step 3 the proxy can route lookups (those are
broker-pool-agnostic on the proxy side) but cannot route
`CommandProducer` / `CommandSubscribe`, so it closes the socket shortly
after `ProducerSuccess`. That's exactly the reconnect storm tracked in
issue #14 (a per-handle backoff bug independent of this ADR) and the
silent-drop in #15 (this ADR).

Magnetar's proto layer is already wired for the contract — `lookup.rs`
emits `LookupOutcome::Connect { broker_service_url,
broker_service_url_tls, proxy_through_service_url }` and `conn.rs`'s
`begin_handshake` already threads `ConnectionConfig.proxy_to_broker_url`
into `CommandConnect`. Only the runtime engines ignore the signal: the
`Redirected` outcome surfaces a `tracing::warn!("broker redirected
lookup; multi-broker redirect is follow-up work")` and the `Connect`
outcome drops the `proxy_through_service_url` flag on the floor
(`crates/magnetar-runtime-tokio/src/client.rs` ~ L351–364).

Alternatives considered:

- **One connection, `proxy_to_broker_url` rotated per topic**: rejected.
  The proxy ties `proxy_to_broker_url` to the connection at `CONNECT`
  time; the value cannot rotate. Setting it to the proxy itself
  (Rémi's experiment on otelgw, recorded on the issue) also fails the
  handshake.
- **Always use the proxy address as both lookup and data target with
  no `proxy_to_broker_url`**: rejected. That is what magnetar does
  today and it doesn't work — the proxy drops as soon as a data frame
  arrives.
- **Per-producer connection (no pool)**: rejected. Java reuses
  connections per `(broker, proxy, randomKey)`; we mirror that for the
  same fan-out reasons (one TLS handshake per broker, not per
  producer).

## Decision

Introduce a per-`PulsarClient` connection pool keyed by
`(logical_broker_url, physical_dial_addr)`, matching the Java pool
key shape (sans the `randomKey` multiplexer — punted as follow-up
when measured contention warrants it). Mechanics:

```text
                       ┌─────────────────────────────────────────────────┐
                       │ PulsarClient<E>                                 │
                       │                                                 │
                       │   bootstrap: Arc<ConnectionShared>              │
                       │   ▲ dials service_url, proxy_to_broker_url=None │
                       │   │ owns lookups, partitioned-metadata,         │
                       │   │ partitions-list, GetSchema, admin pings.    │
                       │                                                 │
                       │   pool:     Arc<ProxyConnectionPool>            │
                       │     entries: HashMap<                           │
                       │       (logical_broker, physical_dial),          │
                       │       Arc<ConnectionShared>                     │
                       │     >                                           │
                       └─────────────────────────────────────────────────┘
```

- **`bootstrap`** — the existing single connection, kept as the
  lookup-and-control plane. Dialled to `service_url` with
  `proxy_to_broker_url = None`. Every lookup, partitioned-metadata,
  partitions-list, txn-coordinator op, get-schema, etc. continues to
  ride on it. This is also the only connection a non-proxied broker
  ever needs.

- **`pool`** — created lazily. The pool stores **data connections**,
  one per `(logical_broker, physical_dial_addr)` pair. Lookup of a
  topic on `bootstrap` returns either:
  - `LookupOutcome::Connect { broker_service_url, broker_service_url_tls,
    proxy_through_service_url: false }` — the broker is talkable
    directly; producer/consumer rides on `bootstrap` (current behaviour
    preserved).
  - `LookupOutcome::Connect { …, proxy_through_service_url: true }` —
    pick the `logical_broker` from `broker_service_url_tls` /
    `broker_service_url` per the client's TLS choice, take or open
    `pool[(logical_broker, physical_dial=service_url)]`, then issue
    `CommandProducer` / `CommandSubscribe` on that pool entry.

- **Pool entry lifecycle**: each `Arc<ConnectionShared>` in the pool
  spawns its own supervised driver loop, with the same
  `SupervisorConfig`, same `Auth`, same TLS config, same
  `ServiceUrlProvider`, same `DnsResolver`, plus
  `ConnectionConfig.proxy_to_broker_url = Some(logical_broker)`. All
  of ADR-0028's anti-thrash, ADR-0024's coverage, and the recent
  ADR-0038 lock split apply unchanged.

- **Dial target**: every pool entry **dials the same physical
  address** — the proxy on `service_url`. Only the `CommandConnect`
  frame's `proxy_to_broker_url` differs. This matches the Java
  `ConnectionPool.connectToAddress(logicalAddress, physicalAddress,
  …)` shape.

- **Routing the per-handle ops**: `Producer` and `Consumer` each carry
  an `Arc<ConnectionShared>` field; the field captured at construction
  time is the one the open-producer / open-consumer routing chose. The
  per-slot mutex split (ADR-0038) survives unchanged — slots are local
  to each `ConnectionShared`.

- **`Producer.close` / `Consumer.close`**: only sends `CloseProducer`
  / `CloseConsumer` on its own pool entry's connection. The pool entry
  is **not** dropped when its last producer/consumer goes away — pool
  entries are owned by the client and torn down on `Client.close`.
  This matches the Java client's "evictable" pool behaviour without
  the eviction policy (which is follow-up #—).

- **`Client.close`** — closes the bootstrap and every pool entry. Each
  supervised driver loop sees `is_user_closed()` and exits cleanly.

- **No connection sharing of lookup work**: lookups stay on `bootstrap`.
  Java reuses any pool entry for lookups when convenient, but the
  proxy doesn't care which connection runs the lookup, and routing
  every lookup through `bootstrap` keeps the model simple and the
  hot-cache for lookup futures local to one `ConnectionShared`.
  Follow-up if measured.

- **Bootstrap conn is also a degenerate pool entry** for the
  no-proxy case — i.e. when every lookup returns
  `proxy_through_service_url=false`, the pool stays empty and
  behaviour is identical to today's single-connection model. Existing
  tests + parity matrix unchanged.

## Consequences

**Easier**:
- Magnetar talks to the Apache Pulsar Proxy without the reconnect
  storm of #14 + the silent drop of #15. otelgw + every other
  Clever-Cloud-managed-Pulsar consumer can switch off the
  `pulsar-rs` fallback.
- The runtime tree grows one cleanly-scoped pool abstraction, not
  thirty per-call branches. Adding the Java-style `randomKey`
  multiplexer later is local to `ProxyConnectionPool`.
- Per-broker reconnects no longer affect unrelated topics: a broker
  rolling restart only churns its own pool entry. Today (single
  connection) every topic on every broker hiccups together.

**Harder**:
- `Client.close` now has to await N supervised loops, not one. The
  facade-level `tokio::join!`/`futures::future::join_all` over the
  pool stays bounded by `O(distinct brokers in flight)`.
- Auth refresh races: each pool entry has its own
  `AuthChallenge → AuthResponse` pipeline. The shared `Auth` trait
  object is referenced from every `ConnectionShared`, so refreshed
  tokens cross over naturally — but the test matrix grows by
  `proxy_through={true,false}`.
- `magnetar-fakes` and the moonpool sim chaos workloads need a
  "fake proxy" — a scripted broker that emits
  `proxy_through_service_url=true` and refuses `CommandProducer`
  unless the second connection's `CommandConnect.proxy_to_broker_url`
  is the URL it advertised.

**Costs**:
- One extra TLS handshake per distinct broker reached through the
  proxy. The handshake cost is amortised over the lifetime of that
  pool entry, and the proxy's broker fan-in is typically << client
  fan-out, so the trade is favourable.
- One extra `tokio::spawn` (or moonpool task) per pool entry. Same
  amortisation argument.

**Incompatibilities**:
- None on the wire — magnetar already encodes `proxy_to_broker_url`
  correctly. The change is observable only as a new connection per
  proxied broker.
- Public API: `PulsarClient::shared()` (used by some tests) keeps
  returning the bootstrap `ConnectionShared`. Tests asserting on
  shared state of producers under proxy must follow
  `Producer::connection()` (a new accessor) to find the pool entry.

## Tests (ADR-0024 four-layer matrix)

- **`magnetar-proto`** — proxy-aware `LookupOutcome::Connect` is
  already plumbed (`lookup.rs::connect_outcome_honours_proxy_through_service_url`).
  Extend `conn.rs` tests to assert `CommandConnect.proxy_to_broker_url`
  is encoded iff `ConnectionConfig.proxy_to_broker_url = Some(_)`,
  and omitted otherwise.
- **`magnetar-runtime-tokio`** — `ScriptedProxyBroker` fake that:
  (a) accepts the bootstrap connection without `proxy_to_broker_url`
  and serves `CommandLookupTopic` with `proxy_through=true`;
  (b) accepts a second connection with `proxy_to_broker_url = broker_url`
  and serves `CommandProducer`/`CommandSubscribe` for that broker. Test
  asserts (i) two distinct TCP connections, (ii) the right
  `CommandConnect` flags on each, (iii) producer publish/consume
  succeeds end-to-end.
- **`magnetar-runtime-moonpool`** — mirror in the chaos workload
  format (`ProxyProxyThroughBroker` workload), 1:1 test count.
- **`magnetar-differential`** — `EventStream` parity assertion across
  engines for the proxy scenario.
- **`crates/magnetar/tests/e2e_*.rs`** — Docker compose with
  `apachepulsar/pulsar:4.0.4` standalone behind the official
  `apachepulsar/pulsar:4.0.4` proxy. Single produce + consume cycle.
  Gated behind the existing `e2e` feature.

## References

- `crates/magnetar-runtime-tokio/src/client.rs` — `open_producer`, the
  decision-routing site.
- `crates/magnetar-runtime-moonpool/src/client.rs` — moonpool mirror.
- `crates/magnetar-proto/src/lookup.rs` — `LookupOutcome::Connect`
  carries `proxy_through_service_url` already.
- `crates/magnetar-proto/src/conn.rs` — `begin_handshake` threads
  `proxy_to_broker_url` into `CommandConnect`.
- Upstream:
  [`BinaryProtoLookupService.java`](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/BinaryProtoLookupService.java),
  [`ConnectionPool.java`](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConnectionPool.java),
  [`Commands.java`](https://github.com/apache/pulsar/blob/master/pulsar-common/src/main/java/org/apache/pulsar/common/protocol/Commands.java)
  (`newConnect(..., targetBroker)`).
- Related ADRs: ADR-0028 (anti-thrash, the per-conn defence in
  depth), ADR-0024 (test matrix), ADR-0038 (per-handle slot mutex,
  preserved unchanged).
- Issues: #14 (storm-mitigation, landed), #15 (this ADR).
