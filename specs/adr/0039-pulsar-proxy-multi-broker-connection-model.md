# ADR-0039 — Per-broker connection pool for the Apache Pulsar Proxy

- **Status**: Accepted (amended by [ADR-0045](0045-proxy-to-broker-url-host-port-format.md), `proxy_to_broker_url` wire-format portion; amended 2026-06-01 — multi-broker DIRECT routing)
- **Date**: 2026-05-27
- **Decider**: Florentin Dubois
- **Tags**: architecture, proxy, lookup, connection-pool, runtime

> **Amendment (2026-06-01, [ADR-0045](0045-proxy-to-broker-url-host-port-format.md)).** The "Incompatibilities → None on the wire" claim below was inaccurate: the proxy requires `CommandConnect.proxy_to_broker_url` to be `host:port` (no scheme), parsed via `InetSocketAddress.createUnresolved`.
> Magnetar previously stuffed the broker's advertised `pulsar://host:port` value verbatim, which made the proxy reject the handshake with `ServerError.ServiceNotReady "Target broker cannot be validated"`.
> See [ADR-0045](0045-proxy-to-broker-url-host-port-format.md) for the scheme-strip helpers and their tests.

> **Amendment (2026-06-01, multi-broker DIRECT routing).**
> The "Consequences" section originally framed multi-broker DIRECT
> routing as follow-up work: when a lookup returns
> `LookupOutcome::Connect { broker_service_url: Some(B), proxy_through_service_url: false }`
> magnetar dropped `B` and opened the producer / consumer on the
> bootstrap connection. Against a multi-broker non-proxy Pulsar cluster
> this manifested as `ServerError::NotConnected "not served by this
instance"` and bouncing retries (HIGH-1 from the lookup multi-agent
> review). This amendment generalises the existing
> `ProxyConnectionPool` to also cover the DIRECT case: the pool key
> stays `(logical_broker_url, physical_dial_addr)` and a new
> `proxy_to_broker_url: Option<String>` parameter on
> `get_or_open` distinguishes the two routing shapes — `Some(host_port)`
> for proxy entries, `None` for direct entries. The tokio engine wires
> this end-to-end; the moonpool engine captures the routing decision at
> the proto level and falls back to the bootstrap until the moonpool
> flavour of `ProxyConnectionPool` lands (`docs/follow-ups.md §3`). See
> the "Multi-broker DIRECT routing (2026-06-01)" section below for the
> full design rationale.

## Context

Magnetar currently runs a single `Arc<ConnectionShared>` per `PulsarClient` — the connection that does the initial handshake to the configured `service_url` is also the one every producer, consumer, lookup, and admin op rides on.
That model breaks against the official [Apache Pulsar Proxy](https://pulsar.apache.org/docs/administration-proxy/), which expects a per-broker-target connection (issue #15, otelgw 2026-05-27 incident).

The proxy's wire contract — derived from the upstream Java client ([`BinaryProtoLookupService.findBroker`](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/BinaryProtoLookupService.java), [`ConnectionPool.getConnection`](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConnectionPool.java), [`Commands.newConnect(..., targetBroker)`](https://github.com/apache/pulsar/blob/master/pulsar-common/src/main/java/org/apache/pulsar/common/protocol/Commands.java)) — is:

1. Client connects to the proxy and sends `CommandLookupTopic`.
2. Proxy answers with the resolved broker URL plus `proxy_through_service_url = true`.
3. Client must open a **new** connection — back to the **proxy** address (the `physicalAddress`) — and set `proxy_to_broker_url` on `CommandConnect` to the broker URL (the `logicalAddress`).
4. The proxy then forwards every frame on that new connection to the resolved broker.

Without step 3 the proxy can route lookups (those are broker-pool-agnostic on the proxy side) but cannot route `CommandProducer` / `CommandSubscribe`, so it closes the socket shortly after `ProducerSuccess`.
That's exactly the reconnect storm tracked in issue #14 (a per-handle backoff bug independent of this ADR) and the silent-drop in #15 (this ADR).

Magnetar's proto layer is already wired for the contract — `lookup.rs` emits `LookupOutcome::Connect { broker_service_url, broker_service_url_tls, proxy_through_service_url }` and `conn.rs`'s `begin_handshake` already threads `ConnectionConfig.proxy_to_broker_url` into `CommandConnect`.
Only the runtime engines ignore the signal: the `Redirected` outcome surfaces a `tracing::warn!("broker redirected lookup; multi-broker redirect is follow-up work")` and the `Connect` outcome drops the `proxy_through_service_url` flag on the floor (`crates/magnetar-runtime-tokio/src/client.rs` ~ L351–364).

Alternatives considered:

- **One connection, `proxy_to_broker_url` rotated per topic**: rejected.
  The proxy ties `proxy_to_broker_url` to the connection at `CONNECT` time; the value cannot rotate.
  Setting it to the proxy itself (Rémi's experiment on otelgw, recorded on the issue) also fails the handshake.
- **Always use the proxy address as both lookup and data target with no `proxy_to_broker_url`**: rejected.
  That is what magnetar does today and it doesn't work — the proxy drops as soon as a data frame arrives.
- **Per-producer connection (no pool)**: rejected.
  Java reuses connections per `(broker, proxy, randomKey)`; we mirror that for the same fan-out reasons (one TLS handshake per broker, not per producer).

## Decision

Introduce a per-`PulsarClient` connection pool keyed by `(logical_broker_url, physical_dial_addr)`, matching the Java pool key shape (sans the `randomKey` multiplexer — punted as follow-up when measured contention warrants it).
Mechanics:

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

- **`bootstrap`** — the existing single connection, kept as the lookup-and-control plane.
  Dialled to `service_url` with `proxy_to_broker_url = None`.
  Every lookup, partitioned-metadata, partitions-list, txn-coordinator op, get-schema, etc. continues to ride on it.
  This is also the only connection a non-proxied broker ever needs.

- **`pool`** — created lazily.
  The pool stores **data connections**, one per `(logical_broker, physical_dial_addr)` pair.
  Lookup of a topic on `bootstrap` returns either:
  - `LookupOutcome::Connect { broker_service_url, broker_service_url_tls, proxy_through_service_url: false }` — the broker is talkable directly; producer/consumer rides on `bootstrap` (current behaviour preserved).
  - `LookupOutcome::Connect { …, proxy_through_service_url: true }` — pick the `logical_broker` from `broker_service_url_tls` / `broker_service_url` per the client's TLS choice, take or open `pool[(logical_broker, physical_dial=service_url)]`, then issue `CommandProducer` / `CommandSubscribe` on that pool entry.

- **Pool entry lifecycle**: each `Arc<ConnectionShared>` in the pool spawns its own supervised driver loop, with the same `SupervisorConfig`, same `Auth`, same TLS config, same `ServiceUrlProvider`, same `DnsResolver`, plus `ConnectionConfig.proxy_to_broker_url = Some(logical_broker)`.
  All of ADR-0028's anti-thrash, ADR-0024's coverage, and the recent ADR-0038 lock split apply unchanged.

- **Dial target**: every pool entry **dials the same physical address** — the proxy on `service_url`.
  Only the `CommandConnect` frame's `proxy_to_broker_url` differs.
  This matches the Java `ConnectionPool.connectToAddress(logicalAddress, physicalAddress, …)` shape.

- **Routing the per-handle ops**: `Producer` and `Consumer` each carry an `Arc<ConnectionShared>` field; the field captured at construction time is the one the open-producer / open-consumer routing chose.
  The per-slot mutex split (ADR-0038) survives unchanged — slots are local to each `ConnectionShared`.

- **`Producer.close` / `Consumer.close`**: only sends `CloseProducer` / `CloseConsumer` on its own pool entry's connection.
  The pool entry is **not** dropped when its last producer/consumer goes away — pool entries are owned by the client and torn down on `Client.close`.
  This matches the Java client's "evictable" pool behaviour without the eviction policy (which is follow-up #—).

- **`Client.close`** — closes the bootstrap and every pool entry.
  Each supervised driver loop sees `is_user_closed()` and exits cleanly.

- **No connection sharing of lookup work**: lookups stay on `bootstrap`.
  Java reuses any pool entry for lookups when convenient, but the proxy doesn't care which connection runs the lookup, and routing every lookup through `bootstrap` keeps the model simple and the hot-cache for lookup futures local to one `ConnectionShared`.
  Follow-up if measured.

- **Bootstrap conn is also a degenerate pool entry** for the no-proxy case — i.e. when every lookup returns `proxy_through_service_url=false`, the pool stays empty and behaviour is identical to today's single-connection model.
  Existing tests + parity matrix unchanged.

## Consequences

**Easier**:

- Magnetar talks to the Apache Pulsar Proxy without the reconnect storm of #14 + the silent drop of #15. otelgw + every other Clever-Cloud-managed-Pulsar consumer can switch off the `pulsar-rs` fallback.
- The runtime tree grows one cleanly-scoped pool abstraction, not thirty per-call branches.
  Adding the Java-style `randomKey` multiplexer later is local to `ProxyConnectionPool`.
- Per-broker reconnects no longer affect unrelated topics: a broker rolling restart only churns its own pool entry.
  Today (single connection) every topic on every broker hiccups together.

**Harder**:

- `Client.close` now has to await N supervised loops, not one.
  The facade-level `tokio::join!`/`futures::future::join_all` over the pool stays bounded by `O(distinct brokers in flight)`.
- Auth refresh races: each pool entry has its own `AuthChallenge → AuthResponse` pipeline.
  The shared `Auth` trait object is referenced from every `ConnectionShared`, so refreshed tokens cross over naturally — but the test matrix grows by `proxy_through={true,false}`.
- `magnetar-fakes` and the moonpool sim chaos workloads need a "fake proxy" — a scripted broker that emits `proxy_through_service_url=true` and refuses `CommandProducer` unless the second connection's `CommandConnect.proxy_to_broker_url` is the URL it advertised.

**Costs**:

- One extra TLS handshake per distinct broker reached through the proxy.
  The handshake cost is amortised over the lifetime of that pool entry, and the proxy's broker fan-in is typically << client fan-out, so the trade is favourable.
- One extra `tokio::spawn` (or moonpool task) per pool entry.
  Same amortisation argument.

**Incompatibilities**:

- None on the wire — magnetar already encodes `proxy_to_broker_url` correctly.
  The change is observable only as a new connection per proxied broker.
- Public API: `PulsarClient::shared()` (used by some tests) keeps returning the bootstrap `ConnectionShared`.
  Tests asserting on shared state of producers under proxy must follow `Producer::connection()` (a new accessor) to find the pool entry.

## Tests (ADR-0024 four-layer matrix)

- **`magnetar-proto`** — proxy-aware `LookupOutcome::Connect` is already plumbed (`lookup.rs::connect_outcome_honours_proxy_through_service_url`).
  Extend `conn.rs` tests to assert `CommandConnect.proxy_to_broker_url` is encoded iff `ConnectionConfig.proxy_to_broker_url = Some(_)`, and omitted otherwise.
- **`magnetar-runtime-tokio`** — `ScriptedProxyBroker` fake that: (a) accepts the bootstrap connection without `proxy_to_broker_url` and serves `CommandLookupTopic` with `proxy_through=true`; (b) accepts a second connection with `proxy_to_broker_url = broker_url` and serves `CommandProducer`/`CommandSubscribe` for that broker.
  Test asserts (i) two distinct TCP connections, (ii) the right `CommandConnect` flags on each, (iii) producer publish/consume succeeds end-to-end.
- **`magnetar-runtime-moonpool`** — mirror in the chaos workload format (`ProxyProxyThroughBroker` workload), 1:1 test count.
- **`magnetar-differential`** — `EventStream` parity assertion across engines for the proxy scenario.
- **`crates/magnetar/tests/e2e_*.rs`** — Docker compose with `apachepulsar/pulsar:4.0.4` standalone behind the official `apachepulsar/pulsar:4.0.4` proxy.
  Single produce + consume cycle.
  Gated behind the existing `e2e` feature.

## Moonpool engine parity (2026-06-01)

The original landing of this ADR shipped the
[`ProxyConnectionPool`](../../crates/magnetar-runtime-tokio/src/pool.rs)
on the **tokio engine** only. The moonpool engine surfaced a
`ClientError::ProxyUnsupportedOnUnsupervisedClient` error on the proxy
branch and was tracked as
[follow-up §3 in `docs/follow-ups.md`](../../docs/follow-ups.md#3-moonpool-proxyconnectionpool-parity).
This amendment lands the moonpool flavour and flips the parity row.

### Shape

The moonpool side mirrors the tokio side 1:1 in
[`crates/magnetar-runtime-moonpool/src/pool.rs`](../../crates/magnetar-runtime-moonpool/src/pool.rs):

- `ConnectionFactory<P: Providers>` captures the bootstrap inputs (proxy
  `addr`, template `ConnectionConfig`, providers bundle, optional
  `ServiceUrlProvider`, optional `DnsResolver`).
- `ProxyConnectionPool<P>` holds the `Mutex<HashMap<(logical, physical),
Arc<EntryState>>>`. `EntryState` is `Pending(PendingDial)` while a dial
  task is in flight and `Ready { shared, driver }` once the supervised
  driver loop is up.
- `pool::get_or_open(Arc<Self>, logical)` is the entry point; takes the
  pool by `Arc` so the spawned dial task can outlive the caller's
  `&self`.

The pool is constructed when (and only when) `Client::connect_plain_supervised`
is the construction path — the `from_parts` / `connect_plain` paths leave
`pool: None`, which keeps the historic single-connection behaviour
intact for tests and unsupervised callers.

### `Send` propagation — the load-bearing detail

`moonpool_core::NetworkProvider` is declared `#[async_trait(?Send)]` on
the published 0.6.0 release (the workspace currently floats `branch =
"main"` per ADR-0043, which has since lifted that restriction; both
shapes are accommodated). A naïve `network.connect(...).await` inside
the producer / consumer open path would break `Send` propagation up to
the facade's
[`CreateProducerApi`](../../crates/magnetar/src/engine/mod.rs) /
[`SubscribeApi`](../../crates/magnetar/src/engine/mod.rs) traits, which
pin their returns as `Pin<Box<dyn Future + Send + '_>>`.

The pool side-steps that by hoisting the dial + handshake + supervised
driver spawn into a task created via
[`moonpool_core::TaskProvider::spawn_task`](https://docs.rs/moonpool-core/0.6.0/moonpool_core/task/trait.TaskProvider.html#tymethod.spawn_task).
`spawn_task` uses `spawn_local` (registry) / `tokio::task::Builder::spawn`
(main rev); either way the spawned future is **not required to be
`Send`**. The outer `get_or_open` future only `.await`s a
[`tokio::sync::Notify`] plus reads an `Arc<Mutex<Option<Arc<DialOutcome>>>>`
slot — both `Send` regardless of `P`'s flavour. Multiple racing waiters
clone the published `Arc<DialOutcome>` out of the slot and unwrap the
result locally; `EngineError` isn't `Clone`, so a `clone_engine_error`
helper hand-rolls a shallow copy for the rare `Err` arm.

### Determinism

The pool keys, dial path, and lock discipline match the tokio side
exactly. Determinism considerations:

- Pool entries are built with
  [`make_shared_with_providers`](../../crates/magnetar-runtime-moonpool/src/lib.rs)
  — the same helper the bootstrap connection uses. The deterministic
  monotonic-clock closure (ADR-0011) flows through, so producer / consumer
  state machine `Instant` reads on a pinned pool entry observe the same
  virtual time as the bootstrap.
- The `spawn_task`-detached dial task runs through the moonpool
  `TaskProvider` — under `SimProviders` (`moonpool-sim`) the scheduler
  pumps it deterministically; under `TokioProviders` it's a plain
  `tokio::spawn`.
- `PendingDial`'s `Notify::notify_waiters()` plus the slot publication
  happen under the same critical section ordering as the tokio engine's
  race-resolution path (`build_entry` → entries-lock → race check). The
  result is observationally equivalent: at most one dial per
  `(logical, physical)`; waiters all get the same `Arc<DialOutcome>`.
- No host-time reads, no out-of-band randomness, no I/O outside the
  spawned dial task. The pool itself is purely a `HashMap` + `Notify`
  bookkeeping layer.

### Deviations from tokio

- **Race resolution**: tokio races by always running `build_entry` and
  discarding the loser's half-built entry post-dial. The moonpool side
  uses a `Pending` slot installed under the entries-lock _before_
  spawning the dial; subsequent callers join the existing dial instead
  of opening a parallel connection. This is cleaner under the
  spawn-task pattern (no `DriverHandle::abort` dance on the losing path)
  and produces the same observable end state (one connection per
  `(logical, physical)`).
- **Pool teardown**: `Pending` entries are dropped without explicit
  abort on `close`. The dial task either completes and inserts a
  `Ready` entry that the close path immediately re-drains, or fails (in
  which case the entry was already evicted) — both paths converge on a
  cleanly-shut-down pool.
- **Pool unit-test count**: moonpool's `pool.rs` ships **two** unit
  tests (`fresh_pool_is_empty`, `debug_includes_pool_state`) matching
  tokio's count. The end-to-end pool behaviour is covered by the
  integration test (`tests/proxy_multi_conn.rs`) and the differential
  equivalence test (`magnetar-differential::proxy_routing_equivalence`).

### Tests (ADR-0024 four-layer matrix)

- **`magnetar-proto`** — unchanged. The proto-layer `LookupOutcome` /
  `ConnectionConfig.proxy_to_broker_url` plumbing was already in place
  from the tokio landing.
- **`magnetar-runtime-tokio`** — unchanged; the existing
  [`tests/proxy_multi_conn.rs`](../../crates/magnetar-runtime-tokio/tests/proxy_multi_conn.rs)
  remains the reference.
- **`magnetar-runtime-moonpool`** — new mirror in
  [`tests/proxy_multi_conn.rs`](../../crates/magnetar-runtime-moonpool/tests/proxy_multi_conn.rs).
  Three tests cover open-producer, subscribe, and pool-entry reuse.
- **`magnetar-differential`** — new
  [`tests/proxy_routing_equivalence.rs`](../../crates/magnetar-differential/tests/proxy_routing_equivalence.rs)
  asserts the tokio + moonpool engines produce equivalent
  `ProxyObservation`s (session count, CONNECT flags, per-session command
  kinds) against an identical proxy fake.
- **e2e** — the existing
  [`crates/magnetar/tests/e2e_pulsar_proxy.rs`](../../crates/magnetar/tests/e2e_pulsar_proxy.rs)
  exercises the proxy path end-to-end via the facade (tokio engine).
  A moonpool facade e2e mirror is out of scope for this amendment
  (the facade-builder surface for moonpool is reqwest-free but the
  testcontainers harness piggybacks the tokio builder); a follow-up
  PR can add it once the `MoonpoolEngine` builder wiring lands.

## References

- `crates/magnetar-runtime-tokio/src/client.rs` — `open_producer`, the decision-routing site.
- `crates/magnetar-runtime-moonpool/src/client.rs` — moonpool mirror.
- `crates/magnetar-proto/src/lookup.rs` — `LookupOutcome::Connect` carries `proxy_through_service_url` already.
- `crates/magnetar-proto/src/conn.rs` — `begin_handshake` threads `proxy_to_broker_url` into `CommandConnect`.
- Upstream: [`BinaryProtoLookupService.java`](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/BinaryProtoLookupService.java), [`ConnectionPool.java`](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConnectionPool.java), [`Commands.java`](https://github.com/apache/pulsar/blob/master/pulsar-common/src/main/java/org/apache/pulsar/common/protocol/Commands.java) (`newConnect(..., targetBroker)`).
- Related ADRs: ADR-0028 (anti-thrash, the per-conn defence in depth), ADR-0024 (test matrix), ADR-0038 (per-handle slot mutex, preserved unchanged).
- Issues: #14 (storm-mitigation, landed), #15 (this ADR).

## Multi-broker DIRECT routing (2026-06-01)

### Problem statement

Before this amendment, magnetar's lookup-target enum had two shapes:

```rust
enum LookupTarget {
    /// Reuse the bootstrap connection — *regardless* of which broker
    /// the lookup actually resolved to.
    Direct,
    /// Use the proxy pool, dialling the proxy address with
    /// CommandConnect.proxy_to_broker_url = Some(broker_url).
    Proxy { broker_url: String },
}
```

The `Direct` arm dropped the lookup's `broker_service_url` on the
floor. The original ADR-0039 §"Consequences" called this out:

> [Direct] The lookup's `broker_service_url` may name a different
> broker than the one currently connected; multi-broker direct routing
> is follow-up work tracked in ADR-0039 §"Consequences".

Against a multi-broker non-proxy Pulsar cluster (e.g. a 3-broker
cluster, no proxy in front), every lookup answers with the broker that
owns the namespace bundle. If that broker is not the bootstrap, magnetar
issues `CommandProducer` / `CommandSubscribe` on the bootstrap anyway,
the broker replies `ServerError::NotConnected "not served by this
instance, please redo the lookup"`, and the producer / subscribe bounces
through reconnect retries. This is HIGH-1 from the 2026-06-01 lookup
multi-agent review.

### Design alternatives considered

**Option A — extend `LookupTarget::Direct` with an inline `Option<String>`,
add a hand-rolled per-broker dial path:**

```rust
enum LookupTarget {
    Direct { broker_url: Option<String> },
    Proxy { broker_url: String },
}
```

Smallest surface change but duplicates the supervised-dial / handshake
/ TLS plumbing that already lives inside `ProxyConnectionPool`. Two
near-identical code paths inevitably drift.

**Option B (CHOSEN) — generalise the existing `ProxyConnectionPool` to
handle both routing shapes by parameterising
`CommandConnect.proxy_to_broker_url`:**

```rust
async fn get_or_open(
    &self,
    logical: &str,
    physical: &ParsedUrl,
    proxy_to_broker_url: Option<String>,   // NEW
) -> Result<Arc<ConnectionShared>, ClientError>;
```

- Proxy entries: `logical = broker_url`, `physical = proxy address`,
  `proxy_to_broker_url = Some(logical)` — unchanged from the original
  ADR.
- Direct entries: `logical = broker_url`, `physical = parsed broker_url`,
  `proxy_to_broker_url = None` — dials the resolved broker directly,
  no proxy in the middle, `CommandConnect` carries no
  `proxy_to_broker_url`.

The pool key shape `(logical, physical)` stays identical to Java's
`ConnectionPool` key (sans `randomKey`) and faithfully captures the
two routing topologies in one map. Java does exactly this — the same
`ConnectionPool.getConnection(logicalAddress, physicalAddress)` entry
point covers both proxy and direct routing.

Option A was rejected because it would have duplicated the supervised
driver-spawn, the handshake `wait_connected`, the race-resolution
logic, and the close-time teardown — all of which the pool already
encapsulates. The B-option diff to the pool is a single new parameter
and a one-line change in `build_entry`.

### Tokio engine: end-to-end

`LookupTarget` becomes:

```rust
enum LookupTarget {
    /// `broker_url = None` — pre-2.4 broker or single-broker cluster.
    ///                      Bootstrap connection.
    /// `broker_url = Some(url)` — multi-broker DIRECT routing target.
    Direct { broker_url: Option<String> },
    Proxy { broker_url: String },
}
```

`Client::lookup_topic` captures the resolved broker URL on both the
`LookupOutcome::Connect { proxy_through_service_url: false, … }` and
`LookupOutcome::Redirected { … }` branches (the proto layer chases
redirect chains internally; the surfaced URL is the terminal hop).

`Client::resolve_target` dispatches:

- `Direct { broker_url: None }` → `self.shared.clone()` (bootstrap).
- `Direct { broker_url: Some(url) }` → `resolve_direct_broker(url,
topic)`:
  1. Parse `url` into a `ParsedUrl`. The synthetic-scheme fallback
     (`parse_direct_broker_url`) tolerates the bare `host:port` form
     in case a broker advertises that (matches `preferred_broker_url`
     output too).
  2. **Bootstrap-equality fast path**: if `parsed.host == bootstrap.host
&& parsed.port == bootstrap.port`, return the bootstrap
     connection. Saves one TCP/TLS handshake on every same-broker
     lookup; mirrors Java's pool-identity check.
  3. Otherwise call
     `pool.get_or_open(url, &parsed, /* proxy_to_broker_url = */ None)`
     — opens (or reuses) a pool entry that dials `parsed` directly with
     no proxy.
- `Proxy { broker_url }` → unchanged.

### TLS posture

The bootstrap's `tls_config: Option<Arc<rustls::ClientConfig>>` is
shared across every pool entry (trust anchors are cluster-wide;
brokers in the same cluster typically run the same TLS posture). The
DIRECT-path dial uses `Transport::connect_with_resolver(parsed,
self.factory.tls_config.clone(), …)`; `Transport` dispatches on
`parsed.scheme`, so a `pulsar+ssl://broker-N:6651` resolved URL gets a
TLS dial and a `pulsar://broker-N:6650` resolved URL gets a plain
dial, even on a TLS-bootstrap connection (rare but happens during
broker rolling upgrades).

Picking between `broker_service_url` and `broker_service_url_tls` on
the DIRECT path mirrors `preferred_broker_url`: prefer the URL that
matches the bootstrap's scheme, fall back to whichever is advertised.
The helper is `direct_broker_url` — same logic, but preserves the full
Pulsar URL (DIRECT path needs to recover the dial target, whereas the
proxy path strips to `host:port` per ADR-0045).

### Auth reuse, anti-thrash, supervised reconnect

- Auth: every pool entry shares the same `Auth` trait object (already
  the original ADR's contract; in-band tokens refresh once and
  propagate to every pinned broker).
- Anti-thrash (ADR-0028): each pool entry carries its own
  `AntiThrashState` (same supervisor config as the bootstrap). A
  broker rolling restart only hits its own pool entry.
- Supervised reconnect: each pool entry has its own supervised driver
  loop, reconnect target is the pool entry's `physical` URL. For
  DIRECT entries that means reconnects re-dial the broker directly —
  the supervisor does not consult `service_url_provider` on the
  per-broker entry because the broker URL is what the lookup resolved
  to, not what the failover provider would emit. This matches Java's
  behaviour: cluster failover replaces the **bootstrap** URL only, not
  the per-broker pool entries (which are torn down with the client).

### Moonpool engine

Moonpool's `LookupTarget` mirror grows the same `Option<String>` field
on the `Direct` arm. `lookup_topic_target` captures the broker URL
identically. The async `resolve_target` (its `Send`-safety is hoisted
into a `spawn_task`-detached dial — see the moonpool pool docs) routes
DIRECT-with-a-broker-URL through the same
[`pool::get_or_open(logical, physical, /* proxy_to_broker_url = */ None)`](../../crates/magnetar-runtime-moonpool/src/pool.rs)
surface used by the proxy path. The bootstrap-equality fast path
mirrors the tokio engine: if the resolved `host:port` matches the
bootstrap's, the bootstrap connection is reused (no extra dial). For
unsupervised clients (`Client::connect_plain` / `Client::from_parts`)
the pool is absent so DIRECT degrades to bootstrap-only with a
`tracing::warn!`. Same `(logical, physical)` +
`proxy_to_broker_url: Option<String>` parameterisation as the tokio
pool — both engines pick their entries via identical keys.

### Tests (ADR-0024 four-layer matrix)

- **Proto unit** — the proto layer already correlates LOOKUP requests
  with their responses and surfaces `broker_service_url` /
  `proxy_through_service_url` verbatim. No new unit test added on the
  proto side; the existing `lookup.rs` tests cover the response decode.
  Justified in the commit body.
- **Tokio integration** —
  `crates/magnetar-runtime-tokio/tests/lookup_direct_multi_broker.rs`:
  two-broker in-process fake (broker A redirects, broker B serves),
  asserts:
  1. Producer / subscribe land on broker B's pinned pool entry, not on
     the bootstrap A.
  2. Pinned CONNECT to B carries **no** `proxy_to_broker_url`.
  3. Second producer to the same topic reuses the pool entry (single B
     session for both).
  4. When the lookup resolves to the bootstrap broker itself, the
     bootstrap-equality fast path bypasses the pool (single session).
- **Moonpool integration** —
  `crates/magnetar-runtime-moonpool/tests/lookup_direct_multi_broker.rs`:
  1:1 mirror of the tokio integration test (two in-process brokers,
  bootstrap A redirects to broker B). Asserts the moonpool runtime opens
  the second TCP session to B with no `proxy_to_broker_url`, that two
  producers reuse one pinned pool entry, and that the bootstrap-equality
  fast path bypasses the pool when the lookup resolves to the bootstrap
  broker itself.
- **Differential** —
  `crates/magnetar-differential/tests/lookup_direct_multi_broker_equivalence.rs`:
  both engines decode the same DIRECT-with-broker-URL LOOKUP response
  to the same `OpOutcome::LookupResponse { LookupOutcome::Connect { … }
}` (the proto-level invariant the runtime decision rides on).
- **E2E** —
  `crates/magnetar/tests/e2e_lookup_direct_multi_broker.rs`: drives
  `PulsarClient::open_producer` + `subscribe` against a real Pulsar 4
  standalone broker. Pulsar 4 standalone is single-broker but its
  lookups advertise `broker_service_url`, so this exercises the
  bootstrap-equality fast path on the real broker code path. A
  multi-broker cluster fixture is out of scope for the per-PR e2e
  budget; the in-process broker pair in the tokio integration test
  reproduces the cross-broker wire behaviour Pulsar 3+ cluster mode
  exhibits.

### References

- `crates/magnetar-runtime-tokio/src/client.rs` — `LookupTarget`,
  `lookup_topic`, `resolve_target`, `resolve_direct_broker`,
  `direct_broker_url`, `parse_direct_broker_url`.
- `crates/magnetar-runtime-tokio/src/pool.rs` — generalised
  `get_or_open` accepting `proxy_to_broker_url: Option<String>`.
- `crates/magnetar-runtime-moonpool/src/client.rs` — mirror
  `LookupTarget` shape + `resolve_direct_broker` (bootstrap-equality
  fast path).
- `crates/magnetar-runtime-moonpool/src/pool.rs` — generalised
  `get_or_open` (matching tokio surface, `(logical, physical,
proxy_to_broker_url: Option<String>)`).
- Upstream:
  [`BinaryProtoLookupService.findBroker`](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/BinaryProtoLookupService.java),
  [`ConnectionPool.getConnection`](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConnectionPool.java),
  [`ConnectionHandler.connectionOpened`](https://github.com/apache/pulsar/blob/master/pulsar-client/src/main/java/org/apache/pulsar/client/impl/ConnectionHandler.java).
