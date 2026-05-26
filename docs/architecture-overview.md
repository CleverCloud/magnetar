# Architecture Overview

A bird's-eye view of the magnetar workspace. For the deep dive (state
machines, trackers, wire framing, schema canonicalisation) read
[`../ARCHITECTURE.md`](../ARCHITECTURE.md). For the binding decisions,
read the [ADR series](../specs/adr/).

## Crate topology

```
crates/
  magnetar/                       Public façade — PulsarClient<E>, builders, typed schemas, partitioned / multi-topics / pattern / reader / table-view / interceptors
  magnetar-proto/                 Sans-io state machine + codec + trackers + topic watcher (zero I/O deps)
  magnetar-runtime-tokio/         Production engine (TCP, tokio-rustls, supervised reconnect)
  magnetar-runtime-moonpool/      Deterministic-simulation engine over moonpool_core::Providers (rustls byte-pipe)
  magnetar-admin/                 reqwest-backed REST admin client (rustls-tls)
  magnetar-cli/                   `magnetar` binary
  magnetar-fakes/                 In-process broker stub for tests
  magnetar-messagecrypto/         PIP-4 AES-GCM (aws-lc-rs)
  magnetar-auth-oauth2/           ClientCredentialsFlow + token caching
  magnetar-auth-sasl/             SASL PLAIN + Kerberos/GSSAPI (libgssapi behind `kerberos` feature)
  magnetar-auth-athenz/           Athenz pre-fetched role token (ZTS round-trip deferred)
  magnetar-differential/          tokio ↔ moonpool differential equivalence harness (test-only)
xtask/                            Workspace automation (check-no-channels, check-no-io-deps, check-no-internal-clock, codegen)
```

The dependency direction is strictly downward:

```
magnetar-cli ──> magnetar-admin
            └──> magnetar ──> magnetar-runtime-tokio    ──┐
                          ├──> magnetar-runtime-moonpool ──┤
                          ├──> magnetar-auth-{oauth2,sasl,athenz}
                          └──> magnetar-messagecrypto    ──┤
                                                           v
                                                    magnetar-proto
```

`magnetar-proto` is the only mandatory dependency for every other
crate. Engine, auth, and crypto crates implement traits owned by
`magnetar-proto` and the façade. Feature flags on `magnetar` gate
which engine and which auth providers compile in
([`../README.md#installation`](../README.md#installation)).

## Sans-io invariants

The crate split is enforced, not aspirational. Five rules sit at the
heart of the architecture; each one is wired into a CI gate and each
has a corresponding ADR.

| Invariant | ADR | Enforcement |
| --- | --- | --- |
| `magnetar-proto` has zero I/O deps | [ADR-0004](../specs/adr/0004-sans-io-protocol-core.md) | `cargo xtask check-no-io-deps` |
| No channel crates anywhere | [ADR-0003](../specs/adr/0003-no-channels-rule.md) | `cargo xtask check-no-channels` + `clippy.toml::disallowed-types` + `cargo deny bans` |
| `magnetar-proto` does not read the host clock | [ADR-0011](../specs/adr/0011-clock-injection-sans-io.md) | `cargo xtask check-no-internal-clock` |
| Generated proto code stays in lockstep with the vendored `.proto` | [ADR-0004](../specs/adr/0004-sans-io-protocol-core.md) | `cargo xtask codegen --check` |
| `rustls` only (openssl admitted only via `rustls-openssl`) | [ADR-0005](../specs/adr/0005-rustls-only-tls.md) amended by [ADR-0035](../specs/adr/0035-pluggable-crypto-provider.md) | `deny.toml` bans `native-tls`; `openssl` / `openssl-sys` scoped via `wrappers = ["rustls-openssl"]` |
| Pluggable rustls crypto provider (aws-lc-rs / ring / openssl / fips) | [ADR-0035](../specs/adr/0035-pluggable-crypto-provider.md) | `cargo xtask check-crypto-matrix` + cfg-cascade `compile_error!` |

The clock-injection check has two documented leak sites in
[`crates/magnetar-proto`](../crates/magnetar-proto): the PIP-37
chunk-set `uuid::Uuid::new_v4()` in
[`producer.rs`](../crates/magnetar-proto/src/producer.rs) and the
one-shot `std::env::var()` bootstrap in
[`auth/token.rs`](../crates/magnetar-proto/src/auth/token.rs). Both are
allowlisted in `xtask/src/main.rs::CLOCK_LEAK_ALLOWLIST` and listed in
[`../ARCHITECTURE.md#known-non-determinism-leaks-documented`](../ARCHITECTURE.md#known-non-determinism-leaks-documented).

## Engine boundary

`PulsarClient<E: Engine = TokioEngine>` is generic over an `Engine`
marker trait that selects per-engine storage
([`crates/magnetar/src/engine.rs`](../crates/magnetar/src/engine.rs)).
Two engines ship:

- `TokioEngine` — production default. Pulls in `tokio` +
  `tokio-rustls`. One driver task per connection. Lives in
  [`magnetar-runtime-tokio`](../crates/magnetar-runtime-tokio).
- `MoonpoolEngine<P>` — deterministic-simulation engine, generic over a
  `moonpool_core::Providers` bundle. Lives in
  [`magnetar-runtime-moonpool`](../crates/magnetar-runtime-moonpool).
  Covered in detail in [`moonpool-engine.md`](moonpool-engine.md).

Engine-specific methods (`producer`, `consumer`, partitioned, …) live
in concrete `impl PulsarClient<TokioEngine>` / `impl
PulsarClient<MoonpoolEngine<P>>` blocks rather than on the trait. The
connect signatures differ enough (URL vs. `host:port` + providers
bundle) that a single `Engine::connect(...)` would either lose
typing or reintroduce per-engine duplication
([ADR-0019](../specs/adr/0019-engine-scope-and-moonpool-parity.md)
§"Option B rejected").

The façade surface that lives on `PulsarClient<TokioEngine>` only
(partitioned, multi-topics, pattern, reader, table-view, transactions,
typed schemas) yields a clean compile error when called against
`PulsarClient<MoonpoolEngine<P>>` rather than a silent fallback.

## Driver loop

Each engine runs one driver task per `Connection`. The loop, common to
both engines:

```
loop {
    let (write_buf, deadline, closing) = lock(state) {
        conn.poll_transmit(&mut write_buf);
        conn.dispatch_pending_event_wakers();
        let deadline = conn.poll_timeout();
        (write_buf, deadline, conn.is_closing())
    };
    if !write_buf.is_empty() { socket.write_all(&write_buf).await?; }
    if closing { socket.shutdown().await?; return Ok(()); }

    tokio::select! { biased;
        _ = shared.driver_waker.notified() => {}            // user submitted op
        r = socket.read_buf(&mut read_buf) => {
            let bytes = r?;
            lock(state).handle_bytes(engine.now(), &bytes);
        }
        _ = engine.sleep_until_opt(deadline), if deadline.is_some() => {
            lock(state).handle_timeout(engine.now());
        }
    }
}
```

The lock is `parking_lot::Mutex`, taken in short non-`.await` critical
sections. The driver is woken via a single `tokio::sync::Notify` cell
(`Notify` is a condvar, not a channel; per
[ADR-0003](../specs/adr/0003-no-channels-rule.md)).

When `driver_loop_inner` returns due to an error, a supervisor wraps
it and reconnects:

1. Record disconnect via `Connection::mark_disconnected(now, wall_now)`.
2. Reset the state machine with `Connection::reset()` — bumps the
   `session_epoch`, drains pending-op slabs, and surfaces
   `OpOutcome::SessionLost` to every in-flight user future.
3. Back off with a small jittered exponential schedule capped by
   `ReconnectConfig::max_backoff`.
4. Reconnect through `Transport::connect` (re-resolving via the
   configured `ServiceUrlProvider` on every attempt — this is where
   PIP-121 plugs in).
5. Rebuild producers and consumers via
   `Connection::rebuild_producers(now)` and
   `Connection::rebuild_consumers(now)`. Each helper re-emits
   `CommandProducer` / `CommandSubscribe` for every still-open handle.
   User-facing futures stay registered; they resume when the broker
   re-issues the producer/consumer IDs.

PIP-188 `TOPIC_MIGRATED` reuses the same path: the driver surfaces a
`ConnectionEvent::TopicMigrated`, returns an error from
`driver_loop_inner`, and the supervisor performs the reset +
reconnect against the new URL.

[ADR-0028](../specs/adr/0028-supervised-reconnect-anti-thrash-policy.md)
layers an **opt-in anti-thrash policy** on top of the supervisor:
a per-`Connection` ring records each `ReAttachOk` outcome and any
TCP drop that follows within `drop_grace`; once `N` re-attaches in
a sliding window of `M` all get dropped within `K` ms, the
supervisor honours an `AntiThrashCooldown { until }` event and
sleeps until the cooldown clears before the next
`Transport::connect`. Default off
(`SupervisorConfig::anti_thrash_threshold: None`). Full
explanation in
[`../ARCHITECTURE.md#anti-thrash-policy-opt-in-adr-0028`](../ARCHITECTURE.md#anti-thrash-policy-opt-in-adr-0028).

## TLS

Three TLS sites in the workspace; all use `rustls`:

1. **`magnetar-runtime-tokio`** drives `tokio_rustls::TlsConnector`.
   Roots come from `rustls-native-certs` by default; users override
   with `ClientBuilder::tls_trust_certs_pem` /
   `tls_trust_certs_file_path`.
2. **`magnetar-runtime-moonpool`** drives a sans-io
   `rustls::ClientConnection` by hand over the byte pipe supplied by
   `moonpool_core::NetworkProvider`. Source:
   [`crates/magnetar-runtime-moonpool/src/tls.rs`](../crates/magnetar-runtime-moonpool/src/tls.rs).
   See [ADR-0006](../specs/adr/0006-moonpool-tls-byte-pipe.md).
3. **`magnetar-admin`** uses `reqwest`. The `rustls-tls` vs
   `rustls-tls-no-provider` sub-feature is picked per `crypto-*`
   selection — `aws-lc-rs` uses `rustls-tls` (which resolves to
   aws-lc-rs internally), the other three providers use
   `rustls-tls-no-provider` and rely on the engine boot's
   `install_default_provider()` shim.

The hostname-verification knob
(`tls_hostname_verification_enable(false)`) is implemented in
[`tls_no_hostname.rs`](../crates/magnetar-runtime-tokio/src/tls_no_hostname.rs)
by wrapping `WebPkiServerVerifier` so chain verification still runs but
`NotValidForName` is intercepted. `tls_allow_insecure_connection(true)`
is a blanket override in
[`tls_insecure.rs`](../crates/magnetar-runtime-tokio/src/tls_insecure.rs).

### Pluggable crypto provider (issue #9, ADR-0035)

The rustls crypto primitives that back the handshake are selected at
compile time on the `magnetar` façade via four mutually-pluggable
features:

| Feature              | Backend                                           |
|----------------------|---------------------------------------------------|
| `crypto-aws-lc-rs`   | `aws-lc-rs` (default; brings X25519MLKEM768)      |
| `crypto-ring`        | `ring`                                            |
| `crypto-openssl`     | `rustls-openssl` (wraps system OpenSSL)           |
| `crypto-fips`        | `aws-lc-fips-sys` (FIPS-validated; needs cmake)   |

Both runtime crates carry a sibling `tls_crypto` module that exposes
`install_default_provider()` (idempotent) and `active_provider()`. The
four production callsites
(`tls_insecure.rs`, `tls_no_hostname.rs`, `transport.rs`, `client.rs`)
go through `active_provider()` rather than the historical
`CryptoProvider::get_default()` + `ring` fallback. Under
`--all-features` the cfg cascade resolves to aws-lc-rs.

`openssl` / `openssl-sys` are admitted only as transitive deps of
`rustls-openssl` via `deny.toml`'s `wrappers = ["rustls-openssl"]`
carve-out; the rest of [ADR-0005](../specs/adr/0005-rustls-only-tls.md)
(no `native-tls`, rustls everywhere) stays in force. See
[ADR-0035](../specs/adr/0035-pluggable-crypto-provider.md).

## Memory accounting

`ClientBuilder::memory_limit(bytes, MemoryLimitPolicy)` enforces a
global publish-bytes budget. Two policies ship:

- `FailImmediately` — atomic CAS reserve on `Producer::send`; overflow
  returns `MemoryLimitExceeded` synchronously. See
  [ADR-0017](../specs/adr/0017-memory-limit-atomic-reservation.md).
- `ProducerBlock` — overflow parks the `SendFut` on a `Waker` slab
  inside `ConnectionShared`; `release_memory` drains the slab and
  re-polls. See
  [ADR-0020](../specs/adr/0020-memory-limit-producer-block.md).

Full mechanics in [`memory-limit.md`](memory-limit.md).

## Auto-update tickers

Several Java client features rely on periodic background work
(`PatternConsumer` topic rediscovery, `TableView` partition tracking,
`PartitionedProducer`/`PartitionedConsumer`/`MultiTopicsConsumer`
partition-count updates). The pattern is uniform:

- The ticker spawns a `tokio::time::interval` task that signals a
  `Notify` on every tick.
- The runtime façade (`magnetar-runtime-tokio`) takes the `Instant::now()`
  snapshot at the call site and forwards it into
  `magnetar-proto::Connection` entries.
- `magnetar-proto` itself never reads the host clock — the
  `check-no-internal-clock` xtask enforces this.

The schedule API lives on the relevant builder
(`PartitionedProducerBuilder::auto_update_partitions_interval`,
`MultiTopicsConsumerBuilder::auto_update_partitions_interval`,
`TableViewBuilder::auto_update_partitions_interval`,
`PatternConsumer::start_auto_reconcile`).

## Receive-path classifiers

The `ConnectionEvent` stream is a single ordered queue, but the
receive dispatch in `magnetar-proto::Connection` runs a thin
classifier before emitting so callers see the most specific variant
that matches the inbound frame. Two features use this pattern:

- **Shadow-topic dispatch (PIP-180 / ADR-0033)** — when a consumer is
  shadow-attached via [`ConsumerState::set_shadow_metadata`](../crates/magnetar-proto/src/consumer.rs)
  AND the inbound `MessageMetadata.replicated_from` is populated,
  the classifier emits `ConnectionEvent::MessageReceivedFromShadow`
  in place of `ConnectionEvent::Message`. Regular (non-shadow)
  topics keep emitting `Message` — wire path is byte-identical to
  v0.1.0. Full surface in [`shadow-topic.md`](shadow-topic.md).
- **Replicated-subscription markers (PIP-33 / ADR-0034)** — markers
  carried in the payload of a `CommandMessage` with magic type
  `MarkerType::REPLICATED_SUBSCRIPTION_*` are intercepted by the
  consumer's receive path and re-emitted as
  `ConnectionEvent::ReplicatedSubscriptionMarkerObserved` rather
  than surfaced to user code as a regular `Message`. Full surface in
  [`replicated-subscriptions.md`](replicated-subscriptions.md).

Both classifiers stay sans-io: they read only the per-consumer
state cache (populated externally by the runtime engine at
subscribe time) and the inbound metadata. No I/O, no clock reads.

## Where the rules are

The binding rules are in:

- [`../GUIDELINES.md`](../GUIDELINES.md) — the workspace spec.
- [`../specs/adr/`](../specs/adr/) — one binding decision per file.
  Index in [`../specs/README.md`](../specs/README.md).

The full deep dive — sans-io rationale, driver loop, protocol state
machine, wire framing, producer batch/chunk paths, consumer trackers,
multi-topics fan-in, schemas, PIP coverage map — is in
[`../ARCHITECTURE.md`](../ARCHITECTURE.md).
