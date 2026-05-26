# Moonpool Engine

[`magnetar-runtime-moonpool`](../crates/magnetar-runtime-moonpool) is
the deterministic-simulation engine. It drives the same sans-io
`magnetar-proto::Connection` state machine as the tokio engine; only
the I/O and clock plumbing differs.

This document covers the engine's surface, supervised reconnect path,
TLS adapter, chaos test pack, and the differential equivalence harness
that proves it stays in lockstep with the tokio engine.

For the production engine and the workspace-wide architecture, see
[`architecture-overview.md`](architecture-overview.md) and
[`../ARCHITECTURE.md`](../ARCHITECTURE.md).

## What moonpool is

[`moonpool-sim`](https://crates.io/crates/moonpool-sim) is a
deterministic simulation engine. Application code talks to
[`moonpool_core::Providers`], a bundle of:

- `NetworkProvider` — TCP-shaped byte pipes.
- `TimeProvider` — virtual or wall-clock time.
- `TaskProvider` — task spawning.
- `RandomProvider` — seeded RNG.
- `StorageProvider` — file I/O.

Under simulation each provider is virtualised so a given seed replays
bit-for-bit. `magnetar-runtime-moonpool` plugs the engine onto a
`Providers` bundle of the caller's choosing:

| Provider bundle | Use |
| --- | --- |
| [`moonpool_core::TokioProviders`] | Production-style runs against a real broker. Wall-clock time, real network, real RNG. |
| `moonpool-sim::SimProviders` | Reproducible chaos under a seed. Virtual clock, scripted network, seeded RNG. |

The crate has no `moonpool-sim` dependency itself — the sim bundle is
plugged in by the caller.

## Engine surface

[`MoonpoolEngine<P: Providers>`](../crates/magnetar-runtime-moonpool/src/lib.rs)
exposes these entries:

| Method | Role |
| --- | --- |
| `MoonpoolEngine::new(providers: P)` | Construct the engine over a `Providers` bundle. |
| `connect_plain(addr, config)` | Plain TCP connect + handshake. Returns `(Arc<ConnectionShared>, DriverHandle)`. |
| `connect_plain_with_resolver(addr, config, resolver)` | Plain TCP via injected `DnsResolver`. |
| `connect_tls(addr, server_name, tls_config, config)` | TLS via the in-crate `rustls` byte-pipe adapter ([`tls.rs`](../crates/magnetar-runtime-moonpool/src/tls.rs)). |
| `connect_plain_supervised(addr, config, service_url_provider, reconnect)` | Plain TCP wrapped in the supervised reconnect loop. |

The user-facing client lives at
[`magnetar-runtime-moonpool::Client<P>`](../crates/magnetar-runtime-moonpool/src/client.rs),
mirroring the tokio engine's `Client` surface: `connect_plain`,
`connect_plain_supervised`, partitioned-metadata lookup, transaction
coordinator helpers, `is_connected`, `close`.

At the façade layer the engine is selected via the `Engine` marker
trait, so `PulsarClient<MoonpoolEngine<P>>` is the canonical public
type ([ADR-0019](../specs/adr/0019-engine-scope-and-moonpool-parity.md)).
The façade surface that lives on `PulsarClient<TokioEngine>` only
(partitioned, multi-topics, pattern, reader, table-view, transactions,
typed schemas) does not compile against the moonpool engine — that
gap is tracked in [`parity-status.md`](parity-status.md).

## Producer + consumer façades

[`magnetar-runtime-moonpool::Producer<P>`](../crates/magnetar-runtime-moonpool/src/producer.rs)
and
[`magnetar-runtime-moonpool::Consumer<P>`](../crates/magnetar-runtime-moonpool/src/consumer.rs)
mirror their tokio counterparts. The two engines share the same
sans-io state machine, so the public method shape (send / flush /
close / stats / ack variants / nack / seek / pause / DLQ drain) is
identical. The difference is which `now: Instant` source the engine
snapshots at the call site and which byte pipe carries the wire bytes.

## Supervised reconnect

The moonpool driver loop mirrors the tokio supervisor exactly. See
[`architecture-overview.md#driver-loop`](architecture-overview.md#driver-loop)
for the shared algorithm. Specifics for the moonpool engine:

- Backoff is driven by `moonpool_core::TimeProvider::sleep_until` —
  under `SimProviders` this advances the virtual clock deterministically.
- DNS is re-resolved on every attempt through the injected
  `DnsResolver`. The crate ships `StaticDnsResolver` and an
  `arc_dns_resolver` helper.
- The `ServiceUrlProvider` is consulted on every attempt before
  `Transport::connect`, so `ControlledClusterFailover` plugs straight
  in (see PIP-121 below).
- After re-handshake the engine calls
  `Connection::rebuild_producers(now)` and
  `Connection::rebuild_consumers(now)` to re-issue `CommandProducer` /
  `CommandSubscribe` for every still-open handle.

## TLS adapter

The moonpool engine cannot use `tokio-rustls` — `tokio-rustls` needs a
real socket. Instead it drives a sans-io
`rustls::ClientConnection` by hand over the byte pipe supplied by
`moonpool_core::NetworkProvider`. The adapter lives at
[`crates/magnetar-runtime-moonpool/src/tls.rs`](../crates/magnetar-runtime-moonpool/src/tls.rs)
and follows the standard rustls "drive it yourself" pattern:

```text
socket.read(buf)                  →  session.read_tls(buf)
                                  →  session.process_new_packets()
                                  →  session.reader().read_to_end(plaintext_in)
plaintext_out                     →  session.writer().write_all(...)
                                  →  session.write_tls(socket_out)
socket.write_all(socket_out)
```

The handshake therefore stays deterministic under `SimProviders` chaos
(connection drops, partial reads, virtual-clock timeouts). The
adapter never blocks on a network call inside `process_new_packets` —
reads and writes go through the byte pipe under simulation control.

See [ADR-0006](../specs/adr/0006-moonpool-tls-byte-pipe.md) for the
binding decision.

## ServiceUrlProvider plumbing (PIP-121)

The supervised reconnect path consults the configured
`ServiceUrlProvider` on every attempt. Two implementations live in
`magnetar-proto` (and are therefore usable by both engines):

- `StaticServiceUrlProvider` — single URL, never changes.
- `ControlledClusterFailover` — `Arc<Mutex<String>>` swappable at
  runtime via `set_url(...)`. Tests or sidecars drive failover by
  swapping the URL between reconnects.

`AutoClusterFailover<P>` (PIP-121 health-probe-driven) ships on the
moonpool engine as well — the probe loop runs on `P::TaskProvider`,
so the simulator drives the schedule deterministically with no real
DNS or TCP. Source:
[`crates/magnetar-runtime-moonpool/src/auto_cluster_failover.rs`](../crates/magnetar-runtime-moonpool/src/auto_cluster_failover.rs).

## PIP-188 TOPIC_MIGRATED

`magnetar-proto::Connection::handle_bytes` decodes
`CommandTopicMigrated` and emits `ConnectionEvent::TopicMigrated` on the
event queue. The moonpool driver consumes the event, logs the new-URL
hint, and returns an error from `driver_loop_inner` — exactly the
mechanism used by the tokio engine. The supervisor catches the error,
calls `Connection::reset()`, and reconnects against the migrated
broker. See
[ADR-0018](../specs/adr/0018-pip-188-reconnect-on-migrate.md).

## Deterministic chaos pack

[`crates/magnetar-runtime-moonpool/tests/`](../crates/magnetar-runtime-moonpool/tests/)
ships a chaos test pack that exercises the supervisor + reconnect +
PIP-121 + PIP-188 paths under deterministic seeds. Tests are normal
`cargo test` integration targets — no Docker, no live broker.

| Scenario | Test |
| --- | --- |
| Mid-handshake network partition | [`mid_handshake_partition.rs`](../crates/magnetar-runtime-moonpool/tests/mid_handshake_partition.rs) |
| Out-of-order frame delivery | [`frame_reorder.rs`](../crates/magnetar-runtime-moonpool/tests/frame_reorder.rs) |
| OAuth2 token refresh edge cases | [`oauth_refresh_edge.rs`](../crates/magnetar-runtime-moonpool/tests/oauth_refresh_edge.rs) |
| PIP-121 oscillation (primary → standby → primary) | [`pip_121_oscillation.rs`](../crates/magnetar-runtime-moonpool/tests/pip_121_oscillation.rs) |
| PIP-188 migrate-then-migrate-again | [`pip_188_migrate_then_migrate_again.rs`](../crates/magnetar-runtime-moonpool/tests/pip_188_migrate_then_migrate_again.rs) |
| Reconnect with in-flight publishes | [`reconnect_with_inflight.rs`](../crates/magnetar-runtime-moonpool/tests/reconnect_with_inflight.rs) |
| Virtual-clock ack-timeout fires | [`virtual_clock_ack_timeout.rs`](../crates/magnetar-runtime-moonpool/tests/virtual_clock_ack_timeout.rs) |
| Virtual-clock send-timeout fires | [`virtual_clock_send_timeout.rs`](../crates/magnetar-runtime-moonpool/tests/virtual_clock_send_timeout.rs) |
| ADR-0028 anti-thrash policy (broker ack-then-drop cascade) | [`anti_thrash.rs`](../crates/magnetar-runtime-moonpool/tests/anti_thrash.rs) |
| Stateful broker + invariant assertions (D2 chaos pack) | [`sim_chaos.rs`](../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs) |
| Targeted ADR-0024 coverage closure for `src/{driver,producer,consumer,lib,transport}.rs` | [`coverage_close.rs`](../crates/magnetar-runtime-moonpool/tests/coverage_close.rs) (mirror: [tokio side](../crates/magnetar-runtime-tokio/tests/coverage_close.rs)) |

Reproduce a flaky run under a specific seed:

```bash
MOONPOOL_SEED=0xdeadbeefcafebabe \
  cargo test -p magnetar-runtime-moonpool --all-features --locked -- --nocapture
```

Sweep a range of seeds locally:

```bash
for seed in $(seq 1 32); do
  MOONPOOL_SEED=$seed cargo test -p magnetar-runtime-moonpool \
    --all-features --locked -- --quiet || echo "seed $seed FAILED"
done
```

In CI, the per-PR / per-push pipeline
([`.github/workflows/ci.yml`](../.github/workflows/ci.yml)) exercises
the moonpool suite under the default seed via the regular `test` job.
A dedicated
[`moonpool-seed-sweep.yml`](../.github/workflows/moonpool-seed-sweep.yml)
workflow runs **daily** with **16 freshly-rolled random `u64` seeds in
parallel** — see
[ADR-0036](../specs/adr/0036-moonpool-seed-sweep-daily-random.md) for
the rationale (fixed seeds in per-PR CI are wasted compute since each
`(commit, seed)` pair is bit-for-bit reproducible; random seeds rolled
daily cover the seed space far better over time). Failing seeds are
echoed in the run summary — reproduce locally with
`MOONPOOL_SEED=<hex> cargo test -p magnetar-runtime-moonpool …`.

## Differential equivalence harness

[`magnetar-differential`](../crates/magnetar-differential) is a
test-only crate that runs a producer/consumer
[`Trace`](../crates/magnetar-differential/src/trace.rs) (a sequence of
operations — connect, open producer, send, subscribe, receive, ack,
seek, close) against **both engines** and compares the user-visible
`EventStream`s for equivalence.

The harness components:

| File | Role |
| --- | --- |
| [`broker.rs`](../crates/magnetar-differential/src/broker.rs) | Scripted in-process Pulsar broker speaking a minimal subset of the wire protocol: CONNECT/CONNECTED, PRODUCER/PRODUCER_SUCCESS, SEND/SEND_RECEIPT, SUBSCRIBE/SUCCESS, pushed MESSAGE, ACK/ACK_RESPONSE, SEEK/SUCCESS, CLOSE_PRODUCER/CLOSE_CONSUMER. |
| [`trace.rs`](../crates/magnetar-differential/src/trace.rs) | `Trace` (operations) and `EventStream` (user-visible outcomes). |
| [`runner_tokio.rs`](../crates/magnetar-differential/src/runner_tokio.rs) | Runs a trace against `magnetar-runtime-tokio` bound to `127.0.0.1`. |
| [`runner_moonpool.rs`](../crates/magnetar-differential/src/runner_moonpool.rs) | Runs the same trace against `magnetar-runtime-moonpool` with `TokioProviders`. |
| [`tests/golden_traces.rs`](../crates/magnetar-differential/tests/golden_traces.rs) | Asserts the two engines produce equivalent event streams on the shipped golden traces. |

The moonpool runner uses `TokioProviders` rather than
`SimProviders` — once `moonpool-sim` is vendored as a workspace
dependency, swapping the provider bundle in the runner is a one-line
change. The harness still exercises the engine surface that diverges
between tokio and moonpool (memory-limit policy plumbing, future
shapes, generic bounds) which is the load-bearing part for
equivalence.

The harness ships per [ADR-0019](../specs/adr/0019-engine-scope-and-moonpool-parity.md)
M8. The remaining structural caveat — the moonpool runner's
`spawn_local` driver requires a 25 ms `Kicker` to bridge the
`LocalSet` pump gap — is tracked in
[`follow-ups.md`](follow-ups.md) §"Differential equivalence harness"
and closes once the moonpool-sim provider lands.

## What is *not* yet exercised under simulation

- **Property-based seed sweeps** in per-PR CI: the per-PR pipeline runs
  the test binary on the moonpool default seed only. Multi-seed
  scheduling is covered by the daily 16-random-seed sweep
  ([ADR-0036](../specs/adr/0036-moonpool-seed-sweep-daily-random.md)),
  not by per-PR CI.
- **TLS handshake byte-level chaos** (corrupted handshake records) is
  not yet swept; handshake correctness is verified but adversarial
  byte mutations are open work.
- **Transparent in-flight publish replay** across reconnect: the
  sans-io machinery is there (`Connection::reset`, epoch bump, rebuild
  plumbing) but the engine surfaces `OpOutcome::SessionLost` rather
  than re-queueing the unconfirmed sends. Stage 3 follow-up.

Tracked in [`follow-ups.md`](follow-ups.md).
