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
The higher-level façade surfaces (partitioned, multi-topics, pattern,
reader, table-view, transactions, typed schemas) were lifted to be
engine-generic over `E: Engine`, so they build on both engines; only a
few narrow tokio-only specialisations remain. See
[`parity-status.md`](parity-status.md) for the authoritative
per-feature, per-engine snapshot.

## Producer + consumer façades

[`magnetar-runtime-moonpool::Producer<P>`](../crates/magnetar-runtime-moonpool/src/producer.rs)
and
[`magnetar-runtime-moonpool::Consumer<P>`](../crates/magnetar-runtime-moonpool/src/consumer.rs)
mirror their tokio counterparts. The two engines share the same
sans-io state machine, so the public method shape (send / flush /
close / stats / ack variants / nack / seek / pause / DLQ drain) is
identical. The difference is which `now: Instant` source the engine
snapshots at the call site and which byte pipe carries the wire bytes.

## PIP-4 message-crypto bridge

The moonpool engine ships the PIP-4 end-to-end encryption bridge,
mirroring the tokio engine exactly
([ADR-0044](../specs/adr/0044-moonpool-message-crypto-bridge.md)).
[`crypto.rs`](../crates/magnetar-runtime-moonpool/src/crypto.rs) defines
the engine's `MessageEncryptor` / `MessageDecryptor` traits +
`EncryptError`, the moonpool counterparts of
`magnetar-runtime-tokio::crypto`. The façade's `MessageCryptoBridge`
([`crates/magnetar/src/crypto_bridge.rs`](../crates/magnetar/src/crypto_bridge.rs))
implements **both** engines' trait pairs over
`magnetar-messagecrypto::MessageCrypto`, so the same bridge value plugs
into either engine's builders.

- **Producer (encrypt-on-send).** The moonpool producer encrypts the
  payload, stamping `pb::MessageMetadata` `encryption_keys` /
  `encryption_algo` / `encryption_param`. This mirrors the tokio
  producer's **compression → encryption** ordering for the encryption
  step; compression itself is not yet wired on the moonpool engine —
  non-`None` `CompressionKind` is refused on send until the runtime codec
  lands (M3) — so in practice the moonpool path is encrypt-only.
- **Consumer (decrypt-on-receive).** The moonpool consumer decrypts the
  payload — honoring the three `CryptoFailureAction` arms (`Fail`,
  `Discard`, `Consume`) identically to tokio — then delivers it. Because
  compression is refused on send, there is no decompression step to
  mirror: the receive path reduces to **decrypt, then deliver** (tokio's
  decrypt-first → decompress ordering, with the decompress branch a no-op
  on moonpool until codecs land).

The façade builders gain `.encryption()` / `.create_with_encryption()`
(producer) and `.encryption()` / `.subscribe_with_decryption()`
(consumer) for the moonpool engine, routing through the new
`Client::open_producer_with` / `Client::subscribe_with` entries. The
engine crypto API (`MessageEncryptorApi` / `MessageDecryptorApi`) is now
**non-stub for both engines**; `NoEncryption` is retained only as the
documented opt-out (the resolved API when no bridge is supplied), not as
the moonpool default. Equivalence is asserted through the differential
harness per [ADR-0024](../specs/adr/0024-cross-runtime-test-and-coverage-policy.md)
— see the [differential equivalence harness](#differential-equivalence-harness)
section and [`testing.md`](testing.md).

## Transport + vectored writes

The engine's transport adapter
([`crates/magnetar-runtime-moonpool/src/transport.rs`](../crates/magnetar-runtime-moonpool/src/transport.rs))
drives the `moonpool_core::NetworkProvider::TcpStream` directly. As of
moonpool `main` (consumed via the temporary git dependency in
[ADR-0043](../specs/adr/0043-temporary-floating-moonpool-git-dep.md))
that stream bounds on the **`futures::io::{AsyncRead, AsyncWrite}`** ext
traits rather than `tokio::io` — `TokioNetworkProvider` wraps its
`tokio::net::TcpStream` in
[`tokio_util::compat::Compat`](https://docs.rs/tokio-util/latest/tokio_util/compat/struct.Compat.html)
to bridge the two ecosystems. The transport adapter therefore imports the
`futures::io` ext traits (`AsyncReadExt` / `AsyncWriteExt`) accordingly.

The read side carries a **reusable heap-backed scratch** (`read_scratch`,
a `Box<[u8]>` of `TLS_WIRE_BUFFER` bytes allocated once per `Transport`
via `new_read_scratch()`): `read_into` lands wire bytes into it / the
caller's spare capacity instead of heap-allocating a fresh 16 KiB buffer
on every read. The scratch lives on the heap rather than as a stack array
so the returned read future stays small (a stack array tripped clippy's
`large_futures`). Perf-only — no behaviour or wire change.

The driver dispatches the sans-io `TransmitOwned` descriptor
([ADR-0040](../specs/adr/0040-vectored-io-transmit-enum.md)) as follows:

| `TransmitOwned` arm | Transport | Behaviour |
| --- | --- | --- |
| `Vectored` on the **plaintext** path under `SimProviders` | `futures::io::AsyncWriteExt::write_vectored` over `SimTcpStream` | **Segment-granular.** moonpool records each `IoSlice` as its own ordered delivery event, with `writev`-style partial-accept semantics — the chaos pack can drop / re-order individual segments. |
| `Vectored` on the **plaintext** path under `TokioProviders` | `futures::io::write_vectored` over the `Compat` wrapper | **Single-write fallback.** The `Compat` stream does not forward vectored writes (`is_write_vectored()` is `false`), so the slices collapse to one buffer write. Byte-identical wire output, no syscall reduction. |
| `Contiguous` (handshake, small frames) | single-buffer `write_all` | unchanged. |
| `Vectored` on the **TLS** path | `Transport::write_all_vectored` coalesces, then writes ciphertext | **Always contiguous.** The TLS arm still *receives* the segment list, but pushes each segment's plaintext through rustls in order and ships one ciphertext stream — rustls owns its own record buffering, so segment boundaries cannot survive encryption. See the TLS adapter section below. |

This replaces the earlier placeholder that coalesced the `Vectored`
segment list into one contiguous `write_all` "until moonpool-core adds
vectored support" — that prerequisite is now satisfied (ADR-0040 wave 2,
[PierreZ/moonpool#111](https://github.com/PierreZ/moonpool/issues/111) /
[PR #113](https://github.com/PierreZ/moonpool/pull/113)).

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

The TLS write path is **always contiguous**, including for producer
batches the plaintext path would emit as a `Vectored` segment list
([ADR-0040](../specs/adr/0040-vectored-io-transmit-enum.md)): rustls
buffers and frames its own records, so per-segment boundaries cannot
survive encryption. The driver still dispatches `Vectored` to
`Transport::write_all_vectored` for TLS connections, but the TLS arm
coalesces the segment list — pushing each segment's plaintext through
rustls in order — before shipping one ciphertext stream. The
segment-granular `write_vectored` benefit therefore applies to the
plaintext arm only — see the
[Transport + vectored writes](#transport--vectored-writes) table.

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
| Supervised redial under a drop → accept → drop → accept cycle (anti-thrash cooldown + multi-attempt redial body) | [`supervised_redial.rs`](../crates/magnetar-runtime-moonpool/tests/supervised_redial.rs) (mirror: [tokio side](../crates/magnetar-runtime-tokio/tests/supervised_redial.rs)) |
| Stateful broker + invariant assertions (D2 chaos pack) | [`sim_chaos.rs`](../crates/magnetar-runtime-moonpool/tests/sim_chaos.rs) |
| Targeted ADR-0024 coverage closure for `src/{driver,producer,consumer,lib,transport}.rs` | [`coverage_close.rs`](../crates/magnetar-runtime-moonpool/tests/coverage_close.rs) (mirror: [tokio side](../crates/magnetar-runtime-tokio/tests/coverage_close.rs)) |

Since the engine dispatches plaintext producer batches through real
`write_vectored` (see
[Transport + vectored writes](#transport--vectored-writes)), the chaos
pack now operates at **segment granularity** on the plaintext arm:
`SimTcpStream` records each `IoSlice` as its own ordered delivery event
with `writev`-style partial-accept semantics, so per-segment drop /
re-order / short-write modelling is available where the pack previously
saw only one coalesced write. The TLS arm stays contiguous, so its chaos
fidelity is unchanged (rustls owns record buffering).

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
| [`broker.rs`](../crates/magnetar-differential/src/broker.rs) | Scripted in-process Pulsar broker speaking a minimal subset of the wire protocol: CONNECT/CONNECTED, PRODUCER/PRODUCER_SUCCESS, SEND/SEND_RECEIPT, SUBSCRIBE/SUCCESS, pushed MESSAGE, ACK/ACK_RESPONSE, SEEK/SUCCESS, CLOSE_PRODUCER/CLOSE_CONSUMER. Round-trips PIP-4 `MessageMetadata` encryption fields verbatim (mirroring a real broker's PIP-4 opacity). |
| [`trace.rs`](../crates/magnetar-differential/src/trace.rs) | `Trace` (operations) and `EventStream` (user-visible outcomes). |
| [`runner_tokio.rs`](../crates/magnetar-differential/src/runner_tokio.rs) | Runs a trace against `magnetar-runtime-tokio` bound to `127.0.0.1`. |
| [`runner_moonpool.rs`](../crates/magnetar-differential/src/runner_moonpool.rs) | Runs the same trace against `magnetar-runtime-moonpool` with `TokioProviders`. |
| [`tests/golden_traces.rs`](../crates/magnetar-differential/tests/golden_traces.rs) | Asserts the two engines produce equivalent event streams on the shipped golden traces. |
| [`tests/crypto_roundtrip_equivalence.rs`](../crates/magnetar-differential/tests/crypto_roundtrip_equivalence.rs) | PIP-4 encrypted round-trip parity across both engines ([ADR-0044](../specs/adr/0044-moonpool-message-crypto-bridge.md)). |
| [`tests/crypto_failure_action_equivalence.rs`](../crates/magnetar-differential/tests/crypto_failure_action_equivalence.rs) | The 3-arm `cryptoFailureAction` matrix (Fail / Discard / Consume), pinned by golden trace [`tests/golden/crypto_failure_action.json`](../crates/magnetar-differential/tests/golden/crypto_failure_action.json). |

The moonpool runner uses `TokioProviders` rather than
`SimProviders`. `moonpool-sim` is now a workspace dependency (pulled in
for the chaos pack via the git `main` float —
[ADR-0043](../specs/adr/0043-temporary-floating-moonpool-git-dep.md)).
The harness still exercises the engine surface that diverges between
tokio and moonpool (memory-limit policy plumbing, future shapes, generic
bounds) which is the load-bearing part for equivalence.

Equivalence holds across the vectored-write change because the
comparison is on wire bytes + user-visible events, not syscall shape:
under `TokioProviders` the moonpool transport's `Compat` stream does not
forward vectored writes (it collapses the `Vectored` segment list to a
single buffer write — see
[Transport + vectored writes](#transport--vectored-writes)), so it emits
byte-identical wire output to the tokio engine's `write_all`. The
segment-granular delivery events are a `SimProviders`-only refinement
and do not perturb the `TokioProviders`-backed differential trace.

The harness ships per [ADR-0019](../specs/adr/0019-engine-scope-and-moonpool-parity.md)
M8. The moonpool runner awaits the engine work directly — no
`tokio::task::LocalSet` wrapper and no periodic pump. The pinned
moonpool `main` ships a `Send`-bound `TaskProvider::spawn_task`
(`TokioProviders` wires `TokioTaskProvider`, which spawns via
`tokio::task::Builder::new().spawn(...)`, **not** `spawn_local`), so the
driver task runs on the ambient runtime and a parked `consumer.receive()`
is woken normally through its `Notify`/`Waker` slab. The earlier
`LocalSet` + 25 ms `Kicker` pump were tied to a stale `spawn_local`
premise and have been removed (the floating-`main` dependency is recorded
in [ADR-0043](../specs/adr/0043-temporary-floating-moonpool-git-dep.md)).

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
