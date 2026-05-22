# Java Parity Status

The authoritative parity matrix lives in
[`../README.md#java-client-parity-matrix`](../README.md#java-client-parity-matrix).
This document gives the matrix its engine-by-engine cut and lists the
genuine deferred-scope items.

Per [ADR-0019](../specs/adr/0019-engine-scope-and-moonpool-parity.md),
the Java parity matrix is satisfied **by the tokio engine**. The
moonpool engine reaches feature parity with tokio on its own
follow-up train; the gap is tracked below.

## Engine surface

| Surface | tokio | moonpool |
| --- | --- | --- |
| Engine driver loop + transport scaffold | ✅ | ✅ |
| Client (lookup + partitioned-metadata + topic-watch) | ✅ | ✅ |
| Producer façade (send / flush / close) | ✅ | ✅ |
| Consumer façade (subscribe / receive / ack) | ✅ | ✅ |
| Supervised reconnect (Stage 2 + Stage 3 rebuild) | ✅ | ✅ |
| DNS resolver injection ([ADR-0015](../specs/adr/0015-dns-resolver-injection.md)) | ✅ | ✅ |
| Driver-level TLS (rustls byte-pipe — [ADR-0006](../specs/adr/0006-moonpool-tls-byte-pipe.md)) | ✅ | ✅ |
| `memory_limit` atomic-CAS reservation ([ADR-0017](../specs/adr/0017-memory-limit-atomic-reservation.md)) | ✅ | ✅ |
| `MemoryLimitPolicy::ProducerBlock` ([ADR-0020](../specs/adr/0020-memory-limit-producer-block.md), [ADR-0022](../specs/adr/0022-memory-limit-producer-block-moonpool.md)) | ✅ | ✅ |
| `ServiceUrlProvider` + `ControlledClusterFailover` ([ADR-0016](../specs/adr/0016-pip-121-cluster-failover.md)) | ✅ | ✅ |
| `AutoClusterFailover` (PIP-121 with `HealthProbe`) | ✅ | ❌ |
| PIP-188 `TOPIC_MIGRATED` → reconnect ([ADR-0018](../specs/adr/0018-pip-188-reconnect-on-migrate.md)) | ✅ | ✅ |
| Generic `PulsarClient<E: Engine>` ([ADR-0019](../specs/adr/0019-engine-scope-and-moonpool-parity.md)) | ✅ | ✅ |
| Partitioned producer / consumer | ✅ | ❌ |
| MultiTopicsConsumer | ✅ | ❌ |
| PatternConsumer (PIP-145) | ✅ | ❌ |
| Reader | ✅ | ❌ |
| TableView | ✅ | ❌ |
| Transactions (PIP-31) | ✅ | ❌ |
| Typed schemas | ✅ | ❌ |
| Deterministic chaos pack | n/a | ✅ |
| tokio ↔ moonpool differential equivalence harness | n/a | ✅ |

`MoonpoolEngine<P>` is generic over the
[`moonpool_core::Providers`](https://crates.io/crates/moonpool-core)
bundle. `TokioProviders` runs it against a real broker;
`moonpool-sim`'s `SimProviders` runs it under deterministic seeds
([`moonpool-engine.md`](moonpool-engine.md)).

The façade surface bound to `PulsarClient<TokioEngine>` (partitioned,
multi-topics, pattern, reader, table-view, transactions, typed schemas)
does not compile under `PulsarClient<MoonpoolEngine<P>>`. Callers that
reach for a tokio-only method on the moonpool engine get a trait-bound
compile error, not a silent fallback — see ADR-0019 §Consequences.

## Genuine deferred-scope items

Everything else with a `🟡` or `❌` in the README parity matrix is one of:

| Item | Status | Why deferred |
| --- | --- | --- |
| **SASL (Kerberos)** | 🟡 pre-alpha | Crate scaffolded ([`magnetar-auth-sasl`](../crates/magnetar-auth-sasl)); full GSSAPI plumbing is large-scope. |
| **Athenz** | 🟡 pre-alpha | Crate scaffolded ([`magnetar-auth-athenz`](../crates/magnetar-auth-athenz)); ZTS/ZMS plumbing is large-scope. |
| **`AutoProduceBytesSchema`** | 🟡 trait surface | Less common than `AutoConsumeSchema` (✅); producer-side deferred to v0.2.0. |
| **PIP-460** — Scalable topics | ❌ | Experimental in Apache Pulsar. |
| **PIP-466** — V5 client surface | ❌ | Inspired by, not adopted verbatim. |
| **PIP-180** — Shadow topic | ❌ | v0.2.0. |
| **PIP-33** — Replicated subscriptions | ❌ | v0.2.0. |

These are not required for v0.1.0 under
[ADR-0010](../specs/adr/0010-v0-1-full-java-parity.md), which v0.1.0
satisfies on the tokio engine.

## Constraints recap

- **No channels.** `tokio::sync::{mpsc,broadcast,watch,oneshot}`,
  `std::sync::mpsc`, `crossbeam-channel`, `flume`, `async-channel`,
  `kanal`, `postage`, `tachyonix`, `thingbuf` — banned. Use
  `Arc<parking_lot::Mutex<...>>` + `tokio::sync::Notify` +
  `core::task::Waker` slabs. See
  [ADR-0003](../specs/adr/0003-no-channels-rule.md).
- **Sans-io clock injection.** Every `magnetar-proto::Connection` entry
  takes `now: Instant` plus a `wall_clock` provider. See
  [ADR-0011](../specs/adr/0011-clock-injection-sans-io.md).
- **`rustls` only.** No `native-tls`, no `openssl`. See
  [ADR-0005](../specs/adr/0005-rustls-only-tls.md).

## Validation chain (per commit)

```
cargo +nightly fmt --all
cargo build --workspace --all-features
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features
cargo deny check
RUSTDOCFLAGS="-D warnings --cfg tokio_unstable" \
  cargo doc --workspace --all-features --no-deps --locked
cargo xtask check-no-channels
cargo xtask check-no-io-deps
cargo xtask check-no-internal-clock
cargo xtask codegen --check
```

Behind `--features e2e`, the suite spins `apachepulsar/pulsar:4.0.4`
in Docker and exercises the public surface against a real broker — see
[`testing.md`](testing.md).
