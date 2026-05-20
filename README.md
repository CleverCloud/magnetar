# magnetar

> **Status: pre-alpha (M0 bootstrap).** No public release. API and crate set unstable. Do not depend on this in production.

A sans-io Apache Pulsar client driver for Rust, with multiple swappable I/O engines.

- **Sans-io core** (`magnetar-proto`): pure-Rust protocol state machine in the `quinn-proto` shape — encode/decode, framing, CRC32C, request correlation, batching, chunking, key_shared, transactions, schemas. **Zero I/O dependencies.** No `tokio`. No `async`. No sockets.
- **Engines**: `magnetar-runtime-tokio` is the public default (production-ready, TLS via `tokio-rustls`); `magnetar-runtime-moonpool` is opt-in for FoundationDB-style deterministic simulation testing via [PierreZ/moonpool](https://github.com/PierreZ/moonpool).
- **No channels**: the entire workspace avoids `mpsc`/`broadcast`/`watch`/`oneshot` (any flavour). Concurrency uses `Arc<parking_lot::Mutex<State>>` + `tokio::sync::Notify` + in-state `Waker` slabs.
- **Java parity** target: producer, consumer, reader, admin REST, transactions, end-to-end encryption (PIP-4), chunking (PIP-37), key_shared full surface (PIP-34/119/282/379), batch-index ACK (PIP-54/391), DLQ + retry (PIP-22/58/124/409), TopicListWatcher (PIP-145), TOPIC_MIGRATED (PIP-188), AUTH_CHALLENGE refresh (PIP-30), exclusive producers (PIP-68), broker-entry-metadata (PIP-90), scalable topics (PIP-460, experimental tag).
- **Minimum broker**: Apache Pulsar 4.0+.

## Workspace

| Crate | Role |
|---|---|
| `magnetar` | Public façade — re-exports + builder. |
| `magnetar-proto` | Sans-io protocol crate. The heart of the project. |
| `magnetar-runtime-tokio` | Tokio engine with rustls TLS. |
| `magnetar-runtime-moonpool` | moonpool engine, ships its own `rustls`-over-bytepipe adapter for deterministic TLS handshakes under chaos testing. |
| `magnetar-admin` | REST admin client. |
| `magnetar-cli` | `magnetar` binary — produce/consume/inspect/admin. |
| `magnetar-fakes` | In-process broker fake (dev-dep). Mirrors Java's `MockBrokerService`. |
| `magnetar-auth-oauth2` | OAuth2 ClientCredentialsFlow auth provider. |
| `magnetar-auth-sasl` | SASL auth provider. |
| `magnetar-auth-athenz` | Athenz auth provider. |
| `magnetar-messagecrypto` | PIP-4 end-to-end encryption (AES-GCM via `aws-lc-rs`). |
| `xtask` | Build helpers — `protoc` codegen, e2e driver. Not published. |

## Validation

```
cargo build --workspace --all-features
cargo clippy --workspace --all-features -- -D warnings
cargo +nightly fmt --check
cargo test --workspace
cargo deny check
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

E2e tests (require Docker):

```
cargo test --workspace --features e2e
```

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

This project vendors and depends on a redacted copy of the Apache Pulsar wire protocol definition (`PulsarApi.proto`, `PulsarMarkers.proto`), released by the Apache Software Foundation under Apache-2.0.
