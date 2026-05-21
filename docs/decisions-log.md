# Magnetar — User decisions + plan deltas (signed off 2026-05-20)

Folds Florentin's answers to the 12 audit questions into the implementation plan. Supersedes the defaults in `ask-magnetar-plan.md` §15 and the audit's question list in `ask-magnetar-audit.md` §D.

## Project identity

- **Internal & published project name**: `magnetar` (replaces `quasar` / `quasar-pulsar` proposal — original `quasar` taken on crates.io, and `quasar-pulsar` is uglier than `magnetar`).
- **Local path**: `/home/florentin/Sources/github.com/FlorentinDUBOIS/magnetar/` (moved from `me/quasar/`).
- **GitHub repo (to create)**: `github.com/FlorentinDUBOIS/magnetar` — `gh repo create` is **approval-gated**.
- **Published crate names**: `magnetar`, `magnetar-proto`, `magnetar-runtime-tokio`, `magnetar-runtime-moonpool`, `magnetar-admin`, `magnetar-fakes`, `magnetar-cli`, plus auth sub-crates `magnetar-auth-oauth2`, `magnetar-auth-sasl`, `magnetar-auth-athenz`, and crypto sub-crate `magnetar-messagecrypto`.

## License

- **Apache-2.0 only.** Matches Pulsar upstream + moonpool. `LICENSE` + `NOTICE` + SPDX headers (`// SPDX-License-Identifier: Apache-2.0`) on every `.rs` file. `cargo deny` license allow-list: `["Apache-2.0", "MIT", "BSD-3-Clause", "ISC", "Unicode-DFS-2016"]` (Unicode is for transitive `unicode-ident`).

## Architecture: no channels

**Hard ban**: `tokio::sync::mpsc`, `tokio::sync::broadcast`, `tokio::sync::watch`, `tokio::sync::oneshot`, `crossbeam-channel`, `flume`, `async-channel`, `std::sync::mpsc`. Any of these appearing in a `Cargo.toml` is a CI failure (`cargo deny bans deny` entries below).

**Replacement pattern** (mirrors how `quinn` *would* look if it didn't use mpsc):

```rust
// magnetar-proto: pure sans-io, no concurrency primitives at all.

// magnetar-runtime-tokio:
pub struct ConnectionShared {
    inner: parking_lot::Mutex<magnetar_proto::Connection>,
    driver_waker: tokio::sync::Notify,
}

// User-facing handle. Cheap to clone (Arc<ConnectionShared>).
pub struct Producer { shared: Arc<ConnectionShared>, handle: ProducerHandle }

impl Producer {
    pub fn send(&self, msg: OutgoingMessage) -> SendFut {
        let sequence_id = {
            let mut conn = self.shared.inner.lock();
            conn.send(self.handle, msg).expect("producer state checked at lock time")
        };
        self.shared.driver_waker.notify_one();
        SendFut { shared: self.shared.clone(), sequence_id }
    }
}

pub struct SendFut { shared: Arc<ConnectionShared>, sequence_id: u64 }

impl Future for SendFut {
    type Output = Result<MessageId, SendError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut conn = self.shared.inner.lock();
        match conn.take_send_receipt(self.sequence_id) {
            Some(result) => Poll::Ready(result),
            None => { conn.register_send_waker(self.sequence_id, cx.waker().clone()); Poll::Pending }
        }
    }
}

// I/O driver task — one per Connection:
async fn driver(shared: Arc<ConnectionShared>, mut socket: TlsStream<TcpStream>) -> Result<()> {
    let mut read_buf  = BytesMut::with_capacity(64 * 1024);
    let mut write_buf = Vec::<u8>::with_capacity(64 * 1024);
    loop {
        // Drain outbound + dispatch events (under lock).
        {
            let mut conn = shared.inner.lock();
            write_buf.clear();
            conn.poll_transmit(&mut write_buf);
            conn.dispatch_pending_event_wakers(); // wakes the per-future Wakers
        }
        let deadline = shared.inner.lock().poll_timeout();

        tokio::select! {
            biased;
            _ = shared.driver_waker.notified() => {},
            r = socket.read_buf(&mut read_buf) => {
                let n = r?;
                if n == 0 { return Err(Error::PeerClosed); }
                let bytes = read_buf.split().freeze();
                shared.inner.lock().handle_bytes(Instant::now(), &bytes);
            }
            r = write_all(&mut socket, &write_buf), if !write_buf.is_empty() => { r?; }
            _ = sleep_until_opt(deadline), if deadline.is_some() => {
                shared.inner.lock().handle_timeout(Instant::now());
            }
        }
    }
}
```

Per-Future `Waker` registration inside `magnetar-proto::Connection` is the cancer-free equivalent of a oneshot channel. The sans-io state machine carries a slab of `(operation_id → Waker)` and resolves wakers when the matching event arrives via `poll_event()`/`dispatch_pending_event_wakers()`. **No `Stream`/`Sink` channel pairs in the public API.**

Allowed concurrency primitives:
- `parking_lot::Mutex` / `parking_lot::RwLock` — for shared state.
- `tokio::sync::Notify` — for driver wakeups (single-cell; not a channel).
- `std::sync::atomic::*` — for stats / state flags.
- `core::task::Waker` — for future completion.
- `tokio::select!` — multiplexes futures (not a channel).

If `Notify` itself is too channel-like for taste, fall back to `parking_lot::Condvar` + `parking_lot::Mutex<bool>` — but Notify is the idiomatic async signal.

## Pulsar 4.0 baseline

- **Minimum supported broker**: Pulsar 4.0 (released 2024-10; LTS line).
- **Wire**: claim `ProtocolVersion::V21` on `CONNECT`; accept lower in `CONNECTED`.
- **PIP-460 scalable topics**: now in scope (was deferred). `SCALABLE_TOPIC_*` commands at `PulsarApi.proto:1236-1246` decoded + emitted as appropriate.
- **PIP-466 V5 client API surface**: re-evaluate Java's V5 surface for inspiration but ship our own idiomatic Rust API.

## v0.1.0 scope (no deferrals)

| Feature | Source | Status |
|---|---|---|
| Producer + Consumer + Reader | Java parity | M2 |
| Chunking (PIP-37/107/131) | Java parity | M2 — Codex's "chunks-never-batched" rule applies |
| Batching (`BatchMessageContainerImpl`) | Java parity | M2 |
| Key_Shared full (PIP-34/119/282/379) | Java parity | M2 |
| Batch-index ACK (PIP-54/391) | Java parity | M2 |
| DLQ + retry letter (PIP-22/58/124/409) | Java parity | M2 |
| TopicListWatcher (PIP-145) | Java parity | M2 |
| TOPIC_MIGRATED (PIP-188) | Java parity | M2 |
| AUTH_CHALLENGE refresh (PIP-30/292) | Java parity | M2 + M6 |
| Schemas: full parity (Avro/JSON/Protobuf/ProtobufNative/KeyValue/AutoConsume/AutoProduce) | Java parity | M5 — **with byte canonicalisation for PROTOBUF_NATIVE + KeyValue** (Codex Q4) |
| Auth: token + TLS (mTLS) + OAuth2 ClientCredentialsFlow + SASL + Athenz | Java parity | M6 — split crates `magnetar-auth-oauth2`/`-sasl`/`-athenz` |
| Transactions (PIP-31) | Java parity | M7 (new) — TC client, NEW_TXN/ADD_PARTITION_TO_TXN/ADD_SUBSCRIPTION_TO_TXN/END_TXN/* |
| End-to-end encryption (PIP-4) | Java parity | M8 (new) — `magnetar-messagecrypto`, AES-GCM via `aws-lc-rs` (FIPS-friendly, audited) |
| Cluster failover (PIP-121) — Auto + Controlled | Java parity | M9 (new) |
| Replicated subscriptions (PIP-33) | Java parity | M9 |
| Scalable topics (PIP-460/466) | Java parity | M9 (experimental tag) |
| Shadow topic (PIP-180) | Java parity | M9 |
| getMessageIdByIndex (PIP-415) | Java parity | M9 |
| `magnetar-admin` REST client | Java parity | M9 (parallel) |
| `magnetar-cli` binary | Greenfield | M9 (parallel) — produce/consume/inspect/admin |

**New milestone schedule**: M0 (bootstrap) → M1 (codec) → M2 (sans-io state machine, long pole) → {M3 tokio engine, M4 moonpool engine} in parallel → M5 (schemas full parity) → M6 (auth full parity) → M7 (transactions) → M8 (encryption) → M9 (admin + CLI + PIP-121/33/460/180/415) → v0.1.0 cut.

This is a multi-month effort with the v0.1.0 cut being a real "feature-complete" release, not a teaser. Florentin's directive "nothing deferred, tackle everything" raises the bar to *true* Java parity for v0.1.0.

## moonpool TLS strategy (option d)

- Implement a **local `rustls` adapter over moonpool's `NetworkProvider`-supplied byte pipe**. rustls is itself sans-io (`read_tls` / `write_tls` / `process_new_packets`) — perfect composition.
- Adapter lives in `magnetar-runtime-moonpool/src/tls.rs`:
  ```
  pub struct RustlsOverMoonpool<S> {
      session: rustls::ClientConnection,
      socket: S,                  // moonpool-supplied byte stream
      plaintext_in: BytesMut,     // ready for magnetar-proto
      plaintext_out: VecDeque<Bytes>, // from magnetar-proto, pending TLS encrypt
  }
  ```
- Each iteration of the moonpool engine's driver loop: pump `socket.read` → `session.read_tls` → `session.process_new_packets()` → drain `session.reader()` into `plaintext_in`. Symmetric on the write path. Standard rustls "by-hand" usage pattern.
- This means TLS handshakes are **deterministic under `moonpool-sim` chaos testing** — a non-trivial differentiator over both `pulsar-rs` and the Java client.

## `cargo-mutants` (Codex push-back accepted)

Promoted from v0.2.0 to **M5/M6 deliverable** as a `mutants-smoke` job: `cargo-mutants` runs on `magnetar-proto` only, time-boxed, nightly + `workflow_dispatch`. Targets frame decode, request correlation, resend/dedup, flow permits, chunk metadata, timeout transitions.

## `Cargo.lock` + `protoc`

- **`Cargo.lock` committed** (libraries publishing as crates can debate; magnetar is workspace + CLI binary, so lock is committed).
- **`protoc ≥ 3.19` required** for contributors who run `xtask codegen`. End users do NOT need it because generated code is checked into `magnetar-proto/src/pb/`. CI installs `protoc` via `arduino/setup-protoc@v3` for the codegen job.

## Coexistence with `pulsar-rs`

Florentin: "do not take care of pulsar-rs". So:
- No `docs/migration-from-pulsar-rs.md`.
- README does not mention pulsar-rs.
- magnetar ships as its own independent project. Florentin's call on what happens to pulsar-rs.

## Hosting / push

- `gh repo create FlorentinDUBOIS/magnetar --public --source=. --remote=origin --description "Sans-io Apache Pulsar client driver for Rust"` — **approval-gated**.
- M0 first push (`git push -u origin main`) — **approval-gated**.

## Updated approval gates (final list — supersedes plan §15)

1. `gh repo create FlorentinDUBOIS/magnetar`.
2. M0 first push.
3. Each subsequent `wt merge -y` to `main`.
4. `cargo publish` of any of the 11 crates.
5. Any dependency outside the (now-final) allow-list below.
6. Any new sub-crate not in the topology table above.

### Final dependency allow-list

`prost`, `prost-build`, `prost-types`, `bytes`, `tokio`, `tokio-util`, `tokio-rustls`, `rustls`, `rustls-pemfile`, `rustls-native-certs`, `crc32c`, `lz4_flex`, `zstd`, `snap`, `flate2`, `serde`, `serde_json`, `apache-avro` 0.21, `tracing`, `tracing-subscriber`, `thiserror`, `anyhow`, `futures`, `futures-util`, `pin-project-lite`, `parking_lot`, `arc-swap`, `slab`, `uuid` (PIP-37 chunk UUIDs), `aws-lc-rs` (PIP-4 AES-GCM), `clap` (CLI), `reqwest` (admin), `url`, `http`, `moonpool-core` 0.6 (`=0.6` pin), `testcontainers` 0.27, `rstest`, `proptest`.

### Banned crates (`cargo deny bans deny`)

`tokio::sync::mpsc`-using crates, `crossbeam-channel`, `flume`, `async-channel`, `kanal`, `postage`, `tachyonix` — anything queue-shaped.
