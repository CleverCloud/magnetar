# Architecture

## Layers

```
+------------------------------------------------------------------------+
|                              user code                                  |
+------------------------------------------------------------------------+
                                  |
                                  v
+------------------------------------------------------------------------+
|  magnetar (façade)              |  magnetar-cli  |  magnetar-admin     |
|  ---------------                |  -------------  |  ------------------ |
|  builder, Pulsar URL parser,    |  clap-driven    |  REST admin client  |
|  schema registry façade, auth   |  produce/       |  (reqwest).         |
|  registry, runtime selector.    |  consume/       |                     |
|                                 |  inspect.       |                     |
+------------------------------------------------------------------------+
                                  |
                                  v
+------------------------------------------------------------------------+
|  magnetar-runtime-tokio   |   magnetar-runtime-moonpool                |
|  ------------------------ |   --------------------------                |
|  Public default.          |   Opt-in. Deterministic sim via moonpool-  |
|  tokio + tokio-rustls.    |   sim. Custom rustls-over-bytepipe TLS     |
|  One driver task per      |   adapter (option d). Same Connection      |
|  Connection.              |   sans-io state machine.                   |
+------------------------------------------------------------------------+
                                  |
                                  v
+------------------------------------------------------------------------+
|  magnetar-proto (sans-io core, NO I/O deps, NO channels)               |
|  ----------------------------------------------------------            |
|  Connection state machine — quinn-proto shape:                         |
|      handle_bytes(now, &[u8])                                          |
|      poll_transmit(&mut Vec<u8>) -> usize                              |
|      poll_event() -> Option<ConnectionEvent>                           |
|      poll_timeout() -> Option<Instant>                                 |
|      handle_timeout(now: Instant)                                      |
|  + handle-based façade:                                                |
|      open_producer(req) -> ProducerHandle                              |
|      subscribe(req) -> ConsumerHandle                                  |
|      send(h, msg) -> sequence_id                                       |
|      ack(h, ack), seek(h, target), close_*(h)                          |
|  Internal state: pending_ops (Slab<Waker>), producers, consumers,      |
|  trackers (ack, nack, unack), lookup, batch container, chunk reasm.    |
+------------------------------------------------------------------------+
```

## No-channels rule

Channels (mpsc / broadcast / watch / oneshot, any flavour) are **forbidden** in the entire workspace. See [GUIDELINES.md](GUIDELINES.md#no-channels) for the why and the replacement pattern.

The driver-to-API path uses:

```
[user task]                       [I/O driver task]
       |                                  |
       v                                  v
  Arc<ConnectionShared>            owns the same Arc
       |                                  |
       v                                  v
  parking_lot::Mutex<magnetar_proto::Connection>
       ^                                  ^
       |  user grabs lock, mutates,       |
       |  releases, then calls            |
       |  shared.driver_waker.notify_one()|
       |                                  |
  user-side Future:                  driver loop:
    poll() locks Connection,            select! {
    looks up pending op,                  _ = notify.notified() => {}
    if ready -> returns it,               r = socket.read_buf() => { lock + handle_bytes }
    else registers cx.waker().clone()     _ = sleep_until(deadline), if some => { lock + handle_timeout }
    in the Connection's slab,             _ = socket.writable(), if write_buf.has_data => { try_write }
    drops lock, returns Pending.        }
                                       after each select! arm, lock + poll_transmit + dispatch_pending_event_wakers
```

`dispatch_pending_event_wakers` walks the Connection's slab of `(op_id → Waker)` and `wake()`s the futures whose responses have arrived.

## Three TLS sites

1. **`magnetar-runtime-tokio`**: `tokio_rustls::TlsConnector::connect(server_name, tcp)` — the standard path.
2. **`magnetar-runtime-moonpool`**: a custom adapter at `magnetar-runtime-moonpool/src/tls.rs` driving `rustls::ClientConnection` via `read_tls` / `process_new_packets` / `write_tls` against the moonpool `NetworkProvider`-supplied byte pipe. This makes TLS handshakes deterministic under `moonpool-sim` chaos.
3. **`magnetar-admin`**: `reqwest` configured with `rustls-tls` — no native-tls anywhere.

## Wire protocol summary

- **Simple frame**: `[total_size u32][cmd_size u32][BaseCommand protobuf]`.
- **Payload frame**: `[total_size u32][cmd_size u32][BaseCommand][magic u16=0x0e01][crc32c u32][meta_size u32][MessageMetadata][payload]`.
- **Broker-entry-metadata envelope** (v16+, opt-in via FeatureFlags): `[magic u16=0x0e02][bem_size u32][BrokerEntryMetadata]` prepended to the standard frame on dispatched messages.
- Cite the Java reference at `pulsar-common/src/main/java/org/apache/pulsar/common/protocol/Commands.java:1866-2038`.

`magnetar-proto::frame` exposes `encode_simple`, `encode_payload`, and `decode_one` returning a `(BaseCommand, Option<(MessageMetadata, Bytes)>, Option<BrokerEntryMetadata>)` triple.

## Producer state machine notes

Critical Java semantics to mirror (per `ProducerImpl.java` and Codex cross-check):

- **Chunks and batches are mutually exclusive.** If `canAddToBatch(msg)`, `totalChunks` is forced to `1`. Two distinct emit paths:
  - **Chunked path** (large message, no batching): non-batch compress → schema/metadata → split → per-chunk metadata → encrypt each chunk → send each chunk frame.
  - **Batched path** (small messages aggregated): add to `BatchMessageContainer` → flush serialises singles → compress the whole batch → encrypt → set batch metadata → send.
- **Sequence id assignment** happens inside the chunk loop (`ProducerImpl.java:696-704`, `:745-753`) for both first-send and resend paths.
- **Dedup** uses `lastSequenceIdPublished` and `lastSequenceIdPushed` for resend safety.

The Rust state machine has separate `emit_chunked` and `emit_batched` paths plus a `canAddToBatch ⇒ totalChunks == 1` invariant test.

## Schema-registry parity

Per Codex cross-check: AVRO, JSON, and PROTOBUF are canonicalised broker-side by Avro `Schema.Parser` before version lookup (`SchemaRegistryServiceImpl.java:405-418, 657-662`). **All other types — including PROTOBUF_NATIVE and KeyValue — use raw-byte equality on `schema.getData()`** (`:429-438`). Magnetar's schema serialisers must emit byte-identical Java output for those types or the broker will create a fresh version on every connect.

## Further reading

- Decision log: `~/.claude/plans/ask-magnetar-decisions.md`
- Research dossier: `~/.claude/plans/ask-magnetar-research.md`
- Implementation plan: `~/.claude/plans/ask-magnetar-plan.md` (also `tasks/todo.md` while the repo is bootstrapping)
- Codex cross-check: `~/.claude/plans/ask-magnetar-codex.md`
