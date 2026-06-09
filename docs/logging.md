# Logging

Magnetar emits structured logs through the [`tracing`](https://docs.rs/tracing) ecosystem.
The library never installs a subscriber: without one, every log call is a no-op and the driver is silent.
The binding policy — level semantics, field conventions, the no-secrets rule — is [ADR-0054](../specs/adr/0054-logging-policy.md); this page is the operator-facing companion.

## Installing a subscriber

Magnetar logs like any `tracing`-instrumented library; install whichever subscriber your application already uses.
The minimal standalone setup:

```rust
use tracing_subscriber::EnvFilter;

tracing_subscriber::fmt()
    .with_env_filter(EnvFilter::from_default_env())
    .init();
```

Then select verbosity with `RUST_LOG`:

```sh
RUST_LOG=info ./my-app                                  # lifecycle + warnings + errors
RUST_LOG=warn,magnetar_runtime_tokio=debug ./my-app     # per-operation detail for the tokio engine only
RUST_LOG=info,magnetar_proto=debug ./my-app             # protocol internals (redirect hops, handshake transitions)
```

JSON output, OTLP export, file rotation, and so on are subscriber concerns — see the [`tracing-subscriber`](https://docs.rs/tracing-subscriber) documentation.

## Level taxonomy

Magnetar is a library: most faults surface to your code as `Err`, so they are **not** double-reported at `error!`.

| Level    | What it means                                                                                             | Examples                                                                                                                                                                                                                                                                                                         |
| -------- | --------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `error!` | A fault your code cannot observe: a protocol-corruption drop or a background-supervisor terminal failure. | CRC32C checksum mismatch (corrupt frame dropped); supervised-reconnect handshake failure after reset.                                                                                                                                                                                                            |
| `warn!`  | Degraded but recovering background state; security-relevant refusals.                                     | Reconnect attempt failed (backoff engaged); anti-thrash cooldown; redirect URL rejected by the allow-list; auth-refresh failure.                                                                                                                                                                                 |
| `info!`  | Lifecycle: one record per state change or unit of work.                                                   | Connection established; producer/consumer created or closed; reconnect TCP-connect (`"supervisor: TCP connected; handshaking"`) vs. reconnect success after the handshake completes (`"supervisor: reconnected to broker; handshake complete, …"`) + state replay; failover swap; dead-letter republish summary. |
| `debug!` | Expected anomalies and per-operation internals.                                                           | Lookup resolution + redirect hops; auth-challenge round-trip; batch-flush decisions; per-message dead-letter detail; memory-limit rejection.                                                                                                                                                                     |
| `trace!` | Per-message hot-path records.                                                                             | Send queued (`sequence_id`, `payload_len`); ack/nack/flow permits.                                                                                                                                                                                                                                               |

Volume expectations: `warn!` and above are bounded by churn (reconnects, refusals, lifecycle), never by send throughput.
Per-message records live at `trace!` and `debug!` only, and the per-message `debug!` paths are allocation-free when the level is disabled, so `debug!` is safe to enable in production with per-target filtering.
Nothing operator-load-bearing lives below `info!` — your application may compile out `trace!`/`debug!` via `tracing`'s `release_max_level_*` features without losing an alarm; magnetar itself never sets those features (they would propagate to your binary through feature unification).

## Field glossary

Logs carry structured snake_case fields, never values formatted into the message string.
The recurring fields:

| Field                                           | Meaning                                                                                     |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `topic`                                         | Fully-qualified topic name (plus partition where relevant).                                 |
| `subscription`                                  | Subscription name.                                                                          |
| `producer_name`                                 | Producer name as registered with the broker.                                                |
| `handle`                                        | Driver-local producer/consumer handle id.                                                   |
| `sequence_id` / `message_id` / `request_id`     | Per-message / per-request identifiers.                                                      |
| `broker_service_url` / `broker_service_url_tls` | Broker-advertised service URLs (truncated to 256 bytes at a char boundary).                 |
| `host` / `port`                                 | Dialled broker endpoint.                                                                    |
| `attempt` / `delay_ms` / `cooldown_ms`          | Reconnect-supervision counters and timings.                                                 |
| `payload_len`                                   | Message payload size in bytes (never the payload itself).                                   |
| `auth_method`                                   | Auth provider name (`token`, `oauth2`, `athenz`, …) — never credentials.                    |
| `auth_challenge_pending`                        | Whether the broker requested an AUTH_CHALLENGE round-trip during connect (moonpool engine). |
| `permits` / `count`                             | Flow-control permits / batch-summary counts.                                                |
| `error` / `source` / `code`                     | Error display, origin tag, server error code.                                               |

Targets follow module paths (`magnetar_runtime_tokio::driver`, `magnetar_proto::conn`, `magnetar::auth::oauth2`, …), so per-target filtering can isolate one layer.

## No-secrets guarantees

At every level, including `trace!`, magnetar never logs:

- token bytes, `auth_data`, AUTH_CHALLENGE challenge or response bytes;
- mTLS private keys or certificate chains;
- OAuth2 `client_secret` or identity-provider response bodies;
- Athenz private keys, cached role tokens, or ZTS response bodies;
- message payload bytes (only `payload_len`);
- inbound `traceparent` / `tracestate` properties (hostile-peer-controlled).

Auth-path errors log the `auth_method` plus a stable error class — never the full provider error chain, which could embed credentials.
Broker-supplied strings (server error messages, redirect URLs) are truncated to 256 bytes (cut at a char boundary) as a log-injection and cardinality defense.
These guarantees are pinned by paired secret-scan capture tests in both runtime engines and an end-to-end assertion against a real broker.

Note that `topic`, `subscription`, `producer_name`, and broker URLs are classified as operational metadata, not secrets ([ADR-0054 §3](../specs/adr/0054-logging-policy.md)).
If your deployment treats topic names as confidential, filter those fields subscriber-side.

## CLI verbosity

The `magnetar` CLI wires its own subscriber behind the `-v` flag ladder (`-v` → `magnetar=debug` … `-vvvvv` → full dependency trace); see [the CLI reference](cli.md#global-flags).

## Correlation with OpenTelemetry

`tracing` events inherit the current span, so consumer-side logs emitted after `attach_context` correlate with the producer's trace automatically.
Magnetar creates no spans of its own — see [`observability.md`](observability.md) and [ADR-0053](../specs/adr/0053-otel-context-propagation.md) for the context-propagation contract.
