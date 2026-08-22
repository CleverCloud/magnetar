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

## Rate-limiting and sampling

[ADR-0054 §7](../specs/adr/0054-logging-policy.md) bounds volume _structurally_ — per-message records live at `trace!` / `debug!`, and `warn!` and above are bounded by churn, never by throughput.
But churn can itself storm: a broker-restart cascade has every supervised connection's reconnect loop emit one `warn!` per attempt at a single callsite (`"supervisor: reconnect attempt failed; will retry"`), so many reconnecting connections collapse onto one line at an unbounded rate.

Rate-limiting and sampling are **subscriber-side**: magnetar emits unchanged and ships no limiter ([ADR-0065](../specs/adr/0065-log-rate-limiting-subscriber-side.md) — a library-side limiter would carry per-callsite state and read a clock, which the sans-io core must not).
Compose these three tiers to taste.

### 1. Static — raise a noisy target's floor

The cheapest tool is the `EnvFilter` you already installed: lift one target above the storming level.

```sh
RUST_LOG=info,magnetar_runtime_tokio::driver=error ./my-app   # drop supervisor warn!s, keep its error!s
```

This is coarse — it silences _every_ `warn!` from `magnetar_runtime_tokio::driver` (reconnect-failed, anti-thrash cooldown, give-up), not just the cascade.
Use it when one target is known-noisy and you don't need its other warnings.

### 2. Dynamic — a per-callsite rate-limiting layer

For per-line control, drop events from any single callsite that exceeds a rate.
A token bucket keyed by `tracing` callsite is a small, dependency-free `Layer`; because the reconnect cascade shares one callsite, a single bucket caps the whole storm while every other callsite is untouched:

```rust
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use tracing::Event;
use tracing::callsite::Identifier;
use tracing_subscriber::layer::{Context, Layer};

struct Bucket {
    tokens: f64,
    last: Instant,
}

/// Drops events from any single callsite exceeding `rate`/sec, allowing
/// bursts up to `burst`. The keyspace is the set of `tracing` callsites
/// compiled into the binary — finite and `&'static`, so the map is
/// naturally bounded; no eviction needed.
pub struct CallsiteRateLimit {
    rate: f64,
    burst: f64,
    buckets: Mutex<HashMap<Identifier, Bucket>>,
}

impl CallsiteRateLimit {
    pub fn new(rate_per_sec: f64, burst: f64) -> Self {
        Self { rate: rate_per_sec, burst, buckets: Mutex::new(HashMap::new()) }
    }
}

impl<S: tracing::Subscriber> Layer<S> for CallsiteRateLimit {
    fn event_enabled(&self, event: &Event<'_>, _ctx: Context<'_, S>) -> bool {
        let id = event.metadata().callsite();
        let now = Instant::now();
        let mut buckets = self.buckets.lock().unwrap();
        let b = buckets.entry(id).or_insert(Bucket { tokens: self.burst, last: now });
        let elapsed = now.saturating_duration_since(b.last).as_secs_f64();
        b.last = now;
        b.tokens = (b.tokens + elapsed * self.rate).min(self.burst);
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}
```

Install it at registry level, so a dropped event is suppressed for every layer:

```rust
use tracing_subscriber::prelude::*;

tracing_subscriber::registry()
    .with(tracing_subscriber::EnvFilter::from_default_env())
    .with(tracing_subscriber::fmt::layer())
    .with(CallsiteRateLimit::new(5.0, 20.0)) // 5 events/sec per callsite, burst 20
    .init();
```

`event_enabled` is the per-event hook (`tracing-subscriber` 0.3); returning `false` skips the event for the whole stack.
The clock (`Instant::now()`) and the bucket state live here, in your process — which is exactly why this belongs subscriber-side and not in the sans-io driver.

### 3. Fleet — sample in the collector

An in-process limiter only sees its own process.
When a storm aggregates across many instances, sample or deduplicate downstream — in the OTLP collector, Vector, or Loki pipeline — where the whole fleet's stream converges.
See [`observability.md`](observability.md) for the export path.

**Keep the signal.**
Every tier drops _log lines_, not the underlying condition.
Pair rate-limiting with a metric or alert on the cause (e.g. reconnect rate) so a genuinely escalating outage still pages you when its logs are being throttled.

## Field glossary

Logs carry structured snake_case fields, never values formatted into the message string.
The recurring fields:

| Field                                           | Meaning                                                                                                |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `topic`                                         | Fully-qualified topic name (plus partition where relevant).                                            |
| `subscription`                                  | Subscription name.                                                                                     |
| `producer_name`                                 | Producer name as registered with the broker.                                                           |
| `handle`                                        | Driver-local producer/consumer handle id.                                                              |
| `sequence_id` / `message_id` / `request_id`     | Per-message / per-request identifiers.                                                                 |
| `broker_service_url` / `broker_service_url_tls` | Broker-advertised service URLs (truncated to 256 bytes at a char boundary).                            |
| `host` / `port`                                 | Dialled broker endpoint.                                                                               |
| `attempt` / `delay_ms` / `cooldown_ms`          | Reconnect-supervision counters and timings; `attempt` also numbers a consumer stall auto-recovery try. |
| `permit_balance` / `stalled_for_ms`             | Un-spent broker permits and observed silence on a `ConsumerStalled` report (issue #414, ADR-0101).     |
| `attempts` / `max_attempts`                     | Consumer stall auto-recovery budget: spent so far, and the configured cap (ADR-0103).                  |
| `payload_len`                                   | Message payload size in bytes (never the payload itself).                                              |
| `auth_method`                                   | Auth provider name (`token`, `oauth2`, `athenz`, …) — never credentials.                               |
| `auth_challenge_pending`                        | Whether the broker requested an AUTH_CHALLENGE round-trip during connect (moonpool engine).            |
| `permits` / `count`                             | Flow-control permits / batch-summary counts.                                                           |
| `error` / `source` / `code`                     | Error display, origin tag, server error code.                                                          |

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
This bound also covers **error and connect-error fields**, not just log fields: a mid-handshake broker `CommandError.message` is truncated at the proto capture site before it is stored in `handshake_failure_reason`, so the `ClientError::Other("handshake failed: …")` (tokio) / `EngineError::HandshakeFailed` (moonpool) connect errors that inherit it — and the adjacent `warn!` field — are length-bounded too ([ADR-0062](../specs/adr/0062-broker-error-field-truncation.md), completing [ADR-0054 §3](../specs/adr/0054-logging-policy.md)). A short broker message still round-trips verbatim.
These guarantees are pinned by paired secret-scan capture tests in both runtime engines and an end-to-end assertion against a real broker.

Note that `topic`, `subscription`, `producer_name`, and broker URLs are classified as operational metadata, not secrets ([ADR-0054 §3](../specs/adr/0054-logging-policy.md)).
If your deployment treats topic names as confidential, filter those fields subscriber-side.

## CLI verbosity

The `magnetar` CLI wires its own subscriber behind the `-v` flag ladder (default → `magnetar=warn`, `-v` → `magnetar=info` … `-vvvvvv` → full dependency trace); see [the CLI reference](cli.md#global-flags).

## Correlation with OpenTelemetry

`tracing` events inherit the current span, so consumer-side logs emitted after `attach_context` correlate with the producer's trace automatically.
Magnetar creates no spans of its own — see [`observability.md`](observability.md) and [ADR-0053](../specs/adr/0053-otel-context-propagation.md) for the context-propagation contract.
