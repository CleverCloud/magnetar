# ADR-0054 — Structured logging policy: sozu-derived levels, hard no-secrets rule, single-owner sites

- **Status**: Accepted
- **Date**: 2026-06-05
- **Decider**: Florentin Dubois
- **Tags**: observability, logging, tracing, security, policy

## Context

Before this ADR the workspace carried 46 bare `tracing` event-macro call sites with no binding policy: level selection was ad-hoc, several supervisor logs inline-formatted values into the message string instead of structured fields, and two diagnostic events the proto layer emits precisely for observability — `ConnectionEvent::ChecksumMismatch` (CRC32C verify-or-drop, workspace invariant 4) and the `LookupOutcome::Redirected` intermediate lookup outcome — were never drained or logged by any engine, so a corrupting peer or a redirect storm was invisible to operators while the undrained events accumulated in the proto queue.
Magnetar is an operator-facing driver; a silent corruption drop, a rejected redirect, or an auth-refresh failure must be diagnosable from logs without a debugger.

The level taxonomy is transferred from sozu, a proxy-grade Rust networking codebase with a battle-tested logging policy (citations pinned at sozu HEAD `ef93a834`).
One adaptation is load-bearing: sozu is a terminal handler — the proxy is where errors die — while magnetar is a library whose faults mostly surface to the caller as `Err`.
A library that logs `error!` for a fault it also returns double-reports it: the operator sees two alarms for one event, and the caller's own error handling becomes noise.

Alternatives considered:

- **Engines-only logging, proto stays silent** (log exclusively from drained `ConnectionEvent`s).
  Rejected: proto holds the richest context at the point of detection (e.g. `computed`/`expected` checksums in the decode loop, hop counts mid redirect-chase), and several proto internals — handshake state transitions, chunk-reassembly progress — have no event at all and would stay invisible.
  The cost this alternative avoided (any `magnetar-proto` diff triggers ADR-0024's full four-layer test set) is accepted instead.
- **Ambient-state objection**: ADR-0053 rejected ambient OTel **reads** (`Context::current()` inside pure conversions) because hidden thread-local reads change wire bytes and break moonpool determinism.
  `tracing` emission is the opposite direction: it is write-only and state-machine-neutral — no wire byte, no `EventStream` entry, no scheduling decision, and no return value depends on whether a subscriber is installed.
  Emitting an event reads no ambient state into the state machine, so the ADR-0053 objection does not transfer.
- **`release_max_level_*` compile-time stripping** (sozu's release-strip rule).
  Rejected for the library: `tracing`'s `release_max_level_*` features are global and propagate to downstream users via Cargo feature unification — a library setting them would silently strip logs from the embedding application.

## Decision

Adopt the following binding logging policy for every crate in the workspace.
Enforcement: `cargo run -p xtask -- check-log-fields` (every `error!` / `warn!` / `info!` in non-`#[cfg(test)]` code carries ≥1 structured field; allowlist starts empty), paired secret-scan capture tests in both runtime crates, and review against this ADR.

### §1 Level semantics

**`error!` is reserved for faults the caller cannot observe** — background/supervisor failures and protocol-corruption drops.
Faults returned as `Err` to a caller log at `warn!` or below.
Consequently all other broker-controlled invalid input (rejected lookups, server errors surfaced to the caller, malformed-but-recoverable frames) stays below `error!`.

| Level    | sozu rule (sozu HEAD `ef93a834`)                                                                                                                               | magnetar rule                                                                                                                                        | Examples                                                                                                                                                                                                                                                               |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `error!` | protocol violation, invariant break, resource exhaustion, actionable fault; never panic, always recover (sozu `CLAUDE.md:59-60`, `doc/observability.md:73-79`) | invariant break or corruption-drop **not surfaced as `Err` to any caller**; supervisor terminal failures                                             | CRC32C `ChecksumMismatch` frame drop at the proto detection point; supervised-reconnect `begin_handshake` failure after reset                                                                                                                                          |
| `warn!`  | degraded but recovering: clamped config, missed deadline with retry path, non-blocking sink failure                                                            | degraded-but-recovering background state; security-relevant refusals                                                                                 | reconnect attempt failed + backoff; anti-thrash cooldown engaged; reconnect budget exhausted (caller still sees `last_inner_result`); redirect URL rejected by `redirect_url_allow_list`; auth-refresh failure; broker-forced producer/consumer close (driver reopens) |
| `info!`  | lifecycle state change, one success record per unit of work, audit (sozu `lib/src/protocol/proxy_protocol/relay.rs:187`)                                       | lifecycle: connection up, producer/consumer create + close, reconnect success + state replay, failover swap, seek/unsubscribe, DLQ republish summary | `connection established`; post-reconnect replay summary; auto-cluster failover swap                                                                                                                                                                                    |
| `debug!` | expected anomalies handled correctly, per-request internals                                                                                                    | expected anomalies; per-operation internals                                                                                                          | lookup resolution + redirect hops; auth-challenge round-trip; batch-flush decisions; per-message DLQ detail; send rejected by closed producer; memory-limit send rejection (caller-visible `Err`, rate scales with send throughput under overload)                     |
| `trace!` | frame/packet-level dumps, compile-time strippable                                                                                                              | per-message hot-path records, flow-permit accounting                                                                                                 | send queued (`sequence_id`, `payload_len`); ack/nack/flow permits                                                                                                                                                                                                      |

Nothing operator-load-bearing may live below `info!` — `debug!`/`trace!` may always be filtered out or compile-time stripped by the application without losing an alarm.

### §2 Context-field conventions

- Structured snake_case `tracing` fields with `%` / `?` sigils; values are never inline-formatted into the message string.
- "Involved in the action" = the identifiers and parameters the operation reads or writes.
  Concept map from sozu's canonical access-log bracket (`lib/src/logging/access_logs.rs:81-96`): `session_id` → connection identity (broker `host`/`port` or pool key — survives nothing; each reconnect attempt logs its own identity), `request_id` → `sequence_id` / `message_id` / `request_id`, `cluster_id` → `topic` (+ partition), `backend_id` → `broker_service_url` + `producer_id`/`consumer_id`.
- Canonical field names (reuse before inventing): `topic`, `subscription`, `producer_name`, `handle`, `sequence_id`, `message_id`, `broker_service_url`, `broker_service_url_tls`, `host`, `port`, `attempt`, `delay_ms`, `cooldown_ms`, `payload_len`, `auth_method`, `permits`, `count`, `error`, `source`, `code`.
- `target:` — keep the existing convention: explicit targets in proto / façade / auth (`magnetar_proto::conn`, `magnetar::pattern_consumer`, `magnetar::auth::oauth2`, `magnetar::auth::athenz`), default module-path targets in the runtime crates (`magnetar_runtime_tokio::driver`, `magnetar_runtime_moonpool::driver`, …). sozu's per-module prefix tags map onto these targets; applications filter per-target.
- Error formatting outside auth paths: `error = %err` (`Display`), never `?err` (`Debug` exposes struct internals and is not a stable operator surface).

### §3 Hard no-secrets rule

Never log, at any level, even at `trace!`:

1. token bytes — `TokenAuth::initial()` returns the raw token (`crates/magnetar-proto/src/auth/token.rs`);
2. `auth_data` sent in `CommandConnect`, `AUTH_CHALLENGE` `challenge` bytes, and the `refreshed` response bytes;
3. mTLS private-key PEM / certificate chains (`crates/magnetar-proto/src/auth/tls.rs` — the redacted `Debug` impl is the house pattern);
4. OAuth2 `client_secret` and IDP response bodies (`crates/magnetar-auth-oauth2`);
5. Athenz `private_key_pem`, cached role tokens, and ZTS response bodies (`crates/magnetar-auth-athenz`).

Allowed instead: the `auth_method` name (the `provider.method()` string), presence booleans, `body_len` + `status` (exemplar: `crates/magnetar-auth-athenz/src/zts.rs`), endpoint + `client_id` but never `client_secret` (exemplar: `crates/magnetar-auth-oauth2/src/lib.rs`).
This mirrors sozu: fingerprints not key material (`doc/observability.md:309-311`); hashed credentials never logged.

**Auth-path error-formatting rule.** Third-party `AuthProvider` `Display` / `Debug` impls are an uncontrolled secret channel — a provider error may embed the very token it failed to refresh.
On auth paths, log `auth_method` plus a stable error **class** (or the first line of the message, truncated) — never the full provider error chain.
Everywhere else `error = %err` (`Display`) applies per §2.

**Broker-controlled string sanitization.** Broker-supplied strings (server error messages, advertised redirect URLs) are truncated to 256 bytes (cut at a `char` boundary) in log fields — log-injection and cardinality defense, mirroring sozu's render-time sanitization (`doc/observability.md:276-279`).

**Tenant-metadata classification.** `topic`, `subscription`, `producer_name`, and broker URLs are classified as operational metadata, not secrets — they appear in log fields by design.
Privacy-sensitive deployments that consider topic names confidential filter them subscriber-side (field-level filtering or per-target suppression); the library does not pre-redact them.

**Untrusted-peer rule** (ADR-0053 §E1/§E2).
Inbound `traceparent` / `tracestate` message properties are hostile-peer-controlled: never log them unbounded; current policy does not log them at all.

### §4 OTel correlation

Magnetar propagates OTel context but creates no spans (ADR-0053).
`tracing` events inherit the current span, so consumer-side logs emitted after `attach_context` correlate automatically.
This policy adds **no spans** (that would need an ADR-0053 amendment) and no `capture = true` fields — that marker is reserved for the moonpool `SimulationLayer`; ordinary logs bypass it and stay determinism-safe.

### §5 Proto logging allowed under policy — single-owner rule

`magnetar-proto` MAY emit `tracing` events under this policy: explicit `target: "magnetar_proto::…"`, structured fields, no secrets, levels per §1.
`tracing` is not an I/O dependency (`check-no-io-deps` stays green), event macros expand to no internal clock reads (`check-no-internal-clock` stays green), and emission is write-only / state-machine-neutral (see Context — the ADR-0053 ambient-**read** objection does not apply to emission).
Proto logs cover internals that have no `ConnectionEvent` (handshake state transitions, chunk-reassembly progress) and points of detection where proto holds the richest context.

**Single-owner rule** — each fault or action logs exactly once, at the layer holding the richest context:

- **proto owns point-of-detection logs**: e.g. the CRC32C `ChecksumMismatch` `error!` lives at the decode-loop detection site in `crates/magnetar-proto/src/conn.rs` where `computed` / `expected` are in scope; redirect-chase hops log `debug!` at the chase site with hop count + URLs (truncated per §3).
- **engines own lifecycle + drained-event reporting** where proto is silent: connection established, producer/consumer create + close, reconnect supervision, DLQ republish.
- Diagnostic events that proto already logs at detection (`ChecksumMismatch`, `LookupResponse` with `LookupOutcome::Redirected`) are still drained by the engines — but consumed **silently**, fixing the unbounded queue accumulation without double-logging.

The three proto `tracing` sites that predate this ADR — the handshake `CommandError` capture (`conn.rs`), the unhandled-command `trace!` (`conn.rs`), and the out-of-range chunk-id drop (`consumer.rs`) — are grandfathered: they already conform to §1–§3 and stay untouched.

Cost accepted: any proto diff triggers ADR-0024's full four-layer test set; proto log-emission tests use a capturing subscriber with `tracing-subscriber` as a **dev-dependency**.
`check-no-io-deps` scans the full `cargo tree` including dev-dependency edges; the dev-only `tracing-subscriber` closure contains no forbidden I/O crate, and any future proto dev-dep that pulled one in would trip the gate — intentionally.

### §6 No `release_max_level_*` from the library

No magnetar crate sets `tracing`'s `release_max_level_*` (or `max_level_*`) features — they are global and propagate to downstream users via feature unification.
Applications MAY set them in their own binary to compile out `trace!` / `debug!`; §1's "nothing operator-load-bearing below `info!`" rule guarantees this is always safe.

### §7 Volume guidance

- Per-message records are confined to `trace!` (send queued, ack/nack/flow) and `debug!` (per-message DLQ detail, memory-limit rejections).
- `warn!` and above are bounded by churn (reconnects, refusals, lifecycle), never by send throughput.
- `debug!` is production-enableable: per-message `debug!` paths must stay allocation-free when the level is disabled (integer / pre-existing-`&str` fields only; the disabled-level cost is a cached callsite check plus a relaxed atomic load), and applications use per-target filtering to scope it.
- A subscriber-less embedding emits nothing.
- Rate-limiting / sampling guidance for churn storms is an open follow-up (`docs/follow-ups.md`).

## Consequences

- **Easier:** every driver action is diagnosable from logs with grep-able snake_case fields; corruption drops and redirect chases become visible; secret-leak review reduces to checking against §3's closed list; `check-log-fields` makes the ≥1-structured-field rule mechanical.
- **Harder:** every new log line needs a level argument against §1 and a field audit against §2/§3; proto log changes pay the full ADR-0024 four-layer cost; the single-owner rule requires knowing which layer already logs a fault before adding a line.
- **Costs:** one `trace!` per send on the ADR-0038 hot path (emitted with no lock held; ~1 ns when disabled); `tracing-subscriber` as a dev-dependency in the crates that capture-test their logs.
- **Incompatible with:** inline-formatted log values; `?err` on error fields; logging provider error chains on auth paths; any `release_max_level_*` feature in a workspace crate; logging while holding a slot or connection mutex on the send hot path (ADR-0038).

## References

- `xtask/src/main.rs` — `check-log-fields` gate (≥1 structured field on `error!`/`warn!`/`info!` outside `#[cfg(test)]`; parenthesis-balanced macro parsing; empty allowlist).
- `crates/magnetar-runtime-tokio/tests/logging_no_secrets.rs` + `crates/magnetar-runtime-moonpool/tests/logging_no_secrets.rs` — paired secret-scan capture tests (sentinel token / challenge bytes / provider-`Display` sentinel must not appear).
- `crates/magnetar/tests/e2e_logging.rs` — end-to-end lifecycle + no-secrets assertion against a real broker.
- `crates/magnetar-auth-athenz/src/zts.rs`, `crates/magnetar-auth-oauth2/src/lib.rs` — pre-existing conformant exemplars (§3).
- `docs/logging.md` — operator-facing companion (subscriber install, level taxonomy, field glossary).
- [ADR-0003](0003-no-channels-rule.md) — no channels (log plumbing included).
- [ADR-0004](0004-sans-io-protocol-core.md) — proto zero-I/O-deps boundary `tracing` must respect.
- [ADR-0011](0011-clock-injection-sans-io.md) — clock injection; event macros read no internal clock.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — four-layer test cost for proto log sites.
- [ADR-0038](0038-split-connection-mutex.md) — lock-ordering constraint on hot-path logging.
- [ADR-0053](0053-otel-context-propagation.md) — span/correlation boundary; ambient-read objection answered in Context.
- sozu (`github.com/sozu-proxy/sozu`, HEAD `ef93a834`) — source taxonomy: `CLAUDE.md`, `doc/observability.md`, `lib/src/logging/access_logs.rs`.
