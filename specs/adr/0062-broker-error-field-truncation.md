# ADR-0062 — Bound broker-supplied handshake error text at the proto capture site

- **Status**: Accepted
- **Date**: 2026-06-09
- **Decider**: Florentin Dubois
- **Tags**: logging, security, runtime, proto, sans-io, reconnect

## Context

[ADR-0054](0054-logging-policy.md) §3 ("Broker-controlled string sanitization") bounds hostile-peer-controlled broker text to 256 bytes (cut at a `char` boundary) — a log-injection and cardinality defense mirroring sozu's render-time sanitization.
As written, that rule covered only `tracing` **log fields**: the proto helper `magnetar_proto::log_fields::truncate_broker_str` is applied at the point-of-detection log sites (e.g. the redirect-chase `debug!` in `crates/magnetar-proto/src/conn.rs`).

One broker-text sink escaped the rule: the mid-handshake `CommandError`.
When a broker rejects `CommandConnect` / `CommandAuthChallenge` with a `CommandError` (proxy auth rejection, namespace-not-found via `proxy_to_broker_url`, etc.), `Connection::handle_bytes` builds the broker's `message` into a `reason` String and stores it **unbounded** in `Connection::handshake_failure_reason` (`crates/magnetar-proto/src/conn.rs`).
The adjacent `warn!` then logged the broker `message` field, and — more importantly — every downstream consumer of the stored reason inherited the unbounded text:

- the tokio engine surfaces it as `ClientError::Other("handshake failed: {reason}")` (`crates/magnetar-runtime-tokio/src/client.rs`);
- the moonpool engine surfaces it as `EngineError::HandshakeFailed(reason)`, whose `Display` is `"handshake failed: {0}"` (`crates/magnetar-runtime-moonpool/src/lib.rs`).

These are **error fields / connect errors**, not log fields, so ADR-0054 §3 — phrased as a log-field rule — did not reach them.
A hostile broker could therefore inflate a returned `ClientError` / `EngineError` (and the `warn!` field) with an arbitrarily long message.
ADR-0054 §3 already chose 256 bytes at a `char` boundary as the house bound; the fix is to **complete** that decision, not invent a new policy.

This ADR also fixes a pre-existing capture-vs-terminal-drop race in the tokio connect path that left the captured reason stranded.
A broker that sends the `CommandError` and then drops the socket made the tokio driver's read return 0; the driver returned the generic `ClientError::PeerClosed`, and the terminal `Connection::fail_all_pending` reason was computed from that generic string — discarding the captured `handshake_failure_reason`.
The `ConnectedFut` connect future then surfaced the opaque `"peer closed the connection"` instead of the broker's explanation.
(The moonpool `connect_plain` path already consults `handshake_failure_reason()` inline at EOF, so only the tokio path was affected; this is why the tokio `connect_surfaces_handshake_failure_reason_from_broker_command_error` integration test was red on `main` while its moonpool twin was green.)

## Decision

### 1. Bound the broker text ONCE at the proto capture site

In `Connection::handle_bytes`'s mid-handshake `CommandError` arm (`crates/magnetar-proto/src/conn.rs`), apply `log_fields::truncate_broker_str` to the broker `message` **before** it is built into `reason` and stored in `handshake_failure_reason`, and use the same bounded value in the adjacent `warn!` field.
`truncate_broker_str` is the existing helper (in use right below for redirect URLs); `MAX_BROKER_STR = 256` is promoted to `pub(crate)` so the proto unit test can pin the bound.

Because the bound is applied at the single capture point, every downstream consumer inherits it automatically — the tokio `ClientError::Other`, the moonpool `EngineError::HandshakeFailed`, and the `warn!` field are all bounded with no per-sink change.

### 2. Surface the captured reason past the terminal-drop race (tokio)

`crates/magnetar-runtime-tokio/src/driver.rs` gains a `terminal_reason(conn, outcome)` helper used by BOTH the plain `spawn` and the `spawn_supervised` terminal-exit paths: it prefers `conn.handshake_failure_reason()` over the generic inner-loop error string when computing the `fail_all_pending` reason.
`ConnectedFut` (`crates/magnetar-runtime-tokio/src/client.rs`) and its `ConnectionEvent::Closed` arm now both route through a `handshake_failure_message(conn)` helper that wraps the captured reason in the `"handshake failed: {reason}"` envelope, so the broker's explanation surfaces regardless of which terminalization path (the `Closed` event or the `Failed` state) wins the race.
The reason is already length-bounded by decision (1); no truncation is repeated in the engine.

### 3. Audit breadth — bound broker-text sinks only

Every error-field sink in both engines that can carry broker text was classified:

- **Broker-text (bounded, route through the §1 capture):** `handshake_failure_reason` → tokio `client.rs` `ClientError::Other` and moonpool `lib.rs` `EngineError::HandshakeFailed` (two sites). The `ConnectedFut::Closed` and `ProducerReady`/`SubscribeAcked` `Closed` arms now also carry the bounded reason via the terminal-exit reason.
- **Local-error (NOT bounded — bounding would be the wrong fix):** the supervisor `error = %err` sites (`driver.rs`: reconnect-failed dial / TLS / DNS, URL-parse fallback, `begin_handshake`-after-reset) wrap LOCAL errors (`std::io::Error`, `ParsedUrl::parse`, `ProtocolError`), never broker text. The send-path `error = %err` sites (`producer.rs`, `auto_cluster_failover.rs`) wrap local state-machine / encryption errors.
- **Out of scope (request-correlated, not the handshake path):** `ClientError::Broker { code, message }` carries the request-correlated `OpOutcome::Error.message` (a different post-handshake error path). It is not part of the handshake-reason capture this ADR reworks and is left unchanged; a future decision may extend the bound to it.

## Consequences

- A hostile broker can no longer inflate a returned `ClientError` / `EngineError` or the handshake `warn!` field with an arbitrarily long message; the bound is the same 256-byte `char`-boundary ceiling ADR-0054 §3 already chose.
- A **short** broker message still round-trips verbatim — the bound is a ceiling, not a fixed-width truncation — so the operator keeps a useful (just length-capped) explanation.
- The tokio connect future now reliably surfaces the broker's handshake-rejection reason instead of `"peer closed the connection"`. This sharpens a previously-opaque surfaced error; callers matching on the exact `ClientError::Other` text of a mid-handshake broker rejection now see the enriched `"handshake failed: broker rejected handshake (server_error=…): …"` message.
- `magnetar-proto` stays zero-I/O ([ADR-0004](0004-sans-io-protocol-core.md)) — `truncate_broker_str` is a pure slice helper — and emits no new internal clock read ([ADR-0011](0011-clock-injection-sans-io.md)).
- This **completes** [ADR-0054](0054-logging-policy.md) §3 for the one broker-text sink that escaped it (the handshake-reason / connect-error path); ADR-0054 §3 carries an append-only amendment pointer back to this ADR. Neither ADR is superseded.

### Test coverage (ADR-0024 four layers + e2e)

- **proto unit** (`crates/magnetar-proto/src/conn.rs`): `handshake_failure_reason_bounds_oversized_broker_message` feeds a >256-byte `é`-repeat `CommandError.message` and asserts the stored reason's embedded broker text is bounded at a `char` boundary; `handshake_failure_reason_preserves_short_broker_message` pins the verbatim short round-trip.
- **tokio integration** (`crates/magnetar-runtime-tokio/tests/handshake_error_capture.rs`): `connect_bounds_oversized_broker_handshake_message` (new) proves the surfaced `ClientError::Other` is bounded; the existing `connect_surfaces_handshake_failure_reason_from_broker_command_error` (red on `main`, now green) keeps the short verbatim round-trip and is the regression for the terminal-drop race.
- **moonpool integration** (`crates/magnetar-runtime-moonpool/tests/handshake_error_capture.rs`): the 1:1 twin `connect_plain_bounds_oversized_broker_handshake_message` asserts `EngineError::HandshakeFailed` is bounded. `check-runtime-test-parity` stays strictly 1:1 (260/260).
- **differential** (`crates/magnetar-differential/tests/handshake_error_bound_equivalence.rs`): asserts both engines bound the broker reason byte-identically, by feeding the SAME oversized broker bytes into the shared proto capture and applying each engine's exact sink transformation.
- **e2e**: a real broker always completes the handshake, so a mid-handshake `CommandError`-then-drop is not reproducible against `apachepulsar/pulsar:4.0.4` without a fault-injecting proxy. The bound is proven by the unit / integration / differential layers above; e2e is deferred (no real-broker handshake-rejection fixture exists, mirroring the ADR-0061 stub-acceptor rationale).

## Alternatives considered

- **Bound at each engine sink instead of the proto capture.** Rejected: it duplicates the bound across both engines and leaves the stored proto reason (and its `warn!` field) unbounded — the wrong layer, against Florentin's root-cause-first preference.
- **Add a new max-length policy const.** Rejected: ADR-0054 §3 already fixed 256 bytes at a `char` boundary as the house bound; this ADR completes that decision rather than introducing a parallel knob.
- **Also bound `ClientError::Broker { message }`.** Out of scope: that is a request-correlated post-handshake error path, not the handshake-reason capture this ADR reworks; folding it in would be a broader, separately-justified change.
