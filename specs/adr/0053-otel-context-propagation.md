# ADR-0053 — OpenTelemetry context propagation via message properties

- **Status**: Accepted
- **Date**: 2026-06-04
- **Decider**: Florentin Dubois
- **Tags**: observability, opentelemetry, feature-flag, determinism

## Context

Distributed tracing across producer → broker → consumer requires propagating the W3C `traceparent` / `tracestate` headers through Pulsar message properties.
The Java client does not inject OTel context natively; users wire it manually.
Magnetar ships this as a first-class, opt-in feature.

Three design questions arose during review:

1. **Where to inject** — inside `From<OutgoingMessage>` (ambient) vs. at the send boundary (explicit).
   The ambient path reads `Context::current()` (a thread-local) and the global propagator inside a pure type conversion, violating ADR-0011's clock-injection spirit and creating a 3rd non-determinism leak.
2. **Moonpool determinism** — identical sim inputs must produce identical wire bytes.
   OTel injection from hidden thread-local / global state breaks this.
3. **Feature composition** — `opentelemetry` without `tokio` should not produce dead code or hard build errors.

## Decision

- Gate the `opentelemetry` feature on `dep:opentelemetry` only (does **not** imply `tokio`).
  The `otel` module is available with or without `tokio`; `inject_context` is public so users can inject manually.
  The façade's `extract_context` / `attach_context` work independently of the engine.
- Inject at the **send boundary**, not in `From<OutgoingMessage>`:
  - The **tokio** `ProducerApi::send` impl and the direct send convenience methods (`OutgoingMessage::send`, `MessageBuilder::send`, `send_with_interceptors`, `TypedMessageBuilder::send`) call `otel::inject_context` before the `From` conversion.
  - The **moonpool** `ProducerApi::send` impl does **not** inject — sim determinism preserved.
  - `From<OutgoingMessage>` is pure: same input, same output, no ambient state.
- Property names `traceparent` and `tracestate` are reserved when the feature is enabled; user-set values are silently overwritten.
- When no propagator is installed (`set_text_map_propagator` not called), the default no-op propagator runs — zero properties written, zero overhead.
- The new non-determinism leak (3rd) is documented in `ARCHITECTURE.md §Known non-determinism leaks`.

## Consequences

- **Easier:** Users get automatic W3C trace propagation on every tokio send path by enabling a single feature flag.
- **Harder:** Direct `producer.send(msg.into())` bypasses the façade convenience methods; users on that path must call `otel::inject_context` manually before `.into()`.
  This is documented.
- **Moonpool:** Unaffected — the moonpool engine does not inject OTel context, preserving ADR-0011 / ADR-0024 determinism guarantees.
- **Wire contract:** `traceparent` / `tracestate` properties are W3C Trace Context Level 1 encoded; the value space is fully defined by the installed propagator (default: W3C `TraceContextPropagator`).
- **Dependency posture:** Only the `opentelemetry` API crate is pulled in (not the SDK).
  The SDK is a dev-dependency for tests.

## References

- `crates/magnetar/src/otel.rs` — inject / extract implementation.
- `crates/magnetar/src/engine/tokio.rs` — tokio `ProducerApi::send` injection site.
- `crates/magnetar/src/client.rs` — direct send path injection sites.
- [ADR-0011](0011-clock-injection-sans-io.md) — clock injection / determinism policy.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — cross-runtime test policy.
- `ARCHITECTURE.md §Known non-determinism leaks` — leak #3 documentation.
