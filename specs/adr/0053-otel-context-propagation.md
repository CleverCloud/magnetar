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

### §D2 — re-inject on retry-letter and dead-letter paths

`reconsume_later` and `republish_dead_letters` build a fresh `OutgoingMessage` from the inbound message (cloning its properties), bypassing the `From` conversion and the send-boundary injection.
Left alone they carry the **inbound** producer's `traceparent` forever, so a retried/dead-lettered message would be traced under the original publish rather than the consumer that retried it.

- On the façade **tokio** `TypedConsumer`, the current span context is re-injected on `reconsume_later`, `reconsume_later_with_properties`, and `republish_dead_letters`, replacing the inbound `traceparent` / `tracestate` (override on key collision) **when the consumer has an active span**.
  This follows standard OTel injection semantics: with no active span (or no installed propagator) nothing is written and the inbound trace is left intact — the same behaviour as the producer send path, which also no-ops without an active span.
  The original trace stays reachable through the `REAL_TOPIC` / `ORIGINAL_MESSAGE_ID` correlation properties these paths always stamp.
- The merge happens at the façade (it routes the injected properties into the runtime), so the runtime crates stay OTel-agnostic.
  The runtime gains a `republish_dead_letters_with_properties` sibling (symmetric across both engines) that stamps caller-supplied properties; both engines share a `Consumer::apply_property_overrides` helper, used uniformly for the caller overrides **and** the `RECONSUMETIMES` / `REAL_TOPIC` / `ORIGINAL_MESSAGE_ID` correlation stamps, so the correlation stamps always win over (and never duplicate) a caller-supplied value, byte-identically on each engine.
- The base `magnetar_runtime_tokio::Consumer` (returned by `client.consumer(..).subscribe()`) and the engine-generic `MultiTopicsConsumer` do **not** auto-inject — same rationale as the direct-producer caveat (no ambient OTel reads on engine-generic / moonpool-reachable paths).
  Callers inject manually via `otel::inject_context` into the `*_with_properties` variants.

### §E1 — bound peer-controlled `tracestate`

Inbound `tracestate` is peer-controlled and the Rust `opentelemetry` propagator does not enforce the W3C 32-member limit.
`extract_context` truncates any over-long `tracestate` to 32 list-members (dropping the right-most, per the W3C truncation order) before handing it to the propagator, bounding the parse work a hostile peer can force.
The fast path allocates nothing when the value is already within the cap.

### §E2 — treat inbound trace context as untrusted

Inbound trace context is not authenticated.
A peer can force-sample (ingest-cost amplification) by setting the `sampled` flag, or pollute traces.
A `ParentBased` sampler does **not** mitigate this — it honours a sampled remote parent unconditionally, and its root delegate only applies when there is no parent.
The guidance (documented in `crates/magnetar/src/otel.rs` and `docs/observability.md`) is therefore to not attach untrusted inbound context as the sampling parent (use a span link or a fresh root), or to use a custom `ShouldSample` sampler that ignores / rate-limits remote-sampled parents, or to strip the inbound `sampled` bit at a hard trust boundary.

## Consequences

- **Easier:** Users get automatic W3C trace propagation on every tokio send path by enabling a single feature flag.
- **Harder:** Direct `producer.send(msg.into())` bypasses the façade convenience methods; users on that path must call `otel::inject_context` manually before `.into()`.
  This is documented.
- **Moonpool:** Unaffected — the moonpool engine does not inject OTel context, preserving ADR-0011 / ADR-0024 determinism guarantees.
- **Wire contract:** `traceparent` / `tracestate` properties are W3C Trace Context Level 1 encoded; the value space is fully defined by the installed propagator (default: W3C `TraceContextPropagator`).
- **Dependency posture:** Only the `opentelemetry` API crate is pulled in (not the SDK).
  The SDK is a dev-dependency for tests.
- **Retry/DLQ (§D2):** Retried and dead-lettered messages are traced under the retrying consumer's span on the façade `TypedConsumer`; the base runtime consumer + multi-topics paths remain manual.
  Property-level OTel equivalence is not observable through the differential `EventStream` (`Event::Received` carries no properties), so that layer guards engine equivalence / no-injection while the property contract is pinned by the proto, runtime-unit, and e2e layers.
- **Security (§E1/§E2):** Inbound `tracestate` is bounded; sampling/trust guidance is documented, not enforced in code (the sampler is the application's to configure).

## References

- `crates/magnetar/src/otel.rs` — inject / extract implementation, `tracestate` cap (§E1), security guidance (§E2).
- `crates/magnetar/src/engine/tokio.rs` — tokio `ProducerApi::send` injection site.
- `crates/magnetar/src/client.rs` — direct send path injection sites; `crate::inject_otel_context` shim.
- `crates/magnetar/src/typed.rs` — `TypedConsumer` retry/DLQ re-injection (§D2).
- `crates/magnetar-runtime-{tokio,moonpool}/src/consumer.rs` — `republish_dead_letters_with_properties` + `apply_property_overrides` (§D2).
- `docs/observability.md` — user-facing how-to.
- [ADR-0011](0011-clock-injection-sans-io.md) — clock injection / determinism policy.
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — cross-runtime test policy.
- `ARCHITECTURE.md §Known non-determinism leaks` — leak #3 documentation.
