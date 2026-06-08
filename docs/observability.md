# Observability — OpenTelemetry context propagation

Magnetar can propagate W3C Trace Context across the producer → broker → consumer hop through Pulsar message properties.
The feature is opt-in behind the `opentelemetry` Cargo feature (default off) and is specified by [ADR-0053](../specs/adr/0053-otel-context-propagation.md).
For the structured `tracing` logs the driver emits — and how they correlate with the propagated context — see [`logging.md`](logging.md) (ADR-0054).

## What it does

When the `opentelemetry` feature is enabled, the current span context is encoded into two message properties — `traceparent` and `tracestate` (W3C Trace Context Level 1) — by the installed global propagator.
On the consumer side you recover the parent context and attach it before processing.

Only the `opentelemetry` **API** crate is pulled in; magnetar does not depend on the SDK (that is a dev-dependency for tests).
Magnetar propagates context — it does not create spans.

If no propagator is installed (you never call `opentelemetry::global::set_text_map_propagator`), the default no-op propagator runs: zero properties are written and inject/extract are silent no-ops.

## Producer side — injection at the send boundary

Injection happens at the tokio **send boundary**, not inside the `From<OutgoingMessage>` conversion (which stays pure so the moonpool simulation engine is deterministic — ADR-0011 / ADR-0053).
The following façade send paths inject automatically:

- `OutgoingMessage::send(&producer)`
- `MessageBuilder::send` and `TypedMessageBuilder::send`
- `send_with_interceptors`
- the tokio `ProducerApi::send` implementation (so `TypedProducer::send` is covered)

```rust
let producer = client.producer("topic").create().await?;
// traceparent injected into the message properties at send time:
OutgoingMessage::with_payload(b"hello").send(&producer).await?;
```

A **direct** `producer.send(msg.into())` on the raw `magnetar_runtime_tokio::Producer` bypasses these convenience methods and is **not** injected — call `magnetar::otel::inject_context(&mut properties)` yourself before building the message.

## Consumer side — extraction

```rust
let msg = consumer.receive().await?;
let parent_cx = magnetar::otel::extract_context(&msg);
let _guard = parent_cx.attach();
// spans created here are children of the producer's span
```

- `extract_context` / `attach_context` accept the proto `magnetar_proto::event::IncomingMessage` returned by the tokio `Consumer::receive()`.
- `extract_context_facade` / `attach_context_facade` accept the façade `magnetar::IncomingMessage` returned by `Reader::read_next`, the V5 / typed / multi / pattern surfaces, and the interceptor paths.

> **`!Send` caveat.** The returned `opentelemetry::ContextGuard` is `!Send`.
> Do not hold it across an `.await` on a multi-threaded runtime — the future would become `!Send` and will not compile.
> Drop the guard before suspending.

## Retry-letter and dead-letter paths (ADR-0053 §D2)

Retry and DLQ republish build a fresh outgoing message from the inbound one, so without intervention they would carry the **inbound** producer's trace forever.
On the façade `TypedConsumer` (tokio), the current span context is **re-injected** on these paths so the republished copy is traced under the retrying/republishing consumer's span:

- `TypedConsumer::reconsume_later` and `reconsume_later_with_properties`
- `TypedConsumer::republish_dead_letters`

Re-injection follows standard OTel semantics: when the consumer has an **active span**, its context replaces the inbound `traceparent` / `tracestate`; with no active span (or no installed propagator) nothing is written and the inbound trace is left intact — attach the consumer's span before calling these methods, exactly as on the producer send path.
Either way, the original trace stays reachable through the `REAL_TOPIC` / `ORIGINAL_MESSAGE_ID` correlation properties that these paths always stamp.

Two paths do **not** auto-inject and need manual injection (consistent with the direct-producer caveat above):

- The base `magnetar_runtime_tokio::Consumer` returned by `client.consumer(..).subscribe()`.
  Pass an injected property vector to `reconsume_later_with_properties` / `republish_dead_letters_with_properties`:

  ```rust
  let mut props = Vec::new();
  magnetar::otel::inject_context(&mut props);
  consumer.reconsume_later_with_properties(&retry_producer, msg, props, delay).await?;
  ```

- `MultiTopicsConsumer::reconsume_later*` — engine-generic (it must stay deterministic for the moonpool engine), so it does not read ambient OTel state.
  Inject into the custom properties yourself.

The moonpool engine never injects on any path, preserving simulation determinism.

## Reserved property names

When the feature is enabled, the property names `traceparent` and `tracestate` (and any other key a composite or baggage propagator writes) are reserved for propagation.
Setting them manually on an `OutgoingMessage` has no effect — they are overwritten at send time.

## Security — treat inbound trace context as untrusted

Inbound `traceparent` / `tracestate` come from a peer you do not control.
A hostile or buggy producer can:

- **Force-sample.** Setting the `sampled` flag forces your pipeline to sample the trace, amplifying ingest cost and polluting traces.
  A `ParentBased` sampler does **not** mitigate this: it honours a sampled remote parent unconditionally, and its root delegate (e.g. `trace_id_ratio_based`) only applies when there is no parent.
  To bound a hostile peer, do not attach untrusted inbound context as the sampling parent (extract it into a span _link_ or a fresh root instead), or use a custom `ShouldSample` sampler that ignores / rate-limits remote-sampled parents, or strip the inbound `sampled` bit at the trust boundary before attaching.
- **Inflate `tracestate`.** The Rust `opentelemetry` propagator does not enforce the W3C 32-member limit.
  `extract_context` defensively truncates over-long `tracestate` (right-most members dropped, per the W3C truncation order) to 32 members before parsing (ADR-0053 §E1).

## Feature reference

| Item                    | Value                                                           |
| ----------------------- | --------------------------------------------------------------- |
| Cargo feature           | `opentelemetry` (default off)                                   |
| Dependencies pulled in  | `opentelemetry` API crate only (SDK is a dev-dependency)        |
| Wire properties         | `traceparent`, `tracestate` (W3C Trace Context Level 1)         |
| No propagator installed | silent no-op (zero properties written)                          |
| Module                  | [`crates/magnetar/src/otel.rs`](../crates/magnetar/src/otel.rs) |
| Decision record         | [ADR-0053](../specs/adr/0053-otel-context-propagation.md)       |
