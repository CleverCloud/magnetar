// SPDX-License-Identifier: Apache-2.0

//! OpenTelemetry context propagation for Pulsar messages.
//!
//! When the `opentelemetry` feature is enabled, the current span context is
//! **automatically injected** into every outgoing message's properties during
//! the [`From<OutgoingMessage>`](crate::OutgoingMessage) conversion that every
//! send path goes through.
//!
//! On the consumer side, call [`extract_context`] to recover the parent context
//! from a received message and [`attach`](opentelemetry::Context::attach) it
//! before processing:
//!
//! ```ignore
//! let msg = consumer.receive().await?;
//! let parent_cx = magnetar::otel::extract_context(&msg);
//! let _guard = parent_cx.attach();
//! // spans created here are children of the producer's span
//! ```
//!
//! If no propagator has been installed via
//! [`opentelemetry::global::set_text_map_propagator`], the global no-op
//! propagator runs and inject / extract are silent no-ops.

use magnetar_proto::pb;
use opentelemetry::propagation::{Extractor, Injector};

// ---------------------------------------------------------------------------
// Injector: facade-level properties Vec<(String, String)>
// ---------------------------------------------------------------------------

/// Adapts `&mut Vec<(String, String)>` to the [`opentelemetry::propagation::Injector`] trait
/// so the global propagator can write `traceparent` / `tracestate` into message properties.
struct MessagePropertiesInjector<'a>(&'a mut Vec<(String, String)>);

impl Injector for MessagePropertiesInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        // Replace an existing key (idempotent) rather than appending a duplicate.
        self.0.retain(|(k, _)| k != key);
        self.0.push((key.to_owned(), value));
    }
}

// ---------------------------------------------------------------------------
// Extractor: proto-level properties Vec<pb::KeyValue>
// ---------------------------------------------------------------------------

/// Adapts `&[pb::KeyValue]` to the [`opentelemetry::propagation::Extractor`] trait
/// so the global propagator can read `traceparent` / `tracestate` from received message metadata.
struct MetadataPropertiesExtractor<'a>(&'a [pb::KeyValue]);

impl Extractor for MetadataPropertiesExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|kv| kv.key == key)
            .map(|kv| kv.value.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|kv| kv.key.as_str()).collect()
    }
}

/// Inject the current OpenTelemetry span context into message properties.
///
/// Called automatically during the [`From<OutgoingMessage>`](crate::OutgoingMessage)
/// conversion. No-op when no propagator is installed.
pub(crate) fn inject_context(properties: &mut Vec<(String, String)>) {
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(
            &opentelemetry::Context::current(),
            &mut MessagePropertiesInjector(properties),
        );
    });
}

/// Extract a parent OpenTelemetry context from a received message's properties.
///
/// Returns a root (empty) context when the message carries no propagation
/// headers or when no propagator is installed.
///
/// Use this when you need the raw [`opentelemetry::Context`] (e.g. to pass
/// it to a span builder). For the common case of attaching it as the
/// current context for the duration of message processing, prefer
/// [`attach_context`].
pub fn extract_context(msg: &magnetar_proto::event::IncomingMessage) -> opentelemetry::Context {
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&MetadataPropertiesExtractor(&msg.metadata.properties))
    })
}

/// Extract the parent context from a received message and
/// [`attach`](opentelemetry::Context::attach) it as the current context.
///
/// The returned guard resets the context when dropped, so hold it for the
/// entire message-processing scope:
///
/// ```ignore
/// let msg = consumer.receive().await?;
/// let _guard = magnetar::otel::attach_context(&msg);
/// // spans created here are children of the producer's span
/// ```
///
/// No-op (returns immediately) when no propagator is installed.
pub fn attach_context(
    msg: &magnetar_proto::event::IncomingMessage,
) -> opentelemetry::ContextGuard {
    extract_context(msg).attach()
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    };

    fn dummy_incoming(proto_props: Vec<pb::KeyValue>) -> magnetar_proto::event::IncomingMessage {
        let metadata = magnetar_proto::pb::MessageMetadata {
            properties: proto_props,
            ..Default::default()
        };
        magnetar_proto::event::IncomingMessage {
            message_id: magnetar_proto::MessageId {
                ledger_id: 0,
                entry_id: 0,
                partition: -1,
                batch_index: -1,
                batch_size: -1,
                #[cfg(feature = "scalable-topics")]
                segment_id: None,
            },
            metadata: std::sync::Arc::new(metadata),
            single_metadata: None,
            payload: bytes::Bytes::new(),
            redelivery_count: 0,
            broker_entry_metadata: None,
            arrived_at: std::time::Instant::now(),
        }
    }

    /// Round-trip: inject with the W3C TraceContext propagator, then extract.
    /// Verifies that `traceparent` survives the facade → proto property conversion.
    #[test]
    fn inject_extract_round_trip() {
        let _guard = PropagatorGuard::install();

        let trace_id = TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap();
        let span_id = SpanId::from_hex("00f067aa0ba902b7").unwrap();
        let span_ctx = SpanContext::new(
            trace_id,
            span_id,
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );

        let otel_ctx = opentelemetry::Context::current().with_remote_span_context(span_ctx.clone());
        let _attached = otel_ctx.attach();

        let mut properties: Vec<(String, String)> = Vec::new();
        inject_context(&mut properties);

        assert!(
            properties.iter().any(|(k, _)| k == "traceparent"),
            "expected traceparent in properties: {properties:?}"
        );

        let proto_props: Vec<pb::KeyValue> = properties
            .into_iter()
            .map(|(k, v)| pb::KeyValue { key: k, value: v })
            .collect();

        let incoming = dummy_incoming(proto_props);
        let extracted_cx = extract_context(&incoming);
        let extracted_sc = extracted_cx.span().span_context().clone();

        assert_eq!(extracted_sc.trace_id(), span_ctx.trace_id());
        assert_eq!(extracted_sc.span_id(), span_ctx.span_id());
        assert!(extracted_sc.trace_flags().is_sampled());
    }

    /// When no propagator is installed (default no-op), inject produces no properties.
    #[test]
    fn no_propagator_is_noop() {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry::trace::noop::NoopTextMapPropagator::new(),
        );

        let mut properties: Vec<(String, String)> = Vec::new();
        inject_context(&mut properties);
        assert!(
            properties.is_empty(),
            "no-op propagator should inject nothing"
        );
    }

    /// Injecting twice replaces the key rather than duplicating it.
    #[test]
    fn inject_is_idempotent() {
        let _guard = PropagatorGuard::install();

        let span_ctx = SpanContext::new(
            TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap(),
            SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let otel_ctx = opentelemetry::Context::current().with_remote_span_context(span_ctx);
        let _attached = otel_ctx.attach();

        let mut properties: Vec<(String, String)> = Vec::new();
        inject_context(&mut properties);
        inject_context(&mut properties);

        let traceparent_count = properties
            .iter()
            .filter(|(k, _)| k == "traceparent")
            .count();
        assert_eq!(
            traceparent_count, 1,
            "traceparent should appear exactly once"
        );
    }

    /// Extract from a message with no `traceparent` returns a root context.
    #[test]
    fn extract_missing_traceparent_returns_root() {
        let _guard = PropagatorGuard::install();

        let incoming = dummy_incoming(vec![]);
        let cx = extract_context(&incoming);
        let sc = cx.span().span_context().clone();
        assert!(!sc.is_valid(), "expected invalid (root) span context");
    }

    // --- test helpers ---

    struct PropagatorGuard;

    impl PropagatorGuard {
        fn install() -> Self {
            opentelemetry::global::set_text_map_propagator(
                opentelemetry_sdk::propagation::TraceContextPropagator::new(),
            );
            Self
        }
    }

    impl Drop for PropagatorGuard {
        fn drop(&mut self) {
            opentelemetry::global::set_text_map_propagator(
                opentelemetry::trace::noop::NoopTextMapPropagator::new(),
            );
        }
    }
}
