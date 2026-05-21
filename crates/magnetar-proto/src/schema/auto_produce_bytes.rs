// SPDX-License-Identifier: Apache-2.0

//! `AutoProduceBytes` schema — broker-driven schema lookup (PIP-87).
//!
//! Mirrors `org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema`. Lets the producer
//! publish opaque bytes against an existing topic schema: the broker validates the schema family
//! (Avro, JSON, Protobuf) on the producer's behalf, but no schema bytes are sent inline.
//!
//! On first publish the runtime engine issues a
//! [`CommandGetSchema`](crate::pb::CommandGetSchema) via
//! [`Connection::get_schema`](crate::conn::Connection::get_schema) to learn the topic's declared
//! schema, then caches the resolved [`pb::Schema`] inside the [`AutoProduceBytesSchema`]
//! instance for the lifetime of the producer. Subsequent encodes reuse the cache without further
//! broker traffic.
//!
//! The cache is shared via `Arc<Mutex<Option<pb::Schema>>>` so the schema instance can sit behind
//! `Arc<dyn Schema>` (see [`Schema`]) while still being mutated by the runtime driver task.
//!
//! # No-channels invariant
//!
//! Per [`GUIDELINES.md`] the sans-io core uses `Arc<Mutex<…>>` for shared mutable state and
//! `Notify`/Waker slabs for cross-task signalling. The cache follows the same pattern.
//!
//! [`GUIDELINES.md`]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/GUIDELINES.md

use std::sync::{Arc, Mutex};

use bytes::Bytes;

use super::{Schema, SchemaError};
use crate::pb;

/// Producer-side schema whose actual definition is looked up from the broker's registry on first
/// use. Encoding is pass-through (the producer feeds already-encoded bytes); the cache exists so
/// the runtime can validate compatibility with the topic's declared schema family without sending
/// schema bytes on every connect.
#[derive(Debug, Default, Clone)]
pub struct AutoProduceBytesSchema {
    cache: Arc<Mutex<Option<pb::Schema>>>,
}

impl AutoProduceBytesSchema {
    /// Construct an `AutoProduceBytes` schema marker. The cache starts empty; the runtime must
    /// call [`AutoProduceBytesSchema::set_cached_schema`] once the broker has resolved the
    /// schema.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(None)),
        }
    }

    /// Populate the cache with the broker-resolved [`pb::Schema`].
    ///
    /// Called by the runtime engine after a successful
    /// [`Connection::get_schema`](crate::conn::Connection::get_schema) round-trip.
    pub fn set_cached_schema(&self, schema: pb::Schema) {
        if let Ok(mut guard) = self.cache.lock() {
            *guard = Some(schema);
        }
    }

    /// Returns a snapshot of the cached schema, if it has been resolved.
    #[must_use]
    pub fn cached_schema(&self) -> Option<pb::Schema> {
        self.cache.lock().ok().and_then(|g| g.clone())
    }

    /// Returns `true` if the broker schema has already been resolved and cached.
    #[must_use]
    pub fn has_cached_schema(&self) -> bool {
        self.cache.lock().map(|g| g.is_some()).unwrap_or(false)
    }

    /// Clears the cache, forcing the next encode/decode round to fetch the schema again.
    pub fn invalidate_cache(&self) {
        if let Ok(mut guard) = self.cache.lock() {
            *guard = None;
        }
    }
}

impl Schema for AutoProduceBytesSchema {
    type Owned = Bytes;

    fn schema_type(&self) -> pb::schema::Type {
        // Pulsar's proto stops at AutoConsume (21); AutoProduceBytes does not have its own enum
        // variant — Java's `AutoProduceBytesSchema` sends `None` because the broker performs no
        // inline schema validation. Mirror that.
        pb::schema::Type::None
    }

    fn schema_data(&self) -> Bytes {
        // Mirror Java parity: even though the producer does not send schema bytes inline, the
        // cached schema's bytes are surfaced for diagnostics / introspection. Empty until the
        // `CommandGetSchema` round-trip has completed.
        self.cache
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(|s| Bytes::copy_from_slice(&s.schema_data)))
            .unwrap_or_default()
    }

    fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError> {
        // Encoding passes through whether or not the cache is populated: the producer hands the
        // broker pre-encoded payloads matching the declared schema family. The broker is the
        // authority on compatibility; the cache exists for client-side diagnostics and to allow
        // future PIP-87-style validation hooks without changing the wire format.
        Ok(value.clone())
    }

    fn decode(&self, bytes: &[u8]) -> Result<Self::Owned, SchemaError> {
        Ok(Bytes::copy_from_slice(bytes))
    }

    fn needs_broker_schema(&self) -> bool {
        // Producer-side schemas use the broker round-trip for compatibility validation only —
        // encode is pass-through whether or not the cache is populated. We still report the
        // miss so the runtime can warm the cache on first send for diagnostics symmetry with
        // [`AutoConsumeSchema`].
        !self.has_cached_schema()
    }

    fn store_resolved_schema(&self, schema: pb::Schema) {
        self.set_cached_schema(schema);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> pb::Schema {
        pb::Schema {
            name: "test.topic-schema".to_owned(),
            schema_data: b"{\"type\":\"record\",\"name\":\"X\",\"fields\":[]}".to_vec(),
            r#type: pb::schema::Type::Avro as i32,
            properties: Vec::new(),
        }
    }

    #[test]
    fn encode_passes_through_regardless_of_cache_state() {
        let schema = AutoProduceBytesSchema::new();
        let payload = Bytes::from_static(b"avro-encoded-bytes");

        // Empty cache: encode still succeeds — the broker validates server-side.
        assert!(!schema.has_cached_schema());
        let encoded_before = schema.encode(&payload).expect("encode succeeds pre-lookup");
        assert_eq!(encoded_before, payload);
        assert!(schema.schema_data().is_empty());

        // After the broker lookup completes, encode behaves identically (pass-through), but the
        // cache is now observable for diagnostics.
        schema.set_cached_schema(sample_schema());
        assert!(schema.has_cached_schema());
        let encoded_after = schema
            .encode(&payload)
            .expect("encode still passes through");
        assert_eq!(encoded_after, payload);
        assert_eq!(
            schema.schema_data().as_ref(),
            sample_schema().schema_data.as_slice(),
        );
        assert_eq!(schema.schema_type(), pb::schema::Type::None);

        // Roundtrip via decode for completeness.
        let decoded = schema
            .decode(&encoded_after)
            .expect("decode is also pass-through");
        assert_eq!(decoded, payload);
    }
}
