// SPDX-License-Identifier: Apache-2.0

//! `AutoConsume` schema — broker-driven schema lookup (PIP-87).
//!
//! Mirrors `org.apache.pulsar.client.impl.schema.AutoConsumeSchema`. Unlike inline schemas (Avro,
//! JSON, primitives), this variant does **not** carry the topic's schema definition inline. Instead
//! it relies on the broker's schema registry: on first poll the runtime engine issues a
//! [`CommandGetSchema`](crate::pb::CommandGetSchema) via
//! [`Connection::get_schema`](crate::conn::Connection::get_schema), then caches the resolved
//! [`pb::Schema`] inside the [`AutoConsumeSchema`] instance for the lifetime of the consumer.
//!
//! The cache is shared via `Arc<Mutex<Option<pb::Schema>>>` so the schema instance can sit behind
//! `Arc<dyn Schema>` (see [`Schema`]) while still being mutated by the runtime driver task.
//!
//! # No-channels invariant
//!
//! Per [`GUIDELINES.md`] the sans-io core uses `Arc<Mutex<…>>` for shared mutable state and
//! `Notify`/Waker slabs for cross-task signalling. The cache here follows the same pattern: the
//! driver task fills the cache when the `CommandGetSchemaResponse` arrives and wakes any
//! consumer-side futures via the [`OpOutcome::GetSchemaResponse`](crate::OpOutcome) outcome.
//!
//! [`GUIDELINES.md`]: https://github.com/CleverCloud/magnetar/blob/main/GUIDELINES.md

use std::sync::{Arc, Mutex};

use bytes::Bytes;

use super::{Schema, SchemaError};
use crate::pb;

/// Generic record returned by an `AutoConsume` decode. Fields are name → bytes pairs because
/// the schema is resolved at runtime against the broker's registry.
#[derive(Debug, Clone, Default)]
pub struct GenericRecord {
    /// Resolved schema name (mirrors Java's `getSchemaName`).
    pub schema_name: String,
    /// Optional schema version assigned by the broker.
    pub schema_version: Option<Vec<u8>>,
    /// Decoded fields (name → byte slice). Order is preserved from the schema definition.
    pub fields: Vec<GenericRecordField>,
}

/// One field inside a [`GenericRecord`].
#[derive(Debug, Clone, Default)]
pub struct GenericRecordField {
    /// Field name (matches the schema's field name).
    pub name: String,
    /// Field value bytes (interpretation depends on `schema_name`).
    pub value: Bytes,
}

/// Schema whose actual definition is looked up from the broker's registry on first use.
///
/// The instance carries an `Arc<Mutex<Option<pb::Schema>>>` cache that the runtime engine fills
/// after the `CommandGetSchema` round-trip completes. Subsequent encode/decode calls reuse the
/// cached schema without further broker traffic.
#[derive(Debug, Default, Clone)]
pub struct AutoConsumeSchema {
    cache: Arc<Mutex<Option<pb::Schema>>>,
}

impl AutoConsumeSchema {
    /// Construct an `AutoConsume` schema marker. The cache starts empty; the runtime must call
    /// [`AutoConsumeSchema::set_cached_schema`] once the broker has resolved the schema.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(None)),
        }
    }

    /// Populate the cache with the broker-resolved [`pb::Schema`].
    ///
    /// Called by the runtime engine after a successful
    /// [`Connection::get_schema`](crate::conn::Connection::get_schema) round-trip. Overwrites any
    /// previously cached value — Java parity with `AutoConsumeSchema#setSchema`.
    pub fn set_cached_schema(&self, schema: pb::Schema) {
        if let Ok(mut guard) = self.cache.lock() {
            *guard = Some(schema);
        }
    }

    /// Returns a snapshot of the cached schema, if it has been resolved.
    ///
    /// `None` means the runtime has not yet completed the `CommandGetSchema` round-trip — the
    /// caller should wait on the `OpOutcome::GetSchemaResponse` outcome before retrying.
    #[must_use]
    pub fn cached_schema(&self) -> Option<pb::Schema> {
        self.cache.lock().ok().and_then(|g| g.clone())
    }

    /// Returns `true` if the broker schema has already been resolved and cached.
    #[must_use]
    pub fn has_cached_schema(&self) -> bool {
        self.cache.lock().map(|g| g.is_some()).unwrap_or(false)
    }

    /// Clears the cache, forcing the next encode/decode round to fetch the schema again. Used by
    /// reconnect paths that need to revalidate the broker registry view.
    pub fn invalidate_cache(&self) {
        if let Ok(mut guard) = self.cache.lock() {
            *guard = None;
        }
    }
}

impl Schema for AutoConsumeSchema {
    type Owned = Bytes;

    fn schema_type(&self) -> pb::schema::Type {
        pb::schema::Type::AutoConsume
    }

    fn schema_data(&self) -> Bytes {
        // The cache may carry the broker-resolved schema bytes; advertise them on
        // `CommandProducer` / `CommandSubscribe` once resolved so the broker can match the
        // registry-assigned version. Mirrors Java `AutoConsumeSchema#getSchemaInfo()` which
        // returns the resolved schema's bytes (or empty before resolution).
        self.cache
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(|s| Bytes::copy_from_slice(&s.schema_data)))
            .unwrap_or_default()
    }

    fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError> {
        // `AutoConsume` is a decode-side schema; encoding passes through.
        Ok(value.clone())
    }

    fn decode(&self, bytes: &[u8]) -> Result<Self::Owned, SchemaError> {
        // Java's AutoConsumeSchema demands the cache be populated before decode. Mirror that:
        // surface `Unsupported` when the broker round-trip has not yet completed.
        if !self.has_cached_schema() {
            return Err(SchemaError::Unsupported(
                "AutoConsumeSchema: broker schema not yet resolved (call Connection::get_schema first)".to_owned(),
            ));
        }
        Ok(Bytes::copy_from_slice(bytes))
    }

    fn needs_broker_schema(&self) -> bool {
        // Auto-fetch is gated on cache state — once populated, subsequent receives reuse the
        // resolved schema without further broker traffic.
        !self.has_cached_schema()
    }

    fn store_resolved_schema(&self, schema: pb::Schema) {
        // Re-uses the existing cache mutex; equivalent to `set_cached_schema` but reachable
        // through the `Schema` trait so the runtime engine can populate the cache without
        // knowing the concrete schema type.
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
    fn decode_misses_when_cache_empty() {
        let schema = AutoConsumeSchema::new();
        assert!(!schema.has_cached_schema());
        let err = schema
            .decode(b"payload")
            .expect_err("decode must fail before cache is populated");
        assert!(
            matches!(err, SchemaError::Unsupported(ref msg) if msg.contains("not yet resolved")),
            "expected Unsupported(not yet resolved), got {err:?}"
        );
        assert!(schema.schema_data().is_empty());
    }

    #[test]
    fn decode_hits_cache_after_set() {
        let schema = AutoConsumeSchema::new();
        schema.set_cached_schema(sample_schema());
        assert!(schema.has_cached_schema());
        let payload = b"hello";
        let decoded = schema
            .decode(payload)
            .expect("decode succeeds once cache is populated");
        assert_eq!(decoded.as_ref(), payload);
        assert_eq!(
            schema.schema_data().as_ref(),
            sample_schema().schema_data.as_slice(),
            "schema_data must reflect cached broker schema after lookup"
        );

        // A second decode hits the same cache (no re-lookup needed).
        let again = schema
            .decode(payload)
            .expect("second decode also hits cache");
        assert_eq!(again.as_ref(), payload);
        // Invalidation forces a miss on the next decode.
        schema.invalidate_cache();
        assert!(!schema.has_cached_schema());
        assert!(schema.decode(payload).is_err());
    }

    #[test]
    fn cache_is_shared_via_arc_clone() {
        // Cloning AutoConsumeSchema must share the cache so a Connection that holds the clone
        // can populate it and the consumer (which holds the original) sees the result. This
        // mirrors how `Arc<dyn Schema>` instances are threaded through the engine.
        let original = AutoConsumeSchema::new();
        let clone = original.clone();
        assert!(!original.has_cached_schema());
        clone.set_cached_schema(sample_schema());
        assert!(
            original.has_cached_schema(),
            "cache populated through clone must be visible through original"
        );
    }

    #[test]
    fn needs_broker_schema_toggles_with_cache_state() {
        // The PIP-87 runtime path in `TypedConsumer::receive` gates the `CommandGetSchema`
        // round-trip on `Schema::needs_broker_schema`. Empty cache -> fetch; populated cache ->
        // skip. Subsequent receives after a populated cache MUST skip the fetch (no extra
        // broker traffic).
        let schema = AutoConsumeSchema::new();
        assert!(
            schema.needs_broker_schema(),
            "empty cache must report needs_broker_schema = true"
        );
        schema.store_resolved_schema(sample_schema());
        assert!(
            !schema.needs_broker_schema(),
            "populated cache must report needs_broker_schema = false — subsequent receives reuse the cached schema without a second get_schema call"
        );
        // Invalidation re-arms the fetch (mirrors Java's reconnect-revalidate path).
        schema.invalidate_cache();
        assert!(
            schema.needs_broker_schema(),
            "post-invalidate cache must re-arm needs_broker_schema = true"
        );
    }

    #[test]
    fn store_resolved_schema_populates_cache_via_trait_hook() {
        // The runtime engine calls `Schema::store_resolved_schema` after a successful
        // `CommandGetSchema` round-trip. The trait hook must round-trip the broker-resolved
        // bytes back through `cached_schema` and `schema_data` — proving the runtime can
        // populate the cache without knowing the concrete schema type.
        let schema = AutoConsumeSchema::new();
        let broker_schema = sample_schema();
        // Round-trip via the trait API (the path the runtime uses).
        let as_trait: &dyn Schema<Owned = Bytes> = &schema;
        as_trait.store_resolved_schema(broker_schema.clone());
        assert!(schema.has_cached_schema());
        let cached = schema
            .cached_schema()
            .expect("cache populated by trait hook");
        assert_eq!(cached.name, broker_schema.name);
        assert_eq!(cached.schema_data, broker_schema.schema_data);
        assert_eq!(
            schema.schema_data().as_ref(),
            broker_schema.schema_data.as_slice(),
            "schema_data() reflects the broker-resolved bytes after trait-driven cache fill"
        );
    }
}
