// SPDX-License-Identifier: Apache-2.0

//! Schema serialisers — Pulsar `Schema<T>` parity for the magnetar workspace.
//!
//! This module exposes the [`Schema`] trait and the nine concrete implementations advertised by
//! the Apache Pulsar Java client (`org.apache.pulsar.client.api.Schema`): [`BytesSchema`],
//! [`StringSchema`], [`JsonSchema`], [`AvroSchema`], [`ProtobufSchema`], [`ProtobufNativeSchema`],
//! [`KeyValueSchema`], [`AutoConsumeSchema`], and [`AutoProduceBytesSchema`].
//!
//! # Wire shape
//!
//! Each schema produces two artefacts that the producer and the consumer thread into
//! `CommandProducer.schema` / `CommandSubscribe.schema`:
//!
//! 1. [`Schema::schema_type`] — the `pb::schema::Type` discriminant advertised to the broker.
//! 2. [`Schema::schema_data`] — the **canonical wire bytes** that identify the schema version
//!    inside the broker's schema registry.
//!
//! # Codex Q4 — canonical byte equality
//!
//! Per Codex cross-check on `SchemaRegistryServiceImpl.java:405-438`:
//!
//! - **AVRO / JSON / PROTOBUF** schemas are re-parsed broker-side via Avro `Schema.Parser` before
//!   the version lookup. Magnetar emits the Avro parsing canonical form for `AvroSchema` so two
//!   logically-identical schemas hash to the same version regardless of whitespace, field order, or
//!   property ordering.
//! - **PROTOBUF_NATIVE** and **KeyValue** are stored as opaque blobs and compared by **raw-byte
//!   equality**. The Java client emits a `FileDescriptorSet` for `PROTOBUF_NATIVE` and a stable
//!   JSON shape (`{"key": ..., "value": ..., "keyValueEncodingType": ...}`) for `KeyValue`.
//!   Magnetar must emit byte-identical output or the broker will create a fresh schema version on
//!   every (re)connect, defeating the registry's deduplication.
//!
//! The relevant invariant is also tracked in `GUIDELINES.md` ("Protocol-correctness invariants",
//! point 6) and `ARCHITECTURE.md` ("Schema-registry parity").
//!
//! # Implementation status
//!
//! - Fully implemented (inline schema data): [`BytesSchema`], [`StringSchema`], [`JsonSchema`],
//!   [`AvroSchema`], [`ProtobufSchema`], [`ProtobufNativeSchema`], [`KeyValueSchema`].
//! - Broker-driven schema lookup (PIP-87 — schema cached in an `Arc<Mutex<Option<pb::Schema>>>`
//!   populated by [`Connection::get_schema`](crate::conn::Connection::get_schema)):
//!   [`AutoConsumeSchema`], [`AutoProduceBytesSchema`].

use bytes::Bytes;

use crate::pb;

mod auto_consume;
mod auto_produce_bytes;
mod avro;
mod bytes_schema;
mod json;
mod key_value;
mod primitive;
mod protobuf;
mod protobuf_native;
mod string;

pub use self::auto_consume::{AutoConsumeSchema, GenericRecord, GenericRecordField};
pub use self::auto_produce_bytes::AutoProduceBytesSchema;
pub use self::avro::AvroSchema;
pub use self::bytes_schema::BytesSchema;
pub use self::json::JsonSchema;
pub use self::key_value::{KeyValueEncodingType, KeyValuePair, KeyValueSchema};
pub use self::primitive::{
    BoolSchema, DateSchema, DoubleSchema, FloatSchema, InstantSchema, Int8Schema, Int16Schema,
    Int32Schema, Int64Schema, LocalDateSchema, LocalDateTimeSchema, LocalTimeSchema, TimeSchema,
    TimestampSchema,
};
pub use self::protobuf::ProtobufSchema;
pub use self::protobuf_native::ProtobufNativeSchema;
pub use self::string::StringSchema;

/// Errors raised by [`Schema::encode`] / [`Schema::decode`] and the schema constructors.
#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    /// Failed to encode a value into the schema's wire form.
    #[error("encoding error: {0}")]
    Encoding(String),

    /// Failed to decode a value from the wire bytes.
    #[error("decoding error: {0}")]
    Decoding(String),

    /// The provided value does not match the schema (e.g. UTF-8 mismatch on `StringSchema`,
    /// type-tag mismatch on `KeyValueSchema`).
    #[error("schema mismatch: expected {expected}, got {actual}")]
    Mismatch {
        /// The expected schema descriptor (e.g. `"String"`, `"Avro:record"`).
        expected: String,
        /// What was actually presented (e.g. `"invalid utf-8 at index 3"`).
        actual: String,
    },

    /// The schema type is recognised but cannot be served in this context (e.g. `AutoConsume`
    /// before a broker lookup has resolved the underlying schema).
    #[error("unsupported schema operation: {0}")]
    Unsupported(String),
}

/// Trait advertised on every concrete Pulsar schema.
///
/// `Self::Owned` is the user-facing Rust type a producer hands to [`Schema::encode`] and a
/// consumer receives from [`Schema::decode`]. The `'static` bound makes the type usable from
/// inside `Box<dyn Schema>` slots — the engine machinery uses dynamic dispatch when it does not
/// know the value type statically (e.g. `AutoConsumeSchema`).
///
/// The trait is `Send + Sync` so schema instances can sit behind an `Arc` shared across the
/// driver task and user-facing futures.
pub trait Schema: Send + Sync + std::fmt::Debug {
    /// The Rust type produced by [`Schema::decode`] and accepted by [`Schema::encode`].
    type Owned: Send + 'static;

    /// The `pb::schema::Type` value advertised on `CommandProducer` and `CommandSubscribe`.
    fn schema_type(&self) -> pb::schema::Type;

    /// Canonical wire bytes for this schema, byte-identical to the Java client output for the
    /// purpose of broker-side de-duplication. See the [module docs](crate::schema) for the
    /// canonicalisation requirements per schema family.
    fn schema_data(&self) -> Bytes;

    /// Encode `value` into the schema's wire form.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::Encoding`] if serialisation fails (e.g. an Avro value that does not
    /// match the parsed schema, a protobuf message that fails to encode).
    fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError>;

    /// Decode `bytes` into the schema's owned type.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::Decoding`] if deserialisation fails or [`SchemaError::Mismatch`]
    /// if the bytes do not satisfy schema-level invariants (e.g. invalid UTF-8 for
    /// [`StringSchema`]).
    fn decode(&self, bytes: &[u8]) -> Result<Self::Owned, SchemaError>;

    /// Optional schema-level metadata for the broker — surfaces in
    /// `CommandProducer.schema.properties` and `CommandSubscribe.schema.properties`.
    ///
    /// Most schemas return an empty list (the default). KeyValue schemas use this to carry
    /// `key.schema.name`, `key.schema.type`, `key.schema.properties`, the equivalent
    /// `value.*` triple, and `kv.encoding.type` — Pulsar's broker only validates KeyValue
    /// schemas when these properties are present.
    fn properties(&self) -> Vec<(String, String)> {
        Vec::new()
    }

    /// Reports whether this schema still needs a broker-side `CommandGetSchema` round-trip
    /// before it can decode payloads. Default is `false` — inline schemas (Avro, JSON,
    /// primitives) carry their definition with them and never need a broker lookup.
    ///
    /// PIP-87 schemas ([`AutoConsumeSchema`], [`AutoProduceBytesSchema`]) override this to
    /// return `true` while their cache is empty so the runtime can issue
    /// [`Connection::get_schema`](crate::conn::Connection::get_schema) on the consumer's
    /// first receive. Mirrors Java's `AutoConsumeSchema#requireFetchingSchemaInfo`.
    #[must_use]
    fn needs_broker_schema(&self) -> bool {
        false
    }

    /// Populate the broker-resolved [`pb::Schema`] into this schema instance's cache.
    /// Default is a no-op so inline schemas can ignore the hook.
    ///
    /// PIP-87 schemas override this to push the resolved schema into their
    /// `Arc<Mutex<Option<pb::Schema>>>` cache. The runtime calls this after a successful
    /// [`Connection::get_schema`](crate::conn::Connection::get_schema) round-trip. Mirrors
    /// Java's `AutoConsumeSchema#setSchemaInfoProvider` populate path.
    fn store_resolved_schema(&self, _schema: pb::Schema) {}
}
