// SPDX-License-Identifier: Apache-2.0

//! `AutoConsume` schema (Java parity stub).
//!
//! Mirrors `AutoConsumeSchema.java`. Probes the broker's schema via
//! `CommandGetSchema` and returns the payload as a `GenericRecord`. Full
//! implementation requires the schema-registry lookup pipeline integrated
//! through `Connection`; this milestone (M5) ships the trait surface and a
//! placeholder marker so downstream crates can reference the type. The runtime
//! engines wire the lookup in a follow-up.

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

/// Marker schema that defers schema resolution to the broker.
///
/// `decode()` currently returns the raw payload bytes; consumers that depend
/// on the resolved schema must use the runtime-engine façade to negotiate the
/// active schema version.
#[derive(Debug, Default, Clone)]
pub struct AutoConsumeSchema {
    _private: (),
}

impl AutoConsumeSchema {
    /// Construct an `AutoConsume` schema marker.
    #[must_use]
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Schema for AutoConsumeSchema {
    type Owned = Bytes;

    fn schema_type(&self) -> pb::schema::Type {
        pb::schema::Type::AutoConsume
    }

    fn schema_data(&self) -> Bytes {
        Bytes::new()
    }

    fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError> {
        Ok(value.clone())
    }

    fn decode(&self, bytes: &[u8]) -> Result<Self::Owned, SchemaError> {
        Ok(Bytes::copy_from_slice(bytes))
    }
}
