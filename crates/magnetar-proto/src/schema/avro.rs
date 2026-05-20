// SPDX-License-Identifier: Apache-2.0

//! Apache Avro schema — `apache_avro` round-trip with canonical-form `schema_data`.
//!
//! Mirrors `org.apache.pulsar.client.impl.schema.AvroSchema`. Per Codex Q4 the broker re-parses
//! Avro `schema_data` through `org.apache.avro.Schema.Parser` before version lookup, so emitting
//! the [Parsing Canonical Form] is sufficient (and necessary) for byte-identical de-duplication.
//!
//! [Parsing Canonical Form]: https://avro.apache.org/docs/current/specification/#parsing-canonical-form-for-schemas

use std::marker::PhantomData;

use apache_avro::Schema as AvroBaseSchema;
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::{Schema, SchemaError};
use crate::pb;

/// Avro schema that serialises `T` via `apache_avro::to_avro_datum` against a writer schema
/// supplied at construction time.
///
/// `schema_data()` returns the [parsing canonical form](apache_avro::Schema::canonical_form) of
/// the writer schema — the form the broker uses for de-duplication.
pub struct AvroSchema<T> {
    writer: AvroBaseSchema,
    canonical_bytes: Bytes,
    _marker: PhantomData<fn() -> T>,
}

impl<T> AvroSchema<T> {
    /// Construct an [`AvroSchema`] from a JSON Avro schema document.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::Encoding`] if the input is not a valid Avro schema (e.g. malformed
    /// JSON, unknown type tag).
    pub fn parse_str(json: &str) -> Result<Self, SchemaError> {
        let writer = AvroBaseSchema::parse_str(json)
            .map_err(|err| SchemaError::Encoding(format!("avro schema parse: {err}")))?;
        let canonical_bytes = Bytes::from(writer.canonical_form().into_bytes());
        Ok(Self {
            writer,
            canonical_bytes,
            _marker: PhantomData,
        })
    }

    /// Return a reference to the underlying parsed Avro schema. Mostly useful in tests.
    pub fn writer_schema(&self) -> &AvroBaseSchema {
        &self.writer
    }
}

impl<T> std::fmt::Debug for AvroSchema<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroSchema")
            .field("type", &std::any::type_name::<T>())
            .field("writer", &"<apache_avro::Schema>")
            .field("canonical_bytes_len", &self.canonical_bytes.len())
            .finish_non_exhaustive()
    }
}

impl<T> Schema for AvroSchema<T>
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    type Owned = T;

    fn schema_type(&self) -> pb::schema::Type {
        pb::schema::Type::Avro
    }

    fn schema_data(&self) -> Bytes {
        self.canonical_bytes.clone()
    }

    fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError> {
        let avro_value = apache_avro::to_value(value)
            .map_err(|err| SchemaError::Encoding(format!("avro to_value: {err}")))?;
        let resolved = avro_value
            .resolve(&self.writer)
            .map_err(|err| SchemaError::Encoding(format!("avro resolve: {err}")))?;
        let bytes = apache_avro::to_avro_datum(&self.writer, resolved)
            .map_err(|err| SchemaError::Encoding(format!("avro to_avro_datum: {err}")))?;
        Ok(Bytes::from(bytes))
    }

    fn decode(&self, bytes: &[u8]) -> Result<Self::Owned, SchemaError> {
        let mut cursor = std::io::Cursor::new(bytes);
        let value = apache_avro::from_avro_datum(&self.writer, &mut cursor, None)
            .map_err(|err| SchemaError::Decoding(format!("avro from_avro_datum: {err}")))?;
        apache_avro::from_value::<T>(&value)
            .map_err(|err| SchemaError::Decoding(format!("avro from_value: {err}")))
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    const PERSON_SCHEMA: &str = r#"{
        "type": "record",
        "name": "Person",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"}
        ]
    }"#;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Person {
        name: String,
        age: i32,
    }

    #[test]
    fn roundtrip() {
        let schema = AvroSchema::<Person>::parse_str(PERSON_SCHEMA).unwrap();
        let value = Person {
            name: "Ada Lovelace".to_owned(),
            age: 36,
        };
        let encoded = schema.encode(&value).unwrap();
        let decoded = schema.decode(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn schema_data_is_canonical_form() {
        let schema = AvroSchema::<Person>::parse_str(PERSON_SCHEMA).unwrap();
        let raw = schema.schema_data();
        let canonical = std::str::from_utf8(&raw).unwrap();
        // Canonical form strips whitespace and reorders keys; assert the rendered output matches
        // exactly what `apache_avro::Schema::canonical_form()` would produce for the same schema.
        let expected = schema.writer_schema().canonical_form();
        assert_eq!(canonical, expected);
        // Sanity-check: canonical form is compact JSON, not pretty-printed.
        assert!(!canonical.contains('\n'));
        assert!(canonical.contains("\"Person\""));
    }

    #[test]
    fn invalid_schema_rejected() {
        let err = AvroSchema::<Person>::parse_str("{ not avro }").unwrap_err();
        assert!(matches!(err, SchemaError::Encoding(_)));
    }

    #[test]
    fn schema_type_is_avro() {
        let schema = AvroSchema::<Person>::parse_str(PERSON_SCHEMA).unwrap();
        assert_eq!(schema.schema_type(), pb::schema::Type::Avro);
    }
}
