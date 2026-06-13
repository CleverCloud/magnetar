// SPDX-License-Identifier: Apache-2.0

//! JSON schema — `serde_json` round-trip with an Avro record definition in `schema_data`.
//!
//! Mirrors `org.apache.pulsar.client.impl.schema.JSONSchema`. Pulsar 4.x stores JSON schemas as
//! Avro schema definitions while the payload itself is still encoded as JSON.
//!
//! Magnetar derives the Avro record at runtime from `T`'s [`schemars::JsonSchema`] impl. The schema
//! document is compact-serialised (no whitespace, stable key ordering) for byte-identical
//! reproducibility across processes — a prerequisite for broker-side schema registry
//! de-duplication.

use std::marker::PhantomData;

use bytes::Bytes;
use schemars::JsonSchema as SchemarsJsonSchema;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::{Schema, SchemaError};
use crate::pb;

/// Schema that encodes `T` as JSON using `serde_json` and advertises an Avro record definition
/// (derived from `T`'s [`schemars::JsonSchema`] impl) as `schema_data`.
///
/// The schema document is precomputed in [`JsonSchema::new`] to keep [`Schema::schema_data`]
/// hot-path-free; cloning the returned [`Bytes`] is O(1) refcount.
pub struct JsonSchema<T> {
    schema_bytes: Bytes,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Default for JsonSchema<T>
where
    T: SchemarsJsonSchema,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> JsonSchema<T>
where
    T: SchemarsJsonSchema,
{
    /// Construct a new [`JsonSchema`], precomputing the Avro record definition for `T`.
    ///
    /// Apache Pulsar 4.x validates JSON schema definitions with the same Avro parser it uses for
    /// `AVRO` schemas, so `schema_data` must be an Avro schema definition rather than a
    /// JSON-Schema document.
    ///
    /// # Panics
    ///
    /// Never panics in practice: `serde_json::to_value` on a `schemars::Schema` only fails on
    /// non-string map keys or non-finite floats, neither of which `schemars` ever emits. If
    /// serialisation does fail we fall back to a valid empty Avro record so the schema remains
    /// usable for producer/consumer wiring.
    #[must_use]
    pub fn new() -> Self {
        let schema = schemars::schema_for!(T);
        let mut value = serde_json::to_value(&schema).unwrap_or(serde_json::Value::Null);
        value = avro_record_from_json_schema::<T>(&value);
        let schema_bytes = serde_json::to_vec(&value)
            .map(Bytes::from)
            .unwrap_or_else(|_| {
                Bytes::from_static(br#"{"type":"record","name":"Record","fields":[]}"#)
            });
        Self {
            schema_bytes,
            _marker: PhantomData,
        }
    }
}

fn avro_record_from_json_schema<T>(schema: &serde_json::Value) -> serde_json::Value {
    let name = avro_name(
        std::any::type_name::<T>()
            .rsplit("::")
            .next()
            .unwrap_or("Record"),
    );
    avro_record_value(&name, schema)
}

fn avro_record_value(name: &str, schema: &serde_json::Value) -> serde_json::Value {
    let required = schema
        .get("required")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .collect::<std::collections::BTreeSet<_>>()
        })
        .unwrap_or_default();

    let fields = schema
        .get("properties")
        .and_then(serde_json::Value::as_object)
        .map(|properties| {
            properties
                .iter()
                .map(|(field_name, field_schema)| {
                    let mut avro_type = avro_type_from_json_schema(field_name, field_schema);
                    let mut field = serde_json::json!({
                        "name": field_name,
                        "type": avro_type,
                    });
                    if !required.contains(field_name.as_str()) {
                        avro_type = serde_json::json!(["null", field["type"].clone()]);
                        field["type"] = avro_type;
                        field["default"] = serde_json::Value::Null;
                    }
                    field
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    serde_json::json!({
        "type": "record",
        "name": name,
        "fields": fields,
    })
}

fn avro_type_from_json_schema(name: &str, schema: &serde_json::Value) -> serde_json::Value {
    if let Some(any_of) = schema
        .get("anyOf")
        .or_else(|| schema.get("oneOf"))
        .and_then(serde_json::Value::as_array)
    {
        let variants = any_of
            .iter()
            .map(|item| avro_type_from_json_schema(name, item))
            .collect::<Vec<_>>();
        return serde_json::Value::Array(variants);
    }

    match schema.get("type").and_then(serde_json::Value::as_str) {
        Some("boolean") => serde_json::json!("boolean"),
        Some("integer") => match schema.get("format").and_then(serde_json::Value::as_str) {
            Some("int64" | "uint64") => serde_json::json!("long"),
            _ => serde_json::json!("int"),
        },
        Some("number") => serde_json::json!("double"),
        Some("string") => serde_json::json!("string"),
        Some("array") => serde_json::json!({
            "type": "array",
            "items": avro_type_from_json_schema("Item", schema.get("items").unwrap_or(&serde_json::Value::Null)),
        }),
        Some("object") => avro_record_value(&avro_name(name), schema),
        Some("null") => serde_json::json!("null"),
        _ => serde_json::json!("string"),
    }
}

fn avro_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len().max(1));
    for (idx, ch) in name.chars().enumerate() {
        let valid = ch == '_' || ch.is_ascii_alphanumeric();
        if idx == 0 {
            if ch == '_' || ch.is_ascii_alphabetic() {
                out.push(ch);
            } else if valid {
                out.push('_');
                out.push(ch);
            } else {
                out.push('_');
            }
        } else if valid {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "Record".to_owned()
    } else {
        out
    }
}

impl<T> std::fmt::Debug for JsonSchema<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonSchema")
            .field("type", &std::any::type_name::<T>())
            .field("schema_bytes_len", &self.schema_bytes.len())
            .finish_non_exhaustive()
    }
}

impl<T> Schema for JsonSchema<T>
where
    T: Serialize + DeserializeOwned + SchemarsJsonSchema + Send + 'static,
{
    type Owned = T;

    fn schema_type(&self) -> pb::schema::Type {
        pb::schema::Type::Json
    }

    fn schema_data(&self) -> Bytes {
        self.schema_bytes.clone()
    }

    fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError> {
        serde_json::to_vec(value)
            .map(Bytes::from)
            .map_err(|err| SchemaError::Encoding(err.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<Self::Owned, SchemaError> {
        serde_json::from_slice::<T>(bytes).map_err(|err| SchemaError::Decoding(err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use schemars::JsonSchema as SchemarsJsonSchema;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemarsJsonSchema)]
    struct Person {
        name: String,
        age: u32,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemarsJsonSchema)]
    struct Foo {
        name: String,
        count: i32,
    }

    #[test]
    fn roundtrip() {
        let schema = JsonSchema::<Person>::new();
        let value = Person {
            name: "Ada Lovelace".to_owned(),
            age: 36,
        };
        let encoded = schema.encode(&value).unwrap();
        let decoded = schema.decode(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn schema_data_is_an_avro_record_definition() {
        let schema = JsonSchema::<Foo>::new();
        let raw = schema.schema_data();
        let value: serde_json::Value =
            serde_json::from_slice(&raw).expect("schema_data must be valid JSON");

        assert_eq!(
            value.get("type").and_then(serde_json::Value::as_str),
            Some("record")
        );
        assert_eq!(
            value.get("name").and_then(serde_json::Value::as_str),
            Some("Foo")
        );
        let fields = value
            .get("fields")
            .and_then(serde_json::Value::as_array)
            .expect("schema document must carry Avro record fields");
        assert_eq!(
            fields.len(),
            2,
            "expected name/count fields; full schema: {value}"
        );
        assert_eq!(fields[0]["name"], "count");
        assert_eq!(fields[0]["type"], "int");
        assert_eq!(fields[1]["name"], "name");
        assert_eq!(fields[1]["type"], "string");
        apache_avro::Schema::parse_str(std::str::from_utf8(&raw).unwrap())
            .expect("broker-facing JSON schema_data must parse as Avro");

        assert_eq!(schema.schema_type(), pb::schema::Type::Json);
    }

    #[test]
    fn schema_data_is_stable_across_constructions() {
        // Byte-identical reproducibility is a precondition for broker-side de-duplication;
        // re-creating a schema for the same `T` must yield the exact same bytes.
        let a = JsonSchema::<Person>::new().schema_data();
        let b = JsonSchema::<Person>::new().schema_data();
        assert_eq!(a, b);
    }

    #[test]
    fn invalid_json_rejected() {
        let schema = JsonSchema::<Person>::new();
        let err = schema.decode(b"{not json}").unwrap_err();
        assert!(matches!(err, SchemaError::Decoding(_)));
    }

    #[test]
    fn unknown_field_in_payload_is_accepted() {
        // serde_json accepts unknown fields by default — this documents the
        // status quo so a future migration to `deny_unknown_fields` is an
        // explicit change, not a silent one.
        let schema = JsonSchema::<Person>::new();
        let payload = br#"{"name":"Bob","age":42,"extra":"ignored"}"#;
        let decoded = schema.decode(payload).unwrap();
        assert_eq!(decoded.name, "Bob");
        assert_eq!(decoded.age, 42);
    }

    #[test]
    fn missing_required_field_rejected() {
        let schema = JsonSchema::<Person>::new();
        let payload = br#"{"name":"Bob"}"#; // missing `age`
        let err = schema.decode(payload).unwrap_err();
        assert!(matches!(err, SchemaError::Decoding(_)));
    }

    #[test]
    fn wrong_type_for_field_rejected() {
        let schema = JsonSchema::<Person>::new();
        let payload = br#"{"name":"Bob","age":"forty-two"}"#;
        let err = schema.decode(payload).unwrap_err();
        assert!(matches!(err, SchemaError::Decoding(_)));
    }

    #[test]
    fn empty_payload_rejected() {
        let schema = JsonSchema::<Person>::new();
        let err = schema.decode(b"").unwrap_err();
        assert!(matches!(err, SchemaError::Decoding(_)));
    }

    #[test]
    fn unicode_in_field_value_round_trips() {
        let schema = JsonSchema::<Person>::new();
        let value = Person {
            name: "Élise · μ".to_owned(),
            age: 30,
        };
        let encoded = schema.encode(&value).unwrap();
        let decoded = schema.decode(&encoded).unwrap();
        assert_eq!(decoded, value);
    }
}
