// SPDX-License-Identifier: Apache-2.0

//! JSON schema — `serde_json` round-trip with type-erased schema data.
//!
//! Mirrors `org.apache.pulsar.client.impl.schema.JSONSchema`. The Java client carries a full
//! JSON-Schema document in `schema_data` so the broker can validate the payload structure.
//! Magnetar currently advertises an empty JSON document (`{}`); full JSON-Schema generation is a
//! future enhancement gated on adding the [`schemars`] crate to the workspace allow-list (not
//! done now).
//!
//! [`schemars`]: https://docs.rs/schemars

use std::marker::PhantomData;

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::{Schema, SchemaError};
use crate::pb;

/// Schema that encodes `T` as JSON using `serde_json`.
///
/// `schema_data` returns the literal `{}` placeholder. See the [module docs](self) for the
/// rationale.
pub struct JsonSchema<T> {
    _marker: PhantomData<fn() -> T>,
}

impl<T> Default for JsonSchema<T> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> JsonSchema<T> {
    /// Construct a new [`JsonSchema`].
    pub const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for JsonSchema<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonSchema")
            .field("type", &std::any::type_name::<T>())
            .finish()
    }
}

impl<T> Schema for JsonSchema<T>
where
    T: Serialize + DeserializeOwned + Send + 'static,
{
    type Owned = T;

    fn schema_type(&self) -> pb::schema::Type {
        pb::schema::Type::Json
    }

    fn schema_data(&self) -> Bytes {
        // TODO(M5+): generate a full JSON-Schema document from `T` via the `schemars` crate.
        //  This requires adding `schemars` to the workspace allow-list — propose to Florentin
        //  with crate name + license + maintenance signal before pulling it in. See
        //  `crates/magnetar-proto/src/schema/mod.rs` Codex Q4 note for the broker-side handling.
        Bytes::from_static(b"{}")
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
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Person {
        name: String,
        age: u32,
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
    fn schema_data_is_empty_object() {
        let schema = JsonSchema::<Person>::new();
        assert_eq!(schema.schema_data().as_ref(), b"{}");
        assert_eq!(schema.schema_type(), pb::schema::Type::Json);
    }

    #[test]
    fn invalid_json_rejected() {
        let schema = JsonSchema::<Person>::new();
        let err = schema.decode(b"{not json}").unwrap_err();
        assert!(matches!(err, SchemaError::Decoding(_)));
    }
}
