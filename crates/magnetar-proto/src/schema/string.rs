// SPDX-License-Identifier: Apache-2.0

//! UTF-8 string schema.
//!
//! Mirrors `org.apache.pulsar.client.impl.schema.StringSchema`. The broker advertises this as
//! `pb::schema::Type::String` with an empty `schema_data` — Pulsar carries the character set
//! either via a `properties` entry on the broker-side schema record (out of scope here) or
//! defaults to UTF-8.

use bytes::Bytes;

use super::{Schema, SchemaError};
use crate::pb;

/// UTF-8 string schema. Encoding is `String::into_bytes`; decoding validates UTF-8 and rejects
/// invalid sequences with [`SchemaError::Mismatch`].
#[derive(Debug, Clone, Copy, Default)]
pub struct StringSchema;

impl StringSchema {
    /// Construct a new [`StringSchema`].
    pub const fn new() -> Self {
        Self
    }
}

impl Schema for StringSchema {
    type Owned = String;

    fn schema_type(&self) -> pb::schema::Type {
        pb::schema::Type::String
    }

    fn schema_data(&self) -> Bytes {
        Bytes::new()
    }

    fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError> {
        Ok(Bytes::copy_from_slice(value.as_bytes()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<Self::Owned, SchemaError> {
        std::str::from_utf8(bytes)
            .map(ToOwned::to_owned)
            .map_err(|err| SchemaError::Mismatch {
                expected: "valid UTF-8 string".to_owned(),
                actual: format!("invalid UTF-8 at byte {}", err.valid_up_to()),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let schema = StringSchema::new();
        let value = "hello, world".to_owned();
        let encoded = schema.encode(&value).unwrap();
        assert_eq!(encoded.as_ref(), value.as_bytes());
        let decoded = schema.decode(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn roundtrip_unicode() {
        let schema = StringSchema::new();
        let value = "héllo, wörld — π ≈ 3.14159".to_owned();
        let encoded = schema.encode(&value).unwrap();
        let decoded = schema.decode(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn invalid_utf8_rejected() {
        let schema = StringSchema::new();
        let bad = [0xFFu8, 0xFE, 0xFD];
        let err = schema.decode(&bad).unwrap_err();
        assert!(matches!(err, SchemaError::Mismatch { .. }));
    }

    #[test]
    fn schema_data_is_empty() {
        let schema = StringSchema::new();
        assert!(schema.schema_data().is_empty());
        assert_eq!(schema.schema_type(), pb::schema::Type::String);
    }
}
