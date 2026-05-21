// SPDX-License-Identifier: Apache-2.0

//! Identity bytes schema — no encoding, no decoding, no schema metadata.
//!
//! Mirrors `org.apache.pulsar.client.impl.schema.BytesSchema`. The broker treats messages
//! produced with this schema as opaque blobs; `schema_data()` is empty and the advertised type is
//! `pb::schema::Type::None`.

use bytes::Bytes;

use super::{Schema, SchemaError};
use crate::pb;

/// Pass-through bytes schema. Encode / decode are the identity function.
#[derive(Debug, Clone, Copy, Default)]
pub struct BytesSchema;

impl BytesSchema {
    /// Construct a new [`BytesSchema`].
    pub const fn new() -> Self {
        Self
    }
}

impl Schema for BytesSchema {
    type Owned = Bytes;

    fn schema_type(&self) -> pb::schema::Type {
        pb::schema::Type::None
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let schema = BytesSchema::new();
        let payload = Bytes::from_static(b"hello world");
        let encoded = schema.encode(&payload).unwrap();
        assert_eq!(encoded, payload);
        let decoded = schema.decode(&encoded).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn schema_data_is_empty() {
        let schema = BytesSchema::new();
        assert!(schema.schema_data().is_empty());
        assert_eq!(schema.schema_type(), pb::schema::Type::None);
    }

    #[test]
    fn empty_input() {
        let schema = BytesSchema::new();
        let decoded = schema.decode(&[]).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn encode_clones_bytes_handle() {
        let schema = BytesSchema::new();
        let payload = Bytes::from_static(b"shared-buf");
        let encoded = schema.encode(&payload).unwrap();
        assert_eq!(encoded, payload);
        assert_eq!(encoded.len(), payload.len());
    }

    #[test]
    fn decode_returns_owned_bytes() {
        let schema = BytesSchema::new();
        let buf = b"transient".to_vec();
        let decoded = schema.decode(&buf).unwrap();
        drop(buf);
        assert_eq!(decoded.as_ref(), b"transient");
    }

    #[test]
    fn binary_payload_round_trips() {
        let schema = BytesSchema::new();
        let payload = Bytes::from(vec![0x00, 0xFF, 0x7F, 0x80, 0xAA, 0x55]);
        let encoded = schema.encode(&payload).unwrap();
        let decoded = schema.decode(&encoded).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn very_large_payload_round_trips() {
        let schema = BytesSchema::new();
        let payload = Bytes::from(vec![0x42u8; 1_000_000]);
        let encoded = schema.encode(&payload).unwrap();
        assert_eq!(encoded.len(), 1_000_000);
        let decoded = schema.decode(&encoded).unwrap();
        assert_eq!(decoded.len(), 1_000_000);
        assert!(decoded.iter().all(|&b| b == 0x42));
    }
}
