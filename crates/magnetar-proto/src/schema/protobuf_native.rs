// SPDX-License-Identifier: Apache-2.0

//! Protobuf native schema — raw FileDescriptorSet, raw payload bytes.
//!
//! Mirrors `org.apache.pulsar.client.impl.schema.ProtobufNativeSchema`. Per Codex Q4 the broker
//! treats this schema family with **raw-byte equality** on `schema_data` (`getData()`), unlike
//! [`AvroSchema`] which is canonicalised. The Java client emits a `FileDescriptorSet` serialised
//! via `FileDescriptorSet.toByteArray()` — magnetar must accept the exact same bytes and pass
//! them through unchanged or the broker will create a fresh schema version on every reconnect.
//!
//! Encoding and decoding are the identity function: messages are already serialised
//! Protobuf bytes coming from `prost::Message::encode_to_vec()` or any other protobuf
//! implementation. This crate does not interpret them.

use bytes::Bytes;

use super::{Schema, SchemaError};
use crate::pb;

/// Protobuf-native schema carrying a `FileDescriptorSet` blob.
///
/// `schema_data()` returns the bytes supplied at construction verbatim. Pulsar's broker compares
/// these bytes with `Arrays.equals(byte[], byte[])` (`SchemaRegistryServiceImpl.java:429-438`),
/// so any framing, varint, or field-ordering drift from the Java side will defeat dedup.
#[derive(Debug, Clone)]
pub struct ProtobufNativeSchema {
    file_descriptor_set: Bytes,
}

impl ProtobufNativeSchema {
    /// Construct a [`ProtobufNativeSchema`] from a serialised `FileDescriptorSet`.
    ///
    /// The caller is responsible for emitting byte-identical output to the Java client — see the
    /// [module docs](self) and `ARCHITECTURE.md` (Schema-registry parity).
    pub fn new(file_descriptor_set: impl Into<Bytes>) -> Self {
        Self {
            file_descriptor_set: file_descriptor_set.into(),
        }
    }

    /// Borrow the raw `FileDescriptorSet` bytes.
    pub fn file_descriptor_set(&self) -> &Bytes {
        &self.file_descriptor_set
    }
}

impl Schema for ProtobufNativeSchema {
    type Owned = Bytes;

    fn schema_type(&self) -> pb::schema::Type {
        pb::schema::Type::ProtobufNative
    }

    fn schema_data(&self) -> Bytes {
        self.file_descriptor_set.clone()
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

    /// Build a tiny but realistic-looking `FileDescriptorSet`. We don't depend on
    /// `prost-types`'s `FileDescriptorSet` definition here — for this test we only care that the
    /// bytes round-trip verbatim, which is the actual contract.
    fn fake_file_descriptor_set() -> Vec<u8> {
        // Hand-crafted: protobuf wire bytes for a single FileDescriptorProto with name
        // "person.proto" (field 1, string, wire type 2).
        //   tag = (1 << 3) | 2 = 0x0a
        //   len = 0x0e
        //   ...
        //   FileDescriptorSet.file = field 1, length-delimited:
        //     tag = 0x0a
        //     len = inner length
        let inner = {
            let name = b"person.proto";
            let mut buf = Vec::new();
            buf.push(0x0a); // field 1 (name), wire type 2 (length-delimited)
            buf.push(name.len() as u8);
            buf.extend_from_slice(name);
            buf
        };
        let mut outer = Vec::new();
        outer.push(0x0a); // field 1 (file), wire type 2
        outer.push(inner.len() as u8);
        outer.extend_from_slice(&inner);
        outer
    }

    #[test]
    fn schema_data_pass_through() {
        let fds = fake_file_descriptor_set();
        let schema = ProtobufNativeSchema::new(fds.clone());
        assert_eq!(schema.schema_data().as_ref(), fds.as_slice());
        assert_eq!(schema.schema_type(), pb::schema::Type::ProtobufNative);
    }

    #[test]
    fn payload_roundtrip() {
        let schema = ProtobufNativeSchema::new(fake_file_descriptor_set());
        let payload = Bytes::from_static(b"\x08\x96\x01"); // tag 1 varint 150 — minimal protobuf
        let encoded = schema.encode(&payload).unwrap();
        assert_eq!(encoded, payload);
        let decoded = schema.decode(&encoded).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn empty_fds_allowed() {
        let schema = ProtobufNativeSchema::new(Bytes::new());
        assert!(schema.schema_data().is_empty());
    }
}
