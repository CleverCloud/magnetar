// SPDX-License-Identifier: Apache-2.0

//! Protobuf schema — `prost::Message` round-trip.
//!
//! Mirrors `org.apache.pulsar.client.impl.schema.ProtobufSchema`. Per Codex Q4 the broker
//! canonicalises `pb::schema::Type::Protobuf` schemas by re-parsing them through Avro's
//! `Schema.Parser` (Pulsar wraps the Protobuf descriptor in an Avro record envelope). For the
//! moment magnetar advertises the **fully-qualified message type name** as schema data — a stable,
//! version-independent identifier that lets a topic carry a single producer's protobuf message.
//!
//! Future work tracked here: emit a Protobuf `FileDescriptorProto` extracted from `prost-reflect`
//! (M5 stretch). The current placeholder is sufficient for end-to-end produce / consume; the
//! schema-registry version dedupes by the constant string and will create one version per (topic,
//! message-name) pair.

use std::marker::PhantomData;

use bytes::Bytes;
use prost::Message;

use super::{Schema, SchemaError};
use crate::pb;

/// Protobuf schema parametrised by a `prost::Message` type.
///
/// `schema_data()` returns the package-qualified message name (e.g. `"com.example.MyMessage"`).
/// See the [module docs](crate::schema) for the broader canonicalisation contract.
pub struct ProtobufSchema<T> {
    fully_qualified_name: Bytes,
    _marker: PhantomData<fn() -> T>,
}

impl<T> ProtobufSchema<T>
where
    T: Message + Default,
{
    /// Build a [`ProtobufSchema`] with the supplied package-qualified message name.
    ///
    /// `fully_qualified_name` should match the `.proto` source — e.g.
    /// `"pulsar.proto.MessageMetadata"`. The Java client compares this string byte-for-byte when
    /// resolving schema versions, so it must be stable across producers.
    pub fn new(fully_qualified_name: impl Into<String>) -> Self {
        Self {
            fully_qualified_name: Bytes::from(fully_qualified_name.into().into_bytes()),
            _marker: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for ProtobufSchema<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtobufSchema")
            .field("type", &std::any::type_name::<T>())
            .field(
                "fully_qualified_name",
                &std::str::from_utf8(&self.fully_qualified_name).unwrap_or("<non-utf8>"),
            )
            .finish()
    }
}

impl<T> Schema for ProtobufSchema<T>
where
    T: Message + Default + Send + 'static,
{
    type Owned = T;

    fn schema_type(&self) -> pb::schema::Type {
        pb::schema::Type::Protobuf
    }

    fn schema_data(&self) -> Bytes {
        self.fully_qualified_name.clone()
    }

    fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError> {
        let mut buf = Vec::with_capacity(value.encoded_len());
        value
            .encode(&mut buf)
            .map_err(|err| SchemaError::Encoding(format!("prost encode: {err}")))?;
        Ok(Bytes::from(buf))
    }

    fn decode(&self, bytes: &[u8]) -> Result<Self::Owned, SchemaError> {
        T::decode(bytes).map_err(|err| SchemaError::Decoding(format!("prost decode: {err}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::KeyValue;

    #[test]
    fn roundtrip() {
        // Use a small, stable pb message from the generated module so we don't need to drag in
        // prost-build into the test fixture.
        let schema = ProtobufSchema::<KeyValue>::new("pulsar.proto.KeyValue");
        let value = KeyValue {
            key: "topic".to_owned(),
            value: "magnetar".to_owned(),
        };
        let encoded = schema.encode(&value).unwrap();
        let decoded = schema.decode(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn schema_data_carries_name() {
        let schema = ProtobufSchema::<KeyValue>::new("pulsar.proto.KeyValue");
        assert_eq!(schema.schema_data().as_ref(), b"pulsar.proto.KeyValue");
        assert_eq!(schema.schema_type(), pb::schema::Type::Protobuf);
    }

    #[test]
    fn invalid_bytes_rejected() {
        let schema = ProtobufSchema::<KeyValue>::new("pulsar.proto.KeyValue");
        // Bytes that cannot parse as a `KeyValue` — missing required fields with bogus tags.
        let err = schema.decode(&[0xFF, 0xFF, 0xFF]).unwrap_err();
        assert!(matches!(err, SchemaError::Decoding(_)));
    }
}
