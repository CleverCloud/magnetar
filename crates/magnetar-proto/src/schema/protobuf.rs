// SPDX-License-Identifier: Apache-2.0

//! Protobuf schema — `prost::Message` round-trip + canonical descriptor wire form.
//!
//! Mirrors `org.apache.pulsar.client.impl.schema.ProtobufSchema`. The Pulsar broker
//! (`SchemaRegistryServiceImpl.java:405-438`) stores PROTOBUF `schema_data` as an opaque
//! blob keyed by raw-byte equality; identical descriptor bytes on every (re)connect
//! collapse to a single registry version. The Java client emits an Avro schema derived
//! from the protobuf type via `org.apache.avro.protobuf.ProtobufData.getSchema(pojo)`;
//! magnetar instead emits a serialised
//! [`prost_types::FileDescriptorProto`] (the upstream
//! `google.protobuf.FileDescriptorProto`) as the version-stable identifier. Both forms
//! satisfy the broker's byte-equality contract, but they are **not** byte-identical to
//! each other — see "Byte parity with the Java client" below.
//!
//! # Construction
//!
//! Three constructors, in order of byte-parity strictness:
//!
//! 1. [`ProtobufSchema::with_file_descriptor_proto`] — pass a pre-serialised `FileDescriptorProto`
//!    (e.g. from `protoc --include_imports --descriptor_set_out`, then extracting the relevant
//!    file). Bytes are stored verbatim and emitted as `schema_data`. This is the recommended path
//!    for cross-client interop: the same descriptor on every producer reconnect collapses to a
//!    single broker registry version.
//! 2. [`ProtobufSchema::with_file_descriptor`] — pass a [`prost_types::FileDescriptorProto`] value;
//!    the schema serialises it deterministically via [`prost::Message::encode`]. Convenience
//!    wrapper around (1) for callers that already have the typed descriptor in hand.
//! 3. [`ProtobufSchema::new`] — pass only the fully-qualified message name (legacy behaviour). The
//!    resulting `schema_data` is the UTF-8 bytes of the name; the broker will still dedupe
//!    by-string, but **this form is not parity-compatible with the Java client**. Retained for
//!    backward compatibility and for tests that don't need descriptor-level round-tripping.
//!
//! # Byte parity with the Java client
//!
//! The Apache Pulsar Java client emits an Avro-schema-derived-from-protobuf JSON
//! document for `SchemaType.PROTOBUF`, **not** a serialised `FileDescriptorProto`.
//! Magnetar's descriptor-based form is a stable, version-independent identifier with
//! the same broker-side dedup semantics, but a topic produced-to by both Java and
//! magnetar will register two distinct schema versions (one per encoding). Full
//! Java-to-magnetar byte parity (i.e. emitting the Avro-from-protobuf JSON) requires
//! either an Avro descriptor bridge (no Rust equivalent of
//! `org.apache.avro.protobuf.ProtobufData` exists today) or a vendored re-implementation.
//! That bridge is tracked separately; byte parity against a real broker should be
//! validated via an e2e gate using `apachepulsar/pulsar:4.0.4` testcontainers.

use std::marker::PhantomData;

use bytes::Bytes;
use prost::Message;
use prost_types::FileDescriptorProto;

use super::{Schema, SchemaError};
use crate::pb;

/// Protobuf schema parametrised by a `prost::Message` type.
///
/// `schema_data()` returns either the serialised `FileDescriptorProto`
/// (descriptor-mode, via [`ProtobufSchema::with_file_descriptor_proto`] or
/// [`ProtobufSchema::with_file_descriptor`]) or the UTF-8-encoded fully-qualified
/// message name (legacy-mode, via [`ProtobufSchema::new`]). See the
/// [module docs](crate::schema::protobuf) for the parity caveat.
pub struct ProtobufSchema<T> {
    schema_data: Bytes,
    _marker: PhantomData<fn() -> T>,
}

impl<T> ProtobufSchema<T>
where
    T: Message + Default,
{
    /// Build a [`ProtobufSchema`] using the **fully-qualified message name** as
    /// `schema_data`.
    ///
    /// `fully_qualified_name` should match the `.proto` source — e.g.
    /// `"pulsar.proto.MessageMetadata"`. The broker compares this string byte-for-byte
    /// when resolving schema versions; identical inputs from any producer collapse to a
    /// single registry version.
    ///
    /// # Java-parity caveat
    ///
    /// The Java client emits an Avro-schema-derived-from-protobuf JSON document, not
    /// the fully-qualified name. Use [`ProtobufSchema::with_file_descriptor_proto`]
    /// or [`ProtobufSchema::with_file_descriptor`] for a more descriptive,
    /// descriptor-shaped identifier. See the [module docs](crate::schema::protobuf).
    pub fn new(fully_qualified_name: impl Into<String>) -> Self {
        Self {
            schema_data: Bytes::from(fully_qualified_name.into().into_bytes()),
            _marker: PhantomData,
        }
    }

    /// Build a [`ProtobufSchema`] from pre-serialised `FileDescriptorProto` bytes.
    ///
    /// The bytes are stored verbatim and emitted as `schema_data`. Typical sources:
    ///
    /// * The output of `protoc --include_imports --descriptor_set_out=...` followed by extracting
    ///   the relevant `FileDescriptorProto` from the `FileDescriptorSet`.
    /// * The encoded form of a [`prost_types::FileDescriptorProto`] built by hand (see
    ///   [`ProtobufSchema::with_file_descriptor`] for the convenience wrapper).
    ///
    /// This is the broker-canonical descriptor form: identical inputs across every
    /// producer reconnect collapse to a single registry version.
    pub fn with_file_descriptor_proto(file_descriptor_proto: impl Into<Bytes>) -> Self {
        Self {
            schema_data: file_descriptor_proto.into(),
            _marker: PhantomData,
        }
    }

    /// Build a [`ProtobufSchema`] from a [`prost_types::FileDescriptorProto`] value.
    ///
    /// The descriptor is serialised via [`prost::Message::encode`] and the resulting
    /// bytes become `schema_data`. Convenience wrapper around
    /// [`ProtobufSchema::with_file_descriptor_proto`] for callers that already have
    /// the typed descriptor in hand (e.g. assembled programmatically).
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::Encoding`] if `prost` fails to serialise the
    /// descriptor (in practice this only happens on allocator failure — the
    /// descriptor protobuf has no required fields that can be unset).
    pub fn with_file_descriptor(
        file_descriptor: &FileDescriptorProto,
    ) -> Result<Self, SchemaError> {
        let mut buf = Vec::with_capacity(file_descriptor.encoded_len());
        file_descriptor
            .encode(&mut buf)
            .map_err(|err| SchemaError::Encoding(format!("FileDescriptorProto encode: {err}")))?;
        Ok(Self::with_file_descriptor_proto(buf))
    }
}

impl<T> std::fmt::Debug for ProtobufSchema<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtobufSchema")
            .field("type", &std::any::type_name::<T>())
            .field("schema_data_len", &self.schema_data.len())
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
        self.schema_data.clone()
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
    use prost_types::{DescriptorProto, FieldDescriptorProto, field_descriptor_proto};

    use super::*;
    use crate::pb::KeyValue;

    /// Build a minimal but well-formed [`FileDescriptorProto`] describing a single
    /// message type `pulsar.proto.KeyValue { string key = 1; string value = 2; }`.
    /// The fixture is hand-rolled so the tests don't depend on `prost-build` or on
    /// fishing the descriptor out of the workspace's generated `pb` module.
    fn key_value_descriptor() -> FileDescriptorProto {
        FileDescriptorProto {
            name: Some("pulsar.proto".to_owned()),
            package: Some("pulsar.proto".to_owned()),
            syntax: Some("proto3".to_owned()),
            message_type: vec![DescriptorProto {
                name: Some("KeyValue".to_owned()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("key".to_owned()),
                        number: Some(1),
                        r#type: Some(field_descriptor_proto::Type::String as i32),
                        ..FieldDescriptorProto::default()
                    },
                    FieldDescriptorProto {
                        name: Some("value".to_owned()),
                        number: Some(2),
                        r#type: Some(field_descriptor_proto::Type::String as i32),
                        ..FieldDescriptorProto::default()
                    },
                ],
                ..DescriptorProto::default()
            }],
            ..FileDescriptorProto::default()
        }
    }

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
    fn schema_data_carries_name_legacy() {
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

    #[test]
    fn with_file_descriptor_proto_roundtrip() {
        // Build a descriptor, serialise it out-of-band, hand it to the schema, then
        // re-decode `schema_data()` and assert the message name survives the
        // round-trip — proving that the broker sees the exact bytes we built.
        let descriptor = key_value_descriptor();
        let mut buf = Vec::with_capacity(descriptor.encoded_len());
        descriptor.encode(&mut buf).expect("descriptor encode");

        let schema =
            ProtobufSchema::<KeyValue>::with_file_descriptor_proto(Bytes::from(buf.clone()));
        let data = schema.schema_data();
        assert_eq!(data.as_ref(), buf.as_slice(), "pass-through must be exact");

        let parsed = FileDescriptorProto::decode(data.as_ref()).expect("descriptor decode");
        assert_eq!(parsed.message_type.len(), 1);
        assert_eq!(parsed.message_type[0].name.as_deref(), Some("KeyValue"));
        assert_eq!(parsed.package.as_deref(), Some("pulsar.proto"));
        assert_eq!(schema.schema_type(), pb::schema::Type::Protobuf);
    }

    #[test]
    fn with_file_descriptor_roundtrip() {
        // The typed wrapper should serialise the descriptor and produce the same
        // bytes as `with_file_descriptor_proto(prost-encoded descriptor)`.
        let descriptor = key_value_descriptor();
        let schema =
            ProtobufSchema::<KeyValue>::with_file_descriptor(&descriptor).expect("encode ok");
        let data = schema.schema_data();

        let parsed = FileDescriptorProto::decode(data.as_ref()).expect("descriptor decode");
        assert_eq!(parsed, descriptor, "round-trip must preserve every field");
        assert_eq!(schema.schema_type(), pb::schema::Type::Protobuf);

        // Payload encode/decode still flows through the prost message — descriptor
        // mode does not change the wire shape, only `schema_data`.
        let value = KeyValue {
            key: "k".to_owned(),
            value: "v".to_owned(),
        };
        let encoded = schema.encode(&value).expect("encode payload");
        let decoded = schema.decode(&encoded).expect("decode payload");
        assert_eq!(decoded, value);
    }

    #[test]
    fn with_file_descriptor_is_deterministic() {
        // Two builds from the same descriptor must produce byte-identical
        // `schema_data` — the broker's dedup hinges on this.
        let descriptor = key_value_descriptor();
        let a = ProtobufSchema::<KeyValue>::with_file_descriptor(&descriptor).expect("a");
        let b = ProtobufSchema::<KeyValue>::with_file_descriptor(&descriptor).expect("b");
        assert_eq!(a.schema_data(), b.schema_data());
    }
}
