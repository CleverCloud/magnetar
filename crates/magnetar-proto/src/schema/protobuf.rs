// SPDX-License-Identifier: Apache-2.0

//! Protobuf schema — `prost::Message` round-trip + canonical descriptor wire form.
//!
//! Mirrors `org.apache.pulsar.client.impl.schema.ProtobufSchema`. The Pulsar broker
//! (`SchemaRegistryServiceImpl.java:405-438`) stores PROTOBUF `schema_data` as an opaque
//! blob keyed by raw-byte equality; identical descriptor bytes on every (re)connect
//! collapse to a single registry version. The Java client emits an Avro schema derived
//! from the protobuf type via `org.apache.avro.protobuf.ProtobufData.getSchema(pojo)`;
//! magnetar supports two emission strategies:
//!
//! * the descriptor wire form ([`ProtobufSchema::with_file_descriptor`] /
//!   [`ProtobufSchema::with_file_descriptor_proto`]) — a stable, version-independent identifier;
//!   satisfies broker byte-equality but is **not** byte-identical to Java;
//! * the Avro-from-protobuf JSON form ([`ProtobufSchema::with_avro_canonical_from_descriptor`]) —
//!   the same shape Java's `org.apache.avro.protobuf.ProtobufData.getSchema(pojo)` builds and
//!   Pulsar's `ProtobufSchema.createProtobufAvroSchema(...).toString().getBytes()` writes.
//!   Best-effort port of the mapping rules — see "Java byte parity caveats" below.
//!
//! # Construction
//!
//! Four constructors, in order of byte-parity strictness against the Java client:
//!
//! 1. [`ProtobufSchema::with_avro_canonical_from_descriptor`] — walks the descriptor and emits the
//!    **Avro-schema-derived-from-protobuf** JSON document. Aimed at byte parity with Java for the
//!    common protobuf-shape subset; remaining gaps are documented below.
//! 2. [`ProtobufSchema::with_file_descriptor_proto`] — pass a pre-serialised `FileDescriptorProto`
//!    (e.g. from `protoc --include_imports --descriptor_set_out`, then extracting the relevant
//!    file). Bytes are stored verbatim and emitted as `schema_data`. Best path for magnetar-only
//!    deployments wanting descriptor-level round-tripping.
//! 3. [`ProtobufSchema::with_file_descriptor`] — pass a [`prost_types::FileDescriptorProto`] value;
//!    the schema serialises it deterministically via [`prost::Message::encode`]. Convenience
//!    wrapper around (2) for callers that already have the typed descriptor in hand.
//! 4. [`ProtobufSchema::new`] — pass only the fully-qualified message name (legacy behaviour). The
//!    resulting `schema_data` is the UTF-8 bytes of the name; the broker will still dedupe
//!    by-string, but **this form is not parity-compatible with the Java client**. Retained for
//!    backward compatibility and for tests that don't need descriptor-level round-tripping.
//!
//! # Java byte parity caveats
//!
//! [`ProtobufSchema::with_avro_canonical_from_descriptor`] implements the Java mapping
//! rules from `org.apache.avro.protobuf.ProtobufData` (Apache Avro 1.11.x):
//!
//! * protobuf message → Avro record (`type: "record"`).
//! * protobuf scalars → Avro scalars: `BOOL→boolean`, `FLOAT→float`, `DOUBLE→double`,
//!   `STRING→string`, `BYTES→bytes`, `INT32/UINT32/SINT32/FIXED32/SFIXED32 → int`,
//!   `INT64/UINT64/SINT64/FIXED64/SFIXED64 → long`.
//! * protobuf enum → Avro enum (`type: "enum"`, `symbols`).
//! * protobuf nested message → nested Avro record.
//! * `LABEL_REPEATED` → Avro array (`type: "array"`, `items`).
//! * proto2 `LABEL_OPTIONAL` messages → Avro union with `null` (Java's `f.isOptional()`).
//! * Default values per `ProtobufData.getDefault(FieldDescriptor)`: numerics → `0`, bool → `false`,
//!   string/bytes → `""`, repeated → `[]`, enum → first symbol, message → `null`, otherwise the
//!   descriptor's `default_value` string is parsed.
//! * Self-recursive and mutually-recursive message references are emitted as the bare
//!   fully-qualified name once the type has been defined (Avro's "name reference" form).
//!
//! Known limitations versus a real `ProtobufData.getSchema(pojo).toString()`:
//!
//! * **Java-namespace synthesis.** `ProtobufData.getNamespace(...)` uses `java_package` /
//!   `java_outer_classname` / `java_multiple_files` to derive the Avro namespace, plus the
//!   containing-message walk for nested types. Magnetar mirrors the `FileOptions` precedence
//!   (`java_package` else proto `package`) and the nested-type traversal, including the
//!   outer-classname computation that Java does when `java_multiple_files` is unset and
//!   `java_outer_classname` is absent (it CamelCases the basename of the `.proto` file). The one
//!   form magnetar cannot reproduce is the side-effect of `java_multiple_files=false` with a
//!   missing `java_outer_classname` **on a file whose name is non-deterministic at runtime** — that
//!   path is exercised by Java only when a generated POJO class is on the classpath, which is not a
//!   notion that applies in Rust.
//! * **Jackson key-ordering quirks.** Avro Java uses Jackson's `JsonGenerator` and writes record
//!   fields in this order: `type`, `name`, `namespace`, (`doc`), `fields`, (props), (aliases).
//!   Fields inside `fields` emit `name`, `type`, (`doc`), `default`, (`order`), (`aliases`),
//!   (props). Enums emit `type`, `name`, `namespace`, `symbols`. Arrays emit `type`, `items`.
//!   Unions emit as a bare array. Magnetar emits in the same order through a small
//!   insertion-order-preserving JSON AST (`avro_from_proto::Node`), since `serde_json::Map` sorts
//!   keys alphabetically under the default feature set and would diverge from Jackson.
//! * **`Conversion` callbacks.** Java's `getSchema(Descriptor)` first asks the `Conversion`
//!   registry for an override (e.g. `Timestamp` → Avro logical type). Magnetar does not honour
//!   Conversions; protobuf well-known types (`Timestamp`, `Duration`, …) round-trip as plain
//!   records, just like the unregistered case in Java. Topics that mix magnetar + Java with custom
//!   `Conversion`s registered will register distinct schema versions.
//! * **proto3 default values.** Java's `getDefault(...)` consults
//!   `FieldDescriptor.hasDefaultValue()`, which in proto3 is always `false`; the per-type fallback
//!   is then applied. Magnetar follows the same fallbacks but cannot round-trip a proto2 explicit
//!   `default = "foo"` for `bytes` fields exactly — Java uses a Jackson `MAPPER.readTree(...)` on
//!   the C-escaped default-value string, and that escape grammar diverges from JSON's. Plain string
//!   / numeric defaults survive the round-trip byte-for-byte.
//! * **Avro `setStringType`.** Java sets the per-string property `{"avro.java.string": "String"}`
//!   on every string schema via `GenericData.setStringType`. Magnetar emits the plain `"string"`
//!   form (matching Avro's `Schema.toString()` when no props are set). This **does** create a
//!   byte-level divergence — see the test `string_field_avro_form_omits_avro_java_string` for the
//!   documented difference. The broker compares bytes verbatim, so a topic produced-to by both
//!   magnetar (via this constructor) and Java will register two distinct schema versions until
//!   either side aligns.
//!
//! Topics that need full Java byte parity should be validated against
//! `apachepulsar/pulsar:4.0.4` end-to-end before being promoted to production.

use std::collections::HashSet;
use std::marker::PhantomData;

use bytes::Bytes;
use prost::Message;
use prost_types::{
    DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorProto,
    field_descriptor_proto,
};

use super::{Schema, SchemaError};
use crate::pb;

/// Protobuf schema parametrised by a `prost::Message` type.
///
/// `schema_data()` returns either the serialised `FileDescriptorProto`
/// (descriptor-mode, via [`ProtobufSchema::with_file_descriptor_proto`] or
/// [`ProtobufSchema::with_file_descriptor`]) or the UTF-8-encoded fully-qualified
/// message name (legacy-mode, via [`ProtobufSchema::new`]). See the
/// [module docs](crate::schema) for the parity caveat.
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
    /// descriptor-shaped identifier. See the [module docs](crate::schema).
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

    /// Build a [`ProtobufSchema`] whose `schema_data` is the **Avro-from-protobuf
    /// JSON form** that Java's
    /// `org.apache.pulsar.client.impl.schema.ProtobufSchema` emits via
    /// `org.apache.avro.protobuf.ProtobufData.getSchema(pojo).toString()`.
    ///
    /// `message_name` is the simple (unqualified) name of the root message inside
    /// `file_descriptor` — e.g. `"SampleMessage"` for a descriptor declaring
    /// `package com.example; message SampleMessage { ... }`. Nested messages are not
    /// addressable as root types: use the outer message and reach the nested type
    /// through its containing fields, mirroring Java's reflective lookup.
    ///
    /// See the [`ProtobufSchema`] type-level docs (and the wider
    /// `schema::protobuf` module docs, which are the binding parity-caveat
    /// reference) for the full list of mapping rules and known divergences from
    /// Java's byte output. The most important ones in short:
    ///
    /// * `string` fields emit the plain `"string"` form, **not** Java's
    ///   `{"type":"string","avro.java.string":"String"}` — the broker will see two distinct schema
    ///   versions for a topic produced-to by both clients until either side aligns.
    /// * Avro `Conversion`s (e.g. `Timestamp` → logical type) are not honoured; well-known types
    ///   round-trip as plain records.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::Encoding`] if:
    /// * `message_name` is not present at the file's top level
    ///   (`file_descriptor.message_type[*].name`);
    /// * a field references a `type_name` (message or enum) that cannot be resolved from the file's
    ///   declared types;
    /// * a field uses `TYPE_GROUP` (deprecated in proto3, unsupported in Java's
    ///   `getNonRepeatedSchema(...)`).
    pub fn with_avro_canonical_from_descriptor(
        file_descriptor: &FileDescriptorProto,
        message_name: &str,
    ) -> Result<Self, SchemaError> {
        let node = avro_from_proto::build(file_descriptor, message_name)?;
        let bytes = node.into_bytes();
        Ok(Self {
            schema_data: Bytes::from(bytes),
            _marker: PhantomData,
        })
    }
}

/// Avro-from-protobuf schema builder, port of
/// `org.apache.avro.protobuf.ProtobufData`.
///
/// The module emits a [`serde_json::Value`] whose shape mirrors Avro Java's
/// `Schema.toString()` output for the schema graph that
/// `ProtobufData.getSchema(descriptor)` constructs. See the
/// [parent module docs](super) for the mapping rules and the
/// known limitations versus the Java reference.
mod avro_from_proto {
    use super::{
        DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorProto, HashSet,
        SchemaError, field_descriptor_proto,
    };

    /// JSON AST that preserves the insertion order of object keys.
    ///
    /// `serde_json::Map` sorts its keys alphabetically (it is a `BTreeMap` under
    /// the default feature set), which would diverge from Avro Java's
    /// `JsonGenerator` output. The enum below is a deliberately tiny subset
    /// covering only what Avro `Schema.toString()` ever emits.
    pub(super) enum Node {
        Null,
        Bool(bool),
        // Java emits `IntNode`, `LongNode`, `DoubleNode`; we keep that as a raw
        // numeric string so the byte form matches Jackson's emission verbatim.
        // The string is always a valid JSON number.
        Number(String),
        String(String),
        Array(Vec<Node>),
        /// Insertion-order-preserving object.
        Object(Vec<(String, Node)>),
    }

    impl Node {
        /// Render the AST into compact JSON (no whitespace), matching Avro's
        /// `Schema.toString()` output shape.
        pub(super) fn into_bytes(self) -> Vec<u8> {
            let mut out = Vec::new();
            self.write_into(&mut out);
            out
        }

        fn write_into(&self, out: &mut Vec<u8>) {
            match self {
                Node::Null => out.extend_from_slice(b"null"),
                Node::Bool(true) => out.extend_from_slice(b"true"),
                Node::Bool(false) => out.extend_from_slice(b"false"),
                Node::Number(n) => out.extend_from_slice(n.as_bytes()),
                Node::String(s) => write_json_string(out, s),
                Node::Array(items) => {
                    out.push(b'[');
                    for (i, item) in items.iter().enumerate() {
                        if i > 0 {
                            out.push(b',');
                        }
                        item.write_into(out);
                    }
                    out.push(b']');
                }
                Node::Object(entries) => {
                    out.push(b'{');
                    for (i, (k, v)) in entries.iter().enumerate() {
                        if i > 0 {
                            out.push(b',');
                        }
                        write_json_string(out, k);
                        out.push(b':');
                        v.write_into(out);
                    }
                    out.push(b'}');
                }
            }
        }
    }

    /// Minimal JSON-string serialiser matching Jackson's default escape rules
    /// (the subset Avro Java uses): escape `"`, `\`, control characters
    /// ` ..=`, and emit non-ASCII characters verbatim as UTF-8.
    fn write_json_string(out: &mut Vec<u8>, s: &str) {
        out.push(b'"');
        for c in s.chars() {
            match c {
                '"' => out.extend_from_slice(b"\\\""),
                '\\' => out.extend_from_slice(b"\\\\"),
                '\n' => out.extend_from_slice(b"\\n"),
                '\r' => out.extend_from_slice(b"\\r"),
                '\t' => out.extend_from_slice(b"\\t"),
                '\x08' => out.extend_from_slice(b"\\b"),
                '\x0c' => out.extend_from_slice(b"\\f"),
                c if (c as u32) < 0x20 => {
                    let mut buf = [0u8; 6];
                    let s = format_u16_hex(c as u32, &mut buf);
                    out.extend_from_slice(s);
                }
                c => {
                    let mut buf = [0u8; 4];
                    out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                }
            }
        }
        out.push(b'"');
    }

    fn format_u16_hex(code: u32, buf: &mut [u8; 6]) -> &[u8] {
        // \uXXXX
        buf[0] = b'\\';
        buf[1] = b'u';
        let hex = b"0123456789abcdef";
        buf[2] = hex[((code >> 12) & 0xF) as usize];
        buf[3] = hex[((code >> 8) & 0xF) as usize];
        buf[4] = hex[((code >> 4) & 0xF) as usize];
        buf[5] = hex[(code & 0xF) as usize];
        buf
    }

    /// Build the Avro JSON document for the message named `root_message`
    /// declared at the top level of `file_descriptor`.
    pub(super) fn build(
        file_descriptor: &FileDescriptorProto,
        root_message: &str,
    ) -> Result<Node, SchemaError> {
        let mut ctx = Context::new(file_descriptor);
        let root = ctx.resolve_top_level_message(root_message).ok_or_else(|| {
            SchemaError::Encoding(format!(
                "avro-from-protobuf: message `{root_message}` is not declared at the top \
                     level of the FileDescriptorProto"
            ))
        })?;
        ctx.message_schema(root, None)
    }

    /// Walking state shared across the recursive descent. Holds the file's symbol
    /// tables and the "seen names" set Avro Java uses to short-circuit recursion
    /// (a record that references itself emits its bare fully-qualified name on the
    /// second visit, exactly like `JsonGenerator.writeNameRef`).
    struct Context<'f> {
        file: &'f FileDescriptorProto,
        /// Java namespace derived from the file's `FileOptions` + `package`.
        file_namespace: String,
        /// Names that have already been written in expanded form. The second visit
        /// of a `record` / `enum` named `X` collapses to the bare string `"X"`,
        /// matching Java's `Schema.toJson` back-reference behaviour.
        seen_names: HashSet<String>,
    }

    impl<'f> Context<'f> {
        fn new(file: &'f FileDescriptorProto) -> Self {
            let file_namespace = compute_file_namespace(file);
            Self {
                file,
                file_namespace,
                seen_names: HashSet::new(),
            }
        }

        fn resolve_top_level_message(&self, name: &str) -> Option<&'f DescriptorProto> {
            self.file
                .message_type
                .iter()
                .find(|m| m.name.as_deref() == Some(name))
        }

        /// Build a message schema. `containing_chain` is the list of outer
        /// containing-message names (outermost first), mirroring Java's
        /// `getNamespace(file, containing)` walk.
        fn message_schema(
            &mut self,
            descriptor: &DescriptorProto,
            containing_chain: Option<&[&str]>,
        ) -> Result<Node, SchemaError> {
            let name = descriptor.name.as_deref().unwrap_or("");
            let namespace = self.namespace_for(containing_chain);
            let full_name = qualify(&namespace, name);

            // Back-reference: emit the bare full name (Avro's `writeNameRef` form).
            if self.seen_names.contains(&full_name) {
                return Ok(Node::String(full_name));
            }
            self.seen_names.insert(full_name);

            // Java toJson order: type, name, namespace, fields.
            let mut entries: Vec<(String, Node)> = Vec::with_capacity(4);
            entries.push(("type".to_owned(), Node::String("record".to_owned())));
            entries.push(("name".to_owned(), Node::String(name.to_owned())));
            if !namespace.is_empty() {
                entries.push(("namespace".to_owned(), Node::String(namespace)));
            }

            // Extend the containing chain for nested-type lookups.
            let mut next_chain: Vec<&str> =
                containing_chain.map(<[&str]>::to_vec).unwrap_or_default();
            next_chain.push(name);

            let mut fields = Vec::with_capacity(descriptor.field.len());
            for f in &descriptor.field {
                fields.push(self.field_schema(f, descriptor, &next_chain)?);
            }
            entries.push(("fields".to_owned(), Node::Array(fields)));

            Ok(Node::Object(entries))
        }

        /// Build a field schema. `containing_descriptor` is the parent message
        /// (used to look up nested types) and `containing_chain` is the list of
        /// containing-message names for namespace synthesis.
        fn field_schema(
            &mut self,
            field: &FieldDescriptorProto,
            containing_descriptor: &DescriptorProto,
            containing_chain: &[&str],
        ) -> Result<Node, SchemaError> {
            let name = field.name.as_deref().unwrap_or("");
            let non_repeated =
                self.non_repeated_schema(field, containing_descriptor, containing_chain)?;

            let value_schema = if is_repeated(field) {
                // Java: Schema.createArray(s) → {"type":"array","items":<s>}.
                Node::Object(vec![
                    ("type".to_owned(), Node::String("array".to_owned())),
                    ("items".to_owned(), non_repeated),
                ])
            } else {
                non_repeated
            };

            // Java toJson order for fields: name, type, default.
            Ok(Node::Object(vec![
                ("name".to_owned(), Node::String(name.to_owned())),
                ("type".to_owned(), value_schema),
                (
                    "default".to_owned(),
                    self.default_for(field, containing_descriptor),
                ),
            ]))
        }

        /// Java's `getNonRepeatedSchema` — returns the schema for the field's
        /// element type, before any `array` wrapping.
        fn non_repeated_schema(
            &mut self,
            field: &FieldDescriptorProto,
            containing_descriptor: &DescriptorProto,
            containing_chain: &[&str],
        ) -> Result<Node, SchemaError> {
            let ty = field
                .r#type
                .and_then(|t| field_descriptor_proto::Type::try_from(t).ok());
            match ty {
                Some(field_descriptor_proto::Type::Bool) => Ok(Node::String("boolean".to_owned())),
                Some(field_descriptor_proto::Type::Float) => Ok(Node::String("float".to_owned())),
                Some(field_descriptor_proto::Type::Double) => Ok(Node::String("double".to_owned())),
                Some(field_descriptor_proto::Type::String) => Ok(Node::String("string".to_owned())),
                Some(field_descriptor_proto::Type::Bytes) => Ok(Node::String("bytes".to_owned())),
                Some(
                    field_descriptor_proto::Type::Int32
                    | field_descriptor_proto::Type::Uint32
                    | field_descriptor_proto::Type::Sint32
                    | field_descriptor_proto::Type::Fixed32
                    | field_descriptor_proto::Type::Sfixed32,
                ) => Ok(Node::String("int".to_owned())),
                Some(
                    field_descriptor_proto::Type::Int64
                    | field_descriptor_proto::Type::Uint64
                    | field_descriptor_proto::Type::Sint64
                    | field_descriptor_proto::Type::Fixed64
                    | field_descriptor_proto::Type::Sfixed64,
                ) => Ok(Node::String("long".to_owned())),
                Some(field_descriptor_proto::Type::Enum) => {
                    let (descriptor, owner_chain) =
                        self.resolve_enum(field, containing_descriptor, containing_chain)?;
                    self.enum_schema(descriptor, Some(&owner_chain))
                }
                Some(field_descriptor_proto::Type::Message) => {
                    let (descriptor, owner_chain) =
                        self.resolve_message(field, containing_descriptor, containing_chain)?;
                    let inner = self.message_schema(descriptor, Some(&owner_chain))?;
                    if is_optional_message(field) {
                        // Java: `f.isOptional()` is true for proto2 optional message
                        // fields → wrap in `["null", <record>]`.
                        Ok(Node::Array(vec![Node::String("null".to_owned()), inner]))
                    } else {
                        Ok(inner)
                    }
                }
                Some(field_descriptor_proto::Type::Group) | None => {
                    Err(SchemaError::Encoding(format!(
                        "avro-from-protobuf: unsupported field type for `{name}` (TYPE_GROUP or \
                         unknown)",
                        name = field.name.as_deref().unwrap_or("<unnamed>"),
                    )))
                }
            }
        }

        fn enum_schema(
            &mut self,
            descriptor: &EnumDescriptorProto,
            containing_chain: Option<&[&str]>,
        ) -> Result<Node, SchemaError> {
            let name = descriptor.name.as_deref().unwrap_or("");
            let namespace = self.namespace_for(containing_chain);
            let full_name = qualify(&namespace, name);
            if self.seen_names.contains(&full_name) {
                return Ok(Node::String(full_name));
            }
            self.seen_names.insert(full_name);

            // Java toJson order for enums: type, name, namespace, symbols.
            let mut entries: Vec<(String, Node)> = Vec::with_capacity(4);
            entries.push(("type".to_owned(), Node::String("enum".to_owned())));
            entries.push(("name".to_owned(), Node::String(name.to_owned())));
            if !namespace.is_empty() {
                entries.push(("namespace".to_owned(), Node::String(namespace)));
            }
            let symbols = descriptor
                .value
                .iter()
                .map(|v| Node::String(v.name.clone().unwrap_or_default()))
                .collect();
            entries.push(("symbols".to_owned(), Node::Array(symbols)));
            Ok(Node::Object(entries))
        }

        /// Resolve a `TYPE_MESSAGE` field's `type_name` against the current
        /// descriptor's nested types, then the file's top-level messages.
        fn resolve_message<'a>(
            &self,
            field: &FieldDescriptorProto,
            containing_descriptor: &'a DescriptorProto,
            containing_chain: &[&'a str],
        ) -> Result<(&'a DescriptorProto, Vec<&'a str>), SchemaError>
        where
            'f: 'a,
        {
            let type_name = field_simple_type_name(field, "TYPE_MESSAGE")?;
            if let Some(nested) = containing_descriptor
                .nested_type
                .iter()
                .find(|m| m.name.as_deref() == Some(type_name))
            {
                // For a nested type, the owner-chain is the parent chain (the
                // nested type's name is the type's own name, written via "name",
                // not part of the namespace).
                return Ok((nested, containing_chain.to_vec()));
            }
            if let Some(top) = self
                .file
                .message_type
                .iter()
                .find(|m| m.name.as_deref() == Some(type_name))
            {
                return Ok((top, Vec::new()));
            }
            Err(SchemaError::Encoding(format!(
                "avro-from-protobuf: unresolved TYPE_MESSAGE reference `{type_name}`"
            )))
        }

        /// Resolve a `TYPE_ENUM` field's `type_name` against nested enums then
        /// file-level enums.
        fn resolve_enum<'a>(
            &self,
            field: &FieldDescriptorProto,
            containing_descriptor: &'a DescriptorProto,
            containing_chain: &[&'a str],
        ) -> Result<(&'a EnumDescriptorProto, Vec<&'a str>), SchemaError>
        where
            'f: 'a,
        {
            let type_name = field_simple_type_name(field, "TYPE_ENUM")?;
            if let Some(nested) = containing_descriptor
                .enum_type
                .iter()
                .find(|e| e.name.as_deref() == Some(type_name))
            {
                return Ok((nested, containing_chain.to_vec()));
            }
            if let Some(top) = self
                .file
                .enum_type
                .iter()
                .find(|e| e.name.as_deref() == Some(type_name))
            {
                return Ok((top, Vec::new()));
            }
            Err(SchemaError::Encoding(format!(
                "avro-from-protobuf: unresolved TYPE_ENUM reference `{type_name}`"
            )))
        }

        /// Java's `getNamespace(fd, containing)` — join the file-level Java
        /// namespace with the `$`-separated chain of containing message names.
        fn namespace_for(&self, containing_chain: Option<&[&str]>) -> String {
            let chain = containing_chain.unwrap_or(&[]);
            if chain.is_empty() {
                return self.file_namespace.clone();
            }
            let inner = chain.join("$");
            if self.file_namespace.is_empty() {
                inner
            } else {
                format!("{}.{}", self.file_namespace, inner)
            }
        }
    }

    /// Mirror Java's `ProtobufData.getNamespace(FileDescriptor, null)` for the
    /// top-level case (no containing message).
    fn compute_file_namespace(file: &FileDescriptorProto) -> String {
        let options = file.options.as_ref();
        let java_package = options
            .and_then(|o| o.java_package.as_deref())
            .filter(|s| !s.is_empty());
        let proto_package = file.package.as_deref().unwrap_or("");
        let p = java_package.unwrap_or(proto_package).to_owned();

        // outer name: only when java_multiple_files is unset/false.
        let java_multiple_files = options.and_then(|o| o.java_multiple_files).unwrap_or(false);
        let outer = if java_multiple_files {
            String::new()
        } else if let Some(name) = options
            .and_then(|o| o.java_outer_classname.as_deref())
            .filter(|s| !s.is_empty())
        {
            name.to_owned()
        } else {
            // Java: basename(fd.getName()), strip extension, CamelCase.
            outer_from_filename(file.name.as_deref().unwrap_or(""))
        };

        let separator_after_pkg = if outer.is_empty() { "" } else { "." };
        format!("{p}{separator_after_pkg}{outer}")
    }

    /// Java's `toCamelCase(stripExtension(basename))` for the file-name fallback.
    fn outer_from_filename(file_name: &str) -> String {
        let basename = file_name.rsplit('/').next().unwrap_or(file_name);
        let stem = basename
            .rsplit_once('.')
            .map(|(s, _ext)| s)
            .unwrap_or(basename);
        let mut out = String::with_capacity(stem.len());
        for part in stem.split('_') {
            if part.is_empty() {
                continue;
            }
            let mut chars = part.chars();
            if let Some(first) = chars.next() {
                out.extend(first.to_uppercase());
            }
            for c in chars {
                out.extend(c.to_lowercase());
            }
        }
        out
    }

    impl Context<'_> {
        /// Java: `Schema.createField(..., getDefault(f), ...)` — see
        /// `ProtobufData.getDefault(FieldDescriptor)`.
        fn default_for(
            &self,
            field: &FieldDescriptorProto,
            containing_descriptor: &DescriptorProto,
        ) -> Node {
            if is_required(field) {
                return Node::Null;
            }
            if is_repeated(field) {
                return Node::Array(Vec::new());
            }
            // proto2 explicit default — best-effort: numerics parsed as numbers,
            // strings/bytes left as the descriptor's literal string, others fall back
            // to type defaults. Java uses Jackson's tree parser on the C-escaped
            // default-value string; magnetar reads it as JSON for numeric types only.
            if let Some(raw) = field.default_value.as_deref() {
                let ty = field
                    .r#type
                    .and_then(|t| field_descriptor_proto::Type::try_from(t).ok());
                match ty {
                    Some(
                        field_descriptor_proto::Type::Int32
                        | field_descriptor_proto::Type::Uint32
                        | field_descriptor_proto::Type::Sint32
                        | field_descriptor_proto::Type::Fixed32
                        | field_descriptor_proto::Type::Sfixed32
                        | field_descriptor_proto::Type::Int64
                        | field_descriptor_proto::Type::Uint64
                        | field_descriptor_proto::Type::Sint64
                        | field_descriptor_proto::Type::Fixed64
                        | field_descriptor_proto::Type::Sfixed64,
                    ) if raw.parse::<i64>().is_ok() => {
                        return Node::Number(raw.to_owned());
                    }
                    Some(
                        field_descriptor_proto::Type::Float | field_descriptor_proto::Type::Double,
                    ) if raw.parse::<f64>().is_ok() => {
                        return Node::Number(raw.to_owned());
                    }
                    Some(field_descriptor_proto::Type::Bool) => match raw {
                        "true" => return Node::Bool(true),
                        "false" => return Node::Bool(false),
                        _ => {}
                    },
                    Some(
                        field_descriptor_proto::Type::String | field_descriptor_proto::Type::Bytes,
                    ) => {
                        return Node::String(raw.to_owned());
                    }
                    _ => {}
                }
            }
            // Java per-type fallback defaults.
            match field
                .r#type
                .and_then(|t| field_descriptor_proto::Type::try_from(t).ok())
            {
                Some(field_descriptor_proto::Type::Bool) => Node::Bool(false),
                Some(
                    field_descriptor_proto::Type::Float | field_descriptor_proto::Type::Double,
                ) => {
                    // Java emits `0.0F` / `0.0D` as Jackson `numberNode(0.0F)` /
                    // `numberNode(0.0D)`, which renders as `0.0`. We mirror that.
                    Node::Number("0.0".to_owned())
                }
                Some(
                    field_descriptor_proto::Type::Int32
                    | field_descriptor_proto::Type::Uint32
                    | field_descriptor_proto::Type::Sint32
                    | field_descriptor_proto::Type::Fixed32
                    | field_descriptor_proto::Type::Sfixed32
                    | field_descriptor_proto::Type::Int64
                    | field_descriptor_proto::Type::Uint64
                    | field_descriptor_proto::Type::Sint64
                    | field_descriptor_proto::Type::Fixed64
                    | field_descriptor_proto::Type::Sfixed64,
                ) => Node::Number("0".to_owned()),
                Some(
                    field_descriptor_proto::Type::String | field_descriptor_proto::Type::Bytes,
                ) => Node::String(String::new()),
                // Java emits `nullNode()` for MESSAGE — represented as JSON null.
                Some(field_descriptor_proto::Type::Message) => Node::Null,
                // Java emits `textNode(f.getEnumType().getValues().get(0).getName())`.
                // Resolve the enum and emit the first symbol's name; fall back to
                // `null` if the reference cannot be resolved.
                Some(field_descriptor_proto::Type::Enum) => {
                    match self.resolve_enum(field, containing_descriptor, &[]) {
                        Ok((descriptor, _chain)) => descriptor
                            .value
                            .first()
                            .and_then(|v| v.name.as_deref())
                            .map(|n| Node::String(n.to_owned()))
                            .unwrap_or(Node::Null),
                        Err(_) => Node::Null,
                    }
                }
                _ => Node::Null,
            }
        }
    }

    fn is_repeated(field: &FieldDescriptorProto) -> bool {
        field
            .label
            .and_then(|l| field_descriptor_proto::Label::try_from(l).ok())
            .map(|l| matches!(l, field_descriptor_proto::Label::Repeated))
            .unwrap_or(false)
    }

    fn is_optional(field: &FieldDescriptorProto) -> bool {
        field
            .label
            .and_then(|l| field_descriptor_proto::Label::try_from(l).ok())
            .map(|l| matches!(l, field_descriptor_proto::Label::Optional))
            .unwrap_or(false)
    }

    fn is_required(field: &FieldDescriptorProto) -> bool {
        field
            .label
            .and_then(|l| field_descriptor_proto::Label::try_from(l).ok())
            .map(|l| matches!(l, field_descriptor_proto::Label::Required))
            .unwrap_or(false)
    }

    /// Java's `f.isOptional()` is true for proto2 optional fields; in proto3 the
    /// label is `LABEL_OPTIONAL` for every singular field but the union-with-null
    /// wrapper is only applied for *message* fields, which is what we mirror.
    fn is_optional_message(field: &FieldDescriptorProto) -> bool {
        is_optional(field)
    }

    /// Strip the leading dot and any package qualifier off a field's `type_name`,
    /// returning the simple name. `"<descriptor>"` is the error tag used in the
    /// resulting `SchemaError`.
    fn field_simple_type_name<'a>(
        field: &'a FieldDescriptorProto,
        tag: &str,
    ) -> Result<&'a str, SchemaError> {
        let raw = field.type_name.as_deref().ok_or_else(|| {
            SchemaError::Encoding(format!(
                "avro-from-protobuf: {tag} field `{name}` has no type_name",
                name = field.name.as_deref().unwrap_or("<unnamed>"),
            ))
        })?;
        let trimmed = raw.trim_start_matches('.');
        Ok(trimmed.rsplit('.').next().unwrap_or(trimmed))
    }

    fn qualify(namespace: &str, name: &str) -> String {
        if namespace.is_empty() {
            name.to_owned()
        } else {
            format!("{namespace}.{name}")
        }
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

    // ---- Avro-from-protobuf form (Java parity-aimed) ----------------------

    use prost_types::{EnumDescriptorProto, EnumValueDescriptorProto, FileOptions};

    /// Sample message used to exercise the major mapping rules in one shot:
    /// scalars (int32, string), a repeated string, a nested enum, and a
    /// reference to that enum.
    ///
    /// ```proto
    /// syntax = "proto3";
    /// package com.example;
    /// option java_package = "com.example.gen";
    /// option java_multiple_files = true;
    /// message SampleMessage {
    ///   int32 id = 1;
    ///   string name = 2;
    ///   repeated string tags = 3;
    ///   Kind kind = 4;
    ///   enum Kind { UNSPEC = 0; ALPHA = 1; BETA = 2; }
    /// }
    /// ```
    fn sample_message_descriptor() -> FileDescriptorProto {
        FileDescriptorProto {
            name: Some("sample.proto".to_owned()),
            package: Some("com.example".to_owned()),
            syntax: Some("proto3".to_owned()),
            options: Some(FileOptions {
                java_package: Some("com.example.gen".to_owned()),
                java_multiple_files: Some(true),
                ..FileOptions::default()
            }),
            message_type: vec![DescriptorProto {
                name: Some("SampleMessage".to_owned()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("id".to_owned()),
                        number: Some(1),
                        r#type: Some(field_descriptor_proto::Type::Int32 as i32),
                        label: Some(field_descriptor_proto::Label::Optional as i32),
                        ..FieldDescriptorProto::default()
                    },
                    FieldDescriptorProto {
                        name: Some("name".to_owned()),
                        number: Some(2),
                        r#type: Some(field_descriptor_proto::Type::String as i32),
                        label: Some(field_descriptor_proto::Label::Optional as i32),
                        ..FieldDescriptorProto::default()
                    },
                    FieldDescriptorProto {
                        name: Some("tags".to_owned()),
                        number: Some(3),
                        r#type: Some(field_descriptor_proto::Type::String as i32),
                        label: Some(field_descriptor_proto::Label::Repeated as i32),
                        ..FieldDescriptorProto::default()
                    },
                    FieldDescriptorProto {
                        name: Some("kind".to_owned()),
                        number: Some(4),
                        r#type: Some(field_descriptor_proto::Type::Enum as i32),
                        type_name: Some(".com.example.SampleMessage.Kind".to_owned()),
                        label: Some(field_descriptor_proto::Label::Optional as i32),
                        ..FieldDescriptorProto::default()
                    },
                ],
                enum_type: vec![EnumDescriptorProto {
                    name: Some("Kind".to_owned()),
                    value: vec![
                        EnumValueDescriptorProto {
                            name: Some("UNSPEC".to_owned()),
                            number: Some(0),
                            ..EnumValueDescriptorProto::default()
                        },
                        EnumValueDescriptorProto {
                            name: Some("ALPHA".to_owned()),
                            number: Some(1),
                            ..EnumValueDescriptorProto::default()
                        },
                        EnumValueDescriptorProto {
                            name: Some("BETA".to_owned()),
                            number: Some(2),
                            ..EnumValueDescriptorProto::default()
                        },
                    ],
                    ..EnumDescriptorProto::default()
                }],
                ..DescriptorProto::default()
            }],
            ..FileDescriptorProto::default()
        }
    }

    #[test]
    fn avro_from_proto_sample_message_matches_expected_json() {
        // Hand-built expected document: matches the structural form Java's
        // `ProtobufData.getSchema(SampleMessage.getDescriptor()).toString()`
        // would produce for the same descriptor (modulo the `avro.java.string`
        // omission noted in the module docs).
        //
        // Java key ordering: record → type, name, namespace, fields. Each
        // field → name, type, default. Enum → type, name, namespace, symbols.
        let descriptor = sample_message_descriptor();
        let schema = ProtobufSchema::<KeyValue>::with_avro_canonical_from_descriptor(
            &descriptor,
            "SampleMessage",
        )
        .expect("avro-from-protobuf build");
        let actual = std::str::from_utf8(schema.schema_data().as_ref())
            .expect("utf-8")
            .to_owned();
        let expected = concat!(
            r#"{"type":"record","name":"SampleMessage","namespace":"com.example.gen","#,
            r#""fields":[{"name":"id","type":"int","default":0},"#,
            r#"{"name":"name","type":"string","default":""},"#,
            r#"{"name":"tags","type":{"type":"array","items":"string"},"default":[]},"#,
            r#"{"name":"kind","type":{"type":"enum","name":"Kind","namespace":"com.example.gen.SampleMessage","symbols":["UNSPEC","ALPHA","BETA"]},"default":"UNSPEC"}]}"#,
        );
        assert_eq!(actual, expected);
        assert_eq!(schema.schema_type(), pb::schema::Type::Protobuf);
    }

    #[test]
    fn avro_from_proto_is_deterministic() {
        let descriptor = sample_message_descriptor();
        let a = ProtobufSchema::<KeyValue>::with_avro_canonical_from_descriptor(
            &descriptor,
            "SampleMessage",
        )
        .expect("a");
        let b = ProtobufSchema::<KeyValue>::with_avro_canonical_from_descriptor(
            &descriptor,
            "SampleMessage",
        )
        .expect("b");
        assert_eq!(a.schema_data(), b.schema_data());
    }

    #[test]
    fn avro_from_proto_unknown_message_rejected() {
        let descriptor = sample_message_descriptor();
        let err = ProtobufSchema::<KeyValue>::with_avro_canonical_from_descriptor(
            &descriptor,
            "DoesNotExist",
        )
        .unwrap_err();
        assert!(
            matches!(err, SchemaError::Encoding(ref m) if m.contains("DoesNotExist")),
            "expected Encoding error referencing the missing message, got {err:?}",
        );
    }

    #[test]
    fn avro_from_proto_key_value_minimal_shape() {
        // The minimal `KeyValue` fixture has no FileOptions, so the namespace
        // collapses to the file's `package` joined with the file-name fallback
        // ("pulsar.proto" -> outer "Pulsar" via the basename CamelCase rule).
        let descriptor = key_value_descriptor();
        let schema = ProtobufSchema::<KeyValue>::with_avro_canonical_from_descriptor(
            &descriptor,
            "KeyValue",
        )
        .expect("avro-from-protobuf build");
        let json = std::str::from_utf8(schema.schema_data().as_ref())
            .expect("utf-8")
            .to_owned();
        // `pulsar.proto` -> stem `pulsar` -> outer `Pulsar`.
        assert!(
            json.contains(r#""namespace":"pulsar.proto.Pulsar""#),
            "namespace must reflect Java's outer-classname fallback; got {json}",
        );
        assert!(json.contains(r#""name":"KeyValue""#));
        assert!(json.contains(r#""name":"key","type":"string","default":"""#));
        assert!(json.contains(r#""name":"value","type":"string","default":"""#));
    }

    #[test]
    fn string_field_avro_form_omits_avro_java_string() {
        // Documented divergence: Java sets `{"avro.java.string":"String"}` on
        // every string schema via `GenericData.setStringType`. Magnetar emits
        // the plain `"string"` form. This test pins the divergence so a future
        // change has to explicitly acknowledge the parity step.
        let descriptor = sample_message_descriptor();
        let schema = ProtobufSchema::<KeyValue>::with_avro_canonical_from_descriptor(
            &descriptor,
            "SampleMessage",
        )
        .expect("build");
        let json = std::str::from_utf8(schema.schema_data().as_ref())
            .expect("utf-8")
            .to_owned();
        assert!(
            !json.contains("avro.java.string"),
            "magnetar deliberately omits the `avro.java.string` property; if that \
             changes, update the module docs and the parity caveat list",
        );
    }
}
