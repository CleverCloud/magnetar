// SPDX-License-Identifier: Apache-2.0

//! Composite key/value schema.
//!
//! Mirrors `org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl`. A `KeyValueSchema<K, V>`
//! composes two child schemas — one for the key, one for the value — into a single Pulsar schema
//! whose `schema_data` is a small **binary** payload the broker stores verbatim and compares
//! by raw-byte equality.
//!
//! # Wire shape of `schema_data`
//!
//! The Java client (`pulsar-common/.../KeyValueSchemaInfo.java::encodeKeyValueSchemaInfo`)
//! emits the **binary** layout:
//!
//! ```text
//! [key_schema_data.len: u32 big-endian]
//! [key_schema_data bytes]    (raw bytes from key sub-schema's `schema_data()`)
//! [value_schema_data.len: u32 big-endian]
//! [value_schema_data bytes]  (raw bytes from value sub-schema's `schema_data()`)
//! ```
//!
//! Sub-schema **metadata** (name, type, properties, encoding mode) does NOT live inside
//! `schema_data` — it goes into `CommandProducer.schema.properties` as a flat key-value map
//! with these seven entries (Java constants verbatim):
//!
//! - `key.schema.name`, `key.schema.type`, `key.schema.properties`
//! - `value.schema.name`, `value.schema.type`, `value.schema.properties`
//! - `kv.encoding.type` (= `SEPARATED` | `INLINE`, all caps)
//!
//! See [`KeyValueSchema::properties`] for the map content. The broker's KeyValue producer
//! validation reads these properties; a missing or mis-cased entry causes the broker to
//! silently fail `CommandProducer` and the user's `producer.create().await` hangs.
//!
//! # Payload encoding mode
//!
//! - [`KeyValueEncodingType::Separated`] (default): only the value bytes go in the payload; the key
//!   bytes are carried in `MessageMetadata.partition_key`. Matches the Java default.
//! - [`KeyValueEncodingType::Inline`]: the wire payload is `[u32 key_len][key bytes][u32
//!   value_len][value bytes]` (big-endian). The decoder reads the same shape back.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{Schema, SchemaError};
use crate::pb;

/// Layout choice for [`KeyValueSchema`] payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeyValueEncodingType {
    /// Key lives in `MessageMetadata.partition_key`; payload carries only the value bytes. This
    /// is the Java default.
    #[default]
    Separated,
    /// Both key and value bytes are framed inside the payload. Used when callers cannot rely on
    /// `partition_key` (e.g. compacted topics with non-string keys).
    Inline,
}

impl KeyValueEncodingType {
    fn as_str(self) -> &'static str {
        // Java emits `SEPARATED` / `INLINE` (all caps, no underscores) as the
        // `kv.encoding.type` schema property and the broker validates the
        // string match. Mismatching case ("Inline" vs "INLINE") makes the
        // broker reject KeyValue producer creation.
        match self {
            Self::Separated => "SEPARATED",
            Self::Inline => "INLINE",
        }
    }
}

/// Decoded `(key, value)` pair produced by [`KeyValueSchema::decode`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValuePair<K, V> {
    /// Decoded key.
    pub key: K,
    /// Decoded value.
    pub value: V,
}

/// Composite schema wrapping a key schema and a value schema.
#[derive(Debug)]
pub struct KeyValueSchema<K, V>
where
    K: Schema,
    V: Schema,
{
    key_schema: K,
    value_schema: V,
    encoding: KeyValueEncodingType,
    schema_data: Bytes,
}

impl<K, V> KeyValueSchema<K, V>
where
    K: Schema,
    V: Schema,
{
    /// Build a [`KeyValueSchema`] over the supplied key and value schemas.
    pub fn new(key_schema: K, value_schema: V, encoding: KeyValueEncodingType) -> Self {
        let schema_data = build_schema_data(
            key_schema.schema_type(),
            key_schema.schema_data().as_ref(),
            value_schema.schema_type(),
            value_schema.schema_data().as_ref(),
            encoding,
        );
        Self {
            key_schema,
            value_schema,
            encoding,
            schema_data,
        }
    }

    /// Borrow the key sub-schema.
    pub fn key_schema(&self) -> &K {
        &self.key_schema
    }

    /// Borrow the value sub-schema.
    pub fn value_schema(&self) -> &V {
        &self.value_schema
    }

    /// Configured [`KeyValueEncodingType`].
    pub fn encoding(&self) -> KeyValueEncodingType {
        self.encoding
    }
}

impl<K, V> Schema for KeyValueSchema<K, V>
where
    K: Schema,
    V: Schema,
{
    type Owned = KeyValuePair<K::Owned, V::Owned>;

    fn schema_type(&self) -> pb::schema::Type {
        pb::schema::Type::KeyValue
    }

    fn schema_data(&self) -> Bytes {
        self.schema_data.clone()
    }

    fn properties(&self) -> Vec<(String, String)> {
        // Mirror Java's `KeyValueSchemaInfo.encodeKeyValueSchemaInfo`. The broker requires
        // these seven keys when `Schema.type = KEY_VALUE`; without them it silently fails
        // CommandProducer validation and the client's `producer.create().await` hangs.
        vec![
            (
                "key.schema.name".to_owned(),
                schema_type_name(self.key_schema.schema_type()),
            ),
            (
                "key.schema.type".to_owned(),
                schema_type_name(self.key_schema.schema_type()),
            ),
            // Java emits `{}` for empty property maps; mirror that.
            ("key.schema.properties".to_owned(), "{}".to_owned()),
            (
                "value.schema.name".to_owned(),
                schema_type_name(self.value_schema.schema_type()),
            ),
            (
                "value.schema.type".to_owned(),
                schema_type_name(self.value_schema.schema_type()),
            ),
            ("value.schema.properties".to_owned(), "{}".to_owned()),
            (
                "kv.encoding.type".to_owned(),
                self.encoding.as_str().to_owned(),
            ),
        ]
    }

    fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError> {
        let key_bytes = self.key_schema.encode(&value.key)?;
        let value_bytes = self.value_schema.encode(&value.value)?;
        match self.encoding {
            KeyValueEncodingType::Separated => {
                // Carrier semantics: when `Separated` is in use the key is meant to land in
                // `MessageMetadata.partition_key`. Until the producer wires that path up (M7),
                // we emit the value bytes alone — callers carry the key out-of-band via the
                // producer API.
                Ok(value_bytes)
            }
            KeyValueEncodingType::Inline => {
                let mut buf = BytesMut::with_capacity(8 + key_bytes.len() + value_bytes.len());
                buf.put_u32(
                    u32::try_from(key_bytes.len())
                        .map_err(|_| SchemaError::Encoding("key length exceeds u32".to_owned()))?,
                );
                buf.extend_from_slice(&key_bytes);
                buf.put_u32(
                    u32::try_from(value_bytes.len()).map_err(|_| {
                        SchemaError::Encoding("value length exceeds u32".to_owned())
                    })?,
                );
                buf.extend_from_slice(&value_bytes);
                Ok(buf.freeze())
            }
        }
    }

    fn decode(&self, bytes: &[u8]) -> Result<Self::Owned, SchemaError> {
        match self.encoding {
            KeyValueEncodingType::Separated => Err(SchemaError::Unsupported(
                "KeyValueSchema::decode in Separated mode requires the key carrier (\
                 MessageMetadata.partition_key) — use decode_with_key once available"
                    .to_owned(),
            )),
            KeyValueEncodingType::Inline => {
                let mut cursor = bytes;
                if cursor.remaining() < 4 {
                    return Err(SchemaError::Decoding("missing key length".to_owned()));
                }
                let key_len = cursor.get_u32() as usize;
                if cursor.remaining() < key_len {
                    return Err(SchemaError::Decoding("truncated key bytes".to_owned()));
                }
                let key_slice = &cursor[..key_len];
                let key = self.key_schema.decode(key_slice)?;
                cursor.advance(key_len);
                if cursor.remaining() < 4 {
                    return Err(SchemaError::Decoding("missing value length".to_owned()));
                }
                let value_len = cursor.get_u32() as usize;
                if cursor.remaining() < value_len {
                    return Err(SchemaError::Decoding("truncated value bytes".to_owned()));
                }
                let value_slice = &cursor[..value_len];
                let value = self.value_schema.decode(value_slice)?;
                Ok(KeyValuePair { key, value })
            }
        }
    }
}

impl<K, V> KeyValueSchema<K, V>
where
    K: Schema,
    V: Schema,
{
    /// Decode a `Separated`-mode payload with the key bytes supplied out-of-band (typically from
    /// `MessageMetadata.partition_key`).
    ///
    /// # Errors
    ///
    /// Propagates [`SchemaError`] from either child schema.
    pub fn decode_with_key(
        &self,
        key_bytes: &[u8],
        value_bytes: &[u8],
    ) -> Result<KeyValuePair<K::Owned, V::Owned>, SchemaError> {
        let key = self.key_schema.decode(key_bytes)?;
        let value = self.value_schema.decode(value_bytes)?;
        Ok(KeyValuePair { key, value })
    }
}

/// Render the broker-side `schema_data` bytes in the **binary** layout Pulsar's Java client
/// emits for KeyValue schemas. The Java code path is
/// `KeyValueSchemaInfo.encodeKeyValueSchemaInfo(...)` (branch-4.0,
/// `pulsar-common/.../KeyValueSchemaInfo.java`):
///
/// ```text
/// [key_schema_data.len: i32 big-endian]
/// [key_schema_data bytes — raw `SchemaInfo.getSchema()` of the key]
/// [value_schema_data.len: i32 big-endian]
/// [value_schema_data bytes — raw `SchemaInfo.getSchema()` of the value]
/// ```
///
/// The sub-schemas' **metadata** (name, type, properties, encoding type) goes into
/// `CommandProducer.schema.properties` as a flat map — see [`Self::sub_schema_properties`].
/// Sending JSON here (the magnetar pre-fix shape) makes the broker silently drop the
/// `CommandProducer` because Pulsar's broker validates the layout shape, not the JSON.
fn build_schema_data(
    _key_type: pb::schema::Type,
    key_data: &[u8],
    _value_type: pb::schema::Type,
    value_data: &[u8],
    _encoding: KeyValueEncodingType,
) -> Bytes {
    let mut out = BytesMut::with_capacity(8 + key_data.len() + value_data.len());
    out.put_u32(u32::try_from(key_data.len()).unwrap_or(u32::MAX));
    out.extend_from_slice(key_data);
    out.put_u32(u32::try_from(value_data.len()).unwrap_or(u32::MAX));
    out.extend_from_slice(value_data);
    out.freeze()
}

/// Map [`pb::schema::Type`] to the **upper-case** name the Java client emits in
/// `key.schema.name` / `value.schema.name` / `key.schema.type` / `value.schema.type`
/// properties. Java's `SchemaType.name()` returns `STRING`, `INT32`, `JSON`,
/// `KEY_VALUE`, etc. — all caps with underscores.
///
/// `prost::Enumeration::as_str_name()` returns the Rust-style title-case form
/// (`"String"`, `"Json"`, `"KeyValue"`) which the broker's KeyValue schema
/// validation rejects as "unknown schema type". Convert here.
fn schema_type_name(ty: pb::schema::Type) -> String {
    use pb::schema::Type;
    match ty {
        Type::None => "BYTES",
        Type::String => "STRING",
        Type::Json => "JSON",
        Type::Protobuf => "PROTOBUF",
        Type::Avro => "AVRO",
        Type::Bool => "BOOLEAN",
        Type::Int8 => "INT8",
        Type::Int16 => "INT16",
        Type::Int32 => "INT32",
        Type::Int64 => "INT64",
        Type::Float => "FLOAT",
        Type::Double => "DOUBLE",
        Type::Date => "DATE",
        Type::Time => "TIME",
        Type::Timestamp => "TIMESTAMP",
        Type::KeyValue => "KEY_VALUE",
        Type::Instant => "INSTANT",
        Type::LocalDate => "LOCAL_DATE",
        Type::LocalTime => "LOCAL_TIME",
        Type::LocalDateTime => "LOCAL_DATE_TIME",
        Type::ProtobufNative => "PROTOBUF_NATIVE",
        Type::AutoConsume => "AUTO_CONSUME",
        Type::External => "AUTO_PUBLISH",
    }
    .to_owned()
}

/// Standard base64 encoder (RFC 4648 alphabet, with `=` padding). Inlined to avoid pulling
/// `base64` into the magnetar-proto dep graph — schema canonicalisation is the only consumer.
// reason: kept alongside the test-only `base64_decode` below for KeyValue canonical-form work
// that has not yet wired through this helper; the schemars-driven path doesn't need it today.
#[allow(dead_code)]
fn base64_encode(out: &mut String, input: &[u8]) {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut i = 0;
    while i + 3 <= input.len() {
        let n =
            (u32::from(input[i]) << 16) | (u32::from(input[i + 1]) << 8) | u32::from(input[i + 2]);
        out.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);
        out.push(ALPHABET[((n >> 6) & 0x3F) as usize] as char);
        out.push(ALPHABET[(n & 0x3F) as usize] as char);
        i += 3;
    }
    match input.len() - i {
        0 => {}
        1 => {
            let n = u32::from(input[i]) << 16;
            out.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
            out.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);
            out.push('=');
            out.push('=');
        }
        2 => {
            let n = (u32::from(input[i]) << 16) | (u32::from(input[i + 1]) << 8);
            out.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
            out.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);
            out.push(ALPHABET[((n >> 6) & 0x3F) as usize] as char);
            out.push('=');
        }
        // reason: invariant #6 forbids panics (including `debug_assert!`) in
        // `magnetar-proto` production code. The above `while i + 3 <= input.len()` loop
        // advances `i` by 3 each iteration, so on exit `input.len() - i ∈ {0, 1, 2}` is
        // provably exhaustive. The wildcard is statically unreachable for valid inputs;
        // the silent no-op fallthrough leaves `out` as a valid (albeit empty-tail)
        // base64 string — a safe fallback rather than a panic.
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use schemars::JsonSchema as SchemarsJsonSchema;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::schema::{JsonSchema, StringSchema};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, SchemarsJsonSchema)]
    struct Person {
        name: String,
        age: u32,
    }

    fn make() -> KeyValueSchema<StringSchema, JsonSchema<Person>> {
        KeyValueSchema::new(
            StringSchema::new(),
            JsonSchema::<Person>::new(),
            KeyValueEncodingType::Inline,
        )
    }

    #[test]
    fn schema_data_shape() {
        // Pulsar wire format: [u32 key_len BE][key bytes][u32 value_len BE][value bytes].
        // String has empty schema_data; Json carries a non-empty schema-document.
        let schema = make();
        let data = schema.schema_data();
        assert!(
            data.len() >= 8,
            "schema_data must be at least 8 header bytes"
        );
        let key_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        assert_eq!(key_len, 0, "String key schema has empty schema_data");
        let value_len_offset = 4 + key_len;
        let value_len = u32::from_be_bytes([
            data[value_len_offset],
            data[value_len_offset + 1],
            data[value_len_offset + 2],
            data[value_len_offset + 3],
        ]) as usize;
        let value_bytes = &data[value_len_offset + 4..value_len_offset + 4 + value_len];
        let value_schema_data = JsonSchema::<Person>::new().schema_data();
        assert_eq!(
            value_bytes,
            value_schema_data.as_ref(),
            "value-schema bytes must match the child schema's schema_data verbatim"
        );
        assert_eq!(schema.schema_type(), pb::schema::Type::KeyValue);
    }

    #[test]
    fn schema_properties_match_java_keys() {
        let schema = make();
        let props: std::collections::HashMap<String, String> =
            schema.properties().into_iter().collect();
        assert_eq!(props.get("key.schema.type"), Some(&"STRING".to_owned()));
        assert_eq!(props.get("value.schema.type"), Some(&"JSON".to_owned()));
        assert_eq!(props.get("kv.encoding.type"), Some(&"INLINE".to_owned()));
        assert!(props.contains_key("key.schema.name"));
        assert!(props.contains_key("value.schema.name"));
        assert!(props.contains_key("key.schema.properties"));
        assert!(props.contains_key("value.schema.properties"));
    }

    /// Minimal base64 decoder (standard alphabet, no padding tolerance) for tests only.
    // reason: unused today; kept for the KeyValue canonical-form follow-up alongside
    // `base64_encode`.
    #[allow(dead_code)]
    fn base64_decode(input: &str) -> Option<Vec<u8>> {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut lookup = [255u8; 256];
        for (i, &b) in ALPHABET.iter().enumerate() {
            lookup[b as usize] = u8::try_from(i).ok()?;
        }
        let bytes = input.as_bytes();
        if !bytes.len().is_multiple_of(4) {
            return None;
        }
        let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
        for chunk in bytes.chunks_exact(4) {
            let (a, b, c, d) = (chunk[0], chunk[1], chunk[2], chunk[3]);
            let av = lookup[a as usize];
            let bv = lookup[b as usize];
            if av == 255 || bv == 255 {
                return None;
            }
            out.push((av << 2) | (bv >> 4));
            if c != b'=' {
                let cv = lookup[c as usize];
                if cv == 255 {
                    return None;
                }
                out.push((bv << 4) | (cv >> 2));
                if d != b'=' {
                    let dv = lookup[d as usize];
                    if dv == 255 {
                        return None;
                    }
                    out.push((cv << 6) | dv);
                }
            }
        }
        Some(out)
    }

    #[test]
    fn inline_roundtrip() {
        let schema = make();
        let pair = KeyValuePair {
            key: "person-1".to_owned(),
            value: Person {
                name: "Ada".to_owned(),
                age: 36,
            },
        };
        let bytes = schema.encode(&pair).unwrap();
        let decoded = schema.decode(&bytes).unwrap();
        assert_eq!(decoded, pair);
    }

    #[test]
    fn separated_payload_is_value_only() {
        let schema = KeyValueSchema::new(
            StringSchema::new(),
            JsonSchema::<Person>::new(),
            KeyValueEncodingType::Separated,
        );
        let pair = KeyValuePair {
            key: "person-1".to_owned(),
            value: Person {
                name: "Ada".to_owned(),
                age: 36,
            },
        };
        let bytes = schema.encode(&pair).unwrap();
        // Bytes should match a direct JsonSchema encode of the value, since the key lands in
        // partition_key out-of-band.
        let raw = JsonSchema::<Person>::new().encode(&pair.value).unwrap();
        assert_eq!(bytes, raw);
    }

    #[test]
    fn separated_decode_requires_decode_with_key() {
        let schema = KeyValueSchema::new(
            StringSchema::new(),
            JsonSchema::<Person>::new(),
            KeyValueEncodingType::Separated,
        );
        let value_bytes = JsonSchema::<Person>::new()
            .encode(&Person {
                name: "Ada".to_owned(),
                age: 36,
            })
            .unwrap();
        // Direct decode in Separated mode is rejected.
        let err = schema.decode(&value_bytes).unwrap_err();
        assert!(matches!(err, SchemaError::Unsupported(_)));
        // But decode_with_key works.
        let pair = schema.decode_with_key(b"person-1", &value_bytes).unwrap();
        assert_eq!(pair.key, "person-1");
        assert_eq!(pair.value.name, "Ada");
    }

    #[test]
    fn base64_encode_known_vectors() {
        let mut buf = String::new();
        base64_encode(&mut buf, b"");
        assert_eq!(buf, "");

        buf.clear();
        base64_encode(&mut buf, b"f");
        assert_eq!(buf, "Zg==");

        buf.clear();
        base64_encode(&mut buf, b"fo");
        assert_eq!(buf, "Zm8=");

        buf.clear();
        base64_encode(&mut buf, b"foo");
        assert_eq!(buf, "Zm9v");

        buf.clear();
        base64_encode(&mut buf, b"foobar");
        assert_eq!(buf, "Zm9vYmFy");
    }

    /// V12: `base64_encode`'s tail match `match input.len() - i { 0 | 1 | 2 | _ =>
    /// unreachable!() }` was an invariant-#6 violation: `unreachable!()` panics on
    /// reach. The fix replaced it with a `debug_assert!(false, …)` plus a fall-
    /// through no-op (output stays a valid base64 prefix). Sweep every input
    /// length from 0 to 9 inclusive — covers all three tail residues (0, 1, 2 mod
    /// 3) without exercising the panic-pattern wildcard. No assertion of the
    /// exact output: we just need to make sure no length panics.
    #[test]
    fn base64_encode_does_not_panic_on_any_tail_residue() {
        let mut buf = String::new();
        for len in 0..=9 {
            buf.clear();
            let input: Vec<u8> = (0..len as u8).collect();
            base64_encode(&mut buf, &input);
            // Output length must be ⌈4 * len / 3⌉ rounded up to nearest 4 (with
            // `=` padding). For len=0 it is empty.
            if len == 0 {
                assert!(buf.is_empty(), "zero-length input produces empty output");
            } else {
                assert!(
                    buf.len().is_multiple_of(4),
                    "len={len} ⇒ output {buf:?} not 4-aligned"
                );
            }
        }
    }

    /// V3 strict: the wildcard arm of `base64_encode`'s tail match previously
    /// contained `debug_assert!(false, …)` — itself a panic path under invariant
    /// #6. Confirm the encoder stays panic-free even on a deliberately large
    /// input that loops through the main per-3 block hundreds of times before
    /// landing on each residue class.
    #[test]
    fn base64_encode_does_not_panic_on_large_input() {
        let mut buf = String::new();
        // Three lengths chosen to land on each `len % 3` residue (0, 1, 2)
        // after a high-iteration-count main loop, exercising the path the
        // removed `debug_assert!(false)` guarded against.
        for len in [4095usize, 4096, 4097] {
            buf.clear();
            let input: Vec<u8> = (0..len).map(|i| i as u8).collect();
            base64_encode(&mut buf, &input);
            assert!(
                buf.len().is_multiple_of(4),
                "len={len} ⇒ output not 4-aligned"
            );
        }
    }
}
