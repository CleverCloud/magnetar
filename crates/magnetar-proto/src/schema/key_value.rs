// SPDX-License-Identifier: Apache-2.0

//! Composite key/value schema.
//!
//! Mirrors `org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl`. A `KeyValueSchema<K, V>`
//! composes two child schemas — one for the key, one for the value — into a single Pulsar schema
//! whose `schema_data` is a small JSON document that the broker stores **verbatim** and compares
//! by raw-byte equality (Codex Q4).
//!
//! # Wire shape of `schema_data`
//!
//! The Java client emits, in this exact key order:
//!
//! ```json
//! {
//!   "key": {"name": "<keyName>", "type": "<KeyType>", "schema": "<base64-schema-data>",
//!           "properties": {...}},
//!   "value": {"name": "<valueName>", "type": "<ValueType>", "schema": "<base64-schema-data>",
//!             "properties": {...}},
//!   "type": "Separated" | "Inline"
//! }
//! ```
//!
//! Magnetar must emit the same field order and value formatting or the broker will create a
//! fresh schema version every (re)connect.
//!
//! # Encoding mode
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
        match self {
            Self::Separated => "Separated",
            Self::Inline => "Inline",
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

/// Render the broker-side `schema_data` JSON document.
///
/// The field order matches the Java client (`KeyValueSchemaInfo.java::encodeKeyValueSchemaInfo`):
/// `key` first, then `value`, then `type`. The schema-data payload for each child is base64-
/// encoded (the Java client uses `Base64.getEncoder().encodeToString`). This keeps the document
/// pure ASCII and avoids escaping issues with binary AVRO / FDS bytes.
fn build_schema_data(
    key_type: pb::schema::Type,
    key_data: &[u8],
    value_type: pb::schema::Type,
    value_data: &[u8],
    encoding: KeyValueEncodingType,
) -> Bytes {
    let mut out = String::new();
    out.push_str("{\"key\":");
    render_child(&mut out, key_type, key_data);
    out.push_str(",\"value\":");
    render_child(&mut out, value_type, value_data);
    out.push_str(",\"type\":\"");
    out.push_str(encoding.as_str());
    out.push_str("\"}");
    Bytes::from(out.into_bytes())
}

fn render_child(out: &mut String, ty: pb::schema::Type, data: &[u8]) {
    // Emit `{"type":"<Type>","schema":"<base64>"}` — the minimum the Java reader expects. We
    // intentionally exclude `properties` here because the Java emitter omits it when the map is
    // empty (the broker accepts either shape, but Magnetar should mirror the empty-map default).
    out.push_str("{\"type\":\"");
    out.push_str(ty.as_str_name());
    out.push_str("\",\"schema\":\"");
    base64_encode(out, data);
    out.push_str("\"}");
}

/// Standard base64 encoder (RFC 4648 alphabet, with `=` padding). Inlined to avoid pulling
/// `base64` into the magnetar-proto dep graph — schema canonicalisation is the only consumer.
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
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::schema::{JsonSchema, StringSchema};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        let schema = make();
        let data = schema.schema_data();
        let s = std::str::from_utf8(&data).unwrap();
        // Field order: key, value, type. The String child has empty schema_data ("").
        assert!(s.starts_with(
            r#"{"key":{"type":"String","schema":""},"value":{"type":"Json","schema":""#
        ));
        assert!(s.ends_with(r#""},"type":"Inline"}"#));
        assert_eq!(schema.schema_type(), pb::schema::Type::KeyValue);
    }

    #[test]
    fn base64_round_trip_marker() {
        // The Json child carries `{}` as its schema_data. Base64("{}") = "e30=".
        let schema = make();
        let data = schema.schema_data();
        let s = std::str::from_utf8(&data).unwrap();
        assert!(s.contains(r#""value":{"type":"Json","schema":"e30="}"#));
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
}
