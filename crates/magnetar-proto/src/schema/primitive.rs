// SPDX-License-Identifier: Apache-2.0

//! Primitive value schemas — Java parity for
//! `BooleanSchema` / `ByteSchema` / `ShortSchema` / `IntSchema` / `LongSchema` /
//! `FloatSchema` / `DoubleSchema`.
//!
//! Every primitive encodes in big-endian (Java network byte order) and the
//! [`Schema::schema_data`] is empty — the broker identifies the type from the
//! `schema_type()` discriminator alone. Mirrors the Java
//! `org.apache.pulsar.client.impl.schema.LongSchema` family.

use bytes::Bytes;

use super::{Schema, SchemaError};
use crate::pb;

macro_rules! primitive_schema {
    ($name:ident, $owned:ty, $pb_type:expr, $size:expr) => {
        #[doc = concat!("Schema for `", stringify!($owned), "` values encoded big-endian.")]
        #[derive(Debug, Clone, Copy, Default)]
        pub struct $name;

        impl $name {
            /// Construct.
            pub const fn new() -> Self {
                Self
            }
        }

        impl Schema for $name {
            type Owned = $owned;

            fn schema_type(&self) -> pb::schema::Type {
                $pb_type
            }

            fn schema_data(&self) -> Bytes {
                Bytes::new()
            }

            fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError> {
                Ok(Bytes::copy_from_slice(&value.to_be_bytes()))
            }

            fn decode(&self, bytes: &[u8]) -> Result<Self::Owned, SchemaError> {
                if bytes.len() != $size {
                    return Err(SchemaError::Mismatch {
                        expected: format!("{}-byte big-endian {}", $size, stringify!($owned)),
                        actual: format!("{}-byte payload", bytes.len()),
                    });
                }
                let mut buf = [0u8; $size];
                buf.copy_from_slice(bytes);
                Ok(<$owned>::from_be_bytes(buf))
            }
        }
    };
}

primitive_schema!(Int8Schema, i8, pb::schema::Type::Int8, 1);
primitive_schema!(Int16Schema, i16, pb::schema::Type::Int16, 2);
primitive_schema!(Int32Schema, i32, pb::schema::Type::Int32, 4);
primitive_schema!(Int64Schema, i64, pb::schema::Type::Int64, 8);
primitive_schema!(FloatSchema, f32, pb::schema::Type::Float, 4);
primitive_schema!(DoubleSchema, f64, pb::schema::Type::Double, 8);

/// Boolean schema. Encodes as a single 0x00 / 0x01 byte. Mirrors Java `BooleanSchema`.
#[derive(Debug, Clone, Copy, Default)]
pub struct BoolSchema;

impl BoolSchema {
    /// Construct.
    pub const fn new() -> Self {
        Self
    }
}

impl Schema for BoolSchema {
    type Owned = bool;

    fn schema_type(&self) -> pb::schema::Type {
        pb::schema::Type::Bool
    }

    fn schema_data(&self) -> Bytes {
        Bytes::new()
    }

    fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError> {
        Ok(Bytes::from_static(if *value { &[1] } else { &[0] }))
    }

    fn decode(&self, bytes: &[u8]) -> Result<Self::Owned, SchemaError> {
        if bytes.len() != 1 {
            return Err(SchemaError::Mismatch {
                expected: "1-byte boolean".to_owned(),
                actual: format!("{}-byte payload", bytes.len()),
            });
        }
        Ok(bytes[0] != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int32_roundtrip() {
        let s = Int32Schema::new();
        for v in [0_i32, 1, -1, i32::MAX, i32::MIN, 42] {
            let bytes = s.encode(&v).unwrap();
            assert_eq!(bytes.len(), 4);
            let back = s.decode(&bytes).unwrap();
            assert_eq!(back, v);
        }
    }

    #[test]
    fn int64_roundtrip() {
        let s = Int64Schema::new();
        for v in [0_i64, 1, -1, i64::MAX, i64::MIN, 4_200_000_000] {
            let bytes = s.encode(&v).unwrap();
            assert_eq!(bytes.len(), 8);
            let back = s.decode(&bytes).unwrap();
            assert_eq!(back, v);
        }
    }

    #[test]
    fn float64_roundtrip() {
        let s = DoubleSchema::new();
        for v in [
            0.0_f64,
            1.5,
            -1.5,
            f64::MAX,
            f64::MIN_POSITIVE,
            std::f64::consts::PI,
        ] {
            let bytes = s.encode(&v).unwrap();
            let back = s.decode(&bytes).unwrap();
            assert_eq!(back.to_bits(), v.to_bits());
        }
    }

    #[test]
    fn bool_roundtrip() {
        let s = BoolSchema::new();
        assert!(s.decode(&s.encode(&true).unwrap()).unwrap());
        assert!(!s.decode(&s.encode(&false).unwrap()).unwrap());
    }

    #[test]
    fn wrong_size_rejected() {
        let s = Int32Schema::new();
        assert!(s.decode(&[0_u8, 0, 0]).is_err());
        assert!(s.decode(&[0_u8, 0, 0, 0, 0]).is_err());
    }
}
