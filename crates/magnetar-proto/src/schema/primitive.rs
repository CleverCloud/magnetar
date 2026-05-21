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

// Date / time schemas. Java's chrono-flavoured schemas all hit the wire as i64 big-endian
// with a distinct schema_type discriminator so the broker stores the semantic intent. We
// stay chrono-free: callers convert their own date/time types to/from i64 and pick the
// schema that matches the field's semantics.
//
// - Date: epoch millis (java.util.Date)
// - Time: millis since midnight (java.sql.Time)
// - Timestamp: epoch millis (java.sql.Timestamp)
// - LocalDate: epoch day (java.time.LocalDate#toEpochDay)
// - LocalTime: nanos since midnight (java.time.LocalTime#toNanoOfDay)
primitive_schema!(DateSchema, i64, pb::schema::Type::Date, 8);
primitive_schema!(TimeSchema, i64, pb::schema::Type::Time, 8);
primitive_schema!(TimestampSchema, i64, pb::schema::Type::Timestamp, 8);
primitive_schema!(LocalDateSchema, i64, pb::schema::Type::LocalDate, 8);
primitive_schema!(LocalTimeSchema, i64, pb::schema::Type::LocalTime, 8);

// Instant / LocalDateTime schemas. Java encodes both as 12 bytes: i64 epoch seconds
// big-endian followed by i32 nanos-of-second big-endian. The pair maps directly to
// `std::time::SystemTime` or `chrono::DateTime` once converted, but magnetar-proto stays
// chrono-free — callers expose their own type and convert.

macro_rules! seconds_nanos_schema {
    ($name:ident, $pb_type:expr, $java_name:literal) => {
        #[doc = concat!(
                    "Schema for `(i64 epoch seconds, i32 nanos-of-second)` values encoded as ",
                    "12 bytes big-endian. Mirrors Java `",
                    $java_name,
                    "`."
                )]
        #[derive(Debug, Clone, Copy, Default)]
        pub struct $name;

        impl $name {
            /// Construct.
            #[must_use]
            pub const fn new() -> Self {
                Self
            }
        }

        impl Schema for $name {
            type Owned = (i64, i32);

            fn schema_type(&self) -> pb::schema::Type {
                $pb_type
            }

            fn schema_data(&self) -> Bytes {
                Bytes::new()
            }

            fn encode(&self, value: &Self::Owned) -> Result<Bytes, SchemaError> {
                let (secs, nanos) = *value;
                let mut buf = [0u8; 12];
                buf[..8].copy_from_slice(&secs.to_be_bytes());
                buf[8..].copy_from_slice(&nanos.to_be_bytes());
                Ok(Bytes::copy_from_slice(&buf))
            }

            fn decode(&self, bytes: &[u8]) -> Result<Self::Owned, SchemaError> {
                if bytes.len() != 12 {
                    return Err(SchemaError::Mismatch {
                        expected: "12-byte big-endian (i64 seconds, i32 nanos)".to_owned(),
                        actual: format!("{}-byte payload", bytes.len()),
                    });
                }
                let mut secs_buf = [0u8; 8];
                let mut nanos_buf = [0u8; 4];
                secs_buf.copy_from_slice(&bytes[..8]);
                nanos_buf.copy_from_slice(&bytes[8..]);
                Ok((i64::from_be_bytes(secs_buf), i32::from_be_bytes(nanos_buf)))
            }
        }
    };
}

seconds_nanos_schema!(InstantSchema, pb::schema::Type::Instant, "InstantSchema");
seconds_nanos_schema!(
    LocalDateTimeSchema,
    pb::schema::Type::LocalDateTime,
    "LocalDateTimeSchema"
);

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

    #[test]
    fn instant_roundtrip() {
        let s = InstantSchema::new();
        for v in [
            (0_i64, 0_i32),
            (1, 1),
            (-1, 999_999_999),
            (1_700_000_000, 500_000_000),
            (i64::MAX, i32::MAX),
            (i64::MIN, 0),
        ] {
            let bytes = s.encode(&v).unwrap();
            assert_eq!(bytes.len(), 12);
            assert_eq!(s.decode(&bytes).unwrap(), v);
        }
        // Wrong size is rejected.
        assert!(s.decode(&[0u8; 11]).is_err());
        assert!(s.decode(&[0u8; 13]).is_err());
    }

    #[test]
    fn instant_and_local_date_time_share_layout_with_distinct_types() {
        let v = (1_234_567_890_i64, 42_i32);
        let instant = InstantSchema::new();
        let ldt = LocalDateTimeSchema::new();
        assert_eq!(instant.encode(&v).unwrap(), ldt.encode(&v).unwrap());
        assert_eq!(instant.schema_type(), pb::schema::Type::Instant);
        assert_eq!(ldt.schema_type(), pb::schema::Type::LocalDateTime);
    }

    #[test]
    fn time_schemas_share_int64_encoding_with_distinct_types() {
        // Same bytes, different semantic discriminators — the broker decides intent
        // from schema_type, not payload shape.
        let v: i64 = 1_700_000_000_000;
        let date = DateSchema::new();
        let time = TimeSchema::new();
        let ts = TimestampSchema::new();
        let ld = LocalDateSchema::new();
        let lt = LocalTimeSchema::new();
        assert_eq!(date.encode(&v).unwrap(), time.encode(&v).unwrap());
        assert_eq!(date.encode(&v).unwrap(), ts.encode(&v).unwrap());
        assert_eq!(date.encode(&v).unwrap(), ld.encode(&v).unwrap());
        assert_eq!(date.encode(&v).unwrap(), lt.encode(&v).unwrap());

        assert_eq!(date.schema_type(), pb::schema::Type::Date);
        assert_eq!(time.schema_type(), pb::schema::Type::Time);
        assert_eq!(ts.schema_type(), pb::schema::Type::Timestamp);
        assert_eq!(ld.schema_type(), pb::schema::Type::LocalDate);
        assert_eq!(lt.schema_type(), pb::schema::Type::LocalTime);

        assert_eq!(date.decode(&date.encode(&v).unwrap()).unwrap(), v);
        assert_eq!(lt.decode(&lt.encode(&v).unwrap()).unwrap(), v);
    }
}
