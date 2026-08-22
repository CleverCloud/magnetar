// SPDX-License-Identifier: Apache-2.0

//! Datum round-trip through the apache-avro 0.22 builder-based
//! `GenericDatumWriter`/`GenericDatumReader` paths in `AvroSchema::encode`
//! and `AvroSchema::decode`, which replaced the deprecated free
//! `to_avro_datum`/`from_avro_datum` functions. Lives in this crate so the
//! ADR-0024 sim-coverage gate executes the migrated lines.

use magnetar_proto::schema::{AvroSchema, Schema};

#[test]
fn avro_datum_roundtrip_through_builders() {
    let schema = AvroSchema::<String>::parse_str(r#""string""#).expect("valid avro schema");
    let encoded = schema.encode(&"pulsar".to_owned()).expect("encode");
    let decoded = schema.decode(&encoded).expect("decode");
    assert_eq!(decoded, "pulsar");
}
