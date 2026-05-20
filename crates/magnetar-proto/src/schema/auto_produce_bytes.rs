// SPDX-License-Identifier: Apache-2.0

//! `AutoProduceBytes` schema (Java parity stub).
//!
//! Mirrors `AutoProduceBytesSchema.java`. The broker accepts opaque bytes and
//! does no schema validation. Useful when the producer wants to publish
//! pre-encoded payloads matching an existing topic schema. This milestone (M5)
//! ships the trait surface; richer validation hooks land later.

use bytes::Bytes;

use super::{Schema, SchemaError};
use crate::pb;

/// Marker schema that lets the producer publish opaque bytes without
/// broker-side validation.
#[derive(Debug, Default, Clone)]
pub struct AutoProduceBytesSchema {
    _private: (),
}

impl AutoProduceBytesSchema {
    /// Construct an `AutoProduceBytes` schema marker.
    #[must_use]
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Schema for AutoProduceBytesSchema {
    type Owned = Bytes;

    fn schema_type(&self) -> pb::schema::Type {
        // Pulsar's proto stops at AutoConsume (21); AutoProduceBytes does not
        // have its own enum variant — Java's `AutoProduceBytesSchema` sends
        // `None` because the broker performs no validation. Mirror that.
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
