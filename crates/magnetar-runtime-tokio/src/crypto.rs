// SPDX-License-Identifier: Apache-2.0

//! Runtime-level encryption hook traits.
//!
//! The canonical trait surface lives in [`magnetar_proto::crypto`]; this module is a thin
//! re-export so existing import paths (`magnetar_runtime_tokio::crypto::MessageEncryptor`, etc.)
//! keep working after the de-duplication. See [`magnetar_proto::crypto`] for the design notes.

pub use magnetar_proto::crypto::{EncryptError, MessageDecryptor, MessageEncryptor};
