// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for tokio integration tests. Mirrors
//! `crates/magnetar-runtime-moonpool/tests/common/mod.rs`'s
//! `handshake_response_bytes`, which had drifted into 3+ separate
//! per-file copies on the tokio side.
//!
//! Each tokio integration-test file lives in its own binary, so a
//! `pub` helper in this module is "unreachable" from the perspective
//! of any single test binary — the integration-test layout *requires*
//! `pub` items in `tests/common/mod.rs` (rustc has no notion of a
//! "shared test helper" crate).

#![allow(dead_code, unreachable_pub)]

use bytes::BytesMut;
use magnetar_proto::{encode_command, pb};

/// Build a synthetic `CommandConnected` frame matching the production
/// engine's expectations. Mirrors the moonpool-side helper so the two
/// runtimes stay in lockstep when the handshake shape changes.
pub fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandConnected");
    buf
}
