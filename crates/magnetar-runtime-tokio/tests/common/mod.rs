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

use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{encode_command, pb};

/// Wall-clock anti-hang backstop shared by every tokio integration test.
///
/// These tests drive real mock brokers over real TCP on the multi-thread tokio
/// runtime, so any `tokio::time::timeout` guarding an operation that is expected
/// to COMPLETE (`connect`, `open_producer`, `receive`, `close`, driver `join`,
/// …) is a real wall-clock guard. This constant is an **anti-hang backstop, not
/// a timing assertion**: an operation that genuinely wedges still fails the test
/// — just later, never silently. It is sized generously so host-scheduling
/// jitter under CI oversubscription cannot trip it.
///
/// Issue #295: tight per-step bounds (`Duration::from_secs(5)` and friends)
/// fired on pure scheduling latency under a saturated CI runner — green
/// locally, `Elapsed(())` on CI — never on a real hang. Unifying every backstop
/// behind one generous constant kills that flake class while keeping a finite
/// deadlock guard (ADR-0021: de-flake, never `#[ignore]`). Mirrors
/// `magnetar_differential::HANG_GUARD` (issue #286).
///
/// This is ONLY for backstops. Timeouts whose *firing* is the behaviour under
/// test — negative assertions ("nothing arrives within X", usually
/// `from_millis`), fail-fast / regression-detection bounds (`connect_resilience`),
/// and deliberately-tight fast-path bounds (`from_secs(2)`) — keep their own
/// short, intentional durations. Do NOT re-tighten this to "make the tests
/// faster".
pub const HANG_GUARD: Duration = Duration::from_secs(60);

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
