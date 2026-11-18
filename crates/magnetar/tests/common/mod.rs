// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for the façade's integration tests.
//!
//! Each `e2e_*.rs` file is its own test binary and declares `mod common;`.
//! [`pulsar`] is the one place that boots an `apachepulsar/pulsar` broker
//! (ADR-0115), and [`container`] runs every Docker container the suites
//! start, through the `docker` command line (ADR-0127).

// Every test binary compiles this module but uses only part of it, and an
// integration test has no way to share a `pub(crate)` item across binaries:
// the layout requires `pub` items here (the convention of
// `magnetar-runtime-moonpool/tests/common/mod.rs`).
#![allow(dead_code, unreachable_pub)]

pub mod container;
pub mod pulsar;
