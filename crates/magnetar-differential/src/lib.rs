// SPDX-License-Identifier: Apache-2.0

//! Tokio ↔ moonpool differential equivalence harness for magnetar.
//!
//! Per ADR-0019 (Moonpool parity train), M8: the harness takes a
//! producer/consumer [`Trace`] (a sequence of operations) and runs it
//! against BOTH engines:
//!
//! - the tokio engine ([`magnetar_runtime_tokio`]) against a scripted in-process broker bound to
//!   `127.0.0.1`,
//! - the moonpool engine ([`magnetar_runtime_moonpool`]) with [`moonpool_core::TokioProviders`]
//!   against the same scripted broker,
//!
//! then compares user-visible [`EventStream`]s for equivalence.
//!
//! The scripted broker (see [`broker`]) speaks a deliberately minimal
//! subset of the Pulsar wire protocol — `CONNECT`/`CONNECTED`,
//! `PRODUCER`/`PRODUCER_SUCCESS`, `SEND`/`SEND_RECEIPT`,
//! `SUBSCRIBE`/`SUCCESS`, pushed `MESSAGE`s, `ACK`/`ACK_RESPONSE`,
//! `SEEK`/`SUCCESS`, and `CLOSE_PRODUCER`/`CLOSE_CONSUMER`. It is enough
//! to drive the four golden traces shipped alongside the harness; new
//! traces extend the broker as needed.
//!
//! ## Why the differential Moonpool leg uses `TokioProviders`
//!
//! Both differential legs talk to the same real in-process broker on an ambient Tokio runtime.
//! `TokioProviders` therefore isolates engine-surface differences while keeping the broker,
//! network, and wall-clock environment shared.
//! The Moonpool runtime's native deterministic-executor contract is exercised separately by the
//! `magnetar-runtime-moonpool` `SimProviders` chaos suite.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]
#![allow(
    // The harness deliberately matches the engines' surface ergonomics,
    // not pedantic API perfection.
    clippy::module_name_repetitions,
    clippy::too_many_lines
)]

use std::time::Duration;

pub mod broker;
pub mod runner_moonpool;
pub mod runner_tokio;
pub mod trace;

pub use crate::trace::{Event, EventStream, Op, Trace};

/// Wall-clock anti-hang backstop for the differential equivalence runners.
///
/// The differential harness runs BOTH engine legs on the real tokio clock
/// (`TokioProviders`; see the module docs above), so every `tokio::time::timeout`
/// guarding a runner leg is a real wall-clock guard. This constant is an
/// **anti-hang backstop, not a timing assertion**: a leg that genuinely wedges
/// still fails the suite — just later, never silently. It is sized generously so
/// host-scheduling jitter under CI oversubscription cannot trip it.
///
/// Issue #286: the old per-test 5 s / 30 s guards fired on pure scheduling
/// latency (the connect → lookup → producer-open and supervised-redial
/// sequences are starved past a tight guard when the runner is oversubscribed),
/// never on a real hang — a 1000+-run local stress sweep produced zero
/// forever-wedges. Unifying them behind one generous constant kills that flake
/// class while keeping a finite deadlock backstop. Do NOT re-tighten this to
/// "make the tests faster".
pub const HANG_GUARD: Duration = Duration::from_mins(1);
