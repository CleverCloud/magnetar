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
//! ## Why both engines run on `TokioProviders`
//!
//! `moonpool-sim` (the deterministic-chaos provider bundle) is not yet
//! a workspace dependency — only [`moonpool_core`] is. With
//! `TokioProviders` plugged in, the moonpool engine still drives the
//! same façade types (`Client<P>`, `Producer<P>`, `Consumer<P>`) the
//! sim path uses, so the harness exercises the engine surface that
//! diverges between tokio and moonpool (memory-limit policies, future
//! shapes, `_providers: PhantomData` plumbing, generic bounds, …).
//! Swapping in a real sim bundle is a one-line change once we vendor
//! it.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]
#![allow(
    // The harness deliberately matches the engines' surface ergonomics,
    // not pedantic API perfection.
    clippy::module_name_repetitions,
    clippy::too_many_lines
)]

pub mod broker;
pub mod runner_moonpool;
pub mod runner_tokio;
pub mod trace;

pub use crate::trace::{Event, EventStream, Op, Trace};
