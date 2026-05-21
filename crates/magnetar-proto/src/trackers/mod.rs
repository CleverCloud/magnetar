// SPDX-License-Identifier: Apache-2.0

//! Per-consumer trackers — ack grouping, negative-ack retry, unacked-message timeout.
//!
//! Each tracker is a single-purpose, tick-driven state machine. The consumer pumps them on
//! every [`Connection::poll_timeout`](crate::Connection::poll_timeout) / `handle_timeout` cycle
//! and emits the resulting commands on the [`Connection`](crate::Connection) outbound queue.
//!
//! The trackers themselves know nothing about the rest of the connection; they take inputs via
//! `add(...)` / `remove(...)` and produce outputs via `poll(now) -> Vec<_>` so they are trivial
//! to unit-test in isolation.

pub mod ack;
pub mod nack;
pub mod unacked;

pub use ack::{AckAction, AckGroupingTracker};
pub use nack::{MultiplierRedeliveryBackoff, NackAction, NegativeAcksTracker};
pub use unacked::{UnackedAction, UnackedMessageTracker};
