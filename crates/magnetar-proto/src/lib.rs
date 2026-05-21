// SPDX-License-Identifier: Apache-2.0

//! Sans-io Apache Pulsar wire protocol — Rust implementation.
//!
//! `magnetar-proto` is the protocol heart of the magnetar workspace. It contains the wire codec
//! (framing + CRC32C + protobuf), the connection state machine, per-producer / per-consumer state
//! machines, lookup, topic-list watcher, acknowledgement trackers, and backoff. The crate has
//! **zero I/O dependencies** and **zero channels** — see [`GUIDELINES.md`] for the rationale and
//! the `Arc<Mutex<…>> + Notify + Waker-slab` pattern that engines (`magnetar-runtime-tokio`,
//! `magnetar-runtime-moonpool`) use to drive it. The architectural choice is recorded as
//! [ADR-0003] (no channels) + [ADR-0004] (sans-io split) + [ADR-0011] (clock injection).
//!
//! # API shape
//!
//! [`Connection`] follows the `quinn-proto` sans-io shape: feed bytes in via
//! [`Connection::handle_bytes`], pull bytes out via [`Connection::poll_transmit`], observe events
//! via [`Connection::poll_event`], drive timers via [`Connection::poll_timeout`] and
//! [`Connection::handle_timeout`]. A typed-handle façade ([`Connection::create_producer`],
//! [`Connection::subscribe`], [`Connection::send`], [`Connection::ack`], …) lets callers operate
//! the protocol without touching raw [`pb::BaseCommand`] frames.
//!
//! [`GUIDELINES.md`]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/GUIDELINES.md
//! [ADR-0003]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0003-no-channels-rule.md
//! [ADR-0004]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md
//! [ADR-0011]: https://github.com/FlorentinDUBOIS/magnetar/blob/main/specs/adr/0011-clock-injection-sans-io.md

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]
// M2 scaffolding: connection / producer / consumer dispatch loops are long by nature; we'll
// refactor and tighten as M3/M5/M7 wire up. Until then, keep the cosmetic lints quiet so the
// real bugs surface.
#![allow(
    clippy::too_many_lines,
    clippy::match_same_arms,
    clippy::large_enum_variant,
    clippy::should_implement_trait,
    clippy::redundant_closure,
    clippy::needless_pass_by_value,
    clippy::manual_let_else,
    clippy::needless_continue,
    clippy::similar_names,
    clippy::if_same_then_else,
    clippy::items_after_statements,
    clippy::redundant_pattern_matching,
    clippy::single_match_else,
    clippy::implicit_hasher,
    clippy::needless_collect,
    clippy::unnecessary_wraps,
    clippy::option_if_let_else,
    clippy::unused_self,
    clippy::map_unwrap_or,
    clippy::clone_on_copy,
    clippy::doc_markdown,
    clippy::needless_borrow,
    clippy::cast_lossless,
    clippy::default_trait_access,
    clippy::derive_partial_eq_without_eq,
    clippy::missing_const_for_fn,
    clippy::field_reassign_with_default,
    clippy::assigning_clones,
    clippy::match_wildcard_for_single_variants,
    dead_code
)]

pub mod auth;
pub mod backoff;
pub mod cluster_failover;
pub mod conn;
pub mod consumer;
pub mod error;
pub mod event;
pub mod frame;
pub mod lookup;
pub mod producer;
pub mod schema;
pub mod service_url;
pub mod supervisor;
pub mod topic_watcher;
pub mod trackers;
pub mod txn;
pub mod types;

/// Protobuf-generated Pulsar wire types.
///
/// Regenerate via `cargo run -p xtask -- codegen`. The contents of this module are committed to
/// the repository so consumers do not need `protoc` available at build time.
#[allow(
    clippy::all,
    clippy::pedantic,
    unreachable_pub,
    missing_debug_implementations
)]
pub mod pb {
    include!("pb/pulsar.proto.rs");
}

pub use crate::auth::{AuthChallengeState, AuthError, AuthProvider, TlsAuth, TokenAuth};
pub use crate::backoff::Backoff;
pub use crate::cluster_failover::ControlledClusterFailover;
pub use crate::conn::{
    AckRequest, Connection, ConnectionConfig, CreateProducerRequest, CryptoFailureAction,
    HandshakeState, KeySharedConfig, MemoryLimitPolicy, OpOutcome, PendingOpKey, SeekTarget,
    SubscribeRequest,
};
pub use crate::consumer::ConsumerStats;
pub use crate::error::{ConsumerError, ProducerError, ProtocolError};
pub use crate::event::{ConnectionEvent, GetSchemaResult, IncomingMessage};
pub use crate::frame::{
    Frame, FrameError, MAGIC_BROKER_ENTRY_METADATA, MAGIC_CRC32C, MAX_FRAME_SIZE, Payload,
    decode_one, encode_command, encode_payload,
};
pub use crate::producer::ProducerStats;
pub use crate::service_url::{
    ServiceUrlProvider, StaticServiceUrlProvider, static_service_url_provider,
};
pub use crate::supervisor::SupervisorConfig;
pub use crate::txn::{TransactionMetadata, TxnAction, TxnClient, TxnError, TxnId, TxnState};
pub use crate::types::{ConsumerHandle, MessageId, ProducerHandle, RequestId, SequenceId};
