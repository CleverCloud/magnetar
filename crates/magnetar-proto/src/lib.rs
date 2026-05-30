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
//! [`GUIDELINES.md`]: https://github.com/CleverCloud/magnetar/blob/main/GUIDELINES.md
//! [ADR-0003]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0003-no-channels-rule.md
//! [ADR-0004]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0004-sans-io-protocol-core.md
//! [ADR-0011]: https://github.com/CleverCloud/magnetar/blob/main/specs/adr/0011-clock-injection-sans-io.md

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]
// Connection / producer / consumer dispatch loops are long by nature; the cosmetic lints below
// are silenced so real bugs surface instead. `dead_code` is intentionally NOT allowed
// crate-wide — scope any unavoidable dead surface to the offending item with
// `#[allow(dead_code)] // reason:` so future drift gets caught by the workspace lint.
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
    clippy::match_wildcard_for_single_variants
)]

/// Pulsar wire-protocol version this driver advertises in
/// `CommandConnect.protocol_version`. Currently `21` — the level
/// negotiated by Pulsar 4.x brokers; covers PIP-188 `TOPIC_MIGRATED`,
/// PIP-145 `CommandWatchTopicList`, the PIP-31 transactional family,
/// and the rest of the v0.1.0 parity surface.
///
/// Exposed so the CLI banner (`magnetar --version`) and any external
/// tooling read the same number the wire driver sends, removing the
/// drift risk of two parallel literals.
pub const SUPPORTED_PROTOCOL_VERSION: i32 = 21;

/// Pulsar wire-protocol version a `scalable-topics`-enabled client
/// advertises in `CommandConnect.protocol_version` (PIP-460, ADR-0031).
///
/// **Experimental.** One past the v4 ceiling
/// ([`SUPPORTED_PROTOCOL_VERSION`]). Only advertised when the
/// `scalable-topics` feature is on; a client compiled without the feature
/// caps at [`SUPPORTED_PROTOCOL_VERSION`] and stays Pulsar-4.0+ compatible.
/// The number is the proposal's best-effort guess for the upstream
/// assignment and will be reconciled by the Pulsar 5.0 RC vendor bump.
#[cfg(feature = "scalable-topics")]
pub const SUPPORTED_PROTOCOL_VERSION_SCALABLE_TOPICS: i32 =
    crate::pb::scalable_topics::PROTOCOL_VERSION_SCALABLE_TOPICS;

pub mod anti_thrash;
pub mod auth;
pub mod backoff;
pub mod cluster_failover;
pub mod conn;
pub(crate) mod conn_types;
pub mod consumer;
pub mod crypto;
/// PIP-460 segment-DAG-watch session state machine (experimental, ADR-0031).
#[cfg(feature = "scalable-topics")]
pub mod dag_watch;
pub mod error;
pub mod event;
pub mod frame;
pub mod health_probe;
pub mod lookup;
pub mod markers;
pub mod producer;
pub mod schema;
pub mod service_url;
pub mod supervisor;
pub mod topic_watcher;
pub mod trackers;
pub mod transmit;
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

    /// PIP-460 (scalable topics) hand-encoded wire commands.
    ///
    /// **Experimental** (PIP-460, ADR-0031). Default OFF. PIP-460 introduces
    /// three new wire commands (`CommandScalableTopicLookup`,
    /// `CommandSegmentDagWatch`, `CommandSegmentDagUpdate`) plus their
    /// responses, an extended `MessageIdData.segment_id` field, and six new
    /// `BaseCommand.Type` discriminators (80-85). **None of these are in the
    /// vendored `PulsarApi.proto`** — upstream PIP-460 is `Draft` and no
    /// Pulsar broker ships the wire surface today.
    ///
    /// Until upstream cuts a Pulsar 5.0 RC including PIP-460, these commands
    /// are hand-maintained `#[derive(prost::Message)]` structs rather than
    /// vendored into `pulsar.proto.rs`; the authoritative proto bump lands
    /// when upstream tags the RC (`cargo run -p xtask -- vendor-proto` as a
    /// dedicated commit per ADR-0026 §D4). Commands ride the standard Pulsar
    /// command-only frame via the hand-built
    /// [`scalable_topics::ScalableBaseCommand`] envelope (shared `type`
    /// field-1 tag), so a v4 peer skips the new additive fields.
    #[cfg(feature = "scalable-topics")]
    pub mod scalable_topics {
        include!("pb/scalable_topics.rs");
    }
}

pub use crate::anti_thrash::{
    AntiThrashDisposition, AntiThrashState, AntiThrashThreshold, AttachOutcome, ReAttachHandle,
    ReAttachOutcomeKind,
};
pub use crate::auth::{
    AuthChallengeState, AuthError, AuthProvider, TlsAuth, TokenAuth, TokenSupplier,
};
pub use crate::backoff::Backoff;
pub use crate::cluster_failover::ControlledClusterFailover;
pub use crate::conn::{
    AckRequest, Connection, ConnectionConfig, CreateProducerRequest, CryptoFailureAction,
    HandshakeState, KeySharedConfig, MemoryLimitPolicy, OpOutcome, PendingOpKey, SeekTarget,
    SubscribeRequest,
};
pub use crate::consumer::{ConsumerIdentity, ConsumerSlot, ConsumerStats, ShadowTopicMetadata};
pub use crate::crypto::{EncryptError, MessageDecryptor, MessageEncryptor};
#[cfg(feature = "scalable-topics")]
pub use crate::dag_watch::{
    DagChangeReason, DagDelta, DagError, DagWatchSession, MergeEvent, SplitEvent,
};
pub use crate::error::{ConsumerError, ProducerError, ProtocolError};
pub use crate::event::{ConnectionEvent, GetSchemaResult, IncomingMessage, LookupOutcome};
pub use crate::frame::{
    Frame, FrameError, MAGIC_BROKER_ENTRY_METADATA, MAGIC_CRC32C, MAX_FRAME_SIZE, Payload,
    decode_one, encode_command, encode_payload,
};
pub use crate::health_probe::HealthProbe;
pub use crate::markers::{
    ClusterMessageId, MarkerDecodeError, MarkersMessageIdData, ReplicatedSubscriptionMarker,
    ReplicatedSubscriptionMarkerDetails, ReplicatedSubscriptionMarkerKind,
    decode_replicated_subscription_marker,
};
pub use crate::producer::{ProducerIdentity, ProducerSlot, ProducerStats};
pub use crate::service_url::{
    ServiceUrlProvider, StaticServiceUrlProvider, static_service_url_provider,
};
pub use crate::supervisor::SupervisorConfig;
pub use crate::transmit::{Transmit, TransmitOwned};
pub use crate::txn::{TransactionMetadata, TxnAction, TxnClient, TxnError, TxnId, TxnState};
pub use crate::types::{ConsumerHandle, MessageId, ProducerHandle, RequestId, SequenceId};
#[cfg(feature = "scalable-topics")]
pub use crate::types::{KeyRange, SegmentDescriptor, SegmentId, SegmentState};
