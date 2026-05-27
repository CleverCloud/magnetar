// SPDX-License-Identifier: Apache-2.0

//! **Experimental** — PIP-466 V5 client surface (ADR-0032).
//!
//! Thin skin over the v4 magnetar surface. The wire protocol is
//! unchanged: every `v5::Producer` / `v5::StreamConsumer` /
//! `v5::QueueConsumer` ultimately delegates to a v4
//! [`magnetar_runtime_tokio::Producer`] / [`magnetar_runtime_tokio::Consumer`]
//! that speaks the same `CommandSend` / `CommandSubscribe` bytes the
//! v4 surface emits today.
//!
//! What V5 changes vs. v4 is **at the type / builder level**:
//!
//! - timeouts are typed `Duration` instead of millis-as-`u64` (the mapping module documents the
//!   conversion).
//! - the consumer surface splits into [`StreamConsumer`] (Exclusive / Failover) and
//!   [`QueueConsumer`] (Shared / KeyShared) so the subscription-type pivot is enforced by the type
//!   system rather than at runtime.
//! - the producer's `send` returns `Option<MessageId>` (the broker may not assign one for
//!   fire-and-forget paths).
//!
//! Java V5 is still iterating upstream, so the magnetar surface ships
//! behind `feature = "experimental-v5-client"` (default off). The full
//! v0.2.0 scope per [ADR-0032](../../../specs/adr/0032-pip-466-v5-client-surface-scope.md):
//! `v5::Producer<T, E>`, `v5::StreamConsumer<T, E>`, `v5::QueueConsumer<T, E>`,
//! and the `PulsarClientV5<E>` entry point. V5 `Reader`, `TableView`,
//! `Transaction`, `CheckpointConsumer` are out of scope and explicitly
//! deferred to v0.3.0+.

pub mod client;
pub mod mapping;
pub mod producer;
pub mod queue_consumer;
pub mod stream_consumer;

pub use client::PulsarClientV5;
pub use mapping::{
    DEFAULT_ACK_TIMEOUT, DEFAULT_MAX_PENDING_MESSAGES, DEFAULT_NEGATIVE_ACK_REDELIVERY_DELAY,
    DEFAULT_RECEIVER_QUEUE_SIZE, DEFAULT_SEND_TIMEOUT, V5SubscriptionInitialPosition,
};
pub use producer::Producer;
pub use queue_consumer::QueueConsumer;
pub use stream_consumer::StreamConsumer;
