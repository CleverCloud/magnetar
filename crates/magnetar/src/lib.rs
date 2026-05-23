// SPDX-License-Identifier: Apache-2.0

//! Apache Pulsar client driver for Rust.
//!
//! Public façade for the magnetar workspace. Re-exports the sans-io core
//! ([`magnetar_proto`]) plus the selected runtime engine, and provides an
//! ergonomic [`PulsarClient`] entry point that wires the protocol layer to
//! the tokio engine by default.
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use magnetar::{OutgoingMessage, PulsarClient};
//!
//! let client = PulsarClient::builder()
//!     .service_url("pulsar://localhost:6650")
//!     .build()
//!     .await?;
//!
//! let producer = client.producer("persistent://public/default/orders").create().await?;
//! producer
//!     .send(OutgoingMessage::with_payload(b"hello".as_slice()).into())
//!     .await?;
//!
//! let consumer = client
//!     .consumer("persistent://public/default/orders")
//!     .subscription("worker")
//!     .subscribe()
//!     .await?;
//! let msg = consumer.receive().await?;
//! consumer.ack(msg.message_id).await?;
//! # Ok(()) }
//! ```
//!
//! ## Feature flags
//!
//! - `tokio` (default): pull in the tokio engine.
//! - `moonpool`: pull in the moonpool engine.
//! - `admin`: re-export [`magnetar_admin`] under [`admin`] for the REST admin client.
//! - `auth-oauth2`, `auth-sasl`, `auth-athenz`: pluggable auth providers.
//! - `encryption`: PIP-4 end-to-end encryption.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

#[cfg(feature = "admin")]
pub use magnetar_admin as admin;
pub use magnetar_proto as proto;
pub use magnetar_proto::conn::{ConnectionConfig, OpOutcome};
// Re-export the most commonly used protocol types at the top level so users
// don't have to remember which crate they live in.
pub use magnetar_proto::{
    AuthProvider, Backoff, ConnectionEvent, ConsumerHandle, MessageId, ProducerHandle,
    ProtocolError, RequestId, SequenceId, SupervisorConfig,
};
#[cfg(feature = "moonpool")]
pub use magnetar_runtime_moonpool as runtime_moonpool;
#[cfg(feature = "tokio")]
pub use magnetar_runtime_tokio as runtime_tokio;

mod engine;
#[cfg(feature = "moonpool")]
pub use engine::MoonpoolEngine;
#[cfg(feature = "tokio")]
pub use engine::TokioEngine;
pub use engine::{Engine, TransactionApi};

#[cfg(feature = "tokio")]
mod client;
#[cfg(feature = "moonpool")]
mod moonpool_client;
#[cfg(feature = "tokio")]
mod multi_topics;
#[cfg(feature = "tokio")]
mod partitioned_consumer;
#[cfg(feature = "tokio")]
mod partitioned_producer;
#[cfg(feature = "tokio")]
mod pattern_consumer;
#[cfg(feature = "tokio")]
mod table_view;
#[cfg(feature = "tokio")]
mod transaction;
#[cfg(feature = "tokio")]
mod typed;
#[cfg(feature = "tokio")]
pub use client::{
    ClientBuilder, ConsumerBuilder, ConsumerInterceptor, IncomingMessage, MemoryLimit,
    MemoryLimitPolicy, MessageBuilder, OutgoingMessage, ProducerBuilder, ProducerExt,
    ProducerInterceptor, PulsarClient, PulsarError, Reader, ReaderBuilder, SeekTarget,
    ack_cumulative_with_interceptors, ack_with_interceptors, receive_with_interceptors,
    send_with_interceptors,
};
#[cfg(feature = "tokio")]
pub use multi_topics::{MultiTopicsConsumer, MultiTopicsConsumerBuilder, MultiTopicsMessage};
#[cfg(feature = "tokio")]
pub use partitioned_consumer::{PartitionedConsumer, PartitionedConsumerBuilder};
#[cfg(feature = "tokio")]
pub use partitioned_producer::{
    JavaStringHashHasher, MessageRouter, MessageRoutingMode, Murmur3HashHasher,
    PartitionedMessageBuilder, PartitionedProducer, PartitionedProducerBuilder, java_string_hash,
    murmur3_32_hash,
};
#[cfg(feature = "tokio")]
pub use pattern_consumer::{
    PatternConsumer, PatternConsumerBuilder, PatternMessage, ReconcileReport,
};
#[cfg(feature = "tokio")]
pub use table_view::{
    TableView, TableViewBuilder, TableViewListener, TypedTableView, TypedTableViewBuilder,
};
#[cfg(feature = "tokio")]
pub use transaction::{Transaction, TxnState};
#[cfg(feature = "tokio")]
pub use typed::{
    TypedConsumer, TypedConsumerBuilder, TypedMessage, TypedMessageBuilder, TypedProducer,
    TypedProducerBuilder,
};

// PIP-4 encryption bridge: implement the runtime's MessageEncryptor / MessageDecryptor traits
// for magnetar-messagecrypto::MessageCrypto. Behind the `encryption` feature so the heavy
// `aws-lc-rs` dep is opt-in.
#[cfg(all(feature = "tokio", feature = "encryption"))]
mod crypto_bridge;
#[cfg(all(feature = "tokio", feature = "encryption"))]
pub use crypto_bridge::MessageCryptoBridge;

#[cfg(test)]
mod tests {
    #[test]
    fn proto_reexport_compiles() {
        let _conn = crate::proto::Connection::new(crate::proto::ConnectionConfig::default());
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn builder_compiles() {
        let _ = crate::PulsarClient::builder().service_url("pulsar://localhost:6650");
    }

    /// Compile-time witness that the [`crate::TransactionApi`] trait is
    /// object-safe-compatible (all methods return `Pin<Box<dyn Future + Send>>`)
    /// AND that
    /// [`magnetar_runtime_tokio::Client`] satisfies the bound. Both
    /// properties are load-bearing for the D1 façade lift; if either
    /// regresses the generic `impl<E: Engine> PulsarClient<E> where
    /// E::ClientState: TransactionApi` will fail to compile. Runs at
    /// compile time only — no broker round-trip, no I/O.
    #[cfg(feature = "tokio")]
    fn assert_transaction_api_bound<T: crate::TransactionApi>() {}

    #[cfg(feature = "tokio")]
    #[test]
    fn transaction_api_is_implemented_by_tokio_client() {
        // Statically assert the bound; this entire function body is
        // dead at runtime — the assertion fires at typeck.
        assert_transaction_api_bound::<magnetar_runtime_tokio::Client>();
    }
}
