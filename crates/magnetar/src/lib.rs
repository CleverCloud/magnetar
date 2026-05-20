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
    AuthProvider, ConnectionEvent, ConsumerHandle, MessageId, ProducerHandle, ProtocolError,
    RequestId, SequenceId,
};
#[cfg(feature = "moonpool")]
pub use magnetar_runtime_moonpool as runtime_moonpool;
#[cfg(feature = "tokio")]
pub use magnetar_runtime_tokio as runtime_tokio;

#[cfg(feature = "tokio")]
mod client;
#[cfg(feature = "tokio")]
mod multi_topics;
#[cfg(feature = "tokio")]
mod partitioned_consumer;
#[cfg(feature = "tokio")]
mod partitioned_producer;
#[cfg(feature = "tokio")]
mod table_view;
#[cfg(feature = "tokio")]
mod transaction;
#[cfg(feature = "tokio")]
mod typed;
#[cfg(feature = "tokio")]
pub use client::{
    ClientBuilder, ConsumerBuilder, IncomingMessage, OutgoingMessage, ProducerBuilder,
    PulsarClient, PulsarError, Reader, ReaderBuilder,
};
#[cfg(feature = "tokio")]
pub use multi_topics::{MultiTopicsConsumer, MultiTopicsConsumerBuilder, MultiTopicsMessage};
#[cfg(feature = "tokio")]
pub use partitioned_consumer::{PartitionedConsumer, PartitionedConsumerBuilder};
#[cfg(feature = "tokio")]
pub use partitioned_producer::{
    MessageRoutingMode, PartitionedProducer, PartitionedProducerBuilder,
};
#[cfg(feature = "tokio")]
pub use table_view::{TableView, TableViewBuilder, TableViewListener};
#[cfg(feature = "tokio")]
pub use transaction::{Transaction, TxnState};
#[cfg(feature = "tokio")]
pub use typed::{
    TypedConsumer, TypedConsumerBuilder, TypedMessage, TypedProducer, TypedProducerBuilder,
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
}
