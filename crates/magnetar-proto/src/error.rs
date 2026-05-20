// SPDX-License-Identifier: Apache-2.0

//! Sans-io error types.
//!
//! Every state-machine API returns a `Result` whose error is one of these variants. The error
//! types are deliberately public so callers (the runtime engines and the high-level `magnetar`
//! façade) can match on them when surfacing diagnostics to the user.
//!
//! The naming and granularity mirrors `org.apache.pulsar.client.api.PulsarClientException` so
//! library users coming from the Java client can map errors 1-for-1.

use crate::frame::FrameError;

/// Errors that the [`Connection`](crate::Connection) state machine can surface.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// Frame decoding failed.
    #[error("frame error: {0}")]
    Frame(#[from] FrameError),

    /// The remote sent a `BaseCommand` whose `type` did not match any populated oneof field.
    #[error("unsupported command type: {0}")]
    UnsupportedCommand(i32),

    /// The remote sent a command that violates the local state machine invariants
    /// (e.g. a `SendReceipt` referencing an unknown producer).
    #[error("protocol invariant violated: {0}")]
    InvariantViolation(&'static str),

    /// The handshake was attempted while the connection was not in a state that supports it.
    #[error("handshake error: {0}")]
    Handshake(&'static str),

    /// The peer signalled a fatal error via `CommandError` or `CommandSendError`.
    #[error("server error {code}: {message}")]
    Server {
        /// Pulsar wire-protocol `ServerError` code (raw i32 — translate via
        /// `pb::ServerError::try_from`).
        code: i32,
        /// Human-readable broker message.
        message: String,
    },
}

/// Errors specific to producer-side operations.
#[derive(Debug, thiserror::Error)]
pub enum ProducerError {
    /// Message is too large for the configured max-message-size and chunking is disabled.
    #[error("message is too large ({size} bytes > {max_message_size}) and chunking is disabled")]
    MessageTooLarge {
        /// Payload size that exceeded the limit.
        size: usize,
        /// Configured `maxMessageSize` from `CommandConnected`.
        max_message_size: usize,
    },

    /// The producer is closing or closed; subsequent sends are rejected.
    #[error("producer is closed")]
    Closed,

    /// The publish was rejected by the broker via `CommandSendError`.
    #[error("send rejected by broker: {0}")]
    SendRejected(String),
}

/// Errors specific to consumer-side operations.
#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {
    /// The consumer is closing or closed; subsequent operations are rejected.
    #[error("consumer is closed")]
    Closed,

    /// A `CommandMessage` arrived for an unknown consumer id.
    #[error("unknown consumer id: {0}")]
    UnknownConsumer(u64),
}
