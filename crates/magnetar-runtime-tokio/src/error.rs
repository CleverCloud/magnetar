// SPDX-License-Identifier: Apache-2.0

//! Engine-layer error type.
//!
//! Wraps `std::io::Error`, `rustls::Error`, `magnetar_proto::ProtocolError`, and `url::ParseError`
//! into a single error consumers can match on. Mirrors the layering used by `quinn`'s engine
//! crate.

use thiserror::Error;

/// Tokio-engine error surface.
#[derive(Debug, Error)]
pub enum ClientError {
    /// Underlying socket I/O failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// TLS handshake or session error.
    #[error("tls error: {0}")]
    Tls(#[from] rustls::Error),

    /// Sans-io state machine reported a fatal protocol violation.
    #[error("protocol error: {0}")]
    Protocol(#[from] magnetar_proto::ProtocolError),

    /// The connect URL could not be parsed.
    #[error("bad url: {0}")]
    BadUrl(#[from] url::ParseError),

    /// The connect URL used an unsupported scheme.
    #[error("unsupported scheme: {0}")]
    UnsupportedScheme(String),

    /// The peer closed the connection (read returned 0).
    #[error("peer closed the connection")]
    PeerClosed,

    /// The connection has been locally closed.
    #[error("connection is closed")]
    Closed,

    /// Send was rejected by the broker.
    #[error("send rejected: code={code} message={message}")]
    SendRejected {
        /// Pulsar wire-protocol `ServerError` code.
        code: i32,
        /// Broker-supplied error string.
        message: String,
    },

    /// Generic broker error correlated with a pending request.
    #[error("broker error: code={code} message={message}")]
    Broker {
        /// Pulsar wire-protocol `ServerError` code.
        code: i32,
        /// Broker-supplied error string.
        message: String,
    },

    /// TLS handshake produced an invalid server name.
    #[error("invalid server name for tls: {0}")]
    InvalidServerName(String),

    /// Catch-all for engine-internal misconfiguration.
    #[error("other: {0}")]
    Other(String),
}
