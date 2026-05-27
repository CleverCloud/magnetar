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

    /// A bounded operation exceeded its caller-supplied deadline. Mirrors Java
    /// `PulsarClientException.TimeoutException` — surfaced by `Producer::flush_with_timeout`
    /// and any other API that wraps a long-running protocol round-trip in
    /// `tokio::time::timeout`.
    #[error("timed out: {0}")]
    Timeout(String),

    /// Configured global publish memory budget exhausted. Mirrors Java
    /// `MemoryLimitController` rejecting a send under
    /// `MemoryLimitPolicy.FailImmediately`. `current` + `requested` would
    /// push past `limit`; the caller can retry later (the counter drains
    /// as outstanding sends complete).
    #[error(
        "memory limit exceeded: current={current} bytes, requested={requested} bytes, limit={limit} bytes"
    )]
    MemoryLimitExceeded {
        /// Bytes already reserved by in-flight sends at the moment of the check.
        current: u64,
        /// The configured budget.
        limit: u64,
        /// The size of the send that triggered the overflow.
        requested: u64,
    },

    /// A lookup answered `proxy_through_service_url = true` but the client has no proxy
    /// connection pool because it was built via [`crate::Client::from_socket`] (a raw socket
    /// has no URL to dial back through). Switch to a URL-based connect entry —
    /// [`crate::Client::connect`] / [`crate::Client::connect_with_resolver_and_provider`] —
    /// to use the pool. See ADR-0039.
    #[error(
        "lookup of topic '{topic}' requires proxy routing (proxy_through_service_url=true) \
         but this client was built via from_socket and has no proxy pool; rebuild with \
         Client::connect"
    )]
    ProxyUnsupportedOnSocketClient {
        /// The topic whose lookup triggered the proxy-routing requirement.
        topic: String,
    },

    /// Catch-all for engine-internal misconfiguration.
    #[error("other: {0}")]
    Other(String),
}
