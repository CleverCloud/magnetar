// SPDX-License-Identifier: Apache-2.0

//! Apache Pulsar admin REST client.
//!
//! Provides tenant / namespace / topic / schema management endpoints over the
//! Pulsar admin REST API (`/admin/v2/...`). Uses `reqwest` with `rustls-tls`.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

/// Placeholder admin client. Implementation lands in M9.
#[derive(Debug, Default)]
pub struct AdminClient {
    _private: (),
}

impl AdminClient {
    /// Construct an unconfigured admin client.
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::AdminClient;

    #[test]
    fn client_can_be_constructed() {
        let _ = AdminClient::new();
    }
}
