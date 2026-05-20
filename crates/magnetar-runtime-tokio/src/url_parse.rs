// SPDX-License-Identifier: Apache-2.0

//! Pulsar URL parsing helper.
//!
//! Accepts `pulsar://host[:port]` (plaintext, default port 6650) and `pulsar+ssl://host[:port]`
//! (TLS, default port 6651). Returns a normalised [`ParsedUrl`] the engine can hand directly to
//! `TcpStream::connect`.

use crate::error::ClientError;

/// Parsed Pulsar service URL scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// Plaintext Pulsar binary protocol.
    Plain,
    /// TLS Pulsar binary protocol.
    Tls,
}

impl Scheme {
    /// Default port for this scheme as documented at <https://pulsar.apache.org>.
    pub const fn default_port(self) -> u16 {
        match self {
            Self::Plain => 6650,
            Self::Tls => 6651,
        }
    }
}

/// A parsed Pulsar service URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedUrl {
    /// Either [`Scheme::Plain`] or [`Scheme::Tls`].
    pub scheme: Scheme,
    /// Hostname (DNS or IP literal). The TLS server-name uses this verbatim.
    pub host: String,
    /// Effective port (URL port if set, otherwise [`Scheme::default_port`]).
    pub port: u16,
}

impl ParsedUrl {
    /// Parse a Pulsar service URL.
    ///
    /// # Errors
    ///
    /// - [`ClientError::BadUrl`] if `url::Url::parse` fails.
    /// - [`ClientError::UnsupportedScheme`] if the scheme is neither `pulsar` nor `pulsar+ssl`.
    /// - [`ClientError::Other`] if the host is missing.
    pub fn parse(input: &str) -> Result<Self, ClientError> {
        let url = url::Url::parse(input)?;
        let scheme = match url.scheme() {
            "pulsar" => Scheme::Plain,
            "pulsar+ssl" => Scheme::Tls,
            other => return Err(ClientError::UnsupportedScheme(other.to_owned())),
        };
        let host = url
            .host_str()
            .ok_or_else(|| ClientError::Other(format!("url has no host: {input}")))?
            .to_owned();
        let port = url.port().unwrap_or_else(|| scheme.default_port());
        Ok(Self { scheme, host, port })
    }

    /// `(host, port)` tuple suitable for `TcpStream::connect`.
    pub fn socket_addr(&self) -> (&str, u16) {
        (self.host.as_str(), self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::{ParsedUrl, Scheme};
    use crate::error::ClientError;

    #[test]
    fn parses_plain_default_port() {
        let u = ParsedUrl::parse("pulsar://broker.example.com").expect("parse");
        assert_eq!(u.scheme, Scheme::Plain);
        assert_eq!(u.host, "broker.example.com");
        assert_eq!(u.port, 6650);
    }

    #[test]
    fn parses_plain_explicit_port() {
        let u = ParsedUrl::parse("pulsar://localhost:6651").expect("parse");
        assert_eq!(u.scheme, Scheme::Plain);
        assert_eq!(u.port, 6651);
    }

    #[test]
    fn parses_tls_default_port() {
        let u = ParsedUrl::parse("pulsar+ssl://broker.example.com").expect("parse");
        assert_eq!(u.scheme, Scheme::Tls);
        assert_eq!(u.port, 6651);
    }

    #[test]
    fn rejects_unsupported_scheme() {
        let err = ParsedUrl::parse("http://localhost:8080").expect_err("unsupported");
        match err {
            ClientError::UnsupportedScheme(s) => assert_eq!(s, "http"),
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn rejects_malformed_url() {
        assert!(matches!(
            ParsedUrl::parse("not-a-url"),
            Err(ClientError::BadUrl(_))
        ));
    }
}
