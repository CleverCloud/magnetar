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

/// **Experimental** (PIP-460, ADR-0031). `true` when `topic` uses the
/// scalable-topic `topic://...` URL scheme (as opposed to the v4
/// `persistent://` / `non-persistent://` schemes). The builder routes a
/// `topic://` URL to the scalable lookup path
/// ([`crate::Client::scalable_topic_lookup`]); every other scheme keeps the v4
/// path untouched.
#[cfg(feature = "scalable-topics")]
#[must_use]
pub fn is_scalable_topic_url(topic: &str) -> bool {
    topic.starts_with("topic://")
}

#[cfg(all(test, feature = "scalable-topics"))]
mod scalable_url_tests {
    use super::is_scalable_topic_url;

    #[test]
    fn recognises_scalable_and_v4_schemes() {
        assert!(is_scalable_topic_url("topic://public/default/scaled"));
        assert!(!is_scalable_topic_url(
            "persistent://public/default/regular"
        ));
        assert!(!is_scalable_topic_url("non-persistent://public/default/np"));
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

    // Mirrors `magnetar_runtime_moonpool::transport::tests::split_host_port_rejects_non_numeric_port`:
    // a non-numeric port should surface a typed error, not panic.
    #[test]
    fn rejects_non_numeric_port() {
        let err = ParsedUrl::parse("pulsar://broker:abc")
            .expect_err("non-numeric port must surface as a typed error");
        // Either BadUrl (url crate rejects it pre-parse) or
        // UnsupportedScheme — both are typed and non-panicking.
        assert!(
            matches!(
                err,
                ClientError::BadUrl(_) | ClientError::UnsupportedScheme(_)
            ),
            "expected typed parse error, got {err:?}",
        );
    }

    // Mirrors `magnetar_runtime_moonpool::transport::tests::split_host_port_handles_high_port`:
    // the maximum legal TCP port (65535) must parse without overflow.
    #[test]
    fn parses_high_port() {
        let parsed = ParsedUrl::parse("pulsar://broker:65535").expect("parse");
        assert_eq!(parsed.port, 65535);
    }
}
