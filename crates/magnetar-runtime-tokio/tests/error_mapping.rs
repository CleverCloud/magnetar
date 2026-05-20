// SPDX-License-Identifier: Apache-2.0

//! Verify the error-conversion surface — every variant the public API claims to wrap should be
//! round-trippable through `From`.

use std::io::{Error as IoError, ErrorKind};

use magnetar_proto::ProtocolError;
use magnetar_runtime_tokio::ClientError;

#[test]
fn from_io_error() {
    let io = IoError::new(ErrorKind::ConnectionRefused, "nope");
    let mapped: ClientError = io.into();
    assert!(matches!(mapped, ClientError::Io(_)));
    assert!(mapped.to_string().contains("io error"));
}

#[test]
fn from_protocol_error() {
    let proto = ProtocolError::InvariantViolation("unknown producer handle");
    let mapped: ClientError = proto.into();
    match mapped {
        ClientError::Protocol(ProtocolError::InvariantViolation(msg)) => {
            assert_eq!(msg, "unknown producer handle");
        }
        other => panic!("expected ClientError::Protocol, got {other:?}"),
    }
}

#[test]
fn from_url_parse_error() {
    let bad = url::Url::parse("not-a-url").expect_err("url should fail");
    let mapped: ClientError = bad.into();
    assert!(matches!(mapped, ClientError::BadUrl(_)));
}

#[test]
fn from_rustls_error() {
    let rustls_err = rustls::Error::General("simulated".into());
    let mapped: ClientError = rustls_err.into();
    assert!(matches!(mapped, ClientError::Tls(_)));
    assert!(mapped.to_string().contains("tls error"));
}

#[test]
fn variants_have_distinct_display() {
    let peer_closed = ClientError::PeerClosed;
    let closed = ClientError::Closed;
    assert_ne!(peer_closed.to_string(), closed.to_string());

    let send_rejected = ClientError::SendRejected {
        code: 7,
        message: "fenced".into(),
    };
    assert!(send_rejected.to_string().contains("fenced"));
    assert!(send_rejected.to_string().contains('7'));

    let broker = ClientError::Broker {
        code: 9,
        message: "no go".into(),
    };
    assert!(broker.to_string().contains("no go"));

    let invalid_sn = ClientError::InvalidServerName("notdns_.weird".into());
    assert!(invalid_sn.to_string().contains("notdns_.weird"));

    let unsupported_scheme = ClientError::UnsupportedScheme("http".into());
    assert!(unsupported_scheme.to_string().contains("http"));

    let other = ClientError::Other("anything".into());
    assert!(other.to_string().contains("anything"));
}
