// SPDX-License-Identifier: Apache-2.0

//! Pulsar service-URL parsing tests.

use magnetar_runtime_tokio::{ClientError, ParsedUrl, Scheme};

#[test]
fn plaintext_with_default_port() {
    let url = ParsedUrl::parse("pulsar://broker.example.com").expect("parse pulsar://");
    assert_eq!(url.scheme, Scheme::Plain);
    assert_eq!(url.host, "broker.example.com");
    assert_eq!(url.port, 6650);
}

#[test]
fn tls_with_default_port() {
    let url = ParsedUrl::parse("pulsar+ssl://broker.example.com").expect("parse pulsar+ssl://");
    assert_eq!(url.scheme, Scheme::Tls);
    assert_eq!(url.host, "broker.example.com");
    assert_eq!(url.port, 6651);
}

#[test]
fn explicit_port_wins_over_default() {
    let plain = ParsedUrl::parse("pulsar://broker.example.com:6651").expect("parse plain");
    assert_eq!(plain.scheme, Scheme::Plain);
    assert_eq!(plain.port, 6651);

    let tls = ParsedUrl::parse("pulsar+ssl://broker.example.com:9443").expect("parse tls");
    assert_eq!(tls.scheme, Scheme::Tls);
    assert_eq!(tls.port, 9443);
}

#[test]
fn ipv4_literal_host() {
    let url = ParsedUrl::parse("pulsar://127.0.0.1:6650").expect("parse ipv4");
    assert_eq!(url.host, "127.0.0.1");
}

#[test]
fn ipv6_literal_host() {
    let url = ParsedUrl::parse("pulsar://[::1]:6650").expect("parse ipv6");
    // Note: `url` strips the surrounding brackets on `host_str()`.
    assert_eq!(url.host, "[::1]");
}

#[test]
fn rejects_http_scheme() {
    let err = ParsedUrl::parse("http://broker:80").expect_err("must reject http");
    match err {
        ClientError::UnsupportedScheme(scheme) => assert_eq!(scheme, "http"),
        other => panic!("unexpected error variant: {other:?}"),
    }
}

#[test]
fn rejects_amqp_scheme() {
    let err = ParsedUrl::parse("amqp://broker:5672").expect_err("must reject amqp");
    assert!(matches!(err, ClientError::UnsupportedScheme(s) if s == "amqp"));
}

#[test]
fn rejects_garbage() {
    let err = ParsedUrl::parse("nope-not-a-url").expect_err("must reject garbage");
    assert!(matches!(err, ClientError::BadUrl(_)));
}

#[test]
fn default_port_constants_are_correct() {
    assert_eq!(Scheme::Plain.default_port(), 6650);
    assert_eq!(Scheme::Tls.default_port(), 6651);
}

#[test]
fn socket_addr_returns_host_and_port() {
    let url = ParsedUrl::parse("pulsar://broker.example.com:1234").expect("parse");
    let (host, port) = url.socket_addr();
    assert_eq!(host, "broker.example.com");
    assert_eq!(port, 1234);
}
