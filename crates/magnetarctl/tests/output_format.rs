// SPDX-License-Identifier: Apache-2.0

//! Rendering tests for `--format human`.
//!
//! The renderer is pure (typed fields → `String`), so these tests pin
//! the exact bytes a terminal sees without talking to a broker.

use magnetar_admin::RetentionPolicies;

#[path = "../src/main.rs"]
#[allow(dead_code, unused_imports)]
mod cli;

#[test]
fn retention_human_view_uses_labels_units_and_infinity() {
    use cli::output::HumanOutput;
    let policies = RetentionPolicies {
        retention_time_in_minutes: 527_040,
        retention_size_in_mb: -1,
    };
    assert_eq!(
        cli::output::render_rows(&policies.human_fields(), false),
        "RETENTION DURATION  366 days\nRETENTION SIZE      ∞\n"
    );
    assert_eq!(
        serde_json::to_string(&policies).expect("serialize"),
        "{\"retentionTimeInMinutes\":527040,\"retentionSizeInMB\":-1}"
    );
    let colored = cli::output::render_rows(&policies.human_fields(), true);
    assert_eq!(
        colored.replace("\x1b[34m", "").replace("\x1b[0m", ""),
        cli::output::render_rows(&policies.human_fields(), false)
    );
}

#[test]
fn retention_units_preserve_partial_days_and_sentinel_values() {
    use cli::output::{DurationMinutes, Limit, SizeMegabytes};
    for (minutes, expected) in [
        (-1, "∞"),
        (0, "0 minutes"),
        (1, "1 minute"),
        (60, "1 hour"),
        (1440, "1 day"),
        (1501, "1 day 1 hour 1 minute"),
        (2882, "2 days 2 minutes"),
        (-2, "-2 minutes"),
    ] {
        assert_eq!(
            Limit::from_sentinel(minutes, DurationMinutes).to_string(),
            expected
        );
    }
    for (mb, expected) in [
        (-1, "∞"),
        (0, "0 MB"),
        (128, "128 MB"),
        (i64::MAX, "9223372036854775807 MB"),
    ] {
        assert_eq!(
            Limit::from_sentinel(mb, SizeMegabytes).to_string(),
            expected
        );
    }
}

#[test]
fn shared_rows_render_arbitrary_command_labels_and_values() {
    let fields = vec![
        ("topic", "orders".to_owned()),
        ("enabled", "true".to_owned()),
    ];
    let plain = cli::output::render_rows(&fields, false);
    assert_eq!(plain, "TOPIC    orders\nENABLED  true\n");
    assert_eq!(
        cli::output::render_rows(&fields, true),
        "\x1b[34mTOPIC  \x1b[0m  orders\n\x1b[34mENABLED\x1b[0m  true\n"
    );
    assert_eq!(cli::output::render_rows(&[], true), "");
}

/// Exercise the real command dispatch, HTTP endpoint and format selection.
#[test]
fn topics_get_retention_uses_shared_human_output_and_preserves_json() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::process::Command;
    use std::time::{Duration, Instant};

    let config_dir = tempfile::tempdir().expect("tempdir");
    let config = config_dir.path().join("config.yaml");
    std::fs::write(&config, "{}\n").expect("write config");
    for format in ["human", "json"] {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address");
        let server = std::thread::spawn(move || {
            listener.set_nonblocking(true).expect("nonblocking");
            let deadline = Instant::now() + Duration::from_secs(10);
            let (mut socket, _) = loop {
                match listener.accept() {
                    Ok(connection) => break connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "CLI never contacted the mock server"
                        );
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accept: {error}"),
                }
            };
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .expect("timeout");
            let mut request = Vec::new();
            let mut buffer = [0; 1024];
            while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let count = socket.read(&mut buffer).expect("read request");
                assert_ne!(count, 0, "request ended before headers");
                request.extend_from_slice(&buffer[..count]);
            }
            assert!(String::from_utf8_lossy(&request).starts_with(
                "GET /admin/v2/persistent/public/default/orders/retention HTTP/1.1\r\n"
            ));
            let body = r#"{"retentionTimeInMinutes":527040,"retentionSizeInMB":-1}"#;
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).expect("respond");
        });
        let result = Command::new(env!("CARGO_BIN_EXE_magnetarctl"))
            .env_remove("MAGNETAR_FORMAT")
            .env_remove("MAGNETAR_CONTEXT")
            .env_remove("MAGNETAR_TOKEN")
            .env("NO_COLOR", "1")
            .arg("--config")
            .arg(&config)
            .arg("--admin-url")
            .arg(format!("http://{address}"))
            .args([
                "admin",
                "topics",
                "get-retention",
                "persistent://public/default/orders",
                "-F",
                format,
            ])
            .output()
            .expect("run magnetarctl");
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        server.join().expect("server");
        let stdout = String::from_utf8(result.stdout).expect("UTF-8");
        if format == "human" {
            assert_eq!(
                stdout,
                "SOURCE              topic policy\n\
                 RETENTION DURATION  366 days\n\
                 RETENTION SIZE      ∞\n"
            );
        } else {
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&stdout).expect("JSON"),
                serde_json::json!({
                    "source": "topic",
                    "retentionTimeInMinutes": 527_040,
                    "retentionSizeInMB": -1
                })
            );
        }
    }
}

/// Serve `responses` in order, one HTTP/1.1 request per connection
/// (`Connection: close`), asserting each request line's path. Returns the
/// server thread to join once the CLI exits.
fn serve_in_order(
    listener: std::net::TcpListener,
    responses: Vec<(&'static str, u16, &'static str)>,
) -> std::thread::JoinHandle<()> {
    use std::io::{Read, Write};
    use std::time::{Duration, Instant};
    std::thread::spawn(move || {
        listener.set_nonblocking(true).expect("nonblocking");
        for (path, status, body) in responses {
            let deadline = Instant::now() + Duration::from_secs(10);
            let (mut socket, _) = loop {
                match listener.accept() {
                    Ok(connection) => break connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "CLI never requested {path}");
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accept: {error}"),
                }
            };
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .expect("timeout");
            let mut request = Vec::new();
            let mut buffer = [0; 1024];
            while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let count = socket.read(&mut buffer).expect("read request");
                assert_ne!(count, 0, "request ended before headers");
                request.extend_from_slice(&buffer[..count]);
            }
            let request = String::from_utf8_lossy(&request);
            assert!(
                request.starts_with(&format!("GET {path} HTTP/1.1\r\n")),
                "expected GET {path}, got: {request}"
            );
            write!(
                socket,
                "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .expect("respond");
        }
    })
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "two scenarios x two formats against an in-process HTTP server; splitting would duplicate the harness"
)]
fn topic_persistence_falls_back_to_namespace_then_broker_and_says_so() {
    use std::process::Command;

    let config_dir = tempfile::tempdir().expect("tempdir");
    let config = config_dir.path().join("config.yaml");
    std::fs::write(&config, "{}\n").expect("write config");
    let broker_config = r#"{"managedLedgerDefaultEnsembleSize":"3","managedLedgerDefaultWriteQuorum":"3","managedLedgerDefaultAckQuorum":"2","managedLedgerDefaultMarkDeleteRateLimit":"1.0"}"#;
    let namespace_policy = r#"{"bookkeeperEnsemble":5,"bookkeeperWriteQuorum":4,"bookkeeperAckQuorum":3,"managedLedgerMaxMarkDeleteRate":0.0}"#;
    // (responses, expected human stdout, expected JSON stdout)
    let scenarios = [
        (
            // Topic unset (204), namespace unset (200 empty): the broker's
            // runtime configuration is the effective policy.
            vec![
                (
                    "/admin/v2/persistent/public/default/orders/persistence",
                    204,
                    "",
                ),
                ("/admin/v2/namespaces/public/default/persistence", 200, ""),
                (
                    "/admin/v2/brokers/configuration/runtime",
                    200,
                    broker_config,
                ),
            ],
            "SOURCE                   broker default (no policy set)\n\
             BOOKKEEPER ENSEMBLE      3\n\
             BOOKKEEPER WRITE QUORUM  3\n\
             BOOKKEEPER ACK QUORUM    2\n\
             MAX MARK-DELETE RATE     1 ops/s\n",
            serde_json::json!({
                "source": "broker",
                "bookkeeperEnsemble": 3,
                "bookkeeperWriteQuorum": 3,
                "bookkeeperAckQuorum": 2,
                "managedLedgerMaxMarkDeleteRate": 1.0
            }),
        ),
        (
            // Topic unset, namespace set: no broker call at all.
            vec![
                (
                    "/admin/v2/persistent/public/default/orders/persistence",
                    200,
                    "null",
                ),
                (
                    "/admin/v2/namespaces/public/default/persistence",
                    200,
                    namespace_policy,
                ),
            ],
            "SOURCE                   namespace policy\n\
             BOOKKEEPER ENSEMBLE      5\n\
             BOOKKEEPER WRITE QUORUM  4\n\
             BOOKKEEPER ACK QUORUM    3\n\
             MAX MARK-DELETE RATE     disabled\n",
            serde_json::json!({
                "source": "namespace",
                "bookkeeperEnsemble": 5,
                "bookkeeperWriteQuorum": 4,
                "bookkeeperAckQuorum": 3,
                "managedLedgerMaxMarkDeleteRate": 0.0
            }),
        ),
    ];
    for (responses, expected_human, expected_json) in scenarios {
        for format in ["human", "json"] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            let address = listener.local_addr().expect("address");
            let server = serve_in_order(listener, responses.clone());
            let result = Command::new(env!("CARGO_BIN_EXE_magnetarctl"))
                .env_remove("MAGNETAR_FORMAT")
                .env_remove("MAGNETAR_CONTEXT")
                .env_remove("MAGNETAR_TOKEN")
                .env("NO_COLOR", "1")
                .arg("--config")
                .arg(&config)
                .arg("--admin-url")
                .arg(format!("http://{address}"))
                .args([
                    "admin",
                    "topics",
                    "get-persistence",
                    "persistent://public/default/orders",
                    "-F",
                    format,
                ])
                .output()
                .expect("run magnetarctl");
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            server.join().expect("server");
            let stdout = String::from_utf8(result.stdout).expect("UTF-8");
            if format == "human" {
                assert_eq!(stdout, expected_human);
            } else {
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&stdout).expect("JSON"),
                    expected_json
                );
            }
        }
    }
}

#[test]
fn tables_align_columns_and_color_only_headers() {
    let rows = vec![
        ["long topic".to_owned(), "4".to_owned()],
        ["short".to_owned(), "—".to_owned()],
    ];
    let plain = cli::output::render_table(["topic", "partitions"], &rows, false);
    assert_eq!(
        plain,
        "TOPIC       PARTITIONS\nlong topic  4\nshort       —\n"
    );
    let colored = cli::output::render_table(["topic", "partitions"], &rows, true);
    assert_eq!(
        colored,
        "\x1b[34mTOPIC     \x1b[0m  \x1b[34mPARTITIONS\x1b[0m\nlong topic  4\nshort       —\n"
    );
    assert_eq!(
        cli::output::render_table(["tenant"], &[["acme".to_owned()]], false),
        "TENANT\nacme\n"
    );
    assert_eq!(
        cli::output::render_topics(&[], false),
        "TOPIC  PARTITIONS\n"
    );
}

#[test]
fn topic_table_uses_dash_for_non_partitioned_topics() {
    use cli::output::TopicRow;
    assert_eq!(
        cli::output::render_topics(
            &[
                TopicRow {
                    name: "orders".to_owned(),
                    partitions: Some(4)
                },
                TopicRow {
                    name: "plain".to_owned(),
                    partitions: None
                },
            ],
            false
        ),
        "TOPIC   PARTITIONS\norders  4\nplain   —\n"
    );
}

#[test]
fn topic_list_collapses_confirmed_partitions_with_declared_count() {
    use std::collections::BTreeMap;
    let topics = [
        "persistent://tenant/ns/plain",
        "persistent://tenant/ns/orders-partition-2",
        "persistent://tenant/ns/orders-partition-0",
        "persistent://tenant/ns/single-partition-0",
        "persistent://tenant/ns/unrelated-partition-0",
        "persistent://tenant/ns/orders-partition-9",
    ]
    .map(str::to_owned);
    let counts = BTreeMap::from([
        ("persistent://tenant/ns/orders".to_owned(), 4),
        ("persistent://tenant/ns/single".to_owned(), 1),
        ("persistent://tenant/ns/unrelated".to_owned(), 0),
    ]);
    let rows = cli::output::collapse_partitioned_topics(&topics, &counts);
    let actual: Vec<_> = rows
        .iter()
        .map(|row| (row.name.as_str(), row.partitions))
        .collect();
    assert_eq!(
        actual,
        vec![
            ("persistent://tenant/ns/plain", None),
            ("persistent://tenant/ns/orders", Some(4)),
            ("persistent://tenant/ns/single", Some(1)),
            ("persistent://tenant/ns/unrelated-partition-0", None),
            ("persistent://tenant/ns/orders-partition-9", None),
        ]
    );
    assert!(cli::output::collapse_partitioned_topics(&[], &counts).is_empty());
}

#[test]
fn partition_candidates_require_a_numeric_suffix() {
    for topic in [
        "plain",
        "x-partition-",
        "x-partition-abc",
        "x-partition-+1",
        "x-partition-4294967296",
    ] {
        assert_eq!(cli::output::partition_parent(topic), None);
    }
    assert_eq!(
        cli::output::partition_parent("x-partition-0"),
        Some(("x", 0))
    );
}

#[test]
fn topic_stats_render_units_and_compact_tables() {
    let stats: magnetar_admin::TopicStats = serde_json::from_value(serde_json::json!({
        "msgRateIn": 1.5,
        "msgThroughputOut": 42.25,
        "msgInCounter": 652,
        "bytesInCounter": 214_661,
        "storageSize": 685_453,
        "publishers": [{"producerId": 3, "address": "/[2001:db8::1]:1234", "clientVersion": "pulsar-rs-v6.9.0", "msgRateIn": 2.555, "extension": {"enabled": true}}],
        "subscriptions": {"CaseSensitive": {"msgBacklog": 7, "consumers": [{"consumerName": "reader"}]}}
    })).expect("stats");
    let plain = cli::output::render_topic_stats(&stats, 1, false);
    for expected in [
        "MESSAGE RATE IN       1.50 msg/s",
        "MESSAGE RATE OUT      0.00 msg/s",
        "THROUGHPUT OUT        42.25 B/s",
        "BYTES RECEIVED        214.66 KB",
        "PUBLISHERS            1",
        "SUBSCRIPTIONS         1",
        "PRODUCER",
        "RATE",
        "2.56 msg/s",
        "2001:db8::1",
        "pulsar-rs-v6.9.0",
        "SUBSCRIPTION",
        "CaseSensitive",
        "CONSUMERS",
        "BACKLOG",
    ] {
        assert!(plain.contains(expected), "missing {expected:?} in {plain}");
    }
    let colored = cli::output::render_topic_stats(&stats, 1, true);
    assert_eq!(
        colored.replace("\x1b[34m", "").replace("\x1b[0m", ""),
        plain
    );
    assert!(
        cli::output::render_topic_stats(&magnetar_admin::TopicStats::default(), 0, false)
            .starts_with("PARTITIONS            —\n")
    );
}

#[test]
fn compact_stats_have_one_row_per_entity_and_dash_for_missing_fields() {
    let stats: magnetar_admin::TopicStats = serde_json::from_value(serde_json::json!({
        "publishers": [{"producerName": "First", "producerId": 7, "address": "/192.0.2.4:51824", "clientVersion": "test-v1"}, {"producerId": 8}],
        "subscriptions": {"CaseSensitive": {"type": "Shared", "consumers": [{"consumerName": "reader-A"}, {"consumerName": "reader-B"}],
            "msgBacklog": 3, "unackedMessages": 1, "msgRateOut": 2.5, "msgThroughputOut": 214_990.0},
            "Empty": {}}
    }))
    .expect("stats");
    let output = cli::output::render_topic_stats(&stats, 1, false);
    let rows: Vec<_> = output
        .lines()
        .filter(|line| {
            line.starts_with("First")
                || line.starts_with("2 ")
                || line.starts_with("CaseSensitive")
                || line.starts_with("Empty")
        })
        .collect();
    assert_eq!(rows.len(), 4, "{output}");
    assert_eq!(
        rows[0].split_whitespace().collect::<Vec<_>>(),
        [
            "First",
            "7",
            "192.0.2.4",
            "test-v1",
            "—",
            "—",
            "—",
            "—",
            "—"
        ]
    );
    assert_eq!(
        rows[1].split_whitespace().collect::<Vec<_>>(),
        ["2", "8", "—", "—", "—", "—", "—", "—", "—"]
    );
    assert_eq!(
        rows[2].split_whitespace().collect::<Vec<_>>(),
        [
            "CaseSensitive",
            "Shared",
            "2",
            "3",
            "1",
            "2.50",
            "msg/s",
            "214.99",
            "KB/s"
        ]
    );
    assert_eq!(
        rows[3].split_whitespace().collect::<Vec<_>>(),
        ["Empty", "—", "—", "—", "—", "—", "—"]
    );
}

#[test]
fn decimal_byte_units_scale_sizes_and_rates_to_two_decimals() {
    use cli::output::{ByteRate, SizeBytes};
    for (value, expected) in [
        (0.0, "0.00 B"),
        (329.0, "329.00 B"),
        (999.0, "999.00 B"),
        (1000.0, "1.00 KB"),
        (214_990.0, "214.99 KB"),
        (999_999.0, "1.00 MB"),
        (1_000_000.0, "1.00 MB"),
        (1_000_000_000.0, "1.00 GB"),
        (-214_990.0, "-214.99 KB"),
    ] {
        assert_eq!(SizeBytes(value).to_string(), expected);
        assert_eq!(ByteRate(value).to_string(), format!("{expected}/s"));
    }
    assert_eq!(SizeBytes::from_counter(214_990).to_string(), "214.99 KB");
}

#[test]
fn consumer_table_preserves_subscription_and_formats_units_and_missing_fields() {
    let stats: magnetar_admin::TopicStats = serde_json::from_value(serde_json::json!({
        "subscriptions": {
            "SubA": {"consumers": [{"consumerName": "ReaderA", "address": "/[2001:db8::2]:1234",
                "clientVersion": "test-v2", "msgRateOut": 12.345, "msgThroughputOut": 214_990,
                "unackedMessages": 4, "availablePermits": 100}]},
            "SubB": {"consumers": [{"consumerName": "ReaderB"}]},
            "Empty": {"consumers": []}, "Missing": {}
        }
    }))
    .expect("stats");
    let plain = cli::output::render_topic_stats(&stats, 0, false);
    let rows: Vec<_> = plain
        .lines()
        .filter(|line| line.starts_with("Reader"))
        .collect();
    assert_eq!(rows.len(), 2, "{plain}");
    assert_eq!(
        rows[0].split_whitespace().collect::<Vec<_>>(),
        [
            "ReaderA",
            "SubA",
            "2001:db8::2",
            "test-v2",
            "12.35",
            "msg/s",
            "214.99",
            "KB/s",
            "4",
            "100"
        ]
    );
    assert_eq!(
        rows[1].split_whitespace().collect::<Vec<_>>(),
        ["ReaderB", "SubB", "—", "—", "—", "—", "—", "—"]
    );
    let colored = cli::output::render_topic_stats(&stats, 0, true);
    assert_eq!(
        colored.replace("\x1b[34m", "").replace("\x1b[0m", ""),
        plain
    );
    let empty = cli::output::render_topic_stats(&magnetar_admin::TopicStats::default(), 0, false);
    assert!(!empty.lines().any(|line| line.starts_with("CONSUMER ")));
}

#[test]
fn failure_domains_name_the_domain_once_per_group_in_payload_order() {
    let domains = serde_json::json!({
        "par7": {"brokers": ["broker-n3:8080", "broker-n4:8080"]},
        "par6": {"brokers": ["broker-n1:8080"]},
        "empty": {"brokers": []},
        "missing": {}
    });
    let plain = cli::output::render_failure_domains(&domains, false);
    assert_eq!(
        plain,
        "DOMAIN   BROKERS\n\
         par7     broker-n3:8080\n\
         \x20        broker-n4:8080\n\
         par6     broker-n1:8080\n\
         empty    —\n\
         missing  —\n"
    );
    let colored = cli::output::render_failure_domains(&domains, true);
    assert_eq!(
        colored.replace("\x1b[34m", "").replace("\x1b[0m", ""),
        plain
    );
}

#[test]
fn failure_domains_without_domains_print_an_empty_table() {
    assert_eq!(
        cli::output::render_failure_domains(&serde_json::json!({}), false),
        "DOMAIN  BROKERS\n"
    );
}

#[test]
fn failure_domains_unexpected_shape_falls_back_to_json() {
    assert_eq!(
        cli::output::render_failure_domains(&serde_json::json!(["a"]), false),
        "[\n  \"a\"\n]\n"
    );
}

#[test]
fn get_failure_domain_wraps_the_single_domain_in_the_shared_table() {
    // `get-failure-domain` returns `{ brokers: [...] }` for one domain; the CLI
    // wraps it under the requested name so the output matches one group of
    // `list-failure-domains`.
    let details = serde_json::json!({"brokers": ["broker-n7:8080", "broker-n2:8080"]});
    let single = serde_json::json!({ "par6": details });
    assert_eq!(
        cli::output::render_failure_domains(&single, false),
        "DOMAIN  BROKERS\n\
         par6    broker-n7:8080\n\
         \x20       broker-n2:8080\n"
    );
}

#[test]
fn persistence_policies_render_quorums_and_disabled_mark_delete_rate() {
    use cli::output::HumanOutput;
    let policies = magnetar_admin::PersistencePolicies {
        bookkeeper_ensemble: 2,
        bookkeeper_write_quorum: 2,
        bookkeeper_ack_quorum: 2,
        managed_ledger_max_mark_delete_rate: 0.0,
    };
    assert_eq!(
        cli::output::render_rows(&policies.human_fields(), false),
        "BOOKKEEPER ENSEMBLE      2\n\
         BOOKKEEPER WRITE QUORUM  2\n\
         BOOKKEEPER ACK QUORUM    2\n\
         MAX MARK-DELETE RATE     disabled\n"
    );
    let throttled = magnetar_admin::PersistencePolicies {
        managed_ledger_max_mark_delete_rate: 1.5,
        ..policies
    };
    assert!(
        cli::output::render_rows(&throttled.human_fields(), false)
            .ends_with("MAX MARK-DELETE RATE     1.5 ops/s\n")
    );
}

#[test]
fn resolved_policy_leads_with_its_source_in_both_formats() {
    use cli::output::{HumanOutput, PolicySource, Resolved};
    let resolved = Resolved {
        source: PolicySource::Broker,
        value: RetentionPolicies {
            retention_time_in_minutes: 0,
            retention_size_in_mb: 0,
        },
    };
    assert_eq!(
        cli::output::render_rows(&resolved.human_fields(), false),
        "SOURCE              broker default (no policy set)\n\
         RETENTION DURATION  0 minutes\n\
         RETENTION SIZE      0 MB\n"
    );
    let json = cli::output::resolved_json(&resolved).expect("json");
    assert_eq!(
        serde_json::to_string(&json).expect("string"),
        r#"{"source":"broker","retentionTimeInMinutes":0,"retentionSizeInMB":0}"#
    );
    for (source, label, name) in [
        (PolicySource::Topic, "topic policy", "topic"),
        (PolicySource::Namespace, "namespace policy", "namespace"),
    ] {
        assert_eq!(source.label(), label);
        assert_eq!(serde_json::to_value(source).expect("json"), name);
    }
}

#[test]
fn broker_defaults_parse_the_string_valued_runtime_configuration() {
    let config = serde_json::json!({
        "defaultRetentionTimeInMinutes": "0",
        "defaultRetentionSizeInMB": "-1",
        "managedLedgerDefaultEnsembleSize": "3",
        "managedLedgerDefaultWriteQuorum": "3",
        "managedLedgerDefaultAckQuorum": "2",
        "managedLedgerDefaultMarkDeleteRateLimit": "1.0",
        "unrelated": "x"
    });
    let retention = cli::output::retention_from_broker(&config).expect("retention");
    assert_eq!(retention.retention_time_in_minutes, 0);
    assert_eq!(retention.retention_size_in_mb, -1);
    let persistence = cli::output::persistence_from_broker(&config).expect("persistence");
    assert_eq!(persistence.bookkeeper_ensemble, 3);
    assert_eq!(persistence.bookkeeper_write_quorum, 3);
    assert_eq!(persistence.bookkeeper_ack_quorum, 2);
    assert!((persistence.managed_ledger_max_mark_delete_rate - 1.0).abs() < f64::EPSILON);
}

#[test]
fn broker_defaults_name_the_missing_or_malformed_key() {
    let missing = serde_json::json!({ "defaultRetentionTimeInMinutes": "0" });
    assert_eq!(
        cli::output::retention_from_broker(&missing).expect_err("missing key"),
        "key `defaultRetentionSizeInMB` is missing from the broker runtime configuration"
    );
    let malformed = serde_json::json!({
        "defaultRetentionTimeInMinutes": "soon",
        "defaultRetentionSizeInMB": "0"
    });
    assert_eq!(
        cli::output::retention_from_broker(&malformed).expect_err("malformed value"),
        "key `defaultRetentionTimeInMinutes` has unexpected value `soon` in the broker runtime configuration"
    );
}

#[test]
fn namespace_of_topic_accepts_schemes_and_rejects_other_shapes() {
    assert_eq!(
        cli::output::namespace_of_topic("persistent://acme/svc/orders").as_deref(),
        Some("acme/svc")
    );
    assert_eq!(
        cli::output::namespace_of_topic("non-persistent://acme/svc/orders").as_deref(),
        Some("acme/svc")
    );
    assert_eq!(
        cli::output::namespace_of_topic("acme/svc/orders").as_deref(),
        Some("acme/svc")
    );
    for bad in [
        "acme/svc",
        "acme/svc/orders/extra",
        "persistent://acme//orders",
        "",
    ] {
        assert!(cli::output::namespace_of_topic(bad).is_none(), "{bad}");
    }
}

#[test]
fn natural_sort_orders_host_numbers_numerically() {
    let brokers: Vec<String> = [
        "broker-n6:8080",
        "broker-n10:8080",
        "broker-n1:8080",
        "broker-n2:8080",
    ]
    .map(str::to_owned)
    .to_vec();
    assert_eq!(
        cli::output::natural_sorted(&brokers),
        [
            "broker-n1:8080",
            "broker-n2:8080",
            "broker-n6:8080",
            "broker-n10:8080"
        ]
    );
    // Leading zeros tie on value and settle on byte order; non-digit text
    // stays in byte order; the empty list is fine.
    let mixed: Vec<String> = ["b", "a10", "a9", "a09", "a"].map(str::to_owned).to_vec();
    assert_eq!(
        cli::output::natural_sorted(&mixed),
        ["a", "a09", "a9", "a10", "b"]
    );
    assert!(cli::output::natural_sorted(&[]).is_empty());
}

#[test]
fn leader_renders_known_keys_first_then_extra_keys_with_derived_labels() {
    let leader = serde_json::json!({
        "serviceUrl": "http://broker-n9:8080",
        "brokerId": "broker-n9:8080",
        "clusterName": "c3",
        "someFlag": true
    });
    assert_eq!(
        cli::output::render_leader(&leader, false),
        "BROKER ID     broker-n9:8080\n\
         SERVICE URL   http://broker-n9:8080\n\
         CLUSTER NAME  c3\n\
         SOME FLAG     true\n"
    );
    assert_eq!(cli::output::label_from_camel("brokerId"), "broker id");
    assert_eq!(cli::output::label_from_camel("id"), "id");
    assert_eq!(cli::output::label_from_camel("TLS"), "t l s");
    assert_eq!(
        cli::output::render_leader(&serde_json::json!("n9"), false),
        "\"n9\"\n"
    );
}

#[test]
fn bookies_render_one_sorted_column_from_either_id_key() {
    let info = serde_json::json!({"bookies": [
        {"bookieId": "bk-n14"},
        {"address": "bk-n2:3181"},
        {"bookieId": "bk-n3"},
        {"other": 1}
    ]});
    assert_eq!(
        cli::output::render_bookies(&info, false),
        "BOOKIE\nbk-n2:3181\nbk-n3\nbk-n14\n{\"other\":1}\n"
    );
    assert_eq!(
        cli::output::render_bookies(&serde_json::json!({"bookies": []}), false),
        "BOOKIE\n"
    );
    assert_eq!(
        cli::output::render_bookies(&serde_json::json!(["bk-n1"]), false),
        "[\n  \"bk-n1\"\n]\n"
    );
}

#[test]
fn racks_info_groups_bookies_by_rack_and_names_blocks_once() {
    let info = serde_json::json!({
        "default": {
            "bk-n10": {"rack": "/par7/K16", "hostname": "bk-n10:3181"},
            "bk-n8":  {"rack": "/par6/N1",  "hostname": "bk-n8:3181"},
            "bk-n14": {"rack": "/par6/N1",  "hostname": "bk-n14:3181"},
            "bk-n9":  {"rack": "/par6/N1"}
        },
        "other": {
            "bk-x": {"hostname": "bk-x:3181"}
        }
    });
    assert_eq!(
        cli::output::render_racks_info(&info, false),
        concat!(
            "AFFINITY GROUP  RACK       BOOKIE  HOSTNAME\n",
            "default         /par6/N1   bk-n8   bk-n8:3181\n",
            "                           bk-n9   —\n",
            "                           bk-n14  bk-n14:3181\n",
            "                /par7/K16  bk-n10  bk-n10:3181\n",
            "other           —          bk-x    bk-x:3181\n",
        )
    );
    assert_eq!(
        cli::output::render_racks_info(&serde_json::json!({}), false),
        "AFFINITY GROUP  RACK  BOOKIE  HOSTNAME\n"
    );
    assert_eq!(
        cli::output::render_racks_info(&serde_json::json!([1]), false),
        "[\n  1\n]\n"
    );
}

fn runtime_config_sample() -> serde_json::Value {
    serde_json::json!({
        "dispatchThrottlingRatePerTopicInMsg": "0",
        "dispatchThrottlingRatePerTopicInByte": "1048576",
        "dispatchThrottlingRateRelativeToPublishRate": "true",
        "dispatchThrottlingRatePerSubscriptionInMsg": "500",
        "dispatchThrottlingRatePerSubscriptionInByte": "0",
        "dispatchThrottlingRatePerReplicatorInMsg": "0",
        "dispatchThrottlingRatePerReplicatorInByte": "0",
        "maxPublishRatePerTopicInMessages": "0",
        "maxPublishRatePerTopicInBytes": "2000",
        "backlogQuotaDefaultLimitBytes": "-1",
        "backlogQuotaDefaultLimitSecond": "3600",
        "backlogQuotaDefaultRetentionPolicy": "producer_request_hold",
        "ttlDurationDefaultInSeconds": "0",
        "brokerDeduplicationEnabled": "false",
        "brokerDeduplicationSnapshotIntervalSeconds": "120",
        "brokerServiceCompactionThresholdInBytes": "0",
        "delayedDeliveryEnabled": "true",
        "delayedDeliveryTickTimeMillis": "1000",
        "maxProducersPerTopic": "200",
        "maxConsumersPerTopic": "0",
        "maxUnackedMessagesPerConsumer": "0",
        "maxUnackedMessagesPerSubscription": "50000"
    })
}

fn rows<T: cli::output::HumanOutput>(policy: &T) -> String {
    cli::output::render_rows(&policy.human_fields(), false)
}

#[test]
fn dispatch_and_publish_rates_render_unlimited_for_non_positive_values() {
    let config = runtime_config_sample();
    let topic = cli::output::topic_dispatch_rate_from_broker(&config).expect("topic rate");
    assert_eq!(
        rows(&topic),
        "MESSAGE RATE              unlimited\n\
         BYTE RATE                 1.05 MB/s\n\
         RATE PERIOD               1 s\n\
         RELATIVE TO PUBLISH RATE  yes\n"
    );
    assert_eq!(
        topic.dispatch_throttling_rate_in_msg, -1,
        "0 in the broker config is a disabled throttle"
    );
    let subscription =
        cli::output::subscription_dispatch_rate_from_broker(&config).expect("subscription rate");
    assert!(rows(&subscription).starts_with("MESSAGE RATE              500 msg/s\n"));
    assert!(rows(&subscription).contains("RELATIVE TO PUBLISH RATE  no\n"));
    let period = magnetar_admin::DispatchRate {
        dispatch_throttling_rate_in_msg: 10,
        dispatch_throttling_rate_in_byte: -1,
        rate_period_in_second: 5,
        relative_to_publish_rate: false,
    };
    assert!(rows(&period).starts_with("MESSAGE RATE              10 msg/5 s\n"));
    let publish = cli::output::publish_rate_from_broker(&config).expect("publish rate");
    assert_eq!(
        rows(&publish),
        "MESSAGE RATE  unlimited\nBYTE RATE     2.00 KB/s\n"
    );
}

#[test]
fn scalar_policies_render_sentinels_and_serialise_under_their_key() {
    use cli::output::{PolicySource, Resolved};
    let cases: Vec<(cli::output::ScalarPolicy, &str, &str)> = vec![
        (
            cli::output::message_ttl(0),
            "MESSAGE TTL  disabled\n",
            r#"{"source":"namespace","messageTTLInSeconds":0}"#,
        ),
        (
            cli::output::message_ttl(90_061),
            "MESSAGE TTL  1 day 1 hour 1 minute 1 second\n",
            r#"{"source":"namespace","messageTTLInSeconds":90061}"#,
        ),
        (
            cli::output::deduplication(true),
            "DEDUPLICATION  enabled\n",
            r#"{"source":"namespace","deduplicationEnabled":true}"#,
        ),
        (
            cli::output::deduplication_snapshot_interval(120),
            "DEDUPLICATION SNAPSHOT INTERVAL  2 minutes\n",
            r#"{"source":"namespace","deduplicationSnapshotIntervalSeconds":120}"#,
        ),
        (
            cli::output::compaction_threshold(0),
            "COMPACTION THRESHOLD  disabled\n",
            r#"{"source":"namespace","compactionThreshold":0}"#,
        ),
        (
            cli::output::compaction_threshold(104_857_600),
            "COMPACTION THRESHOLD  104.86 MB\n",
            r#"{"source":"namespace","compactionThreshold":104857600}"#,
        ),
        (
            cli::output::max_producers_per_topic(0),
            "MAX PRODUCERS PER TOPIC  unlimited\n",
            r#"{"source":"namespace","maxProducersPerTopic":0}"#,
        ),
        (
            cli::output::max_consumers_per_topic(12),
            "MAX CONSUMERS PER TOPIC  12\n",
            r#"{"source":"namespace","maxConsumersPerTopic":12}"#,
        ),
        (
            cli::output::max_unacked_messages_per_consumer(0),
            "MAX UNACKED MESSAGES PER CONSUMER  unlimited\n",
            r#"{"source":"namespace","maxUnackedMessagesPerConsumer":0}"#,
        ),
        (
            cli::output::max_unacked_messages_per_subscription(50_000),
            "MAX UNACKED MESSAGES PER SUBSCRIPTION  50000\n",
            r#"{"source":"namespace","maxUnackedMessagesPerSubscription":50000}"#,
        ),
        (
            cli::output::topic_max_producers(3),
            "MAX PRODUCERS  3\n",
            r#"{"source":"namespace","maxProducers":3}"#,
        ),
        (
            cli::output::topic_max_consumers(0),
            "MAX CONSUMERS  unlimited\n",
            r#"{"source":"namespace","maxConsumers":0}"#,
        ),
    ];
    for (policy, human, json) in cases {
        assert_eq!(rows(&policy), human);
        let resolved = Resolved {
            source: PolicySource::Namespace,
            value: policy,
        };
        assert_eq!(
            serde_json::to_string(&cli::output::resolved_json(&resolved).expect("json"))
                .expect("string"),
            json
        );
    }
}

#[test]
fn broker_scalar_reads_and_wraps_the_runtime_key() {
    let config = runtime_config_sample();
    let ttl = cli::output::broker_scalar("ttlDurationDefaultInSeconds", cli::output::message_ttl)(
        &config,
    )
    .expect("ttl");
    assert_eq!(rows(&ttl), "MESSAGE TTL  disabled\n");
    let producers = cli::output::broker_scalar(
        "maxProducersPerTopic",
        cli::output::max_producers_per_topic,
    )(&config)
    .expect("producers");
    assert_eq!(rows(&producers), "MAX PRODUCERS PER TOPIC  200\n");
    let dedup = cli::output::broker_scalar(
        "brokerDeduplicationEnabled",
        cli::output::deduplication,
    )(&config)
    .expect("dedup");
    assert_eq!(rows(&dedup), "DEDUPLICATION  disabled\n");
    let err =
        cli::output::broker_scalar("nope", cli::output::message_ttl)(&config).expect_err("missing");
    assert!(err.contains("`nope` is missing"), "{err}");
}

#[test]
fn delayed_delivery_renders_state_and_tick() {
    let policy =
        cli::output::delayed_delivery_from_broker(&runtime_config_sample()).expect("delayed");
    assert_eq!(
        rows(&policy),
        "DELAYED DELIVERY  enabled\nTICK TIME         1000 ms\n"
    );
}

#[test]
fn backlog_quotas_detect_unset_maps_and_render_each_type() {
    assert!(cli::output::backlog_quotas(serde_json::json!({})).is_none());
    assert!(cli::output::backlog_quotas(serde_json::json!(null)).is_none());
    let own = cli::output::backlog_quotas(serde_json::json!({
        "destination_storage": {"limitSize": 10_737_418_240_i64, "limitTime": -1, "policy": "producer_exception"},
        "future_type": {"weird": true}
    }))
    .expect("set");
    assert_eq!(
        rows(&own),
        "DESTINATION STORAGE QUOTA  size 10.74 GB, time unlimited, policy producer_exception\n\
         QUOTA                      future_type: size unlimited, time unlimited, policy —\n"
    );
    let default =
        cli::output::backlog_quotas_from_broker(&runtime_config_sample()).expect("broker");
    assert_eq!(
        rows(&default),
        "DESTINATION STORAGE QUOTA  size unlimited, time 1 hour, policy producer_request_hold\n\
         MESSAGE AGE QUOTA          size unlimited, time 1 hour, policy producer_request_hold\n"
    );
    let resolved = cli::output::Resolved {
        source: cli::output::PolicySource::Broker,
        value: default,
    };
    let json = cli::output::resolved_json(&resolved).expect("json");
    assert_eq!(json["source"], "broker");
    assert_eq!(json["message_age"]["limitTime"], 3600);
    assert_eq!(
        json["destination_storage"]["policy"],
        "producer_request_hold"
    );
}

#[test]
fn namespace_message_ttl_falls_back_to_the_broker_with_a_scalar_json_shape() {
    use std::process::Command;
    let config_dir = tempfile::tempdir().expect("tempdir");
    let config = config_dir.path().join("config.yaml");
    std::fs::write(&config, "{}\n").expect("write config");
    for (format, expected) in [
        (
            "human",
            "SOURCE       broker default (no policy set)\nMESSAGE TTL  disabled\n",
        ),
        (
            "json",
            "{\n  \"source\": \"broker\",\n  \"messageTTLInSeconds\": 0\n}\n",
        ),
    ] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address");
        let server = serve_in_order(
            listener,
            vec![
                ("/admin/v2/namespaces/public/default/messageTTL", 204, ""),
                (
                    "/admin/v2/brokers/configuration/runtime",
                    200,
                    r#"{"ttlDurationDefaultInSeconds":"0"}"#,
                ),
            ],
        );
        let result = Command::new(env!("CARGO_BIN_EXE_magnetarctl"))
            .env_remove("MAGNETAR_FORMAT")
            .env_remove("MAGNETAR_CONTEXT")
            .env_remove("MAGNETAR_TOKEN")
            .env("NO_COLOR", "1")
            .arg("--config")
            .arg(&config)
            .arg("--admin-url")
            .arg(format!("http://{address}"))
            .args([
                "admin",
                "namespaces",
                "get-message-ttl",
                "public/default",
                "-F",
                format,
            ])
            .output()
            .expect("run magnetarctl");
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        server.join().expect("server");
        assert_eq!(String::from_utf8(result.stdout).expect("UTF-8"), expected);
    }
}
