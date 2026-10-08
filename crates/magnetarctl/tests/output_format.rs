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
                "RETENTION DURATION  366 days\nRETENTION SIZE      ∞\n"
            );
        } else {
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&stdout).expect("JSON"),
                serde_json::json!({"retentionTimeInMinutes": 527_040, "retentionSizeInMB": -1})
            );
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
