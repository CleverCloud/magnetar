// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the topic operational REST endpoints — stats,
//! compact, compaction-status, unload, terminate, update-partitions.
//!
//! These pin the exact path, verb, query parameter, and JSON body shape
//! against `pulsar-broker/.../v2/PersistentTopics.java`
//! (`getStats`, `getPartitionedStats`, `triggerCompaction`,
//! `compactionStatus`, `unloadTopic`, `terminate`,
//! `updatePartitionedTopic`).

use magnetar_admin::{AdminClient, AdminError, LongRunningProcessStatus};
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn topic_stats_decodes_rates_throughput_and_sizes() {
    // `getStats` returns the large `PersistentTopicStats` envelope. We pin
    // the high-signal metrics we surface — the in/out rates, both throughput
    // dimensions, average message size, the in counters, and the storage /
    // backlog sizes — and let the rest pass through.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/persistent/public/default/orders/stats"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "msgRateIn": 42.5,
            "msgRateOut": 17.25,
            "msgThroughputIn": 5_000.0,
            "msgThroughputOut": 2_048.0,
            "averageMsgSize": 512.0,
            "msgInCounter": 1000,
            "bytesInCounter": 64_000,
            "storageSize": 1_048_576,
            "backlogSize": 4_096,
            "publishers": [],
            "subscriptions": {},
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let stats = admin
        .topic_stats("public/default/orders")
        .await
        .expect("stats returns 200");
    assert!((stats.msg_rate_in - 42.5).abs() < f64::EPSILON);
    assert!((stats.msg_rate_out - 17.25).abs() < f64::EPSILON);
    assert!((stats.msg_throughput_in - 5_000.0).abs() < f64::EPSILON);
    assert!((stats.msg_throughput_out - 2_048.0).abs() < f64::EPSILON);
    assert!((stats.average_msg_size - 512.0).abs() < f64::EPSILON);
    assert_eq!(stats.msg_in_counter, 1000);
    assert_eq!(stats.bytes_in_counter, 64_000);
    assert_eq!(stats.storage_size, 1_048_576);
    assert_eq!(stats.backlog_size, 4_096);
}

#[tokio::test]
async fn topic_stats_scalar_fields_default_to_zero_when_absent() {
    // The struct is permissive: a broker release that omits any of the
    // rate / throughput / size fields must still decode, defaulting them to
    // 0 rather than failing the whole stats call.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/admin/v2/persistent/public/default/idle/stats"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "publishers": [],
            "subscriptions": {},
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let stats = admin
        .topic_stats("public/default/idle")
        .await
        .expect("stats returns 200");
    assert!(stats.msg_rate_in.abs() < f64::EPSILON);
    assert!(stats.msg_rate_out.abs() < f64::EPSILON);
    assert!(stats.msg_throughput_in.abs() < f64::EPSILON);
    assert!(stats.msg_throughput_out.abs() < f64::EPSILON);
    assert!(stats.average_msg_size.abs() < f64::EPSILON);
    assert_eq!(stats.msg_in_counter, 0);
    assert_eq!(stats.bytes_in_counter, 0);
    assert_eq!(stats.storage_size, 0);
    assert_eq!(stats.backlog_size, 0);
}

#[tokio::test]
async fn topic_partitioned_stats_decodes_aggregated_metrics() {
    // The partitioned endpoint returns the same `TopicStats` shape with the
    // metrics summed across partitions; `perPartition=false` keeps it compact.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/partitioned-stats",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "msgRateIn": 128.0,
            "msgRateOut": 64.0,
            "msgThroughputIn": 16_384.0,
            "msgThroughputOut": 8_192.0,
            "averageMsgSize": 256.0,
            "msgInCounter": 4000,
            "bytesInCounter": 256_000,
            "storageSize": 4_194_304,
            "backlogSize": 16_384,
            "publishers": [],
            "subscriptions": {},
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let stats = admin
        .topic_partitioned_stats("public/default/orders")
        .await
        .expect("partitioned-stats returns 200");
    assert!((stats.msg_rate_in - 128.0).abs() < f64::EPSILON);
    assert!((stats.msg_rate_out - 64.0).abs() < f64::EPSILON);
    assert_eq!(stats.msg_in_counter, 4000);
    assert_eq!(stats.storage_size, 4_194_304);
    assert_eq!(stats.backlog_size, 16_384);
}

#[tokio::test]
async fn topic_compact_puts_to_compaction_path() {
    let mock = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/compaction",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .topic_compact("public/default/orders")
        .await
        .expect("compaction trigger returns 204");
}

#[tokio::test]
async fn topic_compaction_status_decodes_long_running_process_status() {
    let mock = MockServer::start().await;
    // `LongRunningProcessStatus` is camelCase on the wire — `lastError`.
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/compaction",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "status": "RUNNING",
            "lastError": "",
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let st: LongRunningProcessStatus = admin
        .topic_compaction_status("public/default/orders")
        .await
        .expect("status returns 200");
    assert_eq!(st.status, "RUNNING");
    assert!(st.last_error.is_empty());
}

#[tokio::test]
async fn topic_unload_puts_to_unload_path() {
    let mock = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/admin/v2/persistent/public/default/orders/unload"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .topic_unload("public/default/orders")
        .await
        .expect("unload returns 204");
}

#[tokio::test]
async fn topic_terminate_posts_and_returns_last_message_id() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/admin/v2/persistent/public/default/orders/terminate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ledgerId": 123,
            "entryId": 456,
            "partitionIndex": -1,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let last = admin
        .topic_terminate("public/default/orders")
        .await
        .expect("terminate returns the last message id")
        .expect("non-sentinel ledgerId/entryId → Some");
    assert_eq!(last.ledger_id, 123);
    assert_eq!(last.entry_id, 456);
    assert_eq!(last.partition, -1);
}

#[tokio::test]
async fn topic_terminate_sentinel_negative_one_is_none() {
    // A topic terminated before any entry was confirmed returns
    // `MessageIdImpl(-1, -1, -1)` on the wire. We surface that as
    // `None` rather than failing with `Protocol("negative entryId")` —
    // freshly-created or just-unloaded topics legitimately hit this.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/admin/v2/persistent/public/default/empty/terminate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ledgerId": -1,
            "entryId": -1,
            "partitionIndex": -1,
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let last = admin
        .topic_terminate("public/default/empty")
        .await
        .expect("terminate sentinel must not surface as Protocol error");
    assert!(last.is_none(), "sentinel (-1, -1) should map to None");
}

#[tokio::test]
async fn topic_update_partitions_posts_bare_integer_body() {
    let mock = MockServer::start().await;
    // Pulsar accepts a bare JSON integer as the body (not an envelope
    // object) — `int newPartitions` in `updatePartitionedTopic`.
    Mock::given(method("POST"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/partitions",
        ))
        .and(body_json(serde_json::json!(8)))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .topic_update_partitions("public/default/orders", 8)
        .await
        .expect("update-partitions returns 204");
}

#[tokio::test]
async fn topic_update_partitions_propagates_409_on_shrink() {
    // Broker rejects shrink with 409; the call site sees `AdminError::Status`.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/partitions",
        ))
        .respond_with(
            ResponseTemplate::new(409)
                .set_body_string("Number of partitions can only be increased"),
        )
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let err = admin
        .topic_update_partitions("public/default/orders", 2)
        .await
        .unwrap_err();
    assert!(matches!(err, AdminError::Status { code: 409, .. }));
}

#[tokio::test]
async fn topic_delete_auto_detects_partitioned_route() {
    // Pulsar exposes two distinct delete endpoints; the partitioned
    // parent at `…/{topic}/partitions?force=…` and the non-partitioned
    // topic at `…/{topic}?force=…`. The client probes
    // `topic_partitions_count` (a `GET .../partitions` returning
    // `partitions: N`) and routes accordingly. Pinned for both shapes.
    let mock = MockServer::start().await;
    // Probe: partitioned → 4 partitions.
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/partitions",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "partitions": 4,
        })))
        .expect(1)
        .mount(&mock)
        .await;
    // Route → partitioned delete endpoint (`/partitions`).
    Mock::given(method("DELETE"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/partitions",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap();
    admin
        .topic_delete("public/default/orders", false)
        .await
        .expect("partitioned delete returns 204");
}

#[tokio::test]
async fn topic_delete_auto_detects_non_partitioned_route() {
    let mock = MockServer::start().await;
    // Probe: non-partitioned → `partitions: 0`.
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/public/default/oneoff/partitions",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "partitions": 0,
        })))
        .expect(1)
        .mount(&mock)
        .await;
    // Route → bare topic endpoint (no `/partitions` suffix).
    Mock::given(method("DELETE"))
        .and(path("/admin/v2/persistent/public/default/oneoff"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap();
    admin
        .topic_delete("public/default/oneoff", true)
        .await
        .expect("non-partitioned delete returns 204");
}
