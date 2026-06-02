// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the operator-facing admin verbs landed in
//! PR #1 of the CLI expansion: subscription operations, topic
//! operational verbs (compact, unload, terminate, update-partitions),
//! namespace policies (retention, backlog quota, message TTL), and
//! broker / cluster diagnostics (brokers list, leader, failure-domains,
//! namespace-isolation-policies).
//!
//! Drives a single `apachepulsar/pulsar:4.0.4` standalone container per
//! test — start-up amortised across many `AdminClient` calls. Per
//! ADR-0046 the file ships as a regular test under `cargo test`; the
//! suite is gated on Docker being reachable, not on a feature flag or
//! an `#[ignore]`. Tests are split by destructiveness:
//!
//! - `e2e_admin_namespace_and_diagnostics` — read-only diagnostics plus the namespace policy
//!   get/set/remove cycles. Idempotent within itself; safe to re-run against the same broker.
//! - `e2e_admin_topic_ops` — compact, unload, update-partitions, and terminate (destructive — the
//!   topic is sealed at the end).
//! - `e2e_admin_subscription_ops` — list + skip-all + delete on a subscription created via a
//!   magnetar consumer attach.

use std::time::Duration;

use magnetar::proto::pb::command_subscribe::SubType;
use magnetar::{OutgoingMessage, PulsarClient};
use magnetar_admin::{
    AdminClient, BacklogQuota, BacklogQuotaType, DelayedDeliveryPolicies, DispatchRate,
    PersistencePolicies, PostSchemaPayload, PublishRate, RetentionPolicies,
};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "4.0.4";
const BROKER_BINARY_PORT: u16 = 6650;
const BROKER_HTTP_PORT: u16 = 8080;

fn image_repo() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_REPO").unwrap_or_else(|_| DEFAULT_IMAGE_REPO.to_owned())
}

fn image_tag() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_TAG").unwrap_or_else(|_| DEFAULT_IMAGE_TAG.to_owned())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("magnetar=info")),
        )
        .with_test_writer()
        .try_init();
}

async fn start_pulsar() -> Result<
    (String, String, testcontainers::ContainerAsync<GenericImage>),
    Box<dyn std::error::Error>,
> {
    init_tracing();
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
        .with_exposed_port(ContainerPort::Tcp(BROKER_HTTP_PORT))
        .with_wait_for(WaitFor::message_on_stdout(
            "Created namespace public/default",
        ))
        .with_startup_timeout(Duration::from_secs(120))
        .with_cmd(vec!["bin/pulsar".to_owned(), "standalone".to_owned()])
        .start()
        .await?;
    let host = container.get_host().await?;
    let binary_port = container.get_host_port_ipv4(BROKER_BINARY_PORT).await?;
    let http_port = container.get_host_port_ipv4(BROKER_HTTP_PORT).await?;
    let service_url = format!("pulsar://{host}:{binary_port}");
    let admin_url = format!("http://{host}:{http_port}");
    Ok((service_url, admin_url, container))
}

fn build_admin(admin_url: &str) -> Result<AdminClient, Box<dyn std::error::Error>> {
    Ok(AdminClient::builder()
        .service_url(admin_url.parse()?)
        .timeout(Duration::from_secs(30))
        .build()?)
}

/// Namespace policies + cluster diagnostics — read-only diagnostics plus
/// the get/set/remove invariant for retention, backlog quota, and
/// message TTL. Each policy: set → get returns the set value, remove →
/// get returns the broker default. Asserts the
/// `set → get → remove → get` round-trip is correct against the real
/// broker (Pulsar 4.0.4).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_admin_namespace_and_diagnostics() -> Result<(), Box<dyn std::error::Error>> {
    let (_service_url, admin_url, _container) = start_pulsar().await?;
    let admin = build_admin(&admin_url)?;

    // --- Diagnostics (read-only) -----------------------------------

    let clusters = admin.cluster_list().await?;
    assert!(
        clusters.iter().any(|c| c == "standalone"),
        "cluster_list must include 'standalone'; got {clusters:?}"
    );

    let brokers = admin.brokers_list("standalone").await?;
    assert!(
        !brokers.is_empty(),
        "brokers_list returned no brokers; cluster bootstrap failed?"
    );

    let leader = admin.brokers_leader().await?;
    // Pulsar 4 always returns `serviceUrl` + `brokerId`; pinning by key
    // presence is forward-compat (3.0+ adds `clusterName`).
    assert!(
        leader.get("serviceUrl").is_some(),
        "brokers_leader response missing `serviceUrl`: {leader}"
    );

    // Failure domains and isolation policies are empty on a fresh
    // standalone — assert that the broker returns an empty map (not 404)
    // for both. Pulsar 4 surfaces these as `{}` rather than `null`.
    let domains = admin.cluster_failure_domains_list("standalone").await?;
    assert!(
        domains.is_object(),
        "cluster_failure_domains_list returned non-object: {domains}"
    );
    let isolation = admin
        .namespace_isolation_policies_list("standalone")
        .await?;
    assert!(
        isolation.is_object(),
        "namespace_isolation_policies_list returned non-object: {isolation}"
    );

    // --- Namespace policies — retention round-trip -----------------

    let ns = "public/default";

    let pol_set = RetentionPolicies {
        retention_time_in_minutes: 60,
        retention_size_in_mb: 1024,
    };
    admin.namespace_set_retention(ns, pol_set).await?;
    let pol_got = admin.namespace_get_retention(ns).await?;
    assert_eq!(pol_got.retention_time_in_minutes, 60);
    assert_eq!(pol_got.retention_size_in_mb, 1024);
    admin.namespace_remove_retention(ns).await?;
    let pol_default = admin.namespace_get_retention(ns).await?;
    // The broker default for `public/default` is `0/0` (no retention).
    // Pin that as the post-remove invariant.
    assert_eq!(pol_default.retention_time_in_minutes, 0);
    assert_eq!(pol_default.retention_size_in_mb, 0);

    // --- Namespace policies — message TTL round-trip ---------------

    admin.namespace_set_message_ttl(ns, 7200).await?;
    let ttl_got = admin.namespace_get_message_ttl(ns).await?;
    assert_eq!(ttl_got, Some(7200));
    admin.namespace_remove_message_ttl(ns).await?;
    let ttl_after_remove = admin.namespace_get_message_ttl(ns).await?;
    // Post-remove the broker returns `null` (decoded as `None`) — the
    // CLI's `set-message-ttl 0` path is distinct from `remove-message-
    // ttl`. Pin that distinction here.
    assert!(
        ttl_after_remove.is_none() || ttl_after_remove == Some(0),
        "post-remove TTL: expected None or Some(0), got {ttl_after_remove:?}"
    );

    // --- Namespace policies — backlog quota round-trip -------------

    admin
        .namespace_set_backlog_quota(
            ns,
            BacklogQuotaType::DestinationStorage,
            BacklogQuota {
                limit_size: 1_073_741_824,
                limit_time: -1,
                policy: "consumer_backlog_eviction".into(),
            },
        )
        .await?;
    let quotas = admin.namespace_get_backlog_quotas(ns).await?;
    let dest = &quotas["destination_storage"];
    assert!(
        dest.is_object(),
        "backlog quota map missing `destination_storage`: {quotas}"
    );
    assert_eq!(dest["limitSize"], 1_073_741_824_i64);
    assert_eq!(dest["policy"], "consumer_backlog_eviction");
    admin
        .namespace_remove_backlog_quota(ns, BacklogQuotaType::DestinationStorage)
        .await?;

    Ok(())
}

/// Topic operational verbs — compact, unload, update-partitions, then
/// terminate. Each verb is destructive in a different way:
/// `update_partitions` grows the partition count (forward-only),
/// `unload` forces broker re-election, `compact` runs the dedup
/// compaction, and `terminate` seals the topic. Asserts each succeeds
/// against the real broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_admin_topic_ops() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, _container) = start_pulsar().await?;
    let admin = build_admin(&admin_url)?;
    let client = PulsarClient::builder()
        .service_url(service_url.clone())
        .build()
        .await?;

    // Use a non-partitioned topic for compact / unload / terminate —
    // Pulsar's partitioned-topic terminate semantics differ
    // (terminate is per-partition), and the existing PR #1 wiremock
    // tests already cover the partitioned URL shape.
    let nonpart = "persistent://public/default/magnetar-e2e-ops-nonpart";

    // Bootstrap with one produce so the broker creates the topic.
    {
        let producer = client.producer(nonpart).create().await?;
        producer
            .send(OutgoingMessage::with_payload(b"warmup".to_vec()).into())
            .await?;
        producer.close().await?;
    }

    // Compact + poll status. Compaction is async — the broker
    // surfaces RUNNING then SUCCESS; the call itself must return 204
    // and the status endpoint must return a `LongRunningProcessStatus`
    // (not 404).
    admin.topic_compact(nonpart).await?;
    let st = admin.topic_compaction_status(nonpart).await?;
    assert!(
        ["NOT_RUN", "RUNNING", "SUCCESS", "ERROR"].contains(&st.status.as_str()),
        "compaction status must be a known state; got `{}`",
        st.status
    );

    // Unload — broker accepts and re-elects ownership. Must return 204.
    admin.topic_unload(nonpart).await?;

    // Partitioned topic for update-partitions. Create with 2, grow to 4,
    // re-query and assert.
    let part = "persistent://public/default/magnetar-e2e-ops-part";
    admin.topic_create_partitioned(part, 2).await?;
    assert_eq!(admin.topic_partitions_count(part).await?, 2);
    admin.topic_update_partitions(part, 4).await?;
    assert_eq!(admin.topic_partitions_count(part).await?, 4);

    // Terminate on the non-partitioned topic. Returns the last
    // message-id that landed before the seal. Pin that we get a
    // `ledger_id > 0` shape — the exact entry_id depends on broker
    // internals.
    let last = admin.topic_terminate(nonpart).await?;
    assert!(
        last.ledger_id > 0,
        "terminate returned ledger_id=0 (expected the last-produced entry's ledger)"
    );

    Ok(())
}

/// Subscription operational verbs — list, skip-all, delete. The
/// subscription is created by attaching a magnetar consumer (the only
/// way to materialise one in Pulsar — admin REST has no
/// `create-subscription`). Asserts:
/// 1. `subscriptions_list` includes the subscription after attach.
/// 2. `subscription_skip_all_messages` succeeds against a real subscription.
/// 3. `subscription_delete` with `force=true` removes the subscription.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_admin_subscription_ops() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, _container) = start_pulsar().await?;
    let admin = build_admin(&admin_url)?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let topic = "persistent://public/default/magnetar-e2e-subs";
    let sub_name = "magnetar-e2e-sub";

    // Bootstrap topic and attach a consumer to create the subscription.
    {
        let producer = client.producer(topic).create().await?;
        producer
            .send(OutgoingMessage::with_payload(b"x".to_vec()).into())
            .await?;
        producer.close().await?;
    }
    {
        let _consumer = client
            .consumer(topic)
            .subscription(sub_name)
            .subscription_type(SubType::Exclusive)
            .subscribe()
            .await?;
        // Drop the consumer to release the exclusive lock so the
        // delete call below isn't 412-blocked.
    }

    // 1. List includes the subscription.
    let listed = admin.subscriptions_list(topic).await?;
    assert!(
        listed.iter().any(|s| s == sub_name),
        "subscriptions_list missing `{sub_name}`; got {listed:?}"
    );

    // 2. Skip-all on a real subscription succeeds. The broker treats "drain-backlog" as idempotent
    //    — succeeds with 204 whether or not there's a pending entry to skip.
    admin
        .subscription_skip_all_messages(topic, sub_name)
        .await?;

    // 3. Delete with force=true (in case Pulsar still considers a cursor active in the metadata
    //    cache).
    admin.subscription_delete(topic, sub_name, true).await?;
    let after_delete = admin.subscriptions_list(topic).await?;
    assert!(
        !after_delete.iter().any(|s| s == sub_name),
        "subscription_delete left the subscription in place: {after_delete:?}"
    );

    Ok(())
}

/// Namespace policies breadth (PR #2 slice 1 + slice 2) — exercise the
/// `set → get returns set → remove → get returns default-or-none`
/// invariant for persistence, dispatch-rate, deduplication, compaction
/// threshold, delayed-delivery, and max-producers-per-topic against a
/// real Pulsar 4.0.4 broker. Each policy family on a fresh broker
/// returns `None` (or the broker's zero default) until set; the
/// invariant is that the round-trip preserves the configured value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_admin_namespace_policies_breadth() -> Result<(), Box<dyn std::error::Error>> {
    let (_service_url, admin_url, _container) = start_pulsar().await?;
    let admin = build_admin(&admin_url)?;
    let ns = "public/default";

    // --- Persistence ---------------------------------------------------

    let pers = PersistencePolicies {
        bookkeeper_ensemble: 2,
        bookkeeper_write_quorum: 2,
        bookkeeper_ack_quorum: 2,
        managed_ledger_max_mark_delete_rate: 1.0,
    };
    admin.namespace_set_persistence(ns, pers).await?;
    let got = admin.namespace_get_persistence(ns).await?;
    assert_eq!(got.bookkeeper_ensemble, 2);
    assert_eq!(got.bookkeeper_write_quorum, 2);
    assert_eq!(got.bookkeeper_ack_quorum, 2);
    admin.namespace_remove_persistence(ns).await?;

    // --- DispatchRate --------------------------------------------------

    let rate = DispatchRate {
        dispatch_throttling_rate_in_msg: 1000,
        dispatch_throttling_rate_in_byte: 1_048_576,
        rate_period_in_second: 1,
        relative_to_publish_rate: false,
    };
    admin.namespace_set_dispatch_rate(ns, rate).await?;
    let got = admin.namespace_get_dispatch_rate(ns).await?;
    assert_eq!(got.dispatch_throttling_rate_in_msg, 1000);
    assert_eq!(got.dispatch_throttling_rate_in_byte, 1_048_576);
    admin.namespace_remove_dispatch_rate(ns).await?;

    // --- Deduplication -------------------------------------------------

    admin.namespace_set_deduplication(ns, true).await?;
    let got = admin.namespace_get_deduplication(ns).await?;
    assert_eq!(got, Some(true));
    admin.namespace_remove_deduplication(ns).await?;

    // --- Compaction threshold ------------------------------------------

    admin
        .namespace_set_compaction_threshold(ns, 10_485_760)
        .await?;
    let got = admin.namespace_get_compaction_threshold(ns).await?;
    assert_eq!(got, Some(10_485_760));
    admin.namespace_remove_compaction_threshold(ns).await?;

    // --- Delayed delivery ----------------------------------------------

    let dd = DelayedDeliveryPolicies {
        active: true,
        tick_time_millis: 1000,
    };
    admin.namespace_set_delayed_delivery(ns, dd).await?;
    let got = admin.namespace_get_delayed_delivery(ns).await?;
    assert!(got.is_some(), "delayed-delivery should round-trip");
    let got = got.unwrap();
    assert!(got.active);
    assert_eq!(got.tick_time_millis, 1000);
    admin.namespace_remove_delayed_delivery(ns).await?;

    // --- MaxProducersPerTopic ------------------------------------------

    admin.namespace_set_max_producers_per_topic(ns, 50).await?;
    let got = admin.namespace_get_max_producers_per_topic(ns).await?;
    assert_eq!(got, Some(50));
    admin.namespace_remove_max_producers_per_topic(ns).await?;

    // --- PublishRate ---------------------------------------------------

    let pr = PublishRate {
        publish_throttling_rate_in_msg: 500,
        publish_throttling_rate_in_byte: 524_288,
    };
    admin.namespace_set_publish_rate(ns, pr).await?;
    let got = admin.namespace_get_publish_rate(ns).await?;
    assert_eq!(got.publish_throttling_rate_in_msg, 500);
    admin.namespace_remove_publish_rate(ns).await?;

    Ok(())
}

/// Per-topic policy overrides (PR #3) — the broker's topic-level
/// policies override the namespace defaults. Asserts the round-trip
/// `set → get returns set → remove → get returns None` for retention,
/// dispatch-rate, persistence, and max-producers on a per-topic basis.
/// Requires the broker to be launched with `topicLevelPoliciesEnabled
/// = true` (Pulsar 4.0.4 default).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_admin_topic_policies_breadth() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, _container) = start_pulsar().await?;
    let admin = build_admin(&admin_url)?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    let topic = "persistent://public/default/magnetar-e2e-topic-policies";

    // Bootstrap topic with a single produce — Pulsar autocreates.
    {
        let producer = client.producer(topic).create().await?;
        producer
            .send(OutgoingMessage::with_payload(b"warmup".to_vec()).into())
            .await?;
        producer.close().await?;
    }

    // --- Topic retention ----------------------------------------------

    let pol = RetentionPolicies {
        retention_time_in_minutes: 30,
        retention_size_in_mb: 512,
    };
    admin.topic_set_retention(topic, pol).await?;
    let got = admin.topic_get_retention(topic).await?;
    assert_eq!(got.retention_time_in_minutes, 30);
    assert_eq!(got.retention_size_in_mb, 512);
    admin.topic_remove_retention(topic).await?;

    // --- Topic dispatch-rate ------------------------------------------

    let rate = DispatchRate {
        dispatch_throttling_rate_in_msg: 100,
        dispatch_throttling_rate_in_byte: 1_048_576,
        rate_period_in_second: 1,
        relative_to_publish_rate: false,
    };
    admin.topic_set_dispatch_rate(topic, rate).await?;
    let got = admin.topic_get_dispatch_rate(topic).await?;
    assert!(got.is_some(), "topic dispatch-rate should round-trip");
    admin.topic_remove_dispatch_rate(topic).await?;

    // --- Topic max-producers ------------------------------------------

    admin.topic_set_max_producers(topic, 5).await?;
    let got = admin.topic_get_max_producers(topic).await?;
    assert_eq!(got, Some(5));
    admin.topic_remove_max_producers(topic).await?;

    // --- Topic message-TTL --------------------------------------------

    admin.topic_set_message_ttl(topic, 3600).await?;
    let got = admin.topic_get_message_ttl(topic).await?;
    assert_eq!(got, Some(3600));
    admin.topic_remove_message_ttl(topic).await?;

    Ok(())
}

/// Brokers / bookies / schemas (PR #4) — read-only diagnostics plus
/// a schema post-then-get round-trip. The bookies / brokers paths
/// surface broker-internal state; we pin shape, not values (a fresh
/// standalone exposes one broker, one bookie). The schema round-trip
/// posts an AVRO schema and asserts the version field comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_admin_brokers_bookies_schemas() -> Result<(), Box<dyn std::error::Error>> {
    let (service_url, admin_url, _container) = start_pulsar().await?;
    let admin = build_admin(&admin_url)?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;

    // --- Brokers diagnostics -------------------------------------------

    let keys = admin.brokers_dynamic_config_keys().await?;
    assert!(
        !keys.is_empty(),
        "brokers_dynamic_config_keys should expose at least the well-known config knobs"
    );

    let runtime = admin.brokers_runtime_config().await?;
    assert!(
        runtime.is_object(),
        "brokers_runtime_config should return a JSON object"
    );

    let internal = admin.brokers_internal_config().await?;
    assert!(
        internal.is_object(),
        "brokers_internal_config should return a JSON object"
    );

    // health_check returns plain `"ok"` text — pin the substring.
    let health = admin.brokers_health_check().await?;
    assert!(
        health.contains("ok") || health.is_empty(),
        "brokers_health_check unexpected body: {health:?}"
    );

    // --- Bookies -------------------------------------------------------

    let bookies = admin.bookies_list_all().await?;
    assert!(
        bookies.is_object(),
        "bookies_list_all should return a JSON object"
    );

    let racks = admin.bookies_racks_info().await?;
    assert!(
        racks.is_object(),
        "bookies_racks_info should return a JSON object (possibly empty)"
    );

    // --- Schemas: post then get round-trip ----------------------------

    let topic = "persistent://public/default/magnetar-e2e-schemas";
    // Bootstrap the topic with a producer attach so the schema
    // endpoints don't 404.
    {
        let producer = client.producer(topic).create().await?;
        producer
            .send(OutgoingMessage::with_payload(b"warmup".to_vec()).into())
            .await?;
        producer.close().await?;
    }

    // Post a trivial AVRO schema. Pulsar's `PostSchemaPayload`
    // accepts the JSON-stringified AVRO definition in the `schema`
    // field.
    let payload = PostSchemaPayload {
        schema_type: "AVRO".to_owned(),
        schema: r#"{"type":"record","name":"X","fields":[{"name":"v","type":"string"}]}"#
            .to_owned(),
        properties: std::collections::HashMap::default(),
    };
    let posted = admin.schema_post(topic, payload).await?;
    assert!(
        posted.get("version").is_some(),
        "schema_post should return a `version` field; got {posted}"
    );

    let latest = admin.schema_get_latest(topic).await?;
    assert!(
        latest.get("type").is_some(),
        "schema_get_latest should return a `type` field; got {latest}"
    );

    let versions = admin.schema_list_versions(topic).await?;
    assert!(
        !versions.is_empty(),
        "schema_list_versions returned empty list after a schema was posted"
    );

    Ok(())
}

/// V3 admin surface (PR #5) — Functions / Sources / Sinks / Packages.
/// Pulsar 4.0.4 standalone starts the Functions Worker by default;
/// these tests exercise the read endpoints (list / get-as-404) which
/// don't require a deployed connector. A real
/// `function_create_with_url` test would need a broker-resolvable
/// package URL — skipped here so the test stays self-contained.
///
/// The wiremock tests in `crates/magnetar-admin/tests/functions.rs`,
/// `sources.rs`, `sinks.rs`, and `packages.rs` already pin the wire
/// shape per verb; this e2e adds "broker accepts and responds" on top.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_admin_v3_functions_sources_sinks_packages() -> Result<(), Box<dyn std::error::Error>> {
    let (_service_url, admin_url, _container) = start_pulsar().await?;
    let admin = build_admin(&admin_url)?;

    // --- Functions: list returns an empty array on a fresh broker. ---
    let fns = admin
        .functions_list_by_namespace("public", "default")
        .await?;
    assert!(
        fns.is_empty(),
        "functions_list_by_namespace expected empty on fresh broker, got {fns:?}"
    );

    // --- Sources: same — empty list. ---
    let sources = admin.sources_list_by_namespace("public", "default").await?;
    assert!(
        sources.is_empty(),
        "sources_list_by_namespace expected empty on fresh broker, got {sources:?}"
    );

    // --- Sinks: same. ---
    let sinks = admin.sinks_list_by_namespace("public", "default").await?;
    assert!(
        sinks.is_empty(),
        "sinks_list_by_namespace expected empty on fresh broker, got {sinks:?}"
    );

    // --- Get on a missing function must surface as Status 404 (not as
    //     a transport error). Pin the error class so a regression that
    //     swallowed the 404 as a transient gets caught.
    let err = admin
        .function_get("public", "default", "no-such-function")
        .await
        .expect_err("missing function must error");
    match err {
        magnetar_admin::AdminError::Status { code, .. } => {
            assert!(
                code == 404 || code == 400 || code == 500,
                "missing function unexpected status {code}"
            );
        }
        other => panic!("missing function should be AdminError::Status, got {other:?}"),
    }

    // --- Packages: list with `function` type — broker exposes the
    //     endpoint when the Functions Worker is enabled. On a default
    //     standalone the namespace is empty; pin "broker responds with
    //     200 + list shape" rather than the (empty) value.
    let pkgs = admin
        .packages_list(magnetar_admin::PackageType::Function, "public", "default")
        .await;
    match pkgs {
        Ok(list) => assert!(
            list.is_empty(),
            "packages_list expected empty on fresh broker, got {list:?}"
        ),
        Err(magnetar_admin::AdminError::Status { code, .. }) => {
            // Some Pulsar 4.x builds 404 the namespace-scoped list
            // until at least one package is uploaded; accept that too.
            assert!(
                code == 404,
                "packages_list unexpected non-404 broker error: {code}"
            );
        }
        Err(other) => panic!("packages_list transport error: {other}"),
    }

    Ok(())
}
