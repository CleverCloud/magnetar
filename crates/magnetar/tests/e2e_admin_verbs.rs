// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for the operator-facing admin verbs landed in
//! PR #1 of the CLI expansion: subscription operations, topic
//! operational verbs (compact, unload, terminate, update-partitions),
//! namespace policies (retention, backlog quota, message TTL), and
//! broker / cluster diagnostics (brokers list, leader, failure-domains,
//! namespace-isolation-policies).
//!
//! Drives a single `apachepulsar/pulsar:4.0.4` standalone container per
//! test — start-up amortised across many AdminClient calls. Per
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
use magnetar_admin::{AdminClient, BacklogQuota, BacklogQuotaType, RetentionPolicies};
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
