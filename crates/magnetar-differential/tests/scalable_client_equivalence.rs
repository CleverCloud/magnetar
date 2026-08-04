// SPDX-License-Identifier: Apache-2.0

//! PIP-460 / ADR-0093 differential equivalence at the **client** surface.
//!
//! **Experimental.** The sibling `scalable_topic_equivalence.rs` drives the
//! sans-io `Connection` directly; this file drives each engine's real `Client`
//! over a real socket against the shared
//! [`ScriptedBroker`](magnetar_differential::broker::ScriptedBroker), so the
//! whole path is exercised — connection negotiation, the driver's event drain,
//! the per-client buffer, and the async client API — and both engines must
//! observe the same thing.
//!
//! That matters beyond parity: the layout session, the consumer registration
//! and the namespace watch each cross three layers before reaching the caller,
//! and only a client-level test proves the driver actually forwards them.
//!
//! The scripted broker advertises `supports_scalable_topics` and
//! `supports_tc_metadata_discovery`, without which the client refuses to emit
//! any of these commands at all (the v4-compatibility gate, ADR-0093 §D3).

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]
#![allow(clippy::doc_markdown)]

use magnetar_differential::HANG_GUARD;
use magnetar_differential::broker::ScriptedBroker;
use magnetar_proto::{ConnectionConfig, ScalableConsumerType};
use magnetar_runtime_moonpool::{Client as MoonpoolClient, MoonpoolEngine};
use magnetar_runtime_tokio::Client as TokioClient;
use moonpool_core::TokioProviders;

/// A normalised, engine-independent description of one scalable exchange.
/// Compared across engines rather than the raw types, so a difference reads as
/// a diff of what the caller actually observes.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    resolved_topic_name: Option<String>,
    controller_broker_url: Option<String>,
    layout_epoch: u64,
    segments: Vec<(u64, u32, u32, Option<String>)>,
    assignment_epoch: u64,
    assignment_topics: Vec<String>,
    topics_watch: Vec<String>,
    broker_supports_scalable: bool,
    broker_supports_tc_discovery: bool,
}

/// Drive the tokio engine's `Client` through the scalable flow.
async fn observe_tokio(url: &str) -> Observed {
    let client = tokio::time::timeout(
        HANG_GUARD,
        TokioClient::connect(url, ConnectionConfig::default()),
    )
    .await
    .expect("tokio connect did not time out")
    .expect("tokio connect succeeded");

    let lookup = tokio::time::timeout(
        HANG_GUARD,
        client.scalable_topic_lookup("topic://public/default/scaled"),
    )
    .await
    .expect("tokio lookup did not time out")
    .expect("tokio lookup resolved");

    let assignment = tokio::time::timeout(
        HANG_GUARD,
        client.scalable_topic_subscribe(
            "topic://public/default/scaled",
            "sub",
            "consumer-a",
            42,
            ScalableConsumerType::Stream,
        ),
    )
    .await
    .expect("tokio subscribe did not time out")
    .expect("tokio subscribe resolved");

    let watch_id = client
        .watch_scalable_topics("public/default", vec![])
        .expect("tokio namespace watch opened");
    // Drain until the watch has applied both scripted updates (snapshot then
    // diff), so the observed set is the post-diff one on both engines.
    let topics = drain_topics_tokio(&client, watch_id).await;

    let observed = Observed {
        resolved_topic_name: lookup.resolved_topic_name.clone(),
        controller_broker_url: lookup.controller_broker_url.clone(),
        layout_epoch: lookup.epoch,
        segments: lookup
            .segments
            .iter()
            .map(|s| {
                (
                    s.segment_id.0,
                    s.key_range.start,
                    s.key_range.end,
                    s.broker_url.clone(),
                )
            })
            .collect(),
        assignment_epoch: assignment.layout_epoch,
        assignment_topics: assignment
            .segment_topics()
            .into_iter()
            .map(ToOwned::to_owned)
            .collect(),
        topics_watch: topics,
        broker_supports_scalable: client.broker_supports_scalable_topics(),
        broker_supports_tc_discovery: client.broker_supports_tc_metadata_discovery(),
    };

    client.close_scalable_topics_watch(watch_id);
    client.close_scalable_topic_session(lookup.session_id);
    observed
}

/// Drive the moonpool engine's `Client` through the identical flow.
async fn observe_moonpool(host_port: &str) -> Observed {
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let client = tokio::time::timeout(
        HANG_GUARD,
        MoonpoolClient::connect_plain(&engine, host_port, ConnectionConfig::default()),
    )
    .await
    .expect("moonpool connect did not time out")
    .expect("moonpool connect succeeded");

    let lookup = tokio::time::timeout(
        HANG_GUARD,
        client.scalable_topic_lookup("topic://public/default/scaled"),
    )
    .await
    .expect("moonpool lookup did not time out")
    .expect("moonpool lookup resolved");

    let assignment = tokio::time::timeout(
        HANG_GUARD,
        client.scalable_topic_subscribe(
            "topic://public/default/scaled",
            "sub",
            "consumer-a",
            42,
            ScalableConsumerType::Stream,
        ),
    )
    .await
    .expect("moonpool subscribe did not time out")
    .expect("moonpool subscribe resolved");

    let watch_id = client
        .watch_scalable_topics("public/default", vec![])
        .expect("moonpool namespace watch opened");
    let topics = drain_topics_moonpool(&client, watch_id).await;

    let observed = Observed {
        resolved_topic_name: lookup.resolved_topic_name.clone(),
        controller_broker_url: lookup.controller_broker_url.clone(),
        layout_epoch: lookup.epoch,
        segments: lookup
            .segments
            .iter()
            .map(|s| {
                (
                    s.segment_id.0,
                    s.key_range.start,
                    s.key_range.end,
                    s.broker_url.clone(),
                )
            })
            .collect(),
        assignment_epoch: assignment.layout_epoch,
        assignment_topics: assignment
            .segment_topics()
            .into_iter()
            .map(ToOwned::to_owned)
            .collect(),
        topics_watch: topics,
        broker_supports_scalable: client.broker_supports_scalable_topics(),
        broker_supports_tc_discovery: client.broker_supports_tc_metadata_discovery(),
    };

    client.close_scalable_topics_watch(watch_id);
    client.close_scalable_topic_session(lookup.session_id);
    observed
}

/// Wait until the namespace watch has applied the scripted diff, so both
/// engines are compared at the same point in the transcript rather than
/// whichever snapshot each happened to reach first.
async fn drain_topics_tokio(client: &TokioClient, watch_id: u64) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + HANG_GUARD;
    loop {
        if let Some(topics) = client.scalable_topics_snapshot(watch_id)
            && topics == vec!["topic://public/default/c".to_owned()]
        {
            return topics;
        }
        if tokio::time::Instant::now() >= deadline {
            return client
                .scalable_topics_snapshot(watch_id)
                .unwrap_or_default();
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

async fn drain_topics_moonpool<P>(client: &MoonpoolClient<P>, watch_id: u64) -> Vec<String>
where
    P: moonpool_core::Providers + Send + Sync + 'static,
{
    let deadline = tokio::time::Instant::now() + HANG_GUARD;
    loop {
        if let Some(topics) = client.scalable_topics_snapshot(watch_id)
            && topics == vec!["topic://public/default/c".to_owned()]
        {
            return topics;
        }
        if tokio::time::Instant::now() >= deadline {
            return client
                .scalable_topics_snapshot(watch_id)
                .unwrap_or_default();
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// (d) — the two engines' `Client`s observe an identical scalable exchange:
/// the resolved layout, the consumer's assignment, and the namespace-watch set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scalable_client_surface_parity() {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_observed = observe_tokio(&url).await;
    let moonpool_observed = observe_moonpool(&host_port).await;

    assert_eq!(
        tokio_observed, moonpool_observed,
        "engine client surfaces diverged on the scalable exchange"
    );

    // Pin the exchange itself, so a broker-script change cannot make both
    // engines agree on nothing.
    assert_eq!(
        tokio_observed.resolved_topic_name.as_deref(),
        Some("topic://public/default/scaled")
    );
    assert_eq!(
        tokio_observed.controller_broker_url.as_deref(),
        Some("pulsar://controller:6650")
    );
    assert_eq!(tokio_observed.layout_epoch, 1);
    assert_eq!(
        tokio_observed.segments,
        vec![
            (1, 0, 32_768, Some("pulsar://seg1:6650".to_owned())),
            (2, 32_768, 65_536, Some("pulsar://seg2:6650".to_owned())),
        ]
    );
    assert_eq!(tokio_observed.assignment_epoch, 1);
    assert_eq!(
        tokio_observed.assignment_topics,
        vec!["segment://public/default/scaled/1".to_owned()]
    );
    assert_eq!(
        tokio_observed.topics_watch,
        vec!["topic://public/default/c".to_owned()],
        "the diff removed `a` and added `c`"
    );
    assert!(tokio_observed.broker_supports_scalable);
    assert!(tokio_observed.broker_supports_tc_discovery);

    broker.shutdown().await;
}

/// (d) — the rebalance the broker pushes right after the registration reaches
/// both engines' event streams identically, naming what to attach and detach.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scalable_assignment_rebalance_parity() {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_delta = {
        let client = TokioClient::connect(&url, ConnectionConfig::default())
            .await
            .expect("tokio connect");
        client
            .scalable_topic_subscribe(
                "topic://public/default/scaled",
                "sub",
                "consumer-a",
                42,
                ScalableConsumerType::Stream,
            )
            .await
            .expect("tokio subscribe");
        wait_for_rebalance_tokio(&client).await
    };

    let moonpool_delta = {
        let engine = MoonpoolEngine::new(TokioProviders::new());
        let client =
            MoonpoolClient::connect_plain(&engine, &host_port, ConnectionConfig::default())
                .await
                .expect("moonpool connect");
        client
            .scalable_topic_subscribe(
                "topic://public/default/scaled",
                "sub",
                "consumer-a",
                42,
                ScalableConsumerType::Stream,
            )
            .await
            .expect("moonpool subscribe");
        wait_for_rebalance_moonpool(&client).await
    };

    assert_eq!(
        tokio_delta, moonpool_delta,
        "engine rebalance observations diverged"
    );
    // Segment 1 is replaced by segment 2 at epoch 2.
    assert_eq!(tokio_delta, Some((2, vec![2_u64], vec![1_u64])));

    broker.shutdown().await;
}

/// Drain the client's scalable events until the scripted rebalance lands,
/// returning `(layout_epoch, gained_ids, lost_ids)` **read off the event**.
///
/// Reading `lost` from the delta rather than hardcoding it is the point: the
/// held assignment only shows what the consumer ends up with, so a `lost` list
/// asserted from the test's own expectations would pass even if the client
/// computed it wrongly.
async fn wait_for_rebalance_tokio(client: &TokioClient) -> Option<(u64, Vec<u64>, Vec<u64>)> {
    let deadline = tokio::time::Instant::now() + HANG_GUARD;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(ev)) = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            client.next_scalable_event(),
        )
        .await
            && let magnetar_runtime_tokio::ScalableEvent::AssignmentChanged { delta, .. } = ev
        {
            return Some((
                delta.layout_epoch,
                delta.gained.iter().map(|s| s.segment_id.0).collect(),
                delta.lost.iter().map(|s| s.0).collect(),
            ));
        }
    }
    None
}

async fn wait_for_rebalance_moonpool<P>(
    client: &MoonpoolClient<P>,
) -> Option<(u64, Vec<u64>, Vec<u64>)>
where
    P: moonpool_core::Providers + Send + Sync + 'static,
{
    let deadline = tokio::time::Instant::now() + HANG_GUARD;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(ev)) = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            client.next_scalable_event(),
        )
        .await
            && let magnetar_runtime_moonpool::ScalableEvent::AssignmentChanged { delta, .. } = ev
        {
            return Some((
                delta.layout_epoch,
                delta.gained.iter().map(|s| s.segment_id.0).collect(),
                delta.lost.iter().map(|s| s.0).collect(),
            ));
        }
    }
    None
}

/// (d) — a **rejected** registration surfaces identically on both engines, and
/// the TC-assignment discovery watch delivers the same coordinator set.
///
/// Both paths cross the driver's event drain, so this is the only place the
/// rejection and TC arms are exercised end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scalable_rejection_and_tc_discovery_parity() {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let url = broker.pulsar_url();
    let host_port = broker.host_port();

    // Consumer id 99 is the scripted rejection.
    let tokio_outcome = {
        let client = TokioClient::connect(&url, ConnectionConfig::default())
            .await
            .expect("tokio connect");
        let rejected = client
            .scalable_topic_subscribe(
                "topic://public/default/scaled",
                "sub",
                "consumer-denied",
                99,
                ScalableConsumerType::Checkpoint,
            )
            .await
            .expect_err("tokio registration rejected")
            .to_string();
        let watch_id = client
            .watch_tc_assignments()
            .expect("tokio TC discovery opened");
        let tc = wait_for_tc_tokio(&client).await;
        client.close_tc_assignments_watch(watch_id);
        (rejected, tc)
    };

    let moonpool_outcome = {
        let engine = MoonpoolEngine::new(TokioProviders::new());
        let client =
            MoonpoolClient::connect_plain(&engine, &host_port, ConnectionConfig::default())
                .await
                .expect("moonpool connect");
        let rejected = client
            .scalable_topic_subscribe(
                "topic://public/default/scaled",
                "sub",
                "consumer-denied",
                99,
                ScalableConsumerType::Checkpoint,
            )
            .await
            .expect_err("moonpool registration rejected")
            .to_string();
        let watch_id = client
            .watch_tc_assignments()
            .expect("moonpool TC discovery opened");
        let tc = wait_for_tc_moonpool(&client).await;
        client.close_tc_assignments_watch(watch_id);
        (rejected, tc)
    };

    assert_eq!(
        tokio_outcome, moonpool_outcome,
        "engine rejection / TC-discovery observations diverged"
    );
    assert!(
        tokio_outcome
            .0
            .contains("not permitted on this subscription"),
        "the broker's rejection message reaches the caller: {}",
        tokio_outcome.0
    );
    assert_eq!(
        tokio_outcome.1,
        Some((2_u32, vec![0_u64, 1_u64])),
        "both coordinators are discovered"
    );

    broker.shutdown().await;
}

/// Drain the client's scalable events until the TC-assignment snapshot lands.
async fn wait_for_tc_tokio(client: &TokioClient) -> Option<(u32, Vec<u64>)> {
    let deadline = tokio::time::Instant::now() + HANG_GUARD;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(ev)) = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            client.next_scalable_event(),
        )
        .await
            && let magnetar_runtime_tokio::ScalableEvent::TcAssignmentsChanged {
                parallelism,
                assignments,
                ..
            } = ev
        {
            return Some((parallelism, assignments.iter().map(|a| a.tc_id).collect()));
        }
    }
    None
}

async fn wait_for_tc_moonpool<P>(client: &MoonpoolClient<P>) -> Option<(u32, Vec<u64>)>
where
    P: moonpool_core::Providers + Send + Sync + 'static,
{
    let deadline = tokio::time::Instant::now() + HANG_GUARD;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(ev)) = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            client.next_scalable_event(),
        )
        .await
            && let magnetar_runtime_moonpool::ScalableEvent::TcAssignmentsChanged {
                parallelism,
                assignments,
                ..
            } = ev
        {
            return Some((parallelism, assignments.iter().map(|a| a.tc_id).collect()));
        }
    }
    None
}

/// (d) — a **rejected lookup** must surface as an error on both engines, not
/// hang the caller.
///
/// The session ends as `DagWatchClosed`, not `LookupResolved`; a wait loop that
/// only recognises the success variant blocks until the connection closes.
/// `scalable_topic_subscribe` had always raced its two outcomes — this pins the
/// same contract for the lookup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scalable_lookup_rejection_errors_rather_than_hangs() {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_err = {
        let client = TokioClient::connect(&url, ConnectionConfig::default())
            .await
            .expect("tokio connect");
        // The timeout is the assertion: before the fix this call never returned.
        tokio::time::timeout(
            HANG_GUARD,
            client.scalable_topic_lookup("topic://public/default/e2e-missing"),
        )
        .await
        .expect("tokio lookup returned rather than hanging")
        .expect_err("a rejected lookup is an error")
        .to_string()
    };

    let moonpool_err = {
        let engine = MoonpoolEngine::new(TokioProviders::new());
        let client =
            MoonpoolClient::connect_plain(&engine, &host_port, ConnectionConfig::default())
                .await
                .expect("moonpool connect");
        tokio::time::timeout(
            HANG_GUARD,
            client.scalable_topic_lookup("topic://public/default/e2e-missing"),
        )
        .await
        .expect("moonpool lookup returned rather than hanging")
        .expect_err("a rejected lookup is an error")
        .to_string()
    };

    assert_eq!(
        tokio_err, moonpool_err,
        "engine lookup-rejection observations diverged"
    );
    assert!(
        tokio_err.contains("scripted: topic does not exist"),
        "the broker's reason reaches the caller: {tokio_err}"
    );

    broker.shutdown().await;
}

/// (d) — a **pushed layout** on an open session reaches the caller identically
/// on both engines: the update event, the drop-on-change signal, and the
/// assignment the client holds.
///
/// The other client tests only ever see a session's *first* layout, so without
/// this the driver's `SegmentDagUpdated` / `DagChangedDuringConsume` forwarding
/// is never exercised end to end — only the sans-io half is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scalable_pushed_layout_reaches_the_client() {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_seen = {
        let client = TokioClient::connect(&url, ConnectionConfig::default())
            .await
            .expect("tokio connect");
        client
            .scalable_topic_lookup("topic://public/default/scaled-split")
            .await
            .expect("tokio lookup resolves");
        // The registration is what makes `scalable_consumer_assignment`
        // meaningful; assert through it as well as through the event.
        client
            .scalable_topic_subscribe(
                "topic://public/default/scaled-split",
                "sub",
                "consumer-a",
                42,
                ScalableConsumerType::Stream,
            )
            .await
            .expect("tokio subscribe");
        // The scripted broker follows every subscribe with a rebalance, so the
        // held epoch races that push — assert the assignment is readable and
        // non-empty, and leave the epoch to `scalable_assignment_rebalance_parity`.
        let held = client
            .scalable_consumer_assignment(42)
            .map(|a| !a.segments.is_empty());
        (wait_for_split_tokio(&client).await, held)
    };

    let moonpool_seen = {
        let engine = MoonpoolEngine::new(TokioProviders::new());
        let client =
            MoonpoolClient::connect_plain(&engine, &host_port, ConnectionConfig::default())
                .await
                .expect("moonpool connect");
        client
            .scalable_topic_lookup("topic://public/default/scaled-split")
            .await
            .expect("moonpool lookup resolves");
        client
            .scalable_topic_subscribe(
                "topic://public/default/scaled-split",
                "sub",
                "consumer-a",
                42,
                ScalableConsumerType::Stream,
            )
            .await
            .expect("moonpool subscribe");
        let held = client
            .scalable_consumer_assignment(42)
            .map(|a| !a.segments.is_empty());
        (wait_for_split_moonpool(&client).await, held)
    };

    assert_eq!(
        tokio_seen, moonpool_seen,
        "engine pushed-layout observations diverged"
    );
    // epoch 2, one split, parent 1 removed.
    assert_eq!(tokio_seen.0, Some((2, 1, vec![1_u64])));
    assert_eq!(
        tokio_seen.1,
        Some(true),
        "the held assignment is readable and non-empty"
    );

    broker.shutdown().await;
}

/// Drain until the pushed split lands, returning
/// `(epoch, split_count, removed_ids)` read off the delta.
async fn wait_for_split_tokio(client: &TokioClient) -> Option<(u64, usize, Vec<u64>)> {
    let deadline = tokio::time::Instant::now() + HANG_GUARD;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(ev)) = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            client.next_scalable_event(),
        )
        .await
            && let magnetar_runtime_tokio::ScalableEvent::DagUpdated { delta, .. } = ev
        {
            return Some((
                delta.epoch,
                delta.split_events.len(),
                delta.removed.iter().map(|s| s.0).collect(),
            ));
        }
    }
    None
}

async fn wait_for_split_moonpool<P>(client: &MoonpoolClient<P>) -> Option<(u64, usize, Vec<u64>)>
where
    P: moonpool_core::Providers + Send + Sync + 'static,
{
    let deadline = tokio::time::Instant::now() + HANG_GUARD;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(ev)) = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            client.next_scalable_event(),
        )
        .await
            && let magnetar_runtime_moonpool::ScalableEvent::DagUpdated { delta, .. } = ev
        {
            return Some((
                delta.epoch,
                delta.split_events.len(),
                delta.removed.iter().map(|s| s.0).collect(),
            ));
        }
    }
    None
}

/// A rejection carrying no `message` still surfaces a reason — the client's own
/// fallback wording — rather than an empty error string.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scalable_lookup_rejection_without_message_still_reports() {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_err = {
        let client = TokioClient::connect(&url, ConnectionConfig::default())
            .await
            .expect("tokio connect");
        client
            .scalable_topic_lookup("topic://public/default/e2e-terse")
            .await
            .expect_err("terse rejection is an error")
            .to_string()
    };
    let moonpool_err = {
        let engine = MoonpoolEngine::new(TokioProviders::new());
        let client =
            MoonpoolClient::connect_plain(&engine, &host_port, ConnectionConfig::default())
                .await
                .expect("moonpool connect");
        client
            .scalable_topic_lookup("topic://public/default/e2e-terse")
            .await
            .expect_err("terse rejection is an error")
            .to_string()
    };

    assert_eq!(tokio_err, moonpool_err);
    assert!(
        !tokio_err.is_empty(),
        "a message-less rejection still names a reason: {tokio_err}"
    );

    broker.shutdown().await;
}

/// (d) — a refused namespace watch and a refused coordinator-discovery watch
/// both reach the caller as close events on either engine.
///
/// These are the only two driver arms a happy-path transcript never runs, and
/// they are exactly the ones a caller depends on to tell "refused" from "no
/// matching topics" / "no coordinators".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scalable_watch_refusals_reach_the_client() {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_seen = {
        let client = TokioClient::connect(&url, ConnectionConfig::default())
            .await
            .expect("tokio connect");
        // watch_id 1 — the scripted refusal for a `-deny` namespace.
        client
            .watch_scalable_topics("public/default-deny", vec![])
            .expect("watch opens");
        // watch_id 2 — even, so the scripted TC refusal fires.
        client.watch_tc_assignments().expect("tc watch opens");
        drain_refusals_tokio(&client).await
    };

    let moonpool_seen = {
        let engine = MoonpoolEngine::new(TokioProviders::new());
        let client =
            MoonpoolClient::connect_plain(&engine, &host_port, ConnectionConfig::default())
                .await
                .expect("moonpool connect");
        client
            .watch_scalable_topics("public/default-deny", vec![])
            .expect("watch opens");
        client.watch_tc_assignments().expect("tc watch opens");
        drain_refusals_moonpool(&client).await
    };

    assert_eq!(
        tokio_seen, moonpool_seen,
        "engine watch-refusal observations diverged"
    );
    let (topics_reason, tc_reason) = tokio_seen;
    assert!(
        topics_reason
            .as_deref()
            .is_some_and(|r| r.contains("namespace watch refused")),
        "the namespace-watch refusal names its reason: {topics_reason:?}"
    );
    assert!(
        tc_reason
            .as_deref()
            .is_some_and(|r| r.contains("coordinators unavailable")),
        "the coordinator-watch refusal names its reason: {tc_reason:?}"
    );

    broker.shutdown().await;
}

/// Drain until both refusals have been seen, returning their reasons.
async fn drain_refusals_tokio(client: &TokioClient) -> (Option<String>, Option<String>) {
    let deadline = tokio::time::Instant::now() + HANG_GUARD;
    let (mut topics, mut tc) = (None, None);
    while tokio::time::Instant::now() < deadline && (topics.is_none() || tc.is_none()) {
        let Ok(Some(ev)) = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            client.next_scalable_event(),
        )
        .await
        else {
            continue;
        };
        match ev {
            magnetar_runtime_tokio::ScalableEvent::TopicsWatchClosed { reason, .. } => {
                topics = reason;
            }
            magnetar_runtime_tokio::ScalableEvent::TcAssignmentsWatchClosed { reason, .. } => {
                tc = reason;
            }
            _ => {}
        }
    }
    (topics, tc)
}

async fn drain_refusals_moonpool<P>(client: &MoonpoolClient<P>) -> (Option<String>, Option<String>)
where
    P: moonpool_core::Providers + Send + Sync + 'static,
{
    let deadline = tokio::time::Instant::now() + HANG_GUARD;
    let (mut topics, mut tc) = (None, None);
    while tokio::time::Instant::now() < deadline && (topics.is_none() || tc.is_none()) {
        let Ok(Some(ev)) = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            client.next_scalable_event(),
        )
        .await
        else {
            continue;
        };
        match ev {
            magnetar_runtime_moonpool::ScalableEvent::TopicsWatchClosed { reason, .. } => {
                topics = reason;
            }
            magnetar_runtime_moonpool::ScalableEvent::TcAssignmentsWatchClosed {
                reason, ..
            } => {
                tc = reason;
            }
            _ => {}
        }
    }
    (topics, tc)
}

/// (d) — a subscribe on a connection that dies mid-wait returns an error
/// rather than waiting for an assignment that can never come.
///
/// `ScriptedBroker::shutdown` stops accepting but leaves established sessions
/// serving, so this uses a socket that completes the handshake and then hangs
/// up — the shape a broker crash presents to a client that has already
/// negotiated the capability.
///
/// Both engines are asserted. Until the driver woke scalable waiters on
/// disconnect this only passed by luck: the loops re-check `is_closed()` when a
/// scalable event arrives, and a dying connection sends none, so whether the
/// guard ran at all depended on winning a race against EOF. It passed locally
/// and timed out on CI for exactly that reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scalable_subscribe_errors_when_the_connection_closes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        // Read the client's CommandConnect, answer it advertising the PIP-460
        // capability, then hang up.
        let mut buf = [0_u8; 4096];
        let _ = sock.read(&mut buf).await;
        let cmd = magnetar_proto::pb::BaseCommand {
            r#type: magnetar_proto::pb::base_command::Type::Connected as i32,
            connected: Some(magnetar_proto::pb::CommandConnected {
                server_version: "closing-broker".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(magnetar_proto::pb::FeatureFlags {
                    supports_scalable_topics: Some(true),
                    ..magnetar_proto::pb::FeatureFlags::default()
                }),
            }),
            ..Default::default()
        };
        let mut out = bytes::BytesMut::new();
        magnetar_proto::encode_command(&mut out, &cmd).expect("encode Connected");
        let _ = sock.write_all(&out).await;
        let _ = sock.flush().await;
        // Drop closes the socket.
    });

    let client = TokioClient::connect(&format!("pulsar://{addr}"), ConnectionConfig::default())
        .await
        .expect("handshake completes before the hang-up");

    let err = tokio::time::timeout(
        HANG_GUARD,
        client.scalable_topic_subscribe(
            "topic://public/default/scaled",
            "sub",
            "consumer-a",
            77,
            ScalableConsumerType::Stream,
        ),
    )
    .await
    .expect("subscribe returned rather than hanging")
    .expect_err("a dead connection cannot assign");
    assert!(
        !err.to_string().is_empty(),
        "the failure names a reason: {err}"
    );

    // Same contract on the moonpool engine, against a second hang-up socket.
    let addr = spawn_hangup_broker().await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let mp_client =
        MoonpoolClient::connect_plain(&engine, &addr.to_string(), ConnectionConfig::default())
            .await
            .expect("moonpool handshake completes before the hang-up");

    let mp_err = tokio::time::timeout(
        HANG_GUARD,
        mp_client.scalable_topic_subscribe(
            "topic://public/default/scaled",
            "sub",
            "consumer-a",
            77,
            ScalableConsumerType::Stream,
        ),
    )
    .await
    .expect("moonpool subscribe returned rather than hanging")
    .expect_err("a dead connection cannot assign");
    assert!(
        !mp_err.to_string().is_empty(),
        "the failure names a reason: {mp_err}"
    );
}

/// Bind a socket that completes one Pulsar handshake — advertising the PIP-460
/// capability — and then hangs up. Models a broker crash after negotiation.
async fn spawn_hangup_broker() -> std::net::SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        let mut buf = [0_u8; 4096];
        let _ = sock.read(&mut buf).await;
        let cmd = magnetar_proto::pb::BaseCommand {
            r#type: magnetar_proto::pb::base_command::Type::Connected as i32,
            connected: Some(magnetar_proto::pb::CommandConnected {
                server_version: "closing-broker".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(magnetar_proto::pb::FeatureFlags {
                    supports_scalable_topics: Some(true),
                    ..magnetar_proto::pb::FeatureFlags::default()
                }),
            }),
            ..Default::default()
        };
        let mut out = bytes::BytesMut::new();
        magnetar_proto::encode_command(&mut out, &cmd).expect("encode Connected");
        let _ = sock.write_all(&out).await;
        let _ = sock.flush().await;
    });
    addr
}

/// The scripted broker tolerates a scalable command frame whose payload is
/// absent — the `None` arm of each of its four dispatch branches.
///
/// A real broker will never send one, but the fake is shared test
/// infrastructure: if it panicked on a malformed frame, every differential test
/// would fail with the fake's stack rather than the client's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scripted_broker_tolerates_payloadless_scalable_frames() {
    use tokio::io::AsyncWriteExt;

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let mut sock = tokio::net::TcpStream::connect(broker.host_port())
        .await
        .expect("raw connect");

    for cmd_type in [
        magnetar_proto::pb::base_command::Type::ScalableTopicLookup,
        magnetar_proto::pb::base_command::Type::ScalableTopicSubscribe,
        magnetar_proto::pb::base_command::Type::WatchScalableTopics,
        magnetar_proto::pb::base_command::Type::WatchTcAssignments,
    ] {
        let cmd = magnetar_proto::pb::BaseCommand {
            r#type: cmd_type as i32,
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        magnetar_proto::encode_command(&mut buf, &cmd).expect("encode payloadless");
        sock.write_all(&buf).await.expect("write payloadless frame");
    }
    sock.flush().await.expect("flush");

    // The broker must still be serving: a fresh client completes a handshake and
    // a lookup after the malformed traffic.
    let client = TokioClient::connect(&broker.pulsar_url(), ConnectionConfig::default())
        .await
        .expect("broker still accepts connections");
    let lookup = tokio::time::timeout(
        HANG_GUARD,
        client.scalable_topic_lookup("topic://public/default/scaled"),
    )
    .await
    .expect("lookup did not hang")
    .expect("lookup resolves after the malformed frames");
    assert_eq!(lookup.epoch, 1);

    broker.shutdown().await;
}

/// (d) — a connection **reset** (RST) mid-wait errors the same way a clean
/// hang-up does, on both engines.
///
/// The drivers take a different branch for each: a clean close returns
/// `Ok(0)`, a reset returns `Err(ECONNRESET)`. Both must mark the connection
/// disconnected *and* wake the scalable waiters, or the caller parks forever on
/// whichever branch was missed — which is precisely the bug the sibling
/// hang-up test caught on the `Ok(0)` side.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scalable_subscribe_errors_when_the_connection_resets() {
    let addr = spawn_resetting_broker().await;
    let client = TokioClient::connect(&format!("pulsar://{addr}"), ConnectionConfig::default())
        .await
        .expect("handshake completes before the reset");
    let err = tokio::time::timeout(
        HANG_GUARD,
        client.scalable_topic_subscribe(
            "topic://public/default/scaled",
            "sub",
            "consumer-a",
            88,
            ScalableConsumerType::Stream,
        ),
    )
    .await
    .expect("tokio subscribe returned rather than hanging")
    .expect_err("a reset connection cannot assign");
    assert!(
        !err.to_string().is_empty(),
        "the failure names a reason: {err}"
    );

    let addr = spawn_resetting_broker().await;
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let mp_client =
        MoonpoolClient::connect_plain(&engine, &addr.to_string(), ConnectionConfig::default())
            .await
            .expect("moonpool handshake completes before the reset");
    let mp_err = tokio::time::timeout(
        HANG_GUARD,
        mp_client.scalable_topic_subscribe(
            "topic://public/default/scaled",
            "sub",
            "consumer-a",
            88,
            ScalableConsumerType::Stream,
        ),
    )
    .await
    .expect("moonpool subscribe returned rather than hanging")
    .expect_err("a reset connection cannot assign");
    assert!(
        !mp_err.to_string().is_empty(),
        "the failure names a reason: {mp_err}"
    );
}

/// Bind a socket that completes one Pulsar handshake and then **resets** the
/// connection, so the client's read fails rather than seeing a clean EOF.
///
/// `SO_LINGER = 0` makes `close(2)` emit RST instead of FIN.
async fn spawn_resetting_broker() -> std::net::SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        let mut buf = [0_u8; 4096];
        let _ = sock.read(&mut buf).await;
        let cmd = magnetar_proto::pb::BaseCommand {
            r#type: magnetar_proto::pb::base_command::Type::Connected as i32,
            connected: Some(magnetar_proto::pb::CommandConnected {
                server_version: "resetting-broker".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(magnetar_proto::pb::FeatureFlags {
                    supports_scalable_topics: Some(true),
                    ..magnetar_proto::pb::FeatureFlags::default()
                }),
            }),
            ..Default::default()
        };
        let mut out = bytes::BytesMut::new();
        magnetar_proto::encode_command(&mut out, &cmd).expect("encode Connected");
        let _ = sock.write_all(&out).await;
        let _ = sock.flush().await;
        // Give the client time to consume CONNECTED, then reset rather than
        // closing cleanly. `SO_LINGER = 0` makes close(2) emit RST instead of
        // FIN; tokio's own `set_linger` is deprecated (it blocks the thread on
        // drop), so this goes through socket2 on the borrowed fd.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let sock2 = socket2::SockRef::from(&sock);
        let _ = sock2.set_linger(Some(std::time::Duration::ZERO));
        drop(sock);
    });
    addr
}
