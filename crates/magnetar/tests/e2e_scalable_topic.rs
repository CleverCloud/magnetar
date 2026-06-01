// SPDX-License-Identifier: Apache-2.0

//! PIP-460 / ADR-0031 — scalable-topic end-to-end coverage.
//!
//! **BLOCKED on upstream Pulsar 5.0 RC.** PIP-460 is upstream `Draft`; **no
//! released Pulsar broker ships the scalable-topic wire surface today**. This
//! file compiles under `feature = "e2e,scalable-topics"` so the e2e surface is
//! wired and the test names exist, but every test is
//! `#[ignore = "e2e: requires Pulsar 5.0 with PIP-460"]` and can never run
//! against a 4.x broker. Per ADR-0031 e2e is **best-effort** on this surface —
//! the four-layer in-process tests (proto unit + tokio + moonpool +
//! differential) are the binding acceptance gate, and this file **does NOT
//! block release**.
//!
//! When Pulsar 5.0 cuts an RC with `scalableTopicsEnabled=true` (broker config
//! TBD by upstream), flesh these out against an
//! `apachepulsar/pulsar:5.0.0-rc-*` `testcontainers-rs` spawn — see the
//! `e2e_shadow_topic_replicator.rs` fixture for the auth + container pattern.
//! Coverage to add: (1) lookup-then-consume happy path; (2) `topic-info` CLI
//! round-trip; (3) drop-on-DAG-change observed against a broker-driven split.

#![cfg(all(feature = "e2e", feature = "scalable-topics"))]

/// Marker indicating the upstream Pulsar version that must ship PIP-460 before
/// these tests can run. Pinned so a future RC bump has a single edit point.
const REQUIRES_PULSAR: &str = "5.0.0-rc (PIP-460, scalableTopicsEnabled=true)";

/// (1) Lookup-then-consume happy path against a real PIP-460 broker.
#[tokio::test]
#[ignore = "e2e: requires Pulsar 5.0 with PIP-460"]
async fn e2e_scalable_topic_lookup_then_consume() {
    // TODO(pulsar-5.0-rc): spawn `apachepulsar/pulsar:5.0.0-rc-*` with
    // `scalableTopicsEnabled=true`, create a scalable topic, open a
    // `PulsarClient::scalable_stream_consumer("topic://...")`, and assert the
    // resolved DAG matches the broker-reported segment layout.
    let _ = REQUIRES_PULSAR;
}

/// (2) `topic-info` CLI round-trip against a real PIP-460 broker.
#[tokio::test]
#[ignore = "e2e: requires Pulsar 5.0 with PIP-460"]
async fn e2e_scalable_topic_info_cli_round_trip() {
    // TODO(pulsar-5.0-rc): run the `magnetar topic-info topic://...`
    // subcommand against the broker and assert the printed DAG matches the
    // lookup response.
    let _ = REQUIRES_PULSAR;
}

/// (3) Drop-on-DAG-change observed against a broker-driven segment split.
#[tokio::test]
#[ignore = "e2e: requires Pulsar 5.0 with PIP-460"]
async fn e2e_scalable_topic_drops_on_broker_split() {
    // TODO(pulsar-5.0-rc): trigger a broker-side segment split (admin API TBD
    // by upstream) while a StreamConsumer is active and assert it surfaces
    // `ConsumerEvent::DagChanged { reason: Split }` (drop-on-change).
    let _ = REQUIRES_PULSAR;
}
