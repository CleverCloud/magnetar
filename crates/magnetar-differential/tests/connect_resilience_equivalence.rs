// SPDX-License-Identifier: Apache-2.0

//! Layer (d) of the ADR-0024 four-layer policy for the dual-cap
//! initial-dial retry (ADR-0052): tokio ↔ moonpool differential
//! equivalence.
//!
//! The ADR-0052 change rewired the *initial* dial behind a dual cap
//! (`connect_max_retries` count + `operation_timeout` total budget) in
//! both engines. On the **fault-free** path — the only path the
//! differential harness exercises, since the scripted broker is always
//! listening and answers the handshake immediately — the retry loop must
//! be a transparent pass-through: a single dial attempt succeeds, no
//! backoff, no timeout. This test pins that the two engines' connect →
//! producer-open → close round-trip still produces byte-identical
//! [`EventStream`]s after the rewrite, i.e. the new retry path does not
//! change observable parity on the happy path.
//!
//! Both runners dial via `ConnectionConfig::default()`, which now carries
//! the new dual-cap defaults (`connect_timeout=10s`,
//! `connect_max_retries=8`, `operation_timeout=30s` — pinned by the proto
//! unit test), so this test transitively exercises the production default
//! config through the new `dial_with_retry` on both engines.
//!
//! ## Also covers the post-dial handshake bound (ADR-0052, extended)
//!
//! The handshake bound that extends `operation_timeout` to the post-dial
//! `CONNECT` → `CONNECTED` round-trip (moonpool `handshake_plain` arms a
//! single `TimeProvider::sleep` deadline; tokio wraps `wait_connected` in
//! `tokio::time::timeout`) sits directly on the connect path both runners
//! take. On the fault-free path the scripted broker answers CONNECT
//! immediately, so the deadline is armed but never fires — it must be a
//! transparent pass-through, leaving the two engines' event streams
//! byte-identical. This test is therefore the differential (layer-d)
//! coverage for the handshake bound as well as the dial cap: a regression
//! that perturbed the handshake path on either engine (an extra event, a
//! spurious timeout, a reordered frame) would diverge the streams here.

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Event, Op, Trace, runner_moonpool, runner_tokio};

#[tokio::test(flavor = "current_thread")]
async fn fault_free_connect_event_stream_parity() {
    // A single `Close` op: the harness still connects (new dual-cap
    // dial), opens the producer, then closes — so this trace's one event
    // is downstream of a full fault-free connect on both engines. If the
    // ADR-0052 rewrite perturbed the connect path on either engine
    // (extra dial attempt, spurious error, reordered handshake), the
    // streams would diverge here.
    let trace = Trace::new(
        "persistent://public/default/connect-resilience-equiv",
        "sub-connect-resilience",
        vec![Op::Close],
    );

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_stream = runner_tokio::run(&pulsar_url, &trace)
        .await
        .expect("tokio runner");
    let moonpool_stream = runner_moonpool::run(&host_port, &trace)
        .await
        .expect("moonpool runner");

    // The connect itself is fault-free, so both engines complete the
    // handshake + producer open + close identically.
    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged on the fault-free connect → close round-trip \
         (the ADR-0052 dual-cap retry must be a transparent pass-through on the happy path)",
    );
    assert_eq!(
        tokio_stream.events,
        vec![Event::Closed],
        "fault-free connect+close must resolve to exactly one Closed event on both engines",
    );

    broker.shutdown().await;
}
