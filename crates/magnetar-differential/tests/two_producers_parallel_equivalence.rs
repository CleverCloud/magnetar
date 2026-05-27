// SPDX-License-Identifier: Apache-2.0

//! ADR-0038 Phase 3 differential equivalence — the tokio and moonpool
//! engines MUST produce byte-identical `EventStream`s under the
//! per-slot hot-path send (Phase 3). Layer (d) of the ADR-0024
//! four-layer test policy.
//!
//! Both engines route `Producer::send` through
//! `magnetar_proto::ProducerSlot::queue_send` (Phase 3) — this test
//! drives a high-volume send sequence through both runners and asserts
//! their event streams agree. If either engine accidentally regressed
//! to the global-lock path or reordered the queue→drain handoff, the
//! resulting frames would land in a different order and the streams
//! would diverge.

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Op, Trace, runner_moonpool, runner_tokio};

#[tokio::test(flavor = "current_thread")]
async fn many_sends_via_per_slot_hot_path_event_stream_parity() {
    // 32 back-to-back sends — enough that the per-slot drain has to merge
    // multiple frames in a single driver tick. Both engines should
    // produce 32 `Event::Sent` entries with the same broker-assigned
    // message ids.
    let ops: Vec<Op> = (0..32_u8)
        .map(|i| Op::Send {
            payload: vec![i; 16],
        })
        .collect();
    let trace = Trace::new(
        "persistent://public/default/slot-hotpath-equiv",
        "sub-hot",
        ops,
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

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the per-slot hot-path send sequence",
    );

    broker.shutdown().await;
}
